# dagster_assets/asset_checks.py
"""
Asset checks para el bronze layer de OHLCV.

Cada exchange/market_type tiene 4 checks:
  1. row_count    — verifica que existan datos (filas > 0)
  2. schema       — verifica columnas OHLCV requeridas
  3. freshness    — verifica que el dato no sea demasiado antiguo
  4. gap_severity — detecta gaps HIGH en la serie temporal

Semántica de severidad (Dagster 1.7+ API):
  severity va en AssetCheckResult, NO en @asset_check.
  Un check FAIL no bloquea downstream (severity=WARN) salvo que sea
  crítico (severity=ERROR → Dagster bloquea la cadena).

Principios:
  OCP  : make_bronze_checks extensible por exchange/market sin modificar código
  DIP  : IcebergStorage y scan_gaps inyectados vía imports (mockables en tests)
  SSOT : ALL_ASSET_CHECKS es la única lista registrada en Definitions
"""

from __future__ import annotations

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    asset_check,
)

from dagster_assets.resources import OCMResource

from market_data.processing.utils.gap_utils import scan_gaps
from market_data.storage.iceberg.iceberg_storage import IcebergStorage

_REQUIRED_COLS = frozenset({"timestamp", "open", "high", "low", "close", "volume"})


def make_bronze_checks(exchange_name: str, market_type: str) -> list:
    """
    Factory — genera los 4 asset checks estándar para un exchange/market_type.

    OCP: añadir un nuevo exchange = llamar esta función, sin modificar nada más.
    """

    asset_key = f"bronze_ohlcv_{exchange_name}_{market_type}"

    # ------------------------------------------------------------------
    # 1. ROW COUNT
    #    blocking=True — tabla vacía es error crítico, bloquea downstream.
    # ------------------------------------------------------------------
    @asset_check(
        asset       = asset_key,
        name        = f"row_count_{exchange_name}_{market_type}",
        description = "Verifica que la tabla bronze tenga al menos 1 fila.",
        blocking    = True,
    )
    def _row_count_check(
        context,
        ocm: OCMResource,
    ) -> AssetCheckResult:

        ocm.build_runtime_context()
        storage = IcebergStorage(exchange=exchange_name, market_type=market_type)
        count   = storage.count()

        if count == 0:
            return AssetCheckResult(
                passed      = False,
                description = f"Tabla bronze vacía para {exchange_name}/{market_type}",
                severity    = AssetCheckSeverity.ERROR,
                metadata    = {"row_count": 0},
            )

        return AssetCheckResult(
            passed      = True,
            description = f"{count} filas presentes",
            metadata    = {"row_count": count},
        )

    # ------------------------------------------------------------------
    # 2. SCHEMA
    #    blocking=True — columnas faltantes rompen todo el pipeline downstream.
    # ------------------------------------------------------------------
    @asset_check(
        asset       = asset_key,
        name        = f"schema_{exchange_name}_{market_type}",
        description = "Verifica que las columnas OHLCV requeridas existan.",
        blocking    = True,
    )
    def _schema_check(
        context,
        ocm: OCMResource,
    ) -> AssetCheckResult:

        runtime_ctx = ocm.build_runtime_context()
        app_cfg     = runtime_ctx.app_config
        exc_cfg     = app_cfg.get_exchange(exchange_name)

        if exc_cfg is None:
            return AssetCheckResult(
                passed      = False,
                description = "Exchange no encontrado",
                severity    = AssetCheckSeverity.ERROR,
            )

        symbols = (
            exc_cfg.markets.spot_symbols
            if market_type == "spot"
            else exc_cfg.markets.futures_symbols
        )
        if not symbols:
            return AssetCheckResult(
                passed      = True,
                description = "Sin símbolos — skip",
            )

        storage = IcebergStorage(exchange=exchange_name, market_type=market_type)
        tf      = app_cfg.pipeline.historical.timeframes[0]
        df      = storage.load_ohlcv(symbol=symbols[0], timeframe=tf)

        if df is None or df.empty:
            return AssetCheckResult(
                passed      = True,
                description = "Sin datos para verificar schema — skip",
            )

        actual_cols  = frozenset(df.columns)
        missing_cols = _REQUIRED_COLS - actual_cols
        passed       = len(missing_cols) == 0

        return AssetCheckResult(
            passed      = passed,
            description = (
                "Schema OK" if passed
                else f"Columnas faltantes: {sorted(missing_cols)}"
            ),
            severity    = AssetCheckSeverity.ERROR,
            metadata    = {
                "required": str(sorted(_REQUIRED_COLS)),
                "present":  str(sorted(actual_cols & _REQUIRED_COLS)),
                "missing":  str(sorted(missing_cols)),
            },
        )

    # ------------------------------------------------------------------
    # 3. FRESHNESS
    #    blocking=False — dato viejo es WARN, no bloquea pipeline.
    # ------------------------------------------------------------------
    @asset_check(
        asset       = asset_key,
        name        = f"freshness_{exchange_name}_{market_type}",
        description = "Verifica que el último dato no tenga más de 24h de antigüedad.",
    )
    def _freshness_check(
        context,
        ocm: OCMResource,
    ) -> AssetCheckResult:
        import time

        ocm.build_runtime_context()
        storage    = IcebergStorage(exchange=exchange_name, market_type=market_type)
        last_ts_ms = storage.get_last_timestamp()

        if last_ts_ms is None:
            return AssetCheckResult(
                passed      = True,
                description = "Sin datos todavía — skip",
            )

        age_hours = (time.time() * 1000 - last_ts_ms) / (1000 * 3600)
        passed    = age_hours < 24.0

        return AssetCheckResult(
            passed      = passed,
            description = (
                f"Dato fresco ({age_hours:.1f}h)"
                if passed
                else f"Dato desactualizado: {age_hours:.1f}h sin actualizar"
            ),
            severity    = AssetCheckSeverity.WARN,
            metadata    = {"age_hours": round(age_hours, 2)},
        )

    # ------------------------------------------------------------------
    # 4. GAP SEVERITY
    #    blocking=False — gaps HIGH son WARN, requieren atención pero no
    #    bloquean la cadena (el repair job los corregirá).
    # ------------------------------------------------------------------
    @asset_check(
        asset       = asset_key,
        name        = f"gap_severity_{exchange_name}_{market_type}",
        description = "Detecta gaps de severidad HIGH en la serie OHLCV.",
    )
    def _gap_check(
        context,
        ocm: OCMResource,
    ) -> AssetCheckResult:

        runtime_ctx = ocm.build_runtime_context()
        app_cfg     = runtime_ctx.app_config
        exc_cfg     = app_cfg.get_exchange(exchange_name)

        if exc_cfg is None:
            return AssetCheckResult(
                passed      = True,
                description = "Exchange no encontrado — skip",
            )

        symbols = (
            exc_cfg.markets.spot_symbols
            if market_type == "spot"
            else exc_cfg.markets.futures_symbols
        )
        if not symbols:
            return AssetCheckResult(
                passed      = True,
                description = "Sin símbolos — skip",
            )

        storage   = IcebergStorage(exchange=exchange_name, market_type=market_type)
        tf        = app_cfg.pipeline.historical.timeframes[0]
        symbol    = symbols[0]
        df        = storage.load_ohlcv(symbol=symbol, timeframe=tf)

        if df is None or df.empty:
            return AssetCheckResult(
                passed      = True,
                description = "Sin datos — skip",
            )

        gaps      = scan_gaps(df, tf)
        high_gaps = [g for g in gaps if g.severity == "high"]
        passed    = len(high_gaps) == 0

        return AssetCheckResult(
            passed      = passed,
            description = (
                "Sin gaps HIGH"
                if passed
                else f"{len(high_gaps)} gap(s) HIGH detectados en {symbol}/{tf}"
            ),
            severity    = AssetCheckSeverity.WARN,
            metadata    = {
                "total_gaps": len(gaps),
                "high_gaps":  len(high_gaps),
                "symbol":     symbol,
                "timeframe":  tf,
            },
        )

    return [_row_count_check, _schema_check, _freshness_check, _gap_check]


# ==============================================================================
# Checks concretos — generados por factory (OCP)
# ==============================================================================

BRONZE_CHECKS_KUCOIN_SPOT        = make_bronze_checks("kucoin",        "spot")
BRONZE_CHECKS_BYBIT_SPOT         = make_bronze_checks("bybit",         "spot")
BRONZE_CHECKS_KUCOINFUTURES_SWAP = make_bronze_checks("kucoinfutures", "futures")

# Lista exportada para Definitions (SSOT)
ALL_ASSET_CHECKS = [
    *BRONZE_CHECKS_KUCOIN_SPOT,
    *BRONZE_CHECKS_BYBIT_SPOT,
    *BRONZE_CHECKS_KUCOINFUTURES_SWAP,
]
