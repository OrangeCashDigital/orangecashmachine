# -*- coding: utf-8 -*-
"""
infrastructure/dagster/assets/asset_checks.py
=============================================

Checks de calidad sobre assets OHLCV materializados.

Checks por asset (exchange, market_type)
-----------------------------------------
  row_count   — blocking=True  — 0 filas = fallo crítico
  schema      — blocking=True  — columnas OHLCV requeridas presentes
  freshness   — blocking=False — dato < 24h (WARN, no bloquea)
  gap_severity— blocking=False — gaps HIGH detectados (WARN)

DIP
---
Los checks reciben OHLCVStorage via OCMResource (composition root).
Ningún check instancia IcebergStorageFactory directamente.
El storage_factory se registra en OCMResource y se inyecta aquí.

Principios: DIP · OCP · SRP · SafeOps · Fail-Fast donde importa
"""
from __future__ import annotations

from typing import List

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetKey,
    asset_check,
)

from infrastructure.dagster.resources import OCMResource
from market_data.quality.pipeline import scan_gaps

_REQUIRED_COLS = frozenset({
    "timestamp", "open", "high", "low", "close", "volume",
})


def make_bronze_checks(exchange_name: str, market_type: str) -> List:
    """
    Factory OCP: genera los 4 checks para (exchange_name, market_type).

    Añadir exchange = llamar esta factory — sin tocar código existente.
    """
    asset_key = AssetKey(f"bronze_ohlcv_{exchange_name}_{market_type}")

    # ------------------------------------------------------------------
    # Helper: obtiene storage via OCMResource (DIP — sin adaptador directo)
    # ------------------------------------------------------------------
    def _storage(ocm: OCMResource):
        """
        Retorna OHLCVStorage para este exchange/market_type.

        DIP: OCMResource es el composition root — los checks no conocen
        IcebergStorageFactory ni IcebergStorage directamente.
        """
        return ocm.get_storage(
            exchange    = exchange_name,
            market_type = market_type,
        )

    # ------------------------------------------------------------------
    # 1. ROW COUNT
    #    blocking=True — 0 filas implica pipeline roto aguas arriba.
    # ------------------------------------------------------------------
    @asset_check(
        asset       = asset_key,
        name        = f"row_count_{exchange_name}_{market_type}",
        description = "Verifica que el asset tenga al menos una fila.",
        blocking    = True,
    )
    def _row_count_check(context, ocm: OCMResource) -> AssetCheckResult:
        runtime_ctx = ocm.build_runtime_context()
        app_cfg     = runtime_ctx.app_config
        exc_cfg     = app_cfg.get_exchange(exchange_name)

        if exc_cfg is None:
            return AssetCheckResult(
                passed      = False,
                description = "Exchange no encontrado en AppConfig",
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
                description = "Sin símbolos configurados — skip",
            )

        storage = _storage(ocm)
        tf      = app_cfg.pipeline.historical.timeframes[0]
        df      = storage.load_ohlcv(symbol=symbols[0], timeframe=tf)
        count   = 0 if (df is None or df.empty) else len(df)
        passed  = count > 0

        return AssetCheckResult(
            passed      = passed,
            description = f"{count} filas presentes",
            **({"severity": AssetCheckSeverity.ERROR} if not passed else {}),
            metadata    = {"row_count": count},
        )

    # ------------------------------------------------------------------
    # 2. SCHEMA
    #    blocking=True — columnas faltantes rompen pipeline downstream.
    # ------------------------------------------------------------------
    @asset_check(
        asset       = asset_key,
        name        = f"schema_{exchange_name}_{market_type}",
        description = "Verifica que las columnas OHLCV requeridas existan.",
        blocking    = True,
    )
    def _schema_check(context, ocm: OCMResource) -> AssetCheckResult:
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
            return AssetCheckResult(passed=True, description="Sin símbolos — skip")

        storage = _storage(ocm)
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
            **({"severity": AssetCheckSeverity.ERROR} if not passed else {}),
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
    def _freshness_check(context, ocm: OCMResource) -> AssetCheckResult:
        runtime_ctx = ocm.build_runtime_context()
        app_cfg     = runtime_ctx.app_config
        exc_cfg     = app_cfg.get_exchange(exchange_name)

        if exc_cfg is None:
            return AssetCheckResult(passed=True, description="Exchange no encontrado — skip")

        symbols = (
            exc_cfg.markets.spot_symbols
            if market_type == "spot"
            else exc_cfg.markets.futures_symbols
        )
        if not symbols:
            return AssetCheckResult(passed=True, description="Sin símbolos — skip")

        # get_last_timestamp requiere (symbol, timeframe) — firma correcta del port
        storage = _storage(ocm)
        tf      = app_cfg.pipeline.historical.timeframes[0]
        last_ts = storage.get_last_timestamp(
            symbol    = symbols[0],
            timeframe = tf,
        )

        if last_ts is None:
            return AssetCheckResult(passed=True, description="Sin datos todavía — skip")

        import pandas as pd
        now_utc   = pd.Timestamp.utcnow()
        age_hours = (now_utc - last_ts).total_seconds() / 3600
        passed    = age_hours < 24.0

        return AssetCheckResult(
            passed      = passed,
            description = (
                f"Dato fresco ({age_hours:.1f}h)"
                if passed
                else f"Dato desactualizado: {age_hours:.1f}h sin actualizar"
            ),
            **({"severity": AssetCheckSeverity.WARN} if not passed else {}),
            metadata    = {"age_hours": round(age_hours, 2)},
        )

    # ------------------------------------------------------------------
    # 4. GAP SEVERITY
    #    blocking=False — gaps HIGH son WARN, no bloquean la cadena.
    # ------------------------------------------------------------------
    @asset_check(
        asset       = asset_key,
        name        = f"gap_severity_{exchange_name}_{market_type}",
        description = "Detecta gaps de severidad HIGH en la serie OHLCV.",
    )
    def _gap_check(context, ocm: OCMResource) -> AssetCheckResult:
        runtime_ctx = ocm.build_runtime_context()
        app_cfg     = runtime_ctx.app_config
        exc_cfg     = app_cfg.get_exchange(exchange_name)

        if exc_cfg is None:
            return AssetCheckResult(passed=True, description="Exchange no encontrado — skip")

        symbols = (
            exc_cfg.markets.spot_symbols
            if market_type == "spot"
            else exc_cfg.markets.futures_symbols
        )
        if not symbols:
            return AssetCheckResult(passed=True, description="Sin símbolos — skip")

        storage   = _storage(ocm)
        tf        = app_cfg.pipeline.historical.timeframes[0]
        symbol    = symbols[0]
        df        = storage.load_ohlcv(symbol=symbol, timeframe=tf)

        if df is None or df.empty:
            return AssetCheckResult(passed=True, description="Sin datos — skip")

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
            **({"severity": AssetCheckSeverity.WARN} if not passed else {}),
            metadata    = {
                "total_gaps": len(gaps),
                "high_gaps":  len(high_gaps),
                "symbol":     symbol,
                "timeframe":  tf,
            },
        )

    return [_row_count_check, _schema_check, _freshness_check, _gap_check]


BRONZE_CHECKS_KUCOIN_SPOT        = make_bronze_checks("kucoin",        "spot")
BRONZE_CHECKS_BYBIT_SPOT         = make_bronze_checks("bybit",         "spot")
BRONZE_CHECKS_KUCOINFUTURES_SWAP = make_bronze_checks("kucoinfutures", "futures")

ALL_ASSET_CHECKS = [
    *BRONZE_CHECKS_KUCOIN_SPOT,
    *BRONZE_CHECKS_BYBIT_SPOT,
    *BRONZE_CHECKS_KUCOINFUTURES_SWAP,
]
