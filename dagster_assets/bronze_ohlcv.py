# -*- coding: utf-8 -*-
"""
dagster_assets/bronze_ohlcv.py
================================

Asset de ingesta OHLCV: exchange → Bronze → Silver.

Fase 1: envuelve OHLCVPipeline sin modificarlo.
Fase 2: declara linaje explícito exchange → bronze_ohlcv_{exchange}_{market}.

Diseño
------
Un asset por (exchange, market_type) — Dagster los ve como entidades
de datos independientes con linaje trazable.

Dagster reemplaza
-----------------
  ExchangeTasks.spot / futures    → asset separado por market_type
  _PIPELINE_SEMAPHORE             → concurrency tag en el asset
  _StageResult merge              → asset materialization status

SafeOps
-------
- Si OHLCVPipeline falla completamente → raise → Dagster marca FAILED.
- Si falla parcialmente (PARTIAL) → emite advertencia, materializa igual.
  El linaje Dagster registra rows_ok / rows_failed como metadata.

Principios: SRP · OCP · DIP · SafeOps · SSOT
"""
from __future__ import annotations

import asyncio

from dagster import (
    Output,
    asset,
)

from dagster_assets.resources import OCMResource
from market_data.adapters.exchange import CCXTAdapter
from market_data.processing.pipelines.ohlcv_pipeline import OHLCVPipeline


# ==============================================================================
# Factory: genera un asset por (exchange_name, market_type)
# ==============================================================================

def make_bronze_ohlcv_asset(exchange_name: str, market_type: str):
    """
    Factory OCP: genera asset Dagster para (exchange, market_type).

    Añadir exchange nuevo = llamar esta factory — sin tocar código existente.

    Args:
        exchange_name: Nombre canónico del exchange (e.g. "kucoin").
        market_type:   "spot" | "futures" | "linear" | "inverse".

    Returns:
        Función decorada con @asset lista para incluir en Definitions.
    """
    asset_name = f"bronze_ohlcv_{exchange_name}_{market_type}"

    @asset(
        name        = asset_name,
        group_name  = "bronze",
        description = (
            f"OHLCV ingesta {market_type} desde {exchange_name}. "
            f"OHLCVPipeline → BronzeStorage → IcebergStorage (silver.ohlcv)."
        ),
        # Concurrencia declarativa — reemplaza _PIPELINE_SEMAPHORE manual.
        # Dagster limita cuántos de estos assets corren simultáneamente.
        tags        = {
            # SSOT: key debe coincidir exactamente con pools.bronze_ingestion
            # en dagster.yaml — de lo contrario el pool no tiene efecto.
            "dagster/concurrency_key": "bronze_ingestion",
            "exchange":               exchange_name,
            "market_type":            market_type,
        },
        metadata    = {
            "exchange":    exchange_name,
            "market_type": market_type,
            "layer":       "bronze",
        },
    )
    def _bronze_ohlcv_asset(
        context,
        ocm: OCMResource,
    ):
        """
        Materializa bronze_ohlcv_{exchange}_{market_type}.

        Flujo
        -----
        1. Construir RuntimeContext desde OCMResource (DIP).
        2. Obtener ExchangeConfig para este exchange.
        3. Construir CCXTAdapter y OHLCVPipeline (igual que batch_flow).
        4. Ejecutar pipeline.run(mode="incremental").
        5. Retornar Output con metadata de linaje.
        """

        runtime_ctx = ocm.runtime_context
        app_cfg     = runtime_ctx.app_config
        exc_cfg     = app_cfg.get_exchange(exchange_name)

        # Fail-Fast: si el exchange no está configurado, el asset falla
        # antes de conectar — error visible en Dagster UI inmediatamente.
        if exc_cfg is None:
            raise ValueError(
                f"Exchange '{exchange_name}' no encontrado en AppConfig. "
                f"Exchanges activos: {app_cfg.exchange_names}"
            )

        # Seleccionar símbolos según market_type
        if market_type == "spot":
            symbols = exc_cfg.markets.spot_symbols
        else:
            symbols = exc_cfg.markets.futures_symbols

        if not symbols:
            context.log.warning(
                f"No hay símbolos para {exchange_name}/{market_type} — skip"
            )
            yield Output(
                value    = {"rows": 0, "pairs": 0, "status": "SKIPPED"},
                metadata = {"rows_written": 0, "pairs_ok": 0, "status": "SKIPPED"},
            )
            return

        timeframes = app_cfg.pipeline.historical.timeframes
        start_date = app_cfg.pipeline.historical.start_date

        context.log.info(
            f"Iniciando ingesta | exchange={exchange_name} "
            f"market={market_type} symbols={symbols} timeframes={timeframes}"
        )

        # Construir adapter — lifecycle completo en este asset
        adapter = CCXTAdapter(
            exchange_id  = exchange_name,
            market_type  = market_type,
            credentials  = exc_cfg.ccxt_credentials(),
            resilience   = exc_cfg.resilience,
        )

        try:
            pipeline = OHLCVPipeline(
                symbols          = symbols,
                timeframes       = timeframes,
                start_date       = start_date,
                exchange_client  = adapter,
                market_type      = market_type,
                dry_run          = app_cfg.safety.dry_run,
                auto_lookback_days = app_cfg.pipeline.historical.auto_lookback_days,
            )

            summary = asyncio.run(pipeline.run(mode="incremental"))

        finally:
            asyncio.run(adapter.close())

        # Metadata de linaje — visible en Dagster asset catalog
        metadata = {
            "rows_written":  summary.total_rows,
            "pairs_ok":      summary.ok,
            "pairs_failed":  summary.failed,
            "duration_ms":   summary.duration_ms,
            "status":        summary.status,
            "exchange":      exchange_name,
            "market_type":   market_type,
            "run_id":        runtime_ctx.run_id,
        }
        context.log.info(
            f"Ingesta completada | {metadata}"
        )

        # Fail-Fast: si todos los pares fallaron, el asset falla.
        # Dagster marca FAILED y no materializa downstream assets.
        if summary.status == "failed":
            raise RuntimeError(
                f"OHLCVPipeline failed completely | "
                f"exchange={exchange_name} market={market_type} "
                f"failed={summary.failed}"
            )

        yield Output(
            value    = metadata,
            metadata = {k: str(v) for k, v in metadata.items()},
        )

    return _bronze_ohlcv_asset


# ==============================================================================
# Assets concretos — generados por la factory (OCP)
# ==============================================================================
# Para añadir un exchange: agregar una línea aquí y en dagster_defs.py.
# Cero modificaciones al código existente.

bronze_ohlcv_kucoin_spot         = make_bronze_ohlcv_asset("kucoin",        "spot")
bronze_ohlcv_bybit_spot          = make_bronze_ohlcv_asset("bybit",         "spot")
bronze_ohlcv_kucoinfutures_swap  = make_bronze_ohlcv_asset("kucoinfutures", "futures")

# Lista exportada para Definitions (SSOT)
BRONZE_OHLCV_ASSETS = [
    bronze_ohlcv_kucoin_spot,
    bronze_ohlcv_bybit_spot,
    bronze_ohlcv_kucoinfutures_swap,
]
