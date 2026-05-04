# -*- coding: utf-8 -*-
"""
dagster_assets/bronze_ohlcv.py
================================

Asset de ingesta OHLCV: exchange → Bronze → Silver.

Diseño
------
Un asset por (exchange, market_type) — Dagster los ve como entidades
de datos independientes con linaje trazable.

Delegación (DIP)
----------------
Este asset NO construye CCXTAdapter ni OHLCVPipeline directamente.
Delega al PipelineOrchestrator (application layer) vía PipelineRequest.

  asset → PipelineRequest → PipelineOrchestrator → OHLCVPipeline

El asset solo conoce:
  - RuntimeContext (para extraer config del exchange)
  - PipelineRequest (DTO de entrada al caso de uso)
  - PipelineOrchestrator (único punto de construcción)

Dagster reemplaza
-----------------
  ExchangeTasks.spot / futures → asset separado por market_type
  _PIPELINE_SEMAPHORE          → concurrency tag en el asset
  _StageResult merge           → asset materialization status

SafeOps
-------
- Si el pipeline falla completamente → raise → Dagster marca FAILED.
- Si falla parcialmente (pairs_ok > 0) → materializa con advertencia.

Principios: SRP · OCP · DIP · SafeOps · SSOT
"""
from __future__ import annotations

import asyncio

from dagster import Output, asset

from infrastructure.dagster.resources import OCMResource
from market_data.application import PipelineOrchestrator, PipelineRequest


# ==============================================================================
# Factory: genera un asset por (exchange_name, market_type)
# ==============================================================================

def make_bronze_ohlcv_asset(exchange_name: str, market_type: str):
    """
    Factory OCP: genera asset Dagster para (exchange, market_type).

    Añadir exchange nuevo = llamar esta factory — sin tocar código existente.
    """
    asset_name = f"bronze_ohlcv_{exchange_name}_{market_type}"

    @asset(
        name        = asset_name,
        group_name  = "bronze",
        description = (
            f"OHLCV ingesta {market_type} desde {exchange_name}. "
            f"PipelineOrchestrator → OHLCVPipeline → Bronze → Silver."
        ),
        tags        = {
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
        1. Extraer config del exchange desde RuntimeContext.
        2. Armar PipelineRequest con todos los parámetros de runtime.
        3. Delegar al PipelineOrchestrator — no sabe qué pipeline concreto.
        4. Retornar Output con metadata de linaje.
        """
        runtime_ctx = ocm.runtime_context
        app_cfg     = runtime_ctx.app_config
        exc_cfg     = app_cfg.get_exchange(exchange_name)

        # Fail-Fast: exchange no configurado → falla antes de conectar
        if exc_cfg is None:
            raise ValueError(
                f"Exchange '{exchange_name}' no encontrado en AppConfig. "
                f"Exchanges activos: {app_cfg.exchange_names}"
            )

        symbols = (
            exc_cfg.markets.spot_symbols
            if market_type == "spot"
            else exc_cfg.markets.futures_symbols
        )

        if not symbols:
            context.log.warning(
                f"No hay símbolos para {exchange_name}/{market_type} — skip"
            )
            yield Output(
                value    = {"rows": 0, "pairs": 0, "status": "SKIPPED"},
                metadata = {"rows_written": 0, "pairs_ok": 0, "status": "SKIPPED"},
            )
            return

        context.log.info(
            f"Iniciando ingesta | exchange={exchange_name} "
            f"market={market_type} symbols={symbols}"
        )

        request = PipelineRequest(
            exchange           = exchange_name,
            market_type        = market_type,
            pipeline           = "ohlcv",
            mode               = "incremental",
            credentials        = exc_cfg.ccxt_credentials(),
            resilience         = exc_cfg.resilience,
            symbols            = symbols,
            timeframes         = app_cfg.pipeline.historical.timeframes,
            start_date         = app_cfg.pipeline.historical.start_date,
            auto_lookback_days = app_cfg.pipeline.historical.auto_lookback_days,
            run_id             = runtime_ctx.run_id,
            dry_run            = app_cfg.safety.dry_run,
        )

        orchestrator = PipelineOrchestrator()
        summary      = asyncio.run(orchestrator.run(request))

        if summary is None:
            raise RuntimeError(
                f"PipelineOrchestrator retornó None — ver logs | "
                f"exchange={exchange_name} market={market_type}"
            )

        metadata = {
            "rows_written": summary.total_rows,
            "pairs_ok":     summary.ok,
            "pairs_failed": summary.failed,
            "duration_ms":  summary.duration_ms,
            "status":       summary.status,
            "exchange":     exchange_name,
            "market_type":  market_type,
            "run_id":       runtime_ctx.run_id,
        }
        context.log.info(f"Ingesta completada | {metadata}")

        if summary.status == "failed":
            raise RuntimeError(
                f"Pipeline falló completamente | "
                f"exchange={exchange_name} market={market_type} "
                f"failed={summary.failed}"
            )

        yield Output(
            value    = metadata,
            metadata = {k: str(v) for k, v in metadata.items()},
        )

    return _bronze_ohlcv_asset


# ==============================================================================
# Assets concretos — generados por factory (OCP)
# Añadir exchange: una línea aquí + una en dagster_defs.py.
# ==============================================================================

bronze_ohlcv_kucoin_spot        = make_bronze_ohlcv_asset("kucoin",        "spot")
bronze_ohlcv_bybit_spot         = make_bronze_ohlcv_asset("bybit",         "spot")
bronze_ohlcv_kucoinfutures_swap = make_bronze_ohlcv_asset("kucoinfutures", "futures")

BRONZE_OHLCV_ASSETS = [
    bronze_ohlcv_kucoin_spot,
    bronze_ohlcv_bybit_spot,
    bronze_ohlcv_kucoinfutures_swap,
]
