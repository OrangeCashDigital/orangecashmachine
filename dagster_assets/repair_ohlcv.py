# -*- coding: utf-8 -*-
"""
dagster_assets/repair_ohlcv.py
================================

Asset de reparación de gaps OHLCV en Silver.

Delegación (DIP)
----------------
Este asset NO construye CCXTAdapter ni OHLCVPipeline directamente.
Delega al PipelineOrchestrator vía PipelineRequest con mode="repair".

Semántica PARTIAL_SUCCESS
-------------------------
Si upstream_bronze.pairs_ok == 0 → skip silencioso.
Si pairs_ok > 0 → ejecutar repair (hay datos que pueden tener gaps).
Esto modela exactamente el nodo 3 del grafo Prefect original,
ahora declarativo en el DAG de Dagster.

Principios: DIP · OCP · SSOT · SafeOps · dependency-satisfaction
"""
from __future__ import annotations

import asyncio

from dagster import AssetIn, Output, asset

from dagster_assets.resources import OCMResource
from market_data.application import PipelineOrchestrator, PipelineRequest


def make_repair_ohlcv_asset(exchange_name: str, market_type: str = "spot"):
    """Factory: genera asset de repair para (exchange, market_type)."""

    bronze_key = f"bronze_ohlcv_{exchange_name}_{market_type}"
    asset_name = f"repair_ohlcv_{exchange_name}_{market_type}"

    @asset(
        name        = asset_name,
        group_name  = "silver",
        ins         = {"upstream_bronze": AssetIn(key=bronze_key)},
        description = (
            f"Repair de gaps OHLCV en Silver para {exchange_name}/{market_type}. "
            f"Depende de {bronze_key} (semántica PARTIAL_SUCCESS)."
        ),
        tags        = {
            "dagster/concurrency_key": "repair_gaps",
            "exchange":               exchange_name,
            "market_type":            market_type,
        },
        metadata    = {
            "exchange":    exchange_name,
            "market_type": market_type,
            "layer":       "silver",
        },
    )
    def _repair_ohlcv_asset(
        context,
        ocm:             OCMResource,
        upstream_bronze: dict,
    ):
        # PARTIAL_SUCCESS: si bronze no escribió nada → skip
        pairs_ok = upstream_bronze.get("pairs_ok", 0)
        if pairs_ok == 0:
            context.log.warning(
                f"repair skip — bronze tuvo 0 pares exitosos | "
                f"exchange={exchange_name} market={market_type}"
            )
            yield Output(
                value    = {"rows": 0, "gaps_found": 0, "status": "SKIPPED"},
                metadata = {"status": "SKIPPED", "reason": "upstream_bronze_empty"},
            )
            return

        runtime_ctx = ocm.runtime_context
        app_cfg     = runtime_ctx.app_config
        exc_cfg     = app_cfg.get_exchange(exchange_name)

        if exc_cfg is None:
            raise ValueError(
                f"Exchange '{exchange_name}' no encontrado en AppConfig"
            )

        symbols = (
            exc_cfg.markets.spot_symbols
            if market_type == "spot"
            else exc_cfg.markets.futures_symbols
        )

        context.log.info(
            f"Repair iniciando | exchange={exchange_name} "
            f"market={market_type} symbols={symbols}"
        )

        request = PipelineRequest(
            exchange    = exchange_name,
            market_type = market_type,
            pipeline    = "ohlcv",
            mode        = "repair",
            credentials = exc_cfg.ccxt_credentials(),
            resilience  = exc_cfg.resilience,
            symbols     = symbols,
            timeframes  = app_cfg.pipeline.historical.timeframes,
            start_date  = app_cfg.pipeline.historical.start_date,
            run_id      = runtime_ctx.run_id,
            dry_run     = app_cfg.safety.dry_run,
        )

        orchestrator = PipelineOrchestrator()
        summary      = asyncio.run(orchestrator.run(request))

        if summary is None:
            raise RuntimeError(
                f"PipelineOrchestrator retornó None en repair — ver logs | "
                f"exchange={exchange_name} market={market_type}"
            )

        metadata = {
            "rows_healed":  summary.total_rows,
            "pairs_ok":     summary.ok,
            "pairs_failed": summary.failed,
            "duration_ms":  summary.duration_ms,
            "status":       summary.status,
            "exchange":     exchange_name,
            "market_type":  market_type,
            "run_id":       runtime_ctx.run_id,
        }
        context.log.info(f"Repair completado | {metadata}")

        yield Output(
            value    = metadata,
            metadata = {k: str(v) for k, v in metadata.items()},
        )

    return _repair_ohlcv_asset


repair_ohlcv_kucoin_spot        = make_repair_ohlcv_asset("kucoin",        "spot")
repair_ohlcv_bybit_spot         = make_repair_ohlcv_asset("bybit",         "spot")
repair_ohlcv_kucoinfutures_swap = make_repair_ohlcv_asset("kucoinfutures", "futures")

REPAIR_OHLCV_ASSETS = [
    repair_ohlcv_kucoin_spot,
    repair_ohlcv_bybit_spot,
    repair_ohlcv_kucoinfutures_swap,
]
