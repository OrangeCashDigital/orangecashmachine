# -*- coding: utf-8 -*-
"""
dagster_assets/repair_ohlcv.py
================================

Asset de reparación de gaps OHLCV en Silver.

Dagster reemplaza
-----------------
  Nodo 3 del grafo batch_flow (repair con PARTIAL_SUCCESS(spot))
  → dep explícita sobre bronze_ohlcv_{exchange}_spot.

  La condición PARTIAL_SUCCESS se modela con skip_on_run_failure=False
  + lógica de check en el asset (si bronze falló completamente, skip).

Diseño
------
dep = bronze_ohlcv_{exchange}_{market_type}
Si el asset upstream fue SKIPPED o FAILED_COMPLETELY → skip silencioso.
Si tuvo al menos 1 par OK → ejecutar repair.

Esto es exactamente la semántica PARTIAL_SUCCESS del flow original,
ahora declarativa y visible en el DAG de Dagster.

Principios: DIP · OCP · SSOT · SafeOps · dependency-satisfaction
"""
from __future__ import annotations

import asyncio

from dagster import (
    AssetIn,
    Output,
    asset,
)

from dagster_assets.resources import OCMResource


def make_repair_ohlcv_asset(exchange_name: str, market_type: str = "spot"):
    """
    Factory: genera asset de repair para (exchange, market_type).

    La dep explícita sobre bronze garantiza que Dagster construye
    el linaje correcto: bronze → repair en el asset graph.
    """
    bronze_key = f"bronze_ohlcv_{exchange_name}_{market_type}"
    asset_name = f"repair_ohlcv_{exchange_name}_{market_type}"

    @asset(
        name        = asset_name,
        group_name  = "silver",
        ins         = {
            # Dep declarativa — reemplaza la condición PARTIAL_SUCCESS manual
            "upstream_bronze": AssetIn(key=bronze_key),
        },
        description = (
            f"Repair de gaps OHLCV en Silver para {exchange_name}/{market_type}. "
            f"Depende de bronze_ohlcv_{exchange_name}_{market_type} "
            f"(semántica PARTIAL_SUCCESS)."
        ),
        tags        = {
            # SSOT: key debe coincidir exactamente con pools.repair_gaps
            # en dagster.yaml. run_limit=1 — repair es secuencial por diseño.
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
        """
        Materializa repair_ohlcv_{exchange}_{market_type}.

        upstream_bronze es el Output del asset bronze — contiene
        rows_written, pairs_ok, status. Si pairs_ok == 0 → skip.
        """
        from market_data.adapters.exchange import CCXTAdapter
        from market_data.processing.pipelines.ohlcv_pipeline import OHLCVPipeline

        # PARTIAL_SUCCESS declarativo: si bronze no escribió nada → skip
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
            raise ValueError(f"Exchange '{exchange_name}' no encontrado en AppConfig")

        symbols    = (
            exc_cfg.markets.spot_symbols
            if market_type == "spot"
            else exc_cfg.markets.futures_symbols
        )
        timeframes = app_cfg.pipeline.historical.timeframes
        start_date = app_cfg.pipeline.historical.start_date

        context.log.info(
            f"Repair iniciando | exchange={exchange_name} "
            f"market={market_type} symbols={symbols}"
        )

        adapter = CCXTAdapter(
            exchange_id  = exchange_name,
            market_type  = market_type,
            credentials  = exc_cfg.ccxt_credentials(),
            resilience   = exc_cfg.resilience,
        )

        try:
            pipeline = OHLCVPipeline(
                symbols         = symbols,
                timeframes      = timeframes,
                start_date      = start_date,
                exchange_client = adapter,
                market_type     = market_type,
                dry_run         = app_cfg.safety.dry_run,
            )
            summary = asyncio.run(pipeline.run(mode="repair"))
        finally:
            asyncio.run(adapter.close())

        metadata = {
            "rows_healed":   summary.total_rows,
            "pairs_ok":      summary.ok,
            "pairs_failed":  summary.failed,
            "duration_ms":   summary.duration_ms,
            "status":        summary.status,
            "exchange":      exchange_name,
            "market_type":   market_type,
            "run_id":        runtime_ctx.run_id,
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
