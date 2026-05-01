# -*- coding: utf-8 -*-
"""
dagster_assets/resample_ohlcv.py
==================================

Asset de resampling OHLCV: 1m Silver → 5m/15m/1h/4h/1d Silver.

Dagster reemplaza resample_flow de Prefect → asset con dep sobre repair.
El linaje 1m → 5m/15m/1h/4h/1d es ahora visible en el asset graph.

Principios: DIP · OCP · SSOT · SafeOps
"""
from __future__ import annotations

import asyncio

from dagster import AssetIn, Output, asset

from dagster_assets.resources import OCMResource


def make_resample_ohlcv_asset(exchange_name: str, market_type: str = "spot"):
    repair_key = f"repair_ohlcv_{exchange_name}_{market_type}"
    asset_name = f"silver_ohlcv_resampled_{exchange_name}_{market_type}"

    @asset(
        name        = asset_name,
        group_name  = "silver",
        ins         = {"upstream_repair": AssetIn(key=repair_key)},
        description = (
            f"Resample 1m→[5m,15m,1h,4h,1d] para {exchange_name}/{market_type}. "
            f"Construye timeframes derivados localmente desde Silver 1m "
            f"sin llamadas extra al exchange."
        ),
        metadata    = {
            "exchange":    exchange_name,
            "market_type": market_type,
            "layer":       "silver",
            "source_tf":   "1m",
        },
    )
    def _resample_asset(
        context,
        ocm:            OCMResource,
        upstream_repair: dict,
    ):
        """
        Materializa silver_ohlcv_resampled_{exchange}_{market_type}.

        Llama ResamplePipeline igual que resample_flow, con linaje
        declarativo en Dagster (dep explícita sobre repair).
        """
        from market_data.processing.pipelines.resample_pipeline import ResamplePipeline
        from market_data.storage.iceberg.iceberg_storage import IcebergStorage

        runtime_ctx = ocm.runtime_context
        app_cfg     = runtime_ctx.app_config
        exc_cfg     = app_cfg.get_exchange(exchange_name)

        if exc_cfg is None:
            raise ValueError(f"Exchange '{exchange_name}' no encontrado en AppConfig")

        symbols   = (
            exc_cfg.markets.spot_symbols
            if market_type == "spot"
            else exc_cfg.markets.futures_symbols
        )
        targets   = app_cfg.pipeline.resample.targets
        source_tf = app_cfg.pipeline.resample.source_tf

        if not symbols:
            context.log.warning(f"No hay símbolos para {exchange_name}/{market_type} — skip")
            yield Output(
                value    = {"rows": 0, "status": "SKIPPED"},
                metadata = {"status": "SKIPPED"},
            )
            return

        context.log.info(
            f"Resample iniciando | exchange={exchange_name} "
            f"market={market_type} {source_tf}→{targets}"
        )

        storage = IcebergStorage(
            exchange     = exchange_name,
            market_type  = market_type,
            dry_run      = app_cfg.safety.dry_run,
        )

        total_rows = 0
        for symbol in symbols:
            pipeline = ResamplePipeline(
                storage   = storage,
                symbol    = symbol,
                source_tf = source_tf,
                targets   = targets,
            )
            result = pipeline.run()
            total_rows += getattr(result, "total_rows", 0) if result else 0

        metadata = {
            "rows_resampled": total_rows,
            "source_tf":      source_tf,
            "targets":        str(targets),
            "exchange":       exchange_name,
            "market_type":    market_type,
            "run_id":         runtime_ctx.run_id,
        }
        context.log.info(f"Resample completado | {metadata}")

        yield Output(
            value    = metadata,
            metadata = {k: str(v) for k, v in metadata.items()},
        )

    return _resample_asset


silver_ohlcv_resampled_kucoin_spot        = make_resample_ohlcv_asset("kucoin",        "spot")
silver_ohlcv_resampled_bybit_spot         = make_resample_ohlcv_asset("bybit",         "spot")
silver_ohlcv_resampled_kucoinfutures_swap = make_resample_ohlcv_asset("kucoinfutures", "futures")

RESAMPLE_OHLCV_ASSETS = [
    silver_ohlcv_resampled_kucoin_spot,
    silver_ohlcv_resampled_bybit_spot,
    silver_ohlcv_resampled_kucoinfutures_swap,
]
