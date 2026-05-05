# -*- coding: utf-8 -*-
"""
dagster_assets/resample_ohlcv.py
==================================

Asset de resampling OHLCV: 1m Silver → 5m/15m/1h/4h/1d Silver.

Responsabilidad (Adapter)
-------------------------
Adaptar el trigger de Dagster al caso de uso ResampleUseCase.
Este archivo NO contiene lógica de orquestación — solo traduce
el contexto Dagster a un ResampleRequest y delega.

La lógica de qué símbolos, qué targets, cómo construir storage
y cómo ejecutar el pipeline está en ResampleUseCase (SSOT).

Dagster reemplaza resample_flow de Prefect → asset con dep sobre repair.
El linaje 1m → 5m/15m/1h/4h/1d es ahora visible en el asset graph.

Principios: SRP (adapter puro) · DIP · OCP · SSOT
"""
from __future__ import annotations

from dagster import AssetIn, Output, asset

from dagster_assets.resources import OCMResource
from market_data.application import ResampleUseCase, ResampleRequest
from market_data.adapters.outbound.storage.iceberg_factory import IcebergStorageFactory

# Instancia del use case — singleton a nivel de módulo (DIP · SafeOps)
# La factory se inyecta aquí: composition root del adapter.
_resample_use_case = ResampleUseCase(
    storage_factory = IcebergStorageFactory(),
)


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
        tags        = {
            # SSOT: key debe coincidir con pools.silver_resample en dagster.yaml.
            # CPU-bound local — run_limit=4 permite más paralelismo que bronze.
            "dagster/concurrency_key": "silver_resample",
            "exchange":               exchange_name,
            "market_type":            market_type,
        },
    )
    def _resample_asset(
        context,
        ocm:            OCMResource,
        upstream_repair: dict,
    ):
        """
        Materializa silver_ohlcv_resampled_{exchange}_{market_type}.

        Adapter puro: construye ResampleRequest y delega a ResampleUseCase.
        Toda la lógica de orquestación vive en el use case (SRP).
        """
        runtime_ctx = ocm.runtime_context
        app_cfg     = runtime_ctx.app_config

        request = ResampleRequest(
            exchange    = exchange_name,
            market_type = market_type,
            app_config  = app_cfg,
            dry_run     = app_cfg.safety.dry_run,
            run_id      = runtime_ctx.run_id,
        )

        result = _resample_use_case.execute(request)

        if result.status == "skipped":
            context.log.warning(
                f"Resample skipped — sin símbolos | "
                f"exchange={exchange_name} market={market_type}"
            )
            yield Output(
                value    = {"rows": 0, "status": "SKIPPED"},
                metadata = {"status": "SKIPPED"},
            )
            return

        if not result.succeeded:
            raise RuntimeError(
                f"ResampleUseCase failed | "
                f"exchange={exchange_name} market={market_type} "
                f"error={result.error}"
            )

        context.log.info(
            f"Resample completado | exchange={exchange_name} "
            f"market={market_type} rows={result.rows} status={result.status}"
        )

        metadata = {
            "rows_resampled": result.rows,
            "source_tf":      result.source_tf,
            "targets":        str(result.targets),
            "exchange":       exchange_name,
            "market_type":    market_type,
            "status":         result.status,
            "run_id":         str(result.run_id),
        }
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
