# -*- coding: utf-8 -*-
"""
market_data/factories/pipeline_factory.py
==========================================

ConcretePipelineFactory — composition root de pipelines.

Responsabilidad única
---------------------
Instanciar el grafo completo de dependencias concretas para cada
tipo de pipeline y devolverlo como PipelineTriggerPort.

Licencia arquitectónica (BC-28)
--------------------------------
Este módulo PUEDE importar desde todas las capas de market_data:
domain, ports, application, adapters, infrastructure.
Es el único módulo con esa licencia — el precio es que nadie
importa desde aquí excepto el entrypoint / Dagster assets.

Principios: DIP · SRP · KISS · SafeOps
"""
from __future__ import annotations

from typing import Any


class ConcretePipelineFactory:
    """
    Implementación concreta de PipelineFactoryPort.

    Todos los imports concretos ocurren lazy dentro de `build()`
    para evitar coste de inicialización en import time y mantener
    el feedback de errores near-fail-fast en construcción, no en
    import.
    """

    def build(self, request: Any) -> Any:  # PipelineRequest → PipelineTriggerPort
        """
        Enruta la construcción según request.pipeline_type.

        Raises
        ------
        ValueError
            Si request.pipeline_type no está registrado.
        """
        dispatch = {
            "ohlcv":        self._build_ohlcv,
            "trades":       self._build_trades,
            "derivatives":  self._build_derivatives,
        }
        builder = dispatch.get(request.pipeline_type)
        if builder is None:
            raise ValueError(
                f"PipelineType desconocido: {request.pipeline_type!r}. "
                f"Registrados: {list(dispatch)}"
            )
        return builder(request)

    # ------------------------------------------------------------------
    # Builders concretos
    # ------------------------------------------------------------------

    def _build_ohlcv(self, request: Any) -> Any:
        """Cabla OHLCVPipeline con CCXTAdapter y sus dependencias."""
        from market_data.adapters.outbound.exchange.ccxt_adapter import CCXTAdapter
        from market_data.application.pipelines.ohlcv_pipeline import OHLCVPipeline

        adapter_kwargs: dict[str, Any] = {
            "exchange_id": request.exchange,
            "market_type": request.market_type,
        }
        if request.credentials is not None:
            adapter_kwargs["credentials"] = request.credentials
        if request.resilience is not None:
            adapter_kwargs["resilience"] = request.resilience

        pipeline_kwargs: dict[str, Any] = {
            "exchange_client": CCXTAdapter(**adapter_kwargs),
            "market_type":     request.market_type,
            "dry_run":         request.dry_run,
        }
        if request.symbols is not None:
            pipeline_kwargs["symbols"] = request.symbols
        if request.timeframes is not None:
            pipeline_kwargs["timeframes"] = request.timeframes
        if request.start_date is not None:
            pipeline_kwargs["start_date"] = request.start_date
        if request.auto_lookback_days is not None:
            pipeline_kwargs["auto_lookback_days"] = request.auto_lookback_days

        from market_data.infrastructure.observability.metrics_adapter import PrometheusPipelineMetrics
        from market_data.infrastructure.kafka.producer import KafkaProducer
        from market_data.infrastructure.storage.iceberg.iceberg_storage import IcebergStorage

        pipeline_kwargs["storage"]  = IcebergStorage(
            exchange    = request.exchange,
            market_type = request.market_type,
            dry_run     = request.dry_run,
        )
        pipeline_kwargs["producer"] = KafkaProducer()
        pipeline_kwargs["metrics"]  = PrometheusPipelineMetrics()
        return OHLCVPipeline(**pipeline_kwargs)

    def _build_trades(self, request: Any) -> Any:
        """Cabla TradesPipeline con CCXTAdapter + TradesFetcher + TradesStorage."""
        from market_data.adapters.outbound.exchange.ccxt_adapter import CCXTAdapter
        from market_data.adapters.inbound.rest.trades_fetcher import TradesFetcher
        from market_data.infrastructure.storage.silver.trades_storage import TradesStorage
        from market_data.application.pipelines.trades_pipeline import TradesPipeline

        adapter_kwargs: dict[str, Any] = {
            "exchange_id": request.exchange,
            "market_type": request.market_type,
        }
        if request.credentials is not None:
            adapter_kwargs["credentials"] = request.credentials
        if request.resilience is not None:
            adapter_kwargs["resilience"] = request.resilience

        exchange_client = CCXTAdapter(**adapter_kwargs)

        return TradesPipeline(
            symbols         = request.symbols or [],
            exchange_client = exchange_client,
            fetcher         = TradesFetcher(exchange_client=exchange_client),
            storage         = TradesStorage(exchange=request.exchange),
            market_type     = request.market_type,
            dry_run         = request.dry_run,
        )

    def _build_derivatives(self, request: Any) -> Any:
        """Cabla DerivativesPipeline con CCXTAdapter + fetchers + DerivativesStorage."""
        from market_data.adapters.outbound.exchange.ccxt_adapter import CCXTAdapter
        from market_data.adapters.inbound.rest.derivatives_fetcher import (
            FundingRateFetcher,
            OpenInterestFetcher,
        )
        from market_data.infrastructure.storage.silver.derivatives_storage import DerivativesStorage
        from market_data.application.pipelines.derivatives_pipeline import DerivativesPipeline

        adapter_kwargs: dict[str, Any] = {
            "exchange_id": request.exchange,
            "market_type": request.market_type,
        }
        if request.credentials is not None:
            adapter_kwargs["credentials"] = request.credentials
        if request.resilience is not None:
            adapter_kwargs["resilience"] = request.resilience

        exchange_client = CCXTAdapter(**adapter_kwargs)

        return DerivativesPipeline(
            symbols              = request.symbols or [],
            exchange_client      = exchange_client,
            funding_rate_fetcher = FundingRateFetcher(exchange_client=exchange_client),
            open_interest_fetcher= OpenInterestFetcher(exchange_client=exchange_client),
            storage              = DerivativesStorage(exchange=request.exchange),
            market_type          = request.market_type,
            dry_run              = request.dry_run,
        )
