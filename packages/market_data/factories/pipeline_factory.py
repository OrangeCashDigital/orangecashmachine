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
        Enruta la construcción según request.pipeline.

        Raises
        ------
        ValueError
            Si request.pipeline no está registrado.
        """
        dispatch = {
            "ohlcv":       self._build_ohlcv,
            "trades":      self._build_trades,
            "derivatives": self._build_derivatives,
        }
        # FIX C-01: el campo del DTO es `pipeline`, no `pipeline_type`.
        # PipelineRequest.__post_init__ valida los valores aceptados — SSOT.
        builder = dispatch.get(request.pipeline)
        if builder is None:
            raise ValueError(
                f"PipelineType desconocido: {request.pipeline!r}. "
                f"Registrados: {list(dispatch)}"
            )
        return builder(request)

    # ------------------------------------------------------------------
    # Builders concretos — SRP: uno por tipo de pipeline
    # ------------------------------------------------------------------

    def _build_ohlcv(self, request: Any) -> Any:
        """Cabla OHLCVPipeline con CCXTAdapter y sus dependencias concretas."""
        from market_data.adapters.outbound.exchange.ccxt_adapter import CCXTAdapter
        from market_data.adapters.inbound.rest.ohlcv_fetcher import HistoricalFetcherAsync
        from market_data.application.pipelines.ohlcv_pipeline import OHLCVPipeline
        from market_data.application.use_cases.ohlcv_transformer import OHLCVTransformer
        from market_data.infrastructure.observability.metrics_adapter import PrometheusPipelineMetrics
        from ocm.runtime.state import build_cursor_store_from_env, InMemoryCursorStore

        adapter_kwargs: dict[str, Any] = {
            "exchange_id": request.exchange,
            "market_type": request.market_type,
        }
        if request.credentials is not None:
            adapter_kwargs["credentials"] = request.credentials
        if request.resilience is not None:
            adapter_kwargs["resilience"] = request.resilience

        exchange_client = CCXTAdapter(**adapter_kwargs)

        try:
            cursor = build_cursor_store_from_env()
        except Exception:
            cursor = InMemoryCursorStore()

        # Kappa: el fetcher no toca Iceberg — cursor Redis es SSOT del offset
        # del productor. Iceberg solo lo escribe el consumer downstream (BronzeWriter).
        fetcher = HistoricalFetcherAsync(
            storage            = None,
            transformer        = OHLCVTransformer(),
            exchange_client    = exchange_client,
            cursor_store       = cursor,
            backfill_mode      = True,
            market_type        = request.market_type,
            config_start_date  = request.start_date or "auto",
            auto_lookback_days = request.auto_lookback_days or 3650,
        )

        # Fail-Fast: OHLCVPipeline.__init__ valida que symbols/timeframes/start_date
        # no estén vacíos. Si el request no los provee, la factory debe proveer
        # un fallback explícito — no silencioso — para que el error sea claro (C-03).
        if not request.symbols:
            raise ValueError(
                f"PipelineRequest.symbols es obligatorio para pipeline='ohlcv'. "
                f"Request recibido: {request}"
            )
        if not request.timeframes:
            raise ValueError(
                f"PipelineRequest.timeframes es obligatorio para pipeline='ohlcv'. "
                f"Request recibido: {request}"
            )
        if not request.start_date:
            raise ValueError(
                f"PipelineRequest.start_date es obligatorio para pipeline='ohlcv'. "
                f"Request recibido: {request}"
            )
        pipeline_kwargs: dict[str, Any] = {
            "exchange_client":    exchange_client,
            "fetcher":            fetcher,
            "metrics":            PrometheusPipelineMetrics(),
            "market_type":        request.market_type,
            "dry_run":            request.dry_run,
            "symbols":            request.symbols,
            "timeframes":         request.timeframes,
            "start_date":         request.start_date,
            "auto_lookback_days": request.auto_lookback_days or 3650,
        }

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
            symbols               = request.symbols or [],
            exchange_client       = exchange_client,
            funding_rate_fetcher  = FundingRateFetcher(exchange_client=exchange_client),
            open_interest_fetcher = OpenInterestFetcher(exchange_client=exchange_client),
            storage               = DerivativesStorage(exchange=request.exchange),
            market_type           = request.market_type,
            dry_run               = request.dry_run,
        )
