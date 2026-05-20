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

    Todos los imports concretos ocurren lazy dentro de cada builder
    para evitar coste de inicialización en import time y mantener
    el feedback de errores near-fail-fast en construcción, no en import.
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
            "ohlcv":         self._build_ohlcv,
            "trades":        self._build_trades,
            "trades_stream": self._build_trades_stream,
            "derivatives":   self._build_derivatives,
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

    def _build_kafka_publisher(self):
        """
        Construye KafkaOHLCVPublisher respetando el feature flag KAFKA_ENABLED.

        Composition Root — único lugar autorizado para instanciar infra de Kafka.
        Movido desde application/pipelines/ohlcv_pipeline.py (DIP: application
        no debe conocer infrastructure directamente).

        Retorna None si Kafka está deshabilitado o no disponible (modo degradado).
        """
        import os
        from ocm.config.env_vars import KAFKA_ENABLED as _KAFKA_ENABLED_VAR
        kafka_flag = os.environ.get(_KAFKA_ENABLED_VAR, "false").strip().lower()
        if kafka_flag not in ("1", "true", "yes"):
            return None
        try:
            from market_data.infrastructure.kafka.producer import KafkaProducerAdapter
            from market_data.infrastructure.kafka.ohlcv_publisher import KafkaOHLCVPublisher
            producer = KafkaProducerAdapter.from_env()
            return KafkaOHLCVPublisher(producer=producer)
        except Exception as exc:
            import logging
            logging.getLogger(__name__).warning(
                "KafkaOHLCVPublisher no disponible — modo degradado: %s", exc
            )
            return None

    def _build_ohlcv(self, request: Any) -> Any:
        """Cabla OHLCVPipeline con CCXTAdapter y sus dependencias concretas."""
        # Imports lazy — evitan circularidades y coste en import-time.
        # ge_checker_factory y PrometheusPipelineMetrics pertenecen a infra:
        # van aquí (lazy) para mantener la arquitectura de capas.
        from market_data.adapters.outbound.exchange.ccxt_adapter import CCXTAdapter
        from market_data.adapters.inbound.rest.ohlcv_fetcher import HistoricalFetcherAsync
        from market_data.application.pipelines.ohlcv_pipeline import OHLCVPipeline
        from market_data.application.use_cases.ohlcv_transformer import OHLCVTransformer
        from market_data.infrastructure.quality.ge_checker import ge_checker_factory
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

        # Composition root: construye QualityPipeline con registry inyectado (D-05).
        # BC-28: única licencia para importar infrastructure/ desde aquí.
        from market_data.application.quality.pipeline import QualityPipeline
        from market_data.infrastructure.quality.anomaly_registry import default_registry
        quality = QualityPipeline(registry=default_registry)
        from market_data.adapters.outbound.chunk_converter import PassthroughChunkConverter
        chunk_converter = PassthroughChunkConverter()

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

        # Kafka publisher — construido aquí (Composition Root), no en application/
        kafka_publisher = self._build_kafka_publisher()

        pipeline_kwargs: dict[str, Any] = {

            "exchange_client":    exchange_client,
            "fetcher":            fetcher,
            "metrics":            PrometheusPipelineMetrics(),
            "quality":            quality,
            "market_type":        request.market_type,
            "dry_run":            request.dry_run,
            "symbols":            request.symbols,
            "timeframes":         request.timeframes,
            "start_date":         request.start_date,
            "auto_lookback_days": request.auto_lookback_days or 3650,
            "_chunk_converter":   chunk_converter,
            "publisher":          kafka_publisher,
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

    def _build_trades_stream(self, request: Any) -> Any:
        """
        Ensambla el pipeline de trades Kappa con GapAwareStream.

        Grafo de dependencias:
          CCXTAdapter
              ↓
          WSTradesSource            (stub WS — reemplazar con impl real)
              ↓
          GapAwareStream            (resiliencia: timeout + gap recovery)
              ↑ recovery_factory
          GapRecoveryFetcher(ccxt)  (source=REST_RECOVERY para gaps)
              ↓
          TradesSourceManager       (árbitro: cursor monotónico + dedup)

        Backfill:
          TradesBackfillFetcher(ccxt) → rest_source del manager

        El consumer downstream lee de TradesSourceManager — nunca conoce
        ni WSTradesSource ni GapRecoveryFetcher directamente (DIP).

        Fail-Fast
        ---------
        - request.symbols obligatorio (al menos un símbolo)
        - request.exchange obligatorio
        - gap_threshold_ms configurable via request.gap_threshold_ms (default 30s)
        """
        if not request.symbols:
            raise ValueError(
                "PipelineRequest.symbols es obligatorio para pipeline='trades_stream'. "
                f"Request recibido: {request}"
            )
        if not getattr(request, "exchange", None):
            raise ValueError(
                "PipelineRequest.exchange es obligatorio para pipeline='trades_stream'."
            )

        from market_data.adapters.outbound.exchange.ccxt_adapter import CCXTAdapter
        from market_data.adapters.inbound.websocket.ws_trades_source import WSTradesSource
        from market_data.adapters.inbound.websocket.gap_aware_stream import GapAwareStream
        from market_data.adapters.inbound.rest.gap_recovery_fetcher import GapRecoveryFetcher
        from market_data.adapters.inbound.rest.trades_backfill_fetcher import TradesBackfillFetcher
        from market_data.application.source_manager import TradesSourceManager

        adapter_kwargs: dict[str, Any] = {
            "exchange_id": request.exchange,
            "market_type": request.market_type,
        }
        if request.credentials is not None:
            adapter_kwargs["credentials"] = request.credentials
        if request.resilience is not None:
            adapter_kwargs["resilience"] = request.resilience

        exchange_client = CCXTAdapter(**adapter_kwargs)

        # gap_threshold_ms: configurable por símbolo/exchange en el futuro.
        # Default: 30s — apropiado para mercados líquidos (BTC/USDT perp).
        gap_threshold_ms: int = getattr(request, "gap_threshold_ms", 30_000)

        # Un manager por símbolo — cada símbolo tiene su propio cursor y dedup.
        managers: dict[str, TradesSourceManager] = {}

        for symbol in request.symbols:
            # WS source (stub — reemplazar cuando WSTradesSource tenga impl real)
            ws_source = WSTradesSource(
                exchange_id = request.exchange,
                symbol      = symbol,
                market_type = request.market_type,
            )

            # RecoveryFactory: closure que captura exchange_client y symbol.
            # Signature: (gap_start_ms: int, gap_end_ms: int) → TradesSourceProtocol
            def _make_recovery_factory(sym: str, client: Any):
                def _factory(gap_start_ms: int, gap_end_ms: int) -> GapRecoveryFetcher:
                    return GapRecoveryFetcher(
                        exchange_adapter = client,
                        exchange_id      = request.exchange,
                        symbol           = sym,
                        market_type      = request.market_type,
                        gap_start_ms     = gap_start_ms,
                        gap_end_ms       = gap_end_ms,
                    )
                return _factory

            gap_stream = GapAwareStream(
                source           = ws_source,
                recovery_factory = _make_recovery_factory(symbol, exchange_client),
                exchange_id      = request.exchange,
                symbol           = symbol,
                gap_threshold_ms = gap_threshold_ms,
                reconnect        = True,
            )

            # Backfill REST: bootstrap histórico antes de que el WS arranque.
            # since_ms desde request (None = desde el más antiguo disponible).
            rest_source = TradesBackfillFetcher(
                exchange_adapter = exchange_client,
                exchange_id      = request.exchange,
                symbol           = symbol,
                market_type      = request.market_type,
                since_ms         = getattr(request, "since_ms", None),
            )

            managers[symbol] = TradesSourceManager(
                rest_source = rest_source,
                ws_source   = gap_stream,
            )

        # Retornar el dict de managers — el caller (Dagster asset, test)
        # itera sobre ellos por símbolo. Un wrapper de pipeline puede
        # envolverlos en un PipelineTriggerPort si se necesita.
        return managers

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
