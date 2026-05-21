# -*- coding: utf-8 -*-
"""
market_data/infrastructure/bootstrap/pipeline_factory.py
=========================================================

ConcretePipelineFactory — Composition Root de pipelines.

Responsabilidad única (SRP)
---------------------------
Instanciar el grafo completo de dependencias concretas para cada
tipo de pipeline y devolverlo como PipelineTriggerPort.

Licencia arquitectónica (BC-28)
--------------------------------
Este módulo PUEDE importar desde todas las capas de market_data:
domain, ports, application, adapters, infrastructure.
Es el único módulo con esa licencia — nadie importa desde aquí
excepto el entrypoint y los Dagster assets.

Principios: DIP · SRP · KISS · SafeOps · Fail-Fast
"""
from __future__ import annotations

from typing import Any, cast


# --------------------------------------------------------------------------- #
# Catalog builder — DRY, único punto de construcción del catalog Iceberg       #
# --------------------------------------------------------------------------- #

def _build_catalog() -> Any:
    """
    Construye y retorna el catalog PyIceberg desde variables de entorno.

    Composition Root — único lugar autorizado para instanciar el catalog.
    Fail-Fast: lanza si la configuración de Iceberg es inválida o
    el REST catalog no está disponible.

    Returns
    -------
    pyiceberg.catalog.Catalog
    """
    from market_data.infrastructure.storage.catalog import build_catalog
    return build_catalog()


# --------------------------------------------------------------------------- #
# ConcretePipelineFactory                                                      #
# --------------------------------------------------------------------------- #

class ConcretePipelineFactory:
    """
    Implementación concreta de PipelineFactoryPort.

    Todos los imports concretos ocurren lazy dentro de cada builder
    para evitar coste de inicialización en import-time y mantener
    el feedback de errores near-fail-fast en construcción, no en import.
    """

    def build(self, request: Any) -> Any:
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
        builder = dispatch.get(request.pipeline)
        if builder is None:
            raise ValueError(
                f"PipelineType desconocido: {request.pipeline!r}. "
                f"Registrados: {list(dispatch)}"
            )
        return builder(request)

    # ----------------------------------------------------------------------- #
    # Helpers internos                                                          #
    # ----------------------------------------------------------------------- #

    def _resolve_adapter_kwargs(self, request: Any) -> dict[str, Any]:
        """Extrae kwargs de CCXTAdapter desde el request (DRY)."""
        kwargs: dict[str, Any] = {
            "exchange_id": request.exchange,
            "market_type": request.market_type,
        }
        if request.credentials is not None:
            kwargs["credentials"] = request.credentials
        if request.resilience is not None:
            kwargs["resilience"] = request.resilience
        return kwargs

    def _build_kafka_publisher(self) -> Any:
        """
        Construye KafkaOHLCVPublisher respetando el feature flag KAFKA_ENABLED.

        Retorna None si Kafka está deshabilitado o no disponible (fail-soft).
        """
        import os
        from ocm.config.env_vars import KAFKA_ENABLED as _KAFKA_ENABLED_VAR

        kafka_flag = os.environ.get(_KAFKA_ENABLED_VAR, "false").strip().lower()
        if kafka_flag not in ("1", "true", "yes"):
            return None
        try:
            from market_data.infrastructure.kafka.producer import KafkaProducerAdapter
            from market_data.infrastructure.kafka.ohlcv_publisher import KafkaOHLCVPublisher
            from market_data.ports.outbound.kafka_producer import KafkaProducerPort
            producer = KafkaProducerAdapter.from_env()
            # cast: KafkaProducerAdapter satisface KafkaProducerPort estructuralmente.
            # mypy no infiere structural subtyping desde clases concretas — cast explicita
            # la intención sin ocultar bugs reales (el runtime verifica en uso).
            return KafkaOHLCVPublisher(producer=cast(KafkaProducerPort, producer))
        except Exception as exc:
            import logging
            logging.getLogger(__name__).warning(
                "KafkaOHLCVPublisher no disponible — modo degradado: %s", exc
            )
            return None

    # ----------------------------------------------------------------------- #
    # Builders concretos — SRP: uno por tipo de pipeline                       #
    # ----------------------------------------------------------------------- #

    def _build_ohlcv(self, request: Any) -> Any:
        """
        Cabla OHLCVPipeline con CCXTAdapter y sus dependencias concretas.

        Grafo de dependencias
        ---------------------
        CCXTAdapter  →  HistoricalFetcherAsync  →  OHLCVPipeline
                                                        ↑
        QualityPipeline ─────────────────────────────────┤
        PrometheusPipelineMetrics ───────────────────────┤
        RedisCursorStore | InMemoryCursorStore ───────────┘

        Notas de cableado
        -----------------
        - publisher: OHLCVPipeline lo gestiona internamente (NullPublisher por
          defecto). No se pasa como kwarg — no es parte del contrato público.
        - _chunk_converter: vive en PipelineContext, no en OHLCVPipeline.__init__.
          Se inyecta via ctx si la estrategia lo necesita.
        - cast(ExchangeClientPort): CCXTAdapter satisface ExchangeClientPort
          estructuralmente (runtime_checkable Protocol). mypy no infiere subtyping
          desde ABC sin anotación explícita en la clase concreta — cast es correcto.
        """
        from market_data.adapters.outbound.exchange.ccxt_adapter import CCXTAdapter
        from market_data.adapters.inbound.rest.ohlcv_fetcher import HistoricalFetcherAsync
        from market_data.application.pipelines.ohlcv_pipeline import OHLCVPipeline
        from market_data.application.use_cases.ohlcv_transformer import OHLCVTransformer
        from market_data.infrastructure.observability.metrics_adapter import (
            PrometheusPipelineMetrics,
        )
        from market_data.application.quality.pipeline import QualityPipeline
        from market_data.infrastructure.quality.anomaly_registry import default_registry
        from market_data.ports.outbound.exchange_client import ExchangeClientPort
        from market_data.ports.outbound.state import CursorStorePort, AsyncCursorStorePort
        from ocm.runtime.state import build_cursor_store, InMemoryCursorStore

        raw_adapter = CCXTAdapter(**self._resolve_adapter_kwargs(request))
        # cast: CCXTAdapter satisface ExchangeClientPort (runtime_checkable Protocol).
        exchange_client = raw_adapter

        quality = QualityPipeline(registry=default_registry)

        # Cursor store: Redis en producción, InMemory en degradación controlada.
        # cast a CursorStorePort: ambas implementaciones satisfacen el protocolo
        # estructuralmente; mypy no lo infiere sin la anotación explícita.
        try:
            cursor = cast(CursorStorePort, build_cursor_store())
        except Exception:
            cursor = cast(CursorStorePort, InMemoryCursorStore())

        fetcher = HistoricalFetcherAsync(
            storage            = None,
            transformer        = OHLCVTransformer(),
            exchange_client    = raw_adapter,
            cursor_store       = cast(AsyncCursorStorePort, cursor),
            backfill_mode      = True,
            market_type        = request.market_type,
            config_start_date  = request.start_date or "auto",
            auto_lookback_days = request.auto_lookback_days or 3650,
        )

        # Fail-Fast: validar campos obligatorios antes de construir el pipeline.
        if not request.symbols:
            raise ValueError(
                "PipelineRequest.symbols es obligatorio para pipeline='ohlcv'. "
                f"Request: {request}"
            )
        if not request.timeframes:
            raise ValueError(
                "PipelineRequest.timeframes es obligatorio para pipeline='ohlcv'. "
                f"Request: {request}"
            )
        if not request.start_date:
            raise ValueError(
                "PipelineRequest.start_date es obligatorio para pipeline='ohlcv'. "
                f"Request: {request}"
            )

        return OHLCVPipeline(
            symbols            = request.symbols,
            timeframes         = request.timeframes,
            start_date         = request.start_date,
            exchange_client    = cast(ExchangeClientPort, exchange_client),
            fetcher            = fetcher,
            metrics            = PrometheusPipelineMetrics(),
            quality            = quality,
            cursor_store       = cast(CursorStorePort, cursor),
            market_type        = request.market_type,
            dry_run            = request.dry_run,
            auto_lookback_days = request.auto_lookback_days or 3650,
        )

    def _build_trades(self, request: Any) -> Any:
        """
        Cabla TradesPipeline con CCXTAdapter + TradesFetcher + TradesStorage.

        DIP — TradesPipeline recibe abstracciones (TradesFetcherPort,
        TradesStoragePort, ExchangeClientPort). Las implementaciones concretas
        se construyen aquí y nunca se importan desde application/.
        """
        from market_data.adapters.outbound.exchange.ccxt_adapter import CCXTAdapter
        from market_data.adapters.inbound.rest.trades_fetcher import TradesFetcher
        from market_data.infrastructure.storage.silver.trades_storage import TradesStorage
        from market_data.application.pipelines.trades_pipeline import TradesPipeline
        from market_data.ports.outbound.exchange_client import ExchangeClientPort

        raw_adapter     = CCXTAdapter(**self._resolve_adapter_kwargs(request))
        exchange_client = raw_adapter

        catalog = _build_catalog()
        storage = TradesStorage(
            exchange    = request.exchange,
            market_type = request.market_type,
            catalog     = catalog,
        )
        fetcher = TradesFetcher(
            exchange_client = raw_adapter,
            storage         = storage,
            market_type     = request.market_type,
            dry_run         = request.dry_run,
        )

        return TradesPipeline(
            symbols         = request.symbols or [],
            exchange_client = cast(ExchangeClientPort, exchange_client),
            fetcher         = fetcher,
            storage         = storage,
            market_type     = request.market_type,
            dry_run         = request.dry_run,
        )

    def _build_trades_stream(self, request: Any) -> Any:
        """
        Ensambla el pipeline de trades Kappa con GapAwareStream.

        Fail-Fast: request.symbols y request.exchange son obligatorios.
        """
        if not request.symbols:
            raise ValueError(
                "PipelineRequest.symbols es obligatorio para pipeline='trades_stream'. "
                f"Request: {request}"
            )
        if not getattr(request, "exchange", None):
            raise ValueError(
                "PipelineRequest.exchange es obligatorio para pipeline='trades_stream'."
            )

        from market_data.adapters.outbound.exchange.ccxt_adapter import CCXTAdapter
        from market_data.adapters.inbound.websocket.ws_trades_source import WSTradesSource
        from market_data.adapters.inbound.websocket.gap_aware_stream import GapAwareStream
        from market_data.adapters.inbound.rest.gap_recovery_fetcher import GapRecoveryFetcher
        from market_data.adapters.inbound.rest.trades_backfill_fetcher import (
            TradesBackfillFetcher,
        )
        from market_data.application.source_manager import TradesSourceManager

        exchange_client = CCXTAdapter(**self._resolve_adapter_kwargs(request))
        gap_threshold_ms: int = getattr(request, "gap_threshold_ms", 30_000)
        managers: dict[str, TradesSourceManager] = {}

        for symbol in request.symbols:
            ws_source = WSTradesSource(
                exchange_id = request.exchange,
                symbol      = symbol,
                market_type = request.market_type,
            )

            # Closure que captura client y symbol — evita late-binding.
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

        return managers

    def _build_derivatives(self, request: Any) -> Any:
        """
        Cabla DerivativesPipeline con CCXTAdapter + fetchers inyectados.

        DIP — DerivativesPipeline recibe un dict de fetchers (DerivativesFetcherPort).
        No conoce FundingRateFetcher ni OpenInterestFetcher directamente.

        SafeOps — Si datasets no se especifica, usa todos los soportados (fail-soft).
        """
        from market_data.adapters.outbound.exchange.ccxt_adapter import CCXTAdapter
        from market_data.adapters.inbound.rest.derivatives_fetcher import (
            FundingRateFetcher,
            OpenInterestFetcher,
        )
        from market_data.infrastructure.storage.silver.derivatives_storage import (
            DerivativesStorage,
        )
        from market_data.application.pipelines.derivatives_pipeline import (
            DerivativesPipeline,
            SUPPORTED_DERIVATIVE_DATASETS,
        )
        from market_data.ports.outbound.exchange_client import ExchangeClientPort

        raw_adapter     = CCXTAdapter(**self._resolve_adapter_kwargs(request))
        exchange_client = raw_adapter

        catalog = _build_catalog()

        # Un storage por dataset — DerivativesStorage es SRP: una tabla por dataset.
        storage_fr = DerivativesStorage(
            exchange    = request.exchange,
            dataset     = "funding_rate",
            market_type = request.market_type,
            catalog     = catalog,
        )
        storage_oi = DerivativesStorage(
            exchange    = request.exchange,
            dataset     = "open_interest",
            market_type = request.market_type,
            catalog     = catalog,
        )

        # Fetchers inyectados como dict — clave = nombre del dataset (SSOT).
        fetchers: dict[str, Any] = {
            "funding_rate": FundingRateFetcher(
                exchange_client = raw_adapter,
                storage         = storage_fr,
                market_type     = request.market_type,
                dry_run         = request.dry_run,
            ),
            "open_interest": OpenInterestFetcher(
                exchange_client = raw_adapter,
                storage         = storage_oi,
                market_type     = request.market_type,
                dry_run         = request.dry_run,
            ),
        }

        datasets: list[str] = (
            list(request.datasets)
            if getattr(request, "datasets", None)
            else list(SUPPORTED_DERIVATIVE_DATASETS)
        )

        return DerivativesPipeline(
            symbols         = request.symbols or [],
            datasets        = datasets,
            exchange_client = cast(ExchangeClientPort, exchange_client),
            fetchers        = fetchers,
            market_type     = request.market_type,
            dry_run         = request.dry_run,
        )
