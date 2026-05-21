# -*- coding: utf-8 -*-
"""
market_data.application.context
================================

PipelineContext — objeto de aplicación que agrupa las dependencias
inyectadas en un pipeline activo.

Pertenece a application/ porque sus campos referencian ports (interfaces
de la capa application) e infraestructura concreta inyectada desde fuera.
domain/ no puede conocer ports — este archivo resuelve BC-08.

Inmutable en runtime: se construye en pipeline_factory.py (Composition Root)
y se pasa hacia abajo sin mutación.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from market_data.ports.outbound.storage        import OHLCVStorage
from market_data.ports.outbound.state          import CursorStorePort
from market_data.ports.outbound.gap_registry   import GapRegistryPort
from market_data.ports.outbound.kafka_producer import KafkaProducerPort
from market_data.ports.outbound.chunk_converter import OHLCVChunkConverterPort


@dataclass
class PipelineContext:
    """
    Aggregate de dependencias de runtime para un pipeline activo.

    Construido por pipeline_factory.py (único Composition Root).
    Inmutable por convención — los pipelines leen, nunca escriben.

    Campos:
        exchange_id     — identificador de exchange (ej. "bybit")
        market_type     — "spot" | "futures"
        fetcher         — adaptador de fetch inyectado
        bronze          — storage Bronze inyectado
        storage         — port OHLCVStorage (Silver/Gold)
        cursor_store    — port de estado de cursor
        gap_registry    — port de registro de gaps
        kafka_producer  — port de Kafka (puede ser NullKafkaProducer)
        credentials     — dict de credenciales de exchange
        resilience      — config de resiliencia (circuit breaker, retry)
        symbols         — lista de symbols activos
        timeframes      — lista de timeframes activos
        start_date      — fecha de inicio para backfill
        auto_lookback_days — días de lookback automático
        _chunk_converter   — port de conversión DataFrame→OHLCVChunk
    """
    exchange_id:        str
    market_type:        str
    fetcher:            Any                       # HistoricalFetcherAsync — evita circular
    bronze:             Any                       # BronzeStorage — evita circular
    storage:            OHLCVStorage
    cursor_store:       CursorStorePort
    gap_registry:       Optional[GapRegistryPort] = None
    kafka_producer:     Optional[KafkaProducerPort] = None
    credentials:        dict                      = field(default_factory=dict)
    resilience:         Any                       = None
    symbols:            list                      = field(default_factory=list)
    timeframes:         list                      = field(default_factory=list)
    start_date:         Optional[Any]             = None
    auto_lookback_days: int                       = 30
    _chunk_converter:   Optional[OHLCVChunkConverterPort] = None

    # ── Invariantes de construcción ──────────────────────────────────────────

    def __post_init__(self) -> None:
        """
        Validación fail-fast de dependencias requeridas.

        Detecta inyección incompleta en construction-time, no en primer uso.
        Principio: fail-fast donde importa (SOLID — ISP + DIP).

        fetcher y bronze se declaran como Any para evitar circular import
        en runtime; __post_init__ compensa la pérdida de static safety
        con una guarda explícita de runtime.
        """
        if self.fetcher is None:
            raise TypeError(
                "PipelineContext.fetcher es requerido. "
                "pipeline_factory debe inyectar el fetcher antes de construir el contexto."
            )
        if self.bronze is None:
            raise TypeError(
                "PipelineContext.bronze es requerido. "
                "pipeline_factory debe inyectar BronzeStorage antes de construir el contexto."
            )
        if self.storage is None:
            raise TypeError("PipelineContext.storage es requerido.")
        if self.cursor_store is None:
            raise TypeError(
                "PipelineContext.cursor_store es requerido. "
                "Usar InMemoryCursorStore() como fallback mínimo si no hay Redis."
            )

    # ── Accesores tipados ─────────────────────────────────────────────────────

    def get_chunk_converter(self) -> OHLCVChunkConverterPort:
        """
        Acceso tipado al converter DataFrame->OHLCVChunk.

        Fail-fast: lanza si _chunk_converter no fue inyectado.
        El pipeline Kappa debe inyectarlo antes de procesar eventos.
        """
        if self._chunk_converter is None:
            raise RuntimeError(
                "PipelineContext.chunk_converter no inyectado. "
                "Verificar pipeline_factory._build_ohlcv — debe setear "
                "_chunk_converter antes de pasar el contexto a las strategies."
            )
        return self._chunk_converter
