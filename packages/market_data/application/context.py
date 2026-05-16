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

from market_data.ports.outbound.storage       import OHLCVStorage
from market_data.ports.outbound.state         import CursorStorePort
from market_data.ports.outbound.gap_registry  import GapRegistryPort
from market_data.ports.outbound.kafka_producer import KafkaProducerPort


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
