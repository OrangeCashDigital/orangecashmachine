# -*- coding: utf-8 -*-
"""
market_data/domain/__init__.py
================================

Fachada pública del bounded context market_data — capa de dominio.

Estructura
----------
domain/
  exceptions/    — jerarquía canónica de excepciones del BC
  value_objects/ — tipos inmutables sin identidad (Timeframe, Candle, OHLCVBatch,
                   Symbol, RawCandle, QualityLabel, GapRange)
  entities/      — tipos con identidad de dominio (DataTier)
  events/        — domain events (DomainEvent, CandleReceived, OHLCVBatchReceived,
                   LineageEvent, PipelineLayer, LineageStatus)
  rules/         — reservado para reglas de dominio puras sin I/O (vacío)
  services/      — reservado para domain services puros sin I/O (vacío)

Ubicaciones canónicas fuera de domain/ (no son dominio puro)
-------------------------------------------------------------
  CandleValidator, CandleNormalizer → market_data.processing.validation
  check_dataset_invariants          → market_data.quality.invariants.invariants
  DataQualityPolicy, QualityPipeline → market_data.quality

Uso recomendado
---------------
Importar siempre desde el submódulo específico:

    from market_data.domain.exceptions import NoDataAvailableError
    from market_data.domain.value_objects import Timeframe, Candle, OHLCVBatch
    from market_data.domain.entities import DataTier
    from market_data.domain.events import OHLCVBatchReceived, LineageEvent

Este __init__.py no re-exporta nada — fuerza imports explícitos (KISS · SSOT).

Principios
----------
Clean Architecture — dominio no depende de infra, frameworks ni I/O
DDD               — value objects, entities, domain events
SSOT              — una ubicación canónica para cada tipo
DIP               — las capas superiores dependen del dominio, nunca al revés
"""
# Intencionalmente vacío — los imports deben ser explícitos hacia los submódulos.
