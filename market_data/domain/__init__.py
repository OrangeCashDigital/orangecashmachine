# -*- coding: utf-8 -*-
"""
market_data/domain/__init__.py
================================

Fachada pública del bounded context market_data — capa de dominio.

Estructura
----------
domain/
  exceptions/    — jerarquía canónica de excepciones del BC
  value_objects/ — tipos inmutables sin identidad (Timeframe, RawCandle, QualityLabel)
  entities/      — tipos con identidad de dominio (OHLCVBar, ValidationResult)
  rules/         — reglas de negocio puras sin I/O (CandleValidator, invariants)
  events/        — eventos de dominio (LineageEvent, PipelineLayer, LineageStatus)
  services/      — servicios de dominio que orquestan reglas/entidades (DataQualityPolicy)

Uso recomendado
---------------
Los consumidores dentro de market_data deben importar desde los
submódulos específicos para máxima claridad semántica:

    from market_data.domain.exceptions import NoDataAvailableError
    from market_data.domain.value_objects import Timeframe, RawCandle
    from market_data.domain.entities import ValidationResult
    from market_data.domain.rules import CandleValidator, check_dataset_invariants
    from market_data.domain.events import LineageEvent, PipelineLayer
    from market_data.domain.services import DataQualityPolicy

Este __init__.py no re-exporta nada — fuerza imports explícitos
y evita la tentación de usar `from market_data.domain import *` (KISS · Clean Code).

Principios
----------
Clean Architecture — dominio no depende de infra, frameworks ni I/O
DDD               — entities, value objects, domain services, domain events
SSOT              — una ubicación canónica para cada tipo de dominio
DIP               — capas externas (application, adapters) dependen del dominio
"""
# Intencionalmente vacío — los imports deben ser explícitos hacia los submódulos.
