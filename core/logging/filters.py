"""
core/logging/filters.py
=======================
Filtros reutilizables para sinks de Loguru.

Diseño de contrato:
  - pipeline_filter acepta por módulo de origen (condición necesaria).
  - Los campos extra (exchange, dataset) enriquecen trazabilidad
    pero NO son obligatorios para pasar el filtro.
  - Esto evita silenciar logs legítimos del pipeline sin contexto
    completo (errores de startup, warnings del scheduler, etc.).

  - strict_pipeline_filter: variante composable para sinks que
    requieran exchange+dataset obligatorios (métricas, alertas).
"""

from typing import Any

_PIPELINE_MODULES = (
    "market_data.",
    "services.exchange.",
    "services.state.",
    "pipeline.",
    "core.pipeline.",
)


def pipeline_filter(record: dict[str, Any]) -> bool:
    """
    Acepta logs originados en módulos del pipeline de ingesta.
    Criterio: módulo de origen — los campos extra son opcionales.
    """
    return record["name"].startswith(_PIPELINE_MODULES)


def strict_pipeline_filter(record: dict[str, Any]) -> bool:
    """
    Variante estricta: módulo del pipeline + exchange + dataset obligatorios.
    Usar en sinks dedicados a métricas o alertas por exchange/dataset.
    """
    return (
        pipeline_filter(record)
        and bool(record["extra"].get("exchange"))
        and bool(record["extra"].get("dataset"))
    )
