from __future__ import annotations

"""
core/logging/filters.py
=======================

Filtros reutilizables para sinks de Loguru.

Contrato de diseño:

- :func:`pipeline_filter` acepta por módulo de origen. Los campos extra
  (``exchange``, ``dataset``) son opcionales — evita silenciar logs legítimos
  sin contexto completo (errores de startup, warnings del scheduler, etc.).

- :func:`strict_pipeline_filter` variante para sinks que requieran
  ``exchange`` + ``dataset`` obligatorios (métricas, alertas).
"""

from typing import Any


_PIPELINE_MODULES: tuple[str, ...] = (
    "market_data.",
    "services.exchange.",
    "services.state.",
    "pipeline.",
    "core.pipeline.",
)


def pipeline_filter(record: dict[str, Any]) -> bool:
    """Acepta logs originados en módulos del pipeline de ingesta.

    Args:
        record: Registro de loguru con claves ``name`` y ``extra``.

    Returns:
        True si el módulo de origen pertenece al pipeline.
    """
    return record["name"].startswith(_PIPELINE_MODULES)


def strict_pipeline_filter(record: dict[str, Any]) -> bool:
    """Variante estricta: módulo del pipeline + ``exchange`` + ``dataset`` requeridos.

    Args:
        record: Registro de loguru con claves ``name`` y ``extra``.

    Returns:
        True si pasa :func:`pipeline_filter` y tiene ``exchange`` y ``dataset``.
    """
    return (
        pipeline_filter(record)
        and bool(record["extra"].get("exchange"))
        and bool(record["extra"].get("dataset"))
    )
