from __future__ import annotations

"""
ocm_platform/observability/filters.py
======================================

Filtros reutilizables para sinks de Loguru.

Módulos canónicos del pipeline OCM
-----------------------------------
Derivados de la estructura real del proyecto (verificada con grep).
Cualquier cambio estructural en market_data/ u ocm_platform/control_plane/
debe reflejarse aquí.

Contratos
---------
- ``pipeline_filter``        — acepta por módulo de origen (sin requerir
                               exchange/dataset). Cubre errores de startup
                               y warnings del scheduler sin contexto completo.
- ``strict_pipeline_filter`` — variante para sinks que requieren
                               exchange + dataset obligatorios.
"""

from typing import Any


# SSOT de módulos que constituyen el pipeline de ingesta y orquestación.
# Prefijos exactos — startswith() es O(len(prefix)), no O(len(name)).
_PIPELINE_MODULES: tuple[str, ...] = (
    "market_data.ingestion.",
    "market_data.processing.",
    "market_data.storage.",
    "market_data.quality.",
    "market_data.adapters.",
    "ocm_platform.control_plane.",
)


def pipeline_filter(record: dict[str, Any]) -> bool:
    """Acepta logs originados en módulos del pipeline de ingesta y orquestación.

    Args:
        record: Registro de loguru con clave ``name`` (módulo de origen).

    Returns:
        True si el módulo pertenece al pipeline OCM.
    """
    name: str = record["name"]
    return any(name.startswith(prefix) for prefix in _PIPELINE_MODULES)


def strict_pipeline_filter(record: dict[str, Any]) -> bool:
    """Variante estricta: módulo del pipeline + ``exchange`` + ``dataset`` requeridos.

    Uso: sinks de métricas o alertas donde el contexto completo es obligatorio.

    Args:
        record: Registro de loguru con claves ``name`` y ``extra``.

    Returns:
        True si pasa ``pipeline_filter`` y tiene ``exchange`` y ``dataset``.
    """
    return (
        pipeline_filter(record)
        and bool(record["extra"].get("exchange"))
        and bool(record["extra"].get("dataset"))
    )
