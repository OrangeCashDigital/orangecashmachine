from __future__ import annotations

"""
ocm/observability/filters.py
======================================

Filtros reutilizables para sinks de Loguru.

Módulos canónicos del pipeline OCM
-----------------------------------
Derivados de la estructura real del proyecto (verificada con grep).
Cualquier cambio estructural en market_data/ u ocm/control_plane/
debe reflejarse aquí — este módulo es el SSOT de qué constituye "pipeline".

Contratos
---------
- ``pipeline_filter``        — acepta por módulo de origen (sin requerir
                               exchange/dataset). Cubre errores de startup
                               y warnings del scheduler sin contexto completo.
- ``strict_pipeline_filter`` — variante para sinks que requieren
                               exchange + dataset obligatorios.
"""

from typing import Any


# SSOT de prefijos que constituyen el pipeline de ingesta y orquestación.
# Prefijos exactos — startswith() es O(len(prefix)), no O(len(name)).
#
# Orden lógico: de capa más interna (domain/application) a infraestructura.
# Añadir un bounded context nuevo = una línea aquí (OCP).
_PIPELINE_MODULES: tuple[str, ...] = (
    # Application layer — pipelines, consumers, use cases
    "market_data.application.",
    # Adapters — inbound (REST, WS) y outbound (exchange, storage)
    "market_data.adapters.",
    # Infrastructure — storage medallion, streaming, event bus
    "market_data.infrastructure.storage.",
    # Quality — validators, invariants, anomaly registry
    "market_data.quality.",
    # Legacy prefijos pre-refactor — mantener hasta migración completa
    "market_data.ingestion.",
    "market_data.processing.",
    # Control plane — orquestación Dagster / Prefect
    "ocm.control_plane.",
)


def pipeline_filter(record: dict[str, Any]) -> bool:
    """Acepta logs originados en módulos del pipeline de ingesta y orquestación.

    No requiere exchange ni dataset en ``extra`` — errores de startup y
    warnings del scheduler legítimamente carecen de contexto completo.

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
    Un módulo legítimo del pipeline sin exchange/dataset NO pasa — es intencional.

    Args:
        record: Registro de loguru con claves ``name`` y ``extra``.

    Returns:
        True si pasa ``pipeline_filter`` y tiene ``exchange`` y ``dataset`` no vacíos.
    """
    return (
        pipeline_filter(record)
        and bool(record["extra"].get("exchange"))
        and bool(record["extra"].get("dataset"))
    )


__all__ = ["pipeline_filter", "strict_pipeline_filter"]
