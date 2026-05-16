# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/lineage.py
=======================================

Puerto OUTBOUND: contrato del tracker de lineage.

Responsabilidad
---------------
Declarar el contrato que cualquier implementación de trazabilidad
de pipeline debe cumplir. La capa application depende de este protocolo,
nunca de la implementación concreta (SQLite, Postgres, OpenLineage, etc.).

Implementación de referencia
-----------------------------
market_data.infrastructure.lineage.tracker.LineageTracker

Principios
----------
DIP    — use cases dependen de abstracción, no de SQLite/infra
OCP    — nuevas implementaciones sin modificar este contrato
ISP    — solo los métodos que los pipelines realmente necesitan
SafeOps — record() DEBE ser fail-soft: un fallo de lineage nunca detiene el pipeline
SSOT   — LineageTrackerPort es la única fuente de verdad del contrato
"""
from __future__ import annotations

import uuid
from typing import Protocol, runtime_checkable

from market_data.domain.events._lineage import LineageEvent


@runtime_checkable
class LineageTrackerPort(Protocol):
    """
    Contrato mínimo para trazabilidad de pipeline.

    SafeOps
    -------
    record() DEBE ser fail-soft: un fallo de lineage nunca detiene el pipeline.
    """

    def record(self, event: LineageEvent) -> None:
        """
        Persiste un LineageEvent.

        Fail-soft: un fallo aquí nunca debe propagar excepción al pipeline.
        """
        ...

    @staticmethod
    def new_run_id() -> str:
        """Genera un run_id UUID v4 para correlacionar eventos de un lote."""
        ...

    def get_run(self, run_id: str) -> list[LineageEvent]:
        """
        Retorna todos los eventos de un run_id.

        Fail-soft: retorna [] en caso de error.
        """
        ...


class NullLineageTracker:
    """
    Implementación no-op de LineageTrackerPort.

    Uso: tests, entornos sin persistencia, DI por defecto.
    Fail-safe por diseño: todos los métodos son no-op o retornan vacíos.
    """

    def record(self, event: LineageEvent) -> None:  # noqa: D102
        pass

    @staticmethod
    def new_run_id() -> str:  # noqa: D102
        return str(uuid.uuid4())

    def get_run(self, run_id: str) -> list[LineageEvent]:  # noqa: D102
        return []


# Alias de backward compat — código existente que usa LineagePort sigue funcionando.
# Deprecado: usar LineageTrackerPort.
LineagePort = LineageTrackerPort

__all__ = [
    "LineageTrackerPort",
    "NullLineageTracker",
    "LineagePort",       # backward compat
    "LineageEvent",      # re-export conveniente para capas superiores
]
