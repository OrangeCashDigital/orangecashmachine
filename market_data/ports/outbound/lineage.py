# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/lineage.py
=======================================

Puerto OUTBOUND: contrato del tracker de lineage.

Responsabilidad
---------------
Declarar el contrato que cualquier implementación de trazabilidad
de pipeline debe cumplir. Application layer depende de este protocolo.

Implementación de referencia
-----------------------------
market_data.adapters.outbound.persistence.sqlite.lineage_tracker.LineageTracker

Principios
----------
DIP  — use cases dependen de abstracción, no de SQLite
OCP  — Postgres, OpenLineage, archivo implementan este contrato
ISP  — solo los métodos que los pipelines realmente usan
"""
from __future__ import annotations

from typing import Protocol, runtime_checkable

from market_data.domain.events._lineage import LineageEvent


@runtime_checkable
class LineagePort(Protocol):
    """
    Contrato mínimo para trazabilidad de pipeline.

    SafeOps
    -------
    record() DEBE ser fail-soft: un fallo de lineage nunca detiene el pipeline.
    """

    def record(self, event: LineageEvent) -> None:
        """
        Persiste un LineageEvent.

        Fail-soft: un fallo nunca interrumpe el pipeline.
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


__all__ = ["LineagePort"]
