# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/lineage.py
======================================

Puerto de trazabilidad de linaje — DIP · SSOT · SRP.

El application layer y quality/pipeline inyectan este protocolo;
la implementación concreta (SQLite / Redis / noop) vive en
infrastructure.lineage y se cabla en factories.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class LineageTrackerPort(Protocol):
    """Contrato mínimo para registrar eventos de linaje."""

    def record(self, event: object) -> None:
        """Persiste un LineageEvent. Implementaciones: fail-soft."""
        ...

    def new_run_id(self) -> str:
        """Genera un run_id único para correlación de pipeline."""
        ...
