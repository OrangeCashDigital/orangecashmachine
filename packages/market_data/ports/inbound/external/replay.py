"""
market_data/ports/inbound/external/replay.py
=============================================

Contrato de adquisición histórica (replay / backfill no-streaming).

Mismo boundary que polling.py, pero para rangos históricos explícitos.
Parte de la capacidad external_ingestion de ADR-0014.

Reutiliza PollingResult como envoltorio crudo de la respuesta.

Principios: DIP · ISP · SRP
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, runtime_checkable

from market_data.ports.inbound.external.polling import PollingResult

__all__ = ["HistoricalRequest", "ReplayPort"]


@dataclass(frozen=True, slots=True)
class HistoricalRequest:
    """Petición de histórico para una fuente externa."""

    metric: str
    symbol: str
    start: datetime
    end: datetime


@runtime_checkable
class ReplayPort(Protocol):
    """Contrato de adquisición histórica (backfill / replay).

    source_id es la identidad canónica de la fuente (ver polling.py).
    """

    source_id: str

    async def fetch_historical(self, request: HistoricalRequest) -> PollingResult:
        """Adquiere el histórico crudo de una métrica en [start, end]."""
        ...
