# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/fetcher.py
======================================

Puertos de fetching de datos secundarios.

Separa la interfaz de la implementación concreta (DIP).
Los pipelines de application dependen de estos protocolos;
los adapters inbound los implementan.
"""
from __future__ import annotations

from typing import Any, List, Protocol, runtime_checkable


@runtime_checkable
class TradesFetcherPort(Protocol):
    """Contrato de fetching de trades tick-by-tick."""

    def fetch(self, symbol: str, since: int | None = None) -> List[dict[str, Any]]:
        """Retorna lista de trades crudos desde `since` (ms epoch)."""
        ...


@runtime_checkable
class FundingRateFetcherPort(Protocol):
    """Contrato de fetching de funding rates."""

    def fetch(self, symbol: str, since: int | None = None) -> List[dict[str, Any]]:
        ...


@runtime_checkable
class OpenInterestFetcherPort(Protocol):
    """Contrato de fetching de open interest."""

    def fetch(self, symbol: str, since: int | None = None) -> List[dict[str, Any]]:
        ...
