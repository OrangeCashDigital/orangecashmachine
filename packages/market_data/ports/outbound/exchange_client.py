# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/exchange_client.py
=============================================

Puerto del cliente de exchange — DIP · SRP · SSOT.

Application y adapters inbound dependen de este protocolo.
CCXTAdapter (adapters/outbound/exchange/) lo implementa.
Tests inyectan fakes/mocks que lo satisfacen estructuralmente
(Protocol + runtime_checkable → no herencia requerida).
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Protocol, runtime_checkable


@runtime_checkable
class ExchangeClientPort(Protocol):
    """Contrato mínimo para comunicación con un exchange."""

    @property
    def exchange_id(self) -> str: ...

    def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> List[List[Any]]: ...

    def fetch_trades(
        self,
        symbol: str,
        since: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]: ...

    def fetch_funding_rate_history(
        self,
        symbol: str,
        since: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]: ...

    def fetch_open_interest_history(
        self,
        symbol: str,
        timeframe: str,
        since: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]: ...
