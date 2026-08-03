# -*- coding: utf-8 -*-
"""
shared/enums.py
================

Literales de dominio neutros (sin dependencias internas de shared/).
SSOT de las enumeraciones compartidas entre bounded contexts.

Independiente de transporte: NO importa nada de shared.kafka ni de
ningún bounded context — solo stdlib (typing). BC-01 safe.
"""

from typing import Literal

SignalDirection = Literal["buy", "sell", "hold"]
"""Dirección de señal de trading: 'buy' | 'sell' | 'hold'."""

OrderSide = Literal["buy", "sell"]
"""Lado de una orden: 'buy' | 'sell'."""

PositionSide = Literal["long", "short"]
"""Lado de una posición: 'long' | 'short'."""

DataSource = Literal["live", "backfill", "replay"]
"""Origen Kappa de un dato de mercado."""

DATASOURCE_LIVE: DataSource = "live"
DATASOURCE_BACKFILL: DataSource = "backfill"
DATASOURCE_REPLAY: DataSource = "replay"

_VALID_SOURCES: frozenset[str] = frozenset({"live", "backfill", "replay"})
_VALID_SIGNAL_DIRECTIONS: frozenset[str] = frozenset({"buy", "sell", "hold"})

__all__ = [
    "SignalDirection",
    "OrderSide",
    "PositionSide",
    "DataSource",
    "DATASOURCE_LIVE",
    "DATASOURCE_BACKFILL",
    "DATASOURCE_REPLAY",
    "_VALID_SOURCES",
    "_VALID_SIGNAL_DIRECTIONS",
]
