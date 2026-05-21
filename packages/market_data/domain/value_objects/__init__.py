# -*- coding: utf-8 -*-
"""
market_data/domain/value_objects/__init__.py
=============================================

Re-exports canónicos de los Value Objects del bounded context market_data.

Este módulo es SOLO re-exports — ningún tipo se define aquí (SSOT).
Cada VO tiene su propio archivo canónico; este __init__ es la fachada
de conveniencia para imports cortos.

Catálogo
--------
Timeframe             — enum canónico de timeframes (str-compatible)
timeframe_to_ms       — conversión timeframe → milisegundos (O(1))
InvalidTimeframeError — excepción Fail-Fast de timeframe inválido
VALID_TIMEFRAMES      — frozenset[str] para validación O(1)
align_to_grid         — alineación de timestamp al grid del timeframe
Candle                — vela OHLCV inmutable con invariantes de dominio
Symbol                — par de trading (base/quote), formato canónico
OHLCVChunk            — fragmento inmutable del stream OHLCV (canónico)
OHLCVSource           — constantes de origen: REST/LIVE/BACKFILL/REPLAY
RawCandle             — tipo alias wire format CCXT (6-tupla tipada)
QualityLabel          — clasificación de calidad CLEAN/SUSPECT/CORRUPT
GapRange              — rango temporal de un gap en un dataset OHLCV

Principios
----------
DDD  — VOs puros: inmutables, definidos por valor, sin identidad
SSOT — un único archivo canónico por tipo; __init__ solo re-exporta
DIP  — infra nunca define tipos de dominio; dominio no depende de infra
OCP  — agregar VOs aquí no modifica consumidores existentes
KISS — sin lógica, solo imports
"""
from __future__ import annotations

from typing import Tuple

# ---------------------------------------------------------------------------
# Timeframe
# ---------------------------------------------------------------------------
from market_data.domain.value_objects.timeframe import (  # noqa: F401
    Timeframe,
    timeframe_to_ms,
    InvalidTimeframeError,
    VALID_TIMEFRAMES,
    align_to_grid,
)

# ---------------------------------------------------------------------------
# Candle
# ---------------------------------------------------------------------------
from market_data.domain.value_objects.candle import Candle  # noqa: F401

# ---------------------------------------------------------------------------
# Symbol
# ---------------------------------------------------------------------------
from market_data.domain.value_objects.symbol import Symbol  # noqa: F401

# ---------------------------------------------------------------------------
# OHLCVChunk + OHLCVSource (SSOT de origen)
# ---------------------------------------------------------------------------
from market_data.domain.value_objects.ohlcv_chunk import (  # noqa: F401
    OHLCVChunk,
    OHLCVSource,
)

# ---------------------------------------------------------------------------
# RawCandle — tipo alias para wire format CCXT crudo (ACL boundary)
# ---------------------------------------------------------------------------
RawCandle = Tuple[int, float, float, float, float, float]

# ---------------------------------------------------------------------------
# QualityLabel — SSOT en quality_label.py
# ---------------------------------------------------------------------------
from market_data.domain.value_objects.quality_label import QualityLabel  # noqa: F401

# ---------------------------------------------------------------------------
# GapRange — SSOT en gap_range.py
# ---------------------------------------------------------------------------
from market_data.domain.value_objects.gap_range import GapRange  # noqa: F401


# ---------------------------------------------------------------------------
# __all__
# ---------------------------------------------------------------------------
__all__ = [
    # Timeframe
    "Timeframe",
    "timeframe_to_ms",
    "InvalidTimeframeError",
    "VALID_TIMEFRAMES",
    "align_to_grid",
    # Market data VOs
    "Candle",
    "Symbol",
    "OHLCVChunk",
    "OHLCVSource",
    # Wire format alias (ACL boundary)
    "RawCandle",
    # Quality
    "QualityLabel",
    # Gap
    "GapRange",
    # Normalized trade
    "NormalizedTrade",
    "Side",
    # Order Book — Nivel 0
    "OrderBookSide",
    "PriceLevel",
    "OrderBookSnapshot",
    "OrderBookDelta",
]

# ---------------------------------------------------------------------------
# NormalizedTrade + Side (SSOT en normalized_trade.py)
# ---------------------------------------------------------------------------
from market_data.domain.value_objects.normalized_trade import (  # noqa: F401
    NormalizedTrade,
    Side,
)

# ---------------------------------------------------------------------------
# Order Book — Nivel 0 (microestructura nativa del exchange)
# ---------------------------------------------------------------------------
from market_data.domain.value_objects.order_book import (  # noqa: F401
    OrderBookSide,
    PriceLevel,
    OrderBookSnapshot,
    OrderBookDelta,
)
