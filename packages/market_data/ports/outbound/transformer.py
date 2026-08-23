# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/transformer.py
=========================================

Puerto del transformador OHLCV — DIP.

Permite que adapters inbound anoten el tipo del transformer
sin crear una dependencia runtime hacia application.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

import polars as pl


@runtime_checkable
class OHLCVTransformerPort(Protocol):
    """Contrato de transformación DataFrame OHLCV crudo → DataFrame Silver."""

    def transform(
        self,
        df: pl.DataFrame,
        symbol: str = "unknown",
        timeframe: str = "unknown",
        exchange: str = "unknown",
        run_id: str | None = None,
    ) -> pl.DataFrame: ...
