# -*- coding: utf-8 -*-
"""Re-export shim — SSOT movido a domain/exchange/exchange_quirks.py (BC-05)."""
from market_data.domain.value_objects.exchange_quirks import (  # noqa: F401
    ExchangeQuirks,
    get_quirks,
)

__all__ = ["ExchangeQuirks", "get_quirks"]
