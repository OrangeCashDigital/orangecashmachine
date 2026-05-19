# -*- coding: utf-8 -*-
"""
market_data/adapters/chunk_converter.py — RE-EXPORT BRIDGE
SSOT movido a adapters/outbound/chunk_converter.py
"""
from market_data.adapters.outbound.chunk_converter import (  # noqa: F401
    PassthroughChunkConverter,
    get_default_converter,
)
__all__ = ["PassthroughChunkConverter", "get_default_converter"]
