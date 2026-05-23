# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/publisher.py
========================================

Re-export desde publisher_port.py (SSOT).

Este módulo existe solo por compatibilidad de imports existentes.
En código nuevo importar desde publisher_port directamente.
"""

from __future__ import annotations

from market_data.ports.outbound.publisher_port import (  # noqa: F401
    SOURCE_BACKFILL,
    SOURCE_LIVE,
    SOURCE_REPLAY,
    NullOHLCVPublisher,
    NullPublisher,
    OHLCVPublisherPort,
)

__all__ = [
    "NullOHLCVPublisher",
    "NullPublisher",
    "OHLCVPublisherPort",
    "SOURCE_BACKFILL",
    "SOURCE_LIVE",
    "SOURCE_REPLAY",
]
