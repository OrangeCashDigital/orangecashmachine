# -*- coding: utf-8 -*-
"""market_data/ports/outbound/publisher_port.py

SHIM de compatibilidad — re-export desde publisher.py (SSOT).
No añadir logica aqui. Toda definicion vive en publisher.py.
"""

from __future__ import annotations

from market_data.ports.outbound.publisher import (  # noqa: F401
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
