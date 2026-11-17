# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/freshness_store_port.py
==================================================

Puerto de salida para el almacenamiento de estados de freshness.
"""

from typing import Protocol, Optional
from shared.contracts.boundaries import FreshnessSnapshot

class FreshnessStorePort(Protocol):
    """
    Protocolo para persistir snapshots de freshness.
    """
    def set(self, stream_id: str, snapshot: FreshnessSnapshot) -> None:
        """Persiste un snapshot de freshness."""
        ...

    def get(self, stream_id: str) -> Optional[FreshnessSnapshot]:
        """Recupera un snapshot de freshness."""
        ...
