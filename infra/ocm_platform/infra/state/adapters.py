from __future__ import annotations

"""
infra/state/adapters.py
========================
Re-exporta las implementaciones concretas de CursorStorePort
para uso exclusivo del composition root.

REGLA: solo main.py / entrypoints importan de aquí.
El dominio importa únicamente market_data.ports.state.CursorStorePort.
"""

from ocm_platform.infra.state.cursor_store import (
    CursorStore as RedisCursorStore,
    InMemoryCursorStore,
)
from ocm_platform.infra.state.factories import build_cursor_store

__all__ = [
    "RedisCursorStore",
    "InMemoryCursorStore",
    "build_cursor_store",
]
