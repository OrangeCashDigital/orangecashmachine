# -*- coding: utf-8 -*-
"""
portfolio/infra/memory_store.py
================================

InMemoryPositionStore — implementación in-memory de PositionStore.

Adecuada para paper trading, dry-run y tests de integración.
Para producción usar RedisPositionStore (OCP — mismo Protocol).

Extraído de portfolio_service.py (SRP — un archivo, una responsabilidad).

Principios: SRP · OCP · DIP · KISS
"""

from __future__ import annotations

import threading
from typing import Optional

from portfolio.models.position import PositionIdCollisionError, PositionSnapshot


class InMemoryPositionStore:
    """
    Implementación in-memory de PositionStore.

    Thread-safe via threading.Lock.
    No persiste entre reinicios — solo para paper trading y tests.
    """

    def __init__(self) -> None:
        self._positions: dict[str, PositionSnapshot] = {}
        self._lock = threading.Lock()

    def save(self, position: PositionSnapshot) -> None:
        """Persiste o actualiza una posición.

        Unicidad del order_id (B-16): si ya existe una posición DIFERENTE
        con el mismo order_id, eleva PositionIdCollisionError en vez de
        sobrescribirla en silencio. El mismo order_id solo puede representar
        la misma posición (mismo symbol/exchange/side).
        """
        with self._lock:
            existing = self._positions.get(position.order_id)
            if existing is not None and (
                existing.symbol != position.symbol
                or existing.exchange != position.exchange
                or existing.side != position.side
            ):
                raise PositionIdCollisionError(order_id=position.order_id, existing=existing, incoming=position)
            self._positions[position.order_id] = position

    def delete(self, order_id: str) -> None:
        """Elimina una posición por order_id (al cerrar)."""
        with self._lock:
            self._positions.pop(order_id, None)

    def get(self, order_id: str) -> Optional[PositionSnapshot]:
        """Recupera una posición por order_id. None si no existe."""
        return self._positions.get(order_id)

    def all(self) -> list[PositionSnapshot]:
        """Todas las posiciones abiertas."""
        with self._lock:
            return list(self._positions.values())

    def clear(self) -> None:
        """Elimina todas las posiciones (reset de sesión)."""
        with self._lock:
            self._positions.clear()

    def __repr__(self) -> str:
        return f"InMemoryPositionStore(positions={len(self._positions)})"
