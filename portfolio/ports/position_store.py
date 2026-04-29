# -*- coding: utf-8 -*-
"""
portfolio/ports/position_store.py
===================================

PositionStore — puerto de persistencia de posiciones.

Implementaciones posibles
--------------------------
  InMemoryPositionStore  — tests y paper trading
  RedisPositionStore     — producción (persistencia cross-restart)

DIP: PortfolioService depende de este Protocol, no de Redis/dict.

Principios: DIP · OCP · SafeOps
"""
from __future__ import annotations

from typing import Optional, Protocol, runtime_checkable

from portfolio.models.position import PositionSnapshot


@runtime_checkable
class PositionStore(Protocol):
    """Contrato de persistencia de posiciones abiertas."""

    def save(self, position: PositionSnapshot) -> None:
        """Persiste o actualiza una posición."""
        ...

    def delete(self, order_id: str) -> None:
        """Elimina una posición por order_id (al cerrar)."""
        ...

    def get(self, order_id: str) -> Optional[PositionSnapshot]:
        """Recupera una posición por order_id. None si no existe."""
        ...

    def all(self) -> list[PositionSnapshot]:
        """Todas las posiciones abiertas."""
        ...

    def clear(self) -> None:
        """Elimina todas las posiciones (reset de sesión)."""
        ...
