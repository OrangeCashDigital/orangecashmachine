# -*- coding: utf-8 -*-
"""Protocolos de sincronización de fills para el bounded context de trading."""

from __future__ import annotations

from typing import Optional, Protocol

from trading.domain.models.order import Order


class SupportsOnFill(Protocol):
    """Protocolo que define la interfaz para recibir notificaciones de fills."""

    def on_fill(self, order: Order) -> None:  # noqa: D401


class SupportsPositionSync(Protocol):
    """Protocolo que define la interfaz para sincronización de posiciones."""

    def open_position(
        self,
        *,
        order_id: str,
        symbol: str,
        side: str,
        avg_entry: float,
        size_pct: float,
        entry_at: Optional[datetime],
        quantity: Optional[float],
    ) -> None:  # noqa: D401

    def close_position(
        self,
        order_id: str,
        quantity: Optional[float] = None,
    ) -> tuple[Any, float]:  # noqa: D401
