# -*- coding: utf-8 -*-
"""
portfolio/models/position.py
=============================

PositionSnapshot — estado puntual de una posición abierta.
PortfolioState   — snapshot completo del portfolio en un instante.

Ambos son value objects inmutables (frozen=True).
Representan hechos — no se modifican, se reemplazan.

Principios: DDD · SSOT · Fail-Fast · KISS
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass(frozen=True)
class PositionSnapshot:
    """
    Estado puntual de una posición abierta.

    Campos
    ------
    symbol       : par normalizado  (e.g. "BTC/USDT")
    exchange     : exchange de origen
    side         : "long" | "short"
    entry_price  : precio de entrada (fill_price del BUY)
    size_pct     : % del capital asignado
    entry_at     : timestamp UTC de apertura
    order_id     : correlación con Order.order_id
    current_price: precio actual (para PnL no realizado); None si no disponible
    """
    symbol:        str
    exchange:      str
    side:          str             # "long" | "short"
    entry_price:   float
    size_pct:      float           # ∈ (0.0, 1.0]
    entry_at:      datetime
    order_id:      str
    current_price: Optional[float] = None

    def __post_init__(self) -> None:
        if self.side not in ("long", "short"):
            raise ValueError(f"PositionSnapshot.side debe ser 'long' o 'short', recibido: {self.side!r}")
        if not (0.0 < self.size_pct <= 1.0):
            raise ValueError(f"PositionSnapshot.size_pct debe estar en (0, 1], recibido: {self.size_pct}")
        if self.entry_price <= 0.0:
            raise ValueError(f"PositionSnapshot.entry_price debe ser positivo, recibido: {self.entry_price}")

    @property
    def unrealized_pnl_pct(self) -> Optional[float]:
        """
        PnL no realizado si current_price está disponible.

        Long:  (current - entry) / entry
        Short: (entry - current) / entry
        """
        if self.current_price is None or self.entry_price <= 0:
            return None
        if self.side == "long":
            return (self.current_price - self.entry_price) / self.entry_price
        return (self.entry_price - self.current_price) / self.entry_price

    def __str__(self) -> str:
        pnl = self.unrealized_pnl_pct
        pnl_str = f" upnl={pnl:+.2%}" if pnl is not None else ""
        return (
            f"Position({self.side.upper()} {self.symbol}@{self.exchange}"
            f" entry={self.entry_price:.4f} size={self.size_pct:.1%}{pnl_str})"
        )


@dataclass(frozen=True)
class PortfolioState:
    """
    Snapshot completo del portfolio en un instante.

    Inmutable — generado por PortfolioService.snapshot().
    El caller recibe este objeto y no puede mutarlo.

    Campos
    ------
    positions     : posiciones abiertas en este momento
    capital_usd   : capital total del portfolio
    as_of         : timestamp del snapshot
    total_exposure: suma de size_pct de todas las posiciones abiertas
    """
    positions:      tuple[PositionSnapshot, ...]  # tuple para garantizar inmutabilidad
    capital_usd:    float
    as_of:          datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        if self.capital_usd <= 0:
            raise ValueError(f"PortfolioState.capital_usd debe ser positivo, recibido: {self.capital_usd}")

    @property
    def total_exposure(self) -> float:
        """Exposición total: suma de size_pct de todas las posiciones."""
        return sum(p.size_pct for p in self.positions)

    @property
    def open_count(self) -> int:
        return len(self.positions)

    @property
    def is_flat(self) -> bool:
        """True si no hay posiciones abiertas."""
        return len(self.positions) == 0

    def by_symbol(self, symbol: str) -> list[PositionSnapshot]:
        """Filtra posiciones por símbolo."""
        return [p for p in self.positions if p.symbol == symbol]

    def by_exchange(self, exchange: str) -> list[PositionSnapshot]:
        """Filtra posiciones por exchange."""
        return [p for p in self.positions if p.exchange == exchange]

    def __str__(self) -> str:
        return (
            f"PortfolioState(positions={self.open_count}"
            f" exposure={self.total_exposure:.1%}"
            f" capital={self.capital_usd:.0f}USD)"
        )
