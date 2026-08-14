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


class PositionIdCollisionError(Exception):
    """
    Dos posiciones distintas comparten el mismo order_id.

    El order_id es la clave única del PositionStore. Una colisión significa
    que el segundo save() sobrescribiría una posición abierta por otra
    (overwrite silencioso = riesgo de portfolio). Los stores deben elevar
    este error en lugar de sobrescribir (B-16 / H-08).
    """

    def __init__(self, order_id: str, existing: "PositionSnapshot", incoming: "PositionSnapshot") -> None:
        self.order_id = order_id
        self.existing = existing
        self.incoming = incoming
        super().__init__(
            f"PositionIdCollisionError: order_id={order_id!r} ya usado por "
            f"{existing.symbol}/{existing.side} e intentado por "
            f"{incoming.symbol}/{incoming.side}"
        )


@dataclass(frozen=True)
class PositionSnapshot:
    """
    Estado puntual de una posición abierta.

    ADR-0025 (F4a/F4b): la unidad de la posición es la cantidad económica
    (``quantity``), no el notional% (``size_pct``). ``avg_entry`` es el
    weighted average entry cost: en multi-entry se acumula como
    ``avg = Σ(qty_i × price_i) / Σqty_i`` y se preserva en cierres parciales.

    Campos
    ------
    symbol       : par normalizado  (e.g. "BTC/USDT")
    exchange     : exchange de origen
    side         : "long" | "short"
    quantity     : cantidad económicamente ejecutada y mantenida (unidades base)
    avg_entry    : weighted average entry cost (fill real; nunca signal.price)
    size_pct     : % del capital asignado (pierna de apertura en multi-entry)
    entry_at     : timestamp UTC de apertura
    order_id     : correlación con Order.order_id de la pierna de apertura
    current_price: mark/valuation price (para PnL no realizado); None si no
                   disponible. Nunca sustituye al execution price (ADR-0025 Q-C).
    """

    symbol: str
    exchange: str
    side: str  # "long" | "short"
    quantity: float  # > 0 — cantidad ejecutada real (INV-01)
    avg_entry: float  # > 0 — weighted average entry cost (INV-04)
    size_pct: float  # ∈ (0.0, 1.0]
    entry_at: datetime
    order_id: str
    current_price: Optional[float] = None

    def __post_init__(self) -> None:
        if self.side not in ("long", "short"):
            raise ValueError(f"PositionSnapshot.side debe ser 'long' o 'short', recibido: {self.side!r}")
        if self.quantity <= 0.0:
            raise ValueError(f"PositionSnapshot.quantity debe ser positivo (ejecutado real), recibido: {self.quantity}")
        if self.avg_entry <= 0.0:
            raise ValueError(f"PositionSnapshot.avg_entry debe ser positivo, recibido: {self.avg_entry}")
        if not (0.0 < self.size_pct <= 1.0):
            raise ValueError(f"PositionSnapshot.size_pct debe estar en (0, 1], recibido: {self.size_pct}")

    @property
    def cost_basis(self) -> float:
        """Base de coste en moneda quote: ``quantity × avg_entry`` (sin fees)."""
        return self.quantity * self.avg_entry

    @property
    def unrealized_pnl_pct(self) -> Optional[float]:
        """
        PnL no realizado (pct) si current_price está disponible.

        Long:  (current - avg_entry) / avg_entry
        Short: (avg_entry - current) / avg_entry
        """
        if self.current_price is None or self.avg_entry <= 0:
            return None
        if self.side == "long":
            return (self.current_price - self.avg_entry) / self.avg_entry
        return (self.avg_entry - self.current_price) / self.avg_entry

    @property
    def unrealized_pnl_usd(self) -> Optional[float]:
        """
        PnL no realizado en USD si current_price está disponible.

        Long:  quantity × (current - avg_entry)
        Short: quantity × (avg_entry - current)
        """
        if self.current_price is None:
            return None
        if self.side == "long":
            return self.quantity * (self.current_price - self.avg_entry)
        return self.quantity * (self.avg_entry - self.current_price)

    def __str__(self) -> str:
        pnl = self.unrealized_pnl_pct
        pnl_str = f" upnl={pnl:+.2%}" if pnl is not None else ""
        return (
            f"Position({self.side.upper()} {self.symbol}@{self.exchange}"
            f" qty={self.quantity:.6f} avg={self.avg_entry:.4f} size={self.size_pct:.1%}{pnl_str})"
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

    positions: tuple[PositionSnapshot, ...]  # tuple para garantizar inmutabilidad
    capital_usd: float
    as_of: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

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
