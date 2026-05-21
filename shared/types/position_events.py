# -*- coding: utf-8 -*-
"""
shared/types/position_events.py
=================================

Eventos de dominio relacionados con el ciclo de vida de posiciones.

Frozen dataclasses — inmutables, sin dependencias externas.

DDD — Eventos de dominio: representan hechos que ocurrieron, inmutables.

Fail-Fast — Los factory methods validan ``side`` contra el Literal
permitido antes de construir el dataclass. Un side inválido indica
un bug en el caller — debe fallar en el punto de origen, no en runtime.

Principios: DDD · SSOT · Fail-Fast · KISS
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal, Optional

# SSOT: tipo canónico de lado de posición.
PositionSide = Literal["long", "short"]
_VALID_POSITION_SIDES: frozenset[str] = frozenset({"long", "short"})


def _validate_position_side(side: str, caller: str) -> PositionSide:
    """
    Valida y retorna ``side`` como PositionSide.

    Fail-Fast: lanza ValueError si ``side`` no es "long" | "short".
    Centralizado aquí para que todos los factory methods sean DRY.

    Parameters
    ----------
    side   : Valor a validar (puede venir de CCXT como str arbitrario).
    caller : Nombre del factory method para mensaje de error contextual.

    Returns
    -------
    PositionSide validado.
    """
    normalized = side.lower().strip() if isinstance(side, str) else ""
    if normalized not in _VALID_POSITION_SIDES:
        raise ValueError(
            f"{caller}: side debe ser 'long' | 'short', "
            f"recibido: {side!r}. "
            "Verificar que el caller normalice el lado antes de crear el evento."
        )
    return normalized  # type: ignore[return-value]


@dataclass(frozen=True)
class PositionOpened:
    """
    Posición abierta tras un BUY filled.

    Publicado por: PortfolioService.open_position()
    Consumido por: risk/, observability, alerting
    """
    order_id:    str
    symbol:      str
    exchange:    str
    side:        PositionSide
    entry_price: float
    size_pct:    float
    opened_at:   datetime

    @classmethod
    def now(
        cls,
        order_id:    str,
        symbol:      str,
        exchange:    str,
        side:        str,
        entry_price: float,
        size_pct:    float,
    ) -> "PositionOpened":
        """
        Factory con timestamp UTC automático.

        Fail-Fast: valida ``side`` antes de construir.
        Acepta ``str`` para compatibilidad con callers externos (CCXT, etc.)
        que no conocen el Literal; normaliza internamente.
        """
        return cls(
            order_id    = order_id,
            symbol      = symbol,
            exchange    = exchange,
            side        = _validate_position_side(side, "PositionOpened.now"),
            entry_price = entry_price,
            size_pct    = size_pct,
            opened_at   = datetime.now(timezone.utc),
        )

    def __str__(self) -> str:
        return (
            f"PositionOpened({self.side.upper()} {self.symbol}"
            f" @ {self.entry_price:.4f} size={self.size_pct:.1%})"
        )


@dataclass(frozen=True)
class PositionClosed:
    """
    Posición cerrada tras un SELL filled.

    Publicado por: PortfolioService.close_position()
    Consumido por: analytics, TradeTracker, observability
    """
    order_id:    str
    symbol:      str
    exchange:    str
    side:        PositionSide
    entry_price: float
    exit_price:  float
    size_pct:    float
    pnl_pct:     float
    opened_at:   datetime
    closed_at:   datetime

    @classmethod
    def from_positions(
        cls,
        order_id:    str,
        symbol:      str,
        exchange:    str,
        side:        PositionSide,
        entry_price: float,
        exit_price:  float,
        size_pct:    float,
        opened_at:   datetime,
        closed_at:   Optional[datetime] = None,
    ) -> "PositionClosed":
        """
        Factory desde una posición abierta existente.

        Fail-Fast: lanza ValueError si entry_price <= 0 (protección contra
        corrupción de datos que produciría pnl_pct infinito o NaN).
        """
        if entry_price <= 0:
            raise ValueError(
                "PositionClosed.from_positions: entry_price debe ser > 0, "
                f"recibido: {entry_price!r} — posible corrupción de datos."
            )
        ts      = closed_at or datetime.now(timezone.utc)
        pnl_pct = (exit_price - entry_price) / entry_price
        return cls(
            order_id    = order_id,
            symbol      = symbol,
            exchange    = exchange,
            side        = side,
            entry_price = entry_price,
            exit_price  = exit_price,
            size_pct    = size_pct,
            pnl_pct     = pnl_pct,
            opened_at   = opened_at,
            closed_at   = ts,
        )

    @property
    def is_winner(self) -> bool:
        return self.pnl_pct > 0.0

    def __str__(self) -> str:
        direction = "WIN" if self.is_winner else "LOSS"
        return (
            f"PositionClosed({direction} {self.symbol}"
            f" pnl={self.pnl_pct:+.2%} entry={self.entry_price:.4f}"
            f" exit={self.exit_price:.4f})"
        )
