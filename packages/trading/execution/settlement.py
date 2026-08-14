# -*- coding: utf-8 -*-
"""
trading/execution/settlement.py
===============================

Settlement canónico — resultado económico de un fill/cierre.

Un solo settlement es generado por OMS._fill y consume downstream
(TradeTracker, TradeRecord, RiskManager, PerformanceEngine) sin que
cada componente recalcule P&L de manera distinta.

Principios: SSOT · KISS · DRY · ADR-0025/0026/0027
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class FeeStatus(str, Enum):
    """Taxonomía de comisiones ADR-0026: KNOWN/UNKNOWN/PROVISIONAL/FINAL."""

    KNOWN = "known"
    UNKNOWN = "unknown"
    PROVISIONAL = "provisional"
    FINAL = "final"


@dataclass(frozen=True)
class Settlement:
    """
    Resultado canónico del cierre de una posición LONG.

    Generado UNA SOLA VEZ en OMS._fill.
    Consumido por: TradeTracker, TradeRecord, RiskManager, PerformanceEngine.

    Campos
    ------
    order_id    : order_id del fill que produjo el cierre
    symbol      : par de trading
    closed_qty  : cantidad realmente cerrada (ej. 1.5 de 2.0 mantenidos)
    avg_entry_price : WAC (weighted average cost) antes del cierre
    exit_price  : precio real del fill SELL
    gross_realized_usd : realized_pnl_bruto = closed_qty * (exit_price - avg_entry_price)
    fee_amount_usd  : costo de comisiones en USD (None si UNKNOWN)
    fee_currency    : moneda de las comisiones (None si no disponible / GAP F7)
    fee_status      : estado de la fee según ADR-0026
    net_realized_usd : realized_pnl_net = gross - fees cuando fee_status != UNKNOWN
    remaining_qty   : cantidad que queda abierta tras este cierre (0.0 = full close)
    """

    order_id: str
    symbol: str
    closed_qty: float
    avg_entry_price: float
    exit_price: float
    gross_realized_usd: float
    fee_amount_usd: Optional[float] = None
    fee_currency: Optional[str] = None
    fee_status: FeeStatus = FeeStatus.UNKNOWN
    net_realized_usd: Optional[float] = None
    remaining_qty: float = 0.0

    @classmethod
    def compute(
        cls,
        *,
        order_id: str,
        symbol: str,
        closed_qty: float,
        avg_entry_price: float,
        exit_price: float,
        fee_amount_usd: Optional[float] = None,
        fee_currency: Optional[str] = None,
        fee_status: FeeStatus = FeeStatus.UNKNOWN,
    ) -> "Settlement":
        """
        Factory para construir un Settlement a partir de los parámetros
        económicos reales del fill.

        La fórmula canónica es:
            gross_realized_usd = closed_qty * (exit_price - avg_entry_price)

        Si fee_status permite calcular net:
            net_realized_usd = gross_realized_usd - fee_amount_usd
        Si fee_status es UNKNOWN:
            net_realized_usd = None (UNKNOWN ≠ ZERO, no se inventa)
        """
        gross = closed_qty * (exit_price - avg_entry_price)
        net: Optional[float] = None
        if fee_status in (FeeStatus.KNOWN, FeeStatus.PROVISIONAL) and fee_amount_usd is not None:
            net = gross - fee_amount_usd

        remaining = closed_qty  # el caller (OMS) informará remaining después de restar

        return cls(
            order_id=order_id,
            symbol=symbol,
            closed_qty=closed_qty,
            avg_entry_price=avg_entry_price,
            exit_price=exit_price,
            gross_realized_usd=gross,
            fee_amount_usd=fee_amount_usd,
            fee_currency=fee_currency,
            fee_status=fee_status,
            net_realized_usd=net,
            remaining_qty=remaining,
        )

    @property
    def realized_pnl_pct(self) -> float:
        """Porcentaje derivado del P&L bruto: (exit - avg) / avg."""
        if self.avg_entry_price == 0:
            return 0.0
        return (self.exit_price - self.avg_entry_price) / self.avg_entry_price

    @property
    def realized_pnl_pct_net(self) -> Optional[float]:
        """Porcentaje neto si las fees permiten el cálculo."""
        if self.net_realized_usd is None or self.avg_entry_price == 0:
            return None
        return self.net_realized_usd / (self.closed_qty * self.avg_entry_price)


__all__ = ["Settlement", "FeeStatus"]
