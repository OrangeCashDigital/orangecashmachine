# -*- coding: utf-8 -*-
"""
trading/analytics/trade_record.py
===================================

TradeRecord — unidad atómica de historial de trading.

Un trade = cierre parcial/completo de una posición (entry + exit).
Inmutable una vez cerrado — frozen dataclass.

F3 — semántica canónica (ADR-0025/0026/0027)
--------------------------------------------
El realized P&L canónico es MONETARIO y quantity-aware:

    gross_realized_usd = closed_qty * (exit_price - avg_entry_price/WAC)

El TradeRecord CONSUME el settlement canónico (Order.settlement) generado
UNA SOLA VEZ en OMS._fill. No reconstruye P&L desde (exit - entry)/entry.

- `pnl_pct` es métrica DERIVADA del gross monetario, no una segunda fuente.
- `net_realized_usd` solo existe si las fees son conocidas (ADR-0026).
  fee UNKNOWN ≠ fee 0: no se asume zero; net permanece UNKNOWN (None).
- `net_pnl_pct` es None cuando las fees son UNKNOWN (net indeterminado).

Principios: SOLID · KISS · SafeOps · ADR-0025/0026/0027
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from trading.execution.settlement import FeeStatus, Settlement


@dataclass(frozen=True)
class TradeRecord:
    """
    Registro de un cierre de trade (entry + exit), quantity-aware.

    Campos de identidad
    -------------------
    trade_id    : correlación con Order.order_id de la entrada
    symbol      : par (e.g. "BTC/USDT")
    exchange    : origen del dato
    timeframe   : timeframe de la señal

    Economía canónica (F3)
    ----------------------
    closed_qty        : cantidad realmente cerrada en este settlement
    avg_entry_price   : WAC (weighted average cost) antes del cierre
    exit_price        : precio real del fill SELL
    gross_realized_usd: closed_qty * (exit_price - avg_entry_price)
    net_realized_usd  : gross - fees cuando fee_status ∈ {KNOWN, PROVISIONAL};
                        None (UNKNOWN) cuando las fees no son determinables

    Métricas derivadas
    ------------------
    pnl_pct     : (gross / (closed_qty * avg_entry_price))  — DERIVADA
    net_pnl_pct : net_realized_usd / (closed_qty * avg_entry_price)
                  cuando fee_status permite el cálculo; None si UNKNOWN
    duration_s  : segundos entre entry y exit

    Costes (ADR-0026)
    -----------------
    fee_amount_usd : coste en USD (None si UNKNOWN; NO asumir 0)
    fee_status     : KNOWN | UNKNOWN | PROVISIONAL | FINAL
    """

    # Identidad
    trade_id: str
    symbol: str
    exchange: str
    timeframe: str

    # Ejecución / metadata
    size_pct: float
    entry_at: datetime
    exit_at: datetime
    duration_s: float

    # Economía canónica (F3) — quantity-aware
    closed_qty: float
    avg_entry_price: float
    exit_price: float
    gross_realized_usd: float
    net_realized_usd: Optional[float] = None

    # Costes (ADR-0026)
    fee_amount_usd: Optional[float] = None
    fee_status: FeeStatus = FeeStatus.UNKNOWN

    # ------------------------------------------------------------------
    # Properties derivadas
    # ------------------------------------------------------------------

    @property
    def entry_price(self) -> float:
        """Entry económico = WAC (avg_entry_price), no último BUY."""
        return self.avg_entry_price

    @property
    def pnl_pct(self) -> float:
        """
        P&L bruto porcentual DERIVADO del gross monetario canónico.

        pnl_pct = gross_realized_usd / (closed_qty * avg_entry_price)
                = (exit_price - avg_entry_price) / avg_entry_price
        """
        if self.closed_qty <= 0 or self.avg_entry_price <= 0:
            return 0.0
        return self.gross_realized_usd / (self.closed_qty * self.avg_entry_price)

    @property
    def net_pnl_pct(self) -> Optional[float]:
        """
        P&L neto porcentual.

        Solo existe si las fees están disponibles y permiten el cálculo
        (net_realized_usd no es None). Si las fees son UNKNOWN, net es
        indeterminado → None (no se falsifica UNKNOWN como 0).
        """
        if self.net_realized_usd is None:
            return None
        if self.closed_qty <= 0 or self.avg_entry_price <= 0:
            return None
        return self.net_realized_usd / (self.closed_qty * self.avg_entry_price)

    @property
    def is_winner(self) -> bool:
        """
        Clasificación del trade. Usa net cuando está disponible; si net es
        UNKNOWN (fees), clasifica por el gross monetario (pnl_pct).
        """
        net = self.net_pnl_pct
        return (net if net is not None else self.pnl_pct) > 0.0

    @property
    def pnl_usd(self) -> Optional[float]:
        """
        P&L neto en USD (None si fees UNKNOWN → net indeterminado).
        """
        return self.net_realized_usd

    @property
    def fees_pct(self) -> Optional[float]:
        """
        Costes relativos al basis de la posición (closed_qty × avg).

        DERIVADO — no es una fuente económica independiente.
        None si fee_amount_usd es UNKNOWN (no asumir 0).
        """
        if self.fee_amount_usd is None:
            return None
        if self.closed_qty <= 0 or self.avg_entry_price <= 0:
            return None
        return self.fee_amount_usd / (self.closed_qty * self.avg_entry_price)

    # ------------------------------------------------------------------
    # Factories
    # ------------------------------------------------------------------

    @classmethod
    def from_settlement(
        cls,
        settlement: Settlement,
        *,
        trade_id: str,
        exchange: str,
        timeframe: str,
        size_pct: float,
        entry_at: datetime,
        exit_at: Optional[datetime] = None,
    ) -> "TradeRecord":
        """
        Factory canónico (F3): construye un TradeRecord desde el Settlement
        generado por OMS._fill.

        Toda la economía (closed_qty, avg_entry_price, exit_price, gross,
        net, fee_status) proviene del settlement. NO recalcula P&L.
        """
        closed_at = exit_at or datetime.now(timezone.utc)
        duration_s = (closed_at - entry_at).total_seconds()

        return cls(
            trade_id=trade_id,
            symbol=settlement.symbol,
            exchange=exchange,
            timeframe=timeframe,
            size_pct=size_pct,
            entry_at=entry_at,
            exit_at=closed_at,
            duration_s=duration_s,
            closed_qty=settlement.closed_qty,
            avg_entry_price=settlement.avg_entry_price,
            exit_price=settlement.exit_price,
            gross_realized_usd=settlement.gross_realized_usd,
            net_realized_usd=settlement.net_realized_usd,
            fee_amount_usd=settlement.fee_amount_usd,
            fee_status=settlement.fee_status,
        )

    @classmethod
    def close(
        cls,
        trade_id: str,
        symbol: str,
        exchange: str,
        timeframe: str,
        entry_price: float,
        exit_price: float,
        size_pct: float,
        entry_at: datetime,
        exit_at: Optional[datetime] = None,
        fees: float = 0.0,
        entry_notional: Optional[float] = None,
        closed_qty: float = 1.0,
    ) -> "TradeRecord":
        """
        Factory LEGACY — compatibilidad (caminos que aún no transportan
        Settlement). NO es la fuente económica canónica de F3.

        Deriva gross_realized_usd desde precios y closed_qty:
            gross = closed_qty * (exit_price - entry_price)

        Usar solo en modo defensivo o tests; el camino canónico es
        `from_settlement`.

        fees: coste total del round-trip en USD (0.0 en paper).
        entry_notional: notional de entrada (entry_price * filled_qty del BUY).
        closed_qty: cantidad realmente cerrada (default 1.0 legacy).
        """
        closed_at = exit_at or datetime.now(timezone.utc)
        duration_s = (closed_at - entry_at).total_seconds()

        avg_entry_price = entry_price
        gross_realized_usd = closed_qty * (exit_price - entry_price)

        # fee_status: si fees fue informada (incluso 0.0), es KNOWN.
        fee_status = FeeStatus.KNOWN
        net_realized_usd: Optional[float] = gross_realized_usd - fees

        return cls(
            trade_id=trade_id,
            symbol=symbol,
            exchange=exchange,
            timeframe=timeframe,
            size_pct=size_pct,
            entry_at=entry_at,
            exit_at=closed_at,
            duration_s=duration_s,
            closed_qty=closed_qty,
            avg_entry_price=avg_entry_price,
            exit_price=exit_price,
            gross_realized_usd=gross_realized_usd,
            net_realized_usd=net_realized_usd,
            fee_amount_usd=fees,
            fee_status=fee_status,
        )

    def __str__(self) -> str:
        direction = "WIN" if self.is_winner else "LOSS"
        net_str = f"{self.net_pnl_pct:+.2%}" if self.net_pnl_pct is not None else "UNKNOWN"
        return (
            f"Trade({self.trade_id} {self.symbol} {direction}"
            f" pnl={self.pnl_pct:+.2%} net={net_str}"
            f" gross={self.gross_realized_usd:+.2f}"
            f" qty={self.closed_qty} entry_wac={self.avg_entry_price:.2f}"
            f" exit={self.exit_price:.2f} dur={self.duration_s:.0f}s)"
        )
