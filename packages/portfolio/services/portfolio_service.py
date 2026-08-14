# -*- coding: utf-8 -*-
"""
portfolio/services/portfolio_service.py
=========================================

PortfolioService — gestión de posiciones abiertas.

Responsabilidad única (SRP)
---------------------------
Mantener el estado actual de qué posiciones están abiertas,
quién las abrió y con qué tamaño.

Integración con OMS
-------------------
PortfolioService se conecta al OMS vía callbacks:
  on_fill   → open_position()   (BUY filled)
  on_fill   → close_position()  (SELL filled)

No genera señales, no valida riesgo, no accede a exchanges.

Thread-safety
-------------
Delegada al PositionStore. InMemoryPositionStore usa threading.Lock.
snapshot() devuelve una copia inmutable — el caller no puede
corromper el estado interno.

SafeOps
-------
- open_position / close_position nunca lanzan — errores logueados.
- snapshot() nunca lanza — retorna PortfolioState vacío en caso de error.

Stores disponibles (OCP — mismo Protocol, intercambiables):
  InMemoryPositionStore  → paper trading / tests
  RedisPositionStore     → producción (cross-restart)

Principios: SOLID · DDD · SafeOps · KISS · SRP
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from loguru import logger

from portfolio.models.position import PortfolioState, PositionSnapshot
from portfolio.ports.position_store import PositionStore


class PortfolioService:
    """
    Gestiona el ciclo de vida de posiciones abiertas.

    Parameters
    ----------
    capital_usd : float         — capital total del portfolio
    store       : PositionStore — backend de persistencia (obligatorio,
                                  sin default — ver BC-43). Paper/tests:
                                  InMemoryPositionStore. Produccion: RedisPositionStore.
    exchange    : str           — exchange principal (para logging)
    """

    def __init__(
        self,
        capital_usd: float,
        store: PositionStore,
        exchange: str = "unknown",
    ) -> None:
        """
        Parameters
        ----------
        store : PositionStore — obligatorio (DIP). La elección de backend
                                 (InMemoryPositionStore vs RedisPositionStore)
                                 es responsabilidad exclusiva del Composition
                                 Root (portfolio.bootstrap.composition_root) —
                                 nunca de PortfolioService. Ver BC-43.
        """
        if capital_usd <= 0:
            raise ValueError(f"PortfolioService: capital_usd debe ser positivo, recibido: {capital_usd}")
        self._capital_usd = capital_usd
        self._store = store
        self._exchange = exchange
        self._log = logger.bind(component="PortfolioService", exchange=exchange)

    # ------------------------------------------------------------------
    # Mutación — llamar desde callbacks OMS
    # ------------------------------------------------------------------

    def open_position(
        self,
        order_id: str,
        symbol: str,
        side: str,
        avg_entry: float,
        size_pct: float,
        quantity: Optional[float],
        entry_at: Optional[datetime] = None,
    ) -> None:
        """
        Registra apertura (o ampliación) de posición.

        Llamar desde OMS.on_fill cuando side == BUY.

        ADR-0025 (F4a/F4b):
        - ``quantity`` es la cantidad económicamente ejecutada del fill real
          (INV-01); nunca la cantidad pedida ni size_pct.
        - ``avg_entry`` es el fill real del leg (nunca signal.price, INV-10).
        - Multi-entry: si ya hay una posición abierta para el mismo
          symbol+side, se acumula en un único weighted average cost
          (qty += qty_i, basis += qty_i×price_i, avg = basis/qty). La clave
          de la posición (order_id/entry_at/size_pct) queda la de la pierna
          de apertura. Limitación documentada: la posición se agrega por
          símbolo; no se rastrea por lotes (WAC, ADR-0025).

        SafeOps: nunca lanza — errores logueados.
        """
        if quantity is None or quantity <= 0:
            self._log.error(
                "open_position: quantity debe ser > 0 (ejecutado real) | order={} qty={}",
                order_id,
                quantity,
            )
            return
        try:
            existing = next(
                (p for p in self._store.all() if p.symbol == symbol and p.side == side),
                None,
            )
            if existing is not None:
                new_qty = existing.quantity + quantity
                new_avg = (existing.quantity * existing.avg_entry + quantity * avg_entry) / new_qty
                position = PositionSnapshot(
                    symbol=existing.symbol,
                    exchange=existing.exchange,
                    side=existing.side,
                    quantity=new_qty,
                    avg_entry=new_avg,
                    size_pct=existing.size_pct,
                    entry_at=existing.entry_at,
                    order_id=existing.order_id,
                    current_price=existing.current_price,
                )
                self._store.save(position)
                self._log.info(
                    "position_merged | {} {} qty={:.6f} avg={:.4f} size={:.1%} order={}",
                    side.upper(),
                    symbol,
                    position.quantity,
                    position.avg_entry,
                    position.size_pct,
                    existing.order_id,
                )
                return
            position = PositionSnapshot(
                symbol=symbol,
                exchange=self._exchange,
                side=side,
                quantity=quantity,
                avg_entry=avg_entry,
                size_pct=size_pct,
                entry_at=entry_at or datetime.now(timezone.utc),
                order_id=order_id,
            )
            self._store.save(position)
            self._log.info(
                "position_opened | {} {} qty={:.6f} avg={:.4f} size={:.1%} order={}",
                side.upper(),
                symbol,
                position.quantity,
                position.avg_entry,
                position.size_pct,
                order_id,
            )
        except Exception as exc:
            self._log.error("open_position_error | order={} err={}", order_id, exc)

    def close_position(
        self,
        order_id: str,
        quantity: Optional[float] = None,
    ) -> tuple[Optional[PositionSnapshot], float]:
        """
        Cierra (total o parcialmente) una posición por order_id.

        Llamar desde OMS.on_fill cuando side == SELL.

        ADR-0025 (F4a/F4b): un cierre parcial reduce ``quantity`` en la
        cantidad realmente cerrada, preserva ``avg_entry`` (WAC) y mantiene
        la posición abierta con su cost basis coherente; la posición se
        elimina solo cuando la cantidad restante llega a 0.

        Parameters
        ----------
        order_id : key de apertura de la posición (order_id del BUY).
        quantity : cantidad a cerrar en unidades base. None = cierre completo
                   (comportamiento legacy).

        Returns
        -------
        (closed, remaining) — la porción cerrada (con quantity = cantidad
        cerrada y avg_entry = WAC al cierre, datos para el realized P&L
        `closed_qty × (exit − avg)`), y la cantidad restante tras el cierre
        (0.0 en cierre completo). `closed` es None si la posición no existía
        o la persistencia falló (SafeOps).
        """
        try:
            position = self._store.get(order_id)
            if position is None:
                self._log.warning("close_position_not_found | order={}", order_id)
                return (None, 0.0)
            if quantity is not None and quantity > position.quantity:
                self._log.warning(
                    "close_position: qty a cerrar excede la posición — cierre completo | "
                    "order={} close_qty={:.6f} pos_qty={:.6f}",
                    order_id,
                    quantity,
                    position.quantity,
                )
            closed_qty = position.quantity if quantity is None else min(quantity, position.quantity)
            remaining = position.quantity - closed_qty

            closed = PositionSnapshot(
                symbol=position.symbol,
                exchange=position.exchange,
                side=position.side,
                quantity=closed_qty,
                avg_entry=position.avg_entry,
                size_pct=position.size_pct,
                entry_at=position.entry_at,
                order_id=position.order_id,
                current_price=position.current_price,
            )
            if remaining <= 1e-12:
                self._store.delete(order_id)
                self._log.info(
                    "position_closed | {} {} qty={:.6f} avg={:.4f} order={}",
                    position.side.upper(),
                    position.symbol,
                    closed_qty,
                    position.avg_entry,
                    order_id,
                )
            else:
                updated = PositionSnapshot(
                    symbol=position.symbol,
                    exchange=position.exchange,
                    side=position.side,
                    quantity=remaining,
                    avg_entry=position.avg_entry,
                    size_pct=position.size_pct,
                    entry_at=position.entry_at,
                    order_id=position.order_id,
                    current_price=position.current_price,
                )
                self._store.save(updated)
                self._log.info(
                    "position_partially_closed | {} {} closed_qty={:.6f} remaining={:.6f} avg={:.4f} order={}",
                    position.side.upper(),
                    position.symbol,
                    closed_qty,
                    remaining,
                    position.avg_entry,
                    order_id,
                )
            return (closed, remaining)
        except Exception as exc:
            self._log.error("close_position_error | order={} err={}", order_id, exc)
            return (None, 0.0)

    # ------------------------------------------------------------------
    # Consulta — solo lectura, retorna inmutables
    # ------------------------------------------------------------------

    def snapshot(self) -> PortfolioState:
        """
        Snapshot inmutable del estado actual del portfolio.

        SafeOps: nunca lanza — retorna estado vacío en caso de error.
        """
        try:
            positions = self._store.all()
            return PortfolioState(
                positions=tuple(positions),
                capital_usd=self._capital_usd,
            )
        except Exception as exc:
            self._log.error("snapshot_error | {}", exc)
            return PortfolioState(
                positions=(),
                capital_usd=self._capital_usd,
            )

    @property
    def open_count(self) -> int:
        """Número de posiciones abiertas. SafeOps: nunca lanza."""
        try:
            return len(self._store.all())
        except Exception:
            return 0

    @property
    def total_exposure(self) -> float:
        """Exposición total (suma de size_pct). SafeOps: nunca lanza."""
        try:
            return sum(p.size_pct for p in self._store.all())
        except Exception:
            return 0.0

    def state(self) -> dict:
        """Estado observable para logging y métricas."""
        try:
            snap = self.snapshot()
            return {
                "open_positions": snap.open_count,
                "total_exposure": round(snap.total_exposure, 4),
                "capital_usd": self._capital_usd,
                "is_flat": snap.is_flat,
            }
        except Exception:
            return {"open_positions": 0, "total_exposure": 0.0}

    def __repr__(self) -> str:
        return (
            f"PortfolioService(open={self.open_count}"
            f" exposure={self.total_exposure:.1%}"
            f" capital={self._capital_usd:.0f})"
        )
