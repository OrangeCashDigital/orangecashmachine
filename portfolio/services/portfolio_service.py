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
Todas las mutaciones ocurren bajo _lock (threading.Lock).
snapshot() devuelve una copia inmutable — el caller no puede
corromper el estado interno.

SafeOps
-------
- open_position / close_position nunca lanzan — errores logueados.
- snapshot() nunca lanza — retorna PortfolioState vacío en caso de error.

Principios: SOLID · DDD · SafeOps · KISS
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Optional

from loguru import logger

from portfolio.models.position import PortfolioState, PositionSnapshot
from portfolio.ports.position_store import PositionStore


# ---------------------------------------------------------------------------
# In-memory store — default para paper trading y tests
# ---------------------------------------------------------------------------

class InMemoryPositionStore:
    """
    Implementación in-memory de PositionStore.

    Adecuada para paper trading y tests.
    Para producción, reemplazar por RedisPositionStore (OCP — mismo Protocol).
    """

    def __init__(self) -> None:
        self._positions: dict[str, PositionSnapshot] = {}
        self._lock = threading.Lock()

    def save(self, position: PositionSnapshot) -> None:
        with self._lock:
            self._positions[position.order_id] = position

    def delete(self, order_id: str) -> None:
        with self._lock:
            self._positions.pop(order_id, None)

    def get(self, order_id: str) -> Optional[PositionSnapshot]:
        return self._positions.get(order_id)

    def all(self) -> list[PositionSnapshot]:
        with self._lock:
            return list(self._positions.values())

    def clear(self) -> None:
        with self._lock:
            self._positions.clear()


# ---------------------------------------------------------------------------
# PortfolioService
# ---------------------------------------------------------------------------

class PortfolioService:
    """
    Gestiona el ciclo de vida de posiciones abiertas.

    Parameters
    ----------
    capital_usd : float         — capital total del portfolio
    store       : PositionStore — backend de persistencia (default: in-memory)
    exchange    : str           — exchange principal (para logging)
    """

    def __init__(
        self,
        capital_usd: float,
        store:       Optional[PositionStore] = None,
        exchange:    str = "unknown",
    ) -> None:
        if capital_usd <= 0:
            raise ValueError(f"PortfolioService: capital_usd debe ser positivo, recibido: {capital_usd}")

        self._capital_usd = capital_usd
        self._store       = store or InMemoryPositionStore()
        self._exchange    = exchange
        self._log         = logger.bind(component="PortfolioService", exchange=exchange)

    # ------------------------------------------------------------------
    # Mutación — llamar desde callbacks OMS
    # ------------------------------------------------------------------

    def open_position(
        self,
        order_id:    str,
        symbol:      str,
        side:        str,          # "long" | "short"
        entry_price: float,
        size_pct:    float,
        entry_at:    Optional[datetime] = None,
    ) -> None:
        """
        Registra apertura de posición.

        Llamar desde OMS.on_fill cuando side == BUY.
        SafeOps: nunca lanza — errores logueados.
        """
        try:
            position = PositionSnapshot(
                symbol      = symbol,
                exchange    = self._exchange,
                side        = side,
                entry_price = entry_price,
                size_pct    = size_pct,
                entry_at    = entry_at or datetime.now(timezone.utc),
                order_id    = order_id,
            )
            self._store.save(position)
            self._log.info(
                "Posición abierta | {} {} @ {:.4f} size={:.1%}",
                side.upper(), symbol, entry_price, size_pct,
            )
        except Exception as exc:
            self._log.error("open_position error | order={} {}", order_id, exc)

    def close_position(self, order_id: str) -> Optional[PositionSnapshot]:
        """
        Cierra una posición por order_id.

        Llamar desde OMS.on_fill cuando side == SELL.

        Returns
        -------
        PositionSnapshot que fue cerrada, o None si no existía.
        SafeOps: nunca lanza.
        """
        try:
            position = self._store.get(order_id)
            if position is None:
                self._log.warning("close_position: order_id no encontrado | {}", order_id)
                return None
            self._store.delete(order_id)
            self._log.info(
                "Posición cerrada | {} {} order={}",
                position.side.upper(), position.symbol, order_id,
            )
            return position
        except Exception as exc:
            self._log.error("close_position error | order={} {}", order_id, exc)
            return None

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
                positions   = tuple(positions),
                capital_usd = self._capital_usd,
            )
        except Exception as exc:
            self._log.error("snapshot error | {}", exc)
            return PortfolioState(
                positions   = (),
                capital_usd = self._capital_usd,
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
                "capital_usd":    self._capital_usd,
                "is_flat":        snap.is_flat,
            }
        except Exception:
            return {"open_positions": 0, "total_exposure": 0.0}

    def __repr__(self) -> str:
        return (
            f"PortfolioService(open={self.open_count}"
            f" exposure={self.total_exposure:.1%}"
            f" capital={self._capital_usd:.0f})"
        )
