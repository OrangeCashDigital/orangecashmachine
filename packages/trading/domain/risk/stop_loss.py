# -*- coding: utf-8 -*-
"""
trading/risk/stop_loss.py
===========================

StopLossEvaluator — evaluación pura de stop-loss sobre posiciones abiertas.

Responsabilidad única (SRP)
---------------------------
Dado el estado de posiciones abiertas y el precio actual, decidir qué
símbolos han cruzado su nivel de stop-loss. No emite órdenes, no accede a
exchanges, no conoce execution (BC-12: trading.risk no importa
trading.execution).

Semántica (S1, aprobada)
-----------------------
- Long  : el stop se cruza cuando current_price <= avg_entry * (1 - default_pct).
- Short : el stop se cruza cuando current_price >= avg_entry * (1 + default_pct).
- avg_entry es el weighted average entry cost (ADR-0025/F4b): el nivel de
  stop se evalúa contra el coste medio real de la posición, no contra la
  última entrada ni contra signal.price.
- Sigue la configuración existente (StopLossConfig.enabled / default_pct);
  no inventa un mecanismo nuevo. La ORDEN de cierre la emite el caller
  (TradingEngine) por el flujo normal del OMS.

Duck-typing de posiciones
-------------------------
`breached()` acepta cualquier iterable de objetos con atributos
`symbol`, `side` y `avg_entry` — p. ej. PositionSnapshot (portfolio),
sin acoplar risk a portfolio (mismo patrón estructural que fill_sync).

Principios: SRP · DIP · KISS · BC-12
"""

from __future__ import annotations

from typing import Any, Iterable, Protocol

from trading.risk.models import StopLossConfig


class StopLossPosition(Protocol):
    """Contrato estructural mínimo de una posición para evaluar stop-loss."""

    symbol: str
    side: str  # "long" | "short"
    avg_entry: float


class SupportsPositionSnapshot(Protocol):
    """Contrato estructural del proveedor de posiciones abiertas.

    Satisfecho por PortfolioService.snapshot() (BC-13: risk no importa
    portfolio — solo estructura).
    """

    def snapshot(self) -> Any: ...


class StopLossEvaluator:
    """
    Evalúa qué posiciones han cruzado su nivel de stop-loss.

    Parameters
    ----------
    config : StopLossConfig — enable / default_pct (SSOT config existente).
    """

    def __init__(self, config: StopLossConfig) -> None:
        self._config = config

    @property
    def enabled(self) -> bool:
        return self._config.enabled

    @property
    def default_pct(self) -> float:
        return self._config.default_pct

    def breached(
        self,
        positions: Iterable[StopLossPosition],
        current_price: float,
    ) -> list[str]:
        """
        Símbolos de posiciones que han cruzado su stop-loss.

        Retorna lista vacía si stop-loss deshabilitado o sin brechas.
        SafeOps: nunca lanza — posiciones malformadas se ignoran.
        """
        if not self._config.enabled or current_price <= 0:
            return []

        pct = self._config.default_pct
        breached: list[str] = []
        for pos in positions:
            try:
                symbol = pos.symbol
                side = pos.side
                entry = float(pos.avg_entry)
            except (AttributeError, TypeError, ValueError):
                continue
            if entry <= 0:
                continue
            if side == "long" and current_price <= entry * (1 - pct):
                breached.append(symbol)
            elif side == "short" and current_price >= entry * (1 + pct):
                breached.append(symbol)
        return breached

    def __repr__(self) -> str:
        return f"StopLossEvaluator(enabled={self._config.enabled} default_pct={self._config.default_pct:.1%})"
