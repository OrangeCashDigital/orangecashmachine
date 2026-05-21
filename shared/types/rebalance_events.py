# -*- coding: utf-8 -*-
"""
shared/types/rebalance_events.py
==================================

Eventos de dominio relacionados con rebalanceo de portfolio.

Frozen dataclasses — inmutables, sin dependencias externas.
Principios: DDD · SSOT · KISS · Fail-Fast
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(frozen=True)
class RebalanceTriggered:
    """
    Rebalanceo iniciado.

    Publicado por: RebalanceService.rebalance()

    Campos
    ------
    trigger     : razón del rebalanceo ("scheduled" | "drift" | "manual")
    targets     : mapeo symbol → target_pct deseado ∈ (0.0, 1.0]
    triggered_at: timestamp UTC

    Nota de inmutabilidad
    ---------------------
    targets es dict[str, float] — mutable por naturaleza en Python.
    frozen=True protege la referencia del campo pero no el contenido
    del dict. Por convención, no modificar targets tras construcción.
    Para inmutabilidad completa usar types.MappingProxyType si se
    requiere enforcement estricto en el futuro.
    """

    trigger: str
    targets: dict[str, float]
    triggered_at: datetime

    @classmethod
    def now(cls, trigger: str, targets: dict[str, float]) -> "RebalanceTriggered":
        return cls(
            trigger=trigger,
            targets=targets,
            triggered_at=datetime.now(timezone.utc),
        )


@dataclass(frozen=True)
class RebalanceCompleted:
    """
    Rebalanceo completado.

    Publicado por: RebalanceService.rebalance() al finalizar

    Campos
    ------
    orders_generated : número de órdenes generadas
    symbols_adjusted : símbolos que recibieron ajuste (tuple para inmutabilidad)
    completed_at     : timestamp UTC
    """

    orders_generated: int
    symbols_adjusted: tuple[str, ...]
    completed_at: datetime

    @classmethod
    def now(
        cls,
        orders_generated: int,
        symbols_adjusted: list[str],
    ) -> "RebalanceCompleted":
        return cls(
            orders_generated=orders_generated,
            symbols_adjusted=tuple(symbols_adjusted),
            completed_at=datetime.now(timezone.utc),
        )
