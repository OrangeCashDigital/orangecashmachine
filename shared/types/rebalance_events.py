# -*- coding: utf-8 -*-
"""
domain/events/rebalance_events.py
===================================

Eventos de dominio relacionados con rebalanceo de portfolio.

Frozen dataclasses — inmutables, sin dependencias externas.
Principios: DDD · SSOT · KISS
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
    targets     : dict symbol → target_pct deseado
    triggered_at: timestamp UTC
    """
    trigger:      str
    targets:      dict             # symbol → target_pct (float)
    triggered_at: datetime

    @classmethod
    def now(cls, trigger: str, targets: dict) -> "RebalanceTriggered":
        return cls(
            trigger      = trigger,
            targets      = targets,
            triggered_at = datetime.now(timezone.utc),
        )


@dataclass(frozen=True)
class RebalanceCompleted:
    """
    Rebalanceo completado.

    Publicado por: RebalanceService.rebalance() al finalizar
    Campos
    ------
    orders_generated : número de órdenes generadas
    symbols_adjusted : símbolos que recibieron ajuste
    completed_at     : timestamp UTC
    """
    orders_generated: int
    symbols_adjusted: tuple          # tuple para inmutabilidad
    completed_at:     datetime

    @classmethod
    def now(
        cls,
        orders_generated: int,
        symbols_adjusted: list,
    ) -> "RebalanceCompleted":
        return cls(
            orders_generated = orders_generated,
            symbols_adjusted = tuple(symbols_adjusted),
            completed_at     = datetime.now(timezone.utc),
        )
