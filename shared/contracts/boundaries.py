# -*- coding: utf-8 -*-
"""
shared/contracts/boundaries.py
====================

Contratos explícitos entre Bounded Contexts.

SSOT de todos los protocolos que cruzan fronteras de dominio.
Ningun bounded context importa internals de otro — solo desde aqui
o desde market_data.ports/.

Regla: si necesitas pasar datos entre dos BCs, el tipo va aqui.
       si necesitas que un BC llame a otro, el Protocol va aqui.

Principios: DIP · OCP · SSOT
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from shared.kafka.schemas._base import SignalDirection

# =============================================================================
# market_data -> signals
# =============================================================================


@runtime_checkable
class FeatureSource(Protocol):
    """
    Puerto de salida de market_data hacia signals.

    Implementado por: GoldLoader, GoldLoaderAdapter.
    Consumido por: signals (estrategias).
    Desacoplamiento: signals testeable con cualquier mock sin Iceberg.
    """

    def load_features(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        market_type: str = "spot",
        **kwargs: Any,
    ) -> Any:
        """
        Retorna DataFrame OHLCV + features.
        Retorna None o DataFrame vacio si no hay datos.
        """
        ...


# =============================================================================
# signals -> execution  (via TradingEngine)
# =============================================================================


@runtime_checkable
class SignalProtocol(Protocol):
    """
    Contrato minimo que execution espera de un Signal.

    Implementado por: trading.strategies.base.Signal
    (se movera a signals/ en la migracion del monolito).
    """

    symbol: str
    timeframe: str
    direction: SignalDirection
    price: float
    confidence: float
    timestamp: datetime

    @property
    def is_actionable(self) -> bool: ...


# =============================================================================
# execution -> risk
# =============================================================================


@runtime_checkable
class RiskGate(Protocol):
    """
    Contrato que execution espera de risk para filtrar senales.

    Implementado por: trading.risk.manager.RiskManager.
    execution no importa RiskManager directamente en tests.
    """

    @property
    def is_halted(self) -> bool:
        """True si el sistema esta en modo emergencia."""
        ...

    def evaluate(self, signal: Any) -> tuple[bool, str]:
        """
        Evalua si la senal puede ejecutarse.

        Returns: (approved, reason)
        reason es '' si approved=True.
        """
        ...
