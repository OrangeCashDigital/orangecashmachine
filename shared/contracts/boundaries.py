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

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from shared.enums import SignalDirection

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


# =============================================================================
# trading -> portfolio  (rebalanceo, ADR-0011)
# =============================================================================


@runtime_checkable
class RebalancePort(Protocol):
    """
    Contrato que trading espera de portfolio para calcular rebalanceo.

    Implementado por: portfolio.services.rebalance_service.RebalanceService.
    trading no importa RebalanceService directamente ni conoce su __init__
    (drift_threshold/min_delta_pct son detalle interno de portfolio).

    ADR-0011: decisión de delegación. portfolio conserva la propiedad
    exclusiva del estado de posiciones (BC-43/ADR-0006).
    """

    def rebalance(
        self,
        state: Any,
        targets: dict[str, float],
        trigger: str = "manual",
    ) -> list[Any]:
        """
        Calcula señales de rebalanceo dado el estado actual y los targets.

        state  : duck-type de portfolio.models.position.PortfolioState.
                 Tipado Any — shared no importa portfolio (BC-01/BC-34,
                 contrato de import-linter, ni siquiera bajo TYPE_CHECKING).
        Returns: duck-type de list[portfolio.services.rebalance_service.RebalanceSignal].

        SafeOps: nunca lanza — retorna lista vacía en error.
        """
        ...

    def validate_targets(self, targets: dict[str, float]) -> tuple[bool, str]:
        """
        Valida que los targets sean coherentes.

        Returns (valid, error_message). error_message vacío si valid=True.
        """
        ...


# =============================================================================
# portfolio -> trading  (RiskManager consume vía port — ADR-0030, B-MD-009)
# =============================================================================


@dataclass(frozen=True)
class BalanceSnapshot:
    """
    Estado del balance de una cuenta en un instante (ADR-0030, B-MD-009).

    Snapshot inmutable del saldo. `age_ms(now_ms)` calcula la edad para
    freshness (B-MD-001). El consumidor (RiskManager) valida freshness
    antes de usar el snapshot para sizing/drawdown.

    Atributos
    ---------
    currency              — "USDT", "USD", "BTC", etc.
    total                 — walletBalance (equity total en USD)
    available_operational — totalAvailableBalance (UNIFIED, disponible operativo)
    locked                — totalOrderIM + totalPositionIM
    unrealised_pnl        — P&L no realizado
    read_at_ms            — timestamp de lectura (epoch ms, UTC)
    source                — "bybit" | "test" | ... (trazabilidad)
    """

    currency: str
    total: float
    available_operational: float
    locked: float
    unrealised_pnl: float
    read_at_ms: int
    source: str = "unknown"

    def age_ms(self, now_ms: int) -> int:
        """Edad del snapshot en milisegundos. Caller pasa `now_ms` para testear sin reloj real."""
        return now_ms - self.read_at_ms


@runtime_checkable
class BalancePort(Protocol):
    """
    Contrato de consulta de balance real del exchange (ADR-0030, B-MD-009).

    SSOT: el balance vive en Portfolio. RiskManager consume vía este port,
    nunca directamente del exchange (evita segunda fuente de verdad).

    Implementado por: _BybitBalanceSource en portfolio/bootstrap/composition_root.py
    (BC-50: único punto autorizado a importar CCXT en portfolio).
    Consumido por: RiskManager (sizing/drawdown), PortfolioReconciler.

    Fail-closed: si no puede leer, lanza. NO retorna None ni valor mágico.
    """

    def get_balance(self, currency: str = "USDT") -> BalanceSnapshot:
        """Retorna snapshot del balance. Fail-closed: lanza si no puede leer."""
        ...
