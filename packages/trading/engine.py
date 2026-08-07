# -*- coding: utf-8 -*-
"""
trading/engine.py
=================

TradingEngine — punto de entrada del sistema de trading.

Responsabilidad
---------------
Orquestar el ciclo completo:
  1. Cargar datos desde GoldStorage
  2. Ejecutar estrategia → señales
  3. Enviar señales al OMS
  4. Respetar el ExecutionGuard

No contiene lógica de riesgo (RiskManager), ni de órdenes (OMS),
ni de estrategia (BaseStrategy). Solo los conecta. (SRP)

Objeto runtime puro (ADR-0012): no construye sus dependencias — el
ensamblaje de Strategy + RiskManager + Executor + OMS vive en
TradingCompositionRoot.assemble_live()/assemble_paper() (ADR-0003).
Los factories build_live()/build_paper() fueron eliminados (2026-08-03).

Principios: SOLID · KISS · DRY · SafeOps
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from trading.risk.manager import RiskDecision

from dataclasses import dataclass, field
from typing import Optional

import polars as pl
from loguru import logger

from ocm.runtime.guard import ExecutionGuard
from shared.contracts.boundaries import (
    FeatureSource,
)  # SSOT — unica definicion del contrato
from trading.execution.order import Order, OrderStatus
from trading.strategies.base import BaseStrategy

# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------


@dataclass
class EngineResult:
    """Resultado de un ciclo run_once()."""

    symbol: str
    timeframe: str
    signals_generated: int = 0
    orders_submitted: int = 0
    orders_filled: int = 0
    orders_rejected: int = 0
    skipped: bool = False
    skip_reason: str = ""
    orders: list[Order] = field(default_factory=list)

    @property
    def status(self) -> str:
        if self.skipped:
            return "skipped"
        if self.orders_filled > 0:
            return "filled"
        if self.signals_generated == 0:
            return "no_signal"
        return "rejected"


# ---------------------------------------------------------------------------
# TradingEngine
# ---------------------------------------------------------------------------


class TradingEngine:
    """
    Orquesta estrategia → riesgo → OMS en un ciclo de trading.

    Parameters
    ----------
    strategy     : BaseStrategy
    oms          : OMS
    data_source  : FeatureSource
    guard        : ExecutionGuard, optional
    exchange     : str
    market_type  : str  — "spot" | "linear" | "inverse"
    """

    def __init__(
        self,
        strategy: BaseStrategy,
        oms,
        data_source: FeatureSource,
        guard: Optional[ExecutionGuard] = None,
        exchange: str = "bybit",
        market_type: str = "spot",
    ) -> None:
        self._strategy = strategy
        self._oms = oms
        self._data_source = data_source
        self._guard = guard
        self._exchange = exchange
        self._market_type = market_type
        self._log = logger.bind(
            engine="TradingEngine",
            strategy=strategy.name,
            exchange=exchange,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_once(self) -> EngineResult:
        """
        Ejecuta un ciclo: datos → señales → OMS.

        Thread-safe. Sincrónico — para uso en Dagster assets o loops.
        """
        s = self._strategy
        result = EngineResult(symbol=s.symbol, timeframe=s.timeframe)

        # Kill switch — propiedad pública stop_reason
        if self._guard and self._guard.should_stop():
            result.skipped = True
            result.skip_reason = f"guard:{self._guard.stop_reason}"
            self._log.warning("run_once bloqueado | reason={}", result.skip_reason)
            return result

        # Cargar datos
        df = self._load_data()
        if df is None or (hasattr(df, "empty") and df.empty):
            result.skipped = True
            result.skip_reason = "no_data"
            self._log.warning(
                "Sin datos | exchange={} symbol={} tf={}",
                self._exchange,
                s.symbol,
                s.timeframe,
            )
            return result

        # Punto único de conversión al framework de estrategias (polars).
        # SSOT: packages/trading/strategies consumen pl.DataFrame; la fuente
        # (FeatureSource.load_features) aún entrega pandas en algunos adapters.
        df = pl.from_pandas(df)

        # Generar señales
        try:
            signals = s.generate_signals(df)
        except Exception as exc:
            result.skipped = True
            result.skip_reason = f"strategy_error:{exc}"
            self._log.error("generate_signals error | {}", exc)
            if self._guard:
                self._guard.record_error(str(exc))
            return result

        result.signals_generated = len(signals)
        self._log.debug(
            "Señales generadas | symbol={} count={}",
            s.symbol,
            len(signals),
        )

        # Enviar al OMS
        for signal in signals:
            order = self._oms.submit(signal)
            if order is None:
                result.orders_rejected += 1
                continue
            result.orders_submitted += 1
            result.orders.append(order)
            if order.status == OrderStatus.FILLED:
                result.orders_filled += 1
                if self._guard:
                    self._guard.record_success()
            else:
                result.orders_rejected += 1

        self._log.info(
            "run_once done | signals={} submitted={} filled={} rejected={}",
            result.signals_generated,
            result.orders_submitted,
            result.orders_filled,
            result.orders_rejected,
        )
        return result

    @property
    def oms_summary(self) -> dict:
        return self._oms.summary()

    def validate_signal(self, signal) -> "RiskDecision":
        """
        Expone la validación de riesgo sin violar Law of Demeter.

        TradingEngine es la fachada que oculta la estructura interna del OMS.

        Returns
        -------
        RiskDecision con campos: rejected (bool), reason (str), size_pct (float).
        """
        return self._oms.validate_signal(signal)

    # ------------------------------------------------------------------
    # Private
    # ------------------------------------------------------------------

    def _load_data(self):
        try:
            return self._data_source.load_features(
                exchange=self._exchange,
                symbol=self._strategy.symbol,
                timeframe=self._strategy.timeframe,
                market_type=self._market_type,
            )
        except Exception as exc:
            self._log.error("load_features error | {}", exc)
            return None

    def __repr__(self) -> str:
        return (
            f"TradingEngine("
            f"strategy={self._strategy.name!r}, "
            f"exchange={self._exchange!r}, "
            f"symbol={self._strategy.symbol!r})"
        )
