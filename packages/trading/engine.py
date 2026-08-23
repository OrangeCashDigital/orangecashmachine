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
from datetime import datetime, timezone
from typing import Optional

import polars as pl
from loguru import logger

from ocm.runtime.guard import ExecutionGuard
from shared.contracts.boundaries import (
    FeatureSource,
)  # SSOT — unica definicion del contrato
from shared.types.signal import Signal
from trading.execution.order import Order, OrderStatus
from trading.risk.models import StopLossConfig
from trading.risk.stop_loss import (
    StopLossEvaluator,
    SupportsPositionSnapshot,
)
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
    stop_loss_closes: int = 0
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

    S1: antes de generar señales, evalúa stop-loss (StopLossConfig) contra el
    snapshot del portfolio inyectado y cierra posiciones que cruzan su nivel
    por el flujo normal del OMS. Si no se inyecta portfolio ni stop_loss, el
    ciclo se comporta como antes (sin stop-loss).

    Parameters
    ----------
    strategy     : BaseStrategy
    oms          : OMS
    data_source  : FeatureSource
    guard        : ExecutionGuard, optional
    exchange     : str
    market_type  : str  — "spot" | "linear" | "inverse"
    portfolio    : SupportsPositionSnapshot, optional — proveedor de posiciones
                   abiertas (PortfolioService); requerido para stop-loss.
    stop_loss    : StopLossConfig, optional — enable/default_pct; sin él no hay
                   stop-loss (el ciclo no lo inventa).
    """

    def __init__(
        self,
        strategy: BaseStrategy,
        oms,
        data_source: FeatureSource,
        guard: Optional[ExecutionGuard] = None,
        exchange: str = "bybit",
        market_type: str = "spot",
        portfolio: Optional[SupportsPositionSnapshot] = None,
        stop_loss: Optional[StopLossConfig] = None,
    ) -> None:
        self._strategy = strategy
        self._oms = oms
        self._data_source = data_source
        self._guard = guard
        self._exchange = exchange
        self._market_type = market_type
        self._portfolio = portfolio
        self._stop_loss_evaluator = StopLossEvaluator(stop_loss) if stop_loss is not None else None
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
        if df is None or (hasattr(df, "empty") and df.empty) or (hasattr(df, "is_empty") and df.is_empty()):
            result.skipped = True
            result.skip_reason = "no_data"
            self._log.warning(
                "Sin datos | exchange={} symbol={} tf={}",
                self._exchange,
                s.symbol,
                s.timeframe,
            )
            return result

        # Punto único de validación del tipo de DataFrame.
        # SSOT: packages/trading/strategies consumen pl.DataFrame nativo.
        if not isinstance(df, pl.DataFrame):
            raise TypeError(f"TradingEngine espera pl.DataFrame, recibió {type(df).__name__}")

        # S1 — stop-loss: evaluar posiciones abiertas contra el close actual
        # ANTES de procesar señales de la estrategia. Conecta la configuración
        # existente (StopLossConfig.enabled/default_pct) con la ejecución
        # existente: emite un SELL por el flujo normal del OMS cuando una
        # posición cruza su nivel. SafeOps: nunca rompe el ciclo.
        current_price = None
        try:
            current_price = float(df.select("close").row(-1)[0])
        except Exception:  # SafeOps: sin close actual → stop-loss no evaluado
            self._log.warning("run_once: sin close actual — stop-loss no evaluado")
        if current_price is not None:
            result.stop_loss_closes = self._run_stop_loss(result, current_price)

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

    def _run_stop_loss(self, result: EngineResult, current_price: float) -> int:
        """Evalúa stop-loss sobre posiciones abiertas y emite SELLs de cierre.

        S1: usa el evaluador de stop-loss (config existente) contra el
        snapshot de posiciones del portfolio inyectado. Cada brecha se cierra
        con una señal SELL enviada por el flujo normal del OMS (submit) — la
        orden pasa por validación de riesgo igual que una señal de estrategia.

        SafeOps: sin portfolio o sin evaluador → 0 cierres, nunca lanza.
        """
        if self._stop_loss_evaluator is None or self._portfolio is None:
            return 0

        symbol = self._strategy.symbol
        try:
            snapshot = self._portfolio.snapshot()
            positions = [p for p in snapshot.positions if getattr(p, "symbol", None) == symbol]
        except Exception as exc:
            self._log.warning("run_once: stop-loss sin snapshot | err={}", exc)
            return 0

        # F1 (ADR-0025): la cantidad del SELL de stop-loss se deriva de la
        # cantidad económica real de la posición (Position.quantity, la SSOT).
        # El TARGET se propaga en signal.quantity y el OMS lo clampa contra su
        # espejo local — nunca se pide más de lo disponible.
        qty_by_symbol: dict[str, float] = {}
        for p in positions:
            q = getattr(p, "quantity", None)
            if q is not None and q > 0:
                qty_by_symbol[p.symbol] = qty_by_symbol.get(p.symbol, 0.0) + float(q)

        try:
            breached = self._stop_loss_evaluator.breached(positions, current_price)
        except Exception as exc:
            self._log.warning("run_once: error evaluando stop-loss | err={}", exc)
            return 0

        if not breached:
            return 0

        closes = 0
        for symbol_hit in breached:
            qty = qty_by_symbol.get(symbol_hit)
            if qty is None or qty <= 0:
                self._log.warning(
                    "STOP-LOSS sin quantity económica conocida — sin SELL | symbol={}",
                    symbol_hit,
                )
                continue
            self._log.warning(
                "STOP-LOSS | {} cruzó {} en close={:.2f} — emitiendo SELL qty={}",
                symbol_hit,
                self._strategy.timeframe,
                current_price,
                qty,
            )
            sell = Signal(
                symbol=symbol_hit,
                timeframe=self._strategy.timeframe,
                direction="sell",
                price=current_price,
                timestamp=self._last_timestamp(),
                confidence=1.0,
                quantity=qty,
            )
            order = self._oms.submit(sell)
            if order is None:
                self._log.warning(
                    "STOP-LOSS SELL rechazado | {} — posición sigue abierta",
                    symbol_hit,
                )
                continue
            result.orders.append(order)
            result.orders_submitted += 1
            if order.status == OrderStatus.FILLED:
                result.orders_filled += 1
                closes += 1
            else:
                result.orders_rejected += 1
        return closes

    def _last_timestamp(self):
        """Timestamp de la última vela para sellos de stop-loss."""
        return datetime.now(timezone.utc)

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
