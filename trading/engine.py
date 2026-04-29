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

Principios: SOLID · KISS · DRY · SafeOps
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Optional

from loguru import logger

from ocm_platform.boundaries import FeatureSource  # SSOT — unica definicion del contrato
from market_data.safety.execution_guard import ExecutionGuard
from trading.execution.order import Order, OrderStatus
from trading.risk.models import RiskConfig
from trading.strategies.base import BaseStrategy
from trading.strategies.registry import StrategyRegistry


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------

@dataclass
class EngineResult:
    """Resultado de un ciclo run_once()."""
    symbol:            str
    timeframe:         str
    signals_generated: int  = 0
    orders_submitted:  int  = 0
    orders_filled:     int  = 0
    orders_rejected:   int  = 0
    skipped:           bool = False
    skip_reason:       str  = ""
    orders:            list[Order] = field(default_factory=list)

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
        strategy:    BaseStrategy,
        oms,                                  # OMS — importado lazy en build_paper
        data_source: FeatureSource,
        guard:       Optional[ExecutionGuard] = None,
        exchange:    str = "bybit",
        market_type: str = "spot",            # default consistente con PaperBot
    ) -> None:
        self._strategy    = strategy
        self._oms         = oms
        self._data_source = data_source
        self._guard       = guard
        self._exchange    = exchange
        self._market_type = market_type
        self._log         = logger.bind(
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

        Thread-safe. Sincrónico — para uso en Prefect tasks o loops.
        """
        s      = self._strategy
        result = EngineResult(symbol=s.symbol, timeframe=s.timeframe)

        # Kill switch — propiedad pública stop_reason
        if self._guard and self._guard.should_stop():
            result.skipped     = True
            result.skip_reason = f"guard:{self._guard.stop_reason}"
            self._log.warning("run_once bloqueado | reason={}", result.skip_reason)
            return result

        # Cargar datos
        df = self._load_data()
        if df is None or (hasattr(df, "empty") and df.empty):
            result.skipped     = True
            result.skip_reason = "no_data"
            self._log.warning(
                "Sin datos | exchange={} symbol={} tf={}",
                self._exchange, s.symbol, s.timeframe,
            )
            return result

        # Generar señales
        try:
            signals = s.generate_signals(df)
        except Exception as exc:
            result.skipped     = True
            result.skip_reason = f"strategy_error:{exc}"
            self._log.error("generate_signals error | {}", exc)
            if self._guard:
                self._guard.record_error(str(exc))
            return result

        result.signals_generated = len(signals)
        self._log.debug(
            "Señales generadas | symbol={} count={}",
            s.symbol, len(signals),
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

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def build_live(
        cls,
        strategy_name: str,
        strategy_cfg:  dict,
        data_source:   FeatureSource,
        risk_config:   Optional[RiskConfig] = None,
        capital_usd:   float = 10_000.0,
        exchange:      str   = "bybit",
        market_type:   str   = "spot",
        guard:         Optional[ExecutionGuard] = None,
        on_fill:       Optional[Callable[[Order], None]] = None,
        on_reject:     Optional[Callable[[Order], None]] = None,
    ) -> "TradingEngine":
        """
        Factory para live trading.

        Construye: Strategy + RiskManager + LiveExecutor + OMS + Engine.
        LiveExecutor envia ordenes reales al exchange via CCXT.

        PRECAUCIÓN SafeOps
        ------------------
        - Requiere ExecutionGuard activo — build_live() falla si guard=None.
          El guard es el kill switch de emergencia en produccion.
        - RiskConfig obligatoria en live — no usa defaults permisivos.
        - Separado de build_paper() (SRP) — sin condicional interno.

        Parameters
        ----------
        on_fill   : callback(order) — TradeTracker + PortfolioService via composite.
        on_reject : callback(order) — alerting / logging externo.
        """
        from trading.execution.oms import OMS
        from trading.execution.live_executor import LiveExecutor
        from trading.risk.manager import RiskManager

        # Fail-Fast: guard obligatorio en live — sin kill switch no hay live trading
        if guard is None:
            raise ValueError(
                "TradingEngine.build_live: guard es obligatorio en live trading. "
                "Proporcionar un ExecutionGuard configurado."
            )
        # Fail-Fast: RiskConfig obligatoria en live — no defaults permisivos
        if risk_config is None:
            raise ValueError(
                "TradingEngine.build_live: risk_config es obligatoria en live trading. "
                "Los defaults de RiskConfig no son apropiados para capital real."
            )

        strategy     = StrategyRegistry.get(strategy_name)(**strategy_cfg)
        risk_manager = RiskManager(
            config      = risk_config,
            capital_usd = capital_usd,
        )
        executor = LiveExecutor(exchange=exchange, market_type=market_type)
        oms      = OMS(
            risk_manager = risk_manager,
            executor     = executor,
            guard        = guard,
            on_fill      = on_fill,
            on_reject    = on_reject,
        )
        return cls(
            strategy    = strategy,
            oms         = oms,
            data_source = data_source,
            guard       = guard,
            exchange    = exchange,
            market_type = market_type,
        )

    @classmethod
    def build_paper(
        cls,
        strategy_name: str,
        strategy_cfg:  dict,
        data_source:   FeatureSource,
        risk_config:   Optional[RiskConfig] = None,
        capital_usd:   float = 10_000.0,
        exchange:      str   = "bybit",
        market_type:   str   = "spot",
        guard:         Optional[ExecutionGuard] = None,
        on_fill:       Optional[Callable[[Order], None]] = None,
        on_reject:     Optional[Callable[[Order], None]] = None,
    ) -> "TradingEngine":
        """
        Factory para paper trading.

        Construye: Strategy + RiskManager + PaperExecutor + OMS + Engine.
        Un único punto de ensamblaje — el caller no necesita conocer
        las dependencias internas. (DIP)

        Parameters
        ----------
        on_fill   : callback(order) invocado por OMS al completar un fill.
                    Uso principal: TradeTracker.on_fill para analytics.
        on_reject : callback(order) invocado por OMS al rechazar una orden.
        """
        from trading.execution.oms import OMS
        from trading.execution.paper_executor import PaperExecutor
        from trading.risk.manager import RiskManager

        strategy     = StrategyRegistry.get(strategy_name)(**strategy_cfg)
        risk_manager = RiskManager(
            config      = risk_config or RiskConfig(),
            capital_usd = capital_usd,
        )
        executor = PaperExecutor()
        oms      = OMS(
            risk_manager = risk_manager,
            executor     = executor,
            guard        = guard,
            on_fill      = on_fill,    # conecta OMS → TradeTracker (o cualquier observer)
            on_reject    = on_reject,
        )
        return cls(
            strategy    = strategy,
            oms         = oms,
            data_source = data_source,
            guard       = guard,
            exchange    = exchange,
            market_type = market_type,
        )

    # ------------------------------------------------------------------
    # Private
    # ------------------------------------------------------------------

    def _load_data(self):
        try:
            return self._data_source.load_features(
                exchange    = self._exchange,
                symbol      = self._strategy.symbol,
                timeframe   = self._strategy.timeframe,
                market_type = self._market_type,
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
