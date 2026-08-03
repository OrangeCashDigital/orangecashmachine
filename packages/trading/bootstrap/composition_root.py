# -*- coding: utf-8 -*-
"""
trading/bootstrap/composition_root.py
=======================================

TradingCompositionRoot — unico punto autorizado para instanciar adapters
concretos de trading (LiveExecutor, PaperExecutor, RiskManager, OMS).

Por que existe
--------------
TradingEngine.build_live()/build_paper() instanciaban adapters concretos
directamente dentro de application/ -- violacion de DIP ya senalada en
ADR-0007 ("trading es el unico contexto sin DIP real entre dominio e
infraestructura"). Mismo patron ya resuelto en portfolio via
PortfolioCompositionRoot (ver commit 527ef5b) y en market_data via
infrastructure/bootstrap/composition_root.py (BC-38, BC-42).

TradingEngine.__init__ ya recibia todo por constructor (DIP correcto).
Solo los factory classmethods rompian la disciplina -- se extraen aqui
sin cambiar el comportamiento observable.

Principios: DIP . SRP . SafeOps (fail-fast en live)
"""

from __future__ import annotations

from typing import Callable, Optional

from trading.application.engine import TradingEngine
from trading.domain.entities.order import Order
from trading.domain.services.risk_manager import RiskManager
from trading.domain.strategies.registry import StrategyRegistry
from trading.domain.value_objects.risk_config import RiskConfig
from trading.ports.outbound.feature_source import FeatureSource

from ocm.runtime.guard import ExecutionGuard


class TradingCompositionRoot:
    """Composition root de trading -- arma el engine, nunca lo ejecuta."""

    @staticmethod
    def build_live(
        strategy_name: str,
        strategy_cfg: dict,
        data_source: FeatureSource,
        risk_config: Optional[RiskConfig] = None,
        capital_usd: float = 10_000.0,
        exchange: str = "bybit",
        market_type: str = "spot",
        guard: Optional[ExecutionGuard] = None,
        on_fill: Optional[Callable[[Order], None]] = None,
        on_reject: Optional[Callable[[Order], None]] = None,
    ) -> TradingEngine:
        """
        Ensambla TradingEngine para live trading.

        Fail-Fast: guard y risk_config son obligatorios -- sin kill switch
        ni limites explicitos no hay live trading con capital real.
        """
        from trading.adapters.outbound.live_executor import LiveExecutor
        from trading.application.oms import OMS

        if guard is None:
            raise ValueError("TradingCompositionRoot.build_live: guard es obligatorio en live trading.")
        if risk_config is None:
            raise ValueError("TradingCompositionRoot.build_live: risk_config es obligatoria en live trading.")

        strategy = StrategyRegistry.get(strategy_name)(**strategy_cfg)
        risk_manager = RiskManager(config=risk_config, capital_usd=capital_usd)
        executor = LiveExecutor(exchange=exchange, market_type=market_type)
        oms = OMS(
            risk_manager=risk_manager,
            executor=executor,
            guard=guard,
            on_fill=on_fill,
            on_reject=on_reject,
        )
        return TradingEngine(
            strategy=strategy,
            oms=oms,
            data_source=data_source,
            guard=guard,
            exchange=exchange,
            market_type=market_type,
        )

    @staticmethod
    def build_paper(
        strategy_name: str,
        strategy_cfg: dict,
        data_source: FeatureSource,
        risk_config: Optional[RiskConfig] = None,
        capital_usd: float = 10_000.0,
        exchange: str = "bybit",
        market_type: str = "spot",
        guard: Optional[ExecutionGuard] = None,
        on_fill: Optional[Callable[[Order], None]] = None,
        on_reject: Optional[Callable[[Order], None]] = None,
    ) -> TradingEngine:
        """Ensambla TradingEngine para paper trading -- sin requisitos fail-fast de live."""
        from trading.adapters.outbound.paper_executor import PaperExecutor
        from trading.application.oms import OMS

        strategy = StrategyRegistry.get(strategy_name)(**strategy_cfg)
        risk_manager = RiskManager(config=risk_config or RiskConfig(), capital_usd=capital_usd)
        executor = PaperExecutor()
        oms = OMS(
            risk_manager=risk_manager,
            executor=executor,
            guard=guard,
            on_fill=on_fill,
            on_reject=on_reject,
        )
        return TradingEngine(
            strategy=strategy,
            oms=oms,
            data_source=data_source,
            guard=guard,
            exchange=exchange,
            market_type=market_type,
        )
