# -*- coding: utf-8 -*-
"""
trading/
========
Sistema de trading desacoplado por dominios.

Importar desde los submódulos directamente:

    from trading.strategies import StrategyRegistry, BaseStrategy
    from trading.risk import RiskManager, RiskConfig
    from trading.execution import OMS, PaperExecutor
    from trading.analytics import TradeTracker

  strategies/  — plugin system + estrategias
  risk/        — RiskManager + RiskConfig + modelos
  execution/   — OMS + executors (paper, live)
  analytics/   — TradeTracker, PerformanceEngine
  engine.py    — TradingEngine (orquestador)

El entrypoint CLI vive en app/run_paper.py (composition root).
"""
__version__ = "0.2.0"
