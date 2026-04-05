# -*- coding: utf-8 -*-
"""
backtesting/
============

Motor de backtesting vectorizado sobre datos Silver/Gold.

Estado
------
🚧 EN CONSTRUCCIÓN — pendiente de que market_data quede estable.

Bloqueado por
-------------
- MarketDataLoader (data_platform/loaders/) debe estar completamente
  validado antes de construir el motor de backtesting encima.
- GoldLoader (features) debe producir datasets reproducibles y versionados
  con as_of support para que los backtests sean deterministas.

Submódulos planificados
-----------------------
backtesting/
  engine/     — loop de simulación vectorizado (pandas/numpy)
  metrics/    — Sharpe, Sortino, max drawdown, Calmar ratio
  report/     — generación de reportes HTML/PDF por run
  walk_forward/ — walk-forward validation y parameter stability

No eliminar este módulo — su existencia es intencional como placeholder
para la siguiente fase de desarrollo del sistema.
"""
