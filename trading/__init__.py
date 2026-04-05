# -*- coding: utf-8 -*-
"""
trading/
========

Módulo de ejecución y estrategias de trading algorítmico.

Estado
------
🚧 EN CONSTRUCCIÓN — pendiente de que market_data quede estable.

Bloqueado por
-------------
- market_data pipeline debe estar completamente hardened (backfill,
  incremental, repair) antes de construir lógica de ejecución encima.
- Se necesita definir la interfaz de señales entre research/ y trading/.

Submódulos planificados
-----------------------
trading/
  strategies/   — implementaciones de estrategias (mean reversion, momentum…)
  execution/    — order management, broker adapters (ccxt live trading)
  risk/         — position sizing, drawdown limits, kill switch
  portfolio/    — gestión de cartera multi-asset

No eliminar este módulo — su existencia es intencional como placeholder
para la siguiente fase de desarrollo del sistema.
"""
