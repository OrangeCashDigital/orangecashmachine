# -*- coding: utf-8 -*-
"""
portfolio/
==========

Bounded context: gestión de posiciones y rebalanceo.

Responsabilidades
-----------------
- Mantener el estado actual de posiciones abiertas
- Calcular la exposición por símbolo, exchange y mercado
- Ejecutar rebalanceos según targets definidos por estrategia
- Exponer el estado para risk/ y analytics/

NO es responsabilidad de portfolio/
-------------------------------------
- Validar riesgo (→ trading/risk/)
- Generar señales (→ trading/strategies/)
- Ejecutar órdenes en exchanges (→ trading/execution/)
- Persistir historial de trades (→ trading/analytics/)

Submódulos
----------
  models/    — PositionSnapshot, PortfolioState (value objects / entidades)
  ports/     — interfaces que portfolio/ necesita del exterior
  services/  — PortfolioService, RebalanceService

Principios: SOLID · DDD · KISS · DRY · SafeOps
"""
__version__ = "0.1.0"
