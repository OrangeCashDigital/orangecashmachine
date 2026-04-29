# -*- coding: utf-8 -*-
"""
portfolio/infra/
================

Implementaciones de infraestructura de portfolio.

OCP: PortfolioService acepta cualquier PositionStore.
     Intercambiar store = reemplazar una línea en el use case.

  memory_store.py  — InMemoryPositionStore (paper trading / tests)
  redis_store.py   — RedisPositionStore    (producción, cross-restart)

Importar desde aquí (SSOT):
    from portfolio.infra import InMemoryPositionStore, RedisPositionStore
"""
from portfolio.infra.memory_store import InMemoryPositionStore
from portfolio.infra.redis_store import RedisPositionStore

__all__ = ["InMemoryPositionStore", "RedisPositionStore"]
