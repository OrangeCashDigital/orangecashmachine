# -*- coding: utf-8 -*-
"""
portfolio/infra/
================
Implementaciones de infraestructura para portfolio/.

Contiene implementaciones concretas de los puertos definidos en
portfolio/ports/. Nunca importadas directamente por servicios —
inyectadas en el composition root (app/use_cases/).

  redis_store.py — RedisPositionStore: implementación Redis de PositionStore
"""
