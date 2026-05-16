# -*- coding: utf-8 -*-
"""
market_data/application/strategies/
=====================================

Estrategias de ingestión OHLCV — capa application.

Cada estrategia orquesta ports (fetcher, storage, kafka, cursor)
para implementar un modo de ingestión. No contienen lógica de negocio
pura — esa vive en domain/policies/base.py y domain/value_objects/.

Estrategias disponibles
-----------------------
  BackfillStrategy    — paginación histórica hacia atrás
  IncrementalStrategy — ingestión incremental hacia adelante
  RepairStrategy      — reparación de gaps en Silver

Importar siempre desde el submódulo específico:
    from market_data.application.strategies.backfill    import BackfillStrategy
    from market_data.application.strategies.incremental import IncrementalStrategy
    from market_data.application.strategies.repair      import RepairStrategy

Este __init__.py está intencionalmente vacío de re-exports.

Principios: SRP · DIP · Clean Architecture · KISS
"""
