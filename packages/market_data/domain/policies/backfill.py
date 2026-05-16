# -*- coding: utf-8 -*-
"""
market_data/domain/policies/backfill.py
=========================================

DEPRECADO — este módulo es un stub de backward compatibility.

BackfillStrategy vive ahora en:
    market_data.application.strategies.backfill

Importar directamente desde allí:
    from market_data.application.strategies.backfill import BackfillStrategy

Este stub re-exporta para no romper imports existentes durante la migración.
"""
from market_data.application.strategies.backfill import (
    BackfillStrategy,
    _publish_chunk_to_kafka,
)

__all__ = ["BackfillStrategy", "_publish_chunk_to_kafka"]
