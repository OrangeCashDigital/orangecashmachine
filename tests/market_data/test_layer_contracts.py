# -*- coding: utf-8 -*-
"""
tests/market_data/test_layer_contracts.py

Gobernanza de tecnología (domain no importa frameworks de infra/datos)
migrada a import-linter (BC-09, architecture/importlinter.toml).

BC-09 está actualmente BROKEN por deuda técnica conocida:
    market_data.domain.value_objects.timeframe -> shared.types.timeframe -> pandas
Pendiente de Pandas Migration Phase 1 (ver architecture/importlinter.toml, BC-09).

Este archivo queda sin tests propios: `uv run lint-imports` (via
architecture/importlinter.toml) es ahora la única fuente de verdad,
corrida en CI/pre-commit.
"""
