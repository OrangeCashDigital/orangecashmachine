# -*- coding: utf-8 -*-
"""
app/
====
Composition root — entrypoints ejecutables de OrangeCashMachine.

Este paquete NO contiene lógica de negocio. Solo orquesta dominios
y expone CLIs. Puede importar libremente de cualquier dominio.

Principio: Composition Root Pattern (Fowler, PEAA) — un único lugar
donde se ensamblan todas las dependencias antes de ejecutar.

Entrypoints
-----------
  run_market_data.py — CLI market data pipeline (Hydra → RuntimeContext → flow)
  run_paper.py       — CLI paper trading (Gold/Iceberg → TradingEngine)

Uso (después de instalar con uv):
    uv run ocm                                         # market data pipeline
    uv run paper [--symbol ETH/USDT] [--dry-run]      # paper trading
    uv run python deploy.py [--prod]                   # Prefect deployment
"""
