# -*- coding: utf-8 -*-
"""
app/
====
Composition Root — entrypoints ejecutables de OrangeCashMachine.

Este paquete NO contiene lógica de negocio. Solo orquesta dominios
y expone CLIs. Puede importar libremente de cualquier dominio.

Estructura interna
------------------
  cli/              — thin CLI entrypoints (argparse, logging, exit codes)
    live.py         — live trading ⚠️  capital real
    paper.py        — paper trading
    market_data.py  — market data pipeline (Hydra)

  use_cases/        — Application Layer (orquestación, DI, flujos)
    execute_live.py
    execute_paper.py
    rebalance.py

Flujo canónico:
  cli/ → use_cases/ → trading/ → portfolio/ → domain/

Principios: Composition Root (Fowler PEAA) · SRP · DIP
"""
