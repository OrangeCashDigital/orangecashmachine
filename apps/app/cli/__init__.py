# -*- coding: utf-8 -*-
"""
app/cli/
========

Thin CLI entrypoints — OrangeCashMachine.

Responsabilidad única: parsear args, configurar logging, llamar
al use case correspondiente, reportar resultado y exit code.
No contiene lógica de negocio ni de ensamblaje (SRP).

  live_hydra.py   — live trading ⚠️  capital real
  paper_hydra.py  — paper trading (Gold/Iceberg o dry-run)
  main.py         — market data pipeline (Hydra orquestado)

Instalados como scripts en pyproject.toml (SSOT del CLI).

Flujo canónico:
  CLI (aquí) → use case (app/use_cases/) → Domain
"""
