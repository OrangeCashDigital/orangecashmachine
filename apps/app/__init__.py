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
    main.py         — market data pipeline (Hydra / AppConfig)
    live_hydra.py   — live trading ⚠️  capital real
    paper_hydra.py  — paper trading (Gold/Iceberg o dry-run)
    entrypoint.py   — runner del pipeline (data)
    _bootstrap.py   — helpers compartidos de los CLIs Hydra (H1/H8)

  use_cases/        — Application Layer (orquestación, DI, flujos)
    execute_live.py — ciclo de live trading (capital real)
    execute_paper.py — ciclo de paper trading (Gold/Iceberg o dry-run)
    run_result.py   — CycleRunResult (contrato único del resultado de ciclo)

Flujo canónico:
  cli/ → use_cases/ → trading/ → portfolio/ → domain/

Principios: Composition Root (Fowler PEAA) · SRP · DIP
"""
