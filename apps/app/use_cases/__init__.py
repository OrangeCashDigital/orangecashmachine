# -*- coding: utf-8 -*-
"""
app/use_cases/
==============

Application Layer — casos de uso de OrangeCashMachine.

Responsabilidad única: coordinar dominio, inyectar dependencias,
orquestar flujos. No contiene lógica de negocio.

Convención de naming: verbos en infinitivo (execute_*, …)
— NO "run_*" (eso es semántica de CLI, no de use case).

  execute_live    → ciclo de live trading (capital real)
  execute_paper   → ciclo de paper trading (Gold/Iceberg o dry-run)
  run_result      → CycleRunResult (contrato único del resultado de ciclo)

Flujo canónico:
  CLI (app/cli/) → use case (aquí) → Domain (trading/ portfolio/ domain/)

Principios: SRP · DIP · KISS · Composition Root
"""
