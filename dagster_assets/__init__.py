# -*- coding: utf-8 -*-
"""
dagster_assets/
===============

Asset catalog de OrangeCashMachine en Dagster.

Arquitectura
------------
Fase 1 — Wrapper: los assets envuelven OHLCVPipeline sin modificarlo.
Fase 2 — DAG explícito: deps declarativas bronze → silver → gold.
Fase 3 — Concurrencia declarativa: elimina _PIPELINE_SEMAPHORE manual.

Principios: DIP · OCP · SSOT · Clean Architecture
"""
