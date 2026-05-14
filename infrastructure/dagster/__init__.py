# -*- coding: utf-8 -*-
"""
infrastructure/dagster/
========================

Adapter de orquestación Dagster para OCM.

Dagster es infraestructura pura — no conoce el dominio.
Solo llama a PipelineOrchestrator via PipelineRequest (DIP).

Estructura
----------
  assets/      — assets Dagster por bounded context
  resources.py — OCMResource (Composition Root de Dagster)
  defs.py      — Definitions (entry point de Dagster)

Principios: DIP · SRP · OCP · infraestructura sin dominio
"""
