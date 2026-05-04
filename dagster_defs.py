# -*- coding: utf-8 -*-
"""
dagster_defs.py — entry point de Dagster (raíz del proyecto).

NOTA: Este archivo debe permanecer en la raíz del proyecto.
Dagster lo localiza vía dagster.yaml → python_file: dagster_defs.py
No mover — es un contrato del framework, no del dominio.

La implementación real está en:
    infrastructure/dagster/defs.py

Este módulo re-exporta Definitions desde allí.
"""
from infrastructure.dagster.defs import defs  # noqa: F401
