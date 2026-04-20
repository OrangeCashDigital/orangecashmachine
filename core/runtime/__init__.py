from __future__ import annotations

"""
core/runtime/
=============

Sistema vivo en ejecución — todo lo que describe el mundo mientras corre.

Separado de core/config/ (que construye el mundo) por principio de
separación de responsabilidades (SRP / Clean Architecture).

Exports públicos
----------------
RunConfig       — configuración de proceso inmutable (env, debug, run_id…)
RuntimeContext  — contexto de ejecución de un run completo
get_git_hash    — hash del commit activo para trazabilidad de lineage
build_lineage   — constructor de LineageRecord
LineageRecord   — registro inmutable de trazabilidad
record_run      — persiste metadata de un run (SQLite + JSONL)
query_runs      — consulta runs recientes desde SQLite

Regla de dependencias
---------------------
core/runtime/ puede importar de:
  core/config/   — para AppConfig y schema (solo tipos)
  core/observability/ — para logging

core/runtime/ NUNCA importa de:
  market_data/   — dominio de negocio
  infra/         — infraestructura
"""

from core.runtime.run_config import RunConfig
from core.runtime.context import RuntimeContext
from core.runtime.lineage import get_git_hash, build_lineage, LineageRecord
from core.runtime.registry import record_run, query_runs

__all__ = [
    "RunConfig",
    "RuntimeContext",
    "get_git_hash",
    "build_lineage",
    "LineageRecord",
    "record_run",
    "query_runs",
]
