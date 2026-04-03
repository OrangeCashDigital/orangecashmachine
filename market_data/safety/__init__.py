from __future__ import annotations

"""
market_data/safety/__init__.py
================================

Capa de safety de ejecución.

Componentes
-----------
ExecutionGuard       — kill switch + límites de runtime y errores consecutivos
EnvironmentValidator — coherencia de entorno antes de ejecutar pipelines
guard_context        — contexto de proceso para compartir guard con Prefect flows
"""

from market_data.safety.execution_guard import ExecutionGuard, ExecutionStoppedError
from market_data.safety.environment_validator import (
    EnvironmentValidator,
    EnvironmentMismatchError,
)
from market_data.safety import guard_context

__all__ = [
    "ExecutionGuard",
    "ExecutionStoppedError",
    "EnvironmentValidator",
    "EnvironmentMismatchError",
    "guard_context",
]
