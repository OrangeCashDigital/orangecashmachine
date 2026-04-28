"""Shim de compatibilidad: re-exporta desde ocm_platform.infra.observability.

TEMPORAL: eliminar cuando todos los callers apunten a ocm_platform.infra.observability.*

Callers actuales:
  main.py: init_metrics_runtime, PrometheusPusher, NoopPusher
"""
from ocm_platform.infra.observability.runtime  import *  # noqa: F401, F403
from ocm_platform.infra.observability.adapters import *  # noqa: F401, F403
