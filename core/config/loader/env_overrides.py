from __future__ import annotations

"""
core/config/loader/env_overrides.py
====================================
Aplica overrides desde variables de entorno OCM_* sobre el dict
mergeado, después del merge YAML y antes de la validación Pydantic.

Patrón:
    OCM_PIPELINE_HISTORICAL_FETCH_ALL_HISTORY=true
    → config["pipeline"]["historical"]["backfill_mode"] = True

Coerción automática de tipos:
    "true" / "false"  → bool
    "123"             → int
    "1.5"             → float
    resto             → str
"""

import os
from typing import Any

from core.config.env_vars import (
    OCM_BACKFILL_MODE,
    OCM_FETCH_ALL_HISTORY,
    OCM_START_DATE,
    OCM_MAX_CONCURRENT,
    OCM_LOG_LEVEL,
    OCM_SNAPSHOT_INTERVAL,
    OCM_DEBUG,
)

from loguru import logger

# Mapping explícito: env var → path en el dict de config
# Keys importados de env_vars.py — SSOT: un solo lugar define los nombres de vars
# Ventaja sobre el recorrido dinámico: auditables, sin typos silenciosos
_OCM_OVERRIDES: dict[str, tuple[str, ...]] = {
    OCM_BACKFILL_MODE:      ("pipeline", "historical", "backfill_mode"),
    OCM_FETCH_ALL_HISTORY:  ("pipeline", "historical", "fetch_all_history"),
    OCM_START_DATE:       ("pipeline", "historical", "start_date"),
    OCM_MAX_CONCURRENT:   ("pipeline", "historical", "max_concurrent_tasks"),
    OCM_LOG_LEVEL:        ("observability", "logging", "level"),
    OCM_SNAPSHOT_INTERVAL: ("pipeline", "realtime", "snapshot_interval_seconds"),
    OCM_DEBUG:            ("environment", "debug"),  # debug gate — bloqueado en production por rules.py
}


def _coerce(value: str, original: Any) -> Any:
    """Coerce string env var al tipo del valor original en el dict.

    Orden de resolución:
    1. Si original es bool  → coerce a bool (prioridad sobre int, evita "1"→True en ints)
    2. Si original es int   → coerce a int
    3. Si original es float → coerce a float
    4. Si value es literal bool sin contexto de original → coerce a bool
    5. Resto → str

    Nota: bool se evalúa ANTES que int porque bool es subclase de int en Python.
    Sin este orden, isinstance(True, int) == True provocaría int("true") → ValueError
    y retornaría el string crudo en lugar del bool esperado.
    """
    # Paso 1: original es bool → prioridad absoluta
    if isinstance(original, bool):
        return value.lower() in ("true", "1", "yes")
    # Paso 2: original es int conocido → coerce numérico
    if isinstance(original, int):
        try:
            return int(value)
        except ValueError:
            pass
    # Paso 3: original es float conocido → coerce numérico
    if isinstance(original, float):
        try:
            return float(value)
        except ValueError:
            pass
    # Paso 4: sin contexto de original, valor parece bool literal
    if value.lower() in ("true", "false", "1", "0", "yes", "no"):
        return value.lower() in ("true", "1", "yes")
    # Paso 5: string crudo
    return value


def _get_nested(d: dict, path: tuple[str, ...]) -> Any:
    """Navega el dict siguiendo el path. Retorna None si no existe."""
    for key in path:
        if not isinstance(d, dict) or key not in d:
            return None
        d = d[key]
    return d


def _set_nested(d: dict, path: tuple[str, ...], value: Any) -> None:
    """Pisa el valor en el dict siguiendo el path, creando dicts intermedios si es necesario."""
    for key in path[:-1]:
        d = d.setdefault(key, {})
    d[path[-1]] = value


def apply_env_overrides(config: dict) -> dict:
    """
    Aplica overrides desde env vars OCM_* sobre el dict mergeado.
    No modifica los YAML. Retorna el mismo dict modificado in-place.
    """
    applied = []

    for env_var, path in _OCM_OVERRIDES.items():
        raw = os.getenv(env_var)
        if raw is None:
            continue

        original = _get_nested(config, path)
        coerced  = _coerce(raw, original)
        _set_nested(config, path, coerced)

        applied.append(f"{env_var}={raw!r} → {'.'.join(path)}={coerced!r}")
        logger.info("env_override_applied | var={} path={} value={}", env_var, ".".join(path), coerced)

    if applied:
        logger.debug("env_overrides_summary | count={} applied={}", len(applied), " | ".join(applied))
    else:
        logger.debug("env_overrides_summary | count=0")

    return config
