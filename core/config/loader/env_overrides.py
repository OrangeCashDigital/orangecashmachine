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

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

# Mapping explícito: env var → path en el dict de config
# Ventaja sobre el recorrido dinámico: auditables, sin typos silenciosos
_OCM_OVERRIDES: dict[str, tuple[str, ...]] = {
    "OCM_BACKFILL_MODE": ("pipeline", "historical", "backfill_mode"),
    "OCM_START_DATE":        ("pipeline", "historical", "start_date"),
    "OCM_MAX_CONCURRENT":    ("pipeline", "historical", "max_concurrent_tasks"),
    "OCM_LOG_LEVEL":         ("observability", "logging", "level"),
    "OCM_SNAPSHOT_INTERVAL": ("pipeline", "realtime", "snapshot_interval_seconds"),
}


def _coerce(value: str, original: Any) -> Any:
    """Coerce string env var al tipo del valor original en el dict."""
    if isinstance(original, bool) or value.lower() in ("true", "false", "1", "0", "yes", "no"):
        return value.lower() in ("true", "1", "yes")
    if isinstance(original, int):
        try:
            return int(value)
        except ValueError:
            pass
    if isinstance(original, float):
        try:
            return float(value)
        except ValueError:
            pass
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
        logger.info("OCM env override applied | %s → %s=%r", env_var, ".".join(path), coerced)

    if applied:
        logger.debug("Total OCM overrides applied: %d | %s", len(applied), " | ".join(applied))
    else:
        logger.debug("No OCM env overrides detected.")

    return config
