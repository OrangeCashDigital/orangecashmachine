from __future__ import annotations

"""
core/config/loader/env_overrides.py
====================================

Aplica overrides desde variables de entorno OCM_* sobre el dict
mergeado de YAML, antes de la validación Pydantic.

Implementación: pydantic-settings BaseSettings.
  - Coerción de tipos: automática vía Pydantic (elimina _coerce manual)
  - Paths anidados:    campo field_name con "__" como separador
  - Validación:        campos tipados (elimina whitelist manual)
  - SSOT:              nombres de vars en env_vars.py

Aliases de compatibilidad (vars legacy sin prefijo OCM_ + sin __):
    OCM_BACKFILL_MODE      → pipeline.historical.backfill_mode
    OCM_START_DATE         → pipeline.historical.start_date
    OCM_MAX_CONCURRENT     → pipeline.historical.max_concurrent_tasks
    OCM_LOG_LEVEL          → observability.logging.level
    OCM_SNAPSHOT_INTERVAL  → pipeline.realtime.snapshot_interval_seconds
    OCM_METRICS_ENABLED    → observability.metrics.enabled
    OCM_METRICS_PORT       → observability.metrics.port
    OCM_DEBUG              → environment.debug

Nuevo formato (también soportado):
    OCM_PIPELINE__HISTORICAL__BACKFILL_MODE=true
    OCM_OBSERVABILITY__METRICS__PORT=9090
"""

import os
from typing import Optional

from loguru import logger
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


# =============================================================================
# Mapping: field_name → path en el dict de config
# El field_name usa "__" como separador — se convierte a tuple en apply()
# =============================================================================

_FIELD_TO_PATH: dict[str, tuple[str, ...]] = {
    "pipeline__historical__backfill_mode":           ("pipeline", "historical", "backfill_mode"),
    "pipeline__historical__start_date":              ("pipeline", "historical", "start_date"),
    "pipeline__historical__max_concurrent_tasks":    ("pipeline", "historical", "max_concurrent_tasks"),
    "pipeline__historical__fetch_all_history":       ("pipeline", "historical", "fetch_all_history"),
    "pipeline__realtime__snapshot_interval_seconds": ("pipeline", "realtime", "snapshot_interval_seconds"),
    "observability__metrics__enabled":               ("observability", "metrics", "enabled"),
    "observability__metrics__port":                  ("observability", "metrics", "port"),
    "observability__logging__level":                 ("observability", "logging", "level"),
    "environment__debug":                            ("environment", "debug"),
}


class OcmSettings(BaseSettings):
    """
    Lee las variables OCM_* del entorno con tipos correctos vía Pydantic.

    Soporta dos formatos:
      1. Aliases legacy:  OCM_BACKFILL_MODE=true
      2. Nuevo formato:   OCM_PIPELINE__HISTORICAL__BACKFILL_MODE=true

    Todos los campos son Optional — solo se aplican los que están presentes.
    """

    model_config = SettingsConfigDict(
        env_prefix           = "OCM_",
        env_nested_delimiter = "__",
        case_sensitive       = False,
        extra                = "ignore",
        populate_by_name     = True,
    )

    # ── pipeline.historical ──────────────────────────────────────────────────
    pipeline__historical__backfill_mode: Optional[bool] = Field(
        default=None,
        validation_alias="OCM_BACKFILL_MODE",
    )
    pipeline__historical__start_date: Optional[str] = Field(
        default=None,
        validation_alias="OCM_START_DATE",
    )
    pipeline__historical__max_concurrent_tasks: Optional[int] = Field(
        default=None,
        validation_alias="OCM_MAX_CONCURRENT",
    )
    pipeline__historical__fetch_all_history: Optional[bool] = Field(
        default=None,
        validation_alias="OCM_FETCH_ALL_HISTORY",
    )

    # ── pipeline.realtime ────────────────────────────────────────────────────
    pipeline__realtime__snapshot_interval_seconds: Optional[int] = Field(
        default=None,
        validation_alias="OCM_SNAPSHOT_INTERVAL",
    )

    # ── observability.metrics ────────────────────────────────────────────────
    observability__metrics__enabled: Optional[bool] = Field(
        default=None,
        validation_alias="OCM_METRICS_ENABLED",
    )
    observability__metrics__port: Optional[int] = Field(
        default=None,
        validation_alias="OCM_METRICS_PORT",
    )

    # ── observability.logging ────────────────────────────────────────────────
    observability__logging__level: Optional[str] = Field(
        default=None,
        validation_alias="OCM_LOG_LEVEL",
    )

    # ── environment ──────────────────────────────────────────────────────────
    environment__debug: Optional[bool] = Field(
        default=None,
        validation_alias="OCM_DEBUG",
    )

    def apply(self, config: dict) -> dict:
        """
        Aplica los overrides sobre el dict mergeado de YAML.
        Solo parchea campos explícitamente seteados (en model_fields_set).
        Retorna el mismo dict modificado in-place.
        """
        applied: list[str] = []

        # model_fields_set es set[str] — nombres de campos con valor explícito
        for field_name in self.model_fields_set:
            value = getattr(self, field_name, None)
            if value is None:
                continue

            path = _FIELD_TO_PATH.get(field_name)
            if path is None:
                # campo no mapeado (extra ignorado) — skip silencioso
                continue

            _set_nested(config, path, value)
            applied.append(f"{field_name.upper()}={value!r} → {'.'.join(path)}")
            logger.info(
                "env_override_applied | path={} value={}",
                ".".join(path), value,
            )

        if applied:
            logger.debug(
                "env_overrides_summary | count={} applied={}",
                len(applied), " | ".join(applied),
            )
        else:
            logger.debug("env_overrides_summary | count=0")

        return config


# =============================================================================
# Helpers
# =============================================================================

def _set_nested(d: dict, path: tuple[str, ...], value: object) -> None:
    """Pisa el valor en el dict siguiendo el path, creando dicts intermedios."""
    for key in path[:-1]:
        d = d.setdefault(key, {})
    d[path[-1]] = value


# =============================================================================
# Public API — firma idéntica a la versión anterior
# =============================================================================

def apply_env_overrides(config: dict) -> dict:
    """
    Punto de entrada para el loader.
    Crea OcmSettings (lee env vars), aplica sobre el dict mergeado, retorna.
    """
    return OcmSettings().apply(config)
