from __future__ import annotations
from typing import Any, Optional, Tuple
from loguru import logger
from pydantic_settings import BaseSettings, SettingsConfigDict

"""
core/config/loader/env_overrides.py
===================================

Environment override layer (Layer 0.5 en arquitectura de config).

Responsabilidad:
    Aplicar overrides desde variables de entorno OCM_* sobre el dict
    mergeado de YAML ANTES de validación Pydantic (AppConfig).

Principios:
    • KISS: sin alias, sin lógica innecesaria
    • DRY: naming = estructura (sin mappings duplicados)
    • SSOT: env var path == config path
    • SafeOps: overrides explícitos, auditables
    • Fail-safe: ignora campos desconocidos sin romper runtime

Formato estándar:
    OCM_<SECTION>__<SUBSECTION>__<FIELD>=value

Ejemplo:
    OCM_PIPELINE__HISTORICAL__START_DATE=2023-01-01T00:00:00Z
"""

# =============================================================================
# SETTINGS MODEL
# =============================================================================

class OcmSettings(BaseSettings):
    """
    Settings explícitos desde OCM_* env vars.

    Diseño:
        • Sin alias → evita ambigüedad
        • Sin mappings → evita duplicación
        • Escalable automáticamente (Open/Closed)
    """

    model_config = SettingsConfigDict(
        env_prefix="OCM_",
        env_nested_delimiter="__",
        case_sensitive=False,
        extra="ignore",
    )

    # ── pipeline.historical ────────────────────────────────────────────────
    pipeline__historical__backfill_mode: Optional[bool] = None
    pipeline__historical__start_date: Optional[str] = None
    pipeline__historical__max_concurrent_tasks: Optional[int] = None
    pipeline__historical__fetch_all_history: Optional[bool] = None

    # ── pipeline.realtime ─────────────────────────────────────────────────
    pipeline__realtime__snapshot_interval_seconds: Optional[int] = None

    # ── observability ─────────────────────────────────────────────────────
    observability__metrics__enabled: Optional[bool] = None
    observability__metrics__port: Optional[int] = None
    observability__logging__level: Optional[str] = None

    # ── environment ───────────────────────────────────────────────────────
    environment__debug: Optional[bool] = None

    # ---------------------------------------------------------------------
    # APPLY OVERRIDES
    # ---------------------------------------------------------------------

    def apply(self, config: dict[str, Any]) -> dict[str, Any]:
        """
        Aplica overrides sobre config dict (in-place).
        Solo modifica claves explícitamente presentes en env.
        """
        applied: list[str] = []

        for field_name in self.model_fields_set:
            value = getattr(self, field_name)
            if value is None:
                continue

            path = _field_to_path(field_name)
            _set_nested(config, path, value)
            applied.append(f"{field_name.upper()}={value!r}")
            logger.info(
                "env_override_applied | path={} value={}",
                ".".join(path),
                value,
            )

        _log_summary(applied)
        return config


# =============================================================================
# HELPERS
# =============================================================================

def _field_to_path(field_name: str) -> Tuple[str, ...]:
    """Convierte nombre de campo a path de config."""
    return tuple(field_name.split("__"))


def _set_nested(d: dict[str, Any], path: Tuple[str, ...], value: Any) -> None:
    """
    Set value en dict anidado creando niveles intermedios.
    Safe: no rompe si faltan niveles.
    """
    current = d
    for key in path[:-1]:
        current = current.setdefault(key, {})
    current[path[-1]] = value


def _log_summary(applied: list[str]) -> None:
    """Log consolidado de overrides."""
    if applied:
        logger.debug(
            "env_overrides_summary | count={} applied={}",
            len(applied),
            " | ".join(applied),
        )
    else:
        logger.debug("env_overrides_summary | count=0")


# =============================================================================
# PUBLIC API
# =============================================================================

def apply_env_overrides(config: dict[str, Any]) -> dict[str, Any]:
    """
    Entry point para loader.
    Flujo:
        1. Parse env → OcmSettings
        2. Apply → config dict
    """
    return OcmSettings().apply(config)
