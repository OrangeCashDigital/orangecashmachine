from __future__ import annotations

"""
core/logging/config.py
======================

LoggingConfig — schema de configuración de logging.

Vive en core/logging para que el módulo sea autónomo:
core/config/schema.py importa desde aquí, no al revés.

Campos nuevos v0.3.0
--------------------
loki_url    : URL del Loki push endpoint.
              None → sink Loki deshabilitado.
              Ejemplo: "http://localhost:3100/loki/api/v1/push"

loki_labels : Labels estáticos adicionales enviados a Loki.
              run_id, env y service se inyectan automáticamente.
"""

from typing import Optional
from pydantic import BaseModel, ConfigDict, field_validator


class LoggingConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    # ── Nivel y archivos locales ──────────────────────────────────────
    level:     str  = "INFO"
    log_dir:   str  = "logs"
    rotation:  str  = "1 day"
    retention: str  = "14 days"
    console:   bool = True
    file:      bool = True
    pipeline:  bool = True

    # ── Loki (sink remoto) ────────────────────────────────────────────
    loki_url:    Optional[str]       = None
    loki_labels: dict[str, str]      = {}

    @field_validator("level")
    @classmethod
    def _validate_level(cls, v: str) -> str:
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        upper = v.upper()
        if upper not in allowed:
            raise ValueError(f"level must be one of {allowed}, got {v!r}")
        return upper
