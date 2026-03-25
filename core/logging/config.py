from __future__ import annotations

"""
core/logging/config.py
======================

LoggingConfig — schema de configuración de logging.

Vive en core/logging para que el módulo de logging sea autónomo:
no depende de core/config/schema para saber cómo configurarse.

core/config/schema.py importa desde aquí — no al revés.
"""

from pydantic import BaseModel, ConfigDict


class LoggingConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    level:     str  = "INFO"
    log_dir:   str  = "logs"
    rotation:  str  = "1 day"
    retention: str  = "14 days"
    console:   bool = True
    file:      bool = True
    pipeline:  bool = True
