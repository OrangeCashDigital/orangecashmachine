from __future__ import annotations

"""
core/config/runtime_context.py
================================

RuntimeContext — contexto inmutable de ejecución.

Construido una sola vez en build_context() (entrypoint local)
o en run_application() (path Hydra/producción), y propagado
hacia abajo sin recomputarse.

Principio: inmutable · construido una vez · inyectado hacia abajo.
"""

from dataclasses import dataclass
from datetime import datetime

from core.config.schema import AppConfig
from core.config.runtime import RunConfig


@dataclass(frozen=True)
class RuntimeContext:
    """Contexto de ejecución inmutable para un run completo.

    Attributes:
        app_config:   Configuración validada de la aplicación.
        run_config:   Configuración de proceso (env, debug, run_id…).
        environment:  Nombre del entorno activo (shortcut de run_config.env).
        run_id:       Identificador único del run (timestamp + uuid).
        started_at:   Momento de arranque (UTC).
    """

    app_config: AppConfig
    run_config: RunConfig
    environment: str
    run_id: str
    started_at: datetime

    @property
    def pushgateway(self) -> str:
        """Gateway de métricas resuelto desde RunConfig — SSoT."""
        return self.run_config.pushgateway
