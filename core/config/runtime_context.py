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
from typing import Any, Dict
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

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RuntimeContext":
        """Reconstruye un RuntimeContext a partir de un dict (útil para Prefect)."""
        # app_config
        app_cfg_data = data["app_config"]
        try:
            from core.config.schema import AppConfig as _AppConfig

            app_config = _AppConfig.model_validate(app_cfg_data)  # type: ignore[attr-defined]
        except Exception:
            # fallback si ya es un dict compatible
            app_config = AppConfig(**app_cfg_data)  # type: ignore

        # run_config
        from core.config.runtime import RunConfig as _RunConfig

        run_config = _RunConfig(**data["run_config"])  # type: ignore

        environment = data["environment"]
        run_id = data["run_id"]
        started_at = datetime.fromisoformat(data["started_at"])  # type: ignore[call-arg]
        return cls(
            app_config=app_config,
            run_config=run_config,
            environment=environment,
            run_id=run_id,
            started_at=started_at,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Serializa este RuntimeContext para pasar a flow (Prefect)"""
        # app_config: prefer dict via model_dump if available
        if hasattr(self.app_config, "model_dump"):
            app_config_dump = self.app_config.model_dump()
        else:
            app_config_dump = self.app_config.__dict__
        # run_config: dataclass -> asdict
        try:
            from dataclasses import asdict

            run_config_dump = asdict(self.run_config)
        except Exception:
            run_config_dump = self.run_config.__dict__  # type: ignore
        return {
            "app_config": app_config_dump,
            "run_config": run_config_dump,
            "environment": self.environment,
            "run_id": self.run_id,
            "started_at": self.started_at.isoformat(),
        }
