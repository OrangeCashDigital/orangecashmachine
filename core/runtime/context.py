from __future__ import annotations

"""
core/runtime/context.py
========================

RuntimeContext — contexto inmutable de ejecución.

Construido una sola vez en run_application() y propagado hacia abajo
sin recomputarse.

Ubicación canónica: core/runtime/ (no core/config/)
Razón: RuntimeContext es el estado del sistema vivo durante un run,
no configuración declarativa. Separación por SRP y Clean Architecture.

Principio: inmutable · construido una vez · inyectado hacia abajo.

SSOT:
    environment → run_config.env         (property, no campo)
    run_id      → run_config.run_id      (property, no campo)
    pushgateway → run_config.pushgateway (property, no campo)
    debug       → run_config.debug       (property, no campo)
"""

from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from core.config.schema import AppConfig
from core.runtime.run_config import RunConfig


def _serialize_run_config(rc: RunConfig) -> Dict[str, Any]:
    """Serializa RunConfig a dict JSON-safe.

    Path → str para sobrevivir round-trip JSON (Prefect workers).
    None permanece None.
    """
    d = asdict(rc)
    if d.get("config_path") is not None:
        d["config_path"] = str(d["config_path"])
    return d


def _deserialize_run_config(data: Dict[str, Any]) -> RunConfig:
    """Reconstruye RunConfig desde dict JSON.

    str → Path para config_path. Fail-fast si faltan campos obligatorios.
    """
    d = dict(data)
    raw_path: Optional[str] = d.get("config_path")
    d["config_path"] = Path(raw_path) if raw_path is not None else None
    return RunConfig(**d)


@dataclass(frozen=True)
class RuntimeContext:
    """Contexto de ejecución inmutable para un run completo.

    Attributes:
        app_config:  Configuración validada de la aplicación (Pydantic).
        run_config:  Configuración de proceso (env, debug, run_id, pushgateway…).
        started_at:  Momento de arranque UTC.
    """

    app_config: AppConfig
    run_config: RunConfig
    started_at: datetime

    # ------------------------------------------------------------------
    # Shortcuts de solo lectura — SSOT en run_config, no campos propios
    # ------------------------------------------------------------------

    @property
    def environment(self) -> str:
        """Entorno activo — delegado a run_config (SSOT)."""
        return self.run_config.env

    @property
    def run_id(self) -> str:
        """Identificador del run — delegado a run_config (SSOT)."""
        return self.run_config.run_id

    @property
    def pushgateway(self) -> str:
        """Gateway de métricas — delegado a run_config (SSOT)."""
        return self.run_config.pushgateway

    @property
    def debug(self) -> bool:
        """Modo debug — delegado a run_config (SSOT)."""
        return self.run_config.debug

    # ------------------------------------------------------------------
    # Serialización para Prefect / workers
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        """Serializa para pasar a Prefect flows.

        Garantiza JSON-safety: Path → str, datetime → ISO 8601.
        """
        app_config_dump = (
            self.app_config.model_dump()
            if hasattr(self.app_config, "model_dump")
            else self.app_config.__dict__
        )
        return {
            "app_config": app_config_dump,
            "run_config": _serialize_run_config(self.run_config),
            "started_at": self.started_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> RuntimeContext:
        """Reconstruye desde dict (deserialización Prefect).

        Fail-fast si faltan claves obligatorias.
        """
        app_config = AppConfig.model_validate(data["app_config"])
        run_config = _deserialize_run_config(data["run_config"])
        started_at = datetime.fromisoformat(data["started_at"])
        return cls(
            app_config=app_config,
            run_config=run_config,
            started_at=started_at,
        )
