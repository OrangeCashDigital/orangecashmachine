from __future__ import annotations

"""
ocm_platform/runtime/context.py
=================================

:class:`RuntimeContext` — contexto inmutable de ejecución.

Construido una sola vez en ``run_application()`` y propagado hacia abajo
sin recomputarse.

Principio: inmutable · construido una vez · inyectado hacia abajo.

SSOT:
    environment → run_config.env           (property delegada)
    run_id      → run_config.run_id        (property delegada)
    pushgateway → run_config.pushgateway   (property delegada)
    debug       → run_config.debug         (property delegada)

Serialización:
    La lógica de serialización de :class:`~ocm_platform.runtime.run_config.RunConfig`
    vive en ``RunConfig.to_dict()`` / ``RunConfig.from_dict()`` (SRP).
    RuntimeContext solo se responsabiliza de su propio envelope.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from ocm_platform.config.schema import AppConfig
from ocm_platform.runtime.run_config import RunConfig


@dataclass(frozen=True)
class RuntimeContext:
    """Contexto de ejecución inmutable para un run completo.

    Attributes:
        app_config:  Configuración validada de la aplicación (Pydantic).
        run_config:  Configuración de proceso (env, debug, run_id, pushgateway…).
        started_at:  Momento de arranque UTC (timezone-aware).
    """

    app_config: AppConfig
    run_config: RunConfig
    started_at: datetime

    # ------------------------------------------------------------------
    # Fail-Fast — invariantes garantizadas en construcción
    # ------------------------------------------------------------------

    def __post_init__(self) -> None:
        if not isinstance(self.run_config, RunConfig):
            raise TypeError(
                f"run_config debe ser RunConfig, recibido {type(self.run_config).__name__}"
            )
        if not isinstance(self.app_config, AppConfig):
            raise TypeError(
                f"app_config debe ser AppConfig, recibido {type(self.app_config).__name__}"
            )
        if self.started_at.tzinfo is None:
            raise ValueError(
                "started_at debe ser timezone-aware (UTC). "
                "Usa datetime.now(timezone.utc) en vez de datetime.utcnow()."
            )

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

    def to_dict(self) -> dict[str, Any]:
        """Serializa para pasar a Prefect flows.

        Garantiza JSON-safety:
        - ``AppConfig`` → ``model_dump()`` (Pydantic v2, siempre disponible).
        - ``RunConfig`` → ``RunConfig.to_dict()`` (SRP: lógica en RunConfig).
        - ``datetime`` → ISO 8601 con timezone.
        """
        return {
            "app_config": self.app_config.model_dump(mode="json"),
            "run_config": self.run_config.to_dict(),
            "started_at": self.started_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RuntimeContext:
        """Reconstruye desde dict (deserialización Prefect).

        Args:
            data: Dict producido por :meth:`to_dict`.

        Returns:
            RuntimeContext reconstruido.

        Raises:
            KeyError:   Si faltan claves obligatorias.
            ValueError: Si ``started_at`` no es parseable o no tiene timezone.
            TypeError:  Si los sub-objetos no pasan sus propias validaciones.
        """
        try:
            app_config = AppConfig.model_validate(data["app_config"])
            run_config = RunConfig.from_dict(data["run_config"])
            started_at = datetime.fromisoformat(data["started_at"])
        except KeyError as exc:
            raise KeyError(
                f"RuntimeContext.from_dict: clave obligatoria ausente — {exc}. "
                f"Claves recibidas: {list(data.keys())}"
            ) from exc
        except (ValueError, TypeError) as exc:
            raise type(exc)(
                f"RuntimeContext.from_dict: datos inválidos — {exc}"
            ) from exc

        # Garantizar timezone-awareness tras deserialización
        if started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=timezone.utc)

        return cls(
            app_config=app_config,
            run_config=run_config,
            started_at=started_at,
        )
