from __future__ import annotations

"""
ocm_platform/runtime/run_config.py
====================================

:class:`RunConfig` — configuración de proceso inmutable.

Centraliza las variables de entorno que controlan **cómo** arranca el proceso,
separadas de :class:`~ocm_platform.config.schema.AppConfig` que controla **qué**
hace la app.

Ubicación canónica: ocm_platform/runtime/
Razón: RunConfig describe el sistema en ejecución, no la configuración
declarativa. Separación por SRP y Clean Architecture.

Orden de resolución de ``debug``
---------------------------------
1. ``OCM_DEBUG`` seteada  → usa ese valor (BOOL_TRUE).
2. ``OCM_DEBUG`` no seteada → default por entorno:
   - ``development`` / ``test``   → ``True``
   - ``staging`` / ``production`` → ``False``

Orden de resolución de ``env``
------------------------------
1. Argumento explícito (CLI / tests).
2. ``OCM_ENV``.
3. ``settings.yaml → environment.default_env``.
4. ``"development"`` (fallback final).

Principios: inmutable · construido una sola vez · propagado hacia abajo.
"""

import os
import re
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from ocm_platform.config.env_vars import (
    OCM_CONFIG_DIR,
    OCM_CONFIG_PATH,
    OCM_DEBUG,
    OCM_VALIDATE_ONLY,
    PUSHGATEWAY_URL,
    default_debug_for,
)
from ocm_platform.config.layers.coercion import BOOL_TRUE  # SSOT — única fuente para bool strings

# ---------------------------------------------------------------------------
# Constantes de módulo — compiladas una vez (DRY + eficiencia)
# ---------------------------------------------------------------------------

VALID_ENVS: frozenset[str] = frozenset({"development", "test", "staging", "production"})

_PROTO_RE = re.compile(r"^https?://")

# run_id: 20 hex chars = 80 bits de entropía.
# P(colisión) < 1e-9 hasta ~1 M de runs simultáneos. (hex[:12] = 48 bits → p=1% en 6 M runs)
_RUN_ID_BYTES: int = 10


# ---------------------------------------------------------------------------
# RunConfig
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RunConfig:
    """Configuración de proceso — inmutable, construida una sola vez al bootstrap.

    Attributes:
        env:           Entorno activo (development | test | staging | production).
        debug:         Activa logging verboso y diagnósticos extra.
        validate_only: Solo valida config, no ejecuta pipeline.
        run_id:        Identificador único del run (20 hex chars / 80 bits).
        config_path:   Directorio de config YAML, o ``None`` si no fue especificado.
        pushgateway:   ``host:port`` del Prometheus Pushgateway.
    """

    env: str
    debug: bool
    validate_only: bool
    run_id: str
    config_path: Path | None
    pushgateway: str

    # ------------------------------------------------------------------
    # Fail-Fast — validación en construcción, no en uso
    # ------------------------------------------------------------------

    def __post_init__(self) -> None:
        if self.env not in VALID_ENVS:
            raise ValueError(
                f"env={self.env!r} no es válido. "
                f"Valores permitidos: {sorted(VALID_ENVS)}"
            )
        if not self.run_id:
            raise ValueError("run_id no puede ser vacío.")

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_env(cls, explicit_env: str | None = None) -> RunConfig:
        """Construye RunConfig leyendo variables de entorno.

        El import de ``env_resolver`` es local para romper el ciclo de importación:
        ``run_config → loader/env_resolver`` es seguro porque ``env_resolver``
        no importa ``run_config`` (verificado con ``import-linter``).

        Args:
            explicit_env: Entorno explícito (CLI o tests). Máxima prioridad.

        Returns:
            RunConfig inmutable con los valores resueltos del entorno.

        Raises:
            ValueError: Si ``env`` resuelto no pertenece a :data:`VALID_ENVS`.
        """
        # Local import para romper ciclo. No mover a nivel de módulo.
        from ocm_platform.config.loader.env_resolver import resolve_env

        env = resolve_env(explicit_env)

        raw_debug = os.getenv(OCM_DEBUG)
        debug: bool = (
            raw_debug.lower() in BOOL_TRUE
            if raw_debug is not None
            else default_debug_for(env)
        )

        raw_path = os.getenv(OCM_CONFIG_PATH) or os.getenv(OCM_CONFIG_DIR)
        config_path = Path(raw_path) if raw_path else None

        validate_only: bool = os.getenv(OCM_VALIDATE_ONLY, "").lower() in BOOL_TRUE

        raw_gw = os.getenv(PUSHGATEWAY_URL, "localhost:9091")
        pushgateway = _PROTO_RE.sub("", raw_gw)  # push_to_gateway requiere host:port sin schema

        return cls(
            env=env,
            debug=debug,
            validate_only=validate_only,
            run_id=uuid.uuid4().bytes.hex()[:_RUN_ID_BYTES * 2],
            config_path=config_path,
            pushgateway=pushgateway,
        )

    # ------------------------------------------------------------------
    # Serialización — la lógica vive en RunConfig, no en el caller (SRP)
    # ------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """Serializa a dict JSON-safe.

        ``Path`` → ``str`` para sobrevivir round-trip JSON (Dagster / OCMResource).
        ``None`` permanece ``None``.
        """
        d = asdict(self)
        if d.get("config_path") is not None:
            d["config_path"] = str(d["config_path"])
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RunConfig:
        """Reconstruye desde dict JSON.

        Args:
            data: Dict producido por :meth:`to_dict`.

        Returns:
            RunConfig reconstruido.

        Raises:
            KeyError:   Si faltan campos obligatorios.
            ValueError: Si ``env`` no es válido (Fail-Fast vía ``__post_init__``).
            TypeError:  Si los tipos no son compatibles.
        """
        try:
            d = dict(data)
            raw_path: str | None = d.get("config_path")
            d["config_path"] = Path(raw_path) if raw_path is not None else None
            return cls(**d)
        except (KeyError, TypeError) as exc:
            raise TypeError(
                f"RunConfig.from_dict: datos inválidos — {exc}. "
                f"Claves recibidas: {list(data.keys())}"
            ) from exc
