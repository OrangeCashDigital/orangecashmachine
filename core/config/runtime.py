from __future__ import annotations
import re

"""
core/config/runtime.py
======================

:class:`RunConfig` — configuración de proceso inmutable.

Centraliza todas las variables de entorno que controlan **cómo** arranca
el proceso, separándolas de :class:`~core.config.schema.AppConfig` que
controla **qué** hace la app.

Orden de resolución de ``debug``
---------------------------------
1. ``OCM_DEBUG`` seteada → usa ese valor.
2. ``OCM_DEBUG`` no seteada → default por entorno:
   - ``development`` / ``test``   → ``True``
   - ``staging`` / ``production`` → ``False``

Orden de resolución de ``env``
------------------------------
1. Argumento explícito.
2. ``OCM_ENV``.
3. ``settings.yaml → environment.default_env``.
4. ``"development"`` (default final).

Principios: inmutable · construido una sola vez · propagado hacia abajo.
"""

import os
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from core.config.env_vars import (
    OCM_CONFIG_DIR,
    OCM_CONFIG_PATH,
    OCM_DEBUG,
    OCM_VALIDATE_ONLY,
    PUSHGATEWAY_URL,
    TRUTHY_VALUES,
    default_debug_for,
)


@dataclass(frozen=True)
class RunConfig:
    """Configuración de proceso — inmutable, construida una sola vez al bootstrap.

    Attributes:
        debug: Activa logging verboso y diagnósticos extra.
        env: Entorno activo (development | test | staging | production).
        config_path: Directorio de config YAML, o None si no fue especificado.
        pushgateway: ``host:port`` del Prometheus Pushgateway.
    """

    env: str
    debug: bool
    validate_only: bool
    run_id: str
    config_path: Optional[Path]
    pushgateway: str

    @classmethod
    def from_env(cls, explicit_env: Optional[str] = None) -> RunConfig:
        """Construye RunConfig leyendo variables de entorno.

        La importación de ``env_resolver`` es local para evitar ciclo:
        ``runtime → loader/env_resolver`` es seguro porque ``env_resolver``
        no importa ``runtime``.

        Args:
            explicit_env: Entorno explícito (CLI o tests). Máxima prioridad.

        Returns:
            RunConfig inmutable con los valores resueltos del entorno.
        """
        from core.config.loader.env_resolver import resolve_env

        env = resolve_env(explicit_env)

        raw_debug = os.getenv(OCM_DEBUG)
        debug = (
            raw_debug.lower() in TRUTHY_VALUES
            if raw_debug is not None
            else default_debug_for(env)
        )

        raw_path = os.getenv(OCM_CONFIG_PATH) or os.getenv(OCM_CONFIG_DIR)
        config_path = Path(raw_path) if raw_path else None

        validate_only: bool = (
            os.getenv(OCM_VALIDATE_ONLY, "").lower() in TRUTHY_VALUES
        )

        pushgateway = os.getenv(PUSHGATEWAY_URL, "localhost:9091")
        # push_to_gateway requiere host:port, no URL completa
        pushgateway = re.sub(r"^https?://", "", pushgateway)

        return cls(
            env=env,
            debug=debug,
            validate_only=validate_only,
            run_id=uuid.uuid4().hex[:12],
            config_path=config_path,
            pushgateway=pushgateway,
        )

    def __str__(self) -> str:
        return (
            f"RunConfig(env={self.env!r}, debug={self.debug}, "
            f"validate_only={self.validate_only}, run_id={self.run_id!r}, "
            f"config_path={self.config_path!r}, pushgateway={self.pushgateway!r})"
        )
