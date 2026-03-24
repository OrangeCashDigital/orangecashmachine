from __future__ import annotations

"""
core/config/runtime.py
======================

RunConfig — configuración de proceso inmutable.

Centraliza todas las variables de entorno que controlan CÓMO arranca
el proceso, separándolas de AppConfig que controla QUÉ hace la app.

Orden de resolución de debug
-----------------------------
1. OCM_DEBUG seteada explícitamente → usa ese valor
2. OCM_DEBUG no seteada            → default según OCM_ENV
   development / test  → True
   staging / production → False

Orden de resolución de env
---------------------------
1. argumento explícito
2. OCM_ENV
3. settings.yaml → environment.default_env
4. "development"

Principios: inmutable · construido una sola vez · propagado hacia abajo
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from core.config.env_vars import (
    OCM_DEBUG,
    OCM_ENV,
    OCM_CONFIG_PATH,
    OCM_CONFIG_DIR,
    PUSHGATEWAY_URL,
    TRUTHY_VALUES,
    default_debug_for,
)


@dataclass(frozen=True)
class RunConfig:
    """
    Configuración de proceso — inmutable, construida una sola vez al bootstrap.

    Attributes
    ----------
    debug       : bool   — nivel de detalle de logging y diagnósticos
    env         : str    — entorno activo (development|test|staging|production)
    config_path : Path | None — directorio de config YAML
    pushgateway : str    — host:port del Prometheus Pushgateway
    """

    debug:       bool
    env:         str
    config_path: Optional[Path]
    pushgateway: str

    @classmethod
    def from_env(cls, explicit_env: Optional[str] = None) -> "RunConfig":
        """
        Construye RunConfig leyendo el entorno.

        Llama a resolve_env() del loader para mantener consistencia
        en la cascada de resolución de entorno.

        Parameters
        ----------
        explicit_env : str | None
            Entorno explícito (desde CLI o tests). Tiene máxima prioridad.
        """
        # Importación local para evitar ciclo:
        # runtime → loader/env_resolver es seguro porque env_resolver
        # no importa runtime.
        from core.config.loader.env_resolver import resolve_env

        env = resolve_env(explicit_env)

        # Resolución de debug con default inteligente según entorno
        raw_debug = os.getenv(OCM_DEBUG)
        if raw_debug is not None:
            debug = raw_debug.lower() in TRUTHY_VALUES
        else:
            debug = default_debug_for(env)

        # Resolución de config_path — soporta alias legacy OCM_CONFIG_DIR
        raw_path = os.getenv(OCM_CONFIG_PATH) or os.getenv(OCM_CONFIG_DIR)
        config_path = Path(raw_path) if raw_path else None

        pushgateway = os.getenv(PUSHGATEWAY_URL, "localhost:9091")

        return cls(
            debug=debug,
            env=env,
            config_path=config_path,
            pushgateway=pushgateway,
        )

    def __str__(self) -> str:
        return (
            f"RunConfig(env={self.env!r}, debug={self.debug}, "
            f"config_path={self.config_path!r}, pushgateway={self.pushgateway!r})"
        )
