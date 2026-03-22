from __future__ import annotations

"""
main.py – Entrypoint principal de OrangeCashMachine
===================================================

Responsabilidad: orquestar arranque → logging → config → pipeline.
No contiene lógica de negocio. main() retorna int, nunca llama sys.exit().

Principios: SOLID · KISS · DRY · SafeOps
"""

import os
import sys
from pathlib import Path
from typing import Optional, Union

from loguru import logger

from core.config.loader import load_config
from core.config.schema import AppConfig
from core.logging import setup_logging
from market_data.orchestration.entrypoint import run as run_pipeline


def initialize_config(
    env: Optional[str] = None,
    path: Optional[Union[str, Path]] = None,
) -> AppConfig:
    """Carga y valida la configuración central del sistema."""
    try:
        config = load_config(env=env, path=Path(path) if path else None)
        logger.info("Configuración cargada | exchanges={}", config.exchange_names)
        return config
    except Exception:
        logger.exception("Fallo al cargar la configuración")
        raise


def main(
    env: Optional[str] = None,
    config_path: Optional[Union[str, Path]] = None,
    debug: bool = False,
) -> int:
    """
    Punto de entrada principal.

    Returns
    -------
    int
        0 → éxito, 1 → error crítico.
    """
    try:
        setup_logging(debug=debug)
        config = initialize_config(env=env, path=config_path)
        return run_pipeline(config=config, debug=debug)
    except Exception:
        logger.exception("Error crítico durante el arranque")
        return 1


if __name__ == "__main__":
    sys.exit(
        main(
            env=os.getenv("OCM_ENV"),
            config_path=(
                Path(p) if (p := os.getenv("OCM_CONFIG_PATH")) else None
            ),
            debug=os.getenv("OCM_DEBUG", "false").lower() in ("1", "true", "yes"),
        )
    )
