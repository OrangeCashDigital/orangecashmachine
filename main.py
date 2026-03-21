from __future__ import annotations

"""
main.py – Entrypoint principal de OrangeCashMachine
===================================================

Responsabilidad
---------------
- Inicializar logging centralizado.
- Cargar y validar configuración multi-entorno.
- Ejecutar pipeline principal de trading / market data.
- Manejar errores críticos de forma segura y trazable.

Principios aplicados
-------------------
- SOLID: SRP → main solo orquesta el arranque.
- KISS: flujo lineal y predecible.
- DRY: reuso de loader y pipeline centralizados.
- SafeOps: logging estructurado, exit codes y excepciones rastreables.
"""

import sys
import os
from pathlib import Path
from typing import Optional, Union

from loguru import logger

from core.config.loader import load_config
from core.config.schema import AppConfig
from market_data.orchestration.entrypoint import run as run_main_pipeline


# ============================================================================
# Constants
# ============================================================================

LOG_DIR: Path = Path("logs")

_LOG_FORMAT_CONSOLE = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level:<8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{line}</cyan> | "
    "{message}"
)

_LOG_FORMAT_FILE = (
    "{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {name}:{line} | {message}"
)


# ============================================================================
# Logging Setup
# ============================================================================

def setup_logging(debug: bool = False, log_dir: Optional[Path] = LOG_DIR) -> None:
    """
    Configura Loguru como sistema de logging global.

    Incluye:
    - Consola coloreada y backtrace
    - Archivo rotativo diario con retención de 14 días y compresión
    """
    level = "DEBUG" if debug else "INFO"
    logger.remove()

    # Consola
    logger.add(
        sys.stderr,
        level=level,
        format=_LOG_FORMAT_CONSOLE,
        backtrace=True,
        diagnose=debug,
        colorize=True,
    )

    # Archivo
    if log_dir:
        log_dir.mkdir(parents=True, exist_ok=True)
        logger.add(
            log_dir / "orangecashmachine_{time:YYYY-MM-DD}.log",
            rotation="1 day",
            retention="14 days",
            compression="gz",
            level="DEBUG",  # archivo siempre DEBUG para trazabilidad completa en produccion
            format=_LOG_FORMAT_FILE,
            backtrace=True,
            diagnose=False,
        )

    logger.debug("Logging configured | level={} log_dir={}", level, log_dir)


# ============================================================================
# Configuration Loader
# ============================================================================

def initialize_config(env: Optional[str] = None, path: Optional[Union[str, Path]] = None) -> AppConfig:
    """
    Carga y valida la configuración central del sistema.

    Parameters
    ----------
    env : str | None
        Entorno opcional (dev, prod, staging)
    path : str | Path | None
        Ruta a archivo o directorio de configuración

    Returns
    -------
    AppConfig
        Configuración validada.
    """
    try:
        config = load_config(env=env, path=Path(path) if path else None)
        logger.info("Configuración cargada correctamente | exchanges=%s", config.exchange_names)
        return config
    except Exception:
        logger.exception("Fallo al cargar la configuración")
        raise


# ============================================================================
# Main Entrypoint
# ============================================================================

def main(env: Optional[str] = None, config_path: Optional[Union[str, Path]] = None, debug: bool = False) -> None:
    """
    Punto de entrada principal de OrangeCashMachine.

    Parameters
    ----------
    env : str | None
        Entorno a utilizar (override vía OCM_ENV)
    config_path : str | Path | None
        Ruta al archivo de configuración (override vía OCM_CONFIG_PATH)
    debug : bool
        Activa logging DEBUG
    """
    try:
        setup_logging(debug=debug)
        config = initialize_config(env=env, path=config_path)

        # Ejecutar pipeline principal
        run_main_pipeline(config)

        logger.info("Pipeline principal ejecutado correctamente.")
        sys.exit(0)

    except Exception:
        logger.exception("Error crítico al iniciar la aplicación")
        sys.exit(1)


# ============================================================================
# CLI / Execution
# ============================================================================

if __name__ == "__main__":
    env_override = os.getenv("OCM_ENV")
    config_path_override = os.getenv("OCM_CONFIG_PATH")

    debug_mode = os.getenv("OCM_DEBUG", "false").lower() in ("1", "true", "yes")
    main(
        env=env_override,
        config_path=Path(config_path_override) if config_path_override else None,
        debug=debug_mode,
    )