from __future__ import annotations

"""
orchestration/entrypoint.py
===========================

Entrypoint operativo del flujo de Market Data de OrangeCashMachine.

Responsabilidad
---------------
- Configurar logging centralizado y redirigir logs externos a Loguru.
- Ejecutar el flow principal de ingestión de datos de mercado.
- Manejar errores críticos y códigos de salida.

Principios aplicados
-------------------
- SOLID: SRP → este módulo solo arranca el flujo.
- KISS: flujo lineal, sin abstracciones innecesarias.
- DRY: logging centralizado y reutilizable.
- SafeOps: manejo explícito de errores, KeyboardInterrupt y métricas.
"""

import asyncio
import logging
import sys
from pathlib import Path
from typing import Optional, Union

from loguru import logger
from services.observability.metrics import push_metrics

from market_data.orchestration.flows.batch_flow import market_data_flow


# ============================================================================
# Constants
# ============================================================================

DEFAULT_CONFIG_PATH: Path = Path("config/settings.yaml")
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
# Logging
# ============================================================================

class InterceptHandler(logging.Handler):
    """
    Redirige logs del módulo `logging` hacia Loguru.
    Permite unificar logs de librerías externas (Prefect, CCXT, Pandas, etc.)
    """

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = str(record.levelno)

        frame = logging.currentframe()
        depth = 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def setup_logging(debug: bool = False, log_dir: Optional[Path] = LOG_DIR) -> None:
    """
    Configura Loguru como sistema global de logging.
    - Consola coloreada
    - Archivo rotativo diario
    - Intercepta logging estándar de librerías externas
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

    # Archivo rotativo
    if log_dir:
        log_dir.mkdir(parents=True, exist_ok=True)
        logger.add(
            log_dir / "market_data_{time:YYYY-MM-DD}.log",
            rotation="1 day",
            retention="14 days",
            compression="gz",
            level=level,
            format=_LOG_FORMAT_FILE,
            encoding="utf-8",
            backtrace=True,
            diagnose=False,
        )

    # Intercepta logging estándar
    intercept = InterceptHandler()
    logging.basicConfig(handlers=[intercept], level=0, force=True)

    for lib_name in ("prefect", "prefect.flow_runs", "prefect.task_runs"):
        lib_logger = logging.getLogger(lib_name)
        lib_logger.handlers = [intercept]
        lib_logger.propagate = False

    logger.debug("Logging configured | level={} log_dir={}", level, log_dir)


# ============================================================================
# Flow runner
# ============================================================================

async def _run_flow(config_path: Path) -> None:
    """
    Ejecuta el flow principal de ingestión de Market Data.

    Raises
    ------
    Exception: cualquier fallo dentro de market_data_flow se propaga.
    """
    logger.info("Launching market_data_flow | config_path=%s", config_path)
    await market_data_flow(config_path=config_path)
    logger.info("market_data_flow completed successfully")


# ============================================================================
# Public entrypoint
# ============================================================================

def run(
    debug: bool = False,
    config_path: Optional[Union[str, Path]] = None,
    log_dir: Optional[Path] = LOG_DIR,
) -> None:
    """
    EntryPoint operativo para el flujo de Market Data.

    Configura logging y ejecuta el flow principal.

    Parameters
    ----------
    debug : bool
        Habilita nivel DEBUG.
    config_path : str | Path | None
        Ruta al archivo de configuración.
    log_dir : Path | None
        Directorio de logs.
    """
    resolved_config = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
    setup_logging(debug=debug, log_dir=log_dir)

    logger.info("OrangeCashMachine starting | config=%s debug=%s", resolved_config, debug)

    try:
        asyncio.run(_run_flow(resolved_config))
    except KeyboardInterrupt:
        logger.warning("Execution interrupted by user")
        sys.exit(0)
    except Exception:
        logger.exception("Market Data Flow terminated with errors. Check logs.")
        sys.exit(1)
    finally:
        push_metrics()
        logger.info("OrangeCashMachine finished successfully.")


# ============================================================================
# CLI execution
# ============================================================================

if __name__ == "__main__":
    # Ejecuta para pruebas locales o debugging
    run(debug=True)

    # Nota: en producción Prefect se encarga de orquestar el flujo:
    # market_data_flow.serve(
    #     name="market-data-ingestion",
    #     cron="0 * * * *",
    #     parameters={"config_path": "config/settings.yaml"},
    # )