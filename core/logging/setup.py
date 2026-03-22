"""
core/logging/setup.py
=====================
Configuración centralizada de Loguru para OrangeCashMachine.

Sinks
-----
1. Consola (stderr)          — nivel configurable, coloreada
2. orangecashmachine_*.log   — DEBUG completo, rotación diaria
3. errors_*.log              — WARNING+, retención extendida
4. pipeline_*.log            — módulos propios con bind context
"""

import logging
import sys
from pathlib import Path
from typing import Optional

from loguru import logger

from core.logging.formats import CONSOLE, FILE, PIPELINE
from core.logging.filters import pipeline_filter


DEFAULT_LOG_DIR: Path = Path("logs")


class InterceptHandler(logging.Handler):
    """Redirige stdlib logging (prefect, ccxt, asyncio) a loguru."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = str(record.levelno)
        logger.opt(exception=record.exc_info).log(level, record.getMessage())


def setup_logging(
    debug: bool = False,
    log_dir: Optional[Path] = DEFAULT_LOG_DIR,
) -> None:
    """
    Inicializa Loguru con todos los sinks del sistema.

    Idempotente: si ya hay handlers activos no resetea sinks,
    solo instala el InterceptHandler para stdlib.

    Parameters
    ----------
    debug : bool
        Si True, nivel efectivo = DEBUG y diagnose activado.
    log_dir : Path | None
        Directorio de logs. Si None, solo consola.
    """
    already_configured = len(logger._core.handlers) > 0

    if not already_configured:
        level = "DEBUG" if debug else "INFO"
        logger.remove()

        logger.add(
            sys.stderr,
            level=level,
            format=CONSOLE,
            backtrace=True,
            diagnose=debug,
            colorize=True,
        )

        if log_dir:
            log_dir.mkdir(parents=True, exist_ok=True)

            logger.add(
                log_dir / "orangecashmachine_{time:YYYY-MM-DD}.log",
                rotation="1 day",
                retention="14 days",
                compression="gz",
                level="DEBUG",
                format=FILE,
                backtrace=True,
                diagnose=False,
            )

            logger.add(
                log_dir / "errors_{time:YYYY-MM-DD}.log",
                rotation="1 day",
                retention="30 days",
                compression="gz",
                level="WARNING",
                format=FILE,
                backtrace=True,
                diagnose=False,
            )

            logger.add(
                log_dir / "pipeline_{time:YYYY-MM-DD}.log",
                rotation="1 day",
                retention="14 days",
                compression="gz",
                level="DEBUG",
                format=PIPELINE,
                filter=pipeline_filter,
            )

        logger.debug(
            "Logging configured | level={} log_dir={} sinks=4",
            level,
            log_dir,
        )

    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)
