from __future__ import annotations

"""
core/logging/setup.py
=====================
Configuración centralizada de Loguru para OrangeCashMachine.

Sinks activos (controlados por LoggingConfig)
---------------------------------------------
1. Consola (stderr)          — si cfg.console=True
2. orangecashmachine_*.log   — si cfg.file=True, rotación/retención desde config
3. errors_*.log              — si cfg.file=True, retención extendida
4. pipeline_*.log            — si cfg.pipeline=True
"""

import logging
import sys
from pathlib import Path
from typing import Optional, Union

from loguru import logger

from core.logging.formats import CONSOLE, FILE, PIPELINE
from core.logging.filters import pipeline_filter


class InterceptHandler(logging.Handler):
    """Redirige stdlib logging (prefect, ccxt, asyncio) a loguru."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = str(record.levelno)
        logger.opt(exception=record.exc_info).log(level, record.getMessage())


def setup_logging(
    cfg: Optional[object] = None,
    debug: bool = False,
    # parámetros legacy para retrocompatibilidad
    log_dir: Optional[Path] = None,
) -> None:
    """
    Inicializa Loguru consumiendo LoggingConfig desde el sistema de configuración.

    Idempotente: si ya hay handlers activos no resetea sinks,
    solo instala el InterceptHandler para stdlib.

    Parameters
    ----------
    cfg : LoggingConfig | None
        Configuración de logging desde config.observability.logging.
        Si None, usa defaults seguros (equivalente al comportamiento anterior).
    debug : bool
        Override de nivel: si True fuerza DEBUG independientemente de cfg.level.
    log_dir : Path | None
        Override de directorio (legacy). Ignorado si cfg está presente.
    """
    # Normalizar cfg a valores concretos
    if cfg is not None:
        _level     = "DEBUG" if debug else cfg.level.upper()
        _log_dir   = Path(cfg.log_dir) if cfg.file or cfg.pipeline else None
        _rotation  = cfg.rotation
        _retention = cfg.retention
        _console   = cfg.console
        _file      = cfg.file
        _pipeline  = cfg.pipeline
    else:
        # modo legacy / bootstrap sin config
        _level     = "DEBUG" if debug else "INFO"
        _log_dir   = log_dir or Path("logs")
        _rotation  = "1 day"
        _retention = "14 days"
        _console   = True
        _file      = True
        _pipeline  = True

    already_configured = len(logger._core.handlers) > 0

    if not already_configured:
        logger.remove()

        if _console:
            logger.add(
                sys.stderr,
                level=_level,
                format=CONSOLE,
                backtrace=True,
                diagnose=debug,
                colorize=True,
            )

        if _log_dir and (_file or _pipeline):
            _log_dir.mkdir(parents=True, exist_ok=True)

            if _file:
                logger.add(
                    _log_dir / "orangecashmachine_{time:YYYY-MM-DD}.log",
                    rotation=_rotation,
                    retention=_retention,
                    compression="gz",
                    level="DEBUG",
                    format=FILE,
                    backtrace=True,
                    diagnose=False,
                )

                logger.add(
                    _log_dir / "errors_{time:YYYY-MM-DD}.log",
                    rotation=_rotation,
                    retention="30 days",
                    compression="gz",
                    level="WARNING",
                    format=FILE,
                    backtrace=True,
                    diagnose=False,
                )

            if _pipeline:
                logger.add(
                    _log_dir / "pipeline_{time:YYYY-MM-DD}.log",
                    rotation=_rotation,
                    retention=_retention,
                    compression="gz",
                    level="DEBUG",
                    format=PIPELINE,
                    filter=pipeline_filter,
                )

        sinks = sum([_console, _file * 2 if _file else 0, _pipeline])
        logger.debug(
            "Logging configured | level={} log_dir={} console={} file={} pipeline={}",
            _level, _log_dir, _console, _file, _pipeline,
        )

    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)
