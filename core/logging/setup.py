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

Extra defaults
--------------
logger.configure(patcher=...) inyecta defaults en cada record ANTES de
que llegue a los sinks. Esto evita KeyError en CONSOLE y FILE cuando
un log no proviene de un contexto con logger.bind(run_id=...).
"""

import logging
import sys
from pathlib import Path
from typing import Optional

from loguru import logger

from core.logging.formats import CONSOLE, FILE, PIPELINE
from core.logging.filters import pipeline_filter

# Flag de módulo: reemplaza logger._core.handlers (API privada de loguru).
# Inmune a cambios de API en minor releases.
_LOGGING_CONFIGURED: bool = False


class InterceptHandler(logging.Handler):
    """Redirige stdlib logging (prefect, ccxt, asyncio) a loguru."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = str(record.levelno)

        # Sube en el call stack hasta salir del propio logging de stdlib,
        # para que loguru registre el módulo que emitió el log, no el handler.
        frame, depth = logging.currentframe(), 0
        while frame and (
            frame.f_code.co_filename == logging.__file__
            or frame.f_globals.get("__name__") == __name__
        ):
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


def _make_patcher(run_id: Optional[str] = None):
    """
    Retorna un patcher que inyecta defaults en extra antes de los sinks.
    Si run_id está disponible, lo inyecta globalmente desde el primer log.
    """
    _run_id = run_id or "-"

    def _patch_extra(record: dict) -> None:
        record["extra"].setdefault("run_id", _run_id)
        record["extra"].setdefault("trace_id", _run_id)  # hoy = run_id, futuro: OTel span

    return _patch_extra


def setup_logging(
    cfg: Optional[object] = None,
    debug: bool = False,
    log_dir: Optional[Path] = None,
    run_id: Optional[str] = None,
) -> None:
    """
    Inicializa Loguru consumiendo LoggingConfig desde el sistema de configuración.

    Idempotente: la segunda llamada (desde entrypoint.py cuando se invoca
    desde main.py) instala el InterceptHandler pero no resetea sinks.

    Parameters
    ----------
    cfg : LoggingConfig | None
        Configuración desde config.observability.logging.
        Si None, usa defaults seguros.
    debug : bool
        Si True fuerza nivel DEBUG independientemente de cfg.level.
    log_dir : Path | None
        Override de directorio (legacy). Ignorado si cfg está presente.
    run_id : str | None
        Identificador de ejecución. Si se provee, se inyecta globalmente
        en todos los records vía patcher desde el primer log.
    """
    global _LOGGING_CONFIGURED

    if cfg is not None:
        _level     = "DEBUG" if debug else cfg.level.upper()
        _log_dir   = Path(cfg.log_dir) if cfg.file or cfg.pipeline else None
        _rotation  = cfg.rotation
        _retention = cfg.retention
        _console   = cfg.console
        _file      = cfg.file
        _pipeline  = cfg.pipeline
    else:
        _level     = "DEBUG" if debug else "INFO"
        _log_dir   = log_dir or Path("logs")
        _rotation  = "1 day"
        _retention = "14 days"
        _console   = True
        _file      = True
        _pipeline  = True

    if not _LOGGING_CONFIGURED:
        logger.remove()

        # Patcher global: inyecta run_id en todos los records desde el inicio.
        # Si run_id está disponible, aparece en logs de arranque (config/loader).
        # Si no, usa "-" como fallback seguro.
        logger.configure(patcher=_make_patcher(run_id))

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

        _LOGGING_CONFIGURED = True
        logger.debug(
            "Logging configured | level={} log_dir={} console={} file={} pipeline={}",
            _level, _log_dir, _console, _file, _pipeline,
        )

    # Siempre — instala el bridge stdlib→loguru aunque los sinks ya existan.
    # force=True garantiza que reemplaza cualquier handler previo de basicConfig.
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)
