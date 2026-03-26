from __future__ import annotations

"""
core/logging/setup.py
=====================

Configuración centralizada de logging basada en Loguru.

Principios aplicados:
- SOLID: separación de responsabilidades (config, sinks, patcher)
- DRY: eliminación de duplicación en creación de sinks
- KISS: lógica clara y predecible
- SafeOps: defaults seguros + tolerancia a fallos
"""

import logging as std_logging
import sys
from pathlib import Path
from typing import Optional, Dict, Any

from loguru import logger

from core.logging.config import LoggingConfig
from core.logging.formats import CONSOLE, FILE, PIPELINE
from core.logging.filters import pipeline_filter

# ---------------------------------------------------------------------
# Estado global (fast-path, la guardia real es _has_active_sinks)
# ---------------------------------------------------------------------
_LOGGING_CONFIGURED: bool = False


# ---------------------------------------------------------------------
# Helpers de idempotencia
# ---------------------------------------------------------------------
def _has_active_sinks() -> bool:
    """
    Devuelve True si loguru ya tiene sinks registrados.

    Usa logger._core.handlers — API privada pero estable desde loguru 0.5.
    Si loguru cambia esta API en el futuro, el except AttributeError
    actúa como fallback seguro devolviendo False.
    """
    try:
        return bool(logger._core.handlers)  # type: ignore[attr-defined]
    except AttributeError:
        return False


# ---------------------------------------------------------------------
# Intercept handler (stdlib → loguru)
# ---------------------------------------------------------------------
class InterceptHandler(std_logging.Handler):
    """Redirige logging estándar hacia Loguru."""

    def emit(self, record: std_logging.LogRecord) -> None:
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = str(record.levelno)

        frame, depth = std_logging.currentframe(), 2
        while frame and (
            frame.f_code.co_filename == std_logging.__file__
            or frame.f_globals.get("__name__") == __name__
        ):
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


# ---------------------------------------------------------------------
# Configuración base
# ---------------------------------------------------------------------
def _resolve_config(
    cfg: Optional[LoggingConfig],
    debug: bool,
    log_dir: Optional[Path],
) -> Dict[str, Any]:
    """Normaliza LoggingConfig externa a un dict interno consistente."""

    if cfg:
        return {
            "level":     "DEBUG" if debug else cfg.level.upper(),
            "log_dir":   Path(cfg.log_dir) if (cfg.file or cfg.pipeline) else None,
            "rotation":  cfg.rotation,
            "retention": cfg.retention,
            "console":   cfg.console,
            "file":      cfg.file,
            "pipeline":  cfg.pipeline,
        }

    return {
        "level":     "DEBUG" if debug else "INFO",
        "log_dir":   log_dir or Path("logs"),
        "rotation":  "1 day",
        "retention": "14 days",
        "console":   True,
        "file":      True,
        "pipeline":  True,
    }


# ---------------------------------------------------------------------
# Patcher (inyección global de contexto)
# ---------------------------------------------------------------------
def _make_patcher(run_id: Optional[str]):

    _run_id = run_id or "-"

    def _patch(record: dict) -> None:
        extra = record["extra"]
        extra.setdefault("run_id", _run_id)
        extra.setdefault("service", "orangecashmachine")
        extra.setdefault("env", "dev")

    return _patch


# ---------------------------------------------------------------------
# Sinks
# ---------------------------------------------------------------------
def _add_console_sink(level: str, debug: bool) -> None:
    logger.add(
        sys.stderr,
        level=level,
        format=CONSOLE,
        backtrace=True,
        diagnose=debug,
        colorize=True,
    )


def _add_file_sinks(cfg: Dict[str, Any]) -> None:
    log_dir: Path = cfg["log_dir"]
    log_dir.mkdir(parents=True, exist_ok=True)

    logger.add(
        log_dir / "orangecashmachine_{time:YYYY-MM-DD}.log",
        rotation=cfg["rotation"],
        retention=cfg["retention"],
        compression="gz",
        level="DEBUG",
        format=FILE,
        backtrace=True,
        diagnose=False,
    )

    logger.add(
        log_dir / "errors_{time:YYYY-MM-DD}.log",
        rotation=cfg["rotation"],
        retention="30 days",
        compression="gz",
        level="WARNING",
        format=FILE,
        backtrace=True,
        diagnose=False,
    )


def _add_pipeline_sink(cfg: Dict[str, Any]) -> None:
    log_dir: Path = cfg["log_dir"]
    log_dir.mkdir(parents=True, exist_ok=True)

    logger.add(
        log_dir / "pipeline_{time:YYYY-MM-DD}.log",
        rotation=cfg["rotation"],
        retention=cfg["retention"],
        compression="gz",
        level="DEBUG",
        format=PIPELINE,
        filter=pipeline_filter,
    )


# ---------------------------------------------------------------------
# Setup principal
# ---------------------------------------------------------------------
def setup_logging(
    cfg: Optional[LoggingConfig] = None,
    debug: bool = False,
    log_dir: Optional[Path] = None,
    run_id: Optional[str] = None,
) -> None:
    """
    Inicializa el sistema de logging.

    - Idempotente: segunda llamada instala bridge stdlib pero no resetea sinks
    - Si se llama dos veces con distintos parámetros (debug, cfg), solo
      la primera llamada configura sinks — las siguientes son no-op excepto
      por el bridge stdlib, que se reinstala siempre.
    - Tipado explícito: cfg es LoggingConfig, no object genérico
    - Preparado para producción
    """
    global _LOGGING_CONFIGURED

    resolved = _resolve_config(cfg, debug, log_dir)

    if not _LOGGING_CONFIGURED:
        logger.remove()
        logger.configure(patcher=_make_patcher(run_id))

        if resolved["console"]:
            _add_console_sink(resolved["level"], debug)

        if resolved["log_dir"] and resolved["file"]:
            _add_file_sinks(resolved)

        if resolved["log_dir"] and resolved["pipeline"]:
            _add_pipeline_sink(resolved)

        _LOGGING_CONFIGURED = True

        logger.bind(event="logging_configured").debug(
            "Logging system initialized | level={level} log_dir={log_dir}"
            " console={console} file={file} pipeline={pipeline}",
            level=resolved["level"],
            log_dir=resolved["log_dir"],
            console=resolved["console"],
            file=resolved["file"],
            pipeline=resolved["pipeline"],
        )

    # Bridge stdlib → loguru — siempre, aunque sinks ya existan
    std_logging.basicConfig(
        handlers=[InterceptHandler()],
        level=0,
        force=True,
    )
