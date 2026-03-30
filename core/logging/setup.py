from __future__ import annotations

"""
core/logging/setup.py
=====================

Sistema de logging centralizado basado en Loguru.
"""

import hashlib
import json
import logging as std_logging
import sys
from pathlib import Path
from typing import Optional, Dict, Any, List
from threading import Lock

from loguru import logger

from core.logging.bootstrap import drain as _drain_bootstrap
from core.logging.config import LoggingConfig
from core.logging.formats import CONSOLE, FILE
from core.logging.filters import pipeline_filter


# ---------------------------------------------------------------------
# Estado global
# ---------------------------------------------------------------------
_BOOTSTRAP_DONE: bool = False
_CONFIG_HASH: Optional[str] = None
_ACTIVE_SINK_IDS: List[int] = []
_CONFIG_LOCK = Lock()


# ---------------------------------------------------------------------
# Bridge stdlib → loguru
# ---------------------------------------------------------------------
class InterceptHandler(std_logging.Handler):
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
# Config resolver
# ---------------------------------------------------------------------
def _resolve_config(
    cfg: Optional[LoggingConfig],
    debug: bool,
    log_dir: Optional[Path],
) -> Dict[str, Any]:

    if cfg:
        resolved_log_dir = (
            Path(cfg.log_dir)
            if (cfg.file or cfg.pipeline) and cfg.log_dir
            else None
        )

        return {
            "level": "DEBUG" if debug else cfg.level.upper(),
            "log_dir": resolved_log_dir,
            "rotation": cfg.rotation,
            "retention": cfg.retention,
            "console": cfg.console,
            "file": cfg.file,
            "pipeline": cfg.pipeline,
        }

    return {
        "level": "DEBUG" if debug else "INFO",
        "log_dir": log_dir or Path("logs"),
        "rotation": "1 day",
        "retention": "14 days",
        "console": True,
        "file": True,
        "pipeline": True,
    }


# ---------------------------------------------------------------------
# Context injection
# ---------------------------------------------------------------------
def _make_patcher(run_id: Optional[str], env: str):
    _run_id = run_id or "-"

    def _patch(record: dict) -> None:
        extra = record["extra"]
        extra.setdefault("run_id", _run_id)
        extra.setdefault("service", "orangecashmachine")
        extra.setdefault("env", env)

    return _patch


# ---------------------------------------------------------------------
# Bootstrap replay
# ---------------------------------------------------------------------
def _replay_bootstrap_buffer() -> None:
    entries = _drain_bootstrap()
    if not entries:
        return

    for entry in entries:
        ts = entry.get("ts", "?")
        event = entry.get("event", "?")
        rest = {k: v for k, v in entry.items() if k not in ("ts", "event")}

        logger.bind(
            phase="pre_init",
            pre_init_ts=ts,
            **rest
        ).debug(event)

    logger.bind(phase="pre_init").debug(
        "logging.bootstrap_drained",
        events=len(entries),
    )


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _ensure_log_dir(log_dir: Path) -> None:
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        logger.warning("log_dir_creation_failed", error=str(exc))


def _install_stdlib_bridge() -> None:
    std_logging.basicConfig(
        handlers=[InterceptHandler()],
        level=0,
        force=True,
    )


# ---------------------------------------------------------------------
# Sinks
# ---------------------------------------------------------------------
def _install_sinks(resolved: Dict[str, Any], debug: bool) -> List[int]:
    ids: List[int] = []
    log_dir: Optional[Path] = resolved.get("log_dir")

    if resolved["console"]:
        ids.append(logger.add(
            sys.stderr,
            level=resolved["level"],
            format=CONSOLE,
            backtrace=True,
            diagnose=debug,
            colorize=True,
        ))

    if not log_dir:
        return ids

    _ensure_log_dir(log_dir)

    ids.append(logger.add(
        log_dir / "system_{time:YYYY-MM-DD}.log",
        rotation=resolved["rotation"],
        retention=resolved["retention"],
        compression="gz",
        level="DEBUG",
        serialize=True,
    ))

    if resolved["file"]:
        ids.append(logger.add(
            log_dir / "orangecashmachine_{time:YYYY-MM-DD}.log",
            rotation=resolved["rotation"],
            retention=resolved["retention"],
            compression="gz",
            level=resolved["level"],
            format=FILE,
        ))

        ids.append(logger.add(
            log_dir / "errors_{time:YYYY-MM-DD}.log",
            rotation=resolved["rotation"],
            retention="30 days",
            compression="gz",
            level="WARNING",
            format=FILE,
        ))

    if resolved["pipeline"]:
        ids.append(logger.add(
            log_dir / "pipeline_{time:YYYY-MM-DD}.log",
            rotation=resolved["rotation"],
            retention=resolved["retention"],
            compression="gz",
            level="DEBUG",
            serialize=True,
            filter=pipeline_filter,
        ))

    return ids


# ---------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------
def bootstrap_logging(
    debug: bool = False,
    run_id: Optional[str] = None,
    env: str = "development",
) -> None:
    global _BOOTSTRAP_DONE, _ACTIVE_SINK_IDS

    if _BOOTSTRAP_DONE:
        return

    resolved = _resolve_config(None, debug, None)

    logger.remove()
    logger.configure(patcher=_make_patcher(run_id, env))

    _ACTIVE_SINK_IDS = _install_sinks(resolved, debug)

    _BOOTSTRAP_DONE = True

    logger.bind(phase="init").debug(
        "logging_initialized",
        level=resolved["level"],
    )

    _replay_bootstrap_buffer()
    _install_stdlib_bridge()


# ---------------------------------------------------------------------
# Reconfiguración
# ---------------------------------------------------------------------
def configure_logging(
    cfg: LoggingConfig,
    env: str,
    debug: bool = False,
    run_id: Optional[str] = None,
) -> None:
    global _CONFIG_HASH, _ACTIVE_SINK_IDS

    resolved = _resolve_config(cfg, debug, None)

    def _stable(obj):
        if isinstance(obj, dict):
            return {k: _stable(v) for k, v in sorted(obj.items())}
        if isinstance(obj, (list, tuple)):
            return [_stable(v) for v in obj]
        if isinstance(obj, Path):
            return str(obj)
        return obj

    new_hash = hashlib.md5(
        json.dumps(_stable(resolved), sort_keys=True).encode()
    ).hexdigest()

    with _CONFIG_LOCK:
        if _CONFIG_HASH == new_hash:
            logger.debug("logging_reconfigure_skipped", hash=new_hash[:8])
            return

        _CONFIG_HASH = new_hash

        # 🔥 Limpieza TOTAL antes de reconfigurar
        logger.remove()
        _ACTIVE_SINK_IDS.clear()

        logger.configure(patcher=_make_patcher(run_id, env))

        _ACTIVE_SINK_IDS = _install_sinks(resolved, debug)

        _install_stdlib_bridge()

    logger.bind(phase="reconfigure").debug(
        "logging_reconfigured",
        level=resolved["level"],
    )


# ---------------------------------------------------------------------
# Helpers públicos
# ---------------------------------------------------------------------
def bind_pipeline(
    component: str,
    exchange: Optional[str] = None,
    dataset: Optional[str] = None,
    **extra: Any,
):
    ctx: Dict[str, Any] = {"component": component, **extra}

    if exchange is not None:
        ctx["exchange"] = exchange

    if dataset is not None:
        ctx["dataset"] = dataset

    return logger.bind(**ctx)


def is_logging_configured() -> bool:
    return _BOOTSTRAP_DONE and _CONFIG_HASH is not None


def setup_logging(*args, **kwargs):
    raise RuntimeError(
        "setup_logging() is deprecated. Use bootstrap_logging() or configure_logging()."
    )