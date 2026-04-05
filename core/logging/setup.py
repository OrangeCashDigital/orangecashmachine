from __future__ import annotations

"""
core/logging/setup.py
=====================

Sistema de logging centralizado para OrangeCashMachine v0.3.0.

Arquitectura
------------
::

    Evento de log
        │
        ▼
    Loguru (dev: colores, backtrace, archivos rotativos)
        │
        ├──► stderr (consola colorizada)
        ├──► logs/system_*.log       (JSON, todos los niveles)
        ├──► logs/ocm_*.log          (formato legible, nivel configurado)
        ├──► logs/errors_*.log       (WARNING+, 30 días)
        ├──► logs/pipeline_*.log     (JSON, filtro por módulo)
        ├──► LokiSink                (HTTP push → Grafana Loki)
        └──► PrometheusLogSink       (counters → Grafana dashboards)
                │
                ▼
            structlog processor chain
            (timestamp · level · service · sanitize · JSON)

Ciclo de vida
-------------
1. ``bootstrap_logging()``  — Fase 1, antes de AppConfig. Idempotente.
2. ``configure_logging()``  — Fase 2, con LoggingConfig validado. Hash-guarded.

Exports públicos
----------------
- bootstrap_logging
- configure_logging
- bind_pipeline
- is_logging_configured
- InterceptHandler
"""

import hashlib
import json
import logging as std_logging
import sys
from pathlib import Path
from threading import Lock
from typing import Any, Optional

from loguru import logger

from core.logging.bootstrap import drain as _drain_bootstrap
from core.logging.config import LoggingConfig
from core.logging.filters import pipeline_filter
from core.logging.formats import CONSOLE, FILE
from core.logging.processors import build_processor_chain, process_event
from core.logging.sinks import LokiSink, PrometheusLogSink


# ── Estado global ─────────────────────────────────────────────────────────────

_BOOTSTRAP_DONE:  bool          = False
_CONFIG_HASH:     Optional[str] = None
_ACTIVE_SINK_IDS: list[int]     = []
_ACTIVE_LOKI:     Optional[LokiSink] = None
_CONFIG_LOCK = Lock()


# ── InterceptHandler (stdlib → loguru) ───────────────────────────────────────

class InterceptHandler(std_logging.Handler):
    """Redirige logs del stdlib a loguru.

    Captura librerías de terceros (SQLAlchemy, httpx, prefect, uvicorn)
    y los unifica en el pipeline de loguru.
    """

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


# ── Config resolver ───────────────────────────────────────────────────────────

def _resolve_config(
    cfg: Optional[LoggingConfig],
    debug: bool,
    log_dir: Optional[Path],
) -> dict[str, Any]:
    """Construye el dict de configuración efectiva.

    Args:
        cfg:     LoggingConfig Pydantic, o None para defaults de bootstrap.
        debug:   Si True, fuerza nivel DEBUG.
        log_dir: Override del directorio de logs (solo en bootstrap).

    Returns:
        Dict con todas las claves necesarias para instalar sinks.
    """
    if cfg:
        resolved_log_dir: Optional[Path] = (
            Path(cfg.log_dir) if (cfg.file or cfg.pipeline) and cfg.log_dir else None
        )
        return {
            "level":       "DEBUG" if debug else cfg.level,
            "log_dir":     resolved_log_dir,
            "rotation":    cfg.rotation,
            "retention":   cfg.retention,
            "console":     cfg.console,
            "file":        cfg.file,
            "pipeline":    cfg.pipeline,
            "loki_url":    cfg.loki_url,
            "loki_labels": cfg.loki_labels,
        }

    return {
        "level":       "DEBUG" if debug else "INFO",
        "log_dir":     log_dir or Path("logs"),
        "rotation":    "1 day",
        "retention":   "14 days",
        "console":     True,
        "file":        True,
        "pipeline":    True,
        "loki_url":    None,
        "loki_labels": {},
    }


# ── Context patcher ───────────────────────────────────────────────────────────

def _make_patcher(run_id: Optional[str], env: str):
    """Crea un patcher que inyecta run_id, service y env en cada record.

    Args:
        run_id: ID del proceso. Usa ``"-"`` si None.
        env:    Entorno activo.

    Returns:
        Callable compatible con ``logger.configure(patcher=...)``.
    """
    _run_id = run_id or "-"

    def _patch(record: dict[str, Any]) -> None:
        extra = record["extra"]
        extra.setdefault("run_id",   _run_id)
        extra.setdefault("service",  "orangecashmachine")
        extra.setdefault("env",      env)

    return _patch


# ── Bootstrap buffer replay ───────────────────────────────────────────────────

def _replay_bootstrap_buffer() -> None:
    """Re-emite al pipeline de loguru los eventos capturados en Fase 0."""
    entries = _drain_bootstrap()
    if not entries:
        return

    for entry in entries:
        ts    = entry.get("ts", "?")
        event = entry.get("event", "?")
        rest  = {k: v for k, v in entry.items() if k not in ("ts", "event")}
        logger.bind(phase="pre_init", pre_init_ts=ts, **rest).debug(event)

    logger.bind(phase="pre_init").debug(
        "logging.bootstrap_drained | events={}", len(entries)
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ensure_log_dir(log_dir: Path) -> None:
    """Crea el directorio de logs si no existe. Fail-soft."""
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        logger.warning("log_dir_creation_failed | error={}", exc)


def _install_stdlib_bridge() -> None:
    """Instala InterceptHandler como único handler del stdlib."""
    std_logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)


def _stable(obj: Any) -> Any:
    """Normaliza un objeto para hashing determinista."""
    if isinstance(obj, dict):
        return {k: _stable(v) for k, v in sorted(obj.items())}
    if isinstance(obj, (list, tuple)):
        return [_stable(v) for v in obj]
    if isinstance(obj, Path):
        return str(obj)
    return obj


# ── Sink installer ────────────────────────────────────────────────────────────

def _install_sinks(
    resolved: dict[str, Any],
    debug: bool,
    run_id: Optional[str],
    env: str,
) -> tuple[list[int], Optional[LokiSink]]:
    """Registra todos los sinks según la configuración resuelta.

    Args:
        resolved: Dict producido por :func:`_resolve_config`.
        debug:    Si True, activa ``diagnose=True`` en consola.
        run_id:   ID del proceso (para labels de Loki).
        env:      Entorno activo (para labels de Loki).

    Returns:
        Tupla (lista de IDs de sinks loguru, instancia LokiSink o None).
    """
    ids:       list[int]          = []
    loki_sink: Optional[LokiSink] = None
    log_dir:   Optional[Path]     = resolved.get("log_dir")

    # ── Consola ───────────────────────────────────────────────────────
    if resolved["console"]:
        ids.append(logger.add(
            sys.stderr,
            level=resolved["level"],
            format=CONSOLE,
            backtrace=True,
            diagnose=debug,
            colorize=True,
        ))

    # ── Prometheus counter ────────────────────────────────────────────
    ids.append(logger.add(
        PrometheusLogSink(),
        level="DEBUG",
        format="{message}",
    ))

    # ── Archivos locales ──────────────────────────────────────────────
    if log_dir:
        _ensure_log_dir(log_dir)

        # system.log — todos los niveles, JSON
        ids.append(logger.add(
            log_dir / "system_{time:YYYY-MM-DD}.log",
            rotation=resolved["rotation"],
            retention=resolved["retention"],
            compression="gz",
            level="DEBUG",
            serialize=True,
        ))

        if resolved["file"]:
            # ocm.log — nivel configurado, formato legible
            ids.append(logger.add(
                log_dir / "orangecashmachine_{time:YYYY-MM-DD}.log",
                rotation=resolved["rotation"],
                retention=resolved["retention"],
                compression="gz",
                level=resolved["level"],
                format=FILE,
            ))
            # errors.log — WARNING+, retención extendida
            ids.append(logger.add(
                log_dir / "errors_{time:YYYY-MM-DD}.log",
                rotation=resolved["rotation"],
                retention="30 days",
                compression="gz",
                level="WARNING",
                format=FILE,
            ))

        if resolved["pipeline"]:
            # pipeline.log — JSON, solo módulos de pipeline
            ids.append(logger.add(
                log_dir / "pipeline_{time:YYYY-MM-DD}.log",
                rotation=resolved["rotation"],
                retention=resolved["retention"],
                compression="gz",
                level="DEBUG",
                serialize=True,
                filter=pipeline_filter,
            ))

    # ── Loki (sink remoto) ────────────────────────────────────────────
    loki_url: Optional[str] = resolved.get("loki_url")
    if loki_url:
        _chain = build_processor_chain()

        def _loki_loguru_sink(message: Any) -> None:
            """Bridge Loguru → structlog processor chain → LokiSink."""
            record = message.record
            event_dict: dict[str, Any] = {
                "event":   record["message"],
                "level":   record["level"].name.lower(),
                "module":  record["name"],
                "run_id":  record["extra"].get("run_id",  "-"),
                "env":     record["extra"].get("env",     env),
                "service": record["extra"].get("service", "orangecashmachine"),
                **{
                    k: v for k, v in record["extra"].items()
                    if k not in ("run_id", "env", "service")
                },
            }
            json_line = process_event(_chain, event_dict["level"], event_dict)

            # LokiSink espera un objeto con .record — adaptamos
            class _Msg:
                pass

            msg        = _Msg()
            msg.record = {         # type: ignore[attr-defined]
                "level":   record["level"],
                "time":    record["time"],
                "message": json_line,
                "extra":   record["extra"],
            }
            loki_sink(msg)  # type: ignore[name-defined]

        loki_labels: dict[str, str] = {
            "env":     env,
            "run_id":  run_id or "-",
            "service": "orangecashmachine",
            **resolved.get("loki_labels", {}),
        }
        loki_sink = LokiSink(url=loki_url, labels=loki_labels)
        ids.append(logger.add(
            _loki_loguru_sink,
            level="INFO",
            format="{message}",
        ))

    return ids, loki_sink


# ── Bootstrap (Fase 1) ────────────────────────────────────────────────────────

def bootstrap_logging(
    debug: bool = False,
    run_id: Optional[str] = None,
    env: str = "development",
) -> None:
    """Configuración mínima de logging — Fase 1, antes de AppConfig.

    Idempotente: llamadas adicionales son no-op.

    Args:
        debug:  Activa nivel DEBUG y diagnósticos en consola.
        run_id: ID del proceso. Si None usa ``"-"``.
        env:    Entorno activo para contexto en logs.
    """
    global _BOOTSTRAP_DONE, _ACTIVE_SINK_IDS, _ACTIVE_LOKI

    if _BOOTSTRAP_DONE:
        return

    resolved = _resolve_config(None, debug, None)

    logger.remove()
    logger.configure(patcher=_make_patcher(run_id, env))
    _ACTIVE_SINK_IDS, _ACTIVE_LOKI = _install_sinks(resolved, debug, run_id, env)
    _BOOTSTRAP_DONE = True

    logger.bind(phase="init").debug(
        "logging_initialized | level={}", resolved["level"]
    )
    _replay_bootstrap_buffer()
    _install_stdlib_bridge()


# ── Configure (Fase 2) ────────────────────────────────────────────────────────

def configure_logging(
    cfg: LoggingConfig,
    env: str,
    debug: bool = False,
    run_id: Optional[str] = None,
) -> None:
    """Reconfigura logging con LoggingConfig Pydantic validado.

    Hash-guarded: si la configuración efectiva no cambió, es no-op.

    Args:
        cfg:    Configuración de logging validada por Pydantic.
        env:    Entorno activo.
        debug:  Si True, fuerza nivel DEBUG.
        run_id: ID del proceso para contexto en logs y labels Loki.
    """
    global _CONFIG_HASH, _ACTIVE_SINK_IDS, _ACTIVE_LOKI

    resolved = _resolve_config(cfg, debug, None)
    new_hash = hashlib.md5(
        json.dumps(_stable(resolved), sort_keys=True).encode()
    ).hexdigest()

    with _CONFIG_LOCK:
        if _CONFIG_HASH == new_hash:
            logger.debug("logging_reconfigure_skipped | hash={}", new_hash[:8])
            return

        _CONFIG_HASH = new_hash

        # Cerrar LokiSink anterior para liberar conexión HTTP
        if _ACTIVE_LOKI is not None:
            try:
                _ACTIVE_LOKI.close()
            except Exception:
                pass
            _ACTIVE_LOKI = None

        logger.remove()
        _ACTIVE_SINK_IDS.clear()
        logger.configure(patcher=_make_patcher(run_id, env))
        _ACTIVE_SINK_IDS, _ACTIVE_LOKI = _install_sinks(resolved, debug, run_id, env)
        _install_stdlib_bridge()

    logger.bind(phase="reconfigure").debug(
        "logging_reconfigured | level={} loki={}",
        resolved["level"],
        bool(resolved.get("loki_url")),
    )


# ── Helpers públicos ──────────────────────────────────────────────────────────

def bind_pipeline(
    component: str,
    exchange:  Optional[str] = None,
    dataset:   Optional[str] = None,
    **extra: Any,
):
    """Retorna un logger con contexto de pipeline pre-enlazado.

    Args:
        component: Nombre del componente (e.g. ``"ohlcv_fetcher"``).
        exchange:  Nombre del exchange, si aplica.
        dataset:   Tipo de dataset, si aplica.
        **extra:   Contexto adicional.

    Returns:
        Logger de loguru con contexto fijo.
    """
    ctx: dict[str, Any] = {"component": component, **extra}
    if exchange is not None:
        ctx["exchange"] = exchange
    if dataset is not None:
        ctx["dataset"] = dataset
    return logger.bind(**ctx)


def is_logging_configured() -> bool:
    """True si bootstrap y configure_logging ya fueron ejecutados."""
    return _BOOTSTRAP_DONE and _CONFIG_HASH is not None


def setup_logging(*args: Any, **kwargs: Any) -> None:
    """Eliminada en v0.2.0. Usar bootstrap_logging o configure_logging.

    Raises:
        RuntimeError: Siempre.
    """
    raise RuntimeError(
        "setup_logging() fue eliminada en v0.2.0. "
        "Usa bootstrap_logging() o configure_logging()."
    )
