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

Ciclo de vida del logging:
  Fase 0  — bootstrap.pre_log()    → stderr + buffer (antes de cualquier sink)
  Fase 1  — bootstrap_logging()    → sinks con defaults + drene del buffer
  Fase 2  — configure_logging()    → remove() atomico + sinks desde YAML
"""

import logging as std_logging
import sys
from pathlib import Path
from typing import Optional, Dict, Any

from loguru import logger

from core.logging.bootstrap import drain as _drain_bootstrap
from core.logging.config import LoggingConfig
from core.logging.formats import CONSOLE, FILE, PIPELINE
from core.logging.filters import pipeline_filter

# ---------------------------------------------------------------------
# Estado global
# ---------------------------------------------------------------------
_BOOTSTRAP_DONE:    bool = False  # True tras bootstrap_logging()
_YAML_CONFIGURED:   bool = False  # True tras configure_logging()

from threading import Lock as _Lock
_CONFIG_LOCK = _Lock()  # protege configure_logging() en multi-thread


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
def _make_patcher(run_id: Optional[str], env: str = "development"):

    _run_id = run_id or "-"

    def _patch(record: dict) -> None:
        extra = record["extra"]
        extra.setdefault("run_id", _run_id)
        extra.setdefault("service", "orangecashmachine")
        extra.setdefault("env", env)

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
# Drene del buffer de Fase 0
# ---------------------------------------------------------------------
def _replay_bootstrap_buffer() -> None:
    """
    Drena los eventos pre-init al archivo ahora que los sinks existen.
    Cada evento se reproduce con su timestamp original para trazabilidad.

    Binding:
      stage="bootstrap"  → permite filtrar en Loki / Datadog / grep
      pre_init_ts        → timestamp real del evento antes del setup
    """
    entries = _drain_bootstrap()
    if not entries:
        return

    for entry in entries:
        ts    = entry.get("ts", "?")
        event = entry.get("event", "?")
        rest  = {k: v for k, v in entry.items() if k not in ("ts", "event")}
        parts = " | ".join(f"{k}={v}" for k, v in rest.items())
        msg   = f"{event}" + (f" | {parts}" if parts else "")
        logger.bind(stage="bootstrap", pre_init_ts=ts).debug(msg)

    logger.bind(stage="bootstrap").debug(
        "logging.bootstrap_drained | events={}", len(entries)
    )


# ---------------------------------------------------------------------
# Helper interno: instala bridge stdlib → loguru
# ---------------------------------------------------------------------
def _install_stdlib_bridge() -> None:
    std_logging.basicConfig(
        handlers=[InterceptHandler()],
        level=0,
        force=True,
    )


# ---------------------------------------------------------------------
# Helper interno: instala sinks según config resuelta
# Requiere logger.remove() previo para evitar duplicación.
# ---------------------------------------------------------------------
def _install_sinks(resolved: Dict[str, Any], debug: bool) -> None:
    if resolved["console"]:
        _add_console_sink(resolved["level"], debug)
    if resolved["log_dir"] and resolved["file"]:
        _add_file_sinks(resolved)
    if resolved["log_dir"] and resolved["pipeline"]:
        _add_pipeline_sink(resolved)


# ---------------------------------------------------------------------
# Fase 1 — bootstrap con defaults seguros
# ---------------------------------------------------------------------
def bootstrap_logging(
    debug: bool = False,
    run_id: Optional[str] = None,
    env: str = "development",
) -> None:
    """
    Fase 1: inicializa logging con defaults seguros antes de tener AppConfig.

    - Registra sinks (consola + archivo + pipeline) con valores hardcoded.
    - Drena el buffer de Fase 0 (eventos pre-init) hacia los sinks.
    - Instala bridge stdlib → loguru.

    Idempotente: segunda llamada es no-op.
    """
    global _BOOTSTRAP_DONE
    if _BOOTSTRAP_DONE:
        return

    resolved = _resolve_config(None, debug, None)
    logger.remove()
    logger.configure(patcher=_make_patcher(run_id, env))
    _install_sinks(resolved, debug)
    _BOOTSTRAP_DONE = True

    logger.bind(event="logging_configured").debug(
        "Logging system initialized | level={level} log_dir={log_dir}"
        " console={console} file={file} pipeline={pipeline}",
        level=resolved["level"],
        log_dir=resolved["log_dir"],
        console=resolved["console"],
        file=resolved["file"],
        pipeline=resolved["pipeline"],
    )

    _replay_bootstrap_buffer()
    _install_stdlib_bridge()


# ---------------------------------------------------------------------
# Fase 2 — reconfiguración desde AppConfig YAML
# ---------------------------------------------------------------------
def configure_logging(
    cfg: LoggingConfig,
    env: str,
    debug: bool = False,
    run_id: Optional[str] = None,
) -> None:
    """
    Fase 2: reemplaza sinks de Fase 1 con configuración real del YAML.

    - Reconfigura patcher con env real.
    - Añade nuevos sinks ANTES de eliminar los antiguos (ventana cero sin sinks).
    - Reinstala bridge stdlib → loguru.
    - No toca el buffer de Fase 0 (ya drenado en Fase 1).

    env es obligatorio — sin default para forzar consistencia.
    Debe llamarse después de bootstrap_logging() y load_config().
    """
    with _CONFIG_LOCK:
        global _YAML_CONFIGURED
        if _YAML_CONFIGURED:
            logger.debug("configure_logging called again — idempotent, skipping")
            return
        _YAML_CONFIGURED = True

    resolved = _resolve_config(cfg, debug, None)

    # Capturar IDs de sinks existentes — acceso defensivo a API privada de loguru
    _core = getattr(logger, "_core", None)
    _handlers = getattr(_core, "handlers", {})
    old_ids = list(_handlers.keys()) if isinstance(_handlers, dict) else []

    # Reconfigurar patcher con env real
    logger.configure(patcher=_make_patcher(run_id, env))

    # Añadir nuevos sinks primero — ventana sin sinks = cero
    _install_sinks(resolved, debug)

    # Bridge stdlib antes de eliminar antiguos — sin gaps para logs de stdlib
    _install_stdlib_bridge()

    # Eliminar sinks antiguos DESPUÉS de instalar nuevos y bridge
    for h_id in old_ids:
        try:
            logger.remove(h_id)
        except Exception:
            pass

    logger.bind(event="logging_reconfigured").debug(
        "Logging reconfigured from YAML | level={level} log_dir={log_dir}"
        " console={console} file={file} pipeline={pipeline}",
        level=resolved["level"],
        log_dir=resolved["log_dir"],
        console=resolved["console"],
        file=resolved["file"],
        pipeline=resolved["pipeline"],
    )


# ---------------------------------------------------------------------
# Helpers de introspección
# ---------------------------------------------------------------------
def is_logging_configured() -> bool:
    """Retorna True si bootstrap_logging() Y configure_logging() han sido llamados."""
    return _BOOTSTRAP_DONE and _YAML_CONFIGURED


# ---------------------------------------------------------------------
# Alias de compatibilidad — deprecado, usar bootstrap_logging()
# ---------------------------------------------------------------------
def setup_logging(
    cfg: Optional[LoggingConfig] = None,
    debug: bool = False,
    log_dir: Optional[Path] = None,
    run_id: Optional[str] = None,
) -> None:
    """Deprecado. Usar bootstrap_logging() o configure_logging() directamente."""
    import warnings
    warnings.warn(
        "setup_logging() is deprecated. Use bootstrap_logging() or configure_logging().",
        DeprecationWarning,
        stacklevel=2,
    )
    if cfg is None:
        bootstrap_logging(debug=debug, run_id=run_id)
    else:
        raise RuntimeError(
            "setup_logging() is deprecated and cannot proxy configure_logging() safely "
            "because env is unknown. Use configure_logging(cfg=..., env=run_cfg.env) directly."
        )
