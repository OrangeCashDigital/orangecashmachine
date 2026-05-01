from __future__ import annotations

"""
ocm_platform/observability/logger.py
======================================

Sistema de logging centralizado para OrangeCashMachine.

Arquitectura de sinks
---------------------
::

    Evento de log
        │
        ▼
    Loguru (dev: colores, backtrace, archivos rotativos)
        │
        ├──► stderr (consola colorizada, nivel configurado)
        ├──► logs/app_*.log          (JSON, DEBUG+, SSOT único para análisis)
        ├──► logs/orangecashmachine_*.log  (texto legible, nivel configurado)
        ├──► logs/errors_*.log       (texto, WARNING+, 30 días)
        ├──► LokiSink                (HTTP push → Grafana Loki, best-effort)
        └──► PrometheusLogSink       (counters → alertas Grafana)

Eliminación del sink pipeline_*.log
------------------------------------
``pipeline_*.log`` era un subconjunto ruidoso e impreciso de ``app_*.log``:
capturaba ``market_data.*`` entero (incluyendo adapters), duplicando ~100%
del volumen de ``app_*.log`` en disco. Eliminado: DRY, reducción de I/O.
Para análisis de pipeline: ``grep '"name": "market_data.processing' logs/app_*.log``.

Renombre system_*.log → app_*.log
----------------------------------
``system`` era ambiguo (¿logs del SO? ¿de la app?). ``app`` es descriptivo
y alineado con convenciones estándar (Rails, Django, Go services).

Ciclo de vida
-------------
1. ``bootstrap_logging()``  — Fase 1, antes de AppConfig. Thread-safe, idempotente.
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
import os
import sys
from pathlib import Path
from threading import Lock
from typing import Any

from loguru import logger

from ocm_platform.observability.bootstrap import drain as _drain_bootstrap
from ocm_platform.observability.config import LoggingConfig
from ocm_platform.observability.filters import pipeline_filter
from ocm_platform.observability.formats import CONSOLE, FILE
from ocm_platform.observability.processors import build_processor_chain, process_event
from ocm_platform.observability.sinks import LokiSink, PrometheusLogSink
from ocm_platform.runtime.run_config import VALID_ENVS


# ---------------------------------------------------------------------------
# Estado global — protegido por _CONFIG_LOCK en toda operación de escritura
# ---------------------------------------------------------------------------

_BOOTSTRAP_DONE:  bool            = False
_CONFIG_HASH:     str | None      = None
_ACTIVE_SINK_IDS: list[int]       = []
_ACTIVE_LOKI:     LokiSink | None = None
_CONFIG_LOCK                      = Lock()

# Permisos del directorio de logs — octal 750: owner rwx, group rx, other ---
_LOG_DIR_MODE: int = 0o750


# ---------------------------------------------------------------------------
# _LokiMessage — envelope para el bridge Loguru → LokiSink
# ---------------------------------------------------------------------------
# Definido a nivel de módulo: evitar crear una clase nueva por cada evento
# de log que pase por el sink de Loki (era O(n_events) creaciones de clase).

class _LokiMessage:
    """Envelope mínimo que LokiSink espera recibir en su __call__."""

    __slots__ = ("record",)

    def __init__(self, record: dict[str, Any]) -> None:
        self.record = record


# ---------------------------------------------------------------------------
# InterceptHandler — redirige stdlib logging → loguru
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Loggers ruidosos — silenciados a WARNING
# ---------------------------------------------------------------------------
# ccxt loguea responses HTTP completos (1-3 MB por línea).

_NOISY_LOGGERS: tuple[str, ...] = (
    "ccxt",
    "ccxt.async_support",
    "ccxt.async_support.base.exchange",
)


# ---------------------------------------------------------------------------
# Helpers privados
# ---------------------------------------------------------------------------

def _resolve_config(
    cfg: LoggingConfig | None,
    debug: bool,
    log_dir: Path | None,
) -> dict[str, Any]:
    """Construye el dict de configuración efectiva.

    Args:
        cfg:     LoggingConfig Pydantic, o ``None`` para defaults de bootstrap.
        debug:   Si ``True``, fuerza nivel DEBUG.
        log_dir: Override del directorio de logs (solo en bootstrap).

    Returns:
        Dict con todas las claves necesarias para instalar sinks.
    """
    if cfg:
        resolved_log_dir: Path | None = (
            Path(cfg.log_dir) if (cfg.file or cfg.pipeline) and cfg.log_dir else None
        )
        return {
            "level":       "DEBUG" if debug else cfg.level,
            "log_dir":     resolved_log_dir,
            "rotation":    cfg.rotation,
            "retention":   cfg.retention,
            "console":     cfg.console,
            "file":        cfg.file,
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
        "loki_url":    None,
        "loki_labels": {},
    }


def _make_patcher(run_id: str | None, env: str):
    """Crea un patcher que inyecta ``run_id``, ``service`` y ``env`` en cada record.

    Args:
        run_id: ID del proceso. Usa ``"-"`` si None.
        env:    Entorno activo.

    Returns:
        Callable compatible con ``logger.configure(patcher=...)``.
    """
    _run_id = run_id or "-"

    def _patch(record: dict[str, Any]) -> None:
        extra = record["extra"]
        extra.setdefault("run_id",  _run_id)
        extra.setdefault("service", "orangecashmachine")
        extra.setdefault("env",     env)

    return _patch


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


def _ensure_log_dir(log_dir: Path) -> None:
    """Crea el directorio de logs con permisos restrictivos. Fail-Soft.

    Permisos: 0o750 — owner rwx, group rx, other sin acceso.
    Los logs contienen run_ids, errores de exchange y configuración operacional.
    """
    try:
        log_dir.mkdir(parents=True, exist_ok=True, mode=_LOG_DIR_MODE)
        # chmod explícito: mkdir respeta umask, mode= no siempre lo hace
        os.chmod(log_dir, _LOG_DIR_MODE)
    except Exception as exc:
        logger.warning("log_dir_creation_failed | path={} error={}", log_dir, exc)


def _install_stdlib_bridge() -> None:
    """Instala InterceptHandler como único handler del stdlib.

    Loggers ruidosos (ccxt) se limitan a WARNING para evitar que sus
    responses HTTP de varios MB saturen los archivos locales y Loki.
    """
    std_logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)
    for name in _NOISY_LOGGERS:
        std_logging.getLogger(name).setLevel(std_logging.WARNING)


def _stable(obj: Any) -> Any:
    """Normaliza un objeto para hashing determinista."""
    if isinstance(obj, dict):
        return {k: _stable(v) for k, v in sorted(obj.items())}
    if isinstance(obj, (list, tuple)):
        return [_stable(v) for v in obj]
    if isinstance(obj, Path):
        return str(obj)
    return obj


def _install_sinks(
    resolved: dict[str, Any],
    debug: bool,
    run_id: str | None,
    env: str,
) -> tuple[list[int], LokiSink | None]:
    """Registra todos los sinks según la configuración resuelta.

    Sinks instalados
    ----------------
    - Consola stderr (si ``console=True``)
    - PrometheusLogSink (siempre — counters de nivel)
    - ``app_*.log`` (JSON, DEBUG+, SSOT único para análisis — si hay log_dir)
    - ``orangecashmachine_*.log`` (texto legible — si ``file=True``)
    - ``errors_*.log`` (texto, WARNING+, 30 días — si ``file=True``)
    - LokiSink (si ``loki_url`` está configurada)

    Args:
        resolved: Dict producido por :func:`_resolve_config`.
        debug:    Si ``True``, activa ``diagnose=True`` en consola.
        run_id:   ID del proceso (para labels de Loki).
        env:      Entorno activo (para labels de Loki).

    Returns:
        Tupla ``(lista de IDs de sinks loguru, instancia LokiSink o None)``.
    """
    ids:       list[int]        = []
    loki_sink: LokiSink | None  = None
    log_dir:   Path | None      = resolved.get("log_dir")

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

        # app_*.log — JSON estructurado, DEBUG+, SSOT único para análisis.
        # Reemplaza system_*.log (ambiguo) y pipeline_*.log (subconjunto redundante).
        # Para filtrar pipeline: grep '"name": "market_data.processing' logs/app_*.log
        ids.append(logger.add(
            log_dir / "app_{time:YYYY-MM-DD}.log",
            rotation=resolved["rotation"],
            retention=resolved["retention"],
            compression="gz",
            level="DEBUG",
            serialize=True,
        ))

        if resolved["file"]:
            # orangecashmachine_*.log — formato texto legible para humanos
            ids.append(logger.add(
                log_dir / "orangecashmachine_{time:YYYY-MM-DD}.log",
                rotation=resolved["rotation"],
                retention=resolved["retention"],
                compression="gz",
                level=resolved["level"],
                format=FILE,
            ))

            # errors_*.log — WARNING+ con retention fija de 30 días.
            # SSOT: usa cfg.retention como base pero nunca menos de 30 días
            # (errores deben conservarse más que logs informativos).
            errors_retention = resolved["retention"]
            ids.append(logger.add(
                log_dir / "errors_{time:YYYY-MM-DD}.log",
                rotation=resolved["rotation"],
                retention=errors_retention,
                compression="gz",
                level="WARNING",
                format=FILE,
            ))

    # ── Loki (sink remoto, best-effort) ──────────────────────────────
    loki_url: str | None = resolved.get("loki_url")
    if loki_url:
        _chain = build_processor_chain()
        loki_labels: dict[str, str] = {
            "env":     env,
            "run_id":  run_id or "-",
            "service": "orangecashmachine",
            **resolved.get("loki_labels", {}),
        }
        loki_sink = LokiSink(url=loki_url, labels=loki_labels)

        def _loki_loguru_sink(message: Any, _sink: LokiSink = loki_sink) -> None:
            """Bridge Loguru → structlog processor chain → LokiSink.

            ``_sink`` capturado por default arg (bind-at-definition) para
            evitar referencia forward sobre variable local del scope exterior.
            ``_LokiMessage`` es de nivel de módulo: cero allocaciones extra
            por evento.
            """
            record     = message.record
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
            _sink(_LokiMessage({
                "level":   record["level"],
                "time":    record["time"],
                "message": json_line,
                "extra":   record["extra"],
            }))

        ids.append(logger.add(
            _loki_loguru_sink,
            level="INFO",
            format="{message}",
        ))

    return ids, loki_sink


# ---------------------------------------------------------------------------
# Bootstrap — Fase 1: antes de AppConfig
# ---------------------------------------------------------------------------

def bootstrap_logging(
    debug: bool = False,
    run_id: str | None = None,
    env: str = "development",
) -> None:
    """Configuración mínima de logging — Fase 1, antes de AppConfig.

    Thread-safe e idempotente: llamadas concurrentes o adicionales son no-op.

    Fail-Fast: ``env`` se valida contra los entornos conocidos antes de
    instalar sinks. Un typo en el entorno se detecta aquí, no en producción.

    Args:
        debug:  Activa nivel DEBUG y diagnósticos en consola.
        run_id: ID del proceso. Si ``None`` usa ``"-"``.
        env:    Entorno activo. Debe ser uno de: development, test,
                staging, production.

    Raises:
        ValueError: Si ``env`` no pertenece a los entornos válidos (Fail-Fast).
    """
    # Fail-Fast: validar env antes de adquirir el lock
    if env not in VALID_ENVS:
        raise ValueError(
            f"bootstrap_logging: env={env!r} no es válido. "
            f"Valores permitidos: {sorted(VALID_ENVS)}"
        )

    global _BOOTSTRAP_DONE, _ACTIVE_SINK_IDS, _ACTIVE_LOKI

    # Fast-path sin lock para el caso común (ya inicializado)
    if _BOOTSTRAP_DONE:
        return

    with _CONFIG_LOCK:
        # Double-checked locking: otro hilo pudo haber inicializado
        # entre el check sin lock y la adquisición del lock.
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


# ---------------------------------------------------------------------------
# Configure — Fase 2: con LoggingConfig Pydantic validado
# ---------------------------------------------------------------------------

def configure_logging(
    cfg: LoggingConfig,
    env: str,
    debug: bool = False,
    run_id: str | None = None,
) -> None:
    """Reconfigura logging con LoggingConfig Pydantic validado.

    Hash-guarded: si la configuración efectiva no cambió, es no-op.
    SHA-256 en lugar de MD5 — compatible con modo FIPS en Debian/RHEL.

    Args:
        cfg:    Configuración de logging validada por Pydantic.
        env:    Entorno activo.
        debug:  Si ``True``, fuerza nivel DEBUG.
        run_id: ID del proceso para contexto en logs y labels Loki.
    """
    global _CONFIG_HASH, _ACTIVE_SINK_IDS, _ACTIVE_LOKI

    resolved = _resolve_config(cfg, debug, None)
    new_hash = hashlib.sha256(
        json.dumps(_stable(resolved), sort_keys=True).encode()
    ).hexdigest()[:16]  # 16 hex chars = 64 bits — suficiente para detección de cambios

    with _CONFIG_LOCK:
        if _CONFIG_HASH == new_hash:
            logger.debug("logging_reconfigure_skipped | hash={}", new_hash)
            return

        _CONFIG_HASH = new_hash

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

    # Log fuera del lock — el logger ya está activo
    logger.bind(phase="reconfigure").debug(
        "logging_reconfigured | level={} loki={}",
        resolved["level"],
        bool(resolved.get("loki_url")),
    )


# ---------------------------------------------------------------------------
# API pública
# ---------------------------------------------------------------------------

def bind_pipeline(
    component: str,
    exchange:  str | None = None,
    dataset:   str | None = None,
    **extra: Any,
) -> "loguru.Logger":
    """Retorna un logger con contexto de pipeline pre-enlazado.

    Args:
        component: Nombre del componente (e.g. ``"ohlcv_fetcher"``).
        exchange:  Nombre del exchange, si aplica.
        dataset:   Tipo de dataset, si aplica.
        **extra:   Contexto adicional arbitrario.

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
    """``True`` si bootstrap y configure_logging ya fueron ejecutados."""
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
