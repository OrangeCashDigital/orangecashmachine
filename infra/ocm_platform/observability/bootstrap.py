from __future__ import annotations

"""
ocm_platform/observability/bootstrap.py
=========================================

Buffer de pre-logging de Fase 0.

Captura eventos ANTES de que ``bootstrap_logging()`` registre sinks en loguru.
Sin dependencias de loguru — importable desde cualquier módulo de arranque
sin riesgo de import circular.

Thread-safety
-------------
``_buffer`` y ``_active`` son estado global mutable. ``_lock`` protege
todas las operaciones de escritura — ``pre_log()`` y ``drain()`` pueden
llamarse desde hilos concurrentes durante el bootstrap del proceso.

Ciclo de vida::

    1. pre_log()  → escribe a stderr + acumula en buffer  (thread-safe)
    2. drain()    → llamado UNA VEZ por bootstrap_logging() al inicializar sinks
    3. Después de drain(): buffer vacío, _active=False, pre_log() es no-op

Convención de naming de eventos::

    "<subsistema>.<accion>"  →  config.dotenv_bootstrap
                                config.env_resolved
                                logging.setup_started
"""

import datetime
import sys
import threading
from typing import Any


_lock:   threading.Lock         = threading.Lock()
_buffer: list[dict[str, Any]]  = []
_active: bool                  = True


def pre_log(event: str, **kwargs: Any) -> None:
    """Registra un evento en la fase de bootstrap (antes de loguru).

    Thread-safe. No-op después de ``drain()``.

    Args:
        event:    Nombre del evento con namespace, e.g. ``"config.env_resolved"``.
        **kwargs: Pares clave=valor para contexto estructurado.
    """
    with _lock:
        if not _active:
            return
        ts    = datetime.datetime.now(datetime.timezone.utc).isoformat(
            sep=" ", timespec="milliseconds"
        )
        parts = " | ".join(f"{k}={v}" for k, v in kwargs.items())
        line  = f"{ts} | PRE-INIT | {event}" + (f" | {parts}" if parts else "")
        print(line, file=sys.stderr, flush=True)
        _buffer.append({"ts": ts, "event": event, **kwargs})


def drain() -> list[dict[str, Any]]:
    """Retorna el buffer acumulado y desactiva el modo bootstrap.

    Thread-safe. Debe llamarse UNA SOLA VEZ desde ``bootstrap_logging()``.

    Returns:
        Lista de entradas acumuladas desde el inicio del proceso.
        Vacía si ya fue llamada anteriormente.
    """
    global _active
    with _lock:
        _active = False
        result  = list(_buffer)
        _buffer.clear()
    return result
