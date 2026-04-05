from __future__ import annotations

"""
core/logging/bootstrap.py
=========================

Buffer de pre-logging de Fase 0.

Captura eventos ANTES de que ``setup_logging()`` registre sinks en loguru.
Sin dependencias de loguru — importable desde cualquier módulo de arranque
sin riesgo de import circular.

Ciclo de vida::

    1. pre_log()  → escribe a stderr + acumula en buffer
    2. drain()    → llamado UNA VEZ por setup_logging() al inicializar sinks
    3. Después de drain(): buffer vacío, _active=False, pre_log() es no-op

Convención de naming::

    "<subsistema>.<accion>"  →  config.dotenv_bootstrap
                                config.env_resolved
                                logging.setup_started
"""

import datetime
import sys
from typing import Any


_buffer: list[dict[str, Any]] = []
_active: bool = True


def pre_log(event: str, **kwargs: Any) -> None:
    """Registra un evento en la fase de bootstrap (antes de loguru).

    Args:
        event: Nombre del evento con namespace, e.g. ``"config.env_resolved"``.
        **kwargs: Pares clave=valor para contexto estructurado.
    """
    if not _active:
        return

    ts = datetime.datetime.now().isoformat(sep=" ", timespec="milliseconds")
    parts = " | ".join(f"{k}={v}" for k, v in kwargs.items())
    line = f"{ts} | PRE-INIT | {event}" + (f" | {parts}" if parts else "")
    print(line, file=sys.stderr, flush=True)
    _buffer.append({"ts": ts, "event": event, **kwargs})


def drain() -> list[dict[str, Any]]:
    """Retorna el buffer acumulado y desactiva el modo bootstrap.

    Debe llamarse UNA SOLA VEZ desde ``setup_logging()``.

    Returns:
        Lista de entradas acumuladas desde el inicio del proceso.
    """
    global _active
    _active = False
    result = list(_buffer)
    _buffer.clear()
    return result
