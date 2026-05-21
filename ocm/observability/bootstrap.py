from __future__ import annotations

"""
ocm/observability/bootstrap.py
=========================================

Buffer de pre-logging de Fase 0.

Captura eventos ANTES de que ``bootstrap_logging()`` registre sinks en loguru.
Sin dependencias de loguru ni de ningún módulo OCM — importable desde cualquier
módulo de arranque sin riesgo de import circular.

Restricción de bounded context
-------------------------------
Este módulo NO debe ser importado por loguru ni por el subsistema de logging
de OCM (``ocm/observability/logging/``).  La dependencia es unidireccional:
los módulos de arranque (config, env_resolver) → bootstrap.py → nadie de OCM.
Violar esta restricción introduce ciclos de importación en el bootstrap.

Thread-safety
-------------
``_buffer`` y ``_active`` son estado global mutable. ``_lock`` protege
todas las operaciones de escritura — ``pre_log()`` y ``drain()`` pueden
llamarse desde hilos concurrentes durante el bootstrap del proceso.

Ciclo de vida::

    1. pre_log()  → escribe a stderr + acumula en buffer  (thread-safe)
    2. drain()    → llamado UNA VEZ por bootstrap_logging() al inicializar sinks
    3. Después de drain(): buffer vacío, _active=False, pre_log() es no-op

Nota: drain() NO es idempotente en contenido. Una segunda llamada devuelve
``[]`` porque el buffer ya fue limpiado en la primera. Esta es la semántica
correcta: solo bootstrap_logging() debe llamar drain(), y solo una vez.

Convención de naming de eventos::

    "<subsistema>.<accion>"  →  config.dotenv_bootstrap
                                config.env_resolved
                                logging.setup_started

Principios: Thread-Safety · Fail-Soft · KISS · Bounded Context (DDD).
"""

import sys
import threading
from datetime import datetime, timezone
from typing import Any, Final

__all__ = ["pre_log", "drain"]

# ---------------------------------------------------------------------------
# Estado global del buffer de pre-logging.
# Final[X] documenta la intención: la referencia al objeto no debe reasignarse.
# El estado interno de cada objeto sí muta (append, clear, bool assignment).
# ---------------------------------------------------------------------------
_lock: Final[threading.Lock] = threading.Lock()
_buffer: Final[list[dict[str, Any]]] = []
_active: list[bool] = [True]  # mutable container para evitar 'global'


def pre_log(event: str, **kwargs: str | int | float | bool | None) -> None:
    """Registra un evento en la fase de bootstrap (antes de loguru).

    Thread-safe. No-op después de ``drain()``.

    Los valores de ``kwargs`` deben ser tipos primitivos serializables
    (str, int, float, bool, None) — se almacenan en el buffer para
    replay post-drain via ``logger.bind(**entry)``.  Pasar objetos
    complejos no está soportado y se rechaza en tiempo de ejecución
    con un warning a stderr (fail-soft: no interrumpe el arranque).

    Args:
        event:    Nombre del evento con namespace, e.g. ``"config.env_resolved"``.
        **kwargs: Pares clave=valor de contexto estructurado (primitivos).
    """
    # Validación fail-soft de tipos no-serializables — no interrumpe arranque.
    _PRIMITIVE = (str, int, float, bool, type(None))
    bad = {k: type(v).__name__ for k, v in kwargs.items() if not isinstance(v, _PRIMITIVE)}
    if bad:
        print(
            f"PRE-LOG WARNING | event={event} | non-primitive kwargs will be str()-coerced: {bad}",
            file=sys.stderr,
            flush=True,
        )
        kwargs = {
            k: (str(v) if not isinstance(v, _PRIMITIVE) else v) for k, v in kwargs.items()
        }  # narrowed: all values are _PRIMITIVE after coercion

    with _lock:
        if not _active[0]:
            return
        ts = datetime.now(timezone.utc).isoformat(sep=" ", timespec="milliseconds")
        parts = " | ".join(f"{k}={v}" for k, v in kwargs.items())
        line = f"{ts} | PRE-INIT | {event}" + (f" | {parts}" if parts else "")
        print(line, file=sys.stderr, flush=True)
        _buffer.append({"ts": ts, "event": event, **kwargs})


def drain() -> list[dict[str, Any]]:
    """Retorna el buffer acumulado y desactiva el modo bootstrap.

    Thread-safe. Debe llamarse UNA SOLA VEZ desde ``bootstrap_logging()``.

    Semántica: NO es idempotente en contenido. Una segunda llamada devuelve
    ``[]`` porque el buffer ya fue limpiado en la primera invocación.
    Llamadas concurrentes están serializadas por ``_lock`` — la segunda
    siempre verá ``_active[0] == False`` y retornará ``[]`` inmediatamente.

    Returns:
        Lista de entradas acumuladas desde el inicio del proceso.
        Vacía si ya fue llamada anteriormente.
    """
    with _lock:
        if not _active[0]:
            return []
        _active[0] = False
        result = list(_buffer)
        _buffer.clear()
    return result


# ---------------------------------------------------------------------------
# TEST UTILITIES — NUNCA llamar en código de producción.
# ---------------------------------------------------------------------------
def _reset_for_testing() -> None:
    """Resetea el estado global del módulo para tests aislados.

    Restaura el buffer, el flag activo y descarta entradas acumuladas.

    REGLA: llamar ÚNICAMENTE desde fixtures de pytest (autouse=True).
    Nunca llamar en código de producción — no está en ``__all__``.

    Principios: SafeOps (tests aislados), SSOT (un solo punto de reset).
    """
    with _lock:
        _buffer.clear()
        _active[0] = True
