# -*- coding: utf-8 -*-
"""
market_data/processing/pipelines/_worker_pool.py
=================================================

Worker pool genérico producer/consumer para pipelines de ingestion.

Responsabilidad única
---------------------
Gestionar concurrencia, stagger de arranque, timeout, cancelación y
drenado de queue. NO conoce el dominio de los items ni el tipo de resultado.

Uso
---
    from market_data.processing.pipelines._worker_pool import run_worker_pool

    results, aborted = await run_worker_pool(
        items           = [(idx, symbol, tf), ...],
        execute_fn      = self._execute_pair,   # async (item) -> Result
        max_concurrency = 4,
        exchange_id     = "bybit",
        log             = self._log,
    )

Parámetros
----------
items           : secuencia de items a procesar (cualquier tipo hashable)
execute_fn      : coroutine async (item) -> T — ejecuta un item
max_concurrency : número máximo de workers simultáneos
exchange_id     : identificador del exchange — usado en logs y errores
log             : logger bindeado (loguru o compatible)
stagger_s       : segundos entre arranques de workers (thundering herd prevention)
timeout_s       : timeout global del pool en segundos (default 3600)
on_abort        : callable opcional (item, exc) -> bool — si retorna True,
                  el pool drena la queue y aborta. Usado por OHLCV para
                  circuit breaker. Si es None, todos los errores se propagan
                  al caller via resultado.

Retorno
-------
tuple[list[T], bool]
    results  : lista de resultados en orden de completión (no determinístico)
    aborted  : True si on_abort señalizó abort durante la ejecución

Principios
----------
SOLID  — SRP: solo gestión de concurrencia, sin lógica de dominio
OCP    — extensible vía execute_fn y on_abort sin modificar este módulo
DRY    — fuente única de verdad para el patrón producer/consumer
KISS   — interfaz mínima; complejidad solo donde es necesaria
SafeOps — timeout, CancelledError y abort siempre limpian workers

Ref
---
- Stevens, "Unix Network Programming" — thundering herd prevention
- asyncio queue docs: https://docs.python.org/3/library/asyncio-queue.html
"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from typing import TypeVar

T = TypeVar("T")

# Stagger por defecto entre workers: 50ms × N workers.
# Imperceptible en pipelines pequeños, efectivo bajo carga.
# Cada pipeline puede sobreescribir via parámetro stagger_s.
_DEFAULT_STAGGER_S: float = 0.05
_DEFAULT_TIMEOUT_S: float = 3600.0


async def run_worker_pool(
    items:           Sequence,
    execute_fn:      Callable[..., Awaitable[T]],
    max_concurrency: int,
    exchange_id:     str,
    log,
    *,
    stagger_s:  float                                    = _DEFAULT_STAGGER_S,
    timeout_s:  float                                    = _DEFAULT_TIMEOUT_S,
    on_abort:   Callable[..., bool] | None               = None,
) -> tuple[list[T], bool]:
    """
    Ejecuta execute_fn sobre cada item con concurrencia limitada.

    El producer encola todos los items al inicio. Cada worker toma
    un item, llama execute_fn(item), y deposita el resultado.
    El número de workers activos nunca supera max_concurrency.

    on_abort
    --------
    Si se proporciona, se llama con (item, exc) cuando execute_fn lanza.
    Si retorna True: abort_event se activa, la queue se drena, todos los
    workers salen limpiamente. Esto implementa circuit breaker coordination
    sin acoplar este módulo al dominio OHLCV.

    SafeOps
    -------
    - queue.join() con timeout evita deadlock si un worker muere
    - CancelledError cancela todos los workers y se re-lanza
    - finally garantiza cancelación de workers en cualquier path de salida
    """
    if max_concurrency < 1:
        raise ValueError(f"run_worker_pool: max_concurrency debe ser >= 1, got {max_concurrency}")
    if not items:
        return [], False

    queue:       asyncio.Queue = asyncio.Queue()
    results:     list[T]       = []
    abort_event: asyncio.Event = asyncio.Event()

    for item in items:
        await queue.put(item)

    async def worker() -> None:
        while True:
            # Abort coordinado: drenar toda la queue y salir limpiamente.
            # Sin este drenado, queue.join() bloquea hasta timeout cuando
            # N items > M workers y algunos items nunca se procesan.
            if abort_event.is_set():
                while True:
                    try:
                        queue.get_nowait()
                        queue.task_done()
                    except asyncio.QueueEmpty:
                        break
                return

            try:
                item = queue.get_nowait()
            except asyncio.QueueEmpty:
                return

            try:
                result = await execute_fn(item)
                results.append(result)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Delegar decisión de abort al caller via on_abort.
                # Si on_abort es None o retorna False: el error se propaga
                # naturalmente — execute_fn debe capturarlo si es esperado.
                if on_abort is not None and on_abort(item, exc):
                    abort_event.set()
                    # Drenar items pendientes para liberar queue.join().
                    drained = 0
                    while True:
                        try:
                            queue.get_nowait()
                            queue.task_done()
                            drained += 1
                        except asyncio.QueueEmpty:
                            break
                    log.warning(
                        "worker_pool_aborted | exchange={} drained={}",
                        exchange_id, drained,
                    )
                    return
                raise
            finally:
                queue.task_done()

    async def _staggered_worker(delay_s: float) -> None:
        if delay_s > 0:
            await asyncio.sleep(delay_s)
        await worker()

    worker_tasks = [
        asyncio.create_task(_staggered_worker(i * stagger_s))
        for i in range(max_concurrency)
    ]

    try:
        await asyncio.wait_for(queue.join(), timeout=timeout_s)
    except asyncio.TimeoutError:
        log.error(
            "worker_pool_timeout | exchange={} timeout_s={}",
            exchange_id, timeout_s,
        )
        for w in worker_tasks:
            w.cancel()
        await asyncio.gather(*worker_tasks, return_exceptions=True)
        raise RuntimeError(
            f"Worker pool timed out after {timeout_s:.0f}s"
            f" | exchange={exchange_id}"
        )
    except asyncio.CancelledError:
        for w in worker_tasks:
            w.cancel()
        await asyncio.gather(*worker_tasks, return_exceptions=True)
        log.warning(
            "worker_pool_cancelled | exchange={} workers={}",
            exchange_id, len(worker_tasks),
        )
        raise
    finally:
        for w in worker_tasks:
            if not w.done():
                w.cancel()

    return results, abort_event.is_set()


__all__ = ["run_worker_pool"]
