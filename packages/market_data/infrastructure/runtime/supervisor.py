# -*- coding: utf-8 -*-
"""
market_data/infrastructure/runtime/supervisor.py
=================================================

RuntimeSupervisor — gestor centralizado del ciclo de vida de tareas
de fondo (background asyncio.Task) del microservicio.

Responsabilidad única (SRP)
---------------------------
Iniciar, supervisar y detener de forma ordenada todas las tareas de
larga duración que hoy viven dispersas en el lifespan de main.py
(ingestion_loop, bronze_writer, feed_orchestrator, y futuras).

Por qué existe
---------------
El lifespan actual repite, por cada tarea nueva, la misma secuencia
frágil: crear la task, guardarla en una variable suelta, y en el
shutdown iterar un tuple armado a mano con cancel() + wait_for()
envuelto en try/except (asyncio.CancelledError, asyncio.TimeoutError).
Ese patrón ya causó el bug C-05 (shield() + wait_for generaba tasks
zombie). Centralizarlo aquí significa que la lógica de cancelación
correcta se escribe y se prueba una sola vez (DRY, SSOT).

No reemplaza a FeedOrchestrator ni a ningún componente de dominio:
FeedOrchestrator sigue administrando sus adapters internos. El
supervisor solo administra el asyncio.Task en el que corre
`orchestrator.run()`, igual que administra el de `_ingestion_loop`
o `_bronze_writer_loop`.

Contrato de uso (Orden Lógico — Fail-Fast por diseño)
------------------------------------------------------
1. register() — cero o más veces, ANTES de start().
2. start()    — UNA vez. Lanza las tareas registradas.
3. stop()     — UNA vez, típicamente en el finally del lifespan.

Llamar register() después de start(), o start() más de una vez, es
un error de programación (no un caso operativo válido) y por lo
tanto levanta RuntimeError inmediatamente (Fail-Fast) en vez de
degradar silenciosamente.

Principios: SRP · KISS · DRY · SSOT · DIP · Fail-Fast (arranque/uso
indebido) · Fail-Soft (tareas no críticas) · SafeOps
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any

from loguru import logger

# --------------------------------------------------------------------------- #
# Especificación de una tarea supervisada                                     #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class ManagedTask:
    """
    Declara una tarea de fondo a supervisar.

    Parameters
    ----------
    name:
        Identificador único y legible — se usa como nombre de asyncio.Task,
        como clave interna del supervisor (SSOT) y en logs/health checks.
    factory:
        Callable sin argumentos que retorna la corrutina a ejecutar.
        Se invoca recién en start(), no en el registro — evita crear
        corrutinas "nunca-await" si el supervisor no llega a arrancar.
    critical:
        True  → un fallo al construir/lanzar la tarea aborta el arranque
                completo del supervisor (Fail-Fast). Usar para tareas sin
                las cuales el servicio no tiene sentido operar (p. ej.
                ingestion_loop).
        False → el fallo se loguea y el supervisor continúa sin esa tarea
                (Fail-Soft). Usar para tareas opcionales/degradables
                (p. ej. feed_orchestrator en modo REST-only).
    shutdown_timeout_s:
        Segundos a esperar tras cancel() antes de abandonar el join de
        esta tarea puntual durante stop().
    """

    name: str
    factory: Callable[[], Coroutine[Any, Any, None]]
    critical: bool = True
    shutdown_timeout_s: float = 10.0


# --------------------------------------------------------------------------- #
# RuntimeSupervisor                                                            #
# --------------------------------------------------------------------------- #


class RuntimeSupervisor:
    """
    Administra el ciclo de vida completo de un conjunto de ManagedTask.

    SSOT: `_entries` es la única fuente de verdad sobre qué tareas existen
    y su configuración (critical, timeout); `_tasks` solo guarda el
    asyncio.Task en ejecución de cada una. Nunca se duplica la lista de
    nombres en una estructura paralela.

    Uso
    ---
        supervisor = RuntimeSupervisor()
        supervisor.register(ManagedTask("ingestion", lambda: _ingestion_loop(ctx, guard)))
        supervisor.register(ManagedTask("bronze_writer", _bronze_writer_loop, critical=False))
        if (orch := CompositionRoot.build_feed_orchestrator(ctx.app_config)) is not None:
            supervisor.register(ManagedTask("feed_orchestrator", orch.run, critical=False))

        await supervisor.start()
        try:
            yield  # FastAPI sirve peticiones aquí
        finally:
            await supervisor.stop()
    """

    def __init__(self) -> None:
        self._entries: dict[str, ManagedTask] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._started = False
        self._log = logger.bind(component="RuntimeSupervisor")

    # ------------------------------------------------------------------ #
    # Registro — solo válido antes de start()                             #
    # ------------------------------------------------------------------ #

    def register(self, spec: ManagedTask) -> None:
        """
        Agrega una tarea a la lista de gestionadas. No la lanza todavía.

        Raises
        ------
        RuntimeError
            Si el supervisor ya fue iniciado (orden lógico violado) o si
            `spec.name` ya está registrado (violación de SSOT — un mismo
            nombre no puede referir a dos tareas distintas).
        """
        if self._started:
            raise RuntimeError(f"No se puede registrar {spec.name!r}: el supervisor ya fue iniciado con start().")
        if spec.name in self._entries:
            raise RuntimeError(f"ManagedTask duplicada: {spec.name!r} ya está registrada.")
        self._entries[spec.name] = spec

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        """
        Lanza todas las tareas registradas como asyncio.Task concurrentes.

        Fail-Fast (críticas): si `factory()` lanza al construirse, se
        detiene todo lo ya lanzado (stop()) y se re-lanza la excepción —
        un servicio a medio arrancar es peor que uno que no arranca.

        Fail-Soft (no críticas): se loguea la excepción y se continúa sin
        esa tarea — replica el comportamiento ya validado hoy con
        `feed_orchestrator_build_failed`.

        Raises
        ------
        RuntimeError
            Si start() ya fue llamado antes (orden lógico violado).
        """
        if self._started:
            raise RuntimeError("RuntimeSupervisor.start() ya fue invocado — no es reentrante.")
        self._started = True

        for name, spec in self._entries.items():
            try:
                task = asyncio.create_task(spec.factory(), name=name)
                self._tasks[name] = task
                self._log.info("task_started", task=name, critical=spec.critical)
            except Exception as exc:
                if spec.critical:
                    self._log.error("critical_task_failed_to_start", task=name, error=str(exc))
                    await self.stop()
                    raise
                self._log.warning(
                    "optional_task_failed_to_start — continuing degraded",
                    task=name,
                    error=str(exc),
                )

    async def stop(self) -> None:
        """
        Cancela y espera todas las tareas lanzadas, en orden inverso de arranque.

        SafeOps: cancel() + wait_for() SIN shield() — la task recibe
        CancelledError real y puede hacer su cleanup en try/finally;
        wait_for() solo acota cuánto esperamos ese cleanup. Nunca se usan
        ambos juntos (bug C-05: shield() impedía la cancelación real y
        wait_for igual expiraba, dejando tasks zombie corriendo).

        Idempotente: llamar stop() sin tareas en ejecución (p. ej. tras
        un start() que falló Fail-Fast) no lanza error.
        """
        for name, task in reversed(list(self._tasks.items())):
            if task.done():
                continue
            task.cancel()
            timeout = self._entries[name].shutdown_timeout_s
            try:
                await asyncio.wait_for(task, timeout=timeout)
            except (asyncio.CancelledError, TimeoutError):
                self._log.warning(
                    "task_shutdown_timeout_or_cancelled",
                    task=name,
                    timeout_s=timeout,
                )
            except Exception as exc:
                # La tarea pudo terminar con una excepción propia durante
                # su cleanup — se loguea pero no bloquea el shutdown del resto.
                self._log.warning("task_shutdown_raised", task=name, error=str(exc))

        self._log.info("supervisor_stopped", tasks=list(self._tasks))

    # ------------------------------------------------------------------ #
    # Introspección — para /health, /ready                                 #
    # ------------------------------------------------------------------ #

    def status(self) -> dict[str, bool]:
        """Retorna {nombre: running} — running=False si terminó o nunca arrancó."""
        return {name: self._tasks.get(name) is not None and not self._tasks[name].done() for name in self._entries}

    @property
    def is_healthy(self) -> bool:
        """True si todas las tareas CRÍTICAS registradas siguen corriendo.

        Las no críticas pueden estar caídas (modo degradado) sin afectar
        este valor — es la señal correcta para /health, no para /ready
        detallado (usar status() para el detalle completo).
        """
        return all(
            self._tasks.get(name) is not None and not self._tasks[name].done()
            for name, spec in self._entries.items()
            if spec.critical
        )

    def __repr__(self) -> str:
        return f"RuntimeSupervisor(tasks={list(self._entries)}, started={self._started})"


__all__ = ["ManagedTask", "RuntimeSupervisor"]
