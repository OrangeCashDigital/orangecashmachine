# -*- coding: utf-8 -*-
"""
ocm_platform/runtime/guard.py
==============================

Kill switch + runtime guard para cualquier proceso de ejecución OCM.

Responsabilidad
---------------
Primitivo de runtime compartido: puede ser usado por market_data,
trading y cualquier otro servicio sin crear dependencias cruzadas.

Diseño
------
El guard se construye en el entrypoint de cada servicio y se pasa
por parámetro a cualquier componente que necesite verificar si debe
continuar. El contrato es explícito: si recibes un ExecutionGuard,
eres cancelable.

Primitiva: threading.Event
    - Thread-safe sin locks adicionales
    - Observable con .is_set() desde cualquier contexto (sync y async)
    - Compatible con asyncio.run() + threads de Prefect

Triggers de stop (en orden de prioridad)
-----------------------------------------
1. Manual via .trigger(reason)           — kill switch explícito
2. Errores consecutivos > max_errors     — exchange o pipeline degradado
3. Runtime > max_runtime_s              — timeout de sesión

Principios: SRP · SafeOps · SSOT
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Optional

__all__ = ["ExecutionGuard", "ExecutionStoppedError"]


class ExecutionStoppedError(RuntimeError):
    """Raised when ExecutionGuard determines execution must stop."""


@dataclass
class ExecutionGuard:
    """
    Kill switch + runtime guard para pipelines.

    Parameters
    ----------
    max_errors    : int           — errores consecutivos antes de stop automático
                                   (0 = desactivado)
    max_runtime_s : float | None  — segundos máximos de ejecución
                                   (None = sin límite)
    """

    max_errors:    int            = 10
    max_runtime_s: Optional[float] = None

    # Estado interno — no expuesto en constructor
    _stop_event:         threading.Event = field(
        default_factory=threading.Event, init=False, repr=False
    )
    _start_time:         Optional[float] = field(default=None,  init=False, repr=False)
    _stop_reason:        Optional[str]   = field(default=None,  init=False, repr=False)
    _consecutive_errors: int             = field(default=0,     init=False, repr=False)
    _total_errors:       int             = field(default=0,     init=False, repr=False)
    _total_successes:    int             = field(default=0,     init=False, repr=False)
    _lock:               threading.Lock  = field(
        default_factory=threading.Lock, init=False, repr=False
    )

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    def start(self) -> None:
        """Marca el inicio de la sesión de ejecución."""
        with self._lock:
            self._start_time         = time.monotonic()
            self._stop_event.clear()
            self._stop_reason        = None
            self._consecutive_errors = 0
            self._total_errors       = 0
            self._total_successes    = 0

    def stop(self) -> None:
        """Detención limpia al finalizar la sesión (llamar en finally)."""
        with self._lock:
            if not self._stop_event.is_set():
                self._stop_event.set()
                self._stop_reason = self._stop_reason or "clean_shutdown"

    # ------------------------------------------------------------------ #
    # Kill switch
    # ------------------------------------------------------------------ #

    def trigger(self, reason: str) -> None:
        """
        Activa el kill switch con razón explícita.

        Thread-safe. Idempotente: si ya está activo, preserva la razón original.
        """
        with self._lock:
            if not self._stop_event.is_set():
                self._stop_event.set()
                self._stop_reason = reason

    def should_stop(self) -> bool:
        """
        Retorna True si el guard indica que la ejecución debe detenerse.

        Verifica en orden:
        1. Kill switch activado manualmente
        2. Runtime excedido
        """
        if self._stop_event.is_set():
            return True

        if self.max_runtime_s is not None and self._start_time is not None:
            elapsed = time.monotonic() - self._start_time
            if elapsed >= self.max_runtime_s:
                self.trigger(f"runtime_limit_exceeded:{elapsed:.0f}s")
                return True

        return False

    @property
    def stop_reason(self) -> Optional[str]:
        """Razón de parada — None si el guard no está activo."""
        return self._stop_reason

    def check(self) -> None:
        """
        Verifica si debe detenerse — lanza ExecutionStoppedError si es así.

        Usar en loops de pipeline donde se quiere corte limpio:
            guard.check()   # al inicio de cada iteración
        """
        if self.should_stop():
            raise ExecutionStoppedError(
                f"Execution stopped: {self._stop_reason or 'kill_switch'}"
            )

    # ------------------------------------------------------------------ #
    # Error tracking
    # ------------------------------------------------------------------ #

    def record_error(self, reason: str = "") -> None:
        """
        Registra un error. Si supera max_errors consecutivos, activa kill switch.

        Llamar desde pipelines cuando una operación falla de forma no recuperable.
        """
        with self._lock:
            self._consecutive_errors += 1
            self._total_errors       += 1

            if self.max_errors > 0 and self._consecutive_errors >= self.max_errors:
                trigger_reason = (
                    f"consecutive_errors:{self._consecutive_errors}"
                    + (f":{reason}" if reason else "")
                )
                if not self._stop_event.is_set():
                    self._stop_event.set()
                    self._stop_reason = trigger_reason

    def record_success(self) -> None:
        """Registra éxito y resetea contador de errores consecutivos."""
        with self._lock:
            self._consecutive_errors = 0
            self._total_successes   += 1

    # ------------------------------------------------------------------ #
    # Observability
    # ------------------------------------------------------------------ #

    def summary(self) -> dict:
        """
        Resumen del estado del guard para record_run y logging.

        SafeOps: nunca lanza excepción.
        """
        try:
            elapsed = (
                time.monotonic() - self._start_time
                if self._start_time is not None
                else 0.0
            )
            return {
                "triggered":          self._stop_event.is_set(),
                "stop_reason":        self._stop_reason,
                "runtime_s":          round(elapsed, 2),
                "total_errors":       self._total_errors,
                "total_successes":    self._total_successes,
                "consecutive_errors": self._consecutive_errors,
            }
        except Exception:
            return {"triggered": False, "stop_reason": None}
