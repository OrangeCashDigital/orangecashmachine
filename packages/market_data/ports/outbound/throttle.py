# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/throttle.py
========================================

Puerto OUTBOUND: retroalimentación de señales de presión al throttle adaptivo.

Desacopla application/strategies de AdaptiveThrottle concreto (adapters/).
Expone solo los métodos que las strategies necesitan — ISP: no más interfaz
de la necesaria.

Implementación concreta: adapters.outbound.exchange.throttle.AdaptiveThrottle
No-op (tests):           ports.outbound.throttle.NullThrottle

Principios: DIP · ISP · Protocol (structural subtyping) · SafeOps
"""

from __future__ import annotations

from typing import Optional, Protocol, runtime_checkable


@runtime_checkable
class ThrottlePort(Protocol):
    """
    Contrato mínimo de retroalimentación para el throttle adaptivo.

    Las strategies registran señales (éxito, error, OCC) por par ejecutado.
    La implementación (AdaptiveThrottle) ajusta la concurrencia dinámicamente.

    Fire-and-forget: ningún método lanza excepciones.

    Propiedades de lectura
    ----------------------
    current : concurrencia activa en este momento. OHLCVPipeline la lee
              tras cada par para sincronizar max_concurrency con el throttle.
    """

    @property
    def current(self) -> int:
        """Nivel de concurrencia activo según el throttle."""
        ...

    def record_success(self, latency_ms: Optional[float] = None) -> None:
        """Registra una operación exitosa con latencia opcional."""
        ...

    def record_error(
        self,
        error_type: str = "network",
        latency_ms: Optional[float] = None,
    ) -> None:
        """
        Registra un error de exchange.

        error_type: "rate_limit" (×2.0) | "timeout" (×1.0) | "network" (×0.5)
        """
        ...

    def record_occ_conflict(self) -> None:
        """
        Registra un OCC conflict de storage (Bronze/Silver).

        Señal de contención de escritura — independiente del error de exchange.
        """
        ...

    def record_rate_limit_hit(self) -> None:
        """Registra un hit de rate limit — señal de presión máxima."""
        ...


class NullThrottle:
    """
    Implementación vacía de ThrottlePort.

    Uso: tests, entornos sin throttle inyectado (modo Kappa — sin Bronze directo).

    current siempre retorna 1 — concurrencia mínima segura para modo no-throttled.
    """

    @property
    def current(self) -> int:
        return 1

    def record_success(self, latency_ms: Optional[float] = None) -> None:
        pass

    def record_error(
        self,
        error_type: str = "network",
        latency_ms: Optional[float] = None,
    ) -> None:
        pass

    def record_occ_conflict(self) -> None:
        pass

    def record_rate_limit_hit(self) -> None:
        pass


__all__ = ["ThrottlePort", "NullThrottle"]
