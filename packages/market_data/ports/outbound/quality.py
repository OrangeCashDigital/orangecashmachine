# -*- coding: utf-8 -*-
"""
market_data/ports/quality.py
=============================

Puertos de calidad de datos del bounded context market_data.

Responsabilidad
---------------
Declarar los contratos que las implementaciones de quality deben cumplir.
Ningún módulo de dominio o aplicación importa implementaciones concretas
de quality/ — solo estos protocolos (DIP).

Exports
-------
AnomalyRegistryPort  — contrato de registro persistente de anomalías

Principios
----------
DIP   — dominio depende de abstracción, nunca de SQLite ni implementación concreta
OCP   — nuevas implementaciones (Redis, Postgres) sin modificar este contrato
ISP   — interfaz mínima: solo lo que los pipelines realmente necesitan
SSOT  — única definición del contrato; reemplaza la declaración en domain/boundaries.py
"""
from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class AnomalyRegistryPort(Protocol):
    """
    Contrato mínimo que debe satisfacer cualquier registro de anomalías.

    Implementación de referencia
    ----------------------------
    market_data.quality.anomaly_registry.AnomalyRegistry  (SQLite + L1 cache)

    SafeOps
    -------
    Todas las implementaciones deben ser fail-soft:
    nunca propagar excepciones al caller — el pipeline no debe detenerse
    por un fallo en el registro de anomalías.

    Métodos
    -------
    is_new   — True si la anomalía es nueva y la registra; False si ya existía.
    stats    — lista de anomalías conocidas con conteos para observabilidad.
    wipe     — borra todo el historial; solo para tests o reset manual.
    """

    def is_new(
        self,
        exchange:  str,
        symbol:    str,
        timeframe: str,
        reason:    str,
    ) -> bool:
        """
        Retorna True si esta anomalía no había sido vista antes.

        Efecto lateral: registra la anomalía en el backend persistente.
        Thread-safe: implementaciones deben garantizar atomicidad del check+write.
        """
        ...

    def stats(self) -> list[dict]:
        """
        Retorna lista de anomalías conocidas con conteos.

        Útil para dashboards y auditorías. Fail-soft: retorna [] en caso de error.
        """
        ...

    def wipe(self) -> None:
        """
        Borra todo el historial de anomalías.

        ⚠️  Solo para tests o reset manual. No llamar en producción.
        """
        ...
