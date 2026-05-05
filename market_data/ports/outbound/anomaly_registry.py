# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/anomaly_registry.py
================================================

Puerto OUTBOUND: contrato del registro persistente de anomalías.

Movido desde: ports/quality.py (SSOT correcto en outbound/)

Responsabilidad
---------------
Declarar el contrato que cualquier implementación de registro
de anomalías debe cumplir. Los use cases del application layer
dependen de este protocolo, nunca de SQLite.

Implementación de referencia
-----------------------------
market_data.adapters.outbound.persistence.sqlite.anomaly_registry.AnomalyRegistry

Principios
----------
DIP  — application depende de abstracción, no de SQLite
OCP  — Redis, Postgres, InMemory implementan este contrato
ISP  — interfaz mínima: is_new · stats · wipe
"""
from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class AnomalyRegistryPort(Protocol):
    """
    Contrato mínimo para registro persistente de anomalías.

    SafeOps
    -------
    Todas las implementaciones DEBEN ser fail-soft:
    nunca propagar excepciones al caller — el pipeline no debe
    detenerse por un fallo en el registro de anomalías.
    """

    def is_new(
        self,
        exchange:  str,
        symbol:    str,
        timeframe: str,
        reason:    str,
    ) -> bool:
        """
        True si esta anomalía es nueva y la registra.
        False si ya existía (incrementa contador en background).

        Efecto lateral: registra en backend persistente.
        Thread-safe: check+write atómico.
        """
        ...

    def stats(self) -> list[dict]:
        """
        Lista de anomalías conocidas con conteos.

        Fail-soft: retorna [] en caso de error.
        """
        ...

    def wipe(self) -> None:
        """
        Borra todo el historial.

        ⚠️  Solo para tests o reset manual.
        """
        ...


__all__ = ["AnomalyRegistryPort"]
