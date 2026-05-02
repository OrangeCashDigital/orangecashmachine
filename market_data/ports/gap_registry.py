# -*- coding: utf-8 -*-
"""
market_data/ports/gap_registry.py
==================================

Puerto del registro de gaps de ingesta OHLCV.

Responsabilidad
---------------
Declarar el contrato mínimo que cualquier implementación de GapRegistry
debe cumplir para ser inyectada en RepairStrategy via PipelineContext.

Por qué existe este puerto
--------------------------
repair.py importaba infra.state.factories directamente (3 ocurrencias),
creando un acoplamiento application → infra que viola DIP.
Con este puerto, repair.py depende de la abstracción, no de la
implementación concreta. infra.state.gap_registry.GapRegistry satisface
este contrato estructuralmente (duck typing via runtime_checkable).

Contrato semántico
------------------
- is_irrecoverable : retorna False ante fallo de backend (SafeOps).
                     Un gap nunca se omite por error de Redis.
- mark_healed      : idempotente — llamar dos veces con los mismos
                     argumentos no produce estado inconsistente.
                     irreversible=True marca el gap como sin datos
                     en el exchange — suprime retries infinitos.

Principios
----------
DIP  — application depende de abstracción, nunca de infra concreta
OCP  — nuevos backends (e.g. Postgres, in-memory) no modifican este contrato
KISS — interfaz mínima: solo los 2 métodos usados por RepairStrategy
"""
from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class GapRegistryPort(Protocol):
    """
    Contrato mínimo del registro de gaps irrecuperables.

    Implementación de referencia : infra.state.gap_registry.GapRegistry
    Implementación noop (tests)  : None — RepairStrategy acepta None
                                   y degrada silenciosamente (SafeOps).

    SafeOps
    -------
    Las implementaciones deben capturar todos los errores de backend
    internamente y retornar el valor por defecto seguro:
      - is_irrecoverable → False   (nunca omitir un gap por error)
      - mark_healed      → no-op   (perder un registro es aceptable)
    """

    def is_irrecoverable(
        self,
        exchange:   str,
        symbol:     str,
        timeframe:  str,
        start_ms:   int,
    ) -> bool:
        """
        Retorna True si este gap fue marcado previamente como sin datos
        disponibles en el exchange.

        SafeOps: retorna False ante cualquier fallo de backend.
        Un gap sano nunca se omite por error de Redis/infra.
        """
        ...

    def mark_healed(
        self,
        exchange:     str,
        symbol:       str,
        timeframe:    str,
        start_ms:     int,
        irreversible: bool = False,
    ) -> None:
        """
        Registra que el gap fue procesado.

        irreversible=True : el exchange no tiene datos para este rango.
                            Suprime retries futuros (NoDataAvailableError).
        irreversible=False: gap sanado exitosamente — limpia el registro.

        SafeOps: idempotente, no lanza excepciones.
        """
        ...
