# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/kafka_consumer.py
=============================================

Puerto OUTBOUND: contrato que usan los stream processors para consumir
mensajes de Kafka.

Responsabilidad
---------------
Definir la abstracción que cualquier consumer Kafka debe cumplir.
Los stream processors dependen de este protocolo — nunca de
aiokafka directamente (DIP).

Consumer groups OCM
-------------------
  "ocm-bronze-writer"   — escribe velas crudas a Iceberg Bronze
  "ocm-quality-gate"    — valida y publica a ohlcv.validated
  "ocm-dagster-trigger" — dispara assets Dagster on new data

Semántica de entrega
--------------------
at-least-once: el consumer hace commit del offset DESPUÉS de procesar
exitosamente. Si falla antes del commit, el mensaje se reprocesa.
El stream processor es responsable de idempotencia (dedup via Iceberg
snapshot o event_id).

Principios
----------
DIP     — stream processors dependen de este Protocol, no de aiokafka
ISP     — interfaz mínima: poll · commit · close
OCP     — KafkaConsumerAdapter implementa sin modificar este contrato
SafeOps — poll() retorna lista vacía si Kafka no responde (no lanza)
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Protocol, runtime_checkable


# ---------------------------------------------------------------------------
# KafkaMessage — value object inmutable del mensaje recibido
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class KafkaMessage:
    """
    Mensaje recibido de Kafka — value object inmutable.

    Campos
    ------
    topic     : tópico de origen
    partition : partición de origen
    offset    : offset dentro de la partición (para commit)
    key       : routing key bytes (puede ser None)
    value     : payload bytes (JSON UTF-8)
    timestamp : timestamp del mensaje en ms (epoch)
    headers   : metadatos opcionales del mensaje
    """
    topic:     str
    partition: int
    offset:    int
    key:       Optional[bytes]
    value:     bytes
    timestamp: int
    headers:   tuple = ()   # tuple para inmutabilidad (frozen=True)


# ---------------------------------------------------------------------------
# KafkaConsumerPort — Protocol (DIP)
# ---------------------------------------------------------------------------

@runtime_checkable
class KafkaConsumerPort(Protocol):
    """
    Contrato async de consumo desde Kafka.

    Implementado por: KafkaConsumerAdapter (infrastructure/kafka/consumer.py)
    Usado por       : infrastructure/kafka/bronze_writer.py
                      infrastructure/kafka/quality_gate.py

    Semántica at-least-once
    -----------------------
    1. poll()   → obtener mensajes
    2. procesar cada mensaje
    3. commit() → confirmar offset solo si procesamiento exitoso
    Si falla entre 2 y 3, el mensaje se reprocesa en el siguiente poll().
    El processor garantiza idempotencia.

    SafeOps
    -------
    poll() DEBE retornar lista vacía si Kafka no responde.
    commit() DEBE ser fail-soft: loguear error, no propagar.
    """

    async def poll(
        self,
        timeout_ms: int = 1000,
        max_records: int = 500,
    ) -> List[KafkaMessage]:
        """
        Obtiene hasta max_records mensajes del broker.

        Parámetros
        ----------
        timeout_ms  : tiempo máximo de espera en ms
        max_records : máximo de mensajes por poll

        Retorna
        -------
        Lista de KafkaMessage (vacía si no hay mensajes o Kafka no responde).
        SafeOps: nunca lanza.
        """
        ...

    async def commit(self) -> None:
        """
        Confirma el offset del último mensaje procesado.

        Llamar DESPUÉS de procesar exitosamente — semántica at-least-once.
        SafeOps: loguea error si Kafka no responde, no propaga.
        """
        ...

    async def close(self) -> None:
        """
        Cierra la conexión al broker limpiamente.

        Llamar en shutdown. Idempotente.
        """
        ...

    async def seek_to_beginning(self) -> None:
        """
        Reposiciona el consumer al inicio de todas las particiones asignadas.

        Útil para replay completo (Kappa reprocessing).
        Solo válido después de que las particiones estén asignadas (post-poll).
        """
        ...


__all__ = [
    "KafkaConsumerPort",
    "KafkaMessage",
]
