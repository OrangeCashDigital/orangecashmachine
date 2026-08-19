# -*- coding: utf-8 -*-
"""
tests/kafka/conftest.py
========================

Harness de integración Kafka para tests/kafka/.

SSOT de: broker address, helpers de producer/consumer, warm-up,
drain y find_event. Ningún test_*.py debe duplicar estas constantes
o helpers — importar siempre desde aquí.

Contrato: tests/kafka/CONTRACT.md
Principios: SSOT · DRY · R-01..R-04 · SafeOps
"""

from __future__ import annotations

import time
import uuid
from typing import List

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from loguru import logger

from shared.kafka.schemas.ohlcv import EventPayload
from shared.kafka.serializer import deserialize

# =============================================================================
# Configuración — SSOT del harness
# =============================================================================

BROKER: str = "localhost:9093"
"""Listener EXTERNAL del broker. El INTERNAL (kafka:9092) no resuelve desde host."""

_DRAIN_TIMEOUT_S: float = 30.0
"""Timeout máximo de _drain(). Tests de replay pueden necesitar >10s."""

_POLL_TIMEOUT_MS: int = 500
"""Timeout de getmany() por ciclo. Bajo para que los tests sean rápidos."""

_WARM_UP_POLLS: int = 3
"""Ciclos de getmany() vacíos para completar partition assignment (R-02)."""


# =============================================================================
# R-01 — Group ID único por test
# =============================================================================


def _unique_group(prefix: str) -> str:
    """
    Genera un group_id único para un test.

    Formato: "{prefix}-{uuid8}"
    Garantía: nunca colisiona entre runs ni entre tests paralelos.

    CONTRACT R-01: cada test debe usar su propio group_id.
    """
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


# =============================================================================
# Factories de producer/consumer raw
# =============================================================================


async def _raw_producer() -> AIOKafkaProducer:
    """
    Crea y arranca un AIOKafkaProducer apuntando al listener EXTERNAL.

    El caller es responsable de llamar .stop() en finally.
    """
    p = AIOKafkaProducer(
        bootstrap_servers=BROKER,
        enable_idempotence=False,
    )
    await p.start()
    return p


async def _raw_consumer(
    group_id: str,
    topic: str,
    *,
    offset_reset: str = "earliest",
) -> AIOKafkaConsumer:
    """
    Crea y arranca un AIOKafkaConsumer apuntando al listener EXTERNAL.

    Args:
        group_id:     Debe ser único por test (R-01).
        topic:        Topic a suscribir.
        offset_reset: 'earliest' para replay, 'latest' para listeners DLQ (R-02).

    El caller es responsable de llamar .stop() en finally.

    CONTRACT R-02: para capturar solo mensajes del test actual,
    usar offset_reset='latest' + warm_up_consumer() ANTES de producir.
    """
    c = AIOKafkaConsumer(
        topic,
        bootstrap_servers=BROKER,
        group_id=group_id,
        auto_offset_reset=offset_reset,
        enable_auto_commit=False,
        session_timeout_ms=10_000,
        heartbeat_interval_ms=3_000,
        fetch_max_wait_ms=_POLL_TIMEOUT_MS,
    )
    await c.start()
    return c


# =============================================================================
# R-02 — warm_up_consumer: completar partition assignment antes de producir
# =============================================================================


async def warm_up_consumer(consumer: AIOKafkaConsumer) -> None:
    """
    Completa el partition assignment realizando ciclos vacíos de getmany().

    CONTRACT R-02: llamar ANTES de producir cuando offset_reset='latest'.
    Sin warm-up, el assignment es async y el mensaje puede llegar antes de
    que el consumer tenga las particiones → se pierde para offset=latest.

    Realiza _WARM_UP_POLLS ciclos de getmany() vacíos como señal de que
    el broker completó el assignment. No es determinista pero es suficiente
    para los tiempos de latencia de un broker local.
    """
    for _ in range(_WARM_UP_POLLS):
        await consumer.getmany(timeout_ms=_POLL_TIMEOUT_MS, max_records=1)


# =============================================================================
# R-03, R-04 — find_event: buscar por event_id, no por posición
# =============================================================================


async def find_event(
    consumer: AIOKafkaConsumer,
    event_id: str,
    *,
    timeout_s: float = 15.0,
) -> bool:
    """
    Busca un evento por event_id en el stream del consumer.

    CONTRACT R-03: nunca asumir posición — filtrar por event_id.
    CONTRACT R-04: ignorar mensajes no parseables (basura histórica del topic).

    Returns:
        True si el evento fue encontrado antes del timeout.
        False en caso contrario (el test debe fallar con mensaje descriptivo).
    """
    from shared.kafka.schemas.ohlcv import EventPayload
    from shared.kafka.serializer import deserialize

    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        raw = await consumer.getmany(
            timeout_ms=_POLL_TIMEOUT_MS,
            max_records=100,
        )
        for _tp, records in raw.items():
            for r in records:
                try:
                    evt = deserialize(r.value, EventPayload)
                    if evt.event_id == event_id:
                        return True
                except Exception:
                    # CONTRACT R-04: ignorar mensajes no parseables.
                    pass
    return False


# =============================================================================
# _drain — drenar hasta N mensajes o timeout
# =============================================================================


async def _drain(
    consumer: AIOKafkaConsumer,
    *,
    expected: int,
    expected_ids: set[str] | None = None,
    timeout_s: float = _DRAIN_TIMEOUT_S,
) -> List:
    """
    Drena mensajes de un consumer hasta cumplir la condición de parada
    o timeout.

    CONTRACT R-05 (fix KAF-001): cuando `expected_ids` se provee, la
    condición de parada es "todos los event_id encontrados", NO un tope
    de conteo bruto de mensajes. Esto es obligatorio cuando el topic
    puede contener mensajes de ejecuciones previas (sin limpieza entre
    test runs, KAFKA_LOG_RETENTION_HOURS=168): un tope de conteo bruto
    puede agotarse consumiendo basura histórica antes de alcanzar los
    mensajes relevantes de esta ejecución — causa raíz de KAF-001.

    Si `expected_ids` es None, preserva el comportamiento legado
    (tope de conteo bruto) para callers que no necesitan filtrar por
    event_id.

    Returns:
        Si `expected_ids` es provisto: únicamente los ConsumerRecord cuyo
        event_id está en `expected_ids`, a lo sumo UNO por event_id (el
        primero recibido) — puede ser incompleto si hay timeout, nunca
        contaminado con mensajes ajenos al contrato, y nunca duplicado
        aunque el mismo event_id llegue más de una vez físicamente
        (redelivery legítimo o residuo histórico).
        Si `expected_ids` es None: todos los ConsumerRecord recibidos,
        tope de conteo bruto (comportamiento legado, puede ser menor a
        `expected` si hay timeout).
    """
    collected: List = []
    matched: List = []
    found_ids: set[str] = set()
    skipped_unparseable = 0
    deadline = time.monotonic() + timeout_s

    # `expected_ids` defines the semantic completion contract. `expected`
    # remains only for legacy callers that do not provide event identities.
    max_records = len(expected_ids) if expected_ids is not None else expected

    def _done() -> bool:
        if expected_ids is not None:
            return found_ids.issuperset(expected_ids)
        return len(collected) >= expected

    while not _done() and time.monotonic() < deadline:
        raw = await consumer.getmany(
            timeout_ms=_POLL_TIMEOUT_MS,
            max_records=max_records,
        )
        for _tp, records in raw.items():
            if expected_ids is None:
                collected.extend(records)
                continue
            for r in records:
                try:
                    evt = deserialize(r.value, EventPayload)
                except (ValueError, KeyError, TypeError):
                    # CONTRACT R-04: mensaje no parseable. Cubre json
                    # inválido y SchemaVersionError (ValueError, ver
                    # shared/kafka/schemas/_base.py), y KeyError/TypeError
                    # porque EventPayload.from_dict()/KafkaOHLCVBar.from_dict()
                    # indexan el dict directamente (p.ej. data["bars"]) sin
                    # .get() — cualquier payload con forma distinta (basura
                    # histórica del topic, formato legado, otro schema)
                    # puede llegar por cualquiera de estas tres rutas.
                    # Se descarta y se cuenta para observabilidad; no se
                    # propaga el error.
                    skipped_unparseable += 1
                    continue
                # CONTRACT R-05b: found_ids ya es la fuente de verdad de
                # "¿este event_id ya fue satisfecho?" (usada por _done() vía
                # issuperset). Sin el guard "not in found_ids", una segunda
                # aparición física del mismo event_id (p.ej. basura histórica
                # residual, o un redelivery legítimo at-least-once si el
                # productor reintenta) se re-agregaba a matched, rompiendo
                # la igualdad estricta que exige el contrato de expected_ids
                # (a lo sumo un ConsumerRecord por event_id esperado).
                if evt.event_id in expected_ids and evt.event_id not in found_ids:
                    found_ids.add(evt.event_id)
                    matched.append(r)

    if skipped_unparseable:
        logger.debug(f"_drain: {skipped_unparseable} mensajes descartados por fallo de deserialización (CONTRACT R-04)")

    # CONTRACT R-05: con expected_ids, el contrato de retorno es "solo los
    # records que matchean" — collected mezclaría basura histórica del
    # topic con los eventos reales de esta ejecución (causa raíz de KAF-001
    # invertida: falso positivo de conteo en vez de falso negativo).
    return matched if expected_ids is not None else collected


# =============================================================================
# API pública del harness
# =============================================================================

__all__ = [
    "BROKER",
    "_DRAIN_TIMEOUT_S",
    "_POLL_TIMEOUT_MS",
    "_unique_group",
    "_raw_producer",
    "_raw_consumer",
    "warm_up_consumer",
    "find_event",
    "_drain",
]
