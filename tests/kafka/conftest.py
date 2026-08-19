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
# drain_until_ids_found — drenar hasta encontrar un set de event_ids
# =============================================================================


async def drain_until_ids_found(
    consumer: AIOKafkaConsumer,
    target_ids: set[str],
    *,
    timeout_s: float = _DRAIN_TIMEOUT_S,
) -> set[str]:
    """
    Lee hasta encontrar todos los target_ids o timeout.

    A diferencia de _drain() — que para en cuanto junta N mensajes
    cualquiera — esta función continúa leyendo hasta que los IDs
    encontrados cubren target_ids, filtrando por event_id real.
    Necesaria cuando el topic contiene basura histórica *parseable*
    (mismo schema, otros event_ids de corridas previas) que agotaría
    la cuota de _drain() antes de llegar a los mensajes objetivo.

    Root cause documentado: docs/audits/2026-08-18-kafka-topology-audit.md

    CONTRACT R-03: verificar por presencia en set, no por posición.
    CONTRACT R-04: ignorar mensajes no parseables.

    Returns:
        Set de event_ids encontrados (subset de target_ids si hay timeout).
    """
    from shared.kafka.schemas.ohlcv import EventPayload
    from shared.kafka.serializer import deserialize

    found: set[str] = set()
    deadline = time.monotonic() + timeout_s
    while found < target_ids and time.monotonic() < deadline:
        raw = await consumer.getmany(timeout_ms=_POLL_TIMEOUT_MS, max_records=200)
        for _tp, records in raw.items():
            for r in records:
                try:
                    evt = deserialize(r.value, EventPayload)
                    if evt.event_id in target_ids:
                        found.add(evt.event_id)
                except Exception:
                    # CONTRACT R-04: ignorar mensajes no parseables.
                    pass
    return found


# =============================================================================
# _drain — drenar hasta N mensajes o timeout
# =============================================================================


async def _drain(
    consumer: AIOKafkaConsumer,
    *,
    expected: int,
    timeout_s: float = _DRAIN_TIMEOUT_S,
) -> List:
    """
    Drena hasta `expected` mensajes o timeout.

    USO RESTRINGIDO: solo para test_A donde se necesita contar el total
    de mensajes recibidos (100 eventos de replay). Para verificar un
    mensaje específico, usar find_event() — CONTRACT R-03.

    Returns:
        Lista de ConsumerRecord recibidos (puede ser < expected si hay timeout).
    """
    collected: List = []
    deadline = time.monotonic() + timeout_s
    while len(collected) < expected and time.monotonic() < deadline:
        raw = await consumer.getmany(
            timeout_ms=_POLL_TIMEOUT_MS,
            max_records=expected,
        )
        for _tp, records in raw.items():
            collected.extend(records)
    return collected


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
    "drain_until_ids_found",
]
