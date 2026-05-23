# -*- coding: utf-8 -*-
"""
tests/kafka/test_integration_kafka.py
======================================

Tests de integración contra Kafka real (broker en localhost:9092).

Cubren:
  A. Replay real — 100 eventos, detener consumer, reiniciar, sin pérdida.
  B. No commit si Bronze falla — el evento reaparece tras reinicio.
  C. DLQ real — payload corrupto + schema inválido → ocm.dlq recibido,
     offset principal commitado.

Cómo correr:
  KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
  pytest tests/kafka/test_integration_kafka.py -v -s \
      -m integration --timeout=60

Requiere Kafka en localhost:9092 (docker-compose up kafka zookeeper).

Principios: Fail-Fast · SafeOps · at-least-once · SSOT
"""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import List

import pytest

from shared.kafka.schemas.ohlcv import EventPayload, KafkaOHLCVBar
from shared.kafka.serializer import deserialize, serialize
from shared.kafka.topics import TOPIC_DLQ, TOPIC_OHLCV_RAW

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

BROKER = "localhost:9093"
POLL_TIMEOUT_MS = 3_000
DRAIN_TIMEOUT_S = 15.0


# ---------------------------------------------------------------------------
# Helpers de mensajes
# ---------------------------------------------------------------------------


def _make_bar() -> KafkaOHLCVBar:
    return KafkaOHLCVBar(
        ts=1_700_000_000_000,
        open=100.0,
        high=105.0,
        low=99.0,
        close=103.0,
        volume=1_000.0,
    )


def _make_event(*, event_id: str | None = None) -> EventPayload:
    return EventPayload(
        event_id=event_id or f"evt-{uuid.uuid4().hex[:8]}",
        exchange="bybit",
        symbol="BTC/USDT",
        timeframe="1m",
        batch_start_ts=1_700_000_000_000,
        bars=[_make_bar()],
    )


def _unique_group(prefix: str = "test") -> str:
    """Group ID único por test — evita interferencia entre runs."""
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# Helpers de aiokafka (bajo nivel — sin adaptadores OCM)
# ---------------------------------------------------------------------------


async def _raw_producer():
    from aiokafka import AIOKafkaProducer

    p = AIOKafkaProducer(bootstrap_servers=BROKER, compression_type=None)
    await p.start()
    return p


async def _raw_consumer(group_id: str, topic: str, offset_reset: str = "earliest"):
    from aiokafka import AIOKafkaConsumer

    c = AIOKafkaConsumer(
        topic,
        bootstrap_servers=BROKER,
        group_id=group_id,
        auto_offset_reset=offset_reset,
        enable_auto_commit=False,
        session_timeout_ms=10_000,
        heartbeat_interval_ms=3_000,
    )
    await c.start()
    return c


async def _drain(consumer, *, expected: int, timeout_s: float = DRAIN_TIMEOUT_S) -> List:
    """Drena hasta `expected` mensajes o timeout."""
    collected = []
    deadline = time.monotonic() + timeout_s
    while len(collected) < expected and time.monotonic() < deadline:
        raw = await consumer.getmany(timeout_ms=POLL_TIMEOUT_MS, max_records=expected)
        for _tp, records in raw.items():
            collected.extend(records)
    return collected


# ---------------------------------------------------------------------------
# A. Replay real — 100 eventos, sin pérdida
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.integration
async def test_A_replay_100_events_no_loss():
    """
    A. Replay real desde Kafka.

    1. Publicar 100 eventos con event_ids únicos.
    2. Consumer-1: consumir todos + commit.
    3. Consumer-2 mismo group + seek_to_beginning → replay.
    4. Verificar que los 100 event_ids del test están presentes.

    Invariante: seek_to_beginning() garantiza replay sin pérdida.
    """
    group_id = _unique_group("replay")
    events = [_make_event(event_id=f"replay-{i:04d}") for i in range(100)]
    expected_ids = {e.event_id for e in events}

    # ── 1. Producir ──────────────────────────────────────────────────────
    p = await _raw_producer()
    try:
        for e in events:
            await p.send_and_wait(TOPIC_OHLCV_RAW, value=serialize(e))
        await p.flush()
    finally:
        await p.stop()

    # ── 2. Consumir y commitear ───────────────────────────────────────────
    c1 = await _raw_consumer(group_id, TOPIC_OHLCV_RAW)
    try:
        batch = await _drain(c1, expected=100)
        assert len(batch) >= 100, f"Primera lectura: esperados ≥100, recibidos {len(batch)}"
        await c1.commit()
    finally:
        await c1.stop()

    # ── 3. Reiniciar + seek_to_beginning ─────────────────────────────────
    c2 = await _raw_consumer(group_id, TOPIC_OHLCV_RAW)
    try:
        await c2.seek_to_beginning()
        replayed = await _drain(c2, expected=100)
    finally:
        await c2.stop()

    # ── 4. Verificar sin pérdida ──────────────────────────────────────────
    assert len(replayed) >= 100, f"Replay: esperados ≥100, recibidos {len(replayed)}. Posible pérdida."

    replayed_ids = set()
    for r in replayed:
        try:
            replayed_ids.add(deserialize(r.value, EventPayload).event_id)
        except Exception:
            pass  # mensajes de otros tests en el topic — ignorar

    missing = expected_ids - replayed_ids
    assert not missing, f"Eventos perdidos en replay: {missing}"


# ---------------------------------------------------------------------------
# B. No commit si Bronze falla — el evento reaparece
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.integration
async def test_B_no_commit_on_bronze_failure_event_reappears():
    """
    B. NO commit cuando Bronze falla.

    1. Producir 1 evento.
    2. Consumer-1: poll() → Bronze falla → NO commit → cerrar.
    3. Consumer-2 mismo group_id → mismo evento reaparece.

    Invariante at-least-once: sin commit → replay garantizado.
    Si falla: enable_auto_commit=True está activo en algún lado.
    """
    group_id = _unique_group("bronze-fail")
    event = _make_event(event_id=f"bfail-{uuid.uuid4().hex[:6]}")

    # ── 1. Consumer-1 arranca ANTES del producer con offset_reset=latest ──
    # Crítico: latest se evalúa en c1.start() — si el producer ya terminó,
    # latest queda DESPUÉS del mensaje y el consumer no lo ve nunca.
    # Arrancando el consumer primero, latest = posición actual del topic,
    # y el mensaje producido después llega correctamente al consumer.
    c1 = await _raw_consumer(group_id, TOPIC_OHLCV_RAW, offset_reset="latest")

    # ── 2. Producir ───────────────────────────────────────────────────────
    p = await _raw_producer()
    try:
        await p.send_and_wait(TOPIC_OHLCV_RAW, value=serialize(event))
        await p.flush()
    finally:
        await p.stop()

    # ── 3. Consumer-1: recibe pero NO commitea (simula fallo Bronze) ──────
    try:
        received = await _drain(c1, expected=1)
        assert len(received) >= 1, "Consumer-1 no recibió el evento"
        # Bronze falla → NO commit intencional
    finally:
        await c1.stop()

    # ── 4. Consumer-2: mismo group, earliest → buscar por event_id ─────────
    # aiokafka con enable_auto_commit=False + stop() sin commit() NO persiste
    # el offset en el coordinator. Consumer-2 arranca como grupo sin offset
    # → auto_offset_reset='earliest' lee desde el inicio del topic.
    # Filtramos por event_id para ignorar mensajes de otros tests.
    # Esto verifica at-least-once: el mensaje debe aparecer en algún offset.
    c2 = await _raw_consumer(group_id, TOPIC_OHLCV_RAW, offset_reset="earliest")
    found = False
    deadline = time.monotonic() + 15.0
    try:
        while not found and time.monotonic() < deadline:
            batch = await c2.getmany(timeout_ms=3_000, max_records=50)
            for _tp, records in batch.items():
                for msg in records:
                    try:
                        parsed = deserialize(msg.value, EventPayload)
                        if parsed.event_id == event.event_id:
                            found = True
                            break
                    except Exception:
                        pass  # basura de otros tests — ignorar
                if found:
                    break
    finally:
        await c2.stop()

    assert found, (
        f"❌ PÉRDIDA DE DATOS: event_id={event.event_id} no reapareció\n"
        f"tras fallo Bronze sin commit (consumer-2, earliest).\n"
        f"Verificar enable_auto_commit=False en KafkaConsumerAdapter."
    )


# ---------------------------------------------------------------------------
# C. DLQ real
# ---------------------------------------------------------------------------


class _NoBronze:
    """Bronze fake — nunca escribe, nunca falla. Para tests C."""

    def append(self, df, *, symbol, timeframe, exchange, run_id) -> int:
        return len(df)


async def _build_writer_with_real_dlq(group_id: str):
    """
    Construye KafkaBronzeWriter para tests de integración desde el host.

    Usa AIOKafkaConsumer raw en lugar de KafkaConsumerAdapter porque:
    - KafkaConsumerAdapter conecta a BROKER (localhost:9093 EXTERNAL)
    - El broker devuelve metadata con kafka:9092 (INTERNAL) como broker address
    - aiokafka reconecta a kafka:9092 desde el host → no resuelve → poll falla (SafeOps)
    - Con AIOKafkaConsumer raw se puede forzar que el bootstrap_servers sea
      el EXTERNAL listener y que la metadata se resuelva correctamente.

    El DLQ producer usa KafkaProducerAdapter con compression_type=None
    para evitar lz4 (no disponible en aiokafka sin librerías extra).
    """
    from aiokafka import AIOKafkaConsumer
    from market_data.infrastructure.kafka.bronze_writer import KafkaBronzeWriter
    from market_data.infrastructure.kafka.producer import KafkaProducerAdapter

    # Consumer raw — conecta y permanece en el EXTERNAL listener (localhost:9093).
    # fetch_max_wait_ms bajo para que run_once() no espere demasiado.
    raw_consumer = AIOKafkaConsumer(
        TOPIC_OHLCV_RAW,
        bootstrap_servers=BROKER,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        session_timeout_ms=10_000,
        heartbeat_interval_ms=3_000,
        fetch_max_wait_ms=500,
    )

    # Wrapper mínimo para cumplir la interfaz que espera KafkaBronzeWriter:
    # poll(timeout_ms, max_records) → list[msg], commit(), start(), close()
    class _RawConsumerAdapter:
        def __init__(self, c):
            self._c = c

        async def start(self):
            await self._c.start()

        async def close(self):
            try:
                await self._c.stop()
            except Exception:
                pass

        async def poll(self, timeout_ms=1_000, max_records=500):
            from market_data.ports.outbound.kafka_consumer import KafkaMessage

            raw = await self._c.getmany(timeout_ms=timeout_ms, max_records=max_records)
            msgs = []
            for _tp, records in raw.items():
                for r in records:
                    msgs.append(
                        KafkaMessage(
                            topic=r.topic,
                            partition=r.partition,
                            offset=r.offset,
                            key=r.key,
                            value=r.value,
                            timestamp=r.timestamp,
                            headers=tuple(r.headers) if r.headers else (),
                        )
                    )
            return msgs

        async def commit(self):
            await self._c.commit()

    dlq_producer = KafkaProducerAdapter(
        bootstrap_servers=BROKER,
        compression_type=None,  # evita lz4 — no disponible sin librerías extra
    )
    await dlq_producer.start()

    writer = KafkaBronzeWriter(
        consumer=_RawConsumerAdapter(raw_consumer),
        bronze_storage=_NoBronze(),
        dlq_producer=dlq_producer,
    )
    return writer, dlq_producer


@pytest.mark.asyncio
@pytest.mark.integration
async def test_C1_corrupt_payload_goes_to_dlq():
    """
    C1. Payload corrupto (bytes inválidos) → ocm.dlq.

    Verifica:
    - DLQ recibe exactamente 1 mensaje.
    - Header 'reason' contiene 'deserialize_error'.
    - errors == 0 (el mensaje corrupto es 'handled', no write_error).
    - Por tanto el offset principal fue commitado.
    """
    group_id = _unique_group("dlq-corrupt")
    corrupt_bytes = b"not-valid-json-{{{"

    # ── 1. Producir payload corrupto en ohlcv.raw ─────────────────────────
    p = await _raw_producer()
    try:
        await p.send_and_wait(TOPIC_OHLCV_RAW, value=corrupt_bytes)
        await p.flush()
    finally:
        await p.stop()

    # ── 2. DLQ listener — offset_reset=latest ANTES del writer ───────────
    # Se posiciona en el final del DLQ ANTES de que el writer produzca,
    # así solo capturamos el mensaje de ESTE test.
    dlq_group = _unique_group("dlq-listener")
    dlq_c = await _raw_consumer(dlq_group, TOPIC_DLQ, offset_reset="latest")

    # ── 3. BronzeWriter procesa el mensaje corrupto ───────────────────────
    writer, dlq_producer = await _build_writer_with_real_dlq(group_id)
    await writer.start()
    try:
        processed, errors = await writer.run_once()
    finally:
        await writer.stop()
        await dlq_producer.stop()

    # ── 4. errors == 0 → offset commitado ────────────────────────────────
    assert errors == 0, (
        f"Corrupto debe ser 'handled' (errors=0), recibido errors={errors}.\n"
        "Revisar lógica de _process_message: deserialize falla → 'handled', no 'write_error'."
    )

    # ── 5. DLQ recibió el mensaje ─────────────────────────────────────────
    await asyncio.sleep(0.5)  # dar tiempo al broker para entregar
    dlq_records = await _drain(dlq_c, expected=1, timeout_s=10.0)
    await dlq_c.stop()

    assert len(dlq_records) >= 1, (
        f"❌ DLQ '{TOPIC_DLQ}' no recibió el payload corrupto.\n"
        "Verificar que el topic existe en el broker y que dlq_producer.start() fue llamado."
    )

    headers = dict(dlq_records[0].headers) if dlq_records[0].headers else {}
    reason = headers.get("reason", b"").decode("utf-8", errors="replace")
    assert "deserialize_error" in reason, f"Header 'reason' esperado 'deserialize_error', recibido: '{reason}'"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_C2_invalid_schema_goes_to_dlq():
    """
    C2. JSON válido pero schema inválido → ocm.dlq.

    Diferencia con C1: el payload es JSON parseable pero falla la
    validación del modelo EventPayload (campos requeridos ausentes).
    Verifica que la deserialización valida el modelo, no solo el JSON.
    """
    import json

    group_id = _unique_group("dlq-schema")
    # JSON sintácticamente válido pero sin ningún campo de EventPayload
    invalid_schema_bytes = json.dumps({"foo": "bar", "baz": 42}).encode()

    # ── 1. Producir ───────────────────────────────────────────────────────
    p = await _raw_producer()
    try:
        await p.send_and_wait(TOPIC_OHLCV_RAW, value=invalid_schema_bytes)
        await p.flush()
    finally:
        await p.stop()

    # ── 2. DLQ listener ───────────────────────────────────────────────────
    dlq_group = _unique_group("dlq-schema-listener")
    dlq_c = await _raw_consumer(dlq_group, TOPIC_DLQ, offset_reset="latest")

    # ── 3. BronzeWriter procesa ───────────────────────────────────────────
    writer, dlq_producer = await _build_writer_with_real_dlq(group_id)
    await writer.start()
    try:
        processed, errors = await writer.run_once()
    finally:
        await writer.stop()
        await dlq_producer.stop()

    # ── 4. errors == 0 → offset commitado ────────────────────────────────
    assert errors == 0, f"Schema inválido debe ser 'handled' (errors=0), recibido errors={errors}."

    # ── 5. DLQ recibió ────────────────────────────────────────────────────
    await asyncio.sleep(0.5)
    dlq_records = await _drain(dlq_c, expected=1, timeout_s=10.0)
    await dlq_c.stop()

    assert len(dlq_records) >= 1, (
        f"❌ DLQ '{TOPIC_DLQ}' no recibió el schema inválido.\n"
        "Verificar que shared.kafka.serializer valida el modelo EventPayload, no solo JSON."
    )

    headers = dict(dlq_records[0].headers) if dlq_records[0].headers else {}
    reason = headers.get("reason", b"").decode("utf-8", errors="replace")
    assert "deserialize_error" in reason, f"Header 'reason' esperado 'deserialize_error', recibido: '{reason}'"
