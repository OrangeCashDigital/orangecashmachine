# -*- coding: utf-8 -*-
"""
tests/kafka/test_integration_kafka.py
======================================

Tests de integración contra Kafka real (broker en localhost:9093).

Cubren:
  A. Replay real — 100 eventos, seek_to_beginning, sin pérdida.
  B. No commit si Bronze falla — el evento reaparece tras reinicio.
  C. DLQ real — payload corrupto + schema inválido → ocm.dlq.

Cómo correr (requiere Kafka en :9093):
  pytest tests/kafka/test_integration_kafka.py -v -s -m integration

Contrato: tests/kafka/CONTRACT.md
Harness:  tests/kafka/conftest.py

Principios: Fail-Fast · SafeOps · at-least-once · SSOT
"""

from __future__ import annotations

import asyncio
import uuid

import pytest

from shared.kafka.schemas.ohlcv import EventPayload, KafkaOHLCVBar
from shared.kafka.serializer import deserialize, serialize
from shared.kafka.topics import TOPIC_DLQ, TOPIC_OHLCV_RAW
from tests.kafka.conftest import (
    BROKER,
    _drain,
    _raw_consumer,
    _raw_producer,
    _unique_group,
    find_event,
    warm_up_consumer,
)

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


# ---------------------------------------------------------------------------
# A. Replay real — 100 eventos, sin pérdida
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.integration
async def test_A_replay_100_events_no_loss():
    """
    A. Replay real desde Kafka.

    1. Producir 100 eventos con event_ids únicos.
    2. Consumer-1: consumir todos + commit.
    3. Consumer-2 mismo group + seek_to_beginning → replay.
    4. Verificar que los 100 event_ids producidos están presentes.

    Invariante: seek_to_beginning() garantiza replay sin pérdida.
    El assert verifica por presencia en el set — CONTRACT R-03.
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

    # ── 3. Reiniciar + seek_to_beginning (replay explícito) ───────────────
    c2 = await _raw_consumer(group_id, TOPIC_OHLCV_RAW)
    try:
        await c2.seek_to_beginning()
        # CONTRACT R-05 (fix KAF-001): expected_ids evita que _drain() se
        # detenga tras consumir basura histórica del topic antes de
        # alcanzar los 100 eventos de esta ejecución.
        replayed = await _drain(c2, expected=100, expected_ids=expected_ids)
    finally:
        await c2.stop()

    assert len(replayed) >= 100, f"Replay: esperados ≥100, recibidos {len(replayed)}. Posible pérdida."


@pytest.mark.asyncio
@pytest.mark.integration
async def test_R05_drain_filters_historical_garbage_by_expected_ids():
    """
    R-05 regression guard (H-25, fix KAF-001).

    _drain(expected_ids=...) debe retornar EXACTAMENTE los records cuyo
    event_id está en expected_ids — nunca contaminado con mensajes
    ajenos del topic (KAFKA_LOG_RETENTION_HOURS=168 permite basura
    histórica de ejecuciones previas).

    A diferencia de test_A_replay_100_events_no_loss (que solo verifica
    len(replayed) >= 100, insensible a contaminación por diseño), este
    test usa igualdad estricta: produce "ruido" con event_ids ajenos
    ANTES de los eventos reales, y confirma que el ruido no aparece en
    el resultado — ni por conteo ni por identidad.
    """
    group_id = _unique_group("r05-guard")

    # CONTRACT R-05c: event_id debe ser único POR EJECUCIÓN, no solo por
    # test. IDs deterministas (sin run_id) colisionan con basura histórica
    # de corridas anteriores si el topic no se purga entre ellas — causa
    # raíz real de KAF-001 (no era topología ni _drain fabricando records).
    run_id = uuid.uuid4().hex[:8]
    noise = [_make_event(event_id=f"r05-noise-{i:04d}-{run_id}") for i in range(20)]
    real_events = [_make_event(event_id=f"r05-real-{i:04d}-{run_id}") for i in range(5)]
    expected_ids = {e.event_id for e in real_events}

    # ── 1. Producir ruido + eventos reales, en ese orden ──────────────────
    p = await _raw_producer()
    try:
        for e in noise + real_events:
            await p.send_and_wait(TOPIC_OHLCV_RAW, value=serialize(e))
        await p.flush()
    finally:
        await p.stop()

    # ── 2. Drenar filtrando por expected_ids ───────────────────────────────
    c = await _raw_consumer(group_id, TOPIC_OHLCV_RAW)
    try:
        await c.seek_to_beginning()
        result = await _drain(c, expected=len(real_events), expected_ids=expected_ids)
    finally:
        await c.stop()

    # ── 3. Igualdad estricta: ni de más (ruido) ni de menos (pérdida) ──────
    result_ids = sorted(deserialize(r.value, EventPayload).event_id for r in result)
    print(f"\nDEBUG R05: expected_ids={sorted(expected_ids)}")
    print(f"DEBUG R05: result_ids={result_ids}")
    print(f"DEBUG R05: duplicates={[x for x in result_ids if result_ids.count(x) > 1]}")
    print(
        f"DEBUG R05: offsets={[(deserialize(r.value, EventPayload).event_id, r.offset, r.partition) for r in result]}"
    )

    assert len(result) == len(real_events), (
        f"_drain con expected_ids debe retornar exactamente "
        f"{len(real_events)} records, recibidos {len(result)}. "
        f"Si es mayor: contaminación con ruido histórico (regresión "
        f"del bug original de H-25 — return collected en vez de matched). "
        f"Si es menor: pérdida real de eventos."
    )

    # CONTRACT R-03: verificar por presencia en set, no por posición.
    # NOTA: `result` ya es exactamente el conjunto matcheado por _drain()
    # (CONTRACT R-05, ver conftest.py) — este bloque es redundante con la
    # igualdad estricta ya afirmada arriba, pero se conserva como doble
    # verificación explícita por presencia (no por conteo).
    # FIX: `replayed` no existía en este scope (NameError, F821) — la
    # variable correcta es `result`, la que retorna _drain().
    replayed_ids = set()
    for r in result:
        try:
            replayed_ids.add(deserialize(r.value, EventPayload).event_id)
        except Exception:
            pass

    missing = expected_ids - replayed_ids
    assert not missing, f"Eventos perdidos en replay: {missing}"


# ---------------------------------------------------------------------------
# B. No commit si Bronze falla — el evento reaparece
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.integration
async def test_B_no_commit_on_bronze_failure_event_reappears():
    """
    B. at-least-once: sin commit → redelivery garantizado.

    1. Consumer-1 arranca con latest + warm_up (R-02) → partition assignment completo.
    2. Producir 1 evento.
    3. Consumer-1: recibe el evento, NO commitea (simula fallo Bronze), cierra.
    4. Consumer-2 (mismo group, earliest): busca el evento por event_id (R-03, R-04).

    Invariante: sin commit() el Kafka Group Coordinator no tiene offset para
    el grupo → consumer-2 con earliest lee desde el inicio → encuentra el evento.

    Si falla: verificar enable_auto_commit=False en KafkaConsumerAdapter.
    """
    group_id = _unique_group("bronze-fail")
    event = _make_event(event_id=f"bfail-{uuid.uuid4().hex[:6]}")

    # ── 1. Consumer-1 arranca ANTES de producir (R-02) ───────────────────
    # latest: solo queremos el mensaje que vamos a producir ahora, no basura histórica.
    # warm_up_consumer: completa el partition assignment antes de producir.
    # Si se produce antes del warm-up, el mensaje llega antes de que el consumer
    # tenga las particiones asignadas y se pierde para offset_reset=latest.
    c1 = await _raw_consumer(group_id, TOPIC_OHLCV_RAW, offset_reset="latest")
    await warm_up_consumer(c1)

    # ── 2. Producir ───────────────────────────────────────────────────────
    p = await _raw_producer()
    try:
        await p.send_and_wait(TOPIC_OHLCV_RAW, value=serialize(event))
        await p.flush()
    finally:
        await p.stop()

    # ── 3. Consumer-1: recibe pero NO commitea (simula fallo Bronze) ──────
    found_c1 = await find_event(c1, event.event_id, timeout_s=15.0)
    await c1.stop()
    assert found_c1, (
        f"Consumer-1 no recibió event_id={event.event_id} tras producir. "
        f"Verificar warm_up_consumer() y timing del producer."
    )

    # ── 4. Consumer-2: mismo group, earliest → redelivery ─────────────────
    # Sin commit() en consumer-1, el coordinator no tiene offset para el grupo.
    # earliest → lee desde el inicio del topic → encuentra el evento entre la
    # basura histórica. find_event() filtra por event_id — CONTRACT R-03, R-04.
    c2 = await _raw_consumer(group_id, TOPIC_OHLCV_RAW, offset_reset="earliest")
    found_c2 = await find_event(c2, event.event_id, timeout_s=15.0)
    await c2.stop()

    assert found_c2, (
        f"❌ PÉRDIDA DE DATOS: event_id={event.event_id} no reapareció\n"
        f"tras fallo Bronze sin commit (consumer-2, earliest).\n"
        f"Verificar enable_auto_commit=False en KafkaConsumerAdapter."
    )


# ---------------------------------------------------------------------------
# C. DLQ real
# ---------------------------------------------------------------------------


class _NoBronze:
    """Bronze stub — acepta todo, no escribe. Para tests C."""

    def append(self, df, *, symbol, timeframe, exchange, run_id) -> int:
        return len(df)


async def _build_writer_with_real_dlq(group_id: str):
    """
    Construye KafkaBronzeWriter con consumer raw y DLQ producer reales.

    Usa AIOKafkaConsumer raw porque KafkaConsumerAdapter reconecta a
    kafka:9092 (INTERNAL) que no resuelve desde el host. El raw consumer
    permanece en el EXTERNAL listener (localhost:9093).
    """
    from aiokafka import AIOKafkaConsumer
    from market_data.infrastructure.kafka.bronze_writer import KafkaBronzeWriter
    from market_data.infrastructure.kafka.producer import KafkaProducerAdapter
    from market_data.ports.outbound.kafka_consumer import KafkaMessage

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

        async def poll(self, timeout_ms: int = 1_000, max_records: int = 500):
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
        compression_type=None,
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
    - Header 'reason' contiene 'deserialize_error'.
    - errors == 0 (el mensaje corrupto es 'handled', no 'write_error').
    - El offset principal fue commitado (el consumer no se atasca).

    DLQ listener arranca con latest ANTES del writer (R-02):
    solo capturamos mensajes de ESTE test.
    """
    group_id = _unique_group("dlq-corrupt")
    corrupt_bytes = b"not-valid-json-{{{"

    # ── 1. Producir payload corrupto ─────────────────────────────────────
    p = await _raw_producer()
    try:
        await p.send_and_wait(TOPIC_OHLCV_RAW, value=corrupt_bytes)
        await p.flush()
    finally:
        await p.stop()

    # ── 2. DLQ listener con latest + warm_up ANTES del writer (R-02) ─────
    dlq_group = _unique_group("dlq-listener")
    dlq_c = await _raw_consumer(dlq_group, TOPIC_DLQ, offset_reset="latest")
    await warm_up_consumer(dlq_c)

    # ── 3. BronzeWriter procesa el mensaje corrupto ───────────────────────
    writer, dlq_producer = await _build_writer_with_real_dlq(group_id)
    await writer.start()
    try:
        processed, errors = await writer.run_once()
    finally:
        await writer.stop()
        await dlq_producer.stop()

    # ── 4. errors == 0 → offset commitado (fail-fast) ────────────────────
    assert errors == 0, (
        f"Corrupto debe ser 'handled' (errors=0), recibido errors={errors}.\n"
        "Revisar _process_message: deserialize falla → 'handled', no 'write_error'."
    )

    # ── 5. DLQ recibió el mensaje — verificar header 'reason' ────────────
    await asyncio.sleep(0.5)  # dar tiempo al broker para entregar al DLQ consumer
    dlq_records = await _drain(dlq_c, expected=1, timeout_s=10.0)
    await dlq_c.stop()

    assert len(dlq_records) >= 1, (
        f"❌ DLQ '{TOPIC_DLQ}' no recibió el payload corrupto.\n"
        "Verificar que el topic existe y que dlq_producer.start() fue llamado."
    )

    headers = dict(dlq_records[0].headers) if dlq_records[0].headers else {}
    reason = headers.get("reason", b"").decode("utf-8", errors="replace")
    assert "deserialize_error" in reason, f"Header 'reason' esperado 'deserialize_error', recibido: '{reason}'"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_C2_invalid_schema_goes_to_dlq():
    """
    C2. JSON válido pero schema inválido → ocm.dlq.

    Diferencia con C1: el payload parsea como JSON pero falla la
    validación de EventPayload (campos requeridos ausentes).
    Verifica que el deserializer valida el modelo, no solo el JSON.
    """
    import json

    group_id = _unique_group("dlq-schema")
    invalid_schema_bytes = json.dumps({"foo": "bar", "baz": 42}).encode()

    # ── 1. Producir ───────────────────────────────────────────────────────
    p = await _raw_producer()
    try:
        await p.send_and_wait(TOPIC_OHLCV_RAW, value=invalid_schema_bytes)
        await p.flush()
    finally:
        await p.stop()

    # ── 2. DLQ listener con latest + warm_up ANTES del writer (R-02) ─────
    dlq_group = _unique_group("dlq-schema-listener")
    dlq_c = await _raw_consumer(dlq_group, TOPIC_DLQ, offset_reset="latest")
    await warm_up_consumer(dlq_c)

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

    # ── 5. DLQ recibió el mensaje ─────────────────────────────────────────
    await asyncio.sleep(0.5)
    dlq_records = await _drain(dlq_c, expected=1, timeout_s=10.0)
    await dlq_c.stop()

    assert len(dlq_records) >= 1, (
        f"❌ DLQ '{TOPIC_DLQ}' no recibió el schema inválido.\n"
        "Verificar que shared.kafka.serializer valida el modelo EventPayload."
    )

    headers = dict(dlq_records[0].headers) if dlq_records[0].headers else {}
    reason = headers.get("reason", b"").decode("utf-8", errors="replace")
    assert "deserialize_error" in reason, f"Header 'reason' esperado 'deserialize_error', recibido: '{reason}'"
