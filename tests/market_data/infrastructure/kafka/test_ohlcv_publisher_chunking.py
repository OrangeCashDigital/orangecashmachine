# -*- coding: utf-8 -*-
"""
Regresion: KafkaOHLCVPublisher trocea chunks grandes.

aiokafka rechaza localmente todo mensaje > 1 MiB (max_request_size).
Un chunk de 10.080 velas (~1,12 MB) fallaba en silencio. Cada trozo
lleva un event_id determinista para que el reintento no duplique en Bronze.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from loguru import logger
from market_data.domain.value_objects.ohlcv_chunk import OHLCVChunk
from market_data.infrastructure.kafka.ohlcv_publisher import KafkaOHLCVPublisher
from market_data.ports.outbound.publisher_port import PublishResult

_MIB = 1_048_576
_T0 = 1_700_000_000_000
_STEP = 60_000


@dataclass(frozen=True)
class _Candle:
    timestamp_ms: int
    open: float = 97123.45
    high: float = 97150.12
    low: float = 97100.5
    close: float = 97130.77
    volume: float = 12.345678


class _FakeProducer:
    def __init__(self, fail_on_attempt: int | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self._attempts = 0
        self._fail_on_attempt = fail_on_attempt

    async def start(self) -> None:
        pass

    async def produce(self, topic: str, value: bytes, key: Any = None, headers: Any = None) -> None:
        self._attempts += 1
        if self._attempts == self._fail_on_attempt:
            raise RuntimeError("boom")
        self.calls.append({"topic": topic, "value": value, "key": key})


def _chunk(n: int, run_id: str = "run-1") -> OHLCVChunk:
    candles = tuple(_Candle(timestamp_ms=_T0 + i * _STEP) for i in range(n))
    return OHLCVChunk(
        exchange="bybit",
        symbol="BTC/USDT",
        timeframe="1m",
        candles=candles,
        source="rest",
        run_id=run_id,
    )


def _decode(fake: _FakeProducer) -> list[dict[str, Any]]:
    return [json.loads(c["value"]) for c in fake.calls]


async def test_large_chunk_is_split_below_1mib() -> None:
    fake = _FakeProducer()
    result = await KafkaOHLCVPublisher(fake).publish_chunk(_chunk(10_080))
    assert result == PublishResult.SUCCESS
    assert len(fake.calls) == 21  # ceil(10080 / 500)
    assert all(len(c["value"]) < _MIB for c in fake.calls)
    assert sum(len(d["bars"]) for d in _decode(fake)) == 10_080


async def test_each_piece_starts_at_its_first_candle() -> None:
    fake = _FakeProducer()
    await KafkaOHLCVPublisher(fake).publish_chunk(_chunk(10_080))
    expected = [_T0 + off * _STEP for off in range(0, 10_080, 500)]
    assert [d["batch_start_ts"] for d in _decode(fake)] == expected


async def test_event_ids_are_deterministic_across_retries() -> None:
    first, second = _FakeProducer(), _FakeProducer()
    await KafkaOHLCVPublisher(first).publish_chunk(_chunk(1_200, run_id="a"))
    await KafkaOHLCVPublisher(second).publish_chunk(_chunk(1_200, run_id="b"))
    ids_a = [d["event_id"] for d in _decode(first)]
    ids_b = [d["event_id"] for d in _decode(second)]
    assert ids_a == ids_b
    assert len(set(ids_a)) == len(ids_a)  # distintos entre trozos


async def test_failure_midway_is_retryable_and_logged() -> None:
    fake = _FakeProducer(fail_on_attempt=3)
    msgs: list[str] = []
    sink_id = logger.add(lambda m: msgs.append(str(m)), level="ERROR")
    try:
        result = await KafkaOHLCVPublisher(fake).publish_chunk(_chunk(10_080))
    finally:
        logger.remove(sink_id)
    assert result == PublishResult.RETRYABLE_FAILURE
    assert len(fake.calls) == 2
    assert any("ohlcv_publish_failed" in m and "bars=10080" in m for m in msgs)
