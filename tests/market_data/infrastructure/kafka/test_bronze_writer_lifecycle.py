"""Regression tests PO-03: el writer cierra DLQ y propaga CancelledError."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from market_data.infrastructure.kafka.bronze_writer import KafkaBronzeWriter


def _make_writer():
    consumer = MagicMock()
    consumer.start = AsyncMock()
    consumer.close = AsyncMock()
    consumer.poll = AsyncMock(return_value=[])
    bronze = MagicMock()
    dlq = MagicMock()
    dlq.stop = AsyncMock()
    writer = KafkaBronzeWriter(
        consumer=consumer,
        bronze_storage=bronze,
        dlq_producer=dlq,
    )
    return writer


@pytest.mark.asyncio
async def test_stop_closes_consumer_and_dlq():
    writer = _make_writer()
    await writer.stop()
    writer._consumer.close.assert_awaited_once()
    writer._dlq.stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_stop_tolerates_dlq_close_failure():
    writer = _make_writer()
    writer._dlq.stop = AsyncMock(side_effect=RuntimeError("boom"))
    await writer.stop()  # no debe levantar


@pytest.mark.asyncio
async def test_stop_without_dlq_does_not_fail():
    writer = _make_writer()
    writer._dlq = None
    await writer.stop()
    writer._consumer.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_cancelled_error_propagates_to_caller():
    """Al cancelar run(), CancelledError debe propagarse (no absorberse)."""
    writer = _make_writer()

    async def _boom():
        raise asyncio.CancelledError()

    writer._run_once = _boom
    writer._running = True

    task = asyncio.create_task(writer.run())
    with pytest.raises(asyncio.CancelledError):
        await task
