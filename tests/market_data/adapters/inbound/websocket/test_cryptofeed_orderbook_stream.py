"""
tests/market_data/adapters/inbound/websocket/test_cryptofeed_orderbook_stream.py
===================================================================================
Regresion para CryptofeedOrderBookStream._translate_and_dispatch.
Comportamientos confirmados empiricamente contra Bybit WS real (2026-07-30):
  1. Firma del callback: (book, receipt_timestamp) -- 2 posicionales.
  2. book.delta is None -> snapshot; dict -> delta incremental.
  3. book.timestamp para Bybit ya viene en milisegundos (int); no multiplicar.
     receipt_timestamp es segundos (float); si se usa de fallback, si multiplicar.
  4. size == "0" en un delta se pasa tal cual, el dominio decide el borrado.
"""

from __future__ import annotations

import datetime
from unittest.mock import AsyncMock

import pytest
from market_data.adapters.inbound.websocket.cryptofeed_orderbook_stream import (
    CryptofeedOrderBookStream,
)


class _FakeOrderBook:
    def __init__(self, symbol, delta, timestamp, book_dict=None, checksum=None):
        self.symbol = symbol
        self.delta = delta
        self.timestamp = timestamp
        self.checksum = checksum
        self._book_dict = book_dict or {"bid": {}, "ask": {}}
        self.book = self

    def to_dict(self):
        return self._book_dict


class _NullLogger:
    def bind(self, **kwargs):
        return self

    def warning(self, *args, **kwargs):
        pass


@pytest.fixture
def stream():
    instance = CryptofeedOrderBookStream.__new__(CryptofeedOrderBookStream)
    instance._exchange = "bybit"
    instance._max_depth = 50
    instance._on_snapshot = AsyncMock()
    instance._on_delta = AsyncMock()
    instance._log = _NullLogger()
    return instance


class TestCallbackSignature:
    @pytest.mark.asyncio
    async def test_accepts_exactly_two_positional_args(self, stream):
        book = _FakeOrderBook("BTC-USDT", None, 1785453668316)
        await stream._translate_and_dispatch(book, 1785453668.822)
        stream._on_snapshot.assert_awaited_once()


class TestSnapshotVsDelta:
    @pytest.mark.asyncio
    async def test_delta_none_dispatches_snapshot(self, stream):
        book = _FakeOrderBook(
            "BTC-USDT",
            None,
            1785453668316,
            book_dict={"bid": {"64900.0": "1.5"}, "ask": {"64901.0": "2.0"}},
        )
        await stream._translate_and_dispatch(book, 1785453668.822)
        stream._on_snapshot.assert_awaited_once()
        stream._on_delta.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_delta_dict_dispatches_delta(self, stream):
        book = _FakeOrderBook(
            "BTC-USDT",
            {"bid": [["64908.5", "0"]], "ask": []},
            1785453668516,
        )
        await stream._translate_and_dispatch(book, 1785453668.824)
        stream._on_snapshot.assert_not_awaited()
        stream._on_delta.assert_awaited_once()


class TestTimestampUnits:
    @pytest.mark.asyncio
    async def test_exchange_timestamp_is_not_multiplied_again(self, stream):
        raw_exchange_ts_ms = 1785453668316
        book = _FakeOrderBook(
            "BTC-USDT",
            None,
            raw_exchange_ts_ms,
            book_dict={"bid": {}, "ask": {}},
        )
        await stream._translate_and_dispatch(book, 1785453668.822)

        args, kwargs = stream._on_snapshot.call_args
        timestamp_ms = kwargs["timestamp_ms"]

        dt = datetime.datetime.fromtimestamp(timestamp_ms / 1000, tz=datetime.timezone.utc)
        assert dt.year == 2026
        assert timestamp_ms == raw_exchange_ts_ms

    @pytest.mark.asyncio
    async def test_receipt_timestamp_fallback_is_multiplied(self, stream):
        receipt_ts_seconds = 1785453668.822
        book = _FakeOrderBook("BTC-USDT", None, None)
        await stream._translate_and_dispatch(book, receipt_ts_seconds)

        args, kwargs = stream._on_snapshot.call_args
        timestamp_ms = kwargs["timestamp_ms"]
        assert timestamp_ms == int(receipt_ts_seconds * 1000)


class TestDeltaZeroSizeMeansRemoval:
    @pytest.mark.asyncio
    async def test_zero_size_level_passed_through_unmodified(self, stream):
        book = _FakeOrderBook(
            "BTC-USDT",
            {"bid": [["64908.5", "0"]], "ask": []},
            1785453668516,
        )
        await stream._translate_and_dispatch(book, 1785453668.824)

        args, kwargs = stream._on_delta.call_args
        assert kwargs["price"] == "64908.5"
        assert kwargs["size"] == "0"
        assert kwargs["side"] == "bid"


class TestDispatchFailSoft:
    @pytest.mark.asyncio
    async def test_snapshot_dispatch_error_does_not_raise(self, stream):
        stream._on_snapshot.side_effect = RuntimeError("boom")
        book = _FakeOrderBook("BTC-USDT", None, 1785453668316)
        await stream._translate_and_dispatch(book, 1785453668.822)
