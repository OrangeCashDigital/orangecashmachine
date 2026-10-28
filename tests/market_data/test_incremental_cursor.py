# -*- coding: utf-8 -*-
"""
tests/market_data/test_incremental_cursor.py
============================================

Test de regresión para el bug del cursor no persistido (await faltante).

IncrementalStrategy._run llama a ctx.cursor.update() sin await en la línea 134.
Como CursorStorePort.update() es async en la implementación real
(RedisCursorStore / InMemoryCursorStore), la corrutina se descarta y el
cursor nunca se persiste, causando "Primer inicio" en cada ejecución
y descarga completa innecesaria.
"""

from __future__ import annotations

import polars as pl
import pytest
from market_data.application.pipeline.runtime import PipelineContext
from market_data.application.strategies.incremental import IncrementalStrategy
from market_data.domain.policies.base import PairResult, PipelineMode
from market_data.domain.value_objects.candle import Candle
from market_data.domain.value_objects.ohlcv_chunk import OHLCVChunk
from market_data.ports.outbound.publisher_port import NullOHLCVPublisher
from market_data.ports.outbound.quality_pipeline import NullQualityPipeline


class FakeCursor:
    """Cursor fake que registra si update fue awaited."""

    def __init__(self) -> None:
        self.update_was_awaited = False
        self.update_call_count = 0
        self.last_ts_ms: int | None = None

    async def update(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        timestamp_ms: int,
    ) -> bool:
        self.update_was_awaited = True
        self.update_call_count += 1
        self.last_ts_ms = timestamp_ms
        return True


class FakeFetcher:
    """Fetcher fake que retorna un DataFrame con una vela."""

    async def download_data(self, symbol: str, timeframe: str, start_date: str):
        return pl.DataFrame(
            {
                "timestamp_ms": [1700000000000],
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.5],
                "volume": [10.0],
            }
        )


class FakeChunkConverter:
    """Chunk converter fake que retorna un OHLCVChunk con una vela."""

    def to_chunk(
        self,
        df: pl.DataFrame,
        exchange: str,
        symbol: str,
        timeframe: str,
        source: str = "rest",
        run_id: str = "",
        chunk_index: int = 0,
        total_chunks: int | None = None,
    ) -> OHLCVChunk:
        return OHLCVChunk(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            candles=(
                Candle(
                    timestamp_ms=1700000000000,
                    open=100.0,
                    high=101.0,
                    low=99.0,
                    close=100.5,
                    volume=10.0,
                ),
            ),
            source=source,
            run_id=run_id,
        )


@pytest.mark.asyncio
async def test_cursor_update_is_awaited():
    """El cursor.update() debe ser awaited en IncrementalStrategy._run.

    Sin el await (bug actual), la corrutina se descarta y
    cursor.update_was_awaited permanece False.
    Con el await (fix), cursor.update_was_awaited se vuelve True.
    """
    cursor = FakeCursor()
    fetcher = FakeFetcher()
    quality = NullQualityPipeline()
    publisher = NullOHLCVPublisher()
    converter = FakeChunkConverter()

    ctx = PipelineContext(
        fetcher=fetcher,
        cursor=cursor,
        quality=quality,
        exchange_id="bybit",
        market_type="spot",
        start_date="2024-01-01",
        publisher=publisher,
    )
    # _chunk_converter es privado; se inyecta directamente para el test
    ctx._chunk_converter = converter

    result = PairResult(
        symbol="BTC/USDT",
        timeframe="1m",
        mode=PipelineMode.INCREMENTAL,
        exchange_id="bybit",
    )

    await IncrementalStrategy()._run("BTC/USDT", "1m", 0, 1, ctx, result)

    assert cursor.update_was_awaited is True, (
        "BUG: cursor.update() no fue awaited — el cursor nunca se persiste. Añadir 'await' en incremental.py:134."
    )
    assert cursor.update_call_count == 1
    assert cursor.last_ts_ms == 1700000000000
