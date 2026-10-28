# -*- coding: utf-8 -*-
"""
tests/market_data/test_quality_consumer_dataframe.py
=====================================================

Contrato de ``QualityPipelineConsumer._to_dataframe`` frente a polars.

El consumer construye el DataFrame desde ``[c.to_tuple() for c in ...]``
(``list[tuple[int, float, float, float, float, float]]``) con ``schema=``
explícito. Sin ``orient="row"`` polars no puede decidir si la secuencia
externa son filas o columnas y emite ``DataOrientationWarning``.

Este test fija las tres cosas que el fix no debe cambiar: orden de
columnas, dtypes y valores — y añade que no se emita el warning.
"""

from __future__ import annotations

import warnings

import polars as pl
from market_data.application.consumers.quality_consumer import (
    _CANDLE_COLS,
    QualityPipelineConsumer,
)
from market_data.domain.events.ingestion import OHLCVBatchReceived
from market_data.domain.value_objects.candle import Candle
from market_data.domain.value_objects.ohlcv_chunk import OHLCVChunk

_EXPECTED_SCHEMA = {
    "timestamp": pl.Int64,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
}


def _event() -> OHLCVBatchReceived:
    candles = (
        Candle(
            timestamp_ms=1700000000000,
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=1000.0,
        ),
        Candle(
            timestamp_ms=1700003600000,
            open=100.5,
            high=102.0,
            low=100.0,
            close=101.5,
            volume=1500.0,
        ),
    )
    batch = OHLCVChunk(
        exchange="bybit",
        symbol="BTC/USDT",
        timeframe="1h",
        candles=candles,
    )
    return OHLCVBatchReceived(batch=batch)


def test_to_dataframe_preserves_columns_dtypes_and_values() -> None:
    df = QualityPipelineConsumer._to_dataframe(_event())

    assert df.columns == list(_CANDLE_COLS)
    assert dict(df.schema) == _EXPECTED_SCHEMA
    assert df.height == 2
    assert df["timestamp"].to_list() == [1700000000000, 1700003600000]
    assert df["open"].to_list() == [100.0, 100.5]
    assert df["high"].to_list() == [101.0, 102.0]
    assert df["low"].to_list() == [99.0, 100.0]
    assert df["close"].to_list() == [100.5, 101.5]
    assert df["volume"].to_list() == [1000.0, 1500.0]


def test_to_dataframe_does_not_emit_data_orientation_warning() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        QualityPipelineConsumer._to_dataframe(_event())

    orientation = [w for w in caught if "DataOrientationWarning" in str(w.category)]
    assert orientation == []
