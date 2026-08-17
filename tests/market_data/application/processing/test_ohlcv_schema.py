"""Tests de regresión para ohlcv_schema.py — cubren el bug de migración a
pandera.polars donde los checks recibían PolarsData en vez de pl.DataFrame."""

from datetime import datetime, timezone

import polars as pl
import pytest
from market_data.application.processing.ohlcv_schema import validate_ohlcv
from pandera.errors import SchemaError, SchemaErrors


def _base_df(timestamps, open_=None, high=None, low=None, close=None, volume=None):
    n = len(timestamps)
    return pl.DataFrame(
        {
            "timestamp": timestamps,
            "open": open_ or [100.0] * n,
            "high": high or [102.0] * n,
            "low": low or [99.0] * n,
            "close": close or [101.0] * n,
            "volume": volume or [10.0] * n,
        }
    ).with_columns(pl.col("timestamp").dt.cast_time_unit("us"))


def test_valid_ohlcv_passes():
    ts = [datetime(2026, 1, 1, tzinfo=timezone.utc), datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc)]
    df = _base_df(ts, open_=[100.0, 101.0], high=[102.0, 103.0], low=[99.0, 100.0], close=[101.0, 102.0])
    validated = validate_ohlcv(df, timeframe="1m")
    assert len(validated) == 2


def test_timestamp_before_market_origin_rejected():
    ts = [datetime(2000, 1, 1, tzinfo=timezone.utc), datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc)]
    df = _base_df(ts)
    with pytest.raises(SchemaErrors, match="market origin"):
        validate_ohlcv(df, timeframe="1m")


def test_ohlc_relationship_violation_rejected():
    ts = [datetime(2026, 1, 1, tzinfo=timezone.utc)]
    # low > high: inválido
    df = _base_df(ts, low=[200.0], high=[102.0])
    with pytest.raises(SchemaErrors, match="OHLC relationship"):
        validate_ohlcv(df, timeframe="1m")


def test_non_monotonic_timestamps_rejected():
    ts = [datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc), datetime(2026, 1, 1, tzinfo=timezone.utc)]
    df = _base_df(ts)
    with pytest.raises(SchemaErrors, match="monotonically"):
        validate_ohlcv(df, timeframe="1m")


def test_duplicate_timestamps_rejected():
    ts_val = datetime(2026, 1, 1, tzinfo=timezone.utc)
    df = _base_df([ts_val, ts_val])
    with pytest.raises(SchemaErrors, match="Duplicate timestamps"):
        validate_ohlcv(df, timeframe="1m")


def test_ohlc_relationship_open_close_out_of_range_rejected():
    """Cubre línea 113: violación donde open queda fuera de [low, high]."""
    ts = [datetime(2026, 1, 1, tzinfo=timezone.utc)]
    df = _base_df(ts, open_=[500.0], high=[102.0], low=[99.0], close=[101.0])
    with pytest.raises(SchemaErrors, match="OHLC relationship"):
        validate_ohlcv(df, timeframe="1m")


def test_grid_alignment_rejects_misaligned_timestamps():
    """Cubre el bloque inline de grid alignment en validate_ohlcv (308-313)."""
    ts = [
        datetime(2026, 1, 1, 0, 0, 15, tzinfo=timezone.utc),
        datetime(2026, 1, 1, 0, 1, 15, tzinfo=timezone.utc),
    ]
    df = _base_df(ts)
    with pytest.raises(SchemaError, match="Timestamp grid misalignment"):
        validate_ohlcv(df, timeframe="1m")


def test_grid_alignment_skipped_for_unknown_timeframe():
    """timeframe='unknown' debe saltarse el chequeo de grid sin lanzar error."""
    ts = [
        datetime(2026, 1, 1, 0, 0, 15, tzinfo=timezone.utc),
        datetime(2026, 1, 1, 0, 1, 30, tzinfo=timezone.utc),
    ]
    df = _base_df(ts)
    validated = validate_ohlcv(df, timeframe="unknown")
    assert len(validated) == 2


def test_timestamp_monotonic_check_returns_false_on_nulls():
    """Cubre línea 113: _check_timestamp_monotonic con nulls."""
    from market_data.application.processing.ohlcv_schema import (
        PolarsData,
        _check_timestamp_monotonic,
    )

    df = pl.DataFrame(
        {
            "timestamp": [
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                None,
            ]
        }
    ).with_columns(pl.col("timestamp").dt.cast_time_unit("us"))
    data = PolarsData(lazyframe=df.lazy(), key="timestamp")
    assert _check_timestamp_monotonic(data) is False


def test_make_grid_alignment_check_returns_none_for_invalid_timeframe():
    """Cubre líneas 165-166: rama except InvalidTimeframeError."""
    from market_data.application.processing.ohlcv_schema import make_grid_alignment_check

    assert make_grid_alignment_check("not_a_real_timeframe") is None


def test_make_grid_alignment_check_builds_check_for_valid_timeframe():
    """Cubre líneas 163-164, 167-172: construcción del Check para timeframe válido."""
    from market_data.application.processing.ohlcv_schema import (
        PolarsData,
        make_grid_alignment_check,
    )

    check = make_grid_alignment_check("1m")
    assert check is not None

    aligned_df = pl.DataFrame({"timestamp": [datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc)]}).with_columns(
        pl.col("timestamp").dt.cast_time_unit("us")
    )
    data_ok = PolarsData(lazyframe=aligned_df.lazy(), key="timestamp")
    assert check._check_fn(data_ok) is True

    misaligned_df = pl.DataFrame({"timestamp": [datetime(2026, 1, 1, 0, 0, 15, tzinfo=timezone.utc)]}).with_columns(
        pl.col("timestamp").dt.cast_time_unit("us")
    )
    data_bad = PolarsData(lazyframe=misaligned_df.lazy(), key="timestamp")
    assert check._check_fn(data_bad) is False


def test_validate_ohlcv_skips_grid_check_for_invalid_timeframe_string():
    """Cubre líneas 308-309: except InvalidTimeframeError: pass dentro de validate_ohlcv."""
    ts = [datetime(2026, 1, 1, tzinfo=timezone.utc)]
    df = _base_df(ts)
    # timeframe no vacío, no "unknown", pero inválido -> debe pasar de largo el grid check
    validated = validate_ohlcv(df, timeframe="not_a_real_timeframe")
    assert len(validated) == 1


def test_assert_utc_rejects_non_utc_timestamp():
    """Cubre línea 353: TypeError si el timestamp no es UTC."""
    ts = [datetime(2026, 1, 1)]  # naive, sin tzinfo
    df = pl.DataFrame(
        {
            "timestamp": ts,
            "open": [100.0],
            "high": [102.0],
            "low": [99.0],
            "close": [101.0],
            "volume": [10.0],
        }
    ).with_columns(pl.col("timestamp").dt.cast_time_unit("us"))
    with pytest.raises(TypeError, match="UTC"):
        validate_ohlcv(df, timeframe="1m")


def test_assert_non_empty_rejects_empty_dataframe():
    """Cubre línea 368: ValueError si el DataFrame está vacío."""
    df = pl.DataFrame(
        {
            "timestamp": [],
            "open": [],
            "high": [],
            "low": [],
            "close": [],
            "volume": [],
        },
        schema={
            "timestamp": pl.Datetime("us", "UTC"),
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Float64,
        },
    )
    with pytest.raises(ValueError, match="empty"):
        validate_ohlcv(df, timeframe="1m")
