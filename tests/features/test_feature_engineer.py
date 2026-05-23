# -*- coding: utf-8 -*-
"""
tests/features/test_feature_engineer.py
========================================

Tests unitarios de GoldTransformer (feature engineering Gold layer).

Migración
---------
Anteriormente testeaban FeatureEngineer (shim deprecated).
Ahora apuntan directamente a GoldTransformer — SSOT del dominio.
El shim sigue existiendo en storage/gold/feature_engineer.py para
compatibilidad backward hasta v3.0.0 pero no se testea aquí.

Principios
----------
• Sin I/O — datos sintéticos deterministas (seed fija).
• Cada test verifica UNA propiedad.
• Nombres describen el comportamiento esperado, no la implementación.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import numpy as np
import polars as pl
import pytest
from market_data.infrastructure.storage.gold.transformer import (
    FEATURE_COLUMNS,
    VERSION,
    GoldTransformer,
)

# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_df(n: int = 30, seed: int = 42) -> pl.DataFrame:
    """DataFrame OHLCV sintético y determinista."""
    rng = np.random.default_rng(seed)
    close = rng.uniform(40_000, 50_000, n)
    spread = rng.uniform(500, 2_000, n)
    ts = pl.datetime_range(
        start=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(hours=n - 1),
        interval="1h",
        eager=True,
    )
    return pl.DataFrame(
        {
            "timestamp": ts,
            "open": (close + rng.uniform(-200, 200, n)).tolist(),
            "high": (close + spread).tolist(),
            "low": (close - spread).tolist(),
            "close": close.tolist(),
            "volume": rng.uniform(100, 1_000, n).tolist(),
        }
    )


def _transform(df: pl.DataFrame, **kwargs) -> pl.DataFrame:
    """Wrapper conveniente con defaults de test."""
    return GoldTransformer.transform(
        df,
        symbol=kwargs.get("symbol", "BTC/USDT"),
        timeframe=kwargs.get("timeframe", "1h"),
        exchange=kwargs.get("exchange", "test"),
    )


@pytest.fixture
def df30() -> pl.DataFrame:
    return _make_df(30)


# ── Contrato de versión ───────────────────────────────────────────────────────


def test_version_is_semver():
    parts = VERSION.split(".")
    assert len(parts) == 3
    assert all(p.isdigit() for p in parts)


def test_feature_columns_constant_matches_computed(df30):
    result = _transform(df30)
    assert set(FEATURE_COLUMNS).issubset(result.columns)


# ── Contratos de salida ───────────────────────────────────────────────────────


def test_transform_preserves_row_count(df30):
    result = _transform(df30)
    assert len(result) == len(df30)


def test_transform_does_not_mutate_input(df30):
    original_cols = list(df30.columns)
    _transform(df30)
    assert list(df30.columns) == original_cols


def test_transform_output_sorted_by_timestamp(df30):
    shuffled = df30.sample(fraction=1.0, seed=0)
    result = _transform(shuffled)
    assert result["timestamp"].is_sorted()


# ── return_1 ─────────────────────────────────────────────────────────────────


def test_return_1_first_row_is_nan(df30):
    result = _transform(df30)
    assert result["return_1"][0] is None


def test_return_1_is_pct_change_of_close(df30):
    result = _transform(df30)
    expected = df30.sort("timestamp")["close"].pct_change()
    np.testing.assert_allclose(
        result["return_1"].to_numpy(),
        expected.to_numpy(),
        rtol=1e-9,  # float64: log(a/b) acumula epsilon ~1e-10; 1e-9 es correcto
        equal_nan=True,
    )


# ── log_return ────────────────────────────────────────────────────────────────


def test_log_return_first_row_is_nan(df30):
    result = _transform(df30)
    assert result["log_return"][0] is None


def test_log_return_is_log_of_close_ratio(df30):
    result = _transform(df30)
    close = df30.sort("timestamp")["close"].to_numpy()
    expected = np.log(close[1:] / close[:-1])
    np.testing.assert_allclose(
        result["log_return"][1:].to_numpy(),
        expected,
        rtol=1e-9,  # float64: log(a/b) acumula epsilon ~1e-10; 1e-9 es correcto
    )


# ── volatility_20 ─────────────────────────────────────────────────────────────


def test_volatility_20_nan_count_respects_min_periods(df30):
    result = _transform(df30)
    nan_count = result["volatility_20"].is_nan().sum()
    assert nan_count <= 5


def test_volatility_20_is_non_negative(df30):
    result = _transform(df30)
    assert (result["volatility_20"].drop_nans() >= 0).all()


# ── high_low_spread ───────────────────────────────────────────────────────────


def test_high_low_spread_is_non_negative(df30):
    result = _transform(df30)
    assert (result["high_low_spread"].drop_nans() >= 0).all()


def test_high_low_spread_formula(df30):
    result = _transform(df30)
    s = df30.sort("timestamp")
    expected = (s["high"] - s["low"]) / s["close"]
    np.testing.assert_allclose(
        result["high_low_spread"].to_numpy(),
        expected.to_numpy(),
        rtol=1e-6,
        equal_nan=True,
    )


# ── vwap ─────────────────────────────────────────────────────────────────────


def test_vwap_nan_count_respects_min_periods(df30):
    result = _transform(df30)
    nan_count = result["vwap"].is_nan().sum()
    assert nan_count <= 5


def test_vwap_is_positive(df30):
    result = _transform(df30)
    assert (result["vwap"].drop_nans() > 0).all()


def test_vwap_is_price_weighted_not_cumulative():
    """
    VWAP rolling no debe quedar anclado al precio inicial.
    Un VWAP acumulado permanece sesgado hacia los primeros valores;
    el rolling debe reflejar el precio reciente de la ventana.
    """
    n = 60
    rng = np.random.default_rng(7)
    close = np.concatenate(
        [
            rng.uniform(900, 1_100, n // 2),
            rng.uniform(49_000, 51_000, n // 2),
        ]
    )
    spread = rng.uniform(10, 50, n)
    df = pl.DataFrame(
        {
            "timestamp": pl.datetime_range(
                start=datetime(2024, 1, 1, tzinfo=timezone.utc),
                end=datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(hours=n - 1),
                interval="1h",
                eager=True,
            ),
            "open": close,
            "high": close + spread,
            "low": close - spread,
            "close": close,
            "volume": rng.uniform(100, 1_000, n),
        }
    )
    result = GoldTransformer.transform(df, symbol="TEST", timeframe="1h", exchange="test")
    vwap_second_half = result["vwap"][40:].drop_nans()
    assert (vwap_second_half > 10_000).all(), "VWAP rolling debe reflejar precio reciente, no estar anclado al inicio"


# ── Edge cases ────────────────────────────────────────────────────────────────


def test_transform_returns_empty_df_when_empty():
    result = GoldTransformer.transform(pl.DataFrame(), symbol="X", timeframe="1h", exchange="test")
    assert result.is_empty()


def test_transform_handles_single_row():
    single = _make_df(1)
    result = _transform(single)
    assert len(result) == 1


def test_transform_handles_zero_volume():
    df = _make_df(30)
    df = df.with_columns(pl.lit(0.0).alias("volume"))
    result = _transform(df)
    assert not result["vwap"].is_infinite().any()


def test_transform_handles_zero_close():
    df = _make_df(30)
    df = df.with_columns(pl.when(pl.int_range(pl.len()) == 5).then(0.0).otherwise(pl.col("close")).alias("close"))
    result = _transform(df)
    assert "high_low_spread" in result.columns


def test_no_infinities_in_output(df30):
    result = _transform(df30)
    import polars.selectors as cs

    numeric = result.select(cs.numeric())
    assert not np.isinf(numeric.to_numpy()).any()
