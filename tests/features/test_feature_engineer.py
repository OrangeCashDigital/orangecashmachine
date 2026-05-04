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

import numpy as np
import pandas as pd
import pytest

from market_data.gold.transformer import FEATURE_COLUMNS, GoldTransformer, VERSION


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_df(n: int = 30, seed: int = 42) -> pd.DataFrame:
    """DataFrame OHLCV sintético y determinista."""
    rng = np.random.default_rng(seed)
    close  = rng.uniform(40_000, 50_000, n)
    spread = rng.uniform(500, 2_000, n)
    return pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=n, freq="1h", tz="UTC"),
        "open":   close + rng.uniform(-200, 200, n),
        "high":   close + spread,
        "low":    close - spread,
        "close":  close,
        "volume": rng.uniform(100, 1_000, n),
    })


def _transform(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """Wrapper conveniente con defaults de test."""
    return GoldTransformer.transform(
        df,
        symbol    = kwargs.get("symbol", "BTC/USDT"),
        timeframe = kwargs.get("timeframe", "1h"),
        exchange  = kwargs.get("exchange", "test"),
    )


@pytest.fixture
def df30() -> pd.DataFrame:
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
    shuffled = df30.sample(frac=1, random_state=0).reset_index(drop=True)
    result   = _transform(shuffled)
    assert result["timestamp"].is_monotonic_increasing


# ── return_1 ─────────────────────────────────────────────────────────────────

def test_return_1_first_row_is_nan(df30):
    result = _transform(df30)
    assert pd.isna(result["return_1"].iloc[0])


def test_return_1_is_pct_change_of_close(df30):
    result   = _transform(df30)
    expected = df30.sort_values("timestamp")["close"].pct_change()
    pd.testing.assert_series_equal(
        result["return_1"].reset_index(drop=True),
        expected.reset_index(drop=True),
        check_names=False,
    )


# ── log_return ────────────────────────────────────────────────────────────────

def test_log_return_first_row_is_nan(df30):
    result = _transform(df30)
    assert pd.isna(result["log_return"].iloc[0])


def test_log_return_is_log_of_close_ratio(df30):
    result = _transform(df30)
    close  = df30.sort_values("timestamp")["close"].values
    expected = np.log(close[1:] / close[:-1])
    np.testing.assert_allclose(
        result["log_return"].iloc[1:].values,
        expected,
        rtol=1e-10,
    )


# ── volatility_20 ─────────────────────────────────────────────────────────────

def test_volatility_20_nan_count_respects_min_periods(df30):
    result    = _transform(df30)
    nan_count = result["volatility_20"].isna().sum()
    assert nan_count <= 5


def test_volatility_20_is_non_negative(df30):
    result = _transform(df30)
    assert (result["volatility_20"].dropna() >= 0).all()


# ── high_low_spread ───────────────────────────────────────────────────────────

def test_high_low_spread_is_non_negative(df30):
    result = _transform(df30)
    assert (result["high_low_spread"].dropna() >= 0).all()


def test_high_low_spread_formula(df30):
    result = _transform(df30)
    s      = df30.sort_values("timestamp").reset_index(drop=True)
    expected = (s["high"] - s["low"]) / s["close"].replace(0, np.nan)
    pd.testing.assert_series_equal(
        result["high_low_spread"].reset_index(drop=True),
        expected.reset_index(drop=True),
        check_names=False,
    )


# ── vwap ─────────────────────────────────────────────────────────────────────

def test_vwap_nan_count_respects_min_periods(df30):
    result    = _transform(df30)
    nan_count = result["vwap"].isna().sum()
    assert nan_count <= 5


def test_vwap_is_positive(df30):
    result = _transform(df30)
    assert (result["vwap"].dropna() > 0).all()


def test_vwap_is_price_weighted_not_cumulative():
    """
    VWAP rolling no debe quedar anclado al precio inicial.
    Un VWAP acumulado permanece sesgado hacia los primeros valores;
    el rolling debe reflejar el precio reciente de la ventana.
    """
    n   = 60
    rng = np.random.default_rng(7)
    close = np.concatenate([
        rng.uniform(900, 1_100, n // 2),
        rng.uniform(49_000, 51_000, n // 2),
    ])
    spread = rng.uniform(10, 50, n)
    df = pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=n, freq="1h", tz="UTC"),
        "open":   close,
        "high":   close + spread,
        "low":    close - spread,
        "close":  close,
        "volume": rng.uniform(100, 1_000, n),
    })
    result = GoldTransformer.transform(df, symbol="TEST", timeframe="1h", exchange="test")
    vwap_second_half = result["vwap"].iloc[40:].dropna()
    assert (vwap_second_half > 10_000).all(), (
        "VWAP rolling debe reflejar precio reciente, no estar anclado al inicio"
    )


# ── Edge cases ────────────────────────────────────────────────────────────────

def test_transform_returns_empty_df_when_empty():
    result = GoldTransformer.transform(
        pd.DataFrame(), symbol="X", timeframe="1h", exchange="test"
    )
    assert result.empty


def test_transform_handles_single_row():
    single = _make_df(1)
    result = _transform(single)
    assert len(result) == 1


def test_transform_handles_zero_volume():
    df = _make_df(30)
    df["volume"] = 0.0
    result = _transform(df)
    assert not result["vwap"].isin([np.inf, -np.inf]).any()


def test_transform_handles_zero_close():
    df = _make_df(30)
    df.loc[5, "close"] = 0.0
    result = _transform(df)
    assert "high_low_spread" in result.columns


def test_no_infinities_in_output(df30):
    result  = _transform(df30)
    numeric = result.select_dtypes(include="number")
    assert not np.isinf(numeric.values).any()
