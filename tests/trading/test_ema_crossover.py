# -*- coding: utf-8 -*-
"""
tests/trading/test_ema_crossover.py
=====================================

Tests unitarios de EMACrossoverStrategy.
Sin I/O — lógica pura con DataFrames sintéticos.

Nota sobre fixtures de cruce
-----------------------------
La estrategia evalúa SOLO la última vela (iloc[-1]).

golden cross — patrón verificado numéricamente:
  N-1 velas a 40k, última vela a 80k.
  EMA(9) reacciona antes que EMA(21) → fast cruza sobre slow en iloc[-1].

death cross — patrón verificado numéricamente:
  Base 40k, bloque alto 80k desde índice 10 hasta índice 58 (penúltima),
  última vela cae a 40k. Fast ya estaba sobre slow (cruce en idx 10),
  y la caída brusca hace que fast cruce bajo slow en la última vela.

  No funciona con precio alto plano desde el inicio porque las EMAs
  convergen al mismo valor — no hay "arriba" establecido antes del drop.
"""
from __future__ import annotations

from datetime import timezone

import numpy as np
import pandas as pd
import pytest

from trading.strategies.ema_crossover import EMACrossoverStrategy


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_df(n: int = 40, seed: int = 0) -> pd.DataFrame:
    """DataFrame OHLCV aleatorio — sin cruce garantizado."""
    rng   = np.random.default_rng(seed)
    close = rng.uniform(40_000, 50_000, n)
    return pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=n, freq="1h", tz="UTC"),
        "open":   close,
        "high":   close + 100,
        "low":    close - 100,
        "close":  close,
        "volume": rng.uniform(100, 1_000, n),
    })


def _make_golden_cross_df(n: int = 60) -> pd.DataFrame:
    """
    N-1 velas a 40k, última vela a 80k.
    Cruce verificado: EMA(9) > EMA(21) en iloc[-1].
    """
    close     = np.full(n, 40_000.0)
    close[-1] = 80_000.0
    return pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=n, freq="1h", tz="UTC"),
        "open":   close,
        "high":   close + 100,
        "low":    close - 100,
        "close":  close,
        "volume": np.ones(n) * 500,
    })


def _make_death_cross_df(n: int = 60) -> pd.DataFrame:
    """
    Base 40k, bloque alto 80k en [10:59], caída a 40k en la última vela.

    Secuencia de cruces verificada numéricamente:
      idx=0  : cruce espurio por NaN en shift (ignorado — no es la última vela)
      idx=10 : fast sube sobre slow (golden cross intermedio)
      idx=59 : fast cae bajo slow (death cross en última vela → señal SELL)

    Un precio alto constante desde el inicio no funciona: las EMAs
    convergen al mismo valor y no hay "arriba" establecido antes del drop.
    """
    close        = np.full(n, 40_000.0)
    close[10:59] = 80_000.0   # fast sube sobre slow en idx 10
    close[59]    = 40_000.0   # fast cae bajo slow en idx 59 (última)
    return pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=n, freq="1h", tz="UTC"),
        "open":   close,
        "high":   close + 100,
        "low":    close - 100,
        "close":  close,
        "volume": np.ones(n) * 500,
    })


@pytest.fixture
def strategy() -> EMACrossoverStrategy:
    return EMACrossoverStrategy(fast_period=9, slow_period=21,
                                symbol="BTC/USDT", timeframe="1h")


# ── Construcción ──────────────────────────────────────────────────────────────

def test_strategy_name():
    assert EMACrossoverStrategy.name == "ema_crossover"


def test_fast_must_be_less_than_slow():
    with pytest.raises(ValueError, match="fast_period"):
        EMACrossoverStrategy(fast_period=21, slow_period=9)


def test_equal_periods_raises():
    with pytest.raises(ValueError):
        EMACrossoverStrategy(fast_period=9, slow_period=9)


# ── generate_signals — datos insuficientes ────────────────────────────────────

def test_returns_empty_when_too_few_rows(strategy):
    df = _make_df(n=10)   # < slow_period + 1 = 22
    assert strategy.generate_signals(df) == []


def test_returns_empty_when_df_missing_column(strategy):
    df = _make_df(n=40).drop(columns=["volume"])
    with pytest.raises(ValueError, match="missing columns"):
        strategy.generate_signals(df)


def test_returns_empty_when_df_is_empty(strategy):
    df = _make_df(n=40).iloc[0:0]
    with pytest.raises(ValueError, match="empty"):
        strategy.generate_signals(df)


# ── generate_signals — lógica de cruce ───────────────────────────────────────

def test_golden_cross_generates_buy(strategy):
    signals = strategy.generate_signals(_make_golden_cross_df())
    assert len(signals) == 1
    assert signals[0].signal == "buy"


def test_death_cross_generates_sell(strategy):
    signals = strategy.generate_signals(_make_death_cross_df())
    assert len(signals) == 1
    assert signals[0].signal == "sell"


def test_flat_price_no_crossover_returns_empty(strategy):
    """Sin cruce en la última vela no hay señal."""
    df          = _make_df(n=40)
    df["close"] = 45_000.0
    df["open"]  = 45_000.0
    df["high"]  = 45_100.0
    df["low"]   = 44_900.0
    assert strategy.generate_signals(df) == []


# ── generate_signals — propiedades de la señal ───────────────────────────────

def test_signal_symbol_matches_strategy(strategy):
    signals = strategy.generate_signals(_make_golden_cross_df())
    assert signals[0].symbol == "BTC/USDT"


def test_signal_timeframe_matches_strategy(strategy):
    signals = strategy.generate_signals(_make_golden_cross_df())
    assert signals[0].timeframe == "1h"


def test_signal_timestamp_is_utc_aware(strategy):
    signals = strategy.generate_signals(_make_golden_cross_df())
    assert signals[0].timestamp.tzinfo is not None


def test_signal_metadata_contains_ema_values(strategy):
    signals = strategy.generate_signals(_make_golden_cross_df())
    meta    = signals[0].metadata
    assert "ema_fast" in meta
    assert "ema_slow" in meta
    assert meta["fast_period"] == 9
    assert meta["slow_period"] == 21


def test_signal_price_equals_last_close(strategy):
    df      = _make_golden_cross_df()
    signals = strategy.generate_signals(df)
    assert signals[0].price == float(df.iloc[-1]["close"])


def test_input_df_is_not_mutated(strategy):
    """generate_signals no debe modificar el DataFrame original."""
    df          = _make_golden_cross_df()
    cols_before = set(df.columns)
    strategy.generate_signals(df)
    assert set(df.columns) == cols_before
