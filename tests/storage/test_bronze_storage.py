"""
tests/storage/test_bronze_storage.py
======================================

Tests unitarios de BronzeStorage.

Estrategia de aislamiento
--------------------------
Todos los tests usan dry_run=True — nunca tocan el catálogo Iceberg real.
Los tests de validación de entrada no requieren ningún mock.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from market_data.storage.bronze.bronze_storage import (
    BronzeStorage,
    BronzeStorageError,
    BronzeWriteError,
    REQUIRED_COLUMNS,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_ohlcv(n: int = 10, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    return pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=n, freq="1h", tz="UTC"),
        "open":   rng.uniform(40_000, 50_000, n),
        "high":   rng.uniform(50_000, 55_000, n),
        "low":    rng.uniform(38_000, 40_000, n),
        "close":  rng.uniform(40_000, 50_000, n),
        "volume": rng.uniform(100, 1_000, n),
    })


@pytest.fixture
def bronze() -> BronzeStorage:
    return BronzeStorage(exchange="kucoin", market_type="spot", dry_run=True)


# ── Construcción ──────────────────────────────────────────────────────────────

def test_instantiation_sets_exchange(bronze):
    assert bronze._exchange == "kucoin"


def test_instantiation_lowercases_exchange():
    b = BronzeStorage(exchange="KuCoin", dry_run=True)
    assert b._exchange == "kucoin"


def test_instantiation_default_market_type():
    b = BronzeStorage(exchange="bybit", dry_run=True)
    assert b._market_type == "spot"


def test_instantiation_without_exchange():
    b = BronzeStorage(dry_run=True)
    assert b._exchange == "unknown"


# ── REQUIRED_COLUMNS ─────────────────────────────────────────────────────────

def test_required_columns_contains_ohlcv():
    assert REQUIRED_COLUMNS >= {"timestamp", "open", "high", "low", "close", "volume"}


# ── Validación de entrada ─────────────────────────────────────────────────────

def test_append_raises_on_empty_dataframe(bronze):
    with pytest.raises(BronzeStorageError):
        bronze.append(pd.DataFrame(), symbol="BTC/USDT", timeframe="1h")


def test_append_raises_on_none(bronze):
    with pytest.raises((BronzeStorageError, AttributeError)):
        bronze.append(None, symbol="BTC/USDT", timeframe="1h")


def test_append_raises_on_missing_columns(bronze):
    df = pd.DataFrame({"timestamp": pd.date_range("2024-01-01", periods=3, freq="1h", tz="UTC")})
    with pytest.raises(BronzeStorageError, match="Missing columns"):
        bronze.append(df, symbol="BTC/USDT", timeframe="1h")


# ── dry_run ───────────────────────────────────────────────────────────────────

def test_append_dry_run_returns_run_id(bronze):
    df     = _make_ohlcv()
    run_id = bronze.append(df, symbol="BTC/USDT", timeframe="1h")
    assert isinstance(run_id, str)
    assert len(run_id) > 0


def test_append_dry_run_accepts_explicit_run_id(bronze):
    df     = _make_ohlcv()
    run_id = bronze.append(df, symbol="BTC/USDT", timeframe="1h", run_id="test-run-001")
    assert run_id == "test-run-001"


def test_append_dry_run_returns_different_run_ids_each_call(bronze):
    df  = _make_ohlcv()
    id1 = bronze.append(df, symbol="BTC/USDT", timeframe="1h")
    id2 = bronze.append(df, symbol="BTC/USDT", timeframe="1h")
    assert id1 != id2
