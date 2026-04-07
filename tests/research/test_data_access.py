"""
tests/research/test_data_access.py
====================================

Tests unitarios de la API pública de research.data.data_access.

Estrategia de aislamiento
--------------------------
Todos los tests mockean IcebergStorage y GoldLoader — sin I/O real.
Se verifica la lógica de enrutamiento, manejo de errores y contratos
de la API pública. El comportamiento de los backends se testa en sus
propios módulos de test.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from data_platform.ohlcv_utils import (
    DataNotFoundError,
    DataReadError,
    MarketDataLoaderError,
)
from research.data.data_access import (
    _reset_gold_loader,
    _reset_storage,
    get_features,
    get_features_dict,
    get_multiple_ohlcv,
    get_ohlcv,
    get_ohlcv_dict,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_ohlcv_df(n: int = 10) -> pd.DataFrame:
    rng = np.random.default_rng(0)
    return pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=n, freq="1h", tz="UTC"),
        "open":   rng.uniform(40_000, 50_000, n),
        "high":   rng.uniform(50_000, 55_000, n),
        "low":    rng.uniform(38_000, 40_000, n),
        "close":  rng.uniform(40_000, 50_000, n),
        "volume": rng.uniform(100, 1_000, n),
    })


def _make_features_df(n: int = 10) -> pd.DataFrame:
    df = _make_ohlcv_df(n)
    df["return_1"]        = df["close"].pct_change()
    df["log_return"]      = np.log(df["close"] / df["close"].shift(1))
    df["volatility_20"]   = df["log_return"].rolling(5).std()
    df["high_low_spread"] = (df["high"] - df["low"]) / df["close"]
    df["vwap"]            = df["close"]
    return df


@pytest.fixture(autouse=True)
def reset_caches():
    """Limpiar caches singleton entre tests — evita contaminación."""
    _reset_storage()
    _reset_gold_loader()
    yield
    _reset_storage()
    _reset_gold_loader()


# ── get_ohlcv — contrato básico ───────────────────────────────────────────────

def test_get_ohlcv_returns_dataframe():
    mock_storage = MagicMock()
    mock_storage.load_ohlcv.return_value = _make_ohlcv_df()

    with patch(
        "research.data.data_access.IcebergStorage",
        return_value=mock_storage,
    ):
        df = get_ohlcv("BTC/USDT", "1h", exchange="bybit")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 10


def test_get_ohlcv_raises_data_not_found_when_empty():
    mock_storage = MagicMock()
    mock_storage.load_ohlcv.return_value = pd.DataFrame()

    with patch(
        "research.data.data_access.IcebergStorage",
        return_value=mock_storage,
    ):
        with pytest.raises(DataNotFoundError):
            get_ohlcv("BTC/USDT", "1h", exchange="bybit")


def test_get_ohlcv_raises_data_not_found_when_none():
    mock_storage = MagicMock()
    mock_storage.load_ohlcv.return_value = None

    with patch(
        "research.data.data_access.IcebergStorage",
        return_value=mock_storage,
    ):
        with pytest.raises(DataNotFoundError):
            get_ohlcv("BTC/USDT", "1h", exchange="bybit")


def test_get_ohlcv_raises_data_read_error_on_storage_exception():
    mock_storage = MagicMock()
    mock_storage.load_ohlcv.side_effect = RuntimeError("Iceberg down")

    with patch(
        "research.data.data_access.IcebergStorage",
        return_value=mock_storage,
    ):
        with pytest.raises(DataReadError):
            get_ohlcv("BTC/USDT", "1h", exchange="bybit")


def test_get_ohlcv_filters_columns_when_requested():
    mock_storage = MagicMock()
    mock_storage.load_ohlcv.return_value = _make_ohlcv_df()

    with patch(
        "research.data.data_access.IcebergStorage",
        return_value=mock_storage,
    ):
        df = get_ohlcv(
            "BTC/USDT", "1h",
            exchange="bybit",
            columns=["timestamp", "close"],
        )

    assert list(df.columns) == ["timestamp", "close"]


def test_get_ohlcv_passes_start_end_as_timestamps():
    mock_storage = MagicMock()
    mock_storage.load_ohlcv.return_value = _make_ohlcv_df()

    with patch(
        "research.data.data_access.IcebergStorage",
        return_value=mock_storage,
    ):
        get_ohlcv(
            "BTC/USDT", "1h",
            exchange="bybit",
            start="2024-01-01",
            end="2024-06-01",
        )

    call_kwargs = mock_storage.load_ohlcv.call_args.kwargs
    assert isinstance(call_kwargs["start"], pd.Timestamp)
    assert isinstance(call_kwargs["end"],   pd.Timestamp)


# ── get_ohlcv — cache singleton ───────────────────────────────────────────────

def test_get_ohlcv_reuses_storage_singleton_for_same_exchange():
    mock_storage = MagicMock()
    mock_storage.load_ohlcv.return_value = _make_ohlcv_df()

    with patch(
        "research.data.data_access.IcebergStorage",
        return_value=mock_storage,
    ) as MockStorage:
        get_ohlcv("BTC/USDT", "1h", exchange="bybit")
        get_ohlcv("ETH/USDT", "1h", exchange="bybit")

    # IcebergStorage solo se instancia una vez para el mismo exchange
    assert MockStorage.call_count == 1


def test_get_ohlcv_creates_separate_storage_for_different_exchange():
    mock_storage = MagicMock()
    mock_storage.load_ohlcv.return_value = _make_ohlcv_df()

    with patch(
        "research.data.data_access.IcebergStorage",
        return_value=mock_storage,
    ) as MockStorage:
        get_ohlcv("BTC/USDT", "1h", exchange="bybit")
        get_ohlcv("BTC/USDT", "1h", exchange="kucoin")

    assert MockStorage.call_count == 2


# ── get_multiple_ohlcv ────────────────────────────────────────────────────────

def test_get_multiple_ohlcv_returns_successful_symbols():
    mock_storage = MagicMock()
    mock_storage.load_ohlcv.return_value = _make_ohlcv_df()

    with patch(
        "research.data.data_access.IcebergStorage",
        return_value=mock_storage,
    ):
        result = get_multiple_ohlcv(
            ["BTC/USDT", "ETH/USDT"], "1h", exchange="bybit"
        )

    assert set(result.keys()) == {"BTC/USDT", "ETH/USDT"}


def test_get_multiple_ohlcv_skips_failed_symbols():
    mock_storage = MagicMock()

    def _side_effect(symbol, **kwargs):
        if symbol == "FAIL/USDT":
            raise RuntimeError("not found")
        return _make_ohlcv_df()

    mock_storage.load_ohlcv.side_effect = _side_effect

    with patch(
        "research.data.data_access.IcebergStorage",
        return_value=mock_storage,
    ):
        result = get_multiple_ohlcv(
            ["BTC/USDT", "FAIL/USDT"], "1h", exchange="bybit"
        )

    assert "BTC/USDT" in result
    assert "FAIL/USDT" not in result


def test_get_ohlcv_dict_is_alias_of_get_multiple_ohlcv():
    mock_storage = MagicMock()
    mock_storage.load_ohlcv.return_value = _make_ohlcv_df()

    with patch(
        "research.data.data_access.IcebergStorage",
        return_value=mock_storage,
    ):
        r1 = get_multiple_ohlcv(["BTC/USDT"], "1h", exchange="bybit")
        _reset_storage()
        r2 = get_ohlcv_dict(["BTC/USDT"], "1h", exchange="bybit")

    assert set(r1.keys()) == set(r2.keys())


# ── get_features ──────────────────────────────────────────────────────────────

def test_get_features_returns_dataframe():
    mock_loader = MagicMock()
    mock_loader.load_features.return_value = _make_features_df()

    with patch(
        "research.data.data_access.GoldLoader",
        return_value=mock_loader,
    ):
        df = get_features("BTC/USDT", "1h", exchange="bybit")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 10


def test_get_features_propagates_data_not_found():
    mock_loader = MagicMock()
    mock_loader.load_features.side_effect = DataNotFoundError("no data")

    with patch(
        "research.data.data_access.GoldLoader",
        return_value=mock_loader,
    ):
        with pytest.raises(DataNotFoundError):
            get_features("BTC/USDT", "1h", exchange="bybit")


def test_get_features_filters_by_start_date():
    df_full = _make_features_df(20)
    mock_loader = MagicMock()
    mock_loader.load_features.return_value = df_full

    with patch(
        "research.data.data_access.GoldLoader",
        return_value=mock_loader,
    ):
        df = get_features(
            "BTC/USDT", "1h",
            exchange="bybit",
            start="2024-01-11",
        )

    # Solo filas con timestamp >= 2024-01-11
    assert (df["timestamp"] >= pd.Timestamp("2024-01-11", tz="UTC")).all()


# ── get_features_dict ─────────────────────────────────────────────────────────

def test_get_features_dict_returns_successful_symbols():
    mock_loader = MagicMock()
    mock_loader.load_features.return_value = _make_features_df()

    with patch(
        "research.data.data_access.GoldLoader",
        return_value=mock_loader,
    ):
        result = get_features_dict(
            ["BTC/USDT", "ETH/USDT"], "1h", exchange="bybit"
        )

    assert set(result.keys()) == {"BTC/USDT", "ETH/USDT"}


def test_get_features_dict_skips_failed_symbols():
    mock_loader = MagicMock()

    def _side_effect(symbol, **kwargs):
        if "FAIL" in symbol:
            raise DataNotFoundError("no data")
        return _make_features_df()

    mock_loader.load_features.side_effect = _side_effect

    with patch(
        "research.data.data_access.GoldLoader",
        return_value=mock_loader,
    ):
        result = get_features_dict(
            ["BTC/USDT", "FAIL/USDT"], "1h", exchange="bybit"
        )

    assert "BTC/USDT" in result
    assert "FAIL/USDT" not in result


# ── Exports públicos ──────────────────────────────────────────────────────────

def test_public_exceptions_are_importable():
    """Los re-exports de __all__ deben ser importables directamente."""
    from research.data.data_access import (
        DataNotFoundError,
        DataReadError,
        MarketDataLoaderError,
    )
    assert DataNotFoundError is not None
    assert DataReadError is not None
    assert MarketDataLoaderError is not None
