"""
tests/storage/test_gold_storage.py
====================================

Tests unitarios de GoldStorage y GoldLoader.

Estrategia de aislamiento
--------------------------
• GoldStorage: dry_run=True — verifica lógica sin escribir en Iceberg.
• GoldLoader:  tests de construcción e invariantes de interfaz sin I/O real.
• Build completo (Silver→features→Gold) requiere Iceberg poblado — queda
  para tests de integración. Aquí solo se testea la capa de lógica pura.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from market_data.storage.gold.gold_storage import GoldStorage, _prepare_gold_df
from data_platform.loaders.gold_loader import (
    GoldLoader,
    GoldLoaderError,
    GoldVersionNotFound,
)
from data_platform.ohlcv_utils import (
    DataNotFoundError,
    VersionNotFoundError,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_silver_df(n: int = 30, seed: int = 0) -> pd.DataFrame:
    """DataFrame OHLCV sintético que simula salida de IcebergStorage."""
    rng = np.random.default_rng(seed)
    close = rng.uniform(40_000, 50_000, n)
    return pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=n, freq="1h", tz="UTC"),
        "open":   close + rng.uniform(-200, 200, n),
        "high":   close + rng.uniform(100, 500, n),
        "low":    close - rng.uniform(100, 500, n),
        "close":  close,
        "volume": rng.uniform(100, 1_000, n),
    })


@pytest.fixture
def gold() -> GoldStorage:
    return GoldStorage(dry_run=True)


# ── GoldStorage — construcción ────────────────────────────────────────────────

def test_gold_storage_instantiates(gold):
    assert gold._dry_run is True


def test_gold_storage_has_feature_engineer(gold):
    from market_data.storage.gold.feature_engineer import FeatureEngineer
    assert isinstance(gold._engineer, FeatureEngineer)


def test_gold_storage_engineer_version_is_semver(gold):
    parts = gold._engineer.VERSION.split(".")
    assert len(parts) == 3 and all(p.isdigit() for p in parts)


# ── GoldStorage.build() — dry_run ────────────────────────────────────────────

def test_build_dry_run_returns_dataframe_with_features(gold):
    """dry_run debe retornar el DataFrame con features calculados, sin escribir."""
    silver_df = _make_silver_df(30)

    with patch.object(
        gold._engineer.__class__, "compute", wraps=gold._engineer.compute
    ) as mock_compute:
        # Mockear IcebergStorage dentro de build()
        mock_silver = MagicMock()
        mock_silver.get_current_snapshot.return_value = {
            "snapshot_id": 12345,
            "timestamp_ms": 1_700_000_000_000,
        }
        mock_silver.load_ohlcv.return_value = silver_df

        with patch(
            "market_data.storage.gold.gold_storage.IcebergStorage",
            return_value=mock_silver,
        ):
            result = gold.build(
                exchange="bybit",
                symbol="BTC/USDT",
                market_type="spot",
                timeframe="1h",
            )

    assert result is not None
    assert len(result) == 30
    assert "vwap" in result.columns
    assert "return_1" in result.columns
    assert mock_compute.called


def test_build_dry_run_returns_none_when_silver_empty(gold):
    """build() retorna None si Silver no tiene datos."""
    mock_silver = MagicMock()
    mock_silver.get_current_snapshot.return_value = {}
    mock_silver.load_ohlcv.return_value = pd.DataFrame()

    with patch(
        "market_data.storage.gold.gold_storage.IcebergStorage",
        return_value=mock_silver,
    ):
        result = gold.build(
            exchange="bybit",
            symbol="BTC/USDT",
            market_type="spot",
            timeframe="1h",
        )

    assert result is None


def test_build_dry_run_returns_none_when_silver_raises(gold):
    """build() retorna None si Silver.load_ohlcv() falla."""
    mock_silver = MagicMock()
    mock_silver.get_current_snapshot.return_value = {}
    mock_silver.load_ohlcv.side_effect = RuntimeError("Iceberg scan failed")

    with patch(
        "market_data.storage.gold.gold_storage.IcebergStorage",
        return_value=mock_silver,
    ):
        result = gold.build(
            exchange="bybit",
            symbol="BTC/USDT",
            market_type="spot",
            timeframe="1h",
        )

    assert result is None


# ── _prepare_gold_df — helper interno ────────────────────────────────────────

def test_prepare_gold_df_adds_identity_columns():
    df = _make_silver_df(10)
    result = _prepare_gold_df(
        df,
        exchange="bybit", symbol="BTC/USDT",
        market_type="spot", timeframe="1h",
        run_id="test-run", engineer_version="1.1.0",
        silver_snapshot_id=999, silver_snapshot_ms=1_700_000_000_000,
    )
    assert (result["exchange"] == "bybit").all()
    assert (result["symbol"] == "BTC/USDT").all()
    assert (result["market_type"] == "spot").all()
    assert (result["timeframe"] == "1h").all()
    assert (result["engineer_version"] == "1.1.0").all()
    assert (result["silver_snapshot_id"] == 999).all()


def test_prepare_gold_df_timestamp_is_microseconds_utc():
    df = _make_silver_df(5)
    result = _prepare_gold_df(
        df,
        exchange="bybit", symbol="BTC/USDT",
        market_type="spot", timeframe="1h",
        run_id=None, engineer_version="1.1.0",
        silver_snapshot_id=0, silver_snapshot_ms=0,
    )
    assert str(result["timestamp"].dtype) == "datetime64[us, UTC]"


def test_prepare_gold_df_run_id_empty_string_when_none():
    df = _make_silver_df(5)
    result = _prepare_gold_df(
        df,
        exchange="bybit", symbol="BTC/USDT",
        market_type="spot", timeframe="1h",
        run_id=None, engineer_version="1.1.0",
        silver_snapshot_id=0, silver_snapshot_ms=0,
    )
    assert (result["run_id"] == "").all()


def test_prepare_gold_df_preserves_row_count():
    df = _make_silver_df(20)
    result = _prepare_gold_df(
        df,
        exchange="kucoin", symbol="ETH/USDT",
        market_type="spot", timeframe="4h",
        run_id="r1", engineer_version="1.1.0",
        silver_snapshot_id=1, silver_snapshot_ms=1,
    )
    assert len(result) == 20


# ── GoldLoader — construcción e interfaz ─────────────────────────────────────

def test_gold_loader_instantiates():
    loader = GoldLoader(exchange="bybit")
    assert loader._exchange == "bybit"


def test_gold_loader_lowercases_exchange():
    loader = GoldLoader(exchange="KuCoin")
    assert loader._exchange == "kucoin"


def test_gold_loader_exchange_none_by_default():
    loader = GoldLoader()
    assert loader._exchange is None


def test_gold_loader_aliases_are_correct_types():
    assert GoldLoaderError is DataNotFoundError
    assert GoldVersionNotFound is VersionNotFoundError


def test_gold_loader_list_versions_returns_list():
    loader = GoldLoader(exchange="bybit")
    versions = loader.list_versions("bybit", "BTC/USDT", "spot", "1h")
    assert isinstance(versions, list)


# ── GoldLoader._resolve_snapshot — lógica de versionado ─────────────────────

def test_resolve_snapshot_latest_returns_none():
    loader = GoldLoader(exchange="bybit")
    assert loader._resolve_snapshot("latest", None) is None


def test_resolve_snapshot_integer_string_returns_int():
    loader = GoldLoader(exchange="bybit")
    snap_id = loader._resolve_snapshot("9876543210", None)
    assert snap_id == 9_876_543_210


def test_resolve_snapshot_legacy_format_raises_version_not_found():
    loader = GoldLoader(exchange="bybit")
    with pytest.raises(VersionNotFoundError, match="legacy"):
        loader._resolve_snapshot("v000003", None)


def test_resolve_snapshot_invalid_string_raises_version_not_found():
    loader = GoldLoader(exchange="bybit")
    with pytest.raises(VersionNotFoundError):
        loader._resolve_snapshot("not-a-number", None)
