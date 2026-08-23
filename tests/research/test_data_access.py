"""
tests/research/test_data_access.py
====================================

Tests unitarios de la API pública de research.data.data_access.

Estrategia de aislamiento (F-1)
--------------------------------
research/data/data_access.py depende de contracts — StorageFactoryPort y
FeatureReaderPort — no de adapters concretos. Los tests inyectan FAKES de
esos ports en el seam de inyección del módulo:

  · data_access._storage_factory    → fake de StorageFactoryPort
  · data_access.build_feature_reader → devuelve fake de FeatureReaderPort

Nunca se patchea la instancia concreta ni se toca _cache interno de un
adapter. Los tests demuestran que research funciona contra el CONTRATO,
no contra la implementación.

Principio: patch where it's used, at the exact point of injection.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
import research.data.data_access as data_access
from market_data.domain.exceptions import (
    DataNotFoundError,
    DataReadError,
)
from market_data.ports.outbound.feature_reader import FeatureReaderPort
from market_data.ports.outbound.storage_factory import StorageFactoryPort
from research.data.composition_root import build_storage_factory
from research.data.data_access import (
    get_features,
    get_features_dict,
    get_multiple_ohlcv,
    get_ohlcv,
    get_ohlcv_dict,
)

# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_ohlcv_df(n: int = 10) -> pl.DataFrame:
    from datetime import datetime, timezone

    import numpy as np

    rng = np.random.default_rng(0)
    return pl.DataFrame(
        {
            "timestamp": [datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(hours=i) for i in range(n)],
            "open": rng.uniform(40_000, 50_000, n),
            "high": rng.uniform(50_000, 55_000, n),
            "low": rng.uniform(38_000, 40_000, n),
            "close": rng.uniform(40_000, 50_000, n),
            "volume": rng.uniform(100, 1_000, n),
        }
    )


def _make_features_df(n: int = 10) -> pl.DataFrame:
    df = _make_ohlcv_df(n)
    df = df.with_columns(
        [
            pl.col("close").pct_change().alias("return_1"),
            (pl.col("close").log() - pl.col("close").shift(1).log()).alias("log_return"),
            pl.col("close").log().rolling_std(window_size=5, min_samples=2).alias("volatility_20"),
            ((pl.col("high") - pl.col("low")) / pl.col("close")).alias("high_low_spread"),
            pl.col("close").alias("vwap"),
        ]
    )
    return df


def _make_storage(df: pl.DataFrame | None = None) -> MagicMock:
    """OHLCVStorage mock — load_ohlcv retorna df por defecto."""
    m = MagicMock()
    m.load_ohlcv.return_value = df if df is not None else _make_ohlcv_df()
    return m


class _FakeStorageFactory:
    """
    Fake de StorageFactoryPort (F-1): no conoce IcebergStorageFactory.

    get_storage devuelve el OHLCVStorage inyectado, contando llamadas para
    los tests que verifican la política de cache del módulo.
    """

    def __init__(self, storage: object) -> None:
        self._storage = storage
        self.call_count = 0

    def get_storage(
        self,
        exchange: str,
        market_type: str = "spot",
        dry_run: bool = False,
    ) -> object:
        self.call_count += 1
        return self._storage


class _FakeFeatureReader:
    """
    Fake de FeatureReaderPort (F-1): no conoce GoldReader.

    load_features devuelve el DataFrame inyectado o lanza el error inyectado.
    """

    def __init__(self, df: pl.DataFrame | None = None, exc: Optional[Exception] = None) -> None:
        self._df = df
        self._exc = exc

    def load_features(
        self,
        symbol: str,
        market_type: str,
        timeframe: str,
        version: str = "latest",
        as_of: Optional[str] = None,
        columns: Optional[list] = None,
        exchange: Optional[str] = None,
    ) -> pl.DataFrame:
        if self._exc is not None:
            raise self._exc
        return self._df if self._df is not None else _make_features_df()

    def list_versions(
        self,
        exchange: str,
        symbol: str,
        market_type: str,
        timeframe: str,
    ) -> list:
        return []

    def list_datasets(self, exchange: str, market_type: str) -> list:
        return []

    def get_manifest(
        self,
        exchange: str,
        symbol: str,
        market_type: str,
        timeframe: str,
        version: str = "latest",
        as_of: Optional[str] = None,
    ) -> Optional[dict]:
        return None


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def restore_seams():
    """
    Restaurar los seams de inyección tras cada test — sin estado global
    (F-1: ya no hay singleton module-level de adaptador concreto).
    """
    yield
    data_access._storage_factory = build_storage_factory()
    data_access._gold_cache.clear()


def _inject_factory(storage: object) -> _FakeStorageFactory:
    factory = _FakeStorageFactory(storage)
    data_access._storage_factory = factory
    return factory


# ── get_ohlcv — contrato básico ───────────────────────────────────────────────


def test_get_ohlcv_returns_dataframe():
    _inject_factory(_make_storage())
    df = get_ohlcv("BTC/USDT", "1h", exchange="bybit")
    assert isinstance(df, pl.DataFrame)
    assert len(df) == 10


def test_get_ohlcv_raises_data_not_found_when_empty():
    _inject_factory(_make_storage(pl.DataFrame()))
    with pytest.raises(DataNotFoundError):
        get_ohlcv("BTC/USDT", "1h", exchange="bybit")


def test_get_ohlcv_raises_data_not_found_when_none():
    storage = _make_storage(None)
    storage.load_ohlcv.return_value = None
    _inject_factory(storage)
    with pytest.raises(DataNotFoundError):
        get_ohlcv("BTC/USDT", "1h", exchange="bybit")


def test_get_ohlcv_raises_data_read_error_on_storage_exception():
    storage = MagicMock()
    storage.load_ohlcv.side_effect = RuntimeError("Iceberg down")
    _inject_factory(storage)
    with pytest.raises(DataReadError):
        get_ohlcv("BTC/USDT", "1h", exchange="bybit")


def test_get_ohlcv_filters_columns_when_requested():
    _inject_factory(_make_storage())
    df = get_ohlcv(
        "BTC/USDT",
        "1h",
        exchange="bybit",
        columns=["timestamp", "close"],
    )
    assert list(df.columns) == ["timestamp", "close"]


def test_get_ohlcv_passes_start_end_as_timestamps():
    storage = _make_storage()
    _inject_factory(storage)
    get_ohlcv(
        "BTC/USDT",
        "1h",
        exchange="bybit",
        start="2024-01-01",
        end="2024-06-01",
    )
    call_kwargs = storage.load_ohlcv.call_args.kwargs
    assert isinstance(call_kwargs["start"], datetime)
    assert call_kwargs["start"].tzinfo is not None
    assert isinstance(call_kwargs["end"], datetime)
    assert call_kwargs["end"].tzinfo is not None


# ── get_ohlcv — cache singleton ───────────────────────────────────────────────


def test_get_ohlcv_reuses_storage_singleton_for_same_exchange():
    """El factory retorna la misma instancia para el mismo (exchange, market_type)."""
    storage = _make_storage()
    factory = _inject_factory(storage)
    get_ohlcv("BTC/USDT", "1h", exchange="bybit")
    get_ohlcv("ETH/USDT", "1h", exchange="bybit")

    # get_storage llamado dos veces — el cache de la factory deduplica instancias
    assert factory.call_count == 2


def test_get_ohlcv_creates_separate_storage_for_different_exchange():
    storage = _make_storage()
    factory = _inject_factory(storage)
    get_ohlcv("BTC/USDT", "1h", exchange="bybit")
    get_ohlcv("BTC/USDT", "1h", exchange="kucoin")

    assert factory.call_count == 2


# ── get_multiple_ohlcv ────────────────────────────────────────────────────────


def test_get_multiple_ohlcv_returns_successful_symbols():
    _inject_factory(_make_storage())
    result = get_multiple_ohlcv(["BTC/USDT", "ETH/USDT"], "1h", exchange="bybit")
    assert set(result.keys()) == {"BTC/USDT", "ETH/USDT"}


def test_get_multiple_ohlcv_skips_failed_symbols():
    storage = MagicMock()

    def _side_effect(symbol, **kwargs):
        if symbol == "FAIL/USDT":
            raise RuntimeError("not found")
        return _make_ohlcv_df()

    storage.load_ohlcv.side_effect = _side_effect
    _inject_factory(storage)
    result = get_multiple_ohlcv(["BTC/USDT", "FAIL/USDT"], "1h", exchange="bybit")
    assert "BTC/USDT" in result
    assert "FAIL/USDT" not in result


def test_get_ohlcv_dict_is_alias_of_get_multiple_ohlcv():
    _inject_factory(_make_storage())
    r1 = get_multiple_ohlcv(["BTC/USDT"], "1h", exchange="bybit")
    r2 = get_ohlcv_dict(["BTC/USDT"], "1h", exchange="bybit")
    assert set(r1.keys()) == set(r2.keys())


# ── get_features ──────────────────────────────────────────────────────────────


def test_get_features_returns_dataframe():
    fake = _FakeFeatureReader(_make_features_df())
    with patch.object(data_access, "build_feature_reader", return_value=fake):
        df = get_features("BTC/USDT", "1h", exchange="bybit")
    assert isinstance(df, pl.DataFrame)
    assert len(df) == 10


def test_get_features_propagates_data_not_found():
    fake = _FakeFeatureReader(exc=DataNotFoundError("no data"))
    with patch.object(data_access, "build_feature_reader", return_value=fake):
        with pytest.raises(DataNotFoundError):
            get_features("BTC/USDT", "1h", exchange="bybit")


def test_get_features_filters_by_start_date():
    df_full = _make_features_df(20)
    fake = _FakeFeatureReader(df_full)
    with patch.object(data_access, "build_feature_reader", return_value=fake):
        df = get_features(
            "BTC/USDT",
            "1h",
            exchange="bybit",
            start="2024-01-11",
        )
    cutoff = datetime(2024, 1, 11, tzinfo=timezone.utc)
    assert (df["timestamp"] >= cutoff).all()


# ── get_features_dict ─────────────────────────────────────────────────────────


def test_get_features_dict_returns_successful_symbols():
    fake = _FakeFeatureReader(_make_features_df())
    with patch.object(data_access, "build_feature_reader", return_value=fake):
        result = get_features_dict(["BTC/USDT", "ETH/USDT"], "1h", exchange="bybit")
    assert set(result.keys()) == {"BTC/USDT", "ETH/USDT"}


def test_get_features_dict_skips_failed_symbols():
    def _side_effect(symbol, **kwargs):
        if "FAIL" in symbol:
            raise DataNotFoundError("no data")
        return _make_features_df()

    fake = _FakeFeatureReader()
    fake.load_features = _side_effect
    with patch.object(data_access, "build_feature_reader", return_value=fake):
        result = get_features_dict(["BTC/USDT", "FAIL/USDT"], "1h", exchange="bybit")
    assert "BTC/USDT" in result
    assert "FAIL/USDT" not in result


# ── Exports públicos ──────────────────────────────────────────────────────────


def test_public_exceptions_are_importable():
    from research.data.data_access import (
        DataNotFoundError,
        DataReadError,
        MarketDataLoaderError,
    )

    assert DataNotFoundError is not None
    assert DataReadError is not None
    assert MarketDataLoaderError is not None


# ── Garantía F-1: research depende de contracts, no de concretos ─────────────


def test_data_access_uses_storage_factory_port_type():
    """El seam de inyección está tipado contra el port, no el adaptador."""
    assert isinstance(data_access._storage_factory, StorageFactoryPort)


def test_composition_root_builds_storage_factory_port():
    """El composition root devuelve una implementación del port (duck-typed)."""
    factory = build_storage_factory()
    assert isinstance(factory, StorageFactoryPort)


def test_feature_reader_fake_satisfies_port_protocol():
    """El fake de features satisface FeatureReaderPort (runtime_checkable)."""
    fake = _FakeFeatureReader()
    assert isinstance(fake, FeatureReaderPort)
