# -*- coding: utf-8 -*-
"""tests/app/test_execute_paper.py

Cobertura de app/use_cases/execute_paper.py (issue #220).

Objetivo: 70%+ sin red, sin Iceberg, sin Kafka.
Escenarios:
  · SyntheticDataSource: determinismo, columnas, kwargs
  · _probe_gold_data: error de carga, DF vacío, None, camino OK
  · execute: build error, run_once error, performance error, success
    sin trades, success con trades
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from app.use_cases.execute_paper import (
    SyntheticDataSource,
    _probe_gold_data,
    execute,
)

# ── SyntheticDataSource ─────────────────────────────────────────────


def test_synthetic_data_source_returns_expected_columns() -> None:
    src = SyntheticDataSource()
    df = src.load_features(exchange="bybit", symbol="BTC/USDT", timeframe="1h")
    assert isinstance(df, pl.DataFrame)
    assert len(df) == 100
    for col in (
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vwap",
        "return_1",
    ):
        assert col in df.columns, f"falta columna {col}"


def test_synthetic_data_source_is_reproducible() -> None:
    src = SyntheticDataSource()
    df1 = src.load_features("bybit", "BTC/USDT", "1h")
    df2 = src.load_features("bybit", "BTC/USDT", "1h")
    assert df1.equals(df2), "seed=42 debe producir DF idénticos"


def test_synthetic_data_source_accepts_market_type_and_kwargs() -> None:
    src = SyntheticDataSource()
    df = src.load_features("bybit", "BTC/USDT", "1h", market_type="futures", extra="ignored")
    assert len(df) == 100


# ── _probe_gold_data ────────────────────────────────────────────────


def test_probe_gold_data_raises_on_load_error() -> None:
    src = MagicMock()
    src.load_features.side_effect = ConnectionError("iceberg down")
    with pytest.raises(RuntimeError, match="Gold data unavailable"):
        _probe_gold_data(
            src,
            exchange="bybit",
            symbol="BTC/USDT",
            timeframe="1h",
            market_type="spot",
        )


def test_probe_gold_data_raises_on_empty_polars_dataframe() -> None:
    """Regresión: Polars usa is_empty() (método), no empty (atributo pandas).
    Con dry_run=False, Gold vacío debe fallar-Fast, no producir 0 señales."""
    src = MagicMock()
    src.load_features.return_value = pl.DataFrame()
    with pytest.raises(RuntimeError, match="Gold data empty"):
        _probe_gold_data(
            src,
            exchange="bybit",
            symbol="BTC/USDT",
            timeframe="1h",
            market_type="spot",
        )


def test_probe_gold_data_raises_on_none() -> None:
    src = MagicMock()
    src.load_features.return_value = None
    with pytest.raises(RuntimeError, match="Gold data empty"):
        _probe_gold_data(
            src,
            exchange="bybit",
            symbol="BTC/USDT",
            timeframe="1h",
            market_type="spot",
        )


def test_probe_gold_data_passes_with_rows() -> None:
    src = MagicMock()
    src.load_features.return_value = pl.DataFrame({"x": [1, 2, 3]})
    # No debe lanzar
    _probe_gold_data(
        src,
        exchange="bybit",
        symbol="BTC/USDT",
        timeframe="1h",
        market_type="spot",
    )


# ── execute ─────────────────────────────────────────────────────────


def _fake_runtime(trades=None):
    rt = MagicMock()
    rt.engine.run_once.return_value = MagicMock()
    rt.engine.oms_summary = {"open": 0, "filled": 0, "rejected": 0, "halted": False}
    rt.tracker.closed_trades = trades if trades is not None else []
    rt.tracker.open_positions = {}
    return rt


def _fake_trading():
    t = MagicMock()
    t.capital_usd = 10_000.0
    return t


def test_execute_success_without_trades() -> None:
    rt = _fake_runtime(trades=[])
    with patch("app.use_cases.execute_paper.build_paper_engine", return_value=rt):
        result = execute(
            _fake_trading(),
            MagicMock(),
            MagicMock(),
            dry_run=True,
            min_confidence=0.5,
        )
    assert result.success is True
    assert result.error is None
    assert result.performance is None


def test_execute_success_with_trades_calls_performance() -> None:
    rt = _fake_runtime(trades=[MagicMock()])
    fake_perf = MagicMock()
    with (
        patch("app.use_cases.execute_paper.build_paper_engine", return_value=rt),
        patch(
            "trading.domain.analytics.performance.PerformanceEngine.summarize",
            return_value=fake_perf,
        ),
    ):
        result = execute(
            _fake_trading(),
            MagicMock(),
            MagicMock(),
            dry_run=True,
            min_confidence=0.5,
        )
    assert result.success is True
    assert result.performance is fake_perf


def test_execute_returns_failure_when_build_fails() -> None:
    with patch(
        "app.use_cases.execute_paper.build_paper_engine",
        side_effect=RuntimeError("no gold"),
    ):
        result = execute(
            _fake_trading(),
            MagicMock(),
            MagicMock(),
            dry_run=False,
            min_confidence=0.5,
        )
    assert result.success is False
    assert "no gold" in result.error


def test_execute_returns_failure_when_run_once_fails() -> None:
    rt = _fake_runtime()
    rt.engine.run_once.side_effect = RuntimeError("engine boom")
    with patch("app.use_cases.execute_paper.build_paper_engine", return_value=rt):
        result = execute(
            _fake_trading(),
            MagicMock(),
            MagicMock(),
            dry_run=True,
            min_confidence=0.5,
        )
    assert result.success is False
    assert "engine boom" in result.error


def test_execute_returns_failure_when_performance_fails() -> None:
    rt = _fake_runtime(trades=[MagicMock()])
    with (
        patch("app.use_cases.execute_paper.build_paper_engine", return_value=rt),
        patch(
            "trading.domain.analytics.performance.PerformanceEngine.summarize",
            side_effect=RuntimeError("perf boom"),
        ),
    ):
        result = execute(
            _fake_trading(),
            MagicMock(),
            MagicMock(),
            dry_run=True,
            min_confidence=0.5,
        )
    assert result.success is False
    assert "perf boom" in result.error
