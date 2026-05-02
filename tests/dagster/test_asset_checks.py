"""
Tests unitarios para dagster_assets.asset_checks.make_bronze_checks.

Principios:
  OCP      : make_bronze_checks extensible sin modificar código existente
  DIP      : IcebergStorage, scan_gaps y RuntimeContext mockeados desde fuera
  Fail-fast: check retorna passed=False ante datos inválidos (no lanza)
  Clean Code: helpers _make_ocm / _make_df mantienen los tests DRY
"""

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

from dagster_assets.asset_checks import make_bronze_checks


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ocm() -> MagicMock:
    """OCMResource cuyo RuntimeContext no pasa por __post_init__ real."""
    runtime_ctx            = MagicMock()
    runtime_ctx.app_config = MagicMock()

    ocm = MagicMock()
    ocm.build_runtime_context.return_value = runtime_ctx
    return ocm


def _make_df(rows: int = 60) -> pd.DataFrame:
    """DataFrame OHLCV mínimo con timestamps consecutivos."""
    ts = pd.date_range("2024-01-01", periods=rows, freq="1min")
    return pd.DataFrame({
        "timestamp": ts.astype("int64") // 10**6,
        "open":      np.random.rand(rows),
        "high":      np.random.rand(rows),
        "low":       np.random.rand(rows),
        "close":     np.random.rand(rows),
        "volume":    np.random.rand(rows),
    })


# ---------------------------------------------------------------------------
# Row count check
# ---------------------------------------------------------------------------

class TestRowCountCheck:

    def test_passes_with_sufficient_rows(self):
        checks    = make_bronze_checks("bybit", "spot")
        row_check = next(c for c in checks if "row_count" in list(c.check_specs)[0].name)
        ocm       = _make_ocm()
        context   = MagicMock()

        with patch("dagster_assets.asset_checks.IcebergStorage") as mock_storage:
            mock_storage.return_value.count.return_value = 100
            result = row_check.op.compute_fn.decorated_fn(context=context, ocm=ocm)

        assert result.passed is True

    def test_fails_with_zero_rows(self):
        checks    = make_bronze_checks("bybit", "spot")
        row_check = next(c for c in checks if "row_count" in list(c.check_specs)[0].name)
        ocm       = _make_ocm()
        context   = MagicMock()

        with patch("dagster_assets.asset_checks.IcebergStorage") as mock_storage:
            mock_storage.return_value.count.return_value = 0
            result = row_check.op.compute_fn.decorated_fn(context=context, ocm=ocm)

        assert result.passed is False


# ---------------------------------------------------------------------------
# Freshness check
# ---------------------------------------------------------------------------

class TestFreshnessCheck:

    def test_passes_when_no_data_yet(self):
        """Sin datos todavía → skip (passed=True, sin falso positivo)."""
        checks          = make_bronze_checks("bybit", "spot")
        freshness_check = next(c for c in checks if "freshness" in list(c.check_specs)[0].name)
        ocm             = _make_ocm()
        context         = MagicMock()

        with patch("dagster_assets.asset_checks.IcebergStorage") as mock_storage:
            mock_storage.return_value.get_last_timestamp.return_value = None
            result = freshness_check.op.compute_fn.decorated_fn(context=context, ocm=ocm)

        assert result.passed is True


# ---------------------------------------------------------------------------
# Gap severity check
# ---------------------------------------------------------------------------

class TestGapCheck:

    def test_passes_when_no_high_gaps(self):
        checks    = make_bronze_checks("bybit", "spot")
        gap_check = next(c for c in checks if "gap_severity" in list(c.check_specs)[0].name)
        ocm       = _make_ocm()
        context   = MagicMock()

        df = _make_df(rows=60)

        with patch("dagster_assets.asset_checks.IcebergStorage") as mock_storage, \
             patch("dagster_assets.asset_checks.scan_gaps", return_value=[]):
            mock_storage.return_value.load_ohlcv.return_value = df
            result = gap_check.op.compute_fn.decorated_fn(context=context, ocm=ocm)

        assert result.passed is True

    def test_fails_when_high_gaps_detected(self):
        checks    = make_bronze_checks("bybit", "spot")
        gap_check = next(c for c in checks if "gap_severity" in list(c.check_specs)[0].name)
        ocm       = _make_ocm()
        context   = MagicMock()

        from market_data.processing.utils.gap_utils import GapRange

        high_gap = GapRange(start_ms=1_000_000, end_ms=2_000_000, expected=100)
        df       = _make_df(rows=10)

        with patch("dagster_assets.asset_checks.IcebergStorage") as mock_storage, \
             patch("dagster_assets.asset_checks.scan_gaps", return_value=[high_gap]):
            mock_storage.return_value.load_ohlcv.return_value = df
            result = gap_check.op.compute_fn.decorated_fn(context=context, ocm=ocm)

        assert result.passed is False
        assert result.metadata["high_gaps"].value == 1

    def test_passes_when_no_data_loaded(self):
        """Sin datos → skip en lugar de falso positivo."""
        checks    = make_bronze_checks("bybit", "spot")
        gap_check = next(c for c in checks if "gap_severity" in list(c.check_specs)[0].name)
        ocm       = _make_ocm()
        context   = MagicMock()

        with patch("dagster_assets.asset_checks.IcebergStorage") as mock_storage:
            mock_storage.return_value.load_ohlcv.return_value = None
            result = gap_check.op.compute_fn.decorated_fn(context=context, ocm=ocm)

        assert result.passed is True
