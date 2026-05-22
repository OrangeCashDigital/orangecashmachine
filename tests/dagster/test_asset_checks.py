"""
Tests unitarios para infrastructure.dagster.assets.asset_checks.make_bronze_checks.

Patch target correcto: "infrastructure.dagster.assets.asset_checks.IcebergStorageFactory"
— el módulo usa IcebergStorageFactory().get_storage(), no IcebergStorage directamente.

Principios:
  OCP      : make_bronze_checks extensible sin modificar código existente
  DIP      : IcebergStorageFactory, scan_gaps y RuntimeContext mockeados desde fuera
  Fail-fast: check retorna passed=False ante datos inválidos (no lanza)
  Clean Code: helpers _make_ocm / _make_df mantienen los tests DRY
"""

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

from infrastructure.dagster.assets.asset_checks import make_bronze_checks

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ocm() -> MagicMock:
    """OCMResource cuyo RuntimeContext no pasa por __post_init__ real."""
    runtime_ctx = MagicMock()
    runtime_ctx.app_config = MagicMock()
    ocm = MagicMock()
    ocm.build_runtime_context.return_value = runtime_ctx
    return ocm


def _make_df(rows: int = 60) -> pd.DataFrame:
    """DataFrame OHLCV mínimo con timestamps consecutivos."""
    ts = pd.date_range("2024-01-01", periods=rows, freq="1min")
    return pd.DataFrame(
        {
            "timestamp": ts.astype("int64") // 10**6,
            "open": np.random.rand(rows),
            "high": np.random.rand(rows),
            "low": np.random.rand(rows),
            "close": np.random.rand(rows),
            "volume": np.random.rand(rows),
        }
    )


def _mock_factory(storage: MagicMock) -> MagicMock:
    """IcebergStorageFactory mock que retorna storage en get_storage()."""
    factory = MagicMock()
    factory.return_value.get_storage.return_value = storage
    return factory


# ---------------------------------------------------------------------------
# Row count check
# ---------------------------------------------------------------------------


class TestRowCountCheck:
    def test_passes_with_sufficient_rows(self):
        checks = make_bronze_checks("bybit", "spot")
        row_check = next(c for c in checks if "row_count" in list(c.check_specs)[0].name)
        ocm = _make_ocm()
        context = MagicMock()

        import numpy as np
        import pandas as pd

        mock_storage = MagicMock()
        mock_df = pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-01", periods=100, freq="1h", tz="UTC"),
                "open": np.ones(100),
                "high": np.ones(100),
                "low": np.ones(100),
                "close": np.ones(100),
                "volume": np.ones(100),
            }
        )
        mock_storage.load_ohlcv.return_value = mock_df

        with patch.object(ocm, "get_storage", return_value=mock_storage):
            result = row_check.op.compute_fn.decorated_fn(context=context, ocm=ocm)

        assert result.passed is True

    def test_fails_with_zero_rows(self):
        checks = make_bronze_checks("bybit", "spot")
        row_check = next(c for c in checks if "row_count" in list(c.check_specs)[0].name)
        ocm = _make_ocm()
        context = MagicMock()

        mock_storage = MagicMock()
        mock_storage.count.return_value = 0

        with patch.object(ocm, "get_storage", return_value=mock_storage):
            result = row_check.op.compute_fn.decorated_fn(context=context, ocm=ocm)

        assert result.passed is False


# ---------------------------------------------------------------------------
# Freshness check
# ---------------------------------------------------------------------------


class TestFreshnessCheck:
    def test_passes_when_no_data_yet(self):
        """Sin datos todavía → skip (passed=True, sin falso positivo)."""
        checks = make_bronze_checks("bybit", "spot")
        freshness_check = next(c for c in checks if "freshness" in list(c.check_specs)[0].name)
        ocm = _make_ocm()
        context = MagicMock()

        mock_storage = MagicMock()
        mock_storage.get_last_timestamp.return_value = None

        with patch.object(ocm, "get_storage", return_value=mock_storage):
            result = freshness_check.op.compute_fn.decorated_fn(context=context, ocm=ocm)

        assert result.passed is True


# ---------------------------------------------------------------------------
# Gap severity check
# ---------------------------------------------------------------------------


class TestGapCheck:
    def test_passes_when_no_high_gaps(self):
        checks = make_bronze_checks("bybit", "spot")
        gap_check = next(c for c in checks if "gap_severity" in list(c.check_specs)[0].name)
        ocm = _make_ocm()
        context = MagicMock()
        df = _make_df(rows=60)

        mock_storage = MagicMock()
        mock_storage.load_ohlcv.return_value = df

        with (
            patch.object(ocm, "get_storage", return_value=mock_storage),
            patch("infrastructure.dagster.assets.asset_checks.scan_gaps", return_value=[]),
        ):
            result = gap_check.op.compute_fn.decorated_fn(context=context, ocm=ocm)

        assert result.passed is True

    def test_fails_when_high_gaps_detected(self):
        checks = make_bronze_checks("bybit", "spot")
        gap_check = next(c for c in checks if "gap_severity" in list(c.check_specs)[0].name)
        ocm = _make_ocm()
        context = MagicMock()

        from market_data.domain.value_objects.gap_scanner import GapRange

        high_gap = GapRange(start_ms=1_000_000, end_ms=2_000_000, expected=100)
        df = _make_df(rows=10)
        mock_storage = MagicMock()
        mock_storage.load_ohlcv.return_value = df

        with (
            patch.object(ocm, "get_storage", return_value=mock_storage),
            patch(
                "infrastructure.dagster.assets.asset_checks.scan_gaps",
                return_value=[high_gap],
            ),
        ):
            result = gap_check.op.compute_fn.decorated_fn(context=context, ocm=ocm)

        assert result.passed is False
        assert result.metadata["high_gaps"].value == 1

    def test_passes_when_no_data_loaded(self):
        """Sin datos → skip en lugar de falso positivo."""
        checks = make_bronze_checks("bybit", "spot")
        gap_check = next(c for c in checks if "gap_severity" in list(c.check_specs)[0].name)
        ocm = _make_ocm()
        context = MagicMock()

        mock_storage = MagicMock()
        mock_storage.load_ohlcv.return_value = None

        with patch.object(ocm, "get_storage", return_value=mock_storage):
            result = gap_check.op.compute_fn.decorated_fn(context=context, ocm=ocm)

        assert result.passed is True
