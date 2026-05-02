"""
Tests unitarios para dagster_assets.bronze_ohlcv.make_bronze_ohlcv_asset.

Principios:
  OCP      : factory extensible por exchange/market_type sin modificar código
  DIP      : RuntimeContext mockeado completo — no depende de RunConfig real
  Fail-fast: RuntimeError en pipeline completamente fallido
  Fail-soft: PARTIAL_SUCCESS emite Output en lugar de lanzar
"""

from unittest.mock import MagicMock, patch

import pytest

from dagster_assets.bronze_ohlcv import make_bronze_ohlcv_asset


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_runtime_ctx(run_id: str = "test-run-001", env: str = "test") -> MagicMock:
    """RuntimeContext mínimo funcional para tests."""
    run_cfg = MagicMock()
    run_cfg.env           = env
    run_cfg.run_id        = run_id
    run_cfg.pushgateway   = "localhost:9091"
    run_cfg.debug         = False
    run_cfg.validate_only = False
    run_cfg.to_dict       = lambda: {}

    ctx            = MagicMock()
    ctx.run_config = run_cfg
    ctx.app_config = MagicMock()
    ctx.run_id = run_id
    return ctx


def _make_ocm_resource(run_id: str = "test-run-001") -> MagicMock:
    """OCMResource que devuelve un RuntimeContext mínimo."""
    ocm = MagicMock()
    ctx = _make_runtime_ctx(run_id=run_id)
    ocm.build_runtime_context.return_value = ctx
    # El asset usa ocm.runtime_context (propiedad), no build_runtime_context()
    ocm.runtime_context = ctx
    return ocm


def _make_summary(*, total_rows: int, ok: int, failed: int, status: str) -> MagicMock:
    s = MagicMock()
    s.total_rows = total_rows
    s.ok         = ok
    s.failed     = failed
    s.status     = status
    return s


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestMakeBronzeOhlcvAsset:

    def test_asset_raises_on_complete_pipeline_failure(self):
        """Si OHLCVPipeline falla completamente, el asset lanza RuntimeError."""
        asset_fn = make_bronze_ohlcv_asset("bybit", "spot")
        ocm      = _make_ocm_resource()
        context  = MagicMock()
        context.log = MagicMock()

        summary = _make_summary(total_rows=0, ok=0, failed=1, status="failed")

        with patch("dagster_assets.bronze_ohlcv.CCXTAdapter"), \
             patch("dagster_assets.bronze_ohlcv.OHLCVPipeline") as mock_pipeline, \
             patch("dagster_assets.bronze_ohlcv.asyncio.run") as mock_run:

            mock_pipeline.return_value.run = MagicMock(return_value=summary)  # asyncio.run mockeado — no se awaita
            mock_run.side_effect = [summary, None]

            with pytest.raises(RuntimeError, match="failed completely"):
                list(asset_fn(context=context, ocm=ocm))

    def test_asset_yields_output_on_success(self):
        """En camino feliz el asset emite Output con metadata correcta."""
        asset_fn = make_bronze_ohlcv_asset("bybit", "spot")
        ocm      = _make_ocm_resource(run_id="test-run-001")
        context  = MagicMock()
        context.log = MagicMock()
        # El asset accede a ocm vía context.resources — wiring explícito
        context.resources.ocm = ocm

        summary = _make_summary(total_rows=42, ok=1, failed=0, status="ok")

        with patch("dagster_assets.bronze_ohlcv.CCXTAdapter"), \
             patch("dagster_assets.bronze_ohlcv.OHLCVPipeline") as mock_pipeline, \
             patch("dagster_assets.bronze_ohlcv.asyncio.run") as mock_run:

            mock_pipeline.return_value.run = MagicMock(return_value=summary)  # asyncio.run mockeado — no se awaita
            mock_run.side_effect = [summary, None]

            outputs = list(asset_fn.op.compute_fn.decorated_fn(context=context, ocm=ocm))

        assert len(outputs) == 1
        val = outputs[0].value
        assert val["rows_written"] == 42
        assert val["exchange"]     == "bybit"
        assert val["status"]       == "ok"
        assert val["run_id"]       == "test-run-001"

    def test_asset_yields_output_on_partial_success(self):
        """PARTIAL_SUCCESS emite Output (fail-soft) en lugar de lanzar."""
        asset_fn = make_bronze_ohlcv_asset("kucoin", "spot")
        ocm      = _make_ocm_resource()
        context  = MagicMock()
        context.log = MagicMock()

        summary = _make_summary(total_rows=10, ok=1, failed=1, status="partial")

        with patch("dagster_assets.bronze_ohlcv.CCXTAdapter"), \
             patch("dagster_assets.bronze_ohlcv.OHLCVPipeline") as mock_pipeline, \
             patch("dagster_assets.bronze_ohlcv.asyncio.run") as mock_run:

            mock_pipeline.return_value.run = MagicMock(return_value=summary)  # asyncio.run mockeado — no se awaita
            mock_run.side_effect = [summary, None]

            outputs = list(asset_fn.op.compute_fn.decorated_fn(context=context, ocm=ocm))

        assert len(outputs) == 1
        assert outputs[0].value["status"] == "partial"
