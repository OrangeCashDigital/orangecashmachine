"""
Tests unitarios para infrastructure.dagster.assets.bronze_ohlcv.make_bronze_ohlcv_asset.

Patch target correcto: "infrastructure.dagster.assets.bronze_ohlcv.PipelineOrchestrator"
— el asset delega a PipelineOrchestrator (DIP), no construye CCXTAdapter
ni OHLCVPipeline directamente.

Principios:
  OCP      : factory extensible por exchange/market_type sin modificar código
  DIP      : RuntimeContext mockeado completo — no depende de RunConfig real
  Fail-fast: RuntimeError en pipeline completamente fallido
  Fail-soft: status="partial" emite Output en lugar de lanzar
"""

from unittest.mock import MagicMock, patch

import pytest

from infrastructure.dagster.assets.bronze_ohlcv import make_bronze_ohlcv_asset


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_runtime_ctx(run_id: str = "test-run-001") -> MagicMock:
    """RuntimeContext mínimo funcional para tests."""
    ctx            = MagicMock()
    ctx.run_id     = run_id
    ctx.app_config = MagicMock()
    return ctx


def _make_ocm(run_id: str = "test-run-001") -> MagicMock:
    """OCMResource que expone runtime_context ya construido."""
    ocm = MagicMock()
    ctx = _make_runtime_ctx(run_id=run_id)
    # bronze_ohlcv usa ocm.runtime_context (propiedad), no build_runtime_context()
    ocm.runtime_context = ctx
    return ocm


def _make_summary(
    *,
    total_rows: int,
    ok:         int,
    failed:     int,
    status:     str,
    duration_ms: int = 100,
) -> MagicMock:
    s              = MagicMock()
    s.total_rows   = total_rows
    s.ok           = ok
    s.failed       = failed
    s.status       = status
    s.duration_ms  = duration_ms
    return s


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestMakeBronzeOhlcvAsset:

    def test_asset_raises_on_complete_pipeline_failure(self):
        """Si PipelineOrchestrator.run() retorna status=failed, el asset lanza."""
        asset_fn = make_bronze_ohlcv_asset("bybit", "spot")
        ocm      = _make_ocm()
        context  = MagicMock()
        context.log = MagicMock()

        summary = _make_summary(total_rows=0, ok=0, failed=1, status="failed")

        with patch(
            "infrastructure.dagster.assets.bronze_ohlcv.PipelineOrchestrator",
        ) as MockOrchestrator, patch(
            "infrastructure.dagster.assets.bronze_ohlcv.asyncio.run",
            return_value=summary,
        ):
            MockOrchestrator.return_value = MagicMock()
            with pytest.raises(RuntimeError):
                list(asset_fn.op.compute_fn.decorated_fn(context=context, ocm=ocm))

    def test_asset_yields_output_on_success(self):
        """En camino feliz el asset emite Output con metadata correcta."""
        asset_fn = make_bronze_ohlcv_asset("bybit", "spot")
        ocm      = _make_ocm(run_id="test-run-001")
        context  = MagicMock()
        context.log = MagicMock()

        summary = _make_summary(total_rows=42, ok=1, failed=0, status="ok")

        with patch(
            "infrastructure.dagster.assets.bronze_ohlcv.PipelineOrchestrator",
        ) as MockOrchestrator, patch(
            "infrastructure.dagster.assets.bronze_ohlcv.asyncio.run",
            return_value=summary,
        ):
            MockOrchestrator.return_value = MagicMock()
            outputs = list(
                asset_fn.op.compute_fn.decorated_fn(context=context, ocm=ocm)
            )

        assert len(outputs) == 1
        val = outputs[0].value
        assert val["rows_written"] == 42
        assert val["exchange"]     == "bybit"
        assert val["status"]       == "ok"
        assert val["run_id"]       == "test-run-001"

    def test_asset_yields_output_on_partial_success(self):
        """status=partial emite Output (fail-soft) en lugar de lanzar."""
        asset_fn = make_bronze_ohlcv_asset("kucoin", "spot")
        ocm      = _make_ocm()
        context  = MagicMock()
        context.log = MagicMock()

        summary = _make_summary(total_rows=10, ok=1, failed=1, status="partial")

        with patch(
            "infrastructure.dagster.assets.bronze_ohlcv.PipelineOrchestrator",
        ) as MockOrchestrator, patch(
            "infrastructure.dagster.assets.bronze_ohlcv.asyncio.run",
            return_value=summary,
        ):
            MockOrchestrator.return_value = MagicMock()
            outputs = list(
                asset_fn.op.compute_fn.decorated_fn(context=context, ocm=ocm)
            )

        assert len(outputs) == 1
        assert outputs[0].value["status"] == "partial"
