"""
Tests unitarios para dagster_assets.resources.OCMResource.

Principios:
  DIP  : RuntimeContext mockeado completo — el test no depende de su implementación
  SSOT : OCMResource es el único punto de acceso al RuntimeContext en Dagster
  Fail-fast : assert sobre call_count y call_args garantiza contratos de interfaz
"""

from unittest.mock import MagicMock, patch

import pytest

from dagster_assets.resources import OCMResource


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_run_mock(env: str = "test", run_id: str = "test-run-001") -> MagicMock:
    return MagicMock(
        env=env,
        run_id=run_id,
        pushgateway="localhost:9091",
        debug=True,
        validate_only=False,
        to_dict=lambda: {},
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestOCMResource:

    def test_explicit_env_passed_to_run_config(self):
        """env='test' se reenvía como explicit_env a RunConfig.from_env()."""
        resource = OCMResource(env="test")

        with patch("dagster_assets.resources.RunConfig.from_env") as mock_run, \
             patch("dagster_assets.resources.load_appconfig_standalone") as mock_cfg, \
             patch("dagster_assets.resources.RuntimeContext") as mock_ctx_cls:

            mock_run.return_value   = _make_run_mock(env="test")
            mock_cfg.return_value   = MagicMock()
            mock_ctx_cls.return_value = MagicMock()

            ctx = resource.build_runtime_context()

            assert ctx is not None
            mock_run.assert_called_once_with(explicit_env="test")
            mock_cfg.assert_called_once()

    def test_empty_env_delegates_to_run_config(self):
        """env='' delega la resolución a RunConfig.from_env() (lee OCM_ENV)."""
        resource = OCMResource(env="")

        with patch("dagster_assets.resources.RunConfig.from_env") as mock_run, \
             patch("dagster_assets.resources.load_appconfig_standalone") as mock_cfg, \
             patch("dagster_assets.resources.RuntimeContext") as mock_ctx_cls:

            mock_run.return_value     = _make_run_mock(env="development", run_id="xyz")
            mock_cfg.return_value     = MagicMock()
            mock_ctx_cls.return_value = MagicMock()

            resource.build_runtime_context()

            # env="" → explicit_env=None (sin override)
            mock_run.assert_called_once_with(explicit_env=None)

    def test_runtime_context_builds_fresh_each_call(self):
        """build_runtime_context() genera contexto fresco por llamada (no cacheado)."""
        resource = OCMResource(env="test")

        with patch("dagster_assets.resources.RunConfig.from_env") as mock_run, \
             patch("dagster_assets.resources.load_appconfig_standalone") as mock_cfg, \
             patch("dagster_assets.resources.RuntimeContext") as mock_ctx_cls:

            mock_run.side_effect = [
                _make_run_mock(run_id="id1"),
                _make_run_mock(run_id="id2"),
            ]
            mock_cfg.return_value     = MagicMock()
            mock_ctx_cls.return_value = MagicMock()

            resource.build_runtime_context()
            resource.build_runtime_context()

            # Cada llamada construye un contexto fresco — sin caché implícito
            assert mock_run.call_count == 2
