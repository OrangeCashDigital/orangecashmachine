# ==============================================================================
# OrangeCashMachine — Config Contract Tests
# ==============================================================================
#
# PROPÓSITO:
#   "given env X → config must equal snapshot Y"
#   Previenen config drift silencioso entre refactors.
#
# FILOSOFÍA:
#   No son unit tests de lógica — son tests de contrato de sistema.
#   Si un contrato rompe, o el cambio es incorrecto (revertir)
#   o el contrato evolucionó intencionalmente (actualizar snapshot).
#
# EJECUCIÓN:
#   uv run pytest tests/config/test_contracts.py -v
#   uv run pytest tests/config/test_contracts.py --snapshot-update  # actualizar snapshots
# ==============================================================================

import pytest
from pathlib import Path
from hydra import compose, initialize_config_dir
from hydra.core.global_hydra import GlobalHydra
from omegaconf import OmegaConf
import os


@pytest.fixture(autouse=True)
def reset_hydra():
    """Hydra es singleton — limpiar entre tests."""
    GlobalHydra.instance().clear()
    yield
    GlobalHydra.instance().clear()


# SSOT: ruta absoluta calculada desde __file__ — independiente del CWD.
# initialize_config_dir() requiere ruta absoluta desde Hydra 1.2+.
_CONFIG_DIR = str(Path(__file__).parent.parent.parent / "config")


def _load_config(env: str, overrides: list[str] | None = None) -> dict:
    """Helper: carga config para un entorno dado y retorna plain dict."""
    with initialize_config_dir(config_dir=_CONFIG_DIR, version_base="1.3"):
        cfg = compose(
            config_name="config",
            overrides=[f"env={env}"] + (overrides or []),
        )
        return OmegaConf.to_container(cfg, resolve=False)  # resolve=False — no depende de env vars


class TestDevelopmentContracts:
    """Contratos del entorno development."""

    def test_dry_run_is_true_by_default(self):
        """En development, dry_run debe ser True por defecto (SafeOps)."""
        cfg = _load_config("development")
        assert cfg["safety"]["dry_run"] is True, (
            "CONTRATO ROTO: safety.dry_run debe ser True en development. "
            "Base.yaml define este default seguro."
        )

    def test_debug_is_enabled(self):
        cfg = _load_config("development")
        assert cfg["environment"]["debug"] is True

    def test_metrics_disabled_in_dev(self):
        """Métricas desactivadas en dev — no exponer Prometheus innecesariamente."""
        cfg = _load_config("development")
        assert cfg["observability"]["metrics"]["enabled"] is False

    def test_historical_timeframes_contains_1m(self):
        """1m debe estar siempre presente — es la fuente del resample pipeline."""
        cfg = _load_config("development")
        assert "1m" in cfg["pipeline"]["historical"]["timeframes"]

    def test_resample_source_tf_in_historical(self):
        """Contrato de consistencia resample↔historical."""
        cfg = _load_config("development")
        source = cfg["pipeline"]["resample"]["source_tf"]
        available = cfg["pipeline"]["historical"]["timeframes"]
        assert source in available, (
            f"CONTRATO ROTO: resample.source_tf={source!r} "
            f"no está en historical.timeframes={available}"
        )


class TestProductionContracts:
    """Contratos del entorno production."""

    def test_dry_run_is_false(self):
        """En production, dry_run DEBE ser False."""
        cfg = _load_config("production")
        assert cfg["safety"]["dry_run"] is False, (
            "CONTRATO ROTO: safety.dry_run debe ser False en production."
        )

    def test_debug_is_false(self):
        cfg = _load_config("production")
        assert cfg["environment"]["debug"] is False

    def test_prevent_full_reingestion(self):
        cfg = _load_config("production")
        assert cfg["safety"]["prevent_full_reingestion"] is True

    def test_require_confirmation(self):
        cfg = _load_config("production")
        assert cfg["safety"]["require_confirmation"] is True

    def test_max_backfill_days_le_90(self):
        cfg = _load_config("production")
        assert cfg["safety"]["max_backfill_days"] <= 90


class TestTestEnvContracts:
    """Contratos del entorno test."""

    def test_dry_run_true(self):
        cfg = _load_config("test")
        assert cfg["safety"]["dry_run"] is True

    def test_metrics_disabled(self):
        cfg = _load_config("test")
        assert cfg["observability"]["metrics"]["enabled"] is False

    def test_redis_disabled_by_default(self):
        cfg = _load_config("test")
        # En test, redis.enabled puede ser string interpolado — verificar el valor YAML
        redis_enabled = cfg["integrations"]["redis"]["enabled"]
        # Acepta False o la interpolación ${oc.env:REDIS_ENABLED,false}
        assert redis_enabled is False or "false" in str(redis_enabled).lower()

    def test_mock_data_enabled(self):
        cfg = _load_config("test")
        assert cfg["testing"]["use_mock_data"] is True


class TestCLIOverrideContracts:
    """Contratos de overrides via CLI."""

    def test_cli_can_override_dry_run(self):
        """El usuario puede desactivar dry_run explícitamente via CLI."""
        cfg = _load_config("development", overrides=["safety.dry_run=false"])
        assert cfg["safety"]["dry_run"] is False

    def test_cli_can_enable_backfill(self):
        cfg = _load_config("development", overrides=["pipeline.historical.backfill_mode=true"])
        assert cfg["pipeline"]["historical"]["backfill_mode"] is True
