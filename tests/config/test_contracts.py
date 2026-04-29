# ==============================================================================
# OrangeCashMachine — Config Contract Tests
# ==============================================================================
#
# PROPÓSITO:
#   "given env X → config must satisfy contract Y"
#   Previenen config drift silencioso entre refactors.
#
# FILOSOFÍA:
#   No son unit tests de lógica — son tests de contrato de sistema.
#   Si un contrato rompe, o el cambio es incorrecto (revertir)
#   o el contrato evolucionó intencionalmente (actualizar test).
#
# EJECUCIÓN:
#   uv run pytest tests/config/test_contracts.py -v
# ==============================================================================

from __future__ import annotations

from pathlib import Path

import pytest
from hydra import compose, initialize_config_dir
from hydra.core.config_store import ConfigStore
from hydra.core.global_hydra import GlobalHydra
from omegaconf import OmegaConf


# SSOT: ruta absoluta calculada desde __file__ — independiente del CWD.
# initialize_config_dir() requiere ruta absoluta desde Hydra 1.2+.
_CONFIG_DIR = str(Path(__file__).parent.parent.parent / "config")


def _register_structured_configs() -> None:
    """Registra Structured Configs en el ConfigStore de Hydra.

    SSOT de registro: replicado desde app/cli/market_data.py.
    Necesario en tests porque compose() no pasa por el entrypoint
    de la aplicación — el ConfigStore estaría vacío sin esta llamada.

    Idempotente: ConfigStore.store() no lanza si ya existe.
    """
    from ocm_platform.config.structured import (
        PipelineConfig,
        HistoricalConfig,
        ResampleConfig,
        ObservabilityConfig,
    )
    cs = ConfigStore.instance()
    cs.store(group="pipeline",            name="schema", node=PipelineConfig)
    cs.store(group="pipeline/historical", name="schema", node=HistoricalConfig)
    cs.store(group="pipeline/resample",   name="schema", node=ResampleConfig)
    cs.store(group="observability",       name="schema", node=ObservabilityConfig)


@pytest.fixture(autouse=True)
def reset_hydra():
    """Hydra es singleton — limpiar entre tests para evitar contaminación."""
    GlobalHydra.instance().clear()
    _register_structured_configs()   # registrar ANTES de cada compose()
    yield
    GlobalHydra.instance().clear()


def _load_config(env: str, overrides: list[str] | None = None) -> dict:
    """Carga config para un entorno dado y retorna plain dict.

    resolve=False — no depende de env vars en el entorno de CI.
    Los tests de contrato verifican estructura y valores literales YAML,
    no valores resueltos en runtime.
    """
    with initialize_config_dir(config_dir=_CONFIG_DIR, version_base="1.3"):
        cfg = compose(
            config_name="config",
            overrides=[f"env={env}"] + (overrides or []),
        )
        return OmegaConf.to_container(cfg, resolve=False)


class TestDevelopmentContracts:
    """Contratos del entorno development."""

    def test_dry_run_is_true_by_default(self):
        """En development, dry_run debe ser True por defecto (SafeOps)."""
        cfg = _load_config("development")
        assert cfg["safety"]["dry_run"] is True, (
            "CONTRATO ROTO: safety.dry_run debe ser True en development. "
            "base.yaml define este default seguro."
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
        """Contrato de consistencia resample↔historical (SSOT cross-field)."""
        cfg       = _load_config("development")
        source    = cfg["pipeline"]["resample"]["source_tf"]
        available = cfg["pipeline"]["historical"]["timeframes"]
        assert source in available, (
            f"CONTRATO ROTO: resample.source_tf={source!r} "
            f"no está en historical.timeframes={available}"
        )

    def test_datalake_default_matches_schema(self):
        """SSOT dual: datalake.yaml default == DataLakeConfig.path en schema.py.

        Si este test rompe significa que uno de los dos fue modificado sin
        actualizar el otro. Ambos deben ser idénticos para que paths.py
        sea predecible sin OCM_DATA_LAKE_PATH seteado.
        """
        from ocm_platform.config.schema import DataLakeConfig
        schema_default = DataLakeConfig().path

        cfg       = _load_config("development")
        yaml_path = str(cfg["storage"]["data_lake"]["path"])

        # Extraer el default de la interpolación ${oc.env:VAR,default} si está sin resolver
        if "," in yaml_path and yaml_path.endswith("}"):
            yaml_default = yaml_path.split(",", 1)[-1].rstrip("}")
        else:
            yaml_default = yaml_path

        assert yaml_default == schema_default, (
            f"CONTRATO ROTO: datalake.yaml default={yaml_default!r} "
            f"!= DataLakeConfig.path={schema_default!r}. "
            f"Actualizar ambos para mantener SSOT."
        )


class TestProductionContracts:
    """Contratos del entorno production."""

    def test_dry_run_is_false(self):
        """En production, dry_run DEBE ser False — escrituras reales activas."""
        cfg = _load_config("production")
        assert cfg["safety"]["dry_run"] is False, (
            "CONTRATO ROTO: safety.dry_run debe ser False en production."
        )

    def test_debug_is_false(self):
        cfg = _load_config("production")
        assert cfg["environment"]["debug"] is False

    def test_prevent_full_reingestion(self):
        """SafeOps en prod: protección contra reingesta masiva accidental."""
        cfg = _load_config("production")
        assert cfg["safety"]["prevent_full_reingestion"] is True

    def test_require_confirmation(self):
        """Prod exige confirmación explícita para operaciones destructivas."""
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

    def test_redis_enabled_is_bool_or_interpolation(self):
        """redis.enabled debe ser bool False o interpolación que resuelve a false.

        En test no debe requerir Redis real en CI.
        Acepta el literal False o la interpolación OmegaConf sin resolver.
        """
        cfg           = _load_config("test")
        redis_enabled = cfg["integrations"]["redis"]["enabled"]

        if isinstance(redis_enabled, bool):
            assert redis_enabled is False, (
                "CONTRATO ROTO: redis.enabled debe ser False en test."
            )
        else:
            raw = str(redis_enabled)
            assert "false" in raw.lower() or raw == "0", (
                f"CONTRATO ROTO: redis.enabled interpolation sin default=false: {raw!r}"
            )


class TestCLIOverrideContracts:
    """Contratos de overrides via CLI."""

    def test_cli_can_override_dry_run(self):
        """El usuario puede desactivar dry_run explícitamente via CLI."""
        cfg = _load_config("development", overrides=["safety.dry_run=false"])
        assert cfg["safety"]["dry_run"] is False

    def test_cli_can_enable_backfill(self):
        cfg = _load_config("development", overrides=["pipeline.historical.backfill_mode=true"])
        assert cfg["pipeline"]["historical"]["backfill_mode"] is True
