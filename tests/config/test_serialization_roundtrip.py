# ==============================================================================
# OrangeCashMachine — RuntimeContext Serialization Round-Trip Tests
# ==============================================================================
#
# PROPÓSITO:
#   Garantizar que RuntimeContext.to_dict() → AppConfig.model_validate()
#   funciona sin pérdida ni error para todos los tipos presentes en AppConfig,
#   incluyendo SecretStr, Optional, Enum y tipos anidados.
#
#   Si este test rompe tras un cambio en AppConfig o RunConfig:
#   - O el cambio es incorrecto (revertir)
#   - O el contrato de serialización evolucionó (actualizar to_dict/from_dict)
#
# FILOSOFÍA:
#   No es un test de lógica de negocio — es un test de contrato de transporte.
#   Prefect serializa RuntimeContext como parámetro JSON al registrar el
#   deployment y al pasar el contexto al Worker. Cualquier tipo no serializable
#   rompe el deployment en producción sin advertencia en CI.
#
# COBERTURA:
#   - SecretStr → str (model_dump mode="json")
#   - Optional[X] → None o valor plano
#   - round-trip: to_dict() → from_dict() → equivalencia semántica
#   - JSON-safety: el dict resultante es serializable con json.dumps estándar
#
# EJECUCIÓN:
#   uv run pytest tests/config/test_serialization_roundtrip.py -v
# ==============================================================================

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from ocm_platform.config.hydra_loader import load_appconfig_standalone
from ocm_platform.runtime.context import RuntimeContext
from ocm_platform.runtime.run_config import RunConfig


# ==============================================================================
# Fixtures
# ==============================================================================

_CONFIG_DIR = Path(__file__).parent.parent.parent / "config"


@pytest.fixture(scope="module")
def runtime_context() -> RuntimeContext:
    """RuntimeContext canónico construido con load_appconfig_standalone.

    Usa scope=module — la carga de config es costosa y el objeto es inmutable.
    Mismo patrón que usa _build_deploy_context() en deploy.py.
    """
    run_cfg = RunConfig.from_env()
    app_config = load_appconfig_standalone(
        env="test",
        config_dir=_CONFIG_DIR,
        run_id=run_cfg.run_id,
        write_snapshot=False,
    )
    return RuntimeContext(
        app_config=app_config,
        run_config=run_cfg,
        started_at=datetime.now(timezone.utc),
    )


@pytest.fixture(scope="module")
def serialized(runtime_context: RuntimeContext) -> dict:
    """Dict producido por to_dict() — entrada del round-trip."""
    return runtime_context.to_dict()


# ==============================================================================
# Tests de JSON-safety
# ==============================================================================

class TestJsonSafety:
    """El dict producido por to_dict() debe ser serializable con json.dumps.

    Dagster serializa RuntimeContext como JSON al construir OCMResource.
    json.dumps estándar es el mínimo garantizable en CI sin dependencias
    de orjson u otros serializadores custom.
    """

    def test_to_dict_is_json_serializable(self, serialized: dict) -> None:
        """CONTRATO: to_dict() no debe contener tipos no serializables.

        Falla si hay SecretStr, datetime sin isoformat, Path, Enum u otros
        objetos Python no primitivos en el dict resultante.
        """
        try:
            json.dumps(serialized)
        except TypeError as exc:
            pytest.fail(
                f"CONTRATO ROTO: RuntimeContext.to_dict() no es JSON-serializable.\n"
                f"Tipo problemático: {exc}\n"
                f"Actualizar to_dict() o el campo que introduce el tipo."
            )

    def test_app_config_section_is_dict(self, serialized: dict) -> None:
        """app_config debe ser un dict plano, no un objeto Pydantic."""
        assert isinstance(serialized["app_config"], dict), (
            "CONTRATO ROTO: app_config debe ser dict tras model_dump(mode='json'). "
            "Verificar que to_dict() usa mode='json'."
        )

    def test_run_config_section_is_dict(self, serialized: dict) -> None:
        """run_config debe ser un dict plano, no un objeto RunConfig."""
        assert isinstance(serialized["run_config"], dict), (
            "CONTRATO ROTO: run_config debe ser dict tras RunConfig.to_dict()."
        )

    def test_started_at_is_iso_string(self, serialized: dict) -> None:
        """started_at debe ser string ISO 8601 con timezone."""
        started_at = serialized["started_at"]
        assert isinstance(started_at, str), (
            f"CONTRATO ROTO: started_at debe ser str ISO 8601, got {type(started_at)}"
        )
        # Fail-Fast: parseable y timezone-aware
        parsed = datetime.fromisoformat(started_at)
        assert parsed.tzinfo is not None, (
            "CONTRATO ROTO: started_at debe incluir timezone (tzinfo != None)."
        )

    def test_no_secret_str_in_serialized(self, serialized: dict) -> None:
        """Ningún valor en el dict debe ser SecretStr.

        Recorre recursivamente el dict para detectar SecretStr en cualquier
        nivel de anidamiento — incluyendo credenciales de exchanges.
        """
        from pydantic import SecretStr

        def _find_secret_str(obj: object, path: str = "") -> list[str]:
            found: list[str] = []
            if isinstance(obj, SecretStr):
                found.append(path or "<root>")
            elif isinstance(obj, dict):
                for k, v in obj.items():
                    found.extend(_find_secret_str(v, f"{path}.{k}" if path else k))
            elif isinstance(obj, list):
                for i, v in enumerate(obj):
                    found.extend(_find_secret_str(v, f"{path}[{i}]"))
            return found

        violations = _find_secret_str(serialized)
        assert not violations, (
            f"CONTRATO ROTO: SecretStr encontrado en to_dict() en paths: {violations}.\n"
            f"Verificar que model_dump usa mode='json' y RunConfig.to_dict() "
            f"llama .get_secret_value() en todos los campos SecretStr."
        )


# ==============================================================================
# Tests de round-trip
# ==============================================================================

class TestRoundTrip:
    """to_dict() → from_dict() debe producir un RuntimeContext equivalente.

    No compara objetos directamente (AppConfig no implementa __eq__ profundo)
    — compara campos semánticos clave que deben sobrevivir el ciclo.
    """

    @pytest.fixture(scope="class")
    def restored(self, serialized: dict) -> RuntimeContext:
        """RuntimeContext reconstruido desde el dict serializado."""
        return RuntimeContext.from_dict(serialized)

    def test_environment_survives_roundtrip(
        self, runtime_context: RuntimeContext, restored: RuntimeContext
    ) -> None:
        """El entorno debe ser idéntico tras el round-trip."""
        assert restored.environment == runtime_context.environment, (
            f"CONTRATO ROTO: environment={runtime_context.environment!r} "
            f"→ {restored.environment!r} tras round-trip."
        )

    def test_run_id_survives_roundtrip(
        self, runtime_context: RuntimeContext, restored: RuntimeContext
    ) -> None:
        """run_id es el identificador de correlación — debe ser idéntico."""
        assert restored.run_id == runtime_context.run_id, (
            f"CONTRATO ROTO: run_id={runtime_context.run_id!r} "
            f"→ {restored.run_id!r} tras round-trip."
        )

    def test_started_at_survives_roundtrip(
        self, runtime_context: RuntimeContext, restored: RuntimeContext
    ) -> None:
        """started_at debe ser timezone-aware y equivalente tras round-trip."""
        assert restored.started_at == runtime_context.started_at, (
            f"CONTRATO ROTO: started_at no sobrevive round-trip. "
            f"Original={runtime_context.started_at!r} "
            f"Restored={restored.started_at!r}."
        )

    def test_dry_run_survives_roundtrip(
        self, runtime_context: RuntimeContext, restored: RuntimeContext
    ) -> None:
        """dry_run es crítico para SafeOps — debe sobrevivir intacto."""
        original = runtime_context.app_config.safety.dry_run
        restored_val = restored.app_config.safety.dry_run
        assert restored_val == original, (
            f"CONTRATO ROTO: safety.dry_run={original!r} "
            f"→ {restored_val!r} tras round-trip."
        )

    def test_exchange_names_survive_roundtrip(
        self, runtime_context: RuntimeContext, restored: RuntimeContext
    ) -> None:
        """Los exchanges configurados deben ser los mismos tras round-trip."""
        original_names = sorted(runtime_context.app_config.exchange_names)
        restored_names = sorted(restored.app_config.exchange_names)
        assert restored_names == original_names, (
            f"CONTRATO ROTO: exchanges={original_names!r} "
            f"→ {restored_names!r} tras round-trip."
        )

    def test_app_config_datasets_survive_roundtrip(
        self, runtime_context: RuntimeContext, restored: RuntimeContext
    ) -> None:
        """Los datasets activos deben ser los mismos tras round-trip."""
        original = sorted(runtime_context.app_config.datasets.active_datasets)
        restored_val = sorted(restored.app_config.datasets.active_datasets)
        assert restored_val == original, (
            f"CONTRATO ROTO: active_datasets={original!r} "
            f"→ {restored_val!r} tras round-trip."
        )
