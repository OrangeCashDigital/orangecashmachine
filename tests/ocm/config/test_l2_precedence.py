"""
tests/ocm/config/test_l2_precedence.py
=======================================

Contrato L2 — apply_env_overrides.

Propiedades verificadas:
  P1  OCM_SECTION__KEY gana sobre el valor resuelto en L1 para la misma clave.
  P2  Clave malformada (sin __) → skip silencioso, no crash (fail-soft en L2).
  P3  Path de tres niveles: OCM_A__B__C → a.b.c correctamente.
  P4  Sin vars OCM_* → retorna (cfg_original, 0) sin modificar el contenido.
  P5  Coerción bool: solo BOOL_TRUE ∪ BOOL_FALSE — numérico/ambiguo pasa como str.
  P6  El conteo es de hojas del dict anidado, no de variables OCM_* vistas.
  P7  El DictConfig de entrada NO es mutado — retorna nuevo objeto.

Principios: SSOT (coerce_string), fail-soft (malformed keys), inmutabilidad.
"""
from __future__ import annotations

import pytest
from omegaconf import OmegaConf

from ocm.config.layers.env_override import apply_env_overrides


# ── Fixture base ───────────────────────────────────────────────────────────────

@pytest.fixture
def base_cfg():
    """DictConfig L1 con valores que L2 puede sobreescribir."""
    return OmegaConf.create({
        "integrations": {
            "redis": {"host": "l1-host", "port": 6379, "enabled": True},
        },
        "safety": {"dry_run": False},
        "section": {"key": "l1-value"},
    })


# ── P1: precedencia ───────────────────────────────────────────────────────────

def test_ocm_override_wins_over_l1_value(base_cfg):
    """OCM_SECTION__KEY sobreescribe el valor ya resuelto por Hydra en L1."""
    merged, count = apply_env_overrides(
        base_cfg, env={"OCM_INTEGRATIONS__REDIS__HOST": "l2-host"}
    )
    assert merged.integrations.redis.host == "l2-host"
    assert count == 1


def test_unoverridden_l1_values_preserved(base_cfg):
    """Claves sin OCM_* correspondiente quedan intactas tras el override."""
    merged, _ = apply_env_overrides(
        base_cfg, env={"OCM_INTEGRATIONS__REDIS__HOST": "new"}
    )
    assert merged.integrations.redis.port == 6379   # intacto
    assert merged.integrations.redis.enabled is True  # intacto


# ── P2: fail-soft en claves malformadas ───────────────────────────────────────

def test_malformed_key_no_separator_is_skipped(base_cfg):
    """OCM_FOO (sin __) → skip silencioso; el override válido se aplica igual."""
    merged, count = apply_env_overrides(
        base_cfg,
        env={
            "OCM_MALFORMED":          "ignored",
            "OCM_SAFETY__DRY_RUN":    "true",
        },
    )
    assert merged.safety.dry_run is True
    assert count == 1   # solo el override válido cuenta


def test_all_malformed_returns_zero_count(base_cfg):
    """Todos malformados → (cfg_original, 0)."""
    _, count = apply_env_overrides(
        base_cfg, env={"OCM_A": "x", "OCM_B": "y"}
    )
    assert count == 0


# ── P3: path de tres niveles ──────────────────────────────────────────────────

def test_three_level_path_parsed_correctly(base_cfg):
    """OCM_INTEGRATIONS__REDIS__ENABLED → integrations.redis.enabled."""
    merged, count = apply_env_overrides(
        base_cfg, env={"OCM_INTEGRATIONS__REDIS__ENABLED": "false"}
    )
    assert merged.integrations.redis.enabled is False
    assert count == 1


def test_two_level_path_parsed_correctly(base_cfg):
    """OCM_SAFETY__DRY_RUN → safety.dry_run (dos niveles)."""
    merged, _ = apply_env_overrides(
        base_cfg, env={"OCM_SAFETY__DRY_RUN": "true"}
    )
    assert merged.safety.dry_run is True


# ── P4: env vacío → sin cambios ───────────────────────────────────────────────

def test_empty_env_returns_count_zero(base_cfg):
    """Sin vars OCM_* el conteo es 0."""
    _, count = apply_env_overrides(base_cfg, env={})
    assert count == 0


def test_empty_env_content_identical(base_cfg):
    """Sin vars OCM_* el contenido del DictConfig no cambia."""
    merged, _ = apply_env_overrides(base_cfg, env={})
    assert OmegaConf.to_container(merged) == OmegaConf.to_container(base_cfg)


# ── P5: coerción bool — SSOT desde coercion.py ────────────────────────────────

@pytest.mark.parametrize("raw,expected", [
    ("true",  True),  ("True",  True),  ("TRUE",  True),
    ("yes",   True),  ("on",    True),
    ("false", False), ("False", False), ("FALSE", False),
    ("no",    False), ("off",   False),
])
def test_bool_recognized_values_coerced(base_cfg, raw, expected):
    """Todos los valores en BOOL_TRUE ∪ BOOL_FALSE se convierten a bool."""
    merged, _ = apply_env_overrides(
        base_cfg, env={"OCM_SAFETY__DRY_RUN": raw}
    )
    assert merged.safety.dry_run is expected


@pytest.mark.parametrize("raw", ["6379", "0", "1", "3.14", "101", "maybe"])
def test_non_bool_strings_pass_through_as_str(base_cfg, raw):
    """Strings que no son bool semántico inequívoco llegan sin modificar a Pydantic L4."""
    cfg = OmegaConf.create({"section": {"key": "original"}})
    merged, _ = apply_env_overrides(cfg, env={"OCM_SECTION__KEY": raw})
    # El valor debe ser el string original — L4 (Pydantic) decide el tipo final
    assert merged.section.key == raw
    assert isinstance(OmegaConf.to_container(merged)["section"]["key"], str)


def test_ambiguous_numeric_zero_not_bool(base_cfg):
    """"0" no debe convertirse a False — es ambiguo sin conocer el tipo destino."""
    cfg = OmegaConf.create({"section": {"key": "placeholder"}})
    merged, _ = apply_env_overrides(cfg, env={"OCM_SECTION__KEY": "0"})
    assert merged.section.key == "0"


# ── P6: conteo exacto de hojas ────────────────────────────────────────────────

def test_count_is_leaf_count_not_ocm_key_count(base_cfg):
    """El conteo refleja hojas del dict anidado — no variables OCM_* vistas."""
    merged, count = apply_env_overrides(
        base_cfg,
        env={
            "OCM_SAFETY__DRY_RUN":             "true",
            "OCM_INTEGRATIONS__REDIS__HOST":   "h",
            "OCM_INTEGRATIONS__REDIS__ENABLED": "false",
        },
    )
    assert count == 3


# ── P7: inmutabilidad del input ───────────────────────────────────────────────

def test_input_dictconfig_not_mutated(base_cfg):
    """apply_env_overrides retorna un NUEVO DictConfig; el input queda intacto."""
    original = OmegaConf.to_container(base_cfg, resolve=True)
    apply_env_overrides(
        base_cfg, env={"OCM_INTEGRATIONS__REDIS__HOST": "mutated?"}
    )
    assert OmegaConf.to_container(base_cfg, resolve=True) == original
