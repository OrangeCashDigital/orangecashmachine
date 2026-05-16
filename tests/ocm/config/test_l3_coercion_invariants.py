"""
tests/ocm/config/test_l3_coercion_invariants.py
=================================================

Contrato L3 — coerce_scalar_values y coerce_string.

Invariantes verificados:
  C1  BOOL_TRUE  → True  (case-insensitive, whitespace-stripped).
  C2  BOOL_FALSE → False (case-insensitive, whitespace-stripped).
  C3  Strings numéricos → sin modificar (Pydantic L4 tiene el tipo destino).
  C4  String vacío "" en _NULLABLE_PATHS → None.
  C5  String vacío "" en _NULLABLE_KEYS (por nombre) → None a cualquier profundidad.
  C6  String vacío "" fuera de nullable → sin modificar.
  C7  Tipos nativos (int, float, bool, None) → sin modificar.
  C8  Anidamiento recursivo — coerción alcanza cualquier profundidad.
  C9  Listas — items str coercionados; sub-dicts recursivos.
  C10 BOOL_TRUE ∩ BOOL_FALSE = ∅ — sin string ambivalente.
  C11 coerce_string es pura — mismo input → mismo output, sin side-effects.

SSOT: todas las constantes vienen de coercion.py — los tests verifican el
módulo canónico, no duplican su lógica.
"""
from __future__ import annotations

import pytest

from ocm.config.layers.coercion import (
    BOOL_TRUE,
    BOOL_FALSE,
    coerce_scalar_values,
    coerce_string,
)


# ── C1: BOOL_TRUE → True ──────────────────────────────────────────────────────

@pytest.mark.parametrize("raw", [
    *BOOL_TRUE,                          # valores canónicos
    *[v.upper() for v in BOOL_TRUE],     # mayúsculas
    *[v.capitalize() for v in BOOL_TRUE],
    *[f"  {v}  " for v in BOOL_TRUE],   # whitespace alrededor
])
def test_coerce_string_true_values(raw):
    """Cualquier variante de BOOL_TRUE → True."""
    assert coerce_string(raw) is True


# ── C2: BOOL_FALSE → False ────────────────────────────────────────────────────

@pytest.mark.parametrize("raw", [
    *BOOL_FALSE,
    *[v.upper() for v in BOOL_FALSE],
    *[v.capitalize() for v in BOOL_FALSE],
    *[f"  {v}  " for v in BOOL_FALSE],
])
def test_coerce_string_false_values(raw):
    """Cualquier variante de BOOL_FALSE → False."""
    assert coerce_string(raw) is False


# ── C3: strings numéricos → sin modificar ────────────────────────────────────

@pytest.mark.parametrize("raw", [
    "0", "1", "6379", "0.5", "3.14", "-1", "101", "9999",
])
def test_coerce_string_numeric_passthrough(raw):
    """Strings numéricos NO se convierten — Pydantic L4 conoce el tipo destino."""
    result = coerce_string(raw)
    assert result == raw
    assert isinstance(result, str), f"Expected str, got {type(result)} for {raw!r}"


@pytest.mark.parametrize("raw", [
    "maybe", "True-ish", "0x1F", "None", "null", "NaN", "", "hello",
])
def test_coerce_string_ambiguous_passthrough(raw):
    """Strings que no son bool semántico inequívoco se retornan sin modificar."""
    assert coerce_string(raw) == raw


# ── C4: nullable path "" → None ──────────────────────────────────────────────

def test_nullable_path_postgres_user():
    """integrations.postgres.user="" → None (en _NULLABLE_PATHS)."""
    d = {"integrations": {"postgres": {"user": ""}}}
    coerce_scalar_values(d)
    assert d["integrations"]["postgres"]["user"] is None


def test_nullable_path_redis_password():
    """integrations.redis.password="" → None (en _NULLABLE_PATHS)."""
    d = {"integrations": {"redis": {"password": ""}}}
    coerce_scalar_values(d)
    assert d["integrations"]["redis"]["password"] is None


def test_nullable_path_kafka_password():
    """integrations.kafka.password="" → None (en _NULLABLE_PATHS)."""
    d = {"integrations": {"kafka": {"password": ""}}}
    coerce_scalar_values(d)
    assert d["integrations"]["kafka"]["password"] is None


# ── C5: nullable key (por nombre) ─────────────────────────────────────────────

def test_nullable_key_api_password_any_depth():
    """api_password="" → None a cualquier profundidad (en _NULLABLE_KEYS)."""
    d = {"exchanges": {"bybit": {"api_password": ""}}}
    coerce_scalar_values(d)
    assert d["exchanges"]["bybit"]["api_password"] is None


def test_nullable_key_api_password_non_empty_preserved():
    """api_password con valor real → no se convierte a None."""
    d = {"exchanges": {"bybit": {"api_password": "real-password"}}}
    coerce_scalar_values(d)
    assert d["exchanges"]["bybit"]["api_password"] == "real-password"


# ── C6: string vacío fuera de nullable → sin modificar ───────────────────────

def test_non_nullable_empty_string_preserved():
    """Campo no nullable con "" debe quedar como "" — Pydantic L4 decide."""
    d = {"section": {"arbitrary_field": ""}}
    coerce_scalar_values(d)
    assert d["section"]["arbitrary_field"] == ""


# ── C7: tipos nativos sin modificar ───────────────────────────────────────────

@pytest.mark.parametrize("native,key", [
    (True,  "flag_true"),
    (False, "flag_false"),
    (0,     "zero"),
    (1,     "one"),
    (6379,  "port"),
    (3.14,  "pi"),
])
def test_native_types_not_modified(native, key):
    """Tipos Python nativos no se tocan — solo se coerciona str."""
    d = {key: native}
    coerce_scalar_values(d)
    # Usar == para int/float, is para bool/None
    if isinstance(native, bool) or native is None:
        assert d[key] is native
    else:
        assert d[key] == native


# ── C8: anidamiento recursivo ─────────────────────────────────────────────────

def test_deep_nesting_coerced():
    """La coerción alcanza nodos a cualquier profundidad del árbol."""
    d = {"a": {"b": {"c": {"enabled": "true", "disabled": "false"}}}}
    coerce_scalar_values(d)
    assert d["a"]["b"]["c"]["enabled"] is True
    assert d["a"]["b"]["c"]["disabled"] is False


def test_sibling_nodes_all_coerced():
    """Todos los nodos hermanos del mismo nivel son coercionados."""
    d = {
        "node1": {"flag": "yes"},
        "node2": {"flag": "no"},
        "node3": {"flag": "on"},
    }
    coerce_scalar_values(d)
    assert d["node1"]["flag"] is True
    assert d["node2"]["flag"] is False
    assert d["node3"]["flag"] is True


# ── C9: listas ────────────────────────────────────────────────────────────────

def test_list_bool_strings_coerced():
    """Items string bool en listas se convierten."""
    d = {"flags": ["true", "false", "yes", "no"]}
    coerce_scalar_values(d)
    assert d["flags"] == [True, False, True, False]


def test_list_non_bool_strings_preserved():
    """Items string no-bool en listas pasan sin modificar."""
    d = {"timeframes": ["1m", "5m", "1h"]}
    coerce_scalar_values(d)
    assert d["timeframes"] == ["1m", "5m", "1h"]


def test_list_of_dicts_recurses():
    """Lista de dicts → cada dict coercionado recursivamente."""
    d = {"exchanges": [{"enabled": "true"}, {"enabled": "false"}]}
    coerce_scalar_values(d)
    assert d["exchanges"][0]["enabled"] is True
    assert d["exchanges"][1]["enabled"] is False


# ── C10: BOOL_TRUE ∩ BOOL_FALSE = ∅ ──────────────────────────────────────────

def test_bool_sets_are_frozensets():
    """BOOL_TRUE y BOOL_FALSE son frozensets — inmutables por contrato."""
    assert isinstance(BOOL_TRUE, frozenset)
    assert isinstance(BOOL_FALSE, frozenset)


def test_bool_sets_are_disjoint():
    """Ningún string puede ser verdadero y falso simultáneamente."""
    assert BOOL_TRUE.isdisjoint(BOOL_FALSE), (
        f"Intersección no vacía: {BOOL_TRUE & BOOL_FALSE}"
    )


def test_bool_sets_canonical_coverage():
    """Los valores canónicos de cada sentido están presentes."""
    assert {"true", "yes", "on"}   <= BOOL_TRUE
    assert {"false", "no", "off"}  <= BOOL_FALSE


# ── C11: pureza de coerce_string ──────────────────────────────────────────────

def test_coerce_string_is_deterministic():
    """Mismo input → mismo output en cualquier invocación."""
    for _ in range(5):
        assert coerce_string("true")  is True
        assert coerce_string("false") is False
        assert coerce_string("hello") == "hello"
        assert coerce_string("6379")  == "6379"


def test_coerce_string_does_not_mutate_input():
    """coerce_string no tiene side-effects sobre el string de entrada."""
    original = "  True  "
    coerce_string(original)
    assert original == "  True  "  # input intacto


def test_coerce_scalar_values_is_idempotent():
    """Aplicar coerce_scalar_values dos veces produce el mismo resultado."""
    d1 = {"flag": "true", "port": "6379"}
    d2 = {"flag": "true", "port": "6379"}
    coerce_scalar_values(d1)
    coerce_scalar_values(d1)  # segunda vez
    coerce_scalar_values(d2)  # una vez
    assert d1 == d2
