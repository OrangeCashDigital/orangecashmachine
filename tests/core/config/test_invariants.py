from __future__ import annotations

"""
Tests de invariantes del sistema de configuración.

Estas pruebas no testean comportamiento puntual — testean propiedades
estructurales de la arquitectura que NO deben degradarse con el tiempo:

  1. Ninguna env var eliminada reaparece en env_vars.py
     (regresión de dead code)

  2. Toda env var usada en el codebase mediante os.getenv / os.environ
     está declarada como constante en env_vars.py
     (SSoT — ningún string mágico suelto)

  3. data_lake_root() resuelve determinísticamente dado el mismo entorno
     (idempotencia de paths)

  4. Los paths derivados son siempre subdirectorios de data_lake_root()
     (invariante estructural del lake)

  5. __all__ de env_vars exporta solo strings (no accidentalmente None u otro tipo)
"""

import ast
import re
from pathlib import Path


import core.config.env_vars as env_vars_module
from core.config.env_vars import (
    OCM_DATA_LAKE_PATH,
    OCM_GOLD_PATH,
)
from core.config.paths import (
    bronze_ohlcv_root,
    data_lake_root,
    gold_features_root,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parents[3]
_SRC_DIRS  = [_REPO_ROOT / "core", _REPO_ROOT / "market_data"]


def _all_py_files() -> list[Path]:
    files = []
    for d in _SRC_DIRS:
        if d.exists():
            files.extend(d.rglob("*.py"))
    return files


def _declared_env_var_values() -> set[str]:
    """Retorna los valores string de todas las constantes en env_vars.py."""
    return {
        v for v in vars(env_vars_module).values()
        if isinstance(v, str) and v.replace("_", "").replace(".", "").isupper()
        and len(v) > 1  # excluye strings vacíos y letras sueltas
    }


def _declared_env_var_names() -> set[str]:
    """Retorna los nombres de constante exportadas (OCM_*, PUSHGATEWAY_URL, etc.)."""
    return {
        k for k, v in vars(env_vars_module).items()
        if isinstance(v, str) and not k.startswith("_")
        and k == k.upper()
    }


def _getenv_literals_in_codebase() -> set[str]:
    """
    Parsea el AST de todos los .py del codebase y extrae los strings literales
    pasados como primer argumento a os.getenv() / os.environ.get().

    Retorna solo strings que parecen env vars (UPPER_CASE con guiones bajos).
    """
    found: set[str] = set()
    pattern = re.compile(r'^[A-Z][A-Z0-9_]+$')

    for fpath in _all_py_files():
        try:
            tree = ast.parse(fpath.read_text(encoding="utf-8"), filename=str(fpath))
        except SyntaxError:
            continue

        for node in ast.walk(tree):
            # os.getenv("VAR") o os.getenv("VAR", default)
            if isinstance(node, ast.Call):
                func = node.func
                is_getenv = (
                    isinstance(func, ast.Attribute)
                    and func.attr in ("getenv", "get")
                    and isinstance(func.value, ast.Name)
                    and func.value.id in ("os", "environ")
                )
                if is_getenv and node.args:
                    first = node.args[0]
                    if isinstance(first, ast.Constant) and isinstance(first.value, str):
                        val = first.value
                        if pattern.match(val):
                            found.add(val)
    return found


# ---------------------------------------------------------------------------
# Invariante 1: vars eliminadas no reaparecen
# ---------------------------------------------------------------------------

_REMOVED_VARS = {
    # Eliminadas en commit 648c37d — nunca deben volver a env_vars.py
    "OCM_BACKFILL_MODE",
    "OCM_FETCH_ALL_HISTORY",
    "OCM_START_DATE",
    "OCM_MAX_CONCURRENT",
    "OCM_LOG_LEVEL",
    "OCM_SNAPSHOT_INTERVAL",
    "OCM_METRICS_ENABLED",
    "OCM_METRICS_PORT",
}


def test_removed_env_vars_not_in_env_vars_module():
    """Ninguna var eliminada debe reaparecer como constante en env_vars.py."""
    declared = _declared_env_var_names()
    regressions = _REMOVED_VARS & declared
    assert not regressions, (
        f"Vars eliminadas reaparecieron en env_vars.py: {sorted(regressions)}\n"
        f"Commit de referencia: 648c37d"
    )


def test_removed_env_vars_not_used_via_getenv_literal():
    """
    Ninguna var eliminada debe usarse como string literal en os.getenv().
    Uso correcto: importar la constante desde env_vars, no escribir el string.
    """
    used_literals = _getenv_literals_in_codebase()
    regressions = _REMOVED_VARS & used_literals
    assert not regressions, (
        f"Vars eliminadas usadas como literal en os.getenv(): {sorted(regressions)}\n"
        f"Solución: importar la constante desde core.config.env_vars"
    )


# ---------------------------------------------------------------------------
# Invariante 2: SSoT — todo os.getenv literal está en env_vars.py
# ---------------------------------------------------------------------------

# Excepciones legítimas: vars del sistema operativo/infraestructura
# que no son propias de OCM y no deben vivir en env_vars.py.
_SYSTEM_VARS_ALLOWLIST: frozenset[str] = frozenset({
    "HOME", "PATH", "USER", "SHELL", "PWD",
    "PYTHONPATH", "VIRTUAL_ENV",
    "REDIS_URL", "DATABASE_URL",       # infraestructura externa
    "PREFECT_API_URL", "PREFECT_API_KEY",
})


def test_all_getenv_literals_declared_in_env_vars():
    """
    Todo string literal pasado a os.getenv() que parezca una env var OCM
    debe estar declarado como constante en env_vars.py (SSoT).
    """
    declared_values = _declared_env_var_values()
    used_literals   = _getenv_literals_in_codebase()

    undeclared = used_literals - declared_values - _SYSTEM_VARS_ALLOWLIST
    assert not undeclared, (
        "Env vars usadas como literal sin declarar en env_vars.py:\n"
        + "\n".join(f"  - {v}" for v in sorted(undeclared))
        + "\nSolución: añadir constante en core/config/env_vars.py"
    )


# ---------------------------------------------------------------------------
# Invariante 3: data_lake_root() es idempotente
# ---------------------------------------------------------------------------

def test_data_lake_root_idempotent(monkeypatch, tmp_path):
    """Llamadas sucesivas con el mismo entorno retornan el mismo path."""
    monkeypatch.setenv(OCM_DATA_LAKE_PATH, str(tmp_path))
    r1 = data_lake_root()
    r2 = data_lake_root()
    assert r1 == r2


def test_data_lake_root_is_absolute(monkeypatch, tmp_path):
    monkeypatch.setenv(OCM_DATA_LAKE_PATH, str(tmp_path))
    assert data_lake_root().is_absolute()


# ---------------------------------------------------------------------------
# Invariante 4: paths derivados son subdirectorios de data_lake_root()
# ---------------------------------------------------------------------------

def test_bronze_root_is_subdir_of_lake(monkeypatch, tmp_path):
    monkeypatch.setenv(OCM_DATA_LAKE_PATH, str(tmp_path))
    monkeypatch.delenv(OCM_GOLD_PATH, raising=False)
    lake   = data_lake_root()
    bronze = bronze_ohlcv_root()
    assert str(bronze).startswith(str(lake)), (
        f"bronze_ohlcv_root() no es subdir de data_lake_root()\n"
        f"  lake:   {lake}\n  bronze: {bronze}"
    )


def test_gold_root_is_subdir_of_lake_when_no_override(monkeypatch, tmp_path):
    monkeypatch.setenv(OCM_DATA_LAKE_PATH, str(tmp_path))
    monkeypatch.delenv(OCM_GOLD_PATH, raising=False)
    lake = data_lake_root()
    gold = gold_features_root()
    assert str(gold).startswith(str(lake)), (
        f"gold_features_root() no es subdir de data_lake_root() sin override\n"
        f"  lake: {lake}\n  gold: {gold}"
    )


# ---------------------------------------------------------------------------
# Invariante 5: __all__ de env_vars exporta solo strings
# ---------------------------------------------------------------------------

def test_env_vars_module_constants_are_strings():
    """Toda constante UPPER_CASE en env_vars.py debe ser un str."""
    bad = {
        k: type(v).__name__
        for k, v in vars(env_vars_module).items()
        if k == k.upper() and not k.startswith("_")
        and not isinstance(v, (str, frozenset, dict, bool))
    }
    assert not bad, (
        f"Constantes en env_vars.py con tipo inesperado: {bad}"
    )
