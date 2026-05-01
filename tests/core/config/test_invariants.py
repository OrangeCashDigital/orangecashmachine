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


import ocm_platform.config.env_vars as env_vars_module
from ocm_platform.config.env_vars import (
    OCM_DATA_LAKE_PATH,
    OCM_GOLD_PATH,
)
from ocm_platform.config.paths import (
    bronze_ohlcv_root,
    data_lake_root,
    gold_features_root,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parents[3]
_SRC_DIRS  = [
    _REPO_ROOT / "core",
    _REPO_ROOT / "market_data",
    _REPO_ROOT / "data_platform",  # facade de acceso al lake
    _REPO_ROOT / "infra",           # cursor_store y estado operacional
    _REPO_ROOT / "research",        # scripts de investigación
]


def _all_py_files() -> list[Path]:
    files = []
    for d in _SRC_DIRS:
        if d.exists():
            files.extend(d.rglob("*.py"))
    return files


def _declared_env_var_values() -> set[str]:
    """Autoridad única: el registro canónico _ENV_VAR_NAMES de env_vars.py.

    Esta función es el único punto de contacto entre el sistema de tests
    y el SSOT. No infiere, no itera vars(module), no usa heurística.

    Invariante garantizado por env_vars.py en import-time:
      _ENV_VAR_NAMES ≡ constantes UPPER_CASE del módulo
    Por tanto, esta función es equivalente a la introspección del módulo
    pero sin depender de reglas de naming implícitas.

    Principio: Explicit over Implicit (PEP 20), SSOT, DRY.
    """
    return set(env_vars_module._ENV_VAR_NAMES)


def _declared_env_var_names() -> set[str]:
    """Alias de _declared_env_var_values() — misma fuente, semántica de nombres.

    En OCM: nombre del atributo Python ≡ valor string (ej. OCM_ENV = "OCM_ENV").
    Por diseño, la distinción nombre/valor colapsa al mismo conjunto.
    Mantener función separada preserva la intención semántica en los tests.
    """
    return _declared_env_var_values()


def _getenv_literals_in_codebase() -> set[str]:
    """
    Parsea el AST de todos los .py del codebase y extrae los strings literales
    pasados como primer argumento a os.getenv() / os.environ.get().

    Retorna solo strings que parecen env vars (UPPER_CASE con guiones bajos).

    Patrones cubiertos
    ------------------
    A: os.getenv("VAR") · environ.get("VAR") · os.putenv("VAR", v)
    B: os.environ.get("VAR") · os.environ.setdefault("VAR", v)
    C: os.environ["VAR"]  (Subscript)

    KNOWN LIMITS (límites arquitectónicos explícitos)
    -------------------------------------------------
    - No sigue aliases de import: ``import os as system; system.getenv(...)``
    - No resuelve wrappers indirectos: ``get_env("VAR")`` si no llama os.getenv
    - No sigue imports dinámicos ni __import__
    - No analiza getattr(os, "environ") ni acceso por reflexión
    - Hydra vars dinámicas (nombre construido en runtime) no son detectables
    Estos casos requieren análisis estático con mypy/pyright o instrumentación
    en runtime. Son riesgos documentados y aceptados conscientemente.
    """
    found: set[str] = set()
    pattern = re.compile(r'^[A-Z][A-Z0-9_]+$')

    for fpath in _all_py_files():
        try:
            tree = ast.parse(fpath.read_text(encoding="utf-8"), filename=str(fpath))
        except SyntaxError:
            continue

        for node in ast.walk(tree):
            val: str | None = None

            # Pattern A: os.getenv("VAR") · environ.get("VAR") · os.putenv("VAR", v)
            if isinstance(node, ast.Call) and node.args:
                func = node.func
                is_simple = (
                    isinstance(func, ast.Attribute)
                    and func.attr in ("getenv", "get", "putenv")
                    and isinstance(func.value, ast.Name)
                    and func.value.id in ("os", "environ")
                )
                # Pattern B: os.environ.get("VAR") · os.environ.setdefault("VAR", v)
                is_environ_get = (
                    isinstance(func, ast.Attribute)
                    and func.attr in ("get", "setdefault")
                    and isinstance(func.value, ast.Attribute)
                    and func.value.attr == "environ"
                    and isinstance(func.value.value, ast.Name)
                    and func.value.value.id == "os"
                )
                if is_simple or is_environ_get:
                    first = node.args[0]
                    if isinstance(first, ast.Constant) and isinstance(first.value, str):
                        val = first.value

            # Pattern C: os.environ["VAR"]  ← Subscript, no Call
            elif isinstance(node, ast.Subscript):
                tgt = node.value
                if (
                    isinstance(tgt, ast.Attribute)
                    and tgt.attr == "environ"
                    and isinstance(tgt.value, ast.Name)
                    and tgt.value.id == "os"
                ):
                    slc = node.slice
                    if isinstance(slc, ast.Constant) and isinstance(slc.value, str):
                        val = slc.value

            if val and pattern.match(val):
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


def _yaml_files() -> list[Path]:
    """Todos los .yaml/.yml bajo conf/, core/ y market_data/."""
    yamls: list[Path] = []
    for d in _SRC_DIRS + [_REPO_ROOT / "conf"]:
        if d.exists():
            yamls.extend(d.rglob("*.yaml"))
            yamls.extend(d.rglob("*.yml"))
    return yamls


def _oc_env_literals_in_yamls() -> set[str]:
    """
    Extrae VAR de interpolaciones Hydra/OmegaConf ``${oc.env:VAR}``
    y ``${oc.env:VAR,default}`` en archivos YAML del proyecto.
    """
    found: set[str] = set()
    pattern = re.compile(r'\$\{oc\.env:([A-Z][A-Z0-9_]+)(?:,[^}]*)?\}')
    for fpath in _yaml_files():
        try:
            text = fpath.read_text(encoding="utf-8")
        except OSError:
            continue
        found.update(pattern.findall(text))
    return found


# ---------------------------------------------------------------------------
# Invariante 2: SSoT — todo os.getenv literal está en env_vars.py
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Infra Registry — política explícita de variables externas al dominio OCM.
#
# Estructura: dict[categoria -> frozenset[str]]
# Política:
#   "os"      → variables del sistema operativo. Nunca son dominio OCM.
#   "redis"   → infraestructura de estado. Candidatas a infra_vars.py si
#               el módulo infra crece lo suficiente.
#   "db"      → bases de datos genéricas. Externas al dominio OCM.
#
# Regla de crecimiento: si una categoría supera 8 entradas, es señal de
# que debería existir un módulo dedicado (infra_vars.py, ops_vars.py).
# Principio: OCP — abierto a extensión por categoría, cerrado a mezcla.
# ---------------------------------------------------------------------------

_INFRA_REGISTRY: dict[str, frozenset[str]] = {
    "os": frozenset({
        "HOME", "PATH", "USER", "SHELL", "PWD",
        "PYTHONPATH", "VIRTUAL_ENV",
    }),
    "redis": frozenset({
        "REDIS_URL", "REDIS_HOST", "REDIS_PORT", "REDIS_DB", "REDIS_PASSWORD",
        "CURSOR_TTL_DAYS",
    }),
    "db": frozenset({
        "DATABASE_URL",
    }),
}

_SYSTEM_VARS_ALLOWLIST: frozenset[str] = frozenset().union(*_INFRA_REGISTRY.values())
"""Unión derivada del registry. No editar directamente — editar _INFRA_REGISTRY."""


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


def test_all_oc_env_literals_declared_in_env_vars():
    """
    Toda var referenciada en YAML via ``${oc.env:VAR}`` debe estar declarada
    en env_vars.py (SSoT). Cubre el vector Hydra/OmegaConf que el scanner
    de AST no alcanza.
    """
    declared_values = _declared_env_var_values()
    yaml_literals   = _oc_env_literals_in_yamls()

    undeclared = yaml_literals - declared_values - _SYSTEM_VARS_ALLOWLIST
    assert not undeclared, (
        "Env vars en ${oc.env:VAR} (YAML) sin declarar en env_vars.py:\n"
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
