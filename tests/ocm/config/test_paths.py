from __future__ import annotations

"""Tests para ocm/config/paths.py."""

from unittest.mock import patch


from ocm.config.paths import (
    _expand_env,
    _read_yaml_lake_path,
    data_lake_root,
    bronze_ohlcv_root,
    gold_features_root,
)
from ocm.config.env_vars import OCM_DATA_LAKE_PATH, OCM_GOLD_PATH


# ---------------------------------------------------------------------------
# _expand_env
# ---------------------------------------------------------------------------

def test_expand_env_with_default(monkeypatch):
    monkeypatch.delenv("MY_VAR", raising=False)
    result = _expand_env("${MY_VAR:-fallback_value}")
    assert result == "fallback_value"


def test_expand_env_with_set_var(monkeypatch):
    monkeypatch.setenv("MY_VAR", "/real/path")
    result = _expand_env("${MY_VAR:-fallback_value}")
    assert result == "/real/path"


def test_expand_env_no_default(monkeypatch):
    monkeypatch.delenv("MISSING_VAR", raising=False)
    result = _expand_env("${MISSING_VAR}")
    assert result == ""


def test_expand_env_no_placeholders():
    assert _expand_env("plain/path") == "plain/path"


# ---------------------------------------------------------------------------
# data_lake_root — prioridad 1: env var
# ---------------------------------------------------------------------------

def test_data_lake_root_env_override(monkeypatch, tmp_path):
    # Limpiar SSOT var para aislar el test de env real del proceso
    monkeypatch.delenv("OCM_STORAGE__DATA_LAKE__PATH", raising=False)
    monkeypatch.setenv(OCM_DATA_LAKE_PATH, str(tmp_path))
    result = data_lake_root()
    assert result == tmp_path.resolve()


def test_data_lake_root_fallback(monkeypatch):
    monkeypatch.delenv(OCM_DATA_LAKE_PATH, raising=False)
    with patch("ocm.config.paths._read_yaml_lake_path", return_value=None):
        result = data_lake_root()
    assert result.parts[-2:] == ("data_platform", "data_lake")


# ---------------------------------------------------------------------------
# Paths derivados
# ---------------------------------------------------------------------------

def test_bronze_ohlcv_root_suffix(monkeypatch, tmp_path):
    monkeypatch.delenv("OCM_STORAGE__DATA_LAKE__PATH", raising=False)
    monkeypatch.setenv(OCM_DATA_LAKE_PATH, str(tmp_path))
    assert bronze_ohlcv_root() == tmp_path.resolve() / "bronze" / "ohlcv"



def test_gold_features_root_env_override(monkeypatch, tmp_path):
    monkeypatch.setenv(OCM_GOLD_PATH, str(tmp_path))
    assert gold_features_root() == tmp_path.resolve()


def test_gold_features_root_derived(monkeypatch, tmp_path):
    monkeypatch.delenv("OCM_STORAGE__DATA_LAKE__PATH", raising=False)
    monkeypatch.setenv(OCM_DATA_LAKE_PATH, str(tmp_path))
    monkeypatch.delenv(OCM_GOLD_PATH, raising=False)
    assert gold_features_root() == tmp_path.resolve() / "gold" / "features" / "ohlcv"


# ---------------------------------------------------------------------------
# _read_yaml_lake_path — falla silenciosa con log
# ---------------------------------------------------------------------------

def test_read_yaml_lake_path_returns_none_on_error(monkeypatch):
    """Si el YAML loader lanza, devuelve None sin propagar."""
    with patch("ocm.config.paths._find_config_dir", side_effect=RuntimeError("boom")):
        result = _read_yaml_lake_path()
    assert result is None


# ---------------------------------------------------------------------------
# Regresión: resolución de config independiente del CWD
# Detecta el bug que existió en load_appconfig_standalone y _find_config_dir
# cuando usaban Path("config").resolve() relativo al CWD.
# ---------------------------------------------------------------------------

def test_find_config_dir_independent_of_cwd(tmp_path, monkeypatch):
    """_find_config_dir resuelve via repo_root() — nunca via CWD."""
    monkeypatch.chdir(tmp_path)  # CWD = directorio vacío sin config/
    monkeypatch.delenv("OCM_CONFIG_PATH", raising=False)
    monkeypatch.delenv("OCM_CONFIG_DIR", raising=False)

    from ocm.config.paths import _find_config_dir
    result = _find_config_dir()

    # Debe encontrar config/ del repo — nunca None ni tmp_path/config
    assert result is not None, "_find_config_dir retornó None con CWD vacío"
    assert result.name == "config"
    assert "orangecashmachine" in str(result) or result.exists()


def test_find_config_dir_env_override_takes_priority(tmp_path, monkeypatch):
    """OCM_CONFIG_PATH tiene prioridad sobre repo_root()."""
    config_via_env = tmp_path / "my_config"
    config_via_env.mkdir()
    monkeypatch.setenv("OCM_CONFIG_PATH", str(config_via_env))

    from ocm.config.paths import _find_config_dir
    result = _find_config_dir()
    assert result == config_via_env


def test_find_config_dir_env_override_invalid_dir_falls_back(tmp_path, monkeypatch):
    """Si OCM_CONFIG_PATH apunta a un path inexistente, devuelve None."""
    monkeypatch.setenv("OCM_CONFIG_PATH", str(tmp_path / "no_existe"))
    monkeypatch.delenv("OCM_CONFIG_DIR", raising=False)

    from ocm.config.paths import _find_config_dir
    result = _find_config_dir()
    assert result is None


def test_repo_root_resolves_to_git_root():
    """repo_root() siempre apunta al directorio que contiene .git."""
    from ocm.config.paths import repo_root
    root = repo_root()
    assert (root / ".git").exists(), f"repo_root() no contiene .git: {root}"
    assert (root / "pyproject.toml").exists(), f"repo_root() no contiene pyproject.toml: {root}"
