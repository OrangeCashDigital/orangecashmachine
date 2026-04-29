from __future__ import annotations

"""Tests para core/config/paths.py."""

from unittest.mock import patch


from ocm_platform.config.paths import (
    _expand_env,
    _read_yaml_lake_path,
    data_lake_root,
    bronze_ohlcv_root,
    gold_features_root,
)
from ocm_platform.config.env_vars import OCM_DATA_LAKE_PATH, OCM_GOLD_PATH


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
    monkeypatch.setenv(OCM_DATA_LAKE_PATH, str(tmp_path))
    result = data_lake_root()
    assert result == tmp_path.resolve()


def test_data_lake_root_fallback(monkeypatch):
    monkeypatch.delenv(OCM_DATA_LAKE_PATH, raising=False)
    with patch("ocm_platform.config.paths._read_yaml_lake_path", return_value=None):
        result = data_lake_root()
    assert result.parts[-2:] == ("data_platform", "data_lake")


# ---------------------------------------------------------------------------
# Paths derivados
# ---------------------------------------------------------------------------

def test_bronze_ohlcv_root_suffix(monkeypatch, tmp_path):
    monkeypatch.setenv(OCM_DATA_LAKE_PATH, str(tmp_path))
    assert bronze_ohlcv_root() == tmp_path.resolve() / "bronze" / "ohlcv"



def test_gold_features_root_env_override(monkeypatch, tmp_path):
    monkeypatch.setenv(OCM_GOLD_PATH, str(tmp_path))
    assert gold_features_root() == tmp_path.resolve()


def test_gold_features_root_derived(monkeypatch, tmp_path):
    monkeypatch.setenv(OCM_DATA_LAKE_PATH, str(tmp_path))
    monkeypatch.delenv(OCM_GOLD_PATH, raising=False)
    assert gold_features_root() == tmp_path.resolve() / "gold" / "features" / "ohlcv"


# ---------------------------------------------------------------------------
# _read_yaml_lake_path — falla silenciosa con log
# ---------------------------------------------------------------------------

def test_read_yaml_lake_path_returns_none_on_error(monkeypatch):
    """Si el YAML loader lanza, devuelve None sin propagar."""
    with patch("ocm_platform.config.paths._find_config_dir", side_effect=RuntimeError("boom")):
        result = _read_yaml_lake_path()
    assert result is None
