"""tests/test_env_overrides.py — Unit tests para apply_env_overrides."""

import pytest
from core.config.loader.env_overrides import apply_env_overrides


@pytest.fixture
def base_config():
    return {
        "pipeline": {
            "historical": {
                "fetch_all_history": False,
                "start_date": "2024-01-01T00:00:00Z",
                "max_concurrent_tasks": 2,
            },
            "realtime": {
                "snapshot_interval_seconds": 60,
            },
        },
        "observability": {
            "logging": {"level": "INFO"},
        },
    }


def test_fetch_all_history_override(monkeypatch, base_config):
    """OCM_FETCH_ALL_HISTORY=true debe pisar el False del YAML mergeado."""
    monkeypatch.setenv("OCM_PIPELINE__HISTORICAL__FETCH_ALL_HISTORY", "true")
    result = apply_env_overrides(base_config)
    assert result["pipeline"]["historical"]["fetch_all_history"] is True


def test_fetch_all_history_false(monkeypatch, base_config):
    """OCM_FETCH_ALL_HISTORY=false debe mantener False."""
    monkeypatch.setenv("OCM_PIPELINE__HISTORICAL__FETCH_ALL_HISTORY", "false")
    result = apply_env_overrides(base_config)
    assert result["pipeline"]["historical"]["fetch_all_history"] is False


def test_start_date_override(monkeypatch, base_config):
    """OCM_START_DATE debe pisar el start_date del YAML."""
    monkeypatch.setenv("OCM_PIPELINE__HISTORICAL__START_DATE", "2017-01-01T00:00:00Z")
    result = apply_env_overrides(base_config)
    assert result["pipeline"]["historical"]["start_date"] == "2017-01-01T00:00:00Z"


def test_max_concurrent_coercion(monkeypatch, base_config):
    """OCM_MAX_CONCURRENT debe coercionar string a int."""
    monkeypatch.setenv("OCM_PIPELINE__HISTORICAL__MAX_CONCURRENT_TASKS", "8")
    result = apply_env_overrides(base_config)
    assert result["pipeline"]["historical"]["max_concurrent_tasks"] == 8
    assert isinstance(result["pipeline"]["historical"]["max_concurrent_tasks"], int)


def test_no_overrides_leaves_config_intact(base_config):
    """Sin env vars OCM_*, el config no debe cambiar."""
    import copy
    original = copy.deepcopy(base_config)
    result = apply_env_overrides(base_config)
    assert result["pipeline"]["historical"]["fetch_all_history"] is False
    assert result == original


def test_log_level_override(monkeypatch, base_config):
    """OCM_LOG_LEVEL debe pisar el nivel de logging."""
    monkeypatch.setenv("OCM_OBSERVABILITY__LOGGING__LEVEL", "DEBUG")
    result = apply_env_overrides(base_config)
    assert result["observability"]["logging"]["level"] == "DEBUG"
