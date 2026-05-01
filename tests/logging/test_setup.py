from __future__ import annotations

"""
tests/logging/test_setup.py
============================

Unit tests para bootstrap, configure_logging y helpers de logger.py.

Invariantes críticos:
  • bootstrap_logging() es idempotente — segunda llamada no reinstala sinks
  • configure_logging() skippea si el hash de config no cambia
  • _resolve_config() produce un dict canónico con defaults correctos
  • setup_logging() está eliminada — cualquier llamada lanza RuntimeError
"""

import pytest
from unittest.mock import patch

import ocm_platform.observability.logger as _setup_mod
from ocm_platform.observability.config import LoggingConfig
from ocm_platform.observability.logger import (
    bootstrap_logging,
    configure_logging,
    bind_pipeline,
    is_logging_configured,
    setup_logging,
    _resolve_config,
    _stable,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def reset_global_state():
    """Resetea el estado global del módulo antes de cada test."""
    original = (
        _setup_mod._BOOTSTRAP_DONE,
        _setup_mod._CONFIG_HASH,
        _setup_mod._ACTIVE_SINK_IDS[:],
        _setup_mod._ACTIVE_LOKI,
    )
    _setup_mod._BOOTSTRAP_DONE  = False
    _setup_mod._CONFIG_HASH     = None
    _setup_mod._ACTIVE_SINK_IDS = []
    _setup_mod._ACTIVE_LOKI     = None
    yield
    _setup_mod._BOOTSTRAP_DONE  = original[0]
    _setup_mod._CONFIG_HASH     = original[1]
    _setup_mod._ACTIVE_SINK_IDS = original[2]
    _setup_mod._ACTIVE_LOKI     = original[3]


# ── _resolve_config ───────────────────────────────────────────────────────────

def test_resolve_config_defaults():
    """
    _resolve_config(None) produce los defaults canónicos del sistema.

    Nota: 'pipeline' fue eliminado en v0.2.0 (bug fix #10 — sink redundante).
    El dict resultante NO contiene esa clave — si reaparece, es una regresión.
    """
    resolved = _resolve_config(None, False, None)
    assert resolved["level"]   == "INFO"
    assert resolved["console"] is True
    assert resolved["file"]    is True
    assert resolved["loki_url"] is None
    # Invariante de eliminación: 'pipeline' no debe estar en el dict
    assert "pipeline" not in resolved, (
        "'pipeline' reapareció en _resolve_config — el sink pipeline_*.log "
        "fue eliminado en v0.2.0 por ser subconjunto redundante de app_*.log."
    )


def test_resolve_config_debug_forces_debug_level():
    resolved = _resolve_config(None, True, None)
    assert resolved["level"] == "DEBUG"


def test_resolve_config_from_logging_config():
    cfg = LoggingConfig(level="WARNING", console=False, loki_url="http://loki:3100")
    resolved = _resolve_config(cfg, False, None)
    assert resolved["level"]    == "WARNING"
    assert resolved["console"]  is False
    assert resolved["loki_url"] == "http://loki:3100"


def test_resolve_config_debug_overrides_cfg_level():
    """debug=True debe ganar sobre cualquier level en LoggingConfig."""
    cfg = LoggingConfig(level="ERROR")
    resolved = _resolve_config(cfg, True, None)
    assert resolved["level"] == "DEBUG"


# ── _stable ───────────────────────────────────────────────────────────────────

def test_stable_sorts_dict_keys():
    result = _stable({"b": 1, "a": 2})
    assert list(result.keys()) == ["a", "b"]


def test_stable_converts_path():
    from pathlib import Path
    result = _stable(Path("/tmp/logs"))
    assert result == "/tmp/logs"


# ── bootstrap_logging ─────────────────────────────────────────────────────────

def test_bootstrap_logging_is_idempotent():
    with patch("ocm_platform.observability.logger._install_sinks", return_value=([], None)) as mock:
        with patch("ocm_platform.observability.logger._replay_bootstrap_buffer"):
            with patch("ocm_platform.observability.logger._install_stdlib_bridge"):
                bootstrap_logging()
                bootstrap_logging()  # segunda llamada — debe ser no-op
                assert mock.call_count == 1


def test_bootstrap_logging_sets_done_flag():
    with patch("ocm_platform.observability.logger._install_sinks", return_value=([], None)):
        with patch("ocm_platform.observability.logger._replay_bootstrap_buffer"):
            with patch("ocm_platform.observability.logger._install_stdlib_bridge"):
                bootstrap_logging()
                assert _setup_mod._BOOTSTRAP_DONE is True


# ── configure_logging ─────────────────────────────────────────────────────────

def test_configure_logging_skips_if_hash_unchanged():
    """Misma config → mismo hash → _install_sinks llamado solo una vez."""
    cfg = LoggingConfig()
    with patch("ocm_platform.observability.logger._install_sinks", return_value=([], None)) as mock:
        with patch("ocm_platform.observability.logger._install_stdlib_bridge"):
            configure_logging(cfg, env="development")
            configure_logging(cfg, env="development")
            assert mock.call_count == 1


def test_configure_logging_reconfigures_on_change():
    """Config diferente → hash diferente → reinstalación de sinks."""
    with patch("ocm_platform.observability.logger._install_sinks", return_value=([], None)) as mock:
        with patch("ocm_platform.observability.logger._install_stdlib_bridge"):
            configure_logging(LoggingConfig(level="INFO"),  env="development")
            configure_logging(LoggingConfig(level="DEBUG"), env="development")
            assert mock.call_count == 2


# ── is_logging_configured ─────────────────────────────────────────────────────

def test_is_logging_configured_false_initially():
    assert is_logging_configured() is False


def test_is_logging_configured_true_after_both():
    with patch("ocm_platform.observability.logger._install_sinks", return_value=([], None)):
        with patch("ocm_platform.observability.logger._replay_bootstrap_buffer"):
            with patch("ocm_platform.observability.logger._install_stdlib_bridge"):
                bootstrap_logging()
                configure_logging(LoggingConfig(), env="development")
                assert is_logging_configured() is True


# ── bind_pipeline ─────────────────────────────────────────────────────────────

def test_bind_pipeline_returns_logger():
    log = bind_pipeline("ohlcv_fetcher")
    assert log is not None


def test_bind_pipeline_with_full_context():
    log = bind_pipeline("fetcher", exchange="bybit", dataset="ohlcv", symbol="BTC/USDT")
    assert log is not None


# ── setup_logging deprecated ──────────────────────────────────────────────────

def test_setup_logging_raises():
    with pytest.raises(RuntimeError, match="v0.2.0"):
        setup_logging()


# ── LoggingConfig validator ───────────────────────────────────────────────────

def test_logging_config_rejects_invalid_level():
    with pytest.raises(Exception):
        LoggingConfig(level="VERBOSE")


def test_logging_config_normalizes_level():
    cfg = LoggingConfig(level="warning")
    assert cfg.level == "WARNING"
