from __future__ import annotations

"""Tests para core/config/loader/prefect_integration.py."""

import asyncio
import pytest

from core.config.loader.prefect_integration import (
    _PREFECT_AVAILABLE,
    load_and_validate_config_task,
)


def test_symbol_always_importable():
    """load_and_validate_config_task debe ser importable con o sin Prefect."""
    assert callable(load_and_validate_config_task)


def test_is_coroutine_function():
    """Tanto el stub como la tarea real deben ser async.
    Cuando Prefect está instalado, @task envuelve la función en un objeto
    Task — asyncio.iscoroutinefunction devuelve False sobre el wrapper.
    Verificamos la función subyacente (.fn) si existe, o el objeto directamente.
    """
    fn = getattr(load_and_validate_config_task, 'fn', load_and_validate_config_task)
    assert asyncio.iscoroutinefunction(fn)


@pytest.mark.skipif(_PREFECT_AVAILABLE, reason="solo aplica cuando Prefect no está instalado")
def test_stub_runs_without_prefect(tmp_path):
    """El stub delega a load_config y devuelve config sin necesitar Prefect."""
    from unittest.mock import patch, MagicMock
    fake_config = MagicMock()
    with patch("core.config.loader.load_config", return_value=fake_config) as mock_lc:
        result = asyncio.run(load_and_validate_config_task(env="development"))
    mock_lc.assert_called_once()
    assert result is fake_config


def test_prefect_available_flag_is_bool():
    assert isinstance(_PREFECT_AVAILABLE, bool)
