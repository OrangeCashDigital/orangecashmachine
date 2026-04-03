from __future__ import annotations

"""core/config/loader/prefect_integration.py — Tarea Prefect opcional."""

import asyncio
from pathlib import Path
from typing import Optional, Union

try:
    from prefect import get_run_logger, task
    _PREFECT_AVAILABLE = True
except ImportError:
    _PREFECT_AVAILABLE = False


if _PREFECT_AVAILABLE:
    @task(name="load_config", retries=2, retry_delay_seconds=[5, 30])
    async def load_and_validate_config_task(
        env:          Optional[str]             = None,
        path:         Optional[Union[str, Path]] = None,
        use_cache:    bool                       = True,
        force_reload: bool                       = False,
        market_type:  Optional[str]             = None,
    ):
        from core.config.loader import load_config
        prefect_log = get_run_logger()
        # load_config es síncrono — correrlo en un thread para no bloquear
        # el event loop de Prefect. asyncio.to_thread requiere Python 3.9+.
        config = await asyncio.to_thread(
            load_config, env, path, use_cache, force_reload, market_type
        )
        prefect_log.info(
            "Config loaded | env=%s exchanges=%s",
            env,
            getattr(config, "exchange_names", []),
        )
        return config

else:
    # Stub para entornos sin Prefect instalado.
    # Permite importar el símbolo sin ImportError — el caller debe verificar
    # _PREFECT_AVAILABLE antes de invocar como tarea Prefect real.
    async def load_and_validate_config_task(  # type: ignore[misc]
        env:          Optional[str]             = None,
        path:         Optional[Union[str, Path]] = None,
        use_cache:    bool                       = True,
        force_reload: bool                       = False,
        market_type:  Optional[str]             = None,
    ):
        """Stub — Prefect no disponible. Delega directamente a load_config."""
        from core.config.loader import load_config
        return load_config(env, path, use_cache, force_reload, market_type)
