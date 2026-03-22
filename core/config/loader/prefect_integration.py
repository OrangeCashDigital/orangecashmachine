from __future__ import annotations

"""core/config/loader/prefect_integration.py — Tarea Prefect opcional."""

import os
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
        config = load_config(env, path, use_cache, force_reload, market_type)
        prefect_log.info(
            "Config loaded | env=%s exchanges=%s",
            env or os.getenv("OCM_ENV"),
            getattr(config, "exchange_names", []),
        )
        return config
