from __future__ import annotations

"""core/config/loader/watch.py — Hot-reload con Watchdog."""

import os
import time
from pathlib import Path
from typing import Any, Optional, Union

from .env_resolver import resolve_env
from .exceptions import ConfigurationError

from loguru import logger

_WATCH_ENABLED_ENVS = {"development", "test", "local"}
_DEBOUNCE_SECONDS   = 0.5

try:
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer
    _WATCHDOG_AVAILABLE = True
except ImportError:
    _WATCHDOG_AVAILABLE = False
    FileSystemEventHandler = object  # type: ignore[misc, assignment]


class ConfigChangeHandler(FileSystemEventHandler):
    def __init__(self, env: Optional[str], path: Optional[Union[str, Path]]) -> None:
        self.env   = env
        self.path  = path
        self._last = 0.0

    def on_modified(self, event: Any) -> None:
        if not str(event.src_path).endswith(".yaml"):
            return
        now = time.monotonic()
        if now - self._last < _DEBOUNCE_SECONDS:
            return
        self._last = now
        from core.config.schema import CONFIG_PATH
        from .cache import _config_cache, make_cache_key
        config_dir = Path(self.path).resolve() if self.path else CONFIG_PATH.parent
        for mt in (None, "spot", "swap", "future"):
            _config_cache.invalidate(make_cache_key(self.env or "development", config_dir, mt))
        try:
            from core.config.loader import load_config
            load_config(self.env, self.path, force_reload=True)
            logger.info("config_hot_reloaded | file={}", event.src_path)
        except ConfigurationError as exc:
            logger.error("config_hot_reload_failed | file={} error={}", event.src_path, exc)


def watch_config_files(
    env:   Optional[str]             = None,
    path:  Optional[Union[str, Path]] = None,
    watch: Optional[bool]            = None,
) -> Optional[Any]:
    if not _WATCHDOG_AVAILABLE:
        logger.warning("hot_reload_unavailable | reason=watchdog_not_installed action=pip_install_watchdog")
        return None
    from core.config.schema import CONFIG_PATH
    env     = env or resolve_env()
    enabled = watch if watch is not None else (env in _WATCH_ENABLED_ENVS)
    if not enabled:
        logger.info("hot_reload_disabled | env={}", env)
        return None
    config_dir = Path(path).resolve() if path else CONFIG_PATH.parent
    observer   = Observer()
    observer.schedule(ConfigChangeHandler(env, path), str(config_dir), recursive=False)
    observer.start()
    logger.info("hot_reload_started | dir={} env={}", config_dir, env)
    return observer


def stop_config_watcher(observer: Optional[Any]) -> None:
    if observer and observer.is_alive():
        observer.stop()
        observer.join(timeout=2.0)
        logger.info("hot_reload_stopped")
