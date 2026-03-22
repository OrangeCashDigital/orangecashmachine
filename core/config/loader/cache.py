from __future__ import annotations

"""core/config/loader/cache.py — Cache en memoria thread-safe."""

import hashlib
import threading
from pathlib import Path
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from core.config.schema import AppConfig


class ConfigCache:
    def __init__(self) -> None:
        self._cache:  dict[str, "AppConfig"] = {}
        self._hashes: dict[str, str]         = {}
        self._lock    = threading.RLock()

    def get(self, key: str, h: str) -> Optional["AppConfig"]:
        with self._lock:
            if self._hashes.get(key) == h:
                return self._cache.get(key)
            return None

    def set(self, key: str, h: str, config: "AppConfig") -> None:
        with self._lock:
            self._cache[key]  = config
            self._hashes[key] = h

    def invalidate(self, key: str) -> None:
        with self._lock:
            self._cache.pop(key, None)
            self._hashes.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
            self._hashes.clear()


def make_cache_key(env: str, config_dir: Path, market_type: Optional[str] = None) -> str:
    dir_hash = hashlib.sha256(str(config_dir.resolve()).encode()).hexdigest()[:8]
    key = f"{dir_hash}:{env}"
    if market_type:
        key = f"{key}:{market_type}"
    return key


_config_cache = ConfigCache()
