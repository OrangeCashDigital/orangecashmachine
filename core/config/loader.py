from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Optional, Union

import yaml
from pydantic import ValidationError

from core.config.schema import AppConfig, CONFIG_PATH

_ENV_VAR_RE = re.compile(r"\$\{([^}]+)\}")


def _resolve_env_vars(raw: str) -> str:
    return _ENV_VAR_RE.sub(lambda m: os.getenv(m.group(1), ""), raw)


def load_config(path: Optional[Union[str, Path]] = None) -> AppConfig:
    resolved = Path(path).resolve() if path else CONFIG_PATH
    if not resolved.exists():
        raise FileNotFoundError(f"Config not found: {resolved}")
    with open(resolved, encoding="utf-8") as f:
        data = yaml.safe_load(_resolve_env_vars(f.read())) or {}
    try:
        return AppConfig.model_validate(data)
    except ValidationError as exc:
        lines = [f"Config validation failed: {resolved}"]
        for e in exc.errors():
            loc = " -> ".join(str(l) for l in e["loc"])
            lines.append(f"  [{loc}] {e['msg']}")
        raise ValueError("\n".join(lines)) from exc
