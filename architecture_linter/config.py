"""architecture_linter.config — carga de configuración TOML.

La configuración no modifica reglas; solo ajusta severidad, activación,
roots y exclusiones. Sin configuración → defaults sensatos y todas las
reglas activas.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path

from architecture_linter.models import Severity

DEFAULT_CONFIG_PATH = Path("architecture_linter/architecture_linter.toml")


@dataclass
class LinterConfig:
    roots: list[str] = field(default_factory=lambda: ["packages", "shared", "apps", "ocm"])
    exclude_dirs: list[str] = field(default_factory=lambda: [".venv", "__pycache__", ".git", ".pytest_cache"])
    enabled_rules: list[str] | None = None  # None = todas
    severity: dict[str, str] = field(default_factory=dict)  # rule_id → "error"|"warning"|"info"
    allow: dict[str, list[str]] = field(default_factory=dict)  # rule_id → símbolos permitidos
    exclude_paths: list[str] = field(default_factory=list)  # subcadenas de path a excluir
    config_path: Path = DEFAULT_CONFIG_PATH

    @property
    def severity_overrides(self) -> dict[str, Severity]:
        out: dict[str, Severity] = {}
        for rid, sev in self.severity.items():
            try:
                out[rid] = Severity(sev)
            except ValueError:
                continue
        return out


def load_config(path: Path | None = None) -> LinterConfig:
    cfg_path = path or DEFAULT_CONFIG_PATH
    cfg = LinterConfig(config_path=cfg_path)
    if not cfg_path.is_file():
        return cfg
    try:
        with cfg_path.open("rb") as fh:
            data = tomllib.load(fh)
    except (OSError, tomllib.TOMLDecodeError):
        return cfg

    linter = data.get("linter", {})
    if isinstance(linter.get("roots"), list):
        cfg.roots = [str(r) for r in linter["roots"]]
    if isinstance(linter.get("exclude_dirs"), list):
        cfg.exclude_dirs = [str(d) for d in linter["exclude_dirs"]]
    if isinstance(linter.get("enabled_rules"), list):
        cfg.enabled_rules = [str(r) for r in linter["enabled_rules"]]
    if isinstance(linter.get("severity"), dict):
        cfg.severity = {str(k): str(v) for k, v in linter["severity"].items()}
    if isinstance(linter.get("allow"), dict):
        cfg.allow = {str(k): [str(s) for s in v] for k, v in linter["allow"].items()}
    if isinstance(linter.get("exclude_paths"), list):
        cfg.exclude_paths = [str(p) for p in linter["exclude_paths"]]
    return cfg
