"""architecture_linter.rules — registro de reglas.

Las reglas no conocen el CLI; el registro las centraliza y permite
activar/desactivar y ajustar severidad vía configuración.
"""

from __future__ import annotations

from typing import Iterable

from architecture_linter.models import Severity
from architecture_linter.rules.arch_001 import Arch001Rule
from architecture_linter.rules.arch_002 import Arch002Rule
from architecture_linter.rules.arch_003 import Arch003Rule
from architecture_linter.rules.arch_004 import Arch004Rule
from architecture_linter.rules.arch_005 import Arch005Rule
from architecture_linter.rules.arch_006 import Arch006Rule
from architecture_linter.rules.arch_007 import Arch007Rule
from architecture_linter.rules.arch_008 import Arch008Rule
from architecture_linter.rules.arch_009 import Arch009Rule
from architecture_linter.rules.arch_010 import Arch010Rule
from architecture_linter.rules.base import Rule

ALL_RULES: list[type[Rule]] = [
    Arch001Rule,
    Arch002Rule,
    Arch003Rule,
    Arch004Rule,
    Arch005Rule,
    Arch006Rule,
    Arch007Rule,
    Arch008Rule,
    Arch009Rule,
    Arch010Rule,
]


def build_rules(
    enabled: set[str] | None = None,
    severity_overrides: dict[str, Severity] | None = None,
    allow: dict[str, list[str]] | None = None,
) -> list[Rule]:
    """Construye instancias de reglas según config (None = todas activas)."""
    rules: list[Rule] = []
    for rule_cls in ALL_RULES:
        rid = rule_cls.rule_id
        if enabled is not None and rid not in enabled:
            continue
        sev = severity_overrides.get(rid) if severity_overrides else None
        allowed = set(allow.get(rid, [])) if allow else set()
        rules.append(rule_cls(severity=sev, allow=allowed))
    return rules


def rule_ids() -> Iterable[str]:
    return (cls.rule_id for cls in ALL_RULES)
