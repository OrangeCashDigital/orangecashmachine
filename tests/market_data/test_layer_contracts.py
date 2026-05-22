# -*- coding: utf-8 -*-
"""
tests/market_data/test_layer_contracts.py — pytest wrapper for the AST linter.

Delegates technology governance checks to scripts/forbidden_frameworks.py.
Keeps the test interface (pytest) for discoverability and CI integration.
"""

from __future__ import annotations

import pytest

from scripts.forbidden_frameworks import (
    DOMAIN_ROOT,
    collect_violations,
    find_stale_exceptions,
)


def test_domain_no_forbidden_framework_imports() -> None:
    if not DOMAIN_ROOT.exists():
        pytest.skip(f"Domain root not found: {DOMAIN_ROOT}")

    violations = collect_violations()
    if not violations:
        return

    from scripts.forbidden_frameworks import format_violations

    pytest.fail(format_violations(violations))


def test_allowed_exceptions_still_exist() -> None:
    if not DOMAIN_ROOT.exists():
        pytest.skip(f"Domain root not found: {DOMAIN_ROOT}")

    stale = find_stale_exceptions()
    if not stale:
        return

    from scripts.forbidden_frameworks import format_stale

    pytest.fail(format_stale(stale))
