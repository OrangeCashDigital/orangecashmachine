"""Helpers compartidos para tests del Architecture Governance Linter.

Construyen RepoContext sobre árboles temporales (fixtures) para probar cada
regla de forma aislada, y un helper para el repo OCM real (golden).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from architecture_linter.engine import RepoContext


@pytest.fixture
def make_repo(tmp_path: Path):
    """Devuelve una función que crea un RepoContext sobre un árbol temporal.

    Uso:
        def test_x(make_repo):
            ctx = make_repo({
                "packages/trading/execution/oms.py": "...código...",
                "packages/trading/risk/manager.py": "...código...",
            })
    """

    def _make_repo(files: dict[str, str], roots: list[str] | None = None) -> RepoContext:
        for relpath, content in files.items():
            p = tmp_path / relpath
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(content, encoding="utf-8")
        return RepoContext(
            root=tmp_path,
            roots=roots or ["packages", "shared", "apps", "ocm"],
        )

    return _make_repo


@pytest.fixture
def repo_root() -> Path:
    """Raíz del repo OCM real (para golden tests)."""
    return Path(__file__).resolve().parents[2]
