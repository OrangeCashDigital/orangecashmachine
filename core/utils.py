from __future__ import annotations

"""
core/utils.py — Utilidades compartidas. Solo stdlib, sin dependencias internas.
"""

import subprocess


def get_git_hash() -> str:
    """Hash corto del commit actual para trazabilidad de lineage."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=2,
        )
        return result.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


__all__ = ["get_git_hash"]
