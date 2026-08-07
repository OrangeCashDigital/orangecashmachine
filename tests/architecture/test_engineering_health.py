"""
tests/architecture/test_engineering_health.py — Engineering Health Check (F2.0).

Ejecuta scripts/engineering_health_check.py como gate reproducible:
  Plan Maestro ↔ tracking.yaml ↔ ADR ↔ contratos ↔ CI (ver Plan §4 F2.0).

Es el pilar de la regla suprema: si los artefactos normativos divergen, el
check falla con exit 1 y bloquea el merge (fail-fast).

Backtest del guard:
  - Con tracking.yaml coherente → el script devuelve exit 0 y PASS.
  - (Regresión negativa no inyectada aquí: mutaría artefactos normativos en
    CI; la robustez del guard se valida por la propia ejecución.)
"""

from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
SCRIPT = ROOT / "scripts" / "engineering_health_check.py"


def test_engineering_health_passes() -> None:
    """El health check (Plan F2.0) debe pasar en un repo coherente."""
    proc = subprocess.run(
        ["uv", "run", "python", str(SCRIPT)],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, (
        f"Engineering Health Check (F2.0) FAIL — coherencia normativa rota:\n{proc.stdout}\n{proc.stderr}"
    )
    assert "PASS" in proc.stdout
