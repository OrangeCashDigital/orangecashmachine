"""
tests/architecture/test_import_linter_no_vacuo — guard no-vacuo del import-linter.

Complementa el job CI `architecture` (que ejecuta `lint-imports`): detecta el
caso en el que import-linter "pasa" de forma vacua porque la configuración que
se le pasa no existe (salida 0 con "Could not find ...toml."). Un gate real
debe fallar cuando el conteo de contratos baja del baseline o cuando no se
ejecuta ninguna contract.

Hallazgo: auditoría externa (2026-08) reportó que un `--config` roto devuelve
salida 0 (falso verde). Verificado en vivo.

No es un test de lógica — es un test de *política de arquitectura*.
Fallar aquí = gate de arquitectura evasivo, no un bug de negocio.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
CONFIG = ROOT / "architecture_linter" / "importlinter.toml"

# Baseline F2.1 — SSOT de contratos BC-NN en architecture_linter/importlinter.toml.
MIN_CONTRACTS_KEPT = 49


def _run_lint_imports(config: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["uv", "run", "lint-imports", "--config", str(config)],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )


def test_import_linter_reports_kept_contracts() -> None:
    """Debe reportar >=MIN_CONTRACTS_KEPT kept, 0 broken; falla si baja del baseline."""
    proc = _run_lint_imports(CONFIG)
    combined = proc.stdout + proc.stderr
    assert proc.returncode == 0, f"lint-imports falló:\n{combined}"
    assert "Could not find" not in combined, f"Config no encontrada:\n{combined}"

    summary = [ln for ln in proc.stdout.splitlines() if "Contracts:" in ln]
    assert summary, f"Sin resumen 'Contracts:' en salida:\n{proc.stdout}"

    line = summary[-1]
    match = re.search(r"(\d+)\s+kept,\s+(\d+)\s+broken", line)
    assert match, f"No se pudo parsear conteo desde: {line!r}"
    kept, broken = int(match.group(1)), int(match.group(2))
    assert broken == 0, f"Hay {broken} contratos rotos:\n{proc.stdout}"
    assert kept >= MIN_CONTRACTS_KEPT, (
        f"Contratos activos {kept} < baseline {MIN_CONTRACTS_KEPT}. "
        "Bajar el conteo requiere ADR + tracking.yaml (F2.1)."
    )


def test_broken_config_must_fail_the_pipeline() -> None:
    """Guard no-vacuo: config rota debe fallar CI, no devolver salida 0.

    Reproduce el falso verde histórico (bug reportado por la auditoría) para
    que quede como regresión: si vuelve a salida 0, este test lo bloquea.
    """
    proc = _run_lint_imports(ROOT / "architecture" / "does-not-exist.toml")
    combined = proc.stdout + proc.stderr
    assert "Could not find" in combined, f"Config rota debería mencionar 'Could not find':\n{combined}"
    assert proc.returncode != 0, f"BUG no-vacuo: config rota devolvió salida 0. CI debe fallar aquí.\n{combined}"
