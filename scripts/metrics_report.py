#!/usr/bin/env python3
"""Informe de salud del Shared Kernel: contratos, mypy, pytest, pip-audit.

Genera architecture/metrics.json con las métricas clave de gobernanza.
En CI se sube como artifact (no se commitea). Uso local:

    uv run python scripts/metrics_report.py
"""

import json
import re
import subprocess
from pathlib import Path


def _run(cmd: list[str]) -> str:
    r = subprocess.run(cmd, capture_output=True, text=True)
    return r.stdout + r.stderr


def _contracts() -> tuple[int, int]:
    out = _run(["uv", "run", "lint-imports", "--config", "architecture/importlinter.toml"])
    m = re.search(r"Contracts: (\d+) kept, (\d+) broken\.", out)
    return (int(m.group(1)), int(m.group(2))) if m else (0, 0)


def _mypy_errors() -> int:
    out = _run(["uv", "run", "mypy", "shared/"])
    if "Success: no issues found" in out:
        return 0
    m = re.search(r"Found (\d+) errors?", out)
    return int(m.group(1)) if m else -1


def _pytest_passed() -> int:
    out = _run(["uv", "run", "pytest", "tests/", "-q", "--no-header"])
    m = re.search(r"(\d+) passed", out)
    return int(m.group(1)) if m else 0


def _audit_vulns() -> int:
    r = subprocess.run(
        ["uv", "run", "pip-audit", "-l", "-f", "json"],
        capture_output=True,
        text=True,
    )
    try:
        data = json.loads(r.stdout or r.stderr)
        deps = data.get("dependencies", [])
        return sum(len(d.get("vulns", [])) for d in deps)
    except json.JSONDecodeError:
        return -1


def main() -> None:
    kept, broken = _contracts()
    report = {
        "contracts_kept": kept,
        "contracts_broken": broken,
        "mypy_errors": _mypy_errors(),
        "pytest_passed": _pytest_passed(),
        "vulnerabilities": _audit_vulns(),
    }
    print(json.dumps(report, indent=2))
    Path("architecture/metrics.json").write_text(json.dumps(report, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
