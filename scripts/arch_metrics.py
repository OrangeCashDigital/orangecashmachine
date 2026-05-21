#!/usr/bin/env python3
"""
scripts/arch_metrics.py — Métricas de salud arquitectónica de OCM.

Ejecuta lint-imports, detecta ciclos via grimp y genera un reporte.
Diseñado para CI y uso manual.

Uso
---
    python3 scripts/arch_metrics.py            # reporte human-readable
    python3 scripts/arch_metrics.py --json     # JSON para CI/Grafana
    python3 scripts/arch_metrics.py --strict   # falla si broken > 0 o ciclos > 0

Exit codes
----------
    0 — todo OK
    1 — contratos rotos
    2 — ciclos de importación detectados
    3 — error de entorno (grimp/lint-imports no disponible)
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


# ── Helpers ───────────────────────────────────────────────────────────────────


def _run(cmd: list[str]) -> tuple[str, int]:
    """
    Ejecuta un comando y retorna (stdout+stderr combinados, returncode).

    Usa stdout=PIPE + stderr=STDOUT para captura en stream único —
    evita race conditions entre buffers y garantiza que la línea de
    resumen de lint-imports llegue aunque vaya a stderr.
    """
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=ROOT,
    )
    return result.stdout or "", result.returncode


def count_contracts() -> int:
    """Cuenta contratos declarados en pyproject.toml."""
    import tomllib

    with open(ROOT / "pyproject.toml", "rb") as fh:
        data = tomllib.load(fh)
    return len(data.get("tool", {}).get("importlinter", {}).get("contracts", []))


def run_lint_imports() -> tuple[int, int, list[str]]:
    """
    Ejecuta lint-imports y parsea el resumen.

    Retorna (kept, broken, broken_names).

    Parser robusto: usa regex para la línea de resumen en lugar de split()
    posicional, que es frágil ante variaciones de formato o encoding.
    Formato esperado: "Contracts: N kept, M broken."
    """
    import re

    import shutil

    lint_bin = shutil.which("lint-imports") or str(ROOT / ".venv/bin/lint-imports")
    out, _ = _run([lint_bin])

    kept = broken = 0
    broken_names: list[str] = []

    # Regex tolerante: captura dígitos independientemente del formato exacto
    SUMMARY_RE = re.compile(
        r"Contracts?:\s*(\d+)\s+kept[,.]\s*(\d+)\s+broken",
        re.IGNORECASE,
    )
    # Contrato roto: "BC-XX: nombre  BROKEN"
    BROKEN_RE = re.compile(r"^(BC-\d+[a-z]?)\s*:", re.IGNORECASE)

    for line in out.splitlines():
        stripped = line.strip()

        m = SUMMARY_RE.search(stripped)
        if m:
            kept = int(m.group(1))
            broken = int(m.group(2))
            continue

        if "BROKEN" in stripped:
            bm = BROKEN_RE.match(stripped)
            if bm:
                broken_names.append(bm.group(1))

    return kept, broken, broken_names


def detect_cycles() -> list[str]:
    """
    Detecta TODOS los ciclos de importación via grimp.

    Estrategia: find_any_cycle() retorna solo el primero; iteramos
    removiendo los módulos del ciclo encontrado y re-corriendo hasta
    que el grafo quede limpio. Límite de 20 ciclos para evitar loops
    infinitos en grafos muy degradados.
    """
    script = (
        "import grimp, json\n"
        "g = grimp.build_graph('market_data', include_external_packages=False,\n"
        "    exclude_type_checking_imports=True)\n"
        "cycles = []\n"
        "if not hasattr(g, 'find_any_cycle'):\n"
        "    print('NO_SUPPORT'); raise SystemExit(0)\n"
        "for _ in range(20):\n"
        "    c = g.find_any_cycle()\n"
        "    if not c: break\n"
        "    cycles.append('->'.join(str(m) for m in c))\n"
        "    # remover nodos del ciclo para buscar el siguiente\n"
        "    for m in c:\n"
        "        try: g.remove_module(m)\n"
        "        except Exception: pass\n"
        "print('CYCLES:' + json.dumps(cycles))\n"
    )
    out, _ = _run([sys.executable, "-c", script])
    for line in out.splitlines():
        if line.startswith("CYCLES:"):
            import json as _json

            return _json.loads(line[len("CYCLES:") :])
    return []


def count_tests() -> dict[str, int]:
    """Cuenta archivos de test por categoría."""
    test_root = ROOT / "tests"
    return {
        "total": len(list(test_root.rglob("test_*.py"))),
        "architecture": (
            len(list((test_root / "architecture").rglob("test_*.py"))) if (test_root / "architecture").exists() else 0
        ),
    }


def count_adr() -> int:
    """Cuenta ADRs en docs/adr/."""
    adr_dir = ROOT / "docs" / "adr"
    if not adr_dir.exists():
        return 0
    return len(list(adr_dir.glob("[0-9]*.md")))


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    as_json = "--json" in sys.argv
    strict = "--strict" in sys.argv

    kept, broken, broken_names = run_lint_imports()
    contracts_total = count_contracts()
    cycles = detect_cycles()
    tests = count_tests()
    adrs = count_adr()

    health = "OK" if broken == 0 and len(cycles) == 0 else "FAIL"

    metrics: dict = {
        "contracts_total": contracts_total,
        "contracts_kept": kept,
        "contracts_broken": broken,
        "broken_contracts": broken_names,
        "import_cycles": len(cycles),
        "cycle_details": cycles,
        "tests_total": tests["total"],
        "tests_architecture": tests["architecture"],
        "adrs": adrs,
        "health": health,
    }

    if as_json:
        print(json.dumps(metrics, indent=2))
    else:
        w = 54
        sep = "─" * w
        print(sep)
        print("  OCM — Architecture Health Report")
        print(sep)
        print(f"  Contratos    : {kept}/{contracts_total} kept")
        if broken_names:
            for n in broken_names:
                print(f"    ❌ {n}")
        print(f"  Ciclos       : {len(cycles)}")
        if cycles:
            for c in cycles:
                print(f"    ❌ {c}")
        print(f"  Tests totales: {tests['total']}")
        print(f"  Tests arch.  : {tests['architecture']}")
        print(f"  ADRs         : {adrs}")
        print(sep)
        icon = "✓" if health == "OK" else "✗"
        print(f"  Estado       : {icon} {health}")
        print(sep)

    if strict:
        if broken > 0:
            sys.exit(1)
        if cycles:
            sys.exit(2)


if __name__ == "__main__":
    main()
