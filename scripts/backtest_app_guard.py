"""scripts/backtest_app_guard.py — Backtest del guard AST contra el historial.

Gate de confianza del blindaje de apps/ (AUDIT-apps-2026-08-03, R12–R16): el
guard debe DETECTAR los hallazgos históricos en sus snapshots pre-fix y quedar
LIMPIO en HEAD. Requisito del PLAN-Maestro-Ingenieria.md §6: un bug corregido
pasa a ser una regla permanente — y esa regla debe poder demostrar que habría
detectado el bug original (cero falsos negativos) y que no alarma código ya
limpio (cero falsos positivos).

Snapshots (rama main, via `git archive` — no toca el working tree):

    39687e7  docs: consolidate composition-root audit into AUDIT-apps-2026-08-03
             → pre-fix H1/H4/H6/H8/H12 → deben disparar R12, R13, R14, R15, R16
    cdd7e7e  refactor(apps): extract CLI bootstrap and unify CycleRunResult
             → post-H1/H4/H8/H12, pre-H6 → debe disparar solo R16
    HEAD     → post-todo → 0 violaciones

Uso:
    uv run python scripts/backtest_app_guard.py
    uv run python scripts/backtest_app_guard.py --snapshot 39687e7

Exit 0 = el guard cumple su promesa; != 0 = regresión del blindaje (bloquea merge).
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

from app_layer_guard import CHECKS, guard_app

RULE_TAG = re.compile(r"R(?P<rule>\d+)/AUDIT-2026-08-03#H(?P<finding>\d+)")


@dataclass(frozen=True)
class Snapshot:
    commit: str
    expected_rules: frozenset[str]
    description: str


# Cada snapshot es el ÚLTIMO commit ANTES de su fix, es decir el estado "buggy"
# que la regla debe haber detectado en su día (no un commit posterior ya limpio):
#   · cdd7e7e "extract CLI bootstrap and unify CycleRunResult" corrigió H1/H4/H8/H12
#     → su padre 39687e7 es el estado pre-fix con todos los anti-patrones vivos.
#   · 2717d06 "tighten rate-limit middleware" corrigió H6 (R16) → su padre cdd7e7e
#     ya no tiene H1/H4/H8/H12 pero todavía conserva H6.
# Al fijar estos parents como referencia, el backtest demuestra que el guard es
# sensible a la reintroducción exacta de cada hallazgo, sin depender de fechas.
SNAPSHOTS = (
    Snapshot(
        commit="39687e7",
        expected_rules=frozenset({"R12", "R13", "R14", "R15", "R16"}),
        description="pre-fix H1/H4/H6/H8/H12 (padre de cdd7e7e)",
    ),
    Snapshot(
        commit="cdd7e7e",
        expected_rules=frozenset({"R16"}),
        description="post-H1/H4/H8/H12, pre-H6 (padre de 2717d06)",
    ),
)


def _rules_fired(violations: list[str]) -> set[str]:
    rules: set[str] = set()
    for violation in violations:
        m = RULE_TAG.search(violation)
        if m:
            rules.add(f"R{m.group('rule')}")
    return rules


def _snapshot_apps(commit: str, dst: Path) -> Path:
    """Extrae apps/ de un commit a un directorio temporal vía git archive.

    Devuelve la base temporal (contenedor de apps/), que es la raiz que espera
    guard_app(root): el guard escanea root/APP_DIR.
    """
    tar = dst / "apps.tar"
    subprocess.run(
        ["git", "archive", commit, "apps/", "-o", str(tar)],
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(["tar", "-xf", str(tar), "-C", str(dst)], check=True)
    return dst


def _run(commit: str) -> tuple[list[str], set[str]]:
    with tempfile.TemporaryDirectory(prefix="app-guard-backtest-") as tmp:
        base = _snapshot_apps(commit, Path(tmp))
        violations = guard_app(base)
        return violations, _rules_fired(violations)


def _check_snapshot(snapshot: Snapshot) -> tuple[bool, set[str], set[str]]:
    _, fired = _run(snapshot.commit)
    missing = set(snapshot.expected_rules) - fired
    unexpected = fired - snapshot.expected_rules
    ok = not missing and not unexpected
    return ok, missing, unexpected


def _check_head(root: Path) -> tuple[bool, list[str]]:
    violations = guard_app(root)
    return (not violations), violations


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--snapshot",
        choices=[s.commit for s in SNAPSHOTS],
        help="Solo este snapshot (HEAD se sigue verificando)",
    )
    args = parser.parse_args(argv)

    root = Path(__file__).resolve().parents[1]
    failed = False

    if args.snapshot:
        selected = [s for s in SNAPSHOTS if s.commit == args.snapshot]
    else:
        selected = list(SNAPSHOTS)

    for snapshot in selected:
        ok, missing, unexpected = _check_snapshot(snapshot)
        status = "OK " if ok else "FAIL"
        print(f"[{status}] {snapshot.commit}  ({snapshot.description})")
        if missing:
            print(f"       reglas que NO disparan (falso negativo): {sorted(missing)}")
        if unexpected:
            print(f"       reglas inesperadas (falso positivo):      {sorted(unexpected)}")
        failed = failed or not ok

    head_ok, head_violations = _check_head(root)
    status = "OK " if head_ok else "FAIL"
    print(f"[{status}] HEAD  (working tree: {len(head_violations)} violaciones)")
    if not head_ok:
        for v in head_violations[:10]:
            print(f"       {v}")
    failed = failed or not head_ok

    print(f"\nContratos: {len(CHECKS)} checks del guard | Backtest: {'PASS' if not failed else 'FAIL'}")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
