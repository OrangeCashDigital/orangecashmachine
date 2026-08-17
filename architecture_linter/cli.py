"""architecture_linter.cli — punto de entrada CLI.

Ejecución: `uv run python -m architecture_linter [--json] [--sarif] [--config PATH]`.

Exit codes:
  0 — todas las reglas PASS (o sin findings FAIL).
  1 — al menos una regla FAIL/PARTIAL.
  2 — error de ejecución (config inválida, syntax errors masivos, etc.).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from architecture_linter.config import load_config
from architecture_linter.engine import LinterEngine, RepoContext
from architecture_linter.models import Status
from architecture_linter.reporters import render_human, render_json, render_sarif
from architecture_linter.rules import build_rules, rule_ids


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="architecture_linter",
        description="Architecture Governance Linter para OCM (AST-based, stdlib-only).",
    )
    parser.add_argument("--config", type=Path, default=None, help="Ruta a architecture_linter.toml")
    parser.add_argument("--json", action="store_true", help="Emitir JSON (CI)")
    parser.add_argument("--sarif", action="store_true", help="Emitir SARIF 2.1.0")
    parser.add_argument("--root", type=Path, default=Path("."), help="Raíz del repositorio")
    parser.add_argument("--rules", type=str, default=None, help="Reglas a ejecutar (comma-separated); default todas")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = load_config(args.config)

    enabled: set[str] | None = None
    if args.rules:
        enabled = {r.strip() for r in args.rules.split(",") if r.strip()}
    elif cfg.enabled_rules:
        enabled = set(cfg.enabled_rules)

    # Un rule ID inexistente debe producir error explícito, no un PASS silencioso.
    if enabled is not None:
        known = set(rule_ids())
        unknown = enabled - known
        if unknown:
            print(
                f"ERROR: regla(s) desconocida(s): {', '.join(sorted(unknown))}. "
                f"Reglas válidas: {', '.join(sorted(known))}.",
                file=sys.stderr,
            )
            return 2

    root = args.root.resolve()
    ctx = RepoContext(
        root=root,
        roots=cfg.roots,
        exclude_dirs=frozenset(cfg.exclude_dirs),
    )

    rules = build_rules(
        enabled=enabled,
        severity_overrides=cfg.severity_overrides,
        allow=cfg.allow,
    )
    engine = LinterEngine(ctx)
    results = engine.run(rules)

    output: str
    if args.sarif:
        output = render_sarif(results, root)
    elif args.json:
        output = render_json(results, root)
    else:
        output = render_human(results, root)
    print(output)

    if any(r.status in (Status.FAIL, Status.PARTIAL) for r in results):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
