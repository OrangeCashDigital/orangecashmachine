#!/usr/bin/env python3
"""
scripts/forbidden_frameworks.py — AST linter for technology governance.

Checks that domain/ never imports heavy data or infrastructure frameworks.
Invariants are checked at the AST level (static imports only).

Uso
---
    uv run python scripts/forbidden_frameworks.py         # human-readable
    uv run python scripts/forbidden_frameworks.py --json   # JSON output
    uv run python scripts/forbidden_frameworks.py --strict # exit 1 on violations

Exit codes
----------
    0 — all clean
    1 — violations found or stale exceptions detected
"""

from __future__ import annotations

import ast
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Raíz del bounded context a inspeccionar
DOMAIN_ROOT = ROOT / "packages" / "market_data" / "domain"

# Frameworks prohibidos en domain/ — cualquier import que empiece con
# alguno de estos prefijos es una violación.
FORBIDDEN_PREFIXES: tuple[str, ...] = (
    "pandas",
    "pandera",
    "polars",
    "numpy",
    "sqlalchemy",
    "redis",
    "ccxt",
    "dagster",
    "prometheus_client",
    "loguru",
    "requests",
    "httpx",
    "aiohttp",
    "boto3",
    "pyarrow",
)

# Excepciones explícitas: (ruta relativa a DOMAIN_ROOT, módulo_prohibido)
# Cada excepción documenta deuda técnica pendiente de una fase concreta.
ALLOWED_EXCEPTIONS: frozenset[tuple[str, str]] = frozenset(
    [
        # Fase 2: polars estructural — scan_gaps() opera sobre pl.DataFrame
        # Migrar cuando se abstraiga el protocolo de timestamps a un VO propio.
        ("value_objects/gap_scanner.py", "polars"),
        # Fase 2: polars estructural — align_to_grid() opera sobre pl.DataFrame
        # Migrado de pandas en esta fase; pendiente abstracción del protocolo.
        ("value_objects/grid_alignment.py", "polars"),
        # Fase 2: polars+pandera.polars estructural — validate_ohlcv() usa DataFrameSchema
        # Migrado de pandera.pandas; pendiente extracción a puerto de validación.
        ("value_objects/ohlcv_schema.py", "polars"),
        ("value_objects/ohlcv_schema.py", "pandera"),
        # Fase 2: loguru estructural — logging en value objects y policies
        # Pendiente: extraer logging a puerto de observabilidad en domain.
        ("value_objects/ohlcv_schema.py", "loguru"),
        ("policies/data_quality_policy.py", "loguru"),
    ]
)


def collect_top_level_imports(source: str) -> list[tuple[str, int]]:
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []

    imports: list[tuple[str, int]] = []
    for child in ast.iter_child_nodes(tree):
        if isinstance(child, ast.Import):
            for alias in child.names:
                imports.append((alias.name, child.lineno))
        elif isinstance(child, ast.ImportFrom):
            module = child.module or ""
            imports.append((module, child.lineno))

    return imports


def is_forbidden(module_name: str) -> str | None:
    for prefix in FORBIDDEN_PREFIXES:
        if module_name == prefix or module_name.startswith(f"{prefix}."):
            return prefix
    return None


def collect_violations(
    domain_root: Path | None = None,
) -> list[tuple[Path, int, str, str]]:
    root = domain_root or DOMAIN_ROOT
    violations: list[tuple[Path, int, str, str]] = []

    for py_file in sorted(root.rglob("*.py")):
        rel_path = py_file.relative_to(root)
        rel_str = str(rel_path)

        source = py_file.read_text(encoding="utf-8")
        imports = collect_top_level_imports(source)

        for module_name, lineno in imports:
            prefix = is_forbidden(module_name)
            if prefix is None:
                continue
            if (rel_str, prefix) in ALLOWED_EXCEPTIONS:
                continue
            violations.append((py_file, lineno, module_name, prefix))

    return violations


def find_stale_exceptions(
    domain_root: Path | None = None,
) -> list[tuple[str, str]]:
    root = domain_root or DOMAIN_ROOT
    stale: list[tuple[str, str]] = []

    for rel_str, framework in ALLOWED_EXCEPTIONS:
        filepath = root / rel_str
        if not filepath.exists():
            stale.append((rel_str, framework))
            continue

        source = filepath.read_text(encoding="utf-8")
        imports = collect_top_level_imports(source)
        module_names = {m for m, _ in imports}

        still_used = any(m == framework or m.startswith(f"{framework}.") for m in module_names)
        if not still_used:
            stale.append((rel_str, framework))

    return stale


def format_violations(violations: list[tuple[Path, int, str, str]]) -> str:
    if not violations:
        return ""

    lines = [
        "",
        "╔══ DOMAIN LAYER CONTRACT VIOLATION ══════════════════════════════════╗",
        "║  domain/ importa frameworks de datos/infra prohibidos.              ║",
        "║  El dominio debe ser independiente de implementación.               ║",
        "╚══════════════════════════════════════════════════════════════════════╝",
        "",
        f"  {len(violations)} violación(es) encontrada(s):",
        "",
    ]
    for filepath, lineno, import_name, prefix in violations:
        lines.append(f"  ✗ {filepath}:{lineno}")
        lines.append(f"      import detectado  : {import_name!r}")
        lines.append(f"      framework prohibido: {prefix!r}")
        lines.append("")

    lines += [
        "  Excepciones documentadas actuales (deuda Fase 2):",
    ]
    for rel_path, framework in sorted(ALLOWED_EXCEPTIONS):
        lines.append(f"    • domain/{rel_path}  [{framework}]")

    lines += [
        "",
        "  Para añadir una excepción temporal documentada:",
        '    Añadir a ALLOWED_EXCEPTIONS: ("path/relativo.py", "framework")',
        "  con comentario obligatorio de fase/ticket.",
        "",
    ]
    return "\n".join(lines)


def format_stale(stale: list[tuple[str, str]]) -> str:
    if not stale:
        return ""

    lines = [
        "",
        "╔══ STALE EXCEPTION IN ALLOWED_EXCEPTIONS ════════════════════════════╗",
        "║  Una excepción documentada ya no es necesaria.                       ║",
        "║  El framework fue migrado pero la excepción no fue removida.         ║",
        "║  Remover la entrada de ALLOWED_EXCEPTIONS para mantener SSOT.      ║",
        "╚══════════════════════════════════════════════════════════════════════╝",
        "",
    ]
    for rel_str, framework in stale:
        lines.append(f"  ✗ domain/{rel_str}  [{framework!r}] ya no importa este framework")
    lines.append("")
    return "\n".join(lines)


def run_checks(
    domain_root: Path | None = None,
) -> tuple[list[tuple[Path, int, str, str]], list[tuple[str, str]]]:
    violations = collect_violations(domain_root)
    stale = find_stale_exceptions(domain_root)
    return violations, stale


def main() -> int:
    use_json = "--json" in sys.argv
    strict = "--strict" in sys.argv
    domain_arg = None

    for i, arg in enumerate(sys.argv[1:], 1):
        if arg == "--domain-root" and i + 1 < len(sys.argv):
            domain_arg = Path(sys.argv[i + 1])

    violations, stale = run_checks(domain_arg)

    has_violations = bool(violations)
    has_stale = bool(stale)
    failed = has_violations or (strict and has_stale)

    if use_json:
        result = {
            "violations": [
                {
                    "file": str(f),
                    "line": ln,
                    "import": imp,
                    "forbidden": pref,
                }
                for f, ln, imp, pref in violations
            ],
            "stale_exceptions": [{"file": f, "framework": fr} for f, fr in stale],
            "status": "fail" if failed else "pass",
        }
        print(json.dumps(result, indent=2))
    else:
        if has_violations:
            print(format_violations(violations))
        if has_stale:
            print(format_stale(stale))

        if not has_violations and not has_stale:
            print("✓ No forbidden framework imports found in domain/")
            print("✓ All exceptions are still necessary")

    if has_stale and not strict:
        print("\n⚠️  Stale exceptions detected (non-strict mode, exit 0)")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
