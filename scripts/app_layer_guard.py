"""scripts/app_layer_guard.py — Guard AST del blindaje de la Application Layer.

Convierte hallazgos ya corregidos de `AUDIT-apps-2026-08-03.md` (H1, H4, H6, H8,
H12) en reglas estructurales verificables de forma automática
(PLAN-Maestro-Ingenieria.md §6): cada bug corregido pasa a ser una regla
permanente para que no vuelva a aparecer — la calidad no depende de disciplina
humana sino de mecanismos verificables por CI.

Escanea el TEXTO FUENTE vía AST, incluyendo imports lazy a nivel de función que
import-linter NO ve al analizar solo el grafo estático de módulos (la
Application Layer de apps/ usa lazy imports deliberados; E402 en apps/app/cli/).

Reglas ↔ hallazgos de AUDIT-apps-2026-08-03 (serie distinta de INFORME-2026-08-06
H-01…H-22; ver reporte BLINDAJE-APPS-2026-08-06.md §2):

    R12 → H1  el Namespace muere en el borde CLI; use_cases solo recibe config tipada
    R13 → H4  sin getattr con default en use_cases (config es la única fuente de verdad)
    R14 → H8  scaffolding de CLI en una sola fuente (app/cli/_bootstrap.py)
    R15 → H12 un único CycleRunResult (apps/app/use_cases/run_result.py)
    R16 → H6  SILENT_PATHS SSOT en api/middleware/__init__.py

Cada regla lleva prueba positiva (código limpio → sin violaciones) y negativa
(anti-patrón → violación): tests/architecture/test_app_layer_architecture.py.
Backtest obligatorio antes de activar en CI: scripts/backtest_app_guard.py.

Uso:
    from scripts.app_layer_guard import guard_app, check_*
    violations = guard_app(ROOT)
    assert not violations, "\\n".join(violations)
"""

from __future__ import annotations

import ast
from pathlib import Path

APP_DIR = "apps"
USE_CASES = "app/use_cases"
CLI_DIR = "app/cli"
BOOTSTRAP = "app/cli/_bootstrap.py"
MIDDLEWARE = "api/middleware"

# Constructores de config tipada que SOLO se permiten en el borde CLI.
CONFIG_CTORS = frozenset({"TradingConfig", "RiskConfig", "AppRiskConfig"})

# Helpers de scaffolding que viven exactamente en una sola fuente (_bootstrap.py).
FLOW_HELPERS = frozenset({"setup_logging", "handle_sigterm", "log_cycle_result", "assemble_cli_config"})

# Complejidad ciclomática máxima de main() en los CLIs Hydra (guard anti re-godificación).
MAIN_COMPLEXITY_MAX = 20

MIDDLEWARE_EXCLUSIONS = {
    "rate_limit.py": "RateLimitMiddleware",
    "logging.py": "RequestLoggingMiddleware",
}


# ── Helpers ───────────────────────────────────────────────────────────────────


def _py_files(directory: Path) -> list[Path]:
    if not directory.is_dir():
        return []
    return sorted(p for p in directory.rglob("*.py") if not p.name.startswith("test_"))


def _parse(path: Path) -> ast.AST:
    return ast.parse(path.read_text(encoding="utf-8"))


def _rel(path: Path, root: Path) -> str:
    return str(path.relative_to(root))


def _mentions(node: ast.AST, name: str) -> bool:
    return any(isinstance(n, ast.Name) and n.id == name for n in ast.walk(node))


def _mccabe(func: ast.FunctionDef) -> int:
    """Complejidad ciclomática aproximada de una función."""
    score = 1
    for node in ast.walk(func):
        if isinstance(
            node,
            (
                ast.If,
                ast.For,
                ast.AsyncFor,
                ast.While,
                ast.With,
                ast.AsyncWith,
                ast.ExceptHandler,
                ast.comprehension,
                ast.BoolOp,
            ),
        ):
            score += 1
    return score


# ── R12 → H1: use_cases no toca argparse/Namespace/constructores de config ────


def check_no_argparse_in_use_cases(root: Path) -> list[str]:
    out: list[str] = []
    for f in _py_files(root / APP_DIR / USE_CASES):
        rel = _rel(f, root)
        try:
            tree = _parse(f)
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Import) and any(
                a.name == "argparse" or a.name.startswith("argparse.") for a in node.names
            ):
                out.append(f"{rel}:{node.lineno}: R12/AUDIT-2026-08-03#H1 — import argparse prohibido en use_cases")
            elif isinstance(node, ast.ImportFrom) and node.module == "argparse":
                out.append(
                    f"{rel}:{node.lineno}: R12/AUDIT-2026-08-03#H1 — 'from argparse import ...' prohibido en use_cases"
                )
            elif isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "argparse":
                out.append(
                    f"{rel}:{node.lineno}: R12/AUDIT-2026-08-03#H1 — referencia a argparse prohibida en use_cases"
                )
    return sorted(out)


def check_no_config_ctor_in_use_cases(root: Path) -> list[str]:
    out: list[str] = []
    for f in _py_files(root / APP_DIR / USE_CASES):
        rel = _rel(f, root)
        try:
            tree = _parse(f)
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in CONFIG_CTORS:
                out.append(
                    f"{rel}:{node.lineno}: R12/AUDIT-2026-08-03#H1 — "
                    f"construcción de {node.func.id} prohibida en use_cases (la deriva el borde CLI vía model_copy)"
                )
    return sorted(out)


def check_no_namespace_in_use_cases(root: Path) -> list[str]:
    out: list[str] = []
    for f in _py_files(root / APP_DIR / USE_CASES):
        rel = _rel(f, root)
        try:
            tree = _parse(f)
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Name) and node.id == "Namespace":
                out.append(
                    f"{rel}:{node.lineno}: R12/AUDIT-2026-08-03#H1 — referencia a Namespace prohibida en use_cases"
                )
            elif isinstance(node, ast.Attribute) and node.attr == "Namespace":
                out.append(f"{rel}:{node.lineno}: R12/AUDIT-2026-08-03#H1 — atributo Namespace prohibido en use_cases")
    return sorted(out)


def check_no_vars_in_use_cases(root: Path) -> list[str]:
    out: list[str] = []
    for f in _py_files(root / APP_DIR / USE_CASES):
        rel = _rel(f, root)
        try:
            tree = _parse(f)
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "vars":
                out.append(
                    f"{rel}:{node.lineno}: R12/AUDIT-2026-08-03#H1 — vars() prohibido en use_cases (tipado explícito)"
                )
    return sorted(out)


# ── R13 → H4: sin getattr con default en use_cases ────────────────────────────


def check_no_getattr_default_in_use_cases(root: Path) -> list[str]:
    out: list[str] = []
    for f in _py_files(root / APP_DIR / USE_CASES):
        rel = _rel(f, root)
        try:
            tree = _parse(f)
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "getattr":
                has_default = len(node.args) > 2 or any(k.arg == "default" for k in node.keywords)
                if has_default:
                    out.append(
                        f"{rel}:{node.lineno}: R13/AUDIT-2026-08-03#H4 — "
                        "getattr con default prohibido en use_cases (config tipada es la única fuente; el "
                        "getattr de shutdown() es de 2 args y es legítimo)"
                    )
    return sorted(out)


# ── R14 → H8: scaffolding de CLI en una sola fuente (_bootstrap.py) ───────────


def check_no_sigterm_outside_bootstrap(root: Path) -> list[str]:
    out: list[str] = []
    bootstrap = (root / APP_DIR / BOOTSTRAP).resolve()
    for f in _py_files(root / APP_DIR / CLI_DIR):
        if f.resolve() == bootstrap:
            continue
        rel = _rel(f, root)
        try:
            tree = _parse(f)
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and "sigterm" in node.name.lower():
                out.append(
                    f"{rel}:{node.lineno}: R14/AUDIT-2026-08-03#H8 — "
                    f"handler de señal '{node.name}' definido fuera de {BOOTSTRAP}"
                )
    return sorted(out)


def check_no_logger_remove_outside_bootstrap(root: Path) -> list[str]:
    out: list[str] = []
    bootstrap = (root / APP_DIR / BOOTSTRAP).resolve()
    for f in _py_files(root / APP_DIR / CLI_DIR):
        if f.resolve() == bootstrap:
            continue
        rel = _rel(f, root)
        try:
            tree = _parse(f)
        except SyntaxError:
            continue
        # AST Call de `logger.remove(...)` — solo ejecutable, no comentarios/docstrings.
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "remove"
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "logger"
            ):
                out.append(
                    f"{rel}:{node.lineno}: R14/AUDIT-2026-08-03#H8 — logger.remove() re-inlineado fuera de {BOOTSTRAP}"
                    " (usar setup_logging de _bootstrap)"
                )
    return sorted(out)


def check_flow_helpers_single_source(root: Path) -> list[str]:
    out: list[str] = []
    bootstrap = (root / APP_DIR / BOOTSTRAP).resolve()
    for helper in sorted(FLOW_HELPERS):
        defs: list[tuple[Path, int]] = []
        for f in _py_files(root / APP_DIR / CLI_DIR):
            try:
                tree = _parse(f)
            except SyntaxError:
                continue
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef) and node.name == helper:
                    defs.append((f, node.lineno))
        if not defs:
            continue  # helper eliminado por refactor legítimo — no es duplicación
        if len(defs) > 1:
            for f, lineno in defs:
                out.append(
                    f"{_rel(f, root)}:{lineno}: R14/AUDIT-2026-08-03#H8 — "
                    f"helper '{helper}' duplicado; debe vivir solo en {BOOTSTRAP}"
                )
        elif defs[0][0].resolve() != bootstrap:
            out.append(
                f"{_rel(defs[0][0], root)}:{defs[0][1]}: R14/AUDIT-2026-08-03#H8 — "
                f"helper '{helper}' fuera de {BOOTSTRAP}"
            )
    return sorted(out)


def check_cli_main_not_god(root: Path) -> list[str]:
    out: list[str] = []
    for fname in ("live_hydra.py", "paper_hydra.py"):
        f = root / APP_DIR / CLI_DIR / fname
        if not f.is_file():
            continue
        rel = _rel(f, root)
        try:
            tree = _parse(f)
        except SyntaxError:
            continue
        for node in tree.body:  # main() de nivel de módulo
            if isinstance(node, ast.FunctionDef) and node.name == "main":
                complexity = _mccabe(node)
                if complexity > MAIN_COMPLEXITY_MAX:
                    out.append(
                        f"{rel}:{node.lineno}: R14/AUDIT-2026-08-03#H8 — "
                        f"main() con complejidad {complexity} > {MAIN_COMPLEXITY_MAX} "
                        "(re-godificación del CLI; extraer scaffolding a _bootstrap)"
                    )
    return sorted(out)


# ── R15 → H12: un único CycleRunResult ────────────────────────────────────────


def check_run_result_single_source(root: Path) -> list[str]:
    out: list[str] = []
    expected = (root / APP_DIR / "app" / "use_cases" / "run_result.py").resolve()
    found: Path | None = None
    for f in _py_files(root / APP_DIR / "app"):
        rel = _rel(f, root)
        try:
            tree = _parse(f)
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and node.name == "CycleRunResult":
                if found is None:
                    found = f
                else:
                    out.append(
                        f"{rel}:{node.lineno}: R15/AUDIT-2026-08-03#H12 — "
                        "CycleRunResult redefinido (SSOT en use_cases/run_result.py)"
                    )
            elif isinstance(node, ast.ClassDef) and node.name in {"LiveRunResult", "PaperRunResult"}:
                out.append(
                    f"{rel}:{node.lineno}: R15/AUDIT-2026-08-03#H12 — "
                    f"{node.name} prohibido; usar CycleRunResult único (use_cases/run_result.py)"
                )
    if found is not None and found.resolve() != expected:
        out.append(
            f"{_rel(found, root)}: R15/AUDIT-2026-08-03#H12 — CycleRunResult debe vivir en use_cases/run_result.py"
        )
    return sorted(out)


# ── R16 → H6: SILENT_PATHS SSOT y exclusión de probes en middleware ───────────


def check_silent_paths_single_source(root: Path) -> list[str]:
    out: list[str] = []
    expected = (root / APP_DIR / MIDDLEWARE / "__init__.py").resolve()
    defs: list[tuple[Path, int]] = []
    for f in _py_files(root / APP_DIR / MIDDLEWARE):
        try:
            tree = _parse(f)
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for t in node.targets:
                    if isinstance(t, ast.Name) and t.id == "SILENT_PATHS":
                        defs.append((f, node.lineno))
            elif (
                isinstance(node, ast.AnnAssign)
                and isinstance(node.target, ast.Name)
                and node.target.id == "SILENT_PATHS"
            ):
                defs.append((f, node.lineno))
    if len(defs) > 1 or (defs and defs[0][0].resolve() != expected):
        for f, lineno in defs:
            out.append(
                f"{_rel(f, root)}:{lineno}: R16/AUDIT-2026-08-03#H6 — "
                "SILENT_PATHS definido fuera de api/middleware/__init__.py (SSOT)"
            )
    return sorted(out)


def check_middleware_excludes_probes(root: Path) -> list[str]:
    out: list[str] = []
    for fname, cls_name in MIDDLEWARE_EXCLUSIONS.items():
        f = root / APP_DIR / MIDDLEWARE / fname
        if not f.is_file():
            continue
        rel = _rel(f, root)
        try:
            tree = _parse(f)
        except SyntaxError:
            continue

        imported = any(
            isinstance(n, ast.ImportFrom)
            and n.module == "api.middleware"
            and any(a.name == "SILENT_PATHS" for a in n.names)
            for n in ast.walk(tree)
        )
        if not imported:
            out.append(
                f"{rel}: R16/AUDIT-2026-08-03#H6 — "
                f"{fname} debe importar SILENT_PATHS desde api.middleware (probes excluidas)"
            )

        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and node.name == cls_name:
                for sub in ast.walk(node):
                    if isinstance(sub, (ast.AsyncFunctionDef, ast.FunctionDef)) and sub.name == "dispatch":
                        awaits = [n.lineno for n in ast.walk(sub) if isinstance(n, ast.Await)]
                        first_await = min(awaits) if awaits else None
                        excludes_early = any(
                            isinstance(stmt, ast.If)
                            and _mentions(stmt.test, "SILENT_PATHS")
                            and (first_await is None or stmt.lineno < first_await)
                            for stmt in sub.body
                        )
                        if not excludes_early:
                            out.append(
                                f"{rel}: R16/AUDIT-2026-08-03#H6 — "
                                f"{cls_name}.dispatch debe excluir SILENT_PATHS antes de procesar (probes no degradan)"
                            )
    return sorted(out)


# ── API pública ───────────────────────────────────────────────────────────────

CHECKS = (
    check_no_argparse_in_use_cases,
    check_no_config_ctor_in_use_cases,
    check_no_namespace_in_use_cases,
    check_no_vars_in_use_cases,
    check_no_getattr_default_in_use_cases,
    check_no_sigterm_outside_bootstrap,
    check_no_logger_remove_outside_bootstrap,
    check_flow_helpers_single_source,
    check_cli_main_not_god,
    check_run_result_single_source,
    check_silent_paths_single_source,
    check_middleware_excludes_probes,
)


def guard_app(root: Path) -> list[str]:
    """Devuelve todas las violaciones del blindaje de apps/ (vacío = reglas cumplidas)."""
    out: list[str] = []
    for check in CHECKS:
        out.extend(check(root))
    return sorted(set(out))
