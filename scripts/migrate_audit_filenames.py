#!/usr/bin/env python3
"""Migración determinista de nombres de docs/audits a la gramática canónica."""

from __future__ import annotations

import argparse
import re
import subprocess
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
AUDITS = ROOT / "docs" / "audits"

DATE_RE = re.compile(r"(?<!\d)(20\d{2}-\d{2}-\d{2})(?!\d)")

REPORT_RE = re.compile(
    r"^AUDIT_OCM_(?P<slug>.+)_(?P<date>\d{4}-\d{2}-\d{2})"
    r"(?:_(?P<seq>\d{2}))?\.md$"
)

REGISTER_RE = re.compile(
    r"^OCM_AUDIT_FINDINGS_(?P<date>\d{4}-\d{2}-\d{2})_"
    r"(?P<slug>.+?)(?:_(?P<seq>\d{2}))?\.md$"
)


def slugify(value: str) -> str:
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = re.sub(r"-+", "-", value).strip("-")
    return value or "audit"


def extract_date(path: Path, text: str) -> str | None:
    match = DATE_RE.search(path.name)
    if match:
        return match.group(1)

    match = DATE_RE.search(text[:5000])
    return match.group(1) if match else None


def classify(path: Path, text: str) -> str:
    name = path.name

    if REPORT_RE.fullmatch(name):
        return "CANONICAL_REPORT"

    if REGISTER_RE.fullmatch(name):
        return "CANONICAL_REGISTER"

    if name.startswith("OCM_AUDIT_FINDINGS_"):
        return "FINDINGS_REGISTER"

    if re.search(r"^##\s+F-\S+\s*[—-]", text, re.MULTILINE):
        return "FINDINGS_REGISTER"

    if re.search(r"^##\s+Matriz de Findings\b", text, re.MULTILINE):
        return "REPORT"

    if re.search(r"^#.*audit|^##.*audit|AUDIT", text, re.I | re.M):
        return "AUDIT"

    return "OTHER"


def proposed_name(path: Path, text: str) -> str | None:
    kind = classify(path, text)
    name = path.stem

    if kind in {"CANONICAL_REPORT", "CANONICAL_REGISTER"}:
        return None

    # Findings registers: la identidad viene del nombre actual.
    if kind == "FINDINGS_REGISTER":
        match = re.match(
            r"^OCM_AUDIT_FINDINGS_(?P<date>\d{4}-\d{2}-\d{2})(?:[-_](?P<slug>.*))?$",
            name,
        )
        if not match:
            return None

        date = match.group("date")
        slug = slugify(match.group("slug") or "audit")
        return f"OCM_AUDIT_FINDINGS_{date}_{slug}.md"

    # Para auditorías, primero usamos la identidad semántica del nombre.
    # Nunca sustituimos el slug por un título encontrado arbitrariamente
    # dentro del contenido.
    report_name = re.sub(
        r"^AUDIT_",
        "",
        name,
        flags=re.IGNORECASE,
    )
    report_name = re.sub(
        r"^OCM_",
        "",
        report_name,
        flags=re.IGNORECASE,
    )

    # Fecha completa explícita en el nombre.
    full_date = re.search(
        r"(?P<date>20\d{2}-\d{2}-\d{2})",
        report_name,
    )

    if full_date:
        date = full_date.group("date")
        slug_part = report_name[: full_date.start()].rstrip("-_")

        # Casos como:
        # AUDIT_OCM_FORENSIC_COMPLIANCE_2026-08-18-b20
        # conservan el sufijo como parte del slug.
        suffix = report_name[full_date.end() :].lstrip("-_")

        if suffix:
            slug_part = f"{slug_part}-{suffix}"

        slug = slugify(slug_part)
        if not slug:
            slug = "audit"

        return f"AUDIT_OCM_{slug}_{date}.md"

    # Nombres antiguos con solo YYYY-MM, por ejemplo:
    # 2026-08-apps-audit.md
    # La fecha completa debe salir del contenido, pero el slug debe
    # eliminar exclusivamente el prefijo YYYY-MM del nombre.
    partial_date = re.match(r"^(20\d{2}-\d{2})[-_](.+)$", report_name)

    if partial_date:
        slug_part = partial_date.group(2)
        content_date = extract_date(path, text)

        if not content_date:
            return None

        slug = slugify(slug_part)
        return f"AUDIT_OCM_{slug}_{content_date}.md"

    # Auditorías tipo AUDIT_HOME_CLEANUP_2026-08-18 ya fueron cubiertas
    # arriba. Si no existe fecha en el nombre, buscamos una fecha en el
    # contenido, pero conservamos el nombre como slug.
    date = extract_date(path, text)
    if not date:
        return None

    slug = slugify(report_name)
    return f"AUDIT_OCM_{slug}_{date}.md"


def git_status(path: Path) -> str:
    result = subprocess.run(
        ["git", "status", "--short", "--", str(path)],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    return result.stdout.strip()


def build_plan() -> list[tuple[Path, Path]]:
    plans: list[tuple[Path, Path]] = []

    for path in sorted(AUDITS.glob("*.md")):
        text = path.read_text(encoding="utf-8", errors="replace")
        target_name = proposed_name(path, text)

        if not target_name:
            continue

        target = AUDITS / target_name

        if target == path:
            continue

        plans.append((path, target))

    return plans


def validate_plan(plans: list[tuple[Path, Path]]) -> None:
    targets: dict[Path, list[Path]] = defaultdict(list)

    for source, target in plans:
        targets[target].append(source)

    collisions = {target: sources for target, sources in targets.items() if len(sources) > 1}

    if collisions:
        print("ERROR: colisiones:")
        for target, sources in collisions.items():
            print(f"  {target.name}")
            for source in sources:
                print(f"    <- {source.name}")
        raise SystemExit(1)

    source_set = {source.resolve() for source, _ in plans}

    for source, target in plans:
        if target.exists() and target.resolve() not in source_set:
            raise SystemExit(f"ERROR: objetivo ya existe: {target.name}")

    for source, _target in plans:
        status = git_status(source)
        if status:
            print(f"WARNING: archivo con cambios Git: {source.name}: {status!r}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--apply",
        action="store_true",
        help="ejecuta los git mv; por defecto solo muestra el plan",
    )
    args = parser.parse_args()

    plans = build_plan()
    validate_plan(plans)

    print("=== MIGRACIÓN DE NOMBRES ===")

    if not plans:
        print("No hay archivos que migrar.")
        return 0

    for source, target in plans:
        print(f"{source.name}")
        print(f"  -> {target.name}")

    print()
    print(f"TOTAL: {len(plans)}")

    if not args.apply:
        print()
        print("DRY-RUN: no se modificó ningún archivo.")
        print("Para aplicar: uv run python scripts/migrate_audit_filenames.py --apply")
        return 0

    for source, target in plans:
        status = git_status(source)

        if status.startswith("??"):
            print(f"[mv]  {source.name} -> {target.name}")
            source.rename(target)
        else:
            print(f"[git] {source.name} -> {target.name}")
            subprocess.run(
                ["git", "mv", str(source), str(target)],
                cwd=ROOT,
                check=True,
            )

    print()
    print(f"OK: {len(plans)} archivos renombrados con git mv")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
