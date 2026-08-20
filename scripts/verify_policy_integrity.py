#!/usr/bin/env python3
"""scripts/verify_policy_integrity.py — Verificación de integridad de archivos protegidos (ADR-0032).

Frontera de confianza (ADVERTENCIA EXPLÍCITA):
  Los hashes viven en `policies/evidence.json`, dentro del mismo repo y con los
  mismos privilegios de escritura que los objetos protegidos. ESO NO ES una
  frontera de confianza suficiente por sí solo: un agente con escritura a main
  puede modificar guard + manifest + CI a la vez.

  El propósito REAL de este mecanismo es:
    1. DETECCIÓN: el job CI `policy-gate` calcula los hashes en el runner de
       GitHub (entorno no modificable por el push del agente en el mismo run)
       y falla si el código protegido no coincide con el manifest.
    2. SEÑAL PARA REVISIÓN HUMANA: un PR que modifica simultáneamente un guard
       y `policies/evidence.json` es señal clara de captura del sistema de
       validación para el reviewer (branch protection + CODEOWNERS).

  La frontera de confianza REAL es la configuración de GitHub (branch
  protection + required reviews + CODEOWNERS), que NO es versionable y NO
  puede ser modificada por el agente desde el repo.

Uso:
  python scripts/verify_policy_integrity.py            # verifica (exit 0/1)
  python scripts/verify_policy_integrity.py --update   # regenera el manifest (SOLO humano, NUNCA CI)

Exit codes: 0 = integridad OK; 1 = mismatch (FAIL); 2 = error de ejecución.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Archivos protegidos por ADR-0032 (ruta relativa al ROOT).
# Los guards se enumeran explícitamente (no glob) para que la lista sea
# estable y auditable: añadir un guard nuevo requiere update deliberado.
PROTECTED_FILES = [
    "scripts/app_layer_guard.py",
    "scripts/domain_subprocess_guard.py",
    "scripts/audit_validator.py",
    "scripts/engineering_health_check.py",
    "scripts/verify_policy_integrity.py",
    "policies/registry.yaml",
    ".github/workflows/ocm-ci.yml",
    ".github/CODEOWNERS",
]

MANIFEST_PATH = ROOT / "policies" / "evidence.json"
MANIFEST_SCHEMA = 1

# Enumeración de archivos permitidos en el manifest (protección contra
# adición de rutas arbitrarias al manifest por el agente).
ALLOWED_PATHS = frozenset(PROTECTED_FILES)


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def current_hashes() -> dict[str, str]:
    out: dict[str, str] = {}
    for rel in PROTECTED_FILES:
        path = ROOT / rel
        if not path.is_file():
            raise FileNotFoundError(f"archivo protegido no existe: {rel}")
        out[rel] = sha256_of(path)
    return out


def load_manifest() -> dict | None:
    if not MANIFEST_PATH.exists():
        return None
    try:
        data = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


def write_manifest(hashes: dict[str, str]) -> None:
    manifest = {
        "schema": MANIFEST_SCHEMA,
        "generated_by": "verify_policy_integrity.py --update",
        "note": (
            "Registro de confianza de ARCHIVOS PROTEGIDOS (ADR-0032). "
            "NO es frontera de confianza por sí solo; el enforcement real es "
            "branch protection + CODEOWNERS en GitHub. Verificar solo en CI "
            "policy-gate; --update solo por humano local."
        ),
        "files": hashes,
    }
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def verify(manifest: dict | None) -> list[str]:
    """Devuelve lista de errores; vacía = integridad OK."""
    errors: list[str] = []
    if manifest is None:
        errors.append(f"manifest inexistente o inválido: {MANIFEST_PATH}")
        return errors
    if manifest.get("schema") != MANIFEST_SCHEMA:
        errors.append(f"schema del manifest inesperado: {manifest.get('schema')!r}")
    recorded = manifest.get("files")
    if not isinstance(recorded, dict):
        errors.append("manifest.files no es un mapping")
        return errors
    unknown = set(recorded) - ALLOWED_PATHS
    if unknown:
        errors.append(f"paths no permitidos en el manifest: {sorted(unknown)}")
    try:
        actual = current_hashes()
    except FileNotFoundError as exc:
        errors.append(str(exc))
        return errors
    for rel in sorted(ALLOWED_PATHS):
        expected = recorded.get(rel)
        got = actual.get(rel)
        if expected is None:
            errors.append(f"{rel}: falta hash en el manifest")
        elif expected != got:
            errors.append(f"{rel}: HASH MISMATCH (esperado {expected}, actual {got})")
    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verificación de integridad de archivos protegidos (ADR-0032)")
    parser.add_argument(
        "--update",
        action="store_true",
        help="regenera policies/evidence.json con los hashes actuales (SOLO humano local, NUNCA CI)",
    )
    parser.add_argument(
        "--print-manifest",
        action="store_true",
        help="imprime el manifest actual y sale (sin verificar)",
    )
    args = parser.parse_args(argv)

    if args.print_manifest:
        manifest = load_manifest()
        print(json.dumps(manifest, indent=2) if manifest else "<sin manifest>")
        return 0

    if args.update:
        try:
            hashes = current_hashes()
        except FileNotFoundError as exc:
            print(f"[integrity] error: {exc}", file=sys.stderr)
            return 2
        write_manifest(hashes)
        print(f"[integrity] manifest actualizado: {MANIFEST_PATH}")
        return 0

    manifest = load_manifest()
    errors = verify(manifest)
    if not errors:
        print(f"[integrity] PASS — {len(ALLOWED_PATHS)} archivos protegidos íntegros")
        return 0
    for err in errors:
        print(f"FAIL  {err}")
    print("[integrity] FAIL — integridad comprometida o manifest desactualizado")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
