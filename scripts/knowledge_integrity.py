#!/usr/bin/env python3
"""scripts/knowledge_integrity.py — Knowledge Integrity & Decision Preflight (OCM).

Validador mecánico y determinista del sistema de Knowledge Base de OCM.
Implementa el contrato documentado en docs/knowledge/KNOWLEDGE_BASE_CONTRACT.md:

- Separación de capas: catálogo lógico (docs/knowledge/manifest.yaml, SSOT de
  metadatos) vs corpus físico local (/home/orangemusic/kb-local-only, artefactos).
- Identidad documental determinista: document_id = fingerprint de contenido
  (sha256). El `id` del manifest es la identidad LÓGICA (SSOT del catálogo);
  el document_id es la identidad FÍSICA del artefacto. No hay segundo SSOT.
- Integridad: baseline derivado (docs/knowledge/integrity.yaml) con
  sha256 + size por document_id catalogado.
- Preflight de decisión: verificación determinista de disponibilidad,
  resolución e integridad del conocimiento requerido por una decisión.
- FAIL-CLOSED: si el conocimiento requerido no puede verificarse, el
  resultado es BLOCKED. La ausencia de evidencia NUNCA se trata como
  evidencia negativa.
- SafeOps: clasificación de mutaciones (Git / Filesystem-repo /
  Knowledge-corpus / External-artifact) para políticas read-only.

Principio: Knowledge ≠ Evidence ≠ Analysis ≠ Recommendation ≠ Decision.
Este mecanismo SOLO verifica disponibilidad/integridad de conocimiento.
Nunca convierte evidencia, recomendación u output de bot/modelo en decisión.

Entradas (por defecto, rutas canónicas):
  --manifest    docs/knowledge/manifest.yaml
  --integrity   docs/knowledge/integrity.yaml
  --corpus      /home/orangemusic/kb-local-only

Exit codes: 0 = PASS / PASS_WITH_WARNINGS; 1 = BLOCKED (FAIL-CLOSED);
2 = error de ejecución.

Determinismo: la salida no incluye timestamps ni depende del orden de
descubrimiento. Dado el mismo catálogo + corpus + baseline, el resultado
es byte-idéntico.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError as exc:  # pragma: no cover
    raise SystemExit(f"pyyaml no disponible: {exc}") from exc

CHUNK_SIZE = 1024 * 1024
DEFAULT_CORPUS = Path("/home/orangemusic/kb-local-only")


# ---------------------------------------------------------------------------
# Document Identity
# ---------------------------------------------------------------------------


def content_sha256(path: Path) -> str:
    """Hash SHA-256 del contenido de un archivo (solo lectura, por chunks)."""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(CHUNK_SIZE), b""):
            h.update(chunk)
    return h.hexdigest()


def document_id(sha256_hex: str) -> str:
    """Identidad física determinista de un artefacto por su contenido.

    Propiedades (ver KNOWLEDGE_BASE_CONTRACT.md §document_id):
    - determinista y estable para el mismo contenido;
    - no depende de ruta física absoluta, timestamps ni orden de descubrimiento;
    - detecta sustitución/modificación (cambia si el contenido cambia);
    - NO es un SSOT competitivo: el SSOT de identidad lógica es el `id`
      del manifest.
    """
    return f"sha256:{sha256_hex}"


# ---------------------------------------------------------------------------
# Verdicts & Conditions
# ---------------------------------------------------------------------------


class Verdict(str, Enum):
    PASS = "PASS"
    PASS_WITH_WARNINGS = "PASS_WITH_WARNINGS"
    BLOCKED = "BLOCKED"


class Condition(str, Enum):
    # Errores (BLOCKED si están presentes)
    MISSING = "MISSING"
    INTEGRITY_FAILURE = "INTEGRITY_FAILURE"
    CATALOG_MISMATCH = "CATALOG_MISMATCH"
    CATALOG_INVALID = "CATALOG_INVALID"
    CORPUS_UNAVAILABLE = "CORPUS_UNAVAILABLE"
    AMBIGUOUS = "AMBIGUOUS"
    READ_ERROR = "READ_ERROR"
    # Advertencias (PASS_WITH_WARNINGS)
    UNVERIFIED = "UNVERIFIED"
    UNCATALOGUED = "UNCATALOGUED"
    DUPLICATE_PLACEMENT = "DUPLICATE_PLACEMENT"
    STATUS_NEEDS_REVIEW = "STATUS_NEEDS_REVIEW"
    # OK
    VERIFIED = "VERIFIED"
    RESOLVED = "RESOLVED"
    OK = "OK"


ERROR_CONDITIONS = frozenset(
    {
        Condition.MISSING,
        Condition.INTEGRITY_FAILURE,
        Condition.CATALOG_MISMATCH,
        Condition.CATALOG_INVALID,
        Condition.CORPUS_UNAVAILABLE,
        Condition.AMBIGUOUS,
        Condition.READ_ERROR,
    }
)
WARNING_CONDITIONS = frozenset(
    {
        Condition.UNVERIFIED,
        Condition.UNCATALOGUED,
        Condition.DUPLICATE_PLACEMENT,
        Condition.STATUS_NEEDS_REVIEW,
    }
)


@dataclass(frozen=True, order=True)
class Check:
    """Resultado atómico de una verificación."""

    condition: Condition
    subject: str
    detail: str


@dataclass(frozen=True)
class CatalogEntry:
    id: str
    title: str
    status: str
    authority: str
    declared_relpath: str


@dataclass(frozen=True)
class IntegrityEntry:
    id: str
    basename: str
    sha256: str
    size: int


@dataclass(frozen=True)
class Artifact:
    relpath: str
    sha256: str
    size: int


@dataclass
class Report:
    verdict: Verdict
    checks: list[Check]

    @property
    def errors(self) -> list[Check]:
        return [c for c in self.checks if c.condition in ERROR_CONDITIONS]

    @property
    def warnings(self) -> list[Check]:
        return [c for c in self.checks if c.condition in WARNING_CONDITIONS]

    def count_by_condition(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for c in self.checks:
            out[c.condition.value] = out.get(c.condition.value, 0) + 1
        return out

    def to_dict(self) -> dict[str, Any]:
        return {
            "verdict": self.verdict.value,
            "conditions": self.count_by_condition(),
            "checks": [asdict(c) for c in sorted(self.checks)],
        }


# ---------------------------------------------------------------------------
# Catalog loading (docs/knowledge/manifest.yaml)
# ---------------------------------------------------------------------------


class CatalogError(Exception):
    pass


def load_catalog(manifest: Path) -> list[CatalogEntry]:
    if not manifest.is_file():
        raise CatalogError(f"manifest no existe: {manifest}")
    try:
        raw: Any = yaml.safe_load(manifest.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise CatalogError(f"manifest YAML inválido: {exc}") from exc
    if not isinstance(raw, dict):
        raise CatalogError("manifest: raíz YAML no es mapping")
    resources = raw.get("resources")
    if not isinstance(resources, list):
        raise CatalogError("manifest: falta clave 'resources' (lista)")
    entries: list[CatalogEntry] = []
    for item in resources:
        if not isinstance(item, dict):
            raise CatalogError(f"manifest: entrada no es mapping: {item!r}")
        doc_id = item.get("id")
        if not isinstance(doc_id, str) or not doc_id:
            raise CatalogError(f"manifest: entrada sin 'id' válido: {item!r}")
        entries.append(
            CatalogEntry(
                id=doc_id,
                title=str(item.get("title", "")),
                status=str(item.get("status", "unknown")),
                authority=str(item.get("authority", "unknown")),
                declared_relpath=str(item.get("path", "")),
            )
        )
    return entries


def load_integrity(baseline: Path) -> dict[str, IntegrityEntry]:
    """Carga el baseline de integridad (índice derivado del catálogo)."""
    if not baseline.is_file():
        return {}
    try:
        raw: Any = yaml.safe_load(baseline.read_text(encoding="utf-8"))
    except yaml.YAMLError:
        return {}
    if not isinstance(raw, dict):
        return {}
    entries: dict[str, IntegrityEntry] = {}
    items = raw.get("entries")
    if not isinstance(items, list):
        return {}
    for item in items:
        if not isinstance(item, dict):
            continue
        doc_id = item.get("id")
        sha = item.get("sha256")
        size = item.get("size")
        basename = item.get("basename")
        if isinstance(doc_id, str) and isinstance(sha, str) and isinstance(size, int) and isinstance(basename, str):
            entries[doc_id] = IntegrityEntry(id=doc_id, basename=basename, sha256=sha, size=size)
    return entries


# ---------------------------------------------------------------------------
# Corpus discovery
# ---------------------------------------------------------------------------


def discover_corpus(corpus_root: Path) -> tuple[dict[str, list[Artifact]], list[Check]]:
    """Descubre artefactos del corpus. Devuelve índice por basename + checks.

    Determinista: el orden de descubrimiento NO afecta el resultado (los
    artefactos se ordenan por relpath en cada cola de basename).
    """
    checks: list[Check] = []
    index: dict[str, list[Artifact]] = {}
    if not corpus_root.is_dir():
        return index, checks
    for path in sorted(corpus_root.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(corpus_root).as_posix()
        try:
            sha = content_sha256(path)
            size = path.stat().st_size
        except OSError as exc:
            checks.append(
                Check(
                    Condition.READ_ERROR,
                    f"corpus:{rel}",
                    f"artefacto ilegible: {exc}",
                )
            )
            continue
        index.setdefault(path.name, []).append(Artifact(relpath=rel, sha256=sha, size=size))
    for key in index:
        index[key].sort(key=lambda a: a.relpath)
    return index, checks


# ---------------------------------------------------------------------------
# Catalog <-> Corpus resolution + integrity
# ---------------------------------------------------------------------------


def _verify_entry(
    entry: CatalogEntry,
    index: dict[str, list[Artifact]],
    baseline: dict[str, IntegrityEntry],
) -> tuple[list[Check], str | None]:
    """Verifica una entrada del catálogo. Devuelve (checks, basename consumida)."""
    checks: list[Check] = []
    basename = Path(entry.declared_relpath).name if entry.declared_relpath else entry.id
    candidates = index.get(basename, [])
    subject = f"catalog:{entry.id}"
    if not candidates:
        checks.append(
            Check(
                Condition.MISSING,
                subject,
                f"artefacto no encontrado en el corpus (basename esperado: {basename})",
            )
        )
        return checks, None
    if len(candidates) > 1:
        distinct = {(a.sha256, a.size) for a in candidates}
        if len(distinct) > 1:
            checks.append(
                Check(
                    Condition.AMBIGUOUS,
                    subject,
                    f"{basename} resuelve a {len(candidates)} artefactos con contenido distinto: identidad ambigua",
                )
            )
            return checks, None
        checks.append(
            Check(
                Condition.DUPLICATE_PLACEMENT,
                subject,
                f"{basename} presente en {len(candidates)} rutas con el mismo "
                f"contenido: {', '.join(a.relpath for a in candidates)}",
            )
        )
    artifact = candidates[0]
    checks.append(
        Check(
            Condition.RESOLVED,
            subject,
            f"resuelto a corpus:{artifact.relpath} (document_id={document_id(artifact.sha256)})",
        )
    )
    baseline_entry = baseline.get(entry.id)
    if baseline_entry is None:
        checks.append(
            Check(
                Condition.UNVERIFIED,
                subject,
                "sin entrada en el baseline de integridad; integridad no demostrable "
                f"(document_id={document_id(artifact.sha256)})",
            )
        )
    elif baseline_entry.sha256 != artifact.sha256 or baseline_entry.size != artifact.size:
        checks.append(
            Check(
                Condition.INTEGRITY_FAILURE,
                subject,
                "el contenido del artefacto no coincide con el baseline "
                f"(esperado sha256={baseline_entry.sha256[:12]}… size={baseline_entry.size}, "
                f"observado sha256={artifact.sha256[:12]}… size={artifact.size})",
            )
        )
    else:
        checks.append(
            Check(
                Condition.VERIFIED,
                subject,
                f"integridad verificada (document_id={document_id(artifact.sha256)})",
            )
        )
    if entry.status != "active":
        checks.append(
            Check(
                Condition.STATUS_NEEDS_REVIEW,
                subject,
                f"status de catálogo no activo: {entry.status} (metadata no verificada no debe citarse como hecho)",
            )
        )
    return checks, basename


def verify(
    catalog: list[CatalogEntry],
    index: dict[str, list[Artifact]],
    baseline: dict[str, IntegrityEntry],
) -> list[Check]:
    checks: list[Check] = []
    consumed_basenames: set[str] = set()

    ids = [e.id for e in catalog]
    dup_ids = sorted({i for i in ids if ids.count(i) > 1})
    for dup in dup_ids:
        checks.append(
            Check(
                Condition.CATALOG_MISMATCH,
                f"catalog:{dup}",
                "identidad documental duplicada en el catálogo",
            )
        )

    for entry in sorted(catalog, key=lambda e: e.id):
        entry_checks, consumed = _verify_entry(entry, index, baseline)
        checks.extend(entry_checks)
        if consumed is not None:
            consumed_basenames.add(consumed)

    for basename, artifacts in sorted(index.items()):
        if basename in consumed_basenames:
            continue
        for art in artifacts:
            checks.append(
                Check(
                    Condition.UNCATALOGUED,
                    f"corpus:{art.relpath}",
                    "artefacto no catalogado (REVIEW_REQUIRED); no se elimina ni se "
                    f"ignora silenciosamente (document_id={document_id(art.sha256)})",
                )
            )
    return checks


def build_report(checks: list[Check]) -> Report:
    if any(c.condition in ERROR_CONDITIONS for c in checks):
        verdict = Verdict.BLOCKED
    elif any(c.condition in WARNING_CONDITIONS for c in checks):
        verdict = Verdict.PASS_WITH_WARNINGS
    else:
        verdict = Verdict.PASS
    return Report(verdict=verdict, checks=checks)


# ---------------------------------------------------------------------------
# Decision Preflight
# ---------------------------------------------------------------------------


def preflight(
    report: Report,
    catalog: list[CatalogEntry],
    required: list[str],
) -> Report:
    """Decision Preflight: verifica que el conocimiento REQUERIDO por una
    decisión está disponible, resuelto e íntegro. FAIL-CLOSED.

    Un documento requerido que:
    - no existe (MISSING),
    - no puede localizarse de forma inequívoca (AMBIGUOUS),
    - no puede verificarse (INTEGRITY_FAILURE / UNVERIFIED),
    - no está catalogado (id desconocido),
    produce BLOCKED. No se continúa como si el conocimiento estuviera
    disponible. La ausencia de evidencia NO es evidencia negativa.
    """
    checks = list(report.checks)
    catalog_ids = {e.id for e in catalog}
    subjects = {c.subject for c in checks}

    for req in sorted(set(required)):
        if req not in catalog_ids:
            checks.append(
                Check(
                    Condition.CATALOG_MISMATCH,
                    f"preflight:{req}",
                    "documento requerido no existe en el catálogo",
                )
            )
            continue
        subject = f"catalog:{req}"
        if subject not in subjects:
            checks.append(
                Check(
                    Condition.CATALOG_MISMATCH,
                    f"preflight:{req}",
                    "documento requerido sin resultado de verificación",
                )
            )
            continue
        sub_checks = [c for c in checks if c.subject == subject]
        bad = {c.condition for c in sub_checks} & (ERROR_CONDITIONS | {Condition.UNVERIFIED})
        if bad:
            checks.append(
                Check(
                    Condition.MISSING,
                    f"preflight:{req}",
                    "documento requerido no usable: FAIL-CLOSED "
                    f"(condiciones: {', '.join(sorted(b.value for b in bad))})",
                )
            )
        else:
            checks.append(
                Check(
                    Condition.OK,
                    f"preflight:{req}",
                    "documento requerido disponible, resuelto e íntegro",
                )
            )
    return build_report(checks)


# ---------------------------------------------------------------------------
# SafeOps — clasificación de mutaciones
# ---------------------------------------------------------------------------


class MutationKind(str, Enum):
    NONE = "NONE"
    GIT = "GIT_MUTATION"
    FILESYSTEM_REPO = "FILESYSTEM_REPOSITORY_MUTATION"
    KNOWLEDGE_CORPUS = "KNOWLEDGE_CORPUS_MUTATION"
    EXTERNAL_ARTIFACT = "EXTERNAL_ARTIFACT_MUTATION"


GIT_MUTATION_OPS = frozenset(
    {
        "commit",
        "push",
        "merge",
        "rebase",
        "reset",
        "clean",
        "stash",
        "add",
        "tag",
        "branch-delete",
    }
)
FS_MUTATION_OPS = frozenset({"create", "modify", "delete", "rename", "move", "copy", "write", "chmod"})


def is_git_mutation(operation: str) -> bool:
    """Clasifica si una operación es mutación de Git (SafeOps §Git Mutation)."""
    return operation.strip().lower() in GIT_MUTATION_OPS


def is_fs_mutation(operation: str) -> bool:
    """Clasifica si una operación es mutación del filesystem."""
    return operation.strip().lower() in FS_MUTATION_OPS


def classify_target(
    target: Path | None,
    repo_root: Path,
    corpus_root: Path,
) -> MutationKind:
    """Clasifica el objetivo de una operación según su ubicación."""
    if target is None:
        return MutationKind.NONE
    try:
        resolved = target.resolve()
    except OSError:
        return MutationKind.EXTERNAL_ARTIFACT
    corpus_resolved = corpus_root.resolve()
    repo_resolved = repo_root.resolve()
    try:
        if resolved.is_relative_to(corpus_resolved):
            return MutationKind.KNOWLEDGE_CORPUS
    except ValueError:
        pass
    try:
        if resolved.is_relative_to(repo_resolved):
            return MutationKind.FILESYSTEM_REPO
    except ValueError:
        pass
    return MutationKind.EXTERNAL_ARTIFACT


def classify_mutation(
    operation: str,
    target: Path | None,
    repo_root: Path,
    corpus_root: Path,
) -> MutationKind:
    """Clasificación completa SafeOps: operación + objetivo.

    Precedencia: Git > Knowledge-corpus > Filesystem-repo > External-artifact.
    Git Read-Only ≠ Filesystem Read-Only: son categorías independientes.
    """
    if is_git_mutation(operation):
        return MutationKind.GIT
    if not is_fs_mutation(operation):
        return MutationKind.NONE
    return classify_target(target, repo_root, corpus_root)


def read_only_violation(kind: MutationKind) -> str | None:
    """SAFEOPS STRICT READ-ONLY: cualquier mutación es violación."""
    if kind is MutationKind.NONE:
        return None
    return f"SAFEOPS READ-ONLY VIOLATION: {kind.value} no permitida salvo autorización explícita posterior"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _resolve_repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def run_check(
    manifest: Path,
    integrity: Path,
    corpus: Path,
    required: list[str],
) -> Report:
    checks: list[Check] = []
    catalog: list[CatalogEntry] = []
    if not corpus.is_dir():
        return Report(
            verdict=Verdict.BLOCKED,
            checks=[
                Check(
                    Condition.CORPUS_UNAVAILABLE,
                    f"corpus:{corpus}",
                    "corpus no disponible; FAIL-CLOSED (KNOWLEDGE UNAVAILABLE, no equivale a KNOWLEDGE ABSENT)",
                )
            ],
        )
    try:
        catalog = load_catalog(manifest)
    except CatalogError as exc:
        return Report(
            verdict=Verdict.BLOCKED,
            checks=[Check(Condition.CATALOG_INVALID, f"manifest:{manifest}", str(exc))],
        )
    index, discovery_checks = discover_corpus(corpus)
    checks.extend(discovery_checks)
    baseline = load_integrity(integrity)
    checks.extend(verify(catalog, index, baseline))
    report = build_report(checks)
    if required:
        return preflight(report, catalog, required)
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--manifest", type=Path, default=None)
    parser.add_argument("--integrity", type=Path, default=None)
    parser.add_argument("--corpus", type=Path, default=DEFAULT_CORPUS)
    parser.add_argument(
        "--require",
        default="",
        help="comma-separated catalog ids requeridos por una decisión (preflight)",
    )
    parser.add_argument("--json", action="store_true", help="salida JSON determinista")
    args = parser.parse_args(argv)

    repo_root = _resolve_repo_root()
    manifest = args.manifest or (repo_root / "docs" / "knowledge" / "manifest.yaml")
    integrity = args.integrity or (repo_root / "docs" / "knowledge" / "integrity.yaml")
    required = [x for x in args.require.split(",") if x]

    try:
        report = run_check(manifest, integrity, args.corpus, required)
    except OSError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
    else:
        print(f"VERDICT: {report.verdict.value}")
        for c in sorted(report.checks):
            if c.condition in ERROR_CONDITIONS:
                prefix = "ERR "
            elif c.condition in WARNING_CONDITIONS:
                prefix = "WARN"
            else:
                prefix = "OK  "
            print(f"{prefix} [{c.condition.value:18}] {c.subject} — {c.detail}")
    return 0 if report.verdict is not Verdict.BLOCKED else 1


if __name__ == "__main__":
    sys.exit(main())
