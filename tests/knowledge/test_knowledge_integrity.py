"""tests/knowledge/test_knowledge_integrity.py — Knowledge Integrity & Decision Preflight.

Cubre (per Gate Controlled Implementation, 2026-10-03):
- Document identity: estabilidad, path-independencia, detección de cambio.
- Catalog: entry presente, missing, unexpected, duplicate identity, hash mismatch.
- Corpus: disponible, inexistente, vacío, permisos insuficientes, ilegible.
- Preflight: PASS, WARNING, MISSING, INTEGRITY_FAILURE, CATALOG_MISMATCH,
  CORPUS_UNAVAILABLE, AMBIGUOUS, FAIL-CLOSED.
- SafeOps: distinción Git / Filesystem-repo / Knowledge-corpus / External.

Mecanismo: scripts/knowledge_integrity.py (importado por path, stdlib + pyyaml).
"""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "knowledge_integrity.py"
DEFAULT_CORPUS = Path("/home/orangemusic/kb-local-only")


def _load_ki():
    import sys as _sys

    spec = importlib.util.spec_from_file_location("knowledge_integrity_under_test", SCRIPT)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    _sys.modules["knowledge_integrity_under_test"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def ki():
    return _load_ki()


def _write_manifest(path: Path, entries: list[dict]) -> None:
    lines = ["resources:"]
    for e in entries:
        lines.append(f"  - id: {e['id']}")
        lines.append(f"    title: {e.get('title', 'T')}")
        lines.append(f"    path: {e['path']}")
        lines.append(f"    status: {e.get('status', 'active')}")
        lines.append(f"    authority: {e.get('authority', 'TIER_2')}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_integrity(path: Path, entries: list[dict]) -> None:
    lines = ["baseline_version: 1", "entries:"]
    for e in entries:
        lines.append(f"  - id: {e['id']}")
        lines.append(f"    basename: {e['basename']}")
        lines.append(f'    sha256: "{e["sha256"]}"')
        lines.append(f"    size: {e['size']}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


@pytest.fixture()
def env(ki, tmp_path: Path) -> dict:
    """Corpus + catálogo + baseline coherentes (PASS)."""
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    pdf_a = corpus / "a.pdf"
    pdf_a.write_bytes(b"CONTENT-A")
    pdf_b = corpus / "b.pdf"
    pdf_b.write_bytes(b"CONTENT-B")

    manifest = tmp_path / "manifest.yaml"
    _write_manifest(
        manifest,
        [
            {"id": "doc-a", "path": "books/a.pdf"},
            {"id": "doc-b", "path": "papers/b.pdf"},
        ],
    )
    integrity = tmp_path / "integrity.yaml"
    _write_integrity(
        integrity,
        [
            {"id": "doc-a", "basename": "a.pdf", "sha256": ki.content_sha256(pdf_a), "size": pdf_a.stat().st_size},
            {"id": "doc-b", "basename": "b.pdf", "sha256": ki.content_sha256(pdf_b), "size": pdf_b.stat().st_size},
        ],
    )
    return {
        "ki": ki,
        "corpus": corpus,
        "manifest": manifest,
        "integrity": integrity,
    }


# --- Document Identity -------------------------------------------------------


def test_same_content_same_id_in_different_paths(ki, tmp_path: Path) -> None:
    d1 = tmp_path / "x"
    d2 = tmp_path / "y"
    d1.mkdir()
    d2.mkdir()
    f1 = d1 / "doc.pdf"
    f2 = d2 / "doc.pdf"
    f1.write_bytes(b"identical")
    f2.write_bytes(b"identical")
    assert ki.document_id(ki.content_sha256(f1)) == ki.document_id(ki.content_sha256(f2))
    assert str(f1) != str(f2)  # rutas absolutas distintas


def test_modified_content_changes_identity(ki, tmp_path: Path) -> None:
    f = tmp_path / "doc.pdf"
    f.write_bytes(b"v1")
    first = ki.document_id(ki.content_sha256(f))
    f.write_bytes(b"v2")
    second = ki.document_id(ki.content_sha256(f))
    assert first != second


def test_different_documents_different_ids(ki, tmp_path: Path) -> None:
    f1 = tmp_path / "1.pdf"
    f2 = tmp_path / "2.pdf"
    f1.write_bytes(b"one")
    f2.write_bytes(b"two")
    assert ki.document_id(ki.content_sha256(f1)) != ki.document_id(ki.content_sha256(f2))


# --- Catalog / Integrity -----------------------------------------------------


def test_catalog_entry_resolved_and_verified(env) -> None:
    mod = env["ki"]
    report = mod.run_check(env["manifest"], env["integrity"], env["corpus"], [])
    assert report.verdict is mod.Verdict.PASS
    assert any(c.condition is mod.Condition.VERIFIED and c.subject == "catalog:doc-a" for c in report.checks)


def test_missing_document_blocks(env) -> None:
    mod = env["ki"]
    (env["corpus"] / "b.pdf").unlink()
    report = mod.run_check(env["manifest"], env["integrity"], env["corpus"], [])
    assert report.verdict is mod.Verdict.BLOCKED
    assert any(c.condition is mod.Condition.MISSING and c.subject == "catalog:doc-b" for c in report.checks)


def test_unexpected_document_warns_not_ignored(env) -> None:
    mod = env["ki"]
    extra = env["corpus"] / "rogue.pdf"
    extra.write_bytes(b"unknown")
    report = mod.run_check(env["manifest"], env["integrity"], env["corpus"], [])
    assert report.verdict is mod.Verdict.PASS_WITH_WARNINGS
    assert any(c.condition is mod.Condition.UNCATALOGUED for c in report.checks)


def test_duplicate_identity_blocks(env) -> None:
    mod = env["ki"]
    _write_manifest(
        env["manifest"],
        [
            {"id": "doc-a", "path": "books/a.pdf"},
            {"id": "doc-a", "path": "books/a.pdf"},
        ],
    )
    report = mod.run_check(env["manifest"], env["integrity"], env["corpus"], [])
    assert report.verdict is mod.Verdict.BLOCKED
    assert any(c.condition is mod.Condition.CATALOG_MISMATCH for c in report.checks)


def test_hash_mismatch_blocks(env) -> None:
    mod = env["ki"]
    (env["corpus"] / "a.pdf").write_bytes(b"ALTERED")
    report = mod.run_check(env["manifest"], env["integrity"], env["corpus"], [])
    assert report.verdict is mod.Verdict.BLOCKED
    assert any(c.condition is mod.Condition.INTEGRITY_FAILURE and c.subject == "catalog:doc-a" for c in report.checks)


def test_ambiguous_basename_blocks(env) -> None:
    mod = env["ki"]
    dup = env["corpus"] / "nested"
    dup.mkdir()
    (dup / "a.pdf").write_bytes(b"OTHER-CONTENT")
    report = mod.run_check(env["manifest"], env["integrity"], env["corpus"], [])
    assert report.verdict is mod.Verdict.BLOCKED
    assert any(c.condition is mod.Condition.AMBIGUOUS and c.subject == "catalog:doc-a" for c in report.checks)


def test_same_content_duplicate_placement_warns(env) -> None:
    mod = env["ki"]
    dup = env["corpus"] / "mirror"
    dup.mkdir()
    (dup / "b.pdf").write_bytes(b"CONTENT-B")
    report = mod.run_check(env["manifest"], env["integrity"], env["corpus"], [])
    assert any(c.condition is mod.Condition.DUPLICATE_PLACEMENT for c in report.checks)
    assert report.verdict is not mod.Verdict.BLOCKED


# --- Corpus ------------------------------------------------------------------


def test_corpus_unavailable_blocks(ki, tmp_path: Path) -> None:
    report = ki.run_check(tmp_path / "manifest.yaml", tmp_path / "integrity.yaml", tmp_path / "nope", [])
    assert report.verdict is ki.Verdict.BLOCKED
    assert any(c.condition is ki.Condition.CORPUS_UNAVAILABLE for c in report.checks)


def test_empty_corpus_blocks(env) -> None:
    mod = env["ki"]
    empty = env["corpus"] / "empty"
    empty.mkdir()
    report = mod.run_check(env["manifest"], env["integrity"], empty, [])
    assert report.verdict is mod.Verdict.BLOCKED
    assert sum(1 for c in report.checks if c.condition is mod.Condition.MISSING) == 2


def test_catalog_invalid_yaml_blocks(ki, tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.yaml"
    manifest.write_text("resources: [unclosed", encoding="utf-8")
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    report = ki.run_check(manifest, tmp_path / "integrity.yaml", corpus, [])
    assert report.verdict is ki.Verdict.BLOCKED
    assert any(c.condition is ki.Condition.CATALOG_INVALID for c in report.checks)


@pytest.mark.skipif(os.geteuid() == 0, reason="root ignora permisos; sin efecto")
def test_unreadable_document_blocks(env) -> None:
    mod = env["ki"]
    target = env["corpus"] / "a.pdf"
    target.chmod(0o000)
    try:
        report = mod.run_check(env["manifest"], env["integrity"], env["corpus"], [])
        assert report.verdict is mod.Verdict.BLOCKED
        assert any(c.condition is mod.Condition.READ_ERROR for c in report.checks)
    finally:
        target.chmod(0o644)


def test_unverified_document_is_warning_not_block(env) -> None:
    mod = env["ki"]
    integrity = env["integrity"]
    integrity.write_text("baseline_version: 1\nentries: []\n", encoding="utf-8")
    report = mod.run_check(env["manifest"], integrity, env["corpus"], [])
    assert report.verdict is mod.Verdict.PASS_WITH_WARNINGS
    assert any(c.condition is mod.Condition.UNVERIFIED for c in report.checks)


# --- Preflight ---------------------------------------------------------------


def test_preflight_pass(env) -> None:
    mod = env["ki"]
    report = mod.run_check(env["manifest"], env["integrity"], env["corpus"], ["doc-a", "doc-b"])
    assert report.verdict is mod.Verdict.PASS
    assert all(c.condition is mod.Condition.OK for c in report.checks if c.subject.startswith("preflight:"))


def test_preflight_fail_closed_on_missing(env) -> None:
    mod = env["ki"]
    (env["corpus"] / "b.pdf").unlink()
    report = mod.run_check(env["manifest"], env["integrity"], env["corpus"], ["doc-b"])
    assert report.verdict is mod.Verdict.BLOCKED
    assert any(c.subject == "preflight:doc-b" for c in report.checks if c.condition in mod.ERROR_CONDITIONS)


def test_preflight_fail_closed_on_unverified(env) -> None:
    mod = env["ki"]
    integrity = env["integrity"]
    integrity.write_text("baseline_version: 1\nentries: []\n", encoding="utf-8")
    report = mod.run_check(env["manifest"], integrity, env["corpus"], ["doc-a"])
    assert report.verdict is mod.Verdict.BLOCKED


def test_preflight_unknown_required_id_blocks(env) -> None:
    mod = env["ki"]
    report = mod.run_check(env["manifest"], env["integrity"], env["corpus"], ["ghost"])
    assert report.verdict is mod.Verdict.BLOCKED
    assert any(c.subject == "preflight:ghost" for c in report.checks)


# --- SafeOps -----------------------------------------------------------------


def test_safeops_git_mutation_ops(ki) -> None:
    assert all(ki.is_git_mutation(op) for op in ("commit", "push", "merge", "rebase", "reset", "clean", "stash", "add"))
    assert not ki.is_git_mutation("read")
    assert not ki.is_git_mutation("status")


def test_safeops_classifies_corpus_vs_repo_vs_external(ki, tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    corpus = tmp_path / "corpus"
    repo.mkdir()
    corpus.mkdir()
    inside_repo = repo / "file.py"
    inside_repo.touch()
    inside_corpus = corpus / "doc.pdf"
    inside_corpus.touch()
    external = tmp_path / "elsewhere"
    external.mkdir()
    ext_file = external / "x.bin"
    ext_file.touch()

    assert ki.classify_mutation("modify", inside_corpus, repo, corpus) is ki.MutationKind.KNOWLEDGE_CORPUS
    assert ki.classify_mutation("modify", inside_repo, repo, corpus) is ki.MutationKind.FILESYSTEM_REPO
    assert ki.classify_mutation("copy", ext_file, repo, corpus) is ki.MutationKind.EXTERNAL_ARTIFACT
    assert ki.classify_mutation("read", ext_file, repo, corpus) is ki.MutationKind.NONE


def test_safeops_git_mutation_wins(ki, tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    corpus = tmp_path / "corpus"
    repo.mkdir()
    corpus.mkdir()
    f = repo / "a.py"
    f.touch()
    assert ki.classify_mutation("commit", f, repo, corpus) is ki.MutationKind.GIT
    assert ki.classify_mutation("stash", None, repo, corpus) is ki.MutationKind.GIT


def test_safeops_read_only_policy_violations(ki) -> None:
    for kind in (
        ki.MutationKind.GIT,
        ki.MutationKind.FILESYSTEM_REPO,
        ki.MutationKind.KNOWLEDGE_CORPUS,
        ki.MutationKind.EXTERNAL_ARTIFACT,
    ):
        assert ki.read_only_violation(kind) is not None
    assert ki.read_only_violation(ki.MutationKind.NONE) is None


# --- Real repository (skip si corpus ausente) --------------------------------


@pytest.mark.skipif(not DEFAULT_CORPUS.is_dir(), reason="corpus local ausente en esta máquina")
def test_real_corpus_passes_with_baseline(ki) -> None:
    repo = ROOT
    manifest = repo / "docs" / "knowledge" / "manifest.yaml"
    integrity = repo / "docs" / "knowledge" / "integrity.yaml"
    report = ki.run_check(manifest, integrity, DEFAULT_CORPUS, [])
    assert report.verdict in (ki.Verdict.PASS, ki.Verdict.PASS_WITH_WARNINGS)
    verified = [c for c in report.checks if c.condition is ki.Condition.VERIFIED]
    assert len(verified) == 30
    assert not any(c.condition in ki.ERROR_CONDITIONS for c in report.checks)
