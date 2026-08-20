"""
tests/architecture/test_verify_policy_integrity.py — Verificación de integridad (ADR-0032).

Demuestra que el mecanismo de integridad de archivos protegidos es determinista:

  - manifiesto inexistente → FAIL
  - manifiesto válido → PASS
  - manipulación de archivo protegido → HASH MISMATCH (FAIL)
  - path no permitido en manifiesto → FAIL
  - --update regenera y luego verifica → PASS
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
SCRIPT = ROOT / "scripts" / "verify_policy_integrity.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("verify_policy_integrity", SCRIPT)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


vpi = _load_module()


def test_missing_manifest_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(vpi, "MANIFEST_PATH", tmp_path / "evidence.json")
    errors = vpi.verify(None)
    assert errors
    assert "manifest inexistente" in errors[0]


def test_valid_manifest_passes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(vpi, "MANIFEST_PATH", tmp_path / "evidence.json")
    hashes = vpi.current_hashes()
    vpi.write_manifest(hashes)
    errors = vpi.verify(vpi.load_manifest())
    assert not errors


def test_tamper_detected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(vpi, "MANIFEST_PATH", tmp_path / "evidence.json")
    vpi.write_manifest(vpi.current_hashes())
    target = ROOT / "scripts" / "audit_validator.py"
    original = target.read_bytes()
    try:
        target.write_bytes(original + b"\n# tamper test\n")
        errors = vpi.verify(vpi.load_manifest())
        assert any("HASH MISMATCH" in e for e in errors)
    finally:
        target.write_bytes(original)


def test_unknown_path_in_manifest_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(vpi, "MANIFEST_PATH", tmp_path / "evidence.json")
    manifest = {
        "schema": vpi.MANIFEST_SCHEMA,
        "files": {"scripts/audit_validator.py": "x", "secret/thing.py": "y"},
    }
    errors = vpi.verify(manifest)
    assert any("paths no permitidos" in e for e in errors)


def test_update_then_verify_passes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(vpi, "MANIFEST_PATH", tmp_path / "evidence.json")
    vpi.write_manifest(vpi.current_hashes())
    assert not vpi.verify(vpi.load_manifest())


def test_manifest_schema_version_guarded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(vpi, "MANIFEST_PATH", tmp_path / "evidence.json")
    manifest = {"schema": 999, "files": {}}
    errors = vpi.verify(manifest)
    assert any("schema del manifest" in e for e in errors)


def test_protected_files_all_exist() -> None:
    for rel in vpi.PROTECTED_FILES:
        assert (ROOT / rel).is_file(), f"archivo protegido ausente: {rel}"