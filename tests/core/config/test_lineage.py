from __future__ import annotations

"""Tests para core/config/lineage.py."""

from unittest.mock import MagicMock, patch

import pytest

from core.config.lineage import LineageRecord, build_lineage, get_git_hash


# ---------------------------------------------------------------------------
# get_git_hash — nunca lanza, siempre retorna string
# ---------------------------------------------------------------------------

def test_get_git_hash_returns_string():
    result = get_git_hash()
    assert isinstance(result, str)
    assert len(result) > 0


def test_get_git_hash_nonzero_returncode():
    """returncode != 0 debe retornar 'unknown' aunque stdout tenga contenido."""
    mock = MagicMock()
    mock.returncode = 128
    mock.stdout = "abc1234\n"
    with patch("subprocess.run", return_value=mock):
        assert get_git_hash() == "unknown"


def test_get_git_hash_zero_returncode_empty_stdout():
    """returncode 0 pero stdout vacío → 'unknown'."""
    mock = MagicMock()
    mock.returncode = 0
    mock.stdout = ""
    with patch("subprocess.run", return_value=mock):
        assert get_git_hash() == "unknown"


def test_get_git_hash_subprocess_exception():
    """Cualquier excepción → 'unknown', nunca relanza."""
    with patch("subprocess.run", side_effect=FileNotFoundError("git not found")):
        assert get_git_hash() == "unknown"


# ---------------------------------------------------------------------------
# LineageRecord — inmutabilidad y defaults
# ---------------------------------------------------------------------------

def test_lineage_record_immutable():
    rec = build_lineage(run_id="r1", version_id="v1")
    with pytest.raises((AttributeError, TypeError)):
        rec.run_id = "x"  # type: ignore[misc]


def test_lineage_record_defaults():
    rec = build_lineage(run_id="r1", version_id="v42")
    assert rec.layer == "silver"
    assert rec.as_of is None
    assert rec.exchange is None
    assert isinstance(rec.written_at, str)
    assert "T" in rec.written_at  # ISO 8601


# ---------------------------------------------------------------------------
# to_manifest — serialización
# ---------------------------------------------------------------------------

def test_to_manifest_required_fields():
    rec = build_lineage(run_id="r1", version_id="v42")
    m = rec.to_manifest()
    assert m["run_id"]     == "r1"
    assert m["version_id"] == "v42"
    assert m["layer"]      == "silver"
    assert "git_hash"   in m
    assert "written_at" in m


def test_to_manifest_optional_fields_present():
    rec = build_lineage(
        run_id="r1", version_id="v1",
        as_of="2026-01-01T00:00:00+00:00",
        exchange="binance",
    )
    m = rec.to_manifest()
    assert m["as_of"]    == "2026-01-01T00:00:00+00:00"
    assert m["exchange"] == "binance"


def test_to_manifest_optional_fields_absent_when_none():
    rec = build_lineage(run_id="r1", version_id="v1")
    m = rec.to_manifest()
    assert "as_of"    not in m
    assert "exchange" not in m


def test_build_lineage_layer_override():
    rec = build_lineage(run_id="r1", version_id="v1", layer="gold")
    assert rec.layer == "gold"
    assert rec.to_manifest()["layer"] == "gold"
