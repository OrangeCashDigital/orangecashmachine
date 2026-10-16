"""P2 tests: engineering-context store + ocm-context-bootstrap skill.

Behaviors introduced by P2 (Persistent Engineering Context):
classification, authority, provenance, historical-state handling,
fail-closed markers, bootstrap retrieval targets, Context Pack sections.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
STORE = REPO / "docs" / "engineering-context"
SKILL = REPO / ".agents" / "skills" / "ocm-context-bootstrap" / "SKILL.md"

ALLOWED_TYPES = {
    "FACT",
    "DECISION",
    "REQUIREMENT",
    "INVARIANT",
    "CONSTRAINT",
    "EVIDENCE",
    "PROVENANCE",
    "RECOMMENDATION",
    "INFERENCE",
    "OPEN_QUESTION",
    "REJECTED_OPTION",
    "HISTORICAL_STATE",
}
ALLOWED_STATUS = {"CURRENT", "HISTORICAL", "PROPOSED", "REJECTED", "UNKNOWN"}
ALLOWED_ORIGIN = {"auto-derived", "agent-written", "human-approved", "historical"}
REQUIRED_FIELDS = {"source", "authority", "type", "status", "origin", "created", "review_date"}

PACK_SECTIONS = [
    "Task",
    "Domain",
    "Authoritative Sources",
    "Relevant ADRs",
    "Current Decisions",
    "Requirements",
    "Invariants",
    "Constraints",
    "Relevant Code",
    "Relevant Tests",
    "Research",
    "Rejected Approaches",
    "Known Failures",
    "Open Questions",
    "Provenance",
    "Authority Classification",
    "Risks",
]

SEED_FILES = [
    "README.md",
    "project/identity.md",
    "architecture/context.md",
    "decisions/d19-d20-identity-decisions.md",
    "decisions/adr-0054-context-decisions.md",
    "research/mves-methodology.md",
    "invariants/architecture-invariants.md",
    "constraints/engineering-constraints.md",
    "rejected/heavy-context-technologies.md",
]


def _entries() -> list[Path]:
    files = sorted(p for p in STORE.rglob("*.md") if p.name != "README.md")
    assert files, "store must contain seed entries"
    return files


def _frontmatter(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    assert text.startswith("---\n"), f"{path}: missing YAML frontmatter"
    parts = text.split("---", 2)
    assert len(parts) == 3, f"{path}: malformed frontmatter fences"
    data = yaml.safe_load(parts[1])
    assert isinstance(data, dict), f"{path}: frontmatter is not a mapping"
    return data


def test_classification_entry_has_valid_type() -> None:
    for path in _entries():
        fm = _frontmatter(path)
        assert fm.get("type") in ALLOWED_TYPES, f"{path}: type {fm.get('type')!r} not in classification set"


def test_required_provenance_fields_present() -> None:
    for path in _entries():
        fm = _frontmatter(path)
        missing = REQUIRED_FIELDS - set(fm)
        assert not missing, f"{path}: missing provenance fields {sorted(missing)}"
        assert str(fm["source"]).strip(), f"{path}: empty source"
        assert str(fm["review_date"]).strip(), f"{path}: empty review_date"


def test_memory_is_not_authority() -> None:
    for path in _entries():
        fm = _frontmatter(path)
        assert fm.get("authority") == "MEMORY", f"{path}: authority must be MEMORY (memory is never authority)"


def test_status_and_origin_valid() -> None:
    for path in _entries():
        fm = _frontmatter(path)
        assert fm.get("status") in ALLOWED_STATUS, f"{path}: bad status"
        assert fm.get("origin") in ALLOWED_ORIGIN, f"{path}: bad origin"


def test_historical_is_not_current_and_rejected_is_rejected() -> None:
    for path in _entries():
        fm = _frontmatter(path)
        if fm.get("type") == "HISTORICAL_STATE":
            assert fm.get("status") != "CURRENT", f"{path}: HISTORICAL_STATE must not be CURRENT"
        if fm.get("type") == "REJECTED_OPTION":
            assert fm.get("status") == "REJECTED", f"{path}: REJECTED_OPTION must have status REJECTED"


def test_no_empty_directories_in_store() -> None:
    for child in sorted(STORE.iterdir()):
        if child.is_dir():
            assert list(child.rglob("*.md")), f"{child}: empty taxonomy directory"


def test_seed_files_exist() -> None:
    for rel in SEED_FILES:
        assert (STORE / rel).is_file(), f"missing seed file {rel}"


def test_skill_frontmatter_valid_for_opencode() -> None:
    assert SKILL.is_file(), "skill SKILL.md missing"
    text = SKILL.read_text(encoding="utf-8")
    parts = text.split("---", 2)
    assert len(parts) == 3, "skill frontmatter malformed"
    fm = yaml.safe_load(parts[1])
    name = fm.get("name")
    assert name == "ocm-context-bootstrap", "skill name mismatch"
    assert re.fullmatch(r"[a-z0-9]+(-[a-z0-9]+)*", name), "skill name invalid"
    assert SKILL.parent.name == name, "skill dir must match name"
    desc = fm.get("description", "")
    assert 1 <= len(desc) <= 1024, "skill description length invalid"
    meta = fm.get("metadata", {})
    assert isinstance(meta, dict) and all(isinstance(k, str) and isinstance(v, str) for k, v in meta.items()), (
        "skill metadata must be string-to-string"
    )


def test_bootstrap_locates_expected_context() -> None:
    text = SKILL.read_text(encoding="utf-8")
    assert "docs/engineering-context" in text, "skill must reference the store"
    assert "AGENTS.md" in text, "skill must reference authoritative L0 source"
    for rel in (
        "docs/architecture",
        "docs/audits",
        "docs/plans",
        "policies",
    ):
        assert rel in text, f"skill source map missing {rel}"


def test_context_pack_contains_required_sections() -> None:
    text = SKILL.read_text(encoding="utf-8")
    for section in PACK_SECTIONS:
        assert f"- {section}\n" in text, f"Context Pack section missing: {section}"


def test_context_pack_state_classes() -> None:
    text = SKILL.read_text(encoding="utf-8")
    for state in ("CURRENT", "HISTORICAL", "PROPOSED", "REJECTED", "UNKNOWN"):
        assert state in text, f"state class missing: {state}"


def test_fail_closed_markers_present() -> None:
    text = SKILL.read_text(encoding="utf-8")
    for marker in ("STOP", "REPORT CONFLICT", "DO NOT INVENT RESOLUTION"):
        assert marker in text, f"fail-closed marker missing: {marker}"


def test_no_automatic_promotion_rules_present() -> None:
    text = SKILL.read_text(encoding="utf-8")
    for banned in (
        "RECOMMENDATION → DECISION",
        "INFERENCE → AUTHORITY",
        "HISTORICAL → CURRENT",
        "MEMORY → ADR",
    ):
        assert banned in text, f"promotion-ban rule missing: {banned}"


def test_data_first_security_rule_present() -> None:
    text = SKILL.read_text(encoding="utf-8")
    assert "DATA FIRST" in text, "data-first security rule missing"
    assert "secrets" in text, "secrets prohibition missing"


def test_skill_is_procedural_not_normative() -> None:
    text = SKILL.read_text(encoding="utf-8")
    assert "NOT a normative source" in text
    assert "does NOT decide architecture" in text


def test_pack_template_headings_follow_pack_sections() -> None:
    tpl = SKILL.parent / "assets" / "context-pack.template.md"
    assert tpl.is_file(), "context-pack template missing"
    heads = [ln[3:] for ln in tpl.read_text(encoding="utf-8").splitlines() if ln.startswith("## ")]
    expected = [*PACK_SECTIONS, "Verdict"]
    assert len(heads) == len(expected), f"template headings: {len(heads)}"
    for head, want in zip(heads, expected):
        assert head.startswith(want), f"template drift: {head!r} vs {want!r}"
