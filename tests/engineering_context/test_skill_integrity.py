"""Guards for ocm-context-bootstrap and ADR numbering (run by CI via pytest)."""

import re
import subprocess
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
ADR_DIR = REPO / "docs" / "architecture" / "decisions"
CHECK_SKILL = REPO / ".opencode" / "skills" / "ocm-context-bootstrap" / "scripts" / "check-skill.sh"

# Known collisions awaiting human renumbering. Remove an entry once fixed;
# test_known_collisions_not_stale fails if an entry no longer collides.
KNOWN_ADR_COLLISIONS: set[str] = set()


def _adr_collisions() -> set[str]:
    numbers = []
    for path in ADR_DIR.glob("ADR-*.md"):
        match = re.match(r"(ADR-\d+)", path.name)
        if match:
            numbers.append(match.group(1))
    return {num for num, count in Counter(numbers).items() if count > 1}


def test_no_new_adr_number_collisions() -> None:
    new = _adr_collisions() - KNOWN_ADR_COLLISIONS
    assert not new, f"duplicate ADR numbers: {sorted(new)}"


def test_known_collisions_not_stale() -> None:
    stale = KNOWN_ADR_COLLISIONS - _adr_collisions()
    assert not stale, f"collision fixed, remove from allowlist: {sorted(stale)}"


def test_check_skill_script_passes() -> None:
    result = subprocess.run(["bash", str(CHECK_SKILL)], capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "SKILL OK" in result.stdout
