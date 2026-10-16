#!/usr/bin/env bash
# Reproduce CI locally: run the non-integration suite against HEAD only
# (no untracked or uncommitted files). Never touches the working tree.
set -euo pipefail

REPO="$(git rev-parse --show-toplevel)"
PY="${PY:-$REPO/.venv/bin/python}"
[ -x "$PY" ] || PY="python3"
WT="$(mktemp -d /tmp/ocm-clean.XXXXXX)"

cleanup() {
  git -C "$REPO" worktree remove --force "$WT" 2>/dev/null || true
  rm -rf "$WT"
  git -C "$REPO" worktree prune
}
trap cleanup EXIT

git -C "$REPO" worktree add --detach "$WT" HEAD >/dev/null
cd "$WT"
# Resolve imports from THIS checkout, not from an editable install of the main tree.
export PYTHONPATH="$WT/packages:$WT/apps:$WT${PYTHONPATH:+:$PYTHONPATH}"

echo "== market_data se importa desde el checkout limpio?"
"$PY" - <<'PYEOF'
import pathlib
try:
    import market_data
except ImportError as exc:
    print(f"n/a ({exc})")
else:
    where = pathlib.Path(market_data.__file__).resolve()
    inside = pathlib.Path.cwd().resolve() in where.parents
    print(f"{where}\n{'OK' if inside else 'FAIL: resuelve FUERA del checkout limpio'}")
    raise SystemExit(0 if inside else 3)
PYEOF

rc=0
LOG="$(mktemp /tmp/ocm-clean-pytest.XXXXXX)"
"$PY" -m pytest tests/ -q -rfE -m "not integration" --no-cov \
  -p no:cacheprovider >"$LOG" 2>&1 || rc=$?
grep -E '^(FAILED|ERROR) |[0-9]+ (passed|failed)' "$LOG" | tail -40 || true
echo "log completo: $LOG"
bash .agents/skills/ocm-context-bootstrap/scripts/check-skill.sh || rc=$?
exit "$rc"
