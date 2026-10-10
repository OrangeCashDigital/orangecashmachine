#!/usr/bin/env bash
# Read-only validator (agentskills.io rules). Usage: bash scripts/check-skill.sh
D="$(cd "$(dirname "$0")/.." && pwd)"; fail=0
n=$(sed -n 's/^name: *//p' "$D/SKILL.md" | head -1)
[ "$n" = "$(basename "$D")" ] || { echo "FAIL name!=dir"; fail=1; }
echo "$n" | grep -Eq '^[a-z0-9]+(-[a-z0-9]+)*$' && [ ${#n} -le 64 ] || { echo "FAIL name format"; fail=1; }
dl=$(sed -n 's/^description: *//p' "$D/SKILL.md" | head -1 | wc -c)
{ [ "$dl" -gt 1 ] && [ "$dl" -le 1025 ]; } || { echo "FAIL description length $dl"; fail=1; }
[ "$(wc -l < "$D/SKILL.md")" -lt 500 ] || { echo "FAIL SKILL.md >=500 lines"; fail=1; }
for r in $(grep -o 'references/[A-Za-z0-9_.-]*\.md' "$D/SKILL.md" | sort -u); do [ -e "$D/$r" ] || { echo "MISSING $r"; fail=1; }; done
cd "$(git -C "$D" rev-parse --show-toplevel)" || exit 2
grep -ohE '(docs|policies|packages|apps|shared|tests|architecture_linter)/[A-Za-z0-9_./-]+' "$D"/SKILL.md "$D"/references/*.md | grep -v '000x' | sort -u | while read -r p; do p=${p%.}; [ -e "$p" ] || echo "MISSING $p"; done | grep . && fail=1
for s in $(grep -o 'scripts/[A-Za-z0-9_.-]*\.sh' "$D/SKILL.md" | sort -u); do [ -x "$D/$s" ] || { echo "MISSING-OR-NOT-EXEC $s"; fail=1; }; done
for a in $(grep -o 'assets/[A-Za-z0-9_.-]*\.md' "$D/SKILL.md" | sort -u); do [ -e "$D/$a" ] || { echo "MISSING $a"; fail=1; }; done
[ $fail -eq 0 ] && echo "SKILL OK" || { echo "SKILL FAIL"; exit 1; }
