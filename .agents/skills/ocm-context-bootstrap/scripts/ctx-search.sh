#!/usr/bin/env bash
# Read-only, capped retrieval for ocm-context-bootstrap (steps 3-9).
# Usage: bash .agents/skills/ocm-context-bootstrap/scripts/ctx-search.sh "<keyword>" [max_lines_per_group]
# Retrieves only. Status labels (CURRENT/HISTORICAL/...) come from reading each source.
kw="${1:?usage: ctx-search.sh <keyword> [max]}"; max="${2:-8}"
cd "$(git rev-parse --show-toplevel)" || exit 2
X=(-g '!*.zip' -g '!*.pdf' -g '!*.bak' -g '!*.swp' -g '!*.lock')
grp() { local label="$1"; shift; echo "## $label"; rg -n -i -m 2 --max-columns 180 --max-columns-preview "${X[@]}" -- "$kw" "$@" 2>/dev/null | head -n "$max"; }
grp "AUTHORITATIVE candidates (read Estado before use)" docs/architecture/decisions policies docs/governance docs/architecture/GOVERNANCE.md
echo "## ADR Estado of hits"
rg -l -i "${X[@]}" -- "$kw" docs/architecture/decisions 2>/dev/null | sort | head -n "$max" | while read -r f; do s=$(grep -m1 'Estado' "$f" | cut -c1-70); echo "$f :: ${s:-NO-ESTADO-LINE => UNKNOWN}"; done
echo "## ADR number collisions"; ls docs/architecture/decisions | sed -E 's/^(ADR-[0-9]+).*/\1/' | sort | uniq -d
echo "## HISTORICAL (SUPERSEDED-*)"; rg -l -i -- "$kw" docs/architecture/SUPERSEDED-* 2>/dev/null | head -n "$max"
grp "REQUIREMENTS/INVARIANTS/CONSTRAINTS" docs/DOMAIN.md docs/PLAN-Maestro-Ingenieria.md docs/plans/engineering-guardrails.md docs/plans/tracking.yaml
grp "MEMORY (never authority; follow its source)" docs/engineering-context
grp "RESEARCH/EVIDENCE (audits never authority)" docs/knowledge/manifest.yaml docs/proposals docs/audits docs/audit
grp "CODE" packages apps shared tests
