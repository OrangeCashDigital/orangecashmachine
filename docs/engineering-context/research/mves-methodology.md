---
source: docs/architecture/decisions/ADR-0054-persistent-engineering-context-decisions.md; AGENTS.md
authority: MEMORY
type: DECISION
status: CURRENT
origin: agent-written
created: 2026-10-08
review_date: 2026-11-08
adr_ref: ADR-0054
---

# MVES methodology + Human Authority / Agent Engineering Autonomy

**Summary.** The working methodology decided in MD-A5 (ADR-0054) and used by
every OCM gate. Procedural summary — ADR-0054 and AGENTS.md are normative.

## Lifecycle (MD-A5)

```
DECIDE → SPECIFY → SCOPE → RESEARCH → DESIGN → PLAN → AUTHORIZE →
IMPLEMENT → VERIFY → CONVERGE → ATTEST
```

## Human Authority, Agent Engineering Autonomy

- The agent decides reversible technical choices determined by: ADRs,
  contracts, requirements, governance, repository evidence, official docs,
  standards, best engineering practice (E1–E7).
- Escalate only when E1–E7 are exhausted AND E8 applies (genuine human
  authority/architecture/scope/risk/contract/policy/strategic/irreversible).

| Test | Source |
|---|---|
| E1 ADR exists | `docs/architecture/decisions/` |
| E2 Specification / Contract | contracts, policies, specs |
| E3 Governance | AGENTS.md, GOVERNANCE.md, AUDIT_PROTOCOL.md |
| E4 Repository evidence | code, tests, CI behavior |
| E5 Knowledge Base / docs | `docs/knowledge/` (TIER/needs_verification labels) |
| E6 Standards / primary sources | official technology documentation |
| E7 Best engineering practice | established practice |
| E8 Genuine human authority/strategy | **HUMAN DECISION REQUIRED** |

Multiple technical alternatives alone are never a reason to escalate.

## Evidence labels

VERSION-VERIFIED · OFFICIAL-DOCUMENTATION · REPOSITORY-VERIFIED · INFERENCE ·
RECOMMENDATION. External books/papers are knowledge/hypotheses, never evidence
of edge or authority (AGENTS.md Knowledge Base policy).

## Precedence (MD-A2)

Human Decision > ADR / Contract > Governance > Registry > Tracking / Plan >
Implementation. Lower authority never overrides higher; contradictions are
surfaced fail-closed, never silently resolved.

**Authority.** MEMORY. ADR-0054 + AGENTS.md are normative.
