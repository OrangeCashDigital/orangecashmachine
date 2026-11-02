# Engineering Context Store — Persistent Engineering Context (OCM)

Memory layer for engineering agents. Created by gate **P2 — Implementation
Authorization** (authorized scope, 2026-10-08). This store is an ADR-0054
materialization (see `docs/architecture/decisions/ADR-0054-persistent-engineering-context-decisions.md`).

## 1. What Persistent Engineering Context IS

- A small, git-versioned collection of concise notes that let a new agent recover
  project identity, architecture, decisions, invariants, constraints, rejected
  approaches and methodology **before** an engineering task.
- A companion to the procedural skill `.opencode/skills/ocm-context-bootstrap/SKILL.md`,
  which retrieves from this store plus existing authoritative sources.

## 2. What it is NOT

- NOT a source of architectural authority (see §3).
- NOT a knowledge base, RAG system, knowledge graph, vector store, database,
  transcript archive, or agent platform. Those technologies are explicitly
  rejected (see `rejected/heavy-context-technologies.md`).
- NOT a copy of the repository's normative documents. Entries are summaries with
  `source` pointers; the authoritative document always wins.

## 3. Authority

```
AUTHORITY != MEMORY != EVIDENCE != PROVENANCE != RECOMMENDATION
```

- ADR / Contract can constitute authority. Governance (AGENTS.md, GOVERNANCE.md)
  constrains process. Everything in this store has `authority: MEMORY`.
- Precedence (ADR-0054, MD-A2): Human Decision > ADR / Contract > Governance >
  Registry > Tracking / Plan > Implementation. **Memory sits outside the chain.**
- Recommendation is not authority; inference is not a decision; historical state
  is not automatically current state; evidence does not become authority by
  being persisted.

## 4. Provenance

Every entry carries frontmatter:

| Field | Rule |
|---|---|
| `source` | Required. Repo-relative path (and line/section) of the normative or evidence source. |
| `authority` | Required. Always `MEMORY` for entries in this store. |
| `type` | Required. One of the 12 classifications below. |
| `status` | Required. One of `CURRENT`, `HISTORICAL`, `PROPOSED`, `REJECTED`, `UNKNOWN`. |
| `origin` | Required. `auto-derived`, `agent-written`, `human-approved`, or `historical`. |
| `created` | Required. ISO date. |
| `review_date` | Required. Suggested staleness review date. |
| `adr_ref` | Optional. **OCM convention**, not an OpenCode-native field. |

Unknown or missing provenance → treat as `INSUFFICIENT EVIDENCE`, do not use.

### Memory classification (`type`)

FACT · DECISION · REQUIREMENT · INVARIANT · CONSTRAINT · EVIDENCE · PROVENANCE ·
RECOMMENDATION · INFERENCE · OPEN_QUESTION · REJECTED_OPTION · HISTORICAL_STATE

## 5. Lifecycle / update policy

- **auto-derived**: regenerated from the repo (fingerprints, inventories).
- **agent-written**: created by an agent under `proposals/`-style review flow;
  P2 seed entries are agent-written and await the verification/review gate.
- **human-approved**: promoted only after explicit human review (never by agent).
- **historical**: describes earlier states; marked `status: HISTORICAL`, kept forever.

Never promote automatically: `RECOMMENDATION → DECISION`,
`INFERENCE → AUTHORITY`, `HISTORICAL → CURRENT`, `MEMORY → ADR`.
Contradictions are flagged, not resolved. Obsolete entries are marked, not deleted.

## 6. How ocm-context-bootstrap works

Identify task → identify domain → retrieve authoritative sources → retrieve
decisions → requirements → invariants → constraints → research → inspect affected
code → build Context Pack → verify authority → proceed (or fail-closed stop).
The skill is procedural only; see its SKILL.md for the exact steps and source map.

## 7. How to build a Context Pack

Use the 17-section template in the skill (Task, Domain, Authoritative Sources,
Relevant ADRs, Current Decisions, Requirements, Invariants, Constraints, Relevant
Code, Relevant Tests, Research, Rejected Approaches, Known Failures, Open
Questions, Provenance, Authority Classification, Risks). Classify every item
CURRENT / HISTORICAL / PROPOSED / REJECTED / UNKNOWN. Unfillable sections get
`INSUFFICIENT EVIDENCE`.

## 8. What may be stored

Summaries with provenance: identity, architecture map, decision indices with
pointers, invariants, constraints, rejected approaches, open questions, research
findings with evidence labels, session handoffs (once needed), known failures
with dates. Prefer `summary + source + authority + status + relevance` over
duplication.

## 9. What must NOT be stored

Secrets, credentials, tokens, PII; normative copies of ADRs/contracts; invented
authority; unverifiable claims; anything promoting a recommendation or inference
to a decision; raw session transcripts; large verbatim duplications.

## 10. Conflict handling (fail-closed)

Authority conflict, unknown provenance, ambiguous or contradictory information:

```
STOP → REPORT CONFLICT → DO NOT INVENT RESOLUTION
```

Escalate to a human only after E1 (ADR) … E7 (best practice) are exhausted and
E8 (genuine human authority) applies.

## Directory taxonomy

Created only with justified initial content:

| Directory | Purpose | State |
|---|---|---|
| `project/` | Project identity | seed present |
| `architecture/` | Architecture map | seed present |
| `decisions/` | Decision indices (pointers, not copies) | seed present |
| `requirements/` | Requirement summaries | reserved (on demand) |
| `invariants/` | Invariant index | seed present |
| `constraints/` | Constraint index | seed present |
| `research/` | Research + methodology notes | seed present |
| `rejected/` | Rejected approaches | seed present |
| `known-failures/` | Dated failure lessons | reserved (on demand) |
| `code-relationships/` | Derived code relation notes | reserved (on demand) |
| `git/` | Derived git-state snapshots | reserved (on demand) |
| `handoffs/` | Agent session handoffs | reserved (on demand) |

Reserved directories are NOT pre-created (no empty taxonomy).
