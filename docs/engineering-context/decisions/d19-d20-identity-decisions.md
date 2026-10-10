---
source: docs/audits/D19_HUMAN_DECISION_PACK_2026-09-30.md; docs/architecture/decisions/ADR-0049-canonical-instrument-identity-g1.md#L548-L561; docs/audits/AUDIT_OCM_high-impact-improvements_2026-10-07.md
authority: MEMORY
type: DECISION
status: CURRENT
origin: agent-written
created: 2026-10-08
review_date: 2026-11-08
adr_ref: ADR-0049
---

# D19 / D20a / D20b — identity & physical materialization decisions (index)

**Summary.** Index of decided items. Pointers only — the pack and ADR-0049 are
the sources. Verified against both on 2026-10-08.

## D19 (pack 2026-09-30, physical materialization)

| Item | Status | Selected |
|---|---|---|
| D19-01 Physical Identity IDs | HUMAN DECISION MADE | C+ Hybrid Controlled Identity (`identity_id`) |
| D19-02 Physical ListingIdentity Model | DECIDED | A — Pure Domain Entity |
| D19-03 DefinitionVersion / ContractTerms Model | DECIDED | A — Pure Domain Entity + Effective Intervals |
| D19-04 Physical ValidityWindow | DECIDED | B — Embedded Fields per Concept |
| D19-05 Physical VenueConstraints Model | DECIDED | A — Independent VenueConstraints Domain Entity |
| D19-06 Evidence / Provenance / Identity Relations | DECIDED | A — Independent Concepts |
| D19-07 Persistence Scope | DECIDED | A — Durable Canonical State |
| D19-08 Persistence / Backend Strategy | DECIDED | D — Hybrid Relational + Iceberg |
| D19-09 Event Stream / Kafka Materialization | DECIDED | B — Domain Events by Semantic Lane, optional transport |
| D19-10 Domain / App / Port / Registry Boundary | DECIDED | C — Hybrid Core Registry + Lane-Specific Extensions |

## D20 (split gates)

- **D20 = DECIDED — SPLIT GATES.**
- **D20a = DECIDED — Transactional Outbox + Domain Event Propagation Semantics**
  (semantic lane semantics, ordering semantics, event identity semantics;
  physical representation explicitly deferred to D20b).
- **D20b = DECIDED — Physical Persistence / Representation Architecture**
  (ADR-0049:548-561):
  - D20b-01 relational canonical model with explicit temporal semantics
  - D20b-02 single logical Transactional Outbox
  - D20b-03 versioned domain-event schemas, controlled compatibility
  - D20b-04 three semantic-lane transport channels
  - D20b-05 logical ordering by ListingIdentity + Semantic Lane
  - D20b-06 hybrid event-history + state/analytical projection
- **D21–D24 = BLOCKED. IMPLEMENTATION = NOT AUTHORIZED.**

## Open / conflict flags (fail-closed)

- **F-OCM-01** (audit 2026-10-07) documents a source contradiction on D19-01
  status: pack + ADR-0049 say DECIDED; `D19-01_INDUSTRY_BOW_ARCHITECTURE_REVIEW_2026-10-03.md`
  and `docs/knowledge/KNOWLEDGE_BASE_CONTRACT.md` say HUMAN DECISION REQUIRED.
  → Do NOT resolve from memory. Consult the sources; escalate per fail-closed
  rules if a task depends on it.

**Authority.** MEMORY. The pack/ADR statuses above are the authority.
