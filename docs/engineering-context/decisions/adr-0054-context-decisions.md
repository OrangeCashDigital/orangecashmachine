---
source: docs/architecture/decisions/ADR-0054-persistent-engineering-context-decisions.md
authority: MEMORY
type: DECISION
status: PROPOSED
origin: agent-written
created: 2026-10-08
review_date: 2026-11-08
adr_ref: ADR-0054
---

# ADR-0054 — Persistent Engineering Context decisions (index)

**Summary.** The seven human decisions governing this store and the
`ocm-context-bootstrap` skill. ADR-0054 `Estado: Aceptado parcial` (aceptación
documental 2026-10-11; MD-A4 permanece Propuesto, ver ADR-0055). La aceptación
no autoriza implementación; la línea Estado del propio ADR manda sobre esta nota.

| Decision | Substance |
|---|---|
| H-CTX-1 | OPTION A — documentary / minimum-sufficient context model; no heavyweight infrastructure |
| H-CTX-2 | OPTION B — controlled/auditable write policy; memory never normative; conflicts → authoritative source wins |
| MD-A1 | `adr:null` = no established ADR authority; fail-closed; never infer/populate ADR refs; rules R2..R8 preserved |
| MD-A2 | Precedence: Human Decision > ADR/Contract > Governance > Registry > Tracking/Plan > Implementation; memory outside chain; conflict → FAIL-CLOSED → REPORT → HUMAN |
| MD-A3 | Evidence ≠ Provenance ≠ Memory ≠ Authority ≠ Decision; persistence does not create authority |
| MD-A4 | D20b = DECIDED / MATERIALIZED / VERIFIED architectural basis; **implementation NOT authorized** by that |
| MD-A5 | MVES + Human Authority / Agent Engineering Autonomy; lifecycle + E1–E8 escalation test |

**Rejected best-of-world list (binding anti-overengineering):** vector DB,
embeddings, graph DB/Neo4j, temporal KG, transcript mining, distributed memory
daemons, specialized memory agents, heavy RAG, SCIP/LSIF, external MCP memory,
SaaS/cloud memory, microservices memory infra. See `../rejected/heavy-context-technologies.md`.

**Relevance.** Any task touching this store, the skill, context retrieval, or
authority semantics must start from ADR-0054.

**Authority.** MEMORY. ADR-0054 is the normative source.
