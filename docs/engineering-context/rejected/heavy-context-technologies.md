---
source: docs/architecture/decisions/ADR-0054-persistent-engineering-context-decisions.md
authority: MEMORY
type: REJECTED_OPTION
status: REJECTED
origin: agent-written
created: 2026-10-08
review_date: 2026-11-08
adr_ref: ADR-0054
---

# Rejected approaches — context/memory technology (binding list)

**Summary.** Rejected for Persistent Engineering Context. Source: ADR-0054
best-of-world rejection list + P2 design gate anti-overengineering analysis.
Do NOT reintroduce without a new, explicit human decision.

| Rejected | Why (short) |
|---|---|
| Vector database + embeddings | Corpus is small docs; rg + classification suffice; unrevisable index; cost without benefit |
| Graph DB / Neo4j / temporal KG | Graph already covered by import-linter + pydeps; heavy ops |
| Transcript mining of sessions | Sessions volatile by design; selective handoff instead |
| Distributed memory daemons / background indexers | Hidden state, non-auditable |
| Specialized memory agents | No separation of responsibilities demonstrated |
| Heavy RAG pipelines | Authority/provenance semantics break; overkill |
| SCIP / LSIF indexes | Extra infra; existing tooling answers the needs |
| External MCP memory server / SaaS memory / cloud memory | External dependency, provenance opacity, attack surface |
| Microservices / memory infra | Violates minimum sufficient system |
| SQLite local index (deferred, not adopted) | Not needed at current scale; rg sufficient; revisit only via dedicated gate if >~500 docs or recall failure |
| Custom OpenCode tools / plugins for retrieval | Native tools (grep/glob/read/bash/skill) suffice |
| Specialized context agents (researcher/code-intel/handoff) | Skill + Explore/natives suffice; prompt drift cost |

**Relevance.** Consult before proposing "improvements" to context retrieval.

**Authority.** MEMORY. ADR-0054 rejection list is the normative basis.
