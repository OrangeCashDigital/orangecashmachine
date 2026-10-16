# Authority and store writes

AUTHORITY != MEMORY != EVIDENCE != PROVENANCE != RECOMMENDATION.
Precedence: Human Decision > ADR/Contract > Governance > Registry > Tracking/Plan > Implementation.
Memory (docs/engineering-context/) sits outside the chain; if it contradicts a source, the source wins and the conflict is reported.

Entry frontmatter (docs/engineering-context/README.md section 4): source, authority (always MEMORY), type, status, origin, created, review_date; adr_ref optional.
type: FACT, DECISION, REQUIREMENT, INVARIANT, CONSTRAINT, EVIDENCE, PROVENANCE, RECOMMENDATION, INFERENCE, OPEN_QUESTION, REJECTED_OPTION, HISTORICAL_STATE.
status: CURRENT, HISTORICAL, PROPOSED, REJECTED, UNKNOWN.

Writes: only origin: agent-written, inside docs/engineering-context/, with complete provenance. Per the README such entries await the human review gate. Never origin: human-approved (human only). Never write to authoritative sources.
Never promote RECOMMENDATION->DECISION, INFERENCE->AUTHORITY, HISTORICAL->CURRENT, MEMORY->ADR.
Contradictions are flagged, not resolved. Mark obsolete entries HISTORICAL/REJECTED with a pointer; never delete.
