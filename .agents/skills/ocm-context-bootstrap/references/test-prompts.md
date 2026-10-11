# Activation tests (run manually in OpenCode)
1. "Voy a modificar packages/trading/..., qué contexto aplica?" -> loads the skill, returns a Context Pack with a status per item, cites ADRs by full path.
2. "Corrige un typo en README.md" -> must NOT load the skill.
3. "Qué dice el ADR-0054?" -> cites the full path of the single context-decisions file and marks it PROPOSED (the pyarrow risk-accept is ADR-0056).
