# Activation tests (run manually in OpenCode)
1. "Voy a modificar packages/trading/..., qué contexto aplica?" -> loads the skill, returns a Context Pack with a status per item, cites ADRs by full path.
2. "Corrige un typo en README.md" -> must NOT load the skill.
3. "Qué dice el ADR-0054?" -> reports two files share the number, marks the context-decisions one PROPOSED.
