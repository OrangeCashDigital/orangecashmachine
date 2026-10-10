---
source: AGENTS.md; architecture_linter/importlinter.toml; architecture_linter/architecture_linter.toml
authority: MEMORY
type: INVARIANT
status: CURRENT
origin: agent-written
created: 2026-10-08
review_date: 2026-11-08
---

# Architecture invariants (index — contracts are normative)

**Summary.** Pointer index of structural invariants. The contracts themselves
are normative; never relax them based on this note.

1. **Dependency direction**: `shared → ocm → domain → ports → application →
   adapters → infrastructure`. Never import infrastructure into domain.
   *(AGENTS.md)*
2. **No direct cross-BC imports** between bounded contexts; use ports/contracts.
   *(AGENTS.md)*
3. **Composition Root per BC**: `market_data.infrastructure.bootstrap.composition_root`,
   `trading.bootstrap.composition_root`, `portfolio.bootstrap.composition_root`.
   *(ADR-0003 inherited series; GOVERNANCE §9)*
4. **49 BC-NN contracts** (import-linter): broken contract = blocked merge.
   *(architecture_linter/importlinter.toml)*
5. **ARCH-001..010**: AST invariants (architecture_linter); exit 1 on
   FAIL/PARTIAL. Reproduces BC-08 layers (ARCH-009) including ignore_imports.
   *(architecture_linter/architecture_linter.toml)*
6. **BC-09**: domain imports no data/framework infra (zero pandas/polars in
   domain). *(import-linter BC-09 + AGENTS.md migration notes)*
7. **BC-35**: all Kafka wire schemas live in `shared/kafka/schemas/` only.
   *(AGENTS.md)*
8. **shared/**: stdlib + approved 3rd-party only.
9. **E402** allowed only in files listed in pyproject per-file-ignores — not a
   global ignore. **`type: ignore` requires an explanatory comment.**
   *(AGENTS.md)*
10. **`ocm/config/env_vars.py`** = SSOT for all `OCM_*` env var names; never
    define env var name strings elsewhere. *(AGENTS.md)*

**Relevance.** Before any structural change, run:
`uv run lint-imports --config architecture_linter/importlinter.toml` and
`uv run python -m architecture_linter --root .`.

**Authority.** MEMORY. The contracts/toml files are the authority.
