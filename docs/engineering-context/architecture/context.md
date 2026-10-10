---
source: docs/architecture/GOVERNANCE.md; docs/architecture/0000-principios-arquitectonicos.md; docs/architecture/0001-bounded-contexts-composition-root.md; docs/architecture/0002-event-driven-kappa-architecture.md; architecture_linter/importlinter.toml
authority: MEMORY
type: FACT
status: CURRENT
origin: agent-written
created: 2026-10-08
review_date: 2026-11-08
---

# Architecture context — current state map

**Summary.** Layered Clean/Hexagonal, dependency direction:

```
shared → ocm → domain → ports → application → adapters → infrastructure
```

- `shared/` — lowest layer: stdlib + approved 3rd-party only (types, kafka
  schemas, contracts/Protocols, exceptions, utils).
- `ocm/` — platform (config/runtime/observability), no business logic.
- `packages/market_data|trading|portfolio` — each a bounded context with
  domain→ports→application→adapters→infrastructure.
- `apps/app` — CLI entrypoints; `apps/api` — FastAPI gateway (experimental);
  `apps/research` — read-only gold-layer consumer.

**Composition roots (per BC — ADR-0003 inherited series; narrow constructor
`decisions/ADR-0003` is a different, trading-specific record):**
`market_data.infrastructure.bootstrap.composition_root`,
`trading.bootstrap.composition_root`, `portfolio.bootstrap.composition_root`.

**Event model.** Event-driven kappa (ADR-0002); Kafka wire schemas only in
`shared/kafka/schemas/` (BC-35).

**Governance automation.** import-linter BC-NN contracts
(`architecture_linter/importlinter.toml`, gate: broken = blocked merge) +
`architecture_linter` AST invariants ARCH-001..010 (exit 1 on FAIL/PARTIAL) +
`tests/architecture/` structural contracts.

**Adrs entry map.** Series canónica 0000–0044 + heredada (see GOVERNANCE §9);
newest decisions live in `docs/architecture/decisions/` (ADR-0045..0054 as of
2026-10-08; superseded records carry `SUPERSEDED-` prefix).

**Relevance.** Start here for any architecture-affecting task; then open the
governing ADRs (never resolve from this note alone).

**Authority.** MEMORY. Normative: GOVERNANCE.md, ADRs, CI contracts.
