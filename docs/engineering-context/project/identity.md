---
source: AGENTS.md; pyproject.toml
authority: MEMORY
type: FACT
status: CURRENT
origin: agent-written
created: 2026-10-08
review_date: 2026-11-08
---

# Project identity — OrangeCashMachine (OCM)

**Summary.** Crypto market data lakehouse: Medallion (Bronze→Silver→Gold) +
Iceberg + Hydra. Clean/Hexagonal with bounded contexts; 49 import-linter contracts
(BC-NN) verified in CI. Domain is 100% framework-agnostic (zero pandas/polars).

**Package remapping (hatchling).**
`packages/market_data → market_data`, `packages/trading → trading`,
`packages/portfolio → portfolio`, `apps/app → app`, `apps/api → api`,
`shared/ → shared` (no remap). `apps/research` importable as `research`.

**Entrypoints.**
- `uv run ocm` — market data pipeline (CLI: `app.cli.main`; **no main.py at root**)
- `uv run ocm-api` — FastAPI gateway (experimental)
- `uv run live` — ⚠️ real capital · `uv run paper` — paper trading
- `./run.sh ocm` — same pipeline via script
- Config validation (no side effects): `OCM_VALIDATE_ONLY=true uv run python -m app.cli.main`
  (`OCM_VALIDATE_ONLY` uses BOOL_TRUE = {true, yes, on}; `1` does NOT work)

**Infra (docker compose).** Redis, Kafka, Prometheus.

**Relevance.** Entry point for every task: identify bounded context + layer
(domain→ports→application→adapters→infrastructure) before touching code.

**Authority.** This entry is MEMORY. Normative: `AGENTS.md`,
`docs/architecture/GOVERNANCE.md`, ADRs, CI contracts.
