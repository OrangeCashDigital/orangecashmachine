---
source: AGENTS.md; config/base.yaml; pyproject.toml; ocm/config/env_vars.py; ocm/config/layers/coercion.py
authority: MEMORY
type: CONSTRAINT
status: CURRENT
origin: agent-written
created: 2026-10-08
review_date: 2026-11-08
---

# Engineering constraints (index — sources are normative)

**Summary.** Operational constraints that bound every task. Consult the source
files for the authoritative text.

## Safety / config

- **`dry_run: true`** = global default in `config/base.yaml`; production must
  override explicitly. Never reach production by omission.
- **`uv run ocm --cfg job`** prints secrets (Hydra DictConfig pre-Pydantic).
  Never pipe to logs in production.
- **`OCM_VALIDATE_ONLY`** accepts only `BOOL_TRUE = {true, yes, on}`
  (`ocm/config/layers/coercion.py`); `1` does NOT activate validate-only.

## Dependencies

- Pinned with documented reasons (read comments before bumping):
  `pydantic==2.14.0`, `loguru==0.7.3`, `pyyaml==6.0.3`,
  `aioresilience==0.2.1`, `pybreaker==1.4.1`.
  `ccxt>=4.5.74` (rango, no pin) y `pyarrow>=17.0,<27.0` (specifier estrecho acoplado
  a `uv.lock`; guard `uv lock --check` en CI).
- `uv.lock`: change only on a real dependency change. Never regenerate/sync/
  revert casually (external drift observed 2026-10-08: report, do not repair).

## Git / process

- Branch: solo main; Conventional Commits; atomic commits; never
  `git push --force` on main; never commit `.coverage`, `.venv`,
  `.pytest_cache`; pre-commit = `ruff check --fix` → `ruff format`, never skip.
- `docs/plans/tracking.yaml` is governance-frozen inside audit/implementation
  gates unless that gate explicitly authorizes edits.
- CD workflow is a placeholder (`workflow_dispatch` only).

## Testing

- Integration tests marked `@pytest.mark.integration` (need infra);
  `asyncio_mode=auto`; pytest pythonpath includes `.` and `apps`.
- mypy excludes `tests/` and `.venv/`; ruff/format/lint-imports are merge gates.

**Authority.** MEMORY. pyproject/config/AGENTS.md/CI are the authority.
