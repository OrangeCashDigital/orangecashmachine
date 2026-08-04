# Informe — Auditoría y remediación de Composition Roots (2026-08-03)

**Alcance:** auditoría de composition roots + remediación por fases (A–E) del
proyecto OrangeCashMachine, verificación §7 por fase y final.

**Resultado global:** contrato de arquitectura **44/44 KEPT (0 BROKEN)** ·
**759 tests passed** · `ruff` y `ruff format` limpios · validate-only OK ·
`mypy` sin errores nuevos (17 pre-existentes, todos fuera del alcance).

---

## 1. Resumen ejecutivo

| Fase | Objetivo | Estado |
|---|---|---|
| A | Fix crítico `uv run ocm` (H1: orquestador construido sin factory) | ✅ Verificada §7 |
| B | Reconciliar `TradingCompositionRoot` contra ADR-0003 (H2/H3) | ✅ Verificada §7 |
| C | Eliminar legados `live.py`/`paper.py` (H4) | ✅ Verificada §7 |
| D | Resolver colisión de numeración de ADRs (H5) | ✅ Verificada §7 |
| E | Nota de proceso: atomic commits (H6) | ✅ Verificada §7 |

Hallazgos **H1–H6** cerrados. **H7** (CompositeFillObserver) y **H9**
(forense predata BC-43/ADR-0006) cerrados en Fase B por decisión. **H8**
no se reproduce.

---

## 2. Contexto y método

- **Auditoría:** revisión manual de todos los entrypoints y composition roots
  (grep/cat), verificación en runtime y cruce contra documentación
  arquitectónica (ADRs, forense de bytecode, GOVERNANCE).
- **Forense:** `packages/trading/bootstrap/composition_root.py` se perdió sin
  committear; su diseño se recuperó por introspección de marshal sobre
  bytecode (intención de diseño, no especificación ejecutable).
- **Gobernanza aplicada:** import-linter BROKEN = merge bloqueado;
  `exclude_type_checking = true`; verificación §7 por fase:
  `ruff check`, `lint-imports`, `pytest -x -q`, validate-only.

---

## 3. Hallazgos (H1–H9) y estado final

| # | Hallazgo | Estado |
|---|---|---|
| H1 | `entrypoint.py`/`main.py` construían `PipelineOrchestrator()` sin factory | 🔴 Crítico → **CERRADO (Fase A)** |
| H2 | `TradingCompositionRoot` en disco contradice el diseño aprobado | 🔴 Crítico → **CERRADO (Fase B)** |
| H3 | `TradingConfig` bloqueado (no existía en schema) | 🔴 Crítico → **CERRADO (Fase B)** |
| H4 | `live.py`/`paper.py` legado sin consumidores | 🟢 Leve → **CERRADO (Fase C)** |
| H5 | Dos series de ADR colisionan en numeración | 🟡 Moderado → **CERRADO (Fase D)** |
| H6 | Commit `feat(portfolio)` tocó dos BCs | 🟡 Proceso → **CERRADO (Fase E)** |
| H7 | `CompositeFillObserver` no localizado | 🔴 → **CERRADO**: SSOT es `build_fill_sync()` |
| H8 | No se reproduce | ⚪ → **No aplica** |
| H9 | Forense predata ADR-0006/BC-43 | 🟡 → **CERRADO por decisión (G1)** |

---

## 4. Fase A — Fix crítico `uv run ocm` (H1)

**Problema:** `apps/app/cli/entrypoint.py:77` y `packages/market_data/main.py`
construían `PipelineOrchestrator()` **sin inyectar factory**, contradiciendo
BC-38 (punto único de ensamblado vía `CompositionRoot`).

**Cambios:**
- `apps/app/cli/entrypoint.py::_run_all()` → `CompositionRoot.assemble(app_cfg).factory`.
- `packages/market_data/main.py::_ingestion_loop` → mismo fix.
- `tests/app/test_cli_entrypoint.py`: 2 tests herméticos (`RunConfig` con
  `config_path=None`, `pushgateway=""`).

**Verificación §7:** lint-imports 43/43 · 750 passed · validate-only OK.

---

## 5. Fase B — Reconciliación de `TradingCompositionRoot` (H2/H3)

### Guardrails del usuario (G1/G2/G3)
- **G1 (H9):** NO reconstruir `_build_position_store_*`/`_build_portfolio`/
  `RedisFactory` del forense (violaría BC-43). El root **inyecta** portfolio
  ensamblado por `PortfolioCompositionRoot.assemble()`.
- **G2 (H3):** la v2 del root (`__init__(config: TradingConfig)`) queda
  **descartada por completo**; reescritura desde cero.
- **G3 (H5):** colisión de ADRs difiere a Fase D (no bloqueante).

### Cambios (B1–B6)
- **B1/B2:** enmiendas a `ADR-0003` (constructor real
  `__init__(trading, risk, portfolio, guard=None)`) y `ADR-0004`
  (frontera trading→market_data formalizada como **BC-50**).
- **B3:** `TradingRuntime` (dataclass `engine, portfolio, tracker`).
- **B4:** reescritura completa del root: `_GoldFeatureSource` interno (único
  import de market_data permitido por BC-50, sin I/O en construcción),
  `_map_risk_config`/`_resolve_risk_config` (mapeo `AppConfig.risk` →
  `trading.risk.models.RiskConfig`, `min_confidence` CLI sin inventar valor),
  `assemble_live()` fail-fast (guard+risk obligatorios), `assemble_paper()`
  fail-soft, `assemble_rebalance()` → stub `NotImplementedError` (D3),
  `build_fill_sync(tracker, portfolio)` inyectado.
- **B5:** `execute_live.py::build_live_engine()` y
  `execute_paper.py::build_paper_engine()` conectados al root; el tracker
  del runtime es la única fuente de fills.
- **B6:** `packages/trading/data/gold_adapter.py` retirado;
  `TradingEngine.build_live/build_paper` deprecados (se mantienen para
  `paper_bot.py`).
- **BC-50:** contrato `forbidden` en `architecture/importlinter.toml`
  (44 contratos tras agregarlo).
- **Forense:** §7 corregido (RedisFactory obsoleto; `build_fill_sync` cierra
  el H7).
- **Tests:** `tests/trading/test_composition_root.py` (9 tests).

**Verificación §7:** ruff limpio · format 404/404 · **44/44 KEPT** ·
**759 passed** · validate-only OK · mypy sin errores nuevos.

---

## 6. Fase C — Eliminar legados (H4)

- **C1:** `git rm apps/app/cli/live.py apps/app/cli/paper.py` — sin
  consumidores; `[project.scripts]` resuelve `live`/`paper` a
  `live_hydra`/`paper_hydra`.
- **C2:** `portfolio_service` ahora **obligatorio** en `execute_live.py` y
  `execute_paper.py`:
  - Eliminados los fallbacks inline (`RedisPositionStore`/`build_redis_client`
    en live; `InMemoryPositionStore` en paper).
  - `PortfolioService` movido a import `TYPE_CHECKING`.
  - `redis_client` en `LiveEngineResources` queda `None` (la conexión Redis
    pertenece al `PortfolioCompositionRoot` del caller, SSOT).
  - Limpieza de docstrings y comentarios stale que referenciaban los legados.
- **C3:** `README.md` actualizado (nota legacy → "eliminados 2026-08-03").
- **C4:** borrado `pyproject.toml.bak.20260803_163334`.

---

## 7. Fase D — Colisión de numeración de ADRs (H5)

Decisión **D5** (sin renombres masivos de archivos):

- Banner **"SERIE HEREDADA"** en cada `docs/architecture/0000-0005*.md`.
- **GOVERNANCE.md §9** (nuevo): serie canónica `decisions/ADR-NNNN-*` + tabla
  de mapeo heredada→canónica.
- `0006-verificacion-adrs-vs-codigo.md` → **reubicado** a
  `docs/architecture/logs/verificacion-adrs-vs-codigo-2026-08-02.md`
  (es un log, no un ADR) y retitulado; su mención al fallback Redis inline
  actualizada (obsoleto tras Fase C).
- Referencias ambiguas corregidas en: `README.md`, `AGENTS.md`,
  `decisions/ADR-0007`, `decisions/ADR-0009`, y notas de estado de
  `0001`/`0002`.

---

## 8. Fase E — Nota de proceso (H6)

- Nota **"atomic commits"** en el forense y en `ADR-0003`: el
  `composition_root.py` se perdió porque el commit `feat(portfolio)` tocó dos
  bounded contexts en un solo commit. Lección: un commit = un cambio lógico
  en un BC.

---

## 9. Verificación §7 final

| Check | Resultado |
|---|---|
| `uv run ruff check .` | All checks passed |
| `uv run ruff format . --check` | 402/402 formatted |
| `uv run lint-imports --config architecture/importlinter.toml` | **44 kept, 0 broken** |
| `uv run pytest tests/ -x -q` | **759 passed** |
| `OCM_VALIDATE_ONLY=true uv run python -m app.cli.main` | validation_complete |
| `uv run mypy .` | 17 errores pre-existentes (6 archivos: paper_bot, redis_stream, research, api, incremental) — **ninguno nuevo** |

El paso previo de Fase A (750 passed, 43 contratos) y el de Fase B
(759 passed, 44 contratos) también quedaron verificados en su momento.

---

## 10. Estado del working tree

Sin commits realizados (por decisión del usuario). Cambios sin commitear:
nuevas implementaciones y enmiendas documentales de las Fases A–E,
eliminación de `live.py`/`paper.py` y de `trading/data/`, contrato BC-50,
tests nuevos, y archivos de informe (`AUDITORIA-...`, este informe).

**WIP previo preservado** (contenido original de `composition_root.py`,
`schema.py`, `pyproject.toml` con `TradingConfig`) integrado en las fases.

---

## 11. Siguientes pasos

- **`assemble_rebalance()`** queda como stub hasta decidir la delegación
  (`RebalanceService` de portfolio vs tracking propio de trading).
  **Decision pending rastreada en `ADR-0011`** + `TODO(ADR-0011)` en el stub.
- **Application bootstrap general** pausado (bar de `ADR-0007`: solo con
  evidencia funcional concreta).
- Los 16 errores mypy pre-existentes (tras eliminar PaperBot, sección 13)
  quedan pendientes de la migración pandas → polars y dependencias.
- Decidir cuándo commitear el working tree (tarea del usuario).

---

## 12. Revisión de coherencia SSOT (2026-08-03, pase posterior)

Revisión puntual del `TradingCompositionRoot` reconstruido contra el SSOT
(ADR-0003 enmendado + forense §2):

- **`TradingRuntime`** — verificado: exactamente `engine, portfolio, tracker`
  (frozen + slots), sin estado oculto. Guardado además por test
  (`test_trading_runtime_exposes_exactly_three_fields`).
- **`build_gold_data_source()`** — único punto real de trading→market_data
  (BC-50). El match de `live_executor.py:99` (`CCXTAdapter`) es un docstring
  de stub, no código ejecutable. Nuevo guard AST
  (`test_market_data_imports_only_in_composition_root`) cubre imports lazy
  que import-linter no ve.
- **`_map_risk_config()`** — mapeo 1:1 verificado campo a campo
  (position/stop_loss/drawdown/order); `signal_filter` no existe en
  AppConfig.risk (gap real) y se resuelve solo vía override CLI
  `min_confidence`, nunca inventado. Guardado por 3 tests nuevos
  (preservación de campos, override, defaults puros).
- **`assemble_live()`** — guard y risk obligatorios (fail-fast), tracker y
  `build_fill_sync` internos, portfolio = instancia inyectada (BC-43).
- **`assemble_paper()`** — fail-soft (risk → defaults, guard opcional),
  `build_fill_sync` cableado, portfolio inyectado.
- **`assemble_rebalance()`** — stub con mensaje y documentación correctos;
  se añadió el tracking que faltaba: **ADR-0011** (decisión pendiente) +
  `TODO(ADR-0011)` en el código.

**Verificación:** ruff limpio · format 402/402 · **44/44 KEPT** ·
**764 passed** (+5 tests) · validate-only OK · mypy 17 pre-existentes.

---

## 13. Integración completa del Composition Root (2026-08-03, cierre)

Eliminación definitiva del camino antiguo — `TradingCompositionRoot` queda
como ÚNICO punto de ensamblado de trading (ADR-0012).

- **`execute_paper.py`:** `strategy_name="ema_crossover"` hardcodeado →
  `args.strategy` (vía `TradingConfig`); comentario stale eliminado.
  `paper_hydra.py` ahora expone `--strategy` (default `"ema_crossover"`,
  espejo de `live_hydra.py`). `execute_live.py` ya usaba `args.strategy`.
- **PaperBot eliminado:** `git rm packages/trading/execution/paper_bot.py`
  `tests/trading/test_paper_bot.py`; re-exports de `PaperBot/PaperOrder/
  RiskConfig` quitados de `trading.execution` (sin consumidores — grep
  completo: README, scripts, docs, notebooks, research). Efecto colateral:
  rompe el ciclo de imports latente `trading.engine ↔ trading.execution`.
- **Factories eliminados:** `TradingEngine.build_live()/build_paper()`
  borrados — el engine es un objeto runtime puro. `assemble_live()/
  assemble_paper()` construyen `Strategy + RiskManager + Executor + OMS`
  inline (relocación 1:1, cero cambio de comportamiento: mismo Strategy,
  RiskManager, Executor, OMS, guard, tracker, `on_fill=build_fill_sync`,
  `on_reject=None`). Fail-fast de live ya vivía en `assemble_live()`.
  **ADR-0012** (aceptado) supersede a **ADR-0005** (queda como histórico).
- **Tests portados:** la cobertura de comportamiento de PaperBot
  (min_confidence, max_open_positions, generación de orden en crossover)
  ahora prueba el sistema real root → `assemble_paper()` → `run_once()`
  en `tests/trading/test_composition_root.py` (+3 tests).
- **Docs:** enmienda ADR-0003, forense, DOMAIN.md, ADR-0007 actualizados;
  `oms.py:242` y `fill_sync.py:72` sin referencias a PaperBot/factories.

**Verificación §7:** ruff limpio · format · **44/44 KEPT** ·
**tests/ -x -q pasando** (67 en trading) · validate-only OK · mypy sin
errores nuevos (16 pre-existentes — desapareció el de `paper_bot.py:103`).

---

*Informe generado el 2026-08-03. Cobertura completa de las Fases A–E y
verificación §7 por fase y final.*
