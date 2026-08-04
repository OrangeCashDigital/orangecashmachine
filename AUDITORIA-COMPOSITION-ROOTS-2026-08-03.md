# Auditoría de Composition Roots y deuda de integración — OrangeCashMachine

**Fecha:** 2026-08-03
**Alcance:** Entrypoints de `apps/` (`cli/main.py`, `cli/entrypoint.py`, `cli/live*.py`, `cli/paper*.py`, `use_cases/execute_*`), Composition Roots de `market_data`, `trading` y `portfolio`, y documentación arquitectónica (ADRs, forense, GOVERNANCE).
**Método:** Revisión manual de código fuente + verificación cruzada contra ADRs, forense de bytecode y contratos de import-linter; verificación en runtime de un defecto crítico (confirmado por ejecución).

---

## 1. Resumen ejecutivo

La auditoría confirma **6 hallazgos**, de los cuales **2 son críticos**:

| # | Hallazgo | Severidad | Estado |
|---|----------|-----------|--------|
| H1 | `uv run ocm` (comando principal de market data) **falla en runtime**: el runner inyecta un `PipelineOrchestrator` sin factory | 🔴 Crítico | **Roto hoy** — verificado por ejecución |
| H2 | `execute_live.py`/`execute_paper.py` no usan `TradingCompositionRoot` (integración sin resolver) | 🟡 Moderado | Confirmado |
| H3 | El `TradingCompositionRoot` actual en disco **contradice el diseño aprobado** en `ADR-0003` + forense (interfaz recuperada por bytecode) | 🔴 Crítico (deuda de diseño) | Confirmado |
| H4 | `live.py`/`paper.py` son legado sin consumidores; pendiente `git rm` (aprobado en ADR-0005) | 🟢 Leve | Confirmado |
| H5 | Dos secuencias de ADR colisionan en numeración (`0000-0006` vs `decisions/ADR-0003..0010`) | 🟡 Moderado (deuda documental) | Confirmado, cuantificado |
| H6 | Commit `bc21c52` mezcla dos bounded contexts (`feat(portfolio)` tocó `trading/bootstrap/`) | 🟢 Leve (proceso) | Confirmado |

Además, durante la verificación se detectaron **3 hallazgos nuevos** (H7, H8, H9) que condicionan la implementación, y se confirmó que el punto "inconsistencia de profundidad de carpetas" **ya está resuelto** por `ADR-0007` (no es trabajo pendiente).

---

## 2. Estado real del árbol de trabajo

El árbol trae **trabajo sin commitear** que corresponde al inicio de esta remediación:

| Archivo | Estado | Qué contiene |
|---|---|---|
| `apps/app/cli/entrypoint.py` | **Nuevo, sin trackear** | Runner por defecto para `cli/main.py` (reemplaza la instanciación directa de `OHLCVPipeline`) |
| `apps/app/cli/main.py` | Modificado | `_default_pipeline_runner` pasa a ser `entrypoint.run` |
| `packages/trading/bootstrap/composition_root.py` | Modificado (v2) | Constructor angosto sobre `TradingConfig` + `from_app_config()` + `assemble_live/assemble_paper` |
| `ocm/config/schema.py` | Modificado | Agrega `TradingConfig` y `AppConfig.trading` |
| `pyproject.toml` | Staged | Reorganización documental de dependencias + `license` PEP 639 |
| `uv.lock` | Modificado | Resolución de dependencias acorde a `pyproject.toml` |
| `pyproject.toml.bak.20260803_163334` | Sin trackear | Clutter — se elimina en Fase C |

> ⚠️ El WIP del `TradingCompositionRoot` (v2) **no cumple el diseño aprobado** (ver H3). Conectarse a él acoplaría a una interfaz no-SSOT.

---

## 3. Hallazgos en detalle

### H1 — `uv run ocm` roto: `PipelineOrchestrator` sin factory 🔴

**Archivos:** `apps/app/cli/entrypoint.py`, `packages/market_data/main.py`, `packages/market_data/application/use_cases/pipeline_orchestrator.py`

`entrypoint.py:77` construye `orchestrator = PipelineOrchestrator()` **sin inyectar factory**, y su comentario (líneas 73-76) afirma falsamente que el orquestador "construye su propio `ConcretePipelineFactory` internamente". La realidad:

- `PipelineOrchestrator.__init__` recibe `factory=None` y `run()` **lanza** si no hay factory (`pipeline_orchestrator.py:242-245`).
- Verificación en runtime (2026-08-03):

  ```
  OUTCOME: PipelineBuildError : PipelineOrchestrator: se requiere factory — inyectar via constructor o parámetro run()
  ```

- Como `cli/main.py` (WIP) ya apunta `_default_pipeline_runner = entrypoint.run`, **`uv run ocm` falla en el camino principal** hoy.
- `packages/market_data/main.py:146` (`_ingestion_loop`) tiene **el mismo defecto latente**: el servicio `market-data-service` fallaría en cada corrida.

**Corrección respecto a la afirmación inicial:** el problema no es "instanciar `ConcretePipelineFactory` directamente" (eso no ocurre); es construir el orquestador **sin factory**. La prescripción es la misma y correcta: usar `CompositionRoot.assemble(config).factory` (punto único de ensamblado, BC-38).

---

### H2 — `execute_live.py`/`execute_paper.py` no usan `TradingCompositionRoot` 🟡

**Archivos:** `apps/app/use_cases/execute_live.py`, `apps/app/use_cases/execute_paper.py`, `packages/trading/engine.py`, `packages/trading/bootstrap/composition_root.py`

- `execute_live.py:218` llama a `TradingEngine.build_live(...)`.
- `execute_paper.py:224` llama a `TradingEngine.build_paper(...)` (con `strategy_name="ema_crossover"` **hardcodeado** en línea 225).
- `TradingEngine.build_live`/`build_paper` (`engine.py:211,282`) **siguen instanciando adaptadores concretos inline** (`LiveExecutor`, `PaperExecutor`, `OMS`, `RiskManager`) dentro de la capa de aplicación — exactamente la violación DIP que el `TradingCompositionRoot` fue creado a resolver.
- `TradingCompositionRoot` tiene **cero consumidores** en todo el árbol (verificado por `grep`).

**Consecuencia:** duplicación de lógica de ensamblaje en dos lugares (`engine.py` y `composition_root.py`) y deuda de integración: existe el root pero nadie lo usa.

---

### H3 — `TradingCompositionRoot` en disco contradice el diseño aprobado 🔴

**Archivos:** `packages/trading/bootstrap/composition_root.py` (v2 en working tree), `docs/architecture/decisions/ADR-0003-trading-composition-root-narrow-constructor.md`, `docs/architecture/recovered/trading-bootstrap-forensic-analysis.md`

**Diseño SSOT aprobado** (ADR-0003 + forense §2, recuperado por introspección de bytecode porque el original nunca se commiteó):

| Aspecto | Diseño aprobado | v2 en disco |
|---|---|---|
| Constructor | `__init__(trading, risk, redis)` — tres sub-configs | `__init__(config: TradingConfig)` — un solo config |
| `AppConfig` completo | **Rechazado explícitamente** (ADR-0003) | `from_app_config()` lo acepta |
| Métodos públicos | `assemble_live()`, `assemble_paper(data_source)`, `assemble_rebalance(*, use_redis)` → `TradingRuntime` | `assemble_live()`, `assemble_paper()` → `TradingEngine` |
| Retorno | `TradingRuntime` (dataclass: `engine, portfolio, tracker`) | `TradingEngine` |
| Deps internas | Delega en `TradingEngine.build_live/paper` (DRY) | Las construye inline (Strategy, RiskManager, Executor, OMS) |
| `assemble_rebalance` | Existe | **No existe** |
| `TradingRuntime` / `RedisFactory` | Existían | **No existen** (0 hits en el árbol) |

**Consecuencia:** cualquier código que se conecte a la interfaz v2 quedaría acoplado a un diseño que no es el SSOT documentado, perpetuando el mismo problema que la auditoría busca resolver.

---

### H4 — `live.py`/`paper.py` son legado sin consumidores 🟢

**Archivos:** `apps/app/cli/live.py`, `apps/app/cli/paper.py`, `pyproject.toml`, `run.sh`

- `pyproject.toml:190-191` registra los scripts `live`/`paper` → `app.cli.live_hydra`/`app.cli.paper_hydra`.
- `run.sh:34-35` invoca `python -m app.cli.live_hydra`/`app.cli.paper_hydra`.
- **Cero consumidores** de `app.cli.live`/`app.cli.paper` en código (solo referencias documentales: README, ADR-0005, auditorías).
- `ADR-0005` (Aceptado, 2026-08-02) autoriza la eliminación una vez confirmado que ningún consumidor externo los invoca.

**Consecuencia:** se eliminan en Fase C; de paso se elimina el parámetro opcional `portfolio_service=None` (candidato declarado por el propio ADR-0005) y los comentarios stale que referencian los módulos legados.

---

### H5 — Colisión de numeración de ADRs 🟡

**Archivos:** `docs/architecture/0000-0006.md` (serie general) y `docs/architecture/decisions/ADR-0003..ADR-0010` (serie de remediación trading/portfolio)

Dos secuencias comparten números con temas **distintos**:

| Serie `0000-NNNN` (docs/architecture/) | Serie `ADR-NNNN-*` (docs/architecture/decisions/) |
|---|---|
| `0003-composition-root-jerarquico` | `ADR-0003-trading-composition-root-narrow-constructor` |
| `0004-rebalance-service-capacidad-adelantada` | `ADR-0004-bc47-market-data-import-boundary` |
| `0005-hydra-reemplaza-cli-legado` | `ADR-0005-trading-engine-internal-external-split` |
| `0006-verificacion-adrs-vs-codigo` | `ADR-0006-portfolio-owns-position-state` |

- **80+ referencias `ADR-000X` ambiguas** en la documentación.
- Ejemplo concreto: `0005-hydra-reemplaza-cli-legado.md` usa "ADR-0003" 10 veces refiriéndose a la serie raíz, pero `decisions/ADR-0003` es otro ADR de tema distinto.
- `GOVERNANCE.md §3` ya declara la serie canónica (`decisions/ADR-NNNN-*`); la serie `0000-0006` es la heredada inconsistente.
- `0006-verificacion-adrs-vs-codigo.md` se autotitula "ADR-0006" pero es un **log de verificación**, no un ADR (colisiona además con `decisions/ADR-0006`).

**Consecuencia:** problema de SSOT en la propia documentación; se resuelve en Fase D (canónica + tabla de mapeo + deprecación, sin renombres masivos).

---

### H6 — Commit `bc21c52` mezcla bounded contexts 🟢

**Archivo:** `packages/trading/bootstrap/composition_root.py` (historial)

`bc21c52` (`feat(portfolio): introduce composition root bootstrap layer`) tocó `trading/bootstrap/`, es decir **dos bounded contexts en un solo commit**. Contradice el principio de atomic commits de `AGENTS.md` ("one logical change per commit"). Se documenta como lección de proceso en Fase E.

---

### Hallazgos nuevos (H7, H8, H9)

**H7 — `build_fill_sync` resuelve el bloqueante `CompositeFillObserver` (CERRADO).**
El forense listaba `CompositeFillObserver` como "no localizado — bloqueante" (sesión de bytecode). En el árbol actual ese símbolo existe como `trading/execution/fill_sync.py:50` (`build_fill_sync(tracker, portfolio)`), aplicado según `ADR-0006` (ver `0006-verificacion`: "P2 SSOT: on_fill_composite corregido vía build_fill_sync()"). **Hallazgo cerrado**: la reconstrucción del root lo usa en vez de crear una clase nueva.

**H8 — Colisión del número de contrato `BC-47`.**
`ADR-0004` propone formalizar la frontera trading→market_data como "BC-47", pero **BC-47 ya está tomado** en `architecture/importlinter.toml:559` (`shared.kafka does not import domain`). Si se formaliza, debe usarse un número nuevo (propuesto: **BC-50**).

**H9 — Tensión entre el diseño forense y la Fase 3.**
El diseño recuperado construye `PositionStore`/`PortfolioService` internamente (`_build_position_store_*`, `_build_portfolio`). Pero `ADR-0006` + `BC-43` + Fase 3 establecen que **solo `PortfolioCompositionRoot` instancia stores y es dueño del Redis**. Reconstruir fielmente el bytecode violaría `BC-43`. Además hay dos adapters del mismo rol: `trading/data/gold_adapter.GoldLoaderAdapter` (usado por `execute_*` hoy) vs `market_data/adapters/outbound/storage/gold_reader.GoldReader` (diseño aprobado). Resuelto por decisión: **portfolio inyectado** + **GoldReader de market_data** (ver §5).

### Confirmación de un falso pendiente

**`ADR-0007` ya formaliza** que `market_data`/`portfolio` no necesitan renombrar sus carpetas para ser uniformes con `trading` (la "inconsistencia de profundidad de carpetas" marcada como pendiente está **resuelta y documentada**). No es trabajo pendiente.

---

## 4. Referencias de código (file:line)

| Referencia | Ubicación |
|---|---|
| `PipelineOrchestrator()` sin factory | `apps/app/cli/entrypoint.py:77` |
| Comentario falso ("construye su propio factory") | `apps/app/cli/entrypoint.py:73-76` |
| Runner default → `entrypoint.run` | `apps/app/cli/main.py:77,135,186` |
| Mismo bug en el servicio | `packages/market_data/main.py:146` |
| Error sin factory | `packages/market_data/application/use_cases/pipeline_orchestrator.py:242-245` |
| `CompositionRoot.assemble()` (punto único) | `packages/market_data/infrastructure/bootstrap/composition_root.py:111-138` |
| `TradingEngine.build_live/build_paper` (inline, DIP roto) | `packages/trading/engine.py:211,282` |
| v2 del root (no-SSOT) | `packages/trading/bootstrap/composition_root.py:61,76,91,122` |
| `execute_live` → `build_live()` | `apps/app/use_cases/execute_live.py:218` |
| `execute_paper` → `build_paper()` (+ `ema_crossover` hardcodeado) | `apps/app/use_cases/execute_paper.py:224-225` |
| `TradingConfig` / `RiskConfig` / `RedisConfig` | `ocm/config/schema.py:540,711,496` |
| `AppConfig.trading` | `ocm/config/schema.py:761` |
| `build_fill_sync` (reemplazo de `CompositeFillObserver`) | `packages/trading/execution/fill_sync.py:50` |
| Contrato BC-47 (ocupado) | `architecture/importlinter.toml:559` |
| Contrato BC-38 | `architecture/importlinter.toml:85` |
| Scripts `live`/`paper` → Hydra | `pyproject.toml:190-191` |
| `run.sh` modos Hydra | `run.sh:34-35` |
| Forense — interfaz recuperada | `docs/architecture/recovered/trading-bootstrap-forensic-analysis.md:§2` |
| ADR constructor angosto | `docs/architecture/decisions/ADR-0003-...md:23` |
| ADR frontera market_data | `docs/architecture/decisions/ADR-0004-...md:22-34` |
| ADR eliminación de CLIs | `docs/architecture/0005-hydra-reemplaza-cli-legado.md:68-83` |
| ADR dueño de estado de posiciones | `docs/architecture/decisions/ADR-0006-...md:23-38` |
| ADR equivalencia de capas | `docs/architecture/decisions/ADR-0007-...md` |
| Gobernanza ADRs (serie canónica) | `docs/architecture/GOVERNANCE.md:§3` |

---

## 5. Decisiones tomadas

| # | Decisión | Resultado |
|---|---|---|
| D1 | Gold source | **Fiel a ADR-0004**: `TradingCompositionRoot` importa `GoldReader` de `market_data` (único import permitido por BC-50); el resto de `trading` consume `FeatureReaderPort`. Se reemplaza `trading/data/gold_adapter.py`. |
| D2 | `TradingRuntime` / portfolio | **Portfolio inyectado** desde `PortfolioCompositionRoot` (honra BC-43/ADR-0006). `RedisFactory` queda **obsoleto** (documentar). `assemble_live/paper` reciben `portfolio` del constructor. |
| D3 | `assemble_rebalance()` | **Stub con `NotImplementedError` documentado** + pendiente ADR (delegación vs tracking propio, sin resolver aún). Coherente con la premisa de que el tracking real de posiciones vive en `portfolio`, no en `trading`. |
| D4 | Población de `TradingConfig` | Estático en `config/trading/trading.yaml` (Hydra L1-L5, consistente con `portfolio.yaml`) + `strategy_cfg` (symbol/timeframe/fast/slow) sobreescrito en el CLI desde flags. |
| D5 | Numeración de ADRs | **Documentar + mapear** (serie canónica en GOVERNANCE + tabla de equivalencia + deprecación de `0000-0006`), sin renombres masivos de archivos. |

---

## 6. Plan de implementación

> **Orden:** Fases A → B → C → D → E. El forense (§7) exige estabilizar `market_data` antes de reconstruir `trading`; la Fase A cubre esa estabilización en esta misma sesión.

### Fase A — H1: arreglar `uv run ocm` (crítico)

1. **A1.** `apps/app/cli/entrypoint.py::_run_all()`: reemplazar `PipelineOrchestrator()` por el punto de ensamblado único:
   ```python
   root = CompositionRoot.assemble(app_cfg)          # import lazy
   orchestrator = PipelineOrchestrator(factory=root.factory)
   ```
   Corregir el comentario falso (73-76).
2. **A2.** `packages/market_data/main.py::_ingestion_loop`: mismo fix (`PipelineOrchestrator(factory=CompositionRoot.assemble(ctx.app_config).factory)`).
3. **A3.** Tests herméticos de `entrypoint.run`: config sin exchanges → retorna `1`; monkeypatch de `CompositionRoot.assemble` → retorna `0` y verifica que se llama con el `AppConfig` correcto.
4. **A4.** Verificación: `ruff` · `lint-imports` · `pytest` · validate-only (ver §7).

### Fase B — H2/H3: reconciliar `TradingCompositionRoot` contra ADR-0003

1. **B1.** `TradingConfig` ya existe en WIP (`schema.py:540`). **Enmienda a `ADR-0003`** documentando el constructor real: `__init__(trading, risk, portfolio, guard=None)` — se elimina `redis` (Fase 3 lo posee) y se agrega `portfolio` inyectado (decisión D2).
2. **B2.** **Enmienda a `ADR-0004`**: la frontera trading→market_data se formaliza como **BC-50** (BC-47 ocupado): `forbidden: trading.* → market_data` excepto `trading.bootstrap.composition_root`. `RedisFactory` NO se recrea (obsoleto).
3. **B3.** Crear `TradingRuntime` (dataclass: `engine, portfolio, tracker`).
4. **B4.** Reescribir `TradingCompositionRoot` con la interfaz aprobada:
   - `assemble_live() -> TradingRuntime` — `build_gold_data_source()` (GoldReader), `_build_guard()`, tracker, `TradingEngine.build_live` (deps internas), `on_fill=build_fill_sync(tracker, portfolio)`.
   - `assemble_paper(data_source) -> TradingRuntime` — `data_source` del caller (sintético en dry-run, real si no), `TradingEngine.build_paper`.
   - `assemble_rebalance(*, use_redis)` → stub `NotImplementedError` documentado.
   - Implementar el mapeo `AppConfig.risk → trading.domain RiskConfig` (pendiente declarado en el v2).
5. **B5.** Conectar `execute_live.py`/`execute_paper.py` a `assemble_live()/assemble_paper()`; `live_hydra.py`/`paper_hydra.py` construyen `TradingConfig`/`RiskConfig`/`portfolio` desde `AppConfig` + flags CLI.
6. **B6.** Retirar `trading/data/gold_adapter.py` (consumido solo por `execute_*`); deprecar `TradingEngine.build_live/build_paper` tras verificar consumidores (`paper_bot.py`, `fill_sync.py`).

### Fase C — H4: eliminar legados

1. **C1.** `git rm apps/app/cli/live.py apps/app/cli/paper.py`.
2. **C2.** Hacer `portfolio_service` obligatorio en `execute_*`; eliminar fallbacks inline y comentarios stale.
3. **C3.** Actualizar `README.md` (nota "coexisten por compatibilidad") y `0006-verificacion`.
4. **C4.** Borrar `pyproject.toml.bak.20260803_163334`.

### Fase D — H5: resolver colisión de ADRs

1. **D1.** GOVERNANCE: serie canónica `decisions/ADR-NNNN-*`; marcar `docs/architecture/0000-0006` como **heredada** con tabla de mapeo; reubicar `0006-verificacion-adrs-vs-codigo.md` como log (no es ADR); corregir referencias ambiguas.

### Fase E — H6: nota de proceso

1. **E1.** Nota breve en el forense/ADR-0003: `feat(portfolio)` tocó `trading/bootstrap/` (dos BCs en un commit) como lección de atomic commits.

---

## 7. Verificación (por fase y final)

```bash
uv run ruff check .
uv run lint-imports --config architecture/importlinter.toml
uv run pytest tests/ -x -q
OCM_VALIDATE_ONLY=true uv run python -m app.cli.main
```

Reglas vigentes: **un contrato de import-linter BROKEN = merge bloqueado** (fail-fast). No se commitea nada salvo pedido explícito.

---

## 8. Caveats y precondiciones

- El árbol preserva el WIP sin commitear; solo se toca lo que cada fase requiere.
- `ADR-0003` justificó el constructor angosto por "CLIs puros sin Hydra", premisa **obsoleta** tras `ADR-0005` (los callers ya tienen `AppConfig`). El constructor angosto se mantiene por SSOT aprobado; los CLIs Hydra construyen los sub-configs desde `AppConfig`.
- `assemble_rebalance()` queda como stub hasta resolver la decisión de delegación (`RebalanceService` de portfolio vs. tracking propio de trading).
- El "Application bootstrap general" queda **pausado** hasta cerrar lo anterior, con el bar de `ADR-0007`: solo se aborda con evidencia funcional concreta, no por preferencia estética.

---

*Informe generado el 2026-08-03. Basado en revisión manual de código fuente, verificación en runtime y cruce contra documentación arquitectónica (ADRs, forense de bytecode, GOVERNANCE).*

**Reconciliación 2026-08-03 (sesión posterior):** confirmado el cierre de `CompositeFillObserver` (H7 → CERRADO), confirmada la decisión de `assemble_rebalance()` como stub (D3) y verificado que `TradingRuntime` incluye `portfolio` (`engine, portfolio, tracker`).

**Reconciliación 2026-08-03 (Fase A completada + Fase B ejecutada):**

- **Fase A verificada §7:** `lint-imports` 43/43 KEPT · `pytest` 750 passed · validate-only OK · `ruff` limpio salvo el error `I001` pre-existente en el v2 de `composition_root.py` (archivo descartado en Fase B, el error desaparece con el rewrite).
- **Guardrails confirmados por decisión (G1/G2/G3):**
  - **G1 (H9):** el forense predata ADR-0006/BC-43. **NO** se reconstruyen `_build_position_store_*`/`_build_portfolio`/`RedisFactory` — violaría BC-43. El root **inyecta** `portfolio` ya ensamblado por `PortfolioCompositionRoot.assemble()` (D2). El §7 del forense fue corregido en consecuencia (paso 2 eliminado).
  - **G2 (H3):** la v2 del root (`__init__(config: TradingConfig)`) quedó **descartada por completo**; no es base a corregir. Se reescribió desde cero con `__init__(trading, risk, portfolio, guard=None)` (ADR-0003 enmendado).
  - **G3 (H5):** la colisión de numeración de ADRs **queda en Fase D** como estaba planeada — no es bloqueante de lo funcional.
- **Fase B ejecutada:** `TradingRuntime` creado (B3); `TradingCompositionRoot` reescrito con la interfaz aprobada y `build_fill_sync(tracker, portfolio)` (B4); `execute_live.py`/`execute_paper.py` conectados a `assemble_live()/assemble_paper()` (B5); `trading/data/gold_adapter.py` retirado + `build_live/build_paper` deprecados (B6); enmiendas ADR-0003/ADR-0004 y contrato **BC-50** formalizado (B1/B2/BC-50). Tests de cobertura agregados en `tests/trading/test_composition_root.py`.
- **Fase B verificada §7 (2026-08-03):** `ruff check` limpio + `ruff format` 404/404 · `lint-imports` **44/44 KEPT, 0 BROKEN** (BC-50 formalizado, sin `ignore_imports` — import-linter no analiza imports lazy, el import de GoldReader del root no genera arista) · `pytest` **759 passed** (+9 tests de composition root) · validate-only OK · `mypy` sin errores nuevos (17 restantes pre-existentes: paper_bot, redis_stream, research, api, incremental). Ajuste de tipado: `SupportsPositionSync.close_position` → `Any` (build_fill_sync descarta el retorno; PortfolioService devuelve `Optional[PositionSnapshot]` sin acoplar trading→portfolio).

**Reconciliación 2026-08-03 (Fases C/D/E ejecutadas + verificación §7 final):**

- **Fase C ejecutada:** `git rm` de `apps/app/cli/live.py` y `paper.py` (C1) · `portfolio_service` **obligatorio** en `execute_live`/`execute_paper` — eliminados los fallbacks inline (`RedisPositionStore`/`build_redis_client` y `InMemoryPositionStore`), `redis_client` de `LiveEngineResources` queda `None` (SSOT: la conexión Redis pertenece al `PortfolioCompositionRoot` del caller) (C2) · `README.md` actualizado (C3) · borrado `pyproject.toml.bak.20260803_163334` (C4).
- **Fase D ejecutada (D5, sin renombres masivos):** banner **"SERIE HEREDADA"** en `0000-0005*.md` · **GOVERNANCE.md §9** nuevo con tabla de mapeo heredada→canónica · `0006-verificacion` **reubicado** a `docs/architecture/logs/verificacion-adrs-vs-codigo-2026-08-02.md` (es log, no ADR) · referencias ambiguas corregidas en README, AGENTS.md, ADR-0007, ADR-0009 y notas de 0001/0002.
- **Fase E ejecutada:** nota **atomic commits** en el forense y en ADR-0003 (H6).
- **Verificación §7 final:** `ruff check` limpio · `ruff format` 402/402 · `lint-imports` **44/44 KEPT, 0 BROKEN** · `pytest` **759 passed** · validate-only OK · `mypy` 17 errores pre-existentes (6 archivos), **ninguno nuevo**.
- **Informe global:** `INFORME-COMPOSITION-ROOTS-2026-08-03.md` (raíz del repo).

**Reconciliación cierre (2026-08-03) — integración completa del Composition Root:**

- **B6 resuelto al extremo:** en lugar de "deprecar", los factories
  `TradingEngine.build_live()/build_paper()` fueron **eliminados**
  (ADR-0012 supersede ADR-0005). Evidencia de consumidores: grep completo
  (`PaperBot`/`build_paper(`/`paper_bot`) en todo el repo — README,
  scripts, docs, notebooks, research — sin consumidores en runtime fuera
  del root.
- **PaperBot eliminado:** `paper_bot.py` + `test_paper_bot.py` +
  re-exports en `trading.execution`. Su cobertura de comportamiento
  (min_confidence, max_open_positions, crossover) se portó a
  `tests/trading/test_composition_root.py` probando el sistema real
  (root → `assemble_paper()` → `run_once()`), +3 tests.
- **`--strategy` en paper:** `execute_paper.py` usa `args.strategy`
  (antes `"ema_crossover"` hardcodeado); `paper_hydra.py` expone el flag
  (espejo de `live_hydra.py`).
- **Docs actualizados:** ADR-0012 (nuevo, supersede ADR-0005), ADR-0005
  (estado Reemplazado, histórico), enmienda ADR-0003, forense, DOMAIN.md,
  ADR-0007 (consecuencia bootstrap completada), INFORME §13.
- **Verificación §7:** ruff · format · `lint-imports` **44/44 KEPT** ·
  `pytest` completo pasando (67 en trading) · validate-only OK · `mypy`
  **16** errores pre-existentes (bajó de 17: desapareció `paper_bot.py:103`),
  ninguno nuevo.
