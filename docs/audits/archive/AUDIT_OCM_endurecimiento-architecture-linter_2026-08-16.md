# Endurecimiento forense del Architecture Governance Linter

- **Fecha**: 2026-08-16
- **Alcance**: `architecture_linter/`, `tests/architecture_linter/`, `architecture/architecture_linter.toml`, `docs/`
- **Entregables previos**: `docs/audits/2026-08-16-architecture-linter.md` (linter), `docs/audits/2026-08-16-auditoria-linter-contra-clean-architecture.md` (auditoría, veredicto B)
- **Reglas**: ARCH-001 … ARCH-010 (10 invariantes)
- **Veredicto final**: **B+** (útil y estructuralmente fundado; limitaciones documentadas impiden A)

---

## 1. Objetivo

Endurecer el linter de gobernanza arquitectónica de OCM para que pase de
**heurísticas de nombres** a **análisis estructural** (AST/símbolos/imports/
grafo/callers), con resultados **reproducibles** y aptos para convertirse en
gate de gobernanza (CI). Reglas de la tarea:

- No reescribir desde cero: inspeccionar primero, concluir después.
- Los hallazgos golden (F1–F4) deben permanecer detectados.
- `precisión > cobertura`: un FP corregido vale más que cobertura artificial.
- `UNKNOWN honesto > falso positivo`: si la evidencia no alcanza, decirlo.
- Todo cambio soportado por evidencia del repo real (probes forenses).
- No modificar código de negocio, ADR-0029, ADR-0030, `docs/plans/tracking.yaml`.
- Sin commits ni pushes.

---

## 2. Fuentes y contexto

- `architecture/importlinter.toml` — SSOT de contratos (4 `layers`: BC-08, BC-30,
  BC-26, BC-44; 46 `forbidden`).
- Golden previo (test + auditoría de la tarea 1) y hallazgos F1–F4.
- Probes forenses sobre el repo real (código, no inferencia):
  `packages/trading/execution/oms.py`, `packages/trading/execution/transport.py`,
  `packages/trading/risk/manager.py`, `packages/trading/analytics/trade_tracker.py`,
  `packages/trading/bootstrap/composition_root.py`,
  `packages/market_data/adapters/inbound/websocket/ws_trades_source.py`,
  `packages/market_data/adapters/inbound/websocket/gap_aware_stream.py`,
  `packages/market_data/adapters/inbound/websocket/infra_metrics_producer.py`,
  `packages/market_data/adapters/inbound/rest/trades_backfill_fetcher.py`,
  `packages/market_data/adapters/inbound/rest/gap_recovery_fetcher.py`,
  `packages/market_data/adapters/inbound/rest/derivatives_fetcher.py`,
  `packages/market_data/ports/outbound/exchange.py`,
  `packages/market_data/application/pipeline/runtime.py`,
  `packages/market_data/ports/outbound/metrics.py`,
  `packages/market_data/adapters/outbound/kafka_gap_publisher.py`,
  `packages/portfolio/infra/memory_store.py`,
  `packages/portfolio/services/portfolio_service.py`.

---

## 3. Estado inicial (antes del endurecimiento)

| Regla | Estado | Detección previa | Problema |
|---|---|---|---|
| ARCH-002 | FAIL | Variables `new_qty`/`remaining` + `.pop(` textual; `WAC_OWNERS`/`REPLACE_OWNERS` por fichero | Heurística de nombres de variable; ruido en attribución por store |
| ARCH-003 | FAIL | Nombres `manage_open_orders`/`fetch_open_orders` en OMS | Solo PASS/FAIL; ignoraba reconciliación puntual (`fetch_state`) |
| ARCH-004 | FAIL | Búsqueda de `fetch_balance`/`BalancePort` | Sin contrapeso estructural (`dict[..., float]` en adapters/ports/infra) |
| ARCH-005 | FAIL | Niveles 1–6 por presencia de métodos | No distinguía "detección/recovery" de "freshness consultable" |
| ARCH-008 | FAIL | Docstring `NOT_IMPLEMENTED` + `raise StopAsyncIteration` | **3 FP** (streams legítimos con terminación normal) y **1 FN** (`InfraMetricsKafkaProducer`) |
| ARCH-009 | FAIL→PASS | Solo BC-08, sin forbidden ni `ignore_imports` ni lazy | Cubría 1/4 contratos de capa; 0/46 forbidden |
| ARCH-010 | FAIL | Stores por nombre | Sin estado global de módulo; sin contenedor por comportamiento |
| CLI | — | Regla inexistente = ignorada | Exit silencioso para `--rules ARCH-999` |

Total: 28 findings, exit 1. Golden: ARCH-001..008 FAIL, ARCH-009 PASS, ARCH-010 FAIL.

---

## 4. Golden findings (F1–F4) — verificados en repo real

| ID | Hallazgo | Evidencia estructural |
|---|---|---|
| F1 | `WSTradesSource` es un stub de producción | `__anext__` termina de inmediato (`raise StopAsyncIteration` sin `await`/`yield`/`return`); `_running` nunca `True`. Sin depender del docstring `NOT_IMPLEMENTED` |
| F2 | Freshness por niveles 3–6 | Existe detección/recovery (1–2) pero no estado consultable, contrato, propagación ni enforcement pre-orden |
| F3 | `MarketDataSourcePort` outbound huérfano | 0 consumidores/implementaciones reales; docstring cita implementaciones inexistentes |
| F4 | Divergencia semántica de posición | OMS acumula/WAC (`_entry_positions`), TradeTracker reemplaza (pyramid), RiskManager espejo reemplaza; SELL hace `pop` incondicional; reducción parcial en OMS |

F4 se conserva con **semántica por método (AST)** en ARCH-002, no por nombres:
WAC = fórmula `Div(Add, Mult)` sobre el store o nombres derivados de él; reemplazo
= escritura sin leer el previo; reducción = `Sub` sobre el store/derivado; pop =
`pop`/`del` incondicional.

---

## 5. Decisión de estado: ARCH-003 → PARTIAL (documentada)

La regla anterior era binaria PASS/FAIL. La tarea §5 exige una rama PARTIAL viva,
analizando callers e implementaciones, no nombres. Evidencia forense en trading:

- `OrderTransport.fetch_state` (`execution/transport.py:118,153`), llamado en
  `OMS._fill`/`submit` (`composition_root.py:246`, `live_executor.py:243`).
- **Ausencia** de `manage_open_orders`/`fetch_open_orders` (loop periódico de
  órdenes abiertas).

Conclusión: existe **reconciliación puntual submit-time** (una orden sin fill
durante el downtime solo se recupera en el siguiente submit del mismo símbolo)
pero **no gestión periódica**. Resultado: `PARTIAL` en el repo real (no FAIL
fabricado, no PASS). El golden se actualizó en consecuencia
(`tests/architecture_linter/test_golden.py`), con comentario de evidencia.

La regla ahora distingue:

- Sin almacenes de órdenes en `trading/execution` → **UNKNOWN** (no auditable).
- Almacenes + gestión periódica → **PASS**.
- Almacenes + reconciliación puntual estructural (caller `fetch_*` que muta el
  almacén, o `fetch_state`/`reconcile`/`rehydrat`/`startup_recovery`) → **PARTIAL**.
- Almacenes + ningún mecanismo → **FAIL**.

La reconciliación puntual se detecta también estructuralmente: métodos del owner
que llaman `.fetch_*` sobre otro objeto mientras mutan el almacén.

---

## 6. Modelo de análisis

Pipeline de 4 niveles, todo stdlib (`ast`, `tomllib`), sin imports de OCM:

1. **Índice AST** (`analyzers/ast_walk.py`): por módulo registra clases (bases,
   decoradores, métodos con nodos AST, `raise_sites`, docstrings/markers),
   atributos de instancia (anotación + mutaciones + lecturas), imports
   (incluidos lazy), stores globales de módulo y markers de docstring de módulo.
2. **Analizadores semánticos**:
   - `analyzers/mutable_state.py`: detección de stores de posición/orden y
     semántica de escritura por método (WAC/accumulate/replace/reduce/pop);
     dataflow-lite de nombres derivados (`_fed_names`, punto fijo transitivo);
     contenedor por anotación, patrón o **forma de valor** (`pair`); stores
     globales de módulo.
   - `analyzers/behavior.py`: evidencia de stub por comportamiento
     (NotImplementedError en público/dunder, stream de parada inmediata,
     estado nunca operativo, cáscara de capacidades no-op; null-objects
     excluidos por convención deliberada).
3. **Reglas** (`rules/arch_*.py`): consumen el índice y los analizadores;
   producen `Finding` con evidencia, símbolos relacionados y confianza.
4. **Engine + CLI**: agregación de estado (FAIL > PARTIAL > PASS/UNKNOWN) y
   superficie de usuario (`--rules`, `--json`, exit codes).

### Dataflow-lite (`_fed_names`)

Distingue "reemplaza sin leer el previo" de "acumula sobre el previo" por el
uso real del valor leído:

```python
prev_qty, prev_avg = self._entry_positions.get(symbol, (0.0, 0.0))  # fed
new_qty = prev_qty + qty                                           # fed (transitivo)
new_avg = (prev_qty * prev_avg + qty * avg_price) / new_qty        # fed
self._entry_positions[symbol] = (new_qty, new_avg)                 # acumula (usa fed)
```

Un método que solo lee el store para loguear/validar y luego escribe no se
clasifica como acumulación (fix del FP que inflaba `TradeTracker`).

---

## 7. Reglas modificadas y nuevas

| Fichero | Cambio |
|---|---|
| `analyzers/ast_walk.py` | `RaiseSite`, `ClassInfo.method_nodes/raise_sites`, `capability_methods`, `is_null_object_name`, `GlobalStore`, `ModuleInfo.global_stores/module_docstring_markers`; anotación de atributos en `__init__` (AnnAssign en métodos); `_track_raise` por método |
| `analyzers/mutable_state.py` | Reescrito: `OwnerSemantics`, `SEM_WAC/ACCUMULATE/REPLACE/REDUCE/POP`, `analyze_owner_semantics`, `_write_uses_prev`, `_fed_names` (punto fijo transitivo), `_has_reduce_shape`, `_is_wac_formula`, `_attr_value_shapes` (`pair`), `has_container_mutation`, `find_global_mutable_stores`; elimina detectores textuales (`detect_wac_semantics`, etc.) |
| `analyzers/behavior.py` | Nuevo: `analyze_stub_class` con triggers estructurales; `_is_noop_body` (docstring/pass/return nulo/logging), `_is_log_call`, `_is_immediate_stop_stream`, null-objects excluidos |
| `rules/arch_002.py` | Reescrito: divergencia 1 y 2 por semántica estructural; dedupe por fichero con precedencia WAC; divergence 2 exige owners distintos (el mismo store que reduce y hace `pop` es SELL coherente); excluye contadores escalares (`has_container_mutation`) y SSOT hints |
| `rules/arch_003.py` | Reescrito: PASS/FAIL/PARTIAL/UNKNOWN; reconciliación puntual estructural por callers |
| `rules/arch_004.py` | Balance estructural: `dict[...]` con `float`/`Decimal` en adapters/ports/infrastructure |
| `rules/arch_005.py` | Mensaje distingue "detección/recovery presente" de "freshness consultable ausente" |
| `rules/arch_007.py` | Comparación de valores de constantes/enums a nivel de clase (`_class_constant_values`) |
| `rules/arch_008.py` | Reescrito: behavior-based, sin doble bucle, dedupe por `(path, clase)` |
| `rules/arch_009.py` | Reescrito: **todos** los contratos `layers` (BC-08/BC-30/BC-26/BC-44) + `forbidden` (46) con `ignore_imports` y lazy imports; excepción `*composition_root` (wiring ADR-0003/0004); fix: si hay violaciones forbidden y `contracts=[]`, se reportan (no UNKNOWN prematuro) |
| `rules/arch_010.py` | Añade estado mutable global de módulo (contenedores mutados a nivel de módulo) |
| `engine.py` | Índice `_dict_returns` + `dict_returns()` para ARCH-004; `_annotation_plain` |
| `cli.py` | Regla inexistente → error explícito en stderr + **exit 2** |

---

## 8. Falsos positivos corregidos

| FP | Causa previa | Fix estructural |
|---|---|---|
| `GapAwareStream`, `GapRecoveryFetcher`, `TradesBackfillFetcher` como stubs | `raise StopAsyncIteration` tratado como señal de stub | `StopAsyncIteration` solo es señal si el método `__anext__`/`__next__` **no tiene** `await`, `yield` ni `return` con valor (stream que se detiene de inmediato sin producir). Los streams legítimos hacen `await` real y ponen `_running = True` en `__aiter__` |
| 5 null-objects (`NullMetrics`, `NullQualityMetrics`, `NullResampleMetrics`, `NullExternalMetrics`, `NoopGapPublisher`) como stubs | "todos los métodos vacíos" = stub | Los null-objects son **no-op intencionales** (patrón Null/Object). Exclusión deliberada por convención `Null*`/`Noop*`, documentada como allowlist gobernada (igual que `SSOT_HINTS`, `ignore_imports`, `composition_root`). El trigger de cáscara ahora exige ≥2 métodos de **capacidad** (públicos, no-dunder) **sin efecto observable** (docstring/`pass`/`return` nulo/solo logging) |
| `exchange.py` (`ExchangeAdapter`), `derivatives_fetcher.py`, `runtime.py` por `NotImplementedError` | (no ocurría en v1) | Prevenido por diseño: `ABC`/`abstractmethod` excluidos; hooks privados (`_fetch_raw`, `_parse`, `_run`) excluidos (trigger solo en público/dunder) |
| `IcebergStorage` (single `write`) | — | La cáscara exige ≥2 métodos de capacidad (un único `write` vacío no es shell) |
| `RiskManager._open_positions` (`int`) como "reemplazo" en ARCH-002 | contador escalar con patrón de nombre | `has_container_mutation`: sin escrituras de contenedor (subíndice/pop/del) → semántica de colección no aplica |
| `OMS.__init__` como "reemplazo" de `_entry_positions` | escritura en `__init__` | Inicialización pura excluida de la clasificación semántica |
| `TradeTracker._open_positions` como "acumula" | cualquier lectura en el método | `_write_uses_prev`: el **valor escrito** debe usar el previo (subíndice directo o nombre derivado) |

**Verificación en repo real**: `ARCH-008` devuelve exactamente
`{WSTradesSource, InfraMetricsKafkaProducer}`; `ARCH-002` solo `{OMS._entry_positions}`
acumula y `{TradeTracker._open_positions, RiskManager._positions}` reemplaza.

---

## 9. Falsos negativos corregidos

| FN | Causa previa | Fix |
|---|---|---|
| `InfraMetricsKafkaProducer` no detectado | `__repr__` no vacío rompía la heurística de "todos vacíos"; docstring NOT_IMPLEMENTED en el **módulo** (no en la clase) | Cáscara por métodos de **capacidad** (`start`/`close`/`produce` no-op); el docstring de módulo solo refuerza |
| Stores con atributo renombrado (`_book`/`_ledger`) | el gate de contenedor ignoraba la evidencia por forma; anotación de `__init__` no capturada | Contenedor por **forma de valor** (`pair`); anotación capturada en AnnAssign de métodos |
| Divergencia semántica cuando las variables WAC se renombran (`delta`/`base`/`moving_qty`) | detección por nombres de variable | Fórmula WAC + dataflow-lite de nombres derivados |
| Violaciones forbidden con `contracts=[]` | UNKNOWN prematuro ocultaba findings | `return UNKNOWN` solo si además no hay findings |
| Estado global de módulo | no se analizaba | `find_global_mutable_stores` + ARCH-010 |

---

## 10. Tests

Suite `tests/architecture_linter/` tras el endurecimiento: **41 tests**.

- `test_golden.py`: statuses del repo real (ARCH-003 → PARTIAL), ARCH-006
  (ports huérfanos incl. `MarketDataSourcePort`), ARCH-007 (9 duplicados +
  exclusión de `CompositionRoot`), ARCH-008 (2 stubs presentes **y** ausencia de
  FPs: streams legítimos y null-objects).
- `test_rules.py`: PASS/FAIL/UNKNOWN/PARTIAL por invariante, incluidos los
  nuevos tests ARCH-003 PARTIAL y UNKNOWN.
- `test_adversarial.py` (12 tests): batería de mutaciones y matriz de estados.

### Batería adversarial

| Test | Mutación / caso | Resultado esperado |
|---|---|---|
| `rename_store_attr_still_fails` | `_entry_positions`→`_book`, `_open_positions`→`_ledger` (semántica intacta) | FAIL (ARCH-002, ARCH-010) |
| `rename_wac_variables_still_fails` | `new_qty`/`new_avg`→`delta`/`base`/`moving_qty` | FAIL (ARCH-002) |
| `rename_classes_still_fails` | `OMS`→`BookingEngine`, `RiskManager`→`RiskLedger` | FAIL (ARCH-001) |
| `single_consistent_owner_passes` | Un solo owner coherente (buy WAC + sell reduce/pop) | PASS (ARCH-002, ARCH-001) |
| `known_fp_patterns_pass` | Stream real, null-object, single-write | PASS (ARCH-008) |
| `partial_reconciliation` | `fetch_state` sin loop periódico | PARTIAL (ARCH-003) |
| `unknown_no_stores` | Sin almacenes de órdenes | UNKNOWN (ARCH-003) |
| `layer_violation_fails` | `domain` importa `infrastructure` (orden real BC-08) | FAIL (ARCH-009) |
| `composition_root_exempt_from_forbidden` | CR importa infra prohibida | PASS (ARCH-009) |
| `forbidden_violation_fails` | `application` (no CR) importa infra prohibida | FAIL (ARCH-009) |
| `global_state_fails` | Contenedor mutado a nivel de módulo | FAIL (ARCH-010) |
| `orphan_port_fails` | Protocol en ports sin consumidores | FAIL (ARCH-006) |

---

## 11. Antes / después

| Métrica | Antes | Después |
|---|---|---|
| Golden statuses | ARCH-003 FAIL, ARCH-008 `{WSTradesSource}` + 3 FP | ARCH-003 PARTIAL, ARCH-008 `{WSTradesSource, InfraMetricsKafkaProducer}` exacto |
| ARCH-002 detección | Variables `new_qty`/`remaining` + `.pop(` textual | Fórmula WAC, dataflow-lite, forma de valor, dedupe por fichero |
| ARCH-009 | 1 layer contract | 4 layer contracts + 46 forbidden + lazy + excepción CR |
| ARCH-010 | Stores por nombre | + estado global de módulo |
| CLI reglas desconocidas | silencioso | exit 2 + stderr |
| Total findings repo real | 28 (exit 1) | 28 (exit 1) — mismas violaciones reales, sin FPs |
| Tests linter | 29 | 41 (incluye 12 adversarial) |

Los 28 findings conservados corresponden a violaciones reales verificadas; la
precisión subió sin perder cobertura.

---

## 12. Limitaciones

1. **Detección de concepto por patrón de nombre aún presente** como *una* vía
   de evidencia (`_positions`, `_open`, `_orders`). No es la única (anotación y
   forma de valor también califican), pero un store renombrado sin anotación ni
   forma de valor (p. ej. `self.book = {}` con writes escalares) no se detecta.
2. **Null-objects por convención de nombre** (`Null*`/`Noop*`): la distinción
   entre no-op intencional y stub accidental es semántica e indistinguible por
   AST puro. Se gobierna como allowlist documentada, no como heurística de
   violación.
3. **Dataflow-lite, no taint completo**: la alimentación transitiva de nombres
   cubre asignaciones locales, no flujos inter-procedurales ni atributos
   derivados complejos. Un `self._x[k] = prev[k][0]` (subíndice anidado) no se
   marca como lectura (caso de borde conocido).
4. **Streams juzgados por estructura**: "stream real" = `__anext__` con
   `await`/`yield`/`return` real. Un stub sofisticado que simule un await no
   sería detectado (limitación asumida; mejor que el FP previo).
5. **`dict_returns` de ARCH-004** detecta la *forma* de los balances, no que
   estén conectados a un exchange real: el balance estructural solo es
   contrapeso, nunca PASS.
6. **Módulos `architecture_linter/` y `tests/` fuera del análisis** por diseño
   (roots configurados).

---

## 13. Matriz Clean Architecture (criterios de auditoría previa)

| Criterio | Veredicto tras endurecimiento |
|---|---|
| Detección estructural (no por nombres) | ✓ ARCH-002/003/008/009/010; parcial en 001/006/007 (clasificación por forma/consumidores/duplicados) |
| UNKNOWN honesto | ✓ ARCH-003 (sin stores), ARCH-009 (sin config) |
| Sin FPs conocidos | ✓ golden + adversarial + repo real (ARCH-008 exacto) |
| Reproducible (mismo root → mismo resultado) | ✓ determinista (índice AST + config) |
| Evidencia trazable por hallazgo | ✓ `Evidence` con path/línea/operación; `related_files/symbols` |
| Gobernanza de excepciones explícita | ✓ `composition_root`, `SSOT_HINTS`, null-objects, `ignore_imports` — todas documentadas |
| No invade el dominio | ✓ linter es stdlib-only; 0 imports de OCM |
| Listo para gate en CI | ✓ exit codes 0/1/2, `--json` (exit 1 con FAIL/PARTIAL), tests del linter en suite |

---

## 14. Gates (2026-08-16, tras el endurecimiento)

| Gate | Resultado |
|---|---|
| `uv run ruff check .` | 0 errores |
| `uv run ruff format . --check` | 492 archivos formateados |
| `uv run mypy .` | 0 issues (382 fuentes) |
| `uv run bandit -r apps ocm packages shared infrastructure` | 51 Low (High confidence), preexistentes; 0 Medium/High severity |
| `uv run lint-imports --config architecture/importlinter.toml` | 50 kept, 0 broken |
| `uv run pytest tests/ -q` | 1150 passed; 4 failed (integración Kafka: broker en `localhost:9093` inalcanzable — ambiental, no relacionado) |
| `uv run pytest tests/architecture_linter/ -q` | 41 passed |
| `uv run python -m architecture_linter --root .` | 28 findings, exit 1 (golden: 8 FAIL, 1 PARTIAL, 1 PASS) |
| `uv run python -m architecture_linter --root . --json` | exit 1 (hay FAIL/PARTIAL) |
| `uv run python -m architecture_linter --root . --rules ARCH-999` | error stderr + exit 2 |
| `uv run mypy architecture_linter/` | 0 issues (23 fuentes) |

Nota: los 4 fallos de `tests/kafka/test_integration_kafka.py` son de entorno
(conexión al broker) y preexistentes; ninguno toca `architecture_linter/`.

---

## 15. Estado Git

- Cambios: solo `architecture_linter/`, `tests/architecture_linter/`,
  `architecture/architecture_linter.toml`, `docs/` (untracked de la tarea 1 +
  este informe). `AGENTS.md` ya estaba modificado por la tarea previa.
- **No modificados**: código de negocio (`packages/`, `shared/`, `apps/`, `ocm/`,
  `config/`), ADR-0029, ADR-0030, `docs/plans/tracking.yaml`.
- **Sin commits ni pushes.**

---

## 16. Autoauditoría

- Sin placeholders: cada sección cita código o salida verificada en esta sesión.
- Golden verificados contra el repo real en el mismo commit de código linter.
- Adversarial: mutaciones A–F + matriz; prueba real que el renombrado no cambia
  el veredicto y que los patrones inocentes no producen FPs.
- Ningún claim sin evidencia: los conteos (28 findings, 4/46 contratos, 2 stubs,
  8 ports huérfanos, 9 duplicados) provienen de la ejecución real del linter.
- Sin código de negocio tocado; sin commit/push.
- Límites declarados (sección 12) sin forzar el veredicto hacia A.

---

## 17. Veredicto final

**B+** — Útil, estructuralmente fundado y listo para governance gate.

El linter pasó de heurísticas de nombres a un modelo de análisis por AST con
dataflow-lite, semántica de escritura por método, contenedor por comportamiento,
reconciliación puntual (PARTIAL honesto) y reproducción de los 50 contratos
architectónicos. Los FPs conocidos se corrigieron (3 streams legítimos, 5
null-objects) y los FN cerrados (`InfraMetricsKafkaProducer`, stores renombrados,
estado global). No alcanza **A** porque la distinción no-op intencional vs stub
y la detección de concepto conservan convenciones gobernadas (nombre de
null-objects, patrones de atributo como *una* vía de evidencia) y el dataflow es
lite, no taint completo — limitaciones declaradas, no ocultas.