# Auditoría del Architecture Governance Linter contra Clean Architecture

**Fecha:** 2026-08-16
**Alcance:** `architecture_linter/`, `tests/architecture_linter/`, repositorio OCM real, `docs/Clean Architecture A Craftsman Guide to Software Structure and Design.pdf`.
**Método:** inspección de código → lectura del PDF → verificación forense en OCM → ejecución de reglas → pruebas adversariales en `/tmp` → gates → falsación → conclusiones (en ese orden, según el encargo).
**Regla de evidencia:** toda afirmación sustancial cita `archivo:línea`, símbolo o salida de comando. Lo que no pudo demostrarse por inspección o ejecución se declara `UNKNOWN`.

---

## 1. PDF localizado y utilizado

- Fichero: `docs/Clean Architecture A Craftsman Guide to Software Structure and Design.pdf` (8.414.896 bytes, 8.4 MB).
- Extracción de texto: `pdftotext -layout` → `/tmp/opencode/adv/ca_full.txt` (11.278 líneas, 538 KB). Las citas del PDF usan capítulo + línea del texto extraído (la extracción no conserva números de página impresos; se cita el texto literal).

### Capítulos leídos (citados con texto extraído)

| Capítulo | Tema | Líneas en `ca_full.txt` |
|---|---|---|
| Ch. 6 | Functional Programming: segregación de mutabilidad (TOC `ca_front.txt:106`) | — |
| Ch. 14 | Component Coupling: SDP/SAP, métricas de estabilidad | 3560–3599 |
| Ch. 17 | Boundaries: Drawing Lines (plugin architecture, firewalls, eje de cambio) | 4965–5002 |
| Ch. 19 | Policy and Level (nivel = distancia a I/O) | 5223–5279 |
| Ch. 20 | Business Rules (Entities, Use Cases) | 5653–5679 |
| Ch. 21 | Screaming Architecture + Testable Architectures | 5564–5608 |
| Ch. 22 | The Clean Architecture: Dependency Rule, cruce de fronteras, DIP, datos | 5641–5784 |
| Ch. 26 | The Main Component (ultimate detail) | 6251–6270 |
| Ch. 28 | The Test Boundary (tests como componentes, testing API, structural coupling) | 6700–6789 |
| Ch. 32 | Frameworks Are Details (no casarse con frameworks, proxies/plugins) | 7673–7790 |

No se asumió contenido del PDF: cada principio se cita con su texto literal extraído (sección 2).

---

## 2. Principios arquitectónicos extraídos del PDF

P1. **Dependency Rule** — *"Source code dependencies must point only inward, toward higher-level policies… Nothing in an inner circle can know anything at all about something in an outer circle… the name of something declared in an outer circle must not be mentioned by the code in an inner circle"* (Ch. 22, ca_full 5641–5651).

P2. **Nivel = distancia a los inputs/outputs** — *"A strict definition of 'level' is 'the distance from the inputs and outputs'"* (Ch. 19, 5224–5227); las dependencias de código deben acoplarse al nivel, no al flujo de datos (5239–5242).

P3. **Separación policy/details y boundaries** — los boundaries se dibujan donde hay *"an axis of change"*; SRP dice dónde dibujar (Ch. 17, 4976–4986); arquitectura plugin = *"firewalls across which changes cannot propagate"* (4972–4974); las flechas apuntan al core (4990–4998).

P4. **Cruce de frontera vía ports + DIP** — el use case no llama al presenter directamente; llama una *"use case output port"* que el presenter implementa; *"we take advantage of dynamic polymorphism to create source code dependencies that oppose the flow of control"* (Ch. 22, 5719–5741).

P5. **Datos a través de la frontera** — *"isolated, simple data structures are passed across the boundaries… We don't want to cheat and pass Entity objects or database rows"* (Ch. 22, 5742–5758).

P6. **Entities y Use Cases** — Entities encapsulan *"Critical Business Rules"* y son las menos propensas a cambiar; Use Cases orquestan el flujo de datos y están aislados de *"externalities such as the database, the UI, or any of the common frameworks"* (Ch. 20, 5653–5679).

P7. **Testability / Testing API** — *"your system architecture is all about the use cases… you should be able to unit-test all those use cases without any of the frameworks in place"* (Ch. 21, 5564–5572); los tests son *"the most isolated system component"* (Ch. 28, 6700–6712); el testing API *"decouples the structure of the tests from the structure of the application"*; el *structural coupling* es la forma más insidiosa de acoplamiento de tests (6761–6779).

P8. **Frameworks son detalles** — *"frameworks are not architectures"*; *"Don't marry the framework… Treat the framework as a detail that belongs in one of the outer circles"*; *"Don't let frameworks into your core code"* (Ch. 32, 7673–7763); proxies/plugins como adaptación (7758–7760).

P9. **Estabilidad medible** — Fan-in, Fan-out, *I = Fan-out/(Fan-in + Fan-out)* (Ch. 14, 3571–3582).

P10. **Main = detalle definitivo** — *"The Main component is the ultimate detail—the lowest-level policy… dependencies should be injected"* en Main (Ch. 26, 6261–6268).

P11. **Segregación de mutabilidad** (Ch. 6, TOC ca_front 106). El cuerpo del Ch. 6 no se extrajo completo; se usa el criterio con cautela (véase 15.8). Para P11 la evidencia primaria en OCM es la duplicación real de estado (F4), no el texto del PDF.

P12. **Screaming Architecture** — *"Your architecture should tell readers about the system, not about the frameworks you used"* (Ch. 21, 5576–5577).

---

## 3. Arquitectura real del linter

Inspección completa de `architecture_linter/` (22 ficheros fuente; stdlib-only, `tomllib`):

- `engine.py` — `RepoContext` (index de módulos, clases y referencias por AST `Name`/`Attribute`/definiciones/imports; 50–96) y `LinterEngine` (148–153). Analiza 353 ficheros `.py` productivos (raíces `packages, shared, apps, ocm`; `engine.py:29`).
- `analyzers/ast_walk.py` — inventario por AST: clases, bases, decoradores, `is_protocol`/`is_enum`/`is_abstract` (138–143), atributos `self._x` con mutaciones/lecturas (171–231), imports top-level y lazy (116–127).
- `analyzers/mutable_state.py` — heurísticas compartidas ARCH-001/002/003/010: patrones de nombre (`POSITION_ATTR_PATTERNS` 19, `ORDER_ATTR_PATTERNS` 20), hints de tipo (23–24), `detect_wac_semantics`/`detect_reduce_semantics`/`detect_unconditional_pop` (120–142).
- `rules/arch_001..010.py` + `rules/base.py` — 10 reglas; `rules/__init__.py` registro.
- `config.py` — `architecture/architecture_linter.toml` (roots, severidades, allowlist por símbolo). `exclude_paths` se parsea en `config.py:62-63` pero **no se usa** en `engine.py`/`cli.py`.
- `cli.py` — `--root/--config/--json/--sarif/--rules`; exit 0 = sin FAIL/PARTIAL, 1 = hay FAIL/PARTIAL, 2 = error de ejecución (71–73).
- `reporters/__init__.py` — human/json/sarif. `models.py` — `Finding`, `Status`, `RuleResult`, `OrderStore`, `PositionStore`.

**Autorreferencia verificada por ejecución:** 0 ficheros bajo `architecture_linter/` o `tests/` en el contexto; 353 ficheros analizados; roots = `apps, ocm, packages, shared`. El linter no se analiza a sí mismo ni a sus tests.

**Exit codes verificados por ejecución:** all-rules → `1`; `--rules ARCH-009` → `0`; JSON válido (schema 1.0). **Gap:** `--rules ARCH-999` → `0` con 0 reglas ejecutadas (`build_rules` descarta silenciosamente IDs desconocidos). Un typo en CI sería un PASS silencioso.

---

## 4. Matriz PDF → regla → código

Convención: **Sí** = detecta la propiedad arquitectónica real; **Parcial** = detecta una manifestación aproximada; **No** = no cubre; **Indirecto** = la regla es un proxy del principio.

| Principio PDF | Regla | Evidencia OCM (verificada) | Detectable | Resultado |
|---|---|---|---|---|
| P1 Dependency Rule | ARCH-009 (BC-08) | capas market_data; PASS en OCM; violación top-level + lazy detectadas en control positivo a9 | Parcial (solo market_data) | Útil |
| P1 Dependency Rule | ARCH-007 (duplicación cruzada) | 9 pares en archivos distintos | Indirecto | Proxy |
| P2 Nivel / I/O | — | — | No | Gap |
| P3 Policy vs details; eje de cambio | ARCH-001/002 (ownership/divergencia de posición) | trade_tracker/oms/risk/portfolio | Parcial (heurística de nombres) | Proxy |
| P3 Plugin architecture / firewalls | ARCH-006 (ports huérfanos) | 8 ports sin consumidores de código | Sí (en OCM) | Útil |
| P4 Ports + DIP (cruce de frontera) | ARCH-006, ARCH-009 | port consumido por subclass (a11) no huérfano; port solo en docstring sí huérfano | Sí/No según la referencia sea código o texto | Útil/Parcial |
| P5 Datos simples a través de fronteras | — | — | No | Gap |
| P6 Entities/Use Cases aislados | ARCH-009 (capa domain) | domain sin imports de infra/frameworks (verificado) | Parcial | Útil |
| P7 Testability / Testing API | — | — | No | Gap |
| P8 Frameworks son detalles | ARCH-008 (stub/capacidad falsa) | WSTradesSource, InfraMetricsKafkaProducer | Parcial (solo marcador NOT IMPLEMENTED) | Proxy débil |
| P9 Estabilidad (fan-in/out, I) | — | — | No | Gap |
| P10 Main = detalle | ARCH-006 (composition roots) | — | Parcial | Proxy |
| P11 Segregación de mutabilidad | ARCH-010 (estado mutable duplicado) | position ×6, order ×4 | Parcial (solo `self._x` + patrones de nombre) | Proxy |
| P12 Screaming Architecture | — | — | No | Gap |

**Conclusión de la matriz:** el linter cubre P1, P3, P4, P6 y P11 de forma parcial/indirecta. P2, P5, P7, P9, P12 no tienen cobertura. Ninguna regla mide estabilidad, testability estructural, paso de DTOs por fronteras ni acoplamiento a frameworks.

---

## 5. Validación ARCH-001..010

Veredictos del repo real (JSON `run1.json`, byte-idéntico a `run2.json`): `PASS=1 FAIL=9 PARTIAL=0 UNKNOWN=0`, `findings_total=28`, `failed_findings=27`.

| Regla | Qué pretende | Qué detecta (real, verificado) | Supuestos | Veredicto |
|---|---|---|---|---|
| ARCH-001 | Multiple position owners | 6 almacenes de posición; excluye SSOT por substring `portfolio/infra` (`arch_001.py:16,33`) → 5 owners → FAIL. Anchor `trade_tracker.py:59`. | El owner se llama `_positions/_open_positions/_entry_positions/_open/_position` o anota `tuple[float`/`Position`/`Order` (`mutable_state.py:19-23`). | VERIFIED para OCM; FRÁGIL (a1) |
| ARCH-002 | Divergencia semántica WAC vs reemplazo | 2 findings (ambos anclados `trade_tracker.py`, line=None): WAC-vs-replace y reduce-vs-pop. Requiere literales `new_qty`+`new_avg`+`*` (`mutable_state.py:125`), `remaining` (:133), `.{attr}.pop(` (:142) y rutas hardcodeadas `WAC_OWNERS`/`REPLACE_OWNERS` (`arch_002.py:24-25`). | Los nombres exactos de variables y rutas de la evidencia forense. | PARTIALLY VERIFIED (dependiente de nombres/rutas) |
| ARCH-003 | Órdenes en memoria sin reconciliación | FAIL correcto en OCM (`oms.py:169` `_orders`). La lista `present` es global al repo e incluye `fetch_state` (3 ficheros), `recovery` (5), `OrderTransport` (3) — no reconciliación trading. Rama PARTIAL (`arch_003.py:84-96`) **inalcanzable** (a14). | Los mecanismos se llaman `fetch_open_orders/manage_open_orders/…` (`arch_003.py:16-25`). | PARTIALLY VERIFIED (dead code + FP/FN por nombre) |
| ARCH-004 | Balance real vs capital estático | FAIL correcto: 0 símbolos de balance; `capital_usd` en `position.py:146`. Índice AST: `references("capital_usd")` = 46 refs en 13 ficheros (4 en risk/portfolio); `references("capital")` = 7 refs en 2 ficheros (apps/app/cli, sin risk/portfolio). | El método de balance contiene una de 8 cadenas exactas (`arch_004.py:14-23`). | PARTIALLY VERIFIED (a7: FP) |
| ARCH-005 | Cadena de freshness | FAIL correcto: niveles 1–2 presentes (`gap_aware_stream.py:259` wait_for, `:285/:314` `_run_recovery`), 3–6 ausentes. Símbolos por nombre exacto (`arch_005.py:17-20`). | `wait_for`/`_handle_silence_gap`/`gap_threshold_ms`/`TimeoutError` en websocket; freshness por `freshness/stale/lag_ms/…`. | VERIFIED para OCM; FRÁGIL por nombre |
| ARCH-006 | Ports huérfanos | 8 ports sin consumidores de código (verificado: `EventConsumerPort`, `OrderBookSourceProtocol`, `EventPublisherPort`, `DerivativesFetcherPort`, `CircuitBreakerPort`, `MarketDataSourcePort`, `BronzeStoragePort`, `ExternalMetricsPort`). Las referencias en docstrings (p. ej. `bronze_writer.py:105`, `metrics_adapter.py:224`) no cuentan → correcto: son huérfanos de código. | El índice de referencias no ve anotaciones string ni `getattr`. | VERIFIED para OCM; FP vía consumo dinámico (a4) |
| ARCH-007 | Duplicados/homónimos | 9 pares en archivos distintos (verificado). `_class_members` compara nombres de miembros/bases, **no valores de enum** pese al docstring (`arch_007.py:86-99`). `_is_mirror_pattern` excluye ocm/config (106–115). Allowlist ADR-0003 aplicada. | Mismo nombre de clase; miembros por nombre. | PARTIALLY VERIFIED (a5: FN por nombre) |
| ARCH-008 | Stub / false capability | 2 stubs: `WSTradesSource` (`ws_trades_source.py:32`; docstring `NOT_IMPLEMENTED` :14; `_running=False` :58/:96; `is_running` :78-79; `StopAsyncIteration` :90) e `InfraMetricsKafkaProducer` (`infra_metrics_producer.py:31`). Solo marcadores `NOT_IMPLEMENTED`/`NOT IMPLEMENTED` (39–41); `STUB_MARKERS` (:17) **muerto**. | Marcador textual; `_body_has_not_implemented` es por archivo (81–87). | PARTIALLY VERIFIED (a3: FP + FN + duplicados) |
| ARCH-009 | Capas BC-08 | PASS correcto en OCM; lee el SSOT `architecture/importlinter.toml` (BC-08, `:209-218`) + `ignore_imports`. Control positivo a9 detecta violación top-level y lazy. No cubre otros containers (portfolio BC-44, medallion). | Contrato BC-08 como SSOT. | VERIFIED para su alcance; alcance parcial |
| ARCH-010 | Estado mutable duplicado | position ×6, order ×4 (reproducido). `TradeTracker._open_positions` cuenta como order por substring `_open` (`mutable_state.py:20`) → solape de conceptos. Anchor en `memory_store.py:33` (el SSOT). | `self._x` dict + patrón de nombre. | PARTIALLY VERIFIED (solapes, a1/a10: FN) |

---

## 6. Golden findings F1–F4

Todos verificados contra código real y reproducidos por el linter (2 ejecuciones idénticas).

**F1 — WSTradesSource / fake capability → ARCH-008 FAIL.** Código: `ws_trades_source.py` — docstring `NOT_IMPLEMENTED` (:14), `self._running = False` (:58), `is_running()` retorna `_running` (:78–79), `raise StopAsyncIteration` (:90), `_running` nunca True (:96). Linter: finding `ws_trades_source.py:32`. **Detecta el problema real** (capacidad prometida no ejecutada), pero el *trigger* es el marcador textual, no el análisis de comportamiento.

**F2 — freshness / GapAwareStream → ARCH-005 FAIL.** Código: `gap_aware_stream.py` — detección `asyncio.wait_for` (:259), recovery `_run_recovery` (:285, :314); el port `trades_source.py` solo expone `is_running` (:113, :169) sin símbolos de freshness; trading/portfolio sin símbolos de freshness (verificado: 0 coincidencias). Linter: niveles 1–2 presentes, 3–6 ausentes → FAIL. **Detecta la propiedad real** (la cadena se corta tras la detección), pero por ausencia de símbolos, no por análisis de política.

**F3 — MarketDataSourcePort huérfano → ARCH-006 FAIL.** Código: `ports/outbound/market_data_source.py:32`; 0 consumidores de código en OCM; docstring "Puerto INBOUND" (:7) en `ports/outbound/`; referencias a `CcxtRestAdapter`/`ReplayAdapter` inexistentes (archivos y clases ausentes, verificado). Linter: finding `:32`, confianza 0.95, con evidencias de docstring contradictorio e impls inexistentes. **Detecta el problema real.**

**F4 — múltiples owners y divergencia semántica de posición → ARCH-001/002/010 FAIL.** Código: `trade_tracker.py:59` `_open_positions`, pyramid (:137–146), pop incondicional (:158); `oms.py:416-421` WAC (`new_qty`/`new_avg`), reduce `remaining` (:445-447); `manager.py:190,193`; `portfolio_service.py:120-148,238-255`; SSOT `infra/memory_store.py:33`. Linter: ARCH-001 (5 owners), ARCH-002 (2 divergencias), ARCH-010 (position ×6, order ×4). **Detecta el problema real en OCM**, pero mediante literales de nombre/ruta (a2/a2b/a5 lo falsan).

---

## 7. Falsos positivos demostrados

| Regla | Archivo (fixture) | Línea | Razón | Evidencia | Corrección propuesta |
|---|---|---|---|---|---|
| ARCH-008 | `a3/b.py` `AlsoCleanSource` | 8 | `_body_has_not_implemented` escanea el fichero completo ignorando `class_line` (`arch_008.py:81-87`); un marcador en otra clase del mismo archivo contamina a todas las clases no abstractas | Ejecución: 4 findings para 2 clases | Restringir el escaneo al cuerpo de la clase (AST del ClassDef) |
| ARCH-008 | `a3/b.py` `StubbedSource` | 1 | `analyze` itera `all_classes()` y por cada entrada re-itera todas las clases del módulo (`arch_008.py:30-34`) → findings duplicados en ficheros multi-clase | Ejecución: StubbedSource 2×, AlsoCleanSource 2× | Iterar clases únicas del módulo |
| ARCH-004 | `a7` `RiskManager` | 1 | Método real de consulta de balance renombrado (`fetch_wallet_total`) no está en `BALANCE_SYMBOLS` (match exacto) → FAIL aunque el balance existe | Ejecución: FAIL | Emparejar por rol/puerto, no por nombre literal |
| ARCH-003 | `a8` `OmsEngine` | 3 | Reconciliación real renombrada (`sweep_open_orders`, `fetch_exchange_orders`) → `present=[]` → FAIL | Ejecución: FAIL "Mecanismos presentes: ninguno" | Detectar por estructura (loop de sync + fetch), no por nombre |
| ARCH-006 | `a4` `RemoteGatewayPort` | 5 | Consumo dinámico (`getattr(module, "RemoteGatewayPort")`) invisible al índice de referencias → "0 consumidores" falso | Ejecución: FAIL | Registrar `getattr` sobre módulos de protocolos como consumo sospechoso |
| ARCH-003 | `a14` (ramificación) | 3 | Con `fetch_state` presente (escenario documentado para PARTIAL) devuelve FAIL: la rama PARTIAL (84–96) es código muerto porque la rama FAIL (65) se evalúa primero y la cubre | Ejecución: FAIL, PARTIAL nunca emitido | Reordenar condiciones; decidir qué escenario debe ser PARTIAL |

**En OCM real no se observó ninguno de estos FP** (los 27 failed_findings corresponden a violaciones reales), pero los vectores son latentes en el código del linter.

---

## 8. Falsos negativos demostrados

| Regla | Fixture | Evidencia (comando) | Problema arquitectónico no detectado |
|---|---|---|---|
| ARCH-001/010 | `a1` (`_book`/`_ledger: dict[str, object]`) | PASS | Estado de posición duplicado con nombres neutros y anotación genérica |
| ARCH-002 | `a2` (`total_qty`/`avg_cost` + `del`) | PASS | WAC vs reemplazo con variables renombradas y pop por `del` |
| ARCH-002 | `a2b` (WAC en `execution/orders.py`) | PASS | Dependencia de las rutas hardcodeadas `WAC_OWNERS` |
| ARCH-008 | `a3/c.py` (raise NotImplementedError sin marcador) | PASS | Stub real sin marcador textual (p. ej. `runtime.py:468`, `derivatives_fetcher.py:209,212` en OCM) |
| ARCH-006 | `a4` (`getattr`) | FAIL (FP) | El port SÍ tiene consumidor dinámico → falsamente huérfano |
| ARCH-007 | `a5` (`OrderState`/`OrderTicket`, mismos miembros) | PASS | Duplicación semántica con nombres distintos |
| ARCH-009 | `a6` (portfolio `models→services`) | PASS | Violación de capa BC-44 no cubierta (ARCH-009 solo lee BC-08) |
| ARCH-010 | `a10` (`OPEN_POSITIONS` global de módulo) | PASS | Estado mutable global fuera de `self._x` |
| ARCH-009 | `a13` (callback `on_fill` cruzando frontera) | PASS | Inversión de dependencia por callback/DI invisible al grafo de imports |
| ARCH-003 | `a8` (reconciliación renombrada) | FAIL (FP) | Reconciliación real no reconocida |

**FNs de mayor peso arquitectónico** (por inspección, sin fixture): dependencias indirectas y service locator; framework leakage en domain (hoy ausente en OCM, pero sin regla que lo vigile fuera de BC-09 estático); paso de entidades/rows por fronteras (P5); métricas de estabilidad (P9).---

## 9. Tests adversariales (ejecutados, resultados)

Harness: `/tmp/opencode/adv/gen_fixtures.py` + 14 árboles en `/tmp/opencode/adv/<id>/`. Todos ejecutados contra el linter real vía `uv run python -m architecture_linter --root … --rules …`. Ningún árbol toca el código productivo.

| ID | Mutación aplicada | Regla | Resultado | Lectura |
|---|---|---|---|---|
| a1 | rename `_positions`→`_book`/`_ledger`, anotación `dict[str, object]` | ARCH-001/010 | PASS | FN: duplicado real invisible |
| a2 | WAC→`total_qty`/`avg_cost`; SELL `del` | ARCH-002 | PASS | FN: divergencia invisible |
| a2b | WAC real (`new_qty`/`new_avg`) en `execution/orders.py` | ARCH-002 | PASS | FN: rutas hardcodeadas |
| a3 | 1 marcador en fichero con 2 clases + clase sin marcador + clase con raise solo | ARCH-008 | FAIL (4 findings) | FP (clase limpia) + duplicación + FN (raise sin marcador) |
| a4 | port consumido por `getattr` | ARCH-006 | FAIL | FP: consumo dinámico invisible |
| a5 | enums mismos miembros, nombres distintos | ARCH-007 | PASS | FN: duplicación semántica |
| a6 | violación capa portfolio | ARCH-009 | PASS | Gap de cobertura (no BC-44) |
| a7 | balance real renombrado (`fetch_wallet_total`) | ARCH-004 | FAIL | FP: balance existe pero renombrado |
| a8 | reconciliación renombrada (`sweep_open_orders`) | ARCH-003 | FAIL | FP: mecanismo existe pero renombrado |
| a9 | **control positivo**: domain→infra top-level + lazy | ARCH-009 | FAIL (2 findings) | ✓ detecta top-level y lazy (`lazy.py:3`) |
| a10 | estado global de módulo (`OPEN_POSITIONS`) | ARCH-001/010 | PASS | FN: solo `self._x` |
| a11 | **control positivo**: port consumido por subclass | ARCH-006 | PASS | ✓ no huérfano |
| a12 | **control positivo**: stub con marcador único | ARCH-008 | FAIL (1 finding) | ✓ |
| a13 | callback cruzando frontera (sin import) | ARCH-009 | PASS | FN: acoplamiento invisible |
| a14 | `fetch_state` presente, sin manage/fetch_open | ARCH-003 | FAIL (no PARTIAL) | rama PARTIAL inalcanzable |

**Lectura global:** el linter entiende patrones de nombre/ruta, no estructura arquitectónica. Cambiar un nombre rompe la detección; los controles positivos (a9, a11, a12) confirman que las reglas ejecutan su lógica, pero sobre heurísticas superficiales.

---

## 10. Cobertura arquitectónica (qué puede verificar de verdad)

- **Sí, con fundamento:** ports huérfanos (ARCH-006) y capas BC-08 (ARCH-009) verifican la Dependency Rule sobre el grafo de imports real; estado mutable duplicado de `self._x` en conceptos críticos (ARCH-010) y ownership distribuido (ARCH-001) verifican la segregación de mutabilidad sobre mutaciones AST reales.
- **Parcial/superficial:** ARCH-002, ARCH-003, ARCH-004, ARCH-005 y ARCH-007 dependen de literales (nombres de variables, métodos, rutas, símbolos). Detectan los casos golden pero no la propiedad arquitectónica general.
- **No verifica:** estabilidad (P9), testability/structural coupling (P7), paso de datos por fronteras (P5), Screaming Architecture (P12), nivel de política (P2), acoplamiento a frameworks fuera del grafo estático (P8, más allá de BC-09).

**"Clean Architecture no debe convertirse en grep":** ARCH-008 es el caso más claro — un stub sin marcador (raise `NotImplementedError` puro) no se detecta, y una clase limpia en un fichero con marcador se marca; ARCH-002 y ARCH-003 marcan/omiten por el *string* literal `new_qty`/`remaining`/nombre de método, no por la semántica de la operación. Los tests verdes (§9) no corrigen esto: los tests unitarios codifican exactamente los literales que las reglas buscan.

---

## 11. Gaps entre PDF y linter

1. **P2 Nivel = distancia a I/O** — sin regla ni métrica (exigiría grafo de dependencias por nivel de política).
2. **P5 Datos a través de fronteras** — sin detección de entidades/rows/datos de framework cruzando hacia adentro.
3. **P7 Testability / Testing API / structural coupling** — el linter no evalúa si los use cases son testeables sin frameworks; sus propios tests sufren *structural coupling* (codifican internals; §9).
4. **P8 Framework leakage** — solo BC-09 (estático, imports top-level); imports lazy/string de frameworks en domain no se vigilan (ARCH-009 detecta lazy para capas pero no se aplica a forbidden-modules).
5. **P9 Estabilidad** — sin fan-in/fan-out/I (datos disponibles en el índice del engine, no usados).
6. **P10 Main / P3 plugin** — ARCH-006 cubre ports huérfanos pero no verifica que las implementaciones sean *plugins* desacoplados.
7. **P12 Screaming Architecture** — no medible por AST de forma fiable; gap declarado.
8. **Conmutabilidad de nombres** — el mayor gap operativo: ninguna regla distingue "nombre distinto, misma arquitectura" de "misma arquitectura, otro nombre" (§8).

---

## 12. Reglas nuevas recomendadas

Criterio del encargo: PDF + manifestación concreta en OCM + evidencia objetiva + test reproducible + estrategia anti-FP. Solo se proponen las que cumplen; el resto queda como gap declarado.

### 12.1 Extender ARCH-009 a todos los contratos `layers` (recomendación fuerte)
- **Principio:** P1 Dependency Rule (Ch. 22, 5641–5651).
- **Manifestación concreta:** `architecture/importlinter.toml` define 4 contratos `type = "layers"` (verificado con `rg -c`, y BC-44 "portfolio layer order" presente en la salida de lint-imports). ARCH-009 solo lee BC-08 (`arch_009.py:46`). La violación de BC-44 demostrada en a6 es invisible.
- **Evidencia requerida:** mismos `layers` + `ignore_imports` de cada contrato; PASS/FAIL/UNKNOWN por contrato.
- **FP strategy:** replicar exactamente `ignore_imports`; no inventar capas (misma política que BC-08).
- **FN conocidos:** callbacks/DI (a13) siguen invisibles.

### 12.2 Extender ARCH-010: estado mutable a nivel de módulo/global
- **Principio:** P11 Segregación de mutability (Ch. 6; en OCM, F4).
- **Manifestación:** el linter no detecta `OPEN_POSITIONS: dict` global (a10). Hoy OCM no tiene dicts globales mutables en trading/portfolio (verificado), pero la extensión es barata y cierra el hueco.
- **Evidencia:** `ast.Assign`/`AnnAssign` a nivel de módulo con anotación dict + mutaciones (`Subscript`/`pop`/`update`).
- **FP strategy:** excluir constantes inmutables y caches de configuración; requerir mutación real.

### 12.3 Refinar ARCH-008: trigger por comportamiento, no solo marcador
- **Principio:** P8 (Ch. 32).
- **Manifestación:** `runtime.py:468`, `derivatives_fetcher.py:209,212` lanzan `NotImplementedError` sin marcador (invisibles); clase limpia contigua a un marcador se marca (a3). `STUB_MARKERS` definido en `arch_008.py:17` está muerto.
- **Propuesta:** trigger = `raise NotImplementedError` en método de nombre público (no `_run`/abstracto) **o** marcador; escanear solo el cuerpo de la clase.
- **FP strategy:** excluir bases abstractas, privados con docstring de hook y ports.

### 12.4 No propuestas (gap declarado)
- Métricas de estabilidad (P9): datos disponibles pero sin decisión de umbral; riesgo alto de ruido → `UNKNOWN`.
- Testability/Testing API (P7): no hay métrica AST fiable → `UNKNOWN`.
- Framework leakage más allá de BC-09: hoy sin manifestación en OCM (domain limpio, verificado) y cubierto estáticamente → no añadir regla redundante.

---

## 13. Resultados de ejecución (gates reales, re-ejecutados hoy)

| Herramienta | Comando | Resultado | Exit |
|---|---|---|---|
| ruff | `uv run ruff check .` | All checks passed | 0 |
| ruff format | `uv run ruff format . --check` | 490 ficheros formateados | 0 |
| mypy | `uv run mypy .` | Success, 381 ficheros | 0 |
| bandit | `uv run bandit -r apps ocm packages shared infrastructure` | 51 issues Low (0 Med/High; B101/B110 mayormente) | 1 |
| import-linter | `uv run lint-imports --config architecture/importlinter.toml` | 50 kept, 0 broken | 0 |
| pytest | `uv run pytest tests/ -m "not integration" -q` | 1136 passed, 4 deselected, 79 warnings | 0 |
| coverage | (misma ejecución) | 50.37% (umbral 40%) | 0 |
| pytest linter | `uv run pytest tests/architecture_linter/ -q` | 27 passed | 1* |

\* El exit 1 en el subconjunto `tests/architecture_linter/` es un artefacto de `addopts = --cov=packages --cov=ocm …` (`pyproject.toml:264`): los tests del linter usan fixtures temporales y no tocan `packages/ocm/shared/apps`, por lo que `--cov-fail-under=40` reporta 0.00%. Los 27 tests pasan; la suite completa (con los módulos en el cov) es verde.

**bandit:** los 51 issues son estilo preexistente (asserts, except-pass), ajenos al linter; `bandit -r architecture_linter` arroja 0 issues. Se registra como limitación del gate, no de esta auditoría.

**Linter sobre OCM:** `uv run python -m architecture_linter` → `PASS=1 FAIL=9 PARTIAL=0 UNKNOWN=0`, `findings_total=28`, `failed_findings=27`, exit 1. JSON válido.

---

## 14. Reproducibilidad

Dos ejecuciones consecutivas sobre el mismo estado del repositorio:
- `uv run python -m architecture_linter --json` (run1) vs (run2): **byte-idénticas** (`json.load` compara `True`), exit code `1` en ambas.
- Distribución idéntica: ARCH-001(1), 002(2), 003(1), 004(1), 005(1), 006(8), 007(9), 008(2), 009(1 PASS), 010(2) → 28 findings, 27 failed.
- Orden de findings, líneas, severidades y mensajes idénticos.

**Reproducibilidad VERIFICADA.**

---

## 15. Limitaciones

1. **Heurísticas por nombre/ruta** (ARCH-002/003/004/005/007/010): falsables con renombrados (§8).
2. **`detect_unconditional_pop`** (`mutable_state.py:142`): `return f".{attr_name}.pop(" in text or f".{attr_name}.pop(" in text` — los dos operandos son idénticos (copia muerta).
3. **`detect_wac_semantics`/`detect_reduce_semantics`** (`mutable_state.py:125,133`): substring sobre el fichero completo; en OCM `remaining` aparece incluso en docstrings de `oms.py` (:53, :67, :69).
4. **ARCH-008 `_body_has_not_implemented` por archivo** (`arch_008.py:81-87`) + duplicación de findings en módulos multi-clase (`arch_008.py:30-34`): FP y ruido demostrados (a3).
5. **Rama PARTIAL inalcanzable en ARCH-003** (`arch_003.py:65` se evalúa antes que `:84`): el matiz documentado no puede emitirse (a14).
6. **`STUB_MARKERS` muerto** (`arch_008.py:17`) y **`exclude_paths` parseado sin uso** (`config.py:26,62-63`).
7. **Índice de referencias incompleto** (`engine.py:50-96`): no ve anotaciones string ni consumo dinámico (`getattr`), con vectores FP/FN en ARCH-006/007.
8. **P11 con evidencia parcial en el PDF:** el cuerpo del Ch. 6 no se extrajo; la regla ARCH-010 se justifica por la manifestación en OCM (F4), no por el texto del PDF (honestidad de evidencia).
9. **Contenido del PDF:** solo los capítulos de la tabla de §1 se leyeron con texto extraído; el resto del libro no se utilizó como criterio.
10. **bandit:** 51 issues Low preexistentes ajenos al linter (exit 1); `bandit -r architecture_linter` = 0 issues.
11. **Anchor de ARCH-002** con `line=None` y **anchor de ARCH-010** en el SSOT (`memory_store.py:33`): la localización del finding puede despistar al consumidor del informe.
12. **Solape conceptual en ARCH-010:** `TradeTracker._open_positions` cuenta como order vía substring `_open` (`mutable_state.py:20`); el conteo order ×4 sobredeclara almacenes de órdenes.

---

## 16. Veredicto

### B — ÚTIL PERO PARCIAL (no confiable como gate arquitectónico único)

**Base de la decisión (todas las evidencias ejecutadas, no inferidas):**

1. **Detecta correctamente los golden F1–F4** contra OCM real (F3 y F4 son hallazgos arquitectónicos reales y verificados en código). 28 findings reproducibles byte-a-byte.
2. **ARCH-006 y ARCH-009 son las únicas reglas con fundamento estructural** (grafo de imports/consumidores reales), alineadas con P1/P3/P4 del PDF. ARCH-001/010 operan sobre mutaciones AST reales de `self._x`, lo que les da base empírica.
3. **El resto de reglas son heurísticas de nombres/rutas** que detectan los casos golden pero colapsan ante renombrados: 9 de 14 mutaciones adversariales produjeron FP o FN (§8–§9). "El objetivo no es demostrar que el linter funciona": la evidencia muestra que funcionó *para los casos conocidos*, no que represente la arquitectura.
4. **Clean Architecture como grep:** ARCH-008 marca por el string `NOT IMPLEMENTED` y ARCH-002/003 por `new_qty`/`remaining`/nombres de método; un test verde (27/27) no equivale a regla arquitectónicamente correcta — los tests unitarios codifican los mismos literales que las reglas buscan.
5. **Cobertura del PDF parcial:** 5 principios (P2, P5, P7, P9, P12) sin cobertura; la matriz (§4) no inventa correspondencias.
6. **Gaps operativos concretos en el código:** rama PARTIAL muerta, marcador STUB_MARKERS muerto, escaneo por archivo, duplicación de findings, `exclude_paths` sin uso, IDs de regla desconocidos con PASS silencioso, `detect_unconditional_pop` con operando duplicado.
7. **Gates:** ruff/mypy/import-linter/pytest verdes; bandit con 51 Low preexistentes (exit 1); reproducibilidad verificada; autorreferencia limpia.

**Condiciones para su uso como gate:** (a) en combinación con import-linter (BC-NN) y las herramientas existentes, no en solitario; (b) tras aplicar 12.1 (cobertura de todos los contratos layers) y 12.3 (trigger por comportamiento en ARCH-008); (c) corrigiendo la rama PARTIAL y el escaneo por archivo; (d) tratando ARCH-002/003/004/005/007 como señales para revisión manual, no como veredictos.

**Clasificación alternativa honesta:** el linter es hoy C (heurístico) en sus reglas basadas en nombres y B en ARCH-006/009. El promedio de la herramienta es **B — ÚTIL PERO PARCIAL**, con riesgo real de **C** si se usa como gate sin las correcciones 12.x, porque los falsos negativos (estado renombrado, reconciliación renombrada, callbacks, capas fuera de market_data) pueden dejar pasar exactamente los defectos que pretende vigilar.

---

*Fin del informe. Evidencia ejecutable: `run1.json`/`run2.json`, fixtures en `/tmp/opencode/adv/`, `ca_full.txt`. No se modificó código de negocio, ADR-0029/0030 ni `tracking.yaml`; no se realizaron `git add/commit/push`; los artefactos temporales quedan fuera del árbol productivo y documentados.*