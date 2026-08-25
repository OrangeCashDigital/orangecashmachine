# PLAN-Maestro-Ingenieria.md

**OrangeCashMachine — Especificación del Sistema de Ingeniería (SSOT)**

- **Estado:** Activo — documento operativo vivo (no un PDF). El historial se preserva en Git, no en versiones de archivo.
- **Rol:** fuente de verdad (SSOT) para la evolución técnica del proyecto.
- **Alineado con:** `docs/audits/2026-08-auditoria-integral.md` — fotografía histórica del estado en commit `dcd1741` (2026-08-06 00:08:04 -0500), **inmutable**.
- **Datos operativos:** `docs/plans/tracking.yaml` (v2) — fuente de verdad **por máquina**; este documento es el **mapa** que explica cómo funciona el sistema, no el tracker.

> ## ⚖️ Regla suprema (preamble)
> **No se implementará ninguna funcionalidad nueva si degrada cualquiera de los artefactos normativos del proyecto.**
> Orden inviolable del cambio: `Plan → Tracking → ADR → Código → Tests → CI → Release`. Un paso nunca se salta; los artefactos normativos son la Constitución (ver §12 · §13).

- **Métricas baseline (remedidas en vivo en F0, 2026-08-06):** 900 tests (suite unit, integration excluidas) / 44 % cobertura / **49 contratos BC** / 25 constantes de tópicos Kafka / 52 237 LOC Python. El baseline "47 contratos" se corrigió a "49" **por BC-53 y BC-54** (trazabilidad del blindaje de `apps/`, serie INFORME-2026-08-06) — es trazabilidad, no un error previo; el `mediciones_f0` del tracking registra el delta 47→49.

---

## 1. Cómo usar este documento (onboarding)

> Objetivo: un ingeniero nuevo debe entender el sistema de evolución **sin conocimiento implícito**. Este es el único punto de entrada obligatorio.

**¿Qué problema evita?** El conocimiento crítico (qué reglas existen, cómo se decide, cómo se cierra un hallazgo) vive en la cabeza de quien lo escribió; cuando esa persona se va, el sistema se degrada.

**¿Qué mecanismo automático lo implementa?** Este documento + `docs/plans/tracking.yaml` son los únicos SSOT. Todo cambio técnico pasa por las cadenas de este documento (traza en §2) y se registra en el yaml. No hay "backlogs por auditoría".

**¿Qué evidencia demuestra que funciona?** El historial de Git: cada decisión tiene un commit que referencia el hallazgo y su ADR; el yaml tiene un `baseline` y un esquema versionado.

**¿Cómo evita que el problema reaparezca?** Los flujos de este documento (hallazgo→cierre, ADR, gate) se re-ejecutan en cada cambio; la automatización (§3) bloquea desviaciones.

### Flujos rápidos (lo primero que busca un ingeniero nuevo)

| Quiero… | Hago… | Referencia |
|---|---|---|
| Saber el estado real del sistema | `cat docs/plans/tracking.yaml` — cada hallazgo tiene `estado` y `estado_auditoria` | §2, §7 |
| Registrar un hallazgo nuevo | Crear entrada `hallazgos[].id=B-NN` en tracking.yaml v2 con evidencia y estado `PENDIENTE` | §2, §7 |
| Proponer una decisión de arquitectura | Verificar numeración: `ls docs/architecture/decisions/` → crear ADR con la plantilla `ADR-template.md` → enlazar al hallazgo | §5 |
| Saber si algo está resuelto | Leer la cadena de trazabilidad del hallazgo en tracking.yaml (cada eslabón con `estado` y `evidencia`) | §2, §7 |
| Verificar si el sistema es "producción-ready" | Ejecutar `scripts/check_production_gates.py` (veredicto binario PASS/FAIL) — **PENDIENTE: script no existe, ver B-49** | §6 |
| Cumplir la Definition of Done | Aplicar la cadena completa de §2 y el DOD de la fase correspondiente (§4) | §2, §4 |

---

## 2. Trazabilidad total — la cadena maestra

> **Principio 1:** todo hallazgo debe tener trazabilidad completa. La cadena es el único camino válido de un problema a su cierre.

**Cadena:** `Hallazgo → Backlog → ADR → Implementación → Tests → CI → Evidencia → Cierre`

Cada eslabón responde a las 4 preguntas del sistema:

| Eslabón | ¿Qué problema evita? | ¿Mecanismo automático? | ¿Evidencia? | ¿Cómo evita que reaparezca? |
|---|---|---|---|---|
| **Hallazgo** | Un defecto sin registrar se pierde | Entrada en tracking.yaml v2 con `estado_auditoria` y evidencia citada | Cita `archivo:módulo:función` re-verificada | El yaml es única fuente; nada existe fuera de él |
| **Backlog** | Trabajo sin prioridad ni dueño | ID único `B-NN`, fase, prioridad, estado | Columna en §7 + yaml | Los estados son un enum cerrado (`PENDIENTE…RECHAZADO`) |
| **ADR** | Decisiones no documentadas o numeración colisionada | **Guard de numeración**: `ls docs/architecture/decisions/` antes de crear cualquier ADR (lección: colisión real de dos series) | ADR commiteada con estado `Aceptado`; enlace en yaml | El guard se repite en cada ADR; nunca se asume el número |
| **Implementación** | Cambios grandes e irreversibles | Commits atómicos; una regla = tres commits (test→fix→docs) | Diff por commit en Git | §8 estrategia de commits; pre-commit hooks siempre activos |
| **Tests** | Defectos que pasan CI | Test de regresión positivo+negativo **commiteado en el mismo PR que el fix** | Tests verdes con fix, rojos sin fix (backtest) | Regla no se activa en CI sin su backtest `ok` |
| **CI** | Calidad dependiente de disciplina humana | Gates: import-linter, mypy, bandit, cobertura `fail_under`, backtest | Job CI rojo ante violación (fail-fast en `ocm-ci.yml`) | Las reglas gateadas son propiedad del repo, no de las personas |
| **Evidencia** | "Está resuelto" sin demostrarlo | Backtest contra snapshot pre-fix; mediciones en vivo con fecha+commit | Salida del cheque + hash de commit | Cualquier afirmación sin cheque medible se marca **No verificado** |
| **Cierre** | Hallazgos "resueltos" que nadie volvió a medir | `estado: VERIFICACION` → solo pasa a `HECHO` con fecha_cierre y evidencia | `fecha_cierre` + referencia del cheque | Job `tracking-consistency` valida coherencia del yaml |

### Ejemplo transversal completo (el ciclo completo, de principio a fin)

> Usamos **B-03 / H-03** (drift del contador de riesgo) como el ejemplo canónico. Demuestra cómo se ve la cadena en la vida real.

1. **Hallazgo** — `docs/audits/2026-08-auditoria-integral.md` H-03: `packages/trading/execution/oms.py:172` llama `record_open()` en todo `submit()`; `record_close()` solo en `cancel()` (`:217`) y `_reject()` (`:308`); `_fill()` (`:270-289`) no decrementa → un ciclo BUY→SELL deja `_open_positions` inflado. **Evidencia:** lectura directa de `oms.py` y `risk/manager.py:126-139`. `estado_auditoria: CONFIRMADO`.
2. **Backlog** — Entrada `B-03` en tracking.yaml v2: fase F1, prioridad CRÍTICA, estado `PENDIENTE`.
3. **ADR** — Revisión: ¿decisión de arquitectura? La semántica de `_open_positions` ("órdenes activas" vs "posiciones reales") **sí** es decisión → se documenta. **Guard ejecutado:** `ls docs/architecture/decisions/` → ADR-0015 ya está ocupado por el blindaje de apps, así que el siguiente libre es **ADR-0016** (ver §5). La decisión queda **dentro de ADR-0016** (tema: LiveExecutor real + reconciliación de fills + semántica del contador de posiciones), que agrupa el ciclo orden→fill→estado (§5, §7).
4. **Implementación** — Commits atómicos: (a) `test(rules): R3 — round-trip BUY→SELL aserta contador` (rojo sobre el bug); (b) `fix(trading): record_close en flujo de fill` (verde); (c) `docs(plans): B-03 → EN_CURSO`.
5. **Tests** — Positivo: round-trip con contador incorrecto → rojo. Negativo: tras el fix, contador correcto → verde.
6. **CI** — La regla R3 (AST guard) se activa en CI **solo** cuando `backtest: ok` en el yaml. CI fail-fast bloquea merges con la regla roja.
7. **Evidencia** — Backtest documentado: sobre commit pre-fix, R3 dispara; sobre post-fix, no falsea. `backtest: ok`, `activada_en_ci: true`.
8. **Cierre** — `estado: VERIFICACION` (una semana después, CI verde sostenido) → `estado: HECHO`, `fecha_cierre`, `riesgo_residual` actualizado.

> **Regla del ejemplo:** los otros hallazgos siguen exactamente esta plantilla; no se repite la prosa de cada uno en este documento. Se leen en tracking.yaml (v2), que es la fuente operativa.

---

## 3. Principios del sistema de ingeniería (10)

> **Principios estructurales:** cada uno responde — problema que evita, mecanismo automático, evidencia, y cómo previene la regresión. (Principios 2, 3, 9, 10 del requisito.)

| # | Principio | ¿Qué problema evita? | ¿Mecanismo automático? | ¿Evidencia? | ¿Cómo previene la regresión? |
|---|---|---|---|---|---|
| 1 | Trazabilidad completa | Defectos huérfanos | Cadena de §2 + tracking.yaml v2 | Backlog con cadena por hallazgo | Job `tracking-consistency` en CI |
| 2 | Automatización > disciplina | "Recordar revisar X" falla | import-linter (50 BC), AST guards (`tests/architecture/`), mypy, bandit, CI gates | Comando que da FAIL ante la violación | Las reglas son propiedad del repo, se ejecutan en cada PR |
| 3 | Evidencia verificable | "Mejoramos" sin demostrarlo | Cada objetivo = cheque medible (no intención) | Salida del cheque + fecha + commit | Cualquier afirmación sin cheque se marca **No verificado** |
| 4 | ADR para decisiones | Arquitectura implícita | Plantilla `ADR-template.md` + guard de numeración | ADR con estado; enlace en yaml | Guard de numeración en cada creación |
| 5 | DOR/DOD por tarea | Trabajo "en progreso" infinito | Criterios de entrada/salida por fase (§4) | Estado en yaml + cheque del DOD | DoD con comando verificable |
| 6 | Backlog único | Backlogs por auditoría | tracking.yaml consolida H-*, R* y B-* | Un solo tracker | Fuente única; los .md no duplican |
| 7 | Cambios pequeños y reversibles | Diffs imposibles de revertir | Commits atómicos (§8) | Git history con 1 cambio lógico/commit | Hooks de pre-commit siempre activos |
| 8 | CI como puerta, no sugerencia | Merges rotos | Gates reales (fail-fast en `ocm-ci.yml`) | CI rojo bloquea merge | Ningún merge a `main` con CI rojo |
| 9 | Umbrales tras medición | Números inventados (13% stale vs 43% real) | Medición en vivo en F0 antes de fijar umbrales | Mediciones con fecha/commit | §10: umbrales solo tras F0 |
| 10 | Sistema que se audita solo | Madurez no medible | `scripts/check_production_gates.py` + conteo de reglas `activada_en_ci` — **PENDIENTE: script no existe (B-49), health check F2.0 cubre coherencia** | % de reglas gateadas (baseline F0, sube cada fase) | La métrica se recalcula en cada fase |

---

## 4. Programa de estabilización — fases (objetivos, DOR, DOD, entregables, criterios de salida)

> Cada fase se cierra cuando su **criterio de salida** es verificable por comando. Un hallazgo crítico puede adelantarse a F1 siempre que lleve su test de regresión y actualice el yaml.

### F0 — Verificación de la auditoría (2–3 días) ✅ Cerrada (2026-08-06)

- **Objetivo:** confirmar/descartar cada hallazgo de `docs/audits/2026-08-auditoria-integral.md` con re-lectura; medir métricas en vivo (cobertura por módulo, conteos) para fijar umbrales **después**, nunca antes.
- **DOR:** informe base + repo en `dcd1741`.
- **Entregables:** tracking.yaml v2 con `estado_auditoria` decidido para todos; mediciones en vivo registradas (con comando y hash).
- **DOD:** 100 % de hallazgos con estado de auditoría; los `PARCIALMENTE_CONFIRMADO` resueltos o marcados como "requiere F0 para decidirse".
- **Criterio de salida (verificable):** `python -c "import yaml; d=yaml.safe_load(open('docs/plans/tracking.yaml')); assert all(h['estado_auditoria'] in {'CONFIRMADO','NO_CONFIRMADO','REFORMULADO'} for h in d['hallazgos'])"`.
- **Cierre:**
  - **22/22** hallazgos clasificados (`estado_auditoria` en enum; `PARCIALMENTE_CONFIRMADO = 0`); T1–T3 resueltos (H-09, H-15, H-16 → `CONFIRMADO`).
  - **Mediciones F0** registradas en `tracking` (`mediciones_f0`, hash `eaca97a`).
  - **Baseline vivo:** 900 tests · **44 %** cobertura · **49 contratos BC** · 25 tópicos · 52 237 LOC. Gate F0 **22/22**.

### F1 — Bloquear lo que causa pérdidas (≈1 semana) ✅ Cerrada (2026-08-06)

- **Objetivo:** eliminar caminos que con capital real causan daño (H-01, H-02, H-03, H-06, H-14 parcial).
- **DOR:** F0 cerrada; fixes de crítica con test de regresión.
- **Entregables:** reglas R1–R4 con `backtest: ok` y `activada_en_ci: true`; guard de arranque live; snapshot sin secrets; `pipeline_factory` corrige + smoke test.
- **DOD:** `uv run live` no arranca con stub; `assemble()` construye ohlcv+trades+derivatives; round-trip BUY→SELL con contador correcto; snapshot sin `SecretStr` en claro; CI bloquea R1–R4.
- **Criterio de salida:** `scripts/check_production_gates.py` → G1–G4 PASS — **PENDIENTE: script no existe (B-49), gate F1 validado por ruff + import-linter 49/49 + pytest 900 + mypy**.
- **Cierre (B-01…B-05 HECHO):**
  - **B-01/H-01** guard fail-closed en `assemble_live` (LiveExecutor `IS_STUB`).
  - **B-02/H-02** `pipeline_factory` crea catálogo Iceberg + guard R2.
  - **B-03/H-03** semántica held-position en OMS (`_fill` cierra en SELL) + round-trip R3.
  - **B-04/H-06** bloqueo `--cfg` en producción + redacción de secrets en snapshot (R4).
  - **B-05/H-14** `.dockerignore`, auth kafka-ui, binds loopback.
  - Gate F1: ruff ✓ · import-linter **49/49** ✓ · pytest **900 passed** ✓ · mypy ✓.

### F2 — Blindar calidad (F2.0 → F2.4, 1–2 semanas)

> F2 se subdivide en **F2.0–F2.4** para aislar frentes reversibles. Por la regla suprema (§14), **todo `main` debe pasar el Engineering Health Check (F2.0)** antes de ejecutar F2.1–F2.4. `tracking.yaml` guarda `fase` por issue (SS del backlog); este documento son mapas y no replica el backlog.

#### F2.0 — Engineering Health Check (gate de entrada)

- **Objetivo:** validar automáticamente la **coherencia** entre Plan Maestro ↔ `tracking` ↔ ADR ↔ contratos de arquitectura ↔ CI, antes de cada ejecución del resto de F2.
- **Entregables:** job CI `engineering-health` que comprueba en una sola pasada:
  1. YAML de `tracking.yaml` válido; enums del propio tracker; `fase` coherente con `estado`.
  2. Artefactos normativos: ADRs activos referenciados; contratos de arquitectura **>= 50** en vivo.
  3. No-vacío de `lint-imports`: salida **sin** `"Could not find…"` y conteo de contratos exigido (≥50).
  4. CI mapea a reglas: cada gate en `ocm-ci.yml` corresponde a una regla con `activada_en_ci: true`.
- **DOD:** `engineering-health` devuelve `PASS` **solo** cuando Plan↔tracking↔ADR↔contratos↔CI están alineados; `FAIL` bloquea el resto.
- **Criterio de salida:** job verde en CI, respaldado por un primer check de prueba en `tests/` y tracking sincronizado.

#### F2.1 — Blindaje de calidad (contract-linter no vacuo + gates)

- **Objetivo:** calidad automática y gateada (H-04, H-05, H-07, H-12, H-20, H-10); en particular **no-vacuo** el `contract-linter`: hoy un `--config` roto devuelve **salida 0** (falso verde).
- **DOR:** F2.0 verde; F1 cerrada; CI verde en `main`.
- **Entregables:** `fail_under` sobre medición en vivo; bandit en CI+pre-commit; Docker endurecido (`.dockerignore`, HEALTHCHECK, bindings); paridad de config; reglas R5–R8 activas.
- **DOD:** `fail_under > 0`; bandit `-ll` sin BLOCKER; mypy completo verde (o fallo documentado); `docker build` sin `.env` horneado; paridad config verde; `lint-imports` falla si el conteo de contratos baja de `50`.
- **Criterio de salida:** G5–G9 PASS; ADR-0020 (Production Gate como gate de release) aceptada.
- **Avance (2026-08-23):** audit_validator.py implementa M22–M25 (ADR-0031); policies/registry.yaml creado (PR #19). Contratos: 50 KEPT.

#### F2.2 — Gobernanza documental (ADR única SSOT)

- **Objetivo:** eliminar la colisión real entre `docs/architecture/0003-0005-*.md` (legacy) y `docs/architecture/decisions/ADR-0003…` (serie activa).
- **DOR:** F2.0 verde; F2.1 avanzada.
- **Entregables:** renombrar los legacy a `SUPERSEDED-00xx-*.md` con nota de sustitución por la ADR activa.
- **DOD:** `docs/architecture/decisions/` como único SSOT activo; cero legacy activos.
- **Criterio de salida:** legacy cerrados (0 colisión); Plan↔tracking coherentes.

#### F2.3 — Contratos Kafka (8 schemas, de 0 % a >0 %) — ⚠️ AVANZADA, verificar cierre formal

- **Objetivo:** elevar cobertura real de los 8 esquemas en `shared/kafka/schemas/` (liquidations, ohlcv, oi, orderbook, orders, positions, signals, trades) desde **0 %**.
- **DOR:** ADR-0013 (modelo de ingestión) aceptada; F2.2 cerrada.
- **Entregables:** tests parametrizados por esquema — round-trip serialización/deserialización, campos, tópico.
- **DOD:** cada esquema con casos positivos+negativos; cobertura de schemas > 0 % medida.
- **Criterio de salida:** los 8 tipos con tests; cobertura > 0.
- **Auditoría 2026-08-07 (cierre confirmado):** `pytest --cov=shared/kafka/schemas --cov=shared/kafka/provenance tests/kafka/` → **orderbook.py 100%, orders.py 100%, positions.py 100%, signals.py 100%, trades.py 100%, topics.py 100%, serializer.py 82%** (208 tests passed). Criterio de salida del DOD ("cobertura de schemas > 0%") **cumplido y superado**. Nota importante: el gate `fail_under = 40` de `pyproject.toml:283` es la cobertura **global** del repo (baseline F2.1/B-06/R5, 44% real) — no aplica a F2.3, que mide solo el módulo de schemas. No confundir ambos umbrales. **Pendiente formalizar:** actualizar `estado: HECHO` + `fecha_cierre` en `tracking.yaml` para el hallazgo correspondiente a F2.3.

#### F2.4 — Engineering health / alineación de backlog

- **Objetivo:** registrar la salud del proyecto en el SSOT operativo (`tracking.yaml`), candidato de los ítems #2/#3 de la nueva auditoría.
- **DOR:** F2.0 cerrado.
- **Entregables:** bloque `Engineering Health` en tracking (contratos 50, snapshot, comandos) sin duplicar el mapa del Plan; nota que documenta los `return True` benignos en OMS/`rebalance`.
- **DOD:** `tracking-consistency` valida el bloque en el snapshot; Plan↔tracking coherentes.
- **Criterio de salida:** job `tracking-consistency` verde (SSOT); G5/G6 documentados.

#### F2.5 — Protocol Discovery (metodología PDF; gate normativo antes de capital)

- **Objetivo:** institucionalizar la ingesta basada en evidencia como metodología permanente y
  única. Aquí la fase es **Protocol Discovery**; el artefacto normativo (ADR-0017) es el
  **Protocol Discovery Framework (PDF)**.
- **DOR:** F2.1 cerrado; ADR-0020 (Production Gate) aceptada; semilla de provenance en F2.3.
- **Entregables:** ADR-0017 (Protocol Discovery Framework con sus 14 componentes, Contract
  Provenance como punto 9); reubicar la ADR de estado de posiciones a **ADR-0021** (renumeración
  por colisión).
- **DOD:** `test_schema_provenance.py` como semilla operativa de los puntos 9/13; taxonomía
  PROTOCOL/DOCUMENTATION/UPSTREAM_LIBRARY/DOMAIN/ASSUMED normativa; Promotion Rule (14) definida.
- **Criterio de salida:** ADR-0017 aceptada y committeada; gate de capital: **F3 no envía órdenes
  reales hasta que Orders/Fills estén promovidos** (Provenance estable).
- **✔️ Gate de capital enforced en código (B-23, verificado 2026-08-07):** el gate que F2.5 exige
  está implementado como guard fail-closed en `composition_root.assemble_live`:
  `require_promoted("OrderFilledPayload", "OrderRejectedPayload")` antes de instanciar
  `LiveExecutor`, precediendo al guard `IS_STUB` (B-01). `shared/kafka/provenance.py` (SSOT
  `PROVIDENCE`) expone `is_promoted()`/`require_promoted()`; defensa en profundidad, no corrige un
  fallo actual (Orders/Fills ya en DOMAIN). Tests positivo/negativo en `test_composition_root.py`,
  ejecutados en CI (job `unit-tests`). B-23 cerrado en `tracking.yaml`.

#### F2.6 — Capacity Planning & Scalability Assessment (gate antes de tooling de escala)

- **Objetivo:** documentar la carga real que `market_data`/`trading` deben soportar hoy y a
  6–12 meses (exchanges, símbolos, tipos de dato por stream, frecuencia msg/s, cómputo por
  evento, presupuesto de latencia `paper` vs `live`, hardware disponible en `orangehouse`)
  antes de que cualquier decisión tecnológica de mayor complejidad (systemd vs. orquestador,
  particionado, Dagster/Flink, DuckDB) se tome por intuición o anticipación. Principio:
  crecimiento evolutivo y desacoplado — empezar con la solución más simple (`systemd` + Kafka
  + lógica actual) y escalar solo cuando la medición lo justifique.
- **DOR:** F2.5 cerrada (ADR-0017 aceptada); ADR-0022 commiteada (entrypoint `streaming`
  operativo o en implementación, ver F3).
- **Entregables:** tabla de capacidad completada (exchanges, símbolos totales, streams por
  símbolo, msg/s promedio y pico, latencia p50/p99 tolerable, CPU/RAM disponible vs. consumido
  por `streaming`); conclusión explícita sobre si el proceso único (`systemd`) es suficiente.
- **DOD:** tabla completa con fuente de cada medición (no estimaciones sin respaldo);
  conclusión documentada — "arquitectura de proceso único suficiente" o déficit específico
  identificado con el mecanismo mínimo suficiente propuesto para resolverlo.
- **Criterio de salida:** ningún entregable de **F5** (catalog remoto, streaming
  Dagster/Flink, decisión DuckDB) se implementa sin que este assessment demuestre,
  con métricas, que la carga excede lo que un solo servidor puede manejar; sirve además
  para revalidar retroactivamente si la elección `systemd` de F3/ADR-0022 sigue siendo
  adecuada una vez haya datos operativos reales.

**Hallazgos verificados (2026-08-07) — insumo para el Entregable de F2.6:**
`market_data` ya tiene la infraestructura de ingestión WS realtime construida
(`CompositionRoot.build_ws_producers()` → `WSProducerBundle`, runners de
Bybit/KuCoin), pero sin entrypoint operativo (`systemctl` sin unit activa).
`packages/market_data/main.py` es un servicio FastAPI YA desplegable, pero
gobierna un pipeline de ingestión **polling** hacia Bronze/Iceberg
(`/ohlcv/...`) — distinto y complementario al streaming WS, no debe
confundirse. `[project.scripts]` no registra `streaming` (solo ocm,
ocm-api, live, paper). El guard R14/H8 (`app_layer_guard.py`) no cubre un
futuro `streaming_hydra.py` por diseño (nombres hardcodeados a
`live_hydra.py`/`paper_hydra.py`; `_bootstrap.handle_sigterm` no es
asyncio-safe para un loop persistente). BC-10/BC-50 (import-linter) ya
cubren el invariante `market_data` ↔ `trading` sin requerir contrato nuevo.
Detalle completo: addendum de ADR-0022.

Sub-secuencia de trabajo dentro de F2.6 (tracking.yaml: `f2_6a`–`f2_6d`):
F2.6a capacidad teórica (sin despliegue) → F2.6b Streaming Entrypoint MVP
(`apps/app/cli/streaming_hydra.py`, canary 1 exchange/pocos símbolos) →
F2.6c capacidad empírica (canary bajo systemd) → F2.6d decisión de
escalabilidad (solo con evidencia).

- **Avance F2.6a (HECHO 2026-08-07):** `docs/planning/f2_6a-capacity-teorico.md` —
  modelo de carga de los 4 WS producers (orderbook/funding/oi/liquidations) para
  Bybit (3 símbolos PERP, canary) + KuCoin (diseño). Conclusión con fuente por celda:
  proceso único `streaming` + broker Kafka local **suficiente para el canary de F2.6b**
  (~160 msg/s pico / ~54 KB/s ingreso Kafka con overhead; promedio ~12 msg/s / ~4 KB/s;
  E2E p50 10–30 ms / p99 60–150 ms teórico). La suficiencia para **producción final se
  declara como NO evaluada aquí** — se valida con medición empírica y hardware real en
  F2.6c, antes de cualquier decisión de escala. Sin déficit para el canario; umbral de
  invalidación (>50–100 símbolos activos o lag/CPU en F2.6c) documentado como input de
  F2.6d. No requiere ADR nuevo (documento de capacidad, no decisión).
- **Avance F2.6b (HECHO 2026-08-08):** `apps/app/cli/streaming_hydra.py` — MVP del
  entrypoint streaming (F3.5b en tracking.yaml), reutiliza CompositionRoot de
  `market_data` (`build_ws_producers()` → `WSProducerBundle`), shutdown vía
  `loop.add_signal_handler` + `asyncio.Event`, sin composition root alternativo.
  983 tests; gates ruff/mypy/lint-imports 49/49. Pendiente operativo: unit systemd
  (`systemd_reinicia_correctamente` NO_VERIFICADO).
- **Avance F2.6c (HECHO 2026-08-08, entregable formalizado 2026-08-10):**
  `docs/planning/fase3.5c-capacity-empirico.md` — canary 30 min bajo arranque
  manual (Bybit, 3 símbolos PERP, 4 producers WS, depth 50). Medido:
  **138.5 msg/s (249,380 eventos), 0 errores, CPU 0.00 %, RAM 40.4 MB RSS,
  latencia procesamiento p50/p99 7.55/33.8 ms, heartbeat 139/139**. Todo muy por
  debajo del umbral de invalidación de F2.6a. Evidencia cruda completa en
  `artifacts/f26c/` (canary_30m.log, canary_cpu.csv, pushgateway_{10..30}min.txt).
- **Avance F2.6d (HECHO 2026-08-10):** decisión de escalabilidad con evidencia —
  **proceso único (`systemd`) + Kafka local suficiente; NO se crea ADR de escala**
  (la evidencia empírica no lo justifica). F5 (catalog remoto, Dagster/Flink,
  DuckDB) queda bloqueado salvo re-medición que cruce el umbral (>50–100 libros
  activos o lag/CPU en tensión). Tracking: `f2_6d_decision_escalabilidad`
  (estado HECHO, cierre 2026-08-10).
- **F2.6 (a–d) CERRADA (2026-08-10):** capacity assessment completo con evidencia
  empírica; criterio de salida cumplido — ningún entregable de F5 se implementa sin
  que este assessment demuestre con métricas que la carga excede un solo servidor.

### F3 — Completar funcionalidades (trading live, 1–2 meses)

- **Objetivo:** trading live **real** (H-01 resolución, H-19, H-22). Sin gobernanza aquí; la calidad se mantiene vía F2.0.
- **DOR:** F2.0 verde (Health Check CI); **ADR-0016 aceptada y commiteada** (Bybit, paper→live). ADR-0011 (rebalance) — originalmente movida a F4 para no bloquear el motor; **resuelta durante F3** (Aceptada 2026-08-07, B-13 HECHO).
- **Entregables:** `LiveExecutor` real sobre `OrderTransport` (create_order + reconciliación fail-closed + kill switch; reglas **R9–R10 activadas en CI**, job `trading-guards`); `RebalanceService.rebalance()` cableado; strategies a polars.
- **Avance:** [x] motor de ejecución (B-12 **HECHO** 2026-08-07, evidencia reproducible en tracking.yaml: `uv run pytest tests/trading/test_live_executor.py tests/trading/test_transport_mapping.py -q -m "not integration" --no-cov` → 14 passed; paper|live via `--mode`) · [x] rebalance (B-13: `RebalancePort` + `assemble_rebalance()` delegando en el port inyectado, wireado en `apps/app/use_cases/execute_live.py` y `execute_paper.py`; ADR-0011 **Aceptada** 2026-08-07) · [x] polars strategies (B-14, migrado a polars, evidencia: cero `import pandas` residual, 23 tests) · [x] **pandas→polars completo** (PR #19, 2026-08-23: `pandas_to_domain.py` → `dataframe_to_domain.py`, pandas eliminado de deps, 0 imports/0 `.to_pandas()`, 50 contratos KEPT).
- **DOD:** test de integración orden→fill→estado en sandbox/mock; `uv run live` real (o deshabilitado explícitamente en prod); rebalance end-to-end.
- **Criterio de salida:** G10–G11 candidatos; prueba de reconciliación documentada.

### F4 — Madurez de producción / Observabilidad (2–4 meses)

- **Objetivo:** consistencia de estado, trazabilidad, semántica de entrega (H-08, H-09, H-11, H-15, H-16, H-17, H-18) + **Observabilidad** (OTel + request-id; único SS de posiciones).
- **DOR:** F3 cerrada; ADR-0021, ADR-0018 en revisión.
- **Entregables:** estado de posición única (PortfolioService); UUID completo; OTel + request-id; evaluación/implementación Schema Registry; exactly-once (dedup + reintento); dominio sin `subprocess`; `RiskGate` alineado.
- **DOD:** una sola fuente de verdad de posiciones; traces end-to-end; schema evolution backward-probada; dedup con test de reintento; dominio 100 % puro.
- **Criterio de salida:** Production Gate release PASS completo.

### F5 — Escala (6+ meses)

- **Objetivo:** millones de eventos/día, multiworker (ADR-0019, ADR-0020, H-13 DuckDB).
- **DOR:** F4 cerrada; ADR-0019/0020 en revisión.
- **Entregables:** catalog Iceberg remoto (REST/Nessie/MinIO); streaming dedicado (Dagster/Flink) para Silver→Gold; decisión DuckDB (adoptar con ADR o eliminar).
- **DOD:** catalog remoto en staging; pipelines fuera del feed; benchmarks documentados.

### Mapa Fase ↔ Hallazgos (gobernanza; SSOT de estado en `tracking.yaml`)

> `tracking.yaml` mantiene su `fase` por hallazgo (SS backlog). Este mapa indica qué hallazgos/backlog se resuelven en cada fase del Plan — no reasigna `fase`; es informativo.

| Fase | Hallazgos / Backlog | ADR / reglas | Nota |
|---|---|---|---|
| F2.0 | — | `engineering-health` (nueva) | gate previo |
| F2.1 | B-06, B-07, B-10, **B-47..B-56** (H-04, H-05, H-07, H-12, H-20, H-10; Policy Layer: ruff complexity, vulture, Production Gate, Policy Registry, AI Governance, Semgrep, SonarQube, complexity strategy) | ADR-0020, ADR-0021, ADR-0023, ADR-0024, ADR-0025, ADR-0026, R5–R8 | contratos no-vacuos + Policy Layer |
| F2.2 | B-13 (legacy ADR 0003–0005) | ADR-0003..0015 SSOT | renaming |
| F2.3 | trabajo relacionado a B-18 (H-15, backlog en F4) | ADR-0013, ADR-0018 | 8 schemas Kafka — prerrequisito de B-18 |
| F2.4 | B-20, B-21 | — | tracking-consistency |
| F2.5 | trabajo relacionado a B-18 (H-15, provenance; backlog en F4) | ADR-0017 (Protocol Discovery), ADR-0021 (ex-0017, posición) | gate normativo antes de capital |
| F3 | B-12, B-01, B-03, B-13 (H-01, H-19, H-22) | ADR-0016 (aceptada), ADR-0011 (aceptada) | trading live — Bybit + rebalance |
| F4 | B-15, B-16, B-17, B-18, **B-57, B-58** (H-08, H-17, H-18; CD, Grafana) | ADR-0021, ADR-0018, ADR-0027, ADR-0028 | obs/estado + CD |
| F5 | B-22, H-13 | ADR-0019, ADR-0020 | escala |

---

## 5. ADRs — decisiones de arquitectura enlazadas

> **Principio 5.** Guard de numeración obligatorio: `ls docs/architecture/decisions/` antes de escribir cualquier ADR. Nunca asumir número (incidente real de colisión de dos series).

### Verificación de numeración (2026-08-06)

`ls docs/architecture/decisions/ADR-*.md` + `cat` de títulos:

- **ADR-0003** TradingCompositionRoot angosto (sub-configs, no AppConfig)
- **ADR-0004** BC-47: TradingCompositionRoot único punto que importa market_data (BC-50)
- **ADR-0005** TradingEngine construye internos; el CR los externos — **sustituido por ADR-0012**
- **ADR-0006** Portfolio es el único dueño del estado de posiciones
- **ADR-0007** Equivalencia de capas por BC (no forzar naming uniforme)
- **ADR-0008** Contrato de capas para portfolio (bootstrap → infra → services → ports → models)
- **ADR-0009** Eliminar FillHandler y TradeHistory huérfanos (superados por fill_sync.py)
- **ADR-0010** Gobernanza automatizada del Shared Kernel
- **ADR-0011** Rebalance — **Aceptada** (2026-08-07: delegación vía `RebalancePort`; assemble_rebalance wireado)
- **ADR-0012** TradingEngine runtime puro; el CR ensambla todo
- **ADR-0013** Modelo unificado de ingestión de datos (feed, fuente, mecanismo) — **commiteada**
- **ADR-0014** Diseño interno de market_data — Market Data Platform (realtime_feeds + external_ingestion) — **commiteada**; implementación parcial en repo (commits `fb7df84`, `6165c11`, `e6cf272`, `dcd1741`: esqueleto `external_ingestion/` con puertos, orquestador, normalizers, config, wiring)
- **ADR-0015** Blindaje de la Application Layer — guard AST + contratos BC-53/54 (serie `AUDIT-apps-2026-08-03#Hx`) — **commiteada y aceptada** (`a48f28e`)

> **Resultado:** el siguiente número libre real es **ADR-0016**. Toda ADR nueva se crea con la plantilla `ADR-template.md` y se re-verifica la numeración al momento de crearla (lección confirmada: el ADR-0015 asumido como "LiveExecutor" en borradores previos quedó ocupado por el blindaje de apps — el guard de numeración existe precisamente para esto).

### ADRs propuestas (estado Propuesto; se crean en su fase tras re-verificar numeración)

> **Nota de numeración (2026-08-19):** ADRs existentes llegan a 0030. Huecos libres: 0018, 0019. Siguiente secuencial: 0031. **Guard obligatorio:** `ls docs/architecture/decisions/` al crear cada ADR — nunca asumir número.

| ADR (tentativo) | Tema | Fase | Enlaza hallazgos | Guard de numeración |
|---|---|---|---|---|
| ADR-0018 | Schema Registry (Avro + compatibilidad backward) | F2.3 / F4 | H-15, B-18 | Verificar al crear (hueco libre) |
| ADR-0019 | Catálogo Iceberg remoto (REST/Nessie) | F5 | — | Verificar al crear (hueco libre) |
| ADR-0020 | Production Gate como gate de release | F2.1 | B-06, B-07 | **Aceptada + commiteada** (2026-08-06) |
| — | *(semántica `_open_positions` cubierta por ADR-0016)* | — | H-03, B-03 | — |
| ADR-0031 | Policy Registry YAML (extensión tracking.yaml + M21..M25) | F2.1 | B-51, B-55, B-56 | Verificar al crear |
| ADR-0032 | AI Agent Governance (branch protection, CODEOWNERS, evidence hash, waiver/expiración) | F2.1 | B-52, B-56 | Verificar al crear |
| ADR-0033 | Production Gate binario (check_production_gates.py G1..G11) | F2.1 | B-49 | Verificar al crear |
| ADR-0034 | Semgrep adoption (non-blocking, arquitectura/policy) | F2.1 | B-53 | Verificar al crear |
| ADR-0035 | SonarQube decision (NOT JUSTIFIED — coste operacional) | F2.1 | B-54 | Verificar al crear |
| ADR-0036 | vulture/complexity strategy (ruff C901/PLR/SIM + vulture CI) | F2.1 | B-47, B-48 | Verificar al crear |
| ADR-0037 | Artifact digest/signature + CD verify/deploy/rollback | F4 | B-57 | Verificar al crear |
| ADR-0038 | Grafana provisioning versionado | F4 | B-58 | Verificar al crear |

---

## 6. Production Gate — criterio objetivo de "apto para producción"

> **Principio 8.** El Production Gate es el criterio objetivo. No es opinión: cada cheque mapea a una regla o hallazgo y es ejecutable.

### Cheques (v1)

| Cheque | Fuente | Umbral | Fase de activación |
|---|---|---|---|
| G1. Sin LiveExecutor stub | AST guard R1 | `uv run live` no arranca con stub | F1 |
| G2. Composition root construye todas las pipelines | smoke test R2 | verde | F1 |
| G3. Contador de riesgo correcto | round-trip R3 | verde | F1 |
| G4. Secrets redactados en snapshot | test R4 | verde | F1 |
| G5. Contratos BC válidos | `lint-imports` | verde (conteo en vivo) | F2 |
| G6. Cobertura crítica | `pytest --cov` | **definido tras medición en vivo en F0** | F2 |
| G7. Bandit integrado y limpio | CI `security` | sin BLOCKER | F2 |
| G8. Mypy completo en CI | CI `quality` | sin errores | F2 |
| G9. Paridad de config | test R7 | verde | F2 |
| G10. Estado de posición único | test B-15 | verde | F4 |
| G11. Trazabilidad activa | test B-17 | verde | F4 |

- **Veredicto binario:** `scripts/check_production_gates.py` → PASS/FAIL con reporte por cheque — **PENDIENTE: script no existe (B-49); veredicto actual: engineering_health_check.py + jobs CI (import-linter, bandit, mypy, pytest, app-guard, domain-guard, trading-guards)**.
- **Dos modos:** `gate-dev` (todo PR a `main`) y `gate-release` (candidatos de release, manual).
- **Regla:** FAIL en `gate-release` bloquea el merge del candidato. FAIL en `gate-dev` bloquea el PR.
- **Mecanismo de longevidad:** un cheque solo se añade con su test+backtest; un cheque solo se **desactiva** con ADR y evidencia, nunca por conveniencia.

---

## 7. Backlog maestro y tracking.yaml v2

> **Principio 6.** tracking.yaml v2 es la **fuente operativa** (consumible por máquinas). Este documento es el mapa; **no repite los datos** — la tabla completa vive solo en el YAML.

### Dónde están los datos (nada duplicado)

| Artefacto | Contenido | Rol |
|---|---|---|
| `docs/plans/tracking.yaml` | Estado real de los 22 hallazgos (backlog + cadena de trazabilidad completa) y las 16 reglas auto-defendibles (backtest + `activada_en_ci`) | **SSOT operativo** |
| Este documento (§2) | La **cadena** (cómo funciona el backlog) y **un ejemplo transversal** (B-03) | Mapa / filosofía |
| `docs/audits/2026-08-auditoria-integral.md` | La **evidencia original** de cada hallazgo (fotografía inmutable de `dcd1741`) | Historial |

### Vista conceptual del backlog (cómo funciona, no qué hay dentro)

El backlog NO es una lista estática: es la materialización de la cadena de trazabilidad de §2. Cada hallazgo `B-NN` es un registro en `tracking.yaml` que contiene:

```
B-NN:
  hallazgo_informe   # H-NN del informe base
  fase               # F1..F5 — en qué ventana se resuelve
  prioridad          # CRITICA | ALTA | MEDIA | BAJA
  estado             # PENDIENTE | EN_CURSO | HECHO | VERIFICACION | RECHAZADO
  estado_auditoria   # CONFIRMADO | NO_CONFIRMADO | PARCIALMENTE_CONFIRMADO | REFORMULADO
  evidencia          # cita archivo:módulo:función (no opinión)
  solucion, pruebas, adr_relacionado, riesgo_residual, fecha_cierre
  cadena             # { hallazgo, backlog, adr, implementacion, tests, ci, evidencia, cierre }
```

**Cómo leerlo:** un ingeniero sabe el estado exacto de cualquier problema leyendo `estado` + los 8 eslabones de `cadena`; las reglas (`reglas:`) indican qué automatización ya lo protege (`backtest` + `activada_en_ci`). Los `*` glob/sustitución `B-NN` no existen: cada ID es único y global.

### Verificación automática de consistencia (CI)

- **Job `tracking-consistency` (F2):** valida el YAML en cada PR — parseo, enums (los declarados en el propio `tracking.yaml`), `hallazgo_informe` no vacío, `estado: HECHO` → `fecha_cierre` + `cadena.cierre.evidencia`, `estado_auditoria: CONFIRMADO` → evidencia no vacía, `backtest: ok` → `activada_en_ci: true`.
- **Métrica de madurez (§11):** % de reglas `activada_en_ci: true` / total se lee del YAML en vivo (baseline en F0, sube cada fase).
- **Regla:** todo cambio de estado del backlog se hace **editando el YAML**, nunca el `.md`; el `.md` no almacena la columna Estado.

---

## 8. Estrategia de commits (cambios pequeños y reversibles)

- **Principio 7.** Commits atómicos en `main` (conventional commits: `fix/feat/refactor/test/docs`).
- **Una regla = tres commits separados y revisables:**
  1. `test(rules): R# — test positivo y negativo` (la regla aún no está activa; el positivo falla demostrando el defecto)
  2. `feat(rules): R# — regla automática` (activa; ambos tests pasan)
  3. `docs(plans): R# — estado en tracking.yaml`
- **Nunca** mezclar regla y fix en un mismo commit (un revert no debe dejar la regla huérfana).
- Cada fix de hallazgo lleva su test de regresión positivo+negativo **en el mismo PR**.
- Pre-commit hooks (ruff, format, import-linter, mypy, ssot-enums) siempre activos; si modifican archivos → `git add -u && git commit` (nunca `--no-verify`).
- CI fail-fast (`architecture` → unit/integration/config/quality) debe pasar antes de avanzar de fase.

---

## 9. Scorecard evolutivo (estado → objetivo → acciones → evidencia)

> **Principios 3, 9, 10.** Los puntajes se recalculan al cierre de cada fase (no se congelan). Cada punto ganado exige **evidencia de cheque** (qué gate/regla lo demuestra). Baseline: `docs/audits/2026-08-auditoria-integral.md` §7 (2026-08-06).

| Eje | Actual | Objetivo | Acciones (mapeo) | Evidencia de cada punto ganado |
|---|---|---|---|---|
| Preparación producción | 3/10 | 8/10 | F1 (B-01…B-05) + F2 (gates) + F3 (live real) | Production Gate release PASS (G1–G9) |
| Seguridad | 4/10 | 8/10 | B-04, B-05, B-07, R6, G7 | bandit limpio en CI; snapshot sin secrets; puertos restringidos |
| Testing | 6/10 | 8/10 | B-06, R5, F3/F4 tests integración/reconciliación | `fail_under` activo y sostenido sobre medición en vivo |
| Observabilidad | 6/10 | 8/10 | B-17, G11 | traces end-to-end + correlación request-id |
| Escalabilidad | 5/10 | 7/10 | ADR-0019/0020, H-13 | catalog remoto en staging; benchmarks documentados |
| Mantenibilidad | 7/10 | 9/10 | tracking.yaml + production gate + ADR-0020 | CI valida tracking; docs de conteo generadas |
| DDD | 7/10 | 8/10 | B-03 (semántica risk), B-13, B-15 | tests de invariante de estado |
| Clean Architecture | 8/10 | 9/10 | B-11, B-20, B-21 | AST guards de pureza (R11, R8) |
| Hexagonal | 8/10 | 9/10 | B-12, B-21 | guards de ports/adapters |
| Event Driven | 6/10 | 8/10 | B-18, B-19 | schema evolution + dedup probados |
| Configuración | 8/10 | 9/10 | B-09, B-04 | secrets redactados (B-04, HECHO) — ⚠️ paridad de config (B-09) sigue PENDIENTE en tracking.yaml (auditoría 2026-08-07); el 8/10 actual no debería atribuirse a B-09 hasta su cierre |
| Performance | 5/10 | 7/10 | B-14, F5 benchmarks | benchmarks documentados |

> **Regla del scorecard:** un punto se considera ganado **solo** cuando existe la evidencia de la columna derecha (cheque verde con fecha+commit). Hasta entonces, el puntaje no sube — coherencia con el principio "evidencia, no intención".

---

## 10. Umbrales — definidos únicamente tras medición en F0

> **Principio 9.** Ningún umbral numérico se fija antes de la medición en vivo en F0. Precedente: una auditoría previa reportó **13 %** de cobertura desde un `.coverage` stale; la medición en vivo dio **43 %**. Fijar ≥70 % a ciegas repetiría ese error.

**Qué se mide en F0 (antes de fijar nada):**

| Umbral pendiente | Se define en | Método de medición |
|---|---|---|
| G6 cobertura crítica | F2 (tras F0) | `pytest --cov` sobre `trading/execution` y `storage/iceberg` (medición en vivo) |
| `fail_under` | F2 | **FIJADO: 40** (baseline medido 44%, margen 4pts; subir gradualmente en PRs) |
| Bandit severidad mínima | F2 | ejecutar `bandit -ll -r ...` y clasificar hallazgos reales — **FIJADO: sin BLOCKER (0 HIGH)** |
| Scorecard (puntajes) | cierre de cada fase | re-ejecución de gates y mediciones |

Todo valor fijado queda registrado en tracking.yaml con el comando y el hash de commit que lo produjo.

---

## 11. Lifecycle del sistema — longevidad

> **Principio 10.** El sistema debe sobrevivir a sus autores.

- **Métrica de madurez:** % de reglas de arquitectura/calidad gateadas por CI (`activada_en_ci: true` / total) vs las que dependen de revisión manual. Baseline medido en F0; sube en cada fase, nunca baja.
- **Rotación:** cualquier ingeniero nuevo entra por §1 (Cómo usar este documento), navega la cadena de §2 con un ejemplo real, y sabe el estado exacto del sistema leyendo tracking.yaml. No hay conocimiento implícito: todo lo que no está en este documento o en el yaml **no es normativo**.
- **Regla de oro del sistema:** una regla no se activa en CI sin su backtest `ok`; un hallazgo no se cierra sin evidencia de cheque; un umbral no se fija sin medición en vivo; una ADR no se numera sin `ls` previo.

---

## 12. Artefactos normativos (Constitución)

> Toda evolución técnica debe respetar estos artefactos. Son la **Constitución** que invoca la Regla suprema (preamble). Cada uno tiene un rol y una ubicación única (SSOT). Al degradar cualquiera de ellos, la funcionalidad nueva queda fuera de `main`.

| # | Artefacto | Rol | Ubicación | Nota |
|---|---|---|---|---|
| N1 | Plan Maestro | Especificación normativa del cambio; fases + DOR/DOD | `docs/PLAN-Maestro-Ingenieria.md` | SSOT documental |
| N2 | tracking.yaml | SSOT operativo del backlog y hallazgos | `docs/plans/tracking.yaml` | SSOT de `fase`/`estado` |
| N3 | ADRs | Decisiones de arquitectura (activo) | `docs/architecture/decisions/ADR-*.md` | único SSOT, sin legacy |
| N4 | Contratos de arquitectura | Boundaries/capas (BC-NN) | `architecture/importlinter.toml` | gate CI, ≥ 50 en vivo |
| N5 | Contratos de código | Guards AST / invariantes | `tests/architecture/` | gate CI |
| N6 | CI | Puerta del cambio | `.github/workflows/ocm-ci.yml` | fail-fast |
| N7 | Auditorías | Fotografías históricas (inmutables) | `docs/audits/` | no se editan |

### Jerarquía de autoridad (decreciente)

`Plan (N1) → Tracking (N2) → ADR (N3) → Contratos (N4/N5) → Código → Tests → CI → Release`

> Cuando N1 y N2 divergen, **N2 gana** para el estado del backlog; cuando N3 y el código divergen, gana N3 (y se abre un hallazgo). La coherencia entre todos es exactamente lo que valida el **Engineering Health Check (F2.0)**.

---

## 13. Ingeniería Continua (Continuous Engineering)

> Workflow por defecto para todo cambio. Respeta la cadena maestra §2 y siempre termina registrado en `tracking.yaml`.

1. **Hallazgo** — defecto/oportunidad registrado en `tracking.hallazgos` con `estado_auditoria` y evidencia.
2. **Tracker** — entra al backlog (ID `B-NN`, `fase`, `prioridad`).
3. **RFC/ADR** — si es decisión, crear o actualizar el ADR (guard de numeración §5).
4. **Implementación** — commits atómicos (una regla = test→fix→docs, §8).
5. **Tests** — regresión positivo+negativo en el mismo PR.
6. **CI** — gates exigidos (import-linter, mypy, bandit, tracking-consistency, health).
7. **Engineering Health Check** — (F2.0) valida coherencia Plan↔tracking↔ADR↔contratos↔CI.
8. **Release** — merge a `main`; gate release opcional (Production Gate §6).
9. **Cierre** — `estado: HECHO` con `fecha_cierre` y evidencia de cheque.

> Cualquier funcionalidad nueva que no pueda pasar este flujo **no se implementa** (regla suprema). Ningún paso se salta.

---

## 14. Registro de cambios

| Fecha | Commit | Cambio |
|---|---|---|
| 2026-08-06 | (baseline `dcd1741`) | Creación como especificación SSOT del sistema de ingeniería; tracking.yaml v2; verificación de numeración ADR — ADR-0015 quedó ocupado por el blindaje de apps (`a48f28e`), siguiente libre: ADR-0016 |
| 2026-08-06 | (F1 cerrada) | Cierre F0 (22/22 clasificados) y F1 (B-01…B-05 HECHO); reestructura §4 en F2.0–F2.4 (Engineering Health Check), F3 trading-only, F4 Observabilidad, F5 Escala; **Regla suprema** (preamble); §§13–14 Artefactos Normativos + Ingeniería Continua; **Mapa Fase ↔ Hallazgos**; tracking.yaml se mantiene SSOT de `fase` (sin reasignar hallazgos) |
| 2026-08-06 | (`397459e`) | **F2.0 ACTIVADO**: `scripts/engineering_health_check.py` + job CI `engineering-health` + pytest gate; valida Plan↔tracking↔ADR↔contratos↔CI (fail-fast). Decisiones F3: exchange inicial **Bybit** (único, paper→live siempre); **ADR-0011 → F4** (no bloquea F3). |
| 2026-08-06 | (`5090245`, `e04f38d`) | **F3 motor de ejecución**: ADR-0016 aceptada; `OrderTransport` (port), `LiveExecutor` real (reconciliación fail-closed + kill switch + `_notional_qty`), `CCXTAdapter.create_order/fetch_order`, adaptador `_BybitTransport` (BC-50) en composition_root, modo `--mode paper\|live` en `uv run live`; reglas **R9–R10** activadas en CI (job `trading-guards`). Queda F3: rebalance (B-13) y polars strategies (B-14). |
| 2026-08-06 | (auditoría de calidad, sesión posterior) | Corrección de consistencia documental del mapa Fase ↔ Hallazgos: B-14 removido de la fila F5 (tracking.yaml lo registra como F3 / HECHO). Las referencias a B-18 en F2.3 y F2.5 se reemplazan por "trabajo relacionado / prerrequisitos de H-15", manteniendo F4 como única fase oficial de B-18 según tracking.yaml (SSOT). Sin cambios en tracking.yaml ni ADRs. |
| 2026-08-19 | (consolidación post-auditorías) | **Consolidación documental completa** tras auditorías Policy Layer (feasibility + complementary + adversarial): tracking.yaml actualizado con B-47..B-60 (Policy Layer findings); Plan Maestro corregido: check_production_gates.py marcado PENDIENTE (B-49), ruff config E/F/I only (B-47), vulture installed not enforced (B-48), CodeQL/Trivy PR+weekly (B-60), fail_under=40 baseline 44% fijado; ADRs propuestas ADR-0021..0028 añadidas; Mapa Fase↔Hallazgos extendido; §6 Production Gate y §10 Umbrales corregidos; §3 Principio 10 corregido. |
| 2026-08-23 | (`c392f8f`, PR #19) | **pandas→polars MIGRATION COMPLETE**: `pandas_to_domain.py` → `dataframe_to_domain.py`; pandas eliminado de `pyproject.toml`; 0 imports, 0 `.to_pandas()` en todo el repo; `pandera` (polars mode) para schema validation; contratos 49→50 KEPT; audit_validator M22–M25 implementado (ADR-0031); `policies/registry.yaml` creado; ccxt 4.3.58→4.5.74 (CVE fixes). AGENTS.md §"Active migration" → "COMPLETE". |
| 2026-08-24 | (B-56) | **B-56 CERRADO**: CI stage ordering — policy-gate job added to ocm-ci.yml; depends on all 9 existing jobs; runs engineering_health + audit_validator M1..M25; integration-tests now depends on policy-gate; CI DAG: architecture+engineering-health → guards/tests/quality → policy-gate → integration; all existing required checks preserved. |

> Actualización de numeración: ADR-0015 real (blindaje Application Layer, serie `AUDIT-apps-2026-08-03#Hx`) se commiteó con ese número; las propuestas que este documento asignaba a ADR-0015–0019 se desplazan a **ADR-0016–0020** (ver §5).
