# PLAN-Maestro-Ingenieria.md

**OrangeCashMachine — Especificación del Sistema de Ingeniería (SSOT)**

- **Estado:** Activo — documento operativo vivo (no un PDF). El historial se preserva en Git, no en versiones de archivo.
- **Rol:** fuente de verdad (SSOT) para la evolución técnica del proyecto.
- **Alineado con:** `INFORME-Auditoria.md` (raíz) — fotografía histórica del estado en commit `dcd1741` (2026-08-06 00:08:04 -0500), **inmutable**.
- **Datos operativos:** `docs/plans/tracking.yaml` (v2) — fuente de verdad **por máquina**; este documento es el **mapa** que explica cómo funciona el sistema, no el tracker.
- **Métricas baseline (medidas en vivo 2026-08-06, no stale):** 844 tests / 43 % cobertura / 47 contratos BC / 44 constantes de tópicos Kafka / 52 429 LOC Python.

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
| Verificar si el sistema es "producción-ready" | Ejecutar `scripts/check_production_gates.py` (veredicto binario PASS/FAIL) | §6 |
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

1. **Hallazgo** — `INFORME-Auditoria.md` H-03: `packages/trading/execution/oms.py:172` llama `record_open()` en todo `submit()`; `record_close()` solo en `cancel()` (`:217`) y `_reject()` (`:308`); `_fill()` (`:270-289`) no decrementa → un ciclo BUY→SELL deja `_open_positions` inflado. **Evidencia:** lectura directa de `oms.py` y `risk/manager.py:126-139`. `estado_auditoria: CONFIRMADO`.
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
| 2 | Automatización > disciplina | "Recordar revisar X" falla | import-linter (47 BC), AST guards (`tests/architecture/`), mypy, bandit, CI gates | Comando que da FAIL ante la violación | Las reglas son propiedad del repo, se ejecutan en cada PR |
| 3 | Evidencia verificable | "Mejoramos" sin demostrarlo | Cada objetivo = cheque medible (no intención) | Salida del cheque + fecha + commit | Cualquier afirmación sin cheque se marca **No verificado** |
| 4 | ADR para decisiones | Arquitectura implícita | Plantilla `ADR-template.md` + guard de numeración | ADR con estado; enlace en yaml | Guard de numeración en cada creación |
| 5 | DOR/DOD por tarea | Trabajo "en progreso" infinito | Criterios de entrada/salida por fase (§4) | Estado en yaml + cheque del DOD | DoD con comando verificable |
| 6 | Backlog único | Backlogs por auditoría | tracking.yaml consolida H-*, R* y B-* | Un solo tracker | Fuente única; los .md no duplican |
| 7 | Cambios pequeños y reversibles | Diffs imposibles de revertir | Commits atómicos (§8) | Git history con 1 cambio lógico/commit | Hooks de pre-commit siempre activos |
| 8 | CI como puerta, no sugerencia | Merges rotos | Gates reales (fail-fast en `ocm-ci.yml`) | CI rojo bloquea merge | Ningún merge a `main` con CI rojo |
| 9 | Umbrales tras medición | Números inventados (13% stale vs 43% real) | Medición en vivo en F0 antes de fijar umbrales | Mediciones con fecha/commit | §10: umbrales solo tras F0 |
| 10 | Sistema que se audita solo | Madurez no medible | `scripts/check_production_gates.py` + conteo de reglas `activada_en_ci` | % de reglas gateadas (baseline F0, sube cada fase) | La métrica se recalcula en cada fase |

---

## 4. Programa de estabilización — fases (objetivos, DOR, DOD, entregables, criterios de salida)

> Cada fase se cierra cuando su **criterio de salida** es verificable por comando. Un hallazgo crítico puede adelantarse a F1 siempre que lleve su test de regresión y actualice el yaml.

### F0 — Verificación de la auditoría (2–3 días)

- **Objetivo:** confirmar/descartar cada hallazgo de `INFORME-Auditoria.md` con re-lectura; medir métricas en vivo (cobertura por módulo, conteos) para fijar umbrales **después**, nunca antes.
- **DOR:** informe base + repo en `dcd1741`.
- **Entregables:** tracking.yaml v2 con `estado_auditoria` decidido para todos; mediciones en vivo registradas (con comando y hash).
- **DOD:** 100 % de hallazgos con estado de auditoría; los `PARCIALMENTE_CONFIRMADO` resueltos o marcados como "requiere F0 para decidirse".
- **Criterio de salida (verificable):** `python -c "import yaml; d=yaml.safe_load(open('docs/plans/tracking.yaml')); assert all(h['estado_auditoria'] in {'CONFIRMADO','NO_CONFIRMADO','REFORMULADO'} for h in d['hallazgos'])"`.

### F1 — Bloquear lo que causa pérdidas (≈1 semana)

- **Objetivo:** eliminar caminos que con capital real causan daño (H-01, H-02, H-03, H-06, H-14 parcial).
- **DOR:** F0 cerrada; fixes de crítica con test de regresión.
- **Entregables:** reglas R1–R4 con `backtest: ok` y `activada_en_ci: true`; guard de arranque live; snapshot sin secrets; `pipeline_factory` corrige + smoke test.
- **DOD:** `uv run live` no arranca con stub; `assemble()` construye ohlcv+trades+derivatives; round-trip BUY→SELL con contador correcto; snapshot sin `SecretStr` en claro; CI bloquea R1–R4.
- **Criterio de salida:** `scripts/check_production_gates.py` → G1–G4 PASS.

### F2 — Blindar calidad (1–2 semanas)

- **Objetivo:** calidad automática y gateada (H-04, H-05, H-07, H-12, H-20, H-10).
- **DOR:** F1 cerrada; CI verde en `main`.
- **Entregables:** `fail_under` sobre medición en vivo; bandit en CI+pre-commit; mypy sobre todos los paquetes; Docker endurecido (`.dockerignore`, HEALTHCHECK, binds); test de paridad config; reglas R5–R8 activas.
- **DOD:** `fail_under > 0`; bandit `-ll` en CI sin BLOCKER; mypy completo verde (o fallo documentado); `docker build` sin `.env` horneado; paridad config verde.
- **Criterio de salida:** G5–G9 PASS; ADR-0020 (Production Gate como gate de release) aceptada.

### F3 — Completar funcionalidades (1–2 meses)

- **Objetivo:** trading live **real** (H-01 resolución, H-19, H-22).
- **DOR:** F2 cerrada; **ADR-0016** aceptada; **ADR-0011** decidida.
- **Entregables:** `LiveExecutor._submit()` con `CCXTAdapter.create_order` + reconciliación de fills; `RebalanceService.rebalance()` cableado; strategies a polars; reglas R9–R10.
- **DOD:** test de integración orden→fill→estado en sandbox/mock; `uv run live` real (o deshabilitado explícitamente en prod); rebalance end-to-end.
- **Criterio de salida:** G10–G11 candidatos; prueba de reconciliación documentada.

### F4 — Madurez de producción (2–4 meses)

- **Objetivo:** consistencia de estado, trazabilidad, semántica de entrega (H-08, H-09, H-11, H-15, H-16, H-17, H-18).
- **DOR:** F3 cerrada; ADR-0017, ADR-0018 en revisión.
- **Entregables:** estado de posición único (PortfolioService); UUID completo; OTel + request-id; evaluación/implementación Schema Registry; exactly-once (dedup + reintento); dominio sin `subprocess`; `RiskGate` alineado.
- **DOD:** una sola fuente de verdad de posiciones; traces end-to-end; schema evolution backward-probada; dedup con test de reintento; dominio 100 % puro.
- **Criterio de salida:** Production Gate release PASS completo.

### F5 — Escala (6+ meses)

- **Objetivo:** millones de eventos/día, multiworker (ADR-0019, ADR-0020, H-13 DuckDB).
- **DOR:** F4 cerrada; ADR-0019/0020 en revisión.
- **Entregables:** catalog Iceberg remoto (REST/Nessie/MinIO); streaming dedicado (Dagster/Flink) para Silver→Gold; decisión DuckDB (adoptar con ADR o eliminar).
- **DOD:** catalog remoto en staging; pipelines fuera del proceso de feed; benchmarks documentados.

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
- **ADR-0011** Rebalance — **decisión pendiente** (assemble_rebalance)
- **ADR-0012** TradingEngine runtime puro; el CR ensambla todo
- **ADR-0013** Modelo unificado de ingestión de datos (feed, fuente, mecanismo) — **commiteada**
- **ADR-0014** Diseño interno de market_data — Market Data Platform (realtime_feeds + external_ingestion) — **commiteada**; implementación parcial en repo (commits `fb7df84`, `6165c11`, `e6cf272`, `dcd1741`: esqueleto `external_ingestion/` con puertos, orquestador, normalizers, config, wiring)
- **ADR-0015** Blindaje de la Application Layer — guard AST + contratos BC-53/54 (serie `AUDIT-apps-2026-08-03#Hx`) — **commiteada y aceptada** (`a48f28e`)

> **Resultado:** el siguiente número libre real es **ADR-0016**. Toda ADR nueva se crea con la plantilla `ADR-template.md` y se re-verifica la numeración al momento de crearla (lección confirmada: el ADR-0015 asumido como "LiveExecutor" en borradores previos quedó ocupado por el blindaje de apps — el guard de numeración existe precisamente para esto).

### ADRs propuestas (estado Propuesto; se crean en su fase tras re-verificar numeración)

| ADR | Tema | Fase | Enlaza hallazgos | Guard de numeración |
|---|---|---|---|---|
| ADR-0016 | LiveExecutor real + reconciliación de fills + **semántica del contador de posiciones** (`_open_positions`) | F3 (guard en F1) | H-01, H-03, B-01, B-03, B-12 | Verificar al crear (libre tras ADR-0015 real) |
| ADR-0017 | Unificación del estado de posiciones | F4 | H-09, B-15 | Verificar al crear |
| ADR-0018 | Schema Registry (Avro + compatibilidad backward) | F4 | H-15, B-18 | Verificar al crear |
| ADR-0019 | Catálogo Iceberg remoto (REST/Nessie) | F5 | — | Verificar al crear |
| ADR-0020 | Production Gate como gate de release | F2 | B-06, B-07 | Verificar al crear |
| — | *(semántica `_open_positions` cubierta por ADR-0016)* | — | H-03, B-03 | — |

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

- **Veredicto binario:** `scripts/check_production_gates.py` → PASS/FAIL con reporte por cheque.
- **Dos modos:** `gate-dev` (todo PR a `main`) y `gate-release` (candidatos de release, manual).
- **Regla:** FAIL en `gate-release` bloquea el merge del candidato. FAIL en `gate-dev` bloquea el PR.
- **Mecanismo de longevidad:** un cheque solo se añade con su test+backtest; un cheque solo se **desactiva** con ADR y evidencia, nunca por conveniencia.

---

## 7. Backlog maestro y tracking.yaml v2

> **Principio 6.** tracking.yaml v2 es la **fuente operativa** (consumible por máquinas). Este documento es el mapa; **no repite los datos** — la tabla completa vive solo en el YAML.

### Dónde están los datos (nada duplicado)

| Artefacto | Contenido | Rol |
|---|---|---|
| `docs/plans/tracking.yaml` | Estado real de los 22 hallazgos (backlog + cadena de trazabilidad completa) y las 11 reglas auto-defendibles (backtest + `activada_en_ci`) | **SSOT operativo** |
| Este documento (§2) | La **cadena** (cómo funciona el backlog) y **un ejemplo transversal** (B-03) | Norma/jilosófulo |
| `INFORME-Auditoria.md` | La **evidencia original** de cada hallazgo (fotografía inmutable de `dcd1741`) | Historial |

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

> **Principios 3, 9, 10.** Los puntajes se recalculan al cierre de cada fase (no se congelan). Cada punto ganado exige **evidencia de cheque** (qué gate/regla lo demuestra). Baseline: `INFORME-Auditoria.md` §7 (2026-08-06).

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
| Configuración | 8/10 | 9/10 | B-09, B-04 | paridad de config + secrets redactados |
| Performance | 5/10 | 7/10 | B-14, F5 benchmarks | benchmarks documentados |

> **Regla del scorecard:** un punto se considera ganado **solo** cuando existe la evidencia de la columna derecha (cheque verde con fecha+commit). Hasta entonces, el puntaje no sube — coherencia con el principio "evidencia, no intención".

---

## 10. Umbrales — definidos únicamente tras medición en F0

> **Principio 9.** Ningún umbral numérico se fija antes de la medición en vivo en F0. Precedente: una auditoría previa reportó **13 %** de cobertura desde un `.coverage` stale; la medición en vivo dio **43 %**. Fijar ≥70 % a ciegas repetiría ese error.

**Qué se mide en F0 (antes de fijar nada):**

| Umbral pendiente | Se define en | Método de medición |
|---|---|---|
| G6 cobertura crítica | F2 (tras F0) | `pytest --cov` sobre `trading/execution` y `storage/iceberg` (medición en vivo) |
| `fail_under` | F2 | medición + margen (estilo "medir, luego fijar") |
| Bandit severidad mínima | F2 | ejecutar `bandit -ll -r ...` y clasificar hallazgos reales |
| Scorecard (puntajes) | cierre de cada fase | re-ejecución de gates y mediciones |

Todo valor fijado queda registrado en tracking.yaml con el comando y el hash de commit que lo produjo.

---

## 11. Lifecycle del sistema — longevidad

> **Principio 10.** El sistema debe sobrevivir a sus autores.

- **Métrica de madurez:** % de reglas de arquitectura/calidad gateadas por CI (`activada_en_ci: true` / total) vs las que dependen de revisión manual. Baseline medido en F0; sube en cada fase, nunca baja.
- **Rotación:** cualquier ingeniero nuevo entra por §1 (Cómo usar este documento), navega la cadena de §2 con un ejemplo real, y sabe el estado exacto del sistema leyendo tracking.yaml. No hay conocimiento implícito: todo lo que no está en este documento o en el yaml **no es normativo**.
- **Regla de oro del sistema:** una regla no se activa en CI sin su backtest `ok`; un hallazgo no se cierra sin evidencia de cheque; un umbral no se fija sin medición en vivo; una ADR no se numera sin `ls` previo.

---

## 12. Registro de cambios

| Fecha | Commit | Cambio |
|---|---|---|
| 2026-08-06 | (baseline `dcd1741`) | Creación como especificación SSOT del sistema de ingeniería; tracking.yaml v2; verificación de numeración ADR — ADR-0015 quedó ocupado por el blindaje de apps (`a48f28e`), siguiente libre: ADR-0016 |

> Actualización de numeración: ADR-0015 real (blindaje Application Layer, serie `AUDIT-apps-2026-08-03#Hx`) se commiteó con ese número; las propuestas que este documento asignaba a ADR-0015–0019 se desplazan a **ADR-0016–0020** (ver §5).
