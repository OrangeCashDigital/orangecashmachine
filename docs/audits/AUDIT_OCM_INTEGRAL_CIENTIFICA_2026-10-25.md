# AUDIT_OCM_INTEGRAL_CIENTIFICA_2026-10-25

**Tipo:** Auditoría integral científica, bibliográfica, arquitectónica, operativa y de gobernanza (solo lectura).  
**Fecha:** 2026-10-25  
**Ámbito:** Repositorio OrangeCashMachine (OCM) — `/home/orangemusic/trading/orangecashmachine`  
**Commit/base:** `cd1ad489569427916cf57e9a3a1f0c61d2d98c2c` (rama `main`), árbol de trabajo limpio excepto 5 ficheros audit no rastreados  
**Metodología:** Conforme a `docs/governance/AUDIT_PROTOCOL.md` v2.1 (orden normativo AGENTS.md → AUDIT_PROTOCOL.md → PLAN-Maestro-Ingenieria.md → GOVERNANCE.md → tracking.yaml → ADRs → auditorías históricas → linters/CI). Machine checks first (scripts/audit_validator.py M1..M20) antes de juicio LLM. Distinción estricta hecho/inferencia/recomendación/decisión requerida al humano. Sin duplicar hallazgos ya registrados; control FAIL != hallazgo nuevo (REVALIDADO).

**Restricciones cumplidas:** Solo lectura. Escritura exclusiva a `docs/audits/`. Sin operaciones git mutantes, sin iniciar servicios/trading, sin dependencias, sin Gate C/Q2/F9/FASE 15, sin implementar decisiones.


## 1) Alcance y metodología

- **Orden normativo seguido:** `AGENTS.md` → `docs/governance/AUDIT_PROTOCOL.md` → `docs/PLAN-Maestro-Ingenieria.md` → `docs/architecture/GOVERNANCE.md` → `docs/plans/tracking.yaml` → ADRs relevantes → auditorías históricas → `architecture_linter/` + `tests/architecture_linter/test_golden.py` → `.github/workflows/ocm-ci.yml`.
- **Lectura directa de fuentes primarias:** código (`packages/market_data/*`, `apps/app/cli/streaming_hydra.py`), configuración (`config/exchanges/bybit.yaml`, `config/market_data/feeds.yaml`, `config/env/development.yaml`), contratos/linters, scripts (`scripts/audit_validator.py`), ADRs (especialmente ADR-0043, ADR-0049, ADR-0054, ADR-0055, ADR-0057).
- **Reconciliación:** Se contrasta con auditorías históricas (incl. `AUDIT_OCM_state-end-to-end_2026-08-28.md`, `AUDIT_OCM_DISCOVERY-BOOKBUILDER-RECONCILIATION_2026-08-28.md`, `AUDIT_OCM_orderbook-resync-*.md`, series 2026-10-20/21/25) sin tomar como "resuelto" lo que solo está descrito así.
- **Clasificación:** HECHO VERIFICADO / EVIDENCIA HISTÓRICA / INFERENCIA / RIESGO / RECOMENDACIÓN / DECISIÓN REQUERIDA AL HUMANO. Severidad P0–P4 con justificación.


## 2) Inventario de fuentes examinadas

### Normativas
- `AGENTS.md`
- `docs/governance/AUDIT_PROTOCOL.md` (v2.1)
- `docs/PLAN-Maestro-Ingenieria.md` (secciones relevantes leídas)
- `docs/architecture/GOVERNANCE.md`
- `docs/plans/tracking.yaml` (lectura ampliada incluyendo F-OCM-25/26 y bloques relacionados)

### ADRs (examinados expresamente)
- `docs/architecture/decisions/ADR-0043-orderbook-snapshot-resync.md` (Propuesto)
- `docs/architecture/decisions/ADR-0049-canonical-instrument-identity-g1.md` (662 líneas) — D01–D15, D19-01..10, D20a/D20b-01..06
- `docs/architecture/decisions/ADR-0054-persistent-engineering-context-decisions.md`
- `docs/architecture/decisions/ADR-0055-d-ocm-01-normative-authority-d19-d20b.md`
- `docs/architecture/decisions/ADR-0057-g1-identity-definition-venue-constraints-consolidation.md` — HD-1..HD-8, HD-VC-1..HD-VC-7 (DECIDED/cerrados conceptualmente), GATE C pendiente
- `docs/architecture/decisions/ADR-0047`, `ADR-0048` (contexto entrypoints/execution contract)

### Auditorías (muestra representativa, reconciliación)
- `docs/audits/AUDIT_OCM_state-end-to-end_2026-08-28.md`
- `docs/audits/AUDIT_OCM_DISCOVERY-BOOKBUILDER-RECONCILIATION_2026-08-28.md`
- `docs/audits/AUDIT_OCM_orderbook-resync-*.md`
- `docs/audits/AUDIT_OCM_high-impact-improvements_2026-10-07.md` (F-OCM-01)
- `docs/audits/AUDIT_OCM_AGENT_GOVERNANCE_SYSTEM_DIAGNOSTIC_2026-10-20.md`
- `docs/audits/AUDIT_OCM_SCIENTIFIC_BIBLIOGRAPHIC_GOVERNANCE_DECISIONS_2026-10-20.md`
- `docs/audits/AUDIT_OCM_PHASE2_EVIDENCE_SYNTHESIS_2026-10-21.md`
- `docs/audits/AUDIT_OCM_CROSS_REVIEW_THREE_REPORTS_2026-10-25.md`
- `docs/audits/AUDIT_OCM_GOVERNANCE_RECONCILIATION_2026-10-25.md`
- `docs/audits/AUDIT_OCM_d19-01-industry-bow-review_2026-10-03.md`
- `docs/audits/AUDIT_OCM_decision-support-hd-venue-constraints-identity_2026-09-29.md`

### Código/Config/CI/Linters
- `packages/market_data/application/processing/book_builder.py`, `packages/market_data/domain/value_objects/order_book.py`, `packages/market_data/infrastructure/kafka/book_builder_consumer.py`
- `apps/app/cli/streaming_hydra.py`
- `config/exchanges/bybit.yaml`, `config/market_data/feeds.yaml`, `config/env/development.yaml`
- `architecture_linter/importlinter.toml`, `tests/architecture_linter/test_golden.py`, `.github/workflows/ocm-ci.yml`
- `scripts/audit_validator.py`


## 3) Revisión del validador (`scripts/audit_validator.py`)

### Comando ejecutado y resultado
- **Comando (en sesión):** `cd /home/orangemusic/trading/orangecashmachine && uv run python scripts/audit_validator.py 2>&1 | tail -30`
- **Resultado disponible:** `WARN [M25] ADR-0020: en related_adrs pero ninguna regla lo referencia` / `WARN [M25] ADR-0044: en related_adrs pero ninguna regla lo referencia` / `WARN [M20] 3 controles FAIL sin ningún finding NUEVO (verificar deduplicación)` / `[audit-validator] PASS — 2 findings, 25 reglas mecánicas (warnings 3, skipped 0)`

### Advertencias concretas
- **M25:** ADR-0020 y ADR-0044 aparecen en `related_adrs` pero ninguna regla los referencia.
- **M20:** 3 controles FAIL sin ningún finding NUEVO (verificar deduplicación).

### Modificaciones producidas por el validador
- **HECHO VERIFICADO (lectura):** Revisado `scripts/audit_validator.py` (código). No se observan llamadas a `open(..., 'w')`, `write`, `mkdir` u otras operaciones de escritura. Es principalmente parser/validador que lee archivos y emite salida/exit code. **No existe evidencia directa de modificaciones al repo por esta ejecución.** Según instrucción, no se afirma "no hubo" sin verificación exhaustiva de efectos colaterales; con la evidencia disponible **no hay indicios de escritura**.
- **EVIDENCIA HISTÓRICA:** Resultado previo ya registrado en contexto (PASS con warnings).

### Comprobaciones que permiten verificar estado del repo
- Exit code / reporte (PASS/FAIL, warnings/skipped).
- Reglas M1..M20 (incluyendo gramática de nombres canónicos, ADR tracking, golden, comandos canónicos, etc.).
- Consistencia entre tracking/ADRs/registro de findings.

### Aspectos no verificables con evidencia disponible
- **INFERENCIA/LIMITACIÓN:** No se verificó si hay artefactos temporales generados por el validador fuera del repo o efectos en entorno. No se inspeccionó si algún hook/post-run escribe. **No se vuelve a ejecutar** conforme a instrucción (revisar comportamiento antes de nueva ejecución).


## 4) Reconciliación: ADR-0049, D12, D19, D20a/D20b, HD-1..8, HD-VC-1..7

### ADR-0049 (Canonical Instrument Identity — G1 D01–D15)
- **HECHO VERIFICADO:** `ADR-0049-canonical-instrument-identity-g1.md` (662 líneas). Estados: D01–D12 adoptados (G1 Human Decision Gate), G1-D13/D14/D15 materializados con aceptación. 
- **HECHO VERIFICADO (tabla estado):** D19-01 = DECIDED (C+), D19-02 = DECIDED (A), D19-03 = DECIDED (A), D19-04 = DECIDED (B), D19-05 = DECIDED (A), D19-06 = DECIDED (A), D19-07 = DECIDED (A), D19-08 = DECIDED (D), D19-09 = DECIDED (B), D19-10 = DECIDED (C). D20 = DECIDED (SPLIT GATES). D20a = DECIDED. D20b = DECIDED con D20b-01..06 DECIDED.
- **HECHO VERIFICADO:** `ADR-0049` L562 indica **IMPLEMENTATION = NOT AUTHORIZED** (también referido en auditorías/tracking).

### Contradicciones históricas (F-OCM-01 / D-OCM-01)
- **HECHO VERIFICADO:** `tracking.yaml` (F-OCM-01, líneas ~3830+) registra contradicción C1–C4 preservada. `ADR-0055` resuelve autoridad normativa a favor de ADR-0049 + registros humanos para Q1, pero **IMPLEMENTATION = NOT AUTHORIZED**, **GATE C: BLOCKED**, Q2 = UNAUTHORIZED. `ADR-0057` inventaría C1–C4 sin resolverlas. Riesgo residual CRÍTICO.
- **INFERENCIA:** "Decidido" en ADR-0049 no equivale a "autorizado a implementar" — la prohibición explícita persiste.

### D12
- **HECHO VERIFICADO:** ADR-0049 §G1-D12 (L335/361/410/421/467/589/631) — separación Identity/Definition: versionado de Definition vs nueva identidad + relación (C/RESTRICTED). Sigue abierto en facetas relevantes (cross-venue, D19, D20–D24). Persisten referencias a D12 como abierto en ADR-0057.

### HD-1..HD-8
- **HECHO VERIFICADO:** `ADR-0057` registra DECISION RECORDs para HD-1..HD-8 (A/D/C refinada/A/I/A/A etc.), justificados. Cerrados conceptualmente según ADR-0057, pero **sin autorización de implementación** implícita.

### HD-VC-1..HD-VC-7
- **HECHO VERIFICADO:** ADR-0057 registra HD-VC-1 (A+ con cláusula de derivación/exclusión semántica), HD-VC-2(A), HD-VC-3(A), HD-VC-4(D), HD-VC-5(B), HD-VC-6(D), HD-VC-7(A). 
- **HECHO VERIFICADO:** ADR-0057 §17.3/17.4 — **GATE C permanece BLOCKED**; requiere ADR nuevo formalizado por el humano + registro en `tracking.yaml` + contrato/linter. **HUMAN ACTION REQUIRED** explícito.
- **INFERENCIA:** Aunque conceptualmente "DECIDED/CERRADO", **GATE C BLOCKED** significa que **no hay autorización para materialización/implementación**.

### Reconciliación con auditorías 2026-10
- **EVIDENCIA HISTÓRICA:** `AUDIT_OCM_d19-01-industry-bow-review_2026-10-03.md` confirma coherencia con D01–D12, HD-1..8, HD-VC-1..7 y refuerza separación estricta Identity≠Representation≠DefV≠VenueConstraints.
- **EVIDENCIA HISTÓRICA:** `AUDIT_OCM_decision-support-hd-venue-constraints-identity_2026-09-29.md` documenta decisiones HD*/HD-VC* con justificación.


## 5) Order book / BookBuilder / streaming_hydra / Bybit dual-enable

### BookBuilder (aplicación)
- **HECHO VERIFICADO:** `packages/market_data/application/processing/book_builder.py` usa `Decimal` (D-7c). `_apply_delta_levels` pre-valida todos los niveles antes de mutar (aplicación atómica de multilevel deltas). `on_delta` requiere `update_id == last_update_id + 1`, si no `GAP_DETECTED` + `reset()`. Continidad por `u`.
- **HECHO VERIFICADO:** `packages/market_data/domain/value_objects/order_book.py` usa `float` (diferente representación en domain VO vs procesamiento Decimal) — **no es contradicción automática** pero merece atención (coherencia de tipos).

### BookBuilder consumer / wiring
- **HECHO VERIFICADO:** `apps/app/cli/streaming_hydra.py:180-198` atrapa `book_builder_consumer_init_failed` y devuelve `(None, None)` sin bloquear canary. No se identifica ruta probada de notificación humana ni publicación de métricas server-side desde el repo.
- **HECHO VERIFICADO:** `packages/market_data/infrastructure/kafka/book_builder_consumer.py` existe (contexto).

### ADR-0043 (orderbook snapshot/resync)
- **HECHO VERIFICADO:** `ADR-0043-orderbook-snapshot-resync.md` **Propuesto**. Marca u-jump sin desconexión como **BLOCKED** con race/REST snapshot path no resueltos.
- **INFERENCIA:** Estado propuesto implica **no decidido**; no se trata como cerrado.

### Bybit dual-enablement (contradicción)
- **HECHO VERIFICADO:** `config/exchanges/bybit.yaml:5` → `exchanges.bybit.enabled: false`. `config/market_data/feeds.yaml:20` → `feeds.feeds.bybit.enabled: true`. `config/env/development.yaml` habilita Bybit. 
- **RIESGO:** Contradicción de configuración (posible ambigüedad de habilitación). Requiere clarificación pero **no se modifica config**.
- **EVIDENCIA HISTÓRICA:** Revalidado en auditorías previas (coherente con lo encontrado).


## 6) Arquitectura, configuración, CI

### Contratos / linters
- **EVIDENCIA HISTÓRICA:** Estado conocido: **51 contratos BC-NN** (51 kept / 0 broken) según AGENTS.md/estado vigente. `architecture_linter/importlinter.toml` existe; `tests/architecture_linter/test_golden.py` presente.
- **HECHO VERIFICADO:** `scripts/audit_validator.py` referencia `CANONICAL_COMMANDS` incluyendo `ARCH_CONTRACTS` (`uv run lint-imports --config architecture_linter/importlinter.toml`), `ARCH_LINTER` (`uv run python -m architecture_linter --root . --json` exit 1 si FAIL/PARTIAL), `GOLDEN`, etc.

### CI
- **HECHO VERIFICADO:** `.github/workflows/ocm-ci.yml` existe. AGENTS.md indica orden CI: `architecture (import-linter) → tests + config-validation (parallel)`. Broken contracts skip downstream.
- **EVIDENCIA HISTÓRICA:** Fases F2.0–F2.6, gates, tracking-consistency, engineering-health documentados en PLAN-Maestro.

### Configuración
- **HECHO VERIFICADO:** `OCM_VALIDATE_ONLY` usa coerción BOOL_TRUE={true,yes,on} (1 no activa validate-only) — gotcha documentado en AGENTS.md.
- **HECHO VERIFICADO:** Múltiples capas Hydra; `dry_run: true` default.


## 7) Matriz de hallazgos y evidencias (reconciliados)

| ID | Tipo | Severidad | Estado | Fuente(s) | Descripción | Evidencia | Acción |
|---|---|---|---|---|---|---|---|
| H-INT-01 | Gobernanza/Autoridad | P0 | **REVALIDADO** | `tracking.yaml` F-OCM-01; `ADR-0049` L542-562; `ADR-0055`; `ADR-0057` | Contradicción C1–C4 sobre D19-01/D20b-01..06 preservada. IMPLEMENTATION=NOT AUTHORIZED, GATE C BLOCKED, Q2 UNAUTHORIZED. | ADR-0055 L212, ADR-0057 L109-113; tracking F-OCM-01 cadena | **DECISIÓN REQUERIDA AL HUMANO** (resolver C1–C4). No autorizar implementación. |
| H-INT-02 | Gobernanza | P0 | **REVALIDADO** | `ADR-0057` §17.3/17.4; `docs/audits/AUDIT_OCM_decision-support-hd-venue-constraints-identity_*.md` | GATE C permanece BLOCKED. HD-1..8, HD-VC-1..7 conceptualmente DECIDED pero requieren ADR nuevo formalizado por humano + tracking + contrato/linter. | ADR-0057 L109, L76-77; decision-support pack | **DECISIÓN REQUERIDA AL HUMANO** antes de materialización. |
| H-INT-03 | Gobernanza/Implementación | P1 | **REVALIDADO** | `tracking.yaml` F-OCM-26; `ADR-0049`; `docs/knowledge/KNOWLEDGE_BASE_CONTRACT.md` | 20 ficheros (código/tests/docs) existen solo en commit `4ad03eba` (pre-L13) ausentes de main; contrato KB exige integridad. PR rescue pendiente. | tracking F-OCM-26 evidencia git diff; KB contract refs | **RIESGO** (alineación D19/D20b). Seguimiento en tracking. |
| H-INT-04 | Documental/Autoridad | P1 | **REVALIDADO** | `tracking.yaml` F-OCM-25 | 7 refs documentales colgantes a grafía antigua (ADR-0054/0055/engineering-context). PR futuro pendiente. | tracking F-OCM-25 | **MEJORA RECOMENDABLE** (trazabilidad). Sin bloqueo runtime. |
| H-INT-05 | Configuración | P1 | **HECHO VERIFICADO** | `config/exchanges/bybit.yaml:5`; `config/market_data/feeds.yaml:20`; `config/env/development.yaml` | Dual-enable contradictorio (bybit.yaml disabled vs feeds.yaml enabled). | Lectura directa | **RIESGO** (ambigüedad). Verificar precedencia/resolución; **no modificar** sin decisión. |
| H-INT-06 | Orderbook/Gobernanza | P1 | **HECHO VERIFICADO** | `ADR-0043-orderbook-snapshot-resync.md` (Propuesto); `book_builder.py`; `streaming_hydra.py:180-198` | ADR-0043 Propuesto, u-jump sin desconexión BLOCKED con race/REST snapshot. Falta ruta probada de notificación humana/métricas server-side en wiring. | ADR-0043; código inspeccionado | **RIESGO**. Requiere decisión/implementación controlada; no inferir cerrado. |
| H-INT-07 | Calidad/Validador | P2 | **HECHO VERIFICADO** | `scripts/audit_validator.py` ejecución + código | PASS con warnings M20/M25. Sin evidencia de escritura por ejecución; comportamiento a verificar antes de re-ejecutar. | Salida comando + lectura código | **LIMITACIÓN/SEGUIMIENTO**. Documentar, no re-ejecutar sin análisis. |
| H-INT-08 | Arquitectura/Tipado | P3 | **HECHO VERIFICADO** | `book_builder.py` (Decimal), `order_book.py` (float) | Diferentes representaciones (procesamiento Decimal vs VO float). | Lectura directa | **OBSERVACIÓN**. Verificar coherencia; no asumir bug sin pruebas. |


## 8) Priorización razonada

| Prioridad | Hallazgos | Justificación (conforme AUDIT_PROTOCOL) |
|---|---|---|
| **P0** | H-INT-01, H-INT-02 | Contradicciones de autoridad con **IMPLEMENTATION NOT AUTHORIZED / GATE C BLOCKED**. Requieren **decisión humana** antes de materializar/implementar D19/D20b/HD*/HD-VC*. No bloquean lectura/verificación, sí bloquean autorización. |
| **P1** | H-INT-03, H-INT-04, H-INT-05, H-INT-06 | Riesgos reales (código ausente en main vs contrato KB, refs documentales, config dual-enable, orderbook resync). Requieren atención/seguimiento; algunos necesitan decisión, ninguno requiere acción inmediata no autorizada. |
| **P2** | H-INT-07 | Validador PASS con warnings; sin evidencia de mutación pero comportamiento no totalmente verificable. Seguimiento. |
| **P3** | H-INT-08 | Observación de coherencia tipado. Sin evidencia de fallo funcional en esta lectura. |

**Diferenciación:** Riesgos que requieren atención antes de avanzar (P0/P1 con bloqueo de autorización), contradicciones que requieren decisión humana (P0), mejoras recomendables no bloqueantes (H-INT-04), aspectos no verificables (H-INT-07).


## 9) Lista explícita de decisiones reservadas al humano

1. **Resolver contradicción C1–C4 (F-OCM-01 / D-OCM-01):** Clarificar estado final de D19-01/D20b-01..06 vs ADR-0049/0055/0057. Mantener o levantar **IMPLEMENTATION = NOT AUTHORIZED** / **GATE C: BLOCKED** / **Q2 UNAUTHORIZED**.
2. **Abrir GATE C (HD-VC/HD):** Formalizar ADR nuevo para HD-VC-1..7/HD-1..8 según §17.3-17.4 de ADR-0057, registrar en `tracking.yaml`, actualizar contrato/linter si procede. **No se infiere autorización.**
3. **Resolver Bybit dual-enable:** Decidir fuente canónica de habilitación (`config/exchanges/bybit.yaml` vs `config/market_data/feeds.yaml` vs env) y precedencia.
4. **Ordenbook resync (ADR-0043):** Decidir si promover ADR-0043 a Aceptado, definir ruta REST snapshot + manejo de race + notificación/métricas, o mantener bloqueado.
5. **Rescate F-OCM-26:** Autorizar PR para extraer 20 ficheros de `4ad03eba` adaptados a gates L13 o rechazar/documentar alternativa.
6. **Limpiar refs F-OCM-25:** Autorizar PR documental para corregir 7 referencias a grafía canónica.
7. **Validador:** Decidir si ejecutar `scripts/audit_validator.py` nuevamente tras revisar su comportamiento (sin asumir mutaciones).


## 10) Limitaciones y aspectos no verificados

- **No verificado funcionamiento integral:** Presencia de código no prueba funcionamiento. Tests/CI no ejecutados en esta auditoría (solo lectura).
- **Validador:** No se re-ejecutó; sin evidencia concluyente de mutaciones. Posibles efectos no inspeccionados.
- **F-OCM-26:** No se compararon los 20 ficheros con estado actual línea a línea (solo diff names/refs).
- **Orderbook:** No se ejecutaron pruebas/integración; validación solo estática (BookBuilder lógica Decimal, wiring streaming_hydra).
- **D12/D19/D20:** Estados leídos en ADRs/tracking; implementación física no inspeccionada en detalle (fuera de alcance para "autorización").
- **Auditorías históricas:** Muchas son evidencia histórica; se evitó tomarlas como "resuelto" sin fuente primaria.
- **Búsqueda limitada:** Ausencia de coincidencia no se presentó como prueba de inexistencia.


## 11) Recomendaciones de siguientes pasos (sin ejecutarlos)

1. **Gobernanza primero:** Resolver H-INT-01/H-INT-02 (humano). No iniciar Gate C/Q2/F9/FASE 15.
2. **Config:** Clarificar Bybit dual-enable (H-INT-05) con decisión documentada.
3. **Orderbook:** Decidir ADR-0043 (H-INT-06) — definir snapshot/resync + notificación/métricas antes de confiar en canary.
4. **Tracking:** Dar seguimiento a F-OCM-26/F-OCM-25 (PRs documentados en tracking).
5. **Validador:** Revisar `scripts/audit_validator.py` comportamiento escritura antes de nueva ejecución si fuera necesario.
6. **Reconciliación continua:** Mantener distinción "DECIDED" vs "AUTHORIZED TO IMPLEMENT" (especialmente ADR-0049 L562).


## 12) Registro de archivos y operaciones realizados

### Archivos leídos (principales)
- `AGENTS.md`, `docs/governance/AUDIT_PROTOCOL.md`, `docs/PLAN-Maestro-Ingenieria.md`, `docs/architecture/GOVERNANCE.md`, `docs/plans/tracking.yaml`
- `docs/architecture/decisions/ADR-0043-*.md`, `ADR-0047-*.md`, `ADR-0048-*.md`, `ADR-0049-*.md`, `ADR-0054-*.md`, `ADR-0055-*.md`, `ADR-0057-*.md`
- `docs/audits/AUDIT_OCM_state-end-to-end_2026-08-28.md`, `AUDIT_OCM_DISCOVERY-BOOKBUILDER-RECONCILIATION_2026-08-28.md`, `AUDIT_OCM_orderbook-resync-*.md`, `AUDIT_OCM_high-impact-improvements_2026-10-07.md`, series 2026-10-20/21/25, `AUDIT_OCM_d19-01-industry-bow-review_2026-10-03.md`, `AUDIT_OCM_decision-support-hd-venue-constraints-identity_2026-09-29.md`
- `packages/market_data/application/processing/book_builder.py`, `packages/market_data/domain/value_objects/order_book.py`, `packages/market_data/infrastructure/kafka/book_builder_consumer.py`
- `apps/app/cli/streaming_hydra.py`
- `config/exchanges/bybit.yaml`, `config/market_data/feeds.yaml`, `config/env/development.yaml`
- `architecture_linter/importlinter.toml`, `tests/architecture_linter/test_golden.py`, `.github/workflows/ocm-ci.yml`
- `scripts/audit_validator.py`

### Operaciones realizadas
- Inventario ADRs (`ls docs/architecture/decisions/`) y auditorías 2026-10 (`ls docs/audits/ | grep 2026-10`)
- Verificación existencia archivo objetivo (`test -f ...AUDIT_OCM_INTEGRAL_CIENTIFICA_2026-10-25.md` → NOT_EXISTS)
- Lectura ampliada `docs/plans/tracking.yaml` (F-OCM-25/26)
- Lectura ADR-0049 completa (grep + líneas clave D12/D19/D20)
- Lectura ADR-0057 (HD*/HD-VC*/GATE C)
- Lectura código orderbook/BookBuilder/streaming_hydra y config Bybit
- Ejecución **única**: `uv run python scripts/audit_validator.py` (lectura resultado, sin re-ejecución)
- **Creación exclusiva:** `docs/audits/AUDIT_OCM_INTEGRAL_CIENTIFICA_2026-10-25.md` (append incremental)

### Verificación final
- **Única modificación intencional:** creación del nuevo informe. No se modificaron código, tests, config, CI, ADRs, tracking.yaml, planes, políticas ni documentos existentes.
- Sin operaciones git mutantes, sin servicios/trading, sin dependencias.


---

## ERRATA — Correcciones post-review (2026-10-26)

### E-B1 — H-INT-02: recomendación obsoleta

El texto original (§148 y §172) lista como pendiente "ADR nuevo formalizado por
humano + tracking + contrato/linter" y "Formalizar ADR nuevo para
HD-VC-1..7/HD-1..8 según §17.3-17.4".

**Corrección:** ADR-0057 §176 indica que §17.3 queda materializada
documentalmente como ADR-0057. El ADR nuevo ya existe. Trabajo restante real:

- **§17.4:** registro en `tracking.yaml` + contrato/linter (NO un ADR nuevo).
- **GATE C:** BLOCKED — requiere autorización humana separada.
- **Materialización** de D19/D20b/HD*/HD-VC*: sujeta a autorización humana.

### E-D1 — Atribución de conteos históricos

El texto original (§43 y derivadas) atribuye los conteos históricos y el
disclaimer de no-reverificación a
`AUDIT_OCM_SCIENTIFIC_BIBLIOGRAPHIC_GOVERNANCE_DECISIONS_2026-10-20.md`.

**Corrección:** ese informe declara en §2 limitaciones (sin acceso completo a
todos los libros en docs/knowledge, sin verificación de branch protection) y no
contiene conteos históricos ni disclaimer de re-verificación.

**Atribución correcta:** solo
`AUDIT_OCM_CROSS_REVIEW_THREE_REPORTS_2026-10-25.md` los contiene.
