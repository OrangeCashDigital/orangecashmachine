# Consolidación Arquitectónica OCM — 2026-08-16

## 1. Objetivo

Convertir el trabajo de auditorías recientes en un estado arquitectónico consolidado y
trazable. Cadena objetivo:

```
AUDITORÍA → HALLAZGO → DECISIÓN → ADR/DOC → TRACKING → CÓDIGO → TESTS/GATES → EVIDENCIA
```

Este documento es la **matriz de verdad** que permite a otro agente/desarrollador/auditor
saber: qué problemas existían, cuáles se corrigieron, cuáles son by-design, cuáles están
BLOCKED, cuáles son deuda, qué ADR gobierna cada decisión, qué queda pendiente y qué NO
debe tocarse todavía.

## 2. Método — evidencia primero

Cada afirmación de este documento fue verificada contra el repositorio el 2026-08-16
(HEAD `54b64ff` + cambios sin commit de la remediación). No se asumió ninguna auditoría
como correcta por estar escrita; se re-verificó contra código, contratos y gates.

**Prioridad en conflicto (KB):** código + contratos + ADR aprobada > auditorías/informes.
**Regla de no-reescritura:** ninguna auditoría histórica fue reescrita; las discrepancias
se documentan aquí y en su caso en la ADR afectada (p. ej. nota de discrepancia ya presente
en ADR-0014:273-287).

## 2bis. Decisiones aprobadas por el owner (2026-08-16)

| Decisión | Resultado |
|---|---|
| ADR-0029 (cancel real, B-MD-008) | **Aprobada** — promovida a Aceptada; tracking B-MD-008 cadena.adr HECHO; implementación sigue PENDIENTE |
| ADR-0030 (balance real, B-MD-009) | **Aprobada** — promovida a Aceptada; tracking B-MD-009 cadena.adr HECHO; implementación sigue PENDIENTE |
| `architecture/metrics.json` | **Eliminado** (Opción B) — archivo untracked/ignorado (`.gitignore:58`); `scripts/metrics_report.py` permanece como generador bajo demanda |
| ARCH-007 (8 contratos duplicados) | **Se mantiene como DEBT** — documentado, pineado por golden test; refactor futuro fuera de alcance |
| ADR-0021 (raíz B-15) | **Borrador redactado como PROPUESTA** (2026-08-16) — enlazada en B-15 cadena.adr PENDIENTE; pendiente de decisión humana |

## 3. Matriz de verdad (findings consolidados)

| Finding | Problema | Evidencia actual (2026-08-16) | Acción realizada | ADR | Tracking | Estado real |
|---|---|---|---|---|---|---|
| **F-1** (H2 apps) | research dependía de adaptadores concretos (`IcebergStorageFactory`, `GoldReader`) en vez de ports | `apps/research/data/data_access.py:49-50,78` importa `StorageFactoryPort`/`FeatureReaderPort`; seam `_storage_factory: StorageFactoryPort = build_storage_factory()` (`:78`); `apps/research/data/composition_root.py` es el único que conoce adapters (`:39-42`); BC-55 en `architecture/importlinter.toml:831`; `tests/research/test_data_access.py` usa fakes (`_FakeStorageFactory`:83, `_FakeFeatureReader`:105, `restore_seams`:158); **sin** `reset()` en `StorageFactoryPort` (`storage_factory.py` grep vacío); **sin** hooks de testing en `IcebergStorageFactory` (`iceberg_factory.py:33-90`) | Composition root de research + data_access sobre contracts + BC-55 (commit `aa547b3`, 2026-08-14) | Sin ADR dedicada; gobernado por ADR-0003 (patrón CR) + BC-55 (contrato) | H2 apps-audit (RESUELTO 2026-08-14) | **RESUELTO** (verificado en código + gates) |
| **F-6** (Corolario) | Imports dinámicos de `market_data` fuera del composition root de trading (BC-50 hueco) | `tests/architecture/test_import_contracts.py:45` `_dynamic_market_data_targets()` (AST, solo literal string); `TestDynamicMarketDataDetector`:403 con 6 casos (`test_detects_importlib...`:423, `test_detects_builtin...`:430, `test_detects_top_level...`:436, `test_ignores_non_market_data...`:441, `test_ignores_dynamic_non_literal...`:448, `test_scan_excludes_authorized...`:456); límites documentados (`:413-421`) | Detector AST + tests de regresión (commit `f9801e7`, 2026-08-14) | Sin ADR (extensión de contratos/guards) | Corolario F-6 apps-audit (RESUELTO 2026-08-14) | **RESUELTO** — sin ampliar alcance (nombres dinámicos no literales quedan fuera por diseño) |
| **ARCH-006** | 8 ports huérfanos + stub no cableado | Linter ARCH-006 **PASS**; `tests/architecture_linter/test_golden.py:56-73` `test_golden_arch006_orphan_ports` guarda no-reaparición; ports eliminados (`git status`: 5 D) | 6 ports eliminados + 2 cableados (remediación 2026-08-16) | ADR-0009 precedente; sin ADR nueva | no B-XX (documentado en informe remediación) | **RESUELTO** |
| **ARCH-007** | Contratos duplicados/homónimos | Linter ARCH-007 **FAIL (8)**; golden pin (`test_golden.py:32,77-97`) lista los 8 restantes; `ExchangeCircuitOpenError` consolidado en `domain/exceptions/__init__.py:227` (verificado: ya no aparece en `resilience.py` — eliminado) | Solo `ExchangeCircuitOpenError` consolidado; **los otros 8 NO se tocaron** | — | — | **DEBT** (8 duplicados documentados, no resueltos) |
| **ARCH-008** | Stubs / false capability | Linter ARCH-008 **FAIL (1)**; `test_golden.py:101-110`: `WSTradesSource` presente (diseñado), `InfraMetricsKafkaProducer` ausente (eliminado) | `InfraMetricsKafkaProducer` eliminado; `WSTradesSource` **mantenido por diseño** (fallback REST, `source_manager.py:213-234`) | — | — | **BY-DESIGN** (intencional; no arreglar) |
| **ARCH-003** | Órdenes sin reconciliación periódica | Linter **PARTIAL (1)**; OMS.cancel() local-only sin callers | ADR aprobada; **implementación PENDIENTE** (no hecha) | ADR-0029 (Aceptada 2026-08-16) | B-MD-008 (PENDIENTE, cadena.adr HECHO) | **APROBADO — implementación pendiente** |
| **ARCH-004** | Sin balance real | Linter **FAIL (1)**; sin `fetch_balance` en repo; `capital_usd` fijo | ADR aprobada; **implementación PENDIENTE** (no hecha) | ADR-0030 (Aceptada 2026-08-16) | B-MD-009 (PENDIENTE, cadena.adr HECHO) | **APROBADO — implementación pendiente** |
| **ARCH-005** | Freshness boundary rota niveles 3-6 | Linter **FAIL (1)**; requiere contrato cross-BC + cambio dominio trading | BLOCKED — no implementado | ADR previo requerido (nonexistente) | no B-XX | **BLOCKED** (cross-BC) |
| **ARCH-001** | Múltiples owners de estado de posición | Linter **FAIL (1)**; 5 owners mutables además del SSOT portfolio | ADR-0021 en PROPUESTA (raíz B-15); no implementada | ADR-0006 (Aceptada), ADR-0021 (Propuesta) | B-15 (EN_CURSO) | **BY-DESIGN** (con deuda residual B-15) |
| **ARCH-002** | Divergencia semántica de posición (WAC vs replace/pop) | Linter **FAIL (2)**; ADR-0025 WAC aceptada; divergencia financiera preservada | No tocado (semántica financiera real; requiere decisión) | ADR-0025 (Aceptada) | — | **DEBT** (divergencia preservada, documentada) |
| **ARCH-009** | Layer violations | Linter **PASS** (0) | PASS desde baseline; SSOT BC-08 leído por la regla | BC-08 / ADR-0007 | — | **RESUELTO** (nunca violado) |
| **ARCH-010** | Estado mutable duplicado (position ×6, order ×4) | Linter **FAIL (2)**; relacionado con B-15/ADR-0006 | No tocado | ADR-0006 | B-15 (EN_CURSO) | **DEBT** (heredado de B-15) |

## 4. Consolidadción F-1 — verificación y decisión

**Estado: RESUELTO** (verificado). Todos los puntos de la evidencia previa se confirmaron:

1. research ya no instancia el singleton concreto → **confirmado**: `data_access.py` usa
   seam tipado contra port (`:78`); el concreto vive solo en `composition_root.py`.
2. composition root introducido → **confirmado**: `apps/research/data/composition_root.py`
   (`build_storage_factory`, `build_feature_reader`).
3. data_access depende del port → **confirmado**: imports `market_data.ports.outbound.storage_factory`
   y `feature_reader` (`:49-50`).
4. no se añadió `reset()` a `StorageFactoryPort` → **confirmado** (grep vacío).
5. no se modificó `IcebergStorageFactory` para testing → **confirmado** (clase `:33-90` sin
   hooks de test; los tests usan fakes externos).
6. se añadió BC-55 → **confirmado**: `architecture/importlinter.toml:831` (forbidden
   research→`market_data.adapters`/`infrastructure`, excepción única composition_root).
7. tests usan seams/fakes → **confirmado**: `tests/research/test_data_access.py` (fakes +
   `restore_seams`).
8. mypy/ruff/import-linter pasan → **confirmado** (gates §9).
9. tests de research pasan → **confirmado** (196 passed en suites relevantes).

**Decisión ADR:** NO se crea ADR nueva para F-1. Es una implementación del patrón de
composition root ya normado por ADR-0003, materializada como contrato BC-55 (enforcement)
+ código. La regla del encargo ("NO crees una ADR solo para registrar una implementación
trivial") aplica: no hay decisión arquitectónica nueva pendiente de representar.

## 5. Consolidadción F-6 — verificación y decisión

**Estado: RESUELTO.** El detector AST cubre los 4 patrones (2 `importlib.import_module`,
2 `__import__`) con literal string, en `tests/architecture/test_import_contracts.py:45-105`;
la clase `TestDynamicMarketDataDetector` (6 tests) demuestra detección, no-detección de
imports legítimos y exclusión del CR autorizado. Límites documentados en `:413-421`
(nombres dinámicos no literales quedan fuera por diseño — no se amplía alcance sin
evidencia de necesidad real).

## 6. Remediación arquitectónica 2026-08-16 — verificación

- **ARCH-006 RESUELTO** — ports eliminados (verificado en `git status` 5 D + linter PASS +
  golden guard `test_golden_arch006_orphan_ports`). No reaparecieron.
- **ARCH-007 DEBT (no RESUELTO)** — `ExchangeCircuitOpenError` ya no duplicado
  (consolidado en domain); quedan 8 duplicados/homónimos NO eliminados (OrderStatus,
  StorageFactoryPort, AnomalyRegistryPort, QualityPipelineResult, RetryExhaustedError,
  SchemaVersionError, _TransientProxy, PipelineContext). El golden test los pinea
  (`test_golden.py:84-94`). Estado del proyecto: **DEBT** (no FAIL por decisión, sino deuda
  documentada con evidencia reproducible).
- **ARCH-008 PARCIAL→BY-DESIGN** — `InfraMetricsKafkaProducer` eliminado (golden `:108`);
  `WSTradesSource` es stub **diseñado y cableado** con fallback REST (`source_manager.py:213-234`);
  el finding restante es intencional y permanece documentado. **No se "arregla"**.
- **ARCH-003/004** — ADR-0029/0030 **ACEPTADAS** (2026-08-16); la decisión está tomada, la
  implementación queda **PENDIENTE** (tracking B-MD-008/009, cadena `implementacion`).
- **ARCH-005 BLOCKED** — requiere contrato cross-BC + cambio en dominio trading; sin ADR
  propia. No se implementa.

## 7. Inventario de ADRs

| ADR | Estado | ¿Coincide con código? | Acción |
|---|---|---|---|
| ADR-0003 | Aceptada | Sí (CR trading angosto, BC-50) | intacta |
| ADR-0004 | Aceptada | Sí (BC-50/BC-47) | intacta |
| ADR-0005 | Reemplazada por ADR-0012 | — | intacta (histórica) |
| ADR-0006 | Aceptada y verificada | Sí (portfolio dueño de posición, BC-43) | intacta |
| ADR-0007 | Aceptada | Sí (equivalencia de capas) | intacta |
| ADR-0008 | Aceptada | Sí (capas portfolio) | intacta |
| ADR-0009 | Aceptada | Sí (precedente de eliminación de huérfanos — usado en remediación) | intacta |
| ADR-0010 | Aceptada | Sí (gobernanza automatizada; `metrics_report.py`) | intacta |
| ADR-0011 | Aceptada | Sí (delegación rebalance) | intacta |
| ADR-0012 | Aceptada | Sí (runtime puro) | intacta |
| ADR-0013 | Aceptada | Sí (modelo unificado de ingesta) | intacta |
| ADR-0014 | **Propuesto** | Diseño objetivo; nota de discrepancia F-031/B-46 `:273-287` | intacta (propuesta) |
| ADR-0015 | Aceptada y verificada | Sí (app layer guard) | intacta |
| ADR-0016 | Aceptada | Sí (LiveExecutor real) | intacta |
| ADR-0017 | Aceptada | Sí (protocol discovery) | intacta |
| ADR-0020 | Aceptada | Sí (production gate) | intacta |
| ADR-0021 | **PROPUESTA** (2026-08-16) | Borrador raíz B-15 (unificación estado de posición); no implementada | escrita, pendiente decisión |
| ADR-0022 | **Propuesto** | Diseño objetivo; auditado 7-ago | intacta (propuesta) |
| ADR-0023 | Aceptada | Sí (deferral gap detection) | intacta |
| ADR-0024 | **Propuesto** | Describe estado real (NIVEL 1-3, auditado 13-ago) | intacta (propuesta) |
| ADR-0025 | Aceptada | Sí (WAC, quantity/avg) | intacta |
| ADR-0026 | Aceptada | Sí (fee semantics) | intacta |
| ADR-0027 | Aceptada | Sí (recovery/SSOT) | intacta |
| ADR-0028 | **PROPUESTA** | Borrador BookBuilder | intacta (propuesta) |
| ADR-0029 | **Aceptada** (2026-08-16) | No implementada (B-MD-008, cadena implementacion PENDIENTE) | promovida |
| ADR-0030 | **Aceptada** (2026-08-16) | No implementada (B-MD-009, cadena implementacion PENDIENTE) | promovida |

**Regla cumplida:** ADR-0029/0030 promovidas a Aceptadas **solo por decisión explícita del
owner** (2026-08-16); las demás PROPUESTAS permanecen intactas; ninguna decisión histórica
fue alterada para coincidir con el código; la evolución se registra solo por el mecanismo ADR.

## 8. Tracking — estado real

`docs/plans/tracking.yaml` (schema v2, baseline commit `dcd1741`, 2026-08-06). Entradas
relevantes y su estado verificado:

| Tracking | Estado declarado | Estado real verificado | Acción |
|---|---|---|---|
| B-MD-008 | PENDIENTE (cadena.adr HECHO 2026-08-16) | Correcto: ADR-0029 ACEPTADA; implementación PENDIENTE | **actualizado** (adr_relacionado + cadena.adr referencia/estado → ADR-0029 ACEPTADA) |
| B-MD-009 | PENDIENTE (cadena.adr HECHO 2026-08-16) | Correcto: ADR-0030 ACEPTADA; implementación PENDIENTE | **actualizado** (adr_relacionado + cadena.adr referencia/estado → ADR-0030 ACEPTADA) |
| B-15 | EN_CURSO (PARCIAL) | Correcto: mitigación de observabilidad; raíz documentada en ADR-0021 (PROPUESTA 2026-08-16) | **actualizado** (adr_relacionado + cadena.adr referencia → ADR-0021 escrita); resto intacto |
| B-16 | HECHO (2026-08-12) | Correcto: UUID4 completo + colisión guard | intacta |
| H2 apps (F-1) | RESUELTO en auditoría | Verificado en código | ya documentado en apps-audit; se confirma |
| Corolario F-6 | RESUELTO en auditoría | Verificado en tests | ya documentado en apps-audit; se confirma |
| B-51..B-56 | — | **No existen** como IDs en tracking.yaml (solo mencionados en commits B-47..B-51) | proteger (no crear) |

## 9. Gates (ejecutados 2026-08-16, estado real)

| Gate | Resultado | vs Baseline auditorías |
|---|---|---|
| `pytest tests/architecture_linter/ tests/architecture/ tests/research/` | **196 passed** | = (linter 41 + architecture + research) |
| `pytest tests/` completo | **1150 passed, 4 failed** (Kafka env, preexistentes) | = baseline (4 ambientales) |
| `mypy .` | **0 errors** (377 sources) | = |
| `ruff check .` | **0** | = |
| `lint-imports --config architecture/importlinter.toml` | **50 kept / 0 broken** | = |
| `python -m architecture_linter --root .` | **PASS=2 FAIL=7 PARTIAL=1** (19 filas / 17 violaciones) | = remediación (exit 1 esperado por FAIL/PARTIAL restantes) |
| `bandit` | 51 Low (preexistentes) | = |
| `git diff --check` | **limpio** (exit 0) | = |
| `engineering_health_check.py` | **PASS** (Plan ↔ tracker ↔ ADR ↔ contratos ↔ CI) | = |

Distingo: **0 fallos introducidos** por esta consolidación; 4 fallos preexistentes
ambientales (Kafka env); el exit 1 del linter es el estado BLOCKED/DEBT restante, no un
fallo de este trabajo.

## 10. Jerarquía de `docs/` (clarificada)

1. **Código + contratos ejecutables** (import-linter BC-NN, guards AST, linter ARCH-001..010).
2. **ADR aprobadas** (`docs/architecture/decisions/`; solo las Aceptadas son normativas).
3. **Contratos de `architecture/`** (importlinter.toml SSOT; architecture_linter.toml config).
4. **AGENTS.md / reglas operativas** (comandos, gates, gobernanza KB).
5. **Auditorías como evidencia** (`docs/audits/` — describen hallazgos, no gobiernan).
6. **PDFs/libros** (`docs/Clean Architecture*.pdf`, `docs/knowledge/`) — fuentes
   **conceptuales**, no SSOT del código.

**PDF Clean Architecture** = "fuente conceptual normativa": inspira invariantes
(Dependency Rule, 4 círculos, database-is-a-detail) que OCM materializa SOLO cuando una
ADR/contrato/código los adopta. No se cita el PDF como fuente literal de comportamiento.

## 11. Knowledge Base y PDFs (clasificación)

| Tipo | Documentos | Rol |
|---|---|---|
| Fuente conceptual normativa | `docs/Clean Architecture A Craftsman Guide...pdf` (ARCHITECTURE_SOURCE) | orienta invariantes; no SSOT |
| Decisiones concretas | ADRs aprobadas + contratos BC-NN + código | gobierno real |
| Evidencia | `docs/audits/` (incl. 2026-08-16-*) | registra hallazgos/decisiones tomadas |
| Instrucciones para agentes | `AGENTS.md`, `docs/knowledge/README.md`, `manifest.yaml` (status/authority) | cómo operar/citar |
| Fuentes conceptuales (KB) | `docs/knowledge/` (libros, papers, TIER_1..4) | contexto técnico; no regla obligatoria |
| Material histórico | TIER_3/4, `needs_verification`, `needs_attribution_review` | no citar como hecho |

Un PDF/KB solo se vuelve regla obligatoria cuando existe ADR/contrato/código que lo
materializa (regla BOOK≠CONTRACT, KB gobernanza). `manifest.yaml` mantiene 30 recursos con
status/authority ya correctamente clasificados; no se modifica.

## 12. `architecture/metrics.json` — decisión con evidencia

**Hallazgo:** stale (43 contracts vs 50 actuales; 748 pytest vs 1150; 56 vulns vs 51).
**Uso real verificado:** NO consumido por CI (`.github/workflows/*.yml` sin referencia a
metrics), NI por tests, NI por scripts de enforcement. Solo lo genera `scripts/metrics_report.py:65`
y lo citan `GOVERNANCE.md:82` y `README.md:99` como "generado". El propio script declara
que en CI se sube como artifact y **no se commitea** (`metrics_report.py:5`).

**Decisión tomada 2026-08-16 (owner): OPCIÓN B — eliminado del árbol de trabajo.**
No era SSOT ni enforcement; su snapshot versionado inducía a confusión (valores que no
correspondían al estado real). El archivo era untracked/ignorado (`.gitignore:58`); se
eliminó del disco y `scripts/metrics_report.py` permanece como generador bajo demanda
(README.md actualizado: "genera `architecture/metrics.json` bajo demanda — artifact, no
versionado"). `GOVERNANCE.md:82-83` ya declaraba correctamente el carácter de artifact.

## 13. Trazabilidad — tablas de consolidación

### 13.1 Findigns

| Tema | Estado anterior | Cambio | Estado actual | Fuente |
|---|---|---|---|---|
| ADR-0029/0030 | PROPUESTA | **promovidas a Aceptadas (owner 2026-08-16)**; tracking cadena.adr HECHO | **ACEPTADAS — implementación pendiente** | ADR-0029, ADR-0030, tracking B-MD-008/009 |
| metrics.json | stale | **eliminado** (Opción B) | **ELIMINADO** — generador permanece | §12 |
| ADR-0021 | inexistente | **redactada como PROPUESTA** (raíz B-15) | **PROPUESTA** — pendiente decisión | ADR-0021, tracking B-15 |
| ARCH-007 | DEBT (8) | decisión: mantener como DEBT (owner 2026-08-16) | **DEBT confirmado** | golden `test_golden.py:77-97` |
| F-1 research DIP | RESUELTO (auditoría 14-08) | verificado contra código + gates | **RESUELTO** | `data_access.py`, `composition_root.py`, BC-55, 196 tests |
| F-6 imports dinámicos | RESUELTO (auditoría 14-08) | verificado contra tests | **RESUELTO** | `test_import_contracts.py:45,403-459` |
| ARCH-006 ports huérfanos | FAIL (8) → remediación | verificado linter PASS + golden | **RESUELTO** | linter, `test_golden.py:56-73` |
| ARCH-007 duplicados | FAIL (9) → (8) | ExchangeCircuitOpenError consolidado | **DEBT** (8 restantes) | linter, `test_golden.py:77-97` |
| ARCH-008 stubs | FAIL (2) → (1) | InfraMetricsKafkaProducer eliminado | **BY-DESIGN** (WSTradesSource) | linter, `test_golden.py:101-110` |
| ARCH-003/004 | FAIL/PARTIAL | ADR aprobada; implementación pendiente | **APROBADO — impl pendiente** | ADR-0029/0030, tracking B-MD-008/009 |
| ARCH-005 | FAIL | cross-BC, sin ADR | **BLOCKED** | — |
| ARCH-001/002/010 | FAIL | no tocado | **BY-DESIGN / DEBT** | ADR-0006, ADR-0025, B-15 |
| metrics.json | stale | eliminado (Opción B) | **ELIMINADO** | §12 |

### 13.2 ADRs

| ADR | Estado | ¿Coincide con código? | Acción |
|---|---|---|---|
| ADR-0029 | Aceptada (2026-08-16) | No implementada | promovida; implementación pendiente |
| ADR-0030 | Aceptada (2026-08-16) | No implementada | promovida; implementación pendiente |
| ADR-0021 | PROPUESTA (2026-08-16) | No implementada | escrita (raíz B-15); pendiente decisión |
| ADR-0006/0009/0010/0025/0026/0027 | Aceptadas | Sí | intactas |
| ADR-0005 | Reemplazada | — | histórica |
| ADR-0014/0022/0024/0028 | Propuestas | diseño objetivo | intactas |

### 13.3 Tracking

| Tracking | Estado | Evidencia | Acción |
|---|---|---|---|
| B-MD-008 | PENDIENTE (cadena.adr HECHO) | ADR-0029 ACEPTADA, impl pendiente | actualizado |
| B-MD-009 | PENDIENTE (cadena.adr HECHO) | ADR-0030 ACEPTADA, impl pendiente | actualizado |
| B-15 | EN_CURSO | mitigación; raíz documentada ADR-0021 PROPUESTA | actualizado (enlace ADR-0021) |
| B-16 | HECHO | UUID4 + guard | intacta |
| F-1/F-6 | RESUELTO | código+tests | documentado |

## 14. NO tocar (protecciones explícitas)

- **B-15** — preexistente; mitigación intacta; solo se enlazó ADR-0021 (no se cambió su estado).
- **B-51..B-56** — no existen como IDs; no crear.
- **ADR-0024, ADR-0028** — PROPUESTA; intactas. **ADR-0029/0030** ahora Aceptadas (decisión del owner).
- **Dockerfile, docker-compose.yml** — no tocados.
- **tracking fuera del alcance** — sin retrofit masivo.
- **cambios preexistentes del usuario** — `AGENTS.md` (M, preexistente) no tocado.
- **código de negocio no relacionado** — no se hace limpieza general ni refactors
  oportunistas; no se "arregla" StorageFactoryPort duplicado (deuda F-1 documentada)
  porque no forma parte de una decisión explícita de esta iniciativa.

## 15. Resultado final esperado — estado consolidado

- No quedan auditorías contradictorias sin explicación: la matriz §3 y el informe
  remediación explican cada estado; `architecture-linter.md` (baseline FAIL=9) quedó
  superada por `architecture-remediation.md` (PASS=2/FAIL=7/PARTIAL=1) — verificado.
- Las ADR reflejan decisiones reales: ADR-0029/0030 Aceptadas (owner 2026-08-16); las
  demás propuestas siguen siendo propuestas; ADR-0021 redactada como PROPUESTA.
- tracking refleja estado real: B-MD-008/009 con cadena.adr HECHO (ADR aceptada) e
  implementación PENDIENTE; B-15 EN_CURSO con ADR-0021 enlazada.
- Findings resueltos con evidencia (F-1, F-6, ARCH-006, ARCH-009).
- Pendientes con razón/dueño: implementación pendiente (ARCH-003/004 por ADR-0029/0030),
  BLOCKED (ARCH-005 cross-BC), DEBT (ARCH-007 8 duplicados, ARCH-002, ARCH-010 vía B-15),
  BY-DESIGN (ARCH-008 WSTradesSource, ARCH-001).
- PDFs como fuentes conceptuales, no falsas SSOT (jerarquía §10).
- `architecture/` vs `architecture_linter/` separados (consolidación A).
- F-1 y F-6 con estado coherente RESUELTO.
- Remediación ARCH-001..010 correctamente registrada.
- Un agente futuro puede leer §3-§14 y saber qué implementar (ADR-0029/0030 ya aprobadas;
  pasos de implementación en las secciones `Implementation Roadmap` de cada ADR) y qué NO tocar.

## 16. Decisiones resueltas y pendientes (2026-08-16)

**Resueltas por el owner en esta sesión:**
1. **ADR-0029 y ADR-0030 aprobadas** → Aceptadas; desbloquea ARCH-003/004 para
   implementación (PENDIENTE). Próximo paso: ejecutar los `Implementation Roadmap` de
   ambas ADR (tracking B-MD-008/009, cadena `implementacion`).
2. **`architecture/metrics.json` eliminado** (Opción B) — confirmado por el owner.
3. **ARCH-007** mantenido como **DEBT** — confirmado por el owner.

**Pendientes (requieren decisión humana):**
4. **ADR-0021 (raíz B-15)** — borrador redactado como PROPUESTA; requiere aprobación para
   implementar la unificación del estado de posición en PortfolioService.
5. **ADR-0028 (BookBuilder)** y **ADR-0014/0022/0024** — siguen PROPUESTA (sin decisión).
6. **ARCH-005** — requiere decisión cross-BC (sin ADR propia).
7. **ARCH-002** (divergencia semántica WAC vs replace/pop) — requiere decisión financiera.