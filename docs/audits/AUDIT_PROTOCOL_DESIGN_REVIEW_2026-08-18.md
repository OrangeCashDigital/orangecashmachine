# AUDIT PROTOCOL DESIGN REVIEW — Modelo Operativo de Auditoría de OrangeCashMachine

**Fecha:** 2026-08-18
**Commit auditado:** `bee9fb5a3917c32fcc81fcc81fa5177ce0e57283`
**Branch:** `main`
**Objetivo:** diseñar y validar el SISTEMA que hará que auditorías de cualquier agente
(Gemini, DeepSeek/OpenCode, Claude, Codex) sean comparables, deterministas y gobernadas
por el proyecto. **No** es una auditoría de compliance de la aplicación.

**Método:** descubrimiento de fuentes de autoridad reales (sin asumir), lectura normativa,
reconciliación de informes concurrentes, y diseño derivado exclusivamente de los artefactos
existentes del proyecto.

> **Nota de concurrencia:** durante esta sesión apareció en `docs/audits/` un informe de otra
> sesión (`AUDIT_AGENTS_MD_PROTOCOL_2026-08-18.md`) con conclusiones compatibles (AGENTS.md
> carece de protocolo de auditoría; recomienda crear un `AUDIT_PROTOCOL.md`). Se cita como
> fuente concurrente en §4 y §14; no es fuente normativa de este diseño.

---

## 1. Current governance model

El proyecto ya posee un sistema de gobierno explícito y maduro. Se reconstruye desde
sus propios artefactos (no desde una metodología externa).

### 1.1 Artefactos normativos (Constitución, Plan Maestro §12)

| N | Artefacto | Rol | Ubicación |
|---|---|---|---|
| N1 | Plan Maestro | Especificación normativa del cambio; fases + DOR/DOD | `docs/PLAN-Maestro-Ingenieria.md` |
| N2 | tracking.yaml | SSOT operativo del backlog y hallazgos | `docs/plans/tracking.yaml` (v2) |
| N3 | ADRs | Decisiones de arquitectura (activo) | `docs/architecture/decisions/ADR-*.md` |
| N4 | Contratos de arquitectura (BC-NN) | Boundaries/capas | `architecture_linter/importlinter.toml` |
| N5 | Contratos de código (AST guards) | Invariantes auto-defendibles | `tests/architecture/` |
| N6 | CI | Puerta del cambio (fail-fast) | `.github/workflows/ocm-ci.yml` |
| N7 | Auditorías | Fotografías históricas inmutables | `docs/audits/` |

### 1.2 Regla suprema (Plan §preamble)

> "No se implementará ninguna funcionalidad nueva si degrada cualquiera de los artefactos
> normativos del proyecto." Orden inviolable del cambio: `Plan → Tracking → ADR → Código →
> Tests → CI → Release`.

### 1.3 Cadena maestra de trazabilidad (Plan §2)

`Hallazgo → Backlog → ADR → Implementación → Tests → CI → Evidencia → Cierre`
Cada eslabón tiene `estado` (PENDIENTE/HECHO/PARCIAL/NO_APLICA) y `referencia`/`evidencia`.
La SSOT operativa de la cadena es `tracking.yaml`; el Plan es el mapa.

### 1.4 Estados válidos del tracking (SSOT del tracker: cabecera de tracking.yaml + `engineering_health_check.py:36-45`)

- `estado`: PENDIENTE | EN_CURSO | HECHO | VERIFICACION | RECHAZADO
- `estado_auditoria`: CONFIRMADO | NO_CONFIRMADO | PARCIALMENTE_CONFIRMADO | REFORMULADO
- `prioridad`: CRITICA | ALTA | MEDIA | BAJA
- `fase`: F1 | F2 | F3 | F4 | F5
- `cadena.*.estado`: NO_APLICA | PENDIENTE | HECHO | PARCIAL
- `reglas[].backtest`: ok | pendiente | fail; `reglas[].activada_en_ci`: bool

Vigente (verificado 2026-08-18): 48 hallazgos (35 HECHO / 12 PENDIENTE / 1 EN_CURSO;
47 CONFIRMADO / 1 PARCIALMENTE_CONFIRMADO) + 16 reglas (13 `activada_en_ci: true`).

### 1.5 Estados de ADR (template + archivos reales)

`Propuesto | Aceptado | Reemplazado por ADR-XXXX | Obsoleto` (template).
Real en repo: `Aceptado` (mayoría), `Propuesto` (ADR-0014, 0022, 0024, 0028),
`Reemplazado` (ADR-0005). NO existe el estado "Superado".

Patrones de estado vigentes verificados:
- ADR-0021: **PROPUESTA** — borrador para decisión humana; ningún contrato cambia.
- ADR-0029/0030: **ACEPTADA pero NO implementada** — la decisión existe; la implementación
  vive en tracking (B-MD-008 / B-MD-009, cadena `implementacion` PENDIENTE). Contratos solo
  cambian cuando la implementación se ejecuta y los gates pasan.
- ADR-0022/0024: formato de header `## Estado` (Propuesto) distinto del template — variante
  documental de la serie, no una categoría nueva.

### 1.6 Contratos de arquitectura

- `architecture_linter/importlinter.toml`: 50 contratos `[[tool.importlinter.contracts]]`
  (BC-NN, 50 KEPT / 0 BROKEN verificado 2026-08-18; numeración hasta BC-54 con BC-02/28/31
  eliminados por comentario). Gate CI job `architecture` (≥49 contratos, no-vacuo).
- `tests/architecture/`: AST guards (app_layer_guard, kafka wiring, kappa publisher,
  engineering health, docker hardening, config parity).
- `architecture_linter/` (standalone, ARCH-001..010): **NO es gate CI**; mide invariantes
  semánticas por AST. Golden fija estado esperado.

### 1.7 CI/CD (ocm-ci.yml, 10 jobs, fail-fast)

`architecture` → `engineering-health` → `app-guard`, `trading-guards`, `unit-tests`,
`security`, `integration-tests`, `config-validation`, `quality`. Gates con backtest
`activada_en_ci` en tracking. Pip-audit con ignore-list de risk-accept documentado
(2026-08-03: pyarrow PYSEC-2026-113, ecdsa PYSEC-2026-1325).

### 1.8 Engineering Health Check (F2.0, `scripts/engineering_health_check.py`)

Gate de coherencia Plan ↔ tracking ↔ ADR ↔ contratos ↔ CI. Valida: YAML parseable, enums
cerrados, `backtest ok ⇒ activada_en_ci true`, `HECHO ⇒ fecha_cierre + cadena.cierre.evidencia`,
`CONFIRMADO ⇒ evidencia`, contratos ≥ 49 no-vacuo, cada gate CI mapeado a regla activa.

### 1.9 Golden states (`tests/architecture_linter/test_golden.py`)

`GOLDEN_EXPECTED` fija el estado esperado de cada regla ARCH-NNN como deuda conocida:
ARCH-001 FAIL, ARCH-002 FAIL, ARCH-003 PARTIAL, ARCH-004 FAIL, ARCH-005 FAIL, ARCH-006 PASS,
ARCH-007 FAIL, ARCH-008 FAIL, ARCH-009 PASS, ARCH-010 FAIL (7 FAIL / 1 PARTIAL / 2 PASS).

**Semántica (verificada en `docs/audits/2026-08-17-architecture-linter-golden-vs-standalone-revalidation.md:61-82`):**
- El linter standalone mide arquitectura contra el ideal (Clean Architecture).
- El test golden mide **regresión** respecto a deuda ya aceptada y documentada.
- `GOLDEN PASS ≠ arquitectura correcta`; significa "el estado observado coincide con el estado
  esperado/documentado". Mecanismo de **no-regresión**, no de corrección.

### 1.10 Recursos adicionales

- `docs/plans/engineering-guardrails.md`: vista de estado de guardrails; ante discrepancia
  mandan `tracking.yaml` y `backlog-priorizado-2026-08-08.md`.
- `docs/architecture/GOVERNANCE.md`: cuándo requiere ADR (cambiar firma de CR, agregar/eliminar
  contrato, cambiar dueño de estado mutable, posponer deuda técnica), serie canónica vs heredada.

---

## 2. Source-of-truth hierarchy

### 2.1 Jerarquía normativa del proyecto (Plan §12)

```
Plan (N1) → Tracking (N2) → ADR (N3) → Contratos (N4/N5) → Código → Tests → CI → Release
```

Reglas de resolución de conflicto (Plan §12, explícitas):
- Cuando N1 (Plan) y N2 (tracking) divergen → **N2 gana** para el estado del backlog.
- Cuando N3 (ADR) y el código divergen → **N3 gana**, y se abre un hallazgo.
- La coherencia entre todos la valida el **Engineering Health Check (F2.0)**.

### 2.2 Jerarquía de autoridad para AUDITORÍA (derivada, de uso del auditor)

Para el auditor la pregunta no es "quién manda", sino "qué define el **estado esperado**".
Derivada directamente de §2.1:

| # | Pregunta | Fuente canónica |
|---|---|---|
| 1 | ¿Cuál es el estado esperado del backlog? | tracking.yaml (N2) — SSOT operativo |
| 2 | ¿Cuál es la decisión arquitectónica? | ADR (N3), estados Propuesto/Aceptado/Reemplazado |
| 3 | ¿Cuáles son los límites de paquetes/capas? | importlinter (N4) + AST guards (N5) |
| 4 | ¿Cuál es el estado esperado de las invariantes ARCH-NNN? | GOLDEN_EXPECTED (deuda fijada) |
| 5 | ¿Cuáles son los gates obligatorios? | ocm-ci.yml + reglas `activada_en_ci` |
| 6 | ¿Qué fases y DOR/DOD rigen? | Plan Maestro (N1) §4 |
| 7 | ¿Qué es el estado histórico inmutable? | `docs/audits/` (N7) — no se editan |

**Regla de precedencia en conflicto (para el auditor):**
1. Contratos y código ejecutable > documentos (lo que corre es autoridad de hecho).
2. Entre documentos normativos, el Plan §12 (N1→N2→N3).
3. La práctica externa (OWASP, SRE, Clean Architecture "de libro") es **contexto técnico**,
   NO autoridad sobre el estado del proyecto, salvo que governance la haya adoptado (la
   KB del proyecto ya lo establece: "Libro ≠ contrato"; "Documentación oficial primero").

### 2.3 Jerarquía de autoridad de la KB (AGENTS.md — "Gobernanza de la KB")

```
código y comportamiento ejecutable/tests → contratos e invariantes arquitectónicos →
ADRs → documentación oficial de tecnologías → documentación interna y KB → literatura externa
```
Compatible y complementaria a §2.1; ambas derivadas de los artefactos del proyecto.

---

## 3. Current AGENTS.md assessment

### 3.1 Qué cubre hoy AGENTS.md

- Comandos (uv, lint-imports con `--config`, ruff, mypy, bandit, pytest, ocm, docker).
- Orden CI fail-fast; pre-commit; dirección de dependencias; remap de paquetes; arquitectura.
- Migración pandas→polars (SSOT de transforms, puente eliminado).
- Gotchas operativos (import-linter 2.x, `--cfg job`, BOOL_TRUE, dry_run, BC-35, env_vars SSOT).
- **Gobernanza de la KB** (jerarquía de autoridad, "Libro ≠ contrato", flujo de consulta,
  conocimiento ≠ evidencia de trading).
- Package remapping, git workflow, tool ownership.

### 3.2 Qué NO cubre (brechas relevantes para auditoría)

| Brecha | Impacto |
|---|---|
| No define flujo de auditoría (estado esperado → observado → diferencia) | Agente arranca por "¿qué problemas encuentro?" |
| No define taxonomía de clasificación de findings (NUEVO/REVALIDADO/...) | Mismas condiciones clasificadas distinto por cada agente |
| No define reglas de deduplicación contra tracking/ADRs/auditorías previas | Findings duplicados o agregados arbitrariamente |
| No separa CONTROL STATUS de FINDING STATUS | Un FAIL de control se reporta como defecto NUEVO |
| No define estados de control (PASS/FAIL/PARTIAL/NO_VERIFICADO/INFRA_FAILURE) | "NO_VERIFICADO" usado como sinónimo de FAIL o ausencia |
| No define semántica de golden (no-regresión, no corrección) | GOLDEN PASS se lee como "arquitectura correcta" |
| No define read-only de auditoría | Riesgo de modificar código/tracking/ADRs durante auditoría |
| No define reconciliación entre auditorías concurrentes | Se elige arbitrariamente un informe o se ignoran los otros |
| No define formato de informe/registro canónico | Estructura documental varía entre agentes |
| No define verificación final obligatoria | Contadores no cuadran con tablas; se entrega con discrepancias |
| No define trazabilidad (finding→control→tracking→ADR→...) con NOT_TRACED | Se inventan relaciones o IDs |
| No define el veredicto canónico | "COMPLIANT"/"SECURE" sin criterio del proyecto |

### 3.3 Conclusión

AGENTS.md es un excelente manual operativo de **desarrollo**, pero **no es un manual de
auditoría**. No contiene protocolo de auditoría, clasificación, deduplicación, read-only,
reconciliación ni golden semantics. Esto explica la variabilidad observada (§4).

---

## 4. Current audit behavior assessment

### 4.1 Evidencia: 5 informes concurrentes sobre el mismo commit

| Informe | Findings | Clasificación | Controles | CI/CD | ADR count | pip-audit |
|---|---|---|---|---|---|---|
| `AUDIT_FORENSE_COMPLIANCE_2026-08-18.md` (20:51) | 2 (F-SEC-01, F-ARCH-01) | taxonomía de evidencia propia | 8 | NO_VERIFICADO | 30 (erróneo) | 6 CVEs (sin ignore) |
| `AUDIT_OCM_COMPLIANCE_AND_GOVERNANCE.md` (16:47) | 6 gaps sin IDs | taxonomía propia + AMBIENTAL | 13 | NO_VERIFICADO | 27 (correcto) | 4 (comando CI) |
| `AUDIT_OCM_COMPLIANCE_GOVERNANCE_ARCHITECTURE_2026-08-17.md` (20:44) | 3 (FINDING-01..03) | taxonomía propia | 11 | **PASS** (solo config) | 30 (erróneo) | 6 CVEs (sin ignore) |
| `AUDIT_OCM_FORENSIC_COMPLIANCE_2026-08-18.md` (20:58) | 2 (F-SEC-01 NUEVO, F-ARCH-01 REVALIDADO) | NUEVO/REVALIDADO + Tier 1/2/3 | 8 | NO_VERIFICADO | (no cuantifica) | 6 CVEs (sin ignore) |
| `AUDIT_OCM_TECHNICAL_COMPLIANCE_2026-08-18.md` (21:28, canónico) | 16 (F-CI-xx, F-ARCH-xx, F-GOV-xx, F-SC-xx) | severidades + trazabilidad | 23 | FAIL (gh) | 27 (correcto) | 4 (comando CI, exit 1) |

### 4.2 Método observado divergente (causas raíz)

1. **Comando de evidencia distinto** (pip-audit 6 vs 4): los que corren `pip-audit .` pelado
   ven 6 CVEs; los que usan el comando exacto de CI (`--ignore-vuln` pyarrow/ecdsa) ven 4.
   → No es contradicción de hecho; es alcance de control no normalizado.
2. **Conteo de ADRs por numeración vs por archivos** (30 vs 27): los que cuentan la numeración
   máxima ("hasta ADR-0030") erró; el conteo real de archivos es 27 (26 ADR + template; faltan
   0001/0002/0018/0019). También se inventó el estado "Superado" (no existe; el real es
   "Reemplazado").
3. **Granularidad arbitraria** (2 / 3 / 6 / 16 findings): mismo estado del repo. Los agregados
   enmascaran que ARCH-005/007/008 no tienen trazabilidad ADR/tracking mientras ARCH-001/002/010 sí.
4. **Severidad del mismo hecho distinta** (CVEs HIGH vs CRITICAL): depende de si se evalúa el
   gate CI real (CRITICAL) o solo la deuda (HIGH).
5. **Control CI/CD triple resultado** (NO_VERIFICADO / PASS / FAIL): evaluar la declaración del
   workflow (config) vs su ejecución remota real. El PASS config-only es metodológicamente débil.
6. **Verificación remota solo en 1 de 5**: yamllint FAIL solo lo detectó el informe que usó `gh`.
7. **5 taxonomías de evidencia incompatibles** (EVIDENCE/SSOT vs EVIDENCIA/OPERATIVA vs
   EVIDENCIA_REPRODUCIDA vs Tier 1/2/3 vs EVIDENCIA/CI/CONTRADICCIÓN).
8. **NUEVO/REVALIDADO solo explícito en 1 de 5**; el resto clasifica por severidad o no clasifica.
9. **Disciplina de ejecución inconsistente**: `engineering_health_check` NO_VERIFICADO en el
   informe que no lo ejecutó ("leído, no ejecutado") vs PASS en los que sí.
10. **Concurrencia sin registro formal** de sesiones auditoras (5 informes en 4h47m, más el
    de esta sesión), con riesgo de colisión en `origin/main`.

### 4.3 Conclusión

La variabilidad no es de "opinión": es de **método no gobernado**. No hay una fuente canónica
que defina qué comando correr, cómo clasificar, cómo granularizar, cómo reconciliar o cómo
verificar. El informe técnico (21:28) es el único que (a) usó el comando de CI para pip-audit,
(b) verificó CI remoto con gh, (c) enumeró tracking (48/16), (d) construyó matriz de
reconciliación, (e) separó deuda trazada de no trazada. **Pero su método tampoco está escrito
en ningún artefacto normativo** — es el comportamiento de una sesión, no un protocolo.

---

## 5. Agent inconsistency risks

Riesgos concretos (cada uno con su mitigación diseñada en §6-§12):

| # | Riesgo | Consecuencia hoy | Mitigación (protocolo) |
|---|---|---|---|
| R1 | Empezar por "qué problemas encuentro" | Deuda gobernada reportada como sorpresa | Flujo §6 (estado esperado primero) |
| R2 | Falta de taxonomía de clasificación | Misma condición = NUEVO en un agente, REVALIDADO en otro | State machine §7 |
| R3 | Falta de deduplicación | Duplicados contra tracking/ADRs/auditorías | Reglas §9 (7 pasos) |
| R4 | Confundir CONTROL FAIL con FINDING NUEVO | Falsos findings de deuda gobernada | Separación §8 + regla de gobierno |
| R5 | Comando de evidencia no normalizado | pip-audit 6 vs 4; yamllint invisible | §13: comandos canónicos (SSOT) |
| R6 | Conteo/inventario por inferencia | ADRs "30" y estado "Superado" inexistentes | §13: inventariar directorios reales |
| R7 | CI evaluado por declaración, no por ejecución | PASS config-only enmascara FAIL real | §8: verificación remota obligatoria |
| R8 | Granularidad arbitraria de findings | 2 vs 16 findings para el mismo estado | §9 dedup + §6 desglose por root cause |
| R9 | Severidad desde práctica externa | HIGH vs CRITICAL sin anclaje | §7: severidad según governance/gates |
| R10 | Golden leído como corrección | GOLDEN PASS = "arquitectura correcta" | §6/§10: golden = no-regresión |
| R11 | Sin matriz de reconciliación entre informes | Se elige arbitrariamente un informe | §10 |
| R12 | Sin read-only | Modificación de tracking/ADRs/código | §12 |
| R13 | Sin trazabilidad con NOT_TRACED | Relaciones inventadas | §11 |
| R14 | Sin verificación final de consistencia | Contadores no cuadran | §6 fase final |
| R15 | Escritura concurrente a origin/main sin registro | Colisiones y pérdida de evidencia | §12 + registro de sesiones (recomendado) |

---

## 6. Canonical audit lifecycle

Derivado de la regla fundamental del encargo y de los flujos del Plan Maestro (§2, §13).

### 6.1 Orden obligatorio

```
ESTADO ESPERADO (según governance: tracking + ADR + contratos + golden + CI)
    ↓
EVIDENCIA HISTÓRICA (auditorías previas, findings, N7)
    ↓
ESTADO ACTUAL (ejecución de controles con comandos canónicos)
    ↓
RECONCILIACIÓN (matriz: previos + concurrentes + evidencia actual)
    ↓
CLASIFICACIÓN (una de las 7 categorías, §7)
    ↓
SEVERIDAD (anclada a governance, no a práctica externa)
    ↓
TRAZABILIDAD (cadena con NOT_TRACED, §11)
    ↓
DECISIÓN (humana si aplica; D#)
    ↓
FINDING (entrada en registro + informe)
```

### 6.2 Fases del encargo (mapa al orden de ejecución)

1. Descubrir AGENTS.md y obedecer instrucciones de agente.
2. Descubrir protocolo de auditoría (AUDIT_PROTOCOL.md); si no existe, reportar gap.
3. Leer Plan Maestro (§1-§13), GOVERNANCE, tracking.yaml, ADRs, contratos, golden, CI, linters.
4. Construir baseline: commit, branch, working tree, CI conocido, tracking, ADRs, contratos,
   findings previos, tests, linters, seguridad, documental.
5. Determinar GOLDEN/EXPECTED STATE.
6. Reconciliar deuda conocida (Fase 2 del protocolo de auditoría).
7. Ejecutar controles relevantes (solo los del sistema de gobierno).
8. Investigar divergencias.
9. Deduplicar findings.
10. Clasificar findings.
11. Generar/actualizar informe + registro.
12. Verificar consistencia (contadores = tablas; IDs únicos; trazabilidad; read-only; working tree).
13. Entregar resumen ejecutivo.

### 6.3 No-regresión (definición canónica)

Una condición es REGRESIÓN si estaba registrada como resuelta/gobernada (estado HECHO en
tracking, o ADR Aceptada con cadena implementacion HECHO, o golden PASS) y el estado actual
vuelve a fallar. Si el estado esperado (golden) también cambia de estatus sin remediación,
eso es además una **regresión del golden** (mecanismo de no-regresión roto).

---

## 7. Finding state machine

### 7.1 Clasificaciones canónicas (definiciones exactas)

| Clasificación | Cuándo corresponde | Ejemplo en OCM |
|---|---|---|
| **NUEVO** | No existe en tracking, ni en ADR, ni en auditoría previa, ni en golden; es una condición real verificada por evidencia. | F-CI-01 (pip-audit 4 CVEs sin mitigar, sin tracking/ADR); F-CI-02 (yamllint alerts.yml); F-ARCH-04 (freshness sin tracking); F-GOV-01 (INVENTORY.md ausente) |
| **REVALIDADO** | La condición ya está registrada (tracking/ADR/golden/auditoría previa) y la evidencia actual la confirma; no hay cambio de estado. | F-ARCH-01/002/003 (B-15/B-MD-008/B-MD-009 + ADR-0021/0029/0030); F-ARCH-05/006 (deuda fijada en golden) |
| **REGRESIÓN** | Estaba resuelto/gobernado (HECHO, PASS) y vuelve a fallar. | Ninguna en este commit (golden 4 passed; ninguna regla activa falló respecto a su backtest) |
| **CERRADO** | Estaba abierto y la evidencia demuestra que está resuelto (con evidencia de cierre: fecha_cierre + cadena.cierre). | Hallazgos HECHO del tracking (B-01..B-35) re-verificados en esta auditoría |
| **CONTRADICCIÓN** | Una fuente afirma PASS/estado X y otra evidencia normativa/ejecutable demuestra FAIL/estado Y. | F-GOV-05 (LICENSE PolyForm vs pyproject.toml MIT vs README MIT) |
| **RECOMENDACIÓN** | Mejora sin obligación normativa; práctica externa (OWASP/SRE/etc.) no adoptada por governance; o gap documental sin efecto de contrato. | F-CI-03 (linter no gate), F-GOV-02/03/04 (drift documental), F-SC-01/02 (pinning, SBOM) |
| **NO_VERIFICADO** | No existe evidencia suficiente (no se ejecutó, infra ausente, acceso remoto sin credenciales). Nunca es sinónimo de FAIL ni de PASS. | — (en este commit todo se ejecutó o se marcó INFRA_FAILURE) |

### 7.2 Reglas de decisión

1. Primero reconciliar con el histórico (dedup §9). Nunca clasificar NUEVO sin esa búsqueda.
2. Un FAIL técnico con deuda formalmente registrada (tracking/ADR/golden) → **REVALIDADO**,
   NUNCA NUEVO (a menos que la deuda esté fuera de su gobernanza, ej. vencida).
3. Un FAIL de control sin cobertura → potencial NUEVO, tras dedup.
4. Una diferencia entre fuentes normativas → **CONTRADICCIÓN** + matriz de reconciliación.
5. Práctica externa no adoptada → **RECOMENDACIÓN** (contexto técnico, no autoridad).
6. Sin evidencia → **NO_VERIFICADO**.
7. `GOLDEN PASS` ≠ correcto: es no-regresión. Un cambio de estatus golden sin remediación
   registrada es regresión del mecanismo.

### 7.3 Severidad (anclada al governance)

| Severidad | Ancla (governance) |
|---|---|
| CRITICAL | Gate CI activo roto (bloquea merge) o riesgo de capital real (fases F1/F3 "lo que causa pérdidas") |
| HIGH | Violación de artefacto normativo (ADR/contrato/tracking) no gobernada; contradicción de governance |
| MEDIUM | Deuda no gobernada sin violación directa de contrato; gate roto en job no crítico |
| LOW | Drift documental / consistencia sin efecto de contrato |
| INFO | Observación / recomendación de mejora de proceso |

### 7.4 Estados del finding (status)

`OPEN → GOVERNED → RESOLVED` (con fecha y evidencia), o `OPEN → WONTFIX/RECHAZADO` por
decisión humana. El registro `OCM_AUDIT_FINDINGS_*.md` mantiene Status por finding; el cierre
real vive en tracking (eslabón cierre), no en el registro.

---

## 8. Control state model

### 8.1 Estados canónicos (5)

| Estado | Definición | Evidencia | No es sinónimo de |
|---|---|---|---|
| PASS | Control ejecutado; resultado = esperado | Salida + comando | — |
| FAIL | Control ejecutado; resultado ≠ esperado (viola gate/contrato) | Salida + comando | finding NUEVO automático |
| PARTIAL | Control ejecutado; cumplimiento parcial | Salida | FAIL de producto |
| NO_VERIFICADO | No ejecutado / sin evidencia | — | PASS ni FAIL |
| INFRA_FAILURE | Falló el entorno de auditoría, no el sistema | Diagnóstico de infra | FAIL del producto |

### 8.2 Cuándo INFRA_FAILURE (regla)

Aparece cuando el control no pudo ejecutarse por causa de entorno: infra ausente (Kafka local),
servicio no desplegado, caché stale, red/bloqueo, permisos. **Nunca se convierte en FAIL del
producto**; se registra como INFRA_FAILURE y se re-evalúa el control en CI remoto (que es el
gate real). Ejemplo verificado: integración local Kafka = INFRA_FAILURE; job `integration-tests`
remoto = SUCCESS → el producto no tiene fallo de integración.

### 8.3 CONTROL STATUS ≠ FINDING STATUS (regla de gobierno)

Un FAIL de control **no genera automáticamente un finding NUEVO**. Flujo obligatorio:
1. ¿El control falla? → registrar CONTROL FAIL.
2. ¿La condición ya está en tracking/ADR/golden? → REVALIDADO (o REGRESIÓN si volvió).
3. ¿Está gobernada pero con decisión pendiente? → REVALIDADO + decisión humana.
4. ¿No está gobernada? → NUEVO, tras dedup.
5. ¿Es práctica no adoptada? → RECOMENDACIÓN.

Ejemplo aplicado (23 controles de esta sesión): 4 controles FAIL → solo 2 findings NUEVOS
(F-CI-01, F-CI-02); secret scanning nativo y pipeline CI/CD no generan finding adicional.

---

## 9. Deduplication rules

Antes de crear un finding, ejecutar en orden (todas obligatorias):

1. Buscar el **mismo finding** en el registro actual (por claim, no por ID).
2. Buscar la **misma causa raíz** (dos síntomas = un finding).
3. Buscar **sinónimos** (misma condición, nombres distintos).
4. Buscar **tracking relacionado** (tracking.yaml: ID B-NN, hallazgo_informe, solucion, cadena).
5. Buscar **ADR relacionada** (docs/architecture/decisions/: tema, estado, referencias).
6. Buscar **auditorías anteriores** (docs/audits/: mismos claim/control/causa).
7. Buscar **findings cerrados** (registros previos + tracking HECHO/RECHAZADO).

Si la condición ya existe → **NO crear** nuevo finding; clasificar REVALIDADO / REGRESIÓN /
CONTRADICCIÓN según corresponda (§7).

Granularidad: **un finding por causa raíz**, con desglose por control distinto solo cuando el
control aporta información de severidad/trazabilidad distinta. Regla aplicada: ARCH-001+002+010
→ dominio "Position State Ownership" (F-ARCH-01); pip-audit 4 advisories → 1 finding (F-CI-01);
2 controles FAIL adicionales → sin findings nuevos (misma causa raíz / práctica no adoptada).

---

## 10. Reconciliation rules

### 10.1 Entre auditorías concurrentes

Si existen varios informes sobre el mismo commit: NO elegir arbitrariamente. Construir matriz:

| Finding/Control | Informe A | Informe B | Informe C | Evidencia actual | Estado reconciliado | Razón |
|---|---|---|---|---|---|---|

Reglas:
- Las discrepancias deben explicarse (ej. pip-audit 6 vs 4 = alcance del comando, no contradicción).
- Los errores de inventario se corrigen contra el filesystem real (ADRs = 27 archivos, no 30).
- Los estados inventados se corrigen contra el template/archivos (no existe "Superado").
- La verificación remota (gh) es autoridad sobre la declaración local cuando aplica.
- Cada hecho mal reportado se registra con su corrección y razón.

### 10.2 Con tracking/ADR/golden

- Tracking es SSOT del estado del backlog: si tracking dice PENDIENTE y el código "parece"
  resuelto, el estado es PENDIENTE hasta que la cadena cierre con evidencia.
- ADR ACEPTADA sin implementar = decisión existe, implementación pendiente → REVALIDADO con
  nota, no CONTRADICCIÓN.
- Golden fija estado esperado; si el linter real coincide → no-regresión (PARTIAL/PASS según
  control), no nuevo defecto.

### 10.3 Resultado aplicado (matriz resumida)

| Afirmación | Informes previos | Reconciliado | Razón |
|---|---|---|---|
| pip-audit CVEs | 6 (×3) / 4 (×2) | **4 sin mitigar, exit 1** | comando exacto de CI (ignore pyarrow/ecdsa) |
| Nº ADRs | 30 (×2) / 27 (×2) / sin cuantificar | **27 archivos** | conteo de filesystem real |
| Estados ADR | "Superado" (×2) | **Aceptado/Propuesto/Reemplazado** | template + archivos reales |
| CI/CD | NO_VERIFICADO (×3) / PASS (×1) | **FAIL** (quality + yamllint) | verificación remota gh (runs 32069832325/32069832475) |
| Linter standalone | 7/1/2 (×5) | **7 FAIL / 1 PARTIAL / 2 PASS** | reproducible con `--no-cov`; exit-code con `--cov` global = AMBIENTAL |
| Golden | 4 passed | **4 passed, no-regresión** | GOLDEN_EXPECTED fija deuda |

---

## 11. Traceability model

Todo finding debe intentar conectar la cadena completa:

```
Finding ID
  → evidencia (comando + salida + E-ID)
  → control (nombre + fuente normativa)
  → componente (módulo/archivo)
  → tracking item (B-NN o NOT_TRACED)
  → ADR (N3 o NOT_TRACED)
  → governance (artefacto normativo: Plan §, GOVERNANCE, contrato, golden)
  → Plan Maestro (fase, DOR/DOD, Production Gate)
  → decisión humana (D# o NOT_TRACED)
```

Reglas:
- Si una relación no existe → **NOT_TRACED**. Nunca inventar IDs, ADRs, tickets ni referencias.
- La trazabilidad del finding se enlaza al registro `OCM_AUDIT_FINDINGS_*.md` y al informe
  canónico; el tracking.yaml es SSOT del backlog (no se modifica para reconciliar).
- Cada finding en informe debe aparecer en registro y viceversa (verificación final §6.2.12).

---

## 12. Read-only boundaries

Una auditoría es READ-ONLY sobre: código, tests, CI, ADRs, tracking.yaml, configuración
funcional, workflows, Docker, pyproject.toml, uv.lock.

Escritura permitida SOLO en `docs/audits/` (artefactos documentales autorizados):
- informe canónico (`AUDIT_*.md`);
- registro de findings (`OCM_AUDIT_FINDINGS_*.md`);
- protocolo y diseños de auditoría.

Prohibido durante auditoría: `git add/commit/push/reset/checkout/restore/clean`.
Nunca modificar tracking para reconciliar un finding. Nunca modificar una ADR para eliminar una
contradicción. Nunca modificar código para conseguir PASS. No "arreglar" findings: documentar
`Recommended remediation` y dejar la decisión humana.

Verificación de integridad al final: `git rev-parse HEAD` intacto, `git diff --stat HEAD`
vacío, `git status --short` solo con untracked documentales en docs/audits/.

---

## 13. Required AGENTS.md rules

Lo que DEBE vivir en AGENTS.md (reglas concisas, deterministas, verificables):

1. **Governance-first (orden de descubrimiento obligatorio):** AGENTS.md → protocolo de
   auditoría (si existe) → Plan Maestro → GOVERNANCE → tracking.yaml → ADRs → contratos →
   golden → CI → linters → auditorías previas. El auditor adapta su método al gobierno del
   proyecto, no al revés.
2. **Estado esperado primero:** empezar por "¿cuál es el estado esperado según governance?",
   luego "¿estado observado?", luego "¿diferencia?", luego "¿gobernada/tracked/ADR'd?",
   luego "¿finding nuevo?".
3. **Taxonomía canónica:** NUEVO / REVALIDADO / REGRESIÓN / CERRADO / CONTRADICCIÓN /
   RECOMENDACIÓN / NO_VERIFICADO (definiciones en AUDIT_PROTOCOL.md §7). Una sola
   clasificación por finding.
4. **Deduplicación obligatoria:** buscar tracking + ADR + auditorías previas + findings cerrados
   antes de declarar NUEVO. Regla de oro: la condición ya existe → no crear.
5. **CONTROL STATUS ≠ FINDING STATUS:** un FAIL de control no es un finding NUEVO hasta
   reconciliarlo. Estados de control: PASS/FAIL/PARTIAL/NO_VERIFICADO/INFRA_FAILURE.
6. **Golden = no-regresión:** GOLDEN PASS no significa "arquitectura correcta"; significa
   "estado observado = estado esperado documentado". Un cambio de golden sin remediación es
   regresión.
7. **Read-only de auditoría:** solo `docs/audits/` es escribible; sin git add/commit/push;
   nunca modificar tracking/ADRs/código para reconciliar.
8. **Comandos de evidencia canónicos (SSOT):** usar siempre los comandos exactos de los gates
   de CI (ej. pip-audit con `--ignore-vuln` de ocm-ci.yml) salvo que se declare otra cosa
   explícitamente como verificación adicional.
9. **Inventario por filesystem real:** listar directorios y leer archivos; nunca inferir conteos
   por numeración ni inventar estados.
10. **Verificación remota de CI cuando exista credencial (gh):** CI se evalúa por ejecución
    real, no por declaración del workflow.
11. **Matriz de reconciliación obligatoria** cuando existan informes concurrentes o previos;
    las discrepancias se explican, no se ignoran.
12. **Verificación final antes de declarar terminada:** reconciliar findings, controles,
    severidades, contadores; IDs únicos; trazabilidad; decisiones humanas; working tree;
    read-only respetado; informe = registro. Si hay discrepancia, no está terminada.
13. **Veredicto canónico:** `AUDIT_READY_WITH_FINDINGS` u otro definido por el proyecto; nunca
    declarar COMPLIANT/SECURE/PRODUCTION READY salvo criterio explícito del governance.
14. **Anti-alucinación:** no afirmar existencia sin comprobarla; no afirmar PASS sin ejecutar;
    no afirmar CI PASS sin evidencia; no inventar IDs/ADR/controles/estados/decisiones.

Estas 14 reglas son el mínimo de AGENTS.md; el detalle operativo vive en AUDIT_PROTOCOL.md.

---

## 14. Proposed AUDIT_PROTOCOL.md structure

Ubicación propuesta: `docs/audits/AUDIT_PROTOCOL.md` (serie N7 complementaria; puede
referenciarse desde AGENTS.md). Estructura derivada de los artefactos y de este diseño:

```
docs/audits/AUDIT_PROTOCOL.md
├── 1. Propósito y ámbito (agentes aplicables; no sustituye a governance)
├── 2. Fuentes de autoridad y jerarquía (Plan §12 + KB §2; tabla de precedencia)
├── 3. Flujo obligatorio (estado esperado → … → finding) — §6
├── 4. Orden de ejecución del encargo (18 pasos)
├── 5. Baseline (campos mínimos + ejemplo)
├── 6. Clasificaciones canónicas (7 categorías + reglas de decisión) — §7
├── 7. Severidades ancladas a governance
├── 8. Estados de control (5) + regla CONTROL vs FINDING + INFRA_FAILURE — §8
├── 9. Deduplicación (7 pasos obligatorios) — §9
├── 10. Reconciliación (matriz entre informes; contra tracking/ADR/golden) — §10
├── 11. Trazabilidad (cadena completa + NOT_TRACED) — §11
├── 12. Read-only boundaries + verificación de integridad — §12
├── 13. Golden semantics (no-regresión)
├── 14. Comandos de evidencia canónicos (SSOT actualizable, vinculados a ocm-ci.yml)
├── 15. Formato del informe canónico (23 secciones) y del registro de findings
├── 16. Verificación final (checklist de consistencia)
├── 17. Veredictos permitidos (AUDIT_READY_WITH_FINDINGS, etc.)
└── 18. Registro de cambios (versionado; la sesión que modifica el protocolo lo declara)
```

Relación con AGENTS.md: AGENTS.md cita las 14 reglas mínimas y enlaza a AUDIT_PROTOCOL.md
como norma operativa. El protocolo NO duplica Plan/GOVERNANCE/tracking/ADR: los referencia.

Nota de concurrencia: el informe `AUDIT_AGENTS_MD_PROTOCOL_2026-08-18.md` (otra sesión)
propone `docs/architecture/AUDIT_PROTOCOL.md`. Este diseño propone `docs/audits/AUDIT_PROTOCOL.md`
para mantener la serie de auditoría en `docs/audits/` (N7). **Decisión humana requerida**
(D-AUDIT-1): ubicación final del protocolo.

---

## 15. Human decisions required

| ID | Decisión | Responsable |
|---|---|---|
| D-AUDIT-1 | Ubicación del protocolo: `docs/audits/AUDIT_PROTOCOL.md` (recomendado, serie N7) vs `docs/architecture/AUDIT_PROTOCOL.md` (propuesta concurrente) | Owner |
| D-AUDIT-2 | Aprobar el conjunto de 14 reglas mínimas para AGENTS.md (§13) | Owner |
| D-AUDIT-3 | Aprobar AUDIT_PROTOCOL.md como norma operativa vinculante para agentes | Owner |
| D-AUDIT-4 | Definir registro formal de sesiones auditoras (evita colisiones de escritura concurrente a origin/main) | Owner |
| D-AUDIT-5 | Decidir si el `architecture_linter` standalone (o golden) debe ser gate CI — bloqueante o informativo (pendiente desde 2026-08-17, F-CI-03) | Owner |
| D-AUDIT-6 | Adoptar oficialmente el veredicto `AUDIT_READY_WITH_FINDINGS` y prohibir COMPLIANT/SECURE/CERTIFIED sin criterio de governance | Owner |
| D-AUDIT-7 | Decidir la política de severidad anclada a governance (§7.3) como estándar del proyecto | Owner |

Las decisiones D1–D7 de la auditoría de compliance de esta fecha (deps, licencia, ADRs,
supply chain, linter) se mantienen en `OCM_AUDIT_FINDINGS_2026-08-18.md` y son independientes.

---

## 16. Evidence

Toda la evidencia de esta sesión se ejecutó/leyó en vivo (no se cita sin verificar):

| Evidencia | Tipo | Resultado |
|---|---|---|
| `docs/PLAN-Maestro-Ingenieria.md` | [ARCHIVO] leído completo | SSOT documental; §2 cadena, §4 fases, §6 Production Gate, §7 tracking, §12 jerarquía, §13 ingeniería continua |
| `docs/architecture/GOVERNANCE.md` | [ARCHIVO] leído completo | §2 cuándo ADR, §8 gobernanza automatizada, §9 series ADR, addendum campos tracking |
| `docs/plans/tracking.yaml` | [EVIDENCIA] parseado | 48 hallazgos (35/12/1; 47/1), 16 reglas (13 CI), enums, cadena B-01/B-15/B-MD-008/009 |
| `scripts/engineering_health_check.py` | [ARCHIVO] leído | Enums SSOT (líneas 36-45), validaciones de coherencia |
| `docs/architecture/decisions/ADR-*.md` | [ARCHIVO] 27 archivos | Estados reales; ADR-0021 PROPUESTA, 0029/0030 ACEPTADA sin implementar; no existe "Superado" |
| `ADR-template.md` | [ARCHIVO] | Estados canónicos: Propuesto/Aceptado/Reemplazado/Obsoleto |
| `architecture_linter/importlinter.toml` | [EVIDENCIA] | 50 contratos `[[tool.importlinter.contracts]]` |
| `lint-imports` ejecutado | [EVIDENCIA] | 50 KEPT / 0 BROKEN |
| `tests/architecture_linter/test_golden.py` | [ARCHIVO] leído | GOLDEN_EXPECTED: 7 FAIL/1 PARTIAL/2 PASS |
| `tests/architecture_linter/test_adversarial.py` | [ARCHIVO] leído | Matriz de estados cubierta |
| `.github/workflows/ocm-ci.yml` | [ARCHIVO] leído | 10 jobs; gates; pip-audit con ignore-list; golden NO es job propio |
| `docs/audits/2026-08-17-architecture-linter-golden-vs-standalone-revalidation.md` | [ARCHIVO] | Semántica golden; conflicto #1; riesgos de concurrencia |
| 5 informes concurrentes de docs/audits/ | [ARCHIVO] comparados | Matriz de reconciliación (§4, §10.3) |
| `AUDIT_AGENTS_MD_PROTOCOL_2026-08-18.md` (concurrente) | [ARCHIVO] | Conclusiones compatibles; propone AUDIT_PROTOCOL.md |
| `git rev-parse HEAD` / `git status` / `git diff` | [EVIDENCIA] | HEAD `bee9fb5a`; solo untracked documentales en docs/audits/ |
| Informe técnico de compliance 2026-08-18 (canónico) | [ARCHIVO] | 27 secciones; 16 findings; controles; reconciliación; evidence index E-001..E-117 |

No se modificó ningún archivo fuera de `docs/audits/`. No se ejecutó git add/commit/push.

---

## 17. Final verdict

**AUDIT_PROTOCOL_DESIGN_READY_WITH_DECISIONS**

- El proyecto YA posee el 80 % del modelo operativo: Plan Maestro (N1), tracking (N2),
  ADRs (N3), contratos (N4/N5), CI (N6), golden, health check y jerarquía de autoridad.
  El diseño derivado NO es una metodología genérica: se construyó sobre esos artefactos.
- **Qué reglas ya existen:** gobierno del cambio, trazabilidad, enums de tracking, estados
  de ADR, gates CI, golden de no-regresión, health check, jerarquía de autoridad (Plan §12).
- **Qué reglas faltan:** protocolo de auditoría (clasificación, dedup, reconciliación,
  read-only, control vs finding, verificación final), golden semantics explícita para el
  auditor, comandos de evidencia canónicos, formato de informe/registro.
- **Qué está ambiguo:** relación linter standalone vs golden vs gate; severidad sin ancla;
  granularidad de findings; veredicto permitido.
- **Qué está contradicho por otros documentos:** el informe concurrente propone
  `docs/architecture/AUDIT_PROTOCOL.md` (este diseño: `docs/audits/AUDIT_PROTOCOL.md`);
  "Superado" inventado por 2 informes; pip-audit 6 vs 4 por comando distinto.
- **Qué comportamiento actual de Gemini/DeepSeek produce inconsistencia:** 15 riesgos (§5),
  todos mitigables por el protocolo.
- **Qué debe vivir en AGENTS.md:** las 14 reglas mínimas (§13).
- **Qué debe vivir en AUDIT_PROTOCOL.md:** el detalle operativo (§14).
- **Qué sigue en Plan/GOVERNANCE/tracking/ADR:** la SSOT del estado (no se duplica).

La variabilidad entre agentes NO es un defecto de los agentes: es ausencia de protocolo
canónico. La adopción de AGENTS.md §13 + AUDIT_PROTOCOL.md es la decisión humana que elimina
esa variabilidad sin tocar el modelo de gobierno del proyecto.
