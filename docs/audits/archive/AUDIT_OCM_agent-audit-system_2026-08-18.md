# OCM — Audit System & Agent Governance Audit (v2)

**Fecha de auditoría:** 2026-08-18
**Commit auditado:** `bee9fb5a3917c32fcc81fcc81fa5177ce0e57283`
**Branch:** `main`
**Alcance:** SISTEMA DE AUDITORÍA de OCM (protocolo, AGENTS.md, jerarquía normativa, comandos, taxonomía, golden, severidad, reconciliación, tooling). NO audita el producto.
**Metodología:** Read-only estricto (escritura solo en `docs/audits/`). Discovery normativo obligatorio → baseline → verificación selectiva de controles → clasificación → veredicto. Evidencia reproducida en vivo (comandos ejecutados, CI remoto vía `gh`).
**Vigencia del sistema auditado:** AUDIT_PROTOCOL.md v2.0 (modificado concurrentemente el 2026-08-17 22:06 durante esta auditoría) + AGENTS.md (modificado 22:09). Se documenta esta evolución como riesgo de reproducibilidad (§9).

---

## 1. Executive Summary

El sistema de auditoría de OCM está **bien fundamentado en su capa normativa** (jerarquía de autoridad, cadena de adopción Conocimiento→Decisión→ADR→Control, golden semantics, read-only, CONTROL FAIL≠FINDING NUEVO), **pero no es determinista en su capa operativa**. La evidencia es directa: sobre el mismo commit, informes concurrentes producen 2/3/6/16 findings, 4 vs 6 CVEs en pip-audit, severidad HIGH vs CRITICAL para la misma CVE, y estados de ADR inventados ("Superado"). Cada divergencia se traza a una regla que depende del criterio del LLM donde debería existir una regla mecánica o un comando canónico.

**Veredicto: `AUDIT_READY_WITH_FINDINGS` — `AUDIT_SYSTEM_STATE = PARTIALLY_DETERMINISTIC`.**

---

## 2. Scope

| Incluye | Excluye |
|---|---|
| AGENTS.md, docs/governance/AUDIT_PROTOCOL.md, Plan Maestro, GOVERNANCE, tracking.yaml, ADRs | Código de producción (no se audita) |
| architecture_linter, golden tests, engineering_health_check | Tests del producto (no se auditan) |
| CI/CD (declaración + ejecución remota) | CI como objetivo de corrección |
| Auditorías anteriores (informes concurrentes sobre `bee9fb5a`) | — |
| Comandos canónicos de cada control | — |
| Determinismo del proceso de auditoría | — |

**No se modificó:** código, tests, CI, ADRs, tracking.yaml, AGENTS.md, AUDIT_PROTOCOL.md. Escritura: solo `docs/audits/`.

---

## 3. Sources Inspected

| # | Fuente | Encontrada | Rol |
|---|---|---|---|
| 1 | `docs/PLAN-Maestro-Ingenieria.md` | ✅ | Normativa N1 (SSOT documental) |
| 2 | `docs/architecture/GOVERNANCE.md` | ✅ | Normativa (SSOT de reglas) |
| 3 | `AGENTS.md` | ✅ | Normativa de agentes (modificada concurrentemente) |
| 4 | `docs/governance/AUDIT_PROTOCOL.md` v2.0 | ✅ | Protocolo de auditoría (modificado concurrentemente) |
| 5 | `docs/plans/tracking.yaml` | ✅ | Estado (48 hallazgos, 16 reglas) |
| 6 | `docs/architecture/decisions/` | ✅ | Decisiones (26 ADR + template; 2 formatos de header) |
| 7 | `tests/architecture_linter/` | ✅ | Golden + adversarial |
| 8 | `architecture_linter/` | ✅ | Linter semántico (ARCH-001..010) |
| 9 | `architecture_linter/importlinter.toml` | ✅ | 50 contratos |
| 10 | `scripts/engineering_health_check.py` | ✅ | Enums SSOT + coherencia |
| 11 | `scripts/check_ssot_enums.py` | ✅ | Validación de enums |
| 12 | `.github/workflows/ocm-ci.yml` + workflows | ✅ | CI (10 jobs + gitleaks/yamllint/trivy/codeql) |
| 13 | `docs/architecture/logs/`, `recovered/` | ✅ | Histórico |
| 14 | `docs/audits/` (informes previos + 8 concurrentes) | ✅ | Histórico |
| 15 | `docs/knowledge/manifest.yaml` | ✅ | KB (30 entradas, TIER_0-4) |
| 16 | `docs/` PDFs/zips (Clean Architecture, freqtrade, hummingbot, nautilus) | ✅ | Conocimiento externo |
| 17 | Decisiones humanas D-XX previas | ✅ | En informes previos |
| 18 | `docs/governance/AUDIT_PROTOCOL.md` v1.0→v2.0 evolución | ✅ | Riesgo de reproducibilidad (§9) |

**No encontrado:** ningún `AUDIT_PROTOCOL.md` antes de las 21:53 del día; ningún tooling de validación de auditorías (solo guardrails de producto); ningún bloque obligatorio de reproducibilidad con agente/modelo/versión en el protocolo.

---

## 4. Current Normative Hierarchy

El proyecto define explícitamente la jerarquía en dos lugares:

**Plan Maestro §12 (N1–N7):**
```
Plan (N1) → Tracking (N2) → ADR (N3) → Contratos (N4/N5) → Código → Tests → CI → Release
```
- Cuando N1 y N2 divergen en estado → gana N2 (tracking) para el backlog.
- Cuando N3 (ADR) y el código divergen → gana N3 (se abre hallazgo).

**AUDIT_PROTOCOL.md §B v2.0 (5 niveles):**
```
LEVEL 4 Normative Governance: Plan, GOVERNANCE, AGENTS.md, AUDIT_PROTOCOL.md
LEVEL 3 Decisiones Humanas: ADRs aprobados
LEVEL 2 Estado e Implementación: tracking, código, tests, linters, CI
LEVEL 1 Conocimiento del proyecto: notas, investigaciones, históricos
LEVEL 0 Conocimiento externo: libros, papers, repositorios de referencia
```

**Evaluación:** jerarquía **inequívoca en las dos fuentes**, pero con una discrepancia de *composición*:
- El Plan (§12) ordena `N1 > N2 > N3 > N4/N5`, mientras AUDIT_PROTOCOL §B ordena `LEVEL4 (Plan+GOVERNANCE+AGENTS+Protocolo) > LEVEL3 (ADR) > LEVEL2 (tracking)`. La posición relativa de **tracking vs ADR** invierte entre ambas: en el Plan, N2 (tracking) > N3 (ADR); en el protocolo, ADR (LEVEL3) > tracking (LEVEL2). Esto es una **inconsistencia normativa cruzada** (GAP-06): un agente puede razonablemente inferir dos precedencias distintas para tracking↔ADR según la fuente que consulte.

**Regla de resolución en conflicto Plan/protocolo:** el Plan §12 dice "N3 gana al código" y el protocolo §M lo replica (código contradice ADR → CONTRADICCIÓN), así que en la práctica ADR > código en ambas. El gap es solo la posición tracking↔ADR.

---

## 5. Knowledge Hierarchy

Sistema de autoridad explícito (derivado y reconciliado con las fuentes reales):

| Categoría | Rol | ¿Crea obligación? | ¿Cierra obligación? | ¿Genera finding? | ¿Solo evidencia? |
|---|---|---|---|---|---|
| **A. NORMATIVA** | Plan, GOVERNANCE, AGENTS, Protocolo | ✅ | ✅ | ✅ | No |
| **B. ESTADO** | tracking.yaml, golden, CI | ⚠️ define expectativa | ✅ | ✅ (regresión) | Evidencia |
| **C. DECISIONES** | ADRs aprobadas | ✅ (tras adopción) | ✅ (deuda = GOVERNED) | ⚠️ (contradicción) | Contexto |
| **D. IMPLEMENTACIÓN** | Código, tests, contratos | ❌ (es efecto) | ❌ | ✅ (estado real) | Evidencia primaria |
| **E. CONOCIMIENTO** | KB, notas, investigaciones | ❌ | ❌ | ❌ (máx. RECOMENDACIÓN) | Contexto |
| **F. HISTÓRICO** | docs/audits/, logs | ❌ | ❌ | ❌ (baseline) | Baseline |
| **G. EVIDENCIA** | salidas de comandos/CI | ❌ | ❌ | ❌ | ✅ |

**Precedencia de resolución:** `A > C > B > D > F > G > E`. Nunca F>A (histórico no invalida norma vigente). E nunca crea obligación.

**GAP-05:** la KB (`docs/knowledge/manifest.yaml`) tiene su propia jerarquía interna `TIER_0..TIER_4` que NO coincide 1:1 con las categorías del modelo — un agente podría confundir "TIER_1 (referencia primaria)" con autoridad normativa. AGENTS.md KB ya aclara que TIER_1-4 no son norma técnica, pero no define el mapeo exacto al modelo de autoridad.

---

## 6. Adoption Chain

La cadena de adopción está formalizada en AUDIT_PROTOCOL §B v2.0:

```
KNOWLEDGE (L0/L1) → PROPOSAL → HUMAN DECISION → ADR/GOVERNANCE/PLAN (L3/L4) → TRACKED STATE (L2) → ENFORCEABLE CONTROL (L2) → AUDIT
```

Regla: si una recomendación de conocimiento externo no completa esta cadena, no puede convertirse en FAIL/finding de incumplimiento → `RECOMENDACIÓN` o `CONOCIMIENTO_EXTERNO`.

**Evaluación:** ✅ correcta. **GAP-04:** la taxonomía de findings NO incluye `CONOCIMIENTO_EXTERNO` como clase canónica (solo RECOMENDACIÓN/NO_VERIFICADO), obligando al agente a encajar conocimiento sin cadena en categorías ambiguas (A4 previo). Además no existe una plantilla de "propuesta de adopción" para ascender conocimiento → ADR.

---

## 7. Audit Protocol Assessment

**AUDIT_PROTOCOL.md v2.0** (124 líneas, 16 secciones A–P). Evaluación independiente:

| Sección | Evaluación | Problema |
|---|---|---|
| A. Scope | ✅ OK | — |
| B. Normative Hierarchy & Knowledge Governance | ✅ Fuerte (añadida concurrentemente) | Posición tracking↔ADR inversa al Plan §12 (GAP-06) |
| C. Discovery Order | ✅ OK | — |
| D. Baseline Construction | ⚠️ Insuficiente | Solo commit/branch/working tree. NO exige agente/modelo/versión de herramientas/fecha de protocolo (GAP-02) |
| E. Golden State | ✅ OK | GOLDEN PASS ≠ arquitectura conforme (explícito) |
| F. Control States | ✅ OK | INFRA_FAILURE ≠ FAIL (explícito) |
| G. Finding Taxonomy | ⚠️ Prosa | Definiciones descriptivas, sin condiciones suficientes formales ni árbol de decisión (GAP-03) |
| H. Reconciliation & Dedup | ⚠️ Parcial | `CONTROL FAIL ≠ FINDING NUEVO` ✅, pero sin procedimiento mecánico de búsqueda por causa raíz ni cardinalidad (GAP-07) |
| I. Traceability | ✅ OK | Cadena completa + NOT_TRACED; prohíbe inferencias |
| J. Severity Model | ⚠️ Incompleto | "Impacto real de negocio" sin tabla ni reglas disparadoras (GAP-01) |
| K. Read-Only | ✅ OK | Prohibición absoluta + `docs/audits/` |
| L. Architecture-Linter & Golden | ✅ OK | FAIL con ADR/tracking = REVALIDADO |
| M. Contradiction | ✅ OK | Mayor autoridad gana; no forzar paridad |
| N. Human Decisions | ✅ OK | D-XXX |
| O. Audit Report Requirements | ⚠️ Nombre único | Exige `AUDIT_OCM_FORENSIC_COMPLIANCE_YYYY-MM-DD.md`; en la práctica coexisten 5+ nombres (GAP-08) |
| P. Final Verification Checklist | ✅ OK | — |

**GAP-09:** el protocolo NO fija comandos canónicos por control (pip-audit, mypy, ruff, bandit, yamllint...). Ver §13 (evidencia de la divergencia 4 vs 6).

**GAP-10:** el protocolo NO se versiona internamente (no hay bloque de versión/fecha/commit). Durante esta auditoría pasó de v1.0 (21:53) a v2.0 (22:06) — sin versionado, dos agentes pueden estar ejecutando protocolos distintos sobre el mismo commit.

---

## 8. AGENTS.md Assessment

Estado vigente (modificado concurrentemente 22:09). Contiene:

| Regla | Presente | Ubicación adecuada |
|---|---|---|
| Referencia obligatoria a AUDIT_PROTOCOL.md | ✅ | AGENTS ✅ |
| Read-only boundary | ✅ | AGENTS ✅ |
| CONTROL FAIL ≠ FINDING NUEVO | ✅ | AGENTS ✅ |
| Cadena de adopción Conocimiento→Decisión→ADR→Control | ✅ | AGENTS ✅ (resumen) |
| Orden de descubrimiento | ✅ | AGENTS ✅ |
| Jerarquía KB (manifest, TIER, status) | ✅ | AGENTS ✅ |
| Taxonomía de 7 clases | ❌ (solo en protocolo) | Solo protocolo ✅ |
| Tabla de severidad | ❌ | Solo protocolo ✅ |
| Comandos canónicos | ❌ | Solo protocolo/tooling ✅ |
| Bloque de reproducibilidad | ❌ | Solo protocolo ✅ |
| Matriz de reconciliación | ❌ | Solo protocolo ✅ |

**Evaluación:** AGENTS.md está **correctamente dimensionado** — contiene lo que debe activarse en todo encargo y delega lo operativo al protocolo. No requiere duplicar la taxonomía. Recomendación: mantener la separación actual (AGENTS = activadores; protocolo = reglas operativas; tooling = mecánica).

---

## 9. Determinism Matrix

Clasificación de cada operación del proceso de auditoría:

| Operación | MECÁNICO | LLM JUDGMENT | HUMANO | AMBIGUO | Comentario |
|---|---|---|---|---|---|
| Commit/branch/working tree | ✅ M | | | | `git rev-parse`, `git status` |
| Golden State (estado actual) | ✅ M | | | | pytest golden (4 passed) |
| Estados de ADR vs enum | | | | ⚠️ | 2 formatos de header; "Superado" inventado (no gate) |
| Conteos / sumatorias | ✅ M | | | | pero hoy se hacen manualmente |
| IDs únicos | ✅ M | | | | sin tooling |
| Severidad | | | | ⚠️ | sin tabla → LLM subjetivo |
| Clasificación NUEVO/REVALIDADO/... | | | | ⚠️ | prosa, no árbol |
| Dedup por causa raíz | | ✅ | | | decide el LLM (justificado parcialmente) |
| Reconciliación entre agentes | | | | ⚠️ | matriz no obligatoria |
| Comandos canónicos | ✅ M | | | | pero no están fijados → ambigüedad real |
| Versión de herramientas | ✅ M | | | | no exigida |
| Trazabilidad estructural | ✅ M | | | | sin validador |
| Causa raíz / impacto | | ✅ | | | legítimo LLM |
| Relación semántica entre findings | | ✅ | | | legítimo LLM |
| Aceptar riesgo / ADR / excepción | | | ✅ | | D-XX |

**GAP-11 (crítico):** operaciones que hoy dependen del LLM y son **mecanizables**: severidad (tabla), clasificación (árbol), conteos (script), dedup parcial (índice tracking+auditorías), reconciliación (matriz/JSON), trazabilidad (validador), estados ADR (gate enum). Estas son la fuente de las divergencias observadas.

---

## 10. Mechanical Checks (deben ser tooling)

| # | Regla | Input | Output | Estado hoy |
|---|---|---|---|---|
| M1 | IDs únicos | findings + tracking + ADR | duplicados/rotos | ❌ no existe |
| M2 | Conteos (∑ clasificación = ∑ severidad = total) | registro + matrix | verificación | ❌ manual |
| M3 | Severidades válidas + regla citada | ficha finding | válido/regla | ❌ manual |
| M4 | Estados válidos (enums) | ADRs, findings, controls | válido/inventado | ⚠️ parcial (`check_ssot_enums`) |
| M5 | Tracking references | finding → tracking ID | existe/no | ❌ manual |
| M6 | ADR references | finding → ADR | existe/estado | ❌ manual |
| M7 | Reconciliación (matriz) | 2+ informes | estado canónico | ❌ manual |
| M8 | Golden State | GOLDEN_EXPECTED + resultados | PASS/REGRESIÓN | ✅ pytest |
| M9 | Reproducibilidad (bloque) | metadata informe | completo/faltante | ❌ manual |
| M10 | Comandos canónicos | comando ejecutado | = canónico/desvío | ❌ no fijado |
| M11 | Trazabilidad (cadena completa) | ficha | completa/NOT_TRACED | ❌ manual |
| M12 | Integridad documental | working tree, HEAD | OK/delta | ✅ manual |

**Solo M8 existe como gate.** El resto se decide por el LLM en cada informe → divergencia.

---

## 11. LLM Judgment Checks (correctamente delegados)

| # | Decisión | Justificación de no mecanización |
|---|---|---|
| L1 | ¿Dos síntomas comparten causa raíz? | Requiere dominio |
| L2 | Mapeo finding → componente/BC/ADR | Requiere comprensión semántica |
| L3 | Impacto de negocio/trading | Contextual |
| L4 | ¿"mejora deseable" vs "obligación incumplida" ante norma ambigua? | Semántico |
| L5 | Contradicciones sutiles entre fuentes | Semántico |
| L6 | Prioridad/remediación contextual | Juicio |

**Correcto delegar al LLM: L1–L6.** El objetivo no es eliminar el juicio del modelo sino confinarlo a esta capa, y que todo lo mecánico (M1–M12) sea validado por tooling.

---

## 12. Human Decisions (separadas de recomendaciones)

| ID | Tema | Responsable |
|---|---|---|
| D-1…D-7 | Producto (deps, licencia, ADR-0021/0029/0030, supply chain, linter gate) | Owner (sesiones previas) |
| D-AS-1…D-AS-10 | Sistema de auditoría (ver §24) | Owner |

Regla verificada: ninguna decisión humana fue ejecutada por los agentes; se registran como D-XX con pregunta/opciones/impacto/recomendación.

---

## 13. Canonical Commands

### 13.1 Discrepancia histórica verificada: 4 vs 6 CVEs

Ejecución real en esta sesión (pip-audit 2.10.1):

| Comando | Resultado |
|---|---|
| `uv run pip-audit .` | **6** known vulnerabilities in **4** packages (aiohttp x3, cryptography, pyarrow, ecdsa) |
| `uv run pip-audit . --ignore-vuln PYSEC-2026-113 --ignore-vuln PYSEC-2026-1325` | **4** known vulnerabilities, ignored 2, in **2** packages (aiohttp x3, cryptography) |

**Causa raíz de la divergencia:** exclusivamente la **ignore-list del CI** (`ocm-ci.yml:308`). No es un error de ningún informe: uno usó el comando canónico de CI (4), los otros usaron el comando desnudo (6). Es un **fallo del sistema**: no existe un comando canónico fijado por control.

### 13.2 Estado de comandos canónicos por control

| Control | Comando canónico (CI) | Fijado en protocolo | Excepciones documentadas |
|---|---|---|---|
| import-linter | `uv run lint-imports --config architecture_linter/importlinter.toml` | ❌ | CI lo usa; 50/50 |
| architecture_linter | `uv run python -m architecture_linter --root . --json` | ❌ | golden 7 FAIL/1 PARTIAL/2 PASS |
| mypy | `uv run mypy .` | ❌ | — |
| ruff | `uv run ruff check .` | ❌ | — |
| ruff format | `uv run ruff format . --check` | ❌ | — |
| bandit | `uv run bandit -r apps ocm packages shared infrastructure -ll` | ❌ | 51 Low / 0 Med-High |
| pip-audit | `uv run pip-audit . --ignore-vuln PYSEC-2026-113 --ignore-vuln PYSEC-2026-1325` | ❌ | **2 ignores NO documentados en protocolo** (solo en CI) → GAP-09 |
| pytest (golden) | `uv run pytest tests/architecture_linter/test_golden.py --no-cov` | ❌ | — |
| engineering_health | `uv run python scripts/engineering_health_check.py` | ❌ | — |
| yamllint | `uvx yamllint -c .yamllint .` | ❌ | alertas |
| gitleaks / trivy / codeql / actionlint | workflows | ❌ | — |

**Verificaciones en vivo de esta sesión:**
- yamllint local: `deploy/monitoring/alerts.yml:66:162 error (no new line at end of file)` — **coincide exactamente** con el FAIL de CI del run `32069832475` (job yamllint, 12s, failure).
- CI remoto @HEAD (push 21:11, run `32069832325`): `OrangeCashMachine CI` = **failure**; 8/9 jobs success; **Quality gates (ruff/mypy/SSOT/audit) = failure**. yamllint = failure por separado.
- Golden test: **4 passed** (`test_golden_statuses_repo_actual` incluido).

**GAP-09 confirmado con evidencia reproducible:** sin comando canónico fijado, la misma auditoría produce 4 o 6 CVEs según el agente.

---

## 14. Reproducibility

**Lo que AUDIT_PROTOCOL §D exige hoy:** commit SHA, branch, working tree, baseline documental.

**Lo que falta (GAP-02):** fecha, agente/modelo, versión del modelo, versión del protocolo, versión de herramientas, comandos exactos + flags, configuración/ambiente, resultados brutos, evidencia.

El bloque mínimo obligatorio propuesto:

```
Commit: <sha>            Branch: <main>
Fecha: <YYYY-MM-DD>
Agente/Modelo: <modelo + versión>
Protocolo: <versión + commit del protocolo>
Herramientas: <nombre + versión de cada una>
Comandos: <lista exacta por control>
Ambiente: <SO/python/uv/deps relevantes>
Resultados brutos: <salidas/hashes>
```

**Sin esto, dos agentes que auditan el mismo commit pueden estar en protocolos/entornos distintos y sus resultados no son comparables.** Hoy esto ocurrió de facto (v1.0 vs v2.0, pip-audit con/sin ignores).

---

## 15. Severity Model

**GAP-01 (crítico): no existe tabla normativa de severidad.** AUDIT_PROTOCOL §J solo declara los 5 niveles y "impacto real de negocio". Evidencia: la misma CVE (aiohttp) recibe **HIGH** en un informe y **CRITICAL** en otro sobre el mismo commit.

**Modelo propuesto (reglas disparadoras evaluadas en orden CRITICAL→INFO):**

| Severidad | Disparadores |
|---|---|
| CRITICAL | Pérdida de capital real, corrupción de posición/cash, CVE explotable RCE en ruta live, gate CI de seguridad roto |
| HIGH | CVE explotable en runtime, contradicción ADR↔código en core trading, regla auto-defendible activa rota |
| MEDIUM | CVE dev/tooling, deuda gobernada (REVALIDADO), PARTIAL invariante con tracking |
| LOW | Práctica documentada sin impacto, inconsistencia menor, RECOMENDACIÓN |
| INFO | Observación informativa / conocimiento externo sin cadena |

Regla: severidad técnica ≠ aceptación de riesgo; un riesgo documentado en ADR mantiene severidad técnica pero estado normativo `GOVERNED` (§J ya lo dice).

---

## 16. Reconciliation Model

**GAP-07: no existe matriz de reconciliación obligatoria entre auditorías.** Hoy se reconcilió manualmente (este informe y el canónico). La divergencia de granularidad (2/3/6/16 findings sobre el mismo commit) persiste porque no hay procedimiento formal.

Matriz propuesta (por dimensión):

| Dimensión | Reporte A | Reporte B | Tracking | ADR | Evidencia actual | Estado final | Explicación |
|---|---|---|---|---|---|---|---|
| pip-audit | 6 CVEs | 4 CVEs | — | — | comando CI = 4 | **4** | ignores no fijados (GAP-09) |

Reglas:
1. Nunca declarar a un informe "equivocado" sin determinar comando/ignores/versiones/fecha/commit/contexto.
2. El estado canónico lo fija la fuente de mayor autoridad + comando canónico.
3. Discrepancia irresoluble → `NO_VERIFICADO` la dimensión (no elegir arbitrariamente).
4. Dos agentes son "compatibles" si sus informes, alimentados a la matriz, producen el mismo **estado final reconciliado** — no requieren IDs idénticos.

---

## 17. Deduplication

**GAP-07 parcial:** la dedup por causa raíz depende 100% del LLM hoy. Regla formal necesaria:

- Cardinalidad del registro = **número de causas raíz gobernables**, no número de mensajes de herramientas.
- Ejemplo: aiohttp PYSEC-2026-3545/3546/3547 (x3) + cryptography PYSEC-3552 (x1) = **1 causa raíz** ("deps vulnerables sin mitigar") = **1 finding** con 4 evidencias.
- Cruce entre agentes: `F-CI-01` (Gemini) + `F-SEC-04` (DeepSeek) con misma causa raíz → **1 finding canónico**, ambos IDs documentados en la matriz.

**Parte mecanizable:** búsqueda por ID exacto en tracking, por regex de síntoma, por paquete/regla ARCH/ADR. **Parte LLM (legítima):** decidir si dos síntomas comparten causa raíz (L1).

---

## 18. Traceability

Cadena obligatoria (protocolo §I): `Hallazgo → Evidencia → Control → Requisito/Obligación → Fuente Normativa → Tracking → ADR → Implementación`. Eslabón faltante → `NOT_TRACED`; prohibido inventar.

Estados de trazabilidad útiles (faltan en el protocolo — GAP-12):

| Estado | Significado |
|---|---|
| NOT_TRACED | No hay relación (debe declararse, no omitirse) |
| REFERENCE_ONLY | Solo referenciado, no gobernado |
| GOVERNED | Deuda cubierta por ADR/tracking (severidad técnica se mantiene) |
| VERIFIED | Evidencia reproducible del estado |
| NOT_VERIFIED | Sin evidencia suficiente |

Hoy no hay **validador mecánico** de que cada ficha tenga la cadena completa → depende del LLM.

---

## 19. Golden State

**Semántica verificada como correcta en todas las fuentes:**

| Fuente | Semántica |
|---|---|
| `test_golden.py` | GOLDEN_EXPECTED = 7 FAIL / 1 PARTIAL / 2 PASS — documentado como estado esperado de deuda |
| AUDIT_PROTOCOL §E | "GOLDEN PASS ≠ Arquitectura Conforme" |
| AGENTS.md | "no-regresión" (no corrección) |
| CI | golden dentro de unit-tests; linter standalone NO es gate |

**Evaluación: ✅ sin ambigüedad normativa.** Golden = no-regresión de estado, no conformidad. Un FAIL esperado + registrado = REVALIDADO. Cambio respecto al golden = REGRESIÓN (peor) o mejora (requiere decisión humana para actualizar golden, nunca durante auditoría).

**Residual:** un informe concurrente interpretó el golden (7 FAIL) como "deuda documentada y reconocida" de forma correcta; otro como si fuera PASS arquitectónico. El texto normativo es claro; la **aplicación** quedó al LLM. Recomendación: gate mecánico que prohíba clasificar PASS arquitectónico una regla cuyo golden es FAIL/PARTIAL (M8 extendido).

---

## 20. INFRA_FAILURE Semantics

**Formalizada correctamente:** AUDIT_PROTOCOL §F define `INFRA_FAILURE` (Kafka, Redis, red, Docker) ≠ FAIL de producto. Regla del encargo: documentar comando, causa, evidencia, impacto sobre confianza.

**Evidencia aplicada (producto):** `tests/kafka/test_integration_kafka.py` local = INFRA_FAILURE (Kafka ausente), mientras el job `integration-tests` remoto = SUCCESS (service container). El estado canónico del control lo fija la evidencia con más autoridad (CI ejecutado).

**GAP-13:** NO_VERIFICADO no distingue "no ejecuté" (por elección/scope) de "no pude" (falta acceso/red). La regla §8 del protocolo de usuario pide esta distinción; AUDIT_PROTOCOL §F no la hace.

---

## 21. Findings

| F-ID | Severity | Classification | Descripción |
|---|---|---|---|
| F-SYS-01 | HIGH | NUEVO | **Sin comandos canónicos fijados.** Evidencia reproducible: `pip-audit .`=6 CVEs vs `pip-audit . --ignore-vuln ...`=4 CVEs; yamllint local coincide con CI. Control C1 FAIL. |
| F-SYS-02 | HIGH | NUEVO | **Severidad sin tabla normativa.** Misma CVE → HIGH y CRITICAL en informes distintos. Control C7 FAIL. |
| F-SYS-03 | HIGH | NUEVO | **Clasificación sin árbol mecánico.** 2/3/6/16 findings sobre el mismo commit por granularidad y criterio NUEVO/REVALIDADO no mecánico. Control C8 FAIL. |
| F-SYS-04 | MEDIUM | NUEVO | **Sin tooling M1–M12** (IDs, conteos, dedup, reconciliación, trazabilidad). Control C15 FAIL. |
| F-SYS-05 | MEDIUM | NUEVO | **Sin bloque de reproducibilidad** (agente/modelo/versión herramientas). Protocolo §D incompleto. Control C11 FAIL. |
| F-SYS-06 | MEDIUM | CONTRADICCIÓN | **Precedencia tracking↔ADR invertida** entre Plan §12 (N2>N3) y AUDIT_PROTOCOL §B (ADR>tracking). Control C16 FAIL. |
| F-SYS-07 | MEDIUM | NUEVO | **Protocolo sin versionado interno.** Evolucionó v1.0→v2.0 durante la auditoría (21:53→22:06) sin marca de versión. Control C12 FAIL. |
| F-SYS-08 | MEDIUM | CONTRADICCIÓN | **Nomenclatura de informes no unificada.** §O exige `AUDIT_OCM_FORENSIC_COMPLIANCE_*`; coexisten 8+ nombres sobre el mismo commit. Control C13 FAIL. |
| F-SYS-09 | LOW | NUEVO | **Estados de ADR sin gate.** "Superado" inventado por un LLM; 2 formatos de header (`**Estado:**` vs `## Estado`). Control C14 FAIL. |
| F-SYS-10 | LOW | RECOMENDACIÓN | **Mapeo KB TIER↔autoridad explícito** y categoría de clasificación `CONOCIMIENTO_EXTERNO` faltante. Control C10 PARTIAL. |
| F-SYS-11 | LOW | NUEVO | **NO_VERIFICADO sin distinción "no ejecuté" vs "no pude".** Control C17 FAIL. |

**Dedup aplicada:** consolidados por causa raíz (los 6 CVEs = 1 finding en F-SYS-01 evidencia, no 6 findings).

---

## 22. Gaps

| GAP | Descripción | Finding |
|---|---|---|
| GAP-01 | Sin tabla de severidad | F-SYS-02 |
| GAP-02 | Reproducibilidad incompleta (§D) | F-SYS-05 |
| GAP-03 | Taxonomía en prosa sin árbol de decisión | F-SYS-03 |
| GAP-04 | Falta categoría CONOCIMIENTO_EXTERNO | F-SYS-10 |
| GAP-05 | Mapeo KB TIER ↔ autoridad sin formalizar | F-SYS-10 |
| GAP-06 | Precedencia tracking↔ADR invertida Plan vs protocolo | F-SYS-06 |
| GAP-07 | Dedup/reconciliación sin procedimiento mecánico ni matriz obligatoria | F-SYS-04 |
| GAP-08 | Nomenclatura de informes no unificada | F-SYS-08 |
| GAP-09 | Comandos canónicos no fijados; ignores pip-audit no documentados | F-SYS-01 |
| GAP-10 | Protocolo sin versionado | F-SYS-07 |
| GAP-11 | Operaciones mecanizables delegadas al LLM | F-SYS-04 |
| GAP-12 | Trazabilidad sin estados ni validador mecánico | F-SYS-04 |
| GAP-13 | NO_VERIFICADO no distingue "no ejecuté" vs "no pude" | F-SYS-11 |

---

## 23. Recommendations

1. **Fijar comandos canónicos** por control en el protocolo (nueva sección), con flags y excepciones documentadas — comenzando por pip-audit con los 2 ignores de CI.
2. **Añadir tabla de severidad** con reglas disparadoras (orden CRITICAL→INFO) y obligación de citar la regla en cada ficha.
3. **Añadir árbol de decisión de clasificación** con condiciones suficientes formales para NUEVO/REVALIDADO/REGRESIÓN/CERRADO/CONTRADICCIÓN/RECOMENDACIÓN/NO_VERIFICADO.
4. **Añadir matriz de reconciliación obligatoria** entre informes concurrentes.
5. **Añadir bloque de reproducibilidad obligatorio** (§14).
6. **Versionar el protocolo** (bloque versión/fecha/commit) y fijarlo como baseline para el commit auditado.
7. **Estandarizar frontmatter ADR** (un solo formato `**Estado:**`) y gate de estados contra enum.
8. **Unificar nombre del informe canónico** a `AUDIT_OCM_FORENSIC_COMPLIANCE_YYYY-MM-DD.md`.
9. **Implementar tooling M1–M12** (fase 1: M1+M2+M3+M4+M8; fase 2: M5+M6+M7+M10+M11), reutilizando el patrón de `engineering_health_check.py`.
10. **Añadir categoría `CONOCIMIENTO_EXTERNO`** y mapeo KB TIER↔autoridad.
11. **Distinguir "no ejecuté" vs "no pude"** en NO_VERIFICADO.
12. **Resolver la precedencia tracking↔ADR** entre Plan §12 y protocolo §B (decisión humana, GAP-06).

---

## 24. Human Decisions Required

| ID | Pregunta | Opciones | Recomendación |
|---|---|---|---|
| **D-AS-1** | ¿Congelar/versionar AUDIT_PROTOCOL.md v2.0 como baseline? | A) Sí, versionar+commitear; B) seguir iterando | A — sin versión fija no hay reproducibilidad (bloqueante) |
| **D-AS-2** | ¿Comando canónico de pip-audit = el de CI (con 2 ignores)? | A) Sí; B) otro y documentar 6 | A — elimina divergencia 4 vs 6 (bloqueante) |
| **D-AS-3** | ¿Añadir tabla de severidad mecánica? | A) Sí; B) no | A |
| **D-AS-4** | ¿Añadir árbol de clasificación mecánico? | A) Sí; B) no | A |
| **D-AS-5** | ¿Añadir matriz de reconciliación obligatoria? | A) Sí; B) solo conflictos | A |
| **D-AS-6** | ¿Añadir categoría CONOCIMIENTO_EXTERNO? | A) Sí; B) no | A |
| **D-AS-7** | ¿Unificar frontmatter ADR + gate enum? | A) Sí; B) tolerar ambos | A |
| **D-AS-8** | ¿Implementar tooling M1–M12? | A) Fase 1 básica; B) fase 2 completa; C) todo | A→B secuencial |
| **D-AS-9** | ¿Bloque de reproducibilidad obligatorio? | A) Sí; B) solo commit | A |
| **D-AS-10** | ¿Unificar nombre informe canónico + consagrar único destino? | A) Sí; B) libre | A |
| **D-AS-11** | ¿Resolver precedencia tracking↔ADR (Plan vs protocolo)? | A) ADR>tracking; B) tracking>ADR; C) matizarlas | C — distinguir "estado" (tracking manda) vs "decisión" (ADR manda) |

**Bloqueantes:** D-AS-1, D-AS-2, D-AS-8.

---

## 25. Proposed Target Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  NORMATIVA (L4): Plan, GOVERNANCE, AGENTS.md, AUDIT_PROTOCOL │
│  └── versionado + bloque de reproducibilidad                 │
├──────────────────────────────────────────────────────────────┤
│  ESTADO (L2): tracking.yaml (enums SSOT)                     │
│  └── engineering_health_check.py (gate existente)            │
├──────────────────────────────────────────────────────────────┤
│  TOOLING DE AUDITORÍA (nuevo: audit_validator.py)            │
│  ├── M1 IDs únicos           ├── M7 reconciliación           │
│  ├── M2 conteos              ├── M8 golden gate              │
│  ├── M3 severidades          ├── M9 reproducibilidad         │
│  ├── M4 estados/enums        ├── M10 comandos canónicos      │
│  ├── M5 tracking refs        ├── M11 trazabilidad            │
│  └── M6 ADR refs             └── M12 integridad documental   │
│  (reusa patrón engineering_health_check.py, stdlib)          │
├──────────────────────────────────────────────────────────────┤
│  LLM JUDGMENT (confinado): causa raíz, impacto, equivalencia,│
│  contradicciones sutiles — validado a posteriori por tooling │
├──────────────────────────────────────────────────────────────┤
│  HUMANO: D-XX (riesgos, ADRs, excepciones, gates)            │
└──────────────────────────────────────────────────────────────┘
```

Principio: **el LLM propone, el tooling valida, el humano decide**. La divergencia final entre agentes se reduce a la capa L (juicio de dominio), nunca a la capa M.

---

## 26. Model-Independence Assessment

**Estado: PARCIAL.**

| Criterio del §19 | Estado |
|---|---|
| 1. Reglas definidas por el proyecto | ✅ (jerarquía, cadena, golden, read-only) |
| 2. Comandos fijados | ❌ (GAP-09) |
| 3. Evidencia reproducible | ⚠️ (posible pero no exigida ni canonizada) |
| 4. Estados con enums claros | ⚠️ (tracking sí; ADR no) |
| 5. Reconciliación definida | ❌ (GAP-07) |
| 6. Contadores verificables mecánicamente | ❌ (manual) |
| 7. Severidad con criterios explícitos | ❌ (GAP-01) |
| 8. Golden State semántica inequívoca | ✅ (texto) / ⚠️ (aplicación LLM) |
| 9. Conocimiento externo no se vuelve norma | ✅ (cadena de adopción) |
| 10. Decisiones humanas separadas | ✅ |
| 11. LLM solo donde hay juicio | ❌ (mecanizable delegado) |
| 12. Otro modelo ejecuta sin inventar reglas | ❌ (evidencia: estado "Superado" inventado; conteos divergentes) |

**Conclusión de independencia:** la normativa protege contra el peor comportamiento (conocimiento→norma, FAIL→NUEVO, golden→conforme), pero **no garantiza resultados idénticos** porque las reglas operativas (severidad, clasificación, dedup, comandos) quedan al criterio del modelo.

---

## 27. Final Verdict

```
Veredicto de auditoría:      AUDIT_READY_WITH_FINDINGS
Estado del sistema:          PARTIALLY_DETERMINISTIC
Independencia del modelo:    PARCIAL
Reproducibilidad:            PARCIAL
Determinismo:                PARCIAL
```

**Síntesis:** el sistema de auditoría de OCM tiene una capa normativa ejemplar (jerarquía, cadena de adopción, golden semantics, read-only, CONTROL FAIL≠NUEVO) que ya elimina las divergencias más graves entre agentes. La causa de los resultados divergentes observados (4 vs 6 CVEs, HIGH vs CRITICAL, 2 vs 16 findings, estado "Superado") es **operativa**: reglas que deberían ser mecánicas o tener comandos fijos quedan delegadas al LLM. El sistema alcanzará `DETERMINISTIC` cuando se implementen las decisiones humanas D-AS-1..11 (congelar y versionar el protocolo, fijar comandos canónicos, tabla de severidad, árbol de clasificación, matriz de reconciliación, y tooling M1–M12).

---

## Integridad

- Commit: `bee9fb5a3917c32fcc81fcc81fa5177ce0e57283` (sin cambios).
- Branch: `main`.
- Escrituras propias: **solo** `docs/audits/AUDIT_AGENT_AUDIT_SYSTEM_2026-08-18.md` (actualización del canónico previo al formato de 27 secciones).
- Sin `git add` / `git commit` / `git push`.
- **Cambios concurrentes de otra sesión (no míos):** `AGENTS.md` (22:09) y `docs/governance/AUDIT_PROTOCOL.md` (v1.0 21:53 → v2.0 22:06). Documentados como riesgo de reproducibilidad (F-SYS-07), no modificados.
- GOLDEN PASS no fue interpretado como arquitectura conforme (§19).
- Evidencia ejecutada en vivo: pip-audit (2.10.1) con/sin ignores, yamllint local, golden pytest, `gh run` (CI remoto @HEAD), versiones de mypy 1.19.1/ruff 0.15.10/bandit 1.9.4/pytest 8.4.2/import-linter 2.6.