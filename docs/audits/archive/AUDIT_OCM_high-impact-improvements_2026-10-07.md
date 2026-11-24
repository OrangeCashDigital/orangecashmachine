# AUDIT — OCM Mejoras de Alto Impacto

**Fecha:** 2026-10-07
**Rol solicitado:** Senior Data Engineer / Architecture Auditor (auditoría priorizada de mejoras de alto impacto)
**Contexto normativo:** protocolo formal [`docs/governance/AUDIT_PROTOCOL.md`](../governance/AUDIT_PROTOCOL.md) v2.1, principio Read-Only (§K), control FAIL ≠ finding NUEVO (§H), Mecánica primero (§Q).

---

## 1. Executive Summary

Auditoría priorizada sobre la deuda de alto impacto de OCM, siguiendo estrictamente el orden
de descubrimiento §C (AGENTS → AUDIT_PROTOCOL → PLAN → GOVERNANCE → tracking → ADRs → audits
→ architecture_linter → CI) y el tooling mecánico primero §Q (M1..M26). Se audita el commit
`05317f14` en `feat/m22b-governance`, con working tree con 18 modificados + 36 untracked
(no modificados por esta auditoría).

Hallazgos de esta auditoría: **7** (1 NUEVO, 0 REVALIDADO, 5 CONTRADICCIÓN, 1 RECOMENDACIÓN;
severidad: 1 CRITICAL, 4 HIGH, 2 MEDIUM). El hallazgo de dependencias (F-OCM-06) es el único
NUEVO: las `CVE-2026-85394` (python-jose) y `CVE-2026-104874` (multidict) no estaban
registradas en auditorías previas ni bajo risk-accept. El sub-hallazgo **F-OCM-06b** (detectado
al preparar los diffs del Punto 2) confirma que el fix de `multidict` (≥6.9.1) es **insatisfacible**
por el pin exacto `multidict==6.7.1` de `ccxt>=4.5.74` — contradicción de grafo, no vulnerabilidad
huérfana. El resto contrasta contra tracking/ADRs
y se clasifica como CONTRADICCIÓN o RECOMENDACIÓN (§H), nunca como NUEVO.

Las conclusiones principales:

- **Bloqueo G1/D20b confirmado (CRITICAL).** Existe contradicción formal entre `D19_HUMAN_
  DECISION_PACK_2026-09-30.md` + `ADR-0049` (D19-01 / D20b = `DECIDED`) y `D19-01_INDUSTRY_
  BOW_ARCHITECTURE_REVIEW_2026-10-03.md` + `KNOWLEDGE_BASE_CONTRACT.md` (D19-01 = `HUMAN
  DECISION REQUIRED`). El pack 2026-09-30 (líneas 2270, 2289-2291) **exige human review antes
  de any materialización**; ADR-0049:553-561 cambió el estado a `DECIDED`. **No se materializa
  D20b-01..06** (IMPLEMENTATION = NOT AUTHORIZED).
- **Efectos colaterales de la materialización G1 (HIGH).** El commit `17e6dc85` introdujo
  `B-25 estado: EN_PROGRESO` en `tracking.yaml` (líneas 456, 492), estado **fuera del enum**
  `ESTADOS` (`engineering_health_check.py:36`) → 2 jobs CI en rojo y `M4` en el validador por
  el `**Estado:**` de ADR-0049 (línea 3), que no es un estado base del enum
  (`{ACEPTADO, OBSOLETO, PROPUESTO, REEMPLAZADO}`).
- **Contradicción de estados B-47/B-51/B-54 (MEDIUM).** El Plan Maestro (§8, líneas 151,
  555-568) los declara **CERRADO**; `tracking.yaml:2537/2740/2931` los mantiene **PENDIENTE**.
  `tracking.yaml` es el SSOT (N2).
- **cobertura Real 57.88%** contra `fail_under = 40` (`pyproject.toml:339`) — margen amplio;
  el badge de README es prematuro y debe retrasarse hasta ≥70%.
- **Contradicción mypy (HIGH).** `tracking.yaml:420` claim "Success, no issues found in 356
  source files (2026-08-12)"; la medición real del commit auditado es **329 errores en 118
  archivos (399 source files)**.
- **2 vulnerabilidades nuevas (HIGH/NUEVO).** `python-jose 3.5.0` CVE-2026-85394 (sin fix) y
  `multidict 6.7.1` CVE-2026-104874 (fix 6.9.1). No cubiertas por los risk-accepts de
  CANONICAL_COMMANDS (PYSEC-2026-113, PYSEC-2026-1325).

Veredicto global: la deuda detectada es **gobernanza/estado, no arquitectura de datos**.
Ninguno de los hallazgos es infraestructura nueva; todos son correcciones de estado,
documentación y gates de CI. El único bloqueo real es G1/D20b, que requiere decisión humana
(no el agente).

---

## 2. Scope y Metodología

- **Alcance:** mejoras de alto impacto priorizadas — identidad canónica (G1/D20b), B-25,
  cobertura, mypy, dependencias, y la contradicción B-47/B-51/B-54 detectada en contraste de
  fuentes. Read-only estricto (protocolo §K): única escritura en `docs/audits/`.
- **Metodología:** orden de descubrimiento §C; tooling mecánico primero §Q (M1..M26);
  taxonomía estricta §G; reconciliación §H; jerarquía de autoridad (código/tests > contratos
  > ADR > tracking > documentación > KB).
- **Ejecución mecánica previa al juicio LLM:** los comandos canónicos §R se ejecutaron sobre el
  commit auditado; el LLM juzga solo los puntos que el validador no cubre (ambigüedad G1,
  priorización).

### 2.1 Governance Baseline

```
REPRODUCIBILIDAD
- commit: 05317f14 (docs(architecture): materialize H3 decision records)
- branch: feat/m22b-governance
- fecha: 2026-10-07
- protocolo: AUDIT_PROTOCOL v2.1
- agente/modelo: opencode
- herramientas: ruff 0.15.10 · mypy 1.19.1 · bandit 1.9.4 · pytest 8.4.2 · yamllint 1.38.0 · pip-audit 2.10.1
- comandos:
    - uv run lint-imports --config architecture_linter/importlinter.toml   (ARCH_CONTRACTS)
    - uv run python scripts/engineering_health_check.py                     (ENGINEERING_HEALTH)
    - uv run python -m architecture_linter --root . --json                  (ARCH_LINTER)
    - uv run pytest tests/architecture_linter/test_golden.py -q --no-cov    (GOLDEN)
    - uv run pip-audit . --ignore-vuln PYSEC-2026-113 --ignore-vuln PYSEC-2026-1325  (DEPENDENCY_AUDIT)
    - uvx yamllint -c .yamllint .                                           (YAMLLINT)
    - uv run python scripts/audit_validator.py                              (M1..M26)
    - uv run pytest tests/ -q -m "not integration" --cov=packages --cov=ocm
        --cov=shared --cov=apps --cov-report=term                           (UNIT_TESTS + cobertura)
- golden: PASS (6 tests)
- resultado: validador FAIL (6 errores: M21×5 + M4×1 — pre-existentes al commit auditado)
```

---

## 3. Findings

### F-OCM-01 — CRITICAL · CONTRADICCIÓN · G1/D20b: estado normativo contradictorio (DECIDED vs HUMAN DECISION REQUIRED)

**Detección:** contraste de fuentes §C sobre el estado de D19-01/D20b tras la materialización
G1.

**Evidencia (conflicto directo):**

- `docs/audits/D19_HUMAN_DECISION_PACK_2026-09-30.md:2262-2270` — "D19 EXECUTIVE STATUS":
  `D19-01 = DECIDED: C+ … D20b = DECIDED — PHYSICAL PERSISTENCE / REPRESENTATION ARCHITECTURE";
  :2270 `**IMPLEMENTATION:** NOT AUTHORIZED`.
- `docs/audits/D19_HUMAN_DECISION_PACK_2026-09-30.md:2289-2291` — "Decision registration":
  `D20b-01..06 = DECIDED…; Implementation: NOT AUTHORIZED`. El pack es una **presentación
  deliberativa**: "No autoriza implementación y no cambia otros estados. Solo presenta
  alternativas… para deliberación humana".
- `docs/architecture/decisions/ADR-0049-canonical-instrument-identity-g1.md:553-561` —
  `D20b = DECIDED… D20b-01..06 = DECIDED; IMPLEMENTATION = NOT AUTHORIZED`.
- `docs/audits/D19-01_INDUSTRY_BOW_ARCHITECTURE_REVIEW_2026-10-03.md:32-35,167` —
  `D19-01 = HUMAN DECISION REQUIRED; DECISION REGISTERED = NO`.
- `docs/knowledge/KNOWLEDGE_BASE_CONTRACT.md:166` — `D19-01 = HUMAN DECISION REQUIRED ·
  D19 = PENDING / BLOCKED · GATE C = BLOCKED`.

**Interpretación (§B, jerarquía de autoridad):** el ADR-0049 (autoridad superior al pack) ha
fijado formalmente D20b = DECIDED, pero el propio ADR-0049:561 y el pack mantienen
`IMPLEMENTATION = NOT AUTHORIZED`. Mientras no exista revisión humana explícita que cierre la
ambigüedad (`D19-01 = _C+` está registrado como DECIDED en el pack y el ADR; pero
KN_BASE_CONTRACT y la Industry BOW Review posterior — 2026-10-03 — siguen marcando HUMAN
DECISION REQUIRED), **no se materializa nada de D20b-01..06**. Es un bloqueo, no un hallazgo
de implementación.

**Decisión humana requerida:** resolver cuál fuente es normativa vigente (¿ADR-0049 tras la
Revisión 2026-10-03, o la revisión posterior que mantiene HUMAN DECISION REQUIRED?) y autorizar
o diferir la materialización D20b-01..06.

### F-OCM-02 — HIGH · CONTRADICCIÓN · B-25 `EN_PROGRESO` inválido + ADR-0049 estado no canónico (M4)

**Detección:** guardianes mecánicos (ENGINEERING_HEALTH + AUDIT_VALIDATOR M4).

**Evidencia:**

- `docs/plans/tracking.yaml:452-456,492` — `id: B-25` con `estado: EN_PROGRESO` (dos
  ocurrencias: item y `cadena.implementacion`).
- `scripts/engineering_health_check.py:36` — enum `ESTADOS = {PENDIENTE, EN_CURSO, HECHO,
  VERIFICACION, RECHAZADO}`. **`EN_PROGRESO` no está en el enum** → 2 incoherencias F2.0 →
  `test_engineering_health_passes` en rojo.
- `scripts/audit_validator.py` M4 — `ADR-0049-canonical-instrument-identity-g1.md:3`:
  `**Estado:** Registro normativo de la serie G1…` no es un estado base válido
  (`{ACEPTADO, OBSOLETO, PROPUESTO, REEMPLAZADO}`) → `test_full_repo_audit_validator_passes`
  en rojo.

**Impacto:** 2 jobs de CI (architecture `test_full_repo_audit_validator_passes` y
`test_engineering_health_passes`) en rojo por deuda de estado introducida/reflejada en los
commits de materialización G1 (`17e6dc85`, luego `05317f14`).

**Decisión humana requerida:** elegir corrección (a) renombrar estado a clave válida del enum
p.ej. `EN_CURSO`, o (b) extender el enum `ESTADOS` con `EN_PROGRESO` (cambia enum SSOT ⇒
requiere cadena de adopción §B).

### F-OCM-03 — MEDIUM · CONTRADICCIÓN · B-47/B-51/B-54 CERRADO (Plan Maestro) vs PENDIENTE (tracking.yaml)

**Detección:** contraste Plan Maestro ↔ tracking (orden §C; N2 = SSOT).

**Evidencia:**

- `docs/PLAN-Maestro-Ingenieria.md:151` — "**B-47 cerrado:** ruff select activa C901/PLR/SIM";
  `:307` — copy F2.1 con B-47..B-56; `:555-568` — "**B-54 CERRADO** … **B-51 CERRADO
  (Option 3)**".
- `docs/plans/tracking.yaml:2537,2541 (B-47)`; `:2740 (B-51)`; `:2931 (B-54)` — los tres con
  `estado: PENDIENTE`.

**Interpretación:** `tracking.yaml` es el SSOT operativo (y `estado_auditoria: CONFIRMADO` en
tracking para B-47). El Plan Maestro documenta avances **del desarrollo** pero su columna de
estado sigue siendo la fuente para el health check. La contradicción es documental; el health
check no la marca (los tres siguen `PENDIENTE` en tracking). Riesgo de ambigüedad para el
equipo y para agentes.

### F-OCM-04 — MEDIUM · RECOMENDACIÓN · cobertura 57.88% con fail_under=40; badge prematuro

**Detección:** mecánica (cobertura real del commit auditado).

**Evidencia:**

- `pyproject.toml:320` — `addopts` con `--cov`; `:339` — `fail_under = 40` (baseline 44%
  histórico, margen por debajo del real).
- Medición del commit auditado (UNIT_TESTS con `--cov=packages --cov=ocm --cov=shared
  --cov=apps`): **TOTAL 57.88%** (15493 statements, 6525 missing; `packages` 50.7%, `ocm`
  75.2%, `shared` 93.8%, `apps` 59.1%).
- README atesora badge de cobertura; no hay gate de "publicable" (≥70%).

**Recomendación (no NUEVO — deuda conocida, R5/B-06):** subir `fail_under` gradualmente
(40 → 50 → 60) en PRs aislados y solo publicar badge en README cuando `fail_under ≥ 70`.
Diff preparado en el punto 2 del encargo (p. ej. `fail_under = 50` como paso siguiente).

### F-OCM-05 — HIGH · CONTRADICCIÓN · claim mypy "Success 356" vs 329 errores reales

**Detección:** mecánica (MYPY `--no-incremental`).

**Evidencia:**

- `docs/plans/tracking.yaml:420` — claim `'mypy . --no-incremental: Success, no issues found
  in 356 source files (2026-08-12)'`.
- Medición real del commit auditado: **`Found 329 errors in 118 files (checked 399 source
  files)`**.

**Interpretación:** el claim documentado (2026-08-12) es un snapshot antiguo; el CI actual
usa `uv run mypy . --no-incremental` por `ocm-ci.yml`. Con 329 errores el gate mypy está roto
de facto. **No es un finding NUEVO** (la deuda de tipado es conocida), pero sí una
**CONTRADICCIÓN** entre `tracking.yaml` y la medición CI vigente — tracking afirma un éxito que
ya no es cierto.

**Decisión humana requerida:** (a) eliminar/actualizar el claim en tracking (marcar VERIFICACION
o dejar evidencia de regresión), (b) fijar un plan de reducción de errores tipado (baseline
329 → meta), y (c) decidir si el gate mypy permanece bloqueante mientras la deuda es 329.

### F-OCM-06 — HIGH · NUEVO · 2 vulnerabilidades nuevas (python-jose, multidict)

**Detección:** mecánica (DEPENDENCY_AUDIT con los ignores canónicos CI).

**Evidencia:**

- `uv run pip-audit . --ignore-vuln PYSEC-2026-113 --ignore-vuln PYSEC-2026-1325` →
  `Found 2 known vulnerabilities, ignored 2 in 2 packages`:
  - `python-jose 3.5.0` — CVE-2026-85394 (sin fix listado).
  - `multidict 6.7.1` — CVE-2026-104874 (fix `6.9.1`).
- `scripts/audit_validator.py:140-145` — CANONICAL_COMMANDS: los ignores vigentes son
  `PYSEC-2026-113` (aiohttp) y `PYSEC-2026-1325`; las CVE-2026-85394/104874 **no están
  cubiertas** por risk-accept previo.

**Interpretación:** NUEVO (no deduplicable contra registers previos, §H). `multidict` tiene fix
(`6.9.1`); `python-jose` no publica fix en el listado — requiere evaluación (¿sustituir
python-jose, risk-accept formal con ADR+tracking, o esperar fix?). No excedía el perímetro
original del encargo, pero es la vulnerabilidad NUEVA de mayor prioridad en seguridad de deps.

### F-OCM-06b — HIGH · CONTRADICCIÓN · bump `multidict>=6.9.1` insatisfacible por pin de ccxt

> Sub-hallazgo de F-OCM-06, detectado durante la preparación de los diffs del Punto 2
> (regla §4: nueva contradicción → detenerse y reportar). Confirmado por la humana
> 2026-10-07.

**Evidencia (resolución real, no inferencia):**

- `uv lock --dry-run` (sin modificar el repo) sobre `[project.dependencies]` con
  `"multidict>=6.9.1": "Because ccxt>=4.5.74 depends on multidict==6.7.1 … your project's
  requirements are unsatisfiable"`.
- `uv lock --dry-run` pinning `ccxt==4.5.74 / 4.5.75 / 4.5.76` + `multidict>=6.9.1` → las tres
  resolven `multidict==6.7.1`.
- `uv.lock:356-371` — `ccxt 4.5.74` declara `{ name = "multidict" }` como dependencia directa.

**Causa raíz:** todas las versiones publicadas de ccxt (4.5.74..4.5.76) fijan
`multidict==6.7.1` con pin exacto. `aiohttp 3.14.3` (uv.lock:52) y `yarl 1.24.2` (uv.lock:3361)
toleran rangos abiertos; **ccxt no**. El bump directo a `>=6.9.1` rompe la resolución.

**Interpretación:** CONTRADICCIÓN (no vulnerabilidad huérfana): el fix formalmente disponible no
es alcanzable con el pin `ccxt>=4.5.74` vigente. No es un defecto de implementación, es un
conflicto de restricciones en el grafo. No es NUEVO (la CVE ya está registrada en F-OCM-06);
clasifica como CONTRADICCIÓN de alcance.

**Decisión humana:** risk-accept formal de CVE-2026-104874 (D-OCM-07a) + item de tracking de
monitoreo de releases de ccxt + actualización del ignore canónico (bump de multidict fuera de
alcance mientras ccxt lo pinte a 6.7.1).

---

## 4. Matriz de Findings

| F-ID | Severidad | Clasificación |
|---|---|---|
| F-OCM-01 | CRITICAL | CONTRADICCIÓN |
| F-OCM-02 | HIGH | CONTRADICCIÓN |
| F-OCM-03 | MEDIUM | CONTRADICCIÓN |
| F-OCM-04 | MEDIUM | RECOMENDACIÓN |
| F-OCM-05 | HIGH | CONTRADICCIÓN |
| F-OCM-06 | HIGH | NUEVO |
| F-OCM-06b | HIGH | CONTRADICCIÓN |

Reconciliación: 7 findings = 1 NUEVO + 0 REVALIDADO + 5 CONTRADICCIÓN + 1 RECOMENDACIÓN =
1 CRITICAL + 4 HIGH + 2 MEDIUM. ✓

---

## 5. Matriz de Controles

| Control | Comando canónico | Exit esperado | Resultado real |
|---|---|---|---|
| ARCH_CONTRACTS | `uv run lint-imports --config architecture_linter/importlinter.toml` | 0 | **PASS** (51 kept, 0 broken) |
| ENGINEERING_HEALTH | `uv run python scripts/engineering_health_check.py` | 0 | **FAIL** (2 incoherencias por B-25) → F-OCM-02 |
| ARCH_LINTER | `uv run python -m architecture_linter --root . --json` | 1 si FAIL/PARTIAL | **FAIL** (7/10 reglas en 16 findings REVALIDADO) |
| GOLDEN | `uv run pytest tests/architecture_linter/test_golden.py -q --no-cov` | 0 | **PASS** (6) |
| UNIT_TESTS | `uv run pytest tests/ -q -m "not integration"` | 0 | **FAIL** (2: audit_validator_passes, engineering_health_passes) → F-OCM-02 |
| RUFF_CHECK | `uv run ruff check .` | 0 | **PASS** |
| RUFF_FORMAT | `uv run ruff format . --check` | 0 | **PASS** |
| MYPY | `uv run mypy . --no-incremental` | 0 | **FAIL** (329 errores / 118 archivos) → F-OCM-05 |
| BANDIT | `uv run bandit -r apps ocm packages shared infrastructure -ll` | 0 | **PASS** |
| DEPENDENCY_AUDIT | `uv run pip-audit . --ignore-vuln PYSEC-2026-113 --ignore-vuln PYSEC-2026-1325` | 0 | **FAIL** (2 vulns) → F-OCM-06 |
| YAMLLINT | `uvx yamllint -c .yamllint .` | 0 | **FAIL** (exit 1, solo warnings) |
| AUDIT_VALIDATOR | `uv run python scripts/audit_validator.py` | 0 | **FAIL** (6 errores: M21×5 + M4×1) → F-OCM-02 |

Controles = PASS(5) + FAIL(7) = 12

Total de controles: 12. PASS 5, FAIL 7. Los FAIL de los controles mapean a F-OCM-01/02/05/06 y a
deuda gobernada REVALIDADO (ARCH_LINTER, YAMLLINT); ningún FAIL genera finding NUEVO en
deduplicación (§H) salvo F-OCM-06.

---

## 6. Matriz de Decisiones

| D-ID | Pregunta | Opciones | Consecuencias | Finding |
|---|---|---|---|---|
| D-OCM-01 | ¿Qué fuente es normativa para D19-01/D20b y autoriza materialización? | (a) Declarar ADR-0049 vigente tras Revisión 2026-10-03 y autorizar D20b-01..06 · (b) Mantener HUMAN DECISION REQUIRED (INDUSTRY_BOW_REVIEW) y diferir · (c) Actualizar KN_BASE_CONTRACT para alinear con ADR | (a) desbloquea roadmap de identidad; (b) mantiene estabilidad, deuda de estado; (c) cierra la contradicción documental | F-OCM-01 |
| D-OCM-02 | B-25 `EN_PROGRESO` inválido | (a) mapper a `EN_CURSO` (recomendada — no toca enum) · (b) añadir `EN_PROGRESO` al enum `ESTADOS` (requiere cadena §B) | (a) restaura CI en 1 commit; (b) amplía el enum SSOT y exige decisión formal | F-OCM-02 |
| D-OCM-03 | ADR-0049 `**Estado:**` no canónico (M4) | (a) corregir frontmatter a un estado base válido p.ej. `ACEPTADO` con referencia adjunta · (b) ampliar enum ADR_BASE_STATES | (a) restaura M4; (b) desnormaliza el enum de estados ADR | F-OCM-02 |
| D-OCM-04 | B-47/B-51/B-54: reconciliar Plan↔tracking | (a) marcar CERRADO + fecha_cierre en tracking conforme al Plan (con evidencia) · (b) corregir el Plan para no declarar CERRADO | (a) alinea con avance real documentado; (b) mantiene tracking como SSOT incuestionable | F-OCM-03 |
| D-OCM-05 | Cobertura | (a) subir `fail_under` escalonado 40→50→60 sin publicar badge hasta ≥70% (recomendada) · (b) publicar badge ya que 57.88% > 40 | (a) conduce a gate real de 70%; (b) riesgo de señal de calidad engañosa | F-OCM-04 |
| D-OCM-06 | Claim mypy desactualizado y gate bloqueante | (a) actualizar tracking (evidencia 329 errores) y mantener gate bloqueante con plan de reducción · (b) relajar gate mientras la deuda persiste · (c) solo documentar | (a) protege calidad; (b) evita bloqueo constante; (c) mínimo coste, sin gate | F-OCM-05 |
| D-OCM-07 | Vulnerabilidades nuevas | (a) bump `multidict` ≥6.9.1 (fix disponible) + evaluar sustitución de `python-jose` o risk-accept formal · (b) risk-accept de ambas con ADR+tracking · (c) esperar fixes | (a) cierra CVE-2026-104874 y gestiona CVE-2026-85394; (b) mantiene riesgo registrado; (c) riesgo abierto sin gestión | F-OCM-06 |

**Resolución humana (2026-10-07):** D-OCM-07a → risk-accept formal de CVE-2026-104874
(ADR-0050 + tracking + ignore canónico); NO se bumpea `multidict`, NO se toca `ccxt`.
D-OCM-07b → sin decidir: evaluar ADR-0051 (sustitución python-jose→PyJWT) vs ADR-0052
(risk-accept CVE-2026-85394). Sub-hallazgo F-OCM-06b registrado en §3/§4.

---

## 7. Integridad

- Read-only estricto (protocolo §K): no se modificó código, tests, CI, ADRs, tracking ni
  dependencias. Única escritura: `docs/audits/` (este informe). No se creó registro
  `OCM_AUDIT_FINDINGS_*` propio (punto 2 del encargo: diffs a preparar, no aplicar).
- Orden de descubrimiento §C cumplido.
- Contraste de cada FAIL contra tracking.yaml y ADRs (§H): los controles FAIL son 4 hallazgos
  (F-OCM-01/02/05/06) + deuda gobernada REVALIDADO (ARCH_LINTER, YAMLLINT). Ningún FAIL se
  convierte en NUEVO sin deduplicación previa.
- Contadores reconciliados matemáticamente (§4-5).
- La KB es referencia, no autoridad (§B): las citas de `KNOWLEDGE_BASE_CONTRACT.md` se usan
  como evidencia de estado normativo ambiguo, no como norma arquitectónica.
- Bloqueo explícito: **D20b-01..06 NO se implementan** (IMPLEMENTATION = NOT AUTHORIZED);
  los diffs que dependan de la resolución G1 se marcarán `BLOQUEADO HASTA G1` en el punto 2.
- Veredicto global: **Gobernanza, no arquitectura de datos.** Deuda priorizable 1-crítica
  (G1/D20b, decisión humana) + 3 high accionables por el equipo con decisiones D-OCM-02/03/05.

---

## 8. Roadmap propuesto (requiere decisión humana; fuera del alcance read-only)

1. **Resolución G1/D20b (D-OCM-01)** — decisión humana; es el único bloqueo crítico.
2. **Punto 2 del encargo:** preparar diffs (sin aplicar) para:
   - B-25 `EN_PROGRESO` → `EN_CURSO` (opciones A/B) y ADR-0049 frontmatter (D-OCM-02/03);
   - reconcilación B-47/B-51/B-54 (D-OCM-04);
   - `fail_under` escalonado (D-OCM-05);
   - claim mypy y estrategia de gate (D-OCM-06);
   - bump `multidict` ≥6.9.1 + gestión python-jose (D-OCM-07);
   - diffs dependientes de G1 marcados `BLOQUEADO HASTA G1`.
3. Aprobación humana de diffs → commits atómicos (`fix`, `chore`) con la cadena de adopción
   §B cuando toque enum/frontmatter.