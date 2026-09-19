# RECONCILIACIÓN DE INCONSISTENCIAS IN-01..IN-06 — Master Consolidation vs Evidencia

- **Fecha:** 2026-09-18
- **Modo:** READ-ONLY / FORENSE / RECONCILIACIÓN (sin escritura de código, CI, tracking, ADRs ni Plan Maestro). Escritura limitada a `docs/audits/`: este documento + correcciones documentales aplicadas a master/legacy/CI-CD (§14).
- **Estado de Git:** rama `feat/adr0028-bookbuilder`, HEAD `cb6c6d7c`. Working tree preexistente preservado (ver §15).
- **Protocolo:** AUDIT_PROTOCOL v2.1 — Control FAIL ≠ Finding NUEVO; REVALIDADO vs NUEVO; evidencia reproducible antes de aserto.
- **Sujeto:** las 6 inconsistencias (IN-01..IN-06) detectadas entre `AUDIT_OCM_master-consolidation_2026-09-17.md` y las fuentes primarias (código, CI, tracking.yaml, audit CI/CD, legacy, deployment-portability).

---

## 1. RESUMEN EJECUTIVO

Se reconciliaron las 6 inconsistencias solicitadas. En todos los casos el aserto del **master consolidation** (o del audit secundario en IN-02/IN-03) se contrastó contra evidencia mecánica reproducible (git, lint-imports, workflows, tracking.yaml):

| IN | Severidad | Aserto correcto (evidencia) | Aserto incorrecto (documental) |
|---|---|---|---|
| IN-01 | CRÍTICA | `scripts/check_production_gates.py` **EXISTE** y corre en CI | Master §17/F-MC-18: "no existe" |
| IN-02 | ALTA | Matriz L-01..L-40 = 40 filas, **18 CONFIRMADO_DEAD, 1 EN_USO (L-30)** | Master/legacy §2: "67 candidatos, 8 CONFIRMADO_DEAD" |
| IN-03 | ALTA | `.env` **nunca commiteado** (git vacío); exposición real = `deploy/host.env` tracked | F-LEGACY-001/F-MC-11: ".env con secretos commiteados" |
| IN-04 | MEDIA | import-linter ejecutado = **50 contracts kept** | Master §1/§3/§20: "49 contratos" |
| IN-05 | MEDIA | Master/PLAN P1/P7/P8 = readiness/ADR-0017/BookBuilder | CI/CD audit §19/§24: "P1 Governance / P7 CI/CD / P8 Verification" |
| IN-06 | MEDIA | Master §5 omite el CI/CD audit | Ausencia de `AUDIT_OCM_CI-CD_github-actions_2026-09-17.md` |

**Veredicto:** ninguna inconsistencia de las 6 altera el veredicto global del master ni bloquea P0..P8. Cuatro (IN-03, IN-04 + las dos documentales IN-01) son errores documentales del propio master; IN-02 e IN-05 son internos de los audits secundarios. La corrección es documental, en `docs/audits/`, sin tocar código/CI/tracking/ADRs/PLAN.

---

## 2. IN-01 (CRÍTICA) — F-MC-18: `check_production_gates.py` "no existe" es FALSO

**Afirmación del master:**
- §17 (líneas 300-301): "Production gate script (`check_production_gates.py`) **no existe** pese a `pyproject.toml` → F-MC-18 CONTRADICCIÓN con ADR-0020."
- F-MC-18 (líneas 730-741): "`scripts/check_production_gates.py` no existe en repo; ADR-0020 lo declara obligatorio". Status OPEN, Classification CONTRADICCIÓN. D-5 (línea 394): "crear script alineado ADR-0020".

**Evidencia mecánica (CONFIRMA EXISTENCIA):**
- `wc -c scripts/check_production_gates.py` → **18 595 bytes**, presente en filesystem.
- `git ls-files scripts/check_production_gates.py` → **trackeado**.
- `git log --all -- scripts/check_production_gates.py` → 9 commits, incl. `b2ffe0ba` (2026-08-23 "feat(quality): fix and integrate check_production_gates.py (B-49)") y `16e04a00` (2026-09-16 "fix(lint): resolve SIM201").
- `.github/workflows/ocm-ci.yml:360-362` → step "Production gates (code-level, G1/G2/G3/G10/G11)" con run `uv run python scripts/check_production_gates.py --mode gate-ci`, comentario `# B-49`.

**Conclusión IN-01:** el aserto del master ("no existe") es **FALSO** al 09-17. El script existe desde 2026-08-23. Ver resolución F-MC-18 vs B-49 en §8.

---

## 3. IN-02 (ALTA) — Conteos legacy internamente inconsistentes

**Inconsistencia dentro del audit legacy (`AUDIT_OCM_legacy-code-dead-code_2026-09-17.md`):**

| Fuente | Total | CONFIRMADO_DEAD | PROBABLE | POSIBLE | EN_USO |
|---|---|---|---|---|---|
| §2 Resumen ejecutivo (por categoría) | **67** | 8 | 23 | 36 | 0 |
| §9 Resumen final | **40** | 8 | 4 | 28 | 0 |
| Matriz §3 (recuento real L-01..L-40) | **40** | **18** | **2** | **19** | **1 (L-30)** |

El recuento de la matriz (§3, 40 filas) difiere de los dos resúmenes: 18 CONFIRMADO_DEAD (L-01 a L-08, L-19 a L-24, L-27, L-28, L-29, L-34), 2 PROBABLE (L-09, L-10), 19 POSIBLE, 1 EN_USO (L-30 signum). §9 (8/4/28) no coincide con la matriz; §2 (67) usa otra base (categorías, no filas de matriz).

**El master repite el agregado erróneo:**
- §14 (líneas 273-274): "matriz L-01..40, 67 candidatos): 8 CONFIRMADO_DEAD incl. `.env`…".
- F-MC-11 (línea 632): "F-LEGACY-001..015, 67 candidatos".

**Conclusión IN-02:** la afirmación basal correcta es la **matriz**: 40 candidatos, 18 CONFIRMADO_DEAD, 1 EN_USO (L-30). "67 candidatos / 8 CONFIRMADO_DEAD" proviene de §2 (base distinta) y no describe la matriz; el master la toma sin contrastar. Corrección documental: master §14/F-MC-11 deben usar "40 candidatos / 18 CONFIRMADO_DEAD" o citar explícitamente la base de §2.

---

## 4. IN-03 (ALTA) — `.env` con "secretos commiteados" es FALSO

**Afirmación:** F-LEGACY-001 (legacy §4, CRITICAL, recomienda `git rm --cached .env`) y F-MC-11 (master línea 629: "`.env` con secretos commiteados (CRITICAL, L-06)"). También CI/CD audit §22 (línea 374): "`.env` has committed secrets".

**Evidencia mecánica (git):**
- `git ls-files .env` → **vacío** (`.env` nunca trackeado).
- `git log --all -- .env` → **vacío** (ningún commit en cualquier rama tocó `.env`).
- `.gitignore:12` → `.env` ignorado.
- `ls -la .env` → `-rw-------` (chmod 600), 3662 bytes, existente solo en disco.
- Deployment-portability §8: "`.env` fuera de Git, chmod 600, hardening test" — verificado en la misma sesión.

**Qué está realmente versionado:** `deploy/host.env` y `deploy/host.env.example` **son trackeados** (git ls-files los lista; H-DEP-01 CONFIRMADO). Contienen **topología del host**, no secretos de API (deployment-portability §9: "No contiene secretos (API keys viven en `.env`)").

**Conclusión IN-03:** el aserto "`.env` commiteado" es **FALSO** (git lo refuta). La fuga real es de **topología** (`host.env` versionado), no de secretos. Correcciones documentales: legacy F-LEGACY-001 → reclasificar (`.env` fuera de Git; riesgo = host.env + secretos en disco con permisos); CI/CD §22 → no afirmar "commited secrets"; master F-MC-11 → corregir la causa raíz (host.env, no `.env`). La recomendación `git rm --cached .env` es inaplicable (no existe en el índice).

---

## 5. IN-04 (MEDIA) — Contratos import-linter: 50, no 49

**Afirmación del master:** §1 (línea 18) "49 contratos import-linter"; §3 (línea 87) tabla herramientas "49 contratos BC"; §20 (línea 354) "contract-linter 49/49".

**Evidencia mecánica:**
- `rg -c "^name = .BC-" architecture_linter/importlinter.toml` → **50**.
- `uv run lint-imports --config architecture_linter/importlinter.toml` (ejecutado en esta reconciliación) → **"Contracts: 50 kept, 0 broken."**
- CI/CD audit §7/§20 → "50 contracts". Tracking B-49 → "50 contracts KEPT".

**Conclusión IN-04:** el número real es **50** (config SSOT + ejecución). El "49" del master está desactualizado (resto del baseline 08-06). Corrección documental en master §1/§3/§20 (las 3 apariciones) y en la línea 15 del §1.

---

## 6. IN-05 (MEDIA) — Taxonomía de fases del CI/CD audit NO corresponde al Plan Maestro

**CI/CD audit** (§19 líneas 327-336 y §24 líneas 432-441) etiqueta gaps como:
- "P7 — CI/CD consolidation" (build, CD, deploy, rollback, self-hosted, environments, secrets, release)
- "P8 — Verification" (container scanning)
- "P1 — Governance" (tracking.yaml/ADR/audit validation)

**Taxonomía oficial (master §19 = PLAN-Maestro §4 "Fases siguientes — P0..P8", líneas 307-317):**
- **P1** = Readiness + crash-loop (B-59) — *no es governance*.
- **P7** = ADR-0017 discovery + perfil Bybit — *no es CI/CD*.
- **P8** = BookBuilder producción (ADR-0028) — *no es verification*.

**Conclusión IN-05:** las etiquetas P1/P7/P8 del CI/CD audit **no existen en la taxonomía del master/PLAN** y no son compatibles. Los gaps de CI/CD no mapean a ninguna P-fase del tramo (dependen del F4 del PLAN: B-57/B-58, CD/Grafana). Ver tabla de correspondencia en §10.

---

## 7. IN-06 (MEDIA) — Master §5 omite el CI/CD audit

**Evidencia:** el inventario de fuentes del master (§5, líneas 108-129, 20 filas) **no contiene** `AUDIT_OCM_CI-CD_github-actions_2026-09-17.md` (búsqueda: cero apariciones del nombre en el master; las únicas refs "CI-CD" del master son a deployment-portability). El audit CI/CD es del mismo día (09-17), está en `docs/audits/` (untracked) y no fue registrado como fuente revisada, a pesar de que su §24/§25 cruza explícitamente con el Plan Maestro.

**Conclusión IN-06:** omisión confirmada. Corrección documental: añadir la fila al §5 del master e incorporar sus conclusiones (incl. branch protection verificada por API, ver §9) al cuerpo de la consolidación.

---

## 8. FASE 2 — F-MC-18 vs B-49 (resolución definitiva)

**Origen:** F-PL-04 (2026-08-19, `OCM_AUDIT_FINDINGS_2026-08-19_policy-layer.md` líneas 119-147): "check_production_gates.py referenciado como ejecutable pero inexistente" — **correcto en esa fecha** (`ls scripts/` → ausente). Cadena: F-PL-04 → B-49 (F2.1, ALTA) → ADR-0020.

**Resolución (tracking.yaml B-49, líneas 2364-2410):**
- `estado: HECHO`, `estado_auditoria: CONFIRMADO`, `fecha_cierre: '2026-08-23'`.
- cadena.implementacion HECHO → "scripts/check_production_gates.py: bugfix… added gate-ci mode + CODE_LEVEL_GATES; .github/workflows/ocm-ci.yml: step 'Production gates (code-level)'".
- cadena.tests HECHO → "gate-ci → exit 0; gate-dev → 6/11 PASS; 1248 tests PASS; **50 contracts KEPT**".
- cadena.ci ECHO y cadena.cierre HECHO → "B-49 cerrado 2026-08-23. check_production_gates.py implementado (b2ffe0ba). PR #25 merged (674ffcb6)."

**Confirmación git independiente:**
- `b2ffe0ba` (2026-08-23) "feat(quality): fix and integrate check_production_gates.py (B-49)" ✓
- `674ffcb6` (2026-09-09) "Merge pull request #25 from OrangeCashDigital/feat/b49-production-gates-script" ✓ (PR #25 merged)
- `16e04a00` (2026-09-16) fix SIM201 en el script (mantenimiento posterior) ✓
- ocm-ci.yml:360-362 step CI invocando `--mode gate-ci` ✓

**Veredicto FASE 2:** **B-49 es el estado CORRECTO** (HECHO/CONFIRMADO con evidencia reproducible). **F-MC-18 es el aserto erróneo**: el master revalidó F-PL-04 (válido en 08-19) **sin consultar B-49 en tracking.yaml**, lo reclamó OPEN y propuso D-5 "crear script" que ya está implementado. Fallo de procedimiento: el master no aplicó la regla "control FAIL ≠ finding nuevo / contrastar contra tracking.yaml" antes de reabrir. La corrección documental sugerida (sin ejecutarse): F-MC-18 → REVALIDADO-CERRADO con referencia a B-49 (b2ffe0ba, PR #25), no OPEN, sin D-5.

---

## 9. FASE 3 — Reconciliación CI/CD (workflows + branch protection)

**Reconciliación de workflows (§ master vs CI/CD audit):** compatible. La tabla de 9 workflows del CI/CD audit (§3) coincidió con la lectura de `.github/workflows/` durante esta reconciliación; ninguna inconsistencia con el master (el master no describe CI/CD, el que lo hace es el CI/CD audit).

**Branch Protection — evidencia primaria reproducida por API en esta reconciliación:**

```
gh api repos/OrangeCashDigital/orangecashmachine/branches/main/protection
→ {"allow_deletions":false,"allow_force_pushes":false,"enforce_admins":true,
   "required_checks":13,"required_reviews":null,"strict":true}
```

Estado real en 2026-09-18:
| Item | Estado | Método |
|---|---|---|
| Branch protection | **PRESENTE** (strict, enforce_admins, sin force push, sin deletions) | GitHub API 200 OK |
| Required status checks | **13 checks** | GitHub API |
| Required PR reviews | 1 approver | API `required_reviews: null` en `main`; ver §31 CI/CD audit (1 approval) — campo no retornado por la API protegida, se respeta el dato del audit |

**Clasificación según encargo:** el §28 del CI/CD audit ("NO VERIFICADO") queda **superado** por su propia verificación complementaria (§29-§39) y confirmado por esta reproducción API independiente. NO se trata como "no verificada": la evidencia API sí permite reproducirla (regla del encargo). Se registra como **evidencia primaria**: estado PRESENTE, regla de rama `main`, 13 required checks, método `gh api`.

**Tabla de los 13 required checks (mapeo del audit §29, consistente con ocm-ci.yml):**
1. Unit tests (unit-tests), 2. Integration (integration-tests), 3. Domain purity (domain-guard), 4. Config validation, 5. Quality gates (quality), 6. Trading guards, 7. Security (bandit), 8. App layer guard, 9. CodeQL analyze, 10. Trivy, 11. Architecture contracts, 12. Engineering Health, 13. Gitleaks. Todos GitHub-hosted (app_id 15368), sin checks de terceros.

---

## 10. FASE 4 — Tabla de correspondencia taxonómica (CI/CD audit → Master/Plan)

| CI/CD audit item (§19/§24) | Etiqueta CI/CD audit | Fase real Master/Plan | Gate | Evidencia |
|---|---|---|---|---|
| Build / Artifact / Release / Deploy / Health post-deploy / Rollback / Environments / Secrets / Self-hosted | "P7 — CI/CD consolidation" | **F4** del PLAN (CD, B-57/B-58; obs) — NO P7 (P7 = ADR-0017/Bybit) | CD gate (pendiente, ocm-cd.yml placeholder) | ocm-cd.yml:2,12; CI/CD audit §9/§10/§11/§15/§16 |
| Container scanning (Trivy filesystem-only) | "P8 — Verification" | sin fase P asignada (mejora hardening F3/F4) — NO P8 (P8 = BookBuilder) | security job (no-bloqueante/UNKNOWN) | trivy.yml; CI/CD audit §6 |
| Governance gaps (tracking.yaml/ADR/audit validation) | "P1 — Governance" | **F2.1** Policy Layer (B-51, B-52, B-55, B-56; ADR-0031/0032) — NO P1 (P1 = readiness B-59) | engineering-health + policy-gate (parcial) | ocm-ci.yml policy-gate; CI/CD audit §8 |

**Conclusión FASE 4:** ninguna de las etiquetas P1/P7/P8 del CI/CD audit coincide con la taxonomía oficial. El mapeo correcto sitúa los gaps en F2.1 (governance) y F4 (CD), con P7/P8 del master intocados por CI/CD. Si el CI/CD audit necesita etiquetas de fase, debe usar las del §19 del master (o F2.1/F4 del PLAN), no inventar "P1 Governance/P7 CI-CD/P8 Verification".

---

## 11. FASE 5 — Estado operativo real de `check_production_gates.py`

| Dimensión | Estado | Evidencia |
|---|---|---|
| ¿Existe? | **SÍ** | `wc -c` 18 595 B; `git ls-files` lo lista |
| ¿Trackeado? | **SÍ** | git log --all: 9 commits (b2ffe0ba 08-23…16e04a00 09-16) |
| ¿Invocado en CI? | **SÍ** | ocm-ci.yml:362 `--mode gate-ci` en job **quality** |
| ¿Bloqueante? | **SÍ** (job quality es required check #5) | branch protection: 13 required checks incl. "Quality gates" |
| Mode gate-ci | G1/G2/G3/G10/G11 paso/bloqueo codificados | CI/CD audit §5/§7; B-49 cadena |
| Histórico | Creado 08-23 (B-49), PR #25 merged 09-09 | b2ffe0ba + 674ffcb6 |

**Distingue operación de "required check":** `check_production_gates.py` no es un status check; se ejecuta **dentro** del job `quality`, que sí es required check. Conclusión: el script es real, vivo y ejecutado en el gate de merge. F-MC-18 (mouth) es el falso aserto (ver §2/§8).

---

## 12. FASE 6 — Jenkins (sin cambios)

IN-01..IN-06 **no alteran** la conclusión previa del CI/CD audit (§17, §18, §26, §38):
- Jenkins ABSENT (grep no-venv → 0 referencias; sin Jenkinsfile).
- Branch protection verificada por API en §9 confirma que GitHub Actions cubre los 13 required checks; enforce_admins impide bypass. No hay capacidad NECESARIA que obligue a Jenkins.

**Conclusión FASE 6:** sin reabrir. Se mantiene "no se identificó capacidad necesaria que obligue a Jenkins".

---

## 13. FASE 7 — Impacto en fases P0..P8

| Fase | Ámbito (master §19) | Clasificación | Fundamento |
|---|---|---|---|
| P0 | Data plane baseline (F-MC-02) | **NO BLOQUEADA** | IN-01..06 no tocan offsets/producción de snapshot/delta |
| P1 | Readiness + crash-loop (B-59, F-MC-03) | **NO BLOQUEADA** | IN-05 tocaba etiqueta de gobernanza, no readiness |
| P2 | SSOT entrypoints/units (F-MC-06/19) | **NO BLOQUEADA** | sin relación con IN-01..06 |
| P3 | Config exchanges + env L2 (F-MC-01/12) | **NO BLOQUEADA** | IN-03 refuerza causa (host.env) pero no la bloquea |
| P4 | Schema v1→v2 + DLQ (F-MC-04/09) | **NO BLOQUEADA** | sin relación con IN-01..06 |
| P5 | GATE validación equipo | **NO BLOQUEADA** | IN-01..06 no afectan sign-off Ops/QA |
| P6 | Observabilidad (F-MC-08/10, B-58) | **NO BLOQUEADA** | sin relación con IN-01..06 |
| P7 | ADR-0017 + Bybit (F-MC-05) | **NO BLOQUEADA** | IN-05 era colisión de etiqueta, no de contenido |
| P8 | BookBuilder (ADR-0028) | **NO BLOQUEADA** | sin relación con IN-01..06 |

**Ninguna fase está BLOQUEADA POR EVIDENCIA ni es NO DETERMINABLE por las 6 inconsistencias.** IN-01..IN-06 son correcciones documentales; la corrección de F-MC-18 (closure B-49) podría **acelerar** P1/P2 al limpiar el inventario, pero no es un prerrequisito gate.

---

## 14. CAMBIOS REALIZADOS

Aplicadas por autorización del dueño de estado (09-18, encargo "correcciones documentales §2-§7"):

- **Master (`AUDIT_OCM_master-consolidation_2026-09-17.md`):** NOTA DE RECONCILIACIÓN en header; §1/§3/§20 → "50 contratos" (IN-04); §5 → CI/CD audit añadido al inventario (IN-06); §14 → "40 candidatos, 18 CONFIRMADO_DEAD, 2 PROBABLE, 19 POSIBLE, 1 EN_USO (L-30)" (IN-02); §17/F-MC-18/D-5 → resueltos por B-49 (b2ffe0ba, PR #25) (IN-01); F-MC-11 → `.env` NUNCA commiteado + host.env topología (IN-03).
- **Legacy (`AUDIT_OCM_legacy-code-dead-code_2026-09-17.md`):** NOTA DE RECONCILIACIÓN en header; §2 → nota base (matriz vs categorías) (IN-02); matriz L-06 → `.env` NO commiteado (IN-03); F-LEGACY-001 → reclasificado CRITICAL → RECHECK con evidencia git (IN-03); §9 → conteos reales 40/18/2/19/1 (IN-02); FASE 4 y §7 → alineados.
- **CI/CD (`AUDIT_OCM_CI-CD_github-actions_2026-09-17.md`):** NOTA DE RECONCILIACIÓN en header; §22 → riesgo `.env` reclasificado (IN-03); §19/§24 → nota taxonomía P1/P7/P8 → F2.1/F4 (IN-05); §25 → fila H-DEP-01 alineada.

Sin modificaciones a código, CI, tracking.yaml, ADRs, PLAN-Maestro, systemd, Docker o producción.

---

## 15. VALIDACIONES

- `git diff --check` → sin errores de whitespace.
- `git status --short` final (idéntico al inicial; las correcciones documentales de §14 se hicieron en los **audit files**, ya untracked):
- Commits: 0 · Pushes: 0 · Servicios: 0 · Código/CI/tracking.yaml/ADRs/PLAN: **sin modificar**.

---

## 16. CONCLUSIÓN

- **A. IN-01 (CRÍTICA):** F-MC-18 afirma que `check_production_gates.py` "no existe" — **falso**; existe (b2ffe0ba 08-23), corre en CI (ocm-ci.yml:362) y B-49 está HECHO/CONFIRMADO. Corrección: F-MC-18 → cerrado por B-49.
- **B. IN-02 (ALTA):** la base correcta de legacy es la **matriz L-01..L-40 = 18 CONFIRMADO_DEAD + 1 EN_USO (L-30)**; "67/8" es el resumen por categorías (§2), no la matriz. El master debe citar la base explícitamente.
- **C. IN-03 (ALTA):** `.env` **no fue commiteado nunca** (git ls-files y git log vacíos; `.gitignore:12`; chmod 600). La exposición real versionada es `deploy/host.env` (topología, sin secretos). F-LEGACY-001/F-MC-11/CI/CD §22 deben corregirse; `git rm --cached .env` es inaplicable.
- **D. IN-04 (MEDIA):** contratos = **50** (config SSOT + ejecución "50 kept, 0 broken"), no 49. Master §1/§3/§20 desactualizado.
- **E. IN-05 (MEDIA):** las etiquetas "P1 Governance / P7 CI/CD consolidation / P8 Verification" del CI/CD audit no existen en la taxonomía master/PLAN; mapean a F2.1/F4, no a P1/P7/P8. Necesita tabla de correspondencia (entregada en §10).
- **F. IN-06 (MEDIA):** master §5 omite el CI/CD audit como fuente; debe incorporarse, incl. su verificación complementaria de branch protection.
- **G. Branch protection (asociada a IN-05/IN-06):** **PRESENTE** por API (13 required checks, strict, enforce_admins, sin force push), **no debe tratarse como NO VERIFICADO** — la evidencia API permite reproducirla (regla del encargo, §9).

**Cierre:** con autorización del dueño de estado (09-18), las correcciones documentales de §2-§7 **fueron aplicadas** a los 3 documentos sujetos (master, legacy, CI/CD — detalle en §14). Sin commits, sin pushes.

---

## Estado final

- **git status:** solo el trabajo preexistente (2 modificados) + 7 auditorías 09-17/09-18 no trackeadas (la nueva incluida).
- **Modificaciones de este agente:** `docs/audits/AUDIT_OCM_reconciliacion-IN-01-06_2026-09-18.md` + correcciones documentales en los 3 audit files sujetos (ver §14).
- **Commits: 0 · Pushes: 0 · Servicios: 0 · Sistema: 0.** Código/CI/tracking.yaml/ADRs/PLAN: sin modificar (los `M` de PLAN/tracking son preexistentes).