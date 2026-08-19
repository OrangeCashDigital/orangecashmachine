# AUDIT — OCM Documentation & Tooling Architecture
**Fecha:** 2026-08-19
**Tipo:** Adversarial, read-only
**Alcance:** Documentación, docstrings, tooling, agent-readiness
**Auditor:** Claude (Principal Software Architect + Docs/Knowledge Auditor)
**Estado:** EN PROGRESO — se completa incrementalmente por sesión

## 1. Executive Summary
_Pendiente — se redacta al cierre, tras findings completos._

## 2. Scope
Auditoría documental completa según protocolo de `docs/governance/AUDIT_PROTOCOL.md`
y encargo definido en sesión 2026-08-19. Read-only estricto: solo se escribe en
`docs/audits/`.

## 3. Governance Baseline (confirmado)
- Orden de descubrimiento canónico (AGENTS.md): Plan Maestro → GOVERNANCE.md →
  tracking.yaml → ADRs → CI/Linters.
- `docs/architecture/GOVERNANCE.md` — EXISTE (verificado, corrige falso positivo inicial).
- `docs/PLAN-Maestro-Ingenieria.md` — EXISTE en raíz de docs/.
- `docs/plans/tracking.yaml` — SSOT de reglas/hallazgos activos (schema_version: 2).
- Tooling mecánico primero: `scripts/audit_validator.py` (reglas M1-M20) — EXISTE
  y CONFIRMADO EJECUTADO (ver Fase 9).

## 4. Methodology
Ejecución de comandos canónicos vía SSH (orangehouse), sin modificación de
código/tests/CI/ADRs/tracking. Evidencia clasificada como EXISTENTE / NO ENCONTRADO
/ INFERIDO por cada afirmación.

## 5. Repository Inventory (parcial)
Estructura `docs/`: architecture/{decisions,logs,recovered}, audits/ (~90 archivos),
governance/, knowledge/{mappings,notes}, planning/, plans/.

### 5.1 Hallazgos preliminares de inventario
- **F-DOC-01** (candidato): `docs/planning/` y `docs/plans/` coexisten — posible
  redundancia de naming. Requiere verificación de contenido antes de confirmar.
- **F-DOC-02** (candidato): Material de KB (`.pdf`, `.zip` de freqtrade/hummingbot/
  nautilus_trader) vive suelto en raíz de `docs/`, fuera de `docs/knowledge/` donde
  reside `manifest.yaml` (SSOT declarado de la KB según AGENTS.md). Contradice el
  modelo de gobernanza documentado.
- **F-DOC-03** (candidato): Naming inconsistente en `docs/audits/` — conviven
  `YYYY-MM-DD-kebab-case.md` y `AUDIT_SCREAMING_SNAKE_CASE.md` sin convención
  declarada.
- **F-DOC-04** (candidato): Posible triplicado semántico mismo día (2026-08-19):
  `OCM_AUDIT_FINDINGS_2026-08-19-policy-complementary.md`,
  `-policy-layer-complementary.md`, `-policy-layer.md`. Pendiente diff de contenido.
- **F-DOC-05**: Ya existe `docs/audits/2026-08-18-kafka-topology-audit.md` — auditoría
  Kafka previa (1 día). Cualquier hallazgo Kafka futuro debe contrastarse contra este
  documento antes de clasificar como NUEVO (protocolo Control FAIL ≠ Finding Nuevo).

## 6. Tooling Audit

### 6.1 audit_validator.py — CONFIRMADO EJECUTADO, FAIL

Resultado real de `uv run python scripts/audit_validator.py`:

- 6x FAIL [M17]: findings en registro pero ausentes en informe (F-PL-06..F-PL-11)
- 8x WARN [M5]: referencias a archivos inexistentes (F-PL-01..F-PL-08), incluyendo
  `scripts/check_production_gates.py`, `codeql.yml`, `tracking.yaml` (paths rotos)
- 17x WARN [M17]: findings en informe pero ausentes en registro de producto
  (F-ARCH-01..06, F-CI-01..03, F-GOV-01..05, F-SC-01..02, F-SYS-01..11)
- **Veredicto del validador: FAIL — 11 error(es) mecánico(s)**

Recomendación: agregar hook local en `.pre-commit-config.yaml` (non-blocking al
inicio, luego blocking) y/o job en CI (`ocm-ci.yml`) equivalente a como
`lint-imports` gatea arquitectura.

Relación con Master Plan: candidato a P0 (Documentation Foundation) dado que
bloquea confiabilidad de todo el resto del sistema de auditoría.

Relación con Policy Layer: extiende el patrón ya usado por import-linter/BC-NN
al dominio documental — mismo principio, sin nueva abstracción.

Verificación: `grep -n "audit_validator" .pre-commit-config.yaml .github/workflows/*.yml`
(reproducible, 0 resultados al momento de esta auditoría).

### 6.2 Comparación entre auditorías de Policy Layer — CONTRADICCIÓN DETECTADA

Comando: `diff docs/audits/OCM_AUDIT_FINDINGS_2026-08-19-policy-layer.md \
  docs/audits/OCM_AUDIT_FINDINGS_2026-08-19-policy-layer-complementary.md`

Contexto normativo: ambos documentos evalúan la misma pregunta ("¿introducir
HashiCorp/OPA/Conftest/Semgrep/SonarQube?") bajo la arquitectura normativa
OCM Constitution (4 pilares → Policy Gate → CI → CD → OrangeHouse), pero
llegan a resúmenes de severidad distintos:

| | `-policy-layer.md` | `-policy-layer-complementary.md` |
|---|---|---|
| Resumen severidad | CRITICAL 1 · HIGH 3 · MEDIUM 4 · LOW 1 · total 9 | CRITICAL 0 · HIGH 1 · MEDIUM 7 · LOW 2 · total 10 |
| NUEVO | 4 (F-PLA-01, 02, 05, 09) | 8 (F-PLC-01..08) |
| REVALIDADO | 2 (F-PLA-04, 06) | 0 |
| CONTRADICCIÓN | 2 (F-PLA-03, 08) | 2 (F-PL-07, F-PL-08) |
| RECOMENDACIÓN | 1 (F-PLA-07) | 0 |

Findings clave de `-policy-layer.md`:
- F-PLA-01 (NUEVO): Ruff solo habilita E/F/I (no C901/PLR/SIM/DUP), lo que
  contradice la afirmación previa de que "SonarQube duplicaría a Ruff".
- F-PLA-02 (NUEVO): vulture instalado pero nunca ejecutado en CI/pre-commit.
- F-PLA-03 (CONTRADICCIÓN): CodeQL se ejecuta en PR, no solo semanalmente
  como afirmaba la auditoría previa.
- F-PLA-04 (REVALIDA F-PL-07): no introducir HashiCorp, sin necesidad
  demostrable.
- F-PLA-05 (NUEVO): `check_production_gates.py` ausente; extiende F-PL-04
  al Policy Gate completo.
- F-PLA-06 (REVALIDA F-PL-02): pip-audit reporta 4 vulnerabilidades activas.
- F-PLA-07 (RECOMENDACIÓN): Semgrep como non-blocking inicial (coste ~0).
- F-PLA-08 (CONTRADICCIÓN): SonarQube sí aportaría señal longitudinal de
  maintainability que Ruff/mypy/pytest no cubren hoy — pero el coste
  operacional en OrangeHouse lo mantiene NO JUSTIFICADO.
- F-PLA-09 (NUEVO): la cadena RULE→CI→EVIDENCE no está completa para
  ninguna regla (falta hash de evidencia + waiver + expiración + ownership).

Findings clave de `-policy-layer-complementary.md`:
- F-PLC-01 (CONTRADICE F-PL-07 / HashiCorp): la conclusión "no introducir"
  se mantiene, pero por razones operativas, no porque "single-host sea
  suficiente" — el argumento F2.6d no elimina la necesidad de secret
  rotation que Vault resolvería.
- F-PLC-02 (CONTRADICE F-PL-08 / Semgrep): Semgrep aporta valor marginal
  real para arquitectura/policy (imports prohibidos, APIs deprecated,
  os.environ, subprocess, crypto, logging de secretos) que ni import-linter
  (solo grafo estático), ni AST Guards (patrones a mano), ni CodeQL
  (triage costoso) cubren hoy.

**Impacto de la contradicción**: ambos documentos concluyen "no introducir
HashiCorp" por razones distintas, y ambos apuntan a Semgrep como
recomendación con valor marginal real, pero ninguno declara cuál prevalece
como versión vigente. Ver F-DOC-07 (6.3) para la causa raíz.

### 6.3 Revisión de F-DOC-04 — CONTRADICCIÓN CON HALLAZGO PROPIO

Comando: `diff docs/audits/OCM_AUDIT_FINDINGS_2026-08-19-policy-layer-complementary.md \
  docs/audits/OCM_AUDIT_FINDINGS_2026-08-19-policy-complementary.md`

**Hallazgo original (F-DOC-04, sección 5) descartado por evidencia.**
No son duplicados: contienen IDs de findings distintos (`F-PLA-*` vs
`F-PLC-*`), conteos distintos (4 NUEVO/2 REVALIDADO/2 CONTRADICCIÓN/1
RECOMENDACIÓN vs 8 NUEVO/0 REVALIDADO/2 CONTRADICCIÓN/0 RECOMENDACIÓN), y
contenido técnico divergente sobre Semgrep y SonarQube.

**F-DOC-07 (NUEVO, reemplaza a F-DOC-04):**
- Severidad: MEDIA
- Clasificación: NUEVO
- Evidencia: `-policy-complementary.md`, `-policy-layer-complementary.md` y
  `-policy-layer.md` (mismo día, 2026-08-19) son tres auditorías de policy
  layer con conclusiones que se contradicen entre sí sobre Semgrep/SonarQube,
  sin mecanismo de reconciliación ni documento que declare cuál prevalece.
- Impacto: un agente IA (o Solano) que consulte solo uno de los tres
  documentos recibe una recomendación potencialmente contradicha por otro
  documento del mismo día, sin señal de cuál es la vigente.
- Causa: ausencia de convención de naming + ausencia de manifest/índice para
  `docs/audits/` (mismo mecanismo que ya gobierna `docs/knowledge/`).
- Recomendación:
  1. Naming por convención con sufijo de sesión/orden explícito, no
     sinónimos (`-complementary` / `-layer-complementary` / `-layer` no
     comunican orden ni autoridad).
  2. Cada documento debe declarar explícitamente su estado: `VIGENTE`,
     `SUPERSEDED-BY: <archivo>`, o `PROPUESTA-NO-CONSOLIDADA`.
  3. Si hay más de un intento sobre el mismo tema el mismo día, consolidar
     en uno solo antes de cerrar la sesión, o declarar cuál prevalece.
- Verificación: `diff` reproducible entre los tres pares de archivos.
## 7. Decision Matrix (preliminar)
| Candidato | Problema que resuelve | Veredicto |
|---|---|---|
| Wiring `audit_validator.py` a pre-commit/CI | Enforcement del validador ya existente | ADOPT |
| `lychee` (link checker) | Referencias rotas fuera del scope de findings (M5 es findings-only) | ADOPT NON-BLOCKING (nightly) |
| Script de naming convention | F-DOC-03 | ADOPT (script propio, no herramienta externa) |
| Extender `manifest.yaml` pattern a `docs/audits/` | F-DOC-01, F-DOC-04, sprawl | ADOPT |
| MkDocs/Sphinx/Vale | — | REJECT (sin evidencia de necesidad; no-sobrediseño) |

## 8-20. [Pendientes — se completan en próximas sesiones]

## Matriz de Findings

| Finding | Severidad | Clasificación | Control |
|---|---|---|---|
| F-DOC-06 | HIGH | NUEVO | DOC-ENFORCEMENT |
| F-DOC-07 | MEDIUM | NUEVO | DOC-CANONICAL-REGISTER |

## Matriz de Controles

| Control | Descripción | Evidencia | Estado |
|---|---|---|---|
| DOC-ENFORCEMENT | Wiring de `audit_validator.py` a pre-commit/CI | F-DOC-06 | NO_VERIFICADO |
| DOC-CANONICAL-REGISTER | Registro canónico de auditorías documentales | F-DOC-07 | NO_VERIFICADO |

Controles = NO_VERIFICADO(2) = 2

## Matriz de Decisiones

| Candidato | Problema | Veredicto |
|---|---|---|
| Wiring `audit_validator.py` a pre-commit/CI | Enforcement documental | ADOPT |
| Manifest para `docs/audits/` | Canonicalidad y sprawl | ADOPT |
| Naming convention | Ambigüedad entre auditorías del mismo día | ADOPT |
| `lychee` | Links externos | ADOPT NON-BLOCKING |

## Comandos canónicos de herramientas

- DEPENDENCY_AUDIT: `uv run pip-audit . --ignore-vuln PYSEC-2026-113 --ignore-vuln PYSEC-2026-1325`
- YAMLLINT: `uvx yamllint .`

## Integridad

- Registro canónico: `docs/audits/OCM_AUDIT_FINDINGS_2026-08-19-documentation-tooling.md`
- Informe canónico: `docs/audits/AUDIT_OCM_DOCUMENTATION_TOOLING_2026-08-19.md`
- Validador: `scripts/audit_validator.py`
- Golden: `tests/architecture_linter/test_golden.py`
- Estado: VALIDACIÓN EN CURSO

## 21. Confirmaciones de proceso
- [x] NO se modificó código
- [x] NO se modificó CI
- [x] NO se modificó Master Plan
- [x] NO se modificó tracking.yaml
- [x] Solo se escribió este artefacto en `docs/audits/`

## 6.2 Enforcement gap — CONFIRMADO
Comando: `grep -n "audit_validator" .pre-commit-config.yaml .github/workflows/*.yml`
Resultado: sin coincidencias (0 matches).

**F-DOC-06 (NUEVO):**
- Severidad: ALTA
- Clasificación: NUEVO (no aparece en tracking.yaml ni en auditorías previas revisadas)
- Evidencia: `audit_validator.py` no está referenciado en `.pre-commit-config.yaml`
  ni en `.github/workflows/*.yml`. El validador solo corre manualmente.
- Impacto: los 11 errores mecánicos detectados (M5, M17) pueden acumularse
  indefinidamente sin bloquear merges ni commits. El propio sistema de auditoría
  (registro↔informe, referencias) queda sin gate — mismo patrón de riesgo que
  motivó ARCH-006/import-linter para código, pero aquí no existe para docs.
- Causa: falta de wiring, no falta de herramienta.
- Recomendación: agregar hook local en `.pre-commit-config.yaml` (non-blocking al
  inicio, luego blocking) y/o job en CI (`ocm-ci.yml`) equivalente a como
  `lint-imports` gatea arquitectura.
- Relación con Master Plan: candidato a P0 (Documentation Foundation) dado que
  bloquea confiabilidad de todo el resto del sistema de auditoría.
- Relación con Policy Layer: extiende el patrón ya usado por import-linter/BC-NN
  al dominio documental — mismo principio, sin nueva abstracción.
- Verificación: `grep -n "audit_validator" .pre-commit-config.yaml .github/workflows/*.yml`
  (reproducible, 0 resultados al momento de esta auditoría).

## 6.3 Revisión de F-DOC-04 — CONTRADICCIÓN CON HALLAZGO PROPIO
Comando: `diff docs/audits/OCM_AUDIT_FINDINGS_2026-08-19-policy-layer-complementary.md
docs/audits/OCM_AUDIT_FINDINGS_2026-08-19-policy-complementary.md`

**Hallazgo original (F-DOC-04, sección 5) descartado por evidencia.**
No son duplicados: contienen IDs de findings distintos (`F-PLA-*` vs `F-PLC-*`),
conteos distintos (4 NUEVO/2 REVALIDADO/2 CONTRADICCIÓN/1 RECOMENDACIÓN vs
8 NUEVO/0 REVALIDADO/2 CONTRADICCIÓN/0 RECOMENDACIÓN), y contenido técnico
divergente sobre Semgrep y SonarQube.

**F-DOC-07 (NUEVO, reemplaza a F-DOC-04):**
- Severidad: MEDIA
- Clasificación: NUEVO
- Evidencia: `-policy-complementary.md`, `-policy-layer-complementary.md` y
  `-policy-layer.md` (mismo día, 2026-08-19) son tres auditorías de policy layer
  con conclusiones que **se contradicen entre sí** sobre Semgrep/SonarQube, sin
  un mecanismo de reconciliación ni un documento que declare cuál prevalece.
- Impacto: un agente IA (o Solano) que consulte solo uno de los tres documentos
  recibe una recomendación potencialmente contradicha por otro documento del
  mismo día, sin señal de cuál es la vigente.
- Causa: ausencia de convención de naming + ausencia de manifest/índice para
  `docs/audits/` (mismo mecanismo que ya gobierna `docs/knowledge/`).
- Recomendación: (1) nombrar por convención con sufijo de versión/sesión clara
  en vez de sinónimos (`-complementary` vs `-layer-complementary` vs `-layer`
  no comunican orden ni autoridad); (2) declarar explícitamente en cada
  documento cuál es el "vigente" si hay más de un intento sobre el mismo tema
  el mismo día — o consolidar en uno solo antes de cerrar la sesión.
- Verificación: `diff` reproducible entre los tres pares de archivos.
