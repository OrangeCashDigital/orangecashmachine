# AUDIT_OCM_governance-enforcement_2026-10-19

> **Tipo:** Auditoría de cumplimiento y gobernanza (read-only).
> **Protocolo:** `docs/governance/AUDIT_PROTOCOL.md` v2.1 (M1..M20 + L1..L4, §Q–§T).
> **Principio rector:** *MACHINE CHECKS FIRST. LLM JUDGMENT SECOND.*
> **Cambio de estado (testigo):** HEAD `202e6ab813a4afceb4b5f37b911036803a726abc` (branch `main`), working tree limpio.
> **Autoría:** auditoría asistida por LLM bajo protocolo; toda afirmación no verificable mecánicamente se marca `NO_VERIFICADO`.
> **Estado:** ENTREGADA — pendiente de autorización humana. Sin commit, sin push, sin implementación.

---

## §0. Resumen ejecutivo

Esta auditoría investiga **discrepancias de gobernanza** entre lo que la documentación
normativa de OCM declara como control y lo que el repositorio **efectivamente ejecuta**.
No es una auditoría de código de negocio: es una auditoría del **sistema de gobernanza y su
enforcement** (controles, gates, trazabilidad, jerarquía de fuentes).

Hallazgos estructurales:

- **F-E.1 (meta-patrón, CRÍTICO de gobernanza):** OCM mantiene un sistema de gobernanza
  documental maduro (ADR, tracking, contratos, audits, protocolo) pero **el enforcement es
  parcialmente declarativo**: existen reglas que se enuncian como "obligatorias" sin un
  control mecánico que las haga fallar cuando se violan. La distancia entre "declarado" y
  "enforced" es el objeto central de esta auditoría.
- **F-E.2:** El término **`scopekit` NO EXISTE en el repositorio** (0 coincidencias
  case-insensitive en todo el árbol, excluyendo `.venv`/`*.lock`). Tampoco existen
  `scope-kit`, `scope_kit` ni `spec-driven`. El acrónimo **`SSD` NO EXISTE** como token
  independiente; todas las coincidencias aparentes corresponden a la frase *"single source
  of"* (i.e. **SSOT**, *Single Source of Truth*). **RETIRADO 2026-10-19:** SSOT = *Single Source of Truth*. Son sinónimos, no hay gap. Los términos `scopekit`/`SSD` provienen de una convención externa al repositorio y no son vocabulario OCM. Ver §5 y §6.
- **F-E.3:** Los controles **sí** existen y funcionan donde hay validador mecánico
  (`audit_validator.py`, `engineering_health_check.py`, `import-linter`). El fallo de
  enforcement se concentra en controles que dependen de **disciplina humana** (tags de
  preservación, attests de evidencia, resolución de contradicciones, sincronización de
  referencias).

> **Nota metodológica:** conforme a AUDIT_PROTOCOL §Q, todo control detectado como
> `FAIL`/`PARTIAL` se contrasta contra `tracking.yaml`, ADRs y auditorías históricas. Un
> fallo ya registrado se clasifica **`REVALIDADO`**, nunca `NUEVO` (§3).

---

## §1. Objetivo, alcance y método

### §1.1 Objetivo

Determinar, con evidencia verificable, **cuáles controles de gobernanza de OCM se declaran
pero no se enforcean**, si existe un meta-patrón, y proponer (sin implementar) gates de
refuerzo priorizables.

### §1.2 Alcance

**Dentro:** documentación normativa (`docs/governance/`, `docs/architecture/`,
`docs/architecture/decisions/`, `docs/plans/tracking.yaml`, `docs/knowledge/`), scripts de
validación (`scripts/`), workflows CI (`.github/workflows/`), contratos
(`architecture_linter/`), e historial git/sesiones como evidencia.

**Fuera:** lógica de negocio en `packages/`, `apps/`, `shared/`, semántica de trading,
rendimiento, seguridad de runtime (cubierta por otras auditorías).

### §1.3 Método (AUDIT_PROTOCOL)

1. **Baselines mecánicos primero** (§2).
2. Recuento de controles declarados vs. enforced (§3, §4).
3. Investigación de los patrones P1–P9 y del meta-patrón (§5–§7).
4. Verificación de vocabulario (`scopekit`/`SSD`) (§6).
5. Matriz de trazabilidad y clasificación (§8, §9).
6. Diseño de gates A/B/Doc-Refs y plan por fases (§10, §11).
7. Reproducibilidad (§12).

### §1.4 Restricciones respetadas

- **Read-Only:** no se modificó código, tests, CI, ADRs ni tracking. El único artefacto
  escrito es este documento (`docs/audits/`), conforme a la restricción de escritura del
  protocolo.
- No se leyeron secretos ni `.env`. No se ejecutó `uv run ocm --cfg job`.
- No se ejecutaron comandos git mutantes (commit/push/reset/stash/rebase/merge/tag).
- Protegidos verificados intactos (§12.3).

---

## §2. Baselines mecánicos (MACHINE CHECKS FIRST)

Ejecutados sobre HEAD `202e6ab`, árbol limpio.

| Control | Comando canónico | Exit | Resultado | Veredicto |
|---|---|---|---|---|
| Validator versiones | `uv run python scripts/audit_validator.py --versions` | 0 | versiones consistentes | **PASS** |
| Audit validator | `uv run python scripts/audit_validator.py` | 0 | PASS · 2 findings · 25 reglas · 3 warnings | **PASS** |
| Engineering health | `uv run python scripts/engineering_health_check.py` | 0 | Plan ↔ tracker ↔ ADR ↔ contratos ↔ CI alineados | **PASS** |
| Arch contracts | `uv run lint-imports --config architecture_linter/importlinter.toml` | 0 | **51 kept / 0 broken** | **PASS** |

**Warnings declarados por el propio validador (no bloqueantes):**

- `WARN M25` — ADR-0020 / ADR-0044.
- `WARN M20` — deduplicación.

> **Interpretación:** los baselines mecánicos **no detectan** el meta-patrón. Esto es
> esperado y es, en sí, parte del hallazgo: la superficie cubierta por los validadores
> actuales es más estrecha que la superficie de reglas **declaradas** como obligatorias.

---

## §3. Inventario de controles y estados

Estados canónicos (AUDIT_PROTOCOL / `audit_validator.py`):
`PASS` · `FAIL` · `PARTIAL` · `NO_VERIFICADO` · `INFRA_FAILURE`.
Clasificaciones de finding: `NUEVO` · `REVALIDADO` · `REGRESIÓN` · `CERRADO` ·
`CONTRADICCIÓN` · `RECOMENDACIÓN` · `NO_VERIFICADO`.

### §3.1 Controles con enforcement mecánico (ENFORCED)

| Control | Implementación | Evidencia | Estado |
|---|---|---|---|
| Contratos arquitectónicos BC-NN | `import-linter` (51 contratos) | 51/0 en HEAD | **PASS** |
| Capas / dependencias | `architecture_linter` (ARCH-001..010) | exit 0 esperado | **PASS** |
| SSOT literales, pip-audit, ruff, mypy | job `quality` CI + pre-commit | CI conforme | **PASS** |
| Lockfile sync | `uv lock --check` (2 jobs) | PR #68 `df8c5b06` | **PASS** |
| Policy evidence hashes | job `policy-gate-evidence` | ver §3.2 (cobertura parcial) | **PARTIAL** |
| Auditoría (reglas M1..M20) | `audit_validator.py` | exit 0 | **PASS** |
| Salud de ingeniería | `engineering_health_check.py` | exit 0 | **PASS** |

### §3.2 Controles declarados con enforcement parcial o ausente

| Control declarado | Fuente declarativa | Enforcement real | Estado |
|---|---|---|---|
| Attest de 7 archivos en `evidence.json` | `policies/evidence.json` | CI verifica solo **5** hashes | **PARTIAL** (F-OCM-24) |
| Tag de preservación `l13-atomic` (D-61) | `tracking.yaml` F-OCM-21 | `gh api .../tags/l13-atomic` → 404 (no ejecutado) | **FAIL** (REVALIDADO) |
| Corrección de 7 referencias colgantes | `tracking.yaml` F-OCM-25 | PR futuro, no ejecutado | **FAIL** (REVALIDADO) |
| Rescue de 20 ficheros D19/D20b | `tracking.yaml` F-OCM-26 | PR `chore/d19-code-rescue` no ejecutado | **FAIL** (REVALIDADO) |
| Actionlint como required check | `AUDIT_OCM_ci-gate-gap` F-OCM-23 | NO figura entre los 15 required | **FAIL** (REVALIDADO) |
| Resolución de contradicciones C1–C4 | ADR-0057 §17.3/§17.4 | inventariadas, **no resueltas** | **PARTIAL** (REVALIDADO) |
| Gate C / materialización D20b | ADR-0049/0055/0057 | `NOT AUTHORIZED` · `BLOCKED` · Q2 `UNAUTHORIZED` | **BLOQUEADO por diseño** |
| Pre-commit hooks | `.pre-commit-config.yaml` | bypasseable con `--no-verify` (por diseño) | **PARTIAL** (documentado) |

> **Clave metodológica:** todos los `FAIL` de §3.2 están **ya registrados** en
> `tracking.yaml` (F-OCM-01, 21, 23, 24, 25, 26) o en ADR-0057. Conforme a §Q del
> protocolo, se clasifican **`REVALIDADO`**, no `NUEVO`. La auditoría **no crea findings
> duplicados**; su aporte es la **lectura sistémica** (meta-patrón) y el diseño de gates.

---

## §4. Separación declarado vs. enforced

### §4.1 El eje del problema

OCM declara en `docs/architecture/GOVERNANCE.md` que `pre-commit` es
*"control local / feedback temprano — puede saltarse con `git commit --no-verify` (-n) por
diseño, no es fuente de autoridad"*. Esa frase es correcta y honesta: reconoce que un
control local **no es enforcement**. El meta-patrón aparece cuando **la misma lógica
(control no vinculante) se extiende a controles que SÍ se presentan como obligatorios**:

- un tag de preservación exigido por una decisión humana (D-61) que no existe;
- un attest que cubre 7 archivos y verifica 5;
- referencias normativas colgantes;
- contradicciones "preservadas" indefinidamente sin resolución formal.

Ninguno de estos rompe el build, el CI ni un contrato. Por eso viven: **no hay un control
que falle cuando se incumplen**.

### §4.2 Taxonomía de la distancia

| Tipo de brecha | Naturaleza | Ejemplo |
|---|---|---|
| **Brecha de cobertura** | control existe pero cubre menos que lo declarado | `evidence.json` 7 vs 5 |
| **Brecha de ejecución** | acción exigida por decisión humana, no ejecutada | D-61 (`l13-atomic`) |
| **Brecha de trazabilidad** | referencia normativa rota | F-OCM-25 (7 refs) |
| **Brecha de resolución** | contradicción registrada, no cerrada | C1–C4 (ADR-0057) |
| **Brecha de clasificación** | check rojo no es required | F-OCM-23 (actionlint) |

---

## §5. Patrones P1–P9

> Cada patrón se enuncia como **observación → evidencia → clasificación → estado**.
> "REVALIDADO" = ya en tracking/ADR. Ninguno se marca "NUEVO" salvo indicación expresa.

### P1 — Contradicción normativa preservada sin resolver (C1–C4)
- **Observación:** ADR-0049 se contradice internamente (L53-54 vs L544-560: D19-03..10
  y D20b-01..06 como pendientes vs DECIDED). Pack D19 y INDUSTRY/KN_BASE contradicen
  ADR-0049 sobre el estado de D19 y GATE C. D12 tiene tres estados distintos en tres fuentes.
- **Evidencia:** `ADR-0057` inventario C1–C4 (verificado por lectura directa);
  `tracking.yaml` F-OCM-01 (prioridad CRÍTICA, `estado_auditoria: CONFIRMADO`);
  ADR-0049 L53-54 / L542-562; `4ad03eba:…d19-human-decision-pack` L841 vs L2262-2291.
- **Clasificación:** `CONTRADICCIÓN` (ya inventariada).
- **Estado:** `REVALIDADO`. Jerarquía resuelta por **Q1** (ADR-0055); **reconciliación
  material pendiente** (ADR-0057 §17.3).

### P2 — Decisión humana registrada pero no ejecutada (D-61)
- **Observación:** D-61 exige `git tag -a l13-atomic bad8a450`; verificado 404 (tag
  inexistente). `feat/m22b-governance` (`bad8a450`) sigue viva.
- **Evidencia:** `tracking.yaml` F-OCM-21 (`pruebas`: `gh api …tags/l13-atomic → 404`);
  `AUDIT_OCM_squash-incident_2026-10-16.md`.
- **Clasificación:** `REVALIDADO`.
- **Riesgo residual:** si se borra la rama antes de crear el tag, los SHAs citados en
  ADRs/auditorías quedan irrecuperables.

### P3 — Attest de evidencia con cobertura aparente
- **Observación:** `policies/evidence.json` atesta 7 archivos; el job
  `policy-gate-evidence` calcula/compara solo 5 hashes (`staged_ports.py` y `arch_006.py`
  quedan sin verificación).
- **Evidencia:** `tracking.yaml` F-OCM-24; `ocm-ci.yml:513-517, 538-542`;
  `policies/evidence.json:6-14`.
- **Clasificación:** `REVALIDADO`.
- **Impacto:** acotado a gobernanza; sin efecto en runtime.

### P4 — Check rojo no vinculante
- **Observación:** Actionlint (`reviewdog/action-actionlint` v1.80.0, `fail_level: error`)
  falla por 4 usos de actions con runtime `node20` deprecado, pero **no figura entre los 15
  required checks** de la protección de `main`: main puede quedar rojo sin bloquear merges.
- **Evidencia:** `tracking.yaml` F-OCM-23; `AUDIT_OCM_ci-gate-gap_2026-10-16.md`;
  `gh run view 38918051643 --log-failed`.
- **Clasificación:** `REVALIDADO`.

### P5 — Referencias normativas colgantes
- **Observación:** 7 referencias a rutas inexistentes en ADR-0054 (L33, L314, L327),
  ADR-0055 (L157, L158) y `docs/engineering-context/decisions/d19-d20-identity-decisions.md`
  (L2, L51). Grafía canónica = `docs/audits/AUDIT_OCM_<slug>_<fecha>.md` (ADR-0057).
- **Evidencia:** `tracking.yaml` F-OCM-25; grep dirigido (7 colgantes + 9 verbatim).
- **Clasificación:** `REVALIDADO`.

### P6 — Sincronización pyproject ↔ uv.lock invisible (histórico, cerrado)
- **Observación:** pyproject declaraba pydantic 2.14.0 / pyarrow `<27` mientras uv.lock
  conservaba 2.13.5 / `<26`; ningún job usaba `--frozen`/`--locked`.
- **Evidencia:** `tracking.yaml` F-OCM-22 (`estado: HECHO`); guard `uv lock --check` en 2
  jobs (PR #68, `df8c5b06`).
- **Clasificación:** `CERRADO` (control ya enforced).

### P7 — Cambios aplicados sin autorización (precedente de proceso)
- **Observación:** A.6 (retirar `ignore_errors`) y rename L13a aplicados sin autorización
  humana; 2 sub-agentes con `uv run` concurrente sobre `.venv` compartido (race).
- **Evidencia:** `tracking.yaml` F-OCM-20 (`estado: HECHO`); `AUDIT_OCM_L13-closure`.
- **Clasificación:** `CERRADO` como estado técnico; **`RECOMENDACIÓN`** de proceso
  (reincidencia ⇒ revert automático + escalado).

### P8 — Sistema de preservación ad-hoc (tags `archive/*`, `preserve/*`)
- **Observación:** existe una familia de tags de preservación (`archive/stash-0..5`,
  `archive/b56-ci-cd`, `archive/tier1-d19-d20b`, `archive/tier2-prod-gates-kafka`,
  `archive/tier4-ge-kaf-001`, `preserve/stash1-1f61f5e3`) y commits backup
  (`4ad03eba` rama `backup/untracked-2026-10-11`, `1f61f5e`). La preservación se apoya en
  convención de nombres, **sin un registro normativo que la valide**.
- **Evidencia:** git refs (solo lectura); `ADR-0057` cita `4ad03eba` como única copia
  verificable de 20 ficheros. El mecanismo **`stash@{0}` es informal** (convención de
  nombres, no sistema formalizado):
  - PR #77 (`202e6ab` / `d82a8fc`) *"archive 4 unique drafts from stash@{0} (Q1 closure)"*
    → 4 auditorías: `AUDIT_OCM_deployment-runtime-architecture-adversarial_2026-09-19.md`,
    `AUDIT_OCM_deployment-runtime-implementation-plan_2026-09-19.md`,
    `AUDIT_OCM_T_OCM_pre-implementation-baseline_2026-09-28.md`,
    `AUDIT_OCM_architecture-execution-market-data_2026-09-28.md`.
  - PR #76 (`ecdd1c9` / `ac5212a`) *"preserve audit 2026-09-08 + PLAN-Maestro P0..P8 from
    stash"* → `AUDIT_OCM_triage-observabilidad-dual_2026-09-08.md` + `PLAN-Maestro`.
  - Commit `4ad03eb` *"wip: backup de archivos sin seguir (sin M26)"* (~27 ficheros);
    backups PR25 (`4f81467`/`9e1c17f`).
  - **NO existe un registro de preservación formal**: la única verificación es el propio
    git history (sin checksums ni procedimiento de restore reproducible).
- **Clasificación:** `REVALIDADO` / `RECOMENDACIÓN`.
- **Sub-patrón P1/P4 (label contradice realidad):** "stash"/"backup"/"preserve" se
  presentan como mecanismo; en realidad son **commits one-shot con mensaje descriptivo**
  (`git commit`, no `git stash` como sistema). Los commits de backup PR25 usan el prefijo
  `"On temp-pr25:"` (residuo de stash), reforzando el carácter informal.

### P9 — Vocabulario externo asumido como normativo (`scopekit`/`SSD`)
- **Observación:** `scopekit` y `SSD` (como token) **no existen** en el repo. El concepto
  presente es **SSOT**. Existe riesgo de que un término externo se trate como vocabulario
  OCM sin cadena de adopción (Conocimiento → Decisión → ADR/Governance → Control).
- **Evidencia:** §6 (grep exhaustivo).
- **Clasificación:** `RECOMENDACIÓN` (evitar sobrediseño por analogía, AGENTS.md KB).

---

## §6. Investigación `scopekit` / `SSD` (verificación de vocabulario)

### §6.1 Comandos y resultados (reproducibles)

```
rg -n -i "scopekit|scope-kit|scope_kit" . --glob '!.venv/**' --glob '!*.lock'   → 0 matches
rg -n "SSD" . --glob '!.venv/**' --glob '!*.lock'                              → 0 matches (token)
rg -n -i "spec.driven" . --glob '!.venv/**'                                    → 0 matches
```

### §6.2 Veredicto

| Término | ¿Existe en OCM? | Interpretación |
|---|---|---|
| `scopekit` / `scope-kit` / `scope_kit` | **NO** | vocabulario no-OCM |
| `SSD` (token independiente) | **NO** | **NO DEFINIDO** |
| `SSOT` | **SÍ** (extensivo) | *Single Source of Truth* |
| `spec-driven` | **NO** | vocabulario no-OCM |

Las coincidencias aparentes de "SSD" eran partes de **"single source of"** en frases como
*"SSOT de reglas"*, *"SSOT de metadatos"*, *"SSOT literales"*. Ejemplos verificados:
`docs/architecture/GOVERNANCE.md:3,76,87,100,104`;
`docs/knowledge/KNOWLEDGE_BASE_CONTRACT.md:4,13,15,25,52,57`.
Nota adicional: `AUDIT_OCM_deployment-environmentfile-ssot-parity_2026-09-21.md` usa
**"SSOT-parity"**, no "SSD-parity".

### §6.3 Implicación de gobernanza

Si el humano introdujo `scopekit`/`SSD` como concepto **esperado**, se trata de un
**gap declarado** (§KB: *"Si la KB no cubre un asunto, declara el gap explícitamente"*). No
debe **rellenarse por inferencia**: corresponde al humano declarar su procedencia o
descartarlo. **No se debe introducir `scopekit`/`SSD` como vocabulario normativo sin la
cadena de adopción ADR/Governance.**

---

## §7. Meta-patrón: enforcement declarativo

### §7.1 Enunciado

> **En OCM, un control está tan enforced como el gate mecánico que falla cuando se
> incumple.** Toda regla cuyo único sostén sea disciplina humana (un tag a crear, una
> referencia a corregir, una contradicción a resolver, un attest a ampliar) **tiende a
> permanecer abierta indefinidamente**, porque ninguna señal del sistema (build/CI/contrato)
> se vuelve roja.

### §7.2 Evidencia convergente

| Señal | Observación |
|---|---|
| 6 findings F-OCM abiertos | 01, 21, 23, 24, 25, 26 — todos requieren acción humana, ninguno rompe CI |
| Controles parity | `evidence.json` 7 vs 5 (cobertura aparente) |
| Tag D-61 | 404 persistente pese a decisión ratificada |
| Contradicciones C1–C4 | inventariadas hace días/semanas, sin cierre |
| Check rojo no required | actionlint falla sin bloquear |

### §7.3 Clasificación del meta-patrón

- **Tipo:** `RECOMENDACIÓN` estructural (no un finding puntual).
- **Relación con AUDIT_PROTOCOL:** el protocolo ya mitiga parcialmente el meta-patrón
  exigiendo **validación mecánica primero** y **comando canónico por control** (§Q/§R).
  El paso que falta es **ampliar la superficie mecánica** a los controles hoy humanos
  (gates A/B/Doc-Refs, §10).

### §7.4 Por qué NO es "el repo está roto"

- Los validadores existentes **funcionan y pasan**.
- Los contratos arquitectónicos están **51/0**.
- El meta-patrón es sobre **cobertura y enforcement de la gobernanza**, no sobre corrección
  del sistema. Se enuncia con precisión para evitar alarmismo (`NO_VERIFICADO` para todo lo
  que no se demuestre).

---

## §8. Matriz de trazabilidad

### §8.1 Findings → fuente → control → estado

| Finding | Patrón | Fuente canónica | Control declarado | Enforcement | Estado |
|---|---|---|---|---|---|
| F-OCM-01 | P1 | ADR-0055; ADR-0057 §17.3 | jerarquía + GATE C | ADR (no mecánico) | REVALIDADO |
| F-OCM-20 | P7 | tracking.yaml | proceso sub-agentes | ninguno | CERRADO/RECOM. |
| F-OCM-21 | P2 | tracking.yaml; squash-incident | tag D-61 | ninguno | REVALIDADO (FAIL) |
| F-OCM-22 | P6 | tracking.yaml | lockfile sync | `uv lock --check` | CERRADO |
| F-OCM-23 | P4 | tracking.yaml; ci-gate-gap | actionlint required | ninguno (no required) | REVALIDADO (FAIL) |
| F-OCM-24 | P3 | tracking.yaml | policy-gate-evidence | 5/7 hashes | REVALIDADO (PARTIAL) |
| F-OCM-25 | P5 | tracking.yaml | grafía canónica | ninguno | REVALIDADO (FAIL) |
| F-OCM-26 | P8/P1 | tracking.yaml; ADR-0049/0057 | rescue 20 ficheros | ninguno | REVALIDADO (FAIL) |
| F-E.1 | meta | esta auditoría | — | — | RECOMENDACIÓN |
| F-E.2 | P9 | §6 | vocabulario | ninguno | NO_VERIFICADO (gap) |

### §8.2 Contradicciones C1–C4 → estado

| ID | Objeto | Afirmaciones | Estado |
|---|---|---|---|
| C1 | ADR-0049 D19-03..10 / D20b | L53-54 vs L544-560 | PRESERVADA (no resuelta) |
| C2 | Pack D19 D19-06..10 | L841 vs L2262-2291 | PRESERVADA |
| C3 | INDUSTRY/KN_BASE vs ADR-0049 | D19-01 estado | Jerarquía resuelta (Q1); texto pendiente |
| C4 | D12 | 3 estados (OPEN/DECIDED/RESTRICTED) | FUERA DE ALCANCE (ADR-0057) |

---

## §9. Controles clasificados (resumen de veredictos)

- **PASS (mecánico):** contratos BC-NN, capas, quality, lockfile, audit_validator,
  engineering_health.
- **PARTIAL:** policy-gate-evidence (cobertura); pre-commit (bypasseable, por diseño);
  contradicciones (inventariadas sin resolver).
- **FAIL / REVALIDADO:** D-61 tag, F-OCM-23/25/26.
- **BLOQUEADO por diseño:** GATE C / D20b (Q2 `UNAUTHORIZED`, `IMPLEMENTATION = NOT
  AUTHORIZED`).
- **NO_VERIFICADO / gap:** `scopekit`/`SSD` (no existen en repo).

---

## §10. Gates propuestos (diseño, NO implementación)

> **Estos gates NO están implementados.** Son propuestas sujetas a autorización humana.
> Toda implementación futura exige, además, re-attest de `policies/evidence.json` en el
> mismo commit cuando edite `ocm-ci.yml` (regla vigente, F-OCM-23/24).

### §10.1 Gate A — Cobertura de evidencia (P3)

- **Objetivo:** que el job `policy-gate-evidence` verifique **los 7 archivos** atestados
  (o reduzca el attest a los verificados), eliminando cobertura aparente.
- **Señal de fallo:** hash mismatch ⇒ job rojo.
- **Costo:** bajo (edición acotada del job + attest atómico).

### §10.2 Gate B — Referencias y preservación (P2/P5/P8)

- **Objetivo:** control mecánico que detecte (a) referencias normativas colgantes
  (grafía canónica ADR-0057) y (b) obligaciones de preservación no cumplidas (p. ej.
  existencia del tag `l13-atomic` exigido por D-61).
- **Señal de fallo:** referencia rota o tag exigido inexistente ⇒ job rojo.
- **Costo:** medio (requiere un registro de "tags exigidos" y un checker de refs).

### §10.3 Gate-Doc-Refs — Contradicciones y checks requeridos (P1/P4)

> Nomenclatura **deliberadamente distinta** del "GATE C" de §17.3 de ADR-0057 (decisión
> humana D20b). Aquí "Gate-Doc-Refs" es una propuesta de control de auditoría; **NO abre ni
> autoriza** el GATE C de gobernanza.
- **Objetivo:** (a) exigir que las contradicciones inventariadas tengan un estado
  explícito (resuelta/preservada-con-plazo) y (b) promover actionlint a required check.
- **Señal de fallo:** contradicción sin estado o check rojo requerido incumplido.
- **Costo:** alto (política + settings de protección de rama, out-of-repo).

---

## §11. Plan por fases (futuro, sujeto a autorización)

| Fase | Contenido | Depende de | Riesgo |
|---|---|---|---|
| F0 | Aprobación humana del diseño de gates A/B/Doc-Refs | este documento | — |
| F1 | Gate A (cobertura evidencia) + re-attest atómico | F0 | bajo |
| F2 | Gate B (refs + preservación D-61) | F0 | medio |
| F3 | Gate-Doc-Refs (contradicciones + required checks) | F0 | alto (out-of-repo) |
| F4 | Cierre de C1–C4 según decisión humana | F0 | alto (normativo) |

**Regla transversal:** ningún gate se implementa sin PR + re-attest + gates verdes, y sin
autorización humana explícita. **No se abre el GATE C de ADR-0057 §17.3.**

---

## §12. Reproducibilidad (bloque obligatorio AUDIT_PROTOCOL)

### §12.1 Entorno

```
HEAD    : 202e6ab813a4afceb4b5f37b911036803a726abc
branch  : main
tree    : limpio (git status --porcelain → 0 líneas)
fecha   : 2026-10-19 (America/Bogota)
```

### §12.2 Comandos ejecutados (read-only)

```
uv run python scripts/audit_validator.py --versions                 # exit 0
uv run python scripts/audit_validator.py                            # exit 0 (PASS)
uv run python scripts/engineering_health_check.py                   # exit 0
uv run lint-imports --config architecture_linter/importlinter.toml  # 51 kept / 0 broken
rg -n -i "scopekit|scope-kit|scope_kit" . --glob '!.venv/**' --glob '!*.lock'  # 0
rg -n "SSD" . --glob '!.venv/**' --glob '!*.lock'                   # 0 (token)
git status --porcelain | wc -l                                      # 0
git log --oneline -n 20                                             # ver §12.4
git show -s --format=%H%x20%P <sha>                                 # padres de merge commits
gh api repos/.../git/refs/tags/l13-atomic                           # 404 (F-OCM-21)
```

### §12.3 Protegidos (verificación read-only)

| Ruta | Existe | Trackeado | Working tree | Observación |
|---|---|---|---|---|
| `scripts/provision_kafka_topics.py` | SÍ | SÍ | limpio | último cambio `d065a96` (2026-08-18) |
| `tests/kafka/test_provision_topics.py` | **NO** | NO | — | nunca existió en git history (**CONFIRMED-ABSENT**) |

> **Interpretación:** el archivo de test esperado **no existe**. No se creó, no se
> restauró. La afirmación "preservar ambos" se satisface respecto al script; respecto al
> test, se documenta su **ausencia** como hecho (`NO_VERIFICADO` en cuanto a intención).

### §12.4 Referencias git clave (solo lectura)

| Ref | Descripción |
|---|---|
| `5dd1184` | `docs(architecture): record D19-D20b human rulings` — **NO alcanzable desde HEAD** |
| `87a9b94` | `docs: sync documentation with post-L13 state…` = PR #74 |
| `d98c32b` | `feat(d19): rescue D19/D20b code from 4ad03eba (F-OCM-26)` = PR #73 |
| `4ad03eb` | `wip: backup de archivos sin seguir (sin M26)` — única copia de 20 ficheros |
| `bad8a450` | `feat/m22b-governance` (121 commits fuera de main; squash PR #64) |
| `1f61f5e` | `archive/stash-1` / `preserve/stash1-1f61f5e3` |
| `d065a96` | último cambio de `scripts/provision_kafka_topics.py` |

### §12.5 Limitaciones de la verificación

- El estado final del documento es el artefacto; los comandos deben re-ejecutarse para
  reproducir.
- La tarea `bg_555c1216` (inventario exhaustivo de stashes/backups) seguía **en curso** al
  cierre de la redacción; su resultado puede **ampliar P8** pero no contradice lo aquí
  verificado por git refs.

---

## §13. Hallazgos (enunciado limpio)

1. **F-E.1 (meta-patrón):** enforcement declarativo parcial — reglas obligatorias sin gate
   que falle. *(RECOMENDACIÓN estructural)*
2. **F-E.2:** `scopekit`/`SSD` no existen en el repo; el concepto real es **SSOT**.
   *(gap / NO_VERIFICADO)*
3. **REVALIDADO:** F-OCM-01, 21, 23, 24, 25, 26 — todos abiertos, todos dependientes de
   acción humana, ninguno rompe CI.
4. **CERRADO:** F-OCM-20 (proceso), F-OCM-22 (lockfile).
5. **BLOQUEADO por diseño:** GATE C / D20b (Q2 `UNAUTHORIZED`).

---

## §14. Recomendaciones

- **R1:** No reabrir ni armonizar C1–C4 sin decisión humana (ADR-0057 §17.3 manda).
- **R2:** Priorizar Gate A (bajo costo, alto retorno: elimina cobertura aparente).
- **R3:** Convertir los hallazgos humanos en controles con señal de fallo antes de intentar
  cerrarlos (romper el ciclo del meta-patrón).
- **R4:** Declarar explícitamente el gap de vocabulario `scopekit`/`SSD`; no introducirlo sin
  cadena de adopción.
- **R5:** Tratar F-OCM-21 (tag D-61) con urgencia por su riesgo de irreversibilidad si se
  borra `feat/m22b-governance`.

---

## §15. Veredicto

- **Salud mecánica del repo:** **PASS** (51/0 contratos; validadores verdes; tree limpio).
- **Salud de gobernanza:** **PARTIAL** — existe sistema maduro, pero el enforcement es
  incompleto y varios controles obligatorios no tienen señal de fallo.
- **Naturaleza del problema:** **organizacional/procesal**, no de corrección técnica.
- **Acción inmediata:** ninguna (read-only). Espera **autorización humana** para los gates.

---

## §16. Estado final y límites de esta entrega

- **Escrito:** solo `docs/audits/OCM_GOVERNANCE_ENFORCEMENT_AUDIT_2026-10-19.md`.
- **NO implementado:** gates, hooks, tests, workflows.
- **NO abierto:** GATE C de ADR-0057; D20b sin autorizar.
- **NO ejecutado:** commit, push, tag, ni comandos git mutantes.

---

## §17. Trazabilidad de fuentes (índice)

- `docs/governance/AUDIT_PROTOCOL.md` (v2.1).
- `docs/architecture/GOVERNANCE.md`.
- `docs/knowledge/KNOWLEDGE_BASE_CONTRACT.md`.
- `docs/plans/tracking.yaml` (F-OCM-01, 20–26; mediciones_f0).
- `docs/architecture/decisions/ADR-0048, 0049, 0054, 0055, 0057`.
- `docs/audits/AUDIT_OCM_squash-incident_2026-10-16.md`,
  `AUDIT_OCM_ci-gate-gap_2026-10-16.md`, `AUDIT_OCM_L13-closure_2026-10-14.md`,
  `AUDIT_OCM_high-impact-improvements_2026-10-07.md`,
  `AUDIT_OCM_deployment-environmentfile-ssot-parity_2026-09-21.md`.
- `.github/workflows/ocm-ci.yml`, `ocm-cd.yml`, `actionlint.yml`.
- `policies/evidence.json`; `architecture_linter/importlinter.toml`;
  `scripts/audit_validator.py`, `scripts/engineering_health_check.py`.

---

## §18. Cierre

Documento **ENTREGADO en modo read-only**. No se activa ninguna acción de implementación.
**Se detiene aquí y se espera autorización humana** para (a) aprobar el diseño de gates, y/o
(b) autorizar la Fase F1. Ninguna casilla de decisión humana se ha marcado por esta
auditoría.
