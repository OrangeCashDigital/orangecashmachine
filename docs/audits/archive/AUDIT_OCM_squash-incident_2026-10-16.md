# AUDIT OCM — Squash incident PR #64 (L13) + desincronización `uv.lock` en `main`

- **Fecha de ejecución:** 2026-10-16
- **Protocolo:** `docs/governance/AUDIT_PROTOCOL.md` v2.1 (§Q–§T: MACHINE CHECKS FIRST)
- **Régimen:** read-only (AUDIT_PROTOCOL §K). Única escritura permitida: este informe en `docs/audits/`.
- **Tipo:** forense de integración Git (merge strategy) + higiene de lockfile
- **Finding cards:** F-OCM-21 (NUEVO) · F-OCM-22 (NUEVO)
- **Relacionado:** F-OCM-23 (NUEVO · MEDIA-ALTA) → `AUDIT_OCM_ci-gate-gap_2026-10-16.md`

---

## 0. Baseline (AUDIT_PROTOCOL §D)

| Campo | Valor |
|---|---|
| Commit local (`git rev-parse HEAD`) | `0ce216d72a39d6fb7f7c3687cb548c75dcf7ca73` (`0ce216d7`) — **baseline actualizado** (era `680ddf53` al abrir el informe) |
| Branch actual | `main` (sincronizada: `git fetch --prune` + `git reset --hard origin/main` ejecutados por el humano) |
| Working tree (`git status --short`) | solo este informe (untracked); 0 modificaciones tracked |
| `origin/main` remoto real | `0ce216d7` — **remoto == local** (divergencia resuelta) |
| Divergencia local/remoto | ninguna |
| Topología de `main` | `0ce216d7` (#66 pyarrow, solo pyproject) ← `fbc022b4` (#65 pydantic, solo pyproject) ← `680ddf53` (#64 squash) ← `09a44a9d` (#58) |
| Baseline documental | `AUDIT_OCM_L13-closure_2026-10-14.md`, `AUDIT_OCM_l13a-closure_2026-10-12.md`, `docs/plans/tracking.yaml` (último ID: F-OCM-20) |

Subcomandos git de fetch/rev-list/branch/stash/reflog **denegados al agente** por política del
entorno; se sustituyen por `git log`, `git show`, `git rev-parse` y `gh api` (GitHub REST), que
permiten verificar la punta remota sin escritura.

---

## 1. Executive Summary

El PR #64 (integración de L13 a `main`) fue mergeado con **SQUASH**, no con merge commit.
Consecuencia: **121 commits atómicos** de la rama `feat/m22b-governance` **no son ancestros de
`main`** y los SHAs citados como evidencia en auditorías y en `policies/evidence.json` dejan de
ser alcanzables desde `main`.

No hay pérdida de contenido: `tree(680ddf53) == tree(bad8a450) == 59b7acab` y `git diff` entre
ambos es vacío. El impacto es de **trazabilidad/ancestría**, no funcional.

La evidencia atómica sigue viva en `origin/feat/m22b-governance` = `bad8a450`, por lo que el
incidente es **reversible sin reescribir `main`** (tag + no borrar la rama).

Durante la misma verificación se detectó un segundo problema **no relacionado**, re-confirmado
sobre el HEAD actual `0ce216d7`: `pyproject.toml` declara `pydantic==2.14.0` y
`pyarrow>=17.0,<27.0`, mientras `uv.lock` conserva `pydantic 2.13.5` y `pyarrow>=17.0,<26.0`
⇒ **dos desincronizaciones** invisibles al CI (ningún job
usa `--frozen`/`--locked`). Se registra como F-OCM-22.

---

## 2. Scope

**In:**

- Verificación forense del merge del PR #64 (`main` @ `680ddf53`): topología, conteo de commits
  colapsados, integridad de contenido.
- Preservación/recuperabilidad de la historia L13 (refs, tag propuesto).
- Higiene de integración: `pyproject.toml` ↔ `uv.lock` ↔ CI.
- Housekeeping asociado (stashes/ramas/refs stale) — inventario, sin ejecución.

**Out:**

- Cualquier remediación aplicada (git tag, `uv lock`, borrado de ramas/stashes, pydantic runtime
  tests). Todo remedio queda como Decisión Humana.
- F-OCM-15 (diferido), G1/D20b (bloqueado), M26 (`stash@{0}`).

---

## 3. Methodology (AUDIT_PROTOCOL §C)

1. **Tooling mecánico primero:** `uv run python scripts/audit_validator.py` → **PASS**
   (2 findings, 25 reglas, 3 warnings: M25 ADR-0020/0044 y M20).
2. Orden de descubrimiento: `AGENTS.md` → `AUDIT_PROTOCOL.md` → `PLAN-Maestro-Ingenieria.md` →
   `GOVERNANCE.md` → `tracking.yaml` → ADRs → `docs/audits/` → CI (`.github/workflows/ocm-ci.yml`).
3. Deduplicación (§H): `rg`/`grep` sobre `tracking.yaml`, ADRs y `docs/audits/` por *squash*,
   *merge strategy* y *uv.lock/pyproject desync*. Único hit de "squash" fuera de este scope:
   `scripts/check_production_gates.py:153,457,466,470` (historial atómico) y
   `docs/audits/2026-08-auditoria-integral.md` (tabla de estrategias permitidas: "Allow squash
   merge = YES"). ⇒ No hay finding previo para un squash **aplicado**: ambos son `NUEVO`.
4. Verificación remota sin escritura: `gh api repos/.../commits`, `/pulls/64`, `/branches`,
   `/git/refs`.

---

## 4. Control states (§F)

| # | Control | Evidencia (comando) | Estado |
|---|---|---|---|
| 1 | Estrategia de merge del PR #64 | `git show -s --format="%H %P" 680ddf53` → 1 padre ⇒ squash | **FAIL** |
| 2 | Ancestría/trazabilidad de los 121 commits L13 | `git log bad8a450 --not main --oneline \| wc -l` (a mantener por verificación manual vía `git log`) | **FAIL** |
| 3 | Integridad de contenido post-merge | `git diff --stat 680ddf53 bad8a450` (vacío) + `tree` = `59b7acab` | PASS |
| 4 | CI sobre `main` @ `680ddf53` | Runs GitHub (OCM CI, ShellCheck, Hadolint, yamllint) | PASS |
| 5 | CI sobre `main` @ `0ce216d7` (antes `fbc022b4`) | Runs GitHub (OCM CI, CodeQL, Trivy, Gitleaks) | PASS |
| 6 | Sincronía `pyproject.toml` ↔ `uv.lock` | `git show --stat 0ce216d7` + `gh api .../commits/fbc022b4` (files: solo `pyproject.toml` en ambos); `uv.lock` mantiene pydantic 2.13.5 y pyarrow `<26` | **FAIL** |
| 7 | Pre-check documentado del pin pydantic | `pyproject.toml` L58–68 exige `uv lock --dry-run` antes de tocar pydantic; no consta | **FAIL** |
| 8 | Gate de evidencia (políticas) | `policies/evidence.json` + `Policy gate (evidence hash)` (15 required checks) | PASS |
| 9 | Disciplina de escritura de este agente | `git status --short` (clean) + este único archivo | PASS |
| 10 | Validador mecánico / reproducibilidad | `uv run python scripts/audit_validator.py` | PASS |

**Controles = PASS(6) + FAIL(4) + PARTIAL(0) + NO_VERIFICADO(0) + INFRA_FAILURE(0) = 10**

> §H: 2 FEEDBACK de control (#1, #2, #6, #7) se reconcilian con `tracking.yaml` (sin entrada previa)
> ⇒ clasificación `NUEVO`, no `REVALIDADO`.

---

## 5. Finding Cards

### F-OCM-21 — PR #64 (L13) mergeado con squash: ancestría atómica perdida en `main`

- **Severidad:** ALTA (trazabilidad). *Nota de juicio (L2/L4):* impacto funcional = **ninguno**
  (contenido íntegro, CI verde); el impacto es de preservación de evidencia en un repositorio que
  cita SHAs como evidencia en ADRs/auditorías y en `policies/evidence.json`.
- **Clasificación (G):** `NUEVO`
- **Control:** #1 (Merge strategy), #2 (ancestría)
- **Status:** OPEN

**Evidence**

- `680ddf53` tiene **un** padre (`09a44a9d`) ⇒ squash (un merge commit tendría ≥2).
- `git log feat/m22b-governance --not main` = **121** commits únicos no presentes en `main`.
  Local `main` = 1291 commits; rama `feat/m22b-governance` = 1411; base común = 1290.
- `tree(680ddf53) == tree(bad8a450) == 59b7acab`; `git diff --stat 680ddf53 bad8a450` → vacío
  (⇒ el contenido L13 está íntegro en `main`; lo que falta es la ancestría).
- `refs/remotes/origin/feat/m22b-governance` = `bad8a450` (punta remota consultada vía `gh api`).
- SHAs de evidencia afectados (declarados en `AUDIT_OCM_L13-closure_2026-10-14.md` y
  `tracking.yaml`): `dbd41a57`, `28f0142d`, `2ca899a1`, `149d3e1a`, `93000bc5`, `bad8a450`.
- Reflog auxiliar: `HEAD@{3}` = `commit (amend): merge: L13 completo…`; `HEAD@{4}` =
  `commit (merge): cd /home/...` ⇒ el tip L13 (`93000bc5`) fue un **amend** de un merge con
  mensaje malformado (hallazgo de higiene, no bloqueante).

**Root cause**

- Selección de la estrategia **"Squash and merge"** en la UI del PR. `required_linear_history`
  no impone squash: un **merge commit** era válido. El protocolo del repo exige commits
  atómicos; el squash **destruye** esa atomicidad en `main` sin violar ningún check de CI.

**Impact / risk (residual)**

- Cualquier `git blame`/bisección sobre el contenido L13 apunta a `680ddf53`, no a los 121
  commits; los SHAs citados enlazan a una historia que `main` no referencia.
- Si se borra `feat/m22b-governance` (local o remota) antes de crear un tag, la historia atómica
  queda solo en reflogs/gc ⇒ pérdida irreversible.

**Remediation options**

| Opción | Efecto | Coste/riesgo |
|---|---|---|
| (b) Tag anotado `l13-atomic` → `bad8a450` (local+remoto) + **no** borrar la rama | Ancla inmutable y citable de los 121 commits | Mínimo, inmediato y reversible — **recomendado** |
| (b') Merge adicional de `bad8a450` en `main` | Restaura ancestría en `git log --graph`; `diff` vacío | Requiere PR a `main` protegido; no recupera SHAs (crea otro); un merge con diff vacío confunde |
| (a) Aceptar y documentar | Cero cambios | No recupera trazabilidad |
| (c) Revert + re-merge | Reintroduce L13 desde cero | Churn alto, riesgo operativo — ❌ |

**Recomendación:** (b) + mantener. Guard que evita nuevas ocurrencias: deshabilitar la estrategia
"squash" en la config del repositorio (allow_squash_merge=false) — una **Decisión Humana** fuera
de `main`; la política no-vigente no se eleva a norma (AUDIT_PROTOCOL §B).

---

## 5b. F-OCM-22 — `uv.lock` desincronizado (pydantic + pyarrow) respecto a `pyproject.toml` en `main`

Severity: MEDIUM
Status: CERRADO
Classification: **NUEVO**
Control: Sincronía pyproject ↔ uv.lock / 1 (ver §4 #6)

**Evidencia**

- PR #65 (dependabot) → `fbc022b4` bump pydantic 2.13.5 → 2.14.0: ficheros = **solo `pyproject.toml`**.
- PR #66 (dependabot) → `0ce216d7` update pyarrow requirement: **también solo `pyproject.toml`**
  (`git show --stat 0ce216d7` → 1 archivo, 1 insert/1 delete) ⇒ el patrón **se repite**.
- Re-verificado en el árbol de trabajo (`HEAD` == `origin/main` == `0ce216d7`):
  - `pyproject.toml:67` → `"pydantic==2.14.0"` · `pyproject.toml:82` → `"pyarrow>=17.0,<27.0"`
  - `uv.lock:2282-2283` → `pydantic` `version = "2.13.5"` · `uv.lock:1792` specifier `"==2.13.5"`
  - `uv.lock:1790` → `pyarrow` specifier `">=17.0,<26.0"` (≠ `<27.0` del pyproject)
- Ningún job de CI ejecuta `--frozen`/`--locked` (`uv sync --group dev`, `uv sync`): el CI no
  detecta la desincronización.
- `pyproject.toml` L58–68 (comentario del pin) exige textualmente un `uv lock --dry-run` antes de
  tocar el pin; **no consta** su ejecución para este cambio.
- `uv lock --dry-run` sobre copia aislada (`/tmp/opencode/uvtest`, pin editado a 2.14.0): sin
  errores; resolvería `pydantic 2.13.5 → 2.14.0` (+ `pydantic-core` y `typing-inspection`). Es
  decir: **la compatibilidad de resolución no es el problema; el lock no se comprometió.**
- `rg model_rebuild` → **0 usos** en el repo (código) ⇒ el motivo del pin no se materializa en
  supresiones actuales, lo que reduce el riesgo real, pero no lo verifica en runtime.

**Observación mecánica directa (reproducida durante esta auditoría):**

- Ejecutar `uv run <script>` con el desfase vigente **re-lockea `uv.lock` de forma silenciosa**.
  Verificado en este mismo repo: `git diff --stat -- uv.lock` → `72 insertions(+), 72 deletions(-)`
  y `pydantic` pasó a `version = "2.14.0"` sin ningún mensaje explícito de actualización de lock.
- El cambio **no** se persiste: `uv.lock` se restauró byte a nivel de blob desde `HEAD`
  (`git show HEAD:uv.lock > uv.lock`), `git status --short` vuelve a mostrar únicamente este
  informe, y `git diff --stat -- uv.lock` queda vacío.
- **Causa raíz identificada (no es la bandera):** `scripts/audit_validator.py` M11 ejecuta
  `subprocess.run(["uv","run","python","-m","architecture_linter","--root",…,"--json"])`
  (L539–545), y el path `--versions` ejecuta `uv run ruff/mypy/bandit/pytest` (L984–988). Con
  `pyproject` ↔ `uv.lock` desfasados, esa invocación **re-lockea `uv.lock`** ⇒ incluso el validador
  canónico (`.venv/bin/python scripts/audit_validator.py`) ensucia el árbol
  (`72 insertions(+), 72 deletions(-)`; pydantic 2.13.5 → 2.14.0). Verificado por aislamiento: tras
  restaurar el lock, ejecutar **solo** el validador vuelve a producir el diff.
- **No reproducido** con el árbol restaurado y el entorno alineado: `uv run --no-sync`,
  `uv run --frozen` y `uv run --frozen --no-sync` sobre `python -c "print('ok')"`, y
  `.venv/bin/python -c pass`, **no** escribieron (`git diff --numstat -- uv.lock` vacío, exit=0).
- Conclusión calibrada: el disparador es la **deriva pyproject ↔ lock** (agravada por entorno
  desalineado), no las banderas `--frozen`/`--no-sync`. Cualquier invocación de `uv` —incluida la
  del propio validador— puede reescribir el lock en silencio.
- Regla operativa para agentes: tras cualquier `uv run`, verificar `git diff -- uv.lock` y
  restaurar desde `HEAD` (`git show HEAD:uv.lock > uv.lock`). Al cierre de este informe `uv.lock`
  es **idéntico a `HEAD`** (`git status --short` solo muestra este archivo).

**Guard mecánico candidato — probado sobre el lock DESFASADO (`0ce216d7`):**

- `uv lock --check` → **exit=1** · `The lockfile at 'uv.lock' needs to be updated, but --check was
  provided.` ⇒ **detecta** la divergencia pyproject ↔ lock y **no escribe** el árbol.
- `uv sync --locked --dry-run` → emite el mismo error (`--locked` provided) ⇒ también detecta.
- `uv run --frozen` → **NO es un guard** (no compara pyproject ↔ lock; con deriva puede re-lockear).

**Impact (transversal)**

- Toda persona/CI que haga `uv sync` **reactiva** el lock; el primer `uv sync` local hará un
  re-lock silencioso ⇒ `uv.lock` aparecerá modificado sin serlo por una decisión humana
  (ruido diff / riesgo de commit accidental).
- El sesgo "el CI está verde" **no prueba** que el lock sea correcto: el CI actual (`uv sync` sin
  `--locked`/`--check`) no mira la sincronía, y `--frozen` no compara pyproject ↔ lock. El único
  guard que la detecta es `uv lock --check` / `uv sync --locked`.

**Remediación propuesta (no aplicada — §K prohíbe tocar `uv.lock`/`pyproject.toml`)**

1. `uv lock` (una sola vez) y **commitear `uv.lock` + `pyproject.toml` juntos** (restaura la
   invariante pyproject ↔ lock).
2. Añadir un step `uv lock --check` en CI (probado: exit=1 con el lock desfasado y sin escribir
   el árbol). `uv sync --frozen` no compara pyproject ↔ lock y, con deriva, `uv run` **puede
   re-lockear** el árbol de trabajo (evidencia arriba).
3. Actualizar `AGENTS.md` (menciona `pydantic==2.8.2`, obsoleto) y el comentario del pin para
   reflejar 2.13.5 → 2.14.x y el guard vigente.

---

## 6. Findings register (resumen)

| ID | Título | Severidad | Clasificación | Estado |
|---|---|---|---|---|
| F-OCM-21 | PR #64 mergeado con squash: colapsó 121 commits L13 | MEDIA-ALTA | NUEVO | ABIERTO |
| F-OCM-22 | `uv.lock` desincronizado (pydantic 2.13.5 / pyarrow `<26`) vs pyproject (2.14.0 / `<27`) | MEDIA | NUEVO | CERRADO |

**Registro en `tracking.yaml`: HECHO (2026-10-16).** Los fragmentos del Anexo A se registraron como
F-OCM-21 (`PENDIENTE`), F-OCM-22 (`HECHO`) y F-OCM-23 (PENDIENTE, ver
`AUDIT_OCM_ci-gate-gap_2026-10-16.md`).

---

## 7. Decisiones humanas requeridas (§N — D-)

### D-61 — Preservación de la historia L13 (F-OCM-21)

**Pregunta:** ¿aplicamos `git tag -a l13-atomic bad8a450 && git push origin l13-atomic` y
mantenemos `feat/m22b-governance` (local y remota)?

**Opciones:** (a) tag + no borrar rama · (b) tag + borrar rama tras verificar el tag ·
(c) merge adicional para restaurar ancestría en `main` (vía PR) · (d) no actuamos.

**Consecuencias:** (a)/(b) hacen inmutable la evidencia citada; (c) recupera vista lineal pero
genera un merge con diff vacío; (d) mantiene el riesgo si alguien borra la rama.

**Recomendación:** (a) — barato, reversible y suficiente. Si se quiere `--graph` continuo, (c)
después y por PR.

### D-62 — Reparación del lock y guard mecánico (F-OCM-22)

**Pregunta:** ¿reparamos con `uv lock` + step `uv lock --check` en CI y bump de `AGENTS.md`?

**Opciones:** (a) `uv lock` + `--frozen` en CI + AGENTS.md · (b) solo `uv lock` ·
(c) `uv lock` + `uv lock --check` en CI + AGENTS.md · (d) esperar a agrupar con otro bump.

**Consecuencias:** (c) es la única que cierra el fallo de forma defensiva (lock reparado +
detección automática de futuras divergencias pyproject ↔ lock, ya ocurridas **2 veces**: PR #65
pydantic y PR #66 pyarrow); (a) repara, pero `--frozen` no compara pyproject ↔ lock, así que no
detecta futuras divergencias; (b) puede repetirse; (d) deja `main` inconsistente.

**Recomendación:** (c) — mínimo movimiento de config; el re-lock manual sigue permitido, pero CI
detecta la divergencia (evidencia: `uv lock --check` → `exit=1` con el lock desfasado).

---

### Rulings humanos registrados (2026-10-16)

- **D-61** → autorizado para ejecución **humana** (`git tag -a l13-atomic bad8a450` +
  `git push origin l13-atomic`, sin borrar `feat/m22b-governance`).
- **D-62** → confirmada la opción **(c)**: `uv lock` + step `uv lock --check` en CI + bump de
  `AGENTS.md`. Nota de precisión: reparar el lock significa **avanzarlo** a lo que pide
  `pyproject.toml` (`pydantic==2.14.0`, `pyarrow>=17.0,<27.0`); "mantener 2.13.5" equivaldría a
  revertir `pyproject.toml` (descartar los bumps ya mergeados PR #65/#66) — no recomendado.
- **F-OCM-21 severidad** ratificada **MEDIA-ALTA**; **F-OCM-22** MEDIA.
- ⚠️ `policies/evidence.json` (L11) pinnea el SHA256 de `.github/workflows/ocm-ci.yml` ⇒ cualquier
  edición del workflow **exige** recalcular ese hash o `policy-gate-evidence` falla.

---

## 8. Reproducibility block (§S)

```
REPRODUCIBILIDAD
- commit: 0ce216d72a39d6fb7f7c3687cb548c75dcf7ca73   (= origin/main; squash L13 histórico: 680ddf53)
- branch: main
- fecha: 2026-10-16
- protocolo: AUDIT_PROTOCOL v2.1
- agente/modelo: OpenCode — big-pickle (OrangeCashMachine)
- herramientas:
    uv 0.11.14 (x86_64-unknown-linux-gnu)
    Python 3.13.5
    pip-audit 2.10.1
    ruff 0.15.10
    mypy 1.19.1 (compiled: yes)
    bandit 1.9.4
    pytest 9.0.3
    yamllint 1.38.0
- comandos: (ver §3 y Evidencias de cada finding card)
    uv run python scripts/audit_validator.py
    uv run python scripts/audit_validator.py --versions
    git rev-parse HEAD / git status --short
    git show -s --format="%H %P"      # topología del commit 680ddf53
    git log <rama> --not main         # conteo / ancestría
    git diff --stat                   # integridad de contenido
    gh api repos/OrangeCashDigital/orangecashmachine/...   # commits, pulls, branches, refs
    uv lock --dry-run (copia aislada /tmp/opencode/uvtest, no en el repo)
    .venv/bin/python scripts/audit_validator.py    # M11 → uv run architecture_linter → re-lockea con deriva
    uv lock --check                    # guard: exit=1 con el lock desfasado (evidencia §5b)
    uv run --frozen …                  # la bandera no evita el re-lock con deriva
    git show --stat 0ce216d7           # PR #66: solo pyproject.toml
- golden: PASS (audit_validator)
- resultado: PASS
```

> Nota de honestidad metodológica: al agente le están **denegados** `git *` (excepto
> `status/diff/log/show/blame/ls-files/rev-parse/check-ignore`) por lo que `fetch/rev-list/
> branch -d/stash drop/reflog` **no** pudieron ejecutarse; las afirmaciones sobre puntas remotas
> usan la GitHub REST API (`gh api`) como fuente verificable.

---

## 9. Anexo A — fragmentos de `tracking.yaml` (propuesta, NO aplicada)

**Ubicación exacta:** al final de la secuencia `hallazgos:` (L117), **después** del entry
`F-OCM-20` (L3495–L3533) y **antes** de `mediciones_f0:` (L3534). Esquema copiado de los entries
vecinos (`F-OCM-19`/`F-OCM-20`): `id, hallazgo_informe, fase, prioridad, estado, estado_auditoria,
evidencia, solucion, pruebas[], adr_relacionado, riesgo_residual, fecha_cierre,
cadena{hallazgo,backlog,adr,implementacion,tests,ci,evidencia,cierre}`. **No existe campo
`severidad` en `tracking.yaml`** (la severidad vive en el informe/register, no en el tracking).

```yaml
- id: F-OCM-21
  hallazgo_informe: F-OCM-21
  fase: F2
  prioridad: ALTA
  estado: ABIERTO
  estado_auditoria: CONFIRMADO
  evidencia: 'docs/audits/AUDIT_OCM_squash-incident_2026-10-16.md §F-OCM-21: PR #64 mergeado con
    squash; 680ddf53 tiene 1 padre (09a44a9d) y los 121 commits de feat/m22b-governance
    (bad8a450) no son ancestros de main. Contenido íntegro: tree(680ddf53) == tree(bad8a450) ==
    59b7acab, git diff vacío.'
  solucion: 'D-61 (autorizado 2026-10-16): git tag -a l13-atomic bad8a450 && git push origin
    l13-atomic; NO borrar feat/m22b-governance. Alternativa (c): merge adicional vía PR para
    restaurar ancestría.'
  pruebas:
  - 'git show -s --format=%H %P 680ddf53 → 1 padre'
  - 'git log feat/m22b-governance --not main → 121 commits'
  - 'git diff --stat 680ddf53 bad8a450 → vacío; tree 59b7acab'
  - 'gh api .../branches | refs: refs/heads/feat/m22b-governance = bad8a450'
  adr_relacionado: null
  riesgo_residual: 'Si se borra feat/m22b-governance antes de crear el tag, los SHAs de evidencia
    citados en ADRs/auditorías quedan irrecuperables.'
  fecha_cierre: null
  cadena:
    hallazgo:
      estado: HECHO
      referencia: docs/audits/AUDIT_OCM_squash-incident_2026-10-16.md F-OCM-21
    backlog:
      estado: HECHO
      referencia: F-OCM-21
    adr:
      estado: NO_APLICA
      referencia: null
    implementacion:
      estado: PENDIENTE
      referencia: 'git tag: l13-atomic → bad8a450'
    tests:
      estado: NO_APLICA
      referencia: null
    ci:
      estado: PENDIENTE
      referencia: 'técnico: permitir únicamente merge commit en la UI (out-of-repo setting)'
    evidencia:
      estado: HECHO
      referencia: docs/audits/AUDIT_OCM_squash-incident_2026-10-16.md §3
    cierre:
      estado: PENDIENTE
      referencia: null
- id: F-OCM-22
  hallazgo_informe: F-OCM-22
  fase: F2
  prioridad: MEDIA
  estado: ABIERTO
  estado_auditoria: CONFIRMADO
  evidencia: 'docs/audits/AUDIT_OCM_squash-incident_2026-10-16.md §F-OCM-22: dos merges dependabot
    (fbc022b4 pydantic 2.14.0; 0ce216d7 pyarrow <27) tocan solo pyproject.toml; uv.lock mantiene
    pydantic version/specifier "2.13.5" y pyarrow specifier ">=17.0,<26.0".'
  solucion: 'D-62 (c) ratificada 2026-10-16 y ejecutada (PR #68): uv lock + step "uv lock
    --check" en CI (guard verificado: exit=1 con el lock desfasado) + bump AGENTS.md.'
  pruebas:
  - 'git show --stat 0ce216d7 → files: [pyproject.toml] (1 ins / 1 del)'
  - 'gh api .../commits/fbc022b4 → files: [pyproject.toml]'
  - 'grep uv.lock: pydantic version = "2.13.5" / specifier ==2.13.5; pyarrow specifier >=17.0,<26.0'
  - 'uv lock --dry-run (copia /tmp/opencode/uvtest con pin 2.14.0) → sin errores'
  - 'rg model_rebuild → 0 usos'
  adr_relacionado: null
  riesgo_residual: 'Re-lock silencioso en local (working tree sucio) + falsa confianza derivada de
    un CI sin --frozen.'
  fecha_cierre: '2026-10-16'
  cadena:
    hallazgo:
      estado: HECHO
      referencia: docs/audits/AUDIT_OCM_squash-incident_2026-10-16.md F-OCM-22
    backlog:
      estado: HECHO
      referencia: F-OCM-22
    adr:
      estado: NO_APLICA
      referencia: null
    implementacion:
      estado: HECHO
      referencia: 'uv.lock sincronizado (419705b3): pydantic 2.14.0, pyarrow <27'
    tests:
      estado: HECHO
      referencia: 'CI run 38919538181 @ c797b249 (y 38928456353 @ df8c5b06): jobs "Unit tests" y "Coverage gate (suite completa)" = success'
    ci:
      estado: HECHO
      referencia: '.github/workflows/ocm-ci.yml — step "Lockfile in sync (pyproject <-> uv.lock)" en 2 jobs (PR #68, df8c5b06)'
    evidencia:
      estado: HECHO
      referencia: docs/audits/AUDIT_OCM_squash-incident_2026-10-16.md §5b
    cierre:
      estado: HECHO
      referencia: 'PR #68 → merge commit df8c5b06 (2026-10-16)'
```

**Actualizado 2026-10-16 (post-merge del PR #68):** el fragmento F-OCM-22 pasa de
`PENDIENTE` a `HECHO` — `uv.lock` regenerado (`419705b3`: pydantic 2.14.0, pyarrow `<27`) y guard
`uv lock --check` vivo en `.github/workflows/ocm-ci.yml` (2 jobs); merge commit `df8c5b06`,
`fecha_cierre: '2026-10-16'`. **F-OCM-21 permanece `PENDIENTE`**: el tag `l13-atomic` no existe
(`gh api .../git/refs/tags/l13-atomic` → 404).

---

## 10. Anexo B — comandos para el humano (nada aplicado)

```bash
# G1 — preservar la historia atómica L13 (F-OCM-21)
git tag -a l13-atomic bad8a450 -m "L13 atomic history (121 commits, pre-squash #64)"
git push origin l13-atomic
# NO borrar feat/m22b-governance (local ni remota)

# G2 — reparar lock (F-OCM-22)  [decisión: D-62]
uv lock
git add pyproject.toml uv.lock   # commit por el humano

# G3 — housekeeping
git fetch --prune
git branch -d feat/b49-production-gates-script feat/b52-governance-controls \
  feat/b55-ast-guards-formalization temp-pr31 feat/f2-6e-p2-fix \
  feature/dependabot/pip/pytest-cov-range2 feature/dependabot/pip/pytest-cov-range4
git stash show -p stash@{5} > /tmp/stash5.patch && git stash drop stash@{5}   # + {3}/{4} con criterio
# stash@{0} = M26 — NO TOCAR
```

---

**Refs:** `docs/governance/AUDIT_PROTOCOL.md` v2.1 · `docs/plans/tracking.yaml` (F-OCM-20 último) ·
`AUDIT_OCM_L13-closure_2026-10-14.md` · `AUDIT_OCM_l13a-closure_2026-10-12.md` ·
`policies/evidence.json` · `.github/workflows/ocm-ci.yml`.
