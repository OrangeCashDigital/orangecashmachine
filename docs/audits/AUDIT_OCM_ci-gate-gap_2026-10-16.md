# AUDIT OCM — CI gate gap: workflow `Actionlint` en rojo, check no-required (F-OCM-23)

- **Fecha de ejecución:** 2026-10-16
- **Protocolo:** `docs/governance/AUDIT_PROTOCOL.md` v2.1 (§Q–§T: MACHINE CHECKS FIRST)
- **Régimen:** read-only (AUDIT_PROTOCOL §K). Única escritura permitida: este informe en `docs/audits/`.
- **Tipo:** higiene de CI + gap de gobernanza (check no-required no bloquea merges)
- **Finding card:** F-OCM-23 (NUEVO · MEDIA-ALTA)
- **Hallazgos relacionados:** F-OCM-21 y F-OCM-22 → `AUDIT_OCM_squash-incident_2026-10-16.md`
- **Origen:** verificación post-merge del PR #68 (`df8c5b06`)

---

## F-OCM-23 — Finding card (MEDIA-ALTA)

| Campo | Valor |
|---|---|
| ID | `F-OCM-23` |
| Título | Workflow `Actionlint` (actionlint v1.80.0) en rojo en `main` por runtime node20 deprecado; el check **no es required** |
| Severidad | **MEDIA-ALTA** (ratificada por decisión humana 2026-10-16) |
| Clasificación | NUEVO (sin registro previo en `tracking.yaml`, ADRs ni auditorías) |
| Estado | **PENDIENTE** |
| Evidencia | §2 de este informe + runs `38918051643` / `38919538102` |

**Evidence**

- Workflow `Actionlint` (`.github/workflows/actionlint.yml`): job `Lint workflow files`,
  `reviewdog/action-actionlint` pin `a8ac3eea598697a830f7d8e701fc9fcbfde88974 # v1.80.0`,
  `fail_level: error` ⇒ la deprecación de runtime se eleva a **error** y rompe el job.
- Triggers: `push` y `pull_request` con `paths: '.github/workflows/**'` ⇒ solo corre en cambios de
  workflows, no en el resto de PRs.
- **4 usos** de actions sobre runtime `node20` (inventario estático, reproducible; los logs del run
  ya no son recuperables vía CLI ⇒ **no** se cita el conteo de líneas `e:`):
  1. `.github/workflows/ocm-cd.yml:23` → `actions/checkout@v4`
  2. `.github/workflows/ocm-cd.yml:26` → `docker/setup-qemu-action@v3`
  3. `.github/workflows/ocm-cd.yml:29` → `docker/setup-buildx-action@v3`
  4. `.github/workflows/ocm-ci.yml:271` → `actions/upload-artifact@v4` (única ocurrencia del archivo;
     idéntica en `main` y en la rama del guard)
- Diagnóstico emitido por el job: `runtime "node20" is deprecated` (líneas `e:` ⇒ falla). Las líneas
  `i:` observadas eran `SC2086`: informativas, no rompen el job.
- **Por confirmar al aplicar el fix:** `astral-sh/setup-uv@v4` (`.github/workflows/ocm-cd.yml:32`) no fue
  citado en el análisis previo del job; su runtime debe verificarse (logs ya no recuperables).
- Referencia interna ya disponible: `nightly-compliance.yml:29` usa `actions/upload-artifact@v7`.

**Root cause**

- El merge `b434f43c` (PR #67: bump automático `reviewdog/action-actionlint` 1.73.4 → 1.80.0)
  elevó a error la deprecación de runtime. El bump se validó en el CI del propio PR #67 (solo mira
  cambios de workflows) y pasó a `main` sin que ningún check verificara el estado de `main`.

**Timeline**

| Momento | Punto de `main` | Estado del check |
|---|---|---|
| 11:01 | `680ddf53` (merge PR #64) | success (último verde) |
| 11:32 | `b434f43c` (merge PR #67) | failure (primer rojo) |
| 16:00 | `df8c5b06` (merge PR #68) | failure (persiste) |

---

## 1. Contexto

Durante la verificación post-merge del PR #68 (`df8c5b06`) el único check en rojo era
`Actionlint` (job `Lint workflow files`). El PR #68 tenía **15/15 required checks en verde** y
`mergeStateStatus: UNSTABLE`; el rojo **no** proviene de ese PR: `main` ya estaba rojo desde
`b434f43c` (11:32, merge del PR #67), con último verde en `680ddf53` (11:01). Se registra como
F-OCM-23 y se separa del PR de limpieza post-merge (no toca `uv.lock`/`tracking.yaml`).

---

## 2. Evidencia (reproducible)

| # | Comando | Resultado |
|---|---|---|
| 1 | `grep -n "uses:" .github/workflows/*.yml` | inventario estático: 4 usos de actions node20 (arriba); `nightly-compliance.yml:29` ya en `@v7` |
| 2 | `gh api /repos/…/actions/runs/38918051643` | Actionlint · `push` · `ci/uv-lock-check-guard` @ `c797b249` · **failure** · 14:43:18Z |
| 3 | `gh api /repos/…/actions/runs/38919538102` | Actionlint · PR #68 @ `c797b249` · **failure** · 14:54:11Z |
| 4 | `gh api /repos/…/actions/runs/38928456429` | Actionlint · `push` · `main @ df8c5b06` · **failure** · 16:00:14Z |
| 5 | `gh api /repos/…/branches/main/protection` | 15 required contexts; `Lint workflow files` **ausente** |
| 6 | `gh run list --workflow=Actionlint --branch main` | último `success` `680ddf53` (11:01:30Z); primer `failure` `b434f43c` (11:32:56Z) |
| 7 | `sha256sum .github/workflows/ocm-ci.yml` ⨯ `policies/evidence.json:11` | coinciden (`766ac6c2…`) |

**Nota de reproducibilidad:** los *logs* de esos runs ya no son recuperables vía CLI, así que el texto
literal de las líneas `e:` **no** se cita como evidencia primaria. Sí se verifican por API la identidad,
conclusión y fecha de cada run, y estáticamente el inventario de actions node20 sobre `main`.

Runs (identidad verificada por API): `38918051643` (push, rama `ci/uv-lock-check-guard`) ·
`38919538102` (PR #68) · `38928456429` (push, `main` post-merge #68)

---

## 3. Impacto

- **Higiene de CI:** `main` está en rojo por un check no-required desde `b434f43c`; el fallo se
  tolera silenciosamente porque no bloquea merges.
- **Gap de gobernanza:** todo PR que toque `.github/workflows/**` arrastra el rojo ⇒ no alcanza
  `mergeStateStatus: CLEAN` (el PR #68 quedó `UNSTABLE` con 15/15 required verdes).
- **Riesgo residual:** normalizar el rojo degrada el check; un fallo real de actionlint sobre
  workflows nuevos pasaría desapercibido dentro del mismo job.

---

## 3b. No-alcance (explícito)

- **No** se modifica ningún workflow ni `policies/evidence.json` (§K: read-only).
- **No** se toca la protección de rama (config fuera del repo; requiere decisión humana).
- F-OCM-15 (diferido) y G1/D20b (bloqueado) quedan fuera.

---

## 4. Remediación propuesta (no aplicada)

1. **Bump a releases con runtime node24** donde existan:
   `actions/upload-artifact@v4 → v7` (precedente interno: `nightly-compliance.yml:29`),
   `actions/checkout@v4 → v7`, y verificar `docker/setup-qemu-action` / `docker/setup-buildx-action`.
2. Si alguna action **no** publica release node24: `-ignore` **dirigido** en actionlint para
   `runtime "node20" is deprecated` (no un ignore global), documentado en `tracking.yaml`.
3. ⚠️ Editar `.github/workflows/ocm-ci.yml` **exige** re-attest de `policies/evidence.json`
   (L11 pinnea su SHA256: `766ac6c2…`) en el **mismo commit**; `policy-gate-evidence` es
   VERIFY ONLY y fallaría con el hash viejo.
4. Cierre del gap de gobernanza (decisión humana, fuera del repo): evaluar si `Lint workflow files`
   debe pasar a *required* o si el job se consolida/retira. El gate no se modifica por esta vía.

Ejecución en **PR separado** (rama `chore/actionlint-node24`), posterior al PR de limpieza
post-merge de F-OCM-21/22.

---

## 5. Referencias

- `docs/audits/AUDIT_OCM_squash-incident_2026-10-16.md` (F-OCM-21, F-OCM-22) · PR #68 (`df8c5b06`) ·
  PR #67 (`b434f43c`).
- `policies/evidence.json` · `.github/workflows/actionlint.yml` · `.github/workflows/ocm-ci.yml` ·
  `.github/workflows/ocm-cd.yml` · `.github/workflows/nightly-compliance.yml`.
- `docs/plans/tracking.yaml` → F-OCM-23.

---

## 6. Estado

**PENDIENTE.** Registrado en `tracking.yaml` como `F-OCM-23` (prioridad `ALTA`, `estado: PENDIENTE`,
`cadena.implementacion|tests|ci|cierre = PENDIENTE`, `estado_auditoria: CONFIRMADO`). Cierre
requiere: workflows con actions node24 (o `-ignore` dirigido documentado) + re-attest de
`evidence.json` si se toca `ocm-ci.yml` + Actionlint verde en `main`.
