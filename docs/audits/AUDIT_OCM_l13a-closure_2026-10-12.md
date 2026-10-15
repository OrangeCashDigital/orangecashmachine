# Cierre L13a — mypy `strict` en `ocm/` + `packages/`

- **Fecha:** 2026-10-12
- **Rama:** `feat/m22b-governance-l13`
- **Worktree:** `ocm-l13` (aislado)
- **Objetivo:** llevar `ocm/` + `packages/` a 0 errores `mypy --strict`.

---

## 1. Resumen ejecutivo

La migración L13a redujo los errores `mypy --strict` de **217 → 62** (**−155, −71%**)
a lo largo de **14 sub-lotes atómicos** (2.2 → 2.6). El **scope objetivo
(`ocm/` + `packages/`) alcanzó 0 errores**.

Los 62 errores restantes están **fuera del scope L13a** (tooling/apps/docs) y se
difieren a L13b/L13c.

- Sin `# type: ignore` introducidos.
- Sin rediseño de contratos más allá de correcciones documentadas.
- `pyproject.toml` y `docs/plans/tracking.yaml` **intactos** (sesión B activa).
- Backups de cada lote en `/tmp/opencode/`.

---

## 2. Cadena de commits

| Sub-lote | Commit | mypy | Δ |
|---|---|---|---|
| Base | `c1fb6564` | 217 | — |
| 2.2 | `17bf7e1f` | 212 | −5 |
| 2.3 | `34f46431` | 203 | −9 |
| 2.4 | `b4b35dda` | 172 | −31 |
| 2.5a | `20072a77` | 155 | −17 |
| 2.5a-fix | `28f0142d` | 155 | 0 |
| 2.5b-1 | `78584962` | 143 | −12 |
| 2.5b-2 | `2ca899a1` | 131 | −12 |
| 2.5b-3 | `9cc5ea15` | 123 | −8 |
| 2.5b-4 | `667061b4` | 112 | −11 |
| 2.5c1 | `48f82cd3` | 105 | −7 |
| 2.5c2 | `f364b5c1` | 95 | −10 |
| 2.5d-1 | `7a502ff5` | 88 | −7 |
| 2.5d-2 | `74e01799` | 83 | −5 |
| 2.5d-3 | `8348c06a` | 76 | −7 |
| 2.5d-4 | `714a2d40` | 73 | −3 |
| 2.5d-5 | `51ed4183` | 70 | −3 |
| **2.6** | **`149d3e1a`** | **62** | **−8** |

Cada sub-lote: pre-commit Passed, `git status` limpio, health PASS, suite verde,
diff mostrado y aplicación tras OK.

---

## 3. Findings registrados

| ID | Título | Estado | Registrado en |
|---|---|---|---|
| F-OCM-14 | Commit `46369128` con `--no-verify` (roto + contaminado) | Resuelto por sesión B | `dbd41a57` (restore imports dropped) |
| F-OCM-15 | `SignalProtocol` settable vs `Signal` frozen | Diferido → L13b | `51ed4183` (detectado, SSOT Signal) |
| F-OCM-17 | Commit `20072a77` con 4 anotaciones string no importables | Reparado | `28f0142d` |
| F-OCM-18 | `LineageTrackerPort.record` mal tipado (`object` vs `LineageEvent`) | Reparado | `2ca899a1` |
| F-OCM-19 | `run_worker_pool` generic mal tipado (un `TypeVar` para item y resultado) | Reparado | `149d3e1a` |

**Pendiente:** registro formal en `docs/plans/tracking.yaml` **post-merge**
(sesión B activa sobre ese archivo; los findings ya están trazados en los
mensajes de commit vía `Refs: L13a-X, F-OCM-Y`).

---

## 4. Estado del scope

| Scope | mypy strict | Fase |
|---|---|---|
| `ocm/` + `packages/` | **0** | ✅ L13a cerrado |
| `architecture_linter/` | 25 | L13b |
| `apps/` | 19 | L13b |
| `scripts/` | 14 | L13c |
| `docs/` | 4 | L13c |
| **Total restante** | **62** | L13b/L13c |

---

## 5. Próximos pasos

1. **L13-commit:** activar `strict=true` en `pyproject.toml` (decisión pendiente;
   evaluar si scope `ocm/`+`packages/` o global).
2. **Merge** de `feat/m22b-governance-l13` → `feat/m22b-governance`.
3. **Registro formal** de findings en `tracking.yaml` (post-merge, coordinado con sesión B).
4. **Decisión** L13b/L13c: ¿expandir scope a los 62 out-of-scope o cerrar aquí?

Ninguno de estos pasos se inicia sin OK explícito.

---

## 6. Notas de proceso

- El **worktree aislado** (`ocm-l13`) evitó interferir con la sesión B activa en
  `orangecashmachine`.
- La **excepción scoped** a la regla de AGENTS.md (agents no ejecutan git
  add/commit) quedó documentada y limitada a `ocm-l13`.
- Incidentes de commits rotos (F-OCM-14, F-OCM-17) se **contuvieron sin reescribir
  historia** (fix-forward vía commits de reparación).
- Patrón **"cascade handled in-lote"** aplicado de forma consistente: tipar un
  boundary destapa errores latentes → resueltos en el mismo lote.
- Corrección genérica de F-OCM-19 (`run_worker_pool`: 2 `TypeVar` `TItem`/`TResult`)
  mostró que los 6 errores dependientes tenían una raíz única; los 3 call sites
  no requirieron cambios.

---

**Refs:** L13a, F-OCM-14/15/17/18/19.
