# BLINDAJE-APPS-2026-08-06 — Blindaje de la Application Layer

**Fecha:** 2026-08-06
**Alcance:** `apps/` (CLI `app/` + gateway `api/`)
**Origen:** hallazgos ya corregidos de `AUDIT-apps-2026-08-03` (H1, H4, H6, H8, H12)
**Principio rector:** PLAN-Maestro-Ingenieria.md §6 — *todo bug corregido pasa a ser una
regla permanente verificable por CI*. La calidad no depende de disciplina humana sino de
mecanismos estructurales.

---

## 1. Qué es esto

Los cinco hallazgos de la Application Layer detectados en `AUDIT-apps-2026-08-03` fueron
corregidos manualmente en `cdd7e7e` (H1/H4/H8/H12) y `2717d06` (H6). Este blindaje convierte
esas correcciones en **reglas auto-defendibles**: si el código vuelve a reintroducir el
anti-patrón, el pipeline **falla** — no se necesita una revisión humana para darse cuenta.

Serie de hallazgos: `AUDIT-apps-2026-08-03#Hx` (serie distinta de
`INFORME-2026-08-06#H-01…H-22`). Ver §2.

## 2. Desambiguación de series H

Existen dos series de hallazgos con numeración similar que **no deben confundirse**:

| Serie | Formato de ID | Fuente |
|-------|---------------|--------|
| `AUDIT-apps-2026-08-03` | `#H1`, `#H4`, `#H6`, `#H8`, `#H12` | Auditoría de composition roots de la Application Layer |
| `INFORME-2026-08-06` | `#H-01`, `#H-02`, … `#H-22` | Auditoría técnica integral (fotografía en `dcd1741`) |

Convención (ADR-0015, tracking.yaml): los artefactos nuevos usan el prefijo desambiguado
`AUDIT-apps-2026-08-03#Hx`. Los documentos legacy y comentarios existentes conservan su
numeración original; esta distinción queda documentada aquí y en el ADR.

## 3. Reglas R12–R16

| Regla | Hallazgo | Anti-patrón bloqueado | Mecanismo |
|-------|----------|----------------------|-----------|
| R12 | `AUDIT-apps-2026-08-03#H1` | `use_cases` tocan `argparse`/`Namespace`/constructores de config; el Namespace moría en el borde CLI | Guard AST en `app/use_cases` + contrato **BC-53** (`forbidden app.use_cases → argparse`) |
| R13 | `AUDIT-apps-2026-08-03#H4` | `getattr` con default en `use_cases` (config tipada es la única fuente de verdad) | Guard AST |
| R14 | `AUDIT-apps-2026-08-03#H8` | scaffolding de CLI duplicado (`_handle_sigterm`, `logger.remove`, helpers, `main()` god) fuera de `app/cli/_bootstrap.py` | Guard AST (una sola fuente + must-import + complejidad ≤ 20) + contrato **BC-54** (`forbidden app.use_cases → app.cli`) |
| R15 | `AUDIT-apps-2026-08-03#H12` | `CycleRunResult` redefinido (`LiveRunResult`/`PaperRunResult`) | Guard AST (único `CycleRunResult` en `app/use_cases/run_result.py`) |
| R16 | `AUDIT-apps-2026-08-03#H6` | `SILENT_PATHS` redefinido en middleware y probes procesadas sin exclusión temprana | Guard AST (SSOT en `api/middleware/__init__.py` + exclusión antes del primer `await`) |

### Por qué guard AST + contratos (y no solo import-linter)

- La Application Layer usa **lazy imports deliberados** (E402 en `apps/app/cli/`): el grafo
  de módulos de import-linter **no los ve**; el guard AST escanea el texto fuente y sí.
- import-linter 2.x no soporta contratos `must_import` (tipos válidos:
  forbidden/layers/independence/protected): el must-import de `_bootstrap` lo garantiza solo
  el guard.
- Reglas de "ubicación exacta", "una sola definición" y "complejidad ciclomática" no son
  expresables como contrato de import-linter.

## 4. Evidencia de verificación (todo verde)

```
$ uv run pytest tests/architecture/test_app_layer_guard.py -q -m "not integration"
42 passed                                  # pruebas pos/neg por regla + master guard_app(ROOT)==[]

$ uv run python scripts/backtest_app_guard.py
[OK] 39687e7  (pre-fix H1/H4/H6/H8/H12 — padre de cdd7e7e)  → dispara R12, R13, R14, R15, R16
[OK] cdd7e7e  (post-H1/H4/H8/H12, pre-H6 — padre de 2717d06) → dispara solo R16
[OK] HEAD     (working tree)                                  → 0 violaciones
Contratos: 13 checks del guard | Backtest: PASS

$ uv run lint-imports --config architecture/importlinter.toml
Contracts: 49 kept, 0 broken.             # incluye BC-53 y BC-54

$ uv run mypy apps/ --no-incremental
Success: no issues found in 26 source files

$ uv run pytest tests/ -q -m "not integration"
882 passed                                # sin regresiones (batería completa)
```

El backtest ejecuta el guard contra los **snapshots pre-fix exactos** (`git archive` de los
parents de los commits de fix, sin tocar el working tree) y exige el *ruleset* correcto por
snapshot: cero falsos negativos (todo hallazgo histórico detectado) y cero falsos positivos
(HEAD limpio, `cdd7e7e` solo R16).

## 5. Gate en CI (fail-fast)

Job `app-guard` en `.github/workflows/ocm-ci.yml`, con `needs: architecture` (un fallo de
arquitectura ya salta todo lo demás):

1. `pytest tests/architecture/test_app_layer_guard.py` — guard AST R12–R16.
2. `python scripts/backtest_app_guard.py` — sensibilidad histórica (`fetch-depth: 0` en el
   checkout para que el backtest pueda `git archive` los snapshots).
3. `mypy apps/ --no-incremental` — tipado de la capa de aplicación (complementa el
   `mypy shared/` del job `quality`).

## 6. Trazabilidad

| Artefacto | Rol |
|-----------|-----|
| `scripts/app_layer_guard.py` | Guard AST (13 checks, tags `Rxx/AUDIT-2026-08-03#Hx`) |
| `scripts/backtest_app_guard.py` | Gate de confianza contra el historial |
| `tests/architecture/test_app_layer_guard.py` | Pruebas pos/neg por regla (42) |
| `architecture/importlinter.toml` | Contratos BC-53/54 |
| `.github/workflows/ocm-ci.yml` | Job `app-guard` (fail-fast) |
| `docs/architecture/decisions/ADR-0015-…md` | Decisión de arquitectura |
| `docs/plans/tracking.yaml` | Reglas R12–R16 (`backtest: ok`, `activada_en_ci: true`) |

## 7. Deuda consciente

- El guard lee archivos `.py` vía `ast.parse`: no ve código generado, imports dinámicos no
  literales (`importlib`), ni archivos sin extensión `.py`.
- BC-53/54 no cubren lazy imports (los cubre el guard); `must_import` no existe en
  import-linter 2.x y lo garantiza solo el guard.
- Si se reintroduce un helper de scaffolding que ya no existe en `_bootstrap.py`, el guard de
  "una sola fuente" no puede detectar duplicación — solo la detección de reintroducción de
  un helper legítimo.
