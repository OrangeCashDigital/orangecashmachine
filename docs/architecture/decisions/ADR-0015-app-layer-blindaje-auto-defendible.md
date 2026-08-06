# ADR-0015: Blindaje de la Application Layer — reglas auto-defendibles (guard AST + contratos BC-53/54)

**Estado:** Aceptado y verificado en código (guard + backtest + CI verdes)
**Fecha:** 2026-08-06
**Bounded context(s) afectado(s):** ocm (plataforma) — Application Layer: `apps/app` (CLI) y `apps/api` (gateway)

## Contexto

La auditoría de composición de roots `AUDIT-apps-2026-08-03` (serie `AUDIT-apps-2026-08-03#Hx`,
distinta de `INFORME-2026-08-06#H-01…H-22`) encontró cinco defectos estructurales en la
Application Layer, corregidos a mano en `cdd7e7e` y `2717d06`:

- **H1** — `use_cases` construían/leían config desde `argparse.Namespace` y constructores de
  config (el Namespace moría en el borde CLI; `use_cases` debe recibir config tipada).
- **H4** — `getattr` con default en `use_cases` (config tipada es la única fuente de verdad).
- **H6** — `SILENT_PATHS` redefinido localmente en los middleware y probes no excluidas antes
  de procesar (degradación bajo carga).
- **H8** — scaffolding de CLI (logging, sigterm, ensamblado de config, `logger.remove`)
  duplicado fuera de una única fuente.
- **H12** — `CycleRunResult` redefinido (`LiveRunResult`/`PaperRunResult` duplicados).

La corrección manual es necesaria pero insuficiente: sin un mecanismo estructural, cualquier
refactor futuro puede **reintroducir** los mismos anti-patrones y la calidad vuelve a depender
de disciplina humana. El PLAN-Maestro-Ingenieria.md §6 fija el principio: *todo bug corregido
pasa a ser una regla permanente verificable por CI*.

## Alternativas evaluadas

1. **Solo contratos import-linter.** Descartada. Tres carencias verificadas: (a) no ve
   *lazy imports* a nivel de función (la Application Layer los usa deliberadamente; E402 en
   `apps/app/cli/`); (b) import-linter 2.x no soporta contratos `must_import` (la lista de
   tipos válidos es forbidden/layers/independence/protected); (c) reglas como "una sola
   definición", "ubicación exacta" o complejidad ciclomática no son expresables como contrato.
2. **Solo tests unitarios de regresión.** Descartada. Prueban el *output*, no la *estructura*;
   requieren añadir un test por cada regresión y no se ejecutan sobre código nuevo.
3. **Guard AST + contratos para imports de módulo + backtest histórico + gate CI.**
   **Elegida.** El guard escanea el TEXTO FUENTE vía AST (ve imports lazy y definiciones en
   cualquier anidamiento); los contratos BC-53/54 son la primera línea para imports de módulo
   que import-linter sí puede ver; el backtest demuestra sensibilidad contra los snapshots
   pre-fix; el job CI `app-guard` lo hace fail-fast.

## Decisión

Convertir los cinco hallazgos en **reglas permanentes y auto-defendibles**, gobernadas por un
guard AST (`scripts/app_layer_guard.py`) con prueba positiva y negativa por regla, contratos
de frontera para lo que import-linter sí ve, un backtest contra el historial y un gate de CI:

| Regla | Hallazgo | Mecanismo que la hace cumplir | Artefacto de prueba |
|-------|----------|-------------------------------|---------------------|
| R12 | H1 | Guard AST: sin `argparse`, `Namespace`, `vars()` ni constructores de config en `app/use_cases` + **BC-53** (forbidden `app.use_cases → argparse`) | `test_app_layer_guard.py::TestNoArgparseInUseCases` etc. |
| R13 | H4 | Guard AST: sin `getattr` con default en `app/use_cases` | `TestNoGetattrDefaultInUseCases` |
| R14 | H8 | Guard AST: scaffolding de CLI en una sola fuente (`app/cli/_bootstrap.py`) — sin sigterm/`logger.remove`/helpers duplicados fuera; `main()` con complejidad ≤ 20; `live_hydra`/`paper_hydra` DEBEN importar `_bootstrap` + **BC-54** (forbidden `app.use_cases → app.cli`) | `TestNoSigtermOutsideBootstrap`, `TestCliMustImportBootstrap`, ... |
| R15 | H12 | Guard AST: un único `CycleRunResult` en `app/use_cases/run_result.py` | `TestRunResultSingleSource` |
| R16 | H6 | Guard AST: `SILENT_PATHS` solo en `api/middleware/__init__.py`, middleware importan el SSOT y excluyen probes antes del primer `await` | `TestSilentPathsSingleSource`, `TestMiddlewareExcludesProbes` |

Mecánica de verificación (todas en verde en HEAD):

1. `scripts/app_layer_guard.py` — 13 checks AST, etiquetados `Rxx/AUDIT-2026-08-03#Hx`.
2. `tests/architecture/test_app_layer_guard.py` — 42 casos pos/neg; el master positivo
   `guard_app(ROOT) == []` garantiza HEAD limpio.
3. `scripts/backtest_app_guard.py` — extrae `apps/` de los parents pre-fix vía `git archive`
   y exige: `39687e7` → dispara R12–R16, `cdd7e7e` → solo R16, HEAD → 0. Cero falsos
   negativos y cero falsos positivos.
4. Contratos BC-53/54 en `architecture/importlinter.toml` (49 contratos, 0 broken).
5. Job CI `app-guard` (fail-fast tras `architecture`): guard tests + backtest + `mypy apps/`
   con `fetch-depth: 0` para el historial.

## Justificación técnica

- **AST por texto es la única vista completa de la capa.** Los lazy imports de la
  Application Layer (E402 deliberado) son invisibles para el grafo de módulos de
  import-linter; el guard los ve por construcción.
- **Dos líneas de defensa complementarias.** BC-53/54 (forbidden) bloquean la dirección de
  import a nivel módulo en el grafo que import-linter sí analiza; el guard cubre el resto
  (lazy, ubicación de definiciones, complejidad, must-import). Ninguna de las dos por sí sola
  cubre el 100%.
- **El backtest elimina la duda sobre utilidad.** No se confía en que la regla "habría
  detectado" el bug: se demuestra ejecutándola sobre los snapshots pre-fix exactos
  (los parents de los commits de fix), de modo que un guard insensible o un falso positivo
  bloquean el merge.
- **Fail-fast sin excepciones** (mismo patrón que `architecture`): un contrato roto o una
  violación del guard saltan todos los jobs aguas abajo.

## Consecuencias

- **Más fácil:** añadir una regla nueva = check AST + prueba pos/neg + línea en `CHECKS` +
  expectativa en el backtest; no hace falta tocar contrato ni CI para reglas nuevas.
- **Deuda aceptada conscientemente:** el guard lee archivos `.py` vía `ast.parse`; no ve
  código generado, imports dinámicos no literales (`importlib`), ni archivos sin `.py`.
  Los contratos BC-53/54 no cubren lazy imports (lo cubre el guard). `must_import` no existe
  en import-linter 2.x → el must-import de `_bootstrap` lo garantiza solo el guard.
- **Contratos que hacen cumplir esta decisión:** BC-53 (`app.use_cases → argparse` forbidden),
  BC-54 (`app.use_cases → app.cli` forbidden), más los checks del guard y el job CI
  `app-guard`.
- La numeración no colisiona: máximo ADR previo = 0014, máximo BC previo = 52.

## Referencias

- Código: `scripts/app_layer_guard.py`, `scripts/backtest_app_guard.py`,
  `tests/architecture/test_app_layer_guard.py`, `architecture/importlinter.toml` (BC-53/54),
  `.github/workflows/ocm-ci.yml` (job `app-guard`).
- Fuentes: `AUDIT-apps-2026-08-03` (hallazgos H1/H4/H6/H8/H12),
  `docs/PLAN-Maestro-Ingenieria.md` §6 (bug corregido → regla permanente),
  `docs/architecture/BLINDAJE-APPS-2026-08-06.md` (reporte del blindaje).
- ADRs relacionados: ADR-0007 (equivalencia de capas), ADR-0003 (composition roots),
  BC-51 (encapsular Hydra en `ocm.config`).
