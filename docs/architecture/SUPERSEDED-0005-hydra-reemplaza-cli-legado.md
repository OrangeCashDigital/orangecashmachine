# [SUPERSEDED] ADR-0005: Hydra/AppConfig reemplaza a los CLIs argparse-puros en live/paper trading

> **SUPERSEDED (2026-08-06).** Serie heredada colisionaba con `decisions/ADR-0005`
> (`decisions/ADR-0005-trading-engine-internal-external-split.md`). Renombrado a `SUPERSEDED-0005`;
> único SSOT activo: `docs/architecture/decisions/` (PLAN §13 N3). Registro histórico.

> **SERIE HEREDADA (deprecada 2026-08-03).** La serie canónica de ADRs es
> `docs/architecture/decisions/ADR-NNNN-*` (ver `GOVERNANCE.md §3` y §9).
> Este documento se conserva como registro histórico; su numeración no debe
> usarse para referencias nuevas. Nota: la colisión de numeración con
> `decisions/ADR-0005` (split TradingEngine) motivó esta deprecación.
> **Estado de implementación:** los CLIs legacy `apps/app/cli/live.py` y
> `paper.py` se eliminaron el 2026-08-03 (Fase C).

**Estado:** Aceptado
**Fecha:** 2026-08-02
**Enmienda a:** ADR-0003, Decisión 3 (declarada superseded por este documento)
**Contexto del bounded context:** `apps/app/cli/live.py`, `apps/app/cli/paper.py`, `apps/app/cli/live_hydra.py`, `apps/app/cli/paper_hydra.py`, y su relación con `portfolio` (Composition Root del bounded context)

## Contexto

`ADR-0003` (2026-08-01) decidió que `cli/live.py`/`cli/paper.py` debían
permanecer como los CLIs oficiales, adoptando `RunConfig.from_env()` y
`bootstrap_logging()` — infraestructura compartida basada en variables de
entorno planas, explícitamente **sin** migrar a Hydra (Decisión 3, línea
"no exige migrar `live.py`/`paper.py` a configuración Hydra").

Ese ADR no tuvo visibilidad de un desarrollo paralelo: `paper_hydra.py` y
`live_hydra.py` ya existían coexistiendo con los CLIs legados (ver sus
propios docstrings: "Coexiste con `app/cli/paper.py` sin reemplazarlo ni
modificarlo") y, en la sesión de implementación de Fase 3
(2026-08-02), se completó sobre esa base:

- `PortfolioCompositionRoot` (`packages/portfolio/bootstrap/composition_root.py`)
  como Composition Root del bounded context `portfolio` — exactamente el
  patrón que el propio `ADR-0003` (Decisión 1) define como objetivo.
- `PortfolioService` migrado a DIP estricto (`store: PositionStore`
  obligatorio, sin fallback interno a `InMemoryPositionStore`).
- La construcción de `redis_client` migrada fuera de
  `execute_live.py::build_live_engine()` hacia el Composition Root de
  `portfolio` cuando `portfolio_service` viene inyectado — resolviendo
  directamente el hallazgo 3.3 de la auditoría, que el `ADR-0003` listaba
  como consecuencia pendiente ("hoy vive en el lugar equivocado de la
  jerarquía").
- `PortfolioConfig` integrado a `AppConfig`/Hydra (`config/portfolio/portfolio.yaml`),
  bajo el mismo pipeline L1-L5 que ya gobierna el resto de la configuración
  de OCM.
- Contrato de import-linter `BC-43` + suite de tests de arquitectura
  protegiendo que `PositionStore` solo se instancie desde el Composition
  Root.
- Validado end-to-end en sesión real: ciclo completo
  `open_position() -> save() en RedisPositionStore -> snapshot() ->
  close_position() -> delete()` vía `PortfolioCompositionRoot`, con Redis
  autenticado, sin mocks.

Es decir: el trabajo de Fase 3 ya implementó, para `portfolio`, el modelo de
Composition Root jerárquico que el `ADR-0003` describe como objetivo — pero
usando Hydra/`AppConfig` como mecanismo de configuración, no
`RunConfig.from_env()`. Mantener ambos caminos (CLIs legados con
`RunConfig` sin Hydra, y CLIs Hydra con `PortfolioCompositionRoot`) sería
sostener dos implementaciones paralelas del mismo Composition Root
jerárquico, violando SSOT y DRY — el problema exacto que motivó la
auditoría original.

## Decisión

`paper_hydra.py`/`live_hydra.py` se adoptan como los **CLIs oficiales** de
paper/live trading, reemplazando a `cli/paper.py`/`cli/live.py`. Se
supersede la Decisión 3 del `ADR-0003` en su totalidad: la vía de adopción
de infraestructura compartida para live/paper trading es Hydra/`AppConfig`
(ya en uso por `cli/main.py`), no `RunConfig.from_env()`.

Las demás decisiones del `ADR-0003` (Decisión 1 — dos niveles de
Composition Root, Decisión 2 — lifecycle de recursos como responsabilidad
del bounded context) **permanecen vigentes sin cambios** y son las que
Fase 3 implementó para `portfolio`.

## Consecuencias

- `pyproject.toml`: los scripts `live`/`paper` (`[project.scripts]`) pasan
  a apuntar a `app.cli.live_hydra:main`/`app.cli.paper_hydra:main`; se
  retiran los scripts que apuntaban a `app.cli.live:main`/`app.cli.paper:main`.
- `run.sh`: los modos `live`/`paper` pasan a invocar
  `python -m app.cli.live_hydra`/`app.cli.paper_hydra`.
- `apps/app/cli/live.py` y `apps/app/cli/paper.py` se eliminan del árbol de
  código una vez confirmado que ningún otro consumidor (systemd, cron,
  scripts externos a este repo) invoca los módulos legados directamente.
- Los docstrings de `paper_hydra.py`/`live_hydra.py` que dicen "Coexiste
  con `app/cli/paper.py` sin reemplazarlo" quedan obsoletos y se actualizan
  en el mismo cambio que retira los CLIs legados.
- El parámetro opcional `portfolio_service=None` en `execute()`/
  `build_*_engine()` de `execute_live.py`/`execute_paper.py` deja de tener
  un caller real que pase `None` una vez retirados los CLIs legados —
  candidato a eliminarse (hacerlo obligatorio) en un cambio posterior,
  no en este ADR.
- Hallazgos del `ADR-0003`/auditoría que **NO** quedan resueltos por este
  ADR y siguen abiertos, para no sobre-reclamar alcance:
  - **2.3 / 3.1** — `live_hydra.py`/`paper_hydra.py` no han sido
    verificados en esta sesión respecto a manejo explícito de señales
    `SIGINT`/`SIGTERM` (`signal.signal`) ni adopción de
    `bootstrap_logging()`/`ocm.observability` multi-sink. El pipeline
    Hydra sí carga `observability.logging` como parte de `AppConfig`
    (confirmado en los logs L1 de esta sesión), pero eso es carga de
    *configuración*, no necesariamente equivalente al *bootstrapping* de
    logging multi-sink que describe el hallazgo 3.1. Pendiente de
    verificación antes de darlo por resuelto.
  - **3.2** — `MetricsRuntime.shutdown()` en `cli/main.py::run_application()`
    — fuera del alcance de este ADR (no toca `cli/main.py`).

## Alternativas consideradas

- **Mantener el ADR-0003 tal cual, retirar `*_hydra.py`**: rechazado —
  descartaría trabajo ya implementado, validado end-to-end, y alineado con
  el resto de la arquitectura Hydra-first de OCM (`cli/main.py` ya usa
  Hydra); habría que reconstruir en `RunConfig.from_env()` algo que
  `PortfolioCompositionRoot` ya resuelve.
- **Coexistencia indefinida de ambos caminos**: rechazado — viola SSOT/DRY,
  y es el estado que ya generaba confusión (dos CLIs por modo, sin que
  ninguno estuviera marcado como el oficial).

Principios: SSOT · DRY · KISS · Composition Root · Fail-Fast
