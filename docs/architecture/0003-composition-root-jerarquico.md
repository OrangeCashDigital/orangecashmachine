# ADR-0003: Composition Root jerárquico en OCM

> **SERIE HEREDADA (deprecada 2026-08-03).** La serie canónica de ADRs es
> `docs/architecture/decisions/ADR-NNNN-*` (ver `GOVERNANCE.md §3` y §9).
> Este documento se conserva como registro histórico; su numeración no debe
> usarse para referencias nuevas. Nota: la colisión de numeración con
> `decisions/ADR-0003` (constructor angosto) motivó esta deprecación.

**Estado:** Aceptado — Decisión 3 reemplazada por `ADR-0005` (ver nota abajo)
**Fecha:** 2026-08-01
**Contexto del bounded context:** `apps/` (todos los entrypoints: `cli/main.py`, `cli/live.py`, `cli/paper.py`, `apps/api/main.py`, `apps/research/`), y su relación con `market_data`, `trading`, `portfolio`, `control_plane`

## Contexto

`docs/audits/2026-08-composition-root-audit.md` identificó que `apps/` no sigue
un modelo único de Composition Root: coexisten tres niveles de madurez
(`apps/api/main.py` con lifecycle completo, `cli/main.py` con DI/Config pero sin
shutdown de `MetricsRuntime`, `cli/live.py`/`cli/paper.py` sin reutilizar
infraestructura compartida) sin que la asimetría estuviera documentada. Esto
generó, entre otros, tres hallazgos críticos: duplicación de `on_fill_composite`
entre `execute_live.py`/`execute_paper.py` (2.2, ya resuelto), ausencia de
lifecycle/shutdown en procesos con capital real (2.3, ya resuelto), y un
`rebalance.py` huérfano que construye sus propias dependencias inline sin que
nada en el grafo de llamadas lo invoque (2.1, pendiente).

El fix de 2.3 (`LiveEngineResources` en `apps/app/use_cases/execute_live.py`)
ya implementa, de facto, el patrón de lifecycle que este ADR formaliza: un
objeto construido por el bounded context que expone `shutdown()` para que el
llamador lo cierre en un `finally`. Este ADR generaliza ese patrón y lo hace
explícito para todo `apps/`, en vez de dejarlo como una solución puntual de
`trading`/`portfolio`.

## Decisión

### Decisión 1 — Dos niveles de Composition Root

OCM adopta una jerarquía de dos niveles, con responsabilidades disjuntas:

**Composition Root General** (`apps/app/composition_root.py`, o equivalente
por entrypoint): ensambla bounded contexts completos sin conocer sus detalles
internos. Su única responsabilidad es invocar `build_market_data()`,
`build_trading()`, `build_portfolio()`, `build_control_plane()` — o el
subconjunto que cada entrypoint necesite — y conectar los objetos que cada
una devuelve. No sabe si `portfolio` usa Redis o PostgreSQL, ni si `trading`
usa una estrategia EMA o de otro tipo.

**Composition Root por bounded context** (uno por dominio, p. ej.
`trading/composition_root.py`, `portfolio/composition_root.py`): es el único
lugar donde se instancian las dependencias concretas de su dominio —
adaptadores, estrategias, repositorios, clientes de infraestructura — hasta
devolver un único objeto listo para usar al Composition Root General.

| Nivel | Conoce | No conoce |
|---|---|---|
| General | Que existen `market_data`, `trading`, `portfolio`, `control_plane` y sus firmas `build_*()` | Cómo se construye cada uno por dentro |
| Por bounded context | Todos los detalles técnicos de su propio dominio (adaptadores, config, credenciales) | Detalles internos de los demás bounded contexts |

### Decisión 2 — Lifecycle de recursos es responsabilidad del bounded context

Cada Composition Root de bounded context es responsable del ciclo de vida de
los recursos que abre durante su construcción (conexión Redis, productor
Kafka, cliente HTTP, etc.). Si `build_portfolio()` abre una conexión Redis,
debe devolverla (o un objeto que la envuelva) para que el código que invocó
la construcción pueda cerrarla explícitamente en un `try`/`finally`, incluso
ante excepción o `SIGINT`/`SIGTERM`.

Precedente ya implementado: `LiveEngineResources` en
`apps/app/use_cases/execute_live.py` (commit `6f4ff38`). Este patrón se
generaliza como el estándar para cualquier `build_*()` que abra recursos
externos, no solo para `trading`/`portfolio`.

> **Nota de enmienda (2026-08-02):** Esta Decisión 3 fue reemplazada por
> `ADR-0005`. `live.py`/`paper.py` no adoptan `RunConfig.from_env()` —
> se retiran en favor de `live_hydra.py`/`paper_hydra.py`, que ya
> resuelven el mismo objetivo (infraestructura compartida, Composition
> Root jerárquico) vía Hydra/`AppConfig`. El texto original de esta
> sección se conserva sin editar como registro histórico de la decisión
> tal como fue aceptada en su momento.
>
### Decisión 3 — Adopción de infraestructura compartida en `live.py`/`paper.py` (SUPERSEDED)

Resuelve el hallazgo 3.1 de `docs/audits/2026-08-composition-root-audit.md`
(anteriormente referenciado como pendiente en "ADR-0002", corregido a este
documento).

`cli/live.py`/`cli/paper.py` adoptan `RunConfig.from_env()` y
`bootstrap_logging()` de `ocm.config`/`ocm.observability` en lugar de
`argparse` plano y `logger.add()` duplicado línea por línea entre ambos
archivos. **No** adoptan `MetricsRuntime`: ese componente está diseñado para
procesos continuos (`cli/main.py` corre como servicio de larga duración),
mientras que Trading/Portfolio corren como ciclo puntual (`run_once()`) sin
un loop de métricas persistente que justifique su overhead.

`RunConfig.from_env()` no depende de Hydra — resuelve desde variables de
entorno planas, por lo que esta adopción no exige migrar `live.py`/`paper.py`
a configuración Hydra.

## Consecuencias

- Se requiere crear `build_market_data()`, `build_trading()`,
  `build_portfolio()` como funciones públicas de cada bounded context,
  siguiendo el contrato "recibe config resuelta, devuelve objeto + recursos
  para cierre".
- `cli/live.py`, `cli/paper.py`, `cli/main.py` y `apps/api/main.py` migran
  progresivamente a invocar solo el Composition Root General — dejan de
  construir `RedisPositionStore`, `TradingEngine`, adaptadores concretos, etc.
  directamente.
- La construcción de `redis_client` (hallazgo 3.3) migra de
  `execute_live.py::build_live_engine()` hacia el Composition Root de
  `portfolio` — hoy vive en el lugar equivocado de la jerarquía.
- El destino de `rebalance.py` (hallazgo 2.1) queda desbloqueado por este
  ADR: conectarlo implica que consuma `build_trading()` + `build_portfolio()`
  igual que `live.py`/`paper.py`, no que siga construyendo sus dependencias
  inline.
- `MetricsRuntime.shutdown()` sin invocar en `cli/main.py::run_application()`
  (hallazgo 3.2) se corrige de forma natural cuando ese entrypoint también
  adopte el patrón de lifecycle de la Decisión 2.

## Alternativas consideradas

- **Composition Root único y plano** (todo `apps/` construye todo
  directamente, sin capas): rechazado — es el estado actual que motivó la
  auditoría; no escala y ya produjo divergencia entre entrypoints.
- **Framework de inyección de dependencias** (contenedor DI de terceros):
  rechazado por KISS — OCM no usa contenedores DI en ningún otro punto del
  código, y el problema real no es la mecánica de inyección sino la falta de
  una frontera clara entre "ensamblar el sistema" y "ensamblar un dominio".
- **Una única función `build_everything()`**: rechazado — viola el
  aislamiento entre bounded contexts que el resto de la arquitectura (BC-13,
  contratos de `lint-imports`) ya impone.

## Pendiente de decisión

- Orden de migración de entrypoints existentes (`live.py`/`paper.py` primero
  vs. `cli/main.py` primero) — no bloquea este ADR, se decide como trabajo
  de implementación.
- Ubicación exacta de `build_*()` por bounded context (módulo dedicado vs.
  `__init__.py` del paquete) — pendiente de precedente en el primer bounded
  context que se migre.
- Destino final de `rebalance.py` (conectar vs. eliminar) — hallazgo 2.1,
  sigue abierto como decisión de producto, no de arquitectura.

Principios: SRP · DIP · SSOT · KISS · SafeOps
