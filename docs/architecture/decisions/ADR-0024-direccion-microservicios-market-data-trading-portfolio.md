# ADR-0024: Dirección arquitectónica hacia microservicios — estado real de market-data, trading y portfolio

## Estado

Propuesto (auditado contra código real el 13-ago-2026).

## Contexto

`docker-compose.yml` define un profile `microservices` con tres servicios
candidatos a proceso independiente: `market-data`, `trading`, `portfolio`.
Hasta ahora esa intención arquitectónica no estaba documentada en ninguna
ADR — solo existía implícita en la infraestructura Docker. Este documento
la formaliza y fija el estado real de madurez de cada bounded context,
verificado contra el código, no contra el diseño aspiracional.

Evidencia verificada en código:

**market-data — IMPLEMENTADO como proceso independiente.**
`packages/market_data/main.py` es una app FastAPI completa: lifespan con
`build_context()` (Hydra standalone vía `load_appconfig_standalone()`),
`ExecutionGuard`, ingestion loop (`_ingestion_loop`), un stream processor
Kappa paralelo (`_bronze_writer_loop`, Kafka `ohlcv.raw` → Bronze Iceberg,
con dedup L2 vía `RedisCursorStore`), y un `FeedOrchestrator` opcional para
WS feeds. Expone `/health`, `/ready` y
`/ohlcv/{exchange}/{symbol}/{timeframe}` (lee de Silver/Iceberg vía
`StorageFactoryPort`, inyectado en el lifespan). Arranca con
`python -m market_data.main`, confirmado como el comando real en
`docker-compose.yml` — el `Dockerfile:40` referencia
`market_data.orchestration.entrypoint`, módulo que no existe en el repo;
esa es una inconsistencia de Dockerfile, no evidencia de que el servicio
no funcione (compose siempre sobreescribe el `CMD`). No existen tests con
`TestClient` que ejerciten el contrato HTTP — brecha real de cobertura,
tratada aparte en el backlog, no en esta ADR.

**trading — implementado como bounded context embebido; sin proceso HTTP propio.**
`packages/trading/bootstrap/composition_root.py` define
`TradingCompositionRoot`, con interfaz angosta SSOT fijada en ADR-0003:
`__init__(trading, risk, portfolio, guard=None, rebalance_port=None)`,
fail-fast si falta `trading`, `portfolio` o `rebalance_port`.
`assemble_live()` y `assemble_paper()` ensamblan `Strategy` + `RiskManager`
+ `Executor` (`LiveExecutor`/`PaperExecutor`) + `OMS` + `TradingEngine`, y
devuelven `TradingRuntime(engine, portfolio, tracker)`. Este composition
root **ya está en el camino de ejecución real**: es importado y usado
directamente por `apps/app/use_cases/execute_live.py` y
`execute_paper.py`, los use cases que corren `paper_hydra.py`/
`live_hydra.py`. `assemble_live()` incluye el guard de Promotion Rule
(`require_promoted("OrderFilledPayload", "OrderRejectedPayload")`, ADR-0017
§14) y bloquea con `RuntimeError` si `LiveExecutor.IS_STUB` es `True`.
`trading.engine.TradingEngine` es deliberadamente un objeto runtime puro
(ADR-0012): no construye sus dependencias, solo orquesta
`Strategy → OMS`, respetando `ExecutionGuard`. No existe `trading/main.py`
ni `trading/entrypoint.py` — `packages/trading/engine.py` no tiene
`__main__`. `docker-compose.yml` referencia `command: python -m
trading.main`, módulo inexistente; el bloque también define
`MARKET_DATA_URL: http://market-data:8001` como env var, evidencia de que
el diseño ya anticipa que el futuro servicio `trading` consumirá
`market-data` por HTTP en vez de import directo — coherente con BC-50
(trading solo puede importar `market_data` desde
`trading/bootstrap/composition_root.py`, vía `_GoldFeatureSource`/
`GoldReader`, hoy in-process). No existe `config/trading/` — asimetría
real frente a portfolio. Cobertura: 9 archivos en `tests/trading/`
(`test_composition_root.py`, `test_live_executor.py`,
`test_oms_fill_lifecycle.py`, entre otros).

**portfolio — implementado como bounded context embebido; sin proceso HTTP propio.**
`packages/portfolio/bootstrap/composition_root.py` define
`CompositionRoot.assemble(config, capital_usd_override=None)`
(`@dataclass(frozen=True, slots=True)`), que decide
`RedisPositionStore` vs `InMemoryPositionStore` según
`config.integrations.redis.enabled` (misma bandera SSOT que gobierna el
cursor store de market_data — no se introduce bandera nueva) y devuelve
`CompositionRoot(portfolio_service, rebalance_service, redis_client)`.
`PortfolioService` expone `open_position`, `close_position`, `snapshot`,
`open_count`, `total_exposure`, `state`. Confirmado en uso real: tanto
`apps/app/cli/paper_hydra.py:193-213` como `apps/app/cli/live_hydra.py:231-252`
importan `PortfolioCompositionRoot`, llaman `.assemble(config)` (con
`capital_usd_override=cli_args.capital` en live) e inyectan
`portfolio_root.portfolio_service` al motor de trading, cerrando con
`portfolio_root.close()`. `config/portfolio/portfolio.yaml` existe como
config de dominio real. No existe `portfolio/main.py`.
`docker-compose.yml` referencia `command: python -m portfolio.main`,
módulo inexistente. Cobertura: 4 archivos en `tests/portfolio/`
(`test_composition_root.py`, `test_portfolio_service.py`,
`test_rebalance_service.py`, `test_position_store_unicity.py`).

**Gobernanza activa, no aspiracional.**
`architecture/importlinter.toml` tiene contratos vivos y específicos:
BC-12 (`trading.risk` aislado de `trading.execution`), BC-36
(`trading.strategies` aislado de `execution`/`analytics`), BC-13
(`portfolio` aislado de `trading.execution`/`trading.strategies`), BC-43
(adapters de `PositionStore` solo instanciables desde
`portfolio/bootstrap/composition_root`), BC-44 (orden de capas de
portfolio: `bootstrap < infra < services < ports < models`), y BC-50
(`trading` solo importa `market_data` desde
`trading/bootstrap/composition_root`). Estos contratos se ejecutan en CI
y descartan por evidencia la hipótesis de que trading/portfolio sean
código muerto.

**CI no valida el profile `microservices`.**
Ninguno de los workflows en `.github/workflows/` (`ocm-ci.yml`,
`ocm-cd.yml`, `docker-lint.yml`, etc.) referencia `profile`,
`microservices`, `trading.main` ni `portfolio.main`. El profile puede
romperse (y de hecho ya está roto: H-2 de la auditoría 2026-08-13) sin
que ningún pipeline lo detecte.

## Decisión

Se fijan tres niveles de madurez explícitos para distinguir "no
implementado todavía" de "código muerto":

| Nivel | Definición | Componentes hoy |
|---|---|---|
| **NIVEL 1 — Implementado y soportado** | Tiene entrypoint ejecutable propio y arranca de forma autónoma | `market-data` (`python -m market_data.main`), `paper`/`live` (CLIs embebidos) |
| **NIVEL 2 — En construcción (real, gobernado)** | Dominio, composition root, tests y contratos import-linter existen y están en el camino de ejecución real (embebido), pero sin proceso HTTP ni entrypoint independiente | `trading` (vía `TradingCompositionRoot`, usado por `execute_live`/`execute_paper`), `portfolio` (vía `PortfolioCompositionRoot`, usado por `paper_hydra`/`live_hydra`) |
| **NIVEL 3 — Aspiracional/scaffolding** | Existe en infraestructura (Docker) pero no tiene contraparte de código ejecutable | Bloques `trading:`/`portfolio:` del profile `microservices` en `docker-compose.yml` (referencian `main.py` inexistentes) |

Ningún componente de trading o portfolio se clasifica como NIVEL 3/muerto:
ambos tienen composition roots activos en producción embebida, tests de
dominio, y contratos de arquitectura enforced en CI. Lo que falta —
`main.py`, servidor HTTP, `config/trading/` — es trabajo pendiente
explícito hacia NIVEL 1, no evidencia de abandono.

El profile `microservices` de `docker-compose.yml` se conserva íntegro
como documentación ejecutable de la dirección futura, con la salvedad de
que sus bloques `trading:` y `portfolio:` deben llevar un comentario
explícito indicando que son scaffolding no funcional hasta que exista el
entrypoint correspondiente (ver plan de cambios en la auditoría
2026-08-13, cambio propuesto #2).

## Consecuencias

- Un colaborador nuevo (o el propio autor en el futuro) puede leer esta
  ADR y entender sin ambigüedad qué puede ejecutar hoy (`market-data`,
  `paper`, `live`) frente a qué es dirección futura (`trading`/`portfolio`
  como servicios HTTP).
- Se habilita avanzar `trading`/`portfolio` hacia NIVEL 1 de forma
  incremental (crear `main.py` con FastAPI análogo al de market-data,
  crear `config/trading/`) sin que eso implique una reescritura — el
  dominio y el composition root ya son reutilizables tal cual.
- Se recomienda que CI valide al menos `docker compose config --profile
  microservices` (sintaxis, no arranque) para detectar futuras
  divergencias como la de H-1/H-2 antes de que lleguen a producción.
- El Dockerfile (`CMD` apuntando a `market_data.orchestration.entrypoint`
  inexistente) debe corregirse o documentarse como default genérico
  siempre sobreescrito por compose — tratado como ítem independiente en
  el backlog, no como parte de esta decisión arquitectónica.

## Alternativas consideradas

- **Eliminar el profile `microservices` y los bloques `trading:`/
  `portfolio:` hasta que existan sus entrypoints.** Rechazada: destruiría
  documentación ejecutable válida de la dirección arquitectónica y
  environment variables ya bien pensadas (`MARKET_DATA_URL`) sin ganancia
  real, dado que el profile no se activa por defecto y no interfiere con
  el runtime actual.
- **Tratar trading/portfolio como código muerto y planear su
  reescritura.** Rechazada por evidencia: ambos están en el camino de
  ejecución real vía composition roots usados por `paper_hydra.py`/
  `live_hydra.py`/`execute_live.py`/`execute_paper.py`, con tests y
  contratos import-linter activos.

## Referencias

- ADR-0003 (interfaz angosta de `TradingCompositionRoot`)
- ADR-0006 (portfolio posee el estado de posiciones)
- ADR-0011 (delegación de rebalanceo)
- ADR-0012 (TradingEngine como runtime puro)
- ADR-0016 (LiveExecutor real sobre Bybit)
- ADR-0017 §14 (Promotion Rule)
- `architecture/importlinter.toml`: BC-12, BC-13, BC-36, BC-43, BC-44, BC-50
- Auditoría 2026-08-13 — hallazgos H-1 a H-10 sobre inconsistencias de Dockerfile/compose/CI
