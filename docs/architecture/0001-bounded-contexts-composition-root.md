# ADR-0001: Mapa de bounded contexts y aplicación del Composition Root jerárquico

**Estado:** Aceptado
**Fecha:** 2026-08-02
**Contexto del bounded context:** Todo el repositorio

> **Nota de estado (2026-08-02):** Ver ADR-0006. Los siguientes elementos descritos abajo como Decisión están **pendientes de implementación**, no materializados: `control_plane` (bounded context, sin paquete propio), `build_market_data()`/`build_trading()` (no existen), y el Composition Root General único en `apps/` (cada entrypoint es hoy su propio CR — permitido por `ADR-0003`, pero no lo que este mapa describe). `build_portfolio()` sí tiene equivalente real: `PortfolioCompositionRoot.assemble()`.

## Contexto

`ADR-0003` formalizó el patrón de Composition Root jerárquico en abstracto:
un Composition Root General que ensambla bounded contexts sin conocer su
interior, y un Composition Root por bounded context que sí conoce los
detalles y gestiona el lifecycle de sus recursos. Ese ADR no enumeraba
cuáles son los bounded contexts reales de OCM ni qué construye cada uno.
Este documento cierra ese vacío: es el mapa concreto, la aplicación
práctica de `ADR-0003` al sistema tal como existe hoy.

## Decisión

### Bounded contexts de OCM

- **`market_data`**: adquisición, calidad y almacenamiento de datos de
  mercado (Bronze/Silver/Gold, Iceberg, Kafka).
- **`trading`**: estrategias, gestión de riesgo, ejecución de órdenes.
- **`portfolio`**: estado de posiciones, rebalanceo (`ADR-0004`).
- **`control_plane`**: coordinación operativa transversal (healthchecks,
  shutdown, observabilidad).
- **Entrypoints (`api/`, `cli/`)**: no son un bounded context de dominio;
  son la capa de entrada que invoca al Composition Root General.

### Composition Root General

Vive en el nivel más alto de `apps/`. Su única responsabilidad es conocer
que existen los bounded contexts y llamar a su función de ensamblaje —
`build_market_data()`, `build_trading()`, `build_portfolio()` — conectando
los objetos resultantes. No importa ni instancia nada interno de ningún
bounded context: si `portfolio` migra de Redis a PostgreSQL, el
Composition Root General no cambia una línea, porque siempre invoca
`build_portfolio()` y recibe el mismo tipo de objeto.

### Composition Root de cada bounded context

Cada uno es el único lugar donde se instancian dependencias concretas de
su dominio:

- **`build_market_data()`**: `CCXTAdapter`, `QualityChecker`, pipelines,
  `PipelineOrchestrator`, publicadores Kafka.
- **`build_trading()`**: estrategia, `RiskManager`, OMS, `TradingEngine`,
  callbacks asociados.
- **`build_portfolio()`**: `RedisPositionStore`, `PortfolioService`, y
  todo lo relacionado a persistencia de posiciones.

### Lifecycle de recursos

Todo Composition Root interno que abre un recurso que requiere cierre
explícito (conexión Redis, productor Kafka, cliente HTTP) debe devolverlo
o gestionarlo, para que quien lo invocó pueda cerrarlo en `try/finally`
incluso ante excepción o `SIGINT`/`SIGTERM`. Ejemplo ya implementado:
`build_live_engine()` devuelve `LiveEngineResources` (incluyendo
`redis_client`) en vez de código muerto, corregido en el commit `6f4ff38`
de esta misma sesión.

## Consecuencias

- Un cambio de dependencia dentro de `market_data` nunca toca el
  Composition Root General.
- Cambiar la tecnología de persistencia de `portfolio` no requiere tocar
  nada fuera de `build_portfolio()`.
- Cada bounded context puede evolucionar de forma independiente sin
  afectar al resto del sistema — la frontera es mecánica, no solo de
  intención, y queda verificable vía `lint-imports` (`ADR-0000`, Principio 3).

## Alternativas consideradas

- **Un único Composition Root plano para todo el sistema**: rechazado —
  ya implícito en `ADR-0003`; mezclar el ensamblaje general con el detalle
  técnico de cada bounded context viola el Principio 3 (Clean Architecture)
  de `ADR-0000`.
