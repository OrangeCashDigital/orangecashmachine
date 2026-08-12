# ADR-0002: Arquitectura objetivo event-driven para OCM

> **SERIE HEREDADA (deprecada 2026-08-03).** La serie canónica de ADRs es
> `docs/architecture/decisions/ADR-NNNN-*` (ver `GOVERNANCE.md §3` y §9).
> Este documento se conserva como registro histórico; su numeración no debe
> usarse para referencias nuevas.

**Estado:** Aceptado (parcial — ver sección "Pendiente de decisión")
**Fecha:** 2026-07-25
**Contexto del bounded context:** `market_data` (y su relación con futuros `control_plane`, `trading`)

> **Nota de estado (2026-08-02):** Ver el log de verificación
> `docs/architecture/logs/verificacion-adrs-vs-codigo-2026-08-02.md`, que supersede la Decisión 4 y el Pendiente #1 de este documento. Dagster fue eliminado por completo del código (commit `9eb6de3`, posterior a este ADR) — la Decisión 4 de abajo ("Dagster permanece instalado y disponible...") ya no refleja el sistema real. Dicha eliminación incluye su retirada de `docker-compose` (verificado 2026-08-05). El Pendiente #1 (¿systemd timers alcanza?) quedó resuelto de facto por la eliminación. La ruta `infrastructure/event_bus/` mencionada en la Decisión 2 también está corregida en el log a su ubicación real.

> **Nota de estado (2026-08-10) — F-031/B-46:** La afirmación del Contexto de que
> la migración Kappa "está completa para el pipeline REST actual (`OHLCVPipeline`
> con `IncrementalStrategy`, `BackfillStrategy`, `RepairStrategy`)" **ya no se
> sostiene**: en HEAD, `OHLCVPipeline` hardcodea `NullPublisher()` como publisher
> (ohlcv_pipeline.py:248), `_chunk_converter` no se inyecta (runtime.py:298) y
> `_build_kafka_publisher` (pipeline_factory.py:156) no tiene callers, por lo que
> `ctx.publisher.publish_chunk()` de las strategies incremental/backfill nunca
> llega a un productor Kafka real — de hecho hoy fallan con `RuntimeError` en
> `get_chunk_converter()` ANTES de publicar (incremental.py:106, backfill.py:427).
> Solo los datos aceptados por el quality gate pasan a publicar, pero el destino
> de esa publicación es un Null publisher: la invariancia "solo datos aceptados
> llegan a Kafka" no se está materializando en `ohlcv.raw`. Registrado en
> F-031/B-46 (docs/audits/2026-08-08-streaming-canary-audit.md, docs/plans/
> tracking.yaml); este documento es serie heredada y se conserva como registro
> histórico, no actualiza su cuerpo.

## Contexto

OCM comenzó como un pipeline REST/CCXT con orquestación Prefect y luego Dagster,
migrando progresivamente hacia Kappa architecture: todo el flujo OHLCV pasa por
`ohlcv.raw` (Kafka) antes de llegar a Bronze (Iceberg). Esta migración está
completa para el pipeline REST actual (`OHLCVPipeline` con `IncrementalStrategy`,
`BackfillStrategy`, `RepairStrategy`), pero el roadmap original also contemplaba
un `Trade Aggregator` alimentado por Cryptofeed (feed WebSocket en vivo) que
todavía no existe en el código.

Durante una auditoría exhaustiva del repositorio (no una sesión de diseño desde
cero) se verificaron varias piezas del roadmap que estaban documentadas pero no
implementadas, o implementadas parcialmente:

- `EventBusPort` / `InMemoryEventBus` existían con diseño correcto (DIP, Protocol
  `runtime_checkable`) pero nunca estaban cableados a nada.
- El contrato BC-28 (licencia arquitectónica amplia para `pipeline_factory.py`)
  había sido retirado en un commit anterior, dejando un docstring desactualizado.
- Dagster está instalado (`docker-compose`) pero inactivo (`stopped`) al momento
  de este análisis.

Esta auditoría cerró, además, cuatro inconsistencias reales de SSOT (tópico Kafka
duplicado `market.trades.raw`/`trades.raw`, contrato BC-39 apuntando a un módulo
fantasma, migración pandas→polars incompleta en `derivatives_storage.py`, y
función muerta `align_to_grid` con re-exports huérfanos) — documentadas en el
historial de commits, no repetidas aquí.

## Decisión 1 — Kafka es el único SSOT cross-proceso

Todo evento de negocio que deba sobrevivir un reinicio de proceso, o que deba
ser consumido por más de un proceso, vive en Kafka. Ningún otro mecanismo
(bus in-process, SQLite de lineage, cache Redis) es fuente de verdad — son
proyecciones o índices secundarios reconstruibles desde Kafka.

**Consecuencia práctica confirmada en código:** `IncrementalStrategy._run` y
`BackfillStrategy` ya siguen este patrón — `ctx.quality.run()` corre antes de
`ctx.publisher.publish_chunk()`; solo datos aceptados llegan a Kafka. Cuando se
construya el feed en vivo (Cryptofeed), debe replicar la misma disciplina:
Cryptofeed → Normalizer → Quality Engine → Kafka, nunca al revés.

## Decisión 2 — El EventBus in-process NO es Kafka y no compite con él

`EventBusPort` / `InMemoryEventBus` (en `infrastructure/event_bus/`) es un
mecanismo de notificación estrictamente intra-proceso, pub/sub síncrono,
sin persistencia ni cruce de fronteras de proceso. Su rol es desacoplar
sub-responsabilidades dentro de un mismo proceso — hoy, exclusivamente,
el observador aditivo de calidad (`QualityPipelineConsumer`) que registra
lineage sobre chunks ya publicados a Kafka.

**No se usa** para transportar datos de mercado entre componentes que
corren en procesos distintos. Ese rol es exclusivo de Kafka.

**Distinción importante — dos "Quality" en el sistema:**
1. **Quality gate síncrono** (`ctx.quality.run`, dentro de las strategies) —
   bloqueante, corre ANTES de publicar a Kafka, decide si el chunk se publica.
2. **`QualityPipelineConsumer`** (vía EventBus) — observador POST-HOC, corre
   DESPUÉS de que el chunk ya fue publicado y aceptado; registra métricas de
   lineage más detalladas. Nunca vetorial, nunca bloqueante.

No son competidores ni redundantes — son capas complementarias con
responsabilidades distintas y momentos de ejecución distintos.

**Rename pendiente (no bloqueante):** el nombre `EventBusPort`/`EventBus`
induce a confundirlo con un backbone de eventos de negocio. Un rename futuro
a algo como `LocalDomainEventDispatcher` clarificaría esto, pero no es
prioritario y se trata como ítem de deuda técnica separado, no como parte de
esta decisión.

## Decisión 3 — Control Plane y Data Plane separados

**Data Plane** — procesa el mercado en vivo. Nunca depende de un orquestador
externo para su operación normal: Exchange → Feed Handler → Normalizer →
Quality → Kafka → Consumers (Iceberg, Indicators, Strategy, Observability).

**Control Plane** — opera el sistema, no procesa datos de mercado. Responsable
de: iniciar/detener backfills, disparar replays desde un offset arbitrario,
reparar datos, consultar estado, rotar credenciales, programar tareas
periódicas de mantenimiento.

Prueba de diseño: si un método de `control_plane/` toca directamente un
`DataFrame` de precios o calcula algo sobre datos de mercado, está mal
ubicado — pertenece al Data Plane.

**Ubicación propuesta (no implementada todavía):** `packages/control_plane/`
con `replay.py`, `backfill.py`, `repair.py`, `scheduler.py`, `cli.py` como
entrypoint único (composition root propio, mismo patrón que
`pipeline_factory.py` para `market_data`).

## Decisión 4 — Rol de Dagster: relegado al Control Plane, nunca al Data Plane

Ninguna responsabilidad de Dagster desaparece al retirarlo del camino
crítico — se redistribuye:

| Responsabilidad              | Con Dagster        | Reemplazo en Control Plane        |
|-------------------------------|---------------------|-------------------------------------|
| Transportar eventos            | (no era su rol)     | Kafka                                |
| Replay de eventos               | Dagster Job          | Replay Consumer + CLI                |
| Backfill histórico               | Dagster Job          | Backfill Service + CLI               |
| Reparación de datos               | Dagster Job          | Repair Service + CLI                 |
| Programar tareas periódicas         | Dagster Scheduler    | systemd timers (cron si es trivial)  |
| Reintentos de consumers Kafka        | Dagster              | Kafka + lógica del consumer (ver pendiente) |
| Persistir en Iceberg                  | Dagster Asset        | Iceberg Consumer                     |
| Observabilidad                          | Dagster UI            | Prometheus + Grafana + Loki (stack ya instalado, hoy inactivo) |

Dagster permanece instalado y disponible como una opción viable para el
Control Plane si, al diseñar `scheduler.py`, se determina que existen
dependencias condicionales reales entre jobs (ver pendiente de decisión).
No se elimina el servicio del `docker-compose.yml` hasta que esa decisión
esté tomada explícitamente.

## Pendiente de decisión (no resuelto — requiere respuesta explícita antes de implementar)

1. **Dependencias condicionales en `scheduler.py`:** ¿existe o existirá algún
   caso donde un job de mantenimiento deba esperar a que termine otro
   (ej. "no correr repair hasta que termine la validación de completitud")?
   Si la respuesta es sí, `systemd timers` no alcanza y Dagster (u otro
   orquestador ligero) conserva un rol real en el Control Plane. Si la
   respuesta es no, `systemd timers` es suficiente y más simple (KISS).

2. **Estrategia de resiliencia para consumers Kafka:** offset commit strategy
   (at-least-once vs at-most-once), dead-letter queue para mensajes que
   fallan repetidamente, y backoff sin bloquear el resto del partition. Esto
   es trabajo nuevo — no una migración de la lógica de retry que ya existe
   en `classify_error`/`ExchangeCircuitOpenError` (esa lógica resuelve
   reintentos dentro de una ejecución REST, no reintentos de consumer Kafka).

3. **`RepairStrategy` — ¿migra a emitir eventos correctivos?** Hoy escribe
   directo a Iceberg vía `ctx.storage.save_ohlcv()` (documentado como
   excepción deliberada: "no es market truth, es maintenance"). Migrar a un
   evento `market.data.repaired` que los consumers reprocesan sería más fiel
   a Kappa puro, pero requiere que el consumer de Iceberg soporte upsert por
   clave natural — es un proyecto separado, no un rename.

## Consecuencias

- El pipeline REST/backfill actual (`OHLCVPipeline` + 3 strategies) no
  cambia su comportamiento — el wiring del EventBus es aditivo y fail-soft
  (ver commits de wiring, `PipelineContext.event_bus` opcional, default `None`).
- Cuando se construya el feed en vivo (Cryptofeed + Aggregator), debe
  seguir el mismo orden Quality→Kafka ya vigente en el pipeline REST — no
  se introduce un patrón nuevo ("Quality como consumer downstream") sin
  una decisión explícita adicional, dado el riesgo de que datos no
  validados lleguen al SSOT.
- `packages/control_plane/` es un bounded context nuevo, todavía no
  implementado — este ADR fija su alcance antes de escribir código.
- El rename de `EventBusPort` queda como ítem de deuda técnica documentado,
  no urgente.

## Referencias

- Kreps, Jay. "Questioning the Lambda Architecture" (Kappa architecture).
- Seemann, Mark. *Dependency Injection in .NET*, capítulo Composition Root.
- Martin, Robert C. *Clean Architecture*, capítulo 26.
- `architecture/importlinter.toml` — contratos BC-05, BC-07, BC-38, BC-42
  (fuente de verdad ejecutable para las reglas de capas).
