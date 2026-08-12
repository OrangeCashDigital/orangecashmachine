# ADR-0013: Modelo unificado de ingestión de datos — feed, fuente y mecanismo

**Estado:** Aceptado
**Fecha:** 2026-08-05
**Bounded context(s) afectado(s):** market_data, shared, y cualquier futuro
bounded context de ingestión de datos.

## Contexto

Durante el refactor de `CompositionRoot` de `market_data` (eliminación de
la lectura manual de `feeds.yaml` en favor de `AppConfig.feeds`) surgió la
necesidad de precisar formalmente qué es un feed dentro de OCM, motivada
por un caso concreto: incorporar índices de mercado (BTC Dominance,
Altcoin Season Index) para estrategias de rotación de capital, que no
encajan en el modelo de `FeedsConfig`/`FeedOrchestrator` tal como existe
hoy.

El desarrollo completo de este modelo conceptual vive en
`docs/architecture/feed-model.md`, que este ADR adopta como documento
normativo. Este ADR no repite esa explicación — solo registra las
decisiones concretas.

## Alternativas evaluadas

1. **Extender `FeedsConfig`/`FeedOrchestrator` para cubrir también
   polling/índices compuestos.** Rechazada: mezclaría dos mecanismos de
   ingestión con volumen y naturaleza distintos (ver `feed-model.md` §3)
   bajo una misma abstracción diseñada para streaming persistente.
2. **Dejar el concepto de feed sin definición formal, resolviendo caso
   por caso.** Rechazada: ya generó ambigüedad real (confundir exchange
   con feed, forzar fuentes REST dentro de `FeedsConfig`).
3. **Formalizar el modelo (fuente ≠ mecanismo ≠ feed) y acotar
   explícitamente el alcance de `FeedOrchestrator`.** Adoptada.

## Decisión

Se adopta el modelo definido en `docs/architecture/feed-model.md`. En
particular:

- Un feed no es una fuente de datos. Un feed es un mecanismo de
  ingestión, específicamente el que mantiene un flujo continuo
  (streaming), independiente del protocolo (WebSocket, FIX, SSE, etc.).
- Fuente de datos y mecanismo de ingestión son conceptos ortogonales. Una
  misma fuente puede exponer varios mecanismos.
- Todo mecanismo de ingestión (feed, polling, batch, replay, cálculo
  derivado) converge en el mismo modelo de eventos del dominio.
- Kafka es la Single Source of Truth operacional de la arquitectura
  Kappa. Las capas Bronze/Silver/Gold sobre Iceberg no constituyen una
  nueva SSOT — son proyecciones materializadas derivadas del mismo flujo
  de eventos.
- `AppConfig.feeds`, `FeedsConfig` y `FeedOrchestrator` quedan acotados
  exclusivamente a feeds de mercado en tiempo real (streaming). No se
  extienden para cubrir polling, batch, replay o cálculos derivados.

## Justificación técnica

Ver `feed-model.md` para el desarrollo completo. En síntesis: forzar
mecanismos de ingestión heterogéneos bajo una misma abstracción de
configuración rompe SSOT (Principio 1, ADR-0000 serie heredada) en cuanto
esos mecanismos diverjan en frecuencia, volumen o forma de fallo — que ya
es el caso hoy entre streaming WS y polling REST.

## Consecuencias

- `FeedsConfig`/`FeedOrchestrator` quedan protegidos de scope creep.
- Cualquier integración nueva de datos (Glassnode, CoinMarketCap, FRED,
  etc.) se evalúa primero contra `feed-model.md` §3 antes de decidir si
  encaja como feed o requiere un mecanismo distinto.
- `feed-model.md` queda como referencia obligatoria para diseñar el
  dominio de ingestión no-streaming.

## Resolución del ownership (enmienda 2026-08-05)

**Bounded context responsable de la ingestión no-streaming (polling,
batch, replay): `market_data`.**

Decisión: la ingestión no-streaming se modela inicialmente **dentro del
bounded context `market_data`** como capacidad interna separada de los
feeds streaming — estructura interna `realtime_feeds` (streaming
persistente, hoy) y `external_ingestion` (polling, batch, replay y
scheduling, futuro), con su propio lifecycle, puertos, adapters,
configuración Hydra y contratos, pero convergiendo al mismo modelo de
eventos y a Kafka como SSOT operacional.

Razonamiento: streaming y no-streaming resuelven la misma capacidad de
negocio — adquirir y normalizar información de mercado hacia el modelo de
eventos interno — y difieren únicamente en el mecanismo de adquisición, no
en el lenguaje de dominio (sources, markets, trades, order books, funding,
open interest, market metrics, events). Separarlos en bounded contexts
distintos fragmentaría el ownership y duplicaría contratos sin ganancia de
cohesión.

La creación de un bounded context independiente (p. ej. `data_ingestion`,
`market_intelligence`) queda reservada a una futura **separación de dominio
real**: si aparecen responsabilidades de inteligencia de mercado o research
cuantitativo que consuman market data para producir conocimiento nuevo
(alpha, modelos de régimen, señales complejas), ese sí sería un dominio
distinto y merecería su propio BC. No por incorporar más proveedores o
protocolos.

El diseño concreto de la estructura interna de `market_data`
(`realtime_feeds` / `external_ingestion`), incluyendo puertos, lifecycle y
contratos de publicación Kafka, se definirá en un ADR de diseño interno
específico (futuro ADR-0014) y quedará fuera del alcance de este documento.

## Referencias

- `docs/architecture/feed-model.md` (documento normativo completo)
- `docs/architecture/0002-event-driven-kappa-architecture.md` (serie
  heredada — Kafka SSOT, EventBus local, Control Plane/Data Plane)
- `docs/architecture/0000-principios-arquitectonicos.md` (serie
  heredada — Principio 1 SSOT)
- `ocm/config/schema.py` (`FeedsConfig`, `FeedsKafkaConfig`,
  `ExchangeFeedEntryConfig`)
- `packages/market_data/infrastructure/bootstrap/composition_root.py`

## Nota de discrepancia (2026-08-10) — F-031 / B-46

La decisión "todo mecanismo de ingestión converge a Kafka como SSOT
operacional" no se cumple hoy para el camino **polling OHLCV** de
`external_ingestion`: `OHLCVPipeline` (incremental/backfill) publica a un
`NullPublisher()` hardcodeado y `_chunk_converter` no está cableado, por lo
que ningún evento llega a `ohlcv.raw` (ver F-031 / B-46). La intención de
diseño de este ADR queda intacta — el incumplimiento es de implementación,
no de decisión. La remediación (cablear Kappa real / fail-fast / degradación
explícita) está pendiente de decisión en F-031/B-46.
