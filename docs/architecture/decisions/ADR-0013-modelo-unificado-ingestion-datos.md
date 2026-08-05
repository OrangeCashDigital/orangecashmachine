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

## Pendiente de decisión

**Bounded context responsable de la ingestión no-streaming (polling,
batch, cálculos derivados de índices compuestos) — sin resolver.**
Alternativas identificadas, ninguna descartada ni favorecida:

- Extender `market_data` con un subpaquete de ingestión no-streaming.
- Crear un bounded context nuevo, p. ej. `data_ingestion` o
  `market_intelligence`.
- Incorporar parte de esta responsabilidad dentro de `control_plane`
  (ADR-0002 serie heredada, Decisión 3, ya reserva espacio para
  scheduling — pero scheduling no implica necesariamente ownership del
  dominio de adquisición de datos).
- Otra alternativa no identificada todavía.

Esta decisión requiere su propio ADR cuando se resuelva. No bloquea la
adopción del modelo conceptual de este documento.

## Referencias

- `docs/architecture/feed-model.md` (documento normativo completo)
- `docs/architecture/0002-event-driven-kappa-architecture.md` (serie
  heredada — Kafka SSOT, EventBus local, Control Plane/Data Plane)
- `docs/architecture/0000-principios-arquitectonicos.md` (serie
  heredada — Principio 1 SSOT)
- `ocm/config/schema.py` (`FeedsConfig`, `FeedsKafkaConfig`,
  `ExchangeFeedEntryConfig`)
- `packages/market_data/infrastructure/bootstrap/composition_root.py`
