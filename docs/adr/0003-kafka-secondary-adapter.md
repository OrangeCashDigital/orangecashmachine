# ADR-0003: Kafka como Secondary Adapter — Puerto de Publicación OHLCV

**Estado:** Aceptado
**Fecha:** 2026-05-20

---

## Contexto

El pipeline OHLCV necesita publicar chunks procesados a sistemas downstream
(Kafka actualmente; potencialmente Redis Streams, WebSocket, webhooks).

Sin una abstracción, el pipeline acopla el transporte de datos a la lógica
de negocio: cambiar de Kafka a otro broker requeriría modificar `application/`.

Además, el modo degradado (sin broker disponible) necesita ser declarado
explícitamente — no un `if publisher is not None:` disperso en el código.

## Decisión

Kafka (y cualquier medio de publicación) es un **secondary adapter** (driven/outbound).

Su abstracción es `OHLCVPublisherPort` — un `Protocol` con `@runtime_checkable`
ubicado en `ports/outbound/publisher_port.py`.

Jerarquía de implementaciones:

    OHLCVPipeline
      uses OHLCVPublisherPort          (abstracción — ports/outbound/)
           |
           +-- KafkaOHLCVPublisher     (concreta — infrastructure/kafka/)
           +-- NullPublisher           (NullObject — ports/outbound/)

`NullPublisher` es el modo degradado explícito cuando:
  - `KAFKA_ENABLED != true` (flag apagado en config)
  - El broker no está disponible al inicio (fail-soft)

El flag `KAFKA_ENABLED` se lee **solo** en `CompositionRoot.assemble()` —
nunca en `application/`, nunca en el pipeline.

### Por qué NullObject en lugar de Optional[publisher]

`Optional[OHLCVPublisherPort]` propagaría ramas `if self._publisher is not None:`
por todo el pipeline y sus colaboradores. NullObject:
  - Elimina esas ramas (SRP: el pipeline publica, no decide si publicar)
  - Documenta el modo degradado como variante válida del sistema
  - Permite tests de pipeline sin broker sin ningún mock

Ref: Fowler, «Refactoring» §Replace Special Case with Object.

## Consecuencias

### Positivas
- Añadir Redis Streams = nueva clase `RedisPublisher` implementando el Protocol.
  Sin modificar `application/`, `domain/`, ni el pipeline.
- Modo degradado declarado en el tipo — visible en el grafo de dependencias.
- Wire schemas Kafka viven en `shared.kafka` (SSOT) — el publisher solo los usa.

### Negativas / Trade-offs
- Un nivel de indirección adicional vs. llamar a Kafka directamente.
- Los errores de publicación son asíncronos — el pipeline no sabe si el chunk llegó.

## Contratos enforced

- `BC-29: kafka wire schemas must be imported from shared.kafka (not from market_data.infrastructure)`
- `BC-33: shared.kafka.schemas isolated from domain types and bounded contexts`
- `BC-35: bounded contexts do not define their own Kafka wire payloads (no duplication)`

## Alternativas consideradas

| Alternativa | Descartada porque |
|-------------|------------------|
| Optional[publisher] en pipeline | Propaga condicionales; viola SRP |
| Publisher concreto inyectado sin Port | Viola DIP; testear requiere Kafka real |
| Publicación síncrona bloqueante | Degrada throughput del pipeline principal |

## Referencias

- Hohpe & Woolf. «Enterprise Integration Patterns» — Message Channel
- Fowler, Martin. «Refactoring» §Replace Special Case with Object (NullObject)
- Cockburn, Alistair. «Hexagonal Architecture» — Secondary Ports
