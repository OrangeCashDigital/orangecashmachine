# ADR-0002: Composition Root — Único Punto de Ensamblaje

**Estado:** Aceptado
**Fecha:** 2026-05-20

---

## Contexto

Sin un punto de ensamblaje único, los módulos de `application/` instanciaban
adaptadores concretos vía lazy imports con fallback a None:

    # Anti-patrón eliminado de ohlcv_pipeline.py
    def _build_kafka_publisher_safe():
        try:
            from market_data.infrastructure.kafka.producer import KafkaProducerAdapter
            ...
        except Exception:
            return None  # silencia el error; el pipeline no sabe qué tiene

Esto violaba DIP (application/ conocía infraestructura concreta),
SRP (el pipeline decidía cómo construir su propia dependencia),
y hacía imposible testear sin un broker real.

## Decisión

`packages/market_data/infrastructure/bootstrap/composition_root.py` es el
**único** punto donde se decide qué implementación concreta se inyecta.

`CompositionRoot.assemble(config)` construye el grafo completo de dependencias
como un dataclass `frozen=True` — inmutable tras construcción.

Ningún módulo fuera de `infrastructure/bootstrap/` instancia adaptadores.
Los lazy fallbacks se reemplazan con:

1. `NullPublisher` (NullObject Pattern) inyectado por CompositionRoot
   cuando Kafka está deshabilitado o no disponible.
2. Parámetros tipados como `OHLCVPublisherPort` en el pipeline —
   nunca `Optional[...]`, nunca `None`.

## Consecuencias

### Positivas
- `application/` es puro: solo conoce abstracciones (Protocol).
- Cambiar de Kafka a otro broker = cambiar solo CompositionRoot.
- Tests de pipeline: inyectar `NullPublisher` sin patches ni mocks.
- Fail-Fast en arranque: config inválida falla en `assemble()`, no en el primer request.

### Negativas / Trade-offs
- CompositionRoot crece con el sistema (mitigado: es el punto de crecimiento correcto).
- Un nivel más de indirección al leer el código por primera vez.

## Contratos enforced

- `BC-38: PipelineFactory solo instanciable desde infrastructure/bootstrap/composition_root`
- `BC-05: market_data.application isolated from infrastructure and adapters`

## Alternativas consideradas

| Alternativa | Descartada porque |
|-------------|------------------|
| DI framework (injector, dependency-injector) | Complejidad innecesaria; Python Protocols son suficientes |
| Service Locator | Anti-patrón: oculta dependencias; dificulta testing |
| Lazy imports con try/except | Viola DIP; acoplamiento oculto; probado y eliminado |

## Referencias

- Seemann, Mark. «Dependency Injection in .NET» — §Composition Root (capítulo 3)
- Martin, Robert C. «Clean Architecture» — Capítulo 26
- Fowler, Martin. «Inversion of Control Containers and the Dependency Injection pattern»
