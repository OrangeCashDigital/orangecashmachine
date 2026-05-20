# ADR-0001: Bounded Context market_data — Aislamiento y Capas

**Estado:** Aceptado
**Fecha:** 2026-05-20

---

## Contexto

OCM gestiona múltiples dominios (market_data, trading, portfolio, research).
Sin una separación explícita, los módulos tienden a acoplarse, dificultando
el testing independiente y el deployment parcial.

## Decisión

Establecemos `market_data` como un Bounded Context aislado con arquitectura
hexagonal (Ports & Adapters) en cinco capas estrictas:



Ninguna capa interna conoce las externas. Las dependencias fluyen hacia adentro.

Las capas se materializan como paquetes Python bajo `packages/market_data/`.

## Consecuencias

### Positivas
- Cada capa es testeable en aislamiento con dobles (Protocol/stub).
- Cambiar el broker (Kafka → Pulsar) no toca application ni domain.
- El dominio es puro Python — cero dependencias de infraestructura.

### Negativas / Trade-offs
- Más archivos y estructura inicial.
- Añadir una feature requiere pensar en qué capa pertenece.

## Contratos enforced

- `BC-03: market_data.domain isolated from outer layers`
- `BC-04: market_data.ports depend only on domain`
- `BC-05: market_data.application isolated from infrastructure and adapters`
- `BC-08: market_data layer order (domain < ports < application < adapters < infrastructure)`
- `BC-10: market_data does not import sibling bounded contexts`

## Alternativas consideradas

| Alternativa | Descartada porque |
|-------------|------------------|
| Monolito plano | Acoplamiento total; imposible testear sin broker |
| Microservicios desde el inicio | Overhead prematuro; mismo proceso es suficiente |

## Referencias

- Evans, Eric. «Domain-Driven Design» — Bounded Contexts
- Vernon, Vaughn. «Implementing Domain-Driven Design» — Capítulo 2
