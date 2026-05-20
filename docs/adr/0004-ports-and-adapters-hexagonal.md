# ADR-0004: Arquitectura Hexagonal (Ports & Adapters)

**Estado:** Aceptado
**Fecha:** 2026-05-20

---

## Contexto

La arquitectura layered clásica (Controller → Service → Repository) acopla
la lógica de negocio al framework, la base de datos y el broker.
En OCM, cambiar de Parquet a Iceberg o de CCXT a otra librería no debería
requerir tocar el dominio.

## Decisión

Adoptamos **Ports & Adapters** (Cockburn, 2005) para market_data:

- **Primary ports** (`ports/inbound/`): lo que el exterior hace con el sistema
  (ej. `OHLCVPipelinePort`). Driven por REST, Prefect, CLI.

- **Secondary ports** (`ports/outbound/`): lo que el sistema necesita del exterior
  (ej. `OHLCVPublisherPort`, `CursorStorePort`, `OHLCVStoragePort`).

- **Primary adapters** (`adapters/inbound/`): traducen señales externas a calls de ports.

- **Secondary adapters** (`adapters/outbound/` e `infrastructure/`): implementan
  los secondary ports usando tecnología concreta (CCXT, Kafka, Iceberg, Redis).

Los dos lados del hexágono son ortogonales — `ports/inbound` nunca importa
`ports/outbound` y viceversa (BC-37a, BC-37b).

## Consecuencias

### Positivas
- El dominio y application/ son testeables sin ninguna infraestructura.
- Sustituir Iceberg por DuckDB = nuevo secondary adapter, cero cambios en domain.
- Cada puerto puede tener múltiples adaptadores (Kafka + Redis Streams en paralelo).

### Negativas / Trade-offs
- Requiere disciplina para no hacer imports directos (enforced por lint-imports).
- La navegación del código requiere seguir las abstracciones.

## Contratos enforced

- `BC-04: market_data.ports depend only on domain`
- `BC-37a: ports/inbound does not import ports/outbound`
- `BC-37b: ports/outbound does not import ports/inbound`

## Referencias

- Cockburn, Alistair. «Hexagonal Architecture» (2005) — alistair.cockburn.us
- Martin, Robert C. «Clean Architecture» — Capítulo 22
