# ADR-0014: Diseño interno de `market_data` — Market Data Platform (realtime_feeds + external_ingestion)

**Estado:** Propuesto
**Fecha:** 2026-08-05
**Bounded context(s) afectado(s):** market_data (y, por contrato, shared)

## Contexto

ADR-0013 fijó la propiedad del dato: `market_data` es el bounded context
propietario de toda la ingestión (streaming y no-streaming), y estableció el
modelo feed/fuente/mecanismo (normativo en `docs/architecture/feed-model.md`).
Este ADR cierra el paso siguiente: **definir la estructura interna** de
`market_data` para que un único dominio admita múltiples mecanismos de captura
sin que "cómo llega el dato" contamine a los consumidores.

Motivación verifiable:

- Hoy solo existe la vía streaming (WebSocket/FIX/SSE): `FeedOrchestrator`,
  `adapters/inbound/websocket/*`, `ports/inbound/*`. La adquisición
  no-streaming (polling/batch/replay) no tiene **capacidad** propia: solo hay
  stubs muertos en `adapters/inbound/data_providers/`
  (`coinmarketcap_adapter.py`, `coinglass_adapter.py`) y fetchers parciales en
  `adapters/inbound/rest/`.
- Próximos casos de uso (índices de mercado, funding/OI agregados, datos
  alternativos) entran por REST/batch, no por streaming. Si cada fuente creara
  su propio modelo y su propio camino de publicación, se rompería la
  consistencia temporal, la calidad y la trazabilidad.

Referencia de industria (documentada en la discusión de diseño): las firmas
cuantitativas no separan un "WebSocket BC" de un "REST BC". Un único **Market
Data Platform / Market Data Domain** posee adquisición, normalización,
distribución y calidad, con pipelines especializados internos (feeds de baja
latencia, batch histórico, vendors, reference data, datos alternativos). La
separación importante es *responsabilidad + contrato*, no *transporte*.

## Alternativas evaluadas

1. **Bounded context por transporte** (WebSocket BC, REST BC, batch BC).
   *Descartada.* Separar por protocolo duplica el modelo de eventos, rompe la
   consistencia y crea el antipatrón de la industria: cada fuente produce un
   universo propio.
2. **BC nuevo `data_ingestion`** que comoditice toda la adquisición no-streaming
   y que `market_data` la consuma como vendor. Descartado (revertido en
   ADR-0013 y en la discusión de control_plane): el scheduling/ejecución no es
   dueño del significado del dato; partirlo fuera haría que el dominio
   no-streaming dejara de pertenecer a market data. Solo se crearía un BC nuevo
   si fuera un dominio genuinamente distinto (research/inteligencia que produce
   conocimiento nuevo), no por tipo de captura.
3. **Capacidades como directorios de alto nivel** en `packages/market_data`
   (`realtime_feeds/`, `external_ingestion/`, `normalization/` como slices
   verticales autocontenidos).
   Ventaja: modelo conceptual alineado con la nomenclatura del feed-model.
   Costo/riesgo: rompería los contratos de capas BC-03/04/05/08 y obligaría a
   reescribir la equivalencia de capas ADR-0007 para volver a aplicar
   Clean/DDD sobre paquetes que hoy son verticales.
4. **Capacidades como subpuntos dentro de las capas existentes.**
   Ventaja: mantiene todos los contratos de capas intactos, no reescribe nada.
   Costo/riesgo: el nombre "capacidad" es conceptual; hay que mapearlo
   explícitamente (ver Decisión) y no debe degenerar en acoplamiento entre
   capacidades vía import directo.
   **Elegida.**

## Decisión

Confirmar que `market_data` es el **bounded context propietario** y diseñarlo
como **Market Data Platform**: un único dominio que posee adquisición,
normalización, distribución y calidad, con dos capacidades internas cohesivas y
una capa de contratos comunes. **Se separa por responsabilidad y ciclo de
vida, nunca por protocolo.**

### Capability (conceptual, alineado con la industria)

```
market_data  (BC dueño — Market Data Platform)
├── realtime_feeds     → conexiones persistentes, streaming, reconexión,
│                         backpressure, normalización rápida, publicación inmediata
├── external_ingestion → adquisición periódica, APIs externas, rate limits,
│                         scheduling, retries, backfill, replay, datos alternativos
├── normalization      → todo a un lenguaje único de dominio (canonical event)
├── data_quality       → reservada (timestamp validation, missing/duplicate,
│                         outliers, schema evolution, source reliability, latency), por capas
└── kafka boundary     → todo camino termina en el mismo log operacional (Kafka SSOT)
```

Un **canonical data model + event-driven distribution**: Binance WS,
CoinGlass REST y FRED no producen tres universos; producen **eventos internos
homogéneos** con timestamp (UTC), source identity, schema version, quality
flags y lineage. El consumidor downstream (estrategia, research, gold layer)
no sabe ni debe saber si el dato vino por WebSocket o por API.

### Realización física (contratos BC-* intactos)

El modelo conceptual se mapea sobre la estructura por capas ya existente y
contratada; se crean subpaquetes nuevos pero **no slices verticales**:

```
packages/market_data
├── domain/                                 # sin cambios de diseño
├── ports/
│   ├── inbound/
│   │   ├── market_data_source.py        # realtime (existe)
│   │   ├── trades_source.py             # realtime (existe)
│   │   └── external/                    # NUEVO — contratos de adquisición
│   │       ├── polling.py               # PollingSourcePort, PollingRequest, PollingResult
│   │       └── replay.py                # ReplayPort, HistoricalRequest
│   └── outbound/
│       ├── normalization.py             # SSOT de transforms (existe)
│       ├── kafka_producer.py            # publisher común (existe)
│       ├── event_publisher.py           # (existe)
│       ├── data_quality_checker.py      # (existe — se amplía, reserva data_quality)
│       └── metrics.py, lineage.py       # (existen — observabilidad/trazabilidad)
├── application/
│   ├── feed_orchestrator.py             # realtime_feeds (existe)
│   └── external_ingestion/              # NUEVO — external_ingestion
│       ├── orchestrator.py              # ExternalIngestionOrchestrator (loop/sched/retry)
│       ├── normalizers/                 # raw provider → canonical event (puros)
│       └── quality.py                   # primero uso de la reserva data_quality
├── adapters/
│   ├── inbound/websocket/               # realtime_feeds (existe)
│   ├── inbound/rest/                    # fetchers históricos (existe — base de replay)
│   ├── inbound/external/                # NUEVO — sdks/sesiones de vendors
│   │   ├── coinmarketcap.py             # formaliza stubs data_providers/
│   │   ├── coinglass.py                 # formaliza stubs data_providers/
│   │   ├── glassnode.py                 # futuro
│   │   └── fred.py                      # futuro
│   └── outbound/                        # publishers Kafka (existen)
└── infrastructure/
    ├── kafka/                           # (existe — operación, no dominio)
    ├── storage/                         # Iceberg materialización (existe)
    └── bootstrap/composition_root.py    # wiring único (BC-38) — se extiende
```

Fase previa (`adapters/inbound/data_providers/*`, muerto) se **retira** y su
sesión aiohttp/adaptadores se **formaliza** en `adapters/inbound/external/`
implementando `PollingSourcePort`.

### Firmas de puertos

```python
# ports/inbound/external/polling.py
class PollingSourcePort(Protocol):
    source_id: str  # identidad canónica: "coinglass" | "coinmarketcap" | ...
    async def fetch(self, request: PollingRequest) -> PollingResult: ...

@dataclass(frozen=True, slots=True)
class PollingRequest:
    metric: str                        # "funding_rate" | "open_interest" | "market_metrics" ...
    symbols: Sequence[str] | None = None

@dataclass(frozen=True, slots=True)
class PollingResult:
    source_id: str
    metric: str
    fetched_at: datetime
    payload: Sequence[Mapping[str, object]]  # registros crudos provider-native sin framework
```

```python
# ports/inbound/external/replay.py
class ReplayPort(Protocol):
    source_id: str
    async def fetch_historical(self, request: HistoricalRequest) -> PollingResult: ...

@dataclass(frozen=True, slots=True)
class HistoricalRequest:
    metric: str
    symbol: str
    start: datetime
    end: datetime
```

El **canonical event** producido por los normalizers (un único modelo):

```python
#  (especificación de contrato; reside en domain/events/_base.py y shared/kafka/schemas)
@dataclass(frozen=True, slots=True)
class CanonicalEvent:
    kind: str                     # "candle" | "ticker" | "funding_rate" | ...
    ts_utc: datetime              # base de tiempo única
    source_id: str                # "binance" | "coinglass" | ...
    schema_version: str
    quality_flags: tuple[str, ...]   # reserva data_quality
    lineage: Lineage              # provenance
    payload: Mapping[str, object]
```

### Config

`ExternalIngestionConfig` (en `ocm/config/schema.py`, SSOT de `OCM_*` en
`ocm/config/env_vars.py`), poblada por Hydra bajo clave `external_ingestion:`:

```yaml
external_ingestion:
  enabled: false
  sources:
    coinglass:
      enabled: false
      metric: funding_rate
      schedule: { every: 300 }        # segundos | cron
      symbols: [BTCUSDT, ETHUSDT]
      topic: market_data.external.coinglass
      rate_limit: { per_minute: 60 }
```

Separada de `FeedsConfig`/`AppConfig.feeds` (streaming) y gobernada por el
mismo `composition_root` (no sub-root nuevo, BC-38). `enable` global default
`false` (mismo patrón que `dry_run: true` en `config/base.yaml`).

### Contratos import-linter

- Reactivar **BC-49** para `ports/inbound/external`: prohibido importar SDK de
  vendor en los puertos.
- Nuevo contrato: `adapters/inbound/external` no importa SDK vendor a nivel
  módulo (acceso diferido, estilo BC-39) y no importa dominio/application.
- Relegan a BC-03/04/05/08 para el aislamiento entre capas y a BC-35 para los
  wire schemas Kafka en `shared/kafka/schemas/`.
- Los puertos `inbound/external` y `outbound/kafka_producer` quedan aislados
  entre sí (no hay import directo adaptador→puerto salvo el orquestador).

## Justificación técnica

- **Una única red neuronal de eventos.** El consumidor no sabe el transporte;
   la estrategia recibe un evento de funding/trade/order book igual venga de
   Binance WS o de una API externa. Esto es lo que la industria llama
   **canonical data model + event-driven distribution**.
- **Separar adquisición de transformación.** El adapter obtiene datos, no
   calcula inteligencia. Cadena obligatoria:
   `Provider Adapter → Raw (PollingResult) → Normalization (normalizers) →
   CanonicalEvent → Kafka → Feature Engineering → Strategy`. Evita meter
   lógica de negocio en adapters.
- **Kappa correcto.** Kafka = SSOT operacional, Iceberg = materialización
   histórica. El sistema no depende de una tabla final como fuente primaria;
   puede reconstruir estado desde eventos (ya heredado del diseño previo).
- **La calidad es parte del dominio.** Se reusa `ports/outbound/
   quality_checker.py` y la reserva `data_quality` deja espacio para:
   timestamp validation, missing candles, outliers, schema evolution, duplicados,
   source reliability, latency — sin implementar todo ahora.
- **Scheduling no se adueña del dominio.** El scheduler/retry vive dentro de
   `external_ingestion` como mecanismo de ejecución; no es un BC para el
   o de datos ni el dueño del significado.
- **No mezclar** adapters con features, ingestión con research, scheduling con
   ownership del dominio. La capa futura de alpha research/intelligence
   consumirá los eventos canónicos sin contaminar `market_data`.

## Consecuencias

- **Más fácil:** sumar una nueva fuente externa = puerto + adapter + normalizador
   + config; los consumidores downstream no cambian.
- **Sin deuda:** permanecen muertos antes los comportamientos del path external;
   `data_providers/` se elimina al formalizarlo en `adapters/inbound/external/`.
- **Deuda consciente:** la reserva `data_quality` empieza con flags de metadata
   y quality checks básicos; la validación completa (outliers, schema evolution
   etc.) se completa en fases posteriores.
- **Contratos que hacen cumplir esta decisión:** BC-03/04/05/08 (capas), BC-38
   (composition root único), BC-49 reactivado (puertos sin SDK vendor),
   BC-35 (wire schemas solo en `shared/kafka/schemas/`), contrato BC-39-style
   para `adapters/inbound/external` (lazy imports).
- La decisión de **nuevo BC de research/intelligence** queda fuera de este ADR
   (no-goal): solo se prepara el terreno para que consuma sin contaminar.

## Referencias

- Código: `packages/market_data/application/feed_orchestrator.py`,
  `packages/market_data/ports/inbound/*.py`, `packages/market_data/ports/outbound/
  normalization.py`, `packages/market_data/ports/outbound/kafka_producer.py`,
  `packages/market_data/adapters/inbound/data_providers/` (→ se formaliza en
  `adapters/inbound/external/`), `packages/market_data/infrastructure/bootstrap/
  composition_root.py`, `ocm/config/schema.py`.
- ADRs relacionados: ADR-0013 (ownership), ADR-0004 (BC-47 mercado datos),
  ADR-0007 (equivalencia de capas), ADR-0003 (composition roots), y
  `docs/architecture/feed-model.md` (§7/§8).

## Nota de discrepancia (2026-08-10) — F-031 / B-46

Este ADR asume "todo camino termina en el mismo log operacional (Kafka
SSOT)" y lista `ports/outbound/publisher_port.py` como "publisher común
(existe)". Válido en el papel; en el código, el camino de publicar OHLCV a
Kafka **no está conectado hoy**: `OHLCVPipeline` instancia `NullPublisher()`
como default local (ohlcv_pipeline.py:248) y `_chunk_converter` no se
inyecta (pipeline_factory._build_ohlcv no les pasa publisher ni converter),
de modo que `KafkaOHLCVPublisher` (`_build_kafka_publisher`,
pipeline_factory.py:156) es código muerto sin callers y las strategies
incremental/backfill fallan con RuntimeError antes de publicar (F-031/B-46).
No invalida la decisión de estructura interna (el esqueleto `external_ingestion`
y sus puertos siguen siendo los correctos); es una falla de wiring en la
realización física, registrada en F-031/B-46 con remediación pendiente de
decisión.