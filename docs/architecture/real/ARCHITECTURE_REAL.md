# ARQUITECTURA REAL ACTUAL — OCM Market Data

> **Fuente de verdad**: Código implementado y configuración desplegada (READ-ONLY audit, 2026-09-04)
> **No presenta arquitectura objetivo** — solo documenta lo que existe hoy

---

## 1. VISIÓN GENERAL DEL SISTEMA

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ORANGE CASH MACHINE                                  │
│  Bounded Contexts: market_data | trading | portfolio | shared | ocm        │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Procesos systemd activos

| Servicio | Entrypoint | Estado | Descripción |
|----------|------------|--------|-------------|
| `ocm-market-data` | `python -m market_data.main` | **ACTIVE** | REST ingestion + Bronze writer + HTTP :8001 |
| `ocm-streaming` | `python -m app.cli.streaming_hydra` | **ACTIVE (desde Sep 03)** | WebSocket L2 orderbook → Kafka |

---

## 2. OCm-MARKET-DATA (market_data.main)

### Qué ejecuta
**Archivo**: `packages/market_data/main.py`
**Proceso**: FastAPI + background tasks (asyncio)

### Lifecycle (lifespan)
```
_lifespan()
├── build_context() → RuntimeContext (AppConfig + RunConfig)
├── bootstrap_logging + configure_logging
├── ExecutionGuard (max_consecutive_errors)
├── IcebergStorageFactory (DIP)
├── TASK 1: _ingestion_loop()  ← OHLCV polling REST
├── TASK 2: _bronze_writer_loop()  ← Kappa: ohlcv.raw → Bronze
└── TASK 3: feed_orchestrator_task (opcional)  ← WS trades si ingestion_mode≠rest
```

### _ingestion_loop() — Flujo OHLCV REST
```
Para cada exchange en app_cfg.exchange_names:
  Para market_type in [spot, futures]:
    symbols = exc_cfg.markets.{spot,futures}_symbols  ← DESDE config/env/*.yaml
    PipelineRequest:
      exchange, market_type, pipeline="ohlcv", mode="incremental"
      credentials=exc_cfg.ccxt_credentials()
      resilience=exc_cfg.resilience
      symbols, timeframes, start_date, auto_lookback_days
      run_id=ctx.run_id, dry_run=app_cfg.safety.dry_run
    PipelineOrchestrator.run(request) → ConcretePipelineFactory.build()
```

### _bronze_writer_loop() — Kappa Bronze
```
KafkaConsumerAdapter.for_bronze_writer()  → topic: ohlcv.raw
KafkaBronzeWriter.run() → poll → deserialize → dedup → BronzeStorage.append()
BronzeStorage.append() → Iceberg table: bronze.ohlcv
```

### Topics Kafka producidos
| Topic | Producer | Schema |
|-------|----------|--------|
| `ohlcv.raw` | KafkaOHLCVPublisher | EventPayload + KafkaOHLCVBar[] |
| `trades.raw` | (opcional, via FeedOrchestrator) | TradePayload |

### Topics Kafka consumidos
| Topic | Consumer | Destino |
|-------|----------|---------|
| `ohlcv.raw` | KafkaBronzeWriter | Bronze.ohlcv (Iceberg) |

---

## 3. OCm-STREAMING (app.cli.streaming_hydra)

### Qué ejecuta
**Archivo**: `apps/app/cli/streaming_hydra.py`
**Proceso**: CLI standalone (sin FastAPI), lifecycle asyncio + signal handlers

### Flujo principal
```
main()
├── _load_config() → load_appconfig_standalone() → AppConfig validado
├── RunConfig.from_env()
├── feeds = config.feeds.feeds  ← DESDE config/market_data/feeds.yaml
├── entry = feeds[exchange] (default bybit)
├── symbols = entry.symbols  ← DESDE feeds.yaml (formato cryptofeed)
├── CompositionRoot.build_ws_producers() → WSProducerBundle
│   └── orderbook: OrderBookKafkaProducer → orderbook.raw
├── CryptofeedOrderBookStream(exchange, symbols, on_snapshot, on_delta)
├── bundle.start_all() → producers Kafka start
├── stream.start() → FeedHandler + cryptofeed Bybit L2_BOOK
├── _heartbeat_loop() → PrometheusPusher → Pushgateway
└── stop event → stream.stop() → bundle.close_all()
```

### Qué obtiene
- **L2 Orderbook** (snapshots + deltas) via Bybit WebSocket canal `L2_BOOK`
- **Formato símbolos**: `BTC-USDT-PERP`, `ETH-USDT-PERP`, `SOL-USDT-PERP` (cryptofeed)

### Qué publica
| Topic | Producer | Schema |
|-------|----------|--------|
| `orderbook.raw` | OrderBookKafkaProducer | OrderBookSnapshotPayload / OrderBookDeltaPayload |

### Qué NO tiene
- ❌ Consumer para `orderbook.raw`
- ❌ Persistencia a Bronze
- ❌ Orderbook Builder / BookBuilder
- ❌ Gap detection / sequence validation / recovery

---

## 4. MARKET UNIVERSE — FUENTES MÚLTIPLES, FORMATOS DIVERGENTES

### 3 fuentes de verdad para Bybit

| Archivo | Clave YAML | Formato | Ejemplo | Consumidor |
|---------|------------|---------|---------|------------|
| `config/exchanges/bybit.yaml` | `exchanges.bybit.enabled` | Solo `enabled: true` (sin símbolos) | `enabled: true` | `AppConfig.parse_exchanges()` → `exchange_names` |
| `config/env/development.yaml` | `exchanges.bybit.markets.spot.symbols` | **CCXT nativo** | `BTC/USDT` | `ConcretePipelineFactory._build_ohlcv()` → `exc_cfg.markets.spot_symbols` |
| `config/market_data/feeds.yaml` | `feeds.feeds.bybit.symbols` | **Cryptofeed** | `BTC-USDT-PERP` | `streaming_hydra.py` → `config.feeds.feeds.bybit.symbols` |

### Discrepancias críticas
1. **Tres fuentes** para el mismo exchange
2. **Dos formatos incompatibles**: `BTC/USDT` (CCXT) vs `BTC-USDT-PERP` (cryptofeed)
3. **Sin capa de normalización** — cada consumidor usa su formato nativo
4. **`auto_discover_symbols: false`** en `ExchangeConfig` (schema.py:201) — **nunca se usa** en código
4. **CCXT `load_markets()`** llamado en `CCXTAdapter.connect()` pero símbolos no se filtran contra config

### Quién consume qué universo
| Componente | Config origen | Símbolos |
|------------|---------------|----------|
| `market_data.main` (OHLCV) | `config/env/development.yaml` | `BTC/USDT` (spot) |
| `streaming_hydra.py` (Orderbook) | `config/market_data/feeds.yaml` | `BTC-USDT-PERP`, `ETH-USDT-PERP`, `SOL-USDT-PERP` |

**REST y WebSocket usan universos diferentes** — riesgo de data mismatch

---

## 5. FLUJO REAL OHLCV (REST → Kafka → Bronze)

```
BYBIT REST API (CCXT)
    │
    ▼
CCXTAdapter.fetch_ohlcv()  [adapters/outbound/exchange/ccxt_adapter.py:287]
    │
    ▼
HistoricalFetcherAsync.fetch()  [adapters/inbound/rest/ohlcv_fetcher.py]
    │
    ▼
OHLCVPipeline.run()  [application/pipelines/ohlcv_pipeline.py]
    │
    ▼
KafkaOHLCVPublisher.publish_chunk()  [infrastructure/kafka/ohlcv_publisher.py:187]
    │
    ▼
Kafka topic: ohlcv.raw (TOPIC_OHLCV_RAW)
    │
    ▼
KafkaBronzeWriter.run()  [infrastructure/kafka/bronze_writer.py]
    │
    ▼
KafkaConsumerAdapter.for_bronze_writer() → poll ohlcv.raw
    │
    ▼
BronzeStorage.append()  [infrastructure/storage/bronze/bronze_storage.py]
    │
    ▼
Iceberg Table: bronze.ohlcv
    Partitioned: exchange/market_type/symbol/timeframe/ts_month
    Schema: BRONZE_SCHEMA (12 fields: IDs 1-10 OHLCV + ingestion_ts + run_id)
```

### Componentes clave
- `ConcretePipelineFactory._build_ohlcv()` → cableado completo (pipeline_factory.py:180)
- `KafkaOHLCVPublisher` → único producer de `ohlcv.raw`
- `KafkaBronzeWriter` → único consumer de `ohlcv.raw` hacia Bronze
- **G4-G9**: PASS cuando infra disponible (Kafka, Redis, Iceberg)

---

## 6. FLUJO REAL ORDERBOOK (WebSocket → Kafka → ?)

```
BYBIT WebSocket (canal L2_BOOK)
    │
    ▼
CryptofeedOrderBookStream.start()  [adapters/inbound/websocket/cryptofeed_orderbook_stream.py:90]
    │
    ▼
FeedHandler.add_feed(Bybit, channels=[L2_BOOK], callbacks={L2_BOOK: _translate_and_dispatch})
    │
    ▼
_translate_and_dispatch(book, receipt_timestamp)  [línea 111]
    │
    ├─ delta is None → SNAPSHOT
    │    └─ book.book.to_dict() → bids/asks → _sorted_levels()
    │    └─ on_snapshot(exchange, symbol, timestamp_ms, bids, asks, depth, checksum)
    │
    └─ delta present → DELTA
         └─ delta.get('bid') / delta.get('ask') → price, size
         └─ on_delta(exchange, symbol, timestamp_ms, side, price, size)
    │
    ▼
OrderBookKafkaProducer.on_snapshot / on_delta  [adapters/inbound/websocket/orderbook_producer.py:86, 137]
    │
    ▼
serialize(OrderBookSnapshotPayload / OrderBookDeltaPayload)
    │
    ▼
KafkaProducerAdapter.produce(topic=TOPIC_ORDERBOOK_RAW, key=make_symbol_key(exchange, symbol))
    │
    ▼
Kafka topic: orderbook.raw (TOPIC_ORDERBOOK_RAW)
    │
    ▼
┌──────────────────────────────────────────────────────────────┐
│  CONSUMER:  ❌ NINGUNO en market_data                         │
│  Bronze:    ❌ NO hay schema ORDERBOOK en schemas.py          │
│  Iceberg:   ❌ NO hay tabla Bronze para orderbook             │
└──────────────────────────────────────────────────────────────┘
```

### Qué falta para Orderbook → Bronze reproducible
1. **Schema Iceberg** para orderbook (snapshot + delta + sequence + checksum)
2. **Consumer Kafka** para `orderbook.raw` (ej. `OrderbookBronzeWriter`)
3. **BronzeStorage** extendido para tabla `bronze.orderbook`
4. **Gap recovery** en `CryptofeedOrderBookStream` (sin sequence_number validation)
5. **Checksum validation** para detectar gaps en L2_BOOK
6. **Orderbook Builder** — reconstrucción de estado L2 desde snapshots+deltas

---

## 7. KAFKA — TOPICS Y FLUJOS

### Topics (SSOT: `shared/kafka/topics.py`)
| Topic | Producer | Consumer | Bronze |
|-------|----------|----------|--------|
| `ohlcv.raw` | KafkaOHLCVPublisher | KafkaBronzeWriter | ✅ |
| `trades.raw` | FeedOrchestrator / CCXT | QualityConsumer, Silver | ⚠️ |
| `orderbook.raw` | OrderBookKafkaProducer | **NINGUNO** | ❌ |
| `ohlcv.validated` | QualityPipeline | FeatureConsumer | — |
| `ohlcv.features` | FeaturePipeline | StrategyConsumer | — |

### Bootstrap
- **SSOT**: `KAFKA_BOOTSTRAP_SERVERS=localhost:9093` en `deploy/host.env` + `.env`
- **Interno Docker**: `kafka:9092`

---

## 8. BRONZE / SILVER / GOLD — ESQUEMAS

### Iceberg Schemas (`infrastructure/storage/iceberg/schemas.py`)

| Layer | Schema | Fields | Entidad |
|-------|--------|--------|---------|
| Bronze | `BRONZE_SCHEMA` | 12 (OHLCV + ingestion_ts + run_id) | `bronze.ohlcv` |
| Silver | `SILVER_SCHEMA` | 10 (OHLCV base) | `silver.ohlcv` |
| Gold | `GOLD_SCHEMA` | 19 (OHLCV + 5 features + 4 lineage) | `gold.features` |
| Trades | `TRADES_SCHEMA` | 10 (IDs 101-110) | `silver.trades` |
| Derivatives | `DERIVATIVES_SCHEMA` | 8 (IDs 201-208) | `silver.derivatives` |
| **Orderbook** | **MISSING** | — | **NO EXISTE** |

### Tablas Bronze actuales
- ✅ `bronze.ohlcv` — particionado por exchange/market_type/symbol/timeframe/ts_month
- ❌ `bronze.orderbook` — **no existe**
- ❌ `bronze.trades` — no existe (va a Silver directo)

---

## 9. SYSTEMd — UNIDADES DESPLEGADAS

### ocm-market-data.service
```ini
ExecStart=/home/orangemusic/trading/orangecashmachine/.venv/bin/python -m market_data.main
EnvironmentFile=/home/orangemusic/trading/orangecashmachine/deploy/host.env
EnvironmentFile=/home/orangemusic/trading/orangecashmachine/.env
```
- **ACTIVE** desde hace 1h 37min (pid 535148)
- Incluye: ingestion_loop + bronze_writer_loop + feed_orchestrator (opcional)

### ocm-streaming.service
```ini
ExecStart=/home/orangemusic/trading/orangecashmachine/.venv/bin/python -m app.cli.streaming_hydra
EnvironmentFile=/home/orangemusic/trading/orangecashmachine/deploy/host.env
EnvironmentFile=/home/orangemusic/trading/orangecashmachine/.env
```
- **ACTIVE** desde 2026-09-03 18:45:31 (pid 535701)
- Histórico: FAILED 2026-08-28 por "At least one exchange must be enabled" (resuelto)
- Solo orderbook canary (F2.6b) — funding/oi/liquidations sin runners

---

## 10. PROTOCOL DISCOVERY — ESTADO

| Tipo | Estado | Evidencia |
|------|--------|-----------|
| REST Discovery (OHLCV) | IMPLEMENTED | `CCXTAdapter.fetch_ohlcv` + `HistoricalFetcherAsync` |
| WebSocket Discovery (Orderbook) | PARTIAL | `CryptofeedOrderBookStream` (solo L2_BOOK, sin gap recovery) |
| REST Discovery (Trades) | IMPLEMENTED | `CCXTAdapter.fetch_trades` + `TradesFetcher` |
| WebSocket Discovery (Trades) | IMPLEMENTED | `BybitCryptofeedRunner` (TRADES channel) |
| Funding/OI Discovery | MISSING | Adapters existen (`funding_producer.py`, `oi_producer.py`) sin runners en streaming_hydra |
| Liquidations Discovery | MISSING | `liquidations_producer.py` sin runner |

---

## 11. INSTRUMENT DISCOVERY — ESTADO

| Capacidad | Estado | Detalle |
|-----------|--------|---------|
| Dynamic symbol discovery (`load_markets()`) | IMPLEMENTED BUT UNUSED | `CCXTAdapter.connect()` llama `load_markets()` pero resultados no se usan para filtrar config |
| `auto_discover_symbols` config | DOCUMENTED ONLY | Campo en `ExchangeConfig` (schema.py:201), nunca leído |
| Static symbol config | IMPLEMENTED | 3 fuentes YAML divergentes (ver §4) |
| Symbol normalization (CCXT ↔ cryptofeed) | MISSING | No hay capa de conversión `BTC/USDT` ↔ `BTC-USDT-PERP` |
| Instrument Metadata (tick size, lot size, etc.) | MISSING | No se captura ni persiste |

---

## 12. RESUMEN: QUÉ EXISTE VS QUÉ FALTA

| Capacidad | Estado | Gap Principal |
|-----------|--------|---------------|
| OHLCV REST ingestion | ✅ IMPLEMENTED | — |
| OHLCV → Bronze | ✅ IMPLEMENTED | — |
| Trades REST ingestion | ✅ IMPLEMENTED | — |
| Trades WS ingestion | ✅ IMPLEMENTED | — |
| Orderbook WS ingestion | ✅ IMPLEMENTED | Solo producer, no consumer |
| Orderbook → Bronze | ❌ MISSING | Schema, consumer, tabla Iceberg |
| Market Universe unificado | ❌ MISSING | 3 configs, 2 formatos |
| Instrument Discovery dinámico | ❌ MISSING | Config solo estática |
| Funding/OI/Liq WS | ⚠️ PARTIAL | Producers existen, sin runners en streaming |
| Protocol Discovery formal | 📋 DOCUMENTED | ADR-0017, no implementado |

---

## 13. EVIDENCIA DE CÓDIGO — ARCHIVOS CLAVE

| Componente | Archivo | Función/Clave |
|------------|---------|---------------|
| market-data entrypoint | `packages/market_data/main.py` | `_ingestion_loop()`, `_bronze_writer_loop()`, `_lifespan()` |
| streaming entrypoint | `apps/app/cli/streaming_hydra.py` | `main()`, `_run_streaming()` |
| OHLCV producer | `infrastructure/kafka/ohlcv_publisher.py` | `KafkaOHLCVPublisher.publish_chunk()` |
| Orderbook producer | `adapters/inbound/websocket/orderbook_producer.py` | `OrderBookKafkaProducer.on_snapshot/on_delta` |
| Orderbook stream | `adapters/inbound/websocket/cryptofeed_orderbook_stream.py` | `CryptofeedOrderBookStream._translate_and_dispatch()` |
| Bronze writer (OHLCV) | `infrastructure/kafka/bronze_writer.py` | `KafkaBronzeWriter.run()` |
| Bronze storage | `infrastructure/storage/bronze/bronze_storage.py` | `BronzeStorage.append()` |
| Iceberg schemas | `infrastructure/storage/iceberg/schemas.py` | `BRONZE_SCHEMA`, etc. |
| Exchange config | `config/exchanges/bybit.yaml` | `enabled: true` (sin símbolos) |
| Env config | `config/env/development.yaml` | `exchanges.bybit.markets.spot.symbols: [BTC/USDT]` |
| Feeds config | `config/market_data/feeds.yaml` | `feeds.bybit.symbols: [BTC-USDT-PERP, ...]` |
| AppConfig validator | `ocm/config/schema.py` | `validate_exchanges()` line 864 |
| Config loader | `ocm/config/hydra_loader.py` | `_MODULE_PACKAGES` (falta exchanges) |
| Systemd market-data | `deploy/systemd/rendered/ocm-market-data.service` | `ExecStart=python -m market_data.main` |
| Systemd streaming | `deploy/systemd/rendered/ocm-streaming.service` | `ExecStart=python -m app.cli.streaming_hydra` |