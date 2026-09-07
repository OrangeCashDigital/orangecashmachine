# ARQUITECTURA OBJETIVO — OCM Market Data

> **Visión arquitectónica** — Hacia dónde debe evolucionar OCM
> **Basada en**: ADR-0013, ADR-0014, ADR-0017, ADR-0022, feed-model.md
> **Principios**: SSOT, DIP, Kappa, Clean/Hexagonal, Protocol Discovery Framework

---

## 1. MODELO CONCEPTUAL OBJETIVO

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ORANGE CASH MACHINE                                  │
│                    Market Data Platform (BC: market_data)                    │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        PROTOCOL DISCOVERY FRAMEWORK                          │
│  (ADR-0017) — Metodología única para descubrir, validar y modelar protocolos │
│  externos. Discovery Profiles por fuente (Bybit, Binance, Hyperliquid...)    │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                    ┌────────────────┴────────────────┐
                    ▼                                 ▼
         ┌─────────────────────┐             ┌─────────────────────┐
         │   REST ADAPTER      │             │   WEBSOCKET ADAPTER │
         │   (CCXT unified)    │             │   (Cryptofeed)      │
         │                     │             │                     │
         │ • OHLCV             │             │ • L2 Orderbook      │
         │ • Trades            │             │ • Trades            │
         │ • Funding/OI        │             │ • Funding/OI        │
         │ • Metadata          │             │ • Liquidations      │
         └─────────┬───────────┘             └─────────┬───────────┘
                   │                                   │
                   └───────────────┬───────────────────┘
                                   ▼
                    ┌─────────────────────────────────┐
                    │    MARKET / INSTRUMENT          │
                    │    DISCOVERY                    │
                    │                                 │
                    │ • Dynamic: load_markets()       │
                    │ • Static: config overrides      │
                    │ • Normalization: CCXT ↔ CF      │
                    │ • Metadata: tick, lot, status   │
                    └───────────────┬─────────────────┘
                                    ▼
                    ┌─────────────────────────────────┐
                    │    INSTRUMENT METADATA          │
                    │    REGISTRY (SSOT)              │
                    │                                 │
                    │ symbol → {tick, lot, category,  │
                    │  status, contract, precision,   │
                    │  trading_rules, source_prov}    │
                    └───────────────┬─────────────────┘
                                    ▼
                    ┌─────────────────────────────────┐
                    │    MARKET UNIVERSE              │
                    │    (Config-driven selection)    │
                    │                                 │
                    │ SSOT: config/market_data/       │
                    │ feeds.yaml + env overrides      │
                    │ Single format (canonical)       │
                    │ Auto-discovery opt-in           │
                    └───────────────┬─────────────────┘
                                    ▼
              ┌─────────────────────┴─────────────────────┐
              ▼                                           ▼
     ┌─────────────────────┐                     ┌─────────────────────┐
     │      REST           │                     │    WEBSOCKET        │
     │  (polling/batch)    │                     │   (streaming)       │
     │                     │                     │                     │
     │ OHLCV historical    │                     │ L2 Orderbook        │
     │ Trades historical   │                     │ Trades (real-time)  │
     │ Funding/OI polling  │                     │ Funding/OI stream   │
     │ Replay/backfill     │                     │ Liquidations        │
     └─────────┬───────────┘                     └─────────┬───────────┘
               │                                           │
               └───────────────────┬───────────────────────┘
                                   ▼
                    ┌─────────────────────────────────┐
                    │    NORMALIZATION LAYER          │
                    │ (ports/outbound/normalization.py)│
                    │                                 │
                    │ Provider-native → Canonical     │
                    │ Event (timestamp, source,       │
                    │  schema_version, quality,       │
                    │  lineage, payload)              │
                    └───────────────┬─────────────────┘
                                    ▼
                    ┌─────────────────────────────────┐
                    │    KAFKA (SSOT Operacional)     │
                    │                                 │
                    │ Topics:                         │
                    │ • ohlcv.raw                     │
                    │ • trades.raw                    │
                    │ • orderbook.raw (snapshot+delta)│
                    │ • funding.raw                   │
                    │ • oi.raw                        │
                    │ • liquidations.raw              │
                    │ • ohlcv.validated               │
                    │ • ohlcv.features                │
                    └───────────────┬─────────────────┘
                                    ▼
              ┌─────────────────────┴─────────────────────┐
              ▼                                           ▼
     ┌─────────────────────┐                     ┌─────────────────────┐
     │   KAPPA CONSUMERS   │                     │   ORDERBOOK BUILDER │
     │                     │                     │                     │
     │ • Bronze Writers    │                     │ • Snapshot state    │
     │   (ohlcv, trades,   │                     │ • Delta apply       │
     │    orderbook,       │                     │ • Sequence validate │
     │    funding, oi,     │                     │ • Gap detect        │
     │    liquidations)    │                     │ • Recovery (snap)   │
     │ • Quality Pipeline  │                     │ • Book state export │
     │ • Feature Pipeline  │                     └─────────────────────┘
     │ • Strategy Consumer │
     └─────────┬───────────┘
               │
               ▼
     ┌─────────────────────────────────┐
     │    ICEBERG MEDALLION LAYERS     │
     │                                 │
     │ Bronze  → append-only raw       │
     │ Silver  → dedup, validated      │
     │ Gold    → features, lineage     │
     │                                 │
     │ Tables per data type:           │
     │ • ohlcv, trades, orderbook,     │
     │   funding, oi, liquidations     │
     └─────────────────────────────────┘
```

---

## 2. SEPARACIÓN DE RESPONSABILIDADES (ADR-0014)

```
market_data (Bounded Context — Market Data Platform)
├── realtime_feeds          → WebSocket persistente, streaming, backpressure
│   ├── feed_orchestrator   → lifecycle management
│   ├── bybit_runner        → Cryptofeed Bybit (trades + L2)
│   ├── kucoin_runner       → Cryptofeed KuCoin
│   └── orderbook_stream    → L2_BOOK ACL + BookBuilder
│
├── external_ingestion      → REST polling, batch, replay, scheduling
│   ├── orchestrator        → ExternalIngestionOrchestrator
│   ├── normalizers/        → provider-native → CanonicalEvent
│   ├── bybit_adapter       → CCXT Bybit
│   ├── coinglass_adapter   → CoinGlass REST
│   ├── coinmarketcap_adapter → CoinMarketCap REST
│   └── glassnode_adapter   → Glassnode REST
│
├── normalization           → CanonicalEvent + schema evolution
│   ├── ohlcv_transformer   → pandas→polars bridge (transitorio)
│   └── event_normalizer    → provider-native → CanonicalEvent
│
├── data_quality            → timestamp validation, missing, duplicates,
│   ├── policies            → outlier, schema evolution, source reliability
│   └── checkers            → GE/native checkers
│
├── kafka_boundary          → Publishers + Consumers (Kappa)
│   ├── publishers/         → OHLCV, Trades, Orderbook, Funding, OI, Liq
│   ├── consumers/          → Bronze Writers, Quality, Features, Strategy
│   └── serializer          → SSOT wire format
│
└── storage                 → Iceberg materialización
    ├── bronze/             → append-only raw (ohlcv, trades, orderbook, ...)
    ├── silver/             → dedup, validated
    └── gold/               → features, lineage
```

---

## 3. MARKET / INSTRUMENT DISCOVERY — OBJETIVO

### Discovery vs Universe Selection (Distinción Fundamental)

| Concepto | Pregunta | Responsabilidad |
|----------|----------|-----------------|
| **Discovery** | "¿Qué existe en el exchange?" | Protocol Discovery Framework |
| **Instrument Metadata** | "¿Cuáles son las reglas de este instrumento?" | Instrument Registry |
| **Market Universe** | "¿Qué decide OCM observar/operar?" | Config-driven (SSOT) |

### Instrument Registry (SSOT Único)
```python
# Conceptual — shared/instrument_registry.py
@dataclass(frozen=True)
class InstrumentMetadata:
    symbol: str                    # Canónico: "BTC/USDT"
    exchange: str                  # "bybit"
    category: Literal["spot", "linear", "inverse", "option"]
    status: Literal["trading", "settling", "pre_launch", "delisted"]
    base_asset: str                # "BTC"
    quote_asset: str               # "USDT"
    tick_size: Decimal             # "0.1"
    lot_size: Decimal              # "0.001"
    min_qty: Decimal               # "0.001"
    max_qty: Decimal               # "10000"
    contract_size: Optional[Decimal]  # Para futures
    price_precision: int           # 1
    qty_precision: int             # 3
    source_provenance: Provenance  # PROTOCOL | DOCUMENTATION | UPSTREAM_LIBRARY
    discovered_at: datetime
    last_validated_at: datetime
```

### Market Universe Config (SSOT Único)
```yaml
# config/market_data/universe.yaml (NUEVO - reemplaza feeds.yaml + env symbols)
market_universe:
  bybit:
    enabled: true
    discovery:
      mode: "auto"          # auto | static | hybrid
      auto_discover: true   # Usar load_markets() + filtrar
    symbols:
      spot:
        - BTC/USDT
        - ETH/USDT
      linear:
        - BTC/USDT
        - ETH/USDT
      inverse:
        - BTC/USD
    metadata_overrides: {}  # Opcional: override tick/lot si exchange wrong
```

### Normalización de Formatos
```
CCXT format (REST)          Cryptofeed format (WS)      Canonical (Internal)
─────────────────          ─────────────────────       ──────────────────
BTC/USDT                   BTC-USDT-PERP               BTC/USDT (spot)
BTC/USDT:USDT              BTC-USDT                    BTC/USDT (linear)
BTC/USD                    BTC-USD-PERP                BTC/USD (inverse)
```

---

## 4. ORDERBOOK BUILDER — OBJETIVO

```
Bybit WebSocket (L2_BOOK)
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│              CRYPTOFEED ORDERBOOK STREAM (ACL)                  │
│  • Confina tipos vendor (FeedHandler, OrderBook, L2_BOOK)       │
│  • Extrae: snapshot (book.delta=None), delta (book.delta)       │
│  • Valida: checksum, sequence_number, timestamp units           │
└─────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│              ORDERBOOK BUILDER (Estado L2 Reconstruido)         │
│                                                                 │
│  State per (exchange, symbol):                                  │
│  ├── bids: SortedDict[price → size]  (desc)                    │
│  ├── asks: SortedDict[price → size]  (asc)                     │
│  ├── last_update_id: int        (sequence validation)          │
│  ├── last_snapshot_ts: int      (staleness detection)          │
│  └── checksum: Optional[int]    (integrity)                    │
│                                                                 │
│  Operations:                                                     │
│  ├── apply_snapshot(bids, asks, update_id, checksum)           │
│  ├── apply_delta(side, price, size, update_id)                 │
│  ├── validate_sequence(expected_id, actual_id) → GapInfo       │
│  ├── detect_gap() → GapInfo                                    │
│  ├── request_recovery(gap_start, gap_end) → RecoveryTask       │
│  └── export_book(depth) → (bids[], asks[])                     │
└─────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│              KAFKA PRODUCER (OrderBookKafkaProducer)            │
│                                                                 │
│  on_snapshot → OrderBookSnapshotPayload → orderbook.raw        │
│  on_delta    → OrderBookDeltaPayload   → orderbook.raw        │
│  headers: x-ocm-source="live", x-ocm-domain="orderbook"        │
│  key: make_symbol_key(exchange, symbol) → FIFO per symbol      │
└─────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│              KAPPA CONSUMERS                                    │
│                                                                 │
│  1. OrderbookBronzeWriter  → orderbook.raw → Bronze.orderbook  │
│  2. BookBuilder (optional) → reconstructed L2 state for        │
│     microprice, OFI, CVD, etc.                                  │
│  3. Quality Pipeline      → freshness, sequence gaps,          │
│     checksum validation                                         │
└─────────────────────────────────────────────────────────────────┘
```

### Gap Detection & Recovery
```python
@dataclass(frozen=True)
class GapInfo:
    exchange: str
    symbol: str
    expected_update_id: int
    actual_update_id: int
    gap_size: int
    detected_at: datetime
    recovery_status: Literal["pending", "in_progress", "recovered", "failed"]

# Recovery flow:
# 1. Detect gap via sequence_number mismatch
# 2. Pause delta application
# 3. Request REST snapshot (CCXT fetch_order_book) for gap range
# 4. Apply snapshot → resume deltas
# 5. If snapshot unavailable → mark gap as failed, alert
```

---

## 5. BRONZE SCHEMA — ORDERBOOK

```python
# infrastructure/storage/iceberg/schemas.py — NUEVO
# IDs 301-320 reservados para Orderbook

ORDERBOOK_SNAPSHOT_SCHEMA = Schema(
    NestedField(301, "timestamp", TimestamptzType(), required=True),
    NestedField(302, "exchange", StringType(), required=True),
    NestedField(303, "market_type", StringType(), required=True),
    NestedField(304, "symbol", StringType(), required=True),
    NestedField(305, "update_id", LongType(), required=True),
    NestedField(306, "bids_price", ListType(DoubleType()), required=True),
    NestedField(307, "bids_size", ListType(DoubleType()), required=True),
    NestedField(308, "asks_price", ListType(DoubleType()), required=True),
    NestedField(309, "asks_size", ListType(DoubleType()), required=True),
    NestedField(310, "depth", LongType(), required=False),
    NestedField(311, "checksum", LongType(), required=False),
    NestedField(312, "ingestion_ts", TimestamptzType(), required=False),
    NestedField(313, "run_id", StringType(), required=False),
)

ORDERBOOK_DELTA_SCHEMA = Schema(
    NestedField(301, "timestamp", TimestamptzType(), required=True),
    NestedField(302, "exchange", StringType(), required=True),
    NestedField(303, "market_type", StringType(), required=True),
    NestedField(304, "symbol", StringType(), required=True),
    NestedField(305, "update_id", LongType(), required=True),
    NestedField(306, "side", StringType(), required=True),  # bid/ask
    NestedField(307, "price", DoubleType(), required=True),
    NestedField(308, "size", DoubleType(), required=True),
    NestedField(309, "action", StringType(), required=True),  # new/update/delete
    NestedField(310, "ingestion_ts", TimestamptzType(), required=False),
    NestedField(311, "run_id", StringType(), required=False),
)
```

### Tabla Bronze: `bronze.orderbook_snapshot` + `bronze.orderbook_delta`
- Particionado: exchange / market_type / symbol / ts_day
- Retención: 7 días (alta frecuencia) → Silver para histórico

---

## 6. UNIFIED EXCHANGE ADAPTER — ANÁLISIS

### Qué resolvería
1. **Eliminar duplicación**: `CCXTAdapter` (REST) + `BybitCryptofeedRunner` (WS) → una clase por exchange
2. **Unificar Market Universe**: Single source of symbols, normalización de formatos
3. **Simplificar config**: Un solo `enabled: true` controla REST + WS
4. **Coordinar dual mode**: Parity validation entre REST y WS en tiempo real

### Qué NO resolvería
1. **Bronze schema para orderbook** — requiere schema Iceberg + consumer nuevo
2. **Gap recovery en L2_BOOK** — lógica de cryptofeed, no de adapter
3. **Systemd unification** — decisión de deployment, no código

### Abstracción equivalente existente
- `MarketDataSource` protocol (ports/inbound/market_data_source.py) — `subscribe_trades()`, `start()`, `stop()`
- `FeedRunnerProtocol` (websocket/feed_runner_protocol.py) — `run_until_stopped()`
- `ExchangeAdapter` (adapters/outbound/exchange/base.py) — `fetch_ohlcv()`, `fetch_trades()`
- **No hay protocolo unificado** que combine ambos

### Clasificación: **LATER** (post-B-49)
**Justificación**: Requiere ADR, exception BC-07, refactor mayor. Prioridad: Orderbook→Bronze (G9 blocker) y Market Universe unificado primero.

---

## 7. PRODUCTION GATES G4-G9 — OBJETIVO

| Gate | Requisito | Implementación Objetivo |
|------|-----------|-------------------------|
| **G4** | Systemd units valid | Ambas units active, restart tested |
| **G5** | Kafka connectivity | Bootstrap consistent, producers/consumers healthy |
| **G6** | Infra health | Kafka + Redis + Iceberg + Schema Registry |
| **G7** | Health checks | `/health`, `/ready` + health_check.sh all PASS |
| **G8** | IS_STUB=False | Live executor + risk guards active |
| **G9** | Bronze freshness | OHLCV <15min + Orderbook <5min + run_id valid |

---

## 8. PRINCIPIOS ARQUITECTÓNICOS A CUMPLIR

| Principio | Aplicación en Market Data |
|-----------|---------------------------|
| **SSOT** | Un solo Market Universe config, un solo Instrument Registry |
| **Separation of Concerns** | Discovery ≠ Ingestion ≠ Storage ≠ Quality |
| **DDD** | Market Data = BC propietario; Instrument = Value Object |
| **Hexagonal / Ports & Adapters** | Puertos inbound (MarketDataSource) / outbound (Kafka, Storage) |
| **Dependency Inversion** | Composition Root único (BC-38), DIP en adapters |
| **Idempotency** | event_id dedup L1+L2, Bronze append idempotente |
| **Deterministic processing** | Mismos inputs → mismos outputs, replay desde Kafka |
| **Replayability** | Kappa: replay desde ohlcv.raw/orderbook.raw |
| **Observability** | Metrics + Logging + Tracing + Lineage en cada hop |
| **Fail-fast** | Config validation at startup, NullPublisher prohibido en prod |
| **Explicit contracts** | Pydantic schemas, Protocol classes, Schema evolution |
| **Schema evolution** | Iceberg schema evolution rules, backward compat |
| **Testability** | Unit + Integration + Contract tests, golden files |
| **Recovery** | Gap detection, snapshot recovery, at-least-once |
| **Backpressure** | aiometer limits, Kafka consumer max_poll_records |
| **Data lineage** | run_id + git_hash + schema_version en cada evento |
| **Auditability** | Config snapshots, promotion rule, provenance tags |
| **Production safety** | dry_run=True default, require_explicit_start en prod |

---

## 9. CRITERIOS DE ACEPTACIÓN ARQUITECTÓNICA

- [ ] **Un Market Universe config** (SSOT) consumido por REST y WS
- [ ] **Un Instrument Registry** con metadata completa y provenance
- [ ] **Orderbook → Bronze** funcional con schema reproducible
- [ ] **Gap detection + recovery** en Orderbook Builder
- [ ] **Protocol Discovery Framework** implementado para Bybit (profile)
- [ ] **Unified config** para REST + WS por exchange
- [ ] **Single systemd service** configurable (rest|dual|websocket)
- [ ] **G4-G9 all PASS** con evidencia
- [ ] **Documentation** actualizada y coherente

---

*Documento vivo — actualizar con cada ADR que afecte Market Data Platform*