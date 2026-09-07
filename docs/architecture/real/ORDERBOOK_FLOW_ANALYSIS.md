# ORDERBOOK FLOW — ANÁLISIS EXHAUSTIVO READ-ONLY

> **Fecha**: 2026-09-04
> **Audit**: Código real, configuración, systemd, Kafka topics, schemas

---

## 1. FLUJO REAL ACTUAL (END-TO-END)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         BYBIT WEBSOCKET (L2_BOOK)                           │
│  wss://stream.bybit.com/v5/public/linear                                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│              CRYPTOFEED ORDERBOOK STREAM (ACL)                              │
│  packages/market_data/adapters/inbound/websocket/                          │
│  cryptofeed_orderbook_stream.py                                             │
│                                                                             │
│  • FeedHandler + Bybit (cryptofeed.exchanges.Bybit)                        │
│  • Canal: L2_BOOK (order book level 2)                                     │
│  • max_depth: 50 (default)                                                 │
│  • Callback: _translate_and_dispatch(book, receipt_timestamp)              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    ▼                               ▼
           ┌─────────────────────┐         ┌─────────────────────┐
           │   SNAPSHOT          │         │      DELTA          │
           │ (book.delta=None)   │         │ (book.delta present)│
           └─────────┬───────────┘         └─────────┬───────────┘
                     │                               │
                     ▼                               ▼
           book.book.to_dict()              delta.get('bid')/delta.get('ask')
           → {bid:{price:size},             → [[price,size],...] raw lists
             ask:{price:size}}              BID='bid', ASK='ask'
                     │                               │
                     ▼                               ▼
           _sorted_levels(desc)            _sorted_levels per side
           → [(price_str, size_str)]       → (side_label, price_str, size_str)
                     │                               │
                     └───────────────┬───────────────┘
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│              ORDERBOOK KAFKA PRODUCER                                       │
│  packages/market_data/adapters/inbound/websocket/orderbook_producer.py     │
│                                                                             │
│  on_snapshot(exchange, symbol, timestamp_ms, bids[], asks[], depth,        │
│              checksum)                                                      │
│    → OrderBookSnapshotPayload → serialize → Kafka produce()                │
│    → topic: orderbook.raw (TOPIC_ORDERBOOK_RAW)                            │
│    → key: make_symbol_key(exchange, symbol)                                │
│    → headers: x-ocm-source="live", x-ocm-domain="orderbook"               │
│                                                                             │
│  on_delta(exchange, symbol, timestamp_ms, side, price, size)              │
│    → OrderBookDeltaPayload → serialize → Kafka produce()                   │
│    → topic: orderbook.raw (TOPIC_ORDERBOOK_RAW)                            │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│              KAFKA TOPIC: orderbook.raw                                     │
│  shared/kafka/topics.py: TOPIC_ORDERBOOK_RAW = "orderbook.raw"             │
│                                                                             │
│  Particiones: por symbol key (FIFO: snapshot antes que deltas)             │
│  Retención: 1h (alta frecuencia, short-window replay only)                 │
│  Schemas: OrderBookSnapshotPayload / OrderBookDeltaPayload                 │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│              ❌ NO CONSUMER EN MARKET_DATA                                  │
│                                                                             │
│  KafkaConsumerAdapter factories:                                           │
│  • for_bronze_writer()      → ohlcv.raw       ✅                           │
│  • for_feature_consumer()   → ohlcv.validated ✅                           │
│  • for_strategy_consumer()  → ohlcv.features  ✅                           │
│  • for_risk_gate()          → signals.raw     ✅                           │
│  • for_execution()          → signals.approved ✅                          │
│  • for_portfolio()          → orders.filled   ✅                           │
│  • ❌ for_orderbook()       → orderbook.raw    NO EXISTE                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. DETALLE DE COMPONENTES

### 2.1 CryptofeedOrderBookStream (ACL)
**Archivo**: `packages/market_data/adapters/inbound/websocket/cryptofeed_orderbook_stream.py`

```python
class CryptofeedOrderBookStream:
    def __init__(self, exchange, symbols, on_snapshot, on_delta, max_depth=50):
        # Exchange mapping (SSOT aquí)
        _EXCHANGE_CLASSES = {"bybit": Bybit, "kucoin": KuCoin}
        
    async def start(self):
        self._handler = FeedHandler()
        self._handler.add_feed(
            self._exchange_cls(
                symbols=self._symbols,
                channels=[L2_BOOK],
                callbacks={L2_BOOK: self._translate_and_dispatch},
                max_depth=self._max_depth,
            )
        )
        self._handler.run(start_loop=False, install_signal_handlers=False)
    
    async def _translate_and_dispatch(self, book, receipt_timestamp):
        # SNAPSHOT
        if getattr(book, "delta", None) is None:
            snapshot = book.book.to_dict()  # {'bid': {price: size}, 'ask': {...}}
            bids = self._sorted_levels(snapshot.get("bid", {}), descending=True)
            asks = self._sorted_levels(snapshot.get("ask", {}), descending=False)
            await self._on_snapshot(exchange, symbol, timestamp_ms, bids, asks, 
                                  depth, checksum)
        
        # DELTA
        else:
            for side_key, side_label in (("bid", "bid"), ("ask", "ask")):
                for price, size in delta.get(side_key, []):
                    await self._on_delta(exchange, symbol, timestamp_ms, 
                                       side_label, str(price), str(size))
```

**Validaciones que NO hace**:
- ❌ `book.sequence_number` no validado (gap detection)
- ❌ `book.checksum` extraído pero no verificado
- ❌ `book.timestamp` units assumidas (ms para Bybit, no validado)
- ❌ No hay reconexión automática con recovery de estado

### 2.2 OrderBookKafkaProducer
**Archivo**: `packages/market_data/adapters/inbound/websocket/orderbook_producer.py`

```python
class OrderBookKafkaProducer:
    topic = TOPIC_ORDERBOOK_RAW  # "orderbook.raw"
    group = GROUP_WS_ORDERBOOK_PRODUCER
    
    async def on_snapshot(self, exchange, symbol, timestamp_ms, bids, asks, 
                         depth=0, checksum=None):
        payload = OrderBookSnapshotPayload(...)
        await self._producer.produce(
            topic=self.topic,
            value=serialize(payload),
            key=make_symbol_key(exchange, symbol),
            headers={HEADER_SOURCE: "live", HEADER_DOMAIN: "orderbook"}
        )
    
    async def on_delta(self, exchange, symbol, timestamp_ms, side, price, size):
        payload = OrderBookDeltaPayload(...)
        await self._producer.produce(...)
```

### 2.3 Schemas Wire (SSOT: `shared/kafka/schemas/orderbook.py`)

```python
@dataclass(frozen=True)
class OrderBookSnapshotPayload(BasePayload):
    exchange: str
    symbol: str
    timestamp_ms: int
    bids: List[Tuple[str, str]]  # (price_str, size_str) desc
    asks: List[Tuple[str, str]]  # (price_str, size_str) asc
    depth: int
    checksum: Optional[int]

@dataclass(frozen=True)
class OrderBookDeltaPayload(BasePayload):
    exchange: str
    symbol: str
    timestamp_ms: int
    side: Literal["bid", "ask"]
    price: str
    size: str
```

---

## 3. DÓNDE SE ROMPA EL FLUJO

### Punto de ruptura 1: Consumer inexistente
```
orderbook.raw (Kafka) 
    → NO consumer group ORDERBOOK_BRONZE_WRITER
    → NO OrderbookBronzeWriter
    → NO persistencia
```

### Punto de ruptura 2: Schema Bronze inexistente
```
schemas.py define:
- BRONZE_SCHEMA (OHLCV, 12 fields)
- TRADES_SCHEMA (10 fields, IDs 101-110)
- DERIVATIVES_SCHEMA (8 fields, IDs 201-208)
- ❌ NO ORDERBOOK_SCHEMA
```

### Punto de ruptura 3: Tabla Bronze inexistente
```
BronceStorage.append() solo conoce bronze.ohlcv
    → get_catalog().load_table("bronze.ohlcv")
    → _BRONZE_COLS = [timestamp, open, high, low, close, volume, 
                      exchange, market_type, symbol, timeframe, 
                      ingestion_ts, run_id]
    → ❌ NO bronze.orderbook_snapshot / bronze.orderbook_delta
```

### Punto de ruptura 4: Orderbook Builder inexistente
```
No hay componente que:
- Mantenga estado L2 (bids/asks SortedDict)
- Aplique deltas incrementalmente
- Valide sequence_number / update_id
- Detecte gaps (sequence mismatch)
- Recupere via snapshot REST (fetch_order_book)
- Exporte book state para microprice/OFI/CVD
```

---

## 4. RESPUESTAS A PREGUNTAS CLAVE

| Pregunta | Respuesta | Evidencia |
|----------|-----------|-----------|
| **¿Quién produce orderbook.raw?** | `OrderBookKafkaProducer` (via `CryptofeedOrderBookStream` callbacks) | `orderbook_producer.py:86,137` |
| **¿Quién consume orderbook.raw?** | **NINGUNO** en market_data | `consumer.py` sin `for_orderbook()` |
| **¿Existe consumer hacia Bronze?** | **NO** | `KafkaConsumerAdapter` factories no incluyen orderbook |
| **¿Existe schema Bronze para orderbook?** | **NO** | `schemas.py` solo OHLCV/Trades/Derivatives |
| **¿Existe Orderbook Builder?** | **NO** | Stream solo traduce, no mantiene estado |
| **¿Está activo?** | Producer SÍ, Builder NO | `streaming_hydra.py` corre producer, no builder |
| **¿Mantiene estado?** | **NO** | Stateless translation only |
| **¿Procesa snapshot + delta?** | **SÍ** (producer) | `on_snapshot` + `on_delta` implementados |
| **¿Valida sequence/gap?** | **NO** | `_translate_and_dispatch` ignora `sequence_number` |
| **¿Tiene recovery?** | **NO** | Sin REST fallback para gap recovery |
| **¿Reproducible desde Bronze?** | **IMPOSIBLE** | No hay datos en Bronze |

---

## 5. QUÉ FALTA PARA ORDEBOOK REPRODUCIBLE DESDE BRONZE

### 5.1 Schema Iceberg (NUEVO en `schemas.py`)
```python
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
    NestedField(306, "side", StringType(), required=True),
    NestedField(307, "price", DoubleType(), required=True),
    NestedField(308, "size", DoubleType(), required=True),
    NestedField(309, "action", StringType(), required=True),  # new/update/delete
    NestedField(310, "ingestion_ts", TimestamptzType(), required=False),
    NestedField(311, "run_id", StringType(), required=False),
)
```

### 5.2 Consumer: OrderbookBronzeWriter (NUEVO)
```python
# packages/market_data/infrastructure/kafka/orderbook_bronze_writer.py

class OrderbookBronzeWriter:
    """Kappa stream processor: orderbook.raw → Bronze.orderbook"""
    
    def __init__(self, consumer, bronze_storage, dlq_producer=None):
        self._consumer = consumer
        self._bronze = bronze_storage  # Extended for orderbook tables
        
    async def run(self):
        while running:
            messages = await self._consumer.poll()
            for msg in messages:
                event = deserialize(msg.value, OrderBookSnapshotPayload|OrderBookDeltaPayload)
                if event.payload_type == "snapshot":
                    await self._bronze.append_snapshot(event)
                else:
                    await self._bronze.append_delta(event)
            await self._consumer.commit()
```

### 5.3 BronzeStorage Extendido (NUEVO)
```python
# packages/market_data/infrastructure/storage/bronze/bronze_storage.py

class BronzeStorage:
    def append_snapshot(self, df, symbol, exchange, run_id, update_id):
        # Table: bronze.orderbook_snapshot
        # Partition: exchange/market_type/symbol/ts_day
        
    def append_delta(self, df, symbol, exchange, run_id, update_id):
        # Table: bronze.orderbook_delta
        # Partition: exchange/market_type/symbol/ts_day
```

### 5.4 Orderbook Builder (NUEVO - para reconstrucción histórica)
```python
# packages/market_data/application/orderbook/builder.py

class OrderbookBuilder:
    """Reconstruye estado L2 desde Bronze snapshots + deltas"""
    
    def __init__(self, bronze_storage):
        self._bronze = bronze_storage
        self._books: dict[tuple, BookState] = {}  # (exchange, symbol) → state
    
    async def rebuild(self, exchange, symbol, from_ts, to_ts) -> BookState:
        """Replay: snapshot más cercano antes de from_ts + deltas hasta to_ts"""
        snapshot = await self._bronze.get_latest_snapshot(exchange, symbol, from_ts)
        deltas = await self._bronze.get_deltas(exchange, symbol, from_ts, to_ts)
        
        book = BookState.from_snapshot(snapshot)
        for delta in deltas:
            book.apply_delta(delta)
        return book
    
    async def get_book_at(self, exchange, symbol, timestamp) -> BookState:
        """Point-in-time reconstruction para backtesting/OFI"""
        ...
```

### 5.5 Gap Detection + Recovery (EN Orderbook Builder)
```python
# En BookState.apply_delta()
def apply_delta(self, delta: OrderBookDeltaPayload) -> GapInfo | None:
    expected_id = self.last_update_id + 1
    if delta.update_id != expected_id:
        # GAP DETECTADO
        gap = GapInfo(
            exchange=self.exchange,
            symbol=self.symbol,
            expected_update_id=expected_id,
            actual_update_id=delta.update_id,
            gap_size=delta.update_id - expected_id,
            detected_at=datetime.now(timezone.utc),
        )
        # Trigger recovery
        asyncio.create_task(self._recover_gap(gap))
        return gap
    self.last_update_id = delta.update_id
    # ... apply price/size change
    return None

async def _recover_gap(self, gap: GapInfo):
    # 1. Request REST snapshot for gap range
    snapshot = await self._ccxt_adapter.fetch_order_book(
        symbol=self.symbol,
        limit=max(gap.gap_size * 2, 100)
    )
    # 2. Apply snapshot (resets state)
    self.apply_snapshot(snapshot)
    # 3. Resume delta application
```

---

## 6. CLASIFICACIÓN DE PROBLEMAS ORDERBOOK

| Problema | Clasificación | Bloquea | Esfuerzo |
|----------|---------------|---------|----------|
| Producer orderbook.raw | ✅ IMPLEMENTED | — | — |
| Consumer orderbook.raw | ❌ MISSING | **G9** | Alto |
| Schema Bronze orderbook | ❌ MISSING | **G9** | Alto |
| Tabla Bronze orderbook | ❌ MISSING | **G9** | Alto |
| Orderbook Builder | ❌ MISSING | Calidad histórica | Alto |
| Gap detection | ❌ MISSING | Calidad temps real | Medio |
| Checksum validation | ❌ MISSING | Integridad | Bajo |
| Snapshot recovery | ❌ MISSING | Completitud histórica | Alto |
| Reproducible desde Bronze | ❌ MISSING | **G9** | Alto (requiere todo lo anterior) |

---

## 7. CONCLUSIÓN

**El flujo Orderbook está ROTO en la mitad**: Producer → Kafka ✅, pero Kafka → Bronze ❌.

**Gap funcional principal de B-49**: **Orderbook → Bronze** (G9 parcial).

**Para lograr orderbook reproducible desde Bronze** se requiere implementar la cadena completa: Consumer + Schema + Tabla + Builder + Gap Recovery.