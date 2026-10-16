# AUDITORÍA COMPARATIVA — Modelos de Referencia: Recovery & Resync de Market Data y OrderBook

- **ID**: MD-RECOVERY-003
- **Fecha**: 2026-09-21
- **Modo**: READ-ONLY. Cero modificaciones.
- **Scope**: Comparación arquitectónica profunda de recuperación de Market Data (OHLCV) y OrderBook entre OCM y tres modelos de referencia (Nautilus, Hummingbot, Freqtrade).
- **Complementa**: `docs/audits/AUDIT_OCM_ohlcv-repair-backfill-execution-paths_2026-09-21.md` (MD-RECOVERY-001), `docs/audits/AUDIT_OCM_ohlcv-repair-backfill-causality_2026-09-21.md` (MD-RECOVERY-002).

---

## 1. EXECUTIVE SUMMARY

**Veredicto**: `CONFIRMED` — Los tres modelos de referencia demuestran que la recuperación de datos y la sincronización de OrderBook son problemas resueltos de formas fundamentalmente distintas. Ninguno es directamente aplicable a OCM sin adaptación. Los hallazgos clave:

**Para OHLCV**:
- **Hummingbot** es el único que demuestra explícitamente un patrón REST+WS unificado con detección de gaps implícita (`deque` con overwrite, `fill_historical_candles` triggered por WS).
- **Freqtrade** demuestra que gaps se manejan con `fill_missing=True` — rellenar con velas "No action", no con recuperación activa.
- **Nautilus** no tiene concepto de backfill/repair en Python — la recuperación está delegada al rust `DataEngine` y al `ParquetDataCatalog`.

**Para OrderBook**:
- **Hummingbot** es el único que demuestra el patrón completo **SNAPSHOT → WS DELTAS → SEQUENCE VALIDATION → GAP → RESET/RESYNC**:
  - `OrderBookTracker._order_book_diff_router()` verifica: `if order_book.snapshot_uid > ob_message.update_id: messages_rejected`.
  - `OrderBookTracker._track_single_book()` usa `restore_from_snapshot_and_diffs()`.
  - `_past_diffs_windows` (deque maxlen=32) almacena diffs para replay.
  - `_saved_message_queues` (deque maxlen=1000) almacena mensajes antes del snapshot.
- **Nautilus** tiene `BookSnapshotter` y `BookUpdater` con `OrderBook.reset()`, `update_count`, `snapshot_uid`, pero no demuestra un patrón de resync explícito a nivel visible.
- **Freqtrade** NO tiene WS nativo. `fetch_l2_order_book()` es una llamada REST puntual sin secuencia.

**DECISIÓN ARQUITECTÓNICA**: No se propone implementación. Este documento establece solo qué patrones están demostrados, cuáles son comunes, y cuáles son aplicables a OCM.

---

## 2. ZIP INVENTORY

| Referencia | Archivo ZIP | Proyecto | Versión/commit | Evidencia |
|---|---|---|---|---|
| **Nautilus** | `docs/nautilus_trader-develop.zip` (28.4 MB) | NautilusTrader | commit `e892532eebeafb811bcc59c4416e06c4d824e2a7` (2026-08-14) | Directorio `.cargo/`, `Cargo.toml`, `Cargo.lock`, `rust-toolchain.toml`. Commit 28.4MB ZIP. |
| **Hummingbot** | `docs/hummingbot-master.zip` (3.9 MB) | Hummingbot | commit `2bfaccc48dd49e71a5b6d9b3011808e127dd00cd` (2026-07-30) | `.github/workflows/`, `Makefile`, `bin/hbot`. Python project. |
| **Freqtrade** | `docs/freqtrade-develop.zip` (45.1 MB) | Freqtrade | commit `1c8edfe4d1e8d11bd4b40e8fc3237c26c3a60e15` (2026-08-13) | `.github/workflows/ci.yml`, `.pre-commit-config.yaml`. Python project. |

### Estructura relevante

**Nautilus** (`nautilus_trader-develop/`):
- `python/nautilus_trader/` — Python bindings (PyO3), thin wrappers sobre Rust
  - `data/__init__.py` → `from nautilus_trader._libnautilus.data import *`
  - `adapters/` → 22 adapters (binance, bybit, okx, databento, etc.)
  - `persistence/` → `loaders.py` (CSV/Parquet), `catalog.py` (ParquetDataCatalog)
  - `backtest/` → backtest engine
  - `_libnautilus/` → Rust compiled extensions
- `crates/data/` — Rust core: `DataEngine`, `BarAggregator`, `BookSnapshotter`, `BookUpdater`, `ParquetDataCatalog`
- `crates/model/src/python/orderbook/` — `OrderBook`, `OwnOrderBook`, `BookLevel`
- `crates/persistence/` — `ParquetDataCatalog`, `backend/session.rs`, `wranglers/` (trade, quote, depth, delta, bar)
- `test_data/` — Sample data (binance, bybit, databento, tardis)

**Hummingbot** (`hummingbot-master/`):
- `hummingbot/data_feed/candles_feed/` — `CandlesBase`, `CandlesFactory`, `candles_base.py`, `market_data_provider.py`
- `hummingbot/core/data_type/` — `order_book.pyx`, `order_book_tracker.py`, `order_book_message.pyx`
- `hummingbot/model/` — `market_data.py` (SQLAlchemy)
- `hummingbot/connector/` — `connector_base.pyx`
- `hummingbot/data_feed/candles_feed/bybit_perpetual_candles/` — Bybit-specific candle feed

**Freqtrade** (`freqtrade-develop/`):
- `freqtrade/data/` — `dataprovider.py`, `history/` (`datahandlers/`, `history_utils.py`)
- `freqtrade/exchange/` — `exchange.py` (CCXT wrapper), `exchange_types.py`
- `freqtrade/data/history/datahandlers/` — `featherdatahandler.py`, `parquetdatahandler.py`, `jsondatahandler.py`, `idatahandler.py`

---

## 3. NAUTILUS ANALYSIS

### 3.1 OHLCV

| Aspecto | Detalle | Evidencia |
|---|---|---|
| **Cómo obtiene historical bars** | `RequestBars` → `DataClient` → `DataEngine.execute()` → `DataResponse.Bars` | `crates/data/src/engine/requests.rs:238`, `crates/data/src/engine/streaming.rs` |
| **Cómo detecta faltantes** | `BarAggregator` construye barras a partir de ticks. `continuous_future` maneja transiciones de contratos con validación de `chain discontinuity`. | `crates/data/src/aggregation.rs`, `crates/data/src/engine/requests.rs:560-570` |
| **Quién decide recuperar** | No existe concepto de "decidir recuperar". El `DataEngine` recibe `RequestBars` y obtiene datos. | `crates/data/src/engine/mod.rs` |
| **Quién ejecuta la recuperación** | `DataEngine` via `DataClient` (exchange adapter). | `crates/data/src/engine/requests.rs` |
| **Qué interfaz utiliza** | `RequestBars`, `SubscribeBars`, `RequestBookSnapshot`, `SubscribeBookDeltas`, `RequestBookDepth` — `DataCommand` messages. | `nautilus_common/messages/data.rs` |
| **Cómo persiste** | `ParquetDataCatalog` — escribe/lee Parquet files. `register_catalog()` permite leer desde catálogo. | `crates/persistence/src/python/catalog.rs`, `crates/data/src/engine/streaming.rs` |
| **Cómo valida** | `book_check_integrity()`, `validate_accepted_buffer()`, `check_candles_sorted_and_equidistant` (en Python connectors). | `crates/model/src/python/orderbook/book.rs:201`, `crates/model/src/python/orderbook/own.rs` |
| **Si distingue historical de live** | Sí — `RequestBars` vs `SubscribeBars`. `skip_catalog_data` y `update_catalog` parameters. | `crates/data/src/engine/streaming.rs` |
| **Cómo maneja inconsistencias** | `continuous_future` con `chain_discontinuity` check y `adjustment`. `BookUpdater` registra errores al aplicar deltas. | `crates/data/src/engine/requests.rs:560-570`, `crates/data/src/engine/book.rs:103-104` |

### 3.2 ORDERBOOK

| Aspecto | Detalle | Evidencia |
|---|---|---|
| **Cómo recibe snapshots** | `RequestBookSnapshot` / `SubscribeBookSnapshots` → `BookSnapshotter` genera snapshots periódicos | `crates/data/src/engine/book.rs:203-301`, `crates/data/src/engine/mod.rs:57` |
| **Cómo recibe updates** | `SubscribeBookDeltas` → `BookUpdater.handle(OrderBookDeltas)` → `book.apply_deltas(deltas)` | `crates/data/src/engine/book.rs:93-118` |
| **Cómo identifica secuencia** | `OrderBook.sequence` (u64 getter). `OrderBook.update_count` (u64). `_last_diff_uid`. | `crates/model/src/python/orderbook/book.rs:71`, `order_book.pyx:50-51` |
| **Qué ocurre ante pérdida de continuidad** | `BookUpdater` registra error si `book.apply_deltas()` falla. No hay reset automático visible. | `crates/data/src/engine/book.rs:103-104` |
| **Qué ocurre ante reconnect** | `SocketReconnectLookup` en `nautilus_common::clients`. No se ve lógica de resync explícita en Python/Rust visible. | `crates/data/src/engine/mod.rs:38` |
| **Qué ocurre ante sequence gap con WS conectado** | `OrderBook.apply_deltas()` valida delta. `BookUpdater` procesa diferencias. Sin gap detection visible. | `crates/data/src/engine/book.rs` |
| **Cómo hace resync** | `OrderBook.reset()` — reset completo. `BookSnapshotter` genera snapshots periódicos. | `crates/model/src/python/orderbook/book.rs:99-103`, `crates/data/src/engine/book.rs:203-301` |
| **Si utiliza REST** | `RequestBookSnapshot` — explícito REST snapshot request. `DatabentoDataLoader` para datos históricos. | `crates/data/src/engine/requests.rs:238`, `adapters/databento/__init__.py` |
| **Cómo sincroniza snapshot y updates** | `BookSnapshotter` genera snapshots periódicas. `BookUpdater` aplica deltas. `OrderBook.apply_depth()` para snapshot. | `crates/data/src/engine/book.rs:203-301` |
| **Cómo evita pérdida/duplicación** | `OrderBook` usa `set[OrderBookEntry]` para bid/ask. `truncateOverlapEntries` para evitar duplicados cruzados. | `crates/model/src/python/orderbook/own.rs` |
| **Cuándo considera válido** | `update_count > 0` para snapshots. `book_check_integrity()` para validación. `accepted_buffer_ns` para tolerancia. | `crates/data/src/engine/book.rs:290-291`, `crates/model/src/python/orderbook/book.rs:201-204` |

### 3.3 Patrón Nautilus resumido

Nautilus NO tiene un patrón explícito de "detect gap → decide → recover → persist → verify". El `DataEngine` maneja todo a un nivel bajo:

- Los datos vienen de `DataClient` (exchange adapter)
- `ParquetDataCatalog` persiste datos históricos
- `BookSnapshotter` genera snapshots periódicas
- `BookUpdater` procesa deltas
- `OrderBook.reset()` existe pero no se invoca automáticamente ante gaps
- La validación es implícita en el orden de operaciones

---

## 4. HUMMINGBOT ANALYSIS

### 4.1 OHLCV

| Aspecto | Detalle | Evidencia |
|---|---|---|
| **Cómo obtiene historical bars** | `CandlesBase.get_historical_candles()` — paginated REST fetch con `HistoricalCandlesConfig` | `candles_base.py:244-274` |
| **Cómo detecta faltantes** | `check_candles_sorted_and_equidistant()` — detecta timestamps no ordenados o intervalos no equidistantes | `candles_base.py:276-293` |
| **Quién decide recuperar** | `fill_historical_candles()` — triggered automáticamente cuando WS entrega el primer candle | `candles_base.py:411-435` |
| **Quién ejecuta la recuperación** | `fetch_candles()` — REST paginado | `candles_base.py:310-354` |
| **Qué interfaz utiliza** | `CandlesBase` con `rest_url`, `candles_url`, `wss_url` | `candles_base.py:24` |
| **Cómo persiste** | `_candles = deque(maxlen=max_records)` — buffer circular en memoria. `MarketData` SQLAlchemy model para orden book. | `candles_base.py:57`, `model/market_data.py` |
| **Cómo valida** | `check_candles_sorted_and_equidistant()` — valida orden y equidistancia. Si falla → `_reset_candles()`. | `candles_base.py:276-293` |
| **Si distingue historical de live** | Sí — `fetch_candles()` para REST, `listen_for_subscriptions()` para WS. `fill_historical_candles()` llena desde REST cuando WS empieza. | `candles_base.py:411-435`, `_process_websocket_messages_task` |
| **Cómo maneja inconsistencias** | `_reset_candles()` limpia deque y event en malformed data. `drop_duplicates()` en `get_historical_candles()`. | `candles_base.py:295-298`, `candles_base.py:258` |

### 4.2 ORDERBOOK (CRÍTICO)

| Aspecto | Detalle | Evidencia |
|---|---|---|
| **Cómo recibe snapshots** | `OrderBookTracker._init_order_books()` → `_data_source.get_new_order_book()` (REST snapshot) | `order_book_tracker.py:388-399` |
| **Cómo recibe updates** | `_data_source.listen_for_order_book_diffs()` → `_order_book_diff_stream` → `_order_book_diff_router()` → `order_book.apply_diffs()` | `order_book_tracker.py:522-591`, `522-591` |
| **Cómo identifica secuencia** | `update_id` en `OrderBookMessage`. `OrderBook._snapshot_uid` (u64). `OrderBook._last_diff_uid` (u64). | `order_book.pyx:50-51`, `order_book_message.pyx` |
| **Qué ocurre ante pérdida de continuidad** | `_order_book_diff_router()`: `if order_book.snapshot_uid > ob_message.update_id: messages_rejected`. Stale diffs rechazados. | `order_book_tracker.py:556-558` |
| **Qué ocurre ante reconnect** | `NetworkIterator` maneja reconexión. `_update_last_trade_prices_loop()` hace REST fallback para last_trade_price. | `order_book_tracker.py:361-387` |
| **Qué ocurre ante sequence gap con WS conectado** | **KEY**: `_track_single_book()` recibe diffs y snapshots. Diffs con `update_id < snapshot_uid` son rechazados. Snapshots pasan por `restore_from_snapshot_and_diffs()`. | `order_book_tracker.py:634-674` |
| **Cómo hace resync** | **`restore_from_snapshot_and_diffs(message, past_diffs)`** — reconstruye el book desde snapshot + diffs almacenados en `_past_diffs_windows` (deque maxlen=32). | `order_book_tracker.py:664-665` |
| **Si utiliza REST** | Sí — `get_new_order_book()` para snapshot inicial. `_update_last_trade_prices_loop()` para last_trade_price. | `order_book_tracker.py:388-399`, `361-387` |
| **Cómo sincroniza snapshot y updates** | **`_order_book_diff_router()`**: `if order_book.snapshot_uid > ob_message.update_id: REJECT`. **`_saved_message_queues`**: mensajes recibidos antes del snapshot se almacenan (deque maxlen=1000) y se procesan cuando el book está listo. | `order_book_tracker.py:556-558`, `542-544` |
| **Cómo evita pérdida/duplicación** | `_past_diffs_windows` (deque maxlen=32) almacena diffs para replay. `_saved_message_queues` (deque maxlen=1000) almacena mensajes pre-snapshot. `message_queue` por trading pair. | `order_book_tracker.py:249`, `254` |
| **Cuándo considera válido** | `_order_books_initialized.is_set()` — evento que se establece cuando todos los books tienen snapshot inicial con bids y asks. | `order_book_tracker.py:284`, `399` |

### 4.3 Patrón Hummingbot resumido

**HUMMINGBOT DEMUESTRA EL PATRÓN COMPLETO:**

```
BASELINE (REST snapshot)
  ↓
WS UPDATES (deltas con update_id)
  ↓
VALIDATE SEQUENCE (snapshot_uid > update_id → REJECT)
  ↓
GAP (diff perdido o WS desconectado)
  ↓
RESET/RESYNC (restore_from_snapshot_and_diffs)
  ↓
REST SNAPSHOT (get_new_order_book)
  ↓
RESUME (_order_books_initialized.set())
```

Componentes clave:
- `OrderBookTracker` orquesta todo
- `_order_book_diff_router()` valida secuencia
- `_track_single_book()` procesa diffs y snapshots
- `_past_diffs_windows` almacena diffs para replay
- `_saved_message_queues` almacena mensajes pre-snapshot
- `restore_from_snapshot_and_diffs()` reconstruye book

---

## 5. FREQTRADE ANALYSIS

### 5.1 OHLCV

| Aspecto | Detalle | Evidencia |
|---|---|---|
| **Cómo obtiene historical bars** | `download_data_main()` → `Exchange.fetch_ohlcv()` (CCXT). `load_data()` carga desde disco. | `data/history/history_utils.py:702`, `exchange/exchange.py` |
| **Cómo detecta faltantes** | `clean_ohlcv_dataframe()` con `fill_missing=True` rellena gaps con "No action" candles. `_check_empty_df()` detecta price gaps > 0.1. | `data/history/datahandlers/idatahandler.py:391-439` |
| **Quién decide recuperar** | No hay decisión automática. `refresh_backtest_ohlcv_data()` re-descarga datos para backtest. | `data/history/history_utils.py` |
| **Quién ejecuta la recuperación** | `download_data_main()` — descarga desde exchange a disco. No hay recuperación automática en runtime. | `data/history/history_utils.py:702` |
| **Qué interfaz utiliza** | `DataProvider` wraps `Exchange` + `IDataHandler`. `load_pair_history()` carga desde disco. | `data/dataprovider.py:39` |
| **Cómo persiste** | Disco local: feather, parquet, json, jsongz. `IDataHandler.ohlcv_store()`, `ohlcv_append()`. | `data/history/datahandlers/idatahandler.py:72-146` |
| **Cómo valida** | `clean_ohlcv_dataframe()`, `trades_df_remove_duplicates()`. `_check_empty_df()` con gap detection. | `data/history/datahandlers/idatahandler.py:391-439` |
| **Si distingue historical de live** | No — datos se descargan y almacenan en disco. No hay streaming en tiempo real nativo. | — |
| **Cómo maneja inconsistencias** | `fill_missing=True` — rellena gaps con "No action" candles. `drop_incomplete` — elimina velas incompletas. | `data/history/datahandlers/idatahandler.py:355-400` |

### 5.2 ORDERBOOK

| Aspecto | Detalle | Evidencia |
|---|---|---|
| **Cómo recibe snapshots** | `fetch_l2_order_book(pair, limit)` — llamada REST puntual vía CCXT. | `exchange/exchange.py:2220-2234` |
| **Cómo recibe updates** | **NO HAY WS NATIVO**. Freqtrade no tiene WebSocket para orderbook. | — |
| **Cómo identifica secuencia** | **NO HAY**. CCXT `OrderBook` es un dict `{asks: [[price, size], ...], bids: [[price, size], ...]}` sin `update_id` ni `sequence`. | `exchange/exchange_types.py` |
| **Qué ocurre ante pérdida de continuidad** | **NO DETECTADO**. Sin secuencia, no hay forma de detectar gaps. | — |
| **Qué ocurre ante reconnect** | **NO APLICA** — sin WS, no hay reconnect. | — |
| **Qué ocurre ante sequence gap con WS conectado** | **NO APLICA** — sin WS. | — |
| **Cómo hace resync** | `fetch_l2_order_book()` — nueva llamada REST puntual. Sin patrón de resync. | `exchange/exchange.py:2220` |
| **Si utiliza REST** | Sí — `fetch_l2_order_book()` para snapshot. `fetch_ohlcv()` para OHLCV. | `exchange/exchange.py:2220`, `2207` |
| **Cómo sincroniza snapshot y updates** | **NO APLICA** — sin WS, no hay sincronización. | — |
| **Cómo evita pérdida/duplicación** | `trades_df_remove_duplicates()` en datos de trades. Sin orderbook dedup. | `data/converter.py` |
| **Cuándo considera válido** | Sin mecanismo de validación de orderbook. | — |

### 5.3 Patrón Freqtrade resumido

Freqtrade NO tiene WS ni patrón de snapshot+delta para OrderBook. Es un sistema basado en datos históricos almacenados en disco. Los gaps en OHLCV se manejan con `fill_missing=True` (velas "No action"). No hay recuperación activa de datos en runtime.

---

## 6. OHLCV COMPARISON

### 6.1 Cadena completa OHLCV

| Paso | Nautilus | Hummingbot | Freqtrade | OCM |
|---|---|---|---|---|
| **Historical fetch** | `RequestBars` → `DataClient` → `DataEngine` → `BarsResponse` | `CandlesBase.get_historical_candles()` → REST paginado | `Exchange.fetch_ohlcv()` → CCXT → disco | `HistoricalFetcherAsync` → CCXT REST paginado |
| **Live ingestion** | `SubscribeBars` → WS → `DataEngine` → `BarAggregator` | `CandlesBase.listen_for_subscriptions()` → WS → `deque` | ❌ No hay WS | `IncrementalStrategy` → CCXT REST (hardcoded `mode="incremental"`) |
| **Gap detection** | ❌ No visible en Python/Rust visible | `check_candles_sorted_and_equidistant()` → `_reset_candles()` | `clean_ohlcv_dataframe(fill_missing=True)` → "No action" candles | `QualityPipeline._scan_and_emit_gaps()` → metrics/logs solo |
| **Gap recovery** | ❌ No hay concepto | `fill_historical_candles()` → REST fill cuando WS entrega primer candle | `refresh_backtest_ohlcv_data()` → re-descarga | ❌ `RepairStrategy` existe pero `ctx.storage=None`, `assert` falla |
| **Persistence** | `ParquetDataCatalog` → archivos Parquet | `_candles = deque(maxlen)` → memoria; `MarketData` → SQLite | Disco local (feather/parquet/json) | Kafka → BronzeWriter → Iceberg |
| **Validation** | `book_check_integrity()`, `validate_accepted_buffer()` | `check_candles_sorted_and_equidistant()` → `_reset_candles()` | `clean_ohlcv_dataframe()`, `trades_df_remove_duplicates()` | `QualityPipeline.run()` → gap scan + metrics |
| **Dedup** | `set[OrderBookEntry]`, `truncateOverlapEntries` | `deque` con overwrite en mismo timestamp, `drop_duplicates()` | `trades_df_remove_duplicates()`, `clean_ohlcv_dataframe()` | `unique(keep="last")` en `IcebergStorage.save_ohlcv()` |
| **Persistence boundary** | Adapter → DataEngine → Parquet (Rust) | REST → CandlesBase deque → SQLite (Python) | Exchange → CCXT → Disco (Python) | CCXT → Kafka → Iceberg |
| **Post-recovery validation** | ❌ No hay concepto | `check_candles_sorted_and_equidistant()` después de `fill_historical_candles()` | `_check_empty_df()` con gap detection | `QualityPipeline._scan_and_emit_gaps()` (siguiente iteración) |
| **Source of truth** | Exchange / Databento / Parquet catalog | Exchange (connector) | Exchange (REST) | Exchange (REST CCXT) |

### 6.2 Patrón REST + WS para OHLCV

| Proyecto | REST | WS | Relación |
|---|---|---|---|
| **OCM** | CCXT `fetch_ohlcv` separado | Cryptofeed `L2_BOOK` separado | **Sin conexión** — pipelines independientes |
| **Hummingbot** | `CandlesBase.candles_url` → `fetch_candles()` | `CandlesBase.wss_url` → `listen_for_subscriptions()` | **Unidos** en una sola clase con `deque` circular |
| **Freqtrade** | `Exchange.fetch_ohlcv()` | ❌ No hay WS | Solo REST |
| **Nautilus** | `RequestBars` | `SubscribeBars` | **Separados** pero unificados en `DataEngine` |

---

## 7. ORDERBOOK COMPARISON

### 7.1 Cadena completa OrderBook

| Paso | Nautilus | Hummingbot | Freqtrade | OCM |
|---|---|---|---|---|
| **Snapshot** | `RequestBookSnapshot` / `SubscribeBookSnapshots` | `get_new_order_book()` (REST inicial) | `fetch_l2_order_book()` (REST puntual) | ❌ Sin snapshot |
| **WS deltas** | `SubscribeBookDeltas` | `listen_for_order_book_diffs()` | ❌ No hay WS | Cryptofeed WS `L2_BOOK` |
| **Sequence validation** | `OrderBook.sequence`, `update_count` | **`snapshot_uid > update_id → REJECT`** | ❌ No hay | ❌ No hay |
| **Disconnect recovery** | `SocketReconnectLookup` | `NetworkIterator` + `_update_last_trade_prices_loop()` | ❌ No aplica | ❌ No hay |
| **Connected sequence-gap recovery** | ❌ No visible | **`restore_from_snapshot_and_diffs()`** | ❌ No aplica | ❌ No hay |
| **REST snapshot para resync** | `RequestBookSnapshot` | `get_new_order_book()` | `fetch_l2_order_book()` | ❌ No hay |
| **Buffering** | `Cache` con `order_book_mut()` | `_past_diffs_windows` (deque maxlen=32), `_saved_message_queues` (deque maxlen=1000) | ❌ No aplica | ❌ No hay |
| **Reconciliation** | `book_check_integrity()` | `restore_from_snapshot_and_diffs()` | ❌ No aplica | ❌ No hay |
| **Resume validation** | `update_count > 0`, `book_check_integrity()` | `_order_books_initialized.is_set()` (snapshot con bids/asks) | ❌ No aplica | ❌ No hay |
| **Persistence** | `ParquetDataCatalog` (Rust) | `MarketData` SQLAlchemy (SQLite) | ❌ No hay | ❌ Sin consumer |
| **Dedup** | `set[OrderBookEntry]`, `truncateOverlapEntries` | `snapshot_uid` reject stale | `trades_df_remove_duplicates()` | ❌ No hay |
| **Source of truth** | Exchange adapter | Exchange (connector) | Exchange (CCXT) | Exchange (Cryptofeed) |

---

## 8. RESPONSIBILITY MAPPING

### 8.1 OHLCV

| Responsabilidad | Nautilus | Hummingbot | Freqtrade | OCM |
|---|---|---|---|---|
| **Detect gap** | ❌ Ningún componente explícito | `CandlesBase.check_candles_sorted_and_equidistant()` | `clean_ohlcv_dataframe(fill_missing=True)` | `QualityPipeline._scan_and_emit_gaps()` |
| **Decide recovery** | ❌ Ningún componente | `fill_historical_candles()` (auto-triggered por WS) | ❌ Ningún componente | ❌ Ningún componente |
| **Fetch snapshot** | `RequestBookSnapshot` → DataClient | `get_new_order_book()` → REST | `fetch_l2_order_book()` → CCXT | ❌ No hay |
| **Fetch historical** | `RequestBars` → DataClient | `get_historical_candles()` → REST | `download_data_main()` → CCXT | `HistoricalFetcherAsync` → CCXT |
| **Reconcile** | ❌ No visible | `restore_from_snapshot_and_diffs()` | `clean_ohlcv_dataframe()` | ❌ No hay |
| **Persist** | `ParquetDataCatalog` → Parquet | `MarketData` SQLAlchemy → SQLite | `IDataHandler.ohlcv_store()` → Disco | `IcebergStorage.save_ohlcv()` → Iceberg |
| **Validate** | `book_check_integrity()` | `check_candles_sorted_and_equidistant()` | `clean_ohlcv_dataframe()` | `QualityPipeline.run()` |
| **Resume stream** | ❌ `SocketReconnectLookup` | `NetworkIterator` + WS reconnect | ❌ No hay | ❌ No hay |

### 8.2 ORDERBOOK

| Responsabilidad | Nautilus | Hummingbot | Freqtrade | OCM |
|---|---|---|---|---|
| **Detect gap** | ❌ No visible | `_order_book_diff_router()`: `snapshot_uid > update_id` | ❌ No hay | ❌ No hay |
| **Decide recovery** | ❌ No visible | `_track_single_book()`: snapshot + diffs → `restore_from_snapshot_and_diffs()` | ❌ No hay | ❌ No hay |
| **Fetch snapshot** | `RequestBookSnapshot` | `get_new_order_book()` | `fetch_l2_order_book()` | ❌ No hay |
| **Fetch historical** | `RequestBars` / `SubscribeBars` | `listen_for_order_book_diffs()` | ❌ No hay | Cryptofeed WS |
| **Reconcile** | `book_check_integrity()` | `restore_from_snapshot_and_diffs()` | ❌ No hay | ❌ No hay |
| **Persist** | `ParquetDataCatalog` | `MarketData` SQLAlchemy | ❌ No hay | ❌ Sin consumer |
| **Validate** | `book_check_integrity()`, `accepted_buffer_ns` | `_order_books_initialized.is_set()` | ❌ No hay | ❌ No hay |
| **Resume stream** | ❌ `SocketReconnectLookup` | `NetworkIterator` | ❌ No hay | ❌ No hay |

---

## 9. SOURCE OF TRUTH

### 9.1 OHLCV

| Proyecto | Historical | Live | Recovered | Evidencia |
|---|---|---|---|---|
| **Nautilus** | `ParquetDataCatalog` / Exchange adapter | `DataEngine` via `DataClient` | ❌ No concepto | `crates/persistence/src/python/catalog.rs`, `crates/data/src/engine/streaming.rs` |
| **Hummingbot** | REST → `_candles` deque → `MarketData` SQL | WS → `_candles` deque | REST fill via `fill_historical_candles()` | `candles_base.py:57`, `244-274` |
| **Freqtrade** | Disco local (feather/parquet) | ❌ No hay live | `download_data_main()` → disco | `data/history/datahandlers/`, `history_utils.py` |
| **OCM** | Iceberg `silver.ohlcv` | Kafka `ohlcv.raw` → Iceberg | ❌ Roto (storage=None) | `iceberg_storage.py`, `bronze_writer.py` |

### 9.2 OrderBook

| Proyecto | Snapshot | Incremental updates | Resync | Evidencia |
|---|---|---|---|---|
| **Nautilus** | `RequestBookSnapshot` / `BookSnapshotter` | `SubscribeBookDeltas` / `BookUpdater` | `OrderBook.reset()` | `crates/data/src/engine/book.rs` |
| **Hummingbot** | `get_new_order_book()` → REST | `listen_for_order_book_diffs()` → WS | `restore_from_snapshot_and_diffs()` | `order_book_tracker.py` |
| **Freqtrade** | `fetch_l2_order_book()` → REST | ❌ No hay WS | ❌ No hay | `exchange/exchange.py` |
| **OCM** | ❌ Sin snapshot | Cryptofeed WS `L2_BOOK` | ❌ Sin resync | `cryptofeed_orderbook_stream.py` |

### 9.3 ¿REST snapshot es baseline?

**Hummingbot**: SÍ. El `OrderBookTracker` inicia con `get_new_order_book()` (REST snapshot), luego aplica WS diffs. El `snapshot_uid` se establece desde el snapshot inicial. Si un diff tiene `update_id < snapshot_uid`, se rechaza como stale.

**Nautilus**: PARCIALMENTE. `RequestBookSnapshot` existe como comando explícito. `BookSnapshotter` genera snapshots periódicos. Pero no hay un "snapshot como baseline para diffs" demostrado a nivel visible.

**Freqtrade**: SÍ pero sin secuencia. `fetch_l2_order_book()` es REST puntual sin `update_id`. No hay concepto de baseline vs incremental.

---

## 10. SNAPSHOT ↔ WS RACE CONDITION

### 10.1 Escenario

```
t0  sequence gap detected (e.g., missing update_id 1005-1010)
t1  REST snapshot requested
t2  WS continues producing updates (update_id 1011, 1012, ...)
t3  snapshot received
t4  more WS updates received
```

### 10.2 Resolución por proyecto

**Hummingbot** — Evidencia completa:
- `_saved_message_queues[trading_pair]` (deque maxlen=1000) almacena mensajes recibidos antes del snapshot
- `_past_diffs_windows[trading_pair]` (deque maxlen=32) almacena los últimos 32 diffs
- `_track_single_book()`:
  ```python
  if len(saved_messages) > 0:
      message = saved_messages.popleft()
  else:
      message = await message_queue.get()
  if message.type is OrderBookMessageType.DIFF:
      order_book.apply_diffs(message.bids, message.asks, message.update_id)
      past_diffs_window.append(message)
  elif message.type is OrderBookMessageType.SNAPSHOT:
      past_diffs = list(past_diffs_window)
      order_book.restore_from_snapshot_and_diffs(message, past_diffs)
  ```
- `_order_book_diff_router()`: `if order_book.snapshot_uid > ob_message.update_id: messages_rejected`
- **Solución**: Mensajes pre-snapshot se almacenan y se procesan después. Diffs se almacenan en `past_diffs_windows` para replay. Snapshots con diffs almacenados → `restore_from_snapshot_and_diffs()`.

**Nautilus** — Evidencia parcial:
- `BookSnapshotter` genera snapshots periódicos
- `BookUpdater` procesa deltas
- `OrderBook.reset()` existe pero no se invita explícitamente ante race
- **Solución**: No demostrada a nivel visible

**Freqtrade** — Evidencia: UNKNOWN
- No hay WS → no hay race condition

**OCM** — Evidencia: UNKNOWN
- Sin snapshot, sin secuencia, sin resync

---

## 11. ¿QUÉ SIGNIFICA "RESUME"?

### 11.1 Diferenciación

| Concepto | Definición | Evidencia |
|---|---|---|
| **reconnected** | WS reconnect completado | Hummingbot: `NetworkIterator` detecta reconexión |
| **resynchronized** | Snapshot + diffs aplicados, book actualizado | Hummingbot: `restore_from_snapshot_and_diffs()` completa |
| **validated/resumable** | Book validado, `_order_books_initialized.is_set()` | Hummingbot: `_order_books_initialized.wait()` |

**Hummingbot demuestra la distinción completa:**
- `reconnected` → `NetworkIterator` → `listen_for_order_book_diffs()` re-establece conexión
- `resynchronized` → `restore_from_snapshot_and_diffs()` reconstruye book
- `validated/resumable` → `_order_books_initialized.is_set()` — snapshot con bids y asks confirmados

**OCM no distingue estos conceptos.** No hay snapshot, no hay validación de resync, no hay evento de readiness.

---

## 12. ¿QUÉ SIGNIFICA "RECOVERY"?

### 12.1 Definiciones

| Concepto | OHLCV | OrderBook | Evidencia |
|---|---|---|---|
| **reconnect** | No es recovery | Reconectar WS | Hummingbot: `NetworkIterator` |
| **backfill** | Descargar TODO desde now → origin | No aplica | OCM: `BackfillStrategy`, Freqtrade: `download_data_main()` |
| **repair** | Sanar gaps en Silver existente | No aplica | OCM: `RepairStrategy` |
| **snapshot** | Baseline de datos | Estado completo del book | Hummingbot: `get_new_order_book()` |
| **resync** | Re-descargar datos faltantes | Reconstruir book desde snapshot | Hummingbot: `restore_from_snapshot_and_diffs()` |
| **reconcile** | Verificar consistencia entre capas | Verificar consistencia book | Hummingbot: `restore_from_snapshot_and_diffs()` |

**No son sinónimos:**
- `reconnect` = conexión WebSocket restablecida. NO es recovery de datos.
- `backfill` = descarga completa desde ahora hacia atrás. NO es reparación selectiva.
- `repair` = sanar gaps específicos en datos existentes. Requiere leer Silver.
- `snapshot` = estado completo. Es baseline para diffs.
- `resync` = reconstruir desde snapshot + diffs almacenados. Es recovery.
- `reconcile` = verificar consistencia. Es validación post-recovery.

---

## 13. COMPARACIÓN CONTRA OCM

### 13.1 OCM OHLCV vs modelos de referencia

```
OCM OHLCV
REST (CCXT) → OHLCVPipeline → Incremental → Quality → gap detection → Backfill → Repair → Storage → Silver → Gold
```

| Capacidad | OCM | Mejor referencia | Aplicable? |
|---|---|---|---|
| REST historical OHLCV | ✅ CCXT paginado | Todos | ✅ Ya existe |
| Live OHLCV | ⚠️ Solo REST (WS hardcoded `mode="incremental"`) | Hummingbot (WS+REST unidos) | ⚠️ Parcial |
| Gap detection | ✅ `scan_gaps()` | Hummingbot (`check_candles_sorted_and_equidistant()`) | ✅ Ya existe |
| Gap recovery | ❌ Ninguna acción | Hummingbot (`fill_historical_candles()`) | ⚠️ Necesita plumbing |
| Backfill | ✅ CLI | Freqtrade (`download_data_main()`) | ✅ Ya existe |
| Repair | ✅ (ROTO) | Ninguno directamente | ⚠️ Necesita storage |
| Historical validation | ✅ `QualityPipeline` | Hummingbot (`check_candles_sorted_and_equidistant()`) | ✅ Ya existe |
| Persistence boundary | ✅ Kafka→Iceberg | Nautilus (Parquet) | ✅ Ya existe |
| Post-recovery validation | ✅ `QualityPipeline` | Hummingbot | ✅ Ya existe |
| Source of truth | Exchange (REST) | Todos | ✅ Ya existe |

### 13.2 OCM OrderBook vs modelos de referencia

```
OCM OrderBook
WS (Cryptofeed) → BookBuilder → book.* (no Bronze)
```

| Capacidad | OCM | Mejor referencia | Aplicable? |
|---|---|---|---|
| WS OrderBook | ✅ Cryptofeed `L2_BOOK` | Hummingbot WS | ✅ Ya existe |
| Snapshot | ❌ Sin snapshot | Hummingbot (`get_new_order_book()`) | ⚠️ Necesita implementar |
| Sequence validation | ❌ Sin sequence | Hummingbot (`snapshot_uid > update_id`) | ⚠️ Necesita implementar |
| Disconnect recovery | ❌ Sin reconnect | Hummingbot (`NetworkIterator`) | ⚠️ Necesita implementar |
| Connected sequence-gap recovery | ❌ Sin resync | Hummingbot (`restore_from_snapshot_and_diffs()`) | ⚠️ Necesita implementar |
| REST OrderBook snapshot | ❌ Sin REST | Hummingbot/Nautilus (`RequestBookSnapshot`) | ⚠️ Necesita implementar |
| Buffering | ❌ Sin buffer | Hummingbot (`_past_diffs_windows`, `_saved_message_queues`) | ⚠️ Necesita implementar |
| Reconciliation | ❌ Sin reconcile | Hummingbot (`restore_from_snapshot_and_diffs()`) | ⚠️ Necesita implementar |
| Resume validation | ❌ Sin ready state | Hummingbot (`_order_books_initialized.is_set()`) | ⚠️ Necesita implementar |
| Persistence | ❌ Sin consumer | Nautilus (`ParquetDataCatalog`) | ⚠️ Necesita implementar |
| Dedup | ❌ Sin dedup | Hummingbot (`snapshot_uid`) | ⚠️ Necesita implementar |
| Source of truth | Exchange (WS) | Hummingbot/Nautilus | ✅ Ya existe |

---

## 14. MATRIZ DE APLICABILIDAD

| Reference Pattern | Source | Problem solved | Evidence | Applicable OCM? | Why |
|---|---|---|---|---|---|
| **CandlesBase (REST+WS unidos)** | Hummingbot | OHLCV historical + live en una sola clase | `candles_base.py:24` | NOT APPLICABLE | OCM tiene REST y WS como dominios separados (OHLCV vs OrderBook) |
| **`deque` circular con overwrite** | Hummingbot | Buffer circular con dedup implícito | `candles_base.py:57` | OBSERVED | OCM usa `unique(keep="last")` en Iceberg (equivalente) |
| **`fill_historical_candles()`** | Hummingbot | Llenar gaps desde REST cuando WS inicia | `candles_base.py:411-435` | OBSERVED | OCM ya tiene `BackfillStrategy` pero no auto-triggered |
| **`check_candles_sorted_and_equidistant()`** | Hummingbot | Validar calidad de datos | `candles_base.py:276-293` | OBSERVED | OCM tiene `QualityPipeline` con `scan_gaps()` |
| **`_reset_candles()`** | Hummingbot | Reset en datos malformados | `candles_base.py:295-298` | APPLICABLE | OCM podría implementar reset en RepairStrategy |
| **`OrderBookTracker` con `snapshot_uid`** | Hummingbot | Validación de secuencia en OrderBook | `order_book_tracker.py:556-558` | APPLICABLE | OCM OrderBook necesita `update_id` tracking |
| **`_past_diffs_windows`** | Hummingbot | Almacenar diffs para replay | `order_book_tracker.py:249` | APPLICABLE | OCM OrderBook necesita buffer de diffs |
| **`_saved_message_queues`** | Hummingbot | Almacenar mensajes pre-snapshot | `order_book_tracker.py:254` | APPLICABLE | OCM OrderBook necesita cola de mensajes |
| **`restore_from_snapshot_and_diffs()`** | Hummingbot | Reconstruir book desde snapshot + diffs | `order_book_tracker.py:665` | APPLICABLE | OCM OrderBook necesita reconciliación |
| **`_order_book_diff_router()` con reject** | Hummingbot | Rechazar diffs stale | `order_book_tracker.py:556-558` | APPLICABLE | OCM OrderBook necesita validación de secuencia |
| **`_order_books_initialized` event** | Hummingbot | Estado de readiness | `order_book_tracker.py:284` | APPLICABLE | OCM OrderBook necesita estado de validación |
| **`BookSnapshotter`** | Nautilus | Snapshots periódicas del book | `crates/data/src/engine/book.rs:203-301` | OBSERVED | OCM no tiene snapshots de OrderBook |
| **`BookUpdater` con `apply_deltas()`** | Nautilus | Procesar deltas de OrderBook | `crates/data/src/engine/book.rs:93-118` | OBSERVED | OCM `BookBuilder` ya aplica updates |
| **`OrderBook.reset()`** | Nautilus | Reset completo del book | `crates/model/src/python/orderbook/book.rs:99-103` | OBSERVED | OCM podría implementar reset |
| **`RequestBookSnapshot` / `SubscribeBookDeltas`** | Nautilus | Mensajes explícitos para snapshot/deltas | `crates/data/src/engine/requests.rs:238` | OBSERVED | OCM no tiene estos mensajes |
| **`SocketReconnectLookup`** | Nautilus | Reconexión automática | `crates/data/src/engine/mod.rs:38` | OBSERVED | OCM no tiene reconexión |
| **`ParquetDataCatalog`** | Nautilus | Persistencia catalogada de datos | `crates/persistence/src/python/catalog.rs` | OBSERVED | OCM usa Iceberg (diferente persistencia) |
| **`download_data_main()`** | Freqtrade | Descarga histórica completa | `history_utils.py:702` | OBSERVED | OCM tiene `BackfillStrategy` |
| **`fill_missing=True`** | Freqtrade | Rellenar gaps con "No action" | `idatahandler.py:355-400` | APPLICABLE | OCM podría implementar fill en RepairStrategy |
| **`IDataHandler`** | Freqtrade | Interfaz de almacenamiento en disco | `idatahandler.py:32-146` | OBSERVED | OCM usa `OHLCVStorage` Protocol |
| **`clean_ohlcv_dataframe()`** | Freqtrade | Limpiar datos OHLCV | `data/converter.py` | OBSERVED | OCM tiene `QualityPipeline` |

---

## 15. PATRONES COMUNES

### 15.1 Patrones identificados

| Patrón | Descripción | Demostrado por | Clasificación |
|---|---|---|---|
| **Pattern A: REST como baseline** | REST snapshot o historical fetch como estado inicial | Hummingbot (snapshot), Nautilus (RequestBookSnapshot), Freqtrade (fetch_l2_order_book) | **COMMON** — todos los proyectos |
| **Pattern B: WS como flujo incremental** | WS deltas como actualizaciones incrementales | Hummingbot (WS diffs), Nautilus (SubscribeBookDeltas) | **COMMON** — excepto Freqtrade |
| **Pattern C: Sequence validation** | Validar `update_id` para detectar gaps | Hummingbot (`snapshot_uid > update_id → REJECT`) | **PROJECT-SPECIFIC** — solo Hummingbot a nivel visible |
| **Pattern D: Detect → Reset → Resync → Validate** | Patrón de recuperación de OrderBook | Hummingbot (`diff_router` → `restore_from_snapshot_and_diffs` → `_order_books_initialized`) | **PROJECT-SPECIFIC** — solo Hummingbot completo |
| **Pattern E: Historical backfill separado de live** | REST para historial, WS para tiempo real | Hummingbot (`get_historical_candles()` + `listen_for_subscriptions()`), Nautilus (`RequestBars` + `SubscribeBars`) | **COMMON** — Hummingbot y Nautilus |
| **Pattern F: Recovery orchestration separada de detection** | Quien detecta gaps no ejecuta recuperación | Hummingbot (`check_candles_sorted` no ejecuta recovery directamente) | **PROJECT-SPECIFIC** — Hummingbot mezcla detección + recuperación |
| **Pattern G: Persistence desacoplada de transport** | Persistencia independiente del mecanismo de transporte | Todos (Parquet, SQLite, Disco, Iceberg) | **COMMON** — todos los proyectos |

### 15.2 Clasificación detallada

**COMMON** (demostrado por ≥2 proyectos):
- REST como mecanismo de baseline/snapshot
- WS como flujo incremental
- Persistence desacoplada de transport
- Historical backfill separado de live streaming
- Dedup por overwrite/snapshot_uid

**PROJECT-SPECIFIC** (demostrado por 1 proyecto):
- Sequence validation con `update_id` (Hummingbot)
- `restore_from_snapshot_and_diffs()` (Hummingbot)
- `_past_diffs_windows` + `_saved_message_queues` (Hummingbot)
- `_order_books_initialized` event (Hummingbot)
- `ParquetDataCatalog` (Nautilus)
- `fill_missing=True` con "No action" candles (Freqtrade)
- `continuous_future` con `chain_discontinuity` check (Nautilus)

**OCM-RELEVANT** (aplicable a OCM):
- Hummingbot OrderBook diff router con reject de stale diffs
- Hummingbot `_past_diffs_windows` para replay
- Hummingbot `_saved_message_queues` para mensajes pre-snapshot
- Hummingbot `restore_from_snapshot_and_diffs()` para reconciliación
- Hummingbot `_order_books_initialized` para estado de readiness
- Hummingbot `fill_historical_candles()` para auto-trigger de recuperación
- Hummingbot `check_candles_sorted_and_equidistant()` para validación
- Nautilus `OrderBook.reset()` para reset completo
- Nautilus `SocketReconnectLookup` para reconexión
- Freqtrade `fill_missing=True` para llenar gaps

**OCM-NOT-RELEVANT** (no aplicable):
- Hummingbot `CandlesBase` REST+WS unido (OCM tiene dominios separados)
- Freqtrade `download_data_main()` (OCM usa Kafka→Iceberg, no disco)
- Nautilus `continuous_future` (OCM no tiene futuros continuos)
- Nautilus `ParquetDataCatalog` (OCM usa Iceberg)
- Freqtrade `IDataHandler` (OCM usa `OHLCVStorage` Protocol)

---

## 16. EVIDENCE CONFLICTS

### 16.1 Conflictos identificados

1. **Hummingbot `_reset_candles()` vs `fill_historical_candles()`**:
   - `_reset_candles()` limpia todo el `_candles` deque en datos malformados
   - `fill_historical_candles()` llena desde REST cuando WS inicia
   - **Conflicto**: `_reset_candles()` destruye datos, `fill_historical_candles()` reconstruye. No está claro si es la estrategia correcta o si debería preservar datos parciales.

2. **Nautilus `BookUpdater` vs `BookSnapshotter`**:
   - `BookUpdater` aplica deltas sin validación explícita de secuencia visible
   - `BookSnapshotter` genera snapshots periódicos
   - **Conflicto**: No está claro si los snapshots sobreescriben diffs o se sincronizan con ellos.

3. **Freqtrade `fetch_l2_order_book()` sin secuencia**:
   - CCXT `OrderBook` no tiene `update_id`
   - **Conflicto**: Sin secuencia, no hay forma de detectar gaps en OrderBook.

### 16.2 Resolución

Estos conflictos son inherentes a cada proyecto y reflejan trade-offs de diseño. Para OCM, no hay conflicto — simplemente no tiene implementada ninguna de estas capacidades.

---

## 17. PREGUNTAS ABIERTAS

### 17.1 Para OHLCV

1. **¿OCM necesita `CandlesBase`-style REST+WS unificado para OHLCV?**
   - Hummingbot demuestra que REST+WS pueden coexistir en una sola clase.
   - OCM mantiene REST y WS como dominios separados (OHLCV vs OrderBook).
   - **Pregunta**: ¿El diseño actual de OCM (REST para OHLCV, WS para OrderBook) es suficiente o necesita convergencia?

2. **¿`fill_historical_candles()` es aplicable a OCM?**
   - Hummingbot usa REST fill triggered por WS.
   - OCM ya tiene `BackfillStrategy` pero es manual (CLI).
   - **Pregunta**: ¿`RepairStrategy` debería auto-trigger como `fill_historical_candles()`?

3. **¿`fill_missing=True` (Freqtrade) es suficiente para OCM?**
   - Freqtrade rellena gaps con "No action" candles.
   - OCM tiene `QualityPipeline` que detecta gaps pero no los rellena.
   - **Pregunta**: ¿`RepairStrategy` debería rellenar gaps o solo descargar datos faltantes?

### 17.2 Para OrderBook

4. **¿OCM necesita `OrderBookTracker`-style?**
   - Hummingbot demuestra el patrón completo: snapshot → WS → sequence validation → gap → resync.
   - OCM tiene `BookBuilder` pero sin snapshot, sin sequence, sin resync.
   - **Pregunta**: ¿`BookBuilder` debería migrar a `OrderBookTracker`-style?

5. **¿Nautilus `BookSnapshotter` es aplicable?**
   - Nautilus genera snapshots periódicas del book.
   - OCM no tiene snapshots de OrderBook.
   - **Pregunta**: ¿OCM necesita snapshots periódicas de OrderBook?

6. **¿`_past_diffs_windows` + `_saved_message_queues` son necesarios?**
   - Hummingbot almacena diffs para replay y mensajes pre-snapshot.
   - OCM no tiene buffer de diffs.
   - **Pregunta**: ¿OCM OrderBook necesita buffering para recuperación?

### 17.3 Para arquitectura general

7. **¿`PipelineContext.storage` debe inyectarse?**
   - `RepairStrategy` tiene `assert ctx.storage is not None` que falla.
   - `IcebergStorageFactory` ya existe en `main.py::_lifespan`.
   - **Pregunta**: ¿El plumbing de `storage` es el primer paso antes de cualquier recovery?

8. **¿Recovery debe ser un task separado o parte de `_ingestion_loop`?**
   - Hummingbot mezcla detección + recuperación en `CandlesBase`.
   - Nautilus delega a `DataEngine`.
   - OCM tiene `_ingestion_loop` con `PipelineOrchestrator`.
   - **Pregunta**: ¿Recovery debe ser un task asyncio dentro de `_lifespan` o dentro de `_ingestion_loop`?

9. **¿OCM necesita `GapDetectedEvent`?**
   - Hummingbot usa `_order_books_initialized` event para readiness.
   - OCM tiene `GapEventPublisherPort` pero no se emite desde `QualityPipeline`.
   - **Pregunta**: ¿El event bridge es necesario o la recuperación puede ser polled?

---

## 18. CONCLUSIÓN

### 18.1 Patrones demostrados

1. **REST como baseline es UNIVERSAL**. Los tres proyectos usan REST para obtener datos históricos o snapshots iniciales. Hummingbot y Nautilus usan REST explícitamente como baseline para diffs.

2. **WS como flujo incremental es COMÚN** (excepto Freqtrade). Hummingbot y Nautilus usan WS para deltas en tiempo real. Freqtrade no tiene WS.

3. **Sequence validation es PROYECTO-ESPECÍFICO**. Solo Hummingbot demuestra explícitamente `snapshot_uid > update_id → REJECT`. Nautilus tiene `update_count` y `sequence` pero sin validación explícita visible. Freqtrade no tiene secuencia.

4. **Recovery con snapshot + diffs replay es PROYECTO-ESPECÍFICO**. Solo Hummingbot demuestra `restore_from_snapshot_and_diffs()` con `_past_diffs_windows` y `_saved_message_queues`.

5. **Persistence desacoplada es COMÚN**. Todos los proyectos tienen persistencia separada del transporte.

6. **Backfill separado de live es COMÚN**. Hummingbot y Nautilus separan REST historical de WS live.

### 18.2 Patrones comunes

| Patrón | Proyectos | Aplicable a OCM |
|---|---|---|
| REST como baseline | Hummingbot, Nautilus, Freqtrade | ✅ Sí |
| WS como incremental | Hummingbot, Nautilus | ⚠️ Parcial (OCM ya tiene WS para OrderBook) |
| Persistence desacoplada | Todos | ✅ Sí |
| Backfill separado de live | Hummingbot, Nautilus | ✅ Sí |
| Dedup | Todos | ✅ Sí |

### 18.3 Patrones específicos

| Patrón | Proyecto | Aplicable a OCM |
|---|---|---|
| `snapshot_uid` validation | Hummingbot | ⚠️ Necesita implementar para OrderBook |
| `restore_from_snapshot_and_diffs()` | Hummingbot | ⚠️ Necesita implementar para OrderBook |
| `_past_diffs_windows` | Hummingbot | ⚠️ Necesita implementar para OrderBook |
| `_saved_message_queues` | Hummingbot | ⚠️ Necesita implementar para OrderBook |
| `_order_books_initialized` | Hummingbot | ⚠️ Necesita implementar para OrderBook |
| `_reset_candles()` | Hummingbot | ⚠️ Parcial |
| `fill_historical_candles()` | Hummingbot | ⚠️ Parcial |
| `fill_missing=True` | Freqtrade | ⚠️ Parcial |
| `ParquetDataCatalog` | Nautilus | ❌ No (OCM usa Iceberg) |
| `continuous_future` | Nautilus | ❌ No |
| `SocketReconnectLookup` | Nautilus | ⚠️ Parcial |

### 18.4 Preguntas arquitectónicas abiertas

1. ¿OCM necesita converger REST+WS para OHLCV o mantenerlos separados?
2. ¿`RepairStrategy` debe auto-trigger o ser manual?
3. ¿`PipelineContext.storage` debe inyectarse antes de cualquier recovery?
4. ¿OCM OrderBook necesita snapshot + sequence validation + resync?
5. ¿Recovery debe ser polled o event-driven?
6. ¿`GapDetectedEvent` debe disparar reparación o la reparación debe ser periódica?
7. ¿`_reset_candles()`-style reset es aplicable a OCM RepairStrategy?
8. ¿`fill_missing` (Freqtrade) es suficiente o se necesita recuperación selectiva (repair)?

### 18.5 Detenerse

Este documento es la fuente de evidencia previa para la siguiente fase de decisión arquitectónica de OCM. La investigación está completa. No se propone implementación.

---

## 19. VERIFICACIÓN FINAL

- **Git status**: Sin cambios en tracked files. Solo archivos sin seguimiento (audit docs existentes).
- **Archivos creados**: `docs/audits/AUDIT_OCM_reference-models-recovery-resync-comparative-study_2026-09-21.md` (este documento).
- **Archivos modificados**: Ninguno.
- **Commit**: NO REALIZADO.
- **Push**: NO REALIZADO.
- **Merge**: NO REALIZADO.
- **Code changes**: NINGUNO.
- **Config changes**: NINGUNA.
- **Systemd changes**: NINGUNA.
- **Test changes**: NINGUNA.
- **ADR changes**: NINGUNO.
- **Plan changes**: NINGUNO.

**Detenerse y esperar autorización humana explícita.**

---

## RESUMEN EJECUTIVO

**¿Qué patrones están demostrados?**

1. **REST como baseline** — Universal. Todos los proyectos usan REST para datos históricos o snapshots iniciales.
2. **WS como incremental** — Común (excepto Freqtrade). WS para deltas en tiempo real.
3. **Persistence desacoplada** — Común. Todos tienen persistencia separada del transporte.
4. **Backfill separado de live** — Común. Hummingbot y Nautilus separan REST historical de WS live.
5. **Sequence validation** — Específico. Solo Hummingbot a nivel visible con `snapshot_uid > update_id`.
6. **Recovery con snapshot + diffs replay** — Específico. Solo Hummingbot completo con `restore_from_snapshot_and_diffs()`.

**¿Qué es común?**
REST baseline, WS incremental, persistence desacoplada, backfill separado de live, dedup, source of truth en exchange.

**¿Qué es específico?**
Hummingbot: `snapshot_uid`, `_past_diffs_windows`, `_saved_message_queues`, `restore_from_snapshot_and_diffs()`, `_order_books_initialized`. Nautilus: `ParquetDataCatalog`, `continuous_future`, `SocketReconnectLookup`. Freqtrade: `fill_missing=True`, `IDataHandler`.

**¿Qué es aplicable a OCM?**
- Hummingbot OrderBook diff router con reject de stale diffs
- Hummingbot `_past_diffs_windows` para replay de diffs
- Hummingbot `_saved_message_queues` para mensajes pre-snapshot
- Hummingbot `restore_from_snapshot_and_diffs()` para reconciliación
- Hummingbot `_order_books_initialized` para estado de readiness
- Hummingbot `fill_historical_candles()` para auto-trigger de recuperación
- Hummingbot `check_candles_sorted_and_equidistant()` para validación
- Nautilus `OrderBook.reset()` para reset completo
- Nautilus `SocketReconnectLookup` para reconexión
- Freqtrade `fill_missing=True` para llenar gaps

**¿Qué requiere más evidencia?**
- Si Nautilus `BookUpdater` tiene validación de secuencia a nivel Rust no visible en Python
- Si Hummingbot `_reset_candles()` es la estrategia correcta vs preservar datos parciales
- Si Freqtrade `fill_missing=True` es suficiente para production OCM

**¿Qué preguntas quedan abiertas?**
7 preguntas arquitectónicas detalladas en sección 17.

**Detenerse y esperar autorización humana.**
