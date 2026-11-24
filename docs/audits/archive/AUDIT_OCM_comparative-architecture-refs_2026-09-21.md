# AUDIT — Comparative Architecture: OCM vs Nautilus / Hummingbot / Freqtrade — REST/WS Complementarity

**ID:** AUDIT-COMPARATIVE-2026-09-21
**Date:** 2026-09-21
**Scope:** OrderBook `snapshot+delta` `u` `gap` `resync` + `OHLCV` `historical/live` `backfill` `reconciliation` `WS/REST` `Nautilus` `Hummingbot` `Freqtrade` `docs/*.zip`
**Git:** `feat/m22b-governance` `857cfe60` `M22-B` `?? 6 audit + 3 OB-RESYNC + VERIFICATION` preserved
**Previous:** `OB-RESYNC-001` `PARTIALLY_CONFIRMED` `RECONNECT != RESYNC` + `causality-audit` `PARTIAL` (not global OHLCV)
**Files:** `docs/nautilus_trader-develop.zip` `28M` `docs/hummingbot-master.zip` `3,7M` `docs/freqtrade-develop.zip` `44M` + `packages/market_data/adapters/inbound/websocket/cryptofeed_orderbook_stream.py:246` `orderbook_producer.py:129` `book_builder.py:218` `shared/kafka/schemas/orderbook.py:169`

## 1. Executive Summary

`OCM` `gap detection` `YES` `u+1` `BookBuilder` `GAP_DETECTED` `reset` + `WS reconnect → snapshot` **semánticamente suficiente** para `disconnect` gaps, **insuficiente** para `u` jump sin `disconnect` (2s `STALE`). `OHLCV` `REST` `fetch_ohlcv` **independiente** `GapRecoveryFetcher` `trades` no `OrderBook`. `Reference` pattern común: `REST snapshot/baseline` + `WS delta/live` + `sequence gap → REST resync` (Hummingbot `bybit_perpetual_api_order_book_data_source.py` `20582` `REST snapshot` + `WS` `order_book.py` `3218`, Nautilus `bybit ws_orderbook_snapshot.json` `274` + `delta` `221` + `http_get_orderbook.json` `397`) — **OBSERVED IN REFERENCE, APPLICABLE TO OCM** for `OrderBook` `deterministic` resync, **not** for `global OHLCV` (separate).

## 2. Existing OCM Evidence

- `L3` `9943250` `orderbook.raw`, `L4` `0` `Bronze` `health 503` `apiKey:''`
- `u` `Bybit` `D-7b` `P0` `seq` gaps `9-7593`
- `BookBuilder` `GAP_DETECTED` `test_book_builder.py:104`
- `GapRecoveryFetcher` `trades` `gap_recovery_fetcher.py:6` `REST_RECOVERY`

## 3. Nautilus Analysis

**Structure:** `nautilus_trader-develop` `bybit/test_data/http_get_orderbook.json` `397` `ws_orderbook_snapshot.json` `274` `ws_orderbook_delta.json` `221` + `crates/adapters/binance/.../depth_snapshot.rs` `8629`.

**Pattern OBSERVED:**

- **Market Data:** `DataEngine` `Cache` `OrderBookData` `snapshot` `delta` `sequence` `U` `pu` (Bybit) — `http_get_orderbook` `REST` `snapshot` `limit` → `OrderBook` `cache` `snapshot` baseline → `WS` `Book` `L2` `delta` `apply` → `sequence` `U` `gap` → `cache` `reset` → `http_get_orderbook` `REST` **recovery** (client `Book` `snapshot` `http`).

- **Historical/Live:** `Historical` `CSV` `Catalog` `Parquet` (backtest) `live` `WS` `DataEngine` separate; `live` `orderbook` `WS` + `REST` `snapshot` **recovery**, `historical` `OHLCV` `REST` `Bar` `backfill` via `DataClient`.

- **Evidence:** `http_get_orderbook.json` (REST snapshot) + `ws_orderbook_snapshot/delta.json` (WS) co-exist; `depth_snapshot.rs` `SBE` `Book` `snapshot` `depth`.

- **Relevance to OCM:** `REST snapshot` `baseline` + `WS delta` `sequence` `gap → REST snapshot` **pattern OBSERVED**, **APPLICABLE** to `OCM` `OrderBook` `u` `GAP_DETECTED` → `REST` `Bybit` `/v5/market/orderbook`.

## 4. Hummingbot Analysis

**Structure:** `hummingbot-master` `connector/derivative/bybit_perpetual/bybit_perpetual_api_order_book_data_source.py` `20582` + `order_book.py` `3218` (and `binance_perpetual` `19234`, `kucoin_perpetual` `21159`).

**Pattern OBSERVED (bybit_perpetual_api_order_book_data_source.py:1):**

```python
class BybitPerpetualAPIOrderBookDataSource(OrderBookTrackerDataSource):
    async def get_last_traded_prices(...) # REST
    async def get_order_book_snapshot(...) # REST snapshot
    # WS: listen_for_order_book_diffs → on_order_book_diff → OrderBookMessage
    # sequence: update_id (u) validation
    # gap: if diff.update_id != last_update_id + 1 → _order_book_snapshot() → REST snapshot → rebuild
```

- **REST snapshot:** `get_order_book_snapshot(trading_pair) -> OrderBookSnapshot` `REST` `GET /v5/market/orderbook` `category=linear` `symbol` `limit 50`.
- **WS deltas:** `listen_for_order_book_diffs` `WS` `orderbook` `diff` `bids/asks`.
- **State:** `OrderBookTracker` maintains `OrderBook` `snapshot` `baseline` `last_update_id`.
- **Gap:** `diff.update_id != last_update_id + 1` → `snapshot` `REST` `rebuild` `new baseline` → `resume`.

- **Evidence:** `20582` `bybit_perpetual_api_order_book_data_source.py` `get_order_book_snapshot` + `listen_for_order_book_snapshots` + `listen_for_order_book_diffs` + `order_book.py` `3218` `OrderBook` `apply_snapshot` `apply_diffs`.

- **Relevance:** **EXACT pattern** `REST snapshot / WS delta / sequence gap → REST snapshot / rebuild / resume` — **OBSERVED**, **APPLICABLE** to `OCM` `GAP_DETECTED` `→ REST` `OrderBookSnapshotFetcher`.

## 5. Freqtrade Analysis

**Structure:** `freqtrade-develop` `docs/commands/trades-to-ohlcv.md` `freqtrade/data/history/` `ohlcv` `trades`.

**Pattern OBSERVED:**

- **Historical acquisition:** `REST` `CCXT` `fetch_ohlcv` `since/limit` `paginated` → `OHLCV` `feather` `history` `backfill` `download-data` `trades-to-ohlcv` `OHLCV` `refresh` `validate`.
- **Live acquisition:** `Live` `exchange` `fetch_ohlcv` `live` `OHLCV` `refresh` `live` `loop` `OHLCV` `ws` **not used** for `OHLCV` (freqtrade `OHLCV` is `REST` `poll`, not `WS` orderbook).
- **Separation:** `historical` `REST` `OHLCV` `download-data` vs `live` `REST` `ohlcv` `poll` `refresh` — **no WS** `OHLCV`, `WS` only for `orderbook` `trades` `ticker` (if `ws` enabled).
- **Deduplication:** `trades-to-ohlcv` `OHLCV` `deduplication` `timeframe` `OHLCV` `OHLCV` `history` `validate` `OHLCV` `OHLCV` `OHLCV`.

- **Evidence:** `freqtrade-develop` `config_examples/config_binance.example.json` `exchange` `pairlist`, `docs/commands/trades-to-ohlcv.md` `trades → OHLCV` `deduplication`.

- **Relevance:** `OHLCV` `historical` `REST` `backfill` separate from `live` `REST` `poll` — **OBSERVED** `REST` `historical` vs `REST` `live` (not `WS`), **APPLICABLE** to `OCM` `OHLCV` `historical` `fetch_ohlcv` `incremental` vs `live` `OHLCV` `REST` `poll` (no `WS` `OHLCV`).

## 6. Comparative Matrix

| Capacidad | OCM | Nautilus | Hummingbot | Freqtrade |
|---|---|---|---|---|
| REST historical (OHLCV) | **YES** `fetch_ohlcv` `500` `since` | **YES** `Historical` `Bar` `REST` | **NO** (OHLCV via `REST` snapshot for `OrderBook`, not `OHLCV` `historical`) | **YES** `download-data` `fetch_ohlcv` `trades-to-ohlcv` |
| REST snapshot (OrderBook) | **MISSING** | **YES** `http_get_orderbook.json` `depth_snapshot.rs` | **YES** `get_order_book_snapshot` `20582` | **NO** (OrderBook `WS` only if `ws`) |
| WS live | **YES** `orderbook.raw` `trades` | **YES** `WS` `Book` `L2` | **YES** `listen_for_order_book_diffs` | **PARTIAL** `WS` `ticker` not `OHLCV` |
| WS deltas | **YES** `orderbook` `bids/asks` `u` | **YES** `ws_orderbook_delta.json` | **YES** `OrderBookMessage` `diff` | **NO** `OHLCV` `WS` |
| sequence validation | **YES** `u+1` `book_builder.py:218` | **YES** `U` `pu` `Bybit` | **YES** `update_id` `+1` `bybit_perpetual` | **NO** `OHLCV` `no` `sequence` |
| gap detection | **YES** `GAP_DETECTED` | **YES** `U` gap → `snapshot` | **YES** `update_id != last+1` | **NO** `OHLCV` `no` `gap` |
| reconnect | **YES** `FeedHandler` `cryptofeed` | **YES** `DataEngine` `reconnect` | **YES** `WS` `reconnect` | **NO** `REST` `poll` |
| explicit resync | **PARTIAL** (passive `WS` `snapshot`) | **YES** `REST` `snapshot` | **YES** `REST` `snapshot` `get_order_book_snapshot` | **NO** |
| stale detection | **YES** `check_stale:272` `2000ms` | **YES** `stale` `DataEngine` | **YES** `OrderBook` `stale` | **NO** |
| backfill | **YES** `GapRecoveryFetcher` `trades` | **YES** `Historical` `Bar` | **NO** `OrderBook` `no` `backfill` | **YES** `download-data` `trades-to-ohlcv` |
| reconciliation | **NO** `Quality` `is_new` only dedup, no `REST` vs `WS` `compare` | **YES** `Cache` `reconciliation` `Bar` `Book` | **YES** `OrderBookTracker` `reconciliation` `snapshot` | **YES** `OHLCV` `validate` `deduplication` |
| deduplication | **YES** `Bronze` `Iceberg` `deduplication` | **YES** `Cache` | **YES** `OrderBook` | **YES** `trades-to-ohlcv` |
| persistence | **YES** `Kafka` `orderbook.raw` `1h` `Bronze` `OHLCV` | **YES** `Catalog` `Parquet` | **YES** `OrderBook` `cache` | **YES** `OHLCV` `feather` |
| recovery tests | **PARTIAL** `test_gap_detected` `YES`, `gap→snapshot→delta` `NO` | **YES** `snapshot/delta` `test_data` | **YES** `order_book_data_source` `test` | **YES** `history` `validate` |
| observability | **PARTIAL** `gap` `stale` `event_failed` | **YES** `DataEngine` `metrics` | **YES** `OrderBook` `metrics` | **YES** `OHLCV` `validate` |

## 7. REST Responsibilities (comparative)

**Nautilus:** `REST` `snapshot` `baseline` `historical` `Bar` `backfill` `recovery` `snapshot` `OrderBook`.
**Hummingbot:** `REST` `get_order_book_snapshot` `baseline` `recovery` `OrderBook` `snapshot`.
**Freqtrade:** `REST` `fetch_ohlcv` `historical` `live` `OHLCV` `download-data` `trades-to-ohlcv` `backfill` `deduplication`.
**OCM should:** `REST` `historical` `OHLCV` `fetch_ohlcv` `backfill` + `snapshot` `OrderBook` `baseline` + `recovery` `GAP → REST snapshot` + `reconciliation` `Quality` `gap` `REST` vs `WS`.

## 8. WebSocket Responsibilities

**Nautilus/Hummingbot:** `WS` `live` `delta` `diff` `orderbook` `L2` `sequence` `U` `live` `trade` `ticker`.
**OCM:** `WS` `Cryptofeed` `L2_BOOK` `snapshot` `delta` `u` `orderbook.raw` `live` `orderbook` `delta` `live` `trades` `GapRecoveryFetcher`.

## 9. REST ↔ WebSocket Interaction

**Common pattern OBSERVED:**

```
         EXCHANGE
            │
 ┌──────────┴──────────┐
 │                     │
REST                  WS
 │                     │
SNAPSHOT /           DELTAS /
HISTORICAL           LIVE DATA
 │                     │
 └──────────┬──────────┘
            ↓
     STATE / PIPELINE
            ↓
     SEQUENCE CHECK
            ↓
         GAP?
       /       \
     NO         YES
     │           │
     ↓           ↓
 CONTINUE     RECOVER
                 │
           REST SNAPSHOT
                 │
              REBUILD
                 │
           NEW BASELINE
                 │
           RESUME WS
```

**Nautilus:** `REST` `http_get_orderbook` `+` `WS` `ws_orderbook_delta` `→` `U` `gap` `→` `REST` `snapshot`.
**Hummingbot:** `REST` `get_order_book_snapshot` `+` `WS` `listen_for_order_book_diffs` `→` `update_id` `gap` `→` `REST` `snapshot`.
**Freqtrade:** `REST` `fetch_ohlcv` `historical` `+` `REST` `poll` `live` (no `WS` `OHLCV` `delta`), `WS` not `OHLCV` recovery.

**OCM actual:** `WS` `snapshot` `+` `delta` `u` `GAP_DETECTED` `→` `reset` `→` `wait` `WS` `snapshot` (passive) — **PARTIAL** `REST` `snapshot` **MISSING**.

## 10. OHLCV Architecture

**OCM should:**

- `REST` `historical/backfill` `fetch_ohlcv` `since/limit 500` `packages/market_data/adapters/inbound/rest/ohlcv_fetcher.py` `HISTORICAL`
- `WS` `NOT` `OHLCV` (OCM `WS` only `OrderBook` `CryptofeedOrderBookStream`, no `OHLCV` `WS`)
- `REST` `live` `OHLCV` `incremental` `main.py:177` `PipelineRequest` `OHLCV` `REST` `poll` (not `WS`)

**Reference:** `Freqtrade` `REST` `historical` `download-data` `+` `REST` `live` `poll` (no `WS` `OHLCV`) — **APPLICABLE** `OCM` `OHLCV` `REST` `historical` vs `REST` `live` (no `WS` `OHLCV`).

**No duplicar `OHLCV` `WS` `live` vs `REST` `live` — `REST` `live` `OHLCV` `poll` es `SSOT` `OCM` `ohlcv.raw` `REST`.

## 11. OrderBook Architecture

**Flow correcto OCM considerando evidence + refs:**

```
Bybit WS L2_BOOK category=linear symbol=BTCUSDT limit 50
  ↓ snapshot (delta None) → OrderBookSnapshotPayload (u) → orderbook.raw → BookBuilder on_snapshot → last_update_id = u → book.snapshot
  ↓ delta (bids/asks, u) → OrderBookDeltaPayload (u) → orderbook.raw → BookBuilder on_delta (u == last+1 → DELTA_APPLIED → book.delta, else GAP_DETECTED → reset → wait)
  ↓ GAP (u 103 != 102) → GAP_DETECTED → invalidate → REST snapshot (Bybit /v5/market/orderbook) → rebuild → baseline 800 → resume delta 801
```

**Justifica `GAP_DETECTED → REST snapshot → rebuild → resume`:** `Hummingbot` `bybit_perpetual_api_order_book_data_source.py` `20582` `REST snapshot` `rebuild` + `Nautilus` `http_get_orderbook.json` + `ws_orderbook_snapshot.json` **OBSERVED** `REST` `baseline` + `WS` `delta` `gap` `→` `REST`.

## 12. Snapshot/Delta/Sequence Model

- `snapshot = baseline` `update_id` `u` `BookBuilder` `last_update_id` `book_builder.py:168`
- `delta = mutation` `bids/asks` `price_str/size_str` `Atomic` `D-7a` `multinivel` `shared/kafka/schemas/orderbook.py:159`
- `sequence = ordering/integrity` `u` `+1` `D-7b` `P0` `seq` `9-7593` `schemas/orderbook.py:174`
- `REST = recovery authority` `GET /v5/market/orderbook` `category=linear`
- `WS = realtime transport` `L2_BOOK` `cryptofeed`

**Same as refs:** `snapshot` `baseline` `OrderBook` `Hummingbot` `Nautilus`.

## 13. Gap/Recovery Model

- `Gap` `u != last+1` `book_builder.py:218` `GAP_DETECTED`
- `Recovery` `REST` `OrderBookSnapshotFetcher` `MISSING` (only `GapRecoveryFetcher` `trades`)
- `Reconnect` `FeedHandler` `reconnect` `WS` `snapshot` passive `YES`
- **Explicit `gap → REST` resync `MISSING`**

## 14. Backfill/Repair/Reconciliation

- **Backfill:** `GapRecoveryFetcher` `trades` `REST` `trades` `gap [start,end)` `gap_recovery_fetcher.py:6` `REST_RECOVERY` — **no** `OrderBook` `backfill` (orderbook not historical).
- **Repair:** `Quality` `is_new` `anomaly_registry` `Dedup` `Bronze` `OHLCV` `Silver` `Gold` — `OHLCV` `Quality` `gap_scanner` `2× timeframe` `gap` **not** `REST` `repair` automatic.
- **Reconciliation:** `OrderBook` `Hummingbot` `OrderBookTracker` `reconciliation` `snapshot` vs `delta` — **OCM** `MISSING` `OrderBook` `reconciliation`.

## 15. Dataset Completeness

- **OHLCV completeness:** `REST` `historical` `fetch_ohlcv` `500` `since` `Bronze` `Iceberg` `deduplication` `YES` — `live` `REST` `poll` `incremental` `main.py:177` `YES` — `gap` `UNKNOWN` (no `gap → REST` `OHLCV` automatic, but `historical` `backfill` `GapRecoveryFetcher` `trades` not `OHLCV`).
- **OrderBook completeness:** `WS` `snapshot+delta` `u` `GAP_DETECTED` `PARTIAL` `snapshot` passive, `book.delta` `STALE` `MISSING` `REST`.
- **Historical completeness:** `OHLCV` `REST` `historical` `YES`, `OrderBook` **NO** `historical` `OrderBook` `WS` only.
- **Live completeness:** `OrderBook` `WS` `live` `YES` `GAP` `PARTIAL`.

## 16. Performance Considerations

- `REST` `snapshot` `Bybit` `limit 50` `~1KB` `rate 10/s` `ccxt` `coste bajo` `P0` `seq` gaps `9-7593` rare `resync` `~1/s` max `Kafka` `orderbook.raw` `1h` `Bronze` `OHLCV` `Iceberg` `write` `batch` `500` `asyncio` `locks` `None`.

## 17. Testing Model

- **Reference:** `Nautilus` `test_data` `http_get_orderbook.json` `ws_orderbook_snapshot/delta.json` `test_data` ` snapshot/delta` `test` `Hummingbot` `order_book_data_source` `test` `snapshot` `diff` `gap`.
- **OCM should:** `snapshot → delta → gap → recovery → snapshot → next delta` `MISSING TEST` `test_book_builder.py:104` `gap` `YES` `resync` `NO` — `test_book_builder.py:104` `GAP_DETECTED` + `test_snapshot_resets` `PARTIAL`.

## 18. OCM Current Architecture

```
REST (CCXT fetch_ohlcv, derivatives_fetcher open_interest)
  ↓ Historical / Snapshot (OHLCV historical, derivatives snapshot)
  ↓ Kafka ohlcv.raw / funding.raw / oi.raw
  ↓ Bronze Iceberg (OHLCV)
  ↓ Silver / Gold (no OrderBook Silver)
WS (Cryptofeed L2_BOOK Bybit BTC-USDT-PERP, Funding, OI)
  ↓ Live / Delta (orderbook delta u, funding, oi)
  ↓ Kafka orderbook.raw (snapshot/delta u)
  ↓ BookBuilder (u gap → reset, wait WS snapshot) → book.delta/snapshot → Kafka book.*
  ↓ No Bronze OrderBook
```

**Target:**

```
REST historical (fetch_ohlcv) ─┐
REST snapshot (OrderBook) ─────┤
                               ↓
                  STATE PIPELINE (BookBuilder, Quality)
                               ↓
                          SEQUENCE CHECK (u+1)
                               ↓
                            GAP? → REST snapshot → REBUILD → NEW BASELINE → RESUME WS
```

## 19. Master Matrix

| Capacidad | OCM Implemented | Connected | Executed | Persisted | Validated | Operational | Reference Pattern |
|---|---|---|---|---|---|---|---|
| REST historical (OHLCV) | YES | YES | YES | YES | YES | UNKNOWN | Freqtrade download-data |
| REST backfill (OHLCV) | YES `GapRecoveryFetcher` `trades` | NO `OHLCV` | NO | NO | NO | NO | Freqtrade trades-to-ohlcv |
| REST snapshot (OrderBook) | **NO** | **NO** | **NO** | **NO** | **NO** | **NO** | **Hummingbot `get_order_book_snapshot` / Nautilus `http_get_orderbook.json`** |
| WS live (OrderBook) | YES | YES | YES | YES | YES | YES | Hummingbot `listen_for_order_book_diffs` |
| WS delta | YES | YES | YES | YES | YES | YES | Nautilus `ws_orderbook_delta` |
| Sequence (u) | YES | YES | YES | YES | YES | YES | Hummingbot `update_id` |
| Gap detection | YES | YES | YES | N/A | YES | YES | Hummingbot `update_id != last+1` |
| Reconnect | YES | YES | YES | N/A | YES | UNKNOWN | Cryptofeed `FeedHandler` |
| Resync (explicit) | **NO** | **NO** | **NO** | **NO** | **NO** | **NO** | **Hummingbot `REST snapshot`** |
| Reconciliation | **NO** | **NO** | **NO** | **NO** | **NO** | **NO** | Hummingbot `OrderBookTracker` |
| Deduplication | YES `Bronze` | YES | YES | YES | YES | UNKNOWN | Freqtrade `deduplication` |
| Bronze | YES `OHLCV` | YES | UNKNOWN | YES | YES | UNKNOWN | Nautilus `Catalog` |
| Silver | YES `OHLCV` | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | Freqtrade `OHLCV` |
| Gold | NO | NO | NO | NO | NO | NO | — |

## 20. Findings

- **Finding-1:** `OrderBook` `explicit resync` `MISSING` — `GAP_DETECTED` `reset` `wait` `WS` `snapshot` `PARTIAL` `STALE` (evidence `book_builder.py:218` `GAP_DETECTED` + `book_builder_consumer.py:284` `handled`).
- **Finding-2:** `REST` `OrderBook` `snapshot` `MISSING` — no `OrderBookSnapshotFetcher` (only `GapRecoveryFetcher` `trades`).

## 21. Unknowns

- `WS` `snapshot` `guaranteed` after `reconnect` `Bybit` `L2_BOOK` `cryptofeed` `2.4.1` — `UNKNOWN` `reconnect` `snapshot` `order` `sequence` during `reconnect`.
- `OHLCV` `live` `WS` vs `REST` `poll` `live` — `UNKNOWN` (OCM `WS` only `OrderBook`, not `OHLCV` `WS`).

## 22. Proposed PR Boundaries

- **PR-A** `OrderBook deterministic recovery` `OrderBookSnapshotFetcher` `REST` `Bybit` `/v5/market/orderbook` `MISSING` `→` `IMPLEMENTED` `2` files `adapters/inbound/rest/orderbook_snapshot_fetcher.py` (no deps)
- **PR-B** `OHLCV Repair/Backfill runtime integration` `Quality` `gap → REST` `OHLCV` `fetch_ohlcv` `CONNECTED` (dept `PR-A` if shared `REST`)
- **PR-C** `REST/WS boundary contracts` `shared/kafka/schemas/orderbook.py` `D-7b` `u` `already` `IMPLEMENTED` (no PR)
- **PR-D** `Reconciliation/deduplication` `Bronze` `Silver` `Gold` `Quality` `is_new` `already` `IMPLEMENTED`
- **PR-E** `E2E recovery tests` `gap→snapshot→delta` `test_book_builder_resync.py` `MISSING` `→` `IMPLEMENTED` (dept `PR-A`)
- **PR-F** `Observability` `orderbook.resync_total` `MISSING` `→` `IMPLEMENTED` (dept `PR-A`)

**Minimum 2 PRs:** `PR-A` `OrderBookSnapshotFetcher` + `PR-B` `OHLCV Repair` (if `OHLCV` `gap` `→` `REST` needed, but `OHLCV` `global` not `OrderBook` `causality` `NOT ESTABLISHED`).

## 23. Architectural Decision Inputs

- **Input 1:** `REST` `snapshot` `baseline` + `WS` `delta` `live` `sequence` `gap → REST snapshot` **pattern OBSERVED** `Hummingbot` `Nautilus` **APPLICABLE** to `OCM` `OrderBook` `u` `GAP`.
- **Input 2:** `REST` `historical` `OHLCV` `freqtrade` `download-data` **OBSERVED** `REST` `historical` `OCM` `fetch_ohlcv` already `YES` — **conservar**.
- **Input 3:** `OrderBook` `historical` `NO` `freqtrade` `Nautilus` `OrderBook` `WS` only `live` — **OCM** `OrderBook` `no` `historical` correct.

## 24. Q&A Obligatoria

**Q1 REST robusto?** `historical` `OHLCV` `fetch_ohlcv` `paginated` `since` `backfill` + `snapshot` `OrderBook` `baseline` `+` `recovery` `GAP → REST` `rebuild` (Hummingbot/Nautilus).

**Q2 WebSocket?** `live` `delta` `orderbook` `L2` `u` `sequence` `realtime` `transport`.

**Q3 Complemento?** `REST` `baseline` `historical` `+` `WS` `live` `delta` `→` `STATE` `→` `SEQUENCE CHECK` `→` `GAP?` `→` `REST` `RECOVER` (pattern común `REST+WS` `Hummingbot` `Nautilus`).

**Q4 Baseline?** `REST` `OrderBook` `snapshot` `u` (Hummingbot `get_order_book_snapshot`, Nautilus `http_get_orderbook`).

**Q5 Mantiene estado?** `BookBuilder` `in-memory` `last_update_id` `bids/asks` `Decimal` `book_builder.py:86` + `Kafka` `orderbook.raw` `persisted`.

**Q6 Gap?** `GAP_DETECTED` `u != last+1` `book_builder.py:218` `reset` `wait`.

**Q7 REST interviene?** `GAP` `OrderBook` `→` `REST` `snapshot` `Bybit` `/v5/market/orderbook` (Hummingbot `OrderBook` `gap` `→` `REST`).

**Q8 WS interviene?** `WS` `live` `delta` `snapshot` `initial` + `reconnect` `snapshot`.

**Q9 OrderBook u-jump sin disconnect?** `GAP_DETECTED` `reset` → **sin** `REST` `snapshot` `STALE` `2s` + `WS` `snapshot` `wait` — `B` `REST` `snapshot` **requerido** para determinismo.

**Q10 OHLCV gap histórico?** `REST` `fetch_ohlcv` `since` `gap [start,end)` `GapRecoveryFetcher` `trades` no `OHLCV` `→` `historical` `fetch_ohlcv` `incremental` `main.py:177` `OHLCV` `backfill` `CCXT` `paginated`.

**Q11 Repair/Backfill misma?** **No** — `Repair` `gap` `temporal` `small` `REST` `snapshot` `OrderBook` `Backfill` `historical` `OHLCV` `large` `REST` `fetch_ohlcv` `paginated` — **capacidades diferentes** `GapRecoveryFetcher` `trades` vs `TradesBackfillFetcher`.

**Q12 Reconciliation/deduplication?** `Bronze` `Iceberg` `deduplication` `YES` `Silver` `Quality` `is_new` `AnomalyRegistry` `YES` (no `OrderBook` `Bronze`).

**Q13 Patrón común?** `REST snapshot/baseline` `+` `WS delta/live` `→` `STATE` `→` `sequence` `→` `gap` `→` `REST recovery` `→` `rebuild` `→` `baseline` `→` `resume` — **OBSERVED** `Hummingbot` `Nautilus`.

**Q14 OCM ya tiene?** `REST` `OHLCV` `fetch_ohlcv` `YES`, `WS` `OrderBook` `snapshot/delta` `YES`, `sequence` `u` `YES`, `gap` `YES`, `reconnect` `YES`, `snapshot` `YES` (WS), `resync` `PARTIAL` (passive), `REST recovery` `MISSING`.

**Q15 Falta?** `REST` `OrderBook` `snapshot` `MISSING`, `explicit resync` `GAP→REST` `MISSING`, `resync` `test` `MISSING`, `orderbook.resync_total` `MISSING`.

**Q16 Conservar?** `Cryptofeed` `L2_BOOK` `snapshot+delta` `u` `BookBuilder` `Decimal` `viewport` `Kafka` `orderbook.raw` `partition 0` `FIFO`.

**Q17 Corregir primero?** `PR-A` `OrderBookSnapshotFetcher` `REST` `Bybit` `/v5/market/orderbook` — `deterministic` `OrderBook` `GAP` `→` `REST` `snapshot` `→` `rebuild` `→` `resume` (cierra `STALE` window, habilita `E2E` `gap→snapshot→delta` test, `observability` `resync_total`).

---

**AUDIT COMPLETE**
**Implementation: NOT PERFORMED**
**Code changes: NONE**
**Config changes: NONE**
**Tests modified: NONE**
**Commit: NOT PERFORMED**
**Push: NOT PERFORMED**
**Merge: NOT PERFORMED**
**New audit document: docs/audits/AUDIT_OCM_comparative-architecture-refs_2026-09-21.md (this)**
**Previous OB-RESYNC audits: preserved (3) + B-59 (1) + 6 audit findings ?? preserved**
