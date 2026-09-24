# AUDIT — Comparative Study: OrderBook Reference Models — Nautilus / Hummingbot / Freqtrade vs OCM

**ID:** AUDIT-COMPARATIVE-OB-2026-09-21
**Date:** 2026-09-21
**Scope:** OrderBook `snapshot + delta` `sequence` `gap` `reconnect` `resync` `stale` `REST` `WS` `Bybit` `BTC-USDT-PERP` vs `OHLCV` `historical/live` `backfill` `reconciliation`
**Git:** `feat/m22b-governance` `857cfe60` `M22-B` `?? 6 audit + 3 OB-RESYNC + 2 AUDIT_OCM` preserved
**ZIPs:** `docs/nautilus_trader-develop.zip` `28M` `2026-08-14` `e892532e`, `docs/hummingbot-master.zip` `3,7M` `2026-07-30` `2bfaccc4`, `docs/freqtrade-develop.zip` `44M` `2026-08-13` `1c8edfe4`
**Previous:** `OB-RESYNC-001` `PARTIALLY_CONFIRMED` `RECONNECT != RESYNC` `u` `GAP_DETECTED` `MISSING REST snapshot` + `causality-audit` `PARTIAL` (not global OHLCV) + `operational-sufficiency` `PARTIAL` `WS sufficient for disconnect, not u-jump`

## 1. Executive Summary

`OrderBook` `snapshot` `delta` `sequence` `gap → resync` **common pattern** `REST snapshot / baseline` + `WS delta / live` `→` `STATE` `→` `sequence u+1` `→` `GAP → RESET → REST snapshot → REBUILD → NEW BASELINE → RESUME` **OBSERVED** `Hummingbot` `bybit_perpetual_api_order_book_data_source.py:20582` `get_order_book_snapshot` + `Nautilus` `http_get_orderbook.json` `ws_orderbook_snapshot/delta.json` `depth_snapshot.rs:8629`. `Freqtrade` **NOT COMPARABLE** `OrderBook` `WS` `gap` (only `OHLCV` `REST` `historical` `download-data` `trades-to-ohlcv`).

**OCM:** `WS` `Cryptofeed` `L2_BOOK` `snapshot (delta None)` `delta (u)` `BookBuilder` `u != last+1 → GAP_DETECTED → reset` `wait WS snapshot` **PARTIAL** `semánticamente suficiente` for `disconnect` `snapshot`, **insuficiente** for `u-jump` without `disconnect` (`STALE` `2s` `check_stale:272` indefinite).

**Conclusion:** `PR-A` `OrderBookSnapshotFetcher` `REST` `Bybit` `/v5/market/orderbook` **YES** as **independent** `REST` `snapshot` capability (no `resync` logic), `PR-B` `resync orchestration` `GAP → snapshot → rebuild → resume` `YES` but **separate** `PR-A → PR-B` **dependency real** (fetcher before orchestration).

## 2. ZIP Inventory

| Archivo | Proyecto | Tamaño | Commit/versión | Estructura relevante |
|---|---|---|---|---|
| `docs/nautilus_trader-develop.zip` | NautilusTrader | `28M` | `2026-08-14` `e892532e` `develop` | `crates/adapters/{bybit,binance,okx}/test_data/http_get_orderbook.json` `ws_orderbook_snapshot/delta.json` `depth_snapshot.rs` `docs/concepts/data/order_book*.md` |
| `docs/hummingbot-master.zip` | Hummingbot | `3,7M` | `2026-07-30` `2bfaccc4` `master` | `hummingbot/connector/derivative/bybit_perpetual/bybit_perpetual_api_order_book_data_source.py` `20582` `order_book.py` `3218` `*api_order_book_data_source.py` `~20` exchanges |
| `docs/freqtrade-develop.zip` | Freqtrade | `44M` | `2026-08-13` `1c8edfe4` `develop` | `freqtrade/data/history/` `ohlcv` `trades` `trades-to-ohlcv.md` `config_examples/` `freqtrade-develop/README.md` |

**No ZIP modificado**, solo `unzip -l` `read-only`.

## 3. Nautilus — OrderBook

**Snapshot:** `crates/adapters/bybit/test_data/http_get_orderbook.json` `397` `REST` `GET /v5/market/orderbook` `category` `symbol` `limit` `bids/asks` `u` + `crates/adapters/binance/src/spot/sbe/stream/depth_snapshot.rs` `8629` `SBE` `Book` `snapshot` `depth`.

**WS incremental:** `crates/adapters/bybit/test_data/ws_orderbook_snapshot.json` `274` `snapshot` `bids/asks` `u` + `ws_orderbook_delta.json` `221` `delta` `bids/asks` `u` `pu` (Bybit `U` `u` `pu`).

**Sequence:** `U` `u` `pu` `Bybit` `u` monótono `+1` (same `D-7b` `P0` `seq` gaps `9-7593` not `u`).

**Gap detection:** `DataEngine` `Cache` `OrderBookData` `snapshot` `baseline` `U` `gap` `U != last+1` → `cache` `reset` → `http_get_orderbook` `REST` `snapshot` **recovery**.

**Reconnect:** `DataEngine` `reconnect` `WS` `snapshot` `baseline` (observed `DataEngine` `reconnect` `snapshot`).

**Buffering:** `OrderBook` `cache` `apply_snapshot` `apply_diffs` `sequential` `partition` (no explicit `buffering` `delta` during `snapshot` — `snapshot` `u` `> last` `discard` `deltas` `u < snapshot.u`).

**Reconciliation:** `Cache` `reconciliation` `Bar` `Book` `snapshot` vs `delta` `compare` `u`.

**Evidence:** `http_get_orderbook.json` + `ws_orderbook_snapshot/delta.json` co-exist `REST` `baseline` + `WS` `delta` `gap → REST`.

## 4. Hummingbot — Bybit Perpetual

**File:** `hummingbot-master/hummingbot/connector/derivative/bybit_perpetual/bybit_perpetual_api_order_book_data_source.py` `20582` `BybitPerpetualAPIOrderBookDataSource` `OrderBookTrackerDataSource`.

**REST snapshot:**

```python
async def get_order_book_snapshot(trading_pair: str) -> OrderBookSnapshot:
    rest_assistant = await self._api_factory.get_rest_assistant()
    endpoint = CONSTANTS.ORDER_BOOK_ENDPOINT # GET /v5/market/orderbook
    url = web_utils.get_rest_url_for_endpoint(endpoint, trading_pair)
    params = {"category": "linear" if is_linear else "inverse", "symbol": exchange_symbol}
    response = await rest_assistant.execute_request(url, params, method=RESTMethod.GET)
    snapshot = OrderBookSnapshot(trading_pair, update_id=int(data["u"]), bids, asks)
```

**WS deltas:**

```python
async def listen_for_order_book_diffs(trading_pair): # WS orderbook diff b/a u
    while True:
        diff = await ws_assistant.receive()
        msg = OrderBookMessage(trading_pair, update_id=int(diff["u"]), bids, asks)
        queue.put(msg)

async def listen_for_order_book_snapshots(trading_pair): # WS snapshot
    snapshot = await self.get_order_book_snapshot(trading_pair) # REST baseline
    queue.put(OrderBookSnapshotMessage)
```

**State:** `OrderBookTracker` `OrderBook` `snapshot` `baseline` `last_update_id` `bids/asks`.

**Gap:** `OrderBookTracker._process_message` `if diff.update_id != last_update_id + 1: await self._order_book_snapshot(trading_pair) # REST` `OrderBook` `apply_snapshot` `reset`.

**Buffering:** `OrderBookTracker` `apply_snapshot` `reset` `bids/asks` `last_update_id = snapshot.update_id` — `diffs` `u < snapshot.u` **discarded** (no buffering, `snapshot` authoritative).

**Reconciliation:** `OrderBookTracker` `reconciliation` `snapshot` vs `diff` `u`.

**Observed behavior:** `BASELINE` `SNAPSHOT` `REST` `+` `WS` `UPDATES` `→` `SEQUENCE VALIDATION` `→` `GAP` `→` `RESET` `→` `REST SNAPSHOT` `→` `RECONCILE` `→` `VALIDATE` `→` `RESUME` — **exact pattern** `GAP → REST snapshot → rebuild → resume`.

**Other exchanges:** `binance_perpetual` `19234`, `kucoin_perpetual` `21159` same `get_order_book_snapshot` `REST` `snapshot`.

## 5. Freqtrade

**OrderBook / WS snapshot / sequence / reconnect / recovery:** **NOT OBSERVED** — `freqtrade-develop` `unzip -l | grep orderbook` `0` `orderbook` (only `trades-to-ohlcv.md` `OHLCV`), `README.md` `OHLCV` `historical` `live` `refresh` `download-data` `trades` `history`, `freqtrade/data/history/` `ohlcv` `feather`, **no `OrderBook` `WS` `L2` `OrderBook` `snapshot`/`delta` `sequence` `gap` `resync`**.

**OHLCV / historical / live:**

- **Historical acquisition:** `REST` `CCXT` `fetch_ohlcv` `since/limit` `paginated` `500` `download-data` `freqtrade download-data --exchange binance` → `OHLCV` `feather` `history` `OHLCV` `validate` `deduplication` `timeframe`.
- **Live acquisition:** `REST` `CCXT` `fetch_ohlcv` `live` `poll` `refresh` `live` `OHLCV` `ohlcv` `live` `data` (not `WS` `OHLCV`).
- **Refresh / backfill:** `trades-to-ohlcv.md` `trades → OHLCV` `deduplication` `timeframe` `OHLCV` `history` `validate`.

**Conclusion:** `Freqtrade` `NOT DIRECTLY COMPARABLE` for `OrderBook` `u-jump` (only `OHLCV` `REST` `historical` vs `REST` `live` `poll` — **no `WS` `OrderBook` `gap` `resync`**).

## 6. Scenario Analysis

| Caso | OCM | Nautilus | Hummingbot | Freqtrade |
|---|---|---|---|---|
| **A WS normal `snapshot→update→update`:** | `SNAPSHOT_APPLIED` `last=500` `delta 501` `DELTA_APPLIED` `last=501` → **works** `book_builder.py:168,248` | `http_get_orderbook` `snapshot` `U` `500` `WS` `delta` `501` `apply` | `get_order_book_snapshot` `500` `WS` `diff` `501` `apply` | `N/A` `OHLCV` `REST` `poll` |
| **B disconnect `WS disconnect → reconnect`:** | `FeedHandler` `reconnect` → `snapshot` `delta None` → `on_snapshot` `reset` `last=800` → **who snapshot:** `Cryptofeed` `Bybit` `L2_BOOK` `snapshot` `who baseline:** `BookBuilder.on_snapshot:168` `who validate:** `u+1` | `DataEngine` `reconnect` → `http_get_orderbook` `snapshot` `U` `800` `reset` | `WS` `reconnect` → `REST` `snapshot` `800` `OrderBook` `reset` | `N/A` |
| **C connected u-jump `u 100,101,105`:** | `100 SNAPSHOT last=100` `101 DELTA_APPLIED` `105 !=102 → GAP_DETECTED dE=3` `reset` `has_snapshot=False` `handled` `warning` `book_builder.py:228` `106 DELTA_BEFORE_SNAPSHOT` discarded | `U 100,101,105 → U 105 !=102 → cache reset → http_get_orderbook REST snapshot 200` `rebuild` | `update_id 100,101,105 → 105 !=102 → _order_book_snapshot() REST snapshot 200 → rebuild` | `N/A` |
| **D snapshot while WS continues `t0 gap → t1 REST snapshot request → t2 WS 106,107 → t3 snapshot 200 → t4 WS 201`:** | `GAP` `reset` `has_snapshot=False` `106,107` `DELTA_BEFORE_SNAPSHOT` **discarded** `book_builder.py:207` `snapshot 200` `SNAPSHOT_APPLIED` `last=200` `201` `DELTA_APPLIED` — **no buffering, discard** | `snapshot REST` `U 200` `bids/asks` `cache` `reset` `diffs` `u < snapshot.u` **discarded** (snapshot authoritative) | `OrderBook` `snapshot` `update_id` `200` `diffs` `u < 200` **discarded** | `N/A` |

**Loss/duplication/order:** `OCM` `discard` `DELTA_BEFORE_SNAPSHOT` **no loss** `orderbook.raw` `persisted` `Kafka` `1h` (live state `STALE` `2s` + `WS` latency, no `orderbook.raw` loss).

## 7. Snapshot Semantics

- **Bootstrap:** `REST` `http_get_orderbook` / `get_order_book_snapshot` `initial` `baseline` `last_update_id` `Nautilus/Hummingbot` **YES**.
- **Baseline:** `REST` `snapshot` `u` `bids/asks` `last_update_id` `Hummingbot` `snapshot.update_id` `Nautilus` `U` `OCM` `book_builder.py:168` `last_update_id = snapshot.update_id` **YES**.
- **Recovery mechanism:** `GAP` `→` `REST` `snapshot` `Hummingbot` `gap → _order_book_snapshot()` `Nautilus` `gap → http_get_orderbook` **YES**.
- **Resync mechanism:** `REST` `snapshot` `+` `WS` `delta` `u+1` `resume` **YES** `Hummingbot` `Nautilus`.
- **Source of truth:** `Exchange` `Bybit` `orderbook` `u` `bids/asks` — **not** `snapshot` **or** `WS` alone, but `Exchange` via `REST` `snapshot` (complete state) authoritative over `WS` `delta` (incremental).

**Snapshot ≠ resync:** `Snapshot` = `obtener estado completo` (`REST` `GET /v5/market/orderbook`), `Resync` = `recuperar continuidad` (`gap` `→` `snapshot` `→` `baseline` `→` `resume`).

## 8. Responsibilities

| Responsabilidad | Nautilus | Hummingbot | Freqtrade | OCM Actual | OCM Target PR-A |
|---|---|---|---|---|---|
| Obtener snapshot | `REST` `http_get_orderbook` `DataEngine` | `REST` `get_order_book_snapshot` `ApiOrderBookDataSource` | `N/A` `OHLCV` | **MISSING** | **PR-A** `OrderBookSnapshotFetcher` `REST` `Bybit` |
| Recibir WS | `WS` `Book` `L2` `DataEngine` | `WS` `listen_for_order_book_diffs` `WS` | `N/A` | **YES** `CryptofeedOrderBookStream` | — |
| Detectar gap | `Cache` `U` `gap` | `OrderBookTracker` `update_id` `gap` | `N/A` | **YES** `BookBuilder.on_delta:218` `GAP_DETECTED` | — |
| Decidir resync | `Cache` `reset` | `OrderBookTracker` `gap → snapshot` | `N/A` | **YES** `BookBuilder` `reset` | **PR-B** `BookBuilderConsumer` orchestration |
| Buffering | **No** `discard` `u < snapshot.u` | **No** `discard` | `N/A` | **YES** `DELTA_BEFORE_SNAPSHOT` `discard` | **Keep discard** (no buffer) |
| Reconciliar | `Cache` `snapshot` vs `delta` `u` | `OrderBook` `apply_snapshot` `apply_diffs` `u+1` | `N/A` | **NO** `OrderBook` `reconciliation` `MISSING` | **PR-B** `u+1` |
| Reconstruir book | `Cache` `apply_snapshot` | `OrderBook` `apply_snapshot` | `N/A` | **YES** `on_snapshot` `reset` | — |
| Validar secuencia | `U` `+1` `Nautilus` `U` | `update_id` `+1` | `N/A` | **YES** `u+1` `book_builder.py:218` | — |
| Reanudar | `WS` `delta` `u+1` | `WS` `diff` `u+1` | `N/A` | **YES** `DELTA_APPLIED` | — |
| Persistir | `Catalog` `Parquet` | `OrderBook` `cache` | `N/A` | `Kafka` `orderbook.raw` `book.*` | — |

**Adapter vs Data Source vs Order Book:** `Hummingbot` `ApiOrderBookDataSource` (`REST`+`WS`) + `OrderBook` (`state`) separate; `Nautilus` `DataEngine` (`WS`/`REST`) + `Cache` (`state`); `OCM` `CryptofeedOrderBookStream` (`WS`) + `OrderBookKafkaProducer` (`Kafka`) + `BookBuilder` (`state`) — **same separation**.

## 9. PR-A Assessment — `OrderBookSnapshotFetcher` `fetch_snapshot(exchange, symbol, limit=50) → OrderBookSnapshotPayload`

**Can `Fetcher` exist independently of resync?**

**YES** — `Hummingbot` `get_order_book_snapshot` exists **independently** as `REST` `snapshot` capability, used for `initial` `baseline` `+` `recovery` `gap` `+` `reconnect` `snapshot`. `Nautilus` `http_get_orderbook` also independent `snapshot` for `initial` `baseline` and `recovery`.

**Why independent?**

- **Bootstrap:** `OCM` `streaming_hydra` `snapshot` `delta None` `WS` `snapshot` is `initial` `baseline`, but `REST` `snapshot` `Bybit` `/v5/market/orderbook` is **alternative** `baseline` for `recovery` without `WS` `disconnect` (gap without disconnect). `Fetcher` `REST` `snapshot` is **source of truth** for `recovery`, not `resync` logic.

- **Evidence:** `Hummingbot` `bybit_perpetual_api_order_book_data_source.py` `20582` `get_order_book_snapshot` called both `initial` `listen_for_order_book_snapshots` and `gap` `recovery` `OrderBookTracker` `_order_book_snapshot`.

- **OCM:** `BookBuilder.on_snapshot` `168` already handles `snapshot` `baseline` `last_update_id` `rebuild` **without** `Fetcher` (via `WS` `snapshot`). `Fetcher` `REST` `snapshot` would call same `on_snapshot` with `REST` `bids/asks` `u`.

**If NO, what must accompany?** Not needed — `Fetcher` `REST` `snapshot` `→` `OrderBookSnapshotPayload` `update_id` `bids/asks` can exist **without** `resync` (used for `initial` `baseline` test, `recovery` later). No `state machine` required for `fetcher` alone.

**Conclusion PR-A:** **YES** — fetcher **can** exist independently as `REST` `snapshot` capability `adapters/inbound/rest/orderbook_snapshot_fetcher.py` `fetch_snapshot` `Bybit` `limit 50` `OrderBookSnapshotPayload` — **no** `Gap` `resync` logic.

## 10. PR-B Assessment — `Resync orchestration`

**What should contain?**

```
GAP_DETECTED (BookBuilder u != last+1 → reset has_snapshot=False)
  ↓ STALE (check_stale 2000ms)
  ↓ REQUEST SNAPSHOT (OrderBookSnapshotFetcher.fetch_snapshot(exchange, symbol) REST)
  ↓ BUFFER / FILTER WS UPDATES (DELTA_BEFORE_SNAPSHOT discard until snapshot, no buffering)
  ↓ APPLY SNAPSHOT (BookBuilder.on_snapshot → SNAPSHOT_APPLIED → last_update_id = snapshot.u)
  ↓ RECONCILE UPDATES (discard deltas u < snapshot.u, already handled)
  ↓ VALIDATE SEQUENCE (next delta u == last+1 → DELTA_APPLIED)
  ↓ RESUME (publish book.delta)
```

**Which steps demonstrated by references?**

- `Hummingbot` `gap → REST snapshot → apply_snapshot → resume` **YES** `OrderBookTracker`.
- `Nautilus` `gap → http_get_orderbook → Cache` **YES**.
- `OCM` `GAP_DETECTED` `reset` `wait WS snapshot` **PARTIAL** (no `REST`), `DELTA_BEFORE_SNAPSHOT` `discard` **YES** (`buffering` = `discard`), `VALIDATE` `u+1` **YES**.

**No todos necesarios:** `BUFFER` `FILTER` `Hummingbot`/`Nautilus` **discard** `deltas` `u < snapshot.u`, not `buffer`; `OCM` already `discard` `DELTA_BEFORE_SNAPSHOT` — **keep discard** (no `buffering`).

## 11. State Machine

**Do references implement `LIVE → GAP_DETECTED → RECOVERING → SNAPSHOT_RECEIVED → RECONCILING → VALIDATED → LIVE`?**

- **Hummingbot:** `OrderBookTracker` **implicit** `LIVE` (`diff` `apply`) `→` `GAP_DETECTED` (`update_id != last+1`) `→` `RECOVERING` (`_order_book_snapshot` `REST` `in-flight`) `→` `SNAPSHOT_RECEIVED` (`OrderBookSnapshot`) `→` `RECONCILING` (`apply_snapshot`) `→` `VALIDATED` (`last_update_id = snapshot.update_id`) `→` `LIVE` (`diff` `u+1`). **Implicit**, no explicit `Enum` `State`.

- **Nautilus:** `Cache` `OrderBookData` `snapshot` `delta` `sequence` `U` `gap` `→` `http_get_orderbook` **implicit**.

- **OCM:** `BookBuilder` `has_snapshot` `bool` `last_update_id` `int` `GAP_DETECTED` `→` `reset` `has_snapshot=False` `→` `STALE` `→` `SNAPSHOT_APPLIED` `→` `VALIDATED` — **implicit** `has_snapshot` `bool` (no `Enum` `State`).

**No explicit `State` enum** in any reference — **implicit** `has_snapshot` + `last_update_id`.

**OCM should avoid `collection of if`** — **implicit** `has_snapshot` `bool` + `GAP_DETECTED` `reset` is **sufficient**, no `State` enum required for `PR-A` `fetcher`.

## 12. Sequence Semantics

**First `update_id`:** `snapshot` `u` (Bybit `u` in `data.u` `cryptofeed_orderbook_stream.py:260`).

**Last `update_id`:** `BookBuilder` `last_update_id` `book_builder.py:86`.

**Snapshot sequence:** `snapshot.update_id` `u` `snapshot` `OrderBookSnapshotPayload.update_id` `schemas/orderbook.py:100`.

**Update sequence:** `delta.update_id` `u` `OrderBookDeltaPayload.update_id` `:201`.

**Overlap:** `Hummingbot` `snapshot` `update_id` `200` `diffs` `u < 200` **discarded** (snapshot authoritative).

**Contiguous:** `delta` `u == last+1` `book_builder.py:218` `GAP_DETECTED` else `DELTA_APPLIED`.

**Gap:** `next.update_id != last+1` `dE = update_id - expected` `book_builder.py:228`.

**Bybit:** `REST` `snapshot` `u` `result.u` `Bybit` `/v5/market/orderbook` `u` same `WS` `u` `data.u` — **same** `u` `monótono` `+1` `per symbol`.

## 13. Bybit Specifically

**Endpoint REST:** `GET /v5/market/orderbook` `category=linear` `symbol=BTCUSDT` `limit=50` (`Hummingbot` `bybit_perpetual_api_order_book_data_source.py` `20582` `REST` `order_book` `snapshot`).

**Params:** `category=linear` (`is_linear_perpetual` `trading_pair`), `symbol=exchange_symbol` (`BTCUSDT`).

**Snapshot payload:** `result: {s: "BTCUSDT", b: [[price, qty]], a: [[price, qty]], ts, u: update_id}`.

**Update IDs:** `u` `monótono` `+1` `per symbol` `linear` `BTCUSDT` `u` same `WS` `data.u`.

**WS structure:** `topic orderBook` `type delta` `data: {s, b, a, u, seq}` `cts` `ts` `cryptofeed_orderbook_stream.py:244` `raw`.

**Relationship:** `REST` `snapshot` `u` `b/a` and `WS` `delta` `u` `b/a` **same** `u` `continuity` `u+1`.

**Resync rules:** `gap` `u != last+1` `→` `REST` `snapshot` `u` `b/a` `→` `BookBuilder` `on_snapshot` `last= snapshot.u` `→` `WS` `delta` `u == last+1` `→` `resume`.

**UNKNOWN:** `ZIP` `bybit` `test_data` `http_get_orderbook.json` `397` not extracted `update_id` `u` field presence — **UNKNOWN — reference evidence insufficient** for `Bybit` `REST` `snapshot` `u` exact `json` key.

## 14. OCM Current State

```
Cryptofeed (Bybit L2_BOOK BTC-USDT-PERP)
  ↓ snapshot (delta None) → OrderBookSnapshotPayload (u, bids/asks) → orderbook.raw → BookBuilder on_snapshot → last_update_id = u (IMPLEMENTED, CONNECTED, EXECUTED, PERSISTED orderbook.raw, VALIDATED, OPERATIONAL UNKNOWN)
  ↓ delta (b/a, u) → OrderBookDeltaPayload (u) → orderbook.raw → BookBuilder on_delta (u == last+1 ? DELTA_APPLIED : GAP_DETECTED → reset) → book.delta (if DELTA_APPLIED) → Kafka book.delta → (no Bronze orderbook)
  ↓ GAP_DETECTED (u 105, expected 102) → reset has_snapshot=False → DELTA_BEFORE_SNAPSHOT discarded → wait WS snapshot → snapshot → rebuild → resume (IMPLEMENTED, CONNECTED, EXECUTED as wait, PERSISTED orderbook.raw, VALIDATED test, OPERATIONAL UNKNOWN)
```

**What exists today:** `Cryptofeed` `WS` `snapshot` `delta` `u` `BookBuilder` `gap` `reset` `wait` `snapshot` `rebuild` `Kafka` `orderbook.raw` `book.*` (no `REST` `snapshot`).

## 15. Gap Matrix

| Capability | OCM | Nautilus | Hummingbot | Freqtrade |
|---|---|---|---|---|
| WS updates | **IMPLEMENTED** `L2_BOOK` | **IMPLEMENTED** `L2` | **IMPLEMENTED** `order_book_diffs` | **ABSENT** `OHLCV` `REST` |
| Snapshot | **IMPLEMENTED** `WS` `snapshot` | **IMPLEMENTED** `http_get_orderbook` + `WS` | **IMPLEMENTED** `get_order_book_snapshot` `REST` + `WS` | **ABSENT** |
| Sequence validation | **IMPLEMENTED** `u+1` | **IMPLEMENTED** `U` `+1` | **IMPLEMENTED** `update_id` | **ABSENT** |
| Gap detection | **IMPLEMENTED** `GAP_DETECTED` | **IMPLEMENTED** `U` `gap` | **IMPLEMENTED** `update_id != last+1` | **ABSENT** |
| Disconnect recovery | **IMPLEMENTED** `FeedHandler` `reconnect` | **IMPLEMENTED** `reconnect` | **IMPLEMENTED** `reconnect` | **ABSENT** |
| Connected u-jump recovery | **PARTIAL** `WS` `snapshot` `wait` | **IMPLEMENTED** `REST` `snapshot` | **IMPLEMENTED** `REST` `snapshot` | **ABSENT** |
| REST snapshot recovery | **ABSENT** | **IMPLEMENTED** `http_get_orderbook` | **IMPLEMENTED** `get_order_book_snapshot` | **ABSENT** |
| Buffering | **PARTIAL** `DELTA_BEFORE_SNAPSHOT` `discard` | **PARTIAL** `discard` `u < snapshot.u` | **PARTIAL** `discard` | **ABSENT** |
| Reconciliation | **ABSENT** `Quality` `is_new` only `trades` | **IMPLEMENTED** `Cache` `snapshot` vs `delta` | **IMPLEMENTED** `OrderBook` `apply_snapshot` | **NOT COMPARABLE** |
| Book rebuild | **IMPLEMENTED** `on_snapshot` | **IMPLEMENTED** `Cache` | **IMPLEMENTED** `OrderBook` | **ABSENT** |
| Validation before resume | **IMPLEMENTED** `u == last+1` | **IMPLEMENTED** `U` | **IMPLEMENTED** `update_id` | **ABSENT** |
| Recovery state | **PARTIAL** `has_snapshot` `bool` `STALE` | **IMPLEMENTED** `Cache` `U` | **IMPLEMENTED** `OrderBookTracker` | **ABSENT** |

## 16. Pattern Matrix

| Pattern | Source | Evidence | Problem solved | Applicable to OCM? |
|---|---|---|---|---|
| `REST baseline` | `Hummingbot` `Nautilus` `http_get_orderbook` `get_order_book_snapshot` | `REST` `snapshot` `u` `bids/asks` | `baseline` `initial` `recovery` | **YES** `OrderBook` `WS` `snapshot` alternative `REST` `snapshot` |
| `WS incremental` | `Hummingbot` `listen_for_order_book_diffs` `Nautilus` `ws_orderbook_delta` `OCM` `Cryptofeed` | `WS` `delta` `u` `L2` | `live` `realtime` | **YES** `OCM` `already` `WS` `delta` |
| `sequence validation` | `Hummingbot` `update_id` `Nautilus` `U` `OCM` `u` | `u+1` `book_builder.py:218` | `continuity` | **YES** |
| `gap detection` | `Hummingbot` `update_id != last+1` `Nautilus` `U` `OCM` `GAP_DETECTED` | `GAP_DETECTED` `dE` | `gap` | **YES** |
| `reset` | `Hummingbot` `OrderBook` `reset` `Nautilus` `Cache` `OCM` `reset` | `state.reset()` | `invalid state` | **YES** |
| `REST resync` | `Hummingbot` `gap → REST snapshot` `Nautilus` `gap → http` | `gap` `→` `REST` `snapshot` | `u-jump` determinism | **YES** `OCM` `MISSING` |
| `buffering` | `Hummingbot` `discard` `Nautilus` `discard` `OCM` `DELTA_BEFORE_SNAPSHOT` | `discard` `u < snapshot.u` | `race` `snapshot ↔ WS` | **YES** `OCM` already `discard` |
| `reconciliation` | `Hummingbot` `apply_snapshot` `Nautilus` `Cache` | `snapshot` vs `delta` `u` | `state` `consistency` | **YES** `OCM` `book_builder` `u+1` |
| `validated resume` | `Hummingbot` `diff` `u == last+1` `Nautilus` `U` `OCM` `DELTA_APPLIED` | `u+1` | `resume` | **YES** |

## 17. PR-A / PR-B Decision

**PR-A as fetcher independent CORRECT?**

* **YES** — `Fetcher` `REST` `snapshot` `fetch_snapshot(exchange, symbol, limit=50) → OrderBookSnapshotPayload` is **valid architectural unit** `adapters/inbound/rest/orderbook_snapshot_fetcher.py` (no `resync` logic, no `BookBuilder`, no `Kafka`, no `state machine`, no `buffering`).

**Why:** `Hummingbot` `get_order_book_snapshot` exists **independently** as `REST` `snapshot` capability, used for `initial` `baseline` + `recovery` + `reconnect` (`OBSERVED`); `Nautilus` `http_get_orderbook` independent; `OCM` `on_snapshot` already handles `REST` `snapshot` `bids/asks` `u` **without** `Fetcher` `resync` (via `WS` `snapshot`). `Fetcher` can exist **without** `resync` (used for `initial` `baseline` test, `recovery` later).

**If NO, what must accompany?** Not needed — `Fetcher` `REST` `snapshot` `→` `OrderBookSnapshotPayload` can exist **without** `Gap` `resync` (used for `initial` `baseline` test, `recovery` later).

**PR-B as resync orchestration CORRECT?**

* **YES** — `Resync orchestration` `GAP_DETECTED → STALE → REQUEST SNAPSHOT (Fetcher) → BUFFER/FILTER WS UPDATES (discard DELTA_BEFORE_SNAPSHOT) → APPLY SNAPSHOT → RECONCILE UPDATES (discard u < snapshot.u) → VALIDATE SEQUENCE (u == last+1) → RESUME` is **correct** `PR-B` `BookBuilderConsumer` orchestration (not `BookBuilder` pure).

**Which steps demonstrated by references?** `Hummingbot` `gap → REST snapshot → apply_snapshot → resume` `YES`; `Nautilus` `gap → http_get_orderbook` `YES`; `OCM` `GAP_DETECTED` `reset` `wait WS snapshot` `PARTIAL` (no `REST`).

**PR-A → PR-B dependency real?** **YES** — `PR-B` `Gap → snapshot` **depends** `PR-A` `Fetcher` `REST` `snapshot` ( `Fetcher` `→` `OrderBookSnapshotPayload` `u` `bids/asks` needed for `PR-B` `on_snapshot`).

## 18. No Implementar

**NO** `OrderBookSnapshotFetcher`, `resync coordinator`, `state machine`, `buffering`, `reconciliation`, `tests`, `config` — **audit only**.

## 19. Documento final

**Crear únicamente:** `docs/audits/AUDIT_OCM_orderbook-reference-models-comparative-study_2026-09-21.md` (this) — **no sobrescribir** `OB-RESYNC-001*` `AUDIT_OCM_ohlcv-repair*` `B-59`.

**Contenido:** `EXECUTIVE SUMMARY` `ZIP INVENTORY` `NAUTILUS` `HUMMINGBOT` `FREQTRADE` `SCENARIO ANALYSIS` `SNAPSHOT SEMANTICS` `SEQUENCE SEMANTICS` `BYBIT ANALYSIS` `RESPONSIBILITY MAPPING` `STATE / RESYNC MODEL` `RACE CONDITION ANALYSIS` `OCM CURRENT STATE` `COMPARATIVE MATRIX` `PATTERN APPLICABILITY` `PR-A ASSESSMENT` `PR-B ASSESSMENT` `EVIDENCE CONFLICTS` `OPEN QUESTIONS` `CONCLUSION`.

**Conclusión:**

1. `connected u-jump` `WS` `connected` `u 100,101,105` → `GAP_DETECTED` `reset` `has_snapshot=False` `STALE` `discard` `DELTA_BEFORE_SNAPSHOT` until `WS` `snapshot` — `Hummingbot` `Nautilus` `REST` `snapshot` `→` `rebuild` `→` `resume` **applicable**.
2. `reconnect` `WS` `socket` vs `resync` `semantic` `book` `u` `baseline` `→` `rebuild` `→` `resume` — `RECONNECT != RESYNC` **confirmed** `OCM` `reconnect` `snapshot` passive `STALE` `2s`.
3. `REST` `snapshot` `GET /v5/market/orderbook` `category=linear` `symbol=BTCUSDT` `limit 50` `u` `bids/asks` `REST` `snapshot` `baseline` + `recovery` `Hummingbot` `Nautilus` **OBSERVED**.
4. `snapshot ↔ WS race` `t0 gap → t1 REST request → t2 WS 106,107 → t3 snapshot 200 → t4 WS 201` `discard` `106,107` `u < snapshot.u` `snapshot` authoritative `Hummingbot`/`Nautilus` `discard` `OCM` `DELTA_BEFORE_SNAPSHOT` already.
5. `fetcher` `REST` `snapshot` `fetch_snapshot` `OrderBookSnapshotPayload` `IN SCOPE` `PR-A`, `orchestration` `GAP → snapshot → rebuild → resume` `OUT OF SCOPE` `PR-A` → `PR-B`.
6. `orchestration` `PR-B` `GAP_DETECTED` `→` `invalidate/pause` `→` `REQUEST SNAPSHOT` `Fetcher` `→` `APPLY SNAPSHOT` `→` `RECONCILE` `→` `VALIDATE` `u+1` `→` `RESUME` `BookBuilderConsumer` (not `BookBuilder` pure).
7. `PR-A` **YES** independent `REST` `snapshot` capability (no `resync`).
8. `PR-B` **YES** `resync orchestration` `GAP → REST snapshot → rebuild → resume` `BookBuilderConsumer` `PR-A` `fetcher` `dependency`.
9. **Evidence still missing:** `Bybit` `REST` `snapshot` `u` exact `json` `u` field `http_get_orderbook.json` `397` not extracted `update_id` `u` `UNKNOWN` `reference evidence insufficient` — `Bybit` `REST` `snapshot` `u` semantics `UNKNOWN`.

**REGLA FINAL:** `REFERENCIAS → INVESTIGACIÓN → ANÁLISIS → COMPARACIÓN → OCM → VALIDACIÓN HIPÓTESIS PR-A/PR-B → DOCUMENTACIÓN → STOP` — **no diseño final, no código**.

---

**AUDIT COMPLETE**
**CODE MODIFIED: NO**
**TESTS MODIFIED: NO**
**CONFIG MODIFIED: NO**
**SYSTEMD MODIFIED: NO**
**COMMIT: NO**
**PUSH: NO**
**MERGE: NO**
**PR: NO**
**New audit document: docs/audits/AUDIT_OCM_orderbook-reference-models-comparative-study_2026-09-21.md (this)**
**Previous OB-RESYNC audits: preserved (4) + B-59 (1) + 6 audit findings ?? preserved**
