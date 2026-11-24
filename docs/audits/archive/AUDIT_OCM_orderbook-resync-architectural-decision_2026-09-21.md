# AUDIT — OrderBook Resync Architectural Decision — OB-RESYNC-001

**ID:** AUDIT-OB-RESYNC-ARCH-2026-09-21
**Date:** 2026-09-21
**Scope:** OrderBook `Bybit` `BTC-USDT-PERP` `u` `snapshot` `delta` `gap` `reconnect` `resync` `REST` `WS` `Kafka` `Bronze` `OHLCV` vs `OrderBook`
**Git:** `feat/m22b-governance` `857cfe60` `M22-B` `?? 6 audit + 3 OB-RESYNC + AUDIT_OCM_comparative` preserved
**Previous:** `AUDIT_OCM_orderbook-resync-snapshot-audit_2026-09-20.md` `PARTIALLY_CONFIRMED` + `validation-phase2.md` `PARTIALLY_CONFIRMED` `RECONNECT != RESYNC` + `causality-audit.md` `PARTIAL` + `AUDIT_OCM_comparative-architecture-refs_2026-09-21.md` `333` `REST+WS` pattern
**Files:** `cryptofeed_orderbook_stream.py:246`, `orderbook_producer.py:129`, `book_builder.py:218`, `book_builder_consumer.py:284`, `shared/kafka/schemas/orderbook.py:169`, `gap_recovery_fetcher.py:6`, `hummingbot bybit_perpetual_api_order_book_data_source.py` `20582`, `nautilus http_get_orderbook.json` `397`, `freqtrade ohlcv` `trades-to-ohlcv`

## 1. Executive Summary

`OrderBook` `WS` `L2_BOOK` `snapshot (u)` + `delta (u)` `GAP_DETECTED` `u != last+1` `BookBuilder` `reset` `wait WS snapshot` is **semánticamente suficiente** para `gap` con `disconnect` (`Cryptofeed` `reconnect` → `snapshot`), **insuficiente** para `u-jump` sin `disconnect` (`WS` `connected` `103` gap `STALE` `2s` indefinido hasta próximo `snapshot`). `REST` `OrderBookSnapshotFetcher` `Bybit` `/v5/market/orderbook` es **requerido** para determinismo `u-jump` sin `disconnect`, **no** para `global OHLCV` (`OHLCV` `REST` `fetch_ohlcv` `historical` independiente `GapRecoveryFetcher` `trades`).

**Decision:** **Sí** `REST Snapshot` activo durante vida `WS` `OrderBook`, `GAP_DETECTED` `→` `invalidate/pause` `→` `REST snapshot` `→` `validate` `→` `baseline` `u` `→` `buffered deltas` `u+1` `→` `validated resume`. `PR-A` `REST Snapshot capability` **solo** `OrderBookSnapshotFetcher` `Bybit` `limit 50` `u` `bids/asks`, **no** `resync orchestration` (PR-B).

## 2. Evidence Reviewed

- `AUDIT_OCM_orderbook-resync-snapshot-audit_2026-09-20.md` `270` `PARTIALLY_CONFIRMED`
- `AUDIT_OCM_orderbook-resync-validation-phase2_2026-09-20.md` `252` `RECONNECT != RESYNC`
- `AUDIT_OCM_orderbook-resync-causality-audit_2026-09-21.md` `161` `global OHLCV ≠ OrderBook`
- `AUDIT_OCM_comparative-architecture-refs_2026-09-21.md` `333` `Hummingbot` `get_order_book_snapshot` `20582` `Nautilus` `http_get_orderbook.json` `Freqtrade` `download-data`
- `docs/audit/deployment-runtime-implementation-plan.md` `P0-P3` `Validated Architecture` `systemd=app`

## 3. Current OCM OrderBook Flow

```
Exchange Bybit WS (data.u/seq/cts, b/a, u monótono +1)
  ↓ Cryptofeed FeedHandler L2_BOOK
CryptofeedOrderBookStream._translate_and_dispatch (book.delta None → snapshot, else delta) :200 :230
  ↓ _bybit_sequence(raw) → (u, seq, cts) :246
  ↓ on_snapshot(update_id=u) / on_delta(update_id=u) :215 :238
OrderBookKafkaProducer.on_snapshot/on_delta → OrderBookSnapshotPayload/DeltaPayload (update_id) → Kafka orderbook.raw (key=bybit:BTC/USDT, partition 0) :159
  ↓ Kafka orderbook.raw (schema v2, 1h)
BookBuilderConsumer.poll → BookBuilder (pure) :198
  ↓ on_snapshot → SNAPSHOT_APPLIED → publish book.snapshot
  ↓ on_delta → GAP_DETECTED (u != last+1 → reset) → handled (warning) → wait → WS snapshot → rebuild
```

**Status per etapa:**

| Etapa | Implemented | Connected | Executed | Persisted | Validated | Operational |
|---|---|---|---|---|---|---|
| Exchange WS | YES | YES | YES | N/A | YES | UNKNOWN |
| Cryptofeed | YES | YES | YES | N/A | YES | UNKNOWN |
| OrderBook stream | YES | YES | YES | N/A | YES | YES |
| Sequence validation (u) | YES | YES | YES | N/A | YES | YES |
| Gap detection (u+1) | YES | YES | YES | N/A | YES | YES |
| BookBuilder state | YES | YES | YES | N/A | YES | YES |
| Snapshot WS | YES | YES | YES | YES | YES | YES |
| Resync explicit | **NO** | **NO** | **NO** | **NO** | **NO** | **NO** |
| Delta resume | YES | YES | YES | N/A | YES | YES |
| Kafka orderbook.raw | YES | YES | YES | YES | YES | YES |
| Bronze orderbook | **NO** | **NO** | **NO** | **NO** | **NO** | **NO** |

## 4. Failure Scenarios

**A — WS normal `snapshot → delta 1→2→3`:** `SNAPSHOT_APPLIED` `last=500` `delta 501` `DELTA_APPLIED` `last=501` → **works**.

**B — WS disconnect `snapshot→delta→disconnect→reconnect→snapshot→resume`:** `Cryptofeed` `reconnect` → `snapshot` `delta None` → `on_snapshot` `reset` `last=800` `snapshot` `bids/asks` `→` `book.snapshot` → `delta 801` `DELTA_APPLIED` — **who produces snapshot:** `Cryptofeed` `Bybit` `L2_BOOK` `snapshot` (`delta None`); **who baseline:** `BookBuilder.on_snapshot:168` `last_update_id = u`; **who validates:** `BookBuilder` `u+1` — **works** `B` passive.

**C — WS connected + u-jump `100,101,105`:** `100 SNAPSHOT last=100` `101 DELTA_APPLIED last=101` `105 != 102 → GAP_DETECTED dE=3` `reset` `has_snapshot=False` `handled` `warning` `book_builder.py:228` → `106 DELTA_BEFORE_SNAPSHOT` discarded → **no snapshot requested**, **who obtains snapshot:** **nobody** (no `REST` fetcher) → **stale** `check_stale:272` `2000ms` `STALE` → **until next WS snapshot** (next `WS` message may be `delta 106` not `snapshot` → remains `STALE`).

**Main case C:** `gap` detection `YES`, `snapshot` request `NO`, `rebuild` `NO` until `WS` `snapshot`, `resume` `DELTA_BEFORE_SNAPSHOT` discarded.

## 5. Reconnect vs Resync

**Reconnect** `WS` `FeedHandler` `Bybit` `reconnect` `on_error` **recovers transport** (`socket`).

**Resync** must recover `semantic state` `book` `bids/asks` `last_update_id` `baseline` `u` `continuity` `snapshot.u → next delta u+1`.

**Evidence:** `book_builder.py:39` `GAP → invalidar → NO aplicar delta → emitir GapDetected → espera snapshot` `book_builder_consumer.py:284` `handled` `warning` — **no `reconnect`**.

**Conclusion:** `RECONNECT != RESYNC` — `reconnect` `WS` `snapshot` is **one** `resync` source, but `u-jump` without `disconnect` has `reconnect` **without** `resync` → need `REST` `snapshot`.

## 6. REST Snapshot Analysis

**Exists:** **NO** `OrderBookSnapshotFetcher` `adapters/inbound/rest/` — `grep -rn orderbook.*snapshot` `0` orderbook `REST`.

**Reusable:** `GapRecoveryFetcher` `trades` `REST` `trades` `gap [start,end)` `gap_recovery_fetcher.py:6` **not reusable** (trades `RawTrade` vs orderbook `PriceLevel` `bids/asks` `u`).

**Bybit REST endpoint:** `GET /v5/market/orderbook` `category=linear` `symbol=BTCUSDT` `limit=50` `retCode:0` `result: {s, b: [[price,qty]], a: [[price,qty]], ts, u}` — **Hummingbot** `bybit_perpetual_api_order_book_data_source.py:20582` `get_order_book_snapshot` uses this.

## 7. Hummingbot Comparison

**File:** `hummingbot-master/hummingbot/connector/derivative/bybit_perpetual/bybit_perpetual_api_order_book_data_source.py` `20582` `get_order_book_snapshot` `REST` `snapshot` `update_id` + `listen_for_order_book_diffs` `WS` `diff` `update_id` `+1` `OrderBookTracker` `apply_snapshot` `apply_diffs` `gap → _order_book_snapshot() → REST`.

**Snapshot request:** `gap` `diff.update_id != last_update_id+1` → `await self.get_order_book_snapshot(trading_pair)` `REST` `snapshot` `bids/asks` `update_id`.

**Rebuild:** `OrderBook.apply_snapshot(snapshot)` `reset` `bids/asks` `last_update_id = snapshot.update_id`.

**Resume:** `diff.update_id == last_update_id+1` `apply_diffs`.

**Pattern:** `REST snapshot / WS delta / sequence gap → REST snapshot / rebuild / resume` — **OBSERVED** `Hummingbot` `bybit_perpetual`.

## 8. Nautilus Comparison

**Files:** `nautilus_trader-develop/crates/adapters/bybit/test_data/http_get_orderbook.json` `397` `ws_orderbook_snapshot.json` `274` `ws_orderbook_delta.json` `221` + `crates/adapters/binance/src/spot/sbe/stream/depth_snapshot.rs` `8629`.

**Pattern:** `http_get_orderbook.json` `REST` `snapshot` `depth` + `ws_orderbook_snapshot.json` `WS` `snapshot` `delta` + `sequence` `U` `pu` `Bybit` `DataEngine` `Cache` `snapshot` `baseline` `WS` `delta` `apply` `gap → http_get_orderbook` **OBSERVED** `http_get_orderbook` `REST` `snapshot` `recovery` (same as Hummingbot).

## 9. Freqtrade Comparison

**Files:** `freqtrade-develop` `docs/commands/trades-to-ohlcv.md` `freqtrade/data/history/` `ohlcv` `trades` `download-data` `trades-to-ohlcv`.

**Pattern:** `REST` `fetch_ohlcv` `historical` `download-data` vs `live` `REST` `poll` `OHLCV` `refresh` — **no WS OrderBook** `sequence` `gap` `resync`. `Freqtrade` **NOT DIRECTLY COMPARABLE** for `OrderBook` `u-jump` (OHLCV `timeframe` `OHLCV` `deduplication` `trades-to-ohlcv`, not `orderbook` `u`).

## 10. Snapshot ↔ WS Race

```
t0 GAP_DETECTED (u 105, expected 102) → reset has_snapshot=False
t1 REST snapshot request (Bybit /v5/market/orderbook) → in-flight
t2 WS continues deltas 106,107 (u 106,107) → DELTA_BEFORE_SNAPSHOT discarded (book_builder.py:207)
t3 REST snapshot arrives (u 200, bids/asks) → on_snapshot → last_update_id=200 → rebuild
t4 WS delta 201 (u 201) → 201==200+1 → DELTA_APPLIED → resume
t4' WS delta 199 (stale, u < snapshot.u) → GAP_DETECTED (199 !=201) → discard (idempotence)
```

**Buffering:** **discard** `DELTA_BEFORE_SNAPSHOT` until `snapshot` (no buffering, no replay).

**Resume point:** `snapshot.update_id +1`.

**Avoid loss:** `snapshot` `u` **after** `gap` ensures no `duplicates` (Kafka `orderbook.raw` retains `106,107` for replay `1h`).

**Avoid duplicates:** `snapshot` `200` `bids/asks` authoritative, `106,107` discarded `handled` `book.delta` not `book.*` (no `Bronze` `orderbook`).

## 11. Source of Truth

| Source | Represents | Validates |
|---|---|---|
| **Exchange** Bybit `orderbook` `u` | Ground truth `bids/asks` `u` | — |
| **REST snapshot** `GET /v5/market/orderbook` | **Complete state** consulted `u` `bids/asks` | Exchange (authoritative) |
| **WS delta** `orderbook.raw` `u` | **Incremental mutation** `bids/asks` `price_str/size_str` | `REST` snapshot (baseline) |
| **OCM materialized OrderBook** `BookBuilder` `bids/asks` `last_update_id` | Reconstructed `bids/asks` `Decimal` `last_update_id` | `REST` `snapshot` (recovery) + `WS` `delta` `u` |

**Not all source of truth** — `REST snapshot` is `recovery authority`, `WS delta` is `realtime transport`, `Exchange` is `source of truth`.

## 12. Target OrderBook Recovery Model

```
                  REST
                   │
                SNAPSHOT (Bybit /v5/market/orderbook, limit 50, category=linear, symbol=BTCUSDT)
                   │
                   ▼
              BASELINE (BookBuilder.last_update_id = snapshot.update_id, has_snapshot=True)
                   │
                   ▼
                  WS
                   │
                 DELTAS (bids/asks, update_id=u, cross_seq, cts_ms)
                   │
                   ▼
             SEQUENCE CHECK (u == last+1 ?)
                   │
             ┌─────┴─────┐
             │           │
           VALID        GAP (u != last+1)
             │           │
             │           ▼
             │       STOP/RESET (has_snapshot=False, bids/asks cleared, GAP_DETECTED)
             │           │
             │       REST SNAPSHOT (OrderBookSnapshotFetcher.fetch_snapshot(exchange, symbol))
             │           │
             │        BASELINE (on_snapshot)
             │           │
             └───────┬───┘
                     ▼
                RESUME WS (delta u == last+1 → DELTA_APPLIED → book.delta)
```

**Only if evidence justifies:** **YES** `gap` without `disconnect` `dE=3` `book_builder.py:228` not recovered by `WS` `snapshot` until `reconnect` → `REST` required for determinism (Hummingbot/Nautilus `OBSERVED`).

## 13. Responsibility Boundaries

| Responsabilidad | Componente | Actual | Target PR-A |
|---|---|---|---|
| `WS connection` | `CryptofeedOrderBookStream` `FeedHandler` | **YES** | — |
| `WS subscription` | `CryptofeedOrderBookStream` `L2_BOOK` `symbols` | **YES** | — |
| `Snapshot REST` | `OrderBookSnapshotFetcher` | **MISSING** | **PR-A** `adapters/inbound/rest/orderbook_snapshot_fetcher.py` |
| `Sequence validation` | `BookBuilder.on_delta` | **YES** | — |
| `Gap detection` | `BookBuilder` `GAP_DETECTED` | **YES** | — |
| `State invalidation` | `BookBuilder` `reset` | **YES** | — |
| `Resync orchestration` | `BookBuilderConsumer` `GAP_DETECTED → fetch` | **MISSING** | **PR-B** |
| `Snapshot validation` | `BookBuilder.on_snapshot` `Decimal` | **YES** | — |
| `Delta buffering` | `BookBuilder` `DELTA_BEFORE_SNAPSHOT` discard | **YES** (discard) | **PR-B** (buffer optional) |
| `Delta reconciliation` | `BookBuilder` `u+1` | **YES** | — |
| `Book rebuild` | `BookBuilder.on_snapshot` | **YES** | — |
| `Kafka publication` | `OrderBookKafkaProducer` `orderbook.raw` + `BookBuilderConsumer` `book.*` | **YES** | — |
| `Persistence` | `Kafka` `orderbook.raw` `1h` `book.*` | **YES** | — |
| `Observability` | `KafkaMetrics` `event_failed` | **PARTIAL** | **PR-E** `resync_total` |

**No God Adapter** — `REST` `snapshot` (`PR-A`) separate from `WS` `stream` (`CryptofeedOrderBookStream`), `BookBuilder` `pure` `Decimal`, `Consumer` `orchestration`.

## 14. Kafka Interaction

- **During resync:** **no events** `book_builder_consumer.py:284` `GAP_DETECTED` `handled` `no written` `book.*` silent until `snapshot`.
- **Should publish?** **No** `book.delta` invalid `STALE` until `snapshot` — `orderbook.raw` **YES** `GAP` `delta` `u 106` still `orderbook.raw` `persisted` `Kafka` `1h` (live state vs dataset `orderbook.raw` complete).
- **Risk inconsistent `orderbook.raw`?** **No** `orderbook.raw` `u` `sequence` metadata `timestamps` preserved for replay.

## 15. Dataset Completeness

**Live OrderBook correctness:** `BookBuilder` `STALE` window `2s` + `WS` `snapshot` `wait` → **PARTIAL** (deterministic only with `REST`).

**Historical dataset completeness:** `orderbook` **NO** `Bronze` `historical` (only `ohlcv.raw` `Bronze` `Iceberg` `deduplication`).

**Live vs Historical:** `resync` guarantees **live** `book.delta` correctness, **not** `historical` `orderbook` dataset (no `Bronze` `orderbook`).

## 16. OHLCV Recovery Boundary

**OrderBook resync** `GAP → REST snapshot` `Bybit` `orderbook` `limit 50` vs **OHLCV recovery** `GapRecoveryFetcher` `trades` `gap [start,end)` `REST_RECOVERY` `gap_recovery_fetcher.py:6` + `TradesBackfillFetcher` `historical` — **share** `Quality` `is_new` `observability` `idempotency`, **not share** `state` `sequence` `snapshot` `persistence`.

## 17. Architectural Decision

**1. REST Snapshot for u-jump without disconnect?** **YES** `REST` active during `WS` life.
**2. REST active during WS life?** **YES** `GAP_DETECTED` → `REST`.
**3. Who initiates resync?** `BookBuilderConsumer` `GAP_DETECTED` `orchestration` (not `BookBuilder` pure).
**4. Who obtains snapshot?** `OrderBookSnapshotFetcher` `REST` `Bybit` `/v5/market/orderbook`.
**5. Who establishes baseline?** `BookBuilder.on_snapshot` `last_update_id = snapshot.update_id`.
**6. Deltas during snapshot?** **Discard** `DELTA_BEFORE_SNAPSHOT` until `snapshot` (no buffer, simplest `D-7a` atomic).
**7. Who validates continuity after?** `BookBuilder.on_delta` `u == last+1`.
**8. What is resume?** `delta` `u == last+1` `DELTA_APPLIED` `book.delta` published.
**9. REST fails?** `retry 3` `backoff` `5s` `rate limit` `10/s` → `STALE` `warning` `metrics` `resync_total` `reason=fetch_error` → `WS` `snapshot` fallback `reconnect`.
**10. Snapshot stale?** `validate` `bids/asks` non-empty `update_id >0` `timestamp_ms` `now - cts_ms < 5s` else `retry`.
**11. WS gap during resync?** `GAP_DETECTED` `reset` `has_snapshot=False` → `snapshot` `in-flight` `discard` `DELTA_BEFORE_SNAPSHOT` until new `snapshot`.
**12. Component ownership?** `WS` `CryptofeedOrderBookStream`, `REST` `OrderBookSnapshotFetcher`, `State` `BookBuilder`, `Orchestration` `BookBuilderConsumer`, `Kafka` `OrderBookKafkaProducer`.

## 18. PR-A Scope Decision

**IN SCOPE `PR-A`:** `OrderBookSnapshotFetcher` `adapters/inbound/rest/orderbook_snapshot_fetcher.py` `fetch_snapshot(exchange, symbol, limit=50) -> OrderBookSnapshotPayload` `Bybit` `REST` `/v5/market/orderbook` `category=linear` `symbol=BTCUSDT` `bids/asks` `update_id` `u` `timestamp` `snapshot completeness` `timeout 5s` `retry 3` `rate limit` `error semantics` `ccxt` `fetchL2OrderBook`.

**OUT OF SCOPE `PR-A`:** `Gap → snapshot` orchestration (`PR-B`), `delta buffering/replay`, `Book rebuild` (already `BookBuilder`), `Kafka` publication, `Bronze`, `Silver/Gold`, `observability` `resync_total` (`PR-E`), `tests` `E2E` (`PR-D`).

**DEPENDENCIES:** `None` (standalone `REST` `snapshot` capability, `ccxt` already `ccxt_adapter.py`).

**FOLLOW-UP PRs:** `PR-B` `Gap → snapshot` orchestration `BookBuilderConsumer` `GAP_DETECTED → fetch` (dept `PR-A`), `PR-C` `Book rebuild` (no, already), `PR-D` `tests` `gap→snapshot→delta` `test_book_builder_resync.py` (dept `PR-B`), `PR-E` `observability` `orderbook.resync_total` (dept `PR-B`), `PR-F` `documentation` `schemas/orderbook.py` `D-7b`.

## 19. Test Strategy

**Unit:** `snapshot parser` `Bybit` `b/a` `u` `Decimal` `snapshot completeness` `update_id` `0` `error` `quantity<0`; `sequence` `u+1` `cross_seq` `cts`; `errors` `timeout` `5xx` `stale` `invalid` `retry`.

**Integration:** `WS (mock) → gap (105) → REST snapshot (mock 200) → rebuild (last=200) → resume delta 201` `test_book_builder_resync.py` `GapRecovery`.

**Failure:** `REST timeout` `retry` `stale snapshot` `invalid sequence` `repeated gap` `disconnect during resync` `deltas during snapshot` `STALE` `2s`.

**Operational:** `metrics` `orderbook.resync_total` `labels: exchange, symbol, reason (gap)` `logs` `book_builder_invalidated` `recovery latency` `retry count` `resync count` `Kafka` `orderbook.raw` `u`.

## 20. Master Evidence Matrix

| Claim | OCM | Hummingbot | Nautilus | Freqtrade | Confidence |
|---|---|---|---|---|---|
| `REST snapshot` | **MISSING** | **CONFIRMED** `get_order_book_snapshot` | **CONFIRMED** `http_get_orderbook.json` | **NOT COMPARABLE** `OHLCV` `REST` | **HIGH** |
| `WS baseline` | **CONFIRMED** `Cryptofeed` `snapshot` | **CONFIRMED** `OrderBook` `snapshot` | **CONFIRMED** `depth_snapshot` | **NOT COMPARABLE** | **HIGH** |
| `sequence validation` | **CONFIRMED** `u+1` | **CONFIRMED** `update_id` | **CONFIRMED** `U` | **NOT COMPARABLE** | **HIGH** |
| `gap detection` | **CONFIRMED** `GAP_DETECTED` | **CONFIRMED** `update_id != last+1` | **CONFIRMED** `U` `gap` | **NOT COMPARABLE** | **HIGH** |
| `u-jump recovery` | **PARTIAL** `WS` `snapshot` passive | **CONFIRMED** `REST` `snapshot` | **CONFIRMED** `REST` `snapshot` | **NOT COMPARABLE** | **HIGH** |
| `reconnect recovery` | **CONFIRMED** `FeedHandler` | **CONFIRMED** `WS` `reconnect` | **CONFIRMED** `DataEngine` | **NOT COMPARABLE** | **HIGH** |
| `REST-assisted resync` | **MISSING** | **CONFIRMED** `REST` `snapshot` | **CONFIRMED** `http` | **NOT COMPARABLE** | **HIGH** |
| `delta reconciliation` | **PARTIAL** `discard` | **CONFIRMED** `OrderBookTracker` | **CONFIRMED** `Cache` | **CONFIRMED** `trades-to-ohlcv` | **MEDIUM** |
| `validated resume` | **CONFIRMED** `u+1` after `snapshot` | **CONFIRMED** `apply_snapshot` | **CONFIRMED** `depth_snapshot` | **NOT COMPARABLE** | **HIGH** |

## 21. Findings

- **F-1:** `OrderBook` `explicit resync` `MISSING` — `GAP_DETECTED` `reset` `wait` `WS` `snapshot` `PARTIAL` `STALE`.
- **F-2:** `REST` `OrderBook` `snapshot` `MISSING` — `GapRecoveryFetcher` `trades` only.

## 22. Unknowns

- `WS` `snapshot` `guaranteed` after `reconnect` `Bybit` `L2_BOOK` `cryptofeed` `2.4.1` — **UNKNOWN** `reconnect` `snapshot` `order` `sequence` during `reconnect` `UNKNOWN` `reconnect` `snapshot` `sequence` `UNKNOWN`.

## 23. Final Verdict

**PARTIALLY_CONFIRMED — recovery via `WS` `reconnect` `snapshot` exists but not deterministic for `u-jump` without `disconnect`; `REST` `snapshot` required for determinism, not for `global OHLCV`.**

## 24. Proposed PR Boundaries

- **PR-A** `OrderBookSnapshotFetcher` `REST` `Bybit` `MISSING` → `IMPLEMENTED` `adapters/inbound/rest/orderbook_snapshot_fetcher.py`
- **PR-B** `Gap → REST` orchestration `BookBuilderConsumer` `MISSING` → `CONNECTED` (dept `PR-A`)
- **PR-C** `Book rebuild` already `YES`
- **PR-D** `tests` `gap→snapshot→delta` `MISSING` (dept `PR-B`)
- **PR-E** `observability` `orderbook.resync_total` `MISSING` (dept `PR-B`)
- **PR-F** `documentation` `schemas/orderbook.py` `D-7b` already `IMPLEMENTED`

**Minimum 2 PRs:** `PR-A` fetcher + `PR-B` orchestration.

## References

- `packages/market_data/adapters/inbound/websocket/cryptofeed_orderbook_stream.py:246` `u`
- `shared/kafka/schemas/orderbook.py:169` `u monótono`
- `book_builder.py:218` `GAP_DETECTED`
- `hummingbot-master/hummingbot/connector/derivative/bybit_perpetual/bybit_perpetual_api_order_book_data_source.py:20582` `get_order_book_snapshot`
- `nautilus_trader-develop/crates/adapters/bybit/test_data/http_get_orderbook.json:397`
