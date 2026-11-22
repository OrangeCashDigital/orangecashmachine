# OB-RESYNC-001 — Causality Audit — OrderBook Gap → Dataset Incompleteness

**ID:** OB-RESYNC-001
**Phase:** 3 — Causality and Operational Sufficiency of Cryptofeed Reconnect+Snapshot
**Date:** 2026-09-21
**Auditor:** Senior Market Data / Distributed Systems — READ-ONLY
**Scope:** OrderBook `Bybit` `BTC-USDT-PERP` vs `OHLCV` `BTC/USDT` — `WS` `REST` `Kafka` `Bronze` `Quality` `Silver` `Gold` — `Gap` `Reconnect` `Resync`
**Git state:** `feat/m22b-governance` `857cfe60` `M22-B` `?? 6 audit + OB-RESYNC-001 + validation-phase2` preserved
**Previous audits:** `docs/audits/AUDIT_OCM_orderbook-resync-snapshot-audit_2026-09-20.md` `270` `PARTIALLY_CONFIRMED` + `AUDIT_OCM_orderbook-resync-validation-phase2_2026-09-20.md` `252` `PARTIALLY_CONFIRMED` `RECONNECT != RESYNC` `u` `GAP_DETECTED` `passive snapshot`
**Files inspected:** `cryptofeed_orderbook_stream.py:1`, `orderbook_producer.py:1`, `book_builder.py:218`, `book_builder_consumer.py:284`, `gap_recovery_fetcher.py:1`, `shared/kafka/schemas/orderbook.py:169`, `test_book_builder.py:104`, `streaming_hydra.py:226`, `config/market_data/feeds.yaml`, `deploy/systemd/ocm-streaming.service.template:11`, `packages/market_data/adapters/inbound/rest/ohlcv_fetcher.py`, `adapters/outbound/exchange/ccxt_adapter.py:692`, `application/consumers/quality_consumer.py:80`

## 1. Gap — What Happens Today

**Condition:** `BookBuilder.on_delta:218` `if last_update_id !=0 and update_id != last+1` `GAP_DETECTED` `book_builder.py:218-231` `dE = update_id - expected`.

**State:** `state.reset()` `has_snapshot=False` `last_update_id=0` `bids/asks cleared`.

**Event:** `BookBuilderOutcome Kind=GAP_DETECTED` `detail "gap detectado: esperado u=102, recibido u=103 (dE=1)"` `book_builder.py:222`.

**Receiver:** `BookBuilderConsumer._apply_outcome:284` `if kind in (GAP_DETECTED, STRUCTURAL_INVALID): event_failed reason=gap_detected → warning "book_builder_invalidated | gap..."` `book_builder_consumer.py:290` **no snapshot request**, `handled` not `written`.

**Deltas discarded:** `GAP_DETECTED` deltas **not published** `book.*` (handled), next `DELTA_BEFORE_SNAPSHOT` `book_builder.py:207` discarded until `snapshot`.

**Snapshot request:** **NO** — no `REST` orderbook snapshot, no `GAP → snapshot` call chain.

**Source:** `WS` `Cryptofeed` `FeedHandler` `Bybit` `L2_BOOK` `snapshot` `delta=None` `cryptofeed_orderbook_stream.py:200`.

**New baseline:** `on_snapshot:168` `last_update_id = snapshot.update_id` `has_snapshot=True` `bids/asks` rebuilt.

**Resume:** `delta` `u == last+1` `DELTA_APPLIED` `book_builder.py:248` `last_update_id = update_id`.

**Evidence:** `book_builder.py:218-231`, `book_builder_consumer.py:284-290`, `test_book_builder.py:104` `GAP_DETECTED` `DELTA_BEFORE_SNAPSHOT`.

## 2. Reconnect vs Resync

**Reconnect:** `Cryptofeed` `FeedHandler` `Bybit` `WebSocket` `on_error` `feedhandler.py` (vendor) **YES** — `FeedHandler.run` `reconnect True` (default). `CryptofeedOrderBookStream.start:137` `run(start_loop=False)`.

**Resync:** Must recover `semantic state` `book` `last_update_id` + `bids/asks` `Decimal` + `continuity` `snapshot.u → next delta u+1`.

**Does reconnect imply snapshot?** **YES** for `Cryptofeed` `Bybit` `L2_BOOK` — first message after `subscribe` is `snapshot` `delta None` `book.book.to_dict()` `cryptofeed_orderbook_stream.py:200`. **Guaranteed** by `Bybit` `topic orderBook` `type snapshot` vs `delta` (vendor contract).

**Does reconnect guarantee baseline?** **YES** `on_snapshot` `reset` `last_update_id = u` establishes.

**Does reconnect guarantee resync?** **PARTIAL** — `RECONNECT` **recovers transport**, `RESYNC` **recovers semantic state** only if `WS` disconnect triggers `GAP_DETECTED` **and** `snapshot` arrives. If `gap` occurs **without** `WS disconnect` (lost `delta` `102` while `WS` connected), `GAP_DETECTED` → `reset` → `DELTA_BEFORE_SNAPSHOT` discards until **next** `WS snapshot` — **no snapshot arrives until next reconnect** (which may not happen) → `STALE` `2000ms` `check_stale:272` `STALE` outcome.

**RECONNECT != RESYNC:** `RECONNECT` is `transport` (`FeedHandler`), `RESYNC` is `semantic` (`BookBuilder` + `snapshot`). `GAP` without `disconnect` → `RECONNECT` does not occur → `RESYNC` **not** triggered → **window STALE**.

## 3. Snapshot Origin

**Current snapshot source:** **A/B** — `WebSocket` `Cryptofeed` `Bybit` `L2_BOOK` `snapshot` via `book.delta None` `cryptofeed_orderbook_stream.py:200` (not `REST`).

**REST endpoint:** Bybit `GET /v5/market/orderbook` `category=linear` `symbol=BTCUSDT` `limit=50` — **NOT used** by OCM (`GapRecoveryFetcher` only `trades` `gap_recovery_fetcher.py:6`, `derivatives_fetcher` only `open_interest`).

**REST OrderBook recovery:** **MISSING** — no `OrderBookSnapshotFetcher` `adapters/inbound/rest/`.

## 4. Need for OrderBookSnapshotFetcher

**Alternative A `WS reconnect → snapshot`:** Consistency `STALE` window `2s` + `WS` reconnect latency `~1s` + `snapshot` depth `50` — simple, uses `cryptofeed` existing; risk `gap` without `disconnect` → indefinite `STALE`.

**Alternative B `GAP → REST snapshot`:** Consistency deterministic `GAP` immediate `REST` `snapshot` `u` baseline; latency `REST` `~100ms` + `rate limit` `10/s` `ccxt` `coste bajo`; needs `OrderBookSnapshotFetcher`.

**Alternative C `GAP → WS reconnect → snapshot`:** `GAP_DETECTED` `→` `FeedHandler.stop_async` + `start` → `snapshot`; risk `WS storm` `multiple gaps`.

**Alternative D `GAP → REST + WS reconnect`:** Most robust but complexity `buffered deltas` `replay`.

**Repository contracts:** `D-7b` `u` `+1` `P0` supports `B`, `cryptofeed` `reconnect` supports `A`, no `REST` orderbook in OCM.

**Conclusion:** `REST` fetcher **not strictly required** if `WS` `reconnect` guaranteed `snapshot` **and** `gap` always coincides with `disconnect` — but `P0` `seq` gaps `9-7593` while `u` `+1` suggests `u` gap can occur **without** `WS` disconnect (e.g., `Bybit` `u` jump), so `B` is **safer**.

## 5. Causality with Dataset Incompleteness

**Separate flows:**

```
OHLCV: REST CCXT fetch_ohlcv (paginated 500, since/limit) → Kafka ohlcv.raw → Bronze Iceberg → Quality (gap scanner) → Silver/Gold (reconciliation) — GapRecoveryFetcher trades only
OrderBook: WS snapshot+delta (u) → Kafka orderbook.raw → BookBuilder → book.delta/snapshot → Kafka book.* (no Bronze) — no REST recovery
```

**No dependency** `OHLCV` vs `OrderBook` (different `Kafka` topics `ohlcv.raw` vs `orderbook.raw`, different `Bronze` `ohlcv` only).

**What dataset has gaps?**

- **OHLCV completeness:** `Quality` `gap_scanner.py:16` `scan_gaps` `2× timeframe` `gap` → **no** `gap → REST` `backfill` automatic for `OHLCV`? `OHLCV` `historical` `backfill` exists `incremental.py` but `Quality` `gap` **not** trigger `REST` — `OHLCV` gap remains **UNKNOWN** causality.
- **OrderBook completeness:** `book.delta` missing until `snapshot` → `Silver` `orderbook` incomplete `STALE` `UNKNOWN` (no `Silver` `orderbook` consumer).
- **Kafka completeness:** `orderbook.raw` **complete** (all `u` published, `GAP` only in `BookBuilder` `handled` not `orderbook.raw` loss).
- **Bronze completeness:** `orderbook` **NO Bronze** (only `ohlcv.raw` → Bronze), so `OrderBook` gap does **not** affect `Bronze` `OHLCV` dataset.

**First missing data:** `BookBuilder` `GAP_DETECTED` `106` not `book.delta`, but `orderbook.raw` `106` exists `Kafka`; `Silver` `orderbook` would be incomplete `STALE` window.

**Causality:** `OrderBook resync missing` **→** `OrderBook` `book.delta` `STALE` **PARTIAL**, **NOT** `global OHLCV` `Bronze/Silver/Gold` `OHLCV` incompleteness (different pipeline, `OHLCV` has own `REST` `fetch_ohlcv` `CCXT` with `paginación` and `backfill`).

## 6. Tests

- `test_gap_detected_and_invalidates` `YES` (gap)
- `gap → snapshot → delta` `MISSING` (no `recovery` chain)
- `reconnect` `NO`
- `snapshot recovery` `PARTIAL` (`test_snapshot_resets`)
- `MISSING TEST` `gap→snapshot→delta 201 accepted` (no `E2E` `resync`)

## 7. Observability

- `gap` `event_failed reason=gap_detected` `book_builder_consumer.py:285`
- `stale` `reason=stale` `check_stale:306`
- `orderbook.resync_total` **MISSING** (proposal)

## 8. Verdict

**Q1 Gap detection real?** **YES** `u != last+1` `book_builder.py:218`.
**Q2 Recovery auto?** **PARTIAL** `passive WS snapshot` **YES**, `active REST` **NO**.
**Q3 Semantically equivalent resync?** **NO** — `RECONNECT` `WS` `snapshot` is `resync` only if `gap` coincides with `disconnect`; `gap` without `disconnect` → `STALE` indefinite.
**Q4 Snapshot guaranteed after reconnect?** **YES** per `cryptofeed` `Bybit` `L2_BOOK` contract.
**Q5 REST explicit?** **NO** `MISSING`.
**Q6 REST absence causes data loss?** **PARTIAL** — `book.delta` `STALE` window `2s` + until `WS` snapshot, not `orderbook.raw` loss.
**Q7 Global incompleteness cause?** **NOT ESTABLISHED** — `OrderBook` `book.*` gap **PARTIAL** causality `OrderBook` `STALE`, not `OHLCV` `Bronze/Silver/Gold` (separate `REST` `fetch_ohlcv`).
**Q8 Fetcher needed?** **PARTIALLY_CONFIRMED** — `gap` without `disconnect` requires `REST` snapshot for deterministic recovery.
**Q9 Layer?** `adapters/inbound/rest/orderbook_snapshot_fetcher.py` `infrastructure` + `BookBuilderConsumer` orchestration `application`.
**Q10 Other components before?** `BookBuilder` `STALE` `check_stale` + `Kafka` `book.*` `Bronze` `orderbook` not needed for fetcher.

## 9. Matrix

| Capacidad | Implemented | Connected | Executed | Persisted | Validated | Operational | Status |
|---|---|---|---|---|---|---|---|
| WS OrderBook | YES | YES | YES | N/A | YES | UNKNOWN | PARTIAL |
| Snapshot | YES | YES | YES | YES | YES | YES | PARTIAL |
| Delta | YES | YES | YES | YES | YES | YES | YES |
| Sequence tracking (u) | YES | YES | YES | YES | YES | YES | YES |
| Gap detection | YES | YES | YES | N/A | YES | YES | YES |
| Reconnect | YES | YES | YES | N/A | YES | UNKNOWN | YES |
| Explicit resync | **NO** | **NO** | **NO** | **NO** | **NO** | **NO** | **MISSING** |
| REST recovery | **NO** | **NO** | **NO** | **NO** | **NO** | **NO** | **MISSING** |
| Book rebuild | YES | YES | YES | N/A | YES | YES | YES |
| Validated resume | YES | YES | YES | N/A | YES | YES | YES |
| Kafka orderbook.raw | YES | YES | YES | YES | YES | YES | YES |
| Bronze orderbook | **NO** | **NO** | **NO** | **NO** | **NO** | **NO** | MISSING |
| Quality | YES | YES | YES | N/A | YES | YES | PARTIAL |
| Dataset completeness | **NO** | **NO** | **NO** | **NO** | **NO** | **NO** | UNKNOWN |

## 10. Recommendation

**Verdict: PARTIALLY_CONFIRMED — recovery exists via `WS reconnect` but not deterministic for `gap` without `disconnect`; `REST` snapshot not required for `global OHLCV` incompleteness, but required for deterministic `OrderBook` `book.delta` `STALE` recovery.**

**PRs:**

- **PR-A** `OrderBookSnapshotFetcher` `REST` `Bybit` `/v5/market/orderbook` (no deps)
- **PR-B** `GAP → REST snapshot` `BookBuilderConsumer` `GAP_DETECTED → fetch` (dept `PR-A`)
- **PR-C** already `Book rebuild` (no PR)
- **PR-D** `tests` `gap→snapshot→delta` (dept `PR-B`)
- **PR-E** `metrics` `orderbook.resync_total` (dept `PR-B`)

**Minimum 2 PRs:** `PR-A` fetcher + `PR-B` orchestration.

**Not implemented:** `REST` fetcher, `explicit resync`, `metrics`, `causality` global `OHLCV` vs `OrderBook` separated.

---

**AUDIT COMPLETE**
**Finding: OB-RESYNC-001**
**Previous: PARTIALLY_CONFIRMED**
**New: PARTIALLY_CONFIRMED (gap without disconnect → STALE, REST not needed for OHLCV global)**
**Implementation: NOT PERFORMED**
**Commit: NOT PERFORMED**
