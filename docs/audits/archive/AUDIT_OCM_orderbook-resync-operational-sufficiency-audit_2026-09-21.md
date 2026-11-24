# OB-RESYNC-001 — Operational Sufficiency — Explicit OrderBook Resync Necessity

**ID:** OB-RESYNC-001
**Phase:** 4 — Operational Sufficiency and Causality with Global Dataset
**Date:** 2026-09-21
**Auditor:** Senior Market Data / Distributed Systems — READ-ONLY
**Scope:** OrderBook `Bybit` `BTC-USDT-PERP` `WS` `u` `snapshot` vs `OHLCV` `BTC/USDT` `REST` `Kafka` `Bronze` `Silver` `Gold` — Explicit `OrderBookSnapshotFetcher` necessity
**Git state:** `feat/m22b-governance` `857cfe60` `M22-B` `?? 6 audit + 3 OB-RESYNC + VERIFICATION_FINAL_REPORT.md` preserved
**Previous:** `AUDIT_OCM_orderbook-resync-snapshot-audit_2026-09-20.md` `PARTIALLY_CONFIRMED` + `validation-phase2.md` `PARTIALLY_CONFIRMED` `RECONNECT != RESYNC` + `causality-audit.md` `PARTIALLY_CONFIRMED` `global OHLCV not causal`
**Files inspected:** `cryptofeed_orderbook_stream.py:246` `orderbook_producer.py:129` `book_builder.py:218` `book_builder_consumer.py:284` `shared/kafka/schemas/orderbook.py:169` `gap_recovery_fetcher.py:6` `ohlcv_fetcher.py` `ccxt_adapter.py:692` `streaming_hydra.py:226` `config/market_data/feeds.yaml` `deploy/systemd/ocm-streaming.service.template:11` `tests/market_data/application/processing/test_book_builder.py:104` `application/consumers/quality_consumer.py:80`

## 1. Gap — Operational Defect?

**Condition:** `BookBuilder.on_delta:218` `u != last+1` `GAP_DETECTED` `dE` `book_builder.py:228`.

**State invalid:** `reset()` `has_snapshot=False` `bids/asks` cleared.

**Receiver:** `BookBuilderConsumer._apply_outcome:284` `GAP_DETECTED → warning`.

**Action:** `handled` not `written` `book.*` silent until next `snapshot`.

**Is it operational defect?** **PARTIAL** — `gap` detection `YES` prevents corrupt book, but `STALE` window `2s` `check_stale:272` until next `WS snapshot` is defect if `gap` without `disconnect` (no `snapshot` trigger). No `REST` snapshot → `book.delta` gap not recovered deterministically.

## 2. Data Loss / Inconsistency

**Can it produce loss?** **YES** `book.delta` `104,105` discarded `DELTA_BEFORE_SNAPSHOT` `book_builder.py:207` until `snapshot 200` → `book.delta` missing `STALE` `2s`+`WS` reconnect latency `~1s`. `orderbook.raw` **no loss** (all `u` persisted `Kafka` `1h`), `book.delta` **loss** (in-memory `BookBuilder`).

**Inconsistency?** **YES** `book.delta` `STALE` `bids/asks` empty until `snapshot`, downstream `microprice` `book.delta` would be `stale` not corrupt (fail-safe `reset` prevents corrupt).

## 3. Global Dataset Incompleteness Causality

**Separate flows:**

```
OHLCV: REST CCXT fetch_ohlcv (500, since) → Kafka ohlcv.raw → Bronze Iceberg (deduplication) → Quality gap_scanner → Silver/Gold (reconciliation) — GapRecoveryFetcher trades only, OHLCV gap via historical backfill, not OrderBook
OrderBook: WS snapshot+delta (u) → Kafka orderbook.raw → BookBuilder → book.* → Kafka (no Bronze) — no REST recovery
```

**What dataset has gaps?**

- **OHLCV completeness:** `Quality` `gap_scanner.py:16` `scan_gaps` `2× timeframe` → **no** `gap → REST` automatic `OHLCV` (only `historical` `incremental` `fetch_ohlcv` independent). `OHLCV` gap remains `UNKNOWN` causality, not `OrderBook`.
- **OrderBook completeness:** `book.delta` `STALE` until `snapshot` → `Silver` `orderbook` `UNKNOWN` (no `Silver` `orderbook` consumer).
- **Kafka completeness:** `orderbook.raw` **complete** (all `u`).
- **Bronze completeness:** `orderbook` **NO Bronze** (only `ohlcv.raw` → Bronze).
- **First missing:** `BookBuilder` `GAP` `106` not `book.delta`, but `orderbook.raw` `106` exists.

**Causality:** `OrderBook resync missing` **→** `OrderBook` `book.delta` `STALE` **PARTIAL**, **NOT** `global OHLCV` `Bronze/Silver/Gold` (separate `REST` `fetch_ohlcv`).

## 4. Need for OrderBookSnapshotFetcher REST

**Alternative A `WS reconnect → snapshot`:** Consistency `STALE` `2s` + `1s` reconnect, 0 `REST`, uses `cryptofeed` — **sufficient** if `gap` always coincides with `disconnect` (Bybit `L2_BOOK` `snapshot` on `subscribe`).

**Alternative B `GAP → REST snapshot`:** Consistency deterministic `GAP` immediate `REST` `snapshot` `limit 50` `~100ms` `rate 10/s` — **sufficient** for `gap` without `disconnect` (e.g., `u` jump while `WS` connected).

**Evidence:** `P0` `seq` gaps `9-7593` while `u` `+1` suggests `u` gap can occur **without** `WS disconnect` (Bybit `u` per `symbol` `+1` per `delta`, `seq` cross-symbol). `gap` without `disconnect` → `RECONNECT` not triggered → `STALE` indefinite → **B needed**.

**Conclusion:** `A` **semantically sufficient** for `WS disconnect` gaps, **insufficient** for `u` gap without `disconnect`. `B` **required** for determinism.

## 5. Cryptofeed Reconnect Sufficiency

**Reconnect:** `FeedHandler` `Bybit` `WS` `on_error` `reconnect` **YES**.

**Reconnect → snapshot?** **YES** `Bybit` `L2_BOOK` first message after `subscribe` is `snapshot` `delta None` `cryptofeed_orderbook_stream.py:200`.

**Guarantees:** `snapshot → delta` `u+1` `BookBuilder` `SNAPSHOT_APPLIED` `last=500` `delta 501` `DELTA_APPLIED` — **YES** `Kafka` `partition 0` `FIFO`.

**Sufficiency:** **PARTIAL** — `reconnect` guarantees `snapshot` only after `disconnect`/`reconnect`; `gap` without `disconnect` → **no snapshot** → **insufficient**.

---

## Verdict

**Q1 Defect operational?** **PARTIAL** — `STALE` window, not `corrupt`.
**Q2 Data loss?** **YES** `book.delta` `STALE` (not `orderbook.raw`).
**Q3 Global incompleteness?** **NOT ESTABLISHED** — `OrderBook` `STALE` ≠ `OHLCV` `Bronze`.
**Q4 Requires REST fetcher?** **PARTIALLY_CONFIRMED** — required for `gap` without `disconnect`.
**Q5 Cryptofeed sufficient?** **PARTIALLY — sufficient for `disconnect` gaps, insufficient for `u` jump without `disconnect`.**

**Overall:** **B. PARTIALLY CONFIRMED — recovery exists via `WS` but not deterministic; further evidence `gap` without `disconnect` + `REST` fetcher needed for determinism.**

**PRs:** `PR-A` `OrderBookSnapshotFetcher` `REST` `Bybit` `/v5/market/orderbook`, `PR-B` `GAP → REST` orchestration, `PR-C` already `rebuild`, `PR-D` `gap→snapshot→delta` test, `PR-E` `orderbook.resync_total`.

**AUDIT COMPLETE — Implementation NOT PERFORMED**
