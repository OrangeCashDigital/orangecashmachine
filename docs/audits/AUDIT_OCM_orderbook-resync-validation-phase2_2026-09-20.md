# OB-RESYNC-001 — Validation Phase 2 — OrderBook Sequence Gap → Snapshot Resync Deep Dive

**ID:** OB-RESYNC-001
**Phase:** 2 — Re-validation of initial PARTIALLY_CONFIRMED
**Date:** 2026-09-20
**Auditor:** Senior Market Data / Distributed Systems — READ-ONLY
**Scope:** OrderBook `Bybit` `BTC-USDT-PERP` `BTC/USDT` — `Cryptofeed` → `BookBuilder` → `Kafka` → `Bronze`
**Git state:** `feat/m22b-governance` `857cfe60` `M22-B` `?? 6 audit` + `?? docs/audit` + `?? VERIFICATION_FINAL_REPORT.md` + `?? OB-RESYNC-001` `270` líneas — preserved
**Previous audit:** `docs/audits/AUDIT_OCM_orderbook-resync-snapshot-audit_2026-09-20.md` `2026-09-20` `270` líneas `PARTIALLY_CONFIRMED` `GAP_DETECTED CONFIRMED` `Recovery PARTIAL` `RECONNECT != RESYNC`
**Files inspected Phase2:** `packages/market_data/adapters/inbound/websocket/gap_aware_stream.py:1`, `cryptofeed_orderbook_stream.py:246`, `orderbook_producer.py:129`, `book_builder.py:218`, `book_builder_consumer.py:284`, `gap_recovery_fetcher.py:1`, `shared/kafka/schemas/orderbook.py:169`, `tests/market_data/application/processing/test_book_builder.py:104`, `apps/app/cli/streaming_hydra.py:226`, `config/market_data/feeds.yaml`, `deploy/systemd/ocm-streaming.service.template:11`

## 1. Central Question

After a `sequence gap` (`u != last+1`), does OCM **deterministically** reconstruct the `OrderBook` via a valid `snapshot` and establish a new `sequence baseline` before accepting new `deltas`?

**Classification:** **PARTIALLY_CONFIRMED** — ** gap detection and rebuild on next passive snapshot `YES`, explicit `GAP → REST snapshot` active resync `NO`**. This validates previous `PARTIALLY_CONFIRMED`.

## 2. Call Chain Complete

```
Exchange Bybit WS (msg.data.u, seq, cts, ts, b/a)
  ↓ cryptofeed FeedHandler L2_BOOK (cryptofeed_orderbook_stream.py:123 FeedHandler.add_feed Bybit(symbols, channels=[L2_BOOK], callbacks={L2_BOOK: _translate_and_dispatch}))
CryptofeedOrderBookStream._translate_and_dispatch (book.symbol, book.delta, book.raw, book.timestamp, book.checksum, book.sequence_number) :152
  ↓ _bybit_sequence(raw) → (u, seq, cts) :246 raw.get('data').get('u')
  ↓ if delta is None → on_snapshot(bids,asks,update_id=u) :207
  ↓ else → on_delta(bids,asks,update_id=u) :232
OrderBookKafkaProducer.on_snapshot/on_delta (update_id) → OrderBookSnapshotPayload/DeltaPayload (update_id) → Kafka orderbook.raw (key=bybit:BTC/USDT, partition 0) orderbook_producer.py:159
  ↓ Kafka orderbook.raw (schema v2, retention 1h)
BookBuilderConsumer.poll → _load_payload_type → deserialize → BookBuilder (application pure) :198
  ↓ on_snapshot → SNAPSHOT_APPLIED → publish book.snapshot
  ↓ on_delta → GAP_DETECTED if u != last+1 → reset + warning (no snapshot request) → wait → WS reconnect → new snapshot → SNAPSHOT_APPLIED → last_update_id = snapshot.u → delta u+1 → DELTA_APPLIED
```

**Per transition file:line:** see above.

**No `snapshot` param for `Cryptofeed` — `L2_BOOK` channel `cryptofeed` does not support `onSnapshot` callback; snapshot is `delta=None` event from `FeedHandler`, not requested.**

## 3. Reconnect Deep Dive

**1. Who initiates reconnect?** `Cryptofeed` `FeedHandler` `Bybit` `WebSocket` `on_error` `feedhandler.py` (vendor) — **Cryptofeed** initiates reconnect on `WS disconnect`/`error`, not `BookBuilder`.

**2. Cryptofeed does it?** **YES** — `FeedHandler.run` `install_signal_handlers=False` but `reconnect` `True` (default `cryptofeed` `reconnect`).

**3. OCM does it?** **NO** — `CryptofeedOrderBookStream.start:137` `run(start_loop=False)` no `reconnect` logic; `BookBuilder` no `reconnect`.

**4. Callback?** **NO** — `BookBuilder` `GAP_DETECTED` does not trigger `WebSocket` callback; `BookBuilderConsumer` `GAP_DETECTED` only `warning` `:290` no `reconnect`.

**5. Event?** **NO** explicit `GAP → reconnect` event — `GAP_DETECTED` `OutcomeKind` `book_builder.py:39` not propagated to `stream`.

**6. BookBuilder receives reconnect?** **NO** — `BookBuilder` is `Kafka` downstream, not `WS` upstream.

**7. State invalidated?** **YES** `BookBuilder.on_delta:220` `state.reset()` `has_snapshot=False`.

**8. Snapshot auto?** **YES** passively — `WS reconnect` → `Cryptofeed` emits new `snapshot` (`delta None`) → `on_snapshot`.

**9. Snapshot always after reconnect?** **YES** per `cryptofeed` `Bybit` `L2_BOOK` `snapshot` on `subscribe` (first message after `subscribe` is `snapshot`).

**10. Who guarantees `snapshot → delta` order?** `Kafka` `make_symbol_key` `partition 0` `FIFO` `orderbook_producer.py:23` + `BookBuilderConsumer` `poll` `max_records 500` `sequential` `:160`.

**11. Sequence baseline reset?** **YES** `BookBuilder.on_snapshot:168` `last_update_id = snapshot.update_id`.

**12. Window deltas incorrectly accepted?** **NO** — `DELTA_BEFORE_SNAPSHOT` `book_builder.py:207` discards deltas `u` until `snapshot` `has_snapshot True`.

**Call chain reconnect:** `WS disconnect → cryptofeed FeedHandler reconnect → Bybit resubscribe L2_BOOK → book.delta None → on_snapshot → Kafka → BookBuilderConsumer → SNAPSHOT_APPLIED`.

## 4. Snapshot — Case A/B/C

**Case A `GAP → OCM request_snapshot() → snapshot`:** **NO** — no `OrderBookSnapshotFetcher`, no `REST` orderbook snapshot in OCM (`GapRecoveryFetcher` only `trades` `gap_recovery_fetcher.py:6`).

**Case B `GAP → disconnect → reconnect → Cryptofeed → snapshot`:** **YES** — observed `snapshot` `delta None` via `WS reconnect`.

**Case C other:** **NO**.

**Determination:** **Case B** — snapshot via `WS reconnect`, not `REST` request.

## 5. RECONNECT != RESYNC

**Reconnect** recovers `transport` (`WS` `FeedHandler` `reconnect`).

**Resync** must recover `semantic state` (`OrderBook` `last_update_id` baseline + `bids/asks` `Decimal`).

**OCM guarantees:** `RECONNECT` **YES** (`cryptofeed`), `RESYNC` **PARTIAL** — semantic `BookBuilder.reset()` + `on_snapshot` `SNAPSHOT_APPLIED` guarantees `snapshot→delta` continuity `u+1`, but **no** `REST` `snapshot` on `GAP_DETECTED` without `WS` disconnect (e.g., `103` gap while `WS` still `connected` → `GAP_DETECTED` → `reset` → `DELTA_BEFORE_SNAPSHOT` discards `104,105` until next `WS snapshot` — **window `STALE` 2s** `check_stale:272`).

## 6. Sequence Baseline

**Snapshot `500`:** `on_snapshot:168` `last_update_id=500` `has_snapshot=True` old `bids/asks` cleared.

**Deltas `501,502,503`:** `on_delta:218` `501==500+1` `DELTA_APPLIED` `last=501`, `502==501+1` `DELTA_APPLIED`, `503` `DELTA_APPLIED`.

**New snapshot `800`:** `on_snapshot:142` `reset()` `last_update_id=800` **previous `503` eliminated**, `bids/asks` rebuilt from `snapshot` `800`.

**Guarantee transition `500→800`:** **YES** `snapshot` `reset` ensures `800` replaces `503`, next `801==800+1` accepted.

## 7. Gap Scenario `100,101,102,103,104` + `105` lost `106` received

- `100` `SNAPSHOT_APPLIED` `last=100`
- `101` `DELTA_APPLIED` `last=101`
- `102` `DELTA_APPLIED` `last=102`
- `103` `DELTA_APPLIED` `last=103`
- `104` `DELTA_APPLIED` `last=104`
- `106` `106 != 104+1=105` → `GAP_DETECTED` `dE=1` `book_builder.py:228` `detail gap dE=1` → `reset` `has_snapshot=False` → **not published** `handled` `book_builder_consumer.py:284` `warning` → state `WAITING_FOR_SNAPSHOT` (implicit `has_snapshot=False`) → next `107` `DELTA_BEFORE_SNAPSHOT` discarded.

**Next `snapshot` `200` → `SNAPSHOT_APPLIED` `last=200` → `201` `DELTA_APPLIED` → `READY/SYNCED`.

## 8. Delta During RESYNC

**GAP → RESYNC (reset) → snapshot request (none) → delta A (106) → delta B (107) → snapshot (200):**

- `A 106` `GAP_DETECTED` `reset` (first gap delta itself triggers gap, not `A` during resync)
- `B 107` **after** `GAP` `DELTA_BEFORE_SNAPSHOT` **discarded** `book_builder.py:207`
- **No buffering, no reordering** — `A/B` **discarded**, **snapshot** `200` rebuilds, **resume** `201`.

**Critical:** No false `resync` — discards during `WAITING`.

## 9. REST Snapshot

**Component:** `MISSING` — no `OrderBookSnapshotFetcher` (only `GapRecoveryFetcher` `trades` and `TradesBackfillFetcher`).

**If exists:** would be `adapters/inbound/rest/orderbook_snapshot_fetcher.py` `fetch_order_book(symbol, limit=50)` `Bybit /v5/market/orderbook` `category=linear` `symbol=BTCUSDT`.

**Connected?** **NO**.
**Executed?** **NO**.
**Persisted?** **NO**.
**Validated?** **NO**.
**Operational?** **NO**.

## 10. Exchange Contract

- **REST snapshot endpoint:** Bybit `GET /v5/market/orderbook` `category=linear` `symbol=BTCUSDT` `limit=50` — **not used**.
- **WS sequence:** `u` `+1` `D-7b` `P0` `seq` gaps `9-7593` `schemas/orderbook.py:174`.
- **Symbol:** `REST` `BTC/USDT` `CCXT` `BTC/USDT` vs `Cryptofeed` `BTC-USDT-PERP` `bybit_cryptofeed_runner.py` — `CryptofeedOrderBookStream:85` `symbols=["BTC-USDT-PERP"]` `market_data` `symbol: "BTC/USDT"` normalized `make_symbol_key` `bybit:BTC/USDT` — risk `PERP` vs `SPOT` during `REST` resync **UNKNOWN** (no `canoncial symbol` `UNKNOWN`).

## 11. Tests

| Test | Exists | Proves recovery? |
|---|---|---|
| `test_book_builder.py:104` `test_gap_detected_and_invalidates` `100→105 dE=4` | **YES** | **NO** (only `GAP_DETECTED`, not `snapshot→delta`) |
| `test_book_builder.py:55` `test_snapshot_resets` | **YES** | **NO** |
| `test_cryptofeed_orderbook_stream.py` | **NO** gap | **NO** |
| `snapshot→delta→gap→recovery→snapshot→next delta` | **NO** `TEST COVERAGE: MISSING` | **NO** |

**Missing:** `gap → snapshot → delta 201 accepted` chain.

## 12. Metrics

- **Exists:** `orderbook_producer` `KafkaMetrics` `event_published/failed` `orderbook_producer.py:179` + `BookBuilderConsumer` `event_failed reason=gap_detected/stale` `:285`.
- **Not exists:** `orderbook.resync_total` (proposal `OB-RESYNC-001`).
- **Where:** `BookBuilderConsumer._apply_outcome` `GAP_DETECTED` `increment counter` `orderbook.resync_total` `labels: exchange, symbol` (convención `ocm_kafka_events_*`).
- **What:** `resync exitosos` (`SNAPSHOT_APPLIED` after `GAP`), not attempts.
- **Labels:** `exchange`, `symbol`, `reason=gap`.
- **Logs:** `warning "book_builder_invalidated | gap"` `book_builder_consumer.py:290` correlacionable.

## 13. Failure Modes

- **Snapshot REST timeout/HTTP 500/rate limit:** **NO handling** (no fetcher) — `gap` remains `WAITING` until `WS snapshot`.
- **Reconnect timeout repeated disconnect:** `FeedHandler` `reconnect` loop, `BookBuilder` stays `WAITING` `STALE` after `2000ms` `check_stale:272` `STALE` outcome.
- **Snapshot corrupto `qty<0`:** `on_snapshot:155` `continue` invalid level, not `GAP`; `on_delta` `STRUCTURAL_INVALID` `reset`.
- **Deltas during resync:** `DELTA_BEFORE_SNAPSHOT` discarded.
- **Multiple gaps `gap→resync→gap`:** `reset` idempotent `GAP_DETECTED` `dE` each gap.

## 14. Kafka/Bronze

- **GAP_DETECTED:** **not published** `book_builder_consumer.py:284` `handled` no `written`.
- **Snapshot:** `book.snapshot` published `SNAPSHOT_APPLIED` `book_builder_consumer.py:242` `TOPIC_BOOK_SNAPSHOT`.
- **Sequence:** conserved `orderbook.raw` `update_id` and `book.*` `update_id`.
- **Bronze:** `orderbook` **NO Bronze** (only `ohlcv.raw` → Bronze `Iceberg`); `orderbook.raw` `1h` retention, `book.*` retention `UNKNOWN`.
- **Live vs Dataset:** `LIVE STATE RECOVERY` (in-memory `BookBuilder`) **PARTIAL** via `WS snapshot`, **DATASET RECOVERY** **NO** (no `Bronze` `orderbook`).

## 15. Dataset Completeness Causality

**OrderBook state recovery** `PARTIAL` via `WS` — **NOT** `global market-data completeness` (`OHLCV` `trades` `funding` `ois` have own `GapRecoveryFetcher` `trades`).

**Causality:** `PARTIAL` — `OrderBook` gap `→` `book.delta` missing until `snapshot` → `Silver/Gold` `orderbook` incomplete `STALE` window, but `OHLCV` `trades` completeness via `REST` `GapRecoveryFetcher` independent.

## 16. Matrix

| Capacidad | Implemented | Connected | Executed | Persisted | Validated | Operational |
|---|---|---|---|---|---|---|
| WS connection | YES | YES | YES | N/A | YES | UNKNOWN |
| Snapshot inicial | YES | YES | YES | YES | YES | YES |
| Sequence tracking (u) | YES | YES | YES | YES | YES (D-7b) | YES |
| Gap detection (u+1) | YES | YES | YES | N/A | YES (test) | YES |
| Book reset | YES | YES | YES | N/A | YES | YES |
| WS reconnect | YES (cryptofeed) | YES | YES | N/A | YES | UNKNOWN |
| Snapshot after reconnect | YES (cryptofeed) | YES | YES | YES | YES | YES |
| Explicit snapshot request | **MISSING** | **NO** | **NO** | **NO** | **NO** | **NO** |
| REST snapshot | **MISSING** | **NO** | **NO** | **NO** | **NO** | **NO** |
| Sequence baseline reset | YES | YES | YES | YES | YES | YES |
| Book rebuild | YES | YES | YES | N/A | YES | YES |
| Delta buffering | **MISSING** (discard) | **NO** | **NO** | **NO** | **NO** | **NO** |
| Delta replay | **NO** | **NO** | **NO** | **NO** | **NO** | **NO** |
| Validated resume | YES (after snapshot) | YES | YES | N/A | YES | YES |
| Kafka orderbook.raw | YES | YES | YES | YES | YES | YES |
| Kafka book.* | YES | YES | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN |
| Bronze orderbook | **MISSING** | **NO** | **NO** | **NO** | **NO** | **NO** |
| Tests gap→snapshot→delta | **MISSING** | **NO** | **NO** | **NO** | **NO** | **NO** |
| Metrics resync_total | **MISSING** | **NO** | **NO** | **NO** | **NO** | **NO** |
| Runtime BookBuilderConsumer | YES | **UNKNOWN** | **UNKNOWN** | **NO** | **NO** | **UNKNOWN** |

## 17. Veredicto

**Previous verdict PARTIALLY_CONFIRMED MAINTAINED** — `GAP_DETECTED` `CONFIRMED`, `Recovery` `PARTIAL` (passive `WS snapshot` `YES`, active `REST snapshot` `NO`).

**New verdict: PARTIALLY_CONFIRMED** (no change, deeper evidence: `WS reconnect → snapshot` `YES`, `GAP → REST snapshot` `NO`).

## 18. Corrección

**Hipótesis `GAP → invalidate/pause → request snapshot → REST → validate → baseline → buffered deltas → resume` es compatible** (`BookBuilder` `reset` + `on_snapshot` `baseline` + `orderbook_producer` `update_id` + `Kafka` `partition 0`).

**Alternativas:**

- **A WS reconnect → snapshot:** Ventaja `cero REST`, usa `cryptofeed` existente; Riesgo `gap` sin `disconnect` no `reconnect` → `STALE` indefinido; Dependencia `cryptofeed` reconnect garantía.
- **B GAP → REST snapshot → rebuild → resume:** Ventaja `determinista` `gap` sin `disconnect` → `snapshot` inmediato; Riesgo `REST` `rate limit` `50` `Bybit`; Dependencia `OrderBookSnapshotFetcher`.
- **C GAP → controlled reconnect → snapshot:** Ventaja `fuerza reconnect` + `snapshot`; Riesgo `WS` storm; Dependencia `FeedHandler` `stop_async`.

**Recomendada:** **B** (`REST` `snapshot` + `A` fallback `WS` `snapshot`).

## 19. PR Split

- **PR-A:** `OrderBookSnapshotFetcher` `adapters/inbound/rest/orderbook_snapshot_fetcher.py` (no deps)
- **PR-B:** `Gap → snapshot` orchestration `BookBuilderConsumer` `GAP_DETECTED → fetch` (depende `PR-A`)
- **PR-C:** `Book rebuild / resume` ya `BookBuilder` (no PR)
- **PR-D:** `tests` `gap→snapshot→delta` `test_book_builder_resync.py` (depende `PR-B`)
- **PR-E:** `observability` `orderbook.resync_total` (depende `PR-B`)
- **PR-F:** `documentation` `schemas/orderbook.py D-7b` (independiente)

Mínimo `2` PRs: `PR-A` fetcher + `PR-B` orchestration.

## 20. Documentación

**Nuevo documento:** `docs/audits/AUDIT_OCM_orderbook-resync-validation-phase2_2026-09-20.md` (este) — referencia `docs/audits/AUDIT_OCM_orderbook-resync-snapshot-audit_2026-09-20.md` `2026-09-20` `270` líneas, profundiza `reconnect` vs `resync` `9-16`.

---

**AUDIT COMPLETE**
**Finding: OB-RESYNC-001**
**Previous verdict: PARTIALLY_CONFIRMED**
**New verdict: PARTIALLY_CONFIRMED**
**Gap detection: CONFIRMED (u != last+1 → reset)**
**Reconnect: CONFIRMED (cryptofeed)**
**Snapshot: PARTIALLY_CONFIRMED (WS passive YES, REST MISSING)**
**Sequence baseline: CONFIRMED (on_snapshot last_update_id)**
**Explicit resync: NOT_CONFIRMED (no REST request)**
**REST recovery: MISSING**
**Book rebuild: CONFIRMED (on_snapshot reset)**
**Validated resume: CONFIRMED (after snapshot)**
**Dataset completeness causality: PARTIAL (OrderBook state, not global OHLCV)**
**Audit document: docs/audits/AUDIT_OCM_orderbook-resync-validation-phase2_2026-09-20.md**
**Implementation: NOT PERFORMED**
**Commit: NOT PERFORMED**
**Push: NOT PERFORMED**
**Merge: NOT PERFORMED**
