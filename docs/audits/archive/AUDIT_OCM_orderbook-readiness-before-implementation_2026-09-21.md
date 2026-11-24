# AUDIT — OrderBook Readiness Before Implementation — PR-A/PR-B Gate

**ID:** AUDIT-READINESS-OB-2026-09-21
**Date:** 2026-09-21
**Scope:** OrderBook `Bybit` `BTC-USDT-PERP` `u` `snapshot` `delta` `gap` `reconnect` `resync` + `OHLCV` `historical/live` `backfill` `repair` `Bronze/Silver/Gold` + `Nautilus`/`Hummingbot`/`Freqtrade` `docs/*.zip` + `Kafka` `Bronze` `Iceberg`
**Git:** `feat/m22b-governance` `857cfe60` `M22-B` `?? 6 audit + 4 OB-RESYNC + 2 AUDIT_OCM_comparative` preserved
**Previous:** `OB-RESYNC-001` `PARTIALLY_CONFIRMED` `RECONNECT != RESYNC` + `comparative` `333` `REST+WS` pattern `OBSERVED APPLICABLE` + `causality-audit` `PARTIAL` (not global OHLCV)
**Files:** `cryptofeed_orderbook_stream.py:246` `orderbook_producer.py:129` `book_builder.py:218` `shared/kafka/schemas/orderbook.py:169` `gap_recovery_fetcher.py:6` `ohlcv_fetcher.py` `streaming_hydra.py:226` `config/market_data/feeds.yaml` `deploy/systemd/ocm-streaming.service.template:11` `test_book_builder.py:104`

## 1. Executive Summary

**Question:** `PR-A OrderBookSnapshotFetcher REST Bybit` + `PR-B Gap → REST Snapshot → Rebuild → Resume` (`PR-A → PR-B`) sufficiently grounded to become implementation plan?

**Answer:** **BLOCKED BY ARCHITECTURAL UNKNOWN — PR-A READY WITH EXPLICIT CONTRACTS, PR-B BLOCKED** — `PR-A` `REST` `snapshot` `Bybit` can be implemented **now** as **independent** `REST` `snapshot` capability (`limit 50` `u` `bids/asks`) with **explicit contracts** (see §9), but `PR-B` `resync` (`GAP → REST → rebuild → resume` + `race` `snapshot ↔ WS`) **requires** `canonical OrderBook contract` (`§8`), `dataset completeness` (`§5`), `multi-exchange` `sequence` `semantics` (`§6`) **before** `PR-B`.

**Reason:** `Dataset completeness: NOT DEMONSTRATED` + `Exchange unknowns` `Bybit` `u` vs `seq` `UNKNOWN` `future exchanges` `sequence` `UNKNOWN` + `Live recovery` vs `Historical dataset repair` conflation risk. `PR-A` without `PR-B` is safe (fetcher alone), `PR-B` without `canonical contract` risks `God Adapter`.

## 2. Evidence Reviewed

- `AUDIT_OCM_orderbook-resync-snapshot-audit_2026-09-20.md` `270` `GAP_DETECTED` `CONFIRMED`
- `AUDIT_OCM_orderbook-resync-validation-phase2_2026-09-20.md` `252` `RECONNECT != RESYNC`
- `AUDIT_OCM_orderbook-resync-causality-audit_2026-09-21.md` `161` `PARTIAL` `WS` `snapshot` passive
- `AUDIT_OCM_orderbook-resync-operational-sufficiency-audit_2026-09-21.md` `83` `WS` sufficient for `disconnect`
- `AUDIT_OCM_comparative-architecture-refs_2026-09-21.md` `333` `Hummingbot` `20582` `Nautilus` `http_get_orderbook.json`
- `docs/nautilus_trader-develop.zip` `28M` `http_get_orderbook.json` `397` `ws_orderbook_snapshot/delta` `274/221`
- `docs/hummingbot-master.zip` `3,7M` `bybit_perpetual_api_order_book_data_source.py` `20582` `get_order_book_snapshot`
- `docs/freqtrade-develop.zip` `44M` `trades-to-ohlcv.md` `OHLCV` `REST` `historical`

## 3. Current Architecture

```
Exchange Bybit WS L2_BOOK BTC-USDT-PERP
  ↓ CryptofeedOrderBookStream._translate_and_dispatch (book.delta None → snapshot, else delta) :200
  ↓ _bybit_sequence(raw) → (u, seq, cts) :246
  ↓ OrderBookKafkaProducer.on_snapshot/on_delta (update_id=u) → Kafka orderbook.raw (key=bybit:BTC/USDT) :159
  ↓ Kafka orderbook.raw (schema v2, 1h, partition 0 FIFO make_symbol_key)
  ↓ BookBuilderConsumer.poll → BookBuilder (pure) :198
  ↓ on_snapshot → SNAPSHOT_APPLIED → book.snapshot → Kafka book.snapshot
  ↓ on_delta → GAP_DETECTED (u != last+1 → reset) → handled (warning) → wait WS snapshot → rebuild
  ↓ book.delta → Kafka book.delta (no Bronze orderbook)
```

**No `REST` `OrderBookSnapshotFetcher`**, **no `Buffer` `WS` during `GAP`**, **no `Bronze` `orderbook`**.

## 4. Dataset State

| Propiedad | Estado | Evidencia |
|---|---|---|
| `snapshots disponibles` | **PARTIAL** `WS` `snapshot` `delta None` `YES` `REST` `snapshot` `NO` | `cryptofeed_orderbook_stream.py:200` `snapshot` `book.book.to_dict()` |
| `deltas disponibles` | **CONFIRMED** `WS` `delta` `b/a` `u` `orderbook.raw` `9943250` | `orderbook_producer.py:238` `update_id` |
| `sequence IDs` | **CONFIRMED** `u` `Bybit` `monótono +1` `D-7b` | `shared/kafka/schemas/orderbook.py:169` `u` |
| `timestamps exchange` | **CONFIRMED** `cts` `ts` `exchange_ts` `receipt_timestamp` `:178` | `cryptofeed_orderbook_stream.py:178` |
| `timestamps ingestion` | **CONFIRMED** `receipt_timestamp` `*1000` | `:195` |
| `profundidad` | **CONFIRMED** `max_depth 50` `CryptofeedOrderBookStream:98` | `:98` |
| `continuidad` | **PARTIAL** `GAP_DETECTED` `u+1` `YES` `STALE` `2s` | `book_builder.py:218` `check_stale:272` |
| `gaps conocidos` | **UNKNOWN** `seq` gaps `9-7593` `P0` `u` `+1` `D-7b` | `schemas/orderbook.py:174` |
| `capacidad reconstrucción` | **PARTIAL** `on_snapshot` `reset` `rebuild` `YES` `REST` `NO` | `book_builder.py:168` |
| `completitud histórica` | **NOT DEMONSTRATED** `orderbook` `NO Bronze` `historical` `orderbook` `NO` | `find Bronze 0 parquet` `health 503` |
| `evidencia recuperación` | **PARTIAL** `WS` `snapshot` passive `YES` `REST` `NO` | `book_builder_consumer.py:284` |

**DATASET COMPLETENESS: NOT DEMONSTRATED** — no `Bronze` `orderbook` `historical` `completitud` `UNKNOWN` (no `parquet` `orderbook`).

## 5. Dataset Completeness

`Historical` `OHLCV` `REST` `fetch_ohlcv` `paginated` `500` `since` `Bronze` `Iceberg` `deduplication` `YES` — `live` `OrderBook` `WS` `live` `YES` `gap` `PARTIAL` `STALE` — **not demonstrated** `orderbook` `historical` `completeness` `UNKNOWN`.

## 6. Exchange Unknowns

| Campo | Bybit | Future exchange | Estado |
|---|---|---|---|
| `exchange` | `bybit` | `binance, okx, ...` | **OCM UNIVERSAL** `SupportedExchange` `schema.py:61` |
| `symbol` | `BTC-USDT-PERP` `Cryptofeed` `BTC/USDT` `Kafka` `BTC/USDT` `bybit:BTC/USDT` | `BTC/USDT` `spot` `BTCUSDT` `PERP` | **EXCHANGE-SPECIFIC** `market_type` `linear` vs `spot` |
| `market type` | `linear` `perpetual` `category=linear` `symbol=BTCUSDT` | `spot` `inverse` | **EXCHANGE-SPECIFIC** `linear` `REST` `category` |
| `timestamp exchange` `cts` | `cts` `ms` `:178` | `ts` `ms` `other` | **EXCHANGE-SPECIFIC** |
| `timestamp ingestion` `receipt` | `receipt_timestamp *1000` `:195` | same | **OCM UNIVERSAL** |
| `sequence` `u` | `u` `+1` `monótono` `per symbol` `linear` | `updateId` `U` `binance` `pu` | **EXCHANGE-SPECIFIC** `u` `vs` `U` `vs` `pu` |
| `first update ID` | `snapshot` `u` | `snapshot` `U` | **OCM UNIVERSAL** `snapshot.update_id` |
| `last update ID` | `last_update_id` `book_builder.py:86` | `lastUpdateId` | **OCM UNIVERSAL** |
| `update range` | `snapshot` `u` `→` `delta` `u+1` | `snapshot` `U` `→` `delta` `U+1` | **OCM UNIVERSAL** `u+1` |
| `bids/asks` | `[[price,qty]]` `str` `Decimal` `D-7c` | same | **OCM UNIVERSAL** `PriceLevel` `str` |
| `depth` | `50` `max_depth` | `10,20,50` | **EXCHANGE-SPECIFIC** `depth` param |
| `checksum` | `book.checksum` `None` `Bybit` | `checksum` `binance` | **OPTIONAL** |
| `snapshot/delta type` | `payload_type snapshot/delta` `v2` | same | **OCM UNIVERSAL** |
| `source` | `DATASOURCE_LIVE` `DATASOURCE_REPLAY` | same | **OCM UNIVERSAL** |

**Unknown:** `Bybit` `REST` `snapshot` `u` exact `json` `u` field `http_get_orderbook.json` `397` not extracted `UNKNOWN` `reference evidence insufficient` — need `Bybit` `REST` `u` `seq` `cts` mapping `UNKNOWN`.

## 7. Canonical OrderBook Contract

**Exchange `Bybit` `PERP` `linear` `BTCUSDT` `50`:**

```json
// REST GET /v5/market/orderbook
{
  "result": {"s": "BTCUSDT", "b": [["30250.00","0.5"]], "a": [["30300.00","0.7"]], "ts": 1700000000000, "u": 100 },
  "retCode": 0
}
// WS delta
{ "topic": "orderBook", "type": "delta", "ts": 1700000000000, "cts": 1700000000000, "data": {"s": "BTCUSDT", "b": [["30250.00","0.5"]], "a": [], "u": 101, "seq": 123 } }
```

**Canonical `OCM` `OrderBookSnapshotPayload` `OrderBookDeltaPayload` `shared/kafka/schemas/orderbook.py:95,196` `exchange` `symbol` `BTC/USDT` `timestamp_ms` `update_id` `u` `cross_seq` `seq` `cts_ms` `bids/asks` `depth` `checksum` `SCHEMA_VERSION 2` — **OCM UNIVERSAL** `exchange`, `symbol` `BTC/USDT` normalized `make_symbol_key`, `timestamp_ms` `ingestion`, `update_id` `u` `OCM UNIVERSAL`, `cross_seq` `seq` `EXCHANGE-SPECIFIC` `cts_ms` `EXCHANGE-SPECIFIC`.

**Contract minimal `PR-A`:** `fetch_snapshot(exchange, symbol, limit=50) -> OrderBookSnapshotPayload` `exchange` `bybit` `symbol` `BTC/USDT` `limit` `50` `bids/asks` `update_id` `timestamp_ms` `depth` `checksum` — **must** `update_id` `u` `int` `>0`, `bids/asks` `List[PriceLevel]` `str`, `timestamp_ms` `int`; **optional** `cross_seq` `cts_ms` `checksum`.

**Contract `PR-A` must NOT assume:** `u` `universal` `+1` for all exchanges (Bybit `u` `+1` `D-7b` `P0` `seq` gaps, Binance `U` `+1` but `pu` `lastUpdateId`).

## 8. Exchange Adapter Boundary

```
OCM DOMAIN / APPLICATION
  OrderBook domain contract (OrderBookSnapshotPayload/DeltaPayload, BookBuilder, sequence u+1, gap, reset, rebuild, validated resume)
  synchronization semantics (snapshot baseline, delta mutation, sequence ordering, recovery)
  recovery state (has_snapshot, last_update_id, STALE)
        │
        ├── Bybit adapter
        │     ├── REST snapshot (GET /v5/market/orderbook, category=linear, symbol=BTCUSDT, limit 50, parse b/a/u/ts/cts) — PR-A
        │     ├── WS deltas (L2_BOOK BTC-USDT-PERP, Cryptofeed, u/seq/cts) — existing
        │     └── sequence mapping (u → update_id, seq → cross_seq, cts → cts_ms)
        │
        └── Future exchange adapter (Binance, OKX)
              ├── REST snapshot (GET /api/v3/depth, symbol=BTCUSDT, limit 500, U, pu)
              ├── WS deltas (depthUpdate, U, pu)
              └── exchange-specific semantics (U, pu, lastUpdateId)
```

**Justified:** `OCM` `market_data` `adapters/inbound/websocket` `Bybit` `Cryptofeed` + `adapters/inbound/rest` `Bybit` `REST` separate, `BookBuilder` `pure` `Decimal` `domain`.

## 9. PR-A Assessment

**Can independent?** **YES** — `Hummingbot` `get_order_book_snapshot` `independent` `REST` `snapshot` used `initial` `baseline` + `gap` `recovery`, `Nautilus` `http_get_orderbook` `independent`.

**Can exist without resync?** **YES** — `fetcher` `REST` `snapshot` `→` `OrderBookSnapshotPayload` used for `initial` `baseline` test, `recovery` later, no `GAP` logic.

**Minimal contract:** `fetch_snapshot(exchange: str, symbol: str, limit: int = 50) -> OrderBookSnapshotPayload` `Bybit` `category=linear` `symbol=BTCUSDT` `bids/asks` `PriceLevel` `str` `update_id` `int` `timestamp_ms` `int`.

**Bybit specific?** `REST` `endpoint` `/v5/market/orderbook` `category` `limit` `symbol` `BTCUSDT` **Bybit-specific** `exchange` `bybit` `adapter` `inbound/rest/bybit_orderbook_snapshot_fetcher.py`.

**Abstract OCM?** `OrderBookSnapshotPayload` `exchange` `symbol` `update_id` `bids/asks` **OCM universal** `shared/kafka/schemas/orderbook.py`.

**Information to return:** `exchange`, `symbol`, `timestamp_ms`, `update_id`, `cross_seq`, `cts_ms`, `bids`, `asks`, `depth`, `checksum` — **must** `update_id` `bids/asks`, **not** assume `cross_seq` `seq` universal.

**Information NOT to assume:** `u` `universal` `+1` for all exchanges; `checksum` `optional`.

**Contractual tests:** `snapshot parser` `Bybit` `b/a` `u` `Decimal` `snapshot completeness` `200` `bids/asks` `update_id` `>0` `error` `qty<0`, `sequence` `u+1` `cross_seq` `cts`.

**Future blocking if badly designed?** **YES** if `Fetcher` returns `raw` `Bybit` `json` not `OrderBookSnapshotPayload` `OCM` `universal` — would couple `BookBuilder` to `Bybit` `u` semantics.

**Verdict PR-A:** **READY WITH EXPLICIT CONTRACTS** — can be `READY TO IMPLEMENT` with `Bybit` `REST` `snapshot` `contract` `OrderBookSnapshotPayload` `limit 50` `update_id` `u` `bids/asks` **explicit** `exchange` `bybit` `adapter`, **not** `universal`.

## 10. PR-B Assessment

**GAP → REST SNAPSHOT → REBUILD → RESUME exact?** `GAP_DETECTED` `u != last+1` `book_builder.py:218` `reset` `has_snapshot=False` `→` `STALE` `check_stale:272` `→` `REQUEST SNAPSHOT` `OrderBookSnapshotFetcher.fetch_snapshot` `REST` `→` `BUFFER / FILTER WS UPDATES` `DELTA_BEFORE_SNAPSHOT` `discard` `book_builder.py:207` `→` `APPLY SNAPSHOT` `on_snapshot` `last= snapshot.u` `→` `RECONCILE` `discard` `u < snapshot.u` `→` `VALIDATE` `next delta u == last+1` `→` `RESUME` `DELTA_APPLIED`.

**Buffering `snapshot ↔ WS` race `t0 gap → t1 REST request → t2 WS 106,107 → t3 snapshot 200 → t4 WS 201`:**

- `t2` `106,107` `DELTA_BEFORE_SNAPSHOT` **discarded** `book_builder.py:207` (no buffering) — **Hummingbot** `OrderBookTracker` `discard` `u < snapshot.u` same.
- `t3` `snapshot 200` `SNAPSHOT_APPLIED` `last=200` `rebuild`
- `t4` `201` `201==200+1` `DELTA_APPLIED` `resume` — **no loss** `orderbook.raw` `106,107` persisted `Kafka` `1h` (live vs dataset).

**Decision required:** `UNKNOWN` `buffering` `replay` vs `discard` — `Hummingbot` `Nautilus` both `discard` `u < snapshot.u` (snapshot authoritative), `OCM` already `discard` `DELTA_BEFORE_SNAPSHOT` — **keep discard** (no `buffer`).

**PR-B needs:** `GAP_DETECTED` `BookBuilderConsumer` `orchestration` `fetch` `REST` `snapshot` `rebuild` `resume` `validated`.

## 11. Recovery Definition

**Snapshot received ≠ recovered.** `RECOVERED` = `snapshot` `valid` `bids/asks` non-empty `update_id >0` + `sequence baseline` `last_update_id = snapshot.update_id` `has_snapshot=True` + `deltas reconciled` `u < snapshot.u` discarded + `book` `rebuilt` `bids/asks` `Decimal` `→` `continuity` `next delta u == last+1` `DELTA_APPLIED` `→` `resume` `book.delta` published.

## 12. Live Recovery vs Dataset Repair

**Live OrderBook resync:** `BookBuilder` `in-memory` `last_update_id` `reset` `→` `REST` `snapshot` `→` `rebuild` `→` `resume` `WS` `live` `correctness` — **does not modify `Bronze`** `orderbook.raw` (no `Bronze` `orderbook`).

**Historical dataset completeness:** `OHLCV` `REST` `fetch_ohlcv` `historical` `backfill` `GapRecoveryFetcher` `trades` `gap [start,end)` + `Quality` `gap_scanner` `Silver` `Gold` — **separate** `OHLCV` `repair` `Silver` `Gold` `reconciliation` `deduplication` `Bronze` `Iceberg`.

**Resync guarantees live `book.delta` correctness, not historical `orderbook` dataset** `Bronze` `Silver` `Gold` (no `Bronze` `orderbook`).

## 13. Source-of-Truth Matrix

| Información | Source of Truth | Estado |
|---|---|---|
| `Live OrderBook` `bids/asks` `u` | `Exchange` `Bybit` `WS` `u` `bids/asks` + `REST` `snapshot` `u` `bids/asks` authoritative | **CONFIRMED** `Bybit` `u` |
| `REST snapshot` `bids/asks` `u` | `Exchange` `Bybit` `GET /v5/market/orderbook` `u` | **CONFIRMED** `Bybit` |
| `WS delta` `b/a` `u` | `Exchange` `Bybit` `WS` `data.u` | **CONFIRMED** `Bybit` |
| `sequence` `u` `+1` | `Exchange` `Bybit` `u` `monótono` `D-7b` `P0` | **CONFIRMED** `u` |
| `Bronze raw` `orderbook.raw` `u` `bids/asks` | `Kafka` `orderbook.raw` `persisted` `1h` `book.raw` `u` | **CONFIRMED** `orderbook.raw` |
| `Silver` `orderbook` | **NO** `Silver` `orderbook` | **MISSING** (no `Silver` `orderbook` consumer) |
| `Gold` `orderbook` | **NO** `Gold` `orderbook` | **MISSING** |
| `historical dataset` `OHLCV` `Bronze` | `Bronze` `Iceberg` `ohlcv.raw` `deduplication` | **CONFIRMED** `OHLCV` `Bronze` |
| `gap detection` `u != last+1` | `BookBuilder` `GAP_DETECTED` | **CONFIRMED** |
| `recovery state` `has_snapshot` `last_update_id` | `BookBuilder` `in-memory` | **CONFIRMED** |

## 14. Master Capability Matrix

| Capacidad | Implemented | Connected | Executed | Persisted | Validated | Operational | Estado |
|---|---|---|---|---|---|---|---|
| `REST snapshot` `Bybit` | **NO** | **NO** | **NO** | **NO** | **NO** | **NO** | **MISSING** |
| `WS OrderBook` `L2` | **YES** | **YES** | **YES** | **YES** `orderbook.raw` | **YES** | **UNKNOWN** | **PARTIAL** |
| `sequence tracking` `u` | **YES** | **YES** | **YES** | **YES** | **YES** | **YES** | **YES** |
| `gap detection` `u+1` | **YES** | **YES** | **YES** | **N/A** | **YES** | **YES** | **YES** |
| `reconnect` `FeedHandler` | **YES** | **YES** | **YES** | **N/A** | **YES** | **UNKNOWN** | **YES** |
| `resync` `gap→snapshot` | **NO** | **NO** | **NO** | **NO** | **NO** | **NO** | **MISSING** |
| `rebuild` `on_snapshot` | **YES** | **YES** | **YES** | **N/A** | **YES** | **YES** | **YES** |
| `Kafka` `orderbook.raw` | **YES** | **YES** | **YES** | **YES** | **YES** | **YES** | **YES** |
| `Bronze` `orderbook` | **NO** | **NO** | **NO** | **NO** | **NO** | **NO** | **MISSING** |
| `Silver` `orderbook` | **NO** | **NO** | **NO** | **NO** | **NO** | **NO** | **MISSING** |
| `Gold` `orderbook` | **NO** | **NO** | **NO** | **NO** | **NO** | **NO** | **MISSING** |
| `OHLCV backfill` `fetch_ohlcv` | **YES** | **YES** | **YES** | **YES** | **YES** | **UNKNOWN** | **YES** |
| `OHLCV repair` `GapRecoveryFetcher` `trades` | **YES** | **NO** `OHLCV` | **NO** | **NO** | **NO** | **NO** | **PARTIAL** |
| `reconciliation` `Quality` `is_new` | **YES** `OHLCV` `trades` | **YES** | **YES** | **NO** | **YES** | **YES** | **YES** |

## 15. PR Dependency Graph

```
PR-A REST Snapshot capability (Bybit adapter, OrderBookSnapshotPayload, limit 50, u, b/a)
  ↓ (no deps, standalone)
PR-B Resync orchestration (GAP_DETECTED → fetch → rebuild → resume, BookBuilderConsumer, delta discard, validated resume)
  ↓ depends PR-A (Fetcher)
PR-C Book rebuild (already BookBuilder, no PR)
PR-D Integration tests (gap→snapshot→delta, failure modes) depends PR-B
PR-E Observability (orderbook.resync_total) depends PR-B
PR-F Documentation (schemas D-7b) independent
```

## 16. Architectural Unknowns

- `Bybit` `REST` `snapshot` `u` exact `json` `u` `seq` `cts` mapping `http_get_orderbook.json` `397` not extracted `update_id` `UNKNOWN` — need `Bybit` `REST` `u` `seq` `cts` `UNKNOWN`.
- `Future exchanges` `Binance` `U` `pu` `lastUpdateId` `sequence` `semantics` `UNKNOWN` — `Binance` `depth` `U` `pu` `+1` vs `Bybit` `u` `+1` `UNKNOWN` `multi-exchange` `sequence` `UNKNOWN`.
- `Dataset completeness` `orderbook` `historical` `completeness` `NOT DEMONSTRATED` — `find Bronze 0 parquet` `health 503`.
- `Snapshot ↔ WS race` `buffering` `replay` `Hummingbot` `discard` vs `buffer` `UNKNOWN` `OCM` `DELTA_BEFORE_SNAPSHOT` `discard` already, but `REST` `snapshot` `in-flight` `WS` `deltas` `u` `buffer` `UNKNOWN`.

## 17. Implementation Gate

**ARCHITECTURE NOT READY** — `PR-A` `REST` `snapshot` `Bybit` `contract` `OrderBookSnapshotPayload` `limit 50` `update_id` `u` `bids/asks` **READY WITH EXPLICIT CONTRACTS** (can implement `PR-A` standalone), `PR-B` `resync` `GAP → REST → rebuild → resume` `race` `snapshot ↔ WS` `buffering` `reconciliation` `BLOCKED BY ARCHITECTURAL UNKNOWN` (`Bybit` `REST` `snapshot` `u` exact `json`, `future exchanges` `sequence` `UNKNOWN`, `dataset completeness` `NOT DEMONSTRATED`).

**Blockers:**

1. `Canonical OrderBook contract` `exchange` `symbol` `BTC/USDT` `vs` `BTC-USDT-PERP` `market_type` `linear` `UNKNOWN` `multi-exchange` `sequence` `UNKNOWN`.
2. `Bybit` `REST` `snapshot` `u` `seq` `cts` `exact` `json` `UNKNOWN` — need `http_get_orderbook.json` `u` field.
3. `Race` `snapshot ↔ WS` `buffering` `replay` `UNKNOWN` — `Hummingbot` `discard` vs `OCM` `discard` `OK`, but `REST` `snapshot` `in-flight` `WS` `deltas` `u` `buffer` `UNKNOWN`.
4. `Dataset` `orderbook` `historical` `completeness` `NOT DEMONSTRATED` — `find Bronze 0` `health 503`.

## 18. Final Recommendation

**PR-A: READY WITH EXPLICIT CONTRACTS** — implement `OrderBookSnapshotFetcher` `REST` `Bybit` `GET /v5/market/orderbook` `category=linear` `symbol=BTCUSDT` `limit=50` → `OrderBookSnapshotPayload` `exchange` `bybit` `symbol` `BTC/USDT` `update_id` `u` `int` `>0` `bids/asks` `List[PriceLevel]` `str` `timestamp_ms` `int` `depth` `checksum` **exchange-specific** `Bybit` `adapter` `inbound/rest/bybit_orderbook_snapshot_fetcher.py`, **OCM universal** `OrderBookSnapshotPayload` `update_id` `bids/asks`.

**PR-B: BLOCKED BY ARCHITECTURAL UNKNOWN** — `GAP → REST snapshot → rebuild → resume` `race` `snapshot ↔ WS` `buffering` `reconciliation` `UNKNOWN` + `future exchanges` `sequence` `UNKNOWN` + `dataset completeness` `NOT DEMONSTRATED` — **not** `READY TO IMPLEMENT`, need `canonical OrderBook contract` `multi-exchange` `sequence` `UNKNOWN` resolved + `Bybit` `REST` `snapshot` `u` exact `json` + `dataset` `Bronze` `completeness` `demonstrated`.

**PRs:** `PR-A` `fetcher` `MISSING → IMPLEMENTED` `adapters/inbound/rest/orderbook_snapshot_fetcher.py` (no deps) **READY**, `PR-B` `resync orchestration` `MISSING → CONNECTED` (dept `PR-A` + `canonical contract`) **BLOCKED**, `PR-D` `tests` `gap→snapshot→delta` `MISSING` (dept `PR-B`), `PR-E` `metrics` `orderbook.resync_total` `MISSING` (dept `PR-B`).

## 19. Proposed PR Boundaries

- **PR-A** `OrderBookSnapshotFetcher` `REST` `Bybit` `limit 50` `u` `bids/asks` `OrderBookSnapshotPayload` `2` files `adapters/inbound/rest/orderbook_snapshot_fetcher.py` `tests` `snapshot parser` `Bybit` `b/a` `u` `Decimal` `update_id` `>0` `error` `qty<0` — **READY WITH EXPLICIT CONTRACTS** (no `resync`).
- **PR-B** `Gap → REST` `BookBuilderConsumer` `GAP_DETECTED → fetch` `rebuild` `resume` `validated` `race` `snapshot ↔ WS` `buffering` `reconciliation` — **BLOCKED** (needs `canonical contract` + `Bybit` `REST` `u` + `dataset`).
- **PR-F** `Documentation` `schemas/orderbook.py` `D-7b` `u` already `IMPLEMENTED` (no PR).

## 20. Unknowns

- `Bybit` `REST` `snapshot` `u` `exact` `json` `http_get_orderbook.json` `397` `update_id` `u` `UNKNOWN`
- `Future exchanges` `Binance` `U` `pu` `sequence` `UNKNOWN` `multi-exchange` `UNKNOWN`
- `Dataset` `orderbook` `historical` `completeness` `NOT DEMONSTRATED` `find Bronze 0`
- `Snapshot ↔ WS` `race` `buffering` `replay` `UNKNOWN`

---

**ARCHITECTURE NOT READY** — `PR-A` `READY WITH EXPLICIT CONTRACTS` (can implement `REST` `snapshot` `Bybit` `fetcher` standalone), `PR-B` `BLOCKED BY ARCHITECTURAL UNKNOWN` (`canonical OrderBook contract` `multi-exchange` `sequence` `UNKNOWN` + `Bybit` `REST` `snapshot` `u` `exact` + `dataset` `Bronze` `completeness` `NOT DEMONSTRATED` + `race` `buffering` `UNKNOWN`).

**Next:** `PR-A` `OrderBookSnapshotFetcher` `REST` `Bybit` `limit 50` `u` `bids/asks` `OrderBookSnapshotPayload` **READY WITH EXPLICIT CONTRACTS** (define `Bybit` `adapter` `exchange-specific` `REST` `endpoint` `category=linear` `symbol=BTCUSDT` `update_id` `u` `int` `>0` `bids/asks` `str` `timestamp_ms` `int`).

**No `PR-B` until `canonical OrderBook contract` `multi-exchange` `sequence` `UNKNOWN` + `Bybit` `REST` `u` + `dataset` `Bronze` `completeness` `demonstrated`.**
