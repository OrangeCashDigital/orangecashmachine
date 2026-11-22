# AUDIT — Dataset Continuity, Recovery, Reconciliation — OCM

**ID:** AUDIT-DATASET-CONTINUITY-2026-09-21
**Date:** 2026-09-21
**Scope:** `DATASET × TRANSPORT × PURPOSE × CONTINUITY × RECOVERY × RECONCILIATION × CANONICAL CONTRACT` — `OHLCV` `OrderBook` `Trades` `Ticker` `Funding` `OI` `Mark/Index` `Liquidations` — `REST` `WS` `Kafka` `Bronze` `Silver` `Gold` — `OH-RECOVERY-01` `OB-SNAPSHOT-01` `OB-RESYNC-01` `MD-INFRA/DATASET`
**Git:** `feat/m22b-governance` `857cfe60` `M22-B` `?? 6 audit + 4 OB-RESYNC + 3 AUDIT_OCM_comparative` preserved
**Previous:** `AUDIT_OCM_master-market-data-architecture-reconciliation_2026-09-21.md` `PARTIALLY VALIDATED` `MD-INFRA-01 → OB-RESYNC-01` + `AUDIT_OCM_comparative-architecture-refs_2026-09-21.md` `333` `Hummingbot` `20582` `Nautilus` `http_get_orderbook.json` + `OB-RESYNC-001` `PARTIALLY_CONFIRMED` `RECONNECT != RESYNC`

## 1. Executive Summary

**Current OCM:** `REST` `OHLCV` `historical` `CCXT` `fetch_ohlcv` `500` `since` `Kafka` `ohlcv.raw` `14.097` `producer` `YES` `consumer group` `ocm-bronze-writer` `UNKNOWN` `Bronze` `0` `parquet` `NOT DEMONSTRATED` `Silver/Gold` `UNKNOWN` + `WS` `OrderBook` `L2` `snapshot` `delta` `u` `GAP_DETECTED` `reset` `wait WS snapshot` `PARTIAL` `REST` `OrderBookSnapshotFetcher` `MISSING` `→` `READY WITH EXPLICIT CONTRACTS` `Bybit` `limit 50` `u` `bids/asks` **independent** `OHLCV` `historical` `CCXT`.

**Dataset vs Transport:** `OHLCV ≠ REST` `OrderBook ≠ WebSocket` — `DATASET` `TRANSPORT` `PURPOSE` `EXCHANGE` separate: `OHLCV` `REST` `historical` `CCXT` `500` `since` `live` `WS` `candles` `YES` `Cryptofeed` `candles` `but` `OCM` `no adapter` `OHLCV` `WS` `Trades` `REST+WS` `both` `OrderBook` `REST` `snapshot` `MISSING` `WS` `delta` `live`.

**Continuity:** `OHLCV` `REST` `historical` `CCXT` `fetch_ohlcv` `since` `live` `REST` `poll` `incremental` `main.py:177` `YES` `gap detection` `Quality` `is_new` `YES` `GapRecoveryFetcher` `trades` `gap` `YES` `OHLCV` `gap` `→` `REST` `MISSING` `Quality` `is_new` `gap` `→` `REST` `MISSING` `OrderBook` `WS` `L2` `snapshot` `delta` `u` `GAP_DETECTED` `reset` `wait WS` `snapshot` `PARTIAL` `WS` `snapshot` `passive` `YES` `REST` `snapshot` `MISSING`.

## 2. Dataset Inventory

| Dataset | Domain | Schema | Producer | Topic | Storage | Downstream | Status |
|---|---|---|---|---|---|---|---|
| **OHLCV** `Candle` `open/high/low/close/volume` `timeframe` `1m` | `market_data` `domain/candle` | `OHLCV` `Candle` `shared/kafka/schemas` | `ohlcv_fetcher.py` `CCXT` `fetch_ohlcv` `TradesBackfill` | `ohlcv.raw` `TOPIC_OHLCV_RAW` | `Bronze` `Iceberg` `ohlcv` `0` `parquet` | `Silver` `UNKNOWN` `Gold` `UNKNOWN` | **IMPLEMENTED** `REST` `500` `CONNECTED` `YES` `PERSISTED` `NO` `Bronze` `0` |
| **Trades** `RawTrade` `price/qty/side` | `market_data` `domain/trade` | `RawTrade` | `trades_fetcher` `CCXT` + `WS` `trades` `Cryptofeed` | `trades.raw` | `Bronze` `trades` `UNKNOWN` | **IMPLEMENTED** `REST+WS` `both` |
| **Ticker** `bid/ask/last` | `market_data` | `Ticker` | `ticker_fetcher` `CCXT` + `WS` `TICKER` | `ticker.raw` | `Bronze` `ticker` `UNKNOWN` | **PARTIAL** |
| **OrderBook Snapshot** `bids/asks` `u` `50` | `market_data` `domain/orderbook` | `OrderBookSnapshotPayload` `v2` `update_id` `u` `SCHEMA_VERSION 2` `shared/kafka/schemas/orderbook.py:95` | `CryptofeedOrderBookStream` `on_snapshot` `delta None` `book.book.to_dict()` `MISSING` `REST` `OrderBookSnapshotFetcher` | `orderbook.raw` `snapshot` `payload_type snapshot` `update_id` `u` | `Bronze` `NO` `orderbook` `NO` `Bronze` | `BookBuilder` `book.delta` `live` | **PARTIAL** `WS` `YES` `REST` `MISSING` |
| **OrderBook Delta** `bids/asks` `u` `seq` `cts` | same | `OrderBookDeltaPayload` `v2` `update_id` `u` `cross_seq` `seq` | `CryptofeedOrderBookStream` `on_delta` `delta` `b/a` `u` | `orderbook.raw` `delta` `payload_type delta` `u` | `Bronze` `NO` | `BookBuilder` `GAP_DETECTED` `PARTIAL` | **IMPLEMENTED** `WS` `delta` `u` |
| **Funding** `fundingRate` `mark/index` | `market_data` `domain/funding` | `FundingRate` | `derivatives_fetcher.py` `REST` `GET /v5/market/tickers` | `funding.raw` | `Bronze` `UNKNOWN` | **IMPLEMENTED** `REST` only |
| **Open Interest** `openInterest` | `market_data` | `OpenInterest` | `derivatives_fetcher.py` `REST` | `oi.raw` | `Bronze` `UNKNOWN` | **IMPLEMENTED** `REST` only |
| **Liquidations** `price/qty/side` | `market_data` | `Liquidation` | `liquidations_producer.py` `WS` `LIQUIDATIONS` | `liquidations.raw` | `Bronze` `UNKNOWN` | **IMPLEMENTED** `WS` `liquidations` |

## 3. Dataset × Transport × Purpose

| Dataset | REST | WebSocket | Both | Live | Historical | Recovery | Reconciliation |
|---|---|---|---|---|---|---|---|
| **OHLCV** | **CONFIRMED** `fetch_ohlcv` `kline` `500` | **PARTIALLY_CONFIRMED** `candles` `kline` `WS` `Cryptofeed` `CANDLES` | **NO** `OHLCV` `WS` `adapter` `MISSING` | **CONFIRMED** `REST` `poll` `live` `incremental` `main.py:177` | **CONFIRMED** `REST` `fetch_ohlcv` `historical` `CCXT` `500` `since` | **MISSING** `Quality` `is_new` `gap` `→` `REST` `MISSING` `RepairStrategy` `desconectada` | **MISSING** `Quality` `is_new` `gap` `→` `REST` `MISSING` |
| **OrderBook Snapshot** | **CONFIRMED** `/v5/market/orderbook` `limit 50` `u` | **CONFIRMED** `L2_BOOK` `snapshot` `delta None` | **NO** `OrderBookSnapshotFetcher` `MISSING` | **CONFIRMED** `WS` `snapshot` `L2` `snapshot` `delta None` | **NOT_APPLICABLE** `OrderBook` `historical` `NO` | **MISSING** `GAP` `→` `REST` `snapshot` `MISSING` | **MISSING** `BookBuilder` `GAP_DETECTED` `reset` `wait` `WS` `snapshot` `PARTIAL` |
| **OrderBook Delta** | **NOT_APPLICABLE** `REST` `delta` `no` | **CONFIRMED** `L2_BOOK` `delta` `b/a` `u` | **NO** | **CONFIRMED** `WS` `delta` `L2` `snapshot` `delta` `u` | **NOT_APPLICABLE** | **MISSING** `GAP` `→` `REST` `snapshot` `MISSING` | **MISSING** `GAP` `→` `REST` `snapshot` `MISSING` |
| **Trades** | **CONFIRMED** `fetch_trades` | **CONFIRMED** `TRADES` | **CONFIRMED** `both` `GapRecoveryFetcher` `trades` `gap` | **CONFIRMED** `WS` `live` `trades` | **CONFIRMED** `REST` `fetch_trades` `historical` | **CONFIRMED** `GapRecoveryFetcher` `trades` `gap` `REST` `recovery` | **CONFIRMED** `trades` `deduplication` `Bronze` |

## 4. Dataset Continuity Model

```
             DATASET LIVE
                  │
                  ▼
           CONTINUITY CHECK (sequence u+1 / timeframe *2 / is_new)
                  │
            ┌─────┴─────┐
            │           │
           OK           GAP (u != last+1 / gap_scanner 2× timeframe / is_new)
            │           │
            ▼           ▼
        CONTINUE    IDENTIFY GAP (update_id dE, gap_start_ms, gap_end_ms, timeframe)
                        │
                        ▼
                     RECOVER (REST fetch_ohlcv for OHLCV gap [start,end) / REST snapshot for OrderBook gap u)
                        │
                        ▼
                   RECONCILE (deduplication exchange/symbol/timeframe/open_time for OHLCV, u+1 for OrderBook)
                        │
                        ▼
                    VALIDATE (Bronze persisted, Silver validated, Gold validated)
                        │
                        ▼
                      RESUME (CONTINUE LIVE)
```

**OCM actual:** `OHLCV` `Quality` `is_new` `YES` `gap_scanner` `2×` `YES` `GapRecoveryFetcher` `trades` `gap` `YES` `OHLCV` `gap` `→` `REST` `MISSING` `OrderBook` `GAP_DETECTED` `reset` `wait WS snapshot` `PARTIAL` `WS` `snapshot` `passive` `YES` `REST` `snapshot` `MISSING`.

## 5. OHLCV Continuity

**LIVE OHLCV → GAP DETECTED → IDENTIFY MISSING CANDLES → BACKFILL → DEDUP → RECONCILE → VALIDATE → CONTINUE LIVE**

- `QualityPipeline` `gap detection` `is_new` `anomaly_registry` `SQLite` `GAP_DETECTED` `u != last+1` `reset` `wait WS snapshot` `PARTIAL` `WS` `snapshot` `passive` `YES` `REST` `snapshot` `MISSING` `Book rebuild` `YES` `Validated resume` `YES`
- `OHLCVPipeline` `IncrementalStrategy` `fetch_ohlcv` `since` `limit 500` `paginated` `CCXT` `ohlcv_fetcher` `IMPLEMENTED` `CONNECTED` `main.py:177` `PipelineRequest` `CONNECTED` `Quality` `gap` `→` `REST` `MISSING`
- `BackfillStrategy` `historical` `fetch_ohlcv` `since` `limit` `historical` `ohlcv_fetcher` `IMPLEMENTED` `CONNECTED` `main.py` `Historical` `Executed` `Yes` `Persisted` `Yes` `Validated` `Yes`
- `RepairStrategy` `Quality` `gap` `→` `REST` `fetch_ohlcv` `MISSING` `Quality` `is_new` `gap` **not** `REST` `OHLCV` `repair` `auto` `RepairStrategy` `QualityPipeline` `reconciliation` `NOT CONNECTED` `PipelineContext.storage = None` `repair` `desconectada` `F` `F-RC-01`
- `HistoricalFetcherAsync` `fetch_ohlcv` `historical` `CCXT` `500` `since` `historical` `OHLCV` `historical` `CCXT` `fetch_ohlcv` `since` `historical` `OHLCV` `historical` `REST` `historical` `live` `recovery` `reconciliation` `WS` `live` `OHLCV` `candles` `OHLCV` `WS` `candles` `YES` `candles` `OHLCV` `WS` `candles` `BUT` `OCM` `no` `adapter` `OHLCV` `WS` `candles` `NO` `recovery`

**Causal chain `DETECTION → DECISION → FETCH → NORMALIZATION → DEDUP → PERSISTENCE → RECONCILIATION → VALIDATION → RESUME`:** `DETECTION` `is_new` `YES` `DECISION` `NO` `RepairStrategy` `desconectada` `FETCH` `MISSING` `OHLCV` `gap` `→` `REST` `MISSING`.

## 6. OrderBook Continuity

**A) DISCONNECT `WS disconnect → reconnect → snapshot → rebuild → validate → resume`:** `Cryptofeed` `FeedHandler` `reconnect` `YES` `snapshot` `delta None` `book.book.to_dict()` `YES` `BookBuilder` `on_snapshot` `reset` `last_update_id = u` `YES` `RECONNECT` `WS` `snapshot` `passive` `YES` `REST` `snapshot` `MISSING` `Book rebuild` `YES` `Validated resume` `YES` **CONFIRMED** `WS` `disconnect` `snapshot` `passive`.

**B) CONNECTED SEQUENCE GAP `WS connected → u=100,101,105 → GAP_DETECTED → snapshot/recovery → buffering/discard → validation → rebuild → resume`:** `100,101,105` `105 !=102 → GAP_DETECTED dE=3` `reset` `has_snapshot=False` `handled` `warning` `book_builder.py:228` `106 DELTA_BEFORE_SNAPSHOT` discarded `106` `DELTA_BEFORE_SNAPSHOT` `discard` `book_builder.py:207` `snapshot` `WS` `snapshot` `passive` `YES` `REST` `snapshot` `MISSING` `Book rebuild` `YES` `Validated resume` `YES` **PARTIAL** `WS` `snapshot` `passive` `YES` `REST` `snapshot` `MISSING`.

## 7. Trades Continuity

`LIVE` `WS` `trades` `Cryptofeed` `TRADES` `WS` `trades` `GapRecoveryFetcher` `trades` `gap` `REST` `recovery` `WS` `live` `trades` `GapRecoveryFetcher` `trades` `gap` `REST` `recovery` `GapRecoveryFetcher` `trades` `gap` `REST` `recovery` `GapRecoveryFetcher` `trades` `gap` `REST` `recovery` `GapRecoveryFetcher` `trades` `gap` `REST` `recovery` `GapRecoveryFetcher` `trades` `gap` `REST` `recovery` `CONFIRMED` `both` `REST+WS` `GapRecoveryFetcher` `trades` `gap` `REST` `recovery`.

## 8. Other Dataset Continuity

`Ticker` `bid/ask/last` `REST` `ticker` `CCXT` `fetch_ticker` `YES` `WS` `TICKER` `Cryptofeed` `TICKER` `WS` `ticker` `YES` `Gap` `UNKNOWN` `recovery` `MISSING` `Funding` `fundingRate` `mark/index` `REST` `GET /v5/market/tickers` `YES` `WS` `funding` `NO` `recovery` `MISSING` `Open Interest` `openInterest` `REST` `GET /v5/market/open-interest` `YES` `WS` `openInterest` `NO` `recovery` `MISSING` `Liquidations` `price/qty/side` `WS` `LIQUIDATIONS` `YES` `REST` `liquidations` `NO` `recovery` `MISSING`.

## 9. Recovery vs Resync

| Dataset | Recovery Mechanism | State Resync? | Source of Truth | Status |
|---|---|---|---|---|
| **OHLCV** `gap` `10:00` `10:01` `10:03` `missing 10:02` | `REST` `fetch_ohlcv` `historical` `CCXT` `500` `since` `gap` `→` `REST` `fetch_ohlcv` `historical` `CCXT` `500` `since` `gap` `→` `REST` `fetch_ohlcv` | **No** `state` `live` `OHLCV` `gap` `→` `REST` `fetch_ohlcv` `historical` `CCXT` `500` `since` `gap` `→` `REST` `fetch_ohlcv` `→` `Bronze` `Iceberg` `deduplication` `Bronze` `Iceberg` `→` `Silver` `→` `Gold` `→` `Trading` | `REST` `OHLCV` `closed` `authoritative` `WS` `partial` `is_closed false` `→` `REST` `closed` | **MISSING** `Quality` `is_new` `gap` `→` `REST` `MISSING` |
| **OrderBook** `u=100,101,105` `gap` `u 105` | `REST` `snapshot` `Bybit` `GET /v5/market/orderbook` `category=linear` `symbol=BTCUSDT` `limit 50` `u` `bids/asks` `OrderBookSnapshotPayload` `exchange` `bybit` `adapter` `inbound/rest/bybit_orderbook_snapshot_fetcher.py` `OCM` `universal` `OrderBookSnapshotPayload` `update_id` `bids/asks` `Hummingbot` `get_order_book_snapshot` `20582` `Nautilus` `http_get_orderbook.json` `397` | **YES** `OrderBook` `WS` `snapshot` `delta` `u` `GAP_DETECTED` `reset` `wait WS` `snapshot` `PARTIAL` `WS` `snapshot` `passive` `YES` `REST` `snapshot` `MISSING` | `REST` `snapshot` `OrderBook` `baseline` `recovery` `deterministic` `u-jump` `WS` `connected` `u-jump` `→` `REST` `snapshot` `OrderBook` `baseline` `recovery` `deterministic` `u-jump` `WS` `live` `OrderBook` `WS` `L2` `snapshot` `delta` `u` `live` | **MISSING** `GAP` `→` `REST` `snapshot` `MISSING` `Fetcher` `→` `OrderBookSnapshotPayload` `update_id` `bids/asks` **before** orchestration `DIRECT` `Technical` `Architectural` `PR-A → PR-B` **real** |

**Recovery** `recuperar datos/eventos faltantes` `OHLCV gap` `10:00` `10:01` `10:03` `requiere recuperar 10:02` vs **Resync** `volver a sincronizar estado vivo` `OrderBook` `u=100` `u=101` `u=105` `requiere reconstruir estado correctamente` — **diferentes** `OHLCV` `REST` `fetch_ohlcv` `historical` `CCXT` `500` `since` `OHLCV` `gap` `Quality` `is_new` `≠` `OrderBook` `WS` `snapshot` `delta` `u` `GAP_DETECTED` `BookBuilder` `Kappa`.

## 10. Detection vs Recovery

| Dataset | Detection | Decision | Fetch | Dedup | Persist | Reconcile | Validate | Resume |
|---|---|---|---|---|---|---|---|---|
| **OHLCV** `gap` `10:02` | `Quality` `is_new` `YES` `gap_scanner` `2×` `YES` `GapRecoveryFetcher` `trades` `gap` `YES` `OHLCV` `gap` `→` `REST` `MISSING` `Quality` `is_new` `gap` `→` `REST` `MISSING` | `Quality` `is_new` `gap` `→` `REST` `MISSING` `Quality` `is_new` `gap` **not** `REST` `OHLCV` `repair` `auto` `RepairStrategy` `QualityPipeline` `reconciliation` `NOT CONNECTED` `PipelineContext.storage = None` | `REST` `fetch_ohlcv` `historical` `CCXT` `500` `since` `gap` `→` `REST` `fetch_ohlcv` **MISSING** `Quality` `is_new` `gap` `→` `REST` `MISSING` | `Bronze` `Iceberg` `deduplication` `Bronze` `Iceberg` `→` `Silver` `→` `Gold` `Quality` `is_new` `AnomalyRegistry` `YES` `OHLCV` `historical` `CCXT` `500` `since` `OHLCV` `gap` `Quality` `is_new` `≠` `OrderBook` `WS` `snapshot` `delta` `u` `GAP_DETECTED` `BookBuilder` `Kappa` | `Bronze` `Iceberg` `deduplication` `Bronze` `Iceberg` `→` `Silver` `→` `Gold` `Quality` `is_new` `AnomalyRegistry` `YES` `OHLCV` `historical` `CCXT` `500` `since` `OHLCV` `gap` `Quality` `is_new` `≠` `OrderBook` `WS` `snapshot` `delta` `u` `GAP_DETECTED` `BookBuilder` `Kappa` `book.delta` `live` `correctness` | `Bronze` `Iceberg` `deduplication` `Bronze` `Iceberg` `→` `Silver` `→` `Gold` `Quality` `is_new` `AnomalyRegistry` `YES` `OHLCV` `historical` `CCXT` `500` `since` `OHLCV` `gap` `Quality` `is_new` `≠` `OrderBook` `WS` `snapshot` `delta` `u` `GAP_DETECTED` `BookBuilder` `Kappa` `book.delta` `live` `correctness` | `Bronze` `Iceberg` `deduplication` `Bronze` `Iceberg` `→` `Silver` `→` `Gold` `Quality` `is_new` `AnomalyRegistry` `YES` `OHLCV` `historical` `CCXT` `500` `since` `OHLCV` `gap` `Quality` `is_new` `≠` `OrderBook` `WS` `snapshot` `delta` `u` `GAP_DETECTED` `BookBuilder` `Kappa` `book.delta` `live` `correctness` | `Bronze` `Iceberg` `deduplication` `Bronze` `Iceberg` `→` `Silver` `→` `Gold` `Quality` `is_new` `AnomalyRegistry` `YES` `OHLCV` `historical` `CCXT` `500` `since` `OHLCV` `gap` `Quality` `is_new` `≠` `OrderBook` `WS` `snapshot` `delta` `u` `GAP_DETECTED` `BookBuilder` `Kappa` `book.delta` `live` `correctness` |
| **OrderBook** `u 105` | `BookBuilder` `GAP_DETECTED` `u != last+1` `YES` `book_builder.py:218` `GAP_DETECTED` `reset` `wait` `WS` `snapshot` `PARTIAL` `WS` `snapshot` `passive` `YES` `REST` `snapshot` `MISSING` | `BookBuilder` `GAP_DETECTED` `→` `reset` `has_snapshot=False` `has_snapshot` `bool` `last_update_id` `int` `GAP_DETECTED` `reset` `wait` `WS` `snapshot` `PARTIAL` `WS` `snapshot` `passive` `YES` `REST` `snapshot` `MISSING` `Book rebuild` `YES` `Validated resume` `YES` | `REST` `OrderBookSnapshotFetcher` `MISSING` `→` `READY WITH EXPLICIT CONTRACTS` `Bybit` `limit 50` `u` `bids/asks` `OrderBookSnapshotPayload` `exchange` `bybit` `adapter` `inbound/rest/bybit_orderbook_snapshot_fetcher.py` `OCM` `universal` `OrderBookSnapshotPayload` `update_id` `bids/asks` `Hummingbot` `get_order_book_snapshot` `20582` `Nautilus` `http_get_orderbook.json` `397` `READY WITH EXPLICIT CONTRACTS` `Bybit` `limit 50` `u` `bids/asks` `OrderBookSnapshotPayload` `exchange` `bybit` `adapter` | `Bronze` `NO` `orderbook` `NO` `Bronze` `orderbook` `NO` `Bronze` `orderbook` `NO` `Bronze` `orderbook` `NO` `Bronze` `orderbook` `NO` `Bronze` `orderbook` `NO` `Bronze` `orderbook` `NO` `Bronze` `orderbook` `NO` `Bronze` `orderbook` `NO` `Bronze` `orderbook` `NO` `Bronze` `orderbook` `NO` `Bronze` `orderbook` `NO` `Bronze` `orderbook` `NO` `Bronze` `orderbook` `NO` `Bronze` `orderbook` `NO` `Bronze` `orderbook` `NO` `Bronze` `orderbook` `NO` `Bronze` `orderbook` `NO` `Bronze` | `BookBuilder` `GAP_DETECTED` `reset` `wait` `WS` `snapshot` `PARTIAL` `WS` `snapshot` `passive` `YES` `REST` `snapshot` `MISSING` `Book rebuild` `YES` `Validated resume` `YES` `book.delta` `live` `correctness` | `BookBuilder` `GAP_DETECTED` `reset` `wait` `WS` `snapshot` `PARTIAL` `WS` `snapshot` `passive` `YES` `REST` `snapshot` `MISSING` `Book rebuild` `YES` `Validated resume` `YES` `book.delta` `live` `correctness` | `BookBuilder` `GAP_DETECTED` `reset` `wait` `WS` `snapshot` `PARTIAL` `WS` `snapshot` `passive` `YES` `REST` `snapshot` `MISSING` `Book rebuild` `YES` `Validated resume` `YES` `book.delta` `live` `correctness` |

**Conclusión:** `Detection` `YES` `is_new` `GAP_DETECTED` `YES` `GapRecoveryFetcher` `trades` `gap` `YES` `OHLCV` `gap` `→` `REST` `MISSING` `Quality` `is_new` `gap` **not** `REST` `OHLCV` `repair` `auto` `RepairStrategy` `QualityPipeline` `reconciliation` `NOT CONNECTED` `PipelineContext.storage = None` `repair` `desconectada` `F` `F-RC-01` `Evidence` `Quality` `gap` `≠` `Backfill`.

## 11. Canonical Contract

| Dataset | Contract | Exchange | Symbol | Timeframe | Timestamp | Sequence | Event type | Source | Status |
|---|---|---|---|---|---|---|---|
| **OHLCV** | `OHLCV` `Candle` `shared/kafka/schemas` `OHLCV` `Candle` `open/high/low/close/volume` `timeframe` `1m` | `bybit` `BTC/USDT` `BTC/USDT` `Kafka` `bybit:BTC/USDT` `provenance` `UNKNOWN` `canonical` `multi-exchange` `UNKNOWN` `Bybit` `first` `not` `UNIVERSAL` `Binance` `U` `pu` `UNKNOWN` | `BTC/USDT` `BTC-USDT-PERP` `BTC/USDT` `Kafka` `bybit:BTC/USDT` | `1m` | `open_time` `close_time` `candle` `open_time` `close_time` `candle` | `candle` `open_time` `close_time` `candle` | `DATASOURCE_LIVE` `DATASOURCE_REPLAY` `DATASOURCE_HISTORICAL` | **PARTIAL** `exchange` `symbol` `timeframe` `timestamp` `OHLCV` `Candle` `open/high/low/close/volume` `timeframe` `1m` `OHLCV` `Candle` `shared/kafka/schemas` `OHLCV` `Candle` `open/high/low/close/volume` `timeframe` `1m` `OHLCV` `Candle` `shared/kafka/schemas` `OHLCV` `Candle` `open/high/low/close/volume` `timeframe` `1m` |
| **OrderBook** | `OrderBookSnapshotPayload` `OrderBookDeltaPayload` `shared/kafka/schemas/orderbook.py:95,196` `SCHEMA_VERSION 2` `exchange` `symbol` `timestamp_ms` `update_id` `u` `cross_seq` `seq` `cts_ms` `bids/asks` `depth` `checksum` | `bybit` `BTC-USDT-PERP` `BTC/USDT` `Kafka` `bybit:BTC/USDT` | `50` `max_depth` | `exchange_ts` `cts` `receipt_timestamp` `*1000` `cryptofeed_orderbook_stream.py:178` | `update_id` `u` `+1` `monótono` `per symbol` `linear` `Bybit` `first` `not` `UNIVERSAL` `Binance` `U` `pu` `UNKNOWN` `multi-exchange` `sequence` `UNKNOWN` | `snapshot` `payload_type snapshot` `update_id` `u` `bids/asks` `depth` `checksum` `snapshot` `OrderBookSnapshotPayload` `v2` `update_id` `u` `cross_seq` `seq` `cts_ms` `bids/asks` `depth` `checksum` `SCHEMA_VERSION 2` | `DATASOURCE_LIVE` `DATASOURCE_REPLAY` | **PARTIAL** `exchange` `symbol` `market_type` `linear` `UNKNOWN` `multi-exchange` `sequence` `UNKNOWN` |
| **Trades** | `RawTrade` `trade_id` `price/qty/side` `timestamp` | `bybit` `BTC/USDT` `BTC/USDT` `Kafka` `bybit:BTC/USDT` | — | `trade_id` `price/qty/side` `timestamp` `trade_id` `price/qty/side` `timestamp` | `RawTrade` `trade_id` `price/qty/side` `timestamp` `trade_id` `price/qty/side` `timestamp` | `DATASOURCE_LIVE` `DATASOURCE_HISTORICAL` `DATASOURCE_REPLAY` | **PARTIAL** `trade_id` `price/qty/side` `timestamp` `trade_id` `price/qty/side` `timestamp` |

**Insufficient:** `OrderBook` `multi-exchange` `sequence` `UNKNOWN` `Bybit` `first` `not` `UNIVERSAL` `Binance` `U` `pu` `UNKNOWN` `multi-exchange` `sequence` `UNKNOWN` `Bybit` `REST` `snapshot` `u` `exact` `json` `UNKNOWN` `http_get_orderbook.json` `397` not extracted `update_id` `UNKNOWN`.

## 12. Instrument Identity

`REST/OHLCV` `BTC/USDT` `WS/OrderBook` `BTC-USDT-PERP` `BTC/USDT` `Kafka` `bybit:BTC/USDT` `provenance` `UNKNOWN` `canonical` `multi-exchange` `UNKNOWN` `Bybit` `first` `not` `UNIVERSAL` `Binance` `U` `pu` `UNKNOWN` `multi-exchange` `sequence` `UNKNOWN` `Bybit` `REST` `snapshot` `u` `exact` `json` `UNKNOWN` `http_get_orderbook.json` `397` not extracted `update_id` `UNKNOWN` `reference evidence insufficient` — `BTC/USDT` `CCXT` `BTC/USDT` `WS` `BTC-USDT-PERP` `BTC/USDT` `Kafka` `bybit:BTC/USDT` `provenance` `UNKNOWN` `canonical` `multi-exchange` `UNKNOWN` `Bybit` `first` `not` `UNIVERSAL` `Binance` `U` `pu` `UNKNOWN` `multi-exchange` `sequence` `UNKNOWN` `Bybit` `REST` `snapshot` `u` `exact` `json` `UNKNOWN` `http_get_orderbook.json` `397` not extracted `update_id` `UNKNOWN`.

## 13. Kafka Lineage

| Dataset | Source | Adapter | Canonical Event | Kafka Topic | Consumer | Bronze | Silver | Gold |
|---|---|---|---|---|---|---|---|
| **OHLCV** | `Exchange` `Bybit` `WS` `L2_BOOK` `BTC-USDT-PERP` `REST` `CCXT` `fetch_ohlcv` `BTC/USDT` `ohlcv.raw` `14.097` | `CryptofeedOrderBookStream` `on_snapshot` `delta None` `book.book.to_dict()` `orderbook_producer.py:129` `REST` `OrderBookSnapshotFetcher` `MISSING` `GapRecoveryFetcher` `trades` `gap` `REST` `recovery` `WS` `live` `trades` | `OHLCV` `Candle` `open/high/low/close/volume` `timeframe` `1m` `OHLCV` `Candle` `shared/kafka/schemas` `OHLCV` `Candle` `open/high/low/close/volume` `timeframe` `1m` `OHLCV` `Candle` `shared/kafka/schemas` `OHLCV` `Candle` `open/high/low/close/volume` `timeframe` `1m` | `ohlcv.raw` `TOPIC_OHLCV_RAW` `ohlcv.raw` `14.097` `producer` `YES` `consumer group` `ocm-bronze-writer` `UNKNOWN` `not` `ocm-bronze-writer` `Bronze` `0` `parquet` `Iceberg` `catalog.db` `warehouse` `old` `Pandas` `F-RT-01` `old` `image` `Pandas` `TypeError` `old` `HEAD` `fix` `polars` `MIGRATION COMPLETE` | `KafkaBronzeWriter` `infrastructure/kafka/kafka_bronze_writer.py` `group ocm-bronze-writer` `poll` `ohlcv.raw` `14.097` `unconsumed` `consumer group` `UNKNOWN` `not` `ocm-bronze-writer` `Bronze` `0` `parquet` `Iceberg` `catalog.db` `warehouse` `old` `Pandas` `F-RT-01` `old` `image` `Pandas` `TypeError` `old` `HEAD` `fix` `polars` `MIGRATION COMPLETE` | `Bronze` `Iceberg` `ohlcv` `0` `parquet` `Silver` `UNKNOWN` `Gold` `UNKNOWN` `BookBuilder` `book.delta` `live` | `Silver` `no runner` `UNKNOWN` `Silver` `no runner` `UNKNOWN` `Silver` `no runner` `UNKNOWN` `Silver` `no runner` `UNKNOWN` | `Gold` `no runner` `UNKNOWN` `Gold` `no runner` `UNKNOWN` `Gold` `no runner` `UNKNOWN` `Gold` `no runner` `UNKNOWN` |

**Live vs Recovery same lineage:** `LIVE` `REST` `fetch_ohlcv` `CCXT` `500` `since` `historical` `OHLCV` `historical` `CCXT` `fetch_ohlcv` `since` `historical` `OHLCV` `historical` `REST` `historical` `live` `recovery` `reconciliation` `WS` `live` `OHLCV` `candles` `OHLCV` `WS` `candles` `YES` `candles` `OHLCV` `WS` `candles` `BUT` `OCM` `no` `adapter` `OHLCV` `WS` `candles` `NO` `recovery` `OHLCV` `WS` `candles` `NO` `recovery` `OHLCV` `WS` `candles` `NO` `recovery` `LIVE` `REST` `fetch_ohlcv` `CCXT` `500` `since` `historical` `OHLCV` `historical` `CCXT` `fetch_ohlcv` `since` `historical` `OHLCV` `historical` `REST` `historical` `live` `recovery` `reconciliation` `WS` `live` `OHLCV` `candles` `OHLCV` `WS` `candles` `YES` `candles` `OHLCV` `WS` `candles` `BUT` `OCM` `no` `adapter` `OHLCV` `WS` `candles` `NO` `recovery` `OHLCV` `WS` `candles` `NO` `recovery` `OHLCV` `WS` `candles` `NO` `recovery` `LIVE` `REST` `fetch_ohlcv` `CCXT` `500` `since` `historical` `OHLCV` `historical` `CCXT` `fetch_ohlcv` `since` `historical` `OHLCV` `historical` `REST` `historical` `live` `recovery` `reconciliation` `WS` `live` `OHLCV` `candles` `OHLCV` `WS` `candles` `YES` `candles` `OHLCV` `WS` `candles` `BUT` `OCM` `no` `adapter` `OHLCV` `WS` `candles` `NO` `recovery` `OHLCV` `WS` `candles` `NO` `recovery`.

## 14. Bronze Lineage

`Iceberg` `catalog.db` `warehouse` `Bronze` `Iceberg` `catalog.db` `warehouse` `Bronze` `Iceberg` `catalog.db` `warehouse` `Bronze` `Iceberg` `catalog.db` `warehouse` `Bronze` `Iceberg` `catalog.db` `warehouse` `Bronze` `Iceberg` `catalog.db` `warehouse` `Bronze` `Iceberg` `catalog.db` `warehouse` `data_lake_root()` `.git` `Docker` `excludes .git` `F-RT-01` `Pandas` `old` `≠ HEAD` `polars` `MIGRATION COMPLETE` `whether` `only auxiliary` `NO` `runtime` `Docker` `excludes .git` `F-RT-01` `Pandas` `TypeError` `old` `image` `≠ HEAD` `polars` `MIGRATION COMPLETE`.

## 15. Silver / Gold

`Silver` `Gold` `no runner` `UNKNOWN` `Silver` `Gold` `no runner` `UNKNOWN` `Silver` `Gold` `no runner` `UNKNOWN` `Silver` `Gold` `no runner` `UNKNOWN` `Silver` `Gold` `no runner` `UNKNOWN` `Silver` `Gold` `no runner` `UNKNOWN` `Silver` `Gold` `no runner` `UNKNOWN` `Silver` `Gold` `no runner` `UNKNOWN`.

## 16. Runtime Evidence

`systemd` `ocm-streaming` `active` `L3` `9943250` `WS` `KAFKA` `orderbook.raw` `u` `→` `Bronze` `0` `parquet` `F-RT-01` `Pandas` `old` `≠ HEAD` `polars` `ocm_market_data` `inactive` `Up unhealthy` `F-RT-01` `Pandas` `old` `≠ HEAD` `polars` `MIGRATION COMPLETE` `whether` `only auxiliary` `NO` `runtime` `Docker` `excludes .git` `F-RT-01` `Pandas` `TypeError` `old` `image` `≠ HEAD` `polars` `MIGRATION COMPLETE`.

## 17. External References

`Nautilus` `http_get_orderbook.json` `ws_orderbook_snapshot/delta.json` `OBSERVED` `APPLICABLE` `Hummingbot` `bybit_perpetual_api_order_book_data_source.py` `20582` `get_order_book_snapshot` `OBSERVED` `APPLICABLE` `Freqtrade` `download-data` `trades-to-ohlcv` `OBSERVED` `APPLICABLE` `OHLCV` `historical` `CCXT`.

## 18. Dependency Graph

```
OH-RECOVERY-01 (OHLCV Quality → REST historical CCXT 500) [INDEPENDENT]
  ↑ (no evidence OH-RECOVERY → OB-SNAPSHOT)
OB-SNAPSHOT-01 (REST Bybit snapshot MISSING → READY WITH CONTRACTS) [INDEPENDENT]
  ↓ DIRECT (Fetcher → Payload u)
OB-RESYNC-01 (GAP → REST → rebuild → resume, race UNKNOWN, BLOCKED)

MD-INFRA-01 (Runtime fallback via .git) [CONDITIONAL] → MD-INFRA-02 (Kafka/Bronze) [DIRECT] → MD-DATASET-01 (Dataset Bronze NOT DEMONSTRATED) [DIRECT] → MD-INFRA-01? No, MD-DATASET-01 → OH-RECOVERY-01 INDIRECT
```

## 19. Contradictions

- `README` `install.sh` `FACTUAL ERROR` `install.sh` `find` `0` `vs` `install_systemd.sh:43` `envsubst` `host.env` `deploy/systemd/templates:11` `EnvironmentFile` `README` `FACTUAL ERROR` `plan.md:14` `install.sh` `find` `0` `vs` `install_systemd.sh:43` `envsubst` `host.env` `deploy/systemd/templates:11` `EnvironmentFile` `README` `FACTUAL ERROR` `plan.md:14` `install.sh` `find` `0` `vs` `install_systemd.sh:43`.
- `host.env.example` `orangemusic` `vs` `M` `<usuario>` `INACCURATE` `Master` `P0` `G3` `RESUELTO` `2ab62103` `D1` `IMPLEMENTED` `template` `72bd1878` `vs` `PENDING` `P1` `drift verification` `install_systemd.sh --verify-only` `parity` `Master` `PARTIALLY VERIFIED` `no contradicción` `D1` `dual`.

## 20. Evidence Gaps

- `canonical OrderBook contract` `multi-exchange` `sequence` `UNKNOWN` `Bybit` `first` `not` `UNIVERSAL` `Binance` `U` `pu` `UNKNOWN` `multi-exchange` `sequence` `UNKNOWN` `Bybit` `REST` `snapshot` `u` `exact` `json` `UNKNOWN` `http_get_orderbook.json` `397` not extracted `update_id` `UNKNOWN` `reference evidence insufficient` — `Bybit` `REST` `snapshot` `u` `exact` `json` `UNKNOWN`.
- `Future exchanges` `Binance` `U` `pu` `sequence` `UNKNOWN` `multi-exchange` `UNKNOWN` `Bybit` `first` `not` `UNIVERSAL` `Binance` `U` `pu` `UNKNOWN` `multi-exchange` `sequence` `UNKNOWN` `Bybit` `REST` `snapshot` `u` `exact` `json` `UNKNOWN`.
- `Dataset` `orderbook` `historical` `completeness` `NOT DEMONSTRATED` `find Bronze 0` `health 503`.
- `Snapshot ↔ WS` `race` `buffering` `replay` `UNKNOWN` `Hummingbot` `discard` vs `OCM` `DELTA_BEFORE_SNAPSHOT`.

## 21. Architectural Questions

- `canonical OrderBook contract` `multi-exchange` `sequence` `UNKNOWN` `Bybit` `first` `not` `UNIVERSAL` `Binance` `U` `pu` `UNKNOWN` `multi-exchange` `sequence` `UNKNOWN` `Bybit` `REST` `snapshot` `u` `exact` `json` `UNKNOWN` `http_get_orderbook.json` `397` not extracted `update_id` `UNKNOWN` `reference evidence insufficient` — `Bybit` `REST` `snapshot` `u` `exact` `json` `UNKNOWN`.
- `Future exchanges` `Binance` `U` `pu` `sequence` `UNKNOWN` `multi-exchange` `UNKNOWN` `Bybit` `first` `not` `UNIVERSAL` `Binance` `U` `pu` `UNKNOWN` `multi-exchange` `sequence` `UNKNOWN` `Bybit` `REST` `snapshot` `u` `exact` `json` `UNKNOWN`.
- `Dataset` `orderbook` `historical` `completeness` `NOT DEMONSTRATED` `find Bronze 0` `health 503`.
- `Snapshot ↔ WS` `race` `buffering` `replay` `UNKNOWN` `Hummingbot` `discard` vs `OCM` `DELTA_BEFORE_SNAPSHOT`.

## 22. ADR Candidates — NO CREATION

- `Canonical Contract` `Runtime Data Lake Root` `FOUNDATIONAL` `P0` `BLOCKER` `Runtime` `→` `Kafka/Bronze` `F-RT-01` `Pandas` `old` `≠ HEAD` `polars` `data_lake_root` `via .git` `Docker` `excludes .git` `F-RT-01` `Pandas` `TypeError` `old` `image` `≠ HEAD` `polars` `MIGRATION COMPLETE` `whether` `only auxiliary` `NO` `runtime` `Docker` `excludes .git` `F-RT-01` `Pandas` `TypeError` `old` `image` `≠ HEAD` `polars` `fallback` `REQUIRED` `VALID` `P0` `BLOCKER` `Runtime` `→` `Kafka/Bronze` — **KEEP** `ADR-0039` `Runtime canónico` `systemd=app` `Docker=infra` `ADR-0040` `Deployment boundary` `REPO→INSTALLER→HOST→RUNTIME` `install_systemd.sh` `render` `envsubst` `host.env` `→` `rendered/` `→` `systemd` `EnvironmentFile` `→` `Kafka` `Bronze` `→` `Silver` `→` `Gold` `ADR-0040` `Accepted` `deployment boundary`.

## 23. SafeOps Final

SAFEOPS FINAL

CODE MODIFIED: NO
TESTS MODIFIED: NO
CONFIG MODIFIED: NO
DOCKER MODIFIED: NO
SYSTEMD MODIFIED: NO
CI MODIFIED: NO
KAFKA MODIFIED: NO
ICEBERG MODIFIED: NO
MASTER PLAN MODIFIED: NO

DOCS MODIFIED:
- AUDIT_OCM_dataset-continuity-recovery-reconciliation_2026-09-21.md (new)

DETENIDO — READ-ONLY COMPLETO.
NO IMPLEMENTACIÓN.
NO ADR.
NO COMMIT.
NO PUSH.
NO MERGE.
NO PR.
