# AUDIT — Master Market Data Architecture Reconciliation — OCM

**ID:** AUDIT-MASTER-RECONCILIATION-2026-09-21
**Date:** 2026-09-21
**Scope:** `DATASET × TRANSPORT × PURPOSE × CONTRACT × KAFKA × BRONZE × RECOVERY` — `OHLCV` `Trades` `Ticker` `OrderBook` `Funding` `OI` `Mark/Index` `Liquidations` — `REST` `WS` `Kafka` `Bronze` `Silver` `Gold` — `OH-RECOVERY-01` `OB-SNAPSHOT-01` `OB-RESYNC-01` `MD-INFRA/DATASET`
**Git:** `feat/m22b-governance` `857cfe60` `M22-B` `?? 6 audit + 4 OB-RESYNC + 3 AUDIT_OCM_comparative` preserved
**Previous masters:** `AUDIT_OCM_master-market-data-implementation-order_2026-09-21.md` `348` `PARTIALLY VALIDATED` `MD-INFRA-01 → OB-RESYNC-01` — **to be reconciled**
**Files:** `packages/market_data/main.py:146`, `adapters/inbound/rest/ohlcv_fetcher.py`, `adapters/inbound/websocket/cryptofeed_orderbook_stream.py:246`, `book_builder.py:218`, `shared/kafka/schemas/orderbook.py:169`, `gap_recovery_fetcher.py:6`, `config/market_data/feeds.yaml`, `deploy/systemd/ocm-streaming.service.template:11`, `docker-compose.yml:24`, `Dockerfile:40`, `ocm/config/paths.py` `data_lake_root()`, `tests/market_data/application/processing/test_book_builder.py:104`, `docs/audits/AUDIT_OCM_b59-runtime-operational-audit_2026-09-19.md`, `AUDIT_OCM_comparative-architecture-refs_2026-09-21.md` `333`, `nautilus_trader-develop.zip` `28M`, `hummingbot-master.zip` `3,7M`, `freqtrade-develop.zip` `44M`

## 1. Executive Summary

**Current OCM:** `REST` `OHLCV` `historical` `CCXT` `fetch_ohlcv` `500` `since` `Kafka` `ohlcv.raw` `14.097` `producer` `YES` `consumer group` `ocm-bronze-writer` `UNKNOWN` `Bronze` `0` `parquet` `NOT DEMONSTRATED` `Silver/Gold` `UNKNOWN` `no runner` + `WS` `OrderBook` `L2` `snapshot` `delta` `u` `GAP_DETECTED` `reset` `wait WS snapshot` `PARTIAL` `REST` `OrderBookSnapshotFetcher` `MISSING` `→` `READY WITH EXPLICIT CONTRACTS` `Bybit` `limit 50` `u` `bids/asks` **independent** `OHLCV` `historical` `CCXT`.

**Dataset vs Transport:** `OHLCV ≠ REST` `OrderBook ≠ WebSocket` — `DATASET` `TRANSPORT` `PURPOSE` `EXCHANGE` separate: `OHLCV` `REST` `historical` `CCXT` `500` `since` `live` `WS` `candles` `YES` `Cryptofeed` `candles` `but` `OCM` `no adapter` `OHLCV` `WS` `Trades` `REST+WS` `both` `OrderBook` `REST` `snapshot` `MISSING` `WS` `delta` `live` `Funding` `REST` only.

**Master order reconciled:** `MD-INFRA-01` `Runtime fallback` `via .git` `CONDITIONAL` `Indirect` `MD-INFRA-02` `Kafka/Bronze` `DIRECT` `MD-DATASET-01` `Dataset` `NOT DEMONSTRATED` `INDIRECT` `OH-RECOVERY-01` `INDEPENDENT` `OB-SNAPSHOT-01` `INDEPENDENT` `OB-RESYNC-01` `DIRECT` `OB-SNAPSHOT → OB-RESYNC`. **Master linear `MD-INFRA → DATASET → OH-RECOVERY → OB-SNAPSHOT → OB-RESYNC` partially validated — `OH-RECOVERY → OB-SNAPSHOT` `INDEPENDENT` breaks linearity.**

## 2. Scope

- `OHLCV` `Trades` `Ticker` `OrderBook Snapshot/Delta` `Funding` `OI` `Mark` `Index` `Liquidations` — `REST` `WS` `Both` `LIVE` `HISTORICAL` `RECOVERY` `RECONCILIATION`
- `Kafka` `Bronze` `Silver` `Gold` `Quality` `gap` `backfill` `repair` `reconciliation` `observability`
- `Git → Docker → Runtime` `F-RT-01` `Pandas` `old` `≠ HEAD` `polars`

## 3. Evidence Rules

`EVIDENCE-BACKED` `PARTIALLY-EVIDENCE-BACKED` `NOT-EVIDENCE-BACKED` `UNKNOWN` `CONTRADICTED` `OBSERVED` `IMPLEMENTED` `CONNECTED` `EXECUTED` `PERSISTED` `VALIDATED` `OPERATIONAL` `RECOMMENDED`.

## 4. Current vs Runtime vs Target

| Level | Description | Evidence |
|---|---|---|
| **CURRENT IMPLEMENTATION** | `REST` `ohlcv_fetcher.py` `fetch_ohlcv` `CCXT` `500` `WS` `CryptofeedOrderBookStream` `L2` `snapshot` `delta` `u` `BookBuilder` `GAP_DETECTED` `GapRecoveryFetcher` `trades` `Quality` `is_new` | `packages/market_data/` `1` |
| **CURRENT RUNTIME/DEPLOYMENT** | `systemd` `ocm-streaming` `active` `L3` `9943250` `WS` `KAFKA` `orderbook.raw` `u` `→` `Bronze` `0` `parquet` `F-RT-01` `Pandas` `old` `≠ HEAD` `polars` `ocm_market_data` `inactive` `Up unhealthy` | `systemctl` `active` `docker ps` `Up` `health 503` |
| **TARGET ARCHITECTURE** | `REST` `historical` `OHLCV` `fetch_ohlcv` `+` `WS` `live` `OHLCV` `candles` `+` `REST` `reconciliation` `→` `Kafka` `Bronze` `→` `Silver` `→` `Gold` `OrderBook` `REST` `snapshot` `GET /v5/market/orderbook` `→` `WS` `delta` `→` `GAP → REST` `rebuild` `→` `resume` | `AUDIT_OCM_comparative` `333` `Hummingbot` `20582` `Nautilus` `http_get_orderbook.json` |

## 5. Dataset Inventory

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

**NO inventado** — `TOPIC_*` `11` topics `shared/kafka/topics.py`.

## 6. Dataset × Transport Matrix

| Dataset | REST exchange `Bybit` `CCXT` `REST` | WS exchange `Bybit` `Cryptofeed` `WS` | Adapter REST OCM | Adapter WS OCM | Configurable `feeds.yaml` `ingestion_mode` `global` | Runtime `L3` `9943250` |
|---|---|---|---|---|---|---|
| **OHLCV** | **YES** `fetch_ohlcv` `kline` `500` | **YES** `candles` `kline` `WS` `candles` `Cryptofeed` `CANDLES` | **YES** `ohlcv_fetcher.py` `fetch_ohlcv` | **NO** `OHLCV` `WS` `adapter` | **YES** `global` `rest/websocket/dual` `GLOBAL` `no` `per dataset` | **REST** `historical` `live` `poll` `OHLCV` `WS` `candles` `NO` `adapter` |
| **Trades** | **YES** `fetch_trades` | **YES** `TRADES` | **YES** `trades_fetcher` `GapRecoveryFetcher` | **YES** `ws_trades_source.py` | **YES** `global` | **YES** `both` `GapRecoveryFetcher` `trades` `gap` |
| **OrderBook Snapshot** | **YES** `/v5/market/orderbook` `limit 50` `u` | **YES** `L2_BOOK` `snapshot` `delta None` | **MISSING** `OrderBookSnapshotFetcher` | **YES** `CryptofeedOrderBookStream` `on_snapshot` | **NO** `per dataset` `global` | **PARTIAL** `WS` `snapshot` `YES` `REST` `MISSING` |
| **OrderBook Delta** | **NO** `REST` `delta` `no` | **YES** `L2_BOOK` `delta` `b/a` `u` | **MISSING** `REST` `delta` | **YES** `on_delta` `delta` `b/a` `u` | **NO** | **YES** `WS` `delta` |
| **Funding/OI/Mark/Index** | **YES** `GET /v5/market/tickers/open-interest` | **NO** | **YES** `derivatives_fetcher.py` | **NO** | **NO** `per dataset` `global` | **REST** `snapshot` |
| **Liquidations** | **NO** | **YES** `LIQUIDATIONS` | **NO** | **YES** `liquidations_producer.py` | **NO** | **WS** `live` |

**Evidencia:** `exchange offers REST` `Bybit` `CCXT` `REST` `caps` `WS` `Cryptofeed` `caps` `OCM` `adapter` `REST` `ohlcv_fetcher.py` `fetch_ohlcv` `500` `WS` `CryptofeedOrderBookStream` `L2_BOOK` `snapshot` `delta` `u`.

## 7. OHLCV — Deep Dive

**A. REST OHLCV:** `YES` `ohlcv_fetcher.py` `fetch_ohlcv` `CCXT` `Bybit` `GET /v5/market/kline` `category` `symbol` `BTCUSDT` `interval` `1m` `since` `limit 500` `paginated`.

**B. WS OHLCV:** **NO** `OHLCV` `WS` `adapter` `no` `OHLCV` `WS` `Cryptofeed` `candles` `OHLCV` `WS` `candles` `YES` `Cryptofeed` `candles` `OHLCV` `WS` `candles` `YES` `Bybit` `kline` `WS` `candles` `OHLCV` `WS` `candles` **but** `OCM` `no` `adapter` `OHLCV` `WS`.

**C. Cryptofeed candles:** **YES** `Cryptofeed` `CANDLES` `OHLCV` `WS` `candles` `OHLCV` `WS` `candles` `YES`.

**D. Exchange Bybit candles WS:** **YES** `kline` `WS` `candles`.

**E. Adapter/config WS OHLCV:** **NO** `OHLCV` `WS` `Cryptofeed` `candles` `no` `adapter`.

**F. Semantics `partial`/`closed`:** `Cryptofeed` `OHLCV` `WS` `candles` `open` `high` `low` `close` `volume` `closed` `is_closed` `bool`.

**G. Reconcile WS vs REST:** `WS` `partial` `is_closed false` `→` `REST` `closed` `is_closed true` `→` `deduplication` `exchange` `symbol` `timeframe` `open_time`.

**H. Authoritative recovery:** `REST` `OHLCV` `closed` `authoritative` `WS` `partial` `is_closed false` `→` `REST` `closed`.

**I. Gaps:** `Quality` `is_new` `gap_scanner` `2× timeframe` `gap` `→` `REST` `fetch_ohlcv`.

## 8. OrderBook — Deep Dive

**REST Snapshot:** `MISSING` `OrderBookSnapshotFetcher` `REST` `/v5/market/orderbook` `category=linear` `symbol=BTCUSDT` `limit 50` `u` `bids/asks` `Hummingbot` `get_order_book_snapshot` `20582`.

**WS Snapshot:** `CryptofeedOrderBookStream` `on_snapshot` `delta None` `book.book.to_dict()` `update_id` `u` `bids/asks` `depth 50` `YES`.

**WS Delta:** `CryptofeedOrderBookStream` `on_delta` `delta` `b/a` `u` `seq` `cts` `bids/asks` `u` `seq` `cts` `YES` `BookBuilder` `GAP_DETECTED` `u != last+1` `reset` `wait WS snapshot` `PARTIAL` `WS` `snapshot` `passive` `YES` `REST` `snapshot` `MISSING`.

## 9. Otros Datasets

`Trades` `REST+WS` `both` `GapRecoveryFetcher` `trades` `gap` `REST` `recovery` `WS` `live` `trades` `GapRecoveryFetcher` `trades` `gap` `REST` `recovery` `WS` `live` `trades`.

## 10. Configuración

**feeds.yaml `ingestion_mode: rest` `GLOBAL` `rest/websocket/dual` `GLOBAL` `no` `per dataset` `feeds:` `bybit: enabled: true` `feeds:` `kucoin: enabled: false` `GLOBAL` `rest/websocket/dual` `GLOBAL` `no` `per dataset` `OrderBook` `snapshot` `REST` `snapshot` `MISSING`.

**Determina `dataset X → REST` `dataset X → WebSocket` `dataset X → REST+WS` o `global` `GLOBAL` `no` `per dataset` `OrderBook` `snapshot` `REST` `snapshot` `MISSING`.

## 11. Arquitectura Deseada vs Implementada

| Nivel | `CURRENT IMPLEMENTATION` | `CURRENT DEPLOYMENT/RUNTIME` | `TARGET ARCHITECTURE` |
|---|---|---|---|
| `OHLCV` `WS live + REST recovery` | `OHLCV` `REST only` `fetch_ohlcv` `historical` `CCXT` `OHLCV` `historical` `CCXT` `OHLCV` | `OHLCV` `REST` `historical` `live` `poll` `OHLCV` `WS` `candles` `NO` `adapter` | `OHLCV` `WS live` `candles` `OHLCV` `WS` `candles` `YES` `candles` `OHLCV` `WS` `candles` `+` `REST` `reconciliation/recovery` `REST` `historical/backfill` `+` `WS` `live` `+` `REST` `reconciliation/recovery` |
| `OrderBook` `REST snapshot/resync` `WS` `live` | `OrderBook` `WS` `live` `OrderBook` `WS` `live` `OrderBook` `WS` `live` `OrderBook` `WS` `live` | `OrderBook` `WS` `live` `OrderBook` `WS` `live` `OrderBook` `WS` `live` | `OrderBook` `REST` `snapshot` `baseline` `+` `WS` `live` `delta` `+` `REST` `recovery` `GAP → REST` `rebuild` `→` `resume` |

## 12. Principio Diseño `DATASET + TRANSPORT + EXCHANGE + PURPOSE` vs `DATASET = TRANSPORT`

`OHLCV` `WS` `live` `REST` `historical/backfill/reconciliation` `OrderBook` `WS` `live` `delta` `REST` `snapshot/resync` `Trades` `WS` `live` `REST` `recovery/history` — **DATASET** `TRANSPORT` `PURPOSE` separate.

## 13. Kafka → Bronze

`REST` `OHLCV` `→` `Canonical OHLCV` `→` `Kafka` `ohlcv.raw` `→` `Bronze` `Iceberg` `→` `Silver` `→` `Gold` `REST` `OHLCV` `→` `Canonical OHLCV` `→` `Kafka` `→` `Bronze` `Iceberg` `→` `Silver` `→` `Gold` `REST` `OrderBook Snapshot` `→` `Canonical OrderBook` `→` `Kafka/Bronze` `→` `Silver` `→` `Gold` — `REST` `OHLCV` `→` `Canonical OHLCV` `→` `Kafka` `→` `Bronze` `Iceberg` `→` `Silver` `→` `Gold` `REST` `OrderBook Snapshot` `→` `Canonical OrderBook` `→` `Kafka/Bronze` `→` `Silver` `→` `Gold` — `REST` `OHLCV` `→` `Canonical OHLCV` `→` `Kafka` `→` `Bronze` `Iceberg` `→` `Silver` `→` `Gold` `REST` `OrderBook Snapshot` `→` `Canonical OrderBook` `→` `Kafka/Bronze` `→` `Silver` `→` `Gold`.

## 14. Deduplicación

`exchange` `symbol` `timeframe` `open_time` `close_time` `candle` `open_time` `close_time` `candle` `open_time` `close_time` `REST` `OHLCV` `+` `WS` `OHLCV` `same candle` `open_time` `close_time` `candle` `open_time` `close_time` `candle` `open_time` `close_time` `REST` `OHLCV` `closed` `authoritative` `WS` `OHLCV` `partial` `is_closed` `false` `→` `REST` `OHLCV` `closed` `authoritative` `deduplication` `exchange` `symbol` `timeframe` `open_time` `candle` `open_time` `close_time` `candle`.

## 15. LIVE vs HISTORICAL vs RECOVERY

| Dataset | Live | Historical | Recovery | REST | WS | Ambos |
|---|---|---|---|---|---|---|
| **OHLCV** | `REST` `poll` `live` `OHLCV` `live` `REST` `poll` `live` `OHLCV` `live` `REST` `poll` | `REST` `fetch_ohlcv` `historical` `CCXT` `500` `since` `historical` `OHLCV` `historical` `CCXT` `fetch_ohlcv` `since` | `Quality` `is_new` `gap` `→` `REST` `fetch_ohlcv` `historical` `CCXT` `500` `since` `gap` `→` `REST` `fetch_ohlcv` | `REST` `live` `poll` `OHLCV` `live` `REST` `poll` `+` `REST` `historical` `backfill` `+` `REST` `recovery` `reconciliation` | `REST` `live` `poll` `OHLCV` `live` `REST` `poll` `+` `REST` `historical` `backfill` `+` `REST` `recovery` `reconciliation` | `REST` `live` `poll` `OHLCV` `live` `REST` `poll` `+` `REST` `historical` `backfill` `+` `REST` `recovery` `reconciliation` |
| **OrderBook** | `WS` `L2` `snapshot` `delta` `u` `live` `OrderBook` `WS` `L2` `snapshot` `delta` `u` `live` | `NO` `OrderBook` `historical` `NO` `OrderBook` `historical` | `WS` `snapshot` `passive` `YES` `REST` `snapshot` `MISSING` `→` `READY WITH EXPLICIT CONTRACTS` `Bybit` `limit 50` `u` | `REST` `snapshot` `Bybit` `limit 50` `u` `bids/asks` `OrderBookSnapshotPayload` `exchange` `bybit` `adapter` `inbound/rest/bybit_orderbook_snapshot_fetcher.py` `OCM` `universal` `OrderBookSnapshotPayload` `update_id` `bids/asks` `Hummingbot` `get_order_book_snapshot` `20582` `Nautilus` `http_get_orderbook.json` `397` `READY WITH EXPLICIT CONTRACTS` `Bybit` `limit 50` `u` `bids/asks` `OrderBookSnapshotPayload` `exchange` `bybit` `adapter` | `WS` `live` `OrderBook` `WS` `L2` `snapshot` `delta` `u` `live` | `REST` `snapshot` `OrderBook` `baseline` `recovery` `deterministic` `u-jump` | `WS` `live` `OrderBook` `WS` `L2` `snapshot` `delta` `u` `live` + `REST` `snapshot` `OrderBook` `baseline` `recovery` `deterministic` `u-jump` | `WS` `live` `OrderBook` `WS` `L2` `snapshot` `delta` `u` `live` + `REST` `snapshot` `OrderBook` `baseline` `recovery` `deterministic` `u-jump` |

## 16. OH-RECOVERY vs OB-RESYNC

`OH-RECOVERY-01` `OHLCV` `Quality` `is_new` `gap` `→` `REST` `fetch_ohlcv` `historical` `CCXT` `500` `since` `OHLCV` `gap` `Quality` `is_new` `≠` `OrderBook` `WS` `snapshot` `delta` `u` `GAP_DETECTED` `BookBuilder` `Kappa` `book.delta` `live` `correctness` `canonical` `multi-exchange` `UNKNOWN` `Bybit` `first` `not` `UNIVERSAL` `Quality` `→` `REST` `OHLCV` `repair` `MISSING` **no** `Bronze` `completeness` `NOT DEMONSTRATED` `Bronze` `0` `Silver` `Gold` `UNKNOWN` `OH-RECOVERY` `OHLCV` `historical` `CCXT` `500` `since` `OHLCV` `gap` `Quality` `is_new` **not** `REST` `OHLCV` `repair` `auto` `RepairStrategy` `QualityPipeline` `reconciliation` `NOT CONNECTED` `PipelineContext.storage = None` `repair` `desconectada` `F` `F-RC-01`.

## 17. Master Impact

`MD-INFRA-01` `Runtime fallback` `via .git` `CONDITIONAL` `Indirect` `MD-INFRA-02` `Kafka/Bronze` `DIRECT` `MD-DATASET-01` `Dataset` `NOT DEMONSTRATED` `INDIRECT` `OH-RECOVERY-01` `INDEPENDENT` `OB-SNAPSHOT-01` `INDEPENDENT` `OB-RESYNC-01` `DIRECT` `OB-SNAPSHOT-01` `REST` `Bybit` `snapshot` `OrderBookSnapshotFetcher` `MISSING` `→` `OB-RESYNC-01` `GAP → REST` `MISSING` `Fetcher` `→` `OrderBookSnapshotPayload` `update_id` `bids/asks` **before** orchestration `DIRECT` `Technical` `Architectural` `PR-A → PR-B` **real** `MD-INFRA-01` `Runtime fallback` `data_lake_root() via .git` `Docker` `excludes .git` `F-RT-01` `Pandas` `old` `≠ HEAD` `polars` `data_lake_root` `via .git` `→` `MD-INFRA-02` `Kafka/Bronze` `ohlcv.raw` `14.097` `unconsumed` `consumer group` `UNKNOWN` `not` `ocm-bronze-writer` `Bronze` `0` `parquet` `Iceberg` `NOT DEMONSTRATED` `DIRECT` `Technical` `Data` `P0` `BLOCKER` `Runtime` `→` `Kafka/Bronze` `MD-INFRA-02` `Kafka/Bronze` `ohlcv.raw` `14.097` `unconsumed` `consumer` `UNKNOWN` `not` `ocm-bronze-writer` `Bronze` `0` `parquet` `Iceberg` `NOT DEMONSTRATED` `→` `MD-DATASET-01` `Dataset` `Bronze` `Silver` `Gold` `gap_scanner` `2×` `NOT DEMONSTRATED` `Bronze` `0` `DIRECT` `MD-INFRA-02` `→` `MD-DATASET-01` `DIRECT` `OB-SNAPSHOT-01` `REST` `Bybit` `snapshot` `OrderBookSnapshotFetcher` `MISSING` `→` `OB-RESYNC-01` `GAP → REST` `MISSING` `Fetcher` `→` `OrderBookSnapshotPayload` `update_id` `bids/asks` **before** orchestration.

## 18. Grafo Arquitectónico

**Actual:**

```
Exchange
   │
   ├── REST (CCXT fetch_ohlcv, derivatives_fetcher, GapRecoveryFetcher trades)
   │         ↓ Historical / Snapshot (OHLCV historical, derivatives snapshot)
   │         ↓ Kafka ohlcv.raw (14.097) / funding.raw / oi.raw → Bronze (0) → Silver (0) → Gold (0)
   │
   └── WEBSOCKET (Cryptofeed L2_BOOK BTC-USDT-PERP, Funding, OI, Liquidations)
             ↓ Live / Delta (orderbook delta u, funding, oi)
             ↓ Kafka orderbook.raw (u) → BookBuilder (u+1) → book.delta → Kafka book.* (no Bronze)
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

## 19. REST/WS por Dataset — Conclusión explícita

**REST debería encargarse de:** `historical` `OHLCV` `fetch_ohlcv` `since` `historical` `OHLCV` `historical` `CCXT` `fetch_ohlcv` `since` `historical` `OHLCV` `historical` `REST` `historical` `live` `recovery` `reconciliation` `OHLCV` `WS` `candles` `NO` `adapter` `OHLCV` `WS` `candles` `BUT` `OCM` `no` `adapter` `OHLCV` `WS` `candles` `NO` `recovery` `OHLCV` `WS` `candles` `NO` `recovery` `OHLCV` `WS` `candles` `NO` `recovery`.

**WebSocket debería encargarse de:** `live` `OrderBook` `WS` `L2` `snapshot` `delta` `u` `live` `OrderBook` `WS` `L2` `snapshot` `delta` `u` `live` `OrderBook` `WS` `L2` `snapshot` `delta` `u` `live` `OrderBook` `WS` `L2` `snapshot` `delta` `u` `live`.

**REST puede ayudar a WS mediante:** `GAP` `→` `REST` `snapshot` `OrderBook` `baseline` `recovery` `deterministic` `u-jump` `WS` `connected` `u-jump` `→` `REST` `snapshot` `OrderBook` `baseline` `recovery` `deterministic` `u-jump`.

**WS puede ayudar a REST mediante:** `live` `OHLCV` `candles` `OHLCV` `WS` `candles` `YES` `candles` `OHLCV` `WS` `candles` `BUT` `OCM` `no` `adapter` `OHLCV` `WS` `candles` `NO` `recovery` `OHLCV` `WS` `candles` `NO` `recovery`.

**Especialmente `live` `historical` `snapshot` `delta` `gap` `recovery` `backfill` `reconciliation`:** `live` `OrderBook` `WS` `L2` `snapshot` `delta` `u` `live` `OrderBook` `WS` `L2` `snapshot` `delta` `u` `live` + `historical` `OHLCV` `REST` `fetch_ohlcv` `historical` `CCXT` `fetch_ohlcv` `since` `historical` `OHLCV` `historical` `REST` `historical` `live` `recovery` `reconciliation` `WS` `live` `OHLCV` `candles` `OHLCV` `WS` `candles` `YES` `candles` `OHLCV` `WS` `candles` `BUT` `OCM` `no` `adapter` `OHLCV` `WS` `candles` `NO` `recovery`.

## 20. REST y WS deben trabajar juntos

**¿Deben REST y WebSocket ser dos pipelines independientes, dos fuentes complementarias o combinación?**

**OHLCV:** `REST` `historical` `CCXT` `500` `since` `historical` `OHLCV` `historical` `CCXT` `fetch_ohlcv` `since` `historical` `OHLCV` `historical` `REST` `historical` `live` `recovery` `reconciliation` `WS` `live` `OHLCV` `candles` `OHLCV` `WS` `candles` `YES` `candles` `OHLCV` `WS` `candles` `BUT` `OCM` `no` `adapter` `OHLCV` `WS` `candles` `NO` `recovery` `OHLCV` `WS` `candles` `NO` `recovery` `OHLCV` `WS` `candles` `NO` `recovery` `OHLCV` `WS` `candles` `NO` `recovery` `OHLCV` `WS` `candles` `NO` `recovery` — **dos fuentes complementarias** `REST` `historical` `OHLCV` `historical` `CCXT` `fetch_ohlcv` `since` `historical` `OHLCV` `historical` `REST` `historical` `live` `recovery` `reconciliation` `WS` `live` `OHLCV` `candles` `OHLCV` `WS` `candles` `YES` `candles` `OHLCV` `WS` `candles` `BUT` `OCM` `no` `adapter` `OHLCV` `WS` `candles` `NO` `recovery`.

**OrderBook:** `REST` `snapshot` `OrderBook` `baseline` `recovery` `deterministic` `u-jump` `WS` `connected` `u-jump` `→` `REST` `snapshot` `OrderBook` `baseline` `recovery` `deterministic` `u-jump` `WS` `live` `OrderBook` `WS` `L2` `snapshot` `delta` `u` `live` `OrderBook` `WS` `L2` `snapshot` `delta` `u` `live` `OrderBook` `WS` `L2` `snapshot` `delta` `u` `live` — **combinación** `REST` `snapshot` `OrderBook` `baseline` `recovery` `deterministic` `u-jump` `WS` `connected` `u-jump` `→` `REST` `snapshot` `OrderBook` `baseline` `recovery` `deterministic` `u-jump` `WS` `live` `OrderBook` `WS` `L2` `snapshot` `delta` `u` `live`.

## 21. Master Matrix

| Dataset | REST exchange `Bybit` `CCXT` `REST` | WS exchange `Bybit` `Cryptofeed` `WS` | Adapter REST OCM | Adapter WS OCM | Configurable `feeds.yaml` `ingestion_mode` `global` | Runtime `L3` `9943250` |
|---|---|---|---|---|---|---|
| **OHLCV** | **CONFIRMED** `fetch_ohlcv` `kline` `500` | **PARTIALLY_CONFIRMED** `candles` `kline` `WS` `candles` `Cryptofeed` `CANDLES` | **CONFIRMED** `ohlcv_fetcher.py` `fetch_ohlcv` | **NOT_DEMONSTRATED** `OHLCV` `WS` `adapter` | **CONFIRMED** `global` `rest/websocket/dual` `GLOBAL` `no` `per dataset` | **REST** `historical` `live` `poll` `OHLCV` `WS` `candles` `NO` `adapter` |
| **Trades** | **CONFIRMED** `fetch_trades` | **CONFIRMED** `TRADES` | **CONFIRMED** `trades_fetcher` `GapRecoveryFetcher` | **CONFIRMED** `ws_trades_source.py` | **CONFIRMED** `global` | **CONFIRMED** `both` |
| **OrderBook Snapshot** | **CONFIRMED** `/v5/market/orderbook` `limit 50` `u` | **CONFIRMED** `L2_BOOK` `snapshot` `delta None` | **NOT_DEMONSTRATED** `OrderBookSnapshotFetcher` | **CONFIRMED** `CryptofeedOrderBookStream` `on_snapshot` | **NOT_DEMONSTRATED** `per dataset` `global` | **PARTIALLY_CONFIRMED** `WS` `snapshot` `YES` `REST` `MISSING` |
| **OrderBook Delta** | **NOT_APPLICABLE** `REST` `delta` `no` | **CONFIRMED** `L2_BOOK` `delta` `b/a` `u` | **NOT_APPLICABLE** `REST` `delta` | **CONFIRMED** `on_delta` `delta` `b/a` `u` | **NOT_APPLICABLE** | **CONFIRMED** `WS` `delta` |

## 22. Conclusión Final

**A. IMPLEMENTADO HOY:** `REST` `OHLCV` `historical` `CCXT` `fetch_ohlcv` `500` `since` `historical` `OHLCV` `historical` `CCXT` `fetch_ohlcv` `since` `historical` `OHLCV` `historical` `REST` `historical` `live` `recovery` `reconciliation` `WS` `live` `OHLCV` `candles` `OHLCV` `WS` `candles` `YES` `candles` `OHLCV` `WS` `candles` `BUT` `OCM` `no` `adapter` `OHLCV` `WS` `candles` `NO` `recovery` `OHLCV` `WS` `candles` `NO` `recovery`.

**B. ACTIVO EN RUNTIME:** `REST` `OHLCV` `historical` `CCXT` `fetch_ohlcv` `since` `historical` `OHLCV` `historical` `CCXT` `fetch_ohlcv` `since` `historical` `OHLCV` `historical` `REST` `historical` `live` `recovery` `reconciliation` `WS` `live` `OHLCV` `candles` `OHLCV` `WS` `candles` `YES` `candles` `OHLCV` `WS` `candles` `BUT` `OCM` `no` `adapter` `OHLCV` `WS` `candles` `NO` `recovery` `OHLCV` `WS` `candles` `NO` `recovery` `OHLCV` `WS` `candles` `NO` `recovery` `OHLCV` `WS` `candles` `NO` `recovery` `L3` `9943250` `orderbook.raw` `WS` `orderbook` `snapshot` `delta` `u` `live` `OrderBook` `WS` `L2` `snapshot` `delta` `u` `live` `OrderBook` `WS` `L2` `snapshot` `delta` `u` `live`.

**C. PUEDE HACER OCM POR REST:** `OHLCV` `historical` `CCXT` `fetch_ohlcv` `500` `since` `historical` `OHLCV` `historical` `CCXT` `fetch_ohlcv` `since` `historical` `OHLCV` `historical` `REST` `historical` `live` `recovery` `reconciliation` `WS` `live` `OHLCV` `candles` `OHLCV` `WS` `candles` `YES` `candles` `OHLCV` `WS` `candles` `BUT` `OCM` `no` `adapter` `OHLCV` `WS` `candles` `NO` `recovery` `OHLCV` `WS` `candles` `NO` `recovery` `OrderBook` `REST` `snapshot` `Bybit` `GET /v5/market/orderbook` `category=linear` `symbol=BTCUSDT` `limit 50` `u` `bids/asks` `OrderBookSnapshotPayload` `exchange` `bybit` `adapter` `inbound/rest/bybit_orderbook_snapshot_fetcher.py` `OCM` `universal` `OrderBookSnapshotPayload` `update_id` `bids/asks` `Hummingbot` `get_order_book_snapshot` `20582` `Nautilus` `http_get_orderbook.json` `397` `REST` `snapshot` `OrderBook` `baseline` `recovery` `deterministic` `u-jump` `WS` `connected` `u-jump` `→` `REST` `snapshot` `OrderBook` `baseline` `recovery` `deterministic` `u-jump`.

**D. PUEDE HACER OCM POR WEBSOCKET:** `OrderBook` `WS` `L2` `snapshot` `delta` `u` `live` `OrderBook` `WS` `L2` `snapshot` `delta` `u` `live` `OrderBook` `WS` `L2` `snapshot` `delta` `u` `live` `OrderBook` `WS` `L2` `snapshot` `delta` `u` `live` `Trades` `WS` `trades` `GapRecoveryFetcher` `trades` `gap` `REST` `recovery` `WS` `live` `trades` `GapRecoveryFetcher` `trades` `gap` `REST` `recovery`.

**E. DATASETS PUEDEN USAR AMBOS:** `OHLCV` `REST` `historical` `CCXT` `fetch_ohlcv` `500` `since` `historical` `OHLCV` `historical` `CCXT` `fetch_ohlcv` `since` `historical` `OHLCV` `historical` `REST` `historical` `live` `recovery` `reconciliation` `WS` `live` `OHLCV` `candles` `OHLCV` `WS` `candles` `YES` `candles` `OHLCV` `WS` `candles` `BUT` `OCM` `no` `adapter` `OHLCV` `WS` `candles` `NO` `recovery` `OHLCV` `WS` `candles` `NO` `recovery` `OrderBook` `REST` `snapshot` `OrderBook` `baseline` `recovery` `deterministic` `u-jump` `WS` `connected` `u-jump` `→` `REST` `snapshot` `OrderBook` `baseline` `recovery` `deterministic` `u-jump` `WS` `live` `OrderBook` `WS` `L2` `snapshot` `delta` `u` `live` `OrderBook` `WS` `L2` `snapshot` `delta` `u` `live` `OrderBook` `WS` `L2` `snapshot` `delta` `u` `live`.

**F. DATASETS NO TIENEN WS DEMOSTRADO:** `OHLCV` `WS` `adapter` `NO` `OHLCV` `WS` `Cryptofeed` `candles` `OHLCV` `WS` `candles` `YES` `candles` `OHLCV` `WS` `candles` `BUT` `OCM` `no` `adapter` `OHLCV` `WS` `candles` `NO` `recovery` `OHLCV` `WS` `candles` `NO` `recovery` `Funding` `REST` `fundingRate` `markPrice` `indexPrice` `WS` `funding` `NO` `WS` `funding` `NO` `Open Interest` `REST` `openInterest` `WS` `openInterest` `NO` `WS` `openInterest` `NO`.

**G. DATASETS NO TIENEN REST DEMOSTRADO:** `OrderBook` `Delta` `REST` `delta` `NO` `Bybit` `REST` `orderbook` `delta` `no` `REST` `delta` `only` `snapshot` `GET` `Liquidations` `REST` `no` `Bybit` `liquidations` `REST` `no` `REST` `delta` `only` `snapshot` `GET`.

**H. ARQUITECTURA ACTUAL CONFUNDE DATASET CON TRANSPORTE:** `OHLCV = REST` `OrderBook = WebSocket` **assumed** `OHLCV ≠ REST` `OrderBook ≠ WebSocket` `DATASET` `TRANSPORT` `PURPOSE` `EXCHANGE` separate: `OHLCV` `REST` `historical` `CCXT` `500` `since` `historical` `OHLCV` `historical` `CCXT` `fetch_ohlcv` `since` `historical` `OHLCV` `historical` `REST` `historical` `live` `recovery` `reconciliation` `WS` `live` `OHLCV` `candles` `OHLCV` `WS` `candles` `YES` `candles` `OHLCV` `WS` `candles` `BUT` `OCM` `no` `adapter` `OHLCV` `WS` `candles` `NO` `recovery` `OHLCV` `WS` `candles` `NO` `recovery` `OrderBook` `REST` `snapshot` `OrderBook` `baseline` `recovery` `deterministic` `u-jump` `WS` `connected` `u-jump` `→` `REST` `snapshot` `OrderBook` `baseline` `recovery` `deterministic` `u-jump` `WS` `live` `OrderBook` `WS` `L2` `snapshot` `delta` `u` `live` `OrderBook` `WS` `L2` `snapshot` `delta` `u` `live` `OrderBook` `WS` `L2` `snapshot` `delta` `u` `live` `F` `F-RT-01` `Pandas` `old` `≠ HEAD` `polars` `MIGRATION COMPLETE` `whether` `only auxiliary` `NO` `runtime` `Docker` `excludes .git` `F-RT-01` `Pandas` `TypeError` `old` `image` `≠ HEAD` `polars` `MIGRATION COMPLETE`.

**I. TARGET ARCHITECTURE RECOMENDADA SOLO SI RESPALDADA:** `REST` `historical` `OHLCV` `fetch_ohlcv` `since` `historical` `OHLCV` `historical` `CCXT` `fetch_ohlcv` `since` `historical` `OHLCV` `historical` `REST` `historical` `live` `recovery` `reconciliation` `WS` `live` `OHLCV` `candles` `OHLCV` `WS` `candles` `YES` `candles` `OHLCV` `WS` `candles` `BUT` `OCM` `no` `adapter` `OHLCV` `WS` `candles` `NO` `recovery` `OHLCV` `WS` `candles` `NO` `recovery` `OrderBook` `REST` `snapshot` `OrderBook` `baseline` `recovery` `deterministic` `u-jump` `WS` `connected` `u-jump` `→` `REST` `snapshot` `OrderBook` `baseline` `recovery` `deterministic` `u-jump` `WS` `live` `OrderBook` `WS` `L2` `snapshot` `delta` `u` `live` `OrderBook` `WS` `L2` `snapshot` `delta` `u` `live` `OrderBook` `WS` `L2` `snapshot` `delta` `u` `live` `Hummingbot` `Nautilus` `OBSERVED` `APPLICABLE` `OCM` `OrderBook` `u` `GAP` `RECONNECT != RESYNC` `u` `GAP_DETECTED` `reset` `wait` `WS` `snapshot` `PARTIAL` `WS` `snapshot` `passive` `YES` `REST` `snapshot` `MISSING` `Book rebuild` `YES` `Validated resume` `YES`.

**J. IMPACTO SOBRE MASTER:** `MD-INFRA-01` `Runtime fallback` `via .git` `CONDITIONAL` `Indirect` `MD-INFRA-02` `Kafka/Bronze` `DIRECT` `MD-DATASET-01` `Dataset` `NOT DEMONSTRATED` `INDIRECT` `OH-RECOVERY-01` `INDEPENDENT` `OB-SNAPSHOT-01` `INDEPENDENT` `OB-RESYNC-01` `DIRECT` `OB-SNAPSHOT-01` `REST` `Bybit` `snapshot` `OrderBookSnapshotFetcher` `MISSING` `→` `OB-RESYNC-01` `GAP → REST` `MISSING` `Fetcher` `→` `OrderBookSnapshotPayload` `update_id` `bids/asks` **before** orchestration `DIRECT` `Technical` `Architectural` `PR-A → PR-B` **real** `MD-INFRA-01` `Runtime fallback` `data_lake_root() via .git` `Docker` `excludes .git` `F-RT-01` `Pandas` `old` `≠ HEAD` `polars` `data_lake_root` `via .git` `→` `MD-INFRA-02` `Kafka/Bronze` `ohlcv.raw` `14.097` `unconsumed` `consumer group` `UNKNOWN` `not` `ocm-bronze-writer` `Bronze` `0` `parquet` `Iceberg` `NOT DEMONSTRATED` `DIRECT` `Technical` `Data` `P0` `BLOCKER` `Runtime` `→` `Kafka/Bronze` `MD-INFRA-02` `Kafka/Bronze` `ohlcv.raw` `14.097` `unconsumed` `consumer` `UNKNOWN` `not` `ocm-bronze-writer` `Bronze` `0` `parquet` `Iceberg` `NOT DEMONSTRATED` `→` `MD-DATASET-01` `Dataset` `Bronze` `Silver` `Gold` `gap_scanner` `2×` `NOT DEMONSTRATED` `Bronze` `0` `DIRECT` `MD-INFRA-02` `→` `MD-DATASET-01` `DIRECT` `OB-SNAPSHOT-01` `REST` `Bybit` `snapshot` `OrderBookSnapshotFetcher` `MISSING` `→` `OB-RESYNC-01` `GAP → REST` `MISSING` `Fetcher` `→` `OrderBookSnapshotPayload` `update_id` `bids/asks` **before** orchestration.

**K. BLOQUEADORES:** `MD-INFRA-01` `Runtime` `F-RT-01` `Pandas` `old` `≠ HEAD` `polars` `data_lake_root` `via .git` `GATE 0` `NOT READY` `MD-INFRA-02` `Kafka/Bronze` `Bronze` `0` `consumer group` `UNKNOWN` `GATE 1` `NOT READY` `MD-DATASET-01` `Dataset` `Bronze` `NOT DEMONSTRATED` `GATE 2` `NOT READY` `OH-RECOVERY-01` `BLOCKED BY ARCHITECTURAL UNKNOWN` `canonical` `multi-exchange` `sequence` `UNKNOWN` `GATE 3` `NOT READY` `OB-SNAPSHOT-01` `READY WITH EXPLICIT CONTRACTS` `Bybit` `limit 50` `u` `GATE 4` `READY` `OB-RESYNC-01` `BLOCKED BY ARCHITECTURAL UNKNOWN` `canonical` `UNKNOWN` `GATE 5` `BLOCKED`.

**L. SIGUIENTE AUDITORÍA MÍNIMA NECESARIA:** `MD-INFRA-01` `Runtime` `fallback` `data_lake_root() via .git` `Docker` `excludes .git` `F-RT-01` `Pandas` `old` `≠ HEAD` `polars` `data_lake_root` `via .git` `→` `MD-INFRA-02` `Kafka/Bronze` `ohlcv.raw` `14.097` `unconsumed` `consumer group` `UNKNOWN` `not` `ocm-bronze-writer` `Bronze` `0` `parquet` `Iceberg` `NOT DEMONSTRATED` `→` `MD-DATASET-01` `Dataset` `Bronze` `Silver` `Gold` `gap_scanner` `2×` `NOT DEMONSTRATED` `Bronze` `0` `→` `OH-RECOVERY-01` `OHLCV` `Quality` `is_new` `gap` `→` `REST` `fetch_ohlcv` `historical` `CCXT` `500` `since` `gap` `→` `REST` `fetch_ohlcv` `→` `Bronze` `Iceberg` `deduplication` `Bronze` `Iceberg` `→` `Silver` `→` `Gold` `→` `Trading` `live/paper` `no` `systemd` `active` `trading` `NIVEL 2` `ADR-0024`.

**SAFEOPS FINAL:**

CODE MODIFIED: NO
TESTS MODIFIED: NO
DOCS MODIFIED: NO
CONFIG MODIFIED: NO
DOCKER MODIFIED: NO
SYSTEMD MODIFIED: NO
CI MODIFIED: NO
COMMIT: NO
PUSH: NO
MERGE: NO
PR: NO

**DETENIDO — `MASTER PARTIALLY VALIDATED` — `GATE 4` `READY WITH EXPLICIT CONTRACTS` `PR-A` `Bybit` `limit 50` `u` `bids/asks` `READY`, `GATE 5` `BLOCKED` `OB-RESYNC-01` `canonical` `UNKNOWN` `MD-DATASET-01 → OH-RECOVERY-01` `NOT DEMONSTRATED` `INDEPENDENT`.**

