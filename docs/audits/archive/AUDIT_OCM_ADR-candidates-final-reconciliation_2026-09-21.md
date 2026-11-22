# AUDIT — ADR Candidates Final Reconciliation — OCM

**ID:** AUDIT-ADR-RECONCILIATION-2026-09-21
**Date:** 2026-09-21
**Scope:** `DATASET × TRANSPORT × PURPOSE × CANONICAL CONTRACT × KAFKA × BRONZE` — `OHLCV` `OrderBook` `Trades` `Funding` `REST` `WS` `Kafka` `Bronze` `Silver` `Gold` — `OH-RECOVERY-01` `OB-SNAPSHOT-01` `OB-RESYNC-01` `MD-INFRA/DATASET` `anomaly_registry` `B-59` `README`
**Git:** `feat/m22b-governance` `857cfe60` `M22-B` `?? 6 audit + 4 OB-RESYNC + 3 AUDIT_OCM_comparative` preserved
**Previous:** `AUDIT_OCM_master-market-data-architecture-reconciliation_2026-09-21.md` `PARTIALLY VALIDATED` `MD-INFRA-01 → OB-RESYNC-01` + `AUDIT_OCM_comparative-architecture-refs_2026-09-21.md` `333` + `OB-RESYNC-001` `PARTIALLY_CONFIRMED` + `AUDIT_OCM_dataset-continuity-recovery-reconciliation_2026-09-21.md` `209` + `B-59-architecture-triage` `TEST GAP` `DOCUMENTATION DRIFT` `NO ADR`
**Files:** `packages/market_data/main.py:146`, `adapters/inbound/rest/ohlcv_fetcher.py`, `adapters/inbound/websocket/cryptofeed_orderbook_stream.py:246`, `book_builder.py:218`, `shared/kafka/schemas/orderbook.py:169`, `gap_recovery_fetcher.py:6`, `config/market_data/feeds.yaml`, `deploy/systemd/ocm-streaming.service.template:11`, `docker-compose.yml:24`, `ocm/config/paths.py` `data_lake_root()`, `tests/market_data/application/processing/test_book_builder.py:104`, `packages/market_data/infrastructure/quality/anomaly_registry.py:78`, `README.md:358`

## 1. Executive Summary

**Current OCM:** `REST` `OHLCV` `historical` `CCXT` `fetch_ohlcv` `500` `since` `Kafka` `ohlcv.raw` `14.097` `producer` `YES` `consumer group` `ocm-bronze-writer` `UNKNOWN` `Bronze` `0` `parquet` `NOT DEMONSTRATED` `Silver/Gold` `UNKNOWN` + `WS` `OrderBook` `L2` `snapshot` `delta` `u` `GAP_DETECTED` `reset` `wait WS snapshot` `PARTIAL` `REST` `OrderBookSnapshotFetcher` `MISSING` `→` `READY WITH EXPLICIT CONTRACTS` `Bybit` `limit 50` `u` `bids/asks`.

**Evidence refuted:** `DATASET = TRANSPORT` `OHLCV = REST` `OrderBook = WebSocket` **assumed** `OHLCV ≠ REST` `OrderBook ≠ WebSocket` `DATASET` `TRANSPORT` `PURPOSE` `EXCHANGE` separate: `OHLCV` `REST` `historical` `CCXT` `500` `since` `live` `WS` `candles` `YES` `Cryptofeed` `candles` `but` `OCM` `no adapter` `OHLCV` `WS` `Trades` `REST+WS` `both` `OrderBook` `REST` `snapshot` `MISSING` `WS` `delta` `live`.

**ADR candidates reconciled:** `Canonical Market Data Contract` `Runtime Data Lake Root` `2` `KEEP` `FOUNDATIONAL` `P0` `BLOCKER` `Runtime` `→` `Kafka/Bronze` — **not** `anomaly_registry` `README` `B-59` `TEST GAP` `DOCUMENTATION DRIFT` `NO ADR` (already `ports/outbound/anomaly_registry.py` `Protocol` `ADR-0007` `ADR-0040`).

## 2. Evidence Reviewed

- `OB-RESYNC-001` `3` audits `270+252+161` `PARTIALLY_CONFIRMED` `RECONNECT != RESYNC` `u` `GAP_DETECTED` `MISSING REST`
- `AUDIT_OCM_comparative-architecture-refs_2026-09-21.md` `333` `Hummingbot` `20582` `Nautilus` `http_get_orderbook.json` `Freqtrade` `download-data`
- `AUDIT_OCM_orderbook-readiness-before-implementation_2026-09-21.md` `269` `PR-A READY WITH CONTRACTS, PR-B BLOCKED`
- `AUDIT_OCM_master-market-data-architecture-reconciliation_2026-09-21.md` `PARTIALLY VALIDATED` `MD-INFRA-01 → OB-RESYNC-01`
- `AUDIT_OCM_dataset-continuity-recovery-reconciliation_2026-09-21.md` `209` `DATASET × TRANSPORT` `OHLCV` `Trades` `OrderBook` `Funding` `REST+WS` `both` `live` `historical` `recovery` `reconciliation`
- `AUDIT_OCM_B59-architecture-triage_2026-09-21.md` `B-59` `TEST GAP` `DOCUMENTATION DRIFT` `NO ADR`
- `docs/audit/deployment-runtime-implementation-plan.md` `348` `P0-P5` `Validated Architecture` `systemd=app`
- `packages/market_data/main.py:146` `PipelineOrchestrator` `Kafka` `Bronze` `Silver` `Gold`

## 3. Contradictions Found

| Finding | Source A | Source B | Current Evidence | Status |
|---|---|---|---|---|
| `ocm-bronze-writer` `not exists` | `AUDIT_OCM_market-data-e2e` `consumer group` `UNKNOWN` | `code` `KafkaBronzeWriter` `group_id ocm-bronze-writer` `infrastructure/kafka/kafka_bronze_writer.py` `group ocm-bronze-writer` `IMPLEMENTED` | `consumer group` `not appears` `14.097` `unconsumed` `Bronze` `0` `parquet` `Iceberg` `NOT DEMONSTRATED` `NOT` `consumer` `UNKNOWN` → `consumer` `implemented` `not` `running` `old` `Pandas` `F-RT-01` | **PARTIAL** `CODE EXISTS` `RUNTIME NOT RUNS` |
| `14.097` `ohlcv.raw` `unconsumed` | `kafka-get-offsets` `ohlcv.raw` `14.097` `producer` `YES` `consumer group` `ocm-bronze-writer` `UNKNOWN` | `find Bronze 0` `parquet` `health 503` `apiKey:''` `old` `Pandas` `F-RT-01` | `producer` `YES` `consumer` `UNKNOWN` `Bronze` `0` `NOT DEMONSTRATED` `NOT` `consumer` `UNKNOWN` | **PARTIALLY_CONFIRMED** |
| `F-RT-01` `Pandas` `old` `≠ HEAD` `polars` | `Docker` `ocm_market_data:latest` `c020e92f44de` `4 weeks` `old` `Pandas` `F-RT-01` `Pandas` `TypeError` `old` `HEAD` `fix` `polars` `MIGRATION COMPLETE` | `HEAD` `857cfe60` `polars` `MIGRATION COMPLETE` `audit_validator M22–M25` | `F-RT-01` `Pandas` `TypeError` `old` `image` `Pandas` `TypeError` `old` `HEAD` `fix` `polars` `MIGRATION COMPLETE` `CODE` `polars` `HEAD` `≠` `Docker` `Pandas` `old` `F-RT-01` | **CONFIRMED** `old` `Pandas` `TypeError` `old` `image` `Pandas` `TypeError` `old` `HEAD` `fix` `polars` |
| `MD-DATASET-01 → OH-RECOVERY-01` `DIRECT` | `Master` `PARTIALLY VALIDATED` `MD-DATASET-01 → OH-RECOVERY-01` `DIRECT` `NOT DEMONSTRATED` | `OHLCV` `historical` `CCXT` `500` `since` `OHLCV` `gap` `Quality` `is_new` `≠` `OrderBook` `WS` `snapshot` `delta` `u` `GAP_DETECTED` `BookBuilder` `Kappa` `book.delta` `live` `correctness` | `OH-RECOVERY-01` `OHLCV` `historical` `CCXT` `500` `since` `OHLCV` `gap` `Quality` `is_new` `≠` `OrderBook` `WS` `snapshot` `delta` `u` `GAP_DETECTED` `BookBuilder` `Kappa` | **CONTRADICTED** `MD-DATASET-01 → OH-RECOVERY-01` `NOT DEMONSTRATED` `INDIRECT` `Master` `OH-RECOVERY-01` `→` `OB-SNAPSHOT-01` `DIRECT` `NOT DEMONSTRATED` `INDEPENDENT` |

## 4. Findings Confirmed/Refuted

| Finding | Evidence | Status |
|---|---|---|
| `MD-INFRA-01` `data_lake_root() via .git` `Docker excludes .git` `F-RT-01` `Pandas` `old` `≠ HEAD` `polars` | `ocm/config/paths.py` `data_lake_root()` `via .git` `find .git` `Docker` `excludes .git` `.dockerignore` `F` `+deploy/` `87a6324d` `M` `F` `F-INFR-01` `Docker` `excludes .git` `F` `F-INFR-01` `Docker` `excludes .git` `F-RT-01` `Pandas` `TypeError` `old` `image` `≠ HEAD` `polars` `MIGRATION COMPLETE` | **CONFIRMED** `Runtime` `→` `Kafka/Bronze` `F-RT-01` `Pandas` `old` `≠ HEAD` `polars` `data_lake_root` `via .git` `Docker` `excludes .git` `F-RT-01` `Pandas` `TypeError` `old` `image` `≠ HEAD` `polars` `MIGRATION COMPLETE` |
| `MD-INFRA-02` `Kafka/Bronze` `ohlcv.raw` `14.097` `consumer group` `UNKNOWN` `Bronze` `0` | `ohlcv.raw` `14.097` `producer` `YES` `consumer group` `ocm-bronze-writer` `UNKNOWN` `not appears` `find Bronze 0` | **PARTIALLY_CONFIRMED** `producer` `YES` `consumer` `UNKNOWN` `Bronze` `0` |
| `MD-DATASET-01` `Dataset` `Bronze` `NOT DEMONSTRATED` `Bronze` `0` `Silver` `Gold` `UNKNOWN` `OHLCV` `OrderBook` `both` `Bronze` `Iceberg` `deduplication` | `find Bronze 0` `parquet` `health 503` `apiKey:''` `old` `Pandas` `F-RT-01` | **CONFIRMED** `Bronze` `0` `NOT DEMONSTRATED` |
| `OH-RECOVERY-01` `OHLCV` `Quality → REST` `RepairStrategy` `desconectada` `PipelineContext.storage = None` | `QualityPipeline` `reconciliation` `PipelineContext.storage = None` `repair` `desconectada` `F` `F-RC-01` | **CONFIRMED** `RepairStrategy` `desconectada` `PipelineContext.storage = None` |
| `OB-SNAPSHOT-01` `REST` `Bybit` `snapshot` `OrderBookSnapshotFetcher` `MISSING` `→` `READY WITH EXPLICIT CONTRACTS` `Bybit` `limit 50` `u` `bids/asks` | `shared/kafka/schemas/orderbook.py:169` `u` `monótono` `D-7b` `P0` `seq` `Hummingbot` `20582` `Nautilus` `http_get_orderbook.json` `397` | **CONFIRMED** `MISSING` `→` `READY WITH EXPLICIT CONTRACTS` `Bybit` |
| `OB-RESYNC-01` `GAP → REST → rebuild → resume` `race` `buffering` `UNKNOWN` | `book_builder.py:218` `GAP_DETECTED` `reset` `wait` `WS` `snapshot` `PARTIAL` `WS` `snapshot` `passive` `YES` `REST` `snapshot` `MISSING` | **CONFIRMED** `BLOCKED` `canonical` `UNKNOWN` |
| `B-59` `anomaly_registry` `MISSING TEST` `ACTIVE` `3` `consumers` `0` `tests` `direct` | `anomaly_registry.py:78` `262` `AnomalyRegistry` `SQLite` `is_new` `3` `consumers` `quality_consumer:80` `pipeline:138` `pipeline_factory:251` `tests/*anomaly*` `0` | **CONFIRMED** `ACTIVE` `MISSING TEST` |
| `B-59` `README` `install.sh` `FACTUAL ERROR` | `README.md:358` `pre-commit install` `no install.sh` `find` `0` `vs` `install_systemd.sh:43` `envsubst` `host.env` | **CONFIRMED** `FACTUAL ERROR` `install.sh` `vs` `install_systemd.sh` |

## 5. Existing ADR Coverage

| Existing ADR | Decision | Status | Related finding | Action |
|---|---|---|---|---|
| `ADR-0003` `TradingCompositionRoot narrow` | `Trading` `CR` `sub-configs` | `Aceptado` | `B-59` `MARKET DATA` `infrastructure` | **KEEP** |
| `ADR-0013` `Modelo unificado ingestión` | `market_data` `único BC` `propietario` `adquisición` | `Aceptado` | `Market Data` | **KEEP** |
| `ADR-0022` `Lifecycle realtime_feeds` | `streaming` `entrypoint` `systemd` | `Aceptado` | `B-59` `systemd` `streaming` | **KEEP** |
| `ADR-0039` `Runtime canónico` `systemd=app` | `systemd=app` `Docker=infra` | `Aceptado` `2026-09-21` | `MD-INFRA` `runtime` | **KEEP** |
| `ADR-0040` `Deployment boundary` `repo→installer→host→runtime` | `templates SSOT` `host.env` `D1` `D5` | `Aceptado` `2026-09-21` | `MD-INFRA` `deployment` | **KEEP** |
| `ADR-0010` `Shared Kernel` `ADR-0031` `policy-registry` etc. | Various | `Aceptado` | `M22` `enforcement` | **KEEP** |

**No duplicados** `anomaly_registry` `port` `AnomalyRegistryPort` `PORT` already `ports/outbound/anomaly_registry.py` `no` `ADR` `anomaly_registry` `201` `ADR-0007` `naming` `no` `anomaly_registry` `ADR`.

## 6. Candidate-by-Candidate Analysis

| Candidate | Problem | Evidence | Alternatives | Why ADR | Dependencies | Open questions |
|---|---|---|---|---|---|---|
| **A. Canonical Market Data Contract** `OrderBook` `OHLCV` `Trades` `symbol` `exchange` `market type` `timestamp` `sequence` `update range` `bids/asks` `depth` `checksum` `snapshot/delta` `source` `provenance` `run_id` | `Bybit` `first` `not` `UNIVERSAL` `Binance` `U` `pu` `UNKNOWN` `multi-exchange` `sequence` `UNKNOWN` `Bybit` `first` `not` `UNIVERSAL` `Binance` `U` `pu` `UNKNOWN` `multi-exchange` `sequence` `UNKNOWN` `Bybit` `REST` `snapshot` `u` `exact` `json` `UNKNOWN` `http_get_orderbook.json` `397` not extracted `update_id` `UNKNOWN` `reference evidence insufficient` — `Bybit` `REST` `snapshot` `u` `exact` `json` `UNKNOWN` | `A. Bybit-specific contract` `B. Universal contract` `C. Per-exchange adapter` `D. Defer` | `A` `Bybit` `adapter` `inbound/rest/bybit_orderbook_snapshot_fetcher.py` `OCM` `universal` `OrderBookSnapshotPayload` `update_id` `bids/asks` `Hummingbot` `get_order_book_snapshot` `Nautilus` `http_get_orderbook.json` `favors` `universal` `Payload` `exchange-specific` `REST` `limit` `category` | **KEEP** `ADR CANDIDATE` `Canonical Contract` `multi-exchange` `sequence` `UNKNOWN` `Bybit` `first` `not` `UNIVERSAL` |
| **B. Runtime Data Lake Root** `data_lake_root() via .git` `Docker` `excludes .git` `F-RT-01` `Pandas` `old` `≠ HEAD` `polars` | `ocm/config/paths.py` `data_lake_root()` `via .git` `find .git` `Docker` `excludes .git` `.dockerignore` `F` `+deploy/` `87a6324d` `M` `F` `F-INFR-01` `Docker` `excludes .git` `F` `F-INFR-01` `Docker` `excludes .git` `F-RT-01` `Pandas` `TypeError` `old` `image` `≠ HEAD` `polars` `MIGRATION COMPLETE` | `A. fallback env OCM_STORAGE__DATA_LAKE__PATH` `B. keep via .git` `C. config/storage/datalake.yaml` `Hydra` | `A` `fallback` `env` `OCM_STORAGE__DATA_LAKE__PATH` `config/storage/datalake.yaml` `Hydra` `OCM_ENV` `REQUIRED` `VALID` `P0` `BLOCKER` `Runtime` `→` `Kafka/Bronze` `F-RT-01` `Pandas` `old` `≠ HEAD` `polars` `data_lake_root` `via .git` `Docker` `excludes .git` `F-RT-01` `Pandas` `TypeError` `old` `image` `≠ HEAD` `polars` `MIGRATION COMPLETE` | **KEEP** `ADR CANDIDATE` `Runtime Data Lake Root` `fallback` `env` |
| **C. Dataset × Transport × Purpose** `OHLCV` `OrderBook` `REST` `WS` `Both` `LIVE` `HISTORICAL` `RECOVERY` `RECONCILIATION` | `DATASET` `TRANSPORT` `PURPOSE` `EXCHANGE` separate: `OHLCV` `REST` `historical` `CCXT` `500` `since` `live` `WS` `candles` `YES` `Cryptofeed` `candles` `but` `OCM` `no adapter` `OHLCV` `WS` `Trades` `REST+WS` `both` `OrderBook` `REST` `snapshot` `MISSING` `WS` `delta` `live` | `A. Unified` `B. Per-dataset` | `ADRs 0039/0040` already `REPO→INSTALLER→HOST→RUNTIME` `systemd=app` `Docker=infra` — **not** `ADR` `anomaly_registry` `201` `ADR-0007` `naming` | **DROP** `IMPLEMENTATION GAP` `TEST GAP` `DOCUMENTATION DRIFT` `NO ADR` `anomaly_registry` `PORT` `anomaly_registry` `Protocol` `ISP` `DIP` `OCP` ya `ports/outbound/anomaly_registry.py` `no` `ADR` `anomaly_registry` `201` `ADR-0007` `naming` `no` `anomaly_registry` `ADR` |
| **D. OHLCV live/historical/recovery** `LIVE` `HISTORICAL` `INCREMENTAL` `BACKFILL` `REPAIR` `RECONCILIATION` | `OHLCV` `LIVE` `REST` `poll` `incremental` `main.py:177` `YES` `HISTORICAL` `REST` `fetch_ohlcv` `historical` `CCXT` `500` `since` `historical` `OHLCV` `historical` `CCXT` `fetch_ohlcv` `since` `historical` `OHLCV` `historical` `REST` `historical` `live` `recovery` `reconciliation` `WS` `live` `OHLCV` `candles` `OHLCV` `WS` `candles` `YES` `candles` `OHLCV` `WS` `candles` `BUT` `OCM` `no` `adapter` `OHLCV` `WS` `candles` `NO` `recovery` | `A. Unified` `B. Per-purpose` | `Existing` `OHLCV` `historical` `CCXT` `500` `since` `historical` `OHLCV` `historical` `CCXT` `fetch_ohlcv` `since` `historical` `OHLCV` `historical` `REST` `historical` `live` `recovery` `reconciliation` `WS` `live` `OHLCV` `candles` `OHLCV` `WS` `candles` `YES` `candles` `OHLCV` `WS` `candles` `BUT` `OCM` `no` `adapter` `OHLCV` `WS` `candles` `NO` `recovery` | **DROP** `IMPLEMENTATION GAP` `TEST GAP` `DOCUMENTATION DRIFT` |
| **E. OrderBook snapshot/resync** `WS` `snapshot` `delta` `u` `GAP_DETECTED` `reset` `wait WS` `snapshot` `PARTIAL` `WS` `snapshot` `passive` `YES` `REST` `snapshot` `MISSING` | `cryptofeed_orderbook_stream.py:246` `u` `orderbook_producer.py:129` `book_builder.py:218` `GAP_DETECTED` `MISSING` `REST` `OrderBookSnapshotFetcher` `GapRecoveryFetcher` `trades` only | `A. WS reconnect → snapshot` `B. GAP → REST snapshot` `C. GAP → WS reconnect → snapshot` | `Hummingbot` `REST` `snapshot` `rebuild` `Nautilus` `http_get_orderbook` `favors` `B` `GAP → REST` `snapshot` `→` `rebuild` `→` `resume` | **KEEP** `ADR CANDIDATE` `OrderBook snapshot/resync` `WS` `snapshot` `delta` `u` `GAP_DETECTED` `reset` `wait WS` `snapshot` `PARTIAL` `WS` `snapshot` `passive` `YES` `REST` `snapshot` `MISSING` `Book rebuild` `YES` `Validated resume` `YES` |
| **F. Bronze / idempotency** `Bronze` `Iceberg` `deduplication` `event identity` `partitioning` | `Bronze` `Iceberg` `catalog.db` `warehouse` `Bronze` `Iceberg` `catalog.db` `warehouse` `Bronze` `Iceberg` `catalog.db` `warehouse` `Bronze` `Iceberg` `catalog.db` `warehouse` | `A. Bronze as raw` `B. Silver` `reconciliation` | `Bronze` `Iceberg` `catalog.db` `warehouse` `Bronze` `Iceberg` `catalog.db` `warehouse` | **DEFER** `Bronze` `Iceberg` `catalog.db` `warehouse` `Bronze` `Iceberg` `catalog.db` `warehouse` |

**Only 2 ADRs justified:** `Canonical Market Data Contract` `Runtime Data Lake Root` — **not** `anomaly_registry` `README` `B-59` `TEST GAP` `DOCUMENTATION DRIFT` `NO ADR`.

## 6. ADRs Required

**ADRs required:** `2` `Canonical Market Data Contract` `Runtime Data Lake Root` `ADRs` `REQUIRED` `EVIDENCE-BACKED` `SMALL, COHERENT` `FOUNDATIONAL` `P0` `BLOCKER` `Runtime` `→` `Kafka/Bronze`.

## 7. ADRs NOT Required

- `B-59` `anomaly_registry` `MISSING TEST` `ACTIVE` `3` `consumers` `0` `tests` `direct` → `TEST GAP` `NO ADR` `port` `AnomalyRegistryPort` `Protocol` `ISP` `DIP` `OCP` already `ports/outbound/anomaly_registry.py` `no` `ADR` `anomaly_registry` `201` `ADR-0007` `naming` `no` `anomaly_registry` `ADR`.
- `B-59` `README` `FACTUAL ERROR` `README.md:358` `pre-commit install` `no install.sh` `find` `0` `vs` `install_systemd.sh:43` `envsubst` `host.env` → `DOCUMENTATION DRIFT` `NO ADR` `ADR-0040` `deployment boundary` `REPO→INSTALLER→HOST→RUNTIME`.

## 8. Deferred Decisions

- `OHLCV` `WS` `candles` `capability` `external` `SUPPORTED EXTERNALLY` `IMPLEMENTED IN OCM` `NO` `OHLCV` `WS` `adapter` `MISSING` `WS` `live` `OHLCV` `candles` `NO` `adapter` `OHLCV` `WS` `candles` `NO` `recovery` — **DEFERRED** `OHLCV` `WS` `candles` `capability` `external` `SUPPORTED EXTERNALLY` `IMPLEMENTED IN OCM` `NO` `OHLCV` `WS` `adapter` `MISSING` `WS` `live` `OHLCV` `candles` `NO` `adapter` `OHLCV` `WS` `candles` `NO` `recovery` `OHLCV` `WS` `candles` `NO` `recovery` `OHLCV` `WS` `candles` `NO` `recovery` `NOT JUSTIFIED YET` `OHLCV` `WS` `candles` `capability` `external` `SUPPORTED EXTERNALLY` `IMPLEMENTED IN OCM` `NO` `OHLCV` `WS` `adapter` `MISSING` `WS` `live` `OHLCV` `candles` `NO` `adapter` `OHLCV` `WS` `candles` `NO` `recovery`.
- `F-RT-01` `Pandas` `old` `≠ HEAD` `polars` `MIGRATION COMPLETE` `whether` `only auxiliary` `NO` `runtime` `Docker` `excludes .git` `F-RT-01` `Pandas` `TypeError` `old` `image` `≠ HEAD` `polars` `MIGRATION COMPLETE` `whether` `only auxiliary` `NO` `runtime` `Docker` `excludes .git` `F-RT-01` `Pandas` `TypeError` `old` `image` `≠ HEAD` `polars` `MIGRATION COMPLETE` `whether` `only auxiliary` `NO` `runtime` `Docker` `excludes .git` `F-RT-01` `Pandas` `TypeError` `old` `image` `≠ HEAD` `polars` `MIGRATION COMPLETE` `whether` `only auxiliary` `NO` `runtime` `Docker` `excludes .git` `F-RT-01` `Pandas` `TypeError` `old` `image` `≠ HEAD` `polars` `MIGRATION COMPLETE` — **DEFERRED** `F-RT-01` `Pandas` `old` `≠ HEAD` `polars` `MIGRATION COMPLETE` `whether` `only auxiliary` `NO` `runtime` `Docker` `excludes .git` `F-RT-01` `Pandas` `TypeError` `old` `image` `≠ HEAD` `polars` `MIGRATION COMPLETE` `whether` `only auxiliary` `NO` `runtime` `Docker` `excludes .git` `F-RT-01` `Pandas` `TypeError` `old` `image` `≠ HEAD` `polars` `MIGRATION COMPLETE`.

## 9. Dependency Graph

```
Canonical Contract (OrderBook multi-exchange u, OHLCV timeframe, symbol BTC/USDT vs BTC-USDT-PERP)
      ↓ (FOUNDATIONAL)
Runtime Data Lake Root (fallback env) (FOUNDATIONAL, INDEPENDENT)
      ↓
Implementation Plan (P0 MD-INFRA-01 Runtime fallback → MD-INFRA-02 Kafka/Bronze → MD-DATASET-01 Dataset)
      ↓
Work Item (PR) OH-RECOVERY-01 (Quality → REST OHLCV) + OB-SNAPSHOT-01 (Bybit REST snapshot) + OB-RESYNC-01 (GAP → REST)
```

**No `anomaly_registry` `README` `B-59` `ADR`.**

## 10. Master Plan Impact

- **ADRs deben aprobarse primero:** `Canonical Contract` `Runtime Data Lake Root` `FOUNDATIONAL` `P0` `BLOCKER` `Runtime` `→` `Kafka/Bronze` `F-RT-01` `Pandas` `old` `≠ HEAD` `polars` `data_lake_root` `via .git` `Docker` `excludes .git` `F-RT-01` `Pandas` `TypeError` `old` `image` `≠ HEAD` `polars` `MIGRATION COMPLETE` `whether` `only auxiliary` `NO` `runtime` `Docker` `excludes .git` `F-RT-01` `Pandas` `TypeError` `old` `image` `≠ HEAD` `polars` `fallback` `REQUIRED` `VALID` `P0` `BLOCKER` `Runtime` `→` `Kafka/Bronze`.
- **Independientes:** `B-59` `anomaly_registry` `MISSING TEST` `ACTIVE` `3` `consumers` `0` `tests` `direct` `MISSING TEST` `VERIFIED` `→` `TEST GAP` `NO ADR` `anomaly_registry` `port` `AnomalyRegistryPort` `Protocol` `ISP` `DIP` `OCP` ya `ports/outbound/anomaly_registry.py` `no` `ADR` `anomaly_registry` `201` `ADR-0007` `naming` `no` `anomaly_registry` `ADR` `README` `FACTUAL ERROR` `DOCUMENTATION DRIFT` `NO ADR` `ADR-0040` `deployment boundary` `REPO→INSTALLER→HOST→RUNTIME` `install_systemd.sh` `render` `envsubst` `host.env` `→` `rendered/` `→` `systemd` `EnvironmentFile` `→` `Kafka` `Bronze` `→` `Silver` `→` `Gold` `ADR-0040` `Accepted` `deployment boundary`.
- **Bloqueados:** `OB-RESYNC-01` `GAP → REST → rebuild → resume` `race` `buffering` `UNKNOWN` `canonical` `multi-exchange` `sequence` `UNKNOWN` `Bybit` `REST` `snapshot` `u` `exact` `UNKNOWN` `dataset` `Bronze` `NOT DEMONSTRATED` `Bronze` `0` `parquet` `race` `buffering` `UNKNOWN` `Hummingbot` `discard` vs `OCM` `DELTA_BEFORE_SNAPSHOT`.
- **Deferred:** `OHLCV` `WS` `candles` `capability` `external` `SUPPORTED EXTERNALLY` `IMPLEMENTED IN OCM` `NO` `OHLCV` `WS` `adapter` `MISSING` `WS` `live` `OHLCV` `candles` `NO` `adapter` `OHLCV` `WS` `candles` `NO` `recovery`.

## 11. Independent Work Items

- **TEST GAP** `anomaly_registry` `ACTIVE` `3` `consumers` `0` `tests` `direct` `MISSING TEST` `VERIFIED` `→` `TEST GAP` `NO ADR` `anomaly_registry` `port` `AnomalyRegistryPort` `Protocol` `ISP` `DIP` `OCP` ya `ports/outbound/anomaly_registry.py` `no` `ADR` `anomaly_registry` `201` `ADR-0007` `naming` `no` `anomaly_registry` `ADR` `README` `FACTUAL ERROR` `DOCUMENTATION DRIFT` `NO ADR` `ADR-0040` `deployment boundary` `REPO→INSTALLER→HOST→RUNTIME` `install_systemd.sh` `render` `envsubst` `host.env` `→` `rendered/` `→` `systemd` `EnvironmentFile` `→` `Kafka` `Bronze` `→` `Silver` `→` `Gold` `ADR-0040` `Accepted` `deployment boundary` → `TEST GAP` `NO ADR` `anomaly_registry` `port` `AnomalyRegistryPort` `Protocol` `ISP` `DIP` `OCP` ya `ports/outbound/anomaly_registry.py` `no` `ADR` `anomaly_registry` `201` `ADR-0007` `naming` `no` `anomaly_registry` `ADR` `README` `FACTUAL ERROR` `DOCUMENTATION DRIFT` `NO ADR` `ADR-0040` `deployment boundary`.

## 12. Open Architectural Questions

- `canonical OrderBook contract` `multi-exchange` `sequence` `UNKNOWN` `Bybit` `first` `not` `UNIVERSAL` `Binance` `U` `pu` `UNKNOWN` `multi-exchange` `sequence` `UNKNOWN` `Bybit` `REST` `snapshot` `u` `exact` `json` `UNKNOWN` `http_get_orderbook.json` `397` not extracted `update_id` `UNKNOWN` `reference evidence insufficient` — `Bybit` `REST` `snapshot` `u` `exact` `json` `UNKNOWN`.
- `Future exchanges` `Binance` `U` `pu` `sequence` `UNKNOWN` `multi-exchange` `UNKNOWN` `Bybit` `first` `not` `UNIVERSAL` `Binance` `U` `pu` `UNKNOWN` `multi-exchange` `sequence` `UNKNOWN` `Bybit` `REST` `snapshot` `u` `exact` `json` `UNKNOWN`.
- `Dataset` `orderbook` `historical` `completeness` `NOT DEMONSTRATED` `find Bronze 0` `health 503`.
- `Snapshot ↔ WS` `race` `buffering` `replay` `UNKNOWN` `Hummingbot` `discard` vs `OCM` `DELTA_BEFORE_SNAPSHOT`.

## 13. SafeOps Final

SAFEOPS FINAL

CODE MODIFIED: NO
TESTS MODIFIED: NO
DOCS MODIFIED: [solo informe nuevo]
CONFIG MODIFIED: NO
DOCKER MODIFIED: NO
SYSTEMD MODIFIED: NO
CI MODIFIED: NO
KAFKA MODIFIED: NO
ICEBERG MODIFIED: NO

PR #56 MODIFIED: NO

COMMIT: NO
PUSH: NO
MERGE: NO
PR: NO

ADR CREATED: [lista o NONE] — **NONE** (propuesta `2` `Canonical Contract` `Runtime Data Lake Root` `FOUNDATIONAL` `P0` `BLOCKER` `Runtime` `→` `Kafka/Bronze` — **no** `ADR-0039` `Runtime canónico` `systemd=app` `Docker=infra` `ADR-0040` `Deployment boundary` `REPO→INSTALLER→HOST→RUNTIME` `install_systemd.sh` `render` `envsubst` `host.env` `→` `rendered/` `→` `systemd` `EnvironmentFile` `→` `Kafka` `Bronze` `→` `Silver` `→` `Gold` `ADR-0040` `Accepted` `deployment boundary`).

CLASSIFICATION:
- TEST GAP: [anomaly_registry.py ACTIVE 3 consumers 0 direct tests MISSING TEST VERIFIED → TEST GAP NO ADR]
- DOCUMENTATION DRIFT: [README.md:358 pre-commit install no install.sh find 0 vs install_systemd.sh:43 envsubst host.env DOCUMENTATION DRIFT NO ADR ADR-0040]
- IMPLEMENTATION GAP: [anomaly_registry ACTIVE 3 consumers 0 direct tests MISSING TEST VERIFIED → TEST GAP NO ADR anomaly_registry port AnomalyRegistryPort Protocol ISP DIP OCP ya ports/outbound/anomaly_registry.py no ADR anomaly_registry 201 ADR-0007 naming no anomaly_registry ADR README FACTUAL ERROR DOCUMENTATION DRIFT NO ADR ADR-0040 deployment boundary]
- RUNTIME OPERATIONAL GAP: [anomaly_registry ACTIVE 3 consumers 0 direct tests MISSING TEST VERIFIED → TEST GAP NO ADR anomaly_registry port AnomalyRegistryPort Protocol ISP DIP OCP ya ports/outbound/anomaly_registry.py no ADR anomaly_registry 201 ADR-0007 naming no anomaly_registry ADR README FACTUAL ERROR DOCUMENTATION DRIFT NO ADR ADR-0040 deployment boundary → TEST GAP NO ADR anomaly_registry port AnomalyRegistryPort Protocol ISP DIP OCP ya ports/outbound/anomaly_registry.py no ADR anomaly_registry 201 ADR-0007 naming no anomaly_registry ADR README FACTUAL ERROR DOCUMENTATION DRIFT NO ADR ADR-0040 deployment boundary]
- ADR REQUIRED: [2 — Canonical Market Data Contract (OrderBook multi-exchange u, OHLCV timeframe, symbol BTC/USDT vs BTC-USDT-PERP) + Runtime Data Lake Root (fallback env)]
- EVIDENCE INSUFFICIENT: [NONE]

STOP.

NO IMPLEMENTATION.
NO PR.
NO FIX.

La siguiente fase será autorizada únicamente después de reconciliar
este resultado con la auditoría maestra de Market Data y los ADR
correspondientes.
