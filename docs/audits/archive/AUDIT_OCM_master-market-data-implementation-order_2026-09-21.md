# AUDIT — Master Market Data Implementation Order — OCM

**ID:** AUDIT-MASTER-MD-2026-09-21
**Date:** 2026-09-21
**Scope:** `Exchange → REST/WS → Adapters → Application → Kafka → Raw → Bronze → Quality → Silver → Gold → Trading → Exchange` — `OHLCV` `OrderBook` `REST` `WS` `Kafka` `Bronze/Silver/Gold` `Quality` `gap` `reconnect` `resync` `backfill` `reconciliation` `recovery`
**Git:** `feat/m22b-governance` `857cfe60` `M22-B` `?? 6 audit + 4 OB-RESYNC + 3 AUDIT_OCM_comparative + 1 AUDIT_OCM_orderbook-resync + 1 AUDIT_OCM_readiness` preserved
**Previous:** `OB-RESYNC-001` `PARTIALLY_CONFIRMED` `RECONNECT != RESYNC` + `comparative` `333` `REST+WS` `Hummingbot/Nautilus` + `readiness` `269` `PR-A READY WITH CONTRACTS, PR-B BLOCKED` + `market-data-e2e-incompleteness` `14.097 ohlcv.raw` `consumer group` `F-RT-01` `Pandas` `dry_run`
**Files:** `packages/market_data/main.py:146`, `adapters/inbound/rest/ohlcv_fetcher.py`, `adapters/inbound/websocket/cryptofeed_orderbook_stream.py:246`, `book_builder.py:218`, `shared/kafka/schemas/orderbook.py:169`, `gap_recovery_fetcher.py:6`, `config/market_data/feeds.yaml`, `deploy/systemd/ocm-streaming.service.template:11`, `docker-compose.yml:24`, `Dockerfile:40`, `ocm/config/paths.py` `data_lake_root()`, `tests/market_data/application/processing/test_book_builder.py:104`

## 1. Executive Summary

**Question:** What must be fixed first for `OCM` to have `reproducible, observable, recoverable` `Market Data` `Market Data` before `OrderBook resync` `PR-A/B`?

**Answer:** **ARCHITECTURE NOT READY FOR IMPLEMENTATION** — `Runtime` `Docker` `Git HEAD ≠ Docker runtime` `F-RT-01` `Pandas` `dry_run` `consumer group` `mismatch` `Bronze` `NOT DEMONSTRATED` `Silver/Gold` `UNKNOWN` `14.097` `ohlcv.raw` `unconsumed` `healthcheck` `false` are **foundational** `MD-INFRA` `GATE 0-1` blockers. `OrderBook` `PR-A` `REST snapshot` `Bybit` `READY WITH EXPLICIT CONTRACTS` but `PR-B` `resync` `GAP → REST → rebuild → resume` `BLOCKED BY ARCHITECTURAL UNKNOWN` (`canonical contract` `multi-exchange` `sequence` `UNKNOWN` + `dataset` `Bronze` `NOT DEMONSTRATED` + `race` `UNKNOWN`). **Order:** `MD-INFRA-01` `Runtime` → `MD-INFRA-02` `Kafka/Bronze` `operational` `→` `MD-DATASET-01` `observable` `→` `OH-RECOVERY-01` `contracts` `→` `OB-SNAPSHOT-01` `PR-A` `→` `OB-RESYNC-01` `PR-B` (not `PR-A → PR-B` direct).

## 2. Evidence Reviewed

- `OB-RESYNC-001` `3` audits `270+252+161` `PARTIALLY_CONFIRMED` `u` `GAP_DETECTED` `MISSING REST`
- `AUDIT_OCM_comparative-architecture-refs_2026-09-21.md` `333` `Hummingbot` `20582` `Nautilus` `http_get_orderbook.json` `Freqtrade` `download-data`
- `AUDIT_OCM_orderbook-readiness-before-implementation_2026-09-21.md` `269` `PR-A READY WITH CONTRACTS, PR-B BLOCKED`
- `AUDIT_OCM_orderbook-resync-architectural-decision_2026-09-21.md` `291` `REST `u` `b/a` `Gap` `Resync`
- `AUDIT_OCM_market-data-e2e-incompleteness_2026-09-21.md` `F-RT-01` `Pandas` `14.097` `consumer group` `F-RT-01` `dry_run` `Silver/Gold` `no runner`
- `docs/audit/deployment-runtime-implementation-plan.md` `P0-P5` `Validated Architecture` `systemd=app`
- `packages/market_data/main.py:622` `load_appconfig_standalone` `FastAPI` `:8001`
- `ocm/config/paths.py` `data_lake_root()` `requires .git` (Docker `excludes .git` `F` `F-INFR-01`)

## 3. Current E2E Architecture (Code)

```
Exchange Bybit (REST CCXT fetch_ohlcv + WS L2_BOOK BTC-USDT-PERP)
  ↓ REST Adapters ohlcv_fetcher.py (paginated 500, since/limit) + WS CryptofeedOrderBookStream (snapshot delta None, delta b/a u)
  ↓ Application PipelineOrchestrator → CCXTAdapter (ccxt_adapter.py:692) + BookBuilder (book_builder.py:218 GAP_DETECTED)
  ↓ Kafka Raw ohlcv.raw (14.097) + orderbook.raw (u) (Producer OrderBookKafkaProducer, CCXT OHLCV)
  ↓ Raw topics (ohlcv.raw, orderbook.raw) → Bronze Iceberg (OHLCV Bronze writer, OrderBook NO Bronze)
  ↓ Quality (DataQualityCheckerPort, anomaly_registry.py is_new) → gap_scanner (2× timeframe) → AnomalyRegistry
  ↓ Silver (OHLCV Silver, OrderBook NO Silver) → Gold (OHLCV Gold, OrderBook NO Gold)
  ↓ Trading (live/paper) → Exchange (create_order)
```

**Status per transition:**

| Transición | Implemented | Connected | Executed | Persisted | Validated | Operational | Estado |
|---|---|---|---|---|---|---|---|
| `Exchange → REST` `fetch_ohlcv` | YES | YES | YES | N/A | YES | UNKNOWN | PARTIAL |
| `Exchange → WS` `orderbook` | YES | YES | YES | N/A | YES | UNKNOWN | YES |
| `Adapters → Application` `CCXT` `BookBuilder` | YES | YES | YES | N/A | YES | YES | YES |
| `Application → Kafka` `ohlcv.raw` `orderbook.raw` | YES | YES | YES | YES | YES | YES (14.097) / UNKNOWN (orderbook) | PARTIAL |
| `Raw → Bronze` `OHLCV` | YES | **NO** `consumer group ocm-bronze-writer` missing `ocm-bronze-writer` not `consumer group` `UNKNOWN` `14.097` unconsumed | **NO** | **NO** | **NO** | **NO** | **MISSING** |
| `Raw → Bronze` `OrderBook` | **NO** `Bronze` `orderbook` `NO` | **NO** | **NO** | **NO** | **NO** | **NO** | **MISSING** |
| `Bronze → Quality` `OHLCV` | YES | YES | UNKNOWN | N/A | YES | UNKNOWN | PARTIAL |
| `Quality → Silver` | **NO** `Quality` `gap` **not** `REST` `repair` | **NO** | **NO** | **NO** | **NO** | **NO** | **MISSING** |
| `Silver → Gold` | **NO** `Gold` no `runner` | **NO** | **NO** | **NO** | **NO** | **NO** | **MISSING** |
| `OrderBook → Kafka` `book.*` | YES | YES | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | PARTIAL |
| `Docker runtime` `ocm_market_data:latest` | YES | **NO** `Git HEAD ≠ Docker` `F-RT-01` `Pandas` | **NO** `old image` | N/A | **NO** `Pandas TypeError` | **NO** | **MISSING** |
| `systemd` `ocm-streaming` `ocm-market-data` | YES `templates` | YES `ocm-streaming active` | **NO** `ocm-market-data inactive` | N/A | **NO** `B-59` `MISSING TEST` | **PARTIAL** | **PARTIAL** |

## 4. Runtime Reality

| Componente | Git | Conectado | Ejecuta | Versión actual | Persistió | Funciona | CODE vs RUNTIME |
|---|---|---|---|---|---|---|---|
| `Docker image` `ocm_market_data:latest` | `Dockerfile:40` `CMD` | **NO** `digest` `UNKNOWN` `14.097` `old` `Pandas` | **NO** `old` `F-RT-01` | **NO** `old` `F-RT-01` `Pandas TypeError` | **NO** `old` | **MISSING** |
| `systemd` `ocm-streaming` | `templates:11` `EnvironmentFile` | **YES** `active` `PID69498` | **YES** `active` 4h49m | **YES** `active` | **NO** `L4` `0` `Bronze` | **PARTIAL** `L3` `9943250` |
| `systemd` `ocm-market-data` | `templates:11` | **NO** `inactive` `disabled` | **NO** `inactive` | **NO** | **NO** | **MISSING** |
| `Python entrypoints` `market_data.main` `streaming_hydra` | `packages/market_data/main.py:622` `apps/app/cli/streaming_hydra.py:226` | **YES** `streaming_hydra` `active` | **YES** `streaming` `active` | `market_data.main` `inactive` | **PARTIAL** | **PARTIAL** |
| `Kafka consumers` `ocm-bronze-writer` | `packages/market_data/infrastructure/kafka/` `consumer` `group_id ocm-bronze-writer` | **NO** `consumer group` `not appears` `ocm-bronze-writer` `UNKNOWN` | **NO** `14.097` `unconsumed` | **NO** | **NO** | **MISSING** |
| `Kafka topics` `ohlcv.raw` `orderbook.raw` | `shared/kafka/topics.py` `TOPIC_OHLCV_RAW` | **YES** `ohlcv.raw` `14.097` `orderbook.raw` `u` | **YES** `14.097` | **YES** `orderbook.raw` `1h` | **YES** | **YES** |
| `Bronze writer` `KafkaBronzeWriter` | `infrastructure/kafka/kafka_bronze_writer.py` | **NO** `consumer group` | **NO** `old image` `F-RT-01` | **NO** `0` `Bronze` `parquet` | **NO** | **MISSING** |
| `Iceberg` `Silver` `Gold` | `packages/market_data/infrastructure/storage/` `iceberg` | **NO** `Silver` `no runner` | **NO** | **NO** | **NO** | **MISSING** |
| `Quality` `gap_scanner` | `application/processing/gap_scanner.py:16` | **YES** `scan_gaps` | **NO** `auto-heal` `desactivado` | **NO** | **YES** `is_new` | **PARTIAL** |
| `OHLCV` `fetch_ohlcv` | `adapters/inbound/rest/ohlcv_fetcher.py` | **YES** | **YES** `historical` | **YES** `ohlcv.raw` | **YES** | **YES** |
| `OrderBook` `BookBuilder` | `application/processing/book_builder.py:218` | **YES** | **YES** `GAP_DETECTED` | **NO** `book.*` `Bronze` | **YES** | **PARTIAL** |

**CODE EXISTS** `YES` (all), **SYSTEM ACTUALLY RUNS IT** `PARTIAL` (`streaming` `active` `orderbook.raw` `YES`, `market_data` `inactive` `old` `Pandas` `F-RT-01`).

## 5. Kafka Reality

**Produce `ohlcv.raw`:** `OrderBook` `CCXT` `fetch_ohlcv` `OHLCV` `ohlcv.raw` `14.097` `Kafka` `orderbook.raw` `u` `orderbook` `Yes`.

**Consume `ohlcv.raw`:** `ocm-bronze-writer` `consumer group` `UNKNOWN` `consumer group ocm-bronze-writer` `not appears` `kafka-consumer-groups.sh --list` `0` `groups` `__consumer_offsets` only `orderbook.raw` `14.097` `unconsumed` `Bronze` `0` `parquet`.

**Should consume:** `KafkaBronzeWriter` `packages/market_data/infrastructure/kafka/kafka_bronze_writer.py` `group_id ocm-bronze-writer` `poll` `max_records 500` `commit` `at-least-once`.

**Actually consumes:** `UNKNOWN` `old` `Pandas` `F-RT-01` `TypeError` `old image` `not` `current` `HEAD`.

**Where written:** `data_platform/iceberg_warehouse/bronze` `0` `parquet` `F-RT-01` `old` `Pandas` `TypeError` `bronze` `NOT DEMONSTRATED`.

**Pending:** `14.097` `ohlcv.raw` `unconsumed` `Bronze` `0` `parquet` `gap` `UNKNOWN` `loss` `UNKNOWN` `duplication` `NO`.

**Not `consumer group` `not` `sufficient` — `process` `should` `consume` `ocm-bronze-writer` `UNKNOWN` `old` `image`.**

## 6. Docker Runtime Divergencia

**Image actual:** `ocm_market_data:latest` `Up` `healthcheck` `false` `docker ps` `ocm_market_data` `Up` `unhealthy` `F-RT-01` `Pandas` `TypeError`.

**Digest/fecha/commit:** `docker inspect ocm_market_data --format '{{.Id}} {{.Created}}'` `UNKNOWN` (no `digest` `14.097` `old` `Pandas` `F-RT-01` `TypeError` belongs to old image, `HEAD` `fix` `dataframe_to_domain.py` `polars` `no Pandas`).

**Código contenido en imagen:** `OLD` `Pandas` `pandas_to_domain.py` `TypeError` `F-RT-01` `old` `HEAD` `fix` `polars` `pandera`.

**Código actual `HEAD`:** `fix` `Pandas` `→` `polars` `MIGRATION COMPLETE` `audit_validator.py` `M22–M25` `policies/registry.yaml` `ccxt 4.5.74`.

**Divergencia:** `Git HEAD` `polars` `≠` `Docker runtime` `Pandas` `F-RT-01`.

**Consecuencia:** `F-RT-01` `Pandas TypeError` **no** en `Git` `HEAD` pero **sí** en `runtime` `old` `image` → `Bronze` `0` `parquet` `consumer group` `mismatch` `healthcheck` `false` `Kafka` `ohlcv.raw` `14.097` `unconsumed`.

## 7. .git / data_lake_root()

**¿Por qué `runtime` necesita `.git`?** `ocm/config/paths.py` `data_lake_root()` `_find_config_dir()` `repo_root()` `via .git` `find .git` `parent` `data_lake_root` `data_platform/data_lake` — `runtime` `Docker` `excludes .git` `.dockerignore` `F` `+deploy/` `F` `Security Fix F` `87a6324d` `+deploy/` `F` `F-INFR-01` `Docker` `excludes .git`.

**¿Legítima?** **No** — `runtime` no debe depender `repository metadata` `.git` (accidental, `data_lake_root` `via .git`).

**¿Debe depender?** **No** — `data_lake_root` debe resolverse `env` `OCM_STORAGE__DATA_LAKE__PATH` `config/storage/datalake.yaml` `Hydra` `OCM_ENV`.

**Propuesta `fallback` `data_lake_root() sin .git`:** **VALID** `fallback` `env` `OCM_STORAGE__DATA_LAKE__PATH` `→` `Path("/app/data_platform/data_lake")` `→` `Path.cwd() / "data_platform"` — `PARTIAL` `VALID` `no` `UNSAFE` `no` `NOT YET JUSTIFIED` `no` `config` `SSOT`.

**Clasificación:** `REQUIRED` `fallback` `VALID`.

## 8. Bronze Reality

**¿Bronze operacional?** **NO** `0` `parquet` `find` `Bronze` `0`.

**¿Congelado?** **NO** `Bronze` `Iceberg` `catalog.db` `warehouse` `0` `parquet` `old` `Pandas` `F-RT-01`.

**¿Quién escribe?** `KafkaBronzeWriter` `infrastructure/kafka/kafka_bronze_writer.py` `group ocm-bronze-writer` `poll` `ohlcv.raw` `14.097` `unconsumed` `old` `image`.

**¿Dónde?** `data_platform/iceberg_warehouse/bronze` `0` `parquet` `Iceberg` `catalog.db` `warehouse`.

**¿Qué versión?** `OLD` `Pandas` `F-RT-01` `old` `image` `Pandas TypeError` `HEAD` `polars`.

**¿Mensajes sin consumir?** **YES** `14.097` `ohlcv.raw` `unconsumed` `consumer group` `not` `ocm-bronze-writer`.

**¿Persistencia?** **NO** `Bronze` `0` `parquet`.

**¿Continuidad?** **NO** `Bronze` `NOT DEMONSTRATED`.

**Completitud:** `NOT DEMONSTRATED` (`find Bronze 0`).

## 9. Silver/Gold Reality

**Bronze → Silver** `Silver` `pipeline` `Silver` `Iceberg` `deduplication` `Quality` `gap` `→` `Silver` — **code exists** `packages/market_data/application/processing/` `Silver` `pipeline` `Silver` `Gold` `YES` `code`, **entrypoint** `packages/market_data/main.py` `Silver` `pipeline` `YES`, **runtime invokes** `NO` `market_data` `inactive` `old` `Pandas`, **data flows** `NO` `Bronze` `0`, **data persists** `NO`, **validation** `NO`.

**Silver → Gold** `Gold` `features` `Gold` `pipeline` — **MISSING** `Gold` `no` `runner` `demonstrado` `runtime`.

**No `Silver` `Gold` `operational`.**

## 10. Quality / Gap Detection / Recovery

**DATA → QUALITY (`is_new` `anomaly_registry`) → GAP DETECTION (`gap_scanner` `2× timeframe`) → RECOVERY (`GapRecoveryFetcher` `trades` `gap [start,end)` ` gap_recovery_fetcher.py:6` `REST_RECOVERY`):**

- `gap detected` `is_new` `YES` `anomaly_registry` `SQLite` `is_new` `YES`.
- `recovery triggered` `GapRecoveryFetcher` `REST` `trades` **NO** `OHLCV` `gap` `→` `REST` `fetch_ohlcv` (only `trades` `gap` `REST` `recovery`).
- `data fetched` `trades` `REST` `gap` `YES` `trades`.
- `persisted` `trades` `gap` `→` `Kafka` `trades` `raw` `YES`.
- `validated` `Quality` `gap` `is_new` `YES`.

**Separar `DETECTION` vs `RECOVERY`:** `DETECTION` `YES` `is_new` `gap_scanner`, `RECOVERY` `NO` `OHLCV` `gap` `→` `REST` `fetch_ohlcv` `MISSING` (only `trades`).

## 11. OHLCV Recovery Integration

**MD-RECOVERY-001** `Incremental` `fetch_ohlcv` `since` `limit 500` `paginated` `CCXT` `ohlcv_fetcher` **Implemented** `Connected` `main.py:177` `PipelineRequest` **Executed** `historical` **Persisted** `ohlcv.raw` `14.097` **Validated** `GapRecoveryFetcher` `trades` **no** `OHLCV`.

**MD-RECOVERY-002** `Backfill` `historical` `fetch_ohlcv` `since` `limit` `historical` `ohlcv_fetcher` **Implemented** `Connected` `main.py` `Historical` `Executed` `Yes` `Persisted` `Yes` `Validated` `Yes`.

**MD-RECOVERY-003** `RepairStrategy` `Quality` `gap` `→` `REST` `fetch_ohlcv` **MISSING** `Quality` `is_new` `gap` **not** `REST` `OHLCV` `repair` `auto` `RepairStrategy` `QualityPipeline` `reconciliation` `NOT CONNECTED` `PipelineContext.storage = None` `repair` `desconectada` `F` `F-RC-01` `Evidence` `QualityPipeline` `gap` `≠` `Backfill`.

**RepairStrategy desconectada:** **VALID** `PipelineContext.storage = None` `repair` `desconectada` `runtime` `Quality` `gap` `≠` `Backfill` `historical`.

## 12. OrderBook Recovery

**WS `L2` `BTC-USDT-PERP` `Cryptofeed` `snapshot` `delta` `u` `30` `b/a` `u` `+1` `BookBuilder` `GAP_DETECTED` `reset` `wait` `WS` `snapshot` `PARTIAL` `WS` `snapshot` `passive` `YES` `REST` `snapshot` `MISSING` `Book rebuild` `YES` `Validated resume` `YES` `Kafka` `orderbook.raw` `book.delta` `no Bronze` (see `OB-RESYNC-001` `PARTIALLY_CONFIRMED`).

## 13. Separar OrderBook vs Dataset

**OrderBook LIVE RESYNC** `WS` `snapshot` `delta` `sequence` `u` `gap` `recovery/resync` `BookBuilder` `Kappa` `book.delta` `live` `correctness` — **trigger** `GAP_DETECTED` `u != last+1` `owner` `BookBuilder` `source` `REST` `snapshot` `Bybit` `/v5/market/orderbook` `persistence` `book.delta` `Kafka` `validation` `u+1`.

**OHLCV HISTORICAL BACKFILL** `OHLCV` `historical` `fetch_ohlcv` `CCXT` `since` `paginated` `gap` `historical` `Bronze` `gap` `→` `REST` `fetch_ohlcv` `historical` `reconciliation` `deduplication` `Bronze` `Iceberg` — **trigger** `historical` `gap` `Quality` `is_new` `owner` `QualityPipeline` `source` `REST` `fetch_ohlcv` `persistence` `Bronze` `Iceberg`.

**No compartir:** `estado` `sequence` `snapshot` `secuencia` `u` `OrderBook` `vs` `OHLCV` `timeframe` `timestamp` `OHLCV` `open/close` `persistence` `book.delta` `vs` `Bronze` `ohlcv` `Silver` `Gold`.

## 14. Exchanges Futuros

`OCM` `no` `conoce` `todos` `exchanges` `futuros` `Bybit` `BTC-USDT-PERP` `category=linear` `symbol=BTCUSDT` `limit 50` `u` `monótono` `+1` `P0` `seq` `gaps` `9-7593` `Bybit` `first` `implementation` `not` `UNIVERSAL SPECIFICATION` `Binance` `U` `pu` `lastUpdateId` `OKX` `seq` `UNKNOWN` `multi-exchange` `sequence` `UNKNOWN`.

**OCM COMMON CONTRACT:** `OrderBookSnapshotPayload` `OrderBookDeltaPayload` `shared/kafka/schemas/orderbook.py:169` `exchange` `symbol` `BTC/USDT` `timestamp_ms` `update_id` `u` `cross_seq` `seq` `cts_ms` `bids/asks` `depth` `checksum` `SCHEMA_VERSION 2` — **OCM UNIVERSAL** `exchange` `symbol` `timestamp_ms` `update_id` `bids/asks` `depth` `checksum` `EXCHANGE-SPECIFIC` `cross_seq` `cts_ms` `depth` `limit` `category`.

**EXCHANGE ADAPTER:** `Bybit` `REST` `/v5/market/orderbook` `category=linear` `symbol=BTCUSDT` `WS` `L2_BOOK` `BTC-USDT-PERP` `sequence mapping` `u → update_id` `seq → cross_seq` `cts → cts_ms` `EXCHANGE-SPECIFIC`.

## 15. Dataset como Dependencia

**Snapshots `WS` `snapshot` `delta` `u` `AVAILABLE` `WS` `snapshot` `YES` `REST` `snapshot` `MISSING`; `deltas` `AVAILABLE` `WS` `delta` `u`; `sequence` `u` `AVAILABLE` `u` `+1` `D-7b`; `timestamps` `exchange` `cts` `ingestion` `receipt` `AVAILABLE`; `exchange` `bybit` `AVAILABLE` `future` `UNKNOWN`; `symbol` `BTC/USDT` `PARTIAL` `BTC-USDT-PERP` `UNKNOWN` `canonical`; `depth` `50` `AVAILABLE`; `checksums` `PARTIAL` `checksum` `None` `Bybit`; `continuity` `PARTIAL` `GAP_DETECTED` `YES` `resync` `PARTIAL`; `provenance` `UNKNOWN` `source` `DATASOURCE_LIVE` `DATASOURCE_REPLAY`.

**¿Dataset suficiente para validar recovery?** **NO** — `Bronze` `0` `parquet` `NOT DEMONSTRATED` `orderbook` `historical` `completeness` `UNKNOWN` `Silver` `Gold` `UNKNOWN` `gap` `UNKNOWN`.

## 16. Race Condition

**t0 GAP `u 105` `expected 102` → reset `has_snapshot=False` → t1 REST snapshot request `Bybit` `/v5/market/orderbook` `in-flight` → t2 WS continues `deltas 106,107` `DELTA_BEFORE_SNAPSHOT` `discard` `book_builder.py:207` → t3 REST snapshot arrives `u 200` `bids/asks` `→ on_snapshot` `last=200` `rebuild` → t4 WS deltas `201` `u 201` `201==200+1` `DELTA_APPLIED` `resume` — **no buffering**, `discard` `u < snapshot.u` `snapshot` authoritative `Hummingbot`/`Nautilus` `discard` `OCM` `DELTA_BEFORE_SNAPSHOT`.

**Falta especificar:** `buffering` `replay` `UNKNOWN` `OCM` `discard` already, but `REST` `snapshot` `in-flight` `WS` `deltas` `u` `buffer` `UNKNOWN` — `Hummingbot` `discard` vs `OCM` `discard` `OK`, but `REST` `snapshot` `stale` `u < gap` `UNKNOWN`.

## 17. Grafo de Dependencias — Real

```
Runtime correctness (Docker image HEAD = polars, data_lake_root fallback env)
        ↓ (F-RT-01, .git)
Kafka correctness (Kafka 13 required, Policy gate REQUIRED, strict true)
        ↓ (Kafka ocm-bronze-writer consumer group)
Bronze operational (KafkaBronzeWriter ocm-bronze-writer group, Iceberg Bronze 0 → 14.097)
        ↓ (Bronze 0, healthcheck false)
Dataset observable (Bronze parquet, Silver, Gold, gap_scanner)
        ↓ (Bronze NOT DEMONSTRATED, Silver/Gold UNKNOWN)
Recovery contracts (Quality is_new, AnomalyRegistry, gap_scanner 2× timeframe)
        ↓ (Quality gap → REST OHLCV repair MISSING)
OHLCV recovery (GapRecoveryFetcher trades, not OHLCV historical backfill)
        ↓ (OHLCV backfill historical fetch_ohlcv)
OrderBook snapshot capability (REST Bybit snapshot, update_id u)
        ↓ (MISSING)
OrderBook resync (GAP → REST snapshot → rebuild → resume, race buffering)
        ↓ (MISSING explicit)

Ramas independientes:
Runtime ───────────────┐
                       ↓
Kafka/Bronze ───────── Dataset ──→ Recovery (OHLCV)
                       ↓                ↑
OrderBook contract ────┘                │
        ↓                            OHLCV Recovery
OrderBook snapshot                      │
        ↓                               │
OrderBook resync ───────────────────────┘
```

**Real orden:** `Runtime` `GATE 0` → `Kafka/Bronze` `GATE 1` → `Dataset` `GATE 2` → `Recovery contracts` `GATE 3` → `OHLCV recovery` `GATE 4` → `OrderBook snapshot` `GATE 4` → `OrderBook resync` `GATE 5` (not `Runtime → Kafka → Bronze → Dataset → Recovery → OrderBook snapshot → resync` linear, but `OrderBook contract` `GATE 3` independent of `Bronze`).

## 18. Gates

**GATE 0 Runtime reproducible** — `Docker image HEAD polars` `data_lake_root fallback env` `systemd` `ocm-streaming active` `ocm-market-data inactive` `F-RT-01` `Pandas` `old` `image` `≠ HEAD` **NOT READY**.

**GATE 1 Kafka/Bronze operacional** — `Kafka 14 required` `strict true` `Policy gate REQUIRED` `Bronze 0` `consumer group` `UNKNOWN` **NOT READY** `Bronze 0`.

**GATE 2 Dataset observable** — `Bronze parquet` `Silver` `Gold` `gap_scanner` `2× timeframe` **NOT DEMONSTRATED** `Bronze 0`.

**GATE 3 Recovery contracts definidos** — `Quality` `is_new` `GapRecoveryFetcher` `trades` vs `OHLCV` `historical` `fetch_ohlcv` `UNKNOWN` `canonical` `multi-exchange` `UNKNOWN` **NOT READY** (`Bybit` `u` `exact` `UNKNOWN`).

**GATE 4 OrderBook snapshot capability** — `REST` `Bybit` `snapshot` `OrderBookSnapshotPayload` `limit 50` `update_id` `u` **READY WITH EXPLICIT CONTRACTS** (can implement `PR-A` standalone), but `dataset` `NOT DEMONSTRATED`.

**GATE 5 OrderBook resync** — `GAP → REST snapshot → rebuild → resume` `race` `buffering` `reconciliation` `BLOCKED BY ARCHITECTURAL UNKNOWN` (`canonical OrderBook contract` `multi-exchange` `sequence` `UNKNOWN` + `Bybit` `REST` `snapshot` `u` `exact` + `dataset` `Bronze` `NOT DEMONSTRATED` + `race` `buffering` `UNKNOWN`).

**Validación:** `GATE 0` `not ready` → `GATE 1` `not ready` → `GATE 2` `not ready` → `GATE 3` `not ready` → `GATE 4` `ready with contracts` `PR-A` but `BLOCKED` `GATE 5`.

## 19. Orden de Implementación — STEP 1-4+

**STEP 1 `MD-INFRA-01` Runtime correctness** — Objetivo `Docker image HEAD = polars` `data_lake_root fallback env` `systemd` `ocm-streaming active` `ocm-market-data` `enable` **Dependencias** `GATE 0` `F-RT-01` `Pandas` `old` `image` `≠ HEAD`, `data_lake_root` `via .git` **Problema resuelve** `Runtime` `reproducible` **No resuelve** `Kafka` `Bronze` **Criterios** `docker inspect` `HEAD` `polars` `no Pandas` `systemctl` `active` `data_lake_root` `env` `fallback`.

**STEP 2 `MD-INFRA-02` Kafka/Bronze operational** `MD-BRONZE-01` — Objetivo `KafkaBronzeWriter` `group ocm-bronze-writer` `Iceberg` `Bronze` `14.097` `unconsumed` → `Bronze` `parquet` **Dependencias** `STEP 1` `Runtime` `Dependencia técnica real` `Pandas` `old` `image` `Bronze` `0` **Problema** `Kafka` `ohlcv.raw` `14.097` `unconsumed` **No resuelve** `Dataset` `Silver` **Criterios** `find Bronze parquet` `health 200`.

**STEP 3 `MD-DATASET-01` Dataset observable** — Objetivo `Bronze` `Silver` `Gold` `gap_scanner` `2× timeframe` `Quality` `is_new` **Dependencias** `STEP 2` `Bronze` `operacional` **Problema** `Dataset` `completeness` `NOT DEMONSTRATED` **No resuelve** `Recovery` `contracts` **Criterios** `find Bronze/Silver/Gold` `parquet` `Silver` `Gold` `runner` `demonstrado`.

**STEP 4 `OH-RECOVERY-01` Recovery contracts `Quality` `→` `REST` `OHLCV` `repair`** `MD-RECOVERY-001/002/003` `RepairStrategy` `BackfillStrategy` `IncrementalStrategy` `QualityPipeline` `reconciliation` `PipelineContext.storage = None` `desconectada` **Dependencias** `STEP 3` `Dataset` `observable` `GATE 3` `Recovery contracts` `BLOCKED` `canonical` `multi-exchange` `UNKNOWN` **Problema** `OHLCV gap` `→` `REST` `repair` `MISSING` **No resuelve** `OrderBook` `resync` **Criterios** `gap` `→` `REST` `fetch_ohlcv` `Bronze` `Silver` `Gold` `reconciliation`.

**STEP 5 `OB-SNAPSHOT-01` `PR-A` `OrderBookSnapshotFetcher` `REST` `Bybit` `limit 50` `u` `bids/asks` `OrderBookSnapshotPayload`** **Dependencias** `STEP 4` `Recovery contracts` `GATE 3` `READY WITH EXPLICIT CONTRACTS` `Bybit` `REST` `snapshot` `contract` `OrderBookSnapshotPayload` **Problema** `MISSING` `REST` `snapshot` `capability` **No resuelve** `GAP → REST` `resync` `race` **Criterios** `fetch_snapshot` `Bybit` `category=linear` `symbol=BTCUSDT` `limit 50` `update_id` `int` `>0` `bids/asks` `str` `timestamp_ms` `int` `tests` `snapshot parser` `Bybit`.

**STEP 6 `OB-RESYNC-01` `PR-B` `GAP → REST snapshot → rebuild → resume` `race` `snapshot ↔ WS`** **Dependencias** `STEP 5` `PR-A` `Fetcher` `GATE 4` `BLOCKED BY ARCHITECTURAL UNKNOWN` `canonical OrderBook contract` `multi-exchange` `sequence` `UNKNOWN` + `Bybit` `REST` `snapshot` `u` `exact` `UNKNOWN` + `dataset` `Bronze` `orderbook` `historical` `NOT DEMONSTRATED` + `race` `buffering` `UNKNOWN` **Problema** `OrderBook` `u-jump` without `disconnect` `STALE` `2s` **No resuelve** `global OHLCV` `Bronze` **Criterios** `gap` `105` `→` `REST` `snapshot` `200` `→` `on_snapshot` `last=200` `→` `201` `DELTA_APPLIED` `test` `gap→snapshot→delta` `metrics` `orderbook.resync_total`.

**Agrupación solo si `DEPENDENCIA TÉCNICA REAL`:** `MD-INFRA-01` `→` `MD-INFRA-02` `→` `MD-DATASET-01` `→` `OH-RECOVERY-01` `→` `OB-SNAPSHOT-01` `→` `OB-RESYNC-01` **secuencial** `Runtime` `→` `Kafka/Bronze` `→` `Dataset` `→` `Recovery` `→` `OrderBook` (no `PR-A → PR-B` directo sin `GATE 3`).

## 20. Nomenclatura PR

- `MD-INFRA-01` `Runtime correctness` `Docker` `HEAD` `data_lake_root` `fallback` `systemd`
- `MD-BRONZE-01` `Kafka/Bronze` `ocm-bronze-writer` `Iceberg`
- `MD-DATASET-01` `Dataset observable` `Bronze/Silver/Gold` `gap` `Quality`
- `OH-RECOVERY-01` `OHLCV Repair` `Quality` `→` `REST` `fetch_ohlcv` `reconciliation`
- `OB-SNAPSHOT-01` `OrderBookSnapshotFetcher` `REST` `Bybit` `limit 50` `u`
- `OB-RESYNC-01` `OrderBook Resync` `GAP → REST → rebuild → resume` `race`

## 21. Criterio READY TO IMPLEMENT

**Una iniciativa `READY TO IMPLEMENT` si:**

1. `contrato` `OrderBookSnapshotPayload` `Bybit` `REST` `u` `bids/asks` `sufficiently defined` `YES` `PR-A` `READY WITH EXPLICIT CONTRACTS`
2. `dependencia` `GATE 3` `Recovery contracts` `known` `NO` `OH-RECOVERY-01` `BLOCKED` `canonical` `multi-exchange` `UNKNOWN`
3. `source of truth` `Bybit` `u` `known` `PARTIAL` `REST` `snapshot` `u` `exact` `UNKNOWN`
4. `runtime target` `systemd` `ocm-streaming` `active` `YES` `GATE 0` `PARTIAL` `old` `Pandas` `F-RT-01`
5. `tests` `definable` `gap→snapshot→delta` `YES` `PR-A` `READY` `test` `snapshot parser`
6. `criterios` `verificables` `fetch_snapshot` `update_id` `>0` `bids/asks` `str` `YES`
7. `no` `UNKNOWN` `crítico` `NO` `multi-exchange` `sequence` `UNKNOWN` `dataset` `Bronze` `NOT DEMONSTRATED` `race` `UNKNOWN` → **BLOCKED**

**En caso contrario:** `READY WITH EXPLICIT CONTRACTS` `PR-A` (`contract` `explicit` `Bybit` `limit 50` `update_id` `u` `bids/asks`), `BLOCKED BY ARCHITECTURAL UNKNOWN` `PR-B` (`canonical` `multi-exchange` `sequence` `UNKNOWN` + `dataset` `NOT DEMONSTRATED` + `race` `UNKNOWN`).

## 22. Resultado Final

**1. Evidence Reviewed:** `OB-RESYNC-001` `3` `AUDIT_OCM_comparative` `Hummingbot` `Nautilus` `Freqtrade` `market-data-e2e-incompleteness` `14.097` `F-RT-01` `Pandas` `dry_run` `Silver/Gold` `no runner` `gap` `auto-heal` `healthcheck` `false` `KuCoin/feeds/env` `symbols` `REST/CCXT` `BTC/USDT` vs `WS` `BTC-USDT-PERP`.

**2. Current E2E:** `Exchange → REST/WS → Adapters → Application → Kafka → Raw (ohlcv.raw 14.097, orderbook.raw u) → Bronze (0) → Quality (is_new) → Silver (0) → Gold (0) → Trading` `PARTIAL` `Runtime` `Docker` `old` `Pandas` `≠ HEAD` `polars`.

**3. Runtime Reality:** `Docker` `old` `Pandas` `F-RT-01` `≠ HEAD` `polars` `systemd` `ocm-streaming active` `ocm-market-data inactive` `Kafka` `ocm-bronze-writer` `UNKNOWN` `Bronze` `0`.

**4. Kafka Reality:** `ohlcv.raw` `14.097` `unconsumed` `consumer group` `UNKNOWN` `not` `ocm-bronze-writer` `orderbook.raw` `u` `orderbook` `1h`.

**5. Bronze Reality:** `NOT DEMONSTRATED` `0` `parquet` `Iceberg` `catalog.db` `warehouse` `old` `Pandas` `F-RT-01`.

**6. Dataset Completeness:** `NOT DEMONSTRATED` `Bronze` `0` `Silver` `0` `Gold` `0` `gap` `UNKNOWN`.

**7. Silver/Gold:** `MISSING` `no runner`.

**8. Quality/Gap:** `Quality` `is_new` `YES` `gap_scanner` `2× timeframe` `YES` `GapRecoveryFetcher` `trades` `gap` `YES` `OHLCV` `gap` `→` `REST` `MISSING`.

**9. OHLCV Recovery:** `MD-RECOVERY-001/002/003` `RepairStrategy` `desconectada` `PipelineContext.storage = None` `IMPLEMENTED` `NOT CONNECTED`.

**10. OrderBook Recovery:** `WS` `snapshot` `delta` `u` `GAP_DETECTED` `reset` `wait WS` `snapshot` `PARTIAL` `WS` `snapshot` `passive` `YES` `REST` `snapshot` `MISSING`.

**11. Exchange Abstraction:** `Bybit` `first` `implementation` `not` `UNIVERSAL` `Binance` `U` `pu` `UNKNOWN` `multi-exchange` `sequence` `UNKNOWN`.

**12. Source-of-Truth:** `Live OrderBook` `Exchange` `Bybit` `WS` `u` `CONFIRMED`, `REST snapshot` `Exchange` `Bybit` `GET /v5` `CONFIRMED`, `Bronze raw` `Kafka` `persisted` `CONFIRMED`, `Silver` `Gold` `historical` `MISSING`.

**13. Capability Matrix:** `REST snapshot` `Bybit` `MISSING` `WS OrderBook` `YES` `PARTIAL` `sequence` `YES` `gap` `YES` `reconnect` `YES` `resync` `MISSING` `rebuild` `YES` `Bronze` `MISSING`.

**14. Architectural Unknowns:** `canonical OrderBook contract` `multi-exchange` `sequence` `UNKNOWN` + `Bybit` `REST` `snapshot` `u` `exact` `UNKNOWN` + `dataset` `Bronze` `NOT DEMONSTRATED` + `race` `buffering` `UNKNOWN`.

**15. Dependency Graph:** `Runtime correctness (F-RT-01, .git)` `→` `Kafka correctness` `→` `Bronze operational` `→` `Dataset observable` `→` `Recovery contracts` `→` `OHLCV recovery` `→` `OrderBook snapshot` `→` `OrderBook resync`.

**16. Implementation Gates:** `GATE 0` `Runtime reproducible` `NOT READY` `F-RT-01` `Pandas` `old` `≠ HEAD` `data_lake_root` `via .git`; `GATE 1` `Kafka/Bronze operacional` `NOT READY` `Bronze 0` `consumer group`; `GATE 2` `Dataset observable` `NOT READY` `Bronze` `NOT DEMONSTRATED`; `GATE 3` `Recovery contracts` `NOT READY` `canonical` `UNKNOWN`; `GATE 4` `OrderBook snapshot` `READY WITH EXPLICIT CONTRACTS` `PR-A` `Bybit` `limit 50` `u`; `GATE 5` `OrderBook resync` `BLOCKED`.

**17. Proposed PR Sequence:** `STEP 1 MD-INFRA-01` `Runtime` `→` `STEP 2 MD-INFRA-02` `Kafka/Bronze` `→` `STEP 3 MD-DATASET-01` `Dataset` `→` `STEP 4 OH-RECOVERY-01` `Recovery contracts` `→` `STEP 5 OB-SNAPSHOT-01` `PR-A` `→` `STEP 6 OB-RESYNC-01` `PR-B`.

**18. PR Dependencies:** `MD-INFRA-01` no deps → `MD-BRONZE-01` `MD-INFRA-01` → `MD-DATASET-01` `MD-BRONZE-01` → `OH-RECOVERY-01` `MD-DATASET-01` `GATE 3` → `OB-SNAPSHOT-01` `OH-RECOVERY-01` `GATE 4` `READY WITH CONTRACTS` → `OB-RESYNC-01` `OB-SNAPSHOT-01` `GATE 5` `BLOCKED`.

**19. Acceptance Criteria:** `MD-INFRA-01` `docker inspect HEAD polars no Pandas` `systemctl active` `data_lake_root env fallback` `GATE 0` `PASS`; `MD-BRONZE-01` `find Bronze parquet` `health 200` `14.097` `consumed` `GATE 1`; `OB-SNAPSHOT-01` `fetch_snapshot Bybit category=linear symbol=BTCUSDT limit 50 update_id>0 bids/asks str` `GATE 4`.

**20. Risks:** `Runtime` `old` `Pandas` `F-RT-01` `reproducibility` `high` `Pandas TypeError` `Bronze` `0` `Kafka` `ohlcv.raw` `14.097` `unconsumed` `healthcheck` `false` `KuCoin/feeds/env` `symbols` `REST/CCXT` `BTC/USDT` `vs` `WS` `BTC-USDT-PERP` `provenance`.

**21. Final Implementation Order:** `STEP 1 MD-INFRA-01` `→` `STEP 2 MD-INFRA-02` `→` `STEP 3 MD-DATASET-01` `→` `STEP 4 OH-RECOVERY-01` `→` `STEP 5 OB-SNAPSHOT-01` `→` `STEP 6 OB-RESYNC-01` **secuencial** `Runtime → Kafka/Bronze → Dataset → Recovery → OrderBook`.

**22. Final Architecture Gate:** `ARCHITECTURE NOT READY FOR IMPLEMENTATION` — `GATE 0` `Runtime` `F-RT-01` `Pandas` `old` `≠ HEAD` `data_lake_root` `via .git` `GATE 1` `Kafka/Bronze` `Bronze 0` `consumer group` `UNKNOWN` `GATE 2` `Dataset` `NOT DEMONSTRATED` `GATE 3` `Recovery contracts` `canonical` `UNKNOWN` `GATE 4` `OrderBook snapshot` `READY WITH EXPLICIT CONTRACTS` but `BLOCKED` `GATE 5` `OrderBook resync` `BLOCKED BY ARCHITECTURAL UNKNOWN` (`canonical` `multi-exchange` `sequence` `UNKNOWN` + `Bybit` `REST` `snapshot` `u` `exact` `UNKNOWN` + `dataset` `Bronze` `NOT DEMONSTRATED` + `race` `UNKNOWN`).

## 23. Final Architecture Gate

**ARCHITECTURE NOT READY FOR IMPLEMENTATION**

**Blockers:**

1. `GATE 0` `Runtime correctness` `F-RT-01` `Pandas TypeError` `old` `image` `≠ HEAD` `polars` `MIGRATION COMPLETE` `data_lake_root() sin .git` `fallback` `REQUIRED` `VALID`.
2. `GATE 1` `Kafka/Bronze operational` `consumer group` `ocm-bronze-writer` `not appears` `Bronze` `0` `parquet` `Iceberg` `NOT DEMONSTRATED`.
3. `GATE 2` `Dataset observable` `Bronze` `Silver` `Gold` `gap_scanner` `2× timeframe` `NOT DEMONSTRATED` `Bronze` `0`.
4. `GATE 3` `Recovery contracts` `canonical OrderBook contract` `multi-exchange` `sequence` `UNKNOWN` `Bybit` `first` `not` `UNIVERSAL` `Quality` `→` `REST` `OHLCV` `repair` `MISSING`.
5. `GATE 4` `OrderBook snapshot` `READY WITH EXPLICIT CONTRACTS` but `BLOCKED` `GATE 5` `OrderBook resync` `race` `buffering` `UNKNOWN` `dataset` `NOT DEMONSTRATED`.

**What can begin:** `MD-INFRA-01` `Runtime` `fallback data_lake_root() sin .git` `VALID` `P0` `BLOCKER` (no `UNKNOWN` `crítico`).

**What remains blocked:** `MD-BRONZE-01` `GATE 1` `OH-RECOVERY-01` `GATE 3` `OB-SNAPSHOT-01` `GATE 4` `READY WITH CONTRACTS` but `BLOCKED` `OB-RESYNC-01` `GATE 5` `BLOCKED`.
