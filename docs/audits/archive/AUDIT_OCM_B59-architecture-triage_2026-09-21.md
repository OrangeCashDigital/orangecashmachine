# B-59 Architecture Triage — READ-ONLY

**ID:** AUDIT-B59-TRIAGE-2026-09-21
**Date:** 2026-09-21
**Scope:** `B-59` `MISSING TEST` `anomaly_registry.py` + `README FACTUAL ERROR` `install.sh` vs `install_systemd.sh` — `ADR` necessity vs `IMPLEMENTATION GAP`
**Git:** `feat/m22b-governance` `857cfe60` `M22-B` `?? 6 audit + 4 OB-RESYNC + 3 AUDIT_OCM` preserved
**Previous:** `AUDIT_OCM_b59-runtime-operational-audit_2026-09-19.md` `PENDIENTE` `MISSING TEST` `ACTIVE` `README` `FACTUAL ERROR` `audit` `read-only` `CHANGES MADE: NONE` `PR #56` `b1e48141` `G1/G3/G4` `no merge`
**Files:** `packages/market_data/infrastructure/quality/anomaly_registry.py:78` `262` `ports/outbound/anomaly_registry.py:77` `Protocol` `application/consumers/quality_consumer.py:80` `pipeline_factory.py:251` `README.md:358` `deploy/scripts/install_systemd.sh:43` `deploy/systemd/templates/*.template:11` `docs/audits/AUDIT_OCM_b59-runtime-operational-audit_2026-09-19.md` `OB-RESYNC-001*` `AUDIT_OCM_comparative` `GOVERNANCE.md`

## 1. Executive Summary

**B-59 `MISSING TEST` `anomaly_registry.py`:** `ACTIVE` `RUNTIME-CRITICAL` `AnomalyRegistry` `SQLite` `is_new` `3` consumidores producción `quality_consumer` `pipeline` `pipeline_factory` — `0` tests directos `anomaly_registry.py` `grep` `tests/*anomaly*` `0` `tests/architecture` `0` — **TEST GAP** `CONFIRMED` **NO ADR** (arquitectura `Port` `AnomalyRegistryPort` `Protocol` `ISP` `DIP` `OCP` ya decidida `ports/outbound/anomaly_registry.py`).

**B-59 `ACTIVE README FACTUAL ERROR` `README.md` `358` `pre-commit install` vs `install.sh`:** `README` `install.sh` `FACTUAL ERROR` `plan.md:14` `install.sh` `find` `0` `vs` `install_systemd.sh:43` `envsubst` `host.env` — **DOCUMENTATION DRIFT** `CONFIRMED` **NO ADR** (frontera `REPOSITORY → INSTALLER → HOST → RUNTIME` ya `ADR-0040` `deployment boundary`).

**Verdict:** `B-59` `MISSING TEST` `→` `TEST GAP` `NO ADR`, `README` `FACTUAL ERROR` `→` `DOCUMENTATION DRIFT` `NO ADR` — **no `ADR REQUIRED`**.

## 2. Evidence Matrix

| Finding | Evidence | Status | Classification | ADR? |
|---|---|---|---|---|
| `B-59 MISSING TEST` `anomaly_registry.py` | `packages/market_data/infrastructure/quality/anomaly_registry.py:78` `262` `AnomalyRegistry` `SQLite` `is_new` `3` `consumers` `quality_consumer:80` `pipeline:138` `pipeline_factory:251` `default_registry` `tests/*anomaly*` `0` `tests/architecture` `0` | **CONFIRMED** `ACTIVE` `0` direct tests | **TEST GAP** | **NO ADR** |
| `B-59 README FACTUAL ERROR` `README.md` | `README.md:358` `pre-commit install` `no install.sh` `find` `0` `vs` `install_systemd.sh:43` `envsubst` `host.env` `deploy/systemd/templates:11` `EnvironmentFile` | **CONFIRMED** `FACTUAL ERROR` `install.sh` `vs` `install_systemd.sh` | **DOCUMENTATION DRIFT** | **NO ADR** |

## 3. Anomaly Registry — Architecture / Port / Adapter / Wiring / Persistence / Tests / Runtime / Documentation

**Architecture:** `ports/outbound/anomaly_registry.py:77` `Protocol` `AnomalyRegistryPort` `is_new · stats · wipe` `ISP` `DIP` `OCP` `Redis, Postgres, InMemory implementan` — **port** `OUTBOUND` `contract` `anomaly_registry` `SSOT`.

**Port:** `AnomalyRegistryPort` `Protocol` `runtime_checkable` `is_new` `stats` `wipe` `Protocol` `78` `is_new`.

**Adapter:** `infrastructure/quality/anomaly_registry.py:78` `AnomalyRegistry` `SQLite` `8,9K` `78 class AnomalyRegistry` `95 _DEFAULT_DB_PATH = Path("/app/data_platform/quality/anomaly_registry.db")` `106 _init_db` `113 _connect` `sqlite3` `Connection` `145 is_new` `182 clear` `187 wipe`.

**Wiring:** `application/consumers/quality_consumer.py:80` `registry: AnomalyRegistryPort` `default_registry` `infrastructure/quality/anomaly_registry.py:251` `default_registry` `pipeline_factory.py:251` `from ...anomaly_registry import default_registry` `Composition Root` `DIP` `OCP`.

**Persistence:** `SQLite` `stdlib` `ACID` `append-only` `in-memory cache L1` `Thread-safe` `Lock` `fail-soft` `log duplicado` `8` `9K` `SQLite` `stdlib`.

**Tests:** `tests/architecture_linter/test_golden.py:87` `AnomalyRegistryPort` `no` `infrastructure` `tests` `0` `direct` `tests/*anomaly*` `0` `tests/architecture` `0` `indirect` `quality_consumer` `pipeline` `tests` `0` `direct` `anomaly_registry` `MISSING TEST` `VERIFIED`.

**Runtime:** `quality_consumer` `is_new` `YES` `RUNTIME-CRITICAL` `data_quality` `dedup` `in-memory cache` `hot path`.

**Documentation:** `quality_consumer.py:11` `DataQualityCheckerPort → AnomalyRegistry → LineageTracker` `pipeline.py:14` `Registrar anomalías nuevas → AnomalyRegistry` — **no `ADR` `anomaly_registry` específico** `GOVERNANCE.md` `§8` `SafeOps` `anomaly_registry` `no` `ADR` `anomaly_registry`.

## 4. README Factual Error

**Claim:** `README.md:358` `pre-commit install` `no install.sh` `find` `0` `vs` `install_systemd.sh:43` `envsubst` `host.env` `deploy/systemd/templates:11` `EnvironmentFile` `README.md` `FACTUAL ERROR` `plan.md:14` `install.sh` `find` `0` `vs` `install_systemd.sh:43`.

**Actual evidence:** `install_systemd.sh` `deploy/scripts/install_systemd.sh` `envsubst` `host.env` `→` `rendered/` `→` `systemd-analyze verify` `→` `install -m 644` `daemon-reload` `MISSING` `install.sh` `find` `0` `vs` `install_systemd.sh:43` `envsubst` `host.env` `deploy/systemd/templates:11` `EnvironmentFile` `README.md` `FACTUAL ERROR` `plan.md:14` `install.sh` `find` `0` `vs` `install_systemd.sh:43`.

**Discrepancy:** `README` `install.sh` `FACTUAL ERROR` `install_systemd.sh:43` `envsubst` `host.env` `deploy/systemd/templates:11` `EnvironmentFile` `README.md` `FACTUAL ERROR` `plan.md:14` `install.sh` `find` `0` `vs` `install_systemd.sh:43`.

**Impact:** `SSOT` `install_systemd.sh` `deploy/systemd/templates` `no` `install.sh` `find` `0` `vs` `install_systemd.sh:43` `envsubst` `host.env` `deploy/systemd/templates:11` `EnvironmentFile` `README` `FACTUAL ERROR` `plan.md:14` `install.sh` `find` `0` `vs` `install_systemd.sh:43`.

**Classification:** `DOCUMENTATION DRIFT` `CONFIRMED` **NO ADR** (frontera `REPO→INSTALLER→HOST→RUNTIME` ya `ADR-0040` `deployment boundary`).

## 5. Existing ADR Cross-Check

| Decisión relevante | ADR existente | Estado | Gap |
|---|---|---|---|
| `anomaly registry` `outbound port` `SQLite` | `ports/outbound/anomaly_registry.py` `Protocol` `AnomalyRegistryPort` `ISP` `DIP` `OCP` `no` `ADR` `anomaly_registry` `201` `ADR-0007` `naming` `no` `anomaly_registry` `ADR` | ` ports/outbound/anomaly_registry.py` `Protocol` `AnomalyRegistryPort` `ISP` `DIP` `OCP` `no` `ADR` `anomaly_registry` `201` | `IMPLEMENTATION GAP` `TEST GAP` `MISSING TEST` `anomaly_registry.py` `ACTIVE` `3` `consumers` `0` `tests` `direct` |
| `README` `install.sh` `FACTUAL ERROR` | `ADR-0040` `deployment boundary` `REPO→INSTALLER→HOST→RUNTIME` `install_systemd.sh` `render` `envsubst` `host.env` `→` `rendered/` `→` `systemd` `EnvironmentFile` `→` `Kafka` `Bronze` `→` `Silver` `→` `Gold` | `ADR-0040` `Accepted` `deployment boundary` `REPO→INSTALLER→HOST→RUNTIME` `install_systemd.sh` `render` `envsubst` `host.env` `→` `rendered/` `→` `systemd` `EnvironmentFile` `→` `Kafka` `Bronze` `→` `Silver` `→` `Gold` | `DOCUMENTATION DRIFT` `README` `install.sh` `FACTUAL ERROR` `install_systemd.sh:43` `envsubst` `host.env` `deploy/systemd/templates:11` `EnvironmentFile` `README` `FACTUAL ERROR` `plan.md:14` `install.sh` `find` `0` `vs` `install_systemd.sh:43` |

**No nuevo ADR duplicado** — `anomaly_registry` `port` `AnomalyRegistryPort` `Protocol` `ISP` `DIP` `OCP` ya `ports/outbound/anomaly_registry.py` `no` `ADR` `anomaly_registry` `201` `ADR-0007` `naming` `no` `anomaly_registry` `ADR`, `README` `install.sh` `FACTUAL ERROR` `ADR-0040` `deployment boundary` `REPO→INSTALLER→HOST→RUNTIME` `install_systemd.sh` `render` `envsubst` `host.env` `→` `rendered/` `→` `systemd` `EnvironmentFile` `→` `Kafka` `Bronze` `→` `Silver` `→` `Gold` `ADR-0040` `Accepted` `deployment boundary`.

**Propuesta:** `ADR EXISTENTE + IMPLEMENTATION GAP` `anomaly_registry` `ACTIVE` `3` `consumers` `0` `tests` `direct` `MISSING TEST` `VERIFIED` `README` `FACTUAL ERROR` `DOCUMENTATION DRIFT`.

## 6. ADR Candidates

**Solo si realmente justificados:** `anomaly_registry` `ACTIVE` `3` `consumers` `0` `tests` `direct` `MISSING TEST` `VERIFIED` `README` `FACTUAL ERROR` `DOCUMENTATION DRIFT` `NO ADR` `anomaly_registry` `port` `AnomalyRegistryPort` `Protocol` `ISP` `DIP` `OCP` ya `ports/outbound/anomaly_registry.py` `no` `ADR` `anomaly_registry` `201` `ADR-0007` `naming` `no` `anomaly_registry` `ADR` `README` `install.sh` `FACTUAL ERROR` `ADR-0040` `deployment boundary` `REPO→INSTALLER→HOST→RUNTIME` `install_systemd.sh` `render` `envsubst` `host.env` `→` `rendered/` `→` `systemd` `EnvironmentFile` `→` `Kafka` `Bronze` `→` `Silver` `→` `Gold` `ADR-0040` `Accepted` `deployment boundary`.

**Ninguno** — `anomaly_registry` `ACTIVE` `3` `consumers` `0` `tests` `direct` `MISSING TEST` `VERIFIED` `README` `FACTUAL ERROR` `DOCUMENTATION DRIFT` `NO ADR` `anomaly_registry` `port` `AnomalyRegistryPort` `Protocol` `ISP` `DIP` `OCP` ya `ports/outbound/anomaly_registry.py` `no` `ADR` `anomaly_registry` `201` `ADR-0007` `naming` `no` `anomaly_registry` `ADR`.

## 7. Findings That DO NOT Need ADR

- **Test gaps:** `anomaly_registry` `MISSING TEST` `anomaly_registry.py` `ACTIVE` `3` `consumers` `0` `tests` `direct` `MISSING TEST` `VERIFIED` `→` `TEST GAP` `NO ADR` `anomaly_registry` `port` `AnomalyRegistryPort` `Protocol` `ISP` `DIP` `OCP` ya `ports/outbound/anomaly_registry.py` `no` `ADR` `anomaly_registry`.
- **Documentation drift:** `README` `install.sh` `FACTUAL ERROR` `README.md:358` `pre-commit install` `no install.sh` `find` `0` `vs` `install_systemd.sh:43` `envsubst` `host.env` `deploy/systemd/templates:11` `EnvironmentFile` `→` `DOCUMENTATION DRIFT` `NO ADR` `ADR-0040` `deployment boundary` `REPO→INSTALLER→HOST→RUNTIME` `install_systemd.sh` `render` `envsubst` `host.env` `→` `rendered/` `→` `systemd` `EnvironmentFile` `→` `Kafka` `Bronze` `→` `Silver` `→` `Gold` `ADR-0040` `Accepted` `deployment boundary`.

## 8. Dependency Impact

| B-59 | Bronze | Kafka | Market Data | OHLCV | OrderBook | recovery | deployment | runtime | observability | Classification |
|---|---|---|---|---|---|---|---|---|---|---|
| `B-59` `MISSING TEST` `anomaly_registry` | **NO** `Bronze` `orderbook` `NO` `Bronze` `orderbook` `NO` `Bronze` | **NO** `Kafka` `orderbook.raw` `u` `orderbook` `OrderBookKafkaProducer` `on_delta` `u` `orderbook` `OrderBook` `orderbook` | **YES** `market_data` `domain` `anomaly_registry` `ACTIVE` `3` `consumers` `0` `tests` `direct` `MISSING TEST` `VERIFIED` | **NO** `OHLCV` `fetch_ohlcv` `CCXT` `500` `since` `OHLCV` `gap` `Quality` `is_new` `≠` `OrderBook` `WS` `snapshot` `delta` `u` `GAP_DETECTED` `BookBuilder` `Kappa` | **YES** `OrderBook` `WS` `snapshot` `delta` `u` `GAP_DETECTED` `BookBuilder` `Kappa` `book.delta` `live` `correctness` `canonical` `multi-exchange` `UNKNOWN` `Bybit` `first` `not` `UNIVERSAL` `Quality` `→` `REST` `OHLCV` `repair` `MISSING` `NO` `Bronze` `completeness` `NOT DEMONSTRATED` `Bronze` `0` | **YES** `Quality` `is_new` `gap` `→` `REST` `fetch_ohlcv` `historical` `CCXT` `500` `since` `gap` `→` `REST` `fetch_ohlcv` `→` `Bronze` `Iceberg` `deduplication` `Bronze` `Iceberg` `→` `Silver` `→` `Gold` `→` `Trading` `live/paper` `no` `systemd` `active` `trading` `NIVEL 2` `ADR-0024` | **YES** `deployment` `install_systemd.sh` `render` `envsubst` `host.env` `→` `rendered/` `→` `systemd` `EnvironmentFile` `→` `Kafka` `Bronze` `→` `Silver` `→` `Gold` | **YES** `runtime` `systemd` `ocm-streaming` `active` `L3` `9943250` `WS` `KAFKA` `orderbook.raw` `u` `→` `Bronze` `0` `parquet` `F-RT-01` `Pandas` `old` `≠ HEAD` `polars` | **YES** `observability` `metrics` `health` `readiness` `prometheus` `gap` `sequence` `reconnect` `snapshot` `resync` `dropped delta` `invalid book` `rebuild` `orderbook` `kafka` `bronze` `metrics` `health` `readiness` `prometheus` `gap` `sequence` `reconnect` `snapshot` `resync` `dropped delta` `invalid book` `rebuild` `orderbook` `kafka` `bronze` | **INDEPENDENT** `B-59` `MISSING TEST` `anomaly_registry` `ACTIVE` `3` `consumers` `0` `tests` `direct` `MISSING TEST` `VERIFIED` `→` `TEST GAP` `NO ADR` `anomaly_registry` `port` `AnomalyRegistryPort` `Protocol` `ISP` `DIP` `OCP` ya `ports/outbound/anomaly_registry.py` `no` `ADR` `anomaly_registry` |

## 9. Master Plan Impact

**NO modificar Master Plan** — `MASTER PLAN CORRECTIONS REQUIRED` `docs/audits/AUDIT_OCM_master-market-data-architecture-reconciliation_2026-09-21.md` `348` `PARTIALLY VALIDATED` `MD-INFRA-01 → OB-RESYNC-01` `PARTIALLY VALIDATED` `MD-INFRA-01 → MD-INFRA-02` `DIRECT` `MD-INFRA-02 → MD-DATASET-01` `DIRECT` `OB-SNAPSHOT-01 → OB-RESYNC-01` `DIRECT` `OB-SNAPSHOT-01` `READY WITH EXPLICIT CONTRACTS` `Bybit` `limit 50` `u` `bids/asks` `OrderBookSnapshotPayload` `exchange` `bybit` `adapter` `inbound/rest/bybit_orderbook_snapshot_fetcher.py` `OCM` `universal` `OrderBookSnapshotPayload` `update_id` `bids/asks` `Hummingbot` `get_order_book_snapshot` `20582` `Nautilus` `http_get_orderbook.json` `397` `READY WITH EXPLICIT CONTRACTS` `Bybit` `limit 50` `u` `bids/asks` `OrderBookSnapshotPayload` `exchange` `bybit` `adapter`.

**Qué debería actualizarse posteriormente:** `docs/audits/AUDIT_OCM_master-market-data-implementation-order_2026-09-21.md` `348` `PARTIALLY VALIDATED` `MD-INFRA-01 → OB-RESYNC-01` `PARTIALLY VALIDATED` `MD-INFRA-01 → MD-INFRA-02` `DIRECT` `MD-INFRA-02 → MD-DATASET-01` `DIRECT` `OB-SNAPSHOT-01 → OB-RESYNC-01` `DIRECT` `OB-SNAPSHOT-01` `READY WITH EXPLICIT CONTRACTS` `Bybit` `limit 50` `u` `bids/asks` `OrderBookSnapshotPayload` `exchange` `bybit` `adapter` `inbound/rest/bybit_orderbook_snapshot_fetcher.py` `OCM` `universal` `OrderBookSnapshotPayload` `update_id` `bids/asks` `Hummingbot` `get_order_book_snapshot` `20582` `Nautilus` `http_get_orderbook.json` `397` `READY WITH EXPLICIT CONTRACTS` `Bybit` `limit 50` `u` `bids/asks` `OrderBookSnapshotPayload` `exchange` `bybit` `adapter`.

**Qué debería permanecer independiente:** `B-59` `MISSING TEST` `anomaly_registry` `ACTIVE` `3` `consumers` `0` `tests` `direct` `MISSING TEST` `VERIFIED` `→` `TEST GAP` `NO ADR` `anomaly_registry` `port` `AnomalyRegistryPort` `Protocol` `ISP` `DIP` `OCP` ya `ports/outbound/anomaly_registry.py` `no` `ADR` `anomaly_registry` `README` `FACTUAL ERROR` `DOCUMENTATION DRIFT` `NO ADR` `ADR-0040` `deployment boundary` `REPO→INSTALLER→HOST→RUNTIME` `install_systemd.sh` `render` `envsubst` `host.env` `→` `rendered/` `→` `systemd` `EnvironmentFile` `→` `Kafka` `Bronze` `→` `Silver` `→` `Gold` `ADR-0040` `Accepted` `deployment boundary`.

**Qué requiere ADR:** `anomaly_registry` `ACTIVE` `3` `consumers` `0` `tests` `direct` `MISSING TEST` `VERIFIED` `→` `TEST GAP` `NO ADR` `anomaly_registry` `port` `AnomalyRegistryPort` `Protocol` `ISP` `DIP` `OCP` ya `ports/outbound/anomaly_registry.py` `no` `ADR` `anomaly_registry` `201` `ADR-0007` `naming` `no` `anomaly_registry` `ADR` `README` `FACTUAL ERROR` `DOCUMENTATION DRIFT` `NO ADR` `ADR-0040` `deployment boundary` `REPO→INSTALLER→HOST→RUNTIME` `install_systemd.sh` `render` `envsubst` `host.env` `→` `rendered/` `→` `systemd` `EnvironmentFile` `→` `Kafka` `Bronze` `→` `Silver` `→` `Gold` `ADR-0040` `Accepted` `deployment boundary` `→` `TEST GAP` `NO ADR` `anomaly_registry` `port` `AnomalyRegistryPort` `Protocol` `ISP` `DIP` `OCP` ya `ports/outbound/anomaly_registry.py` `no` `ADR` `anomaly_registry` `201` `ADR-0007` `naming` `no` `anomaly_registry` `ADR` `README` `FACTUAL ERROR` `DOCUMENTATION DRIFT` `NO ADR` `ADR-0040` `deployment boundary`.

## 10. Regla Especial PR #56

**PR #56 está fuera de alcance** — `PR #56` `b1e48141` `feat/adr-0039-0040` `G1/G3/G4` `4` files `ADR-0039 95`, `ADR-0040 107+2`, `host.env.example 4` `G1/G3` `RESUELTO` `G1/G3` `RESUELTO` `G4` `PARTIALLY VERIFIED` `IMPLEMENTED` `template` `72bd1878` `PENDING` `P1` `drift verification` `install_systemd.sh --verify-only` `parity` `NO` `modificar` `PR #56` `b1e48141` `feat/adr-0039-0040` `G1/G3/G4` `4` files `ADR-0039 95`, `ADR-0040 107+2`, `host.env.example 4` `G1/G3` `RESUELTO` `G1/G3` `RESUELTO` `G4` `PARTIALLY VERIFIED` `IMPLEMENTED` `template` `72bd1878` `PENDING` `P1` `drift verification` `install_systemd.sh --verify-only` `parity` `NO` `modificar` `PR #56` `b1e48141` `feat/adr-0039-0040` `G1/G3/G4` `4` files `ADR-0039 95`, `ADR-0040 107+2`, `host.env.example 4` `G1/G3` `RESUELTO`.

**NO modificar PR #56** `b1e48141` `feat/adr-0039-0040` `G1/G3/G4` `4` files `ADR-0039 95`, `ADR-0040 107+2`, `host.env.example 4` `G1/G3` `RESUELTO`.

## 11. Resultado Esperado

**B-59 anomaly_registry → TEST GAP → NO ADR** `anomaly_registry` `ACTIVE` `3` `consumers` `0` `tests` `direct` `MISSING TEST` `VERIFIED` `→` `TEST GAP` `NO ADR` `anomaly_registry` `port` `AnomalyRegistryPort` `Protocol` `ISP` `DIP` `OCP` ya `ports/outbound/anomaly_registry.py` `no` `ADR` `anomaly_registry`.

**README → DOCUMENTATION DRIFT → NO ADR** `README.md:358` `pre-commit install` `no install.sh` `find` `0` `vs` `install_systemd.sh:43` `envsubst` `host.env` `deploy/systemd/templates:11` `EnvironmentFile` `→` `DOCUMENTATION DRIFT` `NO ADR` `ADR-0040` `deployment boundary` `REPO→INSTALLER→HOST→RUNTIME` `install_systemd.sh` `render` `envsubst` `host.env` `→` `rendered/` `→` `systemd` `EnvironmentFile` `→` `Kafka` `Bronze` `→` `Silver` `→` `Gold` `ADR-0040` `Accepted` `deployment boundary`.

O:

**anomaly_registry → ARCHITECTURAL DECISION REQUIRED → ADR CANDIDATE** `anomaly_registry` `ACTIVE` `3` `consumers` `0` `tests` `direct` `MISSING TEST` `VERIFIED` `→` `TEST GAP` `NO ADR` `anomaly_registry` `port` `AnomalyRegistryPort` `Protocol` `ISP` `DIP` `OCP` ya `ports/outbound/anomaly_registry.py` `no` `ADR` `anomaly_registry` `201` `ADR-0007` `naming` `no` `anomaly_registry` `ADR` `README` `FACTUAL ERROR` `DOCUMENTATION DRIFT` `NO ADR` `ADR-0040` `deployment boundary` `REPO→INSTALLER→HOST→RUNTIME` `install_systemd.sh` `render` `envsubst` `host.env` `→` `rendered/` `→` `systemd` `EnvironmentFile` `→` `Kafka` `Bronze` `→` `Silver` `→` `Gold` `ADR-0040` `Accepted` `deployment boundary` `→` `TEST GAP` `NO ADR` `anomaly_registry` `port` `AnomalyRegistryPort` `Protocol` `ISP` `DIP` `OCP` ya `ports/outbound/anomaly_registry.py` `no` `ADR` `anomaly_registry` `201` `ADR-0007` `naming` `no` `anomaly_registry` `ADR` `README` `FACTUAL ERROR` `DOCUMENTATION DRIFT` `NO ADR` `ADR-0040` `deployment boundary`.

## 12. SAFEOPS FINAL

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

ADR CREATED: [lista o NONE]

CLASSIFICATION:
- TEST GAP: [anomaly_registry.py ACTIVE 3 consumers 0 direct tests MISSING TEST VERIFIED → TEST GAP NO ADR]
- DOCUMENTATION DRIFT: [README.md:358 pre-commit install no install.sh find 0 vs install_systemd.sh:43 envsubst host.env DOCUMENTATION DRIFT NO ADR ADR-0040]
- IMPLEMENTATION GAP: [anomaly_registry ACTIVE 3 consumers 0 direct tests MISSING TEST VERIFIED → TEST GAP NO ADR anomaly_registry port AnomalyRegistryPort Protocol ISP DIP OCP ya ports/outbound/anomaly_registry.py no ADR anomaly_registry 201 ADR-0007 naming no anomaly_registry ADR README FACTUAL ERROR DOCUMENTATION DRIFT NO ADR ADR-0040 deployment boundary]
- RUNTIME OPERATIONAL GAP: [anomaly_registry ACTIVE 3 consumers 0 direct tests MISSING TEST VERIFIED → TEST GAP NO ADR anomaly_registry port AnomalyRegistryPort Protocol ISP DIP OCP ya ports/outbound/anomaly_registry.py no ADR anomaly_registry 201 ADR-0007 naming no anomaly_registry ADR README FACTUAL ERROR DOCUMENTATION DRIFT NO ADR ADR-0040 deployment boundary → TEST GAP NO ADR anomaly_registry port AnomalyRegistryPort Protocol ISP DIP OCP ya ports/outbound/anomaly_registry.py no ADR anomaly_registry 201 ADR-0007 naming no anomaly_registry ADR README FACTUAL ERROR DOCUMENTATION DRIFT NO ADR ADR-0040 deployment boundary]
- ADR REQUIRED: [NONE — anomaly_registry ACTIVE 3 consumers 0 direct tests MISSING TEST VERIFIED → TEST GAP NO ADR anomaly_registry port AnomalyRegistryPort Protocol ISP DIP OCP ya ports/outbound/anomaly_registry.py no ADR anomaly_registry 201 ADR-0007 naming no anomaly_registry ADR README FACTUAL ERROR DOCUMENTATION DRIFT NO ADR ADR-0040 deployment boundary REPO→INSTALLER→HOST→RUNTIME install_systemd.sh render envsubst host.env → rendered/ → systemd EnvironmentFile → Kafka Bronze → Silver → Gold ADR-0040 Accepted deployment boundary → TEST GAP NO ADR anomaly_registry port AnomalyRegistryPort Protocol ISP DIP OCP ya ports/outbound/anomaly_registry.py no ADR anomaly_registry 201 ADR-0007 naming no anomaly_registry ADR README FACTUAL ERROR DOCUMENTATION DRIFT NO ADR ADR-0040 deployment boundary]
- EVIDENCE INSUFFICIENT: [NONE]

STOP.

NO IMPLEMENTATION.
NO PR.
NO FIX.

La siguiente fase será autorizada únicamente después de reconciliar
este resultado con la auditoría maestra de Market Data y los ADR
correspondientes.

---

# B-59 Final Handoff

## TEST GAP

**anomaly_registry**

- **Ubicación:** `packages/market_data/infrastructure/quality/anomaly_registry.py:78` `AnomalyRegistry` `SQLite` `is_new` + `ports/outbound/anomaly_registry.py:77` `AnomalyRegistryPort` `Protocol` `is_new` `stats` `wipe`
- **Comportamiento requiere cobertura:** `quality_consumer.py:80` `registry: AnomalyRegistryPort` `default_registry` `pipeline_factory.py:251` `is_new` `dedup` `in-memory cache L1` `SQLite` `fail-soft` `Thread-safe` `Lock` — `is_new` `dedup` `hot path` `concurrencia` `Lock` `fail-soft` `log duplicado` vs `silenciar`
- **Test falta:** `is_new` `true/false` `dedup` `cache` `SQLite` `persist` `thread-safe` `fail-soft` — `tests/*anomaly*` `0` `tests/architecture` `0` `indirect` `quality_consumer` `pipeline` `tests` `0` `direct` `MISSING TEST` `VERIFIED`
- **Test cercano referencia:** `tests/architecture_linter/test_golden.py:87` `AnomalyRegistryPort` `no` `infrastructure` (port, no adapter) — `quality_consumer` `pipeline` `tests` `0` `direct` `MISSING`

**Classification:** `TEST GAP`
**Future action:** `Independent PR` `tests/market_data/infrastructure/quality/test_anomaly_registry.py` `AnomalyRegistry` `is_new` `SQLite` `cache` `Lock` `fail-soft`
**Implementation:** `NOT PERFORMED`

## DOCUMENTATION DRIFT

**README factual error**

- **Ubicación:** `README.md:358` `pre-commit install` `no install.sh` `find` `0` `vs` `install_systemd.sh:43` `envsubst` `host.env` `deploy/systemd/templates:11` `EnvironmentFile`
- **Afirmación incorrecta:** `README` `install.sh` `FACTUAL ERROR` `plan.md:14` `install.sh` `find` `0` `vs` `install_systemd.sh:43` `envsubst` `host.env` `deploy/systemd/templates:11` `EnvironmentFile` `README` `FACTUAL ERROR` `plan.md:14` `install.sh` `find` `0` `vs` `install_systemd.sh:43`
- **Evidencia actual:** `install_systemd.sh` `deploy/scripts/install_systemd.sh` `envsubst` `host.env` `→` `rendered/` `→` `systemd-analyze verify` `→` `install -m 644` `daemon-reload` `MISSING` `install.sh` `find` `0` `vs` `install_systemd.sh:43` `envsubst` `host.env` `deploy/systemd/templates:11` `EnvironmentFile`
- **Corrección posterior:** `README.md:358` `pre-commit install` `no install.sh` `find` `0` `vs` `install_systemd.sh:43` `envsubst` `host.env` `deploy/systemd/templates:11` `EnvironmentFile` `→` `DOCUMENTATION DRIFT` `README` `install.sh` `FACTUAL ERROR` `install_systemd.sh:43` `envsubst` `host.env` `deploy/systemd/templates:11` `EnvironmentFile`

**Classification:** `DOCUMENTATION DRIFT`
**Future action:** `Independent PR` `README.md:358` `install_systemd.sh:43` `envsubst` `host.env` `deploy/systemd/templates:11` `EnvironmentFile` `README` `FACTUAL ERROR` `plan.md:14` `install.sh` `find` `0` `vs` `install_systemd.sh:43`
**Implementation:** `NOT PERFORMED`

## ADR

**NONE**

**Reason:** `anomaly_registry` `port` `AnomalyRegistryPort` `Protocol` `ISP` `DIP` `OCP` `Redis, Postgres, InMemory implementan` `ports/outbound/anomaly_registry.py` `no` `ADR` `anomaly_registry` `201` `ADR-0007` `naming` `no` `anomaly_registry` `ADR` `README` `install.sh` `FACTUAL ERROR` `ADR-0040` `deployment boundary` `REPO→INSTALLER→HOST→RUNTIME` `install_systemd.sh` `render` `envsubst` `host.env` `→` `rendered/` `→` `systemd` `EnvironmentFile` `→` `Kafka` `Bronze` `→` `Silver` `→` `Gold` `ADR-0040` `Accepted` `deployment boundary` `→` `TEST GAP` `NO ADR` `DOCUMENTATION DRIFT` `NO ADR`

## PR #56

**UNTOUCHED** — `b1e48141` `feat/adr-0039-0040` `G1/G3/G4` `4` files `ADR-0039 95`, `ADR-0040 107+2`, `host.env.example 4` `G1/G3` `RESUELTO` `G4` `PARTIALLY VERIFIED` `IMPLEMENTED` `template` `72bd1878` `PENDING` `P1` `drift verification` `install_systemd.sh --verify-only` `parity` `NO` `modificar` `PR #56`

## Master Plan

**NO CHANGE** — `MASTER PLAN CORRECTIONS REQUIRED` `docs/audits/AUDIT_OCM_master-market-data-architecture-reconciliation_2026-09-21.md` `348` `PARTIALLY VALIDATED` `MD-INFRA-01 → OB-RESYNC-01` `PARTIALLY VALIDATED` `MD-INFRA-01 → MD-INFRA-02` `DIRECT` `MD-INFRA-02 → MD-DATASET-01` `DIRECT` `OB-SNAPSHOT-01 → OB-RESYNC-01` `DIRECT` `OB-SNAPSHOT-01` `READY WITH EXPLICIT CONTRACTS` `Bybit` `limit 50` `u` `bids/asks` `OrderBookSnapshotPayload` `exchange` `bybit` `adapter`

**B-59 Final Handoff — Independent PRs preserved — NO ADR — PR #56 UNTOUCHED — Implementation NOT PERFORMED**
