# TRACEABILITY MATRIX — REQUIREMENT → ARCHITECTURE → CODE → TEST → GATE

> **Fecha**: 2026-09-04
> **Formato**: Requirement → Architecture (ADR) → Implementation (Code) → Test → Production Gate → Status

---

## MARKET UNIVERSE & INSTRUMENT DISCOVERY

| Requirement | Architecture (ADR) | Implementation | Test | Gate | Status |
|-------------|-------------------|----------------|------|------|--------|
| Single Market Universe SSOT | ADR-XXXX (draft) | `config/market_data/universe.yaml` + `MarketUniverseProvider` | `test_market_universe.py` consistency | G5, G9 | MISSING |
| Canonical symbol format | ADR-XXXX | `shared/utils/symbol_normalizer.py` | `test_symbol_normalizer.py` roundtrip | G5, G9 | MISSING |
| Auto-discovery (`load_markets`) | ADR-0017, ADR-XXXX | `CCXTAdapter.discover_instruments()` | `test_auto_discover.py` | G5, G9 | MISSING |
| Instrument Metadata Registry | ADR-0017, ADR-XXXX | `domain/value_objects/instrument.py` | `test_instrument_registry.py` provenance | G5, G9 | MISSING |
| REST + WS consume same universe | ADR-0014 | `ConcretePipelineFactory` + `FeedOrchestrator` + `streaming_hydra` | `test_universe_consumer_parity.py` | G5, G9 | MISSING |
| Deprecate duplicate configs | ADR-XXXX | `config/env/development.yaml` + `feeds.yaml` comments | Config migration test | G5, G9 | MISSING |

---

## PROTOCOL DISCOVERY (ADR-0017)

| Requirement | Architecture | Implementation | Test | Gate | Status |
|-------------|--------------|----------------|------|------|--------|
| Bybit REST Discovery Profile | ADR-0017 | `CCXTAdapter` + `HistoricalFetcherAsync` | `test_bybit_rest_profile.py` | G5 | PARTIAL |
| Bybit WS Orderbook Profile | ADR-0017 | `CryptofeedOrderBookStream` | `test_bybit_ws_orderbook.py` | G5 | PARTIAL |
| Bybit WS Trades Profile | ADR-0017 | `BybitCryptofeedRunner` | `test_bybit_ws_trades.py` | G5 | COMPLETE |
| Bybit Funding/OI Profile | ADR-0017 | `FundingKafkaProducer` + runner | `test_bybit_funding.py` | G5 | MISSING |
| Bybit Liquidations Profile | ADR-0017 | `LiquidationsKafkaProducer` + runner | `test_bybit_liq.py` | G5 | MISSING |
| Contract Provenance tracking | ADR-0017 §9 | `test_schema_provenance.py` | `test_schema_provenance.py` | G5, G9 | PARTIAL (semilla) |
| Promotion Rule gate | ADR-0017 §14, ADR-0020 | `check_production_gates.py` | `test_promotion_rule.py` | G1-G11 | MISSING |
| Fixtures mensajes reales | ADR-0017 §12 | `tests/fixtures/bybit/` | Fixture validation | G5 | MISSING |

---

## ORDERBOOK FLOW

| Requirement | Architecture | Implementation | Test | Gate | Status |
|-------------|--------------|----------------|------|------|--------|
| WS L2_BOOK ingestion | ADR-0014, ADR-0022 | `CryptofeedOrderBookStream` | `test_cryptofeed_orderbook_stream.py` | G5 | COMPLETE |
| Snapshot + Delta translation | ADR-0014 | `_translate_and_dispatch` | `test_translate_dispatch.py` | G5 | COMPLETE |
| Kafka producer orderbook.raw | ADR-0014, BC-35 | `OrderBookKafkaProducer` | `test_orderbook_producer.py` | G5 | COMPLETE |
| Kafka consumer orderbook.raw | ADR-0014 | `KafkaConsumerAdapter.for_orderbook()` | `test_consumer_for_orderbook.py` | G5, G9 | MISSING |
| OrderbookBronzeWriter | ADR-0028 | `OrderbookBronzeWriter` | `test_orderbook_bronze_writer.py` | G9 | MISSING |
| ORDERBOOK_SNAPSHOT_SCHEMA | ADR-0028 | `schemas.py` IDs 301-313 | `test_schemas.py` | G9 | MISSING |
| ORDERBOOK_DELTA_SCHEMA | ADR-0028 | `schemas.py` IDs 301-311 | `test_schemas.py` | G9 | MISSING |
| Bronze tables orderbook | ADR-0028 | `BronzeStorage.append_snapshot/delta` | `test_bronze_orderbook.py` | G9 | MISSING |
| Orderbook Builder (BookState) | ADR-0028 | `application/orderbook/builder.py` | `test_bookstate.py` | G9 (calidad) | MISSING |
| Gap detection (sequence) | ADR-0028 | `BookState.detect_gap()` | `test_gap_detection.py` | G9 (calidad) | MISSING |
| Checksum validation | ADR-0028 | `BookState.validate_checksum()` | `test_checksum.py` | G9 (calidad) | MISSING |
| Snapshot recovery (REST) | ADR-0028 | `BookState._recover_gap()` | `test_gap_recovery.py` | G9 (calidad) | MISSING |
| Historical replay from Bronze | ADR-0028 | `OrderbookBuilder.rebuild_book_at()` | `test_historical_replay.py` | G9 (calidad) | MISSING |

---

## OHLCV / TRADES INGESTION

| Requirement | Architecture | Implementation | Test | Gate | Status |
|-------------|--------------|----------------|------|------|--------|
| OHLCV REST ingestion | ADR-0013, ADR-0014 | `CCXTAdapter` + `HistoricalFetcherAsync` + `OHLCVPipeline` | `test_ohlcv_pipeline.py` | G5, G9 | COMPLETE |
| Trades REST ingestion | ADR-0013, ADR-0014 | `CCXTAdapter` + `TradesFetcher` | `test_trades_pipeline.py` | G5 | COMPLETE |
| Trades WS ingestion | ADR-0013, ADR-0014 | `BybitCryptofeedRunner` + `FeedOrchestrator` | `test_feed_orchestrator.py` | G5 | COMPLETE |
| KafkaOHLCVPublisher fail-fast | ADR-0020, F-031 | `_build_kafka_publisher()` | `test_kafka_publisher_failfast.py` | G5, G9 | COMPLETE |
| Quality pipeline | ADR-0014 | `QualityPipelineConsumer` | `test_quality_consumer.py` | G7 | COMPLETE |
| Bronze writer OHLCV | ADR-0014 | `KafkaBronzeWriter` + `BronzeStorage` | `test_bronze_writer.py` | G9 | COMPLETE |
| Bronze schema OHLCV (12 fields) | ADR-0014 | `BRONZE_SCHEMA` IDs 1-12 | `test_schemas.py` | G9 | COMPLETE |
| Bronze freshness OHLCV <15min | ADR-0014 | `KafkaBronzeWriter` continuous | `check_production_gates.py --gate G9` | G9 | PASS |

---

## KAFKA & BRONZE INFRASTRUCTURE

| Requirement | Architecture | Implementation | Test | Gate | Status |
|-------------|--------------|----------------|------|------|--------|
| Kafka topics SSOT | BC-35 | `shared/kafka/topics.py` | `test_topics_constants.py` | G5, G6 | COMPLETE |
| Kafka schemas SSOT | BC-35 | `shared/kafka/schemas/` | `test_schemas_constants.py` | G5, G6 | COMPLETE |
| Schema Registry (Avro) | ADR-0018 | B-18 pending | `test_schema_registry.py` | G6 | MISSING |
| Kafka consumer groups | ADR-0014 | `KafkaConsumerAdapter` factories | `test_consumer_factories.py` | G5, G6 | COMPLETE |
| DLQ handling | ADR-0014, B-19 | `KafkaBronzeWriter` + `OrderbookBronzeWriter` | `test_dlq_handling.py` | G5, G6 | COMPLETE (OHLCV) |
| Bronze dedup L1+L2 | B-19 | `CompositeSeenFilter` + `RedisCursorStore` | `test_bronze_dedup.py` | G5, G6 | COMPLETE |
| Bronze retention | ADR-0014 | `bronze_retention.py` | `test_bronze_retention.py` | G6 | PARTIAL |
| Health checks infra | ADR-0020 | `health_check.sh` | `scripts/health_check.sh` | G6, G7 | COMPLETE |

---

## SYSTEM & PRODUCTION GATES

| Requirement | Architecture | Implementation | Test | Gate | Status |
|-------------|--------------|----------------|------|------|--------|
| Systemd units valid | ADR-0022 | `ocm-market-data.service`, `ocm-streaming.service` | `systemctl status` | G4 | PASS |
| Systemd restart test | ADR-0022, B-59 | Manual test doc | `test_systemd_restart.py` | G4 | NO_VERIFICADO |
| Kafka connectivity | ADR-0014 | `KafkaProducerAdapter`, `KafkaConsumerAdapter` | `check_production_gates.py --gate G5` | G5 | PARTIAL |
| Infra health | ADR-0020 | `health_check.sh` | `scripts/health_check.sh` | G6, G7 | PASS |
| HTTP health endpoints | ADR-0022 | `market_data.main` `/health`, `/ready` | `curl localhost:8001/health` | G7 | PASS |
| Streaming health endpoint | ADR-0022 | `streaming_hydra.py` (TODO) | `curl streaming:port/health` | G7 | MISSING |
| IS_STUB = FALSE | ADR-0016 | `live_executor.py:80` | Code inspection | G8 | PASS |
| Bronze freshness OHLCV | ADR-0014 | `KafkaBronzeWriter` continuous | `check_production_gates.py --gate G9` | G9 | PASS |
| Bronze freshness Orderbook | ADR-0028 | `OrderbookBronzeWriter` | `check_production_gates.py --gate G9` | G9 | MISSING |
| Position single owner | ADR-0021 | `PortfolioService` + `PositionStore` | `test_position_store_unicity.py` | G10 | PENDIENTE |
| Tracing context | ADR-0017, B-17 | `BaseConsumer._traced_handle` | `test_tracing.py` (14 tests) | G11 | PASS |
| Order fill reconciliation | ADR-0029, B-MD-009 | `OMS` + `fill_sync` | `test_oms_cancel_lifecycle.py` | G12 | PARTIAL |
| Strategy signal validation | ADR-0014 | `StrategyConsumer` | `test_strategy_consumer.py` | G13 | MISSING |

---

## SILVER / GOLD / ORDER FLOW

| Requirement | Architecture | Implementation | Test | Gate | Status |
|-------------|--------------|----------------|------|------|--------|
| Silver OHLCV | ADR-0014 | `silver_storage.py` | `test_silver_storage.py` | — | COMPLETE |
| Silver Trades | ADR-0014 | `trades_storage.py` | `test_trades_storage.py` | — | COMPLETE |
| Silver Derivatives | ADR-0014 | `derivatives_storage.py` | `test_derivatives_storage.py` | — | COMPLETE |
| Silver Orderbook | ADR-0028 | `orderbook_storage.py` (NUEVO) | `test_silver_orderbook.py` | — | MISSING |
| Gold features OHLCV | ADR-0014 | `gold_storage.py` + `transformer.py` | `test_gold_storage.py` | — | COMPLETE |
| Gold Orderbook features | ADR-0028 | `orderbook_transformer.py` (NUEVO) | `test_gold_orderbook.py` | — | MISSING |
| OFI feature | ADR-0028 | `orderbook_features.py` (NUEVO) | `test_ofi.py` | — | MISSING |
| CVD feature | ADR-0028 | `orderbook_features.py` (NUEVO) | `test_cvd.py` | — | MISSING |
| Microprice feature | ADR-0028 | `orderbook_features.py` (NUEVO) | `test_microprice.py` | — | MISSING |
| FeatureReaderPort | ADR-0014 | `gold_reader.py` | `test_feature_reader.py` | — | COMPLETE |

---

## RISK / EXECUTION

| Requirement | Architecture | Implementation | Test | Gate | Status |
|-------------|--------------|----------------|------|------|--------|
| Order cancellation | ADR-0029 | `OMS.cancel()` + `OrderTransport.cancel()` | `test_oms_cancel_lifecycle.py` (18 tests) | G12 | COMPLETE |
| Fill reconciliation | ADR-0029 | `fill_sync.py` + `OMS` | `test_fill_sync.py` | G12 | PARTIAL |
| Balance reconciliation | ADR-0030 | `PortfolioService` + `fetch_balance` | `test_balance_reconciliation.py` | G10, G12 | MISSING |
| Position state single owner | ADR-0021 | `PositionStore` (Redis/InMemory) | `test_position_store_unicity.py` | G10 | PENDIENTE |
| Live executor IS_STUB=False | ADR-0016 | `LiveExecutor` + `OrderTransport` | `test_live_executor.py` | G8 | PASS |
| Paper executor | ADR-0016 | `PaperExecutor` | `test_paper_executor.py` | G8 | PASS |

---

## RESUMEN DE COBERTURA

| Categoría | Requirements | COMPLETE | PARTIAL | MISSING | PENDIENTE |
|-----------|--------------|----------|---------|---------|-----------|
| Market Universe | 6 | 0 | 0 | 6 | 0 |
| Protocol Discovery | 8 | 1 | 1 | 6 | 0 |
| Orderbook Flow | 16 | 3 | 0 | 13 | 0 |
| OHLCV/Trades | 8 | 7 | 0 | 1 | 0 |
| Kafka/Bronze | 8 | 6 | 1 | 1 | 0 |
| System/Gates | 14 | 8 | 2 | 4 | 0 |
| Silver/Gold | 10 | 4 | 0 | 6 | 0 |
| Risk/Execution | 7 | 2 | 2 | 2 | 1 |
| **TOTAL** | **77** | **31** | **6** | **39** | **1** |

**% COMPLETE**: 40% | **% PARTIAL**: 8% | **% MISSING**: 51% | **% PENDIENTE**: 1%