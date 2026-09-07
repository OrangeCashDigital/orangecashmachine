# MASTER CORRECTION PLAN — OCM MARKET DATA

> **Fecha**: 2026-09-04
> **Basado en**: READ-ONLY audit completo, GAP_ANALYSIS, B-49 status
> **Orden**: Por dependencia arquitectónica, NO por facilidad
> **Estados**: COMPLETE | PARTIAL | MISSING | NOT APPLICABLE | NEEDS AUDIT

---

## PHASE 0 — BASELINE / FREEZE
**Objetivo**: Congelar estado actual, documentar todo, sin cambios de código.

| Item | Estado | Entregable |
|------|--------|------------|
| Documentar arquitectura REAL | ✅ COMPLETE | `docs/architecture/real/ARCHITECTURE_REAL.md` |
| Documentar arquitectura TARGET | ✅ COMPLETE | `docs/architecture/real/ARCHITECTURE_TARGET.md` |
| Gap Analysis completo | ✅ COMPLETE | `docs/architecture/real/GAP_ANALYSIS.md` |
| Market Universe analysis | ✅ COMPLETE | `docs/architecture/real/MARKET_UNIVERSE_ANALYSIS.md` |
| Orderbook flow analysis | ✅ COMPLETE | `docs/architecture/real/ORDERBOOK_FLOW_ANALYSIS.md` |
| B-49 status con evidencia | ✅ COMPLETE | `docs/architecture/real/B49_STATUS.md` |
| PR #25 scope definition | ✅ COMPLETE | `docs/architecture/real/PR25_ANALYSIS.md` |
| Unified adapter analysis | ✅ COMPLETE | `docs/architecture/real/UNIFIED_ADAPTER_ANALYSIS.md` |
| Freeze tracking.yaml B-49 = PARTIAL | 🔄 IN PROGRESS | Actualizar tracking.yaml |
| Freeze PR #25 scope | ✅ COMPLETE | `docs/architecture/real/PR25_ANALYSIS.md` |

**Gate de salida**: Documentación completa, tracking.yaml actualizado, sin cambios de código.

---

## PHASE 1 — ARCHITECTURE & CONTRACTS
**Objetivo**: Definir contratos formales para los gaps identificados.

| Item | Estado | Dependencia | Entregable |
|------|--------|-------------|------------|
| ADR-0028 Orderbook Builder Design | MISSING | Phase 0 | `docs/architecture/decisions/ADR-0028-orderbook-builder.md` |
| ADR-XXXX Market Universe SSOT | MISSING | Phase 0 | `docs/architecture/decisions/ADR-XXXX-market-universe-ssot.md` |
| ORDERBOOK_SCHEMA en schemas.py | MISSING | ADR-0028 | `packages/market_data/infrastructure/storage/iceberg/schemas.py` |
| MarketUniverseConfig en schema.py | MISSING | ADR-XXXX | `ocm/config/schema.py` |
| Kafka topic contracts (orderbook) | PARTIAL | — | Verificar `shared/kafka/schemas/orderbook.py` |
| Bronze table contracts | MISSING | ADR-0028 | Definir particionado, retención |
| Promotion Rule gate en CI | MISSING | ADR-0017 | `check_production_gates.py` incluye promotion check |

**Gate de salida**: ADRs aprobados, contratos definidos, CI valida contracts.

---

## PHASE 2 — PROTOCOL INTEGRATION
**Objetivo**: Completar Protocol Discovery Profile para Bybit (ADR-0017).

| Item | Estado | Dependencia | Entregable |
|------|--------|-------------|------------|
| Bybit REST Discovery Profile | PARTIAL | Phase 1 | `packages/market_data/adapters/outbound/exchange/ccxt_adapter.py` docs |
| Bybit WS Orderbook Discovery | PARTIAL | Phase 1 | `cryptofeed_orderbook_stream.py` gap recovery docs |
| Bybit WS Trades Discovery | COMPLETE | — | `BybitCryptofeedRunner` verificado |
| Bybit Funding/OI Discovery | MISSING | Phase 1 | Runners para funding/oi producers |
| Bybit Liquidations Discovery | MISSING | Phase 1 | Runner para liquidations producer |
| Contract Provenance para Bybit | MISSING | Phase 1 | `test_schema_provenance.py` extendido |
| Fixtures mensajes reales Bybit | MISSING | Phase 1 | `tests/fixtures/bybit/` |

**Gate de salida**: Bybit Discovery Profile completo (REST + WS todos los canales), Promotion Rule gate pasa.

---

## PHASE 3 — MARKET / INSTRUMENT DISCOVERY
**Objetivo**: Implementar discovery dinámico y metadata registry.

| Item | Estado | Dependencia | Entregable |
|------|--------|-------------|------------|
| Instrument Metadata Registry | MISSING | Phase 2 | `packages/market_data/domain/value_objects/instrument.py` (NUEVO) |
| CCXT `load_markets()` integration | MISSING | Phase 2 | `CCXTAdapter.discover_instruments()` |
| Metadata capture (tick, lot, precision) | MISSING | Registry | Persistir en Registry con provenance |
| Auto-discovery path (`auto_discover_symbols=True`) | MISSING | Registry | Config + implementación |
| Symbol normalizer (CCXT ↔ Cryptofeed) | MISSING | Phase 2 | `shared/utils/symbol_normalizer.py` (NUEVO) |
| Validation on startup | MISSING | Registry | Fail-fast si símbolo universe no existe en exchange |

**Gate de salida**: Discovery dinámico funcional, metadata capturada con provenance, normalizador operativo.

---

## PHASE 4 — INSTRUMENT METADATA / MARKET UNIVERSE
**Objetivo**: SSOT único para Market Universe, consumido por REST y WS.

| Item | Estado | Dependencia | Entregable |
|------|--------|-------------|------------|
| `config/market_data/universe.yaml` (NUEVO) | MISSING | Phase 3 | SSOT único, formato canónico |
| `MarketUniverseProvider` class | MISSING | universe.yaml | `packages/market_data/application/market_universe.py` (NUEVO) |
| Migrar `ConcretePipelineFactory` → Universe | MISSING | Provider | `_build_ohlcv` usa `universe.get_symbols()` |
| Migrar `FeedOrchestrator` → Universe | MISSING | Provider | `_build_adapters` usa `universe.get_symbols()` |
| Migrar `streaming_hydra.py` → Universe | MISSING | Provider | `main()` usa `universe.get_symbols()` |
| Deprecar `config/env/development.yaml` symbols | MISSING | Migración | Comentario DEPRECATED |
| Deprecar `config/market_data/feeds.yaml` symbols | MISSING | Migración | Comentario DEPRECATED |
| Tests: universe consistency | MISSING | Migración | REST symbols == WS symbols (normalizados) |

**Gate de salida**: Un solo Market Universe config, consumido por REST y WS, formatos normalizados, tests pasan.

---

## PHASE 5 — MARKET DATA INGESTION (OHLCV/TRADES)
**Objetivo**: Verificar y solidificar pipeline OHLCV/Trades existente.

| Item | Estado | Dependencia | Entregable |
|------|--------|-------------|------------|
| OHLCV pipeline verification | COMPLETE | — | Verificado: REST → Kafka → Bronze |
| Trades pipeline verification | COMPLETE | — | Verificado: REST + WS → Kafka |
| `KafkaOHLCVPublisher` fail-fast prod | COMPLETE | — | F-031 implementado |
| `NullOHLCVPublisher` degradación documentada | COMPLETE | — | Docs en pipeline_factory.py |
| Quality pipeline integration | COMPLETE | — | `QualityPipelineConsumer` activo |
| Backfill/repair strategies | COMPLETE | — | Verificado |

**Gate de salida**: Pipeline OHLCV/Trades estable, sin cambios necesarios.

---

## PHASE 6 — ORDERBOOK BUILDER
**Objetivo**: Orderbook → Bronze funcional, reproducible, con gap recovery.

| Item | Estado | Dependencia | Entregable |
|------|--------|-------------|------------|
| `ORDERBOOK_SNAPSHOT_SCHEMA` + `ORDERBOOK_DELTA_SCHEMA` | MISSING | Phase 1 | `schemas.py` IDs 301-320 |
| `bronze.orderbook_snapshot` table | MISSING | Schema | Iceberg table + particionado |
| `bronze.orderbook_delta` table | MISSING | Schema | Iceberg table + particionado |
| `BronceStorage.append_snapshot/delta` | MISSING | Tables | Métodos extendidos |
| `KafkaConsumerAdapter.for_orderbook()` | MISSING | Phase 1 | Factory method |
| `OrderbookBronzeWriter` consumer | MISSING | Consumer | `infrastructure/kafka/orderbook_bronze_writer.py` (NUEVO) |
| CompositionRoot wiring | MISSING | Consumer | `main.py` + `streaming_hydra.py` wiring |
| **G9 Orderbook PASS** | MISSING | Todo lo anterior | `check_production_gates.py` G9 PASS |

### Phase 6B — Orderbook Builder (Calidad Histórica)
| Item | Estado | Dependencia | Entregable |
|------|--------|-------------|------------|
| `BookState` class (SortedDict bids/asks) | MISSING | 6A | `packages/market_data/application/orderbook/builder.py` |
| `apply_snapshot()` + `apply_delta()` | MISSING | BookState | Lógica core |
| Sequence validation + gap detection | MISSING | BookState | `detect_gap()` |
| Checksum validation | MISSING | BookState | Validar `book.checksum` |
| Snapshot recovery (REST `fetch_order_book`) | MISSING | Gap detection | Recovery task |
| Historical replay from Bronze | MISSING | Tables | `rebuild_book_at(timestamp)` |

**Gate de salida 6A**: Orderbook → Bronze funcional, G9 PASS.
**Gate de salida 6B**: Orderbook reproducible desde Bronze, gap recovery operativo.

---

## PHASE 7 — KAFKA / BRONZE
**Objetivo**: Solidificar capa Kafka-Bronze, añadir Schema Registry.

| Item | Estado | Dependencia | Entregable |
|------|--------|-------------|------------|
| Schema Registry (Avro) | MISSING | Phase 6 | B-18: Avro + registry + backward compat |
| Kafka consumer groups monitoring | PARTIAL | — | `health_check.sh` incluye consumer lag |
| DLQ alerting | PARTIAL | — | Métricas + alertas DLQ volume |
| Bronze retention policy | PARTIAL | — | `bronze_retention.py` configurado |
| Bronze compaction strategy | MISSING | — | Iceberg compaction config |

**Gate de salida**: Schema Registry operativo, monitoring completo.

---

## PHASE 8 — SILVER / GOLD
**Objetivo**: Capas Silver/Gold operativas para Orderbook.

| Item | Estado | Dependencia | Entregable |
|------|--------|-------------|------------|
| Silver Orderbook schema | MISSING | Phase 7 | `SILVER_ORDERBOOK_SCHEMA` |
| Gold Orderbook features | MISSING | Phase 7 | OFI, CVD, microprice, spread features |
| Silver/Gold pipeline wiring | MISSING | Phase 7 | Consumers + transformers |
| FeatureReaderPort para orderbook | MISSING | Phase 7 | Query interface |

**Gate de salida**: Orderbook data disponible en Silver/Gold para estrategias.

---

## PHASE 9 — ORDER FLOW
**Objetivo**: Order Flow Imbalance, CVD, microprice desde Orderbook.

| Item | Estado | Dependencia | Entregable |
|------|--------|-------------|------------|
| OFI (Order Flow Imbalance) | MISSING | Phase 8 | Feature Gold |
| CVD (Cumulative Volume Delta) | MISSING | Phase 8 | Feature Gold |
| Microprice | MISSING | Phase 8 | Feature Gold |
| Spread features | MISSING | Phase 8 | Feature Gold |
| Strategy integration | MISSING | Phase 8 | StrategyConsumer consume orderbook features |

---

## PHASE 10 — STRATEGY
**Objetivo**: Estrategias consumen features Gold (OHLCV + Orderbook).

| Item | Estado | Dependencia | Entregable |
|------|--------|-------------|------------|
| Strategy framework | PARTIAL | Phase 9 | `packages/trading/strategies/` |
| Signal generation | MISSING | Phase 9 | Signal → Risk → Execution |
| Backtesting framework | MISSING | Phase 9 | Historical replay from Gold |

---

## PHASE 11 — RISK
**Objetivo**: Risk management con datos reales (balance, positions, drawdown).

| Item | Estado | Dependencia | Entregable |
|------|--------|-------------|------------|
| Balance reconciliation (B-MD-009) | PENDIENTE | Phase 10 | ADR-0030 implementation |
| Position state single owner (B-15) | PENDIENTE | Phase 10 | ADR-0021 implementation |
| Real-time risk gates | PARTIAL | Phase 10 | G10 implementation |

---

## PHASE 12 — EXECUTION
**Objetivo**: Live trading con cancelación real, fill reconciliation.

| Item | Estado | Dependencia | Entregable |
|------|--------|-------------|------------|
| Order cancellation (B-MD-008) | ✅ HECHO | — | ADR-0029 implemented |
| Fill reconciliation | PARTIAL | Phase 11 | G12 implementation |
| OMS manage_open_orders | ✅ HECHO | — | Policy A implemented |

---

## PHASE 13 — PRODUCTION VALIDATION
**Objetivo**: Validación end-to-end en producción.

| Item | Estado | Dependencia | Entregable |
|------|--------|-------------|------------|
| `check_production_gates.py` | MISSING | Phase 1 | Script G1-G11 binario |
| CI integration | MISSING | Script | Job `production-gate` en `ocm-ci.yml` |
| Streaming health endpoint | MISSING | Phase 6 | `/health` en streaming_hydra |
| `health_check.sh` streaming coverage | MISSING | Endpoint | 4th domain: STREAMING |
| Systemd restart test (B-59) | MISSING | — | Documented restart test |
| Canary production run | MISSING | All PASS | 24h run with monitoring |

---

## PHASE 14 — B-49 FINAL GATE
**Objetivo**: B-49 PASS declarado con evidencia.

| Item | Estado | Dependencia | Entregable |
|------|--------|-------------|------------|
| G1-G9 all PASS | MISSING | Phase 13 | `check_production_gates.py` → exit 0 |
| Evidence package | MISSING | Phase 13 | Logs, screenshots, metrics |
| B-49 tracking.yaml update | MISSING | All PASS | `estado: HECHO`, `fecha_cierre` |
| Release candidate tag | MISSING | All PASS | `git tag b49-pass-<date>` |

---

## RESUMEN DE FASES

| Fase | Nombre | Estado | Bloquea | Duración Estimada |
|------|--------|--------|---------|-------------------|
| 0 | Baseline/Freeze | 🔄 IN_PROGRESS | — | 1 día |
| 1 | Architecture & Contracts | MISSING | 2-14 | 3-5 días |
| 2 | Protocol Integration | MISSING | 3-6 | 5-7 días |
| 3 | Market/Instrument Discovery | MISSING | 4-6 | 5-7 días |
| 4 | Market Universe SSOT | MISSING | 5-6 | 3-5 días |
| 5 | Market Data Ingestion | ✅ COMPLETE | — | — |
| 6 | Orderbook Builder | MISSING | 7-9 | **10-14 días** (CRÍTICO) |
| 7 | Kafka/Bronze | MISSING | 8-9 | 5-7 días |
| 8 | Silver/Gold | MISSING | 9-10 | 5-7 días |
| 9 | Order Flow | MISSING | 10 | 5-7 días |
| 10 | Strategy | MISSING | 11 | 5-7 días |
| 11 | Risk | PENDIENTE | 12 | 5-7 días |
| 12 | Execution | PARTIAL | 13 | 3-5 días |
| 13 | Production Validation | MISSING | 14 | 3-5 días |
| 14 | B-49 Final Gate | MISSING | — | 1 día |

**Ruta crítica**: Phase 0 → 1 → 2 → 3 → 4 → 6A → 13 → 14
**Tiempo mínimo a B-49 PASS**: ~4-6 semanas (Phase 6A es el cuello de botella)