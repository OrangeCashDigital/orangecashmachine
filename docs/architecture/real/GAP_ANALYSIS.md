# GAP ANALYSIS — REAL vs TARGET

> **Fecha**: 2026-09-04
> **Basado en**: READ-ONLY audit de implementación real vs arquitectura objetivo
> **Clasificación**: IMPLEMENTED | PARTIALLY IMPLEMENTED | CONFIGURATION ISSUE | CODE ISSUE | MISSING | DOCUMENTED ONLY | UNKNOWN

---

## RESUMEN EJECUTIVO

| Área | Estado Global | Gap Crítico |
|------|---------------|-------------|
| OHLCV Ingestion | ✅ IMPLEMENTED | — |
| Trades Ingestion | ✅ IMPLEMENTED | — |
| Orderbook Ingestion | ⚠️ PARTIALLY IMPLEMENTED | Producer sí, Consumer NO |
| Orderbook → Bronze | ❌ MISSING | Schema, Consumer, Tabla Iceberg |
| Market Universe | ❌ MISSING | 3 configs, 2 formatos, sin SSOT |
| Instrument Discovery | ❌ MISSING | Solo estático, auto_discover no usado |
| Protocol Discovery | 📋 DOCUMENTED ONLY | ADR-0017 aceptado, no implementado |
| Unified Adapter | 📋 DOCUMENTED ONLY | Propuesta, no evaluada formalmente |
| G4-G9 Gates | ⚠️ PARTIAL | G9 Orderbook falla |

---

## GAPS DETALLADOS POR CATEGORÍA

### A. MARKET UNIVERSE & INSTRUMENT DISCOVERY

| # | Gap | Clasificación | Evidencia REAL | Target |
|---|-----|---------------|----------------|--------|
| A1 | **3 fuentes de verdad** para símbolos Bybit | **ARCHITECTURE ISSUE** | `config/exchanges/bybit.yaml` (enabled only), `config/env/development.yaml` (CCXT format), `config/market_data/feeds.yaml` (cryptofeed format) | 1 SSOT: `config/market_data/universe.yaml` |
| A2 | **2 formatos incompatibles** | **CODE ISSUE** | CCXT: `BTC/USDT` vs Cryptofeed: `BTC-USDT-PERP` | Normalizador canónico + single format |
| A3 | **REST y WS usan universos diferentes** | **CODE ISSUE** | market_data.main usa `env/development.yaml`; streaming_hydra usa `feeds.yaml` | Unified config consumida por ambos |
| A4 | `auto_discover_symbols: false` nunca usado | **DOCUMENTED ONLY** | Campo en `ExchangeConfig` (schema.py:201), no leído en código | Implementar path auto-discovery |
| A5 | **Sin Instrument Metadata Registry** | **MISSING** | No existe captura de tick_size, lot_size, min_qty, etc. | Registry con provenance (ADR-0017) |
| A6 | **Sin normalización CCXT ↔ Cryptofeed** | **MISSING** | No hay capa de conversión `BTC/USDT` ↔ `BTC-USDT-PERP` | `shared/utils/symbol_normalizer.py` |
| A7 | **Sin dynamic discovery** | **MISSING** | `CCXTAdapter.load_markets()` llamado pero no usado para filtrar | `auto_discover_symbols=True` path |

### B. ORDERBOOK FLOW

| # | Gap | Clasificación | Evidencia REAL | Target |
|---|-----|---------------|----------------|--------|
| B1 | **OrderbookKafkaProducer produce a `orderbook.raw`** | ✅ IMPLEMENTED | `adapters/inbound/websocket/orderbook_producer.py` | — |
| B2 | **CryptofeedOrderBookStream ACL implementada** | ✅ IMPLEMENTED | `adapters/inbound/websocket/cryptofeed_orderbook_stream.py` | — |
| B3 | **NO consumer para `orderbook.raw`** | ❌ MISSING | `KafkaConsumerAdapter` no tiene `for_orderbook()` | `OrderbookBronzeWriter` consumer |
| B4 | **NO schema Bronze para orderbook** | ❌ MISSING | `schemas.py` solo tiene OHLCV, Trades, Derivatives | `ORDERBOOK_SNAPSHOT_SCHEMA` + `ORDERBOOK_DELTA_SCHEMA` |
| B5 | **NO tabla Bronze orderbook** | ❌ MISSING | `BronzeStorage` solo maneja `bronze.ohlcv` | `bronze.orderbook_snapshot` + `bronze.orderbook_delta` |
| B6 | **NO Orderbook Builder (estado L2)** | ❌ MISSING | Stream solo traduce snapshot/delta, no mantiene estado | BookBuilder con SortedDict + gap detection |
| B7 | **NO gap detection / sequence validation** | ❌ MISSING | `_translate_and_dispatch` no valida `sequence_number` | Validar `book.sequence_number` |
| B8 | **NO checksum validation** | ❌ MISSING | `checksum` extraído pero no validado | Validar `book.checksum` |
| B9 | **NO recovery via snapshot** | ❌ MISSING | Sin lógica de re-snapshot ante gap | REST `fetch_order_book` para recovery |

### C. KAFKA & BRONZE

| # | Gap | Clasificación | Evidencia REAL | Target |
|---|-----|---------------|----------------|--------|
| C1 | `ohlcv.raw` → Bronze funciona | ✅ IMPLEMENTED | `KafkaBronzeWriter` + `BronzeStorage` | — |
| C2 | `orderbook.raw` → **no consumer** | ❌ MISSING | Ningún `for_orderbook()` en consumer.py | Consumer group `ORDERBOOK_BRONZE_WRITER` |
| C3 | **Kafka topics SSOT** | ✅ IMPLEMENTED | `shared/kafka/topics.py` — 25 constantes | — |
| C4 | **Bootstrap servers config** | ⚠️ CONFIG ISSUE | `host.env`: `localhost:9093`, Docker: `kafka:9092` | SSOT único + env-specific override |

### D. STREAMING SERVICE

| # | Gap | Clasificación | Evidencia REAL | Target |
|---|-----|---------------|----------------|--------|
| D1 | `ocm-streaming` service ACTIVE | ✅ IMPLEMENTED (desde Sep 03) | systemd status active | — |
| D2 | **Histórico FAILED (Aug 28)** | 📋 CONFIG ISSUE (resuelto) | "At least one exchange must be enabled" en `validate_exchanges()` | Verificar `_MODULE_PACKAGES` incluye exchanges |
| D3 | **Solo orderbook canary** | ⚠️ PARTIAL | `streaming_hydra.py` solo usa `WSProducerBundle.orderbook` | Funding/OI/Liq runners en phases |
| D4 | **Systemd restart no verificado** | 📋 UNKNOWN | tracking.yaml B-59: `systemd_reinicia_correctamente: NO_VERIFICADO` | Test de reinicio documentado |

### E. PROTOCOL DISCOVERY

| # | Gap | Clasificación | Evidencia REAL | Target |
|---|-----|---------------|----------------|--------|
| E1 | **ADR-0017 aceptado** | 📋 DOCUMENTED ONLY | Framework definido, no implementado | Implementar Discovery Profile Bybit |
| E2 | **Contract Provenance** | 📋 DOCUMENTED ONLY | `test_schema_provenance.py` semilla (F2.3) | Promotion Rule gate en CI |
| E3 | **REST Discovery formal** | 📋 DOCUMENTED ONLY | `CCXTAdapter` + `HistoricalFetcherAsync` existen | Profile Bybit REST completo |
| E4 | **WS Discovery formal** | ⚠️ PARTIAL | `CryptofeedOrderBookStream` + `BybitCryptofeedRunner` | Profile Bybit WS completo con gap recovery |

---

## MAPEO GAPS → B-49 GATES (G4-G9)

| Gate | Requisito | Estado REAL | Gaps Relacionados | Acción Requerida |
|------|-----------|-------------|-------------------|------------------|
| **G4** | Systemd units valid | ✅ PASS | D2 (histórico), D4 (restart test) | Verificar restart |
| **G5** | Kafka connectivity | ⚠️ PARTIAL | B3, B4, B5 (orderbook no consumido) | Consumer orderbook + Bronze |
| **G6** | Infra health | ✅ PASS | C4 (bootstrap dual) | SSOT bootstrap único |
| **G7** | Health checks | ✅ PASS | — | — |
| **G8** | IS_STUB=False | ✅ PASS | — | — |
| **G9** | Bronze freshness | ⚠️ PARTIAL | **B1-B9** (orderbook no en Bronze) | **Orderbook → Bronze completo** |

---

## MATRIZ DE PRIORIZACIÓN

| Prioridad | Gap | Clasificación | Esfuerzo | Impacto B-49 | Dependencias |
|-----------|-----|---------------|----------|--------------|--------------|
| **P0** | B3, B4, B5 | MISSING | Alto | **BLOQUEA G9** | Ninguna |
| **P0** | A1, A2, A3 | ARCHITECTURE/CODE | Medio | **BLOQUEA G5, G9** | Ninguna |
| **P1** | B6, B7, B8, B9 | MISSING | Alto | Calidad orderbook | B3-B5 |
| **P1** | A5, A6, A7 | MISSING | Medio | Consistencia universo | A1-A3 |
| **P2** | C4 | CONFIG ISSUE | Bajo | Operacional | Ninguna |
| **P2** | D2, D4 | CONFIG/UNKNOWN | Bajo | Operacional | Ninguna |
| **P3** | D3, E1-E4 | DOCUMENTED/PARTIAL | Alto | Futuro | P0-P1 completados |

---

## CONCLUSIÓN

**B-49 = PARTIALLY IMPLEMENTED**

**Gap bloqueante principal**: **Orderbook → Bronze (B3-B5)** — sin esto G9 no puede PASS completamente.

**Gap arquitectónico transversal**: **Market Universe fragmentado (A1-A3)** — causa riesgo de data mismatch entre REST y WS, afecta G5 y G9.

**Próxima acción obligatoria**: Implementar Orderbook Bronze consumer + schema + tabla (P0) + Unificar Market Universe config (P0).