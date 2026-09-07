# EXECUTIVE SUMMARY — OCM B-49 AUDIT & CORRECTION PLAN

> **Fecha**: 2026-09-04
> **Audit**: READ-ONLY completo contra código, config, systemd, tracking.yaml
> **Conclusión**: **B-49 = PARTIALLY IMPLEMENTED** — NO declarar PASS

---

## ESTADO ACTUAL DE OCM

### Qué FUNCIONA (✅ COMPLETE)
| Capacidad | Evidencia |
|-----------|-----------|
| **OHLCV REST ingestion** | `market_data.main` → CCXTAdapter → Kafka → Bronze (G9 PASS) |
| **Trades ingestion (REST + WS)** | Dual path verificado, Quality pipeline activo |
| **Kafka infra** | Topics SSOT, consumers OHLCV/Trades/Features/Strategy funcionando |
| **Bronze OHLCV** | 12-field schema, partitionado, freshness <15min, run_id presente |
| **Systemd services** | `ocm-market-data` + `ocm-streaming` ACTIVE |
| **Health checks** | `/health`, `/ready`, `health_check.sh` → 3/3 domains HEALTHY |
| **IS_STUB = FALSE** | Live executor + risk guards operativos (G8 PASS) |
| **Tracing G11** | OpenTelemetry + request-id propagation (14 tests PASS) |
| **Order cancellation** | ADR-0029 implementado, 18 lifecycle tests PASS |

### Qué ESTÁ ROTO / FALTA (❌ MISSING / ⚠️ PARTIAL)

| Gap Crítico | Impacto | Gates Afectados |
|-------------|---------|-----------------|
| **Orderbook → Bronze** | Sin consumer, sin schema, sin tabla Iceberg | **G5, G9** (BLOQUEA) |
| **Market Universe fragmentado** | 3 configs, 2 formatos (`BTC/USDT` vs `BTC-USDT-PERP`) | **G5, G9** |
| **Instrument Discovery dinámico** | `auto_discover_symbols` campo muerto, sin metadata registry | **G5, G9** |
| **Orderbook Builder** | Sin estado L2, sin gap detection, sin recovery | **G9 (calidad)** |
| **Streaming health endpoint** | Solo pushgateway, sin `/health` | **G7** |
| **Schema Registry (Avro)** | Solo dataclasses, sin registry | **G6** (B-18) |

---

## B-49 STATUS — G4-G9

| Gate | Estado | Blocker |
|------|--------|---------|
| **G4** Systemd | ✅ PASS | Restart test pendiente (B-59) |
| **G5** Kafka | ⚠️ PARTIAL | `orderbook.raw` sin consumer |
| **G6** Infra | ✅ PASS | Schema Registry deuda (B-18) |
| **G7** Health | ✅ PASS | Streaming health faltante |
| **G8** IS_STUB | ✅ PASS | — |
| **G9** Bronze | ⚠️ PARTIAL | **Orderbook Bronze MISSING** |

**B-49 = PARTIALLY IMPLEMENTED** — **NO DECLARAR PASS**

---

## ARQUITECTURA REAL vs OBJETIVO

### REAL (Hoy)
```
Two separate pipelines:
1. REST (OHLCV/Trades) → Kafka → Bronze ✅
2. WS (Orderbook) → Kafka → ❌ (no consumer, no Bronze)
```
- Market Universe: 3 fuentes, 2 formatos
- Protocol Discovery: Documentado (ADR-0017), no implementado
- Unified Adapter: No existe

### OBJETIVO (Target)
```
Unified Market Data Platform:
Discovery → Metadata Registry → Universe SSOT → REST/WS → Kafka → Bronze (ALL) → Silver → Gold
```
- Single Market Universe config
- Orderbook reproducible desde Bronze
- Protocol Discovery Framework operativo

---

## PLAN DE CORRECCIÓN — ORDEN DE EJECUCIÓN

### FASE 0: Baseline (ESTA SEMANA) ✅ EN PROGRESO
- [x] Documentación completa READ-ONLY
- [ ] tracking.yaml B-49 = PARTIAL actualizado

### FASE 1: Architecture & Contracts (Semana 1)
- ADR-0028 Orderbook Builder
- ADR-XXXX Market Universe SSOT
- ORDERBOOK schemas en schemas.py

### FASE 2-4: Market Universe + Discovery (Semanas 2-4)
- `config/market_data/universe.yaml` SSOT
- `MarketUniverseProvider` + normalizer CCXT↔Cryptofeed
- Migrar 3 consumidores (pipeline_factory, feed_orchestrator, streaming_hydra)
- Deprecar configs duplicadas

### FASE 6A: Orderbook → Bronze (SEMANAS 5-7) 🔴 **CRÍTICO PATH**
- ORDERBOOK schemas (snapshot + delta)
- Bronze tables (snapshot + delta)
- KafkaConsumerAdapter.for_orderbook()
- OrderbookBronzeWriter consumer
- Wiring en main.py + streaming_hydra.py
- **G9 PASS**

### FASE 13: Production Validation (Semana 8)
- `check_production_gates.py` G1-G11
- CI job production-gate
- Streaming health endpoint
- Systemd restart test

### FASE 14: B-49 Final Gate
- `check_production_gates.py` → exit 0
- Evidence package
- tracking.yaml B-49 = HECHO

---

## PRs PROPUESTOS (MÍNIMO PARA B-49 PASS)

| PR | Objetivo | Bloquea B-49 | Esfuerzo |
|----|----------|--------------|----------|
| **A** | Documentation Baseline | No | 1 día |
| **B** | Market Universe SSOT | **SÍ (G5, G9)** | 3-5 días |
| **C** | Streaming Fixes | Parcial (G4, G7) | 1-2 días |
| **D** | **Orderbook → Bronze** | **SÍ (G9)** | **10-14 días** |
| **F** | **Production Gate Binary** | **SÍ (B-49 gate)** | 3-5 días |

**Mínimo para B-49 PASS**: A + B + C + D + F

---

## RIESGOS TÉCNICOS Y DE PRODUCCIÓN

| Riesgo | Severidad | Mitigación |
|--------|-----------|------------|
| Orderbook Bronze no implementado | **CRÍTICO** | PR D prioridad absoluta |
| Market Universe mismatch | **ALTO** | PR B antes de PR D |
| Schema Registry ausente | MEDIO | B-18 paralelo |
| Unified Adapter scope creep | MEDIO | Clasificación LATER, post-B-49 |
| Systemd restart no verificado | BAJO | Test documentado en PR F |

---

## NEXT ACTION — ACCIÓN ÚNICA RECOMENDADA

> **INICIAR PR D — Orderbook → Bronze Implementation**
> 
> **Por qué**: Es el **único blocker crítico** para G9 y B-49 PASS. Todo lo demás (Market Universe, Streaming fixes) son dependencias o calidad, pero **sin Orderbook Bronze no hay B-49 PASS posible**.
> 
> **Primer commit sugerido**:
> 1. Añadir `ORDERBOOK_SNAPSHOT_SCHEMA` + `ORDERBOOK_DELTA_SCHEMA` en `schemas.py` (IDs 301-320)
> 2. Crear `KafkaConsumerAdapter.for_orderbook()` factory method
> 3. Crear `OrderbookBronzeWriter` consumer básico (sin gap recovery aún)
> 4. Extender `BronzeStorage.append_snapshot/delta`
> 5. Wiring en `main.py` `_bronze_writer_loop` + `streaming_hydra.py`
> 6. Test de integración: WS → Kafka → Bronze verificado
> 7. `check_production_gates.py --gate G9` PASS
> 
> **Paralelo**: PR B (Market Universe) para resolver format mismatch antes de que Orderbook Writer necesite consumir symbols normalizados.

---

## CRITERIO DE ÉXITO — VALIDACIÓN FINAL

Al completar, un nuevo ingeniero puede responder SIN PREGUNTAR:

1. ✅ ¿Cómo habla OCM con Bybit? → REST (CCXTAdapter) + WS (Cryptofeed)
2. ✅ ¿Cómo descubre los instrumentos? → `load_markets()` + universe.yaml SSOT
3. ✅ ¿Dónde está el Market Universe? → `config/market_data/universe.yaml`
4. ✅ ¿Quién consume ese universo? → PipelineFactory, FeedOrchestrator, streaming_hydra
5. ✅ ¿Qué hace REST? → OHLCV/Trades polling → Kafka → Bronze
6. ✅ ¿Qué hace WebSocket? → Orderbook/Trades streaming → Kafka → Bronze
7. ✅ ¿Quién construye el Orderbook? → `CryptofeedOrderBookStream` → `OrderBookKafkaProducer`
8. ✅ ¿Dónde entra Kafka? → SSOT operacional, topics en `shared/kafka/topics.py`
9. ✅ ¿Dónde se persiste el Orderbook? → `bronze.orderbook_snapshot` + `bronze.orderbook_delta`
10. ✅ ¿Cómo se recupera un gap? → `BookState.detect_gap()` → REST snapshot recovery
11. ✅ ¿Cómo llega a Silver? → Quality pipeline → Silver storage
12. ✅ ¿Cómo se calcula Order Flow? → Gold features (OFI, CVD, microprice)
11. ✅ ¿Cómo llega a Gold? → Feature transformer → Gold storage
12. ✅ ¿Cómo llega una señal a Strategy? → FeatureReaderPort → StrategyConsumer
13. ✅ ¿Cómo pasa por Risk? → RiskManager.validate() → signals.approved
14. ✅ ¿Cómo llega a Execution? → ExecutionConsumer → OMS → OrderTransport
15. ✅ ¿Qué valida B-49? → G1-G11 via `check_production_gates.py`
16. ✅ ¿Qué está implementado? → OHLCV/Trades full, Orderbook producer
17. ✅ ¿Qué está parcialmente implementado? → Orderbook Bronze, Market Universe
18. ✅ ¿Qué falta? → Orderbook consumer/schema/tabla, Universe SSOT, gap recovery
19. ✅ ¿Cuál es el siguiente cambio correcto? → **PR D: OrderbookBronzeWriter**
20. ✅ ¿Qué NO debe tocarse todavía? → Unified Adapter, Schema Registry, Strategy framework

---

**DOCUMENTACIÓN GENERADA EN ESTA AUDITORÍA**:
- `docs/architecture/real/ARCHITECTURE_REAL.md`
- `docs/architecture/real/ARCHITECTURE_TARGET.md`
- `docs/architecture/real/GAP_ANALYSIS.md`
- `docs/architecture/real/MARKET_UNIVERSE_ANALYSIS.md`
- `docs/architecture/real/ORDERBOOK_FLOW_ANALYSIS.md`
- `docs/architecture/real/B49_STATUS.md`
- `docs/architecture/real/PR25_ANALYSIS.md`
- `docs/architecture/real/UNIFIED_ADAPTER_ANALYSIS.md`
- `docs/architecture/real/DOCUMENTATION_CHANGES.md`
- `docs/architecture/real/KB_TRAINING_CHANGES.md`
- `docs/architecture/real/MASTER_CORRECTION_PLAN.md`
- `docs/architecture/real/PR_DECOMPOSITION.md`
- `docs/architecture/real/TRACEABILITY_MATRIX.md`
- `docs/architecture/real/EXECUTIVE_SUMMARY.md` (este archivo)

---

**PRÓXIMA ACCIÓN INMEDIATA**: Iniciar **PR D — Orderbook → Bronze Implementation** con commit de schemas Orderbook en `schemas.py`.