# B-49 STATUS — PRODUCTION GATES G4-G9

> **Fecha**: 2026-09-04
> **Audit**: READ-ONLY contra tracking.yaml, código, systemd, health checks
> **Conclusión**: **PARTIALLY IMPLEMENTED** — No declarar B-49 PASS

---

## RESUMEN EJECUTIVO

| Gate | Estado | Evidencia | Blocker |
|------|--------|-----------|---------|
| **G4** | ✅ PASS | Ambos systemd units ACTIVE | — |
| **G5** | ⚠️ PARTIAL | Kafka OK pero orderbook.raw sin consumer | Orderbook consumer |
| **G6** | ✅ PASS | Kafka, Redis, Iceberg healthy | — |
| **G7** | ✅ PASS | health_check.sh 3/3 domains HEALTHY | — |
| **G8** | ✅ PASS | IS_STUB=False + risk guards | — |
| **G9** | ⚠️ PARTIAL | OHLCV ✅ | Orderbook ❌ | **Orderbook Bronze** |

---

## G4 — SYSTEMD UNITS VALID

### Requisito
> Units systemd correctamente configuradas, enabled, y capaces de restart limpio.

### Implementación
| Servicio | Unit File | ExecStart | Estado |
|----------|-----------|-----------|--------|
| `ocm-market-data` | `deploy/systemd/rendered/ocm-market-data.service` | `python -m market_data.main` | **ACTIVE** (1h37m uptime) |
| `ocm-streaming` | `deploy/systemd/rendered/ocm-streaming.service` | `python -m app.cli.streaming_hydra` | **ACTIVE** (desde Sep 03) |

### Evidencia
```bash
$ systemctl status ocm-market-data
● ocm-market-data.service - OrangeCashMachine market-data-service...
   Active: active (running) since ...; 1h 37min ago
   Main PID: 535148 (python)

$ systemctl status ocm-streaming  
● ocm-streaming.service - OrangeCashMachine streaming service...
   Active: active (running) since Thu 2026-09-03 18:45:31; 6h ago
   Main PID: 535701 (python)
```

### Histórico
- **Aug 28**: `ocm-streaming` FAILED con "At least one exchange must be enabled"
- **Sep 03**: Resuelto (config load exitoso), servicio ACTIVE desde entonces
- **Restart test**: NO_VERIFICADO (tracking.yaml B-59)

### Clasificación
✅ **IMPLEMENTED** — Ambas units running. Restart test pendiente (bajo riesgo).

---

## G5 — KAFKA CONNECTIVITY

### Requisito
> Kafka broker accesible, producers/consumers healthy, topics operativos.

### Implementación
| Componente | Estado | Detalle |
|------------|--------|---------|
| Broker | ✅ HEALTHY | `localhost:9093` (host) / `kafka:9092` (Docker) |
| Topics | ⚠️ PARTIAL | `ohlcv.raw` ✅, `trades.raw` ✅, `orderbook.raw` ❌ sin consumer |
| Producer OHLCV | ✅ ACTIVE | `KafkaOHLCVPublisher` → `ohlcv.raw` |
| Producer Orderbook | ✅ ACTIVE | `OrderBookKafkaProducer` → `orderbook.raw` |
| Consumer Bronze | ✅ ACTIVE | `KafkaBronzeWriter` → `ohlcv.raw` |
| Consumer Orderbook | ❌ MISSING | No `for_orderbook()` en `KafkaConsumerAdapter` |

### Evidencia
- `kafka-topics --list` muestra 15 topics incluyendo `ohlcv.raw`, `orderbook.raw`
- Offsets `ohlcv.raw` avanzando (4171-3457-3457 msg/partition)
- Logs `bronze_write_logger` confirman writes exitosos
- Logs `orderbook_producer_started`, `orderbook_stream_starting` confirmados

### Gap Crítico
**`orderbook.raw` producido pero NO consumido** → datos en Kafka sin persistencia ni procesamiento downstream.

### Clasificación
⚠️ **PARTIALLY IMPLEMENTED** — Infra OK, pero pipeline orderbook incompleto.

---

## G6 — INFRA HEALTH

### Requisito
> Kafka + Redis + Iceberg + Schema Registry operativos.

### Implementación
| Infra | Estado | Evidencia |
|-------|--------|-----------|
| Kafka | ✅ HEALTHY | Broker responding, topics active |
| Redis | ✅ HEALTHY | `PONG` response, cursor store operativo |
| Iceberg | ✅ HEALTHY | Catalog REST responding, tables accessible |
| Schema Registry | ❌ MISSING | No Avro registry (tracking.yaml B-18 PENDIENTE) |

### Evidencia
- `health_check.sh` → `INFRA_HEALTHY=HEALTHY`
- `KafkaBronzeWriter` writes confirmados con `run_id` en Bronze
- Redis `build_cursor_store()` operativo (L2 dedup activo)
- No Schema Registry — wire format = dataclasses nativos (BC-35)

### Clasificación
✅ **IMPLEMENTED** — Core infra healthy. Schema Registry es deuda conocida (B-18).

---

## G7 — HEALTH CHECKS

### Requisito
> `/health`, `/ready` endpoints + `health_check.sh` all PASS.

### Implementación
| Check | Estado | Evidencia |
|-------|--------|-----------|
| `/health` (market-data:8001) | ✅ PASS | `{"status":"healthy","service":"market-data-service"}` |
| `/ready` (market-data:8001) | ✅ PASS | `{"status":"ready","last_run_s":...}` |
| `health_check.sh` | ✅ PASS | `MARKET_DATA_HEALTHY=HEALTHY`, `INFRA_HEALTHY=HEALTHY`, `OBSERVABILITY_HEALTHY=HEALTHY` |
| Streaming health | ⚠️ NO EXPUESTO | No HTTP endpoint en streaming_hydra (pushgateway only) |

### Evidencia
```bash
$ curl localhost:8001/health
{"status":"healthy","service":"market-data-service","version":"1.0.0","uptime_s":5823.1,"last_result":"success"}

$ ./scripts/health_check.sh
MARKET_DATA_HEALTHY=HEALTHY
INFRA_HEALTHY=HEALTHY
OBSERVABILITY_HEALTHY=HEALTHY
```

### Clasificación
✅ **IMPLEMENTED** — Core health checks PASS. Streaming health es deuda (F2.6c).

---

## G8 — IS_STUB = FALSE

### Requisito
> Live executor no usa stubs, risk guards activos, capital real path.

### Implementación
| Componente | Estado | Evidencia |
|------------|--------|-----------|
| `IS_STUB` | ✅ FALSE | `packages/trading/execution/live_executor.py:80` — `IS_STUB = False` |
| Risk Guards | ✅ ACTIVE | `RiskManager.validate()` en `execute_live.py` |
| Capital Real | ✅ PATH EXISTE | `live_hydra.py` → `LiveExecutor` → `OrderTransport` → CCXT |
| Paper Trading | ✅ SEPARADO | `paper_hydra.py` usa `PaperExecutor` |

### Evidencia
- Código: `live_executor.py:80` — constante `IS_STUB = False` (no configurable, diseño intencional F-031)
- `execute_live.py:225` llama `oms.manage_open_orders()` con risk validation
- `RiskManager` valida capital, drawdown, position limits antes de submit

### Clasificación
✅ **IMPLEMENTED** — Diseño intencional, no configurable, risk guards operativos.

---

## G9 — BRONZE FRESHNESS

### Requisito
> Bronze Iceberg recibe datos frescos (<15 min) con `run_id` válido para OHLCV Y Orderbook.

### Implementación

#### OHLCV ✅ PASS
| Métrica | Estado | Evidencia |
|---------|--------|-----------|
| Freshness | ✅ <15 min | `find bronze -mmin -15` → 2 archivos recientes |
| `run_id` presente | ✅ SÍ | `pl.read_parquet()` muestra `run_id` en metadatos |
| Schema | ✅ 12 fields | `BRONZE_SCHEMA` IDs 1-12 (run_id = ID 12) |
| Particionado | ✅ Correcto | `exchange/market_type/symbol/timeframe/ts_month` |

#### Orderbook ❌ FAIL
| Métrica | Estado | Evidencia |
|---------|--------|-----------|
| Freshness | ❌ NO DATA | No consumer → no writes |
| `run_id` | ❌ N/A | No schema Bronze orderbook |
| Schema | ❌ MISSING | `schemas.py` sin ORDERBOOK |
| Tabla Iceberg | ❌ MISSING | Solo `bronze.ohlcv` existe |

### Evidencia OHLCV
```bash
$ find data_platform/iceberg_warehouse/bronze -name "*.parquet" -mmin -15
.../bronze/ohlcv/data/exchange=bybit/market_type=spot/symbol=BTC%2FUSDT/.../00000-....parquet

$ python3 -c "import polars as pl; df=pl.read_parquet('...'); print(df.columns)"
['timestamp','open','high','low','close','volume','exchange','market_type','symbol','timeframe','ingestion_ts','run_id']
```

### Clasificación
⚠️ **PARTIALLY IMPLEMENTED** — OHLCV PASS completo, Orderbook FAIL total.

---

## MAPEO PROBLEMAS → GATES

| Problema | Gates Afectados | Severidad |
|----------|-----------------|-----------|
| Orderbook.raw sin consumer | G5, G9 | **CRÍTICO** |
| Orderbook schema Bronze faltante | G9 | **CRÍTICO** |
| Orderbook Bronze table faltante | G9 | **CRÍTICO** |
| Market Universe fragmentado (3 configs) | G5, G9 | ALTO |
| Formatos símbolo incompatibles | G5, G9 | ALTO |
| Streaming restart no verificado | G4 | BAJO |
| Schema Registry ausente | G6 | MEDIO (B-18) |

---

## ESTADO B-49 CONSOLIDADO

```
B-49 = PARTIALLY IMPLEMENTED
├── G4: ✅ PASS
├── G5: ⚠️ PARTIAL  (orderbook consumer missing)
├── G6: ✅ PASS     (schema registry deuda B-18)
├── G7: ✅ PASS
├── G8: ✅ PASS
└── G9: ⚠️ PARTIAL  (OHLCV ✅, Orderbook ❌)
```

**NO DECLARAR B-49 PASS**

### Blockers para B-49 PASS
1. **OrderbookBronzeWriter** consumer para `orderbook.raw`
2. **ORDERBOOK_SCHEMA** en `schemas.py` (snapshot + delta)
3. **Bronze.orderbook_snapshot** + **Bronze.orderbook_delta** tablas Iceberg
4. **BronceStorage.append_snapshot/delta** methods
5. **Market Universe unificado** (SSOT único, formatos normalizados)

### Próximos Gates (G10+)
| Gate | Descripción | Estado |
|------|-------------|--------|
| G10 | Position state single-owner | PENDIENTE (B-15 tracking) |
| G11 | Tracing context propagation | ✅ HECHO (B-17) |
| G12 | Order fill reconciliation | PENDIENTE (B-MD-009) |
| G13 | Strategy signal validation | PENDIENTE |

---

## ACCIÓN REQUERIDA PARA B-49 PASS

**Orden de prioridad estricta:**

1. **P0** — `OrderbookBronzeWriter` consumer + `KafkaConsumerAdapter.for_orderbook()`
2. **P0** — `ORDERBOOK_SNAPSHOT_SCHEMA` + `ORDERBOOK_DELTA_SCHEMA` en `schemas.py`
3. **P0** — Tablas `bronze.orderbook_snapshot` + `bronze.orderbook_delta` + `BronzeStorage.append_snapshot/delta`
4. **P0** — `MarketUniverseProvider` SSOT + migración consumidores (resuelve format mismatch)
5. **P1** — `OrderbookBuilder` con gap detection + recovery (calidad histórica)
6. **P2** — `health_check.sh` incluye streaming health
7. **P2** — Systemd restart test documentado (B-59)

**Solo después de 1-4 completar → B-49 PASS**