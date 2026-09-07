# KB / TRAINING CHANGES — CONTENIDOS A ACTUALIZAR

> **Fecha**: 2026-09-04
> **Objetivo**: Formación técnicamente correcta desde cero. Un nuevo ingeniero debe poder responder las 22 preguntas del criterio de éxito leyendo esto.

---

## MANIFEST.YAML — ENTRADAS NUEVAS / ACTUALIZADAS

```yaml
# docs/knowledge/manifest.yaml — entradas a añadir/actualizar
entries:
  - id: "orderbook-concepts"
    title: "Orderbook: Snapshot, Delta, Sequence, Gap Recovery"
    tier: "TIER_1"  # Primaria — código real
    status: "verified"
    authority: "code"
    path: "notes/orderbook_concepts.md"
    tags: ["orderbook", "market-data", "kappa", "gap-recovery"]
  
  - id: "market-universe-vs-discovery"
    title: "Market Universe vs Instrument Discovery — Distinción Fundamental"
    tier: "TIER_1"
    status: "verified"
    authority: "code"
    path: "notes/market_universe.md"
    tags: ["market-universe", "discovery", "config", "architecture"]
  
  - id: "production-gates-g4-g9"
    title: "Production Gates G4-G9 — Qué valida cada uno"
    tier: "TIER_1"
    status: "verified"
    authority: "code"
    path: "notes/production_gates.md"
    tags: ["gates", "b49", "production", "ci"]
  
  - id: "b49-status"
    title: "B-49 Status — Partially Implemented, Blockers, Next Steps"
    tier: "TIER_1"
    status: "verified"
    authority: "code"
    path: "notes/b49_status.md"
    tags: ["b49", "tracking", "gates", "status"]
  
  - id: "protocol-discovery-framework"
    title: "Protocol Discovery Framework (ADR-0017) — Metodología"
    tier: "TIER_2"  # ADR accepted, not fully implemented
    status: "planned"
    authority: "adr"
    path: "notes/protocol_discovery.md"
    tags: ["protocol-discovery", "adr-0017", "bybit", "profile"]
  
  - id: "unified-adapter-analysis"
    title: "Unified Exchange Adapter — Viability Analysis (LATER)"
    tier: "TIER_3"  # Analysis only, not approved
    status: "analysis"
    authority: "analysis"
    path: "notes/unified_adapter.md"
    tags: ["unified-adapter", "refactor", "later", "architecture"]
```

---

## NOTES — CONTENIDO DETALLADO POR ARCHIVO

### `notes/orderbook_concepts.md`
```markdown
# Orderbook: Snapshot, Delta, Sequence, Gap Recovery

## Qué es un Orderbook (L2)
Libro de órdenes nivel 2: agrega órdenes por precio → (price, size) por lado.
- **Bid**: compras, ordenado descendente (mejor precio primero)
- **Ask**: ventas, ordenado ascendente (mejor precio primero)

## Snapshot vs Delta
| Tipo | Cuándo | Contenido | Acción Builder |
|------|--------|-----------|----------------|
| **Snapshot** | Conexión inicial / reconexión | Libro completo (todos los niveles) | `replace_state(bids, asks)` |
| **Delta** | Cada update incremental | Cambios: new/update/delete por nivel | `apply_delta(side, price, size)` |

## Sequence Numbers / Update IDs
- Cada mensaje del exchange trae `sequence_number` (o `update_id`)
- **Regla de oro**: `expected = last_seen + 1`
- Si `actual != expected` → **GAP DETECTADO**

## Gap Detection
```python
def detect_gap(expected: int, actual: int) -> GapInfo:
    if actual > expected:
        return GapInfo(
            missing_count=actual - expected,
            expected_next=expected,
            received=actual,
            severity="HIGH" if actual - expected > 100 else "MEDIUM"
        )
    return None  # duplicate o reorder (manejar por ID)
```

## Gap Recovery
1. **Pausar** aplicación de deltas
2. **Solicitar snapshot REST** (`fetch_order_book` con limit suficiente)
3. **Aplicar snapshot** → resetea estado, `last_sequence = snapshot.sequence`
4. **Reanudar** deltas desde `last_sequence + 1`
5. Si snapshot falla → alerta, marcar gap como `failed`

## Checksum Validation
- Bybit envía `checksum` en snapshot (CRC32 de top N niveles)
- Validar: `compute_checksum(local_book) == exchange_checksum`
- Mismatch → corrupt state → forzar recovery

## Timestamp Units
| Fuente | Campo | Unidad | Conversión |
|--------|-------|--------|------------|
| `receipt_timestamp` | local (cryptofeed) | segundos (float) | `* 1000` → ms |
| `book.timestamp` | Bybit | **milisegundos** (ya) | **NO multiplicar** |
| `book.timestamp` | KuCoin | microsegundos | `/ 1000` → ms |

## Reconstrucción Histórica desde Bronze
1. Buscar **snapshot más reciente ≤ timestamp objetivo**
4. Aplicar **deltas en orden** hasta timestamp objetivo
5. Resultado: `BookState` point-in-time para backtesting/OFI/CVD

## Métricas Clave
- `orderbook_snapshots_total` — snapshots recibidos
- `orderbook_deltas_total` — deltas aplicados
- `orderbook_gaps_detected_total` — gaps count
- `orderbook_gap_recovery_duration_seconds` — tiempo recovery
- `orderbook_checksum_mismatches_total` — integridad
```

---

### `notes/market_universe.md`
```markdown
# Market Universe vs Instrument Discovery — Distinción Fundamental

## Definiciones

| Concepto | Pregunta | Responsable | Fuente |
|----------|----------|-------------|--------|
| **Instrument Discovery** | "¿Qué existe en el exchange?" | Protocol Discovery | CCXT `load_markets()`, WS subscribe |
| **Instrument Metadata** | "¿Cuáles son las reglas de este instrumento?" | Instrument Registry | Discovery → normalized metadata |
| **Market Universe** | "¿Qué decide OCM observar/operar?" | Config (SSOT) | `config/market_data/universe.yaml` |

## Discovery ≠ Universe Selection
- **Discovery** = observación pasiva de lo disponible
- **Universe** = decisión activa de qué subconjunto usar
- **Nunca** hardcodear universe en discovery code

## Flujo Correcto
```
1. Protocol Discovery (Bybit Profile)
   │
   ▼
2. Instrument Metadata Registry (SSOT)
   ├── symbol → {tick_size, lot_size, min_qty, status, ...}
   └── provenance: PROTOCOL | DOCUMENTATION | UPSTREAM_LIBRARY
   │
   ▼
3. Market Universe Config (SSOT)
   ├── enabled: true/false
   ├── discovery.mode: auto | static | hybrid
   └── symbols: [BTC/USDT, ETH/USDT, ...]  # Canonical format
   │
   ▼
4. Normalizer (canonical → venue-native)
   ├── CCXT (REST): "BTC/USDT"
   ├── Cryptofeed (WS): "BTC-USDT-PERP" (linear)
   └── Internal: "BTC/USDT" (canonical)
```

## Formatos por Venue
| Venue | Spot | Linear | Inverse |
|-------|------|--------|---------|
| **Canonical (Internal)** | `BTC/USDT` | `BTC/USDT` | `BTC/USD` |
| **CCXT (REST)** | `BTC/USDT` | `BTC/USDT:USDT` | `BTC/USD` |
| **Cryptofeed (WS)** | `BTC-USDT` | `BTC-USDT-PERP` | `BTC-USD-PERP` |

## Auto-Discovery vs Static
```yaml
# universe.yaml
discovery:
  mode: "hybrid"        # auto | static | hybrid
  auto_discover: true   # Usa CCXT load_markets()
  refresh_hours: 24
  validate_on_startup: true  # Fail-fast si símbolo config no existe
```

## Qué NO hacer
- ❌ Hardcodear símbolos en código
- ❌ Duplicar símbolos en múltiples YAMLs
- ❌ Mezclar formatos CCXT/Cryptofeed sin normalizer
- ❌ Asumir que Discovery = Universe
```

---

### `notes/production_gates.md`
```markdown
# Production Gates G4-G9 — Qué Valida Cada Uno

## G4 — Systemd Units Valid
**Qué**: Units correctamente configuradas, enabled, restart limpio.
**Evidencia**: `systemctl status ocm-market-data ocm-streaming` → ACTIVE
**Ejecución**: `scripts/check_production_gates.py --gate G4`

## G5 — Kafka Connectivity
**Qué**: Broker accesible, producers/consumers healthy, topics operativos.
**Evidencia**: 
- `kafka-topics --list` incluye `ohlcv.raw`, `trades.raw`, `orderbook.raw`
- Offsets avanzando en consumer groups
- `health_check.sh` → `INFRA_HEALTHY=HEALTHY`
**Gap actual**: `orderbook.raw` sin consumer

## G6 — Infra Health
**Qué**: Kafka + Redis + Iceberg + Schema Registry operativos.
**Evidencia**: `health_check.sh` → `INFRA_HEALTHY=HEALTHY`
**Deuda**: Schema Registry (Avro) — tracking B-18

## G7 — Health Checks
**Qué**: `/health`, `/ready` endpoints + `health_check.sh` all PASS.
**Evidencia**:
- `curl localhost:8001/health` → `{"status":"healthy"}`
- `curl localhost:8001/ready` → `{"status":"ready"}`
- `./scripts/health_check.sh` → 3/3 domains HEALTHY
**Gap**: Streaming health no expuesto (solo pushgateway)

## G8 — IS_STUB = FALSE
**Qué**: Live executor no usa stubs, risk guards activos.
**Evidencia**: `packages/trading/execution/live_executor.py:80` → `IS_STUB = False`
**Diseño**: Intencional, no configurable (F-031)

## G9 — Bronze Freshness
**Qué**: Bronze Iceberg recibe datos frescos (<15 min) con `run_id` válido.
**Evidencia OHLCV** ✅:
- `find bronze -mmin -15` → archivos recientes
- `pl.read_parquet()` → `run_id` presente
- Schema 12 fields (run_id = ID 12)
**Evidencia Orderbook** ❌:
- Sin consumer → sin writes
- Sin schema → sin tabla
- **BLOQUEA G9 PASS COMPLETO**

## G10 — Position State Single Owner (PENDIENTE)
**Qué**: Una sola fuente de verdad para estado de posiciones.
**Tracking**: B-15, ADR-0021 propuesta

## G11 — Tracing Context Propagation (HECHO)
**Qué**: Request-ID propagado en consumers via OpenTelemetry.
**Evidencia**: `BaseConsumer._traced_handle`, 14 tests passing (B-17)
```

---

### `notes/b49_status.md`
```markdown
# B-49 Status — Partially Implemented

## Resumen
| Gate | Estado | Blocker |
|------|--------|---------|
| G4 | ✅ PASS | — |
| G5 | ⚠️ PARTIAL | Orderbook consumer missing |
| G6 | ✅ PASS | Schema Registry deuda (B-18) |
| G7 | ✅ PASS | Streaming health gap |
| G8 | ✅ PASS | — |
| G9 | ⚠️ PARTIAL | **Orderbook → Bronze missing** |

## Blockers Críticos
1. **OrderbookBronzeWriter** consumer para `orderbook.raw`
2. **ORDERBOOK_SCHEMA** en `schemas.py` (snapshot + delta)
3. **Tablas Bronze** `bronze.orderbook_snapshot` + `bronze.orderbook_delta`
4. **BronceStorage.append_snapshot/delta** methods

## Próximos Pasos (Orden Estricto)
1. P0: Orderbook Bronze consumer + schema + tabla
2. P0: Market Universe SSOT (resuelve format mismatch)
3. P1: Orderbook Builder (gap detection + recovery)
4. P2: Streaming health endpoint + health_check.sh
5. P2: Systemd restart test (B-59)

## No Declarar PASS Hasta
- `check_production_gates.py` retorna PASS en G1-G9
- Orderbook datos verificables en Bronze (<5 min freshness)
- Market Universe único consumido por REST y WS
```

---

### `notes/protocol_discovery.md`
```markdown
# Protocol Discovery Framework (ADR-0017) — Metodología

## 14 Componentes del Framework
1. **Objetivo** — Contrato entrada/salida del discovery
2. **Principios** — Evidencia > suposición, linaje obligatorio, dominio ≠ protocolo
3. **Tipos de evidencia** — PROTOCOL, DOCUMENTATION, UPSTREAM_LIBRARY, DOMAIN, ASSUMED
4. **REST Discovery** — Endpoints, schemas, snapshots, errores, límites
5. **WebSocket Discovery** — Streams, snapshots, deltas, sequence, reconnect, recovery
6. **Execution Discovery** — Ciclo orden→fill→estado
7. **Funding Discovery** — Mensajes funding/interest
8. **Liquidation Discovery** — Mensajes liquidaciones
9. **Contract Provenance** — Linaje cada contrato (taxonomía punto 3)
10. **Normalización** — Mensaje observado → Modelo interno (ports/outbound/normalization.py)
11. **Validación** — Invariantes, tipos, rangos, compatibilidad backward
12. **Fixtures** — Muestras congeladas mensajes reales
13. **Tests** — Linaje, normalización, validación
14. **Promotion Rule** — Solo PROTOCOL/DOC/UPSTREAM/DOMAIN → SSOT; ASSUMED bloquea live

## Discovery Profile Bybit (Priority 1)
| Componente | Estado | Evidencia |
|------------|--------|-----------|
| REST OHLCV | ✅ | CCXTAdapter + HistoricalFetcherAsync |
| REST Trades | ✅ | CCXTAdapter + TradesFetcher |
| WS Orderbook (L2_BOOK) | ⚠️ | CryptofeedOrderBookStream (sin gap recovery) |
| WS Trades | ✅ | BybitCryptofeedRunner |
| Funding/OI WS | ❌ | Producers existen, sin runners |
| Liquidations WS | ❌ | Producer existe, sin runner |
| Metadata (tick/lot) | ❌ | No capturado |
| Contract Provenance | 📋 | test_schema_provenance.py semilla |

## Promotion Rule Gate (CI)
```yaml
# En check_production_gates.py
def check_promotion_rule():
    for contract in CRITICAL_CONTRACTS:
        if contract.provenance == "ASSUMED":
            return BLOCK, f"{contract} is ASSUMED — blocks live"
    return PASS
```
```

---

### `notes/unified_adapter.md`
```markdown
# Unified Exchange Adapter — Viability Analysis (LATER)

## Clasificación: LATER (post-B-49)

## Qué Resuelve
- Duplicación: CCXTAdapter + BybitCryptofeedRunner → 1 clase
- Market Universe: 1 SSOT, normalizador CCXT↔Cryptofeed
- Config: 1 `enabled` controla REST + WS
- Dual mode: Parity validation automática

## Qué NO Resuelve (B-49 Blockers)
- Orderbook → Bronze (consumer, schema, tabla)
- Gap detection / recovery
- Instrument Metadata Registry
- Protocol Discovery formal

## Abstracciones Existentes
| Protocol | Archivo | Cubre |
|----------|---------|-------|
| `ExchangeAdapter` | `adapters/outbound/exchange/base.py` | REST only |
| `MarketDataSource` | `ports/inbound/market_data_source.py` | WS trades only |
| `FeedRunnerProtocol` | `adapters/inbound/websocket/feed_runner_protocol.py` | WS lifecycle |
| `TradesSource` | `ports/inbound/trades_source.py` | REST+WS trades unified |

**Falta**: Protocolo unificado REST + WS + Orderbook + Metadata

## Riesgos
- BC-NN contracts (49) requieren update + ADR
- God class risk: REST + WS + Metadata + Discovery
- Dual mode parity validation cambia semántica
- Tests: mock CCXT + Cryptofeed + Kafka

## Prerrequisitos
1. B-49 PASS
2. Market Universe SSOT
3. Orderbook Builder funcional
4. ADR aprobado
5. BC-NN actualizados

## Conclusión: Mejora arquitectura post-MVP, no requisito producción.
```