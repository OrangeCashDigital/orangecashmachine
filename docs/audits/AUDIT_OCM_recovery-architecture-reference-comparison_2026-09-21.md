# AUDITORÍA — Arquitectura de Recuperación OHLCV: OCM vs Nautilus vs Hummingbot vs Freqtrade

- **ID**: MD-RECOVERY-003
- **Fecha**: 2026-09-21
- **Modo**: READ-ONLY. Cero modificaciones.
- **Scope**: Comparación arquitectónica de recuperación de datos OHLCV entre OCM y tres modelos de referencia (Nautilus, Hummingbot, Freqtrade).
- **Complementa**: `docs/audits/AUDIT_OCM_ohlcv-repair-backfill-execution-paths_2026-09-21.md` (MD-RECOVERY-001), `docs/audits/AUDIT_OCM_ohlcv-repair-backfill-causality_2026-09-21.md` (MD-RECOVERY-002).

---

## 1. EXECUTIVE SUMMARY

**Veredicto**: `CONFIRMED` — La arquitectura actual de OCM tiene las piezas individuales correctas pero el diseño de integración es insuficiente. Los tres proyectos de referencia demuestran patrones que OCM no adopta:

- **Hummingbot**: `CandlesBase` une REST + WS en una sola clase con `deque` circular. `MarketDataProvider` orquesta ambos.
- **Freqtrade**: `DataProvider` encapsula exchange + handlers de disco. `download_data_main()` descarga datos históricos.
- **Nautilus**: Framework completo con adapters por exchange, backtesting y persistence integrada.

**OCM tiene un problema de integración, no de piezas.** `RepairStrategy` existe y funciona cuando se le proporciona `storage`. `IncrementalStrategy` y `BackfillStrategy` funcionan correctamente. El problema es que **ningún componente orquesta la recuperación**. `QualityPipeline._scan_and_emit_gaps` detecta gaps pero no dispara ninguna acción.

**DECISIÓN ARQUITECTÓNICA**: Ver sección 17.

---

## 2. EVIDENCIA REVISADA

### 2.1 Documentos OCM
- `AUDIT_OCM_ohlcv-repair-backfill-execution-paths_2026-09-21.md`
- `AUDIT_OCM_ohlcv-repair-backfill-causality_2026-09-21.md`
- `AUDIT_OCM_market-data-audit_2026-05-27.md`
- `AUDIT_OCM_market-data-deep-audit_2026-08-14.md`
- `OCM_AUDIT_FINDINGS_2026-08-20_market-data-runtime.md`
- `AUDIT_OCM_b59-runtime-operational-audit_2026-09-19.md`
- `AUDIT_OCM_orderbook-resync-causality-audit_2026-09-21.md`

### 2.2 Documentos de referencia
- `docs/nautilus_trader-develop.zip` (28.4 MB)
- `docs/hummingbot-master.zip` (3.9 MB)
- `docs/freqtrade-develop.zip` (45.1 MB)

---

## 3. RUNTIME ACTUAL DE OCM

### 3.1 Call chain completo

```
python -m market_data.main
  → uvicorn (FastAPI)
    → _lifespan()
      → CompositionRoot.assemble(config)
        → ConcretePipelineFactory(cfg)
      → PipelineOrchestrator(factory)
      → _ingestion_loop(ctx, guard)
        → PipelineRequest(mode="incremental")  ← HARDCODED
          → OHLCVPipeline.__init__()
            → PipelineContext(
                fetcher=HistoricalFetcherAsync(storage=None),
                cursor=RedisCursorStore,
                quality=QualityPipeline,
                publisher=KafkaOHLCVPublisher / NullOHLCVPublisher,
                metrics=PrometheusPipelineMetrics,
                storage=None,              ← NO INYECTADO
                gap_registry=None,         ← DEFAULT
                _chunk_converter=PassthroughChunkConverter,
              )
            → strategies = {
                INCREMENTAL: IncrementalStrategy(),
                BACKFILL: BackfillStrategy(),
                REPAIR: RepairStrategy(metrics=PrometheusRepairMetrics()),
              }
          → pipeline.run(mode="incremental")
            → IncrementalStrategy._run()
              → fetcher.download_data()
                → _download_chunked()
                  → _resolve_start_timestamp()  ← cursor Redis
                  → _fetch_chunk_with_retry()   ← CCXT REST
                  → transformer.transform()
                  → return pl.DataFrame
              → quality.run(df)               ← gap scan (solo métricas)
              → publisher.publish_chunk()     ← Kafka ohlcv.raw
              → cursor.update()               ← Redis
```

### 3.2 Estado de cada etapa

| Etapa | ¿Existe? | ¿Conectada? | ¿Ejecuta? | ¿Persiste? | ¿Valida? | ¿Producción? | ¿Tests? |
|---|---|---|---|---|---|---|---|
| Entrypoint (`main.py`) | ✅ | — | ✅ | — | — | ✅ (container) | — |
| `_ingestion_loop` | ✅ | ✅ | ✅ | — | — | ✅ | — |
| `PipelineRequest(mode="incremental")` | ✅ | ✅ | ✅ | — | — | ✅ | — |
| `OHLCVPipeline` | ✅ | ✅ | ✅ | — | ✅ | ✅ | ✅ |
| `PipelineContext` | ✅ | ✅ | ✅ | — | — | ✅ | ✅ |
| `PipelineContext.storage` | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ (test con stub) |
| `IncrementalStrategy` | ✅ | ✅ | ✅ | ✅ (Kafka) | ✅ | ✅ | ✅ |
| `BackfillStrategy` | ✅ | ✅ | ⚠️ CLI | ✅ (Kafka) | ✅ | ⚠️ | ✅ |
| `RepairStrategy` | ✅ | ❌ | ❌ | ❌ | ✅ | ❌ | ✅ |
| `HistoricalFetcherAsync` | ✅ | ✅ | ✅ | — | ✅ | ✅ | ✅ |
| `QualityPipeline._scan_and_emit_gaps` | ✅ | ✅ | ✅ | ❌ (solo logs/metrics) | ✅ | ✅ | — |
| `GapScanner` | ✅ | ✅ | ❌ (utilidad) | ❌ | — | ❌ | ✅ |
| `KafkaBronzeWriter` | ✅ | ✅ | ✅ | ✅ | — | ✅ | — |
| `IcebergStorage.save_ohlcv` | ✅ | ✅ | ✅ | ✅ | ✅ (dedup) | ✅ | — |

---

## 4. ANÁLISIS DE CAUSALIDAD DEL GAP

### 4.1 Flujo actual cuando se produce un gap

```
gap ocurre (ej: 10:01-10:05 missing)
  ↓
_próxima iteración de _ingestion_loop (300s después)
  ↓
IncrementalStrategy ejecuta
  ↓
fetcher.download_data(symbol, timeframe, start_date="auto")
  ↓
_resolve_start_timestamp() → cursor Redis = último timestamp
  ↓
Si cursor = 10:00, descarga desde 09:59 (con overlap)
  ↓
Pero 10:01-10:05 YA FALTAN en Silver (no se descargaron nunca)
  ↓
El incremental solo avanza hacia adelante desde el cursor
  ↓
Necesita que el cursor retroceda o que alguien active repair
  ↓
QualityPipeline._scan_and_emit_gaps(df) → gap detected
  ↓
logger.warning("Gap scan | total=1 high=1...")
  ↓
self._metrics.quality_gaps_inc(...)
  ↓
FIN (sin acción correctiva)
```

### 4.2 Ruptura de causalidad

```
gap detected
     ↓
metric/log           ←─── FIN AQUÍ
     ↓
(ninguna acción)
```

**NO existe:**
```
gap detected
     ↓
recovery decision
     ↓
repair/execute
     ↓
persistence
     ↓
validation
     ↓
completeness verified
```

### 4.3 ¿Por qué no hay acción?

1. `QualityPipeline._scan_and_emit_gaps` solo emite métricas y logs
2. No existe ningún componente que escuche gap events y dispare reparación
3. `RepairStrategy._read_silver()` tiene `assert ctx.storage is not None` que falla
4. `PipelineContext.storage` nunca se inyecta
5. El `_ingestion_loop` hardcodea `mode="incremental"` — nunca `mode="repair"`

---

## 5. BACKFILL vs REPAIR vs RECONCILIATION — DEFINICIÓN PRECISA

### 5.1 Tabla arquitectónica

| Concepto | Propósito | Trigger | Fuente | Persistencia | Validación | Runtime actual |
|---|---|---|---|---|---|---|
| **Incremental** | Descargar datos nuevos desde cursor | Temporizador (300s) | REST CCXT (cursor → now) | Kafka → BronzeWriter → Iceberg | Quality + gap scan | ✅ OCM |
| **Backfill** | Cargar historial completo desde now → origin | Manual (CLI) | REST CCXT (now → origin) | Kafka → BronzeWriter → Iceberg | Quality + gap scan | ⚠️ Solo CLI |
| **Repair** | Sanar gaps en Silver existente | ??? (NINGÚN) | REST CCXT (rango gap) + Silver lectura | Directo Iceberg (`save_ohlcv`) | Quality | ❌ ROTO |
| **Reconciliation** | Verificar y corregir inconsistencias entre capas | ??? (NINGÚN) | Iceberg lectura | Iceberg | Idempotencia | ❌ INEXISTENTE |

### 5.2 Solapamiento y confusión arquitectónica

**Backfill y Repair comparten**:
- Mismo `HistoricalFetcherAsync` (REST CCXT)
- Misma `QualityPipeline` (validación)
- Misma dedup `unique(keep="last")`
- Misma persistencia `IcebergStorage`

**Pero son fundamentalmente distintos**:
- Backfill: `start_date` → now, descarga TODO, no lee Silver
- Repair: lee Silver → detecta gaps → descarga SOLO rangos faltantes

**Reconciliation** es un concepto ausente en OCM. No existe ningún mecanismo que verifique que Bronze/Silver/Gold están consistentes. Cada capa es independiente.

---

## 6. PROYECTOS DE REFERENCIA

### 6.1 Nautilus

| Aspecto | Detalle | Evidencia |
|---|---|---|
| **Estructura** | Framework de trading completo, no solo market data | `nautilus/nautilus_trader-develop/python/nautilus_trader/` |
| **Data** | `nautilus_trader/_libnautilus/data` (Rust, libnautilus) | `data/__init__.py` importa `from nautilus_trader._libnautilus.data import *` |
| **Adapters** | Uno por exchange (bybit, binance, okx, etc.) | `adapters/bybit/`, `adapters/binance/`, etc. |
| **Databento** | Adapter para Databento data provider | `adapters/databento/` |
| **Persistence** | `persistence/loaders.py` con `CSVTickDataLoader` | `persistence/loaders.py` |
| **Backtest** | `backtest/` con backtest engine completo | `backtest/__init__.py` |
| **REST historical** | Via adapters (Databento, exchange-specific) | `adapters/databento/` |
| **WebSocket** | Via adapters | `adapters/` |
| **Backfill** | ❌ NO existe concepto explícito de backfill | — |
| **Repair** | ❌ NO existe | — |
| **Reconciliation** | ❌ NO existe | — |
| **Dedup** | No visible en Python (implementado en Rust) | — |
| **Source of truth** | Adapter → Exchange → Databento | — |

**Conclusión Nautilus**: Es un framework de trading completo donde la recuperación de datos está delegada a los adapters (Databento, exchange-specific). No tiene conceptos explícitos de `repair`, `backfill` o `reconciliation`. Los datos fluyen desde el adapter al sistema de trading y el backtesting.

### 6.2 Hummingbot

| Aspecto | Detalle | Evidencia |
|---|---|---|
| **Estructura** | Exchange bot con market data feeds | `hummingbot/hummingbot-master/hummingbot/` |
| **MarketData model** | SQLAlchemy: `timestamp`, `exchange`, `trading_pair`, `mid_price`, `best_bid`, `best_ask`, `order_book` | `model/market_data.py` |
| **MarketDataProvider** | Gestiona connectors + candles feeds + rates | `data_feed/market_data_provider.py` |
| **CandlesBase** | **UNE REST + WS** en una sola clase | `data_feed/candles_feed/candles_base.py` |
| **CandlesBase._candles** | `deque(maxlen=max_records)` para almacenamiento circular | `candles_base.py` |
| **CandlesBase** | `rest_url`, `wss_url`, `candles_url` propiedades | Cada exchange subclass |
| **CandlesFactory** | Mapea connector → CandlesBase subclass | `data_feed/candles_feed/candles_factory.py` |
| **BybitPerpetualCandles** | Extiende `CandlesBase`, implementa `rest_url`, `wss_url` | `candles_feed/bybit_perpetual_candles/` |
| **REST historical** | `candles_url` → REST fetch para bootstrap | `candles_base.py` |
| **WebSocket** | `wss_url` → WS para live data | `candles_base.py` |
| **Backfill** | ❌ No concepto explícito | — |
| **Repair** | ❌ No existe | — |
| **Reconciliation** | ❌ No existe | — |
| **Dedup** | `deque` con `maxlen` + sobreescritura | `candles_base.py` |
| **Source of truth** | Exchange (vía connector) | `connector_base.py` |

**Conclusión Hummingbot**: El patrón clave es **`CandlesBase` une REST y WS en una sola clase**. REST se usa para historical bootstrap y WS para live updates. El `deque` actúa como buffer circular con dedup por sobreescritura. `MarketDataProvider` orquesta todo. No hay backfill/repair explícitos — la recuperación es implícita en el patrón REST+WS.

### 6.3 Freqtrade

| Aspecto | Detalle | Evidencia |
|---|---|---|
| **Estructura** | Trading bot con data handlers | `freqtrade/freqtrade-develop/freqtrade/` |
| **DataProvider** | Wraps exchange + data handlers | `data/dataprovider.py` |
| **IDataHandler** | Interfaz para almacenamiento en disco | `data/history/datahandlers/idatahandler.py` |
| **DataHandlers** | Arrow, Feather, JSON, Parquet | `data/history/datahandlers/` |
| **load_pair_history** | Carga OHLCV desde disco | `data/history/history_utils.py` |
| **download_data_main** | Descarga datos desde exchange → disco | `data/history/history_utils.py` |
| **refresh_backtest_ohlcv_data** | Re-descarga datos para backtest | `data/history/history_utils.py` |
| **REST historical** | `exchange.fetch_ohlcv()` | `exchange/exchange.py` |
| **WebSocket** | ❌ No hay WS nativo (polling REST) | — |
| **Backfill** | `download_data_main()` descarga historial completo | `history_utils.py` |
| **Repair** | ❌ No existe | — |
| **Reconciliation** | ❌ No existe | — |
| **Dedup** | `trades_df_remove_duplicates()`, `clean_ohlcv_dataframe()` | `data/converter.py` |
| **Source of truth** | Exchange → disco local | — |

**Conclusión Freqtrade**: Datos se descargan desde exchange → almacenan en disco local (parquet/csv). `DataProvider` encapsula exchange + handlers. `refresh_backtest_ohlcv_data()` puede re-descargar datos. No hay WS ni repair — es un sistema de trading basado en datos históricos locales.

---

## 7. MATRIZ COMPARATIVA

### 7.1 Capacidades

| Capacidad | OCM | Nautilus | Hummingbot | Freqtrade | Aplicable a OCM |
|---|---|---|---|---|---|
| REST historical | ✅ CCXT paginado | ✅ Adapter/Databento | ✅ `candles_url` REST | ✅ `fetch_ohlcv` | ✅ Ya existe |
| REST snapshot | ❌ | ❌ | ❌ | ❌ | ❌ |
| WS live | ✅ Cryptofeed | ✅ Adapter | ✅ `wss_url` WS | ❌ Polling solo | ✅ Ya existe |
| Snapshot + delta | ❌ | ❌ | ❌ | ❌ | ❌ |
| Sequence validation | ❌ | ❌ | ❌ | ❌ | ❌ |
| Gap detection | ✅ `scan_gaps()` | ❌ | ❌ | ❌ | ✅ Ya existe |
| Reconnect | ❌ | ❌ | ✅ FeedHandler | ❌ | ⚠️ Parcial |
| Explicit resync | ❌ | ❌ | ❌ | ❌ | ❌ |
| Backfill | ✅ CLI | ❌ | ❌ | ✅ `download_data_main` | ✅ Ya existe |
| Repair | ✅ (ROTO) | ❌ | ❌ | ❌ | ⚠️ Implementada pero rota |
| Reconciliation | ❌ | ❌ | ❌ | ❌ | ❌ |
| Dedup | ✅ `unique(keep="last")` | ❌ | ✅ `deque(maxlen)` | ✅ `remove_duplicates` | ✅ Ya existe |
| Persistence boundary | ✅ Kafka→Iceberg | ✅ CSV/Rust | ✅ SQL/JSON | ✅ Disco local | ✅ Ya existe |
| Post-recovery validation | ✅ `QualityPipeline` | ❌ | ❌ | ❌ | ✅ Ya existe |
| Source of truth | Exchange (REST) | Exchange/Adapter | Exchange/Connector | Exchange/Disco | ✅ Exchange |

### 7.2 Patrón REST + WS

| Proyecto | REST | WS | Relación |
|---|---|---|---|
| **OCM** | `CCXTAdapter` (separado) | `CryptofeedOrderBookStream` (separado) | **Sin conexión** — pipelines independientes |
| **Hummingbot** | `CandlesBase.candles_url` | `CandlesBase.wss_url` | **Unidos** en una sola clase con `deque` |
| **Freqtrade** | `exchange.fetch_ohlcv()` | ❌ No hay WS | Solo REST |
| **Nautilus** | Adapter REST | Adapter WS | Vía adapters, sin concepto unificado visible |

**Conclusión**: Hummingbot es el único que une REST y WS en una sola clase (`CandlesBase`). OCM los mantiene completamente separados, lo cual impide que WS pueda usar datos REST para resync y viceversa.

---

## 8. REST ↔ WEBSOCKET COMPLEMENTARIDAD

### 8.1 Patrón Hummingbot (REFERENCIA MÁS APLICABLE)

```
CandlesBase(trading_pair, interval, max_records)
  ├── rest_url → REST fetch para histórico/bootstrap
  ├── wss_url  → WS para live updates
  ├── _candles = deque(maxlen=max_records)  ← buffer circular
  ├── _listen_candles_task  ← WS listener
  └── _fill_candles_task    ← REST filler
```

**Relación**:
- REST proporciona el baseline histórico
- WS proporciona deltas en tiempo real
- `deque` con `maxlen` actúa como buffer con dedup automático
- REST puede rellenar gaps si WS pierde datos

### 8.2 Patrón OCM ACTUAL

```
REST: CCXTAdapter → HistoricalFetcherAsync → IncrementalStrategy → Kafka → BronzeWriter → Iceberg
WS:   CryptofeedOrderBookStream → OrderBookKafkaProducer → orderbook.raw → (SIN consumer)
```

**Relación**: NULA. Son pipelines completamente independientes. WS orderbook no produce OHLCV. REST OHLCV no usa WS para resync.

### 8.3 ¿Debe OCM converger REST y WS?

**EVIDENCIA**: Hummingbot demuestra que REST y WS pueden coexistir en una sola clase (`CandlesBase`). Freqtrade no tiene WS. Nautilus delega a adapters.

**INFERENCIA**: Para OHLCV, REST es suficiente como pipeline principal (OCM ya lo tiene). WS es para OrderBook (dominio separado). La convergencia de REST+WS para OHLCV NO es necesaria si:
1. REST proporciona todo el OHLCV (incremental + backfill + repair)
2. WS es solo para OrderBook (dominio separado)

**RECOMENDACIÓN**: Mantener REST y WS como dominios separados (como OCM ya tiene). No intentar fusionarlos.

---

## 9. SOURCE OF TRUTH

### 9.1 OHLCV

| Capa | OCM Source of Truth | Evidencia |
|---|---|---|
| **Exchange** | CCXT `fetch_ohlcv` | `adapters/inbound/rest/ohlcv_fetcher.py` |
| **Bronze** | Kafka `ohlcv.raw` → Iceberg | `infrastructure/kafka/bronze_writer.py` |
| **Silver** | Iceberg `silver.ohlcv` | `infrastructure/storage/iceberg/iceberg_storage.py` |
| **Gold** | Iceberg Gold (feature engine) | `infrastructure/storage/gold/gold_storage.py` |

### 9.2 OrderBook

| Capa | OCM Source of Truth | Evidencia |
|---|---|---|
| **Exchange** | Cryptofeed WS `L2_BOOK` | `adapters/inbound/websocket/cryptofeed_orderbook_stream.py` |
| **Raw** | Kafka `orderbook.raw` | `adapters/outbound/kafka/orderbook_producer.py` |
| **Book state** | `BookBuilder` (memoria) | `application/processing/book_builder.py` |
| **Persistence** | ❌ Sin consumer | F-MD-003 |

### 9.3 Comparación con referencias

| Proyecto | Source of Truth | Evidencia |
|---|---|---|
| **OCM** | Exchange → REST → Kafka → Iceberg | `ohlcv_fetcher.py`, `bronze_writer.py` |
| **Hummingbot** | Exchange (connector) → CandlesBase deque → SQL | `model/market_data.py` |
| **Freqtrade** | Exchange → Disco local (parquet) | `data/history/history_utils.py` |
| **Nautilus** | Adapter (Databento/exchange) → persistence | `adapters/`, `persistence/loaders.py` |

### 9.4 Distinción necesaria

- **Source of truth del exchange**: CCXT/Cryptofeed son la fuente primaria de datos
- **Source of truth histórico de OCM**: Iceberg (Silver/Gold) es el almacenamiento durable
- **Estado materializado de OrderBook**: `BookBuilder` en memoria (no persistido)

---

## 10. ANÁLISIS DE OrderBook vs OHLCV

### 10.1 Son dominios separados

**Evidencia** de `AUDIT_OCM_orderbook-resync-causality-audit_2026-09-21.md`:
```
OHLCV: REST CCXT → Kafka ohlcv.raw → Bronze Iceberg → Quality → Silver/Gold
OrderBook: WS snapshot+delta → Kafka orderbook.raw → BookBuilder → book.* (no Bronze)
No dependency OHLCV vs OrderBook (different topics, different Bronze, different Silver)
```

### 10.2 Patrón OrderBook recovery (REFERENCIA)

```
WS snapshot (baseline)
  ↓
WS deltas (u=sequence)
  ↓
u != last+1 → GAP_DETECTED
  ↓
BookBuilder.reset() → has_snapshot=False
  ↓
DELTA_BEFORE_SNAPSHOT descartado
  ↓
WS reconnect → nuevo snapshot
  ↓
Resync completado
```

**Problema**: Si el gap ocurre SIN disconnect, no hay reconnect → no hay resync → ventana STALE indefinida.

### 10.3 OHLCV Recovery vs OrderBook Recovery

| Aspecto | OHLCV | OrderBook |
|---|---|---|
| **Gap detection** | `scan_gaps(df, timeframe)` temporal | `update_id != last+1` secuencial |
| **Recovery source** | REST CCXT | WS reconnect + snapshot |
| **Recovery target** | Silver Iceberg | BookBuilder estado |
| **Recovery trigger** | Ninguno | GAP_DETECTED automático |
| **Persistence** | Iceberg | Memoria + Kafka |
| **Dedup** | `unique(keep="last")` | `deque` / sequence check |
| **Convergencia** | No necesita WS | Necesita snapshot |

**Conclusión**: No diseñar una solución genérica para ambos. Son mecanismos fundamentalmente distintos.

---

## 11. PipelineContext.storage ANÁLISIS

### 11.1 Estado actual

| Ubicación | `storage` value | Evidencia |
|---|---|---|
| `PipelineContext.storage` | `field(default=None)` | `pipeline/runtime.py:273` |
| `OHLCVPipeline.__init__` | **No pasa `storage`** | `ohlcv_pipeline.py:281-293` |
| `_build_ohlcv` | **No crea `IcebergStorage`** | `pipeline_factory.py:334-351` |
| `_build_ohlcv` | `HistoricalFetcherAsync(storage=None)` | `pipeline_factory.py:283` |
| `RepairStrategy._read_silver()` | `assert ctx.storage is not None` | `repair.py:326-329` |
| Test `_make_ctx()` | `storage=_StorageStub(...)` | `test_repair_strategy.py:233` |

### 11.2 ¿Por qué `storage=None`?

**Diseño Kappa deliberado** (evidencia de `pipeline/runtime.py:269-272`):
```
storage=None es válido para backfill/incremental — solo Repair lo usa.
El caller que ejecute RepairStrategy debe inyectar storage aquí;
hoy OHLCVPipeline construye PipelineContext sin storage.
```

### 11.3 ¿Debe Repair tener acceso directo a Storage?

**Opción A — Repair → Storage directamente**:
- `PipelineContext.storage` = `IcebergStorage` inyectada
- `RepairStrategy._read_silver()` → `ctx.storage.load_ohlcv()`
- `RepairStrategy._heal_gap()` → `ctx.storage.save_ohlcv()`
- **Ventajas**: Simple, directo, igual que Incremental/Backfill usan `ctx.storage` implícitamente
- **Desventajas**: Rompe el patrón Kappa (producer no debería escribir directamente a Iceberg)

**Opción B — Repair → Port → Storage Adapter**:
- `PipelineContext` recibe `OHLCVStoragePort` (protocolo)
- `IcebergStorage` implementa el protocolo
- **Ventajas**: Clean Architecture, DIP, testabilidad
- **Desventajas**: Igual que A pero con capa de abstracción adicional

**Opción C — Repair → Application service → Storage**:
- Nuevo servicio `RecoveryService` que recibe `OHLCVStorage`
- `RepairStrategy` delega persistencia al servicio
- **Ventajas**: Separación de responsabilidades, repair no conoce Iceberg
- **Desventajas**: Complejidad adicional, puede ser sobreingeniería

**Opción D — Recovery orchestration → persistence boundary**:
- Nuevo componente `RecoveryOrchestrator` que orquesta todo
- No usa `PipelineContext.storage`
- Persiste directamente via `CompositionRoot`
- **Ventajas**: No cambia `PipelineContext` ni `OHLCVPipeline`
- **Desventajas**: Componente adicional, puede duplicar lógica

**Opción E — Reparación como task separado fuera de Pipeline**:
- `_lifespan` lanza task asyncio para `RepairStrategy`
- El task recibe `IcebergStorage` directamente desde `CompositionRoot`
- **Ventajas**: No cambia `PipelineContext`, mantiene `OHLCVPipeline` limpio
- **Desventajas**: Reparación fuera del ciclo de pipeline

### 11.4 Recomendación

**EVIDENCIA**: Hummingbot's `CandlesBase` tiene almacenamiento interno (`deque`). Freqtrade tiene `DataProvider` que gestiona datos. Nautilus tiene persistence integrada.

**INFERENCIA**: `RepairStrategy` es un componente de aplicación que necesita persistencia. Debería seguir el mismo patrón que `IncrementalStrategy` y `BackfillStrategy`: recibir sus dependencias via `PipelineContext`.

**RECOMENDACIÓN**: **Opción B** — `PipelineContext` recibe `OHLCVStoragePort` (el protocolo existente). `ConcretePipelineFactory._build_ohlcv` crea `IcebergStorage` via `IcebergStorageFactory` y lo pasa a `OHLCVPipeline`. Esto mantiene Clean Architecture sin romper el patrón Kappa (repair sigue siendo una estrategia más, no un cambio de patrón).

---

## 12. PRINCIPIO "DETECT → DECIDE → RECOVER → PERSIST → VERIFY"

### 12.1 Flujo formal propuesto

```
DETECT
  ↓  QualityPipeline._scan_and_emit_gaps() → gap detected
  ↓
  gap_ranges = scan_gaps(df, timeframe)
  ↓
  (actualmente: solo logs + metrics — sin acción)

DECIDE
  ↓  ¿Quién decide reparar?
  ↓
  Opciones:
  - _ingestion_loop (automático, cada N iteraciones)
  - Event-driven (GapDetectedEvent → consumer)
  - Dagster asset (lectura Iceberg → disparo)
  - Operador humano (CLI)

RECOVER
  ↓  RepairStrategy._heal_gap()
  ↓
  fetcher.fetch_chunk(symbol, timeframe, since, end_ms)
  → quality.run(df)
  → storage.save_ohlcv(df, symbol, timeframe)
  → gap_registry.mark_healed()
  → GapHealedEvent (si gap_publisher)

PERSIST
  ↓  IcebergStorage.save_ohlcv()
  ↓
  Dedup: unique(keep="last")
  → Iceberg snapshot
  → Silver actualizado

VERIFY
  ↓  Siguiente iteración de _ingestion_loop
  ↓
  scan_gaps() detecta que el gap ya no existe
  → GapDetectedEvent count = 0
  → Métrica quality_gaps = 0
  → Completeness verificado
```

### 12.2 Responsabilidades por fase

| Fase | Componente | Estado actual |
|---|---|---|
| DETECT | `QualityPipeline._scan_and_emit_gaps` | ✅ Funciona (solo metrics) |
| DECIDE | **NINGÚN** | ❌ Ausente |
| RECOVER | `RepairStrategy._heal_gap` | ✅ Funciona (si tiene storage) |
| PERSIST | `IcebergStorage.save_ohlcv` | ✅ Funciona |
| VERIFY | Próxima iteración `scan_gaps` | ✅ Funciona (pero nunca se llama) |

---

## 13. ANÁLISIS Target Architecture

### 13.1 Diagrama propuesto

```
                    EXCHANGE
                       │
              ┌────────┴────────┐
              │                 │
             REST              WS
              │                 │
        CCXTAdapter    Cryptofeed
        (fetch_ohlcv)   (snapshot+delta)
              │                 │
        ┌─────┴─────┐          │
        │            │          │
   Historical   OrderBook     │
   Fetcher      Stream        │
        │            │         │
        ▼            ▼         │
   ┌──────────────────────────────┐
   │       QUALITY GATE           │
   │  QualityPipeline             │
   │  scan_gaps() → gap_report   │
   └──────────────────────────────┘
              │
     ┌────────┴────────┐
     │                 │
  Detect            Detect
  gap_ranges        orderbook
     │            gap_detected
     ▼                 │
 ┌──────────────────────────────┐
 │       RECOVERY ORCHESTRATOR  │  ← NUEVO (o en _ingestion_loop)
 │  ┌─────────┐  ┌───────────┐ │
 │  │ Repair  │  │ Resync    │ │  ← NUEVO (para OrderBook)
 │  │Strategy │  │(OrderBook)│ │
 │  └────┬────┘  └─────┬─────┘ │
 └───────┼─────────────┼───────┘
         │             │
  ┌──────┴──────┐ ┌────┴─────┐
  │  storage    │ │  Book    │
  │  .save_     │ │Builder   │
  │  ohlcv()    │ │.reset()  │
  │  (Iceberg)  │ │          │
  └──────┬──────┘ └──────────┘
         │
   ┌─────┴─────┐
   │  ICEBERG  │
   │  Silver   │
   │  Bronze   │
   └─────┬─────┘
         │
   ┌─────┴─────┐
   │  Gold     │
   │  (feats)  │
   └───────────┘
```

### 13.2 Modificaciones respecto a OCM actual

| Cambio | Justificación | Evidencia |
|---|---|---|
| Añadir RecoveryOrchestrator (o task en _ingestion_loop) | Necesidad demostrada: gaps detectados no se reparan | `_scan_and_emit_gaps` no dispara reparación |
| Inyectar `storage` en `PipelineContext` | `RepairStrategy` necesita `ctx.storage` para leer/escribir Silver | `assert ctx.storage is not None` |
| `ConcretePipelineFactory` crea `IcebergStorage` | `PipelineContext.storage` debe ser `OHLCVStorage` | `IcebergStorageFactory` ya existe en `main.py::_lifespan` |
| Mantener REST y WS separados | Dominios distintos (OHLCV vs OrderBook) | `AUDIT_OCM_orderbook-resync-causality-audit_2026-09-21.md`: sin dependencia |
| No fusionar REST+WS para OHLCV | REST es suficiente para OHLCV (Hummingbot usa WS para OrderBook) | `Hummingbot CandlesBase` une REST+WS pero para OrderBook |

---

## 14. DECISIÓN ARQUITECTÓNICA

### 14.1 DECISIÓN

**OHLCV Recovery debe ser un task separado dentro del ciclo de _ingestion_loop, con storage inyectado en PipelineContext.**

Específicamente:

1. **`_ingestion_loop` ejecuta repair periódicamente** (ej: cada N iteraciones incrementales)
2. **`PipelineContext.storage` recibe `IcebergStorage`** inyectada desde `CompositionRoot`
3. **`ConcretePipelineFactory._build_ohlcv` crea `IcebergStorage`** vía `IcebergStorageFactory`
4. **`RepairStrategy` se invoca como estrategia más** con `mode="repair"`
5. **WS y OrderBook permanecen separados** — no afecta el dominio OHLCV

### 14.2 JUSTIFICACIÓN

**Encaja con OCM**:
- Mantiene Bronze/Silver/Gold tal como existen
- Mantiene Clean Architecture (application no importa infra)
- Mantiene el patrón Kappa (incremental/backfill via Kafka)
- Repair es una estrategia más, no un cambio de paradigma
- `PipelineContext.storage` ya tiene el puerto `OHLCVStorage` definido

**Coincide con modelos de referencia**:
- Hummingbot: `CandlesBase` recibe dependencias y gestiona REST+WS (OCM ya tiene ambas)
- Freqtrade: `DataProvider` encapsula exchange + handlers (OCM tiene `PipelineContext` como equivalente)
- Nautilus: persistence integrada (OCM tiene `IcebergStorageFactory`)

**Respeta Clean Architecture**:
- `OHLCVStorage` es un Protocol en `ports/outbound/storage.py`
- `IcebergStorage` implementa el protocolo
- `PipelineContext` depende del protocolo, no de la implementación
- `ConcretePipelineFactory` es el único lugar que crea `IcebergStorage`

**Mantiene idempotencia**:
- `save_ohlcv` con `unique(keep="last")` ya garantiza idempotencia
- El cursor Redis rastrea el progreso
- `RepairStrategy._heal_gap` filtra al rango del gap y deduplica

**Permite observabilidad**:
- `RepairMetricsPort` ya existe (`PrometheusRepairMetrics`)
- `GapEventPublisherPort` permite eventos de control-plane
- `GapRegistryPort` rastrea gaps conocidos

**Permite recuperación determinista**:
- `scan_gaps` detecta gaps reproduciblemente
- `RepairStrategy` procesa cada gap de forma independiente
- El resultado es determinista dado el mismo estado de Silver

**Evita acoplamiento indebido**:
- `RepairStrategy` no conoce `IcebergStorage` directamente — usa `OHLCVStoragePort`
- `PipelineContext` es el DI container — no conoce Iceberg
- `CompositionRoot` es el único punto de ensamblaje

### 14.3 ALTERNATIVAS DESCARTADAS

| Alternativa | Razón de descarte |
|---|---|
| **Fusionar REST+WS para OHLCV** (Hummingbot-style) | WS no produce OHLCV. Dominios separados. Sin evidencia de necesidad. |
| **Repair como task completamente separado fuera de PipelineOrchestrator** | Rompe el patrón de inyección de dependencias. No usa `PipelineContext`. |
| **Eliminar RepairStrategy y reemplazar con Backfill** | Backfill descarga TODO desde now → origin. Repara SOLO rangos faltantes. Son capacidades distintas. |
| **Eliminar RepairStrategy y usar QualityPipeline para gaps** | QualityPipeline solo detecta, no recupera. Falta el mecanismo de recuperación. |
| **Reconciliation como capa adicional sobre Silver/Gold** | No hay evidencia de inconsistencia entre capas. Repair resuelve el problema a nivel de Silver. |
| **Fusionar RepairStrategy con BackfillStrategy** | Diferentes fuentes de datos (Silver existente vs. desde now), diferente persistencia (directo vs Kafka), diferente scope. |

### 14.4 PUNTOS NO DECIDIDOS

- **Frecuencia de reparación**: ¿Cada iteración de `_ingestion_loop`, cada N iteraciones, o en evento? No hay evidencia de producción que determine el intervalo óptimo.
- **Gestión de errores de repair**: Si `RepairStrategy` falla, ¿el loop continúa o se detiene? El `_ingestion_loop` actual captura excepciones y continúa.
- **Orden de ejecución**: ¿Repair antes o después de incremental? Reparar antes asegura que el incremental trabaje con datos completos. Reparar después es más eficiente si no hay gaps detectados.
- **Si `GapDetectedEvent` se emite**: La auditoría no encontró emisión de `GapDetectedEvent` desde `QualityPipeline`, pero podría existir en `_build_event_bus_wiring()` o `QualityPipelineConsumer`.

---

## 15. PR BOUNDARIES PROPUESTOS

### PR-A: `PipelineContext.storage` plumbing
- **Objetivo**: Inyectar `IcebergStorage` en `PipelineContext` para que `RepairStrategy` funcione
- **Archivos**: `pipeline/runtime.py`, `ohlcv_pipeline.py`, `pipeline_factory.py`
- **Dependencia**: Ninguna externa
- **Riesgo**: Cambio en constructor de `OHLCVPipeline` puede romper tests existentes
- **Tests**: Actualizar tests que construyan `OHLCVPipeline` directamente
- **Por qué separado**: Es el prerequisite para todo lo demás
- **Qué NO incluir**: No incluir lógica de recuperación, solo plumbing

### PR-B: Recovery orchestration en `_ingestion_loop`
- **Objetivo**: Agregar paso de reparación al ciclo de ingesta
- **Archivos**: `main.py`, posiblemente nuevo task asyncio o loop interno
- **Dependencia**: PR-A
- **Riesgo**: Impacto en performance del ciclo de ingesta
- **Tests**: Test de integración que verifique repair se ejecuta
- **Por qué separado**: Afecta el runtime principal, requiere PR-A primero
- **Qué NO incluir**: No incluir cambios en `PipelineContext`, solo lógica de loop

### PR-C: Quality → Recovery event bridge
- **Objetivo**: Si `GapDetectedEvent` se emite, conectarlo con disparo de reparación
- **Archivos**: `quality/pipeline.py`, `quality_consumer.py`, nuevo consumer o callback
- **Dependencia**: PR-B
- **Riesgo**: Complejidad adicional; posible ciclo infinito si repair genera gaps
- **Tests**: Event-driven test
- **Por qué separado**: Es la optimización final, no el foundation
- **Qué NO incluir**: No incluir cambios en `PipelineContext` o `main.py`

### PR-D: Backfill/reconciliation improvements
- **Objetivo**: Mejorar `BackfillStrategy` y añadir mecanismos de reconciliación
- **Archivos**: `strategies/backfill.py`, nuevo módulo de reconciliación
- **Dependencia**: PR-A
- **Riesgo**: Cambio en comportamiento de backfill
- **Tests**: Tests de integración
- **Por qué separado**: Backfill es un mecanismo distinto de repair
- **Qué NO incluyE**: No incluir reparación de gaps

### PR-E: OrderBook resync improvements
- **Objetivo**: Mejorar `BookBuilder` gap recovery con REST snapshot
- **Archivos**: `adapters/inbound/rest/gap_recovery_fetcher.py`, `book_builder.py`
- **Dependencia**: Ninguna (dominio separado)
- **Riesgo**: Afecta OrderBook, no OHLCV
- **Tests**: Tests de BookBuilder
- **Por qué separado**: Dominio completamente distinto
- **Qué NO incluye**: No incluir reparación OHLCV

---

## 16. VERIFICACIÓN FINAL

- **Git status**: Sin cambios en tracked files
- **Git diff**: Sin cambios
- **Archivos modificados**: Ninguno
- **Archivos creados**: `docs/audits/AUDIT_OCM_recovery-architecture-reference-comparison_2026-09-21.md` (este documento)
- **Commit**: NO REALIZADO
- **Push**: NO REALIZADO
- **Merge**: NO REALIZADO
- **Code changes**: NINGUNO
- **Config changes**: NINGUNA
- **Systemd changes**: NINGUNA
- **Test changes**: NINGUNA

---

## 17. STOP CONDITION

```
AUDIT COMPLETE
Implementation: NOT PERFORMED
Tests modified: NO
Configuration modified: NO
Commit: NOT PERFORMED
Push: NOT PERFORMED
Merge: NOT PERFORMED

Audit document: docs/audits/AUDIT_OCM_recovery-architecture-reference-comparison_2026-09-21.md
Previous audits:
  - docs/audits/AUDIT_OCM_ohlcv-repair-backfill-execution-paths_2026-09-21.md
  - docs/audits/AUDIT_OCM_ohlcv-repair-backfill-causality_2026-09-21.md

Detenerse y esperar autorización humana explícita.
```

---

## RESUMEN EJECUTIVO

**¿Qué sabemos ahora?**

1. **OCM tiene las piezas correctas pero la orquestación es insuficiente.** `RepairStrategy`, `BackfillStrategy`, `IncrementalStrategy` existen y funcionan. `QualityPipeline` detecta gaps. Pero nadie conecta la detección con la reparación.

2. **El problema no es `RepairStrategy` sino `PipelineContext.storage`.** Si `PipelineContext` recibiera `IcebergStorage`, `RepairStrategy._read_silver()` funcionaría. El `IcebergStorageFactory` ya existe en `main.py::_lifespan` pero nunca se conecta a `OHLCVPipeline`.

3. **Hummingbot demuestra que REST+WS pueden ser unitarios** (`CandlesBase`), pero OCM no necesita esto para OHLCV ya que WS es para OrderBook (dominio separado).

4. **Freqtrade demuestra que backfill es simplemente descarga histórica a disco.** OCM usa Kafka→Iceberg en su lugar. El concepto es el mismo, la persistencia es diferente.

5. **Nautilus no tiene backfill/repair explícitos** — delega la recuperación a los adapters. OCM tiene recuperación explícita pero mal conectada.

**Decisión arquitectónica**: PR-A (plumbing de `storage`) → PR-B (recovery en `_ingestion_loop`) → PR-C (event bridge opcional). Mantener REST y WS como dominios separados. Inyectar `IcebergStorage` en `PipelineContext`.

**Alternativas descartadas**: Fusionar REST+WS para OHLCV, eliminar RepairStrategy, reemplazar Repair con Backfill, añadir Reconciliation como capa separada.

**Puntos no decididos**: Frecuencia de reparación, orden de ejecución, gestión de errores de repair.

**Detenerse y esperar autorización humana.**
