# AUDITORIA ARQUITECTONICA FORENSE DEFINITIVA — OrangeCashMachine

**Fecha:** 2026-09-06
**Alcance:** Total — todos los patrones arquitectónicos, persistencia de Portfolio, recuperación, fuentes de verdad
**Metodologia:** Evidencia exclusiva del repositorio. Sin suposiciones. Sin cambios de codigo.
**Estado:** CERRADO — diagnostico definitivo

---

## 1. EVENT SOURCING

### 1.1 ¿Event Sourcing esta implementado?

**NO.**

| Componente | Mecanismo real | Evidence |
|---|---|---|
| Posiciones | `RedisPositionStore` usa `SET` (snapshot JSON completo, TTL 7 dias) | `packages/portfolio/infra/redis_store.py` — key `ocm:positions:{exchange}:{order_id}`, valor = JSON con `qty`, `avg_entry`, `unrealised_pnl`, `_index` set |
| Trade history | `TradeTracker._closed` es VOLATILE (dict en memoria, se pierde al restart) | `packages/trading/analytics/trade_tracker.py:58-61` |
| Risk state | `RiskManager._open_positions`, `_daily_pnl_pct`, `_total_pnl_pct`, `_halted` son VOLATILES | `packages/trading/risk/manager.py:94-99` |
| Kafka | Retencion destructiva (`delete 7d/1h/30d/90d`). No es Event Store. | `shared/kafka/topics.py:214-231` |

**Conclusion:** Ningun componente almacena estado como secuencia inmutable de eventos de dominio. No existe log de eventos reconstruible. No existe mecanismo de replay.

### 1.2 ¿Event Sourcing es un requisito de OCM?

**NO.**

Busqueda exhaustiva en todo el repositorio:

| Fuente consultada | Resultado |
|---|---|
| ADR-0027 (accepted) | Define "snapshot + journal + exchange reconciliation". No menciona ES. Linea 186: "TradeStore: tecnologia UNKNOWN por decision explicita". |
| ADR-0006 (accepted) | Portfolio es unico dueño de posiciones. No menciona ES. |
| ADR-0021 (PROPOSED, no aprobado) | Linea 28: "exige un event bus que hoy no existe en trading (100% sincrono)". No requiere ES. |
| ADR-0002 (DEPRECATED) | Linea 63: "proyecciones o indices secundarios reconstruibles desde Kafka". Alcance: solo market_data. Serie deprecada. |
| tracking.yaml | Zero hits para "Event Sourcing", "event store", "rebuild", "replay" (excepto B-11 cerrado). |
| AGENTS.md | No menciona ES como requisito. |
| GOVERNANCE.md | No menciona ES como requisito. |
| architecture_linter/ | Zero hits. |
| PLAN-Maestro-Ingenieria.md | No menciona ES como requisito. |

### 1.3 ¿La ausencia de Event Sourcing constituye una violacion?

**NO.**

| Criterio | Veredicto |
|---|---|
| Algún ADR exige ES? | No |
| Algún contrato BC-NN exige ES? | No |
| ADR-0027 (aceptado) define alternativa? | Si — snapshot + journal + exchange reconciliation |
| ADR-0006 (aceptado) asigna ownership? | Si — portfolio dueño de posiciones |
| La auditoria previa lo declara deficiencia? | No — linea 103 del informe anterior: "La ausencia de Event Sourcing NO es una deficiencia tecnica" |

### 1.4 Clasificacion

```
A. NO IMPLEMENTADO           ✓
B. NO REQUERIDO POR LA       ✓
   ARQUITECTURA ACTUAL
C. DEUDA DOCUMENTADA          ✗ (no aplica — no hay nada que documentar como pendiente)
D. BRECHA REAL                ✗
```

**DECLARACION EXPLICITA:** "Event Sourcing no es un requisito arquitectonico actual de OCM."

---

## 2. MODELO REAL DE PERSISTENCIA DE PORTFOLIO

### 2.1 Diagrama del modelo

```
Portfolio
   │
   ├──► POSITION STATE ──► RedisPositionStore (snapshot JSON, SET, TTL 7d)
   │                           │
   │                           ├──► Journal (NO EXISTE hoy — F6a, tecnologia UNKNOWN)
   │                           │
   │                           └──► Exchange reconciliation (API REST: position/list, closed-pnl)
   │
   ├──► TRADE HISTORY ──► TradeTracker._closed (VOLATILE, en memoria)
   │                           │
   │                           └──► Journal (NO EXISTE — misma deuda F6a)
   │
   └──► RISK STATE ──► RiskManager (VOLATILE, en memoria)
                           │
                           └──► Reconstruido desde PositionStore + journal
```

### 2.2 Auditoria por componente

| Componente | Que almacena | Quien escribe | Quien lee | Es fuente de verdad? | Es snapshot? | Es historico? | Permite recuperacion? | Obligatorio para recuperacion? |
|---|---|---|---|---|---|---|---|---|
| **RedisPositionStore** | Posiciones abiertas: qty, avg_entry, unrealised_pnl, index. Key: `ocm:positions:{exchange}:{order_id}` | `PortfolioService.open_position()`, `PortfolioService.close_position()` | `PortfolioService.snapshot()`, `PortfolioService.get_position()`, `RiskManager` (lectura) | SI — SSOT de posicion (ADR-0006, BC-43) | SI — snapshot completo, no secuencia | NO — solo estado actual | SI — rehidratacion al arranque (ADR-0027 linea 56-58) | SI — unico almacen persistido de posiciones |
| **TradeTracker._closed** | Historial de trades cerrados (TradeRecord[]) | `TradeTracker` internamente | `PerformanceEngine` (analytics) | SI — pero VOLATILE | NO — append-only en memoria | SI — historico | NO — se pierde al restart | NO — es deuda documentada (F6a) |
| **RiskManager** | Estado de riesgo: open_positions, daily_pnl, total_pnl, halted | `RiskManager.validate()` | `TradingEngine` | NO — derivado | NO — estado mutable | NO | NO — se reconstruye desde SSOT | NO — reconstruible desde PositionStore |
| **Journal OCM** | (NO EXISTE) | — | — | — | — | — | — | — |
| **Exchange reconciliation** | Posiciones y fees del exchange (API REST) | Exchange (externo) | `manage_open_orders()` reconciliacion | SI — referencia externa | NO — consulta en vivo | Parcial (closed-pnl) | SI — siempre disponible via API | SI — como referencia de verificacion, no como SSOT primario |

### 2.3 Flujo de recovery (ADR-0027, lineas 94-97)

```
restart
  → PortfolioService rehidrata desde RedisPositionStore
  → OMS/Risk reconstruyen (open_count desde store; drawdown desde journal si existiera)
  → Reconciliacion con position/list (size y avgPrice dentro de tolerancia; discrepancia → alerta)
  → Fees UNKNOWN→FINAL via closed-pnl
```

---

## 3. F6a / TRADESTORE

### 3.1 Que significa F6a

F6a es la etiqueta de tracking para la deuda pendiente: **el journal de trades cerrados no tiene tecnologia de persistencia definida**. ADR-0027 (lineas 60-64) establece:

> "Source of truth: journal OCM persistido de trades cerrados (append-only). Persistence: UNKNOWN — no se decide aqui la tecnologia (no Redis, no Iceberg, no Kafka, ni otra, en este ADR). La eleccion es decision de implementacion posterior (F6a). Hoy `TradeTracker._closed` es VOLATILE."

### 3.2 Que componente falta

- **TradeStore**: un almacén persistente para `TradeRecord[]` (historial de trades cerrados).
- Hoy `TradeTracker._closed` es un dict en memoria (`packages/trading/analytics/trade_tracker.py:58-61`).

### 3.3 Que capacidad falta

- Persistir trades cerrados mas alla del restart del proceso.
- Recuperar historial de trading para analytics (PerformanceSummary).
- Base para realized P&L historico (hoy se recomputa pero no persiste).

### 3.4 Donde esta documentada la deuda

- ADR-0027, lineas 60-64 (definicion) y linea 117-118 ("el journal de trades aun no existe (F6a) y su tecnologia queda UNKNOWN a proposito").
- `docs/plans/tracking.yaml` — F6a registrado.

### 3.5 Que ADR la acepta

- **ADR-0027** (accepted, 2026-08-14) — explicitamente declara la deuda y la clasifica como "UNKNOWN a proposito".

### 3.6 Afecta actualmente a la correccion arquitectonica?

**NO.** La clasificacion arquitectonica de los patrones no depende de F6a. El modelo de persistencia de posiciones (Redis snapshot) funciona correctamente para su proposito. F6a es un gap de funcionalidad (historial no persistido), no un gap arquitectonico (el diseño ya esta definido en ADR-0027).

### 3.7 Existe workaround/modelo alternativo aceptado?

**SI.** ADR-0027 establece que la recuperacion de P&L realized se hace via:
- Fills del exchange (`closed-pnl` API) para fees UNKNOWN→FINAL.
- Reconciliacion con `position/list` para estado actual.
- El journal es complementario, no obligatorio para la operacion basica.

### 3.8 Clasificacion

```
F6a = DEUDA ACEPTADA
```

No es BRECHA (el diseno esta definido y aceptado). No es IMPLEMENTADO (la tecnologia no se ha elegido). No es UNKNOWN (la existencia de la deuda esta documentada explicitamente).

---

## 4. KAFKA

### 4.1 Papel real de Kafka en OCM

| Rol | Aplica? | Evidencia |
|---|---|---|
| **Transport** | SI | Transporta OHLCV, orderbook, trades, metrics entre procesos |
| **Event Bus** | PARCIAL | Para market_data funciona como event bus. Para trading/portfolio, los topics estan definidos pero son ORPHAN (schemas con wire_status "wired" pero sin productor real en codigo — provenance.py lineas 54-60) |
| **Streaming** | SI | Consumidores en tiempo real (BronzeWriter, QualityGate, FeatureEngine) |
| **Durable Log** | NO | Retencion destructiva: `delete 7d/1h/30d/90d` (topics.py lineas 214-231). No es append-only indefinido. |
| **Event Store** | NO | Retencion destructiva invalida como Event Store. No permite replay completo desde genesis. No permite reconstruir estado historico. |

### 4.2 ¿Puede reconstruirse el estado de Portfolio desde Kafka?

**NO.**

Razones:
1. Los topics de trading (`signals.raw`, `orders.filled`, `positions.opened`, `positions.closed`) estan **definidos** en `topics.py` y tienen **schemas registrados** en `shared/kafka/schemas/`, pero son **ORPHAN** segun `provenance.py` (lineas 54-60: wire_status = "wired" pero son DOMAIN payloads sin productor real cableado en codigo).
2. El trading engine es **sincrono** (`TradingEngine.run_once()` en `packages/trading/engine.py:143`), no emite a Kafka.
3. Incluso si existieran productores, la retencion destructiva limitaria la ventana de reconstruccion.

### 4.3 Mecanismo de recuperacion definido por ADR-0027

```
RedisPositionStore (snapshot) + Exchange API (reconciliacion)
```

Kafka no participa en el mecanismo de recuperacion de Portfolio.

---

## 5. EVENT-DRIVEN

### 5.1 Evidencia

| Componente | Event-Driven? | Evidencia |
|---|---|---|
| **Market Data pipeline** | SI | Producers: REST/WS fetchers → Kafka. Consumers: BronzeWriter, QualityGate, FeatureEngine, StrategyConsumer. 17 domain events definidos en `packages/market_data/domain/events/`. |
| **Trading engine** | NO | Sincrono: `TradingEngine.run_once()` (`engine.py:143`). Sin async, sin Kafka, sin event bus. Ejecuta polling loop. |
| **Portfolio** | NO | `PortfolioService` es un servicio sincrono. Opera via `PositionStore` (Redis). Sin emision de eventos. |
| **Integration events** | PARCIAL | Topics definidos (`signals.raw`, `orders.filled`, etc.) con schemas y provenance registrados. Pero productores NO cableados en codigo (orphan segun provenance.py). |

### 5.2 Distinguicion critica

**Event-Driven Architecture ≠ Event Sourcing.**

- OCM es **parcialmente Event-Driven** en el bounded context `market_data`.
- OCM **NO** es Event-Driven en `trading` ni `portfolio` (sincronos).
- OCM **NO** es Event Sourced en ningun bounded context.

### 5.3 Veredicto

```
Market Data:  Event-Driven (funcional, con Kafka como backbone)
Trading:      Sincrono (polling loop, sin eventos)
Portfolio:    Sincrono (servicio directo, sin eventos)
```

---

## 6. CQRS

### 6.1 Busqueda de evidencia

| Patron CQRS | Evidencia encontrada |
|---|---|
| CommandHandler | Zero hits en todo el repositorio |
| QueryHandler | Zero hits |
| Command model / Query model | Zero hits |
| WriteModel / ReadModel | Zero hits |
| Separacion explicita command/query | No existe |

### 6.2 Separacion convencional

Existe separacion natural de lectura/escritura:
- **Escritura:** `PortfolioService.open_position()`, `PortfolioService.close_position()`
- **Lectura:** `PortfolioService.snapshot()`, `PortfolioService.get_position()`

Pero esto es un **servicio normal con metodos de lectura y escritura**, no CQRS. CQRS requiere modelos de comando y query separados, handlers separados, y usualmente stores separados. Ninguno de estos existe.

### 6.3 Veredicto

```
CQRS: NO IMPLEMENTADO (ni completo, ni parcial)
```

---

## 7. HEXAGONAL

### 7.1 Ports

**48 Protocol interfaces** documentadas en `ports/`:

- `packages/market_data/ports/outbound/`: 33 Protocols (OHLCVStorage, KafkaConsumerPort, EventBusPort, FeatureReaderPort, etc.)
- `packages/market_data/ports/inbound/`: 6 Protocols (MarketDataSource, PipelineFactoryPort, ReplayPort, etc.)
- `packages/portfolio/ports/`: 1 Protocol (PositionStore)
- `packages/trading/execution/`: 5 Protocols (OrderExecutor, OrderTransport, SupportsOnFill, etc.)
- `shared/contracts/`: 4 Protocols (FeatureSource, SignalProtocol, RiskGate, RebalancePort)

### 7.2 Adapters

Adapters implementan los Ports:
- `packages/market_data/adapters/outbound/storage/gold_reader.py` → FeatureReaderPort
- `packages/market_data/adapters/outbound/exchange/ccxt_adapter.py` → ExchangeClientPort
- `packages/portfolio/infra/redis_store.py` → PositionStore
- `packages/trading/bootstrap/composition_root.py:213-298` → _BybitTransport implementa OrderTransport

### 7.3 Dependency Inversion

**Cumplido.** Domain define Ports (Protocols). Adapters implementan Ports. Domain nunca importa Adapters. Evidence:
- `packages/portfolio/models/position.py` — zero infrastructure imports
- `packages/market_data/domain/` — zero infrastructure imports (verificado en auditoria previa, BC-09)

### 7.4 Composition Roots

**4 Composition Roots** implementados:

| Bounded Context | File | Que cablea |
|---|---|---|
| market_data | `packages/market_data/infrastructure/bootstrap/composition_root.py` (398 lineas) | PipelineFactory, FeedOrchestrator, WS producers, External ingestion |
| trading | `packages/trading/bootstrap/composition_root.py` (642 lineas) | TradingEngine, TradeTracker, RiskManager, LiveExecutor, OMS, Transport |
| portfolio | `packages/portfolio/bootstrap/composition_root.py` (209 lineas) | PortfolioService, RebalanceService, PositionStore (Redis/InMemory) |
| research | `apps/research/data/composition_root.py` (73 lineas) | IcebergStorageFactory, GoldReader |

### 7.5 Bounded Context boundaries

**50 contratos import-linter**, todos verificados:

```
Contracts: 50 kept, 0 broken.
```

Ejecutado con: `uv run lint-imports --config architecture_linter/importlinter.toml`

### 7.6 AST guards

`architecture_linter` detecta violaciones por AST en 10 invariantes (ARCH-001..010). Ejecutado en auditoria previa con findings registrados (ARCH-001: 6 owners mutables de posicion, ARCH-004, ARCH-007, ARCH-010).

### 7.7 Veredicto

```
Hexagonal: IMPLEMENTADO y FUNCIONAL
- 48 Ports (Protocols)
- Adapters correctamente separados
- DIP cumplido
- 4 Composition Roots
- 50 contratos import-linter: 50 KEPT, 0 broken
```

---

## 8. DDD

### 8.1 Bounded Contexts

| Bounded Context | Path | Sub-layers |
|---|---|---|
| market_data | `packages/market_data/` | domain/, ports/ (inbound+outbound), application/, adapters/, infrastructure/ |
| trading | `packages/trading/` | execution/, strategies/, risk/, analytics/, bootstrap/ |
| portfolio | `packages/portfolio/` | models/, ports/, services/, infra/, bootstrap/ |
| shared | `shared/` | types, contracts, exceptions, kafka schemas, utils |
| ocm | `ocm/` | config, runtime, observability (plataforma, sin logica de negocio) |

### 8.2 Entities

| Entity | File | Identity |
|---|---|---|
| `DataTier` (enum) | `market_data/domain/entities/__init__.py:44` | Clasificacion, no entidad con identity |
| `Order` | `trading/execution/order.py:98` | `order_id` (UUID) — mutable, stateful |
| `OMS` | `trading/execution/oms.py:145` | Maneja orders con identity |
| `RiskManager` | `trading/risk/manager.py:102` | Estado mutable de riesgo |
| `TradeTracker` | `trading/analytics/trade_tracker.py:45` | Tracks fills por symbol |

**Total: 1 entity formal (enum) + 4 entity-like stateful classes = 5**

### 8.3 Value Objects

**~35 value objects** (frozen dataclasses):

- `market_data/domain/value_objects/`: Symbol, Candle, OHLCVChunk, NormalizedTrade, RawTrade, TradeSeries, OrderBookSnapshot, OrderBookDelta, GapRange, QualityLabel, ValidationResult, ExchangeQuirks, PriceLevel (~18)
- `portfolio/models/`: PositionSnapshot, PortfolioState, RebalanceSignal (3)
- `trading/execution/`: OrderState, OrderResult, Settlement, FeeStatus (4)
- `trading/risk/`: RiskDecision (1)
- `trading/analytics/`: TradeRecord, PerformanceSummary (2)
- `market_data/domain/events/`: DomainEvent, LineageEvent, + 15 domain events (~17)

### 8.4 Aggregates

**No existen Aggregate Roots formales** (sin `AggregateRoot` base class). El codigo usa composicion:
- `TradeSeries` se describe como "Aggregate root of microstructure" en su docstring (`trade_series.py:50`)
- `PositionSnapshot`/`PortfolioState` funcionan como snapshot de aggregate pero sin root formal

### 8.5 Domain Services

| Service | File | Descripcion |
|---|---|---|
| `PerformanceEngine` | `trading/analytics/performance.py:94` | Metricas de trading (estatico/puro) |
| `CandleValidator` | `market_data/domain/value_objects/candle_validator.py:157` | Validacion de candles (stateless) |
| `RiskManager` | `trading/risk/manager.py:102` | Validacion de signals contra limites (stateful) |
| `QualityPipeline` | `market_data/application/quality/pipeline.py:120` | Orquestacion de quality checks |

### 8.6 Domain Events

**17 domain events** en `packages/market_data/domain/events/`:
- `DomainEvent` (base, frozen dataclass)
- `CandleReceived`, `OHLCVBatchReceived`, `OHLCVBatchIngested`
- `QualityCheckPassed`, `QualityCheckFailed`
- `SignalGenerated`
- `GapDetectedEvent`, `GapHealedEvent`, `GapFailedEvent`
- `OrderBookSnapshotReceived`, `OrderBookDeltaReceived`
- `ReplayRequested`, `ReplayCompleted`
- `TradeReceived`
- `ExternalMetricEvent`
- `LineageEvent`

**Nota:** Todos los domain events son de `market_data`. No existen domain events en `trading` ni `portfolio`.

### 8.7 Repositories

**48 Protocol interfaces** funcionando como repositories/ports (ver seccion 7.1).

### 8.8 Application Services

| Service | File |
|---|---|
| `PipelineOrchestrator` | `market_data/application/use_cases/pipeline_orchestrator.py:173` |
| `OHLCVTransformer` | `market_data/application/use_cases/ohlcv_transformer.py:53` |
| `CandleNormalizer` | `market_data/application/use_cases/candle_normalizer.py:64` |
| `ResampleUseCase` | `market_data/application/use_cases/resample_ohlcv.py:149` |
| `PortfolioService` | `portfolio/services/portfolio_service.py:50` |
| `RebalanceService` | `portfolio/services/rebalance_service.py:95` |

### 8.9 Veredicto

```
DDD: PARCIALMENTE IMPLEMENTADO

Implementado:
  ✓ Bounded Contexts (3 + shared + ocm)
  ✓ Value Objects (~35 frozen dataclasses)
  ✓ Domain Events (17, solo en market_data)
  ✓ Ports/Repositories (48 Protocols)
  ✓ Application Services (6)
  ✓ Domain Services (4)
  ✓ Dependency Inversion

No implementado / Parcial:
  ✗ Aggregate Roots formales (sin base class)
  ✗ Entities formales (solo 1 enum + 4 entity-like)
  ✗ Domain Events en trading/portfolio (no existen)
  ✗ Ubiquitous Language no formalizada
```

---

## 9. MEDALLION / LAKEHOUSE

### 9.1 Bronze

| Componente | Estado | Evidencia |
|---|---|---|
| **Tablas** | 3 tablas Iceberg: `bronze.ohlcv`, `bronze.orderbook_snapshot`, `bronze.orderbook_delta` | `infrastructure/storage/iceberg/bootstrap.py:117-149` |
| **Writers** | `KafkaBronzeWriter` (OHLCV), `OrderbookBronzeWriter` (orderbook) | `infrastructure/kafka/bronze_writer.py`, `orderbook_bronze_writer.py` |
| **Transformacion** | Append-only, sin dedup, agrega `ingestion_ts` + partition columns | `infrastructure/storage/bronze/bronze_storage.py:162-242` |
| **Retention** | `expire_snapshots()` con cutoff configurable (default 7d) | `infrastructure/storage/bronze/bronze_retention.py` |
| **Operacional** | SI — pipeline funcional con Kafka → Bronze | Supervisor registra bronze_writer como managed task |

### 9.2 Silver

| Componente | Estado | Evidencia |
|---|---|---|
| **Tablas** | 3 tablas Iceberg: `silver.ohlcv`, `silver.trades`, `silver.derivatives` | `bootstrap.py:122-139` |
| **Writers** | `IcebergStorage` (OHLCV), `TradesStorage` (trades), `DerivativesStorage` (funding/OI) | `infrastructure/storage/iceberg/iceberg_storage.py`, `silver/trades_storage.py`, `silver/derivatives_storage.py` |
| **Transformaciones** | Timestamp normalization, dedup (por timestamp+exchange+symbol+timeframe), partition injection, quality classification, gap detection | `ports/outbound/normalization.py`, `application/quality/pipeline.py` |
| **Operacional** | SI — quality pipeline funcional con lineage recording | QualityPipeline registra LineageEvent con SILVER layer |

### 9.3 Gold

| Componente | Estado | Evidencia |
|---|---|---|
| **Tabla** | 1 tabla Iceberg: `gold.features` | `bootstrap.py:127-129` |
| **Writer** | `GoldStorage` con atomic overwrite per dataset | `infrastructure/storage/gold/gold_storage.py:87-430` |
| **Transformer** | `GoldTransformer` — 9 features tecnicas (log_return, return_1, volatility_20, vwap, high_low_spread, volume_z, price_range_pct, body_pct, is_suspect) | `infrastructure/storage/gold/transformer.py:90-281` |
| **Reader** | `GoldReader` con time-travel, list_versions, get_manifest | `adapters/outbound/storage/gold_reader.py:75-352` |
| **Operacional** | SI — features consumidas por TradingEngine via FeatureSource | `packages/trading/bootstrap/composition_root.py:146-204` (_GoldFeatureSource adapter) |

### 9.4 Iceberg

| Aspecto | Estado |
|---|---|
| Catalogo | `SqlCatalog` (SQLite), singleton thread-safe, nombre "ocm" |
| Namespaces | `bronze`, `silver`, `gold` |
| Total tablas | 7 |
| Schemas | Definidos en `infrastructure/storage/iceberg/schemas.py` (7 schemas, 149 lineas) |
| Partitions | Definidos en `infrastructure/storage/iceberg/partitions.py` (3 partition specs) |
| Time-travel | Soportado via Iceberg snapshots + `GoldReader.load_features(version=..., as_of=...)` |

### 9.5 Polars

**Motor principal de DataFrames.** Migracion pandas → polars completada (PR #19, 2026-08-23).

- Domain: 100% framework-agnostic (zero pandas/polars imports)
- Application/infrastructure: Polars nativo para transformaciones
- Boundary: `OHLCVTransformer` hace `pl.from_pandas()` on entry, `.to_pandas()` on exit

### 9.6 Lineage

| Capa | Lineage tracking |
|---|---|
| Silver | `QualityPipeline` registra `LineageEvent` (PipelineLayer.SILVER) con rows_in, rows_out, status, quality_score |
| Gold | `GoldTransformer` registra `LineageEvent` (PipelineLayer.GOLD) |
| Iceberg properties | `GoldStorage.build()` escribe `ocm.exchange`, `ocm.symbol`, `ocm.market_type`, `ocm.timeframe`, `ocm.run_id` como snapshot properties |
| Gold columns | `run_id`, `engineer_version`, `silver_snapshot_id`, `silver_snapshot_ms` embebidos en cada fila |
| Platform | `ocm.runtime.lineage` — `LineageRecord` con `git_hash` auto-capturado |
| Storage | SQLite append-only en `data/lineage/lineage.db` |

### 9.7 Veredicto

```
Medallion/Lakehouse: IMPLEMENTADO y OPERACIONAL

Bronze:  3 tablas Iceberg, 2 writers, append-only, retention    ✓
Silver:  3 tablas Iceberg, 3 writers, dedup + quality + lineage ✓
Gold:    1 tabla Iceberg, 9 features, time-travel, reader       ✓
Iceberg: 7 tablas, catalogo singleton, 3 partition specs        ✓
Polars:  Motor principal, domain agnostic                        ✓
Lineage: Custom SQLite + Iceberg properties + Gold columns       ✓
```

---

## 10. FEATURE STORE

### 10.1 Evidencia

| Criterio Feature Store | Estado | Evidencia |
|---|---|---|
| Feature definitions | SI | 9 features tecnicas en `GoldTransformer` (transformer.py:90-281) |
| Feature computation | SI | `GoldStorage.build()` computa y persiste en `gold.features` |
| Feature versioning | SI | Iceberg snapshot-based + `engineer_version` column ("3.0.0") + `list_versions()` |
| Point-in-time correctness | SI | `silver_snapshot_id` anchoring + Iceberg time travel (`as_of` parameter) |
| Online serving | NO | Solo offline via Iceberg/GoldReader. Sin Redis/DynamoDB online serving |
| Offline serving | SI | `GoldReader.load_features()` con version y as_of |
| Training/serving consistency | SI | GoldTransformer deterministico y stateless. Mismo input → mismo output |
| Feature registry | NO | Sin registry formal de features. Features definidas como constantes en `FEATURE_COLUMNS` |

### 10.2 No confundir

- **Bronze/Silver/Gold ≠ Feature Store.** Bronze y Silver son capas de ingesta/refinamiento. Gold es la capa que produce features. Solo `gold.features` es Feature Store.
- **Gold.features es Offline Feature Store unicamente.** No existe online serving.

### 10.3 Veredicto

```
Feature Store: PARCIALMENTE IMPLEMENTADO

Implementado:
  ✓ Feature definitions (9 features)
  ✓ Feature computation (GoldTransformer)
  ✓ Feature versioning (Iceberg snapshots)
  ✓ Point-in-time correctness (snapshot anchoring + time travel)
  ✓ Offline serving (GoldReader)

No implementado:
  ✗ Online serving (sin Redis/DynamoDB para features)
  ✗ Feature registry formal
  ✗ Training/serving split (no existe pipeline de entrenamiento separado)
```

---

## 11. FUENTES DE VERDAD

| Contexto | Fuente de verdad | Persistencia | Recuperacion |
|---|---|---|---|
| **Market Data** | Iceberg (Bronze→Silver→Gold) | Iceberg tables (append/overwrite) | Re-fetch desde exchange + reprocesamiento |
| **Orders** | Exchange (externo) + OMS (local) | OMS es VOLATILE en memoria (`oms.py:126-130`). Exchange es referencia via `fetch_open_orders` | Exchange API: `fetch_open_orders`, `fetch_order` |
| **Positions** | Portfolio (`PositionStore`, BC-43) | RedisPositionStore (snapshot JSON, TTL 7d) | Rehidratacion desde Redis + reconciliacion con `position/list` |
| **Portfolio state** | `PortfolioService` + `PositionStore` | derivado de PositionStore + capital_usd | Rehidratacion desde Redis + exchange |
| **Historical Market Data** | Iceberg (`silver.ohlcv`) | Iceberg tables | Re-fetch desde exchange |
| **Features** | Gold (`gold.features`) | Iceberg (overwrite per dataset) | Recomputacion desde Silver via `GoldTransformer` |
| **Trade History** | TradeTracker (VOLATILE) | Ninguna persistencia (F6a) | **NO recuperable** — deuda aceptada |
| **Risk State** | RiskManager (VOLATILE) | Ninguna persistencia | Reconstruible desde PositionStore + journal (si existiera) |
| **Balance** | Exchange (externo) | Ninguna persistencia (ADR-0030 no implementado) | Exchange API: `wallet-balance` |

### 11.1 Portfolio / Position State — Relacion Redis ↔ Journal ↔ Exchange

```
Redis (snapshot)          Journal (F6a)           Exchange (reconciliacion)
─────────────────         ──────────────          ─────────────────────────
SSOT de posiciones        NO EXISTE hoy           Referencia externa
Persistencia: SET         Pendiente: UNKNOWN      Siempre disponible via API
TTL: 7 dias               Sin persistencia        Rate limit: 50/s
Snapshot completo         Volatile                position/list + closed-pnl
Recuperacion: SI          Recuperacion: NO        Recuperacion: SI

Flujo de recovery:
1. Rehidratar desde Redis (si existe)
2. Reconciliar con exchange (position/list)
3. Discrepancia → alerta, no auto-corregir
```

---

## 12. RECUPERACION

### 12.1 ¿Que ocurre si se pierde Redis?

| Estado | Recuperable? | Desde donde? |
|---|---|---|
| Posiciones abiertas | PARCIALMENTE | Exchange API: `position/list` (size, avgPrice, unrealisedPnl) |
| Qty + avg_entry | SI | Exchange API: `position/list` |
| Unrealized P&L | SI | Derivado: `qty × (mark − avg_entry)` |
| Trade history | NO | F6a — journal no implementado |
| Realized P&L historico | NO | F6a — journal no implementado |
| Risk state (drawdown) | NO | Volatile, sin persistencia |
| Capital configurado | SI | Config YAML |

**Consecuencia:** Posiciones se pierden del lado OCM pero son recuperables desde el exchange. El historial de trades realizados se pierde completamente.

### 12.2 ¿Que ocurre si se pierde Kafka?

| Estado | Impacto |
|---|---|
| Market data en curso | Se detiene la ingesta. Bronze/Silver/Gold dejan de actualizarse |
| Posiciones | Sin impacto (Redis) |
| Trading signals | Sin impacto (trading es sincrono, no usa Kafka) |
| Portfolio state | Sin impacto (Redis) |

**Consecuencia:** Kafka es critico para market data pero no para portfolio/trading. La perdida detiene el pipeline de datos pero no el trading existente.

### 12.3 ¿Que ocurre si se pierde Bronze?

| Estado | Recuperable? | Desde donde? |
|---|---|---|
| OHLCV raw | SI | Re-fetch desde exchange + reprocesamiento via BronzeWriter |
| Orderbook | SI | Re-fetch desde WS + reprocesamiento |
| Silver/Gold | Depende | Si Silver sobrevive, Gold se recomputa. Si Silver tambien se pierde, todo se re-fetch |

### 12.4 ¿Que ocurre si se pierde Silver?

| Estado | Recuperable? | Desde donde? |
|---|---|---|
| OHLCV validado | SI | Reprocesamiento desde Bronze via QualityPipeline |
| Trades | SI | Reprocesamiento desde Bronze |
| Gold | SI | Recomputacion desde Silver via GoldTransformer |

### 12.5 ¿Que ocurre si se pierde Gold?

| Estado | Recuperable? | Desde donde? |
|---|---|---|
| Features | SI | Recomputacion desde Silver via GoldTransformer (deterministico) |

### 12.6 ¿Que ocurre si el Exchange REST esta disponible?

- Posiciones: reconstruibles via `position/list`
- Fees: reconciliables via `closed-pnl`
- Orders abiertas: reconstruibles via `fetch_open_orders`
- Balance: reconstruible via `wallet-balance`

### 12.7 ¿Que ocurre si el Exchange REST esta indisponible?

| Estado | Consecuencia |
|---|---|
| Posiciones OCM | Se mantienen en Redis (si sobrevive) |
| Sincronizacion | Imposible — no hay reconciliacion |
| Trading | Deberia detenerse (gate de arranque: sin exchange = sin operar) |
| Recovery | Limitado al ultimo snapshot Redis conocido |

---

## 13. CLASIFICACION FINAL

| Patron/Concepto | Estado | Evidencia | ¿Es deuda? |
|---|---|---|---|
| **DDD** | 🟡 PARCIAL | BCs definidos, ~35 VOs, 17 domain events, 48 Ports. Sin Aggregate Roots formales, sin entities formales, sin domain events en trading/portfolio | Parcialmente — Aggregate Roots y domain events en trading/portfolio |
| **Event-Driven** | 🟡 PARCIAL | market_data es EDA con Kafka. trading/portfolio son sincronos. Integration events definidos pero orphan | Parcialmente — trading/portfolio no son EDA |
| **CQRS** | 🔴 NO IMPLEMENTADO | Zero evidence de CommandHandler, QueryHandler, WriteModel, ReadModel | No — es ausencia total, no deuda |
| **Event Sourcing** | 🔴 NO IMPLEMENTADO | Redis usa SET (snapshot). Sin log de eventos. Sin replay. **No requerido por la arquitectura actual.** | No — decision de diseno valida |
| **Hexagonal** | 🟢 IMPLEMENTADO | 48 Ports, Adapters separados, DIP cumplido, 4 Composition Roots, 50 contratos import-linter (50 KEPT, 0 broken) | No |
| **Ports & Adapters** | 🟢 IMPLEMENTADO | Ver Hexagonal. 48 Protocol interfaces. Adapters en infrastructure/ y adapters/ | No |
| **Bounded Contexts** | 🟢 IMPLEMENTADO | 3 BCs (market_data, trading, portfolio) + shared + ocm. Contratos BC-NN en importlinter.toml | No |
| **Domain Events** | 🟡 PARCIAL | 17 events en market_data. Zero en trading/portfolio | Parcialmente — trading/portfolio sin domain events |
| **Integration Events** | 🟡 PARCIAL | Schemas definidos (SignalPayload, OrderFilledPayload, etc.). Wire status: "wired" en provenance. Pero productores NO cableados en codigo (orphan) | Si — productores no implementados |
| **Kafka/Event Bus** | 🟡 PARCIAL | Funcional para market_data. Topics de trading/portfolio definidos pero orphan. Retencion destructiva (no es Event Store) | Parcialmente — trading/portfolio topics sin cablear |
| **Medallion** | 🟢 IMPLEMENTADO | Bronze (3 tablas), Silver (3 tablas), Gold (1 tabla). Todos operacionales | No |
| **Lakehouse** | 🟢 IMPLEMENTADO | Iceberg (7 tablas), Polars, lineage, time-travel, partitioning | No |
| **Feature Store** | 🟡 PARCIAL | Offline Feature Store (Gold). Sin online serving, sin feature registry formal | Parcialmente — online serving no implementado |
| **Portfolio State** | 🟢 FUNCIONAL | Redis snapshot (SSOT posiciones). Exchange reconciliation (referencia externa). Recovery model definido (ADR-0027) | No |
| **Recovery Model** | 🟢 DEFINIDO | ADR-0027: snapshot + journal + exchange reconciliation. Funcional para posiciones. Pendiente para trade history (F6a) | F6a es deuda aceptada |
| **TradeStore/F6a** | 🟡 DEUDA ACEPTADA | Journal de trades NO persistido. Tecnologia UNKNOWN por decision explicita (ADR-0027 linea 62-64) | Si — deuda aceptada y documentada |

---

## 14. SEPARAR PROBLEMAS DE DECISIONES

### A. PROBLEMAS REALES CONFIRMADOS

1. **Risk state es VOLATILE** — se pierde al restart. No hay persistencia de drawdown ni de daily P&L. (ADR-0027 lo documenta como pendiente de reconstruccion)
2. **Trade history es VOLATILE** — `TradeTracker._closed` se pierde al restart. (F6a, deuda aceptada)
3. **OMS state es VOLATILE** — `OMS._open`, `OMS._entry_prices` se pierden al restart. Reconciliacion via exchange existe pero no hay rehidratacion local. (ADR-0027 linea 12-14)
4. **Integration events orphan** — schemas de trading/portfolio definidos en Kafka pero sin productores cableados en codigo. (provenance.py: wire_status "orphan" para TradePayload, TradeSeriesPayload, FundingRatePayload, OpenInterestPayload, LiquidationPayload)
5. **Multi-ownership mutable de posicion** — tres dueños del estado de posicion (fill_sync, TradeTracker, PortfolioService). Mitigacion parcial implementada (log crítico). ADR-0021 propone unificacion pero NO esta aprobado. (B-15 en tracking)
6. **Balance no se conoce** — `fetch_balance` no existe en el repo. RiskManager usa `capital_usd` configurado. (ADR-0030 aceptado pero NO implementado)
7. **Trading engine es sincrono** — no emite eventos a Kafka. Los topics de trading estan definidos pero son orphan.

### B. DEUDAS TECNICAS ACEPTADAS/DOCUMENTADAS

1. **F6a — TradeStore** — Tecnologia UNKNOWN por decision explicita. ADR-0027 linea 62-64, 117-118. Trading engine sincrono sin persistencia de historial.
2. **ADR-0021 — Unificacion de ownership** — Propuesto pero NO aprobado. Multi-ownership mutable mitigate parcialmente con log critico.
3. **ADR-0030 — Balance real** — Aceptado pero NO implementado. Sizing contra capital configurado, no real. Bloqueante P1 para LIVE.
4. **ADR-0029 — Cancelacion real** — Aceptado pero NO implementado. Gestion de ordenes abiertas pendiente.
5. **CLIs legados** — `portfolio_service=None` opcional en ADR-0006 pendiente de eliminar.
6. **EventBusPort rename** — ADR-0002 documento rename pendiente a `LocalDomainEventDispatcher`. No prioritario.
7. **Tradestore orphan schemas** — `TradePayload`, `TradeSeriesPayload` registrados en provenance como ASSUMED/orphan.

### C. DECISIONES ARQUITECTONICAS VALIDAS

1. **Snapshot + journal + exchange reconciliation** (ADR-0027) — diseno aceptado y funcional para posiciones. Exchange como referencia externa, no como SSOT del estado interno.
2. **Portfolio como unico dueño de posiciones** (ADR-0006) — DIP, BC-43, PositionStore como Protocol. Implementado y verificado.
3. **Hexagonal architecture** — 48 Ports, 4 Composition Roots, 50 contratos import-linter. Cumplido y funcional.
4. **Medallion/Lakehouse** — Bronze→Silver→Gold con Iceberg, Polars, lineage. Implementado y operacional.
5. **Kafka como transport para market_data** — no como Event Store. Retencion destructiva es intencional, no un defecto.
6. **Domain agnostic en market_data** — zero framework imports en domain/. BC-09 enforce por import-linter.
7. **Composition Root por bounded context** — ADR-0003 (trading), portfolio root, market_data root. Patron correcto.
8. **Feature Store offline** — Gold con versioning, time-travel, determinismo. Diseno solido para consumo batch.
9. **Lineage tracking** — Custom SQLite + Iceberg properties + Gold columns. Multiples capas de trazabilidad.

---

## 15. RESULTADO EJECUTIVO

### Como esta arquitectonicamente OCM hoy?

1. **Que arquitectura tiene:** Clean/Hexagonal con bounded contexts (market_data, trading, portfolio), Medallion Lakehouse (Iceberg), Kafka como transport para market_data, y Composition Roots por BC. 50 contratos import-linter verificados.

2. **Que esta funcionando:** Market data pipeline completo (REST/WS → Kafka → Bronze → Silver → Gold → Features). Portfolio con posiciones en Redis. Trading engine sincrono con paper/live. Reconciliacion basica con exchange.

3. **Que esta parcialmente implementado:** Event-Driven (solo market_data). DDD (sin Aggregate Roots formales, sin domain events en trading/portfolio). Feature Store (solo offline). Integration events (schemas definidos, productores orphan).

4. **Que NO esta implementado:** CQRS. Event Sourcing (y no es necesario). Trading event-driven (engine sincrono). Online feature serving. Balance real en portfolio (ADR-0030 aceptado, no implementado).

5. **Que NO es necesario implementar:** Event Sourcing (ADR-0027 define alternativa valida). CQRS (no hay necesidad de separacion command/query formal). Trading con event bus (sincrono es suficiente para el modelo actual).

6. **Que deudas estan aceptadas:** F6a (TradeStore/journal), ADR-0021 (unificacion ownership), ADR-0030 (balance real), ADR-0029 (cancelacion real), CLIs legados, EventBusPort rename.

7. **Que brechas reales existen:** Risk/trade/oms state volatile (se pierde al restart). Multi-ownership mutable de posicion (mitigado parcialmente). Sizing contra capital configurado, no real. Trading engine no emite eventos (topics orphan).

8. **Que incertidumbres quedan:** Cuándo se implementara F6a (TradeStore). Cuándo se aprobara ADR-0021. Cuándo se implementara ADR-0030 (balance real). Si trading eventualmente migrara a event-driven o seguira sincrono. Si los topics orphan se cablearan o se eliminaran.

---

**Fecha de cierre:** 2026-09-06
**Metodologia:** Evidencia exclusiva del repositorio. Sin cambios de codigo. Sin commits. Sin modifications.
**Clasificacion general:** OCM tiene una arquitectura **solida y bien fundamentada** en hexagonal/medallion. Las deudas documentadas son conocidas y aceptadas. No existen brechas arquitectonicas criticas que bloqueen la operacion actual. Las decisiones de diseno (sin ES, sin CQRS, snapshot-based persistence) son validas y coherentes con el dominio de trading algoritmico mono-agente.
