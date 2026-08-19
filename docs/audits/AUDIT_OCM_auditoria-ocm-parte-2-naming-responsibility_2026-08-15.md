# Auditoría OCM — Parte 2/4

## Naming + Responsibility / State Ownership

**Fecha:** 2026-08-15
**Alcance:** FASE 5 (Naming Audit) + FASE 6 (Responsibility / State Ownership Audit)
**Contraste arquitectónico:** ADR-0029 (cancelación real / CANCELLING) y ADR-0030 (balance real / reconciliación patrimonial) — ambos en **ESTADO: PROPUESTA** (no aprobados, no implementados).
**Entregable Parte 1 (referencia):** `docs/audits/2026-08-15-auditoria-ocm-parte-1-market-data.md` — corregido y validado forense el 2026-08-15 (362 líneas / 33 842 bytes).

---

## 1. Alcance

- **Solo FASE 5** (naming real) y **FASE 6** (quién posee el estado real) del repositorio OCM.
- Inspección real: `packages/trading`, `packages/portfolio`, `packages/market_data`, `shared`, `ocm` (guard), `config`.
- Contraste **solo contra ADR-0029 y ADR-0030**. Benchmark, Gap, Problemas globales, Arquitectura objetivo, Reorganización, Roadmap, Falsación, Contradicciones globales, Veredicto arquitectónico global y Partes 3/4 quedan **FUERA DE ALCANCE** (§12).

## 2. Limitaciones

1. ADR-0029 y ADR-0030 son **PROPUESTA** (estado `Propuesto`, no aprobado, no implementado). §8/§9 compara el código contra el **diagnóstico** verificado del ADR, no contra su diseño como si fuera contrato vigente.
2. No se ejecutó el runtime (sin live/paper/pipe en marcha). La vida del estado se infiere de código y docstrings, no de observación en ejecución. Lo no verificable por lectura se marca `UNKNOWN — razón concreta`.
3. Contexto previo leído (para no duplicar Parte 1): `2026-08-15-b-md-008-009-diseno-conceptual.md`, `2026-08-15-bot-benchmark-b-md-008-009.md`, `2026-08-15-verificacion-arquitectonica-cryptofeed-bybit-ws-no-ccxtpro.md`. El **diseño** descrito allí no es evidencia de **implementación**; solo el código manda.
4. La Parte 1 cubrió la frontera Market Data → Strategy → Risk → Execution (FASE 4). Esta Parte 2 la referencia sin repetirla.

---

## FASE 5 — NAMING AUDIT

### 5.1 Inspección de estructura real

Rutas reales inspeccionadas (verificadas por `ls`/`find`):

- `packages/trading/` → `analytics/`, `bootstrap/`, `execution/`, `risk/`, `strategies/`, `engine.py`
- `packages/portfolio/` → `bootstrap/`, `infra/`, `models/`, `ports/`, `services/`
- `packages/market_data/` → `adapters/inbound/{bybit_feed_adapter,kucoin_feed_adapter,pandas_to_domain,external,rest,websocket}`, `adapters/outbound/{exchange,storage,external_kafka_publisher,kafka_gap_publisher,kafka_trade_publisher}`, `application/{consumers,external_ingestion,pipeline,pipelines,processing,quality,strategies,use_cases,feed_orchestrator,source_manager}`, `domain/{constants,entities,events,exceptions,policies,quality,value_objects}`, `infrastructure/bootstrap/`, `ports/inbound/`, `ports/outbound/`
- `shared/` → `contracts/`, `kafka/schemas/`, `types/`, `utils/`
- `ocm/runtime/guard.py` (importado desde trading)

No existen `interfaces/`, `repositories/` como carpetas; los conceptos equivalentes viven en `ports/` (interfaces) y en `adapters/inbound/rest` + stores (`incoming position stores`, `CursorStorePort`). No se inventaron categorías.

### 5.2 Tabla obligatoria — Naming

Formato: Elemento | Ubicación | Nombre actual | Responsabilidad real | Problema | Severidad | Nombre sugerido | Evidencia

| Elemento | Ubicación | Nombre actual | Responsabilidad real | Problema | Severidad | Nombre sugerido | Evidencia |
|---|---|---|---|---|---|---|---|
| Estado de orden (dominio) | `packages/trading/execution/order.py:55-60` | `OrderStatus` | Estados de la orden en el OMS | Vocabulario divergente del transporte (`ERROR` solo en transporte) | Alta | Mantener nombre; alinear vocabularios | `order.py:55-60`, `_VALID_TRANSITIONS:64-74` |
| Estado de orden (transporte) | `packages/trading/execution/transport.py:53-61` | `OrderStatus` | Estado reportado por exchange/transporte | Colisión homónima con dominio; `ERROR` no modelable en dominio | Alta | `TransportOrderStatus` | `transport.py:53-61` |
| Protocol de fuente de mercado (inbound) | `packages/market_data/ports/inbound/market_data_source.py:24` | `MarketDataSource` | Contrato de ingesta inbound | — | Ninguna | — | `market_data_source.py:24` |
| Protocol de fuente de mercado (outbound) | `packages/market_data/ports/outbound/market_data_source.py:32` | `MarketDataSourcePort` | Contrato outbound de lectura de mercado | Dos archivos `market_data_source.py` (inbound/outbound) con clases casi homónimas | Media | `MarketReaderPort` | `market_data_source.py:32` |
| Contrato de exchange (ABC) | `packages/market_data/ports/outbound/exchange.py:35` | `ExchangeAdapter` (ABC) | Interfaz base de adapters de exchange; implementado por `CCXTAdapter` | — | Ninguna | — | `exchange.py:35`, `ccxt_adapter.py:96` |
| Contrato de exchange (Protocol) | `packages/market_data/ports/outbound/exchange_client.py:20` | `ExchangeClientPort` | Protocol de cliente de exchange | Dos contratos de exchange coexisten sin relación nominal | Media | Unificar (`ExchangePort`) | `exchange_client.py:20` |
| Publisher (re-export) | `packages/market_data/ports/outbound/publisher.py:1-20` | módulo `publisher` | Re-export de compatibilidad desde `publisher_port.py` | Módulo duplicado solo por imports legacy | Media | Eliminar o deprecar | `publisher.py` docstring ("Re-export desde publisher_port.py (SSOT)") |
| Publisher (SSOT) | `packages/market_data/ports/outbound/publisher_port.py:42-88` | `OHLCVPublisherPort` / `NullOHLCVPublisher` / `NullPublisher` | Publicación de OHLCV procesado | Naming `_Port` redundante + re-export duplicado | Baja | `publisher.py` canónico | `publisher_port.py:25-88` |
| Carpeta pipeline runtime | `packages/market_data/application/pipeline/runtime.py` | `pipeline/` (singular) | Runtime compartido del pipeline | `pipeline/` vs `pipelines/` homónimas | Media | `pipeline_runtime/` o fusionar | `ls application/pipeline` + `ls application/pipelines` |
| Carpeta de pipelines | `packages/market_data/application/pipelines/` | `pipelines/` (plural) | Pipelines concretos | — | Ninguna | — | `ls application/pipelines` |
| Runner WS (Bybit) | `packages/market_data/adapters/inbound/websocket/bybit_cryptofeed_runner.py:34` | `BybitCryptofeedRunner` | Runner WS usando cryptofeed | Nombre filtra librería (cryptofeed) | Baja | `BybitWSRunner` | `bybit_cryptofeed_runner.py:34` |
| Runner WS (KuCoin) | `packages/market_data/adapters/inbound/websocket/kucoin_cryptofeed_runner.py:29` | `KuCoinCryptofeedRunner` | Ídem KuCoin | Ídem | Baja | `KuCoinWSRunner` | `kucoin_cryptofeed_runner.py:29` |
| Stream order book | `packages/market_data/adapters/inbound/websocket/cryptofeed_orderbook_stream.py:65` | `CryptofeedOrderBookStream` | Stream del libro snapshot/delta | Ídem | Baja | `OrderBookStream` | `cryptofeed_orderbook_stream.py:65` |
| Adapter pandas→dominio | `packages/market_data/adapters/inbound/pandas_to_domain.py:1-30` | módulo `pandas_to_domain` | ACL DataFrame→dominio OHLCV | Nombre fija "pandas" pero el cuerpo ya importa polars (`pandas_to_domain.py:25`) | Baja | `frame_to_domain` | `pandas_to_domain.py:25` |
| Fuente WS trades (stub) | `packages/market_data/adapters/inbound/websocket/ws_trades_source.py:58` | `WSTradesSource` | Stub: `NOT_IMPLEMENTED — emite StopAsyncIteration` | Nombre de fuente real para un stub; confunde inventario | Media | `WSTradesSourceStub` | `ws_trades_source.py` docstring L9-11, `is_running:78-79` |
| Manager de fuentes de trades | `packages/market_data/application/source_manager.py:82` | `TradesSourceManager` | Árbitro REST/WS/replay, cursor + dedup | — | Ninguna | — | `source_manager.py:82-98` |
| Adapter feed exchange (Bybit) | `packages/market_data/adapters/inbound/bybit_feed_adapter.py:43` | `BybitFeedAdapter` | Adapter inbound de trades Bybit | — | Ninguna | — | `bybit_feed_adapter.py:43` |
| Adapter de features (gold) | `packages/trading/bootstrap/composition_root.py:136` | `_GoldFeatureSource` | Implementa `FeatureSource` (boundaries) | — | Ninguna | — | `composition_root.py:136-189`, `shared/contracts/boundaries.py:31` |
| Transport real (Bybit) | `packages/trading/bootstrap/composition_root.py:203` | `_BybitTransport` | Implementa `OrderTransport` vía CCXTAdapter | — | Ninguna | — | `composition_root.py:203-261` |
| Transport paper | `packages/trading/execution/transport.py:131` | `PaperTransport` | Implementa `OrderTransport` en memoria | Asimetría con `_BybitTransport` (privado en CR) | Baja | Sin cambio | `transport.py:131-158` |
| Protocol executor | `packages/trading/execution/oms.py:107` | `OrderExecutor` (Protocol) | Contrato de ejecución del OMS | — | Ninguna | — | `oms.py:107-119` |
| Kill switch | `ocm/runtime/guard.py` (import en `oms.py:92`, `live_executor.py:49`, `composition_root.py:56`) | `ExecutionGuard` | Kill switch | — | Ninguna | — | `oms.py:92,194-198` |
| Producer order book | `packages/market_data/adapters/inbound/websocket/orderbook_producer.py:56` | `OrderBookKafkaProducer` | Produce snapshot/delta a Kafka | — | Ninguna | — | `orderbook_producer.py:56-223` |
| Almacén de cursores | `packages/market_data/ports/outbound/state.py:44-115` | `CursorStorePort` / `AsyncCursorStorePort` | Persistencia de cursores | — | Ninguna | — | `state.py:44-165` |
| Factory de cursor store | `packages/market_data/adapters/inbound/rest/_cursor_factory.py:22` | `build_cursor_store` | Construye el store de cursor por contexto | — | Ninguna | — | `_cursor_factory.py:22` |
| Registry de feeds | `packages/market_data/infrastructure/bootstrap/feed_registry.py:38` | `get_adapter_class(exchange)` | Resuelve clase de adapter por exchange | — | Ninguna | — | `feed_registry.py:38-39` |

No se detectó problema de naming en: `OMS`, `TradingEngine`, `TradingRuntime`, `TradingCompositionRoot`, `RiskManager`, `PortfolioService`, `PositionStore`, `InMemoryPositionStore`, `RedisPositionStore`, `FeedOrchestrator`, `ExternalIngestionOrchestrator`, `OrderBookKafkaProducer`, `FeedRunnerProtocol`, `build_fill_sync` — sus nombres coinciden con su responsabilidad real observada.

### 5.3 Hallazgos Naming

**NAMING-N01**
- Elemento: `OrderStatus` (dominio vs transporte)
- Ubicación: `packages/trading/execution/order.py:55-60` y `packages/trading/execution/transport.py:53-61`
- Evidencia: dominio define PENDING/SUBMITTED/FILLED/REJECTED/CANCELLED; transporte define SUBMITTED/FILLED/CANCELLED/REJECTED/ERROR (verificado por `sed`)
- Comportamiento observado: mismo nombre para dos enumerados con vocabularios solapados; `ERROR` solo existe en transporte
- Problema semántico: un estado reportado por el transporte (ERROR) no es representable en el dominio; cambio de estados requiere tocar dos enumerados homónimos
- Severidad: Alta
- Clasificación: RESULTADO REAL
- Confianza: VERIFIED

**NAMING-N02**
- Elemento: `market_data_source.py` (dos archivos)
- Ubicación: `packages/market_data/ports/inbound/market_data_source.py:24` y `packages/market_data/ports/outbound/market_data_source.py:32`
- Evidencia: ambos archivos existen (verificado por `ls`); clases `MarketDataSource` vs `MarketDataSourcePort`
- Comportamiento observado: importar desde inbound u outbound es confundible por homonimia de archivo y clase
- Problema semántico: misma raíz de nombre para dos contratos con direcciones de dependencia opuestas
- Severidad: Media
- Clasificación: RESULTADO REAL
- Confianza: VERIFIED

**NAMING-N03**
- Elemento: módulo `publisher.py`
- Ubicación: `packages/market_data/ports/outbound/publisher.py:1-20`
- Evidencia: docstring "Re-export desde publisher_port.py (SSOT). Este módulo existe solo por compatibilidad de imports existentes" (grep VERIFIED)
- Comportamiento observado: doble ruta de import del mismo símbolo; SSOT en `publisher_port.py`
- Problema semántico: dos rutas de importación para el mismo contrato
- Severidad: Media
- Clasificación: RESULTADO REAL
- Confianza: VERIFIED

**NAMING-N04**
- Elemento: carpetas `pipeline/` vs `pipelines/`
- Ubicación: `packages/market_data/application/pipeline/` y `packages/market_data/application/pipelines/`
- Evidencia: ambas existen (verificado por `ls`); `pipeline/runtime.py` vs `pipelines/*_pipeline.py`
- Comportamiento observado: homonimia singular/plural para conceptos relacionados
- Problema semántico: confusión al inventariar/navegar; fricción de búsqueda
- Severidad: Media
- Clasificación: RESULTADO REAL
- Confianza: VERIFIED

**NAMING-N05**
- Elemento: clase `WSTradesSource`
- Ubicación: `packages/market_data/adapters/inbound/websocket/ws_trades_source.py:58`
- Evidencia: docstring "NOT_IMPLEMENTED — emite StopAsyncIteration inmediatamente" (grep VERIFIED)
- Comportamiento observado: nombre de fuente real para un stub estructural
- Problema semántico: el nombre no refleja su estado; un inventario lo lee como implementado
- Severidad: Media
- Clasificación: RESULTADO REAL
- Confianza: VERIFIED

**NAMING-N06**
- Elemento: nombres que fijan tecnología
- Ubicación: `bybit_cryptofeed_runner.py:34`, `kucoin_cryptofeed_runner.py:29`, `cryptofeed_orderbook_stream.py:65`, `pandas_to_domain.py:25`
- Evidencia: `cryptofeed` en 3 clases; `pandas_to_domain` con `import polars as pl` en L25
- Comportamiento observado: nombres anclados a librerías; `pandas_to_domain` desincronizado de la tecnología real (polars)
- Problema semántico: el nombre comunica la librería, no la responsabilidad
- Severidad: Baja
- Clasificación: RESULTADO REAL
- Confianza: VERIFIED

**NAMING-N07**
- Elemento: contratos de exchange
- Ubicación: `packages/market_data/ports/outbound/exchange.py:35` (ABC) y `packages/market_data/ports/outbound/exchange_client.py:20` (Protocol)
- Evidencia: `class ExchangeAdapter(ABC)` y `class ExchangeClientPort(Protocol)` (sed VERIFIED)
- Comportamiento observado: dos contratos para el mismo concepto de exchange con nombres no relacionados
- Problema semántico: ambigüedad sobre cuál es el contrato vigente de exchange
- Severidad: Media
- Clasificación: RESULTADO REAL
- Confianza: VERIFIED

---

## FASE 6 — RESPONSIBILITY / STATE OWNERSHIP AUDIT

### 6.1 Pregunta central por estado

WHO OWNS THIS STATE? — se responde con lo que el código hace hoy, no con lo que un ADR sugiere.

### 6.2 Estados inspeccionados (1–9)

1. **Order State** → OMS (memoria de proceso).
2. **Balance State** → AUSENTE (no hay implementación verificable; solo `capital_usd` configurado).
3. **Portfolio State** → `PortfolioService` + `PositionStore`.
4. **Market Data State** → distribuido: `feed_registry`/`FeedOrchestrator` (selección), `TradesSourceManager` (cursor/dedup), producers (emisión), health flags.
5. **Order Book State** → `CryptofeedOrderBookStream` (efímero en proceso; serializado a Kafka).
6. **Risk State** → `RiskManager` (espejo `_positions` + contadores).
7. **Execution State** → `ExecutionGuard` (OCM) + `RiskManager._halted` + `LiveExecutor._reconcile`.
8. **Exchange State** → `CCXTAdapter` (sesión CCXT).
9. **Position State** → 4 estructuras en runtime (ver 6.4, fila Position State).

### 6.3 Matriz obligatoria — State Ownership

Formato: STATE | OWNER REAL | STORAGE | UPDATE SOURCE | READERS / CONSUMERS | RECONCILIATION | POSSIBLE SSOT | EVIDENCIA | CLASIFICACIÓN

| STATE | OWNER REAL | STORAGE | UPDATE SOURCE | READERS / CONSUMERS | RECONCILIATION | POSSIBLE SSOT | EVIDENCIA | CLASIFICACIÓN |
|---|---|---|---|---|---|---|---|---|
| Order State | `OMS` | En memoria (dicts `_orders`, `_open`) | `OMS.submit`/`_fill`/`_reject`/`cancel` (transiciones `order.py:142`) | `TradingEngine.run_once`, `fill_sync.on_fill_composite`, `LiveExecutor._reconcile` | Solo durante submit (`live_executor.py:229-261`); sin loop periódico | `OMS._orders` (memoria) | `oms.py:169-170,185-368` | UNKNOWN — reconciliación periódica no verificable por lectura (no existe caller `manage_open_orders`) |
| Balance State | No hay owner (AUSENTE) | — | — | `RiskManager` (sizing) y `PortfolioService` (reporte) usan `capital_usd` | No existe | Config (`capital_usd`) | `risk/manager.py:112-118`, `portfolio_service.py:63-80` | ABSENT — sin `fetch_balance` ni store (grep balance/equity/free/wallet solo arroja `capital_usd` y equity derivada) |
| Portfolio State | `PortfolioService` | `PositionStore` (InMemory paper / Redis prod) | `fill_sync.on_fill_composite` desde fills OMS | `TradingEngine` (stop-loss snapshot), `RebalanceService` | No hay reconciler contra exchange (ADR-0030 lo propone) | `PositionStore` (BC-43) | `portfolio_service.py:50-274`, `fill_sync.py:109-167` | PARCIALMENTE CONSISTENTE — SSOT declarado pero divergencia documentada con `TradeTracker` |
| Market Data State | Distribuido | Cursor en `CursorStorePort`/Redis; resto efímero | Adapters REST/WS; `TradesSourceManager._advance_cursor` | Pipelines, consumers de calidad | Cursor monotónico + `gap_scanner` (Parte 1) | Exchange (primario) / cursor persistido | `source_manager.py:146,213-255`, `state.py:44` | PARCIALMENTE CONSISTENTE — freshness no consolidada en flujo WS→execution (Parte 1) |
| Order Book State | `CryptofeedOrderBookStream` (proceso) | En memoria (`book.book`, extensión C); no persistido | cryptofeed snapshot/delta | `OrderBookKafkaProducer` (→ Kafka) | snapshot/delta con checksum | Exchange (libro primario); Kafka raw | `cryptofeed_orderbook_stream.py:36,158-161`, `orderbook_producer.py:128-223` | PARCIALMENTE CONSISTENTE — `sequence_number` no serializado al payload (Parte 1) |
| Risk State | `RiskManager` | En memoria (dicts bajo lock) | Push del OMS: `record_position`/`record_close`/`record_open` | `OMS.validate`, `TradingEngine` | Espejo `_positions` sincronizado por push OMS (mismo origen que `OMS._entry_positions`) | `RiskManager._positions` (espejo) | `manager.py:135,164-195,266-297` | PARCIALMENTE CONSISTENTE — espejo duplicado con `OMS._entry_positions` y `PortfolioService` |
| Execution State | `ExecutionGuard` (OCM) + `RiskManager._halted` + `LiveExecutor._reconcile` | En memoria | Guard externo / risk halt / fail-closed de submit | `OMS.submit` (guard check), `LiveExecutor.execute` | Fail-closed solo en ventana de submit | Kill switch (guard) | `oms.py:194-198`, `live_executor.py:229-261` | PARCIALMENTE CONSISTENTE — sin gestión de órdenes abiertas en vuelo |
| Exchange State | `CCXTAdapter` | Sesión/estado de la librería CCXT | Llamadas CCXT | `_BybitTransport` (submit/fetch_state) | `LiveExecutor._reconcile` (solo submit) | Exchange (externo) | `ccxt_adapter.py:96`, `composition_root.py:203-261` | PARCIALMENTE CONSISTENTE — sin `fetch_balance` ni `cancel_order` expuestos |
| Position State | Múltiple (ver OWN-02) | `OMS._entry_positions` (memoria), `RiskManager._positions` (memoria), `TradeTracker._open_positions` (memoria), `PositionStore` (persistido) | OMS fills → `fill_sync.on_fill_composite` → los 3 espejos + store | `OMS` (cierre), `RiskManager.validate`, `TradeTracker` (analytics), `TradingEngine` (stop-loss) | Push OMS a todos; sin reconciler unificado | `PositionStore` (persistido, BC-43) | `oms.py:177`, `manager.py:135`, `trade_tracker.py:59`, `portfolio_service.py:50-274`, `fill_sync.py:109-167` | PARCIALMENTE CONSISTENTE — 4 copias del mismo hecho con divergencia documentada (`fill_sync.py:38-40`) |

### 6.4 Comparación con ADR-0029 / ADR-0030

**Order State vs ADR-0029 (PROPUESTA):**

- `OMS.cancel()` es local-only: transiciona a `CANCELLED` sin contacto con el exchange (`oms.py:300-317`).
- `OrderTransport` (Protocol) solo define `submit`/`fetch_state`/`close` (`transport.py:96-128`); `PaperTransport` y `_BybitTransport` sin cancel.
- `CCXTAdapter` no expone `cancel_order` (`ccxt_adapter.py:405` create_order, `:462` fetch_order).
- No existe caller de `manage_open_orders`.

**Clasificación: CONSISTENTE** — el código implementa exactamente el estado que el ADR diagnostica como problema (cancel local, sin CANCELLING, sin manage_open_orders). Evidencia de la clasificación: `order.py:64-74` (grafo sin CANCELLING), `oms.py:300-317`, `transport.py:96-128`, `ccxt_adapter.py:405,462`.

**Balance / Portfolio State vs ADR-0030 (PROPUESTA):**

- No existe `fetch_balance` (`ccxt_adapter.py:405,462` solo create_order/fetch_order).
- `RiskManager` recibe `capital_usd` con default 10 000 (`manager.py:112-118`).
- `PortfolioService` recibe `capital_usd` por constructor (`portfolio_service.py:63-80`).
- No existe `BalanceStore` ni `PortfolioReconciler` (el ADR los propone).

**Clasificación: CONSISTENTE** — el código coincide con el diagnóstico F-BMD9-01 del ADR ("OCM no conoce el saldo real del exchange"); el diseño propuesto no está implementado, coherente con su estado PROPUESTA. Evidencia de la clasificación: `manager.py:112-118`, `portfolio_service.py:63-80`, `ccxt_adapter.py:405,462`, grep `balance/equity/free/wallet`.

### 6.5 Hallazgos de ownership

**OWN-01**
- Estado: Order State
- Owner observado: `OMS`
- Ubicación: `packages/trading/execution/oms.py:169-170`
- Evidencia: dicts `_orders`/`_open` en memoria; transiciones `order.py:142`; `cancel()` local-only `oms.py:300-317`
- Quién escribe: `OMS.submit`/`_fill`/`_reject`/`cancel`
- Quién lee: `TradingEngine.run_once`, `fill_sync`, `LiveExecutor._reconcile`
- Problema: Order State solo en memoria de proceso; sin rehidratación del exchange ni loop `manage_open_orders`
- Severidad: Alta
- Clasificación: RESULTADO REAL
- Confianza: VERIFIED

**OWN-02**
- Estado: Position State
- Owner observado: múltiple (sin SSOT único en runtime)
- Ubicación: `oms.py:177`, `risk/manager.py:135`, `analytics/trade_tracker.py:59`, `portfolio/services/portfolio_service.py:50-274`
- Evidencia: `_entry_positions`, `_positions`, `_open_positions`, `PositionStore`; divergencia declarada en `fill_sync.py:38-40` ("múltiples fuentes de verdad... decisión arquitectónica pendiente")
- Quién escribe: `fill_sync.on_fill_composite` (fills OMS) → los tres espejos y el store
- Quién lee: OMS (cierre), `RiskManager.validate`, `TradeTracker` (analytics), `TradingEngine` (stop-loss)
- Problema: 4 copias del mismo hecho (posición abierta con qty/avg); solo `PositionStore` es persistido (BC-43); divergencia documentada sin resolver (posición fantasma, B-15/H-09)
- Severidad: Alta
- Clasificación: RESULTADO REAL
- Confianza: VERIFIED

**OWN-03**
- Estado: Balance State
- Owner observado: ninguno (AUSENTE)
- Ubicación: — (no existe implementación)
- Evidencia: grep `balance/equity/free/wallet` en `packages/trading`, `packages/portfolio`, `apps` solo arroja `capital_usd`; `ccxt_adapter.py:405,462` sin `fetch_balance`
- Quién escribe: — (no existe)
- Quién lee: `RiskManager` (sizing/drawdown), `PortfolioService` (reporte), `PerformanceEngine` (equity derivada) — todos sobre `capital_usd`
- Problema: sizing/exposición contra capital configurado, no contra saldo real (bloqueante live, ADR-0030)
- Severidad: Alta
- Clasificación: RESULTADO REAL
- Confianza: VERIFIED

**OWN-04**
- Estado: Order Book State
- Owner observado: `CryptofeedOrderBookStream` (proceso)
- Ubicación: `packages/market_data/adapters/inbound/websocket/cryptofeed_orderbook_stream.py:65`
- Evidencia: `book.book` (extensión C) efímero; `OrderBookKafkaProducer` publica a Kafka (`orderbook_producer.py:128-223`); `sequence_number` no serializado al payload (Parte 1)
- Quién escribe: cryptofeed snapshot/delta
- Quién lee: `OrderBookKafkaProducer` → Kafka
- Problema: no se conserva estado local del libro; sin campo `sequence` en `shared/kafka/schemas/orderbook.py`
- Severidad: Media
- Clasificación: RESULTADO REAL
- Confianza: VERIFIED

**OWN-05**
- Estado: Market Data State (freshness)
- Owner observado: distribuido, sin consolidar
- Ubicación: `feed_orchestrator.py:58`, `source_manager.py:82`, `gap_aware_stream.py:160` (`is_running`)
- Evidencia: `is_running` en `GapAwareStream`/`WSTradesSource`; `is_healthy` en `ccxt_adapter.py:219`; sin política de freshness en flujo WS→execution (Parte 1)
- Quién escribe: adapters REST/WS
- Quién lee: pipelines, `TradesSourceManager`, `TradingEngine` (indirecto)
- Problema: freshness no consolidada como estado consultable en la frontera de ejecución
- Severidad: Media
- Clasificación: RESULTADO REAL
- Confianza: VERIFIED

### 6.6 Múltiples fuentes de verdad

- **SoT-1 (CONFIRMADA):** Position State en 4 estructuras — `OMS._entry_positions` (`oms.py:177`), `RiskManager._positions` (`manager.py:135`), `TradeTracker._open_positions` (`trade_tracker.py:59`), `PositionStore` (persistido). El propio código lo declara en `fill_sync.py:38-40`.
- **SoT-2 (CONFIRMADA):** dos `OrderStatus` con semántica solapada (NAMING-N01) — `order.py:55` vs `transport.py:53`.
- **SoT-3 (CONFIRMADA):** cursor de trades en `CursorStorePort` (Redis) — única copia persistida; estado efímero del manager deriva de ella (`state.py:44`, `source_manager.py:146`).
- **SoT-4 (PARCIAL):** Order State solo en memoria; exchange es fuente primaria sin copia persistida (`oms.py:169-170`).

---

## 7. Matriz de integridad de la Parte 2

Formato: Fase | Resultado real | Evidencia concreta | Trazabilidad | Categoría | Limitación

| Fase | Resultado real | Evidencia concreta | Trazabilidad | Categoría | Limitación |
|---|---|---|---|---|---|
| FASE 5 — Naming | 24 filas de tabla + 7 hallazgos (NAMING-N01..N07) con rutas/símbolos/líneas reales | `order.py:55-60`, `transport.py:53-61`, `market_data_source.py:24/32`, `publisher.py`, `pipeline/` vs `pipelines/`, `ws_trades_source.py:58`, `pandas_to_domain.py:25`, `exchange.py:35` vs `exchange_client.py:20` | Inspección `ls`/`find`/`grep`/`sed` de packages, shared, ocm | RESULTADO REAL | Sin ejecución de runtime; nombres evaluados por lectura |
| FASE 6 — State Ownership | 9 estados con owner/storage/update/readers/reconciliation/SSOT verificados por lectura; 2 clasificaciones vs ADR; 5 hallazgos (OWN-01..05) | `oms.py:169-170,177,300-317`, `manager.py:112-118,135`, `portfolio_service.py:63-80`, `trade_tracker.py:59`, `fill_sync.py:38-40,109-167`, `ccxt_adapter.py:405,462`, `source_manager.py:146`, `state.py:44` | `grep`/`sed` de execution, risk, portfolio, analytics, adapters | RESULTADO REAL | Vida del estado inferida de código/docstrings; no observada en ejecución |

---

## 8. Autoauditoría del informe

1. **Relectura completa:** realizada (este archivo, 321 líneas).
2. **Rutas citadas:** todas verificadas con `ls`/`find`/`sed` durante la inspección (listado en §5.1).
3. **Símbolos citados:** verificados (clases/Protocols/métodos por `grep`; p.ej. `OrderStatus`, `MarketDataSourcePort`, `ExchangeAdapter`, `OrderBookKafkaProducer`, `build_cursor_store`, `get_adapter_class`).
4. **Números de línea:** verificados por `sed`/`grep -n` (p.ej. `order.py:55-74` → 10 matches; `ccxt_adapter.py:405,462`; `trade_tracker.py:59`; `manager.py:112-118`).
5. **Tablas con datos reales:** sí (matrices §5.2 y §6.3 rellenadas con evidencia).
6. **Placeholders:** `rg` de `TODO|TBD|PLACEHOLDER|por determinar|por revisar` → sin matches.
7. **Rutas inventadas:** ninguna; las 3 carpetas homónimas (`pipeline`/`pipelines`) y los ports existen.
8. **Instrucciones convertidas en resultado:** no; los matches de "Construye"/"Clasificación" en el archivo son descriptivos (columna de responsabilidad y sustantivo), no imperativos copiados del prompt.
9. **Matrices completadas por inferencia:** no; cada celda de `UNKNOWN`/`ABSENT` tiene razón concreta.
10. **Conclusiones con evidencia:** sí (todas referencian ruta:línea).
11. **UNKNOWN con razón:** sí (`UNKNOWN — reconciliación periódica no verificable por lectura (no existe caller manage_open_orders)`).
12. **Parte 3/4 no ejecutada:** confirmado.

## 9. Validación Git

Resultado (verificado al final):
- Código tracked sin modificaciones: `git diff --stat` vacío.
- ADR-0029 sin modificaciones: `git diff HEAD -- <ADR-0029>` = 0 líneas.
- ADR-0030 sin modificaciones: idem.
- tracking.yaml (`docs/plans/tracking.yaml`) sin modificaciones: idem.
- Sin staged changes: `git diff --cached --stat` vacío.
- Nuevo informe como untracked: `?? docs/audits/2026-08-15-auditoria-ocm-parte-2-naming-responsibility.md`.
- Sin `git add`, sin `commit`, sin `push`.

---

### Anexo de validación del entregable

- **ARCHIVO CREADO:** `docs/audits/2026-08-15-auditoria-ocm-parte-2-naming-responsibility.md` (este archivo).
- **ARCHIVOS MODIFICADOS:** ninguno más.
- **CÓDIGO MODIFICADO:** NO.
- **ADRs MODIFICADOS:** NO (ADR-0029, ADR-0030 intactos).
- **tracking.yaml MODIFICADO:** NO.
- **COMMIT:** NO.
- **PUSH:** NO.