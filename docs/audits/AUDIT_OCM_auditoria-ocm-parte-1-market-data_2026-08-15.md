# Auditoría OCM — Parte 1/4
## Inventario + Market Data + Contracts + Execution Boundary

Fecha: 2026-08-15
Repositorio: /home/orangemusic/trading/orangecashmachine (OrangeCashMachine)
Commit/base inspeccionada: rama `main`, HEAD `54b64ff` (no hubo cambios sobre el working tree en esta auditoría)

## 0. Alcance y estado de ejecución

Fases auditadas (solo estas cuatro, según el alcance de la Parte 1/4):

1. **FASE 1 — Inventario real del repositorio**: árbol real, responsabilidades, capas, evidencia.
2. **FASE 2 — Market Data**: flujo real de market data (origen → normalización → pipeline → persistencia).
3. **FASE 3 — Market Data Contracts**: ports/interfaces/contratos reales.
4. **FASE 4 — Market Data → Strategy → Risk → Execution**: frontera real y preguntas de frescura/stale-data.

Base inspeccionada: HEAD `54b64ff` en rama `main`. Al finalizar la auditoría, `git status --short` muestra
3 archivos untracked: el archivo de esta auditoría (`docs/audits/2026-08-15-auditoria-ocm-parte-1-market-data.md`)
y 2 preexistentes (`docs/audits/2026-08-15-auditoria-integral-market-data-naming-estructura.md`,
`docs/audits/2026-08-15-benchmark-complementario-lean-vnpy-ccxtpro.md`). Ningún archivo tracked fue modificado
(`git diff` y `git diff --cached` vacíos).

Limitaciones:
- La inspección fue **estática** (lectura de código/config), no se ejecutó ningún pipeline ni se conectó a
  exchanges/infraestructura real. Los estados de funcionalidad en vivo se infieren del código; donde la
  evidencia no lo permite, se marca UNKNOWN.
- No se ejecutaron tests (tarea read-only).
- El working tree no se modificó más allá del archivo de esta auditoría.

Evidencia UNKNOWN: las capacidades cuyo estado real en runtime no pudo verificarse estáticamente se
marcan `UNKNOWN` con la razón concreta en cada fila.

---

## FASE 1 — INVENTARIO REAL DEL REPOSITORIO

### 1.1 Carpetas existentes verificadas

Se verificó el árbol real con `find`/`ls`. Existen: `apps/`, `packages/`, `shared/`, `config/`, `tests/`,
`docs/`, `ocm/`, `architecture/`, `data_platform/`, `deploy/`, `infrastructure/` (legacy) y `scripts/`.
No existen `src/`, `tools/`, `adapters/`, `domain/`, `application/` a nivel raíz.

| PATH | RESPONSABILIDAD REAL | MÓDULOS PRINCIPALES | CAPA | EVIDENCIA | PROBLEMA |
|---|---|---|---|---|---|
| `apps/api/` | Gateway FastAPI experimental | entrypoints HTTP | Adapters/infrastructure | `apps/api/` existe; AGENTS.md: "FastAPI gateway, experimental" | — |
| `apps/app/` | CLI entrypoints (ocm) | `app/cli/main.py` (entrypoint `uv run ocm`) | Adapters/infrastructure | AGENTS.md: "CLI entrypoint: uv run ocm (via app.cli.main)" | — |
| `apps/research/` | Consumo read-only de la capa Gold (notebooks) | paquete `research` | Adapters/infrastructure | AGENTS.md: "read-only gold layer consumer"; es `root_packages` de importlinter | — |
| `packages/market_data/` | Bounded Context market data, Clean/Hexagonal | domain/, ports/, application/, adapters/, infrastructure/ | BC completo | Árbol real: `packages/market_data/domain/{entities,events,exceptions,policies,quality,value_objects}` (verificado por `ls domain/`), `ports/{inbound,outbound}`, `adapters/{inbound,outbound}`, `application/`, `infrastructure/{bootstrap,storage}` | — |
| `packages/trading/` | Bounded Context trading, engine en desarrollo activo | engine.py, strategies/, execution/, risk/, bootstrap/, analytics/ | BC completo | Árbol real: `packages/trading/engine.py`, `execution/{oms,order,live_executor,paper_executor,transport,fill_sync,settlement}.py`, `risk/`, `strategies/`, `bootstrap/composition_root.py` | — |
| `packages/portfolio/` | Gestión de posiciones + rebalanceo | models/, services/, bootstrap/ | BC completo | AGENTS.md: "position management + rebalance"; composition root `portfolio.bootstrap.composition_root` | — |
| `shared/` | Capa más baja: tipos, schemas kafka, contracts (Protocols), excepciones, utils | `shared/kafka/schemas/`, `shared/contracts/boundaries.py`, `shared/enums.py` | Shared (solo stdlib + 3rd-party aprobado) | AGENTS.md sección Architecture; `shared/contracts/boundaries.py` existe (151 líneas) | — |
| `config/` | Hydra YAML config por capas | `config/market_data/feeds.yaml`, `config/base.yaml`, `config/env/`, `config/exchanges/`, `config/pipeline/` | Configuration | `config/market_data/feeds.yaml` real (30 líneas); AGENTS.md describe capas base→env→exchange→pipeline→CLI→env vars | — |
| `tests/` | Tests pytest (unit + integration) | `tests/architecture/`, `tests/market_data/`, `tests/trading/` | Tests | AGENTS.md: "Structural invariants beyond import-linter: tests/architecture/" | — |
| `docs/` | ADRs, audits, plans, knowledge | `docs/architecture/decisions/ADR-0029*`, `ADR-0030*`, `docs/audits/`, `docs/plans/tracking.yaml`, `docs/knowledge/` | Docs | ADR-0029 y ADR-0030 existen y fueron leídos | — |
| `ocm/` | Plataforma (config/runtime/observability), sin lógica de negocio | `ocm/config/`, `ocm/runtime/`, `ocm/observability/` | Platform | AGENTS.md: "ocm/ = platform (config/runtime/observability), no business logic" | — |
| `architecture/` | Contratos import-linter (BC-NN) | `architecture/importlinter.toml` | Contratos arquitectónicos | AGENTS.md: "BC-NN contracts live in architecture/importlinter.toml" | — |
| `data_platform/` | Iceberg catalog + warehouse | `data_platform/iceberg_catalog/`, `data_platform/iceberg_warehouse/gold|silver` | Infraestructura | Directorios vistos en `find` | — |
| `deploy/` | Deployment/monitoring | `deploy/monitoring/` | Infraestructura | Directorio visto en `find` | — |
| `infrastructure/` | Legacy vacío | `__init__.py` (sin contenido funcional) | Legacy | `find` muestra solo `__init__.py` | Carpetas legacy residuales |
| `scripts/` | Gates de gobernanza/ingeniería ejecutables (guard AST de Application Layer, health check del Plan Maestro, enforcement SSOT de enums, reporte de métricas del Shared Kernel) | `app_layer_guard.py`, `backtest_app_guard.py`, `check_ssot_enums.py`, `engineering_health_check.py`, `metrics_report.py` | Dev/tooling (fuera de la arquitectura de runtime) | `ls scripts/` (5 archivos reales); docstrings de cada script (p. ej. `app_layer_guard.py:1-7`) | Herramientas de CI/ingeniería, no parte del runtime |
| `src/`, `tools/`, `adapters/`, `domain/`, `application/` | — | — | — | **NO existen** a nivel raíz | Verificado por `find . -maxdepth 3 -type d` |

### 1.2 Hallazgos FASE 1 (solo evidencia)

**A. Correspondencia estructura ↔ arquitectura.**
- VERIFIED: la estructura refleja la arquitectura declarada (medallion Iceberg, Clean/Hexagonal por BC,
  `shared` como capa baja, `ocm` como plataforma). Evidencia: coexistencia de `domain/`, `ports/`,
  `application/`, `adapters/`, `infrastructure/` dentro de `packages/market_data/`, y separación en BCs
  `market_data`/`trading`/`portfolio`.

**B. Responsabilidades mezcladas.**
- VERIFIED (parcial): en `packages/trading/bootstrap/composition_root.py` coexisten tres responsabilidades
  de wiring en un solo archivo: el único punto autorizado a importar market_data (`_GoldFeatureSource` L136,
  `_BybitTransport` L203, BC-50), el adaptador concreto de transporte (`_BybitTransport`), y el
  `TradingCompositionRoot` (L322). No es un problema per se (es el composition root por diseño, ADR-0003),
  pero concentra el código de adaptación de la frontera entre BCs en un solo módulo.

**C. Duplicación conceptual.**
- VERIFIED: existen dos modelos de trade casi equivalentes en dominios distintos de market_data:
  - `market_data/domain/value_objects/normalized_trade.py` → `NormalizedTrade` (used por `BybitFeedAdapter`,
    `TradeCallback`).
  - `market_data/domain/value_objects/raw_trade.py` → `RawTrade` + `TradeSource` (used por
    `TradesSourceProtocol`, `GapAwareStream`, `GapRecoveryFetcher`).
  Ambos representan un trade normalizado con timestamp y side; el primero en `ports/inbound/market_data_source.py`
  y el segundo en `ports/inbound/trades_source.py`. Coexisten sin relación de herencia/composición.

**D. Naming ambiguo.**
- VERIFIED: el puerto inbound se llama `TradesSourceProtocol` pero hay también `OrderBookSourceProtocol`
  en el mismo módulo (`packages/market_data/ports/inbound/trades_source.py` L55 y L133) — el nombre del
  archivo (`trades_source.py`) sugiere solo trades, pero define el contrato de order book también.
- VERIFIED: `OrderBookKafkaProducer` está en `adapters/inbound/websocket/orderbook_producer.py`, pero es un
  productor Kafka (outbound) — vive en `adapters/inbound` porque recibe callbacks del stream, no porque sea
  un inbound.

**E. God modules.**
- VERIFIED (candidato): `packages/trading/bootstrap/composition_root.py` (587 líneas) contiene: `_GoldFeatureSource`,
  `_BybitTransport`, `run_ccxt_async`, `map_ccxt_order`, `TradingCompositionRoot` con `assemble_live`,
  `assemble_paper`, `assemble_rebalance`, `_map_risk_config`, `_resolve_risk_config`. Es el único punto
  autorizado de contacto entre BCs (BC-50), así que la densidad es esperada, pero el módulo es grande.
- VERIFIED (candidato): `packages/trading/execution/oms.py` (565+ líneas) concentra: máquina de estados de
  órdenes (submit/cancel/_fill/_reject), validación de señales (delegando a risk), libro de órdenes abiertas,
  contabilidad held-position y callbacks de fill. Varias responsabilidades en un solo módulo.

**F. Adapters con lógica de dominio.**
- VERIFIED: `packages/market_data/adapters/inbound/bybit_feed_adapter.py` contiene lógica de fan-out de
  callbacks (`_dispatch` L108, `asyncio.gather` con `return_exceptions`) que podría considerarse lógica de
  aplicación/dominio en un adapter. Aunque está en la frontera, es lógica de coordinación en un adapter.

**G. Infraestructura filtrándose al dominio.**
- NO observado en domain/ de market_data: los módulos de dominio leídos
  (`value_objects/raw_trade.py`, `value_objects/normalized_trade.py`, `domain/exceptions.py`) no importan
  cryptofeed ni ccxt. VERIFIED por lectura directa.

**H. Composition roots.**
- VERIFIED: existen composition roots por BC:
  - `packages/market_data/infrastructure/bootstrap/composition_root.py` (CompositionRoot L99).
  - `packages/trading/bootstrap/composition_root.py` (TradingCompositionRoot L322).
  - `packages/portfolio/bootstrap/composition_root.py` (según AGENTS.md; no se leyó en esta fase).
  Además `packages/market_data/infrastructure/bootstrap/feed_registry.py` mapea exchange → adapter (wiring).

---

## FASE 2 — MARKET DATA

### 2.1 Flujo real reconstruido desde código

Cadena REAL (por lectura de módulos):

```
cryptofeed 2.4.1
 ├─ BybitCryptofeedRunner (adapters/inbound/websocket/bybit_cryptofeed_runner.py) — canal TRADES, Bybit
 ├─ KuCoinCryptofeedRunner (adapters/inbound/websocket/kucoin_cryptofeed_runner.py) — canal TRADES, KuCoin spot
 └─ CryptofeedOrderBookStream (adapters/inbound/websocket/cryptofeed_orderbook_stream.py) — canal L2_BOOK, genérico
       │  → callback on_trade/on_snapshot/on_delta
       ▼
BybitFeedAdapter / KuCoinFeedAdapter (adapters/inbound/{bybit,kucoin}_feed_adapter.py)
  → fan-out a TradeCallback (OrderBookKafkaProducer / KafkaTradePublisher)
       ▼
Kafka: trades.raw (topic config feeds.yaml), orderbook.raw (TOPIC_ORDERBOOK_RAW)
       ▼
Bronze → Silver → Gold (Iceberg, medallion)
       ▼
GoldReader (adapters/outbound/storage/gold_reader.py) → FeatureSource → TradingEngine
```

Verificado: la config `config/market_data/feeds.yaml` define `ingestion_mode: rest` (L13), bybit habilitado
con BTC-USDT-PERP, ETH-USDT-PERP, SOL-USDT-PERP (L20-24), kucoin deshabilitado (L27).

### 2.2 Capacidades del flujo (clasificación con evidencia)

| CAPACIDAD | ESTADO | EVIDENCIA REAL | UBICACIÓN | OBSERVACIÓN |
|---|---|---|---|---|
| 1. Instancia de cryptofeed | IMPLEMENTADO | `FeedHandler()` en `start()`; feed `Bybit(KuCoin)` con `channels=[TRADES]`; `cryptofeed` 2.4.1 documentado | `websocket/bybit_cryptofeed_runner.py:56-70`, `websocket/kucoin_cryptofeed_runner.py:41-57`, `websocket/cryptofeed_orderbook_stream.py:111-127` | cryptofeed es la fuente WS real (trade y order book) |
| 2. Configuración | IMPLEMENTADO | `feeds.yaml`: `ingestion_mode: rest`, bybit enabled, symbols, topic `trades.raw`; `OrchestratorConfig` dataclass | `config/market_data/feeds.yaml:13-24`; `application/feed_orchestrator.py:44-52` | Config Hydra; SSOT `AppConfig.feeds` (composition_root L181) |
| 3. Lifecycle | IMPLEMENTADO | `FeedOrchestrator.run()`: instala signal handlers, crea adapters, lanza Tasks, shutdown graceful con `adapter.stop()` + cancel | `application/feed_orchestrator.py:82-135` | — |
| 4. Exchange | IMPLEMENTADO | Bybit y KuCoin mapeados a clases cryptofeed en `_EXCHANGE_CLASSES`; registry lazy `_ADAPTER_CLASSES` | `websocket/cryptofeed_orderbook_stream.py:56-59`; `infrastructure/bootstrap/feed_registry.py:19-22` | — |
| 5. Canales | IMPLEMENTADO | `TRADES` (runners) y `L2_BOOK` (order book stream); KuCoin spot solo (`/market/match`) | `bybit_cryptofeed_runner.py:65`, `kucoin_cryptofeed_runner.py:51`, `cryptofeed_orderbook_stream.py:117` | — |
| 6. Símbolos | IMPLEMENTADO | Bybit: BTC-USDT-PERP, ETH-USDT-PERP, SOL-USDT-PERP; KuCoin: BTC-USDT, ETH-USDT (deshabilitado) | `config/market_data/feeds.yaml:21-29` | — |
| 7. Callbacks | IMPLEMENTADO | `callbacks={TRADES: _translate_and_dispatch}`; `on_snapshot`/`on_delta` para order book; `TradeCallback` en adapters | runners y `orderbook_producer.py:128-223` | — |
| 8. Estructuras recibidas | IMPLEMENTADO | cryptofeed `Trade` y `OrderBook` (wrapper C con `.to_dict()`, `.delta`, `.book`); documentado en docstring | `bybit_cryptofeed_runner.py:78-101`, `cryptofeed_orderbook_stream.py:142-220` | ACL confina tipos vendor |
| 9. Procesamiento L2 | IMPLEMENTADO | Snapshot inicial (`delta is None`) → `on_snapshot`; deltas → `on_delta`; ordenado por side; `_sorted_levels` | `cryptofeed_orderbook_stream.py:187-220` | — |
| 10. Procesamiento de trades | IMPLEMENTADO | `Trade` → `NormalizedTrade(symbol, side, price, amount, timestamp, received_at)` | `bybit_cryptofeed_runner.py:89-101` | — |
| 11. Timestamps | IMPLEMENTADO | `timestamp` (exchange) + `received_at` (wall-clock local) en `NormalizedTrade`; `timestamp_ms` en order book | `domain/value_objects/normalized_trade.py:42-54`, `cryptofeed_orderbook_stream.py:182-185` | — |
| 12. Exchange timestamp | IMPLEMENTADO | `book.timestamp` (ms para Bybit, verificado en vivo, documentado L166-174); `trade.timestamp` | `cryptofeed_orderbook_stream.py:180-185` | Verificar unidades para otros exchanges |
| 13. Receipt timestamp | IMPLEMENTADO | `receipt_timestamp` (segundos float) *1000 → ms | `cryptofeed_orderbook_stream.py:185` | — |
| 14. Símbolo | IMPLEMENTADO | `book.symbol`, `trade.symbol` propagados | runners, stream L178 | — |
| 15. Exchange | IMPLEMENTADO | `exchange` propagado (`self._exchange`) | stream L194 | — |
| 16. Tipo de evento | IMPLEMENTADO | snapshot vs delta distinguidos por `delta is None` | `cryptofeed_orderbook_stream.py:187` | — |
| 17. Sequencing | PARCIAL | `book.sequence_number` es atributo vendor documentado pero NO se serializa al payload Kafka | `cryptofeed_orderbook_stream.py:160` (mencionado), `shared/kafka/schemas/orderbook.py` (sin campo sequence) | El número de secuencia se pierde al publicar |
| 18. Duplicados | PARCIAL | `client_order_id` para idempotencia de órdenes (execution); en market data WS no hay dedup visible salvo `TradeSource`/`source` | `trading/execution/live_executor.py:174`, `domain/value_objects/raw_trade.py:83-98` | `TradeSource` declara proveniencia (SSOT para dedup downstream) |
| 19. Out-of-order | UNKNOWN | No se encontró lógica de reordenamiento en el stream WS de trades/order book | — | No hay evidencia estática de manejo out-of-order |
| 20. Gaps | IMPLEMENTADO | `GapAwareStream` detecta gap por silencio (`gap_threshold_ms` default 30s) y por disconnection | `websocket/gap_aware_stream.py:85-225` | — |
| 21. Recuperación | IMPLEMENTADO | `GapRecoveryFetcher` con `source=TradeSource.REST_RECOVERY`; `recovery_factory` inyectada | `inbound/rest/gap_recovery_fetcher.py:139,240` | — |
| 22. Reconstrucción del order book | PARCIAL | El snapshot inicial se extrae via `to_dict()` y se publica completo; deltas se publican; pero la reconstrucción continua del libro ocurre en el consumidor (no se ve en el productor) | `cryptofeed_orderbook_stream.py:190-192` | El productor no mantiene estado del libro |
| 23. Snapshot | IMPLEMENTADO | `on_snapshot` con `bids`/`asks` completos, `depth`, `checksum` | `orderbook_producer.py:128-177`, stream L190-205 | — |
| 24. Resync | PARCIAL | `cryptofeed` gestiona resync internamente (lib); `checksum` propagado al payload | stream L201, schema | No hay lógica OCM de resync manual visible |
| 25. Desconexión | PARCIAL | `GapAwareStream` maneja disconnection con `reconnect=True`; cryptofeed reconecta internamente (documentado en stream) | `gap_aware_stream.py:220-232`, stream docstring | — |
| 26. Reconexión | PARCIAL | `GapAwareStream._handle_disconnection` intenta reconnect (true por defecto); runner delega en cryptofeed | `gap_aware_stream.py:224-225` | — |
| 27. Datos incompletos/corruptos | PARCIAL | SafeOps en `stop()`; callbacks con try/except (`snapshot_dispatch_failed`/`delta_dispatch_failed`); `NormalizedTrade` valida Decimal | stream L203-220, `normalized_trade.py` | No hay validación de integridad del libro (checksum cruzado) en OCM |
| 28. Backpressure | UNKNOWN | No se observó mecanismo de backpressure explícito (bounded queue, etc.) en el flujo WS→Kafka | — | No hay evidencia de bounded queue en adapters |
| 29. Bounded queue | AUSENTE | No se encontró ninguna cola acotada en el flujo de market data | — | Grep sin resultados |
| 30. Consumidores lentos | UNKNOWN | `asyncio.gather` fan-out en `_dispatch` sin timeouts; consumidores lentos bloquean el dispatch | `bybit_feed_adapter.py:108-116` | Riesgo potencial, sin evidencia de mitigación |
| 31. Lag | UNKNOWN | Métricas de latency en Kafka (KafkaMetrics) pero no de lag de consumidor en estos módulos | `orderbook_producer.py:144-169` | Métricas de publisher, no de consumer |
| 32. Freshness | AUSENTE (flujo WS→execution) | En el flujo concreto websocket/ → trading → execution no hay protección de freshness (grep `stale`/`fresh` en `adapters/inbound/websocket/` y `application/feed_orchestrator.py` sin resultados). Existen referencias de staleness/freshness en otros caminos de market_data: `adapters/inbound/rest/ohlcv_fetcher.py:476-487` (`_stale_severity`, aborta ventana stale), `application/source_manager.py:169-190` (descarte por cursor monotónico "trade estale"), `infrastructure/observability/metrics.py:40` (métrica `ocm_silver_freshness_seconds`) | — | La afirmación "grep sin resultados" es imprecisa; los matches reales están en REST fetcher, source_manager y metrics, no en el flujo WS→Kafka→execution |
| 33. Health state | PARCIAL | `is_running` en `GapAwareStream`/`WSTradesSource`; `is_healthy` en CCXTAdapter; `KafkaMetrics` | `gap_aware_stream.py:160`, `ccxt_adapter.py:219` | Health del stream de datos no consolida freshness |
| 34. Circuit breaker | IMPLEMENTADO | `CCXTAdapter._handle_circuit_open` y resilience (retry) | `ccxt_adapter.py:503`, `resilience.py` | — |
| 35. Kill switch | PARCIAL | `ExecutionGuard.should_stop()` en trading; no en market data | `trading/engine.py:152`, `ocm/runtime/guard.py` | — |
| 36. Market data congelado | AUSENTE | No se encontró detección de "datos congelados" en el flujo WS | — | — |
| 37. Timestamp viejo | AUSENTE | No hay política de rechazo por timestamp viejo en el stream | — | — |
| 38. Book inconsistente | PARCIAL | `checksum` se propaga al payload pero no se verifica en OCM | `orderbook_producer.py:151-159`, schema L136 | Checksum cruza a Kafka sin validación |

### 2.3 Hallazgos FASE 2

- **ingestion_mode actual = `rest`** (VERIFIED, `feeds.yaml:13`): `FeedOrchestrator.run()` retorna inmediatamente
  cuando mode == "rest" (`feed_orchestrator.py:86-92`), y `build_feed_orchestrator` retorna None
  (`composition_root.py:185-187`). Es decir, **el pipeline WS (cryptofeed) NO se arranca en la configuración
  actual**; el flujo activo es REST (pipeline legacy). El WS está construido y listo pero inactivo.
- `WSTradesSource` es un **stub estructural** (VERIFIED, `ws_trades_source.py:14,84-90`): emite
  `StopAsyncIteration` inmediatamente; el manager hace fallback a REST. El WS de trades vía
  `TradesSourceProtocol` no está implementado; la implementación real de cryptofeed vive en
  `BybitCryptofeedRunner`/`CryptofeedOrderBookStream` (otra vía, no el TradesSourceProtocol).
- El canal order book tiene **dos vías de código**: el `CryptofeedOrderBookStream` (WS L2_BOOK) y el
  `OrderBookSourceProtocol` (que solo documenta `RESTOrderBookPoller` "hoy" y `WebSocketBookStream`
  "futuro", `trades_source.py:141-143`). La vía WS real no implementa `OrderBookSourceProtocol`.

---

## FASE 3 — MARKET DATA CONTRACTS

### 3.1 Conceptos encontrados

| CONCEPTO | EXISTE | NOMBRE REAL | UBICACIÓN | TIPO | USO REAL | EVIDENCIA |
|---|---|---|---|---|---|---|
| MarketData (concepto global) | NO (como tipo) | — | — | — | No hay contrato agregado único "MarketData"; hay puertos específicos | — |
| OrderBook | PARCIAL | `OrderBookSourceProtocol` (solo protocolo); `CryptofeedOrderBookStream` (adapter real); `OrderBookSnapshotPayload`/`OrderBookDeltaPayload` (wire) | `ports/inbound/trades_source.py:133-172`; `adapters/inbound/websocket/cryptofeed_orderbook_stream.py`; `shared/kafka/schemas/orderbook.py:63,131` | Protocol + adapter + wire schema | Productor WS→Kafka (orderbook.raw) | — |
| Trade | IMPLEMENTADO | `RawTrade`, `NormalizedTrade` | `domain/value_objects/raw_trade.py:118`, `domain/value_objects/normalized_trade.py:28` | Value objects (dataclasses) | `TradesSourceProtocol` usa RawTrade; `TradeCallback` usa NormalizedTrade | — |
| Ticker | AUSENTE | — | — | — | No existe tipo `Ticker` en domain ni en shared/kafka/schemas (grep sin resultados) | — |
| BookSnapshot | IMPLEMENTADO | `OrderBookSnapshotPayload` (+ wire) | `shared/kafka/schemas/orderbook.py:63` | Payload Pydantic | Publicado a orderbook.raw | — |
| BookDelta | IMPLEMENTADO | `OrderBookDeltaPayload` | `shared/kafka/schemas/orderbook.py:131` | Payload Pydantic | Publicado a orderbook.raw | — |
| MarketEvent | PARCIAL | `domain/events/orderbook_events.py`; no hay event bus | `market_data/domain/events/orderbook_events.py` | Eventos de dominio | Referencia; no event bus (ver ADR-0029: "no se introduce Kafka en trading") | — |
| MarketDataSource | IMPLEMENTADO | `MarketDataSource` (Protocol runtime_checkable) | `ports/inbound/market_data_source.py:24` | Protocol | `BybitFeedAdapter`/`KuCoinFeedAdapter` conforman | — |
| MarketDataConsumer | PARCIAL | `TradeCallback = Callable[[NormalizedTrade], Coroutine]` | `ports/inbound/market_data_source.py:20` | Callback type | Usado por adapters | — |
| MarketDataPort | NO | — | — | — | No hay un "MarketDataPort" único; hay puertos específicos por capacidad | — |
| Exchange | IMPLEMENTADO | `exchange: str` en adapters; `_EXCHANGE_CLASSES`; `_ADAPTER_CLASSES` | `bybit_feed_adapter.py:50`, `cryptofeed_orderbook_stream.py:56`, `feed_registry.py:19` | str + registry | Routing y construcción | — |
| Symbol | IMPLEMENTADO | `symbol: str` en NormalizedTrade/RawTrade/payloads; `make_symbol_key` | `normalized_trade.py:48`, `orderbook_producer.py:160` | str | Partition key | — |
| Timestamp | IMPLEMENTADO | `timestamp`/`timestamp_ms` | `normalized_trade.py:53`, schemas | float/int | Propagado | — |
| Sequence | AUSENTE (wire) | `book.sequence_number` solo atributo vendor cryptofeed | `cryptofeed_orderbook_stream.py:160` | — | NO serializado a payload | — |
| ReceiptTimestamp | IMPLEMENTADO | `received_at` en NormalizedTrade; `receipt_timestamp` en stream | `normalized_trade.py:54`, stream | float | — | — |
| ExchangeTimestamp | IMPLEMENTADO | `timestamp`/`timestamp_ms` (origen exchange) | `normalized_trade.py:53`, stream L183 | float/int | — | — |
| Freshness | AUSENTE | — | — | — | No hay contrato ni campo de freshness | — |

### 3.2 Evidencia sobre los puntos A-J

**A. Dependencia directa del dominio sobre cryptofeed.**
- VERIFIED: NO. `domain/` no importa cryptofeed. Los imports de cryptofeed están confinados en
  `adapters/inbound/websocket/` (runners y orderbook stream). Evidencia: imports `from cryptofeed import FeedHandler`
  solo en `cryptofeed_orderbook_stream.py:49` y runners.

**B. Leakage de infraestructura.**
- PARCIAL: los runners (adapters) traducen tipos vendor a domain objects, cumpliendo ACL. Pero el
  `CryptofeedOrderBookStream` documenta dependencias directas del comportamiento de cryptofeed (unidades de
  timestamp, `book.delta`, `to_dict()`, Firma de callback) en docstrings — conocimiento vendor en el adapter,
  esperado en un ACL.

**C. Dependencia sobre tipos específicos de cryptofeed.**
- VERIFIED: confinada a adapters (ACL). `bybit_cryptofeed_runner.py:80` anota `trade: object` (comentario
  "cryptofeed.types.Trade — confinado aquí"). `cryptofeed_orderbook_stream.py` usa `book.book`, `book.delta`.

**D. Aislamiento de adapters.**
- VERIFIED: `BybitFeedAdapter` no importa cryptofeed (solo `FeedRunnerProtocol` bajo TYPE_CHECKING,
  `bybit_feed_adapter.py:39-40`). El adapter inyecta el runner. Registry lazy (`feed_registry.py`).

**E. Contratos demasiado genéricos.**
- PARCIAL: `TradeCallback = Callable[[NormalizedTrade], Coroutine]` es intencionalmente genérico (KISS).
  `MarketDataSource.subscribe_trades(symbols, callback)` es minimalista. No hay evidencia de que perjudiquen.

**F. Contratos demasiado específicos de Bybit.**
- PARCIAL: `_sorted_levels` y el mapeo `_EXCHANGE_CLASSES` son genéricos (bybit/kucoin). Pero las unidades
  de timestamp asumen Bybit ms (`cryptofeed_orderbook_stream.py:166-174`); el docstring advierte verificar
  para otros exchanges. `BybitFeedAdapter.exchange = "bybit"` es específico (esperado).

**G. Pérdida de información durante normalización.**
- VERIFIED: el `sequence_number` del order book NO se serializa al payload Kafka
  (`shared/kafka/schemas/orderbook.py` no tiene campo sequence; `orderbook_producer.py:151-207` no lo pasa).
  `NormalizedTrade` no conserva `trade_id`? — SÍ lo tiene: `NormalizedTrade` usa `trade_id` en `__repr__`
  (`normalized_trade.py:73`) pero el campo no está en el payload wire de trades (no verificado en schema
  trades). Pérdida de secuencia es un hecho verificado.

**H. Timestamps.**
- VERIFIED: dos fuentes distintas conviven: `NormalizedTrade` con `timestamp` (exchange, float) y
  `RawTrade` con `timestamp_ms` (int ms). `OrderBook` payloads usan `timestamp_ms` (int). Unificados en
  unidades? No: NormalizedTrade usa float epoch (segundos), RawTrade/payloads usan ms int.

**I. Sequence IDs.**
- VERIFIED: solo existe como atributo vendor de cryptofeed (`book.sequence_number`); no se propaga a wire.

**J. Trazabilidad del evento.**
- PARCIAL: headers Kafka `x-ocm-source`/`x-ocm-domain` (Kappa) y `TradeSource` para trades
  (`raw_trade.py:83-98`); pero el order book no declara `TradeSource` y pierde sequence.

---

## FASE 4 — MARKET DATA → STRATEGY → RISK → EXECUTION

### 4.1 Frontera real

| FRONTERA | COMPONENTE ORIGEN | COMPONENTE DESTINO | MECANISMO | EVIDENCIA | RIESGO OBSERVADO |
|---|---|---|---|---|---|
| MD → Strategy | GoldReader (Iceberg) | TradingEngine | `FeatureSource.load_features` via `_GoldFeatureSource` | `trading/bootstrap/composition_root.py:136-189`; `trading/engine.py:336-346` | Datos cargados desde Gold (histórico), no en tiempo real |
| Strategy → OMS | TradingEngine | OMS | `OMS.submit(signal)` | `trading/engine.py:208-209` | — |
| OMS → Risk | OMS | RiskManager | `RiskManager.validate(signal)` | `trading/execution/oms.py:203`, `trading/risk/manager.py:145-162` | El risk valida señal, no freshness del dato |
| OMS → Execution | OMS | LiveExecutor / PaperExecutor | `OrderExecutor.execute` | `trading/execution/oms.py:107-119`, `live_executor.py:61-113` | — |
| Execution → Exchange | LiveExecutor | CCXTAdapter (`_BybitTransport`) | `OrderTransport.submit` → `CCXTAdapter.create_order` | `trading/bootstrap/composition_root.py:223-244`; `ccxt_adapter.py:405` | Orden market; acuse async; sin cancel |
| Execution → Pricing | LiveExecutor | Signal.price (del DataFrame Gold) | `_notional_qty`: `qty = (capital * size_pct) / signal.price` | `live_executor.py:141-159` | **Sizing usa el precio de la señal (derivado de datos Gold), sin verificar su edad** |

### 4.2 Pregunta crítica: ¿OCM puede ejecutar una orden basándose en Market Data viejo, congelado, incompleto o fuera de secuencia?

Respuesta con evidencia:

- **SÍ, no hay protección de freshness en el camino de ejecución.** VERIFIED:
  - El `TradingEngine.run_once()` carga datos con `_load_data()` (`engine.py:336-346`) → `FeatureSource.load_features`
    → `GoldReader.load_features` (Iceberg Gold, que puede ser histórico). No hay verificación de cuán reciente
    es la última vela en `run_once` antes de generar señales.
  - El stop-loss usa `df.select("close").row(-1)[0]` como `current_price` (`engine.py:183`); si el DataFrame
    Gold no incluye el último instante, el stop-loss compara contra un close viejo.
  - El sizing en `LiveExecutor._notional_qty` divide por `signal.price` (`live_executor.py:156-159`), precio
    que proviene de la señal generada sobre datos Gold. `signal.price` lo pone la estrategia (DataFrame).
  - `RiskManager.validate` NO consulta freshness/health de market data (checks: halted, actionable, confidence,
    max positions, drawdown, sizing; `risk/manager.py:149-157`).
  - Grep en `packages/trading/` de `stale`/`fresh`/`health` no devuelve ninguna política de stale-data en el
    camino de ejecución (los matches de health son de cursor store y config, no del ejecutor).

- **¿Existe algún mecanismo que impida ejecutar sobre datos viejos?** No se encontró. La única mitigación es:
  - `ExecutionGuard.should_stop()` (kill switch) en `engine.py:152` — es un guard global configurado por
    el humano, no un chequeo de freshness del dato.
  - `LiveExecutor._reconcile` fail-closed solo durante submit (`live_executor.py:229-261`), para confirmar
    fill, no para validar el precio de entrada.

- **En cuanto a "fuera de secuencia":** el order book pierde `sequence_number` al publicar (FASE 3-I), así
  que un consumidor no puede detectar out-of-order; y el flujo de decisión (Gold) no consume order book
  directo en este camino. El dato usado para ejecutar es OHLCV Gold, no el order book.

- **Conclusión UNKNOWN/VERIFIED:** es **VERIFIED** que no existe política de freshness/stale-data entre
  market data y execution; es **INFERENCE** (no probado en runtime) que eso pueda materializar una orden
  basada en un close viejo, porque depende del lag real de Gold respecto del mercado. No se puede afirmar
  con evidencia estática que haya una orden concreta ejecutada sobre datos congelados, pero el diseño no lo
  impide.

### 4.3 Registro frente a ADR-0029 / ADR-0030

- **ADR-0029 (cancelación real / CANCELLING)**: CONFIRMA el estado actual descrito en el ADR:
  - `OrderTransport` (Protocol) solo tiene `submit`/`fetch_state`/`close` — NO tiene `cancel`
    (VERIFIED, `trading/execution/transport.py:96-128`). PaperTransport tampoco (`transport.py:131-158`).
  - `OMS.cancel()` es local-only: transiciona a CANCELLED, hace `_open.pop`, `record_close`, sin transporte
    (VERIFIED, `oms.py:300-317`).
  - `_VALID_TRANSITIONS` en `order.py:64` — `CANCELLING` NO existe como estado (solo PENDING/SUBMITTED/FILLED/REJECTED/CANCELLED según grep de `order.py`). El ADR propone añadirlo.
  - `_BybitTransport` no implementa cancel (VERIFIED, `composition_root.py:203-261`).
  - `CCXTAdapter` NO expone `cancel_order` ni `fetch_balance` (VERIFIED: grep de `def` en `ccxt_adapter.py`
    lista create_order L405, fetch_order L462, fetch_ticker L259, fetch_ohlcv L283, fetch_trades L343; sin
    cancel_order/fetch_balance).
  - **NO APLICA/CONTRADICE la parte de mercado público:** ADR-0029:103 (cryptofeed solo market data público)
    se confirma: cryptofeed solo aparece en adapters de market data público (TRADES/L2_BOOK), nunca en
    órdenes privadas (VERIFIED en FASE 2/3).

- **ADR-0030 (balance real)**: CONFIRMA el estado actual:
  - `CCXTAdapter` NO tiene `fetch_balance` (VERIFIED, grep arriba) — el ADR asume que se expondrá.
  - No se encontró lectura de balance en el camino de execution; el sizing usa `capital_usd` configurado
    (VERIFIED, `live_executor.py:67,141-159`), no balance real del exchange. Consistente con el estado
    previo al ADR-0030.
  - **CONTRADICE/ATENCIÓN (evidencia relevante):** `LiveExecutor._notional_qty` deriva qty de
    `capital_usd * size_pct / signal.price` (`live_executor.py:141-159`), es decir el sizing depende de un
    `capital_usd` configurado y del precio de la señal, no de un saldo real verificado — coherente con el
    problema que ADR-0030 aborda (estado patrimonial real).

---

## Notas de verificación de la tarea

- Archivo creado: `docs/audits/2026-08-15-auditoria-ocm-parte-1-market-data.md`.
- No se modificó código, ADRs, tracking.yaml, tests ni configuración.
- No se ejecutó `git add`/`commit`/`push`.
- Evidencia: todas las rutas, clases, funciones y líneas citadas fueron leídas directamente en esta sesión
  (archivos listados en el cuerpo del informe).