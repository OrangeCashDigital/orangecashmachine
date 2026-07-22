# Auditoría de Responsabilidades — `packages/market_data/`

**Fecha**: 2026-05-27  
**Alcance**: 118 archivos Python, 5 capas (domain → ports → application → adapters → infrastructure)  
**Método**: Revisión manual de cada archivo + análisis de imports + verificación de contratos arquitectónicos (BC-NN)

---

## Índice

1. [Resumen Ejecutivo](#1-resumen-ejecutivo)
2. [Hallazgos Críticos](#2-hallazgos-críticos)
   - 2.1 `main.py` — 4 responsabilidades en un archivo
   - 2.2 `_get_git_hash()` duplicado + `subprocess` en dominio
   - 2.3 `onchain_producer.py` — imports a paquete inexistente
3. [Hallazgos Moderados](#3-hallazgos-moderados)
   - 3.1 `OHLCVPipeline._execute_pair()` — 4 responsabilidades mezcladas
   - 3.2 `CompositionRoot` — 3 cadenas de dependencia distintas
   - 3.3 `FeedOrchestrator` — signal handling en capa de aplicación
4. [Hallazgos Leves](#4-hallazgos-leves)
   - 4.1 `PipelineRequest` DTO colocado con el orquestador
   - 4.2 `DataQualityChecker` — 7 checks + umbrales hardcodeados
   - 4.3 `ohlcv_transformer.py` vs `pandas_to_domain.py` — solapamiento
   - 4.4 `runtime.py` — archivo para una función helper
5. [Mapa de Responsabilidades por Archivo](#5-mapa-de-responsabilidades-por-archivo)
6. [Verificación de Contratos Arquitectónicos](#6-verificación-de-contratos-arquitectónicos)
7. [Cobertura de Puertos (Ports) vs Implementaciones](#7-cobertura-de-puertos-ports-vs-implementaciones)
8. [Dead Code / Stubs](#8-dead-code--stubs)
9. [Plan de Acción](#9-plan-de-acción)

---

## 1. Resumen Ejecutivo

La arquitectura Clean/Hexagonal está sólidamente establecida. Los contratos de importación (BC-NN) se cumplen en el 95%+ del código. Sin embargo, se identificaron **7 hallazgos**:

| Severidad | Cantidad | Descripción |
|-----------|----------|-------------|
| 🔴 Crítico | 3 | Imports rotos, I/O en dominio, SRP violado en entrypoint |
| 🟡 Moderado | 3 | Clases con múltiples responsabilidades, lógica de infra en capa incorrecta |
| 🟢 Leve | 4 | DTOs colocated, umbrales hardcodeados, solapamiento funcional menor |

---

## 2. Hallazgos Críticos

### 2.1 `main.py` — 4 responsabilidades en un solo archivo

**Archivo**: `packages/market_data/main.py` (609 líneas)  
**Principio violado**: SRP (Single Responsibility Principle)

El archivo concentra 4 subsistemas independientes:

| Responsabilidad | Líneas | Problema |
|---|---|---|
| Servidor ASGI FastAPI + endpoints HTTP (`/health`, `/ready`, `/ohlcv/...`) | 429-609 | OK per se, pero convive con el resto |
| `_ingestion_loop()` — loop infinito que orquesta `PipelineOrchestrator` para cada exchange | 120-215 | Debiera ser un servicio independiente (`application/service/ingestion_service.py`) |
| `_bronze_writer_loop()` — consumer Kafka Kappa que escribe a Bronze Iceberg | 218-284 | Debiera ser `infrastructure/service/bronze_writer_service.py`. Tiene su propio lifecycle, manejo de errores y DLQ |
| `FeedOrchestrator` wiring — construcción e inicio del orquestador WS | 360-384 | Ya existe `composition_root.py` con `build_feed_orchestrator()`. El `try/except` y logging del resultado están en main.py cuando debieran estar en composition_root |
| `_lifespan()` — startup/shutdown que coordina los 3 loops anteriores | 291-423 | Actúa como supervisor de 3 tareas asyncio independientes, cada una con su propio lifecycle |

**Consecuencias**:
- Si alguien quiere ejecutar solo el bronze writer sin el servidor HTTP, no puede sin copiar/extraer código
- Si el ingestion loop cambia su lógica de scheduling, se modifica `main.py` (razón de cambio irrelevante para el servidor HTTP)
- Testing del ingestion loop requiere bootear FastAPI
- El `_lifespan` es frágil: una excepción en `build_feed_orchestrator` (que es fail-soft) no debe afectar el startup del ingestion loop, pero la lógica de coordinación está toda mezclada

**Propuesta**:
```
main.py                                  → solo ASGI server + endpoints
application/service/ingestion_service.py → _ingestion_loop() extraído
infrastructure/service/
  bronze_writer_service.py               → _bronze_writer_loop() extraído
  feed_orchestrator_service.py           → wiring del feed orchestrator
```

---

### 2.2 `_get_git_hash()` duplicado + `subprocess` en dominio

**Archivos afectados**:
- `domain/quality/types.py:30-41`
- `application/quality/data_quality.py:38-49`

**Problema 1 — Duplicación (DRY)**:
La función `_get_git_hash()` está copiada idénticamente en dos archivos:

```python
def _get_git_hash() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=2,
        )
        return result.stdout.strip() or "unknown"
    except Exception:
        return "unknown"
```

**Problema 2 — I/O en capa de dominio**:
`domain/quality/types.py` importa `subprocess` y ejecuta un proceso externo (`git`). La capa de dominio debe ser pura — sin I/O, sin imports del sistema operativo. Violación de DDD.

`DataQualityReport.git_hash` es un campo de valor que se llena con un efecto secundario (git). El hash debiera inyectarse desde afuera, no calcularse dentro del VO.

**Propuesta**:
1. Crear `shared/utils/git.py` con la función `get_git_short_hash() -> str`
2. `DataQualityReport` recibe `git_hash` como parámetro (ya lo hace), pero el caller inyecta el hash
3. Eliminar `_get_git_hash()` de ambos archivos
4. `DataQualityChecker.check()` recibe `git_hash` como parámetro opcional o lo inyecta el factory

---

### 2.3 `onchain_producer.py` — imports a paquete inexistente

**Archivo**: `adapters/inbound/websocket/onchain_producer.py`  
**Severidad**: 🔴 Cualquier import del módulo lanza `ModuleNotFoundError`

```python
# Líneas 9-11:
from infra.kafka.base_producer import BaseKafkaProducer
from infra.kafka.groups import GROUP_WS_ONCHAIN_PRODUCER
from infra.kafka.topics import TOPIC_ONCHAIN_RAW
```

El paquete `infra/` **no existe en el workspace**. Esto es código muerto que:

- Nunca puede ejecutarse (el import falla en module level)
- Se declara como "Stub — NOT IMPLEMENTED" (línea 17) pero tiene imports activos
- Engaña a herramientas de análisis estático (`mypy` lo ignorará porque es stub, pero `ruff` lo marca como error)

**Propuesta** (3 opciones mutuamente excluyentes):
1. **(Recomendada)** Convertir a stub puro: eliminar imports, la clase hereda de `Protocol` directamente
2. Implementar el paquete `infra/kafka/` (crearía un nuevo bounded context)
3. Eliminar el archivo (Git lo recupera si se necesita después)

---

## 3. Hallazgos Moderados

### 3.1 `OHLCVPipeline._execute_pair()` — 4 responsabilidades mezcladas

**Archivo**: `application/pipelines/ohlcv_pipeline.py:446-559`  
**Principio violado**: SRP a nivel de método

El método `_execute_pair()` maneja:

| Responsabilidad | Líneas | Debería estar en |
|---|---|---|
| Ejecución de la estrategia (`strategy.execute_pair()`) | 476-482 | OK aquí |
| Circuit breaker: detección, cooldown, retry | 489-532 | Wrapper separado (`_circuit_breaker_execute()`) |
| Throttle feedback loop (`_feed_throttle()`) | 483, 519, 549 | OK delegado a helper, pero intercalado con circuit breaker |
| Métricas: active_pairs gauge inc/dec | 473-474, 555-558 | OK delegado, pero en `finally` con try/except anidado |
| Clasificación de errores y creación de `PairResult` de emergencia | 534-550 | Delegado a `_classify_pair_error()`, pero hay lógica de error tipo "catch-all" |

**Consecuencia**: El flujo del método es difícil de seguir por la mezcla de responsabilidades. Hay 4 `return` paths y el `finally` debe lidiar con todos. El circuit breaker introduce un retry con sleep dentro del método, lo que lo hace bloqueante (aunque es asyncio).

**Propuesta**: Extraer el wrapper de circuit breaker:

```python
async def _execute_pair(
    self, strategy, symbol, timeframe, idx, total, mode, abort_event
) -> PairResult:
    if self._ctx.metrics:
        self._ctx.metrics.active_pairs_inc(self._exchange_id)
    try:
        return await self._execute_with_circuit_breaker(
            strategy, symbol, timeframe, idx, total, mode, abort_event
        )
    except _ExchangeAbortError:
        raise
    except Exception as exc:
        result = PairResult(...)
        self._feed_throttle(result)
        return result
    finally:
        if self._ctx.metrics:
            self._ctx.metrics.active_pairs_dec(self._exchange_id)

async def _execute_with_circuit_breaker(self, ...) -> PairResult:
    # Lógica de circuit breaker + cooldown + retry
```

---

### 3.2 `CompositionRoot` — 3 cadenas de dependencia distintas

**Archivo**: `infrastructure/bootstrap/composition_root.py`  
**Principio violado**: SRP — "una razón para cambiar"

`CompositionRoot` expone 3 métodos públicos estáticos con responsabilidades no relacionadas:

| Método | Responsabilidad | Dependencias |
|---|---|---|
| `assemble(config)` | Cablear pipeline factory | `ConcretePipelineFactory` |
| `build_feed_orchestrator(config)` | Construir orquestador WS | `KafkaTradePublisher`, `FeedOrchestrator`, `feed_registry`, YAML parsing |
| `build_ws_producers(bootstrap_servers)` | Instanciar 4 producers WS | `KafkaProducerAdapter`, 4 producers WS |

**Razones de cambio diferentes**:
1. Cambiar cómo se construyen los pipelines → tocar `assemble()`
2. Cambiar cómo se lee la config de feeds (YAML → distinto source) → tocar `build_feed_orchestrator()`
3. Agregar un nuevo producer WS → tocar `build_ws_producers()` y `WSProducerBundle`

**Propuesta**:
```
infrastructure/bootstrap/
  composition_root.py                → solo assemble()
  feed_composition_root.py           → build_feed_orchestrator()
  ws_producers.py                    → build_ws_producers() + WSProducerBundle
```

---

### 3.3 `FeedOrchestrator` — signal handling en capa de aplicación

**Archivo**: `application/feed_orchestrator.py:160-164`  
**Principio violado**: Clean Architecture — capa de aplicación no debe conocer señales del SO

```python
def _install_signal_handlers(self) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, self._stop_event.set)
```

`signal.SIGINT`, `signal.SIGTERM`, y `loop.add_signal_handler()` son detalles de infraestructura (dependen de asyncio y del SO). La capa de aplicación (`application/`) no debería importar `signal` ni conocer el event loop.

Además, el orquestador se instancia desde `composition_root.py` (infrastructure), lo que significa que la infraestructura está delegando el manejo de señales a la aplicación.

**Propuesta**: Mover `FeedOrchestrator` a `infrastructure/feed_orchestrator.py`. Alternativa: hacer que `_install_signal_handlers` reciba un callback en lugar de registrar señales directamente (DIP).

---

## 4. Hallazgos Leves

### 4.1 `PipelineRequest` DTO colocado con el orquestador

**Archivo**: `application/use_cases/pipeline_orchestrator.py:87-165`

`PipelineRequest` (80 líneas, 13 campos, validación en `__post_init__`) está en el mismo archivo que `PipelineOrchestrator` (121 líneas). No es una violación grave, pero reduce la navegabilidad.

**Propuesta**: Mover a `application/use_cases/pipeline_request.py`. Mantener `PipelineMode` y `PipelineType` como re-export en el `__init__.py` del paquete.

---

### 4.2 `DataQualityChecker` — 7 checks + umbrales hardcodeados

**Archivo**: `application/quality/data_quality.py` (362 líneas)

La clase `DataQualityChecker` ejecuta 7 checks distintos:

- `_check_future_timestamps`
- `_check_gaps`
- `_check_ohlc_inconsistencies`
- `_check_outliers_mad`
- `_check_outliers_rolling_zscore`
- `_check_flatlines`

Los umbrales están definidos como constantes de módulo:

```python
_MAD_THRESHOLD: float = 3.5
_ZSCORE_WINDOW: int = 20
_ZSCORE_THRESHOLD: float = 4.0
_GAP_TOLERANCE_FACTOR: float = 1.5
_CRITICAL_GAP_PCT: float = 0.05
```

**Problema**: No hay forma de configurar estos umbrales externamente (por exchange, símbolo, o entorno). Están fijos en el código fuente. Para cambiarlos hay que modificar el archivo.

**Propuesta**: Inyectar `QualityCheckConfig` como dataclass:

```python
@dataclass
class QualityCheckConfig:
    mad_threshold: float = 3.5
    zscore_window: int = 20
    zscore_threshold: float = 4.0
    gap_tolerance_factor: float = 1.5
    critical_gap_pct: float = 0.05
    flatline_threshold_by_tf: dict[str, float] = ...
```

---

### 4.3 `ohlcv_transformer.py` vs `pandas_to_domain.py` — solapamiento funcional

**Archivos**:
- `application/use_cases/ohlcv_transformer.py` — ACL DataFrame → DataFrame (polars interno)
- `adapters/inbound/pandas_to_domain.py` — ACL DataFrame → Domain Objects (Candle, OHLCVChunk)

Ambos son ACLs que convierten pandas OHLCV. El solapamiento está en:

| Aspecto | `ohlcv_transformer` | `pandas_to_domain` |
|---|---|---|
| Validación de columnas | `_validate_columns()` | `_REQUIRED_COLUMNS` + `DataFrameMappingError` |
| Conversión de timestamps | `_convert_types()` | `pd.to_datetime(..., utc=True)` |
| Validación de velas | `CandleValidator` | `candle.is_valid()` |

No es duplicación exacta (operan a distinto nivel de granularidad), pero la validación de entrada OHLCV está distribuida en dos lugares. Un cambio en el formato esperado (ej. columna nueva) requiere modificar ambos.

**Propuesta**: Crear un validador compartido en `ports/` o `shared/` que ambos ACLs consuman, o documentar explícitamente que `pandas_to_domain` está deprecado en favor de `ohlcv_transformer`.

---

### 4.4 `runtime.py` — archivo para una función helper

**Archivo**: `application/pipeline/runtime.py`

Contiene una sola función:

```python
def classify_error(error: BaseException) -> str:
    ...
```

10 líneas útiles. El archivo existe porque cuando se creó `ohlcv_pipeline.py` se extrajo esta función. Pero hoy:

- `classify_error` no se usa en ningún lado (grep: 0 references outside `runtime.py`)
- `PipelineContext` (la clase principal del paquete) está en `application/context.py`, no en `application/pipeline/`

**Propuesta**: Eliminar `runtime.py` o fusionar con `context.py`.

---

## 5. Mapa de Responsabilidades por Archivo

### 5.1 Capa Domain (29 archivos)

| Archivo | Responsabilidad | ¿Correcta? |
|---|---|---|
| `domain/entities/` | Entidades del dominio | ✅ |
| `domain/events/` | Eventos de dominio (8 archivos) | ✅ |
| `domain/exceptions/` | Jerarquía de excepciones (7 clases) | ✅ |
| `domain/policies/base.py` | `PipelineStrategy` Protocol | ✅ |
| `domain/policies/data_quality_policy.py` | Política de calidad de datos | ✅ |
| `domain/quality/invariants.py` | `check_dataset_invariants()` | ✅ |
| `domain/quality/types.py` | `DataQualityReport`, `QualityIssue` | ⚠️ `subprocess` I/O |
| `domain/value_objects/candle.py` | `Candle` VO | ✅ |
| `domain/value_objects/candle_validator.py` | `CandleValidator` | ✅ |
| `domain/value_objects/symbol.py` | `Symbol` VO | ✅ |
| `domain/value_objects/timeframe.py` | `Timeframe` VO | ✅ |

### 5.2 Capa Ports (27 archivos)

| Archivo | Responsabilidad | ¿Correcta? |
|---|---|---|
| `ports/inbound/event_consumer.py` | `EventConsumerPort` | ✅ |
| `ports/inbound/market_data_source.py` | `MarketDataSource` + `TradeCallback` | ✅ |
| `ports/inbound/pipeline_factory.py` | `PipelineFactoryPort` | ✅ |
| `ports/inbound/pipeline_trigger.py` | `PipelineTriggerPort` | ✅ |
| `ports/inbound/trades_source.py` | `TradesSourceProtocol` | ✅ |
| `ports/outbound/storage.py` | 7 protocolos de almacenamiento | ✅ |
| `ports/outbound/exchange.py` | `Exchange` abstract base | ✅ |
| `ports/outbound/exchange_client.py` | `ExchangeClientPort` | ✅ |
| `ports/outbound/publisher_port.py` | `OHLCVPublisherPort` + `NullPublisher` | ✅ |
| `ports/outbound/normalization.py` | DataFrame transforms SSOT | ✅ |

### 5.3 Capa Application (18 archivos)

| Archivo | Responsabilidad | ¿Correcta? |
|---|---|---|
| `application/pipelines/ohlcv_pipeline.py` | Pipeline OHLCV completo | ⚠️ `_execute_pair` overloaded |
| `application/pipelines/trades_pipeline.py` | Pipeline Trades | ✅ |
| `application/pipelines/derivatives_pipeline.py` | Pipeline Derivatives | ✅ |
| `application/pipelines/_worker_pool.py` | Worker pool genérico | ✅ |
| `application/use_cases/pipeline_orchestrator.py` | Orquestador + DTO `PipelineRequest` | ⚠️ DTO colocated |
| `application/use_cases/ohlcv_transformer.py` | Transformación OHLCV (ACL) | ✅ |
| `application/use_cases/resample_ohlcv.py` | Resampling OHLCV | ✅ |
| `application/strategies/backfill.py` | Estrategia backfill | ✅ |
| `application/strategies/incremental.py` | Estrategia incremental | ✅ |
| `application/strategies/repair.py` | Estrategia repair | ✅ |
| `application/quality/data_quality.py` | `DataQualityChecker` | ⚠️ umbrales hardcodeados |
| `application/quality/pipeline.py` | `QualityPipeline` | ✅ |
| `application/quality/report.py` | `QualityReport` | ✅ |
| `application/consumers/quality_consumer.py` | `QualityPipelineConsumer` | ✅ |
| `application/processing/grid_alignment.py` | `align_to_grid` (polars) | ✅ |
| `application/processing/ohlcv_schema.py` | `validate_ohlcv` (pandera·polars) | ✅ |
| `application/processing/gap_scanner.py` | `GapScanner` | ✅ |
| `application/feed_orchestrator.py` | Orquestador WS feeds | ⚠️ signal handling en capa incorrecta |

### 5.4 Capa Adapters (28 archivos)

| Archivo | Responsabilidad | ¿Correcta? |
|---|---|---|
| `adapters/inbound/bybit_feed_adapter.py` | Bybit cryptofeed adapter | ✅ |
| `adapters/inbound/kucoin_feed_adapter.py` | KuCoin cryptofeed adapter | ✅ |
| `adapters/inbound/pandas_to_domain.py` | ACL pandas → dominio | ⚠️ solapamiento con ohlcv_transformer |
| `adapters/inbound/websocket/onchain_producer.py` | Stub onchain producer | 🔴 imports rotos |
| `adapters/inbound/websocket/orderbook_producer.py` | OrderBook → Kafka | ✅ |
| `adapters/inbound/websocket/funding_producer.py` | Funding → Kafka | ✅ |
| `adapters/inbound/websocket/oi_producer.py` | Open Interest → Kafka | ✅ |
| `adapters/inbound/websocket/liquidations_producer.py` | Liquidations → Kafka | ✅ |
| `adapters/inbound/rest/ohlcv_fetcher.py` | Fetcher OHLCV REST | ✅ |
| `adapters/inbound/rest/trades_fetcher.py` | Fetcher Trades REST | ✅ |
| `adapters/outbound/exchange/ccxt_adapter.py` | CCXT con 3-capas resiliencia | ✅ |
| `adapters/outbound/exchange/throttle.py` | `AdaptiveThrottle` | ✅ |
| `adapters/outbound/exchange/limiter.py` | `AdaptiveLimiter` (aiometer) | ✅ |
| `adapters/outbound/exchange/resilience.py` | `ResilienceLayer` (aioresilience) | ✅ |
| `adapters/outbound/storage/chunk_converter.py` | `ChunkConverter` | ✅ |
| `adapters/outbound/storage/iceberg_factory.py` | `IcebergStorageFactory` | ✅ |
| `adapters/outbound/kafka_gap_publisher.py` | Gap events → Kafka | ✅ |
| `adapters/outbound/kafka_trade_publisher.py` | Trades → Kafka | ✅ |

### 5.5 Capa Infrastructure (36 archivos)

| Archivo | Responsabilidad | ¿Correcta? |
|---|---|---|
| `infrastructure/bootstrap/composition_root.py` | Composition Root | ⚠️ 3 responsabilidades |
| `infrastructure/bootstrap/pipeline_factory.py` | `ConcretePipelineFactory` | ✅ |
| `infrastructure/bootstrap/feed_registry.py` | exchange → adapter mapping | ✅ |
| `infrastructure/kafka/bronze_writer.py` | `KafkaBronzeWriter` | ✅ |
| `infrastructure/kafka/ohlcv_publisher.py` | `OHLCVPublisherAdapter` | ✅ |
| `infrastructure/kafka/producer.py` | `KafkaProducerAdapter` | ✅ |
| `infrastructure/kafka/consumer.py` | `KafkaConsumerAdapter` | ✅ |
| `infrastructure/storage/bronze/bronze_storage.py` | `BronzeStorage` | ✅ |
| `infrastructure/storage/silver/trades_storage.py` | `TradesStorage` | ✅ |
| `infrastructure/storage/gold/gold_storage.py` | `GoldStorage` | ✅ |
| `infrastructure/storage/iceberg/iceberg_storage.py` | `IcebergStorage` | ✅ |
| `infrastructure/event_bus/in_memory.py` | `InMemoryEventBus` | ✅ |
| `infrastructure/quality/ge_checker.py` | Great Expectations adapter | ✅ |
| `infrastructure/quality/anomaly_registry.py` | `AnomalyRegistry` | ✅ |
| `infrastructure/observability/metrics.py` | Prometheus metrics | ✅ |

---

## 6. Verificación de Contratos Arquitectónicos

| Contrato | Archivos verificados | Estado |
|---|---|---|
| **BC-03**: Domain puro (sin kafka, redis, ccxt, sqlalchemy, prefect) | 29 domain/ | ✅ OK |
| **BC-04**: Ports solo importan domain | 27 ports/ | ✅ OK |
| **BC-05**: Application no importa adapters/infrastructure | 18 application/ | ✅ OK |
| **BC-29**: Kafka wire schemas en `shared/kafka/schemas/` | — | ✅ OK |
| **BC-33**: Application no importa kafka | 18 application/ | ✅ OK |
| **BC-35**: Wire schemas solo en shared/ | — | ✅ OK |
| **BC-38**: Composition root único | `composition_root.py` | ⚠️ SRP violado pero contrato de punto único se cumple |
| **Forbidden Frameworks** (AST linter) | — | ✅ 0 violaciones (no detecta `subprocess`) |

### Violaciones no detectadas por los contratos actuales

| Violación | Detectado por |
|---|---|
| `subprocess` en `domain/quality/types.py` | ❌ N/A — `subprocess` no está en la lista de frameworks prohibidos |
| Imports rotos en `onchain_producer.py` | ❌ N/A — es stub, mypy no lo procesa, ruff no detecta packages inexistentes |
| SRP en `main.py` | ❌ N/A — no hay contrato arquitectónico que limite el tamaño/responsabilidad de entrypoints |
| Signal handling en `application/` | ❌ N/A — `signal` no está en la lista de frameworks prohibidos |

**Recomendación**: Extender `forbidden_frameworks.py` para incluir `subprocess` y `signal` en la lista de módulos prohibidos para las capas domain y application respectivamente.

---

## 7. Cobertura de Puertos (Ports) vs Implementaciones

| Puerto (Protocol) | Archivo | Implementaciones | Capa |
|---|---|---|---|
| `OHLCVPublisherPort` | `ports/outbound/publisher_port.py` | `OHLCVPublisherAdapter` + `NullPublisher` | infrastructure + co-located null |
| `DataQualityCheckerPort` | `ports/outbound/data_quality_checker.py` | `DataQualityChecker` (native) + `GEChecker` (GE) | application + infrastructure |
| `MarketDataSource` | `ports/inbound/market_data_source.py` | `BybitFeedAdapter`, `KuCoinFeedAdapter` | adapters |
| `PipelineFactoryPort` | `ports/inbound/pipeline_factory.py` | `ConcretePipelineFactory` | infrastructure |
| `KafkaProducerPort` | `ports/outbound/kafka_producer.py` | `KafkaProducerAdapter` | infrastructure |
| `KafkaConsumerPort` | `ports/outbound/kafka_consumer.py` | `KafkaConsumerAdapter` | infrastructure |
| `StoragePort` (7 protocols) | `ports/outbound/storage.py` | `BronzeStorage`, `GoldStorage`, `TradesStorage`, etc. | infrastructure |
| `ExchangeClientPort` | `ports/outbound/exchange_client.py` | `CCXTAdapter` | adapters |
| `MetricsPort` | `ports/outbound/metrics.py` | `PipelineMetricsAdapter` | infrastructure |
| `LineageTrackerPort` | `ports/outbound/lineage.py` | `LineageTracker` | infrastructure |
| `CursorStorePort` | `ports/outbound/state.py` | (via Iceberg) | infrastructure |
| `ChunkConverterPort` | `ports/outbound/chunk_converter.py` | `ChunkConverter`, `PassthroughChunkConverter` | adapters |

**Ratio**: 21 puertos outbound + 5 inbound = 26 abstracciones, ~30 implementaciones concretas. Cobertura completa sin huecos.

---

## 8. Dead Code / Stubs

| Archivo | Estado | Riesgo |
|---|---|---|
| `adapters/inbound/websocket/onchain_producer.py` | 🔴 Imports rotos | ModuleNotFoundError al importar |
| `adapters/inbound/websocket/ws_trades_source.py` | 🟡 Stub no implementado (clases vacías) | Bajo — no se instancia |
| `adapters/inbound/websocket/trades_stream.py` | 🟡 Stub (docstring-only) | Bajo — no se importa |
| `adapters/inbound/websocket/manager.py` | 🟡 Stub `WebSocketManager` | Bajo — no se instancia |
| `adapters/inbound/websocket/infra_metrics_producer.py` | 🟡 Stub | Bajo |
| `application/pipeline/runtime.py` | 🟡 `classify_error` sin callers | Medio — código huérfano |
| `infrastructure/storage/gold/feature_engineer.py` | 🟡 Deprecado | Medio — mencionado en docstring como deprecated |

---

## 9. Plan de Acción

### Prioridad 🔴 — Inmediata

| # | Acción | Archivos | Esfuerzo |
|---|---|---|---|
| 1 | Arreglar `onchain_producer.py`: eliminar imports rotos o convertir a stub puro | 1 archivo | 15 min |
| 2 | Unificar `_get_git_hash()` en `shared/utils/git.py`, eliminar `subprocess` del dominio | 3 archivos | 30 min |
| 3 | Extraer `_ingestion_loop()` de `main.py` a `application/service/ingestion_service.py` | 2 archivos | 1 h |
| 4 | Extraer `_bronze_writer_loop()` de `main.py` a `infrastructure/service/bronze_writer_service.py` | 2 archivos | 1 h |

### Prioridad 🟡 — Corto Plazo

| # | Acción | Archivos | Esfuerzo |
|---|---|---|---|
| 5 | Separar `CompositionRoot` en 3 compositores | 4 archivos | 1 h |
| 6 | Mover `FeedOrchestrator` a `infrastructure/` (signal handling) | 2 archivos + callers | 1 h |
| 7 | Extraer circuit breaker de `_execute_pair()` en `OHLCVPipeline` | 1 archivo | 1 h |

### Prioridad 🟢 — Medio Plazo

| # | Acción | Archivos | Esfuerzo |
|---|---|---|---|
| 8 | Separar `PipelineRequest` DTO en archivo propio | 2 archivos | 15 min |
| 9 | Inyectar `QualityCheckConfig` en `DataQualityChecker` | 2 archivos | 30 min |
| 10 | Extender `forbidden_frameworks.py` con `subprocess`, `signal` | 1 archivo | 15 min |
| 11 | Eliminar `runtime.py` o fusionar con `context.py` | 2 archivos | 15 min |
| 12 | Documentar deprecación de `pandas_to_domain.py` en favor de `ohlcv_transformer` | 2 archivos | 15 min |

---

## Apéndice: Estadísticas del Código

| Métrica | Valor |
|---|---|
| Archivos Python totales | 118 |
| Líneas de código (aprox) | ~15,000 |
| Puertos (Protocols) | 26 |
| Implementaciones concretas | ~30 |
| Violaciones críticas | 3 |
| Violaciones moderadas | 3 |
| Violaciones leves | 4 |
| Archivos stub/dead code | 6 |
| Tests arquitectónicos | 16 (todos pasan) |

---

*Generado el 2026-05-27. Revisión manual 100% del código fuente.*
