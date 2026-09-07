# AUDITORÍA ARQUITECTÓNICA FORENSE DE ORANGECASHMACHINE (OCM)
## DDD, Event-Driven, CQRS, Event Sourcing, Hexagonal y Medallion

**Fecha de Ejecución:** 2026-09-06  
**Tipo de Auditoría:** Inspección Forense Basada Exclusivamente en Evidencia del Repositorio  
**Modo:** READ-ONLY (Sin modificaciones de código, configuración, dependencias ni historial Git)  
**Ruta del Informe:** `docs/audits/AUDIT_ARCHITECTURE_DDD_EDA_CQRS_ES.md`  

---

## 1. INTRODUCCIÓN Y REGLA DE EVALUACIÓN

Esta auditoría forense analiza la arquitectura **real y ejecutable** de OrangeCashMachine (OCM), distinguiendo estrictamente entre:
- lo que está implementado en código fuente y configuraciones;
- lo que está declarado en la documentación o ADRs pero no cableado;
- lo que representan los patrones arquitectónicos bajo definiciones de ingeniería rigurosas.

### Principio Rector
> **Event-Driven** para transportar hechos y datos de mercado.  
> **CQRS** para separar operaciones de escritura y lectura cuando exista una razón de dominio.  
> **Event Sourcing** únicamente donde el historial de eventos sea la fuente de verdad necesaria.

### Sistema de Clasificación Obligatorio
Cada patrón auditado se evalúa bajo exactamente una de estas categorías:
- 🟢 **IMPLEMENTADO**: Existe evidencia completa y funcional en el código fuente.
- 🟡 **PARCIALMENTE IMPLEMENTADO**: Existen componentes o diseño estructural, pero su aplicación es incompleta, asimétrica o carece de integración operativa end-to-end.
- 🔴 **NO IMPLEMENTADO**: No existe código, contratos ni infraestructura que satisfaga la definición técnica del patrón.
- ⚪ **NO APLICA**: El patrón contradice los requerimientos de dominio o no aporta valor al contexto del sistema.
- ❓ **NO DETERMINABLE CON LA EVIDENCIA DISPONIBLE**: La evidencia es ambigua o insuficiente para emitir un juicio definitivo.

---

## 2. AUDITORÍA DE LOS 17 PATRONES ARQUITECTÓNICOS

Cada patrón se evalúa bajo la estructura forense obligatoria:
- **FACT:** Hecho observable en código y configuración.
- **EVIDENCE:** Archivo, símbolo, clase, función o línea concreta.
- **INTERPRETATION:** Qué patrón o principio técnico representa la evidencia.
- **LIMITATION:** Qué conclusiones NO pueden extraerse de esa evidencia.

---

### 1. DDD — Domain-Driven Design
**Clasificación:** 🟡 **PARCIALMENTE IMPLEMENTADO**

- **FACT:**  
  Existen Bounded Contexts formalmente delimitados (`market_data`, `trading`, `portfolio`). Se implementan Value Objects inmutables con invariantes numéricas estrictas (`Candle`, `Symbol`, `OHLCVChunk`, `PositionSnapshot`, `Settlement`). Existen Domain Services puros (`RiskManager`, `StopLossEvaluator`, `CandleValidator`). Sin embargo, en el subsistema `trading`, el `OMS` y el `TradingEngine` no operan como Aggregate Roots de DDD clásico con fronteras transaccionales atómicas de eventos; operan como servicios procedurales en memoria coordinados bajo `threading.RLock`. Además, el estado de posición se encuentra disperso y duplicado en 7 almacenes mutables (violación ARCH-001 y ARCH-010).
- **EVIDENCE:**  
  - Value Objects: `packages/market_data/domain/value_objects/candle.py:27`, `packages/portfolio/models/position.py:44`, `packages/trading/execution/settlement.py:32`.
  - Domain Services: `packages/trading/risk/manager.py:102`, `packages/trading/risk/stop_loss.py:60`.
  - Dispersión de estado mutable (ARCH-001): `packages/trading/analytics/trade_tracker.py:59` (`_open_positions`), `packages/trading/execution/oms.py:177,185` (`_orders`, `_entry_positions`), `packages/trading/risk/manager.py:122,135` (`_open_positions`, `_positions`), `packages/portfolio/infra/memory_store.py:33` (`_positions`).
- **INTERPRETATION:**  
  DDD táctico está presente y bien ejecutado a nivel de Value Objects y separación de servicios en `market_data` y `portfolio`. En `trading`, el diseño es predominantemente imperativo/procedural sin Aggregate Roots formales.
- **LIMITATION:**  
  Tener carpetas denominadas `domain/` y clases Value Object no demuestra una implementación completa de DDD si no existen Aggregates que garanticen consistencia transaccional y si no hay Domain Events en el flujo transaccional de órdenes.

---

### 2. Event-Driven Architecture (EDA)
**Clasificación:** 🟡 **PARCIALMENTE IMPLEMENTADO**

- **FACT:**  
  Kafka transporta streams continuos de datos de mercado (`ohlcv.raw`, `trades.raw`, `orderbook.raw`, `market.gaps`) que son consumidos por procesadores Kappa hacia Iceberg Bronze. No obstante, el motor de ejecución de trading (`TradingEngine.run_once()`) opera mediante un ciclo de polling síncrono paso a paso (`run_once`). La ejecución en el OMS y en `LiveExecutor` es síncrona y bloqueante contra CCXT. Los tópicos de señales y órdenes (`signals.raw`, `signals.approved`, `orders.filled`, `positions.opened`) están clasificados formalmente como `orphan` y `ASSUMED` en el registro de procedencia, careciendo de productores activos en producción.
- **EVIDENCE:**  
  - Pipeline de streaming activo: `packages/market_data/infrastructure/kafka/bronze_writer.py:25`, `packages/market_data/infrastructure/kafka/orderbook_bronze_writer.py:10`.
  - Ejecución síncrona del motor de trading: `packages/trading/engine.py:143` (`run_once`), `packages/trading/bootstrap/composition_root.py:305` (`run_ccxt_async`).
  - Esquemas huérfanos sin productor: `shared/kafka/provenance.py:54-78` (declara `SignalPayload`, `OrderFilledPayload`, `TradePayload` como `orphan` o `ASSUMED`).
- **INTERPRETATION:**  
  EDA está implementado exclusivamente en el plano de adquisición y streaming de datos de mercado (Kappa Data Pipeline). El plano de trading y ejecución es una arquitectura de procesamiento por lotes síncrono en memoria.
- **LIMITATION:**  
  La existencia de tópicos declarados en `shared/kafka/topics.py` no demuestra una arquitectura orientada a eventos en trading si los componentes se comunican síncronamente mediante callbacks en memoria (`OMS._on_fill`).

---

### 3. CQRS (Command Query Responsibility Segregation)
**Clasificación:** 🔴 **NO IMPLEMENTADO**

- **FACT:**  
  No existe separación física ni lógica entre un modelo de comandos (Write Model) y un modelo de consultas (Read Model) en ningún Bounded Context. En `trading`, el `OMS` escribe y lee sobre las mismas estructuras en memoria (`_orders`). En `portfolio`, `PortfolioService` muta (`open_position`) y consulta (`snapshot`) sobre el mismo `PositionStore` y el mismo modelo `PositionSnapshot`. En `market_data`, la jerarquía Bronze→Silver→Gold es un pipeline de datos ETL/Lakehouse, no una separación de comandos y queries de dominio.
- **EVIDENCE:**  
  - `packages/trading/execution/oms.py:177,403` (mismas estructuras `_orders` y `_open` para escritura y lectura).
  - `packages/portfolio/services/portfolio_service.py:89,274` (mismo `PositionStore` para `open_position` y `snapshot`).
- **INTERPRETATION:**  
  El sistema sigue una arquitectura de servicios CRUD/almacén de estado tradicional.
- **LIMITATION:**  
  Tener operaciones que leen (`load_features`, `get_order`) y operaciones que escriben (`save_ohlcv`, `submit`) NO constituye CQRS.

---

### 4. Event Sourcing
**Clasificación:** 🔴 **NO IMPLEMENTADO**  
*(Separadamente: **NO NECESARIO SEGÚN LA EVIDENCIA ACTUAL**)*

- **FACT:**  
  Ningún agregado del sistema almacena su estado como una secuencia inmutable de eventos de dominio en un Event Store. El estado de `PortfolioService` se persiste como un snapshot JSON completo en Redis mediante comandos `SET` bajo la clave `ocm:positions:{exchange}:{order_id}`. Si Redis se vacía o se pierde, no existe un mecanismo de replay de eventos de dominio para reconstruir la cartera. Kafka utiliza retención destructiva temporal (`delete 7d`, `delete 1h`) y no actúa como almacén inmutable. Iceberg almacena series temporales analíticas de mercado, no eventos de negocio de OCM.
- **EVIDENCE:**  
  - Persistencia por snapshot en Redis: `packages/portfolio/infra/redis_store.py:186-196` (`pipe.set(key, raw, ex=self._ttl)`).
  - Política de retención de Kafka: `shared/kafka/topics.py:214-230` (política `delete`, no compactación durable infinita).
  - Reconciliación de órdenes al rearranque: `packages/trading/execution/oms.py:654` (`manage_open_orders` consulta al exchange vía REST `fetch_open_orders`, no a un Event Store local).
- **INTERPRETATION:**  
  La arquitectura de persistencia se basa 100% en Estados/Snapshots (State-based persistence).
- **LIMITATION:**  
  La ausencia de Event Sourcing NO es una deficiencia técnica. Para un sistema de trading algorítmico mono-agente, la fuente externa de verdad financiera es el Exchange y la fuente interna es el snapshot con base de coste medio ponderado (WAC). Event Sourcing agregaría una complejidad masiva sin justificación de negocio.

---

### 5. Hexagonal Architecture / Ports & Adapters
**Clasificación:** 🟢 **IMPLEMENTADO**

- **FACT:**  
  Existe una separación estricta entre el núcleo de dominio/aplicación y los adaptadores de infraestructura. Los puertos inbound y outbound están formalizados mediante `typing.Protocol`. La dirección de dependencias está rigurosamente protegida por 50 contratos formales en `architecture_linter/importlinter.toml`, todos verificados mecánicamente como `KEPT` (0 violaciones). El Composition Root de cada Bounded Context es el único punto autorizado para instanciar adaptadores.
- **EVIDENCE:**  
  - Contratos de importación: `architecture_linter/importlinter.toml` (BC-01 a BC-55).
  - Ejecución mecánica: `uv run lint-imports` -> `Contracts: 50 kept, 0 broken`.
  - Puertos abstractos: `packages/portfolio/ports/position_store.py:26` (`PositionStore`), `packages/market_data/ports/outbound/storage.py:40` (`OHLCVStorage`), `shared/contracts/boundaries.py:23` (`SignalProtocol`).
  - Composition Roots: `packages/market_data/infrastructure/bootstrap/composition_root.py:30`, `packages/trading/bootstrap/composition_root.py:361`, `packages/portfolio/bootstrap/composition_root.py:20`.
- **INTERPRETATION:**  
  Implementación genuina y de alto rigor de Arquitectura Hexagonal (Ports & Adapters) con Inversión de Dependencias (DIP) verificada por tooling estático.
- **LIMITATION:**  
  Se detectaron duplicaciones nominales menores en algunos puertos por AST linter (ARCH-007: `OrderStatus`, `AnomalyRegistryPort`), pero no rompen la frontera hexagonal.

---

### 6. Medallion Architecture (Bronze / Silver / Gold)
**Clasificación:** 🟢 **IMPLEMENTADO**

- **FACT:**  
  El pipeline de almacenamiento de datos de mercado está estructurado en tres capas sobre Apache Iceberg:
  1. **Bronze (`bronze_storage.py`):** Ingesta cruda append-only con metadatos técnicos (`run_id`, `ingestion_ts`).
  2. **Silver (`iceberg_storage.py`, `trades_storage.py`):** Deduplicación, alineación a grid temporal y validación formal de schema pandera/polars.
  3. **Gold (`gold_storage.py`, `transformer.py`):** Enriquecimiento analítico de features y sobrescritura atómica por dataset mediante snapshots ACID de Iceberg.
- **EVIDENCE:**  
  - `packages/market_data/infrastructure/storage/bronze/bronze_storage.py:45`
  - `packages/market_data/infrastructure/storage/iceberg/iceberg_storage.py:65`
  - `packages/market_data/infrastructure/storage/gold/gold_storage.py:87`
  - `packages/market_data/application/use_cases/ohlcv_transformer.py:317`
- **INTERPRETATION:**  
  Arquitectura Medallion implementada en código para el almacenamiento y refinamiento de series temporales financieras.
- **LIMITATION:**  
  Distinguiendo lo arquitectónico de lo operacional: la ingesta de `orderbook` hacia Bronze ha presentado brechas en producción documentadas en el gate de calidad G9 (`OrderbookBronzeWriter` pendiente de consolidación completa).

---

### 7. Data Lakehouse
**Clasificación:** 🟢 **IMPLEMENTADO**

- **FACT:**  
  El repositorio utiliza Apache Iceberg (`pyiceberg`) como formato de tabla abierto sobre archivos columnares Parquet en `data_platform/iceberg_warehouse/`. Proporciona soporte transaccional ACID, snapshots inmutables, particionado por tiempo/instrumento, evolución de esquemas y capacidades de time travel.
- **EVIDENCE:**  
  - Catálogo Iceberg: `packages/market_data/infrastructure/storage/iceberg/catalog.py:20`.
  - Sobrescritura transaccional atómica: `packages/market_data/infrastructure/storage/gold/gold_storage.py:257` (`self._table.overwrite(...)`).
  - Almacén físico de tablas: `data_platform/iceberg_warehouse/`.
- **INTERPRETATION:**  
  Cumple estrictamente la definición técnica de Data Lakehouse (almacén de datos sobre object storage/disco que proporciona transacciones ACID, control de versiones y consultas de alto rendimiento sobre formatos abiertos).
- **LIMITATION:**  
  El motor de procesamiento es local (Python con Polars y PyArrow), no un cluster computacional distribuido (Spark/Trino), lo cual es coherente con los requisitos del sistema.

---

### 8. Feature Store
**Clasificación:** 🟡 **PARCIALMENTE IMPLEMENTADO**

- **FACT:**  
  `GoldStorage` y `GoldTransformer` computan un catálogo formal de 9 features técnicas (`log_return`, `return_1`, `volatility_20`, `vwap`, `high_low_spread`, `volume_z`, `price_range_pct`, `body_pct`, `is_suspect`) con versión fijada (`VERSION = "3.0.0"`), registro de linaje (`LineageTracker`) y soporte de recuperación histórica reproducible vía snapshots de Iceberg. Sin embargo, no existe un Feature Store en tiempo real (Online Serving Store de baja latencia <10ms en Redis o DynamoDB para consulta de entidad individual) ni APIs dinámicas de registro y monitorización de drift de features.
- **EVIDENCE:**  
  - Definición y cálculo de features: `packages/market_data/infrastructure/storage/gold/transformer.py:73-85,95-165`.
  - Persistencia de dataset en Gold: `packages/market_data/infrastructure/storage/gold/gold_storage.py:59-79`.
  - Lectura offline: `packages/market_data/adapters/outbound/storage/gold_reader.py:45`.
- **INTERPRETATION:**  
  Funciona como un **Offline Feature Store** analítico sobre Apache Iceberg para backtesting, research y alimentación del motor de estrategias.
- **LIMITATION:**  
  No debe catalogarse como un Feature Store completo de grado producción al carecer de la rama de Online Serving de baja latencia para inferencia en tiempo real.

---

### 9. Repository / Unit of Work
**Clasificación:** 🟡 **PARCIALMENTE IMPLEMENTADO**

- **FACT:**  
  Existen abstracciones tipo Repositorio/Store: `PositionStore` (`packages/portfolio/ports/position_store.py`) y `OHLCVStorage` (`packages/market_data/ports/outbound/storage.py`), implementadas por adaptadores de persistencia (`InMemoryPositionStore`, `RedisPositionStore`, `IcebergStorage`). Sin embargo, **el patrón Unit of Work NO existe** en ningún archivo del repositorio (0 referencias en todo el código fuente). Las transacciones son independientes y locales a cada llamada (commits atómicos de snapshot en Iceberg o pipelines de Redis).
- **EVIDENCE:**  
  - Puertos de Store: `packages/portfolio/ports/position_store.py:26`.
  - Ausencia de Unit of Work: búsqueda recursiva de `UnitOfWork` en `packages/` arroja 0 coincidencias.
- **INTERPRETATION:**  
  El patrón Repository está implementado parcialmente bajo la nomenclatura `Store`. El patrón Unit of Work no está implementado.
- **LIMITATION:**  
  La ausencia de Unit of Work es esperable al no haber persistencia relacional compleja ni transacciones multi-entidad coordinadas en una única base de datos.

---

### 10. Dependency Inversion / Dependency Injection (DIP / DI)
**Clasificación:** 🟢 **IMPLEMENTADO**

- **FACT:**  
  El principio de inversión de dependencias se cumple de forma exhaustiva. Los módulos de aplicación y dominio dependen de contratos (`Protocol`), nunca de implementaciones concretas de infraestructura. La inyección de dependencias es manual y determinista mediante constructores (Constructor Injection / Pure DI).
- **EVIDENCE:**  
  - Inyección en TradingEngine: `packages/trading/engine.py:114` (`strategy: BaseStrategy, oms: OMS, data_source: FeatureSource, portfolio: SupportsPositionSnapshot`).
  - Inyección en PortfolioService: `packages/portfolio/services/portfolio_service.py:63` (`store: PositionStore`).
  - Inyección en OMS: `packages/trading/execution/oms.py:158` (`risk_manager: RiskManager, executor: OrderExecutor`).
- **INTERPRETATION:**  
  DIP e Inyección de Dependencias están completamente implementados siguiendo las mejores prácticas de desacoplamiento de software.
- **LIMITATION:**  
  No se utiliza un framework de inyección automático, lo cual es deliberado para evitar sobre-ingeniería.

---

### 11. Composition Root
**Clasificación:** 🟢 **IMPLEMENTADO**

- **FACT:**  
  Cada Bounded Context posee un Composition Root único, centralizado y explícito que construye el grafo completo de dependencias antes de la ejecución. Ningún adaptador concreto es instanciado fuera de estos módulos, lo cual está verificado mecánicamente por los contratos `BC-38`, `BC-42`, `BC-43`, `BC-50` y `BC-55` de `import-linter`.
- **EVIDENCE:**  
  - `packages/market_data/infrastructure/bootstrap/composition_root.py:30` (`CompositionRoot.assemble()`).
  - `packages/trading/bootstrap/composition_root.py:361` (`TradingCompositionRoot.assemble_live()`, `assemble_paper()`).
  - `packages/portfolio/bootstrap/composition_root.py:20` (`PortfolioCompositionRoot.assemble()`).
  - `apps/research/data/composition_root.py:20` (`ResearchCompositionRoot`).
- **INTERPRETATION:**  
  Patrón Composition Root implementado con máxima rigurosidad arquitectónica.
- **LIMITATION:**  
  Ninguna.

---

### 12. Bounded Contexts
**Clasificación:** 🟢 **IMPLEMENTADO**

- **FACT:**  
  Los límites entre los Bounded Contexts `market_data`, `trading` y `portfolio` están estrictamente definidos y aplicados en CI. El cruce de fronteras está prohibido excepto por interfaces autorizadas en los Composition Roots (ej. BC-50 restringe el único punto donde `trading` puede interactuar con `market_data`).
- **EVIDENCE:**  
  - Contrato BC-10: `packages/market_data` no importa bounded contexts hermanos.
  - Contrato BC-12: `packages/trading/risk` no importa `trading/execution`.
  - Contrato BC-13: `packages/portfolio` no importa `trading/execution` ni `strategies`.
  - Contrato BC-50: `packages/trading` importa `market_data` únicamente desde `trading/bootstrap/composition_root.py`.
- **INTERPRETATION:**  
  Límites de Bounded Contexts formalmente definidos y blindados mediante herramientas de análisis estático en CI.
- **LIMITATION:**  
  A pesar del aislamiento de paquetes, el linter de arquitectura detectó duplicación de estado mutable conceptual entre los contextos (ARCH-001/ARCH-010).

---

### 13. Domain Events
**Clasificación:** 🟡 **PARCIALMENTE IMPLEMENTADO**

- **FACT:**  
  Existen clases Domain Events bien modeladas en `market_data/domain/events/` (`CandleReceived`, `OHLCVBatchReceived`, `TradeReceived`, `GapDetectedEvent`, `GapHealedEvent`). Sin embargo, en el subsistema `trading`, **NO existen Domain Events en su capa de dominio**; los eventos de órdenes y señales se definieron directamente como esquemas wire de Kafka en `shared/kafka/schemas/`. Además, varios eventos en `market_data` contienen atributos de infraestructura técnica (`source: "live" | "backfill"`, `run_id`).
- **EVIDENCE:**  
  - Eventos de market data: `packages/market_data/domain/events/trade_events.py:42`, `gap_events.py:54`.
  - Contaminación técnica: `packages/market_data/domain/events/ingestion.py:53,67`.
  - Ausencia en trading: no existe directorio `packages/trading/domain/events/`.
- **INTERPRETATION:**  
  Domain Events implementados en Market Data con ligera contaminación de infraestructura, pero ausentes en el dominio de Trading.
- **LIMITATION:**  
  Los Domain Events existentes actúan como DTOs inmutables de paso, no como eventos persistidos para transiciones de agregados.

---

### 14. Integration Events
**Clasificación:** 🟡 **PARCIALMENTE IMPLEMENTADO**

- **FACT:**  
  En `shared/kafka/schemas/` se diseñaron 9 esquemas de eventos de integración (`ohlcv.py`, `signals.py`, `orders.py`, `positions.py`, `trades.py`, `orderbook.py`, `external.py`). Todos implementan `BasePayload` con `event_id`, `schema_version` y `occurred_at`. Sin embargo, según el registro formal de procedencia (`shared/kafka/provenance.py`), los esquemas de trading y portfolio (`SignalPayload`, `OrderFilledPayload`, `PositionOpenedPayload`) figuran como huérfanos (`orphan`) sin productores activos en runtime. Solo los esquemas de datos de mercado crudos y métricas externas tienen integración operativa activa (`wired`).
- **EVIDENCE:**  
  - Registro SSOT de procedencia: `shared/kafka/provenance.py:44-79` (clasifica esquemas en `wired` vs `orphan`).
  - Esquema base: `shared/kafka/schemas/_base.py:120`.
- **INTERPRETATION:**  
  El diseño formal de Integration Events existe en el Shared Kernel, pero su adopción en tiempo de ejecución es incompleta en el subsistema de trading.
- **LIMITATION:**  
  Tener clases de payload en `shared/kafka/schemas/` no significa que el sistema esté efectivamente integrado por eventos si los módulos en producción no los emiten ni consumen.

---

### 15. Message/Event Bus
**Clasificación:** 🟡 **PARCIALMENTE IMPLEMENTADO**

- **FACT:**  
  En `market_data`, existe una interfaz `EventBusPort` implementada por `InMemoryEventBus` (`packages/market_data/infrastructure/event_bus/in_memory.py`) para desacoplar el pipeline OHLCV del consumidor de calidad (`QualityPipelineConsumer`). Paralelamente, Kafka actúa como Message Broker distribuido para streaming de datos de mercado. Sin embargo, el motor de trading (`packages/trading/`) no utiliza ningún Event Bus ni Kafka para su ciclo interno de órdenes; las notificaciones se propagan por callbacks síncronos en memoria (`OMS.on_fill`).
- **EVIDENCE:**  
  - Bus en memoria: `packages/market_data/infrastructure/event_bus/in_memory.py:24`.
  - Cableado en pipeline: `packages/market_data/infrastructure/bootstrap/pipeline_factory.py:120`.
  - Despacho directo en OMS: `packages/trading/execution/oms.py:608` (`if self._on_fill: self._on_fill(order)`).
- **INTERPRETATION:**  
  Existe un Event Bus local en Market Data y un Message Broker para ingesta de datos. No existe un bus de eventos de integración para el motor de trading.
- **LIMITATION:**  
  El `InMemoryEventBus` es puramente volátil y no proporciona garantías de entrega persistente ante caídas del proceso.

---

### 16. Transactional Boundaries (Fronteras Transaccionales)
**Clasificación:** 🟡 **PARCIALMENTE IMPLEMENTADO**

- **FACT:**  
  Las fronteras transaccionales están limitadas a componentes individuales de forma asimétrica:
  - En Apache Iceberg: Operaciones atómicas ACID a nivel de tabla mediante commits de snapshots (`table.overwrite()`, `table.append()`).
  - En Redis: Pipelines atómicos `MULTI/EXEC` para persistencia de posición e indexación (`pipe.set`, `pipe.sadd`, `pipe.execute`).
  - En Trading: Secciones críticas en memoria protegidas por mutex (`threading.RLock` en `OMS`, `threading.Lock` en `RiskManager`).
  - **NO existen transacciones distribuidas ni patrón Saga:** Si el proceso colapsa inmediatamente después de que el exchange ejecuta una orden pero antes de que el OMS registre el settlement o notifique a Redis, se produce una inconsistencia local que solo se resuelve por reconciliación manual o mediante el gate `manage_open_orders()` al rearranque.
- **EVIDENCE:**  
  - Redis pipeline: `packages/portfolio/infra/redis_store.py:188-193`.
  - Locks concurrentes en OMS: `packages/trading/execution/oms.py:186,260`.
  - Reconciliación al arranque: `packages/trading/execution/oms.py:654-724`.
- **INTERPRETATION:**  
  Existen fronteras transaccionales locales sólidas en cada almacén, complementadas con mecanismos de reconciliación defensiva (fail-closed) para absorber la consistencia eventual con el exchange externo.
- **LIMITATION:**  
  No hay garantía de consistencia transaccional ACID跨-servicio.

---

### 17. Read Models / Projections
**Clasificación:** 🟡 **PARCIALMENTE IMPLEMENTADO**

- **FACT:**  
  La tabla `gold.features` en Apache Iceberg actúa como una vista materializada de datos (proyección analítica sobre Silver) computada por `GoldTransformer` y optimizada para consumo de estrategias. Asimismo, `PortfolioService.snapshot()` genera una proyección de lectura inmutable (`PortfolioState`) a partir de las posiciones abiertas. No obstante, **ninguna de estas estructuras es una proyección derivada de Event Sourcing**. Son transformaciones analíticas por lotes (ETL) o copias defensivas de estado en memoria.
- **EVIDENCE:**  
  - Proyección de features: `packages/market_data/infrastructure/storage/gold/transformer.py:95`, `gold_storage.py:257`.
  - Proyección de portfolio: `packages/portfolio/services/portfolio_service.py:274`.
- **INTERPRETATION:**  
  Existen modelos de lectura y transformaciones analíticas especializadas, pero corresponden a un Data Lakehouse y a servicios de dominio tradicionales, no a proyecciones reactivas de CQRS/ES.
- **LIMITATION:**  
  Llamar "Read Model" o "Proyección" a un dataset de Iceberg no lo convierte en un patrón CQRS.

---

## 3. EVENT-DRIVEN VS EVENT SOURCING: ANÁLISIS DE LA DISTINCIÓN

Existe una confusión conceptual habitual entre arquitecturas orientadas a eventos y arquitecturas con Event Sourcing. La auditoría forense concluye:

| Aspecto | Implementación Real en OCM | Justificación Basada en Código |
| :--- | :--- | :--- |
| **A. Eventos como comunicación/transporte** | 🟢 **SÍ** | Kafka transporta ticks y deltas de mercado (`ohlcv.raw`, `trades.raw`, `orderbook.raw`). |
| **B. Eventos como fuente de verdad** | 🔴 **NO** | La fuente de verdad del estado de órdenes es el Exchange; la de posiciones es Redis (`PositionSnapshot`). |
| **C. Eventos persistidos para reconstrucción** | 🔴 **NO** | No existe código de replay de eventos de negocio para reconstruir balances o carteras. |
| **D. Persistencia basada únicamente en Snapshots** | 🟢 **SÍ** | `PortfolioService` persiste el snapshot completo en Redis. `RiskManager` mantiene estado en memoria volátil. |
| **E. Combinación de Snapshots + Eventos** | 🔴 **NO** | No hay replay de deltas sobre snapshots de dominio. |

### El Papel Real de Kafka en OCM
1. **Es un Message Transport y Streaming Buffer:** Actúa como un desacoplador de alta capacidad entre los WebSockets de los exchanges (Cryptofeed) y la persistencia en lotes de Apache Iceberg Bronze.
2. **Es un Event Log Temporal, NO un Event Store:**
   - En `shared/kafka/topics.py` (líneas 214–230), la documentación operacional explicita:
     - `ohlcv.raw`: retención `delete 7d`.
     - `orderbook.raw`: retención `delete 1h`.
     - `orders.filled`: retención `delete 30d`.
     - `positions.closed`: retención `delete 90d`.
   - Una política de retención destructiva basada en tiempo (`delete`) **invalida a Kafka como Event Store**. Un Event Store exige retención infinita inmutable para permitir reconstruir cualquier estado histórico desde el origen (`genesis`).

---

## 4. AUDITORÍA DE REDIS Y EL BOUNDED CONTEXT PORTFOLIO

### Estructura y Claves en Redis
- **Clave individual de posición:** `ocm:positions:{exchange}:{order_id}` (tipo String/JSON).
- **Clave de índice:** `ocm:positions:{exchange}:_index` (tipo Set de `order_id`s activos).
- **TTL por defecto:** 7 días (`_TTL_DEFAULT = 7 * 24 * 3600`).
- **Contenido del registro:**
  ```json
  {
    "order_id": "UUID-v4",
    "symbol": "BTC/USDT",
    "exchange": "bybit",
    "side": "long",
    "quantity": 1.5,
    "avg_entry": 65000.0,
    "size_pct": 0.05,
    "entry_at": "2026-09-06T00:00:00Z",
    "current_price": null
  }
  ```

### Análisis de Resiliencia y Recuperación
1. **Naturaleza del Almacenamiento:** Es un **Snapshot de estado actual**. No guarda deltas ni eventos históricos de mutación.
2. **¿Existe historial en Redis?** NO. Cada mutación sobrescribe la clave o la elimina (`delete()`).
3. **¿Existe replay o reconstrucción desde eventos?** NO.
4. **¿Qué ocurre si Redis se pierde completamente?**
   - Si Redis cae o se vacía, **se pierde la totalidad del estado interno de posiciones abiertas y el cálculo de base de coste medio ponderado (WAC)**.
   - OCM no tiene ningún mecanismo para reproducir eventos de Kafka y regenerar ese estado en Redis.
   - La única vía de recuperación es externa: consultar al Exchange vía REST (`fetch_positions()`) mediante intervención manual o tooling operacional, perdiendo los metadatos internos de OCM (estrategia asignada, order_id interno).
5. **Detección de Colisiones (B-16):** `RedisPositionStore.save()` implementa verificación de colisiones: si ya existe una posición con el mismo `order_id` pero distinto símbolo, exchange o lado, eleva `PositionIdCollisionError` evitando el sobreescrito silencioso.

---

## 5. AUDITORÍA DE ARQUITECTURA HEXAGONAL Y CONTRATOS DE IMPORTACIÓN

### Gobernanza Mecánica (`import-linter`)
La auditoría de `architecture_linter/importlinter.toml` confirma la existencia de **50 contratos de importación evaluados** (4 de capas y 46 de dependencias prohibidas).
Al ejecutar `uv run lint-imports --config architecture_linter/importlinter.toml`:
`Analyzed 514 files, 2225 dependencies. Contracts: 50 kept, 0 broken.`

#### Principales Reglas Protegidas:
1. **BC-03 (Domain Isolation):** `market_data.domain` no importa puertos, aplicación, adaptadores ni infraestructura.
2. **BC-07 / BC-08 (Layer Direction):** `infrastructure` no importa `application` (DIP estricto). La dirección de capas se mantiene: `domain < ports < application < adapters < infrastructure`.
3. **BC-09 (Third-Party Governance):** `domain` no importa librerías de infraestructura (cero pandas, cero ccxt, cero pyiceberg, cero aiokafka).
4. **BC-10, BC-12, BC-13 (Bounded Context Isolation):** Ningún contexto de negocio importa directamente a sus hermanos.
5. **BC-50 (Cross-Context Bridge):** `trading` puede importar `market_data` únicamente desde `trading/bootstrap/composition_root.py`.
6. **BC-33, BC-35, BC-47 (Kafka Decoupling):** `shared.kafka.schemas` no importa tipos de dominio; los bounded contexts no definen esquemas wire duplicados.

**Conclusión:** La inversión de dependencias y el aislamiento hexagonal son **reales y mecánicamente inviolables** en CI.

---

## 6. AUDITORÍA DE MEDALLION Y DATA LAKEHOUSE

### Estado Real de las Capas

| Capa | Almacenamiento Físico | Modo de Escritura | Operación Real |
| :--- | :--- | :--- | :--- |
| **Bronze** | Apache Iceberg: `bronze.ohlcv`, `bronze.orderbook_snapshot/delta` | Append-Only Parquet | Activo para OHLCV; brechas operacionales en Orderbook L2 (G9). |
| **Silver** | Apache Iceberg: `silver.ohlcv`, `silver.trades` | Merge / Dedup por `timestamp` y `trade_id` | Activo. Valida schema pandera/polars y limpia gaps. |
| **Gold** | Apache Iceberg: `gold.features` | Overwrite atómico por partición (`overwrite`) | Activo. Calcula 9 features estadísticas rolling vía Polars nativo. |

- **Diferenciación Arquitectónica:** El pipeline Medallion es un flujo de refinamiento analítico de datos (ETL). No tiene relación con CQRS ni con Event Sourcing.

---

## 7. MAPA END-TO-END DE FLUJOS DEL SISTEMA

### Flujo A: Adquisición, Refinamiento y Consumo de Datos de Mercado
```
1. Exchange Externo (WS/REST)
      ↓ (Ticks crudos / Candles CCXT)
2. Inbound Adapters (BybitFeedAdapter, HistoricalFetcherAsync)
      ↓ (Kafka Produce - Serialización BasePayload UTF-8)
3. Kafka Topic: ohlcv.raw / trades.raw / orderbook.raw
      ↓ (Consumer Poll at-least-once con deduplicación L1/L2)
4. Stream Processors (KafkaBronzeWriter, OrderbookBronzeWriter)
      ↓ (Append-Only Write)
5. Apache Iceberg: BRONZE
      ↓ (Quality Gate, Pandera Schema Validation, Temporal Grid Alignment)
6. Apache Iceberg: SILVER (silver.ohlcv - SSOT Canónica de Mercado)
      ↓ (GoldTransformer - Cálculo rolling Polars nativo)
7. Apache Iceberg: GOLD (gold.features - Overwrite atómico de dataset)
      ↓ (FeatureSource.load_features() - Polling Síncrono)
8. TradingEngine.run_once()
```

### Flujo B: Ejecución de Comandos, Validación y Persistencia de Estado
```
1. BaseStrategy.generate_signals(df)
      ↓ (Retorna Signal con TARGET quantity)
2. TradingEngine invoca OMS.submit(signal)
      ↓ (Síncrono)
3. RiskManager.validate(signal)
      ├─► Rechazada: Retorna RiskDecision(approved=False) ──► Fin del ciclo
      └─► Aprobada: Retorna RiskDecision(approved=True)
            ↓
4. OMS crea Order (PENDING → SUBMITTED) y fija clamp de cantidad (INV-08)
      ↓
5. LiveExecutor.execute(order)
      ↓ (run_ccxt_async - Loop síncrono bloqueante)
6. _BybitTransport llama a Exchange API (CCXT create_order)
      ↓ (Reconciliación inmediata fetch_order)
7. Exchange confirma Fill real (fill_price, filled_qty, fees)
      ↓
8. OMS._fill() transiciona Order a FILLED
      ↓
9. OMS calcula Settlement canónico (WAC vs Exit Price)
      ├─► Callback en memoria: PortfolioService.open_position() o close_position()
      │         ↓
      │   Persistencia atómica JSON en Redis (ocm:positions:{exchange}:{order_id})
      │
      ├─► Callback en memoria: TradeTracker.on_fill()
      │         ↓
      │   Registra TradeRecord inmutable en lista en RAM (_closed)
      │
      └─► Callback en memoria: RiskManager.record_close(pnl_usd)
                ↓
          Actualiza contadores de drawdown en RAM y verifica Halt
```

---

## 8. MATRIZ DE FUENTES DE VERDAD (SOURCES OF TRUTH)

| Estado / Entidad | Source of Truth (Fuente Real de Verdad) | Naturaleza Arquitectónica | Tipo de Persistencia |
| :--- | :--- | :--- | :--- |
| **Market Data (Ticks Crudos)** | Exchange Externo | External Data Feed | Volátil / Transitorio |
| **Market Data (Histórico Crudo)** | Apache Iceberg: `bronze.ohlcv` | Data Lakehouse Raw Layer | Append-Only Durable |
| **Market Data (OHLCV Limpio Canónico)**| Apache Iceberg: `silver.ohlcv` | Canonical Clean Lakehouse | Almacén Columnar ACID |
| **Features Estadísticas** | Apache Iceberg: `gold.features` | Derived Projection Dataset | Snapshot Overwrite |
| **Órdenes (Estado Activo)** | Exchange Externo | External Exchange Engine | Reconciliado en memoria RAM |
| **Posiciones Abiertas** | Redis (`ocm:positions:{exchange}:{order_id}`) | Key-Value State Store | Snapshot JSON Durable |
| **Base de Coste (WAC)** | Redis + Espejo local en `OMS._entry_positions` | In-Memory / Redis Snapshot | Snapshot |
| **Trades Cerrados (Historial)** | Memoria RAM (`TradeTracker._closed`) | In-Memory Object List | Volátil de proceso |
| **Riesgo (Drawdown y Exposición)**| Memoria RAM (`RiskManager._daily_pnl_usd`) | In-Memory Variables bajo Lock | Volátil de proceso |
| **Eventos en Kafka** | Buffer de Transporte | Distributed Commit Log | Temporal (Retención TTL) |

---

## 9. DURABILIDAD Y RECUPERACIÓN ANTE DESASTRES

| Componente Perdido | ¿Es Recuperable? | ¿Desde Dónde? | ¿Mecanismo de Recuperación? | ¿Es Event-Sourced? |
| :--- | :--- | :--- | :--- | :--- |
| **Redis** | 🔴 **PARCIAL / NO LOCAL** | Exchange Externo (REST) | Intervención manual / endpoint `fetch_positions()`. No hay reconstrucción local. | 🔴 NO (Pérdida de WAC local). |
| **Kafka** | 🟢 **SÍ** | Exchange REST / Iceberg Silver | Los datos en tránsito se pierden, pero el histórico de velas se recupera mediante `BackfillStrategy` desde el exchange. | 🔴 NO (Kafka es buffer transitorio). |
| **Bronze** | 🟢 **SÍ** | Exchange REST (Backfill) | Re-ingesta masiva desde endpoints históricos de CCXT. | 🔴 NO. |
| **Silver** | 🟢 **SÍ** | Bronze o Exchange REST | Re-ejecución del pipeline de limpieza y deduplicación. | 🔴 NO. |
| **Gold** | 🟢 **SÍ** | Iceberg Silver | `GoldStorage.build()` recalcula todas las features a partir de Silver en minutos. | 🟢 SÍ (Proyección derivada determinista). |
| **Consumer State (Offsets)** | 🟢 **SÍ** | Kafka / L2 Dedup Store | Re-consumo con deduplicación por `event_id` (`CompositeSeenFilter`). | 🔴 NO. |
| **Exchange Connectivity**| 🟢 **SÍ** | Circuit Breakers / SafeOps | El sistema suspende operaciones (`ExecutionGuard`), rechaza nuevas órdenes y reconcilia al volver la conexión. | 🔴 NO. |

---

## 10. RESULTADO FINAL Y TABLA RESUMEN

| Patrón / Principio | Estado | Evidencia de Código | Limitación Identificada |
| :--- | :--- | :--- | :--- |
| **DDD** | 🟡 **PARCIAL** | VOs (`Candle`, `PositionSnapshot`); Domain Services. | Sin Aggregate Roots formales en trading; estado mutable duplicado en 7 almacenes. |
| **Event-Driven** | 🟡 **PARCIAL** | Kafka en streaming de mercado (`ohlcv.raw`, `trades.raw`). | Trading opera por polling síncrono; tópicos de señales y órdenes son huérfanos. |
| **CQRS** | 🔴 **NO IMPLEMENTADO** | Ninguna interfaz command/query separada. | Servicios CRUD tradicionales; Medallion es ETL, no CQRS. |
| **Event Sourcing** | 🔴 **NO IMPLEMENTADO** | Redis persiste snapshots JSON con `SET`. | Sin log de eventos de negocio; no hay reconstrucción de estado. (No necesario). |
| **Hexagonal** | 🟢 **IMPLEMENTADO** | 50 contratos `import-linter` KEPT; Composition Roots. | Duplicaciones nominales leves en contratos detectadas por linter AST. |
| **Medallion** | 🟢 **IMPLEMENTADO** | Bronze (append), Silver (clean), Gold (features). | Gap G9 operacional en la ingesta de Orderbook Bronze. |
| **Data Lakehouse** | 🟢 **IMPLEMENTADO** | Apache Iceberg Parquet con catálogo ACID y snapshots. | Motor computacional local (Polars), no cluster distribuido. |
| **Feature Store** | 🟡 **PARCIAL** | GoldStorage con 9 features versionadas en Iceberg. | Offline Feature Store únicamente; sin serving online en tiempo real. |
| **Composition Root** | 🟢 **IMPLEMENTADO** | Roots independientes en cada Bounded Context. | Ninguna. |
| **Bounded Contexts** | 🟢 **IMPLEMENTADO** | Límites blindados por contratos BC-NN en CI. | Estado conceptual duplicado detectado por ARCH-001. |
| **Domain Events** | 🟡 **PARCIAL** | Eventos en `market_data/domain/events/`. | Ausentes en Trading; contaminados con campos técnicos (`source`). |
| **Integration Events**| 🟡 **PARCIAL** | Esquemas en `shared/kafka/schemas/`. | Esquemas de trading son huérfanos (`orphan`) sin productores activos. |
| **Event Bus** | 🟡 **PARCIAL** | `InMemoryEventBus` en `market_data`. | Inexistente en Trading; volátil en memoria. |

---

### A. Arquitectura Real Encontrada
La arquitectura real de OCM es:
> **Hexagonal (Ports & Adapters) estricta + Data Lakehouse Medallion (Apache Iceberg) + Ingestión de Mercado en Streaming (Kafka Kappa) + Motor de Trading Síncrono Discreto (In-Memory / Redis Snapshot).**

### B. Fuente de Verdad por Bounded Context
- **Market Data:** Apache Iceberg Silver (`silver.ohlcv`).
- **Trading / OMS:** Exchange Externo (reconciliado en memoria RAM).
- **Portfolio:** Redis (`PositionSnapshot` JSON) para persistencia cross-restart.

### C. Flujo de Datos End-to-End
Streaming asíncrono desde el Exchange hacia Kafka → Iceberg Bronze → Iceberg Silver → Iceberg Gold.

### D. Flujo de Comandos y Estado
Llamadas síncronas bloqueantes en memoria: `TradingEngine` → `BaseStrategy` → `OMS` → `RiskManager` → `LiveExecutor` → `CCXT` → `PortfolioService` → `Redis`.

### E. Dependencias Arquitectónicas
Dirección canónica respetada: `shared → ocm → domain → ports → application → adapters → infrastructure`. Cero violaciones de importación de capas.

### F. Qué Patrones NO Están Implementados
1. **CQRS:** No existe separación de modelos Command y Query.
2. **Event Sourcing:** No existe almacenamiento de estado basado en eventos ni replay.
3. **Unit of Work:** No existe coordinación de transacciones multi-repositorio.
4. **Online Real-Time Feature Store:** No hay serving de baja latencia en Redis para inferencia.
5. **Event-Driven Trading:** El motor de órdenes no es guiado por eventos.

### G. Qué Patrones Están Parcialmente Implementados
1. **DDD:** Value Objects excelentes, pero sin Aggregates formales en Trading y con estado duplicado.
2. **EDA:** Completo para datos de mercado; ausente para trading y órdenes.
3. **Integration Events:** Esquemas modelados pero huérfanos sin emisores.
4. **Feature Store:** Existe solo como dataset offline sobre Iceberg.

### H. Afirmaciones de la Documentación que son Incorrectas o No Demostrables
1. **Afirmación:** *"OCM implementa arquitectura Event-Driven reactiva end-to-end desde la vela hasta la orden cursada"* (Documentación conceptual y tópicos de `signals.raw` / `orders.filled`).  
   **Realidad:** Es falso. El motor de trading es un bucle imperativo síncrono `run_once()` que evalúa y envía órdenes directamente en el mismo hilo de ejecución.
2. **Afirmación:** *"El pipeline Medallion Bronze/Silver/Gold es una implementación de CQRS"*.  
   **Realidad:** Es un error conceptual. Medallion es un patrón de ingeniería de datos para refinamiento ETL, no una separación de modelos Command/Query de dominio.
3. **Afirmación:** *"Kafka actúa como el log durable de auditoría financiera del sistema"*.  
   **Realidad:** Kafka tiene políticas de retención destructivas de 7 días y 30 días (`topics.py`), lo que impide su uso como log de auditoría duradera.

### I. Riesgos Arquitectónicos Reales
1. **Riesgo ARCH-001 / ARCH-010 (Múltiples Dueños de Estado de Posición):** El estado de una posición está disperso en 7 almacenes mutables en memoria (`OMS._entry_positions`, `TradeTracker._open_positions`, `RiskManager._positions`, `PortfolioService`, etc.), con riesgo de divergencia aritmética ante cierres parciales.
2. **Riesgo ARCH-004 (Balance Configurado vs Real):** El sizing y el drawdown se calculan sobre `capital_usd` configurado estáticamente, sin consultar el balance real disponible en el exchange (`fetch_balance` ausente).
3. **Riesgo de Pérdida de WAC si cae Redis:** Si Redis colapsa, el cálculo de P&L de posiciones abiertas no puede reconstruirse localmente.

### J. Recomendaciones Arquitectónicas (Solo para Planificación Futura, NO Implementar Ahora)
1. **NO implementar Event Sourcing:** Formalizar en un ADR que OCM descarta Event Sourcing para trading, manteniendo la persistencia de estado basada en Snapshot en Redis.
2. **NO implementar CQRS:** Mantener el modelo actual de servicios de dominio directo para no degradar la latencia ni complicar la consistencia de órdenes.
3. **Deprecar Tópicos Huérfanos de Kafka:** Eliminar o marcar como deprecados los esquemas huérfanos de `shared/kafka/` (`signals.*`, `orders.*`) para que la base de código refleje exclusivamente la infraestructura viva.
4. **Unificar el Dueño de Estado de Posición:** En futuras fases, hacer que `RiskManager` y `TradeTracker` lean directamente el snapshot inmutable de `PortfolioService`, eliminando sus mapas mutables locales duplicados.

---
**FIN DE LA AUDITORÍA FORENSE ARQUITECTÓNICA**  
*Documento autovalidado contra la base de código fuente de OrangeCashMachine.*
