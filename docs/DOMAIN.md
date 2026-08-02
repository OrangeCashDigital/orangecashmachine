# DOMAIN.md — OrangeCashMachine

**Propósito de este documento**

Esto no es documentación escrita después de construir el sistema — es un artefacto de diseño. Describe el dominio *tal como existe hoy* en el código, incluyendo lo que está bien resuelto y lo que no. Cada afirmación aquí fue verificada leyendo el código fuente, no inferida de nombres de carpetas. La sección "Deuda Arquitectónica" es tan importante como el resto: nombra explícitamente lo que aún no está alineado, para que no desaparezca de la vista hasta que explote en un refactor grande.

Los tres bounded contexts documentados (`market_data`, `trading`, `portfolio`) tienen niveles de madurez arquitectónica distintos. Eso no es un defecto que ocultar — es el punto de partida real desde el que se decide qué nivelar y en qué orden.

---

## 1. `market_data` — el bounded context más maduro

Sigue Clean Architecture / Hexagonal de forma explícita: `domain/` → `ports/` → `application/` → `adapters/` → `infrastructure/`. El dominio no depende de pandas, Kafka, Redis, Iceberg ni CCXT — verificado en `domain/exceptions/__init__.py` y en el contrato BC-09 de `import-linter`.

### Entities

Solo una: **`DataTier`** (`domain/entities/__init__.py`) — clasificación de calidad de un dataset Silver (`CLEAN` / `FLAGGED` / `REJECTED`). El propio módulo documenta por qué no hay más: `OHLCVBar` fue eliminado deliberadamente por violar DIP (dependía de streaming/infraestructura). Decisión de diseño explícita, no descuido.

### Value Objects

`Candle`, `NormalizedTrade`, `RawTrade`, `OrderBookSnapshot`, `OrderBookDelta`, `Symbol`, `Timeframe`, `GapRange`, `OHLCVChunk`, `TradeSeries`, `QualityLabel`, `ExchangeQuirks`, `CandleValidator`/`ValidationResult`/`ValidationSummary`.

Nota de corrección: en el diseño inicial de este documento, `Candle`/`Trade`/`OrderBook` se propusieron como *Entities*. El código dice lo contrario — son VOs sin identidad propia, definidos por su valor.

### Domain Events

Jerarquía dividida por nivel de dato, con `DomainEvent` (`events/_base.py`) como raíz compartida — vive separado del resto precisamente para que los eventos de nivel 0 (`orderbook_events`, `trade_events`, `replay_events`) no tengan que importar los derivados (`ingestion.py`), respetando BC-11.

- **Ingestión** (`ingestion.py`): `CandleReceived`, `OHLCVBatchReceived`, `OHLCVBatchIngested`, `QualityCheckPassed`, `QualityCheckFailed`, `SignalGenerated`.
- **Gaps — control plane** (`gap_events.py`): `GapDetectedEvent` → `GapHealedEvent` | `GapFailedEvent`. El módulo distingue explícitamente tres planos: *data plane* (trades/velas), *state plane* (persistencia Iceberg), *control plane* (telemetría del propio pipeline — a este plano pertenecen los gap events).
- **Order book / microestructura** (`orderbook_events.py`): `OrderBookSnapshotReceived`, `OrderBookDeltaReceived`.
- **Replay — Kappa Architecture** (`replay_events.py`): `ReplayRequested`, `ReplayCompleted`. El log de eventos (Kafka) es la fuente de verdad; replay reprocesa desde un punto temporal arbitrario.
- **Lineage** (`_lineage.py`): `LineageEvent` con `PipelineLayer` (RAW/SILVER/GOLD) y `LineageStatus` — trazabilidad de transiciones entre capas del medallion.
- **Trades** (`trade_events.py`): `TradeReceived` — ver Deuda Arquitectónica, punto 1.

### Policies

`domain/policies/base.py` contiene tipos de dominio puros para el pipeline OHLCV: `PipelineMode` (INCREMENTAL/BACKFILL/REPAIR), `PairResult`, `PipelineSummary`, `PipelineStrategy` (Protocol de duck typing). El propio módulo documenta explícitamente qué *no* vive aquí (`PipelineContext`, `StrategyMixin` → pertenecen a `application/`).

`domain/policies/data_quality_policy.py` — `DataQualityPolicy`, Domain Service stateless: dado un `DataQualityReport`, decide `ACCEPT` / `ACCEPT_WITH_FLAGS` / `REJECT` con scoring matemático documentado y umbrales como constantes SSOT.

### Quality

`domain/quality/types.py` — `QualityIssue`, `DataQualityReport` (VOs puros, sin pandas). `domain/quality/invariants.py` — `check_dataset_invariants()`, patrón de Specification funcional que verifica que un dataset Silver sea consistente (particiones sin solapar, timestamps válidos, lag máximo, etc.).

### Exceptions

`domain/exceptions/__init__.py` — jerarquía completa con raíz `MarketDataError`, ramificada en `IngestionError`, `StorageError`, `QualityError`, `PipelineError`, `ExchangeAdapterError`. El atributo `is_transient` en `ExchangeAdapterError` permite que `ResilienceLayer` decida reintentos sin `isinstance()` (OCP/DIP).

---

## 2. `trading` — organizado por capacidad, sin domain/ formal

No sigue el mismo patrón de capas que `market_data`. Está organizado por capacidad técnica: `execution/`, `risk/`, `analytics/`, `strategies/`, `data/`, `engine.py`. No existen carpetas `domain/`, `ports/` ni `application/` explícitas.

**Esto no implica acoplamiento real verificado** — es deuda de convención/nomenclatura, no de dependencias rotas:

- `execution/order.py` define **`Order`** — sí es una Entity de dominio genuina (identidad vía `order_id`, máquina de estados explícita `PENDING → SUBMITTED → FILLED/REJECTED/CANCELLED`, invariantes validadas en `__post_init__`). Convive en la carpeta con `LiveExecutor` (adapter real hacia CCXT, actualmente STUB) y `PaperExecutor`, pero la dirección de dependencia es correcta: los executors importan `Order`, `Order` no importa nada de ellos.
- `execution/oms.py` define **`OMS`** (Order Management System) y el Protocol **`OrderExecutor`** — este último es, conceptualmente, un *port* hexagonal, pero está definido inline en vez de vivir en una carpeta `ports/` dedicada (a diferencia de `portfolio`).
- `risk/models.py` — configuración Pydantic (`RiskConfig`, `PositionConfig`, `DrawdownConfig`, etc.), correctamente separada de:
- `risk/manager.py` — **`RiskManager`**, **`RiskDecision`**, **`RiskViolation`** — estos sí son conceptos de dominio (decisión de riesgo, violación como excepción). BC-12 de `import-linter` ya protege `trading.risk` de importar `trading.execution`.
- `strategies/base.py` — **`BaseStrategy`** es Policy pura: stateless, solo depende de `Signal` (de `shared/types/`) y pandas, sin fugas hacia infraestructura. `strategies/ema_crossover.py` es una implementación limpia del contrato.
- `engine.py` — **`TradingEngine`** actúa como Composition Root informal de `trading`: sus factories `build_live()`/`build_paper()` ensamblan `Strategy + RiskManager + Executor + OMS`. No contiene lógica propia de riesgo/órdenes/estrategia — solo conecta (SRP explícito en su docstring).

---

## 3. `portfolio` — el bounded context mejor resuelto, con nomenclatura distinta

No tiene carpeta `domain/`, pero su forma ya es hexagonal:

- `models/position.py` — **`PositionSnapshot`**, **`PortfolioState`**: VOs inmutables (`frozen=True`), con docstring explícito: *"representan hechos — no se modifican, se reemplazan"*.
- `ports/position_store.py` — **`PositionStore`** (Protocol) — puerto de persistencia explícito, correctamente aislado en su propia carpeta (a diferencia de `OrderExecutor` en `trading`).
- `infra/memory_store.py` y `infra/redis_store.py` — **`InMemoryPositionStore`**, **`RedisPositionStore`** — ambas implementan `PositionStore` sin fugas hacia `services/`. `RedisPositionStore` es Fail-Soft (nunca lanza, retorna vacío si Redis no está disponible).
- `services/portfolio_service.py` — **`PortfolioService`** — coordina apertura/cierre de posiciones vía callbacks del OMS (`on_fill`). Cumple el rol de `application/` sin llamarse así.
- `services/rebalance_service.py` — **`RebalanceService`**, **`RebalanceSignal`** — calcula ajustes de portfolio contra targets, sin ejecutar órdenes ni validar riesgo (separación de concerns explícita en el docstring). Sin consumidor activo; capacidad adelantada del roadmap de portfolio (ver ADR-0004).

---

## 4. `shared/contracts/boundaries.py` — el backbone real de contratos cruzados

No es "shared misceláneo" — es el catálogo SSOT de todos los puertos que cruzan fronteras entre bounded contexts:

| Protocol | Frontera | Implementado por (según docstring) |
|---|---|---|
| `FeatureSource` | market_data → trading | `GoldLoader`, `GoldLoaderAdapter` |
| `SignalProtocol` | strategies → execution | `trading.strategies.base.Signal` |
| `FillHandler` | execution → portfolio | *(ver Deuda Arquitectónica, punto 4)* |
| `TradeHistory` | portfolio → backtesting | *(ver Deuda Arquitectónica, punto 4)* |
| `RiskGate` | execution → risk | *(ver Deuda Arquitectónica, punto 4 — mismatch verificado)* |

`shared/types/signal.py` define **`Signal`** — VO de dominio puro (stdlib únicamente), consumido por `OMS.submit()` y `RiskManager.validate()`. Ver Deuda Arquitectónica, punto 3, sobre su docstring desactualizado.

---

## 5. Deuda Arquitectónica

Todo lo siguiente está verificado leyendo código, no es especulación.

### 1. `TradeReceived` no hereda de `DomainEvent` — viola un invariante que el propio código declara

`domain/events/_base.py` documenta explícitamente: *"DomainEvent es base compartida por TODOS los eventos — tanto crudos (orderbook_events, replay_events, **trade_events**) como derivados (ingestion)"*. Pero `TradeReceived` (`trade_events.py`) no hereda de `DomainEvent`: redefine su propio `event_id` y, más grave, su propio `occurred_at` como `datetime` — mientras que `DomainEvent.occurred_at` es `str` (ISO-8601). Riesgo concreto: cualquier consumidor que trate ambos tipos de evento de forma polimórfica por el nombre del campo puede toparse con un bug de tipo. **Acción sugerida**: hacer que `TradeReceived` herede de `DomainEvent`, o documentar explícitamente por qué es la excepción.

### 2. `classify_error()` duplicado — deuda ya reconocida en el propio código

`domain/policies/base.py` re-exporta una copia idéntica de `classify_error()` cuya fuente real vive en `application/pipeline/runtime.py`. El código ya lo marca: *"se eliminará esta al completar la migración de todos los importadores"*. No es un hallazgo nuevo — es trabajo pendiente ya identificado por el propio equipo.

### 3. Docstrings con rutas de archivo obsoletas — patrón recurrente, no incidente aislado

Aparece al menos tres veces:
- `Signal` (`shared/types/signal.py`) dice `"Ubicación: domain/"` — no existe ningún `domain/` bajo `trading`, y el archivo vive en `shared/types/`.
- `data_quality_policy.py` tiene el encabezado `market_data/quality/policies/data_quality_policy.py` — la ruta real es `market_data/domain/policies/data_quality_policy.py`.
- `strategies/base.py` comenta *"Signal vive en domain/ — re-exportado aquí"* — mismo error que el punto anterior.

**Acción sugerida**: dado que es un patrón, no un error puntual, vale la pena un check de CI que compare la ruta declarada en el docstring contra la ruta real del archivo, en vez de corregir caso por caso.

### 4. `boundaries.py` — contratos con distinto grado de vigencia real (verificado por uso, no solo definición)

Búsqueda de referencias (`grep -rn` sobre `packages/`, `apps/`, `shared/`) distingue dos casos muy distintos:

- **`RiskGate` — contrato adelantado, no un bug.** No tiene ningún método `evaluate(signal) -> tuple[bool, str]` implementado hoy (`RiskManager` tiene `validate(signal) -> RiskDecision`), pero aparece consistentemente referenciado junto a `RiskGateConsumer` en `shared/kafka/topics.py` y `shared/kafka/schemas/signals.py` (topics `signals.raw` → `signals.approved`/`signals.rejected` ya declarados, con `ApprovedSignalPayload`/`RejectedSignalPayload` ya definidos). Es el contrato de puerto para la versión event-driven (Kafka consumer) del risk gating que reemplazará al `RiskManager` síncrono actual — coherente con la decisión de roadmap de migrar el motor de trading de batch/cron a event-driven. No está desconectado: está a la espera de la pieza que lo implementa.
- **`FillHandler` y `TradeHistory` — sí huérfanos.** Cero referencias fuera de su propia definición y re-export en `shared/contracts/__init__.py`. A diferencia de `RiskGate`, no hay ningún topic, schema, ni componente planeado que los respalde en el resto del código. Ambos declaran `"Implementado por: portfolio.TradeTracker"`, pero `TradeTracker` vive en `packages/trading/analytics/trade_tracker.py`, no en `portfolio` — la atribución en el docstring está mal incluso si se decide implementarlos.

**Acción sugerida**: `RiskGate` no se toca todavía — su implementación real depende de construir `RiskGateConsumer`, que es trabajo de la migración a event-driven, no de esta limpieza. `FillHandler`/`TradeHistory` sí son candidatos inmediatos a decisión: implementarlos de verdad (y corregir la atribución a `trading.analytics.TradeTracker`) o eliminarlos de `boundaries.py` si no hay intención de usarlos.

---

### 5. `DataQualityReport.git_hash` — el dominio invoca un proceso externo (verificado por código, no por nombre de carpeta)

La Sección 1 de este documento afirma que "el dominio no depende de pandas, Kafka, Redis, Iceberg ni CCXT" — pero `domain/quality/types.py` (`_get_git_hash()`) llama `subprocess.run(["git", "rev-parse", "--short", "HEAD"], ...)` para poblar `DataQualityReport.git_hash`. Es stdlib, no una librería de terceros, así que no viola DIP contra infraestructura de la misma forma que pandas/Kafka lo harían — pero sí es una dependencia real hacia un proceso del sistema operativo y un binario externo (git debe estar instalado y el proceso debe correr dentro de un repo git), lo cual rompe la garantía de pureza que la Sección 1 declara sin excepciones para este VO.

Fail-soft: la función captura cualquier excepción y retorna "unknown", así que no hay riesgo de fallo — es una cuestión de precisión de la documentación, no de robustez del código.

**Acción sugerida**: o bien mover la resolución de git_hash fuera del dominio (inyectarlo desde application/ al construir el DataQualityReport, manteniendo el VO puro), o bien matizar la afirmación de la Sección 1 para excluir explícitamente esta llamada a subprocess como excepción conocida y aceptada.

---

## 6. Camino de evolución (no ejecutar todavía)

El objetivo de madurez no es tecnología nueva — es que `trading` y `portfolio` alcancen la misma disciplina de capas que ya tiene `market_data`. Orden sugerido, solo después de que este documento sea revisado y aprobado:

1. Escribir tests que protejan los contratos ya identificados (especialmente el mismatch de `RiskGate`) antes de tocar nada.
2. Decidir si `RiskGate`/`FillHandler`/`TradeHistory` se implementan de verdad o se eliminan de `boundaries.py`.
3. Extraer `trading/ports/` con el Protocol `OrderExecutor` movido desde `oms.py`.
4. Evaluar si `Order` se mueve a `trading/domain/entities/order.py` — solo después de (3), para no mover una Entity antes de tener dónde ponerla con sentido.
5. Corregir la herencia de `TradeReceived`.
6. Solo entonces: diseñar el Composition Root definitivo que reemplaza a Dagster.
