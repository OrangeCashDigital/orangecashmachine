# TradingCompositionRoot — Arquitectura Recuperada

**Documento:** Recuperación forense por introspección de bytecode
**Bounded context:** `trading`
**Fecha del hallazgo:** 2026-08-02
**Estado:** Diseño recuperado — implementación pendiente de reconstrucción (ver §5)
**ADRs relacionados:** ADR-0003, ADR-0004, ADR-0005, ADR-0006

---

## 0. Resumen ejecutivo

`packages/trading/bootstrap/` (`composition_root.py`, `redis_factory.py`,
`__init__.py`) perdió todo su código fuente sin haber sido commiteado
nunca a git — confirmado por búsqueda exhaustiva en los 973 commits de
todas las ramas. Solo sobrevivió como bytecode compilado en
`__pycache__/` (Python 3.13). Este documento es el SSOT del diseño
recuperado: separa evidencia objetiva, comparación con el estado actual
del repo y decisiones de arquitectura, sin mezclar las tres capas.

## 1. Contexto y método de recuperación

- `decompyle3`, `uncompyle6` y `xdis` no soportan bytecode 3.13
  (intérprete adaptativo, PEP 659) — decompilación completa no es viable
  hoy con herramientas públicas.
- Recuperación realizada por introspección de `marshal`
  (`co_names`, `co_consts`, `co_varnames`) sobre los `.pyc` originales —
  **esto documenta el diseño, no reconstruye la implementación**.
- SafeOps: backup de los `.pyc` originales tomado antes de cualquier otra
  acción, fuera del working tree — `~/backups/trading-bootstrap-recovery-<fecha>/`.

## 2. Estructura recuperada

TradingCompositionRoot
  __init__(self, trading, risk, redis)

  # privados — ensamblaje interno
  _build_position_store_redis(self)       # SIEMPRE Redis — solo desde assemble_live()
  _build_position_store_memory(self)      # SIEMPRE InMemory — desde assemble_paper()/assemble_rebalance()
  _build_portfolio(self, store)
  _build_guard(self)
  _build_feature_reader(self)             # único punto de trading autorizado a importar market_data (BC-47)
  _build_engine_live(self, data_source, guard, on_fill)
  _build_engine_paper(self, data_source, on_fill)

  # públicos — API del composition root
  build_gold_data_source(self)            # probe Fail-Fast de disponibilidad de datos para execute_paper.py
  assemble_live(self) -> TradingRuntime
  assemble_paper(self, data_source) -> TradingRuntime
  assemble_rebalance(self, *, use_redis) -> TradingRuntime

TradingRuntime (dataclass)
  engine: TradingEngine
  portfolio: PortfolioService
  tracker: Optional[TradeTracker]

## 3. Evidencia objetiva (extraída literalmente del bytecode, sin interpretar)

### 3.1 TradingCompositionRoot

- Clase `TradingCompositionRoot`, constructor `__init__(self, trading, risk, redis)`.
- Los tres parámetros tipan como `TradingConfig`, `RiskConfig`, `RedisConfig`
  (según imports en `co_names`).
- `_build_feature_reader` usa `GoldReader`
  (import: `market_data.adapters.outbound.storage.gold_reader`) y contiene
  el comentario literal: "Único punto de trading autorizado a importar
  market_data (BC-47)".
- `build_gold_data_source` contiene el comentario literal "BC-47 compliant"
  y su docstring explica que existe para permitir a callers como
  `execute_paper.py` hacer un probe Fail-Fast de disponibilidad de datos
  antes de invocar `assemble_paper()`.
- `assemble_live` retorna un `TradingRuntime`; `assemble_paper` recibe
  `data_source` como parámetro y retorna `TradingRuntime`.
- `assemble_rebalance` tiene un keyword-only arg: `use_redis`.
- El docstring completo del módulo se recuperó íntegro (ver §6).

### 3.2 TradingRuntime

- `dataclass` con campos: `engine`, `portfolio`, `tracker`.

### 3.3 redis_factory.build_redis_client

- Función única `build_redis_client(cfg)`.
- Lee de `cfg`: `host`, `port`, `db`, `password` (vía `get_secret_value()`),
  `socket_timeout`, `retry_on_timeout`.
- Docstring recuperado: SSOT del único lugar del sistema que instancia
  `redis.Redis` directamente; resuelve `password: SecretStr` correctamente
  (antes se perdía en `execute_live.py`/`rebalance.py`, que no la pasaban).

## 4. Comparación con el estado actual del repositorio (dependency-satisfaction)

| Dependencia esperada | ¿Existe hoy? | Ubicación real / hallazgo |
|---|---|---|
| TradingConfig | No | No existe en ocm/config/schema.py ni en ningún commit de ninguna rama (973 commits verificados por git grep exhaustivo) |
| RiskConfig | Sí | ocm/config/schema.py:694 |
| RedisConfig | Sí | ocm/config/schema.py:496 |
| CompositeFillObserver | No localizado | Ruta trading.observers.fill_observer referenciada en el bytecode; no se encontró el archivo ni la clase en el árbol actual |
| GoldFeatureReaderPort | Nombre incorrecto | El contrato real es FeatureReaderPort (market_data/ports/outbound/feature_reader.py) — el bytecode reflejaba un alias de import, no el nombre real de la clase |
| GoldReader | Sí | market_data/adapters/outbound/storage/gold_reader.py:74 — implementa FeatureReaderPort estructuralmente (Protocol, duck typing) |
| Contrato BC-47 (import-linter) | No | Nunca formalizado en architecture/importlinter.toml, pese a estar referenciado en comentarios del código recuperado — ver ADR-0004 |
| TradingEngine.build_live / build_paper | Sí | packages/trading/engine.py:211,282 — firma compatible con lo que _build_engine_live/_build_engine_paper necesitarían — ver ADR-0005 |
| ExecutionGuard | Sí | ocm/runtime/guard.py:49 |
| PortfolioService | Sí | Migrado en Fase 3 — PositionStore obligatorio por constructor (DIP) |
| PositionStore (Protocol) | Sí | portfolio/ports/position_store.py |
| InMemoryPositionStore / RedisPositionStore | Sí | portfolio/infra/memory_store.py, portfolio/infra/redis_store.py |

## 5. Decisiones de arquitectura derivadas

- **No se reconstruye el archivo literalmente ahora.** Reconstruir con
  piezas faltantes (TradingConfig, CompositeFillObserver) produciría
  código roto desde el primer import — viola Fail-Fast al introducir un
  módulo que nunca puede pasar su propia validación.
- **Este documento es el SSOT** de la arquitectura recuperada hasta que
  exista implementación real — cualquier reconstrucción futura debe
  partir de acá, no de memoria.
- **Orden de trabajo priorizado:**
  1. Cerrar este documento (completado con esta corrección).
  2. Auditar si el patrón de Bounded Context autónomo
     (domain/ports/adapters/services/bootstrap) se cumple realmente hoy
     en market_data, trading y portfolio, o es solo aspiracional.
  3. Revisar los contratos de import-linter existentes contra ese
     patrón.
  4. Corregir la ruta desactualizada de infrastructure/event_bus/ en
     ADR-0002.
  5. Recién una vez estabilizado market_data en Fase 1, reconstruir
     TradingConfig, CompositeFillObserver, el adaptador correcto hacia
     FeatureReaderPort y finalmente TradingCompositionRoot desde cero,
     usando esta arquitectura recuperada como guía funcional y
     portfolio/bootstrap/composition_root.py como referencia de estilo
     (patrón ya validado y committeado).

## 6. Anexo — docstring original del módulo (recuperado íntegro)

trading/bootstrap/composition_root.py
=======================================

TradingCompositionRoot — punto único de ensamblaje del sistema de trading.

Responsabilidad
---------------
Construye únicamente las dependencias EXTERNAS (PositionStore,
PortfolioService, TradeTracker, ExecutionGuard, CompositeFillObserver,
GoldLoaderAdapter) y delega en TradingEngine.build_live()/build_paper()
la construcción de las dependencias INTERNAS (Strategy, RiskManager,
OMS, Executor) — ya cubierto por esos factories existentes (DRY).

Constructor angosto (no AppConfig completo)
--------------------------------------------
Recibe TradingConfig + RiskConfig + RedisConfig — los mismos tipos
Pydantic que ya viven en AppConfig (SSOT de TIPOS), no el objeto
AppConfig completo. Motivo: AppConfig exige exchanges y pipeline
como campos obligatorios (config de market_data/Hydra) que no
significan nada para entrypoints CLI puros (live.py, paper.py,
rebalance.py) sin Hydra. Pedir AppConfig completo forzaría a esos
callers a inventar un ExchangeConfig/PipelineConfig dummy solo para
satisfacer la validación Pydantic — peor que la duplicación original.

El entrypoint Hydra (ocm) sigue usando AppConfig completo vía
OCMContainer (market_data) — ese composition root no se toca acá.

Principios: SOLID · DDD · SafeOps · KISS · DRY · Composition Root

## 7. Referencias

- Backup de .pyc originales: ~/backups/trading-bootstrap-recovery-<fecha>/
- ADRs relacionados: ADR-0003 (constructor angosto), ADR-0004 (regla BC-47),
  ADR-0005 (split TradingEngine/Composition Root), ADR-0006 (Portfolio
  como dueño de estado)
- Contratos de import-linter relacionados: BC-10, BC-38, BC-42, BC-43
- Referencia de estilo para la reconstrucción futura:
  packages/portfolio/bootstrap/composition_root.py
