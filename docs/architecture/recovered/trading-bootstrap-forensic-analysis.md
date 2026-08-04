# TradingCompositionRoot — Forensic Analysis (Bootstrap Recovery)

**Bounded context:** `trading`
**Fecha del hallazgo:** 2026-08-02
**Estado:** Diseño recuperado — implementación pendiente de reconstrucción (ver §7)
**ADRs relacionados:** ADR-0003, ADR-0004, ADR-0005, ADR-0006

> **Actualización 2026-08-03:** la reconstrucción del bootstrap de trading
> está COMPLETADA (`packages/trading/bootstrap/composition_root.py`, v3,
> ADR-0003 enmendado). Los factories `TradingEngine.build_live()/build_paper()`
> que este forense documenta como colaboradores fueron **eliminados** — el
> root ensambla todas las dependencias (ADR-0012, que reemplaza ADR-0005).
> Las secciones siguientes describen el diseño histórico recuperado.

---

## 0. Resumen ejecutivo

Durante la auditoría del bounded context `trading` se identificó la
pérdida del código fuente de `packages/trading/bootstrap/`
(`composition_root.py`, `redis_factory.py`, `__init__.py`). El análisis
histórico confirmó que dichos archivos nunca fueron registrados en Git —
verificado por búsqueda exhaustiva en los 973 commits de todas las
ramas — por lo que la única evidencia disponible corresponde al bytecode
compilado (`.pyc`, Python 3.13).

Mediante introspección del bytecode fue posible reconstruir con **alto
grado de confianza** la arquitectura, la interfaz pública, las
responsabilidades del componente y las decisiones de diseño originales
(el docstring del módulo se recuperó íntegro). No fue posible recuperar
la implementación interna exacta (cuerpo de los métodos, orden de
llamadas, manejo de errores) — ver §6.

Este documento es el SSOT de dicha recuperación y sirve como base para
la futura reimplementación, siguiendo el plan de §7.

## 1. Contexto y metodología

### Herramientas evaluadas

| Herramienta | Resultado |
|---|---|
| `marshal` | ✓ usada — introspección de `co_names`, `co_consts`, `co_varnames` |
| `dis` | ✓ usada — desensamblado auxiliar para confirmar estructura |
| `inspect` | ✓ usada — validación de firmas de funciones anidadas |
| `decompyle3` | ✗ descartada |
| `uncompyle6` | ✗ descartada |
| `xdis` | ✗ descartada |

### Motivo del descarte

Python 3.13 modifica profundamente el bytecode (intérprete adaptativo,
PEP 659). Actualmente no existen decompiladores públicos maduros que
soporten ese formato, por lo que la decompilación completa de la lógica
no es viable hoy.

### Limitaciones del método

- `marshal`/`dis`/`inspect` exponen **estructura** (nombres, imports,
  firmas, strings literales), no el **comportamiento** (cuerpo real de
  cada método).
- No hay forma de verificar, por este método, el orden interno de
  operaciones ni el manejo de excepciones dentro de cada función.

### Riesgos del proceso

- Backup de los `.pyc` originales tomado antes de cualquier otra acción,
  fuera del working tree — `~/backups/trading-bootstrap-recovery-<fecha>/`
  (SafeOps: preserva la única evidencia disponible ante cualquier error
  de manipulación posterior).

## 2. Evidencia objetiva

Extraída literalmente del bytecode. Sin interpretación — cada línea de
esta sección es verificable reabriendo los `.pyc` respaldados.

### Clase encontrada

TradingCompositionRoot

### Constructor

__init__(self, trading, risk, redis)

### Métodos encontrados

- assemble_live(self) -> TradingRuntime
- assemble_paper(self, data_source) -> TradingRuntime
- assemble_rebalance(self, *, use_redis) -> TradingRuntime
- build_gold_data_source(self)
- _build_position_store_redis(self)
- _build_position_store_memory(self)
- _build_portfolio(self, store)
- _build_guard(self)
- _build_feature_reader(self)
- _build_engine_live(self, data_source, guard, on_fill)
- _build_engine_paper(self, data_source, on_fill)

### Dataclass encontrada

TradingRuntime — campos: engine, portfolio, tracker

### Imports encontrados (co_names)

- TradingEngine (trading.engine)
- PortfolioService (portfolio.services.portfolio_service)
- PositionStore (portfolio.ports.position_store)
- GoldReader (market_data.adapters.outbound.storage.gold_reader)
- TradeTracker (trading.analytics.trade_tracker)
- TradingConfig, RiskConfig, RedisConfig (ocm.config.schema)
- GoldFeatureReaderPort (shared.contracts.boundaries — alias de import, ver §4)
- dataclass, dataclasses, typing, TYPE_CHECKING, Optional, annotations, __future__

### Función auxiliar encontrada (redis_factory.py)

build_redis_client(cfg) — lee de cfg: host, port, db, password
(vía get_secret_value()), socket_timeout, retry_on_timeout

### Strings literales recuperados

- "Único punto de trading autorizado a importar market_data (BC-47)"
- "BC-47 compliant"
- "SIEMPRE Redis — usar solo desde assemble_live()."
- "SIEMPRE en memoria — usar desde assemble_paper()/assemble_rebalance() dry-run."
- "Composition root de trading. Un único punto de entrada por modo."
- "Resultado del ensamblaje — reemplaza la tupla (engine, portfolio)."
- El docstring completo del módulo (ver §Anexo al final del documento)

## 3. Reconstrucción arquitectónica

A partir del conjunto de imports y métodos puede inferirse que
TradingCompositionRoot era responsable del ensamblaje completo del
runtime de trading: construye las dependencias externas al ciclo de
trading (PositionStore, PortfolioService, TradeTracker, ExecutionGuard,
GoldReader) y delega en TradingEngine.build_live()/build_paper() las
dependencias internas (Strategy, RiskManager, Executor, OMS).
*(2026-08-03: la delegación fue eliminada — ver ADR-0012.)*

No existen evidencias de lógica de negocio dentro del bootstrap (no hay
strings ni imports asociados a cálculo de señales, riesgo o ejecución).
La responsabilidad parece limitarse exclusivamente a Dependency
Injection y ensamblaje (Composition Root pattern).

**Confianza: Muy alta.** Sustentado por el docstring recuperado íntegro,
que declara explícitamente esta responsabilidad, y por la ausencia total
de imports de librerías de cálculo/estrategia en co_names.

## 4. Comparación contra el estado actual del repositorio

| Componente | Estado recuperado | Estado actual | Observación |
|---|---|---|---|
| TradingCompositionRoot | Existía | Perdido | Reimplementar (ver §7) |
| TradingRuntime (dataclass) | Existía | Perdido | Reimplementar (ver §7) |
| RedisFactory (build_redis_client) | Existía | Existe en portfolio/infra/redis_factory.py | **OBSOLETO** — portfolio es dueño de Redis (BC-43); NO recrear (ver §7) |
| TradingConfig | Existía (tipo esperado) | No existe en el repo | Bloqueante — crear antes de reconstruir |
| CompositeFillObserver | Existía (import) | No localizado | Bloqueante — ubicar o recrear |
| GoldFeatureReaderPort | Referenciado (alias) | No existe con ese nombre | El contrato real es FeatureReaderPort |
| RiskConfig | Existía | Existe (ocm/config/schema.py:694) | Compatible |
| RedisConfig | Existía | Existe (ocm/config/schema.py:496) | Compatible |
| GoldReader | Existía | Existe (market_data/adapters/outbound/storage/gold_reader.py:74) | Compatible — implementa FeatureReaderPort estructuralmente |
| TradingEngine.build_live / build_paper | Existía (colaborador) | Eliminado (2026-08-03, ADR-0012) | El root ensambla todo — ver ADR-0012 |
| ExecutionGuard | Existía | Existe (ocm/runtime/guard.py:49) | Compatible |
| PortfolioService | Existía | Existe (migrado en Fase 3, PositionStore obligatorio por constructor) | Compatible |
| PositionStore (Protocol) | Existía | Existe (portfolio/ports/position_store.py) | Compatible |
| InMemoryPositionStore / RedisPositionStore | Existía | Existe (portfolio/infra/) | Compatible |
| Contrato BC-47 (import-linter) | Referenciado en comentarios | No formalizado en architecture/importlinter.toml | Bloqueante — ver ADR-0004 |

## 5. Análisis arquitectónico

- **Constructor angosto (TradingConfig+RiskConfig+RedisConfig, no
  AppConfig completo):** AppConfig exige exchanges y pipeline como
  campos obligatorios (config de market_data/Hydra) sin significado para
  CLIs puros sin Hydra. Ver ADR-0003.
- **GoldReader solo instanciable desde el bootstrap (BC-47):** concentra
  el único acoplamiento externo de trading hacia market_data en un
  archivo auditable, en vez de esparcirlo por execution/risk/strategies.
  Ver ADR-0004.
- **TradingEngine.build_live/build_paper construyen solo dependencias
  internas al ciclo:** evita duplicar lógica de ensamblaje ya cubierta
  por esos factories (DRY). Ver ADR-0005.
  *(2026-08-03: ambos factories eliminados — el root ensambla todo, ADR-0012.)*
- **RedisFactory separado del Composition Root:** único lugar del
  sistema que instancia redis.Redis directamente y resuelve
  password:SecretStr correctamente — evita que cada caller reimplemente
  esa resolución (y el bug histórico donde execute_live.py/rebalance.py
  no la pasaban).
- **PortfolioService como colaborador, no como estado propio del
  bootstrap:** consistente con que portfolio es el único dueño del
  estado de posiciones. Ver ADR-0006.

## 6. Riesgos

### No se recuperó

- Implementación exacta de cada método (cuerpo real del código).
- Lógica interna de validación o transformación de datos.
- Orden exacto de llamadas dentro de cada método.
- Manejo de errores y excepciones.
- Logging y niveles de severidad usados.

### Sí se recuperó

- Responsabilidades del componente.
- Interfaz pública completa (firmas de todos los métodos).
- Colaboradores (qué clases/módulos importa y usa).
- Diseño y decisiones de arquitectura (docstring íntegro).
- Estructura de dependencias hacia el resto del sistema.

## 7. Plan de reconstrucción

> **Corrección 2026-08-03 (auditoría de composition roots):** este plan
> recuperó intención de diseño por bytecode, no una especificación
> ejecutable. Dos pasos quedaron OBSOLETOS y NO deben ejecutarse:
>
> - **Paso 2 (Recrear RedisFactory):** obsoleto. `portfolio` es el único
>   dueño de Redis (BC-43, Fase 3). `build_redis_client` vive en
>   `portfolio/infra/redis_factory.py`. No se recrea.
> - **`_build_position_store_*`/`_build_portfolio` (§2):** NO reconstruir.
>   Esos métodos predatan ADR-0006/BC-43; reconstruirlos violaría BC-43
>   directamente. El composition root recibe `portfolio` ya ensamblado por
>   `PortfolioCompositionRoot.assemble()` (decisión D2).

1. **Crear TradingConfig** en `ocm/config/schema.py` — bloqueante, no
   existe hoy en ningún commit del repo. [HECHO en WIP 2026-08-03]
2. ~~**Recrear RedisFactory**~~ (`build_redis_client(cfg)`) — **ELIMINADO**,
   ver corrección arriba. `portfolio/infra/redis_factory.py` ya existe.
3. **Crear TradingRuntime** (dataclass: engine, portfolio, tracker).
4. ~~**Localizar o recrear CompositeFillObserver**~~ — **CERRADO**: el
   símbolo real es `trading/execution/fill_sync.py::build_fill_sync`
   (hallazgo H7 de la auditoría 2026-08-03). No se crea una clase nueva.
5. **Crear TradingCompositionRoot** con la interfaz aprobada
   (`__init__(trading, risk, portfolio, guard=None)`, ver ADR-0003
   enmendado) y usando `portfolio/bootstrap/composition_root.py` como
   referencia de estilo (patrón ya validado y committeado).
6. **Formalizar el contrato BC-50** en `architecture/importlinter.toml`
   (BC-47 quedó ocupado por shared.kafka — ver ADR-0004 enmendado).
7. **Agregar pruebas** unitarias para cada método público
   (assemble_live, assemble_paper, assemble_rebalance,
   build_gold_data_source).
8. **Comparar comportamiento** contra el uso real actual en
   execute_live.py/execute_paper.py/rebalance.py.
9. **Eliminar cualquier bootstrap temporal/parche** una vez que
   TradingCompositionRoot esté committeado y validado.

**Precondición de todo lo anterior:** no arrancar este plan hasta
estabilizar `market_data` en Fase 1, según orden priorizado ya definido.

## 8. Lecciones aprendidas

- Nunca confiar únicamente en Git como respaldo del conocimiento — este
  módulo nunca fue commiteado y solo sobrevivió por bytecode residual.
- Los Composition Roots son componentes críticos y deben tener cobertura
  documental equivalente a la del código (ver GOVERNANCE.md §1).
- Las decisiones arquitectónicas deben registrarse mediante ADR antes de
  implementar, no reconstruirse de memoria después de perderse.
- Los límites entre bounded contexts deben protegerse con reglas
  automáticas (import-linter) — BC-47 estaba documentado en comentarios
  pero nunca formalizado como contrato ejecutable.
- Todo componente crítico debe tener documentación suficiente para
  permitir su reconstrucción aun cuando el código fuente se pierda.

## Anexo — docstring original del módulo (recuperado íntegro)

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

*(Docstring recuperado del diseño histórico. La implementación v3
(2026-08-03) elimina la delegación: el root construye también las
internas, ver ADR-0012.)*

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

## Nota de proceso — atomic commits (2026-08-03)

El `composition_root.py` de trading se perdió dentro de un commit
`feat(portfolio)` que tocó dos bounded contexts (portfolio + trading/
bootstrap). Lección: **un commit = un cambio lógico en un BC**. La
reconstrucción forense fue necesaria solo porque el archivo nunca se
committeó de forma aislada — un commit atómico habría preservado el
código o, al menos, su commit de referencia. Ver también ADR-0003.

## Referencias

- Backup de .pyc originales: ~/backups/trading-bootstrap-recovery-<fecha>/
- ADRs relacionados: ADR-0003 (constructor angosto), ADR-0004 (regla BC-47),
  ADR-0005 (split TradingEngine/Composition Root), ADR-0006 (Portfolio
  como dueño de estado)
- Contratos de import-linter relacionados: BC-10, BC-38, BC-42, BC-43
- Referencia de estilo para la reconstrucción futura:
  packages/portfolio/bootstrap/composition_root.py
