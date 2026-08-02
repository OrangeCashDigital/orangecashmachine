# Auditoría de Composition Roots — `apps/`

**Fecha**: 2026-08-01
**Alcance**: 6 puntos de ensamblaje bajo `apps/` (`cli/main.py`, `cli/live.py`, `cli/paper.py`, `use_cases/rebalance.py`, `api/main.py`, `research/`), y su relación con la infraestructura compartida en `ocm/observability`, `ocm/runtime`, `ocm/config`
**Método**: Revisión manual de cada entrypoint vía `grep`/`cat` + verificación cruzada contra `docs/architecture/0002-event-driven-kappa-architecture.md`, `docs/audits/2026-05-market-data-audit.md` y `docs/DOMAIN.md`

---

## Índice

1. [Resumen Ejecutivo](#1-resumen-ejecutivo)
2. [Hallazgos Críticos](#2-hallazgos-críticos)
   - 2.1 `rebalance.py` — huérfano, sin invocador ni script
   - 2.2 `on_fill_composite` duplicado entre `execute_live.py`/`execute_paper.py`
   - 2.3 Sin lifecycle/shutdown en `live.py`/`paper.py` operando con capital real
3. [Hallazgos Moderados](#3-hallazgos-moderados)
   - 3.1 Infraestructura compartida madura no reutilizada
   - 3.2 `MetricsRuntime.shutdown()` nunca invocado, ni siquiera en el Composition Root maduro
   - 3.3 Conexión Redis construida inline en `execute_live.py`
4. [Hallazgos Leves](#4-hallazgos-leves)
   - 4.1 `apps/api/` y `apps/research/` fuera del inventario original
   - 4.2 Ambigüedad terminológica: "aplicación" vs "bounded context"
   - 4.3 Tercera capa de composición no documentada
5. [Mapa de Entrypoints](#5-mapa-de-entrypoints)
6. [Verificación Cruzada contra Documentación Existente](#6-verificación-cruzada-contra-documentación-existente)
7. [Plan de Acción](#7-plan-de-acción)

---

## 1. Resumen Ejecutivo

No existe un modelo único de Composition Root en `apps/` — coexisten tres niveles de madurez distintos sin que la asimetría estuviera documentada antes de esta auditoría. `apps/api/main.py` resuelve lifecycle/shutdown correctamente (patrón `lifespan()` de FastAPI); `cli/main.py` resuelve DI/Config/Observability correctamente pero omite shutdown explícito de `MetricsRuntime`; `cli/live.py`/`cli/paper.py` no reutilizan ninguna infraestructura compartida existente. Se identificaron **6 hallazgos**:

| Severidad | Cantidad | Descripción |
|-----------|----------|-------------|
| 🔴 Crítico | 3 | Código huérfano operando con riesgo implícito, duplicación de lógica de dominio, ausencia de shutdown en proceso con capital real |
| 🟡 Moderado | 3 | Infraestructura SSOT existente no reutilizada, lifecycle incompleto incluso en el Root maduro, conexión a infraestructura duplicada |
| 🟢 Leve | 3 | Cobertura de inventario incompleta, términos de dominio usados sin precisión, capa de composición no documentada |

---

## 2. Hallazgos Críticos

### 2.1 `rebalance.py` — huérfano, sin invocador ni script

**Archivo**: `apps/app/use_cases/rebalance.py`
**Principio violado**: Dependency-Satisfaction — código que declara dependencias completas (`PortfolioService`, `RebalanceService`, `OMS`, `RiskManager`, `PaperExecutor`) sin que nada en el grafo de llamadas lo alcance

`grep -rn "from app.use_cases.rebalance import" apps/ packages/` solo encuentra el propio archivo. No tiene entrada en `[project.scripts]` de `pyproject.toml`. Construye su propio `PortfolioService`/`OMS`/`RiskManager` inline dentro de `execute()`, mezclando ensamblaje (Composition Root) con lógica de ejecución (Application) en el mismo método.

**Consecuencias**:
- Cualquier bug introducido aquí no se detecta en ejecución normal — solo en tests directos del módulo, si existen
- Constituye una superficie de riesgo latente: si en el futuro alguien lo conecta sin re-auditar, hereda el estado actual sin revisión

**Propuesta**: Decisión explícita — conectar a un CLI propio (`cli/rebalance.py` + script `rebalance` en `pyproject.toml`) o eliminar. No corresponde dejarlo en este estado intermedio.

---

### 2.2 `on_fill_composite` duplicado entre `execute_live.py`/`execute_paper.py`

**Archivos**: `apps/app/use_cases/execute_live.py`, `apps/app/use_cases/execute_paper.py`
**Principio violado**: DRY

La función interna `on_fill_composite(order)` (~30 líneas: mapeo `symbol → buy_order_id`, invocación de `tracker.on_fill()`, `portfolio.open_position()`/`close_position()`) está duplicada casi línea por línea en ambos módulos. No es boilerplate de bootstrap — es lógica de dominio (bridge OMS↔Portfolio) que pertenece a `trading/` o `portfolio/`, no a un use case de aplicación.

**Consecuencias**:
- Un fix del bug ya documentado en el propio código ("SELL sin BUY previo registrado") aplicado en un archivo y no en el otro deja el bug vivo en modo live O paper sin que ningún test lo detecte, salvo cobertura específica en ambos
- Viola SSOT: dos fuentes de verdad para la misma regla de negocio

**Propuesta**: Extraer a `trading/execution/fill_sync.py` como función pura parametrizada por `tracker` y `portfolio`, consumida por ambos `build_*_engine()`.

---

### 2.3 Sin lifecycle/shutdown en `live.py`/`paper.py` operando con capital real

**Archivos**: `apps/app/cli/live.py`, `apps/app/cli/paper.py`, `apps/app/use_cases/execute_live.py`, `apps/app/use_cases/execute_paper.py`
**Principio violado**: SafeOps, Resiliencia

`grep` por `atexit`, `signal.signal`, `__aexit__`, `finally` de cierre no devuelve resultados en `apps/app/cli/` ni `apps/app/use_cases/`. `cli/live.py` trae advertencia explícita en su propio docstring (`⚠️ LIVE TRADING — CAPITAL REAL`) y construye una conexión Redis (`RedisPositionStore`) sin garantía de cierre ordenado ante `SIGINT`/`SIGTERM`.

**Consecuencias**: riesgo operacional real, no solo prolijidad — una interrupción abrupta durante una operación con capital real no tiene punto de cierre garantizado para la conexión de estado (Redis) ni para el exchange.

**Propuesta**: Adoptar el patrón ya existente y probado en `apps/api/main.py::lifespan()` — startup fail-fast + shutdown ordenado explícito — como referencia, no diseñar uno nuevo desde cero.

---

## 3. Hallazgos Moderados

### 3.1 Infraestructura compartida madura no reutilizada

**Archivos**: `apps/app/cli/live.py`, `apps/app/cli/paper.py` vs. `ocm/observability/*`, `ocm/runtime/*`
**Principio violado**: SSOT, DRY

`ocm.observability` (logging multi-sink: consola, `app_*.log` JSON, `errors_*.log`, Loki, Prometheus counters) y `ocm.runtime` (`RunConfig`, `RuntimeContext`, ambos inmutables y fail-fast en construcción) ya existen, están maduros y en uso por `cli/main.py`. `cli/live.py`/`cli/paper.py` no los importan — usan `argparse` plano y `logger.remove()`/`logger.add()` duplicado línea por línea entre ambos archivos.

Verificado además: `RunConfig.from_env()` **no depende de Hydra** — resuelve desde variables de entorno planas (`OCM_ENV`, `OCM_DEBUG`, vía `ocm.config.env_vars`), por lo que la adopción por `live.py`/`paper.py` no exige migrar a Hydra.

**Propuesta**: Evaluar adopción total vs. subconjunto (dado que Trading/Portfolio corre como ciclo puntual, no proceso continuo). Ver Decisión 3 del ADR-0003.

---

### 3.2 `MetricsRuntime.shutdown()` nunca invocado, ni siquiera en el Composition Root maduro

**Archivo**: `ocm/observability/metrics_runtime.py` (definición) vs. `apps/app/cli/main.py::run_application()` (uso)

`MetricsRuntime` implementa `shutdown()` (idempotente, limpia `_started`), pero `run_application()` — el Composition Root más maduro auditado — nunca lo llama. El gap de lifecycle no es "falta construir el mecanismo", es "falta invocarlo", y esto aplica incluso al ejemplo que se consideraría de referencia.

**Propuesta**: Agregar la llamada a `runtime.shutdown()` en el camino de salida de `run_application()`, idealmente dentro de un bloque `finally` o un context manager equivalente al `lifespan()` de `apps/api/`.

---

### 3.3 Conexión Redis construida inline en `execute_live.py`

**Archivos**: `apps/app/use_cases/execute_live.py` vs. `apps/api/deps.py`

`build_live_engine()` construye `redis_lib.Redis(host=..., port=..., ...)` a mano. `apps/api/deps.py` ya expone `_redis_pool()` como factory reutilizable, consumida por `apps/api/main.py`. Mismo patrón de infraestructura no reutilizada del hallazgo 3.1, aplicado a la capa de conexión.

**Propuesta**: Evaluar si `_redis_pool()` puede promoverse fuera de `api/` a un módulo compartido (p. ej. `ocm/infra/redis.py`) consumible por `apps/api/` y `apps/app/use_cases/` sin crear una dependencia de `app` hacia `api`.

---

## 5. Mapa de Entrypoints

| Entry Point | Composition Root propio | Bounded Contexts | Script `pyproject.toml` | Estado |
|---|---|---|---|---|
| `cli/main.py` | Sí — DI container completo (Hydra + `RuntimeContext` + `MetricsRuntime`) | `market_data` | `ocm` | Activo |
| `apps/api/main.py` | Sí — el más completo (`lifespan()` fail-fast + shutdown ordenado) | *(gateway, sin bounded context de dominio propio)* | `ocm-api` | Activo |
| `cli/live.py` | Parcial — delega a `execute_live.py::build_live_engine()` → `TradingEngine.build_live()` | `trading`, `portfolio` | `live` | Activo |
| `cli/paper.py` | Parcial — delega a `execute_paper.py::build_paper_engine()` → `TradingEngine.build_paper()` | `trading`, `portfolio` | `paper` | Activo |
| `use_cases/rebalance.py` | No — sin CLI ni script | `trading`, `portfolio` | *(ninguno)* | Huérfano |
| `apps/research/` | No — solo `data_access.py`, sin `main.py` | *(sin determinar)* | *(ninguno)* | Stub |

---

## 6. Verificación Cruzada contra Documentación Existente

| Afirmación de esta auditoría | Contrastada contra | Resultado |
|---|---|---|
| "No existe `StrategyConsumer` ni consumer de `RiskGate`" | `docs/DOMAIN.md` §5.4 | **Matizada** — `RiskGate` es contrato adelantado a propósito (a la espera de `RiskGateConsumer`, parte de la migración event-driven de ADR-0002), no código huérfano sin plan |
| "Falta definir un launcher común" | `docs/architecture/0001-...md`, Decisión 3 | **Posible redundancia** — `packages/control_plane/` ya está propuesto con ese rol (`replay.py`, `backfill.py`, `repair.py`, `scheduler.py`, `cli.py` como composition root propio), aunque todavía no implementado. Requiere confirmar si su alcance (operar el sistema) coincide con "elegir qué aplicación de trading correr" antes de asumirlo como el mismo componente |
| "`cli/main.py` es el Composition Root de referencia a imitar" | `docs/audits/2026-05-market-data-audit.md`, hallazgo 3.2 | **Matizada** — el `CompositionRoot` que usa `cli/main.py` (`infrastructure/bootstrap/composition_root.py`) tiene SRP violado documentado (3 responsabilidades no relacionadas en un mismo objeto). No es un modelo perfecto a copiar sin revisión |

---

## 7. Plan de Acción

### Prioridad 🔴 — Inmediata

| # | Acción | Archivos | Esfuerzo |
|---|---|---|---|
| 1 | Decidir destino de `rebalance.py`: conectar (CLI + script) o eliminar | 1-2 archivos | 30 min (decisión) + variable (implementación) |
| 2 | Extraer `on_fill_composite` a módulo compartido de `trading/` | 3 archivos | 1 h |
| 3 | Agregar shutdown ordenado a `live.py` (mínimo: cierre de conexión Redis ante `SIGINT`/`SIGTERM`) | 2 archivos | 1-2 h |

### Prioridad 🟡 — Corto Plazo

| # | Acción | Archivos | Esfuerzo |
|---|---|---|---|
| 4 | Agregar llamada a `MetricsRuntime.shutdown()` en `run_application()` | 1 archivo | 15 min |
| 5 | Evaluar adopción de `RunConfig`/`bootstrap_logging()` en `live.py`/`paper.py` | 2-4 archivos | 2-3 h |
| 6 | Promover `_redis_pool()` fuera de `api/deps.py` a módulo compartido | 2-3 archivos | 1 h |

### Prioridad 🟢 — Medio Plazo

| # | Acción | Archivos | Esfuerzo |
|---|---|---|---|
| 7 | Auditar `apps/api/` (routers, middleware, auth) — no cubierto en profundidad aquí | N/D | 2-3 h |
| 8 | Determinar destino de `apps/research/` (aplicación futura vs. utilidad) | N/D | 30 min (decisión) |
| 9 | Leer `packages/control_plane/` (si existe implementación parcial) para confirmar o descartar redundancia con "launcher común" | N/D | 1 h |
| 10 | Corregir SRP de `infrastructure/bootstrap/composition_root.py` (ya propuesto en auditoría de mayo, ítem #5) antes de tomarlo como referencia definitiva | 4 archivos | 1 h |

---

## Apéndice: Estadísticas de la Auditoría

| Métrica | Valor |
|---|---|
| Entrypoints auditados | 6 |
| Composition Roots completos | 1 (`apps/api/main.py`) |
| Composition Roots parciales | 3 (`cli/main.py`, `cli/live.py`, `cli/paper.py`) |
| Composition Roots huérfanos | 1 (`rebalance.py`) |
| Entrypoints sin determinar | 1 (`apps/research/`) |
| Violaciones críticas | 3 |
| Violaciones moderadas | 3 |
| Hallazgos leves | 3 |
| Documentos existentes verificados por cruce | 3 (`ADR-0002`, auditoría de mayo, `DOMAIN.md`) |

---

*Generado el 2026-08-01. Auditoría basada en revisión manual de código fuente vía terminal (grep/cat) más verificación cruzada contra documentación arquitectónica existente.*
