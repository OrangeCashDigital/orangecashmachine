# Verificación de ADRs vs Código Real (log, 2026-08-02)

> **Log de verificación, no un ADR.** Reubicado desde
> `docs/architecture/0006-verificacion-adrs-vs-codigo.md` (2026-08-03, Fase D
> de la auditoría de composition roots) para eliminar la colisión de
> numeración con `decisions/ADR-0006-portfolio-owns-position-state.md`. La
> serie canónica de ADRs es `docs/architecture/decisions/ADR-NNNN-*`
> (ver `GOVERNANCE.md §3` y §9).

**Fecha:** 2026-08-02
**Método:** Revisión manual de los 6 ADRs (`docs/architecture/0000-0005`) contrastada contra el código fuente real (imports, símbolos, scripts, docker-compose, git history).

Este documento registra el estado de cada afirmación de los ADRs frente al código tal como existe hoy, señalando los desvíos (drift) y las contradicciones encontradas.

---

## Resumen ejecutivo

| ADR | Estado |
|---|---|
| ADR-0000 — Principios arquitectónicos | ✅ Verificado (37 contratos exactos) |
| ADR-0001 — Mapa de bounded contexts | ⚠️ Drift parcial (control_plane y build_*() no existen) |
| ADR-0002 — Kappa / event-driven | 🔴 Contradicción principal (Dagster eliminado) |
| ADR-0003 — Composition Root jerárquico | ✅ Consistente, pendientes documentados |
| ADR-0004 — RebalanceService adelantado | ✅ Verificado |
| ADR-0005 — Hydra reemplaza CLIs legados | ✅ Verificado, pendientes confirmados |

---

## ADR-0000 — Principios arquitectónicos — ✅ VERIFICADO

| Afirmación | Verificación |
|---|---|
| P1 SSOT: tópico Kafka único `trades.raw` (sin `market.trades.raw`) | ✅ `shared/kafka/topics.py:108` → `TOPIC_TRADES_RAW = "trades.raw"` (único) |
| P1 SSOT: `on_fill_composite` corregido vía `build_fill_sync()` | ✅ `packages/trading/execution/fill_sync.py:50` |
| P2 DIP: colaboradores vía `Protocol` respetando BC-13 | ✅ `fill_sync.py` recibe `tracker`/`portfolio` por protocolo; BC-13 activo en `architecture/importlinter.toml:431` |
| P3: "37 contratos activos" | ✅ Exacto — 38 `name = "BC-"`, 1 comentado → 37 activos |
| P4 Fail-Soft: `LiveEngineResources.shutdown()` cierra por recurso | ✅ `apps/app/use_cases/execute_live.py:97` |
| P5 Event First: `RiskGate` adelantado a propósito | ✅ `shared/contracts/boundaries.py:128` + tópicos `signals.raw`/`signals.approved`/`signals.rejected` en `shared/kafka/topics.py` |
| P5: `rebalance.py` era huérfano | ✅ Eliminado (no existe en `apps/app/use_cases/`) |

---

## ADR-0001 — Mapa de bounded contexts — ⚠️ DRIFT PARCIAL

| Afirmación | Verificación | Desvío |
|---|---|---|
| BCs `market_data`, `trading`, `portfolio` | ✅ Existen (`packages/`) | — |
| BC `control_plane` | ❌ **NO existe** como paquete | ADR lo presenta como BC real "tal como existe hoy" (`docs/DOMAIN.md` lo lista como deuda). `ADR-0002` lo declara explícitamente "todavía no implementado" — el mapa lo sobre-vende. |
| `build_market_data()`, `build_trading()` | ❌ **No existen** en el árbol | Solo existe el equivalente de portfolio: `PortfolioCompositionRoot.assemble()` (`packages/portfolio/bootstrap/composition_root.py`). ADR los presenta como funciones de ensamblaje reales. |
| "Composition Root General vive en el nivel más alto de `apps/`" | ❌ **No existe** un módulo único | Cada entrypoint (`cli/main.py`, `live_hydra.py`, `paper_hydra.py`) es su propio CR. `ADR-0003` lo permite ("o equivalente por entrypoint"), pero ADR-0001 lo describe como algo materializado. |
| `build_live_engine()` → `LiveEngineResources` (commit `6f4ff38`) | ✅ Verificado | — |

**Lectura:** ADR-0001 es la **intención de arquitectura**, no el estado actual. Sus "Decisión" mezclan implementado con planificado sin distinguir. Debería marcar `control_plane`, `build_*()` y el CR General como *pendientes*.

---

## ADR-0002 — Kappa / event-driven — 🔴 CONTRADICCIÓN PRINCIPAL

| Afirmación | Verificación |
|---|---|
| `ohlcv.raw` como SSOT cross-proceso | ✅ `TOPIC_OHLCV_RAW` en `shared/kafka/topics.py:64` |
| Quality gate ANTES de publicar a Kafka | ✅ `IncrementalStrategy._run`: `ctx.quality.run` (L77) → `ctx.publisher.publish_chunk` (L116) en `packages/market_data/application/strategies/incremental.py` |
| EventBus cableado: `QualityPipelineConsumer` observador post-hoc | ✅ `packages/market_data/infrastructure/bootstrap/pipeline_factory.py:101-132` |

### Contradicción — Dagster

Decisión 4 del ADR: *"Dagster permanece instalado y disponible... No se elimina el servicio del `docker-compose.yml` hasta que esa decisión esté tomada explícitamente."*

**El código dice lo contrario.** El commit `9eb6de3` (2026-08-01, posterior a la última enmienda del ADR) eliminó Dagster por completo:

| Evidencia | Estado |
|---|---|
| `dagster_defs.py` (raíz) | ❌ No existe |
| `infrastructure/dagster/` | ❌ No existe |
| Servicio en `docker-compose.yml` | ❌ No está (solo redis, kafka, zookeeper, monitoring) |
| Modo `./run.sh dagster` | ❌ No existe (solo `ocm | live | paper`) |
| Dependencia `dagster` en `pyproject.toml` | ❌ No está |

El "Pendiente #1" del ADR (¿systemd timers suficiente?) quedó **resuelto de facto** por la eliminación, pero el ADR no fue enmendado. Adicionalmente:

- **Ruta desactualizada (menor):** ADR dice `infrastructure/event_bus/`; la ruta real es `packages/market_data/infrastructure/event_bus/` y `packages/market_data/ports/outbound/event_bus.py` (el `infrastructure/` raíz solo contiene `redis/`).
- **Referencias residuales de Dagster** fuera del ADR: `AGENTS.md` (líneas 25-29, 51, 93, 103) y la descripción de `pyproject.toml:24` siguen mencionando Dagster como presente.

---

## ADR-0003 — Composition Root jerárquico — ✅ CONSISTENTE (pendientes documentados)

| Afirmación | Verificación |
|---|---|
| Decisión 1: CR por bounded context | ✅ `PortfolioCompositionRoot` como único CR por BC implementado |
| Decisión 1: `build_market_data()`/`build_trading()`/`build_control_plane()` | ⏳ No creados — el propio ADR los lista como consecuencia ("Se requiere crear...") ✓ coherente |
| Decisión 2: lifecycle es responsabilidad del BC | ✅ `LiveEngineResources` (commit `6f4ff38`) |
| Decisión 3: SUPERSEDED por ADR-0005 | ✅ Implementado (Hydra CLIs oficiales) |
| Consecuencia: `redis_client` migra al CR de portfolio "cuando viene inyectado" | ✅ `execute_live.py` — el fallback Redis inline (`portfolio_service is None`) se eliminó el 2026-08-03 (Fase C): `portfolio_service` es obligatorio e inyectado por `PortfolioCompositionRoot` |
| Consecuencia: 3.2 `MetricsRuntime.shutdown()` en `cli/main.py` | ⏳ Sigue abierto — no hay `.shutdown()` en `apps/app/cli/main.py` (el ADR lo registra como pendiente) ✓ |

---

## ADR-0004 — RebalanceService como capacidad adelantada — ✅ VERIFICADO

| Afirmación | Verificación |
|---|---|
| `rebalance.py` eliminado (commit `3d5ab3f`) | ✅ No existe en `apps/app/use_cases/` |
| `RebalanceService`/`RebalanceSignal` conservados | ✅ `packages/portfolio/services/rebalance_service.py` |
| `DOMAIN.md` línea 75 referencia ADR-0004 | ✅ Verificado |

---

## ADR-0005 — Hydra/AppConfig reemplaza CLIs legados — ✅ VERIFICADO (pendientes confirmados)

| Afirmación | Verificación |
|---|---|
| Scripts `live`/`paper` → `app.cli.live_hydra`/`app.cli.paper_hydra` | ✅ `pyproject.toml:149-150` |
| `run.sh` modos `live`/`paper` → `live_hydra`/`paper_hydra` | ✅ `run.sh:34-35` |
| `PortfolioCompositionRoot` | ✅ `packages/portfolio/bootstrap/composition_root.py` |
| `PortfolioService` con DIP estricto (`store: PositionStore` obligatorio) | ✅ `portfolio_service.py` — firma `store: PositionStore` sin default; sin fallback interno a `InMemoryPositionStore` (nótese: el docstring de clase en L58 aún dice "default: InMemoryPositionStore" — desactualizado) |
| `PortfolioConfig` integrado a `AppConfig`/Hydra | ✅ `ocm/config/schema.py:519,743` + `config/portfolio/portfolio.yaml` |
| Contrato BC-43 (PositionStore solo desde Composition Root) | ✅ `architecture/importlinter.toml:779` |
| Legados `live.py`/`paper.py` aún presentes (eliminación pendiente de confirmación) | ✅ Siguen en `apps/app/cli/`; ningún módulo los importa (solo docstrings) |
| Docstrings "Coexiste con `app/cli/paper.py` sin reemplazarlo" obsoletos | ✅ Corregido — commit `fef31a0` (2026-08-02) actualiza `live_hydra.py`/`paper_hydra.py` para reflejar ADR-0005 |
| Nota 2.3/3.1: manejo de señales SIGINT/SIGTERM "no verificado" | 🔍 **Ahora verificado — parcial:** `live_hydra.py:162-181` SÍ traduce SIGTERM vía `signal.signal`; `paper_hydra.py` NO maneja señales. Ninguno de los dos llama `bootstrap_logging()` explícito (config de logging vía Hydra/AppConfig). |
| Nota 3.2: `MetricsRuntime.shutdown()` fuera de alcance | ✅ Correcto — sigue sin invocarse en `cli/main.py` |

---

## Hallazgos adicionales (fuera de los ADRs)

1. ~~AGENTS.md desactualizado sobre Dagster~~ — ✅ **Resuelto** (verificado 2026-08-02): sin menciones a Dagster en el archivo actual.
2. ~~pyproject.toml:24 con "orquestación Dagster"~~ — ✅ **Resuelto** (verificado 2026-08-02): la descripción actual no menciona Dagster.
3. ~~Archivos `.bak` sin trackear~~ — ✅ **Resuelto** (verificado 2026-08-02): `find . -name "*.bak"` no encuentra ninguno.
4. **Docstring desactualizado en `live_hydra.py`** — dice "uv run live-hydra", pero el script es `live` (`uv run live`).
5. ~~Docstring de clase en `portfolio_service.py:58`~~ — ✅ **Resuelto** (verificado 2026-08-02): dice "obligatorio, sin default — ver BC-43".

---

## Plan de reconciliación propuesto

1. ~~**ADR-0002**: enmendar Decisión 4 + Pendiente #1~~ — ✅ **Implementado**: nota de enmienda presente al inicio del documento, referenciando este ADR-0006.
2. ~~**ADR-0001**: marcar `control_plane`, `build_market_data()`, `build_trading()` y el CR General como *pendientes*~~ — ✅ **Implementado**: nota de enmienda presente al inicio del documento.
3. ~~**AGENTS.md + pyproject.toml**: eliminar referencias a Dagster~~ — ✅ **Resuelto** (verificado 2026-08-02).
4. **Deuda menor** — ✅ `.bak` eliminados; ✅ docstrings "Coexiste..." corregidos (commit `fef31a0`); ✅ "default: InMemoryPositionStore" corregido; ⏳ pendiente confirmar wording "uv run live-hydra" en `live_hydra.py`.

---

*Documento de verificación generado el 2026-08-02. Cada afirmación fue comprobada contra el árbol de código y el historial de git.*

---

## Adenda — re-verificación 2026-08-02 (sesión posterior)

4 de los 5 "Hallazgos adicionales" y 3 de los 4 ítems del "Plan de
reconciliación" ya estaban resueltos al momento de esta re-verificación,
la mayoría antes de esta sesión (drift positivo: el código avanzó más
rápido que este documento). Único ítem verificado y corregido en esta
sesión: docstrings de `live_hydra.py`/`paper_hydra.py` (commit `fef31a0`).
Pendiente sin verificar: wording "uv run live-hydra" (ítem 4 original).

Lección operativa: este documento, por su propia naturaleza de
verificación puntual, tiende al mismo drift que audita. Re-verificar
periódicamente, no asumir vigencia indefinida.
