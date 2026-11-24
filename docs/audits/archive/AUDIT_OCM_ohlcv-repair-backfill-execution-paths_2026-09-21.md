# AUDITORÍA — OHLCV Repair / Backfill / Incremental — Execution Path Validation

- **ID**: MD-RECOVERY-001
- **Fecha**: 2026-09-21
- **Modo**: READ-ONLY. Cero modificaciones a código, tests, config, systemd, base de datos o tracking.
- **Scope**: `packages/market_data` — estrategias de recuperación, `main.py`, `entrypoint.py`, `pipeline_factory.py`, `HistoricalFetcherAsync`, `PipelineContext`, config.
- **Complementa**: `docs/audits/AUDIT_OCM_market-data-deep-audit_2026-08-14.md` (F-MD-001..008), `docs/audits/OCM_AUDIT_FINDINGS_2026-08-20_market-data-runtime.md` (F-RT-01..04), `docs/audits/AUDIT_OCM_market-data-audit_2026-05-27.md`.

---

## 0. GIT STATE

```
BRANCH: feat/m22b-governance
HEAD:   857cfe60 docs(architecture): document SafeOps LOCAL BYPASS ≠ REPOSITORY BYPASS (M22-B)
STATUS: Sin cambios no rastreados relevantes para esta auditoría
```

Archivos sin modificar: cero. Documento de auditoría es la única escritura.

---

## 1. RESUMEN EJECUTIVO

**Veredicto: CONFIRMED**

Las tres estrategias de recuperación (`RepairStrategy`, `BackfillStrategy`, `IncrementalStrategy`) **existen y están implementadas** como código funcional. Sin embargo, **RepairStrategy no está conectada al runtime de producción** a través de `OHLCVPipeline`. El `PipelineContext.storage` nunca se inyecta en el constructor de `OHLCVPipeline`, y `RepairStrategy._read_silver()` contiene una aserción explícita que falla si `ctx.storage is None`.

**Resumen de estado**:

| Componente | Existe | Conectado | Ejecutado en Runtime | Operacional |
|---|---|---|---|---|
| `IncrementalStrategy` | ✅ | ✅ | ✅ (main.py + entrypoint.py) | ✅ |
| `BackfillStrategy` | ✅ | ✅ | ✅ (entrypoint.py solo, via `backfill_mode=true`) | ⚠️ Requiere CLI |
| `RepairStrategy` | ✅ | ❌ | ❌ | ❌ |
| `HistoricalFetcherAsync` | ✅ | ✅ | ✅ | ✅ |
| `QualityPipeline` | ✅ | ✅ | ✅ | ✅ |
| `FeedOrchestrator` | ✅ | ✅ | ⚠️ Solo si `ingestion_mode != "rest"` | ⚠️ |

---

## 2. CALL CHAIN COMPLETO

### 2.1 main.py — Microservicio (runtime principal)

**Archivo**: `packages/market_data/main.py`

#### `_lifespan()` (líneas 314-445)
1. Construye `RuntimeContext` + `ExecutionGuard`
2. Crea `CompositionRoot.assemble(ctx.app_config)` → `ConcretePipelineFactory`
3. Crea `PipelineOrchestrator(factory=...)`
4. Lanza `_ingestion_loop(ctx, guard)` como task asyncio
5. Lanza `_bronze_writer_loop()` como task asyncio
6. Opcionalmente lanza `FeedOrchestrator` si `ingestion_mode != "rest"`

#### `_ingestion_loop()` (líneas 121-219)
```
while not guard.should_stop():
    for exc_name in app_cfg.exchange_names:
        for market_type, symbols in [("spot", ...), ("futures", ...)]:
            request = PipelineRequest(
                exchange=exc_name,
                market_type=market_type,
                pipeline="ohlcv",
                mode="incremental",    # ← SOLO incremental
                ...
            )
            await orchestrator.run(request)
```
**Líneas 172-185**: `mode` está **hardcodeado como `"incremental"`**. El docstring (línea 127) dice "Cada iteración lanza el flow completo (backfill + incremental + repair)" — **esto es falso**.

#### `PipelineOrchestrator.run()` → `_build_pipeline()` → `factory.build(request)`
- `factory.build(request)` dispara a `ConcretePipelineFactory._build_ohlcv(request)`
- `_build_ohlcv` construye `OHLCVPipeline` con `backfill_mode=(request.mode == "backfill")`
- `OHLCVPipeline.__init__` crea `PipelineContext` **SIN `storage`**

### 2.2 entrypoint.py — CLI batch

**Archivo**: `apps/app/cli/entrypoint.py`

```python
mode: Literal["incremental", "backfill", "repair"] = (
    "backfill" if app_cfg.pipeline.historical.backfill_mode else "incremental"
)
```
- Solo alterna entre `"backfill"` y `"incremental"`
- **`"repair"` nunca se selecciona**
- Requiere `OCM_PIPELINE__HISTORICAL__BACKFILL_MODE=true` o `pipeline.historical.backfill_mode=true` en Hydra

### 2.3 ConcretePipelineFactory._build_ohlcv()

**Archivo**: `packages/market_data/infrastructure/bootstrap/pipeline_factory.py`

Líneas 334-351 — construye `OHLCVPipeline`:
```python
return OHLCVPipeline(
    symbols=request.symbols,
    timeframes=request.timeframes,
    start_date=request.start_date,
    exchange_client=...,
    fetcher=fetcher,
    metrics=...,
    repair_metrics=PrometheusRepairMetrics(),  # ← inyectado
    quality=...,
    cursor_store=cursor,
    market_type=request.market_type,
    dry_run=request.dry_run,
    auto_lookback_days=...,
    event_bus=...,
    publisher=publisher,
    chunk_converter=chunk_converter,
    environment=environment_name,
    # ← NO pasa storage
)
```

### 2.4 OHLCVPipeline.__init__ → PipelineContext

**Archivo**: `packages/market_data/application/pipelines/ohlcv_pipeline.py`, líneas 281-293

```python
self._ctx = PipelineContext(
    fetcher=fetcher,
    cursor=cursor,
    quality=quality,
    exchange_id=self._exchange_id,
    market_type=self.market_type,
    start_date=start_date,
    publisher=publisher,
    metrics=metrics,
    gap_registry=gap_registry,  # None por defecto
    event_bus=event_bus,
    _chunk_converter=chunk_converter,
    # ← NO pasa storage
)
```

### 2.5 PipelineContext

**Archivo**: `packages/market_data/application/pipeline/runtime.py`

Línea 273: `storage: OHLCVStorage | None = field(default=None)` — **default=None**

Comentario en línea 269-272:
```python
# El caller que ejecute RepairStrategy debe inyectar storage aquí;
# hoy OHLCVPipeline construye PipelineContext sin storage (composition
# root: ConcretePipelineFactory), por lo que Repair lo requiere explícito.
```

### 2.6 RepairStrategy._read_silver() — ASSERTION FAILURE

**Archivo**: `packages/market_data/application/strategies/repair.py`, líneas 326-329

```python
assert ctx.storage is not None, (
    "RepairStrategy._read_silver: ctx.storage es None. El caller que ejecute "
    "RepairStrategy debe inyectar ctx.storage en el PipelineContext antes de "
    "llamar a run() — OHLCVPipeline no lo provee hoy (ver pipeline/runtime.py)."
)
```

**Esta aserción fallará SIEMPRE en producción.**

---

## 3. AUDITAR LAS ESTRATEGIAS

### 3.1 IncrementalStrategy

| Aspecto | Detalle |
|---|---|
| **Propósito** | Ingestión incremental hacia adelante (desde cursor Redis o start_date) |
| **Archivo** | `application/strategies/incremental.py` |
| **Método** | `_run()` → `ctx.fetcher.download_data()` → quality → publisher → cursor.update() |
| **Persistencia** | Publica a Kafka via `ctx.publisher.publish_chunk()` → consumer (BronzeWriter) → Iceberg |
| **Caller** | `OHLCVPipeline._run_worker_pool()` via worker pool |
| **Runtime** | ✅ `main.py` (mode="incremental") + `entrypoint.py` (backfill_mode=false) |
| **Status** | **OPERATIONAL** |

### 3.2 BackfillStrategy

| Aspecto | Detalle |
|---|---|
| **Propósito** | Descarga histórica completa hacia atrás (now → origin) |
| **Archivo** | `application/strategies/backfill.py` |
| **Método** | `_run()` → `_discover_origin()` → `_paginate_backward()` → quality → publisher |
| **Persistencia** | Publica a Kafka via `ctx.publisher.publish_chunk()` |
| **Caller** | `OHLCVPipeline._run_worker_pool()` via worker pool |
| **Runtime** | ⚠️ Solo `entrypoint.py` con `backfill_mode=true` |
| **Status** | **OPERACIONAL pero requiere CLI explícito** |

### 3.3 RepairStrategy

| Aspecto | Detalle |
|---|---|
| **Propósito** | Detectar gaps en Silver y sanarlos haciendo fetch REST del rango faltante |
| **Archivo** | `application/strategies/repair.py` |
| **Método** | `_run()` → `_read_silver()` → `scan_gaps()` → `_heal_gap()` → fetch REST → quality → save_ohlcv |
| **Persistencia** | Escribe directamente a Silver via `ctx.storage.save_ohlcv()` |
| **Caller** | `OHLCVPipeline._run_worker_pool()` via worker pool |
| **Runtime** | ❌ **NUNCA** — `ctx.storage` siempre es `None` en PipelineContext |
| **Status** | **IMPLEMENTADA pero NO OPERACIONAL** |

---

## 4. AUDITAR HistoricalFetcherAsync

**Archivo**: `packages/market_data/adapters/inbound/rest/ohlcv_fetcher.py`

### 4.1 Métodos públicos

| Método | Caller | Descripción |
|---|---|---|
| `ensure_exchange()` | `OHLCVPipeline.run()` | Verifica sesión HTTP activa |
| `download_data()` | `IncrementalStrategy._run()` | Descarga incremental (cursor → now) |
| `fetch_chunk()` | `BackfillStrategy._run()`, `RepairStrategy._heal_gap()` | Fetch crudo de un rango |

### 4.2 backfill_mode behavior

**`_resolve_start_timestamp()` (líneas 515-574)**:
- `backfill_mode=True`: usa `config_start_date` como floor, sin discovery dinámico
- `backfill_mode=False` (incremental): cursor Redis → start_date → auto lookback 5 años

**`_download_chunked()` (líneas 369-509)**:
- `_mode = "backfill" if self._backfill_mode else "incremental"` — afecta métricas y logging
- Comportamiento de paginación es el mismo para ambos modos

### 4.3 flujo de datos

```
CCXTAdapter (exchange REST)
  ↓ _fetch_chunk_with_retry()
HistoricalFetcherAsync
  ↓ _download_chunked() o fetch_chunk()
Raw data (List[list])
  ↓ _raw_to_dataframe()
pl.DataFrame
  ↓ transformer.transform()
Normalized pl.DataFrame
  ↓ (IncrementalStrategy → ctx.publisher.publish_chunk())
Kafka → ohlcv.raw → BronzeWriter → Iceberg Bronze
  ↓ (BackfillStrategy → ctx.publisher.publish_chunk())
Mismo flujo
  ↓ (RepairStrategy → ctx.storage.save_ohlcv())
Iceberg Silver directo (sin Kafka)
```

---

## 5. INVESTIGAR backfill_mode

### 5.1 Definición

**`ocm/config/schema.py` línea 323**: `backfill_mode: bool = False` en `HistoricalConfig`
**`ocm/config/structured/pipeline.py` línea 47**: `backfill_mode: bool = False` en `HistoricalConfig`
**Default**: `False`

### 5.2 Lectores

| Archivo | Cómo lo lee |
|---|---|
| `apps/app/cli/entrypoint.py:70` | `mode = "backfill" if app_cfg.pipeline.historical.backfill_mode else "incremental"` |
| `packages/market_data/adapters/inbound/rest/ohlcv_fetcher.py:286` | `backfill_mode` constructor param |
| `packages/market_data/infrastructure/bootstrap/pipeline_factory.py:287` | `backfill_mode=(request.mode == "backfill")` |

### 5.3 Configuración

- **Hydra**: `pipeline.historical.backfill_mode=true`
- **Env var**: `OCM_PIPELINE__HISTORICAL__BACKFILL_MODE=true` (ver `ocm/config/layers/env_override.py:20`)
- **CLI**: `uv run python -m app.cli.main pipeline.historical.backfill_mode=true`
- **systemd**: No hay servicio que active backfill_mode — `ocm-market-data.service` ejecuta `python -m market_data.main` que siempre usa `mode="incremental"`

### 5.4 Clasificación

**`backfill_mode`**: `CONFIG_ONLY` — existe como configuración pero solo se usa en el CLI batch (`entrypoint.py`). El microservicio (`main.py`) lo ignora completamente.

---

## 6. QUALITY → REPAIR

### 6.1 Flujo existente

```
QualityPipeline.run(df, symbol, timeframe, exchange)
  → check_dataset_invariants()
  → DataQualityChecker.check() (7 checks)
  → QualityReport(accepted, score, df)
```

Ambas estrategias (incremental, backfill, repair) llaman `ctx.quality.run()` antes de persistir. **Quality no dispara repair**.

### 6.2 ¿Existe flujo Quality → gap detection → RepairStrategy?

**NO**. El flujo sería:

```
Quality Gate → gap detected → RepairStrategy → HistoricalFetcherAsync → REST → recovered candles → dedup/reconcile → storage
```

**Dónde se rompe**: No existe ningún mecanismo que, tras un `QualityPipeline` rechazo o un gap detection, active `RepairStrategy`. `RepairStrategy` tiene su propio `scan_gaps()` interno que lee Silver y detecta gaps, pero nadie la invoca automáticamente.

`gap_scanner` (`application/processing/gap_scanner.py`) existe como utilidad, pero no hay un loop de retroalimentación que conecte la detección de gaps con la ejecución de repair.

---

## 7. REST VS WEBSOCKET

### 7.1 REST

**Responsable de**: historical bootstrap, backfill, repair (si se activara), incremental (polling periódico)

**Entrypoints**:
- `main.py::_ingestion_loop` — cada 300s
- `entrypoint.py` — una pasada batch
- `HistoricalFetcherAsync` — toda la comunicación con exchanges vía CCXT REST

### 7.2 WebSocket

**Responsable de**: feeds en tiempo real (orderbook, trades, funding, OI, liquidations)

**Entrypoints**:
- `FeedOrchestrator` — iniciado en `main.py::_lifespan` solo si `ingestion_mode != "rest"`
- `ocm-streaming.service` — servicio systemd separado (`streaming_hydra.py`)
- `KafkaBronzeWriter` — consume `ohlcv.raw` (Kappa, no WS)

### 7.3 ingestion_mode

| Modo | FeedOrchestrator | Main.py ingestion | Efecto |
|---|---|---|---|
| `rest` | ❌ No se inicia | ✅ incremental loop | Solo REST polling |
| `websocket` | ✅ WS feeds | ✅ incremental loop | REST + WS |
| `dual` | ✅ WS feeds | ✅ incremental loop | REST + WS (comparación) |

**Ningún modo activa repair ni backfill automáticamente.**

---

## 8. FeedOrchestrator

### 8.1 Qué hace

`FeedOrchestrator` (`application/feed_orchestrator.py`) gestiona adaptadores WS (BybitFeedAdapter, KucoinFeedAdapter, etc.) como `MarketDataSource`. Publica trades en `trades.raw` vía `KafkaTradePublisher`.

### 8.2 Qué produce

- `trades.raw` (Kafka topic) — trades en tiempo real
- NO produce OHLCV
- NO participa en repair/backfill/incremental

### 8.3 Relación con estrategias

**Ninguna**. `FeedOrchestrator` es un sistema independiente que produce `trades.raw`. Las estrategias de OHLCV (incremental/backfill/repair) consumen de exchanges vía REST, no de WS feeds.

### 8.4 Condición de inicio

Solo se inicia si:
1. `config.feeds.ingestion_mode != "rest"`
2. Hay feeds habilitados en `config.feeds`

**Referencia**: `composition_root.py:187-200` y `main.py:384-407`

---

## 9. TESTS

### 9.1 Existen tests para repair

`tests/market_data/strategies/test_repair_strategy.py` — 922 líneas, suite completa.

Los tests **funcionan** porque `_make_ctx()` pasa explícitamente `storage=_StorageStub(...)` (línea 233). Esto confirma que:
1. La lógica de `RepairStrategy` es correcta
2. Los tests aíslan el problema inyectando `storage` manualmente
3. **En producción**, `OHLCVPipeline` no pasa `storage`, por lo que la aserción fallaría

### 9.2 Tests que demuestran gap → repair → persistence

Existen tests unitarios de `RepairStrategy` que cubren:
- skip si Silver vacío
- healed_count con gaps sanables
- partial_count con fill_ratio < 0.95
- gap demasiado grande → skip
- gap irrecuperable → skip
- error en gather → contado pero no propaga

**NINGÚN test demuestra que `main.py` o `OHLCVPipeline` ejecute `RepairStrategy` automáticamente.**

### 9.3 Tests de integración

`tests/architecture/test_kappa_publisher_wiring.py` — verifica que `OHLCVPipeline` reciba publisher real en producción. No cubre `storage`.

---

## 10. DOCUMENTACIÓN VS CÓDIGO

### 10.1 Discrepancias encontradas

| Fuente | Afirmación | Realidad |
|---|---|---|
| `main.py:127` (docstring) | "Cada iteración lanza el flow completo (backfill + incremental + repair)" | Solo `mode="incremental"` |
| `pipeline_orchestrator.py:20` (docstring) | "Qué modo pasarle (incremental/backfill/repair)" | repair existe pero no se dispara |
| `pipeline_runtime.py:269-272` (comentario) | "hoy OHLCVPipeline construye PipelineContext sin storage" | Confirmado — `storage` no se pasa |
| `repair.py:326-329` (assert) | "OHLCVPipeline no lo provee hoy" | Confirmado — falla siempre |
| `entrypoint.py:69-71` | Solo backfill/incremental | repair no está en el CLI |
| `config/schema.py:323` | `backfill_mode: bool = False` | Solo controla incremental vs backfill |

### 10.2 Documentación correcta

- `ohlcv_pipeline.py:170-173` (docstring): "summary = await pipeline.run(mode='repair')" — la API existe, pero el caller nunca la usa
- `pipeline_orchestrator.py:114` (docstring PipelineRequest): "mode: 'incremental' | 'backfill' | 'repair'" — el DTO acepta repair

---

## 11. ESCENARIO CONCRETO DE GAP

Simulando gap: 10:00-10:05 missing (5 velas 1m):

### Flujo ACTUAL (sin repair):

```
10:00 → incremental run (cursor en 10:00)
10:05 → próximo incremental run (cursor en 10:00, busca desde 09:59 con overlap)
       → descubre que 10:01-10:05 faltan en Iceberg (no hay mecanismo de gap detection en tiempo real)
       → descarga desde cursor (10:00) hacia adelante
       → 10:01-10:05 NO se recuperan (el incremental solo avanza hacia adelante desde el cursor)
```

**Dónde se rompe**: No hay detección automática de gaps ni llamada a `RepairStrategy`. Los datos faltantes persisten.

### Flujo SI Repair estuviera conectado:

```
10:05 → RepairStrategy._read_silver() → lee Silver → scan_gaps() → detecta gap 10:01-10:05
       → _heal_gap() → fetcher.fetch_chunk(since=10:00, end_ms=10:05)
       → quality.run() → ctx.storage.save_ohlcv() → Silver actualizado
```

**Este flujo no ocurre porque `RepairStrategy` nunca se invoca.**

---

## 12. DATASET COMPLETENESS RELATIONSHIP

### 12.1 Evidencia

`RepairStrategy` es el único componente diseñado para detectar y sanar gaps en Silver. Si `RepairStrategy` nunca se ejecuta, los gaps detectados por `QualityPipeline` (cuyos resultados se publican como `GapDetectedEvent` si el gap publisher está configurado) nunca se reparan.

Sin embargo, **NO se puede afirmar que la ausencia de repair sea la causa definitiva de dataset incompleto** porque:

1. `IncrementalStrategy` puede crear gaps si el cursor se desincroniza
2. `BackfillStrategy` puede dejar gaps si se interrumpe
3. `QualityPipeline` ya detecta gaps (evidencia: `GapDetectedEvent`, `gap_scanner`)
4. La deduplicación y el idempotencia de Iceberg pueden enmascarar o agravar gaps

### 12.2 Clasificación

**Dataset completeness relationship: PARTIAL**

- `RepairStrategy` no ejecutándose es un **contribuyente probable** de gaps no sanados
- No se ha demostrado causalidad directa (falta análisis de logs de producción, gap counts, y comparación de dataset completeness con/without repair)
- Otros factores (cursor drift, exchange errors, interrupciones) también contribuyen

---

## 13. CONCLUSIÓN

### 13.1 Finding

**MD-RECOVERY-001**: `RepairStrategy` existe como implementación funcional y tiene tests completos, pero está **desconectada del runtime de producción** porque `OHLCVPipeline` no inyecta `storage` en `PipelineContext`, y `RepairStrategy._read_silver()` contiene una aserción que falla cuando `ctx.storage is None`.

### 13.2 Clasificación

**CONFIRMED** — La desconexión entre `RepairStrategy` y el runtime es un hecho verificado en código.

**PARTIALLY_CONFIRMED** — Para `BackfillStrategy`: existe y está conectado, pero solo se activa vía CLI con `backfill_mode=true`. El microservicio principal nunca lo ejecuta.

### 13.3 Impacto

- **Reparación de gaps**: Sin `RepairStrategy`, los gaps en datos OHLCV no se recuperan automáticamente
- **Control de calidad**: `GapDetectedEvent` se publica (si gap publisher está configurado) pero nadie los escucha para reparar
- **Confianza en datos**: El sistema produce datos incompletos sin mecanismo de auto-reparación

---

## 14. DIAGNÓSTICO TÉCNICO DETALLADO

### 14.1 Ruta completa de falla

```
main.py::_ingestion_loop()
  → PipelineRequest(mode="incremental")  ← hardcodeado
    → PipelineOrchestrator.run(request)
      → ConcretePipelineFactory._build_ohlcv(request)
        → OHLCVPipeline.__init__(..., backfill_mode=False)
          → PipelineContext(fetcher=..., cursor=..., quality=..., ..., storage=None)  ← sin storage
            → strategies = {INCREMENTAL: ..., BACKFILL: ..., REPAIR: RepairStrategy(metrics=...)}
              → Run only INCREMENTAL strategy  ← modo hardcodeado
                → RepairStrategy nunca se invoca
                  → si se invocara: assert ctx.storage is not None → AssertionError
```

### 14.2 Reparación requerida

Para que `RepairStrategy` funcione, se necesitan:

1. **`OHLCVPipeline.__init__`** debe aceptar un parámetro `storage: Optional[OHLCVStorage]`
2. **`ConcretePipelineFactory._build_ohlcv`** debe construir `OHLCVStorage` (ya existe como `storage` en factory, pero se usa para fetcher) y pasarlo a `PipelineContext`
3. **`PipelineContext`** debe recibir `storage`
4. **`main.py::_ingestion_loop`** debe agregar una pasada `mode="repair"` o un mecanismo de detección de gaps → repair
5. **O alternativamente**, `RepairStrategy` puede integrarse como un componente separado (post-loop o periodic task) que no dependa de `OHLCVPipeline`

---

## 15. ALTERNATIVAS DE DISEÑO (SIN IMPLEMENTAR)

### A. Conectar main.py con RepairStrategy

- **Dependencias**: `OHLCVPipeline` debe aceptar `storage`; `main.py` debe construir `OHLCVStorage` y pasarlo
- **Riesgos**: Cambio en el constructor de `OHLCVPipeline` rompe compatibilidad con tests existentes
- **Compatibilidad**: Requiere actualizar todos los callers de `PipelineRequest`
- **Impacto**: Reparación automática post-cada ciclo incremental
- **Observabilidad**: GapDetectedEvent → reparación inmediata

### B. Mantener repair como componente separado (post-loop)

- **Dependencias**: `_ingestion_loop` o un task asyncio separado ejecuta `RepairStrategy` periódicamente
- **Riesgos**: Requiere inyectar `storage` desde composition root
- **Compatibilidad**: No cambia `OHLCVPipeline` ni `PipelineContext`
- **Impacto**: Reparación periódica (ej: cada N iteraciones)
- **Observabilidad**: Reparación como tarea independiente con sus propias métricas

### C. Introducir un Recovery Orchestrator

- **Dependencias**: Nuevo componente que escucha `GapDetectedEvent` y ejecuta `RepairStrategy`
- **Riesgos**: Complejidad adicional; requiere Kafka consumer para gap events
- **Compatibilidad**: Mínimo cambio en components existentes
- **Impacto**: Event-driven repair; más robusto pero más infraestructura
- **Observabilidad**: Flujo completo gap → detección → reparación → validación

---

## 16. PR PROPUESTAS (SIN IMPLEMENTAR)

### PR-1: Fix de plumbing (storage → PipelineContext)
- `OHLCVPipeline.__init__` acepta `storage`
- `ConcretePipelineFactory._build_ohlcv` construye/pasa `OHLCVStorage`
- `PipelineContext` recibe `storage`
- **Dependencias**: Ninguna externa
- **Tests**: Actualizar tests que construyan `OHLCVPipeline` directamente

### PR-2: Integración de RepairStrategy en main.py
- Agregar pasada `mode="repair"` en `_ingestion_loop` o task separado
- **Dependencias**: PR-1
- **Tests**: Test de integración que verifique repair se ejecuta

### PR-3: Gap detection → Repair automation
- Escuchar `GapDetectedEvent` y disparar reparación
- **Dependencias**: PR-1, PR-2
- **Tests**: Event-driven test

---

## 17. VERIFICACIÓN FINAL

- Git status: Sin cambios
- Git diff: Sin cambios
- Archivo de auditoría creado: `docs/audits/AUDIT_OCM_ohlcv-repair-backfill-execution-paths_2026-09-21.md`

---

## STOP CONDITION

```
AUDIT COMPLETE
Finding:
MD-RECOVERY-001
Verdict:
CONFIRMED
RepairStrategy:
IMPLEMENTADA pero NO OPERACIONAL (PipelineContext.storage=None, assertion failure)
BackfillStrategy:
OPERACIONAL pero solo vía CLI (backfill_mode=true)
IncrementalStrategy:
OPERACIONAL (main.py + entrypoint.py)
HistoricalFetcherAsync:
OPERACIONAL
main.py integration:
SOLO mode="incremental" hardcodeado; docstring mentira
backfill_mode:
CONFIG_ONLY — solo controla entrypoint.py, no main.py
Quality → Repair:
NO EXISTE — flujo roto, sin trigger automático
REST recovery:
NO EXISTE — repair nunca se invoca
Dataset completeness relationship:
PARTIAL — repair ausente es contribuyente probable pero no causalidad probada
Runtime:
ocm-market-data.service ejecuta main.py → solo incremental
Audit document:
docs/audits/AUDIT_OCM_ohlcv-repair-backfill-execution-paths_2026-09-21.md
Implementation:
NOT PERFORMED
Commit:
NOT PERFORMED
Push:
NOT PERFORMED
Merge:
NOT PERFORMED
```

**DETENERSE y esperar autorización humana explícita.**
