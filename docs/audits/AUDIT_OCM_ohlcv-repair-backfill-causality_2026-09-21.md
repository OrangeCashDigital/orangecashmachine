# AUDITORÍA — OHLCV Repair / Backfill / Incremental: Causalidad e Integración Arquitectónica

- **ID**: MD-RECOVERY-002
- **Fecha**: 2026-09-21
- **Modo**: READ-ONLY. Cero modificaciones.
- **Complementa**: `docs/audits/AUDIT_OCM_ohlcv-repair-backfill-execution-paths_2026-09-21.md` (MD-RECOVERY-001, fase 1).
- **Reglas**: Sin código, tests, config, systemd, commits, push, merge. Sin eliminar auditorías existentes.

---

## 1. EXECUTIVE SUMMARY

**Veredicto**: `CONFIRMED` — `RepairStrategy` está implementada pero arquitectónicamente desconectada del runtime de producción. El `PipelineContext.storage` nunca se inyecta por diseño en `OHLCVPipeline`, y la detección de gaps en `QualityPipeline` solo emite métricas/logs — no dispara reparación. Backfill y Repair son capacidades completamente distintas que comparten fetcher REST pero tienen caminos de persistencia separados (Kappa vs directo).

**Tabla resumen de modo**:

| Mode | Existe | Seleccionable | Caller producción | Ejecutable | Persistencia | Tests |
|---|---|---|---|---|---|---|
| `incremental` | ✅ | ✅ | `main.py::_ingestion_loop` | ✅ | Kafka → BronzeWriter → Iceberg | ✅ |
| `backfill` | ✅ | ✅ (CLI) | `entrypoint.py` (condicional) | ✅ | Kafka → BronzeWriter → Iceberg | ✅ |
| `repair` | ✅ | ❌ | **Ninguno** | ❌ (assertion failure) | Directo Iceberg (sin Kafka) | ✅ |

---

## 2. ESTADO GIT

```
BRANCH: feat/m22b-governance
HEAD:   857cfe60 docs(architecture): document SafeOps LOCAL BYPASS ≠ REPOSITORY BYPASS (M22-B)
STATUS: Sin cambios — solo archivos untracked existentes
```

---

## 3. CALL CHAIN EXACTO CON VALORES

### 3.1 Ruta completa: `main.py` → `incremental`

```
python -m market_data.main
  ↓ uvicorn (lifespan)
  → _lifespan()
    → CompositionRoot.assemble(config)
      → ConcretePipelineFactory(cfg=config)  ← almacena self._cfg
    → PipelineOrchestrator(factory=root.factory)
    → _ingestion_loop(ctx, guard)
      → while not guard.should_stop():
          → PipelineRequest(
              exchange=exc_name,          ← e.g. "bybit"
              market_type="spot",          ← o "futures"
              pipeline="ohlcv",            ← hardcoded
              mode="incremental",          ← HARDCODED, línea 174
              credentials=exc_cfg.ccxt_credentials(),
              resilience=exc_cfg.resilience,
              symbols=symbols,             ← app_cfg.markets.spot_symbols
              timeframes=app_cfg.pipeline.historical.timeframes,  ← ["1m"]
              start_date=app_cfg.pipeline.historical.start_date,  ← "auto"
              auto_lookback_days=app_cfg.pipeline.historical.auto_lookback_days,  ← 3650
              run_id=ctx.run_id,           ← Hydra run_id
              dry_run=app_cfg.safety.dry_run,
            )
          → orchestrator.run(request)
            → _build_pipeline(request)
              → factory.build(request)
                → ConcretePipelineFactory._build_ohlcv(request)
                  → CCXTAdapter(**kwargs)
                  → HistoricalFetcherAsync(
                      storage=None,           ← EXPLICIT NULL
                      transformer=OHLCVTransformer(),
                      exchange_client=raw_adapter,
                      cursor_store=cursor,    ← RedisCursorStore or InMemory
                      backfill_mode=False,    ← (request.mode == "backfill") = False
                      market_type="spot",
                      config_start_date="auto",
                      auto_lookback_days=3650,
                    )
                  → QualityPipeline(...)
                  → cursor = build_cursor_store()  ← Redis o InMemory
                  → publisher = _build_kafka_publisher()  ← KafkaOHLCVPublisher o NullOHLCVPublisher
                  → OHLCVPipeline(
                      symbols=symbols,
                      timeframes=timeframes,
                      start_date=start_date,
                      exchange_client=...,
                      fetcher=fetcher,        ← HistoricalFetcherAsync
                      metrics=PrometheusPipelineMetrics(),
                      repair_metrics=PrometheusRepairMetrics(),
                      quality=QualityPipeline(),
                      cursor_store=cursor,
                      market_type="spot",
                      dry_run=...,
                      auto_lookback_days=3650,
                      event_bus=...,
                      publisher=publisher,
                      chunk_converter=PassthroughChunkConverter(),
                      environment="development",
                      ← storage PARAMETER ABSENT
                    )
                  → OHLCVPipeline.__init__ → PipelineContext(
                      fetcher=fetcher,
                      cursor=cursor,
                      quality=quality,
                      exchange_id=exchange_id,
                      market_type=market_type,
                      start_date=start_date,
                      publisher=publisher,
                      metrics=metrics,
                      gap_registry=None,      ← DEFAULT
                      event_bus=event_bus,
                      _chunk_converter=chunk_converter,
                      ← storage NOT PASSED → storage=None
                    )
                  → self._strategies = {
                      INCREMENTAL: IncrementalStrategy(),
                      BACKFILL: BackfillStrategy(),
                      REPAIR: RepairStrategy(metrics=repair_metrics),
                    }
            → pipeline.run(mode="incremental")
              → strategy = self._strategies[PipelineMode.INCREMENTAL]
              → IncrementalStrategy._run()
                → ctx.fetcher.download_data(symbol, timeframe, start_date="auto")
                  → HistoricalFetcherAsync._download_chunked()
                    → _resolve_start_timestamp() → cursor Redis → since_ts
                    → _fetch_chunk_with_retry() → CCXTAdapter.fetch_ohlcv()
                    → _raw_to_dataframe() → pl.DataFrame
                    → transformer.transform() → normalized pl.DataFrame
                    → return df
                → ctx.quality.run(df, ...) → QualityPipelineResult(accepted=True/False)
                → ctx.publisher.publish_chunk(chunk) → Kafka ohlcv.raw
                → ctx.cursor.update(...) → Redis
                → ctx.metrics.rows_ingested_inc(...)
                → ctx.publish_domain_event(OHLCVBatchReceived)
```

### 3.2 Ruta completa: `entrypoint.py` → `backfill`

```
uv run python -m app.cli.main pipeline.historical.backfill_mode=true
  → entrypoint.run(ctx, pusher)
    → mode = "backfill" if app_cfg.pipeline.historical.backfill_mode else "incremental"
      → mode = "backfill"  ← because backfill_mode=true
    → PipelineRequest(mode="backfill")
      → factory._build_ohlcv(request)
        → HistoricalFetcherAsync(backfill_mode=True)  ← (request.mode == "backfill") = True
        → OHLCVPipeline.__init__ → PipelineContext(storage=None)  ← SAME GAP
      → pipeline.run(mode="backfill")
        → BackfillStrategy._run()
          → _discover_origin() → ctx.cursor.get_raw() or fetch_chunk(since=1, limit=1)
          → _paginate_backward() → ctx.fetcher.fetch_chunk() loop
          → ctx.quality.run(df, ...) → quality gate
          → ctx.publisher.publish_chunk(chunk) → Kafka ohlcv.raw  ← SAME KAPPA PATH
          → ctx.cursor.update() → Redis
```

### 3.3 Ruta `repair` — FALLA

```
(potentially) pipeline.run(mode="repair")
  → strategy = self._strategies[PipelineMode.REPAIR]
  → RepairStrategy._run() (via StrategyMixin.execute_pair)
    → RepairStrategy._read_silver(ctx, symbol, timeframe, columns_only=["timestamp"])
      → assert ctx.storage is not None
        → FAILS: ctx.storage is None
        AssertionError: "RepairStrategy._read_silver: ctx.storage es None. 
        El caller que ejecute RepairStrategy debe inyectar ctx.storage...
        OHLCVPipeline no lo provee hoy."
```

---

## 4. SEMÁNTICA REAL DE LOS MODOS

### 4.1 IncrementalStrategy

| Aspecto | Detalle |
|---|---|
| **Propósito** | Descarga datos desde cursor Redis (o start_date/lookback) hacia adelante |
| **Fuente** | CCXT REST (`fetch_ohlcv` paginado) |
| **Entrada** | `ctx.start_date` = `"auto"` (o config), `ctx.cursor` = último timestamp |
| **Estrategia** | Descarga chunks hasta encontrar datos nuevos (hasta `limit` o `max_chunks`); overlap con velas previas |
| **Salida** | `pl.DataFrame` → quality → `ctx.publisher.publish_chunk()` → Kafka `ohlcv.raw` |
| **Persistencia** | **Indirecta**: Kafka → `KafkaBronzeWriter` → `IcebergStorage.save_ohlcv()` → Silver Iceberg |
| **Dedup** | `_normalize_df`: `unique(subset=["timestamp", "exchange", "symbol", "timeframe"], keep="last")` |
| **Caller producción** | `main.py::_ingestion_loop` (cada 300s) |
| **Tests** | `tests/market_data/strategies/test_incremental_strategy.py` |
| **Usa `ctx.storage`** | ❌ NO |

### 4.2 BackfillStrategy

| Aspecto | Detalle |
|---|---|
| **Propósito** | Descarga histórico completo desde `now` hacia atrás hasta `origin` o `start_date` |
| **Fuente** | CCXT REST (`fetch_chunk` con paginación backward) |
| **Entrada** | `ctx.start_date`, `ctx.cursor` (backfill cursor), `ctx.fetcher` |
| **Estrategia** | Pagina backward en chunks de `chunk_limit * tf_ms` bytes |
| **Salida** | `pl.DataFrame` → quality → `ctx.publisher.publish_chunk()` → Kafka `ohlcv.raw` |
| **Persistencia** | **Idéntica a Incremental**: Kafka → BronzeWriter → Iceberg |
| **Dedup** | Mismo `_normalize_df` |
| **Caller producción** | `entrypoint.py` con `backfill_mode=true` |
| **Tests** | `tests/market_data/strategies/test_backfill_strategy.py` |
| **Usa `ctx.storage`** | ❌ NO |
| **`backfill_mode`** | Solo controla `_resolve_start_timestamp`: usa `config_start_date` como floor, sin cursor Redis |

### 4.3 RepairStrategy

| Aspecto | Detalle |
|---|---|
| **Propósito** | Detectar gaps en Silver existente y sanarlos haciendo fetch REST del rango faltante |
| **Fuente** | CCXT REST (`fetch_chunk` bidireccional) |
| **Entrada** | `ctx.storage` (Silver existente), `ctx.fetcher`, `ctx.quality` |
| **Estrategia** | 1. Lee Silver → `scan_gaps()` → detecta huecos → 2. Para cada gap: fetch REST → quality → `ctx.storage.save_ohlcv()` |
| **Salida** | `pl.DataFrame` recuperado → `ctx.storage.save_ohlcv()` → Iceberg Silver directo |
| **Persistencia** | **DIRECTA**: `IcebergStorage.save_ohlcv()` sin pasar por Kafka |
| **Dedup** | Mismo `_normalize_df`: `unique(keep="last")` |
| **Caller producción** | **NINGUNO** — no hay runtime que lo ejecute |
| **Tests** | `tests/market_data/strategies/test_repair_strategy.py` (922 líneas, suite completa) |
| **Usa `ctx.storage`** | ✅ SÍ — pero `ctx.storage=None` en runtime |
| **`ctx.storage`** | `Optional[OHLCVStorage]` — protocolo |

---

## 5. INVESTIGACIÓN `backfill_mode`

### 5.1 Definición

| Archivo | Definición | Valor default |
|---|---|---|
| `ocm/config/schema.py:323` | `backfill_mode: bool = False` en `HistoricalConfig` | `False` |
| `ocm/config/structured/pipeline.py:47` | `backfill_mode: bool = False` | `False` |
| `ocm/config/layers/env_override.py:20` | `OCM_PIPELINE__HISTORICAL__BACKFILL_MODE=true` | — |

### 5.2 Lectores

| Archivo | Uso | Valor resultante |
|---|---|---|
| `apps/app/cli/entrypoint.py:70` | `mode = "backfill" if app_cfg.pipeline.historical.backfill_mode else "incremental"` | `"backfill"` o `"incremental"` |
| `packages/market_data/adapters/inbound/rest/ohlcv_fetcher.py:286` | Constructor param → `self._backfill_mode` | `True`/`False` |
| `packages/market_data/infrastructure/bootstrap/pipeline_factory.py:287` | `backfill_mode=(request.mode == "backfill")` | `True`/`False` |

### 5.3 Qué controla `backfill_mode=True`

1. **`_resolve_start_timestamp()`**: En modo backfill, usa `config_start_date` como floor. Sin discovery dinámico.
2. **`_download_chunked()`**: `_mode = "backfill"` para métricas/logs.
3. **No cambia el comportamiento de paginación**: `_download_chunked` itiga igual en ambos modos.
4. **No afecta a `main.py`**: `main.py` hardcodea `mode="incremental"` → `backfill_mode=False`.
5. **No afecta a `PipelineRequest.mode`**: `main.py` pasa `mode="incremental"` siempre → `backfill_mode=False` siempre.

### 5.4 Clasificación

**`backfill_mode`**: `CONFIG_ONLY` — controla la lógica interna del fetcher cuando se invoca backfill, pero el runtime principal (`main.py`) nunca lo activa. Solo `entrypoint.py` puede seleccionar backfill.

**Contradicción de nombre**: El nombre `backfill_mode` sugiere que activa un modo de backfill global, pero en realidad solo es un flag que cambia la resolución de timestamp en `HistoricalFetcherAsync`. El modo de ejecución real se decide por `PipelineRequest.mode`.

---

## 6. ANÁLISIS `RepairStrategy` EN PROFUNDIDAD

### 6.1 Constructor

```python
def __init__(
    self,
    gap_tolerance: int = 0,
    metrics: RepairMetricsPort,  ← OBLIGATORIO
    gap_publisher: GapEventPublisherPort | None = None,  ← OPCIONAL
) -> None:
```
- `metrics`: Inyectado desde `pipeline_factory._build_ohlcv` → `PrometheusRepairMetrics()`
- `gap_publisher`: Opcional. Si se inyecta, emite `GapDetectedEvent`, `GapHealedEvent`, `GapFailedEvent`

### 6.2 `execute_pair()` (vía `StrategyMixin.execute_pair`)

1. `_read_silver(ctx, symbol, timeframe, columns_only=["timestamp"])` → `Optional[pl.DataFrame]`
2. `scan_gaps(df_existing, timeframe, tolerance=self._tolerance)` → `List[GapRange]`
3. Si gaps: para cada gap → `_heal_gap()` (con semáforo de concurrencia 4)
4. `_heal_gap()`:
   a. `ctx.fetcher.fetch_chunk(symbol, timeframe, since, limit, end_ms)` → `List[list]`
   b. Construye `pl.DataFrame` desde raw
   c. `ctx.quality.run(df, ...)` → validación
   d. `ctx.storage.save_ohlcv(df, symbol, timeframe)` → persistencia directa
   e. `ctx.gap_registry.mark_healed(...)` (si disponible)
   f. Emite `GapHealedEvent`/`GapFailedEvent` (si gap_publisher)

### 6.3 `_read_silver()` — ASSERTION FAILURE

```python
def _read_silver(self, ctx, symbol, timeframe, columns_only=None) -> Optional[pl.DataFrame]:
    assert ctx.storage is not None, (
        "RepairStrategy._read_silver: ctx.storage es None. El caller que ejecute "
        "RepairStrategy debe inyectar ctx.storage en el PipelineContext antes de "
        "llamar a run() — OHLCVPipeline no lo provee hoy (ver pipeline/runtime.py)."
    )
    df = ctx.storage.load_ohlcv(symbol=symbol, timeframe=timeframe)
    ...
```

**`ctx.storage` es `None`** porque `OHLCVPipeline.__init__` nunca pasa `storage` a `PipelineContext`.

### 6.4 ¿Qué storage necesita exactamente?

**`OHLCVStorage`** (Protocol en `ports/outbound/storage.py`):

| Método | Uso en RepairStrategy |
|---|---|
| `load_ohlcv(symbol, timeframe)` | `_read_silver()` — leer datos Silver existentes para detectar gaps |
| `save_ohlcv(df, symbol, timeframe)` | `_heal_gap()` — persistir datos reparados |
| `get_last_timestamp()` | No usado directamente por RepairStrategy |
| `get_oldest_timestamp()` | No usado directamente |

**Implementación canónica**: `IcebergStorage` (`infrastructure/storage/iceberg/iceberg_storage.py`)
- Constructor: `IcebergStorage(exchange, market_type, dry_run, cursor_store)`
- Crea `IcebergStorageFactory.get_storage(exchange, market_type)` → instancia cacheada
- `save_ohlcv`: append a `silver.ohlcv` con dedup `unique(subset=["timestamp", "exchange", "symbol", "timeframe"], keep="last")`

**Propietario arquitectónico**: `IcebergStorageFactory` vive en `adapters/outbound/storage/iceberg_factory.py`, inyectado en `main.py::_lifespan` como `_state.storage_factory`. **Pero nunca se pasa a `OHLCVPipeline`.**

### 6.5 ¿Por qué `PipelineContext.storage` es `None`?

El `_build_ohlcv` crea:
- `fetcher = HistoricalFetcherAsync(storage=None, ...)` — storage explícitamente `None`
- `cursor_store = build_cursor_store()` — cursor, no storage
- `OHLCVPipeline(...)` — NO recibe `storage` como parámetro

El `IcebergStorage` se crea por `IcebergStorageFactory` pero solo se usa por:
- `_state.storage_factory` en `main.py::_lifespan` → expuesto en `/ohlcv` endpoint
- `_bronze_writer_loop` → usa `BronzeStorage()` directamente (no via factory)
- `QualityPipelineConsumer` → no necesita storage directamente

**Nadie pasa `IcebergStorage` a `PipelineContext`.**

### 6.6 ¿Es esto intencional para incremental?

**Sí, parcialmente.** El diseño Kappa separa al producer (OHLCVPipeline) del storage:
- Producer → Kafka → `KafkaBronzeWriter` (consumer independiente) → Iceberg
- El producer no necesita `storage` porque Kafka es el buffer
- `RepairStrategy` no sigue este patrón porque necesita leer Silver para detectar gaps

**El problema**: `PipelineContext.storage` tiene `default=None` y el docstring dice:
```
storage=None es válido para backfill/incremental — solo Repair lo usa.
El caller que ejecute RepairStrategy debe inyectar storage aquí;
hoy OHLCVPipeline construye PipelineContext sin storage.
```

Esto confirma que es un **disejo deliberadamente incompleto** para el caso de repair, no un bug accidental.

---

## 7. PIPELINECONTEXT.STORAGE — ARQUITECTURA DEL CONTRATO

### 7.1 Dónde se crea

| Ubicación | Código | `storage` value |
|---|---|---|
| `pipeline/runtime.py:273` | `storage: OHLCVStorage \| None = field(default=None)` | `None` (default) |
| `ohlcv_pipeline.py:281-293` | `PipelineContext(fetcher=..., cursor=..., ..., ...)` | **No pasa `storage`** |
| `pipeline_factory.py:334-351` | `return OHLCVPipeline(...)` | **No pasa `storage` a OHLCVPipeline** |
| `tests/strategies/test_repair_strategy.py:233` | `PipelineContext(..., storage=_StorageStub(...), ...)` | `_StorageStub` (test only) |

### 7.2 ¿Quién debería proporcionar `storage`?

**Contractualmente**: `PipelineContext.storage` es un campo opcional. El diseño dice que "el caller que ejecute RepairStrategy debe inyectar storage". En el contexto de `OHLCVPipeline`, el "caller" es `PipelineOrchestrator.run()` que delega a `ConcretePipelineFactory._build_ohlcv`.

**Arquitectónicamente**: `ConcretePipelineFactory._build_ohlcv` tiene acceso a `self._cfg` (AppConfig) y puede construir `IcebergStorage` vía `IcebergStorageFactory`. Sin embargo, no lo hace.

### 7.3 ¿Por qué `PipelineContext.storage` llega `None`?

**Diseño deliberado del patrón Kappa**:
1. El producer (OHLCVPipeline) publica a Kafka
2. El consumer (KafkaBronzeWriter) escribe a Iceberg
3. `PipelineContext.storage` no es necesario porque el producer no escribe directamente a Iceberg
4. `RepairStrategy` rompe este patrón al necesitar lectura/escritura directa de Iceberg
5. La aserción en `_read_silver` documenta que repair no está integrada en el pipeline

### 7.4 ¿Repair requiere composición distinta?

**Sí.** `RepairStrategy` tiene un camino de persistencia fundamentalmente distinto:
- **Incremental/Backfill**: Producer → Kafka → Consumer → Iceberg (Kappa)
- **Repair**: Directamente Iceberg (sin Kafka)

Esto significa que para que `RepairStrategy` funcione, necesita `IcebergStorage` inyectado en `PipelineContext`, lo cual rompe el patrón Kappa actual.

---

## 8. FLUJO DE GAP DETECTION

### 8.1 ¿Qué pasa cuando se detecta un gap?

**En `QualityPipeline._scan_and_emit_gaps()`** (`application/quality/pipeline.py:206-250`):

```python
def _scan_and_emit_gaps(self, df, timeframe, symbol, exchange, rows_removed):
    gaps_raw = scan_gaps(df, timeframe)
    gaps = gaps_raw[rows_removed:] if rows_removed > 0 else gaps_raw
    if not gaps:
        return
    # SOLO LOG + MÉTRICAS:
    log_fn("Gap scan | total={} high={}...")
    self._metrics.quality_gaps_inc(...)
```

**¿Qué NO ocurre**:
- No emite `GapDetectedEvent` (el `GapEventPublisherPort` no se usa aquí)
- No llama a `RepairStrategy`
- No llama a `BackfillStrategy`
- No persiste nada
- No escribe a `GapRegistry`

**¿Qué SÍ ocurre**:
- Log: `logger.warning("Gap scan | total=1 high=1...")`
- Métrica Prometheus: `quality_gaps_inc`

### 8.2 ¿Incremental detecta el gap?

**Sí, en la calidad.** `QualityPipeline._scan_and_emit_gaps` escanea gaps en el DataFrame recién descargado. Si el DataFrame tiene un gap (p. ej., 10:00-10:05 missing), `scan_gaps` lo detecta. Pero solo lo registra como métrica/log.

### 8.3 ¿Incremental lo registra?

Solo como **métrica Prometheus** (`quality_gaps_inc`) y **log** (`logger.warning`). No persiste el gap en ningún estado durable.

### 8.4 ¿Incremental llama Repair?

**NO.** No hay ningún mecanismo de callback o event-driven que conecte `QualityPipeline._scan_and_emit_gaps` con `RepairStrategy`.

### 8.5 ¿Incremental llama Backfill?

**NO.** Son estrategias mutuamente excluyentes seleccionadas por `PipelineRequest.mode`.

### 8.6 ¿El gap llega a Quality?

**Sí, parcialmente.** `QualityPipeline._scan_and_emit_gaps` detecta gaps en el DataFrame de entrada. Pero esto es gap detection en datos *recién descargados*, no en datos *ya persistidos en Silver*. `scan_gaps` opera sobre `df` (el DataFrame de la descarga actual), no sobre el historial de Silver.

### 8.7 ¿Quién decide reparar?

**Nadie.** No existe ningún componente que tome la decisión de ejecutar `RepairStrategy`. `RepairStrategy` es una clase que existe y tiene tests, pero no tiene un trigger.

---

## 9. ANÁLISIS BACKFILL COMPLETO

### 9.1 Flujo completo

```
entrypoint.py (backfill_mode=true)
  → PipelineRequest(mode="backfill")
  → ConcretePipelineFactory._build_ohlcv(request)
    → HistoricalFetcherAsync(backfill_mode=True)
    → OHLCVPipeline → PipelineContext(storage=None)
  → pipeline.run(mode="backfill")
    → BackfillStrategy._run()
      → _discover_origin()
        → ctx.cursor.get_raw(origin_key) → cache hit?
        → fetch_chunk(since=1, limit=1) → origin_ms
        → ctx.cursor.set_raw(origin_key, origin_ms)
      → _resolve_backfill_start(symbol, timeframe, ctx, origin_ms)
        → cursor.get_raw(backfill_key) → resume point
        → or: now_ms (cold start)
      → _paginate_backward(symbol, timeframe, since_ms, origin_ms, ctx)
        → for _ in range(MAX_BACKFILL_CHUNKS):
            → fetch_chunk(..., end_ms=current_end) or since=chunk_start
            → _raw_to_dataframe() → pl.DataFrame
            → ctx.quality.run(df, ...) → accepted?
            → ctx.publisher.publish_chunk(chunk) → Kafka
            → ctx.cursor.update(oldest_in_chunk)
            → current_end = oldest_in_chunk
      → total_rows, chunks
```

### 9.2 ¿Qué datos recupera?

Desde `now` hacia atrás hasta `origin_ms` o `start_date`. Pagina en chunks de `DEFAULT_CHUNK_LIMIT` (1000 velas).

### 9.3 ¿Desde qué fuente?

CCXT REST (`CCXTAdapter.fetch_ohlcv`).

### 9.4 ¿Cómo persiste?

**Idéntico a Incremental**: `ctx.publisher.publish_chunk()` → Kafka `ohlcv.raw` → `KafkaBronzeWriter` → `IcebergStorage.save_ohlcv()` → Silver Iceberg.

### 9.5 ¿Se solapa con RepairStrategy?

**Sí, parcialmente.** Ambos pueden escribir al mismo `silver.ohlcv` Iceberg table. Ambos usan `_normalize_df` con dedup `unique(keep="last")`. Esto significa que si un backfill y un repair escriben el mismo rango, el último prevalece (last-write-wins).

**Pero**: RepairStrategy no se ejecuta, por lo que no hay solapamiento real en producción.

### 9.6 ¿Resuelve gaps de forma equivalente o diferente?

**Diferente**. Backfill descarga datos históricos desde `now` hacia atrás. Repair detecta gaps en Silver existente y descarga solo el rango faltante. Backfill puede crear gaps si se interrumpe; repair los resuelve.

---

## 10. PERSISTENCIA Y RECONCILIACIÓN

### 10.1 Ruta completa: reparación/recuperación → persistencia

```
repair/backfill
     ↓
raw data (List[list] de CCXT)
     ↓ _raw_to_dataframe()
pl.DataFrame (timestamp, open, high, low, close, volume)
     ↓ transformer.transform()
Normalized pl.DataFrame
     ↓ _normalize_df()
pl.DataFrame con columnas [timestamp, open, high, low, close, volume, exchange, market_type, symbol, timeframe]
Dedup: unique(subset=["timestamp", "exchange", "symbol", "timeframe"], keep="last")
     ↓ Incremental/Backfill: ctx.publisher.publish_chunk(chunk) → Kafka ohlcv.raw
     ↓ Repair: ctx.storage.save_ohlcv(df, symbol, timeframe) → Iceberg directly
     ↓
Kafka ohlcv.raw
     ↓ KafkaBronzeWriter
IcebergStorage.save_ohlcv(df, symbol, timeframe, run_id)
     ↓ append a silver.ohlcv table
Iceberg Silver
     ↓ QualityPipelineConsumer (event bus)
QualityPipeline._scan_and_emit_gaps() → metrics + logs
     ↓ Gold (feature engine, no direct write from repair)
```

### 10.2 ¿Dónde se escribe el dato recuperado?

- **Incremental/Backfill**: `IcebergStorage.save_ohlcv()` via KafkaBronzeWriter (consumer)
- **Repair**: `IcebergStorage.save_ohlcv()` directamente (no pasa por Kafka)

### 10.3 ¿Puede duplicarse?

**Técnicamente sí, pero el dedup protege**:
- `_normalize_df` hace `unique(subset=["timestamp", "exchange", "symbol", "timeframe"], keep="last")` dentro del batch
- Iceberg `append` es append-only, pero `unique` dentro del batch evita duplicación intra-batch
- **Inter-batch duplicación**: Si dos runs escriben el mismo timestamp, Iceberg tendría filas duplicadas a menos que el consumer (BronzeWriter) maneje la dedup a nivel de escritura

### 10.4 ¿Cómo se deduplica?

1. **Intra-batch**: `_normalize_df.unique(subset=["timestamp", "exchange", "symbol", "timeframe"], keep="last")`
2. **Inter-batch**: `IcebergStorage._normalize_df` aplica dedup por `(timestamp, exchange, symbol, timeframe)` con `keep="last"`
3. **Last-write-wins**: La escritura más reciente reemplaza a la anterior

### 10.5 ¿Cómo se reconcilia?

**No hay reconciliación explícita entre runs**. Cada run escribe append-only a Iceberg. Si hay superposición de rangos, el último write gana por `keep="last"`.

### 10.6 ¿Cómo se garantiza idempotencia?

- `ctx.cursor.update()` → Redis almacena el último timestamp procesado
- El siguiente run usa cursor → no re-descarga datos ya procesados
- Iceberg `unique(keep="last")` → re-escritura de mismo rango no duplica

### 10.7 ¿Cómo se actualiza Silver?

`IcebergStorage.save_ohlcv()` → `table.append(prepared.to_arrow())` → Iceberg snapshot.

### 10.8 ¿Cómo llega a Gold?

**No directamente**. Gold se construye desde Silver via `GoldStorage.build()` que lee Iceberg. No hay actualización en tiempo real. El `FeatureEngine` (consumidor de Silver) produce `ohlcv.features` que luego `_GoldFeatureSource` lee para el trading.

---

## 11. RELACIÓN CON DATASET COMPLETENESS

### 11.1 Evidencia

**OHLCV Completeness**:
- `QualityPipeline._scan_and_emit_gaps` detecta gaps en DataFrames descargados
- `scan_gaps` opera sobre `df` actual, no sobre historial acumulado
- Si `RepairStrategy` nunca se ejecuta, gaps detectados nunca se sanan
- El cursor Redis avanza hacia adelante → gaps entre runs pueden persistir

**Bronze Completeness**:
- `KafkaBronzeWriter` consume `ohlcv.raw` → Iceberg
- Si el producer (incremental) no descarga un rango, no hay datos en `ohlcv.raw` → no hay datos en Bronze
- Sin repair, el gap persiste

**Silver Completeness**:
- Silver = Iceberg `silver.ohlcv`
- Backfill/Incremental escriben a Silver vía KafkaBronzeWriter
- Repair escribiría directamente a Silver (si estuviera activo)
- Sin repair: gaps en Silver son permanentes a menos que un backfill manual los cubra

**Gold Completeness**:
- Gold se construye desde Silver via feature engine
- Gaps en Silver → gaps en Gold
- Sin reparación automática: Gold tiene gaps permanentes

### 11.2 Clasificación

**Dataset completeness relationship: PARTIAL**

- RepairStrategy no ejecutándose es un **contribuyente significativo** de gaps no sanados
- No se ha demostrado causalidad directa (falta análisis de logs de producción con gap counts reales)
- Otros factores contribuyen: cursor drift, exchange errors, interrupciones, `_INGESTION_INTERVAL_S=300s`
- El gap detection en `QualityPipeline._scan_and_emit_gaps` solo emite métricas — no acción correctiva

---

## 12. RELACIÓN CON ORDERBOOK

### 12.1 Declaración explícita

**OHLCV Repair/Backfill NO tiene dependencia real con OrderBook resync.** Son dominios completamente separados:

| Aspecto | OHLCV | OrderBook |
|---|---|---|
| **Fuente** | REST (CCXT) | WebSocket (Cryptofeed) |
| **Topic** | `ohlcv.raw` | `orderbook.raw` |
| **Bronze** | `silver.ohlcv` | `orderbook` tables |
| **Recovery** | RepairStrategy (si estuviera activo) | `BookBuilder` (snapshot+delta) |
| **Gap detection** | `scan_gaps(df, timeframe)` | `BookBuilder.on_delta: update_id != last+1` |
| **Gap resolution** | REST fetch (repair) | WS reconnect → snapshot |
| **Consumer** | `KafkaBronzeWriter` | `BookBuilderConsumer` |

### 12.2 Evidencia

De `docs/audits/AUDIT_OCM_orderbook-resync-causality-audit_2026-09-21.md`:
```
OHLCV: REST CCXT fetch_ohlcv → Kafka ohlcv.raw → Bronze Iceberg → Quality → Silver/Gold
OrderBook: WS snapshot+delta → Kafka orderbook.raw → BookBuilder → book.* (no Bronze)
No dependency OHLCV vs OrderBook (different Kafka topics, different Bronze, different Silver)
```

### 12.3 Conclusión

**Sin dependencia**. OHLCV repair no depende de OrderBook resync ni viceversa. Son sistemas independientes con diferentes fuentes, topics y consumers.

---

## 13. TESTS

### 13.1 Tests que existen

| Test | Cobertura | ¿Demuestra runtime? |
|---|---|---|
| `tests/market_data/strategies/test_repair_strategy.py` (922 líneas) | `RepairStrategy.execute_pair()` con `_StorageStub` explícito | ❌ NO — test inyecta `storage` manualmente |
| `tests/market_data/strategies/test_backfill_strategy.py` | `BackfillStrategy._run()` con mocks | ❌ NO — test inyecta `ctx` con mocks |
| `tests/market_data/strategies/test_incremental_strategy.py` | `IncrementalStrategy._run()` con mocks | ❌ NO |
| `tests/architecture/test_kappa_publisher_wiring.py` | Verifica que `OHLCVPipeline` reciba publisher real en producción | ❌ NO verifica `storage` |
| `tests/market_data/test_layer_contracts.py` | Contratos de capas | ❌ NO |

### 13.2 ¿Existe test equivalente a gap → repair → fetch → persist → dedup → Silver?

**NO.** Existe un test unitario de `RepairStrategy` que verifica la lógica interna (gaps detectados, healed, parciales), pero **ningún test demuestra que `main.py` o `OHLCVPipeline` ejecute `RepairStrategy` automáticamente**.

**MISSING E2E TEST**: No existe un test de extremo a extremo que verifique:
1. Gap detectado en Silver (lectura)
2. RepairStrategy seleccionado
3. Datos fetch desde exchange
4. Datos persistidos en Silver con dedup
5. Silver actualizado

### 13.3 ¿Qué garantizan realmente los tests?

Los tests de `RepairStrategy` garantizan que **cuando `ctx.storage` es un stub que retorna DataFrame**, `_read_silver` y `_heal_gap` funcionan correctamente. No garantizan que `ctx.storage` sea un `IcebergStorage` real en producción.

---

## 14. RUNTIME/SYSTEMD

### 14.1 Servicio real

```
systemctl status ocm-market-data  → unit NO EXISTE en host
ocm-market-data.service corre como container (docker)
ExecStart: /home/orangemusic/trading/orangecashmachine/.venv/bin/python -m market_data.main
```

### 14.2 Qué ejecuta el servicio

```
python -m market_data.main
  → uvicorn (FastAPI)
    → _lifespan() startup
      → _ingestion_loop(ctx, guard)  ← SOLO incremental
        → PipelineRequest(mode="incremental")  ← HARDCODED
      → _bronze_writer_loop()  ← consume ohlcv.raw → Iceberg
      → FeedOrchestrator (si ingestion_mode != "rest")  ← WS feeds
```

### 14.3 ¿Puede alcanzar RepairStrategy?

**NO.** `main.py` nunca crea un `PipelineRequest(mode="repair")`. Incluso si `PipelineRequest` acepta `"repair"` como modo válido y `RepairStrategy` está registrado en `_strategies`, nadie invoca `pipeline.run(mode="repair")`.

**OPERATIONAL = UNKNOWN** para repair. No existe evidencia runtime de que repair se ejecute.

### 14.4 `entrypoint.py`

```
uv run python -m app.cli.main pipeline.historical.backfill_mode=true
  → mode = "backfill" o "incremental"
  → NO puede seleccionar "repair"
```

**El CLI tampoco puede ejecutar repair.**

---

## 15. CONTRADICCIONES DOCUMENTACIÓN VS CÓDIGO

| # | Fuente | Afirmación | Realidad | Clasificación |
|---|---|---|---|---|
| 1 | `main.py:127` (docstring) | "Cada iteración lanza el flow completo (backfill + incremental + repair)" | Solo `mode="incremental"` hardcodeado | **CONTRADICCIÓN** |
| 2 | `pipeline_orchestrator.py:20` (docstring) | "Qué modo pasarle (incremental/backfill/repair)" | repair existe en API pero no se selecciona en runtime | **INCOMPLETUD** |
| 3 | `pipeline_runtime.py:269-272` (comentario) | "hoy OHLCVPipeline construye PipelineContext sin storage" | Confirmado — `storage` no se pasa | **CONFIRMADO** |
| 4 | `repair.py:326-329` (assert) | "OHLCVPipeline no lo provee hoy" | Confirmado — `ctx.storage is None` | **CONFIRMADO** |
| 5 | `entrypoint.py:69-71` | Solo backfill/incremental | repair no está en CLI | **CONFIRMADO** |
| 6 | `ohlcv_pipeline.py:170-173` (docstring) | `pipeline.run(mode='repair')` — ejemplo | API válida pero no se invoca | **INCOMPLETUD** |
| 7 | `quality/pipeline.py:471-473` (comentario) | "Gap detection e invariant checks son responsabilidad del consumer downstream (RepairStrategy)" | RepairStrategy nunca se ejecuta como consumer | **CONTRADICCIÓN** |
| 8 | `config/schema.py:323` | `backfill_mode: bool = False` | Controla solo fetcher interno, no modo de pipeline completo | **INCOMPLETUD** |
| 9 | `AUDIT_OCM_orderbook-resync-causality-audit_2026-09-21.md:75` | "OHLCV y OrderBook son dominios separados" | Sin dependencia | **CONFIRMADO** |

---

## 16. MATRIZ FINAL

| Capacidad | Implemented | Connected | Executed | Persisted | Validated | Operational | Legacy | Missing | Unknown |
|---|---|---|---|---|---|---|---|---|---|
| Incremental | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Backfill | ✅ | ✅ | ⚠️ CLI | ✅ | ✅ | ⚠️ | ❌ | ❌ | ❌ |
| Repair | ✅ | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ | `ctx.storage` | ❌ |
| Gap detection | ✅ | ✅ | ✅ (metrics only) | ❌ | ✅ | ⚠️ (sin acción) | ❌ | E2E repair trigger | ❌ |
| Quality | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| HistoricalFetcher | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Persistence | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Deduplication | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Reconciliation | ⚠️ | ✅ | ⚠️ | ⚠️ | ⚠️ | ⚠️ | ❌ | Sin mecanismo explícito | ❌ |
| Silver update | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Gold update | ✅ | ✅ | ✅ (batch) | ✅ | ✅ | ⚠️ | ❌ | ❌ | ❌ |
| Repair→Silver path | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | `ctx.storage` wiring | ❌ |

---

## 17. DECISIONES ARQUITECTÓNICAS REQUERIDAS

### A. ¿Repair debe ser parte del runtime automático?

**OPCIÓN A1 — Sí, automático**: `_ingestion_loop` ejecuta repair periódicamente (ej: cada N iteraciones) para sanar gaps detectados en Silver.
- Requiere `ctx.storage` → `IcebergStorage` inyectado en `PipelineContext`
- Impacto: cambio significativo en constructor de `OHLCVPipeline`
- Complejidad: reparación automática cada ciclo de ingesta

**OPCIÓN A2 — Sí, como task separado**: `main.py::_lifespan` lanza un task asyncio que ejecuta `RepairStrategy` periódicamente, independiente de `_ingestion_loop`.
- Requiere `IcebergStorage` inyectado en el task
- No cambia `OHLCVPipeline` ni `PipelineContext`
- Impacto: menor, mantiene `PipelineContext` limpio

**OPCIÓN A3 — No, manual/CLI**: `RepairStrategy` es una herramienta operacional que se invoca manualmente cuando se necesita recuperar datos específicos.
- No requiere cambios en `main.py` ni `PipelineContext`
- Requiere documentación explícita de que repair es manual
- Impacto: mínimo

### B. ¿Repair es una herramienta operacional/manual?

La evidencia actual sugiere que `RepairStrategy` fue diseñada como componente pero nunca integrada en el runtime. La aserción explícita en `_read_silver` ("OHLCVPipeline no lo provee hoy") sugiere que los autores eran conscientes de la desconexión.

**Sin evidencia concluyente sobre la intención original.** La aserción pudo haber sido colocada como "todo pendiente" o como "intención deliberada de no integrar".

### C. ¿Backfill y Repair son capacidades distintas?

**Sí.** Son fundamentalmente distintas:

| Aspecto | Backfill | Repair |
|---|---|---|
| **Propósito** | Cargar historial completo | Sanar gaps específicos |
| **Trigger** | Manual (CLI) | Automático (si se integrara) |
| **Persistencia** | Kafka → BronzeWriter → Iceberg | Directo Iceberg |
| **Fuente de datos** | Exchange REST | Exchange REST + Silver existente |
| **Scope** | Todo el historial desde now → origin | Solo rangos faltantes |
| **Dedup** | `unique(keep="last")` | `unique(keep="last")` |
| **Dependencia de Silver** | ❌ No lee Silver | ✅ Lee Silver para detectar gaps |

### D. ¿Quién debe decidir cuándo reparar?

**Opciones**:
1. `_ingestion_loop` (automático, basado en gap detection)
2. Un orquestador separado (event-driven: GapDetectedEvent → reparación)
3. Operador humano (CLI)
4. Dagster asset (lectura de Iceberg → disparo de reparación)

### E. ¿Dónde debe vivir esa decisión?

Si es automática: en `_ingestion_loop` o en un nuevo componente. Si es event-driven: en un consumer de Kafka que escuche `GapDetectedEvent` (si se emite). Si es manual: en `entrypoint.py` o script operacional.

### F. ¿Qué componente debe poseer `PipelineContext.storage`?

Si `RepairStrategy` se integra en `OHLCVPipeline`: `ConcretePipelineFactory._build_ohlcv` debe crear `IcebergStorage` (via `IcebergStorageFactory`) y pasarlo a `OHLCVPipeline`.

Si `RepairStrategy` es un task separado: el task constructor recibe `IcebergStorage` directamente desde `CompositionRoot`.

### G. ¿La reparación ocurre antes o después de Bronze/Silver?

**Después de Silver**. `RepairStrategy` lee Silver existente (`ctx.storage.load_ohlcv()`) y escribe Silver (`ctx.storage.save_ohlcv()`). No involucra Bronze (Kafka raw) ni Gold (features).

### H. ¿Existe un problema de arquitectura más amplio?

**Sí.** Existe una tensión entre dos patrones:
1. **Kappa**: Producer → Kafka → Consumer → Storage (para incremental/backfill)
2. **Direct**: Reader → Repair → Writer (para repair)

`PipelineContext.storage` es un punto de integración entre estos patrones que actualmente no está resuelto. El `_build_ohlcv` construye todo para Kappa excepto `storage`, que es nulo por diseño para Kappa pero requerido para repair.

---

## 18. PR BOUNDARIES

### PR-1: Fix de plumbing (`ctx.storage` → `PipelineContext`)
- **Problema demostrado**: `PipelineContext.storage` es `None` porque `OHLCVPipeline.__init__` no pasa `storage` a `PipelineContext`, y `ConcretePipelineFactory._build_ohlcv` no crea `IcebergStorage`
- **Objetivo**: Inyectar `IcebergStorage` en `PipelineContext` para que `RepairStrategy._read_silver` funcione
- **Dependencia**: Ninguna externa, pero requiere cambiar `OHLCVPipeline.__init__` para aceptar `storage`
- **Archivos**: `ohlcv_pipeline.py`, `pipeline_factory.py`, `pipeline/runtime.py`
- **Riesgo**: Cambio en constructor puede romper tests existentes
- **Tests**: Actualizar tests que construyan `OHLCVPipeline` directamente
- **Criterio de aceptación**: `RepairStrategy._read_silver()` no lanza assertion en runtime
- **Independencia**: ✅ Puede hacerse independientemente

### PR-2: Integración de RepairStrategy en runtime
- **Problema demostrado**: `_ingestion_loop` solo ejecuta `mode="incremental"`
- **Objetivo**: Agregar paso de reparación al ciclo de ingesta
- **Dependencia**: PR-1
- **Archivos**: `main.py`, posiblemente nuevo task asyncio
- **Riesgo**: Impacto en performance del ciclo de ingesta; reparación puede tardar
- **Tests**: Test de integración que verifique repair se ejecuta
- **Criterio de aceptación**: `RepairStrategy` se ejecuta en runtime
- **Independencia**: ❌ Requiere PR-1

### PR-3: Gap detection → Repair automation (event-driven)
- **Problema demostrado**: `QualityPipeline._scan_and_emit_gaps` solo emite métricas; no dispara reparación
- **Objetivo**: Conectar detección de gaps con reparación automática
- **Dependencia**: PR-1, PR-2
- **Archivos**: `quality/pipeline.py` (emisor de evento), nuevo consumer
- **Riesgo**: Complejidad adicional; posible ciclo infinito si repair genera gaps
- **Tests**: Event-driven test
- **Criterio de aceptación**: Gap detectado → reparación automática
- **Independencia**: ❌ Requiere PR-1, PR-2

### PR-4: Documentación de modo manual
- **Problema demostrado**: Docstring de `main.py` dice "backfill + incremental + repair" sin que repair se ejecute
- **Objetivo**: Documentar que repair es manual/experimental
- **Dependencia**: Ninguna
- **Archivos**: `main.py` docstring, `README.md` si aplica
- **Riesgo**: Mínimo
- **Tests**: Ninguno
- **Criterio de aceptación**: Documentación refleja realidad
- **Independencia**: ✅ Puede hacerse independientemente

---

## 19. INFORME FINAL

### 19.1 Hallazgos

1. **RepairStrategy existe pero no se ejecuta**: Implementación completa con tests 922 líneas, pero `ctx.storage=None` causa `AssertionError` si se invoca.

2. **`PipelineContext.storage` nunca se inyecta**: `OHLCVPipeline.__init__` no pasa `storage` a `PipelineContext`. El `IcebergStorageFactory` existe en `main.py::_lifespan` pero no se conecta a `OHLCVPipeline`.

3. **`_ingestion_loop` hardcodea `mode="incremental"`**: El docstring miente ("backfill + incremental + repair"). El código solo usa incremental.

4. **`entrypoint.py` no soporta repair**: Solo alterna entre backfill e incremental.

5. **`QualityPipeline._scan_and_emit_gaps` no dispara reparación**: Solo log + métricas. No emite `GapDetectedEvent` que pueda activar repair.

6. **`backfill_mode` es un flag interno del fetcher**: Controla `_resolve_start_timestamp` pero no el modo de pipeline. Solo `entrypoint.py` puede activarlo.

7. **Incremental y Backfill comparten persistencia Kappa**: Kafka → BronzeWriter → Iceberg. Repara rompe este patrón con persistencia directa.

8. **Sin dependencia OrderBook/OHLCV**: Son dominios completamente separados.

9. **Sin E2E test de reparación**: Ningún test demuestra que `main.py` ejecute repair.

10. **Tensión arquitectural Kappa vs Direct**: `PipelineContext.storage` es el punto de integración no resuelto entre el patrón Kappa (incremental/backfill) y el patrón Direct (repair).

### 19.2 Desconocidos

- **Intención original de `RepairStrategy`**: ¿Fue diseñada para ser integrada en runtime o es una utilidad que nunca se terminó de conectar?
- **Impacto de performance de reparación automática**: ¿Cuánto tiempo tarda reparar un gap de N horas? ¿Es viable cada N iteraciones?
- **Efecto de reparación en cursor Redis**: Si repair escribe a Silver pero no actualiza el cursor, el siguiente ciclo incremental podría re-descargar los datos reparados.
- **Comportamiento de Iceberg con writes concurrentes**: Si repair y BronzeWriter escriben simultáneamente a `silver.ohlcv`, ¿hay conflicto?
- **Si `GapDetectedEvent` se emite en algún otro lugar**: La auditoría no encontró emisión de `GapDetectedEvent` desde `QualityPipeline`, pero podría existir en otro componente no inspeccionado.

### 19.3 Veredicto final

**CONFIRMED**: `RepairStrategy` está implementada como código funcional y tiene tests completos, pero está arquitectónicamente desconectada del runtime de producción debido a que `PipelineContext.storage` nunca se inyecta en `OHLCVPipeline`. La detección de gaps en `QualityPipeline` solo emite métricas/logs sin acción correctiva. Backfill y Repair son capacidades distintas que comparten fetcher REST pero tienen caminos de persistencia fundamentalmente diferentes (Kappa vs Directo).

---

## 20. STOP CONDITION

```
AUDIT COMPLETE
Implementation: NOT PERFORMED
Tests modified: NO
Configuration modified: NO
Commit: NOT PERFORMED
Push: NOT PERFORMED
Merge: NOT PERFORMED

Audit document: docs/audits/AUDIT_OCM_ohlcv-repair-backfill-causality_2026-09-21.md
Previous audit: docs/audits/AUDIT_OCM_ohlcv-repair-backfill-execution-paths_2026-09-21.md

Detenerse y esperar autorización humana explícita.
```
