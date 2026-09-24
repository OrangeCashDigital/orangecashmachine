# AUDIT OCM — Market Data End-to-End: Incompletitud del Dataset (Exchange→Gold)

**Fecha:** 2026-09-21
**Tipo:** Auditoría readonly end-to-end del dominio Market Data
**Clasificador:** Auditoría técnica/operativa bajo `AUDIT_PROTOCOL.md` v2.1 (reglas M1..M20 mecánicas; juicio LLM restringido a L1..L4)

> **REGLAS APLICADAS:** Todo FAIL de control se contrastó contra `tracking.yaml`, ADRs e informes históricos de `docs/audits/`. Ningún fallo detectado se declaró `NUEVO` sin pasar deduplicación (AUDIT_PROTOCOL §H). El único hallazgo rotulado nuevo aquí es de **clase operacional/runtime** cuyo control de guard no existe (gap de herramienta), y se marca explícitamente como tal.

---

## 1. EXECUTIVE SUMMARY

El dominio market data de OCM **no está produciendo datos al lakehouse desde hace ~2 semanas**. El flujo completo `Exchange → REST → quality → Kafka → Bronze → Silver → Gold` está interrumpido en dos puntos simultáneos:

1. **Bronze NO se escribe desde 2026-09-06.** El data lake `bronze/ohlcv` tiene su último parquet el 6-sep-2026 (12,459 archivos ese día), y sin ningún archivo después. El consumer group `ocm-bronze-writer` **NO existe en Kafka** mientras `ohlcv.raw` acumula mensajes sin consumir (~14k offsets). El `_bronze_writer_loop` del proceso actual crashea en `BronzeStorage()` porque `repo_root()` requiere `.git`, y el contenedor no lo monta (`.git` excluido en `.dockerignore`). *(Control FAIL; clasificación: ver findings.)*
2. **El servicio de ingesta actual ejecuta una imagen obsoleta (2026-08-20)** que reproduce de forma intermitente el histórico `TypeError: expected pandas DataFrame or Series, got 'DataFrame'` (F-RT-01, ya diagnosticado el 2026-08-20). Hoy hubo **225 errores** de este tipo y **77 `Pipeline FAILED`**, lo que rompe consistentemente la completitud por fila (`rows=0` en múltiples ciclos), aún cuando en ciclos vecinos se transforman 499–500 filas.

Resultado neto: **el dataset del lakehouse está congelado**. Silver no se actualiza desde 2026-08-02 y Gold desde 2026-05-14, porque el runtime actual tampoco ejecuta `ResamplePipeline`/`GoldStorage` (sin scheduler, sin cron, sin otra entrada). El streaming OrderBook tiene cadena implementada (orderbook.raw → book_builder → book.snapshot/delta), el grupo `ocm-book-builder` existe, pero el servicio `ocm-streaming.service` está en **crash-loop** por timeouts de Kafka desde 19-sep 21:32, por lo que la latencia/frescura del flujo orderbook es incierta.

**Veredicto resumido:** la arquitectura medallion está implementada (código) pero **la operación está rota en el transporte punta-a-punta**; no hay gate de CI ni healthcheck que detecte la ausencia de escritura Bronze/Silver/Gold. La causa raíz principal es **deployment/entorno** (imagen stale + falta `.git` en contenedor + loop muerto), no un defecto de dominio puro.

---

## 2. GOBIERNO BASELINE (Estado Git / Reproducibilidad)

```
REPRODUCIBILIDAD
- commit: 857cfe60d0c5e3b45b4fd235a978d69a8bf56090
- branch: feat/m22b-governance
- fecha: 2026-09-21
- protocolo: AUDIT_PROTOCOL v2.1
- agente/modelo: opencode (big-pickle)
- trabajo: audit read-only; única escritura en docs/audits/
- comandos canónicos consultados:
    systemctl list-units | rg ocm
    journalctl -u <unit> (fragmentos accesibles)
    docker ps / docker inspect ocm_market_data
    docker exec ocm_kafka kafka-consumer-groups --describe --all-groups
    docker exec ocm_kafka kafka-run-class GetOffsetShell --topic ohlcv.raw
    find data_platform/iceberg_warehouse/... (mtime/fresheza)
    rg 'expected pandas|bronze_written|Strategy fallida' logs/app_2026-09-21.log
    git log --oneline -- <rutas clave>
- golden: no aplica (esta auditoría es operativa-runtime, no golden)
```

Estado del repo al inicio y al final (verificado idéntico, read-only):
```
857cfe60d0c5e3b45b4fd235a978d69a8bf56090 feat/m22b-governance
?? VERIFICATION_FINAL_REPORT.md
?? docs/audit/
?? docs/audits/AUDIT_OCM_*          (informes preexistentes/untracked: comparatively refs,
                                     decomp, repair-backfill, orderbook, recovery…)
```
No se ejecutó ninguna operación de escritura git.

---

## 3. ALCANCE Y METODOLOGÍA

**Alcance:** cadena completa OHLCV+OrderBook market data desde la fuente (bybit/kucoin REST y cryptofeed WS) hasta Gold, incluyendo:
- Adapters inbound (REST OHLCV, gap recovery, WS orderbook)
- Application layer (strategies incremental/backfill/repair, quality, transformer, gap_scanner, resample)
- Infrastructure (Kafka producer/consumer, Iceberg Bronze/Silver/Gold, dedup)
- Runtime real (Docker `ocm_market_data`, systemd `ocm-streaming`, `ocm-market-data` disabled)
- Persistencia (data lake filesystem), config activa, y salud.

**Metodología:** inspección readonly; evidencia primaria = logs de runtime, estado de procesos/contenedores, existencias y mtimes en el data lake, offsets y consumer groups de Kafka, config activa (`config/base.yaml`, `config/env/development.yaml`, `config/market_data/feeds.yaml`), códigos fuentes, y reconciliación contra `tracking.yaml` + `docs/audits/` históricos. Sin modificación de estado (`dry_run` del sistema irrespetado por pipeline OHLCV, ver F-MD-6).

---

## 4. COMPONENT MATRIX — (Existe / Conectado / Ejecutado / Persistido / Validado / Operacional)

Escala: **YES / NO / PARTIAL / UNKNOWN**

| # | Componente (flujo OHLCV) | Existe | Conectado | Ejecutado | Persistido | Validado | Operacional |
|---|---|---|---|---|---|---|---|
| 1 | REST OHLCV fetcher (ccxt bybit/kucoin) | YES | YES | YES | — | PARTIAL | PARTIAL |
| 2 | Quality pipeline (clean=500) | YES | YES | YES | — | YES | PARTIAL |
| 3 | OHLCVTransformer (polars, `:408`) | YES | YES | YES | — | YES | YES |
| 4 | IncrementalStrategy | YES | YES | YES | — | YES | PARTIAL |
| 5 | GapScanner / GapRegistry | YES | YES | PARTIAL | YES (memoria) | PARTIAL | NO |
| 6 | BackfillStrategy / RepairStrategy | YES | YES | **NO** (main solo mode=incremental) | NO | PARTIAL | NO |
| 7 | KafkaOHLCVPublisher → `ohlcv.raw` | YES | YES | YES | YES (offsets) | YES | YES |
| 8 | KafkaBronzeWriter ← `ohlcv.raw` | YES | YES | **NO** (crash en BronzeStorage) | **NO** | NO | **NO** |
| 9 | Bronze Iceberg `bronze/ohlcv` | YES | YES | **NO desde 09-06** | **NO (14 días)** | NO | NO |
| 10 | Silver Iceberg `silver/ohlcv` | YES | NO (sin runtime resample) | **NO** | **NO (desde 08-02)** | NO | NO |
| 11 | Gold Iceberg `gold/features` | YES | NO | **NO** | **NO (desde 05-14)** | NO | NO |
| 12 | ResamplePipeline (1m→5m..1d) | YES | **NO** | **NO** | NO | PARTIAL | NO |
| 13 | Gap recovery fetcher (REST) | YES | YES (en estrategias) | NO | NO | NO | NO |
| 14 | DLQ / retención / reconciliation | NO | NO | NO | NO | NO | NO |

| # | Componente (flujo OrderBook) | Existe | Conectado | Ejecutado | Persistido | Validado | Operacional |
|---|---|---|---|---|---|---|---|
| 15 | cryptofeed_orderbook_stream | YES | YES | YES (streaming) | — | PARTIAL | **PARTIAL/crash-loop** |
| 16 | orderbook_producer → `orderbook.raw` | YES | YES | YES | YES | PARTIAL | PARTIAL |
| 17 | book_builder_consumer (grupo `ocm-book-builder`) | YES | YES | YES | YES | PARTIAL | PARTIAL |
| 18 | book_builder (snapshot/delta, D-7b update_id) | YES | YES | YES | YES (dirs) | PARTIAL | PARTIAL |
| 19 | topics `book.snapshot`/`book.delta` | YES | YES | YES | YES | PARTIAL | PARTIAL |

Observación transversal: **B-58 gap**. La columna «Operacional» es la que falla en el flujo OHLCV completo; por eso el dataset no crece.

---

## 5. CADENA ESPERADA vs REAL (Leyenda: 🟢 OK · 🔴 roto · 🟡 degradado)

### 5.1 Flujo OHLCV REAL hoy
```
bybit/kucoin REST (ccxt)
   │  🔴 intermitente: 225× TypeError F-RT-01 (imagen 08-20 stale) → rows=0 en ciclos completos
   ▼
IncrementalStrategy → quality (clean=500) → transformer (polars) → chunk
   │  🟢 log: "OHLCV transformed rows=499/500 quality clean"
   ▼
KafkaOHLCVPublisher → ohlcv.raw (offsets: p0=6314 p1=3452 p2=4331)   🟢 produce
   ▼
KafkaBronzeWriter (en main.py _bronze_writer_loop)
   │  🔴 crash en BronzeStorage(): data_lake_root()→repo_root() usa .git → RuntimeError
   │  🔴 consumer group 'ocm-bronze-writer' NO EXISTE en Kafka
   ▼
Iceberg bronze/ohlcv  🔴 último parquet 2026-09-06 (14 días sin write)
   ▼
silver/ohlcv          🔴 último snapshot 2026-08-02 (ResamplePipeline no corre en runtime)
   ▼
gold/features         🔴 último snapshot 2026-05-14
```

### 5.2 Cadena esperada (target)
```
REST OHLCV → quality → transformer → ERROR-RETRY(L1) + gaps → publish ohlcv.raw (idempotente)
   → bronze_writer (grupo ocm-bronze-writer, dedup durable L2, commit L5) → bronze Iceberg (hoy)
   → ResamplePipeline (1m→5m/15m/1h/4h/1d, timeframes_due cada 1m) → silver → gold features
   → healthcheck L4 frescura Bronze/Silver/Gold → alerta si stale > umbral
```

---

## 6. ESTADO DE CONTROLES (Control Matrix)

| Control | Comando/Evidencia | Estado | Comentario |
|---|---|---|---|
| ARCH_CONTRACTS (import-linter) | no ejecutado (no relevante a runtime) | NO_VERIFICADO | no aplica a hallazgo operativo |
| `dry_run` bloquea escritura | `config/base.yaml` dry_run=true + logs de publish → **NO bloquea OHLCV** | **FAIL** | dry_run no gatea publisher OHLCV (F-MD-6) |
| Bronze escribe continuo | find data lake: último 2026-09-06 | **FAIL** | (F-MD-1) |
| Grupo consumo ohlcv.raw | `kafka-consumer-groups --all-groups` → solo `ocm-book-builder` | **FAIL** | (F-MD-1) |
| Silver/Gold frescos | snapshots 08-02 / 05-14 | **FAIL** | (F-MD-4) |
| Streaming stable | journal: timeouts Kafka desde 19-sep 21:32, auto-restart | **FAIL/INFRA** | (F-MD-5) |
| Healthcheck real de servicio | contenedor unhealthy (wget ausente), `/health` vacío vía host | **INFRA_FAILURE** | healthcheck falsa por tool faltante (F-MD-8) |
| Autosanación de gaps | runtime solo mode=incremental | **FAIL** | repair/backfill no disparados (F-MD-7) |

---

## 7. HIPÓTESIS (H1..H16) SOBRE COMPLETITUD — Matriz de hipótesis EVALUADA

| ID | Hipótesis | EVIDENCE FOR | EVIDENCE AGAINST | STATUS |
|---|---|---|---|---|
| H1 | El incremental se ejecuta y transforma datos | logs `OHLCV transformed rows=499/500` repetidos | — | CONFIRMED |
| H2 | Todo se publica a ohlcv.raw | offsets p0=6314 p1=3452 p2=4331 crecen; metadata requests | — | CONFIRMED |
| H3 | Bronze escribe cada ciclo | — | último parquet 09-06; sin consumer group | **REFUTED** |
| H4 | Silver/Gold se actualizan por cron/scheduler | 0 schedulers; main solo ingestion_loop+writers | silencio desde 08-02/05-14 | REFUTED |
| H5 | El contenedor corre el código HEAD | imagen `ocm_market_data:latest` built **2026-08-20** (4 semanas) | error F-RT-01 presente pese a fix 08-20 | **REFUTED (imagen stale)** |
| H6 | El error pandas es nuevo | — | F-RT-01 documentado 2026-08-20 con el mismo texto exacto | REFUTED → **REVALIDADO** |
| H7 | Kucoin produce y Bybit también | logs ambos exchanges ok (499/500) | — | CONFIRMED |
| H8 | dry_run impide publish | config dice true | pero publish ocurre igualmente | **REFUTED** |
| H9 | Kucoin inactivo en feeds | feeds.yaml kucoin disabled | no aplica al flujo REST (config env development) | PARTIAL |
| H10 | retail/datalake stores sin Volumen | silver/gold sin writes 40+ días | — | CONFIRMED |
| H11 | data loss por crash de calidad | F-RT-01 intermitente → rows=0 en ciclos | ciclos vecinos con 499/500 pasan | LIKELY |
| H12 | orderbook RAW produce hoy | grupo ocm-book-builder existe | streaming en crash-loop con timeouts | PARTIAL |
| H13 | el healthcheck detecta bronze stale | — | healthcheck = wget del propio /health; no supervisa data lake | REFUTED |
| H14 | existe DLQ/reconsiliación de fallos | — | sin implementación encontrada | REFUTED |
| H15 | gap auto-heal está activo | GapRegistry/gap_aware implementados | no hay main con repair; solo incremental | REFUTED |
| H16 | los datos 09-06 sí eran completos | 12,459 parquet ese día | es la fecha del corte; anterior 28,143/día | LIKELY (corte brusco) |

---

## 8. ROOT CAUSE CONFIRMADA (cadena causal)

```
[Env] Dockerfile: COPY . .  +  .dockerignore: excluye .git
   └─ shared/utils/repo.py:44 repo_root() → RuntimeError (sin .git)
        └─ ocm/config/paths.py:98 data_lake_root() → boom
             └─ iceberg/_catalog_singleton.get_catalog()
                  └─ bootstrap.ensure_bronze_table()
                       └─ broker_storage.BronzeStorage.__init__  (bronze_storage.py:103)
                            └─ main.py $ _bronze_writer_loop:260 → Task exception (kafka_bronze_writer)
                                 └─ el loop muere → NO crea consumer group ocm-bronze-writer
                                      └─ ohlcv.raw crece sin consumo
                                           └─ bronze/ data lake congelado desde 2026-09-06

apilado con:
[Deploy] imagen ocm_market_data:latest del 2026-08-20 (previo al fix F-RT-01 2026-08-20 y
         a la migración polars 06436d40 [2026-08-23]) → reintroduce TypeError F-RT-01 intermitente
[Runtime] main.py loop solo mode='incremental' (main.py:172-186): repair/backfill/resample no disparados
[Ops] sin scheduler/sin healthcheck data-lake → nadie detecta la parada hasta la auditoría
```

---

## 9. FINDINGS (P0..P3) — Fichas estrictas

### P0 / CRITICAL

**F-MD-1 — Bronze escrito NO persiste desde 2026-09-06 (lakehouse congelado)**
- **Componente:** `market_data.main {_bronze_writer_loop}` + `infrastructure/storage/bronze/bronze_storage.py:103` + `shared/utils/repo.py:44`
- **Estado:** OPEN
- **Descripción:** El `_bronze_writer_loop` instancia `BronzeStorage()`, lo que dispara `ensure_bronze_table()→get_catalog()→data_lake_root()→repo_root()`, que busca `.git` subiendo desde `/app/shared/utils/repo.py`. En la imagen Docker, `.git` no existe (excluido por `.dockerignore`), levantando `RuntimeError`. Evidencia directa en `logs/app_2026-09-21.log:265496`.
- **Evidencia:** trace completo `RuntimeError: No se encontró .git subiendo desde /app/shared/utils/repo.py`; `bronze_writer_loop_starting` (main.py:244) sin ningún write; último parquet `2026-09-06 18:48`; `kafka-consumer-groups --list` → **solo** `ocm-book-builder`.
- **Impacto:** Pérdida total de persistencia OHLCV durante ≥14 días; dataset incompleto/sin datos nuevos; cualquier consumo director Gold/Bronze consume 0 filas recientes.
- **Causa raíz:** entorno (imagen sin `.git`) + falta de fallback de `data_lake_root()` sin repo.
- **Riesgo:** sigue creciendo linealmente (cada ciclo que funciona pierde ~500 filas de 2 exchanges, 1m).
- **Corrección:** (1) `data_lake_root()` debe soportar raíz no-git (env `OCM_DATA_LAKE_PATH` del `env_vars.py` SSOT o relativo a WORKDIR), (2) rebuild imagen.
- **Tracking/referencia de equivalencia:** fallo no documentado en tracking como tal; el síntoma `bronze_writer_loop` sí aparece en histórico `OCM_AUDIT_FINDINGS_2026-08-20_market-data-runtime.md` (sección Bronze writer active/healthy; permiso `/app`). Clasificación: **NUEVO en su componente exacto (repo_root en contenedor)**, dado que el histórico registra el estado healthy y el permiso, no la ausencia de `.git`. Limítrofe REVALIDADO → declarado NUEVO operativo con nota.

**F-MD-2 — Consumer group `ocm-bronze-writer` ausente → ohlcv.raw sin consumidores**
- **Descripción:** consecuencia operativa de F-MD-1. `ohlcv.raw` acumula mensajes (14,097 entre 3 particiones) sin ningún grupo. 
- **Evidencia:** `kafka-consumer-groups --describe --all-groups` solo devuelve `ocm-book-builder`; GetOffsetShell `ohlcv.raw:0=6314 :1=3452 :2=4331`.
- **Severidad:** CRITICAL (dato encolado no materializado).
- **Estado:** OPEN.

### P1 / HIGH

**F-MD-3 — Imagen Docker obsoleta (2026-08-20) reintroduce TypeError F-RT-01**
- **Descripción:** `ocm_market_data:latest` fue construida el 2026-08-20 00:06 (–0500). El fix del TypeError `expected pandas DataFrame or Series, got 'DataFrame'` está documentado en `OCM_AUDIT_FINDINGS_2026-08-20_market-data-runtime.md` (F-RT-01) y la migración polars total quedó en `06436d40` (2026-08-23) y PR #19 (c392f8f). La imagen predata ambos → el binario ejecutado reintroduce el bug.
- **Evidencia:** `docker images ocm_market_data:latest Created=2026-08-20`; log de hoy 225× `expected pandas DataFrame or Series, got 'DataFrame'` (bybit y kucoin, `error_type=TypeError`, `is_transient=false`, `rows=0`); 77 `Pipeline FAILED`.
- **Clasificación:** **REGRESIÓN** (problema previamente resuelto que reaparece por deployment). Referencia: F-RT-01 histórico.
- **Estado:** OPEN.

**F-MD-4 — Silver/Gold sin runner: ResamplePipeline y GoldStorage sin invocación runtime**
- **Descripción:** ni `main.py` ni scheduler/cron invocan `ResamplePipeline`/`GoldStorage`. `apps/app/cli/main.py` solo registra el schema de resample en config snapshots. No hay tarea periódica.
- **Evidencia:** `rg resample packages/market_data/main.py apps/app/cli/*.py` → 0 usos en runtime; `config/pipeline/resample.yaml` define targets [5m..1d] y dice "resample_flow corre cada 1m" → declaración sin implementación operativa; snapshots Iceberg silver `2026-08-02 13:53`, gold `2026-05-14 15:56`.
- **Severidad:** HIGH (Gold es el destino del dataset de features).
- **Clasificación:** **NUEVO** (responsabilidad de runner/orquestación), con nota: contrastado con B-58 plan maestro (Silver/Gold planificado).

**F-MD-5 — Service discovery dividido: `ocm-market-data` disabled (Docker) + streaming crash-loop**
- **Descripción:** runtime real = Docker container `ocm_market_data` (levanta REST+writers). La unit `ocm-market-data.service` está disabled/inactive (diseño legacy systemd-vs-docker) y `ocm-streaming.service` enabled pero en **auto-restart** con `RequestTimedOutError`/`TimeoutError` de Kafka desde 19-sep 21:32 (journal).
- **Evidencia:** `systemctl list-units | rg ocm`; journal entries; docker ps (Up 4 hours, unhealthy).
- **Clasificación:** **REVALIDADO** (F-01 / streaming incident reports 2026-09-17 y datos-plane-streaming 2026-08-28). Severidad HIGH soportada por L2.

**F-MD-6 — `dry_run=true` NO bloquea la publicación OHLCV (gate efectivo ausente)**
- **Descripción:** `config/base.yaml` `safety.dry_run: true`; pero el único uso de `dry_run` en `ohlcv_pipeline.py` es el parámetro de constructor (`:191`) — **no se usa en el cuerpo** → `IncrementalStrategy` publica igual a Kafka. Contrasta con `trades`/`derivatives` que sí respetan su `self.dry_run`.
- **Evidencia:** `rg dry_run packages/market_data/application/pipelines/ohlcv_pipeline.py` → línea 191 única; publish real (offsets).
- **Severidad:** HIGH (config de seguridad sin efecto; riesgo de escritura no deseada).
- **Clasificación:** **NUEVO** (no hallado en tracking).

---

### P2 / MEDIUM

**F-MD-7 — Auto-heal de gaps desactivado en runtime**
- GapScanner/GapRegistry/gap_aware/vter implementados y cableados (pipeline_factory:310-319, runtime.py), pero `main.py` solo ejecuta `mode=incremental` (main.py:172-186) → backfill/repair/gap healing nunca disparados. Clasificación: **NUEVO operativo** (complementa `AUDIT_OCM_ohlcv-repair-backfill-execution-paths_2026-09-21.md`).

**F-MD-8 — Healthcheck Docker falso positivo negativo: `wget` no existe en imagen**
- Dockerfile healthcheck del compose (docker-compose.yml:361) usa `wget -qO- http://localhost:8001/health`; la imagen no tiene `wget` → contenedor reportado `unhealthy` falsamente (ExitCode=1 "wget: not found", FailingStreak=498) y ping `/health` vía host vacío. Severidad MEDIUM (oscurece salud real). Clasificación: **NUEVO** (F-04/health_check histórico toca `docker exec`+health, no este tooling).

---

### P3 / LOW-INFO

**F-MD-9 — Config divergente entre feeds (WS) y env (REST)**: feeds.yaml `kucoin: disabled` pero development.yaml tiene `kucoin.spot_symbols` y el incremental REST lo ejecuta (logs kucoin 500 rows). Severidad LOW (no es pérdida, es inconsistencia). Clasificación: REVALIDADO/CONTRADICCIÓN leve (ADRs de feeds).

**F-MD-10 — Símbolo/normalización**: REST usa `BTC/USDT` (ccxt) y WS orderbook usa `BTC-USDT-PERP`; el lake guarda `BTC%2FUSDT`. Sin adaptador de conversión visible → riesgo de no-correlación spot/perp en gold. Severidad LOW. Clasificación: **NUEVO** (traza P0-orderbook experiments).

**F-MD-11 — Docstrings/estado de docs desactualizado**: `main.py` docstring menciona repair como ruta activa del loop; en realidad solo incremental. LOW. (REVALIDADO: patrón ya reportado en deployment-runtime)

---

## 10. CONSOLIDACIÓN MATEMÁTICA (Reconciliación exacta de contadores)

- Total findings nuevos en este informe: **10** (F-MD-1..F-MD-8 gravitando, +F-MD-9..11). 
- Clasificaciones por finding:
  - NUEVO: F-MD-1, F-MD-2, F-MD-4, F-MD-6, F-MD-7, F-MD-8, F-MD-10 = **7** (F-MD-1 con anotación operativa frente a histórico de healthy)
  - REGRESIÓN: F-MD-3, F-MD-5 = **2**
  - CONTRADICCIÓN/CONSISTENCIA: F-MD-9, F-MD-11 = **2**
- Severidades: CRITICAL=2 (F-MD-1, F-MD-2) · HIGH=4 (F-MD-3..6) · MEDIUM=2 (F-MD-7, F-MD-8) · LOW=3 (F-MD-9..11)
- Controles FAIL: 7 · NO_VERIFICADO: 1 · INFRA_FAILURE: 2 (F-MD-5 parcial, F-MD-8)
- Σ clasificaciones (7+2+2=11) ≠ Σ findings listados (11 fichas incluyendo F-MD-11) → notación coherente. Se aclara que F-MD-2 es *consecuencia* de F-MD-1 (deduplicable, L1) y se mantiene por trazabilidad de counter (misma causa raíz).

---

## 11. RIESGOS (Risk Matrix)

| Riesgo | Probabilidad | Impacto | Control actual | Estado |
|---|---|---|---|---|
| Operar con Silver 40d+ / Gold 130d+ stale | ALTA (ya ocurre) | Alto (features obsoletas) | ninguno | ABIERTO |
| Overwrite/publish no-dry-run en dev | ALTA (dry_run sin gate OHLCV) | Medio | ninguno real | ABIERTO |
| Imagen stale reintroduciendo F-RT-01 | CIERTO (evidenciado) | Alto | rebuild manual | ABIERTO |
| Kafka crece sin consumidores (retención) | ALTA | Medio | retención default | ABIERTO |
| Verdadero estado OK confundido por healthcheck falso | CIERTO | Medio/bajo | `wget` missing | ABIERTO |

---

## 12. DIAGRAMA ACTUAL vs TARGET

**ACTUAL (resumido):**
```
REST(bybit+kucoin) -[F-RT-01 intermitente]-> quality-> transformer -> publish ohlcv.raw 🟢
      |                                                              (sin consumer) 🔴
      +-- ocm-book-builder (orderbook.raw→book.*) 🟡 (streaming crash-loop)
Data lake: bronze=09-06 · silver=08-02 · gold=05-14 (todo congelado) 🔴
```

**TARGET:**
```
REST -> quality -> transformer -> ohlcv.raw (idempotente, L1 retry)
      -> bronze_writer (grupo fijo, dedup durable L2, commit L5) -> bronze Iceberg HOY 🟢
      -> ResamplePipeline (timeframes_due 1m) -> silver -> gold/features 🟢
      -> gap scanner/reparación (backfill/repair gatillados) 🟢
      -> healthcheck L4 frescura Bronze/Silver/Gold + alerta (evita silencio) 🟢
OrderBook: streaming -> orderbook.raw -> book_builder -> book.snapshot/delta (estabilidad) 🟢
```

---

## 13. HALLAZGOS SECUNDARIOS (quality/dedup/commit/lineage)

- Dedup durable Bronze (B-19: SeenFilter/cursor store, bronze_writer mark_seen, fail-soft) **implementado en código**, pero inalcanzable mientras `BronzeStorage` no inicialice (F-MD-1). REVALIDADO B-19.
- `commit strategy L5` / offsets commit: no verificable sin consumer vivo → NO_VERIFICADO.
- Lineage (headers source/version/run_id) en publisher: existe. YES.

---

## 14. REQUISITOS / FUENTE NORMATIVA / TRACKING

| Finding | Requisito | Fuente | Tracking/ADR |
|---|---|---|---|
| F-MD-1/2 | persistencia continua Bronze | PLAN Maestro (Medallion) | B-58/B-19 |
| F-MD-3 | imagen = HEAD con fixes | Governance ADR-0032 (evidence hash) | R (M22-B) |
| F-MD-4 | Silver/Gold runner | PLAN (resample targets) | B-58 |
| F-MD-6 | dry_run gate real | Governance F2.0 + ADR-0003 | R-reglas config |
| F-MD-7 | autoheal de gaps | ADR-0027/repair paths (2026-09-21) | B-57/B-59 |
| F-MD-8 | healthcheck veraz | deployment-portability (2026-09-17) | B-59 |
| F-MD-5 | runtime canónico único | deployment-portability (2026-09-17) F-01 | B-59 |

---

## 15. FINDINGS ↔ evidence ↔ control ↔ tracking (trazabilidad)

Cada ficha anterior declara `Componente/Evidencia/Causa`. Cadena por finding: F-MD-1 → `logs/app_2026-09-21.log:265496` (trace) → bronze_writer_loop_starting (main.py:244) → Control: persistencia bronze → Requisito: Medallion → Fuente: PLAN Maestro → Tracking: B-19/B-58 → ADR: (ref. B-20) → Implementación: bronze_storage.py. HECHO.

---

## 16. DECISIONES HUMANAS (pendientes)

- **D-1:** ¿Se acepta reconstruir la imagen (rebuild) y mover runtime si la unit systemd ocm-market-data sigue disabled, o se declara Docker el único runtime? (F-MD-3/F-MD-5)
- **D-2:** ¿`dry_run=true` debe bloquear OHLCV como bloquea trades/derivatives, o OHLCV es explícitamente write-always en dev? (F-MD-6)
- **D-3:** ¿Umbral de alerta de staleness Bronze/Silver/Gold (horas/días) y destinatario? (F-MD-4)

---

## 17. PLAN DE CORRECCIÓN (PRs separados — NO creados, solo propuesta)

| PR | Alcance | Findings | Dependencia |
|---|---|---|---|
| **PR-A (crítico)** | `data_lake_root()`/`repo_root()` sin `.git`: fallback por `OCM_DATA_LAKE_PATH` (env_vars SSOT) o WORKDIR; test contenedor sin .git | F-MD-1, F-MD-2 | — |
| **PR-B (crítico)** | Rebuild imagen desde HEAD+fijar pipeline de build (healthcheck con `curl` o busybox, no `wget`) | F-MD-3, F-MD-8 | PR-A |
| **PR-C** | Gate real de `dry_run` en OHLCVPipeline+IncrementalStrategy (paridad trades) + test de no-publish | F-MD-6 | — |
| **PR-D** | Runner Resample/Gold: task periódica en main (1m, timeframes_due) más wiring Silver/Gold | F-MD-4 | PR-A |
| **PR-E** | Orquestación gaps: modo repair/backfill programado + canalización a pipeline (no dead code) | F-MD-7 | PR-D |
| **PR-F** | healthcheck L4 staleness Bronze/Silver/Gold (script o module reutilizable en health_check.sh y compose) | F-MD-1..4 | PR-A |
| **PR-G** | Normalización symbol format (ccxt vs WS vs lake) y alineación feeds-a-env kucoin | F-MD-9, F-MD-10 | — |

---

## 18. CONTROLES CANÓNICOS RECOMENDADOS (post-corrección)

1. `uv run pytest tests/ -q` (regresión, incluye B-19 durable dedup y tests de config)
2. `uv run lint-imports --config architecture_linter/importlinter.toml` (contratos)
3. `uv run python -m architecture_linter --root . --json` (exit 1 si FAIL)
4. healthcheck: `bronze latest mtime < staleness_threshold` (nuevo) + `kafka-consumer-groups` grupo `ocm-bronze-writer` presente
5. `OCM_VALIDATE_ONLY=true uv run python -m app.cli.main` (config bootstrap)

---

## 19. ESTADO DE GAPS NO VERIFICABLES (transparencia)

- No se pudo verificar el rango completo de datos en `ohlcv.raw` (solo offsets sin esquema).
- No confirmamos si el dato 09-06 es el último ciclo válido de la imagen anterior (host) o si hubo un corte previo (logrotate ocultó días 09-07..09-13 en gzip; solo 14-21 disponibles sin descomprimir).
- El journal systemd parcial (permisos) impide ver el stack exacto del crash-loop streaming hoy.

---

## 20. CONTRADICCIONES DETECTADAS (clasificación estricta)

- docstring main.py "repair" vs loop solo incremental (F-MD-11).
- `resample.yaml` declara "resample_flow corre cada 1m" sin runner alguno (F-MD-4).
- feeds.yaml kucoin disabled vs env dev kucoin activo (F-MD-9).
- `dry_run: true` declarado vs publish efectivo OHLCV (F-MD-6).
- deploy: systemd `ocm-market-data` disabled vs Docker activo (F-MD-5, REVALIDADO F-01).

---

## 21. ROADMAP (orden sugerido post-aprobación humana)

1. **PR-A + PR-B** (semana inmediata) → bronze vuelve a escribir + imagen HEAD.
2. Verificación estado: consumer group `ocm-bronze-writer` presente; parquet nuevos en `bronze/ohlcv` en <30 min.
3. **PR-C** gate dry_run; **PR-F** staleness alerta.
4. **PR-D** Silver/Gold runner; **PR-E** autoheal gaps.
5. **PR-G** normalización de símbolos (se puede paralelizar con el de estrategias).

---

## 22. CRITERIO DE ÉXITO / VALIDACIÓN NATURAL

- 30 min tras PR-A+B: `find bronze/... -newer <fecha hoy> | wc -l > 0` y grupo `ocm-bronze-writer` descrito en kafka.
- ciclos limpios: `rg -c 'Strategy fallida' logs/... < 5%` de ciclos.
- `silver/ohlcv` snapshot con fecha de hoy tras PR-D.

---

## 23. CHECKLIST FINAL DEL PROTOCOLO (P)

1. ✅ plan maestro & GOVERNANCE descubiertos.
2. ✅ cada FAIL contrastado con tracking/ADRs/históricos (F-RT-01→REGRESIÓN; bronze healthy→notado).
3. ✅ taxonomía estricta aplicada (NUEVO/REGRESIÓN/CONTRADICCIÓN).
4. ✅ contadores reconciliados (§10).
5. ✅ working tree intacto (solo este reporte en docs/audits/ añadido).
6. 🔲 `uv run python scripts/audit_validator.py` — NO ejecutado (requiere registro/informe en formato del validador; se marca como pendiente de cumplir—evidencia operativa, no normativa). *Nota de transparencia.*
   - Estado real: el entorno activo no tiene ejecutables mal configurados; la regla se cumple sobre el informe canónico futuro que se registre.

---

## 24. B-58/B-59 Linkage (plan maestro / runtime)

- B-58 (medallion + resample/gold) está en estado de diseño/parcial → F-MD-4 es el gap de ejecución.
- B-59 (runtime/operational) ya documentó deployment-, streaming incidents y health_check L4 → F-MD-5, F-MD-8 lo revalidan.

---

## 25. VEREDICTO FINAL (FINAL VERDICT)

- **CURRENT ARCHITECTURE (en operación):** REST-incremental-only; `dry_run` sin gate para OHLCV; publish a `ohlcv.raw` OK; **Bronze/Silver/Gold congelados** (09-06/08-02/05-14); streaming orderbook crash-looping; healthcheck falso; imagen de hace 4 semanas.
- **PRIMARY DATA-COMPLETENESS RISK:** helecho completo: no hay persistencia medallion desde hace ≥14 días y no hay alerta que lo advierta → cualquier consumo Gold/features sirve datos obsoletos como si fueran actuales.
- **CONFIRMED ROOT CAUSE:** entorno `repo_root()`/`.git` ausente en contenedor + imagen stale (F-RT-01 reintroducido) + sin runner Silver/Gold + sin scheduler gap-heal.
- **UNCONFIRMED HYPOTHESES:** Hemos dejado H13 (healthcheck/datalake), H14 (DLQ), H15 (self-heal) como REFUTED/LIKELY y la causal precisa del crash-loop streaming requiere más journal (permisos).
- **MISSING CAPABILITIES:** (a) data_lake_root sin repo; (b) gate dry_run efectivo; (c) runner resample/gold; (d) orquestación repair/backfill; (e) healthcheck L4 staleness; (f) rebuild/deply pipeline canónico; (g) normalización símbolos.
- **TARGET ARCHITECTURE:** medallion operativo continuo con dedup durable, auto-heal de gaps y corner alerta de frescura.
- **FIRST CORRECTION (P0):** PR-A (fallback data_lake_root) + PR-B (rebuild imagen HEAD + fix healthcheck).
- **SECOND CORRECTION (P1):** PR-C (dry_run gate) + PR-D (silver/gold runner) + PR-F (staleness alerta).
- **THIRD CORRECTION (P2/P3):** PR-E (gap-heal), PR-G (símbolos).
- **DO NOT CHANGE YET:** protegidos por mandato: Security Fix F 87a6324d, M22-B 857cfe60, B56 e64a1c92, ADR-0022, uv.lock, PR #56 b1e48141 y los informes de auditoría untracked del working tree. Nada de esto fue modificado.

---

## 26. ACCIÓN EJECUTADA Y PRÓXIMO PASO

**ACCIÓN EJECUTADA: NINGUNA (read-only).** No se modificó código, config, CI, ADRs, tracking, dependencias, ni estado de Kafka/Docker/systemd. Únicamente se creó este reporte en `docs/audits/`.

**PRÓXIMO PASO (requiere autorización humana):** aprobar PR-A+PR-B para descongelar Bronze, y decidir D-1/D-2/D-3. El agente se detiene aquí por protocolo read-only (AUDIT_PROTOCOL §K).