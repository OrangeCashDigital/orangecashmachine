# OCM Audit Findings: Market Data Runtime Deployment & Master Plan Closure
**Fecha:** 2026-08-20  
**Auditor:** Senior Staff Engineer / SRE / Architect  
**Módulo:** `packages/market_data`, `deploy/`, `apps/app`  
**Estado de Auditoría:** `CLOSED_WITH_ACTIONS`  
**Estatus de Implementación:** `COMPLETED (Runtime Operational Verification Passed)`  

---

## 1. Contexto y Objetivos
Cerrar la brecha entre la verificación declarativa (templates, linters, tests) y el estado operacional real en el host (`orangehouse`).

---

## 2. Hallazgos y Correcciones de Runtime (Bugs Críticos)

### F-RT-01 (CRÍTICO): Crash de calidad en `QualityPipeline` por bridge `pl.from_pandas` redundante
- **Síntoma:** El loop de ingesta fallaba en cada iteración con `TypeError: expected pandas DataFrame or Series, got 'DataFrame'`, resultando en `rows=0` para Bybit y Kucoin.
- **Causa:** `QualityPipeline.run()` asumía recibir `pd.DataFrame` y realizaba `pl.from_pandas(df)` internamente. Sin embargo, los tres strategies (`incremental`, `backfill`, `repair`) ya entregaban `pl.DataFrame` (polars nativo).
- **Fix:** Se refactorizó `QualityPipeline` a polars nativo (`run`, `_scan_and_emit_gaps`, `_record_lineage` aceptan `pl.DataFrame`), eliminando el import de pandas y las conversiones internas.
- **Evidencia Commit:** `61da7a9`
- **Evidencia Runtime:** De `rows=0` con TypeError a `rows=499` (Bybit) y `rows=1500` (Kucoin), `error=None`.

### F-RT-02 (ALTO): Mismatch en `PrometheusPipelineMetrics` (`rows_ingested_inc` no implementado)
- **Síntoma:** `AttributeError: 'PrometheusPipelineMetrics' object has no attribute 'rows_ingested_inc'` al finalizar el ciclo de ingesta incremental.
- **Causa:** El adaptador `PrometheusPipelineMetrics` no implementaba el método `rows_ingested_inc()` declarado en el puerto `MetricsPort`.
- **Fix:** Se implementó `rows_ingested_inc(exchange, timeframe, delta)` en `PrometheusPipelineMetrics` utilizando el contador `ROWS_INGESTED` con el centinela `symbol="*"`.
- **Evidencia Commit:** `7ce7c2b`
- **Evidencia Runtime:** Ingesta finaliza con `last_result=success` sin excepciones.

### F-RT-03 (ALTO): Error de permisos en el catálogo Iceberg por ruta relativa `/app` en `.env`
- **Síntoma:** `PermissionError: [Errno 13] Permission denied: '/app'` en el startup de `KafkaBronzeWriter`.
- **Causa:** `.env` configuraba `OCM_STORAGE__DATA_LAKE__PATH=/app/data_platform/data_lake` (orientado a contenedores Docker).
- **Fix:** Se comentó la variable en `.env` para permitir la resolución SSOT al default YAML (`data_platform/data_lake` relativo a la raíz del repositorio).

### F-RT-04 (MEDIO): HTTP 404 en endpoint `/ohlcv` para símbolos con `/` (ej. `BTC/USDT`)
- **Síntoma:** Peticiones HTTP `/ohlcv/bybit/BTC%2FUSDT/1m` retornaban `404 Not Found`.
- **Causa:** `uvicorn` decodifica `%2F` a `/` antes del enrutamiento de FastAPI, produciendo 4 segmentos de ruta que no matcheaban `{symbol}`.
- **Fix:** Se cambió el convertidor de ruta en `market_data/main.py` a `{symbol:path}`.
- **Evidencia Commit:** `be808c3`
- **Evidencia Runtime:** `BTC/USDT` y `BTC%2FUSDT` alcanzan el handler y retornan `{"detail":"no_data", "symbol":"BTC/USDT"}` correctamente.

---

## 3. Estado de Servicios e Infraestructura en Runtime

| Servicio / Proceso | Tipo / Supervisor | PID / ID | Estado Runtime | Evidencia |
|---|---|---|---|---|
| **ocm-streaming.service** | systemd (system) | `2009566` | **ACTIVE / HEALTHY** | WS Bybit conectado; `orderbook.raw` activo (~26k msg / 5 min). |
| **ocm-market-data.service** | systemd (user) | `2250130` | **ACTIVE / HEALTHY** | `uptime > 30s`, `last_result=success`, HTTP `:8001/health` 200 OK. |
| **Kafka (`ocm_kafka`)** | Docker Compose | `5a475c30f6e3` | **ACTIVE / HEALTHY** | Broker `:9092/:9093` activo; `ohlcv.raw` (~3.5k msgs) y `orderbook.raw` (~4.8M msgs). |
| **Redis (`ocm_redis`)** | Docker Compose | `ocm_redis` | **ACTIVE / HEALTHY** | `PONG` con auth `ocm_local_dev`. Cursor store L2 activo. |
| **Pushgateway (`ocm_pushgateway`)** | Docker Compose | `128605a93eec` | **ACTIVE / HEALTHY** | HTTP `:9091/-/healthy` 200 OK. |
| **Bronze Writer (Kappa)** | Async Task (en `market_data.main`) | Interno | **ACTIVE / HEALTHY** | `bronze_written` en logs; **226 archivos Parquet** recientes en `bronze/`. |

---

## 4. Contrato de Salud Unificado: `health_check.sh`
Ejecución en host:
```
MARKET_DATA_HEALTHY=HEALTHY
INFRA_HEALTHY=HEALTHY
OBSERVABILITY_HEALTHY=HEALTHY
# detail: streaming=OK market-data=OK kafka=OK redis=OK orderbook.raw=OK ohlcv.raw=OK bronze=OK pushgateway=HEALTHY
exit=0
```

---

## 5. Matriz de Gates de Calidad y Cierre

| Gate | Estado | Evidencia Concreta |
|---|---|---|
| **CODE VERIFIED** | **PASS** | 1,248 tests pasando; Ruff y Mypy sin errores. |
| **ARCHITECTURE VERIFIED** | **PASS** | 50/50 contratos import-linter Kept; Architecture Governance Linter PASS. |
| **CONFIGURATION VERIFIED** | **PASS** | `OCM_VALIDATE_ONLY=true` exitoso; `deploy/host.env` renderizado y verificado. |
| **DEPLOYMENT VERIFIED** | **PASS** | Unidades systemd renderizadas y validadas con `systemd-analyze verify` (0 errores). |
| **RUNTIME VERIFIED** | **PASS** | `ocm-market-data.service` (user) + `ocm-streaming.service` (system) activos y sanos. |
| **PRODUCTION VERIFIED** | **PARTIAL** | Entorno de ejecución host validado operativamente (`MARKET_DATA_HEALTHY=HEALTHY`); trading live deshabilitado por diseño de seguridad (`IS_STUB=True`). |

### Veredicto Final por Dominio

| Dominio | Estado | Observaciones |
|---|---|---|
| **Market Data** | **HEALTHY** | Pipeline REST incremental + HTTP `:8001` + Kappa stream processor activos. |
| **Streaming** | **HEALTHY** | Orderbook WS Bybit → `orderbook.raw` (~82 msg/s). |
| **Batch** | **HEALTHY** | Loop del daemon REST operando cada 300s; entrypoint CLI verificado. |
| **Kappa** | **HEALTHY** | Stream processor consumiendo `ohlcv.raw` → escritura en Iceberg Bronze. |
| **Kafka** | **HEALTHY** | Broker Confluent `:9092/:9093` con tópicos y flujos verificados. |
| **Redis** | **HEALTHY** | Contenedor activo con cursor store L2 y dedup. |
| **Observability** | **HEALTHY** | Pushgateway `:9091` respondiendo. |
| **Trading / Portfolio** | **NOT_VERIFIED (BY DESIGN)** | `IS_STUB=True` — prohibido ejecutar live en este entorno. |

---

## 6. Veredicto Final del Plan Maestro

**MASTER PLAN — OPERATIONALLY VERIFIED**
