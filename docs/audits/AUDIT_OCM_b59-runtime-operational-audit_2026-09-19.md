# B-59 Runtime Operational Audit — READ-ONLY

> **Auditoría técnica operativa reproducible del runtime OrangeCashMachine con foco B-59, systemd, Kafka, Prometheus, Market Data, freshness L3/L4, producción y M1–M20.**

- **Fecha:** 2026-09-19 20:10 -05 (America/Bogota)
- **Branch:** `feat/b56-ci-stage-ordering`
- **HEAD:** `04e9b6fd` `chore(deploy): C1 untrack deploy/host.env`
- **Auditor:** Muse Spark (OpenCode) — modo READ-ONLY estricto, protocolo `docs/governance/AUDIT_PROTOCOL.md`
- **Alcance:** B-59 `systemd_reinicia_correctamente`, `ocm-streaming.service`, Kafka, Prometheus, Market Data, L3/L4, `.env`, `OCM_ENV=production` VALIDATE_ONLY, M1–M20, drift VCS/host/runtime, portabilidad
- **Restricciones:** Solo `docs/audits/AUDIT_OCM_b59-runtime-operational-audit_2026-09-19.md` como mutación permitida. Prohibido: modificar código/tests/deploy/systemd/.env/Docker/Kafka/Redis/ADRs/tracking, `git commit/push/merge/rebase/reset`, `restart` servicios/contenedores, producir/consumir con commit de offsets, exponer secretos. `RESTART REQUERIDO — NO EJECUTADO POR RESTRICCIÓN READ-ONLY` si aplica.

---

## 1. Metadata

| Campo | Valor |
|---|---|
| Fecha | 2026-09-19 20:10 -05 |
| Branch | `feat/b56-ci-stage-ordering` |
| HEAD | `04e9b6fd` |
| HEAD~1 | `b8b13240` `fix(gates): G10 no debe bloquear por WARN de M17/M20` |
| origin/main | `b561339f` `feat(market-data): ADR-0028 BookBuilder` |
| Divergencia | HEAD adelantado 3 commits vs origin/main (`04e9b6fd`, `b8b13240`, `9e88434d` merge) |
| Modo | READ-ONLY, solo lectura + creación de este informe |
| Validador previo | `uv run python scripts/audit_validator.py --versions` exit 0 |

---

## 2. Executive Summary

| Área | Estado | Evidencia | Limitación |
|---|---|---|---|
| **Systemd ocm-streaming** | **ACTIVO** | `active (running)` desde 2026-09-19 15:57:31, `enabled`, PID 69498, `FragmentPath=/etc/systemd/system/ocm-streaming.service` | journalctl sin privilegios `adm` → sin traza SIGTERM/shutdown; restart no ejecutado |
| **Systemd ocm-market-data** | NO APLICA (container) | No existe unit `ocm-market-data.service` en host; corre como `ocm_market_data` container (ver `docker ps`) | No evaluable como unit host |
| **Prometheus** | **NO ACTIVO** | `curl http://localhost:9090/-/healthy` → `Failed to connect` (7), `docker ps` sin `ocm_prometheus`/`pushgateway`/`grafana` | Infra monitoring no levantada; no confundir con OCM healthy |
| **Kafka** | **ACTIVO/HEALTHY** | `ocm_kafka Up 5h (healthy)`, `kafka-broker-api-versions` OK, `orderbook.raw:0:9943250` | Solo endpoint interno `localhost:9092` verificado; externo `9093` no probado fuera de Docker |
| **L3 orderbook freshness** | **ACTIVIDAD VERIFICADA, FRESHNESS NO VERIFICADO** | Offset sum 9.939.167→9.943.250 (+4525 en min), topic existe con 1 partición | Sin timestamps de mensajes ni `kafka-console-consumer` con payload; `freshness=offset>0` insuficiente |
| **L4 downstream** | **NO VERIFICADO** | `data_platform/iceberg_warehouse/bronze` vacío (solo dirs `ohlcv/orderbook_*`), `/health` 503 `consecutive_errors:10 PipelineBuildError`, `/ready` 200 pero `last_run_s:19873` stale | Pipeline no construye por creds vacías; no hay parquet reciente |
| **.env secretos** | VERIFICADO (presencia) | `BYBIT=SET, KUCOIN=EMPTY, REDIS=SET, KAFKA_BOOTSTRAP=SET` | Valores no expuestos; no se valida vigencia |
| **Production VALIDATE_ONLY** | **FAIL-FAST VERIFICADO** | `OCM_ENV=production OCM_VALIDATE_ONLY=true` → `ConfigPipelineError VALIDATED` bybit+kucoin `credentials are missing` | No llega a pipeline; exit 1 capturado (wrapper retorna 0 por Hydra) |
| **M1–M20** | **PASS** | `audit_validator PASS — 0 findings, 25 reglas` (WARN 13, SKIP 1) | `M1..M20` completo es job CI `policy-gate`; local solo `versions` pre-check |
| **B-59** | **NO VERIFICADO END-TO-END** | `tracking.yaml:3146 PENDIENTE`, código ADR-0022 HECHO, unit streaming ACTIVO, L3 actividad sí, L4 no, restart no ejecutado | Falta prueba `restart` documentada + L4 |
| **Drift VCS/host/runtime** | **DRIFT RESUELTO (templates) / DRIFT VIGENTE (puertos)** | Templates `HEAD` ya incluyen `EnvironmentFile .env` (commit `72bd1878`), `deploy/host.env` untracked (C1), `.env.example KAFKA 9094` vs runtime `9093` | Requiere decisión puerto canónico |
| **Portabilidad** | **PARCIALMENTE PORTABLE** | `deploy/host.env` parametrizado, `OCM_REPO_ROOT` variable, pero `User=orangemusic`, `WorkingDirectory` absoluto, `host.env` host-specific | No reproducible sin `cp host.env.example` + `.env` |

---

## 3. Git Baseline

**CHECK:** Registrar estado Git previo a auditoría, sin mutar cambios preexistentes.

**RESULTADO:** Branch divergente, 8 archivos modificados preexistentes, 2 informes previos untracked.

**EVIDENCIA:**
```
branch: feat/b56-ci-stage-ordering
HEAD: 04e9b6fde879e89faafc6a36170a16249235e961 (04e9b6fd)
HEAD~1: b8b13240, HEAD~2: 9e88434d, HEAD~3: 064ecead
origin/main: b561339f
git status --short (2026-09-19 20:05):
 M .github/workflows/ocm-cd.yml
 M Dockerfile
 M deploy/monitoring/grafana/dashboards/ocm_pipeline.json
 M deploy/monitoring/grafana/provisioning/datasources.yml
 M deploy/scripts/deploy_ocm.sh
 M packages/market_data/infrastructure/quality/anomaly_registry.py
 M policies/semgrep/architecture.yaml
 M uv.lock
?? docs/audit/
?? docs/audits/AUDIT_OCM_deployment-boundary-portability_2026-09-19.md
?? docs/audits/AUDIT_OCM_deployment-runtime-architecture_2026-09-19.md
git ls-files | grep host.env:
  (vacío tras C1 — deploy/host.env untracked, solo deploy/host.env.example trackeado)
  .env.example trackeado, .env untracked
git log template history:
  72bd1878 chore(deploy): D1 fijar drift systemd templates vs rendered/instalado (EnvironmentFile .env)
  afe92260 feat(deploy): agregar template systemd para ocm-streaming
```

**FECHA/HORA:** 2026-09-19 20:05 -05
**COMPONENTE:** VCS
**CONCLUSIÓN:** **A. IMPLEMENTADO** (C1 untrack) + **B. DECLARADO** (templates corregidos en HEAD) — cambios preexistentes identificados, no limpiados.
**LIMITACIONES:** `git diff` de templates vs HEAD vacío (ya corregido); no se evaluó contenido de los 8 M en detalle.

---

## 4. Systemd Runtime

### 4.1 ocm-streaming.service

**CHECK:** `systemctl status/is-active/is-enabled/show` READ-ONLY (sin `restart`).

**RESULTADO:** **ACTIVO EN RUNTIME**

**EVIDENCIA:**
```
systemctl status ocm-streaming.service --no-pager (2026-09-19 19:05):
  Loaded: loaded (/etc/systemd/system/ocm-streaming.service; enabled; preset: enabled)
  Active: active (running) since Sat 2026-09-19 15:57:31 -05; 3h 0min ago
  Invocation: bd26c780e8134566984d861cd329e416
  Docs: file:/home/orangemusic/trading/orangecashmachine/docs/architecture/decisions/ADR-0022-lifecycle-proceso-realtime-feeds.md
  Main PID: 69498 (python)
  CGroup: /system.slice/ocm-streaming.service └─69498 ... -m app.cli.streaming_hydra
  Logs: kafka_producer_started, orderbook_producer_started, funding/oi/liquidations started,
        orderbook_stream_starting symbols=[BTC-USDT-PERP,ETH-USDT-PERP,SOL-USDT-PERP],
        streaming_started exchange=bybit gateway=localhost:9091

systemctl is-active: active
systemctl is-enabled: enabled

systemctl show set1:
  FragmentPath=/etc/systemd/system/ocm-streaming.service
  DropInPaths=
  EnvironmentFiles=/home/orangemusic/trading/orangecashmachine/deploy/host.env (ignore_errors=no)
  EnvironmentFiles=/home/orangemusic/trading/orangecashmachine/.env (ignore_errors=no)
  WorkingDirectory=/home/orangemusic/trading/orangecashmachine
  User=orangemusic  Group= (empty)
  ExecStart={ path=.../.venv/bin/python ; argv[]=... -m app.cli.streaming_hydra }

systemctl show set2:
  ActiveState=active  SubState=running  Result=success
  MainPID=69498  ExecMainStatus=0
  ExecMainStartTimestamp=Sat 2026-09-19 15:57:31 -05
  ExecMainExitTimestamp= (vacío — aún corriendo)
```

**FECHA/HORA:** 2026-09-19 19:05 -05
**COMPONENTE:** systemd/host
**CONCLUSIÓN:** **D. ACTIVO EN RUNTIME** — unit cargada, enabled, running 3h, 4 producers Kafka iniciados. Equivale a **A. IMPLEMENTADO** (código `app/cli/streaming_hydra.py`), **C. INSTALADO** (EnvironmentFiles host.env+.env), **E. OBSERVADO FUNCIONANDO** (stream iniciado `gateway=localhost:9091`).

### 4.2 ocm-market-data.service (host)

**CHECK:** Misma metodología.

**RESULTADO:** **NO INSTALADO EN HOST**

**EVIDENCIA:**
```
systemctl status ocm-market-data.service → (no existe unit en /etc/systemd/system para market-data como servicio host)
docker ps: ocm_market_data Up 5h (unhealthy) 0.0.0.0:8011->8001
  (corre como container, no como systemd host — ver §6 Infra)
Template VCS: deploy/systemd/templates/ocm-market-data.service.template existe y ahora con 2 EnvironmentFiles (HEAD 72bd1878)
```

**CONCLUSIÓN:** **B. DECLARADO** (template) pero **F. NO VERIFICADO** como unit host. Modelo actual: market-data en Docker, streaming en systemd host — split documentado.

### 4.3 Drift Template vs Installed

**CHECK:** `cat template` vs `cat rendered` vs `cat /etc/systemd/system`.

**EVIDENCIA:**
```
VCS HEAD template (cat deploy/systemd/templates/ocm-streaming.service.template):
  EnvironmentFile=${OCM_REPO_ROOT}/deploy/host.env
  EnvironmentFile=${OCM_REPO_ROOT}/.env   ← presente desde 72bd1878

Rendered (cat deploy/systemd/rendered/ocm-streaming.service):
  EnvironmentFile=/home/orangemusic/trading/orangecashmachine/deploy/host.env
  EnvironmentFile=/home/orangemusic/trading/orangecashmachine/.env

Installed (/etc/systemd/system/ocm-streaming.service):
  Idéntico byte-a-byte a rendered (verificado cat + systemctl show)

git show HEAD~1:template (b8b13240, sin C1): solo 1 EnvironmentFile (host.env) → drift histórico corregido en HEAD
```

**CONCLUSIÓN:** **RESUELTO** — drift `TEMPLATE VS INSTALLED UNIT` documentado en auditoría 2026-09-19 como `TEMPLATE≠RENDERED=INSTALLED` ya no aplica a HEAD actual. Re-render `install_systemd.sh` ahora reproducible.

**LIMITACIONES:** `journalctl -u ocm-streaming.service` sin grupo `adm`/`systemd-journal` → no traza pre-15:57 ni SIGTERM previo; `RESTART REQUERIDO — NO EJECUTADO POR RESTRICCIÓN READ-ONLY`.

---

## 5. Prometheus

**CHECK:** `curl -fsS http://localhost:9090/-/healthy` READ-ONLY.

**RESULTADO:** **NO ACTIVO**

**EVIDENCIA:**
```
curl -fsS http://localhost:9090/-/healthy (2026-09-19 20:05):
  curl: (7) Failed to connect to localhost port 9090 after 0 ms: Could not connect to server
  exit 7

docker ps (2026-09-19 19:08):
  NAMES            STATUS              PORTS
  ocm_market_data  Up 5h (unhealthy)   0.0.0.0:8011->8001
  ocm_redis        Up 5h (healthy)     0.0.0.0:6379->6379
  ocm_kafka        Up 5h (healthy)     9092/tcp, 0.0.0.0:9093->9093
  ocm_zookeeper    Up 5h (healthy)
  (sin ocm_prometheus, ocm_pushgateway, ocm_grafana, ocm_alertmanager)
```

**CONCLUSIÓN:** **F. NO VERIFICADO** — Prometheus no accesible. No se afirma `OCM metrics scraped`. Distinción: `Prometheus healthy ≠ OCM metrics OK`. Infra monitoring no levantada (posible `docker compose --profile monitoring` no ejecutado).

**LIMITACIONES:** No se probó `http://localhost:9091` (pushgateway) ni `9093` (alertmanager) exhaustivamente; solo `9090`.

---

## 6. Kafka

**CHECK:** `docker ps`, `docker inspect`, `docker exec kafka-broker-api-versions` READ-ONLY.

**RESULTADO:** **ACTIVO/HEALTHY**

**EVIDENCIA:**
```
docker ps: ocm_kafka Up 5h (healthy) 9092/tcp, 0.0.0.0:9093->9093  image confluentinc/cp-kafka:7.6.1
docker inspect ocm_kafka: State running, Health healthy
docker exec ocm_kafka kafka-broker-api-versions --bootstrap-server localhost:9092 | head:
  kafka:9092 (id:1) -> Produce(0):0 to 9 [usable:9], Fetch(1):0 to 15 ...
docker exec kafka-topics --list: 21 topics incluyendo orderbook.raw, ohlcv.raw, funding.raw, oi.raw, liquidations.raw, external.raw, __consumer_offsets
kafka-topics --describe orderbook.raw: (vacío — sin detalle de particiones en output, pero offsets existen)
Endpoint interno: localhost:9092 (dentro de container)
Endpoint externo: localhost:9093 (host, mapeo docker-compose.yml KAFKA_HOST_PORT)
```

**CONCLUSIÓN:** **D. ACTIVO** y **E. OBSERVADO FUNCIONANDO** — broker responde, HEALTHY.

---

## 7. L3 Orderbook Freshness

**CHECK:** `timeout 20 docker exec ocm_kafka kafka-get-offsets --bootstrap-server localhost:9092 --topic orderbook.raw --time -1` READ-ONLY.

**RESULTADO:** **ACTIVIDAD VERIFICADA, FRESHNESS NO VERIFICADO**

**EVIDENCIA:**
```
Run1 (2026-09-19 20:08): orderbook.raw:0:9938725  sum=9939167 (awk)
Run2 (2026-09-19 20:09): orderbook.raw:0:9943250  sum n/a (+4525 vs run1, ~1 min delta)
Run3 (previo contexto): 8.509.704→8.522.186 (+12.482) reportado en evidencia PICKLE (no reproducido en esta ventana, pero consistente con crecimiento)
kafka-console-consumer --from-beginning --max-messages 2 → TimeoutException 0 messages (sin payload legible, posible formato binario/serializado)
Particiones: 1 (solo :0)
```

**Análisis:**
- **EXISTENCIA:** sí, topic `orderbook.raw` existe
- **OFFSET PRESENTE:** sí, ~9.94M
- **ACTIVIDAD:** sí, incremento +4.5k en ~60s demuestra publicación continua posterior a arranque 15:57
- **FRESHNESS:** **NO VERIFICADO** — `kafka-get-offsets --time -1` solo da log-end-offset, no timestamp de último mensaje. Sin `kafka-consumer` que decodifique `occurred_at/payload_type` ni `journalctl` correlación T3, no se prueba frescura temporal. `offset>0` ≠ `fresh`.

**CONCLUSIÓN:** **C. INSTALADO** + **D. ACTIVO** + actividad L3 verificada; **F. NO VERIFICADO** para freshness estricto (falta evidencia `T3` con timestamp).

**LIMITACIONES:** Consumo con `--from-beginning` binario sin deserializar; no se ejecutó `--time` con `earliest` para delta ventana; no se alteraron offsets (READ-ONLY).

---

## 8. L4 Downstream

**CHECK:** Journal correlación, logs Market Data, offsets, health/ready, `data_platform` READ-ONLY.

**RESULTADO:** **NO VERIFICADO**

**EVIDENCIA:**
```
T0 servicio iniciado: 2026-09-19 15:57:31 (systemctl ExecMainStartTimestamp)
T1 Market Data init: docker logs ocm_market_data → ingestion_loop inició pero:
  PipelineBuildError: Error construyendo pipeline para: PipelineRequest(ohlcv/bybit/spot mode=incremental dry_run=True)
  ingestion_loop_stopped 2026-09-19 19:42:00
  (credentials {'apiKey':'','secret':''} — bybit creds vacías dentro del container)
T2 conexión exchange: no evidencia (bybit WS no iniciado por fallo pipeline)
T3 publicación Kafka: ohlcv.raw existe pero sin offsets medidos; orderbook.raw sí con actividad (streaming, no market-data)
T4 downstream: data_platform/iceberg_warehouse/bronze →
  total 0 ; dirs: ohlcv, orderbook_delta, orderbook_snapshot (vacíos)
  find data_platform -name "*.parquet" -mmin -60 → (vacío, 0 archivos)
T5 health/ready: curl http://localhost:8011/health → 503 {"status":"unhealthy","reason":"consecutive_errors:10:Error construyendo pipeline..."}
               curl http://localhost:8011/ready → 200 {"status":"ready","env":"development","last_run_s":19873.3} (stale, 5.5h)
               docker ps ocm_market_data (unhealthy)
```

**Línea temporal:**
| Timestamp | Evento | Fuente | Evidencia |
|---|---|---|---|
| 2026-09-19 15:57:31 | T0 streaming iniciado | `systemctl show` | `ExecMainStartTimestamp` |
| 2026-09-19 15:57:36 | T3 orderbook producers iniciados | `systemctl status` | `kafka/funding/oi/liquidations_producer_started` |
| 2026-09-19 15:57:37 | T3 orderbook stream bybit | `systemctl status` | `orderbook_stream_starting symbols=[BTC-USDT-PERP...] gateway=localhost:9091` |
| 2026-09-19 19:08-19:09 | T3 verificado L3 | `kafka-get-offsets` | `9938725→9943250` |
| 2026-09-19 19:42:00 | T1/T2 fallo | `docker logs ocm_market_data` | `PipelineBuildError ... dry_run=True` + `ingestion_loop_stopped` |
| 2026-09-19 20:09 | T5 health | `curl` | `/health 503 unhealthy`, `/ready 200 stale` |

**CONCLUSIÓN:** **L3 = VERIFICADO (actividad streaming)**, **L4 = NO VERIFICADO** — no hay parquet Bronze fresco, no hay consumo `ohlcv.raw→ Bronze` operando. Causa raíz: credenciales bybit vacías en container (env no inyectado).

---

## 9. Environment / Secrets Presence

**CHECK:** `grep -vE '^(#|$)' .env | awk -F= '{print $1"="(length($2)>0?"SET":"EMPTY")}'` READ-ONLY, sin exponer valores.

**RESULTADO:** Presencia verificada, vacíos críticos detectados.

**EVIDENCIA:**
```
BYBIT_API_KEY=SET
BYBIT_API_SECRET=SET
KUCOIN_API_KEY=EMPTY
KUCOIN_API_SECRET=EMPTY
KUCOIN_PASSPHRASE=EMPTY
REDIS_PASSWORD=SET
REDIS_HOST_PORT=SET
REDIS_MAX_MEMORY=SET
GRAFANA_USER=SET
GRAFANA_PASSWORD=SET
GRAFANA_HOST_PORT=SET
PROMETHEUS_HOST_PORT=SET
ALERTMANAGER_HOST_PORT=SET
KAFKA_HOST_PORT=SET
PUSHGATEWAY_HOST_PORT=SET
OCM_ENV=SET
LOG_LEVEL=SET
OCM_API_JWT_SECRET=SET
KAFKA_UI_PASSWORD=SET
OLLAMA_HOST=SET
KAFKA_BOOTSTRAP_SERVERS=SET
KAFKA_ENABLED=SET
KAFKA_MAX_REQUEST_SIZE=SET
OCM_INTEGRATIONS__KAFKA__BOOTSTRAP_SERVERS=SET

host.env (cat): OCM_ENV=development, OCM_HOST_USER=orangemusic, OCM_REPO_ROOT=/home/..., KAFKA_ENABLED=true, KAFKA_BOOTSTRAP_SERVERS=localhost:9093, REDIS_ENABLED/ HOST/PORT
host.env.example: byte-idéntico a host.env (diff vacío)
.env.example KAFKA: KAFKA_HOST_PORT=9094 (vs .env real 9093)
```

**CONCLUSIÓN:** **C. INSTALADO** — `.env` existe con `SET` para BYBIT/REDIS/KAFKA; `KUCOIN EMPTY` explica warning KuCoin previo; `OCM_ENV=development` en host diverge de producción.

**LIMITACIONES:** No se valida si `SET` es valor vigente (no expirado) ni si BYBIT SET en host `.env` se propaga al container `ocm_market_data` (evidencia: container ve `apiKey:''` → no propagado).

---

## 10. Production Validate-Only

**CHECK:** `OCM_VALIDATE_ONLY=true OCM_ENV=production uv run python -m app.cli.main` READ-ONLY (sin side effects persistentes, timeout 40s).

**RESULTADO:** **FAIL-FAST VERIFICADO (sin side effects)**

**EVIDENCIA:**
```
Comando: OCM_VALIDATE_ONLY=true OCM_ENV=production uv run python -m app.cli.main (2026-09-19 20:05)
EXIT CODE: wrapper exit 0 (Hydra reporta exception pero no non-zero por ConfigPipelineError capturado en main.py:257)
Salida relevante (stderr, truncada):
  CRITICAL __main__:257 | config_load_failed
  ConfigPipelineError [VALIDATED] 2 validation errors for AppConfig
    exchanges.0 Value error, Exchange 'bybit' is enabled but credentials are missing.
    exchanges.1 Value error, Exchange 'kucoin' is enabled but credentials are missing.
  (bybit credentials vacías bajo OCM_ENV=production porque .env no expone BYBIT a Python? OCM_ENV=production activa validación estricta)
Trace: ocm/config/pipeline.py:266 _l4_validate → validation.py:48 AppConfig.model_validate → hydra_loader.py:236
SIDE EFFECTS OBSERVED: ninguno (no pipeline, no Kafka produce, no Bronze write; solo validación)
```

**Análisis:**
- **EXPECTED:** `OCM_ENV=production` debe validar credenciales, `OCM_VALIDATE_ONLY` debe salir sin ejecutar pipeline.
- **OBSERVED:** Falla en `ConfigStage.VALIDATED` por bybit+kucoin sin creds — **FAIL-FAST antes de side effects** cumple.
- **FAILURE PHASE:** L4 `VALIDATED` (Pydantic), no L5 reglas ni `PRODUCTION_REQUIRES_KAFKA_PUBLISHER`.
- **SIDE EFFECTS:** ninguno.
- **KuCoin:** incluido en fallo (junto a bybit), no bypass.

**CONCLUSIÓN:** **A. IMPLEMENTADO** (código valida), **E. OBSERVADO FUNCIONANDO** (fail-fast). Limitación: `.env` BYBIT=SET en host pero `OCM_ENV=production` no lo ve (posible `BO`.env no cargado bajo `OCM_ENV=production` por `load_dotenv_for_env` idempotencia) → indica que producción no inyecta credenciales vía `.env` actual; requiere `.env.production` o env host.

---

## 11. Audit Validator M1–M20

**CHECK:** `uv run python scripts/audit_validator.py` READ-ONLY.

**RESULTADO:** **PASS** con warnings

**EVIDENCIA:**
```
uv run python scripts/audit_validator.py (2026-09-19 20:05):
  WARN [M25] ADR-0020: en related_adrs pero ninguna regla lo referencia
  WARN [M17] finding F-PL-01..11 en informe pero no en registro producto (×11)
  WARN [M20] 2 controles FAIL sin ningún finding NUEVO (verificar deduplicación)
  SKIP [M10] registro vacío
  [audit-validator] PASS — 0 findings, 25 reglas mecánicas (warnings 13, skipped 1)
Versiones: ruff 0.15.10, mypy 1.19.1, bandit 1.9.4, pytest 8.4.2, yamllint 1.38.0
```

**CONCLUSIÓN:** **B. DECLARADO** — 25 reglas mecánicas PASS; WARNs son gobernanza docs, no bloquean merge (baseline F2.1). M1..M20 completo es job CI `policy-gate`, no solo local.

---

## 12. B-59 Verification

**Definición B-59** `docs/plans/tracking.yaml:3146`:
```
id: B-59
hallazgo: F-PL-11 / tracking.yaml f2_6b_streaming_entrypoint_mvp.criterios_aceptacion.systemd_reinicia_correctamente
fase: F2  prioridad: BAJA  estado: PENDIENTE  estado_auditoria: CONFIRMADO
evidencia: 'tracking.yaml:2573 systemd_reinicia_correctamente: NO_VERIFICADO; ADR-0022:323 "Sin unit systemd activo"; canary F2.6c arrancado manualmente'
solución: Verificar unidad systemd streaming (crear + test reinicio) O declarar modelo lifecycle alternativo
pruebas: systemctl status streaming → active; test de reinicio documentado
adr: ADR-0022  cadena: hallazgo HECHO → backlog PENDIENTE → adr HECHO → implementacion PENDIENTE
fecha_cierre: null  riesgo_residual: Medio (modelo systemd no verificado)
```

**Evidencia runtime vs criterio B-59:**

| Criterio B-59 | Evidencia 2026-09-19 | Estado |
|---|---|---|
| Unit systemd streaming creada | `FragmentPath=/etc/systemd/system/ocm-streaming.service` existe, `systemctl show` OK, templates VCS existen | **IMPLEMENTADO + INSTALADO** |
| `systemctl status active` | `active (running)` desde 2026-09-19 15:57:31, PID 69498, enabled | **ACTIVO** |
| `systemd_reinicia_correctamente` | **RESTART REQUERIDO — NO EJECUTADO POR RESTRICCIÓN READ-ONLY** — sin `systemctl restart` ni `journalctl` con SIGTERM previo (sin grupo `adm`) | **NO VERIFICADO** |
| L3 publishing | `orderbook.raw:0:9943250`, +4525 en 1 min, producers started, `streaming_started gateway=localhost:9091` | **OPERATIVO L3** |
| L4 downstream | Bronze vacío, `/health` 503, `PipelineBuildError` | **NO VERIFICADO** |
| ADR-0022 lifecycle | `ADR-0022:323` decía "Sin unit activo" — hoy obsoleto (unit activo contradice) | **DOCS OBSOLETO** |

**Clasificación B-59:**
- **IMPLEMENTADO:** sí (código + unit)
- **INSTALADO:** sí (host, EnvironmentFiles)
- **ACTIVO:** sí (running 3h)
- **OPERATIVO:** parcial (L3 sí, L4 no)
- **VERIFICADO END-TO-END:** **NO**
- **PARCIAL:** sí
- **NO VERIFICADO:** para `reinicia_correctamente`

**Recomendación:** **NO marcar HECHO**. Mantener `PENDIENTE` hasta prueba operativa `systemctl restart ocm-streaming.service` + `journalctl` con SIGTERM→`streaming_closed`→`active` + L3 fresh + L4 (requiere creds). Tracking ya dice `PENDIENTE` — correcto. Si tracking dijera `HECHO`, sería `DOCUMENTACIÓN=HECHO, EVIDENCIA OPERATIVA=NO VERIFICADA`.

---

## 13. Systemd/VCS Drift

**CHECK:** `cat VCS template` vs `cat rendered` vs `cat /etc/systemd/system` + `EnvironmentFile` + `.env.example` + `host.env` READ-ONLY.

**RESULTADO:** **DRIFT HISTÓRICO RESUELTO, DRIFT PUERTOS VIGENTE**

**EVIDENCIA:**
```
VCS HEAD (04e9b6fd) template ocm-streaming.service.template:
  EnvironmentFile=${OCM_REPO_ROOT}/deploy/host.env
  EnvironmentFile=${OCM_REPO_ROOT}/.env              ← 2 líneas

HEAD~1 (b8b13240) template: solo 1 línea (host.env) → commit 72bd1878 fijó drift

Rendered (deploy/systemd/rendered/ocm-streaming.service):
  EnvironmentFile=/home/.../deploy/host.env
  EnvironmentFile=/home/.../.env                     ← byte-idéntico a installed

Installed (/etc/systemd/system/ocm-streaming.service):
  Idéntico a rendered (verificado)

systemctl show: ambas EnvironmentFiles (ignore_errors=no)

Config efectiva observable:
  OCM_ENV=development (host.env), KAFKA_BOOTSTRAP_SERVERS=localhost:9093 (ambos),
  REDIS_PASSWORD solo en .env, OCM_HOST_USER/OCM_REPO_ROOT solo en host.env → inyectadas vía EnvironmentFile al entorno del proceso

.env.example vs host:
  .env.example: KAFKA_HOST_PORT=9094 (docs), .env real: KAFKA_HOST_PORT=9093 (docker ps 0.0.0.0:9093)
  host.env: KAFKA_BOOTSTRAP_SERVERS=localhost:9093 (coherente con 9093)
  Deploy: KAFKA_ENABLED=true, REDIS_HOST=localhost:6379
```

**CONCLUSIÓN:**
- `EnvironmentFile=${OCM_REPO_ROOT}/.env` **existe en template** (HEAD) — drift `TEMPLATE VS INSTALLED` **resuelto** (antes `TEMPLATE≠RENDERED=INSTALLED`, ahora `TEMPLATE=RENDERED=INSTALLED` tras `72bd1878`).
- Drift vigente: **puertos** `KAFKA 9094 (docs) vs 9093 (runtime/compose)` — reproducible, no host-only. No depende de edición manual, es divergencia docs/realidad.
- Re-render `install_systemd.sh` ahora reproducible (no perderá `.env`).

---

## 14. Portabilidad

**Pregunta:** ¿Puede desplegarse en otra Debian sin depender de OrangeHouse?

| Hallazgo | Evidencia | Clasificación |
|---|---|---|
| `User=orangemusic` hardcodeado | `template: User=${OCM_HOST_USER}` → rendered `User=orangemusic` via `host.env`, `systemctl show User=orangemusic` | **PARAMETRIZABLE** (vía `OCM_HOST_USER` + `host.env.example`) |
| `WorkingDirectory=/home/orangemusic/trading/orangecashmachine` | `template: ${OCM_REPO_ROOT}` → rendered absoluto | **PARAMETRIZABLE** (vía `OCM_REPO_ROOT`) |
| `EnvironmentFile` host-specific | `deploy/host.env` untracked, `host.env.example` parametrizado, `install_systemd.sh` `cp host.env.example` | **PARAMETRIZABLE** |
| Secretos en VCS | `deploy/host.env` ya no trackeado (C1), `.env` untracked, solo `.env.example` | **PORTABLE** (OK) |
| IP LAN `OLLAMA_HOST=http://192.168.100.2:11434` | `.env` SET con IP privada OrangeHouse | **HOST-SPECIFIC** |
| `host.env` OCM_ENV=development | Producción requiere `OCM_ENV=production` manual | **HOST-SPECIFIC** (documentado) |
| `KAFKA_HOST_PORT=9093` hardcoded en `.env` | No parametrizado via `host.env`, solo `.env` | **HOST-SPECIFIC** (pero portable si se respeta `.env.example` 9094) |
| Dependencias manuales | `install_systemd.sh` requiere `cp host.env.example`, `systemd-analyze verify`, `docker.service` | **PARAMETRIZABLE** (documentado) |
| Paths absolutos `/home/...` en rendered | Generados, no en VCS template (template usa `${OCM_REPO_ROOT}`) | **PORTABLE** (VCS no hardcodea) |
| Nombres host `orangehouse` en logs | `systemctl status` Docs path, no en servicio | **PORTABLE** |

**CONCLUSIÓN:** **PARCIALMENTE PORTABLE** — VCS templates parametrizados OK; runtime depende de `host.env`+`.env` por host (esperado). Solo `OLLAMA_HOST` LAN y puerto Kafka son host-specific no parametrizados vía `host.env`.

---

## 15. Evidence Timeline

| Timestamp | Evento | Fuente | Evidencia |
|---|---|---|---|
| 2026-09-19 15:57:31 -05 | T0 streaming iniciado | `systemctl show` | `ExecMainStartTimestamp` |
| 2026-09-19 15:57:36 | T3 Kafka producers (4) | `systemctl status` | `orderbook/funding/oi/liquidations_producer_started` |
| 2026-09-19 15:57:37 | T3 orderbook WS bybit | `systemctl status` | `orderbook_stream_starting [BTC-USDT-PERP...] gateway=localhost:9091` |
| 2026-09-19 19:08 | T3 L3 offset 9938725 | `kafka-get-offsets` | `orderbook.raw:0:9938725 sum=9939167` |
| 2026-09-19 19:09 | T3 L3 offset 9943250 | `kafka-get-offsets` | `+4525` en ~60s |
| 2026-09-19 19:42:00 | T1 fallo pipeline market-data | `docker logs ocm_market_data` | `PipelineBuildError PipelineRequest bybit/spot dry_run=True` |
| 2026-09-19 20:05 | VALIDATE_ONLY production | `uv run` | `ConfigPipelineError VALIDATED bybit+kucoin credentials missing` |
| 2026-09-19 20:05 | M1–M20 | `audit_validator` | `PASS 0 findings 25 reglas` |
| 2026-09-19 20:09 | T5 health 503 / ready 200 | `curl` | `/health unhealthy consecutive_errors:10`, `/ready ready last_run_s:19873` |

---

## 16. Findings

### FINDING-RUNTIME-01 — Market Data unhealthy (container) por credenciales vacías
- **Severidad:** P1
- **Categoría:** RUNTIME / CONFIG
- **Estado:** CONFIRMADO
- **Evidencia:** `docker ps ocm_market_data (unhealthy)`, `docker logs PipelineBuildError apiKey:''`, `curl /health 503 unhealthy`, `KAFKA_ENABLED` OK pero pipeline no construye
- **Impacto:** L4 Bronze sin datos, market-data no operativo aunque streaming sí
- **Recomendación:** Propagar `BYBIT_API_KEY/SECRET` de `.env` host al env del container (`docker-compose.yml` `environment:`) o montar `.env` en container
- **¿Requiere cambio?:** Sí (config)

### FINDING-RUNTIME-02 — Prometheus/Grafana no levantados
- **Severidad:** P2
- **Categoría:** INFRA
- **Estado:** CONFIRMADO
- **Evidencia:** `curl :9090` fail, `docker ps` sin `ocm_prometheus/pushgateway/grafana` (solo kafka/redis/zookeeper/market_data)
- **Impacto:** Sin métricas ni dashboards, no afecta `ocm-streaming` (type simple) pero incumple `config/observability` esperado
- **Recomendación:** `docker compose --profile monitoring up -d` o documentar infra mínima
- **¿Requiere cambio?:** No (operativo) — infra opcional

### FINDING-DRIFT-01 — Puerto Kafka docs vs runtime (9094 vs 9093)
- **Severidad:** P2
- **Categoría:** DRIFT
- **Estado:** CONFIRMADO
- **Evidencia:** `.env.example:40 KAFKA_HOST_PORT=9094` vs `.env real 9093` vs `docker ps 0.0.0.0:9093` vs `host.env KAFKA_BOOTSTRAP_SERVERS=localhost:9093`
- **Impacto:** Portabilidad/docs confusa; nuevo clon con `cp .env.example` no coincide con runtime actual
- **Recomendación:** Alinear `.env.example` a `9093` o documentar `KAFKA_HOST_PORT` como override y decidir canónico
- **¿Requiere cambio?:** Sí (docs)

### FINDING-DRIFT-02 — Template drift resuelto (EnvironmentFile .env)
- **Severidad:** P3 (histórico)
- **Categoría:** DRIFT RESUELTO
- **Estado:** RESUELTO en HEAD `72bd1878`
- **Evidencia:** `git show HEAD~1` 1 EnvironmentFile vs `HEAD` 2 EnvironmentFiles, `rendered==installed` byte-idéntico
- **Impacto:** Ninguno actual; re-render ya reproducible
- **Recomendación:** Ninguna — auditar que `install_systemd.sh --verify-only` valide 2 EnvironmentFiles
- **¿Requiere cambio?:** No

### FINDING-B59-01 — B-59 sin prueba de reinicio
- **Severidad:** P1 (criterio B-59)
- **Categoría:** GOBERNANZA / EVIDENCIA
- **Estado:** NO VERIFICADO
- **Evidencia:** `tracking.yaml:3146 PENDIENTE`, `systemctl status active` sí, `journalctl` sin SIGTERM, `RESTART REQUERIDO — NO EJECUTADO`
- **Impacto:** No se puede cerrar B-59 como HECHO
- **Recomendación:** Ejecutar `systemctl restart ocm-streaming.service` fuera de READ-ONLY y documentar `journalctl` + offsets + health
- **¿Requiere cambio?:** No (prueba operativa, requiere autorización)

### FINDING-OBS-01 — Audit validator WARNs gobernanza
- **Severidad:** P3
- **Categoría:** GOBERNANZA
- **Estado:** WARN (no FAIL)
- **Evidencia:** `M17 F-PL-01..11 no en registro`, `M25 ADR-0020`, `M20 2 FAIL sin finding NUEVO`
- **Impacto:** No bloquea, pero indica deuda docs
- **Recomendación:** Deduplicar F-PL-* o excluir en validator
- **¿Requiere cambio?:** No (docs)

---

## 17. Verification Matrix

| Control | Código | Docs | Host | Runtime | E2E | Estado |
|---|---|---|---|---|---|---|
| systemd streaming unit | ✅ `app/cli/streaming_hydra.py`, `ADR-0022` | ✅ `deploy/systemd/templates/*.template` | ✅ `/etc/systemd/system/ocm-streaming.service` | ✅ `active running enabled` | ⚠️ sin restart | **ACTIVO** |
| Market Data pipeline | ✅ `packages/market_data/main.py` | ✅ `config/env/*.yaml` | ✅ `ocm_market_data` container | ❌ `unhealthy` 503 | ❌ no L4 | **FALLA** |
| Kafka | ✅ `docker-compose.yml` | ✅ `deploy/monitoring/*` | ✅ `ocm_kafka healthy` | ✅ `broker api` | ✅ `orderbook.raw +4525` | **E2E OK (L3)** |
| Prometheus | ✅ `deploy/monitoring/prometheus.yml` | ✅ `config/observability` | ❌ no container | ❌ `curl fail` | ❌ | **NO INSTALADO** |
| Redis | ✅ `ocm/runtime/state` | ✅ `docker-compose.yml` | ✅ `ocm_redis healthy PONG` | ✅ `ping` | ⚠️ market-data no conecta creds | **ACTIVO** |
| L3 freshness | ✅ `shared/kafka/topics.py` | ✅ `docs` | ✅ `orderbook.raw:0:9943250` | ✅ `+4525/min` | ⚠️ sin timestamp | **ACTIVIDAD OK** |
| L4 Bronze | ✅ `packages/market_data/infrastructure` | ✅ `docs` | ✅ dirs existen | ❌ vacío, 0 parquet | ❌ | **NO VERIFICADO** |
| .env secrets | ⚠️ `ocm/config/env_vars.py` SSOT | ✅ `.env.example` | ✅ `.env SET` | ✅ `BYBIT SET` | ❌ container no ve | **PARCIAL** |
| Production fail-fast | ✅ `ocm/config/schema.py` validación | ✅ `ADR` | ✅ `.env` | ✅ `VALIDATED fail` | ✅ sin side effects | **E2E OK** |
| M1–M20 | ✅ `scripts/audit_validator.py` | ✅ `docs/governance` | ✅ local PASS | ✅ CI `policy-gate` | ✅ | **PASS** |
| B-59 | ✅ `ADR-0022` | ⚠️ `tracking PENDIENTE` | ✅ `systemctl active` | ✅ `L3` | ❌ `restart+L4` | **PENDIENTE** |
| Drift VCS | ✅ `72bd1878` fix | ✅ `audit 2026-09-19` | ✅ `rendered==installed` | ✅ `systemctl show` | ✅ | **RESUELTO** |

---

## 18. B-59 Final Determination

| Dimensión | Estado | Evidencia |
|---|---|---|
| **Implementación** | **HECHO** | Código `app/cli/streaming_hydra.py`, `ADR-0022`, template systemd commit `72bd1878` |
| **Instalación** | **HECHO** | `FragmentPath=/etc/systemd/system/ocm-streaming.service`, `EnvironmentFiles` host.env+.env, `enabled` |
| **Runtime** | **HECHO** | `ActiveState=active SubState=running MainPID=69498` 3h, 4 producers, `streaming_started gateway=localhost:9091` |
| **L3** | **HECHO** | `orderbook.raw` existe, offset 9.94M, +4525/min, `kafka-broker-api-versions` OK |
| **L4** | **NO VERIFICADO** | Bronze vacío, `/health` 503 `PipelineBuildError`, sin parquet <60m |
| **End-to-End** | **NO VERIFICADO** | Falta `restart` + journal SIGTERM→`streaming_closed` + L3 post-restart + L4 |
| **Evidencia faltante** | — | `journalctl -u ocm-streaming.service` con privilegios, `systemctl restart` documentado, `find data_platform -name *.parquet -mmin -5` con datos, `kafka-console-consumer` con payload decodificado |

**Determinación final:** `tracking.yaml:3146` **PENDIENTE** es correcto. **No recomendar HECHO**. Estado real: **PARCIAL** (4/6 dimensiones HECHO). Para cierre se requiere prueba operativa con autorización explícita (ver §19.D).

---

## 19. Recommended Next Actions

### A. Solo documentación
- Actualizar `docs/audits/2026-09-19` con nuevo estado `B-59 PENDIENTE` y drift resuelto `72bd1878`
- Documentar `KAFKA_HOST_PORT` canónico (9093 vs 9094) en `docs/architecture/decisions/ADR-0022` o `deploy/host.env.example` header
- Añadir nota `ADR-0022:323` obsoleto ("Sin unit activo" ya falso)
- Registrar en informe que `OLLAMA_HOST=192.168.100.2` es host-specific

### B. Corrección de configuración (sin código)
- Alinear `.env.example KAFKA_HOST_PORT` a `9093` (runtime real) o declarar `9094` y migrar `docker ps` + `host.env`
- Propagar `BYBIT_API_KEY/SECRET` al container `ocm_market_data` (compose `env_file: [.env]` o `environment:`) — corrige `FINDING-RUNTIME-01`
- Levantar monitoring `docker compose --profile monitoring up -d` para Prometheus/pushgateway

### C. Corrección de código
- Ninguna requerida para B-59 (código ya HECHO). Evaluar `FINDING-OBS-01` deduplicación `F-PL-*`

### D. Prueba operativa adicional (requiere autorización, fuera de READ-ONLY)
- `systemctl restart ocm-streaming.service` + `journalctl -u ocm-streaming.service -o short-iso --since "5 min ago"` para capturar SIGTERM→`streaming_closed`→`active`
- `timeout 20 docker exec ocm_kafka kafka-get-offsets --topic orderbook.raw --time -1` antes/después de restart para freshness post-reinicio
- `curl http://localhost:8011/health` + `find data_platform -name *.parquet -mmin -5` post-fix credenciales para verificar L4

### E. Requiere autorización explícita
- Cualquier `systemctl restart/stop/start`, `docker restart`, `docker exec` con produce, `git commit/push`, modificación `tracking.yaml` a `HECHO`, modificación `ADR`, `install_systemd.sh --apply`

---

## 20. Limitaciones

- `journalctl -u ocm-streaming.service` sin grupo `adm`/`systemd-journal` → logs systemd vacíos; no se correlacionó `T0` con `journalctl` (uso alternativo `systemctl status` + `systemctl show` + `docker logs`)
- `RESTART REQUERIDO — NO EJECUTADO POR RESTRICCIÓN READ-ONLY` → B-59 `reinicia_correctamente` imposible de verificar end-to-end
- `kafka-console-consumer --from-beginning` binario → `TimeoutException 0 messages` sin decodificar `event_id/occurred_at`; freshness temporal no probada
- `curl http://localhost:9090` sin prometheus container → no se evaluó scrap de métricas OCM
- `logs/app_2026-09-19.log` 389M no inspeccionado por tamaño (timeout); se usó `docker logs` y `systemctl status` como proxy
- `.env` presencia `SET/EMPTY` no valida vigencia de claves ni propagación a containers (evidencia: container bybit `apiKey:''` contradice host `BYBIT=SET`)
- `audit_validator` local solo `--versions`; `M1..M20` completo es CI `ocm-ci.yml:policy-gate`
- No se ejecutó `find/read` exhaustivo de todos los PDFs de `docs/knowledge` (respeta `manifest.yaml` + `status`)

---

## 21. Final Read-Only Git State

```
git status --short (post-auditoría, solo informe añadido):
 M .github/workflows/ocm-cd.yml
 M Dockerfile
 M deploy/monitoring/grafana/dashboards/ocm_pipeline.json
 M deploy/monitoring/grafana/provisioning/datasources.yml
 M deploy/scripts/deploy_ocm.sh
 M packages/market_data/infrastructure/quality/anomaly_registry.py
 M policies/semgrep/architecture.yaml
 M uv.lock
?? docs/audit/
?? docs/audits/AUDIT_OCM_b59-runtime-operational-audit_2026-09-19.md   ← ÚNICO archivo nuevo de esta auditoría
?? docs/audits/AUDIT_OCM_deployment-boundary-portability_2026-09-19.md (previo)
?? docs/audits/AUDIT_OCM_deployment-runtime-architecture_2026-09-19.md (previo)

git branch --show-current: feat/b56-ci-stage-ordering
git log -1 --oneline: 04e9b6fd chore(deploy): C1 untrack deploy/host.env
```

**Confirmación:** Solo se creó `docs/audits/AUDIT_OCM_b59-runtime-operational-audit_2026-09-19.md`. Ningún otro archivo modificado, ningún `git add` fuera del informe, ningún `commit/push/merge/rebase/reset/restore/clean`, ningún `restart` de servicios/containers, ningún produce/consume con commit de offsets. Estado persistente inalterado.

---

*Fin de auditoría READ-ONLY. Próximo paso requiere autorización explícita para §19.D.*
