# OCM Deployment & Runtime Architecture Audit

- **Fecha:** 2026-09-19
- **Repositorio:** `OrangeCashDigital/orangecashmachine` (local `/home/orangemusic/trading/orangecashmachine`)
- **Branch / HEAD:** `feat/b56-ci-stage-ordering` / `b8b13240` ("fix(gates): G10 no debe bloquear por WARN de M17/M20")
- **Modo:** READ-ONLY estricto (protocolo `docs/governance/AUDIT_PROTOCOL.md`). Escritura exclusivamente en `docs/audits/`.
- **Comandos mecánicos ejecutados:** `uv run python scripts/audit_validator.py --versions` (exit 0). El gate M1..M20 completo es job de CI (`policy-gate`, `ocm-ci.yml:478`).

---

## 1. Executive Summary

**Pregunta guía:** ¿OrangeCashMachine corre "bare-metal" sobre Debian con infraestructura desacoplada en Docker por contratos de endpoints, y qué modelo de deployment/runtime tiene realmente?

**Respuesta (HECHO verificable):** hoy conviven **dos modelos de ejecución**:

| Modelo | Mecanismo | Estado |
|---|---|---|
| **A — Runtime nativo bare-metal** | systemd units que ejecutan `~/.venv/bin/python -m market_data.main` y `-m app.cli.streaming_hydra` sobre el repo del host | **ACTIVO / HEALTHY** (market-data-runtime 08-20; unidades presentes en `deploy/systemd/rendered/`) |
| **B — Infraestructura contenedorizada** | `docker compose up` (default) levanta Kafka+ZK, Redis, Pushgateway, Prometheus, Alertmanager, Grafana, Loki, Promtail | **Diseño canónico**; OCM respira vía `localhost:9093` (Kafka EXTERNAL) y `localhost:6379` (Redis) |
| **C — CD contenedorizado (scaffolding)** | `deploy_ocm.sh` + `ocm-cd.yml` despliegan la imagen `ocm:<SHA>` con `docker compose up -d --build <SHA>` | **INCOHERENTE/NO EJECUTABLE** contra el compose actual (ver F-01/F-02) |

**Conclusión central:** el diseño intencional es **A+B**: procesos OCM nativos en host (systemd, venv, SafeOps) + infra desacoplada en Docker accesible por endpoints `localhost:<puerto>`. El mecanismo CD (C) quedó como esqueleto que contradice el modelo nativo y, además, no funcionaría tal como está escrito. La hipótesis de `REDIS_URL` **no es correcta**: el código y la config usan variables separadas `REDIS_HOST/REDIS_PORT/REDIS_DB/REDIS_PASSWORD` (no existe `REDIS_URL` en `.env`, `.env.example`, `host.env`, YAMLs ni código).

Severidades encontradas: **0 CRITICAL · 2 HIGH · 8 MEDIUM · 7 LOW/INFO**.

---

## 2. Alcance, método y cumplimiento del protocolo

- Protocolo: `AUDIT_PROTOCOL.md` v2.1 (checks mecánicos M1..M20 antes del juicio del LLM; jerarquía normativa `PLAN-Maestro-Ingenieria.md` → `GOVERNANCE.md` → `tracking.yaml` → ADRs).
- Orden de descubrimiento respetado: Plan Maestro → GOVERNANCE → tracking → ADRs → CI/linters.
- Read-only verificado al inicio y al final: working tree con **exactamente los mismos 8 archivos modificados preexistentes** (`.github/workflows/ocm-cd.yml`, `Dockerfile`, `deploy/monitoring/grafana/dashboards/ocm_pipeline.json`, `deploy/monitoring/grafana/provisioning/datasources.yml`, `deploy/scripts/deploy_ocm.sh`, `packages/market_data/infrastructure/quality/anomaly_registry.py`, `policies/semgrep/architecture.yaml`, `uv.lock`). Sin cambios añadidos.
- Archivos inspeccionados (no exhaustivo): `deploy/scripts/{deploy_ocm,install_systemd,health_check}.sh`, templates systemd y unidades renderizadas, `deploy/host.env(.example)`, `.env(.example)`, `docker-compose.yml`, `.github/workflows/{ocm-ci,ocm-cd}.yml`, `config/{base,config,settings}.yaml`, `config/env/*.yaml`, `config/{storage,observability}/**`, `config/market_data/feeds.yaml`, `ocm/config/{env_vars,schema}.py`, `ocm/config/{hydra_loader,layers/rules}.py`, `ocm/runtime/{run_config,environment_validator}.py`, `ocm/runtime/state/cursor_store.py`, `ocm/observability/*.py`, `packages/market_data/main.py`, `market_data/infrastructure/kafka/{producer,consumer}.py`, `market_data/infrastructure/bootstrap/composition_root.py`, `apps/app/cli/{main,streaming_hydra,_bootstrap}.py`, `apps/api/{main,settings}.py`, `apps/api/routers/health.py`, `scripts/provision_kafka_topics.py`, `shared/kafka/topics.py`, `packages/portfolio/infra/redis_factory.py`, `pyproject.toml`, `Dockerfile`, `run.sh`, `deploy/monitoring/{prometheus,promtail/promtail,alertmanager.yml}`, `deploy/monitoring/grafana/provisioning/datasources.yml`.

---

## 3. Modelo de ejecución actual — veredicto

**Runtime (modelo A) — IMPLEMENTADO y operativo:**
- `ocm-market-data.service` → `ExecStart=<repo>/.venv/bin/python -m market_data.main` (microservicio :8001 + ingestion loop + Kappa bronze writer).
- `ocm-streaming.service` → `ExecStart=<repo>/.venv/bin/python -m app.cli.streaming_hydra` (canary orderbook WebSocket).
- Unidades con SafeOps (`ProtectSystem=strict`, `ReadWritePaths=data_platform logs`, `PrivateTmp`, `NoNewPrivileges`), `Restart=on-failure`, `RestartSec=10`, `KillSignal=SIGINT`, `After=network-online.target docker.service` ⇒ **dependencia explícita de encendido sobre Docker** (la infra vive en contenedores).

**Infra (modelo B) — IMPLEMENTADO y operativo:** Kafka + paper no; ver §5.

**CD (modelo C) — DOCUMENTADO PERO NO IMPLEMENTADO / OBSOLETO vs composición real:** ver F-01/F-02.

---

## 4. Diagrama arquitectónico actual

```
┌────────────────────────────── HOST (Debian, user=orangemusic) ──────────────────────────────┐
│                                                                                              │
│  systemd (system: streaming · user: market-data)                                             │
│   ┌───────────────────────────────┐        ┌──────────────────────────────────────────────┐ │
│   │ .venv/bin/python              │        │ .venv/bin/python                             │ │
│   │ -m app.cli.streaming_hydra    │        │ -m market_data.main  (FastAPI :8001)         │ │
│   │  canary ORDERBOOK (cryptofeed)│        │  /health  /ready  /ohlcv/{ex}/{sym}/{tf}     │ │
│   │  OrderBookKafkaProducer       │        │  ingestion_loop (REST ccxt)                  │ │
│   │  push → Pushgateway (si mét.) │        │  bronze_writer_loop (ohlcv.raw → Bronze)     │ │
│   └───────────┬───────────────────┘        │  feed_orchestrator (rest ⇒ NO corre)         │ │
│               │                            └───────┬──────────────────────────────────────┘ │
│   EnvironmentFile: deploy/host.env + .env          │                                        │
│   CONTRATOS: KAFKA_BOOTSTRAP_SERVERS=localhost:9093 · REDIS_HOST=localhost:6379             │
└───────────────┼────────────────────────────────────┼────────────────────────────────────────┘
                │ localhost:9093 (EXTERNAL)          │ localhost:6379 (REDIS requirepass)
                ▼                                    ▼
┌─────────────────────── Docker Compose (infra-only por defecto) ─────────────────────────────┐
│  ocm_kafka  cp-kafka 7.6.1 + cp-zookeeper 7.6.1   ocm_redis  redis:7.2-alpine (requirepass) │
│  INTERNAL kafka:9092  ·  EXTERNAL localhost:9093   pushgateway:9091 · prometheus:9090       │
│  grafana 127.0.0.1:3000 · alertmanager:9094* · loki:3100 · promtail (scrape ./logs host)    │
│  config-guard (init: valida configs antes de arrancar)                                      │
│  PERFIL [microservices]: market-data/trading/portfolio (NO en default)                      │
│  PERFIL [monitoring]: kafka-ui 127.0.0.1:8080                                               │
└───────────────────────────────────────────────────────────────────────────────────────────────┘
* .env real: ALERTMANAGER_HOST_PORT=9094 (Kafka conserva 9093). Ver F-10.
```

---

## 5. Infraestructura (Docker Compose)

`docker-compose.yml` — infra-por-defecto, microservicios opt-in (`profiles: [microservices]`).

- **Redes:** `ocm_internal` (bridge internal) + `ocm_public`. Redis en ambas ⇒ alcanzable desde host (`localhost:6379`) y desde contenedores (`redis:6379`). Kafka solo en `ocm_public`+`ocm_internal` con dos listeners.
- **Kafka (7.6.1, ZK-mode):**
  - `KAFKA_LISTENERS: INTERNAL://0.0.0.0:9092, EXTERNAL://0.0.0.0:9093`
  - `KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:9092, EXTERNAL://localhost:${KAFKA_HOST_PORT:-9093}`
  - `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"` ⇒ topics creados explícitamente por `scripts/provision_kafka_topics.py` (idempotente, SSOT `shared/kafka/topics.py::ALL_TOPICS`, default `--bootstrap-server localhost:9093`).
  - Retención 7d, RF=1, gzip, healthcheck `kafka-broker-api-versions`.
- **Redis (7.2-alpine):** `--requirepass ${REDIS_PASSWORD:?}`, `--maxmemory`, AOF, healthcheck con `redis-cli -a`.
- **Dependencias con condición:** `prometheus→alertmanager(sano)+pushgateway(sano)`, `grafana→prometheus+alertmanager`, `loki/promtail`, `kafka-ui→kafka`. **config-guard** (init container busybox) valida que `prometheus.yml`, `alertmanager.yml`, `alerts.yml`, `loki.yml` existan y no estén vacíos antes de arrancar la pila.
- **Exposición segura:** Grafana `127.0.0.1:${GRAFANA_HOST_PORT:-3000}`, Kafka-UI `127.0.0.1:8080` (solo profile monitoring). Resto en `ocm_public` para el runtime host.
- **Microservicios (scaffolding):** `market-data` (construye `Dockerfile` → image `ocm_market_data:latest`, CMD `python -m market_data.main`), `trading`/`portfolio` marcados "NO EJECUTABLE" (no existe `trading.main`/`portfolio.main`, ADR-0024 NIVEL 2→1).

**Hallazgo estructural:** el comentario `docker-compose.yml:24-25` confirma el diseño: "market-data: perfil [microservices] — **corre en host por defecto**". El modelo A+B es intencional, no accidental.

---

## 6. Runtime nativo (systemd)

- Templates (`deploy/systemd/templates/*.template`): render vía `envsubst` con lista explícita de variables `deploy/host.env` (`install_systemd.sh:47`). Flags `--verify-only` (systemd-analyze verify) y `--user`.
- **Drift confirmado en `deploy/systemd/rendered/` (gitignored):**
  - Los rendered tienen **paths hardcoded** `/home/orangemusic/trading/orangecashmachine` (no `${OCM_REPO_ROOT}`).
  - Los rendered **añaden una línea `EnvironmentFile=.../.env`** que el template no tiene (templates: solo `EnvironmentFile=${OCM_REPO_ROOT}/deploy/host.env`). El `.env` no está trackeado ni existe en un sistema recién clonado ⇒ un re-render con `install_systemd.sh` **borraría esa línea** y el servicio perdería `REDIS_PASSWORD` (Redis exige requirepass).
  - `--user` se aplica vía `sed -i` sobre el rendered (idempotente, OK).
- `deploy/host.env` está **trackeado en git** pese a que su header dice "NUNCA commitear ... contiene topología local" y `.gitignore` lo excluye (se añadió antes del ignore). Hoy su contenido es idéntico a `host.env.example` (sin secretos), pero la política declarada es otra (F-04).

---

## 7. Kafka — contratos y provisioning

| Punto de acceso | Endpoint | Quién |
|---|---|---|
| Dentro de Docker | `kafka:9092` (INTERNAL) | microservicios, kafka-ui, prometheus (via pushgateway) |
| Desde el host | `localhost:9093` (EXTERNAL, configurable `KAFKA_HOST_PORT`) | runtime systemd, `provision_kafka_topics.py`, tests |

- **Default de código host (foot-gun):** varios constructores usan `"kafka:9092"` (DNS de Docker) como default en lugar del endpoint del host: `ocm/config/schema.py:571`, `consumer.py:64`, `producer.py:46`, `composition_root.py:330`. Un proceso host sin `KAFKA_BOOTSTRAP_SERVERS` fallaría DNS. `base.yaml:46` usa el default correcto `localhost:9092`. (F-09)
- `config/env/production.yaml` mantiene `kafka:9092` como default (pensado para el perfil microservicios). `host.env` protege el runtime host con `localhost:9093`.
- Provisioning: `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` + script idempotente desde SSOT ⇒ **buena práctica ya vigente** (fail-fast ante typos de topic).

---

## 8. Redis — contratos

- **Contrato real (NO `REDIS_URL`):** `REDIS_HOST/REDIS_PORT/REDIS_DB/REDIS_PASSWORD` + `REDIS_ENABLED`.
- Consumido por: `base.yaml:37-40` (Hydra `integrations.redis`), `ocm/runtime/state/cursor_store.py` (`os.getenv(REDIS_HOST/PORT/DB/PASSWORD/CURSOR_TTL_DAYS)`), `packages/portfolio/infra/redis_factory.py` (defaults `localhost:6379 db=1`, doc adjudica el password por `ocm_redis` con requirepass), `apps/api/settings.py` (aliases `REDIS_*` → compone `redis_url` interno del gateway).
- **Gap SSOT:** `ocm/config/env_vars.py` (SSOT canónico de nombres de env var con guard de import-time) **no incluye ninguna `REDIS_*`**. Los consumidores escriben los strings a mano. (F-14)
- Redundancia documentada: `.env` setea tanto `KAFKA_BOOTSTRAP_SERVERS` como `OCM_INTEGRATIONS__KAFKA__BOOTSTRAP_SERVERS` (ambos `localhost:9093`) — coexistencia del protocolo plano y del estructurado L2.

---

## 9. Configuración (Hydra) y validación

- Capas Hydra: `config.yaml` (base + módulos + `env: development`) → `settings.yaml` (cascada env, 4º nivel) → `env/{development,production,test}.yaml` → CLI → env vars (L2 estructurado `OCM_SECCIÓN__CLAVE` + planas conocidas).
- `RunConfig.from_env()` (inmutable, fail-fast): resuelve `OCM_ENV`, `OCM_DEBUG`, `OCM_VALIDATE_ONLY`, `PUSHGATEWAY_URL` (default `localhost:9091`), `run_id`.
- Validación en runtime: `EnvironmentValidator` (local, sin red): exchanges activos; prod exige credenciales; `test_symbol` genérico = warning; **prod prohíbe storage `data_platform/data_lake`** (lanza `EnvironmentMismatchError`).
- Regla L5 (`config/layers/rules.py`): `PRODUCTION_REQUIRES_KAFKA_PUBLISHER` — production no arranca con `kafka.enabled=false`.
- **Observación:** las units systemd corren `OCM_ENV=development` (host.env). En un host "de producción" esto desactiva las protecciones específicas de prod (met icas, L5, storage path) salvo override explícito (F-11).

---

## 10. Health / Liveness / Readiness — matriz real

| Proceso | Endpoint | Semántica | Verifica deps? |
|---|---|---|---|
| `market-data :8001` `/health` | `market_data/main.py:465-492` | liveness + ExecutionGuard (503 si guard disparado) | ❌ (solo proceso) |
| `market-data :8001` `/ready` | `main.py:495-519` | readiness: context cargado + primer run iniciado | ❌ **context-only** |
| `api :8000` `/health` | `apps/api/routers/health.py:41-57` | liveness pura (siempre 200) | ❌ |
| `api :8000` `/ready` | `health.py:60-84` | readiness: `redis.ping()` | ✅ Redis |
| Docker healthcheck market-data | `wget :8001/health` | usa liveness del servicio | ❌ |
| `health_check.sh` (L1..L4) | `deploy/scripts/health_check.sh` | L1 process (systemctl) · L2 deps (`docker exec ocm_kafka/ocm_redis`) · L3 frescura `orderbook.raw`/`ohlcv.raw` · L4 frescura Bronze | ✅ completo |

**Gap (F-07):** el readiness del microservicio no incluye Kafka/Redis; la verificación real de dependencias vive solo en `health_check.sh`, acoplado a `docker exec` con `container_name` fijos (`ocm_kafka`, `ocm_redis`) y a un password default `ocm_local_dev` (fail abierto si difiere de `REDIS_PASSWORD` real).

---

## 11. Startup order y lifecycle (ADR-0022)

- `app.cli.main` (`hydra_main`): bootstrap_logging → resolve_explicit_env → `RunConfig.from_env` → `load_appconfig_from_hydra` → `run_application` (configure_logging → setup_observability → `EnvironmentValidator.check` → `validate_only` exit → pipeline_runner). SIGTERM → KeyboardInterrupt → asyncio graceful (KillSignal=SIGINT en units).
- `market_data/main.py` lifespan: build_context → bootstrap_logging → configure_logging → ExecutionGuard → IcebergStorageFactory → 3 background tasks (`ingestion_loop`, `bronze_writer_loop` Kappa, `feed_orchestrator` que **no arranca** con `ingestion_mode: rest`). Shutdown: guard.trigger → cancel+wait_for(10s) — FIX C-05 (sin shield).
- `streaming_hydra.py`: standalone (sin decorator Hydra, BC-51); exige `run_id` en prod (`hydra_loader.py:263` escribe snapshot de auditoría); SIGINT/SIGTERM → stop event → `stream.stop()` → `bundle.close_all()`.
- **F-15 (INFO):** no existen units systemd para `apps/api`, `live_hydra` ni `paper_hydra`; solo market-data y streaming.

---

## 12. Observabilidad

- **Prometheus** targets: `localhost:9090` (self) y `pushgateway:9091` (workers sin HTTP). Alertmanager `alertmanager:9093` (DNS interno).
- **Pushgateway:** `PUSHGATEWAY_URL` default `localhost:9091` (`run_config.py:138`). `PrometheusPusher`/`NoopPusher` fail-safe.
- **F-08 (MEDIUM):** `observability.metrics.enabled=false` por default (`metrics.yaml:12`); solo `production.yaml:24-25` lo activa. El runtime actual corre `OCM_ENV=development` ⇒ el canary usa `NoopPusher` (`streaming_hydra.py:148-151`) ⇒ el job `ocm_pipeline_orderbook` de Pushgateway **no recibe datos** en la instalación actual. Las métricas de contador de Kafka (KafkaMetrics) son independientes, pero el push heartbeat está deshabilitado.
- **Loki/Promtail:** promtail monta `./logs` del host (ro) + `/var/run/docker.sock`; labels `host: orangehouse`, `env: development` **hardcodeados** en `promtail.yml` trackeado (F-13). Loki sink: `loki_url` desde `observability.logging` (None por default).
- **Alertmanager:** receiver `default` deliberadamente sin destinos (F-016, bloqueado durablemente); resolución interna vía Grafana/Loki.
- **Grafana:** datasource Prometheus + Alertmanager vía provisioning; dashboard `ocm_pipeline.json`.

---

## 13. CI (`ocm-ci.yml`)

Orden (fail-fast, `needs`): `architecture` (import-linter ≥49 contratos «kept», baseline F2.1) → `engineering-health` (F2.0) → en paralelo `app-guard`, `domain-guard`, `trading-guards`, `unit-tests` (git-bash), `security` (bandit), `config-validation` (`OCM_VALIDATE_ONLY=true uv run python -m app.cli.main`), `quality` (ruff + format + `mypy .` + SSOT + pip-audit con risk-accepts documentados + vulture non-blocking), `semgrep` (non-blocking, `--baseline origin/main`) + gates `--mode gate-ci`, `policy-gate-evidence` (SHA256 de guards/workflow contra `policies/evidence.json`), `integration-tests` (Kafka service container **KRaft** 7.6.0, `localhost:9093`), `policy-gate` (eng. health + `audit_validator` M1..M25).
- **F-12 (INFO):** CI usa Kafka **KRaft** (sin ZooKeeper) mientras `docker-compose.yml` usa **ZK-mode 7.6.1**. Dos topologías divergentes en el ecosistema (aceptable, pero documentar cuál es la canónica).

---

## 14. CD (`ocm-cd.yml` + `deploy_ocm.sh`)

- `ocm-cd.yml`: `workflow_dispatch` (action `ACCEPT|ROLLBACK`, input `artifact_sha` obligatorio); ejecuta `deploy/scripts/deploy_ocm.sh`.
- `deploy_ocm.sh`: `ARTIFACT_SHA:?` → `ARTIFACT_IMAGE="ocm:${ARTIFACT_SHA}"` → backup `.env`/compose/`.prev_sha` → `docker compose -f docker-compose.yml up -d --build "${ARTIFACT_SHA}"` → `wait_for_health` (docker exec Redis/Kafka) → post-deploy `health_check.sh` → ACCEPT/ROLLBACK → evidencia JSON.

**Incoherencias (F-01/F-02):**
1. **Selector de servicio inválido:** `docker compose up -d --build "${ARTIFACT_SHA}"` pasa el SHA como **nombre de servicio**; no existe ningún servicio con ese nombre en `docker-compose.yml` (infra) ni la imagen `ocm:<SHA>` se usa en compose (el microservicio es `ocm_market_data:latest`). El comando falla en el primer deploy. `deploy_ocm.sh:161`.
2. **¿Dónde queda la evidencia?** el script escribe `deploy/deploy_evidence_<SHA>.json` (`deploy_ocm.sh:243`) pero el workflow la busca en `${{ github.workspace }}/deploy_evidence_<SHA>.json` (raíz) (`ocm-cd.yml:66`) ⇒ status siempre ROLLBACK por "no evidence".
3. **Puertos de health muertos:** header (`deploy_ocm.sh:11-12`) y defaults `PROMETHEUS_PORT=9091`, `ALERTMANAGER_PORT=9090`, `PUSHGATEWAY_PORT=9093` contradicen compose (9090/9093/9091). `wait_for_health` no los usa (solo docker exec) ⇒ config muerta engañosa (F-03).
4. **Divergencia de modelo:** el CD despliega OCM **en Docker**, mientras el runtime activo es **nativo por systemd**; `install_systemd.sh` (el mecanismo coherente con A+B) no está integrado en ningún pipeline.
5. Sin ejecuciones previas verificables (`deploy/.prev_sha` y evidencias ausentes en disco) ⇒ proceso nunca ejercitado.

---

## 15. Entornos, variables y contratos (SSOT)

- SSOT canónico: `ocm/config/env_vars.py` (guard de import-time: constante ↔ `_ENV_VAR_NAMES`). **No cubre `REDIS_*`** (F-14), ni los puertos `*_HOST_PORT` del compose, ni `OCM_INTEGRATIONS__*` (estos últimos son pactados con Hydra `oc.env` sin registro).
- `.env` real (untracked): `OCM_ENV=development`, `KAFKA_ENABLED=true`, `KAFKA_BOOTSTRAP_SERVERS=localhost:9093`, `OCM_INTEGRATIONS__KAFKA__BOOTSTRAP_SERVERS=localhost:9093`, `KAFKA_HOST_PORT=9093`, `ALERTMANAGER_HOST_PORT=9094`, `REDIS_HOST_PORT=6379`, credenciales de exchanges, `OCM_API_JWT_SECRET`.
- `host.env` (tracked, == example): `OCM_ENV=development`, `MARKET_DATA_HOST=0.0.0.0`, `MARKET_DATA_PORT=8001`, `INGESTION_INTERVAL_S=300`, `KAFKA_ENABLED=true`, `KAFKA_BOOTSTRAP_SERVERS=localhost:9093`, `REDIS_ENABLED=true`, `REDIS_HOST=localhost`, `REDIS_PORT=6379`.
- **F-10 (MEDIUM):** `.env.example:36-40` documenta `KAFKA_HOST_PORT=9094` para no colisionar con `ALERTMANAGER_HOST_PORT=9093` (F-029/B-45); la instalación real lo resolvió **al revés** (Kafka 9093, Alertmanager 9094). La doc queda desincronizada.

---

## 16. Seguridad

| Control | Evidencia | Estado |
|---|---|---|
| Dr y default seguro `safety.dry_run: true` | `base.yaml:26` | IMPLEMENTADO |
| Prod exige credenciales + storage prod | `environment_validator.py:115-178` | IMPLEMENTADO |
| Prod exige Kafka publisher (L5) | `config/layers/rules.py` | IMPLEMENTADO |
| Grafana/Kafka-UI solo loopback | `docker-compose.yml:228,592` | IMPLEMENTADO |
| Alertmanager sin rutas de notificación | `alertmanager.yml:19-21` (F-016) | DECISIÓN DOCUMENTADA |
| No secrets en config (`REDIS_PASSWORD` solo env) | compose `?REDIS_PASSWORD` | IMPLEMENTADO |
| `host.env` trackeado vs "NUNCA commitear" | `deploy/host.env` + `.gitignore:74` | CONTRADICTORIO (F-04) |
| Password default en salud `ocm_local_dev` | `health_check.sh:41` | PARCIAL (fail abierto, F-06) |
| Secrets manager | — | AUSENTE (receiver alertas bloqueado hasta existir) |

---

## 17. Comparación de modelos operativos (freqtrade / nautilus_trader / hummingbot)

Fuente: zips de `docs/` (`docker-compose.yml` respectivos) + conocimiento general. Descriptivo, no normativo.

| Dimensión | freqtrade | nautilus_trader | hummingbot | **OCM (actual)** |
|---|---|---|---|---|
| Runtime del bot | Contenedor Docker (imagen oficial) | Librería Python (pip) — proceso nativo | Contenedor interactivo (tty/stdin) | **Proceso nativo en host (systemd + venv)** |
| Infra provisionada | SQLite en volumen (`user_data`) | postgres + pgadmin + redis (compose) | casi nada (host network) | Kafka+ZK · Redis · obs. completa (compose) |
| Compose es el deploy? | Sí (primario) | Solo infra | Sí (primario) | **Infra sí; OCM solo perfil opt-in [microservices]** |
| Config | `config.json` en user_data | programática | `conf/` dirs montadas | Hydra YAML + env vars con SSOT |
| Observabilidad | REST API 127.0.0.1:8080 | — | logs; dicta CLI | `/health`+`/ready`, Pushgateway/Loki/Prometheus/Grafana |
| Lo más parecido a OCM | — | **infra contenedorizada + core nativo** | — | (el diseño A+B sigue la filosofía nautilus) |

Conclusión: OCM es el único con **core nativo + infra contenedorizada + sistema de salud multi-capa (L1-L4) + pipeline de datos (medallion)**. Freqtrade/hummingbot son "bot-in-container"; nautilus es biblioteca + infra dockerizada. El modelo A+B de OCM es defendible y moderno; la inconsistencia está en el CD (C).

---

## 18. Reconciliación con auditorías previas y `tracking.yaml`

| Origen | Decía | Hoy | Clasificación |
|---|---|---|---|
| `AUDIT_OCM_FORENSIC_COMPLIANCE_2026-08-19` | "deploy_ocm.sh no existe"; "systemd DOCUMENTADO (sin units)" | `deploy_ocm.sh` existe desde 2026-08-25 (fixes 08-28) y hay units rendered + runtime ACTIVO | **OBSOLETO** (revalidar en próxima edición) |
| `OCM_AUDIT_FINDINGS_2026-08-20_market-data-runtime` | streaming=sistema, market-data=usuario, ACTIVE/HEALTHY | Coincide con `health_check.sh` L1 y rendered | **REVALIDADO** |
| `streaming-canary-audit 2026-08-08` (F-008/F-016/F-017/F-029/B-45) | canary no genera señales; alertmanager sin rutas; puerto 9094 vs 9093 | Coherente; F-029 quedó documentado pero la instalación lo resolvió invertido (F-10) | **REVALIDADO PARCIAL** |
| `tracking.yaml` B-59 | `systemd_reinicia_correctamente: NO_VERIFICADO` | sigue PENDIENTE; ADR-0022:323 "Sin unit activo" ya es falso | **PENDIENTE / OBSOLETO (ADR)** |
| `tracking.yaml` B-49/53/56/57 | cerrados | CI/CD y gates presentes en CI | **REVALIDADO** |

---

## 19. Tabla de Findings

| ID | Severidad | Área | Finding | Evidencia | Estado |
|---|---|---|---|---|---|
| F-01 | HIGH | CD / Deployment | Split-brain: runtime vivo es **nativo por systemd** pero el CD despliega OCM **en Docker**; además `up -d --build <SHA>` pasa el SHA como servicio inexistente ⇒ no desplegable contra el compose actual | `deploy_ocm.sh:36,161` · `docker-compose.yml:334-375` · `install_systemd.sh` | DOCUMENTADO PERO NO IMPLEMENTADO / INCOHERENTE |
| F-02 | HIGH | CD / Workflow | Ruta de evidencia divergente script vs workflow ⇒ status siempre ROLLBACK | `deploy_ocm.sh:243` (deploy/) vs `ocm-cd.yml:66` (workspace/) | CONTRADICTORIO |
| F-03 | MEDIUM | CD | Puertos health de `deploy_ocm.sh` contradicen compose; config muerta no usada por `wait_for_health` | `deploy_ocm.sh:11-12,49-51` vs `docker-compose.yml:152,197,131` | PARCIALMENTE IMPLEMENTADO |
| F-04 | MEDIUM | Gobernanza/Secrets | `deploy/host.env` trackeado pese a "NUNCA commitear" + `.gitignore`; hoy es plantilla sin secretos, pero la política dice lo contrario | `git ls-files deploy/host.env` · `.gitignore:74` · `host.env:6` | CONTRADICTORIO |
| F-05 | MEDIUM | Systemd | Drift rendered vs template: línea `EnvironmentFile .env` solo en rendered + paths hardcoded; un re-render la pierde (y con ella `REDIS_PASSWORD`) | `deploy/systemd/templates/ocm-market-data.service.template:11` vs `deploy/systemd/rendered/ocm-market-data.service:11-12` | PARCIALMENTE IMPLEMENTADO |
| F-06 | MEDIUM | Health/Seguridad | `health_check.sh` acoplado a `docker exec <container_name>` fijos y password default `ocm_local_dev` (fail abierto) | `health_check.sh:20-21,35-44` | PARCIALMENTE IMPLEMENTADO |
| F-07 | MEDIUM | Health/Readiness | `/health` y `/ready` de market-data :8001 no verifican Kafka/Redis; readiness real solo en script externo Docker-coupled | `market_data/main.py:465-519` + `health_check.sh` | PARCIALMENTE IMPLEMENTADO |
| F-08 | MEDIUM | Observabilidad | `metrics.enabled=false` en runtime actual (OCM_ENV=development) ⇒ canary usa NoopPusher ⇒ Pushgateway sin datos; job `ocm_pipeline_orderbook` en vacío | `metrics.yaml:12` · `production.yaml:24-25` · `streaming_hydra.py:148-151` | IMPLEMENTADO PERO NO ALIMENTADO |
| F-09 | MEDIUM | Contratos/Defaults | Default host `kafka:9092` (DNS Docker) en schema/producer/consumer ⇒ proceso host sin env falla DNS | `schema.py:571` · `consumer.py:64` · `producer.py:46` · `composition_root.py:330` | PARCIAL (foot-gun) |
| F-10 | MEDIUM | Docs/Config | `.env.example` documenta `KAFKA_HOST_PORT=9094` pero la instalación real usa Kafka 9093 + Alertmanager 9094 | `.env.example:32-40` vs `.env` real · `host.env:21` | CONTRADICTORIO |
| F-11 | MEDIUM | Runtime/Entornos | Units corren `OCM_ENV=development` (host.env); en host de producción hay que override explícito para activar protecciones prod (L5, storage, métricas) | `host.env:10` · `environment_validator.py` · `rules.py` | PARCIALMENTE IMPLEMENTADO |
| F-12 | LOW | Kafka/Topología | CI usa Kafka KRaft 7.6.0; compose usa ZK-mode 7.6.1 ⇒ dos topologías divergentes sin canónica declarada | `ocm-ci.yml:232-244` vs `docker-compose.yml:512-524` | PARCIAL |
| F-13 | LOW | Observabilidad | labels `host: orangehouse`, `env: development` hardcodeados en `promtail.yml` trackeado (maquina-específico en repo) | `promtail/promtail.yml:28,37` | IMPLEMENTADO PERO NO PARAMETRIZADO |
| F-14 | LOW | SSOT | `REDIS_*` fuera del SSOT `env_vars.py`; strings dispersos en `os.getenv`, compose y `lake yaml` | `env_vars.py:145-187` · `cursor_store.py` · `redis_factory.py:20-27` | PARCIALMENTE IMPLEMENTADO |
| F-15 | INFO | Systemd | Sin units para `apps/api`, `live_hydra`, `paper_hydra`; solo market-data y streaming | `deploy/systemd/templates/` | AUSENTE |
| F-16 | INFO | Tracking | B-59 `systemd_reinicia_correctamente` NO_VERIFICADO; ADR-0022:323 "Sin unit activo" ya es falso | `tracking.yaml` · `ADR-0022` | PENDIENTE |

Notas transversales: no se detectaron violaciones de capas ni de BC con import-linter (job `architecture`); el modelo A+B y las prácticas de infra (config-guard, auto-create topics off, depends_on service_healthy, SafeOps, dry_run default) son puntos fuertes.

---

## 20. Gap analysis (objetivo vs actual)

1. **Un solo mecanismo de deployment** declarado como canónico: hoy existen 3 (systemd, docker compose infra, CD Docker OCM) sin autoridad formal.
2. **CD ejercitable:** falta un objetivo real (imagen OCI del microservicio vs despliegue nativo por `install_systemd.sh`), evidencia y health post-deploy coherentes.
3. **Fuentes de secretos declaradas:** runtime depende de `.env` untracked; falta integración con un secret manager o cifrado.
4. **Healthchecks de servicio con dependencias:** `market-data /ready` debería verificar Kafka/Redis o consumir el contrato L1-L4.
5. **SSOT de env vars completo:** `REDIS_*`, puertos `*_HOST_PORT` y nombres `OCM_INTEGRATIONS__*` sin registro canónico.
6. **Métricas alimentando Pushgateway en el runtime activo** (hoy apagadas por default de diseño).
7. **B-59:** verificación de reinicio de las units (kill → restart) sin ejecutar.

---

## 21. Roadmap propuesto (P0-P3)

**P0 (bloqueante de desplegabilidad):**
- Decidir y documentar el modelo canónico (recomendación: A+B nativo para servicios core, infra en compose; CD = puesta al día de units + health L1-L4, no Docker del core).
- Corregir F-02 (ruta de evidencia) y F-01 (si se mantiene Docker CD, alinear `ARTIFACT_SHA` con el tag real y compose profile; si no, eliminar el scaffolding o marcarlo no-ejecutable).
- Cerrar F-05: generar las `EnvironmentFile` desde el template (incluir `.env` como variable) para que re-render no pierda credenciales.

**P1:**
- F-07: `market-data /ready` con dependencias (Kafka+Redis) o reutilizar `health_check.sh` como endpoint; parametrizar nombre de contenedores.
- F-08: activar métricas por sitio (env override) y validar Dashboard/Grafana del canary.
- F-06: secretos de salud desde `REDIS_PASSWORD` real (sin default vulnerable).

**P2:**
- F-14: ampliar `env_vars.py` a `REDIS_*` y puertos `*_HOST_PORT`; F-10: sincronizar `.env.example` con la convención 9093/9094 elegida.
- F-12: declarar topología Kafka canónica (recomendación: KRaft).
- F-13: promtail sin labels de máquina (labels desde env vars).

**P3:**
- F-15: units para API/paper/live; F-16: ejecutar verificación B-59 y actualizar ADR-0022.

---

## 22. Conclusiones

1. **Modelo real verificado:** runtime nativo bare-metal (systemd + `.venv/bin/python`) sobre infra desacoplada en Docker via contratos de endpoints (`localhost:9093` Kafka EXTERNAL, `localhost:6379` Redis). La hipótesis `REDIS_URL` es incorrecta; el contrato real es `REDIS_HOST/PORT/DB/PASSWORD`.
2. **Puntos fuertes verificados:** jerarquía de config Hydra con safe defaults (`dry_run:true`), L5 fail-fast en producción, provisioning explícito de topics (auto-create off), config-guard + dependencias con health, SafeOps en units, `/ready` con Redis en el gateway, CI con 49+ contratos y gates de ingeniería.
3. **Riesgos principales:** split-brain de modelos de deployment (F-01/F-02), read-only del runtime apenas soportada fuera de Docker (health acoplado a container names, F-06/F-07), secrets sin gestión declarada (F-04/F-05), metricas push apagadas por default en el runtime activo (F-08).
4. **Cumplimiento READ-ONLY:** no se modificó código, tests, CI, ADR ni tracking. El único archivo nuevo es este informe en `docs/audits/` (untracked). `git status` conserva exactamente los 8 archivos modificados preexistentes; HEAD `b8b13240`.