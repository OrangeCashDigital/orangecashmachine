# ADR-0039: Runtime canónico — systemd = aplicación, Docker/Compose = infraestructura

**Estado:** Aceptado
**Fecha:** 2026-09-19
**Bounded context(s) afectado(s):** ocm, market_data, observabilidad, deployment

## 1. Título

ADR-0039: Runtime canónico — systemd = aplicación, Docker/Compose = infraestructura

## 2. Estado

**Aceptado** — *Decisión arquitectónica aceptada* ≠ *Implementación realizada* ≠ *Commit realizado* ≠ *Despliegue realizado*. No declara `P0/P1/P2` `HECHO`.

## 3. Contexto

OrangeCashMachine convive con dos mecanismos verificados READ-ONLY:

- `systemd` units `deploy/systemd/templates/ocm-streaming.service.template` y `ocm-market-data.service.template` con `WorkingDirectory=${OCM_REPO_ROOT}` `:10`, `User=${OCM_HOST_USER}` `:9`, `ExecStart=${OCM_REPO_ROOT}/.venv/bin/python -m ...` `:13`.
- `Docker/Compose` con `redis` `:95`, `kafka/zookeeper` `:473/:512` `KAFKA_LISTENERS INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:9093` `:529`, `pushgateway` `:122`, `prometheus` `:143`, `alertmanager` `:185`, `grafana` `:218`, `loki` `:255`, `promtail` `:288`, `kafka-ui` `:577` `profiles: [monitoring]`.

`ADR-0022` (Lifecycle `realtime_feeds`, Aceptado) define `streaming` como cuarto modo `run.sh streaming` con `CompositionRoot.build_ws_producers()`.

Solo hechos verificables.

## 4. Problema

1. `ocm-streaming.service` está `enabled` + `active running` `systemctl status: Loaded /etc/systemd/system/ocm-streaming.service` `MainPID=69498` `ActiveState=active` `FragmentPath=/etc/systemd/system/ocm-streaming.service` mientras `ocm_market_data` existe como `docker ps Up unhealthy` `profiles: [microservices]` `docker-compose.yml:334` — sin canónico declarado.
2. `docker-compose.yml:24` `# 🔕 market-data : perfil [microservices] — corre en host por defecto` + `docker-compose.override.yml:19` contradicen contenedor como aparente runtime.
3. `B-59` `tracking.yaml:3146` `systemd_reinicia_correctamente: NO_VERIFICADO` y `A1` `BYBIT_API_KEY/SECRET` no pueden decidir mecanismo sin runtime.
4. `Bronze` `find 0 parquet` `health 503` vs `L3 orderbook.raw 9943250` — canónico no declarado.

## 5. Decisión

- **Runtime canónico de aplicaciones OCM es `systemd` en Debian** — `ocm-streaming.service` (WS `orderbook.raw`) y `ocm-market-data.service` (REST `ohlcv.raw` → Bronze) ejecutan `WorkingDirectory=${OCM_REPO_ROOT}` `:10`, `User=${OCM_HOST_USER}` `:9`, `ExecStart=${OCM_REPO_ROOT}/.venv/bin/python -m ...` `:13` bajo `systemd`.
- **Docker/Compose es infraestructura** — `redis:95`, `kafka:512`, `zookeeper:473`, `pushgateway:122`, `prometheus:143`, `alertmanager:185`, `grafana:218`, `loki:255`, `promtail:288`, `kafka-ui:577` `profiles: [monitoring]`. No es runtime canónico.
- `ocm_market_data` `profiles: [microservices]` (`:335`) es **prueba operador, clasificada EXPERIMENTAL/NO PROBLEMÁTICO** — no bifurcación productiva.
- Dockerización futura queda como **opción futura** con solo pre-condiciones (`plan:17` `Future Dockerization Preconditions`): `config independiente`, `health contrato`, `endpoint`, `logs`, `señales`, `persistencia`, `secrets env-inyectado`. **No decidir todavía si Market Data debe ejecutarse en Docker.**

## 6. Alcance

**Dentro:** `market_data` (REST `ohlcv.raw` + WS `realtime_feeds`), `streaming` (`apps/app/cli/streaming_hydra.py` `loop.add_signal_handler SIGINT/SIGTERM` `:226`), futuros BC `NIVEL 1` (`ADR-0024`).
**Fuera:** `trading`/`portfolio` `docker-compose.yml:376,424` `NO EJECUTABLE` (sin `trading.main`), CD `ocm-cd.yml` (depende `ADR-C`).

## 7. Consecuencias positivas

- Frontera `REPOSITORY → DEPLOYMENT API → RUNTIME systemd → INFRA Docker` (`plan:45`) verificable `systemctl is-active` + `docker ps`.
- `install_systemd.sh:43` `envsubst ${OCM_HOST_USER}/${OCM_REPO_ROOT}` portabilidad cualquier Debian.
- `health` `P1 D4` `aiokafka/redis` sin `docker exec` porque app es nativa.

## 8. Trade-offs / consecuencias negativas

- `ocm-market-data.service` hoy `inactive` requiere `P2` transición `B2: docker stop ocm_market_data` → `systemctl enable --now ocm-market-data.service` (colisión `:8001`).
- `systemd` single-host `After=network-online.target docker.service` `templates:4` — multi-node requiere `ADR-C`.
- `Dockerfile:40 CMD ["python","-m","market_data.main"]` válido solo como `experiment`.
- `B-59 E2E` sigue `PENDIENTE` hasta `P2`.

## 9. Relación con portabilidad

Compatible `cualquier Debian/Linux` sin depender de `OrangeHouse`: `templates:9` `User=${OCM_HOST_USER}` + `10 WorkingDirectory=${OCM_REPO_ROOT}` variables (no `/home/orangemusic` literal en VCS; `git show HEAD:template` `${OCM_HOST_USER}`), `deploy/host.env.example` `OCM_HOST_USER=<usuario del host>` `M` portabilidad, `install_systemd.sh:43` lista explícita, `docker-compose.yml:41 OCM_ENV=${OCM_ENV:-production}`. `OLLAMA_HOST 192.168.100.2` en `.env` ignorado. No hardcode.

## 10. Evidencia concreta del repositorio que sustenta la decisión

| Evidencia | Archivo:línea/commit |
|---|---|
| `ocm-streaming active running` 4h49m `enabled` | `systemctl status/show` `FragmentPath=/etc/systemd/system/ocm-streaming.service` `ActiveState=active` `EnvironmentFiles=.../deploy/host.env + .../.env` |
| `ocm-market-data template` | `deploy/systemd/templates/ocm-market-data.service.template:1,13` |
| `ocm_market_data Up unhealthy` vs `inactive` | `docker ps` + `plan:62` |
| `microservices OFF` | `docker-compose.yml:24,335` `profiles: [microservices]` |
| `host-local default` | `docker-compose.override.yml:19` |
| `installer` | `deploy/scripts/install_systemd.sh:14,43` |
| `Validated Architecture` | `docs/audit/deployment-runtime-implementation-plan.md:15` |
| `L3` | `streaming_started gateway=localhost:9091` + `kafka-get-offsets orderbook.raw 9943250 +4525/min` |
| `L4` vacío | `find Bronze 0 parquet` `curl :8011/health 503 PipelineBuildError apiKey:''` |

## 11. Relación con D1, si corresponde

Ninguna directa. `D1` `72bd1878` `+EnvironmentFile .env` es `ADR-B`. Esta ADR habilita `P2` que depende de `D1` ya resuelto (`rendered==installed`).

## 12. Qué NO decide este ADR

- No declara `P0/P1/P2` `HECHO`.
- No autoriza `A1` `BYBIT_API_KEY/SECRET` `environment:` — solo determina frontera donde deberán resolverse credenciales: en canónico `systemd`, `BYBIT` via `EnvironmentFile=${OCM_REPO_ROOT}/.env` `:12` → `os.environ` → `credentials.py:59` → `SecretStr`.
- No decide si `market-data` debe ejecutarse en Docker (Docker queda `opción futura P5`).
- No crea `CredentialResolver`, `SecretManager`, nuevos `ports/adapters`, `bounded contexts`, `brokers`, `DB`.
- No convierte hipótesis futura en arquitectura existente.
- No autoriza `B-59` operativo `systemctl restart`.

## 13. Estado de implementación

**Aceptado** ≠ **Implementación realizada** (`P2` `enable --now` pendiente) ≠ **Commit realizado** (propuesta no escrita a disco antes de esta autorización) ≠ **Despliegue realizado** (`systemctl restart` pendiente, `B-59 PENDIENTE` `tracking.yaml:3146`). No autoriza `P0/P1/P2/A1`.

## CAMBIOS QUE ESTE ADR NO AUTORIZA

`P0` (`D5` ya `04e9b6fd`, `D6` puertos, `D7` `KAFKA` default, `G4`), `P1` `install.sh`/`health contrato`, `P2` `Market Data systemd` `C12/B2/B3` `enable --now`/`restart`, `P3` robustez, `ADR-C` CD, `Vertical Slice` `StrategyCandidate`, `Paper/Live`, `A1` propagación `BYBIT`, `B-59` `tracking HECHO`, `docker compose up/down`, `systemctl start/stop/restart/enable/disable/daemon-reload`, `modificación .env/host.env/Dockerfile/Compose/Kafka/código`, `commit/push/merge/rebase/reset/clean`, nuevos `bounded contexts/ports/adapters/brokers`.
