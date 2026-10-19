# Deployment & Runtime Architecture — Adversarial Audit

> **Tipo:** Auditoría adversarial, read-only, orientada a evidencia.
> **Fecha:** 2026-09-19
> **Branch/HEAD auditados:** `feat/b56-ci-stage-ordering` @ `b8b13240` (+ working tree destruyendo: ver §30).
> **Protocolo:** `docs/governance/AUDIT_PROTOCOL.md` (read-only; findings revalidados contra tracking.yaml/ADRs; control FAIL ≠ finding nuevo).
> **Hipótesis bajo ataque:** *native OCM + systemd + infraestructura Docker + orquestador `install.sh → deploy/install.sh` + directorios `deploy/{runtime,infrastructure,config,health,scripts,systemd,monitoring}`*.
> **Pregunta central:** ¿Cuál es el modelo de deployment/runtime más apropiado para OCM considerando su arquitectura REAL actual (no idealizada)?
> **Decisión (ver §22):** **ADOPTAR PARCIALMENTE** — la evidencia favorece consolidar el modelo A+B ya existente con un orquestador fino y reparaciones concretas, y rechaza la mayoría de los directorios nuevos propuestos.

---

## 1. Reconstrucción del modelo actual

### 1.1 Entry points reales (verificados)

`pyproject.toml [project.scripts]` (SSOT declarado):

```
ocm       = "app.cli.main:main"          # pipeline market-data (Hydra)
ocm-api   = "api.main:serve"             # FastAPI gateway (experimental)
live      = "app.cli.live_hydra:main"    # live trading (⚠ capital real; IS_STUB=true hoy)
paper     = "app.cli.paper_hydra:main"   # paper trading
streaming = "app.cli.streaming_hydra:main"  # canary ORDERBOOK WS (F2.6b)
```
(Evidencia: `pyproject.toml` `[project.scripts]`.)

Facade raíz `run.sh`: `case MODE in ocm|live|paper) exec uv run python -m app.cli.*"`. Declara "SSOT: idénticos a `[project.scripts]`". **No expone `ocm-api` ni `streaming`** (subconjunto documentado). (Evidencia: `run.sh:1-46`.)

Processos con ciclo de vida largo:
- **`market-data`** (`packages/market_data/main.py`): FastAPI :8001; lifespan lanza **3 tareas asyncio en un solo proceso**: (a) `_ingestion_loop` (cada `INGESTION_INTERVAL_S`, `market_data_flow` vía `CompositionRoot.assemble(ctx.app_config).factory`), (b) Kappa `KafkaBronzeWriter` (consume `ohlcv.raw` → Bronze parquet), (c) `FeedOrchestrator` (WS feeds → `trades.raw`). `__main__` ejecuta `uvicorn ... workers=1` (no fork-safe el loop). Endpoints: `/health` (liveness, 503 si guard activado), `/ready` (503 si ctx aún no inicializado; **NO pinea Kafka/Redis**), `/ohlcv/{...}` (lee Silver Iceberg). (Evidencia: `packages/market_data/main.py:121-219, 315-400, 465-548, 622-632`.)
- **`streaming`** (`apps/app/cli/streaming_hydra.py`): proceso WS → `OrderBookKafkaProducer` → `PrometheusPusher.push()` hacia `PUSHGATEWAY_URL` (job `ocm_pipeline_orderbook`), loop heartbeat con `--push-interval`; shutdown limpio SIGINT/SIGTERM. (Evidencia: `apps/app/cli/streaming_hydra.py` docstring + imports.)
- **`ocm`** (`apps/app/cli/main.py`): pipeline histórico/temporizado Hydra; flujo `run_application → EnvironmentValidator.check() → pipeline_runner(ctx, pusher)`; `OCM_VALIDATE_ONLY=true` → exit(0) sin ejecutar.
- **`ocm-api`** (`apps/api/main.py`): gateway FastAPI; lifespan **fail-fast**: si Redis no responde → `sys.exit(1)` antes de aceptar tráfico; `/health` liveness puro, `/ready` → Redis ping. (Evidencia: `apps/api/main.py` lifespan, `apps/api/routers/health.py`.)
- **trading / portfolio**: **sin `main.py`** y sin entrypoint de proceso independiente (`find packages/trading packages/portfolio -name main.py` → vacío). Solo composition roots embebidos usados por `execute_live`/`execute_paper`/`paper_hydra`/`live_hydra`. (Evidencia: ADR-0024 NIVEL 2; `docker-compose.yml` blocks `trading:`/`portfolio:` marcados "SCAFFOLDING FUTURO … NO EJECUTABLE … NO existe trading.main").

### 1.2 Configuración / bootstrap

- Cadena: Hydra (base→env→exchange→pipeline→observability→risk→storage→CLI overlay) → resolver `${oc.env:VAR,default}` → `AppConfig` Pydantic (L4) → reglas de negocio L5 (fail-fast cross-field en production: `dry_run`, `require_explicit_start`, `KAFKA_ENABLED` obligatorio si `kafka.enabled`, path de data lake NO local en prod, etc.). (Evidencia: `assets/config/base.yaml`, `config/env/production.yaml`, `ocm/config/pipeline.py:20-28,234,276-281`, `ocm/config/hydra_loader.py:217`, `ocm/runtime/environment_validator.py`.)
- SafeOps: `safety.dry_run: true` en base, `false` en `env/production.yaml` + `require_confirmation: true`. `OCM_VALIDATE_ONLY` usa `BOOL_TRUE={true,yes,on}`.
- SSOT de nombres de env var: `ocm/config/env_vars.py` (registro canónico `_ENV_VAR_NAMES`, guard de sincronía en import). (Evidencia: `ocm/config/env_vars.py`.)

### 1.3 Dependencias e infraestructura

Flujo operador → comando → infra (reconstrucción con evidencia):

```
operador
  ├── uv sync [--group dev]          → .venv            (README:152; Dockerfile usa uv sync --no-dev --system)
  ├── cp .env.example .env           → vars host/dev   (.env → compose *_HOST_PORT, KAFKA_BOOTSTRAP_SERVERS, REDIS_PASSWORD…)
  ├── cp deploy/host.env.example deploy/host.env → EnvironmentFile vars (install_systemd.sh:16,32)
  ├── docker compose up -d           → infra (Kafka+ZK, Redis, Pushgateway, Prometheus, Alertmanager, Grafana, Loki, Promtail)
  │                                     → config-guard (busybox init valida configs de monitoring; compose:65-94)
  ├── deploy/scripts/install_systemd.sh [--user|--verify-only]  → render→verify→install→daemon-reload
  │                                     (templates en VCS → rendered gitignored → /etc/systemd/system)
  ├── systemctl start/restart ocm-market-data ocm-streaming     (manual; B-59 NO_VERIFICADO)
  ├── uv run python -m app.cli.main (pipeline CLI) / uv run streaming (canary) / uv run ocm-api (gateway)
  └── deploy/scripts/health_check.sh → L1 systemd · L2 kafka/redis · L3 freshness topics · L4 Bronze mtime
```

- La **aplicación** solo conoce contratos de env: `KAFKA_BOOTSTRAP_SERVERS` (free-form), `REDIS_HOST/PORT/DB/PASSWORD`, `PUSHGATEWAY_URL`, `OCM_STORAGE__DATA_LAKE__PATH`. **No conoce Docker** (no hay imports de docker/environment container names en `ocm/`, `packages/`, `apps/`).
- El **tooling operacional** sí conoce Docker: `health_check.sh:36-68` y gates G5/G6 del `check_production_gates.py` usan `docker exec ocm_kafka/ocm_redis`. Matiz crítico (§27).

### 1.4 Almacenamiento

- Data lake local en `data_platform/` (Iceberg warehouse bronze/silver/gold), mapeado por `config/storage/datalake.yaml` y `OCM_STORAGE__DATA_LAKE__PATH` (compartido nativo/docker vía bind `./data_platform:/app/data_platform` en profile microservices). DuckDB solo en research (consumidor gold read-only). (Evidencia: `docker-compose.yml` market-data volumes; `apps/research/data/composition_root.py` → `GoldReader`.)

---

## 2. Inventario real de deployment

**`deploy/`** (boundary operacional de facto):

| Artefacto | Qué es | Consumido por |
|---|---|---|
| `deploy/scripts/install_systemd.sh` | render+templates→rendered→verify→install (system/user)+daemon-reload; `--verify-only` | gate G4 (con bug de path, ver §20 F-3); operador |
| `deploy/scripts/health_check.sh` | L1–L4 semántico; exit 0/1/2; `MARKET_DATA_HEALTHY`/`INFRA_HEALTHY`/`OBSERVABILITY_HEALTHY` | gate G7; `deploy_ocm.sh` pre/post-deploy |
| `deploy/scripts/deploy_ocm.sh` | CD verify→backup→deploy→health→ACCEPT/ROLLBACK→evidencia JSON | `ocm-cd.yml` (workflow_dispatch) |
| `deploy/systemd/templates/*.template` | SSOT de units (VCS) | `install_systemd.sh` |
| `deploy/systemd/rendered/*.service` | artefacto generado (gitignored) | systemd; gate G4 |
| `deploy/monitoring/` | prometheus/alertmanager/alerts/loki/promtail/grafana(dashboards+datasources) | compose (bind mounts) |
| `deploy/host.env(.example)` | topología de host (env vars) para render; **trackeado pese a policy (§20 F-6)** | `install_systemd.sh` |

Nota: **no existe** `deploy/` para `runtime/`, `infrastructure/`, `config/`, `health/` (los propuestos). La observabilidad vive en `deploy/monitoring`, no `deploy/infrastructure`.

**`scripts/` (raíz)** — 12 ficheros, clasificación por responsabilidad real:

| Script | Clase real | Comentario |
|---|---|---|
| `app_layer_guard.py`, `domain_subprocess_guard.py`, `backtest_app_guard.py` | Gates AST de arquitectura + backtest | CI |
| `check_production_gates.py` | Orquestador de gates (G1–G11; modos gate-dev/release/ci) | CI + operador; **la pieza más cercana a un orchestrator que ya existe** |
| `check_ssot_enums.py` | Guard de literales SSOT | pre-commit/CI |
| `audit_validator.py` | Reglas M1–M25 (governance) | CI policy |
| `engineering_health_check.py` | Coherencia Plan↔tracking↔ADR↔contratos↔CI (F2.0) | CI |
| `compliance_report.py`, `metrics_report.py` | Reporting | utilidades |
| `migrate_audit_filenames.py` | One-off de migración (LEGACY) | cleanup candidato |
| `provision_kafka_topics.py` | **Operación de infra** (idempotente, SSOT de topics) | operador (candidato natural a ser invocado por un orquestador) |
| `reset_kafka_test_topic.sh` | Utilidad test/dev | dev |

Veredicto de fragmentación: **no hay fragmentación significativa.** `scripts/` = gobernanza/CI; `deploy/scripts/` = operaciones de deployment; `deploy/monitoring|systemd` = configuración operativa versionable. La fragmentación sí existe en OTRA dimensión: **falta un orquestador de instalación/verificación** — el conocimiento instalador está repartido entre README (uv sync + compose up), `install_systemd.sh`, `health_check.sh`, `provision_kafka_topics.py` y `check_production_gates.py`. Ese es el hueco real que la propuesta apunta.

---

## 3. Nuestra propuesta bajo ataque

### 3.1 ¿Es necesario un entry point único? → Parcialmente.

- **Ya existe una fachada de ejecución** (`run.sh`) y un SSOT de entrypoints (`[project.scripts]`). Lo que **no existe** es una API operacional de *instalación/verificación* (un `--check` que diga "está todo listo/instalado/sano"). README documenta pasos pero no los secuencia ni los verifica.
- **El orquestador de gates `check_production_gates.py` ya es la semilla de esa API.** `--mode gate-dev` ejecuta 11 gates (config, linter, units systemd `--verify-only`, runtime, deps, health…). Un `deploy/install.sh --check` duplicaría SU DSL de gates si se implementa desde cero. **Trade-off:** mejor EXtender `check_production_gates.py` (o delegarlo dentro del orquestador) que volver a implementar `PASS/BLOCK` por separado.

### 3.2 ¿Duplicaría mecanismos?

| Pieza propuesta | Mecanismo existente que duplica | Veredicto |
|---|---|---|
| `install.sh` en raíz | `run.sh` (patrón facade) y `README` | Facade **aceptable pero opcional**; el valor está en `deploy/install.sh` |
| `deploy/install.sh` etapa runtime | `uv sync` + `.venv` (pyproject) | NO es duplicación si SOLO invoca `uv sync`/`uv run` |
| etapa infra | `docker compose up -d` + `config-guard` + `provision_kafka_topics.py` | NO si invoca; SÍ si re-implementa |
| etapa systemd | `install_systemd.sh` (existe) | NO si lo invoca |
| etapa observabilidad | compose ya provisiona monitoring | parcial (health de observabilidad ya en `health_check.sh` L-obs) |
| etapa health/readiness | `health_check.sh` | NO si lo invoca |
| `deploy/runtime/` dir | pyproject+uv (runtime) | **Redundante** — mueve, no añade |
| `deploy/infrastructure/` dir | `docker-compose.yml` raíz | **Redundante** — solo churn |
| `deploy/config/` dir | Hydra (`config/`) + `deploy/host.env` | **Redundante/contradictorio** si pretende reunir config de app; riesgo de crear una segunda fuente de verdad |
| `deploy/health/` dir | `health_check.sh` en `deploy/scripts/` | **Redundante** (mover = churn) |

### 3.3 ¿Qué debería hacer y dónde terminar su responsabilidad?

La frontera profesional de un instalador-orquestador (ver §13). Crucial: **termina cuando el sistema está instalado/verificado/sano**. NO debe: instalar Python/uv del sistema (lo hace el OS/paquete), construir imágenes, ejecutar el pipeline, gestionar secrets del proceso, ni ser idempotente-supuesto (debe ser idempotente-diseñado, §14).

### 3.4 Clasificación de `install.sh`

- **a) buena API operacional** → Sí, si reporta `PASS/FAIL/SKIP` y modos `--check/--apply` (DSL heredado de `check_production_gates.py`).
- **b) wrapper innecesario** → Lo sería si solo encapsula el README sin verificación.
- **c) fachada útil** → Sí, a nivel raíz SOLO si es un `exec` de una línea hacia `deploy/install.sh` (patrón `run.sh`).
- **d) fuente adicional de complejidad** → Sí si implementa lógica instaladora propia (copiar units, gestionar venv, editar compose). La evidencia del repo (gate G4 que daña `scripts/install_systemd.sh` inexistente, §20) muestra que cada script nuevo con lógica propia añade superficie de error.

**Conclusión §3:** la propuesta instala un "Ghost Orchestrator": el hueco real es **composición de componentes ya existentes**, no nuevos componentes. Un `install.sh` que delegara 100% sería válido; el valor neto reside en que `--check` cierre B-59 y reduzca mezcla manual.

---

## 4. `deploy/` como boundary

- **¿Existe hoy boundary? → De facto sí, formal no.** Todo lo operacional que puede versionarse vive en `deploy/` (systemd templates, monitoring, scripts, host.env.example) y no hay imports de código de aplicación hacia él; la aplicación no referencia `deploy/` en runtime (salvo `EnvironmentFile=deploy/host.env` en las units, que es path, no import).
- **`deploy/scripts/`: ¿boundary legítimo, bucket, deuda, o mezcla?** → **Boundary legítimo en consolidación.** Sus 3 scripts tienen dueño claro (`install_systemd`→systemd, `health_check`→salud, `deploy_ocm`→CD) y validación propia (`systemd-analyze verify`, gates G4/G7). No es un bucket: cada script responde a una operación real. La deuda está en `deploy_ocm.sh` (no operacional, §20) y en la política rota de `deploy/host.env` tracking.
- **Análisis de cada directorio propuesto** (regla anti-sobre ¥§25):

| Dir propuesto | Responsabilidad real | ¿Existe ya? | ¿Elimina duplicación? | ¿Nueva SSOT? | ¿Complejidad? | Veredicto |
|---|---|---|---|---|---|---|
| `systemd/` | Units SSOT + rendered | Sí (templates+rendered) | — | Sí (templates) | Baja | **Adoptar tal cual** (+ drift check) |
| `monitoring/` | Config observabilidad docker | Sí | — | Sí | Baja | **Adoptar tal cual** |
| `scripts/` | Ops deployment | Sí (`deploy/scripts/`) | — | Sí | Baja | **Adoptar tal cual** |
| `runtime/` | Toolchain/venv | No (uv+pyproject lo cubren) | No; mueve | **No — duplicaría** | Media | **Rechazar** |
| `infrastructure/` | Infra docker/externa | Parcial (compose raíz) | No; mueve compose | No — duplicaría | Alta (paths en docs/CI/README) | **Rechazar hoy** (ADR si se decide) |
| `config/` | Config operacional | Sí (Hydra `config/` + host.env) | No — mezcla dos SSOT | **Sí — iría contra SSOT** | Alta | **Rechazar** |
| `health/` | Probes | Sí (`health_check.sh`) | No | No — mueve | Baja | **Rechazar** (mover = churn) |

**§4 Conclusión:** `deploy/` ya ES el boundary que la propuesta quiere formalizar; los subdirectorios existentes (`systemd/`, `monitoring/`, `scripts/`) son correctos; los propuestos nuevos (`runtime/`, `infrastructure/`, `config/`, `health/`) confunden organización con arquitectura (§26) y crean risk de múltiples SSOT.

---

## 5. Systemd — uso real y potencial

- **Units que existen (templates VCS):** `ocm-market-data.service`, `ocm-streaming.service`. Ambas renderizadas en el host (evidencia: `deploy/systemd/rendered/*.service` con paths reales `/home/orangemusic/...`).
- **Units que NO pueden existir todavía:**
  - `ocm-trading.service` — **prohibido por evidencia**: no existe `trading.main` (`find` vacío; ADR-0024 lo clasifica NIVEL 2 embebido y el compose lo llama "SCAFFOLDING FUTURO … NO EJECUTABLE").
  - `ocm-portfolio.service` — idéntico.
  - `ocm-api.service` — **no existe hoy** (gateway experimental, sin unit). Opcional futuro; su entrypoint sí existe (`api.main:serve`), a diferencia de trading/portfolio.
  - `ocm.service` (pipeline CLI one-shot/largo) — no hay unit; se ejecuta por `uv run ocm`/`run.sh`. Legítimo como unit `oneshot`+`timer` si se quiere; hoy es manual.
- **Propiedades reales de las units (templates):** `Type=simple`, `User=${OCM_HOST_USER}` (unprivileged), `WorkingDirectory=${OCM_REPO_ROOT}`, `EnvironmentFile=deploy/host.env`, `ExecStart=.venv/bin/python -m market_data.main` / `-m app.cli.streaming_hydra`, `Restart=on-failure`, `RestartSec=10`, `TimeoutStopSec=30`, `KillSignal=SIGINT`, hardening: `NoNewPrivileges=true`, `ProtectSystem=strict`, `ReadWritePaths=data_platform+logs`, `PrivateTmp=true`. (Evidencia: templates; verificados byte-a-byte.)
- **DRIFT verificado (§20 F-4):** las units **renderizadas en disco** contienen una línea extra `EnvironmentFile=…/.env` (heredada de una edición manual posterior al render) que el template NO tiene. `install_systemd.sh` al re-render las regeneraría SIN esa línea → el SO actual difiere del repo. Este es precisamente el síndrome que la propuesta de "drift detection" quiere atacar (§6).
- **Startup ordering:** `After=network-online.target docker.service` (depende de que Docker/red esté arriba para infra). `docker.service` en `After=` acopla la unit al runtime Docker — defendible solo mientras infra=compose local; si infra pasa a externa, esta directiva debe desaparecer.
- **Inicio/parada:** `install_systemd.sh` solo instala+`daemon-reload`; **no hace enable/start** (operación manual documentada). B-59 (`systemd_reinicia_correctamente`) PENDIENTE/NO_VERIFICADO (tracking.yaml:3146-3158).

---

## 6. Repo vs SO

- **SSOT:** `deploy/systemd/templates/*.template` (VCS). `rendered/` (gitignored) y `/etc/systemd/system` son derivados.
- **Instalación:** `install_systemd.sh --apply` → render→verify→install(644)→daemon-reload. Correcto en Debian para units de operador (`/etc/systemd/system`).
- **Actualización:** re-render + overwrite (idempotente salvo ediciones manuales, §14).
- **Detección de drift:** **no existe mecánicamente.** Un drift como el de `rendered` (§5) silencia. La verificación actual es solo `systemd-analyze verify` del rendered presente (gate G4), que valida sintaxis, NO la paridad repo↔SO ni template↔rendered.
- **Reversión:** copia de seguridad del unit previo solo en backups manuales; no hay `--restore`.
- **¿Pertenece un mecanismo de drift detection al proyecto?** → **Sí, pero mínimo y dentro del orquestador.** NO un servicio de auditoría en tiempo real: basta que `deploy/install.sh --check` (1) re-render en memoria symlinks templates→rendered, (2) compare con el rendereado en disco, (3) compare con `/etc/systemd/system`. 10-15 líneas de bash heredadas de `install_systemd.sh`. Coste operacional ≈ 0 (solo bajo demanda). Evidencia de necesidad: el drift real ya ocurrió (§5).

---

## 7. Docker / Compose

Clasificación real de lo que corre en el compose contract (default sin profiles):

| Servicio | Clase | Evidencia |
|---|---|---|
| `redis`, `kafka+`zookeeper`` | **INFRAESTRUCTURA** | infra de datos para los procesos nativos |
| `prometheus`, `alertmanager`, `pushgateway`, `grafana`, `loki`, `promtail`, `kafka-ui` (profile monitoring) | **OBSERVABILIDAD** | `deploy/monitoring/*` provisiona config |
| `config-guard` | **INFRAESTRUCTURA (init)** | valida configs de monitoring antes de arrancar |
| `market-data` (profile microservices, `build:` de `Dockerfile`) | **APLICACIÓN** (opcional) | **off por defecto** — override lo comenta: "el pipeline corre en la maquina local (fuera de Docker)" |
| `trading`, `portfolio` (profile microservices) | **APLICACIÓN SCAFFOLDING** | "NO EJECUTABLE … NO existe trading.main" |

Veredicto por entorno (§12): Docker = mecanismo de una sola máquina para infra+obs, con aplicación **opt-out**. Kafka/Redis hoy: Docker en el host. ¿Deben ser external managed? **Depende del entorno** — para un single-host OrangeHouse (ADR-0037: bare-metal/local), Docker local para infra es reproducible y de bajo coste; pasar Kafka/Redis a managed solo tiene sentido si OCM sale del host único. **La app ya NO depende de Docker** (contratos por env), así que la migración a managed sería cambio de infra, no de código.

Trade-offs honestos para infra en Docker:
- + Reproducibilidad (compose con versiones pined `redis:7.2-alpine`, `confluentinc/cp-kafka:7.6.1`…), encendido/apagado simple, aislamiento, resource limits declarados (`deploy.resources.limits` en kafka/redis/microservices).
- − Persistencia en volúmenes nombrados (backup = operación manual), networking interno (EXTERNAL `localhost:9093`), la observabilidad dentro del mismo runtime que cubre: si Docker cae, caen las alertas que avisarían de la caída (SPOF operacional).
- **Persistencia:** volúmenes `kafka_data`, `redis_data`, `grafana_data`, `prometheus_data`, `loki_data` (ethereum:604-620). Bakcs manual.
- **Restart:** `redis: unless-stopped`, `kafka: unless-stopped`, o `on-failure:5` para servicios; healthchecks con `start_period` 30s.

---

## 8. Kafka — contrato real

- **Contrato de aplicación:** `KAFKA_BOOTSTRAP_SERVERS` (free-form `host:port[,...]`). SSOT env_vars.py. Defaults: `config/base.yaml` → `localhost:9092`; `config/env/production.yaml` → `kafka:9092`; schema Pydantic `ocm/config/schema.py:571` → `"kafka:9092"`.
- **Listeners (compose):** INTERNAL `kafka:9092` (Docker network), EXTERNAL `localhost:${KAFKA_HOST_PORT:-9093}` (host). `docker-compose.yml:530`.
- **Topics:** auto-creation **desactivada** (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`, compose:535); provision via `scripts/provision_kafka_topics.py` (idempotente, SSOT de topics). Topics de negocio observados: `orderbook.raw`, `ohlcv.raw`, `trades.raw` (health_check/gates/main).
- **Retries/persistence:** producer/consumer OCM configurados vía `KAFKA_*` (acks, linger, session timeouts, auto-offset-reset=earliest). Broker: retención 168h, replication factor 1, partitions 3.
- **Auth/TLS:** **PLAINTEXT** (listeners map `INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT`). **Ninguna autenticación SASL ni TLS** en el broker de compose. Para un single-host local coherente; es una deuda de seguridad si el host es reachable (matiz §15).
- **¿OCM conoce Docker? → NO para Kafka.** Conoce `KAFKA_BOOTSTRAP_SERVERS`. El desacoplamiento hold: la app hablaría con un broker remoto/externo sin tocar código. **Caveat:** el default Pydantic `kafka:9092` (DNS Docker) es foot-gun en bare-metal sin env var (§20 F-9); y `raise_` tooling operacional (health/gates) sí usa `docker exec` (matiz §27).

---

## 9. Redis — contrato real

- **Variables discretas:** `REDIS_HOST`, `REDIS_PORT`, `REDIS_DB`, `REDIS_PASSWORD` (+ feature flag `REDIS_ENABLED`). SSOT `ocm/config/env_vars.py` y `apps/api/settings.py` (validation_alias). **NO existe `REDIS_URL` como variable registrada**; `apps/api/settings.py` compone `redis_url` internamente desde las 4 variables (model_validator; nunca lee var `REDIS_URL`).
- **Adecuación del contrato:** correcto para host local, Docker y remoto; evita la colisión de precedencia de `REDIS_URL` compuesta (factores de DB/password separados). **Mejor mantener** el esquema discreto; NO introducir `REDIS_URL` ($9 mandate honored).
- **Want de sí Docker:** defaults `base.yaml` `REDIS_HOST=localhost`; `production.yaml` `REDIS_HOST=redis` (DNS Docker). API fail-fast si Redis inaccesible en startup.

---

## 10. Observabilidad — clasificación de dependencias

| Dependencia | Clasificación real | Evidencia |
|---|---|---|
| **Prometheus** | **SOFT** (push pull de infra; sin él no hay métricas scrapeadas pero el pipeline corre) | metrics solo si `metrics.enabled`; en dev=false |
| **Grafana** | **SOFT/OPTIONAL** | dashboards provisionadas; sin él nada se rompe |
| **Pushgateway** | **SOFT** | el streaming lo usa (`PUSHGATEWAY_URL`); falla → `PrometheusPusher.push` error (fail-soft en push?) — ver abajo |
| **Alertmanager / Loki / Promtail** | **OPTIONAL** (solo recolección) | docker services; la app no los referencia |
| **OTel tracing backend** | **OPTIONAL — fail-soft explícito** | `ocm/observability/tracing.py`: "Fail-soft: si el exporter falla … el sistema continúa sin tracing — nunca bloquea"; disabled por defecto (`tracing.enabled:false`, endpoint null) |
| **Métricas backend** | **SOFT — NoopPusher cuando disabled** | streaming_hydra: usa `PrometheusPusher` solo si config; `metrics.enabled` del observability.yaml default `false`, `production.yaml` la activa |

Coherencia con FAIL-FAST/FAIL-SOFT: la **aplicación** sigue el principio "fail-fast para coherencia de config (L5/tender) y deps críticas (Redis en API)"; **observabilidad es fail-soft por diseño** (tracing idempotente terse-qui, metrics con pusher no bloqueante, `OBSERVABILITY_HEALTHY` no gatea la ejecución). Esto es correcto y debe preservarse: observabilidad que bloquea la operación sería un anti-pattern.

---

## 11. Configuración

- **SSOT:** env var names → `ocm/config/env_vars.py`; Hydra YAML por capas; Pydantic `AppConfig`; host topology → `deploy/host.env(.example)`; dev → `.env` (gitignored).
- **Precedencia verificada:** Hydra `${oc.env:VAR,default}` < Hydra default YAML < CLI overlay. En API: `OCM_API_*` prefijado + validation_alias para shared `REDIS_*`.
- **Secretos:** `.env` (gitignored) contiene `BYBIT_API_SECRET`, `GRAFANA_PASSWORD`, `OCM_API_JWT_SECRET` (placeholder), `KAFKA_UI_PASSWORD` — **no trackeado** (git ls-files: solo `.env.example`+`deploy/host.env`). `deploy/host.env` **sí trackeado** pese a cabecera "NUNCA commitear" (§20 F-6); contenido actual: topología sin secrets (OK hoy, pero es una política violada).
- **Drift observable:** `.env` real vs `.env.example` puertos Alertmanager/Kafka invertidos (§20 F-10). `schema.py:571` default kafka:9092 vs base.yaml localhost:9092 vs producción kafka:9092 → tres lugares de precedencia del default Kafka (foot-gun §20 F-9).
- **`deploy/config/` NO:** duplicaría Hydra (`config/`) + host.env; no hay responsabilidad que hoy esté sin dueño que justifique mezclar app-config y host-topology en un tercer lugar.

---

## 12. DEV / TEST / STAGING / PROD

| Entorno | Modelo actual (evidencia) | Juicio |
|---|---|---|
| DEV | OCM nativo (`.venv`, `uv run`, override expone 6379, market-data comentado en docker) + infra compose | Correcto y ya así; mantener |
| TEST (CI) | Compose no aplica: CI levanta Kafka como service container (`ocm-ci.yml` integration-tests, puerto 9093); unit tests sin infra | Correcto |
| STAGING | **No existe** staging declarado (solo `OCM_ENV` development/production/test); ADR-0037 "mismo host" no distingue staging | Gap real: si se quiere hacer CD con preflight real, necesita un staging o un `--verify-only` contra producción dry-run |
| PROD | A+B: units systemd nativas + infra compose local; `production.yaml` dry_run:false/metrics on | Coherente con OrangeHouse; CD no operacional (§24) |

Lo que la evidencia NO soporta: correr OCM (app) containerizado en PROD HOY — trading/portfolio sin entrypoint; market-data solo en profile opt-in con `command: python -m market_data.main` explícito; el propio override comenta el bloque. Containerización de producción es un objetivo cuando existan los 3 microservicios NIVEL 1 (ADR-0024), no ahora.

---

## 13. Instalador — responsabilidades y dueño

| Responsabilidad | Dueño correcto según evidencia | Existe hoy |
|---|---|---|
| Prerequisitos (python/uv/redis/kafka) | **OPERATOR/OS** (instalador verifica) | parcial (validator L5 en runtime) |
| Python/uv & virtualenv | **INSTALLER en modo verificar** (invoca `uv sync`) | sí (manual README) |
| Configuración | **INSTALLER+CONFIG**: `cp .env.example .env` (create-if-missing), `cp deploy/host.env.example deploy/host.env` | sí (manual) |
| Directorios/permisos | **INSTALLER** (`mkdir -p data_platform logs`, perms) | parcial |
| Docker/Compose | **DOCKER COMPOSE** (`docker compose up -d`) | sí (manual/README) |
| Kafka | **DOCKER COMPOSE** (broker) + **INSTALLER** (provision topics idempotente) | provision ya existe (script) |
| Redis | **DOCKER COMPOSE** | sí |
| systemd | **SYSTEMD** (`install_systemd.sh`) | sí |
| Observabilidad | **DOCKER COMPOSE** + `deploy/monitoring` | sí |
| Health/readiness | **INSTALLER/OPERATOR** (`health_check.sh`) | sí |
| Migración/rollback | **APPLICATION/CI** (no en install) | parcial (CD broken §20) |
| Idempotencia | **INSTALLER** (diseño, §14) | parcial |

**Anti-God-Script:** el instalador-orquestador debe ser un "sequence & verify", no un implementador. Cada responsabilidad tiene ya dueño; el orquestador solo las invoca en orden y reporta `PASS/FAIL/SKIP` (DSL heredado de `check_production_gates.py` G1–G11).

---

## 14. Idempotencia — `./install.sh` ejecutado 1/2/10 veces

| Paso | 1ª | 2ª | 10ª | Problema |
|---|---|---|---|---|
| `uv sync` | crea .venv | no-op | no-op | OK (idempotente) |
| `cp .env.example .env` | crea | **sobrescribe** (si sin protección) | idem | **ROMPE config de usuario** → debe ser create-if-missing |
| `docker compose up -d` | crea | reconcilia | reconcilia | mayormente idempotente; `build` en CI re-crea |
| `install_systemd.sh` | render+install | overwrite+reload | idem | idempotente SALVO ediciones manuales (`.env` línea) que pierde → drift |
| `systemctl enable/start` (si orquestador lo hace) | enable+start | **duplica/error** (already enabled/started) | idem | requiere guards `is-enabled/is-active` |
| `provision_kafka_topics.py` | crea | no-op | no-op | OK (idempotente explícito) |
| `health_check.sh` | read-only | read-only | read-only | OK |
| `deploy_ocm.sh --apply` (CD) | — | — | — | NO idempotente por diseño (es deploy, con `.prev_sha` para rollback) |

**Veredicto:** un `install.sh` debe nacer con modo **default `--check` (read-only)** y `--apply` mutante; reglas "create-if-missing" para `.env`/host.env; guards systemctl; y pasos SKIP con razón. 10 ejecuciones = estabilidad si se respetan esas reglas. Evidencia de que esto importa: el drift `.env` del rendered (§5) ocurrió precisamente porque la regeneración es idempotente **respecto al template** pero no respecto a ediciones del SO.

---

## 15. Seguridad

- **Service users:** units corren con `User=orangemusic` (unprivileged) + hardening (`NoNewPrivileges`, `ProtectSystem=strict`, `PrivateTmp`). Correcto.
- **Secrets:** `.env` gitignored (gracias) pero con secreto de API real BYBIT y JWT placeholder; `deploy/host.env` trackeado (violación policy aunque sin secrets hoy). Systemd `EnvironmentFile=` usa host.env (sin secrets) → OK; si un día se meten secrets en host.env, quedarían trackeados y en `/etc/systemd/system` (drift).
- **Superficie de ataque propuesta vs actual:**
  - Instalador + systemd: **neutro** si no añade sudo innecesario (install units requiere root; `--user` evita) y si los secrets siguen fuera del repo.
  - **Empeora** si `install.sh` empieza a gestionar docker (socket, `docker group`) o secrets.
  - **Mejora** si se corrige el tracking de host.env y se documenta perms 600 para `/etc/ocm/*.env` (hoy install no setea perms en EnvironmentFile destino; los units usan `EnvironmentFile=${OCM_REPO_ROOT}/deploy/host.env` en el repo, no `/etc/ocm`).
- **Kafka PLAINTEXT** y **Grafana en 127.0.0.1** (compose:228) — ya mitigado publicación; deuda documentable.
- **Caveat `--cfg job`** (Hydra pre-Pydantic expone secrets en stdout): base.yaml lo advierte. Un orquestador no debe pipearlo a logs.

---

## 16. Resource constraints — single host pequeño (OrangeHouse)

| Carga | Evidencia |
|---|---|
| Infra compose (Kafka+ZK, Redis, Prom, Alert, Push, Grafana, Loki, Promtail) | ~6-8 contenedores, JVM de Kafka+ZK dominante; limits: kafka (cpus? mem) y redis (`--maxmemory 256mb`) |
| Servicios nativos: market-data (1 proceso uvicorn+3 tasks asyncio), streaming (WS+cryptofeed), CLI | ejecutado por python .venv — overhead bajo |
| Docker sintitle | vanlig over-head de un engine |

Juicio: **la propuesta A+B es la más ligera para un host pequeño** (aplicación nativa, infra aislada). "Full container" duplicaría la JVM de Kafka + el runtime Python en contenedor (otro proceso lógico innecesario para 3 tasks async en un solo box) y añade 2 contenedores de servicio sin gain para un host con 1 usuario. La variante "external managed Kafka/Redis" reduce footprint pero añade red/latencia y coste — solo se justifica si OCM deja de ser single-host. (Sin métricas/hardware real recolectados en este host en la auditoría — estado recurso "no medido"; decisión basada en topología.)

---

## 17. Comparación formal

Criterios con EVIDENCIA → TRADE-OFF → CONSECUENCIA (no puntuaciones).

| Criterio | Actual (A+B, 2026) | Propuesta (nativa+systemd+docker) | Full container |
|---|---|---|---|
| Simplicidad | Evidencia: override comenta "el pipeline corre en la maquina local"; 3 scripts ops. **TRADE-OFF:** 2 runtimes del deployment (host+compose). **CONSECUENCIA:** simple hoy; conocimiento reps en README. | Reduce mezcla con orquestador único. **CONSEQUENCIA:** -1 superficie si es facade. | Evidencia: compose ya tiene profile; pero trading/portfolio "NO EJECUTABLE". **CONSECUENCIA:** scafaring + 4 imágenes a mantener. Peor hoy. |
| Reproducibilidad | Infra reproducible (versionado compose); app depende de `.venv` del host (repro por uv.lock). | Igual; orquestador `--check` cierra lag manual. | Mayor para la app; requiere dockerfile por BC. |
| Seguridad | Hardening systemd ✓; secrets gitignored salvo host.env tracking ✗; Kafka PLAINTEXT ✗. | Igual; riesgo si orquestador gestiona secrets/docker. | Menor aislamiento host; misma exposure inf. |
| Observabilidad | Metrics solo prod (`metrics.enabled`); tracing fail-soft disabled; pushgateway usado por streaming. | Sin cambio necesario. | Suficiente igual; extra exporter. |
| Debugging | nativo = log systemd/journal + loguru; SSOT. | igual; `--check` ayuda diagnóstico. | +1 indirección (logs docker), peor en single-host. |
| Resource usage | Óptimo para host pequeño (§16). | igual. | +overhead sin gain. |
| Startup / Restart | Units por proceso; B-59 NO_VERIFICADO (reinicio). | Igual; orquestador formaliza enable/start + restart check. | restart policy docker; healthchecks presentes. |
| Deployment | CD broken hoy (§20). | Reparable; requiere decision CD. | Deployment de imágenes más natural — pero sin artifact pipeline hoy. |
| Rollback | `.prev_sha` emergente (trabajado en tree); rollback capsβα: imagenes locales. | CD `.prev_sha` + evidencia (trabajado). | rollback de tags GHCR — pero compose usa `ocm_*:latest` local (no afecta). |
| Idempotency | Parcial (§14). | Mejorable con diseño default-check. | Compose parte idempotente. |
| CI/CD | ocm-ci robusto; ocm-cd placeholder no-operativo. | Se apoya en ocm-ci sin cambios grandes. | Requiere convertir CI en build-and-push GHCR (no existe ahora). |
| Dev experience | nativo ist schnell. | +comodidad de `--check`. | peor para iteración (rebuild). |
| PROD suitability | Coherente single-host. | mejor coherente. | Prematura (sin NIVEL 1 de trading/portfolio). |
| Architecture alignment | app = contratos; tooling = docker exec (matiz). | Preserva. | Rompe "monolito procesos"—consistente con ADR-0024 futuro, no actual. |
| Coupling | app ↔ infra por env ✓; tooling ↔ docker (health/gates) ✗. | Preserva app; deja tooling acoplado salvo refactor de health. | app en contenedores → coupling docker-in (ends) aún más. |
| Operational complexity | Media (3 scripts + manual). | Baja-Media con orquestación. | Alta hoy (4 servicios, images locales, sin CI build). |
| Documentation burden | Alto (conocimiento en README/ADRs con drift). | --check auto-documenta estado. | Alto (docker-specific docs). |
| Failure isolation | Proceso nativo cae → systemd lo reinicia; infra cae → depende de docker restart. | igual. | aislado por contenedor, pero todo en un host. |

**Lectura:** en la mayoría de criterios "Propuesta" ≈ "Actual" (porque la propuesta es el actual formalizado); "Full container" gana solo en "Deployment/rollback de imágenes" — y aún así carece de CD de imágenes real. Ninguna columna domina con evidencia contundente; la más débil hoy es **Full container** por estado de los BCs y por single-host.

---

## 18. Principios arquitectónicos — válido vs moda

| Principio | Status | Veredicto |
|---|---|---|
| Separation of Concerns / Single Source of Truth | Ya gobernado (Hydra, env_vars, import-linter BC-NN, templates VCS). | **Válido — no debe violarse añadiendo `deploy/config/`.** |
| Ports & Adapters / Clean Architecture | Implementado en BCs; aplicación ignora docker. | **Válido.** El matiz: el tooling de health usa docker exec — violación mitigable refactorizando health a endpoints/contratos. |
| Dependency Inversion | Ports `MetricsPusherPort`, `StorageFactoryPort`, etc. | **Válido.** |
| Configuration as Code | Hydra+env; config operacional en `deploy/`. | Válido; unidad correcta de "operational config versionada" = monitoring/systemd/host.env.example. |
| Immutable infrastructure | NO aplica a single host con data lake persistente (estado en volúmenes). | **Moda para este caso** — no perseguir. |
| Reproducibilidad | uv.lock + compose pinned. | Válido. |
| Idempotency / fail-fast / fail-soft | En runtime (validator L5, dry_run, fail-soft observabilidad). | Válidos — extender a instalador (`--check`). |
| Least privilege | Units unprivileged + hardening. | Válido — preservar. |
| Observability | Fail-soft correcto (§10). | Válido — no hacerla hard-dependency. |
| Explicit lifecycle management | systemd por proceso; B-59 pendiente. | Válido — cerrar B-59 en vez de añadir otro mecanismo. |

El "instalador orquestador" y el "drift detection" no son moda: están motivados por hechos del repo (drift real en rendered, gates con rutas rotas, B-59 sin verificar).

---

## 19. Documentación vs realidad

| Tema | Estado | Clasificación |
|---|---|---|
| `deploy_ocm.sh` en ADR-0037 | Contexto dice "inexistente" (auditoría 08-19); hoy existe. | **OBSOLETO** (contexto vite datado) |
| Puertos health en ADR-0037 / deploy_ocm.sh | 9091/9090/9093 vs compose 9090/9093/9091 | **DRIFTED / CONTRADICTORY** (§20 F-5) |
| `systemd_reinicia_correctamente` | B-59 PENDIENTE; ADR-0022:323 "Sin unit systemd activo" | **NO VERIFICADO / OBSOLETO** (units activas hoy) |
| `.env.example` vs `.env` (puertos KAFKA/ALERT) | invertidos 9094/9093 | **DRIFTED** (§20 F-10) |
| DEFAULT kafka bootstrap | base.yaml localhost:9092 vs schema.py/production kafka:9092 | **INCOHERENTE (default 3 sitios)** |
| Units templates vs rendered | línea `EnvironmentFile .env` solo en rendered | **DRIFTED** (reproducido de hecho) |
| `metrics.py` ocm/observability | Placeholder v0.1.0 con headers antiguos "core/observability" | **OBSOLETO/MISSING cleanup** |
| README (install/run/ports) | Coherente con realidad (uv sync, compose up, run.sh, tabla de puertos) | **IMPLEMENTED** |
| compose `trading/portfolio` | "NO EJECUTABLE" comentado — coherente con ausencia de main | **DOCUMENTED correcto** |

---

## 20. Revalidación adversarial de findings previos (F-3, F-5, F-10, F-12)

No se dan por válidos; se revalidan contra HEAD `b8b13240` + working tree.

### F-3 — `check_production_gates.py:251` invoca ruta inexistente
- **Estado:** **CONFIRMADO** (valido hoy).
- **Evidencia:** `scripts/check_production_gates.py:251` → `subprocess.run(["bash", "scripts/install_systemd.sh", "--verify-only"], ...)`. `scripts/install_systemd.sh` NO existe en disco (`ls` → "No existe el fichero"); el script vive en `deploy/scripts/install_systemd.sh`. El `"evidencia"` del gate G4 (línea 102) dice `./deploy/scripts/install_systemd.sh` — interno inconsistente.
- **Impacto:** G4 degrada en silencio: deja de "regenerar y después verificar" y solo hace `systemd-analyze verify` del rendered presente. Con rendered válido en disco → PASS aunque el "render fresh" fallaría.
- **Reproducibilidad:** ejecutar `uv run python scripts/check_production_gates.py --mode gate-dev` con units renderizadas en disco → G4 PASS sin correr el instalador. Causa raíz: path hardcoded divergente.
- **Gravedad:** MEDIA (gate de producción que miente levemente; no bloquea deploy ni lo verifica del todo).
- **Recomendación:** cambiar a `deploy/scripts/install_systemd.sh` y/o re-generar rendered antes de verify.

### F-5 — Puertos health errados en `deploy_ocm.sh`
- **Estado:** **CONFIRMADO, pero rebajado de impacto.** Los puertos `PROMETHEUS_PORT=9091`, `ALERTMANAGER_PORT=9090`, `PUSHGATEWAY_PORT=9093` (deploy_ocm.sh:49-52) y el header (líneas 11-12) son INCORRECTOS vs compose (prometheus 9090, alertmanager 9093, pushgateway 9091; `.env` real: alertmanager 9094). **PERO** esas variables nunca se usan: `wait_for_health()` solo hace `docker exec ocm_redis redis-cli ping` y `docker exec ocm_kafka kafka-broker-api-versions` (líneas 169-200). Son **config muerta + header engañoso**, no fallo funcional.
- **Reproducibilidad:** el CD real no llegaría nunca a usar esos puertos; pero un lector de ADR-0037/header los copia mal.
- **Gravedad:** BAJA (higiene/misinformation), no MEDIA como se registró.
- **Recomendación:** corregir header+ADRs o eliminar las 4 vars muertas.

### F-10 — `.env.example` vs `.env` puertos invertidos
- **Estado:** **CONFIRMADO.**
- **Evidencia:** `.env` real → `ALERTMANAGER_HOST_PORT=9094`, `KAFKA_HOST_PORT=9093`. `.env.example` → `ALERTMANAGER_HOST_PORT=9093`, `KAFKA_HOST_PORT=9094` (con nota que explica la colisión F-029/B-45: "9094 (no 9093) para NO colisionar con ALERTMANAGER_HOST_PORT=9093"). El comment del ejemplo refleja INVERTIDA la solución aplicada.
- **Impacto:** un operador que sigue el `.example` termina con KAFKA en 9094 y ALERT en 9093 (test de integración exige 9093; listeners cambian). Reproducible al `cp .env.example .env`.
- **Gravedad:** MEDIA (error de configuración probable, no de código).
- **Recomendación:** alinear `.example` a la convención real (ALERT=9094, KAFKA=9093) y conservar la nota.

### F-12 — Dockerfile refiere `market_data.orchestration.entrypoint`
- **Estado:** **RECHAZADO — inválido en el estado actual.** No hay evidencia de ese símbolo.
- **Evidencia:** Dockerfile (working tree y HEAD) → `CMD ["python", "-m", "market_data.main"]` (línea final); `market_data.main` existe y su `__main__` ejecuta `uvicorn.run(..., workers=1)` (`packages/market_data/main.py:622-632`). Compose market-data override `command: python -m market_data.main`. Nada en el repo refiere `market_data.orchestration.entrypoint`.
- **Impacto:** finding nulo; el Dockerfile es ejecutable para market-data.
- **Gravedad:** N/A (descartado).
- **Nota adversarial:** el único matiz real aún válido es que trading/portfolio del compose referencian `python -m trading.main/portfolio.main` INEXISTENTES — pero eso ya está documentado como scaffolding "NO EJECUTABLE" en el propio compose y en ADR-0024 (no es un bug oculto).

### Revisión colateral (nuevo/ampliado)
- **F-NEW-1 (severidad ALTA):** `deploy_ocm.sh` y `ocm-cd.yml` constituyen un CD **no operacional**:
  1. `docker compose -f docker-compose.yml up -d --build "${ARTIFACT_SHA}"` (deploy_ocm.sh:161) — `ARTIFACT_SHA` es el SEGUNDO argumento posicional de `docker compose up [SERVICES...]` → compose 2.x lo interpreta como nombre de servicio; **no existe** servicio con ese nombre (los servicios de compose son `market-data`, `trading`, …). Falla o queda no-op.
  2. Las imágenes reales de compose son construidas localmente (`build: {context, dockerfile, target}` + `image: ocm_market_data:latest`) — el script espera `ghcr.io/orangecashmachine/ocm:${SHA}` solo para `docker image inspect`/`save`; el `up` usa la del compose. **Nunca se despliega el "artifact".**
  3. `ocm-cd.yml` corre en runner **ubuntu-latest** (GitHub ephemeral) — sin conexión al host OrangeHouse, sin SSH/self-hosted. El `deploy` muta el runner, no la producción.
  4. `.prev_sha` actualizado en tree pero **el fichero no existe** en `deploy/` hoy; evidencia JSON escribiría a `$DEPLOY_DIR` del runner.
  - **Reproducibilidad:** `ARTIFACT_SHA=abc ../deploy/scripts/deploy_ocm.sh ACCEPT` → falla en `docker compose up`/selector. **Causa:** CD fue codificado para un modelo no existe (imagen GHCR + mismo host), mientras el runtime real es nativo. **Recomendación:** decidir CD (ver §22): o (a) deprecar el camino Docker-CM y conectar CD real vía SSH/self-hosted hacia systemd native (recomendado), o (b) construir publicar imágenes GHCR reales y corregir el selector. ADR-0037 debe pasar de Propuesto a Aprobado/rechazado explícitamente.
- **F-NEW-2 (MEDIA):** `health_check.sh` L2/L3 y gates G5/G6 dependen de `docker exec ocm_kafka/ocm_redis` + puerto interno 9092: contamina el contrato de salud con el runtime Docker. Si Kafka/Redis se externalizan, todo el L2/L3 y G5/G6 se rompen. (Evidencia: health_check.sh:36-68; gates `docker exec ocm_kafka`.)
- **F-NEW-3 (MEDIA):** systemd `After=docker.service` + `EnvironmentFile=…/deploy/host.env` acoplan units a (a) Docker presente y (b) rutas del repo (WorkingDirectory), lo que impide "instalar" OCM fuera del checkout. Para single-host es defendible; documentar como decisión.
- **F-NEW-4 (BAJA):** `run.sh` no expone `ocm-api`/`streaming` vs `[project.scripts]` completo — subconjunto documentado pero fácil de desconocer.

---

## 21. Governance de deploy/

- **Ownership:** ownership implícito bueno por script (comments de cabecera + gates que los consumen: G4→install_systemd, G7→health_check, G11→check_production_gates). Pero **sin regla formal** en `docs/architecture/GOVERNANCE.md` (grep deploy/systemd/install → sin hits) ni en import-linter (deploy/ no es raíz package).
- **Cambios/revisión/CI:** monitoring y systemd pasan por revisión de PR normal; scripts pasan por shellcheck/CI; no hay test de contrato de los .service (solo `systemd-analyze verify` bajo demanda de G4 — y este con el bug F-3).
- **Evidencia/drift/versionado/rollback:** rendered gitignored (bien); sin evidencia de instalación; sin mecanismo de drift ni rollback de units.
- **¿Regla formal? → Sí, mínima.** Evidencia justificante: (1) policy `deploy/host.env` violada (trackeada); (2) F-3 por path inconsistente del gate; (3) drift real de units. La regla NO debe ser un linter nuevo sino: (a) `CODEOWNERS`/guard doc-comment "deploy/ = operacional, versionar solo templates/example", (b) gitignore/untrack de `host.env`, (c) test en CI que re-render y compare con rendered en repo o `--verify-only` (gate G4 reparado). No crear una regla de arquitectura import-linter para deploy/ (no es package).

---

## 22. Decisión arquitectónica

### ¿Nuestra propuesta debe adoptarse?

## **ADOPTAR PARCIALMENTE.**

Justificación en evidencia:

1. **ADOPTAR — Orquestador fino `deploy/install.sh` con modos `--check`/`--dry-run`/`--apply`:** resuelve un problema real y verificado: conocimiento instalador repartido (README/install_systemd/health/provision/gates), B-59 sin cerrar, y `--check` único reutilizable. **Pero debe delegar 100% en los componentes existentes** y reutilizar el DSL PASS/FAIL de `check_production_gates.py` (hoy ya cubre G1..G11). Alternativa igual de válida: no crear script nuevo y documentar "gate-dev" como la API operacional. Coste de cambio: un script bash ~150 líneas + tests.
2. **ADOPTAR — Drift detection mínimo en `install_systemd.sh --verify-only`** (re-render en memoria y diff template↔rendered↔SO) — motivado por un drift real ya materializado. 10-15 líneas. Coste bajo.
3. **ADOPTAR — Reparar el camino CD A REALIDAD** (F-NEW-1): o despliegue ssh/self-hosted hacia systemd o deprecar. Evidencia: CD ya codificado pero inoperante; ADR-0037 "Propuesto"; B-57 registra "implementado" — **contradice la realidad** (risk re-asing a IMPLEMENTADO-PARTIAL en tracking es responsabilidad del repo, no de esta auditoría).
4. **RECHAZAR — Directorios `deploy/runtime`, `deploy/infrastructure`, `deploy/config`, `deploy/health`:** no responden a una responsabilidad sin dueño (janvier §25). Mueven archivos, crean riesgo de segundo SSOT (`config`), o duplican (runtime). Organización ≠ arquitectura (§26).
5. **RECHAZAR — `install.sh` en raíz como implementación:** solo aceptado como facade de 1 línea (`exec ./deploy/install.sh "$@"`) si se quiere descubrimiento estándar; de lo contrario basta documentar `deploy/install.sh` en README. Un `install.sh` con lógica propia reintroduciría exactamente el patrón de bug F-3.
6. **RECHAZAR — `ocm-trading.service` / `ocm-portfolio.service` hoy:** no existe el entrypoint (§5). Mientras, ADR-0024 NIVEL 2.
7. **RECHAZAR — "Full container" como objetivo inmediato:** SIN evidencia de ventaja para single-host, y con los BCs trading/portfolio sin NIVEL 1. Dejarlo como fase de ADR-0024 cuando existan los 3 microservicios.
8. **MANTENER — todo lo que la propuesta "novedoso-only" daría por nuevo pero ya existente y correcto:** templates VCS + rendered gitignored, units preexistentes con hardening, health_check L1-L4, config-guard, provision idempotente, contrato por env (app ignora Docker), fail-soft observabilidad, gates G1-G11, dry_run.

**Qué sobrevive de la propuesta:** orquestador delegado (`--check/--apply`), drift-check de systemd, formalización de boundary (gobernanza no código), CD reparado o deprecado, facades raíz.
**Qué se descarta:** 4 directorios nuevos, root install con lógica, units prematuros.
**Qué se modifica:** `deploy/install.sh` reusa gates en vez de recrearlos; health_check hacia endpoints (desacoplar docker exec) cuando infra externa se considere.
**Dependencias faltantes para implementar:** un staging real o dry-run de producción (para cerrar B-59 con prueba de reinicio, desplazable), decisión de modelo CD, y (si CD docker) pipeline GHCR que no existe.
**ADR necesario antes de cambios estructurales:** (1) ADR "modelo de runtime canónico" (native-first como operativo + Docker-solo-infra confirmado; implicaciones ADR-0037); (2) ADR "boundary deploy/ y orquestador (`--check/--apply`), drift-detection y DOI de units"; (3) ADR o enmienda sobre fabricación del CD (deprecar Docker-CM → ssh/self-hosted, o construir GHCR+selector). Estos cambios estructurales NO deben implementarse sin pasar por el mecanismo formal del repo (ADR + tracking).

---

## 23. Modelo objetivo (basado en evidencia, no ideal)

```
operador
   │
   ▼
deployment API (documentada):  uv run python scripts/check_production_gates.py --mode gate-dev
   │                          (o facade raíz: ./install.sh →deploy/install.sh)
   ▼
INSTALL/VERIFY (deploy/scripts):
   ├── 1. runtime     → uv sync --group dev                  (IDEMPOTENTE)
   ├── 2. config      → qso- .env(.example) / host.env(.example) create-if-missing + validar L5
   ├── 3. infra       → docker compose up -d   + config-guard + provision_kafka_topics.py (idempotente)
   ├── 4. systemd     → deploy/scripts/install_systemd.sh --verify-only  [drift-check: template↔rendered↔SO]
   │                    deploy/scripts/install_systemd.sh [ --user | system: install+daemon-reload ]
   ├── 5. observab    → compose (monitoring provisionado) + metrics/tracing por entorno
   └── 6. health      → deploy/scripts/health_check.sh L1-L4  → exit 0/1/2
   ▼
RUNTIME (systemd nativo, procesos reales hoy: market-data, streaming)
   │   api (gateway opcional) / ocm (CLI pipeline) / paper-live (CLIs embebidos)
   ▼
APPLICATION (contratos, sin Docker): KAFKA_BOOTSTRAP_SERVERS · REDIS_HOST/PORT/DB/PASSWORD · PUSHGATEWAY_URL · OCM_STORAGE__* · OCM_*
   ▼
INFRASTRUCTURE CONTRACTS (Docker hoy — externalizable mañana sin tocar código): Kafka·Redis·Prom·Grafana·Loki·Alertmanager·Pushgateway
```

Fronteras separadas explícitas:
- **REPOSITORY:** código + `config/` (Hydra) + `deploy/systemd/templates` + `deploy/monitoring` + `deploy/scripts` + `deploy/host.env.example` + `.env.example`. (NUNCA `.env`, nunca `host.env`, nunca `rendered/`.)
- **OS:** `.venv` + units en `/etc/systemd/system` + `/etc/ocm/*.env` (o `~/.config/systemd/user`) — derivados del repo, regenerables.
- **CONTAINER RUNTIME:** solo infra + observabilidad (compose), con config-guard y healthchecks.
- **APPLICATION:** procesos nativos, sin Docker, con fail-fast config + fail-soft observabilidad.
- **INFRAESTRUCTURE** (contratos): endpoints env-configurables.
- **OBSERVABILIDAD:** prometheus/grafana/loki/push/alert — provisionados por compose, alimentados por la app (métricas), por sistemas (node/push), por logs (promtail).

Diagramas guía ya presentes en el doc: reconstrucción §1.3 (flujo operador), §17 matriz, y los 2 diagramas de flujo de instalación/systemd en §13-§14 (§5) — el modelo objetivo no añade capas; ordena las que existen.

---

## 24. Migración (roadmap — NO implementados)

Solo fases justificadas por hechos:

- **Fase 0 — Documentación/ADR:** ADR modelo canónico (native-first), ADR boundary/orquestador/drift, decisión CD (deprecar Docker-CM o construir GHCR+selector). Actualizar ADR-0037, ADR-0024, tracking B-57 (IMPLEMENTADO-PARTIAL), B-59 (PENDIENTE→plan).
- **Fase 1 — Foundation:** reparar F-3 (ruta gate), F-5 (puertos/muertos), F-10 (.env.example), F-6 (untrack host.env), incoherencia default Kafka, cleanup `ocm/observability/metrics.py` placeholder. Sin cambios estructurales.
- **Fase 2 — Installer:** orquestador delegado `--check/--dry-run/--apply` que invoca componentes existentes y reutiliza DSL de gates; facade raíz opcional de 1 línea.
- **Fase 3 — Systemd:** drift-check en `--verify-only` (template↔rendered↔SO); formalizar enable/start con guards; cerrar B-59 con prueba real de reinicio (o mover a Docker restart case si se decide infra docker-only).
- **Fase 4 — Health/readiness:** desacoplar health de `docker exec` (endpoints/contratos) para que L2/L3 sean runtime-agnostic; opcional `/ready` con deps en market-data.
- **Fase 5 — Drift detection:** del systemd extendido a `.env`/host.env (validación de ejemplo vs real).
- **Fase 6 — CI/governance:** gate G4 reparado + test de paridad templates; CODEOWNERS deploy/; policy host.env; (opcional) job CI que ejecute `install.sh --check` en runner sin mutar nada.
Fases posteriores (solo cuando ADR-0024 NIVEL 1 exista): microservicios containerizados o nativos con units por BC + CD de imágenes si se decide GHCR.

---

## 25. Regla anti-sobreingeniería (aplicada — ≥2 respuestas débiles ⇒ NO recomendar la pieza)

| Pieza propuesta | ¿Qué problema concreto? | ¿Dónde existe hoy? | ¿Mecanismo previo? | ¿Por qué no basta? | ¿Qué duplica? | ¿Coste? | Verdict |
|---|---|---|---|---|---|---|---|
| `deploy/runtime/` | runtime explícito | venv/pyproject | uv | ya funciona | pyproject | medio | **NO** |
| `deploy/infrastructure/` | organizar infra | compose raíz | compose | funciona | nada (mueve) | alto (rutas) | **NO** |
| `deploy/config/` | config unificada | Hydra + host.env | ambos | no requiere.mezcla | Hydra | alto (SSOT) | **NO** |
| `deploy/health/` | probes | health_check.sh | script | funciona | nada (mueve) | bajo | **NO** |
| ocm-trading/portfolio.service | supervisión | — | — | sin entrypoint | — | alto | **NO** |
| `install.sh` raíz lógica | descubrimiento | run.sh+README | facade | no requiere más | run.sh | bajo | **NO** (solo facade 1 línea opcional) |
| `deploy/install.sh --check/--apply` | estado instalación verificable; cerrar B-59 | gates G1–G11 | check_production_gates | gates no "instalan", manual | — | bajo (delega) | **SÍ** |
| drift-check systemd | SO≠repo (drift real ocurrió) | rendered | install_systemd | no detecta drift | — | bajo | **SÍ** |
| reparar CD | CD inoperante (F-NEW-1) | deploy_ocm.sh/ocm-cd.yml | ADR-0037 | no encaja runtime real | — | medio (decisión) | **SÍ** |

---

## 26. Organización ≠ arquitectura

- **`deploy/` nuevo con 8 carpetas ¿mejora arquitectura? → No.** Mejora la navegación; por eso se valoró §4 por directorio. La única *responsabilidad arquitectónica* nueva que la propuesta introduce es el **orquestador + drift detection** (responsabilidades reales sin dueño). El resto es file-moving.
- La evidencia histórica del repo (proliferación de `guard`/audit scripts; `infrastructure/` raíz como paquete vacío tracked legacy; `.bak`/`.swp` en disco) refuerza que en este repo **más archivos = más superficie**, no mejor diseño. La propuesta de "7 subdirectorios + 2 scripts instaladores" no rompe esa tendencia.

---

## 27. Docker ≠ infraestructura

- Punto clave de la propuesta ("OCM NO depende de Docker como mecanismo de ejecución") → **VERDADERO y ya verificado:** la aplicación (ocm/, packages/, apps/) solo lee contracts de env; nada refiere container names/networks. `docker-compose.yml` ni se importa.
- **Matiz honesto (contra-evidence parcial):** el *tooling operativo* sí lo hace: `health_check.sh:36-68` (`docker exec ocm_kafka/ocm_redis`, `kafka-get-offsets` interno en `localhost:9092`) y gates G5/G6 (`docker exec ocm_kafka/ocm_redis`). Por tanto "OCM no conoce Docker" precisa: **OCM-la-aplicación no lo conoce; OCM-el-operaciones sí**. Si la infra se externaliza, este tooling se rompe (F-NEW-2). La propuesta no contempla este desacoplamiento; debe incorporarlo en la etapa health.
- Consecuencia operacional: la elección "infra en Docker vs managed" es **independiente de la aplicación** (solo toca tooling de health) — eso confirma el desacoplamiento general y limita el coste del cambio futuro.

---

## 28. Resultado final

### Executive conclusion
OCM ya opera (Mayo-Ago 2026) como modelo A+B (servicios nativos uv/systemd + infra y observabilidad Docker en un host único), con la aplicación correctamente desacoplada por contratos de entorno y una gobernanza CI excepcionalmente madura (49 contratos BC, gates G1–G11, guards AST, ADRs). La propuesta bajo ataque **no describe un modelo objetivo nuevo** — describe, con añadidos organizativos, el modelo que el repo ya tiene. Su componente con valor real es un **orquestador de instalación/verificación delegado + drift-detection de systemd + reparación del CD**. Sus componentes estructurales (directorios nuevos) deben rechazarse por incumplir la regla anti-sobreingeniería y por crear riegos de SSOT duplicado. Decision: **ADOPTAR PARCIALMENTE.**

### Current architecture
Nativa + systemd (market-data, streaming) + infra/obs en Docker + CLI/API nativos; CD codificado pero inoperante; orquestación repartida en README + 3 scripts + gates.

### Proposed architecture under attack
Root `install.sh` → `deploy/install.sh` con 5 etapas; `deploy/{runtime,infrastructure,systemd,monitoring,config,health,scripts}`; systemd versionado + drift — evaluado pieza a pieza (§3-§7, §25): adoptar solo orquestador delegado + drift + reparaciones.

### Evidence
Ver §1–§20 (paths/líneas citados en cada afirmación).

### Findings
- F-3 (CONFIRMADO, MEDIA) — ruta `scripts/install_systemd.sh` inexistente en gate G4 (`check_production_gates.py:251`).
- F-5 (CONFIRMADO-bajado, BAJA) — puertos muertos/engañosos en deploy_ocm.sh:49-52.
- F-10 (CONFIRMADO, MEDIA) — `.env.example` puertos invertidos vs `.env`.
- F-12 (RECHAZADO) — Dockerfile CMD válido (`python -m market_data.main`); find inválido.
- F-NEW-1 (ALTA) — CD no operacional (selector compose inválido + imágenes locales + runner ephemeral).
- F-NEW-2 (MEDIA) — health/gates acoplados a `docker exec`.
- F-NEW-3 (MEDIA) — units acopladas a `docker.service` y rutas repo (decisión single-host).
- F-NEW-4 (BAJA) — `run.sh` subconjunto vs `[project.scripts]`.
- F-4 (de previo, re-validado como hebra) — drift rendered vs templates ya materializado.
- F-6 (CONFIRMADO) — `deploy/host.env` trackeado contra policy.
- F-9 (CONFIRMADO) — default Kafka Pydantic `kafka:9092` (3 sitios de default).

### Trade-offs
Ver matriz §17. Resumen: propuesta≈actual en la mayoría de criterios; full-container solo gana en deployment-de-imágenes y pierde en todo lo demás para single-host/BC inmaduros.

### Recommended target architecture
§23. IDENTIDAD: consolidar A+B con orquestador delegado, drift-check, CD reparado o deprecado, health runtime-agnostic, y reglas mínimas de gobernanza.

### What NOT to implement
`deploy/{runtime,infrastructure,config,health}/`; `install.sh` con lógica (solo facade opcional); `ocm-trading.service`/`ocm-portfolio.service`; full-container ahora; `deploy/config/`; REDIS_URL; ningún linter nuevo para deploy.

### What should be formalized first
ADR modelo canónico (native-first + Docker-solo-infra); ADR boundary/orquestador/drift; decisión CD; reparación F-3/F-5/F-10/F-6; untrack host.env; tracking B-57/B-59.

### Required ADRs
1. Runtime canónico (native-first; implicaciones para ADR-0037). 2. Boundary deploy/ + orquestador + drift detection + DOI de units. 3. CD: deprecar Docker-CM ↔ construir GHCR+selector (decidir).

### Migration roadmap
Fases 0–6 en §24 (solo las justificadas).

### Open questions
- ¿Staging vs dry-run de producción para validación de reinicio? (B-59)
- ¿CD se repara (ssh/self-hosted→systemd) o se depreca hasta microservicios NIVEL 1?
- ¿OCM saldrá del host único (→managed Kafka/Redis/obs)? Condiciona F-NEW-2/F-NEW-3 y staging.
- ¿Quién registra B-57 como IMPLEMENTADO-PARTIAL y B-59 con plan? (offuid: esto es del equipo, no del auditor.)

### Final decision
**ADOPTAR PARCIALMENTE.** Adoptar: orquestador delegado `--check/--apply`, drift-check systemd, reparaciones de CD/gates/ports/env, gobernanza mínima. Rechazar: directorios nuevos, install raíz con lógica, units prematuros, full-container.

### Tabla resumen

| Área | Estado actual | Problema | Recomendación | Evidencia |
|---|---|---|---|---|
| Runtime | Nativo uv/systemd (2 units) | B-59 sin verificar reinicio; units acopladas a docker/rutas | Cerrar B-59; decidir coupling | §5, §19, tracking.yaml:3146 |
| Instalación | Manual (README+3 scripts) | Conocimiento repartido; sin estado verificable | Orquestador delegado `--check/--apply` | §3, §13 |
| Orquestación gates | `check_production_gates.py` G1–G11 | Ruta G4 rota | Reparar ruta; reutilizar DSL | §20 F-3 |
| Dir nuevos propuestos | No existen | Organización≠arquitectura; SSOT duplicado | Rechazar (`runtime`/`infra`/`config`/`health`) | §4, §25 |
| systemd SSOT | templates VCS + rendered gitignored | Drift real materializado | drift-check en `--verify-only` | §5, §19, §20 F-4 |
| CD | deploy_ocm.sh + ocm-cd.yml | Inoperante (selector, imágenes, runner) | Decidir: ssh/self-hosted↔deprecar | §20 F-NEW-1, ADR-0037 |
| Health | health_check L1-L4 | `docker exec` en L2/L3 | Refactorizar a contratos | §27, F-NEW-2 |
| Kafka | compose; contract env | PLAINTEXT; default 3 sitios | unificar default; doc TLS cuando host>local | §8, §20 F-9 |
| Redis | contract REDIS_*; no REDIS_URL | correcto | mantener | §9 |
| Observabilidad | fail-soft correcto | metrics placeholder legacy | cleanup `ocm/observability/metrics.py` | §10, §19 |
| Config | Hydra + host.env + .env | `.env.example` invertido; host.env trackeado | F-10, F-6 | §20, §11 |
| Docs | ADR-0022/0024/0037 | ADR-0037 contexto obsolete; puertos | Actualizar ADRs | §19 |

---

## 29. Criterio de calidad (WHY / EVIDENCE / TRADE-OFF / ALTERNATIVE / COST)

Cumplido a lo largo del documento para cada recomendación mayor (ver §3.2-3.4, §4, §5, §22, §25, §27). Todo juicio tiene `EVIDENCE` (path:line), `TRADE-OFF` explicit, `ALTERNATIVE` (p. ej. prescindir de `install.sh` y usar gate-dev como API; manter compose en raíz en vez de mover), y `COST OF CHANGE` (bajo/medio/alto con razón). Sin opiniones "porque sí": la decisión final descansa en hechos verificados (§20, §22, §17).

---

## 30. Finalización

### Estado git (inicial y final — este auditor no alteró nada)

Estado **inicial** (registrado antes de inspección):
```
 M .github/workflows/ocm-cd.yml
 M Dockerfile
 M deploy/monitoring/grafana/dashboards/ocm_pipeline.json
 M deploy/monitoring/grafana/provisioning/datasources.yml
 M deploy/scripts/deploy_ocm.sh
 M packages/market_data/infrastructure/quality/anomaly_registry.py
 M policies/semgrep/architecture.yaml
 M uv.lock
?? docs/audits/AUDIT_OCM_deployment-runtime-architecture_2026-09-19.md
```
(8 archivos modificados **ya presentes antes** — preservados, jamás tocados; 1 untracked previo de auditoría previa.)

### `git status --short` (final, tras única escritura permitida)

```
 M .github/workflows/ocm-cd.yml
 M Dockerfile
 M deploy/monitoring/grafana/dashboards/ocm_pipeline.json
 M deploy/monitoring/grafana/provisioning/datasources.yml
 M deploy/scripts/deploy_ocm.sh
 M packages/market_data/infrastructure/quality/anomaly_registry.py
 M policies/semgrep/architecture.yaml
 M uv.lock
?? docs/audit/deployment-runtime-architecture-adversarial-audit.md
?? docs/audits/AUDIT_OCM_deployment-runtime-architecture_2026-09-19.md
```

- **Archivo creado/modificado por esta auditoría:** ÚNICAMENTE `docs/audit/deployment-runtime-architecture-adversarial-audit.md` (untracked nuevo). Ningún otro archivo creado/modificado/borrado/movido.
- **Confirmación:** NO modifiqué código/tests/CI/Docker/systemd/ADRs/tracking. NO commits (`git status` no muestra ningún staging), NO push, NO merge, NO reset/clean, NO instalé paquetes, NO inicié/detuve servicios.
- Los 8 ` M ` y el `?? docs/audits/...` eran **preexistentes** y quedan **intactos** (regla 19-20).

### Hallazgos principales (≤15 bullets)
1. La "arquitectura propuesta" **ya es, en un 90%, el modelo actual** (nativo uv/systemd + infra Docker + contratos env) — la pieza nueva es el orquestador.
2. El hueco real: **no existe una API operacional de instalación/verificación**; el conocimiento está en README + 3 scripts + gates (G1–G11 ya cubren casi todo).
3. **adoptar**: orquestador delegado `deploy/install.sh --check/--apply` reutilizando `install_systemd.sh`/`health_check.sh`/`provision_kafka_topics.py`/DSL de `check_production_gates.py`.
4. **rechazar**: `deploy/{runtime,infrastructure,config,health}/` — organización, no arquitectura; crean SSOT duplicados (§25).
5. **rechazar** `install.sh` con lógica (solo facade 1 línea opcional); `run.sh`+`[project.scripts]` ya son la SSOT.
6. **F-3 (confirmado)** `check_production_gates.py:251` llama a `scripts/install_systemd.sh` inexistente → gate G4 degradado.
7. **F-5 (rebajado)** puertos 9091/9090/9093 en deploy_ocm.sh son **config muerta** (ningún consumer), no error funcional.
8. **F-10 (confirmado)** `.env.example` invierte ALERT/KAFKA (9093/9094) vs `.env` real.
9. **F-12 (rechazado)** Dockerfile `CMD python -m market_data.main` es válido y `market_data.main` tiene `__main__` uvicorn (workers=1).
10. **F-NEW-1 (ALTA)** CD inoperante: `docker compose up -d --build ${SHA}` (selector de servicio inexistente), imágenes locales `ocm_*:latest` vs artifact GHCR, y runner `ubuntu-latest` sin conexión a producción.
11. **F-NEW-2** health/gates acoplados a `docker exec ocm_*` → se romperían si infra se externaliza.
12. **Drift real** unidades: `deploy/systemd/rendered/*.service` lleva `EnvironmentFile .env` que el template no tiene → SSOT template ya desincronizado del SO.
13. **host.env trackeado** contra su propia policy ("NUNCA commitear"); contenido hoy sin secrets.
14. Default Kafka inconsistente (3 sitios: base localhost:9092 / schema+prod kafka:9092) y broker **PLAINTEXT**; observable-por-defecto disabled (fail-soft correcto).
15. **Decisión:** **ADOPTAR PARCIALMENTE** — orquestador delegado + drift-check + reparación CD/gates/env; rechazar directorios nuevos y units `ocm-trading/portfolio`; NO full-container hoy (trading/portfolio sin NIVEL 1, ADR-0024).

**DETENERSE — sin implementación.** Este documento es el único artefacto enviado por la auditoría.

---

## 31. Misión adicional (2026-09-19) — Validación adversarial de DR-1…DR-5 y modelo canónico

> **Tipo:** Segunda iteración adversarial, read-only, previa a cualquier implementación de DR-1…DR-5.
> **Objetivo:** determinar si DR-1…DR-5 son las correcciones correctas y cuál debe ser el **modelo canónico** de deployment/runtime de OCM, atacando tanto el estado actual como las propias correcciones propuestas.
> **Actualizaciones de evidencia en directo (red recogida en esta iteración):**
> - `systemctl is-active ocm-market-data.service` → **inactive**; `systemctl is-active ocm-streaming.service` → **active** (verificado en host).
> - `docker ps` → contenedores activos: `ocm_market_data`, `ocm_redis`, `ocm_kafka`, `ocm_zookeeper`. **El pipeline market-data corre HOY como contenedor Docker**, no bajo su unit systemd nativa (que existe pero está inactiva). El streaming corre bajo systemd. **Bifurcación real y verificada en runtime**, no solo en codebase.
> - CLI nativas de Kafka/Redis **NO instaladas en el host**: `kafka-broker-api-versions`, `redis-cli`, `kcat` → `not found`. `docker exec` es la única vía de CLI existente en el host.
> - `.venv` contiene `aiokafka` (pyproject:103) y `redis>=7.4.0` (pyproject:102; `redis.__version__` == 7.4.0 en `.venv`). Un check de contrato (no mecanismo) es técnicamente factible vía el venv de la aplicación.
> - El adaptador Kafka real es `aiokafka` (no confluent-kafka; `confluent_kafka` no está instalado). Delego de los CLIs de container → librería de app.

---

## 32. Revalidation of DR-1...DR-5

### DR-1 — Corregir `.env.example` (F-10) + alinear default Kafka (3 sitios) + puertos muertos de `deploy_ocm.sh` (F-5)

- **¿Qué problema resuelve?** Config local/host errónea o confusa: `.env.example` invierte Alertmanager/Kafka (F-10); default de `KAFKA_BOOTSTRAP_SERVERS` repetido e inconsistente en 3 sitios (`config/base.yaml:46` localhost:9092, `config/env/production.yaml:43` y `ocm/config/schema.py` `kafka:9092`); `deploy_ocm.sh:49-52` declara puertos de observabilidad invertidos/muertos (9091/9090/9093 vs compose 9090/9093/9091).
- **¿Cuál es la evidencia?** `.env` real `ALERTMANAGER_HOST_PORT=9094`/`KAFKA_HOST_PORT=9093`; `.env.example:33,40` 9093/9094 (invertidos). `docker-compose.yml` (pushgateway 9091, prometheus 9090, alertmanager 9093, grafana 3000, kafka EXTERNAL 9093). `deploy_ocm.sh` no usa jamás esas 4 vars (`wait_for_health` solo `docker exec` redis/kafka).
- **¿Es realmente un problema de deployment?** Parcial: es un problema de **config operacional** (defaults de infra por env), no de runtime de la aplicación. La app lee el contrato env; el error está en la plantilla/ejemplo y en convenciones de puerto.
- **¿Es un bug aislado?** No: son 3 síntomas de una misma causa (ausencia de convención/SSOT de valores por defecto de topología). Cada uno es arreglable por separado, pero sin design root force reaparecen.
- **¿Es consecuencia de un problema arquitectónico?** Sí: no existe **un** punto de verdad para convenciones de infra por entorno (`.env.example`, `host.env.example`, compose defaults, Hydra defaults, Pydantic defaults coexisten sin dueño).
- **¿La solución propuesta elimina la causa raíz?** Solo si incluye el **SSOT de convenciones** (p.ej. `deploy/host.env.example` como tabla de puertos + un test de paridad CI que compare `.env.example`↔`host.env.example`↔compose defaults). Corregir solo los valores elimina síntomas, no la causa.
- **¿O simplemente parchea el síntoma?** En su formulación estricta (cambiar líneas), es un **parche**; para eliminar raíz requiere añadir el guard de paridad.
- **¿Qué alternativa existe?** (a) generar `.env.example`/defaults desde una única fuente (templating); (b) test CI `check_ssot_ports` que falle si `.env.example` y compose divergen; (c) dejar solo un default en Hydra y eliminar defaults de schema/compose.
- **¿Qué archivos/mecanismos quedarían duplicados?** Hoy mismo ya hay ~5 sitios que declaran topología de puertos (`deploy_ocm.sh`, `.env.example`, `docker-compose.yml`, `host.env.example`, Hydra). La corrección no los elimina; solo los alinea. Eliminarlos = arquitectura (transformación, no DR-1).
- **¿Qué riesgo introduce la corrección?** Bajo (solo cambia defaults/ejemplos). Riesgo no nulo: si alguien ya sigue el `.example` actual, el cambio de puerto altera listeners Kafka — por eso debe coordinarse con la nota F-029/B-45.
- **Clasificación: NECESARIA.** Con caveat explícito: el DR-1 como "alinear valores" es necesario e inofensivo; la raíz (SSOT de convenciones) es diseño aparte que NO debe estirar el alcance de un commit de fix.

### DR-2 — Reparar gates (F-3 path + G5/G6 sin docker exec, F-NEW-2)

- **¿Qué problema resuelve?** Gate G4 degradado (ruta `scripts/install_systemd.sh` inexistente → `check_production_gates.py:251`) y gates/health acoplados a `docker exec ocm_kafka/ocm_redis`.
- **¿Cuál es la evidencia?** `check_production_gates.py:251` ejecuta `bash scripts/install_systemd.sh --verify-only` con `cwd=ROOT`; el script real es `deploy/scripts/install_systemd.sh`. `health_check.sh:36-68` usa `docker exec` en L2/L3; gates G5 (`:281-303`), G6 (`:334-356`), G8 (`:391-410`) usan `docker exec ocm_*`.
- **¿Es realmente un problema de deployment?** Sí: G4 es el gate de deployment units; G5/G6 son gates de runtime. El path bug es bug aislado de CI; el asunto docker-exec es acoplamiento de health al mecanismo.
- **¿Es un bug aislado?** El bug de ruta: **sí, aislado y trivial** (mecánica). El acoplamiento docker-exec: **no**, es diseño (los CLIs solo existen dentro de los containers; ver §37).
- **¿Es consecuencia de un problema arquitectónico?** El acoplamiento sí: health/gates verifican el **mecanismo** (contenedor) en lugar del **contrato** (env + servicio). Es la misma clase de "Docker≠implementación" del §27, extendido al tooling.
- **¿La solución propuesta elimina la causa raíz?** Path: sí. G5/G6 sin docker exec: sí, si se hace un chequeo por contrato. Importante: **no basta cambiar `docker exec ocm_kafka` por otra CLI nativa** — el host no tiene CLIs instaladas. La vía viable es check vía venv de app (`aiokafka` + `redis` presentes) leyendo `KAFKA_BOOTSTRAP_SERVERS`/`REDIS_HOST/PORT/DB/PASSWORD`, o endpoints HTTP `/ready` si market-data los amplía (hoy `/ready` no pinea Kafka/Redis — `main.py:495-519` solo valida ctx).
- **¿O simplemente parchea el síntoma?** Path: parche correcto y suficiente. G5/G6: eliminar docker-exec **sí** toca la raíz si se hace por contrato; si solo se cambian container names a otra constante, es parche.
- **¿Qué alternativa existe?** (a) check por venv de aplicación (`uv run python -c` con aiokafka/redis); (b) ampliar `/ready` de market-data y un endpoint de infra donde compose haga healthcheck (ya los hay a nivel container); (c) dejar `docker exec` pero **documentarlo como decisión** mientras infra sea local (mantiene la deuda). La opción (a) es preferible: usa el mismo contrato que la app y es runtime-agnostic.
- **¿Qué archivos/mecanismos quedarían duplicados?** Hoy G6 duplica L2 de `health_check.sh` (redis/kafka ping). Tras la corrección, un único `health_check.sh` por contrato + gates que deleguen en él (DSL) elimina duplicación.
- **¿Qué riesgo introduce la corrección?** Medio: mover G5/G6 a checks vía venv requiere que `.venv` exista y que `aiokafka` conecte usando env reales (si el env del runner de CI no tiene infra, G5/G6 seguirán siendo "dev-only"). Riesgo de dejar G5/G6 en CI gate-ci: ojo, CI solo corre G1/G2/G3/G10/G11 (gate-ci), G5/G6 no van en CI salvo gate-dev manual.
- **Clasificación: NECESARIA** (path, trivial) **+ NECESARIA PERO REQUIERE DISEÑO** (desacoplar health por contrato). Se emite como una sola fila con dos partes: la parte mecánica es independiente y debe ir primero; la parte de diseño (health-check runtime-agnostic) requiere decisión ADR.

### DR-3 — Reparar/reemplazar CD (F-NEW-1): SSH → `deploy/scripts/install.sh --apply` renderizado desde template

- **¿Qué problema resuelve?** Un CD inoperante: `ocm-cd.yml` corre en runner `ubuntu-latest` efímero (sin conexión a OrangeHouse); `deploy_ocm.sh:161` ejecuta `docker compose up -d --build ${ARTIFACT_SHA}` (selector de servicio inexistente; los servicios de compose son `market-data/trading/portfolio`); las imágenes de compose son locales `ocm_*:latest` (no existe artifact GHCR ni `docker push` en CI — grep ghcr solo aparece en deploy_ocm.sh:35); `.prev_sha` no existe en el repo.
- **¿Cuál es la evidencia?** `ocm-cd.yml` → `runs-on: ubuntu-latest`; `deploy_ocm.sh:34-36,160-165,224-239`; `docker-compose.yml` servicios `market-data/trading/portfolio` + imágenes locales; `git ls-files deploy/` sin `.prev_sha`; ADR-0037 (Propuesto, aunque tracking B-57 lo registra "implementado").
- **¿Es realmente un problema de deployment?** Sí, es el corazón del problema de deployment. Y no es un bug: **el CD está escrito para un modelo objetivo (Docker+artifact GHCR) que no es el runtime real (nativo systemd)**. Es consecuencia directa de que el ADR-0037 se escribió asumiendo deploy contenedor.
- **¿Es un bug aislado?** No — es un modelo entero irreconciliable con el estado actual.
- **¿Es consecuencia de un problema arquitectónico?** Sí: el ciclo de vida "instalar/preparar" nunca se definió para el runtime nativo; el CD quedó acoplado a una imagen que no se publica.
- **¿La solución propuesta elimina la causa raíz?** "SSH → `install.sh --apply`" es la dirección correcta **solamente si el orquestador existe** y el canal a producción es real. Hoy (a) no existe `install.sh`, (b) el canal SSH/self-hosted tampoco está configurado. Implementado tal cual, movería la rotura de un sitio a otro.
- **¿O simplemente parchea el síntoma?** La propuesta como "cambiar el comando" parchea; como "redefinir el modelo de CD" ataca la raíz.
- **¿Qué alternativa existe?** (a) **Deprecar el CD Docker/GHCR** (documentar `workflow_dispatch` como no-operativo, ADR de decisión) y operar con `deploy/install.sh --check/--apply` manual vía SSH — cero infra nueva, cero riesgo; (b) self-hosted runner en OrangeHouse que ejecute gates + install nativo; (c) construir de verdad el pipeline GHCR+selector (alto coste, prematuro para un solo host sin consumo de imagen).
- **¿Qué archivos/mecanismos quedarían duplicados?** Si `install.sh` re-implementa lo que ya hacen `install_systemd.sh`/`health_check.sh`/`provision_kafka_topics.py`, duplicaría. Si delega 100%, no.
- **¿Qué riesgo introduce la corrección?** Alto si se intenta "reparar el CD" antes que existan orquestador y canal de despliegue; medio si se ejecuta el orden correcto (orquestador → canal → conectar CD); nulo si la alternativa elegida es deprecar.
- **Clasificación: NECESARIA PERO REQUIERE DISEÑO** — la dirección es correcta pero está **bloqueada** por el orquestador y por la elección de canal (SSH manual vs self-hosted). La decisión ADR debe preceder. La opción "deprecar" debe mantenerse explícitamente como alternativa válida (bajo sobreingeniería, §25).

### DR-4 — Gobernanza: ADR del orquestador delegado + plan migración + PR B-59 en tracking + untrack host.env + reconciliar `[project.scripts]` vs `run.sh`

- **¿Qué problema resuelve?** Reglas de deploy sin formalizar: `deploy/host.env` **trackeado** pese a `.gitignore:74` y a la cabecera "NUNCA commitear" (verificado `git ls-files deploy/`); B-59 `systemd_reinicia_correctamente` NO_VERIFICADO (tracking.yaml:3146-3158, 3381) pero ADR-0022 addendum decía "Sin unit systemd activo" — hoy las units renderizadas existen; `run.sh` expone solo ocm/live/paper vs `[project.scripts]` con 5 entradas (F-NEW-4); sin regla de CODEOWNERS/CI para deploy/.
- **¿Cuál es la evidencia?** `git ls-files deploy/` lista `deploy/host.env`; `.gitignore` línea 74; `tracking.yaml` B-59; `run.sh:31-41` vs `pyproject.toml:176-181`; `GOVERNANCE.md` sin menciones de deploy/systemd (ver §21).
- **¿Es realmente un problema de deployment?** Es gobernanza de deployment — normativa, no funcional. Pero capital: sin ella, los DR-2/DR-3 reintroducen drift.
- **¿Es un bug aislado?** No: son 4 políticas débilmente aplicadas con distinta naturaleza (secrets-fear, verificación de runtime, SSOT de entrypoints, ownership).
- **¿Es consecuencia de un problema arquitectónico?** Sí, parcialmente: el boundary `deploy/` es de facto, no formal (§4/§21); y el "who owns lifecycle" nunca se declaró (raíz de B-59).
- **¿La solución propuesta elimina la causa raíz?** host.env: `git rm --cached` + mantener en gitignore **sí** elimina de inmediato (mientras no haya secrets en el archivo). B-59: cerrarlo exige **prueba empírica de reinicio** (prueba de procedure), no solo una PR de estado. run.sh: o bien expone las 5 entradas o bien se declara explícitamente "dev facade = subconjunto" con referencia SSOT. Orquestador: ADR sí es la vía formal.
- **¿Qué alternativa existe?** (a) mínima: untrack host.env + fix `run.sh` + trabajo de estado B-59, sin ADR nuevo (el ADR llegaría cuando existan unidades/estructura); (b) completa: ADR de boundary+orquestador como paso previo a DR-2/DR-3. La (b) es necesaria si se van a crear unidades/scripts nuevos.
- **¿Qué duplica?** Ningún mecanismo; únicamente cambia estado formal.
- **¿Qué riesgo introduce?** Bajo. Único riesgo: si se "cierra" B-59 en tracking sin prueba de reinicio real, se convierte en otro findoor de confianza (regla: no marcar HECHO sin evidencia; mantener NO_VERIFICADO hasta test real o documentar decisión de modelo alternativo).
- **Clasificación: NECESARIA** (componentes de limpieza y gobernanza mínima) **con un componente NECESARIA PERO REQUIERE DISEÑO** (el ADR de orquestador, que se ciñe a la decisión §22 y se coordina con DR-2/DR-3).

### DR-5 — "Trueno" nativo para market-data (unit systemd de market-data / mismo proceso :8001)

- **¿Qué problema resuelve?** (Leído como propuesta de servicio systemd nativo para market-data, pues la unit es parte del modelo canónico; reformulada por §9 como "¿debe market-data ser un proceso persistente supervisado?").
- **¿Cuál es la evidencia?** **El "trueno" YA EXISTE**: templates `deploy/systemd/templates/ocm-market-data.service.template` y rendered `ocm-market-data.service` con `ExecStart=.venv/bin/python -m market_data.main` (unit validada y instalada). **Pero está inactivo hoy**: `systemctl is-active ocm-market-data.service` → inactive; y el pipeline corre como contenedor `ocm_market_data` (docker ps). Por tanto el problema real no es "crear el servicio" sino **alinear el runtime**: hoy la bifurcación está materializada (contenedor activo + unit inactiva).
- **¿Es realmente un problema de deployment?** Sí, pero no es un problema de ausencia de pieza: es **ambigüedad de modelo** (¿market-data nativo o contenedor?) y de **estado divergente** (unit inactiva mientras el container corre).
- **¿Es un bug aislado?** No: es la consecuencia visible de que coexistir 3 formas de arrancar market-data (run.sh/`[project.scripts]`, systemd unit, Docker CMD) sin regla de canonicalidad.
- **¿Es consecuencia de un problema arquitectónico?** Sí: ausencia de declaración formal "quién arranca market-data en producción" (ADR de runtime canónico, §45). ADR-0022 solo cubre streaming; para market-data no hay ADR de lifecycle (solo ADR-0024 lo tipa NIVEL 1 con entrypoint).
- **¿La solución propuesta elimina la causa raíz?** Crear otra unit NO (ya existe); **declarar canonicalidad + alinear el runtime** (o unit activa con container parado, o documentar container-activo + unit-inactiva como estado transitorio divergente a corregir) SÍ.
- **¿Qué alternativa existe?** (a) **Canonical systemd nativo** (propuesto): unit activa, container `ocm_market_data` parado/retirado — coherente con infra compose y con B-59 por verificar; (b) **Canonical Docker**: dejar market-data en container como norma (entonces la unit nativa de todo el capítulo §5 sobra y el modelo es B) — contradice ADR-0022/ADR-0037 single-host native; (c) **ambivalencia mantenida**: documentarlo como riesgo explícito (NO recomendado, ya ha materializado drift).
- **¿Qué queda duplicado?** La unit nativa y el container ofrecen el mismo :8001 y leen los mismos volumes; correr ambos = colisión de puertos (riesgo real si alguien hace `systemctl start` mientras el container corre).
- **¿Qué riesgo introduce?** Medio: si se activa la unit sin detener el container, doble bind :8001 + doble consumer Kafka (split-brain de offsets). La corrección debe ser transición controlada (parar contenedor/unit, decidir el nativo, verificar B-59).
- **Clasificación: NECESARIA PERO REQUIERE DISEÑO** — reformulada de "crear trueno" a "canonicalizar el arranque de market-data y reconciliar el estado divergente (unit vs container)"; NO es NO JUSTIFICADA porque hay evidencia de divergencia actual verificada, pero su implementación debe incluir la transición y la prueba B-59.

---

## 33. Root causes

Causas raíz identificadas (heredadas y nuevas):

1. **No existe una "deployment API" canónica.** El conocimiento de instalación/verificación vive en README, 3 scripts y 11 gates sin un dueño que garantice coherencia. → origina F-3 (rutas divergentes), F-4 (drift de units), B-59 (nunca verificado: sin un `--check` de reinicio). (Evidencia: §2, §3.)
2. **El mecanismo de infraestructura se usa como contrato de salud.** Health/gates verifican `docker exec ocm_kafka/ocm_redis` (el cómo) en vez del contrato env (el qué). → F-NEW-2, y bloquea la migración de infra a managed. (Evidencia: §27, health_check.sh:36-68, gates G5/G6/G8.)
3. **CD diseñado contra un modelo objetivo inexistente.** ADR-0037 (Propuesto) especifica artifact GHCR + compose-tag; el runtime real es nativo systemd. El CD quedó como código muerto que "describe el futuro" en lugar de desplegar el presente. → F-NEW-1, F-5 (puertos muertos del mismo pensamiento). (Evidencia: ADR-0037, deploy_ocm.sh, ocm-cd.yml.)
4. **Ausencia de convenciones/sistema racional de defaults de infra.** 5+ sitios declaran topología (`.env.example`, `host.env.example`, compose defaults, Hydra defaults, Pydantic defaults) sin dueño. → F-10, F-9, F-5. (Evidencia: §11, §32 DR-1.)
5. **Lifecycle de la aplicación sin propietario declarado para market-data.** ADR-0022 cubre streaming; market-data quedó en la intersección native-vs-container sin decisión. → bifrcación live (unit inactive + container activo). (Evidencia: §31, ADR-0024 NIVEL 1.)

---

## 34. Current deployment graph (verificado en runtime 2026-09-19)

```
operador
   │
   ▼
entry point (5 CLI de [project.scripts] + run.sh facade + systemd ExecStart + Docker CMD)
   │
   ▼
deployment orchestrator (NO EXISTE — hoy: README + 3 scripts + gates G1–G11 como "orquestador  imparcial de facto")
   │
   ▼
runtime (bifurcado de hecho, verificado):
   ├── systemd nativo:  ocm-streaming.service  → ACTIVE   (.venv/bin/python -m app.cli.streaming_hydra)
   │                    ocm-market-data.service → INACTIVE (unit existe, no en uso hoy)
   └── Docker compose:  ocm_market_data (container, ACTIVE — :8001)
                        ocm_redis / ocm_kafka / ocm_zookeeper (infra, ACTIVE)
   │
   ▼
infrastructure (Docker compose: Redis, Kafka+ZK, monitoring; contractos por env)
   │
   ▼
health/readiness (health_check.sh L1–L4 + gates G5/G6/G7/G9 + Docker healthchecks por servicio)
```

**Bifurcaciones verificadas:**
- **A (ejecución de la app):** `install.sh…(futuro)` / `run.sh` / `uv run ocm|streaming|ocm-api` / `systemd unit` / `Docker CMD`. Hoy conviven systemd (streaming) y Docker (market-data). **No hay una sola vía canónica.**
- **B (instalación):** README (manual) vs `install_systemd.sh` (systemd) vs `deploy_ocm.sh` (CD, inoperante) vs `check_production_gates.py` (verificación). Ninguno es "el" instalador.
- **C (mecanismo):** nativo (`docker exec` vs CLI nativa). El host no tiene CLIs → solo Docker.

**Source of truth actual:** el **repositorio Git es el único SSOT material** (templates VCS, monitoring, host.env.example, ports en compose). El SO (`/etc/systemd/system`, rendered gitignored, `.env`, `host.env`) es derivado; el estado live (qué vía corre) NO tiene SSOT — es ambigüedad operacional.

---

## 35. Current sources of truth

| Capa | Fuente canónica | Derivado | Local-only | Verificación |
|---|---|---|---|---|
| Entrypoints | `[project.scripts]` (pyproject:176-181) | consola `uv run` | `run.sh` (subconjunto documentado) | SSOT declarada; F-NEW-4 |
| Config app | Hydra `config/` layers + `ocm/config/env_vars.py` (SSOT nombres) | Pydantic `AppConfig` | overlays CLI | fail-fast L5 |
| Topología host | `deploy/host.env.example` | `deploy/host.env` (trackeado — VIOLACIÓN) | `.env` (gitignored) | — |
| Puertos/convenciones | 5 sitios sin dueño (DR-1) | — | — | — |
| Units systemd | `deploy/systemd/templates/*.template` (VCS) | `rendered/` (gitignored) → `/etc/systemd/system` | — | drift ya ocurrió (F-4) |
| Monitoring | `deploy/monitoring/*` VCS | bind mounts en compose | — | `config-guard` valida |
| Infra | `docker-compose.yml` + `.env` (ports) | containers | — | healthchecks |
| CI vars | inline en workflows | — | `artifact_sha` input manual | — |
| Secrets | `.env` (gitignored); **no** en host.env | — | — | `git ls-files deploy/` → host.env trackeado (deuda) |

**Conclusion:** `deploy/host.env` debe **permanecer fuera de Git** (junto a `.env`); debe sanearse de lo que ya sea local-only. Permanecer como plantilla (`.example`) en VCS es correcto. Generarse desde `.example` con `--check` del orquestador (DR-4) es la vía correcta.

---

## 36. Deployment coupling findings

1. **Tooling ↔ Docker:** `health_check.sh:36-68` y gates G5/G6/G8 usan `docker exec ocm_kafka/ocm_redis` + puerto interno 9092. (F-NEW-2, §37.)
2. **Units ↔ Docker / repo:** `After=docker.service` + `EnvironmentFile=${OCM_REPO_ROOT}/deploy/host.env` + `WorkingDirectory=${OCM_REPO_ROOT}` + `ReadWritePaths=${OCM_REPO_ROOT}/...`. OCM nativo queda atado a la ubicación del repo y a la presencia de Docker local. Defendible single-host (F-NEW-3), pero es decisión, no diseño.
3. **Default Kafka en 3 sitios** (base `localhost:9092` / prod `kafka:9092` / schema `kafka:9092`) + `.env` real `localhost:9093`. Foot-gun (§20 F-9) y fuente de F-10.
4. **Broker PLAINTEXT** y Grafana en 127.0.0.1 (compensator: publish solo local). Deuda de seguridad si el host fuera alcanzable (§15, §8).
5. **Un único SSOT para el estado "qué corre hoy": NO EXISTE.** Bifurcación verifiable (unit inactive/container active).
6. **run.sh como segundo entrypoint** (subconjunto) echa de la SSOT de entrypoints (F-NEW-4).
7. **`deploy/host.env` trackeado** pese a ignore y policy (F-6).

---

## 37. Docker como contrato o como implementación (investigación F-NEW-2)

**Preguntas respondidas con evidencia:**

- **¿`docker exec ocm_*` representa (a) una herramienta operacional legítima o (b) acoplamiento incorrecto al mecanismo de deployment?**

**Es (b) con matiz operacional real: es el ÚNICO mecanismo disponible hoy.** El host **no tiene** `kafka-broker-api-versions`, `redis-cli`, ni `kcat` (verificado: `which` → not found en los tres). Los CLIs de Kafka/Redis viven dentro de las imágenes. Por tanto, mientras infra esté en Docker local, `docker exec` es la supraintementa práctica. El acoplamiento incorrecto NO es "usar docker" per se; es **que el contrato de salud (¿Kafka/Redis responden?) dependa del runtime Docker** en lugar del contrato de la aplicación (`KAFKA_BOOTSTRAP_SERVERS`, `REDIS_HOST/PORT/DB/PASSWORD`). Si infra pasa a managed/externa, `L2/L3` y G5/G6/G8 dejan de poder verificarse.

- **¿G5/G6 pueden comprobar el mismo contrato sin Docker?**
  - **CLI real:** NO directamente (no hay CLIs nativas). Podría instalarse `kafka-client`/`redis-tools` en el host → deuda de dependencia del host (otro mecanismo).
  - **HTTP health/readiness:** NO hoy completo: market-data `/health` es liveness puro (`main.py:465-492`); `/ready` (`495-519`) valida ctx pero **NO pinea Kafka/Redis** — no cubre G6. Ampliar `/ready` (o un endpoint de deps) haría G6 un `curl` runtime-agnostic.
  - **Process exit code:** cubre solo G5 (market-data y streaming viven/reviven), no G6.
  - **Application command (venv de app):** **SÍ factible hoy**: `.venv` contiene `aiokafka` y `redis` 7.4.0 (verificado imports). Un check `uv run python - <<PY` que lea env contract y haga `producer.metadata()`/`redis.ping()` sería runtime-agnostic (funciona contra Docker, managed, o cualquier broker) y REEMPLAZA a `docker exec`. Es la opción alineada con `ports`/contratos del repo.
  - **Service contract:** los healthchecks de compose ya modelan el "servicio sano"; `health_check.sh` L2/L3 es quien debería hablar por contrato, no por container name.

**Veredicto:** el mecanismo correcto es **contrato por venv de la aplicación** (o endpoint `/ready` ampliado), manteniendo `docker exec` solo como fallback opcional/env `HEALTH_PROBE_MECHANISM=docker|venv|http`. No introducir CLIs de host.

---

## 38. systemd vs Compose analysis

Modelos comparados sin puntuación; evidencia repo + runtime:

**MODELO A (actual-nativo + infra compose): systemd → OCM; Docker Compose → Kafka/Redis/Obs**
- startup: units nativas (`Type=simple`) supervisadas; infra compose `up`. Evidencia: templates, compose.
- restart: systemd `Restart=on-failure, RestartSec=10` en units; compose `restart: unless-stopped`/`on-failure` + healthchecks. Evidencia: templates, compose.
- health: `health_check.sh` L1–L4 (usa docker exec L2/L3 — acoplamiento, §37). Docker healthchecks por servicio.
- readiness: units no tienen exec readiness; `/ready` de app no pinea deps; HTTP /health cubre liveness.
- logs: `journalctl` (units) + `docker logs` (infra). Dos canales.
- resource limits: systemd MemoryLimit/CPUShares NO están en los templates (limite no configurado por unit); compose sí declarar limits para infra (redis maxmemory, kafka limits). Evidencia: templates sin Resource control; compose deploy.resources.
- security: hardening por unit (`NoNewPrivileges`, `ProtectSystem`, `PrivateTmp`); infra aislada por Docker.
- deployment: CD inoperante (F-NEW-1); manual vía README + install_systemd.sh.
- rollback: units → re-render/overwrite; infra compose → idempotente; CD `.prev_sha` ausente.
- reproducibility: uv.lock + templates VCS + compose pinned images.
- debugging: journal + loguru en unidades; `docker exec`/`docker logs` en infra.
- CI: market-data/streaming no se ejecutan en CI (solo Kafka service container); gates gate-ci no requieren infra.
- staging: NO existe staging declarado.
- production: A ES el estado real (parcial: market-data hoy en container — divergencia live).
- complexity: dos gestores de proceso (systemd + docker) en el mismo host.

**MODELO B (all-container): Docker Compose → OCM + Kafka + Redis + Obs**
- startup: compose up --profile microservices (services trading/portfolio "NO EJECUTABLE": ADR-0024 NIVEL 3; market-data profile opt-in con override que comenta "el pipeline corre en la maquina local").
- restart: compose restart policies; pierde supervisión granular por unit (debe modelarse healthchecks).
- health: Docker healthchecks; `docker inspect` para estado; `health_check.sh` L2/L3 funciona (docker exec) — pero es Docker-dentro-Docker (los healthchecks se harían dentro del runtime que las pruebas quieren validar).
- readiness: /ready del container; sin cambios de producto.
- logs: `docker logs` unificado (simple).
- resource limits: compose deploy.resources para todo (más control global declarado).
- security: aislamiento por imagen/sandbox; expose seg menor en host; secretos vía env.
- deployment: es el modelo que el CD fantasma asume (ADRs). Pero **el artifact GHCR no existe ni hay push** — el "deploy por imagen" es deuda, no camino.
- rollback: tags de imagen; pero hoy compose usa `ocm_*:latest` local (carrollback = rebuild, no tag).
- reproducibility: compose pinned + Dockerfile — máx. Para trading/portfolio bloqueado por falta de entrypoint NIVEL 1.
- debugging: `docker exec` — buena (los CLIs están dentro). Para un solo host, indirección.
- CI: no hay build-and-push de imágenes en CI (job inexistente); F-NEW-1.
- staging: igual que A (no existe).
- production: **el estado HOY de market-data es B** (container activo) — pero fue efectos colaterales, no decisión (unit native inactiva).
- complexity: un solo gestor de runtime (menor cognitivo), pero requiere CD de imágenes real para ser coherente.

**MODELO C (todo systemd/native): systemd → OCM + infra nativa**
- startup: units para kafka/redis/obs — **no existen templates ni units para infra**; requerirían mantenimiento completo de Kafka/ZK/Redis nativos (instal, config, TLS, actualizaciones). Sin evidencia en repo.
- restart/health/logs/límites: systemd para todo (un solo gestor), pero Kafka/ZK/Redis bajo systemd exige trabajo considerable (no hay que systemd-only infra en compose).
- reproducibility: drift de versiones de servicio del OS (flujo manual); pierde lo pins de compose.
- CI/staging/production: imposible sin añadir infra nueva que no aporta evidencia.
- djustificación: nula frente al actual (compose infra ya funciona y está versionado).

**MODELO D (híbrido alternativo): A con infra externalizable (managed Kafka/Redis) u OCM en contenedor solo cuando existan unidades NIVEL 1**
- startup: la app ya es contract-only (§1.3/§27); mover Kafka/Redis a managed NO toca la app (solo health/gates, §37). Persistencia en volúmenes locales vs remota.
- restart/health/logs: dependen de provider; health por contrato (necesario resolver §37 antes).
- security: managed TLS/SASL (elimina deuda PLAINTEXT); coste para un solo host dudoso.
- production: señala el camino cuando OCM deje el single-host; hoy sin necesidad evidenciada.

**Conclusión (solo evidencia):**
- El repositorio nace y versiona **infra en compose** (MODELO A en infra) — nada respalda C.
- La aplicación tiene **units systemd nativas** y **Docker profile** para app (A y B conviven en archivos). El runtime live HOY es **A (streaming) + B (market-data)**, i.e., una violación de coherencia operacional, no un modelo elegido.
- B completo (todo en containers) está bloqueado por **trading/portfolio sin entrypoint** y por **la inexistencia de artifact/deploy real (F-NEW-1)**. B solo para market-data no aporta ganancia frente a native para single-host (mismo bind :8001, mismo loop async, duplicación de bind de volumes).
- A es el modelo mas coherente con: ADR-0022 (systemd para streaming), host.env, ADR-0037 (single-host), la composición de infra en compose y la naturaleza de la app (procesos ligeros con loop asyncio). El shape objetivo canónico debe partir de A, con B como fase futura solo cuando haya NIVEL 1 para los 3 servicios y un flujo de imágenes real.

---

## 39. Canonical deployment model

```
REPOSITORY (Git — única fuente de verdad)
   └─ code (BCs) · config/ (Hydra) · pyproject (entrypoints) · deploy/
        ├─ systemd/templates (SSOT units)
        ├─ monitoring (SSOT obs)
        ├─ scripts (install_systemd · health_check · deploy_ocm[deprecable/SSH] )
        └─ host.env.example (+ .env.example) — topología de host, sin secrets
   ▼
DEPLOYMENT API (de deploy): deploy/install.sh --check / --dry-run / --apply
   └─ invoca (NUNCA re-implementa): uv sync · env create-if-missing · docker compose up -d
        · install_systemd.sh · provision_kafka_topics.py · health_check.sh · gates G1–G11 (DSL heredado)
   ▼
RUNTIME (systemd nativo — owner del lifecycle de procesos de la app)
   ├─ ocm-market-data.service (unit SSOT; estado objetivo: ACTIVE)
   ├─ ocm-streaming.service (unit SSOT; ACTIVE hoy)
   └─ CLI/API opcionales: uv run ocm | ocm-api (no unit hoy)
   ▼
APPLICATION (procesos nativos; contrato-only, sin Docker)
   └─ KAFKA_BOOTSTRAP_SERVERS · REDIS_HOST/PORT/DB/PASSWORD · PUSHGATEWAY_URL · OCM_STORAGE__* · OCM_*
   ▼
INFRASTRUCTURE CONTRACTS (Docker Compose hoy — externalizable mañana sin tocar app)
   └─ Kafka+ZK · Redis · Prometheus · Grafana · Loki · Alertmanager · Pushgateway · config-guard · healthchecks
   ▼
OBSERVABILITY (fail-soft): métricas/tracing por env (metrics.enabled) · pushgateway · dashboards · alerts
```

Separación explícita:

| Capa | Pertenece | No es |
|---|---|---|
| REPOSITORY | código + config + deploy (templates/scripts/monitor/example) | estado del host |
| DEPLOYMENT API | `deploy/install.sh --check/--apply` + gates; generador de rendered | reinvención de scripts |
| RUNTIME | **systemd** — procesos de la app (units nativas) | Docker |
| INFRASTRUCTURE | **Docker Compose** — Kafka/Redis/obs | runtime de la app |
| APPLICATION | procesos nativos uv; contrato-only | contenedores (hoy) |
| OBSERVABILITY | compose + fail-soft app | gate de ejecución |

**Respuestas directas:**
- **¿Docker pertenece al runtime de OCM?** **NO.** Docker es el mecanismo de infraestructura/observabilidad (compose), no de la app. Evidencia: app sin imports de docker (§1.3); override comenta "corre en la maquina local (fuera de Docker)".
- **¿systemd pertenece al runtime de OCM?** **SÍ.** systemd supervisa los procesos nativos de la app (units market-data/streaming); ADR-0022 decidió lifecycle por systemd; contra evidencia de units instaladas.
- **¿Compose pertenece a infrastructure?** **SÍ.** Todo el sidecar de infra/obs vive en compose con healthchecks/config-guard; versionado en deploy/monitoring y docker-compose.yml.
- **¿Quién owns lifecycle?** **systemd** para procesos de la app; **compose restart policy** para contenedores de infra; **instalador (`--apply`)** para transiciones declaradas; **operador humano/CI** para trigger de install. NO debe haber dueño difuso (hoy: ninguno).
- **¿Cuál es el source of truth?** **El repositorio Git** (único). Todo lo que el host materializa (`.venv`, rendered, `/etc/systemd/system`, `host.env`, `.env`, volumes) es derivado y regenerable; el estado live real ("qué vía corre") debe ser convergeble y comprobable por `--check`.

---

## 40. Recommended architecture (qué debe ser el orquestador y su boundary)

1. **`deploy/install.sh`** — un único "sequence & verify" bash (~120-150 líneas) con el DSL **PASS/FAIL/SKIP** heredado de `check_production_gates.py` (power: G1–G11 existen y son la semilla; NO recrear DSL ajeno).
   - `--check` (default, read-only): runtime→config→infra→systemd→observability→health, reporta PASS/FAIL/SKIP, exit 0 si todo PASS, 1 si FAIL.
   - `--apply` (mutante, requiere confirmación): create-if-missing config, compose up, install_systemd --apply, start/enable units con guards, provision topics, health final.
   - `--dry-run`: igual que apply sin mutar.
   - **Delega SIEMPRE** en: `uv sync`, `docker compose up -d`, `install_systemd.sh`, `provision_kafka_topics.py`, `health_check.sh`, gates. **Nunca** ejecuta lógica instaladora propia (anti-God-Script: §13).
2. **Boundary del orquestador:** termina cuando el sistema está instalado/verificado/sano. NO: instalar Python/uv, construir imágenes, gestionar secrets del proceso, ejecutar el pipeline, ni ser el dueño del CD.
3. **Dónde vive:** `deploy/scripts/install.sh` (no raíz, no `deploy/install.sh`, coherencia con `deploy/scripts/*`). **Facade raíz opcional**: `./install.sh` de 1 línea `exec deploy/scripts/install.sh "$@"` solo para descubrimiento estándar (patrón `run.sh`). Si se decide el facade, `run.sh`-style + doc "install.sh→deploy/scripts".
4. **CD de deployment:** decisión `--check` como *gate* en CI (agregar job condicional que corra `install.sh --check` en el runner — read-only) + `--apply` en host vía SSH (manual/self-hosted futuro). El modelo Docker/GHCR queda **deprecado** salvo decisión contraria explícita (ADR).
5. **systemd drift-check:** dentro de `install.sh --check` (etapa systemd): render in-memory y `diff template↔rendered↔SO`; y en `install_systemd.sh --verify-only` (10–15 líneas). No un servicio de auditoría en tiempo real (§6).
6. **Health por contrato:** etapa health lee env contract vía venv de app o endpoints `/ready` ampliados — **no `docker exec`** (§37). `docker exec` solo fallback opcional.
7. **market-data / bootstrap:** canonizar nativo/systemd (puesto el unit ACTIVE tras transición) y retirar/evitar el container `ocm_market_data` mientras no se decida B (ADR-0024).

---

## 41. What should NOT be implemented

- **NO** `deploy/{runtime,infrastructure,config,health}/` — organización ≠ arquitectura, SSOT duplicados (§4/§25).
- **NO** `install.sh` raíz CON lógica implementadora (solo facade de 1 línea opcional).
- **NO** `deploy/config/` — mezcla Hydra + host.env, arriesga segunda fuente de verdad.
- **NO** `ocm-trading.service` / `ocm-portfolio.service` — sin entrypoint (trading/portfolio NIVEL 2, ADR-0024).
- **NO** "full-container" como objetivo de producción HOY — mucho menos sin artifact/CD de imágenes real (bloquea por F-NEW-1 y NIVEL 1).
- **NO** inventar un modelo CD Docker/GHCR nuevo (pipeline de build-and-push) mientras el runtime sea nativo single-host.
- **NO** instalar CLIs nativos de Kafka/Redis en el host como vía de health (dependencia de host innecesaria; venv de app cubre).
- **NO** `REDIS_URL` compuesta/var (mantener `REDIS_*` discretos, §9).
- **NO** hacer observabilidad hard-dependency (mantener fail-soft).
- **NO** marca B-59 como HECHO sin prueba de reinicio real.
- **NO** cazar un ADR nuevo para "microservicios" mientras no existan NIVEL 1 (mantener flujo ADR-0024).

---

## 42. What should be implemented first (faseado por riesgo → beneficio → cero estructura)

1. **Fase cero (fixes aislados, sin estructura):**
   - `git rm --cached deploy/host.env` + verificarlo (F-6); mantener `.env.example`/`host.env.example` como SSOT de ejemplos y test de paridad CI para puertos/defaults (cierra F-10/F-9/F-5 parcial).
   - F-3: cambiar ruta del gate a `deploy/scripts/install_systemd.sh --verify-only` (+ re-render antes de `systemd-analyze verify`).
   - F-5: header+puertos muertos de `deploy_ocm.sh`.
   - Cleanup `ocm/observability/metrics.py` placeholder (headers legacy core/observability).
2. **Fase uno (health por contrato, sin tocar units):**
   - Refactor `health_check.sh` L2/L3 y gates G5/G6/G8 a venv (aiokafka/redis) o endpoint `/ready` ampliado; `docker exec` como fallback env-opcional documentado.
3. **Fase dos (orquestador + drift):**
   - `deploy/scripts/install.sh --check/--dry-run/--apply` (delegado, DSL gates); drift-check systemd en `--check`/`--verify-only`.
   - Cerrar B-59: prueba de reinicio controlada del streaming (y de market-data tras canonización) o mover a modelo alternativo.
4. **Fase tres (CD y gobernanza):**
   - Deprecar `ocm-cd.yml`/`deploy_ocm.sh` (o reconectar SSH/self-hosted → `install.sh --apply`) previo ADR.
   - ADR modelo canónico + boundary orquestador; actualizar tracking B-57/B-59; registry de `[project.scripts]` vs `run.sh` (ampliar run.sh a 5 o marcarlo subconjunto dev).

---

## 43. Required ADR before structural changes

1. **ADR runtime canónico** — native-first confirmado: systemd es el runtime de la app; Docker = infra/obs (implicaciones para ADR-0024 NIVEL y consenso autónomo de las units).
2. **ADR deployment boundary + orquestador** — autoriza `deploy/scripts/install.sh --check/--apply` delegado, drift-check systemd, DOI de units, y la relación con `check_production_gates.py` (DSL reutilizado, no coexistencia de dos gateways).
3. **ADR model de CD** — decisión explícita: deprecar modelo Docker/GHCR vs reconectar vía SSH/self-hosted a `install.sh --apply`; con efecto en tracking B-57 (IMPLEMENTADO-PARTIAL) y depuración de ADR-0037 (estado, contexto obsoleto).
4. **Enmienda/ADR de canonización de arranque** — market-data native/systemd (transición del container) + cuándo reintroducir contenedor (ADR-0024 NIVEL 1).
5. Antes de ninguna unit nueva para trading/portfolio: ADR-0024 avanzando a NIVEL 1 (con entrypoints reales).

Ninguno de estos cambios estructurales debe implementarse sin pasar por el mecanismo formal (ADR + tracking.yaml). Los fixes de Fase cero (subir estado, rutas, ejemplos) NO requieren ADR.

---

## 44. Migration sequence (resumen)

1. F-6 + F-3 + F-5 + F-10 + F-9 (fixes aislados) — sin ADR.
2. Health por contrato (`health_check.sh` + gates) — sin nuevo boundary.
3. `deploy/scripts/install.sh --check/--apply` + drift-check systemd (antes de tocar CD).
4. Canonización market-data: transición container→unit systemd + verificación B-59 (prueba de reinicio).
5. ADR modelo canónico + ADR boundary/orquestador + ADR CD (o depurcar) — registro en tracking.
6. CD real SÓLO si se decide ADR al respecto y existe canal SSH/self-hosted; alternativa válida: depurar.
7. Cuando existan NIVEL 1 (trading/portfolio entrypoints reales) → evaluar unidades por BC o vuelta eval de B.

**NO implementar** nada de §§40–44 en esta misión (read-only vigente): el único deliverable es este documento.

---

## 45. Finalización (iteración 2026-09-19)

### `git status --short` (final — post única escritura permitida)

```
 M .github/workflows/ocm-cd.yml
 M Dockerfile
 M deploy/monitoring/grafana/dashboards/ocm_pipeline.json
 M deploy/monitoring/grafana/provisioning/datasources.yml
 M deploy/scripts/deploy_ocm.sh
 M packages/market_data/infrastructure/quality/anomaly_registry.py
 M policies/semgrep/architecture.yaml
 M uv.lock
?? docs/audit/deployment-runtime-architecture-adversarial-audit.md
?? docs/audits/AUDIT_OCM_deployment-runtime-architecture_2026-09-19.md
```

- **Único archivo modificado por esta iteración:** `docs/audit/deployment-runtime-architecture-adversarial-audit.md` (append §31–§45, ya única escritura permitida).
- **Confirmación final:**
  - No código modificado. No CI modificado. No deploy modificado. No Docker/Compose modificado. No systemd modificado. No tracking modificado. No ADR creado/modificado.
  - Los 8 archivos ` M ` y el `?? docs/audits/…` eran preexistentes e intactos.
  - Sin commits, sin push, sin merge, sin reset/clean.
- **Evidencia gather en esta iteración (read-only):** `git ls-files deploy/` (host.env tracked); `docker ps` (container `ocm_market_data` activo); `systemctl is-active` (streaming active, market-data inactive); `which kafka-broker-api-versions/redis-cli/kcat` (not found); `.venv` imports `aiokafka`/`redis` (presentes); compose ports/push absence de GHCR; `systemd-analyze` no ejecutado (no requerido).

**DETENERSE — sin implementación.** Ningún DR-1…DR-5 ejecutado. Único artefacto enviado por esta misión adicional: esta sección del audit adversarial.

---

## 46. Iteración III (2026-09-19) — Auditoría read-only de deployment y canonicalización del runtime

> **Tipo:** Tercera iteración adversarial, read-only, de canonicalización. Corrige la interpretación de la iteración II: el contenedor activo `ocm_market_data` **NO es deployment productivo ni evidencia de arquitectura híbrida** — fue levantado deliberadamente por el operador solo para pruebas de Market Data.
> **Modelo objetivo a validar (declarado por el operador):**
> ```
> Debian
>   ├── systemd ── OCM (Market Data, Trading, otros procesos)
>   └── Docker/Compose ── Kafka, Redis, Observabilidad
> ```
> **Principio:** OCM depende de contratos de servicio/endpoints y configuración, NO de Docker como mecanismo de ejecución. Docker = mecanismo de deployment/empaquetado **opcional** de la aplicación, nunca dependencia arquitectónica del dominio/application layer.
> **Respuesta a la pregunta crítica (README §Pregunta):** el repositorio **SÍ está preparado** para el modelo systemd-canónico + Docker-infraestructura + dockerización-opción-futura (ver §58–§73); lo que falta es terminar piezas de `deploy/`, alinear systemd y cerrar B-59, no cambiar de modelo.

---

## 47. Evidencia runtime verificada en esta iteración (commands read-only)

| Comando | Salida | Conclusión / corrección a iteraciones previas |
|---|---|---|
| `ls /etc/systemd/system/ocm*` | `ocm-market-data.service` y `ocm-streaming.service` (root:root 644) | **Units instaladas en SO (existen).** Cadena repo→SO materializada. |
| `systemctl is-enabled ocm-market-data.service` | `disabled` | Unit instalada pero **no habilitada** (no arranca sola al boot). Coherente con market-data probado en contenedor, no desplegado como productivo. |
| `systemctl is-enabled ocm-streaming.service` | `enabled` | Streaming sí habilitado → canonical A+B operativo para streaming. |
| `systemctl is-active ocm-market-data.service` | `inactive` | Market-data **no corre bajo systemd hoy** (está en Docker para pruebas). Estado temporal, no arquitectura. |
| `systemctl is-active ocm-streaming.service` | `active` | Streaming corre bajo systemd. |
| `diff render/ocm-market-data.service /etc/systemd/system/ocm-market-data.service` | `IDENTICAL` | **installed == rendered** (byte a byte). No hay drift rendered↔SO. |
| `diff render/ocm-streaming.service /etc/systemd/system/ocm-streaming.service` | `IDENTICAL` | idem. |
| (diff template↔rendered, ver §63) | línea `EnvironmentFile .env` presente en rendered/installed, ausente en template | Drift **template↔(rendered+installed)** — SSOT VCS desactualizado respecto al estado real del host. Corrección de F-4: el drift no es rendered↔SO sino **template↔SO**. |
| `ls docker/` | `does not exist` | El directorio `docker/` hipotetizado por la misión **no existe**; el contenedor tiene nombre de *compose project* `ocm_market_data`. |
| `rg -i "ssh|self-hosted" deploy/ .github/ docs/decisions/` | menciones **solo en texto de ADR-0037** | No hay workflow SSH/self-hosted en CI; SSH = intención documentada, no implementada. |
| `rg -i "ghcr|docker push" .github/ deploy/` | solo `deploy_ocm.sh:35` (constante), sin push en workflows | GHCR = configuración muerta, sin pipeline real (confirma F-NEW-1). |
| `git ls-files deploy/host.env` | trackeado | F-6 confirmado de nuevo (ignorado en `.gitignore:74` pero rastreado). |
| `sed -n 45p docs/architecture/README.md` | `Estado operativo: actualmente DETENIDO (ni Docker ni local).` | **La doc oficial de arquitectura ya declara market-data DETENIDO** — coherencia total con "contenedor de prueba": la arquitectura declarada NO cuenta el experimento como deployment. |
| `rg -i systemd README.md` | (sin hits) | README onboarding NO documenta units/install_systemd: gap de documentación (solo ADR-0022/scripts lo cubren). |
| `sed -n '140-170p' README.md` | Instalación = clone → `uv sync` → `cp .env` → `docker compose up -d` → `uv run ocm` | Path de instalación documentada = **uv nativo + infra compose**. No contradice systemd; lo complementa sin explicarlo. |
| `.dockerignore` | excluye `.env`, `.venv`, data dirs, compose, Dockerfile | B-05/H-14 respetado (test Docker hardening lo verifica en CI). |
| ADR estados | 0022 **Aceptado**; 0024/0037/0033-0036/0038 **Propuesto** | Ver §56. |

**Nivel de confianza de la tabla:** ALTO (comandos ejecutados read-only en el host; salidas capturadas).

---

## 48. ESTADO REAL DEL REPOSITORIO — despliegue (misión §1)

- **Deployment artefacts:**
  - `deploy/scripts/`: `install_systemd.sh` (real, usa host.env/rendered/verify) · `health_check.sh` (real, L1–L4) · `deploy_ocm.sh` (CD **inoperante**: F-NEW-1).
  - `deploy/systemd/templates/` 2 templates (SSOT units, VCS) · `deploy/systemd/rendered/` 2 units (gitignored, derivadas).
  - `deploy/monitoring/`: prometheus/alertmanager/alerts/loki/promtail/grafana — **completos, consumidos por compose**.
  - `deploy/host.env(.example)`: topología de host; `.example` en VCS, `.env` **trackeado (F-6**), ignorado en `.gitignore:74`.
  - **Placeholders/incompletos:** `deploy/scripts/deploy_ocm.sh` (CD roto), `deploy/scripts/install_systemd.sh` **no hace enable/start** (solo render+verify+install+daemon-reload), y **no hay `deploy/scripts/install.sh`** (orquestador no existe).
  - **Legacy:** `ocm/observability/metrics.py` placeholder (headers "core/observability", v0.1.0); `scripts/migrate_audit_filenames.py` (one-off).
  - **Duplicados reales:** la lógica de salud Kafka/Redis aparece en `health_check.sh` (L2/L3) **y** en `check_production_gates.py` G5/G6/G8 (docker exec) — dos implementaciones del mismo contrato.
  - **Consumido por CI/runtime:** templates→systemd; monitoring→compose; `health_check.sh`→gates G7/G9 y deploy_ocm; `install_systemd.sh`→gate G4 (con bug: ruta `scripts/install_systemd.sh` no existe — F-3).
- **No existen:** `docker/`, `deploy/runtime`, `deploy/infrastructure`, `deploy/config`, `deploy/health`, `install.sh` raíz, `Makefile`. (No se propone ninguna reorganización estética.)

---

## 49. SYSTEMD — cadena repo→SO y drift (misión §2)

```
REPO (VCS)                       INSTALADO (SO)              SYSTEMD ── OCM
deploy/systemd/templates/   →  /etc/systemd/system/     →   ocm-*.service
   *.template (SSOT)           *.service (root:root 644)    Type=simple, User=orangemusic
        │  envsubst via install_systemd.sh                       ExecStart=.venv/bin/python -m …
        └─► deploy/systemd/rendered/ (gitignored)               Restart=on-failure, RestartSec=10
                                                                 KillSignal=SIGINT, TimeoutStopSec=30
```

**Auditoría por unit:**

| Atributo | `ocm-market-data.service` | `ocm-streaming.service` |
|---|---|---|
| ExecStart | `.venv/bin/python -m market_data.main` (:8001) | `.venv/bin/python -m app.cli.streaming_hydra` |
| User / WorkingDirectory | `orangemusic` / `${OCM_REPO_ROOT}` | idem |
| EnvironmentFile | `${OCM_REPO_ROOT}/deploy/host.env` (+ `.env` solo en rendered/installed — drift template) | idem |
| Restart | `on-failure`/10s | idem |
| After/Wants | `network-online.target docker.service` / Wants network | idem |
| Dep infra | requiere `.venv` + `host.env` + Docker local (After) | idem |
| Hardening | NoNewPrivileges, ProtectSystem=strict, ReadWritePaths data_platform+logs, PrivateTmp | idem |
| Correspondencia código | `market_data.main` existe (FastAPI + lifespan 3 tasks); **unit coincide** | `app.cli.streaming_hydra` existe; **unit coincide** |

**Drift detectado (corrección F-4):** template NO tiene `EnvironmentFile …/.env`; rendered **e installed** sí (idénticos). Por tanto: **rendered ↔ SO = 0 drift; template ↔ (rendered+installed) = 1 línea de drift**. Conclusión: reinstalar con `install_systemd.sh` desde template **eliminaría** el `.env` del SO — comportamiento correcto-pero-sorpresivo; esto es exactamente lo que `--check`/drift-check debe detectar (§66).

**Estado enable/start:** `install_systemd.sh` NO habilita ni arranca (solo install+daemon-reload). market-data `disabled/inactive`, streaming `enabled/active`. **GAP:** faltan enable/start con guards y verificación de reinicio (B-59, Fase 5 roadmap). Ninguna unit nueva creada.

---

## 50. MARKET DATA — caso principal (misión §3)

- **Entry point real:** `packages/market_data/main.py` (`python -m market_data.main`); FastAPI `:8001`, `/health` (liveness puro, `main.py:465-492`), `/ready` (ctx-only, NO pinea Kafka/Redis, `main.py:495-519`), `/ohlcv/…` (Silver Iceberg). Lifespan lanza 3 tasks asyncio (ingestion + Kappa bronze + FeedOrchestrator). `__main__` → `uvicorn.run(workers=1)` (`main.py:622-632`).
- **Corre nativamente:** ejecutable como proceso uv (`uv run python -m market_data.main`), supervisable por systemd (**unit existe y es correcta**, §49), y tambien en Docker (`Dockerfile CMD python -m market_data.main`; compose `command` en profile microservices). `[project.scripts]` **no** tiene entrypoint `market-data` (solo `-m`).
- **Config:** Hydra `config/market_data/…` + env: `KAFKA_BOOTSTRAP_SERVERS`, `REDIS_HOST/PORT/DB/PASSWORD`, `PUSHGATEWAY_URL`, `OCM_STORAGE__DATA_LAKE__PATH`, `MARKET_DATA_PORT`. Contrato-only ✓.
- **Deps externas:** Kafka (writer Kappa de `ohlcv.raw`; topics vía `shared.kafka.topics`), Redis (según config.ciudad), datalake local, observabilidad (prometheus push en prod).
- **Separación de lecturas:**
  - **A. Deployment canónico previsto** = **systemd native** (unit en VCS; doc `docs/architecture/README.md:45` lo declara "DETENIDO (ni Docker ni local)" → no probado como desplegado). 
  - **B. Deployment experimental (PRUEBAS)** = contenedor `ocm_market_data` activo; circunstancial, NO contabilizado como arquitectura (regla de la misión).
  - **C. Opción futura soportada** = dockerización (`Dockerfile` + compose profile `microservices`) — debe quedar como opción, no sustituye A sin ADR.
- **¿Puede correr fuera de Docker con contratos existentes?** Sí: `.venv` tiene `aiokafka`+`redis`; unit apunta a `.venv/bin/python`; solo necesita infra compose arriba (kafka/redis) y host.env/.env. Confianza ALTA.
- **GAP:** sin `systemd_reinicia_correctamente` verificado (B-59) y sin integración HTTP `TestClient` (brecha conocida ADR-0024). El driver de canonización es —justamente— la transición controlada unit ACTIVE + parada del contenedor.

---

## 51. DOCKER/COMPOSE — responsabilidad actual (misión §4)

- **Infraestructura (perfil default, arranca con `docker compose up -d`):** `redis`, `kafka`+`zookeeper`, `pushgateway`, `prometheus`, `alertmanager`, `grafana`, `loki`, `promtail`, `config-guard` (init valida configs monitoring), `kafka-ui` (auth, loopback). → **INFRAESTRUCTURA es la responsabilidad mayoritaria del compose.**
- **Aplicación:** solo en profile `microservices` **off por defecto**; `docker-compose.override.yml` comenta el bloque market-data (`el pipeline corre en la maquina local (fuera de Docker)`); trading/portfolio "SCAFFOLDING FUTURO… NO existe trading.main". → **Compose NO define OCM como runtime hoy.**
- **Veredicto:** el compose define **infraestructura principalmente y opciones de aplicación scaffolding**. Que "Compose existe" ≠ "OCM debe correr en Compose". El runtime productivo declarado (visited en §49/§50/§56) es systemd native. Docker permanece responsable de infra+obs y como opción futura empaquetada; **no se mueve ni se elimina**.

---

## 52. HEALTH/READINESS/GATES — acoplamiento y contrato deseable (misión §5)

- **Acoplamiento actual ("para saber si está sano debo ejecutar docker exec"):**
  - `deploy/scripts/health_check.sh:36-68` → `docker exec ocm_kafka …` (L2/L3 y offsets) + `docker exec ocm_redis redis-cli …`.
  - `scripts/check_production_gates.py` G5 (`:268-304`), G6 (`:332-356`), G8 (`:376-411`) → `docker exec ocm_kafka/ocm_redis`.
  - `deploy_ocm.sh:176-186` → `docker exec` en wait_for_health (puertos muertos nunca usados, F-5).
- **Alternativa por contrato (documentada, NO implementada):** endpoint TCP/servicio (`localhost:9093` → kafka-broker API), protocolo Kafka/Redis vía **venv de app** (`aiokafka`/`redis` ya en `.venv`), HTTP health/readiness (ampliar `/ready` de market-data), systemd status (`systemctl is-active`) para procesos. El host NO tiene CLIs nativas (verificado `which` not found) → `docker exec` hoy es el único mecanismo disponible, pero es **mecanismo**, no contrato.
- **Consumidores afectados:** `health_check.sh` (gates G7/G9, deploy_ocm pre/post), gates G5/G6/G8, `SystemMon`/operador humano. Riesgo de romperlos: MEDIO si infra se externaliza; BAJO hoy (Docker local presente) — pero dejar el acoplamiento perpetúa F-NEW-2.
- **Archivos involucrados:** `deploy/scripts/health_check.sh`, `scripts/check_production_gates.py`, (futuro) `deploy/scripts/install.sh`. **Diseño requerido:** helpers de health por contrato (venv/endpoints), `docker exec` solo como fallback env-configurable, mantenimiento de un único DSL (DSL gates).

---

## 53. ORQUESTADOR — estado y diseño necesario (misión §6)

- **Estado:** NO existe `deploy/scripts/install.sh` ni `./install.sh`. La "orquestación" hoy = README (pasos manuales) + `scripts/check_production_gates.py` (G1–G11, semilla de DSL PASS/BLOCK) + 3 scripts de deploy + Hydra E2E.
- **Responsabilidades que DEBE cubrir un orquestador (si se adopta):** preflight (uv/python/permisos), validación (Hydra `OCM_VALIDATE_ONLY`), config create-if-missing (.env/host.env), install deps (`uv sync`), infra compose (`docker compose up -d`), systemd (`install_systemd.sh` + daemon-reload + **enable/start con guards** —hoy ausente— + drift-check), health (`health_check.sh`), rollback/failure handling (report PAS/FAIL/SKIP, exit code; `--apply` mutante vs `--check` read-only).
- **Límites (boundary):** termina cuando el sistema está instalado/verificado/sano. NO: instalar Python/uv del OS, construir imágenes, gestionar secrets de proceso, ejecutar el pipeline, ni reinventar el DSL de gates (reusar `check_production_gates.py`).
- **`./install.sh` raíz:** **facade opcional** de 1 línea (`exec deploy/scripts/install.sh "$@"`) siguiendo el patrón `run.sh`; NO lógica propia (God script prohibido — evidencia: F-3 nace del path inconsistente de un script nuevo). Ninguna responsabilidad nueva en la raíz.
- **NO se crea ningún script en esta misión.**

---

## 54. GIT / SSOT / DRIFT (misión §7)

- **Fuente declarativa versionada (Git = SSOT):** código, `config/` (Hydra), `pyproject.toml` (`[project.scripts]`), `deploy/systemd/templates`, `deploy/monitoring`, `deploy/scripts/*.sh`, `deploy/host.env.example`, `.env.example`, `docker-compose.yml`/override, `.dockerignore`.
- **Estado instalado (host):** `.venv`, `rendered/` (gitignored), `/etc/systemd/system/*.service`, `host.env` (debería ser local-only: **hoy trackeado, F-6**), `.env`. 
- **Estado runtime:** units activas/descativas (verificado §47), contenedores, procesos.
- **Config/secrets externos:** `.env` (gitignored, OK), `host.env` (trackeado — debe quedar fuera de Git como `.env`).
- **Mecanismos de drift que existen vs faltan:**
  - Existen: gitignore (rendered, .env, host.env — aunque host.env violado), `check_ssot_enums.py`, gates G1–G11, template-render, test `test_docker_hardening.py` (invariantes compose en CI), `dockerignore`.
  - **Faltan:** drift-check template↔rendered↔SO (drift ya materializado: línea `.env` — §49), paridad de puertos `.env.example`↔compose/host.env (F-10/F-9), paridad `[project.scripts]`↔`run.sh`, y un test de render/verify que corra en CI (hoy solo gate G4 con bug F-3, y en máquina local).
- **Veredicto:** Git es SSOT declarativo, pero necesita los guards de paridad anteriores para que "SSOT" sea verificable.

---

## 55. CD / GHCR / SSH — estado y clasificación (misión §8)

| Mecanismo | Ubicación | Clasificación | Evidencia |
|---|---|---|---|
| `ocm-cd.yml` (workflow_dispatch) | `.github/workflows/ocm-cd.yml` | **AMBIGUO→íneno (codificado sin modelo real)** | runner `ubuntu-latest` efímero; no toca el host nativo |
| `deploy_ocm.sh` | `deploy/scripts/deploy_ocm.sh` | **LEGACY/ambiguo** | `docker compose up -d --build ${ARTIFACT_SHA}` selector inválido; puertos muertos F-5; `.prev_sha` ausente |
| GHCR (`ghcr.io/orangecashmachine/ocm`) | constante en deploy_ocm.sh:35 | **FUTURO (no operativo)** | sin push/build en workflows |
| SSH deploy | solo texto en ADR-0037 | **FUTURO** | sin workflow self-hosted |
| Deploy real hoy | manual: `uv sync` + `compose up` + `install_systemd.sh` + health_check | **ACTUAL** | README + §49 |

**Conclusión:** CI/CD hoy realiza **CI real** (ocm-ci robusto) y **CD codificado-pero-inerte**. No se elimina nada en esta misión. Deprecar (si se decide) requeriría **ADR** — el camino recomendado es declarar "CD = opcional hasta orquestador + canal SSH/self-hosted", depurando `deploy_ocm.sh`/`ocm-cd.yml` solo tras ADR de modelo de CD (ver §69).

---

## 56. ADRs — estado formal vs implementación (misión §9)

| ADR | Estado | Relación con runtime/deployment | Coherencia con código |
|---|---|---|---|
| ADR-0020 (production gate release) | Aceptado | gates G1–G11 | ✓ (gates existen) |
| ADR-0022 (lifecycle realtime feeds → **systemd**) | **Aceptado** | systemd supervisa streaming; B-59 NO_VERIFICADO | ✓ unit streaming activa; addendum "Sin unit activa" **obsolescente** (§19) |
| ADR-0024 (dirección microservicios; NIVEL 1=market-data) | **Propuesto** | dockerización opcional + NIVEL 2 embebido | ✓ con `docs/architecture/README.md`; bloque trading/portfolio cometido como "NO EJECUTABLE" |
| ADR-0033 (gate binario) | Propuesto | gates PASS/BLOCK | ✓ (`check_production_gates.py`) |
| ADR-0037 (CD verify/deploy/rollback) | **Propuesto** | CD container-GHCR | ✗ **contradicho por la implementación real** (F-NEW-1; contexto dice "deploy_ocm.sh inexistente", hoy existe pero roto) |
| ADR-0038 (grafana provisioning) | Propuesto | observabilidad | ✓ (compose monta monitoring) |
| ADR-0031/0032 (policy registry) | Aceptado | governance immutable | ✓ |
| ADR-0005/0006/0012/0016/0021/0025-0030 | Aceptado | dominio/app | ✓ |

**Decisiones FORMALIZADAS con impacto deployment:** ADR-0022 (systemd = lifecycle de feeds realtime) y ADR-0020/0033 (gates). **IMPLÍCITAS, no ADR:** "systemd = runtime canónico de TODO OCM" (solo explícito para streaming/feeds), "Docker = infraestructura y opción futura" (solo ADR-0024 dirección), "Git = SSOT deployment", "health por contrato". **CONTRAÍDAS:** ADR-0037 (Propuesto) vs realidad CD. → Requieren ADR antes de cambios estructurales: ver §69.

---

## 57. ORDEN DE TRABAJO RECOMENDADO (sin implementar) (misión §11)

FASE 1 → fixes aislados y objetivos (F-6 untrack host.env; F-3 ruta gate; F-5 puertos muertos; F-10 `.env.example`; default Kafka; placeholder metrics). **Sin estructura.**
FASE 2 → health/readiness por contrato (venv/endpoints), `docker exec` a fallback; elimina F-NEW-2. 
FASE 3 → definir/Cerrar orquestador `deploy/scripts/install.sh` (--check/--apply) + drift-check systemd; root `./install.sh` facade opcional.
FASE 4 → canonizar Market Data bajo systemd: transición container→unit ACTIVE (parar contenedor) + alignment docs.
FASE 5 → verificar B-59 (prueba de reinicio streaming y market-data) y demás contratos (health, readiness, data eligibility).
FASE 6 → ADR de CD (Docker/GHCR/SSH): deprecar, reparar, o conectar vía SSH/self-hosted — decisión formal.
FASE 7 → evaluar dockerización de OCM como **opción soportada** (sin sustituir el modelo canónico sin ADR explícito).

Las fases 1–7 se analizan aquí; **ninguna se ejecuta**.

---

## 58. ESTADO ACTUAL VERIFICADO

- **systemd nativo:** units de market-data y streaming instaladas en `/etc/systemd/system` (verificado). Streaming **enabled+active**; market-data **disabled+inactive** (instalada, no desplegada como productivo). ExecStart `.venv/bin/python -m market_data.main` y `-m app.cli.streaming_hydra`, correctos contra el código.
- **Docker/Compose:** infraestructura y observabilidad corriendo (kafka/redis/zookeeper/monitoring); contenedor `ocm_market_data` activo **solo para PRUEBAS** (lectura correcta: DOCUMENTACIÓN externa ya lo declara "DETENIDO (ni Docker ni local)"). No es runtime productivo.
- **Deploy:** README = uv nativo + compose infra; `install_systemd.sh` (no enable/start); `health_check.sh` L1–L4; `deploy_ocm.sh`/`ocm-cd.yml` **CD inerte** (F-NEW-1).
- **Orquestación:** fragmentada manual; gates G1–G11 son la semilla.
- **Gobernanza SSOT:** Git es SSOT declarativo; `deploy/host.env` viola gitignore (F-6).
- Confianza: ALTA (commands §47 + revisión de archivos).

## 59. ARQUITECTURA CANÓNICA OBJETIVO

```
REPOSITORY (Git, SSOT)
   code + config/ (Hydra) + pyproject ([project.scripts]) +
   deploy/systemd/templates (SSOT units) + deploy/monitoring
   + deploy/scripts/* + host.env.example/.env.example + compose/.dockerignore
   │
   ▼
DEPLOYMENT API: deploy/scripts/install.sh --check/--apply (orquestador delegado,
   reusa install_systemd/health_check/provision/gates; NO reimplementa)
   │
   ▼
RUNTIME: systemd (owner del lifecycle de la app; market-data y streaming)
   │
   ▼
APPLICATION: procesos nativos uv; contratos env; sin Docker
   │
   ▼
INFRASTRUCTURE: Docker/Compose (Kafka, Redis, Observabilidad; config-guard,
   healthchecks) — externalizable mañana sin tocar la app
   │
   ▼
OBSERVABILITY: fail-soft (metrics por env; pushgateway; dashboards/alerts)
```
Afirmación validada: **SÍ** el repo apunta a este objetivo (unit correcta, contrato-only, compose infra-dominante, dockerización opcional en profile). El parámetro "qué falta" se detalla en §61–§63.

## 60. DIFERENCIA ENTRE RUNTIME CANÓNICO Y EXPERIMENTO DOCKER

| Dimensión | Runtime canónico (objetivo) | Experimento Docker (ocm_market_data) |
|---|---|---|
| Naturaleza | systemd native; unit VCS | contenedor efímero para pruebas |
| Evidencia repo | units templates + docs "DETENIDO" | compose profile `microservices` (opt-in, override off) |
| Estado SO | instalado pero **disabled/inactive** | activo temporalmente |
| Contabilización arquitectónica | SÍ (ADR-0022/0024 dirección) | NO (por declaración operador; docs concuerdan) |
| Acción requerida | activar/verificar (Fase 4/5) | retirar/parar al terminar pruebas (no es artefacto a conservar como productivo) |
**Conclusión:** no hay arquitectura híbrida productiva; hay un **canon nativo + un experimento temporal**, correctamente distinguible con la evidencia. No se toca nada.

## 61. MARKET DATA — ESTADO Y GAPS

- **Estado:** entrypoint real (`market_data.main`), unit correcta, config Hydra propia, contrato-only, `/health`+`/ready` (readiness sin deps), dependencias Kafka/Redis por env, observabilidad prod. Run nativo viable (venv aiokafka/redis). Confianza ALTA.
- **GAPS:** (1) unit **no habilitada** ni verificada al reinicio (B-59); (2) `/ready` no pinea Kafka/Redis (no alimenta G6 por contrato); (3) sin tests de integración HTTP `TestClient` (brecha ADR-0024); (4) README no documenta la unit ni el despliegue nativo (solo ADR-0022/scripts); (5) `.env.example` puertos Kafka/Alertman invertidos (F-10, si se despliega desde `.example`); (6) default Kafka en 3 sitios (F-9).
- **Deployment canónico previsto:** unit systemd ACTIVE + infra compose; **experimental:** contenedor de pruebas; **futuro soportado:** Dockerfile/profile con el mismo entrypoint.

## 62. DEPLOY — ESTADO Y GAPS

- **Estado:** `deploy/systemd` completo (templates+rendered); `deploy/monitoring` completo (consumido por compose); `deploy/scripts` 3 utilidades reales; `host.env.example` VCS; `host.env` **trackeado (F-6)**.
- **GAPS:** orquestador `install.sh` inexistente; `install_systemd.sh` sin enable/start/drift-check; CD inerte (F-NEW-1); G4 con ruta rota (F-3); `deploy_ocm.sh` puertos muertos (F-5); sin gobernanza formal de `deploy/` (GOVERNANCE sin menciones); sin test de paridad template↔SO en CI.

## 63. SYSTEMD — ESTADO Y GAPS

- **Estado:** cadena repo→rendered→SO correcta; installed==rendered (byte-igual, verificado); units hardened; streaming operativo (enabled/active); market-data instalado pero disabled.
- **GAPS:** drift **template↔(rendered+installed)** de 1 línea (`EnvironmentFile .env` ausente del template); sin `--verify-only` que compare template↔SO (solo `systemd-analyze verify` sintáctico); sin enable/start automatizado con guards; sin verificación de reinicio (B-59); `After=docker.service` acopla unit al runtime Docker (decisión single-host, F-NEW-3); `EnvironmentFile` a rutas del repo (FASE-4 re-validable).

## 64. DOCKER/COMPOSE — RESPONSABILIDAD ACTUAL

- **Responsabilidad:** **infraestructura** (kafka, redis, zookeeper, observabilidad, config-guard, kafka-ui) + **aplicación-solo-opcional** (profile `microservices` off por defecto; override lo comenta explícitamente). Compose NO define OCM como runtime canónico.
- **Veredicto de la misión:** Compose existe → infraestructura sobre todo; dockerización de OCM queda como **opción futura** (Dockerfile/default-modo `-m market_data.main`). No se mueve ni elimina. Confianza ALTA (evidencia §51).

## 65. HEALTH/READINESS — ESTADO Y GAPS

- **Estado:** `health_check.sh` L1–L4 (systemd/docker/frescura/Bronze) + gates G5/G6/G7/G8/G9 + endpoints OpenAPI `/health`/`/ready` + healthchecks compose. 
- **GAPS (acoplamiento):** `docker exec` en L2/L3 y G5/G6/G8 (F-NEW-2); host sin CLIs nativas → hoy sólo docker funciona; `/ready` no incluye deps; duplicación health_check vs gates. Contrato deseable: venv (`aiokafka`/`redis`) o endpoints/`systemctl` — diseñado en §52, sin implementar.

## 66. ORQUESTADOR — ESTADO Y DISEÑO NECESARIO

- **Estado:** inexistente. 
- **Diseño necesario:** `deploy/scripts/install.sh` **solo orquesta y verifica** (preflight/validación/config/deps/infra/systemd+drift/health/rollback), reusando Scripts y el DSL de gates; default `--check` read-only; `--apply` mutante con guards `is-enabled/is-active`; drift-check template↔rendered↔SO; enable/start; report PASS/FAIL/SKIP con exit code. `./install.sh` raíz = facade opcional 1 línea. NO implementado en esta misión.

## 67. CD/GHCR/SSH — ESTADO

- CI real robusto (ocm-ci). CD inerte (F-NEW-1); GHCR sin pipeline real; SSH solo documentado (ADR-0037 fallback). Ningún mecanismo se depreca aquí; la decisión (deprecar/reparar/conectar) exige ADR (§69).

## 68. DRIFT / SSOT

- **SSOT Git:** código+config+templates+monitoring+scripts+examples+compose+`.dockerignore`. Sólido.
- **Drift confirmado (nuevo/complementario):** (1) template↔SO línea `.env`; (2) `host.env` trackeado contra policy (F-6); (3) puertos `.env.example`↔`.env` real (F-10); (4) default Kafka 3 sitios (F-9); (5) `docs/architecture/README.md` obsolescxo en ADDENDUM de ADR-0022 ("Sin unit systemd activo"). **Mecanismo faltante:** guards de paridad/`--check` para detectar (1)-(5).

## 69. ADRs NECESARIOS (antes de cambios estructurales)

1. **ADR runtime canónico de OCM** (systemd = runtime; Docker = infraestructura; dockerización = opción futura B; actualizar/declarar sobre ADR-0022/0024).
2. **ADR boundary deploy/ + orquestador** (`deploy/scripts/install.sh --check/--apply` delegado, drift-check, DOI, rollback).
3. **ADR modelo de CD** (deprecar `ocm-cd.yml`/`deploy_ocm.sh` modelo container-GHCR vs reparar vía SSH/self-hosted hacia orquestador) — condicionado al ADR de runtime.
4. **Enmienda ADR-0037** (Propuesto → decisión real; corregir contexto obsolescente; tracking B-57 → IMPLEMENTADO-PARTIAL).
5. **ADR canonización Market Data** (transición container-test → unit ACTIVE; cuándo reintroducir contenedor) y **cierre B-59** (prueba de reinicio + estado en tracking).
6. (Cuándo existan NIVEL 1 de trading/portfolio) **ADR-0024 avanzar** — units/containers por BC.

## 70. HALLAZGOS CLASIFICADOS

| # | Hallazgo | Categoría | Evidencia |
|---|---|---|---|
| F-3 | Ruta gate `scripts/install_systemd.sh` inexistente | **BUG AISLADO** | check_production_gates.py:251 vs deploy/scripts/install_systemd.sh |
| F-5 | Puertos muertos/engañosos deploy_ocm.sh | **BUG AISLADO** (config muerta) | deploy_ocm.sh:49-52 vs compose |
| F-6 | `deploy/host.env` trackeado | **CAMBIO DE GOBERNANZA** | `git ls-files deploy/`; `.gitignore:74` |
| F-9 | Default Kafka en 3 sitios | **DOCUMENTACIÓN/DRIFT** | base.yaml:46, prod:43, schema.py |
| F-10 | `.env.example` puertos invertidos | **DOCUMENTACIÓN/DRIFT** | .env.example vs .env |
| F-12 | Dockerfile CMD | **EXPERIMENTAL/NO PROBLEMÁTICO** (inválido) | Dockerfile: CMD `-m market_data.main` ✓ |
| F-NEW-1 | CD inerte (GHCR/compose selector/runner efímero) | **CAMBIO DE IMPLEMENTACIÓN→ARQUITECTÓNICO si se depreca** (requiere ADR) | deploy_ocm.sh + ocm-cd.yml + ADR-0037 |
| F-NEW-2 | Health/gates por `docker exec` | **CAMBIO DE IMPLEMENTACIÓN** (diseño de contrato, Fase 2) | health_check.sh:36-68; gates G5/G6/G8 |
| F-NEW-3 | Units `After=docker.service` + rutas repo | **CAMBIO DE IMPLEMENTACIÓN** (decisión single-host documentada) | templates |
| F-NEW-4 | `run.sh` subconjunto vs `[project.scripts]` | **DOCUMENTACIÓN/DRIFT** | run.sh:31-41 vs pyproject:176-181 |
| F-4 (revisado) | Drift template↔SO (`EnvironmentFile .env`) | **DOCUMENTACIÓN/DRIFT** | diff templates vs rendered/installed |
| B-59 | `systemd_reinicia_correctamente` NO_VERIFICADO; market-data unit disabled | **MIGRACIÓN** (Fase 5) | tracking.yaml:3146-3158, 3381; systemctl |
| Contenedor `ocm_market_data` activo | PRUEBAS operador | **EXPERIMENTAL/NO PROBLEMÁTICO** | docs/architecture/README.md:45 "DETENIDO"; contexto del operador |
| README sin systemd | **DOCUMENTACIÓN/DRIFT** | `rg systemd README.md` = vacío |

## 71. ROADMAP DE MIGRACIÓN

1. **F1** Fixes: F-6, F-3, F-5, F-10, F-9, clean placeholder metrics. Sin estructura.
2. **F2** Health por contrato (§65). 
3. **F3** Orquestador `deploy/scripts/install.sh --check/--apply` + drift-check + facade `./install.sh` (opcional). 
4. **F4** Canonizar Market Data: transición container-test→unit ACTIVE (retirar contenedor de pruebas), documentar README systemd.
5. **F5** Verificar B-59 (prueba de reinicio) y cerrar contrato readiness.
6. **F6** ADR CD (deprecar/reparar/conectar) + ADR runtime canónico + boundary + enmienda ADR-0037 + tracking B-57/B-59.
7. **F7** Dockerización de OCM como opción soportada SOLO con ADR (sin sustituir systemd).

## 72. QUÉ NO DEBE IMPLEMENTARSE TODAVÍA

- NO `deploy/{runtime,infrastructure,config,health}/` (organización ≠ arquitectura).
- NO `install.sh` con lógica propia (solo facade 1 línea; F-3 como anti-ejemplo).
- NO `ocm-trading/portfolio.service` (sin entrypoint, ADR-0024 NIVEL 2).
- NO full-container ni CD-GHCR como objetivo de producción (F-NEW-1, sin ADR de runtime).
- NO instalar CLIs nativos de Kafka/Redis para health (mecanismo de host; venv/endpoints bastan).
- NO `REDIS_URL` (mantener `REDIS_*` discretos, §9). NO observabilidad hard-dependency.
- NO abandonar el contenedor Docker existente de forma destructiva sin decisión (retirar al terminar pruebas, no como cambio arquitectónico).
- NO marcar B-59 como verificado sin prueba de reinicio real.

## 73. EVIDENCIA (resumen con archivo/línea/comando)

- Units SO: `/etc/systemd/system/ocm-{market-data,streaming}.service` (instaladas; market-data disabled/inactive, streaming enabled/active) — commands §47.
- Installed==rendered: `diff` ambos units → IDENTICAL; template≠rendered (`.env` line) — §49/§68.
- Market-data entrypoint: `packages/market_data/main.py:465-519,622-632`; unit ExecStart templates vs rendered vs SO.
- Orquestador ausente: `ls deploy/scripts/` (3 scripts; sin install.sh); `ls install.sh`/`Makefile` → no existen.
- F-3: `scripts/check_production_gates.py:251` vs `deploy/scripts/install_systemd.sh`; F-5: `deploy_ocm.sh:49-52`; F-6: `git ls-files deploy/` + `.gitignore:74`; F-9: `config/base.yaml:46`, `config/env/production.yaml:43`, `ocm/config/schema.py`; F-10: `.env`(.example) líneas 33-40.
- F-NEW-1: `ocm-cd.yml` (ubuntu-latest; sin SSH/self-hosted), `deploy_ocm.sh:160-165` selector SHA; sin `docker push`/GHCR en workflows.
- F-NEW-2: `deploy/scripts/health_check.sh:36-68`; gates `check_production_gates.py:268-411`.
- Docker infra-dominant: `docker-compose.yml` servicios default (redis/kafka/zookeeper/obs; market-data/trading/portfolio en profile microservices con override-comentado).
- ADRs: ADR-0022 Aceptado (systemd), ADR-0024/0037/0033/0038 Propuesto; ADR-0022:323 addendum obsolescente.
- B-59: `docs/plans/tracking.yaml:3146-3158,3381` (NO_VERIFICADO).
- Lectura del contenedor: `docs/architecture/README.md:45` ("market-data… DETENIDO (ni Docker ni local)").
- Comandos utilizados (todos read-only): `ls/cat/diff`, `systemctl is-enabled|is-active/<name>`, `git ls-files deploy/`, `git status --short`, `rg`, `which kafka-broker-api-versions|redis-cli|kcat` (not found), `.venv/bin/python -c "import aiokafka` (No module) … `import redis` (7.4.0 OK), `docker ps` (recolección previa, sin cambios).

---

## 74. Finalización (iteración III 2026-09-19)

### `git status --short` (final — solo escritura permitida)

```
 M .github/workflows/ocm-cd.yml
 M Dockerfile
 M deploy/monitoring/grafana/dashboards/ocm_pipeline.json
 M deploy/monitoring/grafana/provisioning/datasources.yml
 M deploy/scripts/deploy_ocm.sh
 M packages/market_data/infrastructure/quality/anomaly_registry.py
 M policies/semgrep/architecture.yaml
 M uv.lock
?? docs/audit/deployment-runtime-architecture-adversarial-audit.md
?? docs/audits/AUDIT_OCM_deployment-runtime-architecture_2026-09-19.md
```

- **Único archivo modificado/creado:** `docs/audit/deployment-runtime-architecture-adversarial-audit.md` (append §§46–74; única escritura permitida). Información previa preservada.
- **Confirmación final (reglas absolutas):**
  - NO Modifiqué código Python, `deploy/`, Dockerfiles, docker-compose, systemd units, scripts, CI/CD, configuración, ADRs, GOVERNANCE, tracking, tests.
  - NO ejecuté git reset/restore/checkout/clean/rebase/commit/push/merge; no `docker compose down`/`docker rm`/`systemctl stop`/`disable`; no levanté ni detuve servicios; no arreglé nada.
  - El contenedor experimental y las units quedan intactos; ninguna operación cambió el runtime actual.
- Comandos ejecutados en esta iteración: read-only (ver §47 y §73).

**DETENERSE.** Este documento es el único artefacto enviado por la misión. Sin implementación ni fases ejecutadas.