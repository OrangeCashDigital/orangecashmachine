# AUDITORÍA — DESPLIEGUE Y PORTABILIDAD DE ORANGECASHMACHINE

- **Fecha:** 2026-09-17
- **Modo:** READ-ONLY. Cero modificaciones a código, configuración de deployment, systemd, infraestructura o producción. Una sola edición documental: este archivo.
- **Ámbito:** Deployment y portabilidad de OCM en un servidor Debian genérico (independiente de usuario, hostname, ruta de repositorio y topología concreta). Diseño definitivo NO implementado (FASE 2 de diseño, READ-ONLY).
- **Estado de Git durante la auditoría:** rama `feat/adr0028-bookbuilder`, HEAD `cb6c6d7c`. Único cambio preexistente en working tree: `docs/plans/tracking.yaml` (de otro agente — preservado intacto, no tocar).

## Regla de evidencia aplicada

Evidencia real recopilada durante la auditoría (`CONFIRMADO`), propuestas de diseño (`PROPUESTA`) y puntos no comprobados (`NO VERIFICADO`). Prohibido convertir recomendaciones en hechos. No se inventa evidencia.

---

## 1. OBJETIVO DE LA AUDITORÍA

1. Determinar el estado real del deployment de OCM en el servidor actual (systemd, entrypoints, configuración, secretos, infraestructura, instalación).
2. Determinar las dependencias reales del servidor actual (usuario `orangemusic`, ruta `/home/orangemusic`, `orangehouse`, `.git`, rutas absolutas, puertos, servicios externos, config local).
3. Determinar qué elementos de deployment están acoplados al host y cuáles son portables.
4. Producir un **diseño definitivo** de deployment basado en la evidencia, sin implementarlo.

---

## 2. ALCANCE

**Incluido:**
- Entrypoints reales (systemd, `[project.scripts]`, `run.sh`).
- Configuración (Hydra/YAML, `.env`, env vars, paths) y secretos.
- Units systemd (template, rendered, instaladas) + `install_systemd.sh`.
- Kafka (puertos, listeners, readiness), Redis, Docker Compose.
- Acoplamientos a usuario/ruta/hostname (grep en código y fuera).
- `repo.py` / `repo_root()`.
- CI (workflows) y tests relevantes.
- Diseño target, fases, decisiones pendientes.

**Excluido deliberadamente (seguridad / no permiso):**
- Contenido real de `.env` (chmod 600; solo se comprobó su existencia y métricas).
- Offsets/logins reales del broker Kafka.
- `systemd-analyze verify` en el host (para no tocar el sistema).
- Ejecución de la prueba de validación en un Debian limpio.
- Auditorías F2.6e, PR #16 y PR #52 (fuera de alcance de este trabajo).

---

## 3. ESTADO ACTUAL DEL DEPLOYMENT

Evidencia recopilada en el host real:

| Ítem | Estado observado | Fuente |
|---|---|---|
| Hostname | `orangehouse` | comando `hostname` |
| Usuario real | `orangemusic` | `id -un` |
| Ruta del repo | `/home/orangemusic/trading/orangecashmachine` | `pwd` |
| Sistema | Debian (host), kernel Linux | entorno |
| systemd (streaming) | `ocm-streaming.service` **active**, **enabled** | `systemctl is-active/is-enabled` |
| systemd (market-data) | `ocm-market-data.service` **inactive**, **disabled** | `systemctl is-active/is-enabled` |
| ExecStart (streaming) | `${OCM_REPO_ROOT}/.venv/bin/python -m app.cli.streaming_hydra` | unidad instalada |
| ExecStart (market-data) | `${OCM_REPO_ROOT}/.venv/bin/python -m market_data.main` | unidad instalada |
| Usuario en units | `User=orangemusic` (rendered/instalado) | unidades instaladas |
| WorkingDirectory | `${OCM_REPO_ROOT}` (template) → `/home/orangemusic/...` (rendered/instalado) | template + rendered |
| .venv | presente, `Python 3.13.5` | `.venv/bin/python --version` |
| uv | `0.11.14 (x86_64-unknown-linux-gnu)` | `uv --version` |
| Python requerido | `>=3.11,<3.14` | `pyproject.toml` |
| Kafka | broker Docker en host; EXTERNAL listener default `localhost:${KAFKA_HOST_PORT:-9093}` | docker-compose L521/530 |
| Docker | Compose con Kafka, ZK, Redis, Pushgateway, Prometheus, Alertmanager, Grafana, Loki, Promtail, kafka-ui (perfil) | docker-compose.yml |
| Configuración | Hydra (base→env→exchange→pipeline) + `.env` + env vars (SSOT `ocm/config/env_vars.py`) | config/ + ocm/config/ |
| Secretos | `.env` local presente, `-rw-------` (chmod 600); contenido NO inspeccionado (seguridad) | `ls -la .env` |
| Logs | `journalctl` (stdout) + `logs/` (ReadWritePaths) | units + config |
| Instalación | `install_systemd.sh` (render + verify + install + daemon-reload; sin enable/start) | deploy/scripts/ |

---

## 4. EVIDENCIA

### 4.1 Estado de Git

| Recurso | Evidencia | Comportamiento | Conclusión |
|---|---|---|---|
| `git status --short` | ` M docs/plans/tracking.yaml` | único cambio en working tree | trabajo previo de otro agente; preservado |
| `git branch --show-current` | `feat/adr0028-bookbuilder` | rama de integración B-47..B-54 | branch en curso |
| `git log --oneline -10` | `cb6c6d7c` (HEAD), `16e04a00` (fix SIM201), `3b42d4e2` (docs governance), merges b47-b54 | log estable | sin cherry-pick en curso (`.git/CHERRY_PICK_HEAD` ausente) |
| `git ls-files deploy/host.env deploy/host.env.example` | **ambos** trackeados en Git | | **CONFIRMADO: `deploy/host.env` está versionado** pese a su cabecera "NUNCA commitear" |

### 4.2 Entrypoints

| Recurso | Evidencia | Conclusión |
|---|---|---|
| `[project.scripts]` (pyproject.toml:176-181) | `ocm`, `ocm-api`, `live`, `paper`, `streaming` | 5 entrypoints registrados |
| `run.sh` | modos `ocm\|live\|paper`; **sin `streaming`** | **CONFIRMADO: SSOT roto** — run.sh declara ser SSOT de pyproject y no coincide |
| systemd streaming | `.venv/bin/python -m app.cli.streaming_hydra` | usa módulo directo, no el entrypoint registrado `streaming` |
| `apps/app/cli/streaming_hydra.py` | entrypoint real del canary ORDERBOOK (F2.6b); `load_appconfig_standalone`; `CompositionRoot.build_ws_producers` | primer módulo Python del modo streaming |

### 4.3 Acoplamientos a máquina

| Recurso | Evidencia | Conclusión |
|---|---|---|
| Código Python (`packages/`, `apps/`, `ocm/`, `shared/`) | grep `/home/orangemusic`, `orangehouse`, `orangemusic/trading` → **0 hits** | **CONFIRMADO: código app sin hardcodes de máquina** |
| `pyproject.toml` | `authors = [{name = "orangemusic"}]`; `repository = github.com/orangemusic/...` | metadata (pip/rueda), no runtime |
| `.github/CODEOWNERS`, `LICENSE` | mención `orangemusic` | dev/metadata |
| `deploy/host.env.example` y `.env` | `OCM_HOST_USER=orangemusic`, `OCM_REPO_ROOT=/home/orangemusic/trading/orangecashmachine` | **CONFIRMADO: ejemplo acoplado al host real** |
| `deploy/host.env` | idem (valores reales, no ejemplo) | **CONFIRMADO: acoplado al host real** |
| Units rendered + instaladas | rutas absolutas `/home/orangemusic/...` | artefacto local (gitignored rendered/); normal pero no portable por sí solo |
| `promtail.yml` | refs `orangehouse` | observabilidad local |
| `docs/` (PLAN-Maestro, planning, audits) | menciones `orangehouse` | documentación |

### 4.4 Configuración

| Recurso | Evidencia | Conclusión |
|---|---|---|
| Cascada de entorno | explicit → `OCM_ENV` → `.env` → `settings.yaml` → default `development` | cascada correcta |
| `ocm/config/env_vars.py` | SSOT de nombres de env vars | correcto (test guard) |
| `ocm/config/paths.py` | `data_lake_root`: `OCM_STORAGE__DATA_LAKE__PATH` → (deprecated `OCM_DATA_LAKE_PATH`) → YAML → `repo_root()/data_platform/data_lake` | **CONFIRMADO: existe fallback a `repo_root()`** |
| `shared/utils/repo.py` | `repo_root()` sube por padres buscando `.git`; `RuntimeError` si no | **CONFIRMADO: ejecución sin `.git` falla** |
| `config/env/production.yaml` | `dry_run: false`, `require_explicit_start: true`, `require_confirmation: true`, kafka `${oc.env:KAFKA_BOOTSTRAP_SERVERS,kafka:9092}` | hardening ok |
| `config/` | sin archivos `.py` (test `test_config_dir_is_data_only.py`) | declarativo ok |

### 4.5 Units systemd

| Recurso | Evidencia | Conclusión |
|---|---|---|
| Template streaming | `After=network-online.target docker.service`; `User=${OCM_HOST_USER}`; `WorkingDirectory=${OCM_REPO_ROOT}`; `EnvironmentFile=${OCM_REPO_ROOT}/deploy/host.env`; `ExecStart=${OCM_REPO_ROOT}/.venv/bin/python -m app.cli.streaming_hydra`; SafeOps | **deployed correct; sin readiness Kafka** |
| Template market-data | idem; `-m market_data.main` | idem |
| Rendered == instalado | `diff /etc/systemd/system/ocm-streaming.service deploy/systemd/rendered/ocm-streaming.service` → **IDENTICOS** | **CONFIRMADO** |
| Template vs rendered | `diff (con vars sustituidas)`: **`11a12 > EnvironmentFile=.../.env`** | **CONFIRMADO: la línea `EnvironmentFile=<root>/.env` existe en rendered/instalado pero NO en el template** → divergencia 3-vías (F-004) |
| SafeOps | `NoNewPrivileges`, `ProtectSystem=strict`, `ReadWritePaths=data_platform logs`, `PrivateTmp` | correcto |
| Recovery | `Restart=on-failure`, `RestartSec=10`, `TimeoutStopSec=30`, `KillSignal=SIGINT` | reintento genérico |
| Readiness Kafka | **sin `ExecStartPre`**, sin `Requires` de Kafka, sin espera a healthcheck | **CONFIRMADO: `After=docker.service` no implica Kafka listo (B-59)** |

### 4.6 Installer

| Recurso | Evidencia | Conclusión |
|---|---|---|
| `install_systemd.sh` | render (envsubst lista explícita de vars de host.env) → `systemd-analyze verify` → `install -m 644` → `daemon-reload`; flags `--verify-only`, `--user` | **CONFIRMADO: NO valida prerrequisitos, NO `enable`, NO `start`** |
| Precheck | falta validación de `.venv`, docker, rutas writable, vars obligatorias | instalación puede fallar sin diagnóstico claro |
| `--verify-only` | usado por gate G4/G11 (`_deployment_units_ok`) | integración CI ok |

### 4.7 Kafka: puertos y listeners

| Contexto | Host:puerto | Propósito | Fuente |
|---|---|---|---|
| INTERNAL (container→container) | `kafka:9092` | comunicación entre contenedores Docker | compose L509/530; schema.py:571; producer/consumer default |
| EXTERNAL (host→Kafka) | `localhost:${KAFKA_HOST_PORT:-9093}` | pipeline local/CLI/tests | compose L521/530; host.env:21 |
| Healthcheck Docker | `localhost:9092` (intra-container) | readiness del broker | compose L562 |
| Tests integración | `localhost:9093` (hardcoded) | broker real | tests/kafka/conftest.py:28 |
| `.env.example` | `KAFKA_HOST_PORT=9094` | **evitar colisión con Alertmanager 9093** (F-029/B-45) | `.env.example` |
| `deploy/host.env(.example)` | `localhost:9093` | bootstrap systemd | host.env:21 |
| Alertmanager | host port 9093 | infra observabilidad | compose L197 |

**CONFIRMADO:** no existe un "9093 correcto" universal. El runtime usa `KAFKA_BOOTSTRAP_SERVERS`; compose usa `KAFKA_HOST_PORT`. `.env.example` documenta 9094, `host.env.example` usa 9093, conftest hardcodea 9093 → **inconsistencia documental/de tests**.

### 4.8 Docker/infra

| Recurso | Evidencia | Conclusión |
|---|---|---|
| Kafka | healthcheck `kafka-broker-api-versions --bootstrap-server localhost:9092` (interval 60s, timeout 10s, retries 5, start_period 30s); `restart: unless-stopped` | healthcheck presente |
| kafka-ui | `profiles: [monitoring]`; `depends_on: kafka: condition: service_healthy` | coordina con healthcheck dentro de compose |
| systemd ↔ compose | **no hay coordinación**: systemd no ve healthchecks de compose | B-59 real |

### 4.9 Health/gates

| Recurso | Evidencia | Conclusión |
|---|---|---|
| `deploy/scripts/health_check.sh` | L1 process (systemd) / L2 dep (kafka via `docker exec ocm_kafka`, redis) / L3 data flow / L4 bronze; exit 0/1/2 | contrato de salud por dominio |
| `scripts/check_production_gates.py` | G1..G11; `_deployment_units_ok` (install_systemd.sh --verify-only); `_runtime_market_data_ok` (curl `localhost:8001/health`); `_git_clean_and_atomic` | gates asumen host actual: `localhost:8001`, `ocm_kafka`; G11 solo detecta `.env` en `git status --short` (**no `deploy/host.env`**) |

---

## 5. DEPENDENCIAS DEL SERVIDOR ACTUAL

| Dependencia | Tipo | Evidencia | Estado |
|---|---|---|---|
| Usuario `orangemusic` | usuario | `host.env`, `host.env.example`, units rendered/instaladas | ACOPLADO |
| `/home/orangemusic` | ruta | `OCM_REPO_ROOT` en host.env + units rendered | ACOPLADO |
| `orangehouse` | hostname | hostname real; menciones en docs/promtail | ACOPLADO (no runtime) |
| Repositorio `.git` | filesystem | `repo_root()` busca `.git` → RuntimeError sin él | RIESGO (bloquea instalación sin .git) |
| Rutas absolutas | ruta | units rendered/instaladas con `/home/orangemusic/...` | ARTEFACTO LOCAL (gitignored rendered/) |
| Puertos | red | Kafka 9093/9094, Redis 6379, services 8001-8003 | CONFIGURABLE vía env |
| Servicios externos | red | Kafka, Redis, exchanges WS (bybit/kucoin) | externo a OCM |
| Configuración local | config | `deploy/host.env`, `.env`, `config/*.yaml` | `.env` y host.env fuera del código |

**CONFIRMADO:** el código de aplicación (packages/, apps/, ocm/, shared/) no depende de ninguno de estos. La dependencia vive en la **capa de deployment** (units renderizadas, host.env versionado, repo_root(), docs).

---

## 6. HALLAZGOS

### 6.1 Confirmados por evidencia

- **H-DEP-01 (CONFIRMADO):** `deploy/host.env` versionado en Git con topología real del host, contradiciendo su propia cabecera ("NUNCA commitear") y escapando de G11.
- **H-DEP-02 (CONFIRMADO):** `repo_root()` requiere `.git` → instalación/ejecución sin checkout git falla (RuntimeError).
- **H-DEP-03 (CONFIRMADO):** SSOT de entrypoints roto: `run.sh` no incluye `streaming`; systemd usa `python -m` y no el script registrado `[project.scripts]`.
- **H-DEP-04 (CONFIRMADO):** Divergencia 3-vías de unit systemd (F-004): rendered==instalado, pero ambos añaden `EnvironmentFile=<root>/.env` que el template no declara.
- **H-DEP-05 (CONFIRMADO):** B-59 — `After=docker.service` no garantiza que Kafka escuche; sin `ExecStartPre`/readiness; reintento solo vía `Restart=on-failure`.
- **H-DEP-06 (CONFIRMADO):** Puerto Kafka inconsistente entre `.env.example` (9094), `host.env.example` (9093) y `tests/kafka/conftest.py` (9093 hardcoded).
- **H-DEP-07 (CONFIRMADO):** `install_systemd.sh` no valida prerrequisitos, no `enable` ni `start`; `install ≠ start` no diferenciado.
- **H-DEP-08 (CONFIRMADO):** gates de runtime (`check_production_gates.py`) asumen host actual: `localhost:8001`, `docker exec ocm_kafka`.
- **H-DEP-09 (CONFIRMADO):** No existe runbook ni documentación de deployment (INSTALL/DEPLOYMENT/OPERATIONS/TROUBLESHOOTING) en el repo.
- **H-DEP-10 (CONFIRMADO):** Código de aplicación sin hardcodes de máquina (grep limpio). Configuración externa y declarativa.

### 6.2 Riesgo potencial

- **R1:** ejecución en producción de un artifact sin `.git` (bloqueada hoy por `repo_root()`).
- **R2:** fuga de topología si `deploy/host.env` persiste en Git.
- **R3:** arranque no determinista si Kafka tarda o reinicia (sin B-59 resuelto).
- **R4:** futura instalación en Debian con usuario/ruta distintos requiere editar artefactos que hoy llevan valores hex del host.

### 6.3 No verificado

- **NV-1:** `systemd-analyze verify` en el host (no ejecutado por política read-only; solo se hizo `diff` de units).
- **NV-2:** readiness real de Kafka en arranque (el unit no lo implementa; no se probó).

### 6.4 Decisión pendiente (ver §14)

---

## 7. KAFKA READINESS (B-59)

**Hallazgo B-59 — `systemd_reinicia_correctamente`, PENDIENTE / NO_VERIFICADO.**

**Evidencia (CONFIRMADO):**
- Ambos templates declaran `After=network-online.target docker.service`; **sin `ExecStartPre`**, sin `Requires` de Kafka, sin espera a healthcheck.
- `After=` solo ordena la activación del unit respecto al daemon Docker; no implica que el broker escuche.
- El healthcheck de Kafka existe en Docker (`kafka-broker-api-versions --bootstrap-server localhost:9092`, interval 60s, start_period 30s) y compose lo coordina para kafka-ui (`condition: service_healthy`), pero **systemd no ve ese healthcheck**.
- Reintento de arranque actual: genérico `Restart=on-failure` + `RestartSec=10`.

**Responsabilidades de diseño (PROPUESTA, B-59):**
- **Docker:** healthcheck del broker (ya existe).
- **OCM (app):** reintento con backoff al construir producers (si el broker no responde, reintentar N veces con backoff; agotadas → exit 1 limpio para que systemd reintente). *(PROPUESTA)*
- **systemd:** reintento (Restart on-failure) — no decide readiness.
- **Operador/health:** `health_check.sh` l2_kafka + gates G6/G7.

**No verificado:** si `streaming_hydra` en su estado actual muere al instante con Kafka apagado o aguanta (NV-2).

---

## 8. CONFIGURACIÓN Y SECRETOS

**Inspeccionado:**
- Arquitectura de configuración: Hydra (base→env→exchange→pipeline) + `.env` + env vars; SSOT `env_vars.py`; cascada de entorno; `test_config_dir_is_data_only.py`.
- `config/env/production.yaml` hardened; example de `.env` (sin valores).
- Presencia/permisos de `.env` local (chmod 600) y su exclusión de Git (`.gitignore`; `.dockerignore`).
- credenciales resueltas por `ocm/config/credentials.py` (SSOT de credenciales exchange; cae a "" si falta — fail-safe).

**Deliberadamente NO inspeccionado (seguridad):**
- **Contenido real de `.env`** (valores de API keys, JWT, passwords). Solo se verificó existencia y permisos.
- **Offsets/logins reales del broker Kafka**.
- **Credenciales headseat de exchanges** en las cuentas del host.

**Conclusión:** la separación config/secretos es correcta; el riesgo de secretos está bien mitigado (`.env` fuera de Git, chmod 600, hardening test). Los `EnvironmentFile` de systemd deben uniformarse (H-DEP-04).

---

## 9. deploy/host.env

**Qué contiene/representa (CONFIRMADO):**
- Topología del host: `OCM_HOST_USER=orangemusic`, `OCM_REPO_ROOT=/home/orangemusic/trading/orangecashmachine`, `OCM_ENV=development`, puertos/paths de market-data, `KAFKA_ENABLED=true`, `KAFKA_BOOTSTRAP_SERVERS=localhost:9093`, Redis localhost.
- **No contiene secretos** (API keys viven en `.env`).

**Por qué está acoplado al host:**
- Los valores son del servidor actual, no valores genéricos. `host.env.example` repite los mismos valores reales (debería ser neutro).

**Propuesta de sacarlo del repositorio (PROPUESTA):**
- `git rm --cached deploy/host.env` + añadir `deploy/host.env` a `.gitignore`.
- Rellenar `deploy/host.env.example` con valores neutros (`OCM_HOST_USER=ocm`, `OCM_REPO_ROOT=/opt/orangecashmachine`).
- Ampliar G11 (`_git_clean_and_atomic`) para detectar `host.env` en `git ls-files`, no solo `.env`.

**Implicaciones para el instalador (PROPUESTA):**
- El installer ya requiere `deploy/host.env` (`cp host.env.example host.env`) → sacarlo de Git no rompe instalación (el operador lo crea local en cada host).
- Debe conservarse `deploy/host.env` en el host actual antes de cualquier cambio (para no dejar el servidor sin unidad).

---

## 10. RELEASE Y ROLLBACK

**Modelo actual:** `git pull` + `uv sync` + restart → no versionado por release.

**Estrategia propuesta (PROPUESTA):**
| Etapa | Mecanismo |
|---|---|
| DEV | `git pull` + `uv sync` (lock) |
| STAGING | tag `vX.Y.Z` + gates G1-G11 + `uv sync` con lock congelado |
| PROD | release por tag + lock congelado; deploy script idempotente |
| Rollback | restaurar lock anterior + `uv sync` + restart (o imagen previa si Docker) |

**Reproducibilidad:** `uv.lock` pin completo; `requires-python >=3.11,<3.14`; release = tag semver verificado en CI.

---

## 11. OCM DOCTOR

**Propuesta (PROPUESTA):** superficie de diagnóstico `ocm doctor` (CLI), reutilizando capas existentes.

**Checks (seguro, sin exponer secretos):**
- Python/uv/venv y versiones; packaging/entrypoints (`[project.scripts]`).
- Configuración: `OCM_VALIDATE_ONLY` equivalent; Hydra+Pydantic.
- Secretos: **solo presencia** (`VERIFIED`/`MISSING`), **nunca valores**.
- Paths (`repo_root`, data lake, config dir) y permisos/writability.
- Kafka (bootstrap reachable, topics), Redis, Docker Compose up, systemd (units enabled/estado).
- Connectivity a exchanges y filesystem (espacio).

**Forma:** CLI + reutilizar `health_check.sh` (L1-L4) + `/health` del service. Regla dura: no imprimir valores de secretos en ningún check.

---

## 12. DISEÑO TARGET

**Flujo objetivo (PROPUESTA):**
```
 OCM RELEASE (repo + uv.lock + [project.scripts])
      │
      ├───────────────┬────────────────┐
      │ cam. oficial  │ alternativa    │
      ▼               ▼                │
 NATIVE + UV      DOCKER-IMAGE      (no soportado:
   │   │          (compose)          install manual /
   ▼   ▼                            ad-hoc)
 .venv            contenedores
   │   │
 systemd (systemd supervisor)
   │   │
 .env + host.env  (EnvironmentFile)
      │
  infra Docker: Kafka/Redis/obs (healthchecks)
```

**EJEMPLO DEBIAN NUEVO (PROPUESTA):**

```
Debian 13 · usuario=ocm · instalación=/opt/orangecashmachine

[1] useradd -r -m -s /bin/bash ocm
[2] git clone <url> /opt/orangecashmachine   (o release artifact sin .git)
[3] chown -R ocm:ocm /opt/orangecashmachine
[4] cp deploy/host.env.example deploy/host.env
      OCM_HOST_USER=ocm
      OCM_REPO_ROOT=/opt/orangecashmachine
      KAFKA_BOOTSTRAP_SERVERS=localhost:9094
[5] cp .env.example .env   (chmod 600, ownership ocm)
[6] sudo -u ocm uv sync    (crea .venv)
[7] docker compose up -d (infra) + provision_kafka_topics.py --bootstrap-server localhost:9094
[8] deploy/scripts/install_systemd.sh --install → --enable → --start
[9] deploy/scripts/health_check.sh → MARKET_DATA_HEALTHY=HEALTHY
```

**Principios target:** instalación por script idempotente; usuario dedicado no-interactivo; datos/logs fuera de la ruta de código cuando aplique (configurado por env); entrypoint oficial vía `[project.scripts]`; sin `.git` requerido; sin rutas/usuario del host actual en código ni templates.

---

## 13. FASES 0–8

| Fase | Objetivo | Archivos/componentes | Resultado esperado | Validaciones | Riesgos | Autorización requerida |
|---|---|---|---|---|---|---|
| **F0 Preparación** | Baseline y preservar estado | — (documentación) | estado documentado; `docs/plans/tracking.yaml` intacto | `git status` | bajo | aprobación de inicio |
| **F1 Packaging/entrypoints** | Alinear SSOT; repo portable | `run.sh`, `shared/utils/repo.py`, `[project.scripts]` | run.sh incluye `streaming`; `repo_root()` acepta `OCM_REPO_ROOT` y fallback package-root; error explícito si nada | `./run.sh streaming --help`; unit test `repo_root` sin `.git`; ruff/mypy/lint-imports; `uv run pytest tests/ -x` | bajo | aprobación F1 |
| **F2 Configuración/topología** | Host neutral; Kafka SSOT | `deploy/host.env.example`, `deploy/host.env` (rm --cached), `.gitignore`, `check_production_gates.py` G11, `tests/kafka/conftest.py`, `.env.example` | `host.env` fuera de Git; ejemplo neutro; `KAFKA_HOST_PORT=9094` canónico; G11 detecta host.env | `git ls-files` sin host.env; G11 pasa con/sin host.env local; `pytest tests/kafka` | medio (operador debe recrear host.env antes de deploy) | aprobación F2 (incluye elrm) |
| **F3 Deployment tooling** | Installer contractual | `install_systemd.sh`, templates units | PRECHECK/INSTALL/ENABLE/START/VERIFY; idempotente; precheck de prereqs | `--verify-only` en CI (G4); máquina de prueba con 2 runs → mismo estado; `systemd-analyze verify` 0 errores; diff template↔rendered salvo vars | medio | aprobación F3 |
| **F4 systemd/readiness (B-59)** | Tolerancia Kafka lenta/reinicios | templates units (readiness), `streaming_hydra.py` (backoff), health_check | OCM arranca antes que Kafka y aguanta (backoff); reinicio → recovery; verificación externa | prueba con Kafka apagado→encendido; matar Kafka en runtime; reboot convivo | medio | aprobación F4 (toca canary streaming) |
| **F5 Seguridad/perfil servicio** | Hardening + usuario dedicado | templates units (LimitCORE=0, PrivateDevices), docs | units endurecidas; usuario `ocm`; secrets fuera del servicio | `ocm doctor`; `systemd-analyze verify`; permisos | medio | aprobación F5 (cambio de usuario en prod afecta ownership) |
| **F6 Doctor/operabilidad** | Superficie diagnóstica | `apps/app/cli/doctor.py`, `ocm/diagnostics/` | `ocm doctor` funcional sin exponer secrets | `ocm doctor` en máquina sana y rota; verificación de no-impresión de secrets | medio | aprobación F6 |
| **F7 Documentación** | Runbook + operación | `INSTALL.md`, `DEPLOYMENT.md`, `OPERATIONS.md`, `TROUBLESHOOTING.md` | operador nuevo instala end-to-end siguiendo solo docs | checklist runbook | bajo | aprobación F7 |
| **F8 Release/update + Debian limpio** | Release por tag + validación | scripts release, audit doc, máquina Debian limpia | tag semver + lock congelado + rollback; prueba §12 en `ocm`/`/opt`, reboot/recovery | ejecutar §12 en Debian limpio; reboot; recovery; `pytest` integración | medio | aprobación F8 (ejecución en otra máquina) |

---

## 14. DECISIONES PENDIENTES (requieren autorización humana)

1. **Camino oficial:** nativo (script + uv + systemd + infra Docker) vs contenedor total. Recomendación: **nativo**.
2. **Entrypoint systemd:** script registrado (`ocm-streaming`) vs módulo (`python -m`). Recomendación: **script**.
3. **B-59 estrategia de readiness:** backoff en app (recomendado) vs `ExecStartPre` vs combinado.
4. **Mover `.env` fuera del repo** (`~/.ocm/.env`) vs mantener `<repo>/.env`.
5. **Ampliar G11** a `deploy/host.env` y ejecutar el `git rm --cached` (F2) — implica commit autorizado.
6. **Usuario de servicio dedicado** `ocm` y migración del servidor actual (ownership de datos).
7. **Modelo de release** a producción por tag + lock congelado + rollback (y promoción a CI/CD).
8. **Crear `ocm doctor` como superficie pública**.
9. **Puerto Kafka canónico 9094** para host-listener y adopción en tests/infra.

---

## 15. LIMITACIONES DE LA AUDITORÍA

**Deliberadamente no inspeccionado o no ejecutado (por seguridad o read-only):**

1. `.env` real (valores de secretos) — NO VERIFICADO (solo existencia/permisos; chmod 600 protegido).
2. Offsets/logins reales de Kafka — NO VERIFICADO.
3. `systemd-analyze verify` en el host — NO VERIFICADO (read-only; solo diff de units).
4. Prueba en Debian limpio (usuario `ocm`, `/opt/orangecashmachine`) — NO VERIFICADO (no había máquina disponible; diseño §12 es PROPUESTA).
5. Readiness real de Kafka en arranque — NO VERIFICADO (NV-2).
6. `docker inspect` de healthchecks en vivo — NO VERIFICADO (compose no tocado).
7. Contenido de credenciales de exchanges en el host — NO VERIFICADO.

Ningún hallazgo declarado en este documento depende de lo anterior para su validez; las limitaciones se listan para no sobrevender la auditoría.

---

## Estado final

- **git status --short:** ` M docs/plans/tracking.yaml` (preexistente, de otro agente — preservado).
- **Modificaciones de este agente:** solo este documento `docs/audits/AUDIT_OCM_deployment-portability_2026-09-17.md`.
- **Commits:** 0 · **Pushes:** 0 · **Servicios:** 0 · **Sistema:** 0.