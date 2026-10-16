# Deployment & Runtime Architecture Audit

> **Tipo:** Auditoría READ-ONLY de viabilidad de arquitectura objetivo (deployment/runtime).
> **Fecha:** 2026-09-19
> **Branch/HEAD auditados:** `feat/b56-ci-stage-ordering` @ `b8b13240`
> **Metodología:** `docs/governance/AUDIT_PROTOCOL.md` (Control FAIL ≠ Finding Nuevo; modelo de fuentes; …).
> **Veredicto:** **PARTIALLY** — el modelo A+B (servicios nativos + infra externa por contrato) ya existe y es sólido; la propuesta (`install.sh` en raíz → `deploy/install.sh` orquestador → `deploy/{runtime,infrastructure,systemd,monitoring,config,health}/`) es viable como *facade orquestadora* sobre componentes que ya existen, pero varias rutas/directorios propuestos están ya cubiertos (reorganización, no adición) y hay incoherencias de CD/script/systemd que deben resolverse antes de prometer idempotencia.
> **Único archivo escrito por la auditoría:** este documento (`docs/audit/deployment-runtime-architecture-audit.md`).

---

## 1. Executive Summary

OCM no tiene hoy ningún `install.sh` ni Makefile (verificado: `find . -name install.sh` → vacío; no hay Makefile). La instalación operativa real se compone de `uv sync` + `docker compose up -d` + scripts de `deploy/scripts/` (`install_systemd.sh`, `health_check.sh`, `deploy_ocm.sh`), con precedente claro de launcher raíz en `run.sh` (facade que delega en `uv run python -m …` y declara SSOT con `pyproject.toml [project.scripts]`).

La arquitectura propuesta (raíz = interfaz pública, `deploy/` = implementación operacional, systemd versionado, OCM nativo + infra externa por contrato, DEV Docker vs PROD externo) **coincide en un 70-80% con el estado ya existente y con los ADR vigentes (ADR-0022, ADR-0024, ADR-0037)**. La brecha real no es estructural sino de **consolidación**: (a) falta un orquestador único de instalación (hoy el conocimiento vive en README + scripts sueltos), (b) hay incoherencias verificadas entre el CD de Docker (deploy_ocm.sh/ADR-0037) y el runtime nativo systemd (split-brain), (c) `check_production_gates.py` referenciar `scripts/install_systemd.sh` que no existe (debe ser `deploy/scripts/`), (d) el systemd renderizado sufre drift respecto a los templates, (e) gobernanza de deployment ausente en GOVERNANCE.md, (f) `ocm-trading.service` es prematuro (ADR-0024: sin `trading.main`).

El veredicto por sección clave: root `install.sh` = **SÍ viable como facade** (patrón `run.sh` ya lo demuestra); `deploy/install.sh` orquestador = **SÍ viable si reutiliza** los 3 scripts existentes y `provision_kafka_topics.py`; nuevos directorios `deploy/runtime|infrastructure|config|health` = **NO necesarios / contraproducentes hoy** (uv+pyproject, docker-compose raíz y `config/` Hydra ya cumplen); contrato Kafka/Redis desacoplado del mecanismo de instalación = **SÍ, ya implementado** con caveats (`kafka:9092` por defecto en Python = foot-gun bare-metal).

---

## 2. Current Repository Structure

```
orangecashmachine/
├── run.sh                      # launcher raíz (facade): ocm|live|paper → uv run python -m …
├── pyproject.toml              # SSOT build/deps/tools + [project.scripts]
├── docker-compose.yml          # infra por defecto (Kafka/Redis/Prom/…); microservicios profile opt-in
├── docker-compose.override.yml # SOLO dev: redis 6379 expuesto; bloque market-data COMENTADO
├── config/                     # Hydra: base → env → exchange → pipeline → CLI → env vars
├── ocm/                        # plataforma (config/runtime/observability), sin lógica de negocio
├── apps/{app,api,research}/    # entrypoints CLI / FastAPI / consumidor read-only
├── packages/{market_data,trading,portfolio}/  # BCs (Clean/Hexagonal)
├── shared/                     # capa más baja (stdlib + 3rd-party aprobados)
├── architecture_linter/        # linter gobernanza (AST, stdlib-only) — fuera de packages/apps
├── scripts/                    # guards de gobernanza/CI + utilidades (ver §7/§26)
├── deploy/                     # EL BOUNDARY OPERACIONAL REAL (ver §3)
│   ├── host.env(.example)      # render plantilla → /etc/ocm/*.env (install_systemd.sh)
│   ├── scripts/                # deploy_ocm.sh, install_systemd.sh, health_check.sh
│   ├── systemd/                # templates/ + rendered/ (gitignored)
│   └── monitoring/             # prometheus/alertmanager/grafana/loki/promtail (+ dashboards provisionadas)
├── infrastructure/             # paquete VACÍO tracked (único __init__.py) — LEGACY
├── artifacts/                  # f26c/ (artefactos CD locales sin evidencias)
└── docs/                       # architecture/ (ADR-0000..0037+), audits/, governance/, planning/, plans/
```

**Observaciones clave de estructura:**
- No existe directorio `deploy/runtime`, `deploy/infrastructure`, `deploy/config` ni `deploy/health` (propuesta §4).
- `scripts/` y `deploy/scripts/` son dos zonas distintas: la primera = gobernanza/CI; la segunda = operaciones de deployment. La propuesta "scripts/" dentro de `deploy/` ya existe como `deploy/scripts/`.
- `docker-compose.yml.bak`, `pyproject.toml.bak`, `PLAN-Maestro-Ingenieria.md.bak`, `.docs.swp` existen en disco pero **NO están trackeados** (gitignore `*.bak`/`*.swp`) — higiene de repo correcta, vestigios locales.
- Raíces de import-linter: `packages, shared, apps, ocm`; `infrastructure/` tracked vacío aparece en comandos de bandit/pre-commit (LEGACY).

---

## 3. Current Deployment Architecture

Modelo **A+B intencional** (confirmado por `docker-compose.override.yml`: *"Por defecto el pipeline corre en la maquina local (fuera de Docker)"* y por ADR-0024):

```
        +------------------------------- HOST (OrangeHouse, single-host) -------------------------------+
        |                                                                                                |
        |  A) SERVICIOS NATIVOS (uv/.venv)           B) INFRA EXTERNA (Docker)                            |
        |  ---------------                           -------------------                                  |
        |  market-data-service   (:8001 /health…)   Kafka (port 9092/9093)  ← EXTERNAL localhost:9093      |
        |  streaming (Hydra)                        Redis (6379)            ← dev override expone 6379     |
        |  api gateway (:8000 /ready → Redis)       Prometheus 9090, Alertmanager 9093 (F-05: .env 9094)   |
        |  ocm/paper/live (CLI, profile opt-in)     Pushgateway 9091, Grafana 3000                        |
        |  ↑ Systemd units (templates→rendered)    Loki/Promtail 3100/9080                                |
        +------------------------------------------------------------------------------------------------+
```

- **Instanciación de servicios:** el kernel del pipeline corre como procesos nativos supervisados por systemd (units renderizadas desde `deploy/systemd/templates/`, instaladas por `deploy/scripts/install_systemd.sh` a `/etc/systemd/system` con `systemctl daemon-reload`; `--user` también soportado). Market-data es un microservicio FastAPI real (`packages/market_data/main.py:465-519` `/health`, `/ready`), streaming incorpora Ticker→OrderBook (Kappa) alimentado por `FeedOrchestrator` (ADR-0022).
- **Infra:** Docker Compose (root) provee Kafka/Redis/observabilidad; perfiles de servicio opt-in; contrato por variables de entorno (`KAFKA_BOOTSTRAP_SERVERS`, `REDIS_HOST/PORT/DB`, `*_HOST_PORT`).
- **CD:** `deploy_ocm.sh` (approve/rollback) + `.github/workflows/ocm-cd.yml` = **placeholder deshabilitado (workflow_dispatch only)**, catalogado en ADR-0037 (estado **Propuesto**).
- **Observabilidad:** Prometheus scrape de targets, dashboards Grafana provisionadas, alertas, OTel tracing runtime (`ocm/observability/tracing.py`, fail-soft, disabled por defecto `tracing.enabled:false`).
- **Configuración:** Hydra (base→env→exchange→pipeline→CLI→env) + `deploy/host.env` para sistemas operativos + vals secrets vía `ocm/config/env_vars.py` (SSOT).

---

## 4. Proposed Target Architecture

(Objetivo evaluado, según encargo)

```
raíz
├── install.sh                  # launcher PÚBLICO (interfaz única)
├── deploy/
│   ├── install.sh              # orquestador de instalación (delegado por raíz)
│   ├── runtime/                # toolchain Python/uv/.venv
│   ├── infrastructure/         # (¿mover docker-compose?; Kafka/Redis externos)
│   ├── systemd/                # units VERSIONADAS en repo
│   ├── monitoring/             # ya existe
│   ├── config/                 # configuración operacional (host.env? env files?)
│   ├── health/                 # probes L1..Ln
│   └── scripts/                # ya existe
```

Regla declarada: **"LA RAÍZ EXPONE LA INTERFAZ PÚBLICA. DEPLOY CONTIENE LA IMPLEMENTACIÓN OPERACIONAL."**

Veredicto por pieza:

| Pieza | Veredicto | Razón principal reconstructible |
|---|---|---|
| `install.sh` raíz | **SÍ** (facade fina) | Patrón ya validado por `run.sh` (delega en `uv run python -m …`, SSOT `[project.scripts]`). NO debe contener lógica de instalación. |
| `deploy/install.sh` orquestador | **SÍ** (estadofull, idempotente, `--check/--dry-run/--apply`) | Todas sus etapas ya existen como componentes (ver §30). Riesgo único: duplicar/desincronizar con README+run.sh. |
| `deploy/runtime/` | **NO** | uv + `pyproject.toml` gestionan el runtime de forma declarativa (`.venv`); un dir de runtime duplicaría el SSOT. El "runtime" real = `uv sync` + units systemd + `EnviromentFile`. |
| `deploy/infrastructure/` | **NO hoy** (decision ADR) | `docker-compose.yml` raíz + `config/` no es "infrastructure" de aplicación; moverlo implica trazar dokumentación/CI/ADR-0037 (blast radius alto). Opcional fase-2 con ADR explícito. |
| `deploy/systemd/` versionado | **SÍ** (ya existe parcialmente) | Solo templates en VCS; `rendered/` es artefacto generado (gitignored) — correcto. Drift actual §25/F-4. |
| `deploy/config/` | **PARCIAL** | La config operacional hoy es `deploy/host.env(.example)` + Hydra `config/`. Unizar/unificar `deploy/config/` es reorganización; requiere trazar `config/`, `host.env`, unidades. |
| `deploy/health/` | **OPCIONAL** | `health_check.sh` (L1-L4) ya vive en `deploy/scripts/` y es consumido por gate G7. Mover = churn sin ganancia. |
| systemd versionado instalado al SO | **SÍ** | Correcto en Debian: `/etc/systemd/system` = dominio del operador (no `/usr/lib`). `daemon-reload` + `enable/start` deben ser pasos del orquestador (§11). |
| OCM nativo + infra externa por contrato | **SÍ** | Es el modelo A+B ya vigente; los contratos existen (`KAFKA_BOOTSTRAP_SERVERS`, `REDIS_*`). Caveat: defaults Python `kafka:9092` (§F-9). |
| DEV Docker vs PROD externo | **SÍ** | Compose raíz = dev; override/notas confirman "pipeline corre en la maquina local". |

---

## 5. Feasibility Assessment

**¿Es factible la arquitectura objetivo? → SÍ, con precisión de alcance.**

1. **El modelo A+B es la versión ya implementada del objetivo.** No se parte de cero; se consolida. La propuesta es una **normalización** (orquestador único, plantillas systemd SSOT, contratos explícitos), no una re-plataforma.
2. **La única pieza genuinamente nueva es un orquestador de instalación** (`install.sh` raíz + `deploy/install.sh`). Su complejidad es la de un "script que encadena componentes existentes en orden fijo e idempotente", no un framework.
3. **Tensión a resolver (cuello de botella):** el CD documentado (ADR-0037, "Propuesto") depliega OCM **en Docker** (imagen + `docker compose up -d`), mientras el runtime canónico operativo es **nativo/systemd**. Hay split-brain §F-1. Factible si se fija un modelo canónico (veredicto: native-first) y se corrige o depreca el camino Docker-CM.
4. **Restricciones de factibilidad técnica confirmadas:**
   - `ocm-trading.service` (propuesta sistemd lista como `ocm-trading.service` en sección 30 del encargo) **no es implementable hoy**: no existe `trading.main`/entrypoint; ADR-0024 lo posiciona "Propuesto/NIVEL 2" (§F-11).
   - `--cfg job` expone secrets (Hydra pre-Pydantic) — el orquestador jamás debe pipearlo a logs con secrets.
   - Idempotencia de la etapa "config": copia `.env(.example)` **solo si no existe**; nunca sobreescribir configuraciones de usuario (host.env/.env) — de lo contrario NO idempotente y destructivo.
   - `provision_kafka_topics.py` y `restart` systemd: provision es IDEMPOTENT; reinstalar units es PARTIALLY (overwrite + daemon-reload idempotentes; enable/start NO deben repetirse ciegamente).
5. **Riesgo principal:** over-engineering / duplicación si `install.sh` se vuelve "lógica de deployment" en raíz en vez de facade. Mitigación: el orquestador vive en `deploy/`, la raíz solo `exec ./deploy/install.sh "$@"`.

---

## 6. Root `install.sh` Assessment

**Pregunta:** ¿tiene sentido `install.sh` en raíz como launcher público?

**SÍ, como facade fina — y el repo ya demuestra el patrón con `run.sh`.**

- **Las preguntas del encargo, respondidas:**
  - ¿launcher/facade **vs** lógica? → **Facade sólamente.** La raíz expone el contrato público; la lógica vive en `deploy/`. Ejecución: `./install.sh` → `exec ./deploy/install.sh "$@"` (idéntico al estilo de `run.sh`).
  - ¿delegar en `deploy/install.sh`? → **Sí.** Coherente con `run.sh` delegando en `uv run python -m …` y con `deploy/scripts/*` como implementación operacional.
  - ¿otro nombre? → `install.sh` es correcto y autoexplicativo. Alternativa `bootstrap.sh`/`setup.sh` es intercambiable; se recomienda mantener `install.sh` para "instala/restaura/verifica" vs `run.sh` para "ejecuta".
  - ¿entry point equivalente ya existe? → **Existe el patrón de launcher (`run.sh` + `[project.scripts]`)**, pero NO existe un *instalador*. `run.sh` ejecuta; no instala. La brecha real es de orquestación de instalación, no de ejecución.
- **¿Duplica funcionalidad?** En estado actual NO. `run.sh` = ejecución; `install.sh` = instalación/verificación/desinstalación. Eso sí, si `install.sh` replicara `uv sync` + `docker compose up` *por sí mismo* sin delegar, DUPLICARÍA los pasos ya documentados en README y en CI. Riesgo de duplicación gestionado con: (1) el orquestador llama a los scripts existentes, (2) el SSOT de entrypoints se mantiene en `[project.scripts]`, (3) `--verify-only/-check` son modos de lectura.
- **Ready state:** B-57 cerrado (CD per ADR-0037), B-59 PENDIENTE (`systemd_reinicia_correctamente: NO_VERIFICADO`). Un `install.sh` с `--check` ayudaría a cerrar B-59, pero NO lo sustituye (requiere prueba de reinicio real).
- **Implicaciones CI/CD/dev/prod:** un `install.sh` consistentey idempotente permite: gates de instalación en CI (G4 ya intenta algo similar con `install_systemd.sh --verify-only`), reproducibilidad dev/prod, y un solo entry público. **Atención:** introducir `install.sh` toca docs/README/ADR (normativa de Root Interface) — requiere decisión formal si se promueve a contrato.

---

## 7. `deploy/install.sh` Assessment

**Pregunta:** ¿`deploy/install.sh` como orquestador? → **SÍ, si es la orquestación (no la implementación) de componentes existentes.**

- Etapas propuestas y su mapeo a lo que YA existe:

| Etapa del orquestador | Componente existente | Estado implementado |
|---|---|---|
| runtime (bootstrap Python/uv/.venv) | `uv sync` (+`--group dev` en CI); `pyproject.toml [project.scripts]`; Dockerfile usa UV | IMPLEMENTED |
| infrastructure (Kafka/Redis/…) | `docker compose up -d` (raíz) + `config-guard` + `depends_on: service_healthy`; `provision_kafka_topics.py` (idempotente, SSOT) | IMPLEMENTED (partial: provisioning manual) |
| configuration | `deploy/host.env(.example)` render → `/etc/ocm/*.env`; Hydra `config/`; copia `.env(.example)`solo-si-no-existe | IMPLEMENTED (render) / PARTIAL (orquestación) |
| systemd | `deploy/scripts/install_systemd.sh` (`--verify-only`, `--user`, install+daemon-reload) | IMPLEMENTED |
| monitoring | `docker compose` (grafana/prom/pushgateway/loki/promtail) provisionadas | IMPLEMENTED |
| health | `deploy/scripts/health_check.sh` (L1-L4) — consumido por gate G7 | IMPLEMENTED |
| CD (deploy) | `deploy_ocm.sh` (rollback/.prev_sha) — ADR-0037 Propuesto | CODE_ONLY / PARTIAL |

- **Recomendación de firma:** `deploy/install.sh [--check|--dry-run|--apply] [--user] [stage…]`; salida estructurada para gates; fail-fast; sin secrets en stdout; idempotente (ver §22).
- **Advertencia de colisión de nombres:** existe `deploy/scripts/install_systemd.sh`. Un `deploy/install.sh` es distinto, pero se recomienda no crear más `install*.sh` sueltos; el conjunto {`install.sh`, `install_systemd.sh`} + `deploy_ocm.sh` debe documentarse como "orquestador → operaciones" en GO/README.
- **Regla de oro:** el orquestador NO implementa instalación; SOLO secuencia llamadas a componentes y verifica invariantes. Si empieza a contener lógica de instalación (copy de units, gestión de venv, edición de compose), duplica y sobrepasas el boundary.

---

## 8. `deploy/` Boundary Assessment

**¿Es `deploy/` un boundary? → SÍ, de facto ya lo es; falta declararlo formalmente.**

- **Evidencia de boundary de facto:**
  - No es un paquete Python ni raíz de import-linter (`architecture_linter/architecture_linter.toml` raíces: `packages, shared, apps, ocm`). No hay imports de aplicación → deploy.
  - Contiene SOLO artefactos operacionales: `scripts/`, `systemd/{templates,rendered}/`, `monitoring/`, `host.env(.example)`. Ningún código de negocio.
  - `check_production_gates.py` (scripts/, gobernanza) consume `deploy/scripts/*` como gates G4/G6/G7 — el gobernanza ya trata `deploy/scripts/` como el plano de deployment oficial.
- **Falta formalidad:**
  - No hay regla gobernada («deploy/ no importa código de aplicación; la aplicación no referencia deploy/ en runtime») — soloun precedente.
  - `infrastructure/` raíz (paquete vacío tracked) es un vestigio que granulariza del "boundary" y confunde a bandit/pre-commit (§F-16).
- **Recomendación:** añadir al frame de GOVERNANCE + ADR un invariante: *"`deploy/` es el único boundary operacional de instalación; EMPIEZA desde host.env/systemd/monitoring y NO desde código de aplicación; la raíz expone `run.sh`/`install.sh` (facades)."* (Mecanismo formal: ADR + regla en linter si se quiere gobernar con código.)

---

## 9. Runtime Assessment

**Pregunta:** ¿"runtime" como directorio `deploy/runtime/`? → **NO.**

- El runtime OCM es 100% Python con gestionado por **uv**: `requires-python ">=3.11,<3.14"` (sin `.python-version`), `.venv` local, entrypoints declarados en `[project.scripts]` (`ocm`, `ocm-api`, `live`, `paper`, `streaming`). Añadir `deploy/runtime/` introduciría un segundo SSOT de toolchain.
- El runtime efectivo en PROD = `.venv` + `EnvironmentFile` (deploy/host.env rendereado a `/etc/ocm/*.env`) + units systemd. No requiere directorio nuevo.
- **Lo que SÍ conviene robustecer:**
  - Freeze de toolchain: fijar `python.version` en `pyproject.toml [tool.uv]` o `.python-version` para determinismo host (hoy `>=3.11,<3.14` deja deriva).
  - Validación de entorno ya existe: `ocm/config/validator.py` (`EnvironmentValidator`, modo fail-fast L5) y `dry_run: true` default en `config/base.yaml`, `false` en production.yaml. Mantener y documentar en el orquestador (`--check` degrade el fail-fast?).
  - El orquestador debe ejecutar `uv sync` bajo el MISMO usuario del unit (unprivileged); nunca `sudo uv sync`.

---

## 10. Infrastructure Assessment

- **El contrato de infra ya está desacoplado del mecanismo de instalación:**
  - Kafka: `KAFKA_BOOTSTRAP_SERVERS` (free-form `host:port,...`) — puede apuntar a Docker (`kafka:9092`), a externo (`localhost:9093`) o a host remoto. Listeners configurados en compose: EXTERNAL `localhost:9093`, INTERNAL `kafka:9092` (+ KAFKA_ADVERTISED_LISTENERS).
  - Redis: `REDIS_HOST`, `REDIS_PORT`, `REDIS_DB`, `REDIS_PASSWORD` (SSOT `ocm/config/env_vars.py`). **No existe `REDIS_URL`** (contrato actual usa variables discretas — respetar).
  - Puertos de observabilidad: `*_HOST_PORT` (factores `_HOST_PORT` en `.env`, `config.yaml`).
- **Resultado:** cambiar "Kafka/Redis en Docker" por "Kafka/Redis en otro host/baremetal" NO toca el código de la aplicación — solo variables de entorno. La premisa de la propuesta ("infra externa por contrato") **se cumple HOY** con **dos caveats** (§F-9, §F-5).
- ¿Mover `docker-compose.yml` a `deploy/infrastructure/`? → NO recomendado en esta iteración: la ruta aparece en `README`, `deploy_ocm.sh`, ADR-0037, CI y config-guard; es churn con blast radius alto y ningún beneficio contractual (el contrato ya es por env vars, no por path del compose).

---

## 11. Systemd Assessment

**Pregunta:** ¿units systemd versionadas en repo e instaladas al SO? → **SÍ.**

- **Fuente de verdad:** `deploy/systemd/templates/*.service` (declarativo, VCS). `deploy/systemd/rendered/*.service` = artefacto generado (gitignored; `.gitignore:75`). Correcto: la traza `repo → template → installer → systemd` es **sana**.
- **Ruta de instalación (Debian):** `install_systemd.sh` rinde a **`/etc/systemd/system`** (DEST_DIR). En política Debian, `/usr/lib` (o `/usr/local/lib/systemd/system` para software no distro) lo usan *packages*; `/etc/systemd/system` es el dominio del **operador/administrador local** → para unidades instaladas por script de operación es LA ruta correcta (override y administración manual). `systemctl daemon-reload` (presente) es obligatorio tras escribir/alterar units.
- **Flujo actual y verificación:**
  - `install_systemd.sh` hace render→(verify-only o install+daemon-reload), soporta `--user` (`~/.config/systemd/user`). Gate G4 invoca `--verify-only` (con bug de path §F-3).
  - `health_check.sh` L1 valida servicio activo vía `systemctl is-active`.
  - **Falta en el flujo:** `enable` + `start`/`restart` no están formalizados como pasos de instalación (documentados en README/ADR-0022 como operación). Un `install.sh --apply` debería: render → verify → install → daemon-reload → enable → start, con idempotencia (no re-start si ya corre salvo `--reinstall`/flag).
- **Drift verificado §F-4:** rendered vs templates difieren (`EnvironmentFile .env` extra en algunas units renderizadas; paths hardcodeados contra templates con placeholders). El verificador debe comparar `rendered` contra re-render en `--verify-only` (Fast drift detectado).
- **Seguridad/SO:** units `644 root:root`; `User=orangemusic` (unprivileged) — correcto; `EnvironmentFile=/etc/ocm/*.env` con perms 600 (reach out host.env; evitar secrets en `Environment=` inline). `--user` alternativo OK para servicios de usuario.
- **`ocm-trading.service`: NO hoy** (§F-11): no existe `trading.main`; ADR-0024 lo declara NIVEL 2 "Propuesto". Su unit no es versionable operativamente (nada que arrancar). Mientras tanto `ocm.service` (pipeline CLI) + `ocm-market-data.service` + `ocm-streaming` son el conjunto real.

---

## 12. Docker Assessment

- **Uso dual intencional:**
  1. **Infra (dev/PROD por defecto):** Compose raíz provee Kafka/Redis/Pushgateway/Alertmanager/Grafana/Prometheus/Loki — el "stack" citado en README con `*_HOST_PORT`.
  2. **Microservicios (profile opt-in):** `docker-compose.yml` profiles `microservices`/`streaming`; `market-data` docker en override está **COMENTADO** con nota *"Por defecto el pipeline corre en la maquina local (fuera de Docker)"* → modelo A+B explícito.
- **CD de OCM en Docker (ADR-0037/deploy_ocm.sh):** camino conflictivo (§F-1). `deploy_ocm.sh` asume servicio estibado con nombre `docker compose up -d --build <SHA>` y health ports erróneos (§F-5); `ocm-api` y `streaming` no están en imágenes publicadas (imágenes HIVE: `octernity/...`). No hay evidencia/`artifacts/evidence/` ni `.prev_sha` (verificados ausentes).
- **Healthchecks:** compose usa `depends_on: service_healthy` + `config-guard` antes de arrancar — patrón de arranque con espera-de-salud OK para infra.
- **Conclusión:** Docker queda **reducido a infra + dev**, a menos que se active y repare el camino CD (requiere decisión ADR: native-first vs docker-first del *servicio*; infra siempre Docker es compatible con ambos).

---

## 13. Kafka Assessment

- Contrato real: `KAFKA_BOOTSTRAP_SERVERS` (free-form). Defaults: compose INTERNAL `kafka:9092`, EXTERNAL `localhost:9093`; `.env` real de instalación = `localhost:9093`; schemas capturados en `shared/kafka/schemas/` (BC-35).
- **Auto-creación de topics deshabilitada:** `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` (compose:535). Provision de topics idempotente vía `scripts/provision_kafka_topics.py` (SSOT de topics) — etapa de infra integrable al orquestador **tal cual** (IDEMPOTENT).
- Listeners/usos para trazas wire/binding ya documentados en auditorías previas (ADP<=-conformance); el foco aquí: **el orquestador debe invocar provision SOLO cuando el broker sea nuevo/vacío** y nunca crear topics en tratos de un broker ajeno a OCM sin consentimiento.
- Caveat §F-9: default Python `kafka:9092` (DNS Docker) como fallback en `schema.py`/`consumer.py`/`producer.py` → en bare-metal sin env var apuntaría a un host name inexistente. Recomendación: default `localhost:9092`+ doc de contraste, o exigir la var en `--check`.

---

## 14. Redis Assessment

- Contrato: `REDIS_HOST`, `REDIS_PORT`, `REDIS_DB`, ``REDIS_PASSWORD` (variables discretas, NO `REDIS_URL`). SSOT nombres: `ocm/config/env_vars.py`.
- Defaults: `production.yaml` host `redis` (Docker DNS); en host real sin env var falla — cubierto por validator fail-fast L5 (env requerida en PROD).
- `api:/ready` (apps/api health) pinea Redis (contrato operativo); `/health` de market-data :8001 es process-level (§F-7).
- Dev override expone `6379:6379` al host (para servicios nativos). En PROD, si Redis corre en Docker sin publish, los servicios nativos usan la IP del contenedor/red definida en env (contrato externo). Ok.
- **Orquestador:** stage infra debería verificar `redis-cli ping` (gate G6 ya lo hace) antes de arrancar servicios nativos.

---

## 15. Monitoring Assessment

- Componentes reales (compose + `deploy/monitoring/`): Prometheus `:9090`, Alertmanager `:9093` (instalación real `.env`), Pushgateway `:9091`, Grafana `:3000` (dashboards provisionadas desde `deploy/monitoring/grafana/`), Loki `:3100` + Promtail `:9080`.
- **Caveat F-5:** `deploy_ocm.sh` y ADR-0037 citan puertos **intercambiados** (prometheus:9091, alertmanager:9090, pushgateway:9093) vs compose (9090/9093/9091) → desalineación documental+script que debe corregirse.
- `.env` instalado dispara `ALERTMANAGER_HOST_PORT=9094` (para evitar colisión con `KAFKA_HOST_PORT=9093`) — mientras `.env.example` mantiene 9093/9094 invertidos (§F-10).
- **Pipeline de métricas:** `metrics.enabled=false` por defecto (dev) → `NoopPusher`; pushgateway sin datos en runtime dev. En PROD pushgateway pipeline real.
- **OTel tracing runtime existe** (`ocm/observability/tracing.py`): `init_tracing`, `trace_event`, propagation de `request_id` vía W3C baggage, exportador OTLP HTTP, fail-soft, desactivado por defecto. No hay collector/OTLP endpoint configurado (futuro, no bloqueante).
- **Veredicto:** monitoring está provisionado de fábrica (dashboards, alertas, datasources). El orquestador solo debe: (a) `--check` contra puertos reales, (b) opcional `enabled` por entorno, (c) en PROD, fijar target de pushgateway/OTLP coherente con host o remoto.

---

## 16. Configuration Assessment

- **Cadena Hydra:** `config/base.yaml` → `core` (server/exchange/broker) + `env/{development,production}.yaml` + `observability/{metrics,tracing}.yaml` → CLI overlay → env vars (`OCM_*` desde `ocm/config/env_vars.py`). Validate-only: `OCM_VALIDATE_ONLY=true uv run python -m app.cli.main` (BOOL_TRUE={true, yes, on}).
- **Config operacional de host:** `deploy/host.env(.example)` → `install_systemd.sh` render → `/etc/ocm/*.env`; units usan `EnvironmentFile`. `.env` raíz (dev) con `*_HOST_PORT`.
- **Riesgo de la propuesta `deploy/config/`:** unificarla podría duplicar `config/` (Hydra) o `host.env`. **Recomendación:** mantener `config/` (Hydra) como configuración de app, `deploy/host.env` como configuración operacional del SO, `.env` como dev local. No crear `deploy/config/`; documentar esta frontera **en vez de** reorganizar.
- **SafeOps:** nunca sobreescribir `.env`/`host.env` existentes desde un instalador (create-if-missing). `host.env` está tracked en el repo (¡contra policy "NUNCA commitear" §F-6) — limpiar y dejar solo `.example` + gitignore.

---

## 17. Startup Sequence

Secuencia de arranque observada (nativa):

```
deploy/scripts/install_systemd.sh render + daemon-reload
   │
[ infra ]  docker compose up -d  (kafka, redis, prom, pushgw, alert, grafana, loki, promtail)
   │        config-guard (valida .env) + depends_on: service_healthy
   │        scripts/check_production_gates.py (gate-ci → gates G1..G11)   [si /d]
   │
[ servicios nativos ]
   systemctl (re)start ocm-market-data.service
        ├─ iniciar FastAPI :8001 (/health, /ready)
        └─ FeedOrchestrator → conecta KAFKA_BOOTSTRAP_SERVERS → mercado
   systemctl (re)start ocm-streaming.service
        └─ Hydra: Ticker→OrderBook (Kappa), DP-> pushgateway (si metrics.enabled)
   systemctl (re)start ocm-api.service (opcional) / uv run ocm (pipeline CLI)
   │
[ verificación ]  deploy/scripts/health_check.sh  → L1 systemd · L2 HTTP/ready · L3 dependencias · L4 datos
```

Puntos fuertes: `config-guard` + `service_healthy` antes de arrancar; L5 fail-fast clara. Gaps: sin orquestador único que ordene `check → infra → units → health` (hoy esa lógica está parcialmente en G6/G7/README); `--check` del orquestador propuesto cubre exactamente este hueco.

---

## 18. Health & Readiness

- `market-data /health` (:8001) → process-level liveness (no toca deps); `market-data /ready` → readiness sin pines de Kafka/Redis (§F-7).
- `api /ready` (:8000) → pinea Redis. No hay endpoint global que reporte "infra lista" para el orquestador.
- `health_check.sh` (deploy/scripts/): chequeo L1=systemd, L2=http+ready, L3=dependencias infra, L4=frescor de datos (orderbook.raw) — la pieza más completa; consumida por G7.
- **Recomendación:** el orquestador debe usar `health_check.sh --check` como gate final (ya es gate G7) y exponer su resultado como variable de salida (p.ej. `MARKET_DATA_HEALTHY=HEALTHY`). Añadir opcionalmente pines típicos de Kafka/Redis en `/ready` de market-data si se pretende readiness semántico.

---

## 19. Market Data Runtime

- Microservicio real (`packages/market_data/main.py`): ingestion + Kappa bronze (Ticker→OrderBook→bronze) + `FeedOrchestrator`; ports/HTTP:8001; integración tests con Kafka `:9093` (README). ADR-0022 documenta lifecycle realtime feeds.
- `Dockerfile:40` referencia `market_data.orchestration.entrypoint` **inexistente** — compose sobreescribe CMD, pero el Dockerfile solo está "casi bien" (§F-12). No se usan Tests contract de TestClient.
- Se ejecuta nativo por systemd (no Docker) — coherente con A+B; el orquestador profile `streaming`/`microservices` existe pero el docker de market-data está comentado (override dev).
- `/health`/`/ready` como arriba; Freshness L4 viene de `orderbook.raw`.

---

## 20. Clean Architecture Assessment (deploy/ y la regla raíz/deploy)

- El árbol de aplicación es Clean/Hexagonal y lo sigue gobernado import-linter (49 contratos BC-NN, baseline F2.1). **`deploy/` no es parte de la aplicación**; es el boundary de operación. La regla propuesta (raíz = interfaz pública; deploy = implementación operacional) es **coherente** con la arquitectura limpia: la aplicación no conoce el mecanismo de instalación (contratos por env), y el deployment no conoce el dominio.
- **Punto a cuidar:** `deploy/` no debe convertirse en "otra capa de aplicación". Si `deploy/scripts/` empieza a importar módulos de `packages/*` o `ocm/*`, violaría separación (hoy no ocurre; `check_production_gates.py` vive en `scripts/` y NO importa código de app — invoca por subprocess).
- El orquestador `install.sh` (deploy/) debe ser código independiente (bash/python sin imports de dominio), como son `install_systemd.sh`, `health_check.sh`, `deploy_ocm.sh`.

---

## 21. Security Assessment

- **Principio:** jamás ejecutar instalación como root salvo pasos específicos (write `/etc/systemd/system`, `daemon-reload`). El orquestador debe detectar usuario e informar (`--user` para todo-user-less). Units `User=` unprivileged.
- **Secrets:** `EnvironmentFile=/etc/ocm/*.env` 600; prohibido secretos inline en `Environment=`; nunca `--cfg job` en logs; broker/exchange credentials en env vars `OCM_*` SSOT; gitleaks en pre-commit.
- **Repo:** `host.env` tracked actualmente (§F-6) — pendiente de limpiar (secretos de infraestructura textuales).
- **Systemd:** units 644 root:root en `/etc/systemd/system`; `--reinstall` con backup del unit previo; verificación `systemd-analyze verify`.
- **Docker:** no exponer puertos de infra fuera de lo necesario (dev override expone 6379/9091). No sudo implícito en `docker compose`.
- **CIFALL: gates fail-closed** (§G4 fail-closed §F-3); bandit/shellcheck/actionlint/codeql/trivy/gitleaks presentes en CI.
- **Orquestador:** input validation en flags; nunca `eval`/cadena de usuario; outputs estructurados sin secrets.

---

## 22. Idempotency Assessment

| Componente / operación | Clasificación | Nota |
|---|---|---|
| `uv sync` | **IDEMPOTENT** | determinista según lock |
| `docker compose up -d` (infra) | **PARTIALLY** | no recrea si ya existe; pero `up` puede regenerar contenedores si config_guards cambian |
| `scripts/provision_kafka_topics.py` | **IDEMPOTENT** | explícito (SSOT topics, create-if-not-exists) |
| `install_systemd.sh` render → install → daemon-reload | **PARTIALLY** | overwrite+reload idempotentes; copia pierde ediciones manuales del unit renderizado (templates = SSOT) |
| copia `.env`/`host.env` | **PARTIALLY / casi-NO** | NO debe sobreescribir existentes; create-if-missing = IDEMPOTENT |
| `systemctl enable/start` | **NOT IDEMPOTENT** si se re-ejecutan ciegamente | debe ser `is-enabled`/`is-active` guard antes de action |
| `deploy_ocm.sh` / CD | **PARTIALLY** | backups + `.prev_sha` para rollback (ausentes → §F-2) |
| `health_check.sh` | **IDEMPOTENT** | read-only |
| creación de dirs (`/etc/ocm`, systemd) | **IDEMPOTENT** | `mkdir -p`, `install -d` |

Recomendación: `deploy/install.sh` implementa **modo por defecto = `--check`** (read-only, status), y solo `--apply` muta. Cada etapa retorna PASS/FAIL/SKIP con razón (estilo de `check_production_gates.py`).

---

## 23. DEV vs PROD

- **DEV:** Compose (infra + profiles opt-in), `run.sh`/`uv run`, `metrics.enabled=false` (NoopPusher), override expone redis/kafka al host, bloques docker de servicios comentados. Coherente con "pipeline corre en la maquina local".
- **PROD (state actual):** unidades systemd nativas + infra Docker misma máquina (OrangeHouse, single-host, sin cloud/K8s/Terraform — ADR-0037) o infra externa por contrato (remoto). `production.yaml`: `dry_run:false`, requerimientos de env p.indispensables.
- **Transición:** no hay flujo automatizado dev→prod salvo gates de producción (`check_production_gates.py` gate-dev/gate-ci). B-59 (reinicio systemd) NO verificado → gap para confiar estado inicial/final.
- **Compatibilidad de la propuesta:** DEV=Docker, PROD=infra externa es EXACTAMENTE el A+B; no hay conflicto inherente. El conflicto real está en Docker-CM del servicio (ADR-0037) vs nativo (runtime) — resolver el modelo canónico (§3/§12/§30).

---

## 24. CI/CD

- **Gates CIs:** `.github/workflows/ocm-ci.yml` (tests + config-validation, `OCM_VALIDATE_ONLY=true uv run python -m app.cli.main`), `ocm-cd.yml` **placeholder deshabilitado (workflow_dispatch only)**, además de actionlint/codeql/docker-lint/gitleaks/shellcheck/trivy/yamllint/nightly-compliance. Pre-commit: ruff, gitleaks, readme-size-guard, import-linter, mypy-shared, ssot-enums, bandit, yamllint (+ vulture, pytest en pre-push).
- **`architecture_linter`** (raíces packages/shared/apps/ocm; exit 0/1/2) complementa import-linter en CI.
- **Gates de producción** (`scripts/check_production_gates.py`): G1 config-validate, G3 import-linter?, G4 systemd units (--verify-only), G5 runtime market-data+streaming, G6 kafka+redis, G7 health_check.sh, G8 trading stub, G10 audit-consistencia, G11 (gate-ci)… — **ya integran deployment scripts** → la infraestructura para un `install.sh --check` gobernado está montada.
- **Propuesta en CI/CD:** CI puede llamar `./install.sh --check` en un runner con `~/.venv` (verifica determinismo instalación); `install.sh --apply` NO en CI (es operación de host).
- **Artefactos CD:** `artifacts/f26c/` sin evidencias de creación verificable (¿AWeại), `.prev_sha`/evidence ausentes → §F-2.

---

## 25. Documentation Drift

| # | Fuente | Afirmación | Estado real | Clasificación |
|---|---|---|---|---|
| D1 | `ADR-0037:11-21` | `deploy_ocm.sh` **inexistente**; `scripts/` = 8 scripts (sin `check_production_gates.py`, sin `deploy_ocm.sh`) | deploy_ocm.sh existe (created 08-25); scripts/ ahora 12 (+`check_production_gates.py`, `migrate_audit_filenames.py`, `reset_kafka_test_topic.sh`) | **OBSOLETO** (contexto con fecha auditoría F-PLC-10) |
| D2 | `ADR-0037` (§ health ports) + `deploy_ocm.sh:11/49` | prometheus:9091, alertmanager:9090, pushgateway:9093 | compose: prometheus 9090, alertmanager 9093, pushgateway 9091 | **CONTRADICTORY** (§F-5) |
| D3 | `ADR-0022:323` | "Sin unit systemd activo" | units systemd activas (market-data-runtime 08-20; rendered) | **OBSOLETO** |
| D4 | `FORENSIC_COMPLIANCE_2026-08-19` | "deploy_ocm.sh no existe", "systemd sin units" | superado | **OBSOLETO** |
| D5 | `.env.example:32-40` | `ALERTMANAGER_HOST_PORT=9093`, `KAFKA_HOST_PORT=9094` (nota "colisión") | `.env` real instalado: `ALERTMANAGER_HOST_PORT=9094`, `KAFKA_HOST_PORT=9093` | **CONTRADICTORY** (§F-10) |
| D6 | `docs/architecture/GOVERNANCE.md` | (sin menciones a deployment/systemd/install) | — | **GAP de gobernanza** (sin reglas de deployment) |
| D7 | `check_production_gates.py:251` | invoca `bash scripts/install_systemd.sh --verify-only` | el script está en `deploy/scripts/` | **CODE bug** (§F-3) |
| D8 | `docs/plans/tracking.yaml` B-59 | pw `systemd_reinicia_correctamente` NO_VERIFICADO | — | PENDIENTE |
| D9 | `AGENTS.md`/comandos | bandit target incluye `infrastructure` (paquete vacío) | `infrastructure/` = solo `__init__.py` | LEGACY (§F-16) |

---

## 26. Scripts Assessment

**`scripts/` (raíz) — 12 archivos → gobernanza/CI + utilidades:**

| Script | Clasificación | Consumidor | Nota |
|---|---|---|---|
| `app_layer_guard.py` | ORCHESTRATOR/GUARD (AST) | CI app-guard | apropiado |
| `backtest_app_guard.py` | UTILITY | CI backtest guard | interno |
| `domain_subprocess_guard.py` | GUARD (AST R11) | CI | apropiado |
| `check_production_gates.py` | ORCHESTRATOR (G1..G11, modos gate-ci/gate-dev) | CI + operador | consume deploy/scripts (G4/G6/G7). **Buge de path §F-3** |
| `check_ssot_enums.py` | UTILITY (literales SSOT) | pre-commit + CI | apropiado |
| `audit_validator.py` | ORCHESTRATOR (M1..M25) | CI governance | apropiado |
| `engineering_health_check.py` | ORCHESTRATOR (F2.0 Plan↔tracking↔ADR↔contratos↔CI) | CI gate | apropiado |
| `compliance_report.py` | UTILITY (reporting) | manual/CI | — |
| `metrics_report.py` | UTILITY (reporting) | manual/CI | — |
| `migrate_audit_filenames.py` | **LEGACY** (one-off migration) | — | candidato a cleanup |
| `provision_kafka_topics.py` | **DEPLOYMENT OPERATION** (infra) | operador | candidato a mover a `deploy/scripts/` o a ser llamado por el orquestador |
| `reset_kafka_test_topic.sh` | UTILITY (test/dev) | dev | — |

**`deploy/scripts/` — 3 archivos → deployment ops:**

| Script | Clasificación | Estado |
|---|---|---|
| `deploy_ocm.sh` | DEPLOYMENT OPERATION + ORCHESTRATOR CD | CODE_ONLY (ADR-0037 Propuesto); puertos erróneos §F-5; evidencia/rollback ausentes §F-2 |
| `install_systemd.sh` | DEPLOYMENT OPERATION (render→verify→install→daemon-reload; `--user`) | IMPLEMENTED; usado por G4 |
| `health_check.sh` | DEPLOYMENT OPERATION (L1-L4) | IMPLEMENTED; gate G7 |

**Veredicto:** no hay smoke de arquitectura en `scripts/`; mezcla es normal (guard/utility/operation) con 2 notables: `provision_kafka_topics.py` es operación de deployment (reubicar opcional), `migrate_audit_filenames.py` es LEGACY. La propuesta de "deploy/scripts/" ya existe; **no duplicarla**.

---

## 27. Findings

| ID | Sev | Class | Área | Finding | Evidencia |
|---|---|---|---|---|---|
| F-1 | HIGH | CONTRADICTORY | CD/Runtime | Split-brain: CD documentado = OCM en Docker (`deploy_ocm.sh` + compose + imagen); runtime canónico operativo = nativo/systemd. Dos modelos concurrentes sin resolución formal | `docs/architecture/decisions/ADR-0037-…md` (Propuesto) vs `docker-compose.override.yml` (comentado "pipeline corre en la maquina local") vs units systemd renderizadas |
| F-2 | HIGH | CODE_ONLY / PARTIAL | CD | `deploy_ocm.sh` no es ejercitable: `docker compose up -d --build <SHA>` con selector de servicio (no existe servicio con ese nombre); ausencia de `.prev_sha` y de `artifacts/evidence/` (sin evidencia de despliegue verificado); rootea desde directorio equivocado (`deploy/` vs workspace) | `deploy/scripts/deploy_ocm.sh` (—actual) + `artifacts/` (solo f26c/ sin evidence) + grid de health ports §F-5 |
| F-3 | MEDIUM | CODE | Gates/Systemd | `check_production_gates.py:251` invoca `bash scripts/install_systemd.sh --verify-only` — el script vive en `deploy/scripts/`. `_deployment_units_ok` degrada a `systemd-analyze verify` del rendered existente (fail-closed), enmascarando el error de path | `ls scripts/install_systemd.sh` → "No existe"; `ls deploy/scripts/install_systemd.sh` → existe; `check_production_gates.py:246-257` |
| F-4 | MEDIUM | CODE | Systemd | Drift rendered vs templates: units renderizadas contienen línea extra `EnvironmentFile .env` y paths hardcodeeados no presentes en los templates; re-render pierde ediciones | diff `deploy/systemd/templates/*.service.tpl` vs `deploy/systemd/rendered/*.service` (auditoría market-data-runtime 08-20; install_systemd.sh render) |
| F-5 | MEDIUM | CONTRADICTORY | Monitoring/CD | Puertos health desalineados: compose=prometheus 9090, alertmanager 9093, pushgateway 9091; `deploy_ocm.sh` y ADR-0037 citan prometheus:9091, alertmanager:9090, pushgateway:9093 | `docker-compose.yml` (9090/9093/9091) vs `deploy_ocm.sh` header/constants vs `ADR-0037` §Wait-for-health |
| F-6 | MEDIUM | CONTRADICTORY | Security | `deploy/host.env` **trackeado** en git pese a policy "NUNCA commitear" (`.example` + gitignore es lo normativo) | `git ls-files deploy/host.env` (verificado previo) + `deploy/host.env.example` |
| F-7 | MEDIUM | DOCUMENTED_ONLY | Health | `/health`/:8001 process-level; `/ready`/:8001 no pinea Kafka/Redis; único pine de dependencia real = `api /ready` (:8000) a Redis | `packages/market_data/main.py:465-519`, `apps/api/routers/health.py` |
| F-8 | MEDIUM | DOCUMENTED_ONLY | Monitoring | `metrics.enabled=false` por defecto → pushgateway sin métricas en runtime dev (NoopPusher); pipeline "monitoring" solo documentada | `ocm/observability/metrics.py` NoopPusher/streaming; `config/observability/metrics.yaml` |
| F-9 | MEDIUM | CODE | Infra/Contract | Defaults Python `kafka:9092` (DNS Docker) en `schema.py`/`consumer.py`/`producer.py` → foot-gun en bare-metal sin env var; `production.yaml` Redis host `redis` requerido en runtime nativo | `ocm/config/schema.py`, `consumer.py`, `producer.py`, `config/env/production.yaml` |
| F-10 | MEDIUM | CONTRADICTORY | Config | `.env.example` invierte puertos reales: example=ALERTMANAGER 9093 / KAFKA 9094; instalación real=ALERTMANAGER 9094 / KAFKA 9093 | `deploy/.env.example:32-40` vs `.env` instalado (verificado) |
| F-11 | LOW | CODE/PARTIAL | Runtime futuro | `ocm-trading.service` en la propuesta no es implementable: no existe `trading.main`; ADR-0024 lo declara NIVEL 2 "Propuesto". Units trading prematura | `packages/trading` (sin main); `docs/architecture/decisions/ADR-0024-…md` |
| F-12 | LOW | CODE | Docker | `Dockerfile:40` referencia `market_data.orchestration.entrypoint` inexistente; compose sobreescribe CMD — Dockerfile "casi bien" pero entrypoint inválido | `Dockerfile:40`, `docker-compose*` overrides |
| F-13 | LOW | INFO | Boundary | Directorios propuestos `deploy/runtime`, `deploy/infrastructure`, `deploy/config`, `deploy/health` redundan con locaciones existentes (uv+pyproject, compose raíz, config/ Hydra, deploy/scripts) | estructura actual (§2) |
| F-14 | INFO | IMPLEMENTED | Observabilidad | Tracing OTel real (fail-soft, disabled default, request-id en logs) sin collector/OTLP configurado; monitoring/OTLP futuro opcional | `ocm/observability/tracing.py` |
| F-15 | INFO | PARTIAL | Idempotency | Idempotencia mixta ✓provision/uv/health/check; ✗env copy (debe ser create-if-missing); ✗systemctl start/enable sin guards; CD sin evidence | §22 |
| F-16 | INFO | LEGACY | Estructura | `infrastructure/` paquete vacío (único `__init__.py`) trackeado y citado en bandit/AGENTS; `.bak`/`.swp` presentes solo en disco (no trackeados) | `git ls-files infrastructure/__init__.py`; `.gitignore:19,25,54` |

---

## 28. Architecture Gaps

1. **Gobernanza de deployment ausente:** GOVERNANCE.md sin reglas sobre `deploy/`, systemd, orquestador de instalación, root interface. Las únicas autoridades son ADR-0022/0024/0037 (los dos últimos *Propuestos*).
2. **Modelo canónico de servicio sin decidir:** nativo (systemd) vs Docker-CM (compose) concurrentes sin resolución → F-1.
3. **Readiness semántico incompleto:** los servicios nativos no reportan dependencias (F-7).
4. **Proveimiento de infra no orquestado:** provision de topics idempotente existe pero es manual; no-boundary entre "infra opcional/Otro host".
5. **Determinismo toolchain:** sin fijar `.python-version`/`tool.uv python` (libertad `3.11..3.14`).
6. **Enforcement formal de boundary deploy/:** de facto OK; ninguna regla linteable.
7. **Idempotencia CD/instalación formalizada** y evidencia de despliegue/rollback (B-59, artifacts).

---

## 29. Existing Good Patterns

- `run.sh` = facade raíz que delega + SSOT declarado con `[project.scripts]` (precedente directo de install.sh).
- `[project.scripts]` como registro único de entrypoints (`ocm`, `ocm-api`, `live`, `paper`, `streaming`).
- `ocm/config/validator.py` (`EnvironmentValidator`, L5 fail-fast) + `dry_run` default true.
- `config-guard` + `depends_on: service_healthy` en compose (espera de salud de infra).
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` + provision idempotente de topics desde SSOT.
- `install_systemd.sh` con `--verify-only` / `--user` y verificación `systemd-analyze verify` (gate G4).
- `health_check.sh` L1-L4 (systemd→http/ready→deps→datos) y su integración como gate G7.
- `check_production_gates.py` con gates G1..G11 y semántica fail-closed — ya consume los deployment scripts → el `install.sh --check` puede reutilizar su DSL de gates.
- CI 49 contratos BC-NN + `architecture_linter` + engineering-health + pre-commit (gitleaks, readme-guard, bandit, yamllint).
- Configuración de Hydra por capas + validate-only (`OCM_VALIDATE_ONLY=true uv run python -m app.cli.main`).
- `.gitignore` correcto para `rendered/`, `*.env` de infra, backups (`*.bak`/`*.swp`).
- OTel tracing runtime (fail-soft) e `request_id` propagado.

---

## 30. Recommended Target Architecture

**Mantener A+B y consolidar** (no re-plataformar):

```
root interface (interfaz pública, facade):
  run.sh        → ejecuta (ocm|live|paper|…)
  install.sh    → NUEVO facade → exec ./deploy/install.sh "$@"

deploy/ (boundary operacional — declarado formalmente):
  install.sh            → NUEVO orquestador: --check | --dry-run | --apply [stage…]
                            stages: runtime→infra→config→systemd→monitoring→health
  scripts/              → deploy_ocm.sh · install_systemd.sh · health_check.sh   (KEEP)
  systemd/templates/    → SSOT de units (VCS); rendered/ = artefacto gitignored
  monitoring/           → prom/alertmanager/grafana/loki/promtail (ya provisionado)
  host.env(.example)    → render → /etc/ocm/*.env (create-if-missing)

fuera de deploy/ (decidido conscientemente):
  docker-compose.yml    → raíz (infra + profiles dev); NO mover a deploy/infrastructure
  config/               → Hydra (config de app) ;  NO deploy/config
  pyproject.toml + uv   → runtime/.venv (NO deploy/runtime)
  scripts/              → guards de gobernanza/CI (KEEP; provision_kafka_topics.py opcional→deploy)
```

**Decisiones formales pendientes (ADR):**
- Modelo canónico del *servicio*: **native-first (systemd)** como operativo; deprecar o reparar camino Docker-CM (deploy_ocm.sh se convierte en "CD del host nativo" o se marca Legacy). ADR-0037 pasa de Propuesto a Aprobado/deprecado con corrección de puertos y evidence de despliegue.
- Root interface + boundary deploy/ → regla en GOVERNANCE (y opcional linter).
- Determinismo toolchain (fijar versión Python para uVS).
- `ocm-trading.service` solo cuando exista `trading.main` (ADR-0024 NIVEL 1).

**Correcciones mínimas inmediatas:** F-3 (path gate), F-5 (puertos), F-2 (selector/evidence CD), F-10 (.env.example), F-6 (host.env untracked), F-7 (readiness o documento), F-12 (Dockerfile entrypoint), y drift validator systemd (F-4).

**Diagrama 1 — Repository boundary:**

```
                    ┌────────────── Repo OCM ──────────────┐
                    │                                      │
   INTERFAZ PÚBLICA│  run.sh · install.sh (facade thin)    │
                    └──────────────────────────────────────┘
                                      │ exec/delegate
                    ┌──────────────────────────────────────┐
   BOUNDARY OP      │  deploy/  (install.sh, scripts/,      │
   DEployment       │  systemd/templates, monitoring,       │
   (NO importa app) │  host.env example)                    │
                    └──────────────────────────────────────┘
                                      │ contrato env: KAFKA_BOOTSTRAP_SERVERS,
                                      │ REDIS_*, *_HOST_PORT, OCM_*
                    ┌──────────────────────────────────────┐
   INFRA externa    │  Kafka · Redis · Prom · Pushgw ·      │
   (Docker/remoto)  │  Alert · Grafana · Loki               │
                    └──────────────────────────────────────┘
        Aplicación (ocm/, packages/, apps/, shared/) NO referencia deploy/ en runtime.
```

**Diagrama 2 — Deployment flow:**

```
./install.sh ──(facade)──> deploy/install.sh [--check|--dry-run|--apply]
                                 │
                                 ├─[runtime]------- uv sync (venv) ─────────── IDEMPOTENT
                                 ├─[infrastructure]- docker compose up -d      PARTIALLY
                                 │                   provision_kafka_topics.py IDEMPOTENT
                                 ├─[configuration]- render host.env → /etc/ocm/*.env (create-if-missing)
                                 │                   .env.example → .env (si NO existe)
                                 ├─[systemd]------- install_systemd.sh render→verify→install→daemon-reload
                                 │                   [enable|start con is-enabled/is-active guards]
                                 ├─[monitoring]---- compose observabilidad (ya provisionado)
                                 └─[health]-------- health_check.sh L1-L4 → GATE final
                                                          │
                                                          └──► OCM READY (status: PASS/FAIL/SKIP)
```

**Diagrama 3 — Systemd flow:**

```
deploy/systemd/templates/*.service.tpl          (SSOT, VCS, placeholders)
        │  install_systemd.sh --apply | install.sh --apply --systemd
        ▼
deploy/systemd/rendered/*.service               (artefacto generado; gitignored)
        │  install -m 644 → /etc/systemd/system/  [+ backup previo]
        ▼
systemd unit search path (/etc/systemd/system)  (p.ej. ocm-market-data.service, ocm-streaming.service)
        │  systemctl daemon-reload  +  enable + start (guards is-active)
        ▼
systemd → User=orangemusic → ExecStart=.venv/bin/…  →  EnvironmentFile=/etc/ocm/*.env
        │
        ▼  health_check.sh L1 (systemctl is-active) · L2 /health /ready · L3 deps · L4 datos
```

---

## 31. Migration Roadmap

**P0 (correcciones inmediatas, sin decisión de arquitectura):**
- F-3 `check_production_gates.py:251` → `deploy/scripts/install_systemd.sh`.
- F-5 puertos health en `deploy_ocm.sh` y ADR-0037 → align compose (9090/9093/9091).
- F-10 `.env.example` → align instalación real (9094/9093).
- F-6 `deploy/host.env` → dejar `.example` + gitignore (untrack).
- F-12 `Dockerfile:40` → entrypoint inexistente (o eliminar / composición explícita).
- F-2 `deploy_ocm.sh` → selector de servicio + evidencia de despliegue (`artifacts/evidence/`) o deprecar.

**P1 (normalización — la "fase 1" de la propuesta):**
- ADR: modelo canónico (native-first), boundary `deploy/`, root interface (`install.sh`/`run.sh`), determinismo toolchain (pin `.python-version`).
- `deploy/install.sh` orquestador mínimo (`--check`/`--dry-run`/`--apply`) **reutilizando** `install_systemd.sh`, `health_check.sh`, `deploy_ocm.sh` (si se mantiene CD), `provision_kafka_topics.py`, y DSL de gates de `check_production_gates.py`.
- `install.sh` raíz facade (`exec ./deploy/install.sh "$@"`).
- `--verify-only` del sistema con drift check (re-render vs rendered, comparación canónica) — hereda lógica de G4.
- Idempotencia formalizada: env create-if-missing; systemctl guards; etapa FAIL/SKIP/PASS reportable.
- `ocm-trading.service` NO incluida hasta `trading.main` exista.

**P2 (robustez):**
- Readiness semántico de market-data (`/ready` con deps opcionales) o documentación explícita de project-level.
- `metrics.enabled` por entorno (dev NoopPusher / prod pushgateway) y target OTLP si hay collector.
- `provision_kafka_topics.py` → a `deploy/scripts/` (opcional) y llamado por el orquestador.

**P3 (higiene / opcional):**
- Eliminar `infrastructure/` vacío (actualizar bandit/AGENTS).
- Cleanup `migrate_audit_filenames.py` (LEGACY) y backups en disco (`*.bak`/`.swp`) si dejan de usarse.
- Evaluar `deploy/infrastructure/` (mover compose) **solo** con ADR y trazado completo de docs/CI/README.
- Cerrar B-59 con prueba real de reinicio systemd (el orquestador `--check` facilita repetibilidad).

---

### Anexo — Cumplimiento del encargo

- Veredicto global: **PARTIALLY viable**. Estructuralmente sí; operativamente exige correcciones P0 y las decisiones ADR de P1. NO se modificó ningún código, test, CI, ADR ni tracking (protocolo READ-ONLY); NO se implementó nada.
- Estado git: baseline `feat/b56-ci-stage-ordering` @ `b8b13240` con 8 archivos de trabajo ya modificados + `?? docs/audits/OCM_AUDIT_FINDINGS_2026-09-19_…` (informe previo). Este documento es el ÚNICO archivo añadido por la auditoría (`?? docs/audit/deployment-runtime-architecture-audit.md`). Sin commit/push/merge.
- Evidencia citada verificada en HEAD `b8b13240` salvo indicación (NOT VERIFIED/B-59). Números de línea aproximados/verificados donde se indican.