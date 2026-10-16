# OCM — Auditoría READ-ONLY: Frontera Repo ↔ Host (Deployment Boundary / Portabilidad)

- **Fecha:** 2026-09-19
- **Modo:** READ-ONLY (FASE C1, auditoría + preparación controlada). Nada de código/deploy/systemd/CI config modificado.
- **Estado previo heredado:** `git rm --cached deploy/host.env` stagado en la misión C1 anterior (aprobada), SIN commit.
- **Referencias:** `docs/audit/deployment-runtime-implementation-plan.md` (§4 D5), `docs/audit/deployment-runtime-architecture-adversarial-audit.md` (F-6, §70), `docs/audit/deployment-runtime-architecture-audit.md`.

## 1. ESTADO ACTUAL (evidencia)

- `git status --short`: `D deploy/host.env` (staged, de la misión C1 previa) + 8 ` M ` pre-existentes + `?? docs/audit/` + `?? docs/audits/`. Branch `feat/b56-ci-stage-ordering` @ `b8b13240`.
- `deploy/host.env` y `deploy/host.env.example`: **byte-idénticos** (`cmp` OK, `sha256sum` idéntico `8d3a7192…`), blob `5787abef…` ambos en índice. Archivo físico presente (mtime 7-sep, 1613 B).
- `git check-ignore deploy/host.env` → NOT IGNORED (por estar trackeado); `git check-ignore -v --no-index deploy/host.env` → **`.gitignore:74:deploy/host.env`** y `.gitignore:75` cubre `deploy/systemd/rendered/`.
- Consumidores (rg global): templates systemd (`:11` EnvironmentFile), `install_systemd.sh` (`:10,16,32,36`), `.gitignore:74`, `host.env.example` (cabecera). **NO hay consumidores** en `.github/`, `docker-compose*.yml`, código, ni docs operativas (solo referencias documentales).

## 2. MATRIZ DE VARIABLES (clasificación)

| Variable | Ubicación | Consumidor | Cómo se obtiene | Versionado hoy | ¿Debe versionarse? | Clase |
|---|---|---|---|---|---|---|
| `OCM_HOST_USER` | host.env | templates (`User=`) | manual host | sí (en host.env) | NO | **B** (solo materialización template) |
| `OCM_REPO_ROOT` | host.env | templates (`WorkingDirectory/ExecStart/ReadWritePaths/EnvironmentFile/Documentation`) + health_check fallback | manual host | sí | NO | **B** (efecto render) |
| `OCM_ENV` | host.env / .env | `RunConfig.from_env()` (`ocm/config/env_vars.py` SSOT) | EnvironmentFile + dotenv | sí (host.env) / no (.env) | valor: NO; nombre: sí | **D** (nombre=A) |
| `MARKET_DATA_HOST/PORT` | host.env | `main.py:85-86` vía `env_vars.py:80-81` | EnvironmentFile | sí | valor NO | **D** (nombre=A) |
| `INGESTION_INTERVAL_S` | host.env | `main.py:87` vía `env_vars.py:83` | EnvironmentFile | sí | valor NO | **D** (nombre=A) |
| `KAFKA_ENABLED` / `KAFKA_BOOTSTRAP_SERVERS` | host.env / .env | `config/base.yaml:45-46`, `env/production.yaml` (hydra `${oc.env:}`) | EnvironmentFile/dotenv | sí | valor NO | **D** (nombre=A) |
| `REDIS_ENABLED/HOST/PORT` | host.env | `base.yaml:37-39`, `cursor_store.py:460-461`, `apps/api/settings.py:65-72` | EnvironmentFile/settings | sí | valor NO | **D** (nombre=A) |
| Keys BYBIT/KUCOIN, `REDIS_PASSWORD`, JWT | `.env` | `credentials.py`, `cursor_store.py:463`, api | manual | NO | NO | **C** |
| `host.env.example` | repo | onboarding/referencia | VCS | sí | sí | **A** |
| templates systemd (placeholders) | repo | installer→systemd | VCS | sí | sí | **A** |

**Sin secretos en host.env:** verificado por inspección (26 líneas: topología/runtime únicamente). Declaración en cabecera propia "sin secretos".

## 3. FRONTERA ACTUAL

- **Git:** templates portables (**placeholders `${OCM_HOST_USER}`, `${OCM_REPO_ROOT}`** — NO hardcodean `/home/orangemusic`), `host.env.example`, `install_systemd.sh` (renderer+installer), contracts `env_vars.py`, Hydra con defaults, `.gitignore` (cubre `.env`, `host.env`, `rendered/`). **Excepción/violación:** `deploy/host.env` trackeado = estado de máquina en Git (F-6).
- **Host:** `host.env` (valores), `.env` (secretos), `rendered/` (gitignored), `/etc/systemd/system/ocm-*.service` (materializados), `.venv`.
- **Flujo instalación:** `install_systemd.sh source host.env` → `envsubst` (lista explícita generada de `^[A-Z_]+=` de host.env) → `rendered/` → `systemd-analyze verify` → (apply) `install -m 644` → `daemon-reload`. Units: `EnvironmentFile=${OCM_REPO_ROOT}/deploy/host.env` (+ `.env` solo en rendered/installed — **DRIFT, fuera de C1**).

## 4. FRONTERA DESEADA

| Capa | Debe vivir en |
|---|---|
| Definición reproducible (templates, example, installer, contracts, .gitignore, Hydra) | **Git** |
| Valores de topología/runtime por host (`host.env`) | **Host** (untracked, esquema/contract versionado) |
| Secretos (`.env`) | **Host** (nunca) |
| Artefactos derivados (`rendered/`, `/etc/systemd/system`, `.venv`) | **Host** (regenerables desde Git + config externa) |

Mecanismo objetivo: **template versionado + config host externa + renderer = unit reproducible**. Es **el modelo actual** en templates (portables). No hay que inventar `/etc/ocm`, `REDIS_URL`, ni directorios nuevos: la convención establecida del repo es `EnvironmentFile → ${OCM_REPO_ROOT}/deploy/host.env` y `${OCM_REPO_ROOT}/.env`. Las menciones a `/etc/ocm/*.env` en docs/audit son **PROPUESTA**, no hecho.

## 5. Clone limpio → host nuevo (respuestas)

1. **Git** conoce: templates portables, `host.env.example`, installer, contracts (`env_vars.py`), Hydra, `.gitignore`.
2. **Host** aporta: valores (host.env), secretos (.env), `.venv`, infra Docker.
3. **Config de host**: `deploy/host.env` (untracked, misma ruta repo) — convención existente.
4. **systemd** la recibe por `EnvironmentFile` (host.env y .env) — convención existente.
5. **Instalación**: `install_systemd.sh` (ya existe) re-renderiza en el host con sus propios valores.
6. **Template versionado**: `${OCM_REPO_ROOT}/deploy/systemd/templates/*.template`.
7. **Materialización**: durante instalación (rendered/ + `/etc/systemd/system`).
8. **Anti-hardcode**: los templates ya usan placeholders; el valor concreto vive en host.env untracked del host. `health_check.sh` autoderiva fallback.
9. **Reproducibilidad**: `clone → cp example → editar → install_systemd.sh` en cualquier Debian/Linux.
10. **Aislamiento**: `.gitignore:74-75` garantiza que host.env/rendered no ensucien el árbol de un clone; un clone nuevo solo trae example + templates.

## 6. deploy/host.env — diagnóstico

1. Config específica de host: **SÍ** (OCM_HOST_USER, OCM_REPO_ROOT) + runtime config (MARKET_DATA_*, KAFKA_*, REDIS_*).
2. Secretos: **NO**.
3. Parte de deployment (envsubst) **y** parte de config de app (EnvironmentFile → env vars); los **nombres** de variables son contratos versionados.
4. Consumido directamente por systemd: **SÍ** (EnvironmentFile).
5. Consumido por scripts: install_systemd.sh. No por docker-compose, no por CI/CD, no por health_check (solo `OCM_REPO_ROOT` opcional).
6. Alternativa establecida: `.env` raíz + `env_vars.py` + Hydra `${oc.env:}` (no es reemplazo sino complemento; host.env es input del renderer y env runtime).
7. **Decisión: (b) dejar de estar versionado.** `.example` permanece (a), valores host quedan externos (d).
8. Cambio mínimo: **`git rm --cached deploy/host.env`** (ya preparado). No requiere cambio de `.gitignore` (línea 74 ya lo cubre). El file físico se mantiene; el instalador lo exige localmente con mensaje claro (`cp host.env.example host.env`).

## 7. Hallazgos (clasificación)

| Hallazgo | Clase | Estado |
|---|---|---|
| `deploy/host.env` trackeado pese a `.gitignore:74` (F-6) | **CAMBIO DE GOBIERNO** | C1 preparado (untrack); commit pendiente de autorización |
| Línea `EnvironmentFile=…/.env` solo en rendered/instalado | **DRIFT** | Fuera de C1 → siguiente fase D1/drift-check primero |
| Templates portables con placeholders | **SIN ACCIÓN** (correcto) | — |
| rendered/instalado con paths absolutos materializados | **EXPERIMENTAL/NO PROBLEMÁTICO** (derivado regenerable) | — |
| Puertos 9091-9094; métricas `enabled=false`; creds kucoin; B-59 | independiente | No tocar en C1 (matriz de puertos separada) |

## 8. RIESTORIES DE PORTABILIDAD

- **R1:** un clone limpio no trae `host.env` → `install_systemd.sh` aborta con mensaje claro (no auto-genera). Gap de bootstrap aceptado; NO se cambia el instalador en C1.
- **R2:** re-render sin fix de drift eliminaría la línea `.env` (secretos). Separado, D1 primero.
- **R3:** valores duplicados entre `host.env` y `.env` (OCM_ENV, KAFKA_BOOTSTRAP_SERVERS) → 2 fuentes de valores. No resolver ahora.
- **R4:** un `git add -A` futuro no puede re-trakear host.env (ignorado). El blob viejo queda en historia de Git (no es secreto; topología pública). Aceptable.
- **R5:** `install` a `/etc/systemd/system` requiere sudo; `--user` es alternativa documentada.

## 9. CAMBIO MÍNIMO PARA C1 (ya preparado)

```
git rm --cached deploy/host.env          # staged, SIN commit
# .gitignore:74 ya cubre el archivo      # sin cambios
# deploy/host.env físico se mantiene     # sin cambios
```

## 10. NO HACER EN C1

- commit/push/merge; arreglar EnvironmentFile (DRIFT); puertos; Kafka/Redis; health_check; gates; install.sh nuevo; CD; Compose; habilitar Market Data; cerrar B-59; ADRs; reorganizar deploy/; inventar `/etc/ocm`/`REDIS_URL`.

## 11. RECOMENDACIÓN SIGUIENTE FASE

1. Commit de C1 (requiere autorización): `git rm --cached deploy/host.env` + mensaje `chore(deploy): deja deploy/host.env fuera de VCS (config de host local, F-6)`.
2. Después: D1 drift-check + fix templates `.env` + imagen del instalador; matriz de puertos independiente; B-59 con prueba reproducible.