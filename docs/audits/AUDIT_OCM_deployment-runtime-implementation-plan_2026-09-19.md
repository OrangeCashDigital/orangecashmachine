# Deployment & Market Data Implementation Plan

> **Tipo:** Plan de implementación read-only — validación final del plan previo + resolución de la contradicción crítica `EnvironmentFile` + comité de commits definitivo.
> **Base:** audit adversarial `docs/audit/deployment-runtime-architecture-adversarial-audit.md` (2026-09-19) + plan previo `deployment-runtime-implementation-plan.md` + **verificación read-only adicional de esta misión** (drift, host.env, ADRs, orquestador, health, puertos).
> **Regla absoluta:** ESTE TASK SIGUE SIENDO READ-ONLY. Único artefacto modificable: este documento. NO código, NO deploy/, NO systemd, NO Docker, NO Compose, NO CI/CD, NO config, NO ADRs, NO tracking, NO commits, NO push/merge, NO reset/restore/checkout/clean/rebase, NO iniciar/detener/reiniciar servicios.
> **Contradicción resuelta:** la afirmación previa *"drift template↔SO corregido"* es **FALSA**. Ver §3: el drift estaba y sigue vivo; lo que se observa (`rendered == installed`, ambos con línea `.env`) materializa una edición **manual post-render**, no una corrección del template. Un re-render desde el template SSOT eliminaría la línea `.env` y rompería secrets.
> **Respuesta al criterio final:** el orden de implementación validado es `ADR-A + ADR-B (gobernanza, pré-requisito formal) → P0 (5 fixes aislados, sin ADR) → P1 (completar deploy/, tras ADR-B) → P2 (Market Data systemd, tras ADR-A) → P3 (robustez) → ADR-C → P4 (CD) → P5 (Dockerización futura, solo pre-condiciones)`. La corrección sobre el orden propuesto por el operador: los ADRs de gobernanza deben aprobarse (y committearse) **antes** de P1/P2, no al final. Ver §5.

---

## 1. Validated Architecture

Arquitectura objetivo aceptada como hipótesis de trabajo (confirmada por el audit §59/§70):

```
Debian
  ├── systemd                            ← RUNTIME canónico de la aplicación OCM
  │     └── OCM
  │          ├── Market Data             (unit: ocm-market-data.service)
  │          ├── Streaming               (unit: ocm-streaming.service)
  │          └── otros BC cuando alcancen NIVEL 1 (ADR-0024)
  └── Docker / Compose                   ← INFRAESTRUCTURA
        ├── Kafka · Redis                (dependencias de la app)
        └── Observabilidad               (Prometheus · Grafana · Loki · Alertmanager · Pushgateway)
```

**Principios del encargo (map a evidencia / estado):**

| # | Principio | Estado de evidencia |
|---|---|---|
| 1 | systemd = runtime canónico | units en VCS (`deploy/systemd/templates/`); streaming **enabled+active**; ADR-0022 Aceptado. Falta ADR formal que declare canónico a *todo* OCM (§6 ADR-A) |
| 2 | Docker/Compose = infra | compose default = redis/kafka/zk/obs; app solo en profile `microservices` OFF. Confirmado compose actual |
| 3 | Dockerización = opción futura | Dockerfile CMD `python -m market_data.main` válido; NO es runtime canónico |
| 4 | `ocm_market_data` = prueba deliberada | reclasificado EXPERIMENTAL/NO PROBLEMÁTICO (audit §60); NO bifurcación productiva |
| 5 | Git/repo = fuente declarativa | templates VCS + monitoring + `.example` (SSOT). EXCEPCIÓN VIVA: host.env trackeado (§4 D5) y `.env` line fuera del template (§3) |
| 6 | No reorganizar `deploy/` | sin evidencia nueva que lo justifique → sin mover directorios |
| 7 | `install.sh` = orquestador fino | diseño delegado §9; NUNCA God Script |
| 8 | No crear `deploy/{runtime,infrastructure,config,health}/` | mantenido; sin evidencia nueva que lo justifique |
| 9 | No units Trading/Portfolio | sin entrypoint NIVEL 1 (ADR-0024) → no units |
| 10 | CD/GHCR/SSH sin modificar | sin decisión formal de CD → **no tocar** (§6 ADR-C, §19) |

**Fronteras que NO se cruzan (verificadas):**

| Capa | Pertenece | NO es |
|---|---|---|
| REPOSITORY (Git) | código + config + deploy/{templates,scripts,monitoring,examples} | estado del host |
| DEPLOYMENT API | `deploy/scripts/install.sh` + gates G1–G11 (DSL PASS/BLOCK heredado) | reinvención de scripts |
| RUNTIME | **systemd** — procesos de la app | Docker |
| INFRAESTRUCTURE | **Docker/Compose** — Kafka/Redis/obs | runtime de la app |
| APPLICATION | procesos nativos (contrato-only, sin imports de Docker) | contenedores (hoy) |
| OBSERVABILITY | compose + app fail-soft | gate de ejecución |

---

## 2. Current State

Estado verificado read-only (2026-09-19; audit §§46–74 + re-verificación de esta misión):

| Área | Estado | Evidencia |
|---|---|---|
| Entry points | `[project.scripts]` = {ocm, ocm-api, live, paper, streaming}; market-data SOLO vía `-m market_data.main` | `pyproject.toml:176-181`; `packages/market_data/main.py:622-632` |
| Units systemd instaladas | `ocm-market-data.service` (**disabled, inactive**), `ocm-streaming.service` (**enabled, active**) | `/etc/systemd/system/ocm-*.service`; `systemctl is-enabled/is-active` |
| Installed == rendered | `diff /etc/systemd/system/*.service deploy/systemd/rendered/*.service` → **IDENTICAL** (ambos con línea `EnvironmentFile=…/.env`) | §3 tabla + diff de esta misión |
| Template (SSOT VCS) | solo `EnvironmentFile=…/deploy/host.env`; **SIN** la línea `.env` | `deploy/systemd/templates/*.template:11` vs rendered/installed `:12` |
| Contenedores | `ocm_market_data` (Up 2h, **unhealthy** — prueba del operador), `ocm_redis`, `ocm_kafka`, `ocm_zookeeper` healthy; `ocm_alertmanager` **NO existe** | `docker ps`; `docker port ocm_kafka` → 9093; `ss -ltnp` solo 9093 |
| Health | `health_check.sh` L1–L4; L2/L3 y gates G5/G6/G8 usan `docker exec` (9 sitios, §8) | `health_check.sh:36-68`; `check_production_gates.py:268-304,332-411,376-411` |
| Gates | G1–G11; **G4 roto** (`check_production_gates.py:251` apunta a `scripts/install_systemd.sh` inexistente; real: `deploy/scripts/install_systemd.sh`) | ls `scripts/` (no existe install_systemd.sh) |
| Orquestador | NO existe `deploy/scripts/install.sh`; hoy = README + 3 scripts + gates | `ls deploy/scripts/` (3 scripts) |
| Config | Hydra + `deploy/host.env` + `.env` raíz; **`.env` real con puertos INVERTIDOS vs `.env.example`** (F-10); **default Kafka 3 sitios** (F-9); **`deploy/host.env` trackeado pese a .gitignore** (D5) | §4 D5/D6/D7 |
| Market Data bootstrap | `main.py` standalone (`load_appconfig_standalone` fail-fast); FastAPI :8001; lifespan 3 tasks; `/health`:465-492, `/ready`:495-519, `/ohlcv`; `__main__ → uvicorn workers=1`:622-632 | `packages/market_data/main.py` |
| CD | `ocm-cd.yml` runner ubuntu-latest (workflow_dispatch only); `deploy_ocm.sh:161` selector compose inválido; ADR-0037 estado **Propuesto** | verificación de esta misión |
| ADRs | 0022 **Aceptado**; 0024, 0037, 0033-0038 **Propuesto**; ADR-0037 contexto obsoleto (dice "deploy_ocm.sh = inexistente") | `docs/architecture/decisions/*.md` |
| Tracking | **B-59** `systemd_reinicia_correctamente` **NO_VERIFICADO** (`tracking.yaml:3146-3158`); **B-57** estado HECHO pero CD real inoperante (reclasificar: equipo) | `docs/plans/tracking.yaml` |
| Docker host CLIs | `kafka-broker-api-versions`/`redis-cli`/`kcat` **no instalados**; `.venv` SÍ tiene `aiokafka 0.14.0` + `redis 7.4.0` → venv-based checks viables | verificación de esta misión |
| Working tree | 8 archivos pre-modificados + `docs/audit/` untracked + doc auditar | `git status --short` final (§20) |

---

## 3. Critical Drift Finding

**Contradicción a resolver (términos del encargo):**

> Anteriormente: *"installed == rendered byte-igual"* y *"drift template ↔ SO corregido"*.
> Análisis reciente: *templates NO contienen `EnvironmentFile=.../.env`; rendered SÍ; installed SÍ*.
> Potencial: `template ≠ rendered = installed`.

### Estado real observado (read-only, 2026-09-19)

| Artefacto | `EnvironmentFile=…/deploy/host.env` | `EnvironmentFile=…/.env` | Fuente |
|---|---|---|---|
| Template market-data (`deploy/systemd/templates/ocm-market-data.service.template:11`) | ✅ | ❌ | VCS (git log: sin cambios desde c70bbb69) |
| Template streaming (`deploy/systemd/templates/ocm-streaming.service.template:11`) | ✅ | ❌ | VCS |
| Rendered market-data (`deploy/systemd/rendered/ocm-market-data.service:11-12`) | ✅ | ✅ | gitignored, mtime 5-sep-00:35 |
| Rendered streaming (`deploy/systemd/rendered/ocm-streaming.service:11-12`) | ✅ | ✅ | gitignored, mtime 5-sep-00:35 |
| Installed market-data (`/etc/systemd/system/ocm-market-data.service:11-12`) | ✅ | ✅ | SO |
| Installed streaming (`/etc/systemd/system/ocm-streaming.service:11-12`) | ✅ | ✅ | SO |
| `systemctl show ocm-* -p EnvironmentFiles` | ✅ | ✅ | `host.env` Y `.env` activos, `ignore_errors=no` |

### Verificación empírica del mecanismo de render

Reproducción exacta del `render()` de `install_systemd.sh` (línea 43-49: `envsubst` con lista explícita de vars de `host.env`) sobre un directorio temporal:

```bash
set -a; source deploy/host.env; set +a
vars=$(grep -oE '^[A-Z_]+=' deploy/host.env | sed 's/=$//;s/^/$/' | tr '\n' ' ')
envsubst "${vars}" < deploy/systemd/templates/ocm-market-data.service.template > /tmp/out
#  →  /tmp/out:11 solo "EnvironmentFile=.../deploy/host.env" (SIN línea .env)
```

**Conclusión: el `render()` actual NO produce la línea `.env`.** No existe mecanismo, flag, ni condicional que la añada. El script `install_systemd.sh` en working tree == HEAD (mtime modificado pero `git diff` vacío; último commit 1bf93ad1 28-ago). No hay `git log --all` de rendered/ (gitignored, nunca versionado).

### Origen de la línea `.env`

`rendered/` se materializó el **5-sep-00:35** con la línea ya presente. El template SSOT nunca la tuvo (git log de templates: c70bbb69 2026-08-20 y afe92260 2026-08-28). Ambas units instaladas son **idénticas** a rendered (`diff` vacío). **Conclusión documental:** la línea `.env` es una **edición manual posterior al render** (operador, 5-sep) aplicada sobre rendered + SO, incompatible con la fuente declarativa. No quedó rastro en VCS.

### Respuestas a las 10 preguntas del encargo

1. **¿El template realmente carece de `EnvironmentFile`?** — NO carece de `EnvironmentFile` en general: tiene `EnvironmentFile=${OCM_REPO_ROOT}/deploy/host.env`. Carece de la **segunda** línea `EnvironmentFile=…/.env`.
2. **¿El rendered realmente lo contiene?** — SÍ (línea 12 de ambos rendered).
3. **¿La unit instalada realmente lo contiene?** — SÍ (línea 12 de ambos; confirmado también por `systemctl show -p EnvironmentFiles`).
4. **¿De dónde aparece esa línea?** — Edición manual post-render (5-sep). No la genera ningún componente del repo (verificación empírica del render, arriba). No está en git log de templates ni de install_systemd.
5. **¿El rendering añade la línea dinámicamente?** — NO. `envsubst` solo sustituye `${VAR}` de la lista explícita; no inyecta líneas.
6. **¿Existe más de una fuente de verdad?** — SÍ, tres artefactos en dos dominios: **VCS** (templates = SSOT) vs **disco** (`rendered/` gitignored + `/etc/systemd/system`). Hoy VCS ≠ disco para la línea `.env`.
7. **¿Cuál debería ser la SSOT?** — El **template VCS**. Las variables de secreto residen en `.env` (raíz, gitignored, perms 600) y las de topología en `host.env`; el proceso final consume **ambos** (co-EnvironmentFile). La SSOT debe declarar esa realidad: template con AMBAS líneas.
8. **¿Cuál es el cambio mínimo necesario?** — Añadir `EnvironmentFile=${OCM_REPO_ROOT}/.env` a **ambos templates** (commit B1/§11 C12). Alternativas rechazadas: (a) quitar `.env` del SO → rompe secrets; (b) mover secrets a `host.env` → host.env trackeado + secrets en VCS = peor; (c) unit `Environment=` estático → pierde secretos fuera del repo.
9. **¿Existe riesgo de romper secretos/configuración si se vuelve a renderizar?** — **SÍ, riesgo real inmediato.** Un re-render desde el template actual **eliminaría** la línea `.env`. Consecuencia: el proceso de market-data/streaming perdería `REDIS_PASSWORD` (requerida por `redis --requirepass` en compose `:106` y leída por `cursor_store.py:463`), `BYBIT_API_KEY/SECRET`, `KAFKA_UI_PASSWORD`, `GRAFANA_PASSWORD`, `OCM_API_JWT_SECRET`. Redis AUTH fallaría → market-data DOWN. Es el escenario que el drift-check (D1) debe prevenir.
10. **¿El drift-check actual detectaría esta divergencia?** — **NO.** No existe drift-check mecánico. G4 (`_deployment_units_ok`, `check_production_gates.py:246-265`) corre `bash scripts/install_systemd.sh --verify-only` = **ruta rota** (Scripts/ no existe) y solo ejecuta `systemd-analyze verify` (validación de sintaxis, NO paridad). `install_systemd.sh --verify-only` tampoco compara template↔rendered↔SO. La divergencia es hoy **silenciosa**.

### Verdict

- `installed == rendered` → **VERDADERO** (byte-igual, re-verificado).
- `drift template↔SO corregido` → **FALSO**. Nunca fue corregido: se *enmascaró* con una edición manual. La relación real es `template ≠ rendered = installed`.
- Riesgo de re-render sin fix previo: rompe secrets (Q9). El drift-check (D1) debe implementarse ANTES que cualquier re-render/`--apply` (orden crítico, §5).

---

## 4. D0–D12 Findings

### D5 — `deploy/host.env`: análisis completo (preguntas del encargo)

- **Contenido (26 líneas, byte-idéntico a `host.env.example`):** solo topología local — `OCM_ENV=development`, `OCM_HOST_USER=orangemusic`, `OCM_REPO_ROOT`, `MARKET_DATA_HOST/PORT=:8001`, `INGESTION_INTERVAL_S=300`, `KAFKA_ENABLED=true`, `KAFKA_BOOTSTRAP_SERVERS=localhost:9093`, `REDIS_ENABLED/HOST/PORT`. Sin credenciales.
- **¿Contiene secretos?** NO. Su propio header lo declara: *"plantilla de configuración de host (sin secretos)"*.
- **¿Valores sensibles?** Topología local (paths/ports/username). No credencial. Riesgo menor de exposición.
- **¿Quién lo consume?** `install_systemd.sh` (source para `envsubst`, line 16/38) y units systemd vía `EnvironmentFile=…/deploy/host.env` (template:11). NO lo consume docker-compose (usa `.env` raíz), ni health_check (autoderíva `OCM_REPO_ROOT`), ni CI/CD.
- **¿Está versionado intencionalmente?** NO. Está **trackeado** (`git ls-files deploy/host.env` lo lista) pese a `.gitignore:74` (`deploy/host.env`). Causa raíz: se commiteó en a2431d1f (28-ago) y el ignore se añadió después (4354ea0b "chore(gitignore): ignora deploy/host.env"); git no aplica ignore sobre archivos ya trackeados. Header + example dicen *"NUNCA commitear deploy/host.env (contiene topología local)"* → **policy violada**.
- **`.gitignore`:** entrada `deploy/host.env` presente (`:74`). Efecto nulo sobre archivo ya trackeado.
- **`host.env.example`:** existe, tracked (`git ls-files deploy/host.env.example`), **byte-idéntico** a `host.env` (diff vacío). NO está ignorado.
- **Referencias en scripts/systemd/CI:** script = `install_systemd.sh:10,16,32,36,38`; systemd = template:11 (EnvironmentFile); CI/CD = ninguna (no aparece en `.github/`).
- **¿Debe ser untracked?** SÍ. Cumple su propia policy, evita futuro leak si un operador añade secrets, y `host.env.example` queda como SSOT versionado.
- **Mecanismo de valores en producción:** `host.env` = copia local del operador (README header: `cp deploy/host.env.example deploy/host.env`); el orquestador `--apply` lo crea create-if-missing; `.env` raíz (secrets, gitignored) permanece como hoy.
- **Cambio exacto recomendado:** `git rm --cached deploy/host.env` (mantiene archivo en disco) + commit `chore(deploy): untrack host.env` (C1). Sin tocar `.gitignore` (ya tiene la entrada).

### Tabla D0–D12 (clasificación final)

| # | Acción | Categoría | ADR | Independiente |
|---|---|---|---|---|
| D0 | Fix G4 path `check_production_gates.py:251` → `deploy/scripts/install_systemd.sh` | 1 FIX AISLADO | NO | Sí |
| D1 | `install_systemd.sh`: drift-check en `--verify-only` (template↔rendered↔SO) + enable/start con guards | 2 IMPLEMENTACIÓN | SÍ (ADR-B) | Con D2 (orquestador); previo a cualquier re-render |
| D2 | Orquestador delegado `deploy/scripts/install.sh --check/--dry-run/--apply` | 2 IMPLEMENTACIÓN | SÍ (ADR-B) | No (núcleo P1) |
| D3 | Facade raíz `./install.sh` 1 línea (opcional) | 2 IMPLEMENTACIÓN | Con ADR-B | No (depende D2) |
| D4 | Health/readiness por contrato (venv aiokafka/redis; fallback `HEALTH_PROBE_MECHANISM`) | 2/3 IMPLEMENTACIÓN | SÍ (ADR-B) | No (con D2) |
| D5 | Untrack `deploy/host.env` | 5 GOBERNANZA | NO | Sí |
| D6 | Puertos Kafka/Alertmanager: decidir canonical y homogeneizar `.env.example`→runtime (F-10) | 7 DOC/1 FIX | NO (decisión de config, no ADR) | Sí (decisión del operador sobre canonical) |
| D7 | Default `KAFKA_BOOTSTRAP_SERVERS` único (F-9) | 7 DOC/1 FIX | NO | Sí |
| D8 | Paridad config: test CI `.env.example↔compose↔host.env.example` | 2/5 | NO | Sí |
| D9 | Gobernanza mínima `deploy/` (CODEOWNERS o guard doc-comment) | 5 GOBERNANZA | NO (regla, no ADR) | Sí |
| D10 | CD: decidir modelo (SSH/self-hosted / deprecar Docker / GHCR) | 4 ARQUITECTURA | **SÍ (ADR-C)** | No |
| D11 | Legacy: `deploy_ocm.sh` puertos muertos (`:49-52`) y `ocm/observability/metrics.py` placeholder | 1 FIX | D11a NO (post-decisión D10) / D11b NO (aislado) | D11b Sí; D11a No |
| D12 | `run.sh` alcance vs `[project.scripts]` | 7 DOC | NO | Sí |

**Detalles clave por finding:**

- **D0 (G4 path):** `_deployment_units_ok()` invoca `bash scripts/install_systemd.sh --verify-only` (`:251`) — ruta inexistente (real: `deploy/scripts/install_systemd.sh`). La evidencia del gate (`:102`) ya cita la ruta correcta. Fix de 1 línea + `ROOT`-absoluto.
- **D1 (drift-check + guards):** añadir a `--verify-only` re-render en memoria y diff `template↔rendered↔/etc/systemd/system` (10–15 líneas bash, audit §6). Añadir enable/start con guards `is-enabled`/`is-active` (hoy `install_systemd.sh:69-86` solo install+daemon-reload). **PRIMERO que cualquier `--apply`** (riesgo §3 Q9).
- **D6 (puertos F-10):** `.env.example` (SSOT documentado): `ALERTMANAGER_HOST_PORT=9093` (:33), `KAFKA_HOST_PORT=9094` (:40) + nota *"9094 (no 9093) para NO colisionar… F-029/B-45"*. `.env` real: `ALERTMANAGER_HOST_PORT=9094`, `KAFKA_HOST_PORT=9093` → **invertidos**. docker-compose defaults: ambos `…:-9093` (`:197`,`:521`) + comentario *"Desde host → localhost:9093"* (`:509-510`). Runtime real: kafka escucha 9093 (`docker port ocm_kafka` → 9093; `ss` solo 9093; no hay AM corriendo). `host.env` KAFKA_BOOTSTRAP=localhost:9093 = consistente con runtime 9093. **La decisión que falta (operador/equipo):** canonical = 9093 (runtime, compose, host.env) o 9094 (B-45/`.env.example`). Recomendación del plan: alinear a **runtime 9093** (menos invasivo; no toca broker) y enmendar `.env.example`/nota → D6 no es solo "arreglar .env.example", es **homogeneizar con decisión registrada**.
- **D7 (F-9):** default Kafka en 3 sitios — `config/base.yaml:46` `${oc.env:KAFKA_BOOTSTRAP_SERVERS,localhost:9092}`, `config/env/production.yaml:43` `${…,kafka:9092}`, `ocm/config/schema.py:571` `bootstrap_servers: str = "kafka:9092"`. En host solo sería válido `localhost:9093` (real). Decidir fuente canónica única + documentar.
- **D11a/D11b:** `deploy_ocm.sh:49-52` define ports muertos ALERT=9093/KAFKA=9094/… que coinciden con la CONVENCIÓN invertida vieja; metrics placeholder.

---

## 5. P0–P5 Dependency Graph

**Validación del orden propuesto por el operador** (`P0 → P1 health → P2 orchestrator → P3 market data → P4 gov → P5 CD/CI`). El análisis de dependencias obliga a **corregirlo**:

```
GOBERNANZA (pré-requisito formal; no depende de código):
  ADR-A (runtime canónico nativo) ──no deps──┐
  ADR-B (deploy boundary + orquestador) ─no deps─┤   aprobar/committear ANTES de P1/P2
                                            │    │
P0 (sin ADR; 5 fixes aislados; NADA toca runtime):      ← orden interno por coste/riesgo
  C1 D5 untrack host.env   (no deps)
  C2 D0 G4 path            (no deps)
  C3 D6 puertos canonical  (decisión config, no ADR)
  C4 D7 default Kafka      (no deps)
  C5 D11b metrics cleanup  (no deps)

P1 (completar deploy/) ← requiere ADR-B aprobado (C16/§11), P0 implícito (G4 fiable)
  C6 D1 install_systemd: drift-check + enable/start guards   (PRIMERO, previene §3-Q9)
  C7 D2 install.sh orquestador delegado (--check/--dry-run/--apply)
  C8 D3 facade raíz optional
  C9 D4 health por contrato (venv) — desacopla docker exec
  C10 D8 parity config guard
  C11 D9 gobernanza deploy/

P2 (Market Data systemd) ← requiere ADR-A aprobado (C15/§11); usa D1+D2+D4
  C12 B1 template: añadir EnvironmentFile …/.env (fix drift §3)
  C13 B5 /ready deps por contrato + TestClient smoke (opcional)
  B2  transición operador: stop container test → enable --now unit   (operación, no commit)
  B3  B-59 prueba de reinicio real → evidencia → tracking           (operación, no commit)

P3 (robustez/doc; sin bloqueo)
  C14 B4/D12 docs (README, run.sh scope, README arch "DETENIDO"→operativo, enmienda ADR-0022:323)

GOBERNANZA (P4): AGDR-C (modelo CD + enmienda/cierre ADR-0037 + tracking B-57/B-59) ← después de ADR-A/B y runtime real consolidado

P4 (ejecutar decisión CD) ← tras ADR-C
  D10/D11a CD (SSH/self-hosted → install.sh --apply | deprecar | GHCR)

P5 (Dockerización futura) ← SOLO pre-condiciones; NO implementar (§17)
```

**Dependencias que FUERZAN este orden (no opinión):**
1. **ADR-B antes de P1** y **ADR-A antes de P2**: el repo exige cadena *conocimiento → decisión humana → ADR → control* (AGENTS.md). Implementar el orquestador o canonizar Market Data sin ADR sería saltarse gobernanza. → **corrección al plan previo**, que ponía los ADRs en los commits 15–17 (al final).
2. **D1 antes que cualquier re-render/`--apply`**: riesgo §3-Q9 (re-render sin drift-check elimina `.env` y rompe secrets). En P2 no se puede llegar sin D1.
3. **P0 ≥ G4 reparado** antes de P1: D1 difiere del gate roto; sin C2, la verificación de P1 es ruido.
4. **B2 (parar container) antes de `enable --now`**: colisión :8001 (DR-5, audit §32).
5. **ADR-C antes de tocar CD**: principio 10 del encargo (no modificar CD sin decisión formal).

---

## 6. ADR Requirements

Clasificación (A = decisión ya existente solo alinear / B = decisión nueva requiere ADR / C = cambio pequeño NO requiere ADR):

| ADR | Tema | Clase | Decisión | Requerido antes de |
|---|---|---|---|---|
| **ADR-A** (nuevo) | Runtime canónico: systemd = app, Docker = infra, dockerización opción futura | **B** | ratifica el modelo del encargo + audit §59; formaliza que `ocm_market_data` es experimento | P2 (canonización Market Data) |
| **ADR-B** (nuevo) | Boundary `deploy/` + orquestador delegado + drift-check + DOI units (templates = SSOT) | **B** | autoriza `install.sh --check/--apply` delegado, guards, drift-check; sin dirs nuevos | P1 |
| **ADR-C** (nuevo) | Modelo de CD: SSH/self-hosted ↔ deprecar Docker/GHCR | **B** | elegir UNA vía; cierra ADR-0037 (Propuesto, contexto obsoleto) y reclasifica B-57 | P4 (D10) |
| ADR-0022 | Lifecycle streaming y market-data (Aceptado) | **A** | ya norma streaming/systemd; **enmienda** `:323` "Sin unit systemd activo" → obsoleto | B4/C14 (doc) |
| ADR-0037 | CD verify/deploy/rollback (**Propuesto**) | **A→C cierre** | contexto obsoleto (dice "deploy_ocm.sh = inexistente" y "revisto inexistentes" en `:21`, hoy existen); el ADR es propuesta, no norma. Cerrar/enmendar dentro de ADR-C | ADR-C |
| ADR-0024 | Dirección microservicios (Propuesto) | **A** | refuerza no crears units trading/portfolio (NIVEL 2); alinear `README.md` con niveles | P2/P3 docs |
| D0, D5, D6, D7, D9, D11b, D12 | fixes aislados | **C** | sin ADR | — |

**Regla aplicada:** cada bug pequeño NO requiere ADR (D0, D5, D6, D7, D9, D11b, D12). Requieren ADR los cambios estructurales: ADR-A (runtime canónico), ADR-B (boundary/orquestador/drift/DOI), ADR-C (CD). Obsolescencia de ADR-0037 y ADR-0022:323 se resuelven como **enmienda dentro de** ADR-C / B4 (no ADRs independientes).

---

## 7. Deployment Implementation Plan

Secuencia de fases para "terminar `deploy/`":

1. **P0 (5 fixes aislados, sin ADR):**
   - **D5** `git rm --cached deploy/host.env` → sanity: `git ls-files deploy/` sin host.env; no re-add.
   - **D0** `check_production_gates.py:251` ruta → `deploy/scripts/install_systemd.sh` (/ROOT-resolved). Restaura confianza de G4.
   - **D6** decisión canonical de puertos + homogeneizar `.env.example` ↔ runtime (§4). Requiere decisión del operador.
   - **D7** unificar default `KAFKA_BOOTSTRAP_SERVERS` (decisión de fuente: base.yaml `localhost:9093` o schema único) + tests config.
   - **D11b** `ocm/observability/metrics.py` placeholder cleanup (headers/dead).
2. **P1 (completar deploy/, tras ADR-B):**
   - **D1** `install_systemd.sh` drift-check en `--verify-only` + enable/start con guards. **Orden crítico:** antes de cualquier re-render.
   - **D2** `install.sh --check/--dry-run/--apply` delegado (diseño §9). Default read-only.
   - **D3** facade raíz opcional (1 línea `exec`).
   - **D4** health por contrato (venv) — fallback `HEALTH_PROBE_MECHANISM=docker|venv|http`.
   - **D8** parity guard config en CI.
   - **D9** gobernanza mínima `deploy/`.
3. **P2 (Market Data systemd, tras ADR-A)** — ver §10.
4. **P3 (robustez/doc)** — D12, TestClient, parity en CI.
5. **P4 (CD)** — tras ADR-C: D10/D11a.
6. **P5 (Dockerización)** — NO implementar hoy; solo precondiciones (§17).

---

## 8. Health Contract Migration

**7 consumidores actuales de `docker exec` (tabla por contrato):**

| Consumidor | Uso actual | Dependencia Docker | Contrato objetivo | Archivo | Test | Riesgo |
|---|---|---|---|---|---|---|
| `health_check.sh` L2 Kafka | `docker exec ocm_kafka kafka-broker-api-versions --bootstrap-server localhost:9092` | ALTA (exec + container) | venv `aiokafka → producer.metadata()` o TCP raw a `localhost:9093`, o `HEALTH_PROBE_MECHANISM` | `deploy/scripts/health_check.sh:36-38` | unit mock + integración infra | migra INDEPENDIENTE |
| `health_check.sh` L2 Redis | `docker exec ocm_redis redis-cli -a "$pass" ping` | ALTA | venv `redis → ping()` | `deploy/scripts/health_check.sh:41-44` | unit mock + integración | migra INDEPENDIENTE |
| `health_check.sh` L3 offsets | `docker exec ocm_kafka kafka-get-offsets …` | ALTA | venv `aiokafka end_offsets/list_offsets` sobre topics `ohlcv.raw`/`orderbook.raw` | `deploy/scripts/health_check.sh:47-64` | unit + integración Kafka | migra INDEPENDIENTE |
| Gate G5 market-data HTTP+fresh | `curl :8001/health` + `docker exec ocm_kafka kafka-get-offsets orderbook.raw` | MEDIA (HTTP ya local) | `curl :8001/health` + venv offsets (o `/ready` deps) | `check_production_gates.py:268-304` | prueba local gates `--mode gate-dev` | migra INDEPENDIENTE |
| Gate G6 Kafka/Redis | `docker exec ocm_kafka kafka-broker-api-versions` + `docker exec ocm_redis redis-cli ping` | ALTA | venv (aiokafka metadata, redis ping) | `check_production_gates.py:332-356` | gates local | migra INDEPENDIENTE |
| Gate G8 trading stub | `docker exec ocm_kafka kafka-get-offsets orderbook.raw` | ALTA | venv offsets | `check_production_gates.py:376-411` | gates local | migra INDEPENDIENTE |
| `deploy_ocm.sh` wait_for_health | `docker exec ocm_redis/ocm_kafka` (L2 duplicado) | ALTA | **delega en `health_check.sh`** (eliminar duplicado) | `deploy/scripts/deploy_ocm.sh:169-200` | — (CD tras ADR-C) | **NO migrar hasta ADR-C** (script depende del destino del CD) |

**Especificación (sin implementar):**
- **Helper compartido:** `deploy/scripts/health_probe.py` (o extensión de `health_check.sh`) que lee env contract (SSOT `ocm/config/env_vars.py`) y usa libs del venv de la app (`aiokafka 0.14.0`, `redis 7.4.0` — verificadas en `.venv`). Fallback explícito `HEALTH_PROBE_MECHANISM=docker|venv|http`.
- **Compatibilidad:** exit codes `0=HEALTHY / 1=DEGRADED / 2=DOWN`, formato `MARKET_DATA_HEALTHY=…` y gates G7/G9 NO cambian. Host sin CLIs nativas OK (venv presente).
- **Migración:** dual docker+venv durante una release; flip default a venv cuando Docker deje de ser el único mecanismo. Sin novación en caliente.
- **Migración independiente:** los 6 consumidores de health_check+gates son independientes entre sí (mismo contrato). `deploy_ocm.sh` queda en focha de ADR-C (no migrar ahora).

---

## 9. Orchestrator Design

**Criterio estricto del encargo — `deploy/scripts/install.sh` como COMPOSITOR/ORQUESTADOR:**

Debe **validar → delegar → coordinar → comprobar → reportar**. Para cada responsabilidad, el componente existente que recibe la delegación:

| Responsabilidad | Delega en | Prohíbe / notas |
|---|---|---|
| Dependencias de runtime | `uv sync --group dev` | no instalar Python/uv del OS; no `pip` directo |
| Config create-if-missing | `cp .env.example .env` / `cp deploy/host.env.example deploy/host.env` (solo si falta) | NO sobrescribir; NO redefinir valores |
| Validación config | `OCM_VALIDATE_ONLY=true uv run python -m app.cli.main` (Hydra+Pydantic) | no duplicar Hydra; no re-implementar L5 |
| Infra up | `docker compose up -d` (config-guard nativo) | no re-implementar compose, ni `install` services |
| Topics | `uv run python scripts/provision_kafka_topics.py` (idempotente) | no re-crear lógica de tópicos |
| Systemd render/verify/install | `deploy/scripts/install_systemd.sh --apply` (con guards + drift-check D1) | no duplicar render/envsubst; no re-escribir units |
| Drift-check systemd | `install_systemd.sh --verify-only` (re-render en memoria + diff) | no implementar su propio framework de health |
| Health/readiness | `deploy/scripts/health_check.sh` (L1–L4, exit 0/1/2) | no re-implementar health; no `docker exec` nuevo |
| Gates | `python scripts/check_production_gates.py --mode gate-dev` (DSL PASS/BLOCK heredado, sin DSL nuevo) | no crear DSL paralelo |
| Secrets | `.env` (gitignored) + `host.env` — **solo create-if-missing + permisos** | NO implementar sistema de secrets propio; NO aceptar secrets por arg/flag |
| Reporte | stdout `PASS/FAIL/SKIP` + exit code 0/1 | — |

**No hace:** no criando un segundo sistema de config (toda config viene de Hydra/Pydantic/host.env/.env), no re-implementa Docker Compose, no gestiona secrets de proceso, no ejecuta el pipeline, no es dueño del CD, no es un God Script. Default = `--check` (read-only); `--apply` mutante requiere confirmación y pasa por los guards.

**Boundary:** termina cuando sistema instalado/verificado/sano (units enable+active, drift-check PASS, health 0, gates PASS). No supervisa procesos (`systemd` lo hace), no construye imágenes (`Dockerfile` no es de él).

---

## 10. Market Data Completion Plan

Market Data permanece como **trabajo separado del deployment general**. Definición de Done completa en §16. Resumen de trabajo por eje:

- **A. Definition of Done funcional (§16-FUNC):** `python -m market_data.main` arranca HTTP :8001 + 3 tasks; `/health` 200, `/ready` 200, `/ohlcv` 200 con datos; flows `ohlcv.raw`/`orderbook.raw` fresh; Bronze nuevo.
- **B. Definition of Done deployment (§16-DEPLOY):** unit `ocm-market-data.service` **enabled+active**; template SSOT == rendered == `/etc/systemd/system` (drift-check PASS); container test parado; **B-59 verificado con prueba de reinicio** (NO marcar verificado solo porque arranca).
- **C. Definition of Done observabilidad (§16-OBS):** métricas por env engine; pushgateway; dashboards/alerts; fail-soft preservado.
- **D. Definition of Done health/readiness (§16-HEALTH):** `health_check.sh` L1-L4 por contrato; gates G5/G6/G7/G9 PASS; `/ready` (si B5) pinna Kafka/Redis sin Docker.
- **E. Definition of Done operacional (§16-OPER):** journald, restarts documentados, SIGINT graceful.
- **F. Definition of Done testing (§16-TEST):** units `tests/market_data/` PASS; smoke `TestClient` `/health`+`/ready` (cierra brecha ADR-0024); gates PASS.
- **G. Definition of Done B-59 (§16-B59):** prueba REAL de restart en OrangeHouse (streaming y market-data) con evidencia; tracking actualizado SOLO con esa evidencia. B-59 se mantiene **NO_VERIFICADO** hasta entonces.

**Transición controlada (P2, orden crítico):**
1. **C12/B1** — fix template `.env` (drift-check debe estar disponible; instalado == rendered).
2. **B2** — operador: `docker stop ocm_market_data` (container test) → `systemctl enable --now ocm-market-data.service` → verificar `is-enabled`/`is-active`, `:8001/health`, `:8001/ready`, Bronze fresh. Colisión :8001 evitada por el orden.
3. **B3/B-59** — `systemctl restart ocm-market-data.service ocm-streaming.service` → `is-active`, datos siguen fluyendo (L3/L4), documentar. Solo entonces tracking B-59 → verificado.
4. **B4** — docs README + `docs/architecture/README.md:45` quitar "DETENIDO".

---

## 11. Commit-by-Commit Plan

**Pre-condición obligatoria (G11):** el working tree tiene 8 archivos ` M ` pre-existentes + `docs/audit/` untracked. **No mezclar commits**: esos cambios deben resolverse (commitear/revertir) por el operador ANTES de abrir ramas. Ningún commit de este plan los toca.

Reglas por commit: UN propósito · mínimo de archivos · reversible · validable independiente · no mezcla bug+arquitectura · no mezcla deployment+market-data sin necesidad.

**ORDEN ejecución valida (dependencias §5):** gobernanza ADRs primero (C15, C16), luego P0 (C1–C5), luego P1 (C6–C11, tras ADR-B), luego P2 (C12–C14, tras ADR-A), luego ADR-C (C17) y P4/CD (C18, potestad).

| # | Tipo | Mensaje propuesto | Problema | Archivos | Requiere ADR | Dep. de |
|---|---|---|---|---|---|---|
| C1 | chore(deploy) | `untrack host.env` | D5 — host.env trackeada pese a .gitignore/policy | `deploy/host.env` (git rm --cached) | NO | — |
| C2 | fix(deploy) | `repair G4 install_systemd path` | D0 — ruta rota `:251` | `scripts/check_production_gates.py` | NO | — |
| C3 | fix(config) | `align Kafka/Alertmanager port canonical` | D6/F-10 — puertos invertidos | `.env.example`, nota F-029/B-45 | NO (decisión config) | decisión operador canonical |
| C4 | fix(config) | `single Kafka bootstrap default` | D7/F-9 — 3 defaults | `config/base.yaml`, `config/env/production.yaml`, `ocm/config/schema.py` (según decisión) | NO | — |
| C5 | chore(obs) | `clean metrics placeholder` | D11b | `ocm/observability/metrics.py` | NO | — |
| C15 | adr | `runtime canónico nativo (ADR-A)` | modelo implícito; Market Data en intersección | `docs/architecture/decisions/ADR-00XX-…md`, tracking | (es el ADR) | — |
| C16 | adr | `deploy boundary + orquestador (ADR-B)` | sin API operacional; drift real; B-59 sin cerrar | idem + tracking | (es el ADR) | — |
| C6 | feat(deploy) | `systemd drift-check + enable/start guards (D1)` | §3-Q9; sin drift-check hoy; sin guards | `deploy/scripts/install_systemd.sh` + test | SÍ (ADR-B) | C16 |
| C7 | feat(deploy) | `install.sh orchestrator --check/--dry-run/--apply (D2)` | sin API de instalación/verificación | `deploy/scripts/install.sh` | SÍ (ADR-B) | C6 |
| C8 | feat(deploy) | `root install.sh facade (D3)` | onboarding `./install.sh` | `./install.sh` (1 línea) | Con ADR-B | C7 |
| C9 | feat(deploy) | `health probes by contract (D4)` | 6 consumidores docker exec migrables | `deploy/scripts/health_check.sh`, `health_probe.py`, gates G5/G6/G8 | SÍ (ADR-B) | C7 |
| C10 | feat(deploy) | `parity ports guard (D8)` | drift .env↔compose no testado | nuevo `scripts/check_ssot_ports.py` o test | NO | — |
| C11 | chore(deploy) | `governance rule deploy/ (D9)` | sin dueño de deploy/ | CODEOWNERS o guard | NO (regla) | — |
| C12 | fix(systemd) | `add .env EnvironmentFile to templates` | UUID §3 drift Q8 | `deploy/systemd/templates/*.template` ×2 | SÍ (ADR-A) | C6 (drift-check listo) |
| C13 | feat(md) | `/ready deps by contract + TestClient` | B5 opcional; brecha ADR-0024 | `packages/market_data/main.py`, test TestClient | SÍ (ADR-A) | C12 |
| C14 | docs(deploy) | `systemd install path + run.sh scope` | B4/D12; README "DETENIDO" | `README.md`, `docs/architecture/README.md`, `run.sh` | NO | C12 |
| C17 | adr | `modelo CD + enmienda ADR-0037/0022 + tracking B-57/B-59 (ADR-C)` | CD inerte; ADR-0037 obsoleto; B-57 vs realidad | ADRs, tracking (equipo) | (es el ADR) | C15,C16 |
| C18 | fix(cd) | `execute CD decision per ADR-C` (D10/D11a) | selector compose inválido, runner ephemeral | `deploy/scripts/deploy_ocm.sh`, `ocm-cd.yml` | SÍ (ADR-C) | C17 |

**Detalle 14 campos por commit (solo C1, C2, C6, C7, C12 — los críticos; resto en tabla):**

**C1 — `chore(deploy): untrack host.env`**
- Tipo: chore → mensaje `chore(deploy): untrack host.env (topología local; host.env.example es SSOT)`.
- Problema: `deploy/host.env` trackeada pese a `.gitignore:74` y su propio header "NUNCA commitear"; policy §4-D5.
- Archivos: `deploy/host.env` (git rm --cached). `.gitignore` ya tiene la entrada.
- Cambios: deja de trackearse; permanece en disco.
- Tests: `git ls-files deploy/host.env` → vacío; `git status` limpio post-commit.
- Gates: G11 (working tree + .env check no captura host.env — verificar falso negativo).
- Local: `git ls-files deploy/ | rg host.env` → sin salida.
- OrangeHouse: N/A (sin efecto runtime).
- Riesgos: bajo; host.env y host.env.example byte-idénticos → sin pérdida de estado.
- Rollback: `git add -f deploy/host.env` (o re-cp desde example).
- Dep: ninguna.
- ADR: NO.

**C2 — `fix(deploy): repair G4 install_systemd path`**
- Problema: `_deployment_units_ok()` → `bash scripts/install_systemd.sh` (NO existe). Real: `deploy/scripts/install_systemd.sh`.
- Archivos: `scripts/check_production_gates.py:251`.
- Cambios: ruta con `ROOT / "deploy" / "scripts" / "install_systemd.sh"`.
- Tests: gates `--mode gate-ci`/local; `systemd-analyze verify` del rendered.
- Gates: G4 (paso a blue/Q con units presentes y verify OK).
- Local: `uv run python scripts/check_production_gates.py --mode gate-ci`.
- OrangeHouse: `install_systemd.sh --verify-only` manual previo (idempotente).
- Riesgos: si units rendered ausentes, G4 sigue FAIL (esperado en CI ubuntu, no es systemd).
- Rollback: revert commit.
- Dep: ninguna.
- ADR: NO.

**C6 — `feat(deploy): systemd drift-check + enable/start guards (D1)`**
- Problema: re-render elimina línea `.env` silenciosamente (§3-Q9); sin guards de enable/start hoy (`install_systemd.sh:69-86`).
- Archivos: `deploy/scripts/install_systemd.sh` + test de drift (editar template→FAIL).
- Cambios: en `--verify-only`: re-render en memoria y comparar template↔rendered↔`/etc/systemd/system`→ reportar FAIL si difieren; nuevo modo `--enable` con guards `is-enabled`/`is-active`.
- Tests: unit del drift-check (mock files); `--verify-only` sobre repo limpio.
- Gates: G4 (corregido C2) + gates locales.
- Local: `install_systemd.sh --verify-only` 2× idempotente; editar temporalmente template → esperar FAIL (sin aplicar).
- OrangeHouse: `install_systemd.sh --verify-only` (detectará el drift `.env` actual → evidencia §3).
- Riesgos: alters de SO no (solo verifica); `--apply` con guards evita doble enable.
- Rollback: revert; units no cambian hasta que operador aplique.
- Dep: C16 (ADR-B), C2.
- ADR: SÍ (ADR-B).

**C7 — `feat(deploy): install.sh orchestrator --check/--dry-run/--apply (D2)`**
- Problema: conocimiento instalador repartido README+3 scripts+gates; sin API única.
- Archivos: `deploy/scripts/install.sh` (~120-150 líneas, delegado §9) + test de secuencia.
- Cambios: default `--check` read-only; `--apply` mutante con confirmación; secuencia uv→env→validate→compose→topics→systemd→health→gates.
- Tests: `install.sh --check` 2× (idempotencia) en checkout limpio + infra compose.
- Gates: G1–G11 (DSL heredado, no nuevo).
- Local: checkout limpio con infra compose; `--check` x2; `--dry-run` log de pasos.
- OrangeHouse: `install.sh --check` read-only (sin mutar); luego `--apply` en ventana con operador.
- Riesgos: `--apply` con falso sudo; main ordenado por guards; palabras clave `SKIP` con razón.
- Rollback: revert; no muta units hasta `--apply`.
- Dep: C6 (D1), C16.
- ADR: SÍ (ADR-B).

**C12 — `fix(systemd): add .env EnvironmentFile to templates`**
- Problema: drift crítico §3 — template SSOT sin `.env`; re-render rompería secrets.
- Archivos: `deploy/systemd/templates/ocm-market-data.service.template:11`, `ocm-streaming.service.template:11` (añadir `EnvironmentFile=${OCM_REPO_ROOT}/.env`).
- Cambios: declarar contenido operativo real (host.env + .env) en SSOT.
- Tests: drift-check (C6) → template↔rendered↔SO PASS tras re-instalar; `systemd-analyze verify`.
- Gates: G4.
- Local: `install_systemd.sh --verify-only` → diff vacío; sin `--apply` salvo operador.
- OrangeHouse: aplicar una vez (`install_systemd.sh`), verificar `systemctl cat` y `is-active`.
- Riesgos: bajo (alinea SSOT con realidad); si se aplica sin drift-check primero→C6 previo. `.env` perms 600.
- Rollback: revert template; re-instalar si procede.
- Dep: C6 (drift-check), C15 (ADR-A).
- ADR: SÍ (ADR-A, canonización Market Data).

---

## 12. Local Validation

```
uv run ruff check . && uv run ruff format . --check
uv run mypy .
uv run lint-imports --config architecture_linter/importlinter.toml
uv run pytest tests/ -x -q
uv run python scripts/audit_validator.py                # M1..M20 (governance)
uv run python scripts/check_production_gates.py --mode gate-ci   # G1,G2,G3,G10,G11
OCM_VALIDATE_ONLY=true uv run python -m app.cli.main    # Hydra+Pydantic bootstrap
```
Específicos por fase:
- **P0/C1-5:** `git ls-files deploy/host.env` vacío; G4 oracle; `diff .env.example` vs canonical decidido; OCM_VALIDATE_ONLY; ruff/mypy/audit_validator.
- **P1/C6-11:** `install_systemd.sh --verify-only` x2 (idempotencia); drift test (editar template→FAIL); `install.sh --check` x2 en checkout limpio con infra compose; health_check exit codes 0/1/2; gates G5/G6/G7.
- **P2/C12-14:** `diff template vs rendered vs /etc/systemd/system` vacío; `systemctl is-enabled/is-active`; `curl :8001/health`/`/ready`; health_check L4; prueba de reinicio documentada.
- **P3:** pytest TestClient `/health`+`/ready`; parity guard; G4 en CI.
- NO asumir que local sustituye OrangeHouse (gates de runtime requieren host real).

---

## 13. OrangeHouse Validation

Host de producción — transición controlada (post ADR-A/B, P1, dentro de P2):
```
docker compose ps                                   # infra sana (redis/kafka/zk healthy)
# transición B2 (orden crítico):
docker stop ocm_market_data                         # 1º parar container test (colisión :8001)
systemctl enable --now ocm-market-data.service
systemctl is-enabled ocm-market-data.service        # → enabled
systemctl is-active  ocm-market-data.service        # → active
curl -fsS localhost:8001/health ; curl -fsS localhost:8001/ready
./deploy/scripts/health_check.sh → MARKET_DATA_HEALTHY=HEALTHY, exit 0
# B-59 (prueba real de reinicio):
systemctl restart ocm-market-data.service ocm-streaming.service
sleep 30; systemctl is-active ocm-* ; health_check.sh ; journalctl -u ocm-* -n 20
# drift-check: template↔rendered↔SO = PASS
```
Tabla estado real (2026-09-19) a re-validar tras P2:
| Check | Estado objetivo | Estado hoy |
|---|---|---|
| `systemctl is-enabled ocm-market-data` | enabled | disabled |
| `systemctl is-active ocm-market-data` | active | inactive |
| container `ocm_market_data` | parado/retirado | Up (unhealthy) |
| `diff rendered /etc/systemd/system` | IDENTICAL | IDENTICAL |
| `drift-check template↔SO` | PASS | **FAIL (drift `.env`)** |
| `curl :8001/health` | 200 healthy | N/A (unit inactiva) |
| Kafka listener | EXTERNAL host 9093 | 9093 |
| B-59 restart | evidencia | NO_VERIFICADO |

---

## 14. CI Validation

- **Workflow actual (ocm-ci):** architecture contracts (import-linter) → tests + config-validation (parallel, fail-fast). Sin cambios estructurales para P0–P2.
- **P3 (no-bloqueantes salvo decision):** job `install.sh --check` read-only en runner (valida templates/scripts sin mutar; systemd-analyze solo donde factible); job parity `.env.example↔compose`; job drift re-render+diff; G4 corregido corre en modos code-level (no en ubuntu ephemeral sin systemd: se cubre con parity/rendered tests).
- **Policy/evidence:** gates G1–G11, audit_validator M1–M20, engineering_health_check PASS, semgrep, bandit. Evidencia de cada job en artifacts.
- **CD:** no se modifica hasta ADR-C/P4 (§6, §19). Si ADR-C elige self-hosted → `install.sh --check` dentro del runner becomes standard; si depreca → se retira `ocm-cd.yml`/`deploy_ocm.sh`.

---

## 15. Definition of Done — Deploy

`deploy/` TERMINADO cuando TODOS verificables (nada de "funciona en mi máquina"):
1. **Instalación reproducible:** `deploy/scripts/install.sh --apply` sobre checkout limpio (Debian + uv + docker) converge a sistema sano; `--check` idempotente (2ª/10ª = mismas PASSes, sin mutaciones).
2. **systemd:** `is-enabled`=enabled y `is-active`=active para `ocm-market-data.service` y `ocm-streaming.service`; units SOLO de templates VCS.
3. **Configuración:** `.env` y `host.env` creados por `--apply` create-if-missing sin sobrescritura; defaults de puertos únicos y coherentes (F-9/F-10 resueltos); `host.env` NO trackeado (D5).
4. **Dependencias/orden:** units `After=network-online.target docker.service` documentados como decisión single-host (ADR); infra compose arranca antes vía `--apply`.
5. **Health/readiness:** `health_check.sh` L1–L4 por contrato (sin docker exec obligatorio; fallback `HEALTH_PROBE_MECHANISM`); exit 0 = HEALTHY; gates G5/G6/G7/G8 por contrato.
6. **Logging:** `journalctl -u ocm-*` operativo; rutas ReadWritePaths documentadas.
7. **Restart:** B-59 verificado con prueba de reinicio documentada; `Restart=on-failure` confirmado.
8. **Idempotencia:** `install.sh --check` no muta; `--apply` 10× estable.
9. **Drift detection:** `--verify-only`/`--check` detecta divergencia template↔rendered↔SO y reporta FAIL (incluye el caso `.env` que hoy es silencioso; §3-Q10).
10. **Validación post-install:** health_check + gates + OCM_VALIDATE_ONLY PASS tras `--apply`.
11. **Rollback:** procedimiento escrito (backup units; re-render; `systemctl disable --now`); `install.sh` reporta PASS/FAIL/SKIP + exit code.
12. **Correspondencia Git↔systemd:** `diff deploy/systemd/rendered/*.service /etc/systemd/system/*.service` = vacío Y templates = rendered (drift-check PASS). Units re-generables 1:1 desde repo.

---

## 16. Definition of Done — Market Data

DoD independiente por eje (no declarar terminado solo porque el proceso arranca):

**FUNCIONALIDAD**
- `python -m market_data.main` inicia: HTTP :8001, lifespan 3 tasks (ingestion + Kappa Bronze + FeedOrchestrator).
- `GET /health` 200 (liveness), `GET /ready` 200 (readiness) al inicializar; `/ohlcv/...` 200 con datos Silver.
- Datos fluyen: `ohlcv.raw`/`orderbook.raw` con fresh offsets y Bronze nuevo (L3/L4 HEALTHY).

**DEPLOYMENT**
- Unit `ocm-market-data.service` = **enabled + active** (systemd canónico).
- Template SSOT == rendered == `/etc/systemd/system` (drift-check PASS; §3).
- Container `ocm_market_data` de pruebas parado/retirado (sin colisión :8001).
- Reinicio verificado (DoD B-59): restart → is-active + datos continúan.

**OBSERVABILIDAD**
- Métricas por env (`metrics.enabled`) activas en production; streaming → Pushgateway; dashboards/alerts presentes; **fail-soft preservado** (no hard-dependency).

**HEALTH/READINESS**
- `health_check.sh` L1/L2/L3/MD HEALTHY; gates G5/G6/G7/G9 PASS por contrato; `/ready` (si B5) pinna Kafka/Redis sin Docker.

**OPERACIONAL**
- Logs journald (`journalctl -u ocm-market-data.service`), loguru estructurado.
- restart/stop/start documentados; SIGINT graceful (KillSignal=SIGINT, TimeoutStopSec=30).

**TESTING**
- Unit tests `tests/market_data/` PASS (G1).
- Smoke `/health`+`/ready` vía `TestClient` (cierra brecha ADR-0024).
- `check_production_gates.py --mode gate-dev` con market-data HEALTHY.

**B-59**
- Prueba REAL de restart en OrangeHouse (evidencia: command output + journald + L3/L4 post-restart). Tracking B-59 actualizado a verificado SOLO con esa evidencia (§10-G).

---

## 17. Future Dockerization Preconditions

NO implementar. Solo pre-condiciones de contrato (invariantes para que la dockerización sea otra forma de ejecutar OCM, no una dependencia interna):
- **Config independiente del mecanismo de runtime:** Hydra/Pydantic SSOT; env por contrato (`KAFKA_BOOTSTRAP_SERVERS`, `REDIS_*`, `PUSHGATEWAY_URL`, `OCM_STORAGE__*`, `MARKET_DATA_HOST/PORT`); secretos SOLO en `.env` gitignored.
- **Health/readiness contract:** `/health` liveness + `/ready` readiness HTTP (mismo contrato dentro/fuera); `health_check.sh` por contrato (D4) independiente del mecanismo.
- **Dependencia de infra por endpoint/protocolo:** Kafka/Redis por host:port (nunca container names desde la app) — verificado hoy (sin refs).
- **Logs:** stdout/journald (Docker no requiere más).
- **Señales/shutdown:** SIGINT/SIGTERM graceful (KillSignal, handlers asyncio; streaming usa `add_signal_handler`), TimeoutStopSec.
- **Persistencia:** `data_platform/` bind-mount; `OCM_STORAGE__DATA_LAKE__PATH` env-configurable.
- **Secrets:** Docker no debe necesitar `.env` en imagen; solo env inyectado.
- **Observabilidad:** métricas/tracing por env; pushgateway en red del container.
- **Networking:** containers en red compose; app escucha 8001/.. por env.
- **Reproducibilidad:** `uv.lock` + Dockerfile multi-stage (targets); pins por razones (pyproject).
- **Habilitadores previos (ADR):** ADR-A (runtime canónico) y ADR-C (modelo CD) aprobados; deploy/ terminado (D1–D9); Market Data bajo systemd verificado; entrypoints NIVEL 1 de trading/portfolio (ADR-0024) o CD de imágenes real. Sin ello, sigue siendo experimento.

---

## 18. Risks and Mitigations

| Riesgo | Causa | Impacto | Prob. | Mitigación | Evidencia | Rollback |
|---|---|---|---|---|---|---|
| Re-render elimina `.env` y rompe secrets | drift template↔SO (§3) | market-data/streaming pierden REDIS_PASSWORD/BYBIT/… → Redis AUTH fail, DOWN | **Alta** (si se aplica sin C6/C12) | D1 drift-check PRIMERO; C12 template fix; orden crítico §5 | §3-Q9; `cursor_store.py:463`; compose `:106` | revert; re-install desde template corregido |
| Working tree contaminado | 8 ` M ` + untracked pre-existentes | mezclas de commits, G11 FAIL | Alta | resolver antes de abrir ramas; este plan no los toca | `git status --short` (§20) | — |
| Colisión :8001 | container test + unit nativa simultáneos (DR-5) | doble bind/consumer | Media | B2: `docker stop` ANTES de `enable --now` | audit §32 | `systemctl disable --now`; re-start container |
| CD toca antes de decisión | ADR-C no aprobado | CD sigue inerte; manual | Alta si P4 temprano | ADR-C primero; no tocar CD en P0–P3 | §6 | — |
| G4 sigue roto/regresión CI | path `:251` | gates de units sin confianza | Media | C2 + tests parity/rendered; no systemd en CI ubuntu | §4-D0 | revert |
| host.env re-add accidental | flujo sin guard | secrets futuros en VCS | Media | D5 + gobernanza D9 + guard CI (peso `.env` checks) | §4-D5 | `git rm --cached` |
| Health contract rompe exit codes | migración venv | G7/G9 regresan | Media | dual docker+venv; tests unit+mock; `HEALTH_PROBE_MECHANISM` fallback | §8 | revert C9 |
| Kafka/Redis indisponibles | infra down (SPOF single-host) | DEGRADED/DOWN honestos | Media | monitoreo; infra externalizable vía contratos (D4) | compose | — |
| B-59 marcado sin prueba | presión de cierre | modelo operativo no verificado | Media | NO marcar sin restart real; procedimiento §13 | tracking:3146 | — |
| ADR-A/B sin aprobar antes de P1/P2 | orden del plan previo | gobernanza saltada | Alta (proceso) | gobernanza primero (§5, §11 C15/C16) | §6 | — |
| puertos canonical ambigu | F-10 / B-45 vs runtime 9093 | operador equivocado al copiar .env.example | Media | decisión registrada + D8 parity guard | §4-D6 | — |
| `.env` perms débiles | unit systemd lee `.env` | exposición local | Baja | chmod 600 documentado; secrets nunca en host.env/repo | §3-Q8 | — |

---

## 19. Explicitly Out of Scope

En esta fase NO se hará (confirmado):
- **NO** dockerizar OCM.
- **NO** mover Market Data a Docker (sigue systemd nativo; container test se PARARÁ, no se promueve).
- **NO** reorganizar `deploy/`.
- **NO** crear `deploy/{runtime,infrastructure,config,health}/` (sin evidencia nueva + decisión formal = rechazado).
- **NO** crear units nuevas para Trading/Portfolio (NIVEL 2, ADR-0024).
- **NO** modificar CD/GHCR/SSH sin ADR-C.
- **NO** eliminar GHCR/SSH sin ADR.
- **NO** crear God Script (`install.sh` delegado; §9).
- **NO** duplicar Hydra/configuración (install.sh solo create-if-missing + valida).
- **NO** introducir `REDIS_URL` compuesta (se mantienen `REDIS_*` discretos, audit §9/§41).
- **NO** cambiar arquitectura interna de Market Data sin necesidad.
- **NO** hacer cleanup no relacionado.
- **NO** tocar los 8 archivos ` M ` pre-existentes del working tree.
- **NO** marcar B-59 verificado sin prueba real de restart.
- **NO** declarar Deploy/Market Data Done sin evidencia de §15/§16.
- **NO** ejecutar ningún commit/push/merge durante esta fase read-only.

---

## 20. Evidence

| Afirmación | Evidencia |
|---|---|
| **[project.scripts]** SSOT (5 entrypoints) | `pyproject.toml:176-181`; `run.sh:31-41` |
| market-data entrypoint /health /ready /ohlcv / uvicorn | `packages/market_data/main.py:58-73,145-150,465-519,622-632`; config `config/market_data/{external_ingestion,feeds}.yaml` |
| Bootstrap standalone fail-fast | `main.py:183-184` → `load_appconfig_standalone` (`ocm/config/hydra_loader.py`) |
| Units instaladas + estado | `/etc/systemd/system/ocm-{market-data,streaming}.service`; `systemctl is-enabled/is-active` → md: disabled/inactive, st: enabled/active (verificado esta misión) |
| installed == rendered byte-igual | `diff deploy/systemd/rendered/* /etc/systemd/system/*` → IDENTICAL (verificado esta misión) |
| **Drift template↔SO vivo (resuelto §3)** | template `EnvironmentFile=…/deploy/host.env` SOLO (`ocm-market-data.service.template:11`, `ocm-streaming.service.template:11`) vs rendered/installed con `.env` adicional `:12`; re-render empírico (envsubst) SIN `.env`; mtime rendered 5-sep; git log templates sin cambios; `systemctl show -p EnvironmentFiles` confirma `host.env` y `.env` activos |
| drift-check inexistente + sin enable/start | `deploy/scripts/install_systemd.sh:69-72,74-86` (solo verify/install/daemon-reload) |
| G4 roto (path) | `scripts/check_production_gates.py:251` vs real `deploy/scripts/install_systemd.sh`; evidencia gate en `:102`; `ls scripts/` |
| G5/G6/G8 usa docker exec (6 de 7 consumidores) | `check_production_gates.py:268-304` (G5), `:332-356` (G6), `:376-411` (G8); `deploy/scripts/health_check.sh:36-44,47-64` |
| `deploy_ocm.sh` wait_for_health duplica L2 | `deploy_ocm.sh:169-200` (docker exec redis/kafka) |
| **D5 host.env trackeado** | `git ls-files deploy/host.env`; `.gitignore:74`; header "NUNCA commitear"; commit a2431d1f (antes del ignore 4354ea0b); `git check-ignore` no reporta (archivo tracked) |
| D6/F-10 puertos invertidos | `.env.example:33` ALERT=9093 / `:40` KAFKA=9094 + nota F-029/B-45 vs `.env` real (ALERT=9094 / KAFKA=9093); compose defaults `…:-9093` `:197,:521`; `docker port ocm_kafka`→9093; `ss -ltnp` solo 9093; host.env KAFKA_BOOTSTRAP=localhost:9093 |
| D7/F-9 default Kafka 3 sitios | `config/base.yaml:46` (localhost:9092), `config/env/production.yaml:43` (kafka:9092), `ocm/config/schema.py:571` (kafka:9092) |
| B-59 NO_VERIFICADO | `docs/plans/tracking.yaml:3146-3158,3381`; ADR-0022 `:323` ("Sin unit systemd activo" — obsoleto) |
| B-57 HECHO vs CD real inoperante | `tracking.yaml:3042-3098` vs `ocm-cd.yml` (workflow_dispatch only, runner ubuntu-latest) y `deploy_ocm.sh:161` (selector inválido), `:49-52` puertos muertos; ADR-0037 estado **Propuesto** con contexto obsoleto (`:3`, `:21`) |
| `.venv` libs para health por contrato | `aiokafka 0.14.0` + `redis 7.4.0` (verificado imports `.venv`); host sin CLIs nativas (audit §37) |
| Instalador/orquestador inexistente | `ls deploy/scripts/` → solo install_systemd, health_check, deploy_ocm |
| Semilla gates | `scripts/check_production_gates.py:75-158` (GATES G1–G11), `:166-266` (helpers) |
| Composition/SSOT config | `ocm/config/env_vars.py` (SSOT env names); `load_appconfig_standalone` fail-fast |
| Gobernanza deploy débil | `GOVERNANCE.md` sin menciones deploy/systemd; host.env policy violada |
| Working tree (baseline) | `git status --short`: 8 ` M ` (`ocm-cd.yml`, `Dockerfile`, monitoring×2, `deploy_ocm.sh`, anomaly_registry, semgrep, uv.lock) + `?? docs/audit/` + `?? docs/audits/…md` — **sin cambios por esta misión** |
| Comandos de esta misión (todos read-only) | `cat/diff/systemctl is-*/git status/ls-files/log/check-ignore/grep/envsubst-en-memoria/docker port/ss/import venv` — sin efectos |

---

**Nota final:** Este plan NO ejecuta nada. Autoriza (una por una, con evidencia §20, validación §12–14, rollback y DOd §15–16) los commits §11, respetando gobernanza §6 y orden §5. DETENERSE al terminar.