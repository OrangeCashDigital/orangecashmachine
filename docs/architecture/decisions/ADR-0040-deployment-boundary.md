# ADR-0040: Deployment boundary — repositorio, installer, host y runtime

**Estado:** Aceptado
**Fecha:** 2026-09-19
**Bounded context(s) afectado(s):** ocm, deployment, portabilidad

## 1. Título

ADR-0040: Deployment boundary — repositorio, installer, host y runtime

## 2. Estado

**Aceptado** — *Decisión aceptada* ≠ *Implementación* ≠ *Commit* ≠ *Despliegue*. No declara `P0/P1/P2` `HECHO`.

## 3. Contexto

`docs/audit/deployment-runtime-implementation-plan.md` frontera difusa `REPOSITORY/HOST/RUNTIME/INFRA`. Hallazgos `D5` `host.env` trackeado pese a `.gitignore:74` + header `NUNCA commitear`, `D1` drift `template≠rendered=installed` `PLAN §3` (`templates:11` solo `host.env` vs `rendered/installed:11-12` `host.env + .env` `72bd1878`), `G4` `scripts/install_systemd.sh` vs `deploy/scripts/...`.

Solo hechos verificables READ-ONLY.

## 4. Problema

1. `deploy/host.env` trackeado `git log a2431d1f` pese a `header NUNCA commitear` (`D5`).
2. `templates:11` solo `host.env` vs `rendered/installed:11-12` `host.env + .env` — `install_systemd.sh:43` `envsubst` lista `^[A-Z_]+=` no generaba `.env`; `rendered` edición manual 5-sep, re-render rompería `REDIS_PASSWORD` `docker-compose.yml:106`.
3. Sin `deploy/scripts/install.sh` (`plan:68` `NO existe`) conocimiento en `README + 3 scripts + gates`.
4. `G4` histórico `scripts/install_systemd.sh` inexistente vs `deploy/scripts/...` (hoy `grep -rn` 0 refs en código).
5. `host.env` no auto-generado en clon limpio — `install_systemd.sh:32` `ERROR: falta host.env (cp host.env.example)`.

## 5. Decisión

```
REPOSITORIO (Git)
  deploy/systemd/templates/*.template  ${OCM_HOST_USER}/${OCM_REPO_ROOT} placeholders :9-12
  deploy/scripts/install_systemd.sh    render envsubst lista explícita host.env :43
  deploy/host.env.example              SSOT versionado
  deploy/monitoring/*.yml              prometheus.yml etc.
  config/                              Hydra base + env
  .env.example                         KAFKA_HOST_PORT, BYBIT placeholders vacíos
      ↓ envsubst + install
DEPLOYMENT / INSTALLER
  install_systemd.sh --verify-only     render + systemd-analyze verify :64
  install_systemd.sh --apply (no existe hoy; actual: --verify-only :64) → render → /etc/systemd/system + daemon-reload
  (futuro) install.sh --check/--dry-run/--apply delegado plan:9
      ↓ source host.env + envsubst
HOST (OrangeHouse, no VCS)
  deploy/host.env                      topología OCM_ENV, OCM_HOST_USER, OCM_REPO_ROOT, KAFKA_BOOTSTRAP_SERVERS=localhost:9093 — untracked C1 04e9b6fd
  .env                                 secretos BYBIT 18/36 chars, REDIS_PASSWORD — ignored :12
      ↓ EnvironmentFile
RUNTIME
  systemd  ocm-streaming/market-data  WorkingDirectory=${OCM_REPO_ROOT} User=${OCM_HOST_USER} EnvironmentFile=.../deploy/host.env + .../.env :11-12 ExecStart=.venv
  Docker/Compose  redis, kafka/zookeeper:473/512, pushgateway:122, prometheus:143 — ocm_internal/public
```

**Reglas:** `REPO` templates parametrizados nunca hardcodean `/home/orangemusic`; `HOST` `host.env` local `cp host.env.example`; `INSTALLER` descubre `REPO_ROOT:14` `HOST_ENV:16` `vars:47`; `RUNTIME` systemd `User/WorkingDirectory/EnvironmentFile`; `INFRA` Docker `KAFKA_LISTENERS INTERNAL:9092 EXTERNAL:9093 :529`.

## 6. Alcance

**Dentro:** `deploy/systemd/`, `install_systemd.sh`, `host.env` boundary, `D1` drift, `D5`, `G4`.
**Fuera:** `deploy/{runtime,...}` nuevos directorios (`plan:8` no reorganizar), `install.sh` God Script, `secrets management` (solo `EnvironmentFile`).

## 7. Consecuencias positivas

- `templates = SSOT` — `rendered/` `gitignored:75` y `/etc` derivados, `diff` 0 tras `72bd1878`.
- `host.env` untracked evita leak; `host.env.example` `<usuario>` portabilidad Debian.
- `G4` `deploy/scripts/install_systemd.sh` habilita `P1`.

## 8. Trade-offs / consecuencias negativas

- `host.env` no auto-generado — clon limpio aborta `ERROR: falta host.env` (gap aceptado).
- `install.sh` delegado aún no existe — `plan:68` `NO existe`.
- `health` hoy `docker exec` 9 sitios `health_check.sh:36` — `P1 D4` `venv` pendiente.

## 9. Relación con portabilidad

Compatible `cualquier Debian`: `templates:9` `User=${OCM_HOST_USER}` variable, `10 WorkingDirectory=${OCM_REPO_ROOT}`, `install_systemd.sh:14 REPO_ROOT` relativo, `host.env` ignored local, `rendered` gitignored. No hardcode VCS.

## 10. Evidencia concreta del repositorio que sustenta la decisión

| Evidencia | Archivo:línea/commit |
|---|---|
| `templates:11-12` 2 `EnvironmentFile` | `deploy/systemd/templates/*.template:11-12` |
| `install_systemd.sh:43` `vars` | `deploy/scripts/install_systemd.sh:43` |
| `rendered==installed` | `cat rendered/ocm-streaming.service` == `cat /etc/...` + `72bd1878 +EnvironmentFile .env` |
| `host.env untracked` | `git ls-files deploy/host.env exit 1` + `.gitignore:74` + `04e9b6fd 26 deletions` |
| `host.env.example` portable | `deploy/host.env.example: M` `<usuario del host>` vs `HEAD: orangemusic` |
| `G4` no rota hoy | `grep -rn install_systemd` 0 `scripts/install_systemd.sh` |
| `microservices OFF` | `docker-compose.yml:24,335` `profiles: [microservices]` |

## 11. Relación con D1

`D1` `72bd1878` ya añadió `EnvironmentFile=${OCM_REPO_ROOT}/.env` (`diff +1` cada). Esta ADR **no ejecuta `D1`** — lo documenta como `templates = SSOT` y autoriza `D1` `drift-check` `P1` antes de `enable --now`. `D1` ya resuelto, `diff 0`.

## 12. Qué NO decide este ADR

- No modifica `host.env`, `.gitignore`, `install_systemd.sh`, `templates` (solo documenta `D1` ya resuelto `72bd1878`).
- No resuelve `D5/G4` con `git rm --cached` / fix path (referencia `D5` evidencia, no ejecuta `C1` ya `04e9b6fd`).
- No crea `CredentialResolver/SecretManager/ports/adapters/bounded contexts`.
- No autoriza `P0/P1/P2` `HECHO`, `B-59` `tracking HECHO`, `A1`, `docker compose up/down`, `systemctl restart`.
- No re-ordena `deploy/` directorios.

## 13. Estado de implementación

**Aceptado** ≠ **Implementación** (`install.sh` no existe, `P1 D1` pendiente) ≠ **Commit** (propuesta no escrita antes de esta autorización) ≠ **Despliegue** (`daemon-reload` pendiente `P2`). No autoriza `P0/P1/P2`.

## CAMBIOS QUE ESTE ADR NO AUTORIZA

`P0` (`D5` ya, `D6` puertos, `D7` `KAFKA` default, `G4`), `P1` `install.sh`/`health contrato`, `P2` `Market Data systemd` `C12/B2/B3`, `P3` robustez, `ADR-C` CD, `Vertical Slice` `StrategyCandidate`, `Paper/Live`, `A1` `BYBIT` `environment:` `docker-compose.yml:347`, `B-59` `tracking HECHO`, `docker compose up/down`, `systemctl start/stop/restart/enable/disable/daemon-reload`, `modificación .env/host.env/Dockerfile/Compose/Kafka/código`, `commit/push/merge/rebase/reset/clean`, nuevos `bounded contexts/ports/adapters/brokers`.
