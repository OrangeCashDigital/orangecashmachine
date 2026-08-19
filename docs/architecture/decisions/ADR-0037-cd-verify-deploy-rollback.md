# ADR-0037: Artifact digest/signature + CD verify/deploy/rollback

**Estado:** Propuesto
**Fecha:** 2026-08-19
**Bounded context(s) afectado(s):** ocm (plataforma), CI/CD, OrangeHouse operations

## Contexto

La OCM Constitution (Policy Layer) propone: **Policy Gate → CI → Artifact Build + SHA/Digest → CD Gate (verify/deploy/rollback) → OrangeHouse health**.

**Estado real (auditoría F-PLC-10, F-PLA-05, Plan Maestro):**
- `ocm-cd.yml` = **placeholder deshabilitado** (`workflow_dispatch` only)
- `deploy_ocm.sh` = **inexistente**
- `deploy` = manual SSH
- `rollback` = no automatizado
- Artifact SHA/digest = **NO existe**
- CD Gate = **NO EXISTE**

**Evidencia real:**
- `.github/workflows/ocm-cd.yml`: solo `workflow_dispatch`, sin jobs reales
- `ls scripts/` → 8 scripts: `app_layer_guard.py`, `audit_validator.py`, `backtest_app_guard.py`, `check_ssot_enums.py`, `domain_subprocess_guard.py`, `engineering_health_check.py`, `metrics_report.py`, `provision_kafka_topics.py` — **NO `deploy_ocm.sh`, NO `check_production_gates.py`**
- Docker Compose: `healthchecks` en redis, prometheus (9091), alertmanager (9090), pushgateway (9093) — pero **post-deploy health no automatizado**
- Deploy manual: `docker compose up -d` vía SSH, sin verificación de artifact identity, sin rollback automatizado

**Arquitectura OrangeHouse (single-host, bare-metal/local, Docker Compose, systemd, shell, Git + CI):**
- Sin cloud, sin Kubernetes, sin Terraform
- Infraestructura existente: Docker Compose + systemd + Git + CI
- Deploy target: mismo host que corre CI (self-hosted runner) o host separado vía SSH

## Alternativas evaluadas

1. **Implementar CD completo con artifact registry (GHCR) + digest verification + deploy_ocm.sh + health checks + rollback** — Ventaja: completa la Constitution; artifact identity + deploy verification + rollback automatizado. Desventaja: requiere scripting, pero sin infra nueva.
2. **Mantener deploy manual + documentar** — Ventaja: cero esfuerzo. Desventaja: viola Constitution; sin artifact identity; rollback manual propenso a error; no apto para capital real.
3. **Solo artifact SHA en CI + deploy manual verificado** — Ventaja: mejora trazabilidad. Desventaja: no automatiza verify/deploy/rollback; gap Constitution persiste.

## Decisión

Implementar **secuencia CD completa** con shell + Docker Compose + Git + CI artifacts (sin infraestructura cloud):

### 1. CI Build → Artifact SHA256 (`.github/workflows/ocm-ci.yml` + nuevo `ocm-cd.yml`)
- Build Docker image multi-stage
- `docker build --build-arg GIT_SHA=$(git rev-parse HEAD) -t ocm:$GIT_SHA .`
- `sha256sum <(docker save ocm:$GIT_SHA) > artifact.sha256`
- Subir `artifact.sha256` como CI artifact (inmutable)
- Opcional: push a GHCR/local registry con tag `$GIT_SHA`

### 2. CD Gate Verify (`.github/workflows/ocm-cd.yml` / `deploy_ocm.sh`)
```bash
# deploy_ocm.sh responsabilidades:
1. Verificar digest del artifact (identidad inmutable)
2. Backup .env + docker-compose.yml (snapshot)
3. docker compose pull/up -d con tag SHA
4. Wait-for-health (healthchecks: redis, prometheus:9091, alertmanager:9090, pushgateway:9093, grafana:3000)
5. Health post-deploy (deadman alert, kafka lag, redis memory, disk)
6. Decisión ACCEPT/ROLLBACK con rollback automático a SHA anterior
7. Escribir resultado (timestamp, SHA, health) a evidencia inmutable
```

### 3. Rollback Automatizado
- `docker compose down && docker compose up -d` con SHA anterior (guardado en `deploy_ocm.sh` o CI artifact)
- Solo health checks (NO re-ejecutar Policy Layer — artifact ya validado en build)

### 4. OrangeHouse Health Endpoints (requeridos)
- `redis-cli ping`
- `prometheus:9091/-/healthy`
- `alertmanager:9090/-/healthy`
- `pushgateway:9091/-/healthy` (o 9093)
- `grafana:3000/api/health`
- Kafka lag monitoring (consumer group lag)

## Justificación técnica

- **Sin infraestructura nueva**: todo con shell + Docker Compose + systemd + Git + CI artifacts
- **Completa la Constitution**: Policy Gate → Artifact SHA → CD Verify → Deploy → Health → Accept/Rollback
- **Artifact identity**: SHA256 inmutable evita deploy de versión distinta
- **Rollback seguro**: solo health checks, no re-ejecuta Policy Layer (artifact ya validado)
- **Compatible con OrangeHouse**: single-host, bare-metal, Docker Compose, systemd
- **SSH opcional**: si runner = host, deploy local; si separado, `deploy_ocm.sh` via SSH

## Consecuencias

- **Más fácil:** Release = tag → CI build SHA → CD verify → deploy → health → accept/rollback
- **Deuda aceptada:** `deploy_ocm.sh` y `ocm-cd.yml` nuevos (mantenimiento); health endpoints requieren instrumentación en servicios
- **Contratos que hacen cumplir:** ADR-0033 (Production Gate binario previo), `ocm-cd.yml` job `cd-gate`, `deploy_ocm.sh`
- **Prerequisitos:** G9 (paridad config), G10 (estado posición único), health endpoints en servicios

## Referencias

- Código: `.github/workflows/ocm-cd.yml` (placeholder), `docker-compose.yml` (healthchecks), `scripts/engineering_health_check.py` (patrón)
- Hallazgos: B-57 (tracking.yaml)
- ADRs relacionados: ADR-0020, ADR-0022, ADR-0033
- Auditorías: `AUDIT_OCM_POLICY_LAYER_COMPLEMENTARY_2026-08-19.md` (F-PLC-10), `AUDIT_OCM_POLICY_LAYER_COMPLEMENTARY_2026-08-19.md` (adversarial, F-PLA-05)