# ADR-0060: CD por systemd — identidad commit + uv.lock, gate de salud y rollback (reemplaza ADR-0037)

**Estado:** Propuesto
**Fecha:** 2026-11-12
**Bounded context(s) afectado(s):** ocm

## Contexto

- ADR-0037 (Propuesto) define el CD sobre una imagen Docker (`docker build`, `artifact.sha256` de `docker save`, `docker compose up -d` con tag SHA). ADR-0039 (Aceptado) fija systemd como runtime canónico de las aplicaciones y Docker/Compose como infraestructura, y deja el CD pendiente de un "ADR-C" (§6).
- El `deploy_ocm.sh` original (B-57, cerrado 2026-08-24) no podía desplegar: ejecutaba `docker compose up -d --build <SHA>` (el SHA se interpreta como nombre de servicio); definía `ROOT_DIR` como `deploy/` en lugar de la raíz del repo, por lo que `health_check.sh` no se encontraba y el health post-deploy se omitía; aceptaba el deploy con salud DEGRADED/DOWN; el modo ROLLBACK no leía el SHA previo; la verificación de identidad fallaba abierto; y los puertos de salud copiados de ADR-0037 no coinciden con `docker-compose.yml` (Prometheus 9090, Pushgateway 9091, Alertmanager 9093 interno).
- `.github/workflows/ocm-cd.yml` usa `runs-on: ubuntu-latest` (sin acceso a las units systemd del host), exige `artifact_sha` también para ROLLBACK y busca `deploy_evidence_<sha>.json` en el workspace del runner.

## Alternativas evaluadas

1. Mantener el CD sobre imagen Docker (ADR-0037) — contradice ADR-0039 y exige dockerizar las aplicaciones (precondiciones de dockerización de ADR-0039 §5 sin cumplir).
2. CD por systemd: checkout del commit + `uv sync --frozen` + reinicio de units + gate de salud (elegida) — sin infraestructura nueva.
3. Deploy manual documentado — sin gate de salud, rollback ni evidencia verificable.

## Decisión

- El CD despliega por **systemd** (ADR-0039); no toca Docker/Compose.
- **Identidad del artifact** = SHA completo del commit + sha256 de `uv.lock` de ese commit. Controles opcionales que fallan cerrado: `OCM_EXPECTED_LOCK_SHA256`, `OCM_REQUIRE_SIGNED=1`.
- **Secuencia** (`deploy/scripts/deploy_ocm.sh`): verificar identidad → exigir árbol limpio → seleccionar units (solo activas o habilitadas, con `WorkingDirectory` igual al repo objetivo `OCM_REPO_ROOT`) → `git checkout --detach` + `uv sync --frozen` + reinicio por unit → esperar salud con `health_check.sh` → ACCEPT, o rollback automático al SHA previo → evidencia.
- **Criterio de aceptación:** `MARKET_DATA_HEALTHY=HEALTHY` dentro de `OCM_HEALTH_TIMEOUT_S`; DEGRADED solo con `OCM_ACCEPT_DEGRADED=1`. La observabilidad (Prometheus, Pushgateway, Alertmanager, Grafana) se verifica y registra; bloquea solo con `OCM_REQUIRE_OBS=1`.
- **Estado y evidencia fuera del repo** (`${OCM_STATE_DIR:-~/.local/state/ocm/deploy}`): `current_sha`, `prev_sha`, log y `evidence/deploy_<ts>_<sha12>.json` (modo 0444, escritura atómica).
- **Códigos de salida:** 0 aceptado | 1 falló y se revirtió | 2 falló y no se pudo revertir | 3 precondición.
- **Transporte (propuesto):** `ocm-cd.yml` en un runner self-hosted del host que corre las units, solo `workflow_dispatch`, con `environment` protegido y `concurrency`; el workflow usa el exit code del script y la evidencia del state dir. Sin runner en el host, el despliegue es `./deploy/scripts/deploy_ocm.sh ACCEPT <sha>` manual por SSH, con la misma evidencia.

## Justificación técnica

- Coherente con ADR-0039 y ADR-0040: systemd es el runtime, Docker es infraestructura, el repo no guarda estado de host.
- Fallar cerrado ante identidad no verificable, árbol sucio, units inexistentes o `WorkingDirectory` distinto evita falsos ACCEPT.
- El rollback se verifica con el mismo gate de salud, y la evidencia es inmutable y auditable.

## Consecuencias

- Queda más fácil: un único SSOT de runtime, rollback a un SHA conocido con verificación de salud, y evidencia fuera del árbol de trabajo.
- Deuda aceptada:
  - `ocm-cd.yml` debe reescribirse (no es funcional hoy).
  - Sin respaldo de `.env`/compose (el deploy no los modifica); es un desvío del paso 2 de ADR-0037.
  - La identidad no es un digest de imagen: protege contra código o lockfile distintos, no contra un entorno alterado fuera de `uv.lock`.
  - El repo de despliegue queda con HEAD desacoplado.
  - Requiere `sudo -n systemctl restart <unit>` sin contraseña, con regla sudoers por unit, en modo system.
  - ADR-0039 §4 registró Bronze sin parquet y `health_check.sh` marca DEGRADED cuando Bronze está STALE; mientras tanto un deploy solo se acepta con `OCM_ACCEPT_DEGRADED=1`.
- Contratos que lo hacen cumplir: ningún BC-NN; el script (fail-closed) y `ocm-cd.yml`. Prerrequisito: Production Gate (ADR-0033).

## Referencias

- Código: `deploy/scripts/deploy_ocm.sh`, `deploy/scripts/health_check.sh`, `.github/workflows/ocm-cd.yml`
- ADRs relacionados: ADR-0037 (reemplazado), ADR-0039, ADR-0040, ADR-0033
- Hallazgos: B-57 (tracking.yaml)
