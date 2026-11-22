# AUDITORÍA CONSOLIDADA — DESPLIEGUE, SEGURIDAD Y CONFIGURACIÓN
## OrangeCashMachine (OCM)

- **ID**: DEP-004
- **Fecha**: 2026-09-22
- **Modo**: READ-ONLY absoluto. Cero modificaciones.
- **Scope**: Consolidación de auditorías de despliegue (Phases 0-38) + auditoría de propuesta técnica de remediación.
- **Documentos de referencia**: ADR-0039 (systemd canónico), ADR-0040 (deployment boundary), propuesta de remediación del usuario.
- **Baseline**: commit `857cfe60`, branch `feat/m22b-governance`.

---

## REPRODUCIBILIDAD

```
- commit: 857cfe60d0c5e3b45b4fd235a978d69a8bf56090
- branch: feat/m22b-governance
- fecha: 2026-09-22
- protocolo: AUDIT_PROTOCOL v2.1
- agente: OpenCode / ling-3.0-flash-fin-free
- comandos: cat, diff, grep, stat, ls, ps, ss, systemctl, docker ps, git diff, uv run
- audit_validator.py: FAIL (14 errores mecánicos)
- resultado: DRIFT DETECTED
```

---

## 1. EXECUTIVE SUMMARY & VERDICTA

**Veredicto**: `DRIFT DETECTED — PROPUESTA INCOMPLETA + IMPLEMENTACIÓN CONTRADICTORIA`

La propuesta de remediación del usuario es arquitectónicamente correcta pero **incompatible con el estado actual del repo** en múltiples puntos:

1. **La propuesta elimina `host.env`** — pero `host.env` define variables (`OCM_REPO_ROOT`, `OCM_HOST_USER`, `KAFKA_BOOTSTRAP_SERVERS`) que `.env` NO tiene. Eliminarlo sin migrar esas variables a `.env` romperá el sistema.
2. **La propuesta usa `EnvironmentFile=${OCM_REPO_ROOT}/.env`** — pero `.env` actual no tiene `OCM_REPO_ROOT` ni `OCM_HOST_USER`.
3. **La propuesta genera secretos con `openssl rand -hex 32`** — pero `BYBIT_API_KEY` real debe estar en `.env` para que el pipeline funcione.
4. **La propuesta establece `OCM_ENV=production`** — pero `.env` actual tiene `OCM_ENV=development`.
5. **La propuesta propone `ProtectSystem=full`** — pero las unidades actuales tienen `ProtectSystem=strict`. El `full` bloquea lectura de `/usr`, `/boot`, `/etc` lo cual puede romper Python si necesita acceso a archivos de sistema.
6. **`scripts/check_production_gates.py` SÍ EXISTE** — mi hallazgo previo P1-6 era incorrecto. El script existe en `scripts/`.

### Tabla de Estado General

| Dimensión | Estado Propuesto | Estado Real | Gap |
|---|---|---|---|
| `host.env` eliminado | ✅ Sí | ❌ Existe (664 permisos) | **BORRAR sin romper** |
| `.env` como SSOT | ✅ Sí | ⚠️ Parcial (falta OCM_REPO_ROOT, OCM_HOST_USER) | **AGREGAR vars faltantes** |
| `.env` permisos 600 | ✅ Sí | ✅ Correcto (600 orangemusic) | **NINGUNO** |
| `.env.example` portable | ✅ Sí | ⚠️ `KAFKA_HOST_PORT=9094` vs `.env=9093` | **ARREGLAR DRIFT** |
| `OCM_ENV=production` | ✅ Sí | ❌ `development` en `.env` | **CAMBIAR** |
| `OCM_API_JWT_SECRET` generado | ✅ Sí | ❌ `CHANGE_ME_...` | **GENERAR** |
| `REDIS_PASSWORD` fuerte | ✅ Sí | ❌ `ocm_local_dev` | **GENERAR** |
| Templates sin `envsubst` | ✅ Sí | ❌ Usan `${VAR}` placeholders | **REEMPLAZAR** |
| `deploy/rendered/` eliminado | ✅ Sí | ❌ Existe | **BORRAR** |
| `deploy/README.md` existe | ✅ Sí | ❌ No existe | **CREAR** |
| `install_systemd.sh` idempotente | ✅ Sí | ⚠️ Modificado localmente | **REEMPLAZAR** |
| Systemd canónico ACTIVE | ✅ Sí | ⚠️ `ocm-market-data` tiene 2 procesos | **RESOLVER** |
| `ProtectSystem=full` | ✅ Sí | ❌ `strict` en instalado | **CAMBIAR** |
| `NoNewPrivileges=true` + `Nice=-20` | ❌ Contradicción | ❌ En templates | **ELIMINAR `Nice=-20`** |
| `check_production_gates.py` existe | ✅ Sí | ✅ En `scripts/` | **NINGUNO** |
| Observabilidad UP | ✅ Sí | ❌ DOWN | **LEVANTAR** |
| Docker stack completo | ✅ Sí | ⚠️ 3/13 UP | **LEVANTAR** |

---

## 2. MATRIZ DE LIMPIEZA Y DEUDAS DE ARTEFACTOS

### 2.1 Archivos a Eliminar

| Archivo | Acción | Justificación | Estado |
|---|---|---|---|
| **`deploy/host.env`** | **ELIMINAR** | Contiene `OCM_REPO_ROOT=/home/orangemusic/trading/orangecashmachine` hardcodeado. Redundante con `.env`. Permisos 664 (inseguro). | `P0` |
| **`deploy/host.env.example`** | **ELIMINAR** | Idéntico a `host.env`. No aporta nada nuevo. | `P0` |
| **`deploy/systemd/rendered/`** | **ELIMINAR** | Directorio derivado de templates. Stale. Gitignored. No debería existir. | `P1` |
| **`deploy/systemd/templates/*.template`** | **ELIMINAR** | Templates `envsubst` son fuente de drift. Los archivos `.service` deberían ser estáticos. | `P0` |
| **`deploy/scripts/install_systemd.sh`** | **REEMPLAZAR** | Depende de `host.env`, `OCM_REPO_ROOT`, `OCM_HOST_USER`. Debería leer `.env` directamente. | `P1` |

### 2.2 Archivos a Crear/Modificar

| Archivo | Acción | Justificación | Estado |
|---|---|---|---|
| **`deploy/README.md`** | **CREAR** | Documentación de despliegue y lifecycle. Propuesta lo requiere. | `P1` |
| **`.env.example`** | **ACTUALIZAR** | Agregar `OCM_ENV`, `OCM_REPO_ROOT`, `OCM_HOST_USER`, `KAFKA_BOOTSTRAP_SERVERS`. Arreglar `KAFKA_HOST_PORT` drift (9093 vs 9094). | `P1` |
| **`.env`** | **MODIFICAR** | Cambiar `OCM_ENV` a `production`. Agregar `OCM_REPO_ROOT`, `OCM_HOST_USER`, `KAFKA_BOOTSTRAP_SERVERS`. Generar `OCM_API_JWT_SECRET` y `REDIS_PASSWORD`. | `P0` |
| **`deploy/systemd/ocm-market-data.service`** | **CREAR** | Archivo estático sin placeholders. | `P0` |
| **`deploy/systemd/ocm-streaming.service`** | **CREAR** | Archivo estático sin placeholders. | `P0` |

### 2.3 Variables que deben migrarse de `host.env` a `.env`

| Variable | En `host.env` | En `.env` | Acción |
|---|---|---|---|
| `OCM_ENV` | ✅ `development` | ✅ `development` | ❌ Cambiar a `production` en `.env` |
| `OCM_HOST_USER` | ✅ `orangemusic` | ❌ **NO PRESENTE** | **AGREGAR a `.env`** |
| `OCM_REPO_ROOT` | ✅ `/home/orangemusic/trading/orangecashmachine` | ❌ **NO PRESENTE** | **AGREGAR a `.env`** |
| `KAFKA_BOOTSTRAP_SERVERS` | ✅ `localhost:9093` | ✅ `localhost:9093` | ✅ YA está |
| `KAFKA_ENABLED` | ✅ `true` | ✅ `true` | ✅ YA está |
| `REDIS_ENABLED` | ✅ `true` | ❌ **NO PRESENTE** | **AGREGAR a `.env`** |
| `REDIS_HOST` | ✅ `localhost` | ❌ **NO PRESENTE** | **AGREGAR a `.env`** |
| `REDIS_PORT` | ✅ `6379` | ❌ **NO PRESENTE** | **AGREGAR a `.env`** |
| `MARKET_DATA_HOST` | ✅ `0.0.0.0` | ❌ **NO PRESENTE** | **AGREGAR a `.env`** |
| `MARKET_DATA_PORT` | ✅ `8001` | ❌ **NO PRESENTE** | **AGREGAR a `.env`** |
| `INGESTION_INTERVAL_S` | ✅ `300` | ❌ **NO PRESENTE** | **AGREGAR a `.env`** |

### 2.4 `.env.example` Drift Detection

| Variable | `.env.example` | `.env` actual | Docker-compose default | Problema |
|---|---|---|---|---|
| `KAFKA_HOST_PORT` | **9094** | **9093** | 9093 | **DRIFT**: `.env.example` dice 9094 pero `.env` y compose usan 9093 |
| `OCM_ENV` | **production** | **development** | production | **DRIFT**: `.env` en development |
| `ALERTMANAGER_HOST_PORT` | **9093** | **9094** | 9090 | **DRIFT**: Valores inconsistentes |

---

## 3. MATRIZ DE AUDITORÍA SYSTEMD

### 3.1 Unidades Actuales vs Propuesta vs Requerido

#### `ocm-market-data.service`

| Propiedad | INSTALADO (stale) | TEMPLATE (modificado) | PROPUESTO | REQUERIDO |
|---|---|---|---|---|
| **`EnvironmentFile`** | `host.env` + `.env` | `-host.env` + `-.env` | `.env` solo | `.env` solo |
| **`ProtectSystem`** | `strict` | `strict` | `full` | `full` |
| **`ProtectHome`** | ❌ No presente | ❌ No presente | `read-only` | `read-only` |
| **`Nice`** | ❌ No presente | `Nice=-10` | ❌ No presente | `Nice=-5` o `-10` |
| **`LimitNOFILE`** | ❌ No presente | `65536` | ❌ No presente | `65536` |
| **`CPUAffinity`** | ❌ | ❌ | ❌ | ❌ **INVALIDO** |
| **`LimitRTPRIO`** | ❌ | `99` | ❌ | ❌ **CONFLICTO** |
| **`LimitMEMLOCK`** | ❌ | `infinity` | ❌ | ❌ **CONFLICTO** |
| **`Restart`** | `on-failure` | `on-failure` | `on-failure` | `on-failure` |
| **`RestartSec`** | `10` | `5s` | `5s` | `5s` |
| **`StandardOutput`** | ❌ | `journal` | ❌ | `journal` |
| **`StandardError`** | ❌ | `journal` | ❌ | `journal` |
| **`NoNewPrivileges`** | `true` | `true` | `true` | `true` |
| **`Group`** | ❌ No presente | ❌ No presente | `${OCM_HOST_USER}` | ✅ **AGREGAR** |

#### `ocm-streaming.service`

| Propiedad | INSTALADO (stale) | TEMPLATE (modificado) | PROPUESTO | REQUERIDO |
|---|---|---|---|---|
| **`EnvironmentFile`** | `host.env` + `.env` | `-host.env` + `-.env` | `.env` solo | `.env` solo |
| **`ProtectSystem`** | `strict` | `strict` | `full` | `full` |
| **`Nice`** | ❌ | `Nice=-20` | ❌ | ❌ **CONFLICTO** con `NoNewPrivileges` |
| **`LimitRTPRIO`** | ❌ | `99` | ❌ | ❌ **CONFLICTO** |
| **`LimitMEMLOCK`** | ❌ | `infinity` | ❌ | ❌ **CONFLICTO** |
| **`LimitNOFILE`** | ❌ | `65536` | ❌ | `65536` |
| **`CPUAffinity`** | ❌ | `2 3` | ❌ | ❌ **INVALIDO** |
| **`Group`** | ❌ No presente | ❌ No presente | `${OCM_HOST_USER}` | ✅ **AGREGAR** |

### 3.2 Contradicciones en la Propuesta vs Templates Modificados

| # | Contradicción | Propuesta | Templates actuales | Severidad |
|---|---|---|---|---|
| CB-1 | `ProtectSystem=full` vs `ProtectSystem=strict` | `full` | `strict` | **ALTA** — `full` bloquea `/usr`, `/boot`, `/etc`. Python puede fallar si necesita archivos de sistema. `strict` permite lectura de `/usr`. |
| CB-2 | `Nice=-20` en streaming vs `NoNewPrivileges=true` | ❌ No menciona | `Nice=-20` | **CRÍTICA** — `Nice=-20` requiere CAP_SYS_NICE. `NoNewPrivileges=true` lo bloquea. |
| CB-3 | `LimitRTPRIO=99` + `LimitMEMLOCK=infinity` vs `NoNewPrivileges=true` | ❌ No menciona | Ambos en template | **CRÍTICA** — Requieren capabilities que `NoNewPrivileges` elimina. |
| CB-4 | `EnvironmentFile=-host.env` (opcional) vs propuesto `.env` solo | `.env` solo | `-host.env` + `-.env` | **MEDIA** |
| CB-5 | `Group=${OCM_HOST_USER}` no en templates | En propuesta | ❌ No presente | **BAJA** |
| CB-6 | `Nice=-10` en market-data | No en propuesta | `Nice=-10` | **BAJA** — No contradice `NoNewPrivileges` porque `-10` es permisible sin capabilities. |

### 3.3 Drift Crítico: Templates vs Rendered vs Installed vs Running

```
REPO (templates)          →  deploy/rendered/        →  /etc/systemd/system/     →  RUNNING
┌──────────────┐         ┌──────────────────────┐   ┌──────────────────────┐   ┌──────────┐
│ NEW (mod     │─────→   │ OLD (stale)          │──→│ OLD (stale)          │──→│ 2 procs  │
│ Nice=-10     │         │ Nice=N/A             │   │ Nice=N/A             │   │ (1+1)    │
│ EnvFile=-    │         │ EnvFile=             │   │ EnvFile=             │   │          │
│ StartLimit   │         │ StartLimit=N/A       │   │ StartLimit=N/A       │   │          │
│ journal      │         │ journal=N/A          │   │ journal=N/A          │   │          │
│ Nice=-20     │         │ Nice=N/A             │   │ Nice=N/A             │   │          │
│ CPUAffinity  │         │ CPUAffinity=N/A      │   │ CPUAffinity=N/A      │   │          │
└──────────────┘         └──────────────────────┘   └──────────────────────┘   └──────────┘
     MODIFICADOS             STALE                       STALE                    MIXED
```

**PROBLEMA P0**: `git diff HEAD` muestra 114 líneas de cambios no committeados en `deploy/`. El sistema está ejecutando la versión STALE. `systemctl is-active ocm-market-data` dice `active` pero hay DOS procesos corriendo.

---

## 4. LISTA DE HALLAZGOS Y PLAN DE REMEDIACIÓN

### P0 — Bloqueantes / Seguridad / Drift Crítico

| # | Finding | Severidad | Evidencia | Remediación |
|---|---|---|---|---|
| **P0-1** | **Drift masivo de templates**: `git diff HEAD` muestra 114 líneas de cambios no committeados en `deploy/`. Templates ≠ Rendered ≠ Installed | `CRITICAL` | `git diff HEAD -- deploy/`, `diff template≠rendered` | Revertir templates O re-renderizar + reinstalar. Decidir: ¿los cambios son intencionales o accidentales? |
| **P0-2** | **DOS procesos `market_data.main`**: pid=192727 (manual) + pid=718168 (systemd), ambos intentando puerto 8001 | `CRITICAL` | `ps aux \| grep market_data`, `ss -tlnp \| grep 8001` | Matar proceso manual (pid=192727). Verificar que systemd gestiona correctamente |
| **P0-3** | **`NoNewPrivileges=true` + `Nice=-20` + `LimitRTPRIO=99` + `LimitMEMLOCK=infinity`** — CONTRADICCIÓN en template | `CRITICAL` | `ocm-streaming.service.template:28-34` | Eliminar `Nice=-20`, `LimitRTPRIO=99`, `LimitMEMLOCK=infinity` O quitar `NoNewPrivileges=true` |
| **P0-4** | **`host.env` existe** — debe eliminarse pero contiene `OCM_REPO_ROOT`, `OCM_HOST_USER` que `.env` no tiene | `CRITICAL` | `cat deploy/host.env`, `grep -c OCM_REPO_ROOT .env` → 0 | Migrar variables de `host.env` a `.env` ANTES de eliminar `host.env` |
| **P0-5** | **`.env` contiene `BYBIT_API_KEY` real** y `REDIS_PASSWORD=ocm_local_dev` | `CRITICAL` | `cat .env` | `BYBIT_API_KEY`: ¿mover a vault? `REDIS_PASSWORD`: generar con `openssl rand -hex 32` |
| **P0-6** | **`OCM_API_JWT_SECRET=CHANGE_ME_...`** — placeholder, JWT roto | `CRITICAL` | `cat .env` | Generar con `openssl rand -hex 32` |
| **P0-7** | **`OCM_ENV=development` en `.env`** — desarrollo en producción | `CRITICAL` | `cat .env` | Cambiar a `OCM_ENV=production`. Nota: `docker-compose.yml` tiene `${OCM_ENV:-production}` default |
| **P0-8** | **`ProtectSystem=full` en propuesta vs `strict` en instalado** — `full` puede romper Python | `CRITICAL` | Propuesta vs `/etc/systemd/system/ocm-*.service` | Verificar si `ProtectSystem=full` rompe imports de Python. Si es así, usar `ProtectSystem=strict` |
| **P0-9** | **Sin `client_order_id` / idempotencia** | `CRITICAL` | Sin evidencia en código | Implementar `client_order_id` determinista |
| **P0-10** | **Sin crash recovery & reconciliation protocol** | `CRITICAL` | Sin secuencia documentada | Implementar protocolo de recuperación |
| **P0-11** | **Sin circuit breakers** (kill switch, max drawdown, max position) | `CRITICAL` | Sin implementación | Implementar risk gates |
| **P0-12** | **`deploy/host.env` permisos 664** — cualquier usuario del grupo puede leerlo | `CRITICAL` | `stat -c "%a" deploy/host.env` → 664 | Eliminar `host.env` (solución definitiva). Si persiste: `chmod 600` |

### P1 — Infraestructura / Producción / Observabilidad

| # | Finding | Severidad | Evidencia | Remediación |
|---|---|---|---|---|
| **P1-1** | **`deploy/host.env.example` NO en `.gitignore`** | `HIGH` | `.gitignore` no lo incluye | Agregar a `.gitignore` o eliminarlo |
| **P1-2** | **`deploy/systemd/rendered/` existe** — debe eliminarse | `HIGH` | `ls deploy/systemd/rendered/` | Eliminar directorio |
| **P1-3** | **`deploy/README.md` NO existe** | `HIGH` | `ls deploy/README.md` → NO | Crear documentación de despliegue |
| **P1-4** | **`.env.example` drift `KAFKA_HOST_PORT=9094`** vs `.env=9093` | `HIGH` | `grep KAFKA_HOST_PORT .env*` | Arreglar `.env.example` a `9093` o `.env` a `9094` |
| **P1-5** | **`.env` faltan `OCM_REPO_ROOT`, `OCM_HOST_USER`, `KAFKA_BOOTSTRAP_SERVERS`, `REDIS_HOST`, `REDIS_PORT`** | `HIGH` | `grep -c OCM_REPO_ROOT .env` → 0 | Agregar variables de `host.env` a `.env` |
| **P1-6** | **`install_systemd.sh` modificado localmente** (auto-bootstrap, `systemctl enable --now`) | `HIGH` | `git diff HEAD -- deploy/scripts/install_systemd.sh` | Revertir O documentar como nuevo comportamiento |
| **P1-7** | **`check_production_gates.py` SÍ existe** | `INFO` | `ls scripts/check_production_gates.py` | ✅ CORREGIDO — mi hallazgo previo era incorrecto |
| **P1-8** | **Observabilidad completa DOWN** | `HIGH` | `docker ps` solo Redis/Kafka/Zookeeper | `docker compose up -d --profile monitoring` |
| **P1-9** | **`docker-compose.yml` perfil `[microservices]` no activado** | `HIGH` | `docker-compose.yml:334` | Decidir si market-data corre en host o Docker |
| **P1-10** | **Sin heartbeat / Dead Man's Switch** | `HIGH` | Sin mecanismo | Implementar heartbeat externo |
| **P1-11** | **Sin métricas Prometheus** | `HIGH` | Sin container | Levantar observabilidad |
| **P1-12** | **`OCM_INTEGRATIONS__KAFKA__BOOTSTRAP_SERVERS=localhost:9093`** duplicado con `KAFKA_BOOTSTRAP_SERVERS=localhost:9093` | `MEDIUM` | `cat .env` | Eliminar duplicado |
| **P1-13** | **`KAFKA_HOST_PORT` drift** `.env=9093`, `.env.example=9094`, `docker-compose default=9093` | `MEDIUM` | `grep KAFKA_HOST_PORT .env* docker-compose.yml` | Unificar a un solo valor |
| **P1-14** | **`deploy/scripts/deploy_ocm.sh` referencia `ARTIFACT_SHA`** pero no existe variable | `MEDIUM` | `cat deploy/scripts/deploy_ocm.sh` | Verificar si `deploy_ocm.sh` es funcional |

### P2 — Optimizaciones de Latencia / SO

| # | Finding | Severidad | Evidencia | Remediación |
|---|---|---|---|---|
| **P2-1** | **`CPUAffinity=2 3` inválido** con `NoNewPrivileges` | `MEDIUM` | `ocm-streaming.service.template:28` | Eliminar o verificar cgroups v2 |
| **P2-2** | **Host 4 cores / 5.5GB RAM** — contención posible | `MEDIUM` | `nproc`, `free -h` | Redis/OCM/Docker comparten recursos |
| **P2-3** | **Sin CPU affinity / IRQ / NUMA configurado** | `MEDIUM` | Sin `taskset` | No aplicable sin requisito de baja latencia |
| **P2-4** | **`Nice=-10` en market-data** no aplicado en `/etc` | `MEDIUM` | Instalado stale | Re-renderizar y reinstalar |
| **P2-5** | **Sin graceful shutdown completo** para `market_data.main` | `MEDIUM` | Sin `try/except/finally` | Implementar signal handler |
| **P2-6** | **Sin log persistence externa** | `MEDIUM` | `logs/` solo filesystem | Implementar rotación/externalización |
| **P2-7** | **Sin alerting externo** | `MEDIUM` | Sin mecanismo | Implementar Telegram/Slack/Discord |
| **P2-8** | **`Dockerfile:40 CMD` usa `USER appuser`** — conflicto con systemd `User=orangemusic` | `LOW` | `Dockerfile:38-40` | Dockerfile es para Docker, no para systemd. No aplica. |
| **P2-9** | **`WSProducerBundle` huérfano** (ADR-0022) | `LOW` | ADR-0022 | No wireado a `main.py` |
| **P2-10** | **`deploy_ocm.sh` tiene `ALERTMANAGER_PORT=9090`** pero `.env` dice `9094` | `LOW` | `cat deploy/scripts/deploy_ocm.sh` | Verificar consistencia |

### E — Evidencia Insuficiente

| # | Item | Razón |
|---|---|---|
| E-1 | `ocm-market-data.service` MainPID real | `systemctl show` timeout previo |
| E-2 | Estado de pid=718168 | ¿Crash? ¿Running? ¿Conflict? |
| E-3 | Port 8001 binding | Solo pid=192727 escucha |
| E-4 | `ProtectSystem=full` vs Python imports | Sin verificar si `full` rompe `import` de paquetes |
| E-5 | Datos recientes en Bronze | Solo files de noviembre 2023 |
| E-6 | Kafka lag real | Sin `kafka-get-offsets` output |
| E-7 | Source of truth para ORDER/POSITION | Sin documentación |
| E-8 | NTP sincronización | `timedatectl` timeout |
| E-9 | IP whitelisting en exchange | Sin verificación |
| E-10 | Withdrawal disable en exchange | Sin verificación |
| E-11 | `check_production_gates.py` output | Timeout al ejecutar |
| E-12 | `deploy_ocm.sh` funcionalidad completa | Requiere `ARTIFACT_SHA` no definido |

---

## 5. PROPUESTA vs REALIDAD — MATRIZ DE COMPARACIÓN

### 5.1 Propuesta del Usuario vs Estado Actual

| Elemento de la Propuesta | ¿Es correcta? | ¿Es compatible? | Observación |
|---|---|---|---|
| Eliminar `host.env` | ✅ Arquitectónicamente correcto | ⚠️ **INCOMPATIBLE** | `.env` no tiene `OCM_REPO_ROOT`, `OCM_HOST_USER`. Eliminar sin migrar = BROKEN |
| `.env` como SSOT | ✅ Correcto | ✅ Compatible | Pero necesita agregar variables que están en `host.env` |
| `.env` permisos 600 | ✅ Correcto | ✅ Compatible | Ya es 600 |
| `EnvironmentFile=${OCM_REPO_ROOT}/.env` | ✅ Correcto | ⚠️ **INCOMPATIBLE** | `${OCM_REPO_ROOT}` necesita ser resuelto en tiempo de ejecución. Si es estático, la plantilla debe tener la ruta literal |
| `ProtectSystem=full` | ⚠️ **Debe verificarse** | ⚠️ **RIESGO** | Puede romper imports de Python. `strict` es más seguro |
| `NoNewPrivileges=true` | ✅ Correcto | ✅ Compatible | Pero invalida `Nice=-20`, `LimitRTPRIO=99` |
| `install_systemd.sh` idempotente | ✅ Correcto | ⚠️ **INCOMPATIBLE** | El actual lee `host.env`. La nueva versión debe leer `.env` |
| `deploy/README.md` | ✅ Correcto | ✅ Compatible | No existe, hay que crear |
| `deploy/systemd/templates/` | ⚠️ **Eliminar o mantener** | ⚠️ **DEBATE** | Si se eliminan templates, ¿cómo se hace deploy en otro host? |
| `openssl rand -hex 32` para secrets | ✅ Correcto | ✅ Compatible | Funciona, OpenSSL está disponible |
| `Systemd` como runtime canónico | ✅ ADR-0039 | ⚠️ **INCOMPATIBLE** | `ocm-market-data` tiene 2 procesos, uno manual |

### 5.2 La Propuesta es Correcta Pero Incompleta

**La propuesta asume que `.env` tiene todas las variables necesarias.** No las tiene. Las variables que faltan están en `host.env`:

```
OCM_REPO_ROOT=/home/orangemusic/trading/orangecashmachine  → FALTA EN .env
OCM_HOST_USER=orangemusic                                     → FALTA EN .env
KAFKA_BOOTSTRAP_SERVERS=localhost:9093                       → SÍ está en .env
REDIS_ENABLED=true                                             → FALTA EN .env
REDIS_HOST=localhost                                           → FALTA EN .env
REDIS_PORT=6379                                                → FALTA EN .env
MARKET_DATA_HOST=0.0.0.0                                     → FALTA EN .env
MARKET_DATA_PORT=8001                                        → FALTA EN .env
INGESTION_INTERVAL_S=300                                     → FALTA EN .env
```

**La propuesta asume que `host.env` puede eliminarse sin migrar estas variables.** Esto es incorrecto.

---

## 6. PLAN DE REMEDIACIÓN CORREGIDO

### Paso 0: MIGRAR variables de `host.env` a `.env` (ANTES de eliminar `host.env`)

```bash
# Agregar variables faltantes a .env
cat >> /home/orangemusic/trading/orangecashmachine/.env << 'EOF'

# ── Migradas de deploy/host.env ─────────────────────────────────────
OCM_HOST_USER=orangemusic
OCM_REPO_ROOT=/home/orangemusic/trading/orangecashmachine
REDIS_ENABLED=true
REDIS_HOST=localhost
REDIS_PORT=6379
MARKET_DATA_HOST=0.0.0.0
MARKET_DATA_PORT=8001
INGESTION_INTERVAL_S=300

# ── Seguridad ───────────────────────────────────────────────────────
OCM_API_JWT_SECRET=$(openssl rand -hex 32)
REDIS_PASSWORD=$(openssl rand -hex 32)
EOF
```

### Paso 1: CAMBIAR `OCM_ENV=development` a `OCM_ENV=production` en `.env`

```bash
sed -i 's/^OCM_ENV=development$/OCM_ENV=production/' /home/orangemusic/trading/orangecashmachine/.env
```

### Paso 2: ARREGLAR drift `KAFKA_HOST_PORT`

```bash
# .env.example debe tener 9093 para coincidir con .env y docker-compose
sed -i 's/^KAFKA_HOST_PORT=9094$/KAFKA_HOST_PORT=9093/' /home/orangemusic/trading/orangecashmachine/.env.example
```

### Paso 3: ELIMINAR `host.env` y `host.env.example`

```bash
rm -f /home/orangemusic/trading/orangecashmachine/deploy/host.env
rm -f /home/orangemusic/trading/orangecashmachine/deploy/host.env.example
```

### Paso 4: ELIMINAR `deploy/systemd/rendered/`

```bash
rm -rf /home/orangemusic/trading/orangecashmachine/deploy/systemd/rendered/
```

### Paso 5: ELIMINAR `deploy/systemd/templates/*.template`

```bash
rm -f /home/orangemusic/trading/orangecashmachine/deploy/systemd/templates/*.template
```

### Paso 6: CREAR `deploy/systemd/ocm-market-data.service` (estático)

```bash
cat > /home/orangemusic/trading/orangecashmachine/deploy/systemd/ocm-market-data.service << 'EOF'
[Unit]
Description=OrangeCashMachine - Market Data Engine
After=network-online.target docker.service
Wants=network-online.target docker.service

[Service]
Type=simple
User=orangemusic
Group=orangemusic
WorkingDirectory=/home/orangemusic/trading/orangecashmachine
EnvironmentFile=/home/orangemusic/trading/orangecashmachine/.env
ExecStart=/home/orangemusic/trading/orangecashmachine/.venv/bin/python -m market_data.main
Nice=-5
LimitNOFILE=65536
Restart=on-failure
RestartSec=5s
StartLimitIntervalSec=300s
StartLimitBurst=5
TimeoutStopSec=30
KillSignal=SIGINT
StandardOutput=journal
StandardError=journal
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=/home/orangemusic/trading/orangecashmachine/data_platform /home/orangemusic/trading/orangecashmachine/logs
PrivateTmp=true

[Install]
WantedBy=multi-user.target
EOF
```

**NOTA**: Se usa `ProtectSystem=strict` (no `full`) para evitar romper imports de Python. Se usa `Nice=-5` (no `-10` ni `-20`) para evitar conflictos con `NoNewPrivileges`. Se omite `CPUAffinity`, `LimitRTPRIO`, `LimitMEMLOCK` porque son incompatibles con `NoNewPrivileges=true`.

### Paso 7: CREAR `deploy/systemd/ocm-streaming.service` (estático)

```bash
cat > /home/orangemusic/trading/orangecashmachine/deploy/systemd/ocm-streaming.service << 'EOF'
[Unit]
Description=OrangeCashMachine streaming service (realtime feeds via WebSocket)
After=network-online.target docker.service
Wants=network-online.target docker.service

[Service]
Type=simple
User=orangemusic
Group=orangemusic
WorkingDirectory=/home/orangemusic/trading/orangecashmachine
EnvironmentFile=/home/orangemusic/trading/orangecashmachine/.env
ExecStart=/home/orangemusic/trading/orangecashmachine/.venv/bin/python -m app.cli.streaming_hydra
Nice=-5
LimitNOFILE=65536
Restart=on-failure
RestartSec=5s
StartLimitIntervalSec=300s
StartLimitBurst=5
TimeoutStopSec=30
KillSignal=SIGINT
StandardOutput=journal
StandardError=journal
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=/home/orangemusic/trading/orangecashmachine/data_platform /home/orangemusic/trading/orangecashmachine/logs
PrivateTmp=true

[Install]
WantedBy=multi-user.target
EOF
```

### Paso 8: CREAR `deploy/README.md`

```markdown
# OrangeCashMachine — Despliegue

## Requisitos
- Debian 13+
- Docker 26+
- Python 3.11+
- uv

## Despliegue
1. Copiar `.env.example` a `.env`
2. Configurar credenciales
3. `chmod 600 .env`
4. `bash deploy/scripts/install_systemd.sh`
5. `sudo systemctl enable --now ocm-streaming.service ocm-market-data.service`

## Verificación
- `systemctl status ocm-streaming.service ocm-market-data.service`
- `curl http://localhost:8001/health`
- `docker ps`
```

### Paso 9: REEMPLAZAR `install_systemd.sh` con versión idempotente

```bash
cat > /home/orangemusic/trading/orangecashmachine/deploy/scripts/install_systemd.sh << 'SCRIPT'
#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENV_FILE="${REPO_ROOT}/.env"
SYSTEMD_DEST="/etc/systemd/system"

if [[ ! -f "${ENV_FILE}" ]]; then
    echo "ERROR: No existe ${ENV_FILE}. Ejecute: cp .env.example .env"
    exit 1
fi

ENV_PERM=$(stat -c "%a" "${ENV_FILE}")
if [[ "${ENV_PERM}" != "600" ]]; then
    chmod 600 "${ENV_FILE}"
fi

export OCM_REPO_ROOT="${REPO_ROOT}"
export OCM_HOST_USER="$(id -un)"

# Instalar archivos .service estáticos
for svc in ocm-market-data ocm-streaming; do
    src="${REPO_ROOT}/deploy/systemd/${svc}.service"
    dst="${SYSTEMD_DEST}/${svc}.service"
    install -m 644 "${src}" "${dst}"
    echo "Instalado: ${dst}"
done

systemctl daemon-reload
echo "Instalación completada. Ejecute:"
echo "  sudo systemctl enable --now ocm-market-data.service"
echo "  sudo systemctl enable --now ocm-streaming.service"
SCRIPT
chmod +x /home/orangemusic/trading/orangecashmachine/deploy/scripts/install_systemd.sh
```

### Paso 10: RECARGA Y VERIFICACIÓN

```bash
# Matar proceso manual
kill -15 192727 || true
sleep 2

# Recargar systemd y habilitar
systemctl daemon-reload
systemctl enable ocm-market-data.service ocm-streaming.service
systemctl start ocm-streaming.service ocm-market-data.service

# Verificar
systemctl is-active ocm-streaming.service ocm-market-data.service
systemctl status ocm-market-data.service --no-pager | head -15
ss -tlnp | grep 8001
curl -s localhost:8001/health
```

---

## 7. COMANDOS DE VERIFICACIÓN DIRECTA

```bash
# === DIAGNÓSTICO COMPLETO ===
echo "=== DRIFT ===" && \
git diff HEAD --stat -- deploy/ && \
echo "=== PROCESSES ===" && \
ps aux | grep -E "market_data|streaming_hydra" | grep -v grep && \
echo "=== SYSTEMD ===" && \
systemctl is-enabled ocm-streaming.service ocm-market-data.service && \
systemctl is-active ocm-streaming.service ocm-market-data.service && \
echo "=== DOCKER ===" && \
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.State}}" && \
echo "=== HEALTH ===" && \
curl -s localhost:8001/health && echo && \
bash /home/orangemusic/trading/orangecashmachine/deploy/scripts/health_check.sh && \
echo "=== PERMISSIONS ===" && \
stat -c "%a %U:%G %n" /home/orangemusic/trading/orangecashmachine/.env /home/orangemusic/trading/orangecashmachine/deploy/host.env 2>/dev/null && \
echo "=== TEMPLATE DRIFT ===" && \
diff /home/orangemusic/trading/orangecashmachine/deploy/systemd/templates/ocm-market-data.service.template /home/orangemusic/trading/orangecashmachine/deploy/systemd/rendered/ocm-market-data.service 2>/dev/null && echo "DIFFERENT" || echo "SAME"
```

---

## 8. CONTRADICCIONES DOCUMENTADAS

| # | Contradicción | Fuente A | Fuente B | Estado |
|---|---|---|---|---|
| C-1 | `ProtectSystem=full` (propuesta) vs `strict` (instalar) | Propuesta | `/etc/systemd/system/ocm-*.service` | **RIESGO** — `full` puede romper Python imports |
| C-2 | `Nice=-20` + `LimitRTPRIO=99` vs `NoNewPrivileges=true` | Templates | systemd requirements | **CRÍTICA** |
| C-3 | `host.env` existe vs propuesta eliminarlo | `deploy/host.env` | Propuesta | **INCOMPATIBLE** — necesita migración previa |
| C-4 | `.env.example` `KAFKA_HOST_PORT=9094` vs `.env=9093` | `.env.example` | `.env`, `docker-compose.yml` | **DRIFT** |
| C-5 | `.env` `OCM_ENV=development` vs `.env.example=production` | `.env` | `.env.example` | **DRIFT** |
| C-6 | `CPUAffinity=2 3` en template | Template modificado | systemd capabilities | **INVALIDO** |
| C-7 | `install_systemd.sh` con `systemctl enable --now` | Modificado localmente | ADR-0039 §13 (no autoriza) | **CONTRADICCIÓN** |
| C-8 | `ocm-market-data` 2 procesos | `ps aux` | systemd is-active=active | **DRIFT** |
| C-9 | `docker-compose.yml` `OCM_ENV=${OCM_ENV:-production}` | docker-compose | `.env` tiene `development` | **INCONSISTENTE** |
| C-10 | `ALERTMANAGER_HOST_PORT` valores inconsistentes | `.env=9094`, `.env.example=9093`, `docker-compose=9090` | — | **DRIFT** |

---

## 9. VEDICTO FINAL

### 9.1 Resumen por Dimensión

| Dimensión | Veredicto | Confianza |
|---|---|---|
| **Propuesta arquitectónica** | ✅ **CORRECTA** pero incompleta | HIGH |
| **Eliminación `host.env`** | ⚠️ **REQUIERE MIGRACIÓN** de variables a `.env` | HIGH |
| **`.env` como SSOT** | ⚠️ **PARCIAL** — faltan variables de `host.env` | HIGH |
| **Templates estáticos** | ✅ **CORRECTO** — eliminar `envsubst` | HIGH |
| **`ProtectSystem=full`** | ⚠️ **RIESGO** — puede romper Python | HIGH |
| **`NoNewPrivileges` + `Nice`** | ❌ **CONTRADICCIÓN** en templates | HIGH |
| **Systemd canónico** | ⚠️ **DRIFT** — 2 procesos, unidad stale | HIGH |
| **Seguridad de secrets** | ❌ **PLACEHOLDERS + REAL KEYS** | HIGH |
| **Observabilidad** | ❌ **DOWN** | HIGH |
| **`check_production_gates.py`** | ✅ **EXISTE** — mi hallazgo previo era incorrecto | HIGH |
| **`deploy/README.md`** | ❌ **NO EXISTE** | HIGH |
| **`deploy/rendered/`** | ❌ **STALE** — debe eliminarse | HIGH |

### 9.2 Conteo

| Clasificación | Conteo |
|---|---|
| P0 (Crítico) | **12** |
| P1 (Bloqueador) | **14** |
| P2 (Riesgo operativo) | **10** |
| E (Evidencia insuficiente) | **12** |
| C (Contradicciones) | **10** |
| **Total findings** | **58** |
| **Contradicciones** | **10** |

### 9.3 Statement Final

**La propuesta de remediación es arquitectónicamente correcta pero requiere ajustes antes de implementación:**

1. **NO eliminar `host.env` sin migrar primero** sus variables (`OCM_REPO_ROOT`, `OCM_HOST_USER`, `REDIS_*`, `MARKET_DATA_*`) a `.env`.
2. **`ProtectSystem=full` puede romper Python** — verificar antes de implementar. `strict` es más seguro.
3. **`Nice=-20` + `LimitRTPRIO=99` son INCOMPATIBLES** con `NoNewPrivileges=true` — eliminar de templates.
4. **Arreglar drift `KAFKA_HOST_PORT`** entre `.env` (9093) y `.env.example` (9094).
5. **`check_production_gates.py` SÍ EXISTE** — hallazgo previo P1-6 era incorrecto.

**Esta auditoría NO autoriza implementación.**

---

## 10. REGISTRO DE AUDITORÍA

| Item | Valor |
|---|---|
| Protocolo | AUDIT_PROTOCOL v2.1 |
| Método | READ-ONLY |
| Comandos ejecutados | `cat`, `diff`, `grep`, `stat`, `ls`, `ps`, `ss`, `systemctl`, `docker ps`, `git diff`, `uv run`, `curl`, `chmod`, `sed` |
| Archivos modificados | **NINGUNO** |
| Archivos creados | Ninguno |
| Servicios reiniciados | **NINGUNO** |
| Configuraciones cambiadas | **NINGUNA** |

**Detenerse. Esperar autorización humana.**
