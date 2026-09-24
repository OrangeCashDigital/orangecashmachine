# AUDITORÍA TÉCNICA DE DESPLIEGUE, SYSTEMD Y LIMPIEZA DE ARTEFACTOS

- **ID**: DEP-003
- **Fecha**: 2026-09-22
- **Modo**: READ-ONLY absoluto. Cero modificaciones.
- **Scope**: Eliminación de indirecciones, estandarización systemd, detección de drifts, seguridad, estado Docker.
- **Baseline**: commit `857cfe60`, branch `feat/m22b-governance`.

---

## 1. EXECUTIVE SUMMARY & VERDICTA

**Veredicto**: `DRIFT DETECTED`

La arquitectura de despliegue sufre de **drift documentado masivo** entre tres niveles:
1. **Templates** (repo) → contienen cambios no committeados
2. **Rendered** (`deploy/systemd/rendered/`) → stale, NO reflejan templates actuales
3. **Installed** (`/etc/systemd/system/`) → stale, NO reflejan templates actuales
4. **Running** → DOS procesos `market_data.main` compitiendo (uno manual, uno systemd)

**Estado general**:

| Dimensión | Estado | Evidencia |
|---|---|---|
| Templates | ⚠️ **MODIFICADOS localmente** | `git diff HEAD` |
| Rendered | ❌ **STALE** | `diff template≠rendered` = DIFFERENT |
| Installed | ❌ **STALE** | `diff rendered≠installed` = SAME |
| `ocm-streaming` | ✅ ENABLED + ACTIVE | `systemctl is-active` |
| `ocm-market-data` | ⚠️ **ENABLED + ACTIVE** pero con unidad STALE + 2 procesos | `systemctl is-active` + `ps aux` |
| Docker | ⚠️ **3/13 containers UP** | `docker ps` |
| Observabilidad | ❌ **DOWN** | Sin Prometheus/Grafana/Loki |
| `.env` secrets | ❌ **REAL + PLACEHOLDER** | `BYBIT_API_KEY`, `OCM_API_JWT_SECRET=CHANGE_ME...` |
| `host.env` | ❌ **REDUNDANTE + HARDCODED** | Idéntico a `host.env.example` |

---

## 2. MATRIZ DE LIMPIEZA Y DEUDAS DE ARTEFACTOS

### 2.1 Archivos a Eliminar/Depurar

| Archivo | Acción | Justificación | Estado |
|---|---|---|---|
| **`deploy/host.env`** | **ELIMINAR** | Redundante con `.env`. Contiene `OCM_REPO_ROOT=/home/orangemusic/trading/orangecashmachine` hardcodeado (host-specific). `host.env` y `.env` tienen las mismas claves. | `P1` |
| **`deploy/host.env.example`** | **ELIMINAR** | Idéntico a `host.env`. No aporta nada nuevo. Todo se puede mover a `.env.example`. | `P1` |
| **`deploy/scripts/install_systemd.sh`** | **REFACTORIZAR** | Depende de `host.env`, `OCM_REPO_ROOT`, `OCM_HOST_USER`. Debería leer directamente de `.env` y descubrir `REPO_ROOT` dinámicamente. | `P1` |
| **`deploy/systemd/templates/*.template`** | **ELIMINAR** | Las plantillas `envsubst` son la fuente de drift. Los archivos `.service` deberían ser estáticos en el repo. | `P0` |
| **`deploy/systemd/rendered/`** | **ELIMINAR** | Directorio derivado, gitignored, pero stale. No debería existir. | `P1` |
| **`deploy/systemd/templates/ocm-market-data.service.template`** | **REEMPLAZAR** | Con `deploy/systemd/ocm-market-data.service` estático (sin placeholders). | `P0` |
| **`deploy/systemd/templates/ocm-streaming.service.template`** | **REEMPLAZAR** | Con `deploy/systemd/ocm-streaming.service` estático (sin placeholders). | `P0` |
| **`scripts/check_production_gates.py`** | **CREAR** | ADR-0033 "Propuesto" pero script NO existe. | `P1` |

### 2.2 Dependencias que Referencian `host.env` o `OCM_REPO_ROOT`

| Ubicación | Referencia | Tipo | Impacto |
|---|---|---|---|
| `deploy/scripts/install_systemd.sh:14-15` | `HOST_ENV`, `HOST_ENV_EXAMPLE` | Script | **BLOQUEANTE** |
| `deploy/scripts/install_systemd.sh:36-44` | Auto-bootstrap `host.env` | Script | **BLOQUEANTE** |
| `deploy/scripts/install_systemd.sh:52-54` | `source host.env` | Script | **BLOQUEANTE** |
| `deploy/scripts/install_systemd.sh:65-67` | `grep -oE '^[A-Z_]+=' host.env` | Script | **BLOQUEANTE** |
| `deploy/systemd/templates/ocm-market-data.service.template:13` | `EnvironmentFile=-${OCM_REPO_ROOT}/deploy/host.env` | Template | **BLOQUEANTE** |
| `deploy/systemd/templates/ocm-streaming.service.template:13` | `EnvironmentFile=-${OCM_REPO_ROOT}/deploy/host.env` | Template | **BLOQUEANTE** |
| `deploy/scripts/health_check.sh:19` | `OCM_REPO_ROOT` fallback | Script | **MENOR** |
| `deploy/scripts/deploy_ocm.sh` | No referencia `host.env` | Script | **NINGUNO** |
| `ocm/config/` | No referencia `host.env` | Código | **NINGUNO** |
| `packages/market_data/` | No referencia `host.env` | Código | **NINGUNO** |

### 2.3 `.gitignore` Higiene

| Archivo | En `.gitignore` | ¿Correcto? | Nota |
|---|---|---|---|
| `.env` | ✅ Sí | ✅ Correcto | Secrets protegidos |
| `deploy/host.env` | ✅ Sí | ✅ Correcto | Pero será ELIMINADO |
| `deploy/systemd/rendered/` | ✅ Sí | ✅ Correcto | Pero será ELIMINADO |
| `deploy/host.env.example` | ❌ **NO** | ⚠️ Debería estar | Actualmente trackeado en VCS |

**PROBLEMA**: `deploy/host.env.example` NO está en `.gitignore` pero es un template. Debería estar tracked como ejemplo portable. Sin embargo, si `host.env` se elimina, `host.env.example` pierde sentido.

---

## 3. MATRIZ DE AUDITORÍA SYSTEMD

### 3.1 Estado Actual vs. Propuesta Estándar

#### `ocm-market-data.service`

| Propiedad | INSTALADO (stale) | TEMPLATE (modificado) | ESTÁNDAR PROPUESTO |
|---|---|---|---|
| **`EnvironmentFile`** | `EnvironmentFile=/home/.../deploy/host.env` + `.env` | `EnvironmentFile=-/home/.../deploy/host.env` + `-.env` | `EnvironmentFile=/home/.../.env` (único) |
| **`WorkingDirectory`** | `/home/orangemusic/trading/orangecashmachine` | `${OCM_REPO_ROOT}` | `/home/orangemusic/trading/orangecashmachine` |
| **`User`** | `orangemusic` | `${OCM_HOST_USER}` | `orangemusic` |
| **`ExecStart`** | `.venv/bin/python -m market_data.main` | `${OCM_REPO_ROOT}/.venv/bin/python -m market_data.main` | `.venv/bin/python -m market_data.main` |
| **`Nice`** | ❌ No presente | ✅ `Nice=-10` | `Nice=-10` o `Nice=-5` |
| **`LimitNOFILE`** | ❌ No presente | ✅ `LimitNOFILE=65536` | `LimitNOFILE=65536` |
| **`Restart`** | `on-failure` | `on-failure` | `on-failure` |
| **`RestartSec`** | `10` | `5s` | `5s` |
| **`StartLimitIntervalSec`** | ❌ No presente | ✅ `300s` | `300s` |
| **`StartLimitBurst`** | ❌ No presente | ✅ `5` | `5` |
| **`StandardOutput/Error`** | ❌ No presente | ✅ `journal` | `journal` |
| **`CPUAffinity`** | ❌ No presente | ❌ No presente | ❌ **INVALID** (ver abajo) |
| **`After/Wants`** | `After=network-online.target` | `After=network-online.target docker.service` + `Wants=...docker.service` | `After=network-online.target` |

#### `ocm-streaming.service`

| Propiedad | INSTALADO (stale) | TEMPLATE (modificado) | ESTÁNDAR PROPUESTO |
|---|---|---|---|
| **`EnvironmentFile`** | `EnvironmentFile=/home/.../deploy/host.env` + `.env` | `EnvironmentFile=-/home/.../deploy/host.env` + `-.env` | `EnvironmentFile=/home/.../.env` (único) |
| **`Nice`** | ❌ No presente | ✅ `Nice=-20` | `Nice=-10` |
| **`LimitRTPRIO`** | ❌ No presente | ✅ `99` | ❌ **CONFLICTO** con `NoNewPrivileges` |
| **`LimitMEMLOCK`** | ❌ No presente | ✅ `infinity` | ❌ **CONFLICTO** con `ProtectSystem=strict` |
| **`LimitNOFILE`** | ❌ No presente | ✅ `65536` | `LimitNOFILE=65536` |
| **`CPUAffinity`** | ❌ No presente | ✅ `CPUAffinity=2 3` | ❌ **INVALID** |
| **`Restart`** | `on-failure` | `on-failure` | `on-failure` |
| **`RestartSec`** | `10` | `5s` | `5s` |

### 3.2 Contradicciones Detectadas en Templates Modificados

| # | Contradicción | Template | Severidad |
|---|---|---|---|
| CB-1 | `Nice=-20` + `LimitRTPRIO=99` + `LimitMEMLOCK=infinity` + `NoNewPrivileges=true` + `ProtectSystem=strict` | `ocm-streaming.service.template` | **CRÍTICA** — systemd RECHAZARÁ estas opciones. `Nice=-20` requiere CAP_SYS_NICE, `LimitRTPRIO=99` requiere CAP_SYS_NICE, `LimitMEMLOCK=infinity` requiere CAP_IPC_LOCK. `NoNewPrivileges=true` bloquea todas estas capabilities. |
| CB-2 | `CPUAffinity=2 3` en `[Service]` | `ocm-streaming.service.template` | **ALTA** — `CPUAffinity` es directiva de `[Service]` en systemd v229+, pero requiere cgroups v2. Además, el host tiene 4 cores (i5-3330), pero los cores 2 y 3 son los únicos disponibles (0-3). Asignar solo cores 2-3 al streaming deja 2 cores para market-data, Redis, Docker y OS. |
| CB-3 | `EnvironmentFile=-${OCM_REPO_ROOT}/deploy/host.env` | `ocm-*.service.template` | **MEDIA** — El prefijo `-` significa "opcional". Si `host.env` no existe, systemd no falla. Pero `host.env` existe en el host. El `-` es innecesario si se elimina `host.env`. |
| CB-4 | `Wants=network-online.target docker.service` | `ocm-*.service.template` | **BAJA** — `ocm-streaming` depende de Docker para Kafka/Redis, lo cual es correcto. Pero `ocm-market-data` depende de Docker para Kafka, lo cual es correcto para el pipeline. |
| CB-5 | `Nice=-10` en `ocm-market-data` vs `Nice=-20` en `ocm-streaming` | Templates | **BAJA** — Diferente prioridad. Streaming tiene prioridad mayor que market-data. Justificado por latencia WS vs batch REST. |

### 3.3 Drift Crítico: Templates vs Rendered vs Installed vs Running

```
REPO (templates)          deploy/systemd/rendered/         /etc/systemd/system/         RUNNING
┌──────────────┐      ┌──────────────────────┐      ┌──────────────────────┐      ┌──────────┐
│ NEW content  │─────>│ OLD content (stale)  │─────>│ OLD content (stale)  │─────>│ ?        │
│ Nice=-10     │      │ Nice=N/A             │      │ Nice=N/A             │      │ 2 procs  │
│ EnvFile=-    │      │ EnvFile=             │      │ EnvFile=             │      │ (1+1)    │
│ StartLimit   │      │ StartLimit=N/A       │      │ StartLimit=N/A       │      │          │
│ journal      │      │ journal=N/A          │      │ journal=N/A          │      │          │
│ CPUAffinity  │      │ CPUAffinity=N/A      │      │ CPUAffinity=N/A      │      │          │
│ Nice=-20     │      │ Nice=N/A             │      │ Nice=N/A             │      │          │
└──────────────┘      └──────────────────────┘      └──────────────────────┘      └──────────┘
     TEMPLATE              RENDERED                      INSTALLED                    RUNNING
     (NEW)                 (STALE)                       (STALE)                       MIXED
```

**PROBLEMA P0**: Los templates tienen nuevo contenido pero nunca fueron re-renderizados ni re-instalados. El sistema está ejecutando la versión STALE.

### 3.4 Estado Systemd Detallado

| Unidad | `is-enabled` | `is-active` | MainPID | Estado Real |
|---|---|---|---|---|
| `ocm-streaming.service` | enabled | active | 69498 | ✅ CORRECTO |
| `ocm-market-data.service` | **enabled** | **active** | **?** | ⚠️ **PROBLEMA** — `systemctl is-active` dice `active` pero `ps aux` muestra DOS procesos `market_data.main` |

**DOS PROCESOS `market_data.main` CORRIENDO**:
- `pid=192727` (Sep 21, manual, escucha puerto 8001)
- `pid=718168` (Sep 22 14:36, posiblemente systemd, NO escucha puerto 8001)

Esto indica que systemd inició un nuevo proceso (pid=718168) pero el puerto 8001 ya estaba ocupado por el proceso manual (pid=192727). El proceso systemd probablemente falló al binding o está en conflicto.

---

## 4. MATRIZ DE HALLAZGOS Y PLAN DE REMEDIACIÓN

### P0 — Bloqueantes / Seguridad / Drift Crítico

| # | Finding | Severidad | Evidencia | Remediación Requerida |
|---|---|---|---|---|
| **P0-1** | **Drift masivo de templates**: templates modificados localmente (no commit) pero renderizado e instalación stale | `CRITICAL` | `diff template≠rendered` = DIFFERENT | Revertir templates o re-renderizar + reinstalar |
| **P0-2** | **DOS procesos `market_data.main`** corriendo: pid=192727 (manual) + pid=718168 (systemd), compitiendo por puerto 8001 | `CRITICAL` | `ps aux \| grep market_data` | Matar proceso manual (pid=192727) o sistema systemd gestionar ambos |
| **P0-3** | **`NoNewPrivileges=true` + `Nice=-20` + `LimitRTPRIO=99` + `LimitMEMLOCK=infinity`** — CONTRADICCIÓN en `ocm-streaming.service.template` | `CRITICAL` | `ocm-streaming.service.template:28-34` | Eliminar `Nice=-20`, `LimitRTPRIO=99`, `LimitMEMLOCK=infinity` O quitar `NoNewPrivileges=true` |
| **P0-4** | **`.env` contiene `BYBIT_API_KEY` real** expuesto via `EnvironmentFile` en systemd unit | `CRITICAL` | `cat .env` + `cat /etc/systemd/system/ocm-*.service` | Mover a archivo de secrets separado, NO en `.env`, NO en `EnvironmentFile` |
| **P0-5** | **`OCM_API_JWT_SECRET=CHANGE_ME_generate_with_secrets_token_hex_32`** — placeholder, JWT roto | `CRITICAL` | `cat .env` | Generar secreto real con `secrets.token_hex(32)` |
| **P0-6** | **`REDIS_PASSWORD=ocm_local_dev`** — contraseña trivial, Redis sin autenticación real | `CRITICAL` | `cat .env` | Generar password seguro o configurar autenticación Redis |
| **P0-7** | **`OCM_ENV=development` en `host.env`** — entorno de desarrollo en producción | `CRITICAL` | `cat deploy/host.env` | Cambiar a `OCM_ENV=production` o eliminar `host.env` |
| **P0-8** | **Sin `client_order_id` / idempotencia** — reenvío de órdenes puede producir duplicados | `CRITICAL` | Sin evidencia en código | Implementar `client_order_id` determinista |
| **P0-9** | **Sin crash recovery & reconciliation protocol** — sin START→LOAD→QUERY→RECONCILE→ENABLE | `CRITICAL` | Sin secuencia documentada | Implementar protocolo de recuperación |
| **P0-10** | **Sin circuit breakers** — sin kill switch, max drawdown, max position | `CRITICAL` | Sin implementación | Implementar risk gates |

### P1 — Infraestructura / Producción / Observabilidad

| # | Finding | Severidad | Evidencia | Remediación Requerida |
|---|---|---|---|---|
| **P1-1** | **`deploy/host.env` redundant** — mismas claves que `.env`, hardcodea `OCM_REPO_ROOT` | `HIGH` | `diff host.env host.env.example` = IDÉNTICO | Eliminar `host.env` y `host.env.example`; consolidar en `.env` |
| **P1-2** | **`deploy/systemd/templates/` con `envsubst`** — fuente de drift | `HIGH` | Template vs rendered = DIFFERENT | Reemplazar con archivos `.service` estáticos |
| **P1-3** | **`deploy/systemd/rendered/` stale** — directorio derivado sin sentido | `HIGH` | `diff rendered≠installed` = SAME pero ambos stale | Eliminar directorio |
| **P1-4** | **`install_systemd.sh` con `systemctl enable --now`** — modificación no documentada | `HIGH` | `git diff HEAD` | Revertir o documentar como nuevo comportamiento |
| **P1-5** | **Observabilidad completa DOWN** — Prometheus, Grafana, Loki, Alertmanager, Pushgateway, Promtail no corren | `HIGH` | `docker ps` solo muestra Redis/Kafka/Zookeeper | `docker compose up -d --profile monitoring` |
| **P1-6** | **`check_production_gates.py` NO existe** — ADR-0033 "Propuesto" | `HIGH` | `ls scripts/` | Implementar script |
| **P1-7** | **`docker-compose.yml` perfil `[microservices]` no activado** — market-data Docker no corre | `HIGH` | `docker-compose.yml:334` | Decidir si market-data corre en host o Docker |
| **P1-8** | **Sin `market-data` entrypoint en `packages/market_data/main.py`** — no es entrypoint de deploy | `HIGH` | `main.py` tiene FastAPI pero no run.sh entrypoint | Verificar `run.sh ocm` vs `python -m market_data.main` |
| **P1-9** | **Sin PAPER/BACKTEST/LIVE separación** | `HIGH` | `OCM_ENV=development` único selector | Implementar separación de entornos |
| **P1-10** | **Sin heartbeat / Dead Man's Switch** | `HIGH` | Sin mecanismo | Implementar heartbeat externo |
| **P1-11** | **Sin métricas Prometheus** | `HIGH` | Sin container | Levantar observabilidad |
| **P1-12** | **Sin NTP verificado** | `MEDIUM` | `timedatectl` timeout | Verificar `chrony` o `systemd-timesyncd` |
| **P1-13** | **`deploy/host.env.example` NO en `.gitignore`** | `MEDIUM` | `.gitignore` no lo incluye | Agregar a `.gitignore` o eliminarlo |

### P2 — Optimizaciones de Latencia / SO

| # | Finding | Severidad | Evidencia | Remediación Requerida |
|---|---|---|---|---|
| **P2-1** | **`CPUAffinity=2 3` inválido/conflictivo** con `NoNewPrivileges` | `MEDIUM` | `ocm-streaming.service.template:28` | Eliminar o verificar cgroups v2 |
| **P2-2** | **Host 4 cores / 5.5GB RAM** — contención posible | `MEDIUM` | `nproc`, `free -h` | Redis/OCM/Docker comparten recursos |
| **P2-3** | **Sin CPU affinity / IRQ / NUMA configurado** | `MEDIUM` | Sin `taskset` | No aplicable sin requisito de baja latencia |
| **P2-4** | **`Nice=-10` en market-data** no aplicado en `/etc` | `MEDIUM` | Instalado stale | Re-renderizar y reinstalar |
| **P2-5** | **Sin graceful shutdown completo** para `market_data.main` | `MEDIUM` | Sin `try/except/finally` | Implementar signal handler |
| **P2-6** | **Sin log persistence externa** | `MEDIUM` | `logs/` solo filesystem | Implementar rotación/externalización |
| **P2-7** | **Sin alerting externo** | `MEDIUM` | Sin mecanismo | Implementar Telegram/Slack/Discord |
| **P2-8** | **`Dockerfile:40 CMD` válido solo como experiment** | `LOW` | `Dockerfile` | No usar como runtime |
| **P2-9** | **`WSProducerBundle` huérfano** (ADR-0022) | `LOW` | ADR-0022 | No wireado a `main.py` |
| **P2-10** | **`deploy/monitoring/grafana/provisioning/` gitignored** | `LOW` | `.gitignore` | No afecta runtime |

### E — Evidencia Insuficiente

| # | Item | Razón |
|---|---|---|
| E-1 | `ocm-market-data.service` MainPID real | `systemctl show` timeout previo |
| E-2 | Estado de pid=718168 | ¿Crash? ¿Running? ¿Conflict? |
| E-3 | Port 8001 binding | Solo pid=192727 escucha |
| E-4 | Datos recientes en Bronze | Solo files de noviembre 2023 |
| E-5 | Kafka lag real | Sin `kafka-get-offsets` output |
| E-6 | Source of truth para ORDER/POSITION | Sin documentación |
| E-7 | NTP sincronización | `timedatectl` timeout |
| E-8 | IP whitelisting en exchange | Sin verificación |
| E-9 | Withdrawal disable en exchange | Sin verificación |
| E-10 | Trading execution real | Sin entrypoint activo |

### H — Hipótesis

| # | Hipótesis | Validación |
|---|---|---|
| H-1 | `ocm-market-data.service` fue `systemctl enable --now` recientemente | `systemctl show ocm-market-data --property=ActiveEnterTimestamp` |
| H-2 | El usuario ejecutó `install_systemd.sh` modificado | `history` / shell logs |
| H-3 | `host.env` y `.env` deberían consolidarse en UNO | Comparar contenido |
| H-4 | Los templates modificados son intencionales (mejora de latencia) | `git log --oneline deploy/systemd/templates/` |
| H-5 | `CPUAffinity=2 3` es un error, no una feature | Contexto de hardware |

---

## 5. COMANDOS DE EJECUCIÓN DIRECTA (ONE-LINERS / SHELL)

> **NOTA**: Estos comandos son para referencia del estado actual del sistema. La implementación requiere autorización humana explícita.

### 5.1 Diagnóstico de Drift (Verificar Estado Actual)

```bash
# Verificar drift completo
echo "=== DRIFT REPORT ===" && \
for f in ocm-market-data.service ocm-streaming.service; do \
  echo "--- $f ---" && \
  diff /home/orangemusic/trading/orangecashmachine/deploy/systemd/templates/$f.template \
       /home/orangemusic/trading/orangecashmachine/deploy/systemd/rendered/$f && \
  diff /home/orangemusic/trading/orangecashmachine/deploy/systemd/rendered/$f \
       /etc/systemd/system/$f && \
  echo "OK: template=rendered=installed" || echo "DRIFT DETECTED"; \
done && \
echo "=== PROCESSES ===" && \
ps aux | grep -E "market_data|streaming_hydra" | grep -v grep && \
echo "=== SYSTEMD STATUS ===" && \
systemctl is-enabled ocm-streaming.service ocm-market-data.service && \
systemctl is-active ocm-streaming.service ocm-market-data.service && \
echo "=== DOCKER ===" && \
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.State}}" && \
echo "=== HEALTH ===" && \
curl -s localhost:8001/health && echo && \
bash /home/orangemusic/trading/orangecashmachine/deploy/scripts/health_check.sh
```

### 5.2 Limpieza de Artefactos (Requiere Autorización)

```bash
# 1. Detener todos los procesos de market_data
pkill -f "python -m market_data.main" || true
sleep 2

# 2. Detener servicios systemd
systemctl stop ocm-market-data.service ocm-streaming.service || true
systemctl disable ocm-market-data.service ocm-streaming.service || true

# 3. Eliminar templates obsoletos y directorio rendered
rm -f /home/orangemusic/trading/orangecashmachine/deploy/systemd/templates/*.template
rm -rf /home/orangemusic/trading/orangecashmachine/deploy/systemd/rendered/

# 4. Eliminar host.env y host.env.example
rm -f /home/orangemusic/trading/orangecashmachine/deploy/host.env
rm -f /home/orangemusic/trading/orangecashmachine/deploy/host.env.example

# 5. Eliminar scripts de renderizado obsoletos
rm -f /home/orangemusic/trading/orangecashmachine/deploy/scripts/install_systemd.sh
# NOTA: Backup primero: cp deploy/scripts/install_systemd.sh /tmp/install_systemd.sh.bak

# 6. Eliminar /etc/systemd/system/ocm-*.service obsoletos
rm -f /etc/systemd/system/ocm-market-data.service /etc/systemd/system/ocm-streaming.service

# 7. Crear nuevos .service estáticos (ejemplo para market-data)
cat > /etc/systemd/system/ocm-market-data.service << 'EOF'
[Unit]
Description=OrangeCashMachine market-data-service (REST ingestion + Kappa bronze writer + HTTP :8001)
Documentation=file:${OCM_REPO_ROOT}/docs/architecture/decisions/ADR-0022-lifecycle-proceso-realtime-feeds.md
After=network-online.target docker.service
Wants=network-online.target docker.service

[Service]
Type=simple
User=orangemusic
WorkingDirectory=/home/orangemusic/trading/orangecashmachine
EnvironmentFile=/home/orangemusic/trading/orangecashmachine/.env
ExecStart=/home/orangemusic/trading/orangecashmachine/.venv/bin/python -m market_data.main
Nice=-10
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

cat > /etc/systemd/system/ocm-streaming.service << 'EOF'
[Unit]
Description=OrangeCashMachine streaming service (realtime feeds via WebSocket)
Documentation=file:${OCM_REPO_ROOT}/docs/architecture/decisions/ADR-0022-lifecycle-proceso-realtime-feeds.md
After=network-online.target docker.service
Wants=network-online.target docker.service

[Service]
Type=simple
User=orangemusic
WorkingDirectory=/home/orangemusic/trading/orangecashmachine
EnvironmentFile=/home/orangemusic/trading/orangecashmachine/.env
ExecStart=/home/orangemusic/trading/orangecashmachine/.venv/bin/python -m app.cli.streaming_hydra
Nice=-10
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

# 8. Recargar systemd y habilitar
systemctl daemon-reload
systemctl enable ocm-market-data.service ocm-streaming.service
systemctl start ocm-streaming.service ocm-market-data.service

# 9. Verificar
systemctl is-active ocm-streaming.service ocm-market-data.service
systemctl status ocm-streaming.service ocm-market-data.service --no-pager
```

### 5.3 Consolidación `.env` como SSOT (Requiere Autorización)

```bash
# 1. Copiar contenido de host.env a .env (merge, no overwrite)
# host.env contiene: OCM_ENV, OCM_HOST_USER, OCM_REPO_ROOT, MARKET_*, KAFKA_*, REDIS_*
# .env contiene: BYBIT_*, KUCOIN_*, REDIS_PASSWORD, GRAFANA_*, PROMETHEUS_*, etc.

# 2. Verificar que .env tiene TODAS las claves necesarias
echo "=== .env keys ===" && grep -E "^[A-Z_]+=" /home/orangemusic/trading/orangecashmachine/.env | sort

# 3. Verificar que no hay duplicados
echo "=== Duplicados ===" && grep -E "^[A-Z_]+=" /home/orangemusic/trading/orangecashmachine/.env | cut -d= -f1 | sort | uniq -d

# 4. Backup de .env actual
cp /home/orangemusic/trading/orangecashmachine/.env /home/orangemusic/trading/orangecashmachine/.env.bak.$(date +%Y%m%d)

# 5. Agregar OCM_* variables que faltan a .env
# Nota: hacer manualmente, no automatizado
echo "# OCM Runtime Config" >> /home/orangemusic/trading/orangecashmachine/.env
echo "OCM_ENV=production" >> /home/orangemusic/trading/orangecashmachine/.env
echo "OCM_HOST_USER=orangemusic" >> /home/orangemusic/trading/orangecashmachine/.env
echo "OCM_REPO_ROOT=/home/orangemusic/trading/orangecashmachine" >> /home/orangemusic/trading/orangecashmachine/.env
echo "KAFKA_BOOTSTRAP_SERVERS=localhost:9093" >> /home/orangemusic/trading/orangecashmachine/.env

# 6. Verificar .env no tiene secrets en claro
echo "=== Verificar secrets ===" && grep -E "SECRET|PASSWORD|KEY" /home/orangemusic/trading/orangecashmachine/.env | grep -v "CHANGE_ME" | grep -v "^#"
```

### 5.4 Levantar Observabilidad (Requiere Autorización)

```bash
# Levantar stack de monitoreo completo
docker compose --profile monitoring up -d

# Verificar
docker ps --filter "name=prometheus\|name=grafana\|name=loki\|name=alertmanager\|name=pushgateway\|name=promtail" --format "table {{.Names}}\t{{.Status}}\t{{.State}}"

# Verificar health
curl -s localhost:9091/-/healthy && echo " Prometheus OK"
curl -s localhost:3000/api/health && echo " Grafana OK"
```

### 5.5 Generar Secretos Seguros (Requiere Autorización)

```bash
# Generar JWT secret real
python3 -c "import secrets; print(secrets.token_hex(32))"
# Salida: reemplazar OCM_API_JWT_SECRET en .env

# Generar Redis password seguro
python3 -c "import secrets; print(secrets.token_hex(16))"
# Salida: reemplazar REDIS_PASSWORD en .env

# Generar Kafka UI password
python3 -c "import secrets; print(secrets.token_urlsafe(24))"
# Salida: reemplazar KAFKA_UI_PASSWORD en .env
```

### 5.6 Verificar NTP (Requiere Autorización)

```bash
# Verificar sincronización temporal
timedatectl status
chronyc tracking 2>/dev/null || echo "chrony no instalado"
ntpstat 2>/dev/null || echo "ntpstat no disponible"
cat /sys/devices/system/clocksource/clocksource0/current_clocksource
```

### 5.7 Limpieza Completa de Drift (Script Completo)

```bash
#!/usr/bin/env bash
# DEPLOYMENT CLEANUP — Requiere autorización explícita
set -euo pipefail

REPO_ROOT="/home/orangemusic/trading/orangecashmachine"

echo "=== 1. PARAR PROCESOS ==="
pkill -f "python -m market_data.main" 2>/dev/null || true
sleep 2

echo "=== 2. DETENER SYSTEMD ==="
systemctl stop ocm-market-data.service ocm-streaming.service 2>/dev/null || true
systemctl disable ocm-market-data.service ocm-streaming.service 2>/dev/null || true

echo "=== 3. ELIMINAR ARTEFACTOS OBSOLETOS ==="
rm -f "${REPO_ROOT}/deploy/systemd/templates/"*.template
rm -rf "${REPO_ROOT}/deploy/systemd/rendered/"
rm -f "${REPO_ROOT}/deploy/host.env" "${REPO_ROOT}/deploy/host.env.example"
# NOTA: install_systemd.sh se preserva como backup: cp a /tmp/

echo "=== 4. REINSTALAR UNIDADES ==="
cat > /etc/systemd/system/ocm-market-data.service << 'EOF'
[Unit]
Description=OrangeCashMachine market-data-service
After=network-online.target docker.service
Wants=network-online.target docker.service
[Service]
Type=simple
User=orangemusic
WorkingDirectory=/home/orangemusic/trading/orangecashmachine
EnvironmentFile=/home/orangemusic/trading/orangecashmachine/.env
ExecStart=/home/orangemusic/trading/orangecashmachine/.venv/bin/python -m market_data.main
Nice=-10
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

cat > /etc/systemd/system/ocm-streaming.service << 'EOF'
[Unit]
Description=OrangeCashMachine streaming service
After=network-online.target docker.service
Wants=network-online.target docker.service
[Service]
Type=simple
User=orangemusic
WorkingDirectory=/home/orangemusic/trading/orangecashmachine
EnvironmentFile=/home/orangemusic/trading/orangecashmachine/.env
ExecStart=/home/orangemusic/trading/orangecashmachine/.venv/bin/python -m app.cli.streaming_hydra
Nice=-10
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

systemctl daemon-reload
systemctl enable ocm-market-data.service ocm-streaming.service
systemctl start ocm-streaming.service ocm-market-data.service

echo "=== 5. VERIFICAR ==="
systemctl is-active ocm-streaming.service ocm-market-data.service
systemctl status ocm-streaming.service ocm-market-data.service --no-pager | head -20

echo "=== 6. VERIFICAR DRIFT ==="
echo "Templates eliminados: $(ls ${REPO_ROOT}/deploy/systemd/templates/ 2>/dev/null | wc -l) files"
echo "Rendered eliminado: $(ls ${REPO_ROOT}/deploy/systemd/rendered/ 2>/dev/null | wc -l) files"
echo "host.env eliminado: $(ls ${REPO_ROOT}/deploy/host.env 2>/dev/null | wc -l) files"

echo "=== LIMPIEZA COMPLETADA ==="
```

---

## 6. REGISTRO DE AUDITORÍA

| Item | Valor |
|---|---|
| Protocolo | AUDIT_PROTOCOL v2.1 |
| Método | READ-ONLY |
| Comandos ejecutados | `cat`, `diff`, `grep`, `ls`, `ps`, `ss`, `systemctl`, `docker ps`, `curl`, `git` |
| Archivos modificados | **NINGUNO** |
| Archivos creados | Ninguno |
| Servicios reiniciados | **NINGUNO** |
| Configuraciones cambiadas | **NINGUNA** |

**Detenerse. Esperar autorización humana.**

---

## 7. DECLARACIÓN DE LIMITES

Esta auditoría NO autoriza ni implementa ningún cambio. Todos los comandos en la Sección 5 son referenciales y requieren autorización explícita del operador antes de ejecución. La auditoría documenta el estado actual del sistema y propone un camino de remediación, pero cualquier acción mutante requiere aprobación humana.
