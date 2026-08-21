# Auditoría Arquitectónica y de Runtime — Infraestructura, Systemd y Market Data Platform (OCM)

**Fecha:** 2026-08-20
**Rol:** Arquitecto Senior de Infraestructura, Runtime y Software Architecture
**Protocolo:** AUDIT_PROTOCOL.md (Read-Only, escritura restringida a `docs/audits/`)
**Archivo de Registro:** `docs/audits/AUDIT_OCM_infrastructure-runtime-architecture_2026-08-20.md`

---

## 1. OBJETIVO PRINCIPAL Y ALCANCE

Esta auditoría resuelve la ambigüedad operativa y de nomenclatura entre el paquete de dominio `market_data`, el servicio systemd `ocm-streaming.service`, los procesos batch y el banner/dashboard de estado.

### Conclusión Preliminar
1. **`market-data` no es solo el streaming realtime.** Es una plataforma de datos completa (`Market Data Platform`, ADR-0014) que engloba ingestión en tiempo real (WebSocket), ingestión periódica/batch (REST polling, backfills), normalización a un modelo de eventos canónico, control de calidad y materialización en almacenamiento lakehouse (Bronze/Silver/Gold sobre Iceberg vía Kafka como SSOT operacional).
2. **`ocm-streaming.service`** es un proceso persistente específico (canary F2.6b) que ejecuta exclusivamente el streaming de libro de órdenes L2 (`CryptofeedOrderBookStream` / `streaming_hydra.py`). **No representa a todo el paquete `market_data`.**
3. El supuesto conflicto entre `ocm-streaming.service active` y un banner que muestra `market-data detenido` / `trading detenido` **no es un fallo técnico del runtime**, sino un **error de semántica y supervisión**: el banner evalúa capacidades globales o servicios batch/trading que no tienen daemons systemd activos, mientras que systemd solo supervisa el canary de streaming.

---

## 2. SEGUNDA FASE: AUDITORÍA DE CÓDIGO Y ENTRYPOINTS

Inspección exhaustiva de los componentes ejecutables (`[project.scripts]` en `pyproject.toml` y entrypoints directos):

| Entrypoint / Módulo | Comando CLI / Invocación | Rol en la Arquitectura | Supervisor Actual | Tipo |
|---------------------|--------------------------|------------------------|-------------------|------|
| `ocm` | `app.cli.main:main` | Pipeline batch/histórico OHLCV (Hydra) | Manual / Cron | Batch / Oneshot |
| `streaming` | `app.cli.streaming_hydra:main` | Canary WebSocket L2 Orderbook (F2.6b) | **systemd** (`ocm-streaming.service`) | Daemon persistente |
| `market_data.main` | `python -m market_data.main` | Microservicio FastAPI (`:8001`), ingestion loop REST + `KafkaBronzeWriter` (Kappa stream processor) + `FeedOrchestrator` | Docker Compose (profile `microservices`) / Manual | Daemon HTTP + background loops |
| `live` | `app.cli.live_hydra:main` | Live trading con capital real (`⚠️ capital real`) | Manual | CLI iterativo por ciclo |
| `paper` | `app.cli.paper_hydra:main` | Paper trading con `PaperExecutor` | Manual | CLI iterativo por ciclo |
| `ocm-api` | `api.main:serve` | FastAPI gateway experimental | Manual | HTTP Gateway |

---

## 3. TERCERA FASE: AUDITORÍA DE ADRs Y DOCUMENTACIÓN

Se contrastó el estado real del código contra la serie canónica de ADRs:
- **ADR-0013 / ADR-0014 (Market Data Platform):** Establecen que `market_data` posee toda la adquisición (streaming y no-streaming) con un único composition root (BC-38). La bifurcación interna en capacidades (`realtime_feeds`, `external_ingestion`, `normalization`, `data_quality`) es conceptual; el transporte nunca fragmenta el dominio.
- **ADR-0022 (Lifecycle de `realtime_feeds`):** Define el diseño del entrypoint de streaming y su separación de `trading`. Especifica que `streaming_hydra.py` debe supervisarse como unidad systemd independiente utilizando espera activa de señales (`asyncio.Event` + `loop.add_signal_handler`), lo cual fue implementado correctamente en `ocm-streaming.service`.
- **ADR-0024 (Dirección hacia microservicios):** Fija tres niveles de madurez. `market-data` está en Nivel 1 (ejecutable autónomo, `market_data.main`). `trading` y `portfolio` están en Nivel 2 (dominio y composition root embebidos y ejecutados por `live`/`paper`, sin microservicio HTTP independiente).
- **ADR-0037 (CD Verify, Deploy & Rollback):** Establece la necesidad de artefactos inmutables con digest SHA256 y scripts de verificación post-deploy (`deploy_ocm.sh`).

---

## 4. CUARTA FASE: MAPA DE `market_data`

Análisis estructural del paquete `packages/market_data/`:

### A. Componentes Realtime (`realtime_feeds`)
- **Websockets:** `CryptofeedOrderBookStream` (Bybit, KuCoin - canal L2_BOOK), `BybitCryptofeedRunner` (TRADES).
- **Producers Kafka:** `OrderBookKafkaProducer`, `FundingKafkaProducer`, `OIKafkaProducer`, `LiquidationsKafkaProducer` (publican a `orderbook.raw`, `funding.raw`, `oi.raw`, `liquidations.raw` con headers Kappa `x-ocm-source` y `x-ocm-domain`).
- **Orquestador:** `FeedOrchestrator` (gestiona múltiples adapters WS según `ingestion_mode: websocket | dual | rest`).

### B. Componentes Batch / Ingestión Periódica (`external_ingestion` y OHLCV)
- **Fetchers REST:** `OHLCVFetcher`, `TradesFetcher`, `DerivativesFetcher`, `GapRecoveryFetcher`.
- **Orquestador Batch:** `PipelineOrchestrator`, `OHLCVPipeline`, `TradesPipeline`.
- **Fuentes Externas:** `CoinglassPollingSource`, `CoinMarketCapPollingSource`.
- **Kappa Stream Processor:** `KafkaBronzeWriter` (consume `ohlcv.raw` → escribe a Bronze Iceberg con dedup L2 en Redis).

### C. Componentes Compartidos / Núcleo
- **Dominio:** Entidades de velas, trades, order book, excepciones.
- **Puertos (Ports):** Interfaces outbound e inbound (`ExchangeAdapter`, `StorageFactoryPort`, `KafkaProducerPort`, etc.).
- **Normalización:** `normalization.py` (SSOT de transforms para persistencia).

---

## 5. QUINTA FASE: MATRIZ DE PERSISTENCIA Y SUPERVISIÓN DE `market_data`

| Componente / Subsistema | Tipo | Persistente (Daemon) | Supervisor Adecuado | Batch / Job | Realtime | Dependencias Críticas |
|-------------------------|------|----------------------|---------------------|-------------|----------|-----------------------|
| **Streaming Orderbook (F2.6b)** | Servicio | **Sí** | **systemd** (`ocm-streaming@.service`) | No | Sí | Red, Kafka, Bybit WS |
| **Market-Data Service FastAPI (`market_data.main`)** | Servicio HTTP + Loops | **Sí** | **systemd** o **Docker** (`market-data`) | Sí (Ingestion Loop) | Sí (FeedOrchestrator) | Red, Kafka, Redis, Iceberg Storage |
| **Kafka Bronze Writer** | Stream Consumer | **Sí** (integrado en `market_data.main`) | Supervisado por FastAPI lifespan | No | Sí (Kafka consume) | Kafka, Iceberg Storage |
| **Pipeline Histórico / Backfill (`ocm`)** | CLI Batch | No | **systemd Timer** / Cron / Job | Sí | No | CCXT Exchange API, Iceberg Storage |
| **External Ingestion Polling** | Polling Loop | **Sí** (integrado en `market_data.main` o dedicado) | **systemd** / Cron / Worker | No (Intervalo) | No | APIs externas (Coinglass, etc.), Kafka |

---

## 6. SEXTA Y SÉPTIMA FASE: AUDITORÍA DE `deploy/systemd/` Y `ocm-streaming.service`

### Análisis de `deploy/systemd/ocm-streaming.service`
- **¿Es correcto?** Sí, en su alcance actual (supervisar el canary ORDERBOOK).
- **¿Es incompleto?** Sí, no está parametrizado y carece de health checks avanzados (`WatchdogSec`, `Type=notify`).
- **¿Problema de nomenclatura?** Sí. Se llama `ocm-streaming.service`, lo cual induce a creer que supervisa *todo* el streaming o todo `market_data`, cuando en realidad solo ejecuta el entrypoint `streaming` (`streaming_hydra.py`).

### Acoplamiento al Host (Problemas de Portabilidad)
El archivo actual está rígidamente acoplado al entorno de desarrollo del host `orangehouse`:
```ini
User=orangemusic
WorkingDirectory=/home/orangemusic/trading/orangecashmachine
EnvironmentFile=/home/orangemusic/trading/orangecashmachine/.env
ExecStart=/home/orangemusic/trading/orangecashmachine/.venv/bin/streaming --env production
```
*Impacto:* No se puede desplegar en otro servidor sin editar manualmente paths absolutos y nombres de usuario.

---

## 8. DIAGNÓSTICO DEL PROBLEMA CRÍTICO (El Banner vs Systemd)

La aparente contradicción:
```
OCM — ESTADO DEL SISTEMA
○ market-data detenido
○ trading detenido
─────────────────────────
ocm-streaming.service active (running)
```
Se explica por:
1. **Desacople de dominios:** `ocm-streaming.service` solo conoce al proceso Python del canary de orderbook. No supervisa el microservicio HTTP de market-data (`market_data.main`), ni el engine de trading.
2. **Definición errónea de salud en el banner:** Un banner/healthcheck que verifique "market-data" probablemente está intentando consultar un proceso HTTP (ej. `http://localhost:8001/health` del microservicio `market-data.main` o un contenedor Docker `ocm_market_data`) o buscando procesos específicos (`python -m market_data.main`), los cuales no están corriendo ni tienen unidades systemd activas en ese momento.
3. **Confusión conceptual:** Se confundió la capacidad arquitectónica `market_data` con el único proceso daemon activo (`ocm-streaming`).

---

## 9. DISEÑO OBJETIVO — Arquitectura de Runtime y Despliegue

```
OCM Runtime Target
│
├── Infrastructure (Docker Compose)
│      ├── Kafka + Zookeeper
│      ├── Redis
│      └── Observability (Prometheus, Grafana, Alertmanager, Pushgateway, Loki)
│
├── Realtime Layer (systemd units templateados)
│      ├── ocm-streaming@bybit.service  (Canary / Feeds WS)
│      └── ocm-market-data-service.service (Microservicio FastAPI :8001)
│
├── Batch Layer (systemd Timers + Oneshot services)
│      ├── ocm-pipeline-batch.service + .timer (Ingebación OHLCV periódica)
│      └── ocm-backfill.service (Bajo demanda)
│
└── Trading & Portfolio Layer (Embedded in Runtime / Daemons futuros)
       └── ocm-trading-daemon.service (Nivel 1 futuro)
```

---

## 10. ESTRATEGIA DE PORTABILIDAD (Despliegue Universal)

Para eliminar rutas hardcodeadas (`/home/orangemusic/...`), se adopta el estándar de **Templates Systemd con Environment Files de Host**:
1. El repositorio incluye plantillas en `deploy/systemd/templates/*.service.template`.
2. Un script de aprovisionamiento (`deploy/scripts/install_systemd.sh`) lee un archivo de configuración de host (`/etc/ocm/host.env` o variables de entorno) y renderiza las unidades finales en `/etc/systemd/system/`.

---

## 11. SEPARACIÓN DE RESPONSABILIDADES: SYSTEMD VS OCM

- **Systemd:** L1 Process Health (¿El PID vive?, restart en crash, señales SIGTERM/SIGINT, límites de recursos, logging en journald).
- **OCM (Aplicación):** L2 Dependency Health (Kafka/Redis ping), L3 Data Health (Staleness, rate de eventos), L4 Pipeline Health (Consumer lag, calidad de datos), L5 Business Health (Trading execution guards).

---

## 12. PROPUESTA DE CAMBIOS (SIN EJECUTAR)

### A. Estructura Propuesta para `deploy/`
```
deploy/
├── systemd/
│   ├── templates/
│   │   ├── ocm-streaming@.service.template
│   │   ├── ocm-market-data.service.template
│   │   └── ocm-pipeline-batch.service + .timer
│   └── ocm-realtime.target
├── docker/
│   ├── docker-compose.yml
│   └── host.env.example
└── scripts/
    ├── install_systemd.sh
    └── health_check.sh
```

### B. Plantilla Propuesta: `deploy/systemd/templates/ocm-streaming@.service.template`
```ini
[Unit]
Description=OrangeCashMachine Streaming Feeds — %i
After=network-online.target ocm-kafka.service
Wants=network-online.target
PartOf=ocm-realtime.target

[Service]
Type=simple
User=${OCM_USER}
WorkingDirectory=${OCM_REPO_ROOT}
Environment=PYTHONUNBUFFERED=1
Environment=OCM_ENV=production
Environment=KAFKA_BOOTSTRAP_SERVERS=${OCM_KAFKA_BOOTSTRAP}
EnvironmentFile=${OCM_REPO_ROOT}/.env
ExecStart=${OCM_VENV_PATH}/bin/streaming --env production --exchange %i
Restart=on-failure
RestartSec=10
StartLimitIntervalSec=60
StartLimitBurst=3
KillSignal=SIGTERM
TimeoutStopSec=30

[Install]
WantedBy=ocm-realtime.target
```

### C. Plantilla Propuesta: `deploy/systemd/templates/ocm-market-data.service.template`
```ini
[Unit]
Description=OrangeCashMachine Market Data Platform API & Ingestion Service
After=network-online.target ocm-kafka.service ocm-redis.service
Wants=network-online.target

[Service]
Type=simple
User=${OCM_USER}
WorkingDirectory=${OCM_REPO_ROOT}
Environment=PYTHONUNBUFFERED=1
Environment=OCM_ENV=production
Environment=MARKET_DATA_PORT=8001
EnvironmentFile=${OCM_REPO_ROOT}/.env
ExecStart=${OCM_VENV_PATH}/bin/python -m market_data.main
Restart=on-failure
RestartSec=15
KillSignal=SIGTERM
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
```

---

## 13. PLAN DE IMPLEMENTACIÓN RECOMENDADO

1. **Fase 1 (Infraestructura base):** Corregir conflicto de puertos (Alertmanager 9093 vs Kafka) y levantar el stack de observabilidad mediante Docker Compose.
2. **Fase 2 (Portabilidad de Systemd):** Crear la carpeta `deploy/systemd/templates/`, redactar las plantillas y el script `install_systemd.sh`.
3. **Fase 3 (Unificación de Health Checks):** Crear un script unificado de salud (`deploy/scripts/health_check.sh`) que consulte el endpoint HTTP `/health` de `market-data.main`, el estado de systemd para streaming, y la conectividad de Kafka/Redis. El banner/dashboard de OCM debe consumir este script como fuente de verdad única para evitar falsos positivos de "detenido".

---
*Fin de la auditoría arquitectónica y de runtime.*