# OrangeCashMachine 🟠

Data lakehouse pipeline para ingestión, procesamiento y almacenamiento de datos de mercado de criptoactivos. Arquitectura medallion Bronze → Silver → Gold con Apache Iceberg, orquestación Dagster, configuración Hydra y observabilidad Prometheus / Grafana / Loki.

[![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue.svg)](https://www.python.org/)
[![Dagster](https://img.shields.io/badge/dagster-1.13-blue.svg)](https://dagster.io/)
[![Hydra](https://img.shields.io/badge/hydra-1.3-lightblue.svg)](https://hydra.cc/)
[![ccxt](https://img.shields.io/badge/ccxt-4.3-orange.svg)](https://github.com/ccxt/ccxt)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

-----

## Arquitectura lógica

El sistema se organiza en bounded contexts con dependencias unidireccionales verificadas
estáticamente por `import-linter` en cada CI run. Violar un contrato rompe el pipeline
antes de llegar a review.

### Bounded contexts

|Módulo                |Responsabilidad                                                                       |
|----------------------|--------------------------------------------------------------------------------------|
|`shared`              |Shared Kernel — tipos canónicos sin dependencias internas (BC-01)                     |
|`ocm`                 |Plataforma transversal: config, runtime, observabilidad. Sin lógica de negocio (BC-14)|
|`packages/market_data`|Bounded context de datos de mercado — autocontenido (BC-10)                           |
|`packages/trading`    |Motor de trading paper + live ⚠️ en desarrollo activo                                  |
|`packages/portfolio`  |Gestión de posiciones y rebalanceo                                                    |
|`infrastructure`      |Composition Root de Dagster + adaptadores Redis                                       |
|`apps/app`            |Entrypoints CLI (Hydra)                                                               |
|`apps/api`            |API Gateway experimental (FastAPI, JWT, rate limiting) ⚠️ WIP                          |
|`apps/research`       |Acceso a datos para notebooks — read-only, no instalable como paquete                 |

### Flujo de dependencias

Las dependencias fluyen **hacia adentro** — el dominio no conoce a nadie (BC-08):

```
shared (Shared Kernel)
  ↑
ocm (plataforma)
  ↑
packages/market_data/domain
  ↑
packages/market_data/ports          ← contratos (Protocols runtime_checkable)
  ↑
packages/market_data/application    ← use cases, strategies, pipelines
  ↑
packages/market_data/adapters       ← CCXTAdapter, fetchers HTTP/WS
  ↑
packages/market_data/infrastructure ← storage Iceberg/Parquet, Kafka, lineage
  ↑
infrastructure/dagster/assets       ← Composition Root (único punto de ensamblaje)
```

### Contratos de frontera verificados por import-linter

|ID   |Regla                                                                                      |
|-----|-------------------------------------------------------------------------------------------|
|BC-01|`shared` solo depende de stdlib y third-party                                              |
|BC-02|`market_data.exceptions` es stdlib-only                                                    |
|BC-03|`market_data.domain` aislado de capas externas (DIP)                                       |
|BC-04|`market_data.ports` solo depende del dominio propio                                        |
|BC-05|`market_data.application` aislado de infrastructure y adapters                             |
|BC-06|`market_data.adapters` aislado de infrastructure                                           |
|BC-07|`infrastructure` no importa `application` en nivel módulo                                  |
|BC-08|Dependencias fluyen hacia adentro: domain ← ports ← application ← adapters ← infrastructure|
|BC-09|`market_data.domain` no importa librerías de infraestructura (pyiceberg, redis, ccxt)      |
|BC-10|`market_data` no importa bounded contexts hermanos                                         |
|BC-11|`trading.strategies` aislado de execution y analytics                                      |
|BC-12|`trading.risk` aislado de execution                                                        |
|BC-13|`portfolio` aislado de trading execution y strategies                                      |
|BC-14|`ocm` sin dependencias de lógica de negocio                                                |
|BC-15|`infrastructure.dagster.assets` no bypasea la capa de ports                                |
|BC-16|`infrastructure` solo depende de plataforma y abstracciones de market_data                 |
|BC-17|`infrastructure.timeouts` es SSOT de timeouts — sin dependencia circular con iceberg       |
|BC-18|Ningún dominio importa la capa `api`                                                       |
|BC-19|Ningún dominio importa `research`                                                          |
|BC-20|`research` es consumidor read-only del gold layer                                          |

### Composition Root

`infrastructure/dagster/assets/` es el único punto autorizado para ensamblar use cases
con infraestructura concreta. Los assets construyen un `PipelineRequest` completo
(credentials, resilience, symbols, timeframes, start_date, auto_lookback_days) y lo
delegan a `PipelineOrchestrator` — ningún flow reconstruye configuración por su cuenta.

### Modos de pipeline

|Modo         |Descripción                                                        |Activación                              |
|-------------|-------------------------------------------------------------------|----------------------------------------|
|`incremental`|Descarga desde el último cursor conocido (Redis)                   |Default                                 |
|`backfill`   |Histórico desde `start_date` con paginación backward               |`pipeline.historical.backfill_mode=true`|
|`repair`     |Detecta y rellena gaps en Bronze/Silver (`fill_ratio` FULL/PARTIAL)|Automático post-backfill                |

### Exchange quirks

Comportamientos específicos por exchange centralizados en
`packages/market_data/domain/value_objects/exchange_quirks.py`:

|Quirk                |Exchanges afectados|
|---------------------|-------------------|
|`backward_pagination`|KuCoinFutures      |
|`requires_end_at`    |KuCoinFutures      |
|`reject_zero_since`  |KuCoin             |

-----

## Repository layout

```
orangecashmachine/
│
├── shared/                         # Shared Kernel — sin dependencias internas (BC-01)
│   ├── contracts/boundaries.py     # Definición de bounded contexts
│   ├── exceptions/
│   └── types/                      # OHLCVBar, Timeframe, Signal, eventos de dominio
│
├── ocm/                            # Plataforma transversal (BC-14)
│   ├── config/
│   │   ├── schema.py               # AppConfig — schema Pydantic
│   │   ├── env_vars.py             # SSOT de todas las variables OCM_*
│   │   ├── paths.py                # SSOT de resolución de paths
│   │   ├── credentials.py
│   │   ├── hydra_loader.py
│   │   ├── pipeline.py
│   │   ├── layers/                 # coercion → env_override → validation
│   │   ├── loader/                 # YamlLoader, env_resolver, snapshot, excepciones
│   │   └── structured/             # Structured Configs Hydra
│   ├── observability/              # Loguru: bootstrap, sinks, filtros, métricas, Prometheus
│   └── runtime/
│       ├── context.py              # RuntimeContext — inmutable, construido una vez
│       ├── run_config.py           # RunConfig.from_env()
│       ├── lineage.py              # git_hash, written_at
│       ├── environment_validator.py
│       ├── registry.py
│       └── state/                  # RedisCursorStore, InMemoryCursorStore, GapRegistry,
│                                   # lateness calibration, factories, encoding
│
├── packages/
│   ├── market_data/                # Bounded context de datos de mercado (BC-10)
│   │   ├── domain/
│   │   │   ├── entities/
│   │   │   ├── events/             # ingestion.py, _lineage.py
│   │   │   ├── exceptions/
│   │   │   ├── policies/           # base.py, repair.py
│   │   │   └── value_objects/      # candle, timeframe, gap_utils, grid_alignment,
│   │   │                           # exchange_quirks, ohlcv_batch, symbol
│   │   ├── ports/
│   │   │   ├── inbound/            # event_consumer, pipeline_trigger
│   │   │   └── outbound/           # exchange, storage, storage_factory, state,
│   │   │                           # gap_registry, lineage, metrics, observability,
│   │   │                           # kafka_producer, kafka_consumer, event_bus,
│   │   │                           # publisher, quality, throttle, feature_reader
│   │   ├── application/
│   │   │   ├── use_cases/          # pipeline_orchestrator, ohlcv_transformer,
│   │   │   │                       # resample_ohlcv, candle_normalizer
│   │   │   ├── pipelines/          # ohlcv_pipeline, resample_pipeline,
│   │   │   │                       # trades_pipeline, derivatives_pipeline, _worker_pool
│   │   │   ├── strategies/         # backfill, incremental, repair
│   │   │   └── consumers/          # base, quality_consumer
│   │   ├── adapters/
│   │   │   ├── inbound/
│   │   │   │   ├── rest/           # OHLCVFetcher, TradesFetcher, DerivativesFetcher
│   │   │   │   ├── websocket/      # WebSocket manager, streams
│   │   │   │   └── data_providers/ # CoinGlass, CoinMarketCap
│   │   │   └── outbound/
│   │   │       ├── exchange/       # CCXTAdapter, resiliencia, throttle adaptativo,
│   │   │       │                   # circuit breaker
│   │   │       └── storage/
│   │   ├── infrastructure/
│   │   │   ├── bootstrap/          # OCMContainer — DI interno
│   │   │   ├── storage/
│   │   │   │   ├── bronze/         # BronzeStorage — Parquet raw con retención
│   │   │   │   ├── silver/         # SilverStorage — Parquet limpio + manifiestos
│   │   │   │   ├── gold/           # GoldStorage — features procesados (Iceberg)
│   │   │   │   └── iceberg/        # SqlCatalog, schemas, particiones, CursorStore
│   │   │   ├── kafka/              # BronzeWriter, consumer, producer, dedup,
│   │   │   │                       # ohlcv_publisher, serializer, metrics, payloads
│   │   │   ├── lineage/tracker.py
│   │   │   ├── observability/      # métricas Prometheus del bounded context
│   │   │   ├── event_bus/          # in_memory.py
│   │   │   └── timeouts.py         # SSOT de todos los timeouts del sistema (BC-17)
│   │   ├── quality/                # Pandera: schemas, validators, policies,
│   │   │                           # invariants, anomaly_registry, pipeline
│   │   └── exceptions.py           # SSOT de jerarquía de errores del bounded context
│   │
│   ├── trading/                    # Motor de trading ⚠️ en desarrollo activo
│   │   ├── strategies/             # BaseStrategy, EMA Crossover, registry
│   │   ├── execution/              # OMS, LiveExecutor, PaperExecutor, PaperBot, Order
│   │   ├── risk/                   # RiskManager, modelos de riesgo
│   │   ├── analytics/              # TradeTracker, TradeRecord, performance
│   │   ├── data/gold_adapter.py
│   │   └── engine.py
│   │
│   └── portfolio/
│       ├── services/               # PortfolioService, RebalanceService
│       ├── models/position.py
│       ├── ports/position_store.py # Protocol
│       └── infra/                  # RedisPositionStore, MemoryStore
│
├── infrastructure/
│   ├── dagster/
│   │   ├── assets/                 # ← Composition Root externo
│   │   │   ├── bronze_ohlcv.py     # backfill + incremental
│   │   │   ├── repair_ohlcv.py     # repair de gaps
│   │   │   ├── resample_ohlcv.py   # 1m → 5m, 15m, 1h, 4h, 1d
│   │   │   ├── asset_checks.py
│   │   │   └── partitions.py
│   │   ├── defs.py
│   │   └── resources.py
│   └── redis/redis_stream.py
│
├── apps/
│   ├── app/
│   │   ├── cli/                    # main (Hydra), live, paper
│   │   └── use_cases/              # execute_live, execute_paper, rebalance
│   ├── api/                        # API Gateway experimental ⚠️ WIP
│   │   ├── auth/jwt.py
│   │   ├── middleware/             # logging, rate_limit
│   │   ├── routers/health.py
│   │   ├── main.py
│   │   └── settings.py
│   └── research/
│       └── data/data_access.py     # Acceso read-only al gold layer
│
├── data_platform/                  # Iceberg catalog + warehouse (lectura)
│   ├── iceberg_catalog/
│   └── iceberg_warehouse/
│
├── config/                         # YAML Hydra
│   ├── config.yaml                 # Raíz: defaults list
│   ├── base.yaml                   # Defaults globales (dry_run=true)
│   ├── env/                        # development, production, test
│   ├── exchanges/                  # bybit, kucoin, kucoinfutures
│   ├── pipeline/                   # historical, realtime, resample
│   ├── observability/              # logging, metrics
│   ├── storage/datalake.yaml       # SSOT del path anchor
│   ├── risk/risk.yaml
│   ├── datasets.yaml
│   └── features.yaml
│
├── deploy/
│   └── monitoring/                 # prometheus.yml, alerts.yml, alertmanager.yml,
│                                   # loki/loki.yml, promtail/promtail.yml
│
├── tests/                          # 439 tests — mirrors estructura de paquetes
│
├── dagster_defs.py                 # Contrato de framework (entry point fijo)
├── dagster.yaml
├── docker-compose.yml
├── docker-compose.override.yml
├── Dockerfile
└── pyproject.toml                  # Contratos BC-01..BC-20, ruff, mypy, pytest
```

-----

## Flujo de ejecución

```
dagster_defs.py                           (contrato de framework — posición fija)
  └── infrastructure/dagster/defs.py
        └── infrastructure/dagster/assets/   (Composition Root)
              ├── bronze_ohlcv    → PipelineOrchestrator → BackfillStrategy | IncrementalStrategy
              ├── repair_ohlcv    → PipelineOrchestrator → RepairStrategy
              └── resample_ohlcv  → ResamplePipeline (1m → 5m, 15m, 1h, 4h, 1d)

PipelineOrchestrator
  └── construye PipelineRequest (credentials, resilience, symbols,
      timeframes, start_date, auto_lookback_days)
        └── Strategy → CCXTAdapter → BronzeStorage → SilverStorage → GoldStorage
```

-----

## Configuración

La configuración se compone en capas via Hydra. Orden de precedencia de menor a mayor:

```
config/base.yaml
  → config/exchanges/{exchange}.yaml
  → config/pipeline/{module}.yaml
  → config/observability/{module}.yaml
  → config/storage/datalake.yaml
  → config/datasets.yaml / features.yaml / risk/risk.yaml
  → config/env/{env}.yaml
  → CLI overrides
  → Variables de entorno OCM_*__ (L2, máxima prioridad)
```

Inspeccionar config efectivo sin ejecutar:

```bash
./run.sh ocm --cfg job
./run.sh ocm --cfg job env=production
```

### Variables de entorno

Todas registradas en `ocm/config/env_vars.py` (SSOT). El separador `__` mapea a la
jerarquía del schema: `OCM_SECTION__KEY=valor`.

|Variable                      |Descripción                      |Default        |
|------------------------------|---------------------------------|---------------|
|`OCM_ENV`                     |Entorno activo                   |`development`  |
|`OCM_DEBUG`                   |Logging verboso                  |`false` en prod|
|`OCM_VALIDATE_ONLY`           |Valida config y sale sin ejecutar|`false`        |
|`OCM_STORAGE__DATA_LAKE__PATH`|Path absoluto al Data Lake (SSOT)|*(lee YAML)*   |
|`OCM_GOLD_PATH`               |Override del Gold layer          |*(derivado)*   |
|`REDIS_HOST`                  |Host Redis                       |`localhost`    |
|`REDIS_PORT`                  |Puerto Redis                     |`6379`         |
|`REDIS_PASSWORD`              |Password Redis                   |`""`           |
|`PUSHGATEWAY_URL`             |URL Prometheus Pushgateway       |—              |
|`LOG_LEVEL`                   |Nivel de log en producción       |`INFO`         |

### Resolución del path del Data Lake

```
1. OCM_STORAGE__DATA_LAKE__PATH  →  máxima prioridad (L2-aligned)
2. storage.data_lake.path (YAML) →  configurable por entorno via Hydra
3. repo_root()/data_platform/data_lake  →  fallback estructural seguro
```

### Entornos

|Entorno      |`dry_run`|Descripción                              |
|-------------|---------|-----------------------------------------|
|`development`|`true`   |Debug activo, escribe solo si se fuerza  |
|`production` |`false`  |Credenciales requeridas, paths de sistema|
|`test`       |`true`   |CI, datos aislados, Redis deshabilitado  |

**SafeOps:** `dry_run: true` es el default global en `base.yaml`. Producción lo
sobrescribe explícitamente. Nunca se llega a producción por omisión.

-----

## Requisitos

- Python ≥3.11
- Redis 6+
- Docker + Docker Compose
- [uv](https://github.com/astral-sh/uv)

-----

## Setup

```bash
# 1. Clonar
git clone https://github.com/OrangeCashDigital/orangecashmachine.git
cd orangecashmachine

# 2. Instalar dependencias
uv sync

# 3. Configurar entorno
cp .env.example .env
# Editar .env: API keys, REDIS_HOST, OCM_STORAGE__DATA_LAKE__PATH

# 4. Levantar servicios
docker compose up -d

# 5. Abrir Dagster UI
open http://localhost:3001
```

-----

## Observabilidad

Con `docker compose up` se levantan automáticamente:

|Servicio    |URL                  |Descripción                                                                           |
|------------|---------------------|--------------------------------------------------------------------------------------|
|Dagster UI  |http://localhost:3001|Orquestación, assets, runs, schedules                                                 |
|Prometheus  |http://localhost:9090|Métricas de sistema y pipeline                                                        |
|Grafana     |http://localhost:3000|Dashboards provisionados automáticamente desde deploy/monitoring/                     |
|Loki        |http://localhost:3100|Agregación de logs estructurados (Promtail)                                           |
|Pushgateway |http://localhost:9091|Push de métricas desde jobs batch                                                     |
|Alertmanager|http://localhost:9093|Routing de alertas (deadman switch `PipelineHeartbeatDead`)                           |

El pipeline expone métricas Prometheus vía
`packages/market_data/infrastructure/observability/metrics.py`.
Dashboards y alertas provisionados en `deploy/monitoring/` — se cargan automáticamente.

**Logging estructurado** vía Loguru con tres sinks:

|Sink                      |Nivel       |Condición                             |
|--------------------------|------------|--------------------------------------|
|Consola                   |configurable|siempre                               |
|`logs/errors_{date}.log`  |WARNING+    |siempre                               |
|`logs/pipeline_{date}.log`|DEBUG+      |requiere `bind_pipeline()` en contexto|

-----

## Acceso al Data Lake

`data_platform/iceberg_catalog/` y `data_platform/iceberg_warehouse/` son el catalog
SQLite y el warehouse Iceberg en disco — no son un paquete importable.

`GoldReader` es el adaptador de lectura Gold sobre Apache Iceberg.
Implementa `FeatureReaderPort` estructuralmente (duck typing — no hereda explícitamente).

```python
from market_data.adapters.outbound.storage.gold_reader import GoldReader

reader = GoldReader(exchange="kucoin")

# Snapshot actual (default)
df = reader.load_features("BTC/USDT", "spot", "1h")

# Time travel — snapshot reproducible en un instante dado
df = reader.load_features("BTC/USDT", "spot", "1h",
                           as_of="2026-03-17T22:40:00Z")

# Snapshot exacto por ID de versión
df = reader.load_features("BTC/USDT", "spot", "1h", version="123456789")

# Datasets disponibles para un exchange/market_type
datasets = reader.list_datasets("kucoin", "spot")

# Metadata del snapshot resuelto (retorna None ante error: Fail-Soft)
manifest = reader.get_manifest("kucoin", "BTC/USDT", "spot", "1h")
```

-----

## Lineage y trazabilidad

Cada escritura al Data Lake registra trazabilidad reproducible. `git_hash` y `written_at`
se capturan automáticamente vía `ocm/runtime/lineage.py`. Los manifiestos de versión se
almacenan en Silver junto a cada partición. El linaje persiste en `data/lineage/lineage.db`.

-----

## Tests y tooling

```bash
uv run pytest tests/          # 439 tests
uv run ruff check .           # linting
uv run mypy .                 # 0 errores genuinos
uv run lint-imports           # contratos BC-01..BC-20
uv run bandit .               # seguridad
```

`type: ignore` en el código requiere comentario explicativo — la presencia sin
justificación es deuda técnica explícita visible en el diff.

-----

## CI/CD

`.github/workflows/ocm-ci.yml` ejecuta en cada PR:

1. `uv run pytest tests/`
1. `uv run ruff check .`
1. `uv run mypy .`
1. `uv run lint-imports`
1. `uv run bandit .`

`.github/workflows/ocm-cd.yml` — 🚧 placeholder, pendiente de implementación
(`workflow_dispatch` manual, no automatizado).

-----

## Contribuir

1. Crear rama desde `main`
1. Commits en formato [Conventional Commits](https://www.conventionalcommits.org/)
1. Verificar antes del PR:

   ```bash
   uv run ruff check . && uv run lint-imports && uv run pytest tests/ -q
   ```
1. `mypy` debe reportar 0 errores genuinos
