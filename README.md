# OrangeCashMachine 🟠

Data lakehouse pipeline para ingestión, procesamiento y almacenamiento de datos de mercado de criptoactivos. Soporta OHLCV histórico e incremental desde múltiples exchanges con arquitectura medallion (Bronze → Silver → Gold), orquestación Prefect, configuración Hydra, y observabilidad Prometheus/Grafana.

[![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12-blue.svg)](https://www.python.org/)
[![Prefect](https://img.shields.io/badge/prefect-2.19-blue.svg)](https://www.prefect.io/)
[![Hydra](https://img.shields.io/badge/hydra-1.3-lightblue.svg)](https://hydra.cc/)
[![ccxt](https://img.shields.io/badge/ccxt-4.3-orange.svg)](https://github.com/ccxt/ccxt)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)


---

## Arquitectura

```
app/
    cli/                → Entrypoints CLI: market_data (Hydra), live, paper
    use_cases/          → Casos de uso: execute_live, execute_paper, rebalance

ocm_platform/           → Plataforma transversal — sin dependencias de negocio (BC-06)
    config/             → AppConfig, env_vars (SSOT), paths, hydra_loader, schema Pydantic
    config/layers/      → Pipeline de config: coercion → env_override → validation
    config/loader/      → YamlLoader, env_resolver, snapshot, excepciones
    config/structured/  → Structured Configs Hydra (pipeline, observability)
    observability/      → bootstrap_logging, configure_logging, sinks, filtros (Loguru)
    infra/observability/→ Prometheus server, MetricsRuntime, adaptadores
    infra/state/        → CursorStore Redis, GapRegistry, lateness calibration
    runtime/            → RunConfig, RuntimeContext (inmutables), lineage, run registry

market_data/            → Dominio de datos de mercado — autocontenido (BC-02)
    adapters/exchange/  → CCXTAdapter, resiliencia, throttle adaptativo, circuit breaker
    adapters/data_providers/ → CoinGlass, CoinMarketCap
    ingestion/rest/     → OHLCVFetcher, TradesFetcher, DerivativesFetcher
    ingestion/websocket/→ WebSocket manager, orderbook stream, trades stream
    processing/strategies/  → BackfillStrategy, IncrementalStrategy, RepairStrategy
    processing/pipelines/   → OHLCVPipeline, ResamplePipeline, TradesPipeline
    processing/utils/   → gap_utils, grid_alignment, timeframe helpers
    quality/            → Validación Pandera, políticas, invariantes, schemas
    storage/bronze/     → BronzeStorage — Parquet raw con retención
    storage/silver/     → SilverStorage — Parquet limpio con manifiestos de versión
    storage/gold/       → GoldStorage, FeatureEngineer — features procesados
    storage/iceberg/    → SqlCatalog singleton, schemas, particiones, bootstrap
    safety/             → ExecutionGuard (kill switch), EnvironmentValidator
    observability/      → Métricas de dominio Prometheus
    orchestration/flows/→ batch_flow (spot/futures secuencial), resample_flow
    orchestration/tasks/→ batch_tasks, exchange_tasks
    orchestration/entrypoint.py → Composition root
    streaming/          → WebSocket consumer, publisher, router, dedup
    ports/              → Interfaces: exchange, storage, state, gap_registry

data_platform/          → Acceso de lectura al Data Lake
    loaders/            → MarketDataLoader (Silver), GoldLoader (Gold)
    ohlcv_utils.py      → safe_symbol, normalize_ohlcv_df

domain/                 → Entidades y eventos de dominio (DDD)
    entities/           → Entidades base
    events/             → OrderEvents, PositionEvents, RebalanceEvents
    value_objects/      → Signal

trading/                → Motor de trading paper + live ⚠️ en desarrollo activo
    strategies/         → BaseStrategy, EMA Crossover, registry
    execution/          → OMS, LiveExecutor, PaperExecutor, PaperBot, Order
    risk/               → RiskManager, modelos de riesgo
    analytics/          → TradeTracker, TradeRecord, performance
    engine.py           → Motor principal

portfolio/              → Gestión de posiciones y rebalanceo
    services/           → PortfolioService, RebalanceService
    models/             → Position
    infra/              → RedisPositionStore, MemoryStore

research/               → Acceso a datos para notebooks (no instalable como paquete)

config/                 → YAML de configuración Hydra
    config.yaml         → Raíz: defaults list (compose chain)
    base.yaml           → Defaults globales seguros (dry_run=true)
    env/                → development.yaml, production.yaml, test.yaml
    exchanges/          → bybit.yaml, kucoin.yaml, kucoinfutures.yaml
    pipeline/           → historical.yaml, realtime.yaml, resample.yaml
    observability/      → logging.yaml, metrics.yaml
    storage/datalake.yaml → SSOT del path anchor (OCM_STORAGE__DATA_LAKE__PATH)
    risk/risk.yaml      → Parámetros de riesgo operacional
    datasets.yaml       → Feature flags por dataset
    features.yaml       → Feature flags experimentales
```

---

## Flujo de ejecución

```
./run.sh ocm [args]
  └── app/cli/market_data.py                     (Hydra entrypoint)
        ├── bootstrap_logging()                  logging mínimo pre-config
        ├── RunConfig.from_env()                 RunConfig inmutable (env, run_id, debug)
        ├── load_appconfig_from_hydra(cfg)        AppConfig validado por Pydantic
        ├── EnvironmentValidator.check()          Fail-Fast: coherencia env + credenciales
        └── run_application(config, run_cfg)
              ├── configure_logging()             logging estructurado completo
              ├── init_metrics_runtime()          Prometheus fail-soft
              └── pipeline_runner(ctx, pusher)
                    └── market_data/orchestration/entrypoint.py
                          └── batch_flow
                                ├── por exchange (bybit, kucoin, kucoinfutures)
                                │     ├── spot  → OHLCVPipeline (incremental | backfill | repair)
                                │     └── futures → OHLCVPipeline (incremental | backfill | repair)
                                └── resample_flow (1m → 5m, 15m, 1h, 4h, 1d)
```

### Composition Root

`RuntimeContext` es inmutable y se construye exactamente una vez en `run_application`.
Contiene `AppConfig` + `RunConfig` + `started_at`. Ningún flow ni pipeline reconstruye
configuración — la recibe inyectada.

### Modos de pipeline por exchange

| Modo | Descripción | Activación |
|---|---|---|
| `incremental` | Descarga desde el último cursor conocido | Default |
| `backfill` | Descarga histórico desde `start_date` hacia atrás | `pipeline.historical.backfill_mode=true` |
| `repair` | Detecta y rellena gaps en Bronze/Silver | Automático post-backfill |

---

## Configuración

La configuración se compone en capas via Hydra. El orden de precedencia es de menor a mayor:

```
config/base.yaml
  -> config/exchanges/{exchange}.yaml
  -> config/pipeline/{module}.yaml
  -> config/observability/{module}.yaml
  -> config/storage/datalake.yaml
  -> config/datasets.yaml
  -> config/risk/risk.yaml
  -> config/features.yaml
  -> config/env/{env}.yaml
  -> CLI overrides
```

Para inspeccionar el config efectivo sin ejecutar el pipeline:

```
./run.sh ocm --cfg job
./run.sh ocm --cfg job env=production
```

### Variables de entorno

Las variables de entorno tienen prioridad sobre el YAML via `apply_env_overrides()` (L2).
El separador `__` mapea a la jerarquía del schema: `OCM_SECTION__KEY=valor`.

| Variable | Descripción | Default |
|---|---|---|
| `OCM_ENV` | Entorno activo | `development` |
| `OCM_DEBUG` | Logging verboso | `false` en prod |
| `OCM_VALIDATE_ONLY` | Valida config y sale sin ejecutar | `false` |
| `OCM_STORAGE__DATA_LAKE__PATH` | Path absoluto al Data Lake (SSOT) | *(lee YAML)* |
| `OCM_GOLD_PATH` | Override del Gold layer | *(derivado)* |
| `REDIS_HOST` | Host Redis | `localhost` |
| `REDIS_PORT` | Puerto Redis | `6379` |
| `REDIS_PASSWORD` | Password Redis | `""` |
| `PUSHGATEWAY_URL` | URL Prometheus Pushgateway | — |
| `LOG_LEVEL` | Nivel de log en producción | `INFO` |

### Resolución del path del Data Lake

1. OCM_STORAGE__DATA_LAKE__PATH (env var)  -- maxima prioridad, L2-aligned
2. storage.data_lake.path (YAML)           -- configurable por entorno via Hydra
3. repo_root()/data_platform/data_lake     -- fallback estructural seguro

### Entornos disponibles

| Entorno | Descripción | `dry_run` | `backfill_mode` |
|---|---|---|---|
| `development` | Debug activo, escribe solo si se fuerza explícitamente | `true` | `false` |
| `production` | Opera real, credenciales requeridas, paths de sistema | `false` | `false` |
| `test` | CI, datos aislados en `test_data_lake`, Redis deshabilitado | `true` | `false` |

### SafeOps

`dry_run: true` es el default global en `base.yaml`. Producción lo sobrescribe
explícitamente a `false`. Nunca se puede llegar a producción por omisión.


---

## Requisitos

- Python 3.11 o 3.12
- Redis 6+
- [uv](https://github.com/astral-sh/uv)

---

## Setup

```
# 1. Clonar
git clone https://github.com/orangemusic/orangecashmachine.git
cd orangecashmachine

# 2. Instalar dependencias
uv sync

# 3. Configurar entorno
cp .env.example .env
# Editar .env: API keys, REDIS_HOST, OCM_STORAGE__DATA_LAKE__PATH

# 4. Levantar servicios
docker compose up -d redis prefect-server pushgateway

# 5. Ejecutar
./run.sh ocm
```

## Modos de ejecución

| Comando | Descripción |
|---|---|
| `./run.sh ocm` | Ingestión incremental (development) |
| `./run.sh ocm env=production` | Ingestión en producción |
| `./run.sh ocm pipeline.historical.backfill_mode=true` | Backfill histórico |
| `./run.sh ocm --cfg job` | Inspeccionar config efectivo sin ejecutar |
| `OCM_VALIDATE_ONLY=1 ./run.sh ocm` | Validar config y salir |
| `./run.sh live` | Live trading (capital real) |
| `./run.sh paper` | Paper trading |
| `./run.sh deploy` | Prefect deployment runner |
| `docker compose up` | Stack completo con observabilidad |


---

## Observabilidad

Con `docker compose up` se levantan automáticamente:

| Servicio | URL | Descripción |
|---|---|---|
| Prefect UI | http://localhost:4200 | Monitoreo de flows y runs |
| Prometheus | http://localhost:9090 | Métricas de sistema y pipeline |
| Grafana | http://localhost:3000 | Dashboards (26 paneles precargados) |
| Pushgateway | http://localhost:9091 | Push de métricas desde jobs batch |
| Alertmanager | http://localhost:9093 | Routing de alertas |

El pipeline expone métricas Prometheus via `market_data/observability/metrics.py`.
Los dashboards están provisionados en `infra/grafana/dashboards/` y se cargan automáticamente.

Logging estructurado via Loguru con tres sinks: consola, `logs/errors_{date}.log` (WARNING+),
y `logs/pipeline_{date}.log` (contexto `bind_pipeline()` requerido).


---

## Acceso al Data Lake

Los loaders abstraen path resolution y lectura de Parquet. No hay paths hardcodeados en el código.

```
from data_platform.loaders.market_data_loader import MarketDataLoader
from data_platform.loaders.gold_loader import GoldLoader

# Silver — datos OHLCV limpios
loader = MarketDataLoader(exchange="kucoin")
df = loader.load_ohlcv("BTC/USDT", "1h")

# Gold — features procesados
gold = GoldLoader(exchange="kucoin")
df_gold = gold.load("BTC/USDT", "1h")
```

---

## Lineage

Cada escritura al Data Lake registra trazabilidad reproducible via `ocm_platform/runtime/lineage.py`.
`git_hash` y `written_at` se capturan automáticamente. Los manifiestos se almacenan en Silver
junto a cada versión de datos.


---

## Tests y tooling

```
# Tests
uv run pytest tests/

# Linting
uv run ruff check .

# Type checking
uv run mypy .

# Boundary contracts (import-linter)
uv run lint-imports
```

Los contratos de arquitectura están definidos en `pyproject.toml` y verificados por `import-linter`:

| Contrato | Regla |
|---|---|
| BC-01 | Ningún módulo externo importa `ccxt_adapter` directamente |
| BC-02 | `market_data` no importa `trading` ni `data_platform` |
| BC-03 | `trading.strategies` no conoce `execution` ni `analytics` |
| BC-04 | `trading.risk` no conoce `execution` |
| BC-06 | `ocm_platform` no tiene dependencias de negocio |
| BC-07 | `portfolio` aislado de `execution` y `strategies` |

---

## Contribuir

1. Crear rama desde `main`
2. Commits en formato [Conventional Commits](https://www.conventionalcommits.org/)
3. `uv run ruff check .` y `uv run lint-imports` antes de PR

