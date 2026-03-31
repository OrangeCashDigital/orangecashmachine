# OrangeCashMachine 🟢

Pipeline profesional de ingestión y procesamiento de datos de mercado para trading algorítmico de criptoactivos.

[![Python](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/)
[![Prefect](https://img.shields.io/badge/prefect-2.19-blue.svg)](https://www.prefect.io/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

---

## ¿Qué hace?

Descarga, procesa y almacena datos OHLCV históricos y en tiempo real desde múltiples exchanges (Bybit, KuCoin, KuCoin Futures) en un data lake por capas (Bronze → Silver → Gold), listo para estrategias de trading e investigación.

## Arquitectura

```
core/
    config/         → RunConfig, paths, env_vars, lineage
    logging/        → Setup estructurado con loguru
    utils.py        → repo_root() — anchor del repositorio
infra/
    monitoring/     → Prometheus, Grafana, Alertmanager
    observability/  → Servidor de métricas Prometheus
    state/          → Cursor store Redis
market_data/
    adapters/       → Clientes de exchanges y data providers
    ingestion/      → Fetchers REST y WebSocket
    processing/     → Pipelines: backfill, incremental, repair
    quality/        → Validación y políticas de calidad de datos
    storage/        → Bronze / Silver / Gold
    observability/  → Métricas de dominio
    orchestration/  → Flows y tasks Prefect
data_platform/
    loaders/        → MarketDataLoader, GoldLoader — acceso al Data Lake
    ohlcv_utils.py  → safe_symbol, normalize_ohlcv_df
    data_lake/      → Bronze / Silver / Gold (datos en disco)
config/             → YAML por entorno: base, development, production
trading/            → Estrategias y ejecución (en desarrollo)
backtesting/        → Motor de backtesting (en desarrollo)
research/           → Notebooks exploratorios (no instalable como paquete)
```

---

## Requisitos

- Python 3.11+
- Redis 6+
- [uv](https://github.com/astral-sh/uv) (recomendado) o pip

---

## Setup rápido

```bash
# 1. Clonar
git clone https://github.com/OrangeCashDigital/orangecashmachine.git
cd orangecashmachine

# 2. Instalar dependencias
uv sync
# pip install -r requirements.txt   # alternativa

# 3. Configurar entorno
cp .env.example .env
# Editar .env con tus API keys

# 4. Levantar servicios
docker compose up -d redis prefect-server pushgateway

# 5. Ejecutar
uv run python main.py
```

---

## Modos de ejecución

| Modo | Descripción | Comando |
|---|---|---|
| Normal | Ingesción incremental + backfill | `uv run python main.py` |
| Validación | Verifica config sin ejecutar pipeline | `VALIDATE_ONLY=1 uv run python main.py` |
| Docker | Stack completo | `docker compose up` |

### Variables de entorno clave

| Variable | Descripción | Default |
|---|---|---|
| `OCM_ENV` | Entorno activo (`development`/`production`/`test`) | `development` |
| `OCM_DATA_LAKE_PATH` | Path absoluto al Data Lake (Bronze + Silver) | *(lee YAML)* |
| `OCM_GOLD_PATH` | Path absoluto a Gold si difiere del Data Lake | *(lee YAML)* |
| `LOCAL_DATA_LAKE_PATH` | Path relativo al Data Lake en dev local | `data_platform/local_data_lake` |
| `REDIS_HOST` | Host Redis | `localhost` |
| `REDIS_PORT` | Puerto Redis | `6379` |
| `VALIDATE_ONLY` | Solo validar config, sin ejecutar pipeline | `false` |

Cascada de resolución de paths:
1. Variable de entorno explícita (`OCM_DATA_LAKE_PATH`)
2. `LOCAL_DATA_LAKE_PATH` interpolada en el YAML de entorno
3. Default hardcodeado en `config/base.yaml`

Ver `.env.example` para la lista completa.

---

## Acceso al Data Lake

Los loaders abstraen el path resolution y la lectura de Parquet:

```python
from data_platform.loaders.market_data_loader import MarketDataLoader
from data_platform.loaders.gold_loader import GoldLoader

# Silver — datos OHLCV limpios
loader = MarketDataLoader(exchange="kucoin")
df = loader.load_ohlcv("BTC/USDT", "1h")

# Gold — features procesados
gold = GoldLoader(exchange="kucoin")
df_gold = gold.load("BTC/USDT", "1h")
```

El path base se resuelve automáticamente desde `OCM_DATA_LAKE_PATH`
o `LOCAL_DATA_LAKE_PATH`. No hay paths hardcodeados en el código.

---

## Lineage

Cada escritura al Data Lake registra trazabilidad reproducible:

```python
from core.config.lineage import build_lineage

rec = build_lineage(
    run_id="20260330T000000-abc12345",
    version_id="v000042",
    layer="silver",
    exchange="kucoin",
)
print(rec.to_manifest())
# {
#   "run_id": "20260330T000000-abc12345",
#   "version_id": "v000042",
#   "git_hash": "fd2aef9",
#   "written_at": "2026-03-30T19:28:13+00:00",
#   "layer": "silver",
#   "exchange": "kucoin"
# }
```

`git_hash` y `written_at` se capturan automáticamente en el momento de la llamada.

---

## Stack de observabilidad

Con `docker compose up` se levantan:

- **Prefect UI** → http://localhost:4200
- **Prometheus** → http://localhost:9090
- **Grafana** → http://localhost:3000
- **Pushgateway** → http://localhost:9091
- **Alertmanager** → http://localhost:9093

---

## Tests

```bash
pytest tests/
```

---

## Entornos de configuración

El sistema carga configuración en capas:

```
config/base.yaml → config/<env>.yaml → config/settings.yaml
```

Entornos disponibles: `development`, `production`, `test`.

---

## Contribuir

1. Crear rama desde `main`
2. Commits en formato [Conventional Commits](https://www.conventionalcommits.org/)
3. Pull request con descripción del cambio
