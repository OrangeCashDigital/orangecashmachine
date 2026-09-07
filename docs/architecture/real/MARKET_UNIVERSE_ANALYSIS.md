# MARKET UNIVERSE — ANÁLISIS EXHAUSTIVO

> **Fecha**: 2026-09-04
> **Audit**: READ-ONLY contra código y configuración real

---

## 1. FUENTES DE VERDAD IDENTIFICADAS

### 1.1 `config/exchanges/bybit.yaml` — Exchange Config Base
```yaml
# @package _global_
exchanges:
  bybit:
    enabled: true
    resilience:
      limits:
        max_concurrency: 8
        max_rate: 15
      ...
```
- **Clave**: `exchanges.bybit.enabled: true`
- **Símbolos**: **NINGUNO** definido aquí
- **Consumidor**: `AppConfig.parse_exchanges()` → filtra `enabled: true` → `AppConfig.exchange_names`
- **Formato**: N/A (solo flag enabled)

### 1.2 `config/env/development.yaml` — Environment Override
```yaml
exchanges:
  bybit:
    enabled: true
    markets:
      spot:
        enabled: true
        symbols:
          - BTC/USDT
  kucoin:
    enabled: true
    markets:
      spot:
        enabled: true
        symbols:
          - BTC/USDT
```
- **Clave**: `exchanges.bybit.markets.spot.symbols`
- **Formato**: **CCXT nativo** — `BTC/USDT` (slash separator)
- **Consumidor**: `ConcretePipelineFactory._build_ohlcv()` → `exc_cfg.markets.spot_symbols`
- **Usado por**: `market_data.main` → OHLCV REST pipeline

### 1.3 `config/market_data/feeds.yaml` — Feeds Config (WebSocket)
```yaml
feeds:
  ingestion_mode: dual
  kafka:
    topic_trades: trades.raw
  feeds:
    bybit:
      enabled: true
      symbols:
        - BTC-USDT-PERP
        - ETH-USDT-PERP
        - SOL-USDT-PERP
    kucoin:
      enabled: false
      symbols:
        - BTC-USDT
        - ETH-USDT
```
- **Clave**: `feeds.feeds.bybit.symbols`
- **Formato**: **Cryptofeed nativo** — `BTC-USDT-PERP` (dash + PERP suffix)
- **Consumidor**: `streaming_hydra.py` → `config.feeds.feeds[exchange].symbols`
- **Usado por**: `ocm-streaming` → Orderbook WebSocket

---

## 2. QUIÉN CONSUME QUÉ

| Componente | Entry Point | Config Origen | Símbolos | Formato |
|------------|-------------|---------------|----------|---------|
| **market_data.main** (OHLCV) | `packages/market_data/main.py` | `config/env/development.yaml` → `exc_cfg.markets.spot_symbols` | `BTC/USDT` | CCXT (`BTC/USDT`) |
| **market_data.main** (WS trades opcional) | `CompositionRoot.build_feed_orchestrator()` | `config/market_data/feeds.yaml` → `feeds.feeds.bybit.symbols` | `BTC-USDT-PERP` | Cryptofeed |
| **streaming_hydra.py** (Orderbook) | `apps/app/cli/streaming_hydra.py` | `config/market_data/feeds.yaml` → `feeds.feeds.bybit.symbols` | `BTC-USDT-PERP`, `ETH-USDT-PERP`, `SOL-USDT-PERP` | Cryptofeed |

---

## 3. DISCREPANCIAS CRÍTICAS

### 3.1 Tres Fuentes, Un Exchange
```
Bybit
├── config/exchanges/bybit.yaml          → enabled: true (SIN símbolos)
├── config/env/development.yaml          → BTC/USDT (spot, CCXT format)
└── config/market_data/feeds.yaml        → BTC-USDT-PERP, ETH-USDT-PERP, SOL-USDT-PERP (cryptofeed format)
```

### 3.2 Dos Formatos Incompatibles
| Formato | Uso | Ejemplo | Parser |
|---------|-----|---------|--------|
| **CCXT** | REST polling | `BTC/USDT` | `SYMBOL_REGEX`: `^[A-Z0-9]+/[A-Z0-9]+(:[A-Z0-9]+)?$` |
| **Cryptofeed** | WebSocket | `BTC-USDT-PERP` | Hardcoded en `feeds.yaml` + cryptofeed internals |

### 3.3 Universos Diferentes por Pipeline
| Pipeline | Símbolos | Cuenta |
|----------|----------|--------|
| OHLCV REST (market_data.main) | `BTC/USDT` (spot only) | 1 |
| Orderbook WS (streaming_hydra) | `BTC-USDT-PERP`, `ETH-USDT-PERP`, `SOL-USDT-PERP` | 3 |

**Resultado**: REST ve 1 símbolo spot; WS ve 3 símbolos perpetual. **Data mismatch garantizado**.

---

## 4. AUTO_DISCOVER_SYMBOLS — CAMPO MUERTO

### En Schema (`ocm/config/schema.py:201`)
```python
class ExchangeConfig(StrictBaseModel):
    ...
    auto_discover_symbols: bool = False  # ← NUNCA LEÍDO
```

### En Código — Búsqueda exhaustiva
```bash
grep -r "auto_discover_symbols" packages/ ocm/ apps/ --include="*.py"
# Resultado: SOLO en schema.py (definición) y tests
# NO hay: if exc_cfg.auto_discover_symbols: ...
# NO hay: load_markets() filtrado por config
```

### En CCXTAdapter
```python
# packages/market_data/adapters/outbound/exchange/ccxt_adapter.py
async def connect(self) -> None:
    ...
    await self._load_markets()  # Llama load_markets() PERO...
    # Los mercados cargados NO se usan para validar/filtrar config.symbols
```

**Conclusión**: `auto_discover_symbols` es **DOCUMENTED ONLY** — campo en schema sin implementación.

---

## 5. INSTRUMENT METADATA — ESTADO

### Qué NO existe
- ❌ Instrument Registry (SSOT metadata por símbolo)
- ❌ Tick size, lot size, min_qty, max_qty por símbolo
- ❌ Contract info (para futures/options)
- ❌ Trading rules (precision, limits)
- ❌ Status (trading, settling, delisted)
- ❌ Provenance tracking (PROTOCOL vs DOCUMENTATION vs ASSUMED)

### Qué SÍ captura CCXT (pero no se usa)
```python
# CCXT load_markets() retorna:
{
    "BTC/USDT": {
        "symbol": "BTC/USDT",
        "base": "BTC",
        "quote": "USDT",
        "precision": {"price": 1, "amount": 3},
        "limits": {"amount": {"min": 0.001, "max": 10000}},
        "info": {...}  # raw exchange response
    }
}
```

**Se llama en `CCXTAdapter._load_markets()` pero resultado solo se cachea para `get_market()` — nunca se persiste ni se usa para validar config.**

---

## 6. QUÉ OCURRE CUANDO BYBIT CAMBIA INSTRUMENTOS

| Evento | Comportamiento Actual | Riesgo |
|--------|----------------------|--------|
| **Nuevo símbolo listado** | No detectado automáticamente. Requiere deploy de config YAML + restart servicios | Símbolo invisible para OCM hasta deploy manual |
| **Símbolo deslistado** | Pipeline sigue intentando fetch → errores 404/rate limit → guard kills ingestion | Errores repetidos, kill switch activado |
| **Cambio tick/lot size** | No detectado. Orders con precision vieja → reject por exchange | Rechazo de órdenes, P&L incorrecto |
| **Cambio de formato (ej. PERP suffix)** | Hardcoded en YAML → mismatch silencioso | WS conecta a símbolo inexistente, sin data |

---

## 7. FUENTE DE VERDAD RECOMENDADA

### Archivo Único: `config/market_data/universe.yaml` (NUEVO)

```yaml
# @package _global_
# SSOT: Market Universe — qué instrumentos OCM decide observar/operar
# Discovery ≠ Universe. Discovery = "qué existe". Universe = "qué usa OCM".

market_universe:
  # Configuración global de descubrimiento
  discovery:
    mode: "hybrid"              # auto | static | hybrid
    auto_discover: true         # Usar CCXT load_markets() como base
    refresh_interval_hours: 24  # Re-descubrimiento periódico
    validate_on_startup: true   # Fail-fast si símbolos config no existen en exchange
  
  # Normalización de formatos (SSOT interno = CCXT format)
  format:
    canonical: "ccxt"           # BTC/USDT, BTC/USDT:USDT
    mappings:
      cryptofeed:               # Conversión automática para WS
        spot: "{base}/{quote}"           # BTC/USDT
        linear: "{base}/{quote}"         # BTC/USDT
        inverse: "{base}/{quote}"        # BTC/USD
  
  # Universe por exchange
  exchanges:
    bybit:
      enabled: true
      discovery:
        mode: "auto"
        quote_assets: ["USDT", "USDC"]  # Filtrar solo USDT/USDC pairs
      # Override estático (opcional, para forzar subconjunto)
      symbols:
        spot:
          - BTC/USDT
          - ETH/USDT
        linear:
          - BTC/USDT
          - ETH/USDT
        inverse:
          - BTC/USD
      # Metadata overrides (opcional, si exchange reporta mal)
      metadata_overrides: {}
    
    kucoin:
      enabled: false
      discovery:
        mode: "static"
      symbols:
        spot:
          - BTC/USDT
          - ETH/USDT
```

### Consumo Unificado

```python
# Nuevo: MarketUniverseProvider (SSOT)
class MarketUniverseProvider:
    """Única fuente de símbolos para TODOS los pipelines (REST + WS)."""
    
    def __init__(self, config: MarketUniverseConfig):
        self._universe = config
    
    def get_symbols(self, exchange: str, market_type: str, venue: str = "ccxt") -> list[str]:
        """
        venue: "ccxt" (REST) | "cryptofeed" (WS)
        Retorna símbolos en formato nativo del venue.
        """
        symbols = self._universe.exchanges[exchange].symbols.get(market_type, [])
        if venue == "cryptofeed":
            return [self._to_cryptofeed(s) for s in symbols]
        return symbols
    
    def _to_cryptofeed(self, symbol: str) -> str:
        # BTC/USDT → BTC-USDT (spot)
        # BTC/USDT:USDT → BTC-USDT (linear)  
        # BTC/USD → BTC-USD-PERP (inverse)
        ...
```

---

## 8. PLAN DE MIGRACIÓN

### Fase 1: Crear SSOT (config/market_data/universe.yaml)
1. Crear archivo con estructura arriba
2. Añadir `MarketUniverseConfig` en `ocm/config/schema.py`
3. Crear `MarketUniverseProvider` en `packages/market_data/application/`

### Fase 2: Migrar Consumidores
| Consumidor | Cambio |
|------------|--------|
| `ConcretePipelineFactory._build_ohlcv()` | `universe.get_symbols(exchange, "spot", "ccxt")` |
| `FeedOrchestrator._build_adapters()` | `universe.get_symbols(exchange, "spot", "cryptofeed")` |
| `streaming_hydra.py` | `universe.get_symbols(exchange, "linear", "cryptofeed")` |

### Fase 3: Deprecar Fuentes Antiguas
- ❌ `config/env/development.yaml` → `exchanges.*.markets.*.symbols`
- ❌ `config/market_data/feeds.yaml` → `feeds.feeds.*.symbols`
- ✅ Mantener `config/exchanges/bybit.yaml` solo para `enabled` + `resilience`

### Fase 4: Auto-Discovery (Opcional, Post-B-49)
- Implementar `auto_discover_symbols=True` path
- `CCXTAdapter.load_markets()` → filtrar contra universe → validar

---

## 9. CLASIFICACIÓN DEL PROBLEMA

| Dimensión | Clasificación | Justificación |
|-----------|---------------|---------------|
| **Arquitectura** | **ARCHITECTURE ISSUE** | 3 fuentes de verdad violan SSOT |
| **Código** | **CODE ISSUE** | Formatos incompatibles sin normalizador |
| **Configuración** | **CONFIGURATION ISSUE** | YAMLs divergen sin validación cruzada |
| **Capacidad** | **MISSING CAPABILITY** | Sin auto-discovery, sin metadata registry |

**Impacto en B-49**: **BLOQUEA G5 (Kafka) y G9 (Bronze)** — universos divergentes causan data mismatch y gaps en Bronze.