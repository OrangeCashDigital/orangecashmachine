# UNIFIED EXCHANGE ADAPTER — ANÁLISIS DE VIABILIDAD

> **Fecha**: 2026-09-04
> **Propuesta**: "Implement unified adapter per exchange (REST + WS in single class)"
> **Decisión**: **LATER** (post-B-49) — Con justificación documentada

---

## 1. QUÉ PROBLEMA CONCRETO RESOLVERÍA

### Problemas Actuales
| Problema | Evidencia | Impacto |
|----------|-----------|---------|
| **Duplicación de exchange logic** | `CCXTAdapter` (REST) + `BybitCryptofeedRunner` (WS) — dos clases para Bybit | Mantenimiento doble, risk de divergencia |
| **Market Universe fragmentado** | 3 configs, 2 formatos (`BTC/USDT` vs `BTC-USDT-PERP`) | Data mismatch REST vs WS |
| **Config dual** | `enabled: true` en exchanges + `enabled: true` en feeds | Confusión, sync manual |
| **Dual mode sin coordinación** | `ingestion_mode: dual` corre ambos sin parity validation | No hay garantía de consistencia |
| **Composition Root split** | `ConcretePipelineFactory` (REST) + `CompositionRoot.build_ws_producers()` (WS) | Dos puntos de ensamblado |

### Qué Resolvería Un Adapter Unificado
```python
# Conceptual — BybitUnifiedAdapter
class BybitUnifiedAdapter:
    """Single class: REST (CCXT) + WebSocket (Cryptofeed)"""
    
    def __init__(self, config: ExchangeConfig, mode: Literal["rest", "ws", "dual"]):
        self._rest = CCXTAdapter(...) if mode in ("rest", "dual") else None
        self._ws = BybitCryptofeedRunner(...) if mode in ("ws", "dual") else None
        self._symbol_normalizer = SymbolNormalizer()  # CCXT ↔ Cryptofeed
    
    # Unified interface
    async def fetch_ohlcv(self, symbol, timeframe, ...): ...
    async def fetch_trades(self, symbol, ...): ...
    async def subscribe_orderbook(self, symbols, callbacks): ...
    async def subscribe_trades(self, symbols, callbacks): ...
    async def start(self): ...  # Starts REST polling AND/OR WS
    async def stop(self): ...
    
    # Dual mode coordination
    async def validate_parity(self) -> ParityReport:  # REST vs WS comparison
        ...
```

---

## 2. QUÉ PROBLEMA ACTUAL NO RESUELVE

| Problema | Por qué no lo resuelve |
|----------|------------------------|
| **Orderbook → Bronze** | Requiere consumer, schema, tabla Iceberg — no es problema de adapter |
| **Gap detection/recovery** | Lógica de Cryptofeed/Orderbook Builder — no de adapter |
| **Instrument Metadata Registry** | Nueva capacidad (tick size, lot size, etc.) — no adapter |
| **Protocol Discovery formal** | ADR-0017 framework — no adapter |
| **Market Universe SSOT** | Config refactor — no adapter |
| **Schema Registry (Avro)** | B-18 — wire format evolution |
| **Systemd unification** | Deployment decision — no código |

**Conclusión**: Un adapter unificado resuelve **duplicación y coordinación**, pero **NO** resuelve los **gaps funcionales de B-49** (Orderbook Bronze, Market Universe).

---

## 3. ABSTRACCIONES EQUIVALENTES EXISTENTES EN OCM

| Abstracción | Archivo | Qué Cubre | Gap vs Unified |
|-------------|---------|-----------|----------------|
| `ExchangeAdapter` (Protocol) | `adapters/outbound/exchange/base.py` | REST: `fetch_ohlcv`, `fetch_trades`, `load_markets` | Solo REST |
| `MarketDataSource` (Protocol) | `ports/inbound/market_data_source.py` | WS: `subscribe_trades`, `start`, `stop` | Solo WS trades |
| `FeedRunnerProtocol` | `adapters/inbound/websocket/feed_runner_protocol.py` | WS: `run_until_stopped(symbols, on_trade, stop_event)` | Solo WS |
| `TradesSource` (Protocol) | `ports/inbound/trades_source.py` | REST + WS trades unified interface | Solo trades, no orderbook |
| `MarketDataSource` | `ports/inbound/market_data_source.py` | WS only | No REST |

**No existe**: Protocolo unificado `ExchangeAdapter + MarketDataSource` que cubra REST + WS + Orderbook + Metadata.

---

## 4. COMPATIBILIDAD CON ARQUITECTURA ACTUAL

### ✅ Compatible (Principios)
- **DIP**: Adapter implementa protocols, no al revés
- **Composition Root**: Un solo punto de ensamblado (`CompositionRoot`)
- **BC-07/BC-08**: Adapter en `adapters/`, no importa domain/application
- **SSOT config**: Consumiría `MarketUniverseProvider` único

### ⚠️ Requiere Cambios (Arquitectura)
| Cambio | Impacto | Archivos Afectados |
|--------|---------|-------------------|
| Nuevo Protocol `UnifiedExchangeAdapter` | Nuevo bounded context contract | `ports/outbound/unified_exchange.py` (NUEVO) |
| Exception BC-07/BC-08 | Adapter importa CCXT + Cryptofeed | `architecture/importlinter.toml` |
| `ConcretePipelineFactory` refactor | Usa unified adapter | `pipeline_factory.py` |
| `CompositionRoot.build_ws_producers()` deprecado | Reemplazado | `composition_root.py` |
| `FeedOrchestrator` refactor | Usa unified adapter | `feed_orchestrator.py` |
| `streaming_hydra.py` refactor | Usa unified adapter | `streaming_hydra.py` |
| Tests de integración | Cobertura dual mode | `tests/market_data/...` |

### ❌ Incompatible (Sin Refactor Mayor)
- **BC-NN contracts**: 49 import-linter contracts deben actualizarse
- **ADR-0014**: `market_data` structure (realtime_feeds vs external_ingestion) — unified adapter blurs this line
- **Dual mode semantics**: `ingestion_mode: dual` → parity validation automática cambia semántica

---

## 5. RIESGO DE REFACTORIZACIÓN

| Riesgo | Severidad | Mitigación |
|--------|-----------|------------|
| **Rompe BC-NN contracts** | CRÍTICO | Requiere ADR + contract updates + CI validation |
| **Rompe dual mode parity** | ALTO | Tests de paridad REST vs WS obligatorios |
| **Scope creep en adapter** | ALTO | Adapter crece: REST + WS + Metadata + Discovery = God class |
| **Tests de integración** | ALTO | Necesita mock CCXT + Cryptofeed + Kafka |
| **Config migration** | MEDIO | Migration path para configs existentes |
| **Systemd impact** | MEDIO | Un solo servicio configurable vs dos actuales |

---

## 6. IMPACTO EN B-49

| Gate | Impacto | Comentario |
|------|---------|------------|
| G4 (Systemd) | Medio | Un servicio configurable vs dos |
| G5 (Kafka) | Positivo | Single producer path |
| G6 (Infra) | Neutro | Sin cambio |
| G7 (Health) | Positivo | Single health endpoint |
| G8 (IS_STUB) | Neutro | Sin cambio |
| G9 (Bronze) | **Neutro** | **NO resuelve Orderbook Bronze** |

**Conclusión**: Unified adapter **NO desbloquea B-49**. El blocker G9 (Orderbook Bronze) es independiente.

---

## 7. CLASIFICACIÓN FINAL

| Clasificación | Justificación |
|---------------|---------------|
| **LATER** (post-B-49) | 1. No resuelve blockers B-49 (G9 Orderbook Bronze)<br>2. Requiere ADR formal + BC-NN updates<br>3. Major refactor con riesgo alto<br>4. Depende de Market Universe SSOT (que debe hacerse primero)<br>5. Value add: maintainability, no funcionalidad crítica |

### Roadmap Sugerido
```
B-49 PASS (G1-G9)
    │
    ├── PR Market Universe (SSOT config + normalizer)
    │
    ├── PR Orderbook Builder (gap detection + recovery)
    │
    ├── PR Protocol Discovery (Bybit profile + metadata registry)
    │
    └── PR Unified Adapter (ADR + BC-NN + refactor)
         │
         ├── Nuevo Protocol: UnifiedExchangeAdapter
         ├── Exception BC-07/BC-08
         ├── Deprecate: FeedOrchestrator, build_ws_producers
         ├── Single systemd service configurable
         └── Tests: parity validation, dual mode
```

---

## 8. RECOMENDACIÓN

**NO implementar ahora**. 

**Prerrequisitos para considerarlo**:
1. ✅ B-49 PASS (G1-G9 verdes)
2. ✅ Market Universe SSOT implementado
3. ✅ Orderbook Builder funcional (gap recovery)
4. ✅ ADR aprobado para unified adapter
5. ✅ BC-NN contracts actualizados y validados en CI

**Valor real del unified adapter**: Reducir mantenimiento y eliminar data mismatch REST/WS. **No es requisito para producción** — es mejora de arquitectura post-MVP.