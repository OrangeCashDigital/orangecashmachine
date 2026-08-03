# ADR-0005: TradingEngine construye dependencias internas; el Composition Root, las externas

**Estado:** Aceptado (verificado en código actual)
**Fecha:** 2026-08-02
**Bounded context(s) afectado(s):** trading

## Contexto

TradingEngine.build_live()/build_paper() ya construyen Strategy,
RiskManager, Executor, OMS. El bytecode recuperado muestra que
TradingCompositionRoot construye, en cambio: PositionStore,
PortfolioService, TradeTracker, ExecutionGuard, CompositeFillObserver,
GoldLoaderAdapter.

## Alternativas evaluadas

1. Un solo composition root construye todo — duplicaría lógica que
   TradingEngine.build_* ya resuelve (viola DRY).
2. Split: Composition Root construye externas; TradingEngine.build_*
   construye internas.

## Decisión

TradingCompositionRoot no reconstruye lo que TradingEngine.build_live/
build_paper ya construyen. Solo ensambla piezas externas al ciclo
estrategia-riesgo-ejecución y las pasa como parámetros.

## Justificación técnica

DRY: reconstruir esa lógica crearía dos fuentes de verdad para el mismo
ensamblaje.

## Consecuencias

- El Composition Root depende de TradingEngine.build_* como colaborador.
- Cambios en la firma de build_live/build_paper deben propagarse a
  _build_engine_live/_build_engine_paper.

## Referencias

- packages/trading/engine.py (build_live linea ~211, build_paper ~282)
- docs/architecture/recovered/trading-bootstrap-forensic-analysis.md
