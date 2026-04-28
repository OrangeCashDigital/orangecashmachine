# OrangeCashMachine — Architecture

## Patron: Modular Monolith con Bounded Contexts (DDD)

OCM no es un sistema de microservicios. Es un monolito modular: un
unico proceso Python donde cada bounded context es un top-level package
con responsabilidad exclusiva, interfaz publica explicita, y frontera
de importacion enforceada por import-linter.

La extraccion a microservicio ocurriria exactamente en estas fronteras,
sin cambios internos.

---

## Bounded Contexts y flujo de datos

    market_data --> signals --> execution --> portfolio --> backtesting
                                   ^
                                  risk

    Cross-cutting (todos los BC pueden importarlos):
      infra/   -> Redis, Prometheus
      core/    -> tipos compartidos, config, boundaries
      config/  -> YAMLs Hydra

---

## Responsabilidades

market_data/
  Que hace  : ingerir, almacenar y exponer datos OHLCV de exchanges
  Produce   : Gold layer (Iceberg) via FeatureSource protocol
  Regla     : NO conoce signals, execution, risk ni portfolio

signals/  (actualmente trading/strategies/)
  Que hace  : generar senales de trading a partir de features
  Consume   : FeatureSource (Gold layer de market_data)
  Produce   : Signal[]
  Regla     : NO conoce execution ni risk — solo produce senales

risk/  (actualmente trading/risk/)
  Que hace  : evaluar si una senal puede ejecutarse dado el capital
  Consume   : Signal + estado de portfolio
  Produce   : aprobado/rechazado + sizing
  Regla     : NO conoce execution — es un filtro, no un actor

execution/  (actualmente trading/execution/)
  Que hace  : transformar senales aprobadas en ordenes (OMS)
  Consume   : Signal aprobado por RiskManager
  Produce   : Order fills via callback on_fill
  Regla     : NO genera senales ni evalua riesgo de portfolio

portfolio/  (pendiente de crear)
  Que hace  : estado vivo de posiciones, capital, NAV
  Consume   : Order fills via on_fill callback
  Produce   : TradeRecord[], equity curve, open positions
  Regla     : NO envia ordenes — solo observa y registra

backtesting/  (pendiente de implementar)
  Que hace  : simular estrategias sobre datos historicos
  Consume   : market_data + signals + risk
  Produce   : PerformanceSummary, equity curves, estadisticas
  Regla     : NO usa execution real — solo executors simulados

---

## Reglas de dependencia

  Permitido                       Prohibido
  ---------                       ---------
  signals    -> core              signals    -> execution
  signals    -> infra             signals    -> risk
  risk       -> core              risk       -> execution
  risk       -> infra             market_data -> trading/*
  execution  -> core              market_data -> signals
  execution  -> infra             portfolio  -> execution (solo via callback)
  execution  -> risk              backtesting -> execution (usa mocks)
  portfolio  -> core
  market_data -> core

---

## Flujo principal (paper trading, un ciclo)

  1. market_data  ingesta OHLCV -> Gold (Iceberg)
  2. signals      load_features() -> DataFrame
  3. signals      generate_signals(df) -> Signal[]
  4. execution    TradingEngine.run_once():
                    risk.evaluate(signal) -> aprobado/rechazado
                    OMS.submit(signal) -> Order
                    PaperExecutor.execute(order) -> fill simulado
  5. portfolio    TradeTracker.on_fill(order) -> TradeRecord
  6. portfolio    PerformanceEngine.summarize(trades) -> PerformanceSummary

---

## Contratos entre Bounded Contexts (SSOT: core/boundaries.py)

  Frontera                  Contrato
  --------                  --------
  market_data -> signals    FeatureSource
  signals -> execution      SignalProtocol
  execution -> portfolio    FillHandler
  portfolio -> backtesting  TradeHistory
  execution -> risk         RiskGate

---

## Estado actual (abr 2026)

  BC           Package actual            Estado
  ----------   -----------------------   -------
  market_data  market_data/              maduro
  signals      trading/strategies/       en monolito
  risk         trading/risk/             en monolito
  execution    trading/execution/        en monolito
  portfolio    trading/analytics/        incompleto
  backtesting  backtesting/              vacio
