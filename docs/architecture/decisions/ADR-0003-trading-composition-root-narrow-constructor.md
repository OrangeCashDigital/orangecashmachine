# ADR-0003: TradingCompositionRoot recibe sub-configs angostos (trading, risk, portfolio), no AppConfig completo

**Estado:** Aceptado (recuperado por introspección de bytecode)
**Fecha:** 2026-08-02
**Bounded context(s) afectado(s):** trading

## Contexto

trading/bootstrap/composition_root.py se perdió sin haber sido commiteado
nunca a git. Se recuperó su diseño por introspección de marshal sobre el
bytecode, incluyendo el docstring completo del módulo. AppConfig exige
exchanges y pipeline como campos obligatorios, sin significado para
entrypoints de trading (CLIs Hydra de live/paper, rebalance).

## Alternativas evaluadas

1. Recibir AppConfig completo — forzaría a callers CLI puros a inventar
   config dummy solo para pasar validación Pydantic.
2. Constructor angosto: TradingConfig+RiskConfig+RedisConfig sueltos.

## Decisión

TradingCompositionRoot.__init__(self, trading, risk, redis) recibe los
tres sub-configs sueltos. El entrypoint Hydra (ocm) sigue usando
AppConfig completo vía OCMContainer, sin tocar ese composition root.

## Justificación técnica

Forzar datos dummy solo para pasar validación viola Fail-Fast: los
datos dummy no representan estado real.

## Consecuencias

- Dos formas de construir dependencias de trading: Hydra para ocm,
  constructor angosto para CLIs puros — deuda documentada.
- Bloqueado: TradingConfig no existe hoy en ocm/config/schema.py.

## Referencias

- docs/architecture/recovered/trading-bootstrap-forensic-analysis.md
- packages/portfolio/bootstrap/composition_root.py (referencia de estilo)

---

## Enmienda 2026-08-03 (auditoría de composition roots)

**Constructor real aprobado:**

```python
TradingCompositionRoot.__init__(self, trading, risk, portfolio, guard=None)
```

Cambios respecto al ADR original (`__init__(trading, risk, redis)`):

- **`redis` se elimina.** La Fase 3 estableció que `portfolio` es el único
  dueño de Redis (BC-43, `PortfolioCompositionRoot`). `RedisFactory` queda
  **obsoleto** y NO se recrea — ver corrección del §7 del forense.
- **`portfolio` se inyecta ya ensamblado** (`PortfolioService`) desde
  `PortfolioCompositionRoot.assemble()`. El root NO reconstruye
  `_build_position_store_*`/`_build_portfolio` del forense: esos métodos
  predatan ADR-0006/BC-43 y reconstruirlos violaría BC-43 (decisión D2 del
  informe de auditoría).
- **`guard` es parámetro opcional del constructor** (kill switch). Obligatorio
  en `assemble_live()` (fail-fast) — sin guard no hay live trading con
  capital real.
- Retorno: `assemble_live()/assemble_paper()` → `TradingRuntime`
  (dataclass `engine, portfolio, tracker`).
- Ensamblado: el root construye TODAS las dependencias del engine
  (Strategy, RiskManager, Executor, OMS) y las externas (gold data source
  BC-50, TradeTracker, callback `build_fill_sync(tracker, portfolio)`).
  Los factories `TradingEngine.build_live()/build_paper()` fueron eliminados
  (2026-08-03) — ver **ADR-0012** (reemplaza ADR-0005).
- `assemble_rebalance(*, use_redis)` → stub `NotImplementedError` documentado
  (decisión D3: el tracking real de posiciones vive en portfolio).
  **Decisión de delegación PENDIENTE — rastreada en `ADR-0011`.**

Tipos: `trading` → `ocm.config.schema.TradingConfig`; `risk` →
`ocm.config.schema.RiskConfig` (AppConfig.risk); `portfolio` →
`portfolio.services.portfolio_service.PortfolioService`.

**Antecedente que motivó la enmienda:** el forense recuperó intención de
diseño (bytecode), no una especificación ejecutable. La interfaz v2 armada
en working tree (`__init__(config: TradingConfig)`) fue DESCARTADA por la
auditoría (H3) por contradecir este ADR; no es base a corregir.

**Nota de proceso (2026-08-03):** el archivo se perdió dentro de un commit
`feat(portfolio)` que tocó dos BCs (portfolio + trading/bootstrap). Lección
de atomic commits: un commit = un cambio lógico en un BC. Ver también el
forense (§ "Nota de proceso — atomic commits").
