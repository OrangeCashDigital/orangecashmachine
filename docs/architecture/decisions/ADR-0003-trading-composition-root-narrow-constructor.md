# ADR-0003: TradingCompositionRoot recibe TradingConfig+RiskConfig+RedisConfig, no AppConfig completo

**Estado:** Aceptado (recuperado por introspección de bytecode)
**Fecha:** 2026-08-02
**Bounded context(s) afectado(s):** trading

## Contexto

trading/bootstrap/composition_root.py se perdió sin haber sido commiteado
nunca a git. Se recuperó su diseño por introspección de marshal sobre el
bytecode, incluyendo el docstring completo del módulo. AppConfig exige
exchanges y pipeline como campos obligatorios, sin significado para
entrypoints CLI puros de trading (live.py, paper.py, rebalance.py).

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
