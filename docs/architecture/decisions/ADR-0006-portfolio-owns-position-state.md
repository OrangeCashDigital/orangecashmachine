# ADR-0006: Portfolio es el único dueño del estado de posiciones

**Estado:** Aceptado y verificado en código (Fase 3 completada)
**Fecha:** 2026-08-02
**Bounded context(s) afectado(s):** portfolio, trading

## Contexto

PortfolioService recibe PositionStore obligatorio por constructor (DIP)
desde PortfolioCompositionRoot. BC-13 prohíbe que portfolio importe
trading.execution/trading.strategies; BC-43 restringe la instanciación
de los stores concretos al composition root de portfolio.

## Alternativas evaluadas

1. Cada bounded context mantiene su propia vista del estado de
   posiciones — riesgo de estado divergente entre contextos.
2. portfolio es la única fuente de verdad; trading consume, nunca
   escribe directamente.

## Decisión

PortfolioService es el único dueño del estado de posiciones. trading se
comunica por inyección de PortfolioService, nunca importando
portfolio.services/portfolio.infra directamente.

## Justificación técnica

Un solo dueño de estado mutable elimina la clase de bugs de estado
divergente entre bounded contexts.

## Consecuencias

- execute_paper.py/execute_live.py migrados para aceptar
  portfolio_service inyectado, preservando compatibilidad con CLIs
  legados.
- Pendiente: eliminar CLIs legados y el parámetro opcional
  portfolio_service=None.

## Referencias

- packages/portfolio/bootstrap/composition_root.py
- Contratos: BC-13, BC-43
