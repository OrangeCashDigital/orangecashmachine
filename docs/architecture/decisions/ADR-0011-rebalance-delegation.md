# ADR-0011: Aceptada — delegación de rebalanceo (assemble_rebalance)

**Estado:** Aceptada — 2026-08-07
**Fecha:** 2026-08-03
**Bounded context(s) afectado(s):** trading, portfolio

## Contexto

`TradingCompositionRoot.assemble_rebalance()` existe como stub en la
reconstrucción 2026-08-03 (decisión D3 de la auditoría de composition
roots). El tracking real de posiciones vive en el bounded context portfolio
(BC-43, ADR-0006): `PortfolioService` + `PositionStore` + `RebalanceService`
(`packages/portfolio/services/rebalance_service.py`). trading no debe
duplicar estado de posiciones.

El forense de bytecode recuperó el método original
(`assemble_rebalance(self, *, use_redis) -> TradingRuntime`) con la string
literal "SIEMPRE en memoria — usar desde assemble_paper()/assemble_rebalance()
dry-run", pero sin el cuerpo.

## Alternativas evaluadas

1. **Delegar en `RebalanceService` de portfolio** — trading ensambla su
   runtime y llama al servicio de portfolio para el rebalanceo. Ventaja:
   respeta BC-43/ADR-0006 (portfolio es dueño del estado). Costo/riesgo:
   acoplamiento trading→portfolio vía servicio (debe ir por protocolo/port,
   no por import directo de infraestructura).
2. **Tracking propio en trading** — trading mantiene su propia vista de
   posiciones para rebalancear. Ventaja: independencia del BC. Costo/riesgo:
   viola el SSOT de portfolio (BC-43) — dos fuentes de verdad para el mismo
   estado.

## Decisión

**Alternativa 1 — Delegar en `RebalanceService` de portfolio, vía protocolo/port.**

`trading` no importa infraestructura de `portfolio` directamente. Se define
`RebalancePort` (Protocol, `shared/contracts/boundaries.py`) que declara la
API pública real de `RebalanceService` (`rebalance()`, `validate_targets()`).
Los tipos que cruzan la frontera (`PortfolioState`, `RebalanceSignal`) se
tipan bajo `TYPE_CHECKING` — sin import real en runtime, mismo patrón ya
usado en `composition_root.py` para `PortfolioService`. `trading` recibe una
instancia que satisface `RebalancePort` inyectada por Composition Root;
nunca instancia `RebalanceService` ni conoce su `__init__`
(`drift_threshold`/`min_delta_pct` son detalle interno de portfolio).

Se descarta la Alternativa 2 (tracking propio en trading): viola SSOT
(BC-43) al crear una segunda fuente de verdad sobre el estado de
posiciones.

## Justificación técnica

No ensamblar un camino de rebalanceo sin SSOT: cualquier implementación
prematura arriesga duplicar estado de posiciones (BC-43) o acoplar trading a
la infraestructura de portfolio (BC-08, DIP). Fail-Fast > fail-soft cuando
el diseño no está resuelto.

## Consecuencias

- `RebalancePort` (Protocol, `@runtime_checkable`) declarado en
  `shared/contracts/boundaries.py`.
- `assemble_rebalance()` en `composition_root.py` recibe el port inyectado
  y delega — deja de lanzar `NotImplementedError`.
- `portfolio` sigue siendo dueño exclusivo del estado de posiciones
  (BC-43/ADR-0006); `trading` solo conoce el contrato, no la implementación.
- Cambios futuros en la construcción interna de `RebalanceService`
  (parámetros de `__init__`, lógica de `_compute`) no rompen `trading`
  mientras la forma de `RebalancePort` se mantenga estable.

## Referencias

- Código: `packages/trading/bootstrap/composition_root.py` (`assemble_rebalance`)
- ADRs relacionados: ADR-0003 (constructor angosto, enmienda D3),
  ADR-0006 (portfolio dueño de estado), ADR-0004 (serie heredada,
  RebalanceService adelantado)
- Auditoría: `AUDITORIA-COMPOSITION-ROOTS-2026-08-03.md` (decisión D3)
