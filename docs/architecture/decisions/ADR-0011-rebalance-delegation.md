# ADR-0011: Decisión pendiente — delegación de rebalanceo (assemble_rebalance)

**Estado:** Pendiente — decisión abierta, sin resolver (tracking de la deuda)
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

**SIN DECIDIR.** El stub `NotImplementedError` permanece hasta resolver la
delegación. Este ADR existe como tracking persistente de la decisión abierta
(requisito de la auditoría 2026-08-03, revisión de coherencia SSOT): un
método público bloqueado necesita un artefacto descubrible que documente
por qué y qué se evaluó.

## Justificación técnica

No ensamblar un camino de rebalanceo sin SSOT: cualquier implementación
prematura arriesga duplicar estado de posiciones (BC-43) o acoplar trading a
la infraestructura de portfolio (BC-08, DIP). Fail-Fast > fail-soft cuando
el diseño no está resuelto.

## Consecuencias

- `assemble_rebalance()` lanza `NotImplementedError` documentado
  (composition_root.py, con `TODO(ADR-0011)`).
- El roadmap debe cerrar esta decisión antes de habilitar rebalanceo desde
  trading.
- Mientras tanto, el rebalanceo desde trading no está disponible; portfolio
  conserva `RebalanceService` como capacidad adelantada (ADR-0004, serie
  heredada).

## Referencias

- Código: `packages/trading/bootstrap/composition_root.py` (`assemble_rebalance`)
- ADRs relacionados: ADR-0003 (constructor angosto, enmienda D3),
  ADR-0006 (portfolio dueño de estado), ADR-0004 (serie heredada,
  RebalanceService adelantado)
- Auditoría: `AUDITORIA-COMPOSITION-ROOTS-2026-08-03.md` (decisión D3)
