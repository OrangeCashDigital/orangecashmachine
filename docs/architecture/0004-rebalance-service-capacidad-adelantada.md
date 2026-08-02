# ADR-0004: RebalanceService como capacidad adelantada del bounded context portfolio

**Estado:** Aceptado
**Fecha:** 2026-08-01
**Contexto del bounded context:** portfolio

## Contexto

La auditoría de composition roots (hallazgo 2.1) encontró `apps/app/use_cases/
rebalance.py` sin importadores ni tests, y determinó que era código huérfano
(Principio 5, ADR-0000): no existía una decisión documentada de hacia dónde
iba, a diferencia de `RiskGate`. El use case fue eliminado en consecuencia
(commit `3d5ab3f`).

Al eliminarlo se descubrió que `RebalanceService`/`RebalanceSignal`
(`packages/portfolio/services/rebalance_service.py`) quedaban sin ningún
consumidor. Este domain service sí está documentado como pieza del dominio
en `DOMAIN.md` (línea 75), y a diferencia del use case, tiene un rol
concreto en el roadmap: OCM no se limita a ejecutar señales, sino que
apunta a una plataforma cuantitativa donde `portfolio` es un bounded
context con responsabilidades propias — entre ellas, rebalanceo automático
contra targets.

## Decisión

`RebalanceService` se conserva como contrato adelantado a propósito
(Principio 5), en espera de un futuro consumidor: el flujo de gestión de
portafolio que se implementará cuando se materialice `build_portfolio()`
(ADR-0003) dentro del roadmap de bounded contexts (ADR-0001, pendiente de
escribir). No se elimina en cascada junto con el use case huérfano.

## Consecuencias

- `DOMAIN.md` línea 75 se actualiza para referenciar este ADR.
- El audit de composition roots (hallazgo 2.1) se marca resuelto: destino
  final decidido — conservar como diseño adelantado, no conectar todavía.
- Cuando exista el consumidor real (futuro flujo de rebalanceo automático),
  este ADR puede citarse como el origen de la decisión de mantenerlo.

## Alternativas consideradas

- **Eliminar en cascada junto con `rebalance.py`**: rechazado — confundiría
  "código sin consumidor actual" con "código sin plan", violando la
  distinción que el propio Principio 5 establece.
