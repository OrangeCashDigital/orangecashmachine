# ADR-0009: Eliminar FillHandler y TradeHistory de boundaries.py — huérfanos, superados por fill_sync.py

**Estado:** Aceptado
**Fecha:** 2026-08-02
**Bounded context(s) afectado(s):** shared, trading, portfolio

## Contexto

`docs/DOMAIN.md` §5.4 distinguía, dentro de `shared/contracts/boundaries.py`,
entre `RiskGate` (contrato adelantado a propósito, con topics Kafka y
schemas ya declarados esperando su consumidor) y `FillHandler`/`TradeHistory`
(candidatos a decisión: cero referencias fuera de su propia definición y
re-export).

Verificación en esta sesión (`grep -rn "FillHandler\|TradeHistory"
--include="*.py" .`) confirma: ningún módulo del árbol importa ninguno de
los dos Protocols para tipar un parámetro o retorno real. Los únicos
matches son la propia definición y el re-export en
`shared/contracts/__init__.py`.

Más grave que la ausencia de consumidor: el problema que ambos prometían
resolver (desacoplar `execution` de `portfolio`/`analytics` vía Protocol)
ya está resuelto, por otro camino, en
`packages/trading/execution/fill_sync.py`:

- `fill_sync.py` define sus propios Protocols locales (`SupportsOnFill`,
  `SupportsPositionSync`), estructuralmente equivalentes a `FillHandler`,
  pero sin importar `FillHandler` de `boundaries.py`.
- `TradeTracker.on_fill()`/`TradeTracker.closed_trades`
  (`packages/trading/analytics/trade_tracker.py:69,87`) cumplen la forma
  de `FillHandler`/`TradeHistory`, pero los callers reales
  (`execute_live.py:302`, `execute_paper.py:334`) acceden a
  `tracker.closed_trades` tipando el objeto concreto `TradeTracker`, no
  el Protocol de `boundaries.py`.
- La atribución `"Implementado por: portfolio.TradeTracker"` en ambos
  docstrings es incorrecta incluso en el supuesto de implementarlos:
  `TradeTracker` vive en `trading.analytics`, nunca existió en
  `portfolio`.

## Alternativas evaluadas

1. **Implementarlos de verdad** — tipar `fill_sync.py`/`TradeTracker`
   contra los Protocols de `boundaries.py`. Rechazada: crearía un tercer
   punto de verdad para el mismo contrato (el real ya vive en
   `fill_sync.py`), violando el Principio 1 (SSOT) de `ADR-0000`. DRY es
   subordinado a SSOT (Principio 7) — el síntoma real no es duplicación
   de código sino dos fuentes de verdad para la misma frontera.
2. **Dejarlos documentados sin implementar**, igual que `RiskGate`.
   Rechazada: `RiskGate` tiene evidencia objetiva de plan futuro (topics
   Kafka `signals.raw`/`signals.approved`/`signals.rejected` ya
   declarados en `shared/kafka/schemas/signals.py`). `FillHandler`/
   `TradeHistory` no tienen ningún artefacto equivalente — la distinción
   entre "adelantado a propósito" y "huérfano sin plan" (Principio 5,
   ADR-0000) exige evidencia, no solo la posibilidad teórica de uso
   futuro.
3. **Eliminarlos de boundaries.py.**

## Decisión

Se eliminan `FillHandler` y `TradeHistory` de
`shared/contracts/boundaries.py` y su re-export en
`shared/contracts/__init__.py`. La frontera execution→portfolio/analytics
sigue desacoplada vía los Protocols locales ya en uso en
`trading/execution/fill_sync.py` — que no se tocan ni se renombran en
este ADR.

## Justificación técnica

SSOT (Principio 1, ADR-0000): un contrato documentado que nadie consume y
que además compite conceptualmente con un contrato real en uso no aporta
desacoplamiento — aporta confusión sobre cuál es la fuente de verdad. La
atribución incorrecta del docstring agrava esto: un lector que confíe en
`boundaries.py` como SSOT (tal como el propio archivo se declara en su
docstring de módulo) sería dirigido a un dueño equivocado (`portfolio`
en vez de `trading.analytics`).

## Consecuencias

- `shared/contracts/boundaries.py` queda con `FeatureSource`,
  `SignalProtocol`, `RiskGate` — los tres con evidencia verificada de uso
  o plan real.
- Ningún import se rompe: `grep` confirmó cero consumidores reales antes
  de este cambio.
- Si en el futuro se decide formalizar el contrato execution→portfolio a
  nivel de `boundaries.py`, el punto de partida correcto es promover los
  Protocols ya probados de `fill_sync.py` — no reintroducir uno nuevo.

## Referencias

- `docs/DOMAIN.md` §5.4 (distinción RiskGate vs FillHandler/TradeHistory)
- `packages/trading/execution/fill_sync.py` (implementación real vigente)
- `packages/trading/analytics/trade_tracker.py:69,87`
- `ADR-0000` Principios 1 (SSOT), 5 (Event First — adelantado vs huérfano), 7 (DRY subordinado a SSOT)
- `ADR-0004` (precedente de tratamiento: rebalance.py huérfano vs RebalanceService adelantado)
