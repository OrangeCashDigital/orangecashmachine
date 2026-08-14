# ADR-0025: Position accounting y cost basis — cantidad económica y weighted average cost

**Estado:** Aceptado
**Fecha:** 2026-08-14
**Bounded context(s) afectado(s):** trading (execution, analytics), portfolio (models)
**Referencia de dominio:** decisión aprobada en S1.2/S1.3/S1.4 (etiqueta interna "ADR-1")

## Contexto

OCM representa posiciones sin cantidad económica: `PositionSnapshot` solo guarda
`symbol`, `entry_price` y `size_pct` (`packages/portfolio/models/position.py:60-64`).
En multi-entry, `OMS._entry_prices` sobreescribe el precio de entrada con el último
BUY (last-entry-wins, `packages/trading/execution/oms.py:327`), de modo que el P&L
realizado de un SELL posterior se calcula contra una entrada arbitraria y sin cantidad
(`oms.py:331`: `(fill_price - entry) / entry`). El `filled_qty` real llega al OMS vía
`OrderState` (`oms.py:313`) pero no se propaga ni a la posición ni al P&L.

El exchange expone su propia semántica de posición: `GET /v5/position/list` devuelve
`size` (cantidad, siempre positiva, en unidades de base) y `avgPrice` (average entry
price), y el Help Center oficial define el average entry de los perpetuals lineales
USDT como media ponderada: `(Σ Qty_i × Price_i) / Σ Qty_i`. El P&L realizado y las
fórmulas de liquidación de Bybit operan sobre `Position Size` y `Avg Entry Price`
(`docs/knowledge/notes/bybit-perpetuals-reference.md` §6.4, §7.4).

Sin una base de coste determinista no hay P&L ni riesgo correctos en posiciones
multi-entry ni en cierres parciales.

## Alternativas evaluadas

1. **Weighted Average Cost (WAC). Elegida.** `avg = Σ(qty_i × price_i) / Σqty_i`;
   se preserva en cierres parciales. Coincide con `avgPrice` de Bybit, habilitando
   reconciliación directa posición↔exchange. Determinista, simple, sin tracking de lotes.
2. **FIFO.** Requiere tracking por lote y asigna unrealized por lote; el exchange no
   expone lotes de perpetuals; sin objetivo de reconciliación. Rechazada.
3. **LIFO.** Mismo coste que FIFO e inusual en perps. Rechazada.
4. **Last-entry (estado actual).** Metodología UNKNOWN; avg incorrecta en multi-entry;
   P&L erróneo en cierres parciales; diverge del exchange. Rechazada.

## Decisión

1. **quantity** = cantidad económicamente ejecutada y mantenida de la posición, en
   unidades del activo base (p. ej. BTC). Campo obligatorio nuevo en `PositionSnapshot`.
   La unidad de la posición es la cantidad, no el notional%.
2. **Múltiples entradas:** se acumulan en un único **weighted average cost**. Tras cada
   entrada `i` con `qty_i` y `price_i`: `qty += qty_i`, `basis += qty_i × price_i`,
   `avg = basis / qty`.
3. **Average entry** = `Σ(qty_i × price_i) / Σqty_i`. Se conserva al cerrar parcialmente
   (el resto de la posición mantiene su avg).
4. **Cost basis** = `qty × avg` (en moneda quote). No incluye fees (ver ADR-0026).
5. **Partial closes:** reducen `qty` en la cantidad cerrada, realizan P&L sobre la
   cantidad realmente cerrada y preservan `avg`. La posición se cierra a `qty = 0`.
6. **Realized P&L** (gross) = `Σ closed_qty × (exit_price − avg_entry_al_cierre)`.
   **Net P&L** = gross − fees de entrada − fees de salida, con los estados de fee del
   ADR-0026.
7. **Unrealized P&L** = `qty_abierta × (valuation_price − avg_entry)`. El valuation
   price es el mark price (decisión Q-C, S1.3); el execution price es siempre el fill
   real. Ninguno se sustituye por señal ni por el otro (INV-02, INV-10).
8. **Long/short:** la WAC es simétrica para ambos lados. OCM es hoy **long-only**
   (`packages/trading/execution/fill_sync.py:111` fija `side="long"`); la decisión aplica
   al caso actual y queda lista para short.
9. **Fees:** se asignan por fill; no forman parte del cost basis; solo afectan al net P&L.

**Ejemplo canónico (multi-entry + cierre parcial):** BUY 1@100 + BUY 2@110 →
qty=3, basis=320, avg=106.667. SELL 1@120 → realizado = 1×(120−106.667) = 13.333
(gross), resto qty=2, basis=213.333, avg=106.667. FIFO daría 20.0; LIFO 10.0.

## Justificación técnica

- La **convergencia** con la semántica oficial del exchange (Bybit `avgPrice` = media
  ponderada) es la justificación principal: hace la posición OCM comparable 1:1 con la
  del exchange, habilita la reconciliación de ADR-0027 y elimina la sobreescritura
  arbitraria last-entry-wins.
- El código ya lee el candidate de avg desde el adapter (`raw.get("average")`,
  `packages/trading/bootstrap/composition_root.py:317`); WAC es la única opción que se
  corresponde con ese campo sin transformación.
- Determinista (INV-04): mismo conjunto de fills → mismo avg, misma base, mismo P&L.
- No es "es estándar por ser estándar": es la semántica que el exchange expone como
  estado de cuenta oficial y que OCM puede reconciliar.

## Consecuencias

- **Más fácil:** P&L correcto en multi-entry y cierres parciales; reconciliación de
  posiciones contra el exchange; base para unrealized mark-based.
- **Deuda aceptada:** la WAC pierde granularidad de lotes (irrelevante para perps);
  requiere persistir quantity+avg en `PositionSnapshot` (F4a/F4b).
- **Contratos BC-NN que lo hacen cumplir:**
  - `BC-13` — la posición se asienta vía el puente existente (fill_sync + inyección);
    portfolio nunca importa trading.
  - `BC-43`/`BC-44` — el almacén de posiciones se instancia solo en el composition root
    de portfolio.
  - `BC-12` — risk no importa execution.
- **Elimina:** `OMS._entry_prices` (last-entry-wins) como fuente de entrada.

## No-objetivos (Non-goals)

- No se implementa tracking de lotes FIFO/LIFO ni contabilidad fiscal.
- No se cubre el settlement por sesión de los perpetuals USDC (el avg de USDC resetea a
  mark en cada settlement de 8h); los símbolos target son USDT lineales. Si OCM amplía a
  USDC, requiere ADR propio.
- No se crea un Position model independiente ni un P&L engine: se extiende
  `PositionSnapshot` existente y el P&L se asienta en el camino único de `OMS._fill`.
- No se crea infraestructura nueva.

## Invariantes financieros

- **INV-01** — `Position.quantity == executed quantity` (suma de qty ejecutadas de las
  piernas abiertas). Testeable.
- **INV-03** — un partial fill modifica la quantity real de la posición. Testeable.
- **INV-04** — el average entry es determinista: mismo conjunto de fills → mismo avg.
  Testeable.
- **INV-05** — el realized P&L utiliza la quantity realmente cerrada:
  `closed_qty × (exit − avg)`. Testeable.
- Adicionales: cierre parcial preserva `avg`; posición cerrada a `qty = 0`; el net P&L
  resta fees del gross (ADR-0026). Testeables.

## Evidencia y autoridad (Evidence & Authority)

- **OFFICIAL_EXCHANGE_FACT:** Bybit Help Center "Average Entry Price" (fórmula de media
  ponderada para linear USDT); `GET /v5/position/list` (`size`, `avgPrice`);
  fórmulas de liquidación que usan `Avg Entry Price`; `closed-pnl` con `avgEntryPrice`
  (`bybit-exchange.github.io/docs/v5/position`, `bybit.com/en/help-center/article/
  Average-entry-price`, `docs/knowledge/notes/bybit-perpetuals-reference.md` §6.4).
- **CONCEPTUAL_KNOWLEDGE:** Aldridge 2010 (órdenes por cantidad especificada; P&L de
  posición y stops; retornos netos auditables); Harris 2002 draft (long/short = unidades
  poseídas; costes explícitos de trading). **Neutrales** respecto a la metodología de
  cost basis interna — los libros no prescriben la contabilidad interna de OCM.
- **OCM_EVIDENCE:** `oms.py:327` (last-entry-wins), `oms.py:331` (P&L sin qty),
  `position.py:60-64` (sin quantity), `composition_root.py:317` (`raw.get("average")`),
  `fill_sync.py:111` (long-only).
- **OCM_RESEARCH_HYPOTHESIS:** ninguna aplicable a este ADR.
- **UNVERIFIED:** el mapeo de `raw.get("average")` de CCXT a `avgPrice` de Bybit en
  todos los modos de cuenta (confirmar en testnet).
- **Convergencia:** el estándar de accounting del exchange (media ponderada) y la
  necesidad de reconciliación de OCM convergen en WAC. Los libros son input conceptual,
  no autoridad arquitectónica (BOOK ≠ CONTRACT).

## Relación con la Knowledge Base

- **Influyó:** `docs/knowledge/notes/bybit-perpetuals-reference.md` (§6.4 avg entry en
  liquidación, §7.4 posiciones por qty); Aldridge y Harris como conocimiento conceptual.
- **NO utilizado:** Kaufman 2013 (`COVERAGE_UNVERIFIED`, PDF corrupto), Oxford, Kaabar,
  AFML, Liebowitz, Kanungo y los libros de data-engineering — no aportan a cost basis.
- **Decisión que NO puede derivarse de libros:** la metodología de cost basis interna
  (WAC vs FIFO vs LIFO) es una decisión de dominio OCM fundamentada en la semántica del
  exchange, no en la literatura.
- **Documentación oficial con precedencia:** Bybit API/Help Center sobre libros.
- **Regla:** BOOK ≠ CONTRACT; BOOK ≠ ADR; BOOK ≠ OCM_EVIDENCE. La KB informa; este ADR
  gobierna.

## Implicaciones de implementación

- `packages/portfolio/models/position.py`: añadir `quantity` y `avg_entry` (F4a/F4b).
- `packages/portfolio/ports/position_store.py` y `redis_store.py`/`memory_store.py`:
  persistir los campos nuevos (tecnología de almacén de posiciones sin cambios).
- `packages/trading/execution/oms.py`: el asentamiento usa WAC (acumular qty/basis/avg)
  en `_fill`; eliminar `_entry_prices`; realized/unrealized basados en cantidad.
- `packages/trading/execution/fill_sync.py`: el Protocol `SupportsPositionSync` recibe
  quantity/avg_entry en `open_position` y devuelve la posición cerrada con cantidad en
  `close_position` (decisión Q-A, opción D — mecanismo existente, sin BC nuevo).
- `packages/trading/analytics/trade_record.py`/`trade_tracker.py`: P&L por cantidad y
  estados de fee (ADR-0026).
- No se crea BC nuevo, ni Position model independiente, ni P&L engine.

## Referencias

- Código: `packages/portfolio/models/position.py`, `packages/trading/execution/oms.py`,
  `packages/trading/execution/fill_sync.py`, `packages/trading/analytics/trade_record.py`,
  `packages/trading/bootstrap/composition_root.py`.
- ADRs relacionados: ADR-0006 (portfolio posee el estado de posiciones), ADR-0026
  (semántica de fees), ADR-0027 (recovery/source of truth), ADR-0016 (live executor real).
- Doc oficial: `https://bybit-exchange.github.io/docs/v5/position`,
  `https://www.bybit.com/en/help-center/article/Average-entry-price`.
- KB: `docs/knowledge/notes/bybit-perpetuals-reference.md` (§6.4, §7.4).