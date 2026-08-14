# ADR-0026: Semántica de fees — UNKNOWN FEE != ZERO FEE y estados KNOWN/UNKNOWN/PROVISIONAL/FINAL

**Estado:** Aceptado
**Fecha:** 2026-08-14
**Bounded context(s) afectado(s):** trading (execution, risk, analytics)
**Referencia de dominio:** decisión aprobada en S1.2/S1.3/S1.4 (etiqueta interna "ADR-2")

## Contexto

El exchange no siempre reporta fees en un fill. `map_ccxt_order` devuelve `fees=None`
cuando el exchange no las entrega (`packages/trading/bootstrap/composition_root.py:304-311`),
y `Order.fees` es `Optional[float]` (`packages/trading/execution/order.py:114`). En el
cierre de un trade, `TradeTracker` trata la fee ausente como cero:
`fees = (entry_order.fees or 0.0) + (order.fees or 0.0)`
(`packages/trading/analytics/trade_tracker.py:174`), y cuando `fees_pct is None` el net
P&L se iguala al bruto (`packages/trading/analytics/trade_record.py:132-134`).

Tratar una fee desconocida como cero distorsiona el P&L y el riesgo: subestima el coste
y sobreestima el rendimiento. Por otra parte, bloquear operaciones por fee desconocida
puede atrapar una posición que debería cerrarse por seguridad.

## Alternativas evaluadas

1. **Default 0 (estado actual).** Rechazada: UNKNOWN == ZERO, distorsiona P&L y riesgo.
2. **Exigir fee siempre y bloquear si unknown.** Rechazada: atrapa cierres protectores;
   convierte una incertidumbre contable en un riesgo de ejecución.
3. **UNKNOWN ≠ 0 con estados + estimación conservadora para risk. Elegida.** La fee
   ausente se marca UNKNOWN; el net P&L se reporta UNKNOWN o PROVISIONAL; el risk usa una
   estimación conservadora; ningún cierre se bloquea por fee desconocida.

## Decisión

1. **UNKNOWN FEE != ZERO FEE.** La ausencia de fee es un dato ausente, no un coste nulo.
2. **Separación explícita de componentes:**
   - **Gross P&L**: P&L de precio sobre la cantidad realmente cerrada (ADR-0025), sin
     fees. Siempre conocible desde los fills.
   - **Fees**: coste de transacción del entry + del exit, en moneda quote. Puede ser
     KNOWN, UNKNOWN o PROVISIONAL.
   - **Net P&L**: `gross − fees`. Solo se reporta como KNOWN/FINAL cuando las fees son
     conocidas; nunca se iguala a gross por fee ausente.
3. **Estados de fee/P&L:**
   - **KNOWN** — fee reportada por el exchange (`Order.fees` presente, moneda normalizada
     a quote). Net P&L exacto.
   - **UNKNOWN** — fee no reportada (`fees=None`). Gross P&L conocido; **net P&L UNKNOWN**
     (no 0, no gross).
   - **PROVISIONAL** — net P&L estimado con `max(taker_fee_rate)` del instrumento (leído
     de `instruments-info`/`fee-rate`, no constante de código). Se usa para riesgo,
     drawdown/halt y reporting, etiquetado `provisional=true`.
   - **FINAL** — net P&L confirmado tras reconciliación con `closed-pnl` del exchange
     (`openFee` + `closeFee`, ADR-0027). Sustituye a UNKNOWN/PROVISIONAL.
4. **Comportamiento de Risk:** una fee desconocida **NO bloquea por sí sola** una
   operación de reducción o cierre. La única razón de bloqueo válida es seguridad
   demostrable (p. ej. rechazo por margen del propio exchange), nunca la falta de un dato
   contable.
5. **ACCOUNTING UNCERTAINTY vs EXECUTION/RISK SAFETY** — son conceptos distintos:
   - *Accounting uncertainty*: qué se reporta (UNKNOWN/PROVISIONAL/FINAL). No decide
     ejecución.
   - *Execution/risk safety*: qué se permite ejecutar. El risk usa la estimación
     conservadora (PROVISIONAL) para drawdown/halt/sizing — la incertidumbre de fee
     reduce, no infla, el apetito de riesgo — pero nunca bloquea un cierre por fee
     ausente.

## Justificación técnica

- **Convergencia de fuentes:** los costes de transacción explícitos (comisiones y fees de
  exchange) son un componente real y medible del resultado (Harris 2002, costes
  explícitos; Aldridge 2010, fees contabilizadas aparte; Kaabar 2023, gross vs net y
  comisiones impagadas que restan del neto; AFML, evaluación precisa de costes). Bybit
  define fees por instrumento (`takerFeeRate`/`makerFeeRate`) y las reporta en
  `closed-pnl` (`openFee`/`closeFee`).
- **Seguridad:** una fee desconocida no debe impedir cerrar una posición porque bloquear
  el cierre es un riesgo mayor que la incertidumbre contable (misma lógica que
  `closeOnTrigger` de Bybit: garantizar que el stop reduzca la posición).
- **Es decisión de arquitectura OCM** (estado máquina KNOWN/UNKNOWN/PROVISIONAL/FINAL);
  ninguna fuente externa la prescribe.

## Consecuencias

- **Más fácil:** P&L honesto ante fees ausentes; drawdown que incluye costes estimados;
  cierres protectores nunca bloqueados por contabilidad.
- **Deuda aceptada:** la estimación PROVISIONAL usa `max(taker_fee_rate)` (conservador);
  el valor exacto puede diferir hasta la reconciliación.
- **Contratos BC-NN que lo hacen cumplir:**
  - `BC-12` — risk no importa execution (la estimación de fee llega como dato, no como
    import).
  - `BC-35` — los wire schemas de fee/orden siguen viviendo en `shared/kafka/schemas/`.
- **Elimina:** `fees or 0.0` (trade_tracker.py:174) y `neto == bruto` cuando fees_pct es
  None (trade_record.py:132-134).

## No-objetivos (Non-goals)

- No se modela cashback, descuentos VIP ni devoluciones de fee.
- No se modela el funding fee por settlement (8h) — es coste de financiación del
  backtest/paper y de market-data, no fee de ejecución; fuera de alcance.
- No se crea un fee engine nuevo ni infraestructura.
- No se consignan valores numéricos de fee como constantes de código (leer del endpoint
  en uso; Bybit §12.1).

## Invariantes financieros

- **INV-06** — UNKNOWN fee nunca equivale a zero fee: `fee=None` no produce
  `net == gross` ni `fees == 0.0` en accounting. Testeable.
- **INV-07** — Gross P&L puede existir aunque el net P&L sea UNKNOWN. Testeable.
- Adicionales: un cierre protector nunca se bloquea por fee desconocida (testeable);
  el drawdown/risk incluye la fee estimada PROVISIONAL (testeable); la fee se normaliza a
  moneda quote (testeable).

## Evidencia y autoridad (Evidence & Authority)

- **OFFICIAL_EXCHANGE_FACT:** Bybit `takerFeeRate`/`makerFeeRate` por instrumento
  (`GET /v5/market/instruments-info`, `GET /v5/account/fee-rate`); estimated fee to close
  en las fórmulas de liquidación; `closed-pnl` con `openFee`/`closeFee`
  (`bybit-exchange.github.io/docs/v5/position`; `docs/knowledge/notes/bybit-perpetuals-
  reference.md` §6.4, §9.1).
- **CONCEPTUAL_KNOWLEDGE:** Harris 2002 (costes explícitos: comisiones, fees de exchange,
  impuestos); Aldridge 2010 (fees contabilizadas aparte, retornos netos auditables);
  Kaabar 2023 (gross vs net; comisiones impagadas que reducen el neto); AFML (costes
  evaluados con precisión en la graduación de una estrategia).
- **OCM_EVIDENCE:** `trade_tracker.py:174` (`fees or 0.0`), `trade_record.py:132-134`
  (`neto == bruto`), `composition_root.py:304-311` (`fees=None` cuando no reportadas),
  `order.py:114` (`fees: Optional[float]`).
- **OCM_RESEARCH_HYPOTHESIS:** ninguna aplicable a este ADR.
- **UNVERIFIED:** valores numéricos de fee vigentes (lectura del endpoint en uso; no hay
  valor canónico — Bybit §12.1); la cobertura de reporte de fees de CCXT por modo.
- **Convergencia:** exchange + literatura convergen en que la fee es un coste real y
  medible; la máquina de estados contable es decisión propia de OCM.

## Relación con la Knowledge Base

- **Influyó:** Harris, Aldridge, Kaabar y AFML como conocimiento conceptual sobre costes;
  `bybit-perpetuals-reference.md` §9.1 (fees por instrumento) y §6.4 (estimated fee).
- **NO utilizado:** Kaufman (`COVERAGE_UNVERIFIED`), Oxford, Liebowitz, Kanungo y los
  libros de data-engineering — no aportan a la semántica de fees de ejecución.
- **Decisión que NO puede derivarse de libros:** la máquina de estados
  KNOWN/UNKNOWN/PROVISIONAL/FINAL y la regla "una fee desconocida nunca bloquea un cierre"
  son decisiones de dominio/seguridad de OCM, no derivables de la literatura.
- **Documentación oficial con precedencia:** Bybit fee semantics sobre libros.
- **Regla:** BOOK ≠ CONTRACT; BOOK ≠ ADR; BOOK ≠ OCM_EVIDENCE.

## Implicaciones de implementación

- `packages/trading/analytics/trade_tracker.py`: eliminar `fees or 0.0`; producir estados
  KNOWN/UNKNOWN/PROVISIONAL/FINAL por trade (F7a).
- `packages/trading/analytics/trade_record.py`: net P&L solo KNOWN/FINAL; PROVISIONAL con
  flag; nunca igualar a gross por fee ausente (F7a).
- `packages/trading/risk/manager.py`: drawdown/halt/sizing sobre P&L neto con fee
  estimada PROVISIONAL (F3).
- `packages/trading/bootstrap/composition_root.py` (`map_ccxt_order`): normalizar la
  moneda de fee a quote (F7a).
- Tests unitarios/integración para cada estado y para el no-bloqueo de cierres.

## Referencias

- Código: `packages/trading/analytics/trade_tracker.py`,
  `packages/trading/analytics/trade_record.py`, `packages/trading/risk/manager.py`,
  `packages/trading/bootstrap/composition_root.py`, `packages/trading/execution/order.py`.
- ADRs relacionados: ADR-0025 (cost basis/P&L por cantidad), ADR-0027 (reconciliación
  fee UNKNOWN→FINAL), ADR-0016 (live executor real).
- Doc oficial: `https://bybit-exchange.github.io/docs/v5/position`,
  `https://bybit-exchange.github.io/docs/v5/account/fee-rate`.
- KB: `docs/knowledge/notes/bybit-perpetuals-reference.md` (§6.4, §9.1); Harris 2002
  (costes explícitos); Aldridge 2010; Kaabar 2023; AFML SSRN preprint.