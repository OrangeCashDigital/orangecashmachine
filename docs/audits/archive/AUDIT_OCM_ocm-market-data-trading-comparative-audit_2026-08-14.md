# Auditoría comparativa — OCM Market Data, Trading, Documentación y Propuestas

**Fecha:** 2026-08-14
**Auditor:** OpenAI Codex (asistido por agente de exploración)
**Alcance:** `packages/market_data`, `packages/trading`, `packages/portfolio`, `shared/` (kafka, contracts), `apps/`, `docs/` (incl. untracked), `ocm/runtime` (guard), `docs/plans/tracking.yaml`
**Método:** reconstrucción real desde código (quién instancia qué, qué se consume en runtime) + comparación con patrones de Freqtrade (`docs/Untitled_2.py`) y Hummingbot (`docs/data.py`). Regla de honestidad: cada hallazgo VERIFIED/UNVERIFIED/INFERENCE con evidencia `file:line`. Distinguir **"el código existe"** de **"la capacidad existe en runtime"**.
**Regla especial:** nada de capacidades/gaps inventados; `Unknown`/`Needs Investigation` cuando no es verificable. No se copia código externo (Freqtrade GPL-3.0, Hummingbot Apache-2.0) — solo patrones.

**Referencias cruzadas:** `docs/audits/2026-08-14-market-data-deep-audit.md` (hallazgos F-MD-001..010) · `docs/audits/2026-08-14-b-md-proposals-detalle.md` (propuestas B-MD-001..007).

---

## 1. Executive Summary

1. **Market data (Fase 1) es fuerte y madura**: OHLCV con invariantes + control-plane, dedup multi-nivel, provenance Kappa, order book producido pero **sin consumidor** (F-MD-003), trades WS live **huérfano** (F-MD-008), sin freshness (F-MD-001) ni tripleta temporal (F-MD-002). Todo documentado en el audit de market data.
2. **Trading/execution (Fase 2/3) es sólido en lo que toca, pero incompleto para live**: reconciliación fail-closed (ADR-0016, `fill_sync`), WAC (ADR-0025), settlement canónico, máquina de estados de orden estricta. **Sin embargo**: RiskManager no recibe market data (F-MD-006), transporte es **solo market orders** (`composition_root.py:238`), no hay cancelación en exchange, no hay balance reconciliation, no hay precision/min-order (F-MD-007).
3. **La conexión Market Data → Risk → Execution NO existe** (§10). El RiskManager valida contra capital×size_pct y drawdown; el mercado (spread, stale, liquidez) no influye en absoluto en la decisión.
4. **Freqtrade 7 patrones**: 3 ya superados en OCM (post-fill state transition, cancelación tipada parcialmente, risk protections globales), 2 ausentes con riesgo (balance reconciliation, fee fallback), 1 parcial (open-order management — solo market orders), 1 ausente con diseño propio mejor (entry/exit symmetry vía SELL=reducción).
5. **Hummingbot MarketData/MarketState**: la separación Evento vs Estado existe en OCM a nivel de VOs y eventos, pero **falta el reducer de estado (BookBuilder)** que convierta eventos en estado accesible (F-MD-003).
6. **Portfolio/Position es correcto y bien modelado** (PositionSnapshot, WAC, cost_basis, unrealized condicional) pero sin `cum_fees`/break-even ajustado a fees en la posición, y `current_price` queda None por defecto (P&L no realizado no disponible en runtime hasta que exista mark/valuation).
7. **Riesgos financieros cubiertos**: drawdown (daily/total) con halt, kill switch global (guard), dedup/ordenes duplicadas, WAC, settlement de fees KNOWN/UNKNOWN, posición por WAC. **NO cubiertos**: stale market data, spread, slippage, liquidez, balance mismatch, fee fallback, precision/min-order.
8. **Live readiness: NO está listo para capital real.** Los gaps P0/P1 (B-MD-004 pre-submit market validity, B-MD-001 freshness, B-MD-002 BookBuilder) deben cerrarse antes de cualquier `--mode live`.
9. **Nuevos docs (untracked)**: `docs/Untitled_2.py` (freqtrade source) y `docs/data.py` (hummingbot dataclasses) son material de investigación legítimo; ADR-0024 y `docs/knowledge/` contienen decisiones/KB no publicadas (ver auditoría 2026-08-13).
10. **Recomendación**: aprobar B-MD-003+B-MD-001+B-MD-005 dentro de Fase 1; B-MD-002 (BookBuilder, ADR A-MD-002) y B-MD-004 (pre-submit, ADR A-MD-003) en Fase 2; NO operar live sin esa cadena.

---

## 2. Estado real de OCM (qué existe realmente)

Verificado desde código (dónde se instancia, qué se consume):

| Capacidad | ¿Existe? | ¿Consumida en runtime? | Evidencia |
|---|---|---|---|
| Ingesta OHLCV REST (backfill) | Sí | Sí | `OHLCVFetcher` → `TradesPipeline`/`OHLCVPipeline` |
| OHLCV Kappa → Kafka → Bronze | Sí | **Parcial** (canary/streaming; F-031/B-46 corregido 2026-08-12) | `KafkaOHLCVPublisher`, `bronze_writer` |
| Order book producer | Sí | Productor solo — **sin consumer** | `OrderBookKafkaProducer`; grep `orderbook.raw` = ∅ consumidores (F-MD-003) |
| Trades live WS | Parcial (builder huérfano) | **No** | `_build_trades_stream` sin invocación (F-MD-008) |
| Trades REST backfill → Silver | Sí | Sí | `TradesPipeline`, `trades_storage.py` |
| Gold features | Sí | Sí (trading) | `_GoldFeatureSource` (composition_root.py:136-194) |
| OMS + máquina de estados | Sí | Sí | `oms.py`, `order.py` |
| RiskManager (drawdown, sizing, positions) | Sí | Sí | `risk/manager.py` |
| RiskManager con market data | **No** | — | sin bid/ask/spread (F-MD-006) |
| LiveExecutor real (CCXT) | Sí | Solo `--mode live` con credenciales | `_BybitTransport`, `IS_STUB=False` |
| PaperExecutor | Sí | Default `--mode paper` | `paper_executor.py`, `live_hydra.py:74` |
| Fill reconciliation fail-closed | Sí | Sí | `fill_sync.py`, ADR-0016 |
| Portfolio/Position (WAC) | Sí | Sí | `position.py`, `portfolio_service.py` |
| Kill switch global | Sí | Sí | `ocm/runtime/guard.py` |
| Circuit breaker por símbolo | **No** | — | solo guard global |

---

## 3. Nuevos documentos encontrados (docs/ untracked)

| Archivo | Tipo | Conocimiento | Estado | Acción propuesta |
|---|---|---|---|---|
| `docs/Untitled_2.py` (2671 líneas) | Fuente `freqtrade` (`FreqtradeBot`) | Patrones de reconciliación, pricing, precision, order book en estrategias | Untracked | **KEEP AS RESEARCH** — referencia de patrones; no publicar como doc oficial |
| `docs/data.py` (81 líneas) | Dataclasses Hummingbot (proposals) | `OrdersProposal`/`PricingProposal`/`SizingProposal`/`PriceSize`/`Proposal`/`HangingOrder` | Untracked | **KEEP AS RESEARCH** — no contiene `MarketData`/`mid_price`; es capa de propuestas |
| `docs/architecture/decisions/ADR-0024-*.md` | ADR (dirección microservicios market-data/trading/portfolio) | Decisión arquitectónica | Untracked | **PROMOTE TO ADR** (decisión pendiente humana, ver auditoría 2026-08-13) |
| `docs/knowledge/` (~35 archivos) | Knowledge base | KB interna (incl. bybit-perpetuals-reference) | Untracked | **PROMOTE TO KNOWLEDGE BASE** (pendiente decisión de publicación) |
| `docs/audits/2026-08-13-auditoria-arquitectonica.md` | Auditoría | Estado arquitectónico | Untracked | **KEEP AS AUDIT** |
| `docs/audits/2026-08-13-knowledge-base-audit.md` | Auditoría | Gobernanza KB | Untracked | **KEEP AS AUDIT** |
| `docs/audits/2026-08-14-market-data-deep-audit.md` | Auditoría (esta sesión) | F-MD-001..010 | Untracked | **KEEP AS AUDIT** (base de B-MD-XXX) |

**Clasificación de cada hallazgo documental:** NUEVO (ADR-0024, KB) · YA EXISTE (mucho del contenido de market data) · PARCIAL (docs/data.py) · SOLO DOCUMENTACIÓN (resto). Ninguno se publica automáticamente.

---

## 4. Market Data — estado completo

Ver `docs/audits/2026-08-14-market-data-deep-audit.md` §3–§8. Resumen del pipeline real:

```
Exchange ─► adapters (REST OHLCV / WS cryptofeed) ─► eventos de dominio ─► Kafka
  ├─ ohlcv.raw ─► BronzeWriter ─► QualityGate ─► ohlcv.validated ─► FeatureEngine ─► ohlcv.features
  ├─ orderbook.raw (producer-only, SIN consumer)
  └─ trades.raw (producer WS, sin consumer confirmado; backfill REST → Silver vivo)
```

**4.1 Trades:** raw→normalized OK; event_id/occurred_at/source OK; **sequence: NO**; dedup 3 niveles OK; ordering por cursor monotónico (source_manager) OK; replay OK (KappaSourceMixin); persistence Silver OK; **consumers de trades.raw: ninguno confirmado** (`Needs Investigation`: ¿apps/research?).

**4.2 Order Book:** **PRODUCIDO pero NO CONSUMIDO** (F-MD-003). VOs sólidos (bids DESC/asks ASC, qty=0=delete). **Sin** best_bid/best_ask/mid/spread/microprice/imbalance en runtime. sequence capturado pero no propagado (F-MD-004). Sin gap detection por secuencia.

**4.3 OHLCV:** construcción OK; invariantes OK (low≤open/high/close≤high, volume≥0, timestamp>0); gaps ≥2×tf detectados por `gap_scanner`; stale candles no (solo OHLCV persistido); SSOT Gold→trading; consumers OK (FeatureEngine, StrategyConsumer).

**4.4 Freshness:** **NO puede responder "¿qué tan viejo es esto?"** (F-MD-001). Falta `event_time`/`received_at`/`processed_at` (F-MD-002). Impacto: sin staleness no se puede proteger la ejecución.

**4.5 Data quality:** cubre datos persistidos (empty, future timestamps, gaps, OHLC inconsistencies, MAD/zscore outliers, flatlines). **No** cubre crossed books, sequence gaps, duplicate events en vivo, zero/negative prices en feed (solo invariantes en VOs).

---

## 5. Trading — estado completo

**Motor (`engine.py`):** `run_once` → load_features → `current_price = last close` → stop-loss (si inyectado) → generate_signals → `oms.submit`. Un solo punto de entrada; guard.check() al inicio.

**OMS (`oms.py`):**
- `submit` → kill switch → `risk.validate` → `executor.execute` → `_fill` o `_reject`. Fail-fast en transiciones (`_VALID_TRANSITIONS`).
- `_fill`: BUY → WAC acumulado (`_entry_positions`); SELL → clamp `min(filled_qty, prev_qty)` (INV-08), cierre parcial preserva WAC, settlement canónico, `risk.record_close`.
- `cancel(order_id)` → local transition a CANCELLED + `risk.record_close`. **NO cancela en el exchange** (el transporte no expone cancel).
- `reject_reason`: **string libre, no tipado** (Freqtrade patrón 6.3 NO cumple).

**Entrada/Salida (6.1):** `is_reduction = side=="sell"`; Risk exime a SELL de reglas de apertura (INV-F2-05). Simetría entrada/salida **parcial por diseño** (cierre gobernado por posición, no por señal). VERIFIED.

**Open-order management (6.4):** solo market orders → no hay órdenes limit flotando. `OMS.cancel` es local; `LiveExecutor._submit` tiene `timeout_s=10` y reintentos con backoff, pero **no cancela en exchange ante timeout** (fail-closed: retorna None, el OMS rechaza). **No hay replace/reprice.** Parcial.

---

## 6. Risk — estado completo

`RiskManager` (`risk/manager.py`):
- Checks: halt → actionable → min_confidence → max_open_positions (solo aperturas) → drawdown daily/total con halt atómico → sizing USD (min/max_order_usd, solo aperturas).
- Espejo económico real: `_positions[symbol]=(qty, avg_entry)` desde fills del OMS (INV-F2), UNKNOWN ≠ ZERO (INV-F2-06), partial close decrementa qty pero no conteo (INV-F2-02/03).
- **NO consulta market data** (F-MD-006). No hay spread/stale/liquidity/precision/min-order checks.
- Kill switch global vía `ExecutionGuard` (max_errors consecutivos, max_runtime, manual). **Sin circuit breaker por símbolo** (Freqtrade patrón 6.7 parcial).

---

## 7. Portfolio / Position / PnL — estado completo

`PositionSnapshot` (position.py): symbol, exchange, side, quantity, avg_entry (WAC, ADR-0025), size_pct, entry_at, order_id, current_price (opcional, None por defecto). `cost_basis = qty×avg`. `unrealized_pnl_pct/usd` solo si `current_price` presente (mark/valuation F9 = UNKNOWN, no implementado).

**Separación de conceptos (buena):** Order (ejecución) ≠ Fill (resultado) ≠ Position (posición abierta) ≠ Settlement (P&L realizado del cierre) ≠ PortfolioState (snapshot). P&L realizado SOLO vía Settlement del SELL (SSOT, no recalculado — criterio G/S1). P&L no realizado: **no disponible en runtime** (current_price None).

**Gaps:**
- `PositionSnapshot` **no tiene `cum_fees` ni break-even ajustado a fees** (Hummingbot `Position.cum_fees`/`breakeven_price` no existen como equivalente). Break-even actual = avg_entry (sin fees).
- `fee_currency` = None (GAP F7 documentado, `settlement.py:466`). Fee fallback ante exchange sin fees (Freqtrade patrón 6.6): **no existe** (fee_status KNOWN/UNKNOWN; si UNKNOWN, net P&L = None).
- Balance reconciliation (6.5): **no existe** `fetch_balance` en trading (grep = ∅). Equity curve de analytics es derivada del P&L realizado, no del balance real del exchange.

---

## 8. Execution — estado completo

**PaperExecutor:** fill = signal.price (paper no simula slippage/spread). Default de `--mode paper`.

**LiveExecutor:** `IS_STUB=False`. `_submit` con retries+backoff y `_reconcile` fail-closed (fetch_state; no confirmado → rechazo). `timeout_s=10`. **Envia market orders** vía `_BybitTransport` (`create_order(order_type="market")`). **Sin** validación de mercado pre-submit (B-MD-004), **sin** precision/min-order (B-MD-005), **sin** cancel en exchange, **sin** balance check.

**Fill reconciliation (fail-closed, ADR-0016):** `build_fill_sync(tracker, portfolio)` → OMS. En SELL, `Settlement.compute` con WAC. `Order.settlement` es el P&L canónico.

**Orden de ejecución actual:** live requiere `--mode live` + credenciales + `--capital` obligatorio (`live_hydra.py:94`). Guard obligatorio, risk obligatorio, `require_promoted("OrderFilledPayload","OrderRejectedPayload")`, `IS_STUB=False` check (fail-closed).

---

## 9. Freqtrade comparison — 7 patrones

| # | Patrón | OCM | Estado | Evidencia |
|---|---|---|---|---|
| 6.1 | Entry/Exit symmetry | SELL = reducción gobernada por posición (no señal) | **ALREADY EXISTS** (diseño propio mejor) | `manager.py:335`, INV-F2-05 |
| 6.2 | Single post-fill state transition | `OMS._fill` único punto + `fill_sync` composite | **ALREADY EXISTS** (fail-closed, superior a `update_trade_state`) | `oms.py:368`, `fill_sync.py:84` |
| 6.3 | Typed cancellation reason | `reject_reason` string libre; cancel sin razón | **PARTIAL / GAP** | `order.py:121`, `oms.py:300` |
| 6.4 | Open-order management (timeout/cancel/replace) | Solo market orders; timeout fail-closed; **no cancel en exchange, no replace** | **PARTIAL** | `live_executor.py:161-261`, `composition_root.py:223-256` |
| 6.5 | Balance reconciliation (DB vs exchange) | **No existe** | **GAP** | grep `fetch_balance` = ∅ |
| 6.6 | Fee reconciliation | FeeStatus KNOWN/UNKNOWN; `fee_currency`=None; **sin fallback ni detección de fees absurdas** | **PARTIAL / GAP** | `settlement.py:22-67`, GAP F7 |
| 6.7 | Risk protections (symbol vs global breaker) | Guard global (max_errors, runtime, manual) + drawdown halt | **PARTIAL** (global sí, por símbolo no) | `guard.py`, `manager.py:370-386` |

---

## 10. Hummingbot comparison — MarketData / MarketState / Position

| Concepto Hummingbot | Problema que resuelve | OCM | Veredicto |
|---|---|---|---|
| `MarketData` (best bid/ask, mid, depth, last trade) | Estado de mercado accesible a estrategias | **No existe en runtime** (order book producido, sin consumer, F-MD-003) | **ADAPT** → B-MD-002 (BookBuilder) |
| `MarketState` (estado persistente/contextual) | Separar evento efímero de estado durable | OCM tiene eventos + VOs; **falta el reducer** | **ADAPT** — la separación conceptual ya existe, falta el consumer |
| `Position` (amount, breakeven, realized/unrealized, cum_fees) | Contabilidad de posición | PositionSnapshot (WAC, cost_basis, unrealized condicional) **sin cum_fees/break-even ajustado** | **PARTIAL** → ver §7 |
| `OrdersProposal`/`PricingProposal`/`SizingProposal` | Precio vs tamaño separados | OCM: tamaño=Risk (capital×size_pct), precio=señal; **sin capa de pricing de ejecución** | **INVESTIGATE** (solo si se introduce pricing de ejecución) |

**Nota de honestidad (INFERENCE):** `docs/data.py` no contiene `MarketData`/`mid_price` en el fragmento — el modelo de `MarketData` de Hummingbot es conocimiento de la herramienta (INFERENCE), no evidencia del archivo. Verificar contra doc oficial antes de usarlo como premisa.

---

## 11. Architecture gaps

1. **Market Data → Risk → Execution desconectado** (F-MD-006). El RiskManager no recibe el estado del mercado.
2. **Order book sin consumer** (F-MD-003) → sin best bid/ask/spread/liquidez en runtime.
3. **Sin freshness** (F-MD-001) y **sin tripleta temporal** (F-MD-002).
4. **Sin instrumentos/precisión/min-order** como datos (F-MD-007).
5. **trades_stream huérfano** (F-MD-008).
6. **Sin cancelación en exchange / replace / balance reconciliation** (ejecución F2/3).
7. **`current_price` (mark/valuation) = None por defecto** → P&L no realizado no disponible (F9 UNKNOWN).
8. **Fee fallback/currency** (GAP F7) — fees UNKNOWN → net P&L None.

---

## 12. Financial / trading risks

| Riesgo | Cubierto | No cubierto | Evidencia |
|---|---|---|---|
| Market: stale data | — | **SÍ** gap (B-MD-001) | engine usa `last close` gold |
| Market: spread | — | **SÍ** gap (B-MD-002/004) | Risk sin spread |
| Market: slippage | — | **SÍ** gap (market orders) | `composition_root.py:238` |
| Market: liquidity/depth | — | **SÍ** gap (B-MD-002) | sin BookBuilder |
| Execution: timeout | Parcial (fail-closed, timeout_s=10) | sin cancel en exchange | `live_executor.py:229-261` |
| Execution: partial fills | Sí (clamp INV-08, WAC preservado) | — | `oms.py:442-456` |
| Execution: cancel failures | — | **SÍ** gap (no cancela en exchange) | `oms.py:300` |
| Execution: rejected orders | Sí (REJECTED + reason) | reason no tipada | `order.py:177-178` |
| Position: incorrect amount | Sí (filled_qty real, clamp) | — | `oms.py:410-430` |
| Position: balance mismatch | — | **SÍ** gap (sin fetch_balance) | §8 |
| Position: duplicate execution | Sí (client_order_id idempotencia) | — | `live_executor.py:174` |
| Accounting: wrong fees | Parcial (FeeStatus) | fee_currency None, sin fallback | GAP F7 |
| Accounting: realized PnL | Sí (Settlement canónico) | — | `settlement.py` |
| Accounting: unrealized PnL | — | **SÍ** gap (current_price None) | `position.py:107-118` |
| Accounting: break-even | Parcial (avg_entry) | sin fees en break-even | `position.py` |
| Operational: exchange disconnected | Sí (retries/backoff/guard) | — | `live_executor.py:178-196` |
| Operational: Kafka lag | — | **SÍ** gap (sin métricas de latencia) | F-MD-002/007 |
| Operational: missing/stale data | — | **SÍ** gap | F-MD-001 |

---

## 13. Live readiness

**NO listo para capital real.** Barreras:

1. **P0 — B-MD-004** (pre-submit market validity): sin él, RiskManager no ve el mercado; market orders a precio potencialmente stale/desplazado.
2. **P0/P1 — B-MD-002** (BookBuilder): sin él no hay spread/liquidez/bid-ask.
3. **P1 — B-MD-001** (freshness): sin él un feed congelado es indistinguible.
4. **P1 — B-MD-005** (instrumentos/precisión): sin él una orden puede violar límites del exchange.

Además, para una operación live completa: **cancelación en exchange, balance reconciliation, fee fallback (F7)**, y mark/valuation (F9) para P&L no realizado. Estos últimos pueden ser fases 2/3 sin bloquear una primera prueba acotada, pero los tres primeros (B-MD-001/002/004) son **bloqueantes de seguridad**.

---

## 14. Recommended improvements

| ID | Mejora | Problema | Beneficio | Prioridad | Fase | ADR |
|---|---|---|---|---|---|---|
| B-MD-001 | Freshness/staleness | Mercado congelado indistinguible | Ejecución protegida, alertas | P1 | F1 | No |
| B-MD-002 | BookBuilder consumer | orderbook.raw sin consumidor | bid/ask/spread/liquidez en runtime | P0 | F2 | A-MD-002 |
| B-MD-003 | `sequence` en wire order book | gap-detection imposible | BookBuilder correcto | P1 | F1 | A-MD-001 |
| B-MD-004 | Pre-submit market validity | Risk sin market data | Evita slippage/órdenes a precio stale | P0 | F2/3 | A-MD-003 |
| B-MD-005 | Instrumentos/precisión/min-order | ExchangeQuirks solo paginación | Órdenes válidas vs límites reales | P1 | F1/2 | No |
| B-MD-006 | Resolver trades_stream huérfano | Builder muerto | Trades live o sin dead code | P2 | F2 | No |
| B-MD-007 | `received_at`/`processed_at` | Latencia no medible | Observabilidad end-to-end | P2 | F2/3 | A-MD-001 |
| *(nuevo, F2/3)* | Cancel en exchange + replace | Timeout sin cancel real | Control de órdenes | P2 | F2 | No |
| *(nuevo, F2/3)* | Balance reconciliation | DB ≠ exchange | Cuenta exacta | P2 | F3 | ADR riesgo |
| *(nuevo, F2/3)* | Fee fallback + currency (F7) | Fees UNKNOWN → net None | Contabilidad completa | P2 | F3 | No |
| *(nuevo, F3)* | Mark/valuation (F9) | current_price None | P&L no realizado | P3 | F3 | ADR riesgo |
| *(nuevo, F2/3)* | Cancel reason tipado | reason libre | Diagnóstico/auditoría | P4 | F3 | No |

---

## 15. Proposed tracking items

Ver `docs/audits/2026-08-14-b-md-proposals-detalle.md` (B-MD-001..007 con detalle completo: prioridad, ADR, archivos, riesgo, dependencias, esfuerzo, orden de aprobación). **No se modificó `tracking.yaml`.** Los items 8–12 de §14 quedan como candidatos a propuesta formal cuando se decida la Fase 2/3 (se añadirán al mismo formato B-XXX tras aprobación).

---

## 16. Decisions required from human

1. **¿Aprobar B-MD-003 + B-MD-001 + B-MD-005 dentro de Fase 1?** (aditivos, bajo riesgo, desbloquean la cadena). Abrir ADR **A-MD-001** (contrato canónico: sequence + received_at/processed_at).
2. **¿Aprobar B-MD-002 (BookBuilder) en Fase 2?** Requiere ADR **A-MD-002** (componente + port inter-BC `MarketDataView`).
3. **¿Aprobar B-MD-004 (pre-submit market validity) en Fase 2/3?** Requiere ADR **A-MD-003** (política de fallo de ejecución ante market data ausente/stale).
4. **¿B-MD-006: cablear o eliminar `trades_stream`?** Decisión previa al trabajo.
5. **¿Estatus de ADR-0024 y publicación de `docs/knowledge/`?** (pendiente de la auditoría 2026-08-13).
6. **¿Cuándo se permite `--mode live`?** Recomendación: solo tras B-MD-001+002+004 implementados y probados (fail-closed verificado).
7. **¿Orden de cierre de Fase 1?** Si Fase 1 debe cerrar sin B-MD-003/B-MD-001, documentar el riesgo residual (ver §13) y no abrir live.

---

## 17. Git safety

- **HEAD:** `aa547b3` (`refactor(research): F-1 — composition root para research (DIP)`), rama `main`. Sin commits ni pushes en esta sesión.
- **Archivos modificados (working tree, previos, NO de esta sesión):** `AGENTS.md`, `README.md`, `apps/app/use_cases/execute_live.py`, `docs/audits/2026-08-apps-audit.md`, `tests/architecture/test_import_contracts.py` (incluyen las correcciones de documentación 2026-08-13/14).
- **Archivos untracked (pre-existentes):** `docs/Untitled_2.py`, `docs/data.py`, `ADR-0024-*.md`, `docs/audits/2026-08-13-*.md`, `docs/knowledge/`.
- **Creados en esta sesión de auditoría (solo documentación):** `docs/audits/2026-08-14-market-data-deep-audit.md`, `docs/audits/2026-08-14-b-md-proposals-detalle.md`, `docs/audits/2026-08-14-ocm-market-data-trading-comparative-audit.md`.
- **No modificados:** producción, tests, `tracking.yaml`, ADRs existentes. `git status` verificado tras el análisis.
