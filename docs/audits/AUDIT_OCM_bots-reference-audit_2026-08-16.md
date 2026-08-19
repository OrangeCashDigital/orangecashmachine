# Auditoría de bots de referencia — B-MD-008 (cancelación) y B-MD-009 (balance)

**Fecha:** 2026-08-16
**Propósito:** Benchmark arquitectónico de los bots almacenados en `docs/` contra OCM, para validar o mejorar los ADR-0029 (B-MD-008) y ADR-0030 (B-MD-009). **No se copia implementación externa; solo patrones, invariantes y estrategias** (§5).
**Método:** Inventario → inspección controlada de ZIPs → mapeo por capacidad → comparación OCM/Bybit/CCXT/bots → evaluación arquitectónica → conclusiones.

---

## 1. Inventario de referencias

| Referencia | Qué contiene | Relevancia B-MD-008 | Relevancia B-MD-009 |
|---|---|---|---|
| `docs/hummingbot-master.zip` (3.7M) | Código Hummingbot (Cython legacy + `ExchangePyBase`/`ClientOrderTracker`/`InFlightOrder` moderno) | **Alta** — `PENDING_CANCEL`, caché de órdenes para fills tardíos, REST autoritativo | **Alta** — `_update_balances` UTA, `BudgetChecker` sizing |
| `docs/freqtrade-develop.zip` (44M) | Código Freqtrade (monolito `FreqtradeBot` + `Wallets` + `Trade`/`Order` SQLAlchemy) | **Alta** — cancel→fetch, "bail & retry", replace cancel+create, `manage_open_orders` | **Alta** — `Wallets` free/used/total, dry-run desde DB |
| `docs/nautilus_trader-develop.zip` (28M) | NautilusTrader v2 (Rust; `crates/`; event-sourcing, máquinas de estado, reconcilers) | **Alta** — máquina de estados con `(PendingCancel, Filled)=>Filled`, `check_inflight_orders`, reconcilers | **Alta** — `AccountState` event-sourced, `check_position_discrepancy`, tolerancias |
| `docs/audits/2026-08-15-b-md-008-cancel-b-md-009-balance-audit.md` | Auditoría forense F-BMD8-01..05, F-BMD9-01..05 | Evidencia de veredicto (ya en ADR-0029) | Evidencia de veredicto (ya en ADR-0030) |
| `docs/audits/2026-08-15-b-md-008-009-diseno-conceptual.md` | Diseño conceptual §3-6 | Origen del diseño CANCELLING | Origen del diseño Portfolio-SSOT |
| ADR-0029 / ADR-0030 (PROPUESTA) | Diseño OCM para B-MD-008/009 | Solución propuesta (referencia) | Solución propuesta (referencia) |

No hay `.tar`/`.gz`/`.rar`/`.7z`. Los tres ZIPs se extrajeron a `/tmp/opencode/bots/` para inspección controlada (fuera del repo, sin modificar nada).

---

## 2. Soluciones existentes por bot

### 2.1 Hummingbot

**Órdenes (ciclo de vida):** enum `OrderState` (`core/data_type/in_flight_order.py:21-32`): `PENDING_CREATE, OPEN, PENDING_CANCEL, CANCELED, PARTIALLY_FILLED, FILLED, FAILED, ...`. El estado vive en cada `InFlightOrder`; `update_with_order_update` machaca el estado y dedup si no cambió (`in_flight_order.py:327-349`). No es tabla de transiciones: es enum + asignación + propiedades derivadas (`is_open`/`is_done`/`is_filled`).

**Carrera CANCEL/FILL:** `PENDING_CANCEL` existe (`in_flight_order.py:24`). `_execute_order_cancel_and_process_update` (`exchange_py_base.py:556-571`): tras cancel exitoso, `CANCELED` si el exchange es síncrono, `PENDING_CANCEL` si asíncrono (Bybit spot síncrono `bybit_exchange.py:108-110`; **Bybit perpetual asíncrono** `bybit_perpetual_derivative.py:131-133`, mapea `"PendingCancel"→PENDING_CANCEL`). **Fill tras cancel:** las órdenes terminadas se cachean 30 s (`client_order_tracker.py:34-35`, `all_fillable_orders` = activas+cacheadas+perdidas `:86-91`) para absorber fills tardíos. **La ambigüedad no se decide localmente:** si llega `OrderUpdate FILLED`, espera hasta 5 s los TradeUpdates (`:277-286`); si no, **REST `fetch_order`/`_request_order_status` es la fuente de verdad final** (`exchange_py_base.py:1032-1040`; bybit `/v5/order/realtime` `bybit_exchange.py:484-516`). La prioridad de completado es `is_cancelled → is_filled → is_failure` (`client_order_tracker.py:423-439`); `is_filled` también se cumple por cantidad ejecutada (`in_flight_order.py:193-200`) — una orden PENDING_CANCEL con fills completos termina FILLED.

**Cancelación:** `cancel()` fire-and-forget (`exchange_py_base.py:356-366`); `cancel_all(timeout)` en paralelo (`:368-398`); **no reintenta** la cancelación; timeout `GET_EX_ORDER_ID_TIMEOUT=10s` o "order not found" → `process_order_not_found` (`:534-554`) → contador, si >3 y no done → `OrderState.FAILED` + `_lost_orders` (loop cada 5 s `:851-867`).

**Balance:** REST `_update_balances` (`bybit_exchange.py:518-541` `/v5/account/wallet-balance`): `total=walletBalance`; `available=free` (SPOT) o `walletBalance - locked - totalOrderIM - totalPositionMM - totalPositionIM` (UNIFIED `:537-538`). WS canal wallet en tiempo real (`:360-383`). Flag `_real_time_balance_update` (`connector_base.pyx:51-53`); si no hay real-time, `apply_balance_update_since_snapshot` corrige el snapshot con in-flight+fills (`:381-394`). **`BudgetChecker.adjust_candidates`** hace sizing pre-trade con colateral hipotético (`budget_checker.py:38-105`).

**Posiciones (perpetual):** SSOT = REST `_update_positions` por par (`bybit_perpetual_derivative.py:436-490`); **si `amount==0` → `remove_position`** (`:489-490`), si no `set_position`. Modo de posición se re-sincroniza al arrancar (`_initialize_position_mode` `:115-134`).

**Restart:** `tracking_states` serializa órdenes (`exchange_py_base.py:199-203`); restore solo reactiva `is_open`, `is_failure` → `_lost_orders` (`client_order_tracker.py:153-164`).

**Idempotencia:** `client_order_id` con nonce+instance_id (`connector/utils.py:50-83`); dedup fills por `trade_id` (`in_flight_order.py:356-361`); dedup status por `(exchange_order_id, current_state)`.

**REST/WS:** WS primario, REST backup explícito (`_status_polling_loop` "backup logic in case the main update source (websocket) fails" `exchange_py_base.py:805-813`); ambos alimentan el mismo tracker (mismo tipo `OrderUpdate`/`TradeUpdate`); dedup por estado/id; **REST autoritativo para estados finales**.

**Riesgo:** sin circuit breaker genérico (solo XRPL); `AsyncThrottler` rate-limit; `BudgetChecker`; `max_retries=10`; cooldowns; triple barrier SL/TP/time.

**Stale:** WS muerto → poll corto 5 s (`_get_poll_interval` `exchange_py_base.py:1129-1137`); lost-orders con límite; `TimeSynchronizer` para drift de reloj (`time_synchronizer.py:23-42`).

### 2.2 Freqtrade

**Órdenes:** `Order` SQLAlchemy (`persistence/trade_model.py:65-383`), única por `(ft_pair, order_id)` (`:83`), `ft_is_open` flag (`:94`), actualizado en `Order.update_from_ccxt_object()` (`:197-229`).

**Carrera CANCEL/FILL:** patrón **cancel → fetch posterior, "bail out" si sigue abierta**: `handle_cancel_enter` (`freqtradebot.py:1878-1967`) solo cancela si status no está en `NON_OPEN_EXCHANGE_STATES` (`:1896`); si el resultado del cancel sigue open → `return False` con log *"Avoid race condition where the order could not be cancelled coz its already filled. Simply bailing here is the only safe way - as this order will then be handled in the next iteration."* (`:1923-1928`). `cancel_order_with_result` (`exchange.py:1824-1853`): cancel → si no usable → **`fetch_order()` posterior** → si tampoco → fake dict `filled=0.0`. Fill tardío absorbido: `filled_amount>0` tras cancel → `update_trade_state()` + `PARTIALLY_FILLED` (`freqtradebot.py:1952-1961`), recalculando el trade desde órdenes (`recalc_trade_from_orders` `trade_model.py:1265-1344`).

**Cancelación:** `exchange.cancel_order` con `@retrier` (`exchange.py:1782-1809`); timeout vía `manage_open_orders` (`freqtradebot.py:1602-1633`) + `ft_check_timed_out` (`unfilledtimeout`, `strategy/interface.py:1734-1757`); **replace = cancel+create** (`replace_order` `freqtradebot.py:1693-1750`); `exit_timeout_count` → `emergency_exit` (`:1649-1660`).

**Órdenes abiertas:** `Order.get_open_orders()` DB (`trade_model.py:364-370`); `fetch_orders` con dedup por id (`exchange.py:1990`); **`startup_update_open_orders`** en restart (`freqtradebot.py:402-447`): fetch+update; orden >5 días y not found → asume cancelada → `handle_cancel_order(TIMEOUT)` (`:434-444`).

**Balance:** `Wallet(free, used, total)` (`wallets.py:21-25`); live: `get_balances()` + `fetch_positions()` + **`_strip_unrealized_pnl`** para exchanges cuyo total incluye PnL no realizado (`:225-243`); dry: reconstruye desde DB; rate-limit 1 h (`:245-264`); `check_exit_amount()` re-updatea si falla; `_safe_exit_amount` ajusta al wallet (98% umbral) (`freqtradebot.py:2047-2081`).

**Posiciones:** SSOT = Trade **derivado de sus órdenes** (`recalc_trade_from_orders` `trade_model.py:1265-1344` recalcula amount/open_rate/stake). Futuros: `PositionWallet.position` desde `fetch_positions` (`wallets.py:200-222`).

**Idempotencia:** **NO usa clientOrderId para dedup** (solo bitget:93). Dedup = UniqueConstraint + `handle_similar_open_order` (`freqtradebot.py:1843-1876`) + `handle_onexchange_order` (`:516-620`).

**REST/WS:** **WS SOLO OHLCV (klines), nunca órdenes** (`exchange_ws.py:21-297`); órdenes/trades SIEMPRE REST; REST gana por defecto; WS es acelerador de cache con frescura de media vela (`exchange.py:2680-2714`).

**Fees:** `get_fee` usa `ccxt.calculate_fee` (`exchange.py:2463-2505`); `get_real_amount` + `extract_cost_curr_rate` rechaza fee>2% (bug parsing) (`freqtradebot.py:2548-2551`); `update_fee` idempotente "Once per side".

**Riesgo:** `max_open_trades`; stoploss walk-up-only; `PairLocks`; `emergency_exit` market; timeouts.

**Stale:** `drop_incomplete` última vela (`exchange.py:2827-2868`); time-jump evict cache; dataprovider devuelve `(vacío, epoch 0)`.

### 2.3 NautilusTrader (v2, Rust)

**Máquina de estados:** `OrderStatus::transition` (`crates/model/src/orders/mod.rs:194-286`) **valida transiciones ilegales** (`InvalidStateTransition`); event-sourcing `OrderCore::apply`. **Transiciones que admiten Filled tras terminal/transitorio:** `(Canceled, Filled)=>Filled` (`:237`, comentario "Real world possibility"), `(PendingCancel, Filled)=>Filled` (`:259`), `(Submitted, Filled)` `:227`, `(PendingUpdate, Filled)` `:250`; `(PendingCancel, Accepted)=>Accepted` (`:257`, "Allow failed cancel requests").

**Carrera CANCEL/FILL:** `OrderManager::cancel_order` no-op si `cache.is_order_pending_cancel_local` (`order_manager/manager.rs:95-112`). `validate_fill_for_order` (`engine/mod.rs:3099-3122`): `is_duplicate_fill` (dedup por trade_id+side+qty+px, `orders/mod.rs:389-400`) + `position_contains_trade_id` + `check_overfill` (`:3340-3368`). `update_cached_order` (`:3143-3228`) maneja `InvalidStateTransition` en órdenes cerradas como **"venue race" log-debug** — no aborta.

**Reconciliación (el corazón):** tipos `FillSnapshot`/`VenuePositionSnapshot` (`reconciliation/types.rs:28,43`); `orders.rs` genera eventos de reconciliación (`create_reconciliation_rejected/canceled/expired/updated` con flag `reconciliation=true`); `reconcile_fill_quantity_mismatch` (`orders.rs:1267-1304`): **venue>cached infiere fill; venue<cached preserva el estado cache**. Engine: `reconcile_execution_report` dispatch (`engine/mod.rs:1044-1066`); `reconcile_order_status_report` **publica el reporte crudo ANTES de mutar estado** (`:1077-1109`, topic `reconciliation_raw_order_status_report_topic`); `reconcile_fill_report` mismo patrón (`:1415-1484`); `reconcile_order_with_fills` **fills reales primero, residuo inferido con avg_px** (`:1494+`); `reconcile_execution_mass_status` (`:1754-1809`). Live manager: `check_inflight_orders` (`live/execution/manager.rs:1443-1548`, threshold 5 s, tras `inflight_max_retries` genera `Rejected("INFLIGHT_TIMEOUT")` para Submitted o `Canceled(reconciliation=true)` para PendingUpdate/PendingCancel); `check_open_orders` (`:1796-1842`); `check_position_discrepancy` con tolerancia + grace window + retries (`:2976-3055`). Config defaults: `reconciliation=true, lookback_mins=60, generate_missing_orders=true, inflight 2s/5s/5, open_check 60min/5s, position 60min/60s/3` (`:257-349`).

**REST vs WS:** Bybit REST `generate_order_status_report(s)`/`generate_fill_reports`/`generate_mass_status` (`adapters/bybit/execution.rs:905-1089`); WS `dispatch_ws_message` → `dispatch_order_update`/`dispatch_execution_fill` (`websocket/dispatch.rs:202,302,644`); **dedup doc**: "For tracked identity produces proper order events... For untracked orders falls back to execution reports" (`:196-201`); sin garantía de orden entre topics (`:575-577`).

**Balance:** `AccountState.is_reported` (`model/src/events/account/state.rs:56`); `cash.rs apply` (`:235+`) + `recalculate_balance` (`:153`); `balance_free = total - locked` (`wallet.rs`); Portfolio `update_balances` muta in-place y **restaura si falla** (`portfolio/manager.rs:71-139`).

**Posiciones:** `Position` event-sourced (`position.rs:62`, `apply_fill` `:312`); `determine_position_id` cache-first con warning si mismatch (`engine/mod.rs:2927-2996`); hedging/netting según venue (`:3017,3095`); `handle_position_update` abre/actualiza/flip (`:3716-3756`).

**Idempotencia:** `reconciliation/ids.rs` FNV-1a hashing determinista para `TradeId`/`VenueOrderId` (`:34-35`); dedup por trade_id en engine.

**Restart:** `load_cache` (`engine/mod.rs:991-1014`): cache_general + cache_all + build_index + `check_integrity`; `sanitize_order_position_index` (`cache/mod.rs:2459-2474`).

**Fills parciales:** handler `filled` (`orders/mod.rs:1231-1310`): saturating add `last_qty`→`filled_qty`, `overfill_qty` si excede, `leaves_qty` saturating_sub, status=Filled si `new_filled_qty>=quantity` (`:1249`) si no PartiallyFilled (`:1256`); `avg_px_from_fills` (`:1314-1344`).

**Fees:** `FeeModel` trait (`execution/src/models/fee.rs:31-60`); `OrderFilled.commission`.

**Riesgo:** `check_orders_risk` (free balance + net long vs pending sells) (`risk/engine/mod.rs:1070+,1120-1150`); `execution_gateway` `TradingState::Halted/Reducing/Active` (`:2140-2226`); `handle_submit_order` reduce-only validation (`:580-652`); throttled submit.

**Stale:** `is_stale` (`data/src/aggregation.rs:451-452`, `ts_init < ts_last`); `sanitize_order_position_index` en carga.

---

## 3. Comparación contra OCM

| Capacidad | Bot | Qué hace | OCM actual | Bybit/CCXT | Conclusión |
|---|---|---|---|---|---|
| Estado intermedio cancel | Hummingbot | `PENDING_CANCEL` si cancel async (`exchange_py_base.py:556-571`) | `CANCELLED` terminal directo, sin transitorio (`order.py:64-74`) | Cancel async; estado final vía WS/fetch | **OCM ADR-0029 ya propone `CANCELLING` — patrón confirmado** |
| Resolución carrera CANCEL/FILL | Nautilus | `(PendingCancel, Filled)=>Filled` en tabla de transiciones (`orders/mod.rs:259`) | CANCELLED terminal → fill se pierde en silencio | Fill prevalece oficialmente | **ADR-0029: fill prevalece — confirmado** |
| Ambigüedad no resuelta localmente | Hummingbot | Espera update autoritativo; REST `fetch_order` decide (`exchange_py_base.py:1032-1040`) | `fetch_state` fail-closed ya existe en `OrderTransport` (`transport.py:96-128`) | REST es autoridad recuperable | **OCM ya tiene la base; falta `cancel` en port** |
| Cancel duplicado | Nautilus | `OrderManager.cancel_order` no-op si pending_cancel local (`manager.rs:95-112`) | no-op si terminal/inexistente (propuesto en ADR-0029) | idempotente por diseño | **ADR-0029 correcto** |
| Fills tardíos post-cancel | Hummingbot | Caché 30 s de órdenes terminadas (`client_order_tracker.py:34-35`) | `_fill` maneja `_open.pop`; sin caché explícito de terminadas | WS puede entregar fill tardío | **Patrón utilizable; OCM lo cubre con fetch de verificación** |
| Orden huérfana/loop | Freqtrade | `manage_open_orders` cada iteración (`freqtradebot.py:1602-1633`) + timeout | No existe | `/v5/order/realtime` | **ADR-0029 ya propone `manage_open_orders` — confirmado** |
| Reconciliación in-flight timeout | Nautilus | `check_inflight_orders` → `Rejected(INFLIGHT_TIMEOUT)`/`Canceled(reconciliation=true)` (`manager.rs:1443-1548`) | No existe; `LiveExecutor._reconcile` fail-closed solo submit (`live_executor.py:229-261`) | Aceptación asíncrona | **Patrón a considerar para CANCELLING atascado** |
| Reconciliación de reportes crudos | Nautilus | Publica reporte crudo ANTES de mutar estado (`engine/mod.rs:1077-1109`) | Callbacks síncronos; sin bus de eventos en trading | — | **⚠️ Útil cuando exista event bus (fuera de alcance F3)** |
| Balance total vs available | Hummingbot | `walletBalance` total; UNIFIED: `walletBalance-locked-IM-MM` (`bybit_exchange.py:537-538`) | No existe `fetch_balance` (`ccxt_adapter.py` solo create/fetch_order) | `totalAvailableBalance` es el campo operativo UNIFIED | **ADR-0030 ya fija `totalAvailableBalance` — confirmado** |
| PnL no realizado en balance | Freqtrade | `_strip_unrealized_pnl` (`wallets.py:225-243`) | — | `unrealisedPnl` en wallet-balance | **⚠️ Considerar al materializar el saldo** |
| Balance stale | Freqtrade | Rate-limit 1 h + re-update (`wallets.py:245-264`) | — | Aviso oficial de latencia en volatilidad | **ADR-0030: freshness + fail-closed — correcto** |
| Posición exchange sin reflejo OCM | Hummingbot | `amount==0 → remove_position` (`bybit_perpetual_derivative.py:489-490`) | PositionStore SSOT BC-43; divergencia = crítica, no auto-corregir | `/v5/position/list` | **ADR-0030: NO auto-corregir — confirmado** |
| Reconciler posición con tolerancia | Nautilus | `check_position_discrepancy` tolerancia+grace window+retries (`manager.rs:2976-3055`) | — | — | **Patrón de calibración (política de riesgo) para PortfolioReconciler** |
| Mismatch de qty en reconciliación | Nautilus | venue>cached infiere fill; venue<cached preserva (`orders.rs:1267-1304`) | — | — | **⚠️ Semántica conservadora; OCM fail-closed es compatible** |
| Sizing pre-trade | Hummingbot | `BudgetChecker.adjust_candidates` (`budget_checker.py:38-105`) | `RiskManager.capital_usd` fijo (`manager.py:112-118`) | `totalAvailableBalance` | **ADR-0030: risk consume saldo vía port — confirmado** |
| Restart recovery | Freqtrade | `startup_update_open_orders` (`freqtradebot.py:402-447`) | ADR-0027 recovery; sin rehidratación de órdenes abiertas | fetch_open_orders | **ADR-0029 §Fallos: reconstruir desde exchange — confirmado** |
| Idempotencia | Nautilus | trade_id/venue_order_id hash determinista (`reconciliation/ids.rs:34-35`) | client_order_id UUID (LiveExecutor) | clientOrderId soportado | **OCM correcto; no adoptar hashing FNV** |

---

## 4. Evaluación arquitectónica

| Patrón | Evaluación | Por qué |
|---|---|---|
| `PENDING_CANCEL`/`CANCELLING` transitorio (Hummingbot, Nautilus) | ✅ **Mejor que OCM hoy** | OCM decreta `CANCELLED` sin confirmación; el transitorio elimina la divergencia silenciosa. ADR-0029 ya lo propone. |
| Tabla de transiciones explícita con `(PendingCancel, Filled)=>Filled` (Nautilus) | ✅ **Mejor que OCM hoy** | ADR-0029 lo expresa como política "fill prevalece"; Nautilus lo codifica en el grafo. OCM lo hará en `_VALID_TRANSITIONS` + `CANCELLING`. |
| "Bail out & retry next iteration" (Freqtrade) | ⚠️ **Útil parcialmente** | Simple y seguro; OCM lo cubre con "CANCELLING + reintento + fetch de verificación" (más determinista que esperar iteración). |
| Caché TTL de órdenes terminadas (Hummingbot) | ⚠️ **Útil parcialmente** | Absorbe fills tardíos; OCM lo sustituye por `fetch_state` de verificación tras cancel. Cache es optimización, no requisito. |
| `manage_open_orders` + timeouts (Freqtrade, Nautilus `check_inflight_orders`) | ✅ **Necesario** | Es el único caller real de cancel en OCM; ADR-0029 lo propone. |
| Publicar reporte crudo antes de mutar (Nautilus) | ❌ **No aplicable hoy** | Requiere event bus en trading; OCM es 100% síncrono/callback (ADR-0029 §Decisión 6). |
| Reconciliación con tolerancia + grace window (Nautilus) | ⚠️ **Útil parcialmente** | Buena calibración; OCM lo traduce a tolerancias configurables de política de riesgo (fail-closed). |
| `totalAvailableBalance` (UNIFIED) para disponible (Hummingbot Bybit) | ✅ **Confirmado por ambos** | OCM ADR-0030 lo fija; Hummingbot usa la misma semántica. |
| `_strip_unrealized_pnl` (Freqtrade) | ⚠️ **Útil parcialmente** | Evita doble contabilización; OCM lo considera al materializar saldo (unrealisedPnl separado). |
| `BudgetChecker` pre-trade (Hummingbot) | ⚠️ **Útil parcialmente** | OCM ya rechaza SELL sin posición (`oms.py` F1 fail-closed) y RiskManager bloqueará con saldo real (ADR-0030). |
| `Wallets` como servicio separado (Freqtrade) | ❌ **No aplicable** | OCM: portfolio es dueño del estado patrimonial (ADR-0030); wallets no serían un BC nuevo. |
| `Trade derivado de órdenes` (Freqtrade recalc) | ❌ **No aplicable** | OCM ya tiene PositionStore SSOT + WAC (ADR-0025/0027); no se reemplaza. |
| `client_order_id` con hash FNV (Nautilus) | 🚫 **Incompatible** | OCM usa UUID4 completo (documentado `order.py`, evita colisiones de store); el hash no aporta. |
| Monolito `FreqtradeBot` | 🚫 **Incompatible** | God Object que mezcla orquestación, órdenes, fees, stoploss, RPC. OCM = Clean/Hexagonal; **no se reproduce**. |
| `OrderState` enum por asignación (Hummingbot) | ❌ **No aplicable** | OCM tiene grafo explícito con transiciones validadas; más seguro (fail-fast). |

---

## 5. No copiar arquitectura externa

**Regla cumplida:** no se importa código de ningún bot. Solo se extraen invariantes. La traducción a OCM respeta: Hexagonal, BCs, Kappa/eventos, ports/adapters, composition roots, eventos inmutables, SoT único.

**Transformaciones concretas:**
- El patrón "PENDING_CANCEL con fill prevalece" (Nautilus `(PendingCancel,Filled)=>Filled`) → `CANCELLING` en `order.py` + resolución en `OMS._fill` (dominio, sin ccxt).
- El "reconciler que publica crudo y luego muta" (Nautilus) → NO se adopta sin event bus; OCM usará `fetch_state` + callbacks existentes.
- El "Wallets" (Freqtrade) → NO como BC nuevo; el saldo se materializa en portfolio (ADR-0030), risk consume vía `BalancePort`.
- El "recalc_trade_from_orders" (Freqtrade) → NO; OCM ya tiene PositionStore/WAC determinista.
- `manage_open_orders` (Freqtrade) → concepto adoptado como caller único de cancel (ADR-0029), no como clase Freqtrade.

---

## 6. Comparación específica B-MD-008

**Escenarios evaluados para `SUBMITTED → CANCELLING → CANCELLED` y `SUBMITTED → CANCELLING → FILLED`:**

| Aspecto | OCM ADR-0029 | Nautilus | Hummingbot | Freqtrade | Mejor patrón |
|---|---|---|---|---|---|
| Confirmación | `fetch_state` de verificación; nunca CANCELLED sin confirmación | Event-sourcing; reconcile_report raw-then-mutate | REST autoritativo + caché de fills | `cancel_order_with_result`→fetch→fake dict | **OCM (fetch de verificación)** — Nautilus raw-then-mutate exige bus |
| Idempotencia | no-op si terminal/inexistente; dedup por fetch_state | `is_order_pending_cancel_local` no-op | dedup por (id, estado) | UniqueConstraint + bail-retry | **OCM + Nautilus (no-op local)** |
| Eventos duplicados | `transition(FILLED)` desde FILLED no-op controlado; `_fill` `_open.pop` | `is_duplicate_fill` por trade_id+side+qty+px | dedup por trade_id | recalc idempotente | **OCM ya cubre; Nautilus añade dedup por 4-tupla (opcional)** |
| REST/WS | REST autoridad recuperable; WS opcional/futuro | REST generate_reports; WS dispatch; dedup tracked/untracked | WS primario + REST backup; REST decide estados finales | WS SOLO OHLCV; órdenes por REST | **OCM + Freqtrade: REST para órdenes; WS incremental** |
| Pérdida de conexión | CANCELLING visible, reintento+backoff+alerta | `check_inflight_orders` timeouts → Rejected/Canceled(reconciliation) | lost-orders loop + poll adaptativo | manage_open_orders reintenta | **OCM correcto; añadir timeout acotado tipo Nautilus** |
| Restart durante CANCELLING | reconstruir desde exchange (fetch_open_orders/fetch_order) | `load_cache` + reconciliation en startup | restore tracking_states (solo is_open) | `startup_update_open_orders` | **OCM correcto; adición: timeout de in-flight tras N reintentos** |
| Reconciliación | `manage_open_orders` periódico | `check_open_orders` 60min/5s | poll REST + lost-orders | `manage_open_orders` por vela | **OCM confirmado** |

**¿Mejora la solución de un bot el diseño actual de ADR-0029?** → **NO hay cambio sustancial.** El diseño CANCELLING con fill-prevalece es equivalente a los mejores patrones de Nautilus (`(PendingCancel,Filled)=>Filled`) y Hummingbot (`PENDING_CANCEL` + fill por cantidad). **Dos adiciones opcionales (no modifican el ADR):** (a) timeout acotado para `CANCELLING` atascado (inspirado en `check_inflight_orders` de Nautilus): tras N reintentos → alerta + estado visible persistente, nunca inventar estado final (ya implícito en ADR-0029 §Fallos); (b) dedup de fill por 4-tupla (trade_id+side+qty+px) como red de seguridad contra dobles fills (Nautilus `is_duplicate_fill`) — complementa el dedup por order_id de OCM.

---

## 7. Comparación específica B-MD-009

**Escenario evaluado: `exchange balance → portfolio → risk → execution`:**

| Aspecto | OCM ADR-0030 | Nautilus | Hummingbot | Freqtrade | Mejor patrón |
|---|---|---|---|---|---|
| Balance utilizable | `totalAvailableBalance` (UNIFIED) | `balance_free = total - locked` | UNIFIED: `walletBalance-locked-IM-MM` | `free` de CCXT (availableToWithdraw) | **OCM + Hummingbot: totalAvailableBalance/derivado correcto** |
| Balance stale | freshness (B-MD-001) + fail-closed | `is_stale` en data; account event-sourced | snapshot + apply updates | rate-limit 1h + re-update | **OCM correcto (freshness + bloqueo)** |
| Detección mismatch | tolerancias configurables; material → bloquear | `check_position_discrepancy` tolerancia+grace+retries | `amount==0 → remove_position` | `_safe_exit_amount` 98% | **OCM correcto; calibración de tolerancias = política de riesgo** |
| Tolerancias | configurables (no se fijan en ADR) | default position 60min/60s/3 | — | 98% umbral exit | **OCM correcto (no inventar números); Nautilus da guía de dimensiones** |
| Fail-closed | discrepancia material → bloquear órdenes; imposible → halt global | `TradingState::Halted` gateway | BudgetChecker anula órdenes | dependency exception | **OCM más estricto que todos (correcto para live)** |
| Startup reconciliation | gate de arranque obligatorio | reconciliation startup + lookback 60min | re-init position mode | `startup_update_open_orders` | **OCM correcto** |
| Post-fill reconciliation | asíncrono tras fill | evento fill → update account | fills actualizan in-flight → snapshot | recalc desde órdenes | **OCM correcto (asíncrono, no en camino de submit)** |
| PnL no realizado | `unrealisedPnl` separado (por activo) | account balances + commissions | `unrealisedPnl` en wallet | `_strip_unrealized_pnl` | **⚠️ Añadir a ADR-0030: explicitar que `total` de UTA incluye PnL no realizado y separar `unrealisedPnl` (no restarlo del disponible operativo)** |
| Restart recovery | gate materializa saldo; rehidratar posiciones (ADR-0027) | `load_cache` + check_integrity | restore tracking | startup + sqlite | **OCM correcto** |

**¿Mejora la solución de un bot el diseño actual de ADR-0030?** → **NO hay cambio estructural.** Portfolio-SSOT + `totalAvailableBalance` + fail-closed es coherente con el mejor de los tres bots. **Dos adiciones opcionales al ADR-0030 (no lo invalidan):** (a) explicitar la separación `unrealisedPnl` al materializar el saldo (lección de Freqtrade `_strip_unrealized_pnl` y del propio campo `unrealisedPnl` de Bybit) — el disponible operativo es `totalAvailableBalance`; el `equity`/`walletBalance` incluye PnL no realizado y no debe usarse como capital de riesgo; (b) guía de dimensiones de calibración de tolerancias basada en Nautilus (intervalo, threshold, retries) para fijarlas en Sandbox (ADR-0030 ya dice "se determina por medición/pruebas en Sandbox").

---

## 8. Resultado final

### Tabla resumen

| Problema | OCM actual | Bybit/CCXT | Bot de referencia | Mejor patrón | Propuesta OCM |
|---|---|---|---|---|---|
| Cancel real | `OMS.cancel()` local-only, sin callers (`oms.py:300-317`) | `cancel_order` async; ack; estado vía WS/fetch; fill prevalece | Hummingbot `PENDING_CANCEL`; Nautilus `(PendingCancel,Filled)=>Filled` | Estado transitorio + fill prevalece + fetch de verificación | ADR-0029 `CANCELLING` (sin cambios) |
| Carrera CANCEL/FILL | CANCELLED terminal → fill perdido en silencio | doble-Filled; `EC_TooLateToCancel`; fill gana | Nautilus grafo explícito; Hummingbot caché + REST autoritativo | Fill prevalece; no decidir localmente | ADR-0029 (sin cambios) + dedup 4-tupla opcional |
| Órdenes abiertas huérfanas | No existe loop | `/v5/order/realtime` | Freqtrade `manage_open_orders`; Nautilus `check_open_orders` | Loop periódico + timeout | ADR-0029 `manage_open_orders` (confirmado) |
| Balance real | No existe `fetch_balance`; `capital_usd` fijo | UNIFIED `totalAvailableBalance` (free deprecado) | Hummingbot UTA; Freqtrade wallets | totalAvailableBalance como disponible operativo | ADR-0030 (confirmado) |
| Reconciliación patrimonial | No existe | wallet-balance + position list | Nautilus tolerancia+grace; Hummingbot `amount==0→remove` | Portfolio SSOT; tolerancias configurables; NO auto-corregir | ADR-0030 (confirmado) + separar unrealisedPnl |

### ¿Qué debemos adoptar?

1. **Estado transitorio `CANCELLING` con fill-prevalece** (ADR-0029) — ya propuesto; confirmado como el mejor patrón entre los 3 bots. **Adoptar tal cual.**
2. **`manage_open_orders` como caller único de cancel** (ADR-0029) — confirmado por Freqtrade y Nautilus. **Adoptar.**
3. **Portfolio-SSOT con `totalAvailableBalance`** (ADR-0030) — confirmado. **Adoptar tal cual.**
4. **Tolerancias configurables + gate de arranque + NO auto-corregir posiciones** (ADR-0030) — confirmado. **Adoptar.**
5. **Separar `unrealisedPnl` del disponible operativo** al materializar el saldo (lección Freqtrade/Bybit). **Adoptar como aclaración del ADR-0030.**
6. **Timeout acotado para CANCELLING atascado** (lección Nautilus `check_inflight_orders`): tras N reintentos → alerta + estado visible persistente; nunca inventar estado final. **Adoptar como refinamiento del ADR-0029 §Fallos.**

### ¿Qué NO debemos adoptar?

1. **Monolito FreqtradeBot** (God Object) — incompatible con Clean/Hexagonal OCM.
2. **Wallets como servicio independiente** (Freqtrade) — duplica el SoT patrimonial; portfolio ya es dueño (ADR-0030).
3. **`recalc_trade_from_orders`** (Freqtrade) — OCM ya tiene PositionStore + WAC determinista (ADR-0025/0027).
4. **Hash FNV para order_id** (Nautilus) — OCM usa UUID4 completo (documentado, evita colisiones).
5. **Publicar reportes crudos antes de mutar** (Nautilus) — requiere event bus en trading; fuera de alcance F3 (ADR-0029 §Decisión 6).
6. **Caché TTL de órdenes terminadas** (Hummingbot) — optimización; OCM usa `fetch_state` de verificación (más determinista).
7. **`OrderState` por asignación directa** (Hummingbot) — OCM tiene grafo con transiciones validadas (más seguro).

### ¿Qué cambia en B-MD-008?

**La investigación de bots CONFIRMA el ADR-0029 sin modificarlo estructuralmente.** No hay solución superior a `CANCELLING` + fill-prevalece + `manage_open_orders`. Solo se recomienda añadir (opcional, no bloqueante): timeout acotado para CANCELLING atascado y dedup de fill por 4-tupla.

### ¿Qué cambia en B-MD-009?

**La investigación CONFIRMA el ADR-0030 sin modificarlo estructuralmente.** Portfolio-SSOT + `totalAvailableBalance` + fail-closed es el mejor diseño entre los 3 bots. Solo se recomienda añadir como aclaración: separar `unrealisedPnl` (no usarlo como capital de riesgo) y dimensionar tolerancias con la guía de Nautilus (intervalo/threshold/retries) durante calibración en Sandbox.

### ¿Hay una solución mejor que la propuesta actual?

**No.** Ningún bot ofrece una solución superior a los ADR-0029/0030. El diseño OCM ya incorpora las mejores invariantes (fill prevalece, estado transitorio, SoT único, fail-closed, fetch de verificación REST como autoridad recuperable). Las mejoras identificadas son refinamientos de calibración y aclaraciones, no cambios de arquitectura.

---

## 9. Regla de evidencia

- **OCM** — archivo:línea citado (oms.py, transport.py, order.py, ccxt_adapter.py, ADR-0029/0030).
- **Bybit** — documentación oficial vía ADR-0029/0030 (mercado=IOC, cancel async, doble-Filled, UTA totalAvailableBalance, free deprecado 2025-01-09).
- **CCXT** — `bybit.py:4611` cancel_order, `:3525` fetch_balance, `:3363` parse_balance (free→availableToWithdraw).
- **Bots** — ruta exacta bajo `/tmp/opencode/bots/<bot>/` (cita en §2).
- **INFERENCE** — solo donde se marca explícitamente (p. ej. "dedup 4-tupla opcional" como red de seguridad).

**Limitaciones:** los tres bots son versiones open-source en desarrollo; el código de Hummingbot tiene legado Cython (deprecado de facto); NautilusTrader v2 es Rust (los stubs Python no contienen lógica); ninguna conclusión atribuye capacidades no verificadas.

---

## 10. Recomendación final

1. **Patrón que conservar:** `CANCELLING` transitorio + fill prevalece (B-MD-008) y Portfolio-SSOT + `totalAvailableBalance` + fail-closed (B-MD-009) — confirmados por el benchmark como el estado del arte.
2. **Patrón que modificar:** ninguno estructural. Añadir como refinamiento: timeout acotado de CANCELLING (Nautilus) y separación de unrealisedPnl (Freqtrade/Bybit).
3. **Patrón que descartar:** monolito FreqtradeBot, Wallets como BC, recalc-from-orders, hash FNV, raw-then-mutate sin bus, caché TTL, OrderState por asignación.
4. **Arquitectura recomendada para OCM:** la de ADR-0029/0030 tal como están redactadas (PROPUESTA → aprobar), con las dos aclaraciones menores documentadas en este informe.
5. **Qué debería quedar escrito en los ADR:** ADR-0029 — añadir "timeout acotado de CANCELLING (alerta + estado visible, nunca estado final inventado)"; ADR-0030 — añadir "`unrealisedPnl` se separa del disponible operativo; `equity`/`walletBalance` no es capital de riesgo".
6. **Qué debería implementarse posteriormente:** el orden sugerido de ADR-0029/0030 (sandbox → CCXTAdapter cancel_order/fetch_balance → ports → dominio → loop/reconciler → política → tests).
7. **Qué pruebas demostrarían que quedó resuelto:** test carrera cancel/fill (fill prevalece, cancel confirmado→CANCELLED), test cancel duplicado idempotente, test orden huérfana limpiada por manage_open_orders, test timeout CANCELLING sin inventar estado, test sizing contra saldo real (no configurado), test discrepancia material→bloqueo, test gate de arranque (capital configurado ≠ real → no live), test restart durante CANCELLING (reconstrucción desde exchange), test reconciler con tolerancias (MATCH/MISMATCH sin auto-corregir posiciones).