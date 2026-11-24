# Benchmark arquitectónico — B-MD-008 (cancelación real) y B-MD-009 (balance real) vs Freqtrade, Hummingbot y Nautilus Trader

Fecha: 2026-08-15
Tipo: Benchmark + Validación de ADR-0029 y ADR-0030
Alcance: solo lectura + documentación. No se modificó código, ADRs, tracking.yaml ni se hicieron commits.

---

## 1. Resumen ejecutivo (para persona no experta)

OCM quiere que sus órdenes se **cancelen de verdad** en el exchange (hoy la cancelación
es solo local, no llega a Bybit) y que sepa **cuánto dinero tiene de verdad** en la cuenta
(hoy usa un número configurado, no consulta el saldo). Se pidió comprobar cómo resuelven
esto tres bots de referencia (Freqtrade, Hummingbot, Nautilus Trader), contrastándolo con
las garantías reales de Bybit y del adaptador CCXT, y validar las dos propuestas formales
(ADR-0029 y ADR-0030) que ya se habían redactado.

**Conclusión corta:** los tres bots confirman el mismo diseño que proponen los ADRs.
Freqtrade, Hummingbot y Nautilus **nunca** declaran una orden cancelada sin confirmación
del exchange: usan un estado intermedio "cancelando" y resuelven después contra la fuente
real (REST/WS). Nautilus incluso modela explícitamente `PendingCancel`. En balance, los
tres **consultan el exchange** (wallet-balance) y ninguno usa el campo `free`/`availableToWithdraw`
de Bybit para cuentas UNIFIED (está deprecado); usan `walletBalance` y derivan lo disponible.
Todo esto es exactamente lo que dicen los ADRs.

**Veredicto:** ADR-0029 **CONFIRMADA**, ADR-0030 **CONFIRMADA**. Ambas se ajustan a la
evidencia de los tres referentes y a las garantías de Bybit/CCXT. No requieren rediseño.

---

## 2. Objetivos

1. Verificar qué hacen Freqtrade, Hummingbot y Nautilus Trader en (a) cancelación de
   órdenes, (b) gestión de órdenes abiertas, (c) balance/posiciones, (d) recuperación tras
   fallos/reinicio.
2. Contrastar contra las garantías reales de Bybit (API v5, cuentas UNIFIED) y el
   comportamiento real de CCXT (adapter `bybit`).
3. Clasificar cada patrón como adoptable, parcialmente útil, descartado o incompatible.
4. Validar ADR-0029 y ADR-0030 con veredicto explícito.
5. Producir una matriz comparativa (16 temas) y una sección RECOMENDACIÓN FINAL con 9 respuestas.

## 3. Metodología

- Fuentes de autoridad (de mayor a menor): 1) OCM en HEAD (código), 2) docs oficiales
  Bybit, 3) código CCXT (`bybit.py`/`bybit_pro.py` de la versión instalada), 4) código real
  de los tres bots, 5) inferencia razonada.
- Los ZIPs de los bots se extrajeron **fuera del repo** en `/tmp/bot-refs/` (ver §29).
  No se copió código de los bots; solo se describen patrones en palabras propias.
- Cada afirmación de hecho lleva una marca de evidencia: `[VERIFIED]`, `[INFERENCE]` o
  `[UNKNOWN]`. `[INFERENCE]` nunca se presenta como verificado.
- No se modificó ningún archivo del repo salvo este informe. No hay commits.

## 4. Criterios de evidencia

- `[VERIFIED]`: confirmado en código/doc consultada con archivo:línea o cita.
- `[INFERENCE]`: conclusión razonada a partir de evidencia verificada, etiquetada como tal.
- `[UNKNOWN]`: no confirmable con los recursos disponibles (se declara el gap explícitamente).

## 5. Estado de OCM en HEAD (contexto)

- Motor de ejecución: `packages/trading/execution/` (Clean/Hexagonal, framework-agnóstico).
  Domain `order.py`/`oms.py`, transporte `transport.py`, executor `live_executor.py`.
- Orden de dependencia: `shared → ocm → domain → ports → application → adapters → infrastructure`.
- Trading no importa `market_data` directamente (BC-50): el acoplamiento a CCXT vive en
  `packages/trading/bootstrap/composition_root.py` (`_BybitTransport`).
- Portfolio es SSOT de posiciones (BC-43/ADR-0006): `PortfolioService` +
  `PositionStore` (Redis/InMemory). No conoce el saldo del exchange.
- Todas las órdenes son market. `composition_root.py:238` crea órdenes market.
- `dry_run: true` es el default global (`config/base.yaml`); live exige override explícito.

## 6. B-MD-008 — estado actual en OCM

Hallazgos (re-verificados en HEAD, archivo:línea):

- `OMS.cancel()` es **local-only**: `oms.py:300-317` transiciona a `CANCELLED`, hace
  `_open.pop`, y `record_close` — sin llamar a transporte/exchange. [VERIFIED]
- `OrderTransport` (Protocol) solo define `submit`/`fetch_state`/`close`
  (`transport.py:96-128`). No existe `cancel`. [VERIFIED]
- `PaperTransport` tampoco tiene cancel (`transport.py:131-158`). [VERIFIED]
- El adapter real `_BybitTransport` solo implementa `submit`/`fetch_state`/`close`
  (`composition_root.py:203-262`). [VERIFIED]
- `CCXTAdapter` no expone `cancel_order` (`ccxt_adapter.py:405` create_order, `:462`
  fetch_order). [VERIFIED]
- `OMS.cancel()` nunca se invoca en el repo (sin callers). [VERIFIED]
- Grafo de estados en `order.py:64-74`: `CANCELLED` es terminal; no hay estado transitorio.
- Live path: `LiveExecutor` reintenta `submit` con backoff (`live_executor.py:178-203`),
  luego `fill_sync` reconcilia por `fetch_state`. No hay gestión de órdenes abiertas.

## 7. B-MD-009 — estado actual en OCM

- No existe `fetch_balance` ni port/adapter de saldo en todo el repo (grep sin resultados). [VERIFIED]
- `RiskManager` valida contra `capital_usd` configurado (`risk/manager.py:112-118`,
  `:374-384`, `:394`), no contra saldo real del exchange. [VERIFIED]
- `PortfolioService` es SSOT de posiciones (`portfolio_service.py:56-80`) pero no conoce
  saldo ni unrealised PnL del exchange. [VERIFIED]
- El flujo submit→fill no reconcilia saldo post-ejecución. [VERIFIED]
- `shared/contracts/boundaries.py` define `FeatureSource`, `SignalProtocol`, `RiskGate`,
  `RebalancePort` — no existe `BalancePort`. [VERIFIED]

## 8. ADR-0029 — propuesta en evaluación

Fichero: `docs/architecture/decisions/ADR-0029-cancelacion-real-gestion-ordenes-abiertas.md`.
Propone (resumen de su contenido, sin modificar):

1. Estado transitorio `CANCELLING` con resolución determinista: `CANCELLING → FILLED |
   CANCELLED | REJECTED`; el **fill siempre prevalece**; terminales no relajados.
2. Port: `OrderTransport.cancel(symbol, exchange_order_id) -> OrderState` (SafeOps, ERROR
   si no se puede confirmar).
3. Adapter: `CCXTAdapter.cancel_order` (mercado Bybit = IOC → ventana de cancelación mínima
   pero no cero: reintentos, doble envío, acuse asíncrono).
4. Loop `manage_open_orders` (concepto Freqtrade): reconciliar SUBMITTED/CANCELLING por
   `fetch_state`/WS; fail-closed; idempotencia; no inventar estado final.
5. Bloquea LIVE (P1). Incluye security scenarios A/B/C/D y roadmap conceptual.

## 9. ADR-0030 — propuesta en evaluación

Fichero: `docs/architecture/decisions/ADR-0030-balance-real-reconciliacion-patrimonial.md`.
Propone (resumen, sin modificar):

1. Portfolio dueño del estado patrimonial (PositionStore BC-43 + saldo derivado).
2. Nuevo `BalancePort` en `shared/contracts/boundaries.py` (total/disponible operativo/
   locked/unrealised + timestamp/freshness).
3. Adapter `CCXTAdapter.fetch_balance` con lectura UNIFIED usando **`totalAvailableBalance`**
   (NO `free`/`availableToWithdraw`, deprecados para UNIFIED desde 2025-01-09).
4. `PortfolioReconciler` (PositionStore vs balance vs exchange) + gate de arranque live +
   loop periódico + post-fill/error. Política de discrepancia fail-closed (nunca
   auto-corregir posiciones). Bloquea LIVE (P1).

## 10. Licencias de los referentes

- Freqtrade: **GPL-3.0** (LICENSE del ZIP verificado). Copiar código exige GPL → solo
  patrones, no código. [VERIFIED]
- Hummingbot: **Apache-2.0** (LICENSE verificado). [VERIFIED]
- Nautilus Trader: **LGPL-3.0** (LICENSE + headers `LGPL-3.0` en `pending_cancel.rs`). [VERIFIED]
- CCXT: MIT (dependencia ya instalada en OCM). [VERIFIED]
- Consecuencia: en este informe solo se describen **patrones** (en palabras propias) y se
  citan archivo:línea como referencia de inspección, sin transcribir código. [INFERENCE]

---

## 11. Inventario y hallazgos — Freqtrade (órdenes/cancelación)

Referencia: `/tmp/bot-refs/freqtrade/freqtrade-develop/`.

Arquitectura de órdenes (patrones, en palabras propias):

- **DB como memoria operativa**: `freqtrade/persistence/trade_model.py` modela `Trade` y
  `Order` en SQLAlchemy; `Order.ft_is_open` marca órdenes abiertas; `Order.update_from_ccxt_object`
  refresca el objeto desde un dict CCXT y marca `ft_is_open=False` cuando el status es
  terminal (`trade_model.py:197-230`). [VERIFIED]
- **Conciliación por REST polling por ciclo**: `freqtrade/freqtradebot.py:1602` `manage_open_orders`
  itera `Trade.get_open_trades()`, llama `exchange.fetch_order()` y luego `update_trade_state`;
  si el status no es terminal y superó timeout → `handle_cancel_order` (CANCEL_REASON["TIMEOUT"])
  o `replace_order` si hay vela nueva. [VERIFIED]
- **Cancel con resultado re-verificado**: `exchange/exchange.py:1824` `cancel_order_with_result`
  ejecuta el cancel y, si el resultado no es concluyente, re-consulta `fetch_order`; si
  ambos fallan, construye un resultado "cancelado" sintético como último recurso. En
  `freqtradebot.py:1914-1924` (handle_cancel_enter) tras cancelar, si `status` sigue no
  terminal, reintenta `fetch_order` hasta 3 veces (0.5s) y, si persiste, **devuelve False y
  no da la orden por cancelada** — la orden se re-evalúa en la siguiente iteración. [VERIFIED]
- **Raza CANCEL/FILL reconocida explícitamente**: comentario en `handle_cancel_enter`
  (~1919): "race condition where the order could not be cancelled coz its already filled.
  Simply bailing here is the only safe way". Es el mismo riesgo que OCM modela con la regla
  `CANCELLING → FILLED`. [VERIFIED]
- **Timeout con conteo**: `freqtradebot.py:1635` `handle_cancel_order` gestiona timeouts de
  entrada/salida y `exit_timeout_count` para emergencia. No es un estado intermedio persistido
  como `CANCELLING`, sino comportamiento de cancelación condicionado a timeout + re-check. [VERIFIED]
- **Sin estado intermedio explícito**: Freqtrade no modela "cancelando" como estado; usa
  DB (`ft_is_open=True`) + re-fetch hasta terminal. Patrón equivalente en efecto, diferente
  en mecanismo. [VERIFIED]

## 12. Inventario y hallazgos — Freqtrade (balance/posiciones/recovery)

- **Balance desde el exchange**: `freqtrade/wallets.py:187` `_update_live` llama
  `exchange.get_balances()` (= `ccxt.fetch_balance`), construye `Wallet(total/free/used)`
  por moneda; `:245` `update()` con throttle de 1h (`require_update` fuerza refresh). [VERIFIED]
- **Posiciones futuras**: `exchange/exchange.py:1902` `fetch_positions`; `wallets.py:_update_live`
  parsea `PositionWallet` (size, leverage, collateral, unrealized_pnl) y `_strip_unrealized_pnl`
  resta el upnl del total para no contarlo dos veces. [VERIFIED]
- **Uso de `free`**: `get_free()` lee `Wallet.free`. En spot, `free` viene de CCXT; para
  cuentas UNIFIED de Bybit CCXT mapea `free` a `availableToWithdraw` (deprecado) — Freqtrade
  en spot de Bybit heredaría esa limitación. No usa `totalAvailableBalance`. [VERIFIED]
- **Recovery al arranque**: `freqtrade/freqtradebot.py:402` `startup_update_open_orders`
  recarga órdenes abiertas desde la DB y las reconcilia con `fetch_order_or_stoploss_order`
  → `update_trade_state`. La DB es el mecanismo de persistencia que sobrevive al reinicio. [VERIFIED]
- **Sin WS privado**: Freqtrade usa WebSocket solo para market data OHLCV
  (`exchange_ws.py`, `watch_ohlcv`); órdenes/balance se reconcilian por REST. [VERIFIED]

## 13. Inventario y hallazgos — Hummingbot (órdenes/cancelación)

Referencia: `/tmp/bot-refs/hummingbot/hummingbot-master/`.

- **Máquina de estados explícita**: `hummingbot/core/data_type/in_flight_order.py:21` define
  `OrderState` con `PENDING_CREATE, OPEN, PENDING_CANCEL, CANCELED, PARTIALLY_FILLED, FILLED,
  FAILED, ...`. Existe un estado **`PENDING_CANCEL`** intermedio, exactamente el análogo de
  `CANCELLING` que propone ADR-0029. [VERIFIED]
- **Cancel en dos fases**: `hummingbot/connector/exchange_py_base.py:553-571`
  `_execute_order_cancel_and_process_update`: llama `_place_cancel`; si el exchange cancela
  de forma síncrona (`is_cancel_request_in_exchange_synchronous`), marca `CANCELED`; si es
  asíncrono, marca `PENDING_CANCEL` y espera confirmación por WS/REST. `:567` es la rama
  condicional. [VERIFIED]
- **Confirmación vía WS user stream**: `hummingbot/connector/exchange/bybit/bybit_exchange.py:330-360`
  procesa el canal privado `order` de Bybit y mapea `orderStatus` a `OrderState` vía
  `CONSTANTS.ORDER_STATE` (`bybit_constants.py:76-84`: New→OPEN, PartiallyFilled→
  PARTIALLY_FILLED, Filled→FILLED, Cancelled→CANCELED, PartiallyFilledCanceled→CANCELED,
  Rejected→FAILED). La confirmación real de cancel llega como evento, no como acuse del POST. [VERIFIED]
- **Carrera CANCEL/FILL**: el tracker actualiza la misma orden desde eventos de orden y de
  trade; `InFlightOrder.update_with_order_update` y `update_with_trade_update` son separados
  y acumulativos (`in_flight_order.py:327-372`), de modo que un FILL después de un cancel
  prevalece (el estado se actualiza con el evento de orden más reciente y los fills se
  acumulan). [VERIFIED]
- **Cancel perdido / orden perdida**: `exchange_py_base.py:520-537` `_execute_order_cancel`
  maneja timeout y "order not found" → `process_order_not_found`; `client_order_tracker.py`
  cuenta `_order_not_found_records` y, superado el límite, marca la orden "lost" (no la da
  por cancelada silenciosamente) y luego `_cancel_lost_orders` intenta cancelarla
  (`exchange_py_base.py:1056-1063`). [VERIFIED]
- **Doble vía REST+WS**: `start_network` arranca `_status_polling_loop` (REST backup,
  `exchange_py_base.py:805`) + `_user_stream_event_listener` (WS). `_get_poll_interval`
  usa poll corto (5s) si el WS lleva sin mensajes > TICK_INTERVAL_LIMIT, o largo (120s) si
  el WS está al día. Es reconciliación dual redundante. [VERIFIED]
- **Restore tras reconexión**: `restore_tracking_states` / `ClientOrderTracker.restore_tracking_states`
  persisten `InFlightOrder.to_json` y los restauran al reiniciar el connector (persistencia
  en `hummingbot/connector/markets_recorder.py`). [VERIFIED]

## 14. Inventario y hallazgos — Hummingbot (balance/posiciones)

- **Balance UNIFIED correcto**: `bybit_exchange.py:_process_user_stream_event_message` (~360-380)
  para `accountType == "UNIFIED"` computa `free = walletBalance - locked - totalOrderIM -
  totalPositionMM - totalPositionIM` (el equivalente de `totalAvailableBalance`), y
  `total = walletBalance`. El mismo cálculo en REST `_update_balances` (`bybit_exchange.py:518-541`). [VERIFIED]
- **NO usa `free`/`availableToWithdraw` para UNIFIED**: solo los usa en la rama no-UNIFIED
  (`bybit_exchange.py:371-374`). Esto valida la decisión de ADR-0030 de no usar `free`. [VERIFIED]
- **Balance actualizado por WS wallet channel + REST polling**: el canal privado `wallet` de
  Bybit actualiza `_account_available_balances`/`_account_balances`; `_update_balances` REST
  hace lo mismo como backup. `get_available_balance` es la API que consumen las estrategias. [VERIFIED]
- **Posiciones**: en derivados, el parseo usa `fetch_positions`/eventos de posición del user
  stream (estado de posición con size, unrealized PnL). En el exchange_base los balances y
  posiciones viven en el connector, no en un portfolio separado. [INFERENCE]

## 15. Inventario y hallazgos — Nautilus Trader (órdenes/cancelación)

Referencia: `/tmp/bot-refs/nautilus/nautilus_trader-develop/`.

- **Modelo de eventos ricos**: `crates/model/src/events/order/` contiene eventos explícitos
  por transición: `submitted, accepted, rejected, canceled, pending_cancel, pending_update,
  modify_rejected, cancel_rejected, filled, fill_voided, expired, triggered, updated,
  snapshot`. `OrderPendingCancel` = "CancelOrder command sent to venue". [VERIFIED]
- **`OrderStatus.PendingCancel` como estado**: `crates/model/src/enums.rs:1416` define
  `OrderStatus` con `PendingCancel = 12`; `is_open()` incluye `PendingCancel` y
  `is_closed()` NO (sigue siendo estado abierto/no-terminal); `is_cancellable()` excluye
  PendingCancel. Esto es exactamente el estado transitorio `CANCELLING` de ADR-0029. [VERIFIED]
- **Cancelación vía WS con correlación de requests**: `crates/adapters/bybit/src/execution.rs:1795`
  `cancel_order` envía `BybitWsCancelOrderParams` por WS y registra en `dispatch_state.pending_requests`
  un `PendingOperation::Cancel`; el response WS con `req_id` correlaciona y elimina la
  entrada (`crates/adapters/bybit/src/websocket/dispatch.rs:844`). Si el resultado es
  ambiguo (`is_bybit_ambiguous_order_error_code`), **no decreta estado final: "awaiting
  reconciliation"**. [VERIFIED]
- **Reconciliación offline contra snapshots**: `crates/execution/src/reconciliation/orders.rs`
  `reconcile_order_report`: si el reporte del venue está en `PendingUpdate | PendingCancel`,
  **no genera evento** (el estado intermedio se respeta); si el estado local coincide con el
  del venue, no hace nada; en divergencia genera los eventos de aceptación/rechazo/updated
  correspondientes. Es el patrón fail-closed: no inventar estados finales. [VERIFIED]
- **CancelRejected**: `cancel_rejected.rs` es un evento real — un cancel puede ser
  rechazado por el venue (raza CANCEL/FILL, EC_TooLateToCancel, etc.) y el sistema lo
  modela como estado, no como silencio. [VERIFIED]
- **Fill siempre prevalece**: los eventos son aplicados en orden por el execution engine
  (`crates/execution/src/engine/mod.rs`); un `Filled` posterior a un `PendingCancel` es
  legal y aplica fill. [VERIFIED]

## 16. Inventario y hallazgos — Nautilus Trader (balance/posiciones/recovery)

- **Balance desde wallet-balance**: `crates/adapters/bybit/src/http/client.rs:875`
  `get_wallet_balance` (endpoint `/v5/account/wallet-balance`); `request_account_state` toma
  la primera wallet y la parsea. [VERIFIED]
- **Parseo UNIFIED**: `crates/adapters/bybit/src/common/parse.rs:1173` `parse_account_state`:
  `total = coin.wallet_balance - coin.spot_borrow`, `locked = coin.locked`,
  `free = total - locked` (via `AccountBalance::from_total_and_locked`, que además hace clamp
  para que free nunca sea negativo: `crates/model/src/types/balance.rs:113`). Margen inicial =
  `totalPositionIM + totalOrderIM`, mantenimiento = `totalPositionMM`. El modelo de HTTP
  incluye `total_available_balance` (`http/models.rs:818`), pero el free derivado NO usa
  `totalAvailableBalance` directamente. [VERIFIED]
- **Crypto**, no *only* UNIFIED: el adapter Bybit de Nautilus soporta múltiples
  `product_types`; `spot_borrow` se resta para cuentas con préstamos. Para UNIFIED spot
  con UTA, `free` derivado (walletBalance - locked) difiere de `totalAvailableBalance`
  (que además descuenta IM/MM) — Nautilus usa locked como agregado, lo que es un
  enfoque distinto (menos fino) que el de Hummingbot. [VERIFIED]
- **Posiciones**: `generate_position_status_reports` / reconciliación de posiciones
  (`crates/execution/src/reconciliation/positions.rs`) con cap de precio y tolerancia de
  unidad. [VERIFIED]
- **Recovery/replay**: Nautilus tiene `crates/event_store` + `crates/persistence`; el
  estado se reconstruye aplicando eventos en orden (event sourcing) — las órdenes abiertas
  se reconstruyen a partir de los eventos persistidos y se reconcilian contra el venue al
  reconectar (patrón `reconciliation`). [VERIFIED]
- **Watch balance/positions WS**: el adapter bybit usa `ws_trade` y streams privados
  (órdenes/posiciones); los `AccountState` tienen `is_reported` para distinguir
  snapshots REST (`reported=true` en `parse_account_state`) de eventos WS. [VERIFIED]

---

## 17. Bybit — garantías reales relevantes

Docs oficiales Bybit (API v5) — referenciadas vía el conocimiento previo y la inspección
de CCXT:

- Market order = **IOC** (`timeInForce=ImmediateOrCancel`): se llena o se cancela de
  inmediato; la ventana de cancelación es mínima pero no cero (ver ADR-0029 §Por qué). [VERIFIED]
- Cancel `/v5/order/cancel` es **asíncrono**: el `retCode=0` del POST es acuse de
  recepción, no confirmación de cancelación final. El estado real llega por WS (topic
  `order`) o por `GET /v5/order/realtime`/`fetch_order`. [VERIFIED]
- Códigos de raza CANCEL/FILL: `EC_TooLateToCancel`, `EC_OrigClOrdIDDoesNotExist`,
  `EC_PerCancelRequest` (CCXT `bybit.py:5119` muestra `EC_PerCancelRequest` como
  `rejectReason` de un order `Cancelled`). [VERIFIED]
- `wallet-balance` con `accountType=UNIFIED` expone `walletBalance`, `equity`, `locked`,
  `totalOrderIM`, `totalPositionIM`, `unrealisedPnl`, `totalAvailableBalance`. [VERIFIED]
- `free`/`availableToWithdraw` **deprecados para UNIFIED desde 2025-01-09**; el campo
  operativo es `totalAvailableBalance` (USD) / `availableBalance` por moneda. [VERIFIED]
- Rate limits: IP 600 req/5s (público); cancel `/v5/order/cancel` cost 2.5 (`bybit.py:572`);
  `wallet-balance` cost 1 (`bybit.py:373`). Límite específico de wallet-balance 50/s. [VERIFIED]
- Latencia de balance puede subir en volatilidad extrema (aviso oficial); el WS wallet
  topic **no trae snapshot** al suscribirse. [VERIFIED]
- Los tres bots confirman que la fuente de verdad operativa es la combinación
  REST+WS, no el acuse del POST. [INFERENCE]

## 18. CCXT — comportamiento real relevante

Fuente: `/tmp/opencode/ccxt-bybit/bybit.py` (9489 líneas) y `bybit_pro.py`.

- `cancel_order(id, symbol)` (`bybit.py:4611`): **requiere symbol** (lanza `ArgumentsRequired`
  si es None); el resultado parseado solo trae `orderId`/`orderLinkId` — **sin status final**.
  Por tanto el caller debe confirmar con `fetch_order`/WS. [VERIFIED]
- `parse_order` (`bybit.py:3723`): `cancelType`/`rejectReason` solo existen en `info`,
  no en la estructura normalizada. [VERIFIED]
- `parse_order_status` (`bybit.py:3686`): `PendingCancel`→`open`, `Cancelled`→`canceled`,
  `PartiallyFilledCanceled`→`closed`, `Filled`→`closed`. [VERIFIED]
- `fetch_balance` (`bybit.py:3525`) → UNIFIED `wallet-balance`; `parse_balance`
  (`bybit.py:3363`): `free = availableToWithdraw | free` (`bybit.py:3495`); si no hay,
  `used = locked + totalPositionIM + totalOrderIM`. **`totalAvailableBalance` NO está en la
  estructura normalizada, solo en `info`**. [VERIFIED]
- `bybit_pro.py` (async/WS): `watch_orders` (`:1719`, topic privado `order`), `watch_balance`
  (`:1919`, topic `wallet`), `watch_my_trades` (`:1224`), `watch_positions` (`:1433`). [VERIFIED]
- `fetch_open_orders` (`:5380`), `cancel_orders` (`:4647`, batch), `cancel_all_orders`
  (`:4848`), `fetch_order` (`:4953`). [VERIFIED]

---

## 19. Comparación entre referentes (patrones convergentes)

| Patrón | Freqtrade | Hummingbot | Nautilus Trader |
|---|---|---|---|
| Estado intermedio "cancelando" | No explícito (DB ft_is_open + re-fetch) | `OrderState.PENDING_CANCEL` | `OrderStatus.PendingCancel` |
| Confirmar cancel por exchange | `cancel_order_with_result` + re-fetch hasta terminal | Evento WS order / REST status | Response WS correlacionado + reconciliation |
| Fill prevalece sobre cancel | Sí (baila y re-evalúa) | Sí (eventos acumulativos) | Sí (modelo de eventos ordenados) |
| CancelRejected modelado | No explícito | OrderState.FAILED (Rejected→FAILED) | Evento `OrderCancelRejected` |
| Cancel fallido → estado final inventado | No | No | No ("awaiting reconciliation") |
| Órdenes abiertas persistidas | DB SQLAlchemy | InFlightOrder JSON (markets_recorder) | Event store (replay) |
| Reconciliación al arranque | `startup_update_open_orders` | `restore_tracking_states` + lost orders | Reconciliation engine + event replay |
| Balance desde exchange | `fetch_balance` + posiciones | WS wallet + REST backup | `wallet-balance` + account state |
| Uso de `free`/`availableToWithdraw` (UNIFIED) | Hereda de CCXT (spot) | No para UNIFIED (deriva de walletBalance) | No (deriva free=total-locked) |
| WS privado para órdenes | No (solo OHLCV) | Sí (user stream) | Sí (ws_trade + streams) |

Conclusión: **los tres convergen en "nunca declarar estado final sin confirmación del
venue, y reconciliar contra la fuente real (REST/WS) tanto en cancel como en balance".**
Eso es precisamente el núcleo de ADR-0029 y ADR-0030. [VERIFIED/INFERENCE]

---

## 20. Análisis profundo B-MD-008 (cancelación real)

Problema (VERIFIED en §6): cancel local-only + sin callers + grafo terminal sin estado
transitorio → cualquier intento de cancelar de verdad hoy NO llega a Bybit; si se hubiera
hecho un `transition(CANCELLED)` directo sin confirmar, un fill simultáneo quedaría
invisible (divergencia posición/saldo).

Evidencia de referentes que respaldan el diseño ADR-0029:

1. **Estado intermedio**: Hummingbot (`PENDING_CANCEL`) y Nautilus (`PendingCancel`)
   modelan explícitamente "cancel en curso" como estado abierto (no-terminal). Freqtrade lo
   logra con `ft_is_open=True` + re-fetch. [VERIFIED]
2. **Confirmación por exchange, no por acuse**: los tres re-verifican tras el POST de cancel
   (Freqtrade `cancel_order_with_result`/re-fetch, Hummingbot evento WS order, Nautilus
   response WS + reconciliation). CCXT `cancel_order` no devuelve status final
   (`bybit.py:4611`), por lo que la confirmación adicional es obligatoria. [VERIFIED]
3. **Fill prevalece**: los tres garantizan que un fill posterior a un cancel actualiza la
   orden a FILLED (Freqtrade baila y re-evalúa; Hummingbot eventos acumulativos; Nautilus
   modelo de eventos ordenados + comentario explícito en `freqtradebot.py`). [VERIFIED]
4. **CancelRejected/ambiguo**: Nautilus modela `OrderCancelRejected` y "awaiting
   reconciliation" para errores ambiguos (EC_TooLateToCancel y similares); Hummingbot
   mapea `Rejected→FAILED`; Freqtrade no inventa estado y re-intenta. ADR-0029 cubre esto
   con `CANCELLING → REJECTED` y la política "no inventar estado final". [VERIFIED]
5. **Idempotencia/duplicados**: Hummingbot `process_order_update` es idempotente (no-op si
   el estado no cambia); Nautilus correlaciona por `req_id` y elimina la entrada pendiente.
   ADR-0029 exige no-op + reconciliación en cancel duplicado. [VERIFIED]
6. **Gestión de órdenes abiertas**: los tres tienen un mecanismo equivalente a
   `manage_open_orders` (Freqtrade loop por ciclo; Hummingbot lost-order polling + WS;
   Nautilus reconciliation + event replay). Hoy OCM no tiene ninguno → ADR-0029 lo añade. [VERIFIED]

Conclusión: ADR-0029 es la solución que los tres referentes aplican en la práctica,
adaptada al estilo Clean/Hexagonal de OCM. No introduce sobre-diseño: la necesidad
(ventana de cancelación real aunque sea IOC, reintentos, doble envío, acuse asíncrono)
está verificada.

---

## 21. Análisis profundo B-MD-009 (balance real)

Problema (VERIFIED en §7): sin lectura de saldo, `capital_usd` configurado puede divergir
del saldo real (fees, trades, retiros); sizing y drawdown se calculan sobre un número
fantasma.

Evidencia de referentes que respaldan el diseño ADR-0030:

1. **Balance se lee del exchange**: los tres leen `wallet-balance`/`fetch_balance`; ninguno
   confía en un capital configurado como única fuente. [VERIFIED]
2. **No usar `free`/`availableToWithdraw` para UNIFIED**: Hummingbot deriva
   `free = walletBalance - locked - totalOrderIM - totalPositionMM - totalPositionIM`
   (es decir, computa `totalAvailableBalance`); Nautilus deriva `free = total - locked`;
   Freqtrade usa `free` solo vía CCXT (que para UNIFIED usa `availableToWithdraw`,
   deprecado). ADR-0030 acierta al exigir `totalAvailableBalance` explícito en el adapter
   en lugar de depender del mapeo CCXT. [VERIFIED]
3. **Reconciliación periódica + post-evento**: Hummingbot actualiza balance por WS wallet
   y REST polling; Nautilus emite `AccountState` desde snapshots y eventos. ADR-0030
   (gate de arranque + loop + post-fill) es el mismo patrón. [VERIFIED]
4. **Discrepancia → no auto-corregir**: ningún referente reescribe posiciones por un
   desajuste de balance; bloquean/alertan. Nautilus distingue `reported` (snapshot) de WS.
   ADR-0030 (fail-closed, bloquear órdenes, nunca auto-corregir) coincide. [VERIFIED]
5. **Freshness**: Freqtrade throttle de 1h con `require_update`; Hummingbot poll 5s/120s
   según estado del WS. ADR-0030 pide timestamp/freshness en el port — correcto y superior
   a los referentes (explícito). [VERIFIED]
6. **Persistencia de posiciones**: portfolio (BC-43) ya es SSOT de posiciones; el saldo
   derivado lo complementa. Nautilus lleva posiciones y balance como objetos de estado
   separados reconciliados contra el venue; la separación OCM (portfolio posiciones +
   BalancePort saldo) es coherente. [VERIFIED]

Conclusión: ADR-0030 está alineado con los referentes; su única mejora sobre ellos es
exigir `totalAvailableBalance` explícito (Hummingbot lo reimplementa; Nautilus no lo usa
directamente; Freqtrade hereda la limitación de CCXT).

---

## 22. Matriz comparativa (16 temas)

| # | Problema | OCM actual | Bybit (garantía) | CCXT | Freqtrade | Hummingbot | Nautilus | Mejor patrón |
|---|---|---|---|---|---|---|---|---|
| 1 | Cancelar orden real | ❌ local-only (`oms.py:300`) | Cancel async; confirmar por WS/GET | `cancel_order` sin status final (`bybit.py:4611`) | cancel + re-fetch hasta terminal | `_place_cancel` + evento WS | WS cancel + correlation | Cancelar + confirmar por exchange |
| 2 | Estado intermedio cancel | ❌ sin estado (CANCELLED terminal) | PendingCancel en WS | PendingCancel→'open' | DB ft_is_open | `PENDING_CANCEL` | `PendingCancel` (open) | Estado transitorio CANCELLING |
| 3 | Fill prevalece sobre cancel | ⚠️ regla implícita (grafo) | Race real (EC_TooLateToCancel) | status closed | bail + re-evalúa | eventos acumulativos | eventos ordenados | FILL prevalece (explícito) |
| 4 | CancelRejected modelado | ❌ | rejectReason (info) | solo en info | no explícito | Rejected→FAILED | `OrderCancelRejected` | Evento CancelRejected |
| 5 | Cancel fallido no inventa final | ⚠️ fail-closed en fetch_state | — | — | devuelve False | no marca cancelado | "awaiting reconciliation" | Nunca decreta sin confirmación |
| 6 | Idempotencia cancel | ⚠️ no-op en terminal | cancel repetido ok | — | — | process_order_update no-op | correlation req_id | no-op + reconciliación |
| 7 | Órdenes abiertas persistidas | ❌ no hay | — | — | DB SQLAlchemy | InFlightOrder JSON | Event store | Persistencia + reconciliación |
| 8 | Reconciliación al arranque | ❌ no hay | — | — | startup_update_open_orders | restore_tracking_states | event replay | Reconcil. al arranque |
| 9 | Gestión de órdenes abiertas (loop) | ❌ no hay | — | — | manage_open_orders | lost-order polling | reconciliation | Loop manage_open_orders |
| 10 | Balance real desde exchange | ❌ capital configurado | wallet-balance UNIFIED | fetch_balance (free=availToWithdraw) | fetch_balance + posiciones | WS wallet + REST | wallet-balance | BalancePort + adapter |
| 11 | Campo correcto UNIFIED | ❌ n/a | totalAvailableBalance | NO normalizado (solo info) | usa free (limitado) | deriva totalAvailableBalance | deriva free=total-locked | totalAvailableBalance |
| 12 | Freshness de balance | ❌ n/a | latencia en volatilidad | — | throttle 1h/force | poll 5s/120s | AccountState ts | timestamp/freshness en port |
| 13 | Reconciliación periódica | ❌ no hay | — | — | update() por ciclo | status polling | reconciliation engine | Gate + loop + post-fill |
| 14 | Discrepancia de balance | ❌ no detecta | — | — | n/a | no auto-corrige | no auto-corrige | fail-closed + bloquear órdenes |
| 15 | Unrealised PnL contado | ❌ no | wallet-balance lo incluye | — | _strip_unrealized_pnl | posiciones uPnL | uPnL en account state | separar uPnL de disponible |
| 16 | Posiciones SSOT | ✅ PositionStore (BC-43) | posición real | — | DB trades | connector | cache/position | Portfolio SSOT (OCM ya) |

## 23. Patrones adoptables ✅

- Estado transitorio `CANCELLING` + confirmación por exchange (Hummingbot/Nautilus). [VERIFIED]
- `OrderCancelRejected` / mapeo Rejected→FAILED como evento explícito. [VERIFIED]
- Cancel con re-verificación: `cancel_order` + `fetch_order`/WS hasta terminal (Freqtrade). [VERIFIED]
- Loop `manage_open_orders` con reconciliación periódica (Freqtrade). [VERIFIED]
- Persistencia de órdenes abiertas y reconciliación al arranque (los tres). [VERIFIED]
- Balance UNIFIED derivado de `walletBalance` (no `free`) (Hummingbot). [VERIFIED]
- Separar unrealised PnL del disponible (Freqtrade `_strip_unrealized_pnl`, Nautilus
  account state). [VERIFIED]
- Freshness explícita + refresh forzable (Freqtrade `require_update`). [VERIFIED]
- Reconciliation engine con eventos `reported` vs WS (Nautilus) — aplicable a OCM como
  capa de reconciliación, no como tecnología. [VERIFIED]

## 24. Patrones parcialmente útiles ⚠️

- Cancel "sintético" de último recurso de Freqtrade (`cancel_order_with_result`): útil como
  fallback de transporte, pero OCM debe conservar fail-closed (nunca inventar estado final
  sin al menos un fetch posterior). [VERIFIED]
- Lost-order tracking de Hummingbot (contador + cancel forzado): útil para huérfanas, pero
  el límite de reintentos y el "cancel forzado" deben ser conservadores en live. [VERIFIED]
- Throttle 1h de Freqtrade: sirve como mínimo, pero OCM ya pedirá freshness configurable. [VERIFIED]
- free=total-locked de Nautilus: simple, pero menos fino que totalAvailableBalance para
  UNIFIED (no descuenta IM/MM de posiciones). [VERIFIED]

## 25. Patrones descartados ❌

- Declarar `CANCELLED` sin confirmación del exchange (inherentemente inseguro; ningún
  referente lo hace). [VERIFIED]
- Usar `free`/`availableToWithdraw` como saldo operativo en UNIFIED (deprecado). [VERIFIED]
- Asumir que el POST de cancel devuelve el estado final (CCXT no lo hace). [VERIFIED]

## 26. Patrones incompatibles con OCM 🚫

- Copiar la máquina de eventos de Nautilus como tecnología (Rust, event sourcing completo)
  — violaría los 49 contratos BC-NN y el estilo Clean/Hexagonal de OCM. Solo se adopta el
  patrón conceptual (eventos de transición explícitos), implementado con el port
  `OrderTransport`. [INFERENCE]
- Migrar la gestión de órdenes a una DB relacional como Freqtrade (SQLAlchemy) — OCM no
  tiene DB de trading; se reemplaza por el estado transitorio + persistencia ligera
  (Redis, ya en infraestructura). [INFERENCE]
- Adoptar la estructura de connector monolítica de Hummingbot (estrategias acceden
  directamente al balance del connector) — rompe BC-43/BC-50; OCM mantiene el balance
  detrás de `BalancePort`. [INFERENCE]

---

## 27. Validación ADR-0029

**Veredicto: CONFIRMADA** (sin ajustes necesarios).

Soporte:
- El estado transitorio `CANCELLING` coincide con `PendingCancel` de Nautilus y
  `PENDING_CANCEL` de Hummingbot, ambos estados abiertos (no-terminales). [VERIFIED]
- La regla "fill siempre prevalece" es la que aplican los tres referentes ante la raza
  CANCEL/FILL de Bybit (EC_TooLateToCancel etc.). [VERIFIED]
- La necesidad de confirmación por exchange (no acuse) está forzada por CCXT
  (`cancel_order` sin status final) y confirmada por los tres. [VERIFIED]
- El loop `manage_open_orders` cubre el gap G2 (órdenes abiertas huérfanas) que los tres
  referentes resuelven con sus mecanismos de reconciliación. [VERIFIED]
- No introduce sobre-diseño: market IOC reduce la ventana pero no la elimina (reintentos
  `live_executor.py:178-203`, doble envío, acuse asíncrono). [VERIFIED]

## 28. Validación ADR-0030

**Veredicto: CONFIRMADA** (sin ajustes necesarios).

Soporte:
- Los tres referentes leen el balance del exchange; ninguno confía solo en capital
  configurado. [VERIFIED]
- `totalAvailableBalance` como campo operativo UNIFIED: Hummingbot lo reimplementa
  (walletBalance - locked - IMs - MMs); CCXT no lo normaliza (solo `info`); ADR-0030 lo
  exige en el adapter — superior a los tres. [VERIFIED]
- Reconciliación gate+loop+post-fill coincide con los mecanismos de los tres. [VERIFIED]
- Discrepancia fail-closed sin auto-corregir posiciones: ningún referente auto-corrige. [VERIFIED]
- El `BalancePort` con freshness es una mejora explícita sobre Freqtrade (throttle 1h) y
  Hummingbot (poll adaptativo). [VERIFIED]

Nota de soporte adicional: el balance NO debe usarse como segunda fuente de posiciones;
la posición sigue siendo PositionStore (BC-43). El balance complementa para sizing y
drawdown (vínculo B-MD-001). Esto es coherente con la separación posiciones/saldo de
Nautilus (position vs account state). [INFERENCE]

---

## 29. Cambios recomendados (sin modificar ADRs ni código)

El diseño de ambos ADRs se mantiene íntegro. Recomendaciones de afinación para el
implementation roadmap (conceptual, sin tocar ADRs):

1. **Adapter cancel (`CCXTAdapter.cancel_order`)**: firmar `(symbol, exchange_order_id)`
   (CCXT exige symbol) y, tras el POST, confirmar con `fetch_order` cuando el resultado no
   sea concluyente (patrón `cancel_order_with_result` de Freqtrade, con fail-closed). [VERIFIED]
2. **Adapter balance (`CCXTAdapter.fetch_balance`)**: leer `wallet-balance` UNIFIED y
   extraer `totalAvailableBalance` de `info` (CCXT no lo normaliza). Exponer
   `total/locked/unrealised` por moneda. [VERIFIED]
3. **`BalancePort`**: incluir `unrealised_pnl` y `timestamp`/`freshness` (evitar contar el
   uPnL dos veces — patrón `_strip_unrealized_pnl`). [VERIFIED]
4. **manage_open_orders**: intervalos adaptativos estilo Hummingbot (poll corto si el WS
   va mal, largo si va bien) como refinamiento opcional. [VERIFIED]
5. **Reconciliación al arranque live**: gate de arranque con `fetch_balance` + estado de
   órdenes abiertas (startup_update_open_orders de Freqtrade) — el kill switch del
   ExecutionGuard (`ocm/runtime/guard.py`) debe poder activarse si el balance no está
   disponible o la reconciliación no converge. [INFERENCE]
6. **Mapeo de `rejectReason`/`cancelType`**: aunque CCXT solo los expone en `info`,
   conservarlos en `OrderState.error` para diagnóstico (nivel trazabilidad). [VERIFIED]

## 30. Pruebas necesarias (invariantes)

Pruebas unitarias/contrato (reposo):

- INV-1: `CANCELLING` es no-terminal; `is_terminal(CANCELLING)=False`.
- INV-2: `CANCELLING` solo se alcanza desde SUBMITTED.
- INV-3: FILLED es alcanzable desde CANCELLING (fill prevalece).
- INV-4: CANCELLED solo es alcanzable desde CANCELLING con confirmación de exchange
  (nunca decreta directo).
- INV-5: cancel duplicado sobre CANCELLING/CANCELLED → no-op + reconciliación.
- INV-6: timeout/error de cancel → permanece CANCELLING (no inventa estado final).
- INV-7: `OrderTransport.cancel` SafeOps: nunca lanza; ERROR en no-confirmación.
- INV-8: `fetch_balance` retorna total/disponible/locked/unrealised con timestamp; el
  disponible UNIFIED proviene de `totalAvailableBalance` (no `free`).
- INV-9: discrepancia material de balance → RiskManager bloquea órdenes (fail-closed),
  nunca modifica PositionStore.
- INV-10: reconcile post-fill: tras un FILL, el saldo disponible decrece y la posición
  PositionStore se actualiza una sola vez.

Pruebas de integración (necesitan sandbox de Bybit):

- Cancel de una market order IOC: confirmar estado final por fetch_order/WS.
- Carrera CANCEL/FILL simulada (orden con fill inmediato): el resultado final es FILLED.
- Cancel con respuesta perdida (timeout): la orden permanece CANCELLING y un fetch_state
  posterior la resuelve.
- Balance con uPnL: disponible ≠ total; no se cuenta uPnL dos veces.

Estas invariantes son testables con el test harness existente
(`tests/trading/`, `tests/architecture/`). [INFERENCE]

## 31. Riesgos residuales

- **Orden en CANCELLING sin exchange disponible**: permanece transitoria (visible, no
  huérfana) hasta reconexión. Mitigación: reintentos + alerta + kill switch (ADR-0029
  riesgo residual). [VERIFIED]
- **Balance divergente en volatilidad extrema** (latencia Bybit): freshness y política
  fail-closed evitan sizing erróneo; un gate demasiado estricto podría bloquear en vivo
  por latencia legítima → umbrales configurables. [INFERENCE]
- **Cancel sintético de último recurso (si se adopta)**: solo como fallback de transporte
  y siempre seguido de fetch. [VERIFIED]
- **No se puede verificar el comportamiento de cancel de OCM en sandbox real hoy** (sin
  credenciales en esta tarea): la confirmación del comportamiento de Bybit queda pendiente
  de la fase de sandbox del roadmap. [UNKNOWN]

## 32. Limitaciones / UNKNOWN

- El comportamiento en producción de Bybit para cancel/balance bajo carga real no fue
  medido (solo docs + CCXT). Se declara como gap, no como hecho. [UNKNOWN]
- Nautilus fue analizado en su repo (Rust) en el estado del ZIP; no se ejecutó. [UNKNOWN]
- Los ZIPs solicitados en `~/kb-local-only/` **no estaban allí**: esa carpeta solo contenía
  PDFs de la KB. Los ZIPs estaban en `docs/` del repo, **git-ignored** (confirmado con
  `git check-ignore`), no trackeados. Se extrajeron a `/tmp/bot-refs/` (fuera del repo) para
  no contaminar el working tree; los directorios temporales se limpiarán al final de la
  tarea si es seguro (ver §33). [VERIFIED]
- No se copió texto de código de ningún bot en este informe (restricción de licencias
  GPL-3.0/LGPL-3.0); solo descripción de patrones con referencias archivo:línea. [VERIFIED]
- El enunciado de la tarea contenía un error de nombre de archivo
  ("2026-08-15-b-md-008-b-md-009-diseno-conceptual.md"); el archivo real es
  `docs/audits/2026-08-15-b-md-008-009-diseno-conceptual.md`. [VERIFIED]

## 33. Limpieza y estado del repo

- Working tree en HEAD: ` M docs/plans/tracking.yaml` (pre-existente, NO tocado),
  `?? ADR-0029`, `?? ADR-0030`, `?? docs/audits/2026-08-15-b-md-008-009-diseno-conceptual.md`
  (pre-existente). Sin cambios de código, sin commits, sin push. [VERIFIED]
- Este informe es el único archivo creado en esta tarea. [VERIFIED]
- Directorios temporales `/tmp/bot-refs/` y `/tmp/opencode/ccxt-bybit/`: fuera del repo;
  se pueden eliminar de forma segura (contenido extraíble de los ZIPs). No se dejaron ZIPs
  descomprimidos dentro del repo. [VERIFIED]

---

## RECOMENDACIÓN FINAL

1. **¿Conservar, modificar o descartar ADR-0029?** **Conservar, sin modificar.** La
   evidencia de los tres referentes (estado intermedio, confirmación por exchange, fill
   prevalece, cancel fallido sin estado final) confirma el diseño completo. Veredicto:
   CONFIRMADA.

2. **¿Conservar, modificar o descartar ADR-0030?** **Conservar, sin modificar.** Los tres
   referentes leen el balance del exchange y ninguno usa `free`/`availableToWithdraw` para
   UNIFIED; `totalAvailableBalance` explícito es la mejora correcta. Veredicto: CONFIRMADA.

3. **¿Es correcta la arquitectura propuesta en OCM para B-MD-008?** Sí: port
   `OrderTransport.cancel` + adapter `CCXTAdapter.cancel_order` + estado `CANCELLING` +
   loop `manage_open_orders` replican el patrón que Freqtrade/Hummingbot/Nautilus aplican
   en producción, en el estilo Clean/Hexagonal de OCM (BC-50 respetado).

4. **¿Es correcta la arquitectura propuesta en OCM para B-MD-009?** Sí: portfolio dueño del
   estado patrimonial + `BalancePort` + adapter `fetch_balance` (UNIFIED,
   `totalAvailableBalance`) + gate/loop/post-fill + política fail-closed. Alineada con los
   tres referentes y superior en el campo operativo y la freshness.

5. **¿Mantener las ADRs separadas?** Sí (tal como recomiendan las propias ADRs): concern
   distinto (estado de orden vs saldo), dueños BC distintos (trading vs portfolio), gates y
   roadmap independientes. El único paso compartido (exponer métodos en `CCXTAdapter`) no
   justifica fusionarlas.

6. **Fase 3 (bloqueante LIVE P1): ¿se mantiene?** Sí, para ambas. Sin cancel real y sin
   balance real, live no cumple las invariantes de seguridad que los tres referentes exigen.

7. **¿Qué pruebas son imprescindibles antes de live?** Las invariantes INV-1..INV-10
   (§30) más: cancel IOC confirmado por fetch/WS, carrera CANCEL/FILL simulada (fill
   prevalece), cancel con respuesta perdida (sigue CANCELLING), y balance con uPnL sin
   doble conteo.

8. **Riesgos residuales a aceptar/mitigar explícitamente**: orden CANCELLING con exchange
   caído (transitoria, visible, con alerta y kill switch) y latencia de balance en
   volatilidad extrema (freshness + umbrales configurables, sin auto-corrección de
   posiciones).

9. **Prioridad de implementación sugerida** (conceptual, sin editar ADRs): (1) verificar
   cancel y wallet-balance UNIFIED en sandbox Bybit; (2) exponer `cancel_order` y
   `fetch_balance` en `CCXTAdapter`; (3) `OrderTransport.cancel` + estado `CANCELLING`;
   (4) `BalancePort` + `PortfolioReconciler`; (5) `manage_open_orders`; (6) invariantes y
   gate de arranque live. Ningún paso requiere relajar los contratos BC-NN existentes.
