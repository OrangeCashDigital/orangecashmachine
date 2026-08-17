# Auditoría — Benchmark complementario: LEAN + vn.py + CCXT Pro vs ADR-0029 / ADR-0030

**Fecha:** 2026-08-15
**Auditor:** OpenAI Codex (asistido por agentes de exploración)
**Rol:** Arquitecto Principal de Software · Trading Systems Reviewer
**Objetivo:** verificar de forma independiente las conclusiones del benchmark principal (`2026-08-15-bot-benchmark-b-md-008-009.md`, Freqtrade + Hummingbot + Nautilus) contra **tres referentes adicionales**: QuantConnect LEAN, vn.py y CCXT Pro, y comprobar el estado real del código OCM en HEAD para los temas B-MD-008 (cancelación real) y B-MD-009 (saldo real).
**Alcance:** `packages/trading` (execution, transport, order, risk, portfolio) + `packages/market_data/adapters/outbound/exchange/ccxt_adapter.py` + `packages/trading/bootstrap/composition_root.py` (transporte real) + `ocm/runtime/guard.py` + fuentes de terceros descargadas (QuantConnect LEAN, vn.py, CCXT).
**Método:** fuentes primarias de terceros (`Common/Orders/OrderTypes.cs`, `BrokerageTransactionHandler.cs`, `SecurityPortfolioManager.cs`, `vnpy/trader/constant.py`, `vnpy/trader/object.py`, `ccxt/pro/bybit.py`, `ccxt/bybit.py`) con `archivo:línea` + verificación directa del código OCM en HEAD. **Sin copiar arquitectura ni código**; solo se extraen patrones y se etiquetan con su procedencia.
**Regla de honestidad:** todo hallazgo se marca **VERIFIED** (confirmado en fuente directa), **UNVERIFIED** (pendiente de comprobar) o **INFERENCE** (deducción razonable, no demostrada). Se distingue **HECHO observado** / **INFERENCIA técnica** / **RECOMENDACIÓN de diseño**.
**Restricciones respetadas:** sin código de producción; `tracking.yaml` NO modificado; sin ports/adapters/composition roots nuevos; sin ADRs aprobadas cambiadas; sin commits/push. Único archivo creado: este informe.

---

## 1. Resumen ejecutivo

Esta ronda añade tres referentes al benchmark de bots (Freqtrade/Hummingbot/Nautilus): **LEAN** (QuantConnect), **vn.py** y **CCXT Pro**. El objetivo es comprobar si los patrones que OCM propone en ADR-0029 (cancelación real + gestión de órdenes abiertas) y ADR-0030 (saldo real + reconciliación patrimonial) convergen o divergen de lo que hacen motores de referencia reales, y si hay algún patrón que contradiga las decisiones ya tomadas.

**Conclusión general:** los tres referentes **confirman** el núcleo de las ADR. Ninguno introduce evidencia que obligue a cambiarlas:

1. **Estado intermedio de cancelación:** LEAN tiene `OrderStatus.CancelPending`; vn.py **no** tiene estado intermedio (solo `CANCELLED`/`REJECTED` terminales); Hummingbot tiene `PENDING_CANCEL`; Nautilus tiene `PendingCancel`. La propuesta OCM (`CANCELLING` transitorio) es convergente con LEAN/Hummingbot/Nautilus y **más defensiva que vn.py** (que no distingue un cancel confirmado de uno pendiente).
2. **El fill prevalece sobre el cancel:** confirmado por LEAN (callback de orden procesa el fill y elimina el estado de cancelación pendiente), por vn.py (el cancelado se reconcilia por query) y por CCXT Pro (la caché `watch_orders` se actualiza con el estado real; un `cancel_order` + `watch_orders` puede devolver `closed`). OCM ya lo establece como política en ADR-0029.
3. **Reconciliación al arranque:** LEAN reconcilia solo al inicio (`GetOpenOrders`/`GetAccountHoldings` en `BrokerageSetupHandler`) + `PerformCashSync` periódico; vn.py reconcilia por query tras reconexión; Nautilus usa un reconciliation engine. La propuesta OCM (verificación de órdenes abiertas al arranque) es convergente.
4. **Saldo real:** los tres leen el saldo del exchange por REST/WS con derivación explícita de `free/used/locked`; vn.py y CCXT derivan `available = balance − frozen`; CCXT Pro expone `watch_balance` (topic `wallet`) y `totalAvailableBalance` en `info`. La propuesta OCM (`fetch_balance` + reconciliación) es convergente y la derivación UNIFIED propuesta coincide con la de CCXT.

**Veredicto:** ADR-0029 **CONFIRMADA SIN CAMBIOS**; ADR-0030 **CONFIRMADA SIN CAMBIOS**. El benchmark complementario no encontró evidencia suficiente para modificar las ADR. Se documentan 3 observaciones de diseño (no bloqueantes) para fases posteriores.

---

## 2. Registro de hallazgos

### [F-LEAN-01] LEAN tiene un estado intermedio de cancelación: `OrderStatus.CancelPending`
- **Severidad:** Informativa (confirma ADR-0029)
- **Estado:** VERIFIED (HECHO observado)
- **Evidencia:** `Common/Orders/OrderTypes.cs` (líneas ~138-183) define `enum OrderStatus { New = 0, Submitted = 1, PartiallyFilled = 2, Filled = 3, Canceled = 5, None = 6, Invalid = 7, CancelPending = 8, UpdateSubmitted = 9, ... }`. Existe **`CancelPending` (8)**: estado transitorio entre la petición de cancelación y su confirmación.
- **Implicación:** el concepto de un estado transitorio "cancelando" no es exótico — lo usa un motor institucional (LEAN). OCM (`CANCELLING` transitorio en ADR-0029) converge.

### [F-LEAN-02] LEAN revierte `CancelPending` si llega un fill: el fill prevalece sobre el cancel
- **Severidad:** Informativa (confirma ADR-0029)
- **Estado:** VERIFIED (HECHO observado)
- **Evidencia:** en `Common/Orders/OrderTypes.cs`, `CancelPendingOrders.RemoveAndFallback(orderId)` elimina la orden del conjunto de cancelaciones pendientes y la revierte a su estado previo (submitted/partially-filled) si aún no estaba cancelada. El flujo de cancelación de LEAN, al recibir el evento de orden del brokerage, **procesa el fill real** (el `BrokerageTransactionHandler.HandleOrderEvent`/`UpdateOrderState`) y el estado transitorio de cancel se resuelve en favor del evento real.
- **Implicación:** LEAN no decreta `Canceled` localmente; deja que el brokerage resuelva la carrera y el fill **siempre gana**. Coincide con la regla "el fill prevalece sobre el cancel" de ADR-0029.

### [F-LEAN-03] LEAN reconcilia saldo y posiciones solo al arranque + `PerformCashSync` periódico
- **Severidad:** Informativa (confirma ADR-0030)
- **Estado:** VERIFIED (HECHO observado)
- **Evidencia:** `BrokerageSetupHandler` (arranque) llama `GetCashBalance`/`GetOpenOrders`/`GetAccountHoldings` para sincronizar el `SecurityPortfolioManager` con el estado real del brokerage. Además LEAN ejecuta un `PerformCashSync` periódico (sincronización de caja) para corregir desviaciones.
- **Implicación:** la reconciliación al arranque que OCM propone para ADR-0030 es el patrón estándar. No se requiere reconciliación continua.

### [F-LEAN-04] `MarginRemaining` de LEAN = TPV − caja no liquidada − margen usado
- **Severidad:** Informativa (matiz para B-MD-009)
- **Estado:** VERIFIED (HECHO observado)
- **Evidencia:** `Common/Securities/SecurityPortfolioManager.cs` (líneas ~580-593): `MarginRemaining = TotalPortfolioValue - UnsettledCashAmount - TotalMarginUsed`. El saldo "disponible" se deriva del patrimonio total neto de compromisos, no es un campo directo del brokerage.
- **Implicación:** refuerza que "saldo disponible" es una **derivación**, no un dato crudo. OCM en ADR-0030 debe derivar `free/used` con la misma disciplina (coincide con la derivación CCXT/vn.py de `free = available`).

### [F-VNPY-01] vn.py NO tiene estado intermedio de cancelación
- **Severidad:** Informativa (divergencia no bloqueante)
- **Estado:** VERIFIED (HECHO observado)
- **Evidencia:** `vnpy/trader/constant.py` (líneas ~30-39): `class Status(Enum) { SUBMITTING, NOTTRADED, PARTTRADED, ALLTRADED, CANCELLED, REJECTED }`. No existe `CANCELLING`/`PENDING_CANCEL`. En `vnpy/trader/object.py` (línea ~14) `ACTIVE_STATUSES = [SUBMITTING, NOTTRADED, PARTTRADED]`.
- **Implicación:** vn.py resuelve la carrera CANCEL vs FILL por **reconciliación posterior** (query de estado tras reconexión), no por estado intermedio. Es una alternativa válida pero menos defensiva que OCM: no distingue un cancel pendiente de uno confirmado. OCM con `CANCELLING` es **más seguro**.

### [F-VNPY-02] vn.py deriva `available = balance − frozen`
- **Severidad:** Informativa (confirma ADR-0030)
- **Estado:** VERIFIED (HECHO observado)
- **Evidencia:** `vnpy/trader/object.py` (líneas ~200-215): `AccountData` con `balance`, `frozen`, `available`; la lógica de derivación `available = balance − frozen` es el patrón estándar de vn.py. En el gateway de Bybit, `frozen = locked + totalOrderIM + totalPositionIM + bonus`.
- **Implicación:** coincide con la derivación UNIFIED que OCM propone en ADR-0030 y con la de CCXT (`used = locked + totalPositionIM + totalOrderIM`).

### [F-CCXTPRO-01] CCXT Pro `watch_orders` mantiene una caché local de órdenes
- **Severidad:** Informativa (matiz para ADR-0029)
- **Estado:** VERIFIED (HECHO observado)
- **Evidencia:** `ccxt/pro/bybit.py` (líneas ~1705-1903): `watch_orders` usa `ArrayCacheBySymbolById` — una **caché en memoria** de órdenes que se actualiza con eventos WS (topic privado `order`). No hay snapshot desde REST al suscribirse.
- **Implicación:** si OCM adoptara CCXT Pro, la "fuente de verdad" de `watch_orders` sería una **caché local**, no el estado directo del exchange. Cualquier hueco de eventos (reconexión, mensajes perdidos) dejaría la caché inconsistente sin una **reconciliación REST** (`fetch_open_orders`) explícita. ADR-0029 ya exige `fetch_order`/`fetch_open_orders` como confirmación — correcto.

### [F-CCXTPRO-02] CCXT Pro `watch_balance` sin snapshot ni merge REST
- **Severidad:** Informativa (matiz para ADR-0030)
- **Estado:** VERIFIED (HECHO observado)
- **Evidencia:** `ccxt/pro/bybit.py` (líneas ~1919+): `watch_balance` usa el topic privado `wallet`. El tópico `wallet` de Bybit **no emite snapshot al suscribirse**; `watch_balance` en CCXT Pro tampoco hace merge con REST `fetch_balance` por defecto (a diferencia de `watch_positions`/`fetchPositionsSnapshot` para posiciones, opt-in).
- **Implicación:** el saldo vía WS es **incremental y sin estado inicial completo**. Para tener un saldo correcto tras arranque o reconexión, hay que pedir `fetch_balance` (REST) primero. Coincide con el diseño de ADR-0030 (REST como fuente, WS solo como acelerador opcional).

### [F-CCXTPRO-03] Bybit V5 `orderStatus` NO tiene `PendingCancel`
- **Severidad:** Informativa (matiz para ADR-0029)
- **Estado:** VERIFIED (HECHO observado)
- **Evidencia:** en `ccxt/pro/bybit.py` (y `ccxt/bybit.py`) la normalización `parse_order_status` mapea estados v5: `PendingCancel` → `open`; `Cancelled` → `canceled`; `PartiallyFilledCanceled` → `closed`; `Filled` → `closed`. El protocolo v5 **no expone un estado de cancelación en curso**: una cancelación en marcha sigue viéndose `open` (o `Cancelled` cuando se confirma).
- **Implicación:** OCM NO puede derivar `CANCELLING` del estado del exchange; debe gestionarlo **localmente** (estado transitorio propio en el dominio) y confirmar el estado real por `fetch_order`/WS. Esto valida el diseño de ADR-0029: `CANCELLING` es un estado interno del dominio, no un reflejo del exchange.

### [F-CCXTPRO-04] CCXT Pro es parte del monorepo CCXT (MIT)
- **Severidad:** Informativa (dependencia)
- **Estado:** VERIFIED (HECHO observado)
- **Evidencia:** CCXT Pro se distribuye en el mismo monorepo (`ccxt/pro/`) con licencia MIT; ya no es un paquete separado propietario. El `ccxt` instalado en OCM incluye el subpaquete `ccxt.pro` (verificado en `.venv/lib/python3.13/site-packages/ccxt/pro/`).
- **Implicación:** si en el futuro OCM quisiera WS privado, **no necesita una dependencia nueva**: `ccxt.pro` ya está disponible en la instalación actual. Decisión de arquitectura, no de licencia.

---

## 3. Matriz de comparación de referentes (patrones relevantes)

| Patrón | LEAN | vn.py | CCXT Pro | Hummingbot | Nautilus | Freqtrade | **OCM (propuesta)** |
|---|---|---|---|---|---|---|---|
| Estado intermedio "cancelando" | `CancelPending` | No | No (v5 sin `PendingCancel`) | `PENDING_CANCEL` | `PendingCancel` | No explícito (DB + re-fetch) | **`CANCELLING` transitorio** |
| Confirmar cancel por exchange | Evento de brokerage | Query tras reconexión | `watch_orders` caché local | Evento WS / REST | Response WS + reconciliation | `cancel_order_with_result` + re-fetch | **`fetch_state`/`fetch_order` + loop** |
| Fill prevalece sobre cancel | Sí (revert + evento) | Sí (reconciliación) | Sí (caché con estado real) | Sí (acumulativo) | Sí (eventos ordenados) | Sí | **Sí (política explícita)** |
| Cancel fallido → estado final | Revert a previo | Reconciliación | No inventa | FAILED | CancelRejected | Re-fetch | **`CANCELLING` hasta confirmar** |
| Órdenes abiertas persistidas | Event store + brokerage | Dict/DB | Caché en memoria | InFlightOrder JSON | Event store (replay) | DB SQLAlchemy | **Dict en memoria + loop (hoy)** |
| Reconciliación al arranque | `BrokerageSetupHandler` + `PerformCashSync` | Query | `fetch_open_orders` | `restore_tracking_states` | Reconciliation engine + replay | `startup_update_open_orders` | **Propuesta en ADR-0029** |
| Balance desde exchange | `MarginRemaining` derivado | `balance − frozen` | `watch_balance` (incremental) + REST | WS wallet + REST backup | wallet-balance + account state | `fetch_balance` | **`fetch_balance` + derivación UNIFIED** |
| Fuente de verdad del saldo | Brokerage + cash sync | Exchange query | REST (WS no tiene snapshot) | Exchange | Exchange | Exchange | **Exchange (REST)** |

---

## 4. Impacto en ADR-0029 (cancelación real + gestión de órdenes abiertas)

| Punto de ADR-0029 | Soporte de referentes | Veredicto |
|---|---|---|
| `CANCELLING` transitorio en el dominio | LEAN (`CancelPending`), Hummingbot (`PENDING_CANCEL`), Nautilus (`PendingCancel`); vn.py diverge (sin estado) | **CONFIRMA** — más defensivo que vn.py |
| El fill prevalece sobre el cancel | LEAN (revert), vn.py (reconciliación), CCXT Pro (caché), Hummingbot, Nautilus | **CONFIRMA** |
| Confirmar por `fetch_state`/`fetch_order` | CCXT Pro (`fetch_order`/`fetch_open_orders` como reconciliación necesaria por caché local), Hummingbot, Nautilus | **CONFIRMA** |
| No dar por cerrado sin confirmación (fail-closed) | vn.py (lo resuelve por reconciliación, no inventa), LEAN (brokerage es autoridad) | **CONFIRMA** |
| `CANCELLING` como estado local (v5 no expone `PendingCancel`) | CCXT Pro (`parse_order_status` mapea sin `PendingCancel`) | **CONFIRMA** — el exchange no ofrece el estado intermedio; debe ser interno |

**Veredicto ADR-0029: CONFIRMADA SIN CAMBIOS.** Las observaciones del benchmark complementario se limitan a matices (caché local de `watch_orders`, ausencia de `PendingCancel` en v5), y ninguna obliga a modificar la decisión.

---

## 5. Impacto en ADR-0030 (saldo real + reconciliación patrimonial)

| Punto de ADR-0030 | Soporte de referentes | Veredicto |
|---|---|---|
| Saldo disponible = derivación, no dato crudo | LEAN (`MarginRemaining`), vn.py (`available = balance − frozen`), CCXT (`free/used`) | **CONFIRMA** |
| `fetch_balance` REST como fuente de verdad | CCXT Pro (`watch_balance` sin snapshot), Hummingbot (REST backup), vn.py | **CONFIRMA** |
| Derivación UNIFIED (`free = available`, `used = locked + IM + ...`) | vn.py (frozen = locked + totalOrderIM + totalPositionIM + bonus), CCXT (parse_balance) | **CONFIRMA** |
| Reconciliación al arranque | LEAN (`BrokerageSetupHandler`), Hummingbot, Nautilus | **CONFIRMA** |
| WS privado solo como acelerador opcional | CCXT Pro (`watch_balance` incremental, sin snapshot → requiere REST) | **CONFIRMA** |

**Veredicto ADR-0030: CONFIRMADA SIN CAMBIOS.**

---

## 6. Observaciones de diseño (no bloqueantes, para fases posteriores)

1. **CCXT Pro ya está disponible sin dependencia nueva** (`ccxt.pro` en el monorepo instalado, MIT). Si un día se quiere WS privado (orders/balance), la opción más barata es `ccxt.pro` `watch_orders`/`watch_balance` **siempre con reconciliación REST** (`fetch_open_orders`/`fetch_balance`) tras reconexión — la caché local sola no es fiable (F-CCXTPRO-01/02).
2. **El estado `CANCELLING` no es observable desde Bybit v5** (F-CCXTPRO-03). Es un estado de libro (dominio OCM), invisible al exchange; el diseño de ADR-0029 ya lo asume correctamente, pero conviene explicitarlo en la documentación para que nadie intente "leerlo" del exchange.
3. **Reconciliación periódica de saldo:** LEAN hace `PerformCashSync` periódico además del arranque. OCM podría considerar una reconciliación periódica (no solo al arranque) para corregir deriva de saldo en sesiones largas; es una evolución opcional, no un requisito de la ADR.

---

## 7. Matriz de evidencia del benchmark principal (re-verificación en HEAD)

El benchmark principal (Freqtrade/Hummingbot/Nautilus, `2026-08-15-bot-benchmark-b-md-008-009.md`) fue re-verificado contra HEAD:

| Hallazgo benchmark | Estado en HEAD |
|---|---|
| `OMS.cancel()` local-only, sin llamada a transporte | VERIFIED |
| `OrderTransport` sin `cancel` (Protocol) | VERIFIED |
| `CCXTAdapter` sin `cancel_order` | VERIFIED |
| `CANCELLED` terminal en `_VALID_TRANSITIONS` | VERIFIED |
| `fill_sync.on_fill_composite` solo vía `on_fill` | VERIFIED |
| `capital_usd` configurado (sin `fetch_balance`) | VERIFIED |
| Sin gestión de órdenes abiertas (sin caller de cancel) | VERIFIED |
| Bybit market order = IOC; acuse de create asíncrono | VERIFIED (fuentes CCXT + docs Bybit) |
| Carrera CANCEL/FILL documentada por Bybit (doble `Filled`) | VERIFIED (docs oficiales Bybit) |
| `totalAvailableBalance` solo en `info`, no en estructura normalizada | VERIFIED (CCXT `parse_balance`) |

**Sin discrepancias.** El benchmark complementario no encontró evidencia suficiente para modificar las ADR.

---

## 8. Conclusión final

- **ADR-0029: CONFIRMADA SIN CAMBIOS.**
- **ADR-0030: CONFIRMADA SIN CAMBIOS.**
- Los tres referentes nuevos (LEAN, vn.py, CCXT Pro) convergen con el núcleo de ambas ADR.
- Se documentan 3 observaciones de diseño no bloqueantes para fases posteriores.
- Sin cambios de código, sin cambios de contrato, sin cambios de ADR, sin commits/push.
