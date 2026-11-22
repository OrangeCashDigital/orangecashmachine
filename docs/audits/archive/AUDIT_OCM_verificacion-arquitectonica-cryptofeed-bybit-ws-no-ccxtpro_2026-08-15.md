# Verificación Arquitectónica — cryptofeed, Bybit Private WS y eliminación de CCXT Pro

**Fecha:** 2026-08-15
**Auditor:** OpenAI Codex (asistido por agentes de exploración)
**Rol:** Arquitecto Principal de Software · Trading Systems Reviewer
**Objetivo:** corregir definitivamente la evidencia del benchmark B-MD-008/B-MD-009 respecto al transporte WebSocket, verificar el uso real de cryptofeed en OCM, evaluar cryptofeed 2.4.1, el WebSocket privado nativo de Bybit y CCXT estándar, y determinar la arquitectura objetivo **sin CCXT Pro** (no es dependencia objetivo).
**Método:** evidencia directa de código (`archivo:línea`), fuentes instaladas en `.venv/` (cryptofeed 2.4.1, ccxt 4.5.70) y documentación oficial Bybit v5 (`bybit-exchange.github.io`). **Sin implementar nada.**
**Restricciones respetadas:** sin modificar código, ADR-0029, ADR-0030, `tracking.yaml`, ni docs existentes; sin instalar/añadir CCXT Pro; sin commits/push. Único archivo creado: este informe.
**Regla de honestidad:** cada hallazgo se marca **VERIFIED** (evidencia directa), **INFERENCE** (deducción razonable no demostrada) o **UNKNOWN**.

---

## 1. Uso real de cryptofeed en OCM (HEAD)

| Pregunta | Respuesta | Evidencia |
|---|---|---|
| ¿Dónde se inicializa? | `CompositionRoot.build_ws_producers` (producers Kafka) + `CryptofeedOrderBookStream` (canary) + runners `BybitCryptofeedRunner`/`KucoinCryptofeedRunner` | `packages/market_data/infrastructure/bootstrap/composition_root.py:323,340-380`; `apps/app/cli/streaming_hydra.py:186-216` |
| ¿Qué versión? | `cryptofeed>=2.4.1` (instalado: **2.4.1**) | `pyproject.toml:101`; `.venv` verificado |
| ¿Qué exchanges/canales? | Bybit `TRADES` (`publicTrade`), KuCoin `TRADES`, Bybit/KuCoin `L2_BOOK` | `bybit_cryptofeed_runner.py:62-66`; `kucoin_cryptofeed_runner.py:51-52`; `cryptofeed_orderbook_stream.py:114-118` |
| ¿Qué callbacks? | `_translate_and_dispatch` (TradeCallback → `NormalizedTrade`); `_translate_and_dispatch` L2 (→ `on_snapshot`/`on_delta`) | `bybit_cryptofeed_runner.py:78`; `cryptofeed_orderbook_stream.py:148-170` |
| ¿Qué datos recibe? | Trades públicos (`publicTrade`) y order book L2 público (`orderbook.N`) | idem |
| ¿Solo market data pública? | **SÍ** — solo canales públicos | runners solo `TRADES`/`L2_BOOK` |
| ¿Implementación de private/user stream? | **NO** — `WSTradesSource` es un stub con TODO | `ws_trades_source.py:73,85` |
| ¿Callbacks de órdenes/fills/balance? | **NO** | grep `ORDER_INFO`/`FILLS`/`watch_` en adapters OCM = 0 |
| ¿Qué adapters/ports conectan a cryptofeed? | `BybitFeedAdapter`/`KucoinFeedAdapter` (adapters inbound WS) + `FeedRunnerProtocol` (abstracción) | `bybit_feed_adapter.py:53-60`; `websocket/feed_runner_protocol.py` |
| ¿Qué composition root lo inicializa? | `market_data.infrastructure.bootstrap.composition_root` (BC-38) + registry `feed_registry.py:20-23` | `feed_registry.py:16-30` |

**Nota de realidad:** `ingestion_mode: rest` por defecto (`config/market_data/feeds.yaml:17`); el orquestador solo arranca feeds WS en modo `websocket`/`dual` (`feed_orchestrator.py:88-95`). El único WS activo hoy es el canary ORDERBOOK (`streaming_hydra.py`).

---

## 2. Capacidades reales de cryptofeed 2.4.1 (Bybit)

### Market data pública — **VERIFIED**
- Order book L2: `L2_BOOK` → `orderbook.N` sobre `wss://stream.bybit.com/v5/public/linear` y `/v5/public/spot`. [cryptofeed `bybit.py:29-44`]
- Trades: `TRADES` → `publicTrade` (v5 public). [idem]
- Ticker: `TICKER` → `tickers` (v5). [idem]
- Snapshot + deltas L2 con checksum; timestamps normalizados. [cryptofeed `bybit.py:405` `book_callback`]
- Reconexión: cryptofeed gestiona reconexión del `FeedHandler` (mecanismo interno del library). [INFERENCE — no verificado en detalle; ver §I UNKNOWN]

### Private account data — **PARCIAL / NO VIABLE para v5**
- cryptofeed 2.4.1 **declara** canales privados `ORDER_INFO` (`order`) y `FILLS` (`execution`) para Bybit: `bybit.py:32-33`.
- **PERO** el endpoint privado fijado es **`wss://stream.bybit.com/realtime_private`** (`bybit.py:44`) — este es el endpoint **legacy v3**, **NO** el v5 oficial `wss://stream.bybit.com/v5/private` (verificado en docs Bybit). El parseo usa campos **v3** (`order_id`, `order_status`, `cum_exec_qty`, `exec_id`, `trade_time`, `update_time`) — `bybit.py:505-568`.
- **Sin wallet/balance para Bybit:** el método `_balances` está **comentado** (`bybit.py:604-608`). No hay soporte de posiciones en el exchange Bybit de cryptofeed.
- **Sin order entry:** cryptofeed es un library de **market data** (keywords de METADATA: "market feed, market data... Trades, Tickers, Order book"). No crea/cancela órdenes.

**Conclusión privada:** cryptofeed 2.4.1 **NO puede cubrir** órdenes/fills/balance/posiciones de Bybit de forma fiable: el endpoint privado es v3-legacy (no v5), el balance está deshabilitado y no hay order entry. **Es un transporte de market data pública, no de cuenta.**

---

## 3. Bybit Private WebSocket (docs oficiales v5) — **VERIFIED**

- Endpoint: `wss://stream.bybit.com/v5/private` (mainnet) / `wss://stream-testnet.bybit.com/v5/private` (testnet). [docs `/v5/ws/connect`]
- Autenticación: `op: auth`, `args: [api_key, expires, signature]`, `signature = HMAC_SHA256(secret, "GET/realtime{expires}")`. Ping/pong cada 20 s; conexión cortada a los 10 min sin actividad (`max_active_time` configurable). [docs]
- Topics privados: `order` (all-in-one) u `order.spot|linear|inverse|option`; `execution` (+`execution.fast`); `wallet`; `position`; `greek`. [docs + AsyncAPI oficial]
- **`order`:** sin snapshot al suscribirse (eventos por orden). **Carrera cancel/fill documentada**: "You may receive two `orderStatus=Filled` messages when the cancel request is accepted but the order is executed at the same time" — uno `EC_NoError`, otro `cancelType=CancelByUser, rejectReason=EC_OrigClOrdIDDoesNotExist`. → El fill gana. [docs `/v5/websocket/private/order`]
- **`wallet`:** **sin snapshot al suscribirse** ("There is no snapshot event given at the time when the subscription is successful") y el cambio de UPL no dispara evento. → Incremental; tras reconexión se requiere REST `wallet-balance`. [docs `/v5/websocket/private/wallet`]
- **`position`:** incluye `seq` (permite detectar out-of-order). `order`/`wallet` no tienen seq. [AsyncAPI oficial]
- Duplicados: posibles (doble `Filled`); out-of-order: solo detectable en `position`. **Reconciliación REST necesaria** para estado completo tras reconexión.

**Pregunta crítica:** ¿Puede OCM usar el WS privado nativo de Bybit sin pagar CCXT Pro? **SÍ.** Es una API oficial, libre y gratuita (solo requiere API key/secret). Encajaría en OCM como un **port nuevo** (p. ej. `AccountEventPort`/`PrivateOrderEventPort` en `shared/contracts/`) + un **adapter dedicado** (`packages/trading/.../bybit_private_ws_adapter.py`) que autentique, suscriba `order`/`execution`/`wallet`, traduzca eventos al dominio y publique a callbacks `on_order_update`/`on_fill`/`on_balance_update` — análogo a cómo `BybitCryptofeedRunner` aísla cryptofeed del dominio. **Sin implementar.**

---

## 4. CCXT estándar (Bybit v5) — **VERIFIED**

Métodos REST presentes en `ccxt/bybit.py` (4.5.70):

| Método | Línea | Nota |
|---|---|---|
| `cancel_order` | `bybit.py:4583` | requiere `symbol`; parsea solo `orderId`/`orderLinkId` → estado real por `fetch_order` |
| `cancel_orders` (batch) | `:4619` | — |
| `cancel_all_orders` | `:4818` | — |
| `fetch_order` | `:4923` | estado real de una orden |
| `fetch_orders` | `:5015` | — |
| `fetch_open_orders` | `:5370` | `/v5/order/realtime` — órdenes abiertas |
| `fetch_closed_orders` | `:5318` | `/v5/order/history` |
| `fetch_balance` | `:3509` | `/v5/account/wallet-balance`; UNIFIED |
| `fetch_my_trades` | `:5506` | fills |
| `fetch_order_trades` | `:5484` | — |
| `fetch_positions` | `:6314` | `/v5/position/list` |
| `fetch_position` | `:6242` | — |
| `set_leverage` | `:6762` | — |

**WebSocket:** `ccxt/bybit.py` **no tiene ningún método `watch_*`** (grep = 0). Los `watch_*` viven en `ccxt/pro/` (subpaquete CCXT Pro) — disponible en el mismo paquete instalado, pero **no usado por OCM**. **CCXT estándar NO proporciona WebSocket privado.** Todo lo relacionado con account (órdenes, fills, balance, posiciones) queda necesariamente en **REST**.

**Balance UNIFIED:** `fetch_balance` usa `/v5/account/wallet-balance?accountType=UNIFIED`; `parse_balance` (`bybit.py:3470-3503`): `accountType==UNIFIED` → `total = walletBalance`, `free = availableToWithdraw|free` (deprecado), si no `used = locked + totalPositionIM + totalOrderIM`. **`totalAvailableBalance` NO está en la estructura normalizada, solo en `info`** — hay que leerlo de `info` o derivarlo. [VERIFIED]

---

## 5. Restricción CCXT Pro

- CCXT Pro **NO es dependencia objetivo de OCM**: solución comercial de pago que no forma parte de la arquitectura.
- En esta investigación se menciona únicamente como **referencia comparativa externa** para demostrar qué funcionalidad no debe depender de una librería comercial.
- La solución recomendada es viable con **software libre/open-source + APIs oficiales Bybit** (cryptofeed MIT, CCXT MIT, WebSocket privado nativo de Bybit gratis).

---

## 6. Arquitectura objetivo (evaluación de alternativas)

| Criterio | A: cryptofeed + CCXT REST | B: A + Bybit Private WS | C: cryptofeed + Bybit REST nativo + Bybit WS | D: otra |
|---|---|---|---|---|
| Coste licencias | 0 | 0 | 0 | — |
| Estabilidad | Alta | Alta | Media (más código de integración nativo) | — |
| Seguridad | Alta (fail-closed REST) | Alta | Alta | — |
| Mantenibilidad | Alta (reutiliza CCXTAdapter) | Alta (WS aislado por adapter) | Media (duplicar parsing que CCXT ya hace) | — |
| Testabilidad | Alta | Alta | Media | — |
| Observabilidad | Alta | Alta | Media | — |
| Recuperación | Alta (REST snapshot) | Alta (WS + REST resync) | Media | — |
| Reconciliación | Alta | Alta | Media | — |
| Dependencia exchange | Media (CCXT abstraction) | Media | **Alta** (REST nativo = vendor lock directo) | — |
| Complejidad | Baja | Media | Media-Alta | — |
| Compatible con OCM | Sí | Sí | Parcial (duplica lógica CCXT) | — |

**ELEGIDA: Alternativa B** — `cryptofeed → market data pública`; `CCXT estándar (REST) → trading/account (autoridad, reconciliación)`; `Bybit Private WebSocket nativo → eventos de cuenta (observación)`. 

Justificación: reutiliza la infraestructura existente (`CCXTAdapter`, `_BybitTransport`), añade WS privado **solo como observador** aislado por port/adapter, y REST sigue siendo la autoridad — la regla "el transporte de eventos no es el Source of Truth". Alternativa C (REST nativo) **no se elige**: duplica parsing que CCXT ya mantiene y aumenta vendor lock sin ganancia de seguridad (el REST nativo no aporta nada que CCXT no envuelva; la capa de valores de CCXT es estable y testeada).

```
                    ┌────────────────────┐
                    │      Bybit         │
                    └─────────┬──────────┘
                              │
             ┌────────────────┴────────────────┐
             │                                 │
        REST API                         Private WebSocket
             │                                 │
             ▼                                 ▼
      Exchange Adapter                  Event Adapter
     (CCXTAdapter REST)              (BybitPrivateWSAdapter)
             │                                 │
             └──────────────┬──────────────────┘
                            │
                    Reconciliation
                    (REST = autoridad)
                            │
              ┌─────────────┴─────────────┐
              │                           │
             OMS                       Portfolio
              │                           │
              ▼                           ▼
            Risk                       Risk
              │                           │
              └─────────────┬─────────────┘
                            ▼
                         Execution

cryptofeed ───────────────► Market Data (pública, fuera del camino de órdenes)
```

---

## 7. B-MD-008 — CANCEL/FILL sin CCXT Pro

**REST solamente:** `request_cancel(order_id)` → `CANCELLING` (transición local, `order.py`); `_BybitTransport.cancel(symbol, id)` → `CCXTAdapter.cancel_order` (nuevo método REST, `ccxt_adapter.py`); confirmación vía **`fetch_order`** (REST) con reintento/backoff hasta estado terminal; timeout/error/desconexión → permanece `CANCELLING` (fail-closed, nunca decreta `CANCELLED` sin confirmación). El fill real se detecta por `fetch_order` (estado `Filled`) → `OMS._fill` → `FILLED`.

**WebSocket privado (observador):** `BybitPrivateWSAdapter` suscribe `order`; eventos `orderStatus` (`New`/`PartiallyFilled`/`Filled`/`Cancelled`) llegan en realtime; el `manage_open_orders` los usa como **señal de resolución temprana**, pero la transición final la confirma REST (`fetch_order`).

**Contradicción temporal REST vs WS:** la regla es que **REST gana**. Un evento WS `Cancelled` que el siguiente `fetch_order` devuelve `Filled` (carrera cancel/fill) se resuelve como `FILLED` (el fill prevalece — política explícita ADR-0029). El WS nunca decreta estado terminal por sí solo: notifica, REST confirma.

**Veredicto:** **Private WS + REST reconciliation ES la solución más robusta sin CCXT Pro** — la más defensiva (eventos tempranos + autoridad REST), coste cero, alineada con ADR-0029.

---

## 8. B-MD-009 — BALANCE sin CCXT Pro

- **Qué lee el adapter:** `CCXTAdapter.fetch_balance` (REST) con `accountType=UNIFIED` → `/v5/account/wallet-balance`; leer `totalAvailableBalance` (USD) de `info` o derivar `free`/`used` de `walletBalance`/`locked`/`totalOrderIM`/`totalPositionIM` (CCXT `parse_balance` no normaliza `totalAvailableBalance`).
- **Qué expone el `BalancePort`:** `total` (walletBalance/equity), `available_operativo` (`totalAvailableBalance` USD), `locked`, `unrealisedPnL`, `totalOrderIM`, `totalPositionIM`, `freshness` (timestamp de lectura). **Nunca `free`/`availableToWithdraw`** como disponible operativo (deprecados para UNIFIED desde 2025-01-09, docs Bybit).
- **SSOT:** `Exchange (Bybit) = autoridad externa` → `Portfolio = único dueño del estado patrimonial interno` (BalanceStore/PositionStore BC-43) → `Risk/Execution consumen vía port`. Diagrama `EXCHANGE → PORTFOLIO → RISK → EXECUTION` se mantiene.
- **Reconciliación:** gate de arranque (comparar `capital_usd` configurado vs `totalAvailableBalance` real; discrepancia material → no habilitar live); loop periódico asíncrono; tras fill y tras error/desconexión/reconexión. Discrepancias clasificadas con tolerancias configurables (fail-closed: bloqueo de órdenes si material; nunca auto-corregir posiciones).
- **WS vs REST difieren:** REST gana (autoridad); WS wallet es incremental y sin snapshot → nunca es fuente; tras reconexión → REST resync obligatorio.
- **Volatilidad extrema:** latencia en `wallet-balance` (aviso oficial Bybit) → saldo cacheado puede estar stale; mitigado por freshness (B-MD-001) y bloqueo ante discrepancia material. Nunca operar con saldo sospechoso.

---

## 9. Recuperación

- **Startup:** gate de saldo (REST) + rehidratar posiciones desde `PositionStore` (ADR-0027) + `manage_open_orders` reconcilia órdenes `SUBMITTED`/`CANCELLING` contra `fetch_open_orders`/`fetch_order` (REST).
- **WS desconectado:** órdenes/fills/posiciones/balance NO se pierden (el WS no es autoridad); `manage_open_orders` sigue con REST; el adapter reconecta y **pide snapshot/reconciliación REST** (order: `fetch_open_orders`; wallet: `fetch_balance`) antes de volver a confiar en eventos.
- **REST caído:** bloquear **submit/cancel/fetch** (fail-closed) — no operar sin poder reconciliar; alertar.
- **WS reconectado:** solicitar resync REST (snapshot) y solo entonces aplicar incrementos.
- **Restart durante CANCELLING:** el OMS reconstruye su estado desde REST al arranque (`fetch_open_orders`); una orden que quedó CANCELLING en memoria no se da por cerrada: se reconcilia contra exchange (REST) y se resuelve `CANCELLED` (confirmado) o `FILLED` (fill real) / `REJECTED`.
- **Recomendación:** **REST snapshot + WS incremental** cuando sea necesario, nunca eventos solos.

---

## 10. Source of Truth

| Ámbito | SoT |
|---|---|
| Órdenes | **OMS = SSOT interno** (estado de la orden) |
| Patrimonio | **Portfolio = SSOT patrimonial interno** |
| Estado real de cuenta | **Bybit = autoridad externa** |
| Market data | **cryptofeed = transporte**, no autoridad patrimonial |
| WebSocket | **fuente de eventos/incrementos**, no sustituto de reconciliación |
| REST | **mecanismo de consulta/reconciliación** del estado real |

Regla: **un solo SoT por ámbito**. WS nunca decreta terminales; REST confirma. cryptofeed nunca alimenta estado de cuenta.

---

## 11. Impacto en ADR-0029 (revisión, sin modificar)

- La ADR queda **CONFIRMADA**, con recomendación de **especificar el transporte** en fases posteriores.
- **No contiene dependencia incorrecta de WebSocket**: la ADR ya trata el WS como medio de confirmación *alternativo/complementario* a `fetch` y el fill prevalece. [ADR-0029:19-23,56,89-91]
- **Aclaración recomendada (no bloqueante):** declarar explícitamente que **cryptofeed NO es responsable de private order updates** (es market data pública) y que la confirmación de cancel usa **REST (`fetch_order`/`fetch_open_orders`)** como autoridad, con WS privado nativo (Bybit) solo como observador opcional. No cambia la mecánica CANCELLING ni la regla fill-prevalece.
- **No requiere modificar ahora.** Es una precisión documental.

---

## 12. Impacto en ADR-0030 (revisión, sin modificar)

- La ADR queda **CONFIRMADA**; ya exige `totalAvailableBalance` (no `free`) para UNIFIED y define REST `wallet-balance` como fuente. [ADR-0030:15-19, Decisión §3, §5]
- **Aclaración recomendada (no bloqueante):** distinguir explícitamente REST `wallet-balance` (autoridad, SSOT) de los WebSocket wallet updates (incrementales, sin snapshot, nunca fuente). ADR-0030 no menciona el WS wallet; añadir esa distinción documental reforzaría el diseño, sin cambiar la decisión.
- **`totalAvailableBalance` explícito en el adapter:** la ADR ya lo exige en el `BalancePort`; el matiz es que CCXT no lo normaliza (solo en `info`) — el adapter debe leerlo de `info` o derivarlo. La ADR no lo detalla; es un detalle de implementación que puede quedar en la fase de implementación.

---

## 13. Resultado (A-I)

**A. Qué usa OCM actualmente** — cryptofeed 2.4.1 solo market data pública (`TRADES`/`L2_BOOK`) vía runners (`bybit_cryptofeed_runner.py:62-66`, `kucoin_cryptofeed_runner.py:51-52`, `cryptofeed_orderbook_stream.py:114-118`); CCXT estándar REST para `create_order`/`fetch_order` (`composition_root.py:230-255`, `ccxt_adapter.py:405,462`); **sin** cancel/saldo/posiciones/órdenes abiertas; **sin** WS privado; **sin** CCXT Pro (solo comentario ISP + TODO stub). `ingestion_mode: rest` por defecto (`feeds.yaml:17`).

**B. Qué puede hacer cryptofeed 2.4.1** — pública: order book L2, trades, ticker, candles, OI/funding/liquidation (canales v5 público) — **VERIFIED**. Privada: **NO viable para v5** (endpoint privado legacy v3 `realtime_private`, parseo v3, balance comentado, sin order entry) — **VERIFIED**. Es solo market data.

**C. Qué NO debe hacer OCM** — **CCXT Pro no es dependencia objetivo** (comercial de pago; fuera de la arquitectura). No diseñar OCM alrededor de sus streams. Solo referencia comparativa: demuestra que la funcionalidad de account (órdenes/fills/balance/posiciones) no debe depender de una librería comercial.

**D. Arquitectura recomendada** — Alternativa B (ver diagrama §6): cryptofeed → market data; CCXT REST → trading/account (autoridad); Bybit Private WS nativo → eventos de cuenta (observador aislado); reconciliación REST.

**E. B-MD-008** — REST como autoridad: `cancel_order` + `fetch_order`/`fetch_open_orders` confirman; WS privado `order` solo observa (resolución temprana); REST gana ante contradicción; fill prevalece; `CANCELLING` local (v5 no expone `PendingCancel`).

**F. B-MD-009** — REST `wallet-balance` UNIFIED como SSOT; `totalAvailableBalance` en `info`; Portfolio como dueño del estado patrimonial; WS wallet incremental sin snapshot (nunca fuente); resync REST tras reconexión; fail-closed ante discrepancia material.

**G. ADR-0029** — **CONFIRMADA**. Aclaración recomendada: declarar cryptofeed no-responsable de private updates; REST como autoridad de confirmación. Sin modificación.

**H. ADR-0030** — **CONFIRMADA**. Aclaración recomendada: distinguir REST wallet-balance (autoridad) de WS wallet updates (observación). Sin modificación.

**I. UNKNOWN restantes** — ver §14.

---

## 14. UNKNOWN reales

- Comportamiento exacto de reconexión de cryptofeed 2.4.1 en condiciones de red adversas (mecanismo interno del `FeedHandler`; no probado en runtime OCM).
- Si la renovación de la conexión v5 `order`/`wallet` de Bybit emite eventos de estado completo bajo `max_active_time` cortos (comportamiento de sesión, no documentado exhaustivamente).
- Rendimiento/latencia real del WS privado nativo frente a REST polling en OCM (no medido).
- Si Bybit mantiene `realtime_private` (v3) activo por compatibilidad (puede retirarse; afecta solo a la ruta privada de cryptofeed, que OCM no usa).
- Detalle de parseo de `execution.fast` (v5) no verificado.

---

## 15. Pregunta final

**SÍ. OCM puede construir una arquitectura robusta de trading en Bybit SIN CCXT Pro.** Separación de responsabilidades:

1. **Market data pública → cryptofeed 2.4.1** (lo que resuelve bien: trades, L2, ticker; MIT, ya integrado).
2. **Trading/account → CCXT estándar REST** (ya presente: `create_order`/`fetch_order`; añadir `cancel_order`, `fetch_open_orders`, `fetch_balance` como métodos REST del `CCXTAdapter`). **Autoridad y reconciliación**.
3. **Eventos de cuenta (opcional, solo si aporta) → Bybit Private WebSocket nativo** (`wss://stream.bybit.com/v5/private`, gratis, API oficial) como **adapter aislado** tras un **port nuevo** (`shared/contracts/`), análogo a cómo `BybitCryptofeedRunner` aísla cryptofeed. Observador, nunca SoT.
4. **Reconciliación → REST** como única autoridad de estado real; WS = incrementos.

No falta ninguna capacidad. cryptofeed cubre el market data; CCXT estándar (MIT) cubre todas las responsabilidades de account por REST; el WS privado nativo de Bybit (libre) cubre, si se desea, la observación realtime. Todo con coste de licencias **cero**. **No se implementa nada.**

---

# Auditoría de Cierre — Verificación final de consistencia (estado real vs diseñado)

**Fecha:** 2026-08-15
**Objetivo:** auditoría estricta de consistencia sobre el HEAD actual: separar lo IMPLEMENTADO de lo DISEÑADO/AUSENTE, verificar la afirmación crítica sobre `fetch_balance`, y confirmar la arquitectura final sin asumir capacidades inexistentes.
**Restricciones:** solo lectura; sin modificar código/ADRs/tracking.yaml; sin commits. Este doc se actualizó con esta sección (autorizado por el usuario).

---

## A. Verificación por capacidad (estado real en HEAD)

| Capacidad | Estado | Evidencia |
|---|---|---|
| `create_order` | **IMPLEMENTADO** | `ccxt_adapter.py:405` → `client.create_order`; `_BybitTransport.submit` `composition_root.py:230-240` |
| `cancel_order` | **AUSENTE** | `CCXTAdapter` sin `cancel_order` (métodos listados `ccxt_adapter.py:116-549` no lo incluyen); grep `cancel_order` en packages/apps = 0 |
| `fetch_order` | **IMPLEMENTADO** | `ccxt_adapter.py:462`; `_BybitTransport.fetch_state` `composition_root.py:245-255` |
| `fetch_open_orders` | **AUSENTE** | grep = 0; no existe `manage_open_orders` en packages/apps/shared |
| `fetch_balance` | **AUSENTE** | `CCXTAdapter` no lo expone (lista de métodos); grep `fetch_balance` en packages/apps = 0; solo `capital_usd` configurado |
| `fetch_positions` | **AUSENTE** | grep = 0 |
| `OrderTransport` (Protocol) | **IMPLEMENTADO** (sin `cancel`) | `transport.py:96-128` (`submit`/`fetch_state`/`close`) |
| `BalancePort` | **AUSENTE** | `shared/contracts/boundaries.py` solo tiene `FeatureSource`, `SignalProtocol`, `RiskGate`, `RebalancePort`; sin `BalancePort` |
| `OMS` | **IMPLEMENTADO** | `oms.py:119-565` (`execute`, `submit`, `cancel` local, `open_orders`, `_fill`, `_reject`) |
| `Portfolio` (PortfolioService) | **IMPLEMENTADO** | `portfolio_service.py:63-294` (open/close/snapshot; `capital_usd` fijo) |
| `RiskManager` | **IMPLEMENTADO** | `risk/manager.py:112-259` (validate, sizing contra `capital_usd`) |
| `LiveExecutor` | **IMPLEMENTADO** | `live_executor.py:113-278` (`execute`, `_submit`, `_reconcile` fail-closed, guard) |
| `manage_open_orders` | **AUSENTE** | no existe; grep = 0 |
| Reconciliación de órdenes | **PARCIAL** | `live_executor.py:229-266` `_reconcile` (solo confirma fill post-submit); sin reconciliación de órdenes abiertas/abandonadas |
| Reconciliación de balance | **AUSENTE** | sin `fetch_balance`; sin gate de saldo al arranque |
| Composition roots | **IMPLEMENTADO** | `market_data/.../composition_root.py`, `trading/bootstrap/composition_root.py:203-262` (_BybitTransport), `portfolio/bootstrap/composition_root.py` |
| Adapters | **PARCIAL** | `CCXTAdapter` (trading REST presente); sin adapter de balance; sin observer WS privado |
| Transport | **IMPLEMENTADO** (sin cancel) | `_BybitTransport` `composition_root.py:203-262` (`submit`/`fetch_state`/`close`); `PaperTransport` `transport.py:131-158` |

## B. Verificación de la afirmación crítica sobre `fetch_balance`

**En el HEAD actual `fetch_balance` NO existe en OCM.** La afirmación "fetch_balance UNIFIED + BalancePort + totalAvailableBalance" está **diseñada en ADR-0030** (Decisión §3: nuevo `BalancePort` + `_BybitBalanceSource` en portfolio composition root → `CCXTAdapter.fetch_balance`), pero **no está implementada**: `CCXTAdapter` no expone el método, no existe `BalancePort` en `shared/contracts/boundaries.py`, y no hay adapter de balance en `portfolio/bootstrap/composition_root.py` (solo `capital_usd` por configuración).

- La afirmación anterior "CCXTAdapter no tiene fetch_balance actualmente" es la **cierta**.
- La afirmación "fetch_balance UNIFIED + BalancePort + totalAvailableBalance" describe el **estado objetivo diseñado**, no el actual.
- **Trabajo pendiente:** (1) añadir `fetch_balance` (REST UNIFIED) a `CCXTAdapter`; (2) definir `BalancePort` en `shared/contracts/boundaries.py`; (3) materializar en portfolio composition root (`_BybitBalanceSource`); (4) RiskManager consume vía port con `totalAvailableBalance` (en `info` de CCXT, no normalizado).

## C. Verificación de la arquitectura final

El diseño propuesto se **confirma como coherente**:

- **REST API → CCXTAdapter → OMS (SSOT órdenes) / Portfolio (SSOT patrimonial) → Risk → Execution:** coherente con el estado real (`CCXTAdapter` REST ya existe, OMS/Portfolio/Risk/LiveExecutor implementados, BC-43/BC-50). [VERIFIED]
- **Private WS opcional → future observer:** coherente con la arquitectura de ports/adapters (mismo patrón que `BybitCryptofeedRunner` aísla cryptofeed); aún no existe port/adapter. [VERIFIED/DISEÑADO]
- **cryptofeed → Market Data público:** coherente con el estado real (TRADES/L2_BOOK). cryptofeed **no** participa en órdenes/balance/fills/cancel confirmations. [VERIFIED]
- **Sin contradicciones de SoT:** un SoT por ámbito (exchange=autoridad, OMS=órdenes, Portfolio=patrimonio, REST=reconciliación, WS=eventos, cryptofeed=market data).

## D. B-MD-008 — Cancelación sin Private WS

`CANCELLING` **debe existir incluso sin Private WebSocket**: es el estado transitorio que expresa "cancel solicitado, no confirmado" y evita decretar `CANCELLED` ciego. [VERIFIED — ADR-0029 Decisión §3; el exchange no ofrece `PendingCancel` en v5, `ccxt/bybit.py` `parse_order_status`]

- **Cancel llega correctamente:** REST `cancel_order` ack → `fetch_order` confirma `Cancelled` → `CANCELLED`. [VERIFIED]
- **Orden ya filled:** `fetch_order` → `Filled` → `FILLED` (fill prevalece). [VERIFIED, docs Bybit doble-Filled]
- **Orden ya cancelled:** cancel duplicado → no-op idempotente + `fetch_state` de verificación. [VERIFIED]
- **Orden no existe:** `110001 OrderNotFound` → `fetch_state` de verificación → `CANCELLED` con alerta o `REJECTED`. [VERIFIED, CCXT `bybit.py:721-729`]
- **Bybit inaccesible / REST falla:** permanece `CANCELLING`; reintento con backoff; **fail-closed** (nunca decreta terminal sin confirmación). [VERIFIED — ADR-0029]
- **Restart durante CANCELLING:** el OMS reconstruye desde REST (`fetch_open_orders`) y resuelve contra el exchange; no da la orden por cerrada por defecto. [DISEÑADO — requiere `manage_open_orders` + reconciliación, no implementado]
- **Carrera CANCEL/FILL:** REST confirma `Filled` → `FILLED` (el fill gana). [VERIFIED]
- **Duplicados / fuera de orden:** sin WS no existen; con WS, REST es la autoridad que los resuelve. [VERIFIED/INFERENCE]

**Conclusión:** REST + reconciliación resuelve el estado final de forma determinista; el WS solo acelera la observación.

## E. B-MD-009 — Balance

Cadena objetivo (no implementada): `Bybit REST wallet-balance → CCXTAdapter.fetch_balance → BalancePort → Portfolio → RiskManager → Execution`. [VERIFIED/DISEÑADO — ADR-0030]

- **Campo disponible operativo UTA:** `totalAvailableBalance` (USD). [VERIFIED, docs Bybit `/v5/account/wallet-balance`]
- **Diferencias:** `walletBalance`=saldo total; `totalEquity`=patrimonio total (con UPL); `locked`+`totalOrderIM`+`totalPositionIM`=fondos comprometidos; `free`/`availableToWithdraw`=deprecado para UNIFIED; `totalAvailableBalance`=disponible real. [VERIFIED]
- **RiskManager consume:** el disponible operativo derivado de `totalAvailableBalance` (o la derivación por coin) vía `BalancePort` con freshness — **nunca `free` genérico**. [VERIFIED/DISEÑADO]
- **Portfolio conserva:** posiciones (PositionStore BC-43) + saldo materializado (BalanceStore futuro). [VERIFIED/DISEÑADO]
- **Stale / mismatch / respuesta incompleta / restart:** tolerancias configurables; discrepancia material → bloqueo (fail-closed); respuesta incompleta → no operar; restart → gate de saldo + rehidratación desde PositionStore. [VERIFIED — ADR-0030]

## F. cryptofeed — alcance confirmado

Solo canales públicos (`TRADES`, `L2_BOOK`); **no** participa en órdenes, balances, fills ni cancel confirmations; sin dependencia real de cryptofeed para account/private state. **Conclusión VERIFIED:** *cryptofeed NO es responsable del estado privado de cuenta de OCM* — evidenciado por los runners (`bybit_cryptofeed_runner.py:62-66`, `kucoin_cryptofeed_runner.py:51-52`, `cryptofeed_orderbook_stream.py:114-118`) y la ausencia de canales privados en uso.

## G. CCXT estándar vs CCXT Pro

- **CCXT estándar (REST) cubre** `create_order`, `cancel_order`, `fetch_order`, `fetch_open_orders`, `fetch_closed_orders`, `fetch_balance`, `fetch_positions` (`ccxt/bybit.py:4583,4923,5370,5318,3509,6314`). [VERIFIED]
- **CCXT Pro aportaría** WS de observación (órdenes/fills/balance en realtime) — mejora de latencia/observabilidad, **no de corrección** (caché local sin snapshot, requiere reconciliación REST).
- **Respuesta:** NO existe funcionalidad de CCXT Pro que sea NECESARIA para que OCM opere Bybit de forma segura. No recomendarlo solo por tener WebSockets. [VERIFIED]

## H. Private WS de Bybit — ¿obligatorio para LIVE?

**No.** La comparación REST vs REST+WS favorece a REST para la corrección: REST da snapshot/reconciliación/recuperación determinista; el WS es incremental, sin snapshot (order/wallet), puede duplicar/perder eventos y solo detecta out-of-order en `position`. **El WS mejora principalmente latencia/observabilidad, no corrección** → permanece **opcional** y nunca SSOT. [VERIFIED]

## I. Referentes (patrones transferibles, ya verificados)

- **Hummingbot `PENDING_CANCEL` / Nautilus `PendingCancel` / LEAN `CancelPending`:** confirman el estado transitorio de cancelación — patrón transferible: `CANCELLING`. [VERIFIED]
- **vn.py** (sin estado intermedio, reconcilia por query): válido pero menos defensivo; no adoptar como única vía. [VERIFIED]
- **Reconciliación al arranque** (LEAN `BrokerageSetupHandler`+`PerformCashSync`, Hummingbot `restore_tracking_states`, Nautilus reconciliation engine): patrón transferible para `manage_open_orders` + gate de saldo. [VERIFIED]
- **Separación Portfolio/Risk/Execution** (coincide en referentes): OCM ya la tiene (BC-43/BC-12). [VERIFIED]
- **Recuperación tras restart:** reconciliar contra el exchange por REST antes de operar. [VERIFIED]

## J. Matriz final

| Capacidad | OCM actual | Bybit (API) | CCXT estándar | CCXT Pro | cryptofeed | Decisión OCM |
|---|---|---|---|---|---|---|
| Market data | cryptofeed TRADES/L2 | v5 public | REST fetch | WS | **sí** | cryptofeed (VERIFIED) |
| Create order | CCXT REST | `/v5/order/create` | **sí** | sí | no | CCXT REST (VERIFIED) |
| Cancel order | ausente | `/v5/order/cancel` | **sí** (REST) | sí | no | CCXT REST (pendiente) |
| Order status | CCXT REST | `/v5/order` | **sí** | sí | no | CCXT REST (VERIFIED) |
| Open orders | ausente | `/v5/order/realtime` | **sí** | sí | no | CCXT REST (pendiente) |
| Balance | ausente | `/v5/account/wallet-balance` UNIFIED | **sí** (`info.totalAvailableBalance`) | sí | no | CCXT REST + BalancePort (pendiente) |
| Positions | ausente | `/v5/position/list` | **sí** | sí | no | CCXT REST (opcional F3) |
| Private order events | ausente | v5/private `order` | no | sí | no (v3 legacy) | opcional: Bybit private WS observer |
| Private wallet events | ausente | v5/private `wallet` | no | sí | no | opcional: Bybit private WS observer |
| Reconciliation | parcial (post-submit) | REST | **sí** | sí | no | REST autoridad (pendiente) |

## K. Decisión final (respuestas exactas)

1. **¿Necesita OCM CCXT Pro? NO.** Comercial/de pago; toda la funcionalidad de account está en CCXT estándar REST (MIT) + APIs oficiales Bybit (gratis). No es necesario para corrección.
2. **¿Necesita OCM cryptofeed? SÍ, exclusivamente para market data público** (trades y order book L2 en vivo).
3. **¿Debe OCM usar REST como autoridad? SÍ.** Cada consulta REST es estado real y recuperable; fail-closed; idempotente; sin dependencia de snapshot WS.
4. **¿Debe existir `CANCELLING` sin Private WS? SÍ.** Es un estado de dominio que expresa "cancel no confirmado" y no depende del WS (Bybit v5 no expone `PendingCancel`).
5. **¿Debe Portfolio ser SSOT patrimonial? SÍ.** Ya es dueño de posiciones (BC-43); el saldo es la otra mitad del estado patrimonial (ADR-0030).
6. **¿Debe RiskManager consumir BalancePort? SÍ.** Diseñado (ADR-0030); no implementado; evita segunda fuente de verdad.
7. **¿Debe Private WS ser obligatorio para LIVE? NO.** Mejora latencia/observabilidad, no corrección; REST+reconciliación ya da corrección.
8. **¿Qué está realmente implementado hoy?** create_order, fetch_order, OrderTransport (sin cancel), OMS, PortfolioService, RiskManager, LiveExecutor (con `_reconcile` fail-closed post-submit), composition roots, _BybitTransport/PaperTransport, CCXTAdapter (REST sin cancel/balance).
9. **¿Qué queda pendiente?** cancel_order, fetch_balance, fetch_open_orders, BalancePort, CANCELLING, manage_open_orders, gate de saldo al arranque, loops de reconciliación (órdenes+balance), (opcional) private WS observer.
10. **¿Qué implementar primero?** Fase 1: cancel_order + OrderTransport.cancel + CANCELLING; fetch_balance + BalancePort + totalAvailableBalance. Fase 2: fetch_open_orders + manage_open_orders + gate de saldo + reconciliación. Fase 3 (opcional): private WS observer.

## L. ADR-0029 — CONFIRMADA CON ACLARACIÓN (documental)

El núcleo (CANCELLING, fill prevalece, fail-closed, idempotencia) queda confirmado. Aclaración documental recomendada (sin modificar): (1) **REST es la autoridad** de confirmación; (2) Private WS es **opcional** (observador, nunca SoT); (3) **cryptofeed queda fuera del dominio de órdenes privadas**.

## M. ADR-0030 — CONFIRMADA CON ACLARACIÓN (documental)

Ya exige `totalAvailableBalance` y REST como fuente. Aclaración documental recomendada (sin modificar): (1) **REST wallet-balance es la autoridad**; (2) **WS wallet es solo incremental, sin snapshot, nunca fuente**; (3) **Portfolio sigue siendo SSOT patrimonial**.

## N. Roadmap

- **Fase 1 (imprescindible LIVE):** `cancel_order` (REST) + `OrderTransport.cancel` + `CANCELLING`; `fetch_balance` (UNIFIED) + `BalancePort` + `totalAvailableBalance`. Seguridad de cancelación y saldo.
- **Fase 2 (robustez):** `fetch_open_orders` + `manage_open_orders`; gate de saldo al arranque; reconciliación periódica de saldo y órdenes; resync tras error/desconexión; métricas de reconciliación.
- **Fase 3 (optimización, opcional):** Bybit Private WS nativo como observador (fills/wallet realtime). No es requisito LIVE.

## O. UNKNOWN restantes

- Reconexión real de cryptofeed bajo red adversa (no probado en runtime OCM).
- Comportamiento de sesión v5 `order`/`wallet` con `max_active_time` cortos.
- Latencia medida de REST polling vs WS privado en OCM.
- Vida útil del endpoint `realtime_private` v3 de cryptofeed (irrelevante para OCM).
- Detalle de parseo de `execution.fast` v5.

## P. RECOMENDACIÓN FINAL

**La arquitectura mínima, segura, recuperable y económicamente razonable para que OCM opere Bybit en LIVE es: cryptofeed (MIT) para market data público; CCXT estándar (MIT) REST como autoridad para crear/cancelar/consultar órdenes, saldo y reconciliación; OMS como SSOT de órdenes; Portfolio como SSOT patrimonial; RiskManager consumiendo el disponible operativo (`totalAvailableBalance`) vía BalancePort; sin CCXT Pro; y el Private WS nativo de Bybit como observador opcional de fase posterior, nunca como SSOT.** Todo lo necesario para LIVE se implementa con REST + reconciliación, coste de licencias cero y recuperación determinista ante fallos de red/REST.
