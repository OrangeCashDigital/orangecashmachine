# ADR-0029: Cancelación real de órdenes y resolución determinista CANCEL/FILL (B-MD-008)

> **ESTADO: ACEPTADA** — decisión aprobada por el owner el 2026-08-16. NO implementada todavía: los contratos cambian solo cuando la implementación (tracking B-MD-008, cadena `implementacion`) se ejecute y los gates pasen.
> **Corresponde a:** A-MD-004 (etiqueta interna en tracking.yaml, B-MD-008), diseño conceptual en `docs/audits/2026-08-15-b-md-008-009-diseno-conceptual.md` §3.

**Estado:** Aceptado
**Fecha:** 2026-08-15
**Bounded context(s) afectado(s):** trading (execution), market_data (adapter CCXT)

## Contexto

`OMS.cancel()` es **local-only** (VERIFIED): `packages/trading/execution/oms.py:300-317` transiciona a `CANCELLED`, hace `_open.pop` y `record_close` sin llamar a ningún transporte/executor/exchange. `OrderTransport` (Protocol) solo define `submit`/`fetch_state`/`close` (`packages/trading/execution/transport.py:96-128`); `PaperTransport` tampoco tiene cancel (`transport.py:131-158`). El adapter real `_BybitTransport` solo implementa `submit`/`fetch_state`/`close` (`packages/trading/bootstrap/composition_root.py:203-262`). `CCXTAdapter` no expone `cancel_order` (`packages/market_data/adapters/outbound/exchange/ccxt_adapter.py:405` create_order, `:462` fetch_order). Además **`OMS.cancel()` nunca se invoca** en el repo (grep F-BMD8-04, VERIFIED): no hay gestión de órdenes abiertas.

**Carrera CANCEL vs FILL (VERIFIED + INFERENCE):** `CANCELLED` es terminal en el grafo `_VALID_TRANSITIONS` (`packages/trading/execution/order.py:64-74`) → `transition(FILLED)` desde CANCELLED lanzaría `ValueError`. `fill_sync.on_fill_composite` solo corre vía `on_fill` (FILLED) (`packages/trading/execution/fill_sync.py:109`). Si una orden se cancela localmente y el exchange la ejecuta en la ventana, el fill se pierde en silencio: OCM queda `CANCELLED` + posición real en exchange → divergencia sin error.

**Mitigaciones existentes (VERIFIED):** solo market orders (`composition_root.py:238`, `order_type="market"`); idempotencia por `client_order_id` (`live_executor.py:174`); `LiveExecutor._reconcile` fail-closed solo durante submit (`live_executor.py:229-261`). Reducen la ventana, no la eliminan.

**Hechos del exchange (OFFICIAL, investigación 2026-08-15):**
- **Market order Bybit = IOC**: la orden de mercado se convierte en IOC limit; si no hay libro dentro del slippage, no se ejecuta; liquidez insuficiente → cancelada. El acuse de create es **asíncrono** (el estado real se confirma por WS/fetch).
- **Cancel es asíncrono**: la respuesta del cancel es un ack; el estado final (cancelado vs ejecutado) se confirma por el WS de órdenes o fetch. Si la orden ya se ejecutó, el cancel devuelve error de "too late to cancel" (`EC_TooLateToCancel`) o "no se puede cancelar" (`EC_PerCancelRequest`), y el **fill prevalece**.
- **Carrera CANCEL/FILL documentada por Bybit**: doble mensaje `orderStatus=Filled` (uno `EC_NoError`, otro `cancelType=CancelByUser, rejectReason=EC_OrigClOrdIDDoesNotExist`) — el fill siempre gana.
- Errores de cancel: `110001` OrderNotFound, `110008` completed/cancelled, `110010` already cancelled (spot: `170139` filled, `170142` cancelled, `170145` cancel no soportada, `170147` timeout).
- CCXT `cancel_order` (`bybit.py:4611`, fuentes descargadas): requiere `symbol`; parsea solo `orderId`/`orderLinkId` (sin status final) → el resultado real se obtiene por `fetch_order`/WS. Errores CCXT mapeados: `110001`→OrderNotFound, `110008`/`110010`→InvalidOrder (`bybit.py:721-729`).
- **REST es la autoridad recuperable** para determinar el estado real de las órdenes: `cancel_order` + `fetch_order`/`fetch_open_orders`. El WebSocket privado NO sustituye esta autoridad. REST no elimina latencia ni incertidumbre: si el estado todavía no puede determinarse, se conserva el estado transitorio correspondiente (`CANCELLING`, o UNKNOWN cuando proceda), conforme a este diseño.

## Alternativas evaluadas

1. **Cancel local sin exchange (estado actual).** Costo: pérdida de control sobre órdenes en vuelo; divergencia CANCEL/FILL silenciosa. Rechazada — es el problema que este ADR resuelve.
2. **`CANCELLED` terminal + reabrir el grafo permitiendo `CANCELLED → FILLED` solo por reconciliación.** Ventaja: mínimo cambio en transiciones. Costo-riesgo: debilita la invariante de terminalidad (`order.py:64-74`); exige distinguir en runtime el origen manual del de reconciliación; más fácil de romper por error. **No elegida como mecánica principal** (ver alternativa 3), aunque es compatible si se quiere admitir `CANCELLED→FILLED` post-reconciliación.
3. **Estado transitorio `CANCELLING` con resolución determinista. ELEGIDA.** El estado local nunca decreta `CANCELLED` sin confirmación del exchange. `CANCELLING → FILLED | CANCELLED | REJECTED` (o permanece CANCELLING en timeout/error). Preserva el grafo terminal sin relajar terminales: FILLED y CANCELLED definitivos quedan terminales; la resolución ocurre SIEMPRE antes de llegar a ellos.
4. **Solo `manage_open_orders` sin estado transitorio.** Ventaja: monitoriza. Costo: sin estado intermedio, un cancel con respuesta perdida no puede distinguirse de uno confirmado → sigue habiendo divergencia. Rechazada como solución única; es complementaria (ver Decisión).

## Decisión

1. **Port:** extender `OrderTransport` (`packages/trading/execution/transport.py`) con `cancel(symbol, exchange_order_id) -> OrderState` (SafeOps: nunca lanza; ERROR si no se puede confirmar). Es extensión de port existente, no port nuevo.
2. **Adapter:** `_BybitTransport.cancel` (`composition_root.py`) → `CCXTAdapter.cancel_order` (nuevo método en `ccxt_adapter.py`, con `symbol` obligatorio). `PaperTransport.cancel` → retorna `CANCELLED` confirmado (sin I/O).
3. **Máquina de estados (dominio, `order.py`):** añadir `CANCELLING` como estado transitorio con las transiciones:

   ```
   PENDING → SUBMITTED → FILLED
                      ↘ REJECTED
                      ↘ CANCELLING → CANCELLED   (confirmado por exchange)
                                   → FILLED       (fill prevalece — vía OMS._fill)
                                   → REJECTED     (cancel rechazado / not found concluyente)
   ```
   - `CANCELLED` y `FILLED` siguen siendo terminales; **no se relaja el grafo de terminales**.
   - Si un fill real llega durante `CANCELLING` → se aplica el flujo `OMS._fill` existente (WAC, settlement, fill_sync, portfolio) y la orden termina en `FILLED`. **El fill SIEMPRE prevalece** (regla explícita de política, no comportamiento accidental).
4. **Regla CANCEL vs FILL (determinista e idempotente):**
   - `request_cancel(order_id)` → transición a `CANCELLING` (solo desde SUBMITTED; no-op si terminal o inexistente — idempotente).
   - Resultado del exchange:
     - Cancel confirmado → `CANCELLED` definitivo; revertir posición HELD (`record_close`, ya implementado en `oms.py:315`).
     - Orden ya ejecutada (FILLED completo) → `FILLED` con fill real vía `_fill` (WAC/settlement/portfolio ya funcionan).
     - Parcialmente ejecutada → `FILLED` parcial + `remaining`; log; cancel del resto.
     - `110001`/NOT_FOUND → `fetch_state` de verificación antes de decidir; si no aparece → `CANCELLED` con alerta.
     - Timeout/error/desconexión → permanece `CANCELLING`; reintento con backoff; luego `fetch_state` de verificación; **no dar por cerrado sin confirmación** (fail-closed).
   - Cancelación duplicada (segunda petición sobre CANCELLING/CANCELLED): no-op + reconciliación por `fetch_state` (idempotencia).
5. **Loop `manage_open_orders`:** nuevo caller (concepto Freqtrade, patrón solo) que recorre órdenes `SUBMITTED`/`CANCELLING` y reconcilia contra `fetch_state`/WS. Hoy no existe; el diseño lo asume como el único caller real de cancel.
6. **Notificación:** se reutiliza el patrón de callbacks existente (`on_fill`/`on_reject` → `fill_sync` → TradeTracker/PortfolioService). **No se introduce Kafka en trading en esta fase** (hoy trading es 100% síncrono/callback, VERIFIED). `OrderCancelled` como evento de dominio se alinea cuando exista event bus (mismo patrón que `market_data/domain/events/`), no es prerrequisito.
7. **Composition root:** `TradingCompositionRoot.assemble_live` inyecta el transporte con cancel al `LiveExecutor`/`OMS` (único punto de ensamblado, BC-50/ADR-0003).

## Justificación técnica

- **El fill prevalece sobre el cancel** es la regla segura: el fill es un hecho del exchange con consecuencia económica directa (posición/P&L); el cancel es una intención. Bybit lo confirma oficialmente (el fill vence al cancel en la carrera; los códigos `EC_TooLateToCancel`/`EC_PerCancelRequest` indican que la cancelación llegó tarde). OCM la hace política explícita en el dominio, no comportamiento del adapter.
- **`CANCELLING` transitorio** evita relajar la terminalidad del grafo (`order.py:64-74`) y hace imposible "CANCELLED ciego": nunca se decreta cancel sin confirmación del exchange; en timeout/error la orden sigue viva en estado transitorio y visible para el loop de gestión.
- **Port extendido + adapter en composition root** respeta la dirección de dependencias: trading/execution define el contrato (framework-agnostic, sin ccxt — `transport.py` docstring, BC-50); el adapter CCXT vive en el único punto autorizado a tocar market_data.
- **`manage_open_orders`** cierra el gap G2 (audit): sin él, una orden abierta huérfana (market order rara vez, posible con fallos de red) nunca se limpia; con él, hay reconciliación periódica de órdenes abiertas además del cancel puntual.
- **Reutiliza la reconciliación de fill existente** (`OMS._fill`, WAC/settlement/ADR-0025) para ALREADY_FILLED — no se duplica lógica de asentamiento (SSOT `OMS._fill`, ADR-0027).
- **Veredicto LIVE:** bloqueante P1 (audit §16). Sin cancel real no hay control sobre órdenes en vuelo. Este ADR es prerrequisito (junto a B-MD-009) para operar capital real.

## Consecuencias

- **Más fácil:** control real sobre órdenes; divergencia CANCEL/FILL eliminada por diseño (resolución determinista); base para stop-loss/emergencia reales; las órdenes abiertas huérfanas se limpian por loop.
- **Deuda aceptada:** el ack asíncrono de cancel de Bybit añade un ciclo de verificación (fetch/WS) — latencia de resolución no nula; `reject_reason` sigue siendo string libre (`order.py:121`, G5 del audit — fuera de alcance); la reconciliación periódica depende del adapter `fetch_order` (últimas 500 órdenes UTA).
- **Contratos BC-NN que lo hacen cumplir:**
  - `BC-50` — trading no importa market_data fuera del composition root (el adapter vive en bootstrap).
  - `BC-09` — domain sigue framework-agnostic (CCXT queda en adapters).
  - `BC-13`/`BC-43` — el asentamiento a portfolio sigue por el puente existente (fill_sync + inyección); portfolio sigue siendo dueño de posiciones.
  - `BC-35` — si se añade `OrderCancelled` wire, va en `shared/kafka/schemas/` (solo si se introduce event bus; no es prerrequisito).
- **Riesgo residual:** una orden en `CANCELLING` con exchange no disponible permanece en estado transitorio (visible, no huérfana, pero sin resolución). Se mitiga con reintentos + alerta + (opcional) kill switch vía `ExecutionGuard`.

## Análisis market-IOC

Bybit market order = IOC: la ventana de cancelación real es mínima (la orden se llena o muere casi instantáneamente; si no hay libro dentro del slippage no se ejecuta, y si hay liquidez insuficiente el exchange la cancela). Sin embargo el diseño sigue siendo obligatorio porque la ventana **no es cero** en presencia de: latencia de red, reintentos con backoff (`live_executor.py:178-203`), doble envío, o acuse asíncrono. La regla `CANCELLING → FILLED` cubre exactamente el caso "creí que cancelaba pero el exchange ya la llenó". Este ADR es válido también si en el futuro se introducen limit orders (B-MD-004/market validity), donde la ventana de cancelación es estructuralmente mayor.

## Fallos de comunicación

| Escenario | Estado | Acción |
|---|---|---|
| Respuesta de cancel perdida | `CANCELLING` | Reintento con backoff (idempotente); luego `fetch_state` de verificación; nunca `CANCELLED` sin confirmación |
| WS desconectado | `CANCELLING`/SUBMITTED | `manage_open_orders` reconcilia por fetch en reconexión; alerta |
| REST falló (timeout/5xx/429) | `CANCELLING` | Reintento acotado por `max_retries`; si se agota → alerta + estado transitorio visible; no inventar estado final |
| Evento duplicado (doble fill WS) | `FILLED` | Idempotencia: `transition(FILLED)` desde FILLED es no-op controlado; `_fill` ya maneja `_open.pop` |
| Cancel sobre orden ya FILLED | `FILLED` | El cancel devuelve `EC_TooLateToCancel`/`110008`; OCM aplica el fill real (prevalece) |
| **Restart/reinicio durante `CANCELLING`** | (reconstruido desde exchange) | En la recuperación de arranque, reconstruir el estado local consultando el estado **recuperable** del exchange mediante `fetch_open_orders` y, cuando aplique al flujo de este ADR, `fetch_order`. No declarar `CANCELLED` sin confirmación recuperable; el propósito es reconstruir el estado local a partir del estado recuperable del exchange tras el reinicio, sin inventar una nueva máquina de estados |

> **Aclaración (auditoría de consistencia 2026-08-15):** el WebSocket privado de Bybit es **opcional y futuro** en OCM; su rol es incremental (latencia/observabilidad), no de fuente de verdad. No sustituye la confirmación/reconciliación REST, no debe declarar unilateralmente un estado final, y no es requisito para LIVE.

## Relación con BookBuilder / market data

- **Independiente de la cadena B-MD-003→002→004** (VERIFIED, audit §8): el cancel no necesita order book ni market validity.
- Se relaciona con B-MD-004 solo en el momento de la orden: B-MD-004 valida el mercado pre-envío; B-MD-008 da control post-envío. Capas complementarias, sin dependencia forzada.
- **No se fuerza ninguna dependencia artificial** con BookBuilder (ADR-0028).
- **`cryptofeed` se usa exclusivamente para market data público** (TRADES/L2_BOOK, VERIFIED) y queda separado de las órdenes privadas, cancelaciones, fills de cuenta, wallet/balance y estado privado de la cuenta. No es un mecanismo de confirmación de órdenes privadas ni de estado patrimonial.

## Roadmap

- **Fase 3 (Business Rules / Trading)**, sin mover trabajo de Fase 1/2 (VERIFIED, audit §15). Pasos de infraestructura de soporte (adapter CCXT) se ejecutan dentro de Fase 3.
- Orden sugerido (conceptual): verificar capacidad de cancel Bybit en sandbox → exponer `cancel_order` en CCXTAdapter → extender `OrderTransport` → estado `CANCELLING` + resolución en OMS → loop `manage_open_orders` → tests.
- **Bloqueante de live** (P1), junto a B-MD-009, B-MD-001, B-MD-004.

## Security scenarios

| Escenario | Garantía |
|---|---|
| A. Orden enviada por error | `request_cancel` la detiene en el exchange; si ya llenó, se aplica el fill real (no divergencia) |
| B. Carrera CANCEL/FILL | Resolución determinista: el fill prevalece; CANCELLING nunca decreta CANCELLED sin confirmación |
| C. Exchange caído durante cancel | Orden permanece CANCELLING (visible), nunca falsamente CANCELLED; reintento + alerta; kill switch disponible |
| D. Cancel duplicado / evento duplicado | Idempotente; reconciliación por `fetch_state`; sin estados inconsistentes |

## ¿Por qué ADR separadas para B-MD-008 y B-MD-009?

Se recomienda **mantenerlas separadas** (ADR-0029 y ADR-0030), no combinarlas:

1. **Concern distinto:** B-MD-008 es **control de órdenes** (estado de ejecución, máquina de estados CANCEL/FILL); B-MD-009 es **estado patrimonial** (saldo/posiciones, reconciliación). Un ADR conjunto mezclaría dos ciclos de vida (orden vs portfolio) con ciclos de decisión diferentes.
2. **Dueños de BC distintos:** B-MD-008 vive en `trading` (execution); B-MD-009 vive en `portfolio` (patrimonio) con `trading/risk` como consumidor. Combinar exigiría decidir la frontera BC en un solo documento.
3. **Gates de aceptación independientes:** cada uno tiene tests, adapter CCXT y riesgos propios; uno puede aprobarse sin bloquear al otro (ambos son independientes entre sí, VERIFIED).
4. **Roadmap independiente:** aunque ambos son F3 y bloqueantes de live, se implementan en paralelo y comparten solo el paso de infraestructura (exponer métodos en CCXTAdapter), que no justifica fusionar los ADRs.

Si el owner prefiriera una sola ADR, el costo sería: menor granularidad de decisión, acoplamiento innecesario entre ejecución y patrimonio, y dificultad para reabrir/rechazar uno sin arrastrar el otro. **Recomendación: mantener separadas.**

## Decision Summary

| Campo | Valor |
|---|---|
| Problema | `OMS.cancel()` local-only y sin callers (`oms.py:300`); sin port/adapter de cancel; carrera CANCEL/FILL silenciosa (`CANCELLED` terminal) |
| Solución propuesta | Estado transitorio `CANCELLING` + resolución determinista (fill prevalece); port `OrderTransport.cancel`; adapter `CCXTAdapter.cancel_order`; loop `manage_open_orders` |
| BC responsable | trading (execution) — dominio/port/loop; market_data (adapter CCXT en composition root) |
| Bloquea LIVE | Sí (P1) — sin cancel real no hay control sobre órdenes en vuelo |
| Fase | F3 |
| ADR | ADR-0029 (esta ADR; etiqueta A-MD-004 en tracking.yaml) |

## Implementation Roadmap (conceptual — no implementado)

1. **Sandbox/verificación Bybit (S):** confirmar comportamiento de `cancel_order` de CCXT para market orders (IOC) y el mapeo de errores. Evidencia de resolución: test de integración sandbox cancel → confirmación exchange → estado final.
2. **Adapter (`market_data`, S):** exponer `cancel_order(symbol, order_id, params)` en `CCXTAdapter` (+ `fetch_balance`, compartido con ADR-0030). Evidencia: test del adapter.
3. **Port (`trading/execution`, S):** `OrderTransport.cancel(symbol, exchange_order_id) -> OrderState`; implementar en `_BybitTransport` y `PaperTransport`. Evidencia: `tests/trading/test_transport_mapping.py` extendido.
4. **Dominio (`order.py`/`oms.py`, M):** estado `CANCELLING` + transiciones + `request_cancel` + resolución CANCEL/FILL reutilizando `OMS._fill`. Evidencia: test carrera cancel vs fill (fill prevalece), test cancel confirmado → CANCELLED + revert posición.
5. **Loop `manage_open_orders` (M):** recorrer SUBMITTED/CANCELLING, reconciliar por `fetch_state`. Evidencia: test de orden huérfana detectada/limpiada.
6. **Composition root:** inyectar transporte con cancel en `assemble_live`. Evidencia: test de wiring.
7. **Tests/CI:** los puntos 2-6 en `pytest`; ruff + import-linter como gate.

## Referencias

- Código: `packages/trading/execution/oms.py:300-317`, `transport.py:96-128,131-158`, `order.py:64-74`, `live_executor.py:161-261`, `fill_sync.py:109-167`, `packages/trading/bootstrap/composition_root.py:203-262`, `packages/market_data/adapters/outbound/exchange/ccxt_adapter.py:405,462`, `ocm/runtime/guard.py`.
- Docs: `docs/audits/2026-08-15-b-md-008-cancel-b-md-009-balance-audit.md` (F-BMD8-01..05), `docs/audits/2026-08-15-b-md-008-009-diseno-conceptual.md` §3, `docs/plans/tracking.yaml` B-MD-008 (no editado).
- ADRs relacionados: ADR-0016 (reconciliación de fills), ADR-0003 (CR trading), ADR-0025 (WAC/cost basis), ADR-0027 (recovery/SSOT), ADR-0026 (fees). Complementa: ADR-0030 (B-MD-009).
- Doc oficial Bybit: `/v5/order/cancel` (cancel async; errores 110001/110008/110010/1701xx), `/v5/order/realtime`, WS private/order (doble-Filled, `EC_OrigClOrdIDDoesNotExist`, `EC_TooLateToCancel`, `EC_PerCancelRequest`), market order = IOC.
- CCXT (fuente descargada, `/tmp/opencode/ccxt-bybit/bybit.py`): `cancel_order` :4611, `cancel_order_request` :4590, errores mapeados :721-729.
