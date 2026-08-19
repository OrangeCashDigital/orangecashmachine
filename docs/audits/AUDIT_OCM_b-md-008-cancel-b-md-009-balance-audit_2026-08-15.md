# Auditoría — B-MD-008 (OMS.cancel local-only) y B-MD-009 (sin fetch_balance/reconciliación de saldo)

**Fecha:** 2026-08-15
**Auditor:** OpenAI Codex (asistido por agente de exploración)
**Rol:** Arquitecto Principal de Software · Trading Systems Reviewer
**Alcance:** `packages/trading` (OMS, execution, transport, fill_sync, risk, settlement, analytics, engine, composition root) + `packages/portfolio` (services, models, ports, composition root) + `packages/market_data/adapters/outbound/exchange/ccxt_adapter.py` + `shared/contracts/boundaries.py` + `apps/app/cli/live_hydra.py` + `apps/app/use_cases/execute_live.py` + `ocm/runtime/guard.py` + tests.
**Método:** evidencia directa de código (verificada en esta sesión, con `archivo:línea`) + referencia conceptual Freqtrade (`docs/freqtrade-develop.zip`) y Hummingbot (`docs/hummingbot-master.zip`) **sin copiar arquitectura ni código** (GPL).
**Regla de honestidad:** todo hallazgo se marca **VERIFIED** (confirmado en código/evidencia directa vista en esta sesión), **UNVERIFIED** (pendiente de comprobar) o **INFERENCE** (deducción razonable, no demostrada). Las conclusiones de auditorías anteriores (2026-08-14/15) se trataron como **hipótesis y se re-verificaron** contra el código actual. Se distingue **HECHO observado** / **INFERENCIA técnica** / **RECOMENDACIÓN de diseño**.
**Restricciones respetadas:** sin código de producción; `tracking.yaml` NO modificado; sin ports/adapters/composition roots nuevos; sin ADRs aprobadas cambiadas; sin commits/push. Único archivo creado: este informe.

---

## 1. Resumen ejecutivo (para persona no experta)

OCM es un sistema de trading. Cuando envía una orden al exchange (mercado real o simulado), debe saber siempre qué le pasó a esa orden y cuánto dinero tiene realmente disponible. Esta auditoría verifica dos sospechas:

1. **Cancelar una orden no la cancela de verdad (B-MD-008).** Cuando OCM "cancela" una orden, solo cambia un estado interno en memoria a `CANCELLED`. **No se comunica con el exchange.** Verificado: `OMS.cancel()` (`packages/trading/execution/oms.py:300`) no usa ningún transporte; el protocolo `OrderTransport` (`transport.py`) no tiene método de cancelación; el adapter CCXT (`ccxt_adapter.py`) no expone `cancel_order`. **Además, `OMS.cancel()` no se llama desde ningún sitio del código** — existe pero nadie lo usa.
2. **OCM no conoce su saldo real (B-MD-009).** No existe `fetch_balance`/reconciliación de saldo en todo el repositorio. OCM valida el riesgo contra un `capital_usd` **configurado** (número fijo), no contra el saldo real del exchange. Verificado: grep exhaustivo de `balance`/`fetch_balance`/`saldo` = sin mecanismo real (solo "rebalance" — falso positivo).
3. **Carrera CANCEL vs FILL:** si OCM marca una orden como CANCELLED y el exchange la ejecuta, **OCM puede quedar creyendo que está cancelada mientras el exchange la ejecutó** — divergencia de estado. El camino exacto se demuestra en §3. La mitigación parcial actual es que el transporte solo emite **market orders** (llenado inmediato), lo que reduce la ventana, pero no la elimina.

**Veredicto Live-Readiness: NO.** Operar con capital real sin cancelación real ni conocimiento del saldo real es un riesgo de seguridad material. Paper trading sigue siendo seguro.

---

## 2. Registro de hallazgos

### [F-BMD8-01] `OMS.cancel()` es local-only — no llega al exchange
- **Severidad:** P1 (live readiness / pérdida de control sobre órdenes)
- **Estado:** VERIFIED (HECHO observado)
- **Evidencia:** `packages/trading/execution/oms.py:300-317` — `cancel(order_id)` con `self._lock`: obtiene la order del dict `_orders`, valida `is_terminal`, llama `order.transition(OrderStatus.CANCELLED)`, `self._open.pop(order_id)` y `self._risk.record_close(pnl_usd=None)`. **Ninguna llamada a transporte, executor, CCXT o exchange.** No genera eventos. No persiste nada más allá del dict en memoria.
- **Flujo real (falta la última capa):** `caller → OMS.cancel() → [dict interno]` — **no existe** `ports → adapter/transport → CCXT/exchange`. La capa transport no se alcanza.

### [F-BMD8-02] El protocolo `OrderTransport` no tiene método de cancelación
- **Severidad:** P1 (estructural)
- **Estado:** VERIFIED (HECHO observado)
- **Evidencia:** `packages/trading/execution/transport.py:96-128` — `OrderTransport` Protocol define solo `submit()`, `fetch_state()`, `close()`. No hay `cancel()`/`cancel_order()`. `PaperTransport` (transport.py:131-158) tampoco.
- **Corrección a conclusión previa:** la auditoría 2026-08-15 decía "el transporte no tiene cancel" como inferencia. **Verificado directamente**: correcto, y además el adapter real `_BybitTransport` (`bootstrap/composition_root.py:203-262`) solo implementa `submit`/`fetch_state`/`close`.

### [F-BMD8-03] CCXTAdapter no expone `cancel_order`
- **Severidad:** P1
- **Estado:** VERIFIED (HECHO observado)
- **Evidencia:** `packages/market_data/adapters/outbound/exchange/ccxt_adapter.py` — métodos de I/O: `fetch_ticker` (:259), `fetch_ohlcv` (:283), `fetch_trades` (:343), `load_markets` (:372), `create_order` (:405), `fetch_order` (:462). **No existe `cancel_order` ni `fetch_balance`.** El cliente CCXT subyacente (`ccxt.Exchange`) sí los ofrece, pero el adapter no los expone.
- **Nota:** CCXT sí soporta `cancel_order`/`fetch_balance` en el cliente subyacente — la capacidad existe a nivel de librería, falta exponerla. (INFERENCIA técnica: el cliente `ccxt.Exchange` expone ambos métodos; verificado en la librería, no en uso.)

### [F-BMD8-04] `OMS.cancel()` nunca es invocado en todo el código
- **Severidad:** P2 (dead code / ausencia de función de control)
- **Estado:** VERIFIED (HECHO observado)
- **Evidencia:** grep de `\.cancel\(` sobre ordenes/OMS en `packages/`, `apps/`, `tests/`: las únicas coincidencias son `task.cancel()`/`w.cancel()` (cancelación de asyncio tasks en market_data) y `map_ccxt_order` mapeando `"canceled"→CANCELLED`. **Ninguna llamada a `OMS.cancel()`.** No existe gestión de órdenes abiertas (equivalente a `manage_open_orders` de Freqtrade).
- **Implicación (INFERENCIA):** además de ser local-only, la cancelación ni siquiera se usa. El gap de control es doble: no hay función de cancelación real Y no hay nada que la invoque.

### [F-BMD8-05] Carrera CANCEL vs FILL — camino de divergencia demostrable
- **Severidad:** P1 (estado local divergente del exchange)
- **Estado:** VERIFIED (HECHO observado + INFERENCIA sobre el camino)
- **Evidencia del camino:**
  1. `OMS.submit` (`oms.py:185`) → `order.transition(SUBMITTED)` (:271) → `self._executor.execute(order)` (:283). Con `LiveExecutor` + `_BybitTransport` emite market order (`composition_root.py:238`).
  2. `LiveExecutor._submit` (`live_executor.py:161-227`): reintentos con backoff, `_reconcile` fail-closed (:229-261). Si no confirma FILLED → `OrderResult(accepted=False)` → OMS `_reject` (oms.py:296) → REJECTED. **Esta es la única vía de reconciliación post-submit.**
  3. Si algo marca `CANCELLED` (via OMS.cancel, hoy no invocado), `order.py:73` `CANCELLED` es terminal (`_VALID_TRANSITIONS[CANCELLED] = set()`) → **un fill posterior no puede aplicarse**: `transition(FILLED)` lanzaría `ValueError`.
  4. `fill_sync.on_fill_composite` (`fill_sync.py:109`) solo se dispara por `on_fill` (FILLED). Una orden CANCELLED localmente pero FILLED en exchange: TradeTracker/PortfolioService no reciben el fill, `OMS._entry_positions`/Risk `_positions` no se actualizan → **la posición real del exchange queda fuera del estado OCM**.
  - **Conclusión (INFERENCIA fundamentada):** si una orden abierta en el exchange es cancelada localmente y el exchange la ejecuta en la ventana, OCM queda con `CANCELLED` local + posición real en el exchange → divergencia silenciosa. Mitigación parcial actual: solo market orders (fill inmediato) reduce la ventana; `LiveExecutor._reconcile` fail-closed (no marca fill sin confirmación) reduce el riesgo inverso (creer filled sin serlo). **No elimina la divergencia.**
- **Relación:** no existe reconciliación periódica de órdenes abiertas (B-MD-008 está en Fase 3; Freqtrade lo resuelve con `manage_open_orders` + `update_trade_state`, ver §8).

### [F-BMD9-01] No existe `fetch_balance`/mecanismo de saldo en todo el repo
- **Severidad:** P1 (live readiness / decisiones basadas en información incorrecta)
- **Estado:** VERIFIED (HECHO observado)
- **Evidencia:** grep case-insensitive de `balance|saldo|fetch_balance|get_balance` en `packages/`, `shared/`, `apps/`, `ocm/`: todas las coincidencias de "balance" son **"rebalance"** (`portfolio/services/rebalance_service.py`, `bootstrap/composition_root.py`) — falso positivo. Sin `fetch_balance`, sin `get_balances`, sin port de balance, sin reconciliation de saldo.
- **Nota a conclusión previa:** la auditoría 2026-08-14 lo marcaba como "grep = ∅". **Re-verificado**: correcto y ampliado (el adapter CCXT tampoco lo expone).

### [F-BMD9-02] RiskManager valida contra `capital_usd` configurado, no contra saldo real
- **Severidad:** P1 (sizing/exposición incorrecta)
- **Estado:** VERIFIED (HECHO observado)
- **Evidencia:** `packages/trading/risk/manager.py` — `__init__(config, capital_usd=10_000.0)` (:115), `_capital_usd` fijo. `_validate_internal` usa `self._capital_usd * size_pct` para sizing (:394-401) y drawdown normalizado sobre capital (:374-386). `record_position`/`exposure_usd` acumulan `qty × avg_entry` (cost basis) (:279-289). **Ninguna lectura de saldo del exchange.**
- **INFERENCIA:** con fees no contabilizados, transferencias externas o fills no sincronizados, el saldo real puede diferir del configurado → sizing incorrecto u órdenes rechazadas por insuficiencia.

### [F-BMD9-03] El flujo submit→fill no reconcilia el saldo después de ejecutar
- **Severidad:** P2 (detección tardía de discrepancias)
- **Estado:** VERIFIED (HECHO observado)
- **Evidencia:** `LiveExecutor._submit` (`live_executor.py:161-227`) reconcilia el **fill de la orden** (fetch_state → confirmar FILLED), no el **saldo**. `Settlement` (`settlement.py`) calcula P&L/fees sin consultar balance. `fill_sync` (`fill_sync.py:109-167`) sincroniza posición en PortfolioService desde el fill, nunca desde un balance del exchange.

### [F-BMD9-04] PortfolioService es el SSOT de posiciones (BC-43), pero no conoce el saldo del exchange
- **Severidad:** N/A (base de diseño)
- **Estado:** VERIFIED (HECHO observado)
- **Evidencia:** `packages/portfolio/services/portfolio_service.py` — `open_position`/`close_position`/`snapshot` sobre `PositionStore` (InMemory/Redis). `PortfolioState.capital_usd` es el configurado. No hay balance del exchange. BC-43: PositionStore solo instanciable en portfolio bootstrap (`composition_root.py:13-24`). Los stores: `portfolio/ports/position_store.py`.

### [F-BMD9-05] Hummingbot referencia: balance por connector + `get_balance` (patrón conceptual)
- **Severidad:** N/A (referencia)
- **Estado:** VERIFIED (HECHO observado en fuente externa)
- **Evidencia:** `/tmp/opencode/hummingbot/hummingbot-master/hummingbot/connector/connector_base.pyx` — `self._account_balances = {}` (:49), `self._account_available_balances = {}` (:50), `get_balance(currency)` (:354), `get_available_balance(currency)` (:399). Patrón: el connector mantiene y expone el saldo; las estrategias consultan `get_balance`/`get_available_balance`. GPL — solo se extrae el patrón.

---

## 3. Flujo actual de cancelación (VERIFIED)

```
caller
  │   (ninguno en el repo — OMS.cancel() no se invoca)
  ▼
OMS.cancel(order_id)                     [packages/trading/execution/oms.py:300]
  │   bajo RLock:
  │     _orders.get(order_id) → None o is_terminal → False
  │     order.transition(CANCELLED)      [order.py:310, terminal]
  │     _open.pop(order_id)
  │   si BUY: _risk.record_close(pnl_usd=None)   [oms.py:315]
  ▼
ports/interfaces
  │   NINGUNO — no se toca OrderTransport ni OrderExecutor
  ▼
adapter/transport
  │   NO EXISTE este paso — no hay cancel en _BybitTransport (composition_root.py:203)
  ▼
CCXT/exchange
  │   NO SE ALCANZA
```

**Capas faltantes (señaladas explícitamente):**
- `OrderTransport` no define `cancel` (transport.py:96-128).
- `CCXTAdapter` no expone `cancel_order` (ccxt_adapter.py).
- No existe ningún caller de `OMS.cancel()` (grep §2 F-BMD8-04).
- No existe gestión de órdenes abiertas/abandono (equivalente a Freqtrade `manage_open_orders`).

---

## 4. Flujo actual de balance/posición (VERIFIED)

```
exchange (Bybit)
   ▲                          ✗ NO se consulta saldo (fetch_balance inexistente)
   │
   │  submit (market order) ────► _BybitTransport.submit (composition_root.py:223)
   │                                  └─ CCXTAdapter.create_order (ccxt_adapter.py:405)
   │  fetch_state ──► _BybitTransport.fetch_state (:246) ──► CCXTAdapter.fetch_order (:462)
   │                                     └─ confirma FILLED (fail-closed) [live_executor.py:229]
   ▼
OMS._fill (oms.py:368) ──► fill_sync.on_fill_composite (fill_sync.py:109)
   │                          ├─ TradeTracker.on_fill → TradeRecord (analytics)
   │                          └─ PortfolioService.open/close_position → PositionStore (BC-43)
   ▼
RiskManager._positions (qty × avg_entry = cost basis, manager.py:279)
   ▼
capital_usd configurado (RiskManager/PortfolioService) — NO saldo real
```

**Puntos ciegos:** saldo real, fees acumuladas vs balance, transferencias externas, fills externos al OCM.

---

## 5. Riesgo operativo

| Riesgo | Problema | Evidencia | Severidad |
|---|---|---|---|
| Pérdida de control sobre órdenes | No se puede cancelar/revocar una orden en vuelo | oms.py:300, transport.py:96-128, ccxt_adapter.py | **Alta (P1)** |
| Divergencia de estado | Orden cancelada localmente pero ejecutada en exchange → posición real fuera del estado OCM | oms.py:310, order.py:73, live_executor.py:229 | **Alta (P1)** |
| Exposición superior a la esperada | Sizing contra capital configurado, no real; fees/transferencias no contabilizadas | manager.py:394, portfolio_service.py:63-80 | **Alta (P1)** |
| Decisiones con info incorrecta | Sin saldo real, un drawdown/halt se computa sobre capital supuesto | manager.py:374-386 | **Media (P1)** |
| Sin gestión de órdenes abiertas | Nadie monitoriza/limpia órdenes pendientes (no hay manage_open_orders) | grep de `OMS.cancel()`/open-order | **Media (P2)** |

---

## 6. Mitigaciones existentes (VERIFIED)

1. **Fail-closed en reconciliación de fills:** `LiveExecutor._reconcile` (`live_executor.py:229-261`) solo da por FILLED si `fetch_state` lo confirma; timeout/error → rechazo. Evita "creer llenada una orden que no lo está" — el riesgo inverso.
2. **Solo market orders:** `_BybitTransport.submit` usa `order_type="market"` (`composition_root.py:238`) → fill inmediato → la ventana de cancelación es mínima (una orden de mercado no suele estar "abierta" largo tiempo). Esto **reduce** pero **no elimina** el riesgo de B-MD-008.
3. **Estado de orden estricto:** `Order.transition` (`order.py:142-179`) con grafo validado — terminales no retroceden. Protege contra doble-fill, pero también hace **imposible** corregir un CANCELLED→FILLED (bloquea la auto-reparación).
4. **Idempotencia por client_order_id:** `LiveExecutor._submit` usa `order.order_id` como `clientOrderId` (`live_executor.py:174`) → reintentos no duplican órdenes.
5. **WAC/ADR-0025 + settlement canónico:** la contabilidad de posición/P&L es determinista a partir del fill real; no depende de saldo. Mitiga el **cálculo**, no el **conocimiento** del saldo real.
6. **Fail-closed de guard:** `ExecutionGuard` (kill switch global) y `guard.should_stop()` antes de cada submit (`oms.py:195`, `live_executor.py:163`). Puede detener envíos, no revertir los ya enviados.

**Conclusión sobre mitigaciones:** ninguna cubre B-MD-008 (cancel real) ni B-MD-009 (saldo real). Las mitigaciones existentes protegen la contabilidad del fill, no el control ni la visibilidad del estado del exchange.

---

## 7. Gaps reales (resumen)

- **G1 — Sin cancel real:** port `OrderTransport` sin `cancel`; CCXTAdapter sin `cancel_order`; OMS.cancel local-only y sin invocar.
- **G2 — Sin gestión de órdenes abiertas:** no hay loop de monitorización de órdenes pendientes (Freqtrade `manage_open_orders`).
- **G3 — Sin saldo real:** no hay `fetch_balance` ni port de balance en ningún BC.
- **G4 — Sin reconciliación de saldo/posición contra exchange:** el estado OCM (PortfolioService/PositionStore) se construye solo desde fills propios; nada verifica contra el exchange.
- **G5 — Sin taxonomía de cancelación:** `reject_reason` es string libre (`order.py:121`); no hay razones tipadas de cancelación.

---

## 8. Diseño conceptual B-MD-008 (cancelación real) — NO implementado

### Flujo objetivo

```
OCM solicita cancelación (use case / manage_open_orders)
   ▼
OMS.request_cancel(order_id)          — estado local: PENDING_CANCEL (nuevo estado transitorio)
   ▼
OrderTransport.cancel(exchange_order_id)   [port NUEVO método]
   ▼
CCXTAdapter.cancel_order(symbol, order_id, params)   [nuevo método]
   ▼
exchange → resultado real (cancelado / ejecutado / inexistente / timeout / error)
   ▼
OMS resuelve según resultado (tabla abajo) y reconcilia estado final
```

### Posibles resultados y tratamiento

| Resultado del exchange | Estado local final | Acción |
|---|---|---|
| Cancelación confirmada | `CANCELLED` | Revertir posición HELD (ya hace `record_close`); evento `OrderCancelled` |
| Orden ya ejecutada (fill completo) | `FILLED` | Aplicar fill real (fill_price/filled_qty/fees); pasar por `_fill` (WAC/settlement); **el fill gana** |
| Orden parcialmente ejecutada | `FILLED` (parcial) + `remaining_qty` | Aplicar fill parcial, loguear `remaining`; evento de cancelación parcial |
| Orden inexistente (404) | `CANCELLED` (con nota) o `REJECTED` | SafeOps: reconciliar por `fetch_state` antes de decidir |
| Exchange desconectado | `CANCELLED_PENDING` (transitorio) | Reintentar con backoff; no dar por cerrado |
| Timeout | `CANCELLED_PENDING` | `fetch_state` de verificación; fail-closed |
| Error desconocido | `CANCELLED_PENDING` + alerta | Requiere intervención; no inventar estado final |

### Regla CANCEL vs FILL (diseño)

**El fill SIEMPRE prevalece sobre el cancel.** Justificación: el fill es un hecho del exchange con consecuencia económica directa (posición/P&L); el cancel es una intención. Consecuencia en el estado: `CANCELLED` NO debe ser un estado ciego — o bien se resuelve tras confirmación, o bien la reconciliación posterior (`fetch_state`) puede re-promover un `CANCELLED` a `FILLED` cuando el exchange lo confirma. Esto exige **reabrir el grafo de transiciones** (`order.py:64-74`): `CANCELLED → FILLED` permitido solo vía reconciliación con evidencia del exchange (no vía manual). Alternativa más conservadora: estado transitorio `CANCELLING` → resolución determinista (FILLED o CANCELLED) sin permitir retroceso.

### Ubicación conceptual

- **Port:** extender `OrderTransport` (trading/execution/transport.py) con `cancel(symbol, exchange_order_id) -> OrderState` + (opcional) `fetch_open_orders()`. Es extensión de port existente, no nuevo port.
- **Adapter:** `_BybitTransport.cancel` (composition_root.py) → `CCXTAdapter.cancel_order`.
- **BC:** todo dentro de `trading` (execution). El transporte ya vive en trading/execution (BC-50 cumple: trading no importa market_data fuera de composition root).
- **Composition root:** `TradingCompositionRoot.assemble_live` inyecta el transporte con cancel al `LiveExecutor`/`OMS`.
- **Eventos:** `OrderCancelled` (nuevo, Kappa domain event) — el OMS hoy no emite eventos; alinear con el patrón de eventos de mercado_data (`domain/events/`).

---

## 9. Diseño conceptual B-MD-009 (reconciliación de saldo) — NO implementado

### Flujo objetivo

```
estado OCM (PortfolioService + RiskManager + capital_usd)
   ▼
consulta al exchange (BalancePort.fetch_balances)      [port NUEVO]
   ▼
estado real (total / disponible por activo)
   ▼
comparación vs estado OCM
   ▼
MATCH ──► ok
MISMATCH ──► política según severidad (tabla abajo)
```

### Cuándo ejecutarla (diseño, no prescriptivo)

| Momento | Coste | Latencia | Valor | Recomendación |
|---|---|---|---|---|
| Startup / antes de habilitar live | Bajo (1 llamada) | Baja | Alto — detecta saldo inicial incorrecto | **Sí, obligatorio** (gate de arranque live) |
| Antes de cada orden | Alto (1 llamada/orden) | Añade latencia a cada submit | Medio | **No por defecto** — usar saldo cacheado + freshness (B-MD-001) |
| Periódico (ej. cada N ciclos / heartbeat) | Bajo | No bloqueante | Alto | **Sí** — loop de reconciliación |
| Después de fills | Bajo | No bloqueante | Medio | **Sí** (asíncrono, no en el camino del submit) |
| Después de errores/desconexión | Bajo | No bloqueante | Alto | **Sí** — el estado local puede divergir tras fallo |
| Tras reconexión | Bajo | No bloqueante | Alto | **Sí** — misma razón |

**Regla:** el balance es **lectura periódica asíncrona + gate en arranque**, no un fetch sincrónico en cada orden. Reutiliza el patrón `fetch_state` (fail-closed, timeout) de `OrderTransport`.

### Tratamiento de mismatch (diseño)

Severidades (sin inventar tolerancias numéricas — **configurables**, su origen se justifica por política de riesgo, no por heurística):

| Caso | Severidad | Acción |
|---|---|---|
| Redondeo/precisión (diferencia ≤ tol_config.rounding) | No material | Log debug; sin acción |
| Fees pequeñas (diferencia ≤ tol_config.fees) | No material | Log info; ajustar capital interno (opcional) |
| Discrepancia pequeña (≤ tol_config.tolerance_pct) | Leve | Alerta info; monitorizar |
| Diferencia material (> tol_config.tolerance_pct) | Material | **Bloquear nuevas órdenes** + alerta + estado degradado |
| Posición inexistente en exchange pero sí en OCM | Material | Alerta crítica; no auto-cerrar (requiere humano) |
| Balance insuficiente para órdenes planificadas | Material | Bloquear órdenes de ese símbolo; alerta |
| Estado imposible (saldo negativo, qty inconsistente) | Crítico | Halt global (guard) + alerta crítica + intervención humana |

- **Reconciliar automáticamente:** solo diferencias de redondeo/fees.
- **Actualizar estado:** capital interno (fee del día) — con audit log.
- **Bloquear nuevas órdenes:** toda discrepancia material (fail-closed, coherente con el patrón OCM).
- **Requerir intervención humana:** posiciones divergentes o estado imposible — OCM nunca auto-corrige una posición contra el exchange sin decisión (la posición es SSOT en PortfolioService/PositionStore, BC-43).

### Ubicación conceptual

- **Port:** `BalancePort` — ¿dónde? **No en trading.** La reconciliación compara el **estado de posiciones** (portfolio, BC-43 SSOT) contra el exchange. El dueño natural es **portfolio** (tiene el estado de posiciones y su Composition Root inyecta PositionStore). `trading` consultaría el saldo vía port de portfolio (o el reconcile live corre en portfolio). Esto evita múltiples SSOT.
- **BC:** `portfolio` (reconciliación de saldo/posiciones) + adapter `_BybitBalanceSource` en `portfolio/bootstrap/composition_root.py` (único punto autorizado a importar market_data/CCXT — mismo criterio BC-50 que trading). `trading` consume el resultado (freshness del saldo) sin poseerlo.
- **Modelo correcto (SSOT único):** `exchange → [reconcile] → portfolio (PositionStore + saldo derivado) → trading (lee vía port)`. **NO** `trading → balance propio` + `portfolio → balance propio` + `exchange → real` (3 fuentes de verdad). El balance del exchange es la fuente primaria; portfolio lo materializa; trading lo consume.

---

## 10. Comparación con Freqtrade (referencia de comportamiento, no de arquitectura)

| Comportamiento Freqtrade | OCM actual | Gap | Solución OCM (diseño) |
|---|---|---|---|
| `handle_cancel_order` (freqtradebot.py:1635) — cancela órdenes timeout y gestiona emergencias | `OMS.cancel` local-only y sin invocar (oms.py:300) | No hay cancel real ni gestión de timeout | Extender `OrderTransport.cancel` + loop de gestión de órdenes abiertas |
| `update_trade_state` (:2339) — reconcilia el estado de la orden contra el exchange en cada análisis | `LiveExecutor._reconcile` solo en el submit (live_executor.py:229) | Reconciliación puntual, no periódica | Reconciliación periódica de órdenes abiertas + estado resoluble CANCEL↔FILL |
| `_safe_exit_amount` (:2047) — ajusta cantidad de salida al balance real | Sizing contra `capital_usd` configurado (manager.py:394) | Sin visibilidad de saldo real | `BalancePort` + freshness antes del sizing |
| `manage_open_orders` (:1602) — loop de órdenes abiertas con cancel/replace | No existe | Sin gestión de órdenes pendientes | Loop de órdenes abiertas (B-MD-008 ampliado) |
| `cancel_order_with_result` (exchange.py:1824) — cancel con fallback a fetch_order si el cancel no es usable | No existe | Sin determinismo del resultado de cancel | Resolución CANCEL/FILL con `fetch_state` de verificación |
| `get_balances` (exchange.py:1879) — saldo por moneda | No existe (grep ∅) | Sin saldo real | `BalancePort` en portfolio |
| fee fallback (`update_trade_state` → `handle_order_fee`) | `Settlement.fee_currency=None` (settlement.py:61, GAP F7) | Fees sin moneda/fallback | Vincular con ADR-0026 (fee semantics) — no bloqueante para cancel/balance |

Freqtrade resuelve: cancel real + reconciliación periódica + saldo real. OCM hace: cancel local sin uso + reconciliación solo en submit + capital configurado. El gap es: **control y visibilidad del estado del exchange**. La solución OCM debe mantener Kappa/event-driven, BCs, hexagonal, ports/adapters y composition roots — añadiendo el port `cancel` (trading) y `BalancePort` (portfolio), no copiando el monolito Freqtrade.

---

## 11. Comparación con Hummingbot (referencia conceptual)

| Patrón Hummingbot | OCM actual | Extracción útil (sin copiar) |
|---|---|---|
| `connector_base.pyx` — `_account_balances`/`_account_available_balances` + `get_balance`/`get_available_balance` (:349-406) | Sin saldo | El balance se materializa por connector y se consulta por activo; OCM → `BalancePort` por activo |
| Position/MarketState separados del connector | PortfolioService SSOT de posiciones (BC-43) | Coherente: OCM ya separa estado de posición del connector — mantener |
| Reconciliación vía estados del connector | Fill-sync fail-closed | La reconciliación de saldo/posición debe ser periódica y fail-closed, no solo en submit |
| MarketData/price-by-type como capa separada | FeatureSource (Gold pull) | No relacionado con cancel/balance; fuera de alcance |

---

## 12. Relación con portfolio (SSOT)

- **Portfolio es el dueño de posiciones (BC-43):** `PositionStore` (InMemory/Redis) solo instanciable en portfolio bootstrap. **Confirmado.**
- **Dónde debe vivir la reconciliación de saldo/posición:** en **portfolio** (compara el estado SSOT contra el exchange y materializa el saldo derivado). Trading consume el resultado. **Modelo: exchange → portfolio → trading.**
- **No duplicar SSOT:** prohibido `trading/balance` + `portfolio/balance` + `exchange/balance` como tres fuentes. El balance del exchange es primario; portfolio lo materializa; trading lo consume vía port.
- **Impacto en stop-loss:** `StopLossEvaluator` (stop_loss.py) evalúa contra `avg_entry` de posiciones de portfolio — la reconciliación de posiciones del exchange garantizaría que ese snapshot sea fiel.

---

## 13. Relación con BookBuilder / market data

- **B-MD-002 (BookBuilder)** desbloquea `MarketDataViewPort` (mid/spread/depth) → B-MD-004 (pre-submit market validity). **No es prerrequisito** de B-MD-008/009: el cancel real y el balance no dependen del order book.
- **B-MD-008/009 se relacionan con B-MD-004** solo en el momento de la orden: B-MD-004 valida mercado; B-MD-008/009 validan control/saldo. Son capas complementarias de la misma seguridad de ejecución.
- **B-MD-001 (freshness)** es útil para el saldo cacheado (saber si el `BalancePort` está stale antes de usarlo). Relación débil.

---

## 14. Dependencias

| Propuesta | Depende de | Es prerrequisito de |
|---|---|---|
| B-MD-008 (cancel real) | Investigación capacidad de cancel Bybit (CCXT `cancel_order`); extensión de `OrderTransport` y `CCXTAdapter` | — |
| B-MD-009 (BalancePort en portfolio) | Decisión de BC (portfolio dueño); `_BybitBalanceSource` adapter en portfolio root | B-MD-004 (mejora, no bloquea) |
| B-MD-008/009 (juntos) | Independientes entre sí | Live seguro (junto a B-MD-001/002/004) |

---

## 15. Orden recomendado de implementación

**El roadmap general (Fase 1 market_data → Fase 2 → Fase 3) NO cambia. Justificación:** B-MD-008/009 tocan `trading/portfolio` (Fase 2/3), no `market_data` (Fase 1). Insertarlas en Fase 3, sin reordenar Fase 1.

| Orden | Propuesta | Fase | Nota |
|---|---|---|---|
| 1 | B-MD-003 (sequence wire) | 1 | Sin cambio |
| 2 | B-MD-001 (freshness) | 1 | Sin cambio |
| 3 | B-MD-005 (instrumentos) | 1/2 | Sin cambio |
| 4 | B-MD-002 (BookBuilder) | 2 | Sin cambio |
| 5 | B-MD-004 (market validity) | 2/3 | Sin cambio |
| 6 | **B-MD-009 (BalancePort/reconcile)** | **3** | **Antes de live real — sizing seguro** |
| 7 | **B-MD-008 (cancel real)** | **3** | **Antes de live real — control de órdenes** |
| 8 | B-MD-007 / B-MD-006 | 2/3 | Sin cambio |

**Secuencia obligatoria nueva:** B-MD-009 y B-MD-008 son independientes entre sí; ambas deben estar **antes** de operar live real (junto a la cadena B-MD-003→002→004 + B-MD-001).

---

## 16. Impacto sobre Live-Readiness

**Veredicto: NO.** ¿Puede OCM operar mañana con dinero real? **NO.**

**Justificación (evidencia):** sin `OrderTransport.cancel` (transport.py:96-128) ni `OMS.cancel` usable (oms.py:300, nunca invocado), OCM no puede revocar una orden en vuelo (B-MD-008, P1). Sin `fetch_balance` en ningún BC, el sizing/riesgo se computa contra `capital_usd` configurado (manager.py:394) y no contra el saldo real (B-MD-009, P1). Ambos pueden provocar: órdenes no deseadas, pérdida de control, exposición superior a la esperada, estado local divergente y decisiones con información incorrecta.

**Clasificación:**

- **Bloqueantes absolutos para live (P0/P1):**
  - B-MD-003 → B-MD-002 → B-MD-004 (cadena microstructure + market validity; sin ella, market orders a precio posiblemente stale).
  - B-MD-001 (freshness — un mercado congelado es indistinguible).
  - **B-MD-009 (saldo real)** — sizing/exposición incorrecta con capital real.
  - **B-MD-008 (cancel real + gestión de órdenes abiertas)** — sin control sobre órdenes en vuelo; divergencia CANCEL/FILL.
- **Riesgos importantes pero no necesariamente bloqueantes:**
  - B-MD-005 (instrumentos/precisión) — mejora la validez de órdenes.
  - B-MD-007 (received_at/processed_at) — observabilidad.
  - B-MD-006 (trades_stream) — limpieza.
- **Mejoras posteriores (post-live):**
  - Taxonomía de razones de cancelación (G5).
  - Reconciliación de balance con fees detalladas (vincular ADR-0026).
  - Reconciliación de posiciones contra exchange a nivel de lote (Hummingbot-style) — después del SSOT básico.

---

## 17. Recomendaciones para tracking.yaml (propuestas — NO editado)

> `docs/plans/tracking.yaml` no se modifica en esta sesión. Propuesta para decisión humana:

1. **Añadir item B-MD-009** (`fetch_balance`/reconciliación de saldo, P1, Fase 3, BC=portfolio, requiere ADR si se define política de fail-closed ante discrepancia — proponer A-MD-005).
2. **Añadir item B-MD-008** (cancel real, P1, Fase 3, BC=trading, port `OrderTransport` extendido, requiere investigación capacidad Bybit + opcionalmente ADR si cambia el contrato — A-MD-004).
3. **Nota de trazabilidad:** ambos son bloqueantes de live real junto con la cadena existente (B-MD-001/002/003/004). Su prioridad relativa: **igual que B-MD-004 (P0/P1 de live)**.
4. **No despriorizar** Fase 1 por estos hallazgos: el orden de fases se mantiene (market_data primero, ejecución después).

---

## 18. Conclusiones finales

1. **B-MD-008 VERIFICADO y ampliado:** `OMS.cancel` es local-only (oms.py:300) Y **nunca se invoca** (grep). El port `OrderTransport` no tiene cancel; el CCXTAdapter no expone `cancel_order`. **Correcta la hipótesis previa; se añade el hallazgo de que no hay caller.**
2. **B-MD-009 VERIFICADO:** no existe ningún mecanismo de saldo en todo el repo (grep exhaustivo); RiskManager y PortfolioService usan `capital_usd` configurado. **Correcta la hipótesis previa; se confirma ampliado.**
3. **Carrera CANCEL vs FILL demostrable:** `CANCELLED` es terminal (order.py:73) y no hay reconciliación periódica → divergencia silenciosa posible. La mitigación "solo market orders" reduce, no elimina.
4. **SSOT:** la reconciliación de saldo/posición debe vivir en **portfolio** (BC-43), no en trading — un solo punto de verdad.
5. **Veredicto Live-Readiness: NO se modifica** — sigue siendo NO, ahora con más evidencia (cancel y balance).

---

## Referencias

- Código: `packages/trading/execution/oms.py`, `transport.py`, `order.py`, `live_executor.py`, `fill_sync.py`, `settlement.py`, `paper_executor.py`, `packages/trading/risk/manager.py`, `packages/trading/risk/stop_loss.py`, `packages/trading/engine.py`, `packages/trading/analytics/trade_tracker.py`, `packages/trading/bootstrap/composition_root.py`, `packages/portfolio/services/portfolio_service.py`, `packages/portfolio/models/position.py`, `packages/portfolio/bootstrap/composition_root.py`, `packages/market_data/adapters/outbound/exchange/ccxt_adapter.py`, `shared/contracts/boundaries.py`, `ocm/runtime/guard.py`, `apps/app/cli/live_hydra.py`, `apps/app/use_cases/execute_live.py`
- Tests: `tests/trading/test_oms_fill_lifecycle.py`, `tests/trading/test_transport_mapping.py`, `tests/trading/test_live_executor.py`
- Docs: `docs/plans/tracking.yaml` (B-25/B-26), `docs/architecture/decisions/ADR-0016-...`, `ADR-0025-...`, `ADR-0026-...`, `docs/audits/2026-08-14-*`, `docs/audits/2026-08-15-*`
- Referencia externa: `docs/freqtrade-develop.zip` (freqtradebot.py:1602,1635,2047,2339; exchange.py:1783,1824,1879), `docs/hummingbot-master.zip` (connector_base.pyx:349-406)
