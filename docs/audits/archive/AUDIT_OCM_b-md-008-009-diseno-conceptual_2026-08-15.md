# Diseño conceptual — B-MD-008 (cancelación real) y B-MD-009 (balance real)

**Fecha:** 2026-08-15
**Autor:** OpenAI Codex (asistido por agente de exploración)
**Rol:** Arquitecto Principal de Software · Trading Systems Reviewer
**Documento de origen de evidencia:** `docs/audits/2026-08-15-b-md-008-cancel-b-md-009-balance-audit.md` (este documento **no lo duplica ni lo sobrescribe**; profundiza el diseño conceptual, Partes 3–8, que el audit anterior cubrió solo parcialmente).
**Método:** evidencia directa de código verificada en esta sesión contra HEAD (`archivo:línea`), re-confirmando las líneas citadas por el audit. Distingue **VERIFIED** (comprobado en código), **INFERENCE** (deducción razonable), **UNKNOWN** (sin evidencia suficiente).
**Restricciones respetadas:** sin código de producción; `tracking.yaml` NO modificado; ADRs existentes NO modificadas; sin commits/push. Único archivo creado: este documento.

---

## 1. Resumen ejecutivo (para persona no experta)

OCM es un sistema de trading que envía órdenes a un exchange (Bybit). Para operar con dinero real necesita resolver dos cosas básicas:

1. **Poder cancelar una orden de verdad (B-MD-008).** Hoy, cuando OCM "cancela" una orden, **solo cambia un número en su memoria interna** a `CANCELLED`. No envía ninguna instrucción al exchange. El exchange puede seguir ejecutando la orden sin que OCM se entere, y OCM quedaría creyendo que la orden no existe cuando en realidad sí se ejecutó. Peor aún: **ese método de cancelación no se usa desde ningún lugar del código** — OCM no tiene manera de cancelar nada hoy.
2. **Saber cuánto dinero tiene de verdad (B-MD-009).** OCM calcula cuánto puede comprar/vender usando un número de capital **escrito en la configuración** (`capital_usd`), no el saldo real de la cuenta del exchange. Si el exchange tiene menos dinero (por comisiones, transferencias, o una orden ejecutada fuera del sistema), OCM puede intentar operar con dinero que no tiene, o calcular mal su exposición.

**Veredicto Live-Readiness: NO.** Con el estado actual, no se puede operar con dinero real mañana. Los dos problemas son bloqueantes de live, junto con la cadena ya conocida de market data (B-MD-001/002/003/004). La buena noticia: **B-MD-008 y B-MD-009 son independientes de la cadena de market data** — no necesitan esperar al BookBuilder, se pueden diseñar y validar en paralelo dentro de la Fase 3 del roadmap.

Este documento propone cómo deberían funcionar las soluciones, sin escribir código.

---

## 2. Evidencia re-verificada en HEAD (confirmando el audit)

> Toda la evidencia del audit 2026-08-15 se re-verificó en esta sesión contra HEAD. **Las líneas citadas siguen siendo válidas.** Cambios significativos: ninguno. Se confirma además un hecho nuevo: **trading no usa Kafka ni ningún event bus** (solo callbacks síncronos), lo que condiciona el diseño de la sección 6.

### B-MD-008

| Evidencia | Línea (HEAD) | Estado |
|---|---|---|
| `OMS.cancel()` — transición local a CANCELLED, pop de `_open`, `record_close` para BUY; **sin llamada a transporte/executor/exchange** | `packages/trading/execution/oms.py:300-317` | VERIFIED |
| `OrderTransport` Protocol — solo `submit`/`fetch_state`/`close`; **sin `cancel`** | `packages/trading/execution/transport.py:96-128` | VERIFIED |
| `PaperTransport` — sin cancel | `transport.py:131-158` | VERIFIED |
| `_BybitTransport` adapter — solo `submit`/`fetch_state`/`close` | `packages/trading/bootstrap/composition_root.py:203-262` | VERIFIED |
| `CCXTAdapter` — `create_order` (:405), `fetch_order` (:462); **sin `cancel_order` ni `fetch_balance`** | `packages/market_data/adapters/outbound/exchange/ccxt_adapter.py` | VERIFIED |
| `OrderStatus.CANCELLED` es terminal en el grafo (`_VALID_TRANSITIONS[CANCELLED] = set()`) | `packages/trading/execution/order.py:64-74` | VERIFIED |
| `LiveExecutor._reconcile` — fail-closed, solo durante submit | `packages/trading/execution/live_executor.py:229-261` | VERIFIED |
| `OMS.cancel()` **no se invoca desde ningún lugar** (grep de `\.cancel(` sobre órdenes = ∅) | — | VERIFIED |
| `fill_sync.on_fill_composite` solo dispara en `on_fill` (FILLED) | `packages/trading/execution/fill_sync.py:109-167` | VERIFIED |

### B-MD-009

| Evidencia | Línea (HEAD) | Estado |
|---|---|---|
| Sin `fetch_balance`/`get_balance`/`balance` en todo el repo (grep exhaustivo; solo "rebalance") | — | VERIFIED |
| `RiskManager.__init__(..., capital_usd=10_000.0)` — capital configurado | `packages/trading/risk/manager.py:112-118` | VERIFIED |
| Sizing y drawdown sobre `self._capital_usd` configurado | `manager.py:394-401`, `:374-386` | VERIFIED |
| `PortfolioService.__init__(capital_usd, store, exchange)` — capital configurado | `packages/portfolio/services/portfolio_service.py:63-80` | VERIFIED |
| `PortfolioState.capital_usd` — configurado | `portfolio_service.py:281-284` | VERIFIED |
| `PositionStore` solo instanciable en portfolio bootstrap (BC-43) | `packages/portfolio/bootstrap/composition_root.py:13-24` | VERIFIED |
| Sin Kafka/event bus en trading (grep `kafka|produce|consume` en `packages/trading` = ∅) | — | VERIFIED |

---

## 3. B-MD-008 — Cancelación real

### 3A. Qué ocurre hoy (cancelación local, VERIFIED)

`OMS.cancel(order_id)` (`oms.py:300-317`) hace exactamente:

```
OMS.cancel(order_id)
  ├─ bajo RLock:
  │    _orders.get(order_id) → None o is_terminal → return False
  │    order.transition(OrderStatus.CANCELLED)   # terminal
  │    _open.pop(order_id)
  ├─ si side == BUY: _risk.record_close(pnl_usd=None)
  └─ log "Order cancelled"
```

Es el modelo **OCM → cambia estado local**. No hay ninguna llamada a `OrderTransport`, `OrderExecutor`, CCXT ni exchange. Además, **nadie llama a `OMS.cancel()`** en todo el código (VERIFIED) — el método es código muerto en la práctica.

### 3B. Cancelación en exchange (VERIFIED: no existe)

Búsqueda exhaustiva de `cancel_order`/`cancel`/`cancel_all`:
- `OrderTransport` (port) — sin `cancel` (`transport.py:96-128`).
- `_BybitTransport` (adapter) — sin `cancel` (`composition_root.py:203-262`).
- `CCXTAdapter` — sin `cancel_order` (`ccxt_adapter.py`; solo `create_order`/`fetch_order`).
- El cliente CCXT subyacente (`ccxt.Exchange`) sí expone `cancel_order`, pero **no está expuesto por el adapter** — la capacidad existe en la librería, no en OCM.

**Conclusión inequívoca:** `OMS.cancel()` **nunca llega al exchange**. Cuando OCM dice "cancelé esta orden", solo ha cambiado su estado interno.

### 3C. Carrera CANCEL vs FILL (VERIFIED + INFERENCE)

Camino demostrado en el audit (re-verificado):

1. `OMS.submit` → `transition(SUBMITTED)` → `executor.execute` (`oms.py:271,283`).
2. `LiveExecutor._submit` + `_reconcile` (`live_executor.py:161-261`): la única reconciliación con el exchange, y ocurre **durante el submit**. Fail-closed: si no confirma FILLED → rechazo.
3. Si una orden queda CANCELLED (hoy sin invocar, pero si se invocara): `CANCELLED` es **terminal** (`order.py:73`) → `transition(FILLED)` lanzaría `ValueError`. **No existe `CANCELLED → FILLED`.**
4. `fill_sync.on_fill_composite` (`fill_sync.py:109`) solo corre vía `on_fill` (solo para órdenes FILLED). Una orden cancelada localmente pero ejecutada en exchange: ni `TradeTracker` ni `PortfolioService` reciben el fill → la posición real del exchange queda **fuera del estado OCM**.

**Respuestas a las preguntas concretas:**
- ¿`fill_sync` puede descubrir el fill posteriormente? **No.** Solo se dispara por `on_fill` (fill síncrono del executor).
- ¿El grafo permite `CANCELLED → FILLED`? **No.** `CANCELLED` es terminal (`order.py:73`).
- ¿Se produce un error? Un `transition(FILLED)` manual sobre CANCELLED lanzaría `ValueError`; pero como no hay flujo que lo intente, el efecto es **silencio** (divergencia sin error).
- ¿Se pierde el fill? **Sí, silenciosamente.** Ningún componente del estado OCM lo ve.
- ¿Se genera una inconsistencia? **Sí.** OCM cree `CANCELLED`, exchange tiene la posición ejecutada → portfolio/risk/pnl desincronizados.
- ¿Existe reconciliación? **Solo la de submit** (`live_executor.py:229`). No hay reconciliación periódica de órdenes abiertas ni posterior a cancelación.

**Mitigaciones parciales existentes (VERIFIED):**
- Solo market orders (`composition_root.py:238`) → fill casi inmediato → ventana de cancelación mínima en la práctica (una market order no suele estar "abierta" mucho tiempo). **Reduce el riesgo, no lo elimina.**
- Idempotencia por `client_order_id` (`live_executor.py:174`).
- Fail-closed en `_reconcile` (evita creer FILLED sin confirmación).

### 3D. Diseño conceptual (sin implementar)

**Flujo objetivo:**

```
use case / manage-open-orders
   ▼
OMS.request_cancel(order_id)                 # estado local transitorio CANCELLING
   ▼
OrderTransport.cancel(symbol, exchange_order_id)   [nuevo método del port]
   ▼
CCXTAdapter.cancel_order(symbol, order_id, params) [nuevo método del adapter]
   ▼
exchange → resultado real
   ▼
OMS resuelve el estado final según resultado
   ▼
notificación (callback / evento de dominio) → fill_sync / portfolio / risk
```

**Tratamiento por resultado del exchange:**

| Respuesta exchange | Estado local | Acción |
|---|---|---|
| CANCELLED (confirmado) | `CANCELLED` (definitivo) | Revertir posición HELD (ya lo hace `record_close`); notificar |
| ALREADY_FILLED / FILLED | `FILLED` | **El fill prevalece.** Aplicar fill real completo por el flujo normal `_fill` (WAC, settlement, fill_sync, portfolio). Corregir el CANCELLING → FILLED (requiere reabrir el grafo, ver abajo) |
| PARCIALMENTE ejecutada | `FILLED` (parcial) + `remaining` | Aplicar fill parcial; loguear `remaining`; registrar cancelación del resto |
| NOT_FOUND (404) | `CANCELLED` con nota (o reconciliar por `fetch_state` primero) | SafeOps: `fetch_state` antes de decidir; si tampoco aparece → CANCELLED + alerta |
| TIMEOUT | `CANCELLING` persistente | Reintentar con backoff; luego `fetch_state` de verificación; **no dar por cerrado sin confirmación** |
| ERROR desconocido | `CANCELLING` + alerta | Requiere intervención; no inventar estado final |

**Regla CANCEL vs FILL: el fill SIEMPRE prevalece.** El fill es un hecho del exchange con consecuencia económica (posición, P&L); la cancelación es una intención. Consecuencia de diseño:

- **El estado terminal `CANCELLED` no debe ser ciego.** O bien se usa un estado transitorio `CANCELLING` que se resuelve por confirmación, o bien se permite `CANCELLED → FILLED` **solo por reconciliación con evidencia del exchange** (nunca por ruta manual). La opción recomendada es el estado transitorio `CANCELLING` → resolución determinista, que evita relajar el grafo de terminales.
- La decisión "qué prevalece" debe ser **política explícita** (ADR A-MD-004 propuesto), no comportamiento accidental.

**Adaptación a la arquitectura OCM:**
- **BC:** todo dentro de `trading/execution`. El transporte ya vive ahí (BC-50: trading no importa market_data fuera del composition root).
- **Port:** extender `OrderTransport` (`transport.py`) con `cancel(symbol, exchange_order_id) -> OrderState`. Es extensión de port existente, no port nuevo.
- **Adapter:** `_BybitTransport.cancel` (`composition_root.py`) → `CCXTAdapter.cancel_order` (nuevo método en `ccxt_adapter.py`).
- **Composition root:** `TradingCompositionRoot.assemble_live` inyecta el transporte con cancel a `LiveExecutor`/`OMS`.
- **Kafka/event-driven:** hoy trading es 100% síncrono/callback. El diseño **no introduce Kafka** en esta fase: usa el patrón existente de callbacks (`on_fill`/`on_reject`) para notificar `fill_sync` → `TradeTracker`/`PortfolioService`. Si en el futuro se introduce event bus, se alinea con el de market_data (`domain/events/`), pero no es prerrequisito.
- **Gestión de órdenes abiertas:** el diseño asume un caller nuevo (`manage_open_orders` — concepto Freqtrade §5) que recorre órdenes SUBMITTED/CANCELLING y dispara cancel/verificación. Hoy no existe.
- **fill reconciliation:** el resultado ALREADY_FILLED debe reutilizar `OMS._fill` tal cual (WAC/settlement/portfolio ya funcionan) — la reconciliación de fill ya existe y es sólida; lo que falta es conectarla a la cancelación.

---

## 4. B-MD-009 — Balance real

### 4A. Qué ocurre hoy (VERIFIED)

Grep exhaustivo de `fetch_balance`/`balance`/`wallet`/`available_balance`/`free`/`equity` en `packages/`, `shared/`, `apps/`, `ocm/`: **ningún mecanismo de saldo**. Las únicas coincidencias de "balance" son **"rebalance"** (falso positivo).

- `RiskManager` recibe `capital_usd` por constructor (`manager.py:112-118`, default 10_000). Sizing (`:394-401`) y drawdown (`:374-386`) se computan contra ese número fijo.
- `PortfolioService` recibe `capital_usd` por constructor (`portfolio_service.py:63-80`) y lo reporta en `PortfolioState.capital_usd` (`:281-284`).
- `PositionStore` (InMemory/Redis) es el estado persistido de posiciones, SSOT (BC-43, solo instanciable en portfolio bootstrap).

**Conclusión inequívoca:** OCM **no conoce el balance real del exchange**. Usa un capital configurado/interno. No existe llamada real al exchange para obtener balances (`ccxt_adapter.py` no expone `fetch_balance`).

### 4B. Respuestas a las comprobaciones

1. **¿Existe llamada real al exchange para balances?** No (VERIFIED, grep + adapter).
2. **¿Dónde vive el estado de Portfolio?** `PositionStore` (InMemory/Redis) vía `PortfolioService` (VERIFIED, BC-43).
3. **¿Quién es el dueño de posiciones/balance?** Portfolio es dueño de **posiciones** (BC-43). Del **balance** no hay dueño hoy porque no existe.
4. **¿Portfolio tiene mecanismo reutilizable?** El patrón `PortfolioService` + `PositionStore` + `snapshot()` es reutilizable como estructura (estado interno + consulta), pero no hay nada de balance.
5. **¿Riesgo de segunda fuente de verdad?** **Sí, real.** Si trading creara su propio balance y portfolio otro, habría 3 fuentes (exchange real + trading + portfolio). El diseño §5 lo evita.

---

## 5. Diseño correcto — flujo y fuente de verdad

**Modelo propuesto (conceptual):**

```
EXCHANGE (Bybit)
   │  fetch_balance / fetch_positions  (vía adapter CCXT)
   ▼
PORTFOLIO  ←── dueño del estado patrimonial (posiciones + saldo derivado)
   │          · PositionStore (BC-43) — SSOT de posiciones
   │          · BalanceStore (nuevo)  — saldo materializado por activo
   ▼
TRADING / RISK  (consume vía ports, nunca posee estado patrimonial propio)
   │
   ▼
OMS → exchange (ejecución)
```

**¿Por qué Portfolio es el dueño?**
- **Regla arquitectónica: un solo punto de verdad.** Portfolio ya es el dueño de posiciones (BC-43, enforcement por import-linter). El balance es la otra mitad del estado patrimonial → mismo dueño natural.
- Trading ya depende de portfolio (BC-43, inyección en `TradingCompositionRoot`); agregar el saldo a portfolio **no añade una frontera nueva** — solo una dependencia más en la dirección existente.
- Risk consume "capital disponible" para sizing/drawdown; si lo leyera del exchange directamente o de un store propio, divergiría de las posiciones.

**Alternativa evaluada y rechazada:** Trading poseyendo su propio balance (con `fetch_balance` directo en `trading/`). Rechazada: crearía 2ª/3ª fuente de verdad y violaría la direccionalidad BC-50/BC-43 (trading no debería poseer estado patrimonial duplicado).

**El adapter CCXT (`fetch_balance`) vive en el composition root de portfolio** (único punto autorizado a importar market_data/CCXT, mismo criterio BC-50 que trading). Portfolio materializa; trading consume.

**Especificación conceptual del dueño patrimonial (Portfolio):**
- **Posiciones:** `PositionStore` (SSOT, ya existe).
- **Saldo:** `BalanceStore`/estado interno derivado del balance del exchange (nuevo). Expuesto vía `PortfolioService`.
- **Reconciliación:** `PortfolioReconciler` (nuevo servicio en portfolio) compara `PositionStore` + balance materializado contra el exchange y produce MATCH/MISMATCH.

---

## 6. Cuándo reconciliar el balance — estrategia

### Comparación de alternativas

| Momento | Coste | Latencia | Valor | Riesgo si se hace mal |
|---|---|---|---|---|
| 1. Al iniciar OCM | Bajo (1 llamada) | Baja | **Alto** — detecta saldo inicial incorrecto | Bajo |
| 2. Periódicamente (loop) | Bajo (1 llamada/ciclo) | No bloqueante | **Alto** — detecta fugas/fees/transferencias | Bajo |
| 3. Antes de cada orden | Alto (1 llamada/orden) | Añade latencia a cada submit | Medio | Puede ralentizar la ejecución |
| 4. Después de ejecutar una orden | Bajo (asíncrono) | No bloqueante | Medio | Bajo |
| 5. Tras recuperación/restart | Bajo | Baja | **Alto** — el estado en memoria se perdió; Redis puede divergir | Bajo |
| 6. Tras detectar inconsistencia | Bajo | No bloqueante | Alto | Bajo |

**Recomendación de estrategia (INFERENCE, no prescriptiva):**
- **Gate en arranque (1) + tras restart (5):** obligatorio antes de habilitar live — verifica que el capital configurado coincide con el real.
- **Periódico (2):** loop de reconciliación asíncrono (heartbeat, ej. cada N ciclos o T segundos), no bloqueante.
- **Después de cada fill (4) y tras error/desconexión (6):** asíncrono, no en el camino del submit.
- **Antes de cada orden (3): NO por defecto.** Usar saldo cacheado con frescura (vínculo con B-MD-001) en lugar de fetch sincrónico. Evita añadir latencia a cada submit sin ganancia de seguridad proporcional.

### Política de discrepancia (OCM dice $10.000, exchange dice $9.700)

Definir **sin inventar tolerancias numéricas** — la tolerancia debe ser **configurable** y su origen justificado por política de riesgo, no por heurística:

| Diferencia | Clasificación | Acción |
|---|---|---|
| Redondeo/precisión (≤ `tol.rounding`) | No material | Log debug; sin acción |
| Fees pequeñas (≤ `tol.fees`) | Leve | Log info; ajuste opcional del capital interno (audit log) |
| Pequeña (< `tol.tolerance_pct`) | Leve | Alerta info; monitorizar |
| Material (> `tol.tolerance_pct`) | **Material** | **Bloquear nuevas órdenes** + alerta + estado degradado |
| Balance insuficiente para órdenes planificadas | Material | Bloquear órdenes de ese símbolo; alerta |
| Posición en exchange sin reflejo en OCM (o viceversa) | **Crítico** | Alerta crítica; **no auto-corregir** — intervención humana |
| Estado imposible (negativo, qty inconsistente) | **Crítico** | Halt global (`ExecutionGuard` — existe, `guard.py:87-109`) + alerta + humana |

**Principios:**
- **Corrección automática solo** para redondeo/fees menores (con audit log).
- **Bloquear por defecto** ante discrepancia material (fail-closed, coherente con el patrón OCM).
- **Nunca auto-corregir posiciones** contra el exchange sin decisión humana — la posición es SSOT en Portfolio (BC-43).
- El umbral exacto lo define la política de riesgo, **configurable** (`config/risk/...`), con default conservador y valor en el ADR A-MD-005 propuesto.

---

## 7. Relación con Freqtrade (patrón conceptual, sin copiar)

| Problema que resolvió Freqtrade | Cómo lo resuelve | Cómo lo resolvería OCM (compatible) |
|---|---|---|
| Cancelar órdenes timeout / sin fill | `handle_cancel_order` (`freqtradebot.py:1635`) + `cancel_order_with_result` (`exchange.py:1824`, con fallback a `fetch_order`) | Port `OrderTransport.cancel` + estado transitorio `CANCELLING` + verificación por `fetch_state` si el cancel no es concluyente |
| Reconciliar estado de la orden contra exchange en cada ciclo | `update_trade_state` (`freqtradebot.py:2339`) + `manage_open_orders` (`:1602`) | Loop `manage_open_orders` en OCM que recorre SUBMITTED/CANCELLING y reconcilia con `fetch_state`; resolver CANCEL↔FILL |
| Ajustar cantidad de salida al balance real | `_safe_exit_amount` (`:2047`) | RiskManager consume saldo de Portfolio (via `BalancePort`) antes del sizing, con frescura (B-MD-001) |
| Conocer saldo por moneda | `get_balances` (`exchange.py:1879`) + wallets | `BalancePort` en portfolio + adapter `fetch_balance` en su composition root |
| Fees con fallback | `handle_order_fee` / `update_trade_state` | Ya resuelto parcialmente por `Settlement`/ADR-0026 (GAP F7: fee_currency None) — vincular, no rehacer |

**Gap central:** Freqtrade tiene cancel real + reconciliación periódica + saldo real. OCM tiene cancel local sin uso + reconciliación solo en submit + capital configurado. **La solución OCM adapta el comportamiento, no la arquitectura** (mantiene BCs, ports/adapters, composition roots, SSOT).

---

## 8. Relación con Market Data / BookBuilder

| Dependencia | B-MD-008 (cancel) | B-MD-009 (balance) |
|---|---|---|
| B-MD-002 (BookBuilder) | **No depende.** El cancel no necesita order book | **No depende.** El balance no necesita order book |
| B-MD-003 (sequence number) | **No depende** | **No depende** |
| B-MD-004 (market validity / fallo de market data) | **No depende** (el cancel es post-envío) | **Dependencia débil:** B-MD-001 (freshness) es útil para el saldo cacheado; B-MD-004 es sobre precio/mercado, no sobre saldo |

**Conclusión:** B-MD-008 y B-MD-009 son **independientes de la cadena de microstructure** (003→002→004). Pueden desarrollarse en paralelo dentro de su fase. **No se fuerza ninguna dependencia.** Lo único que comparten es que ambos son bloqueantes de live.

**¿Qué debe esperar?** Nada de la cadena de market data. Sí requieren, cada uno: (a) su ADR (A-MD-004 cancel, A-MD-005 balance), (b) el adapter CCXT correspondiente, (c) decisión de BC (cancel → trading; balance → portfolio).

---

## 9. Prioridad para live

| Problema | Clasificación | Justificación |
|---|---|---|
| B-MD-008 — sin cancel real + sin gestión de órdenes abiertas | **P1 (bloqueante Live)** | Pérdida de control sobre órdenes en vuelo; divergencia CANCEL/FILL silenciosa; sin `manage_open_orders` |
| B-MD-009 — sin balance real + sin reconciliación | **P1 (bloqueante Live)** | Sizing/drawdown sobre capital configurado, no real; exposición y decisiones incorrectas |
| B-MD-004 (market validity) | **P0/P1 (bloqueante Live)** | Market orders a precio posiblemente stale sin validación de mercado (ya clasificado) |
| B-MD-001 (freshness) | **P1 (bloqueante Live)** | Mercado congelado indistinguible (ya clasificado) |

**¿Qué impediría poner dinero real mañana? Respuesta técnica y honesta:**
1. **No puedes cancelar una orden.** Si el motor envía una orden equivocada, no hay manera de detenerla en el exchange (B-MD-008). Y no hay gestión de órdenes abiertas: una orden que quede abierta (market order rara vez, pero posible con fallos de red) nunca se limpia.
2. **No sabes cuánto dinero tienes.** El sizing se calcula contra un número configurado. Si difiere del real (fees, transferencias, fills externos), el sistema puede intentar operar con saldo inexistente o sobreexponerse (B-MD-009).
3. **Aunque lo anterior se resolviera, la ejecución sigue a ciegas del mercado:** market orders sin validación de precio/spread/frescura (B-MD-001/004) pueden llenarse a precio desplazado.
4. **Divergencia silenciosa CANCEL/FILL:** sin reconciliación periódica, el estado local puede divergir del exchange sin ningún error visible.

**Respuesta corta: NO. Tres razones: cancel inexistente (B-MD-008), saldo desconocido (B-MD-009), ejecución sin validación de mercado (B-MD-001/004).**

---

## 10. Matriz de riesgo

| Problema | Estado actual | Riesgo | ¿Bloquea Live? | Prioridad |
|---|---|---|---|---|
| B-MD-008 — cancel local-only, sin uso | `oms.py:300` local; sin port/adapter; sin caller | Pérdida de control; divergencia CANCEL/FILL | **Sí** | **P1** |
| B-MD-009 — sin balance real | Grep ∅; `capital_usd` configurado | Sizing/exposición incorrecta | **Sí** | **P1** |
| Carrera CANCEL vs FILL | `CANCELLED` terminal; sin reconciliación post-cancel | Estado local divergente silencioso | **Sí** | **P1** |
| Gestión de órdenes abiertas | No existe | Órdenes huérfanas en exchange | **Sí** (con B-MD-008) | **P1** |
| Fees sin moneda | `Settlement.fee_currency=None` (GAP F7) | Contabilidad de fees incompleta | No | P2 |

---

## 11. Flujo actual vs flujo propuesto

### Flujo actual (VERIFIED)

```
Exchange → (solo durante submit)
              │  submit → fetch_state (fail-closed) → confirma FILLED
              ▼
            OMS._fill → fill_sync → TradeTracker + PortfolioService
              ▲
              │  (nunca) OMS.cancel → estado local CANCELLED (sin exchange)
              │
            RiskManager → sizing sobre capital_usd configurado
              ▲
              │  (nunca) no hay consulta de balance
            PortfolioService → PositionStore (posiciones desde fills propios)
```

### Flujo propuesto (conceptual)

```
Exchange (Bybit)
   │  fetch_balance / fetch_positions / cancel_order (nuevos métodos adapter)
   ▼
Portfolio (dueño patrimonial)
   │  Reconciler: compara PositionStore + balance vs exchange
   │  (gate en arranque · loop periódico · tras fill/restart/inconsistencia)
   ▼
RiskManager (consume vía port)
   │  sizing/drawdown contra saldo real (con frescura)  [B-MD-009]
   ▼
TradingEngine / OMS
   │  submit (market order) · request_cancel (estado transitorio CANCELLING)
   ▼
Exchange
   │  resultado real → OMS resuelve CANCELLED / FILLED (fill prevalece)
   ▼
OMS → _fill (si ALREADY_FILLED) → fill_sync → TradeTracker + PortfolioService
```

---

## 12. Plan de implementación conceptual (sin escribir código)

| Paso | Propuesta | BC | Esfuerzo | Requiere ADR |
|---|---|---|---|---|
| 1 | Verificar capacidad de cancelación de Bybit (CCXT `cancel_order` para market orders — algunas exchanges no permiten cancelar market ya en cola) | trading | S (investigación) | — |
| 2 | Exponer `cancel_order` en `CCXTAdapter` + `fetch_balance` | market_data (adapter) | S | — |
| 3 | Extender `OrderTransport` con `cancel()` + implementar en `_BybitTransport`/`PaperTransport` | trading | S | A-MD-004 |
| 4 | Estado transitorio `CANCELLING` + resolución CANCEL/FILL (fill prevalece) en OMS | trading | M | A-MD-004 |
| 5 | Loop `manage_open_orders` (reconciliación periódica de órdenes abiertas) | trading | M | A-MD-004 |
| 6 | `BalancePort` en portfolio + `PortfolioReconciler` + adapter `fetch_balance` en portfolio root | portfolio | M | A-MD-005 |
| 7 | RiskManager consume saldo de portfolio con frescura; gate en arranque live | trading/portfolio | M | A-MD-005 |
| 8 | Política de discrepancia (bloqueo/alerta/tolerancia configurable) + tests | portfolio/trading | M | A-MD-005 |

**Orden sugerido:** 1→2→3→4→5 (B-MD-008) y 2→6→7→8 (B-MD-009), compartiendo el paso 2 (adapter CCXT). Ambos caminos paralelos.

---

## 13. Impacto en roadmap

| Propuesta | Fase formal | Nota |
|---|---|---|
| B-MD-008 (cancel real + manage_open_orders) | **Fase 3 — Business Rules / Trading** | Toca trading/execution + risk |
| B-MD-009 (balance + reconciliación) | **Fase 3 — Business Rules / Trading** | Toca portfolio + trading/risk; el adapter CCXT del paso 2 toca market_data pero es infraestructura de soporte, no cambia la Fase 1 |
| Preparación específica para Live | Al final de Fase 3 | Gate de arranque (saldo + órdenes abiertas) antes de operar capital real |

**No se propone saltar el roadmap.** Fase 1 (market data) y Fase 2 (composition roots) siguen iguales. Los pasos 2 (adapter CCXT) son infraestructura de soporte que se ejecuta dentro de Fase 3, no adelantan trabajo de Fase 1.

---

## 14. Tracking

> `tracking.yaml` NO se edita en esta sesión. Entradas sugeridas para decisión humana posterior:

- **B-MD-008** — cancelación real de órdenes (P1, Fase 3, live blocker). Campos según esquema del archivo (id, hallazgo_informe, fase, prioridad, estado, evidencia, solucion, pruebas, adr_relacionado=A-MD-004, riesgo_residual, fecha, cadena).
- **B-MD-009** — reconciliación de balance real (P1, Fase 3, live blocker). idem, `adr_relacionado=A-MD-005`.
- Evidencia a referenciar: `docs/audits/2026-08-15-b-md-008-cancel-b-md-009-balance-audit.md` + este documento + líneas de código citadas.
- Dependencias: **ninguna con la cadena B-MD-003→002→004**. Independientes entre sí.

---

## 15. Respuestas inequívocas a las 5 preguntas

1. **¿OCM realmente cancela órdenes en el exchange?** **NO.** `OMS.cancel()` es local-only (`oms.py:300-317`) y ni siquiera se invoca. No existe `cancel_order` en port/adapter (`transport.py:96-128`, `ccxt_adapter.py`). El exchange nunca recibe la cancelación.
2. **¿OCM realmente sabe cuánto dinero tiene el exchange?** **NO.** No existe `fetch_balance` en ningún BC (grep exhaustivo). RiskManager y PortfolioService usan `capital_usd` configurado (`manager.py:394`, `portfolio_service.py:63-80`).
3. **¿Qué ocurre si OCM y el exchange dejan de estar de acuerdo?** **Divergencia silenciosa.** `CANCELLED` es terminal (`order.py:73`); no hay reconciliación periódica; `fill_sync` solo corre en FILLED (`fill_sync.py:109`). Un fill tras cancel local se pierde sin error. Para balance: OCM simplemente no se entera (no consulta).
4. **¿Dónde debe vivir la fuente de verdad?** **En Portfolio** (posiciones via `PositionStore` BC-43 + saldo derivado via reconciler). Trading/Risk **consumen** vía ports; nunca poseen estado patrimonial duplicado. El exchange es la fuente primaria; portfolio la materializa.
5. **¿Qué necesitamos solucionar antes de poner dinero real?** **Cuatro bloqueantes:** B-MD-008 (cancel real + gestión de órdenes abiertas), B-MD-009 (balance real + reconciliación), B-MD-001 (freshness), B-MD-004 (market validity). Todos P0/P1. La cadena B-MD-003→B-MD-002 los habilita pero no es prerrequisito de B-MD-008/009.

---

## Referencias

- Código: `packages/trading/execution/oms.py:300-317`, `transport.py:96-128`, `order.py:64-74`, `live_executor.py:229-261`, `fill_sync.py:109-167`, `packages/trading/bootstrap/composition_root.py:203-262`, `packages/trading/risk/manager.py:112-118,374-401`, `packages/portfolio/services/portfolio_service.py:63-80,281-284`, `packages/portfolio/bootstrap/composition_root.py:13-24`, `packages/market_data/adapters/outbound/exchange/ccxt_adapter.py:405,462`, `ocm/runtime/guard.py:87-109`
- Docs: `docs/audits/2026-08-15-b-md-008-cancel-b-md-009-balance-audit.md` (evidencia), `docs/audits/2026-08-15-b-md-008-009-diseno-conceptual.md` (este documento), `docs/plans/tracking.yaml` (no editado), ADR-0016 (reconciliación de fills), ADR-0025/0026/0027 (posición/fees/recovery)
- Referencia externa: `docs/freqtrade-develop.zip` (freqtradebot.py:1602,1635,2047,2339; exchange.py:1783,1824,1879) — solo patrón conceptual
