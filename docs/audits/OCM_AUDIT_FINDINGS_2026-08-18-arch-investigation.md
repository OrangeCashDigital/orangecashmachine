# OCM — AUDIT FINDINGS REGISTER (Investigación Arquitectónica: SUBMITTED & Órdenes Huérfanas)

**Ejecución de auditoría:** 2026-08-18 (baseline `bee9fb5a3917c32fcc81fcc81fa5177ce0e57283`)
**Fuente primaria:** `docs/audits/AUDIT_OCM_ARCH_INVESTIGATION_SUBMITTED_ORPHAN_2026-08-18.md`
**Estado de este registro:** OPEN (se actualiza conforme los findings se resuelven o se toma decisión humana)
**Relación con el registro canónico:** registro complementario. El registro canónico `OCM_AUDIT_FINDINGS_2026-08-18.md` (16 findings de producto) permanece intacto; este registro documenta exclusivamente la investigación arquitectónica del ciclo de vida SUBMITTED / órdenes huérfanas (B-MD-008, paso 5 de ADR-0029). Deduplicación (L1): F-SUB-02 y F-SUB-03 revalidan (no duplican) el gap G2 y la decisión 5 de ADR-0029 y el finding F-ARCH-02 del registro canónico.

Resumen: CONTRADICCIÓN 1 · REVALIDADO 1 · RECOMENDACIÓN 2 · **Total: 4**.

Clasificación (taxonomía del protocolo de auditoría de OCM):
- NUEVO: 0
- REVALIDADO: 1 — F-SUB-02
- REGRESIÓN: 0
- CERRADO: 0
- CONTRADICCIÓN: 1 — F-SUB-01
- RECOMENDACIÓN: 2 — F-SUB-03, F-SUB-04
- NO_VERIFICADO: 0

---

## F-SUB-01 — SUBMITTED es un estado test-only: contradice la premisa del paso 5 de ADR-0029

Severity: HIGH
Status: OPEN
Classification: CONTRADICCIÓN
Control: Execution — Order Lifecycle (B-MD-008 paso 5)
Source: inspección de código (oms.py, live_executor.py) + tests + ADR-0029

Evidence:
- `packages/trading/execution/oms.py:299-303` — `OMS.submit()` resuelve SIEMPRE de forma incondicional a `_fill`/`_reject`; no deja una orden persistente en `SUBMITTED`
- `packages/trading/execution/live_executor.py:209-227` — `LiveExecutor._submit` devuelve `accepted=True` solo con `confirmed_filled`; `_reconcile` → `None` si no confirma fill (fail-closed)
- `tests/trading/test_oms_cancel_lifecycle.py:64-73` — los 15 tests de cancel inyectan `SUBMITTED` artificialmente vía `_inject` (test-only state, no alcanzable por el flujo real)
- ADR-0029 (decisión 5) asume un caller/loop que gestiona órdenes persistentes en `SUBMITTED`/`CANCELLING`; el modelo síncrono deliberado de ADR-0016 no produce ese estado persistente
- grep `fetch_open_orders` = 0 hits en todo el repo

Impact:
- El paso 5 de B-MD-008 (manage_open_orders) no puede recorrer entradas reales en `SUBMITTED`: el estado que ADR-0029 asume como base del loop no es producible por el flujo de ejecución actual. Contradicción ADR↔código que bloquea la implementación del paso 5 tal como está diseñado.

Required human decision:
- D-ARC-1 (BLOCKING): elegir el modelo para el paso 5 — A) submit asíncrono; B) submit síncrono + reconciliación contra exchange; C) leave-as-is. Recomendación: B (ver F-SUB-03).

Recommended remediation:
- Adoptar la Alternativa B (síncrono + reconciliación) con enmienda a ADR-0029 que documente la contradicción y redefina el mecanismo de recuperación de huérfanas.

Verification required:
- Tras decisión humana: enmienda ADR-0029 + implementación del mecanismo elegido + test de integración con confirmación de exchange.

Traceability:
- Tracking: B-MD-008 (PENDIENTE) · ADR: ADR-0029, ADR-0016 · Implementation: NOT_TRACED · Tests: NOT_TRACED · CI: NOT_TRACED · Evidence: oms.py:299-303, live_executor.py:209-227, test_oms_cancel_lifecycle.py:64-73 · Closure: OPEN

---

## F-SUB-02 — Órdenes huérfanas indetectables: sin fetch_open_orders ni exchange_order_id

Severity: HIGH
Status: OPEN
Classification: REVALIDADO
Control: Execution — Order Recovery / Reconciliation
Source: inspección de código (order.py, transport.py, composition_root) + ADR-0029 gap G2 + ADR-0027

Evidence:
- `packages/trading/execution/order.py:64-90` — `Order` no tiene `exchange_order_id` (imposible correlacionar con el exchange)
- `packages/trading/execution/transport.py:53-139` — el port `OrderTransport` no declara `fetch_open_orders`
- `packages/trading/bootstrap/composition_root.py:203-334` — `map_ccxt_order` mapea `"open" → SUBMITTED`; sin consulta de órdenes abiertas del exchange
- ADR-0029 documenta explícitamente el gap G2: una orden abierta huérfana nunca se limpia
- ADR-0027 — journal de órdenes VOLATILE, sin recovery de órdenes abiertas
- grep `fetch_open_orders` = 0 hits (sin capacidad de descubrir la verdad del exchange)

Impact:
- Una orden aceptada por el exchange cuyo estado local diverga (timeout de create_order, caída del proceso, reinicio) queda huérfana sin mecanismo de detección ni limpieza. Riesgo financiero en live: posición abierta no gestionada.

Required human decision:
- D-ARC-2 (NON_BLOCKING): ampliar `Order` con `exchange_order_id` y `OrderTransport` con `fetch_open_orders` como habilitadores de reconciliación.

Recommended remediation:
- Implementar `fetch_open_orders` en el adaptador CCXT y reconciliación periódica (Alternativa B, F-SUB-03).

Verification required:
- Grep `fetch_open_orders` > 0; test de integración con sandbox/mock que devuelva la orden abierta.

Traceability:
- Tracking: B-MD-008 (PENDIENTE) · ADR: ADR-0029, ADR-0027 · Implementation: NOT_TRACED · Tests: NOT_TRACED · CI: NOT_TRACED · Evidence: order.py:64-90, transport.py:53-139, composition_root.py:203-334, ADR-0029 gap G2 · Closure: OPEN

---

## F-SUB-03 — Recomendación: Alternativa B (submit síncrono + reconciliación contra exchange)

Severity: MEDIUM
Status: OPEN
Classification: RECOMENDACIÓN
Control: Execution — Order Recovery / Reconciliation (diseño)
Source: investigación F4/F5 (CCXT, NautilusTrader, Hummingbot, Freqtrade, Bybit, Ember, MatrixTrak, StaxInvesting) — conocimiento externo, no norma

Evidence:
- CCXT issue #7411 / #2698 + Manual: timeout de `create_order` puede implicar orden aceptada; recuperar consultando `fetchOpenOrders`/`fetchOrders`/`fetchMyTrades` por `clientOrderId`
- NautilusTrader: 3 resultados de reconciliación (definitive local failure / definitive result / unknown live outcome → mantiene in-flight, no inventa reject); reconciliation de arranque + continua; safeguard single-order probe
- Hummingbot: `lost_orders` (alerta + FAILED, nunca CANCELLED silencioso); `restore_tracking_states`
- Freqtrade: `manage_open_orders` sobre BD SQLAlchemy; `cancel_order_with_result` con fallback fetch
- Bybit: `orderLinkId`/clientOrderId; market order = IOC
- Ember: "más seguro mantener la orden abierta en casos cuestionables" (contraejemplo al auto-cancel)
- MatrixTrak: reconcile antes de operar ("Exchange always wins")
- StaxInvesting/Nubra/Limitless: idempotency key durable ANTES del envío; on timeout query broker, nunca resubmit

Impact:
- Sin la Alternativa B, el paso 5 de B-MD-008 no tendrá mecanismo de detección de huérfanas y la contradicción F-SUB-01 permanece sin solución.

Required human decision:
- D-ARC-1 (BLOCKING): aprobar la Alternativa B como modelo del paso 5 (síncrono + reconciliación, coherente con ADR-0016).

Recommended remediation:
- Añadir `fetch_open_orders` (adaptador CCXT) + `Order.exchange_order_id` + gate de arranque que reconcilie órdenes abiertas antes de operar; adoptar la política "mantener in-flight en caso de unknown" (NautilusTrader/Ember) con alerta (Hummingbot `lost_orders`).

Verification required:
- Decisión humana D-ARC-1; luego implementación + tests de integración.

Traceability:
- Tracking: B-MD-008 (PENDIENTE) · ADR: ADR-0029 · Implementation: NOT_TRACED · Tests: NOT_TRACED · CI: NOT_TRACED · Evidence: CCXT #7411/#2698, NautilusTrader reconciliation, Hummingbot lost_orders, Freqtrade manage_open_orders, Bybit orderLinkId/IOC · Closure: OPEN

---

## F-SUB-04 — Recomendación: actualizar tracking.yaml B-MD-008 (pasos 2-4 cerrados; paso 5 bloqueado)

Severity: LOW
Status: OPEN
Classification: RECOMENDACIÓN
Control: Governance — tracking accuracy
Source: revalidación de esta sesión + tracking.yaml

Evidence:
- Pasos 2-4 del diseño conceptual B-MD-008 verificados en esta sesión: 28 tests dirigidos PASS + suite completa 1200 PASS
- `docs/plans/tracking.yaml:2246-2300` — B-MD-008 `estado: PENDIENTE` y cadena `implementacion/tests/ci/cierre` en PENDIENTE, sin reflejar los pasos 2-4 cerrados ni el bloqueo del paso 5 por F-SUB-01
- `docs/audits/2026-08-15-b-md-008-009-diseno-conceptual.md` define los pasos 1-8 y los gaps

Impact:
- La cadena de trazabilidad del hallazgo queda desalineada con la realidad verificada (pasos 2-4 HECHO), lo que puede inducir a re-auditar lo ya verificado.

Required human decision:
- D-ARC-3 (NON_BLOCKING): actualizar tracking.yaml B-MD-008 reflejando pasos 2-4 cerrados y paso 5 bloqueado por F-SUB-01 (tras D-ARC-1).

Recommended remediation:
- Tras decisión humana D-ARC-1, actualizar la cadena `implementacion` de B-MD-008 con referencia a esta investigación y al estado del paso 5.

Verification required:
- `docs/plans/tracking.yaml` refleja la actualización; `uv run python scripts/engineering_health_check.py` sigue PASS.

Traceability:
- Tracking: B-MD-008 (PENDIENTE) · ADR: NOT_TRACED · Implementation: NOT_TRACED · Tests: NOT_TRACED · CI: NOT_TRACED · Evidence: tracking.yaml:2246-2300, design doc pasos 1-8 · Closure: OPEN