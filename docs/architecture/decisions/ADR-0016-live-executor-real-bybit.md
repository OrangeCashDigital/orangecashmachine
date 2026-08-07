# ADR-0016: Motor de ejecución live — LiveExecutor real (Bybit), reconciliación de fills y semántica del contador de posiciones

**Estado:** Aceptado
**Fecha:** 2026-08-06
**Bounded context(s) afectado(s):** trading (execution), portfolio (reconciliación), market_data (adapter CCXT)

## Contexto

El sistema está gobernado para no operar capital real con código simulado. Desde F1 (B-01),
`LiveExecutor.IS_STUB = True` y el arranque live aborta (guard fail-closed R1). En
`packages/trading/execution/live_executor.py:22` se documenta el estado: *"STUB con lógica real
de CCXT comentada"*. Para que OCM deje de ser "un simulador muy bien construido" (decisión del
owner, 2026-08-06) hay que construir el **motor de ejecución real**.

Requisitos del owner para F3 (transcritos del check de fases):

1. Exchange inicial **Bybit** — único, aprovechando el uso actual en OCM, con soporte paper y
   producción. **Nada de multi-exchange desde el día uno.**
2. Modo de despliegue **siempre paper → live**, nunca live directo: el comportamiento real del
   exchange solo se valida en un entorno operativo, incluso con toda la gobernanza y tests.
3. **No se toca capital real** hasta que el Engineering Health Check (F2.0) sea un gate CI real
   (hecho, `397459e`), esté el ADR-0016 commiteado, y la reconciliación + kill switch estén
   probados con tests.
4. ADR-0011 (rebalance) **se mueve a F4**: no bloquea la capacidad de enviar y reconciliar
   órdenes. El objetivo de F3 es que el **motor opere de forma segura y consistente**.

El `ExecutionGuard` (kill switch) ya existe en `ocm/runtime/guard.py` con `trigger(reason)`,
`should_stop()`, `stop_reason` y `record_error()` (breaker por errores consecutivos). F1 (B-03)
ya fijó la semántica del contador `_open_positions` como **held-position** (BUY abre, SELL cierra).

## Alternativas evaluadas

1. **Multi-exchange desde el día uno.** Descartada. Multiplica la superficie de riesgo (credenciales,
   param/orderId, modelo de fills) sin capital real aún; contradice el requisito "Bybit único".
   El contrato de adaptador (`ExchangeAdapter`) ya permite añadir exchanges después sin tocar OMS.
2. **Live directo (sin pasar por paper).** Descartada. El comportamiento real del exchange
   (timeouts, partial fills, HTTP 429/rate limits, reconnect) solo se valida operando; saltarse
   paper arriesga capital con una primera ejecución no contrastada. Regla: **paper → live**.
3. **Reconciliación ad-hoc al final de cada orden.** Descartada. Deja un estado interno divergente
   durante el vuelo; ante un fill que el WS no notifica, el contador se descuadra. El motor debe
   reconciliar **por orden** con el exchange (fetch del estado real).
4. **LiveExecutor real síncrono dentro de `execute()` + reconciliación por contador + kill switch
   de OMS. Elegida.** Precisamente porque el `OrderExecutor` Protocol de `oms.py:53` ya devuelve
   `bool`, extendemos con reconciliación explícita y el contador held-position ya gobernado.

## Decisión

1. **Exchange inicial: Bybit**, único, a través del `CCXTAdapter` (adapter de `market_data`); modo
   de ejecución gateado por config (`paper` | `live`). Nunca live directo: el sistema arranca en
   `paper`; para `live` se exige que el health check y las pruebas de reconciliación hayan
   pasado y se lance el `--mode live` explícito.
2. **`LiveExecutor.IS_STUB` pasa a `False`** SOLO cuando la implementación real de `_submit`
   está commiteada con tests y activos `R9`/`R10`. En paper/partial la lógica real se sustituye
   por envío simulado a un `PaperCCXTAdapter`, pero el flujo orden→fill→estado es idéntico.
3. **Reconciliación de fills:** tras `_submit`, el motor consulta el estado del exchange
   (fetch_order / fills) y reconcilia contra `Order`/contador interno. Si el estado del exchange
   no confirma el fill, NO se da por filled internamente (política fail-closed: sin fill
   confirmado, sin countdown de posición).
4. **Semántica del contador (held-position, de B-03):** BUY abre; SELL fill cierra. La
   reconciliación actualiza `_open_positions` desde el resultado confirmado del exchange, no
   desde una suposición local.
5. **Kill switch:** se usa `ExecutionGuard` (manual `trigger(reason)` + breaker por errores
   consecutivos `record_error`) ANTES de cada submit. Un kill active aborta la orden y detiene el
   flujo live. Solo se libera por decisión explícita tras intervención humana.
6. **Reintentos con backoff** ante errores del exchange (timeouts, 5xx, 429): reintento
   acotado por `max_retries` configurable y ventana de backoff, respetando idempotencia
   (clientOrderId).

## Justificación técnica

- **Bybit único** reduce el rango de credenciales, esquemas de id/leveraged de solo un exchange;
  `pipeline_factory` y `ExchangeAdapter` ya están pensados para intercambiar el adaptador sin
  tocar `OMS` (adherido Clean/Hexagonal).
- **paper→live obligatorio** alinea con la regla suprema (no degradar artefactos normativos):
  el modelo de ejecución es el mismo, el transporte es simulada. Entre el papel y el dinero real
  solo cambia el adapter de transporte; el OMS, contador y reconciliación son idénticos.
- **Reconciliación consulta-exchange** es la única manera de garantizar *consistencia* entre el
  estado del mundo (exchange) y el estado local sin depender de la entrega fiable de eventos de
  fill (push engines no son de fiar para el audit trail real de capital).
- CCXT asíncrono con `asyncio.wait_for` da timeouts deterministas; `ExecutionGuard.record_error`
  provee el breaker para que un exchange caído no llene errores infinitos (kill switch por
  degradación).
- El contador held-position ya está resuelto (B-03); ADR-0016 solo le agrega la fuente de
  verdad del exchange en el fill.

## Consecuencias

- **Más fácil:** el motor pasa a ser un componente reemplazable (transport adapter); pruebas
  de reconciliación aisladas del adapter; un cambio de exchange es un adapter nuevo sin tocar OMS.
- **Deuda aceptada:** la reconciliación por polling (fetch_order) implica un ciclo de consulta
  que añade latencia vs un push stream. Se mitigará en F4 con Observabilidad (B-17) para medir
  el coste real.
- **Contratos BC-NN que lo hacen cumplir:**
  - `BC-29` — schemas wire en `shared.kafka` (no se inventan payloads nuevos sin pasar por ahí).
  - `BC-44` — layering portfolio (reconciliación no salta capas).
  - `BC-09` / guards — domain sigue framework-agnostic (CCXT queda en adapter, no en domain).

## Referencias

- Código: `packages/trading/execution/oms.py` (OMS + `OrderExecutor` Protocol), `.../live_executor.py`
  (IS_STUB), `ocm/runtime/guard.py` (`ExecutionGuard` kill switch), `packages/market_data/adapters/outbound/exchange/ccxt_adapter.py`.
- Hallazgos: H-01 (stub devolvía éxito sin enviar), H-03 (contador), H-19/H-22 (vías F3),
  B-01, B-03, B-12.
- ADRs relacionados: ADR-0003 (CR trading), ADR-0011 (rebalance → movido a F4), ADR-0013
  (modelo de ingestión). Próximo: ADR-0017 (estado de posición, F4).