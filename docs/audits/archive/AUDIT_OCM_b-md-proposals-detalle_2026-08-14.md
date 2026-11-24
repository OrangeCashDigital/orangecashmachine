# Detalle de propuestas B-MD-001 a B-MD-007 — para decisión humana

**Fecha:** 2026-08-14
**Fuente:** `docs/audits/2026-08-14-market-data-deep-audit.md` (hallazgos F-MD-001..F-MD-010)
**Propósito:** detalle operativo de cada propuesta para decidir cuáles aprobar **antes** de tocar `docs/plans/tracking.yaml`.
**Restricciones:** solo lectura/análisis. Cero cambios de código, cero cambios a tracking.yaml.

> Convención de prioridad usada aquí (alineada con el prompt de la auditoría, §13):
> **P0** = correctness/safety (puede provocar pérdida financiera, posición/órdenes/contabilidad incorrecta) ·
> **P1** = live readiness (bloquea operar live con seguridad) ·
> **P2** = reliability (recuperación/observabilidad/robustez) ·
> **P3** = performance · **P4** = nice-to-have.

---

## Tabla resumen

| ID | Hallazgo | Prioridad | ADR | Archivos que tocaría | Riesgo | Dependencias | Esfuerzo |
|---|---|---|---|---|---|---|---|
| **B-MD-001** | F-MD-001 — sin freshness/staleness | **P1** | No (extiende config + métricas) | `shared/kafka/schemas/_base.py`, `ocm/runtime/guard.py` (opcional), config | **Medio** | Ninguna | **S** |
| **B-MD-002** | F-MD-003 — BookBuilder consumer de orderbook.raw | **P0** | **Sí — A-MD-002** | `packages/market_data/infrastructure/bootstrap/`, `shared/kafka/topics.py` (consumidor), `shared/contracts/boundaries.py` (port) | **Alto** | B-MD-003 (sequence) | **L** |
| **B-MD-003** | F-MD-004 — `sequence` en wire order book | **P1** | **Sí — A-MD-001** | `shared/kafka/schemas/orderbook.py`, `adapters/inbound/websocket/cryptofeed_orderbook_stream.py`, `adapters/outbound/kafka/orderbook_producer.py` | **Medio** | Ninguna (precede/permite B-MD-002) | **S** |
| **B-MD-004** | F-MD-006 — pre-submit market validity en ejecución | **P0** | **Sí — A-MD-003** | `packages/trading/risk/manager.py`, `packages/trading/bootstrap/composition_root.py`, config trading | **Alto** | B-MD-001 (freshness) + B-MD-005 (instrumentos) | **M** |
| **B-MD-005** | F-MD-007 — instrumentos/límites/precisión como datos | **P1** | No (extiende VO de dominio) | `packages/market_data/domain/value_objects/exchange_quirks.py`, `adapters/outbound/exchange/ccxt_adapter.py` | **Medio** | Ninguna | **M** |
| **B-MD-006** | F-MD-008 — pipeline `trades_stream` huérfano | **P2** | No (chore) | `packages/market_data/infrastructure/bootstrap/pipeline_factory.py`, `application/source_manager.py` | **Bajo** | Ninguna | **S** |
| **B-MD-007** | F-MD-002 — `received_at`/`processed_at` en envelope | **P2** | **Sí — A-MD-001** (mismo ADR que B-MD-003) | `shared/kafka/schemas/_base.py`, `shared/kafka/schemas/orderbook.py`, `shared/kafka/schemas/trades.py` | **Medio** | Ninguna | **S** |

**Esfuerzo relativo (S/M/L comparado entre las 7):** S = un archivo + schema/check aislado (001, 003, 006, 007); M = cambio de contrato + config + tests (004, 005); L = componente nuevo (consumer) + port inter-BC + ADR (002).

---

## B-MD-001 — Contrato de frescura (freshness/staleness) de market data

**1. ID y nombre:** B-MD-001 · **Freshness/staleness contract**.

**2. Hallazgo que resuelve:** F-MD-001 — `BasePayload` solo tiene `event_id`/`schema_version`/`occurred_at`; no hay `last_seen` por símbolo, heartbeat, ni umbral de staleness. La calidad se mide solo sobre datos persistidos (`application/quality/data_quality.py`), no sobre la frescura del feed en vivo. OCM no puede responder "¿qué tan vieja es esta información de mercado?".

**3. Prioridad:** **P1 (live readiness).** Sin umbral de staleness, el sistema no puede distinguir un mercado congelado de uno activo antes de operar capital real. No bloquea el desarrollo de Fase 1 (market_data), pero **sí bloquea la operación live segura**: es prerrequisito de B-MD-004.

**4. ADR:** **No requiere ADR nuevo.** Extiende configuración y observabilidad (métricas/alertas); no cambia el contrato canónico de wire ni las fronteras de BC. Opcionalmente reutiliza `ExecutionGuard` (ya existe) para disparar stop si el feed excede staleness.

**5. Archivos que tocaría:**
- `config/base.yaml` y `config/production.yaml` — umbrales `staleness.max_age_s` por símbolo/feed.
- `packages/market_data/application/quality/pipeline.py` (o módulo de control-plane nuevo) — cálculo de `last_seen` y alerta.
- `ocm/runtime/guard.py` — opcional: trigger de kill switch ante staleness (ya existe el mecanismo).
- `shared/kafka/metrics.py` (o `KafkaMetrics`) — métrica `ocm_market_data_last_seen_age_ms` / `ocm_market_data_stale_total`.

**6. Riesgo de implementación:** **Medio.** Bajo riesgo de contrato (no toca wire), pero introduce estado/métrica nueva en el pipeline de calidad y exige decidir dónde vive el control-plane de frescura (¿market_data? ¿shared?). Riesgo de config: si el umbral es muy agresivo, alertas espurias; si muy laxo, inútil.

**7. Dependencias:** Ninguna obligatoria. **Es prerrequisito de B-MD-004** (sin freshness no se puede rechazar precio stale). Orden recomendado: B-MD-001 → B-MD-004.

**8. Esfuerzo:** **S** (1–2 archivos + config + métrica + tests).

---

## B-MD-002 — BookBuilder consumer de `orderbook.raw`

**1. ID y nombre:** B-MD-002 · **BookBuilder/MarketState consumer**.

**2. Hallazgo que resuelve:** F-MD-003 — grep de `TOPIC_ORDERBOOK_RAW` en `packages/`+`apps/` = solo productor (`adapters/outbound/kafka/orderbook_producer.py`); `GROUP_BOOK_BUILDER`/`TOPIC_BOOK_DELTA`/`TOPIC_BOOK_SNAPSHOT` = 0 usos productivos. No hay BookBuilder/MarketState/MicropriceEngine. El order book se produce pero jamás se consume; sin best_bid/ask/mid/spread/depth en runtime. ADR-0023 + tracking B-25 difieren el gap/DLQ product-side hasta que exista consumidor.

**3. Prioridad:** **P0 (correctness/safety).** El order book es la única fuente de bid/ask/mid/spread/depth — son los datos de mercado más relevantes para ejecución (slippage, spread, liquidez). Sin consumer, el capital de la ingesta de order book está producido y descartado, y B-MD-004 (pre-submit market validity) no tiene de dónde leer spread/liquidez. **Bloquea cualquier mejora de ejecución basada en microstructure.**

**4. ADR:** **Sí — A-MD-002** ("BookBuilder/MarketState en BC market_data como primer consumidor de `orderbook.raw` + nuevo port `MarketDataView` expuesto a trading/portfolio"). Necesita ADR porque introduce un componente nuevo + un port inter-BC.

**5. Archivos que tocaría:**
- `packages/market_data/infrastructure/bootstrap/composition_root.py` — registrar el consumer.
- `packages/market_data/infrastructure/bootstrap/pipeline_factory.py` — pipeline/builder del consumer.
- `shared/kafka/topics.py` — grupo de consumer `GROUP_BOOK_BUILDER` (reservado en B-25).
- `shared/kafka/schemas/orderbook.py` — lectura (consume snapshot+delta).
- `shared/contracts/boundaries.py` — nuevo port `MarketDataView` (best bid/ask, mid, spread, depth, last_seen).
- `packages/market_data/domain/value_objects/order_book.py` — reducer/reconstructor (reutiliza VOs existentes).

**6. Riesgo de implementación:** **Alto.** Componente nuevo de runtime (consumer de Kafka en market_data), primer consumidor de un tópico sin consumidores hasta hoy, y expone un port inter-BC nuevo (trading lo consumiría). Implica la lógica de reconstrucción snapshot+delta con resync por secuencia, que es precisamente donde pueden introducirse bugs sutiles (books cruzados, resync incorrecto). Mitigación: implementar tras B-MD-003 (sequence) y con tests de reconstrucción.

**7. Dependencias:** **Depende de B-MD-003** (sequence en el wire) para poder validar continuidad y hacer resync correcto. Sin sequence, la reconstrucción es frágil. Orden: B-MD-003 → B-MD-002. B-MD-004 depende de B-MD-002 (spread/liquidez).

**8. Esfuerzo:** **L** (consumer nuevo + reducer + port inter-BC + ADR + tests de reconstrucción).

---

## B-MD-003 — `sequence` en wire del order book

**1. ID y nombre:** B-MD-003 · **Sequence field en schema order book (v2)**.

**2. Hallazgo que resuelve:** F-MD-004 — `cryptofeed_orderbook_stream.py:158-161` captura `book.sequence_number`, pero el payload de wire (`shared/kafka/schemas/orderbook.py`) no lo transporta → imposible gap-detection por secuencia en deltas. Es exactamente el paso previo documentado en B-25 ("sequence aditivo en schema v2").

**3. Prioridad:** **P1 (live readiness) / P0 en la cadena de microstructure.** Aislado es P1; es **prerrequisito estructural de B-MD-002** (P0). Sin sequence, el BookBuilder no puede detectar deltas perdidas y el book reconstruido puede ser incorrecto (riesgo de spread/mid erróneos usados en ejecución).

**4. ADR:** **Sí — A-MD-001** ("evolución del contrato canónico de market data: añadir `sequence` (order book) y `received_at`/`processed_at` (envelope) con compatibilidad `SCHEMA_VERSION`"). Modifica contrato de wire → requiere ADR. B-MD-003 y B-MD-007 comparten el mismo ADR (A-MD-001).

**5. Archivos que tocaría:**
- `shared/kafka/schemas/orderbook.py` — añadir campo `sequence: Optional[int]` + `SCHEMA_VERSION` bump.
- `packages/market_data/adapters/inbound/websocket/cryptofeed_orderbook_stream.py` — propagar `book.sequence_number`.
- `packages/market_data/adapters/outbound/kafka/orderbook_producer.py` — incluir `sequence` en el payload.
- `shared/kafka/schemas/trades.py` — (opcional, si también se quiere sequence en trades).
- `tests/market_data/test_external_wire.py` — roundtrip del nuevo campo.

**6. Riesgo de implementación:** **Medio.** Cambio de contrato de wire (schema_version bump, backward-compatible por diseño — campo opcional). Riesgo bajo de regresión si se respeta la compatibilidad; riesgo de diseño si no se define bien la semántica de sequence (¿por exchange? ¿por símbolo? ¿global?). Sin consumer que consuma sequence hoy, es aditivo sin impacto en runtime.

**7. Dependencias:** Ninguna obligatoria. **Precede a B-MD-002** (secuencia obligatoria: B-MD-003 → B-MD-002).

**8. Esfuerzo:** **S** (2–3 archivos + schema + roundtrip tests).

---

## B-MD-004 — Pre-submit market validity en ejecución

**1. ID y nombre:** B-MD-004 · **Market validity checks pre-submit (RiskManager)**.

**2. Hallazgo que resuelve:** F-MD-006 — `RiskManager` (`packages/trading/risk/manager.py`) solo valida `min/max_order_usd` sobre capital×size_pct y `max_open_positions`; no consulta market data. `LiveExecutor` → `_BybitTransport` (CCXT **market order**) sin chequeo de spread/stale/liquidez. Freqtrade sí lo hace: `raise PricingError("Could not determine entry price.")` (`docs/Untitled_2.py:1157`), `get_min_pair_stake_amount` (:1183), `price_to_precision`. La conexión **Market Data → Risk → Execution no existe** (§10 de la auditoría).

**3. Prioridad:** **P0 (correctness/safety).** La ejecución actual puede enviar una orden de mercado bajo condiciones de mercado que cambiaron desde que se generó la señal (señal de gold `last close`, posiblemente vieja). Un mercado congelado o con bid/ask desplazado → slippage/orden desfavorable con capital real. **Bloquea live seguro.** Es el gap más peligroso para operar con capital real.

**4. ADR:** **Sí — A-MD-003** ("política de fallo de ejecución ante market data ausente/stale: fail-closed por defecto; modo degrade explícito y auditable"). Es política de riesgo → ADR.

**5. Archivos que tocaría:**
- `packages/trading/risk/manager.py` — nuevo `validate_market(market_view)` con checks: precio stale (> umbral), bid/ask presentes, spread ≤ máx, precio señal vs mid/último (desviación), min order size, precisión.
- `packages/trading/risk/models.py` (`RiskConfig`) — umbrales `market.stale_max_age_s`, `market.max_spread_pct`, `market.max_price_deviation_pct`.
- `packages/trading/bootstrap/composition_root.py` — inyectar `MarketDataView` (de B-MD-002) al RiskManager / ejecutor.
- `packages/trading/execution/live_executor.py` — pre-submit check + fail-closed.
- `packages/trading/execution/transport.py` — port `OrderTransport` (no cambia firma; el check vive en Risk/Executor).
- `config/` trading — umbrales.

**6. Riesgo de implementación:** **Alto.** Toca trading/execution (zona sensible de capital real) y añade dependencia del port `MarketDataView` (que solo existirá tras B-MD-002). Riesgo de falsos rechazos (mercado momentáneamente spread alto) → órdenes legítimas bloqueadas; riesgo de configuración de umbrales. Requiere tests exhaustivos por check y revisión de las invariantes INV-08/INV-10 (no usar `signal.price` como precio de ejecución).

**7. Dependencias:** **B-MD-001** (freshness/staleness — el precio stale se mide contra el umbral) + **B-MD-005** (precision/min order size del instrumento) + **B-MD-002** (spread/liquidez desde el book). Orden obligatorio: B-MD-001 → B-MD-005 → B-MD-003 → B-MD-002 → B-MD-004. Es la propuesta con la cadena de dependencias más larga.

**8. Esfuerzo:** **M** (RiskManager + config + wiring + tests por check).

---

## B-MD-005 — Instrumentos/límites/precisión como datos de mercado

**1. ID y nombre:** B-MD-005 · **Instrument metadata (precision/min_amount/tick) para ejecución**.

**2. Hallazgo que resuelve:** F-MD-007 — `ExchangeQuirks` (`packages/market_data/domain/value_objects/exchange_quirks.py`) cubre paginación REST (backward_pagination, requires_end_at, origin_fallback_date) pero **no** precision/min_amount/tick/min_cost. `ccxt_adapter.py` carga `load_markets` (con cache), pero esos metadatos no se exponen como contrato de dominio. Freqtrade: `get_precision_amount/price`, `precisionMode` (`docs/Untitled_2.py:390-398,1052-1055`).

**3. Prioridad:** **P1 (live readiness).** Sin precision/min order size del instrumento no se puede validar una orden contra los límites reales del exchange (una orden con cantidad por debajo del mínimo o precisión incorrecta será rechazada o redondeada por el exchange). Prerrequisito de B-MD-004.

**4. ADR:** **No requiere ADR nuevo.** Extiende un VO de dominio existente (`ExchangeQuirks`) y su mapeo en el adapter CCXT; no cambia contrato de wire ni fronteras de BC.

**5. Archivos que tocaría:**
- `packages/market_data/domain/value_objects/exchange_quirks.py` — añadir `price_precision`, `amount_precision`, `min_amount`, `min_cost`, `tick_size` (con defaults y SSOT por exchange).
- `packages/market_data/adapters/outbound/exchange/ccxt_adapter.py` — extraer de `load_markets()` los metadatos del símbolo.
- `packages/market_data/adapters/outbound/exchange/base.py` — exponer en el contrato.
- `packages/trading/risk/manager.py` — (consumidor, en B-MD-004) o `shared/contracts/boundaries.py` si se expone como dato canónico.
- `tests/` — mapeo de metadatos por exchange.

**6. Riesgo de implementación:** **Medio.** Los metadatos de `load_markets` de CCXT son por-exchange y a veces inexactos/incompletos; requiere curaduría por exchange (Bybit, KuCoin). Riesgo bajo de contrato (no toca wire), riesgo de datos si el mapeo es incorrecto (orden inválida en live).

**7. Dependencias:** Ninguna obligatoria. **Prerrequisito de B-MD-004.** Independiente de B-MD-002/B-MD-003.

**8. Esfuerzo:** **M** (VO + adapter CCXT + curaduría por exchange + tests).

---

## B-MD-006 — Resolver pipeline `trades_stream` huérfano

**1. ID y nombre:** B-MD-006 · **Cablear o eliminar `trades_stream`**.

**2. Hallazgo que resuelve:** F-MD-008 — `_build_trades_stream` (`infrastructure/bootstrap/pipeline_factory.py:86`) construye `TradesSourceManager`s (cursor monotónico + dedup LRU cross-source), pero: (a) no conforman `PipelineTriggerPort` (no tienen `.run()`, ver `application/use_cases/pipeline_orchestrator.py` que exige `pipeline.run()`); (b) grep de `trades_stream` en `apps/`/`config/` = ∅ → nunca invocado. `TradesPipeline` REST (backfill → Silver) sí está vivo.

**3. Prioridad:** **P2 (reliability).** No bloquea live (el backfill REST a Silver funciona). Es deuda técnica: capacidad WS trades construida pero sin consumidor → o se cablea (trades live a Silver/Kafka) o se elimina el builder muerto. Mantenerlo sin cablear es código muerto que confunde y puede sugerir falsamente que hay trades live.

**4. ADR:** **No.** Chore de limpieza/cableado interno de market_data; sin cambio de contrato.

**5. Archivos que tocaría:**
- `packages/market_data/infrastructure/bootstrap/pipeline_factory.py` — o bien conectar `_build_trades_stream` a un consumidor, o eliminar el builder.
- `packages/market_data/application/source_manager.py` — si se cablea: convertir `TradesSourceManager` en iterable consumido por un writer (Silver/Kafka).
- `apps/app/cli/` — registrar el pipeline si se decide cablear.
- `tests/` — cobertura del path elegido.

**6. Riesgo de implementación:** **Bajo.** No toca la ruta de producción activa (backfill REST). Si se elimina, riesgo nulo (dead code). Si se cablea, riesgo bajo (reutiliza `TradesSourceManager` ya probado, con dedup/fallback REST).

**7. Dependencias:** Ninguna. Decisión previa humana: ¿cablear (trades live) o eliminar? Recomendación de esta auditoría: **cablear** si el roadmap quiere trades WS live para research/paridad (feed_orchestrator `mode=dual` ya lo anticipa); **eliminar** si no es prioridad (evita código muerto).

**8. Esfuerzo:** **S.**

---

## B-MD-007 — `received_at`/`processed_at` en envelope del wire

**1. ID y nombre:** B-MD-007 · **Timestamps de recepción/proceso en wire**.

**2. Hallazgo que resuelve:** F-MD-002 — `DomainEvent` (`packages/market_data/domain/events/_base.py`) y `BasePayload` (`shared/kafka/schemas/_base.py`) tienen solo `occurred_at` (momento de creación del evento). No hay `received_at` (cuándo el consumer lo recibió) ni `processed_at` (cuándo se procesó) → no se puede medir latencia end-to-end de ingestión ni detectar eventos viejos en consumo. Vinculado a G-02/G-09 (sin métricas de observabilidad live).

**3. Prioridad:** **P2 (reliability/observabilidad).** No bloquea live (no es safety), pero sin latencia medible es difícil diagnosticar Kafka lag, backpressure o procesamiento lento. Mejora significativa de observabilidad operacional.

**4. ADR:** **Sí — A-MD-001** (mismo ADR que B-MD-003: evolución del contrato canónico con `SCHEMA_VERSION` bump). Modifica el contrato de wire.

**5. Archivos que tocaría:**
- `shared/kafka/schemas/_base.py` — añadir `received_at: Optional[str]`, `processed_at: Optional[str]` al `BasePayload` (aditivo, backward-compatible).
- `shared/kafka/schemas/orderbook.py`, `shared/kafka/schemas/trades.py` — heredar automáticamente.
- Consumidores (e.g. `QualityConsumer`, `KafkaBronzeWriter`) — poblarlos en recepción/proceso.
- `tests/market_data/test_external_wire.py` — roundtrip.

**6. Riesgo de implementación:** **Medio.** Cambio de contrato de wire (bump `SCHEMA_VERSION`), pero aditivo y sin romper `occurred_at`. Riesgo de diseño: definir semántica de los tres timestamps (dónde se puebla cada uno) para no crear ambigüedad.

**7. Dependencias:** Ninguna. Se agrupa con B-MD-003 bajo el mismo ADR (A-MD-001), lo que sugiere implementarlos juntos en un único bump de schema.

**8. Esfuerzo:** **S.**

---

## Recomendación de orden de aprobación/implementación

**Coherente con el roadmap ya decidido (Fase 1 market_data → Fase 2 Composition Roots → Fase 3 estrategias):**

| Orden | Propuesta | Fase roadmap | Motivo |
|---|---|---|---|
| 1 | **B-MD-003** (sequence en wire) | Fase 1 | Aditivo, bajo riesgo, desbloquea la cadena de microstructure. Cabe dentro de Fase 1 market_data. |
| 2 | **B-MD-001** (freshness) | Fase 1 | Aditivo, bajo riesgo, control-plane. Cabe en Fase 1. |
| 3 | **B-MD-005** (instrumentos/precisión) | Fase 1/2 | Extiende VO de dominio market_data; alimenta ejecución en Fase 2. |
| 4 | **B-MD-002** (BookBuilder) | Fase 2 | Consumer nuevo + port inter-BC → pertenece a Fase 2 (Composition Roots). Requiere A-MD-002. |
| 5 | **B-MD-004** (pre-submit market validity) | Fase 2/3 | Política de riesgo en ejecución → Fase 2 (wiring) / Fase 3 (reglas). Requiere A-MD-003. |
| 6 | **B-MD-007** (received_at/processed_at) | Fase 2/3 | Observabilidad; puede agruparse con B-MD-003 en el mismo bump de schema. |
| 7 | **B-MD-006** (trades_stream) | Fase 2 | Chore de limpieza/cableado; requiere decisión humana previa (cablear vs eliminar). |

**¿Alguna B-MD debería ejecutarse ANTES de cerrar Fase 1?**

Sí, con matiz:
- **B-MD-003** y **B-MD-001** deben completarse **dentro de Fase 1** (market_data): son aditivos, de bajo riesgo, y sin ellos Fase 2 no puede construir ni el BookBuilder (B-MD-002 necesita sequence) ni la ejecución segura (B-MD-004 necesita freshness).
- **B-MD-002** y **B-MD-004** son las dos de **P0/P1 que bloquean live**; deben resolverse antes de cualquier operación con capital real, pero su lugar natural de implementación es **Fase 2** (Composition Roots) porque introducen port inter-BC (`MarketDataView`) y wiring de ejecución.
- **B-MD-006/007** pueden esperar a Fase 2/3 sin riesgo de seguridad.

**Secuencia obligatoria (por dependencias):**
```
B-MD-003 ──► B-MD-002 ──► B-MD-004
B-MD-001 ────────────────┘
B-MD-005 ────────────────┘
```
Es decir: (003+001+005) → (002) → (004). B-MD-007 y B-MD-006 son independientes.

---

## Qué pasa si NO se aprueba ninguna por ahora

**Riesgo concreto de dejar el sistema como está, con foco en RiskManager sin controles de mercado antes de ir a live:**

1. **Ejecución a ciegas sobre señal stale (F-MD-006 → B-MD-004).** `TradingEngine.run_once` usa `current_price = last close` del frame Gold (`engine.py:183`), que puede tener minutos/horas de antigüedad. `RiskManager` no valida que ese precio sea actual, ni el spread, ni la liquidez. Con `_BybitTransport` enviando **órdenes de mercado** (`composition_root.py:238`, `order_type="market"`), el precio de ejecución es el del libro en el momento del envío, que puede desviarse materialmente del precio de la señal. **Sin B-MD-001/004, una señal vieja + mercado movido = slippage o fill a precio desfavorable con capital real.** No hay `PricingError` (Freqtrade) ni bloqueo por spread.

2. **Sin book reconstruido, sin protección de microstructure (F-MD-003 → B-MD-002).** No hay best bid/ask/mid/spread en runtime. El order book se produce a `orderbook.raw` y se descarta. **No hay manera de detectar un book cruzado, spread anómalo o falta de liquidez antes de la orden.** Un mercado ilíquido o en condiciones extremas (flash move) pasaría sin filtro.

3. **Sin freshness, un feed congelado es indistinguible de uno sano (F-MD-001 → B-MD-001).** Si el WS del exchange se desconecta silenciosamente o el dato no se actualiza, el motor seguiría operando con el último `last close` disponible como si fuera actual. **Esto es exactamente el escenario de "mercado congelado" que produce pérdidas por falta de datos, no por malas señales.**

4. **Riesgo operacional adicional sin B-MD-005/006/007:** órdenes con cantidad/precisión inválidas rechazadas por el exchange (B-MD-005), trades live inexistentes o confusos (B-MD-006), y latencia de ingestión no diagnosticable (B-MD-007). Estos tres son **no bloqueantes** para una primera prueba paper, pero degradan la capacidad de operar/observar en live.

**Conclusión de riesgo:** operar **paper** con las propuestas pendientes es aceptable (no hay capital real en riesgo; el flujo paper fill = signal.price, `paper_executor.py:51`). Operar **live** (capital real) **sin B-MD-001, B-MD-002, B-MD-004 resueltas es un riesgo de seguridad material**: el RiskManager actual no tiene visibilidad del mercado, y la ejecución envía market orders sin validación de precio/spread/frescura. **Recomendación: no aprobar live hasta que la cadena (B-MD-003 → B-MD-002 → B-MD-004, más B-MD-001) esté implementada y testeada.**
