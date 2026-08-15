# Detalle de propuestas — BookBuilder/MarketState y ejecución (sesión 2026-08-15)

**Fecha:** 2026-08-15
**Fuente:** `docs/audits/2026-08-15-ocm-market-data-position-execution-risk-deep-audit.md` (hallazgos F-MD-011..F-MD-018, §§2/5/6) + detalle previo `docs/audits/2026-08-14-b-md-proposals-detalle.md` (B-MD-001..007)
**Propósito:** detalle operativo (formato de 14 campos) de las propuestas que desbloquean el BookBuilder/MarketState (B-MD-002/003) y de las nuevas de ejecución detectadas en esta sesión (B-MD-008/009). Para decisión humana **antes** de tocar `docs/plans/tracking.yaml`.
**Restricciones:** solo lectura/análisis. Cero cambios de código, cero cambios a tracking.yaml.

> Convención de prioridad (alineada con el prompt): **P0** = correctness/safety · **P1** = live readiness · **P2** = reliability · **P3** = performance · **P4** = nice-to-have.
> Formato de propuesta (14 campos): ID provisional / problema / evidencia / riesgo / solución conceptual / BC / port / eventos / composition root / dependencias / prioridad / fase / ADR / esfuerzo (S-M-L).

---

## Tabla resumen

| ID | Problema | Evidencia | Riesgo | Solución conceptual | BC | Port | Eventos | Comp. root | Dependencias | Prioridad | Fase | ADR | Esfuerzo |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **B-MD-003** | Wire sin `sequence`/`first_update_id` → imposible gap-detection y resync del book | `shared/kafka/schemas/orderbook.py`; `cryptofeed_orderbook_stream.py:158-161` (sequence disponible, no propagada) | Medio (contrato público) | Schema v2 aditivo `sequence`/`first_update_id`; propagar `book.sequence_number` en producer | shared (BC-35) | — | — | — | Ninguna (prerrequisito de 002) | **P1** | 1 | **A-MD-001** | **S** |
| **B-MD-002** | `orderbook.raw` producido sin consumir; sin bid/ask/mid/spread en runtime | `orderbook_producer.py`; grep `TOPIC_ORDERBOOK_RAW` = solo producer; `GROUP_BOOK_BUILDER`/`book.*` 0 usos | Alto (componente nuevo + port inter-BC) | BookBuilder (consumer `GROUP_BOOK_BUILDER`) con BookState en memoria + `MarketDataViewPort` | market_data (nuevo BC-56) | `MarketDataViewPort` (outbound, nuevo) | `GapDetected/Healed/Failed` reutilizados | `build_book_consumer()` + `_book_builder_loop` | B-MD-003 | **P0** | 2 | **A-MD-002** | **L** |
| **B-MD-008** | `OMS.cancel` es local-only, no cancela en exchange | `oms.py:300`; transporte sin cancel | Medio (posición en vuelo no revocable) | Añadir cancel real al transporte/port; semántica cancel asíncrona con confirmación | trading | `OrderTransport` (extensión) | eventos de cancel | `composition_root.py` | Investigación previa (capacidad Bybit) | **P1** | 3 | No (o A-MD-004 si cambia contrato) | **M** |
| **B-MD-009** | Sin `fetch_balance`/reconciliación de saldo real en trading | grep `fetch_balance` en `trading/` = ∅ | Alto (capital real, risk ciego a saldo) | Reconciliación periódica balance via CCXT + guard en RiskManager (fail-closed si gap) | trading | port de balance (nuevo) | eventos de balance | `composition_root.py` | Independiente (mejora RiskManager) | **P1** | 3 | No (o A-MD-005) | **M** |

---

## B-MD-003 — `sequence`/`first_update_id` en wire del order book (v2)

**1. ID y nombre:** B-MD-003 · **Sequence field en schema order book (v2)**.

**2. Hallazgo que resuelve:** F-MD-012 / F-MD-004 — el wire (`OrderBookSnapshotPayload`/`OrderBookDeltaPayload`, `shared/kafka/schemas/orderbook.py`) no transporta secuencia. `cryptofeed_orderbook_stream.py:158-161` documenta que `book.sequence_number` se asigna pero no se propaga. Sin `sequence` el BookBuilder no puede validar continuidad ni detectar deltas perdidas.

**3. Prioridad:** **P1** (live readiness) / **P0 estructural** en la cadena de microstructure. Es el prerrequisito absoluto de B-MD-002.

**4. ADR:** **A-MD-001** — "evolución del contrato canónico de market data: `sequence` (order book) + `received_at`/`processed_at` (envelope) con compatibilidad `SCHEMA_VERSION`". B-MD-003 y B-MD-007 comparten el ADR.

**5. Archivos que tocaría:**
- `shared/kafka/schemas/orderbook.py` — añadir `sequence: Optional[int]` (snapshot) y `sequence`+`first_update_id` (delta) + `SCHEMA_VERSION` bump.
- `packages/market_data/adapters/inbound/websocket/cryptofeed_orderbook_stream.py` — propagar `book.sequence_number`.
- `packages/market_data/adapters/inbound/websocket/orderbook_producer.py` — incluir `sequence` en payload.
- `tests/market_data/test_external_wire.py` — roundtrip.
- `tests/architecture/test_kafka_contracts.py` — actualizar contrato de schema (documentado).

**6. Riesgo de implementación:** **Medio.** Cambio de contrato público (bump version, aditivo-backward-compatible). Sin consumer que lo lea hoy, es aditivo sin impacto de runtime; el riesgo es de diseño semántico (¿sequence global por symbol? ¿first_update_id del delta?).

**7. Dependencias:** Ninguna. **Precede a B-MD-002.**

**8. Esfuerzo:** **S.**

---

## B-MD-002 — BookBuilder/MarketState (consumer de `orderbook.raw`) + `MarketDataViewPort`

**1. ID y nombre:** B-MD-002 · **BookBuilder/MarketState consumer**.

**2. Hallazgo que resuelve:** F-MD-003 + F-MD-016 — `orderbook.raw` producido (canary `streaming_hydra.py`) y **sin consumidor**; no existe port de lectura de estado de book en vivo (solo Gold pull vía `FeatureReaderPort`). ADR-0023 + B-25 difieren gap/DLQ product-side *hasta que exista consumidor* → el BookBuilder es el desbloqueador.

**3. Prioridad:** **P0** (correctness/safety). Sin bid/ask/mid/spread/depth en runtime, B-MD-004 no tiene de dónde leer spread/liquidez; y el order book producido se descarta. **Bloquea cualquier mejora de ejecución basada en microstructure.**

**4. ADR:** **A-MD-002** — ver diseño completo en `docs/architecture/decisions/ADR-0028-draft-bookbuilder-marketstate.md` (Estado=Propuesto).

**5. Archivos que tocaría:**
- `packages/market_data/application/use_cases/book_builder.py` (o `application/pipelines/orderbook_book_builder.py`) — use case consumidor.
- `packages/market_data/ports/outbound/market_data_view.py` — **nuevo** `MarketDataViewPort` (mid_price, best_bid, best_ask, spread, depth, is_ready, stale_threshold_ms).
- `packages/market_data/domain/value_objects/order_book.py` — reutiliza VOs; añade `BookState` (last_snapshot_uid/last_diff_uid).
- `packages/market_data/infrastructure/bootstrap/composition_root.py` — `build_book_consumer()`.
- `packages/market_data/main.py` — `_book_builder_loop` (patrón `_bronze_writer_loop`, `main.py:222-307`).
- `packages/market_data/infrastructure/kafka/consumer.py` — `for_book_builder()` (topic `orderbook.raw`, group `GROUP_BOOK_BUILDER`).
- `architecture/importlinter.toml` — **BC-56** (nuevo contrato).
- `tests/architecture/test_import_contracts.py` — registrar BC-56 (documentado).

**6. Riesgo de implementación:** **Alto.** Primer consumidor de un tópico sin consumidores; lógica de reconstrucción snapshot+diff con resync por secuencia (books cruzados, resync incorrecto). Mitigación: tras B-MD-003, con tests de reconstrucción y `is_ready` como compuerta.

**7. Dependencias:** **B-MD-003** (sequence). Conecta con el control-plane existente (`gap_events.py` + `GapEventPublisherPort` + `market.gaps`) para gaps de secuencia.

**8. Esfuerzo:** **L.**

---

## B-MD-008 — Cancel real en exchange (OMS.cancel hoy local-only)

**1. ID y nombre:** B-MD-008 · **Exchange-level cancel**.

**2. Hallazgo que resuelve:** F-MD-019 (nuevo, VERIFIED) — `packages/trading/execution/oms.py:300` `cancel()` solo transita estado interno; **no** envía cancelación al exchange (el transporte no tiene método de cancel). Una orden en vuelo no puede revocarse.

**3. Prioridad:** **P1** (live readiness). En live, una orden de mercado no siempre puede cancelarse, pero **órdenes limit/staged o fills parciales** sí requieren cancel real para deshacer. Sin él, un error de ejecución no es reversible.

**4. ADR:** No (o A-MD-004 si extiende el contrato del transporte de forma pública).

**5. Archivos que tocaría:**
- `packages/trading/execution/transport.py` — port `OrderTransport`: método `cancel(exchange_order_id)`.
- `packages/trading/execution/oms.py` — `cancel()` delega al transporte + confirmación asíncrona.
- `packages/trading/bootstrap/composition_root.py` — wiring.
- `tests/` — cancel real con mock de CCXT.

**6. Riesgo de implementación:** **Medio.** Requiere verificar capacidad de cancelación de Bybit (CCXT `cancel_order`) y definir semántica de estados intermedios (PENDING_CANCEL/SUBMITTED). Riesgo bajo de contrato si es extensión del port.

**7. Dependencias:** Investigación previa de capacidad del exchange. Independiente de la cadena BookBuilder.

**8. Esfuerzo:** **M.**

---

## B-MD-009 — Reconciliación de saldo (fetch_balance) en trading

**1. ID y nombre:** B-MD-009 · **Balance reconciliation / fetch_balance**.

**2. Hallazgo que resuelve:** F-MD-020 (nuevo, VERIFIED) — grep `fetch_balance` en `packages/trading` = ∅. El RiskManager valida capital×size_pct contra el capital **configurado**, no contra el saldo real del exchange. Un saldo distinto al esperado (fees, transferencias, fills no sincronizados) no se detecta.

**3. Prioridad:** **P1** (live readiness). En live, operar sin conocer el saldo real puede provocar órdenes rechazadas (insuficiencia) o sizing erróneo.

**4. ADR:** No (o A-MD-005 si se define política de fail-closed ante discrepancia).

**5. Archivos que tocaría:**
- `packages/trading/ports/outbound/balance.py` (nuevo) — `BalancePort` (get_balances/fetch).
- `packages/trading/risk/manager.py` — check de saldo real + fail-closed si gap.
- `packages/trading/bootstrap/composition_root.py` — wiring.
- `config/` trading — umbral de tolerancia.

**6. Riesgo de implementación:** **Alto.** Toca riesgo con capital real; una reconciliación incorrecta puede bloquear legítimas o dejar pasar discrepancia. Requiere definir política ante discrepancia (fail-closed por defecto).

**7. Dependencias:** Independiente de la cadena BookBuilder; mejora la seguridad del RiskManager.

**8. Esfuerzo:** **M.**

---

## Recomendación de orden (coherente con Fase 1 market_data → Fase 2 → Fase 3)

| Orden | Propuesta | Fase | Motivo |
|---|---|---|---|
| 1 | **B-MD-003** (sequence wire) | 1 | Aditivo, bajo riesgo, desbloquea cadena microstructure. |
| 2 | **B-MD-001** (freshness) | 1 | Aditivo, control-plane; prerrequisito de 004. |
| 3 | **B-MD-005** (instrumentos/precisión) | 1/2 | Extiende VO de dominio; alimenta ejecución. |
| 4 | **B-MD-002** (BookBuilder + MarketDataViewPort) | 2 | Consumer nuevo + port inter-BC + BC-56; requiere A-MD-002/ADR-0028. |
| 5 | **B-MD-004** (pre-submit market validity) | 2/3 | Política de riesgo en ejecución; requiere A-MD-003. |
| 6 | **B-MD-007** (received_at/processed_at) | 2/3 | Observabilidad; mismo bump schema que 003. |
| 7 | **B-MD-006** (trades_stream) | 2 | Chore; requiere decisión cablear/eliminar. |
| 8 | **B-MD-008** (cancel real) | 3 | Extensión de transporte; requiere investigación capacidad. |
| 9 | **B-MD-009** (fetch_balance) | 3 | Reconciliación de saldo; mejora RiskManager. |

**Secuencia obligatoria (dependencias):**
```
B-MD-003 ──► B-MD-002 ──► B-MD-004
B-MD-001 ────────────────┘
B-MD-005 ────────────────┘
```
(003+001+005) → (002) → (004). 006/007/008/009 independientes.

---

## Qué pasa si NO se aprueba nada

1. **orderbook.raw sigue produciéndose sin consumidor** (F-MD-003): capital de ingesta desperdiciado; sin bid/ask/mid/spread en runtime; ADR-0023/B-25 siguen bloqueados.
2. **Live sigue inseguro**: LiveExecutor market-order sin market validity (B-MD-004), sin freshness (B-MD-001), sin saldo real (B-MD-009), y cancels local-only (B-MD-008). Paper aceptable; **live NO**.
3. **Sin sequence (B-MD-003)**, ni siquiera paper con microstructure sería posible: cualquier intento futuro de BookBuilder será frágil.
4. **Observabilidad limitada**: sin received_at/processed_at (B-MD-007) la latencia de ingestión no es medible; sin consumers de `market.gaps` (F-MD-015) el control-plane OHLCV es ciego.

**Conclusión de riesgo:** operar **paper** sin estas propuestas es aceptable; **live** sin (B-MD-003 → B-MD-002 → B-MD-004) + B-MD-001 es **riesgo de seguridad material**. B-MD-008/009 deben resolverse **antes** de operar live real.
