# OB-RESYNC-001 — OrderBook Sequence Gap → Snapshot Resync → Rebuild → Resume

**ID:** OB-RESYNC-001
**Área:** OrderBook — Market Data WebSocket
**Fecha:** 2026-09-20
**Auditor:** Senior Market Data / Distributed Systems — READ-ONLY
**Scope:** OrderBook WebSocket `Bybit` `BTC-USDT-PERP` → `Cryptofeed` → `BookBuilder` → `Kafka` `orderbook.raw` → `Bronze` → `Silver` → `Gold`
**Estado Git inicial:** `feat/m22b-governance` `857cfe60` `M22-B` `GOVERNANCE.md` `?? VERIFICATION_FINAL_REPORT.md` + `?? docs/audit/ (3)` + `?? docs/audits/B-59 + 2 OCM_AUDIT_FINDINGS + 3 AUDIT_OCM_deployment (6)` — preservados
**Archivos inspeccionados:** `packages/market_data/adapters/inbound/websocket/cryptofeed_orderbook_stream.py:1`, `orderbook_producer.py:1`, `shared/kafka/schemas/orderbook.py:1`, `application/processing/book_builder.py:1`, `infrastructure/kafka/book_builder_consumer.py:1`, `adapters/inbound/rest/gap_recovery_fetcher.py:1`, `apps/app/cli/streaming_hydra.py:1`, `tests/market_data/application/processing/test_book_builder.py:1`, `tests/market_data/adapters/inbound/websocket/test_cryptofeed_orderbook_stream.py`, `config/market_data/feeds.yaml`, `deploy/systemd/ocm-streaming.service.template:1`

## 1. Hallazgo a auditar

**OB-RESYNC-001:** Se detecta `sequence gap` (`update_id` discontinuo) pero no existe mecanismo automático confirmado que solicite `snapshot`, reconstruya `OrderBook` y reanude deltas.

**Estado inicial reportado:** CONFIRMED — Impacto: libro temporalmente inválido tras pérdida WS. Corrección propuesta: `gap → snapshot → rebuild → resume`, `PENDIENTE`, `orderbook.resync_total`.

**Verificación contra repo:** **PARTIALLY_CONFIRMED** — `gap` sí se detecta y se invalida, pero no se solicita `snapshot` activo; se espera `snapshot` pasivo vía `WS reconnect`.

## 2. Flujo real verificado

```
Exchange Bybit WS (topic orderBook, type delta, data.u/seq/cts)
  ↓ cryptofeed FeedHandler (L2_BOOK channel)
CryptofeedOrderBookStream._translate_and_dispatch (book.symbol, book.delta, book.raw, book.timestamp)
  ↓ _bybit_sequence(raw) → (u, seq, cts) cryptofeed_orderbook_stream.py:246
  ↓ on_snapshot(bids,asks,update_id=u) / on_delta(bids,asks,update_id=u) :207
OrderBookKafkaProducer.on_snapshot/on_delta → OrderBookSnapshotPayload/DeltaPayload (update_id) → Kafka orderbook.raw (key=bybit:BTC/USDT, partition 0, FIFO) orderbook_producer.py:129
  ↓ Kafka orderbook.raw (schema v2, payload_type snapshot/delta, retention 1h)
BookBuilderConsumer.poll → deserialize → BookBuilder (application pure) book_builder.py:103
  ↓ on_snapshot → SNAPSHOT_APPLIED → publish book.snapshot
  ↓ on_delta → GAP_DETECTED (if u != last+1) → reset + warning, DELTA_BEFORE_SNAPSHOT, STALE
  ↓ book.snapshot / book.delta → Kafka (no Bronze/Silver/Gold orderbook downstream evidenciado)
```

**Estado por etapa:**

| Etapa | Implemented | Connected | Executed | Persisted | Validated | Operational |
|---|---|---|---|---|---|---|
| Exchange WS | YES | YES | YES | N/A | YES (Bybit v5) | UNKNOWN (runtime) |
| Cryptofeed | YES | YES | YES | N/A | YES (2.4.1) | UNKNOWN |
| OrderBook stream | YES | YES | YES | N/A | YES (u/seq/cts) | YES (streaming active) |
| Sequence validation | YES (`u`) | YES | YES | N/A | YES (D-7b) | YES |
| Gap detection | YES (`GAP_DETECTED`) | YES | YES | N/A | YES (test) | YES |
| BookBuilder state | YES | YES | YES | N/A (memory) | YES | YES |
| Snapshot | YES (initial + reconnect) | YES (passive) | YES | YES (orderbook.raw) | YES | YES |
| Resync (gap→snapshot) | **PARTIAL** (passive wait, no active request) | **NO** (no REST snapshot) | **NO** | N/A | **NO** | **NO** |
| Delta resume | YES (after snapshot) | YES | YES | N/A | YES | YES |
| Kafka orderbook.raw | YES | YES | YES | YES (1h) | YES | YES |
| Kafka book.* | YES | YES | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN |
| Bronze | **NO** orderbook (only OHLCV Bronze) | **NO** | **NO** | **NO** | **NO** | **NO** |
| Silver/Gold | **NO** orderbook | **NO** | **NO** | **NO** | **NO** | **NO** |

## 3. Sequence

**¿Tienen sequence ID?** **YES** — `update_id`=`u` Bybit monótono `+1`, `cross_seq`=`seq`, `cts_ms`=`cts` `cryptofeed_orderbook_stream.py:246` `shared/kafka/schemas/orderbook.py:84`.

**¿Dónde se obtiene?** `CryptofeedOrderBookStream._bybit_sequence(raw)` `raw.get('data').get('u')` `:260`.

**¿Dónde se conserva?** `BookBuilder._books[(exchange,symbol)].last_update_id` `book_builder.py:86` + `OrderBookPayload.update_id` `schemas/orderbook.py:100`.

**¿Dónde se valida?** `BookBuilder.on_delta:218` `if last_update_id !=0 and update_id != last+1 → GAP_DETECTED`.

## 4. Gap detection

**Si `100,101,103`:** `103 != 102` → `GAP_DETECTED` `dE=1` `book_builder.py:222` `detail "gap detectado: esperado u=102, recibido u=103 (dE=1) — estado invalidado"`.

**Archivo/clase/función/condición:** `book_builder.py` `BookBuilder.on_delta:218` `if state.last_update_id !=0 and update_id != state.last_update_id+1`.

## 5. Después del gap

**Exacto:**

1. **B registra:** `BookBuilderConsumer._apply_outcome:284` `GAP_DETECTED → event_failed reason=gap_detected → warning "book_builder_invalidated | gap detectado..."` `book_builder_consumer.py:290`.
2. **E invalida:** `BookBuilder.on_delta:220` `state.reset()` `has_snapshot=False`.
3. **A continúa ignorando?** No — **NO aplica delta** `GAP_DETECTED → not publishes` `book_builder.py:221`.
4. **C reconecta?** **No** `BookBuilder` — `Cryptofeed FeedHandler` reconecta `WS` autónomo.
5. **D solicita snapshot?** **NO** — no `REST` orderbook snapshot, no `snapshot request`.
6. **Espera snapshot pasivo** vía `WS reconnect` → `Cryptofeed` emite `delta=None` → `on_snapshot` `book_builder.py:141` `reset` `SNAPSHOT_APPLIED` → base `update_id`.

**Clasificación:** **B + E + F pasiva** (registra, invalida, reconstruye solo al llegar snapshot).

## 6. Resync — gap → snapshot → rebuild → resume

**Existe flujo?** **PARTIAL** — `gap → invalidate → wait → WS snapshot → rebuild → resume` sí, pero `gap → REST snapshot request` **NO**.

```
gap (u 103) → reset → DELTA_BEFORE_SNAPSHOT (deshecha delta 104,105) → WS snapshot (u 200) → SNAPSHOT_APPLIED → last_update_id=200 → delta 201 → DELTA_APPLIED
```

No `snapshotFetcher`, no `adapter REST` orderbook.

## 7. Conexión — IMPLEMENTED vs CONNECTED

**IMPLEMENTED:** `BookBuilder` `GAP_DETECTED` + `on_snapshot` rebuild.
**CONNECTED:** `orderbook_producer` → `orderbook.raw` → `BookBuilderConsumer` → `book.snapshot/delta` **sí**, pero `resync` **NO conectado** a `REST` snapshot (solo `GapRecoveryFetcher` para `trades` `gap_recovery_fetcher.py:6`, no orderbook).
**EXECUTED:** Solo si `BookBuilderConsumer` corre (sin `systemd` unit evidenciada).
**TESTED:** `test_gap_detected_and_invalidates` sí, `gap→snapshot→delta` **NO**.
**OPERATIONAL:** `orderbook.raw` `active` (streaming), `book.*` `UNKNOWN`.

**RECONNECT != RESYNC:** `Cryptofeed` **reconnect** `WS` automático sí, `OrderBook` **resync** `state rebuild` solo tras `snapshot` pasivo, no `snapshot request` activo — documentado `RECONNECT != RESYNC`.

## 8. Snapshot

1. Inicial **YES** `Cryptofeed` `delta None` → `on_snapshot`.
2. Periódico **NO**.
3. Bajo demanda **NO** (no `SnapshotFetcher` orderbook).
4. `SnapshotFetcher` **NO** (solo `GapRecoveryFetcher` trades).
5. Adapter REST orderbook **NO** (solo `derivatives_fetcher.py` `open_interest` snapshot).
6. Exchange `Bybit` `BTC-USDT-PERP` `BTC/USDT` `category=linear` `symbol=BTCUSDT` `limit=50` (no REST orderbook endpoint en OCM).
7. Normalización `bids/asks` `Decimal` `book_builder.py:150` `D-7c`.
8. Incorporación `on_snapshot:168` `state.last_update_id=update_id`.
9. Baseline `update_id` snapshot.
10. Deltas durante resync `DELTA_BEFORE_SNAPSHOT` deshechados hasta snapshot.

## 9. Contrato exchange

- **Snapshot endpoint:** Bybit `/v5/market/orderbook` `category=linear` `symbol=BTCUSDT` `limit=50` — **NO usado por OCM** (solo `Trades` `fetch_ohlcv`).
- **Symbol:** `REST` `BTC/USDT` `CCXT` vs `Cryptofeed` `BTC-USDT-PERP` — `CryptofeedOrderBookStream:85` `symbols=["BTC-USDT-PERP"]` `market_data` `exchange: bybit` `symbol: "BTC/USDT"` normalizado `make_symbol_key` `bybit:BTC/USDT` — riesgo `PERP` vs `SPOT` durante resync **NO contrato canónico** (`NO DETERMINADO`).
- **Sequence:** `u` monótono `+1` `D-7b` `P0` `seq` huecos `9-7593` `schemas/orderbook.py:174`.
- **Depth:** `50` `CryptofeedOrderBookStream:98` `max_depth=50`.

## 10. Estado OrderBook — snapshot → delta 100→101→102 perdido →103

**103** `→ GAP_DETECTED` `state.reset()` `has_snapshot=False` `OUTCOME GAP_DETECTED` **no publicado**, siguiente `104` `DELTA_BEFORE_SNAPSHOT` deshechado hasta snapshot. Estado `INVALID` → `WAITING_FOR_SNAPSHOT` (implícito `has_snapshot=False`), no `STALE` (stale es timeout `check_stale:272`).

## 11. Kafka y Bronze

- **GAP_DETECTED:** deltas inválidos **no publicados** `book_builder_consumer.py:284` `handled` no `written`, no `DLQ`, no `resync` evento, no `snapshot` publicado tras gap.
- **Secuencia:** `snapshot`/`delta` conservan `update_id` en `orderbook.raw` y `book.*` `persisted` `Kafka` `1h` `book_builder_consumer.py:264`.
- **Bronze:** `orderbook` **NO** `Bronze` (solo `ohlcv.raw` → `Bronze` `Iceberg`); `orderbook.raw` retención `1h` no `Bronze`.

## 12. Silver/Gold

**NO DOWNSTREAM RECOVERY EVIDENCE** — `Bronze` `Silver` `Gold` sin `orderbook` `Silver` `Gold` consumidores evidenciados; solo `book.delta`/`book.snapshot` `Kafka`.

## 13. Tests

| Caso | Archivo | Estado |
|---|---|---|
| `sequence gap 100→105` | `test_book_builder.py:104` `test_gap_detected_and_invalidates` | **YES** |
| `missing delta 102` | mismo | **YES** |
| `out-of-order` | `GAP_DETECTED` `expected 101 got 103` | **YES** |
| `reconnect` | `test_cryptofeed_orderbook_stream.py` | **NO** |
| `snapshot recovery` | `test_book_builder.py:55` `test_snapshot_resets_previous_state` | **PARTIAL** (reset, no gap→snapshot) |
| `resync` | `test_gap → DELTA_BEFORE_SNAPSHOT` `112` | **PARTIAL** |
| `state reconstruction` | `test_contiguous_delta_applied` | **YES** |

**Faltante:** `gap → snapshot → delta 201 accepted` **MISSING** (no `snapshot recovery` chain).

## 14. Métricas

- **Existentes:** `orderbook_producer` `KafkaMetrics` `event_published/failed` `orderbook_producer.py:179`, `BookBuilderConsumer` `event_failed reason=gap_detected/stale` `:285`.
- **No existe:** `orderbook.resync_total` (propuesta `OB-RESYNC-001`).
- **Dónde medir:** `BookBuilderConsumer._apply_outcome` `GAP_DETECTED` `increment counter` `orderbook.resync_total` `labels: exchange, symbol` (convención `ocm_kafka_events_*` `infrastructure/kafka/metrics.py`).
- **Qué contar:** `resync exitosos` (`SNAPSHOT_APPLIED` tras `GAP_DETECTED`), no intentos.

## 15. Configuración

`config/market_data/feeds.yaml` `feeds: bybit: enabled: true symbols: [BTC-USDT-PERP]` `max_depth 50` — **no** `resync` `snapshot` `reconnect` `gap recovery` `orderbook recovery` switch. `GapRecoveryFetcher` solo `trades`.

## 16. Runtime / systemd

`systemctl cat ocm-streaming.service` (no ejecutado, `RUNTIME = UNKNOWN` — `repo evidence` `streaming_hydra.py` `CryptofeedOrderBookStream` `orderbook_producer` sí, `systemctl status` previo `active` pero no `BookBuilderConsumer` unit).

**CODE EXISTS** `OrderBook` stream/builder, **RUNTIME CONNECTED** `streaming_hydra` `orderbook.raw`, **RUNTIME EXECUTED** `UNKNOWN` `BookBuilderConsumer`, **RUNTIME OPERATIONAL** `UNKNOWN`.

## 17. Documentación

`shared/kafka/schemas/orderbook.py:169` `update_id monótono GAP` + `book_builder.py:39` `GAP → invalidar` vs `book_builder_consumer.py:18` `GAP ... espera snapshot fresco (stream reenvía)` — **alineada** (pasiva). No `REST` snapshot doc.

## 18. Performance

`snapshot REST` `Bybit` `limit 50` `~1KB` `rate limit` `10/s` `ccxt` `coste bajo`, `resync` frecuencia `gap` raro (P0 `seq` huecos no `u`), no `backpressure` `BookBuilder` `O(1)` `Dict[Decimal]`, `Kafka ordering` `make_symbol_key` `partition 0` FIFO.

## 19. Corrección propuesta

```
WebSocket
  ↓ sequence validation (u)
  ↓ GAP (u != last+1) → invalidate / pause book (has_snapshot=False)
  ↓ request snapshot → OrderBookSnapshotFetcher (REST Bybit /v5/market/orderbook)
  ↓ rebuild state (on_snapshot) → establish baseline (last_update_id)
  ↓ resume deltas (u == last+1) → validate continuity
  ↓ metrics orderbook.resync_total + stale timeout 2000ms + retry 3 + backoff
```

Componente: `OrderBookSnapshotFetcher` `adapters/inbound/rest/orderbook_snapshot_fetcher.py` (análogo `GapRecoveryFetcher`), `BookBuilderConsumer` orquesta `GAP → fetch → rebuild`, `BookBuilder` ya `reset`, `CCXT` REST.

## 20. Clasificación final

**ID:** OB-RESYNC-001
**Área:** OrderBook
**Estado:** **PARTIALLY_CONFIRMED** (gap detectado sí, snapshot resync no)
**Severidad:** `MEDIUM` (según `OCM_AUDIT_FINDINGS` `F-16` `INFO` tracking, no `CRITICAL`)
**Impacto:** Libro inválido temporal tras pérdida WS, `book.delta` gap no publicado, estado vacío hasta próximo snapshot pasivo.
**Causa:** `BookBuilder` invalida pero no solicita `snapshot` REST; `BookBuilderConsumer` solo `log.warning`, espera `WS` snapshot.
**Evidencia:** `book_builder.py:218 GAP_DETECTED`, `book_builder_consumer.py:284 handled`, `cryptofeed_orderbook_stream.py:246 u`, `shared/kafka/schemas/orderbook.py:169`, `test_book_builder.py:104`.
**Estado implementación:** `IMPLEMENTED` gap detection, `MISSING` snapshot fetcher, `PARTIAL` resync.
**Tests:** `PARTIAL` (gap sí, resync no).
**Observabilidad:** `PARTIAL` (gap metric sí, `resync_total` no).
**Corrección:** `gap → snapshot → rebuild → resume` `PENDIENTE` `PR-A/B/C` separados.
**Prioridad:** `P1` (según `P0-P3` `P1` `health` + `P2` `Market Data`).

## 21. Documentación del finding

**Ubicación canónica:** `docs/audits/AUDIT_OCM_orderbook-resync-snapshot-audit_2026-09-20.md` (este archivo) — sigue `docs/audits/AUDIT_OCM_b59-runtime-operational-audit_2026-09-19.md` `B-59` y `docs/audit/deployment-runtime-implementation-plan.md` `§1` `audit/`.

**Plantilla:** `AUDIT_OCM_*` no, `B-59-*` sí — `OB-RESYNC-001` sigue `B-59` `OB-` prefix `OrderBook`.

**Contenido:** 1 fecha, 2 scope, 3 git `feat/m22b-governance 857cfe60`, 4 archivos, 5 call chain, 6 evidencia, 7 estado por etapa, 8 tests, 9 config, 10 runtime `UNKNOWN`, 11 discrepancias `RECONNECT != RESYNC`, 12 impacto, 13 corrección, 14 no implementado, 15 próximos pasos.

**AUDIT: DONE — IMPLEMENTATION: NOT DONE**

## 22. PR futura

- **PR-A:** `OrderBookSnapshotFetcher` `adapters/inbound/rest/orderbook_snapshot_fetcher.py` + `ccxt` `fetch_order_book`
- **PR-B:** `Gap → snapshot` orchestration `BookBuilderConsumer` `GAP_DETECTED → fetch`
- **PR-C:** `Book rebuild / resume` `BookBuilder` ya `reset`, test `gap→snapshot→delta`
- **PR-D:** `tests` `gap→snapshot→delta` `test_book_builder_resync.py`
- **PR-E:** `observability` `orderbook.resync_total` `metrics.py`
- **PR-F:** `documentation` `schemas/orderbook.py` `D-7b`

Mínimo `2` PRs: `PR-A` fetcher + `PR-B` orchestration (depende `PR-A`).

---

# REPORTE FINAL

## OB-RESYNC-001 — RESULTADO

**Veredicto:** `PARTIALLY_CONFIRMED` — gap detectado e invalidado sí, snapshot resync automático no.

**Flujo actual:** `Bybit WS (u) → cryptofeed → on_delta(u) → orderbook.raw → BookBuilder on_delta (u != last+1 → GAP_DETECTED → reset, handled, wait-for-next-snapshot) → snapshot (WS reconnect) → rebuild → resume`.

**Flujo esperado:** `WS → u validation → GAP → invalidate/pause → REST snapshot request → rebuild → baseline → resume deltas → validate`.

**Evidencia principal:**

* `book_builder.py:218` `GAP_DETECTED` `expected u, got u`
* `book_builder_consumer.py:284` `GAP_DETECTED → warning` `handled` no `snapshot request`
* `cryptofeed_orderbook_stream.py:246` `u` `seq`
* `test_book_builder.py:104` `gap dE=4 GAP_DETECTED` `DELTA_BEFORE_SNAPSHOT`
* `shared/kafka/schemas/orderbook.py:169` `update_id monótono`
* `runtime` `UNKNOWN` (no `BookBuilderConsumer` unit)

**Matriz estado:** ver `§2` `Implemented/Connected` `PARTIAL` `snapshot resync`.

**Tests:** `gap detection` `YES`, `resync` `MISSING`.

**Observabilidad:** `gap` metric `YES`, `resync_total` `MISSING`.

**Documentación:** alineada `RECONNECT != RESYNC` (pasiva).

**Corrección:** `gap → REST snapshot → rebuild → resume` `PENDING`.

**PRs futuras:** `PR-A` fetcher, `PR-B` gap→snapshot (mínimo 2).

**Archivos auditoría creados:** `docs/audits/AUDIT_OCM_orderbook-resync-snapshot-audit_2026-09-20.md` (este).

```
AUDIT COMPLETE
OB-RESYNC-001 documented
IMPLEMENTATION NOT PERFORMED
COMMIT NOT PERFORMED
PUSH NOT PERFORMED
MERGE NOT PERFORMED
```

**Preservados:** `6` audit `??`, `uv.lock`, `Security Fix F`, `M22-B`, `B56`, `feat/adr-0022`, `PR #56 b1e48141`.

