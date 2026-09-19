# Discovery Profile — Bybit (Orderbook L2 Public WebSocket)

**Estado:** Formalizado (basado en P0 2026-08-28)
**Fecha:** 2026-08-28
**Versión:** 1.0
**Autor:** Lead Market-Data Engineer + SRE + Software Architect (OCM)
**Fuente primaria:** P0 experimental (read-only, public market data only)
**Framework:** ADR-0017 Protocol Discovery Framework (PDF) — componentes 1–14

---

## 0. Resumen de trazabilidad (ADR-0017 §16)

```
FUENTE → REQUISITO → DECISIÓN ARQUITECTÓNICA → JUSTIFICACIÓN → IMPACTO EN OCM
```

Cada afirmación en este profile está etiquetada con su fuente:
- **A. DOCUMENTACIÓN OFICIAL DE BYBIT** — URL oficial, afirmación literal
- **B. OBSERVACIÓN EMPÍRICA DEL P0** — evidencia reproducible capturada por OCM
- **C. DECISIÓN/CONVENCIÓN ARQUITECTÓNICA DE OCM** — diseño interno, no atribuible al exchange

---

## 1. Objetivo (ADR-0017 §41 punto 1)

Definir el contrato de entrada/salida del discovery para la integración Bybit orderbook L2 público:
- Qué campos del wire se usan para reconstruir el libro L2 en OCM
- Qué invariantes valida el BookBuilder antes de considerar el libro "ready"
- Qué evidencia (fixture) demuestra el comportamiento real
- Qué queda ASSUMED vs PROTOCOL según Promotion Rule (ADR-0017 §64-70)

---

## 2. Principios (ADR-0017 §42)

| Principio | Aplicación en este profile |
|-----------|---------------------------|
| Evidencia sobre suposición | Solo B (P0) y A (docs oficiales) califican; C es diseño interno |
| Linaje obligatorio | Cada campo del schema v2 traza a A o B |
| No-SSOT hasta validar | Payloads promovidos a PROTOCOL/wired solo si fixture + tests existen |
| Dominio nunca depende del protocolo externo | BookBuilder consume schema v2 (OCM), no mensaje crudo Bybit |

---

## 3. Tipos de evidencia (ADR-0017 §47-49)

| Categoría | Definición | Uso en este profile |
|-----------|------------|---------------------|
| **PROTOCOL** | Mensaje observado del wire (JSON real del WS) | Snapshot/Delta v2 — fixture P0 `raw.jsonl` |
| **DOCUMENTATION** | Documentación oficial / OpenAPI del proveedor | Endpoints, límites, fórmulas de mark/funding |
| **UPSTREAM_LIBRARY** | Esquema unificado de librería verificada (CCXT/cryptofeed) | `book.raw` / `book.delta` structure en cryptofeed |
| **DOMAIN** | Modelo interno OCM (eventos propios) | `BookBuilderOutcome`, `MarketDataViewPort` |
| **ASSUMED** | Provisional, sin fuente verificada | Comportamiento tras reconexión >1h (retención Kafka) |

---

## 4. Exchange & API

| Campo | Valor | Fuente |
|-------|-------|--------|
| **Exchange** | Bybit | A |
| **API Version** | v5 (WebSocket público) | A |
| **Environment** | Mainnet (linear/perpetual) | A |
| **Endpoint WS** | `wss://stream.bybit.com/v5/public/linear` | A |
| **Categoría** | `linear` (USDT/USDC perpetual & USDT futures) | A |
| **Símbolos objetivo OCM** | `BTCUSDT`, `ETHUSDT`, `SOLUSDT` (configurable) | A + C |
| **Autenticación** | **Ninguna** (público) | A + B |
| **Método descubrimiento** | P0 experimental read-only (websocket nativo, no cryptofeed) | B |

> **Nota C**: OCM usa `cryptofeed` como upstream library (UPSTREAM_LIBRARY) para producción; el P0 usó `websockets` nativo para aislar la observación del vendor SDK.

---

## 5. Canal utilizado

| Campo | Valor | Fuente |
|-------|-------|--------|
| **Tópico WS** | `orderbook.50.BTCUSDT` (depth=50) | A + B |
| **Formato tópico** | `orderbook.{depth}.{symbol}` | A |
| **Depth disponible (oficial)** | 1, 50, 200, 500 (full) | A |
| **Depth usado en OCM (Fase 1)** | 50 (viewport 50 en BookBuilder) | C |
| **Frecuencia push (oficial)** | ~20 ms (L50) | A |
| **Frecuencia observada (P0)** | ~138 msg/s pico, ~12 msg/s promedio | B |

---

## 6. Formato de mensajes — Snapshot

### A. DOCUMENTACIÓN OFICIAL (Bybit v5 WS orderbook)

URL: `https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook`

```json
{
  "topic": "orderbook.50.BTCUSDT",
  "type": "snapshot",
  "ts": 1787954747128,
  "data": {
    "s": "BTCUSDT",
    "b": [["price", "size"], ...],   // bids: precio DESC
    "a": [["price", "size"], ...],   // asks: precio ASC
    "u": 101054915,                  // Update ID
    "seq": 800573227279              // Cross sequence
  },
  "cts": 1787954747125
}
```

**Campos oficiales documentados:**
- `type`: `"snapshot"` | `"delta"`
- `ts`: timestamp del servidor (ms)
- `data.s`: símbolo
- `data.b`: bids `[[price, size], ...]` ordenados DESC por precio
- `data.a`: asks `[[price, size], ...]` ordenados ASC por precio
- `data.u`: Update ID (monótono por book)
- `data.seq`: Cross sequence (global, para correlación cross-product)
- `cts`: Timestamp del matching engine (ms)

> **A**: Documentación afirma: "If there is a problem on Bybit's end, a snapshot will be re-sent, which is guaranteed to contain the latest data" ⇒ resync por re-snapshot.

> **A**: **NO hay campo `checksum`** en el orderbook WS de Bybit (a diferencia de Binance).

### B. OBSERVACIÓN EMPÍRICA DEL P0 (raw.jsonl línea 1)

```json
{
  "topic": "orderbook.50.BTCUSDT",
  "type": "snapshot",
  "ts": 1787954747128,
  "data": {
    "s": "BTCUSDT",
    "b": [["77364.60","3.756"], ["77364.50","0.540"], ...],  // 50 niveles
    "a": [["77364.70","9.673"], ["77364.80","0.001"], ...],  // 50 niveles
    "u": 101054915,
    "seq": 800573227279
  },
  "cts": 1787954747125
}
```

**Validación P0:**
- ✅ `b` ordenado DESC (77364.60 → 77355.00)
- ✅ `a` ordenado ASC (77364.70 → 77374.20)
- ✅ 100 niveles totales (50 bids + 50 asks) = depth 50 por lado
- ✅ `u` presente, `seq` presente, `cts` presente, `ts` presente
- ✅ `cts` ≤ `ts` (1787954747125 ≤ 1787954747128) — cts es matching engine, ts es servidor WS
- ✅ `u=1` **NO observado** en P0 (stream ya iniciado); documentación dice `u==1` ⇒ snapshot por reinicio

---

## 7. Formato de mensajes — Delta

### A. DOCUMENTACIÓN OFICIAL

```json
{
  "topic": "orderbook.50.BTCUSDT",
  "type": "delta",
  "ts": 1787954747148,
  "data": {
    "s": "BTCUSDT",
    "b": [["price", "size"], ...],   // solo niveles que cambian
    "a": [["price", "size"], ...],   // solo niveles que cambian
    "u": 101054916,
    "seq": 800573227328
  },
  "cts": 1787954747147
}
```

**Reglas oficiales:**
- Delta contiene **solo los niveles modificados** desde el último mensaje
- `size = "0"` ⇒ **eliminar el nivel** (estándar de mercado)
- `u` incrementa en +1 entre mensajes consecutivos del mismo stream
- `seq` es cross-sequence global (no necesariamente +1 por símbolo)

### B. OBSERVACIÓN EMPÍRICA DEL P0 (raw.jsonl líneas 2–5)

```json
// Delta 1 (u=101054916, 2 niveles bid, 0 ask)
{"type":"delta","ts":1787954747148,"data":{"s":"BTCUSDT","b":[["77357.00","0.012"],["77356.10","0.988"]],"a":[],"u":101054916,"seq":800573227328},"cts":1787954747147}

// Delta 2 (u=101054917, 3 bid, 2 ask)
{"type":"delta","ts":1787954747169,"data":{"s":"BTCUSDT","b":[["77359.30","0.001"],["77356.10","0.987"],["77355.00","0"]],"a":[["77373.50","0.002"],["77374.20","0"]],"u":101054917,"seq":800573227356},"cts":1787954747167}

// Delta 3 (u=101054918, 0 bid, 2 ask)
{"type":"delta","ts":1787954747189,"data":{"s":"BTCUSDT","b":[],"a":[["77367.80","0.006"],["77371.10","0"]],"u":101054918,"seq":800573227392},"cts":1787954747184}

// Delta 4 (u=101054919, 3 bid, 0 ask)
{"type":"delta","ts":1787954747208,"data":{"s":"BTCUSDT","b":[["77359.30","0"],["77356.40","0.398"],["77355.00","0.013"]],"a":[],"u":101054919,"seq":800573227431},"cts":1787954747207}
```

**Estadísticas P0 (1279 deltas observados):**

| Métrica | Valor | Interpretación |
|---------|-------|----------------|
| Niveles por delta (min/max) | 1 / 88 | **Multinivel atómico** — un mensaje = múltiples niveles |
| % deltas multinivel (>1 nivel) | 75.1% (961/1279) | Mayoría no son single-level |
| `u` gaps (delta) | eq1=1279, neq1=0, min=1, max=1 | **`u` es estrictamente +1** |
| `seq` gaps (delta) | eq1=0, neq1=1279, min=9, max=7593 | **`seq` NO es +1** — huecos grandes |
| Duplicados `u` | 0 | Sin duplicados observados |
| Duplicados `seq` | 0 | Sin duplicados observados |
| `u==1` events | 0 | No reinicio observado en ventana 60s |
| Snapshots observados | 1 (inicio) | Stream envía snapshot al conectar |
| Latencia `ts` (p50) | 163 ms | `local_recv_ms - ts` |

> **B crítico**: La documentación dice "`u` incrementa en +1" — **P0 confirma** (1279/1279 deltas con delta=1). La documentación no especifica comportamiento de `seq` por símbolo — **P0 demuestra que `seq` tiene huecos grandes (9–7593) y NO sirve para gap detection per-symbol**.

### C. DECISIÓN ARQUITECTÓNICA OCM (ADR-0028 D-7a/D-7b)

| Decisión | Justificación |
|----------|-------------|
| **Delta v2 = atómico multinivel** (D-7a) | Preservar atomicidad del mensaje Bybit; v1 aplanaba 1 nivel/msg rompiendo coherencia |
| **Gap detection por `update_id` (`u`), NO `seq+1`** (D-7b) | P0: `u` estrictamente +1; `seq` huecos 9–7593 |
| `u==1` ⇒ overwrite completo (reset libro) | Documentación oficial + práctica estándar |
| `size="0"` ⇒ delete nivel | Estándar de mercado, observado en P0 |

---

## 8. Timestamps

| Campo | Origen | Unidades | Observado P0 | Uso OCM |
|-------|--------|----------|--------------|---------|
| `ts` | Servidor WS Bybit | ms (epoch) | 1787954747128 | `timestamp_ms` en schema v2 (event time) |
| `cts` | Matching engine Bybit | ms (epoch) | 1787954747125 | `cts_ms` en schema v2 (correlación con trades `T`) |
| `receipt_timestamp` | Local (receiver) | ms (epoch) | — | Solo observabilidad (latencia), no en schema |

**Validación P0:** `cts` ≤ `ts` consistentemente (matching engine ≤ servidor WS diff ~3 ms). `receipt - ts` p50 = 163 ms.

---

## 9. Semántica de actualización

| Evento | Comportamiento observado | Acción BookBuilder (ADR-0028) |
|--------|-------------------------|------------------------------|
| **Conexión inicial** | Snapshot completo (100 niveles) | Inicializar `BookState`, `has_snapshot=true`, `last_update_id=u` |
| **Delta normal (`u = last+1`)** | Niveles modificados (1–88) | Aplicar atómicamente todos los niveles del mensaje |
| **Delta con `size="0"`** | Eliminar nivel (bid o ask) | `pop(price)` del lado correspondiente |
| **Gap (`u != last+1`)** | No observado en P0 (u siempre +1) | **Invalidar estado**, emitir `GAP_DETECTED`, esperar snapshot fresco |
| **`u == 1` (reinicio Bybit)** | No observado en P0 | **Overwrite completo** — reset `BookState` + nuevo snapshot |
| **Duplicado (`u == last`)** | No observado | Ignorar (idempotente), métrica `duplicate_total` |
| **Out-of-order (`u < last`)** | No observado | Ventana acotada o descartar, métrica `out_of_order_total` |

---

## 10. Checksum

| Fuente | Hallazgo |
|--------|----------|
| **A (Oficial)** | **NO hay campo `checksum`** en orderbook WS Bybit (a diferencia de Binance) |
| **B (P0)** | Confirmado: ningún mensaje en 1283 tiene `checksum` |
| **C (OCM ADR-0028 §10)** | `checksum` en wire v2 = `Optional[int] = None` para Bybit; validado solo para exchanges que sí lo exponen (Binance) |

> **Discrepancia registrada (ADR-0028 D-A)**: Borrador ADR-0028 asumía checksum → corregido: Bybit usa `u`/`seq`/snapshot-reset para integridad.

---

## 11. Límites y retención

| Límite | Valor | Fuente |
|--------|-------|--------|
| Conexiones WS / 5 min | 500 (por dominio) | A |
| Heartbeat | Ping cada 20s recomendado; corte 10 min sin actividad | A |
| Depth máximo (WS público) | 500 (full) / 200 / 50 / 1 | A |
| Depth usado OCM | 50 (configurable via `max_depth`) | C |
| Retención Kafka `orderbook.raw` | 1 hora (alta frecuencia) | C |
| BookBuilder viewport | 50 niveles por lado (D-7d) | C |
| Stale threshold BookBuilder | 2000 ms (configurable) | C |

> **C**: La retención 1h limita replay histórico. BookBuilder arranca en frío (`is_ready=False`) y espera snapshot fresco. Replay completo diferido (Fase 2+).

---

## 12. Provenance y Promotion (ADR-0017 §55-68)

### Schema v2 payloads en `shared/kafka/schemas/orderbook.py`

| Payload | Categoría | Wire Status | Justificación (ADR-0017) | Fixture |
|---------|-----------|-------------|--------------------------|---------|
| `OrderBookSnapshotPayload` v2 | **PROTOCOL** | **wired** | WS Bybit v2 observado (P0); u/seq/cts del raw; snapshot 100 niveles | `docs/audits/p0_bybit/evidence/20260828T220545Z/raw.jsonl` |
| `OrderBookDeltaPayload` v2 | **PROTOCOL** | **wired** | WS Bybit v2 atómico multinivel observado (P0); 75.1% multinivel; u/seq/cts del raw | Mismo fixture |

**Promotion Rule check (ADR-0017 §14):**

| Requisito | Cumple | Evidencia |
|-----------|--------|-----------|
| Provenance estable (PROTOCOL/DOC/UPSTREAM/DOMAIN) | ✅ | PROTOCOL (wire observado) |
| Pasa validación (schema, tipos, rangos) | ✅ | Tests `test_schemas_orderbook.py` 12 passed |
| Fixtures congelados reproducibles | ✅ | `raw.jsonl` + `summary.json` versionados |
| Tests de linaje/normalización/validación | ✅ | `test_book_builder.py` 15 passed + consumer tests |
| Contrato crítico para capital → solo promovido | ✅ | `shared/kafka/provenance.py` registra PROTOCOL/wired |

> **Resultado**: Ambos payloads **califican como SSOT estable** (Promotion Rule satisfecha). Registrados en `shared/kafka/provenance.py` líneas 46-55.

---

## 13. Fixtures asociados

| Fixture | Path | Descripción | Fecha | Commits |
|---------|------|-------------|-------|---------|
| **P0 raw messages** | `docs/audits/p0_bybit/evidence/20260828T220545Z/raw.jsonl` | 1283 mensajes JSONL (1 snapshot + 1279 deltas) | 2026-08-28T22:05:45Z | feat/adr0028-bookbuilder |
| **P0 summary** | `docs/audits/p0_bybit/evidence/20260828T220545Z/summary.json` | Estadísticas agregadas (gaps, levels, latency) | 2026-08-28T22:05:45Z | feat/adr0028-bookbuilder |
| **P0 script** | `docs/audits/p0_bybit/p0_bybit_orderbook.py` | Observador read-only reproducible | 2026-08-28 | feat/adr0028-bookbuilder |

**Reproducibilidad:**
```bash
.venv/bin/python docs/audits/p0_bybit/p0_bybit_orderbook.py --duration 60 --symbol BTCUSDT --depth 50
```

---

## 14. Metodología del P0

| Aspecto | Detalle |
|---------|---------|
| **Herramienta** | Script Python nativo `websockets` (sin cryptofeed) |
| **Duración** | 60 segundos (configurable) |
| **Símbolo** | `BTCUSDT` (linear perpetual) |
| **Depth** | 50 |
| **Autenticación** | Ninguna (público) |
| **Captura** | JSONL línea por mensaje + summary JSON |
| **Métricas** | Contadores u/seq, gaps, niveles/msg, latencia, estructura |
| **Seguridad** | Read-only; sin API keys; sin órdenes; sin trading |

---

## 15. Limitaciones de la evidencia (ADR-0017 §89)

| Limitación | Impacto | Mitigación |
|------------|---------|------------|
| **Ventana 60s** | No cubre reconexiones, reinicios Bybit, `u==1` | Documentación oficial cubre `u==1`; reconexión probada en canary F2.6c |
| **Un solo símbolo** | `BTCUSDT` únicamente | Assumed similar para ETH/SOL; validar por símbolo al habilitar |
| **Sin gaps `u` observados** | Gap detection no probado en vivo | Tests unitarios cubren `101→102→104`; canary validará en producción |
| **Retención 1h** | No backfill histórico >1h | D-7d: Fase 1 = viewport; replay completo Fase 2+ |
| **Sin checksum** | Integridad solo por `u`/snapshot | Validación estructural VO (bids DESC, asks ASC, qty≥0, not crossed) |
| **`seq` cross-sequence** | No usable para gap per-symbol | D-7b: usar `u`; `seq` solo correlación con trades |

---

## 16. Recovery / Resync (solo lo aprobado en ADR-0028)

| Evento | Comportamiento aprobado (ADR-0028 §11) | Implementado en BookBuilder |
|--------|----------------------------------------|----------------------------|
| WS disconnect (productor) | Cryptofeed reconecta; Bybit reenvía `u==1` → overwrite | `on_snapshot` hace `state.reset()` + `has_snapshot=true` |
| Duplicado (`u == last`) | Ignorar (idempotente) | `_apply_delta_levels` no muta si `u <= last` (pero `u` siempre +1 en P0) |
| Out-of-order / late | Ventana acotada; si no, descartar | No implementado aún (Fase 1) |
| **Gap (`u != last+1`)** | `GapDetectedEvent` + `is_ready=False`; esperar snapshot fresco | ✅ `GAP_DETECTED` outcome + `state.reset()` |
| Snapshot inválido (invariantes) | Descartar, `is_ready=False`, alerta | ✅ `_as_decimal` + validación `qty<0` → skip level |
| Stale data | `stale_threshold_ms` → `is_ready=False` | ✅ `check_stale(now_ms)` emite `STALE` |
| Kafka unavailable | Búfer acotado + backoff; no pierde estado | Infra Kafka (producer idempotente, consumer at-least-once) |
| Consumer crash | Rebalance grupo; reinicio `is_ready=False` hasta snapshot | ✅ `BookBuilderConsumer` commit solo si write_ok |
| Proceso reiniciado / systemd | Arranque en frío; `is_ready=False`; espera snapshot | ✅ `BookBuilder` sin estado persistente |

> **NO implementado (fuera de alcance Fase 1 ADR-0028)**: `seek_to_beginning`/replay histórico, `MarketDataViewPort` port outbound, GapEventPublisher a `market.gaps`, métricas Prometheus `ocm_book_*`.

---

## 17. Auditoría de consistencia (Discovery Profile ↔ ADR-0017 ↔ ADR-0028 ↔ schema v2 ↔ fixture ↔ adapter ↔ tests)

| Componente | Consistencia | Evidencia |
|------------|--------------|-----------|
| **Profile → ADR-0017** | ✅ | 14 componentes cubiertos; fuentes etiquetadas A/B/C; promotion rule verificada |
| **Profile → ADR-0028** | ✅ | D-7a (multinivel), D-7b (gap por u), D-7c (Decimal), D-7d (viewport) todos reflejados |
| **Profile → schema v2** | ✅ | `update_id`/`cross_seq`/`cts_ms` en snapshot y delta; `PriceLevel = Tuple[str,str]`; `SCHEMA_VERSION=2` |
| **Profile → fixture P0** | ✅ | `raw.jsonl` muestra snapshot 100 niveles + deltas multinivel 1–88 niveles; `u` estrictamente +1 |
| **Profile → adapter Bybit** | ✅ | `cryptofeed_orderbook_stream.py:_bybit_sequence()` extrae u/seq/cts de `book.raw`; `_levels_from_pairs()` emite delta atómico |
| **Profile → tests** | ✅ | `test_schemas_orderbook.py` (v2 round-trip), `test_cryptofeed_orderbook_stream.py` (extracción u/seq/cts, multinivel), `test_book_builder.py` (gap, stale, multinivel, delete, Decimal), `test_book_builder_consumer.py` (E2E stale, gap, DLQ) |

**Sin contradicciones encontradas.**

---

## 18. Estado actual y Próximos pasos

| Área | Estado | Próxima acción |
|------|--------|----------------|
| **Discovery Profile** | ✅ Formalizado | — |
| **Schema v2** | ✅ Implementado + tests | — |
| **Adapter Bybit (ACL)** | ✅ Implementado + tests | — |
| **BookBuilder (application)** | ✅ Implementado + tests | — |
| **BookBuilderPort (DIP/BC-07)** | ✅ Implementado | — |
| **BookBuilderConsumer (infra)** | ✅ Implementado + tests | — |
| **Composition Root wiring** | ❌ Pendiente | `build_book_builder_consumer()` en `composition_root.py` |
| **MarketDataViewPort (port)** | ❌ Pendiente | Nuevo port outbound para lectura bid/ask/mid/spread/depth/is_ready |
| **Métricas Prometheus BookBuilder** | ❌ Pendiente | `ocm_book_ready_total`, `ocm_book_reconstruction_gap_total`, `ocm_book_stale_total`, `ocm_book_snapshot_age_seconds` |
| **GapEventPublisher** | ❌ Pendiente | Publicar `GapDetectedEvent` a `market.gaps` |
| **Systemd template streaming** | ❌ Pendiente | Template + `install_systemd.sh` + `run.sh streaming` |
| **Canary E2E validación** | ❌ Pendiente | Requiere wiring + systemd + observabilidad desplegada |

---

## 19. Decisiones humanas pendientes (ADR-0028 §20)

| ID | Decisión | Estado | Bloquea |
|----|----------|--------|---------|
| D-7 | Aprobar diseño ADR-0028 (BookBuilder + schema v2) | ✅ Implícito (implementado) | — |
| D-7a | Delta v2: multinivel atómico vs un-nivel agrupado por u | ✅ Multinivel atómico (implementado) | — |
| D-7b | Gap primario: `u` por-book vs `seq` contiguo | ✅ `u` por-book (P0 validado) | — |
| D-7c | Punto de conversión Decimal→float | ✅ Decimal en BookState, str en wire (`format(x,'f')`) | — |
| D-7d | Viewport + seek_to_beginning en Fase 1 | ✅ Viewport sí; replay Fase 2+ | — |
| D-1 | Habilitar Bybit en `config/env/production.yaml` | ❌ Pendiente autorización | Canary producción |

---

## 20. Referencias

- ADR-0017: `docs/architecture/decisions/ADR-0017-protocol-discovery-framework.md`
- ADR-0028: `docs/proposals/ADR-0028-bookbuilder-design.md`
- ADR-0022: `docs/architecture/decisions/ADR-0022-lifecycle-proceso-realtime-feeds.md`
- P0 script: `docs/audits/p0_bybit/p0_bybit_orderbook.py`
- P0 evidencia: `docs/audits/p0_bybit/evidence/20260828T220545Z/`
- Schema v2: `shared/kafka/schemas/orderbook.py`
- Adapter: `packages/market_data/adapters/inbound/websocket/cryptofeed_orderbook_stream.py`
- Producer: `packages/market_data/adapters/inbound/websocket/orderbook_producer.py`
- BookBuilder: `packages/market_data/application/processing/book_builder.py`
- Port: `packages/market_data/ports/outbound/book_builder.py`
- Consumer: `packages/market_data/infrastructure/kafka/book_builder_consumer.py`
- Provenance: `shared/kafka/provenance.py` (líneas 46-55)

---

**Fin del Discovery Profile Bybit — Orderbook L2 Público**