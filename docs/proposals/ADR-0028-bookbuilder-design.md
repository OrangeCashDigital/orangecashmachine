# ADR-0028 — PROPUESTA DE DISEÑO: BookBuilder / MarketState

> **ESTADO: PROPUESTA (documento de diseño para revisión humana).** NO aprobado, NO implementado.
> Este documento NO modifica ADR-0028, NO modifica código, NO modifica configuración de producción,
> NO arranca/detiene servicios, NO modifica systemd, NO modifica `.gitignore`.
> Trading permanece completamente separado y detenido. Market Data NO se declara READY.
>
> **Relación con B-25 / ADR-0023:** ADR-0023 difiere el gap detection product-side de `orderbook.raw`
> "hasta que exista un consumidor real". Este diseño define ese consumidor (BookBuilder). Al aprobarse,
> se reabre B-25 con la infraestructura que ADR-0023 anticipó.
>
> **Fase:** en progreso. El documento se entrega para revisión humana; no se avanza a implementación.

---

## 0. Contrato de trazabilidad

Cada requisito importante conserva su cadena:

```
FUENTE → REQUISITO → DECISIÓN ARQUITECTÓNICA → JUSTIFICACIÓN → IMPACTO EN OCM
```

Fuentes empleadas (siempre citadas antes de decidir):

- **F1 — Bybit oficial (order book WS):**
  `https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook` (consultado 2026-08-28).
- **F2 — Nota derivada de Bybit oficial (diferenciada):**
  `docs/knowledge/notes/bybit-perpetuals-reference.md` (§3.2, §8.2, §8.5, §9.2, §14) — `needs_verification`, no es contrato.
- **F3 — ADRs de OCM:** ADR-0013, ADR-0014, ADR-0022, ADR-0023, ADR-0028 (borrador), ADR-0016, ADR-0038, serie heredada ADR-0000.
- **F4 — Literatura TIER_1 de la KB:** *Market Microstructure In Practice* (Lehalle & Laruelle); *Data Quality Engineering in Financial Services* (Buzzelli) — informativa, no normativa (jerarquía KB del repo).
- **F5 — Código:** `shared/kafka/schemas/orderbook.py`, `orderbook_producer.py`, `cryptofeed_orderbook_stream.py`, `KafkaConsumerAdapter`, `KafkaConsumerPort`, `gap_aware_stream.py`, `pipeline_factory.py`, `domain/value_objects/order_book.py`, `domain/events/orderbook_events.py`, `shared/kafka/topics.py`.

Jerarquía de autoridad (registro formal de conflicto, §D): código/ADR/contratos OCM > documentación oficial Bybit > esta referencia derivada > libros/literatura. Cuando una fuente de mayor autoridad contradice una menor, prevalece la mayor y se registra el conflicto.

---

## 1. Context

`orderbook.raw` se produce (canary `streaming_hydra.py` → `CryptofeedOrderBookStream` → `OrderBookKafkaProducer`) pero **no tiene consumidor** (grep exhaustivo VERIFIED). No existe bid/ask/mid/spread en runtime; el único acceso del motor a market data es Gold pull (`FeatureReaderPort`). ADR-0023 + B-25 (2026-08-09) diferieron formalmente el gap detection product-side hasta que exista un consumidor real de `orderbook.raw`.

Este diseño es ese consumidor: un **BookBuilder** que reconstruye el L2 en memoria por `(exchange, symbol)`, valida integridad, detecta gaps, hace resync y expone un **viewport** de lectura — habilitando B-MD-004 (market validity pre-envío) en Fase 2 y reabriendo B-25.

## 2. Problem

Sin BookBuilder no se puede:
1. Reconstruir y **demostrar** un order book L2 íntegro.
2. Detectar gaps de secuencia (el caso de corrección `101→102→104`).
3. Exponer bid/ask/mid/spread/depth a consumidores futuros sin acoplar trading a cryptofeed.
4. Cumplir el DEBER SER de ADR-0014 (`data_quality`: timestamp/missing/duplicate) y de la literatura TIER_1 (Lehalle/Laruelle: coherencia L2; Buzzelli: detección de missing/gaps).
5. Declarar el **data-plane** sano con evidencia, no solo "proceso activo".

## 3. Evidence (hechos verificados, no suposiciones)

- **F1 (Bybit oficial orderbook WS):**
  - Tópico `orderbook.{depth}.{symbol}`; linear L50 push **20 ms**.
  - Mensajes `snapshot` y `delta` (campo `type`).
  - `u` (Update ID): al recibir `u==1` ⇒ snapshot por reinicio del servicio ⇒ **sobrescribir libro local**.
  - `seq` (cross sequence): "compare different levels orderbook data; the smaller seq ⇒ data generated earlier"; ordenable/correlacionable.
  - `cts`: timestamp del matching engine, correlacionable con `T` de trades.
  - "If there is a problem on Bybit's end, a snapshot will be re-sent, which is guaranteed to contain the latest data" ⇒ **resync por re-snapshot**.
  - **NO hay campo `checksum`** en el orderbook WS de Bybit (a diferencia de Binance).
- **F2:** heartbeat ~20 s ping/pong; corte a los 10 min sin actividades; límite WS 500 conexiones/5 min. (§3.2, §9.2, `needs_verification`).
- **F5-schema v1** (`shared/kafka/schemas/orderbook.py`): snapshot con `checksum: Optional` y **sin `seq`/`u`/`cts`**; delta **de un nivel** y **sin `seq`/`u`**.
- **F5-producer** (`orderbook_producer.py:136,158`): `checksum` capturado y publicado, **nunca contrastado**; delta aplanado un nivel por mensaje.
- **F5-stream** (`cryptofeed_orderbook_stream.py:158-161`): `book.sequence_number` expuesto, **no propagado**.
- **F5-consumer:** no existe `for_book_builder()`; `GROUP_BOOK_BUILDER`/`TOPIC_BOOK_*` reservados sin uso (topics.py:117-120,179).
- **F5-domain:** VOs `OrderBookSnapshot`/`OrderBookDelta` con invariantes bids DESC / asks ASC / qty≥0 / `is_crossed`; wire usa **str** (Decimal), dominio usa **float** (D-D).

## 4. Requisitos industrial/científicos (FUENTE → REQUISITO)

| FUENTE | REQUISITO |
|---|---|
| F1 Bybit oficial | Reconstrucción snapshot+delta; overwrite en `u==1`; `seq` para ordenar/correlacionar; resync por re-snapshot; sin checksum |
| F3 ADR-0014 | `data_quality` reservada: timestamp validation, missing/duplicate detection |
| F3 ADR-0022 | "proceso vivo" ≠ "datos aptos"; separación process-health vs data-eligibility |
| F3 ADR-0023/B-25 | gap detection product/consumer-side con sequence + DLQ cuando exista consumidor |
| F4 Lehalle & Laruelle | book L2 coherente: snapshot+deltas encadenados por UID/seq; aplicar delta solo sobre snapshot correcto; detectar gaps |
| F4 Buzzelli | calidad = validación + detección de missing/duplicate/gap + trazabilidad + monitoreo |
| F5 código OCM | Reutilizar infraestructura Kafka existente (consumer + DLQ + dedup + gap-publisher) |

## 5. Arquitectura actual (ruta orderbook)

```
CryptofeedOrderBookStream
  → on_snapshot → OrderBookSnapshotPayload (checksum Optional, SIN seq/u/cts)
  → on_delta    → OrderBookDeltaPayload    (un nivel, SIN seq/u)
  → OrderBookKafkaProducer → orderbook.raw (routing bybit:BTC/USDT → partición única)
  → (SIN consumidor)
```

## 6. Gaps actuales

1. Sin `seq`/`u`/`cts` en el wire v1.
2. Sin validación de integridad ajustada al protocolo (Bybit: `seq`+`u`+snapshot-reset; **no** checksum).
3. Sin gap detection (B-25 diferido).
4. Sin consumidor / BookBuilder / viewport.
5. Sin `is_ready`/stale gating.
6. Delta aplanado sin atomicidad multinivel (riesgo de estados intermedios).
7. Precision/Domain: str(Decimal) en wire vs float en dominio.

## 7. Arquitectura propuesta

```
Bybit WS orderbook.50
  → CryptofeedOrderBookStream
  → OrderBookKafkaProducer (schema v2: u, seq, cts, delta multinivel atómico)
  → orderbook.raw
  → [NUEVO] BookBuilder (consumer GROUP_BOOK_BUILDER)
      • valida seq/u monotónicos y snapshot-before-delta
      • detecta gaps (101→102→104) → GapDetectedEvent → market.gaps
      • resync: espera snapshot fresco (re-snapshot de Bybit o seek/replay)
      • aplica deltas atómicamente (por u/seq)
      • mantiene BookState in-memory (bids DESC / asks ASC)
      • expone MarketDataViewPort (mid/best_bid/best_ask/spread/depth/is_ready)
  → (opcional) book.snapshot / book.delta / Bronze persistence
  → Observabilidad: métricas ocm_book_*, health, alerting
```

NO existe ninguna ruta hacia trading (ver §15).

---

## 8. BookBuilder design

| FUENTE | REQUISITO | DECISIÓN | JUSTIFICACIÓN | IMPACTO EN OCM |
|---|---|---|---|---|
| F3 ADR-0028 borrador | consumidor de `orderbook.raw` | `market_data/application/use_cases/book_builder.py` (nuevo) consume Kafka con `GROUP_BOOK_BUILDER` | Kappa: productor/consumidor separados; reabre B-25; aprovecha infraestructura existente | Nuevo módulo application; sin tocar trading |
| F1 Bybit | snapshot inicial | `BookState` se inicializa **solo** con snapshot válido; `is_ready=False` hasta entonces | Bybit exige snapshot base para aplicar deltas | Vista no-ready hasta snapshot |
| F5-infra | consumidor | añadir `KafkaConsumerAdapter.for_book_builder()`; usar `poll`/`commit`/`seek_to_beginning` | patrón establecido `_bronze_writer_loop`; `seek_to_beginning` habilita replay/resync | Nuevo builder en infrastructure |
| F4 Lehalle/Laruelle | aplicar delta sobre snapshot correcto | orden/detección por `u`/`seq`; descartar deltas si no hay snapshot o hay gap | evita corrupción silenciosa del libro | invariante de coherencia |

**Componentes del BookBuilder:**
- `BookState` (VO): `bids` (desc), `asks` (asc), `last_snapshot_u`, `last_delta_u`, `last_seq`, `is_ready`, `updated_ms`.
- Buffer de diffs pendientes: `deque` acotado (patrón `_past_diffs_windows`, concepto Hummingbot en prosa, sin código GPL) para deltas que llegan antes de un snapshot; se aplican si su `seq > snapshot.seq`.
- Aplicación atómica: un mensaje wire v2 con `u`/`seq` representa **el delta multinivel completo** del exchange; se aplica entero o no se aplica (no estados intermedios).

## 9. Secuencia / Gap model (criterio de corrección)

**Caso esperado (`101→102→103→104`):** cada delta tiene `u`/`seq` contiguo; se aplica en orden.

**Caso gap (`101→102→104`):** al recibir `104` con esperado `103`:

| FUENTE | REQUISITO | DECISIÓN | JUSTIFICACIÓN | IMPACTO |
|---|---|---|---|---|
| F4 Lehalle/Laruelle | detectar gap | NO aplicar el delta; emitir `GapDetectedEvent`; `is_ready=False`; esperar resync (nuevo snapshot) | un delta sobre libro corrupto propaga la corrupción | libro no-ready + alerta + control-plane |

**Duplicado** (`seq==last`): ignorar (idempotencia), métrica `ocm_book_duplicate_total`.
**Fuera de orden / late** (`seq<last`): descartar o re-ordenar en ventana acotada; métrica `ocm_book_out_of_order_total`.
**Overwrite `u==1`** (reinicio Bybit): reemplazar estado (F1).

> **⚠ DECISIÓN PENDIENTE DE VERIFICACIÓN EMPÍRICA (no inferible):** `seq` de Bybit es **cross-sequence global** (por todo el exchange, no únicamente por símbolo). Si entre dos mensajes consecutivos del mismo símbolo el `seq` no es contiguo (+1), usar `seq+1` para gap daría **falsos gaps**. `u` es el Update ID **por book** y es el candidato natural para el gap primario; `seq` se usa para ordenar y correlacionar con trades (`cts`/`T`). **Antes de fijar el algoritmo se debe verificar empíricamente el patrón `seq`/`u` del símbolo (test read-only en WS público, P0).** Riesgo de asumir `seq+1` sin verificación: R1.

## 10. Checksum model (si corresponde)

| FUENTE | REQUISITO | DECISIÓN | JUSTIFICACIÓN | IMPACTO |
|---|---|---|---|---|
| F1 Bybit oficial | (no hay checksum) | **No corresponde checksum para Bybit.** `checksum` en wire queda `None` para Bybit | Bybit garantiza integridad con `seq`/`u`/snapshot-reset, no con checksum | Corrige supuesto del borrador ADR-0028 |
| F5-código | validación estructural | BookBuilder valida invariantes VO: bids desc / asks asc / qty≥0 / `not is_crossed` | detección local de estado inválido | rechazo de snapshot/delta inválido + alerta |
| F3 ADR-0028 borrador | genérico | campo `checksum` transportado y validado **solo para exchanges que sí lo expongan** (p. ej. Binance) | mecanismo por-exchange | Backward-compatible |

## 11. Recovery / resync model

| Evento | Comportamiento |
|---|---|
| WS disconnect (productor) | cryptofeed reconecta; Bybit reenvía `u==1` → BookBuilder reemplaza estado al recibir el nuevo snapshot |
| Duplicated | ignora (idempotente), métrica |
| Out-of-order / late | ventana acotada ordena; si no, descarta, métrica |
| Gap (`u`/`seq` no contiguo) | `GapDetectedEvent` + `is_ready=False`; espera nuevo snapshot; no aplica deltas |
| Snapshot inválido (invariantes) | descarta snapshot, `is_ready=False`, alerta; espera siguiente snapshot |
| Stale data | `stale_threshold_ms` (config): `now - updated_ms > umbral` ⇒ `is_ready=False`; alerta |
| Kafka unavailable | búfer acotado + backoff; no pierde estado; al reconectar resync por snapshot |
| Consumer failure / crash | rebalance de grupo; al reiniciar `is_ready=False` hasta snapshot fresco; opcional `seek_to_beginning` (replay corto) |
| Proceso reiniciado / systemd restart | arranque en frío; `is_ready=False`; espera snapshot fresco (deuda aceptada: retención 1h limita backfill histórico) |

## 12. Kafka contract (schema v2)

**`OrderBookSnapshotPayload` v2 (aditivo, backward-compatible):** `+u: int`, `+seq: int`, `+cts: int (opcional)`. `checksum` se conserva (`None` para Bybit).

**`OrderBookDeltaPayload` v2 (cambio de forma — requiere bump + coordinación; 0 consumidores hoy):**
- **Recomendado:** delta **multinivel atómico** `{u, seq, cts, bids:[(p,q)...], asks:[(p,q)...]}` (size 0 = delete), alineado con el mensaje nativo Bybit y que garantiza atomicidad.
- **Alternativa (conservadora):** mantener un-nivel-por-mensaje pero añadir `u`/`seq` y agrupar por `u` en el consumidor. Más simple de migrar pero complejiza agrupamiento.

| FUENTE | REQUISITO | DECISIÓN | JUSTIFICACIÓN | IMPACTO |
|---|---|---|---|---|
| F1 Bybit | mensaje delta multinivel | v2 multinivel atómico (recomendado, pendiente D-7a) | casar con protocolo; atomicidad simple | breaking change wire (0 consumidores hoy) |
| F3 ADR-0023 | campo sequence | `u`/`seq` aditivos; bump `SCHEMA_VERSION` | ADR-0023 anticipó exactamente esto | reabre B-25 |

Nuevo builder `KafkaConsumerAdapter.for_book_builder()` → `GROUP_BOOK_BUILDER`. Topics `book.snapshot`/`book.delta` opcionales (path primario = viewport en memoria).

**Precisión (D-D):** wire preserva Decimal (str); dominio usa float. **Decisión D-7c** sobre el punto de conversión para no perder precisión (riesgo falso mid/spread).

## 13. Observability

- Métricas `ocm_book_*`: `ocm_book_ready_total`, `ocm_book_reconstruction_gap_total`, `ocm_book_duplicate_total`, `ocm_book_out_of_order_total`, `ocm_book_stale_total`, `ocm_book_snapshot_age_seconds`, `ocm_book_latency_ms` (si B-MD-007 añade received/processed).
- Health: `is_ready` + `snapshot_age` + ausencia de gaps recientes + consumer lag (`ocm_kafka_consumer_lag_book_builder`).
- Logging estructurado (component=BookBuilder, symbol).
- Alerting: gap recurrente, stale, consumer down, book no-ready sostenido — **depende del despliegue de observabilidad (hoy NO desplegado; F3 ADR-0038 = provisioning, no despliegue).**
- Audit trail: `GapDetected/Healed/Failed` ya publicados a `market.gaps` (`GapEventPublisherPort`/`KafkaGapPublisher`).

## 14. Failure modes

Cubiertos en §11 + criterio de corrección §9. Determinístico para cada condición del requerimiento.

## 15. Security boundaries — separación Market Data / Trading

| FUENTE | REQUISITO | DECISIÓN | JUSTIFICACIÓN | IMPACTO |
|---|---|---|---|---|
| F3 ADR-0013/0014/0016 | desacople | BookBuilder en `market_data` (application); expone `MarketDataViewPort` (port outbound); **sin dependencia de trading/portfolio/oms/execution** | Kappa + BC-56 garantizan dirección | BC-56 (import-linter) nuevo |
| F3 plan (D-1..D-4) | trading congelado | trading engine OFF; `LiveExecutor.IS_STUB=True` (ADR-0016 R1) + ExecutionGuard kill-switch | decisión humana ya tomada | no hay ejecutor |
| F3 ADR-0022 | "vivo" ≠ "apto" | `is_ready` como **compuerta fail-closed**: ningún consumidor lee un book no reconstruido/stale para decidir | extiende fail-closed de OCM | seguridad de capital |

**Demostración formal de que ninguna condición (§14) genera orden:** no existe ruta de datos desde BookBuilder hacia execution; no hay ejecutor real; `is_ready=False` bloquea lectura a consumidores futuros; trading consume via puertos, no directo. Un fallo de market-data **no puede** producir órdenes ni activar trading.

## 16. Testing strategy

- **Unit (BookBuilder):** snapshot; deltas contiguos `101→102→103`; gap `101→102→104` (gap event + no-ready + resync); duplicado; out-of-order; snapshot inválido (crossed/sorting); overwrite `u==1`; delete qty=0; precisión Decimal; atomicidad multinivel.
- **Unit (schema v2):** round-trip, versionado, backward-compat aditivo snapshot, atómico delta.
- **Integration (Kafka):** producer→orderbook.raw→BookBuilder con broker real (`-m integration`); verificar offsets + reconstrucción + gap simulado.
- **Contratos:** import-linter BC-56; `tests/architecture_linter`; AST guard.
- **Propiedad global:** `is_ready` pasa `false→true` solo tras snapshot válido + deltas consistentes.

## 17. Operational verification (criterio DATA-PLANE HEALTH)

1. BookBuilder `is_ready=true` para `bybit:BTCUSDT`.
2. `ocm_book_snapshot_age_seconds < stale_threshold` (fresco).
3. `ocm_kafka_consumer_lag_book_builder` bajo/estable.
4. Sin `ocm_book_reconstruction_gap_total` en ventana N min (o gaps justificados con resync).
5. End-to-end: mensaje reciente en `orderbook.raw` con `u`/`seq`, consumido, libro coherente (bids desc/asks asc, no-crossed).
6. Métricas/alerts alcanzables (requiere observabilidad desplegada — bloqueo transversal actual).
7. Reinicio de servicio → recuperación a `is_ready=true` con snapshot fresco.

## 18. Alternatives considered

1. **BookBuilder in-process con el producer (PubSub tipo Hummingbot):** rechazada — rompe Kappa, acopla el canary, no usa `orderbook.raw` ni `GROUP_BOOK_BUILDER` (ADR-0028 borrador).
2. **Estado en Redis como SSOT:** rechazada — latencia/consistencia (partial writes), TTL, duplica `CursorStorePort`.
3. **Solo viewport sin BookBuilder (leer cryptofeed directo):** rechazada — duplica conexión WS, acopla trading a cryptofeed, no reabre B-25.
4. **Consumidor Kafka (ELEGIDA)** ✓ — Kappa, reutiliza infraestructura, reabre B-25 naturalmente.
5. **Delta v2 "un-nivel agrupado por `u`" vs "multinivel atómico":** recomendada multinivel (alinea protocolo, atomicidad simple) — pendiente D-7a.

## 19. Risks

- **R1 (alto):** asumir `seq` contiguo (+1) sin verificación → falsos gaps/resync. **Mitigación:** verificación empírica P0; usar `u` por-book como gap primario si procede.
- **R2 (medio):** Decimal→float pierde precisión (D-D). **Mitigación:** definir frontera; considerar Decimal en BookState (D-7c).
- **R3 (medio):** delta multinivel atómico = breaking change wire → bump + coordinación (0 consumidores hoy).
- **R4 (bajo/medio):** `orderbook.raw` retención 1h limita replay; BookBuilder requiere snapshot fresco al arrancar (deuda aceptada).
- **R5 (medio):** observabilidad no desplegada → métricas/alerts no operacionales aún (bloqueo transversal).
- **R6 (medio):** flujo real de Bybit no conectado (ningún exchange habilitado en producción) — el canary funciona en dev/test, no en prod; evidencia operacional no disponible hasta D-1 config.

## 20. HUMAN DECISIONS REQUIRED

Decisiones **NO determinables objetivamente** a partir de la evidencia. Requieren aprobación humana antes de implementar:

- **D-7 (gobierno):** ¿aprobar este diseño ADR-0028 (BookBuilder + schema v2)?
- **D-7a (schema, inferible no):** modelo de delta v2 — **multinivel atómico** (recomendado) vs **un-nivel agrupado por `u`**. Afecta wire, atomicidad y complejidad.
- **D-7b (algoritmo de gap, requiere evidencia empírica):** gap primario por `u` por-book (recomendado tras análisis de protocolo) vs `seq` contiguo. **No se asume**; requiere P0 (test read-only WS público) para fijarlo.
- **D-7c (precisión):** punto de conversión Decimal→float en el pipeline del BookBuilder.
- **D-7d (alcance Fase 1):** incluir `MarketDataViewPort` y `seek_to_beginning`/replay en Fase 1, o diferirlos.
- **D-1 (config, ya decidida en estrategia, pendiente de autorización para tocar producción):** habilitar Bybit en `config/env/production.yaml` para poder verificar operacionalmente.

> (Decididas y NO re-preguntadas por directiva: D-1 Bybit primero, D-2 market-data público, D-3 sin permisos de trading, D-4 trading congelado.)

## 21. Recommended implementation plan (fases; NO ejecutadas)

- **P0 Control/evidencia empírica:** test read-only WS público Bybit → registrar patrón `seq`/`u` por símbolo (resuelve D-7b). *(read-only, sin tocar producción)*
- **P1 Schema v2** en `shared/kafka/schemas/orderbook.py` (aditivo snapshot + delta multinivel si D-7a=A) + bump.
- **P2 Productor:** propagar `u`/`seq`/`cts` en `cryptofeed_orderbook_stream.py` + `orderbook_producer.py`.
- **P3** `KafkaConsumerAdapter.for_book_builder()` + `GROUP_BOOK_BUILDER`.
- **P4 BookBuilder** + `BookState` + buffer + gap/resync.
- **P5 `MarketDataViewPort`** + wiring `build_book_consumer()` + loop en `main.py`.
- **P6 Contrato BC-56** (import-linter) + tests unit/integration.
- **P7 Observabilidad** (despliegue Prometheus/Grafana/Loki) + métricas/alerts/health.
- **P8** Reabrir B-25, validar criterio §17, PR→review→merge→deploy→verify.

---

## Anexo A — Auditoría de GapAwareStream (obligatoria)

**Qué hace:** wrapper sobre `TradesSourceProtocol` (stream de trades): detección de silencio (timeout `gap_threshold_ms`) y de desconexión (StopAsyncIteration); recovery por REST (`GapRecoveryFetcher`, source=REST_RECOVERY) + reconexión con backoff.

**Dónde está conectado:** `pipeline_factory.py` — `gap_stream = GapAwareStream(WSTradesSource(...), recovery_factory=...)`; envuelve el **stream de trades**, no el orderbook.

**Qué consume sus eventos:** el pipeline kappa de trades (OHLCV builder, feature pipeline, futuro OrderBook builder). Contrato `AsyncIterator[RawTrade]` con `trade.timestamp_ms` y `TradeSource`.

**Por qué está cableado al trades stub:** fue construido para el stream de trades; `WSTradesSource` aún es stub. No fue diseñado para el orderbook.

**Reutilización para Order Book:** **NO directamente.** Su contrato (`AsyncIterator[RawTrade]` y semántica de "silencio") es específico de trades. El orderbook es un flujo snapshot+delta con su propio modelo de integridad (`u`/`seq`/estructura).

**¿Modificar / reemplazar?** Para ADR-0028 **no se reutiliza ni modifica** GapAwareStream en la ruta orderbook: el BookBuilder implementa su lógica sobre **Kafka** (patrón Kappa correcto; la alternativa in-process/trades-stream se rechazó en §18). GapAwareStream **se conserva** para su propósito (trades WS cuando el adapter real exista). **No se reemplaza** — son responsabilidades distintas (resiliencia de transporte de trades WS vs validación/estado de orderbook desde Kafka).

## Anexo B — Discrepancias registradas (no ocultadas)

| # | Discrepancia | Autoridad |
|---|---|---|
| D-A | Bybit **sin checksum** (borrador ADR-0028 asumía checksum) → integridad por `seq`+`u`+snapshot-reset | Bybit oficial > borrador ADR-0028 |
| D-B | `book.sequence_number` (cryptofeed) = `seq` cross-sequence, no `u`; `u==1` = overwrite | Bybit oficial > código |
| D-C | delta aplanado (1 nivel) sin atomicidad → schema v2 multinivel | Bybit oficial > código v1 |
| D-D | wire str(Decimal) vs dominio float → frontera de precisión (D-7c) | código |
