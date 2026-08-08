# F2.6a — Capacity Planning teórico (sin despliegue)

- **Hallazgo:** `f2_6a_capacity_teorico` (tracking.yaml)
- **Fecha:** 2026-08-07
- **Estado:** HECHO — estimación documentada, con fuente por medición según DOD de F2.6

## 1. Propósito y alcance

Estimar el volumen de datos que `realtime_feeds` (streaming WS de microestructura)
impondrá a Kafka y al proceso único `streaming` antes de desplegar nada (F2.6b canary).
Cubre los **4 producers WS ya construidos** en `market_data`:

| Producer | Topic Kafka | Payload wire (SSOT) |
|---|---|---|
| `OrderBookKafkaProducer` | `orderbook.raw` | `OrderBookSnapshotPayload` / `OrderBookDeltaPayload` |
| `FundingKafkaProducer` | `funding.raw` | `FundingRatePayload` |
| `OIKafkaProducer` | `oi.raw` | `OpenInterestPayload` |
| `LiquidationsKafkaProducer` | `liquidations.raw` | `LiquidationPayload` |

Alcance de símbolos (config SSOT `config/market_data/feeds.yaml`):

- **Bybit** (perps USDT): `BTC-USDT-PERP`, `ETH-USDT-PERP`, `SOL-USDT-PERP`
- **KuCoin** (spot, deshabilitado hoy): `BTC-USDT`, `ETH-USDT`

> Nota: `trades.raw` llega hoy por **REST poll** (`market_data/main.py` es servicio
> FastAPI de ingestión polling → Bronze/Iceberg), no por WS streaming. No se modela
> aquí como stream; se reincorpora desde REST en F2.6c si procede.

## 2. Fuentes primarias (por medición)

| # | Dato | Fuente | URL |
|---|------|--------|-----|
| F1 | Bybit orderbook: depths y push frequency por nivel (10ms/20ms/100ms/200ms…), snapshot+delta | Documentación oficial Bybit V5 WebSocket | https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook |
| F2 | Bybit: max 10 tópicos por request de suscripción; ping 20s; delta "siempre que el orderbook cambie"; re-snapshot si 3s sin cambio | Documentación Bybit | https://bybit-exchange.github.io/docs/v5/ws/connect |
| F3 | Bybit allLiquidation: push frequency 500ms (≤2 msg/s/símbolo) | Documentación oficial Bybit | https://bybit-exchange.github.io/docs/v5/websocket/public/all-liquidation |
| F4 | Bybit ticker (incluye OI como campo): derivativos 100ms | Documentación oficial Bybit | https://bybit-exchange.github.io/docs/v5/websocket/public/ticker |
| F5 | KuCoin orderbook: Best1 realtime; Best50 100ms; increment@10ms (cota 500 niveles); "si no hay cambio NO se empuja" | Documentación oficial KuCoin | https://www.kucoin.com/docs-new/3470221w0 |
| F6 | KuCoin mark/index/OI: push 1s; funding rate 1min | Documentación oficial KuCoin | https://www.kucoin.com/docs-new/3470272w0 |
| F7 | KuCoin rate limits WS (conexiones, mensajes client→server 100/10s) | Documentación oficial KuCoin | https://www.kucoin.com/docs-new/rate-limit |
| F8 | Tamaño de payload: serialize en código (JSON UTF-8, no compacto) | Código repo | `shared/kafka/serializer.py`, `shared/kafka/schemas/*.py` |
| F9 | Config de símbolos y producers | Código/config | `config/market_data/feeds.yaml`, `composition_root.build_ws_producers()` |

## 3. Tamaño de mensaje (cálculo, no estimación)

Base de cálculo: JSON UTF-8 de `BasePayload.to_dict()` con separadores default
(`, ` y `: `), ya que `json.dumps` no compacta — `shared/kafka/serializer.py:56` —
más los campos propios de cada payload.

Campo adicionales de envelope: `event_id` (36), `event_version` (int), `occurred_at`
(ISO-8601 ~30) → ~90 B de envelope por mensaje.

| Stream | Bytes/msg medio calculado | Composición |
|---|---|---|
| `orderbook.raw` **delta** | ~250 B | envelope + exchange/symbol/timestamp_ms/side/price/size (1-3 niveles) |
| `orderbook.raw` **snapshot** | ~3 KB (depth-50 ambos lados) | envelope + 100 tuplas price/size; solo inicial y resync |
| `funding.raw` | ~300 B | envelope + rate, next_funding_ms, interval_h, predicted_rate |
| `oi.raw` | ~300 B | envelope + contracts, value, mark_price |
| `liquidations.raw` | ~300 B | envelope + price, quantity, quantity_usd, side, order_type |

Fuente: F8 (enumerable sobre el código, no anecdótico).

> El overhead Kafka (CRC + partición + headers `shared/kafka/provenance.py`
> `KAPPA_HEADERS` + key binaria `make_symbol_key`) se añade en §6 con ~70-100 B/msg.

## 4. Modelo de msg/s por stream (cota documentada y realista)

Los límites documentados (F1/F3/F5) son **cota superior de envío por conexión**,
pero los feeds empujan solo **cuando cambia** el dato (explícito en F2/F5: "si no hay
cambio, no se empuja"). Diagonal:

- **pico_doc**: frecuencia máxima documentada del push (techo de diseño).
- **promedio**: cota realista respaldada por la propiedad "push on change"; no es
  medición — se validará en F2.6c.

### Bybit — por símbolo (perpetual lineal)

| Stream | msg/s pico (doc) | msg/s promedio | B/msg | B/s pico |
|---|---|---|---|---|
| orderbook depth-50 | 50 | ~2–5 (deltas en calma; más en compresión de mercado) | 250 | ~12.5 KB/s |
| orderbook snapshot | 1 (transitorio) | ~0 (solo resync) | 3 KB | 3 KB (evento) |
| funding | 1/8h ≈ 0.00003 | ~0 | 300 | despreciable |
| oi | 1 | 1 | 300 | 300 B/s |
| liquidations | 2 | 0–0.2 (evento reales escasos) | 300 | 600 B/s |

### KuCoin (spot, deshabilitado hoy — para diseño)

| Stream | msg/s pico (doc) | msg/s promedio | B/msg | B/s pico |
|-------|------------------|-----------------|-------|----------|
| orderbook increment@10ms | 100 | ≤5 | 300 | hasta 30 KB/s |
| mark/index/OI | 1 | 1 | 300 | 300 B/s |
| funding rate | 1/60 | 1/60 | 300 | despreciable |

## 5. Tabla de capacidad agregada (por exchange/símbolo/topic)

Escenario de diseño: canario F2.6b = **Bybit, 3 símbolos (BTC/ETH/SOL PERP)**.

| Exchange | Símbolos | Stream | msg/s prom (pico) | B/s prom (pico) | B/msg |
|---|---|---|---|---|---|
| bybit | 3 | orderbook | ~9 (150) | ~2.3 KB/s (37.5 KB/s) | 250 |
| bybit | 3 | funding | ~0 | ~0 | 300 |
| bybit | 3 | oi | 3 (3) | ~1 KB/s | 300 |
| bybit | 3 | liquidations | ~0.6 (6) | ~180 B/s (1.8 KB/s) | 300 |

**Total Bybit (3 símbolos, pico de diseño):** ~160 msg/s, **~40 KB/s wire**.
Promedio steaty: ~12 msg/s, ~4 KB/s wire (funding despreciable; oi 1/s, liquidations
casi nulas, deltas de libro dominantes).

**Con overhead Kafka (~90 B/msg):**

- **Pico:** 160 × (250 + 90) ≈ **~54 KB/s pico de ingreso al broker**.
- **Promedio:** ~12 × (250 + 90) ≈ **~4 KB/s promedio**.

Si se habilitara KuCoin (2 símbolos más): añade hasta 200 msg/s increment@10ms
(~60 KB/s wire) más OI/BBO → **total pico ≈ 360 msg/s, ~100-115 KB/s** (aún trivial).

## 6. Kafka throughput (ingresos)

- **Ingresos agregados:** ~54 KB/s pico Bybit (≈ 4.7 GB/día en el peor caso teórico
  sostenido; pero el pico no es sostenido: promedio ~4 KB/s ≈ 0.3 GB/día). La retención
  de `orderbook.raw` es 1h (en inicio de Kappa en `shared/kafka/schemas/orderbook.py`),
  reduciendo el costo de almacén.
- **Broker:** un único broker Kafka en `docker compose` (orangehouse) maneja varios MB/s;
  ~55 KB/s pico es ~2 % de las capacidades nominales de un broker local.
- **Peak transitorio:** resync de los 3 libros simultáneos ≈ 3 × 3 KB = ~9 KB en una
  breve ventana; sin riesgo de backpressure.
- **No se requiere particionado adicional** para el canario; Kafka con partición única
  por tópico (routing key `make_symbol_key`) basta. F2.6d re-evalúa con la medición F2.6c.

## 7. Latencia teórica (presupuesto, no medición)

Presupuesto de diseño en un host único `orangehouse`:

| Segmento | p50 | p99 |
|---|---|---|
| Exchange → cliente WS (public, RTT de red) | 5–15 ms | 50–100 ms |
| Decodificación + construcción del payload en `streaming` | <1 ms | <5 ms |
| **Ingesta app → broker Kafka (loopback/kafka local)** | **1–10 ms** | **20–100 ms** |
| **End-to-end: broker → consumer `trading`** | **<1 ms** (loopback) | 5–50 ms |
| **Total E2E dato-a-consumo (teórico)** | **~10–30 ms** | **~60–150 ms** |

> El presupuesto es **teórico** (cota de diseño, no medido). La medición real E2E p50/p99 es
> F2.6c (canary bajo una unit systemd). El motor `trading` asume estas latencias
> para el mapeo de órdenes; el baseline R9/R10 no depende de este presupuesto.

## 8. CPU / RAM teórico

- **CPU:** ~160 msg/s pico → tarea ligera. ≤1 core dedicado es suficiente para
  decodificación JSON concurrente (aiokafka). Todo proceso `streaming` estimado
  **< 1 core efectivo en pico** para el canary.
- **RAM:** estado del libro en `BookBuilder` en memoria:
  - depth-50 × 2 lados × 50 niveles × ~20 B → ~4 KB/símbolo.
  - 5 símbolos → **<20 KB** de estado de libro; con buffers (config
    `pipeline.realtime.max_stream_buffer: 50000`) + envelopes + overhead asyncio →
    **<50 MB presupuestario** para el proceso completo.
- CPU/RAM reales disponibles en orangehouse: se registrarán en F2.6c, contrastando
  el presupuesto con medición.

## 9. Conclusión (DOD)

La tabla anterior lleva fuente por celda (límites documentados oficiales + cálculo de
schemas de código). Resultado:

**"Proceso único `streaming` + Kafka local suficiente para el canary de F2.6b": SÍ.**
Un único proceso `streaming` (systemd + producers de los 4 topics en un mismo sustento)
con un broker Kafka local soporta el canary de F2.6b — **~160 msg/s pico / ~40 KB/s wire
(~54 KB/s con overhead Kafka)**, y **~12 msg/s / ~4 KB/s promedio**, con latencia E2E
teórica de 10–30 ms p50 / 60–150 ms p99. Esta suficiencia es **válida solo para el
canary de F2.6b** (Bybit, 3 símbolos PERP). La suficiencia para la **producción final**
(más exchanges/símbolos, 6–12 meses) no se declara aquí: se valida con medición
empírica y hardware real en **F2.6c** (msg/s, bytes/s, CPU, RAM, red, lag Kafka, p50/p99)
antes de cualquier decisión de escala (F2.6d). Esta tabla es el insumo teórico de esa
medición, no un sustituto de ella.

**Deficit específico del canario:** ningún déficit identificado en capacidad.
No se exige segundo proceso, ni particionado Kafka, ni orquestador para el canario.

**Umbral de invalidación (cuándo NO bastaría single-server):** cuando el pico se
acercara a la cota de un broker local — p.ej. >50–100 símbolos de libro activo
(profundidad 50+ sostenidos en mercados con tensión), o si F2.6c mide lag creciente,
>50% CPU o latencia p99 >500 ms. En ese punto esta tabla, con fuentes, alimenta
**F2.6d** (decisión con evidencia: Kafka multi-partición → workers, o tooling de
escala). Umbral documentado aquí; la decisión queda reservada a F2.6d con datos reales.

## 10. Supuestos y límites de este documento

- Los límites documentados son **cotas superiores de push**; los promedios derivan de
  la propiedad "push solo si cambio" (F2/F5/F6) — no de mediciones propias.
- No se incluye `trades.raw` (REST poll), que no forma parte del streaming WS.
- Hardware de orangehouse no registrado en doc único — se capturará en F2.6c
  (CPU cores, RAM, NIC) junto a la medición real.
- La **medición real** (msg/s, bytes/s, CPU/RAM, lag Kafka, p50/p99 end-to-end, QoS)
  es responsabilidad de **F2.6c**, una vez `streaming_hydra.py` (F2.6b) esté bajo systemd.
- Mensajes individuales (snapshot 3 KB, orden de niveles) no cambian el orden de
  magnitud de ingestiones Kafka para el canario.