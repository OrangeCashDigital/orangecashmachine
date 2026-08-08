# Auditoría — Orderbook Streaming Canary (F2.6b/F2.6c)

**Fecha inicio:** 2026-08-08
**Auditor:** Claude (sesión interactiva con orangemusic)
**Alcance:** `08e9696 feat: add orderbook streaming canary`, `2cf1c0e fix: repair tracking yaml syntax`
**Estado:** EN PROGRESO — auditoría incompleta, ver sección "Cobertura" al final

**Regla de honestidad:** todo hallazgo se marca VERIFIED (confirmado en código/evidencia
directa vista en esta sesión), UNVERIFIED (pendiente de comprobar) o INFERENCE (deducción
razonable, no demostrada). Nada se marca HECHO/correcto sin evidencia directa.

---

## Registro de hallazgos

### [F-001] KafkaMetrics — diseño fail-soft con No-Op fallback
- **Severidad:** P4 (positivo, no es un problema — anotado como diseño confirmado)
- **Estado:** VERIFIED
- **Evidencia:** `packages/market_data/infrastructure/kafka/metrics.py` — `try/except ImportError`
  sobre `prometheus_client`, con `_NoOpCounter`/`_NoOpHistogram` implementando el mismo
  `Protocol` (`_CounterProtocol`, `_HistogramProtocol` via `runtime_checkable`). Si Prometheus
  no está instalado, las llamadas a `.labels().inc()`/`.observe()` son no-op sin excepción.
- **Nota:** confirma "zero overhead sin Prometheus" tal como declara el docstring del módulo.

### [F-002] KafkaMetrics — ubicación en market_data, no en shared/kafka
- **Severidad:** P3
- **Estado:** VERIFIED
- **Evidencia:** ruta real `packages/market_data/infrastructure/kafka/metrics.py`, no
  `shared/kafka/`. Implica que si otro bounded context (ej. `trading`) necesitara la misma
  instrumentación de Kafka, no puede reusar esta clase sin violar BC-10/BC-50 — tendría que
  reimplementarla o se necesitaría promoverla a `shared/kafka/`.
- **Pendiente:** confirmar si esto ya se consideró explícitamente (ver ADR-0022 addendum) o
  si es deuda no documentada.

### [F-003] KafkaMetrics API — labels topic/exchange/reason, tres Counters + un Histogram
- **Severidad:** N/A (documentación de diseño)
- **Estado:** VERIFIED
- **Evidencia:** `KafkaMetrics(topic: str = "ohlcv.raw")`; métodos `event_published(exchange)`,
  `event_processed(exchange, latency_ms)`, `event_failed(exchange, reason)`. Métricas:
  `ocm_kafka_events_published_total`, `ocm_kafka_events_processed_total`,
  `ocm_kafka_events_failed_total`, `ocm_kafka_processing_latency_ms`.
- **Pendiente crítico:** confirmar que `orderbook_producer.py` instancia
  `KafkaMetrics(topic="orderbook.raw")` y no deja el default `"ohlcv.raw"` por error de
  copy-paste — **no verificado todavía, es el siguiente archivo a auditar**.

### [F-004] streaming_hydra.py — reutiliza _bootstrap.setup_logging
- **Severidad:** N/A (documentación de diseño)
- **Estado:** PARTIALLY VERIFIED
- **Evidencia:** `_setup_logging(debug)` importa `from app.cli._bootstrap import setup_logging`
  con comentario explícito `R14/AUDIT-H8: scaffolding de CLI en SOLA fuente`.
- **Pendiente:** confirmar si también importa (o evita deliberadamente, como se había
  planificado) `handle_sigterm` — el addendum de ADR-0022 documentaba que NO debía heredar
  `handle_sigterm` por no ser asyncio-safe. Falta ver el manejo de señales real del archivo.

### [F-005] streaming_hydra.py — _load_config sin contexto Hydra, requiere run_id en producción
- **Severidad:** P2 (a confirmar impacto — reproducibilidad)
- **Estado:** VERIFIED (existencia del comportamiento) / UNVERIFIED (si el canary lo cumplió)
- **Evidencia:** `_load_config(env, run_id)` usa `load_appconfig_standalone` (BC-51). Docstring:
  "run_id obligatorio en producción, donde load_appconfig_standalone escribe snapshot de
  auditoría y falla sin él (hydra_loader.py:263)".
- **Pendiente:** confirmar si el canary corrió con `env=production` o `env=development`/`test`
  — si fue development, el snapshot de auditoría (necesario para reproducibilidad, sección 14
  de la auditoría) puede no haberse generado.

### [F-006] streaming_hydra.py — _build_pusher, fail-soft (PrometheusPusher | NoopPusher)
- **Severidad:** N/A (documentación de diseño)
- **Estado:** PARTIALLY VERIFIED
- **Evidencia:** docstring "`PrometheusPusher` si `metrics.enabled`, `NoopPusher` si no.
  Fail-Soft." — cuerpo de la función aún no visto completo.
- **Pendiente:** confirmar labels reales usados (¿`exchange="orderbook"` como se acordó, para
  evitar colisión de jobs en el Pushgateway?), y de dónde saca la URL del gateway (¿`ctx.pushgateway`
  como SSOT, según lo acordado, o hardcoded?).

---

## Cobertura de la auditoría (checklist de la sección 2 del pedido original)

| Archivo | Estado |
|---|---|
| `apps/app/cli/streaming_hydra.py` | PARCIAL — visto fragmentado, falta reconstrucción completa y verificación línea por línea |
| `packages/market_data/adapters/inbound/websocket/orderbook_producer.py` | NO VISTO |
| `packages/market_data/infrastructure/kafka/metrics.py` | COMPLETO (reconstruido por fragmentos solapados) |
| `packages/market_data/infrastructure/bootstrap/composition_root.py` (diff) | NO VISTO |
| `ocm/config/hydra_loader.py` | NO VISTO |
| `ocm/config/schema.py` | PARCIAL (visto en sesión anterior, no en el contexto de este canary) |
| `ocm/runtime/run_config.py` | NO VISTO |
| `ocm/runtime/context.py` | NO VISTO |
| `ocm/observability/prometheus.py` | NO VISTO |
| `ocm/observability/pushers.py` | PARCIAL (un fragmento visto en sesión anterior) |
| `config/env/production.yaml` | NO VISTO |
| `config/market_data/feeds.yaml` | NO VISTO |
| `architecture/importlinter.toml` (diff: +3 líneas) | NO VISTO — qué contrato se agregó |
| `pyproject.toml` (diff: +9/-0) | NO VISTO |
| `tests/app/cli/test_streaming_hydra.py` | NO VISTO |
| `tests/market_data/.../test_orderbook_producer_metrics.py` | NO VISTO |
| `tests/architecture/*` | NO VISTO |
| `scripts/engineering_health_check.py` | NO VISTO |
| `scripts/app_layer_guard.py` | VISTO en sesión anterior (R14/H8), no re-verificado contra este canary |
| `docs/plans/tracking.yaml` (diff: +41/-?) | NO VISTO |
| `2cf1c0e fix: repair tracking yaml syntax` | NO VISTO — qué se rompió y por qué |
| Evidencia empírica del canary (logs, métricas reales, duración, symbols) | NO VISTA — nada confirma que corrió, por cuánto tiempo, ni con qué resultado |

**Conclusión de cobertura:** la auditoría está lejos de completa. No hay evidencia todavía
para las secciones 6 (Market Data Audit), 7 (Trading-Infrastructure Standards), 8 (Latency
Audit), 9 (Capacity Engineering), 10 (Failure Modes), 13 (Security), 14 (Reproducibility) del
pedido original. Cualquier veredicto (sección 19) emitido en este punto sería prematuro y
violaría la propia regla de honestidad del documento auditor.

**Siguiente paso:** continuar la verificación archivo por archivo, en orden de prioridad:
1. `orderbook_producer.py` (confirmar F-003)
2. `streaming_hydra.py` completo (cerrar F-004, F-006)
3. diff de `tracking.yaml` + el commit `2cf1c0e` (¿qué syntax error hubo y por qué?)
4. `tests/app/cli/test_streaming_hydra.py` (sección 4 — testing audit)
5. evidencia empírica real del canary (¿corrió? ¿cuánto tiempo? ¿qué métricas arrojó?)

### [F-007] orderbook_producer.py — exception swallowing en el publish path
- **Severidad:** P1 (production blocker — pérdida silenciosa de datos de mercado)
- **Estado:** VERIFIED (el patrón), UNVERIFIED (si hay compensación en otro lado)
- **Evidencia:** bloque de publish (método aún sin nombre confirmado, probablemente
  `on_delta`/`on_snapshot`) — `except Exception as exc:` captura TODO, incrementa
  `self._metrics.event_failed(exchange=exchange, reason="write_error")`, loguea `.warning()`,
  y NO relanza. El mensaje de orderbook delta/snapshot se pierde silenciosamente si Kafka
  falla — el único rastro es una métrica incrementada, no hay retry, no hay DLQ visible en
  este bloque, no hay circuit breaker visible.
- **Impacto en market data correctness (sección 6):** si esto ocurre durante un gap real
  de mercado, el sistema no tiene forma de saber "perdí eventos" salvo mirando el contador
  `ocm_kafka_events_failed_total` manualmente — no hay gap detection ni resync automático
  visible en este archivo.
- **Pendiente:** ver el método completo desde su firma (nombre, parámetros), y si existe
  un mecanismo de reintento/DLQ en `self._producer.produce()` (KafkaProducerAdapter) que
  compense esto en una capa inferior.

### [F-008] orderbook_producer.py — HEADER_SOURCE hardcodeado a "live" (semánticamente incorrecto)
- **Severidad:** P2
- **Estado:** VERIFIED (el hardcode existe), UNVERIFIED (impacto real downstream)
- **Evidencia:** `_KAPPA_HEADERS: dict = {HEADER_SOURCE: "live", HEADER_DOMAIN: "orderbook"}`
  en `OrderBookKafkaProducer`. El valor `"live"` es constante de clase, no derivado del
  proceso real que publica. Este producer se invoca desde `streaming_hydra.py` (canary de
  market data), NO desde `live_hydra.py` (trading real con capital). Cualquier consumidor
  downstream que confíe en `HEADER_SOURCE` para distinguir origen (auditoría, provenance,
  filtrado) recibiría información falsa.
- **Relación con hallazgo previo:** `shared/kafka/provenance.py` (`require_promoted()`,
  Promotion Rule ADR-0017 §14) fue documentado como "no cubre hoy eventos de realtime_feeds"
  — pero si en el futuro se extiende, el valor "live" hardcodeado podría causar que datos
  de streaming/canary sean tratados incorrectamente como datos de trading en vivo.
- **Pendiente:** confirmar si algún consumidor real lee este header hoy, o si es aspiracional
  / sin consumidores todavía (lo que bajaría la severidad a P3).

### [F-007-UPDATE] Reclasificación tras evidencia adicional
- **Severidad revisada:** P1 → P2
- **Razón:** el `except Exception` amplio en `on_snapshot`/`on_delta` es DELIBERADO y
  documentado explícitamente: "SafeOps: captura cualquier excepción — el stream no debe
  morir porque el producer falle en un mensaje." Es un trade-off consciente de diseño
  (disponibilidad del stream > garantía de entrega de cada mensaje individual), consistente
  con el patrón SafeOps usado en el resto del proyecto (ver CompositionRoot.close() fail-soft
  visto en sesiones anteriores).
- **Lo que sigue siendo un hallazgo válido (no descartado):** no hay DLQ visible, no hay
  retry visible, no hay alerta activa sobre `ocm_kafka_events_failed_total` (solo la métrica
  existe, pero sección 11 de la auditoría original pregunta explícitamente si un on-call
  podría diagnosticar esto a las 3AM — eso depende de dashboards/alertas que aún no hemos
  visto, no confirmados en este repo todavía).
- **Institutional-grade gap:** firmas de trading electrónico normalmente NO aceptan pérdida
  silenciosa de order book deltas sin DLQ + resync automático (gap detection), porque un
  delta perdido puede dejar el book local desincronizado indefinidamente sin que nadie lo
  note hasta que se manifiesta como un crossed book o una decisión de trading mala. Esto es
  INFERENCE basada en prácticas públicas de la industria, no conocimiento propietario.

---

## Addendum — Cierre formal de F-008

- **ID formal en tracking:** H-23 (docs/plans/tracking.yaml, entrada B-24)
- **Estado:** RESUELTO — 2026-08-08
- **Fix aplicado:** los 4 producers WS (`OrderBookKafkaProducer`, `FundingKafkaProducer`,
  `OIKafkaProducer`, `LiquidationsKafkaProducer`) reciben ahora `source: DataSource`
  inyectado por constructor (default `DATASOURCE_LIVE`, preserva compat con
  `live_hydra.py` cuando lo adopte). `_KAPPA_HEADERS` pasó de constante de clase
  a atributo de instancia derivado del `source` real.
- **Composition root:** `CompositionRoot.build_ws_producers(bootstrap_servers, source=...)`
  propaga el valor a los 4 producers — único punto de decisión (DIP, BC-38).
- **Streaming canary:** `streaming_hydra.py` invoca con `source=DATASOURCE_REPLAY`
  explícito — el canary no genera señales de trading, por lo tanto no debe declararse
  `source="live"`.
- **Valor elegido:** `DATASOURCE_REPLAY` (no se extendió el `Literal DataSource` de
  `shared/enums.py` con un cuarto valor tipo "canary" — se evaluó y se descartó por
  alcance: hubiera requerido tocar `KappaSourceMixin`, `_VALID_SOURCES`, `provenance.py`
  y los 3 schemas de payload, una decisión de ADR, no de parche de auditoría).
- **Regresión de mayor alcance encontrada durante el fix:** el hardcode de `"live"`
  estaba duplicado idéntico en los 4 producers (violación DRY), y hoy en producción
  **solo** `streaming_hydra.py` los instancia — `live_hydra.py` no usa
  `build_ws_producers` todavía. Es decir, el 100% del tráfico real de estos 4 topics
  llevaba el header falso, no era un riesgo hipotético futuro.
- **Tests:** `tests/app/cli/test_streaming_hydra.py::TestRunStreaming::test_shutdown_order_and_return_zero`
  actualizado con assert explícito (`bundle.received_source == DATASOURCE_REPLAY`)
  para blindar contra reintroducción de este bug. 16/16 tests verdes
  (`test_orderbook_producer_metrics.py`, `test_composition_root.py`,
  `test_streaming_hydra.py`). `import-linter` (BC-08) sin violaciones.
- **Pendiente real (no cerrado por este fix, ver F-007):** sigue sin haber DLQ/retry/
  alerta activa sobre `ocm_kafka_events_failed_total`. F-008 corrige la semántica del
  header; no corrige la ausencia de gap detection.
