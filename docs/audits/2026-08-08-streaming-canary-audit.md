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

---

## [F-009] Ausencia de retry/DLQ/gap detection en pipeline WS→Kafka
- **Severidad:** P2
- **Estado:** VERIFIED → **DEFERRED 2026-08-09 (ver ADR-0023)** — señal de fallo resuelta; recuperación bloqueada hasta consumidor real
- **ID formal:** H-24 (docs/plans/tracking.yaml)
- **Evidencia:** `KafkaProducerAdapter.send_async()` (packages/market_data/infrastructure/kafka/producer.py)
  captura toda excepción, logea `warning`, retorna `False` — sin reintento, sin cola de
  mensajes fallados, sin circuit breaker. Confirmado en 2 capas: los 4 producers WS
  (`on_snapshot`/`on_delta`) y el adapter subyacente. Flush() es la única excepción
  documentada al patrón SafeOps del archivo.
- **Dato relevante:** el enum de `reason` en `KafkaMetrics.event_failed()` ya contempla
  `dlq_sent` como valor válido (`deserialize_error | schema_mismatch | write_error | dlq_sent`),
  pero ningún código dispara ese reason — el DLQ fue anticipado en el diseño pero nunca
  implementado en el side del productor. (El lado consumidor OHLCV sí usa DLQ: `bronze_writer`).
- **Impacto:** un delta de order book perdido deja el book local desincronizado sin
  mecanismo automático de detección (no hay sequence numbers verificados en
  `OrderBookDeltaPayload`) ni de recuperación.
- **Decisión 2026-08-09 (ADR-0023):** DIFERIR oficialmente la implementación hasta
  que exista un consumidor real de `orderbook.raw` (hoy BookBuilder es aspiracional;
  `grep TOPIC_ORDERBOOK_RAW` en packages/apps = solo el producer; GROUP_BOOK_BUILDER/
  TOPIC_BOOK_DELTA/TOPIC_BOOK_SNAPSHOT = 0 usos productivos). Cambiar el wire schema
  sin beneficiario sería especulativo. Condiciones de reapertura: `book.sequence_number`
  ya expuesto por cryptofeed (cryptofeed_orderbook_stream.py:158-161) → sequence aditivo
  en schema v2, métrica `ocm_kafka_gap_total`, reutilizando `TOPIC_DLQ`.
- **Mitigado parcialmente (bloques 1-3):** fallo visible vía `KafkaProducerError` (F-013)
  y causas clasificadas en `send_async()` (F-019); alerta `KafkaWSEventsFailed` sobre
  `ocm_kafka_events_failed_total` (F-010 resuelto). Lo pendiente es recuperación.

## [F-010] Sin alertas operacionales para el pipeline WS/Kafka
- **Severidad:** P2
- **Estado:** RESUELTO 2026-08-09
- **ID formal:** H-25 (docs/plans/tracking.yaml)
- **Evidencia:** `deploy/monitoring/alerts.yml` contiene 4 reglas (`PipelineDown`,
  `HighFetchErrors`, `CircuitBreakerOpen`, `KafkaWSEventsFailed`), las 4 cubren
  el pipeline batch/REST + WS→Kafka (`ocm_pipeline_*`, `ocm_fetch_errors_*`,
  `ocm_exchange_circuit_*`, `ocm_kafka_events_failed_total`).
- **Impacto (antes):** `ocm_kafka_events_failed_total` existía como métrica (confirmado
  en `metrics.py:115-119`, labels `[topic exchange reason]`) pero no disparaba ninguna
  alerta — sin señal proactiva, registro inicial aceptable.
- **Remediacion aplicada (2026-08-09):** nueva regla `KafkaWSEventsFailed`
  `rate(ocm_kafka_events_failed_total[5m]) > 1` durante 5m, severidad warning,
  metrica real verificada en `metrics.py` (labels `[topic exchange reason]`).
  `yaml.safe_load(alerts.yml)` OK. Con F-016 (B-32) bloqueado por secretos, la
  no notifica a ningún humano (grafana/loki la exponen) — dependencia abierta.
- **Relación con F-009:** complementario — F-009 es la ausencia de recuperación
  automática (pendiente, B-25), F-010 era la ausencia de notificación humana (cerrado).

## [F-011] `infrastructure/` en raíz del repo — colisión de nomenclatura con la capa Clean Architecture
- **Severidad:** P3
- **Estado:** VERIFIED → **RESUELTO (2026-08-09)** — remediación segura aprobada aplicada
- **ID formal:** H-26 (docs/plans/tracking.yaml)
- **Evidencia:** existe `infrastructure/` como directorio hermano de `packages/` en la
  raíz del repo (`infrastructure/redis/redis_stream.py`, `infrastructure/__init__.py`),
  registrado en `pyproject.toml` (líneas 249, 330, 341: excepción ruff E402 + path
  mapping). Es un módulo standalone legítimo — no importa nada de `packages/`, solo
  `redis`/`loguru`/stdlib — pero el nombre colisiona conceptualmente con la capa
  `infrastructure/` que cada bounded context ya usa (`packages/market_data/infrastructure/`,
  etc.), la misma palabra con dos significados arquitectónicos distintos en el mismo repo.
- **Consecuencia concreta:** el docstring de `packages/market_data/infrastructure/timeouts.py`
  instruye `from infrastructure.timeouts import Timeouts` — un import que **no existe**
  (`ModuleNotFoundError` confirmado). Documentación desactualizada o nunca correcta,
  probablemente residuo de una reubicación de archivo sin actualizar el ejemplo de uso.
- **Remediación aplicada (2026-08-09):** (1) docstring de `timeouts.py` corregido a la ruta
  canónica `market_data.infrastructure.timeouts`, con nota F-011; (2) coexistencia de la raíz
  `infrastructure/` vs `packages/*/infrastructure/` documentada explícitamente en
  ADR-0007 §Addendum 2026-08-09. Sin renombrar/mover/alterar imports.
- **No es una violación de import-linter ni de Clean Architecture en sentido estricto**
  (no hay dependencia cruzada real) — es un hallazgo de naming/Clean Code y de
  documentación falsa dentro del propio código.

## [F-012] Timeouts con claims de p99/SLA sin fuente verificable
- **Severidad:** P3
- **Estado:** VERIFIED (el claim existe tal cual) → **RESUELTO (2026-08-09)** — claims reformulados
  a heurística, valores numéricos intactos
- **ID formal:** H-27 (docs/plans/tracking.yaml)
- **Evidencia:** `packages/market_data/infrastructure/timeouts.py` — comentarios como
  "p99 en Bybit/KuCoin spot: ~800ms" (línea 46), "scan() sobre S3... p99 ~3s" (línea 52),
  y el docstring general ("Los valores reflejan SLAs observados en producción") presentan
  cifras cuantitativas específicas sin indicar fuente (¿medición propia de OCM? ¿doc
  oficial del exchange? ¿heurística del autor?).
  cuantitativos sin fuente citable — indistinguibles de datos medidos para quien lea el
  código después.
- **Riesgo:** en un sistema de trading, un timeout mal calibrado (demasiado corto → falsos
  positivos de fallo; demasiado largo → latencia de detección de problemas reales) es un
  parámetro operativo crítico. Presentarlo como "observado" sin serlo dificulta que alguien
  audite o recalibre esos valores con confianza.
- **Remediación aplicada (2026-08-09):** docstring y comentarios reformulados a lenguaje
  explícitamente heurístico — "Cota operativa inicial... NO medición garantizada: recalibrar
  con telemetría real" y comentarios inline a "~0.8s latencia típica en spot Bybit/KuCoin" /
  "tarda típicamente ~3s en tablas <100GB (no medido en prod aún)". Los valores numéricos de
  los timeouts NO se modificaron (sin cambio funcional).

## Kafka failure semantics / delivery guarantees (sesion 2026-08-08, continuacion)

### [F-013] KafkaProducerAdapter.produce() descarta el bool de send_async() -> falso event_published
- **Severidad:** P0
- **Estado:** VERIFIED
- **ID formal:** B-29 (docs/plans/tracking.yaml)
- **Evidencia:** `packages/market_data/infrastructure/kafka/producer.py` — `produce()`
  (firma `-> None`) hace `await self.send_async(...)` sin capturar ni evaluar el bool
  de retorno. `send_async()` retorna `False` ante cualquier excepcion de
  `AIOKafkaProducer.send_and_wait()` (broker reject, timeout, conexion, etc.), pero
  `produce()` la descarta y retorna `None` sin excepcion en todos los casos.
- **Impacto:** `OrderBookKafkaProducer.on_snapshot()`/`on_delta()` (y el mismo patron
  confirmado en `OIKafkaProducer`, `LiquidationsKafkaProducer`, `FundingKafkaProducer` —
  4 de 6 producers WS) llaman a `self._producer.produce(...)` y disparan
  `event_published`/`event_processed` incondicionalmente tras el `await` sin excepcion,
  incluyendo los casos donde Kafka rechazo o no confirmo el mensaje. `event_failed` solo
  se dispara por excepciones *previas* a `produce()`, nunca por fallo real de entrega.
- **Riesgo trading/market-data:** metricas de exito de publicacion pueden estar
  sistematicamente incorrectas; imposible distinguir perdida real de datos de mercado
  via dashboards/alertas basadas en
  `ocm_kafka_events_published_total`/`ocm_kafka_events_processed_total`.
- **Principio afectado:** SafeOps mal aplicado — absorbe la excepcion a nivel correcto
  (`send_async()`) pero pierde la senal en la capa que la propaga (`produce()`).
- **Remediacion propuesta (dos caminos, decision pendiente):**
  - Camino A (sin ADR): `send_async()==False` dispara log critical y/o metrica adicional
    dentro de `produce()`, sin cambiar su firma publica `-> None`.
  - Camino B (requiere ADR): cambiar `produce()` a `-> bool`, obliga a los 4+ callers a
    manejar el resultado — modifica el contrato compartido de `KafkaProducerPort`.
- **Requiere:** codigo + tests. ADR solo si se elige Camino B.

### [F-014] KafkaProducerAdapter.close() no hace flush() antes de stop() del cliente
- **Severidad:** P0
- **Estado:** VERIFIED
- **ID formal:** B-30 (docs/plans/tracking.yaml)
- **Evidencia:** `producer.py::close()` llama directo a `await self._producer.stop()`,
  sin invocar `self.flush()`. El docstring del metodo publico `stop()` afirma "flush
  implicito" — falso, no hay tal llamada en el codigo.
- **Impacto:** mensajes en el buffer local del cliente (`linger_ms=5`, `max_batch_size`
  configurados) pueden perderse en cualquier shutdown ordenado (SIGTERM, ADR-0022) sin
  ninguna traza.
- **Riesgo trading/market-data:** perdida de datos de mercado en cada restart/deploy del
  streaming canary, silenciosa.
- **Remediacion propuesta:** `close()` debe invocar `await self.flush(timeout=...)`
  antes de `self._producer.stop()`, con manejo explicito de `TimeoutError`.
- **Requiere:** codigo + test de shutdown. No requiere ADR (fix interno).

### [F-015] AIOKafkaProducer sin enable_idempotence explicito
- **Severidad:** P1
- **Estado:** VERIFIED
- **ID formal:** B-31 (docs/plans/tracking.yaml)
- **Evidencia:** `producer.py` instancia `AIOKafkaProducer(...)` sin
  `enable_idempotence`. Queda en el default de aiokafka (`False`), con `acks="all"`
  ya configurado.
- **Impacto:** un retry tras ack perdido en red puede duplicar el mensaje en el topic.
- **Riesgo trading/market-data:** duplicados en `orderbook.raw` y topics equivalentes.
- **Remediacion propuesta:** evaluar `enable_idempotence=True` en los 4+ producers que
  comparten `KafkaProducerAdapter`.
- **Requiere:** codigo + tests. No requiere ADR (parametro de configuracion).

### [F-016] Alertmanager receiver 'default' sin destino configurado
- **Severidad:** P0
- **Estado:** VERIFIED → **BLOQUEADO (2026-08-08, reconfirmado 2026-08-09): sin destino seguro disponible en el repo**
- **ID formal:** B-32 (docs/plans/tracking.yaml)
- **Evidencia:** `deploy/monitoring/alertmanager.yml` — `route.receiver: 'default'`,
  `receivers: [{name: 'default'}]` sin `slack_configs`/`email_configs`/
  `webhook_configs`/`pagerduty_configs`. Confirmado que `deploy/monitoring/alerts.yml`
  se monta en `/etc/prometheus/alerts.yml` via `docker-compose.yml:161-162` — es la
  config real desplegada, no teorica.
- **Impacto:** cualquier alerta que dispare (incluidas `PipelineDown`,
  `HighFetchErrors`, `CircuitBreakerOpen`, mas las que B-26/F-010 proponga) no
  notifica a ningun humano por ningun canal.
- **Decisión 2026-08-08/09 (bloqueo durable):** el repo NO contiene ruta de notificación
  segura (webhook/Slack/SMTP con credenciales gestionadas) ni secret manager. Configurar
  un receiver obligaría a commitear credenciales (rechazado por política no-secrets,
  GOVERNANCE.md) o a inventar un destino (no verificado). El bloqueo está documentado
  en el propio `alertmanager.yml` (comentario F-016) y reforzado por:
  - `.github/workflows/gitleaks.yml` — scan de secretos en push/PR a main.
  - `docker-compose.yml:63-78` config-guard — impide levantar prometheus/alertmanager
    con config rota (un `$(VAR)` sin definir rompería el arranque).
  - Mitigación activa: observabilidad interna (grafana/loki) expone las alertas.
- **Condición de reapertura:** existir secret manager externo en el repo.
  No simular un destino sin credenciales reales.
- **Requiere:** configuración exclusivamente; no requiere ADR. Para implementación el
  arranque con destino `$(OCM_ALERT_*)` requiere infra externa (TBD con secret manager).

### [F-017] _PUSH_EXCHANGE hardcodeado invalida aislamiento por job en Pushgateway
- **Severidad:** P0
- **Estado:** RESUELTO 2026-08-08 — **re-auditado 2026-08-09 (aislamiento confirmado en config actual)**
- **ID formal:** B-33 (docs/plans/tracking.yaml)
- **Evidencia:** `apps/app/cli/streaming_hydra.py:73` — `_PUSH_EXCHANGE = "orderbook"`,
  constante fija usada como `exchange` en `push_metrics(exchange=_PUSH_EXCHANGE, ...)`.
  El comentario de diseno ("un job por exchange evita last-write-wins") es falso en la
  practica porque el label nunca varia entre exchanges.
- **Impacto:** si 2+ procesos de streaming corren en paralelo (multi-exchange), todos
  comparten `job=ocm_pipeline_orderbook` en el Pushgateway -> last-write-wins real.
- **Remediacion aplicada (2026-08-08):** eliminada la constante; `_heartbeat_loop(pusher,
  exchange, gateway, stop, push_interval)` propaga el `exchange` real del stream y
  `pusher.push({"exchange": exchange, ...})` deriva `job=ocm_pipeline_{exchange}`
  (prometheus.py construye el job desde el label). Test dedicado `test_exchange_is_derived_not_hardcoded`.
- **Re-auditoria 2026-08-09:** confirmado en `streaming_hydra.py` — `--exchange` CLI
  (linea 95) es SSOT, `_run_streaming(exchange=...)` (213), `_heartbeat_loop` recibe
  `exchange` (243) y el push usa el valor real (169); 7 tests de `test_streaming_hydra.py`
  verdes. Sin `_PUSH_EXCHANGE` en el archivo.
- **Requiere:** codigo + test de aislamiento entre 2 exchanges. No requiere ADR.

### [F-018] Alerta PipelineDown referencia metrica inexistente en el codigo actual
- **Severidad:** P0
- **Estado:** RESUELTO 2026-08-08 — **re-auditado 2026-08-09 (solo metricas reales en alerts.yml)**
- **ID formal:** B-34 (docs/plans/tracking.yaml)
- **Evidencia:** `deploy/monitoring/alerts.yml:6` —
  `expr: absent(ocm_pipeline_runs_total)`. Busqueda exhaustiva no encuentra esa
  metrica en ningun `.py` del proyecto. La metrica real es
  `ocm_pipeline_heartbeat_total` (`ocm/observability/prometheus.py:30`).
- **Impacto:** la unica alerta que en teoria cubre el deadman-switch del canary esta
  rota, posiblemente residuo de la era Prefect.
- **Remediacion aplicada (2026-08-08):** `alerts.yml` reescrito: deadman por staleness
  `(time() - ocm_pipeline_last_run_timestamp) > 600` + `absent()` (el pushgateway persiste
  series de jobs muertos — el deadman real compara la gauge, ver nota en el propio YAML),
  `HighFetchErrors` → `rate(ocm_fetch_chunk_errors_total[5m])`,
  `CircuitBreakerOpen` → `ocm_exchange_circuit_open_total > 0`.
- **Re-auditoria 2026-08-09 (metricas reales confirmadas):** grep de los nombres usados
  en `alerts.yml` contra el codigo:
  `ocm_pipeline_last_run_timestamp`=1, `ocm_pipeline_heartbeat_total`=1,
  `ocm_fetch_chunk_errors_total`=1, `ocm_exchange_circuit_open_total`=1 hit; las tres
  metricas falsas previas (`ocm_pipeline_runs_total`, `ocm_fetch_errors_total`,
  `ocm_exchange_circuit_open`) = 0 hits. `yaml.safe_load` OK. Ninguna regla referencia
  metrica inexistente.

### [F-019] Captura generica en send_async() sin distinguir causas de fallo Kafka
- **Severidad:** P1
- **Estado:** VERIFIED
- **ID formal:** B-35 (docs/plans/tracking.yaml)
- **Evidencia:** `producer.py::send_async()` tiene una unica clausula
  `except Exception`. Confirmado contra `aiokafka.errors`: ~44 subclases de
  `BrokerResponseError` mas `KafkaTimeoutError`/`KafkaConnectionError`/
  `KafkaUnavailableError` colapsan todas en el mismo log `kafka_send_failed` y el
  mismo `reason="write_error"` en la metrica. No existe DLQ, retry topic, ni
  persistencia para replay para ninguno de estos tipos.
- **Impacto:** imposible diagnosticar operacionalmente si una falla es transitoria
  vs permanente sin revisar logs manualmente.
- **Remediacion propuesta:** se resuelve naturalmente al abordar F-009/B-25 (DLQ/gap
  detection ya pendiente) — no requiere ADR propio, extiende el mismo espacio de
  problema.
- **Requiere:** codigo (clasificacion de excepciones). ADR ya cubierto por B-25.

### [F-020] OnchainKafkaProducer hereda de BaseKafkaProducer, clase no encontrada en el repo
- **Severidad:** P3 (dead code — import roto confirmado)
- **Estado:** RESUELTO 2026-08-08 (onchain_producer.py eliminado; rg global 0 hits post-eliminacion)
- **ID formal:** B-36 (docs/plans/tracking.yaml)
- **Evidencia (ejecutada):** `onchain_producer.py:9` —
  `from infra.kafka.base_producer import BaseKafkaProducer`. Comando de
  verificacion: `uv run python -c "import market_data.adapters.inbound.websocket.onchain_producer"`
  → `ModuleNotFoundError: No module named 'infra'`. Ademas `onchain_producer.py:13`
  import `from market_data.domain.ports.kafka_producers import OnchainKafkaProducerProtocol`
  (ruta inexistente; los ports viven en `market_data/ports`, no en
  `market_data/domain/ports`) → mismo `ModuleNotFoundError`.
  Busqueda de callers global (`rg onchain_producer|OnchainKafkaProducer`): 0
  hits fuera del propio archivo; `composition_root` no lo importa.
- **Riesgo confirmado:** module no importable; sin embargo es dead code aislado
  (nada lo importa) — no rompe runtime actual del canary.
- **Remediacion propuesta:** eliminar `onchain_producer.py`. No requiere ADR.

### [F-021] Metodo legacy produce(payload, key) en OrderBookKafkaProducer sin callers
- **Severidad:** P3
- **Estado:** RESUELTO 2026-08-08 (metodo legacy eliminado de los 4 producers WS; grep 0 callers pre-eliminacion)
- **ID formal:** B-37 (docs/plans/tracking.yaml)
- **Evidencia:** `orderbook_producer.py:229` expone `produce(self, payload: bytes,
  key=None)`. Grep de invocaciones a `.produce(` en los 4 producers WS legacy
  `(payload, key)` → **0 hits globales** (solo hits: el port
  `KafkaProducerPort.produce(topic,value,key,headers)`, metodo distinto con
  callers reales: orderbook via on_snapshot/on_delta, oi/funding/liquidations
  on_*, ohlcv_publisher, bronze_writer, external_kafka_publisher). Ademas
  `on_open_interest`/`on_funding_rate`/`on_liquidation` sin callers directos
  (grep 0 hits — los runners los inyectan por referencia).
- **Nota:** mismo patron hostil en oi/funding/liquidations — confirmado sin callers.
- **Remediacion propuesta (decision):** eliminar los metodos legacy
  `produce(payload, key)` de los 4 archivos. No requiere ADR.

### [F-022] Twin-import de metricas duplica series en el CollectorRegistry (nuevo)
- **Severidad:** P2
- **Estado:** RESUELTO 2026-08-08 (guard estatico activo)
- **ID formal:** B-038 (docs/plans/tracking.yaml)
- **Evidencia:** importar ambos registros de metricas en un solo proceso con
  doble ruta (`packages.market_data.infrastructure.observability.metrics` tras
  `market_data.infrastructure.observability.metrics`) lanza
  `ValueError: Duplicated timeseries in CollectorRegistry:
  {'ocm_pipeline_rows_ingested_total', ...}`. Causa raiz (corregida): es el
  pythonpath de `pyproject.toml` — `[tool.pytest.ini_options] pythonpath=['.','apps']`
  — que hace la raiz del repo importable y `packages/` visible como paquete
  gemelo (`packages.market_data.*`). No hay `pytest.ini` en el repo (verificado).
- **Riesgo:** fallo de import en entornos con ambos paths en sys.path (tests,
  mypy via packages).
- **Remediacion aplicada (2026-08-08):** ruta canonica unica `market_data.*`
  + guard estatico `tests/market_data/test_layer_contracts.py::test_no_twin_import_via_packages_prefix`
  (scan AST de todo el repo; falla si algun .py importa con prefijo `packages.`).
  1 passed, incluido en la suite. No requiere ADR.

### Pendientes de esta fase (no cerrados, explicitamente abiertos)
- `InfraMetricsKafkaProducer` — no auditado a fondo, tiene su propio `produce()` con
  semantica distinta, pendiente revision completa.
- Retry interno de `aiokafka` (`_message_accumulator`/`_sender`) — confirmado que
  `send()` no tiene retry loop propio visible; el mecanismo real queda en capas
  internas de la libreria no auditadas (el loop de retry vive en el SenderTask).
- **Cerrado en esta sesion (2026-08-08):** F-020 confirmado import roto real
  (dead code aislado); F-021 confirmado sin callers en los 4 producers legacy;
  F-022 nuevo (twin-import metricas P2). Ver secciones F-020/F-021/F-022.
- Gobernanza de ADR aplicada estrictamente (GOVERNANCE.md secc. 2): ninguno de
  F-013 a F-022 dispara ADR automaticamente salvo F-019 (ya cubierto por el ADR
  pendiente de B-25). F-013 resuelto por Camino A (sin cambio de contrato
  KafkaProducerPort) — no requiere ADR.

### [F-023] EngineeringHealth FAIL — prioridades B-24..B-28 fuera del enum normativo (detectado en gate)
- **Severidad:** P2 (gate de CI roto: tests/architecture/test_engineering_health.py)
- **Estado:** RESUELTO 2026-08-09 (normalizadas B-24..B-28; health check PASS)
- **ID formal:** B-39
- **Evidencia:** `uv run python scripts/engineering_health_check.py` reportaba
  5 incoherencias — `B-25: prioridad='P2'`, `B-26: P2`, `B-27: P3`, `B-28: P3`,
  `B-24: P2` — fuera del enum normativo `['ALTA','BAJA','CRITICA','MEDIA']`.
  Confirmado preexistente: `git show HEAD:docs/plans/tracking.yaml` muestra los 5
  ya con prioridad P2/P3 en HEAD; los items B-29..B-38 de esta sesion usan
  prioridades validas (CRITICA/ALTA/MEDIA/BAJA). El test de arquitectura que
  ejecuta el health check fallaba (1 failed en suite completa).
- **Riesgo:** CI rojo de forma preexistente; incoherencia normativa en el SSOT
  de tracking que rompe el gate de coherencia.
- **Remediacion aplicada (2026-08-09):** normalizadas B-24..B-28 mapeando las
  severidades P2/P3 del audit a severidades semánticas de la F-series:
  Verificado: health check exit 0 (PASS) y `test_engineering_health_passes` verde.
  No requiere ADR (dato del tracking, no arquitectura).

---

## Addendum 2026-08-09 — Hallazgo F-024 (nuevo, independiente de F-011)

### [F-024] `ocm.runtime.state.redis_stream` importado por factories.py pero módulo inexistente (ruta funcional rota)
- **Severidad propuesta:** P2 (import roto real en ruta funcional; hoy sin callers, pero si se alcanza falla con ModuleNotFoundError)
- **Estado:** PENDIENTE — VERIFICADO el import roto (reproducido por ejecución); NO resuelto
- **ID formal:** H-28 (docs/plans/tracking.yaml, entrada B-40)
- **Archivo exacto y imports:**
  - `ocm/runtime/state/factories.py:198` — `from ocm.runtime.state.redis_stream import RedisStreamPublisher` (dentro de `build_stream_publisher`)
  - `ocm/runtime/state/factories.py:236` — `from ocm.runtime.state.redis_stream import RedisStreamConsumer` (dentro de `build_stream_source`)
  - Ambos son imports LAZY (dentro de la función) y están **FUERA del try/except** (el try arranca línea 199) → la excepción se propaga al caller (no hay retorno silencioso).
- **Evidencia de reproducción (2026-08-09):**
  ```
  $ uv run python -c "from ocm.runtime.state.factories import build_stream_publisher; build_stream_publisher(stream_name='ohlcv')"
  ModuleNotFoundError: No module named 'ocm.runtime.state.redis_stream'
  ```
  El módulo `ocm.runtime.state.redis_stream` NO existe en el repo (ls de `ocm/runtime/state/`: adapters, cursor_store, encoding, factories, gap_*, lateness_calibration, retry — no hay redis_stream).
- **Callers:**
  - **factories.py ← importado por:** `ocm/runtime/state/__init__.py:67`, `adapters.py:19` (módulo-level, import OK), más `packages/market_data/adapters/inbound/rest/_cursor_factory.py:46`, `ohlcv_fetcher.py:40,208`.
  - **`build_stream_publisher`/`build_stream_source` en sí NO tienen callers** en `apps/`, `packages/`, `ocm/`, `tests/` (grep 0 hits fuera de su definición) ni se re-exportan en `ocm/runtime/state/__init__.py`. En hoy la ruta NO es alcanzada.
- **Símbolo equivalente real:** `RedisStreamPublisher` existe en `infrastructure/redis/redis_stream.py:71` y `RedisStreamConsumer` en `:145` (la raíz `infrastructure/` hermana de `packages/` — colisión ya documentada en F-011). **Ningún código importa ese módulo raíz** (único uso: el docstring falso de `timeouts.py:13`).
- **Origen:** `git log -S 'ocm.runtime.state.redis_stream' -- factories.py` → introducido en `66cd1c4 refactor(layout): Fase 3 — renombrar ocm_platform→ocm + lint-imports verde`; el rename de paquete no movió el archivo de streams a la nueva ruta.
- **Impacto:** latente — si se habilita el event bus Redis (`RedisStreamsEventBus` futuro, ver `ports/outbound/event_bus.py:23`) y se invoca `build_stream_publisher`/`build_stream_source`, falla con `ModuleNotFoundError`. Hoy sin efecto funcional (ningún caller).
- **Remediación propuesta (NO ejecutada, requiere auditoría de "qué debe importar"):** decidir el hogar canónico de `RedisStreamPublisher`/`Consumer` (mover `infrastructure/redis/redis_stream.py` a `ocm.runtime.state.redis_stream`, o apuntar factories.py a la ruta real), según resolución de F-011. Ver ADR-0023 (deferral) para política similar de no tocar sin beneficiario real.
- **No requiere ADR hasta decidir la ruta canónica** (cuando se defina, si mueve el módulo, actualizar importlinter/pyproject como ya señala F-011).

### [F-025] Comentario de alertmanager.yml asume observabilidad de respaldo (Grafana/Loki) que no esta operativa
- **Severidad:** P1
- **Estado:** VERIFIED
- **ID formal:** B-41 (docs/plans/tracking.yaml)
- **Evidencia:** el comentario agregado en F-016 (`deploy/monitoring/alertmanager.yml`)
  justifica dejar el receiver `default` sin destino argumentando "las alertas se
  resuelven HOY por observabilidad interna (grafana/loki)". Verificado en vivo:
  `docker compose ps` falla al interpolar el servicio `grafana` — `GRAFANA_PASSWORD`
  no esta seteada en `.env` (aunque si existe como placeholder vacio en
  `.env.example:26`). Grafana no puede levantar en el estado actual del repo.
- **Impacto:** el razonamiento que sostiene la decision de F-016 (dejar Alertmanager
  sin receiver) se apoya en un respaldo que no existe operativamente hoy — no hay
  ninguna via de notificacion real, ni por Alertmanager ni por Grafana/Loki, mientras
  `.env` no tenga `GRAFANA_PASSWORD`.
- **Remediacion propuesta:** copiar `GRAFANA_PASSWORD` (y verificar el resto de
  variables de `.env.example` relacionadas a monitoring) a `.env`, confirmar que
  `grafana`/`loki` levantan, y solo entonces el comentario de F-016 queda respaldado
  por la realidad. Accion operativa simple, no requiere codigo ni ADR.
