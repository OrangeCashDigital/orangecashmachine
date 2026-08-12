# Backlog priorizado por riesgo — auditoría streaming canary (2026-08-08)

**Fuente:** docs/audits/2026-08-08-streaming-canary-audit.md (SSOT de hallazgos)
**Tracking formal:** docs/plans/tracking.yaml (IDs H-NN / B-NN)
**Regla:** este documento es una vista de priorización, no una fuente de verdad
alternativa. Ante cualquier discrepancia, el audit.md y tracking.yaml mandan.

---

## 🔴 Capital / data-integrity risk

*(ver F-031 abajo — detectado 2026-08-10, ver nota de categoría que sigue)*

Criterio de entrada a esta categoría: el sistema opera o podría operar con datos
incorrectos/stale/corruptos **sin saberlo**, con riesgo directo de decisión de
trading errónea. Los hallazgos de hoy son de *ausencia de detección* ante fallos
ya visibles (logs, métricas), no de *corrupción silenciosa en curso*. Si F-009
(gap detection) se posterga indefinidamente, reevaluar si escala a esta categoría.

### F-031 / B-46 — El path Kappa OHLCV no está conectado: publisher Null + chunk_converter sin cablear (2026-08-10)
- **Severidad:** P1 | **Estado:** VERIFIED → **PARCIALMENTE RESUELTO** (2026-08-12, ver nota abajo; decisión A/B/C aún no elegida formalmente)
- `OHLCVPipeline` (incremental/backfill) publica a `NullPublisher()` hardcodeado
  (ohlcv_pipeline.py:248) y `_chunk_converter` no se inyecta (runtime.py:298);
  `_build_kafka_publisher` (pipeline_factory.py:156) no tiene callers. Nada llega a
  `ohlcv.raw` desde este pipeline; hoy las strategies fallan con `RuntimeError` en
  `get_chunk_converter()` ANTES de `publish_chunk` (incremental.py:106,
  backfill.py:427). Estado actual: fallo **visible** (no corrupto).
- **Riesgo latente (mayor daño):** si se remedia solo el converter sin publisher
  real, `NullPublisher.publish_chunk` retorna `True` → éxito simulado → cursor
  avanza, métricas suben, pero ningún evento llega a Kafka/Iceberg = **pérdida
  silenciosa de datos OHLCV** (bronze_writer de `ohlcv.raw` nunca recibe nada).
- **Decisión pendiente:** A) cablear Kappa real (KafkaOHLCVPublisher +
  `_chunk_converter` desde composition root); B) fail-fast sin silencio (Null
  publisher falla fuera de tests); C) degradación explícita configurada.
  Requiere decisión de arquitectura (ADR nuevo o addendum a ADR-0013/ADR-0014) +
  guard/test que verifique publisher != Null en producción.
- **Actualización 2026-08-12 (Guardrail #3):** `publish_chunk()` migró de bool
  ambiguo a `PublishResult(SUCCESS|RETRYABLE_FAILURE)` explícito (commit
  `5724191`) en `publisher_port.py`, `KafkaOHLCVPublisher`, `NullOHLCVPublisher`
  y los call-sites (`backfill.py`, `incremental.py`). Esto no resuelve la
  decisión A/B/C pendiente ni el riesgo latente de pérdida silenciosa descrito
  arriba — solo hace inequívoco el contrato de retorno para cuando se
  implemente la opción elegida.
- **Contradice:** 0002 "migración completa", ADR-0013/0014 "todo camino termina en
  Kafka", ADR-0022 addendum ("main.py gobierna ingestión polling→Bronze"),
  ADR-0023 nota (bronze_writer de `ohlcv.raw` como patrón existente sin productor —
  actualizacion 2026-08-12: esta contradiccion especifica ya no aplica, ver nota abajo).
  Relacionado: F-027/B-43 (KAFKA_ENABLED, confirmado RESUELTO como efecto colateral).
- **Actualización verificada (2026-08-12):** wiring confirmado con evidencia real
  (ver tracking.yaml B-46 y streaming-canary-audit.md). `_build_kafka_publisher()`
  tiene caller real, construye `KafkaOHLCVPublisher` con `produce()` real contra
  `ohlcv.raw`; `KafkaOHLCVPublisher` (el productor real de `ohlcv.raw`) ya existe
  y tiene caller — la nota de ADR-0023 sobre "sin productor" queda desactualizada.
  Correccion de rol: `bronze_writer` es el CONSUMIDOR de `ohlcv.raw`, no el
  productor — son piezas distintas del mismo flujo Kappa. Fail-fast real en
  produccion impide `NullOHLCVPublisher`. Riesgo residual: fuera de produccion
  (dev/paper/staging) sin Kafka, el pipeline aun cae a `NullOHLCVPublisher`
  (perdida silenciosa posible ahi). Decision A/B/C formal sigue pendiente — el
  codigo ya implementa algunas propiedades de A y B, pero la politica definitiva
  para entornos no productivos no fue formalizada como decision de arquitectura
  en ningun ADR.

---

## 🟠 Market-data reliability

### F-009 / H-24 / B-25 — Sin retry/DLQ/gap detection en pipeline WS→Kafka
- **Severidad:** P2 | **Estado:** DEFERRED 2026-08-09 (ADR-0023) — señal de fallo resuelta; recuperación pendiente hasta consumidor real
- Un delta de order book perdido no se detecta ni se recupera automáticamente.
- Bloqueador de diseño: requiere sequence numbers en `OrderBookDeltaPayload`
  (no existen hoy) + decisión de arquitectura de DLQ.
- **Decisión (ADR-0023, 2026-08-09):** deferir — no hay consumidor real de
  `orderbook.raw` (BookBuilder aspiracional; TOPIC_ORDERBOOK_RAW solo usado por el
  producer); cambiar el wire schema sin beneficiario sería especulativo. El DLQ
  infrastructure y el patrón consumidor-OHLCV ya existen (`bronze_writer`). Mitigado
  parcialmente: fallo visible (F-013/F-019) + alerta KafkaWS (F-010). Reapertura:
  BookBuilder consumidor → sequence aditivo v2 + `ocm_kafka_gap_total` + TOPIC_DLQ.

### F-010 / H-25 / B-26 — Sin alertas operacionales WS/Kafka
- **Severidad:** P2 | **Estado:** RESUELTO 2026-08-09
- `ocm_kafka_events_failed_total` existe pero no disparaba ninguna alerta.
- Menor esfuerzo que F-009: extensión directa de `deploy/monitoring/alerts.yml`
  siguiendo el formato de las 3 reglas ya existentes. Candidato a "quick win".
- **Implementacion (2026-08-09):** regla `KafkaWSEventsFailed` agregada —
  `rate(ocm_kafka_events_failed_total[5m]) > 1` durante 5m, severity warning; metrica
  real confirmada en `metrics.py:115-119` (labels `[topic exchange reason]`).
  `yaml.safe_load(alerts.yml)` OK. Notificacion humana pendiente de F-016 (B-32,
  bloqueado); la regla se visualiza en grafana/loki.

---

## 🟡 Architecture / maintainability

### F-011 / H-26 / B-27 — `infrastructure/` raíz ambiguo vs. capa Clean Architecture
- **Severidad:** P3 | **Estado:** VERIFICADO → **RESUELTO 2026-08-09**
- Sin dependencia cruzada real verificada — riesgo bajo, confusión de naming.
- **Remediación aplicada:** docstring falso en `timeouts.py` corregido a ruta canónica
  `market_data.infrastructure.timeouts`; coexistencia raíz `infrastructure/` vs
  `packages/*/infrastructure/` documentada en ADR-0007 §Addendum 2026-08-09. Sin
  renombrar/mover/alterar imports.

---

## 🔵 Documentation / hygiene

### F-012 / H-27 / B-28 — Timeouts con claims de p99/SLA sin fuente verificable
- **Severidad:** P3 | **Estado:** VERIFICADO → **RESUELTO 2026-08-09**
- No es defecto funcional; riesgo indirecto si alguien recalibra confiando en
  números no verificados como si fueran medición real.
- **Remediación aplicada:** docstring y comentarios reformulados a heurística explícita
  ("cota operativa inicial, recalibrar con telemetría real"); valores numéricos intactos
  (sin cambio funcional).

---

## Recomendación de secuencia

0. **F-031 / B-46** (2026-08-10, data-integrity) — **actualizado 2026-08-12:**
   el path Kappa OHLCV completo (Incremental → `ohlcv.raw` → Bronze) ya esta
   cableado y verificado en produccion (ver nota en la seccion de detalle
   arriba). Pendiente real: decidir formalmente A/B/C (el codigo ya implementa
   algunas propiedades de A y B, pero la politica definitiva para entornos no
   productivos no fue formalizada como decision de arquitectura en ningun ADR)
   para cerrar el riesgo residual fuera de produccion. Ya no bloquea F-010 de
   la misma forma — reevaluar prioridad relativa.
1. **F-010** primero — menor esfuerzo, cierra el gap de "nadie se entera a las 3AM"
   sin requerir decisiones de diseño nuevas.
2. **F-009** — mayor esfuerzo, requiere ADR; evaluar si el volumen actual del
   canary (ver F2.6c: 138.5 msg/s, 0 errores en 30 min) justifica priorizarlo
   ahora o esperar a que `live_hydra.py` efectivamente use estos producers.
3. **F-011 y F-012** — sin urgencia operativa; agrupables en un único PR de
   limpieza cuando haya ventana disponible.

*(F-008/H-23/B-24 no aparece aquí — ya cerrado, ver commit cade1d3)*

---

## Addendum 2026-08-08 (sesion continuacion) — Kafka delivery semantics

### F-013 / B-29 — produce() descarta bool de send_async(), falso event_published
- **Severidad:** P0 | **Estado:** EN_REMEDIACION (verificado; decision Camino A)
- KafkaProducerAdapter.produce() (infrastructure/kafka/producer.py:184) hace
  `await self.send_async(...)` sin evaluar el bool de retorno; send_async()
  retorna False ante cualquier excepcion de Kafka pero produce() no lo
  propaga. **Alcance preciso (verificado 2026-08-08):** el falso éxito en
  métricas aplica SOLO a `orderbook_producer.on_snapshot/on_delta`
  (los únicos que emiten `event_published`). oi/funding/liquidations NO
  emiten métricas (`oi_producer.py:75` sin KafkaMetrics) — el riesgo de
  métrica falsa no aplica a ellos, aunque sí comparten el patrón de
  tragar la señal sin propagar.
- **Decisión (Camino A, sin ADR):** mantener contrato `produce() -> None`;
  evaluar explícitamente el bool y, si False, log critical + `event_failed`
  (o excepción controlada que los callbacks ya capturan). NO cambiar la
  firma pública de KafkaProducerPort.
- **Evidencia cadena real:** `producer.py:150` usa `send_and_wait()`
  (= send() que espera el future del mensaje, acumulado con linger_ms=5);
  fallo → excepción → `send_async` log + False; `produce()` pierde la señal.

### F-014 / B-30 — close() no hace flush() antes de stop() del cliente Kafka
- **Severidad:** P0 | **Estado:** EN_REMEDIACION
- close() llama directo a self._producer.stop() sin flush() propio.
- **Matiz verificado en aiokafka 0.14.0:** `AIOKafkaProducer.stop()`
  (producer.py:365) SÍ drena pendientes vía
  `message_accumulator.close()->flush()` (message_accumulator.py:384-386),
  por lo que el docstring "flush implícito" de stop() NO es 100% falso.
  El riesgo real es: (a) `asyncio.wait(..., return_when=FIRST_COMPLETED)`
  (producer.py:377) entre `close()` del accumulator y `sender_task` puede
  truncar el drenado si el sender termina primero; (b) sin deadline propio,
  un broker irresponsivo deja el shutdown colgado sin `TimeoutError`
  propagado (el contrato `flush(timeout=...)` del port sí lo exige).
- **Decisión:** fix interno — `close()` invoca `await self.flush(timeout=...)`
  antes de `stop()`, manejando `TimeoutError` con log error (no silenciar).

### F-015 / B-31 — Sin enable_idempotence en AIOKafkaProducer
- **Severidad:** P1 | **Estado:** PENDIENTE
- acks=all configurado pero enable_idempotence ausente (default False en
  aiokafka); retry sin idempotencia productor-side puede duplicar
  mensajes en el topic.

### F-016 / B-32 — Alertmanager receiver 'default' sin destino configurado
- **Severidad:** P0 | **Estado:** BLOQUEADO (reconfirmado 2026-08-09)
- deploy/monitoring/alertmanager.yml — receiver sin slack/email/webhook
  configs. Complementa F-010/B-26: aunque se agreguen las reglas que B-26
  propone, ninguna alerta notificaria a un humano sin esta pieza.
- **Decisión (2026-08-08, reconfirmada 2026-08-09):** bloquear durablemente — el repo
  NO tiene ruta segura de notificación (webhook/Slack/SMTP con credenciales gestionadas)
  ni secret manager. Configurar un receiver obligaría a commitear credenciales (política
  no-secrets, GOVERNANCE.md) o a inventar un destino. El bloqueo está documentado en
  `alertmanager.yml` (comentario F-016). Guarda CI que lo sostienen: gitleaks (scan en
  push/PR a main) y config-guard de docker-compose (no levanta con `$(VAR)` sin definir).
  Mitigación activa: grafana/loki. Condición de reapertura: secret manager externo en el
  repo. No simular destino sin credenciales reales.

### F-017 / B-33 — _PUSH_EXCHANGE hardcodeado invalida aislamiento por job
- **Severidad:** P0 | **Estado:** RESUELTO 2026-08-08 — **re-auditado 2026-08-09**
- apps/app/cli/streaming_hydra.py:73 — _PUSH_EXCHANGE = "orderbook" fijo;
  el comentario de diseño ("un job por exchange evita last-write-wins")
  es falso en la practica porque el label nunca varia entre exchanges.
- **Fijo (2026-08-08):** eliminada la constante; `_heartbeat_loop(pusher, exchange,
  gateway, stop, push_interval)` usa el `exchange` real del stream (job=ocm_pipeline_{exchange},
  prometheus.py deriva job del label). Test `test_exchange_is_derived_not_hardcoded`.
- **Re-auditoria (2026-08-09):** `--exchange` CLI SSOT (streaming_hydra.py:95),
  `_run_streaming(exchange=...)` (213), push usa exchange real (169); 7 tests verdes.
  Sin `_PUSH_EXCHANGE`.

### F-018 / B-34 — Alerta PipelineDown apunta a metrica inexistente
- **Severidad:** P0 | **Estado:** RESUELTO 2026-08-08 — **re-auditado 2026-08-09 (metricas reales)**
- deploy/monitoring/alerts.yml:6 — absent(ocm_pipeline_runs_total); esa
  metrica no existe en ningun .py del proyecto (busqueda exhaustiva sin
  resultados). Metrica real: ocm_pipeline_last_run_timestamp +
  ocm_pipeline_heartbeat_total.
- **Fijo (2026-08-08):** deadman por staleness `(time()-ocm_pipeline_last_run_timestamp) > 600`
  + absent(), ya que el pushgateway persiste series de jobs muertos (ver nota en YAML);
  HighFetchErrors→ocm_fetch_chunk_errors_total; CircuitBreakerOpen→ocm_exchange_circuit_open_total.
- **Verificado (2026-08-09):** grep de las 4 metricas usadas = 1 hit c/u en codigo;
  las 3 falsas previas = 0 hits. YAML OK.

### F-019 / B-35 — Captura generica en sendasync() sin distinguir causas
- **Severidad:** P1 | **Estado:** RESUELTO 2026-08-08 (clasificacion de razones)
- except Exception unico en send_async() colapsaba ~44 BrokerResponseError
  + KafkaTimeoutError/KafkaConnectionError/KafkaUnavailableError en el
  mismo reason=write_error.
- **Implementacion (bloque 3):** send_async() ahora clasifica razones
  (`broker_timeout`/`connection_error`/`broker_response` con errno/`unknown_error`,
  imports lazy desde aiokafka.errors). Test parametrizado (4 categorias) en
  test_producer_adapter.py. El espacio de problema (DLQ/gap detection) sigue
  cubierto por F-009/B-25 (ADR pendiente).

### F-020 / B-36 — OnchainKafkaProducer hereda de clase no encontrada
- **Severidad:** P3 (dead code; import roto confirmado) | **Estado:** VERIFICADO 2026-08-08
- **Verificado:** `onchain_producer.py:9` importa
  `from infra.kafka.base_producer import BaseKafkaProducer` — módulo `infra`
  NO existe en el repo → `ModuleNotFoundError: No module named 'infra'`
  (ejecutado concretamente). Además `onchain_producer.py:13` importa
  `market_data.domain.ports.kafka_producers` — path inexistente (los ports
  viven en `market_data/ports`, no `domain/ports`). Sin callers en todo el
  repo (grep `onchain_producer`/`OnchainKafkaProducer` vacío salvo el propio
  archivo; `composition_root` no lo importa).
- **Decisión:** eliminar `onchain_producer.py` (dead code con imports rotos).

### F-021 / B-37 — Metodo legacy produce(payload, key) sin callers
- **Severidad:** P3 | **Estado:** VERIFICADO 2026-08-08 (sin callers)
- Confirmado sin callers reales: grep de invocaciones a `.produce(` de los
  4 producers WS legacy (payload,key) = 0 hits. El método activo del port
  (`KafkaProducerPort.produce(topic,value,key,headers)`) es distinto y SÍ
  tiene callers reales (orderbook via on_snapshot/on_delta; oi/funding/
  liquidations on_*; ohlcv_publisher; bronze_writer; external_kafka_publisher).
  Nota: `on_open_interest`/`on_funding_rate`/`on_liquidation` tampoco tienen
  callers (grep global 0 hits).
- **Decisión:** eliminar los métodos legacy `produce(payload, key)` de
  orderbook/oi/funding/liquidations en el bloque de limpieza.

### F-022 / B-38 — Twin-import de métricas duplica series en el CollectorRegistry (P2)
- **Severidad:** P2 | **Estado:** VERIFICADO 2026-08-08
- Importar `ocm.observability.prometheus` + `market_data.infrastructure.
  observability.metrics` en el mismo proceso por doble ruta (`packages.*`
  vs `market_data.*`; pytest.ini añade `packages` al pythonpath) provoca
  `ValueError: Duplicated timeseries in CollectorRegistry` (reproducido
  realmente). Artefacto de ruta dual (twin-import), no defecto de runtime
  único.
- **Decisión:** usar siempre ruta canónica `market_data.*`; test de regresión
  que importe ambos módulos sin duplicar.

### F-023 / B-39 — EngineeringHealth FAIL: B-24..B-28 con prioridad fuera del enum normativo
- **Severidad:** P2 (gate de CI roto) | **Estado:** VERIFICADO 2026-08-08 (preexistente en HEAD) → **RESUELTO 2026-08-09**
- `scripts/engineering_health_check.py` valida prioridad ∈
  `['ALTA','BAJA','CRITICA','MEDIA']`; B-24,B-25,B-26 tienen `P2` y B-27,B-28
  `P3` → el test tests/architecture/test_engineering_health.py falla (1 failed
  en suite completa). Confirmado preexistente en HEAD (git show) — la remediacion
  de F-013/F-014 no lo introduce; B-29..B-38 ya usan el enum correcto.
- **Decisión:** normalizar prioridades de B-24..B-28 al enum normativo.
  No requiere ADR.
- **Implementación (2026-08-09):** P2→MEDIA (B-24/F-008, B-25/F-009, B-26/F-010),
  P3→BAJA (B-27/F-011, B-28/F-012). Health check exit 0 (PASS) y
  `test_engineering_health_passes` verde — suite completa verde (997 passed + 0 failed).

### F-024 / H-28 / B-40 — `ocm.runtime.state.redis_stream` importado por factories.py pero módulo inexistente
- **Severidad propuesta:** P2 (import roto real en ruta funcional, hoy sin callers) | **Estado:** PENDIENTE — VERIFICADO (reproducido), NO resuelto
- `ocm/runtime/state/factories.py:198,236` importan lazy
  `from ocm.runtime.state.redis_stream import RedisStreamPublisher/Consumer`
  **fuera del try/except**, pero `ocm.runtime.state.redis_stream` NO existe.
- **Reproducido (2026-08-09):** `build_stream_publisher()` → `ModuleNotFoundError`.
  Símbolo equivalente real en `infrastructure/redis/redis_stream.py:71,145`, sin
  importers. Sin callers de `build_stream_publisher`/`build_stream_source` (grep 0).
  Origen: `66cd1c4 refactor(layout) Fase 3` renombró `ocm_platform→ocm` sin mover streams.
- **Decisión:** NO tocar hasta auditar el hogar canónico (junto a F-011). No requiere
  ADR todavía (se decidirá al resolver F-011). Hallazgo independiente de F-011.

---

## Recomendacion de secuencia (addendum)

1. **F-013 / F-014 (B-29 / B-30)** — P0 integridad de métricas y shutdown;
   Camino A (sin contrato) + flush en close. Bloque 1.
2. **F-018 (B-34) + F-016 (B-32) + F-017 (B-33)** — P0 observabilidad:
   reglas de alerta reales + receiver (si ruta segura disponible) +
   aislamiento por exchange con test 2 exchanges. Bloque 2.
3. **F-015 (B-31) + F-019 (B-35)** — P1; idempotencia (verificar
   compatibilidad acks/retries antes) + clasificación extendida a B-25. Bloque 3.
4. **F-020 / F-021 / F-022 (B-36 / B-37 / B-38)** — P2/P3 limpieza dead code
   y twin-import. Bloque 4.

*(Auditoria de codigo real, no inferencia — ver docs/audits/2026-08-08-streaming-canary-audit.md seccion "Kafka failure semantics / delivery guarantees" para evidencia completa archivo:linea.)*
