# AUDITORÍA — INCIDENTE DE ARRANQUE/RECUPERACIÓN DE ocm-streaming (2026-09-17)

- **Fecha:** 2026-09-17
- **Modo:** READ-ONLY. Cero modificaciones a código, deployment, systemd, Kafka, Docker o tracking. Única escritura: este documento.
- **Complementa:** `docs/audits/AUDIT_OCM_deployment-portability_2026-09-17.md` (auditoría de deployment/portabilidad).
- **Regla de honestidad:** toda afirmación está etiquetada `VERIFIED` (comprobada por código/config/comandos/evidencia observada), `UNVERIFIED` (no comprobada) o `INFERENCE` (deducción razonable no demostrada). No se convierte ninguna inferencia en hecho.

---

## 1. RESUMEN DEL INCIDENTE

**Ventana observada en journal (`journalctl -u ocm-streaming.service`):**

| Hecho | Evidencia | Clasificación |
|---|---|---|
| `ocm-streaming.service` en loop de arranque fallido | journal 2026-09-17 00:00:02 → 00:01:11+: `streaming_failed \| KafkaConnectionError: Unable to bootstrap from [('localhost', 9093, ...)]` repetido con PIDs nuevos cada ~11 segundos | VERIFIED |
| Participación de `Restart=on-failure` + `RestartSec=10` | intervalos entre fallos ~11s (00:00:02 → 00:00:13 → 00:00:25 → 00:00:36...) consistente con RestartSec=10 (+ tiempo de arranque) | VERIFIED |
| Error de conexión a Kafka | `KafkaConnectionError: Unable to bootstrap from [('localhost', 9093, <AddressFamily.AF_UNSPEC: 0>)]` (journal, PIDs 49702/49734/49743/49751...) | VERIFIED |
| Kafka/Zookeeper no levantados en el momento del fallo | journal 09-15 03:28: `Unable connect to node with id 1: ... Connect call failed ('::1', 9093) / ('127.0.0.1', 9093)` (días previos) y fallos de bootstrap continuados hasta el 09-17 | VERIFIED |
| `docker.service` disponible NO implica Kafka ready | el loop ocurre dentro de systemd activo mientras el broker no respondía; los templates declaran solo `After=network-online.target docker.service` | VERIFIED (semántica de systemd) |
| Recuperación | `streaming_started \| exchange=bybit symbols=['BTC-USDT-PERP','ETH-USDT-PERP','SOL-USDT-PERP'] job=ocm_pipeline_bybit` a las 17:20:13; servicio estable desde 17:20:02 (MainPID 9371, NRestarts=0, ExecMainStatus=0) | VERIFIED |

**Relación con B-59 / H-DEP-05 (deployment audit):** este incidente es **evidencia operacional directa** del problema de readiness de Kafka: el proceso **muere** (`streaming_failed`, exit) cuando Kafka no está listo en el arranque, y systemd lo reintenta en bucle. La separación entre `docker.service` activo y Kafka "ready" queda demostrada: no hay `ExecStartPre`, no hay espera activa, la recuperación solo llegó cuando Kafka estuvo disponible (y el servicio fue reiniciado).

---

## 2. INCIDENTE 1 — KAFKA NO DISPUESTA

### KAFKA READINESS NUNCA SE COMPROBÓ

- **Template systemd** (`deploy/systemd/templates/ocm-streaming.service.template`): solo `After=network-online.target docker.service` + `Wants=network-online.target`. **Sin `ExecStartPre`, sin `Requires`, sin wait de healthcheck.** VERIFIED.
- **Unidad instalada/rendered:** idéntica en la directiva de arranque; ambas añaden `EnvironmentFile=<root>/.env` (divergencia F-004, fuera de este incidente). VERIFIED.
- **Consecuencia en el incidente:** el proceso arrancó (la documentación `After=docker.service` satisfecha porque Docker daemon estaba activo), pero el bootstrap a Kafka falló → `KafkaConnectionError` → `book_builder_consumer_init_failed`/`streaming_failed` → exit 1 → systemd `Restart=on-failure` → nuevo intento. VERIFIED (journal).
- **`docker compose ps` en el momento de la investigación posterior:** `ocm_kafka` y `ocm_zookeeper` `Up (healthy)` (`docker ps`): solo estos dos contenedores estaban corriendo entre los de la infra. La recuperación observable es consistente con Kafka/ZK operativos una vez levantados. VERIFIED.
- **Comando de recuperación históricamente reportado** `docker compose up -d zookeeper kafka`: el usuario indica que fue ejecutado durante el incidente. **No hay evidencia revalidable** del momento exacto de esa invocación en los logs disponibles para esta auditoría; el estado final (Kafka healthy, servicio estable) es consistente con una recuperación de ese tipo. Clasificación: **evidencia reportada / INFERENCE** para el momento exacto; **VERIFIED** para el estado final.

**Diferencia documentada (evidencia):**
- `After=docker.service` = orden de activación del unit respecto al daemon Docker. NO garantiza que el broker escuche (Kafka puede: estar caído, llevar 30-90s en `start_period`, haberse reiniciado, o tener healthcheck aún pendiente). VERIFIED (semántica systemd + template).
- Kafka "ready" = acepta conexiones (bootstrap, metadata, cluster API). Se comprueba con `kafka-broker-api-versions` (healthcheck Docker exists, `docker-compose.yml` L562). VERIFIED.

---

## 3. INCIDENTE 2 — BOOK BUILDER DESERIALIZATION

### Evidencia observada

| Ítem | Evidencia | Clasificación |
|---|---|---|
| `book_builder_deserialize_error` en journal | journal 09-17 entre 00:00 y 17:17: **641.073 eventos** (cuenta sobre journal del día); último con PID 2911 a las 17:17:55 | VERIFIED |
| Cese después de las 17:20:05 | con el PID actual (9371, restarted 17:20:02) **0** `book_builder_deserialize_error` desde 17:20:05 | VERIFIED (grep en journal posterior al restart = vacío) |
| El log no incluye el detalle del error | las líneas `book_builder_deserialize_error` NO muestran `error=...`/`ptype=...` (formato loguru compacto) → **no se puede ver el mensaje exacto** de cada error desde el journal | UNVERIFIED (detalle por-error no disponible) |
| Mensajes v1 residuales en DLQ | `ocm.dlq` `from-beginning` devuelve payloads `{"event_version": 1, "payload_type": "delta", ..., "side": "ask", "price": ..., "size": ..., "update_id": 0}` (los primeros con `occurred_at` del 2026-09-07) → **formato v1 (un solo nivel con side/price/size) sí existió en el topic** | VERIFIED |
| DLQ volumen | offsets acumulados: `ocm.dlq:0:217399 / 1:217401 / 2:216804` | VERIFIED |
| Schema actual exige v2 | `shared/kafka/schemas/orderbook.py`: `SCHEMA_VERSION=2`; `OrderBookDeltaPayload.from_dict()` → `if version != 2: raise SchemaVersionError` (L226-230); idem snapshot (L129-133). `OrderBookSnapshotPayload/DeltaPayload` v2 tienen campo `bids`/`asks` multinivel y `update_id`, `cross_seq`, `cts_ms` | VERIFIED |
| Schema incompatibilidad rompe-v1 | commit `3706d61c feat(kafka)!: orderbook schema v2 — delta atomico multinivel + u/seq/cts` (breaking); v1 usaba `side/price/size` de un solo nivel (documentación del módulo L30-43) | VERIFIED |

### Comparación solicitada (payload real vs schema)

- **Payload real (formato v1 en DLQ):** `event_version=1, payload_type=delta, side=ask, price, size, update_id=0`; **no** `bids`/`asks` multinivel. VERIFIED (mensajes DLQ leídos).
- **Schema actual (`OrderBookDeltaPayload.from_dict`):** espera `event_version=2` y `bids`/`asks` multinivel (L204-242); `snapshot` idem (L103-147). VERIFIED (código).
- **Conclusión:** un mensaje con `event_version=1` recibido por `BookBuilderConsumer` → `deserialize(..., OrderBookDeltaPayload)` → `SchemaVersionError` → `book_builder_deserialize_error` → DLQ. El mecanismo está confirmado por código. VERIFIED (mecanismo).
- **La hipótesis de que los 641.073 errores fueran causados por mensajes v1 residuales** de una versión anterior en `orderbook.raw` es **coherente** con: (a) existencia comprobada de mensajes v1 en DLQ, (b) que el topic haya tenido tráfico v1 antes de la migración v2 (commit breaking `3706d61c`), (c) el cese total tras el restart a las 17:20 (que consumió/drenó el backlog o el topic fue reconstruido). **No hay evidencia directa adicional** (el error detallado no está en el journal; no puedo ver los campos exactos de cada mensaje que falló). Clasificación: **INFERENCE** (no demostrado directamente qué y cuánto del backlog era v1).
- **Recreación del tópico `orderbook.raw`:** el topic hoy tiene **1 partición** (`kafka-topics --describe` → `PartitionCount: 1`) y `from-beginning` devuelve mensajes v2 con timestamps del día (09-17T22:18:50) — no se conservan v1 del 09-07 a pesar de retención global de 7d (`KAFKA_LOG_RETENTION_HOURS=168`). Esto **sugiere** topic reconstruido o segmento truncado/drenado, pero **la acción de recreación no está verificada** en esta auditoría (solo estado final). Clasificación: **INFERENCE** (estado final VERIFIED; la acción de recreación UNVERIFIED).

---

## 4. OBSERVACIÓN book_builder_stale

**Código (`packages/market_data/application/processing/book_builder.py`):**

- `BookBuilder.__init__(viewport=50, stale_ms=2000)` — umbral de stale = **2000 ms** por defecto. VERIFIED (L113).
- `_BookState.has_snapshot: bool` — estado del símbolo; `check_stale()` solo evalúa libros con `has_snapshot=True` (skip si no hay snapshot, L270-271). VERIFIED (L270).
- `check_stale(now_ms)`: marca `STALE` si `now_ms - last_update_at_ms > stale_ms` (L272). VERIFIED.
- El adapter (`book_builder_consumer.py`) publica outcomes `STALE` como warning `book_builder_stale` (L299-311). VERIFIED.

**Observación en vivo (journal, 09-17 ~21:30, PID 9371):**

```
book_builder_stale | libro stale: último update hace 2288ms (umbral 2000ms)
book_builder_stale | libro stale: último update hace 2212ms (umbral 2000ms)
... (decenas, valores 2013–2288ms)
```

- **Número:** 304 eventos `book_builder_stale` después de las 17:20:05 (journal del día). VERIFIED.
- **Presentes ahora:** sí, se siguen emitiendo (también a las 21:30). VERIFIED.
- **Momento del arranque:** las primeras ocurrencias del día aparecen cuando el proceso (PID 9371) ya corre estable; no hay un "burst único al arrancar" sino emisiones recurrentes a lo largo del tiempo con valores justo alrededor del umbral (2013–2288 ms vs 2000 ms). VERIFIED (journal).
- **Interpretación:** con un umbral de 2000 ms y latencias de feed alrededor de ese valor (Bybit/reconexión), un libro que recibe updates con jitter >2 s se marca STALE. **No afirmo "bug" ni "no bug"**: es el comportamiento del umbral configurado frente a la cadencia real del feed. La explicación de fondo (feed más lento que el umbral en ciertos tramos, no cold-start típico por ausencia de snapshot) es razonable pero no demostrada: clasificación **INFERENCE**. El valor observado de `has_snapshot` no puede verificarse en runtime directamente aquí (no hay estado vivo exportado): UNVERIFIED para la traza por-símbolo; el mecanismo (skip sin snapshot) VERIFIED por código.

---

## 5. SYSTEMD VS DOCKER

**Verificación contra:**

- `docs/architecture/decisions/ADR-0022-lifecycle-proceso-realtime-feeds.md` (estado: **Propuesto**; L3).
- `docker-compose.yml`.
- `deploy/systemd/templates/ocm-streaming.service.template`.

**Mapa de qué vive dónde (observado):**

| Componente | Dónde | Evidencia |
|---|---|---|
| OCM streaming (orden WS + BookBuilder + producers) | **host / systemd** (nativo, `.venv`) | unit activa; `docker ps` muestra solo kafka/zookeeper entre contenedores de producción |
| Kafka, Zookeeper | **Docker** | `ocm_kafka`, `ocm_zookeeper` `Up (healthy)` |
| Volume/datos | Docker volumes (`kafka_data`…) | docker-compose |
| Observabilidad (pushgateway/prometheus/grafana/loki/promtail) | **NO corriendo** en el momento de la investigación | `docker ps`: solo kafka + zookeeper (`Up`); pushgateway ausente → `health_check.sh` OBSERVABILITY quedaría DOWN |

**Distinción crítica (solicitada):**

- **A. Arquitectura de despliegue:** proceso OCM **nativo bajo systemd** + infraestructura (Kafka/ZK) **en Docker** = intencional y documentada (ADR-0022 A. L98/L138; el documento elegirá systemd como supervisor). Correcto y no cambia.
- **B. Mecanismo de readiness:** **deuda pendiente, reconocida en ADR-0022** ("deuda consciente: el mecanismo concreto de health/readiness... explícitamente fuera de alcance", L215-217; "Fuera de alcance... mecanismo concreto de exposición de health/readiness", L239-240). El incidente confirma la deuda en la práctica: no basta `After=docker.service`.

**Conclusión:** A es intencional/coherente; B tiene una deuda de readiness demostrada operacionalmente (B-59). **No afirmo "sin cambios necesarios"**: el incidente refuerza que B-59 debe resolverse, sin alterar A. VERIFIED para ambos hechos.

---

## 6. BYBIT DISCOVERY

**Contraste:**

- `docs/discovery_profiles/bybit.md`: `u` como UID de encadenamiento, `seq` global por exchange (no usable para gaps), deltas **multinivel** (75.1% >1 nivel, máx 88), sin checksum, snapshot = reset. (L12, 17-21, 96-116 del P0.)
- `docs/audits/AUDIT_OCM_p0-bybit-public-orderbook-experiment_2026-08-28.md`: mismo cuerpo (snapshot inicial, `u` contiguo, multinivel atómico, sin checksum).

**Coincidencia demostrable con el incidente:**

1. **Schema v2 multinivel = modelo Bybit observado en P0:** los mensajes v2 que hoy fluyen en `orderbook.raw`/`book.snapshot` tienen `bids`/`asks` multinivel + `update_id`/`cross_seq`/`cts_ms` (VERIFIED, mensajes reales leídos de Kafka: un delta ETH/SOL con bid/ask arrays multinivel y campos `update_id`/`cross_seq`/`cts_ms` poblados), coherente con el wire P0. La coincidencia (schema v2 ↔ P0) es **VERIFIED por evidencia de los propios mensajes**.
2. **`book.snapshot` sample leído:** `event_version=2, payload_type=snapshot, SOL-USDT-PERP, update_id=45137653, cross_seq=null` (snapshot normal de Bybit sin checksum) — consistente con snapshot=reset y sin checksum del P0. VERIFIED.
3. **El incidente (deserialize v1) no contradice bybit.md:** el v1 (side/price/size de un nivel) es un formato INterno anterior de OCM, no el wire actual de Bybit; la migración a v2 fue breaking (`3706d61c`). No hay contradicción demostrable entre el incidente y bybit.md. VERIFIED (ausencia de contradicción).

**Sin contradicción encontrada entre el incidente y la documentación P0/Bybit** (cualquier correlación causal de "backlog v1" sigue siendo INFERENCE — ver §3).

---

## 7. DEUDA PENDIENTE

Documentado como **pendiente**, no como defecto confirmado (salvo lo señalado):

| ítem | Estado | Clasificación |
|---|---|---|
| **Kafka readiness real** (B-59 / H-DEP-05) | sin implementar; el incidente demuestra el riesgo (crash-loop al arrancar sin broker) | VERIFIED (incidente) + PENDIENTE |
| **Validación E2E book.snapshot/book.delta** | hoy el topic **produce** (`book.delta` offsets 327.447/0/155.197; `book.snapshot` 6/0/3), pero no hay verificación E2E fiel de continuidad/calidad publicada | parcial: producción VERIFIED; validación E2E UNVERIFIED |
| **Composition Root wiring** | `streaming_hydra` usa `CompositionRoot.build_ws_producers` + `build_book_builder_consumer` (dip); en operación `book_builder_consumer_started` no me es observable en el journal actual | código VERIFIED; runtime UNVERIFIED |
| **MarketDataViewPort** | no se encontró un componente nombrado `MarketDataViewPort` en `packages/` (rg sin coincidencias); el viewport es un parámetro de BookBuilder (`viewport=50`) | UNVERIFIED / no localizado |
| **Métricas `ocm_book_*`** | `KafkaMetrics.event_published/event_failed` existen en el código; **no confirmé** que estén exportadas hoy (pushgateway no corriendo; sin endpoint Prometheus operativo al momento) | parcial: código VERIFIED; export runtime UNVERIFIED |
| **GapEventPublisher** | `ports/outbound/gap_event_publisher.py` (Protocol) y `adapters/outbound/kafka_gap_publisher.py` existen (topic `market.gaps`); **no** verifiqué su integración/wiring en el consumer (no aparece en `book_builder_consumer.py`) | código VERIFIED; wiring UNVERIFIED |

---

## 8. COMANDOS / EVIDENCIA UTILIZADOS

**Comandos de la investigación (read-only; NO se ejecutó nada destructivo):**

- `systemctl is-active ocm-streaming.service` / `systemctl show ocm-streaming.service -p ...` → estado actual (active, MainPID 9371, NRestarts=0, ExecMainStatus=0, ActiveEnter 17:20:02).
- `journalctl -u ocm-streaming.service` (ventanas 09-15 → 09-17) → crash-loop, KafkaConnectionError, deserialize, stale, startup.
- `journalctl -u ocm-streaming.service --since ... | rg -c book_builder_deserialize_error` (641.073 antes de 17:20; 0 después).
- `journalctl -u ocm-streaming.service --since ... | rg -c book_builder_stale` (304 después de 17:20).
- `docker ps` → solo `ocm_kafka`/`ocm_zookeeper` up.
- `kafka-topics --list` / `--describe --topic orderbook.raw` → topics y 1 partición.
- `kafka-get-offsets` (book.snapshot, book.delta, ocm.dlq) → offsets actuales.
- `kafka-console-consumer` (SOLO LECTURA) en `orderbook.raw` (2 msgs), `book.snapshot` (1 msg) y `ocm.dlq` (2 msgs) → payloads reales v2/v1.
- Lectura de código: `shared/kafka/schemas/orderbook.py`, `shared/kafka/serializer.py`, `packages/market_data/infrastructure/kafka/book_builder_consumer.py`, `book_builder.py`, `metrics.py`, `gap_event_publisher.py`, templates systemd, ADR-0022, bybit.md, P0 audit.

**Comando histórico reportado (NO reejecutado):** `docker compose up -d zookeeper kafka` (recuperación). **NO** se creó ni borró ningún tópico Kafka durante esta auditoría.

---

## 9. RELACIÓN CON LA AUDITORÍA DE DEPLOYMENT

Este documento es **complementario** a `docs/audits/AUDIT_OCM_deployment-portability_2026-09-17.md` y aporta evidencia operacional al hallazgo central de readiness:

- **H-DEP-05 / B-59 (`After=docker.service` NO garantiza Kafka ready):** el incidente lo **demuestra en producción**: el daemon Docker estaba activo (systemd pudo satisfacer `After`) mientras Kafka no escuchaba → el proceso murió y entró en loop. VERIFIED.
- Refuerza también:
  - **H-DEP-02 / repo_root / paths:** irrelevante aquí (no fue causa).
  - **F-004 (divergencia template↔instalado):** observada de nuevo (la unidad añade `EnvironmentFile=.env`), sin impacto directo en este incidente.
  - **Readiness solución propuesta (ExecStartPre con retry / backoff en app / runbook):** pendiente; en §Addendum de este documento (PROPUESTA).

---

## 10. ADR-0022

**NO se modifica ADR-0022.** Texto propuesto abajo, como PROPUESTA (no aplicada).

---

## 11. ADDENDUM PROPUESTO PARA ADR-0022

> **Estado de este addendum: PROPUESTA (no aplicado a ADR-0022).**
>
> ## Addendum (2026-09-17): incidente de readiness — Kafka no disponible en el arranque
>
> **Incidente observado:** `ocm-streaming.service` entró en loop de arranque fallido (09-16 → 09-17) con
> `streaming_failed | KafkaConnectionError: Unable to bootstrap from [('localhost', 9093, ...)]`,
> reintentado por systemd (`Restart=on-failure`, `RestartSec=10`, intervalos ~11 s). La recuperación
> llegó cuando Kafka/Zookeeper quedaron operativos y el servicio fue reiniciado a las ~17:20,
> logrando `streaming_started` (bybit, 3 símbolos). Referencia: `docs/audits/AUDIT_OCM_streaming-incident-recovery_2026-09-17.md`.
>
> **Diferencia Docker daemon vs Kafka readiness:** `After=network-online.target docker.service` solo
> ordena el arranque respecto al daemon Docker; **no** garantiza que el broker acepte conexiones
> (bootstrap/metadata). El incidente demuestra que el proceso OCM muere si Kafka no está listo al
> arrancar (B-59/H-DEP-05).
>
> **Relación con la deuda reconocida por ADR-0022:** este ADR declara "deuda consciente: health/readiness
> fuera de alcance" (§Consecuencias, §Fuera de alcance). El incidente convierte esa deuda en riesgo
> operacional demostrado.
>
> **Posibles soluciones (sin elegir todavía):**
> - **ExecStartPre con espera activa:** un pre-check acotado que espere a que Kafka acepte
>   `kafka-broker-api-versions` (o una lógica de reconexión tolerante) antes de arrancar el proceso; el reintento queda
>   en systemd.
> - **Runbook explícito:** documentar/ordenar el arranque (1. infra Docker healthy, 2. topics, 3.
>   OCM) y la recuperación (`docker compose up -d zookeeper kafka` + restart), convirtiendo el
>   procedimiento observado en operación estándar.
> - Posibilidad de combinar ambos (espera acotada en systemd + runbook) sin cambiar la arquitectura
>   de despliegue (sigue: infra Docker + proceso nativo supervisado por systemd).
>
> **Decisión:** pendiente de autorización humana.

---

## 12. REFERENCIAS

- `docs/architecture/decisions/ADR-0022-lifecycle-proceso-realtime-feeds.md` (Propuesto; deuda consciente L215-217, 239-240; supervisión systemd L98/138/164).
- `docs/discovery_profiles/bybit.md` (u/seq/cts; multinivel; snapshot=reset).
- `docs/audits/AUDIT_OCM_p0-bybit-public-orderbook-experiment_2026-08-28.md` (P0: `u` contiguo, seq no usable, 75.1% multinivel, sin checksum).
- `docs/audits/AUDIT_OCM_market-data-and-adr0028_2026-08-28.md` (F-004 — variantes de unit).
- `docs/audits/AUDIT_OCM_deployment-portability_2026-09-17.md` (H-DEP-05, B-59).
- `deploy/systemd/templates/ocm-streaming.service.template` / unidad instalada en `/etc/systemd/system/ocm-streaming.service`.
- `docker-compose.yml` (Kafka healthcheck L562; retención L541).
- `shared/kafka/schemas/orderbook.py`; `shared/kafka/serializer.py`; `shared/kafka/schemas/_base.py` (SchemaVersionError).
- `packages/market_data/infrastructure/kafka/book_builder_consumer.py`; `packages/market_data/infrastructure/kafka/metrics.py`.
- `packages/market_data/application/processing/book_builder.py`.
- `packages/market_data/adapters/outbound/kafka_gap_publisher.py`; `packages/market_data/ports/outbound/gap_event_publisher.py`.
- Commits: `3706d61c` (orderbook schema v2 breaking), `144d8552` (BookBuilder), `09310652`, `6ea049ea`.

---

## 13. LIMITACIONES DE LA AUDITORÍA

1. **Detalle del error `book_builder_deserialize_error` no disponible:** el journal solo muestra el nombre del evento; `offset`/`ptype`/`error=` no quedaron registrados → no puedo demostrar la causa exacta de cada deserialización fallida (solo el mecanismo por código y la existencia de v1 en DLQ). UNVERIFIED.
2. **Momento exacto de `docker compose up -d zookeeper kafka`:** evidencia reportada por el usuario, no revalidable desde journal. INFERENCE para el momento; VERIFIED el estado final.
3. **Recreación de `orderbook.raw`:** UNVERIFIED; solo se conoce su estado posterior (1 partición, sin v1 retenido pese a retención 7d). INFERENCE.
4. **Contenido completo de `orderbook.raw` (offset exacto desde/ hasta):** no se volcó ni consumió todo el topic (solo muestras de 2 mensajes) para no interferir. UNVERIFIED.
5. **Metricas `ocm_book_*` exportadas hoy:** pushgateway no corriendo; sin endpoint Prometheus operativo verificado → no confirmé su exportación en vivo. UNVERIFIED.
6. **`MarketDataViewPort`:** no localizado en `packages/` (rg). O su nombre es otro o su alcance está fuera de `packages/market_data`. UNVERIFIED.
7. **Ausencia/presencia de `book_builder_consumer_started`:** no verificado en este journal; solo se observó el warning `book_builder_consumer_init_failed` en la ventana de fallo y la ausencia posterior de deserialize. UNVERIFIED.
8. **Trazas por símbolo de `book_builder_stale`** (cuál exchange/símbolo): el journal no las distinguía en las líneas observadas (campo `exchange`/`symbol` bound al logger, no en el mensaje compacto). UNVERIFIED.
9. **`systemd-analyze verify`** NO se ejecutó en el host (read-only). UNVERIFIED (deploy audit ya lo marcó).
10. **No se ejecutó ningún comando destructivo:** no se creó, borró ni transformó ningún tópico; no se restartearon servicios ni se editó configuración.

---

## Estado del repositorio

- Cambio de este agente: **solo** `docs/audits/AUDIT_OCM_streaming-incident-recovery_2026-09-17.md` (nuevo).
- Preexistente preservado intacto: ` M docs/plans/tracking.yaml` (de otro agente).
- ADR-0022, tracking.yaml, código, deployment, systemd, Kafka, Docker: **no modificados**.
- Commits: 0 · Pushes: 0 · Servicios: 0.