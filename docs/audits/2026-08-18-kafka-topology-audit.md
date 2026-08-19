# Auditoría de Topología Kafka — 2026-08-18

**Alcance:** `shared/kafka/` (SSOT de topics/consumer groups), bloque `kafka:` de
`docker-compose.yml`, contratos de import-linter relacionados (BC-29, BC-40).

**Metodología:** inspección directa de código + config vía SSH (sin acceso a red
saliente desde el entorno de Claude), sin asumir comportamiento — cada hallazgo
se verificó contra el archivo fuente antes de proponer corrección.

---

## Hallazgos

### 🔴 P1 — `KAFKA_AUTO_CREATE_TOPICS_ENABLE` en `true` (RESUELTO)

**Problema:** contradecía el propio comentario en `docker-compose.yml`
("producción: desactivar y crear explícitamente") y el fail-fast ya presente
en `shared/kafka/topics.py` (`assert len(_ALL_TOPICS) == len(set(_ALL_TOPICS))`).
Con auto-create en `true`, un typo en un nombre de topic no fallaba — creaba
silenciosamente un topic fantasma con 3 particiones / replication factor 1,
invisible hasta que se notaban datos faltantes río abajo.

**Fix aplicado:**
1. `docker-compose.yml`: `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"` (commit `fa98b32`)
2. `shared/kafka/topics.py`: se expone `ALL_TOPICS: tuple[str, ...]` derivado de
   `_ALL_TOPICS` antes del `del`, para que herramientas externas (provisioning)
   puedan enumerar el SSOT sin duplicar la lista (DRY) (commit `10ec308`)
3. `scripts/provision_kafka_topics.py`: script idempotente (SafeOps —
   `TopicAlreadyExistsError` no es error) que crea explícitamente los 25 topics
   del SSOT vía `AIOKafkaAdminClient` (commit pendiente de confirmar)

**Verificación:**
- `mypy shared/kafka/topics.py scripts/provision_kafka_topics.py` → `Success: no issues found in 2 source files`
- `ruff check` → `All checks passed!`
- `python3 -c "from shared.kafka.topics import ALL_TOPICS; print(len(ALL_TOPICS))"` → `25`
- `import-linter` (vía pre-commit) → `Passed`

### 🟡 P2 — Contradicción de diseño en DLQ (ABIERTO, fuera de alcance esta sesión)

El docstring de `topics.py` sugiere DLQ por-topic ("cualquier topic → [*.dlq]"),
pero el código solo declara `TOPIC_DLQ = "ocm.dlq"` (global). Decisión pospuesta
explícitamente por Solano — opciones sobre la mesa para una sesión dedicada:
- (a) un solo `ocm.dlq` global + header de topic-origen, actualizar doc
- (b) DLQ por topic (`ohlcv.raw.dlq`, `signals.raw.dlq`, ...)

### 🟢 Informativo — single-node broker

`KAFKA_DEFAULT_REPLICATION_FACTOR=1`, `KAFKA_MIN_INSYNC_REPLICAS=1`,
`KAFKA_BROKER_ID=1`. Aceptable para homelab de desarrollo; no es un hallazgo,
queda documentado como decisión consciente vinculada a F2.6 (Capacity Planning).

### 🟢 Informativo — BC-29 excepción `market_data`

El contrato ya declara la excepción temporal con criterio de salida ("Fase 2 de
migración en curso... una vez finalizada: añadir market_data a source_modules").
No requiere acción — solo seguimiento en la migración en curso.

### 🟢 Descartado — naming inconsistente

Hallazgo inicial incorrecto: todos los topics siguen el patrón `dominio.evento`
consistentemente (`book.snapshot`, `microprice.rt`, `orderbook.raw`, etc.). Se
descarta tras revisión completa del archivo.

---

## Item abierto: test flaky en `test_integration_kafka.py`

`test_A_replay_100_events_no_loss` falló con pérdida del 100% de los 100
eventos esperados (`replay-0000`...`replay-0099`), no pérdida parcial — patrón
consistente con race condition de timing productor/consumidor, no con
`UnknownTopicOrPartitionError` (el topic `ohlcv.raw` existía y tenía datos
de corridas previas, offsets 668-694 confirmados por log).

El propio test ya documenta contaminación cruzada esperada entre runs
("El topic contiene mensajes de otros tests — ignorar los no parseables"),
lo que sugiere que el topic de test no usa un nombre efímero por ejecución.

**Estado: RESUELTO.**

Aislamiento (confirmado preexistente): se alternó `KAFKA_AUTO_CREATE_TOPICS_ENABLE`
a `"true"` en el broker real (reinicio de contenedor), y se re-ejecutó el test.
Falló igual — descartado como relacionado a esa config.

**Causa raíz real (dos factores, ambos necesarios):**

1. **Partition assignment async sin warm-up.** `c2` llamaba `seek_to_beginning()`
   inmediatamente tras `_raw_consumer()`, antes de que el rebalance del grupo
   completara la asignación de particiones — `seek_to_beginning()` sin
   particiones asignadas es un no-op silencioso. Contrato R-02 del propio
   harness (`warm_up_consumer()`) ya cubría este caso, pero `test_A` no lo
   usaba (a diferencia de `test_B`/`test_C`, que sí). Fix: se agregó
   `await warm_up_consumer(c2)` antes de `seek_to_beginning()`.

2. **`_drain(expected=100)` se agota con basura histórica parseable.**
   `TOPIC_OHLCV_RAW` acumula mensajes de corridas previas con el mismo schema
   (`KafkaOHLCVBar`/`EventPayload`). `_drain()` para en cuanto junta N
   registros *cualquiera*, sin filtrar contenido — en un replay desde el
   inicio del topic, agotaba su cuota de 100 con basura histórica antes de
   llegar a los eventos de la corrida actual. El comentario original del test
   asumía que la basura sería "no parseable" y se saltearía sola, pero al
   compartir schema, sí es parseable — solo tiene otros `event_id`.

**Fix aplicado:** se agregó `drain_until_ids_found()` en `tests/kafka/conftest.py`
(mismo patrón que `find_event()` — R-03/R-04, no toca `_drain()` para no afectar
otros tests que dependen de su contrato actual). Lee hasta que los `event_id`
encontrados cubren el set esperado, no hasta juntar N mensajes cualquiera.
`test_A` fue actualizado para usar esta función en vez de `_drain()` +
filtrado manual.

**Verificación:** `test_A_replay_100_events_no_loss` → `1 passed in 8.26s`.
Suite completa `tests/kafka/` → `214 passed in 36.21s`, cero regresiones.

---

## Commits de esta sesión

- `fa98b32` — fix(kafka): disable AUTO_CREATE_TOPICS_ENABLE
- `10ec308` — feat(kafka): expose ALL_TOPICS as public SSOT export
- (pendiente) — feat(kafka): add explicit topic provisioning script
