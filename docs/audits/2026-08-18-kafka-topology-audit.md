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

**Estado: AISLADO — preexistente, no relacionado a este cambio.**

Verificación: se alternó `KAFKA_AUTO_CREATE_TOPICS_ENABLE` a `"true"` en el
broker real (reinicio de contenedor, no solo config), y se re-ejecutó el test
contra un topic limpio. El test **falló igual** (1.72s, incluso más rápido),
con el mismo patrón de pérdida total del batch. Config revertido a `"false"`
inmediatamente después (confirmado con grep post-restart).

Se observó adicionalmente un warning `Unclosed AIOKafkaProducer` en la
ejecución con `"true"` — el producer no cerró/flusheó limpio antes de que el
test finalizara. Esto refuerza la hipótesis de race condition
productor/consumidor (el consumer lee antes de que el producer confirme el
envío de los 100 mensajes) como causa raíz más probable, independiente de
`AUTO_CREATE_TOPICS_ENABLE`.

**Acción:** flaky test registrado como deuda técnica separada, fuera de
alcance de esta auditoría. Recomendado para sesión dedicada: revisar si el
test usa `producer.send_and_wait()` / `flush()` antes de iniciar el consumer,
o si falta un `await` en el fixture de setup.

---

## Commits de esta sesión

- `fa98b32` — fix(kafka): disable AUTO_CREATE_TOPICS_ENABLE
- `10ec308` — feat(kafka): expose ALL_TOPICS as public SSOT export
- (pendiente) — feat(kafka): add explicit topic provisioning script
