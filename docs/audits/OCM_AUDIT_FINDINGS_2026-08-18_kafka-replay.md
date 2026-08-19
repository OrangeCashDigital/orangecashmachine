# OCM — AUDIT FINDINGS REGISTER (Kafka Replay Integrity & Topology)

**Ejecución de auditoría:** 2026-08-18 (baseline `a4d8298`, branch `main`)
**Alcance:** Verificación de integridad del fix de replay Kafka (`test_A_replay_100_events_no_loss`
y tests B/C del harness `tests/kafka/`) y de la topología real del broker local
(listeners, bootstrap, topics, particiones, consumer groups, offset management, semántica at-least-once).
**Fuente primaria:** `docs/audits/2026-08-18-kafka-topology-audit.md` + verificación read-only
en vivo de esta sesión (broker, metadata, offsets, tests de integración).
**Estado de este registro:** OPEN → ACTUALIZADO 2026-08-18 (post-auditoría): F-KAFKA-02 y
F-KAFKA-04 CERRADOS tras aplicar sus correcciones documentales; F-KAFKA-03 abierto (requiere
decisión de puerto). El defecto de harness (F-KAFKA-01) está CERRADO y verificado.

Resumen: CRITICAL 0 · HIGH 0 · MEDIUM 0 · LOW 4 · INFO 0 · **total 4**.

Clasificación (taxonomía del protocolo de auditoría de OCM):
- NUEVO: 0
- REVALIDADO: 0
- REGRESIÓN: 0
- CERRADO: 2 — F-KAFKA-01, F-KAFKA-02
- CONTRADICCIÓN: 1 — F-KAFKA-03
- RECOMENDACIÓN: 1 — F-KAFKA-04
- NO_VERIFICADO: 0

Deduplicación (regla §11):
- F-KAFKA-03 (bootstrap/port inconsistencies) se contrastó contra `docs/plans/backlog-priorizado-2026-08-08.md`
  F-032/B-47 (KafkaConfig.bootstrap_servers decorativo). El hallazgo de fondo ya está documentado
  allí; esta ficha registra la parte de incoherencia de puertos/plantilla que F-032 no cubre
  (`.env.example`=9094 vs `.env`=9093 vs tests hardcoded 9093 vs `config/base.yaml` default 9092).
- F-KAFKA-02 (comentario `dlq.ohlcv` vs `ocm.dlq`) no aparece en tracking/ADRs previos → se
  registra como CONTRADICCIÓN documental, LOW.
- F-KAFKA-04 (`tests/kafka/CONTRACT.md` referenciado e inexistente) → RECOMENDACIÓN documental.

Verificación matemática: total 4

---

## F-KAFKA-01 — Flakiness de test_A_replay_100_events_no_loss (causa raíz en harness, no en producto)

Severity: LOW
Status: CERRADO
Classification: CERRADO
Control: Integration — Kafka replay (tests/kafka)
Source: tests/kafka/test_integration_kafka.py + tests/kafka/conftest.py + `docs/audits/2026-08-18-kafka-topology-audit.md`

Evidence:
- Commit `68b460b`: `drain_until_ids_found()` añadido a `tests/kafka/conftest.py` (+49 líneas).
- Commit `cc40a7f`: `test_A` pasa a `warm_up_consumer(c2)` + `seek_to_beginning()` + `drain_until_ids_found()` (14+/12-).
- `git diff HEAD~3..HEAD -- tests/kafka/ packages/market_data/infrastructure/kafka/consumer.py`:
  conftest.py +49, test_integration_kafka.py 26 líneas, **consumer.py 0 líneas** (producto intacto).
- Causa raíz factor 1: `seek_to_beginning()` sin particiones asignadas = no-op silencioso
  (partition assignment async); fix = `warm_up_consumer(c2)` antes del seek (CONTRACT R-02).
- Causa raíz factor 2: `_drain(expected=100)` agotaba su cuota con basura histórica parseable
  (mismo schema, otros event_id); fix = `drain_until_ids_found()` filtra por set (R-03/R-04).
- Verificación en vivo: `test_A_replay_100_events_no_loss` → 1 passed in 8.46s;
  B/C1/C2 → 3 passed in 27.09s; suite completa 1222 passed.

Impact:
- El defecto era del harness (asignación async + filtrado por cantidad, no por contenido),
  no de la implementación Kafka (`consumer.py` correcto: `enable_auto_commit=False`,
  `seek_to_beginning()` en línea 295). Sin impacto en capital real.

Required human decision:
- Ninguna — cerrado y verificado.

Recommended remediation:
- N/A (ya aplicada). Mantener CONTRACT R-02 (warm_up antes de seek) y R-03/R-04 (set-membership + ignorar no parseables).

Verification required:
- `uv run pytest tests/kafka/test_integration_kafka.py::test_A_replay_100_events_no_loss -v -s -m integration` → PASS.

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: 68b460b, cc40a7f · Tests: test_A/B/C PASS ·
  CI: suite 1222 passed · Evidence: docs/audits/2026-08-18-kafka-topology-audit.md · Closure: CERRADO 2026-08-18

---

## F-KAFKA-02 — Comentario de topología en docker-compose dice `dlq.ohlcv` pero el SSOT es `ocm.dlq`

Severity: LOW
Status: CERRADO
Classification: CERRADO
Control: Documentation — Kafka topology SSOT
Source: docker-compose.yml + shared/kafka/topics.py

Evidence:
- `docker-compose.yml:502` comenta el topic DLQ como `dlq.ohlcv` (Dead Letter Queue).
- `shared/kafka/topics.py:102` define `TOPIC_DLQ: str = "ocm.dlq"` (SSOT real).
- Verificado en runtime: el broker local tiene `ocm.dlq` (no `dlq.ohlcv`).
- Sin referencia en tracking/ADRs previos (grep `dlq.ohlcv` en docs/ = 0 coincidencias documentales).
- **CERRADO 2026-08-18:** comentario corregido en docker-compose.yml a `ocm.dlq` con
  referencia al SSOT (`shared/kafka/topics.py → TOPIC_DLQ="ocm.dlq"`).

Impact:
- Documentación engañosa: un operador que busque el DLQ por el comentario de compose
  no lo encontrará en el broker. Bajo impacto funcional (no afecta código).

Required human decision:
- Ninguna — comentario corregido.

Recommended remediation:
- (aplicada) Corregir `docker-compose.yml:502` a `ocm.dlq` para alinear comentario con SSOT.

Verification required:
- `rg dlq.ohlcv` en repo = 0 tras el fix.

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: docker-compose.yml:502 (2026-08-18) ·
  Tests: N/A · CI: N/A · Evidence: docker-compose.yml:502, topics.py:102 · Closure: CERRADO 2026-08-18

---

## F-KAFKA-03 — Incoherencias de bootstrap/port: `.env.example` 9094 vs `.env` 9093 vs tests 9093 vs base.yaml default 9092

Severity: LOW
Status: OPEN
Classification: CONTRADICCIÓN
Control: Configuration — Kafka bootstrap coherence
Source: .env, .env.example, tests/kafka/conftest.py, config/base.yaml

Evidence:
- `.env:34` → `KAFKA_HOST_PORT=9093` (coherente con broker real mapeado y con tests).
- `.env.example:36` → `KAFKA_HOST_PORT=9094` (divergente de `.env` y de tests).
- `tests/kafka/conftest.py:28` → `BROKER = "localhost:9093"` **hardcoded** (acoplado a `.env` actual;
  si alguien replica desde `.env.example`=9094, los tests fallan al conectar).
- `config/base.yaml:46` → default `localhost:9092` (puerto del listener INTERNAL, inaccesible desde
  host; solo 9093 está mapeado). El runtime real usa `KAFKA_BOOTSTRAP_SERVERS`/`kafka:9092`
  (producer.py:81, consumer.py:64) — coherente.
- Fondo documentado en `docs/plans/backlog-priorizado-2026-08-08.md` F-032/B-47:
  `KafkaConfig.bootstrap_servers` (Pydantic) decorativo, no leído por el runtime.

Impact:
- Bajo riesgo funcional (el runtime usa env vars, no base.yaml), pero riesgo operativo de onboarding:
  un entorno nuevo creado desde `.env.example` (9094) rompería la integración local de tests que
  hardcodean 9093, y `config/base.yaml` documenta un default inalcanzable desde host.

Required human decision:
- A) unificar puerto host (elegir 9093) y alinear `.env.example`+tests; o B) parametrizar el
  BROKER de los tests desde env var (SSOT) en vez de hardcodear. Recomendado: A + B (progresivo).

Recommended remediation:
- Alinear `.env.example` con `.env` (9093) y/o leer el puerto del BROKER de tests desde env var;
- actualizar el comentario/default de `config/base.yaml` al listener externo real (9093).

Verification required:
- Entorno nuevo con `.env.example` → `uv run pytest tests/kafka/test_integration_kafka.py -m integration` → PASS.

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: pendiente · Tests: N/A ·
  CI: N/A · Evidence: .env:34, .env.example:36, conftest.py:28, base.yaml:46 · Closure: OPEN

---

## F-KAFKA-04 — `tests/kafka/CONTRACT.md` referenciado en docstrings pero inexistente

Severity: LOW
Status: CERRADO
Classification: RECOMENDACIÓN
Control: Documentation — test harness contract
Source: tests/kafka/conftest.py + tests/kafka/test_integration_kafka.py

Evidence:
- `tests/kafka/conftest.py:12` — `Contrato: tests/kafka/CONTRACT.md`.
- `tests/kafka/test_integration_kafka.py` — misma referencia en su docstring de cabecera.
- Verificado: `tests/kafka/CONTRACT.md` NO existía (ls = No such file).
- Las reglas R-01..R-04 están documentadas como comentarios inline en conftest.py:53,93,119,145-146,168,196-197,216.
- **CERRADO 2026-08-18:** `tests/kafka/CONTRACT.md` creado materializando R-01..R-04
  (SSOT del harness, broker, semántica de replay y ejecución).

Impact:
- Referencia rota en documentación de código: el "contrato" del harness no tenía SSOT materializado;
  riesgo de que las reglas R-01..R-04 se pierdan.

Required human decision:
- Ninguna — opción A aplicada (crear el documento).

Recommended remediation:
- (aplicada) Crear `tests/kafka/CONTRACT.md` con R-01..R-04 y la topología de pruebas.

Verification required:
- `ls tests/kafka/CONTRACT.md` → existe.

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: tests/kafka/CONTRACT.md (2026-08-18) ·
  Tests: N/A · CI: N/A · Evidence: conftest.py:12, docstring test_integration_kafka.py · Closure: CERRADO 2026-08-18
