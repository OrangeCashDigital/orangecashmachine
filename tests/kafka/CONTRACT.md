# tests/kafka — CONTRACT.md (Harness de Integración Kafka)

**Contrato de referencia:** `tests/kafka/conftest.py` (SSOT de broker, helpers y reglas).
**Trazabilidad:** findings F-KAFKA-01 (CERRADO) y F-KAFKA-04 de la auditoría
`docs/audits/AUDIT_OCM_FORENSIC_COMPLIANCE_2026-08-18-kafka-replay.md`.

Este documento materializa las reglas R-01..R-04 que el harness referencia como
comentarios inline. Ningún `test_*.py` debe duplicar constantes o helpers: importar
siempre desde `tests/kafka/conftest.py`.

---

## Broker de integración

- **BROKER:** `localhost:9093` — listener EXTERNAL del broker (definido en
  `docker-compose.yml` como `EXTERNAL://localhost:${KAFKA_HOST_PORT:-9093}`).
- El listener INTERNAL (`kafka:9092`) **no resuelve desde el host** — solo desde
  contenedores Docker.
- Requiere el broker local levantado: `docker compose up -d` (kafka + zookeeper).

### Nota de puerto (F-029/B-45, 2026-08-09)

`KAFKA_HOST_PORT` evita colisionar con `ALERTMANAGER_HOST_PORT=9093`. El valor real
se declara en `.env`/`.env.example`; los tests usan el puerto en `BROKER` y **deben
mantenerse en sincronía** con el puerto mapeado del broker.

---

## Reglas del contrato

### R-01 — Group ID único por test

Cada test debe usar su propio `group_id`. Usar `_unique_group(prefix)` que genera
`"{prefix}-{uuid8}"` — nunca colisiona entre runs ni entre tests.

```python
group = _unique_group("replay")
```

### R-02 — Partition assignment completado antes de producir/seek

- Para capturar solo mensajes del test actual con `offset_reset='latest'`:
  llamar `warm_up_consumer(c)` **ANTES de producir**.
- Para **Kappa replay** (`seek_to_beginning()`): llamar `warm_up_consumer(c)`
  **ANTES del seek**. Sin warm-up, el assignment de particiones es async y el
  seek sobre particiones sin asignar es un **no-op silencioso** → pérdida total
  (root cause del flake F-KAFKA-01, resuelto en `68b460b`/`cc40a7f`).

```python
c = _raw_consumer(group, topic, offset_reset="earliest")
await warm_up_consumer(c)          # R-02: completar assignment
await c.seek_to_beginning()        # replay desde el inicio de todas las particiones
```

### R-03 — Verificar por event_id, nunca por posición

Los topics de integración acumulan basura histórica *parseable* (mismo schema,
otros `event_id` de corridas previas). Nunca asumir posición ni contar mensajes
cualquiera como si fueran los del test.

- Mensaje específico → `find_event(c, event_id)`.
- Set de eventos (replay de N) → `drain_until_ids_found(c, target_ids)`, que lee
  hasta que el set de `event_id` encontrados cubre `target_ids`.

`_drain(expected=N)` queda **restringido** a contar el total recibido; no valida
contenido.

### R-04 — Ignorar mensajes no parseables

`deserialize(r.value, EventPayload)` puede fallar sobre basura histórica del topic.
Los helpers la ignoran con `except Exception: pass`. Un test que encuentre mensajes
no parseables **no debe fallar por eso** — debe filtrar por `event_id` (R-03).

---

## Semántica de replay y at-least-once

- `enable_auto_commit=False` en `_raw_consumer()`: el commit es manual y explícito.
- Replay: `warm_up_consumer(c)` → `seek_to_beginning()` → leer filtrando por
  `event_id`. El consumer del producto real implementa lo mismo en
  `packages/market_data/infrastructure/kafka/consumer.py` (`seek_to_beginning()`).

---

## Ejecución

```bash
uv run pytest tests/kafka/test_integration_kafka.py -v -m integration
```

Requiere broker local (`docker compose up -d`). Sin broker, los tests fallan por
infraestructura (registrado en `AUDIT_OCM_TECHNICAL_COMPLIANCE_2026-08-18.md`).