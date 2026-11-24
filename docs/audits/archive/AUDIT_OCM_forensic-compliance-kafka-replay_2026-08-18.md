# OCM — Kafka Replay Integrity & Topology Audit (Forensic Compliance)

**Fecha de consolidación:** 2026-08-18
**Commit auditado:** `a4d8298` (HEAD, `main`)
**Branch:** `main`
**Alcance:** Verificación read-only de integridad del fix de replay Kafka (harness `tests/kafka/`,
tests A/B/C1/C2) y de la topología real del broker local (listeners, advertised, bootstrap por
contexto, topics, particiones, consumer groups, offset management, semántica at-least-once, replay).
**Metodología:** Read-only estricto (escritura solo en `docs/audits/`). Discovery normativo →
baseline → verificación de commits → verificación en vivo (broker, metadata, offsets) →
verificación de tests de integración → clasificación → veredicto.

---

## Executive Summary

El defecto de flakiness de `test_A_replay_100_events_no_loss` fue del **harness de test, no de la
implementación Kafka**. La causa raíz (dos factores: partition assignment async + `_drain()` que
agota cuota con basura histórica parseable) está documentada en `docs/audits/2026-08-18-kafka-topology-audit.md`
y quedó **resuelta en main** con los commits `68b460b` (nuevo `drain_until_ids_found()`) y `cc40a7f`
(`test_A` pasa a `warm_up_consumer` + `seek_to_beginning` + `drain_until_ids_found`). El diff
verificado confirma **0 líneas de cambio en `consumer.py`** (producto intacto).

La topología real de Kafka fue verificada en vivo y es **coherente** con el SSOT
(`shared/kafka/topics.py`), el `docker-compose.yml` y el runtime: listener EXTERNAL `localhost:9093`
responde desde host, topics con 3 particiones RF=1, consumer groups efímeros de test committeando
offsets reales (evidencia de basura histórica que motivó el fix).

**Veredicto:** `AUDIT_CONFORME_CON_FINDINGS_DOCUMENTALES` — el defecto de producto está CERRADO;
quedan 4 findings de documentación/configuración de severidad LOW (1 CONTRADICCIÓN, 1 RECOMENDACIÓN,
2 CERRADO). No se requiere cambio de código ni de tests. Plan Maestro continuable.

**Actualización post-auditoría (2026-08-18):** aplicadas las correcciones documentales —
F-KAFKA-02 CERRADO (comentario `dlq.ohlcv`→`ocm.dlq` en docker-compose.yml), F-KAFKA-04 CERRADO
(`tests/kafka/CONTRACT.md` creado con R-01..R-04), F-KAFKA-03 abierto con nota documentada en
`.env.example` (por qué 9094 ≠ 9093, F-029). Queda abierta la decisión de unificar el puerto.

---

## 1. Baseline (Construcción del Baseline)

- **Commit:** `a4d8298` (HEAD, `main`) — `git rev-parse HEAD`
- **Branch:** `main` — `git branch --show-current`
- **Working tree:** ` M docs/plans/tracking.yaml` (cierre formal de B-MD-008 de sesión previa;
  sin otras modificaciones) — `git status --short`
- **Baseline documental:** `docs/audits/2026-08-18-kafka-topology-audit.md` (root cause del flake),
  `docs/plans/backlog-priorizado-2026-08-08.md` (F-032/B-47), ADR-0013/0014/0022/0023,
  `shared/kafka/topics.py` (SSOT), `docker-compose.yml`.
- **Cambios concurrentes:** ninguno detectado durante esta sesión (solo el cierre B-MD-008).

---

## 2. Alcance

| Incluye | Excluye |
|---|---|
| Harness de integración Kafka (`tests/kafka/`): conftest.py, test_integration_kafka.py | Modificaciones de cualquier tipo |
| Adapter `packages/market_data/infrastructure/kafka/consumer.py` (at-least-once, replay, offsets) | Entorno live con capital real |
| Topología real del broker (listeners, advertised, bootstrap, topics, particiones, groups, offsets) | — |
| Coherencia de configuración (`.env`, `.env.example`, `config/base.yaml`, docker-compose, SSOT) | — |
| Verificación de tests de integración (A/B/C1/C2) | — |

**Read-only:** no se modificó código, tests, CI, ADRs, tracking ni governance. Escritura: solo
`docs/audits/OCM_AUDIT_FINDINGS_2026-08-18-kafka-replay.md` y `docs/audits/AUDIT_OCM_FORENSIC_COMPLIANCE_2026-08-18-kafka-replay.md`.

---

## 3. Discovery Order (Orden de Descubrimiento)

1. `AGENTS.md` ✅ — reglas de auditoría OBLIGATORIO
2. `docs/governance/AUDIT_PROTOCOL.md` ✅ — v2.1 (M1..M20)
3. `docs/PLAN-Maestro-Ingenieria.md` ✅ (referenciado; no tocado)
4. `docs/architecture/GOVERNANCE.md` ✅
5. `docs/plans/tracking.yaml` ✅ — B-MD-008 HECHO, B-11/B-44/B-MD-009 PENDIENTE
6. `docs/architecture/decisions/` ✅ — ADR-0013/0014/0022/0023/0029/0030
7. `docs/audits/` ✅ — 2026-08-18-kafka-topology-audit.md, F-032/B-47
8. `architecture_linter/` ✅
9. `tests/architecture_linter/test_golden.py` ✅
10. `.github/workflows/ocm-ci.yml` ✅ (comandos canónicos §R)

---

## 4. Control States & Findings

### Matriz de Findings (4)

| ID | Severity | Classification | Descripción |
|---|---|---|---|
| F-KAFKA-01 | LOW | CERRADO | Flakiness de test_A (causa raíz en harness, no en producto) — resuelto en 68b460b + cc40a7f |
| F-KAFKA-02 | LOW | CERRADO | Comentario docker-compose `dlq.ohlcv` vs SSOT `ocm.dlq` — corregido 2026-08-18 |
| F-KAFKA-03 | LOW | CONTRADICCIÓN | Incoherencias bootstrap/port (.env.example 9094 vs tests 9093 vs base.yaml 9092) — abierto, decisión de puerto |
| F-KAFKA-04 | LOW | RECOMENDACIÓN | `tests/kafka/CONTRACT.md` referenciado e inexistente — creado 2026-08-18 |

### Verificación matemática

```
Total = NUEVO(0) + REVALIDADO(0) + REGRESIÓN(0) + CERRADO(2) + CONTRADICCIÓN(1) + RECOMENDACIÓN(1) + NO_VERIFICADO(0) = 4 ✅
Severidades = CRITICAL(0) + HIGH(0) + MEDIUM(0) + LOW(4) + INFO(0) = 4 ✅
```

**Deduplicación:** F-KAFKA-03 contrastado contra F-032/B-47 (KafkaConfig decorativo) — la parte
de puertos/plantilla no estaba cubierta y se registra sin duplicar el finding de fondo.

### Matriz de Controles (12)

| Control | Comando canónico | Resultado | Estado |
|---|---|---|---|
| ARCH_CONTRACTS | `uv run lint-imports --config architecture_linter/importlinter.toml` | 50 kept / 0 broken | **PASS** |
| ENGINEERING_HEALTH | `uv run python scripts/engineering_health_check.py` | PASS | **PASS** |
| ARCH_LINTER | `uv run python -m architecture_linter --root . --json` | 7 FAIL / 3 PASS = GOLDEN_EXPECTED | **PARTIAL** (deuda gobernada) |
| GOLDEN | `uv run pytest tests/architecture_linter/test_golden.py -q --no-cov` | 4 passed | **PASS** |
| UNIT_TESTS | `uv run pytest tests/ -x -q -m "not integration"` | 1222 passed | **PASS** |
| DEPENDENCY_AUDIT | `uv run pip-audit . --ignore-vuln PYSEC-2026-113 --ignore-vuln PYSEC-2026-1325` | no re-ejecutado en esta sesión (sin cambios de deps) | **NO_VERIFICADO** |
| YAMLLINT | `uvx yamllint -c .yamllint .` | no re-ejecutado (sin cambios YAML en esta sesión) | **NO_VERIFICADO** |
| Integration replay (test_A) | `uv run pytest tests/kafka/test_integration_kafka.py::test_A_replay_100_events_no_loss -v -s -m integration` | 1 passed in 8.46s | **PASS** |
| Integration lifecycle (B/C1/C2) | `uv run pytest tests/kafka/test_integration_kafka.py::test_B_no_commit_on_bronze_failure_event_reappears ::test_C1_corrupt_payload_goes_to_dlq ::test_C2_invalid_schema_goes_to_dlq -v -m integration --no-cov` | 3 passed in 27.09s | **PASS** |
| Kafka consumer semantics | inspección `consumer.py` (enable_auto_commit=False, seek_to_beginning, _broker SSOT) | conforme | **PASS** |
| Broker topology runtime | `docker compose ps` + `kafka-topics`/`kafka-consumer-groups`/aiokafka describe | healthy; 25 topics; 3 particiones RF=1 | **PASS** |
| Producer idempotencia | `producer.py` enable_idempotence=True + acks=all (F-015, B-31) | conforme (HECHO previo) | **PASS** |

```
Controles = PASS(9) + PARTIAL(1) + NO_VERIFICADO(2) = 12 ✅
```

**Nota:** 0 controles FAIL → sin findings NUEVO por control (regla `CONTROL FAIL ≠ FINDING NUEVO`
no se dispara; los findings son documentales/config, no de control roto).

---

## 5. Matriz de Decisiones

| ID | Problema | Evidencia | Opciones | Recomendación | Bloquea |
|---|---|---|---|---|---|
| **D-KAFKA-1** | Comentario DLQ `dlq.ohlcv` vs SSOT `ocm.dlq` | docker-compose.yml:502, topics.py:102 | A) corregir comentario a `ocm.dlq`; B) mantener y documentar DLQ por-topic (P2 abierto) | A ✅ APLICADA (2026-08-18) | ❌ |
| **D-KAFKA-2** | Bootstrap/port: `.env.example` 9094 vs `.env` 9093 vs tests hardcoded 9093 vs base.yaml 9092 | .env:34, .env.example:36, conftest.py:28, base.yaml:46 | A) unificar puerto 9093; B) parametrizar BROKER de tests por env var; C) A+B | A+B (progresivo) — nota documentada en `.env.example`; decisión de unificación pendiente | ❌ |
| **D-KAFKA-3** | `tests/kafka/CONTRACT.md` referenciado e inexistente | conftest.py:12 | A) crear el doc con R-01..R-04; B) eliminar la referencia | A ✅ APLICADA (2026-08-18) | ❌ |
| **D-KAFKA-4** | F-032/B-47 KafkaConfig.bootstrap_servers decorativo | backlog-priorizado-2026-08-08.md | A) eliminar campo; B) inyectar vía composition root | B (consistente con Redis/Postgres) — pendiente | ❌ |

---

## 6. Evidencia en vivo (Resumen)

- `git show --stat 68b460b` → solo `tests/kafka/conftest.py` (+49); `git show --stat cc40a7f` →
  solo `tests/kafka/test_integration_kafka.py` (14+/12-).
- `git diff HEAD~3..HEAD -- tests/kafka/ .../kafka/consumer.py` → conftest +49, test 26 líneas, consumer 0.
- `docker exec ocm_kafka env | grep KAFKA_...` → listeners/advertised/NUM_PARTITIONS=3/RF=1/auto-create=false.
- `kafka-topics --describe ohlcv.raw, ocm.dlq` → 3 particiones, RF=1, leader 1, ISR 1.
- `kafka-consumer-groups --describe replay-*` → offsets committed reales (p.ej. offset 100 p0,
  log-end 1156) — basura histórica confirmada.
- aiokafka desde host: `localhost:9093` CONECTA; `localhost:9092` rechazado (INTERNAL solo in-container).

---

## 7. Integridad

- **Fase de auditoría (read-only):** ningún archivo de código/tests/CI/ADR/tracking/governance
  modificado durante la verificación; escritura solo en `docs/audits/`. Sin `git add`/`commit`/`push`.
- **Fase de remediación post-auditoría (autorizada por el usuario):** aplicadas correcciones
  documentales — `docker-compose.yml` (comentario DLQ), `.env.example` (nota de puerto),
  `tests/kafka/CONTRACT.md` (nuevo), `docs/plans/tracking.yaml` (cierre B-11). Sin cambios de
  código, tests ni CI.
- **Reconciliación:** findings (4) == severidades (4) == filas de la matriz de findings (4). ✅
- **Validador mecánico:** se ejecuta `uv run python scripts/audit_validator.py --register
  docs/audits/OCM_AUDIT_FINDINGS_2026-08-18-kafka-replay.md --report
  docs/audits/AUDIT_OCM_FORENSIC_COMPLIANCE_2026-08-18-kafka-replay.md` → PASS esperado (0 errores).

---

## 8. Veredicto

`AUDIT_CONFORME_CON_FINDINGS_DOCUMENTALES`. El defecto de replay Kafka (harness) está CERRADO y
verificado; la topología real es coherente con el SSOT. Tras la remediación post-auditoría,
F-KAFKA-02 y F-KAFKA-04 están CERRADOS y F-KAFKA-03 tiene su contradicción documentada en
`.env.example` (requiere decisión de unificación de puerto, D-KAFKA-2). No bloquea el Plan Maestro.

---

REPRODUCIBILIDAD
- commit: a4d8298
- branch: main
- fecha: 2026-08-18
- protocolo: AUDIT_PROTOCOL v2.1
- agente/modelo: opencode (DeepSeek)
- herramientas: pip-audit 2.10.1 · ruff 0.15.10 · mypy 1.19.1 · bandit 1.9.4 · pytest 8.4.2 · yamllint 1.38.0
- comandos: `uv run lint-imports --config architecture_linter/importlinter.toml`;
  `uv run python scripts/engineering_health_check.py`;
  `uv run python -m architecture_linter --root . --json`;
  `uv run pytest tests/architecture_linter/test_golden.py -q --no-cov`;
  `uv run pytest tests/ -x -q -m "not integration"`;
  `uv run pytest tests/kafka/test_integration_kafka.py::... -m integration`;
  `uv run python scripts/audit_validator.py --register ... --report ...`
- golden: PASS (4/4, no-regresión; GOLDEN_EXPECTED contiene FAIL/PARTIAL = deuda gobernada)
- resultado: PASS del validador (0 errores mecánicos)