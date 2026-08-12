# Engineering Guardrails — correctness by construction

**Origen:** iniciativa surgida a partir de F-031/B-46 (NullOHLCVPublisher
silencioso en el path OHLCV de market_data, ver
`docs/audits/2026-08-08-streaming-canary-audit.md` y
`docs/plans/backlog-priorizado-2026-08-08.md`).

**Objetivo:** convertir cada hallazgo de ingeniería, empezando por F-031, en
una restricción verificable (test, contrato de arquitectura, o gate de CI)
que haga difícil o imposible reintroducir el mismo tipo de error. No es una
lista de 12 bugs aislados — es una lista de 12 mecanismos de prevención.

**Regla:** este documento es la vista de estado de los guardrails. Los
hallazgos y backlog items individuales siguen viviendo en `tracking.yaml`
(SSOT operativo) y `backlog-priorizado-2026-08-08.md` (vista de
priorización). Ante discrepancia, esos dos mandan.

---

## Estado

| # | Guardrail | Estado | Evidencia |
|---|---|---|---|
| 1 | Contratos import-linter (arquitectura por capas) | ✅ Maduro, sin trabajo nuevo | 49+ contratos activos en `architecture/importlinter.toml` |
| 2 | Fix F-031: publisher/chunk_converter obligatorios, NullOHLCVPublisher prohibido en producción | ✅ Cerrado | commit `74f8b09` |
| 3 | `publish_chunk()` retorna `PublishResult` explícito en vez de `bool` ambiguo | ✅ Cerrado | commit `5724191` |
| 4 | Test de wiring real del composition root sin mocks (`_build_kafka_publisher` → `KafkaOHLCVPublisher` real) | ✅ Cerrado | `tests/architecture/test_kafka_publisher_wiring.py`, commit `8afc09d` |
| 5 | Test de guardrails de Kappa (NullPublisher prohibido en prod, publisher/chunk_converter obligatorios) + inyección real verificada en `_build_ohlcv` | ✅ Cerrado | `tests/architecture/test_kappa_publisher_wiring.py` (9 tests), commit `5749c1f` |
| 6 | Separación explícita de publishers permitidos por entorno en config | ✅ Cerrado | docstring `EnvironmentConfig.name` corregido (commit `1253c8f`); publishers gobernados por `integrations.kafka.enabled` + guard fail-fast en `_build_ohlcv` |
| 7 | Pipeline CI como barrera dura: ruff → mypy → import-linter → tests → arch → config → health → docs | ⏳ Pendiente | — |
| 8 | Detección automática de documentación contradictoria (ADRs vs código real) | 🔄 Parcial — docstrings contradictorios corregidos a mano (market_data/main.py "modo degradado", paper/live stale) | — |
| 9 | Contratos de observabilidad obligatorios (ej. alertar si `_build_event_bus_wiring` falla en silencio) | ✅ Cerrado | `_build_event_bus_wiring` ahora loguea por loguru + counter `ocm_quality_consumer_wiring_failures_total`; test `tests/market_data/test_quality_consumer_wiring.py` |
| 10 | Test de lifecycle SIGTERM/SIGINT | ✅ Cerrado | handler SIGTERM→KeyboardInterrupt en `apps/app/cli/main.py`; test `tests/app/test_sigterm_handler.py` |
| 11 | Bootstrap común entre entrypoints (`ocm`/`paper`/`live`) | ⏳ Pendiente | — |
| 12 | Cada hallazgo se documenta y se convierte en test sistemáticamente | 🔄 Parcial — F-031 como caso piloto | — |

---

## Notas de diseño por guardrail

### #6 — Publishers por entorno
Hallazgo colateral durante #3: `EnvironmentConfig.name` está documentado en
`ocm/config/schema.py:73` como *"solo descriptivos, no controlan
comportamiento"*, pero `pipeline_factory._build_ohlcv` ya lo usa para decidir
fail-fast en producción (`is_production = ... == "production"`). El
docstring está desactualizado respecto al comportamiento real — revisar antes
de diseñar la separación formal de publishers por entorno, para no construir
sobre una premisa incorrecta.

### #12 — Hallazgo → test sistemático
F-031/B-46 es el piloto: audit → backlog → fix → test de arquitectura
(#4, #5) → tipado explícito (#3). El patrón a formalizar: ningún hallazgo se
cierra sin al menos un test que falle si la regresión vuelve a ocurrir.
