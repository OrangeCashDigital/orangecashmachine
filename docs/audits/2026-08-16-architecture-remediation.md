# Remediación Arquitectónica OCM — 2026-08-16

## 1. Architecture Source

**ARCHITECTURE_SOURCE** = `docs/Clean Architecture A Craftsman Guide to Software Structure and Design.pdf` (Robert C. Martin, 2017).

Es fuente **conceptual normativa**, no SSOT operativo de OCM (KB: ADR/código/contratos prevalecen). Extractos aplicados:

- **Dependency Rule** — "source code dependencies must point only inward, toward higher-level policies".
- **4 círculos** — Entities → Use Cases → Interface Adapters → Frameworks & Drivers; cruce de boundaries por DIP.
- **"The database is a detail"** — persistencia y canales externos son capa externa, no dominio.

## 2. Baseline

| Gate | Baseline (antes) | Después | Delta |
|---|---|---|---|
| Linter findings (filas) | 28 | 19 | −9 |
| Violaciones reales (FAIL/PARTIAL) | 27 | 17 | −10 |
| ARCH-006 | FAIL (8) | **PASS (0)** | ✓ |
| ARCH-007 | FAIL (9) | FAIL (8) | −1 |
| ARCH-008 | FAIL (2) | FAIL (1) | −1 |
| Linter tests | 41 passed | 41 passed | = |
| pytest (sin Kafka env) | 1150 passed | 1150 passed | = |
| pytest Kafka env (preexistente) | 4 failed | 4 failed | = |
| mypy | 0 | 0 | = |
| import-linter | 50 kept / 0 broken | 50 kept / 0 broken | = |
| ruff check | 0 | 0 | = |
| bandit (severity Low) | 51 | 51 | = |

## 3. Matriz PDF → código

| PDF rule | Principio | ARCH-00X | Detección estática |
|---|---|---|---|
| Dependency Rule | DIP: interior no importa exterior | ARCH-009 | grafo de imports real (incl. lazy) vs capas BC-08 de `architecture/importlinter.toml` |
| Database is a detail | domain no importa infra/datos | ARCH-009 / BC-09 | forbidden contracts SSOT |
| State ownership | una sola fuente de verdad por entidad | ARCH-001/010 | almacenes mutables por símbolo |
| Business rules en el círculo interior | excepciones de dominio en domain | ARCH-007 | homónimos/duplicados de contracts |
| Boundaries no falsos | no exponer capability sin ejecutarla | ARCH-008 | comportamiento (no nombres) de stubs |

## 4. Golden Findings (F1–F4)

- **F1** — `WSTradesSource` stub de producción (detección por comportamiento: `__anext__` lanza `StopAsyncIteration` inmediato; `_running` nunca True). **Mantenido como stub diseñado** con fallback REST explícito (ver §12).
- **F2** — cadena de freshness rota niveles 3–6.
- **F3** — `MarketDataSourcePort` (outbound) huérfano. **Eliminado** (§8).
- **F4** — divergencia semántica de posición (WAC vs replace/pop). Preservada (§12).

## 5. Priorización

- **P0 (corregido)**: ports huérfanos (ARCH-006), excepción duplicada (ARCH-007), stub muerto no cableado (ARCH-008).
- **P1 (documentado)**: ARCH-003/004 → BLOCKED (ADR PROPUESTA); ARCH-005 → BLOCKED (cross-BC).
- **P2 (documentado)**: ARCH-001/010 → by-design con SSOT Portfolio (ADR-0006/BC-43).

## 6. Root causes

1. **Ports huérfanos** — contratos de fases de diseño previas sin consumidores ni implementaciones; algunos con docstring contradictorio y clases fantasma (`CcxtRestAdapter`, `ReplayAdapter` no existen). Evidencia: 0 referencias fuera del archivo de definición (regla `architecture_linter/rules/arch_006.py:39`).
2. **Excepción duplicada** — `ExchangeCircuitOpenError` definido en `domain/exceptions/__init__.py:227` y `ports/outbound/resilience.py:23`; el único consumidor del port era `ohlcv_pipeline.py:70`.
3. **Stub muerto** — `InfraMetricsKafkaProducer` con 0 referencias en producción (solo su propio archivo y el golden test).

## 7. Plan ejecutado

1. Inspección forense completa de los 8 ports huérfanos (consumidores, implementaciones, ADR-0014, importlinter).
2. Clasificación por evidencia: 6 ELIMINAR, 2 CABLEAR.
3. Eliminación de código muerto (precedente ADR-0009).
4. Consolidación de la excepción en domain (círculo interior, Dependency Rule).
5. Cableado de ports válidos con implementaciones reales.
6. Actualización de golden tests; reejecución de todos los gates.

## 8. Cambios realizados

**Eliminados (6 ports huérfanos + 1 stub muerto):**
- `ports/outbound/market_data_source.py` — huérfano, docstring se autodeclara INBOUND en `ports/outbound/`, implementación citada inexistente; rol cubierto por `ExchangeClientPort`/`HistoricalFetcherPort`.
- `ports/inbound/event_consumer.py` — huérfano; función cubierta por `EventBusPort` (vivo).
- `ports/outbound/event_publisher.py` — huérfano; publish cubierto por `EventBusPort` + publishers específicos (`OHLCVPublisherPort`, `ExternalEventPublisherPort`, `GapEventPublisherPort`).
- `ports/outbound/resilience.py` — `CircuitBreakerPort` huérfano (estado consultado vía `get_breaker_state()` del adapter) + excepción duplicada.
- `OrderBookSourceProtocol` (`ports/inbound/trades_source.py:133`) — huérfano; el flujo L2 real es callback-Kafka (`CryptofeedOrderBookStream`).
- `BronzeStoragePort` (`ports/outbound/storage.py:231`) — huérfano y no conforme (exigía `get_last_timestamp` que `BronzeStorage` no tiene).
- `adapters/inbound/websocket/infra_metrics_producer.py` — stub no cableado (0 consumidores).

**Consolidado (ARCH-007):**
- `domain/exceptions/__init__.py:227` — `ExchangeCircuitOpenError` extendido con `exchange_id`, `cooldown_remaining_ms`, `fail_counter` (kwargs opcionales, retrocompatible) como contrato canónico del dominio.
- `application/pipelines/ohlcv_pipeline.py:70` — import movido de `ports.outbound.resilience` a `domain.exceptions`.

**Cableados (ARCH-006):**
- `application/pipelines/derivatives_pipeline.py` — fetchers tipados `dict[str, DerivativesFetcherPort]` (import `ports/outbound/fetcher.py:65`); eliminado `# type: ignore[attr-defined]` en `:240`.
- `application/external_ingestion/orchestrator.py` — `ExternalMetricsPort` inyectado (default `NullExternalMetrics`); emisión en `_poll_loop`/`_run_cycle`/rate-limit. Wiring en `infrastructure/bootstrap/composition_root.py` con `PrometheusExternalMetrics()`.

**Ajustes derivados:** `infrastructure/kafka/bronze_writer.py:105` (docstring), `ports/__init__.py:14` (comentario), `ports/outbound/storage.py:16` (comentario).

## 9. Tests

- `tests/architecture_linter/` — 41 passed.
- Golden tests actualizados al nuevo estado verificado: ARCH-006 PASS, ARCH-007 8 duplicados, ARCH-008 1 stub.
- Guards anti-regresión añadidos: los ports huérfanos eliminados no deben reaparecer (`test_golden_arch006_orphan_ports`); `ExchangeCircuitOpenError` no debe volver a duplicarse (`test_golden_arch007_duplicates`); `InfraMetricsKafkaProducer` no debe reaparecer (`test_golden_arch008_stubs`).

## 10. Linter before/after

| Regla | Antes | Después |
|---|---|---|
| ARCH-001 | FAIL (1) | FAIL (1) |
| ARCH-002 | FAIL (2) | FAIL (2) |
| ARCH-003 | PARTIAL (1) | PARTIAL (1) |
| ARCH-004 | FAIL (1) | FAIL (1) |
| ARCH-005 | FAIL (1) | FAIL (1) |
| ARCH-006 | FAIL (8) | **PASS (0)** |
| ARCH-007 | FAIL (9) | FAIL (8) |
| ARCH-008 | FAIL (2) | FAIL (1) |
| ARCH-009 | PASS (0) | PASS (0) |
| ARCH-010 | FAIL (2) | FAIL (2) |
| **Resumen** | PASS=1 FAIL=8 PARTIAL=1 | PASS=2 FAIL=7 PARTIAL=1 |

## 11. Golden before/after

- **F3 (`MarketDataSourcePort`)** — ya no es finding: eliminado en raíz (no ocultado; el símbolo desapareció del repo).
- **ARCH-007** — `ExchangeCircuitOpenError` deja de ser duplicado; quedan 8 (OrderStatus, StorageFactoryPort, AnomalyRegistryPort, QualityPipelineResult, RetryExhaustedError, SchemaVersionError, _TransientProxy, PipelineContext).
- **ARCH-008** — solo `WSTradesSource` (diseñado); `InfraMetricsKafkaProducer` eliminado.

## 12. Arquitectura preservada

- `WSTradesSource` se mantiene: es un stub **diseñado y cableado** con fallback REST explícito (`source_manager.py:213-234`: rest_source obligatorio; ws opcional vía GapAwareStream). El finding queda honestamente FAIL.
- F4 divergencia semántica de posición: no se tocó (semántica financiera real; ADR-0025 WAC, ADR-0027).
- Domain sigue framework-agnostic (cero pandas/polars); no se introdujeron imports de infra en dominio.
- Contratos BC-NN intactos: 50 kept / 0 broken.

## 13. Riesgos

- `ExchangeCircuitOpenError` con estado: los adapters lanzan `CircuitBreakerOpenError` interno (adapter layer), por lo que el path de cooldown del pipeline sigue inactivo hasta que el adapter mapee a la excepción de dominio. Cambio de comportamiento diferido — ver §14.
- Cableado de métricas external_ingestion añade emisión Prometheus en runtime (SafeOps: no lanza).

## 14. UNKNOWN / BLOCKED

- **BLOCKED** — ARCH-003 (cancelación real) y ARCH-004 (balance real): ADR-0029 y ADR-0030 en estado PROPUESTA; no implementables sin aprobación.
- **BLOCKED** — ARCH-005 (freshness consultable): requiere contrato cross-BC y cambio de dominio trading (ADR previo).
- **UNKNOWN** — impacto completo de consolidar `CircuitBreakerOpenError` (adapter) → dominio: requiere tests de integración con breaker real; no hay tests de runtime que lo cubran hoy.

## 15. No-regresión

pytest 1150 passed (idéntico a baseline, excluyendo 4 fallos ambientales Kafka preexistentes `tests/kafka/test_integration_kafka.py`), mypy 0, import-linter 50/0, ruff 0, bandit 51 Low (sin nuevos).

## 16. Git state

- `M AGENTS.md` (preexistente, no tocado).
- 5 archivos eliminados, 9 modificados (ver `git status --short`).
- Sin commits, sin push, ADR-0029/0030 y `docs/plans/tracking.yaml` intactos.

## 17. Veredicto

**REMEDIACIÓN COMPLETA — PASS en el criterio de éxito.** Se corrigieron causas raíz reales (eliminación de ports huérfanos y stub muerto, consolidación de excepción en dominio, cableado de 2 ports válidos), sin debilitar reglas, sin suppressions, sin degradar negocio. El linter pasa a PASS=2/FAIL=7/PARTIAL=1 porque el código es ahora arquitectónicamente más correcto, no porque se haya maquillado la salida. Los hallazgos restantes son: 1 by-design documentado (ARCH-001/010), 1 divergencia semántica preservada (ARCH-002), 2 BLOCKED por ADR PROPUESTA (ARCH-003/004), 1 BLOCKED cross-BC (ARCH-005), 1 stub diseñado (ARCH-008), y 8 contratos duplicados (ARCH-007) — todos con evidencia reproducible.