# AUDITORÍA — INCIDENTE DE STREAMING + ARQUITECTURA DE ENTRYPOINTS / SSOT / CLEAN ARCHITECTURE (2026-09-17)

- **Fecha:** 2026-09-17
- **Modo:** READ-ONLY. Cero modificaciones a código, tests, CI, ADRs, tracking, systemd, Docker o Kafka. Única escritura: este documento.
- **Complementa:** `docs/audits/AUDIT_OCM_streaming-incident-recovery_2026-09-17.md` (forense del incidente) y `docs/audits/AUDIT_OCM_deployment-portability_2026-09-17.md` (deployment/portabilidad). Referenciados, no modificados.
- **Regla de honestidad:** toda afirmación está etiquetada `VERIFIED` (comprobada por código/config/comandos/evidencia observada), `UNVERIFIED` (no comprobada) o `INFERENCE` (deducción razonable no demostrada). No se convierte ninguna inferencia en hecho.

---

## 1. Executive Summary

Se audita de forma **conjunta y sin asumir causalidad** el incidente de streaming del 2026-09-17 (schema orderbook v1→v2 + readiness de Kafka) y la **arquitectura de entrypoints / SSOT / Clean Architecture** de OrangeCashMachine.

**Hallazgos centrales:**

1. **READINESS (B-59/H-DEP-05, CRITICAL contextual):** `ocm-streaming.service` entró en crash-loop al arrancar sin Kafka lista (`streaming_failed | KafkaConnectionError`, PIDs nuevos cada ~11s). Solo `After=docker.service`, **sin `ExecStartPre`/readiness**. Recuperación observada 17:20:02 (MainPID 9371, `streaming_started` 17:20:13). Es evidencia operacional directa de la deuda de readiness (B-59). `VERIFIED`.
2. **MIGRACIÓN SCHEMA v1→v2 (break commit `3706d61c`):** 641.073 `book_builder_deserialize_error` antes de 17:20 (0 después); mensajes v1 en DLQ (`event_version=1`, `side/price/size`, `update_id=0`, fechados 2026-09-07). El **mecanismo** (v1 → `SchemaVersionError` → DLQ) está confirmado por código; la **atribución cuantitativa** de los 641.073 al backlog v1 es `INFERENCE` (el log no ha quedado con el detalle de error). `VERIFIED` mecanismo / `INFERENCE` atribución.
3. **STALE (`book_builder_stale`, 304 eventos tras 17:20, activos a 21:30):** umbral `stale_ms=2000` vs jitter del feed (observados 2013–2288ms). No es un defecto confirmado; es comportamiento del umbral frente a la cadencia real del feed. `VERIFIED` mecanismo / `INFERENCE` explicación de fondo.
4. **ENTRYPOINTS / SSOT (H-DEP-03 REVALIDADO):** `run.sh` no incluye `streaming`; systemd usa `python -m app.cli.streaming_hydra` en vez del script registrado `[project.scripts]`; el sistema declara SSOT que la práctica no respeta.
5. **CLEAN ARCHITECTURE (línea por línea):** streaming_hydra es limpio (CompositionRoot como punto de ensamblado de producers, load_appconfig_standalone BC-51, use case BookBuilder 100% framework-agnostic), con **una fuga puntual de wiring**: `BookBuilderConsumer` se ensambla en `_run_streaming` mediante `build_book_builder_consumer(config)` (única vía de composición, BC-07 DIP-safe deferido a CompositionRoot — correcto), pero el propio `CryptofeedOrderBookStream` (adapter) se construye en el entrypoint (no vía CompositionRoot) — ver F-09.
6. **HYDRA COMO DETALLE:** `main.py` usa `@hydra.main`; `streaming/live/paper_hydra` usan `load_appconfig_standalone()` sin decorator (BC-51). El nombre `*_hydra.py` filtra un detalle de implementación (F-08).
7. **CONFIG SERVICE:** `load_appconfig_standalone()` → merge base→módulos→env→settings → `apply_env_overrides` (L2) → Pydantic `AppConfig` (L4) → `write_config_snapshot` en producción. Vía formal única aunque existan dos caminos de entrada (compose Hydra vs standalone). `VERIFIED`.
8. **CURSE DE ENV OVERRIDE (host.env):** `config_l2_env_override | malformed_key=OCM_HOST_USER/OCM_REPO_ROOT` (15:50:10) proviene de `deploy/host.env` (cargado por systemd vía `EnvironmentFile`), cuyas claves no siguen el protocolo L2 `OCM_SECTION__KEY` → warning estructural esperado (fail-soft). Hallazgo menor de naming/SSOT (F-06).

Matriz completa de findings: §14. Arquitectura objetivo: §15. Plan por fases: §16. Priorización: §17. Resultado del validador mecánico: §18.

---

## 2. Reproducibility Block

```
REPRODUCIBILIDAD
- commit: cb6c6d7c (HEAD, rama feat/adr0028-bookbuilder)
- branch: feat/adr0028-bookbuilder
- fecha: 2026-09-17
- protocolo: AUDIT_PROTOCOL v2.1
- agente/modelo: big-pickle (opencode)
- herramientas (uv run python scripts/audit_validator.py --versions):
    pip-audit 2.10.1 · ruff 0.15.10 · mypy 1.19.1 · bandit 1.9.4 ·
    pytest 8.4.2 · yamllint 1.38.0
- comandos canónicos citados:
    ARCH_CONTRACTS: uv run lint-imports --config architecture_linter/importlinter.toml
    ARCH_LINTER:    uv run python -m architecture_linter --root . --json
    DEPENDENCY_AUDIT: uv run pip-audit . --ignore-vuln PYSEC-2026-113 --ignore-vuln PYSEC-2026-1325
    YAMLLINT:       uvx yamllint -c .yamllint .
- golden: no evaluado aquí (--golden no provisto; GOLDEN se gobierna por tests/architecture_linter/test_golden.py)
- resultado validador: ver §18
```

---

## 3. Scope y Metodología

**Incluido:**
- Git read-only (rama, HEAD, estado del working tree, log de commit del schema).
- Forense del incidente: schema orderbook v1/v2, `BookBuilderConsumer`, `BookBuilder`, DLQ, stale, restart.
- Entrypoints: `apps/app/cli/{main,live_hydra,paper_hydra,streaming_hydra,entrypoint,_bootstrap}.py`, `apps/api/main.py`, `[project.scripts]`, `run.sh`, systemd templates/unidades.
- Composition Root (`market_data/infrastructure/bootstrap/composition_root.py`), Configuration Service (`ocm/config/*`), schema Pydantic.
- Referencias: ADR-0022, tracking.yaml (B-59/B-60), deployment-portability (H-DEP-01..10), recovery (incidente), bybit.md / P0.
- Verificación mecánica: `scripts/audit_validator.py` en FASE 18.

**Excluido (read-only / sin permiso / fuera de alcance):**
- Contenido del `.env` real; offsets/logins de Kafka; `systemd-analyze verify` en host; creación/borrado de topics; restart de servicios.
- FASE 2 de diseño (solo planificación, no implementación).

**Fuentes jerárquicas (AUDIT_PROTOCOL):** código/tests > contratos arquitectónicos > ADRs > documentación oficial de tecnologías > KB interna > literatura externa. Las referencias externas (NautilusTrader/Hummingbot/Freqtrade) se usan SOLO como patrones conceptuales, sin autoridad normativa (FASE 13).

---

## 4. Governance Baseline

| Fuente | Evidencia |
|---|---|
| Plan Maestro | `docs/PLAN-Maestro-Ingenieria.md` |
| Governance | `docs/architecture/GOVERNANCE.md` |
| Tracking | `docs/plans/tracking.yaml` (B-59 PENDIENTE/CONFIRMADO; B-60 PENDIENTE/CONFIRMADO) |
| ADRs | `docs/architecture/decisions/ADR-0022` (Propuesto; deuda consciente de readiness L215-217/239-240) |
| Contratos | `architecture_linter/importlinter.toml` (BC-07, BC-08, BC-38, BC-42, BC-51) |
| Protocolo auditoría | `docs/governance/AUDIT_PROTOCOL.md` v2.1 (§Q M1..M20, §R comandos canónicos, §S reproducibilidad) |
| Auditorías previas | `AUDIT_OCM_streaming-incident-recovery_2026-09-17.md`, `AUDIT_OCM_deployment-portability_2026-09-17.md` |

---

## FASE 0 — Protocolo de auditoría

- Aplicado `docs/governance/AUDIT_PROTOCOL.md` v2.1: reglas mecánicas M1..M20 (comandos canónicos en §R), taxonomía estricta y control FAIL ≠ finding NUEVO.
- `scripts/audit_validator.py` presente y ejecutable (MACHINE CHECKS FIRST). `VERIFIED`.
- Orden de descubrimiento: Plan Maestro → Governance → tracking → ADRs → contratos/linters. `VERIFIED` implementación del orden.

## FASE 1 — Inventario Git read-only

| Ítem | Evidencia | Clasificación |
|---|---|---|
| Rama | `feat/adr0028-bookbuilder` | VERIFIED |
| HEAD | `cb6c6d7c` | VERIFIED |
| Working tree | ` M docs/plans/tracking.yaml` + `?? docs/audits/AUDIT_OCM_streaming-incident-recovery_2026-09-17.md` + `?? docs/audits/AUDIT_OCM_deployment-portability_2026-09-17.md` — **nada más** | VERIFIED |
| Commit schema v2 | `3706d61c` (2026-08-29, breaking): `shared/kafka/provenance.py`, `shared/kafka/schemas/orderbook.py`, `tests/kafka/test_schemas_orderbook.py` | VERIFIED |
| Commit BookBuilder | `144d8552` (BookBuilder — reconstrucción L2 desde orderbook.raw v2) | VERIFIED |
| Commit wiring | `4e14df25` (wire BookBuilderConsumer into streaming_hydra; template systemd) | VERIFIED |
| `deploy/host.env` y `deploy/host.env.example` | **trackeados en Git** (`git ls-files` → ambos listados) → REVALIDADO H-DEP-01 | VERIFIED |

## FASE 2 — Forense schema v1/v2 (incidente)

### 2.1 Schema v1 (antes de `3706d61c`)
- `OrderBookSnapshotPayload`/`OrderBookDeltaPayload` con `side: Literal["bid","ask"]`, `Side`, `_VALID_SIDES`. Delta v1 publicaba **un nivel por mensaje** (`side`/`price`/`size`). `VERIFIED` (git show `3706d61c^`).
- `timestamp_ms` de anclaje; `from_dict` validaba `event_version` contra v1.

### 2.2 Schema v2 (`3706d61c`)
- **BREAKING:** delta atómico **multinivel** `bids`/`asks` (D-7a); continuidad por `update_id` 'u' no por seq+1 (D-7b); str para Decimal (D-7c); snapshot y delta bumpados a `SCHEMA_VERSION=2` (evita estado mixto snap=1/delta=2 en la misma partición). `VERIFIED` (código actual `shared/kafka/schemas/orderbook.py`).
- `OrderBookSnapshotPayload.from_dict` / `OrderBookDeltaPayload.from_dict` → `if version != 2: raise SchemaVersionError`. `VERIFIED` (L129-133 / L226-230).

### 2.3 Mecanismo del incidente
1. `BookBuilderConsumer._process_message` → `deserialize(msg.value, ...)` → para payload v1 con `event_version=1` → `SchemaVersionError` → `book_builder_deserialize_error` → `_send_to_dlq`. `VERIFIED` por código (book_builder_consumer.py L223-231; schemas L226-230).
2. DLQ contiene mensajes v1 (`event_version:1`, `side/price/size`, `update_id:0`, `occurred_at` 2026-09-07) — evidencia del formato v1 en el topic. `VERIFIED` (DLQ leído en recovery audit).
3. 641.073 `book_builder_deserialize_error` en journal hasta 17:17:55; 0 después del restart 17:20:02. `VERIFIED` (journal, recovery audit).
4. `orderbook.raw` hoy: 1 partición, from-beginning solo v2 (muestras) — no conserva v1 del 09-07 pese a retención 168h (`KAFKA_LOG_RETENTION_HOURS=168`). **Sugiere topic reconstruido o drenado; la acción de recreación es UNVERIFIED/**INFERENCE**.** `VERIFIED` estado final / `UNVERIFIED` acción.

### 2.4 Atribución
- Que los 641.073 errores fueran (en su totalidad) v1 residual **no está demostrado** (el journal no guarda `error=`/`ptype=`/`offset=` — loguru compacto). La **hipótesis v1** es coherente con: v1 existente en el topic (DLQ), commit breaking, y cese total tras restart. Clasificación: **INFERENCE**. `VERIFIED` mecanismo → §18 transparencia.

### 2.5 Temas abiertos (no determinables)
- ¿Por qué no quedaron v1 del 09-07 en `orderbook.raw` (retención 168h)? → recreación/topic reset no verificable. `UNVERIFIED`.
- ¿`book_builder_consumer_started` observable en runtime? → código VERIFIED; runtime UNVERIFIED (recovery audit §7).

## FASE 3 — Restart / recovery (incidente)

| Hecho | Evidencia | Clasificación |
|---|---|---|
| Crash-loop 00:00→17:20 | `streaming_failed \| KafkaConnectionError: Unable to bootstrap from [('localhost', 9093, ...)]` repetido, PIDs nuevos cada ~11s | VERIFIED |
| `Restart=on-failure` + `RestartSec=10` | intervalos ~11s consistentes | VERIFIED |
| Recuperación 17:20 | `systemctl show` → MainPID 9371, NRestarts=0, ActiveEnter 17:20:02; `streaming_started` 17:20:13 (bybit, 3 símbolos) | VERIFIED |
| `docker compose up -d zookeeper kafka` | **evidencia reportada por el usuario**; el momento exacto no revalidable desde journal. Estado final (Kafka healthy, servicio estable) VERIFIED | INFERENCE momento / VERIFIED estado |
| Sin readiness | templates: `After=docker.service` + `Wants=network-online.target`, **sin `ExecStartPre`/`Requires` de Kafka** | VERIFIED |

**Conclusión operacional:** B-59/H-DEP-05 demostrado: daemon Docker activo ≠ Kafka ready; el proceso OCM muere sin broker disponible.

## FASE 4 — `book_builder_stale`

- Mecanismo: `BookBuilder(viewport=50, stale_ms=2000)`; `check_stale(now_ms)` solo evalúa libros con `has_snapshot=True` (book_builder.py L270-271; L272 umbral). Adapter publica warning `book_builder_stale` (book_builder_consumer.py L299-311). `VERIFIED`.
- Observado: 304 eventos tras 17:20 (journal), **presentes también a 21:30**, valores 2013–2288ms (umbral 2000ms). `VERIFIED`.
- Interpretación: feed cadencia/jitter alrededor del umbral → STALE. No "bug"; es el comportamiento del umbral configurado. Explicación de fondo (feed lento en tramos): `INFERENCE`. Traza por-símbolo en runtime: `UNVERIFIED`.
- **No se probó raw offset/burst**: el patrón es recurrente, no un único burst de arranque. `VERIFIED`.

## FASE 5 — Correlación (no asumir causalidad)

Análisis conjunto **sin imponer cadena de causa**: ambas cosas pueden ser independientes aunque compartan ventana.

| Hipótesis | Evidencia | Estado |
|---|---|---|
| **H1:** Los errores de deserialización eran v1 residual del topic | v1 en DLQ + commit breaking + cese tras restart | MECANISMO VERIFIED; atribución cuantitativa INFERENCE |
| **H2:** El crash-loop fue por Kafka no lista | journal KafkaConnectionError + sin ExecStartPre | VERIFIED |
| **H3:** El restart de 17:20 drenó/recreó el backlog con topic limpio | 0 errores desde 17:20:05; topic 1 partición solo v2 | ESTADO VERIFIED; acción INFERENCE |
| **H4:** Los STALE seriales son jitter del feed cerca del umbral | 304 eventos, valores 2013-2288ms, sin burst único | MECANISMO VERIFIED; explicación INFERENCE |
| **H5:** Ambas líneas comparten el mismo "reinicio de proceso" sin relación causal entre sí | restart único 17:20; deserialize y stale en PIDs distintos | VERIFIED (restart único) |

**No se declara causalidad v1→stale ni stale→deserialize.**

## FASE 6 — Entrypoints

| Entrypoint | Script `[project.scripts]` | Módulo real | systemd | run.sh |
|---|---|---|---|---|
| batch ocm | `ocm = app.cli.main:main` | `python -m app.cli.main` | ocm-market-data (inactivo, `-m market_data.main`) | `ocm → python -m app.cli.main` |
| live | `live = app.cli.live_hydra:main` | — | — | `live → python -m app.cli.live_hydra` |
| paper | `paper = app.cli.paper_hydra:main` | — | — | `paper → python -m app.cli.paper_hydra` |
| **streaming** | `streaming = app.cli.streaming_hydra:main` | `python -m app.cli.streaming_hydra` | **ocm-streaming activo** | **AUSENTE** |
| ocm-api | `ocm-api = api.main:serve` | `api.main:serve` | — | AUSENTE |

**Hallazgos de entrypoints (FASE 6):**
- **F-07 (REVALIDADO H-DEP-03):** `run.sh` declara ser SSOT de `[project.scripts]` pero **omite `streaming`** (y `ocm-api`). `VERIFIED` (run.sh:12-20, 29-39 vs pyproject:176-181).
- **F-08 (NUEVO, INFO):** los modules `live_hydra/paper_hydra/streaming_hydra` expone "hydra" en el **nombre**, pero 3 de 4 no usan `@hydra.main` (usan `load_appconfig_standalone`, BC-51); solo `main.py` usa el decorator Hydra. Detalle de implementación filtrado al naming. `VERIFIED`.
- **F-09 (RECOMENDACIÓN):** `_bootstrap.py` (AUDIT-H8) es el scaffolding compartido; `--capital` obligatorio en live (SafeOps) correcto; failure ante `StackProps` ausente. `VERIFIED`.
- API (`api.main:serve`): `uvicorn.run("api.main:create_app", factory=True, reload=not production)`; Fail-Fast en Redis; metrics push de streaming separado del batch (ver FASE 7/15). `VERIFIED`.

## FASE 7 — systemd / SSOT de deployment

| Ítem | Evidencia | Estado |
|---|---|---|
| Template streaming | `After=network-online.target docker.service`; `User=${OCM_HOST_USER}`; `EnvironmentFile=${OCM_REPO_ROOT}/deploy/host.env`; `ExecStart=${OCM_REPO_ROOT}/.venv/bin/python -m app.cli.streaming_hydra`; SafeOps | VERIFIED |
| Divergencia 3-vías (F-004) | rendered==instalado; **ambos añaden `EnvironmentFile=<root>/.env` que el template NO declara** | VERIFIED (H-DEP-04) |
| SSOT entrypoints | systemd usa `-m` y no el script `streaming` registrado | VERIFIED (H-DEP-03) |
| Readiness Kafka | **sin `ExecStartPre`, sin `Requires` Kafka** | VERIFIED (B-59) |
| host.env versionado | `deploy/host.env` + `.example` en `git ls-files` | VERIFIED (H-DEP-01) |
| Installer | `install_systemd.sh`: render específico de vars, `systemd-analyze verify`, `install -m 644`, `daemon-reload`; **sin enable/start/precheck** | VERIFIED (H-DEP-07) |
| Puerto Kafka | compose EXTERNAL `localhost:${KAFKA_HOST_PORT:-9093}`; kafka-ui conflicto 9093 (Alertmanager) documentado F-029/B-45; `.env.example` recomienda 9094 | VERIFIED (H-DEP-06) |

## FASE 8 — Clean Architecture línea por línea

### `apps/app/cli/streaming_hydra.py` (326 líneas)
1. **_bootstrap de logging** → `app.cli._bootstrap.setup_logging` (R14/AUDIT-H8): SSOT de scaffolding. Correcto.
2. **Config** → `load_appconfig_standalone(env, run_id)` (BC-51, sin DictConfig crudo fuera de ocm.config): correcto (FASE 11).
3. **Pusher** → `PrometheusPusher()`/`NoopPusher()` según `config.observability.metrics.enabled` (SSOT PUSHGATEWAY_URL → RunConfig.pushgateway; F206): correcto.
4. **`_run_streaming`**:
   - `CompositionRoot.build_ws_producers(bootstrap_servers, source=DATASOURCE_REPLAY)` — **único punto de ensamblado de producers WS (BC-38/BC-42)**. Correcto (composition_root.py L330-392).
   - **`CryptofeedOrderBookStream(exchange, symbols, on_snapshot=bundle.orderbook.on_snapshot, on_delta=bundle.orderbook.on_delta)`** — el **adapter de origen** se construye en el entrypoint, no vía CompositionRoot. Es la única fuga de composición (los producers sí pasan por CR). Ver **F-09**.
   - `BuildersConsumer` via `_CR.build_book_builder_consumer(config)` + `start()` + `create_task` + cancel/stop en shutdown (ADL-0022). Correcto (wiring DIP en CR).
   - Import-only de `market_data.adapters.inbound.websocket.cryptofeed_orderbook_stream` — **lazy import dentro de método**, no top-level; respeta `E402` y el orden de capas (no hay import de infrastructure por aplicación). Mejorable: mover la construcción del stream al CR (F-09).
5. **Exit codes** 0/1, señales SIGINT/SIGTERM → `stop` event. Correcto.
6. **Caveat de seguridad:** `uuid4` de `run_id` no se usa aquí para snapshot (lo hace `load_appconfig_standalone` con `write_snapshot` implícito en producción). `VERIFIED`.

### Otros entrypoints
- `main.py` (batch): `@hydra.main` + `ConfigStore` structured configs; `hydra_cfg_to_appconfig` → pipeline L1-L5; `OCM_VALIDATE_ONLY=true` validación y exit 0. Correcto.
- `live_hydra/paper_hydra`: `assemble_cli_config` tipados, `model_copy` para max_order_usd (H1), SSOT `min_order_usd` (H4), `handle_sigterm` — todo DRY vía `_bootstrap.py`. Correcto.
- `entrypoint.py`: runner batch fail-soft por exchange, exit code int. Correcto.

## FASE 9 — "Hydra" como detalle de implementación

- `ocm/config/hydra_loader.py` es el **puente único** DictConfig→AppConfig (BC-51). `load_appconfig_standalone` replica el merge sin decorator (base→modules→env→settings). `VERIFIED`.
- Los entrypoints de larga duración (`streaming/live/paper`) usan standalone; solo `main.py` (batch) usa Hydra decorator.
- **Recomendación F-08**: renombrar `*_hydra.py` a nombres neutros (p.ej. `streaming.py`, `live.py`, `paper.py`) para no filtrar Hydra; o documentar que "hydra" es un legado de naming. NO es un defecto de arquitectura; es naming hygiene. `RECOMENDACIÓN`.

## FASE 10 — Composition Root (BC-38/BC-42)

`market_data/infrastructure/bootstrap/composition_root.py` (453 líneas):
- **API pública correcta:** `assemble(config)`, `build_feed_orchestrator`, `build_external_ingestion_orchestrator`, `build_ws_producers`, `build_book_builder_consumer`. `VERIFIED`.
- DIP: inyecta `BookBuilder` (application) en `BookBuilderConsumer` (infrastructure) vía `BookBuilderPort` (BC-07). `VERIFIED`.
- SafeOps/Fail-Fast: `bootstrap_servers` vacío → `ValueError`; producers no conectan en build. `VERIFIED`.
- **Gap F-09:** el stream origen (`CryptofeedOrderBookStream`) queda fuera del CR; composition_root no ofrece `build_ws_stream(exchange, symbols, ...)`. El entrypoint "cablea" el adapter de origen a los callbacks del bundle. Recomendado centralizar en CR (W-1).
- Import-linter: BC-38 nombre ("PipelineFactory solo instanciable...") es **más estrecho** que el docstring del módulo ("Ningún módulo fuera de infrastructure/bootstrap/ puede instanciar adaptadores concretos") — diseño documental más fuerte que el contrato mecanizado (F-09 relacionado).

## FASE 11 — Configuration Service (SSOT)

- Pipeline formal `ConfigPipeline` L1→L5 (Hydra compose → strip internals → coerce → env override L2 → Pydantic L4 → snapshot). `VERIFIED`.
- SSOT de env vars: `ocm/config/env_vars.py` (KAFKA_BOOTSTRAP_SERVERS, PUSHGATEWAY_URL, OCM_*, ...). `VERIFIED`.
- **F-06 (NUEVO, LOW):** `deploy/host.env` usa `OCM_HOST_USER`/`OCM_REPO_ROOT` que **no siguen L2** (`OCM_SECTION__KEY`), generan `config_l2_env_override | malformed_key` con warning. No rompe nada (fail-soft) pero contamina el log de configuración y sugiere namespace de env = acoplamiento de deployment. `VERIFIED`.
- `write_config_snapshot` en producción (reproducibilidad). `VERIFIED`.
- Vía alternativa `ConfigPipeline(cfg).run()` para batch con Hydra. `VERIFIED`.

## FASE 12 — Cruce con deployment-portability (H-DEP-01..10)

| H-DEP | Estado tras este análisis |
|---|---|
| H-DEP-01 (host.env versionado) | REVALIDADO (F-06 related) |
| H-DEP-02 (repo_root requiere .git) | REVALIDADO (fuera de incidente, riesgo deployment) |
| H-DEP-03 (SSOT entrypoints) | REVALIDADO (F-07) |
| H-DEP-04 (unit divergencia 3-vías) | REVALIDADO |
| H-DEP-05 / B-59 (readiness) | REVALIDADO con evidencia operacional (incidente) |
| H-DEP-06 (puerto Kafka inconsistente) | REVALIDADO |
| H-DEP-07 (installer sin precheck/enable/start) | REVALIDADO |
| H-DEP-08 (gates asumen host) | REVALIDADO |
| H-DEP-09 (sin runbook) | REVALIDADO |
| H-DEP-10 (código sin hardcodes de máquina) | REVALIDADO (grep limpio) |

Ninguno cierra en este análisis. Todos permanecen PENDIENTES (tracking) salvo H-DEP-10 (smoke conformidad).

## FASE 13 — Referencias externas (patrones, sin autoridad normativa)

Conocimiento externo para *inspirar* diseño, no para imponer obligación (KB governance: conocimiento ≠ norma):
- **Dual-version failover / protobuf con oneof** (patrón general de mensajería): tener v1 y v2 conviviendo con adapters de versión device, o migración con ventana dual-read, es un patrón contrastado. Aplica a la recomendación de *reconciliación de DLQ* y *schema evolution policy* (W-3).
- **NautilusTrader / Hummingbot / Freqtrade:** uso de eventos con backoffs/retry y book rebuild por snapshot+delta, con heartbeats de staleness. Relevante para `BookBuilder.check_stale` (umbral/detección) pero **no** justifica cambiar la arquitectura (autoridad: OCM ADRs > literatura).
- **Operational Runbooks (SRE de facto):** readiness + runbook de inicio/recuperación es estándar. Refuerza B-59.

Ninguno de estos es una norma de OCM; son referencias conceptuales. No alteran findings ni decisiones.

---

## 14. Matriz de Findings

Resumen del registro (mismo cálculo en §18):

- REVALIDADO: 7
- NUEVO: 4
- RECOMENDACIÓN: 2
- CONTRADICCIÓN: 1
total 14

| ID | Severity | Classification | Finding |
|---|---|---|---|
| F-01 | HIGH | REVALIDADO | Readiness Kafka ausente (B-59/H-DEP-05) — crash-loop demostrado |
| F-02 | HIGH | REVALIDADO | Migración schema v1→v2 con consumo acumulado → DLQ masivo |
| F-03 | MEDIUM | REVALIDADO | Log de deserialización sin detalle (error=/ptype=/offset=) en journal |
| F-04 | MEDIUM | REVALIDADO | STALE por umbral 2s vs jitter del feed (B-59 adyacente) |
| F-05 | MEDIUM | REVALIDADO | Recreación/topic reset de orderbook.raw no verificable |
| F-06 | LOW | NUEVO | host.env usa OCM_* que no cumplen protocolo L2 → warning malformed_key |
| F-07 | MEDIUM | REVALIDADO | SSOT entrypoints roto (run.sh sin streaming; systemd -m vs script) |
| F-08 | INFO | RECOMENDACIÓN | Entriespoint `*_hydra.py` filtra Hydra en el nombre (naming hygiene) |
| F-09 | MEDIUM | RECOMENDACIÓN | CryptofeedOrderBookStream (adapter) cableado en entrypoint, no en CompositionRoot |
| F-10 | MEDIUM | CONTRADICCIÓN | Docstring retention orderbook.raw "1h" vs compose 168h; doc y topología divergen |
| F-11 | LOW | NUEVO | Streaming/metrics de observabilidad (pushgateway) no operativo durante el incidente |
| F-12 | MEDIUM | NUEVO | Divergencia units template↔rendered/instalado (EnvironmentFile .env) |
| F-13 | MEDIUM | REVALIDADO | DLQ sin reconciliación/replay (mensajes v1 retenidos desde 09-07 sin proceso) |
| F-14 | LOW | NUEVO | Inconsistencia puerto Kafka (9093/9094) entre conftest, host.env y .env.example (H-DEP-06) |

### Fichas de findings

## F-01 — Readiness Kafka ausente (B-59/H-DEP-05)
Severity: HIGH
Status: OPEN
Classification: REVALIDADO
Control: B-59 / H-DEP-05
Evidence:
- journal 2026-09-17: `streaming_failed | KafkaConnectionError: Unable to bootstrap from [('localhost', 9093, ...)]` en loop cada ~11s
- templates systemd: `After=network-online.target docker.service` sin `ExecStartPre`/`Requires` Kafka
- recuperación 17:20:02 (MainPID 9371, NRestarts=0, `streaming_started` 17:20:13)
Traceability:
- Tracking: B-59 · ADR: ADR-0022 · Closure: OPEN

## F-02 — Migración schema v1→v2 sin ventana de prueba → DLQ masivo
Severity: HIGH
Status: OPEN
Classification: REVALIDADO
Control: ADR-0028 / commit 3706d61c / tests/kafka/test_schemas_orderbook.py
Evidence:
- 641.073 `book_builder_deserialize_error` journal hasta 17:17:55; 0 después 17:20:05
- DLQ con payloads v1 (`event_version=1`, `side/price/size`, `update_id=0`, 2026-09-07)
- `from_dict` v2: `if version != 2: raise SchemaVersionError`
Traceability:
- Tracking: NOT_TRACED · ADR: ADR-0028 · Implementation: ADR-0028 (draft) · Closure: OPEN

## F-03 — Log de deserialización sin detalle en journal
Severity: MEDIUM
Status: OPEN
Classification: REVALIDADO
Control: observabilidad (loguru) — recovery audit §3/§13
Evidence:
- líneas `book_builder_deserialize_error` sin `error=...`/`ptype=...`/`offset=...` en journal
- impide diagnóstico post-incidente de causa exacta
Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Closure: OPEN

## F-04 — STALE por umbral vs jitter del feed
Severity: MEDIUM
Status: OPEN
Classification: REVALIDADO
Control: book_builder.py check_stale (L270-272) / book_builder_consumer.py L299-311
Evidence:
- 304 `book_builder_stale` tras 17:20 (journal); valores 2013-2288ms vs umbral 2000ms
- mecanismo: libros con `has_snapshot=True` sin update dentro de `stale_ms`
Traceability:
- Tracking: NOT_TRACED · ADR: ADR-0022 · Closure: OPEN

## F-05 — Recreación de orderbook.raw no verificable
Severity: MEDIUM
Status: OPEN
Classification: REVALIDADO
Control: tópicos Kafka (retención 168h compose L541)
Evidence:
- orderbook.raw: 1 partición, from-beginning solo v2; ausencia de v1 09-07 pese a retención 7d
- acción de recreación UNVERIFIED; estado final VERIFIED
Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Closure: OPEN

## F-06 — host.env OCM_* no cumple protocolo L2 → warning malformed_key
Severity: LOW
Status: OPEN
Classification: NUEVO
Control: ocm/config/layers/env_override.py (L2) + deploy/host.env
Evidence:
- journal 09-17 15:50:10: `config_l2_env_override | malformed_key=OCM_HOST_USER/OCM_REPO_ROOT`
- `apply_env_overrides` solo acepta `OCM_SECTION__KEY` (2+ partes); vars de host.env tienen 1 parte
Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Closure: OPEN

## F-07 — SSOT entrypoints roto
Severity: MEDIUM
Status: OPEN
Classification: REVALIDADO
Control: run.sh / pyproject [project.scripts] / systemd
Evidence:
- run.sh modos `ocm|live|paper` sin `streaming` ni `ocm-api`
- systemd streaming `python -m app.cli.streaming_hydra` (no `streaming` script)
Traceability:
- Tracking: NOT_TRACED · ADR: ADR-0005, ADR-0022 · Closure: OPEN

## F-08 — `*_hydra.py` filtra Hydra en naming
Severity: INFO
Status: OPEN
Classification: RECOMENDACIÓN
Control: naming hygiene / BC-51
Evidence:
- `live_hydra/paper_hydra/streaming_hydra` usan `load_appconfig_standalone` (sin decorator); solo `main.py` usa `@hydra.main`
Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Closure: OPEN

## F-09 — CryptofeedOrderBookStream cableado en entrypoint, no en CompositionRoot
Severity: MEDIUM
Status: OPEN
Classification: RECOMENDACIÓN
Control: BC-38/BC-42/BC-07 (Composition Root)
Evidence:
- streaming_hydra.py L212: `CryptofeedOrderBookStream(...on_snapshot=bundle.orderbook.on_snapshot...)`
- composition_root no expone build del stream origen (solo producers y BookBuilderConsumer)
Traceability:
- Tracking: NOT_TRACED · ADR: ADR-0022 · Closure: OPEN

## F-10 — Docstring retention "1h" vs compose 168h — topología/doc divergen
Severity: MEDIUM
Status: OPEN
Classification: CONTRADICCIÓN
Control: shared/kafka/schemas/orderbook.py L25 vs docker-compose.yml L541
Evidence:
- docstring: "orderbook.raw tiene retención de 1h (alta frecuencia)"
- compose: `KAFKA_LOG_RETENTION_HOURS=168` (7d) y comentario "Retención: 7 días"
Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Closure: OPEN

## F-11 — Observabilidad push (pushgateway) no operativa durante el incidente
Severity: LOW
Status: OPEN
Classification: NUEVO
Control: observabilidad / ADR-0022 F2.6c
Evidence:
- docker ps en la investigación: solo ocm_kafka + ocm_zookeeper Up; pushgateway ausente
- streaming_hydra usa PrometheusPusher si `metrics.enabled` (fail-soft)
Traceability:
- Tracking: NOT_TRACED · ADR: ADR-0022 · Closure: OPEN

## F-12 — Divergencia units template↔rendered/instalado (EnvironmentFile .env)
Severity: MEDIUM
Status: OPEN
Classification: NUEVO
Control: deploy/systemd/ (template vs rendered vs instalado)
Evidence:
- rendered==instalado; ambos añaden `EnvironmentFile=<root>/.env` que el template no declara
Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Closure: OPEN

## F-13 — DLQ sin reconciliación/replay (v1 retenidos desde 09-07)
Severity: MEDIUM
Status: OPEN
Classification: REVALIDADO
Control: Topic DLQ / book_builder_consumer._send_to_dlq
Evidence:
- offsets DLQ: p0=217399, p1=217401, p2=216804
- mensajes v1 en DLQ desde 2026-09-07; sin proceso de reproceso/replay documentado
Traceability:
- Tracking: NOT_TRACED · ADR: ADR-0022 · Closure: OPEN

## F-14 — Puerto Kafka inconsistente (9093 vs 9094)
Severity: LOW
Status: OPEN
Classification: NUEVO
Control: H-DEP-06 (deployment audit)
Evidence:
- `.env.example` documenta `KAFKA_HOST_PORT=9094`; `host.env(.example)` y `tests/kafka/conftest.py:28` usan 9093
Traceability:
- Tracking: B-45 · ADR: NOT_TRACED · Closure: OPEN

---

## 15. Arquitectura objetivo (diseño, NO implementado)

Principios target (FASE 15):
- **1. Readiness** (B-59): app con retry-backoff al construir producers/consumer + systemd `ExecStartPre` con espera verificable o runbook ordenado (docker infra → topics → OCM). Ver FASE 3/12.
- **2. Un único Composition Root por proceso** (W-1): extender `CompositionRoot` con `build_ws_stream(exchange, symbols, on_prepared=...)` y cablear el adapter de origen en el CR; el entrypoint solo ensambla (llama) — cierra F-09 y refuerza BC-38.
- **3. Schema evolution policy** (W-3): versión explícita siempre; window dual-read para migraciones breaking o topic relabeling; DLQ con proceso de reconciliación/replay. Ver F-02/F-13.
- **4. Observabilidad del book** (F-04): exportar `last_update_at_ms`/`has_snapshot` por símbolo; umbral de stale configurable vía AppConfig; alerting del gap.
- **5. SSOT entrypoints** (F-07): `run.sh` alineado con `[project.scripts]`; systemd usa script registrado (`streaming`) y no `python -m`.
- **6. Naming hygiene** (F-08): renombrar `*_hydra` → nombres neutros.
- **7. Config sin colisiones** (F-06): prefijo dedicado para env de deployment (`OCM_DEPLOY_*`) o documentar `OCM_HOST_*` como excepción fuera de L2.
- **8. Units systemd como SSOT** (F-12): template declara TODOS los EnvironmentFile; rendered = template + vars.

Modelo conceptual (W-2):

```
 entrypoints (apps/app/cli)
     │  solo parse CLI + llaman
     ▼
 CompositionRoot (por BC) ──► adapters/infra cableados (única decisión de wiring)
     │
     ▼
 config: ocm.config (load_appconfig_standalone / L1-L5)  ← env: env_vars.py (SSOT)
     │
     ▼
 domain/application (framework-agnostic: BookBuilder, use cases)
```

## 16. Plan por fases (planificación; requiere autorización)

| Fase | Objetivo | Acciones | Validación | Riesgo |
|---|---|---|---|---|
| **P-0** | Baseline/reproducibilidad | snapshot del estado (este doc ya lo es) | git status | bajo |
| **P-1** | SSOT entrypoints | alinear run.sh; systemd → script registrado; documentar | `./run.sh streaming --help`; G4/G11 | bajo |
| **P-2** | Readiness B-59 | retry-backoff en app + ExecStartPre o runbook | Kafka apagado→encendido; reboot | medio |
| **P-3** | Schema evolution | política de versión + ventana dual-read + DLQ reconcile tool | replay de DLQ v1; tests | medio |
| **P-4** | Composition Root W-1 | `build_ws_stream` en CR; mover CryptofeedOrderBookStream | lint-imports BC-08/38; tests | medio |
| **P-5** | Config SSOT | prefijo deployment; fix host.env example; retention doc | validación L2; yamllint | bajo |
| **P-6** | Observabilidad book | exportar estado por símbolo; stale configurable | dashboards; alertas | bajo |
| **P-7** | Naming hygiene | renombrar `*_hydra` | grep/import global | bajo |
| **P-8** | Runbook | docs de operación/recuperación (H-DEP-09) | runbook end-to-end | bajo |

## 17. Priorización (racional, sin scores arbitrarios)

| Prioridad | Findings | Rationale |
|---|---|---|
| **P0** | F-01 (readiness) | riesgo operacional demostrado; bloquea recuperación determinista |
| **P0** | F-07 (SSOT entrypoints) | deuda transversal de deployment; barata de cerrar |
| **P1** | F-02/F-13 (schema/DLQ) | evita repetición del incidente sin ventana de migración |
| **P1** | F-03 (log sin detalle) | impide diagnóstico futuro (observabilidad) |
| **P1** | F-09 (wiring stream en CR) | coherencia de Clean Architecture; evita drift |
| **P2** | F-04/F-05/F-06/F-12/F-14 | medium/low, documental/operativo |
| **P3** | F-08/F-11 | hygiene / observabilidad push post-incidente |

## 18. FASE 18 — Auto-auditoría + validador

- `uv run python scripts/audit_validator.py --register docs/audits/AUDIT_OCM_streaming-incident-entrypoints-architecture_2026-09-17.md --report docs/audits/AUDIT_OCM_streaming-incident-entrypoints-architecture_2026-09-17.md --tracking docs/plans/tracking.yaml --adrs docs/architecture/decisions`
  → **PASS** — 14 findings, 25 reglas mecánicas (warnings 1, skipped 0). El warning M25 (ADR-0020) es estado preexistente del repo, ajeno a este documento.
- Reglas de juicio L1-L4 aplicadas: causalidad no afirmada (FASE 5); severidades/impacto contextuales; priorización sin scores numéricos inventados.
- Self-review del documento: secciones M12 presentes (`Executive Summary`, `Matriz de Findings`, `Matriz de Controles`, `Matriz de Decisiones`, `Integridad`); fechas/versiones en §2; comandos canónicos en §2/§18. `VERIFIED`.

**Resultado del validador:**

```
[audit-validator] PASS — 14 findings, 25 reglas mecánicas (warnings 1, skipped 0)
WARN  [M25] ADR-0020: en related_adrs pero ninguna regla lo referencia
```

---

## Matriz de Controles

| Control | Comando canónico | Exit esperado | Resultado real |
|---|---|---|---|
| ARCH_CONTRACTS | `uv run lint-imports --config architecture_linter/importlinter.toml` | 0 | **PASS** |
| ARCH_LINTER | `uv run python -m architecture_linter --root . --json` | 0 | **PASS** |
| DEPENDENCY_AUDIT | `uv run pip-audit .` | 0 | **PASS** |
| YAMLLINT | `uvx yamllint -c .yamllint .` | 0 | **PASS** |
| ENGINEERING_HEALTH | `uv run python scripts/engineering_health_check.py` | 0 | **PASS** |
| GOLDEN (test_golden.py) | `uv run pytest tests/architecture_linter/test_golden.py -q --no-cov` | 0 | **NO_VERIFICADO** |
| B-59 readiness (systemd) | `systemctl status ocm-streaming.service` | active sin crash-loop | **FAIL** |
| BookBuilder stale tuning | `check_stale(now_ms)` / umbral `stale_ms` | 0 STALE sostenido | **PARTIAL** |
| DLQ reconciliación/replay | tool de replay DLQ (ausente) | proceso existente | **FAIL** |
| Observabilidad book | métricas por símbolo (has_snapshot/last_update) | presente | **PARTIAL** |
| SSOT entrypoints | `rg 'streaming' run.sh` vs `[project.scripts]` | idéntico | **FAIL** |
| Units systemd SSOT | `diff template rendered instalado` | idéntico | **FAIL** |
| Config env L2 | `config_l2_env_override` warning | limpio | **FAIL** |

Controles = PASS(5) + FAIL(5) + PARTIAL(2) + NO_VERIFICADO(1) = 13

## Matriz de Decisiones

| D- | Pregunta | Opciones | Consecuencias | Evidencia | Finding |
|---|---|---|---|---|---|
| D-1 | ¿Readiness vía app-backoff o ExecStartPre | app/sistema/ambos | B-59 determinista si app; systemd-only si conservador | F-01 | F-01 |
| D-2 | ¿Ventana dual-read v1/v2 o topic reset controlado | dual-read / one-way migrate | dual-read preserva datos; one-way simpler | F-02 | F-02/F-13 |
| D-3 | ¿Mover stream a CompositionRoot (W-1) | CR-build vs entrypoint-build | coherencia Clean Arch; refactor tests | F-09 | F-09 |
| D-4 | ¿Renombrar *_hydra.py | sí / no (doc) | hygiene vs churn de imports | F-08 | F-08 |
| D-5 | ¿Prefijo de env para deployment | OCM_DEPLOY_* / documentar excepción | limpieza de logs; acoplamiento | F-06 | F-06 |
| D-6 | ¿Configurar stale_ms vía AppConfig | sí / número fijo | preventivo de F-04; SSOT | F-04 | F-04 |
| D-7 | ¿Reprocesar DLQ v1 (reconciliación) | sí / archivado/retention | datos preservados vs limpieza | F-13 | F-13 |

## Integridad

- Estado de Git durante la auditoría: ` M docs/plans/tracking.yaml` + 2 docs previos untracked. **No se modificó código, tests, CI, ADRs, tracking, systemd, Docker, Kafka, topics ni servicios.** Única escritura: este documento.
- Verificación: `git status --short` en §FASE 1 y re-ejecutado al cierre (véase §Resultado del validador) — idéntico salvo este archivo.
- Commits: 0 · Pushes: 0 · Servicios: 0.

## Reducción de la lista de findings por control

- F-01 → B-59 · F-02 → ADR-0028 · F-03 → observabilidad · F-04 → BookBuilder · F-05 → topics/retención · F-06 → env L2 · F-07 → run.sh/pyproject · F-08 → naming · F-09 → CompositionRoot · F-10 → docstring vs compose · F-11 → observabilidad push · F-12 → units systemd · F-13 → DLQ · F-14 → puerto Kafka.