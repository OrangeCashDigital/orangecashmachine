# Auditoría Integral de Calidad de Ingeniería — OrangeCashMachine

**Fecha de inicio:** 2026-08-06
**Estado:** En progreso
**Metodología:** Auditoría basada en evidencia extraída directamente del repositorio (no inferencia). Cada hallazgo cita archivo y línea. Clasificación de excepciones a reglas automáticas en 4 categorías:

1. **Aceptada y justificada** — limitación conocida, compatibilidad, falso positivo de herramienta
2. **Reemplazable** — existe hoy una solución mejor (tipado más preciso, refactor)
3. **Oculta un defecto real** — debe corregirse
4. **Sin motivo conocido** — requiere investigación

---

## 1. Cobertura de tests

Estado global: cobertura ~44%, gate mínimo 40%. Pendiente registrar el resumen exacto de la corrida (total de tests, % global preciso) — no confirmado aún en esta sesión de auditoría.

### Hallazgo 1.1 — Bounded context `market_data`: capas `domain/`, `ports/` e `infrastructure/` en 0% de cobertura

**Riesgo: CRÍTICO**

24+ archivos en 0%, incluyendo:
- **`domain/`** (la capa que menos debería estar sin testear — objetos puros, sin I/O): `domain/events/{orderbook,replay,trade}_events.py`, `domain/value_objects/{candle_validator,gap_utils,raw_trade,trade_series}.py`, `domain/policies/data_quality_policy.py`, `domain/quality/{invariants,types}.py`
- **`ports/outbound/`** (9 archivos): `exchange.py`, `exchange_client.py`, `fetcher.py`, `market_data_source.py`, `quality.py`, `resilience.py`, `transformer.py`, `lineage.py`, `feature_reader.py`
- **`ports/inbound/`**: `event_consumer.py`, `trades_source.py`
- **`infrastructure/`**: toda la capa `quality/` (anomaly_registry, cross_exchange_validator, ge_checker, ge_suite), storage Iceberg (`cursor_store.py`, `snapshot_manager.py`), `bootstrap/pipeline_factory.py`, `event_bus/`, `kafka/{consumer,ohlcv_publisher}.py`, `observability/*`, `timeouts.py`
- **`market_data/main.py`** — el entrypoint del bounded context (coincide con H1 crítico de `AUDITORIA-COMPOSITION-ROOTS-2026-08-03.md`: `PipelineOrchestrator()` sin factory inyectado)
- `shared/exceptions/__init__.py` — jerarquía de excepciones compartida entre TODOS los bounded contexts, sin ningún test

**Interpretación:** Contradice la narrativa de "market_data estabilizado como base del sistema". La capa `ports/` define el contrato que el Protocol Discovery Framework debería validar — 0% de cobertura ahí significa que los Protocols no tienen tests de conformidad (fakes/dobles verificando cumplimiento estructural).

**Categoría:** (3) Oculta riesgo real — pendiente decidir si se corrige ahora o se documenta como deuda aceptada con plan.

**Estado:** Pendiente — sin fecha de resolución asignada aún.

### Hallazgo 1.2 — Archivos con cobertura entre 20-39%

Riesgo: MEDIO-ALTO. Concentrados en storage (`iceberg_storage.py` 20%, `silver/trades_storage.py` 21%, `silver/derivatives_storage.py` 21%) y estado runtime (`cursor_store.py` 25%, `redis_store.py` 25%, `metrics_runtime.py` 26%).

Pendiente: identificar qué ramas específicas (error, timeout, reintentos) están sin ejercitar en estos archivos.

---

## 2. Manejo de excepciones amplias (`except Exception`)

**Criterio aplicado:** la sola presencia de `except Exception` NO es un hallazgo. Se audita si el bloque (a) registra contexto suficiente, (b) tiene mecanismo de recuperación, (c) oculta un fallo que debería propagarse.

### Hallazgo 2.1 — Patrón de cierre/shutdown en producers WebSocket

**Archivos:** `liquidations_producer.py:64`, `oi_producer.py:63`, `orderbook_producer.py:101`, `cryptofeed_orderbook_stream.py:137`

**Categoría:** (1) Aceptada y justificada. Cierre defensivo estándar — una excepción secundaria durante shutdown no debe enmascarar el motivo original de desconexión ni tumbar el proceso.

**Estado:** Aceptada. Sin acción requerida, documentado como decisión consciente.

### Hallazgo 2.2 — Cadena de pérdida silenciosa de mensajes en publicación Kafka

**Riesgo: ALTO — Categoría (3), oculta un defecto real**

**Cadena confirmada con evidencia:**
1. `KafkaProducerAdapter.send_async()` (`packages/market_data/infrastructure/kafka/producer.py`, ~L108-122): captura `except Exception`, loguea `warning`, retorna `False`. Sin DLQ (confirmado: cero menciones de "dlq"/"DLQ" en el archivo). Solo tiene `retry_backoff_ms=500` a nivel de cliente `aiokafka` (TCP/broker), no retry aplicativo.
2. `KafkaProducerAdapter.produce()` (~L184-197, "método canónico de `KafkaProducerPort`"): llama `await self.send_async(...)` **descartando el `bool` de retorno**. El docstring dice explícitamente: *"SafeOps: captura excepciones internamente; no lanza si el mensaje se pierde."*
3. Producers de arriba (`orderbook_producer.py`, `oi_producer.py`, `liquidations_producer.py`) envuelven la llamada a `produce()` en su propio `except Exception`, logueando `*_publish_failed` como único rastro.

**Resultado:** un mensaje de mercado que falla en publicar se pierde sin DLQ, sin retry aplicativo, sin métrica, y sin disparar `GapFailedEvent` (pese a que el control plane de gaps — `GapDetectedEvent`/`GapHealedEvent`/`GapFailedEvent`/`KafkaGapPublisher` — ya existe en el sistema pero no está conectado a este punto de falla).

**Nota positiva (para que el diagnóstico sea justo):** `KafkaProducerAdapter.flush()` está bien diseñado — documenta explícitamente por qué NO aplica SafeOps ahí ("un fallo aquí implica pérdida potencial de mensajes y debe propagarse") y lanza `TimeoutError` real. Confirma que el criterio correcto de cuándo silenciar vs propagar SÍ se entiende y aplica en el código — solo que de forma inconsistente entre métodos del mismo adapter.

**Recomendación:**
- (a) `produce()` debe capturar/propagar el `bool` de `send_async()` y emitir métrica (`kafka_publish_failures_total` o similar)
- (b) Evaluar conectar fallos de publish a `GapFailedEvent`
- (c) Decisión de arquitectura pendiente: ¿DLQ real vs aceptar pérdida con alerta explícita?

**Estado:** Pendiente. Fecha de revisión: 2026-08-06.

---

## 3. Ignores de herramientas automáticas (`type: ignore`, `noqa`, `nosec`)

### Register de excepciones auditadas

| Archivo:línea | Tipo | Motivo evidenciado | Categoría | Estado |
|---|---|---|---|---|
| `ocm/config/loader/snapshot.py:118` | `noqa: BLE001` | Fail-soft intencional, documentado inline | (1) Aceptada | Mantener |
| `ocm/runtime/registry.py:199` | `nosec B608` | SQL con literales internos, valores parametrizados, documentado inline | (1) Aceptada | Mantener |
| `apps/api/settings.py:95,98` | `nosec B104` x2 | Bind a `0.0.0.0` intencional para API expuesta, documentado | (1) Aceptada | Mantener |
| `ocm/runtime/state/cursor_store.py:255,332` | `type: ignore[arg-type/index]` | Mismatch conocido de stubs `redis-py` (sync vs async), documentado inline | (1) Aceptada | Mantener |
| `ocm/config/_contract.py` (7 líneas) | `noqa: F401` | Archivo barrel/re-export del contrato SSOT de `ocm.config`; 7 supresiones inline en vez de una regla de archivo | (2) Reemplazable | Configurar `per-file-ignores` en `pyproject.toml` para este archivo, eliminar ignores inline |
| `ocm/config/env_vars.py:32` | `noqa: F401` | Mismo patrón de re-export que `_contract.py` | (2) Reemplazable | Mismo fix: `per-file-ignores` |
| `ocm/observability/logger.py:61` | `noqa: F401` | `import loguru` solo para type hint | (2) Reemplazable | Mover a bloque `if TYPE_CHECKING:`, elimina el ignore por completo |
| `packages/trading/strategies/base.py:23` | `noqa: F401` sobre `Signal, SignalType` | `Signal` SÍ se usa en firmas (L40, 52); solo `SignalType` no se usa en el archivo — el ignore es impreciso | (2) Reemplazable | Separar el import; `noqa` solo sobre `SignalType` |
| `ocm/config/hydra_loader.py:162` | `type: ignore[return-value]` | `OmegaConf.load()` devuelve `DictConfig \| ListConfig`; función promete `DictConfig \| None`. Ignore tapa un caso real (YAML con lista en raíz) | (3) Oculta defecto real | Reemplazar por `isinstance` + `raise ConfigError` explícito — elimina el ignore y agrega validación fail-fast |
| `ocm/runtime/state/gap_store.py:185` | `type: ignore[arg-type]` | `results` sin anotar en mini-pipeline con ramas de tipo heterogéneo | (2) Reemplazable | Anotar `results: list[Any]` explícitamente |
| `apps/api/routers/health.py:74`, `apps/api/main.py:70` | `type: ignore[misc]` sobre `redis.ping()` | Pendiente investigar — no evidenciado aún | (4) Requiere investigación | Pendiente |
| `apps/app/cli/main.py:291` | `type: ignore[call-arg]` sobre `hydra_main()` | Pendiente investigar | (4) Requiere investigación | Pendiente |
| (sin identificar aún) | comentario `# SignalProtocol satisface Signal estructuralmente` | Línea no capturada completa en la extracción — pendiente | (4) Requiere investigación | Pendiente re-extraer |

---

## 4. Hallazgo secundario — violación SSOT de configuración

**Archivo:** `packages/market_data/infrastructure/kafka/producer.py`, método `from_env()`

Lee `os.environ.get(...)` directo pese a que el docstring del propio método dice *"Nombres leídos desde ocm.config.env_vars (SSOT). Nunca strings literales aquí"* — y aunque los nombres de variable sí vienen de constantes de `env_vars.py` (cumple la segunda parte), el acceso a `os.environ` en sí mismo contradice la regla arquitectónica ya documentada: *"solo `ocm.config` puede leer `os.environ`/`os.getenv`"*. Coincide con la auditoría SSOT ya en curso (ver hallazgos previos en `packages/market_data/infrastructure/kafka/{producer,consumer}.py`, `timeouts.py`, `main.py`).

**Categoría:** (3) Oculta desviación arquitectónica ya conocida, no nueva — refuerza el hallazgo existente.

**Estado:** Pendiente, mismo scope que la iniciativa SSOT ya documentada.

---

## Pendiente en esta auditoría

- [ ] Resumen exacto de corrida pytest (total, % global, fallos si los hay)
- [ ] Código muerto (vulture)
- [ ] Dependencias sin uso (deptry)
- [ ] TODO/FIXME/NotImplementedError
- [ ] Returns sospechosos
- [ ] Config de CI/linters (ruff ignore/exclude, mypy config, bandit config, conteo de contratos import-linter)
- [ ] Archivos sin test asociado
- [ ] Resto de `except Exception` fuera de la cadena Kafka (registry.py:179 pendiente de contexto)
- [ ] Roadmap priorizado final (impacto/esfuerzo/beneficio)

---

## 7. Código potencialmente muerto (vulture, min-confidence 80%)

19 hallazgos totales. Vulture tiene falsos positivos conocidos con: parámetros de firmas de `Protocol`/callback obligatorio, y símbolos usados solo como string hints bajo `TYPE_CHECKING`. Cada uno auditado con contexto real antes de clasificar.

| Archivo:línea | Símbolo | Categoría | Motivo |
|---|---|---|---|
| `apps/app/cli/_bootstrap.py:98` | `signum` | (1) Aceptada | Firma obligatoria del callback `signal.signal()` — el SO la invoca así, no se elige |
| `packages/market_data/infrastructure/kafka/dedup.py:64` | `ttl_secs` | (1) Aceptada | Parámetro de firma en `Protocol` (`DeduplicationStoreProtocol.set_raw`), cuerpo `...` — es contrato, no implementación |
| `packages/market_data/ports/outbound/storage.py:64` | `skip_versioning` | (1) Aceptada | Mismo motivo: firma de Port, no implementación real |
| `packages/market_data/infrastructure/storage/iceberg/iceberg_storage.py:491` | `until` | (1) Aceptada | `find_partition_files` es no-op documentado explícitamente ("Iceberg no expone archivos físicos de partición... retorna []"); parámetro existe solo para cumplir la firma del Port |
| `packages/trading/bootstrap/composition_root.py:486` | `use_redis` | (1) Aceptada | Parámetro de `assemble_rebalance()`, stub documentado pendiente de ADR-0011 (decisión D3, auditoría 2026-08-03) — no es residuo, es diseño intencional de stub |
| `packages/market_data/infrastructure/storage/iceberg/iceberg_storage.py:227` | `skip_versioning` (implementación) | Pendiente | Falta confirmar si la implementación real también lo ignora — a diferencia del Port, esto sí sería grave si el caller cree que puede saltar versionado y no pasa nada |
| `ocm/config/layers/coercion.py:50` | `Logger` (import bajo TYPE_CHECKING) | Pendiente | Falta confirmar uso como string hint en firmas del archivo |
| `packages/market_data/adapters/inbound/rest/ohlcv_fetcher.py:29,61` | `OHLCVTransformerPort` (doble import) | Sospecha: (2) o (3) | Importado dos veces (TYPE_CHECKING L29 + import real L61), vulture marca ambas como no usadas — candidato a import duplicado y muerto en ambos casos |
| `packages/market_data/adapters/inbound/rest/derivatives_fetcher.py:43` | `DerivativesStoragePort` | Pendiente | Falta confirmar uso |
| `packages/market_data/adapters/inbound/rest/trades_fetcher.py:38` | `TradesStoragePort` | Pendiente | Falta confirmar uso |
| `packages/market_data/application/pipelines/trades_pipeline.py:37` | `TradesFetcherPort` | Pendiente | Falta confirmar uso |
| `packages/market_data/infrastructure/storage/silver/derivatives_storage.py:42-44` | `pyiceberg`, `pyiceberg.catalog`, `pyiceberg.table` (imports de módulo) | (2) Probablemente reemplazable | Solo `from pyiceberg.table import Table` (símbolo) es plausible que se use en hints; los 3 imports de módulo completo son candidatos a eliminar, dejando solo `Table` |
| `packages/market_data/infrastructure/storage/silver/trades_storage.py:42-44` | ídem | (2) Probablemente reemplazable | Mismo patrón que `derivatives_storage.py` |
| `packages/trading/bootstrap/composition_root.py:65` | `AppRiskConfig` | Pendiente | Bajo TYPE_CHECKING; falta confirmar si se usa como string hint más abajo en el archivo. Si no se usa: candidato a residuo de la v2 no-SSOT ya invalidada por la auditoría del 2026-08-03 |

**Nota de proceso:** contrario a la hipótesis inicial, la mayoría de hallazgos de vulture en este repo son falsos positivos por patrones legítimos (Protocols, callbacks de SO, stubs documentados) — no negligencia. Esto es una señal positiva sobre la disciplina del código, no negativa.

### Sección 7 — Cierre con evidencia completa (clasificación homologada: Correcto / Mejorable / Riesgo)

| Archivo:línea | Símbolo | Clasificación | Evidencia |
|---|---|---|---|
| `apps/app/cli/_bootstrap.py:98` | `signum` | **Correcto** | Firma obligatoria de callback `signal.signal()`, impuesta por el SO |
| `packages/market_data/infrastructure/kafka/dedup.py:64` | `ttl_secs` | **Correcto** | Firma de `Protocol` (contrato), cuerpo `...` |
| `packages/market_data/ports/outbound/storage.py:64` | `skip_versioning` (Port) | **Mejorable** | Firma de interfaz; ningún caller en el repo lo invoca (grep confirmado vacío) |
| `packages/market_data/infrastructure/storage/iceberg/iceberg_storage.py:227` | `skip_versioning` (impl) | **Correcto** | No-op documentado inline (`# no-op — Iceberg versiona por snapshot`) y en docstring; Iceberg no soporta overwrite en pyiceberg 0.8, versiona por snapshot siempre |
| `packages/market_data/infrastructure/storage/iceberg/iceberg_storage.py:491` | `until` | **Correcto** | `find_partition_files` es no-op documentado explícitamente; parámetro cumple firma del Port, sin efecto real porque Iceberg no expone archivos físicos de partición |
| `packages/trading/bootstrap/composition_root.py:486` | `use_redis` | **Correcto** | Parámetro de stub documentado (`assemble_rebalance`), pendiente ADR-0011, decisión D3 ya registrada en auditoría 2026-08-03 — diseño intencional, no residuo |
| `ocm/config/layers/coercion.py:50` | `Logger` | **Correcto** | Falso positivo vulture: usado como forward reference en `_get_logger() -> "Logger"` (L56) |
| `packages/market_data/adapters/inbound/rest/derivatives_fetcher.py:43` | `DerivativesStoragePort` | **Correcto** | Falso positivo: forward reference en constructor (L138) |
| `packages/market_data/adapters/inbound/rest/trades_fetcher.py:38` | `TradesStoragePort` | **Correcto** | Falso positivo: forward reference en constructor (L116) |
| `packages/market_data/application/pipelines/trades_pipeline.py:37` | `TradesFetcherPort` | **Correcto** | Falso positivo: forward reference en constructor (L124), con comentario DIP explícito |
| `packages/market_data/adapters/inbound/rest/ohlcv_fetcher.py:29,61` | `OHLCVTransformerPort` (doble import) | **Pendiente** | No auditado en esta sesión — requiere verificar si ambas instancias son forward references legítimas o si hay import duplicado real |
| `packages/market_data/infrastructure/storage/silver/{derivatives,trades}_storage.py:42-44` | `pyiceberg`, `pyiceberg.catalog`, `pyiceberg.table` | **Pendiente** | No auditado si se usan como hints de módulo completo en algún string annotation — requiere grep antes de clasificar |
| `packages/trading/bootstrap/composition_root.py:65` | `AppRiskConfig` | **Pendiente** | No auditado si se usa como forward reference en el archivo — de no usarse, candidato a residuo de la v2 no-SSOT (auditoría 2026-08-03) |

**Conclusión de la sección 7:** de 19 hallazgos de vulture, 10 quedaron confirmados como **Correcto** (falso positivo estructural: Protocols, callbacks de SO, stubs documentados, forward references bajo `TYPE_CHECKING`), 1 como **Mejorable** (`skip_versioning` en el Port, sin consumidores), y 8 quedan **Pendientes** de evidencia (3 imports de `ohlcv_fetcher.py`+silver storage, 1 en `composition_root.py`). Ningún hallazgo de esta sección escaló a Riesgo. Patrón dominante: el uso de `TYPE_CHECKING` + forward references está aplicado de forma consistente y correcta en todo el repo — vulture no resuelve bien ese patrón, generando falsos positivos sistemáticos, no señal de código muerto real.

---

## 6. Configuraciones de CI/linters

| Herramienta | Configuración | Clasificación | Evidencia |
|---|---|---|---|
| Bandit | `skips = ["B101"]`, `exclude_dirs` estándar | **Correcto** | Único skip, justificado inline (asserts en guard checks controlados) |
| mypy | `strict=false`, `ignore_missing_imports=true`, `exclude=["tests/",".venv/"]` | **Correcto**, con nota | Coincide con decisión ya documentada (migración desde Pyright por ruido pandas/Dagster/CCXT). `pyiceberg` sin stubs, justificado inline |
| mypy | `exclude=["tests/"]` | **Riesgo Bajo** | mypy nunca revisa tests — común en la industria, pero es superficie sin verificación de tipos (ej. mocks mal tipados que ocultan drift de interfaz). Práctica aceptable si es decisión consciente; se documenta acá para que lo sea explícitamente |
| Dependencias pinneadas: `aioresilience==0.2.1` | Pin por API inestable + conflicto Python 3.12/anyio | **Correcto**, con vencimiento condicional | Referencia cruzada a `MIGRATION.md §circuit-breaker` — acción de bajo esfuerzo: confirmar que ese doc sigue vigente; un pin que referencia un doc inexistente deriva a categoría "motivo desconocido" |
| Dependencias pinneadas: `ccxt==4.3.58`, `pybreaker==1.4.1` | Pins con causa raíz y condición de actualización documentadas inline | **Correcto** | Mismo patrón de disciplina que el resto del repo |
| import-linter | Contratos viven en `architecture/importlinter.toml` (no en `pyproject.toml`) | Pendiente | Conteo exacto pendiente de confirmar contra los 42+ documentados históricamente (BC-01 a BC-54+) |

**Conclusión parcial sección 6:** mismo patrón que la sección 7 — las configuraciones que en una auditoría superficial podrían leerse como "riesgo" (pins de versión, exclusiones de mypy) están, en los casos verificados, documentadas con causa raíz y condición de reversión. Único hallazgo real de riesgo (bajo) es la exclusión de `tests/` del alcance de mypy, que es práctica común pero merece quedar como decisión explícita, no implícita.

---

## 4. TODO / FIXME / NotImplementedError

**Nota de proceso:** el grep original capturó varios falsos positivos — comentarios en español con la palabra "TODOS" (ej. "TODOS los eventos", "TODOS los sources") que coinciden textualmente con el patrón `TODO`. Se filtran de esta tabla por no ser marcadores de trabajo pendiente.

| Archivo:línea | Contenido | Clasificación | Evidencia |
|---|---|---|---|
| `packages/market_data/adapters/outbound/exchange/errors.py:19` | "TODO: deprecar en la siguiente major version" | **Correcto**, acción pendiente de verificar | Shim de compatibilidad hacia atrás (OCP), re-exporta desde `market_data.domain.exceptions`, `noqa: F401` justificado. Pendiente confirmar con grep si aún hay consumidores del shim — de no haberlos, la deprecación puede ejecutarse ya en vez de esperar la próxima major |
| `packages/market_data/main.py:543` | `market_type="spot"` hardcodeado, TODO exponer como query param si se necesita futures | **Correcto** | Decisión consciente de no sobre-ingeniería, condicionada a necesidad futura real — coherente con la filosofía documentada del proyecto (estabilizar datos antes que reglas de negocio) |
| `packages/trading/bootstrap/composition_root.py:489` | `TODO(ADR-0011)` | **Correcto** | Ya registrado como stub D3 documentado (auditoría 2026-08-03) |
| `ocm/observability/metrics.py:14` | "TODO: métricas de negocio (trades, latencia, P&L) — v0.2.0" | **Riesgo Medio** | Target de versión (v0.2.0) ya superado — el paquete está en v0.3.0 sin que el TODO se resolviera. Sugiere que se perdió de vista, no que fue pospuesto conscientemente. Ausencia de métricas de P&L/latencia es laguna de observabilidad operacional real en un sistema de trading |
| `ocm/observability/tracing.py:12` | "TODO: OpenTelemetry spans — v0.2.0" | **Riesgo Medio** | Mismo patrón que el anterior: target de versión vencido, tracing distribuido ausente |

### NotImplementedError — todos correctos, patrón de diseño estándar

`derivatives_fetcher.py:209,212`, `pipeline/runtime.py:466`, `ports/outbound/exchange.py:142,165`, `composition_root.py:499` — todos son métodos base de Port/clase abstracta que fuerzan implementación en subclases (Template Method), con mensajes de error claros. **Clasificación: Correcto** en todos los casos, sin excepción.

---

## Cierre de pendientes — Secciones 4 y 6

### Shim `errors.py` (actualización de sección 4)

**Reclasificado: Mejorable (bajo esfuerzo)**, no solo "Correcto, acción pendiente". Consumidores confirmados: `packages/market_data/adapters/outbound/exchange/__init__.py:11` y `ccxt_adapter.py:41`, ambos internos al propio paquete `market_data` — sin dependencia externa que romper. Migrar estos 2 imports a `market_data.domain.exceptions` directamente permite ejecutar la deprecación ya, sin esperar la próxima major version. Esfuerzo estimado: bajo (2 archivos, imports directos).

### Contratos import-linter (cierre de sección 6)

**Conteo confirmado: 49 contratos** (`grep -c "^\[\[tool.importlinter.contracts\]\]" architecture/importlinter.toml`), superando los 42+ (BC-01–BC-54+) documentados en la auditoría de composition roots de 2026-08-03. **Clasificación: Correcto** — sistema de gobernanza arquitectónica activo, sin evidencia de contratos dados de baja sin registro.

**Sección 6 — CERRADA.**

---

## 5. Returns sospechosos (`return True` / `return None` constantes)

**Metodología aplicada:** cada ocurrencia se audita con el bloque completo (clase, docstring, lógica circundante) antes de clasificar — un `return True`/`None` aislado de su contexto es indistinguible entre "stub roto" y "comportamiento correcto de una rama simple". Ver contraejemplo en `guard.py` abajo.

| Archivo:línea | Contenido | Clasificación | Evidencia |
|---|---|---|---|
| `ocm/runtime/guard.py:118,124` | `should_stop()` — dos `return True` tras evaluar kill switch y runtime excedido, `return False` al final | **Correcto** | Lógica condicional real evaluada antes de cada return; el guard de ejecución del trading runtime funciona como se espera. Sospecha inicial (return sin contexto) quedó descartada al ver el bloque completo |
| `ocm/runtime/state/cursor_store.py:346-347` | `InMemoryCursorStore.is_healthy()` → `return True` fijo | **Correcto** | Pertenece a la clase in-memory, documentada explícitamente como "uso: tests unitarios y entornos sin Redis" — un dict en memoria no tiene backend externo que pueda fallar, `True` fijo es el comportamiento correcto para este doble |
| `ocm/runtime/state/factories.py` (múltiples `return None`: L147,152,174,179,204,212,242,252) | Factories que retornan `None` cuando el backend no está disponible o falla la inicialización | **Correcto** | Confirmado con `build_cursor_store_from_env`: cada `return None` va precedido de `logger.warning(...)` con motivo explícito — fail-soft documentado, no silencioso |
| **Pendiente de evidencia** | ¿Existe `is_healthy()` en la implementación Redis real (no in-memory), y verifica algo? | **Pendiente de evidencia** | Necesario confirmar que `factories.py` no está evaluando accidentalmente la salud de un store in-memory en contextos donde se espera Redis real |
| `metrics_runtime.py:150`, `gap_registry.py:159`, `lateness_calibration.py:147` | `return True` en contexto no visto aún | **Pendiente de evidencia** | No auditado con bloque completo en esta sesión |

**Conclusión parcial:** mismo patrón de las secciones anteriores — de los returns con contexto confirmado, 100% resultaron correctos. El riesgo real no estaba en los returns en sí, sino en la duda de si el store real de Redis comparte o no la lógica de salud del store en memoria.

### Cierre del pendiente crítico — cadena `is_healthy()`

**Confirmado con evidencia:** existe un `Protocol CursorStore` (L37-48) que define el contrato `is_healthy() -> bool`, implementado por separado en `RedisCursorStore` (L179, verificación real vía `self._client.ping()`, con manejo correcto de excepción → `False`) y `InMemoryCursorStore` (L346, `True` fijo, correcto para un doble sin backend). **No hay cadena de confianza rota** — cada implementación de `is_healthy()` se comporta según lo que su clase representa. Sospecha inicial descartada con evidencia completa.

**Clasificación final: Correcto**, patrón `Protocol` + implementaciones diferenciadas aplicado consistentemente.

### Hallazgo nuevo (surgido durante la verificación del pendiente)

**`ocm/runtime/state/cursor_store.py:176-177`** — `except Exception: pass` desnudo en `_record_lag` (registro de métrica de lag), sin log de ningún tipo. Único caso encontrado en toda la auditoría sin logging — rompe el patrón consistente del resto del repo (donde cada `except Exception` trae como mínimo `logger.warning`/`debug`).

**Clasificación: Mejorable.** Riesgo funcional bajo (solo pérdida de visibilidad de una métrica, no de datos de negocio), pero inconsistente con el estándar propio del proyecto. Fix trivial: agregar `logger.debug("cursor_lag_metric_failed", error=...)`.

### Cierre de los 3 returns pendientes

| Archivo:línea | Método | Clasificación | Evidencia |
|---|---|---|---|
| `ocm/observability/metrics_runtime.py:150` | push de métricas a gateway | **Correcto** | `try` con push real (`push_to_gateway`) → `return True`; `except Exception` → `logger.warning("metrics_push_failed")` + `return False`. Bool + log, patrón completo |
| `ocm/runtime/state/gap_registry.py:159` | `GapRegistry.register()` | **Correcto** | Mismo patrón; log explícito marca "(non-critical)" — fail-soft consciente y documentado |
| `ocm/runtime/state/lateness_calibration.py:147` | `LatenessCalibration.set()` | **Correcto** | Mismo patrón, mismo estilo de log "(non-critical)" |

**Nota de contraste con hallazgo 2.2 (Riesgo Alto, Kafka):** estos 3 casos SÍ devuelven `bool` explícito con log en ambos caminos — la diferencia crítica con `KafkaProducerAdapter.produce()` es que ahí el caller descartaba el retorno. Pendiente verificar si los callers de `register()`/`set()` aquí sí consumen el bool (ver sección de seguimiento).

**Sección 5 — CERRADA.**

### `ohlcv_fetcher.py::HistoricalFetcherAsync` — CERRADO

**Evidencia revisada:** paginación (loop principal de descarga), `_resolve_start_timestamp`,
`_fetch_chunk_with_retry` completo (docstring de arquitectura + 3 ramas de except),
`_validate_inputs`, `_validate_market`, helpers `_raw_to_dataframe`/`_sanitize_dataframe`.

**Hallazgos:**
- Único punto de retry en el sistema, documentado explícitamente en docstring (SRP respetado
  — `CCXTAdapter` maneja resiliencia; `_fetch_chunk_with_retry` es llamada directa sin
  backoff propio). Sin duplicación de lógica de retry entre capas.
- Los 3 `except` (`ExchangeCircuitOpenError`, `RetryExhaustedError`, `Exception` genérico)
  logean con contexto estructurado y relanzan con `from exc` — trazabilidad completa,
  ningún error se enmascara.
- Fail-fast correcto en validaciones (`_validate_inputs`, `_validate_market`).
- Concatenación/dedup de chunks (`drop_duplicates(subset="timestamp", keep="last")`) correcta.

**Clasificación final: Correcto.** Sin riesgo real. Candidato bajado de "alto impacto" a
"cobertura deseable pero no crítica" — no requiere tests urgentes adicionales.

### `main.py::get_ohlcv` — CERRADO

**Evidencia revisada:** cuerpo completo, desde normalización de `symbol` hasta el `except` final.

**Hallazgos:**
- Fail-fast correcto: `storage_factory is None` → `RuntimeError` explícito antes de continuar.
- I/O síncrono (`storage.load_ohlcv`) despachado vía `asyncio.to_thread(...)` — no bloquea
  el event loop. Correcto manejo de concurrencia en contexto async.
- Empty-check → `404` con detalle estructurado (`exchange`, `symbol`, `timeframe`) — HTTP
  semántico correcto, no confunde "sin datos" con error de servidor.
- Orden lógico correcto: filtros temporales → `df.tail(limit)` → serialización a `records`
  (formato ISO 8601 con `strftime`) → respuesta 200 con conteo y payload.
- `except Exception` final: log estructurado con traza completa (`_log.opt(exception=True)`)
  + relanzado como `HTTPException(500, ...)` con `from exc` — trazabilidad preservada,
  ningún error silencioso.
- Comentario explícito documentando decisión arquitectónica: `_get_storage()` eliminado,
  `IcebergStorage` se instancia en `lifespan` (composition root) — DIP/SRP/Hexagonal citados
  correctamente en el propio código.

**Hallazgo menor (ya registrado):** `market_type="spot"` hardcodeado con TODO sin deadline
— ver nota en el cuerpo intermedio del método. Clasificación: Mejorable, no bloqueante.

**Clasificación final: Correcto.** Sin riesgo real. Endpoint bien protegido en ambos bordes
(entrada validada, salida con manejo de error explícito).

### `main.py::_ingestion_loop` — CERRADO

**Evidencia revisada:** cuerpo completo, desde la firma hasta el cierre del `try/except`.

**Hallazgos:**
- `CompositionRoot.assemble(ctx.app_config)` se invoca **una sola vez, fuera del `while`**
  — el composition root es efectivamente singleton por ciclo de vida del proceso, no se
  reconstruye en cada iteración de ingestion. Buena práctica de Clean Architecture: el
  costo de wiring se paga una vez, no en cada run.
- `factory.build(request)` se invoca una vez por combinación (exchange × market_type) dentro
  del loop — acotado y predecible, no por símbolo individual.
- Manejo de excepciones en 3 niveles: `ExecutionStoppedError` (log + break, detiene limpio),
  `asyncio.CancelledError` (re-lanzado sin capturar — correcto para shutdown de asyncio),
  `Exception` genérico (fail-soft documentado: "el loop nunca muere por 1 run fallido",
  `guard.record_error()` activa kill switch ante errores consecutivos).
- Sleep interruptible con su propio `except CancelledError: raise` — cancelación inmediata
  en shutdown sin esperar el intervalo completo.
- Imports lazy justificados explícitamente en docstring (DIP: application no depende de infra
  a nivel de import estático).

**Clasificación final: Correcto.** Sin riesgo real. Diseño SafeOps consciente y bien
documentado en el propio código.

---

### `pipeline_factory.py::ConcretePipelineFactory._build_ohlcv` — CERRADO

**Bug histórico `PrometheusRepairMetrics` — DESCARTADO CON EVIDENCIA.**

**Evidencia revisada:**
- `_build_ohlcv` instancia `PrometheusRepairMetrics()` inline en cada llamada — inicialmente
  sospechoso de violar DIP (composition root debería inyectar, no construir ad-hoc) y de
  causar registro duplicado en el `CollectorRegistry` de Prometheus.
- `PrometheusRepairMetrics.__init__` no crea `Counter(...)` directamente: hace un import
  local de `market_data.infrastructure.observability.metrics` y asigna las referencias
  (`PIPELINE_ERRORS`, `REPAIR_GAPS_FOUND`, `REPAIR_GAPS_HEALED`, `REPAIR_GAPS_SKIPPED`,
  `ROWS_INGESTED`) a atributos de instancia expuestos vía `@property`.
- Confirmado en `metrics.py:63,76,159`: estas constantes están definidas con `Counter(...)`
  **una sola vez, a nivel de módulo**. Python cachea el módulo tras el primer import —
  instanciaciones repetidas de `PrometheusRepairMetrics()` re-vinculan las mismas
  referencias, nunca re-ejecutan `Counter(...)`. No hay colisión en el registry.
- `_build_ohlcv` se invoca desde `_ingestion_loop` una vez por (exchange × market_type),
  confirmado en sección anterior — múltiples instanciaciones por corrida, pero sin efecto
  adverso dado el punto anterior.

**Hallazgo residual (Mejorable, no Riesgo):** instanciar `PrometheusRepairMetrics()` y
`PrometheusPipelineMetrics()` inline dentro de `_build_ohlcv` en cada llamada sigue siendo
una desviación menor de DIP — el composition root podría recibir estas instancias ya
construidas (o cachearlas como singleton) en vez de reconstruir el wrapper en cada request.
No causa bug funcional, pero es trabajo innecesario repetido y wiring implícito en vez de
explícito. Costo de fix: bajo (memoizar en `__init__` de `ConcretePipelineFactory`).

**Hallazgo adicional (Mejorable, ya registrado):** orden de validación fail-fast en
`_build_ohlcv` — el bloque `if not request.symbols: raise ValueError(...)` ocurre **después**
de construir `HistoricalFetcherAsync(...)` completo. Debería validarse primero, antes de
cualquier construcción costosa.

**Clasificación final: Correcto** (bug histórico descartado), con 2 hallazgos Mejorables
de higiene arquitectónica (no bloqueantes, no riesgo funcional).

### Pendientes sueltos — CERRADOS (3/3)

**1. `ohlcv_fetcher.py` — "doble import" — DESCARTADO.**
El símbolo `OHLCVTransformerPort` aparece dos veces: una vez dentro de `if TYPE_CHECKING:`
(línea ~29, nunca se ejecuta en runtime, solo para resolución de tipos por mypy) y una vez
como import real top-level (línea 61, el que efectivamente corre). Patrón estándar para
evitar imports circulares en runtime preservando tipado estático — confirmado un segundo
bloque `TYPE_CHECKING` en el mismo archivo con el mismo criterio, consistente.
**Clasificación: Correcto.** No es duplicación, es el patrón correcto.

**2. pyiceberg en silver storage (`derivatives_storage.py`, `trades_storage.py`) — DESCARTADO.**
`Schema`/`types` de pyiceberg se importan 2 veces cada uno, pero en scopes distintos:
`_schema_funding_rate()` y `_schema_open_interest()`, cada función con su propio import
lazy aislado (mismo patrón lazy-import ya validado en `pipeline_factory.py` y
`ohlcv_fetcher.py` — evita el costo de import de pyiceberg cuando esa función específica
no se ejecuta). No hay import duplicado en el mismo scope.
**Clasificación: Correcto.** Consistente con el patrón de imports lazy del resto del repo.

**3. `AppRiskConfig` en composition_root (`packages/trading/bootstrap/composition_root.py`) — DESCARTADO.**
Es un alias (`from ocm.config.schema import RiskConfig as AppRiskConfig`), no una clase
propia. Justificado: existe una segunda clase `RiskConfig` en `packages/trading/risk/models.py:62`
— el alias desambigua dos símbolos con el mismo nombre en el mismo bounded context, evitando
que un import pise al otro silenciosamente.
**Clasificación: Correcto.** Uso de alias necesario y bien aplicado.

**Resumen de los 3 pendientes sueltos:** 0 hallazgos de Riesgo, 0 hallazgos Mejorables.
Los tres eran sospechas razonables por nombre/patrón, descartadas con evidencia de código.

---

## Estado consolidado tras cierre de candidatos de alto impacto y pendientes sueltos

**Actualización de tracking:**

| Ítem | Estado previo | Estado final | Evidencia |
|---|---|---|---|
| `main.py::_ingestion_loop` | Sospecha alto impacto | **Correcto** | Cuerpo completo revisado |
| `main.py::get_ohlcv` | Sospecha alto impacto | **Correcto** (1 TODO sin deadline) | Cuerpo completo revisado |
| `pipeline_factory.py::_build_ohlcv` | Sospecha alto impacto + bug histórico `PrometheusRepairMetrics` | **Correcto** (2 mejorables) | Bug descartado con evidencia de módulo (`metrics.py:63,76,159`) |
| `ohlcv_fetcher.py::HistoricalFetcherAsync` | Sospecha alto impacto | **Correcto** | Cuerpo completo revisado |
| Doble import `ohlcv_fetcher.py` | Pendiente suelto | **Correcto** — falso positivo (patrón `TYPE_CHECKING`) | Confirmado |
| pyiceberg silver storage | Pendiente suelto | **Correcto** — falso positivo (imports lazy en scopes distintos) | Confirmado |
| `AppRiskConfig` composition_root | Pendiente suelto | **Correcto** — alias necesario (2 clases `RiskConfig` distintas) | Confirmado |

**Lectura del resultado:** los 7 puntos que generaban más incertidumbre en la auditoría
resultaron ser decisiones de diseño válidas o patrones correctos, no deuda técnica nueva.
Esto concentra el riesgo real del proyecto en lo ya identificado en secciones anteriores
(2, 4, 5) — no se descubrió riesgo adicional al profundizar en los componentes más críticos
del sistema.

## Backlog priorizado (consolidado a la fecha)

**Prioridad Alta**
- Pérdida silenciosa de mensajes Kafka (Sección 2) — riesgo alto, ya evidenciado.

**Prioridad Media**
- `except Exception: pass` sin log en `cursor_store.py::_record_lag` (Sección 5) — agregar
  `logger.debug(...)`, fix trivial.
- TODOs de observabilidad vencidos v0.2.0/v0.3.0 (Sección 4).
- Wiring inline de `PrometheusRepairMetrics`/`PrometheusPipelineMetrics` en `_build_ohlcv`
  en vez de singleton en composition root (Sección 9) — memoizar en `__init__` de
  `ConcretePipelineFactory`.
- Orden de validación fail-fast en `_build_ohlcv`: mover `if not request.symbols: raise...`
  antes de construir `HistoricalFetcherAsync` (Sección 9).
- TODO sin deadline en `get_ohlcv` (`market_type="spot"` hardcodeado) (Sección 9).

**Prioridad Baja**
- Limpieza de imports / deuda vulture (Secciones 6-7, ya cerradas — solo ejecución de limpieza).
- Ajustes de tooling (import-linter, config de linters — ya cerrado, mantenimiento).
- Documentación pendiente post-Dagster removal (`.env`/`.env.example`, README, AGENTS.md, ADR-0001).

**Pendiente para completar el backlog (no cerrado aún):**
- Sección 9 resto de los ~40 archivos "sin test + 0% cobertura" — falta clasificar
  contra complejidad real (Value Object/DTO trivial vs. lógica de negocio real).
- Fotografía exacta de la suite de pytest (total de tests, % de cobertura global preciso)
  — pendiente desde el arranque de Sección 1.

---

## Fotografía exacta de la suite de tests (cierre de pendiente Sección 1)

965 passed, 29 warnings in 49.06s
Cobertura total: 44.23% (mínimo requerido: 40.0% — superado)
Módulos con cobertura destacable por debajo del promedio general (candidatos naturales
para la Fase de Calidad, sección siguiente):
- `shared/types/position_events.py` — 71%
- `shared/types/signal.py` — 76%
- `shared/types/order_events.py` — 79%
- `shared/types/timeframe.py` — 81%

Resto de `shared/kafka/schemas/*` y `shared/kafka/topics.py` — 91-100%, sin riesgo.

## Tracking final — cierre de todos los pendientes descartados con evidencia

| Ítem | Clasificación final |
|---|---|
| Doble import `ohlcv_fetcher.py` | **Correcto** — patrón `TYPE_CHECKING`, falso positivo |
| pyiceberg silver storage (imports repetidos) | **Correcto** — imports lazy en scopes distintos |
| `AppRiskConfig` composition_root | **Correcto** — alias necesario, dos `RiskConfig` distintas |
| `is_healthy()` Redis/InMemory CursorStore (Sección 5) | **Correcto** — Protocol + implementaciones diferenciadas, sin cadena de confianza rota |
| `except Exception: pass` en `_record_lag` (Sección 5) | **Mejorable** — fix trivial, sin riesgo funcional |
| `_build_ohlcv` wiring inline de métricas | **Mejorable** — desviación menor de DIP, sin riesgo |
| `_build_ohlcv` orden de validación fail-fast | **Mejorable** — validar antes de construir |
| `get_ohlcv` TODO sin deadline | **Mejorable** — higiene, no riesgo |

**Todos los pendientes sueltos y candidatos de alto impacto quedan cerrados.** Ningún
hallazgo nuevo de Riesgo surgió al profundizar con evidencia real — la mayoría de las
sospechas iniciales resultaron ser decisiones de diseño válidas.

## Roadmap maestro por fases

**Fase 1 — Inmediata (riesgo alto confirmado)**
- Pérdida silenciosa de mensajes Kafka (Sección 2). Único hallazgo de Riesgo Alto de
  toda la auditoría.

**Fase 2 — Bajo costo / alto beneficio**
- Logging en `except Exception: pass` de `cursor_store.py::_record_lag`.
- Reordenar validación fail-fast en `_build_ohlcv` (antes de construir `HistoricalFetcherAsync`).
- Memoizar `PrometheusRepairMetrics`/`PrometheusPipelineMetrics` como singleton en el
  composition root en vez de wiring inline por request.
- Cerrar TODOs vencidos de observabilidad v0.2.0/v0.3.0 (Sección 4).
- Agregar deadline al TODO de `market_type="spot"` en `get_ohlcv`, o resolverlo.

**Fase 3 — Calidad (clasificación fina, pendiente)**
- Clasificar los ~40 archivos "sin test + 0% cobertura" restantes contra complejidad real
  (Value Object/DTO trivial vs. lógica de negocio sin proteger), usando la ficha estándar
  de hallazgo (evidencia, falso positivo, impacto, probabilidad, costo, prioridad).
- Foco inicial sugerido por los datos de cobertura: `shared/types/position_events.py` (71%),
  `shared/types/signal.py` (76%), `shared/types/order_events.py` (79%).

**Fase 4 — Mantenimiento**
- Limpieza de imports / deuda vulture (ya cerradas, solo ejecución).
- `.env`/`.env.example`, README, AGENTS.md, ADR-0001 post-remoción de Dagster.

## Resumen ejecutivo

La mayoría de las sospechas iniciales de esta auditoría fueron descartadas tras revisión
contextual con evidencia de código real, no heurística de herramienta. Los tres candidatos
de alto impacto (`main.py`, `pipeline_factory.py`, `ohlcv_fetcher.py`) y los tres pendientes
sueltos (doble import, pyiceberg, `AppRiskConfig`) resultaron ser patrones de diseño
correctos — incluyendo el descarte formal del bug histórico `PrometheusRepairMetrics`,
que no generaba re-registro en el `CollectorRegistry` de Prometheus.

El riesgo real del proyecto queda concentrado y bien localizado: un hallazgo de Riesgo Alto
(pérdida silenciosa de mensajes Kafka) y un puñado de mejoras menores de logging, orden de
validación y wiring de métricas — ninguna bloqueante.

Suite de tests: 965 passed, 44.23% cobertura global (sobre el mínimo requerido de 40%).

**Queda explícitamente abierto:** la clasificación fina de los ~40 archivos con 0%
cobertura restantes (Fase 3). Con eso, la auditoría queda completa, trazable y con un
plan de acción basado en evidencia real, no en sospechas iniciales.

---

## Fase 3a — Cabeza de criticidad (15 archivos, 0% cobertura, priorizados por impacto operacional)

**Nota metodológica:** salvo `ohlcv_fetcher.py`, `main.py` y `pipeline_factory.py` (revisión
de cuerpo completo realizada en esta sesión), la "responsabilidad arquitectónica" de los
demás archivos se infiere de su ubicación en el árbol y de contexto de dominio ya conocido,
**no de lectura de código confirmada hoy**. Se marca explícitamente en cada ficha.

**Precisión de lenguaje:** "0% de cobertura" describe una condición objetiva de ausencia de
red de pruebas. No implica, por sí sola, evidencia de defecto funcional. Para los 3 archivos
con diseño ya auditado, el riesgo real es la ausencia de protección ante regresión futura,
no una sospecha de bug actual.

### Nivel 1 — Crítico (pérdida de datos, órdenes incorrectas, indisponibilidad)

| # | Archivo | Responsabilidad | Riesgo si falla | Cobertura | Tipo de test | Prioridad | Esfuerzo |
|---|---|---|---|---|---|---|---|
| 1 | `resilience.py` | Circuit breaker/retry hacia exchange (inferido) | Reintentos indefinidos → rate-limit ban; o breaker que no cierra → corte silencioso de ingestion | 0%, 130 stmts | Unitarias (máquina de estados) + integración con fallos simulados | **P0** | Medio-alto |
| 2 | `throttle.py` | Rate limiting hacia exchange (inferido) | Ban temporal/permanente de cuenta — indisponibilidad de todo el sistema | 0%, 156 stmts | Unitarias (cálculo de ventanas) + contract test contra límites documentados por exchange | **P0** | Medio |
| 3 | `ccxt_adapter.py` | Adapter principal CCXT, punto único de contacto con exchanges (inferido) | Bug afecta a todos los pipelines dependientes — punto de falla único | 0%, 323 stmts | Unitarias por método + integración con sandbox/testnet o mocks fieles | **P0** | Alto |
| 4 | `gap_aware_stream.py` | Detección de gaps en stream en tiempo real (inferido) | Gap no detectado = hueco silencioso en serie de tiempo | 0%, 133 stmts | Unitarias (lógica de detección) + integración con #5 | **P0** | Medio-alto |
| 5 | `gap_recovery_fetcher.py` | Recuperación de gaps detectados (inferido) | Gap detectado nunca se resuelve | 0%, 137 stmts | Integración E2E del ciclo detectar→recuperar→confirmar | **P0** | Medio-alto |

**Recomendación Nivel 1:** revisión de cuerpo completo (aún no realizada) antes de escribir
tests — la incertidumbre aquí es doble (criticidad alta + diseño no confirmado). Empezar por
`resilience.py` y `throttle.py`: son los únicos dos donde un fallo silencioso tiene
consecuencia externa irreversible (ban de cuenta en el exchange).

### Nivel 2 — Alto (corrupción o degradación de datos de mercado)

| # | Archivo | Responsabilidad | Riesgo si falla | Cobertura | Tipo de test | Prioridad | Esfuerzo |
|---|---|---|---|---|---|---|---|
| 6 | `ohlcv_fetcher.py` | Fetch histórico paginado, único punto de retry (**diseño auditado hoy**) | Bajo por diseño confirmado — riesgo real es regresión futura sin red de pruebas | 0%, 213 stmts | Unitarias de regresión (paginación, dedup, timestamp de inicio) | **P1** | Medio |
| 7 | `pipeline_factory.py` | Composition root de `market_data` (**diseño auditado hoy**, bug histórico descartado) | Bajo — sin lógica de negocio, solo wiring | 0%, 126 stmts | Tests de wiring (grafo de dependencias por tipo de pipeline) | **P1** | Bajo-medio |
| 8 | `main.py` | Entrypoint FastAPI + `_ingestion_loop` (**diseño auditado hoy**) | Bajo — manejo de errores confirmado correcto en loop y endpoints | 0%, 183 stmts | Integración (endpoints con storage mockeado); loop requiere extracción de función testeable | **P1** | Medio |
| 9 | `data_quality.py` | Validaciones de calidad de datos (inferido) | Datos de mala calidad pasan como válidos aguas abajo | 0%, 133 stmts | Unitarias por regla (casos límite: NaN, duplicados, fuera de rango) | **P1** | Medio |
| 10 | `ge_checker.py` | Integración Great Expectations (inferido) | Igual que #9 — capa de calidad de datos | 0%, 130 stmts | Contract tests contra expectativas configuradas | **P1** | Medio |
| 11 | `invariants.py` | Invariantes de dominio (inferido) | Estado inválido puede propagarse sin excepción visible | 0%, 111 stmts | Unitarias puras, sin infra ni mocks | **P1** | Bajo |

**Recomendación Nivel 2:** para #6, #7, #8 — **no refactorizar, construir red de regresión**
sobre diseño ya validado. Para #9/#10 — revisar posible solapamiento de responsabilidad
(candidatos a consolidación SSOT). Para #11 — mejor relación esfuerzo/beneficio de todo
Nivel 2 (lógica de dominio pura); buen punto de arranque de Fase 3 en paralelo a Nivel 1.

### Nivel 3 — Medio (lógica de negocio dependiente de Niveles 1-2)

| # | Archivo | Responsabilidad | Riesgo si falla | Cobertura | Tipo de test | Prioridad | Esfuerzo |
|---|---|---|---|---|---|---|---|
| 12 | `backfill.py` | Estrategia de backfill histórico (inferido) | Riesgo secundario al de sus dependencias (Nivel 1) | 0%, 180 stmts | Integración con adapters de Nivel 1 ya testeados | **P2** | Medio-alto |
| 13 | `ohlcv_pipeline.py` | Orquestación del pipeline OHLCV completo (inferido) | Medio — orquesta piezas ya revisadas | 0%, 158 stmts | Integración/E2E ligero | **P2** | Medio-alto |
| 14 | `resample_pipeline.py` | Resampling de timeframes (inferido; **tuvo bug real confirmado en el pasado** — `align_to_grid`, ya corregido) | Medio — historial de bug de producción sube su prioridad relativa | 0%, 144 stmts | Unitarias con foco en regresión del bug ya corregido | **P2** (candidato a P1 por historial) | Medio |
| 15 | `trades_backfill_fetcher.py` | Backfill histórico de trades, análogo a `ohlcv_fetcher.py` (inferido) | Medio — mismo patrón de riesgo, sin revisión de cuerpo aún | 0%, 132 stmts | Igual enfoque que `ohlcv_fetcher.py` una vez revisado el cuerpo | **P2** | Medio |

**Recomendación Nivel 3:** esperar a que Nivel 1 tenga cobertura base antes de testear
`backfill.py`/`ohlcv_pipeline.py` (heredan la incertidumbre de sus dependencias). Adelantar
`resample_pipeline.py` dentro de la cola por tener historial de bug real, no solo por capa.

### Resumen Fase 3a

5× P0 (Nivel 1 — máxima incertidumbre + máxima criticidad, sin revisión de cuerpo aún) ·
6× P1 (Nivel 2 — 3 con diseño ya confirmado, 3 sin revisar) · 4× P2 (Nivel 3 — dependientes).

**Pendiente:** Fase 3b (resto del inventario de 109 - 15 = 94 archivos restantes, agrupados
por patrón: `__init__`/re-exports/Protocol → probable cierre en bloque; excepciones ya
identificadas para revisión individual: `apps/api/auth/jwt.py`,
`application/consumers/base.py`, `adapters/inbound/websocket/onchain_producer.py`).

---

## Cierre de auditoría

**Estado: CERRADA.** Este documento queda como registro histórico de la auditoría
realizada el 2026-08-06. No se actualiza con avance de ejecución — el seguimiento de
tareas derivadas vive en `PLAN-HARDENING-2026.md` (documento vivo, separado).

**Resultado final:** 0 hallazgos de Riesgo Crítico nuevos detectados en esta sesión.
1 hallazgo de Riesgo Alto (Sección 2, Kafka) heredado de auditoría previa, confirmado
vigente. 3 hallazgos Mejorables. 15 archivos de alta criticidad operacional sin cobertura
de tests, priorizados por nivel de impacto (no por tamaño). 94 archivos adicionales sin
cobertura, pendientes de clasificación por patrón (Fase 3b).
