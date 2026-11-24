# Auditoría técnica integral (INFORME)

**OrangeCashMachine — Auditoría técnica integral (enfoque Staff/Principal Engineer y CTO)**

- **Fecha de medición de métricas:** 2026-08-06
- **Commit de referencia:** `dcd1741` (`git log -1 --format='%ci'` → 2026-08-06 00:08:04 -0500)
- **Alcance:** repositorio completo (`apps/`, `architecture/`, `config/`, `data_platform/`, `docs/`, `infrastructure/`, `ocm/`, `packages/`, `shared/`, `tests/`, CI/CD, Docker).
- **Método:** lectura directa de código y configuración + ejecución de la suite de pruebas con cobertura en vivo. Nada se da por supuesto: cada hallazgo cita archivo, módulo, función o ADR. Los ítems no demostrables se marcan **No verificado**.
- **Compromiso del entregable:** este documento es el único archivo generado. No se modificó código, no se hicieron commits.

> **Política de números:** todas las métricas cuantitativas se recalculan del repositorio al momento de la auditoría y llevan la fecha de medición. Los recuentos del documento son los del **estado actual del repositorio en `dcd1741`** (2026-08-06); no son compromisos ni valores congelados. Si un dato no pudo obtenerse de forma automática, se marca explícitamente como *No verificado*.

---

## 0. Resumen de verificación ejecutada

| Métrica | Valor medido (en vivo) | Cómo se obtuvo |
|---|---|---|
| Tests ejecutados | **844 passed** (58 archivos de test) en 46,06 s | `uv run pytest --cov=... -q` (2026-08-06) |
| Cobertura de líneas | **43 %** (14 061 sentencias, 8 048 sin cubrir) | `pytest --cov` en vivo (mismo comando) |
| Gate de cobertura | `fail_under = 0` (sin gate real) | `pyproject.toml:280-284` |
| Contratos import-linter | **47** bloques BC | `rg -c "[[tool.importlinter.contracts]]" architecture/importlinter.toml` |
| Constantes de tópicos/grupos Kafka | **44** | `rg -c "^(TOPIC\|GROUP)_" shared/kafka/topics.py` |
| Archivos de test de integración | 6 | `rg -l "integration" tests/**` |
| LOC Python (paquetes) | 52 429 | `find packages apps shared ocm infrastructure -name '*.py' \| xargs wc -l` |
| Hallazgos verificados | 25 (21 confirmados, 4 parcialmente confirmados) | ver §4 |

Nota crítica de método: una auditoría previa del repo (`docs/audits/2026-08-apps-audit.md`, `docs/audits/2026-08-composition-root-audit*.md`) reportaba cobertura **13 %** a partir del archivo `.coverage` (stale y gitignoreado en `.gitignore:48`). La medición en vivo al escribir este informe da **43 %**. **Ningún número de este documento proviene de artefactos stale.**

---

## 1. Caso de uso: visión declarada vs. realidad

**Visión declarada** (README.md, pyproject.toml:27, AGENTS.md): "pipeline profesional de ingestión, procesamiento y almacenamiento de datos de mercado cripto — arquitectura medallion (Bronze/Silver/Gold), data lakehouse Iceberg/DuckDB, observabilidad Prometheus", con trading live y paper.

**Realidad verificada:**

| Afirmación | Realidad | Evidencia |
|---|---|---|
| "Lakehouse Iceberg/**DuckDB**" | Iceberg **sí**; DuckDB **no se usa en absoluto**: es dependencia declarada pero sin un solo `import duckdb` en todo el repo (solo `pyproject.toml:125`). El catálogo Iceberg es SQLite (`data_platform/iceberg_catalog/catalog.db`, `pyiceberg[pyarrow,sql-sqlite]` en `pyproject.toml:124`). | `rg "import duckdb|from duckdb"` → 0 resultados; `rg -l "duckdb"` → solo `pyproject.toml` |
| Trading **live** ("⚠️ capital real") | `LiveExecutor.execute()` es un **stub**: loguea `[LIVE-STUB]` y devuelve `True` sin enviar nada; el bloque CCXT está comentado. `uv run live` es ejecución simulada con config/secrets de producción. | `packages/trading/execution/live_executor.py:91-134`; docstring `:95-97`; banner en AGENTS.md |
| "Medallion Bronze/Silver/Gold" | Implementado como capas PyIceberg: `infrastructure/storage/{bronze,silver,gold}` más `storage/iceberg/*`. Correcto como concepto, sin DuckDB. | `packages/market_data/infrastructure/storage/` |

**Conclusión:** la visión y el código coinciden en la arquitectura de datos (medallion + Iceberg reales), pero dos pilares de la descripción — DuckDB y ejecución live real — **no están implementados**. La brecha mayor es **live**: no existe camino verificado a una orden real en un exchange.

---

## 2. Análisis por perspectiva

### 2.1 Calidad arquitectónica

**Fuerte:**
- Hexagonal/Clean en `market_data` y `portfolio` con orden de capas enforced (BC-08 y BC-44 en `architecture/importlinter.toml:875` y `:674`), contratos AST (`tests/architecture/test_import_contracts.py`), dominio 100 % framework-agnostic (cero `pandas/polars/ccxt/pandera` en `domain/`, BC-09).
- DIP respetado en el borde exterior: CCXT está confinado a `adapters/outbound/exchange/` (`ccxt_adapter.py`); `application/` no importa adapters (BC-05).
- Composition roots disciplinados: `CompositionRoot` es `@dataclass(frozen=True, slots=True)` (`market_data/infrastructure/bootstrap/composition_root.py:91`), con imports lazy y `cast()` explícito; trading inyecta solo lo mínimo (ADR-0003, `trading/bootstrap/composition_root.py:210-216`).
- ACL pandas→polars correcto: `application/use_cases/ohlcv_transformer.py:51-52,367,420` (`pl.from_pandas` a la entrada, `.to_pandas` a la salida); `polars_interop.py` eliminado.

**Débil:**
- `trading` **no tiene capas formales** (dominio/ports/application). No existe contrato tipo `layers` ni equivalente a BC-09 para `trading`. Sus "estrategias" importan `pandas` a nivel de módulo (`strategies/base.py:19`, `ema_crossover.py:22`), sin contrato de gobierno tecnológico.
- La capa `ports` filtra tipos de DataFrame en contratos: `pandas` en runtime en `ports/outbound/historical_fetcher.py:53` y `polars` en 7 archivos de `ports/outbound/` (normalización, storage, chunk_converter…). Es un compromiso documentado (AGENTS.md), pero diluye el principio "ports = contratos puros" que BC-04 declara.
- Hay dos rutas de construcción de config paralelas (§2.2) y `apps/research/data/data_access.py` instancia adapters de `market_data` directamente, sin pasar por composition root (superficie DIP adicional).
- Excepciones en `domain/`: `domain/quality/types.py:25,33` ejecuta `subprocess` (`git rev-parse --short HEAD`) dentro del núcleo — el dominio depende de un binario externo.

**Veredicto calidad:** la disciplina es de nivel medio-alto en `market_data`/`portfolio` y media en `trading`. La mayor amenaza no es el estilo sino los defectos latentes (§4).

### 2.2 Escalabilidad a 5–10 años

Puntos de datos verificados:
- Productores Kafka con `enable_idempotence=True, acks="all"` (`adapters/outbound/kafka_trade_publisher.py:50-51`, `kafka_gap_publisher.py:114-115`) → entrega **al-menos-una-vez idempotente** (no exactly-once end-to-end).
- Schema registry: los wire schemas viven en `shared/kafka/schemas/` (BC-35), pero son **dataclasses** (no Avro/Protobuf); no hay `Schema Registry` (Confluent) ni evolución de esquema versionada — riesgo directo a 5 años cuando el catálogo de tópicos crezca.
- Ingestiona por proceso único: `main.py` (market-data service), composición en memoria; no hay consumidores de streaming dedicados escalables (los consumidores en `application/consumers/` son de calidad, no de réplica de estado).
- Iceberg soporta particionado/snapshot (`storage/iceberg/{partitions,snapshot_manager}.py`) y existe `cursor_store`/`timestamp_cache` para incrementales; sin embargo el catálogo es SQLite local (no hay un catalog remoto tipo REST → límite para múltiples workers escritores y para despliegue multinodo).
- **Sin evidencia de backpressure, retención por política, ni baldeo de flujo para millones de eventos/día.** Los productores son sincrónicos dentro del proceso de feed.

**Veredicto escalabilidad:** el diseño Iceberg/particionado y productores idempotentes son una base correcta, pero la plataforma actual es **de un solo nodo / proceso único** con catálogo SQLite y sin consumidores de streaming escalables. Escala a millones de eventos/día solo si se añade la capa de procesamiento distribuido (Dagster/Flink/Kafka Streams) y un catalog Iceberg remoto.

### 2.3 Mantenibilidad para un equipo de ingeniería

**Fuerte:**
- 47 contratos import-linter; CI fail-fast (`ocm-ci.yml`: el job `architecture` bloquea todos los demás; `concurrency: cancel-in-progress`).
- ADRs y GOVERNANCE.md bien documentados; SSOT de env vars con guard de importación (`ocm/config/env_vars.py:163-181`); SSOT de tópicos con assert de duplicados (`shared/kafka/topics.py:274`).
- Composición Root + SafeOps (fail-soft donde conviene, `dry_run` true por defecto, `config/base.yaml`; L5 `PRODUCTION_DRY_RUN` en `ocm/config/layers/rules.py`).

**Débil:**
- **Dos pipelines de config que ya divergen** (§2.2/§4, hallazgo ALTO): `config/config.yaml:35` carga `market_data/external_ingestion` (ausente de `_MODULE_GLOBS` en `ocm/config/hydra_loader.py:94-109`) y `_MODULE_GLOBS` carga `portfolio/portfolio.yaml` (ausente de `config.yaml`). No hay test de paridad (el referido `tests/config/test_structured_parity.py` **no existe**; el `FeedsConfig` de `ocm/config/structured/market_data_feeds.py` no se registra en Hydra — `apps/app/cli/main.py:55-73`).
- Doc drift: AGENTS.md dice 44 contratos, GOVERNANCE.md 43, el archivo tiene 47. Entrada `E402` obsoleta en pyproject.toml para `.../container.py` (archivo eliminado el 8/3). Bytecode huérfano `paper_bot*.pyc` en `packages/trading/__pycache__/`.
- Mezcla de idiomas en docstrings/errores (ES/EN) y referencias a `core/...` obsoletas (`ocm/config/credentials.py`).

**Veredicto mantenibilidad:** buena para un proyecto personal–small-team; la divergencia de config y el drift de documentación son los que más costarán a un equipo nuevo.

### 2.4 Competitividad frente al estado del arte

Prácticas públicas usadas como referencia (ver §6 para el detalle por pilar):

| Pilar | Práctica pública de referencia | Posición de OrangeCashMachine |
|---|---|---|
| DDD | Tácticas de Eric Evans; bounded contexts | Sólida en `market_data`/`portfolio`; débil en `trading` (sin dominio formal) |
| Hexagonal | Cockburn; ports & adapters | Correcta; 2 violaciones menores (research bypass, ports con pandas) |
| EDA | Kafka oficial / Confluent (at-least-once + idempotencia) | Base correcta (acks=all+idempotence), sin exactly-once ni schema registry |
| Lakehouse | Databricks Medallion; Iceberg (spec pública, curaduría Netflix/Iceberg) | Medallion correcto con PyIceberg; **DuckDB anunciado pero no usado** |
| Data Contracts | Airbnb/Stripe (Schema Registry, versionado de esquema) | Schemas SSOT en dataclasses, sin versionado ni registry |
| Observabilidad | Three pillars (Metrics/Logs/Traces); RED/USE | Logs+Metrics (Prometheus) OK; **sin traces, sin correlación de request-id** |
| Testing | Test pyramid; mutation/fault injection (Netflix Chaos) | 844 tests, contracts fuertes; cobertura 43 %, sin gate |
| CI/CD | Trunk-based + gate de calidad (Farley, público) | Fail-fast excelente; **CD es placeholder** (`workflow_dispatch` solo) |
| Seguridad | Secrets managers (Vault) + least-privilege | SecretStr Pydantic y non-root; **bandit sin CI, fugas de secrets en snapshot/`--cfg job`** |
| Escalabilidad | Streaming distribuido (Kafka, Flink/Kafka Streams) | Productores idempotentes; **consumo/replica estado no implementado** |

**Veredicto competitividad:** por debajo del estado del arte solo en los ejes de **seguridad**, **schema evolution** y **streaming/replica de estado**. Por encima de la media para proyectos personales en **gobernanza de arquitectura** (import-linter, ADRs, config SSOT).

---

## 3. Deuda técnica (clasificada)

La deuda se separa de las decisiones de diseño. **Decisiones conscientes** (ADR) no son defectos: se respetan y solo se señala su costo.

### 3.1 Decisiones conscientes (ADR — no defectos)

| Decisión | ADR | Costo asumido |
|---|---|---|
| Split interno/externo del trading engine | ADR-0005, sustituido por ADR-0012 (runtime-puro, composición solo en composition root) | Bien documentado; costó el inline de fábricas |
| Constructor angosto de trading | ADR-0003 | Correcto; impide acoplar trading a `AppConfig` completa |
| Delegación de rebalance | ADR-0011 — **abierta** (sin decisión) | `assemble_rebalance()` = `NotImplementedError` |
| Ports con transforms de DataFrame | AGENTS.md (SSOT de normalización en `ports/outbound/normalization.py`) | "ports" dejan de ser contratos puros |
| Modelo unificado de ingestión / evento | ADR-0013, ADR-0014, 0002 (kappa→event-driven) | Documentado |
| Gobernanza automática del shared kernel | ADR-0010 | Cumplido (SSOT env vars, topics, schemas) |

### 3.2 Deuda aceptable
- Mezcla ES/EN en mensajes y docstrings.
- Referencias `core/...` obsoletas en docstrings (`ocm/config/credentials.py`).
- `TradeRecord.pnl_usd` siempre `None` (placeholder) — `analytics/trade_record.py:102-109`.
- `__pycache__` con bytecode huérfano (`paper_bot*.pyc`).

### 3.3 Deuda prioritaria
- Cobertura 43 % sin gate (`pyproject.toml:284`).
- Mypy solo en `shared/` en CI (job `quality` de `ocm-ci.yml`), `strict=false`.
- Doc drift (47 vs 44 vs 43 contratos; E402 de archivo eliminado).
- `ports` con pandas/polars en runtime; `strategies/` de trading con pandas sin contrato.

### 3.4 Deuda crítica
- **LiveExecutor STUB** — el camino de producción de capital real no envía órdenes (`live_executor.py:130-134`).
- **Import roto latente** en composition root (`pipeline_factory.py:49`).
- **Drift del contador de riesgo** (`oms.py:172` vs `:217/:308`).
- **Fuga de secrets** en snapshot de config (`snapshot.py:92`).

---

## 4. Hallazgos (evidencia, impacto, riesgo, prioridad, complejidad, recomendación)

Leyenda de estado: **Confirmado** = verificado por lectura directa del archivo citado; **Parcialmente confirmado** = verificado parcialmente (el punto depende de una cadena de llamadas que no se recorrió entera); **No verificado** = no demostrable con la evidencia disponible.

### CRÍTICO

#### H-01 — LiveExecutor es un stub: no existe camino real a órdenes
- **Estado:** Confirmado
- **Evidencia:** `packages/trading/execution/live_executor.py:91-134` — `_submit()` loguea `[LIVE-STUB]` y `return True`; el bloque CCXT está comentado (`:99-128`). `apps/app/cli/live_hydra.py` y `execute_live.py:64-70` asumen un `exchange_client` que es `None`.
- **Impacto:** `uv run live` simula órdenes con config/credenciales de producción; un operador que lo confíe creerá tener posiciones reales. **La funcionalidad central del negocio (trading live) no existe.**
- **Riesgo:** Daño financiero real por falsa confianza + fallo al conectar producción.
- **Prioridad:** CRÍTICA — bloquea el objetivo "plataforma profesional de trading".
- **Complejidad:** Alta (requiere conector CCXT real, conversión size→qty, gestión de IDs, reconciliation con fill events).
- **Recomendación concreta:** (1) Renombrar `uv run live` a `paper --live-config` o añadir un guard que **impida** arrancar live con executor stub; (2) implementar `_submit` usando `market_data.adapters.outbound.exchange.ccxt_adapter.CCXTAdapter.create_order` (ya referenciado en el comentario); (3) añadir un test de arquitectura que falle si `live_executor.py` no contiene lógica de envío real.
- **¿Contradice algún ADR?** No; es deuda crítica no gobernada.

#### H-02 — Import roto latente en el composition root de market_data
- **Estado:** Confirmado
- **Evidencia:** `packages/market_data/infrastructure/bootstrap/pipeline_factory.py:49` → `from market_data.infrastructure.storage.catalog import build_catalog`. Ese módulo **no existe** (solo existe `storage/iceberg/catalog.py` con `get_catalog`/`ensure_*_table`; `ls packages/market_data/infrastructure/storage/catalog.py` → ENOENT). Es import lazy: no falla al importar, solo al construir trades/derivatives.
- **Impacto:** `ModuleNotFoundError` en tiempo de ejecución al construir las pipelines de trades/derivatives (path que los contracts de import-linter y mypy no ven: import dentro de cuerpo de función).
- **Riesgo:** Fallo en producción en un path de datos (silver trades/derivatives) no cubierto por la suite (43 % de cobertura).
- **Prioridad:** CRÍTICA (bug latente de runtime).
- **Complejidad:** Baja (una línea).
- **Recomendación concreta:** Corregir a `from market_data.infrastructure.storage.iceberg.catalog import get_catalog` y añadir un smoke test de construcción de la composition root (verificación de `assemble(config)`).
- **¿Contradice algún ADR?** No.

#### H-03 — Drift del contador de posiciones abiertas en RiskManager
- **Estado:** Confirmado
- **Evidencia:** `packages/trading/execution/oms.py:172` llama `self._risk.record_open()` en todo `submit()` (BUY y SELL); `record_close()` solo se llama en `cancel()` (`oms.py:217`) y `_reject()` (`oms.py:308`). La transición `_fill()` (`oms.py:270-289`) **no** llama `record_close()`. `risk/manager.py:126-139` incrementa/decrementa `_open_positions`. Un ciclo BUY fill→SELL fill deja `_open_positions` en +2 sin decremento.
- **Impacto:** `max_open_positions` (`risk/manager.py:224-227`) termina contando posiciones fantasma y rechaza señales válidas; el contador es una tercera fuente de verdad sobre la posición junto a `TradeTracker` y `PortfolioService`.
- **Riesgo:** Divergencia de estado (exactamente lo que ADR-0006 pretende evitar) y comportamiento incorrecto de gate de riesgo.
- **Prioridad:** CRÍTICA.
- **Complejidad:** Media (definir semántica: contar posiciones abiertas reales vs órdenes activas; cerrar en `_fill`).
- **Recomendación concreta:** Mover el `record_close()` al flujo de fill (o redefinir `_open_positions` como "órdenes activas" y renombrarlo); añadir test de round-trip BUY→SELL que aserte el contador.
- **¿Contradice algún ADR?** No contradice; refuerza el invariante de ADR-0006.

### ALTO

#### H-04 — Cobertura 43 % sin gate real
- **Estado:** Confirmado
- **Evidencia:** medición en vivo `pytest --cov` (2026-08-06): 43 % (14 061 sentencias, 8 048 sin cubrir). `pyproject.toml:280-284`: `fail_under = 0` con TODO para fijarlo tras medir. CI (job `quality`) no lo gatea.
- **Impacto:** Los bugs latentes (H-02, H-03) pasan CI; superficies críticas (iceberg storage, kafka consumers, trading OMS) parcialmente cubiertas.
- **Riesgo:** Regresiones silenciosas en el corazón de datos/trading.
- **Prioridad:** ALTA.
- **Complejidad:** Baja fijar el gate (pero subir la cobertura es medio).
- **Recomendación concreta:** Fijar `fail_under` por etapas (p.ej. 45 % → 60 % en 3 meses) y exigir cobertura >0 en los módulos críticos (`trading/execution`, `storage/iceberg`). Los números se fijarán tras medición real, nunca sobre `docs/audits/2026-08-apps-audit.md`.
- **¿Contradice algún ADR?** No.

#### H-05 — Dos pipelines de config divergentes sin test de paridad
- **Estado:** Confirmado
- **Evidencia:** `config/config.yaml:35` carga `market_data/external_ingestion`; `ocm/config/hydra_loader.py:94-109` `_MODULE_GLOBS` **no** lo incluye y sí incluye `portfolio/portfolio.yaml` que `config.yaml` **no** lista. El test de paridad referido (`tests/config/test_structured_parity.py`) **no existe**; `FeedsConfig` (`ocm/config/structured/market_data_feeds.py`) no se registra en `apps/app/cli/main.py:55-73`.
- **Impacto:** La ruta Hydra (`uv run ocm`) y la standalone (`paper/live/research`) pueden computar `AppConfig` distintos; cualquier edición futura de `portfolio.yaml`/`external_ingestion.yaml` diverge comportamientos entre entrypoints.
- **Riesgo:** Config dispar entre entornos/CLIs sin detección automática.
- **Prioridad:** ALTA.
- **Complejidad:** Baja (añadir test de paridad; unificar listas).
- **Recomendación concreta:** Derivar `_MODULE_GLOBS` de `config/config.yaml#defaults` en vez de mantener dos listas; añadir test que falle si difieren (mismas claves `portfolio`/`external_ingestion`).
- **¿Contradice algún ADR?** No.

#### H-06 — Fuga de secrets en snapshot y en `--cfg job`
- **Estado:** Confirmado
- **Evidencia:** `ocm/config/loader/snapshot.py:92` → `config.model_dump(mode="json")` serializa `SecretStr` de `AppConfig` en **texto plano** en `logs/config_snapshots/{run_id}_{hash}.json`. `apps/app/cli/main.py:27` (`--cfg job`) expone el DictConfig Hydra resuelto por stdout (documentado como riesgo en AGENTS.md). Los campos son `SecretStr` (`ocm/config/schema.py:198-200,514,579-580`), así que el masking solo protege en `repr`, no en serialización.
- **Impacto:** Credenciales en repositorio de logs/snapshots (texto claro) y en stdout.
- **Riesgo:** Rotación de secrets / exfiltración si logs se comparten.
- **Prioridad:** ALTA.
- **Complejidad:** Baja (mascar en snapshot; `SecretStr` ya da `get_secret_value()` para redactar).
- **Recomendación concreta:** Redactar `SecretStr` al serializar snapshot (`model_dump(mode="json", exclude={'api_secret'...})` o un serializador que emita `***`); eliminar o restringir `--cfg job` en prod.
- **¿Contradice algún ADR?** No.

#### H-07 — Bandit instalado pero no aplicado
- **Estado:** Confirmado
- **Evidencia:** `bandit>=1.9.4` en deps dev (`pyproject.toml:169`); **sin** `[tool.bandit]`, **sin** job en `.github/workflows/ocm-ci.yml`, **sin** hook en `.pre-commit-config.yaml`.
- **Impacto:** Auditoría de seguridad solo por invocación manual (AGENTS.md); no hay gate.
- **Riesgo:** Vulnerabilidades de código (subprocess, pickle, eval) sin detección automática.
- **Prioridad:** ALTA (para una plataforma que manejará capital).
- **Complejidad:** Baja (añadir a CI + pre-commit con `-ll`).
- **Recomendación concreta:** Añadir `bandit -r apps ocm packages shared infrastructure` al job `quality` de `ocm-ci.yml` y a pre-commit.
- **¿Contradice algún ADR?** No.

#### H-08 — `order_id` de 8 hex: riesgo de colisión de clave de posición
- **Estado:** Confirmado
- **Evidencia:** `packages/trading/execution/order.py:94` → `order_id = str(uuid.uuid4())[:8]`; docstring `:84-87` admite colisión "negligible < 10^6". Es la PK de `RedisPositionStore` (`ocm:positions:{exchange}:{order_id}` en `portfolio/infra/redis_store.py`).
- **Impacto:** a volumen (millones de órdenes/5 años) una colisión sobrescribe silenciosamente otra posición.
- **Riesgo:** Corrupción silenciosa de estado de posición.
- **Prioridad:** ALTA (para objetivo a 5 años).
- **Complejidad:** Baja (usar UUID completo o secuencia exchange-scoped).
- **Recomendación concreta:** Usar `uuid4().hex` completo (32 chars) o sufijo de exchange en la clave; añadir assert de unicidad en el store.
- **¿Contradice algún ADR?** No.

#### H-09 — Estado de posición replicado en tres estructuras paralelas
- **Estado:** Parcialmente confirmado (los mapas existen; la divergencia entre los tres en runtime no se ejercitó en una prueba)
- **Evidencia:** `execution/fill_sync.py:75` (`open_order_ids`), `analytics/trade_tracker.py:57` (`_open_positions`), y `PortfolioService`/`PositionStore` (`services/portfolio_service.py:89-152`). En SELL, `fill_sync:96-98` hace `portfolio.close_position(buy_id)`; `portfolio_service.py:150-152` traga errores y devuelve `None` (el retorno se descarta en `fill_sync:49`).
- **Impacto:** Tres fuentes de verdad que pueden divergir (fallo silencioso de cierre = posición fantasma).
- **Riesgo:** Estado de portfolio incorrecto en paper/live.
- **Prioridad:** ALTA.
- **Complejidad:** Media (unificar en `PortfolioService` como único dueño — alineado con ADR-0006 — y derivar analytics/tracker de los eventos de llenado).
- **Recomendación concreta:** Que `PortfolioService` sea la única fuente mutable; `fill_sync`/`TradeTracker` deben consumir eventos, no mantener copias; no descartar el retorno de `close_position`.
- **¿Contradice algún ADR?** Alinea con ADR-0006 ("portfolio owns position state"); no contradice.

### MEDIO

#### H-10 — Stub muerto `onchain_producer.py` con imports colgantes
- **Estado:** Confirmado
- **Evidencia:** `packages/market_data/adapters/inbound/websocket/onchain_producer.py:9-13` importa `infra.kafka.base_producer`, `infra.kafka.groups`, `infra.kafka.topics` y `market_data.domain.ports.kafka_producers` — **ninguno existe** (`infra/` no es paquete top-level; `domain/ports` no existe). Su docstring dice "Stub — NOT IMPLEMENTED". Nada lo importa.
- **Impacto:** Código muerto que rompería contracts/mypy si se cargara.
- **Prioridad:** MEDIA.
- **Complejidad:** Baja.
- **Recomendación concreta:** Eliminar o implementar; si es on-chain futuro, moverlo bajo ADR y dejar el topic `TOPIC_ONCHAIN_RAW` gobernado en `shared/kafka/topics.py` (ya existe a nivel de constantes).
- **¿Contradice algún ADR?** No.

#### H-11 — `subprocess git` dentro del dominio
- **Estado:** Confirmado
- **Evidencia:** `packages/market_data/domain/quality/types.py:25,33` ejecuta `git rev-parse --short HEAD` vía `subprocess`.
- **Impacto:** El dominio depende de un binario externo y ejecuta procesos en tiempo de reporte; rompe en espíritu la regla framework-agnostic de BC-09.
- **Prioridad:** MEDIA.
- **Complejidad:** Baja (inyectar el hash como metadato traceado).
- **Recomendación concreta:** Pasar el git-hash como parámetro/metadato desde infra (se obtiene en el composition root), eliminando `subprocess` del dominio.
- **¿Contradice algún ADR?** No.

#### H-12 — `ports` con pandas/polars en runtime
- **Estado:** Confirmado
- **Evidencia:** `ports/outbound/historical_fetcher.py:53` (`import pandas as pd`); `ports/outbound/normalization.py:25`, `storage.py`, `quality_pipeline.py`, `data_quality_checker.py`, `chunk_converter.py`, `feature_reader.py` (`import polars as pl`).
- **Impacto:** La capa de contratos deja de ser pura; cualquier adapter/application se acopla al DataFrame elegido.
- **Prioridad:** MEDIA (compromiso consciente en AGENTS.md).
- **Complejidad:** Media (introducir tipos intermedios o mover los transforms a una capa "ports.implementations").
- **Recomendación concreta:** Documentarlo como decisión consciente (ya lo es) y aislarlo en un submódulo `ports/outbound/transforms/` para que la superficie de contratos siga siendo pura.
- **¿Contradice algún ADR?** No; es decisión registrada en AGENTS.md.

#### H-13 — DuckDB declarada pero no usada
- **Estado:** Confirmado
- **Evidencia:** `pyproject.toml:125` (`duckdb>=1.5.1,<2.0`); **cero** imports en el código. La descripción del proyecto (`pyproject.toml:27`, AGENTS.md) la promociona como pilar del lakehouse.
- **Impacto:** Dependencia sin uso (superficie de ataque y peso) + descripción inexacta del producto.
- **Prioridad:** MEDIA.
- **Complejidad:** Baja.
- **Recomendación concreta:** O bien eliminar la dependencia y corregir la descripción, o bien adoptarla para la capa de consulta/analítica local (Silver/Gold) documentada en ADR.
- **¿Contradice algún ADR?** No (no hay ADR de DuckDB).

#### H-14 — Docker: `COPY . .` sin `.dockerignore`; puertos expuestos sin auth
- **Estado:** Confirmado
- **Evidencia:** `Dockerfile:35` (`COPY . .`); no existe `.dockerignore` (glob confirmado). `docker-compose.yml` bindea servicios a `0.0.0.0` por defecto (Redis 6379, Prometheus 9090, Grafana 3000, Loki 3100, kafka-ui 8080 sin auth).
- **Impacto:** `.env`/`secrets.yaml` pueden quedar horneados en la imagen; servicios de observación/UI alcanzables por red.
- **Prioridad:** MEDIA-ALTA (seguridad de infraestructura).
- **Complejidad:** Baja.
- **Recomendación concreta:** Añadir `.dockerignore` (`**/.env`, `*.pem`, `logs/`, `data/`); restringir binds a `127.0.0.1` o red interna; auth en kafka-ui; `HEALTHCHECK` en el Dockerfile.
- **¿Contradice algún ADR?** No.

#### H-15 — Sin schema registry ni evolución de esquema versionada
- **Estado:** Parcialmente confirmado (los schemas son SSOT; la ausencia de versionado/registry se deduce de su forma: dataclasses en `shared/kafka/schemas/`, sin metadatos de versión ni integración con un registry).
- **Evidencia:** `shared/kafka/schemas/_base.py` (`BasePayload`), 12 módulos de payload; `shared/kafka/topics.py` (44 constantes). No hay Avro/Protobuf ni servidor de registry en `docker-compose.yml`.
- **Impacto:** Cambios de esquema rompen consumidores sin detección; a 5 años, migración costosa.
- **Prioridad:** MEDIA (para objetivo a 5 años).
- **Complejidad:** Media (adoptar Avro+Confluent Schema Registry o Redpanda).
- **Recomendación concreta:** Migrar payloads a Avro con `compatibility=backward` y un registry en compose; mantener BC-35 (los schemas siguen en `shared/kafka/schemas/`, ahora generados).
- **¿Contradice algún ADR?** No.

#### H-16 — Sin exactly-once end-to-end
- **Estado:** Parcialmente confirmado (idempotencia confirmada; la semántica de consumidores/escritura Bronze-Silver no se verificó completa).
- **Evidencia:** productores `enable_idempotence=True, acks="all"` (`kafka_trade_publisher.py:50-51`); consumidores en `application/consumers/` sin evidencia de transacciones/offsets atómicos con escritura.
- **Impacto:** Duplicados posibles en Bronze/Silver en reintentos (data lake tolera pero corrompe agregados Gold).
- **Prioridad:** MEDIA.
- **Complejidad:** Alta (transacciones Kafka o idempotencia por clave de negocio en storage).
- **Recomendación concreta:** Definir claves de idempotencia naturales (exchange+ts+side+qty+id) y dedup en Bronze (`infrastructure/kafka/dedup.py` ya existe), con test de reintento.
- **¿Contradice algún ADR?** No.

#### H-17 — `RiskGate` protocol muerto/incoherente
- **Estado:** Confirmado (el contrato existe y no es usado)
- **Evidencia:** `shared/contracts/boundaries.py:86-106` define `RiskGate` con `.is_halted` y `.evaluate(signal)->(bool,str)`; el implementador real es `RiskManager.validate()->RiskDecision` (`risk/manager.py`), y `oms.py:134,250` lo consume directamente. Grep: `RiskGate` solo aparece en docstrings de schemas kafka y `contracts/__init__.py`.
- **Impacto:** Contrato público que ya no describe la implementación; confunde a futuros integradores.
- **Prioridad:** MEDIA.
- **Complejidad:** Baja.
- **Recomendación concreta:** Actualizar `RiskGate` a `RiskDecision` o eliminar el protocol y documentar el contrato real en el shared kernel (BC-45 no lo prohíbe; es coherencia).
- **¿Contradice algún ADR?** No.

#### H-18 — Sin traces ni correlación request-id en observabilidad
- **Estado:** Confirmado
- **Evidencia:** `ocm/observability/` tiene logger multi-sink, Prometheus (push/pull) y Loki (`ocm/observability/logger.py`, `metrics_runtime.py`); **no hay** módulo de tracing (sin OpenTelemetry en `pyproject.toml`), ni request-id propagado entre sinks.
- **Impacto:** Diagnóstico de latencia/causa-raíz end-to-end difícil; el "three pillars" está a 2/3.
- **Prioridad:** MEDIA.
- **Complejidad:** Media (OTel + propagación de contexto en consumers).
- **Recomendación concreta:** Introducir OpenTelemetry con exporter OTLP a un backend (Grafana Tempo) y correlacionar con los logs ya existentes.
- **¿Contradice algún ADR?** No.

#### H-19 — `assemble_rebalance()` = `NotImplementedError`
- **Estado:** Confirmado
- **Evidencia:** `packages/trading/bootstrap/composition_root.py:359-376` levanta `NotImplementedError` con `TODO(ADR-0011)`. `RebalanceService.rebalance()` (`services/rebalance_service.py:131-157`) no se invoca fuera de sus tests.
- **Impacto:** Capacidad de rebalance documentada pero no conectada; ADR-0011 queda abierta.
- **Prioridad:** MEDIA (funcionalidad nueva, no regresión).
- **Complejidad:** Media (decidir ADR-0011 y cablear a un disparador/use-case).
- **Recomendación concreta:** Tomar la decisión del ADR-0011 (delegación a portfolio) y añadir un use-case de orquestación + test de integración.
- **¿Contradice algún ADR?** No; cierra la ADR abierta.

### BAJO

#### H-20 — Drift de documentación de gobierno
- **Estado:** Confirmado
- **Evidencia:** AGENTS.md "44 contracts", `docs/architecture/GOVERNANCE.md` "43", archivo real **47** (medido). Entrada `E402` en `pyproject.toml` para `packages/market_data/infrastructure/bootstrap/container.py` (eliminado el 8/3).
- **Impacto:** Confusión en revisiones; guardias rotos no visibles.
- **Prioridad:** BAJA.
- **Complejidad:** Baja.
- **Recomendación concreta:** Generar el conteo de contratos en CI (script) y verificar contra `GOVERNANCE.md`; limpiar E402 obsoleta.
- **¿Contradice algún ADR?** No.

#### H-21 — `apps/research` instancia adapters directamente (bypass de composition root)
- **Estado:** Confirmado
- **Evidencia:** `apps/research/data/data_access.py` construye `IcebergStorageFactory → OHLCVStorage + GoldLoader` sin pasar por `market_data.infrastructure.bootstrap.composition_root`.
- **Impacto:** Duplica lógica de ensamblado; riesgo de divergencia de config.
- **Prioridad:** BAJA (lectura-only, fuera del camino crítico).
- **Complejidad:** Media (exponer un constructor research-only desde el composition root).
- **Recomendación concreta:** Añadir un factory de solo-lectura en el composition root y que research lo use.
- **¿Contradice algún ADR?** No.

#### H-22 — `strategies/` de trading con pandas sin contrato de tecnología
- **Estado:** Confirmado
- **Evidencia:** `packages/trading/strategies/base.py:19`, `ema_crossover.py:22` importan `pandas`; no existe para trading un contrato BC-09-equivalente (solo point contracts BC-12/36/50).
- **Impacto:** La "capa de dominio" de trading queda acoplada a pandas sin guardia.
- **Prioridad:** BAJA (consistencia; no es violación actual).
- **Complejidad:** Baja (extender el contrato a trading o mover a polars como el resto).
- **Recomendación concreta:** Añadir contrato `trading.strategies no pandas` (o adoptar polars) y migrar `ema_crossover`.
- **¿Contradice algún ADR?** No.

---

## 5. Métricas de repositorio (estado actual `dcd1741`, 2026-08-06)

| Métrica | Valor | Fuente |
|---|---|---|
| Tests | 844 passed (58 archivos) | `uv run pytest -q` en vivo |
| Cobertura | 43 % | `pytest --cov` en vivo |
| Contratos BC | 47 | `architecture/importlinter.toml` |
| Tópicos/grupos Kafka | 44 constantes | `shared/kafka/topics.py` |
| LOC Python | 52 429 | wc -l sobre packages/apps/shared/ocm/infrastructure |
| Peso del repo | ~62 KB pyproject; uv.lock 621 KB | `du -h` |
| Recuentos congelados | **ninguno**: todo lo anterior es medible en vivo | — |

---

## 6. Benchmark frente a prácticas públicas (por pilar)

Solo prácticas públicas y documentadas. No se infiere nada sobre sistemas internos de ninguna firma.

| Pilar | Práctica pública de referencia (fuente) | Estado del arte | OrangeCashMachine | Brecha |
|---|---|---|---|---|
| **DDD** | Tácticas de Evans (bounded contexts, aggregates, domain events); "Domain-Driven Design" (Evans, 2003) | Bounded contexts con contracts explícitos y eventos de dominio | `market_data` y `portfolio` sólidos; `trading` sin dominio formal; rebalance sin decisión | Media |
| **Clean/Hexagonal** | Cockburn "Ports & Adapters"; Clean Architecture (Uncle Bob) | Domain puro, dependencias apuntan hacia adentro | Cumplido y enforced por import-linter; 2 violaciones menores (H-12, H-21) | Baja |
| **Event-Driven** | Confluent/Kafka: at-least-once + idempotencia; "Exactly-Once Semantics" (Confluent docs); transactional outbox (Pautasso) | ES con registry, idempotencia, replay, outbox | Productores acks=all+idempotence; sin registry (H-15), sin exactly-once end-to-end (H-16), consumidores de estado no escalables | Media-Alta |
| **Lakehouse** | Databricks Medallion (Bronze/Silver/Gold, público); Apache Iceberg spec (gobernada públicamente, adoptada en Netflix/BigQuery/Trino) | Medallion + Iceberg + catalog remoto | Medallion correcto con PyIceberg; catálogo **SQLite local**; **DuckDB anunciado y no usado (H-13)** | Media |
| **Data Contracts** | Airbnb/Stripe engineering: Schema Registry, versionado, compatible backward (público) | Contratos versionados con compatibilidad | SSOT de schemas (BC-35) pero dataclasses sin versionado | Media |
| **Observabilidad** | Three pillars (Metrics/Logs/Traces) — SRE book (público); RED/USE | 3 pilares correlacionados | 2/3 (logs+metrics); sin traces (H-18) | Media |
| **Testing** | Test pyramid (público); mutation/fault testing (Netflix Chaos, público) | Gates de cobertura + tests de invariantes | 844 tests + contracts AST excelentes; cobertura 43 % sin gate (H-04) | Media-Alta |
| **CI/CD** | Trunk-based + quality gates (D. Farley, "Continuous Delivery", público) | CI y CD automatizados con gates | CI fail-fast excelente; **CD placeholder** (`workflow_dispatch`) | Media |
| **Seguridad** | Secrets managers (HashiCorp Vault), least-privilege, SAST en CI (público) | Secrets fuera de discos/logs; SAST gateado | SecretStr + non-root; **bandit sin CI (H-07)**, fugas de secrets (H-06), puertos expuestos (H-14) | Alta |
| **Escalabilidad** | Kafka como backbone + Flink/Kafka Streams; schema evolution (público) | Streaming distribuido con replay | Productores idempotentes; single-process, catálogo SQLite, sin consumidores escalables | Alta |

**Conclusión benchmark:** las brechas estructurales están en **seguridad**, **schema evolution** y **streaming/replica de estado**. En gobernanza de arquitectura (contracts, ADRs, config SSOT) el proyecto está por encima de la media de proyectos personales y comparable a lo que equipos profesionales documentan en repositorios públicos.

---

## 7. Scorecard Arquitectónico (0–10)

Puntuación con justificación basada en evidencia. 0 = inexistente, 10 = estado del arte.

| Eje | Puntaje | Justificación (evidencia) |
|---|---|---|
| **DDD** | **7** | Bounded contexts limpios y ADRs (ADR-0006/0012); eventos y value objects en `market_data/domain`; pero `trading` sin dominio formal y rebalance sin decisión (H-19). |
| **Clean Architecture** | **8** | Domain 100 % framework-agnostic (BC-09), dependencias hacia adentro enforced; castigos: ports con pandas (H-12) y research bypass (H-21). |
| **Hexagonal** | **8** | Ports/adapters reales y composition roots frozen/lazy (composition_root.py:91); excepciones ya citadas. |
| **Event Driven** | **6** | SSOT de tópicos (44) y schemas (BC-35), productores idempotentes; sin registry ni exactly-once (H-15/H-16). |
| **Configuración** | **8** | L1→L5 documentada, SSOT env_vars con guard, SafeOps; drift de doble pipeline (H-05) y fuga de secrets (H-06). |
| **Testing** | **6** | 844 tests + contracts AST; cobertura 43 % sin gate (H-04), integración mínima. |
| **Observabilidad** | **6** | Logs multi-sink + Prometheus + Loki; sin traces ni correlación (H-18). |
| **Seguridad** | **4** | SecretStr y non-root OK; bandit sin CI (H-07), secrets en snapshot/stdout (H-06), puertos 0.0.0.0 (H-14), sin `.dockerignore`. |
| **Escalabilidad** | **5** | Iceberg+particionado y productores idempotentes; single-process, catálogo SQLite, sin streaming escalable. |
| **Performance** | **5** | Migración polars correcta (ACL en ohlcv_transformer); colas de pandas en backfill/repair/gold y round-trip listas (`_validate_and_classify`); sin benchmarks. |
| **Operabilidad** | **6** | Composition roots, SafeOps, run registry (`ocm/runtime/registry.py`); sin HEALTHCHECK, kafka-ui sin auth. |
| **Mantenibilidad** | **7** | 47 contracts, ADRs, SSOTs; doc drift (H-20), 52k LOC en 3 BC. |
| **Preparación para producción** | **3** | **No confiaría producción hoy**: live es stub (H-01), import roto (H-02), contador de riesgo (H-03), seguridad (H-06/07/14). La config y el CI son sólidos; falta lo crítico del negocio. |

**Promedio ponderado (producción):** ~6 en arquitectura, ~3 en preparación real para operar capital.

---

## 8. Top 20 mejoras de mayor ROI (ordenadas por impacto)

> Tiempos estimados para un ingeniero senior, un solo foco. Dificultad y riesgo cualitativos. "Impacto" = reducción de riesgo + valor de negocio.

| # | Mejora | Beneficio esperado | Dificultad | Riesgo | Tiempo estimado | Prioridad |
|---|---|---|---|---|---|---|
| 1 | Implementar o bloquear `LiveExecutor` (H-01) | Único camino real a capital; elimina falsa confianza | Alta | Medio (si se bloquea: bajo) | 1–2 semanas (bloqueo: 1 día) | CRÍTICA |
| 2 | Arreglar import roto `pipeline_factory.py:49` + smoke test de CR (H-02) | Evita fallo en trades/derivatives | Baja | Bajo | 2 horas | CRÍTICA |
| 3 | Corregir contador de riesgo BUY/SELL (H-03) | Gate de riesgo correcto; estado consistente | Media | Bajo | 1 día | CRÍTICA |
| 4 | Redactar secrets en snapshot y restringir `--cfg job` (H-06) | Deja de filtrar credenciales | Baja | Bajo | 2 horas | ALTA |
| 5 | Fijar `fail_under` de cobertura y subirla en módulos críticos (H-04) | Evita regresiones en trading/iceberg | Baja→Media | Bajo | 1–3 días | ALTA |
| 6 | Test de paridad config `config.yaml` vs `_MODULE_GLOBS` (H-05) | Config uniforme entre CLIs | Baja | Bajo | 1 día | ALTA |
| 7 | Añadir bandit a CI + pre-commit (H-07) | SAST gateado | Baja | Bajo | 2 horas | ALTA |
| 8 | UUID completo en `order_id` (H-08) | Sin colisiones de posición a volumen | Baja | Bajo | 1 hora | ALTA |
| 9 | `.dockerignore` + binds locales + auth kafka-ui + HEALTHCHECK (H-14) | Superficie de red/riesgo de imagen reducida | Baja | Bajo | 1 día | MEDIA-ALTA |
| 10 | Unificar estado de posición en `PortfolioService` (H-09) | Una sola fuente de verdad (ADR-0006) | Media | Medio | 3–5 días | ALTA |
| 11 | Eliminar `onchain_producer.py` o implementarlo bajo ADR (H-10) | Sin dead code que rompe contracts | Baja | Bajo | 1 hora | MEDIA |
| 12 | Mypy sobre todos los paquetes en CI (no solo `shared/`) | Errores de tipos en mercado/trading | Media | Bajo | 1 semana (limpiar) | MEDIA |
| 13 | Schema Registry Avro con compatibilidad backward (H-15) | Evolución de esquema segura a 5 años | Media | Medio | 1–2 semanas | MEDIA |
| 14 | Idempotencia end-to-end (claves naturales + dedup en Bronze) (H-16) | Sin duplicados en Silver/Gold | Alta | Medio | 1–2 semanas | MEDIA |
| 15 | OpenTelemetry + Tempo (H-18) | 3 pilares completos; causa-raíz rápida | Media | Bajo | 1 semana | MEDIA |
| 16 | Quitar DuckDB o adoptarla con ADR (H-13) | Descripción real + sin dep sin uso | Baja | Bajo | 1 hora | MEDIA |
| 17 | Inyectar git-hash sin `subprocess` (H-11) | Dominio puro 100 % | Baja | Bajo | 1 hora | MEDIA |
| 18 | Cerrar ADR-0011 y cablear rebalance (H-19) | Capacidad anunciada usable | Media | Medio | 1 semana | MEDIA |
| 19 | Migrar `strategies/` de trading a polars + contrato (H-22) | Coherencia con la migración | Media | Bajo | 2–3 días | BAJA |
| 20 | Generar conteo de contracts y docs en CI (H-20) | Documentación siempre verdadera | Baja | Bajo | 1 día | BAJA |

---

## 9. Roadmap recomendado (impacto × esfuerzo)

> Cada ítem incluye beneficio, complejidad, riesgo, dependencias e impacto arquitectónico.

### Quick wins (1–2 días)
1. **Bloquear `uv run live` con executor stub** — *Beneficio:* cero falsa confianza. *Complejidad:* baja. *Riesgo:* bajo. *Dependencias:* ninguna. *Impacto arquitectónico:* ninguno (guard de seguridad).
2. **Arreglar `pipeline_factory.py:49`** + smoke test de `assemble()`. — *Beneficio:* elimina fallo latente. *Complejidad:* baja. *Riesgo:* bajo. *Dependencias:* H-02. *Impacto:* ninguno.
3. **Redactar secrets en snapshot + restringir `--cfg job`.** — *Beneficio:* fin de la fuga. *Complejidad:* baja. *Riesgo:* bajo. *Dependencias:* H-06. *Impacto:* ninguno.
4. **Bandit en CI/pre-commit y `.dockerignore`.** — *Beneficio:* SAST gateado + imagen limpia. *Complejidad:* baja. *Riesgo:* bajo. *Impacto:* ninguno.
5. **Test de paridad de config** (derivar `_MODULE_GLOBS` de `config.yaml#defaults`). — *Beneficio:* config uniforme. *Complejidad:* baja. *Riesgo:* bajo. *Impacto:* elimina la dualidad de registros.

### Corto plazo (1–2 semanas)
6. **Corregir el contador de riesgo (H-03) + tests BUY/SELL.** — *Beneficio:* gate de riesgo correcto. *Complejidad:* media. *Riesgo:* medio (toca OMS). *Dependencias:* tests de OMS. *Impacto:* invariante ADR-0006.
7. **Fijar `fail_under` por etapas y subir cobertura en `trading/execution` y `storage/iceberg`.** — *Beneficio:* gate real. *Complejidad:* media. *Riesgo:* bajo. *Impacto:* ninguno.
8. **Implementar `LiveExecutor` con CCXTAdapter (desbloqueo H-01).** — *Beneficio:* trading real. *Complejidad:* alta. *Riesgo:* alto (capital). *Dependencias:* ccxt_adapter, fill events, reconciliation. *Impacto:* mayor: abre el camino de producción; requiere ADR nuevo (fill/reconciliation).

### Medio plazo (1–2 meses)
9. **Unificar estado de posición en `PortfolioService` (H-09) y derivar analytics/tracker de eventos de fill.** — *Beneficio:* una sola fuente de verdad. *Complejidad:* media. *Riesgo:* medio. *Dependencias:* H-03, llenado real. *Impacto:* cumple ADR-0006 de raíz.
10. **Mypy completo en CI + limpieza.** — *Beneficio:* tipos verificados en todo el repo. *Complejidad:* media. *Riesgo:* bajo. *Impacto:* ninguno.
11. **Migrar `strategies/` de trading a polars + contrato de tecnología.** — *Beneficio:* coherencia de la migración. *Complejidad:* media. *Riesgo:* bajo. *Impacto:* ninguno.
12. **Schema Registry Avro + compatibilidad backward.** — *Beneficio:* evolución de esquema a 5 años. *Complejidad:* media. *Riesgo:* medio. *Dependencias:* BC-35. *Impacto:* cambia formato de cable (requiere ADR).
13. **Cerrar ADR-0011 y cablear rebalance a un use-case.** — *Beneficio:* capacidad usable. *Complejidad:* media. *Riesgo:* medio. *Impacto:* nuevo flujo de negocio.

### Largo plazo (6 meses+)
14. **Reemplazar catálogo SQLite por Iceberg REST catalog** (MinIO/Nessie o Hive metastore) — *Beneficio:* multiworker, transacciones reales. *Complejidad:* alta. *Riesgo:* medio. *Impacto:* habilita escala horizontal.
15. **Capa de streaming: consumidores dedicados (Dagster o Flink/Kafka Streams) para Silver→Gold y replica de estado.** — *Beneficio:* escala a millones de eventos/día. *Complejidad:* alta. *Riesgo:* alto. *Dependencias:* 14. *Impacto:* arquitectura multi-nodo; es el cambio estructural más grande.
16. **Exactly-once end-to-end (dedup + transactional outbox).** — *Beneficio:* datos exactos. *Complejidad:* alta. *Riesgo:* alto. *Dependencias:* 14-15. *Impacto:* semántica de entrega.
17. **OpenTelemetry completo.** — *Beneficio:* 3 pilares. *Complejidad:* media. *Impacto:* observabilidad transversal.

---

## 10. Resumen ejecutivo — para el CTO

### ¿Confiarías en esta arquitectura para operar durante los próximos 5 años?

**No, no hoy — pero sí como base si se cierran 4 puntos en 3 meses.** La arquitectura de datos y la gobernanza son confiables: el medallion Iceberg (`packages/market_data/infrastructure/storage/{bronze,silver,gold}`), los 47 contratos import-linter, el dominio puro y la pipeline de config L1→L5 son cimientos sólidos. Lo que impide la confianza a 5 años son **defectos de producto, no de arquitectura**: el camino live es un stub (`trading/execution/live_executor.py:91-134`), existe un import roto latente (`market_data/infrastructure/bootstrap/pipeline_factory.py:49`), el contador de riesgo deriva (`trading/execution/oms.py:172` vs `:217`), y la seguridad es la más débil (bandit sin gate, secrets en snapshot en `ocm/config/loader/snapshot.py:92`). Resueltos esos, la base aguanta 5 años para un equipo pequeño; para escala de millón de eventos/día exige la fase 14–15 del roadmap (catalog Iceberg remoto + streaming dedicado).

### ¿Cuáles son los mayores riesgos reales?
1. **Falsa sensación de trading real** (live stub) — riesgo financiero y reputacional. [H-01]
2. **Datos incorrectos por fallos de runtime silenciosos** — import roto en trades/derivatives (H-02) y contador de riesgo (H-03).
3. **Fuga de credenciales** en snapshots/stdout y superficie de red sin auth (H-06, H-14).
4. **Divergencia de config entre CLIs** (H-05) → comportamiento distinto en paper vs live.
5. **Regresiones no detectadas** — 43 % de cobertura sin gate (H-04).

### ¿Qué eliminarías?
- `packages/market_data/adapters/inbound/websocket/onchain_producer.py` (stub muerto con imports a módulos inexistentes, H-10).
- `duckdb` como dependencia declarada sin uso (H-13) — o se adopta con ADR o se elimina.
- `RiskGate` como contrato público incoherente con `RiskManager` (H-17).
- `TradeRecord.pnl_usd` placeholder y bytecode huérfano (`paper_bot*.pyc`).
- La entrada `E402` obsoleta de `container.py` en `pyproject.toml`.

### ¿Qué mantendrías exactamente igual?
- **La disciplina de contratos** (`architecture/importlinter.toml`, 47 BC) y el fail-fast de CI (`ocm-ci.yml`).
- **La migración pandas→polars** con ACL único en `application/use_cases/ohlcv_transformer.py`.
- **El pipeline de config L1→L5 + SSOT de env vars** (`ocm/config/env_vars.py:163-181`).
- **La composición de raíz** (frozen, lazy, fail-soft) y los SafeOps (`dry_run` true por defecto, L5 prod-not-dry-run).
- **La separación de bounded contexts** y el shared kernel con SSOT de tópicos/schemas (BC-35).

### ¿Qué rediseñarías desde hoy?
- **La semántica de estado de posición**: una sola fuente de verdad (`PortfolioService`), con `fill_sync`/`TradeTracker` consumiendo eventos en vez de mantener copias (H-09), y el contador de riesgo coherente (H-03).
- **El registro de módulos de config** (derivar de `config.yaml#defaults`, H-05).
- **El camino live** como feature de primera clase con ADR de fill/reconciliation (H-01).

### ¿Cuál sería el siguiente roadmap recomendado?
Fase 0 (1 semana): bloqueo de live stub, fix `pipeline_factory.py:49`, redacción de secrets, bandit+`.dockerignore`, test de paridad de config, fix del contador de riesgo. Fase 1 (1–2 meses): cobertura con gate, unificación de estado de posición, implementación real de LiveExecutor bajo ADR, mypy completo. Fase 2 (3–6 meses): Schema Registry, idempotencia end-to-end, OTel. Fase 3 (6+ meses): catalog Iceberg remoto y streaming dedicado para escala horizontal.

---

*Este informe refleja el estado del repositorio en `dcd1741` (2026-08-06). Las métricas cuantitativas son mediciones en vivo; se recalculan en cualquier re-auditoría y no deben tratarse como constantes.*
