# Auditoría de arquitectura — `apps/` (api · app · research)

**Fecha:** 2026-08-03
**Alcance:** `apps/api` (9 módulos, ~171 LOC) · `apps/app` (11 módulos, ~1.166 LOC) · `apps/research` (4 módulos, ~326 LOC)
**Estado del repo:** working tree limpio
**Modo:** revisión de arquitectura de producción (solo lectura). No se modificó código.

> **Revisión 2 (2026-08-03).** Aplicado a `apps/` el marco de **Application Layer pura**
> (responsabilidades, SSOT, DIP, DRY, KISS, Fail-Fast/Fail-Soft, SafeOps, Research).
> Clasificación revisada incorporando la categoría **"No corregir (deuda aceptable)"**.
> **Ámbito ejecutado: solo actualización del informe** — ningún cambio de código.
> El plan de remediación de código queda documentado (§9) y pendiente de aprobación.
>
> **Revisión 3 (2026-08-03).** Incorporadas las **6 reglas obligatorias de refactor** de
> `apps/` (Application Layer pura) que gobiernan todo cambio de código:
> **R1** solo Application Layer (sin reglas de negocio/trading/riesgo/persistencia/infra);
> **R2** un archivo = una responsabilidad; **R3** flujo unidireccional
> `CLI → AppConfig → Use Case → Ports → CR → BCs` (nunca reverso, nunca saltar capas);
> **R4** un solo SSOT por concepto; **R5** sin bridge `argparse.Namespace` (Namespaces
> solo en el borde del CLI; comunicación interna tipada); **R6** Fail-Fast en config
> (prohibido `getattr(..., default)` / `dict.get(..., default)` para datos obligatorios).
> El plan §9 queda actualizado con el mapeo hallazgo→regla y **dos decisiones aprobadas**:
> regla R2 en modo **pragmático** (un `_bootstrap.py` + use cases como orquestación tipada,
> sin proliferación de módulos diminutos) y `max_order_usd = capital × max_risk_pct`
> **mantenido derivado en el CLI** (config-time vía `model_copy`). Ámbito: solo informe.
>
> **Revisión 4 (2026-08-03).** Añadidas las **reglas 7–19** del contrato de refactor
> (Fail-Soft runtime, Composition Roots únicos, DRY con criterio, KISS, DIP estricto,
> Event-Driven, Bounded Contexts, Naming, archivos justificados, árbol mínimo, resultado
> menor, beneficio justificado, sin arquitectura especulativa — ver §3.4). Decisiones
> resultantes: **H11 pasa de "No corregir" a "Corregir"** (R19 prohíbe placeholders y
> arquitectura especulativa); **H2 y H3 se mantienen "No corregir"** (deuda documentada
> y justificada por R18); el helper CLI compartido se nombra **`_bootstrap.py`** en vez
> de `_shared.py` (**R14** prohíbe `shared`/`common`/`helper`). Ámbito: solo informe,
> sin ejecutar el plan (§9 sigue pendiente de aprobación).
>
> **Revisión 5 (2026-08-03).** Incorporadas las **decisiones del propietario** sobre el
> plan: (1) se confirman como **propuestas de corrección** H1, H4, H6, H7, H8, H9, H12,
> H13; (2) **H11 re-evaluado con evidencia histórica** (commits `6f4ff38`/`4e5f53b`,
> hallazgo 2.3 del audit de composition roots, ADR-0005/0006) → se confirma **Corregir**
> (§5): nació con propósito legítimo (SafeOps) pero hoy es **código muerto** tras la
> propiedad de Redis por `PortfolioCompositionRoot`; (3) **H2** y **H3** se **mantienen
> No corregir** (sin introducir puertos/abstracciones sin beneficio demostrado);
> (4) **no se proponen capas/managers/helpers/factories nuevos** sin reducción
> demostrable de complejidad o duplicación; (5) **nueva §3.5 con métricas objetivas**
> (complejidad ciclomática, tamaño, fan-in/fan-out, ciclos) que justifican la
> priorización; (6) **§9 reordenado**: F1=H1,H4,H9 · F2=H6,H8,H12 · F3=H13,H7 ·
> F4=H11 (post-reevaluación) · F5=cobertura (O1). Ámbito: solo informe.
>
> **Revisión 6 (2026-08-03, tarde).** **Validación de cada hallazgo contra el código
> real** (snapshot `apps-audit.tar.gz` verificado 100% idéntico al working tree) con
> métricas objetivas recomputadas sobre el snapshot. Resultados: **(C1)** la citación
> de ADR-0005 en H1 es falsa — la frase citada no existe en ningún ADR ni en el forense;
> su única fuente es el comentario `paper_hydra.py:123-125`. **(C2)** la observación O2
> es **incorrecta**: ningún test referencia `SyntheticDataSource._SEED` — no existe
> acoplamiento con `tests/`. **Decisión del propietario (más conservador que el agente):
> H11 pasa de "Corregir" a "No corregir (deuda aceptable)"** — el objeto de ciclo de vida
> no es código muerto, está justificado por su origen (hallazgo 2.3, SafeOps) y por la
> evolución prevista del motor live (CCXT/websocket en roadmap event-driven), y
> eliminarlo no simplifica sino que **reubica** una responsabilidad que hoy ya está
> correctamente delegada (cierre de Redis en `PortfolioCompositionRoot`). **H2 y H3 se
> confirman No corregir** (misma posición que el propietario). **Nueva §3.6 con la
> matriz validada por hallazgo** (evidencia real, riesgo, beneficio, costo, riesgo de
> regresión, prioridad, decisión final) y **§9 sin Fase 4**. Ámbito: solo informe, sin
> ejecutar el plan.
>
> **Revisión 7 (2026-08-10) — F-031/B-46.** Precisión sobre las filas "Event-Driven:
> ✅ Cumple / No rompe Kappa (ADR-0002)" y el punto "el CLI de datos consume vía
> orquestador" (Resumen ejecutivo, §3.4 completitud canvas). Esa afirmación constata
> que `apps/` no viola el modelo event-driven por import ni por invocación — sigue
> siendo correcta. **No debe leerse como que el path Kappa de OHLCV exista**: en HEAD,
> `OHLCVPipeline` publica a `NullPublisher()` hardcodeado y `_chunk_converter` no está
> inyectado, de modo que ningún evento OHLCV llega a `ohlcv.raw` (F-031/B-46,
> docs/audits/2026-08-08-streaming-canary-audit.md). "No romper Kappa" ≠ "Kappa OHLCV
> implementado y publicado". Sin cambios en las conclusiones de `apps/`.

---

## 1. Resumen ejecutivo

La capa `apps/` cumple de forma sólida su rol de **capa de aplicación/orquestación**:

- No contiene lógica de dominio que pertenezca a `packages/`, salvo **una excepción clara** (`SyntheticDataSource` en `execute_paper.py`, hallazgo H3).
- No instancia infraestructura fuera de los Composition Roots autorizados (ADR-0003/0012, BC-43/BC-50), con una excepción parcial en `research` (H2).
- El enrutado entre bounded contexts respeta los 44 contratos de `import-linter` (BC-01..BC-50) verificados en esta auditoría.
- Los entrypoints no rompen el modelo event-driven (Kappa, ADR-0002): el CLI de datos consume vía orquestador, y los ciclos de trading son invocación síncrona por diseño.

**No hay hallazgos Críticos.** La deuda detectada es de mantenibilidad/SSOT/robustez, no bloqueante. El hallazgo más relevante (H1) es la reconstrucción de `TradingConfig`/`RiskConfig` desde `argparse.Namespace` que ignora `AppConfig` (SSOT), que esta auditoría cuantifica. ~~Deuda prevista en ADR-0005~~ → **corrección C1 (Revisión 6):** la citación de ADR-0005 era falsa; ADR-0005 está reemplazado por ADR-0012 y la fuente real de la deuda prevista es el comentario `paper_hydra.py:123-125`.

**Herramientas ejecutadas (read-only):**

| Check | Comando | Resultado |
|---|---|---|
| Lint | `uv run ruff check apps/` | ✅ limpio (24 ficheros) |
| Formato | `uv run ruff format apps/ --check` | ✅ 24/24 |
| Contratos | `uv run lint-imports --config architecture/importlinter.toml` | ✅ 44/44 KEPT |
| Tests apps | `uv run pytest tests/app tests/research -q` | ✅ 19 passed |
| Seguridad | `uv run bandit -r apps` | 0 High · 1 Medium (B104 `0.0.0.0`, esperado) · 2 Low (asserts deliberados) |
| Tipos | `uv run mypy apps/` | ❌ **7 errores** (H9) |

---

## 2. Metodología

La evaluación se hizo sobre los 17 ejes solicitados, cruzando cada hallazgo con los ADRs canónicos (`docs/architecture/decisions/ADR-0003…0012`), la serie heredada (`docs/architecture/0000…0005`), los contratos de import-linter (`architecture/importlinter.toml`) y los `ports`/`schema` existentes, para evitar recomendaciones que contradigan el diseño ya aprobado.

**Reglas aplicadas:** (1) sin cambios por preferencia personal; (2) toda recomendación con evidencia `archivo:línea`; (3) severidad Crítico/Alto/Medio/Bajo/Observación; (4) impacto técnico explícito; (5) solución concreta y justificada; (6) no se modificó código; (7) deuda bloqueante vs. mejoras futuras separadas; (8) cruce con ADRs y contratos.

---

## 3. Marco de responsabilidad de `apps/` (Application Layer pura)

Contrato aprobado que gobierna esta revisión: `apps/` es **exclusivamente** la capa
de aplicación. Su responsabilidad se limita a recibir entradas, validar parámetros,
cargar configuración, orquestar use cases, coordinar bounded contexts, invocar
Composition Roots autorizados, transformar entradas/salidas, manejar errores,
logging y observabilidad. Nunca contiene reglas de negocio, algoritmos de trading,
cálculos de riesgo, persistencia ni acceso a infraestructura fuera de los
Composition Roots autorizados.

| `apps/` puede | `apps/` no puede |
|---|---|
| Recibir entradas (CLI/API/jobs) | Lógica de negocio / reglas de dominio |
| Validar parámetros y cargar config | Algoritmos de trading |
| Orquestar use cases y coordinar BCs | Cálculos de riesgo |
| Invocar Composition Roots autorizados | Persistencia |
| Transformar entradas/salidas | Acceso a infra fuera de CR autorizados |
| Manejar errores, logging, observabilidad | Conocer internos de otros BCs |

### 3.1 Inventario de responsabilidades por módulo

**`apps/api`**

| Módulo | Responsabilidad | Cumple el marco |
|---|---|---|
| `main.py` | Factory FastAPI, lifespan (Redis fail-fast/shutdown), `serve()` | ✅ |
| `settings.py` | `ApiSettings` (SSOT env API) + singleton | ✅ |
| `deps.py` | DI: `SettingsDep`, `RedisDep` (pool), `CurrentUserDep` (JWT→401) | ✅ |
| `auth/jwt.py` | sign/verify HS256 | ✅ |
| `middleware/logging.py` | log request/response; excluye `_SILENT_PATHS` | ✅ |
| `middleware/rate_limit.py` | sliding window Redis por IP | ✅ (H5/H6) |
| `routers/health.py` | `/health`, `/ready` | ✅ (H6) |

**`apps/app`**

| Módulo | Responsabilidad | Cumple el marco |
|---|---|---|
| `cli/main.py` | ocm pipeline: Hydra, RunConfig, EnvironmentValidator, pusher | ✅ |
| `cli/entrypoint.py` | runner batch (exchanges×markets) vía CR de market_data | ✅ |
| `cli/live_hydra.py` | CLI live: capital obligatorio, SIGTERM, CR portfolio+trading | ✅ (H1/H8) |
| `cli/paper_hydra.py` | CLI paper: dry-run, SIGTERM, CR portfolio+trading | ✅ (H1/H8) |
| `use_cases/execute_live.py` | ciclo live; `LiveEngineResources` | ⚠️ (H1, H7, H11, H12) |
| `use_cases/execute_paper.py` | ciclo paper; `SyntheticDataSource`; probe Gold | ⚠️ (H1, H3, H7, H10, H12) |

**`apps/research`**

| Módulo | Responsabilidad | Cumple el marco |
|---|---|---|
| `data/data_access.py` | API de lectura gold/silver → polars | ✅ como *research app* (H2 aceptada) |

### 3.2 Decisión de reorganización del directorio

**No hay reorganización estructural justificada.** El árbol actual
(`cli/` + `use_cases/` en app; `routers` + `middleware` + `auth` en api; `data/`
en research) ya está alineado con el marco. Las únicas consolidaciones internas
con beneficio técnico demostrable:

1. `apps/app/cli/_bootstrap.py` — helpers compartidos de los dos CLIs Hydra (H8: DRY
   real; ya divergieron de facto en H4). Nombrado por responsabilidad (`_bootstrap.py`,
   R14: se evita `shared`/`common`/`helper`).
2. `apps/app/use_cases/run_result.py` — `CycleRunResult` único (H12: dos dataclasses
   idénticas).
3. Eliminación de `_merge_config_into_args` (H1) — reduce el bridge no tipado,
   sin añadir capa.

No se mueven ficheros entre paquetes, no se crean capas nuevas y no se toca
`apps/api/` ni `apps/research/` (research queda como *research app*, H2 aceptada).

### 3.3 Clasificación de severidad (revisada)

**Críticos: 0 · Alto: 1 · Medio: 7 · Bajo: 5 · Corregir: 8 · No corregir: 5** (detalle en §4).
Revisión 6: H11 pasa de Bajo/Corregir a Bajo/No corregir → **Corregir: 8** (H1, H4, H6,
H7, H8, H9, H12, H13) · **No corregir: 5** (H2, H3, H5, H10, H11).

### 3.4 Concordancia con las reglas 7–19 (Revisión 4)

Contrato complementario al marco de Application Layer pura. Cada regla se contrasta
con los hallazgos y con las decisiones del plan:

| Regla | Impacto en hallazgos / plan |
|---|---|
| **R7 — Fail-Soft en runtime** | Refuerza **H7** (ya Corregir): los use cases devuelven `CycleRunResult` tipado y nunca abortan el proceso por excepciones esperadas. Coherente con el contrato "nunca lanza" declarado en `execute_*`. |
| **R8 — Composition Roots únicos** | `apps/` ya instancia infraestructura exclusivamente vía CR (`portfolio_service`, `CompositionRoot.assemble()`). Única excepción: `research/_storage_factory` (**H2**) — se mantiene como deuda documentada por decisión (R18). |
| **R9 — DRY con criterio** | Valida **H8**: extracción legítima por misma responsabilidad/ciclo/semántica (ambos CLIs Hydra), no por similitud textual. El helper `_bootstrap.py` se justifica por responsabilidad, no por texto repetido. |
| **R10 — KISS** | `_bootstrap.py` debe **reducir** LOC neto y complejidad (R17); si solo moviera código entre archivos, se rechaza. ~~La eliminación de `LiveEngineResources` (H11) simplifica el camino live.~~ → **Revisión 6:** H11 queda No corregir (objeto de ciclo de vida justificado; ver R15). |
| **R11 — DIP estricto** | Tensión directa con **H2** y **H3**. Decisión aprobada: se mantienen **No corregir** (research app; test-double de un puerto, no infraestructura). No aplica a los use cases de trading, que ya reciben `portfolio_service` y configs tipados. |
| **R12 — Event-Driven (solo puertos)** | Satisfecha en trading: `execute_*` conoce solo `FeatureSource`/`PortfolioService` vía CR — nunca Kafka/Redis/Iceberg/REST. `research` (H2) es la excepción documentada. |
| **R13 — Bounded Contexts** | ✅ Satisfecha: 44/44 contratos import-linter KEPT (BC-18/19/20/24/43/50). |
| **R14 — Naming** | Rechaza `helper`/`utils`/`common`/`manager`. Consecuencia: `_shared.py` → **`_bootstrap.py`** en §3.2/§9. Los módulos actuales de `apps/` cumplen (nombres por intención). |
| **R15 — Cada archivo justifica su existencia** | `run_result.py` (un dataclass) está justificado: 4 módulos lo consumen. **H11 (Revisión 6):** `LiveEngineResources` **sí está justificado** — no es redundante con `TradingRuntime` porque es el objeto que gestiona el ciclo de vida de recursos (pasado, presente `None`, futuro CCXT/websocket), una responsabilidad que `TradingRuntime` (dataclass de ensamblado) no cubre. |
| **R16 — Minimizar el árbol** | ✅ Solo 2 archivos nuevos (`_bootstrap.py`, `run_result.py`), sin carpetas nuevas. Sin nuevos BCs ni responsabilidades estables que justifiquen directorios. |
| **R17 — El resultado debe ser más pequeño** | Métrica de éxito de cada fase: LOC, dependencias, duplicación y complejidad ciclomática deben **bajar**; comportamiento idéntico. Aplica a H8 y H12. |
| **R18 — Todo cambio justifica su beneficio** | Plantilla obligatoria por cambio: ¿qué principio mejora / qué duplicación elimina / qué acoplamiento reduce / qué riesgo evita? H2/H3/H11 se mantienen porque el beneficio no justifica el costo. |
| **R19 — Sin arquitectura especulativa** | **Revisión 6 (propietario):** `LiveEngineResources` **no es** arquitectura especulativa — es un objeto de ciclo de vida con evolución prevista documentada (CCXT/websocket en roadmap event-driven) y origen legítimo (hallazgo 2.3, SafeOps). "No usado hoy" ≠ "sin uso previsto". Ver §5 (H11). |

### 3.5 Métricas objetivas (Revisión 5)

Medidas computadas con `ast` estándar sobre los 24 ficheros de `apps/` (complejidad
ciclomática McCabe, tamaño LOC por función/módulo, acoplamiento intra-app, ciclos).

**Resumen por módulo (LOC, funciones, clases, complejidad máxima):**

| Módulo | LOC | fn | cls | maxCC | Función peor |
|---|---|---|---|---|---|
| `app/cli/live_hydra.py` | 308 | 4 | 0 | **11** | `main` (136 LOC) |
| `app/cli/paper_hydra.py` | 310 | 4 | 0 | **9** | `main` (137 LOC) |
| `research/data/data_access.py` | 296 | 11 | 0 | **10** | `get_ohlcv` (52 LOC) |
| `app/cli/entrypoint.py` | 140 | 2 | 0 | 8 | `run` (91 LOC) |
| `app/cli/main.py` | 291 | 5 | 0 | 6 | `run_application` |
| `app/use_cases/execute_live.py` | 257 | 4 | 2 | 4 | `shutdown` |
| `app/use_cases/execute_paper.py` | 322 | 5 | 2 | 5 | `_probe_gold_data` |
| `api/*` | ≤154 | ≤5 | ≤1 | ≤3 | — |
| `research/notebooks/` | 10 | 0 | 0 | 0 | — |

**Funciones de alta complejidad (CC > 6) — priorizan el refactor:**

| Módulo | Función | CC | LOC |
|---|---|---|---|
| `app/cli/live_hydra` | `main` | 11 | 136 |
| `research/data/data_access` | `get_ohlcv` | 10 | 52 |
| `app/cli/paper_hydra` | `main` | 9 | 137 |
| `app/cli/entrypoint` | `run` | 8 | 91 |
| `app/cli/main` | `run_application` | 6 | ~50 |

**Acoplamiento (intra-`apps/`, directo):**

- **Mayor fan-out:** `api/main` → 5 (`deps`, `middleware/logging`, `middleware/rate_limit`,
  `routers/health`, `routers.__init__`).
- **Mayor fan-in:** `api/deps` ← 2 (`api/main`, `routers/health`): es el SSOT de DI de la
  API, correcto. `app/use_cases/execute_live` / `execute_paper` ← 1 (su CLI, correcto).
- **Ciclos de import:** **ninguno** entre módulos de `apps/`. DAG limpio, verificado con
  Tarjan SCC y sumado a los 44 contratos import-linter KEPT.

**Lectura de priorización (cruce con §7 [reordenado]):**

- Las 3 funciones de mayor complejidad son precisamente los entrypoints de trading
  `live_hydra.main` (CC 11) y `paper_hydra.main` (CC 9) + el orchestrator de datos
  `entrypoint.run` (CC 8). Confirmar que **H1/H8 (config tipada + `_bootstrap.py`)** son
  la palanca de mayor reducción de complejidad ciclomática en `apps/`.
- `data_access.get_ohlcv` (CC 10, 52 LOC) concentra la complejidad de research; asumida
  como *research app* (H2, No corregir), no requiere acción salvo la tipificación (H9).
- `execute_live.shutdown` (CC 4) ya es trivial — el loop de cierres aislados de H11
  es de bajo costo; H11 se mantiene No corregir (Revisión 6).
- Acyclicidad confirmada: no hay riesgo de dependencias cíclicas en `apps/`; el único
  acoplamiento notable es el esperado (CLIs → use cases; API → `deps`).

### 3.6 Matriz validada por hallazgo (Revisión 6 — código como fuente de verdad)

Cada hallazgo se validó contra el código real del snapshot (idéntico al working tree),
verificando existencia, flujo de ejecución, consumidores y coherencia con ADR/contratos.
Métricas de duplicación recomputadas: `main` live/paper **0.67** de similitud token,
`_build_parser` **0.69**, `execute` live/paper **0.70**, 62 líneas no-comentario
compartidas; `mypy apps/` re-ejecutado = **7 errores en 3 ficheros** (exacto).

| Hallazgo | Evidencia encontrada | Riesgo real | Beneficio esperado | Costo | Riesgo regresión | Prioridad | Decisión |
|---|---|---|---|---|---|---|---|
| **H1** SSOT config | `live_hydra.py:127-156`/`paper_hydra.py:128-153` (`_merge_config_into_args`) descartan `AppConfig`→`Namespace`; `execute_live.py:165-186`/`execute_paper.py:178-199` reconstruyen `TradingConfig`/`RiskConfig`; `max_order_usd=capital×max_risk_pct` duplicado | Cambios en `AppConfig.risk/trading` no llegan al camino de trading; contrato `Namespace` no tipado | SSOT restaurado; contrato tipado (mypy valida) | Medio (4 ficheros + 2 use cases + `_bootstrap.py`) | Medio-bajo (borde CLI, no dominio) | **1 (F1)** | **Corregir** |
| **H2** DIP research | `data_access.py:40-41,69` importa/instancia `GoldReader`+`IcebergStorageFactory`; 0 consumidores de `research` | Bajo (paquete de notebooks) | Mínimo (puertos sin consumidor) | Medio | Bajo | — | **No corregir** |
| **H3** SyntheticDataSource | `execute_paper.py:71-136,208-222`; fake documentado de `FeatureSource`, solo dry-run; 0 tests lo importan | Nulo (marker de prohibición; live lo excluye) | Moverlo a tests crearía dependencia runtime | Medio | Bajo | — | **No corregir** |
| **H4** `min_order_usd` | `live_hydra.py:148` lo setea desde config; `paper_hydra.py` **no** → `execute_paper.py:196` usa fallback `10.0` siempre | Órdenes paper con umbral distinto del aprobado | Muere naturalmente con H1 | ~0 (incremental sobre H1) | Bajo | **1 (F1)** | **Corregir** |
| **H5** rate limit IP | `rate_limit.py:44` `request.client.host`; sin `X-Forwarded-For`; fail-soft `:78-80` | Bajo hoy (sin proxy); degrada todos tras proxy | Documentar limitación | ~0 | Bajo | — | **No corregir** (solo doc) |
| **H6** probes no excluidas | `health.py:15` afirma exclusión por `_SILENT_PATHS`, pero `rate_limit.py:43-82` no filtra rutas (solo `logging.py:22,29`) | 429 en liveness/readiness bajo carga | Probes estables; SSOT compartido | Bajo (filtro path) | Bajo | **2 (F2)** | **Corregir** |
| **H7** "nunca lanza" | `execute_live.py:248-249`/`execute_paper.py:311-312`: `summarize` + lectura de trades **fuera** del `try` | Fallo de analytics derriba el CLI sin `RunResult` | Contrato cumplido | Bajo | Bajo | **3 (F3)** | **Corregir** |
| **H8** DRY CLI | 62 líneas compartidas; `main` 0.67, `_build_parser` 0.69; los únicos 2 puntos CC>10 son estos `main` (15) | Cada cambio de config/rendering ×2; drift ya visible (H4) | Un `_bootstrap.py`; menor CC | Medio | Medio (capital real — mitigado) | **2 (F2)** | **Corregir** |
| **H9** mypy | `uv run mypy apps/` re-ejecutado: 7 errores en 3 ficheros (data_access, health, main) | Frontera pandas↔polars sin tipar | Green en frontera de migración | Bajo (anotar + awaits) | Bajo | **1 (F1)** | **Corregir** |
| **H10** doble lectura | `execute_paper.py:217` probe descarta df; `engine.py:137→211` relee Gold | Bajo (1 scan extra por ciclo CLI) | Ahorro marginal; acopla probe y engine | Medio | Bajo | — | **No corregir** |
| **H11** LiveEngineResources | Ver §5 (H11) Revisión 6 | Mínimo (no-op hoy) | Eliminarlo no simplifica: reubica responsabilidad | Bajo | Bajo | — | **No corregir** (propietario) |
| **H12** RunResults duplicados | `execute_live.py:49-66`/`execute_paper.py:46-63` idénticas (hash de `exit_code` igual) | 2 fuentes para un contrato | Un `CycleRunResult` | Bajo | Bajo | **2 (F2)** | **Corregir** |
| **H13** docs obsoletas | `app/__init__.py:13-20` refs a `live.py`/`paper.py`/`market_data.py`/`rebalance.py` (eliminados `4e5f53b`); `use_cases/__init__.py:16`; `api/main.py:10-12` orden LIFO invertido | Doc describe un árbol inexistente | Doc = realidad | Nulo | Bajo | **3 (F3)** | **Corregir** |

**Observaciones validadas (Revisión 6):** **O1** confirmada (no existe `tests/api/`).
**O2 descartada** (corrección C2: ningún test usa `_SEED`). **O3** confirmada
(`notebooks/` vacío, sin acción). **O4** confirmada (`data_access.py:66-67` lee
`os.environ` en import-time; aceptable para notebooks).

---

## 4. Tabla resumen de hallazgos

| # | Severidad | Decisión | Área | Hallazgo | Evidencia |
|---|---|---|---|---|---|
| H1 | **Alto** | Corregir | SSOT · DIP | `TradingConfig`/`RiskConfig` se reconstruyen desde `argparse.Namespace` ignorando `AppConfig.trading`/`risk`; el `AppConfig` cargado se descarta en un bridge CLI→Namespace | `execute_live.py:126-186`, `execute_paper.py:144-199`, `live_hydra.py:127-156`, `paper_hydra.py:128-142` |
| H2 | Medio | **Corregido (F-1, 2026-08-14)** | DIP · Clean | `research` importaba adaptadores concretos (`IcebergStorageFactory`, `GoldReader`) en lugar de `StorageFactoryPort`/`FeatureSource` | `research/data/data_access.py:40-41,69` → resuelto vía composition root + BC-55 |
| H3 | Medio | **No corregir** | Clean · KISS | `SyntheticDataSource` (test-double numpy/pandas) vive en la capa de aplicación y decide el `FeatureSource` en runtime | `app/use_cases/execute_paper.py:71-136,208-222` |
| H4 | **Medio** | Corregir | SSOT · Fail-Fast | `getattr(args, "min_order_usd", 10.0)` silencioso; paper ignora `config.risk.order.min_order_usd` mientras live lo usa | `execute_paper.py:196`, `execute_live.py:183`, `live_hydra.py:148`, `paper_hydra.py:128-153` |
| H5 | Medio | **No corregir** (documentar) | Resiliencia | Rate limit por `request.client.host` colapsa todos los clientes tras un reverse proxy (sin `X-Forwarded-For`) | `api/middleware/rate_limit.py:43-44`, `api/main.py:112-117` |
| H6 | **Medio** | Corregir | Robustez | `/health` y `/ready` no están excluidos del rate limit aunque el doc afirma que sí | `api/routers/health.py:15-16`, `api/middleware/logging.py:22`, `api/middleware/rate_limit.py` |
| H7 | **Bajo** | Corregir | SafeOps | `PerformanceEngine.summarize` fuera del `try`; `execute_*` no cubre todo el flujo con su contrato "nunca lanza" | `execute_live.py:248-249`, `execute_paper.py:313-314` |
| H8 | **Medio** | Corregir | DRY | Duplicación funcional real: ensamblado de config (live/paper) y scaffolding CLI (live_hydra/paper_hydra) | H1 + `live_hydra.py:164-304`, `paper_hydra.py:161-306` |
| H9 | **Medio** | Corregir | Tooling | `uv run mypy apps/` rojo: 7 errores en 3 ficheros | `data_access.py:100,253-269`, `api/main.py:67`, `api/routers/health.py:71` |
| H10 | **Bajo** | Corregir (opcional) | Eficiencia | Doble lectura completa del Gold: `_probe_gold_data` + `engine._load_data` | `execute_paper.py:230-275`, `engine.py:211-221` |
| H11 | Bajo | **No corregir** (Revisión 6, propietario) | KISS | `LiveEngineResources` con campos placeholder `None` y loop de shutdown no-op — objeto de ciclo de vida justificado por origen (hallazgo 2.3, SafeOps) y evolución prevista; no es código muerto (ver análisis §5) | `execute_live.py:74-119` |
| H12 | **Bajo** | Corregir | DRY | `LiveRunResult`/`PaperRunResult` dataclasses idénticas | `execute_live.py:49-66`, `execute_paper.py:46-63` |
| H13 | **Bajo** | Corregir | Docs | Docstrings obsoletos (refs a `live.py`/`paper.py`/`rebalance.py` eliminados) y comentario de orden de middleware erróneo | `app/__init__.py:13-20`, `app/use_cases/__init__.py:16`, `api/main.py:10-12` |

**Observaciones (no numeradas):** `api` sin suite de tests; `research/notebooks/` vacío. ~~`SyntheticDataSource._SEED=42` acoplada con `tests/`~~ → **descartada (C2, Revisión 6)**: ningún test referencia `_SEED`; no hay acoplamiento real.

---

## 5. Hallazgos detallados

### H1 — (Alto) La configuración de trading se reconstruye, no se consume (SSOT roto)

**Evidencia concreta.**

`ocm/config/schema.py` ya define y expone los modelos que los use cases reconstruyen a mano:

- `TradingConfig` — `ocm/config/schema.py:540-555` (`strategy_name`, `strategy_cfg`, `capital_usd`, `exchange`, `market_type`).
- `RiskConfig` — `ocm/config/schema.py:711-721` (`position`, `stop_loss`, `drawdown`, `order`).
- `AppConfig` los expone como `trading:` y `risk:` — `ocm/config/schema.py:759-761`.

Sin embargo, el camino de trading es:

1. `live_hydra.py` / `paper_hydra.py` cargan `AppConfig` completo (`load_appconfig_standalone`, `live_hydra.py:198`, `paper_hydra.py:200`).
2. Ambos lo **descartan** en `_merge_config_into_args`, convirtiéndolo en un `argparse.Namespace` no tipado (`live_hydra.py:127-156`, `paper_hydra.py:128-142`).
3. Los use cases vuelven a **construir desde cero** `TradingConfig` + `RiskConfig` desde ese `Namespace`:
   - `execute_live.py:165-186` (`trading_cfg = TradingConfig(...)`, `risk_cfg = AppRiskConfig(...)`).
   - `execute_paper.py:178-199` (idéntico).

Es decir: **doble traducción** `AppConfig → Namespace → TradingConfig`, y dos fuentes de verdad para la misma configuración de negocio.

**Impacto técnico.**

- Si `AppConfig.trading` o `AppConfig.risk` cambian (p. ej. `market_type`, `max_position_pct`), los CLIs de trading **no lo reflejan**: la configuración aprobada por el pipeline Hydra L1-L5 se ignora silenciosamente en el camino de trading.
- `max_order_usd = capital * max_risk_pct` se deriva en dos archivos con la misma fórmula, duplicando una regla de negocio.
- El contrato use case↔CLI es un `Namespace` no tipado: mypy no puede validar campos, y un cambio de nombre rompe en runtime.

**Solución propuesta.**

Que los use cases consuman directamente objetos tipados `TradingConfig` y `RiskConfig`. Los flags CLI sin modelo en `AppConfig` (`--symbol`, `--timeframe`, `--strategy`, `--fast/--slow`, `--min-confidence`) se fusionan en el CLI mediante `config.trading.model_copy(update={...})`. El use case deja de importar `argparse`.

**Justificación.**

- Restaura `AppConfig` como única fuente (SSOT), elimina la duplicación DRY y vuelve tipado el contrato (DIP/mypy).
- No contradice el diseño aprobado: **ADR-0003** define el constructor angosto precisamente recibiendo `TradingConfig` + `RiskConfig` ya tipados.
- **Corrección C1 (Revisión 6):** la Revisión 5 citó *"ADR-0005 (Consecuencias): execute_paper.py execute() espera argparse.Namespace (Fase 3 lo reemplaza por AppConfig directo vía PortfolioCompositionRoot)"*. **Esa frase no existe** en ADR-0005 (ni versión actual ni `git show 2fe8f5a`) ni en el forense de composition roots; su única fuente es el comentario de código `paper_hydra.py:123-125`. Además ADR-0005 está **reemplazado por ADR-0012** (2026-08-03). La evidencia de código del hallazgo (doble traducción `AppConfig→Namespace→TradingConfig`) **es real y verificada**; solo era incorrecto el respaldo documental citado.

---

### H2 — (Medio) `research` depende de adaptadores concretos, no de puertos (DIP)

**Decisión inicial: No corregir (deuda aceptable).** Según el criterio de *Research* del
marco, no se aplica el mismo nivel de abstracción a `apps/research` sin beneficio
claro. `IcebergStorageFactory` actúa como el de-facto composition root del paquete
de notebooks; inyectar `StorageFactoryPort`/`FeatureSource` añadiría abstracción
sin un consumidor que la justifique. Revisar solo si `research` pasa a ser servicio.

**Estado 2026-08-14 (F-1): RESUELTO.** La deuda se pagó con la Opción C de la
auditoría F-1: composition root real de research + data_access sobre contracts.

**Evidencia concreta (histórica, pre-F-1).**

`apps/research/data/data_access.py:40-41` importaba:

```python
from market_data.adapters.outbound.storage.gold_reader import GoldReader
from market_data.adapters.outbound.storage.iceberg_factory import IcebergStorageFactory
```

y `:69` instanciaba `_storage_factory = IcebergStorageFactory()` a nivel de módulo.

Existen puertos que cumplen exactamente este contrato:

- `OHLCVStorage` / `StorageFactoryPort` — `packages/market_data/ports/outbound/storage.py:37,270`.
- `FeatureSource` — `shared/contracts/boundaries.py:31` (implementado estructuralmente por `GoldReader`).

`import-linter` **no** prohibía este import (BC-20 solo fija la dirección: research no importa trading/portfolio/app/api/infrastructure), por lo que era un hueco DIP/Clean, no una violación de contrato.

**Impacto técnico (histórico).**

- `research` quedaba acoplado al detalle de implementación del backend de storage. Un cambio de backend (p. ej. un nuevo factory Iceberg con catálogo) rompía `research` aunque su contrato no cambiara.
- Los tests se veían obligados a patchear la **instancia concreta** (`_storage_factory.get_storage`, `tests/research/test_data_access.py:93`), acoplados a internos del módulo.

**Solución aplicada (F-1, 2026-08-14).**

1. `apps/research/data/composition_root.py` (nuevo) — único punto de research que
   conoce los adapters concretos (`IcebergStorageFactory`, `GoldReader`); expone
   `build_storage_factory()` (→ `StorageFactoryPort`) y `build_feature_reader()`
   (→ `FeatureReaderPort`).
2. `apps/research/data/data_access.py` — deja de importar adapters concretos;
   depende de `StorageFactoryPort` (canónico, `market_data.ports.outbound.storage_factory`)
   y `FeatureReaderPort`; el singleton module-level de concreto se sustituye por un
   seam tipado contra el port con default del composition root; se elimina el acceso
   a `_storage_factory._cache`.
3. `tests/research/test_data_access.py` — sustituye patches de la instancia concreta
   por fakes de `StorageFactoryPort`/`FeatureReaderPort` inyectados en el seam.
4. `architecture/importlinter.toml` — nuevo **BC-55**: research importa
   `market_data.adapters`/`market_data.infrastructure` SOLO desde
   `research/data/composition_root.py` (verificado: detecta violación artificial).

**Arquitectura resultante.**

```
research.data.data_access          (importa ports + composition_root)
        ↓  StorageFactoryPort / FeatureReaderPort
research.data.composition_root     (único importador de adapters concretos)
        ↓
IcebergStorageFactory / GoldReader (market_data.adapters)
```

**Justificación.**

- Restaura DIP: research depende de contracts, no de implementaciones; los tests
  mockean el contrato. Coherente con el resto del repo (application→ports, y solo
  los composition roots instancian concretos — patrón BC-38/BC-50).
- **Clasificación previa:** mejora futura para paquete de notebooks. La decisión de
  pagarla (2026-08-14) se tomó al formalizar research como consumidor del gold layer
  con composition root propio.

---

### Corolario F-6 — (2026-08-14) Imports dinámicos de `market_data` fuera del composition root de trading

**Estado: RESUELTO (extensión de contratos de arquitectura).** Complementa la frontera
BC-50 (trading importa `market_data` solo desde `trading/bootstrap/composition_root.py`)
cubriendo el hueco de los imports **dinámicos** con literal string, que el grafo estático
de import-linter y el test AST de BC-50 no veían.

**Qué se añadió.** Detector AST en `tests/architecture/test_import_contracts.py`:
`_dynamic_market_data_targets()` recorre los nodos de cada archivo de `packages/trading/`
y detecta `importlib.import_module("market_data.adapters...")`,
`importlib.import_module("market_data.infrastructure...")`, `__import__("market_data.adapters...")`
y `__import__("market_data.infrastructure...")` **solo cuando el primer argumento es un
literal string** (`ast.Constant`), en cualquier archivo de trading salvo el composition
root autorizado. Los tests de la clase `TestDynamicMarketDataDetector` (6 casos) demuestran:
detección de los 4 patrones, no-detección de imports internos legítimos de trading
(`registry.py` → `trading.strategies.ema_crossover`), no-detección de stdlib, y exclusión
explícita del composition root autorizado.

**Fuera de alcance (documentado en el propio detector).** Nombres dinámicos construidos con
variables o f-strings no se detectan — requeriría análisis semántico con ejecución, pertenece
a otro nivel de análisis. No se amplió el detector a esos casos para no introducir falsos
positivos.

---

### Deuda técnica registrada — (2026-08-14) `StorageFactoryPort` duplicado

**No corregida (fuera de alcance de F-1).** Existen dos definiciones del mismo port:

- `packages/market_data/ports/outbound/storage_factory.py:40` — **canónica real**, usada por
  6 consumidores (`main.py`, `resample_ohlcv.py`, `adapters/outbound/storage/__init__.py`,
  `apps/research/data/*`, tests).
- `packages/market_data/ports/outbound/storage.py:270` — **duplicado sin consumidores**,
  semánticamente idéntico.

**Riesgo de mantener ambas:** dos "SSOT" del mismo contrato; si una evoluciona sin la otra,
el chequeo estructural (`isinstance`/runtime_checkable) y el tipado divergen silenciosamente.

**Decisión.** Consolidar la definición en `storage_factory.py` y eliminar la duplicada de
`storage.py` toca contratos de import y archivos no necesarios para F-1. Queda como deuda
técnica documentada; se consolidará en un refactor propio.

---

### H3 — (Medio) Test-double `SyntheticDataSource` en la capa de aplicación

**Decisión: No corregir (deuda aceptable).** Es un *fake* de un port (`FeatureSource`),
no lógica de dominio; está contenido, documentado y solo activo en `dry_run`.
Moverlo a `tests/` crearía dependencia runtime de un paquete de tests, y extraerlo
a una subcapa nueva violaría KISS. Mejora opcional futura: inyectarlo desde el CLI.

**Evidencia concreta.**

`apps/app/use_cases/execute_paper.py:71-136` define `SyntheticDataSource` (genera un DataFrame OHLCV+features con `numpy`/`pandas`, seed fijo 42) y `build_paper_engine` decide en runtime qué `FeatureSource` usar (`:208-222`):

```python
if args.dry_run:
    data_source = SyntheticDataSource()
else:
    data_source = root.build_gold_data_source()
    _probe_gold_data(data_source, args)
```

**Impacto técnico.**

- El caso de uso (capa de aplicación) contiene una implementación de fuente de datos (I/O, contrato de dominio) y la lógica de selección real/sintético.
- `SyntheticDataSource` es código muerto fuera de `dry_run`, pero viaja en el artefacto de producción.
- Acopla la capa de aplicación a una implementación concreta del contrato `FeatureSource`.

**Solución propuesta.**

Inyectar el `FeatureSource` desde el CLI/Composition Root, igual que ya se inyecta `portfolio_service`. El use case solo recibe `data_source`. El double sintético se mueve a `tests/` (o `infrastructure/`) como colaborador del modo `dry_run`.

**Justificación.**

- Devuelve a la capa de aplicación su rol puro de orquestación (Clean Architecture) y replica el patrón de inyección que ya usa el resto de `apps/`.
- Mantener `_SEED = 42` como SSOT compartida con `tests/` (ver Observación O2).

---

### H4 — (Medio) Fallback silencioso `min_order_usd` y divergencia paper/live

**Evidencia concreta.**

- `execute_paper.py:196` y `execute_live.py:183`:

```python
min_order_usd=getattr(args, "min_order_usd", 10.0),
```

- `live_hydra.py:148` **sí** setea `merged.min_order_usd = config.risk.order.min_order_usd`.
- `paper_hydra.py:128-142` **no** lo setea → paper siempre usa `10.0`, ignorando `config.risk.order.min_order_usd`.

**Impacto técnico.**

- Un operador que configure `risk.order.min_order_usd` y corra `uv run paper` verá su valor **silenciosamente ignorado**. Fallo combinado de SSOT y Fail-Fast: el default enmascara la ausencia del valor real.
- Paper y live divergen en una regla de negocio idéntica sin que ningún log lo advierta.

**Solución propuesta.**

Eliminar `getattr(args, ...)`. Derivar `min_order_usd` de la fuente única en ambos CLIs (como ya hace live) y hacer el campo obligatorio en el use case cuando el camino lo requiera. Si un caller no lo provee, fallar con mensaje accionable en lugar de asumir.

**Justificación.** Fail-Fast real: ausencia de configuración ≠ default tácito. Alinea paper con el comportamiento ya correcto de live y restaura el SSOT de `risk.order`.

---

### H5 — (Medio) Rate limit por `request.client.host` (problema detrás de proxy)

**Decisión: No corregir ahora — documentar.** No hay reverse proxy en el despliegue
actual; el fix real (`ProxyHeadersMiddleware`) depende de una decisión de
infraestructura. Acción mínima: documentar la limitación en el docstring del
middleware. Corregir cuando exista proxy.

**Evidencia concreta.**

- `api/middleware/rate_limit.py:43-44`: `ip = request.client.host`; clave `rl:{ip}`.
- `api/main.py:112-117`: solo se registran `RateLimitMiddleware` y `RequestLoggingMiddleware`. No hay `ProxyHeadersMiddleware`, ni lectura de `X-Forwarded-For`.

**Impacto técnico.**

- Detrás de un reverse proxy / load balancer (configuración típica de producción), todo el tráfico comparte la IP del proxy:
  - O el rate limit es **global** (un solo cliente legítimo o un burst alcanza el tope y bloquea a todos),
  - o el límite es inefectivo como protección por cliente.
- El `host: 0.0.0.0` de `settings.py:95` (hallazgo bandit B104, esperado) implica que el API expone en todas las interfaces — el rate limit por IP es la única barrera por cliente, y está rota bajo proxy.

**Solución propuesta.**

- Si el proxy es de confianza: configurar `ProxyHeadersMiddleware` (Starlette) y documentar que la confianza del header debe establecerse en infraestructura.
- Alternativa robusta: clave de rate limit por (`X-Forwarded-For` de primer hop) o por identidad autenticada cuando exista auth.
- Documentar en el docstring del middleware que sin header de confianza el límite es per-proxy.

**Justificación.** Fail-Fast/SafeOps en la frontera de exposición pública: el rate limiting debe ser un control real, no una ilusión bajo el proxy típico de producción.

---

### H6 — (Medio) Health checks no excluidos del rate limit (inconsistencia)

**Evidencia concreta.**

- `api/routers/health.py:15-16` afirma: *"Sin auth … Sin rate limit — el middleware los excluye (`_SILENT_PATHS`)"*.
- Pero `_SILENT_PATHS` (`api/middleware/logging.py:22`) solo lo usa `RequestLoggingMiddleware`. `RateLimitMiddleware` **no** excluye ningún path.

**Impacto técnico.**

- `/health` y `/ready` están sujetos al rate limit. Un `HEALTHCHECK` de Docker o sondeo de load balancer con frecuencia superior a `rate_limit_rpm` (default 60/min) recibiría `429`, y un nodo sano podría salir de rotación por un falso negativo del readiness.
- El docstring y el código describen comportamientos opuestos (SSOT de documentación roto).

**Solución propuesta.**

Excluir `/health`, `/ready`, `/metrics` también del rate limit (mismo set `_SILENT_PATHS`, extraído a un SSOT compartido) o corregir el docstring. La exclusión de las probes es la opción operativamente correcta: las probes de infraestructura no representan carga de cliente.

**Justificación.** Fail-Operative en la ruta de observabilidad: las probes no deben poder degradar la rotación del nodo. Elimina la contradicción doc↔código.

---

### H7 — (Bajo) SafeOps: `execute_*` no cubre todo su flujo con el contrato "nunca lanza"

**Decisión: Corregir** (Bajo). Riesgo real bajo (función pura), pero el contrato
del punto de entrada queda roto por religión de call-site, no por la API. Corrección
barata y sin efecto observable.

**Evidencia concreta.**

- `execute_paper.py:307-322`: el `try` envuelve `run_once()`; pero `trades = runtime.tracker.closed_trades` y `PerformanceEngine.summarize(...)` quedan **después del try**, fuera de protección.
- `execute_live.py:236-257`: idéntico; y el `finally` de `resources.shutdown()` solo cubre `run_once`.

**Impacto técnico.**

- El contrato documentado del módulo ("SafeOps: nunca lanza — errores retornados en `PaperRunResult`/`LiveRunResult`") no se cumple para el tramo de analytics. Si `summarize` lanzara (hoy es puro y no lanza — `trading/analytics/performance.py:106`), el CLI `paper_hydra`/`live_hydra` debería capturar una excepción no contemplada en su flujo.
- El riesgo real es bajo (función pura), pero el contrato del punto de entrada queda roto por religión de call-site, no por la API.

**Solución propuesta.**

Mover `summarize` y la lectura de `closed_trades` dentro del bloque protegido, o envolver el tramo de analytics en su propio `try/except` que devuelva `RunResult(success=False, ...)`. Para live, incluir `resources.shutdown()` en el `finally` que ya existe (ya es así) pero ampliando el `try` al bloque de analytics.

**Justificación.** Robustez en el punto de entrada: el CLI depende de que `execute()` nunca propague. Centraliza el manejo de errores donde el use case lo declara.

---

### H8 — (Medio) DRY: duplicación funcional real en use cases y CLIs

**Evidencia concreta.**

- Ensamblado de `TradingConfig` + `RiskConfig` idéntico (~30 líneas) en `execute_live.py:165-186` y `execute_paper.py:178-199` (raíz: H1).
- Scaffolding CLI casi idéntico entre `live_hydra.py:169-304` y `paper_hydra.py:170-306`:
  - `_handle_sigterm` idéntico (`live_hydra.py:164-167` vs `paper_hydra.py:161-167`).
  - Mismo patrón `signal.signal(SIGTERM, ...)`, `logger.remove()/add(...)`, carga de config con `try/except (ConfigurationError, ConfigValidationError)`.
  - Bloque de logging de resultado (loop de órdenes + resumen de performance) duplicado con diferencias menores (sharpe/profit factor solo en paper).

**Impacto técnico.**

- Dos lugares que deben cambiar a la vez ante cualquier cambio en el flujo de resultado de un ciclo (exit codes, formato de log, manejo de señales). El riesgo de divergencia es real: ya divergieron en `min_order_usd` (H4).
- No es similitud sintáctica: es la misma secuencia de operaciones (parsear → config → ensamblar root → ejecutar → loguear → exit code).

**Solución propuesta.**

- El fix de H1 elimina la duplicación de ensamblado de config (los use cases reciben config tipada).
- Para el CLI: extraer un helper compartido en `apps/app/cli/` (p. ej. `_setup_logging`, `_handle_sigterm`, `_log_engine_result(engine_result, performance, open_positions)`), consumido por ambos Hydra CLIs.

**Justificación.** Consolidación de la única fuente del flujo de entrypoint Hydra, sin contradecir ADR-0005 (ambos CLIs son hermanos por diseño). Beneficio demostrable: elimina la divergencia ya ocurrida en H4 y reduce ~60 LOC duplicadas.

---

### H9 — (Medio) `uv run mypy apps/` rojo (7 errores)

**Evidencia concreta.** `apps/` está dentro del scope de mypy (solo excluye `tests/` y `.venv/` — `pyproject.toml:293`).

1. `apps/research/data/data_access.py:100` — `pl.from_pandas(df)` sobre `object`; `_ensure_polars` sin anotación de retorno.
2. `apps/research/data/data_access.py:253,269` — `df` inferida como `pd.DataFrame` y reasignada a `pl.DataFrame` (`Incompatible types in assignment`).
3. `apps/research/data/data_access.py:258,260` — `df.filter(pl.col("timestamp") >= ...)` tipado contra el stub de pandas (`filter` no acepta `Expr`).
4. `apps/api/main.py:67` y `apps/api/routers/health.py:71` — `await redis.ping()` contra stub `Awaitable[bool] | bool`.

**Impacto técnico.**

- `uv run mypy .` no es reproducible en verde, aunque mypy no está en el gate pre-push (ruff + import-linter + pytest). La migración pandas→polars del commit `4ea4a86` dejó incoherencias de tipos sin resolver en `data_access.py`.

**Solución propuesta.**

- Tipar `_ensure_polars(...) -> pl.DataFrame` y anotar explícitamente `df` en `get_features` como `pl.DataFrame` tras la conversión.
- En `get_features`, convertir `start_dt`/`end_dt` y filtrar con polars (ya es polars tras `_ensure_polars`).
- Para `ping()`: capturar el resultado como `bool` (`result = await redis.ping(); assert result is True` con comentario de `type: ignore`, o castear `# type: ignore[arg-type]` con comentario justificativo, conforme a la política de `type: ignore` con explicación del repo).

**Justificación.** Vuelve el toolchain a verde y deja la frontera pandas→polars de research bien tipada (SSOT de la conversión).

---

### H10 — (Bajo) Doble lectura completa del Gold en paper trading

**Evidencia concreta.**

- `execute_paper.py:230-275` `_probe_gold_data` ejecuta `data_source.load_features(...)` completo (lectura integral Iceberg) solo para validar disponibilidad.
- `engine.run_once` → `engine._load_data` (`engine.py:211-221`) ejecuta `load_features(...)` **de nuevo** completo.

**Impacto técnico.** Trabajo duplicado: dos lecturas integrales de la misma ventana de datos en cada ciclo no-dry. I/O innecesario en el camino caliente de paper.

**Solución propuesta.**

- Que `_probe_gold_data` reutilice el DataFrame ya cargado inyectándolo como `data_source` cache (el engine ya acepta un `FeatureSource` arbitrario), o
- limpiar el doble `load` guardando el df del probe y pasándolo al engine (p. ej. un pequeño wrapper que devuelve el df ya cargado la primera vez).

**Justificación.** Eficiencia demostrable (una lectura en lugar de dos) sin tocar el contrato `FeatureSource`.

---

### H11 — (Bajo) Recursos placeholder en `LiveEngineResources`

**Decisión (Revisión 6 — propietario): No corregir (deuda aceptable).**
Reclasificado de **"Corregir" → "No corregir"** tras la validación contra el código
real. El propietario es más conservador que el agente: **no se elimina un objeto de
ciclo de vida preparado para gestionar recursos solo porque hoy varios campos sean
`None`**. Hay una diferencia material entre *código muerto* y *objeto de ciclo de vida
preparado para recursos*. Se exige demostrar: (a) ¿hay ADR que justifique su
existencia? (b) ¿está previsto incorporar recursos ahí? (c) ¿eliminarlo realmente
simplifica o solo mueve responsabilidades?

**Demostración exigida (respondida con código + historial + ADRs):**

1. **¿Hay ADR que lo justifique?** Su origen es un hallazgo formal: commit `6f4ff38`
   (2026-08-01) resuelve el hallazgo **2.3** de `docs/audits/2026-08-composition-root-audit.md`
   (SafeOps: cierre ordenado de recursos ante SIGINT/SIGTERM en capital real). En ese
   momento `LiveEngineResources.shutdown()` cerraba un recurso **real** (la conexión
   Redis que `build_live_engine()` construía internamente). No es especulación a priori:
   nació para cumplir una garantía de SafeOps con un recurso concreto.
2. **¿Está previsto incorporar recursos ahí?** Sí — la evolución prevista está
   documentada en el propio código (`execute_live.py:83-88`): `exchange_client`,
   `kafka_producer` y `metrics_server` son **placeholders declarados explícitamente**
   "para cuando esos recursos existan de verdad", para que `shutdown()` no deba
   reescribirse. El motor live está en evolución activa: `LiveExecutor` es un **STUB**
   con lógica CCXT real comentada (`packages/trading/execution/live_executor.py:22,77,95,130`)
   y la integración CCXT/websocket está en el **roadmap event-driven** (ADR-0002/Kappa;
   `RiskGateConsumer` es contrato adelantado documentado en `docs/DOMAIN.md:120`). Los
   placeholders no son arbitrarios: corresponden a los recursos que ese roadmap declara.
3. **¿Eliminarlo simplifica o solo mueve responsabilidades?** **Solo mueve
   responsabilidades.** El wrapper hoy delega el único recurso real (Redis) a
   `PortfolioCompositionRoot` (`execute_live.py:196-203`, cierre en `live_hydra.py:244`),
   y `shutdown()` itera campos `None` (`:106-118`) — no hace daño, pero **sí preserva la
   frontera** por donde entrarán `exchange_client`/`kafka_producer`/`metrics_server`.
   Eliminarlo devolvería `TradingRuntime` directo y, cuando CCXT/websocket se materialice,
   **habría que reintroducir** el mismo ciclo de vida en el use case o en el root —
   ninguna ganancia neta, un riesgo de regresión al reintroducir.

**Evidencia concreta.**

`execute_live.py:74-119`: `exchange_client`, `kafka_producer`, `metrics_server` son
`None` hoy; `redis_client` se fija a `None` (`:202`) porque la conexión la posee
`PortfolioCompositionRoot`. `shutdown()` itera un loop de cierres aislados
(`:106-118`, SafeOps: el fallo de uno no bloquea el resto).

**Impacto técnico.** Nulo en runtime (no-op); costo cognitivo mínimo (objeto de 45 LOC
con ciclo de vida explícito y documentado). No contradice R19: **no es arquitectura
especulativa sin plan** — es un objeto de ciclo de vida cuya evolución está declarada
en el código y el roadmap.

**Solución propuesta (descartada por decisión del propietario).** Mantener tal cual.
Si en el futuro los placeholders se materializan (CCXT/websocket), el ciclo de vida de
la conexión debe **seguir** el patrón del root (R8), sin reintroducir indirección en el
use case — pero esa decisión se toma cuando exista la pieza, no hoy.

**Justificación.** Criterio del propietario: eliminar código solo porque sus campos
son `None` confunde "no usado hoy" con "sin uso previsto". El objeto cumple una
función declarada de gestión de ciclo de vida con evolución prevista; su costo de
mantenimiento es trivial. **H11 se retira del plan de remediación (§9, Fase 4).**

---

### H12 — (Bajo) `LiveRunResult` / `PaperRunResult` duplicados

**Evidencia concreta.**

`execute_live.py:49-66` y `execute_paper.py:46-63` definen dataclasses idénticas (campos `success`, `error`, `engine_result`, `performance`, `open_positions`, `oms_summary` + propiedad `exit_code`).

**Impacto técnico.** Dos tipos para el mismo contrato; un cambio en el resultado de un ciclo debe propagarse a dos archivos. Los CLIs ya los consumen de forma idéntica (`live_hydra.py:246-252` ≈ `paper_hydra.py:244-250`).

**Solución propuesta.** Un único `RunResult` tipado en `apps/app/use_cases/` (p. ej. `CycleRunResult`) con el campo `engine_result` genérico tipado como `EngineResult`.

**Justificación.** Elimina duplicación real sin cambiar el contrato externo.

---

### H13 — (Bajo) Documentación obsoleta y comentario de middleware erróneo

**Evidencia concreta.**

- `apps/app/__init__.py:13-20` referencia `cli/live.py`, `cli/paper.py`, `cli/market_data.py` y `use_cases/rebalance.py` — todos eliminados el 2026-08-03 (commit `4e5f53b`).
- `apps/app/use_cases/__init__.py:16` referencia `rebalance.py` inexistente.
- `apps/api/main.py:10-12` documenta *"primero registrado = más externo"*: es falso en Starlette (el **último** registrado es el más externo). El orden real del código es correcto (RequestLogging externo, RateLimit interno), solo el principio documentado es incorrecto.

**Impacto técnico.** La documentación del paquete describe un árbol que ya no existe y un comportamiento de middleware invertido; un lector nuevo seguiría la doc, no el código.

**Solución propuesta.** Actualizar los docstrings de `__init__.py` al árbol real (cli: `main.py`, `live_hydra.py`, `paper_hydra.py`; use_cases: `execute_live.py`, `execute_paper.py`) y corregir el comentario de `main.py` al comportamiento real de Starlette.

**Justificación.** SSOT de documentación; el costo es nulo y evita desorientación.

---

## 6. Observaciones (sin severidad)

**O1 — `apps/api` sin suite de tests.** No existe `tests/api/`. JWT, rate limit, health, settings y middleware (el único componente expuesto públicamente) no tienen cobertura regresiva. Recomendación: `tests/api/` con Redis fake y `httpx.AsyncClient(app=create_app())` (ya facilitado por el factory). **Mejora futura, prioridad alta** por ser la superficie pública.

**O2 — ~~`SyntheticDataSource._SEED = 42` compartida con `tests/trading/`~~ → DESCARTADA (C2, Revisión 6).** Se verificó con `grep` en todo `tests/`: **ningún test** referencia `_SEED`, `SyntheticDataSource`, `execute_paper` ni `execute_live`. El comentario `execute_paper.py:87` ("cambiar aquí y en tests/trading/") es stale — no existe acoplamiento real entre el seed y los tests. La observación de la Revisión 5 era incorrecta; se elimina. Si en el futuro un test de integración consumiera el double, mantener el seed como SSOT documentada.

**O3 — `research/notebooks/` vacío.** Solo contiene `__init__.py`. Observación, sin acción requerida.

**O4 — `research/data/data_access.py` lee `os.environ` en import-time** (`:66-67`) para defaults (`kucoin`/`spot`). Estado global a nivel de módulo que los tests deben resetear (`_reset_storage`/`_reset_gold_loader`). Aceptable para un paquete de notebooks; si `research` se convierte en servicio, migrar a inyección (relacionado con H2).

---

## 7. Evaluación por aspecto solicitado

| Aspecto | Resultado | Evidencia / referencia |
|---|---|---|
| **Clean Architecture** | ✅ Cumple (H3 = No corregir) | `apps/` orquesta; dominio intacto; `SyntheticDataSource` es la única lógica fuera de lugar y queda como deuda aceptada |
| **DDD / bounded contexts** | ✅ Cumple | 44/44 contratos KEPT; `portfolio_service` inyectado (no importado); trading→market_data solo vía BC-50 |
| **DIP** | ✅ En composition roots | `execute_*` recibe `portfolio_service`; research (H2) queda como deuda aceptada |
| **SRP / OCP / ISP / LSP** | ✅ Sólido | Ports ISP-ajustados (`storage.py:15-16`); `_resolve_risk_config` OCP-friendly; sin violaciones LSP detectadas |
| **SSOT** | ❌ H1, H4 | `TradingConfig`/`RiskConfig` reconstruidos; `min_order_usd` divergente |
| **DRY** | ❌ H8, H12 | Duplicación real (no sintáctica) en config/CLI y dataclasses de resultado |
| **KISS** | ✅ (H11 → No corregir) | Complejidad acc. baja; `LiveEngineResources` es objeto de ciclo de vida con evolución prevista, no especulativa (Revisión 6) |
| **Fail-Fast / Fail-Soft** | ✅ en runtime, ❌ H4 | Guard obligatorio en live (ADR-0003); fallback silencioso de `min_order_usd` |
| **SafeOps** | ✅ con H7 | Shutdown ordenado en `finally`; `summarize` fuera del contrato "nunca lanza" |
| **Resiliencia** | ⚠️ H5 | Rate limit por IP roto bajo proxy; sin proxy en el despliegue actual → documentar, no corregir ahora |
| **Event-Driven** | ✅ Cumple | No rompe Kappa (ADR-0002); ciclos trading síncronos por diseño; CLI de datos vía orquestador |
| **Dependency Satisfaction** | ✅ En trading/portfolio | `assemble()` exige config no-None y `portfolio` inyectado (`composition_root.py:101-127,217-227`) |
| **Composition Roots** | ✅ Cumple | Solo roots de BC instancian stores/clientes; `app.cli.entrypoint` usa `CompositionRoot.assemble()` |
| **Naming** | ✅ Coherente | `execution/position_store/feature_source/portfolio_service` consistentes con Ubiquitous Language |
| **Orden lógico** | ✅ Bueno | cli → use_cases → BCs; bridges de config tipada (H1) son la excepción |
| **Robustez** | ⚠️ H6, H7 | Contradicción probes/rate-limit; contrato "nunca lanza" incompleto |
| **Eficiencia** | ⚠️ H10 | Doble lectura del Gold en paper (corrección opcional) |

---

## 8. Deuda técnica: bloqueante vs. mejoras futuras

### Bloqueante (corregir antes de consolidar como producción)
Ningún hallazgo alcanza severidad **Crítico**. Prioridad aprobada en Revisión 4
(justificada con métricas de §3.5 — las funciones de mayor CC son los entrypoints):

- **Fase 1 (integridad de config + toolchain):** **H1** (config reconstruida, SSOT),
  **H4** (fallback silencioso) y **H9** (mypy rojo).
- **Fase 2 (superficie pública + DRY):** **H6** (probes libres de rate limit),
  **H8** (DRY del flujo Hydra) y **H12** (resultado de ciclo único).
- **Fase 3 (limpieza):** **H13** (docs) y **H7** (contrato "nunca lanza").

### Mejoras futuras (deuda no bloqueante)
- H10 (doble lectura), O1 (tests de api), O3-O4.
- **H11** — reclasificado a **"No corregir (deuda aceptable)"** en Revisión 6 por
  decisión del propietario (§5 H11): objeto de ciclo de vida con evolución prevista
  (CCXT/websocket); no es código muerto. **Retirado del plan de remediación** (ya no
  está en Fase 4).

### No corregir (deuda aceptable)
- **H2** — `research` depende de adaptadores concretos: aceptada por el criterio
  de *Research* (sin abstracción sin beneficio claro). R11/R12 tensionan, pero el
  beneficio no justifica el costo (R18). Revisión 6: propietario de acuerdo.
- **H3** — `SyntheticDataSource` en capa de aplicación: fake de un port, contenido
  y documentado; moverlo a tests crearía dependencia runtime. R11/R12 tensionan,
  pero no es infraestructura ni lógica de negocio (R18). Revisión 6: propietario de acuerdo.
- **H5** — rate limit por IP bajo proxy: sin proxy en el despliegue actual; se
  documenta la limitación y se corrige cuando exista proxy.
- **H11** — `LiveEngineResources`: objeto de ciclo de vida justificado por SafeOps
  (hallazgo 2.3) y evolución prevista; eliminar no simplifica, reubica (§5 H11).
  Revisión 6, decisión del propietario.

---

## 9. Plan de remediación

> **Estado: PENDIENTE DE APROBACIÓN.** No se ha ejecutado ningún cambio de código.
> Ámbito decidido: actualización del informe (hecha). La ejecución de fases de
> código requiere aprobación explícita.
>
> **Reglas vinculantes (aprobadas, Revisión 3, 4 y 5):** las 6 reglas de Application Layer
> pura (§3 y cabecera) + las reglas 7–19 (§3.4). Modo de R2: **pragmático**.
> `max_order_usd`: **derivado en el CLI** (decisión aprobada — no se mueve al BC en Fase 1).
> Helper CLI nombrado **`_bootstrap.py`** (R14). Cada cambio justifica beneficio (R18)
> y el resultado debe ser menor en LOC/complejidad (R17). **No se añaden capas, managers,
> helpers genéricos, factories o services** sin reducción demostrable de complejidad o
> duplicación (decisión propietario, Revisión 5). **H2/H3 No corregir** — sin puertos o
> abstracciones adicionales en `research` sin beneficio técnico demostrado.

Orden de ejecución por fases, cada una con commits atómicos (Conventional Commits)
y verificación con el pre-push del repo:
`uv run ruff check . && uv run lint-imports --config architecture/importlinter.toml && uv run pytest tests/ -q`.

> **Orden de prioridad aprobado (Revisión 6).** Fase 1: H1, H4, H9 · Fase 2: H6, H8,
> H12 · Fase 3: H13, H7 · Fase 4: **eliminada** (H11 reclasificado a No corregir en
> Revisión 6, decisión propietario) · Fase 5: cobertura (O1). Justificación en §3.5
> (métricas), §3.6 (matriz validada) y §8.

### Fase 0 — Informe (ejecutada)
- `docs(audit)`: `AUDIT-apps-2026-08-03.md` actualizado con el marco de
  Application Layer pura, inventario por módulo, clasificación revisada con
  "No corregir (deuda aceptable)", decisión de reorganización, las 6 reglas
  obligatorias (Revisión 3), las reglas 7–19 con su concordancia (Revisión 4),
  métricas objetivas (§3.5) y la **matriz validada contra el código real** + correcciones
  C1/C2 (§3.6, Revisión 6).

### Fase 1 — Configuración tipada + tooling — H1, H4, H9 · **R3, R4, R5, R6** (`refactor(app)` + `fix(...)`)
1. **Eliminar** `_merge_config_into_args` (`live_hydra.py:127-156`, `paper_hydra.py:128-142`).
   El `argparse.Namespace` queda solo en el borde del CLI (**R5**).
2. **Config assembly tipada** (H1): `assemble_cli_config(app_cfg, cli_args)
   -> tuple[TradingConfig, RiskConfig, RunParams]` vía
   `config.trading.model_copy(update=...)` / `config.risk.model_copy(update=...)`.
   SSOT = `AppConfig`; sin `getattr(..., default)` (**R4, R6**). Vive en
   `apps/app/cli/_bootstrap.py` (que Fase 2 amplía con scaffolding/rendering para H8).
3. **Tipar firmas de use cases** (sin `argparse`):
   - `execute_live.execute(trading: TradingConfig, risk: RiskConfig, portfolio_service,
     *, max_errors: int, min_confidence: float) -> CycleRunResult`
   - `execute_paper.execute(trading: TradingConfig, risk: RiskConfig, portfolio_service,
     *, dry_run: bool, min_confidence: float) -> CycleRunResult`
   Los use cases construyen guard + `TradingCompositionRoot` desde configs tipados
   (**R3**); `min_order_usd` llega tipado — H4 muere naturalmente.
4. `max_order_usd = capital × max_risk_pct` se deriva **una vez** en el CLI
   (decisión aprobada): config-time vía `model_copy`, no runtime de negocio.
5. **H9** — tipar `_ensure_polars(...) -> pl.DataFrame`, anotar `df` en
   `get_features`, corregir `ping()` de Redis (`api/main.py:67`,
   `api/routers/health.py:71`).
6. **Verificación:** `ruff`, `lint-imports`, `uv run mypy apps/` **en verde (0 errores)**,
   `pytest tests/app tests/research tests/architecture`.

### Fase 2 — Superficie pública + DRY — H6, H8, H12 (+ H5-docs) (`fix(...)` + `refactor(app)`)
1. **H6** — excluir `/health`, `/ready`, `/metrics` del rate limit (SSOT
   `_SILENT_PATHS` compartido); corregir la afirmación de `health.py:15` y el
   comentario LIFO invertido de `api/main.py:10-12`.
2. **H5-docs** — documentar en el docstring del middleware la limitación del rate
   limit bajo proxy (`X-Forwarded-For`), sin cambio de infraestructura.
3. **H8** — `cli/_bootstrap.py` centraliza el scaffolding y rendering compartidos de
   ambos CLIs Hydra (`setup_logging`, `handle_sigterm`, `log_cycle_result`) — R9/R10.
4. **H12** — nuevo `apps/app/use_cases/run_result.py` con `CycleRunResult` único;
   `LiveRunResult`/`PaperRunResult` quedan eliminados; ambos CLIs usan el mismo
   rendering. (R15: archivo justificado por 4 consumidores.)
5. **Verificación:** `ruff`, `lint-imports`, `mypy apps/`, `pytest tests/app`.

### Fase 3 — Limpieza y robustez — H13, H7 (+ H10 opcional) (`chore(...)` + `fix(...)`)
1. **H13** — actualizar docstrings de `__init__.py` (árbol real: `main.py`,
   `live_hydra.py`, `paper_hydra.py`; use_cases: `execute_live.py`,
   `execute_paper.py`) y comentario de middleware.
2. **H7** — envolver el tramo de analytics (`summarize` + lectura de `closed_trades`)
   en el bloque protegido → `CycleRunResult(success=False, ...)`. (R7: never abort.)
3. **H10** — (opcional) reutilizar el DataFrame del probe en el engine de paper.
4. **Verificación:** `ruff`, `lint-imports`, `pytest tests/app`.

### Fase 4 — ~~H11 — eliminación de `LiveEngineResources`~~ ELIMINADA (Revisión 6)
H11 se reclasificó a **No corregir (deuda aceptable)** por decisión del propietario
(§5 H11, Revisión 6): `LiveEngineResources` es un objeto de ciclo de vida justificado
por SafeOps (hallazgo 2.3) y por la evolución prevista del motor live (CCXT/websocket
en roadmap event-driven); no es código muerto. Eliminarlo no simplifica, **reubica**
la responsabilidad. **No se ejecuta esta fase.** Cuando CCXT/websocket se materialice,
la decisión de dónde vive el ciclo de vida de esa conexión se toma con la pieza real
(patrón del root, R8), no anticipadamente.

### Fase 5 — Cobertura (mejora futura)
1. **O1** — crear `tests/api/` (settings, jwt, health, rate limit con Redis fake,
   middleware) + `tests/app/test_use_cases_typed.py` (use cases con
   `portfolio_service` fake y `assemble_cli_config` para live y paper).
   Verificación: probes excluidas del rate limit; 429 tras umbral; `/ready` → 503
   con Redis caído.

### Excluido por decisión (No corregir)
- **H2**, **H3** — deuda aceptada (ver §8; R11/R12 tensionan, R18 justifica).
  Revisión 6: propietario de acuerdo.
- **H5** — solo documentación del docstring; sin cambio de infraestructura.
- **H11** — `LiveEngineResources` (Revisión 6, propietario): objeto de ciclo de vida,
  no código muerto; ver §5 (H11). Antes en Fase 4, ahora excluido.
- **H10** — doble lectura del Gold (opcional; beneficio marginal).

---

## 10. Referencias cruzadas

| Hallazgo | ADR / Contrato |
|---|---|
| H1 | ADR-0003 (constructor angosto). ~~ADR-0005~~ → **corrección C1 (Revisión 6)**: la frase citada no existe; ADR-0005 está reemplazado por ADR-0012. Deuda prevista solo en comentario `paper_hydra.py:123-125`. |
| H2 | BC-20 (dirección), DIP/ports (`storage.py`, `boundaries.py`) |
| H3 | Clean Architecture; ADR-0003 (inyección desde roots) |
| H4 | SSOT de `risk.order`; Fail-Fast |
| H5 | SafeOps / superficie pública |
| H6 | Fail-Operative; doc vs código |
| H8 | ADR-0005 (CLIs Hydra hermanos), DRY |
| H9 | pyproject mypy scope; AGENTS (frontera pandas→polars) |
| H10 | Contrato `FeatureSource` (`boundaries.py`) |
| H11 | Hallazgo 2.3 `docs/audits/2026-08-composition-root-audit.md`; commits `6f4ff38`/`4e5f53b`; ADR-0012 (reubica, no elimina); roadmap event-driven ADR-0002/Kappa; `docs/DOMAIN.md:120` (`RiskGateConsumer` adelantado) |
| Composition Roots | ADR-0003, ADR-0006, ADR-0012; BC-43, BC-50 |
| Event-Driven | ADR-0002 (Kappa); decisión sincrónica de ciclos trading |

---

*Documento generado el 2026-08-03 · Revisión 6: **validación de cada hallazgo contra
el código real** (snapshot verificado idéntico al working tree), con la matriz
validada por hallazgo (§3.6: evidencia, riesgo, beneficio, costo, riesgo de regresión,
prioridad, decisión final) y métricas recomputadas. Correcciones: **C1** — la citación
de ADR-0005 en H1 no existe (ADR reemplazado por ADR-0012; fuente real = comentario
`paper_hydra.py:123-125`); **C2** — O2 descartada: ningún test usa `_SEED`. Decisiones
del propietario: **H11 pasa a No corregir** (objeto de ciclo de vida con evolución
prevista, no código muerto — Fase 4 eliminada del plan); **H2/H3 No corregir**
confirmadas. Prioridad F1=H1,H4,H9 · F2=H6,H8,H12 · F3=H13,H7 · F5=cobertura (O1).
Sin modificaciones de código durante la auditoría; el plan de remediación (§9) queda
pendiente de aprobación.*
