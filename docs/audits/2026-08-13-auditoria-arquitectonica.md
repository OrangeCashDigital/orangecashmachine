# Auditoría arquitectónica de OrangeCashMachine (OCM) — 2026-08-13

**Alcance:** estado real de `market-data`, `trading`, `portfolio`, `paper`, `live`;
Docker/compose; configuración; documentación; tests; historial git.
**Método:** solo lectura. 1,147 commits, 21 YAML de config, 20+ ADRs, docker-compose
completo, CI completo, verificación directa de entrypoints e imports.
**Estado:** NINGÚN archivo fue modificado por esta auditoría. Los cambios propuestos
quedan a la espera de aprobación explícita.

> Nota de vigencia: el working tree contiene, sin commit, `docs/architecture/README.md`,
> `docs/architecture/decisions/ADR-0024-*.md` y una corrección de `Dockerfile` (CMD →
> `market_data.main`). Esta auditoría los incorpora como "trabajo en curso" y los trata
> en H-13/H-1; no son resultado de esta auditoría.

---

## 1. Resumen ejecutivo

OCM es un **monolito modular gobernado por contratos** (bounded contexts `market_data`,
`trading`, `portfolio` + plataforma `ocm` + `shared`), NO un conjunto de microservicios
en producción. Dirección explícita del repo: commit `c408536` ("modular monolith layout"),
decisión de capacidad F2.6d (2026-08-10, "proceso único (systemd) + Kafka local suficiente;
NO se crea ADR de escala"), y cero apariciones de "microservicio" en la documentación `.md`
(aparece solo en commits y en el docstring de `market_data/main.py`).

Lo que hoy es real y ejecutable:
- **`market-data`** como **servicio FastAPI independiente** (`packages/market_data/main.py`,
  :8001, `/health`, `/ready`, `/ohlcv/{exchange}/{symbol}/{timeframe}`; 3 tareas background:
  ingestion loop, bronze writer Kafka→Bronze, feed orchestrator). Import verificado OK.
- **`paper` / `live`** como CLIs Hydra que ensamblan **trading + portfolio embebidos en el
  mismo proceso** (`apps/app/cli/paper_hydra.py`, `live_hydra.py`).
- Todo el pipeline de datos + 1054 tests verdes + 49 contratos import-linter.

Lo que está roto (inconsistencias, no servicios muertos):
- `Dockerfile:40` CMD apunta a `market_data.orchestration.entrypoint` (módulo eliminado en
  `09a1890`). Ya hay corrección sin commit en el working tree.
- Bloques `trading:`/`portfolio:` del profile `microservices` referencian `python -m
  trading.main`/`portfolio.main`, módulos que **nunca existieron** (git vacío).

Lo que NO debe borrarse: `packages/trading/**`, `packages/portfolio/**`,
`shared/contracts/boundaries.py`, `apps/api`, profile `microservices` (dirección futura),
ADRs. Todos son evolución prevista con contratos CI activos.

## 2. Explicación para principiante

- **Hoy:** un solo repositorio con varios bounded contexts. `market-data` corre solo como
  servicio web. `trading` y `portfolio` son bibliotecas potentes que `paper` y `live`
  "ensamblan" dentro de su propio proceso (todo en memoria, sin red entre ellos).
- **En construcción:** trading (motor paper/live real vía `TradingCompositionRoot` +
  `TradingEngine`, ADR-0012/0016) y portfolio (bounded context completo embebido, ADR-0006/
  0008/0011). Ambos se ejecutan en el camino de producción de paper/live.
- **Futuro:** extraer trading y portfolio a servicios propios (`market-data → trading →
  portfolio`). Ya existen los contratos (Protocols, BC-50) y el scaffolding (profile Docker,
  `MARKET_DATA_URL`).
- **Roto:** entradas de Docker que apuntan a módulos inexistentes.
- **Qué NO borrar:** nada de trading/portfolio — están vivos y gobernados.

## 3. Runtime ACTUAL (verificado)

```
PROCESOS REALES HOY
─────────────────────────────────────────────────────────────────
[ocm]       app.cli.main ─► Hydra AppConfig ─► PipelineOrchestrator ─► Iceberg (batch)
[paper]     app.cli.paper_hydra ─► TradingCompositionRoot.assemble_paper ─► TradingEngine
              └─► PortfolioCompositionRoot.assemble() ─► PortfolioService
                    (IN-PROCESS; close() en finally; InMemory/RedisPositionStore)
[live]      app.cli.live_hydra (--capital, ADR-0016) ─► assemble_live ─► TradingEngine
              └─► PortfolioCompositionRoot.assemble() (IN-PROCESS)
[market-data SERVICE] python -m market_data.main (FastAPI :8001)
              ── ingestion_loop · bronze_writer_loop · feed_orchestrator (background)
              ── /health /ready /ohlcv/{exchange}/{symbol}/{timeframe}
[streaming] app.cli.streaming_hydra (canary WS → Kafka)
[ocm-api]   apps/api (FastAPI :8000, experimental; /health /ready)

INFRA (docker compose, default): Redis · Kafka · Zookeeper · Prometheus · Grafana ·
Alertmanager · Pushgateway · Loki · Promtail · config-guard
DOCKER profile [microservices]: market-data ✅ · trading ❌ (main inexistente) · portfolio ❌
```

## 4. Arquitectura OBJETIVO

```
FUTURO (respaldado por shared/contracts/boundaries.py, ADR-0003/0006/0011/0024, BC-50):
      ocm-api (gateway)
          │
 market-data service ─HTTP(MARKET_DATA_URL)─► trading service ─RebalancePort─► portfolio service
     (:8001)              (:8002)                                (:8003)
      Bronze/Silver/Gold     OMS + executors + RiskManager        RedisPositionStore
HOY: market-data ya es proceso propio; trading+portfolio comparten el proceso de paper/live.
```

## 5. Mapa de bounded contexts

| BC | Paquete | Rol | Estado |
|---|---|---|---|
| market_data | `packages/market_data` | Ingestión, medallion, calidad, Kafka | IMPLEMENTADO (NIVEL 1) |
| trading | `packages/trading` | Motor paper/live (strategy→risk→OMS→engine) | EN CONSTRUCCIÓN (NIVEL 2) |
| portfolio | `packages/portfolio` | Posiciones, rebalanceo | EN CONSTRUCCIÓN (NIVEL 2) |
| ocm (plataforma) | `ocm/` | Config Hydra, runtime, observabilidad | IMPLEMENTADO |
| shared | `shared/` | Types, contracts, kafka schemas | IMPLEMENTADO |
| apps | `apps/app`, `apps/api`, `apps/research` | Entrypoints CLI + gateway experimental | CLI IMPLEMENTADO; API experimental |

## 6. Matriz de madurez

| Componente | Nivel | Existe | Ejecutable | HTTP | Docker | Tests | Clasificación |
|---|---|---|---|---|---|---|---|
| **market-data** | 1 | ✅ `main.py:452` | ✅ `python -m market_data.main` (import OK) | ✅ FastAPI :8001 | ⚠️ compose OK / Dockerfile CMD roto | ✅ unit+config; ❌ sin TestClient | **IMPLEMENTADO** |
| **trading** | 2 | ✅ `engine.py`+`bootstrap/`+execution/risk/analytics | ⚠️ biblioteca, sin main | ❌ | ❌ `trading.main` no existe | ✅ 96 unit + guards CI | **EN CONSTRUCCIÓN** |
| **portfolio** | 2 | ✅ services/bootstrap/infra/ports/models | ⚠️ embebido, sin main | ❌ | ❌ `portfolio.main` no existe | ✅ 46 unit | **EN CONSTRUCCIÓN** |
| **paper** | 1 | ✅ `paper_hydra.py` | ✅ `uv run paper` | ❌ | ❌ | ✅ offline smoke | **IMPLEMENTADO** |
| **live** | 1 | ✅ `live_hydra.py` | ✅ `uv run live` (--capital) | ❌ | ❌ | ✅ unit (sin smoke E2E) | **IMPLEMENTADO** |
| compose trading/portfolio | 3 | ✅ en YAML | ❌ | ❌ | ✅ declarado | ❌ | **SCAFFOLDING / FUTURO (roto)** |

## 7. Hallazgos (numerados)

**H-1 — Dockerfile CMD roto.** `Dockerfile:40` → `python -m market_data.orchestration.entrypoint`.
El módulo no existe desde `09a1890` (git mv `market_data/orchestration` → `ocm_platform/control_plane/orchestration`).
Sobrevive solo porque `docker-compose.yml:359` sobreescribe el CMD. *Ya existe corrección sin commit (CMD → `market_data.main`).*

**H-2 — compose `trading` apunta a módulo inexistente.** `docker-compose.yml:402` `python -m trading.main`;
`packages/trading/main.py` nunca existió (git vacío). Healthcheck `:403-404` a `/health` idem.

**H-3 — compose `portfolio` apunta a módulo inexistente.** `docker-compose.yml:441` `python -m portfolio.main`;
`packages/portfolio/main.py` nunca existió. Healthcheck `:443-444` idem.

**H-4 — profile `[microservices]` = scaffolding legítimo parcialmente roto.** Introducido en `05dfa8f`.
Mezcla un servicio real (market-data) con dos placeholders (trading/portfolio). ADR-0024 lo
formaliza como direcciones NIVEL 1 / NIVEL 2 / NIVEL 3.

**H-5 — Portfolio NO es servicio; es bounded context embebido.** Único caller de
`PortfolioCompositionRoot`: `paper_hydra.py:193-197`, `live_hydra.py:231-235`, in-process.
`config/portfolio/portfolio.yaml` (capital_usd, exchange) es **config de negocio** (Hydra `_MODULE_GLOBS`),
no endpoints de despliegue.

**H-6 — Env vars trading/portfolio huérfanas fuera del SSOT.** `ocm/config/env_vars.py:80-81` registra
solo `MARKET_DATA_HOST/PORT`. `TRADING_HOST/PORT` (`compose:394-395`), `PORTFOLIO_HOST/PORT`
(`compose:434-435`), `MARKET_DATA_URL` (`compose:393`) no están en el SSOT y ningún código las lee.

**H-7 — `.env.example` invierte puertos.** `:80-81` `PORTFOLIO_HOST_PORT=8002`/`TRADING_HOST_PORT=8003`
vs compose `TRADING_HOST_PORT:-8002` (`:397`)/`PORTFOLIO_HOST_PORT:-8003` (`:437`). Copiar `.env.example`
cruza los puertos.

**H-8 — README no menciona el servicio market-data ni el profile microservices.** Entrypoints table
(`README.md:126-138`) solo lista `ocm/ocm-api/paper/live`; stack (`:165`) omita los 3 servicios.

**H-9 — README "LiveExecutor es stub" obsoleto.** `README.md:211-217` vs `live_executor.py:78`
`IS_STUB: ClassVar[bool] = False` + ADR-0016 aceptado (`5090245`).

**H-10 — `packages/trading/__init__.py:20` obsoleto.** "El entrypoint CLI vive en app/run_paper.py" —
módulo inexistente (eliminado en `4e5f53b`).

**H-11 — market-data service sin tests HTTP.** No existe `tests/market_data/test_main.py`; no hay
`TestClient`/`httpx` en todo el repo. Solo se valida por import y CI config-validation.

**H-12 — CI no valida Docker ni compose.** Ningún workflow referencia `docker-compose.yml`/`docker build`
(`docker-lint.yml` solo hadolint). `docker compose config --quiet` pasa (exit 0) pero no corre en CI.

**H-13 — Falta documentación indexada de arquitectura.** `docs/architecture/` no tenía `README.md`/`index.md`;
la serie 0000–0005 está SUPERSEDED. *Borrador nuevo: `docs/architecture/README.md` + `ADR-0024` (sin commit).*

## 8. Severidad / prioridad

| Hallazgo | Severidad | Prioridad | Acción mínima |
|---|---|---|---|
| H-1 | Roto | P1 | corregir CMD (ya redactado sin commit) |
| H-2 | Roto | P1 | comentar scaffold o documentar |
| H-3 | Roto | P1 | comentar scaffold o documentar |
| H-4 | Ambigüedad | P2 | documentar (ADR-0024 ya lo hace) |
| H-5 | Informativo | P2 | confirmar en docs |
| H-6 | Config huérfana | P2 | registrar en SSOT o eliminar |
| H-7 | Config | P1 | corregir `.env.example` |
| H-8 | Doc | P2 | actualizar README |
| H-9 | Doc | P2 | corregir README |
| H-10 | Doc | P2 | corregir docstring |
| H-11 | Validación | P2 | test HTTP (brecha compartida, ver ADR-0024) |
| H-12 | Validación | P2 | job `docker compose config` en CI |
| H-13 | Governance | P2 | índice (borrador ya existe) |

## 9. Evidencia concreta

- H-1: `Dockerfile:40`; `git log -S "orchestration.entrypoint" -- Dockerfile` → última vez `c39ada4`;
  `09a1890` movió `market_data/orchestration`; `ls packages/market_data/orchestration` → no existe.
- H-2: `docker-compose.yml:402`; `git log --all --oneline -- '*trading/main.py'` → vacío.
- H-3: `docker-compose.yml:441`; `git log --all --oneline -- '*portfolio/main.py'` → vacío.
- H-4: `05dfa8f` ("profiles para microservicios"); `docker-compose.yml:24-26` (🔕 por defecto);
  `docs/architecture/decisions/ADR-0024` (borrador) define NIVEL 1/2/3.
- H-5: `apps/app/cli/paper_hydra.py:193-197`; `live_hydra.py:231-235`; `config/portfolio/portfolio.yaml:14-19`;
  `ocm/config/hydra_loader.py:108` carga `portfolio/portfolio.yaml`; sin `portfolio/main.py`, sin CLI, sin HTTP.
- H-6: `ocm/config/env_vars.py:80-81,157-158`; `docker-compose.yml:393-395,434-435`; grep código Python de
  `TRADING_HOST`/`PORTFOLIO_HOST`/`MARKET_DATA_URL` → cero lecturas.
- H-7: `.env.example:79-81`; `docker-compose.yml:397,437`.
- H-8: `README.md:126-138,165`; grep "microservices" en README → cero.
- H-9: `README.md:211-217`; `packages/trading/execution/live_executor.py:78`; `5090245` ADR-0016.
- H-10: `packages/trading/__init__.py:20`; `apps/app/run_paper.py` no existe; `4e5f53b` eliminó legados.
- H-11: `tests/market_data/` sin `test_main.py`; grep `TestClient|httpx` → cero.
- H-12: `.github/workflows/` sin referencias a compose/build; `docker compose config --quiet` → exit 0.
- H-13: `docs/architecture/` sin índice (salvo borrador nuevo); `SUPERSEDED-0003..0005`; ADR-0024 borrador.

## 10. Cambios propuestos (pendientes de aprobación)

1. **P1 — `Dockerfile:40`** CMD → `python -m market_data.main`. *(ya redactado sin commit)*
2. **P1 — `docker-compose.yml`** añadir comentario explícito en bloques `trading:`/`portfolio:`
   ("scaffolding no funcional hasta que exista el entrypoint"). No eliminar servicios.
3. **P1 — `.env.example:79-81`** corregir puertos (TRADING=8002, PORTFOLIO=8003).
4. **P2 — `ocm/config/env_vars.py`** registrar `TRADING_HOST/PORT`, `PORTFOLIO_HOST/PORT`, `MARKET_DATA_URL`
   en el SSOT (si se conservan) o eliminarlos del compose.
5. **P2 — `packages/trading/__init__.py:20`** corregir docstring → `apps/app/cli/paper_hydra.py`.
6. **P2 — `README.md`** añadir servicio market-data en entrypoints/estado; corregir nota LiveExecutor.
7. **P2 — CI** añadir `docker compose config --profile microservices` (sintaxis) a `ocm-ci.yml`.
8. **P2 — tests** (opcional) test HTTP de `market_data.main` (TestClient).

## 11. Decisiones que requieren tu aprobación

1. Servicios compose `trading`/`portfolio`: ¿comentarlos como scaffold (recomendado) o eliminarlos?
2. `MARKET_DATA_URL`: ¿conservar como contrato futuro (recomendado) o eliminar?
3. Env vars `TRADING/PORTFOLIO_*`: ¿registrarlas en SSOT o eliminarlas?
4. Test HTTP de market-data: ¿ahora (P2) o difiere?
5. Índice `docs/architecture/README.md` + ADR-0024 (borradores sin commit): ¿commitearlos?

## 12. Documentación propuesta

- `docs/architecture/README.md` — índice estado real vs objetivo (borrador existente).
- `docs/architecture/decisions/ADR-0024-*.md` — dirección microservicios NIVEL 1/2/3 (borrador existente).
- `docs/architecture/current-runtime.md` / `target-services.md` / `compose-profile.md` (opcional,
  no excesivo). ADRs quedan como SSOT.

## 13. Problemas fuera de alcance

- **B-19 (dedup durable Bronze):** intacto (`3fb973f`). No se encontró contradicción crítica en
  dedup/BronzeWriter/RedisCursorStore → solo reportado.
- **H-01 live (posible stub de capital):** independiente, no mezclado. Impacto: si `live` usara un
  capital de reserva sin `--capital`, el dimensionado de riesgo sería erróneo; requiere tarea aparte.
- **`ocm/config/env_vars.py` cambio pre-existente sin commit** (vars JWT de API) — no parte de esta auditoría.

## 14. Archivos que tocaría (solo tras aprobación)

`Dockerfile` · `docker-compose.yml` (comentarios) · `.env.example` · `ocm/config/env_vars.py` ·
`packages/trading/__init__.py` · `README.md` · `.github/workflows/ocm-ci.yml` · (opcional)
`tests/market_data/test_main.py`.

## 15. Archivos que NO tocaría

`packages/trading/**` (no crear `trading.main`) · `packages/portfolio/**` (no crear `portfolio.main`) ·
`packages/market_data/**` (B-19 intacto) · `ocm/runtime/state/cursor_store.py` ·
`market_data/infrastructure/kafka/{dedup,bronze_writer}.py` · `shared/contracts/boundaries.py` ·
`docs/plans/tracking.yaml` · `config/*` (sin crear `config/trading/`) · `.env` · `apps/api/**`.

## Comparación con la premisa arquitectónica

| Premisa | Verificación | Conclusión |
|---|---|---|
| "OCM evoluciona incrementalmente hacia microservicios" | ADR-0024 (borrador), profile `microservices`, `MARKET_DATA_URL`, Protocols | ✅ CONFIRMADO como dirección, NO como runtime |
| "paper/live ensamblan trading + portfolio embebidos" | `paper_hydra.py:193-197`, `live_hydra.py:231-235` in-process | ✅ CONFIRMADO |
| "market-data ya es servicio independiente" | `main.py:452,625-635`, import OK | ✅ CONFIRMADO |
| "objetivo: market-data → trading → portfolio" | `docker-compose.yml:393` (MARKET_DATA_URL), BC-50, boundaries.py | ✅ CONFIRMADO como futuro |
| "trading/portfolio EN CONSTRUCCIÓN, no muertos" | CompositionRoots activos en producción, tests, BC-12/13/36/43/44/50 | ✅ CONFIRMADO — NO es código muerto |
| "no crear main.py por no existir no = muerto" | `git log --all -- '*trading/main.py'` vacío + runtime embebido activo | ✅ CONFIRMADO |

### Clasificación por componente

| Componente | Clasificación | Evidencia |
|---|---|---|
| market-data | **IMPLEMENTADO** | FastAPI real, entrypoint ejecutable, compose OK, tests+CI |
| trading | **EN CONSTRUCCIÓN** | CompositionRoot en camino de producción (paper/live), 96 tests, BC activos; sin main |
| portfolio | **EN CONSTRUCCIÓN** | CompositionRoot embebido usado por paper/live, 46 tests, BC-13/43/44; sin main |
| paper | **IMPLEMENTADO** | `uv run paper`, offline smoke OK |
| live | **IMPLEMENTADO** | `uv run live` (--capital, ADR-0016); brecha B-23 independiente |
| compose trading/portfolio | **SCAFFOLDING / FUTURO** | Profile docker + vars; main inexistentes → no ejecutables |
| Dockerfile CMD | **OBSOLETO** | Apunta a módulo movido en `09a1890`; corrección ya redactada |
| `packages/trading/__init__.py:20` | **OBSOLETO** | Ref a `app/run_paper.py` inexistente |
| Env vars TRADING/PORTFOLIO/MARKET_DATA_URL | **SCAFFOLDING / FUTURO** (huérfanas hoy) | Solo en compose; cero lecturas en código |
| apps/api | **SCAFFOLDING / FUTURO** | Gateway experimental, solo /health |

**Ningún componente se clasifica como OBSOLETO/MUERTO ni UNKNOWN.**

---

## APROBACIÓN REQUERIDA

| ID | Cambio propuesto | Archivo(s) | Prioridad | Riesgo | ¿Requiere mi aprobación? |
|---|---|---|---|---|---|
| C1 | CMD → `python -m market_data.main` | `Dockerfile:40` | P1 | Bajo (ya redactado en working tree) | **SÍ** |
| C2 | Comentar bloques `trading:`/`portfolio:` como scaffold no funcional | `docker-compose.yml:376-456` | P1 | Bajo (no elimina servicios) | **SÍ** |
| C3 | Corregir puertos invertidos | `.env.example:79-81` | P1 | Bajo | **SÍ** |
| C4 | Registrar vars en SSOT o eliminar del compose | `ocm/config/env_vars.py` | P2 | Medio (depende de decisión D1/D2) | **SÍ** |
| C5 | Corregir docstring | `packages/trading/__init__.py:20` | P2 | Bajo | **SÍ** |
| C6 | Actualizar README (servicio market-data + LiveExecutor) | `README.md` | P2 | Bajo | **SÍ** |
| C7 | Job CI `docker compose config --profile microservices` | `.github/workflows/ocm-ci.yml` | P2 | Bajo | **SÍ** |
| C8 | Test HTTP market-data (TestClient) | `tests/market_data/test_main.py` (nuevo) | P2 | Bajo | **SÍ** (opcional, ver D4) |
| C9 | Commitear borradores README-arch + ADR-0024 | `docs/architecture/README.md`, `docs/architecture/decisions/ADR-0024-*.md` | P2 | Bajo | **SÍ** (ver D5) |

**Decisiones previas requeridas:** D1 (compose trading/portfolio), D2 (MARKET_DATA_URL),
D3 (env vars), D4 (test HTTP), D5 (commit de borradores de documentación).

---

# FASE 1 — BASELINE SOLO LECTURA (2026-08-13, verificado contra el repositorio)

> Metodología: Staff/Principal Architect — **Primero verdad arquitectónica; después fronteras;
> después contratos; después implementación; finalmente separación física de procesos.**
> Ningún archivo de código/config/tests/CI/Docker fue modificado. Esta sección se añadió al
> informe existente (no se duplica). Fuente de verdad = el repositorio real; auditorías previas
> de Claude/OpenCode = evidencia secundaria, verificada y discrepada donde proceda.

## A. Estado real actual

| Componente | Estado | Entrypoint real | Cómo corre | Dependencias clave | Almacenamiento | Tests |
|---|---|---|---|---|---|---|
| **market-data** | **IMPLEMENTADO** (NIVEL 1) | `python -m market_data.main` | Servicio FastAPI independiente (`packages/market_data/main.py`) | Hydra standalone, Redis (cursor/dedup), Kafka (bronze writer), Iceberg (Silver) | Iceberg Silver + Redis cursor store | `tests/market_data/` (~130 tests, 0 HTTP) |
| **trading** | **EN CONSTRUCCIÓN** (NIVEL 2) | — (sin `main.py`, embebido) | Ensamblado in-process por `TradingCompositionRoot` desde `execute_live.py`/`execute_paper.py` (llamados por `live_hydra.py`/`paper_hydra.py`) | `trading.bootstrap.composition_root`, `portfolio.services` (BC-50 limitado a bootstrap) | In-memory / via PortfolioService (RedisPositionStore) | `tests/trading/` (96 tests, 9 archivos) |
| **portfolio** | **EN CONSTRUCCIÓN** (NIVEL 2) | — (sin `main.py`, embebido) | Ensamblado in-process por `PortfolioCompositionRoot.assemble()` desde `paper_hydra.py:193-213`/`live_hydra.py:231-252` | `portfolio.bootstrap.composition_root`, RedisPositionStore/InMemoryPositionStore | Redis (si `integrations.redis.enabled`) o memoria | `tests/portfolio/` (46 tests, 4 archivos) |
| **paper** | **IMPLEMENTADO** | `uv run paper` → `app.cli.paper_hydra:main` | CLI Hydra, trading+portfolio embebidos | TradingCompositionRoot.assemble_paper | — | offline smoke |
| **live** | **IMPLEMENTADO** | `uv run live` → `app.cli.live_hydra:main` | CLI Hydra, trading+portfolio embebidos, guard fail-closed (ADR-0016/0017) | TradingCompositionRoot.assemble_live | — | unit |
| **streaming** | **IMPLEMENTADO** (canary) | `uv run streaming` → `app.cli.streaming_hydra:main` | Entrypoint WS → Kafka (ADR-0022) | market_data WSProducerBundle | Kafka | 983 tests suite |

## B. Arquitectura runtime (hoy, verificado)

```
PROCESOS REALES HOY
[ocm]          app.cli.main ──► PipelineOrchestrator ──► Iceberg (batch medallion)
[paper]        app.cli.paper_hydra ──► TradingCompositionRoot.assemble_paper ──► TradingEngine
                  └──► PortfolioCompositionRoot.assemble() (IN-PROCESS)
[live]         app.cli.live_hydra (--capital, ADR-0016/0017 guards) ──► assemble_live ──► TradingEngine
                  └──► PortfolioCompositionRoot.assemble() (IN-PROCESS)
[streaming]    app.cli.streaming_hydra (canary WS → Kafka)
[market-data]  python -m market_data.main (FastAPI :8001) ── ingestion_loop · bronze_writer_loop · feed_orchestrator
                  ──► /health /ready /ohlcv/{exch}/{sym}/{tf}
[ocm-api]      apps/api (FastAPI :8000, experimental, /health)
DOCKER default: Redis · Kafka · Zookeeper · Prometheus · Grafana · Alertmanager · Loki · Promtail
DOCKER [microservices]: market-data ✅ · trading ❌ · portfolio ❌ (mains inexistentes)
```

## C. Arquitectura objetivo (dirección, NO estado actual)

```
market-data ──► trading ──► portfolio   (dirección evolutiva)
  :8001           :8002        :8003
```

- `MARKET_DATA_URL: http://market-data:8001` (compose `:393`) anticipa el contrato HTTP trading→market-data.
- **NO es una afirmación de que trading/portfolio sean hoy servicios.**
- Extracción futura requiere: boundary estable → contrato estable → tests+observabilidad → lifecycle independiente → necesidad operacional real (principio F2.6d: proceso único suficiente hoy).

## D. Madurez por bounded context

| BC | Código | Entrypoint | Tests | Configuración | Docker | Estado |
|---|---|---|---|---|---|---|
| market-data | ✅ completo (FastAPI, loops, bronze, feed) | ✅ `market_data.main` | ✅ ~130 unit, ❌ 0 HTTP | ✅ `config/market_data/{feeds,external_ingestion}.yaml` | ⚠️ compose OK; Dockerfile CMD roto | **IMPLEMENTADO** |
| trading | ✅ dominio (engine/execution/risk/strategies/analytics) | ❌ no `trading.main` | ✅ 96 unit | ❌ no `config/trading/` | ⚠️ scaffold (main inexistente) | **EN CONSTRUCCIÓN** |
| portfolio | ✅ dominio (services/ports/infra/models) | ❌ no `portfolio.main` | ✅ 46 unit | ✅ `config/portfolio/portfolio.yaml` | ⚠️ scaffold (main inexistente) | **EN CONSTRUCCIÓN** |

## E. Fronteras actuales (import boundaries verificadas)

`uv run lint-imports --config architecture/importlinter.toml` → **49 KEPT / 0 broken**

| Contrato | Resultado | Nota |
|---|---|---|
| BC-12 trading.risk aislado de execution | **KEPT** | legítimo |
| BC-13 portfolio aislado de trading execution/strategies | **KEPT** | legítimo |
| BC-36 trading.strategies aislado de execution/analytics | **KEPT** | legítimo |
| BC-43 PositionStore adapters solo desde portfolio/bootstrap | **KEPT** | legítimo |
| BC-44 portfolio layer order | **KEPT** | legítimo |
| BC-50 trading→market_data solo desde trading/bootstrap/composition_root | **KEPT** | único punto de entrada (correcto por diseño, ADR-0004) |

Imports cruzados reales verificados:
- `packages/trading/bootstrap/composition_root.py:150,164,214` — único importador de market_data (GoldReader, DataNotFoundError, CCXTAdapter) → **permitido por BC-50**.
- `packages/trading/bootstrap/composition_root.py:61-62` — importa PortfolioService/RebalanceSignal → **permitido** (portfolio no depende de trading).
- portfolio → trading: **cero imports**. market_data ← trading: solo bootstrap. Sin acceso a infra interna de otro BC.

**Ninguna violación real. Cero falsos positivos relevantes.**

## F. Violaciones reales (ninguna de import boundaries)

| ID | Tipo | Detalle | Evidencia |
|---|---|---|---|
| **F-01** | Infraestructura | Dockerfile CMD → módulo inexistente | `Dockerfile:40` (`market_data.orchestration.entrypoint`) |
| **F-02** | Infraestructura | compose trading → `trading.main` inexistente | `docker-compose.yml:402` |
| **F-03** | Infraestructura | compose portfolio → `portfolio.main` inexistente | `docker-compose.yml:441` |
| **F-04** | CI | CI no valida compose/profile | `.github/workflows/ocm-ci.yml` (ningún ref a compose) |
| **F-05** | Gobernanza | Engineering Health FAIL preexistente | B-15 `estado='PARCIAL'` fuera del enum; `scripts/engineering_health_check.py` |
| **F-06** | Testing | Cero cobertura HTTP de market-data | sin `TestClient`/`httpx`/`test_main.py` en repo |
| **F-07** | Config | Puertos invertidos en `.env.example:80-81` | PORTFOLIO=8002/TRADING=8003 vs compose inverso |
| **F-08** | Config | Env vars `TRADING/PORTFOLIO_HOST/PORT`, `MARKET_DATA_URL` huérfanas (solo compose) | `ocm/config/env_vars.py:80-81` solo market-data; cero readers |
| **F-09** | Docs | `trading/__init__.py:20` ref a `app/run_paper.py` inexistente | módulo eliminado en `4e5f53b` |
| **F-10** | Docs | README dice "LiveExecutor es stub" | `README.md:211-217` vs `live_executor.py:78 IS_STUB=False`, ADR-0016 |

## G. Scaffolding futuro (NO ejecutable)

- Bloques `trading:`/`portfolio:` del profile `[microservices]` (`docker-compose.yml:376-456`) — comandos/mains inexistentes, imágenes sin build, healthchecks hacia endpoints inexistentes → **NIVEL 3** (ADR-0024).
- `MARKET_DATA_URL` (`:393`) — contrato futuro, sin consumidor.
- Vars `TRADING/PORTFOLIO_HOST/PORT` (`:394-395,434-435`) — declarativas.
- Profile `[microservices]` desactivado por defecto (comentarios `:24-26`).

## H. Problemas reales (consolidado)

1. Dockerfile promete un entrypoint inexistente (F-01).
2. Compose sugiere que trading/portfolio "existen como servicios" cuando son NIVEL 2 embebidos (F-02/F-03) — developer engañado.
3. CI ciego al profile microservices (F-04) — puede romperse sin señal.
4. ADR-0024 existe como archivo untracked pero NO registrada en tracking.yaml (huerfana de gobernanza).
5. Engineering Health FAIL por B-15 (preexistente).
6. Contrato HTTP de market-data sin tests (F-06).

## I. Riesgos

| Riesgo | Nivel | Mitigación |
|---|---|---|
| Ambigüedad de estado trading/portfolio → trabajo prematuro | Alto | Documentar NIVEL 1/2/3 (ADR-0024); no crear mains |
| CI no detecta rotura del profile | Medio | Job `docker compose config` en CI (tras C1/C2) |
| B-15 estado de posición multi-owner | Medio | Preexistente; fuera de alcance; observabilidad mitigada |
| `docker run` directo falla (Dockerfile) | Medio | C1 |
| Config huérfana si se crea `config/trading/` | Bajo | No crear |

## J. Cambios recomendados (ordenados por prioridad)

| ID | Archivo | Problema | Solución | Riesgo | Impacto | Prioridad | ¿Aprobación? |
|---|---|---|---|---|---|---|---|
| C1 | `Dockerfile:40` | CMD inexistente | → `python -m market_data.main` (verificar `python -c "import market_data.main"`) | Bajo | Docker run directo funcional | P1 | **SÍ** |
| C2 | `docker-compose.yml:376-456` | servicios ficticios | Comentario `# SCAFFOLDING NIVEL 3 — sin main, no ejecutable` (sin eliminar) | Bajo | Señal clara | P1 | **SÍ** |
| C3 | `.env.example:80-81` | puertos invertidos | Intercambiar TRADING↔PORTFOLIO | Bajo | Consistencia | P1 | **SÍ** |
| C4 | `ocm/config/env_vars.py` | vars huérfanas | Registrar en SSOT (o eliminar de compose) | Medio | Gobernanza | P2 | **SÍ** |
| C5 | `packages/trading/__init__.py:20` | docstring obsoleto | Actualizar ref | Bajo | Docs | P2 | **SÍ** |
| C6 | `README.md` | LiveExecutor "stub" + falta market-data | Actualizar | Bajo | Docs | P2 | **SÍ** |
| C7 | `.github/workflows/ocm-ci.yml` | no valida compose | Job `docker compose config --quiet` (valida todo, incluye profile) | Bajo | Detección | P2 | **SÍ** (tras C1/C2) |
| C8 | `tests/market_data/test_main.py` (nuevo) | sin cobertura HTTP | TestClient sobre /health /ready /ohlcv | Bajo | Contrato blindado | P2 | **SÍ** |
| C9 | tracking.yaml + ADR-0024 + README-arch | ADR huerfana | Registrar en SSOT; adoptar ADR-0024 (no duplicar); commitear | Bajo | Gobernanza | P2 | **SÍ** |

## K. Cambios que NO deben hacerse (obligatorio)

- ❌ **NO crear `trading.main`** ni `portfolio.main` para satisfacer Compose.
- ❌ **NO crear `config/trading/`** (config huérfana).
- ❌ **NO crear APIs HTTP** de trading/portfolio para "completar" Compose.
- ❌ **NO convertir scaffolding en servicio ejecutable**.
- ❌ **NO introducir comunicación distribuida** (HTTP/eventos entre BCs) hoy.
- ❌ **NO mover responsabilidades entre BCs** (portfolio dueño de estado, trading dueño de decisiones, market-data dueño de datos) sin ADR.
- ❌ **NO modificar** `paper_hydra.py`, `live_hydra.py`, `streaming_hydra.py`, B-19, contratos, schemas, persistencia.
- ❌ **NO implementar** indicadores (OHLC/MFI/Elliott/fractales...) del material de trading sin requisito técnico real + ADR — el material es conocimiento de dominio, no especificación.
- ❌ **NO tocar** cambios preexistentes: `ocm/config/env_vars.py` (JWT API), `Dockerfile` (CMD ya corregido en working tree).

## L. Baseline técnico obtenido (2026-08-13)

| Validación | Resultado | Clasificación |
|---|---|---|
| `uv run ruff check .` | ✅ All checks passed | — |
| `uv run ruff format . --check` | ✅ 458 files already formatted | — |
| `uv run mypy . --no-incremental` | ✅ Success, 356 files | — |
| `uv run pytest tests/ -q --no-cov -m "not integration"` | ⚠️ **1054 passed / 1 failed** | FAIL preexistente (test_engineering_health_passes ← B-15) |
| `pytest tests/market_data tests/trading tests/portfolio` | ✅ 272 passed | — |
| `uv run lint-imports --config architecture/importlinter.toml` | ✅ **49 KEPT / 0 broken** | — |
| `OCM_VALIDATE_ONLY=true uv run python -m app.cli.main` | ✅ exit 0, validation_complete | — |
| `uv run python scripts/engineering_health_check.py` | ❌ **FAIL** (B-15 PARCIAL fuera de enum) | **Preexistente** (no atribuible a esta tarea) |
| `docker compose config --quiet` | ✅ exit 0 | — |
| `docker compose config --profile microservices` | ❌ flag no soportado por compose 2.26.1 (válido en CI moderno) | Herramienta local; CI debe usar `--quiet` |
| pre-commit | (instalado local: ruff, import-linter, mypy, readme-size-guard) | — |

**Fallos preexistentes demostrables:** B-15 (`estado='PARCIAL'` fuera del enum
`['EN_CURSO','HECHO','PENDIENTE','RECHAZADO','VERIFICACION']`) causa el FAIL del Engineering
Health Check y del test `test_engineering_health_passes`. **No introducido por esta auditoría.**

**Cambios preexistentes del usuario preservados (NO sobrescritos):**
- `ocm/config/env_vars.py` (+14 líneas: `OCM_API_JWT_SECRET`, `OCM_API_JWT_EXPIRE_MINUTES` para apps/api)
- `Dockerfile` (CMD ya corregido → `market_data.main`, que coincide con C1)
- Untracked: `docs/architecture/README.md`, `docs/architecture/decisions/ADR-0024-*.md`

---

# FASE 2 — HALLAZGOS CONSOLIDADOS H1–H6 + ANÁLISIS BC BACKTESTING (2026-08-14, solo lectura)

> Metodología: auditoría READ-ONLY sobre el trading engine, la cadena P&L/riesgo y la
> arquitectura de datos. Verificación directa de líneas de código (OBSERVED), cita de
> `docs/plans/tracking.yaml` para hallazgos preexistentes, y distinción explícita entre
> DOCUMENTED / IMPLEMENTED / DEPLOYED. **Ningún archivo de código/config/tests fue
> modificado por esta fase.** Los cambios propuestos quedan a la espera de aprobación.
>
> Estados usados: `CONFIRMED` / `PARTIALLY_CONFIRMED` / `INFERRED` / `UNKNOWN` / `PENDING_FIX`.
> Nivel de certeza de la evidencia: `OBSERVED` (línea leída directamente) / `INFERRED` /
> `DOCUMENTED` (docstring/config/manifest) / `VERIFIED` (reproducción) / `UNKNOWN`.

## 1. Resumen ejecutivo de la Fase 2

El motor de trading single-cycle (paper/live) tiene una **cadena P&L/riesgo rota** que no
solo afecta a la simulación, sino a la integridad de ejecución en live:

1. `LiveExecutor` es real (`IS_STUB=False`, `live_executor.py:78`) pero el docstring de
   `execute_live.py:64-70` y el README aún lo describen como stub; el guard de
   `composition_root.py:430-435` depende de `IS_STUB` para decidir — **el gate que
   documenta el riesgo live es inconsistente con la realidad del código.**
2. `OMS._fill()` (`oms.py:285`) usa `fill_price=order.signal.price` — **el precio real de
   ejecución se descarta**: `LiveExecutor.execute()` solo devuelve `result.accepted`
   (`live_executor.py:122`), nunca el `fill_price` del transporte.
3. `record_close(pnl_pct=0.0)` se llama **siempre** (`oms.py:222,291,322`) → el drawdown
   halt de `RiskManager` (`risk/manager.py:131,233,237`) es **funcionalmente muerto**, y
   `PerformanceEngine`/`TradeTracker` computan P&L nulo.
4. **Stop-loss** está configurado (`config/risk/risk.yaml`: `enabled: true, default_pct: 0.02`)
   pero **no ejecutado**: `StopLossConfig` se valida en `risk/models.py:24` y jamás se lee
   en `risk/manager.py` ni en `oms.py`.
5. **Autoridad de schema Iceberg contradicha**: `schemas.py` se autodeclara SSOT
   (`TRADES_SCHEMA` IDs 101–110) pero `trades_storage.py:55-74` construye su propio schema
   local (IDs 1–9, sin `ingestion_ts`, `side` no-required) → **doble verdad**.
6. **Reproducibilidad vs retención Bronze**: el docstring de `bronze_retention.py:9-16`
   promete reproducibilidad vía Iceberg, pero `RETENTION_DAYS_DEFAULT=7` (expira snapshots
   >7 días) → la promesa de "reconstruir el pasado" no coincide con la retención física.
7. **Event-driven/Kappa**: `KAFKA_ENABLED` default `false` (`config/base.yaml:45`), sin
   consumidores en el código auditado (tracking B-43/F-027), `NullPublisher` prohibido en
   producción (`ohlcv_pipeline.py:247-252`), ADR-0002 marca la migración Kappa "completa
   para el pipeline REST actual" → contradicción entre DOCUMENTED y DEPLOYED.

**Backtesting NO es hoy un Bounded Context nuevo (decisión propuesta, ver §4):** los
conceptos (señal, orden, fill, posición, P&L, riesgo, métricas) ya están distribuidos en
`trading` y `portfolio`; no existe motor de backtest, ni walk-forward, ni experimentos
(grep → solo docstrings). Crear un BC nuevo ahora sería sobrediseño sin necesidad concreta
(KB governance). Se registra la pregunta como `POTENTIAL_ARCHITECTURE_QUESTION`
(`DEFERRED_PENDING_S1`), a re-evaluar tras S1.

## 2. Consolidación de hallazgos (formato canónico)

### H1 — LiveExecutor: documentación y gate inconsistentes con la implementación

| Campo | Valor |
|---|---|
| **ID** | H1 |
| **Título** | LiveExecutor documentado como stub, implementación real |
| **Severidad** | HIGH |
| **Estado** | `CONFIRMED` |
| **Descripción** | `LiveExecutor` ya opera contra `OrderTransport` real (`IS_STUB=False`), pero la documentación de usuario y del entrypoint lo describe como stub. El guard de ensamblaje depende de `IS_STUB` para bloquear — inconsistencia entre lo que se documenta (riesgo live mitigado por gate) y lo que el código hace. |
| **Evidencia** | `OBSERVED` — `live_executor.py:78` `IS_STUB: ClassVar[bool] = False`; `execute_live.py:64-70` "hoy LiveExecutor es un stub sin conexion real al exchange"; `README.md:211-217` (nota obsoleta "es stub"); `composition_root.py:430-435` guard `if executor.IS_STUB: raise RuntimeError(...)`. |
| **Archivos/líneas** | `packages/trading/execution/live_executor.py:78`; `apps/app/use_cases/execute_live.py:64-70`; `packages/trading/bootstrap/composition_root.py:430-435`; `README.md:211-217`. |
| **Impacto** | Decisión de gate basada en un flag que ya no refleja la realidad; documentación engañosa para operadores (falsa sensación de "stub/no-riesgo"). Enlazado con H-9 de esta auditoría (README). |
| **Qué está documentado** | `IS_STUB` como proxy de "CCXT no activo"; el gate bloquea si es stub. |
| **Qué ocurre realmente** | `IS_STUB=False`; el guard nunca bloquea; el docstring del entrypoint desactualizado. |
| **Contradicción** | DOCUMENTED vs IMPLEMENTED. |
| **Nivel de certeza** | ALTO (`OBSERVED` en 4 ubicaciones). |
| **Dependencias** | ADR-0016, F3/B-12, B-01/R1. |
| **Acción propuesta** | Corregir `execute_live.py` docstring y README; eliminar/repensar el guard muerto de `composition_root.py` (el gate real debería verificar el transporte, no un flag de clase). |
| **Estado de decisión** | `PENDING_FIX` (documentación; no bloquea ejecución live). |

### H2 — Cadena P&L/fill/precio real: P&L y drawdown-halt muertos (CRÍTICO)

| Campo | Valor |
|---|---|
| **ID** | H2 |
| **Título** | `POTENTIAL_CRITICAL_EXECUTION_INTEGRITY_ISSUE` — cadena señal→orden→fill→P&L rota |
| **Severidad** | CRITICAL |
| **Estado** | `CONFIRMED` |
| **Descripción** | El precio de señal (`signal.price`) se usa como precio de llenado (`oms.py:285`) y el P&L realizado se registra siempre como `0.0` (`oms.py:222,291,322`). `LiveExecutor` reconcilia fills reales del transporte pero descarta `fill_price` en el contrato `execute() → bool` (`live_executor.py:122`). Consecuencia: el drawdown halt de `RiskManager` (basado en P&L) nunca se dispara y las métricas de trading (`PerformanceEngine`, `TradeTracker`) no reflejan el resultado real. |
| **Evidencia** | `OBSERVED` — `oms.py:285` `fill_price=order.signal.price`; `oms.py:291` `record_close(pnl_pct=0.0)`; `oms.py:222,322` idem en cancel/reject; `live_executor.py:122` `return result.accepted`; `risk/manager.py:131` acumula `pnl_pct`. |
| **Archivos/líneas** | `packages/trading/execution/oms.py:222,285,291,322`; `packages/trading/execution/live_executor.py:122,151-152` (fill_price solo logueado); `packages/trading/risk/manager.py:131,233,237`. |
| **Impacto** | CRÍTICO en live: protección de drawdown inoperante, P&L reportado incorrecto, decisiones de tamaño/riesgo basadas en datos falsos. En paper: backtest-like incorrecto. |
| **Qué está documentado** | OMS reconcilia fill y computa P&L; RiskManager haltea por drawdown. |
| **Qué ocurre realmente** | fill = precio de señal (no real), P&L siempre 0, halt nunca alcanzable. |
| **Contradicción** | DOCUMENTED vs IMPLEMENTED (integridad de ejecución). |
| **Nivel de certeza** | ALTO (`OBSERVED` directo en todas las líneas). |
| **Dependencias** | `RiskManager`, `OMS`, `OrderTransport`, `TradeTracker`, `PerformanceEngine`, ADR-0016/R10. |
| **Acción propuesta** | **S1**: propagar `fill_price` real desde el transporte hasta `record_close(pnl real)`; `record_close` debe recibir `(entry, exit)` o `pnl` calculado con costos; alinear `_fill` del OMS. |
| **Estado de decisión** | `PENDING_FIX` — **P0**. **S1 registrado como `PROPOSED_NEXT_STEP` / `WAITING_FOR_APPROVAL`** (no implementado). |

### H3 — Stop-loss configurado pero no ejecutado (CRÍTICO)

| Campo | Valor |
|---|---|
| **ID** | H3 |
| **Título** | `DOCUMENTED_BUT_NOT_EFFECTIVE` — stop-loss configurado, nunca ejecutado |
| **Severidad** | CRITICAL |
| **Estado** | `CONFIRMED` |
| **Descripción** | `config/risk/risk.yaml` define `stop_loss: {enabled: true, default_pct: 0.02}`; `RiskConfig`/`StopLossConfig` lo validan (`risk/models.py:24,80`), pero **ningún lector** del código lo consume: `risk/manager.py` no referencia `stop_loss`/`default_pct` (grep exit 1), y `oms.py` solo cierra por señal SELL del usuario, no por precio de stop. |
| **Evidencia** | `OBSERVED` — `config/risk/risk.yaml` (stop_loss enabled/default_pct); `risk/models.py:24,69,80`; grep `stop_loss|default_pct|StopLoss` en `risk/manager.py` → **0 resultados**. |
| **Archivos/líneas** | `config/risk/risk.yaml`; `packages/trading/risk/models.py:24,69,80`; `packages/trading/risk/manager.py` (sin uso). |
| **Impacto** | CRÍTICO en live: sin límite automático de pérdida por posición; riesgo de pérdida ilimitada ante caídas (mitigado parcialmente por guard/drawdown, que además está muerto por H2). |
| **Qué está documentado** | Stop-loss activo por defecto (2%). |
| **Qué ocurre realmente** | Configurado (CONFIGURED) pero ni implementado (IMPLEMENTED) ni ejecutado (EXECUTED). |
| **Contradicción** | CONFIGURED vs IMPLEMENTED vs EXECUTED. |
| **Nivel de certeza** | ALTO (`OBSERVED`). |
| **Dependencias** | `RiskManager`, `OMS`, `PortfolioService` (posición), configuración `config/risk/risk.yaml`. |
| **Acción propuesta** | **S1**: implementar evaluación de stop en el camino de ejecución (en `RiskManager` o `OMS._fill`/posición), con `default_pct` real y cierre por precio; probar en paper antes de live. |
| **Estado de decisión** | `PENDING_FIX` — **P0**. **S1 registrado como `PROPOSED_NEXT_STEP` / `WAITING_FOR_APPROVAL`** (no implementado). |

### H4 — Autoridad de schema Iceberg: SSOT contradicho por schema local (HIGH)

| Campo | Valor |
|---|---|
| **ID** | H4 |
| **Título** | `SCHEMA_AUTHORITY_CONTRADICTION` — `schemas.py` (SSOT) vs `trades_storage.py` (schema local) |
| **Severidad** | HIGH |
| **Estado** | `CONFIRMED` |
| **Descripción** | `storage/iceberg/schemas.py` se autodeclara SSOT y define `TRADES_SCHEMA` (IDs 101–110, con `ingestion_ts`, `side` required). `storage/silver/trades_storage.py:55-74` construye su **propio** schema (IDs 1–9, sin `ingestion_ts`, `side` no-required) vía `_build_schema()` y no importa el SSOT. Dos verdades para `silver.trades`. |
| **Evidencia** | `OBSERVED` — `schemas.py:3-6` "Schemas Iceberg para todas las capas del medallón — SSOT"; `schemas.py:77-100` `TRADES_SCHEMA` (IDs 101–110); `trades_storage.py:55-74` `_build_schema()` (IDs 1–9). |
| **Archivos/líneas** | `packages/market_data/infrastructure/storage/iceberg/schemas.py:3-6,77-100`; `packages/market_data/infrastructure/storage/silver/trades_storage.py:55-74`. |
| **Impacto** | Divergencia de columnas/IDs entre escritor (`trades_storage`) y consumidores que usen el SSOT; riesgo de colisión de field-IDs Iceberg (1–9 vs 101–110) y de evolución incompatible (Iceberg spec §3.5). |
| **Qué está documentado** | Un único SSOT gobierna todos los schemas del medallón. |
| **Qué ocurre realmente** | El escritor de `silver.trades` define su propio schema divergente. |
| **Contradicción** | SCHEMA_AUTHORITY_CONTRADICTION. |
| **Nivel de certeza** | ALTO (`OBSERVED` ambos archivos). |
| **Dependencias** | pyiceberg, `TradesStorage`, BC-19 (kafka trades schemas) si aplica. |
| **Acción propuesta** | Unificar: `trades_storage` debe importar `TRADES_SCHEMA` (SSOT) o eliminarse la duplicación; añadir test de contrato de schema (estilo `tests/architecture/`). |
| **Estado de decisión** | `PENDING_FIX` — **P1**. |

### H5 — Reproducibilidad documentada vs retención Bronze de 7 días (HIGH)

| Campo | Valor |
|---|---|
| **ID** | H5 |
| **Título** | `DOCUMENTATION_CLAIM_VS_DATA_RETENTION` — reproducibilidad prometida, retención física de 7 días |
| **Severidad** | HIGH |
| **Estado** | `PARTIALLY_CONFIRMED` |
| **Descripción** | `bronze_retention.py` docstring (`:9-16`) afirma que los datos expirados ya fueron versionados en Silver y quedan "accesibles para reproducibilidad vía IcebergStorage". Pero `RETENTION_DAYS_DEFAULT=7` (`:55`) expira snapshots Bronze >7 días; la reproducibilidad *desde Bronze* (reprocesar el pasado raw) queda limitada a 7 días a menos que Silver/Gold baste. |
| **Evidencia** | `OBSERVED` — `bronze_retention.py:9-16,55` (`RETENTION_DAYS_DEFAULT=7`), `:57` (`MIN_KEEP_DAYS=2`). |
| **Archivos/líneas** | `packages/market_data/infrastructure/storage/bronze/bronze_retention.py:9-16,55,57`. |
| **Impacto** | Promesa de lakehouse reproducible (re-ingesta/re-procesamiento) vs retención de raw limitada; afecta investigación/backtesting futuro y auditoría. |
| **Qué está documentado** | Bronze versionado + reproducible vía Iceberg. |
| **Qué ocurre realmente** | Raw expira a 7 días por defecto (dry_run a menos `--execute`). |
| **Contradicción** | DOCUMENTATION_CLAIM_VS_DATA_RETENTION. |
| **Nivel de certeza** | MEDIO-ALTO (`OBSERVED` en código; la promesa de "reproducibilidad" depende de qué capa se considere reproducible — INFERRED). |
| **Dependencias** | Iceberg `expire_snapshots`, `SilverStorage`, decisión de retención. |
| **Acción propuesta** | Documentar explícitamente la ventana de reproducibilidad real (7d raw / Silver versionado), o ajustar retención a la promesa; revisar si `--execute` se invoca en producción. |
| **Estado de decisión** | `PARTIALLY_CONFIRMED` — **P1**. |

### H6 — Event-driven/Kappa documentado vs desplegado (HIGH)

| Campo | Valor |
|---|---|
| **ID** | H6 |
| **Título** | `DOCUMENTED vs IMPLEMENTED vs DEPLOYED/ACTIVE` — arquitectura Kappa/event-driven |
| **Severidad** | HIGH |
| **Estado** | `PARTIALLY_CONFIRMED` |
| **Descripción** | ADR-0002 declara la arquitectura objetivo event-driven/Kappa y el log (2026-08-02) afirma que la migración Kappa "está completa para el pipeline REST actual (`OHLCVPipeline`)". Realidad: `KAFKA_ENABLED` default `false` (`config/base.yaml:45`), sin consumidores en el código auditado, `NullPublisher` prohibido en producción (`ohlcv_pipeline.py:247-252`), `publish_domain_event` solo a event bus in-process (`runtime.py:328`). El estado de consumidores externos no fue verificado. |
| **Evidencia** | `OBSERVED` — `config/base.yaml:45` `enabled: ${oc.env:KAFKA_ENABLED,false}`; `ohlcv_pipeline.py:247-252` (guard NullPublisher en producción); `runtime.py:328-348` (event bus opcional, default `None`); `OBSERVED` en tracking.yaml:735 ("sin consumidores reales en packages/apps/ocm"); `DOCUMENTED` ADR-0002 + log. |
| **Archivos/líneas** | `config/base.yaml:45`; `config/env/production.yaml:42`; `packages/market_data/application/pipelines/ohlcv_pipeline.py:247-252`; `packages/market_data/application/pipeline/runtime.py:328-348`; `docs/architecture/0002-event-driven-kappa-architecture.md`; `docs/plans/tracking.yaml:724-739,804-820` (F-027, B-43, F-031/B-46). |
| **Impacto** | Contradicción de estado: "Kappa completa" (DOCUMENTED) vs Kafka desactivado por defecto y sin consumidores (DEPLOYED/ACTIVE incierto). Riesgo de asumir backbone de eventos que no está activo. |
| **Qué está documentado** | ADR-0002 objetivo Kappa; log 2026-08-02 "migración completa para pipeline REST". |
| **Qué ocurre realmente** | `KAFKA_ENABLED=false` por defecto; publisher obligatorio pero no desplegado por defecto; sin consumidores en el código auditado; consumidores externos `UNKNOWN`. |
| **Contradicción** | DOCUMENTED vs IMPLEMENTED vs DEPLOYED/ACTIVE. |
| **Nivel de certeza** | MEDIO (`OBSERVED` en código; estado de despliegue externo `UNKNOWN`). |
| **Dependencias** | F-027, B-43, F-031/B-46, `ocm/config/env_vars.py` (SSOT `OCM_*`), docker-compose. |
| **Acción propuesta** | Alinear ADR-0002/log con el despliegue real; decidir si el fail-fast de `NullPublisher` debe activarse por defecto (producción) o documentar `KAFKA_ENABLED=true` como requisito de despliegue live. |
| **Estado de decisión** | `PARTIALLY_CONFIRMED` — **P1**. |

## 3. Ranking de prioridad

| Prioridad | Hallazgos | Racional |
|---|---|---|
| **P0** | H2 (P&L/fill real), H3 (stop-loss), H1 (estado live/gate) | Integridad de ejecución y protección de capital en live; H1 agrupado por ser el mismo camino de ejecución. |
| **P1** | H4 (autoridad schema), H5 (reproducibilidad), H6 (event-driven) | Consistencia arquitectónica de datos y backbone de eventos. |
| **P2/P3** | Docstrings obsoletos (H1-doc), README, matices de retención | Documentación/cleanup; no bloquean. |

## 4. Análisis BC: ¿Debe Backtesting ser un Bounded Context propio? (READ-ONLY)

**Conclusión propuesta: NO hoy — `DEFERRED_PENDING_S1`.** Registrado como
`POTENTIAL_ARCHITECTURE_QUESTION`.

### 4.1 Ownership de conceptos (mapeado en el código actual)

| Concepto | Propietario actual | Evidencia |
|---|---|---|
| Signal | `shared` (tipo) + `trading` (generación) | `shared/types/signal.py`, `shared/contracts/boundaries.py`, `trading/strategies/` |
| Strategy | `trading.strategies` | `strategies/base.py`, `ema_crossover.py`, `registry.py` |
| Order / Fill | `trading.execution` | `execution/order.py`, `oms.py`, `fill_sync.py`, `transport.py` |
| Position | `portfolio` | `portfolio/models/position.py`, `ports/position_store.py` |
| P&L / métricas | `trading.analytics` | `analytics/performance.py`, `trade_record.py`, `trade_tracker.py` |
| Riesgo | `trading.risk` | `risk/manager.py`, `risk/models.py` |
| Datos históricos | `market_data` (Gold) + `research` (lectura) | `apps/research/data/data_access.py`, `market_data` FeatureSource |

### 4.2 Hallazgos del análisis

- **No existe motor de backtest ni experimentos**: grep de `backtest|walk_forward|cross_val`
  en `packages/`, `apps/`, `shared/` → solo docstrings (`grid_alignment.py:32`,
  `domain/exceptions/__init__.py:276`, `research/data/data_access.py:6`). Cero código de
  simulación, walk-forward, CPCV u OOS.
- **Los conceptos ya tienen dueño**: strategy/signal/order/fill/pnl/risk viven en `trading`;
  posición en `portfolio`. Un BC "backtesting" duplicaría o movería todos ellos (viola
  BC-13/BC-36/BC-44 y el principio de no-sobrediseño de la KB governance).
- **BC-09/BC-12/BC-13/BC-36/BC-44** ya aíslan trading.strategies de execution/analytics:
  el motor de backtest, cuando exista, puede vivir como *modo* de `trading` (reusando
  strategies/analytics) sin nueva frontera.
- **No hay requisito concreto de escala**: sin motor, sin usuarios, sin necesidad de
  despliegue independiente → crear un BC nuevo ahora sería arquitectura especulativa.
- **R1 (Bybit reference) marca fechas de "cara lenta"** aplicables a simulaciones de datos
  históricos, pero no justifica un BC: es referencia de mecánica, no especificación.

### 4.3 Decisión (A/B/C/D)

| Opción | Resultado |
|---|---|
| A) BC propio | **NO ahora** — duplicaría conceptos de `trading`/`portfolio`; sin código existente. |
| B) Dentro de un BC existente (`trading`) | **Dirección preferida futura** — como modo/motor que reutiliza `strategies`+`analytics`+`risk`, consumiendo Gold vía `research`/FeatureSource. |
| C) No maduro aún | **Estado actual** — `DEFERRED_PENDING_S1`. |
| D) Evidencia insuficiente | Rechazado parcialmente: la evidencia (ownership + cero código) sí permite concluir C. |

**Estado del item:** `POTENTIAL_ARCHITECTURE_QUESTION` — *"Should Backtesting become its
own Bounded Context?"* — status `DEFERRED_PENDING_S1`. Re-evaluar cuando exista un motor de
backtest real y evidencia de necesidad de frontera/deploy independiente.

## 5. Siguiente paso (S1)

| Campo | Valor |
|---|---|
| **ID** | S1 |
| **Título** | Arreglar cadena P&L/riesgo real (fill_price → P&L → drawdown → stop-loss) |
| **Estado** | `PROPOSED_NEXT_STEP` / `WAITING_FOR_APPROVAL` (**no implementado**) |
| **Alcance** | (a) propagar `fill_price` real del transporte→`OMS._fill`→`record_close`; (b) `record_close(pnl real, con costos)`; (c) habilitar drawdown-halt real; (d) implementar stop-loss configurado; (e) corregir docstring/gate de H1. |
| **Puerta de entrada** | Aprobación explícita del usuario; impl en paper primero; tests + CI. |
| **Nota** | No confundir con S1-S2 del roadmap previo (este S1 es el único siguiente paso acordado). |

## 6. Qué NO se tocó en esta fase (por restricción)

- Ningún cambio de código, configuración, Docker, CI, contracts, ADRs, `tracking.yaml`,
  `pyproject.toml` ni KB (`manifest.yaml`, mappings, PDFs).
- Backtesting BC: análisis READ-ONLY; no se creó `packages/backtesting/` ni contratos.
- R1 (`docs/knowledge/notes/bybit-perpetuals-reference.md`): no modificado; usado solo como
  referencia de mecánica. Su clasificación por afirmación se mantiene.
