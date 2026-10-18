# Execution Contract — DECISION PACK (D5 materializacón; propuesta para gates posteriores)

> **ESTADO: PROPUESTA.** Documento de diseño para revisión humana (Human/Build Gate posterior).
> NO aprobado, NO implementado, NO migra nada.
>
> **Base normativa:** D5 = DECIDED — HUMAN (2026-09-28; D5-01=D, D5-02=A, D5-03=B, D5-04=D) +
> ADR-0047 (HR PASS, RESULTADO B, aceptado con NB-1/NB-2 no bloqueantes). Ver tracking.yaml
> `d5_entrypoints_execution_model`.
>
> **Alcance de este documento:** diseñar en concreto el **Execution Contract** (12 campos +
> matrices) y el orden de migración de consumers (fases A-L) que ADR-0047 §20/§21 declara
> diferidos. Es una **propuesta**: ningún campo, matriz ni fase es autoridad hasta su aprobación.
>
> **Non-goals:** este documento NO modifica código, `pyproject.toml`, `run.sh`, Dockerfile,
> docker-compose, systemd, CI, tests ni ADR-0047. NO crea `trading.main`/`portfolio.main`.
> NO implementa Backtest.
>
> **Fecha:** 2026-09-28.

---

## 0. Contrato de trazabilidad

Cada propuesta conserva su cadena: `D5 human decision → ADR-0047 (autoridad) → execution
contract (este documento) → execution registry → consumers`. Hechos verificados contra el
repositorio (ver §2). Ningún artefacto por existir es autoridad (D5-01, D5-04).

## 1. Problema que resuelve el Execution Contract

ADR-0047 fijó el modelo (BC ≠ PROCESS/ENTRYPOINT, MODE ≠ BC, DEPLOYMENT ≠ ARCHITECTURE,
3 capas ADR→Registry→Consumers) pero difirió el diseño concreto del registro. Este design
pack concreta **qué se registra** (campos), **dónde se materializará** (registry), **cómo
se verifica** (matrices de paridad) y **en qué orden se migra** (fases A-L).

## 2. Inventario verificado de consumers y entrypoints

| Artefacto | Entrada | Verificación |
|---|---|---|
| `pyproject.toml` `[project.scripts]` (176-181) | `ocm`, `ocm-api`, `live`, `paper`, `streaming` | registrados |
| `market_data.main` | SERVICE entrypoint | NO en `[project.scripts]` ni `run.sh` |
| `run.sh` (33-35) | `ocm`, `live`, `paper` | cabecera reclama "SSOT idéntico a [project.scripts]" — contradicción (faltan `ocm-api`, `streaming`). DISP-D5-01 |
| `Dockerfile:40` | `CMD ["python","-m","market_data.main"]` | invoca MARKET DATA SERVICE |
| `docker-compose.yml:359` | `command: python -m market_data.main` | OK |
| `docker-compose.yml:407,451` | `python -m trading.main`, `python -m portfolio.main` | módulos inexistentes. DISP-D5-02 (DEAD RUNTIME REFERENCE) |
| `deploy/systemd/templates/ocm-market-data.service.template:16` | `python -m market_data.main` | OK |
| `deploy/systemd/templates/ocm-streaming.service.template:16` | `python -m app.cli.streaming_hydra` | OK |
| CI `ocm-ci.yml` config-validation | `OCM_VALIDATE_ONLY=true uv run python -m app.cli.main` | ok |
| `scripts/backtest_app_guard.py` | gate de CI | NO es modo de ejecución (Backtest NOT IMPLEMENTED) |

## 3. Execution Contract — 12 campos propuestos

Cada **entrypoint público registrado** (D5-01: PUBLIC = registro explícito) tendrá una
entrada del execution registry con 12 campos:

| # | Campo | Semántica | Base normativa |
|---|---|---|---|
| 1 | `id` | identificador estable del entrypoint (EP-001, …) | convención inventario | 
| 2 | `entrypoint` | símbolo de invocación (`módulo:main` o console script) | hecho verificado |
| 3 | `rol` | rol arquitectónico (D5-02 y derivados) | D5-02, ADR-0047 §5/§10-12 |
| 4 | `frontera` | `APPLICATION EXECUTION` / `MARKET DATA SERVICE EXECUTION` | D5-02, ADR-0047 §5 |
| 5 | `modo` | modo(s) de ejecución (`BATCH`, `STREAMING`, `LIVE`, `PAPER`, futuro `BACKTEST`) | D5-01, ADR-0047 §3 |
| 6 | `tipo` | `PUBLIC` / `INTERNAL` | D5-01 |
| 7 | `invocacion` | mecanismo de invocación actual (`-m`, console script, ExecStart) | D5-03 (mecanismo ≠ contrato) |
| 8 | `registro` | registro explícito materializado (candidato: `[project.scripts]` extendido) | D5-04, ADR-0047 §14 |
| 9 | `bc` | bounded context(s) alcanzados (ocm, market_data, trading, portfolio) | ADR-0024, ADR-0047 §12-13 |
| 10 | `consumers` | consumers autorizados (Docker, Compose, systemd, run.sh, CI, tests) | D5-04, ADR-0047 §15 |
| 11 | `estado` | `ACTIVO` / `DISCREPANCIA` / `DEAD` / `NO_IMPLEMENTADO` | verificación |
| 12 | `deuda` | discrepancia asociada (`DISP-D5-0X` / `NINGUNA`) | ADR-0047 §16 |

## 4. Matrices propuestas

### 4.1 Matriz de role/frontera (ancla: D5-02 + ADR-0047 §§10-12)

| Entrypoint | Rol | Frontera | BCs |
|---|---|---|---|
| `app.cli.main` | APPLICATION BATCH ENTRYPOINT | APPLICATION EXECUTION | ocm (plataforma) |
| `app.cli.live_hydra` | APPLICATION RUNTIME ENTRYPOINT (LIVE) | APPLICATION EXECUTION | trading+portfolio (embedded, N2) |
| `app.cli.paper_hydra` | APPLICATION RUNTIME ENTRYPOINT (PAPER) | APPLICATION EXECUTION | trading+portfolio (embedded, N2) |
| `app.cli.streaming_hydra` | APPLICATION RUNTIME ENTRYPOINT (STREAMING) | APPLICATION EXECUTION | market_data (ingestión) |
| `api.main` (`ocm-api`) | APPLICATION API ENTRYPOINT (experimental) | APPLICATION EXECUTION | ocm |
| `market_data.main` | MARKET DATA SERVICE ENTRYPOINT | MARKET DATA SERVICE EXECUTION | market_data (N1, runtime propio) |
| `trading.main`/`portfolio.main` | — (refs muertas) | — | — (DISP-D5-02: NO crear) |

### 4.2 Matriz consumer × entrypoint (paridad propuesta)

| Consumer | `app.cli.main` | `live` | `paper` | `streaming` | `ocm-api` | `market_data.main` |
|---|---|---|---|---|---|---|
| `[project.scripts]` | ocm ✔ | live ✔ | paper ✔ | streaming ✔ | ocm-api ✔ | ✘ (candidato) |
| `run.sh` | ocm ✔ | live ✔ | paper ✔ | ✘ (DISP-D5-01) | ✘ (DISP-D5-01) | ✘ (no esperado salvo decisión) |
| Dockerfile | — | — | — | — | — | ✔ |
| docker-compose | — | — | — | — | — | ✔ (359); ✘ refs trading/portfolio (DISP-D5-02) |
| systemd | — | — | — | ✔ (streaming svc) | — | ✔ (market-data svc) |
| CI config-validation | ✔ | — | — | — | — | — |

### 4.3 Matriz modo × BC (invariante MODE ≠ BC)

| Modo | BCs |
|---|---|
| `BACKTEST` (objetivo, NO implementado) | trading+portfolio (embedded) |
| `LIVE` | trading+portfolio (embedded) + market_data vía servicio |
| `PAPER` | trading+portfolio (embedded) |
| `STREAMING` | market_data (ingestión) |
| `SERVICE (Market Data)` | market_data (runtime N1) |

## 5. Preguntas de decisión para el humano (gate de diseño)

1. **Q1 — Dónde materializar el Execution Registry:** ¿`[project.scripts]` extendido (registro
   ejecutable ya existente) o un artefacto dedicado (ej. `policies/` o sección en tracking)?
   Recommended: `[project.scripts]` como registro live + este contrato como autoridad (D5-04).
2. **Q2 — Registro de `market_data.main`:** ¿añadirlo a `[project.scripts]` (console script
   propio) manteniendo `-m` como invocación actual, o dejarlo solo vía Docker/systemd?
3. **Q3 — `run.sh` (DISP-D5-01):** ¿alinear con `[project.scripts]` completo (añadir
   `ocm-api`, `streaming` y opcionalmente `market_data`), o eliminar la cabecera "SSOT"?
4. **Q4 — Compose (DISP-D5-02):** ¿eliminar las refs a `trading.main`/`portfolio.main`
   (dead runtime references) sin crear los módulos?
5. **Q5 — Fases A-L y orden:** ¿aprobar el plan §6 con su secuencia y gates?

## 6. Fases de materialización/migración (propuestas A-L)

| Fase | Objetivo | Gate |
|---|---|---|
| **A. Contrato ADR** | Elevar este decision pack a ADR-0048 (Execution Contract) con Q1..Q5 resueltos | Human Decision |
| **B. Registry** | Materializar el registry (según Q1) y registrar entrada `market_data.main` (Q2) | Build |
| **C. `app.cli`** | Alinear `app.cli.main`/`live`/`paper`/`streaming` como application runtimes | Build |
| **D. market_data runtime** | Alinear `market_data.main` con su frontera SERVICE (D5-02) | Build |
| **E. Dockerfile** | CMD según contrato (sin cambios de arquitectura) | Build |
| **F. docker-compose** | Corregir DISP-D5-02 (Q4) | Build + revisión |
| **G. systemd** | Alinear ExecStart de templates con el registry | Build |
| **H. run.sh** | Corregir DISP-D5-01 (Q3) | Build |
| **I. Tests de paridad** | Tests contract↔consumers (matríz 4.2 como assertions) | Build + CI |
| **J. CI** | Gates que validen registry↔run.sh↔compose↔systemd | Human + CI |
| **K. Refs legacy** | Grep/cleanup de refs a `trading.main`/`portfolio.main` en docs/scripts | Build |
| **L. Validación final** | lint-imports + ruff + mypy + pytest + audit validator + cierre | Human Acceptance |

Cada fase se ejecuta SOLO tras su gate; la fase A depende de este doc. No se auto-inicia
ninguna fase sin autorización humana explícita (ADR-0047 §20).

## 7. Información de trazabilidad

- Decisión humana D5: 2026-09-28 (D5-01=D, D5-02=A, D5-03=B, D5-04=D).
- ADR-0047: HR PASS (RESULTADO B, NB-1/NB-2) — aceptado, no editado.
- tracking.yaml `d5_entrypoints_execution_model`: HUMAN ACCEPTANCE GATE APROBADO (2026-09-28).
- ADR-0024 (niveles BC), ADR-0039 (runtime systemd), ADR-0045/0046 (H2/H3).
- Hechos verificados: pyproject [project.scripts] 176-181, run.sh 33-35, Dockerfile:40,
  docker-compose.yml 359/407/451, systemd templates 16, ocm-ci.yml config-validation.