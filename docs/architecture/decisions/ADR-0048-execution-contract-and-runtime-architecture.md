# ADR-0048: Execution Contract & Runtime Architecture

**Estado:** ACEPTADO CON ACLARACIONES (G4.3 — 2026-09-28)
**Decisión humana asociada:** D5 = DECIDED — HUMAN — 2026-09-28 + MASTER ARCHITECTURE TRANSFORMATION mandate = DECIDED — HUMAN — 2026-09-28 (Q1-Q8) + G4.3 = ACCEPT WITH CLARIFICATIONS — 2026-09-28
**Materialización:** REALIZADA VÍA MANDATO 2026-09-28 — PENDIENTE DE HUMAN REVIEW (GATE A) (RESUELTO: ACEPTADO CON ACLARACIONES, G4.3 — 2026-09-28)
**Fecha:** 2026-09-28
**Bounded context(s) afectado(s):** ocm (plataforma), market_data, trading, portfolio, apps, shared, CI/CD
**Base normativa:** ADR-0047 (D5) + `docs/proposals/EXECUTION-CONTRACT-DECISION-PACK-2026-09-28.md` (PROPUESTA elevada por este ADR) + mandato 2026-09-28 (Q1-Q8 resueltos por el humano).

## 1. Contexto

ADR-0047 fijó el modelo arquitectónico de ejecución (D5): BC ≠ PROCESS, BC ≠ ENTRYPOINT, MODE ≠
BC, DEPLOYMENT MECHANISM ≠ ARCHITECTURE; tres capas ADR → Execution Registry → Consumers; PUBLIC =
registro explícito en el execution contract; `app.cli.main` = APPLICATION BATCH ENTRYPOINT y
`market_data.main` = MARKET DATA SERVICE ENTRYPOINT (conceptualmente `python -m market_data.main`);
trading/portfolio Nivel 2 EMBEDDED sin `main.py`; Backtest = capacidad objetivo no implementada.

ADR-0047 §14 registró `[project.scripts]` como "registro ejecutable vigente y candidato a
materializar el execution contract", y difirió (ADR-0047 §20/§21) el diseño concreto del
Execution Contract (12 campos + matrices) y la migración de consumers (fases A-L) a un
Human/Build Gate posterior.

El humano resolvió ese gate mediante el mandato **"OCM — MASTER ARCHITECTURE TRANSFORMATION"
(2026-09-28)**, que autoriza: materializar ADR-0048, materializar el Execution Registry en
`config/execution_registry.yaml`, ejecutar las fases de migración A-L con gates, y fijar la
arquitectura objetivo de Market Data. Este ADR materializa la decisión; NO migra consumers ni
crea registros (Phase B en adelante, tras sus gates).

## 2. Problema

El execution contract no estaba materializado: `pyproject.toml [project.scripts]`, `run.sh`,
Dockerfile, docker-compose, systemd y CI actuaban como fuentes implícitas — y a veces
contradictorias (DISP-D5-01 run.sh "SSOT"; DISP-D5-02 refs `trading.main`/`portfolio.main` a
módulos inexistentes) — de qué es un entrypoint público. Faltaba:

- un registro normativo consultable (qué entrypoints existen, con qué rol/frontera/modo/tipo/estado);
- la autoridad única para distinguir PUBLIC vs INTERNAL a nivel de artefacto;
- el orden y los gates de migración de los consumers al contrato.

## 3. Decisión

Este ADR adopta el **Execution Contract** (12 campos + 3 matrices) y el **Execution Registry**
materializable en `config/execution_registry.yaml`, con la autoridad jerárquica:

```
      ADR-0047 (§14, D5-04)
              │
              ▼
      THIS ADR (ADR-0048)
   Execution Contract authority
              │
              ▼
   EXECUTION REGISTRY (config/execution_registry.yaml)
              │
    ┌─────────┼──────────┐
    ▼         ▼          ▼
 pyproject  systemd   Docker/run.sh/CI
  (registry  (ExecStart)  (consumers)
   execute)
```

Regla de autoridad materializada (Q8): **`ADR-0048 → Execution Contract → Approved Registry
Rule → Execution Registry`**. Registry con derivación de ADR; consumers derivan del Registry; un
consumer NO legisla por existir (D5-01, D5-04, ADR-0047 §15).

## 4. Resoluciones Q1-Q8 del mandato (human gate cerrado)

| Pregunta | Resolución |
|---|---|
| **Q1** — ¿dónde materializar el registry? | **NO** en `policies/registry.yaml` (gobernanza/policy). Crear artefacto dedicado **`config/execution_registry.yaml`** (separado de runtime config; sin colisión con grupos Hydra; Hydra compone solo vía `defaults` explícito de `config/config.yaml`, no autoloads `.yaml` root arbitrarios). |
| **Q2** — ¿registrar `market_data.main` en `[project.scripts]`? | `market_data.main` = **PUBLIC MARKET DATA SERVICE ENTRYPOINT**. **PUBLIC ≠ necesariamente console script**. Mantener invocación actual `python -m market_data.main` (Docker/Compose/systemd). NO añadir automáticamente `[project.scripts] market-data = …`; un console script para `market_data` sería un cambio separado y no está autorizado por este ADR. |
| **Q3** — `run.sh` (DISP-D5-01) | `run.sh` **NO es SSOT**. Es **developer/operational convenience layer**. Eliminar el claim "SSOT idéntico a [project.scripts]" en la fase H. El SSOT es el Execution Registry. |
| **Q4** — Compose (DISP-D5-02) | Refs `python -m trading.main` / `python -m portfolio.main` = **DEAD RUNTIME REFERENCES** (módulos inexistentes). Corregir/eliminar **sin crear** `trading.main`/`portfolio.main` (ADR-0047 §11/§12). |
| **Q5** — fases y orden | Aprobar fases A-L del Decision Pack con su secuencia y gates (ver §9). |
| **Q6** — enum de `estado` | `ACTIVE / DISCREPANCY / DEAD / NOT_IMPLEMENTED` (UPPER_SNAKE; ver §5 campo 11). |
| **Q7** — roles/fronteras de live/paper/streaming/ocm-api | Roles del Decision Pack §4.1 materializados: live/paper/streaming = APPLICATION RUNTIME ENTRYPOINTS dentro de APPLICATION EXECUTION; `ocm-api` = APPLICATION API ENTRYPOINT (experimental); `market_data.main` = SERVICE (MARKET DATA SERVICE EXECUTION). |
| **Q8** — autoridad del registry | Cadena `ADR-0048 → Execution Contract → Approved Registry Rule → Execution Registry` (H3-H6/H3-H7). El registro en el Registry es la condición de PUBLIC; registry y consumers se verifican por paridad (fase I). |

## 5. Execution Contract — 12 campos

Cada **entrypoint público registrado** tiene una entrada con 12 campos:

| # | Campo | Semántica | Base normativa |
|---|---|---|---|
| 1 | `id` | identificador estable (`EP-001`, …) | convención inventario |
| 2 | `entrypoint` | símbolo de invocación (`módulo:main` o console script) | hecho verificado |
| 3 | `rol` | rol arquitectónico | D5-02, ADR-0047 §5/§10-12, Q7 |
| 4 | `frontera` | `APPLICATION EXECUTION` / `MARKET DATA SERVICE EXECUTION` | D5-02, ADR-0047 §5 |
| 5 | `modo` | `BATCH`, `STREAMING`, `LIVE`, `PAPER`, futuro `BACKTEST` | D5-01, ADR-0047 §3 |
| 6 | `tipo` | `PUBLIC` / `INTERNAL` | D5-01 |
| 7 | `invocacion` | mecanismo actual (`-m`, console script, ExecStart) | D5-03 (mecanismo ≠ contrato) |
| 8 | `registro` | registro/materialización explícita | D5-04, ADR-0047 §14, Q1 |
| 9 | `bc` | bounded context(s) alcanzados | ADR-0024, ADR-0047 §12-13 |
| 10 | `consumers` | consumers autorizados | D5-04, ADR-0047 §15 |
| 11 | `estado` | `ACTIVE` / `DISCREPANCY` / `DEAD` / `NOT_IMPLEMENTED` | Q6 |
| 12 | `deuda` | discrepancia asociada (`DISP-D5-0X` / `NINGUNA`) | ADR-0047 §16 |

## 6. Matrices del contract

### 6.1 Rol/frontera (ancla: D5-02 + ADR-0047 §§10-12 + Q7)

| Entrypoint | Rol | Frontera | BCs |
|---|---|---|---|
| `app.cli.main` | APPLICATION BATCH ENTRYPOINT | APPLICATION EXECUTION | ocm |
| `app.cli.live_hydra` | APPLICATION RUNTIME ENTRYPOINT (LIVE) | APPLICATION EXECUTION | trading+portfolio (embedded N2) |
| `app.cli.paper_hydra` | APPLICATION RUNTIME ENTRYPOINT (PAPER) | APPLICATION EXECUTION | trading+portfolio (embedded N2) |
| `app.cli.streaming_hydra` | APPLICATION RUNTIME ENTRYPOINT (STREAMING) | APPLICATION EXECUTION | market_data (ingestión) |
| `api.main` (`ocm-api`) | APPLICATION API ENTRYPOINT (experimental) | APPLICATION EXECUTION | ocm |
| `market_data.main` | MARKET DATA SERVICE ENTRYPOINT | MARKET DATA SERVICE EXECUTION | market_data (N1, runtime propio) |
| `trading.main`/`portfolio.main` | — (refs muertas) | — | — (DISP-D5-02; NO crear) |

### 6.2 Consumer × entrypoint (paridad objetivo)

| Consumer | `app.cli.main` | `live` | `paper` | `streaming` | `ocm-api` | `market_data.main` |
|---|---|---|---|---|---|---|
| `[project.scripts]` | ocm ✔ | live ✔ | paper ✔ | streaming ✔ | ocm-api ✔ | — (Q2: no añadir) |
| `run.sh` | ocm ✔ | live ✔ | paper ✔ | (opcional tras fase H) | (opcional tras fase H) | — |
| Dockerfile | — | — | — | — | — | ✔ |
| docker-compose | — | — | — | — | — | ✔ (359); ✘ refs trading/portfolio (DISP-D5-02 → fase F) |
| systemd | — | — | — | ✔ (streaming svc) | — | ✔ (market-data svc) |
| CI config-validation | ✔ | — | — | — | — | — |

### 6.3 Modo × BC (invariante MODE ≠ BC)

| Modo | BCs |
|---|---|
| `BACKTEST` (objetivo, NOT IMPLEMENTED) | trading+portfolio (embedded) |
| `LIVE` | trading+portfolio (embedded) + market_data vía servicio |
| `PAPER` | trading+portfolio (embedded) |
| `STREAMING` | market_data (ingestión) |
| `SERVICE (Market Data)` | market_data (runtime N1) |

## 7. Arquitectura de aplicación (target, mandato)

```
                     OCM
                      │
                  APPLICATION
              ┌───────┴────────┐
         BACKTEST... PAPER   LIVE
              └───────┬────────┘
                      ▼
            APPLICATION CORE
    Strategy · Trading · Portfolio · Risk · Execution
                      │
              ┌───────┴────────┐
              ▼                ▼
       MARKET DATA PORT   EXECUTION PORT
              │                │
        Market Data Service    Exchange/Broker
```

- Trading/portfolio permanecen Nivel 2 EMBEDDED dentro de la aplicación (ADR-0047 §12).
- Market Data = servicio N1 independiente (ADR-0024/ADR-0047 §11).
- Ports hacia Market Data (lectura) y hacia ejecución (órdenes) como contractos, no imports directos cross-BC.
- No se crean BCs, servicios ni procesos nuevos. No big-bang.

## 8. Arquitectura objetivo de Market Data (mandato §9-18)

```
Market Data Service
  Ingestion → Normalization → Canonical Instrument Identity → Canonical Market Data
    → Events/State/Derivatives → Data Quality → Bronze → Silver → Gold → Iceberg
```

- **Ingestion**: feeds/adapters (bybit, kucoin spot/futures, ccxt outbound) → flujo real-time + polling/replay externo.
- **Normalization**: `ports/outbound/normalization.py` = SSOT de transforms (polars; frontera única `ohlcv_transformer.py`).
- **Canonical Instrument Identity**: instrument_id, venue, símbolos, market_type (SPOT/PERPETUAL/FUTURE/OPTION…). Estado actual: catalog parcial en adapters/domain → dataset canónico a definir en Phase E.
- **Canonical Market Data**: Trades (base atómica; OHLCV derivable), OrderBook (L1-L2/Spread/Mid/microstructure), eventos/estado/metadata.
- **Data Quality**: correctness, completeness, freshness, lineage, sequence integrity, gap detection, cross-source consistency, reproducibilidad.
- **Medallion**: Bronze (raw) → Silver (canonical/validated) → Gold (derived/features). OHLCV EXISTS; Trades/OrderBook PARTIAL; resto NOT_IMPLEMENTED (ver baseline §6).

### Non-goals de Market Data (mandato §16, §36, §51)
- NO inventar schemas/datasets nuevos en este ADR.
- NO convertir Gold en source canónico (Gold = derivado).
- NO big-bang: Current→Target matrix espera Phase E.
- Si el análisis de Market Data demostrara necesidad de ADR independiente, se registra como decisión posterior (no se inventa aquí).

## 9. Migración de consumers — fases A-L (Q5 aprobadas; gates obligatorios)

| Fase | Objetivo | Gate |
|---|---|---|
| **A. Contrato ADR** | Este ADR-0048 (materializado ahora; Q1-Q8 resueltos) | HUMAN GATE (GATE A — este reporte) |
| **B. Registry** | Materializar `config/execution_registry.yaml` con las entradas del contract (Q1/Q8) | GATE: Build + HUMAN (ENTRYPOINT PUBLIC clasificado) |
| **C. `app.cli`** | Alinear `app.cli.main`/`live`/`paper`/`streaming` como APPLICATION RUNTIME ENTRYPOINTS | Build |
| **D. market_data runtime** | Alinear `market_data.main` con frontera SERVICE (D5-02, Q2) | Build |
| **E. Dockerfile** | CMD según contract (sin cambio de arquitectura) | Build |
| **F. docker-compose** | Corregir DISP-D5-02 (Q4): eliminar refs dead sin crear módulos | Review |
| **G. systemd** | Alinear ExecStart de templates con el Registry | Build |
| **H. run.sh** | Corregir DISP-D5-01 (Q3): eliminar claim SSOT; conveniencia | Build |
| **I. Tests de paridad** | Assertions contract↔consumers (matriz §6.2) | Build + CI |
| **J. CI** | Gates que validen registry↔run.sh↔compose↔systemd | HUMAN + CI |
| **K. Refs legacy** | Grep/cleanup de `trading.main`/`portfolio.main` en docs/scripts | Build |
| **L. Validación final** | lint-imports + ruff + mypy + pytest + audit validator + cierre + reporte §12 | HUMAN ACCEPTANCE |

Cada fase SOLO tras su gate (ADR-0047 §20). No se auto-inicia Phase B sin el GATE A humano.

## 10. Registry: invariancia y no-conflicto

- El registry en `config/execution_registry.yaml` NO es parte de los Hydra `defaults` de
  `config/config.yaml`; ningún tool hace glob de `config/*.yaml` (verificado Phase 0).
- `policies/registry.yaml` permanece como policy registry de gobernanza, **sin** entradas de
  entrypoints. ADR-0048 NO se añade a `related_adrs` de `registry.yaml` (evita WARN M25 nuevo);
  su gobernanza se registra en `tracking.yaml` (H3).
- `[project.scripts]` permanece como registro ejecutable; el Registry es la autoridad
  normativa; la paridad la verifica la fase I/J.

## 11. Consecuencias

- Autoridad clara PUBLIC/INTERNAL a nivel de artefacto (D5-01 materializado en registry).
- Identidad estable por entrypoint (`id` EP-NNN) para trazabilidad sin acoplar a rutas.
- Estado explícito (ACTIVE/DISCREPANCY/DEAD/NOT_IMPLEMENTED) → consumidores y CI pueden
  verificar paridad y deuda.
- `run.sh` baja de "SSOT" a conveniencia; su claim se corrige en fase H.
- Compose pierde refs dead de `trading.main`/`portfolio.main` sin crear los módulos (fase F).
- `market_data.main` sigue como SERVICE vía `-m` (Docker/Compose/systemd), sin console script nuevo.
- Arquitectura objetivo de Market Data documentada; inventario de datasets del baseline sirve
  de base a la Current→Target matrix (Phase E).
- Contratos BC-NN existentes intactos; no se crean contratos técnicos nuevos en este ADR.

## 12. Reporte final comprometido

`docs/audit/OCM-TARGET-ARCHITECTURE-EXECUTION-MARKET-DATA-REPORT.md` (26 secciones, mandato §58)
se materializa en Phase L, no ahora. Baseline: `docs/audit/OCM-PRE-IMPLEMENTATION-BASELINE-2026-09-28.md`.

## 13. Non-goals

Este ADR NO:

- implementa la arquitectura de Market Data, ni schemas/datasets nuevos, ni big-bang;
- modifica `pyproject.toml`, `run.sh`, Dockerfile, docker-compose, systemd, CI, código ni tests;
- crea `config/execution_registry.yaml` (eso es GATE A → Phase B);
- corrige DISP-D5-01/DISP-D5-02 (fases F/H);
- crea `trading.main`/`portfolio.main`; implementa Backtest;
- modifica `policies/registry.yaml` ni `related_adrs`; modifica contratos BC-NN;
- promueve BCs a procesos independientes (ADR-0047 §13);
- convierte Gold en source canónico.

## 14. Explicit deferred

- Materialización del Registry (Phase B, tras GATE A).
- Migración de consumers (fases C-K). Current→Target matrix de Market Data (Phase E).
- ADR independiente de Market Data si el análisis lo exige (decisión posterior, no inventar).
- Implementación de BACKTEST. Resolución de M21/M25/M20/B-25/ARCH-006 (pre-existentes, se preservan).
- Todo lo declarado fuera de alcance por ADR-0045/0046 y ADR-0047 (H4, H5, B4, B5, M26 enforcement,
  `adr:null`, precedencia registry↔ADR) permanece sin resolver.

## 15. Authority/acceptance state

- **D5 = DECIDED — HUMAN** (2026-09-28). **Mandate = DECIDED — HUMAN** (2026-09-28; Q1-Q8).
- **DECISION ≠ MATERIALIZATION ≠ IMPLEMENTATION ≠ ACCEPTANCE** (H3).
- ADR-0048 materializa el mandato; NO lo sustituye ni amplía.
- **Aceptación humana: G4.3 = ACCEPT WITH CLARIFICATIONS (2026-09-28).**
  GATE A resuelto: Execution Contract + Execution Registry + Q1-Q8 quedan
  ACEPTADOS CON ACLARACIONES. Las aclaraciones formales se incorporan en la
  sección 16. La aceptación NO autoriza implementación funcional ni
  migración adicional; las fases B-L ya materializadas no se reinterpretan como
  autorización de nuevas fases (cada fase requiere su propio gate, sección 9).
- Respeta H1/H2/H3: evidence ≠ authority, CI ≠ authority, existencia ≠ authority,
  autoridad indeterminada → human gate.

## 16. Aclaraciones formales de aceptación (G4.3 — 2026-09-28)

El humano aceptó este ADR CON ACLARACIONES (G4.3, 2026-09-28). Aclaraciones
estrictamente documentales/arquitectónicas: **no introducen decisiones nuevas de
diseño** ni alteran el contenido técnico de Q1-Q8.

### 16.1 Separación de responsabilidades

| Capa | Rol | Ejemplificación en OCM |
|---|---|---|
| **ADR** | WHY — decisión arquitectónica normativa | ADR-0047, ADR-0048 |
| **CONTRACT** | WHAT MUST BE TRUE — invariantes/condiciones requeridas | §5 (12 campos) + §6 (matrices) + §10 (invariancia) de este ADR |
| **REGISTRY** | representación ejecutable/registrable del contrato aprobado | `config/execution_registry.yaml` |
| **IMPLEMENTATION** | HOW — realización técnica | consumers (pyproject, run.sh, Docker, Compose, systemd) |
| **VALIDATION** | HOW KNOWN TRUE — verificación de conformidad | fases I/J; CI parity tests |
| **EVIDENCE** | WHAT WAS OBSERVED — observaciones registradas | baseline, tracking, reportes de audición |

Ninguna capa inferior crea autoridad sobre las superiores. En particular:
- el **Execution Registry** es la representación machine-readable del contrato
  aprobado; representa la decisión, **no crea autoridad**;
- **CI/parity** verifica conformidad; **no crea autoridad**;
- **Evidence** registra lo observado; **no crea autoridad**;
- la **existencia de un artefacto** (módulo, `main.py`, `python -m`, consumer,
  cada fichero de infra) **no confiere autoridad** por sí misma (D5-01, D5-04).

### 16.2 Invariantes formales (I-ENTRY)

Se adoptan formalmente (sin cambios de diseño; donde el documento ya contenía la
formulación, se conserva la existente):

- **I-ENTRY-01** — ENTRYPOINT ≠ BC.
- **I-ENTRY-02** — ENTRYPOINT ≠ PROCESS.
- **I-ENTRY-03** — ENTRYPOINT ≠ SERVICE.
- **I-ENTRY-04** — ENTRYPOINT ≠ DEPLOYMENT UNIT.
- **I-ENTRY-05** — PUBLIC ENTRYPOINT requiere registro explícito en el
  Execution Contract/Registry.
- **I-ENTRY-06** — APPLICATION MODE no implica automáticamente un BC.
- **I-ENTRY-07** — BACKTEST/PAPER/LIVE comparten los contratos de
  aplicación/dominio cuando el contrato sea aplicable; las diferencias de runtime
  quedan detrás de ports/adapters.
- **I-ENTRY-08** — Venue-specific behavior permanece detrás de adapters.
- **I-ENTRY-09** — Application Core no depende directamente de infraestructura de
  venue/deployment.
- **I-ENTRY-10** — formulación equivalente a I-ENTRY-08 (venue-specific detrás de
  adapters); se conserva la de I-ENTRY-08 como única fuente (sin duplicado).
- **I-ENTRY-11** — ENTRYPOINT ≠ PROCESS ≠ SERVICE ≠ BC ≠ DEPLOYMENT UNIT
  (superconjunto de I-ENTRY-01..04).
- **I-ENTRY-12** — Autoridad/contrato/seguridad → fail-closed; zonas no críticas →
  fail-soft únicamente cuando sea apropiado.

Nota de no-duplicación: I-ENTRY-10 replica el texto de I-ENTRY-08 y I-ENTRY-11
compone I-ENTRY-01..04; no se crean invariantes nuevas no previstas.

### 16.3 Frontera de autoridad

- La autoridad se determina por la gobernanza aprobada / H2-E: ausencia de regla
  aprobada → FAIL-CLOSED → human gate.
- Registry, CI, Evidence y la existencia de artefactos no crean autoridad.
- La aceptación de este ADR NO autoriza implementación funcional, refactoring ni
  migración de consumers; las fases B-L materializadas permanecen como están
  hasta su gate correspondiente.

## Relación con H1/H2/H3

- H1 = CUSTOM/HYBRID; H2 = H2-E (ADR-0045); H3 = HUMAN (ADR-0046). Este ADR no los modifica.
- La cadena del registry (`ADR-0048 → Contract → Approved Registry Rule → Registry`) es la
  instanciación de H3-H6/H3-H7 para execution entrypoints.

## Relación con ADR-0024, ADR-0039, ADR-0047

- ADR-0024: conserva market_data N1, trading/portfolio N2. D5/este ADR solo definen ejecución.
- ADR-0039: systemd = runtime canónico de aplicaciones; Docker/Compose = infraestructura. Ambos
  son consumers del contract (no autoridad).
- ADR-0047: este ADR materializa lo que ADR-0047 §14/§20/§21 difirió. No lo modifica.

## Referencias

- D5 = DECIDED — HUMAN 2026-09-28 (D5-01=D, D5-02=A, D5-03=B, D5-04=D).
- Mandato MASTER ARCHITECTURE TRANSFORMATION 2026-09-28 (Q1-Q8).
- ADR-0047, ADR-0024, ADR-0039, ADR-0044, ADR-0045, ADR-0046.
- Decision Pack `docs/proposals/EXECUTION-CONTRACT-DECISION-PACK-2026-09-28.md` (PROPUESTA; este ADR la eleva).
- Baseline: `docs/audit/OCM-PRE-IMPLEMENTATION-BASELINE-2026-09-28.md`.
- Código: `pyproject.toml` [project.scripts] 176-181; `run.sh` 4-5/31-41; `Dockerfile:40`;
  `docker-compose.yml` 359/407/451; systemd templates 16; `ocm-ci.yml` config-validation;
  `packages/market_data/main.py`; `config/datasets.yaml`; `shared/kafka/schemas/`.