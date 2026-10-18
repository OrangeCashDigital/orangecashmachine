# ARCHITECTURE DISCOVERY AUDIT — 2026-09-27

> ⚠️ **CLASIFICACIÓN DEL ARTEFACTO**
> Este informe representa **evidencia derivada de una auditoría READ-ONLY**. No establece
> autoridad arquitectónica, no aprueba la Target Architecture y no sustituye ADRs,
> contratos, registry, tracking ni decisiones humanas. Todo "destino candidato" es un
> apunte de investigación, NO una orden de migración.

## 1. METADATA

| Campo | Valor |
|---|---|
| Audit | Architecture Discovery Audit |
| Date | 2026-09-27 |
| Repository | orangecashmachine |
| Branch | `feat/m22b-governance` |
| HEAD | `05317f149c57afa05fa5f667b53272e6ace7ba49` |
| Mode | READ-ONLY DISCOVERY |
| Authoring agent | opencode (big-pickle) — Autonomous Agent |
| Status | DERIVED AUDIT EVIDENCE |

## 2. SCOPE

Se investigó (solo lectura, sin mutaciones):

- `apps/` — portafolio de aplicaciones (app CLI, api FastAPI, research gold-read).
- `packages/` — bounded contexts `market_data/`, `trading/`, `portfolio/`.
- `scripts/` — 12 artefactos a nivel raíz (11 `.py` + 1 `.sh`) clasificados.
- `architecture_linter/` — motor AST ARCH-001..010 + `importlinter.toml` + `staged_ports.py`.
- `shared/` — kernel compartido (29 `.py`).
- `infrastructure/` — paquete raíz (stub: solo `__init__.py`).
- `ocm/` — plataforma (config / runtime / observability).
- `deploy/` — systemd, monitoring (prom/loki/grafana/alert), scripts `.sh`.
- `tests/` — mirror de áreas (BCs, kafka, logging, app, api, architecture).
- `config/` — Hydra YAML (22 archivos).
- `policies/` — `registry.yaml`, `evidence.json` (sha256 pins), `semgrep/`.
- `docs/` — ADRs, governance, plans, knowledge, audits.
- CI — `.github/workflows/` (11 workflows; `ocm-ci.yml` analizado en detalle).
- pre-commit — `.pre-commit-config.yaml`.
- `pyproject.toml` — tooling, entrypoints, per-file-ignores.

No se ejecutó ningún guard, test, linter ni comando de verificación con efectos; las
observaciones de contrato/CI se tomaron de lectura de archivos y del estado git (idéntico
al inicio y al final de la auditoría).

## 3. EXECUTIVE SUMMARY

OCM es un **data lakehouse cripto con arquitectura Clean/Hexagonal real y ejecutable**: 3
bounded contexts en `packages/` (market_data, trading, portfolio) gobernados por 51
contratos import-linter (baseline F2.1 ≥ 49 verificado en CI), un kernel compartido
(`shared/`), una plataforma (`ocm/`) y un portafolio de apps (`app` CLI, `api`
experimental, `research` read-only).

El candidato de Target Architecture `{apps/, packages/, tools/{architecture,governance}/,
tests/, docs/, deploy/}` es **en su mayoría coherente con la arquitectura ya ejecutable**,
pero:

1. **`tools/` NO existe hoy**; su contenido candidato está concentrado en dos directorios
   de raíz: `architecture_linter/` y `scripts/`. Reubicarlos dispara **gates de gobernanza
   ya activos** (evidence.json con sha256 de 3 guardas; CI; pre-commit; CODEOWNERS) →
   requeriría decisión humana + ADR; no es reorg. mecánica.
2. **`scripts/` es la raíz más mal agrupada**: mezcla ARCHITECTURE_TOOL, GOVERNANCE_TOOL,
   REPORTING, TEST_SUPPORT, INFRASTRUCTURE, MIGRATION y DEVELOPER_TOOL en un directorio plano.
3. **`infrastructure/` de raíz es un stub vacío** pero figura como root_package y en
   contratos BC (BC-07, BC-16, BC-22) → namespace reservado, no habitable.

Hallazgo de integridad adicional: `Dockerfile` CMD apunta a `python -m market_data.main`
(módulo que existe en `packages/market_data/main.py`), mientras el resto de entrypoints usa
`app.cli.*` (run.sh, pyproject). Documentado; no corregido.

## 4. CURRENT REPOSITORY TOPOLOGY

```
orangecashmachine/
├── packages/            → 3 BCs (market_data 161 .py, trading 23, portfolio 15)
├── apps/                → app (CLI), api (FastAPI exp.), research (gold read-only)
├── shared/              → kernel compartido (29 .py) — SHARED KERNEL
├── ocm/                 → plataforma (config/runtime/observability) — 58 .py
├── architecture_linter/ → motor ARCH-001..010 + importlinter.toml + staged_ports.py
├── scripts/             → 12 artefactos mixtos (guards/gobernanza/reporting/infra)
├── infrastructure/      → STUB vacío (solo __init__.py)
├── deploy/              → systemd (rendered+templates), monitoring, scripts .sh
├── tests/               → mirror de BCs + architecture/ + kafka/ + logging/ + app/ + api/
├── config/              → Hydra YAML (22 archivos; base/env/exchanges/options/storage…)
├── policies/            → registry.yaml, evidence.json (sha256 pins), semgrep/
├── docs/                → architecture (47 ADRs + template), governance, plans, knowledge, audits/
├── data_platform/       → DATOS (iceberg_warehouse bronze/silver/gold, quality) — no código
├── .github/workflows/   → 11 workflows (ocm-ci, ocm-cd, nightly, codeql, trivy…)
├── Dockerfile · docker-compose{,.override}.yml · run.sh · .pre-commit-config.yaml
```

Entrypoints observados: `run.sh` (`ocm`/`live`/`paper` → `uv run python -m app.cli.*`),
`uv run ocm-api`, Dockerfile CMD → `market_data.main`.

## 5. TARGET ARCHITECTURE CANDIDATE

> **TARGET ARCHITECTURE CANDIDATE** — NO aprobado. No es una arquitectura vigente ni
> autorizada; fue evaluado únicamente como hipótesis de estudio.

```
orangecashmachine/
├── apps/
├── packages/
│   ├── market_data/
│   ├── trading/
│   └── portfolio/
├── tools/
│   ├── architecture/
│   └── governance/
├── tests/
├── docs/
└── deploy/
```

Evaluación observada: `apps/`, `packages/`, `tests/`, `docs/`, `deploy/` son coherentes
con el estado real; `tools/` es una convención propuesta, **nunca existente**; el costo
real de adoptarla está en `scripts/` (12 artefactos con consumidores de gobernanza activa)
y en la resolución pendiente del stub `infrastructure/`.

## 6. BOUNDED CONTEXT FINDINGS

Contextos de negocio encontrados y evidencia que los respalda:

| BC | Ubicación | Evidencia que lo respalda |
|---|---|---|
| **market_data** | `packages/market_data` | Clean/Hexagonal completa (domain/ports/application/adapters/infrastructure). Contratos BC-03/04/05/06/07/08/09/30/37/38/49/56. |
| **trading** | `packages/trading` | Engine (strategies/risk/execution/analytics/bootstrap). Contratos BC-12/36/50. |
| **portfolio** | `packages/portfolio` | Posiciones+rebalance (models/ports/services/infra/bootstrap). Contratos BC-13/43/44. |

- Los BCs **no se importan entre sí** (BC-10/12/13); comunicación por eventos/ports.
- `trading → market_data` solo del composition root (BC-50); `research → market_data` solo
  de su composition root (BC-55).
- No se declaran **nuevos** bounded contexts en este informe; `ocm`, `shared`, `app`,
  `api`, `research` se clasifican como soporte/platform/apps, no como BCs nuevos.

## 7. SCRIPTS/ — CLASIFICACIÓN COMPLETA

| Archivo | Responsabilidad | Tipo | Consumidores | Dependencias | Destino candidato | Confianza |
|---|---|---|---|---|---|---|
| `app_layer_guard.py` | Guard AST R12-R16 Application Layer | ARCHITECTURE_TOOL (guard) | CI app-guard (+sha256), test_app_layer_guard | stdlib (ast, pathlib) | `tools/architecture/` | ALTA |
| `backtest_app_guard.py` | Backtest del guard contra historial | TEST_SUPPORT / ARCHITECTURE_TOOL | CI backtest job | `app_layer_guard` (mismo dir) | `tools/architecture/` (par) | ALTA |
| `domain_subprocess_guard.py` | Guard AST R11 pureza de dominio | ARCHITECTURE_TOOL (guard) | CI domain-guard (+sha256), test_domain_subprocess_guard | stdlib (ast, pathlib) | `tools/architecture/` | ALTA |
| `audit_validator.py` | Validador M1..M20 del sistema de auditoría | GOVERNANCE_TOOL | CI policy-gate, audits manuales, test_audit_validator | `architecture_linter` (config/engine/staged_ports), yaml | `tools/governance/` | ALTA |
| `check_production_gates.py` | Gate binario de producción (ADR-0020/0033) | GOVERNANCE_TOOL / CI_SUPPORT | CI production-gates job | stdlib+subprocess | `tools/governance/` | ALTA |
| `check_ssot_enums.py` | SSOT literales en shared/enums.py | GOVERNANCE_TOOL | pre-commit ssot-enums, CI SSOT | stdlib+re | `tools/governance/` | ALTA |
| `engineering_health_check.py` | Health F2.0 normativo (Plan↔tracking↔ADR↔CI) | GOVERNANCE_TOOL | CI engineering-health + policy-gate, test_engineering_health | yaml+subprocess+re | `tools/governance/` | ALTA |
| `compliance_report.py` | Informe nocturno maintainability (B-54) | REPORTING / GOVERNANCE | nightly-compliance.yml (x2) | stdlib+json+subprocess | `tools/governance/` | MEDIA-ALTA |
| `metrics_report.py` | Salud shared kernel (mypy/pytest/pip-audit) | REPORTING | manual (docs) | json+subprocess+re | `tools/governance/` | MEDIA |
| `migrate_audit_filenames.py` | Migración one-shot de nombres docs/audits | MIGRATION | manual | stdlib+subprocess | `tools/` u obsoleto | BAJA-MEDIA (one-shot) |
| `provision_kafka_topics.py` | Provisionar topics Kafka | INFRASTRUCTURE / DEVELOPER_TOOL | manual; shared/kafka/topics como fuente | **aiokafka + shared.kafka.topics** | `deploy/` o infraestructura | ALTA |
| `reset_kafka_test_topic.sh` | Reset topic de tests | INFRASTRUCTURE / TEST_SUPPORT | manual/integración | bash | `deploy/` | MEDIA |

Nota crítica: 3 artefactos (`app_layer_guard`, `backtest_app_guard`, `domain_subprocess_guard`)
tienen **sha256 pineados en `policies/evidence.json`** verificados por CI `policy-gate-evidence`
(ocm-ci.yml:430-457). Reubicarlos rompería el gate si no se actualiza evidencia primero.

## 8. ARCHITECTURE_LINTER/

- **Responsabilidad**: framework de gobernanza arquitectónica propio (AST, stdlib-only,
  sin imports de OCM); complementa a import-linter y a los guards de `scripts/`.
- **Estructura**: `analyzers/{ast_walk, behavior, mutable_state}`, `engine.py`,
  `models.py`, `config.py`, `cli.py`, `__main__.py`, `reporters/__init__.py`,
  `rules/` (arch_001..010 + base), `staged_ports.py`.
- **Reglas**: ARCH-001 Multiple Position State Owners; 002 Position Semantic Divergence;
  003 Order State Without Reconciliation; 004 Balance State; 005 Market Data Freshness
  Boundary; 006 Orphaned Contract/Port; 007 Duplicate/Homonymous Contracts;
  008 False Capability/Stub; 009 Layer/Dependency Governance; 010 Duplicated Mutable State.
- **Config**: `architecture_linter.toml` — roots `packages, shared, apps, ocm`;
  allowlist por símbolo ARCH-007 (CompositionRoot, RiskConfig, ConfigurationError, CursorStore).
- **Contratos**: `importlinter.toml` (51 contratos BC-NN, SSOT ejecutable de arquitectura)
  — citado por CI (ocm-ci.yml), pre-commit y AGENTS.md.
- **CI**: el job `architecture` (fail-fast) exige `kept >= 49` (baseline F2.1);
  `lint-imports --config architecture_linter/importlinter.toml` es el comando canónico.
- **Consumidores**: `scripts/audit_validator.py` (config/engine/staged_ports),
  tests/architecture_linter/ (6 archivos). Sin imports hacia OCM → reubicación sería
  mecánicamente limpia, pero no autorizada ni propuesta como decisión.
- **Relación con governance**: es tooling transversal de control, no bounded context, no
  infraestructura de runtime.
- **Clasificación arquitectónica**: TRANSVERSAL TOOLING interno (gobernanza), NO dominio.

## 9. APPS/

| App | Rol | Hallazgos |
|---|---|---|
| `app` (CLI) | Entrypoint compositor | `cli/{main,entrypoint,live_hydra,paper_hydra,streaming_hydra,_bootstrap}` + `use_cases` puros. Hydra encapsulado detrás de `ocm.config` (BC-51, única excepción `app.cli.main`). Blindaje auto-defendible ADR-0015 (R12-R16 + sha256). BC-53/54 gobiernan uso de use_cases. |
| `api` (FastAPI) | Gateway experimental | Factory + middleware (rate_limit/logging) + router health + auth/jwt + redis. BC-18 impide dependencia ascendente a api. |
| `research` | Gold read-only | `data/composition_root` + `data_access`; BC-19/20/55 (único módulo autorizado a importar adapters de market_data vía composition root). |

Las 3 son apps legítimas. `app` = app principal (producción), `api` = experimental,
`research` = consumidor analítico post-hoc.

## 10. PACKAGES/

- **market_data (161 .py)**: referencia de Clean Hexagonal. Árbol: `domain`
  (value_objects/events/entities/policies/quality) → `ports` (inbound/outbound, 30+ ports
  outbound) → `application` (use_cases/pipelines/processing/quality/strategies) →
  `adapters` (inbound: rest/websocket/external · outbound: exchange/storage) →
  `infrastructure` (bootstrap/kafka/storage/observability/event_bus/quality/runtime/lineage).
  Migración polars activa: SSOT en `ports/outbound/normalization.py`; frontera única en
  `use_cases/ohlcv_transformer.py`.
- **trading (23 .py)**: engine en desarrollo: strategies (base/ema_crossover/registry),
  risk (manager/models/stop_loss), execution (oms/transport/fill_sync/settlement/paper/live),
  analytics, bootstrap/composition_root. BC-50 limita acoplamiento con market_data al
  composition root.
- **portfolio (15 .py)**: posición/rebalance; DIP cableado (BC-43/44).

## 11. SHARED/

Clasificación: **SHARED KERNEL legítimo** con sub-kernels de dominio cruzado (29 .py).

- `shared/enums.py` → SSOT de literales cross-wire (BC-45/46).
- `shared/kafka/{schemas,provenance,serializer,topics}` → wire schemas SSOT (BC-32/33/35/47).
- `shared/types/{signal,timeframe,order_events,position_events}` → value objects de dominio
  neutro (BC-34/48).
- `shared/contracts/boundaries.py` → protocolos.
- `shared/exceptions/`, `shared/utils/repo.py` → utilidades genéricas.

No se detectó DOMAIN LOGIC ni INFRASTRUCTURE en shared (BC-01 lo garantiza). Consumidores:
18 archivos en packages + 2 en apps + 2 en ocm.

## 12. DEPLOY / INFRASTRUCTURE

- **deploy/systemd**: rendered (`ocm-market-data.service`, `ocm-streaming.service`) +
  templates (render/verify vía `install_systemd.sh`); ADR-0039 runtime canónico.
- **deploy/monitoring**: prometheus.yml, loki, promtail, alertmanager+alerts.yml, grafana
  (dashboard ocm_pipeline.json + provisioning).
- **deploy/scripts .sh**: `deploy_ocm.sh` (verify→deploy→health→accept/rollback→evidencia),
  `health_check.sh` (L1..L4), `install_systemd.sh`.
- **Deploy**: Dockerfile multi-stage + docker-compose (Redis/Kafka/Prometheus). ocm-cd.yml
  es placeholder (workflow_dispatch).
- **infrastructure/ (raíz)**: STUB (solo `__init__.py`). Aparece como root_package de
  import-linter y en contratos BC-07/16/22. Namespace reservado, sin código habitable.
- Discrepancia: CMD Dockerfile → `market_data.main`; resto → `app.cli.*` (ver §16).

## 13. DEPENDENCY FINDINGS

```
    shared  ← {packages(18), apps(2), ocm(2)}
        ↑
  market_data ├─→ trading (BC-50: solo composition_root)
    ports     ├─→ portfolio (BC-13 aislado)
      ↑       └─→ app/use_cases · research (BC-55 composition_root)
    ocm ← apps(3) + packages(15)      [BC-14: ocm no importa BCs de negocio]
 infrastructure (raíz stub) → ocm + market_data  [BC-16 — sin código real]
 CI → architecture_linter + scripts/guards + audit_validator
```

Dependencias que condicionarían una futura reorganización (no ejecutada):
1. `evidence.json` sha256 → app_layer/backtest/domain guards (CI policy-gate-evidence).
2. `importlinter.toml`: root_packages y BC-07/16/22/40/51 referencian rutas
   `apps`, `packages`, `shared`, `infrastructure`, `tests`.
3. CI (ocm-ci 14 jobs) + pre-commit + nightly citan `scripts/*` por ruta.
4. `scripts/audit_validator.py` importa `architecture_linter.*` → moverían juntos o
   actualizando imports.
5. `backtest_app_guard.py` importa `app_layer_guard` (mismo directorio) → par inseparable.
6. `.github/CODEOWNERS`: `/scripts/*_guard.py`.

No se detectaron ciclos, ni domain→infrastructure, ni governance→product, ni
architecture→implementación, ni imports ilegales (contratos vigentes; último gate de CI
limpio).

## 14. ARCHITECTURE CONTRACT FINDINGS

- 51 bloques `[[contracts]]` activos en `architecture_linter/importlinter.toml`
  (baseline F2.1 ≥ 49; eliminaciones documentadas inline: BC-02/17/23/28/31).
- Cobertura por tier: T0 shared; T1 market_data (capas+WS+external+medallion); T2 BCs+shared;
  T3 ocm; T4 infrastructure; T5 api; T6 research; T7 tests; plus app-layers (BC-53/54) y
  ADR-0028/BookBuilder (BC-56).
- Gate CI: fail-fast `kept < 49 = exit 1` (ocm-ci.yml:38-47).
- ARCH-001..010: 10 invariantes AST complementarios; config con allowlist por símbolo
  (ARCH-007).
- **Limitación documentada**: import-linter 2.x no tiene tipo `must_import`; los guards AST
  de `scripts/` cubren los lazy-imports que el motor estático no ve (comentado en BC-53/54).

## 15. GOVERNANCE FINDINGS

Hechos observados (sin resolución de conflictos):

- ADR: 46 decisiones + template; secuencial, sin reutilizar números.
  ADR-0028 "Aceptado con revisión" (2026-09-24); ADR-0033/0043/0045 Propuesto; ADR-0042
  Obsoleto; ADR-0046 Propuesto (materialización H3, commit `05317f14`).
- `registry.yaml` (ADR-0031): **7 reglas con `adr: null`** (R2,R3,R4,R5,R6,R7,R8).
- `tracking.yaml` reglas: H-01..H-12; **H-05, H-10, R7, R8 con backtest=pendiente o
  activada_en_ci=false**.
- `evidence.json`: sha256 de 3 guardas de scripts; CI `policy-gate-evidence` compara hash
  contra árbol (ocm-ci.yml:430-457).
- CI: 11 workflows; ocm-ci fail-fast «architecture → tests+config».
- Estado local (working tree): 7 archivos modificados + 7 untracked preexistentes
  (incl. `scripts/audit_validator.py`, `architecture_linter/importlinter.toml`);
  NO commiteados. La reorg. coexistiría con estos cambios pendientes.

No se declara conflictos; se documentan para decisión humana.

## 16. RELEVANT DISCREPANCIES

1. **Dockerfile vs CLI entrypoint**: CMD → `python -m market_data.main`; run.sh/pyproject →
   `app.cli.*`. Dos fuentes de arranque de producción distintas.
2. **scripts sin consumidor claro**: `metrics_report.py` (solo manual/docs), `compliance_report.py`
   solo nightly; `provision_kafka_topics.py` y `reset_kafka_test_topic.sh` sin invocación en CI.
3. **Posibles duplicidades**: `metrics_report.py` vs `compliance_report.py` (informes de
   salud/calidad con métricas solapadas); contractos BC-37a/37b (par simétrico, no duplicación).
4. **Infraestructura stub**: `infrastructure/` de raíz solo `__init__.py`, pero referenciado
   en 3 contratos y root_packages.
5. **Guards con hash pineado**: 3 guardas en evidence.json; mover/cambiar sin evidencia
   rompe gate.
6. **Reporting sin runtime**: informe nocturno fuera del servicio (nightly workflow).
7. **adr:null**: 7 reglas del registry sin ADR que las ampare.
8. **Entrypoint dual oculto**: `market_data/main.py` como CMD Dockerfile frente a CLI canónico.

## 17. MIGRATION CANDIDATES

> NO ejecutar ninguna migración. Tabla de estudio.

| Actual | Destino candidato | Evidencia | Riesgo | Dependencias | Confianza |
|---|---|---|---|---|---|
| `scripts/{app_layer,backtest_app,domain_subprocess}_guard.py` | `tools/architecture/` | Guards AST puros, sha256 pineados | CRÍTICO: rompe policy-gate-evidence sin actualizar evidence.json | app_layer↔backtest (mismo dir); evidence.json; CI; CODEOWNERS | ALTA (reorg); bloqueada por gobernanza |
| `scripts/audit_validator.py` | `tools/governance/` | Importa `architecture_linter` | ALTO: CI policy-gate + audits manuales | architecture_linter; yaml | ALTA |
| `scripts/{check_production_gates,check_ssot_enums,engineering_health_check,compliance_report,metrics_report}.py` | `tools/governance/` | Consumidores CI/pre-commit/nightly | ALTO: rutas fijas en 3 workflows + pre-commit | CI, pre-commit, pyproject | ALTA-MEDIA |
| `scripts/provision_kafka_topics.py`, `reset_kafka_test_topic.sh` | `deploy/` o infraestructura | Naturaleza INFRASTRUCTURE | MEDIO: consumidor manual | aiokafka; shared.kafka.topics | ALTA |
| `scripts/migrate_audit_filenames.py` | `tools/` u obsoleto | One-shot MIGRATION | BAJO: sin consumidor | stdlib | BAJA-MEDIA |
| `architecture_linter/` | `tools/architecture/linter/` | Sin imports a OCM | ALTO: 5 refs (CI, pre-commit, pyproject, AGENTS, audit_validator) | importlinter.toml; audit_validator | ALTA mecánica |
| `infrastructure/` (stub) | resolver destino (mantener/absorber en `ocm/`) | Stub vacío | ALTO: toca BC-07/16/22 + root_packages | contratos BC | MEDIA |

## 18. OPEN ARCHITECTURAL QUESTIONS

1. ¿`tools/` es convención deseada o solo candidato? (nunca ha existido → ADR para decidir).
2. ¿`infrastructure/` de raíz debe seguir existiendo? ¿a quién pertenece?
3. Los guards (sha256) ¿son "architecture" o "governance"? (evidencia.json los trata como
   una sola familia).
4. ¿El reporting (metrics/compliance) merece destino runtime (cron) o solo nocturno manual?
5. ¿Unificar todos los entrypoints a `app.cli.*` y eliminar `market_data.main`?
6. ¿Crear `tools/` implicaría un "segundo scripting dir" — cómo se evita?

## 19. HUMAN DECISION REQUIRED

Sin resolución aquí (decisión humana):

- [ ] D1: Autorizar (o no) `tools/{architecture,governance}/` como target formal (ADR propio).
- [ ] D2: Destino de los 3 guards (en bloque con `evidence.json` + actualización CI).
- [ ] D3: Resolución del stub `infrastructure/` (mantener vs absorber en `ocm/`).
- [ ] D4: Resolver las 7 reglas `adr: null` del registry y R8/R7 de tracking con backtest
       pendiente.
- [ ] D5: Estabilizar el working-tree (7M + 7??) ANTES de cualquier reorganización.
- [ ] D6: Entrypoint dual Dockerfile — unificar a `app.cli` (o ADR que lo justifique).

## 20. RECOMMENDED NEXT AUDIT

1. Estabilizar working-tree (revisar los 7M + 7??, incl. `scripts/audit_validator.py`,
   `architecture_linter/importlinter.toml`, `book_builder.py`).
2. Auditoría formal de evidencia de gobernanza (M1..M20 + revalidación de R2-R8 con
   `adr: null`) para informar la decisión D4 antes de tocar estructura.
3. Si se autorizara: piloto de reorganización de `scripts/` + `architecture_linter/` en rama
   aislada con CI simulado; jamás en main sin evidencia actualizada.
4. No tocar H3-M2..M6 / H4 / H5 (no autorizados).

---

**FIN DEL INFORME.** Artefacto de evidencia derivada. No constituye aprobación, autoridad
arquitectónica ni decisión formal.