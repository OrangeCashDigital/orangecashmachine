# OCM — PRE-IMPLEMENTATION BASELINE REPORT (2026-09-28)

> **Contexto:** Phase 0 del mandato "OCM — MASTER ARCHITECTURE TRANSFORMATION" (2026-09-28).
> READ-ONLY respecto a código, tests, CI, ADRs y tracking: este reporte **NO modifica nada**.
> Verifica el estado previo exigido por el mandato (§0, §57, §65) antes de Phase A (ADR-0048).

## 1. Alcance y cadena de gobernanza

- Decisión humana: **D5** (2026-09-28) = DECIDED — HUMAN (D5-01=D, D5-02=A, D5-03=B, D5-04=D).
- ADR-0047 = registro normativo de D5; **HR PASS — RESULTADO B** (2026-09-28), aceptado sin ediciones (NB-1/NB-2 no bloqueantes).
- Decision Pack `docs/proposals/EXECUTION-CONTRACT-DECISION-PACK-2026-09-28.md` = PROPUESTA (no autoridad); 12 campos + matrices + fases A-L.
- Mandato 2026-09-28 = autorización humana para resolver Q1-Q8, ADR-0048, Execution Registry, arquitectura Market Data y fases A-L con gates.
- Orden: Phase 0 (baseline) → Phase A (ADR-0048) → Human Gate → Phase B (registry) → …

## 2. Baseline git

- Branch: `feat/m22b-governance`. HEAD: `05317f149c57afa05fa5f667b53272e6ace7ba49` (idéntico al checkout del mandato y al HEAD post-materialización D5).
- Working tree: **8 modificados** + **10 untracked**. Sin staged. **No se modifica nada en Phase 0.**

### 2.1 Modificados (8)
| Archivo | Observación |
|---|---|
| `architecture_linter/importlinter.toml` | preexistente (baseline linter) |
| `deploy/scripts/install_systemd.sh` | preexistente |
| `deploy/systemd/templates/ocm-market-data.service.template` | preexistente (diff = tuning resiliencia; ExecStart intacto) |
| `deploy/systemd/templates/ocm-streaming.service.template` | preexistente (diff = tuning resiliencia; ExecStart intacto) |
| `docs/plans/tracking.yaml` | **+97 líneas = aceptación D5** (bloque `d5_entrypoints_execution_model`) |
| `packages/market_data/application/processing/book_builder.py` | preexistente |
| `scripts/audit_validator.py` | preexistente |
| `tests/architecture_linter/test_golden.py` | preexistente |

### 2.2 Untracked (10)
| Archivo | Observación |
|---|---|
| `.vscode/` | preexistente |
| `VERIFICATION_FINAL_REPORT.md` | preexistente |
| `docs/architecture/decisions/ADR-0047-d5-entrypoints-execution-model.md` | fase D5 (aceptado) |
| `docs/audit/` | dir con auditorías previas + **este reporte** |
| `docs/audits/ARCHITECTURE-DISCOVERY-2026-09-27.md` | preexistente |
| `docs/audits/AUDIT_OCM_STAGED_PORTS_GOVERNANCE_2026-09-24.md` | preexistente |
| `docs/proposals/EXECUTION-CONTRACT-DECISION-PACK-2026-09-28.md` | fase D5 (PROPUESTA) |
| `packages/market_data/ports/outbound/market_data_view.py` | staged port (ADR-0044, MarketDataViewPort F1, expires 2026-12-31) |
| `tests/architecture/test_m26_staged_governance.py` | preexistente |
| `tests/market_data/application/processing/test_market_data_view.py` | preexistente |

## 3. Estado verificado del bloque D5

- `tracking.yaml:d5_entrypoints_execution_model` → `estado: DECIDIDA`; `estado_materializacion: HUMAN ACCEPTANCE GATE APROBADO (RESULTADO B — 2026-09-28)`.
- Cadena completa: conocimiento HECHO → decisión humana HECHO → adr HECHO (ADR-0047 aceptado, inalterado) → gobernanza HECHO → implementación NO_APLICA → tests NO_APLICA → ci NO_APLICA → evidencia HECHO → cierre HECHO.
- `registry.yaml` SIN CAMBIO (no existe estructura para decisiones/entrypoints; añadir ADR-0048 a `related_adrs` generaría WARN M25).
- ADR-0048 **no existe** todavía (`ls docs/architecture/decisions/ADR-0048*` → vacío).

## 4. Inventario de entrypoints y consumers (verificado)

| Artefacto | Entrada | Estado |
|---|---|---|
| `pyproject.toml` `[project.scripts]` (176-181) | `ocm`, `ocm-api`, `live`, `paper`, `streaming` | registrados |
| `market_data.main` | service entrypoint (`:8001`, FastAPI, ingestion loop) | **NO** en `[project.scripts]` ni `run.sh` |
| `run.sh` (4-5 cabecera, 31-41 casos) | `ocm l live l paper`; cabecera declara "SSOT" | **DISP-D5-01** (falta `ocm-api`, `streaming`) |
| `Dockerfile:40` | `CMD ["python","-m","market_data.main"]` | SERVICE ✔ |
| `docker-compose.yml:359` | `python -m market_data.main` | SERVICE ✔ |
| `docker-compose.yml:407,451` | `python -m trading.main`, `python -m portfolio.main` | **DISP-D5-02** (módulos inexistentes) |
| `deploy/systemd/templates/ocm-market-data.service.template:16` | `python -m market_data.main` | ✔ |
| `deploy/systemd/templates/ocm-streaming.service.template:16` | `python -m app.cli.streaming_hydra` | ✔ |
| CI `ocm-ci.yml` config-validation | `OCM_VALIDATE_ONLY=true uv run python -m app.cli.main` | ✔ |
| `scripts/backtest_app_guard.py` | gate CI sobre apps | NO es modo de ejecución (Backtest NOT IMPLEMENTED) |

Re-verificado en Phase 0: diffs preexistentes de los 2 templates systemd son **tuning/resiliencia** (EnvironmentFile `-`, RestartSec 5s, Nice, CPUAffinity, StartLimitBurst); `ExecStart` sin cambios.

## 5. Arquitectura actual de `packages/market_data` (inventario Phase 0)

- 204 archivos Python, ~33 865 líneas: `packages/market_data/main.py` (635), domain (4 949), application (8 351), ports (4 005), adapters (7 752), infrastructure (8 173).
- **domain/** (framework-agnostic): constants, entities, events (`_base`, `_lineage`, `ingestion`, `trade_events`, `orderbook_events`, `replay_events`, `gap_events`, `external_events`), exceptions (30), policies (`base`, `data_quality_policy`), quality (`invariants`, `types`), value_objects (`symbol`, `candle`, `raw_trade`, `normalized_trade`, `trade_series`, `order_book`, `ohlcv_chunk`, `gap_range`, `gap_utils`, `timeframe`, `quality_label`, `exchange_quirks`, `candle_validator`).
- **ports/**: inbound (`market_data_source`, `pipeline_factory`, `pipeline_trigger`, `trades_source`, `external/{errors,polling,replay}`), outbound (`normalization` SSOT, `storage_factory`, `fetcher`, `exchange_client`, `event_bus`, `state`, `publisher`, `kafka_consumer/producer`, `quality`, `storage`, `book_builder`, **untracked `market_data_view`** staged P1, `historical_fetcher`, `feature_reader`, `gap_registry`, `lineage`, `observability`, `throttle`, `chunk_converter`, `data_quality_checker`, `transformer`, `anomaly_registry`, `exchange`, `gap_event_publisher`, `external_publisher`, `metrics`, `publisher_port`, `quality_pipeline`, `kafka_*`).
- **adapters/**: inbound (`kucoin_feed_adapter`, `bybit_feed_adapter`), outbound (`ccxt_adapter`, `throttle`).
- **infrastructure/**: bootstrap, event_bus, kafka, lineage, observability, quality, runtime, storage; storage/iceberg (`schemas.py`, `partitions.py`, `bootstrap.py` ensure_bronze/silver, `catalog.py`, `gold/gold_storage.py` → `gold.features`/`l.features` + `l/transformer`), fechas/snapshots.
- **polars-first**: domain SSOT polars; `application/use_cases/ohlcv_transformer.py` = única frontera `pl.from_pandas`/`.to_pandas`.
- **Staged port**: `market_data_view.py` = MarketDataViewPort (ADR-0044/ADR-0028 §3.6, F1, consumer B-MD-004, expires 2026-12-31); enforcement M26/ARCH-006 pendiente de autorización.

## 6. Inventario de datasets (mandato §34 — clasificación previa)

| Dataset | Flag datasets.yaml | Evidencia en repo | Clasificación previa |
|---|---|---|---|
| OHLCV | `ohlcv: true` | bronze.ohlcv / silver.ohlcv / gold.features (Iceberg), API `/ohlcv/{exch}/{sym}/{tf}`, pipeline resample | **EXISTS** |
| Trades | `trades: false` | schema `shared/kafka/schemas/trades.py`, `trades_source`, `trade_series`, feed adapters; pipeline Trades ingestion | **PARTIAL** (schema+ingestion, flag off, sin dataset canónico gold) |
| OrderBook | `orderbook: false` | schema `orderbook.py`, `book_builder.py` (M), `market_data_view` staged | **PARTIAL** (book_builder P1, sin consumo F2) |
| Funding Rate | `funding_rate: false` | schema `funding.py` | **NOT_IMPLEMENTED** (schema wire, sin pipeline) |
| Open Interest | `open_interest: false` | schema `oi.py` | **NOT_IMPLEMENTED** |
| Liquidations | `liquidations: false` | schema `liquidations.py` | **NOT_IMPLEMENTED** |
| Mark Price | `mark_price: false` | — (sin schema propio en kafka/schemas) | **NOT_IMPLEMENTED** |
| Index Price | `index_price: false` | — (sin schema propio) | **NOT_IMPLEMENTED** |
| Instrument Reference | — | catalog/symbols en adapters+domain | **PARTIAL** (no dataset canónico publicado) |
| Historical Dataset | — | `historical_fetcher` port, replay events | **PARTIAL** |

> Clasificación **previa** de Phase 0 (no normativa). La Current→Target matrix definitiva (§35 mandato) se construirá en Phase E con autoridad post ADR-0048.

## 7. Hallazgos preexistentes preservados (mandato §50 — NO autofix)

- `audit_validator`: M21 FAIL (naming `ARCHITECTURE-DISCOVERY-2026-09-27.md` canónico), WARN M25 (ADR-0020/ADR-0044), WARN M20.
- `engineering_health_check`: 2 errores (B-25 EN_PROGRESO, etc.).
- `architecture_linter`: 3 PASS / 7 FAIL (ARCH-006 consume staged ports; M26 enforcement pendiente).
- semgrep + bandit: hallazgos registrados previamente. Ninguno se corrige en Phase 0/Phase A (registrar, preservar, no autofix).

## 8. Validaciones READ-ONLY Phase 0

- `config/config.yaml` compone vía `defaults` explícito (Hydra no autoloads `*.yaml` root arbitrarios): **Q1 del mandato (registry en `config/execution_registry.yaml`) NO rompe la composición Hydra** ni el glob. Sin otro glob de `config/*.yaml` en el repo.
- `registry.yaml` usa `related_adrs`: ADR-0048 **NO se añade a `related_adrs`** para no crear el WARN M25 nuevo (se registra solo en `tracking.yaml`; ver Phase A).
- `CONFIG_PATH = "config/<config.yaml>"` (schema.py) — el registry no debe colisionar con nombres de grupo Hydra; `execution_registry.yaml` no es nombre de grupo.

## 9. No contradicciones con el mandato (Phase 0 → permite iniciar Phase A)

1. **Q1** (`config/execution_registry.yaml` dedicado): viable; sin glob/autoload Hydra; separado de `policies/registry.yaml`. ✔
2. **Q2** (`market_data.main` PUBLIC SERVICE; PUBLIC ≠ console script): hecho verificado (no está en `[project.scripts]`; invocado vía `-m` en Docker/Compose/systemd). ✔
3. **Q3** (run.sh = convenience; eliminar claim SSOT): DISP-D5-01 confirmada. ✔
4. **Q4** (refs `trading.main`/`portfolio.main` = DEAD; NO crear módulos): DISP-D5-02 confirmada; `trading/`/`portfolio/` sin `main.py`. ✔
5. **Q5-Q8** (fases A-L; status enum; registry authority): alinean con D5-04 + ADR-0047 §14 + Decision Pack. ✔

No se detecta contradicción → **Phase A (ADR-0048) queda autorizada por el mandato** (Gate A al cierre de la materialización).

## 10. Reporte final comprometido (mandato §58)

`docs/audit/OCM-TARGET-ARCHITECTURE-EXECUTION-MARKET-DATA-REPORT.md` — 26 secciones; se materializará en Phase L/cierre, NO ahora.