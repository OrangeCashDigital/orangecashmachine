# PR DECOMPOSITION — PROPUESTA DE PRs PEQUEÑOS Y AUDITABLES

> **Fecha**: 2026-09-04
> **Principio**: Un PR = un cambio lógico atómico. Dependencias explícitas. Tests + Gates + Rollback.

---

## PR A — Documentation & Architecture Baseline
**Objetivo**: Consolidar toda la documentación READ-ONLY generada en esta auditoría.

| Atributo | Valor |
|----------|-------|
| **Archivos** | `docs/architecture/real/*.md` (8 archivos), `docs/architecture/real/DOCUMENTATION_CHANGES.md`, `docs/architecture/real/KB_TRAINING_CHANGES.md` |
| **Dependencias** | Ninguna (Phase 0 completa) |
| **Tests** | Ninguno (docs-only) |
| **Gates** | `ruff`, `ruff format`, `lint-imports` (docs no afectan) |
| **Criterios aceptación** | - Documentación coherente, sin contradicciones<br>- `docs/architecture/real/` completo<br>- `tracking.yaml` B-49 = PARTIAL actualizado |
| **Riesgo** | BAJO — solo documentación |
| **Rollback** | `git revert` del commit de docs |

---

## PR B — Market Universe SSOT
**Objetivo**: Unificar Market Universe en `config/market_data/universe.yaml` + `MarketUniverseProvider`.

| Atributo | Valor |
|----------|-------|
| **Archivos** | `config/market_data/universe.yaml` (NUEVO), `ocm/config/schema.py` (+MarketUniverseConfig), `packages/market_data/application/market_universe.py` (NUEVO), `packages/market_data/infrastructure/bootstrap/pipeline_factory.py` (migración), `packages/market_data/application/feed_orchestrator.py` (migración), `apps/app/cli/streaming_hydra.py` (migración), `shared/utils/symbol_normalizer.py` (NUEVO) |
| **Dependencias** | PR A (docs), ADR-XXXX aprobado |
| **Tests** | - `tests/market_data/test_market_universe.py`: universe consistency, normalizer, migration<br>- `tests/market_data/test_symbol_normalizer.py`: CCXT↔Cryptofeed roundtrip |
| **Gates** | `ruff`, `ruff format`, `lint-imports`, `mypy`, `pytest -x -q` |
| **Criterios aceptación** | - `universe.yaml` SSOT único<br>- REST y WS consumen mismo universe (normalizado)<br>- Formatos: canonical `BTC/USDT` → CCXT `BTC/USDT`, Cryptofeed `BTC-USDT-PERP`<br>- `auto_discover_symbols` path implementado (aunque `false` por defecto)<br>- Tests pasan |
| **Riesgo** | MEDIO — Cambia configs consumidas por 3 entrypoints |
| **Rollback** | Revertir configs + código; `universe.yaml` se ignora si no existe |

---

## PR C — Streaming / Orderbook Ingestion Fixes
**Objetivo**: Estabilizar streaming service, añadir health endpoint, fix configs.

| Atributo | Valor |
|----------|-------|
| **Archivos** | `apps/app/cli/streaming_hydra.py` (+health endpoint), `config/market_data/feeds.yaml` (ajustes), `config/exchanges/bybit.yaml` (verify enabled), `deploy/systemd/rendered/ocm-streaming.service` (verify), `scripts/health_check.sh` (+streaming domain) |
| **Dependencias** | PR A, PR B (universe config para symbols) |
| **Tests** | - `tests/app/test_streaming_hydra.py`: health endpoint, config loading<br>- `tests/scripts/test_health_check.py`: 4 domains HEALTHY |
| **Gates** | `ruff`, `ruff format`, `lint-imports`, `mypy`, `pytest -x -q`, `health_check.sh` PASS |
| **Criterios aceptación** | - `ocm-streaming` ACTIVE con restart test PASS (B-59)<br>- `health_check.sh` → 4/4 domains HEALTHY<br>- `ingestion_mode: websocket` funciona con universe symbols<br>- Streaming health endpoint responde 200 |
| **Riesgo** | BAJO — Solo estabilización, sin cambios de lógica core |
| **Rollback** | Revertir configs + health endpoint |

---

## PR D — Orderbook → Bronze (CORE B-49 BLOCKER)
**Objetivo**: Orderbook consumer + schema + tabla Iceberg + Bronze writer.

| Atributo | Valor |
|----------|-------|
| **Archivos** | `packages/market_data/infrastructure/storage/iceberg/schemas.py` (+ORDERBOOK schemas), `packages/market_data/infrastructure/storage/bronze/bronze_storage.py` (+append_snapshot/delta), `packages/market_data/infrastructure/kafka/consumer.py` (+for_orderbook), `packages/market_data/infrastructure/kafka/orderbook_bronze_writer.py` (NUEVO), `packages/market_data/main.py` (wiring), `apps/app/cli/streaming_hydra.py` (wiring), `packages/market_data/infrastructure/bootstrap/composition_root.py` (wiring) |
| **Dependencias** | PR A, PR B (universe), ADR-0028 aprobado |
| **Tests** | - `tests/market_data/infrastructure/kafka/test_orderbook_bronze_writer.py`: consumer, dedup, write, DLQ<br>- `tests/market_data/infrastructure/storage/bronze/test_orderbook_bronze.py`: schema, append, query<br>- `tests/market_data/test_orderbook_bronze_integration.py`: end-to-end WS → Kafka → Bronze |
| **Gates** | `ruff`, `ruff format`, `lint-imports`, `mypy`, `pytest -x -q`, `check_production_gates.py --gate G9` PASS |
| **Criterios aceptación** | - `orderbook.raw` consumido por `OrderbookBronzeWriter`<br>- `ORDERBOOK_SNAPSHOT_SCHEMA` + `ORDERBOOK_DELTA_SCHEMA` en schemas.py<br>- Tablas `bronze.orderbook_snapshot` + `bronze.orderbook_delta` creadas<br>- Datos verificables en Bronze (<5 min freshness, `run_id` presente)<br>- `check_production_gates.py` G9 PASS |
| **Riesgo** | ALTO — Nueva tabla Iceberg, nuevo consumer, wiring en 2 entrypoints |
| **Rollback** | - Drop tablas Iceberg (manual)<br>- Revertir wiring en main.py + streaming_hydra.py<br>- Consumer no se registra si falla start |

---

## PR E — Silver / Order Flow (Orderbook)
**Objetivo**: Orderbook data en Silver/Gold con features OFI, CVD, microprice.

| Atributo | Valor |
|----------|-------|
| **Archivos** | `packages/market_data/infrastructure/storage/iceberg/schemas.py` (+SILVER_ORDERBOOK, GOLD_ORDERBOOK), `packages/market_data/infrastructure/storage/silver/orderbook_storage.py` (NUEVO), `packages/market_data/infrastructure/storage/gold/orderbook_transformer.py` (NUEVO), `packages/market_data/application/feature_engineering/orderbook_features.py` (NUEVO), consumers Silver/Gold para orderbook |
| **Dependencias** | PR D (Bronze OK), Phase 7 |
| **Tests** | - Silver: dedup, validated, query<br>- Gold: OFI, CVD, microprice, spread calculations<br>- Integration: Bronze → Silver → Gold pipeline |
| **Gates** | `ruff`, `ruff format`, `lint-imports`, `mypy`, `pytest -x -q` |
| **Criterios aceptación** | - Orderbook data en Silver (dedup, validated)<br>- Orderbook features en Gold (OFI, CVD, microprice, spread)<br>- `FeatureReaderPort` query orderbook features |
| **Riesgo** | MEDIO — Nueva capa de transformación |
| **Rollback** | Drop tablas Silver/Gold orderbook, revertir consumers |

---

## PR F — Production Validation & B-49 Gate
**Objetivo**: `check_production_gates.py` binario + CI integration + B-49 PASS.

| Atributo | Valor |
|----------|-------|
| **Archivos** | `scripts/check_production_gates.py` (NUEVO), `.github/workflows/ocm-ci.yml` (+job production-gate), `apps/app/cli/streaming_hydra.py` (health endpoint si PR C no lo hizo), `scripts/health_check.sh` (verify 4 domains), `deploy/systemd/` (restart test doc), `tracking.yaml` (B-49 = HECHO) |
| **Dependencias** | PR A-E (G1-G9 PASS) |
| **Tests** | - `tests/scripts/test_check_production_gates.py`: pos/neg cases, mock infra<br>- CI: job `production-gate` PASS en PR F |
| **Gates** | `ruff`, `ruff format`, `lint-imports`, `mypy`, `pytest -x -q`, `check_production_gates.py` → exit 0 |
| **Criterios aceptación** | - `check_production_gates.py` exit 0 (G1-G9 PASS)<br>- CI job `production-gate` PASS<br>- `tracking.yaml` B-49: `estado: HECHO`, `fecha_cierre` actual<br>- Evidence package documentado |
| **Riesgo** | BAJO — Solo validación, sin lógica de negocio |
| **Rollback** | Revert script + CI job + tracking.yaml |

---

## PR G — Protocol Discovery Profile Bybit (Post-B-49)
**Objetivo**: Bybit Discovery Profile completo (ADR-0017).

| Atributo | Valor |
|----------|-------|
| **Archivos** | `packages/market_data/adapters/outbound/exchange/ccxt_adapter.py` (docs), `cryptofeed_orderbook_stream.py` (gap recovery docs), funding/oi/liquidations runners, `tests/kafka/test_schema_provenance.py` (extendido), fixtures Bybit reales |
| **Dependencias** | PR D-E (Bronze/Silver OK), ADR-0017 |
| **Tests** | - Promotion Rule gate: ASSUMED contracts → BLOCK<br>- Fixtures: mensajes reales Bybit congelados |
| **Gates** | `ruff`, `ruff format`, `lint-imports`, `mypy`, `pytest -x -q` |
| **Criterios aceptación** | - Bybit Profile: REST (OHLCV, Trades, Metadata) + WS (Orderbook, Trades, Funding, OI, Liq) documentados<br>- Contract Provenance registrado para todos los schemas Bybit<br>- Promotion Rule gate en CI pasa |
| **Riesgo** | MEDIO — Documentación + tests, sin lógica core |
| **Rollback** | Revertir docs + tests |

---

## PR H — Unified Adapter (Post-B-49, LATER)
**Objetivo**: Adapter unificado REST+WS por exchange (si ADR aprobado).

| Atributo | Valor |
|----------|-------|
| **Archivos** | ADR nuevo, `ports/outbound/unified_exchange.py` (NUEVO protocol), `adapters/outbound/exchange/bybit_unified.py` (NUEVO), `architecture/importlinter.toml` (exceptions BC-07/BC-08), `pipeline_factory.py`, `composition_root.py`, `feed_orchestrator.py`, `streaming_hydra.py` (migración), tests paridad |
| **Dependencias** | PR B, PR D, PR E, ADR aprobado, BC-NN actualizados |
| **Tests** | - Paridad REST vs WS (dual mode)<br>- Migration tests: old adapters → unified<br>- Integration: unified adapter → pipelines |
| **Gates** | `ruff`, `ruff format`, `lint-imports`, `mypy`, `pytest -x -q` |
| **Criterios aceptación** | - ADR aprobado<br>- BC-NN contracts actualizados y CI verde<br>- Unified adapter reemplaza CCXTAdapter + CryptofeedRunner para Bybit<br>- Parity validation en dual mode<br>- Tests pasan |
| **Riesgo** | ALTO — Major refactor, BC-NN changes |
| **Rollback** | Revertir todo, mantener adapters legacy |

---

## DEPENDENCIAS ENTRE PRs

```
PR A (Docs)
    │
    ├── PR B (Market Universe) ← ADR-XXXX
    │       │
    │       ├── PR C (Streaming Fixes) ← PR B
    │       │
    │       ├── PR D (Orderbook Bronze) ← PR B, ADR-0028
    │       │       │
    │       │       ├── PR E (Silver/Gold Orderbook) ← PR D
    │       │       │
    │       │       └── PR F (Production Gate) ← PR D, PR E
    │       │
    │       └── PR G (Protocol Discovery) ← PR D, PR E
    │
    └── PR H (Unified Adapter) ← PR B, PR D, PR E, ADR, BC-NN
```

**Orden de ejecución obligatorio**: A → B → C → D → E → F → (G, H en paralelo post-B-49)

---

## RESUMEN DE PRs PARA B-49

| PR | Bloquea B-49 | Prioridad | Esfuerzo |
|----|--------------|-----------|----------|
| A | No (docs) | P0 | 1 día |
| B | Sí (G5, G9 format mismatch) | P0 | 3-5 días |
| C | Parcial (G4, G7 streaming) | P0 | 1-2 días |
| **D** | **SÍ (G9 Orderbook Bronze)** | **P0 - CRÍTICO** | **10-14 días** |
| E | Post-B-49 (calidad) | P1 | 5-7 días |
| **F** | **SÍ (B-49 gate binario)** | **P0 - CRÍTICO** | **3-5 días** |
| G | Post-B-49 | P2 | 5-7 días |
| H | Post-B-49 (LATER) | P3 | 10-14 días |

**Mínimo para B-49 PASS**: PR A + B + C + D + F