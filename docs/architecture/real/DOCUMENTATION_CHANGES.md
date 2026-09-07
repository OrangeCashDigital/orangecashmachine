# DOCUMENTATION CHANGES — ACTUALIZACIONES REQUERIDAS

> **Fecha**: 2026-09-04
> **Principio**: Actualizar existentes, NO crear duplicados. Jerarquía: ARCHITECTURE → ADRs → PLANS → RUNBOOKS → KB → IMPLEMENTATION

---

## ARCHITECTURE (docs/architecture/)

| Documento | Cambio | Tipo |
|-----------|--------|------|
| `0002-event-driven-kappa-architecture.md` | Añadir Orderbook flow a diagrama Kappa | UPDATE |
| `feed-model.md` | Clarificar: Orderbook ES un feed (L2_BOOK), no está en external_ingestion | UPDATE |
| `GOVERNANCE.md` | Referenciar B-49 gates como release criteria | UPDATE |
| `real/ARCHITECTURE_REAL.md` | **NUEVO** — Documenta estado real actual | CREATE |
| `real/ARCHITECTURE_TARGET.md` | **NUEVO** — Arquitectura objetivo | CREATE |
| `real/GAP_ANALYSIS.md` | **NUEVO** — Gap analysis detallado | CREATE |
| `real/MARKET_UNIVERSE_ANALYSIS.md` | **NUEVO** — Market Universe deep dive | CREATE |
| `real/ORDERBOOK_FLOW_ANALYSIS.md` | **NUEVO** — Orderbook flow + gaps | CREATE |
| `real/B49_STATUS.md` | **NUEVO** — G4-G9 status con evidencia | CREATE |
| `real/PR25_ANALYSIS.md` | **NUEVO** — PR #25 scope | CREATE |
| `real/UNIFIED_ADAPTER_ANALYSIS.md` | **NUEVO** — Unified adapter viability | CREATE |

---

## ADRs (docs/architecture/decisions/)

| ADR | Cambio | Tipo |
|-----|--------|------|
| `ADR-0013` | Añadir nota: Orderbook feed incluido en realtime_feeds | UPDATE |
| `ADR-0014` | Añadir: Orderbook Builder capability en realtime_feeds | UPDATE |
| `ADR-0017` | Marcar: Bybit Discovery Profile = PRIORITY 1 post-B-49 | UPDATE |
| `ADR-0020` | Referenciar `check_production_gates.py` como gate binario | UPDATE |
| `ADR-0022` | Actualizar: streaming service ACTIVE, health endpoint faltante | UPDATE |
| `ADR-0028` | **NUEVO** — Orderbook Builder design (draft) | CREATE |
| `ADR-XXXX` | **NUEVO** — Market Universe SSOT (draft) | CREATE |

---

## PLANS (docs/plans/)

| Documento | Cambio | Tipo |
|-----------|--------|------|
| `tracking.yaml` | Actualizar B-49: `estado: PARTIAL`, añadir gaps G9, G5 | UPDATE |
| `tracking.yaml` | Añadir items Orderbook Bronze (consumer, schema, tabla, builder) | UPDATE |
| `tracking.yaml` | Añadir items Market Universe (SSOT, normalizer, auto-discovery) | UPDATE |
| `backlog-priorizado-2026-08-08.md` | Re-priorizar: Orderbook Bronze → P0, Market Universe → P0 | UPDATE |
| `PLAN-Maestro-Ingenieria.md` | **NUEVO** — Master Correction Plan (ver MASTER_CORRECTION_PLAN.md) | CREATE |

---

## RUNBOOKS (docs/runbooks/ — crear directorio si no existe)

| Runbook | Cambio | Tipo |
|---------|--------|------|
| `market_data_service.md` | **NUEVO** — Operación ocm-market-data (start/stop/logs/health) | CREATE |
| `streaming_service.md` | **NUEVO** — Operación ocm-streaming (config, restart, troubleshooting) | CREATE |
| `orderbook_bronze.md` | **NUEVO** — Orderbook → Bronze pipeline (schema, consumer, recovery) | CREATE |
| `market_universe.md` | **NUEVO** — Gestión Market Universe (add/remove symbols, formats) | CREATE |
| `production_gates.md` | **NUEVO** — `check_production_gates.py` usage, G4-G9 troubleshooting | CREATE |

---

## KB / TRAINING (docs/knowledge/)

| Contenido | Cambio | Tipo |
|-----------|--------|------|
| `manifest.yaml` | Añadir entradas: Orderbook, Market Universe, B-49, Production Gates | UPDATE |
| `notes/` | **NUEVOS** — Ver KB_TRAINING_CHANGES.md | CREATE |

---

## IMPLEMENTATION (código con docstrings)

| Archivo | Cambio | Tipo |
|---------|--------|------|
| `packages/market_data/main.py` | Docstring: aclarar que orderbook NO va a Bronze desde aquí | UPDATE |
| `apps/app/cli/streaming_hydra.py` | Docstring: añadir health endpoint TODO | UPDATE |
| `packages/market_data/infrastructure/kafka/consumer.py` | Añadir `for_orderbook()` factory (TODO comment) | UPDATE |
| `packages/market_data/infrastructure/storage/iceberg/schemas.py` | Añadir ORDERBOOK schemas (TODO comments) | UPDATE |
| `packages/market_data/infrastructure/storage/bronze/bronze_storage.py` | Añadir `append_snapshot/delta` stubs (TODO) | UPDATE |
| `ocm/config/schema.py` | Añadir `MarketUniverseConfig` (TODO) | UPDATE |
| `config/market_data/feeds.yaml` | Comentario: DEPRECATED → usar universe.yaml | UPDATE |
| `config/env/development.yaml` | Comentario: symbols DEPRECATED → usar universe.yaml | UPDATE |

---

## RESUMEN DE ARCHIVOS NUEVOS REQUERIDOS

```
docs/
├── architecture/
│   ├── real/
│   │   ├── ARCHITECTURE_REAL.md
│   │   ├── ARCHITECTURE_TARGET.md
│   │   ├── GAP_ANALYSIS.md
│   │   ├── MARKET_UNIVERSE_ANALYSIS.md
│   │   ├── ORDERBOOK_FLOW_ANALYSIS.md
│   │   ├── B49_STATUS.md
│   │   ├── PR25_ANALYSIS.md
│   │   └── UNIFIED_ADAPTER_ANALYSIS.md
│   └── decisions/
│       ├── ADR-0028-orderbook-builder.md (draft)
│       └── ADR-XXXX-market-universe-ssot.md (draft)
├── plans/
│   └── MASTER_CORRECTION_PLAN.md
├── runbooks/
│   ├── market_data_service.md
│   ├── streaming_service.md
│   ├── orderbook_bronze.md
│   ├── market_universe.md
│   └── production_gates.md
└── knowledge/
    ├── manifest.yaml (updated)
    └── notes/
        ├── orderbook_concepts.md
        ├── market_universe.md
        ├── production_gates.md
        └── b49_status.md
```

---

## VERIFICACIÓN DE CONSISTENCIA

Antes de commit, verificar:
- [ ] No hay contradicciones entre ARCHITECTURE_REAL y ARCHITECTURE_TARGET
- [ ] ADRs referencian documentos real/ correctamente
- [ ] tracking.yaml estados consistentes con GAP_ANALYSIS
- [ ] Runbooks accionables sin ambigüedad
- [ ] KB entries explican conceptos desde cero
- [ ] Code docstrings apuntan a runbooks/ADRs, no duplican info