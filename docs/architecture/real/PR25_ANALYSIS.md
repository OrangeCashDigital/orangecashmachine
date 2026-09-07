# PR #25 — ANÁLISIS Y RELACIÓN CON B-49

> **Fecha**: 2026-09-04
> **Nota**: Este análisis se basa en el estado actual del repositorio y tracking.yaml. 
> Si PR #25 no existe como pull request real, este documento sirve como especificación 
> de qué cambios deberían agruparse en el PR que resuelva B-49.

---

## OBJETIVO ORIGINAL DE PR #25 (INFERIDO)

Basado en tracking.yaml B-49 y ADR-0020, PR #25 debería implementar:
> **`scripts/check_production_gates.py` — veredicto binario PASS/BLOCK sobre G1..G11 con evidencia por cheque**

---

## CAMBIAS REALIZADOS (EN REPOSITORIO ACTUAL)

### ✅ Completados (Pre-PR #25 o paralelos)
| Item | Estado | Referencia |
|------|--------|------------|
| `health_check.sh` | ✅ HECHO | 3 domains HEALTHY |
| `KafkaBronzeWriter` durable dedup | ✅ HECHO (B-19) | mark_after_write + L2 Redis |
| `OrderBookKafkaProducer` + `CryptofeedOrderBookStream` | ✅ HECHO (F2.6b) | Producer orderbook.raw |
| `streaming_hydra.py` entrypoint | ✅ HECHO (F2.6b) | Canary ORDERBOOK |
| Systemd units `ocm-market-data` + `ocm-streaming` | ✅ HECHO | Ambos ACTIVE |
| `CompositionRoot.build_ws_producers()` | ✅ HECHO | WSProducerBundle |
| `BronceStorage.append()` con `run_id` | ✅ HECHO | Schema 12 fields |

### ⚠️ Parciales (Requieren PR #25 o posteriores)
| Item | Estado | Falta |
|------|--------|-------|
| `check_production_gates.py` | ❌ MISSING | Script completo G1-G11 |
| Orderbook → Bronze | ❌ MISSING | Consumer, Schema, Tabla |
| Market Universe unificado | ❌ MISSING | SSOT único |
| Streaming health endpoint | ❌ MISSING | health_check.sh coverage |

---

## CAMBIOS PENDIENTES PARA PR #25 (CORE B-49)

### Must Have para PR #25 (B-49 Blocker)
```python
# scripts/check_production_gates.py (NUEVO)
"""
Veredicto binario PASS/BLOCK sobre G1..G11 con evidencia.
Exit codes: 0=PASS, 1=BLOCK, 2=ERROR
"""
def check_g4_systemd(): ...
def check_g5_kafka(): ...
def check_g6_infra(): ...
def check_g7_health(): ...
def check_g8_is_stub(): ...
def check_g9_bronze(): ...  # DEBE incluir orderbook
def check_g10_position(): ...
def check_g11_tracing(): ...

if __name__ == "__main__":
    results = run_all_gates()
    print(json.dumps(results, indent=2))
    sys.exit(0 if all_pass else 1)
```

### Should Have para PR #25 (Calidad)
- OrderbookBronzeWriter consumer básico (sin gap recovery aún)
- ORDERBOOK_SCHEMA en schemas.py
- Tablas bronze.orderbook_snapshot/delta básicas

### Nice to Have (Post-PR #25)
- Gap detection en Orderbook
- Market Universe SSOT
- Unified health endpoint

---

## QUÉ PROBLEMAS RESUELVE PR #25

| Problema | Resuelto por PR #25? | Notas |
|----------|---------------------|-------|
| Sin veredicto binario G1-G11 | ✅ SÍ | `check_production_gates.py` |
| B-49 no tiene gate ejecutable | ✅ SÍ | Script + CI integration |
| Orderbook no en Bronze | ⚠️ PARCIAL | Solo si incluye consumer+schema básicos |
| Market Universe fragmentado | ❌ NO | Requiere config refactor separado |
| Streaming health | ❌ NO | Requiere endpoint en streaming_hydra |

---

## QUÉ PROBLEMAS NO RESUELVE PR #25

| Problema | Por qué no | Dónde pertenece |
|----------|------------|-----------------|
| Market Universe unificado | Scope creep — config refactor | PR separado (Market Universe) |
| Orderbook gap recovery | Complejidad alta, no gate | PR Orderbook Builder |
| Symbol normalization CCXT↔CF | Requiere shared utility | PR Market Universe |
| Instrument Metadata Registry | Nueva capacidad | PR Protocol Discovery |
| Unified Adapter | Major refactor, ADR needed | Post-B-49 |
| Schema Registry (Avro) | B-18 separado | PR B-18 |

---

## QUÉ DEBERÍA PERMANECER EN PR #25

1. `scripts/check_production_gates.py` — core deliverable
2. Integración CI: job `production-gate` en `ocm-ci.yml`
3. Tests: `tests/scripts/test_check_production_gates.py` (pos/neg)
4. OrderbookBronzeWriter **mínimo** (consumer + schema + tabla) — solo lo necesario para G9
5. health_check.sh extendido para streaming

---

## QUÉ DEBERÍA SER PRs POSTERIORES

| PR | Objetivo | Dependencia |
|----|----------|-------------|
| **PR Market Universe** | `config/market_data/universe.yaml` + `MarketUniverseProvider` | PR #25 (G9 PASS primero) |
| **PR Orderbook Builder** | Gap detection, recovery, BookBuilder, historical replay | PR #25 (Bronze OK) |
| **PR Protocol Discovery** | Bybit Discovery Profile (ADR-0017), metadata registry | PR Market Universe |
| **PR Unified Health** | Streaming `/health` endpoint + health_check.sh coverage | PR #25 |
| **PR Unified Adapter** | REST+WS single class per exchange (ADR needed) | Post-B-49 |

---

## RELACIÓN CON B-49

```
PR #25 = "Production Gate Binary Verdict" (ADR-0020)
         │
         ├── Entrega: check_production_gates.py
         ├── Habilita: B-49 PASS/FAIL ejecutable
         ├── Requiere: G9 PASS (OHLCV + Orderbook básico en Bronze)
         └── Bloquea: Release/CD hasta PASS
```

**Criterio de aceptación PR #25**:
```bash
$ uv run scripts/check_production_gates.py
{
  "G4": "PASS",
  "G5": "PASS", 
  "G6": "PASS",
  "G7": "PASS",
  "G8": "PASS",
  "G9": "PASS",  # DEBE incluir orderbook
  "G10": "PENDING",
  "G11": "PASS"
}
# Exit code: 0
```

---

## RIESGO DE SCOPE CREEP EN PR #25

**ALTO** — Tentación de meter:
- ❌ Market Universe refactor
- ❌ Orderbook gap recovery  
- ❌ Symbol normalization
- ❌ Instrument Registry

**Regla**: Si no es necesario para que `check_production_gates.py` retorne PASS en G1-G9, **NO va en PR #25**.

---

## CONCLUSIÓN

**PR #25 = `check_production_gates.py` + Orderbook Bronze mínimo + CI integration**

Todo lo demás → PRs posteriores con dependencias claras.