# ADR-0033: Production Gate binario (check_production_gates.py G1..G11)

**Estado:** Propuesto
**Fecha:** 2026-08-19
**Bounded context(s) afectado(s):** ocm (plataforma), CI/CD

## Contexto

El Plan Maestro (§1, §6, §10) y ADR-0020 documentan un **Production Gate binario** (`scripts/check_production_gates.py` → PASS/FAIL sobre G1..G11) como criterio objetivo de "apto para producción". Sin embargo:

- `ls scripts/` → **NO existe** `check_production_gates.py` (8 scripts reales)
- El gate G1..G11 no tiene veredicto binario ejecutable
- `engineering_health_check.py` (F2.0) valida coherencia normativa pero **NO** el Production Gate de release completo (G1..G11)
- Un agente de IA **no puede demostrar "production-ready" con un comando** (F-PL-04, F-PLA-05)

**Checks G1..G11 definidos en Plan Maestro §6 y ADR-0020:**

| Check | Fuente | Umbral | Estado actual |
|---|---|---|---|
| G1. Sin LiveExecutor stub | AST guard R1 | `uv run live` no arranca con stub | IMPLEMENTADO (R1) |
| G2. Composition root construye pipelines | smoke test R2 | verde | IMPLEMENTADO (R2) |
| G3. Contador de riesgo correcto | round-trip R3 | verde | IMPLEMENTADO (R3) |
| G4. Secrets redactados en snapshot | test R4 | verde | IMPLEMENTADO (R4) |
| G5. Contratos BC válidos | `lint-imports` | verde (conteo vivo ≥49) | IMPLEMENTADO |
| G6. Cobertura crítica | `pytest --cov` | `fail_under=40` (baseline 44%) | IMPLEMENTADO |
| G7. Bandit limpio | CI `security` | sin BLOCKER (0 HIGH) | IMPLEMENTADO |
| G8. Mypy completo | CI `quality` | sin errores | IMPLEMENTADO |
| G9. Paridad de config | test R7 | verde | **PENDIENTE** (R7 `backtest: pendiente`, `activada_en_ci: false`) |
| G10. Estado de posición único | test B-15 | verde | F4 (B-15 `EN_CURSO`) |
| G11. Trazabilidad activa | test B-17 | verde | F4 (B-17 `HECHO`) |

## Alternativas evaluadas

1. **Implementar `check_production_gates.py` como script binario** — Ventaja: veredicto único consumible por agente; reutiliza patrones `engineering_health_check.py` / `audit_validator.py`; F-PL-04/F-PLA-05 lo requieren. Desventaja: nuevo script (mantenimiento).
2. **Shell + CI jobs agregados** — Ventaja: sin nuevo código. Desventaja: sin veredicto agregado normativo; no consumible programáticamente.
3. **Makefile target** — Ventaja: simple. Desventaja: no da veredicto agregado.

## Decisión

Implementar **`scripts/check_production_gates.py`** como script binario que produce veredicto `PASS`/`BLOCK` + lista de reglas que provocaron el resultado + evidencia por cheque.

**Interfaz:**
```bash
$ uv run python scripts/check_production_gates.py [--mode gate-dev|gate-release]
# Exit 0 = PASS, Exit 1 = BLOCK
# stdout: JSON con {verdict, checks: [{id, name, status, evidence, threshold, actual}]}
```

**Implementación (reutilizando patrones existentes):**
- Patrón idéntico a `engineering_health_check.py`: clase `ProductionGate`, método `run()`, salida JSON
- Cada cheque G1..G11 → método `check_G1()`, `check_G2()`, etc.
- Reutiliza `lint-imports` count, `pytest --cov` output, `bandit -ll`, `mypy`, etc.
- Lee umbrales de `pyproject.toml` y `tracking.yaml` (SSOT)
- `gate-dev` mode: todos los cheques (G1..G9 + G10/G11 si F4 cerrada)
- `gate-release` mode: estricto, todos G1..G11 obligatorios

**Integración CI:**
- Job `policy-gate` (serial, tras security+quality) ejecuta `check_production_gates.py --mode gate-dev`
- Job `release-gate` (manual/tag) ejecuta `--mode gate-release`
- FAIL bloquea merge (gate-dev) o release (gate-release)

## Justificación técnica

- **Patrón probado**: `engineering_health_check.py` + `audit_validator.py` ya usan esta arquitectura (clase + run() + JSON output + exit codes)
- **Veredicto binario consumible**: agente IA puede ejecutar `check_production_gates.py` y obtener PASS/BLOCK inequívoco
- **Evidencia por cheque**: cada G1..G11 reporta status + evidence + threshold + actual → auditoría automática
- **Extensible**: nuevos checks G12+ se añaden como métodos sin cambiar interfaz
- **Consistente con ADR-0020**: Production Gate = suma de reglas `activada_en_ci: true` + `backtest: ok`

## Consecuencias

- **Más fácil:** Release decision = un comando; no leer 10 jobs CI
- **Deuda aceptada:** Nuevo script mantenimiento; mitigar con tests unitarios + backtest
- **Prerequisitos:** G9 (paridad config) y G10 (estado posición único) deben estar HECHOS para gate-release completo
- **Contratos que hacen cumplir:** ADR-0020 (Production Gate normativo), `ocm-ci.yml` job `policy-gate`

## Referencias

- Código: `scripts/engineering_health_check.py` (patrón), `scripts/audit_validator.py` (patrón)
- Hallazgos: B-49 (tracking.yaml)
- ADRs relacionados: ADR-0020, ADR-0031, ADR-0032
- Auditorías: `AUDIT_OCM_POLICY_LAYER_FEASIBILITY_2026-08-19.md` (F-PL-04), `AUDIT_OCM_POLICY_LAYER_COMPLEMENTARY_2026-08-19.md` (F-PLA-05)