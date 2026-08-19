# ADR-0036: vulture/complexity strategy (ruff C901/PLR/SIM + vulture CI)

**Estado:** Propuesto
**Fecha:** 2026-08-19
**Bounded context(s) afectado(s):** ocm (plataforma), CI/CD, quality tooling

## Contexto

La auditoría adversarial (F-PLA-01, F-PLA-02) identificó dos gaps reales de maintainability:

1. **Ruff solo E/F/I** (`pyproject.toml:189` `[tool.ruff.lint] select = ["E", "F", "I"]`):
   - C901 (mccabe/complexity) NO cubierta
   - PLR (refactor: long functions, nested blocks) NO cubierta
   - SIM (simplify) NO cubierta
   - DUP (duplication) NO cubierta
   - Cognitive complexity NO cubierta
   - Prueba: `uv run ruff check /tmp/opencode/test_c901.py --select E,F,I,C901` → "All checks passed!"

2. **vulture>=2.16 instalado** (`pyproject.toml:188` en dev deps) pero **nunca ejecutado** en CI/pre-commit:
   - Dead code detection no enforced
   - 0 matches en `.github/workflows/` ni `.pre-commit-config.yaml`

**Estado actual de maintainability tooling:**
| Herramienta | Qué cubre | Estado |
|---|---|---|
| ruff E/F/I | errores, pyflakes, imports | BLOCKING (CI + pre-commit) |
| ruff C901/PLR/SIM/DUP | complexity, refactor, simplify, duplication | **NO ACTIVADO** |
| mypy | tipos, unused, unreachable | BLOCKING |
| pytest --cov | coverage (fail_under=40, baseline 44%) | BLOCKING |
| vulture | dead code | **INSTALADO PERO NO ENFORCED** |
| SonarQube | maintainability trend, cognitive complexity, duplication | **NO JUSTIFIED (ADR-0035)** |

## Alternativas evaluadas

1. **Activar todo en ruff + vulture CI blocking ya** — Ventaja: gap cerrado ya. Desventaja: posible ruido inicial (falsos positivos en código legacy), rompe CI sin margen.
2. **Activar non-blocking → blocking progresivo** — Ventaja: margen para limpiar código; CI verde durante transición. Desventaja: gap persiste temporalmente.
3. **Solo ruff extendido, sin vulture** — Ventaja: una herramienta menos. Desventaja: vulture detecta dead code que ruff no ve (imports no usados, funciones no llamadas, clases no instanciadas).
4. **Solo vulture, sin ruff extendido** — Ventaja: dead code. Desventaja: no cubre complexity/duplication/cognitive complexity.

## Decisión

**Estrategia en dos fases (coste ~0, reproducible, config as code):**

### Fase 1 — Non-blocking (inmediato, F2.1)
1. `pyproject.toml`: `select = ["E", "F", "I", "C901", "PLR", "SIM", "DUP"]` (añadir complexity/duplication rules)
2. `.pre-commit-config.yaml`: añadir `vulture` hook (non-blocking: `vulture packages ocm shared apps --min-confidence 80 || true`)
3. `.github/workflows/ocm-ci.yml` job `quality`: añadir step `vulture` (non-blocking: `|| true`)
3. Documentar baseline actual: `uv run ruff check . --select C901,PLR,SIM,DUP` + `uv run vulture packages ocm shared apps --min-confidence 80` → registrar count en tracking.yaml

### Fase 2 — Blocking (tras limpieza, F2.1/F2.2)
1. Limpiar violations reales (PRs dedicados, uno por regla/archivo)
2. Cambiar pre-commit/CI a blocking (quitar `|| true`)
3. Añadir a `engineering_health_check.py` validación: ruff C901/PLR/SIM/DUP + vulture sin hallazgos nuevos vs baseline
4. Policy Registry (ADR-0031): registrar reglas C901/PLR/SIM/DUP + vulture con owner, severity, evidence, tests

**Reglas ruff a activar (priorizadas):**
- `C901` — mccabe complexity (>10)
- `PLR0911` — too many return statements
- `PLR0912` — too many branches
- `PLR0913` — too many arguments
- `PLR0915` — too many statements
- `PLR1702` — too many nested blocks
- `SIM102` — nested ifs
- `SIM117` — merge nested ifs
- `DUP` — duplication detection

## Justificación técnica

- **Gap real confirmado** (F-PLA-01/02): complexity/duplication/dead code NO cubiertas por ninguna herramienta enforced
- **Coste ~0**: ruff ya corre en CI; vulture ya instalado; solo activar reglas
- **Reproducible**: config en `pyproject.toml` + pre-commit + CI; sin servidor/DB
- **Progresivo**: non-blocking → blocking evita CI roto; baseline documentada en tracking.yaml
- **Coherente con ADR-0035**: alternativa coste ~0 a SonarQube para maintainability
- **Compatible con ADR-0031**: reglas registradas en Policy Registry con owner/evidence/tests

## Consecuencias

- **Más fácil:** Maintainability tooling completo sin SonarQube; CI detecta complexity/duplication/dead code
- **Deuda aceptada:** Código legacy puede tener violations iniciales; fase non-blocking da margen
- **Contratos que hacen cumplir:** `pyproject.toml` ruff select, CI job quality + pre-commit, `engineering_health_check.py` validación
- **Relación con ADR-0031/0035:** Reglas en Policy Registry; alternativa a SonarQube

## Referencias

- Código: `pyproject.toml` (ruff select, vulture), `.pre-commit-config.yaml`, `.github/workflows/ocm-ci.yml`
- Hallazgos: B-47, B-48 (tracking.yaml)
- ADRs relacionados: ADR-0031, ADR-0035
- Auditorías: `AUDIT_OCM_POLICY_LAYER_COMPLEMENTARY_2026-08-19.md` (adversarial, F-PLA-01/02)