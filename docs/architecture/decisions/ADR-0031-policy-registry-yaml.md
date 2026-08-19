# ADR-0031: Policy Registry YAML (extensión tracking.yaml + M21..M25)

**Estado:** Propuesto
**Fecha:** 2026-08-19
**Bounded context(s) afectado(s):** ocm (plataforma), shared, packages (todos)

## Contexto

La auditoría de Policy Layer (2026-08-19: feasibility + complementary + adversarial) identificó que OCM ya tiene ~70% de la Policy Layer en forma embrionaria:

- `tracking.yaml` bloque `reglas:` con R1..R16 (`id`, `descripcion`, `mecanismo`, `backtest`, `activada_en_ci`) — SSOT consumible por máquina
- `engineering_health_check.py` valida coherencia Plan↔tracking↔ADR↔contratos↔CI
- ADR-0020 convierte `activada_en_ci` en normativo (gate de release)
- ADR-0015 formaliza patrón guard AST + tests pos/neg + backtest + CI
- `scripts/audit_validator.py` (M1..M20) valida registros con enums cerrados y reconciliación matemática

**Gap crítico para control de agentes IA (F-PLA-09, F-PLC-05/06/11):**
- Sin `owner` humano por regla → sin accountability → agente puede cambiar regla sin revisión
- Sin `tests` obligatorios declarados → reglas sin validación
- Sin `enforcement` obligatorio + verificación CI real
- Sin `waiver` con `expires` (ISO date) → excepciones eternas
- Sin `evidence` estructurada + hash → evidencia falsificable
- Sin `dead rule` detection → reglas huérfanas
- Sin `ADR huérfano` detection → deuda documental
- Sin `status` (ACTIVE/DEPRECATED/SUPERSEDED) ni `history`

## Alternativas evaluadas

1. **Extender `tracking.yaml` in-place** — Ventaja: compatibilidad total, SSOT único, `audit_validator` ya lo parsea. Desventaja: tracking.yaml crece, mezcla backlog + registry.
2. **Nuevo `policies/registry.yaml`** — Ventaja: separación conceptual, esquema dedicado. Desventaja: duplicación de R1..R16, migración necesaria.
3. **Registry Python (clases + metaclases)** — Ventaja: tipado estático. Desventaja: rompe principio "YAML consumible por máquina sin runtime Python", añade complejidad, no parseable por `audit_validator` sin importar OCM.

## Decisión

Adoptar **YAML como fuente de verdad del Policy Registry**, evolucionando el bloque `reglas:` de `tracking.yaml` hacia esquema completo, manteniendo compatibilidad con `audit_validator` M1..M20 y extendiéndolo con M21..M25.

**Esquema mínimo por regla (`PolicyRule`):**

```yaml
id: R17                          # único, estable
name: "Nombre legible"
description: "Descripción normativa"
scope: "domain | application | infrastructure | platform | cross-cutting"
severity: "HIGH | MEDIUM | LOW"
owner: "arquitectura | platform | trading | security | market_data"
enforcement: "blocking | warning | informational"
mechanism: "import-linter | AST guard | Semgrep | bandit | mypy | custom"
tests:
  positive: "tests/architecture/test_xxx_guard.py::TestPositive"
  negative: "tests/architecture/test_xxx_guard.py::TestNegative"
evidence:
  type: "backtest | ci_log | artifact_hash"
  path: "scripts/backtest_xxx.py"
  hash: "sha256:..."                 # hash inmutable de evidencia
ci:
  job: "app-guard"
  command: "uv run pytest ..."
master_plan: "F2.1"
adr: "ADR-0031"
waiver:
  allowed: true
  expires: "2026-12-31"             # ISO date, obligatorio si waiver
  adr: "ADR-XXXX"
status: "ACTIVE"                    # ACTIVE | DEPRECATED | SUPERSEDED
created: "2026-08-19"
modified: "2026-08-19"
history:
  - {date: "2026-08-19", author: "auditor", change: "created"}
```

**Nuevas reglas de validación M21..M25 en `scripts/audit_validator.py`:**

- **M21**: `tests.positive` y `tests.negative` obligatorios (path + patrón) — evita reglas sin validación
- **M22**: `enforcement` obligatorio + verificación contra CI real (engineering_health base)
- **M23**: `dead rule` detection — regla sin implementación referenciada (guard script / Semgrep rule / import-linter contract) → FAIL
- **M24**: `waiver` con `expires` (ISO date) — waiver expirado → FAIL; waiver sin ADR → FAIL
- **M25**: `ADR huérfano` — ADR referenciado en registry sin rule asociada → WARNING

## Justificación técnica

- YAML + `audit_validator` = infraestructura existente, cero dependencias nuevas
- M1..M20 ya validan IDs únicos, enums, severidades, ADR states, refs, evidencia, control counts, informe↔registro, comandos canónicos, versiones, estados inventados, duplicados
- M21..M25 cierran los gaps de enforcement identificados por las auditorías (F-PLA-09, F-PLC-05/06/11)
- Compatible con `engineering_health_check.py` que ya valida `activada_en_ci` contra CI
- No introduce policy engine externo (OPA/Conftest/Semgrep/Sonar/CodeQL-as-gate) — consistente con decisión F-PL-08/F-PLC-08/F-PLA-08

## Consecuencias

- **Más fácil:** Policy Registry consumible por agente IA (parse YAML → conoce reglas, scope, tests, evidence, waivers, ownership)
- **Deuda aceptada:** Migración de `tracking.yaml:reglas` a esquema completo requiere esfuerzo; mantener compatibilidad M1..M20 durante transición
- **Contratos que hacen cumplir:** `audit_validator` M21..M25 + `engineering_health_check.py` (CI gate)
- **Relación con ADR-0020:** Production Gate = suma de reglas con `activada_en_ci: true` + `backtest: ok`; registry formaliza esto

## Referencias

- Código: `scripts/audit_validator.py`, `docs/plans/tracking.yaml`, `scripts/engineering_health_check.py`
- Hallazgos: B-47, B-48, B-49, B-51, B-52, B-53, B-54, B-55, B-56 (tracking.yaml)
- ADRs relacionados: ADR-0015, ADR-0020
- Auditorías: `AUDIT_OCM_POLICY_LAYER_FEASIBILITY_2026-08-19.md`, `AUDIT_OCM_POLICY_LAYER_COMPLEMENTARY_2026-08-19.md`, `AUDIT_OCM_POLICY_LAYER_COMPLEMENTARY_2026-08-19.md` (adversarial)