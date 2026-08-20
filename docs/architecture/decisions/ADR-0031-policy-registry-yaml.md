# ADR-0031: Policy Registry YAML (extensión tracking.yaml + M22..M26)

**Estado:** Aceptado (2026-08-19)
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

Adoptar **YAML como fuente de verdad del Policy Registry** (`policies/registry.yaml`), evolucionando el bloque `reglas:` de `tracking.yaml` hacia esquema completo, manteniendo compatibilidad con `audit_validator` M1..M20 y extendiéndolo con M22..M26.

**Esquema mínimo por regla (`PolicyRule`):**

```yaml
id: R17                          # único, estable
name: "Nombre legible"
description: "Descripción normativa"
scope: "domain | application | infrastructure | platform | cross-cutting"
severity: "HIGH | MEDIUM | LOW"
owner: "arquitectura | platform | trading | security | market_data"
enforcement: "blocking | warning | informational"
mechanism_type: "guard_script | tool_gate | absence_gate | import_linter"
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
  ticket: "B-XX"                    # deuda de backlog, obligatorio si waiver
status: "ACTIVE"                    # ACTIVE | DEPRECATED | SUPERSEDED
created: "2026-08-19"
modified: "2026-08-19"
history:
  - {date: "2026-08-19", author: "auditor", change: "created"}
```

**Nuevas reglas de validación M22..M26 en `scripts/audit_validator.py`**
(renumeración de las M21..M25 del borrador original: `M21` quedó ocupado por `m21_canonical_audit_filenames` en la implementación base; se mantiene esa numeración y las reglas de registry ocupan M22..M26):

- **M22**: tests obligatorios según `mechanism_type` — `guard_script`/`import_linter` exigen `tests.positive` y `tests.negative` resolubles en disco (AST); `tool_gate` exige `ci.job`+`ci.command`; `absence_gate` exige `evidence` de la ausencia. Con waiver → WARN; sin waiver → FAIL
- **M23**: `enforcement` obligatorio (enum `blocking|warning|informational`) + verificación contra gate de CI real para `blocking` (no `absence_gate`)
- **M24**: `dead rule` detection — regla `DEPRECATED` sin `absence_gate` o con waiver → FAIL
- **M25**: `waiver` con `expires` (ISO date) — waiver expirado → FAIL; waiver vigente → WARN; exige `motivo`, `adr` autorizante y `ticket` de backlog existente en `tracking.yaml`
- **M26**: `ADR huérfano` — ADR referenciado en registry sin archivo real en `docs/architecture/decisions/` → FAIL

**Mecanismos de enforcement (`mechanism_type`):** la implementación reconoce que no todo enforcement es un test de archivo pos/neg:

- `guard_script`: guard AST + tests positivos/negativos explícitos (R1, R3, R4, R7, R9..R16)
- `import_linter`: contrato de capas BC-NN con tests de contrato (R2)
- `tool_gate`: gate de CI (job + comando) que impone el umbral — p.ej. cobertura (`R5`) y bandit (`R6`) — sin test de archivo dedicado
- `absence_gate`: ausencia verificada de un módulo eliminado (R8, status DEPRECATED)

Los waivers cubren solo deuda temporal verificable (R7 → B-61, expira 2026-10-31), nunca un problema de modelado.

## Justificación técnica

- YAML + `audit_validator` = infraestructura existente, cero dependencias nuevas
- M1..M20 ya validan IDs únicos, enums, severidades, ADR states, refs, evidencia, control counts, informe↔registro, comandos canónicos, versiones, estados inventados, duplicados
- M22..M26 cierran los gaps de enforcement identificados por las auditorías (F-PLA-09, F-PLC-05/06/11)
- Compatible con `engineering_health_check.py` que ya valida `activada_en_ci` contra CI
- No introduce policy engine externo (OPA/Conftest/Semgrep/Sonar/CodeQL-as-gate) — consistente con decisión F-PL-08/F-PLC-08/F-PLA-08

## Consecuencias

- **Más fácil:** Policy Registry consumible por agente IA (parse YAML → conoce reglas, scope, tests, evidence, waivers, ownership)
- **Deuda aceptada:** Migración de `tracking.yaml:reglas` a esquema completo (B-61) — R5/R6/R8 (tool_gate/absence_gate) requieren cobertura explícita; R7 requiere activación en CI y consumo del waiver (expira 2026-10-31)
- **Contratos que hacen cumplir:** `audit_validator` M22..M26 + `engineering_health_check.py` (CI gate)
- **Relación con ADR-0020:** Production Gate = suma de reglas con `activada_en_ci: true` + `backtest: ok`; registry formaliza esto

## Referencias

- Código: `scripts/audit_validator.py`, `policies/registry.yaml`, `docs/plans/tracking.yaml`, `scripts/engineering_health_check.py`
- Hallazgos: B-47, B-48, B-49, B-51, B-52, B-53, B-54, B-55, B-56, B-61 (tracking.yaml)
- ADRs relacionados: ADR-0015, ADR-0020
- Auditorías: `AUDIT_OCM_POLICY_LAYER_FEASIBILITY_2026-08-19.md`, `AUDIT_OCM_POLICY_LAYER_COMPLEMENTARY_2026-08-19.md`, `AUDIT_OCM_POLICY_LAYER_COMPLEMENTARY_2026-08-19.md` (adversarial)