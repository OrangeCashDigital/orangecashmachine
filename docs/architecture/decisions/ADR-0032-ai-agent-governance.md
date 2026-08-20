# ADR-0032: AI Agent Governance (branch protection, CODEOWNERS, evidence hash, waiver/expiración)

**Estado:** Aceptado (2026-08-19)
**Fecha:** 2026-08-19
**Bounded context(s) afectado(s):** ocm (plataforma), shared, packages (todos), CI/CD

## Contexto

La auditoría adversarial (F-PLA-09) y complementaria (F-PLC-06/11) demostraron que **un agente de IA con acceso de escritura a `main` puede capturar el sistema de validación completo**:

| Caso | Descripción | Detectado por | Gap actual |
|---|---|---|---|
| **B** | Agente cambia código + **AST Guard** para ocultar violación | CodeQL (dataflow) + **Policy Registry hash** (guard modificado ≠ hash registrado) | Requiere hash inmutable de guards en artifact CI |
| **C** | Agente cambia código + **CI** para eliminar gate | **Branch protection** (required status checks) + workflow file review | Requiere GitHub branch protection rules (no en repo hoy) |
| **D** | Agente agrega excepción a una regla (waiver) | **M24** (waiver requiere ADR + expiración) + **M25** (ADR huérfano) | Requiere M24/M25 implementados |
| **E** | Agente modifica tracking/ADR/docs para aparentar cumplimiento | **Policy Registry hash** + **audit_validator M6/M9** + **evidencia firmada** | Requiere hash de registry + tracking en artifact CI |

**Evidencia real:** guards (`scripts/*_guard.py`), tests (`tests/architecture/test_*_guard.py`), registry (`tracking.yaml` / futuro `policies/registry.yaml`), CI (`.github/workflows/ocm-ci.yml`) viven en el **mismo repo y rama**. Sin separación de privilegios, un agente puede modificar cualquier combinación y obtener PASS.

**Estado actual:**
- Branch protection: **NO configurada** en GitHub
- CODEOWNERS: **NO existe**
- Evidence hash: **NO existe** (solo git_hash inyectado desde composition root, manipulable)
- Waiver mechanism: **NO existe** (M24/M25 pendientes)
- Policy changes: **NO requieren PR separado ni revisión humana**

## Alternativas evaluadas

1. **Solo documentar el riesgo** — Ventaja: cero esfuerzo. Desventaja: riesgo real no mitigado (F-PLA-09 CRÍTICO).
2. **Branch protection + CODEOWNERS** — Ventaja: defensa nativa GitHub, cero código. Desventaja: requiere configuración en GitHub (no en repo), no impide que owner malicioso apruebe.
3. **Branch protection + CODEOWNERS + evidence hash + M24/M25 + policy change PR separation** — Ventaja: defensa en profundidad completa. Desventaja: más configuración, pero toda en GitHub/CI, sin infraestructura nueva.

## Decisión

Implementar **defensa en profundidad completa** (opción 3) como prioridad P0:

### 1. Branch Protection en `main` (GitHub config)
- Required status checks: `architecture`, `engineering-health`, `app-guard`, `domain-guard`, `trading-guards`, `unit-tests`, `security`, `config-validation`, `quality`
- Required reviews: 1 approval mínimo
- Dismiss stale reviews on new commits
- Require review from CODEOWNERS for policy files

### 2. CODEOWNERS (`.github/CODEOWNERS`)
```
# Policy files - require architecture team review
/scripts/*_guard.py          @arquitectura
/policies/                   @arquitectura
/architecture_linter/        @arquitectura
/docs/plans/tracking.yaml    @arquitectura
.github/workflows/ocm-ci.yml @arquitectura
```

### 3. Evidence Hash en CI Artifact (`.github/workflows/ocm-ci.yml`)
- Job `policy-gate` (nuevo, serial tras security+quality):
  - Calcula SHA256 de: `scripts/*_guard.py`, `policies/registry.yaml` (o `tracking.yaml`), `.github/workflows/ocm-ci.yml`
  - Compara contra hash registrado en `policies/evidence.json` (artifact inmutable)
  - FAIL si mismatch → evidencia de manipulación (Caso B/E)

### 4. M24/M25 en `audit_validator.py` (ADR-0031)
- **M24**: waiver con `expires` (ISO date) obligatorio; waiver expirado → FAIL; waiver sin ADR → FAIL
- **M25**: ADR referenciado en registry sin rule asociada → WARNING

### 5. Separación de PRs para Policy Changes
- PRs que tocan archivos en CODEOWNERS → label `policy-change` obligatoria
- CI job verifica label + approval de CODEOWNERS
- No merge directo a `main` sin revisión humana explícita

## Justificación técnica

- **Cero infraestructura nueva**: todo es configuración GitHub + CI + scripts existentes
- **Mitiga Caso B/C/D/E** del threat model demostrado por F-PLA-09/F-PLC-06
- **Compatible** con `audit_validator` M24/M25 (ADR-0031) y `engineering_health_check.py`
- **No rompe** flujo de desarrollo normal: feature PRs no tocan policy files
- **Principio "Automatización > disciplina" (Plan §3, Principio 2)**: elimina dependencia de revisión manual espontánea

## Consecuencias

- **Más fácil:** Auditoría de cambios de policy trazable y enforzable
- **Deuda aceptada:** Configuración inicial en GitHub (branch protection, CODEOWNERS) requiere admin access; no versionable en repo (limitación GitHub)
- **Contratos que hacen cumplir:** Branch protection (GitHub), CODEOWNERS (GitHub), CI job `policy-gate` (hash verification), `audit_validator` M24/M25
- **Relación con ADR-0031:** M24/M25 implementados en `audit_validator` validan waivers/ADRs del Policy Registry

## Referencias

- Código: `scripts/audit_validator.py` (M24/M25), `.github/workflows/ocm-ci.yml`, `.github/CODEOWNERS`
- Hallazgos: B-52, B-56 (tracking.yaml)
- ADRs relacionados: ADR-0015, ADR-0020, ADR-0031, ADR-0033
- Auditorías: `AUDIT_OCM_POLICY_LAYER_COMPLEMENTARY_2026-08-19.md` (F-PLA-09, F-PLC-06/11)