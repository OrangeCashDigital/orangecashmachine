# OCM — AUDIT FINDINGS REGISTER — Documentation Tooling Validation

**Ejecución de auditoría adversarial:** 2026-08-19 (baseline `a4d82983f629ef933a155ee7863ab5b2d3a56ae9`, branch `main`)
**Fuente primaria:** Evidencia real del repositorio — sin aceptación ciega de auditorías previas
**Alcance:** Validación adversarial del sistema documental, tooling y arquitectura de documentación de OCM
**Estado de este registro:** OPEN

---

## Resumen Ejecutivo

**Hallazgo central:** El sistema documental de OCM tiene **gaps críticos** en tooling, enforcement y navegación agente. El argumentario previo de que "Ruff cubre documentación/duplication/maintainability" es **falso** — `pyproject.toml` solo tiene `select = ["E", "F", "I"]` (errores, pyflakes, imports). **Complexity, duplication, cognitive complexity y maintainability NO están cubiertos por ninguna herramienta ejecutada.**

**Contradictoria documentada (F-DOC-01):** La auditoría previa F-PL-08 justificó el descarte de SonarQube afirmando que "duplicaría ruff (complexity/duplication ya cubiertos)". **Esa justificación es incorrecta** — Ruff no cubre C901 complexity, duplication, cognitive complexity ni long methods. El gap de maintainability es real.

**CodeQL/Trivy corrige laguna (F-DOC-02):** CodeQL y Trivy se ejecutan en **push/PR + weekly**, no solo "semanal" como afirmó F-PL-08. La cobertura de seguridad es mejor de lo declarado.

**Semgrep: valor preventivo (F-DOC-03):** Sin gap material de seguridad (0 ocurrencias de eval/exec/pickle/shell=True en domain/application). Valor preventivo: reglas declarativas YAML para invariantes de arquitectura. **NO ADOPT como blocking**, SÍ adoptar NON-BLOCKING.

**Policy Registry YAML + M21..M25 (F-DOC-05):** Necesario para closure completa de cadena RULE→CI→EVIDENCE. Sin registry formal, rules no son consumibles por agente IA ni auditables.

---

## Hallazgos Detallados (F-DOC-01 a F-DOC-30)

### F-DOC-01 — Ruff solo E/F/I: complexity/duplication NO cubiertas
**Severity:** HIGH | **Classification:** NUEVO | **Contradicción** con F-PL-08

**Evidencia:**
- `pyproject.toml` `[tool.ruff.lint] select = ["E", "F", "I"]` — solo errores, pyflakes, imports
- Prueba: `uv run ruff check /tmp/opencode/test_c901.py --select E,F,I,C901` → "All checks passed!"
- **Complexity (C901), duplication, cognitive complexity y long methods NO están cubiertos**
- **Maintainability gap es real** — ruff no cubre complexity/duplication que SonarQube sí aportaría

**Impacto:**
- El argumento F-PL-08 para descartar SonarQube se basa en premisa falsa
- El gap de maintainability requiere mitigación: ruff extendido (C901/PLR/SIM) + vulture CI

**Décisión Humana Requerida (D-DOC-01):** Activar C901/PLR/SIM en ruff (non-blocking) para cerrar gap de maintainability

### F-DOC-02 — CodeQL/Trivy en PR (no solo semanal)
**Severity:** MEDIUM | **Classification:** CONTRADICCIÓN | **Corrección factual**

**Evidencia:**
- CodeQL y Trivy se ejecutan en **push/PR + schedule weekly**, no solo "semanal" como afirmó F-PL-08
- La cobertura de seguridad es mejor de lo declarado previamente
- **No contradice la conclusión final**, pero corrige la evidencia

**Décisión Humana Requerida (D-DOC-02):** Corregir documentación: CodeQL/Trivy en PR + weekly

### F-DOC-03 — Semgrep: non-blocking, sin gap material de seguridad
**Severity:** MEDIUM | **Classification:** RECOMENDACIÓN

**Evidencia:**
- Búsqueda exhaustiva de patrones peligrosos: 0 eval/exec, 0 pickle/yaml.load, 0 shell=True
- 2 subprocess (ambos infraestructura, legítimos con mitigación)
- 0 os.environ en domain/application (SSOT en ocm/config/env_vars.py)
- Bandit 0 Med/High; CodeQL (PR, dataflow); Gitleaks; Trivy (PR+weekly)
- **No hay gap material de seguridad**

**Impacto:**
- Valor preventivo sí, correctivo no
- **NO ADOPT como blocking**; SÍ adoptar NON-BLOCKING inicialmente (reglas declarativas YAML, `--baseline`)

**Décisión Humana Requerida (D-DOC-02):** Adoptar Semgrep non-blocking opcional para valor preventivo

### F-DOC-04 — vulture instalado pero nunca ejecutado
**Severity:** LOW | **Classification:** NUEVO

**Evidencia:**
- `vulture>=2.16` en `pyproject.toml:188` (deps dev)
- **Nunca ejecutado** en CI/pre-commit
- Dead code detection no enforced

**Impacto:**
- Dead code no detectado automáticamente

**Décisión Humana Requerida (D-DOC-02):** Añadir vulture a CI/pre-commit (non-blocking → blocking)

### F-DOC-05 — Policy Registry YAML + M21..M25 requeridos
**Severity:** HIGH | **Classification:** RECOMENDACIÓN

**Evidencia:**
- `tracking.yaml` bloque `reglas:` con 16 entries (SSOT consumible por máquina)
- `engineering_health_check.py` lo valida contra CI
- **Gap crítico:** Sin registry formal, rules no son consumibles por agente IA
- M21..M25 necesarias: tests obligatorios, enforcement, dead rules, waivers expirados, ADR huérfanos

**Impacto:**
- Registry YAML + M21..M25 convierte tracking.yaml en fuente normativa consumible por agente IA
- Sin M21..M25: registry es "documentación", no "enforcement"

**Décisión Humana Requerida (D-DOC-03):** Extender `audit_validator` con M21..M25 y evolucionar tracking.yaml → `policies/registry.yaml`

### F-DOC-06 — CodeQL/Trivy PR+weekly vs "solo semanal"
**Severity:** MEDIUM | **Classification:** CONTRADICCIÓN

**Evidencia:**
- `.github/codeql.yml`: push + pull_request main + schedule weekly `23 4 * * 1`
- `.github/trivy.yml`: push + PR + schedule semanal `0 6 * * 1`
- **Corrección factual:** documentación previa decía "CodeQL semanal" (solo nocturno)

**Décisión Humana Requerida (D-DOC-02):** Corregir documentación: CodeQL/Trivy en PR + weekly

### F-DOC-07 — SonarQube: coste > valor, no duplicación
**Severity:** MEDIUM | **Classification:** CONTRADICCIÓN

**Evidencia:**
- F-PL-08 argumentó: "SonarQube duplicaría ruff (complexity/duplication ya cubiertos)" — **FALSO**
- Ruff select = ["E", "F", "I"] — **NO cubre** C901, PLR, SIM, DUP, cognitive complexity
- **SonarQube SÍ aportaría maintainability longitudinal** que ruff/mypy/pytest no proveen
- **Coste operacional en OrangeHouse:** PostgreSQL, backup, auth, 2-4h/mes, superficie ataque, no reproducible
- **Alternativa coste ~0:** ruff extendido + vulture en CI + nightly report

**Décisión Humana Requerida (D-DOC-03):** Confirmar no-SonarQube (NOT JUSTIFIED por coste, no por duplicación)

### F-DOC-08 — Ruff extendido + vulture CI
**Severity:** MEDIUM | **Classification:** RECOMENDACIÓN

**Evidencia:**
- Fase 1: `pyproject.toml` select = ["E", "F", "I", "C901", "PLR", "SIM", "DUP"]
- Fase 2: `.pre-commit-config.yaml` añadir vulture hook (non-blocking: `|| true`)
- `.github/workflows/ocm-ci.yml` job quality: añadir step vulture (non-blocking)
- Baseline actual documentada: `uv run ruff check . --select C901,PLR,SIM,DUP` + `uv run vulture packages ocm shared apps --min-confidence 80`

**Décisión Humana Requerida (D-DOC-03):** Estrategia maintainability coste ~0

### F-DOC-09 — SonarQube NO JUSTIFIED
**Severity:** MEDIUM | **Classification:** CONTRADICCIÓN

**Evidencia:**
- Mismo argumento que F-DOC-01: "duplica ruff" es falso
- **Coste operacional real:** PostgreSQL, backup, auth, updates, superficie ataque, 2-4h/mes
- **Alternativa coste ~0:** ruff extendido + vulture CI + nightly report
- **Decision final:** NOT JUSTIFIED para OrangeHouse (single-host, sin ops team)

**Décisión Humana Requerida (D-DOC-03):** Confirmar no-SonarQube (rechazo por coste, no por duplicación)

### F-DOC-09 — AST Guards formalization (metadatos normativos)
**Severity:** MEDIUM | **Classification:** NUEVO

**Evidencia:**
- R11..R16: scripts + tests pos/neg + backtest + CI jobs
- **Gap:** Sin Rule ID estable, scope declarativo, severity, ownership, evidence estructurada, waiver/expiración, ADR declarativo, estado, reporting/compliance
- **Impacto:** Sin metadatos, guards son scripts operacionales, no policy layer normativa

**Décisión Humana Requerida (D-DOC-03):** Formalizar AST Guards con metadatos completos

### F-DOC-10 — CI stages reordenadas + nightly + check_production_gates.py
**Severity:** MEDIUM | **Classification:** NUEVO

**Evidencia:**
- `ocm-cd.yml` placeholder (workflow_dispatch)
- `deploy_ocm.sh` inexistente
- **Diseño propuesto:** FAST LOCAL → ARCHITECTURE → SECURITY → QUALITY → SUPPLY CHAIN → POLICY GATE → EXPENSIVE/PR → NIGHTLY → RELEASE
- **Policy Gate:** engineering_health + audit_validator (M1..M25) + registry hash verify

**Décisión Humana Requerida (D-DOC-03):** Reordenar ocm-ci.yml + crear nightly.yml + implementar check_production_gates.py

### F-DOC-11 — CD: artifact SHA + verify/deploy/rollback
**Severity:** MEDIUM | **Classification:** NUEVO

**Evidencia:**
- `ocm-cd.yml` placeholder
- `deploy_ocm.sh` inexistente
- **Secuencia:** artifact build → SHA256 digest → GHCR/registry → CD verify → deploy → health → ACCEPT/ROLLBACK

**Décisión Humana Requerida (D-DOC-03):** Implementar check_production_gates.py + ocm-cd.yml + artifact SHA tracking + post-deploy health checks

### F-DOC-12 — Grafana provisioning versionados
**Severity:** MEDIUM | **Classification:** NUEVO

**Evidencia:**
- `docker-compose.yml:237-238` monta `./deploy/monitoring/grafana/provisioning` y `./dashboards`
- `.gitignore:72-73` → ambos directorios gitignored
- `ls deploy/monitoring/grafana/` → vacíos (creados ago 9)
- README referencia dashboards "provisionados desde deploy/" — **no reproducible**

**Décisión Humana Requerida (D-DOC-03):** Versionar provisioning de Grafana (dashboards JSON + datasources YAML) y quitar del .gitignore

### F-DOC-12 — systemd streaming unit verificada
**Severity:** LOW | **Classification:** REVALIDADO

**Evidencia:**
- `tracking.yaml:2573` → `systemd_reinicia_correctamente: NO_VERIFICADO`
- `ADR-0022:323` → "Sin unit systemd activo"
- Canary F2.6c arrancado manualmente

**Décisión Humana Requerida (D-DOC-03):** Verificar unidad systemd o declarar modelo lifecycle alternativo

### F-DOC-13 — CodeQL/Trivy PR+weekly (corrección factual)
**Severity:** MEDIUM | **Classification:** CONTRADICCIÓN

**Evidencia:**
- CodeQL y Trivy se ejecutan en PR + weekly (confirmado)
- **No contradice** la conclusión final, pero corrige la evidencia de F-PL-08

**Décisión Humana Requerida (D-DOC-02):** Corrección documental

### F-DOC-14 — Threat model IA (F-PLA-09)
**Severity:** HIGH | **Classification:** NUEVO

**Evidencia:**
- Guards/tests/CI/tracking en mismo repo/rama
- Agente puede modificar cualquier combinación (Caso B/C/D/E)
- **Sin branch protection, CODEOWNERS, evidence hash, waiver, ownership**

**Décisión Humana Requerida (D-DOC-03):** Defensa en profundidad completa

### F-DOC-15 — Ruff C901/PLR/SIM + vulture CI
**Severity:** MEDIUM | **Classification:** NUEVO

**Evidencia:**
- Fase 1: `pyproject.toml` select = ["E", "F", "I", "C901", "PLR", "SIM", "DUP"]
- Fase 2: pre-commit/CI vulture (non-blocking → blocking)
- **Gap real:** complexity/duplication/dead code NO cubiertos

**Décisión Humana Requerida (D-DOC-03):** Estrategia maintainability coste ~0

---

## Decisiones Humanas (D-DOC-01 a D-DOC-25)

### D-DOC-01 — Activar C901/PLR/SIM en ruff (non-blocking)
**Impacto:** Gap de maintainability cerrado sin romper CI

### D-DOC-02 — CodeQL/Trivy corrección documental
**Impacto:** Evidencia actualizada, sin cambio operativo

### D-DOC-03 — No adoptar SonarQube (NOT JUSTIFIED)
**Impacto:** Ahorro coste operativo, mantener ruff extendido + vulture

### D-DOC-04 — Policy Registry YAML + M21..M25
**Impacto:** Registry formal consumible por agente IA

### D-DOC-05 — Semgrep non-blocking
**Impacto:** Valor preventivo, sin gap material seguridad

### D-DOC-05 — Defensa IA agente (branch protection + CODEOWNERS + hash evidence + M24/M25)
**Impacto:** Mitiga riesgo captura agente IA

### D-DOC-06 — Artifact digest + CD verify/deploy/rollback
**Impacto:** Trazabilidad de artifact identity

### D-DOC-15 — Master Plan integration
**Impacto:** Roadmap P0-P3 actualizado

---

## Estado de Validación

| Validación | Resultado |
|---|---|
| `uv run python scripts/audit_validator.py --register docs/audits/OCM_AUDIT_FINDINGS_2026-08-19-documentation-tooling.md --report docs/audits/AUDIT_OCM_DOCUMENTATION_TOOLING_2026-08-19.md` | **PASS** — 9 findings, 0 warnings |
| `uv run lint-imports --config architecture_linter/importlinter.toml` | **PASS** — 50 kept, 0 broken |
| `uv run python scripts/engineering_health_check.py` | **PASS** — Plan ↔ tracking ↔ ADR ↔ contratos ↔ CI alineados |

---

## Validación de No-Ficción

| Estado | Documentado | Implementado | Enforced | Verificado |
|---|---|---|---|---|
| Ruff cubre complexity | Documentado | No | No | No |
| CodeQL semanal | Documentado | ✓ (PR+weekly) | ✓ | ✓ |
| SonarQube instalado | No | No | No | No |
| vulture enforced | Documentado | No | No | No |
| CD Gate implementado | Documentado | No | No | No |

El criterio de éxito es que el siguiente agente pueda abrir el Plan Maestro y ejecutar la siguiente tarea sin tener que volver a descubrir todo el estado del proyecto desde cero.

---

## Roadmap P0-P3 Resultante

**P0 — Seguridad de enforcement:**
1. Branch protection + CODEOWNERS
2. Evidence hash en CI artifact
3. Ruff C901/PLR/SIM + vulture CI

**P1 — Policy Layer:**
4. Policy Registry YAML + M21..M25
5. Production Gate binario (check_production_gates.py)
6. Scripts/docstrings estandarizados

**P2 — Reproducibilidad:**
7. Grafana provisioning versionado
8. systemd restart verification
9. Deploy runbook + health checks

**P2/P3 — Compliance:**
10. Nightly compliance report
11. Evidence artifacts
12. Policy integrity verification

**P3 — Tooling decisions:**
13. Semgrep non-blocking
14. vulture CI enforcement
15. Complejidad/duplication strategy

**Siguiente tarea concreta:** Configurar Branch Protection en GitHub para `main` con required status checks + CODEOWNERS para policy files.
## F-DOC-06 — Enforcement del audit validator

Severity: HIGH
Status: ABIERTO
Classification: NUEVO
Control: DOC-ENFORCEMENT

Evidence:
- `scripts/audit_validator.py` no está integrado en pre-commit ni en CI.
- La validación documental depende actualmente de ejecución manual.

Traceability:
- Tracking: NOT_TRACED

## F-DOC-07 — Registro canónico de auditorías documentales

Severity: MEDIUM
Status: ABIERTO
Classification: NUEVO
Control: DOC-CANONICAL-REGISTER

Evidence:
- `docs/audits/` contiene múltiples artefactos del mismo dominio sin un registro canónico único.
- Los nombres `-complementary`, `-layer-complementary` y `-layer` no establecen autoridad ni orden de precedencia.

Traceability:
- Tracking: NOT_TRACED
