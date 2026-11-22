# AUDIT FORENSE — OrangeCashMachine (OCM)
## Compliance, Governance, Arquitectura, Seguridad, Supply Chain y Auditabilidad

**Fecha de auditoría:** 2026-08-18  
**Commit Baseline (E-001):** `bee9fb5a3917c32fcc81fcc81fa5177ce0e57283`  
**Branch (E-002):** `main` (sincronizado con `origin/main`)  
**Working Tree Inicial (E-003):** 2 archivos sin seguimiento (`docs/audits/AUDIT_OCM_COMPLIANCE_AND_GOVERNANCE.md`, `docs/audits/AUDIT_OCM_COMPLIANCE_GOVERNANCE_ARCHITECTURE_2026-08-17.md`), preservados intactos.

---

### 1. Executive Summary

La presente auditoría forense aplica un rigor estricto de evidencia reproducible sobre el repositorio `orangecashmachine`. Se confirma un diseño arquitectónico sólido en capas (Clean Architecture / Hexagonal) con separación estricta de dominios y 50 contratos de importación validados (`import-linter`). Sin embargo, la ejecución standalone del linter de arquitectura (`architecture_linter`) revela que 7 de las 10 reglas semánticas avanzadas se encuentran en estado `FAIL` (ARCH-001, ARCH-002, ARCH-004, ARCH-005, ARCH-007, ARCH-008, ARCH-010), demostrando deuda técnica documentada y reconocida pero no resuelta. Asimismo, la ejecución de `pip-audit` identifica vulnerabilidades activas en dependencias de terceros (`aiohttp`, `cryptography`, `pyarrow`, `ecdsa`), afectando la postura de seguridad de la cadena de suministro.

---

### 2. Scope

- Repositorio completo: `/home/orangemusic/trading/orangecashmachine`
- Componentes: `packages/`, `apps/`, `shared/`, `ocm/`, `infrastructure/`, `architecture_linter/`, `tests/`, `.github/workflows/`, `docs/`.

---

### 3. Methodology

Clasificación estricta de fuentes de evidencia:
- **[EVIDENCE]** — Comando ejecutado directamente durante esta auditoría.
- **[SSOT]** — Configuración que gobierna el comportamiento real.
- **[DOCUMENTAL]** — Documentación interna (ADRs, tracking).
- **[HISTORICAL]** — Informes de auditoría anteriores (tratados exclusivamente como contexto).
- **[REMOTE_UNVERIFIED]** — Acceso a GitHub Actions no verificable directamente desde la sesión local.

---

### 4. Repository State

- **Commit:** `bee9fb5a3917c32fcc81fcc81fa5177ce0e57283`
- **Branch:** `main`
- **Working Tree:** Limpio de cambios en código; dos archivos de auditoría previa sin seguimiento conservados.

---

### 5. Governance

- **ADRs:** 30 ADRs formalizados con estados explícitos (`Aceptado`, `Propuesto`, `Superado`).
- **tracking.yaml:** Esquema v2 estructurado con trazabilidad hallazgo→backlog→adr→implementacion→tests→ci→evidencia→cierre.
- **engineering_health_check.py:** PASS verificado normativamente.

---

### 6. Architecture

- **Domain:** Aislado de infraestructura y frameworks de datos (BC-03, BC-09, BC-01 KEPT).
- **Composition Roots:** Definidas por bounded context (`market_data`, `trading`, `portfolio`).

---

### 7. Architecture Linter

Ejecución standalone (`python -m architecture_linter --json`):

| Regla | Estado | Hallazgos |
|---|---|---|
| ARCH-001 | **FAIL** | Múltiples owners del estado de posición |
| ARCH-002 | **FAIL** | Divergencia semántica (WAC/acumulación vs reemplazo) |
| ARCH-003 | **PARTIAL** | Reconciliación submit-time presente, sin loop periódico |
| ARCH-004 | **FAIL** | Balance configurado (`capital_usd`) ≠ balance real consultado |
| ARCH-005 | **FAIL** | Silence detection presente, freshness propagation/enforcement ausente |
| ARCH-006 | **PASS** | Sin puertos huérfanos |
| ARCH-007 | **FAIL** | 8 contratos duplicados u homónimos |
| ARCH-008 | **FAIL** | Stub de producción activo (`WSTradesSource`) |
| ARCH-009 | **PASS** | Sin violaciones de capas |
| ARCH-010 | **FAIL** | Almacenes mutables de posición/orden duplicados |

- **Golden Regression Tests:** `pytest tests/architecture_linter/test_golden.py -q` → **PASSED** (el comportamiento esperado del linter es estable).
- **Adversarial Tests:** `pytest tests/architecture_linter/test_adversarial.py -q` → **PASSED**.

---

### 8. Tests

- **Unit Tests:** `pytest tests/ -x -q -m "not integration"` → **1164 passed, 4 deselected**.
- **Coverage:** 51.46% (supera el umbral de CI de 40%).

---

### 9. Security

- **Bandit:** 0 Medium/High severity (51 Low). Estado: **PASS** localmente.

---

### 10. Dependency Security (`pip-audit`)

Ejecución reproducible (`pip-audit .`):
- `aiohttp` (3.14.1) — PYSEC-2026-3545, PYSEC-2026-3546, PYSEC-2026-3547 (Severidad Alta)
- `cryptography` (49.0.0) — PYSEC-2026-3552 (Severidad Alta)
- `pyarrow` (19.0.1) — PYSEC-2026-113 (Severidad Media)
- `ecdsa` (0.19.2) — PYSEC-2026-1325 (Severidad Media)
- **Estado:** **FAIL** (6 vulnerabilidades detectadas en 4 paquetes).

---

### 11. Supply Chain

- `uv.lock` presente y reproducible.
- SBOM, firma de artefactos (Cosign) y provenance: **No implementados**.

---

### 12. CI/CD (`ocm-ci.yml`)

- Configurado con jobs y dependencias (`needs`).
- Ejecución remota: **REMOTE_UNVERIFIED**.

---

### 13. Documentation

- Documentación organizada en `docs/architecture/`, `docs/audits/`, `docs/plans/`.

---

### 14. Traceability

- Vinculación sólida entre `tracking.yaml`, ADRs y código ejecutable.

---

### 15. Auditor Independence

- Riesgo **Juez = Parte** presente: el motor de auditoría (`architecture_linter`) y sus pruebas golden residen en el mismo repositorio y son mantenidos por el mismo equipo.

---

### 16. Findings

- **F-SEC-01 (High):** 6 vulnerabilidades CVE detectadas en dependencias de terceros por `pip-audit`.
- **F-ARCH-01 (Medium):** 7 reglas de `architecture_linter` en FAIL (duplicidad de estado mutable, stubs activos, divergencias semánticas).

---

### 17. Risk Matrix

| ID | Severidad | Descripción | Mitigación |
|---|---|---|---|
| F-SEC-01 | HIGH | CVEs en `aiohttp`, `cryptography`, `pyarrow`, `ecdsa` | Actualizar dependencias en `pyproject.toml` |
| F-ARCH-01 | MEDIUM | Estado mutable duplicado y stubs de WS activos | Refactorización planificada en Fase 3 |

---

### 18. Control Matrix

| Dominio | Control | Herramienta | Resultado |
|---|---|---|---|
| Architecture | Boundaries | import-linter | **PASS** |
| Architecture | Invariantes | architecture_linter | **FAIL** |
| Quality | Tipado | mypy | **PASS** |
| Quality | Lint/Format | ruff | **PASS** |
| Testing | Unitaria | pytest | **PASS** |
| Security | SAST | bandit | **PASS** |
| Dependencies | CVEs | pip-audit | **FAIL** |
| CI/CD | Pipeline | GitHub Actions | **NO_VERIFICADO** |

---

### 19. Human Decisions Required

1. Actualizar dependencias vulnerables detectadas por `pip-audit`.
2. Aprobar la estrategia de unificación de estados mutables para la Fase 3.

---

### 20. Remediation Roadmap

- **P0:** Actualizar dependencias CVE.
- **P1:** Eliminar stubs de producción (`WSTradesSource`).
- **P2:** Unificar almacenes mutables de posición.

---

### 21. Final Verdict

**AUDIT_READY_WITH_FINDINGS**

---

### 22. Evidence Index

- E-001: `git rev-parse HEAD` → `bee9fb5a3917c32fcc81fcc81fa5177ce0e57283`
- E-002: `git branch --show-current` → `main`
- E-003: `git status --short` → working tree verificado
- E-034: `python -m architecture_linter --json` → 7 FAIL, 1 PARTIAL, 2 PASS
- E-035: `pytest tests/architecture_linter/test_golden.py -q` → 4 passed
- E-036: `pip-audit .` → 6 vulnerabilidades en 4 paquetes

---

### 23. Repository Integrity

- **Estado:** OK (únicamente archivos documentales creados/modificados).
