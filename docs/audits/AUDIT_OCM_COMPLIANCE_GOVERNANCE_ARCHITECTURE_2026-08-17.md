# AUDIT — OrangeCashMachine (OCM)
## Compliance, Governance, Arquitectura, Seguridad y Auditabilidad

**Fecha de auditoría:** 2026-08-17 / 2026-08-18  
**Commit Baseline:** `bee9fb5a3917c32fcc81fcc81fa5177ce0e57283` (rama `main`, sincronizada con `origin/main`, working tree limpio excluyendo archivo sin seguimiento de auditoría previa).  
**Metodología:** Read-only estricto sobre el repositorio. Ejecución directa de herramientas (`import-linter`, `architecture_linter`, `mypy`, `ruff`, `pytest`, `bandit`, `pip-audit`, `engineering_health_check.py`). Verificación cruzada entre código ejecutable, configuración, pruebas y documentación normativa.

---

### 1. Executive Summary

OrangeCashMachine (OCM) demuestra un nivel de madurez técnica, estructuración Clean/Hexagonal y disciplina de gobernanza documental **muy por encima de la media** para proyectos de su escala. El repositorio cuenta con separación estricta de dominios, contratos de importación validados formalmente (`import-linter`), un motor de gobierno arquitectónico por AST (`architecture_linter` con pruebas golden y adversariales), un validador de salud de ingeniería (`engineering_health_check.py`), y disciplina estricta de estados en ADRs (`Propuesto`, `Aceptado`, `Superado`) y trazabilidad estructurada (`tracking.yaml` v2).

No obstante, la auditoría integral revela hallazgos críticos de deuda técnica y brechas operativas:
1. **Arquitectura semántica (`architecture_linter`):** 7 de 10 reglas se encuentran en estado `FAIL` (ARCH-001, ARCH-002, ARCH-004, ARCH-005, ARCH-007, ARCH-008, ARCH-010), reflejando duplicación de estado mutable, divergencias semánticas en posiciones, stubs de producción activos y contratos homónimos.
2. **Seguridad de dependencias (`pip-audit`):** Se detectaron 6 vulnerabilidades conocidas en dependencias de terceros (`aiohttp`, `cryptography`, `pyarrow`, `ecdsa`), lo cual rompe el umbral de dependencias seguras si no se gestionan adecuadamente.
3. **Supply Chain y Firma:** Ausencia de SBOM formal, firma de artefactos (Cosign) y escaneo de dependencias bloqueante estricto en el pipeline principal.
4. **Independencia del Auditor ("Juez = Parte"):** El linter de arquitectura y sus golden tests residen y son mantenidos en el mismo repositorio por el mismo equipo, careciendo de validación externa independiente (ej. OPA, Semgrep standalone externo).

---

### 2. Scope

- Repositorio completo: `/home/orangemusic/trading/orangecashmachine`
- Componentes auditados: `packages/`, `apps/`, `shared/`, `ocm/`, `infrastructure/`, `architecture_linter/`, `tests/`, `.github/workflows/`, `docs/`.
- Exclusiones: Entorno de ejecución en vivo con capital real (`live`), infraestructura externa en contenedores (Kafka, Redis, Prometheus en ejecución local).

---

### 3. Methodology

Toda afirmación se respalda en una de las siguientes categorías de evidencia:
- **[EVIDENCIA_REPRODUCIDA]** — Comando ejecutado exitosamente en esta sesión y verificado por salida estándar.
- **[CONFIGURACIÓN_SSOT]** — Archivo de configuración que gobierna directrices normativas (p. ej., `importlinter.toml`, `ocm-ci.yml`).
- **[INSPECCIÓN_ESTRUCTURAL]** — Lectura directa de código fuente, esquemas y registros normativos.
- **[HISTÓRICA]** — Antecedentes documentales en `docs/audits/` revalidados contra el HEAD actual.

---

### 4. Git Baseline

| Métrica | Valor Verificado | Comando |
|---|---|---|
| **Commit HEAD** | `bee9fb5a3917c32fcc81fcc81fa5177ce0e57283` | `git rev-parse HEAD` |
| **Branch** | `main` (sincronizado con `origin/main`) | `git branch --show-current`, `git status` |
| **Working Tree** | Limpio (excepto informe sin seguimiento) | `git status --short` |
| **Diferencia con Origin** | 0 commits ahead / 0 commits behind | `git rev-list --left-right --count HEAD...origin/main` |

---

### 5. Repository State

El repositorio se encuentra operativo en el commit baseline indicado. No hay cambios pendientes en código de producción, tests ni configuración de CI.

---

### 6. Architecture Assessment

- **Clean / Hexagonal Architecture:** Respetada estrictamente en bounded contexts principales (`market_data`, `trading`, `portfolio`).
- **Dependencia de Capas:** Dominio (`domain`) exento de frameworks de infraestructura y dependencias de base de datos u ORMs.
- **Composition Root:** Cada bounded context cuenta con su propio punto de composición estricto (`market_data.infrastructure.bootstrap.composition_root`, `trading.bootstrap.composition_root`, `portfolio.bootstrap.composition_root`).

---

### 7. Architecture Linter

Ejecución del linter independiente de arquitectura (`python -m architecture_linter --json`):

| Regla ID | Nombre de Regla | Estado | Hallazgos |
|---|---|---|---|
| **ARCH-001** | Position Mutability Ownership | **FAIL** | 1 bloque (múltiples owners de posición/orden) |
| **ARCH-002** | Position Semantic Divergence | **FAIL** | 2 divergencias (WAC/acumulación vs reemplazo/pop) |
| **ARCH-003** | Order State Without Reconciliation | **PARTIAL** | 1 hallazgo (reconciliación submit-time, sin loop periódico) |
| **ARCH-004** | Balance State (configured vs exchange) | **FAIL** | 1 hallazgo (capital estático vs balance real de exchange) |
| **ARCH-005** | Market Data Freshness Boundary | **FAIL** | 1 hallazgo (silence detection presente, freshness propagation ausente) |
| **ARCH-006** | Orphaned Contract / Port | **PASS** | 0 puertos huérfanos |
| **ARCH-007** | Duplicate / Homonymous Contracts | **FAIL** | 8 contratos duplicados u homónimos |
| **ARCH-008** | False Capability / Stub | **FAIL** | 1 stub activo (`WSTradesSource` termina de inmediato) |
| **ARCH-009** | Layer / Dependency Governance | **PASS** | 0 violaciones de capas |
| **ARCH-010** | Duplicated Mutable State | **FAIL** | 2 almacenes mutables duplicados de position/order |

---

### 8. Golden vs Standalone

- **Pruebas Golden (`tests/architecture_linter/test_golden.py`):** Ejecutadas tras purgar cachés locales (`.import_linter_cache`, `.pytest_cache`, `.mypy_cache`, `.ruff_cache`).
- **Resultado:** 4 pasadas (`test_golden_statuses_repo_actual`, `test_golden_arch006_orphan_ports`, `test_golden_arch007_duplicates`, `test_golden_arch008_stubs`).
- **Discrepancia:** Ninguna. El resultado standalone (`python -m architecture_linter --json`) coincide exactamente con el `GOLDEN_EXPECTED` definido en código. Las alertas previas sobre ARCH-006 se debían a contaminación ambiental por cachés obsoletas.

---

### 9. Code Quality

- **Mypy (`uv run mypy . --no-incremental`):** 0 issues encontrados en 377 archivos analizados (`PASS`).
- **Ruff (`uv run ruff check .` y `ruff format . --check`):** Todos los checks de estilo y formato pasados exitosamente (`PASS`).

---

### 10. Testing

- **Unit Tests (`uv run pytest tests/ -x -q -m "not integration"`):** 1164 tests pasados exitosamente, 4 deselected.
- **Cobertura:** Umbral configurado en CI (40%); cobertura real medida en 51.46% (`PASS`).

---

### 11. Security

- **Bandit (`uv run bandit -r apps ocm packages shared infrastructure -ll`):** 0 issues Medium/High (51 Low severity reportadas en análisis estático por uso de aserciones o logs, consideradas aceptables con umbral `-ll`). Estado: **PASS**.

---

### 12. Dependency Security

- **Pip-Audit (`uv run pip-audit .`):** Encontradas 6 vulnerabilidades conocidas en 4 paquetes:
  - `aiohttp` (3.14.1): PYSEC-2026-3545, PYSEC-2026-3546, PYSEC-2026-3547
  - `cryptography` (49.0.0): PYSEC-2026-3552
  - `pyarrow` (19.0.1): PYSEC-2026-113
  - `ecdsa` (0.19.2): PYSEC-2026-1325
  - **Estado:** **FAIL** (requiere actualización o exclusiones documentadas en tracking/pipeline).

---

### 13. Supply Chain

- **Lockfile (`uv.lock`):** Presente y reproducible mediante `uv sync`.
- **SBOM / Firma / Provenance:** Ausentes formalmente en el pipeline de entrega.
- **SHA Pinning en GitHub Actions:** Parcial (algunos actions usan etiquetas flotantes como `@v4`).

---

### 14. CI/CD

- **Workflow Principal (`.github/workflows/ocm-ci.yml`):**
  - Jobs configurados con dependencias explícitas (`needs`).
  - Gates obligatorios: `architecture` (import-linter ≥ 49), `engineering-health`, `app-guard`, `trading-guards`, `unit-tests`, `security` (bandit).
  - Estado del linter custom (`architecture_linter`): Se ejecuta en pruebas locales y golden, pero el gate duro en CI está delegado primariamente a `import-linter` (BC-NN) y `engineering_health_check.py`.

---

### 15. Governance

- **ADRs:** 30 ADRs formalizados con estados explícitos (`Aceptado`, `Propuesto`, `Superado`).
- **tracking.yaml:** Estructura formal v2 que vincula hallazgos, backlogs, ADRs, implementación, pruebas y CI.
- **engineering_health_check.py:** Ejecutado exitosamente (`[EngineeringHealth] PASS — Plan ↔ tracker ↔ ADR ↔ contratos ↔ CI alineados`).

---

### 16. ADR Assessment

- Los ADRs reflejan de manera transparente tanto las decisiones de diseño adoptadas como la deuda técnica reconocida (p. ej., ADR-0021, ADR-0028, ADR-0029, ADR-0030).
- Coherencia mantenida entre la documentación de arquitectura y el código ejecutable.

---

### 17. Tracking / Traceability

- Trazabilidad formal verificada para los hitos críticos (F1, F2).
- Cadena de cumplimiento validada por el script de salud de ingeniería.

---

### 18. Auditor Independence

- **Riesgo "Juez = Parte":** Presente. El motor de auditoría (`architecture_linter`) y sus pruebas golden son mantenidos internamente en el mismo repositorio y por el mismo equipo de desarrollo. No existe auditoría de terceros independiente automatizada en CI (ej. escáneres OPA externos).

---

### 19. Documentation Integrity

- Integridad general alta. Los informes de auditoría previos se conservan por valor histórico, y los nuevos reportes de revalidación eliminan contradicciones por caché.

---

### 20. Findings

1. **FINDING-01 (Architecture Linter FAILs):** 7 reglas en FAIL (ARCH-001, 002, 004, 005, 007, 008, 010) que reflejan duplicidad de estados mutables y stubs de WS.
2. **FINDING-02 (Dependency Vulnerabilities):** 6 CVEs detectadas por `pip-audit` en paquetes de terceros (`aiohttp`, `cryptography`, `pyarrow`, `ecdsa`).
3. **FINDING-03 (Auditor Independence):** Falta de motor de governance externo independiente al repositorio.

---

### 21. Risk Matrix

| Severidad | Descripción | Impacto | Probabilidad | Mitigación |
|---|---|---|---|---|
| **HIGH** | Vulnerabilidades en dependencias (`pip-audit`) | Explotación de CVEs en transporte o criptografía | Media | Actualizar paquetes afectados en `pyproject.toml` |
| **MEDIUM** | Duplicidad de estado mutable de posición/orden (ARCH-010) | Desincronización de estado en ejecución compleja | Media | Consolidar en `PortfolioService` / `PositionStore` (F3) |
| **LOW** | Dependencia de linter interno ("Juez=Parte") | Falsos positivos no detectados externamente | Baja | Mantener test adversarial y revisión humana |

---

### 22. Control Matrix

| Dominio | Control | Herramienta | Evidencia | Resultado | Severidad |
|---|---|---|---|---|---|
| Architecture | Boundaries de paquetes | import-linter | `lint-imports --config ...` | **PASS** | LOW |
| Architecture | Invariantes semánticos | architecture_linter | `python -m architecture_linter --json` | **FAIL** | MEDIUM |
| Code Quality | Tipado estático | mypy | `mypy . --no-incremental` | **PASS** | LOW |
| Code Quality | Lint y formato | ruff | `ruff check .` | **PASS** | LOW |
| Testing | Suite unitaria | pytest | `pytest tests/ -q` | **PASS** | LOW |
| Security | SAST código propio | bandit | `bandit -r ... -ll` | **PASS** | LOW |
| Dependencies | Vulnerabilidades CVE | pip-audit | `pip-audit .` | **FAIL** | HIGH |
| Supply Chain | Lockfile | uv | `uv.lock` presente | **PASS** | LOW |
| CI/CD | Pipeline automatizado | GitHub Actions | `ocm-ci.yml` | **PASS** | LOW |
| Governance | Salud normativa | engineering_health_check | Script Python | **PASS** | LOW |
| Documentation | ADRs y tracking | Estructura markdown/yaml | Directorio `docs/` | **PASS** | LOW |

---

### 23. Human Decisions Required

1. **Aceptación o remediación de vulnerabilidades de dependencias (`pip-audit`):** El owner debe decidir si actualizar `aiohttp`, `cryptography`, `pyarrow` y `ecdsa` inmediatamente o registrar una excepción justificada.
2. **Priorización de la Fase de Refactorización Arquitectónica:** Aceptar formalmente los FAILs del linter de arquitectura como deuda técnica planificada para la Fase 3, o bloquear evoluciones funcionales hasta su resolución.
3. **Activación de Reglas de Governance Pendientes:** Evaluar la activación en CI de las reglas R7 y R8 de `tracking.yaml`.

---

### 24. Remediation Roadmap

- **P0 (Inmediato):** Actualizar dependencias con vulnerabilidades críticas reportadas por `pip-audit`.
- **P1 (Alta Prioridad):** Resolver el stub activo en `WSTradesSource` (ARCH-008) y unificar los almacenes mutables de posición (ARCH-010).
- **P2 (Media Prioridad):** Mitigar los contratos duplicados (ARCH-007) y consolidar la semántica de posiciones (ARCH-001 / ARCH-002).
- **P3 (Mejora):** Incorporar escaneo SBOM y firma de artefactos en el pipeline de CI/CD.

---

### 25. Final Verdict

**AUDIT_READY_WITH_FINDINGS**  
*(El repositorio cuenta con un nivel sobresaliente de auditabilidad, trazabilidad y controles automatizados, exhibiendo al mismo tiempo deuda técnica arquitectónica explícita y vulnerabilidades de dependencias que requieren atención humana prioritaria).*

---

### 26. Evidence Index

- Configuración de import-linter: `architecture_linter/importlinter.toml`
- Configuración de linter arquitectónico: `architecture_linter/`
- Salud de ingeniería: `scripts/engineering_health_check.py`
- Trazabilidad y Backlog: `docs/plans/tracking.yaml`
- Pipeline de CI: `.github/workflows/ocm-ci.yml`
