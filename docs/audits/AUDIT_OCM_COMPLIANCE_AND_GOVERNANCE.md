# AUDIT — OrangeCashMachine (OCM) — Compliance, Governance, Arquitectura y Auditabilidad

**Auditor:** sesión interactiva (usuario ejecuta comandos, Claude analiza evidencia cruda pegada)
**Fecha:** 2026-08-17
**Commit baseline:** `bee9fb5a3917c32fcc81fcc81fa5177ce0e57283` (main, HEAD, árbol limpio confirmado por `git status`)
**Metodología:** read-only estricto. Cero `git add/commit/push`. Cero modificaciones de código, tests, ADRs, CI o tracking.yaml. Toda cifra proviene de comandos ejecutados y pegados por el usuario en esta sesión, o de contenido de archivo leído directamente — nunca de memoria ni de informes previos sin re-verificar.

---

## 1. Executive Summary

OCM tiene un nivel de auditabilidad **significativamente por encima de lo típico** para un repositorio de un solo desarrollador: separación real entre regla (import-linter/architecture_linter), evidencia (tests golden + adversarial), gate de CI (9 jobs con dependencias explícitas), y gobernanza documental (ADRs con máquina de estados Propuesto/Aceptado, tracking.yaml con cadena de trazabilidad hallazgo→ADR→implementación→tests→CI→evidencia→cierre, validada por un script propio `engineering_health_check.py`).

Sin embargo, existen gaps reales: (a) un posible **gate roto en este momento** (pip-audit con CVEs no cubiertas por el ignore-list), (b) deuda arquitectónica activa y correctamente documentada pero no resuelta (7/10 reglas del linter en FAIL), (c) ausencia total de SBOM, firma de artefactos y herramientas de supply-chain más allá de lo que GitHub Actions ejecuta de forma gestionada, y (d) reglas de gobernanza diseñadas pero no activas en CI (R7, R8).

No hay evidencia de que ningún claim de "compliance formal" (ISO, SOC2, SLSA) haya sido hecho por el propio repositorio — es una base limpia para razonar sobre madurez, no una certificación.

## 2. Alcance

Lectura y ejecución read-only sobre `~/trading/orangecashmachine` en `orangehouse`. No se auditó infraestructura de producción (Docker/Kafka/Redis en ejecución), solo el repositorio y sus gates declarados.

## 3. Metodología

Evidencia clasificada según:
- **[EVIDENCIA]** — comando ejecutado y reproducido en esta sesión
- **[OPERATIVA_SSOT]** — archivo de configuración que gobierna comportamiento real (pre-commit, CI, import-linter config)
- **[REFERENCIA]** — leído pero no ejecutado (p. ej. workflows de CodeQL/Trivy/Gitleaks, cuya ejecución histórica en GitHub no pudo verificarse desde esta sesión sin acceso a `gh` CLI o red saliente)
- **[HISTÓRICA]** — proveniente de audits anteriores en `docs/audits/`, tratada como hipótesis hasta reverificación
- **[AMBIENTAL]** — discrepancia explicada por causa no arquitectónica (caché, banner desincronizado)

## 4. Estado real del repositorio

| Ítem | Valor | Evidencia |
|---|---|---|
| HEAD | `bee9fb5a3917c32fcc81fcc81fa5177ce0e57283` | `git rev-parse HEAD` |
| Branch | `main`, sincronizado con `origin/main` | `git status` |
| Working tree | Limpio | `git status` (reproducido) |
| Discrepancia banner (`main *2`) | AMBIENTAL — banner cacheado, no refleja `git status` real | contradicción resuelta explícitamente en esta sesión |

## 5. Governance actual

- **ADRs (27 archivos + template):** disciplina de estados real y consistente. Cada ADR declara `**Estado:** Aceptado` / `Propuesto` explícitamente. Varios llevan advertencia reforzada tipo `> **ESTADO: PROPUESTA** — NO aprobado. NO implementado.` (ADR-0021, ADR-0028). ADR-0029/0030 van más allá: separan explícitamente "decisión aprobada por el owner" de "implementación NO ejecutada todavía" — exactamente la disciplina PROPUESTA≠APROBADA≠IMPLEMENTADA que exige un modelo de auditabilidad serio.
- **tracking.yaml (schema_version 2):** SSOT operativo real. Cada hallazgo tiene una cadena estructurada (`hallazgo→backlog→adr→implementacion→tests→ci→evidencia→cierre`) con estados cerrados por enum (`PENDIENTE/EN_CURSO/HECHO/VERIFICACION/RECHAZADO`, `CONFIRMADO/NO_CONFIRMADO/...`). No es prosa libre — es un formato verificable programáticamente.
- **engineering_health_check.py:** gate de CI (`engineering-health` job) que valida coherencia interna del propio tracking.yaml: enums cerrados, `backtest: ok ⇒ activada_en_ci: true`, `estado: HECHO ⇒ evidencia no vacía`, contratos ≥ baseline (49), no-vacuo. Este script **es** el mecanismo de "quién audita al auditor" aplicado a la capa documental — corre antes que el resto de F2 y bloquea el merge con fail-fast (Sección 21 del prompt maestro, parcialmente resuelta).

Estado: **[CONFORME]** para disciplina de estados ADR; **[PARCIAL]** para cobertura de reglas activas en CI (ver R7/R8 abajo).

## 6. Architecture actual

- Import-linter: **50 contratos KEPT, 0 BROKEN**, reproducido en vivo (`uv run lint-imports --config architecture_linter/importlinter.toml`).
- CI exige un mínimo duro de 49 contratos (`ocm-ci.yml:43`), con mensaje de error que exige ADR + tracking.yaml para bajar el conteo — un guardrail contra regresión silenciosa de gobernanza.
- Mypy: **0 issues en 377 archivos** (`--no-incremental`, reproducido).
- Ruff: **All checks passed** (reproducido).

## 7. Architecture vs Architecture Linter

Fusión ya consumada: `architecture/` fue absorbido en `architecture_linter/` (según `docs/audits/2026-08-16-architecture-linter-consolidation.md`, HISTÓRICA pero consistente con la config actual: `architecture_linter/architecture_linter.toml` + `architecture_linter/importlinter.toml` en un solo directorio). No se detectó duplicación activa — evidencia consistente con SSOT único.

`architecture_linter/rules/`: 9 archivos de reglas (arch_001..010, falta arch_003 en el wc pero confirmado en el JSON de salida), 1517 líneas totales, más `base.py` y `__init__.py`. Tests: `conftest.py`, `test_golden.py`, `test_rules.py`, **`test_adversarial.py`** — la existencia de tests adversariales es evidencia positiva contra el riesgo "JUEZ = PARTE" (Sección 9/21 del prompt maestro): el linter no solo se prueba contra casos felices.

## 8. OCM Architecture Linter — auto-verificación (Sección 9)

**Resultado clave de esta sesión — contradicción histórica RESUELTA:**

Ejecución standalone (`python -m architecture_linter --json`), reproducida:

| Regla | Estado | Findings |
|---|---|---|
| ARCH-001 | FAIL | 1 |
| ARCH-002 | FAIL | 2 |
| ARCH-003 | PARTIAL | 1 |
| ARCH-004 | FAIL | 1 |
| ARCH-005 | FAIL | 1 |
| ARCH-006 | PASS | 1 |
| ARCH-007 | FAIL | 8 |
| ARCH-008 | FAIL | 1 |
| ARCH-009 | PASS | 1 |
| ARCH-010 | FAIL | 2 |

Golden test (`tests/architecture_linter/test_golden.py`), ejecutado **tras purgar todas las cachés** (`.import_linter_cache`, `.mypy_cache`, `.ruff_cache`, `.pytest_cache`) para eliminar la variable de contaminación que dejó abierta la revalidación anterior: **4 passed**. `GOLDEN_EXPECTED` en el código coincide exactamente, regla por regla, con el resultado standalone reproducido arriba.

**Conclusión verificable:** no hay divergencia entre golden y standalone. La afirmación histórica de un informe anterior ("solo ARCH-006 fallaba") queda formalmente clasificada como **AMBIENTAL / caché stale**, no como hallazgo de arquitectura ni como fallo del linter. Esto cierra el Conflicto #1 que quedó abierto en `2026-08-17-architecture-linter-golden-vs-standalone-revalidation.md`.

**Riesgo "JUEZ=PARTE":** mitigado parcialmente — el golden test es una fuente independiente del comportamiento esperado (fijado en código, requiere PR para cambiar), pero ambas fuentes (golden y motor) viven en el mismo repositorio y son mantenidas por el mismo autor. No hay una tercera fuente de corroboración cruzada activa (Semgrep/OPA no están instalados). Import-linter sí actúa como corroboración parcial independiente para las reglas de boundary (no para las semánticas como ARCH-001/002).

## 9. Matriz de controles

| Control | Evidencia | Herramienta | Comando reproducido | Resultado | Estado |
|---|---|---|---|---|---|
| Boundaries arquitectónicos | Reproducida | import-linter | `lint-imports --config architecture_linter/importlinter.toml` | 50 kept, 0 broken | **PASS** |
| Tipado estático | Reproducida | mypy | `mypy . --no-incremental` | 0 issues / 377 files | **PASS** |
| Lint / formato | Reproducida | ruff | `ruff check .` | All checks passed | **PASS** |
| Invariantes semánticos custom | Reproducida | architecture_linter | `python -m architecture_linter --json` | 7 FAIL / 1 PARTIAL / 2 PASS | **PARTIAL** (deuda documentada, no oculta) |
| Suite de tests | Reproducida | pytest | `pytest tests/ -q -m "not integration"` | 1164 passed, 4 deselected | **PASS** |
| Seguridad estática (código propio) | Reproducida | bandit | `bandit -r apps ocm packages shared infrastructure -ll` | 51 Low, 0 Medium/High | **PASS** (umbral -ll) |
| Vulnerabilidades de dependencias | Reproducida | pip-audit | `pip-audit . --ignore-vuln PYSEC-2026-113 --ignore-vuln PYSEC-2026-1325` | **4 vulns encontradas, 0 en la lista de ignorados** (aiohttp×3, cryptography×1) | **FAIL — ver Sección 10** |
| SAST más profundo (CodeQL) | Config presente, ejecución histórica NO_VERIFICADA desde esta sesión | GitHub Actions | — | — | **NO_VERIFICADO** |
| Secret scanning | Config presente, ejecución histórica NO_VERIFICADA | Gitleaks Action | — | — | **NO_VERIFICADO** |
| Filesystem/dependency scan | Config presente, ejecución histórica NO_VERIFICADA | Trivy Action | — | — | **NO_VERIFICADO** |
| Coherencia normativa (Plan↔tracking↔ADR↔CI) | Script leído, no ejecutado en esta sesión | `engineering_health_check.py` | — | — | **NO_VERIFICADO** (config coherente por inspección, no por ejecución) |
| Contratos ADR: estado Propuesto/Aceptado | Reproducida (grep) | — | `grep -riE "aceptad|propuest"` | Disciplina consistente | **PASS** |
| SBOM | No existe artefacto | — | — | — | **NO_INSTALADA** |
| Firma de artefactos / provenance | No existe | Cosign | `which cosign` → sin confirmación clara | — | **NO_VERIFICADO** |

## 10. Security posture

- Bandit: limpio a nivel BLOCKER (umbral `-ll`), 51 hallazgos Low preexistentes y ya documentados como deuda aceptada en sesiones anteriores.
- **pip-audit — hallazgo crítico de esta sesión:** el comando real de CI ignora únicamente `PYSEC-2026-113` (pyarrow) y `PYSEC-2026-1325` (ecdsa/python-jose), documentados con fecha 2026-08-03. La ejecución reproducida en vivo el 2026-08-17 encontró 4 vulnerabilidades **distintas**: `aiohttp 3.14.1` → `PYSEC-2026-3545/3546/3547` (fix disponible: 3.14.2/3.14.3) y `cryptography 49.0.0` → `PYSEC-2026-3552` (fix disponible: 50.0.0). Ninguna está en la ignore-list del workflow. **Esto implica que, salvo verificación en contrario en GitHub Actions, el job `quality` de CI está fallando ahora mismo o lo hará en el próximo push/PR.** No se pudo confirmar el estado del último run remoto (sin acceso a `gh` CLI ni red saliente desde esta sesión) — clasificado **NO_VERIFICADO** el estado remoto, pero el hallazgo local es reproducible y objetivo.
- CodeQL, Gitleaks, Trivy: configurados con triggers reales (push/PR a main + cron), permisos correctos (`security-events: write`), Trivy con acción pineada por SHA (buena práctica de supply chain para la propia acción de CI). Ejecución histórica no verificable desde esta sesión.

## 11. Supply-chain posture

- Sin lockfile de contenedor firmado, sin SBOM generado, sin Cosign confirmado instalado localmente.
- `uv.lock` existe (656K) — pinning de dependencias Python real.
- Herramientas de supply-chain más avanzadas (Syft, Grype, Cosign, OPA, Semgrep) — **no instaladas localmente** (`which` confirmó `not found` para syft, grype, gitleaks, semgrep, opa; cosign y trivy con resultado ambiguo en la captura, se recomienda reverificar con `which cosign; which trivy` por separado).
- Trivy sí corre en CI vía GitHub Action (no requiere binario local) — el hueco es solo en verificación local/reproducible por un tercero fuera de GitHub Actions.

## 12. CI/CD posture

`ocm-ci.yml`: 9 jobs reales con dependencias explícitas (`needs:`), no solo declarativos:
1. `architecture` — gate duro (≥49 contratos, no-vacuo)
2. `engineering-health` — gate de coherencia normativa, corre primero (fail-fast real)
3. `app-guard` — AST guard + backtest histórico + mypy apps/
4. `trading-guards` — R9/R10 (reconciliación de fills, ADR-0016)
5. `unit-tests` — coverage gate (fail_under=40%, baseline real 44%)
6. `security` — bandit -ll
7. `integration-tests` — Kafka real como service container con healthcheck
8. `config-validation` — Hydra bootstrap real
9. `quality` — ruff+format+mypy+SSOT+pip-audit (ver hallazgo Sección 10)

Separación build/test/security: **sí existe**, con jobs independientes y gates diferenciados. Reproducibilidad: alta — cada comando de CI es replicable localmente (y se replicó en esta sesión con resultados consistentes salvo pip-audit).

## 13. Traceability analysis

Cadena REQUISITO→...→EVIDENCIA verificada de punta a punta para el caso ARCH-001:
`hallazgo (H-08 histórico) → tracking.yaml (cadena estructurada) → ADR-0021 (estado: PROPUESTA, 2026-08-16) → implementación: PARCIAL (mitigación de observabilidad en fill_sync.py, unificación en PortfolioService PENDIENTE) → tests: PARCIAL → CI: sin gate específico (regla no resuelta) → evidencia: log crítico + tests de mitigación`.

**Ningún eslabón fue inventado ni asumido** — la cadena es explícita en `tracking.yaml` y consistente con el ADR real. Esto es evidencia fuerte de trazabilidad genuina, no aspiracional.

## 14. Auditability model / Auditor-independence

- Golden test = fuente independiente del comportamiento esperado del linter, mantenida en código versionado (requiere PR para cambiar) — mitigación parcial de "JUEZ=PARTE".
- `test_adversarial.py` existe — indica que se ha pensado en falsos positivos, aunque su cobertura específica no fue auditada línea por línea en esta sesión.
- `engineering_health_check.py` es una segunda capa de auditoría automatizada sobre la *documentación* (tracking.yaml/ADRs/CI), independiente del linter de arquitectura.
- No existe una tercera herramienta externa (OPA, Semgrep) corroborando las reglas semánticas del propio architecture_linter — sigue siendo la única fuente de verdad para ARCH-001..010.

## 15. Gaps encontrados

1. **pip-audit desalineado** — ignore-list desactualizada, CVEs reales sin mitigar ni documentar (Sección 10). **Prioridad: ALTA.**
2. R7 (paridad config.yaml) y R8 (dead stub reimportable) — diseñadas, `backtest: pendiente`, `activada_en_ci: false`. Deuda reconocida explícitamente, no oculta.
3. 7/10 reglas del architecture linter en FAIL — deuda arquitectónica real (posiciones con múltiples owners, sin balance real, freshness roto, contratos duplicados, stub de producción, estado mutable duplicado). Toda documentada y trazada, pero sin resolver.
4. Ausencia de SBOM y de firma de artefactos.
5. Ejecución histórica real de CodeQL/Gitleaks/Trivy no verificable desde esta sesión (requiere revisión manual del tab Security de GitHub o `gh run list`).
6. No se auditó `test_adversarial.py` en profundidad — pendiente para cerrar completamente la Sección 21 (auditoría del auditor).

## 16. Riesgos

- **Riesgo inmediato:** si el pip-audit gate está realmente roto, cualquier PR nuevo no puede mergear a `main` sin intervención — o peor, si alguien baja el nivel de exigencia del check para "pasar", se pierde visibilidad real sobre CVEs activas.
- **Riesgo de arquitectura:** ARCH-004 (sin balance real, sizing contra capital_usd) y ARCH-005 (freshness roto) tocan directamente el motor de trading — coherente con el veredicto histórico "Live-Readiness: NO" ya documentado en sesiones anteriores.
- **Riesgo de gobernanza:** ninguno detectado en esta sesión — la disciplina ADR/tracking es sólida.

## 17. Herramientas recomendadas

- **ADOPTAR:** ninguna herramienta nueva es estrictamente necesaria para elevar auditabilidad — el gap principal es *operativo* (arreglar pip-audit), no de tooling.
- **EVALUAR:** Syft+Grype (SBOM + scan) si se necesita entregar evidencia de supply-chain a un tercero formal; OPA solo si se quiere policy-as-code centralizada más allá de tracking.yaml (actualmente cubierto ad-hoc por `engineering_health_check.py`).
- **YA_CUBIERTA:** CodeQL, Gitleaks, Trivy (fs scan), bandit, import-linter, mypy, ruff, pip-audit (mecanismo presente, requiere mantenimiento).
- **NO_NECESARIA:** Semgrep — redundante con CodeQL para Python en este alcance salvo reglas custom muy específicas no cubiertas hoy.

## 18. Herramientas redundantes

Ninguna detectada — no hay duplicación de función entre las herramientas activas.

## 19. Architecture objetivo propuesta

No se diseña una nueva arquitectura en esta auditoría (fuera de alcance por regla suprema del prompt maestro). El modelo ya implementado (ADR/tracking → import-linter/architecture_linter → pytest → CI → evidencia) es consistente con el modelo objetivo genérico de la Sección 19 del prompt maestro, con `engineering_health_check.py` cumpliendo el rol de "evidence collector" de gobernanza documental.

## 20. Roadmap de adopción

1. Verificar y corregir el gate pip-audit (inmediato, bajo costo).
2. Confirmar ejecución histórica real de CodeQL/Gitleaks/Trivy vía GitHub Security tab.
3. Decidir prioridad de resolución de ARCH-001/004/005 (ya con ADRs relacionados, pendientes de aprobación/implementación).
4. Evaluar activar R7/R8 en CI si se prioriza esa deuda.

## 21. Decisiones que requieren aprobación humana

1. ¿Actualizar la ignore-list de pip-audit, o corregir las dependencias vulnerables (aiohttp→3.14.3, cryptography→50.0.0)?
2. ¿Aprobar ADR-0021 (estado de posición único dueño) para desbloquear ARCH-001?
3. ¿Priorizar R7/R8 para activación en CI o mantenerlas como deuda documentada?
4. ¿Instrumentar verificación remota de CodeQL/Gitleaks/Trivy (vía `gh` CLI) como parte del pipeline de auditoría recurrente?

## 22. Veredicto final

**[CONSISTENTE — REQUIERE DECISIONES HUMANAS]**

Justificación: la gobernanza documental (ADRs, tracking.yaml, engineering_health_check.py) es coherente y verificable — no se encontró ningún claim falso ni deuda oculta. La arquitectura tiene deuda real pero **correctamente trazada y reconocida**, no negada. El único hallazgo que cambia la clasificación de "CONSISTENTE" simple a "requiere decisiones humanas" es el posible gate de pip-audit roto — un hecho objetivo y reproducible que necesita decisión del owner (Orangel), no una corrección automática de este auditor.

No se declara "COMPLIANT" ni "CERTIFICADO" para ningún framework (ISO 27001, SOC2, SLSA) — no hay evidencia formal aplicable para ello, y el propio repositorio nunca reclamó esa certificación.

## Integridad de la sesión

- Código fuente modificado: NO
- Tests modificados: NO
- CI modificado: NO
- ADRs modificados: NO
- git add/commit/push ejecutados por esta sesión: NO
- Único artefacto generado: este informe
