# ADR-0035: SonarQube decision (NOT JUSTIFIED — coste operacional)

**Estado:** Propuesto
**Fecha:** 2026-08-19
**Bounded context(s) afectado(s):** ocm (plataforma), CI/CD

## Contexto

La auditoría de viabilidad (F-PL-08) concluyó "No introducir SonarQube" argumentando: *"SonarQube duplicaría ruff+mypy+pytest (complexity/duplication/coverage ya cubiertas)"*.

La auditoría adversarial (F-PLA-01, F-PLA-08) **demostró que esta justificación era FALSA**:

- `pyproject.toml` `[tool.ruff.lint]` `select = ["E", "F", "I"]` — **solo errores, pyflakes, imports**
- **NO cubiertas por ruff**: C901 (complexidad ciclomática), PLR (refactor), SIM (simplify), DUP (duplicación), ANN (annotations), TID, PT, cognitive complexity
- **Prueba real**: `uv run ruff check /tmp/opencode/test_c901.py --select E,F,I,C901` → "All checks passed!" — función con complejidad >10 NO detectada

**Por tanto:** SonarQube **SÍ aportaría una señal longitudinal de maintainability** que ruff/mypy/pytest no proveen con la configuración actual (complexity, cognitive complexity, duplication, long methods, coupling, deuda técnica trend).

**Sin embargo**, la auditoría complementaria (F-PLC-08) y adversarial (F-PLA-08) evaluaron el coste real en OrangeHouse:

| Requisito | Realidad OrangeHouse | Coste |
|---|---|---|
| Base de datos | PostgreSQL NO existe | +1 contenedor + 512MB RAM + backup |
| Almacenamiento | Volúmenes Docker locales (sin backup DR) | Sin persistencia garantizada |
| Backup | No hay backup documentado | Debe implementarse desde cero |
| Actualizaciones | Java + SonarQube upgrades manuales | Maintenance window + compatibilidad plugins |
| Autenticación | Local (sin LDAP/OIDC) — admin token en .env | Superficie de ataque + secret management |
| Acceso local | Puerto 9000 (¿loopback? ¿reverse proxy?) | Si loopback: inútil para CI; si expuesto: riesgo |
| Integración GitHub | GitHub App / PAT / webhook | Config + secret + network egress |
| Integración CI | `sonar-scanner` + quality gate wait | +2-5 min/PR; quality gate binario sin contexto OCM |
| Reproducibilidad | Estado en DB + config en UI — **NO reproducible desde repo** | **Viola principio "config as code"** |
| Coste operacional | ~1-2 GB RAM + 1 CPU + DB + backup + updates + auth | **Alto** para single-host sin equipo ops dedicado |
| Superficie ataque | Web UI + API + DB + webhook receiver | Crítico si expuesto; inútil si solo localhost |
| Mantenimiento | Logs, GC, reindex, plugin updates, version upgrades | ~2-4 horas/mes |

**Alternativa coste ~0 (identificada por F-PLA-08):**
- Ruff extendido: activar `C901, PLR, SIM, DUP` en `select` → complexity/duplication cubierta
- vulture en CI/pre-commit → dead code detection (ya instalado vulture>=2.16, nunca ejecutado, F-PLA-02)
- Nightly compliance report → trend histórico maintainability

## Alternativas evaluadas

1. **Adoptar SonarQube Server (Community Edition)** — Ventaja: maintainability trend + quality gates + coverage visualization. Desventaja: coste operacional alto, PostgreSQL requerido, backup/DR desde cero, auth, updates, superficie ataque, viola reproducibilidad, 2-4h/mes maintenance.
2. **SonarCloud (SaaS)** — Ventaja: sin infra propia. Desventaja: datos en cloud, network egress, coste recurrente, mismo quality gate genérico.
3. **NO adoptar SonarQube; mitigar gap con ruff extendido + vulture + nightly report** — Ventaja: coste ~0, reproducible, config as code, sin infra nueva. Desventaja: requiere activar reglas ruff + vulture en CI; no da "quality gate" binario Sonar-style (pero OCM ya tiene gates binarios reales).

## Decisión

**NO adoptar SonarQube (NOT JUSTIFIED). La justificación correcta es el coste operacional en OrangeHouse, NO la duplicación con ruff (que era incorrecta).**

**Estrategia maintainability coste ~0:**
1. Activar en `pyproject.toml`: `select = ["E", "F", "I", "C901", "PLR", "SIM", "DUP"]` (non-blocking inicialmente)
2. Añadir vulture a pre-commit/CI (non-blocking → blocking) — ya instalado `vulture>=2.16`
3. Nightly compliance report con `ruff check --select C901,PLR,SIM,DUP` + `vulture` → trend histórico
4. Re-evaluar SonarQube SOLO si: (a) equipo ops dedicado, (b) PostgreSQL gestionado + backup/DR, (c) acepta superficie ataque, (d) ADR documenta trade-off

## Justificación técnica

- **Gap real confirmado**: complexity/duplication/cognitive complexity NO cubiertas por ruff E/F/I (F-PLA-01)
- **SonarQube SÍ llenaría el gap** pero coste >> valor para single-host sin ops team
- **Alternativa coste ~0 existe** y es coherente con principios OCM (config as code, reproducibilidad, automatización > disciplina)
- **No introduce deuda técnica**: ruff extendido + vulture detectan los mismos patrones que SonarQube (McCabe, cognitive complexity, duplication, dead code)

## Consecuencias

- **Más fácil:** Mantiene stack simple (single-host, sin DB, sin servidor Java)
- **Deuda aceptada:** Quality gate binario "maintainability" no existe (pero OCM tiene gates binarios reales: import-linter 0 broken, bandit 0 HIGH, mypy 0 errors, tests pass, audit_validator PASS)
- **Contratos que hacen cumplir:** `pyproject.toml` ruff select extendido, vulture en CI, nightly compliance report
- **Relación con ADR-0031/0036:** Reglas de complexity en Policy Registry con owner/evidence; vulture enforcement en CI

## Referencias

- Código: `pyproject.toml` (ruff select), `vulture` (instalado)
- Hallazgos: B-47, B-48, B-54 (tracking.yaml)
- ADRs relacionados: ADR-0031, ADR-0036
- Auditorías: `AUDIT_OCM_POLICY_LAYER_FEASIBILITY_2026-08-19.md` (F-PL-08), `AUDIT_OCM_POLICY_LAYER_COMPLEMENTARY_2026-08-19.md` (F-PLC-03/08), `AUDIT_OCM_POLICY_LAYER_COMPLEMENTARY_2026-08-19.md` (adversarial, F-PLA-01/08)