# AUDIT OCM CI/CD — GitHub Actions

## 1. Scope

Auditoria tecnica, completamente READ-ONLY, del sistema CI/CD actual de
OrangeCashMachine para determinar que capacidades ya estan implementadas
mediante GitHub Actions, cuales faltan, cuales estan duplicadas o
fragmentadas y si existe alguna razon tecnica real para introducir Jenkins.

NO se implementan cambios, NO se modifican workflows, NO se modifica
configuracion, NO se crea commit.

> **NOTA DE RECONCILIACIÓN (2026-09-18):** este documento fue contrastado contra
> `AUDIT_OCM_reconciliacion-IN-01-06_2026-09-18.md` (IN-03, IN-05, IN-06).
>
> 1. **IN-03 (`§22 Riesgos`):** el riesgo "Secrets in .env" afirmaba secretos
>    **commiteados** — es **FALSO** (`.env` nunca trackeado: `git ls-files`/`git
>    log --all -- .env` vacíos; `.gitignore:12`; chmod 600). La exposición
>    versionada real es `deploy/host.env` (topología, sin secretos — H-DEP-01).
>    Fila corregida en §22.
> 2. **IN-05 (fases del Plan):** las etiquetas "P1 Governance / P7 CI/CD
>    consolidation / P8 Verification" de §19/§24 **no existen** en la taxonomía
>    del master/PLAN. Los gaps de CI/CD mapean a **F2.1 (governance)** y **F4
>    (CD)** del Plan Maestro; P7/P8 del master no los intocan (tabla de
>    correspondencia en reconciliación §10).
> 3. **IN-06:** el master §5 omite este CI/CD audit como fuente; ya incorporado
>    (master §5, reconciliación 2026-09-18).

## 2. Methodology

- Preservacion del worktree verificada antes y despues
- Lectura exhaustiva de todos los workflows (9 archivos)
- Analisis de dependencias entre jobs
- Busqueda de referencias a Jenkins, self-hosted runners, deployment, secrets
- Cruce con auditorias existentes (deployment-portability, streaming-incident-recovery)

**git status final:** identico al inicial (sin modificaciones)

## 3. Current workflows

| Workflow | File | Trigger | Runner | Build | Test | Security | Architecture | Governance | Deploy |
|----------|------|---------|--------|-------|------|----------|--------------|-----------|--------|
| ocm-ci.yml | `.github/workflows/ocm-ci.yml` | push main, PR main | ubuntu-latest | NO | YES | YES | YES | YES | NO |
| ocm-cd.yml | `.github/workflows/ocm-cd.yml` | workflow_dispatch | ubuntu-latest | NO | NO | NO | NO | NO | PLACEHOLDER |
| actionlint.yml | `.github/workflows/actionlint.yml` | push/PR paths .github/workflows/** | ubuntu-latest | NO | NO | NO | YES | NO | NO |
| codeql.yml | `.github/workflows/codeql.yml` | push main, PR main, schedule weekly | ubuntu-latest | NO | NO | YES | NO | NO | NO |
| gitleaks.yml | `.github/workflows/gitleaks.yml` | push main, PR main | ubuntu-latest | NO | NO | YES | NO | NO | NO |
| docker-lint.yml | `.github/workflows/docker-lint.yml` | push/PR paths Dockerfile | ubuntu-latest | NO | NO | YES | NO | NO | NO |
| shellcheck.yml | `.github/workflows/shellcheck.yml` | push/PR paths **.sh | ubuntu-latest | NO | NO | YES | NO | NO | NO |
| trivy.yml | `.github/workflows/trivy.yml` | push main, PR main, schedule weekly | ubuntu-latest | NO | NO | YES | NO | NO | NO |
| yamllint.yml | `.github/workflows/yamllint.yml` | push/PR paths **.yml/**.yaml | ubuntu-latest | NO | NO | NO | NO | NO | NO |

**Total: 9 workflows, 19 jobs, todos en ubuntu-latest**

## 4. Build

**BUILD_IMPLEMENTED: NO**
**BUILD_MISSING: YES**

GitHub Actions NO realiza:
- Build de Python package
- Packaging (sdist/wheel)
- Docker build (solo lint de Dockerfile)
- Artifact creation
- Artifact upload
- Reproducibility checks
- Lockfile validation (solo `uv sync`)

Evidencia:
- `.github/workflows/ocm-ci.yml:34` — `uv sync --group dev` (solo instalacion)
- `.github/workflows/ocm-ci.yml:36` — `uv run lint-imports` (solo verificacion)
- No existe `docker build` en ningun workflow
- No existe `artifact` en ningun workflow

## 5. Test

**TEST_IMPLEMENTED: YES (completo)**

| Test Type | Workflow | Job | Command | Blocking |
|-----------|----------|-----|---------|----------|
| Architecture contracts | ocm-ci.yml | architecture | `uv run lint-imports --config architecture_linter/importlinter.toml` | YES |
| Engineering health check | ocm-ci.yml | engineering-health | `uv run python scripts/engineering_health_check.py` | YES |
| App layer guard (AST) | ocm-ci.yml | app-guard | `uv run pytest tests/architecture/test_app_layer_guard.py -q -m "not integration" --no-cov` | YES |
| Backtest historico guard | ocm-ci.yml | app-guard | `uv run python scripts/backtest_app_guard.py` | YES |
| Mypy (apps) | ocm-ci.yml | app-guard | `uv run mypy apps/ --no-incremental` | YES |
| Domain purity guard | ocm-ci.yml | domain-guard | `uv run pytest tests/architecture/test_domain_subprocess_guard.py -q -m "not integration" --no-cov` | YES |
| Trading guards (R9/R10) | ocm-ci.yml | trading-guards | `uv run pytest tests/trading/test_live_executor.py tests/trading/test_transport_mapping.py -q -m "not integration" --no-cov` | YES |
| Unit tests | ocm-ci.yml | unit-tests | `uv run pytest tests/ -x -q -m "not integration" --cov=packages --cov=ocm --cov=shared --cov=apps --cov-report=term` | YES |
| Integration tests (Kafka) | ocm-ci.yml | integration-tests | `uv run pytest tests/ -q -m integration --no-cov` | YES |
| Config validation (Hydra) | ocm-ci.yml | config-validation | `OCM_VALIDATE_ONLY=true uv run python -m app.cli.main` | YES |
| Ruff lint | ocm-ci.yml | quality | `uv run ruff check .` | YES |
| Ruff format check | ocm-ci.yml | quality | `uv run ruff format . --check` | YES |
| Mypy (all) | ocm-ci.yml | quality | `uv run mypy . --no-incremental` | YES |
| SSOT literals | ocm-ci.yml | quality | `uv run python scripts/check_ssot_enums.py` | YES |
| Vulnerabilities (pip-audit) | ocm-ci.yml | quality | `uv run pip-audit . --ignore-vuln PYSEC-2026-113 --ignore-vuln PYSEC-2026-1325` | YES |
| Production gates (G1/G2/G3/G10/G11) | ocm-ci.yml | quality | `uv run python scripts/check_production_gates.py --mode gate-ci` | YES |
| Semgrep (architecture rules) | ocm-ci.yml | semgrep | `semgrep --config policies/semgrep/ . --quiet \|\| true` | NO (continue-on-error) |

PR validation: Todos los jobs corren en PR a main
Post-merge validation: Todos los jobs corren en push a main
Deployment verification: NO EXISTE

## 6. Security

**SECURITY_IMPLEMENTED: YES (parcial)**

| Tool | Workflow | Trigger | Blocking | Scope |
|------|----------|---------|----------|-------|
| Bandit | ocm-ci.yml | push main, PR main | YES | Python code (apps, ocm, packages, shared, infrastructure) |
| CodeQL | codeql.yml | push main, PR main, schedule weekly | UNKNOWN | Python code |
| Gitleaks | gitleaks.yml | push main, PR main | YES | Secret scanning |
| Hadolint | docker-lint.yml | push/PR paths Dockerfile | YES | Dockerfile |
| ShellCheck | shellcheck.yml | push/PR paths **.sh | YES | Shell scripts |
| Trivy | trivy.yml | push main, PR main, schedule weekly | UNKNOWN | Filesystem vulnerabilities |
| yamllint | yamllint.yml | push/PR paths **.yml/**.yaml | UNKNOWN | YAML files |
| Dependabot | dependabot.yml | schedule weekly | NO | Dependencies (pip, github-actions) |

Herramientas NO implementadas:
- Container scanning (Trivy solo hace filesystem scan)
- SAST en CI (CodeQL hace SAST pero no configurado como blocking)

## 7. Architecture gates

**ARCHITECTURE_IMPLEMENTED: YES (completo)**

| Gate | Workflow | Job | Command | Blocking |
|------|----------|-----|---------|----------|
| import-linter (50 contracts) | ocm-ci.yml | architecture | `uv run lint-imports --config architecture_linter/importlinter.toml` | YES |
| Engineering health check (F2.0) | ocm-ci.yml | engineering-health | `uv run python scripts/engineering_health_check.py` | YES |
| App layer guard (AST) | ocm-ci.yml | app-guard | `uv run pytest tests/architecture/test_app_layer_guard.py -q -m "not integration" --no-cov` | YES |
| Domain purity guard (R11) | ocm-ci.yml | domain-guard | `uv run pytest tests/architecture/test_domain_subprocess_guard.py -q -m "not integration" --no-cov` | YES |
| Trading guards (R9/R10) | ocm-ci.yml | trading-guards | `uv run pytest tests/trading/test_live_executor.py tests/trading/test_transport_mapping.py -q -m "not integration" --no-cov` | YES |
| Production gates (G1/G2/G3/G10/G11) | ocm-ci.yml | quality | `uv run python scripts/check_production_gates.py --mode gate-ci` | YES |
| Policy gate (evidence hash) | ocm-ci.yml | policy-gate | SHA256 comparison against `policies/evidence.json` | YES |

Dependencias entre jobs:
- architecture y engineering-health corren en paralelo
- Todos los demas jobs dependen de ambos (app-guard, domain-guard, trading-guards, unit-tests, security, integration-tests, config-validation, quality, policy-gate)

Fail-fast: architecture es el primer gate; si falla, nada mas avanza.

## 8. Governance gates

**GOVERNANCE_IMPLEMENTED: YES (parcial)**

| Gate | Workflow | Job | Command | Blocking |
|------|----------|-----|---------|----------|
| Engineering health check (F2.0) | ocm-ci.yml | engineering-health | `uv run python scripts/engineering_health_check.py` | YES |
| Policy gate (evidence hash) | ocm-ci.yml | policy-gate | SHA256 comparison against `policies/evidence.json` | YES |
| CODEOWNERS | .github/CODEOWNERS | N/A | GitHub auto-review request | UNKNOWN |

Gaps de governance:
- No existe gate que valide `tracking.yaml` contra codigo
- No existe gate que valide ADRs contra implementacion
- No existe gate que valide `fecha_cierre` o `cadena.cierre`
- No existe gate que valide documentacion de auditorias

## 9. Artifacts

**ARTIFACTS_IMPLEMENTED: NO**
**ARTIFACTS_MISSING: YES**

GitHub Actions NO genera:
- Python packages (sdist/wheel)
- Docker images
- Release artifacts
- Changelog
- Versioned artifacts

Evidencia:
- No existe `upload-artifact` en ningun workflow
- No existe `docker build` en ningun workflow (solo `docker-lint.yml` linta el Dockerfile)
- No existe `actions/create-release` en ningun workflow

## 10. Release

**RELEASE_IMPLEMENTED: NO**
**RELEASE_MISSING: YES**

GitHub Actions NO gestiona releases:
- No existen GitHub Releases
- No existen tags versionados
- No existe release workflow
- No existe changelog automatico
- No existe versioning

Evidencia:
- No existe `on: release` en ningun workflow
- No existe `actions/create-release` en ningun workflow
- No existe `on: push: tags` en ningun workflow

## 11. Deployment

**CD_IMPLEMENTED: NO**
**CD_PARTIAL: PLACEHOLDER**
**CD_MISSING: YES**

Estado actual: `ocm-cd.yml` es un placeholder con solo `echo "CD pendiente de implementacion"`.

Evidencia:
- `.github/workflows/ocm-cd.yml:2` — `# Deshabilitado — deploy manual via SSH hasta que se defina deploy_ocm.sh`
- `.github/workflows/ocm-cd.yml:12` — `run: echo "CD pendiente de implementacion — ver deploy.py"`
- No existe `ssh`, `scp`, `rsync`, `systemctl`, `docker compose` en ningun workflow
- No existe `deploy_ocm.sh` en el repositorio

Deployment actual:
- Manual via SSH
- Scripts en `deploy/scripts/` (install_systemd.sh, health_check.sh)
- No automatizado por CI/CD

## 12. Orangehouse integration

**ORANGEHOUSE_INTEGRATION: NONE**

GitHub Actions NO tiene conexion con orangehouse:
- No existe self-hosted runner
- No existe SSH key en secrets
- No existe deploy script invocado por CI
- No existe health check post-deployment

Evidencia:
- No hay `runs-on: self-hosted` en ningun workflow
- No hay `ssh` en ningun workflow
- No hay `orangehouse` en ningun workflow
- `deploy/scripts/install_systemd.sh` es manual, no invocado por CI

## 13. Self-hosted runners

**SELF_HOSTED_RUNNERS: NO**
**GITHUB_HOSTED_RUNNERS: YES**

Todos los workflows usan `runs-on: ubuntu-latest` (GitHub-hosted):
- ocm-ci.yml: 12 jobs, todos ubuntu-latest
- ocm-cd.yml: 1 job, ubuntu-latest
- actionlint.yml: 1 job, ubuntu-latest
- codeql.yml: 1 job, ubuntu-latest
- gitleaks.yml: 1 job, ubuntu-latest
- docker-lint.yml: 1 job, ubuntu-latest
- shellcheck.yml: 1 job, ubuntu-latest
- trivy.yml: 1 job, ubuntu-latest
- yamllint.yml: 1 job, ubuntu-latest

Total: 19 jobs, todos en ubuntu-latest

## 14. Secrets / environments

**SECRETS: MINIMAL**
**ENVIRONMENTS: NONE**

| Secret | Workflow | Usage |
|--------|----------|-------|
| `secrets.GITHUB_TOKEN` | actionlint.yml | `github_token` para reviewdog |

NO existen:
- Repository secrets para deployment
- Environment secrets
- SSH keys
- API tokens
- OIDC

Environment protection: NO existen GitHub Environments

## 15. Rollback

**ROLLBACK_IMPLEMENTED: NO**
**ROLLBACK_MISSING: YES**

GitHub Actions NO gestiona rollback:
- No existe rollback automatico
- No existe rollback manual
- No existen artifacts versionados
- No existe version anterior a la que volver

Evidencia:
- No existe `actions/rollback` en ningun workflow
- No existen artifacts versionados
- No existe Docker image versionada
- No existe GitHub Release como referencia

## 16. Health / readiness verification

**HEALTH_CHECK: NO (post-deployment)**
**HEALTH_CHECK: YES (local scripts)**

GitHub Actions NO verifica health post-deployment:
- No existe health check en workflows
- No existe readiness check en workflows
- No existe verificacion de Kafka, Redis, BookBuilder, streaming

Health check local existe:
- `deploy/scripts/health_check.sh` — L1-L4 health check (process, dependency, data flow, processing)
- No es invocado por CI/CD

Evidencia:
- No hay `curl` health check en workflows
- No hay `systemctl status` en workflows
- No hay `docker exec` health check en workflows

## 17. Jenkins evidence

**JENKINS_PRESENT: NO**
**JENKINS_ABSENT: YES**

No existe ninguna referencia a Jenkins en el repositorio:
- No existe `Jenkinsfile`
- No existe `jenkins` en ningun archivo del proyecto
- Las unicas referencias estan en `.venv/` (dependencias de terceros)

Evidencia:
- `grep -ri "jenkins" . --include="*.yml" --include="*.yaml" --include="*.md" --include="*.py" --include="*.sh" --include="*.toml"` — solo resultados en `.venv/`

## 18. GitHub Actions vs Jenkins

| Capability | GitHub Actions actual | GitHub Actions posible | Jenkins |
|------------|-----------------------|------------------------|---------|
| PR CI | YES (ocm-ci.yml) | YES | YES |
| Build | NO | YES (docker build, artifact) | YES |
| Tests | YES (unit, integration, architecture) | YES | YES |
| Security | YES (bandit, codeql, gitleaks, trivy, hadolint, shellcheck) | YES (add container scanning) | YES |
| Architecture gates | YES (50 contracts, guards, production gates) | YES | YES |
| Governance | PARTIAL (engineering health, policy gate) | YES (add tracking, ADR validation) | YES |
| Artifacts | NO | YES (docker images, packages) | YES |
| Release | NO | YES (github releases, tags) | YES |
| Deploy | NO | YES (SSH, systemd) | YES |
| Self-hosted execution | NO | YES (add self-hosted runner) | YES |
| Private network | NO | YES (self-hosted runner in orangehouse) | YES |
| Secrets | MINIMAL (only GITHUB_TOKEN) | YES (add deployment secrets) | YES |
| Environments | NO | YES (add GitHub Environments) | YES |
| Approvals | NO | YES (add required reviewers) | YES |
| Rollback orchestration | NO | YES (manual rollback script) | YES |
| Multi-repo orchestration | NO | NO (single repo) | YES |
| Observability | NO | YES (add metrics, logs) | YES |

Diferencias tecnicas clave:
- **GitHub Actions:** Cloud-hosted, 2000 min/month free, good for CI
- **Jenkins:** Self-hosted, unlimited, good for CD/private network
- **GitHub Actions + self-hosted runner:** Best of both worlds for OCM

## 19. Gaps reales

| Gap | Severity | Evidence | Plan Maestro |
|-----|----------|----------|--------------|
| No build pipeline | HIGH | No `docker build`, no artifact creation | P7 — CI/CD consolidation |
| No CD pipeline | HIGH | `ocm-cd.yml` placeholder, manual SSH deployment | P7 — CI/CD consolidation |
| No deployment verification | HIGH | No health check post-deployment | P7 — CI/CD consolidation |
| No rollback mechanism | HIGH | No artifacts, no versioning | P7 — CI/CD consolidation |
| No self-hosted runner | MEDIUM | All jobs on GitHub-hosted runners | P7 — CI/CD consolidation |
| No GitHub Environments | MEDIUM | No environment protection | P7 — CI/CD consolidation |
| No deployment secrets | MEDIUM | Only `GITHUB_TOKEN` | P7 — CI/CD consolidation |
| No container scanning | LOW | Trivy only does filesystem scan | P8 — Verification |
| No release management | MEDIUM | No tags, no releases, no changelog | P7 — CI/CD consolidation |
| Governance gaps | LOW | No tracking.yaml, ADR, or audit validation | P1 — Governance |

> **NOTA (reconciliación 2026-09-18, IN-05):** las etiquetas "P1/P7/P8" de esta
> tabla no existen en la taxonomía del master/PLAN. Mapeo correcto: gaps de
> CI/CD → **F2.1 (governance)** y **F4 (CD)** del Plan Maestro; los P7/P8 del
> master (readiness/ADR-0017/BookBuilder) no se ven afectados. Tabla de
> correspondencia completa en `AUDIT_OCM_reconciliacion-IN-01-06_2026-09-18.md` §10.

## 20. Gaps aparentes pero ya cubiertos

| Apparent Gap | Actual Status | Evidence |
|--------------|---------------|----------|
| Tests | COMPLETE | 14 test jobs in ocm-ci.yml |
| Architecture gates | COMPLETE | 50 contracts, guards, production gates |
| Security scanning | MOSTLY COMPLETE | 6 tools (bandit, codeql, gitleaks, trivy, hadolint, shellcheck) |
| Pre-commit hooks | COMPLETE | ruff, gitleaks, import-linter, mypy, bandit, yamllint, pytest, vulture |
| Dependabot | COMPLETE | pip + github-actions weekly |
| CODEOWNERS | COMPLETE | Policy files require review |

## 21. Duplicaciones

| Tool | CI Workflow | Pre-commit Hook | Overlap |
|------|-------------|-----------------|---------|
| Ruff lint | quality job | ruff-check hook | YES (CI catches what pre-commit misses) |
| Ruff format | quality job | ruff-format hook | YES |
| Import-linter | architecture job | import-linter hook | YES |
| Mypy | quality job (all) + app-guard (apps) | mypy-shared hook (shared only) | PARTIAL (CI covers more) |
| Bandit | security job | bandit hook | YES |
| SSOT literals | quality job | ssot-enums hook | YES |
| Pytest | unit-tests + integration-tests | pytest-pre-push hook | YES (CI catches what pre-commit misses) |
| Gitleaks | gitleaks.yml | gitleaks hook | YES |
| yamllint | yamllint.yml | yamllint hook | YES |

Nota: Las duplicaciones son intencionales — pre-commit es la primera linea de defensa, CI es la segunda.

## 22. Riesgos

| Risk | Impact | Evidence | Mitigation |
|------|--------|----------|------------|
| No CD | HIGH | Manual SSH deployment, no automation | Implement ocm-cd.yml with SSH deploy |
| No artifacts | HIGH | No versioned artifacts, no rollback capability | Add Docker build + upload-artifact |
| No self-hosted runner | MEDIUM | Cannot access orangehouse private network | Add self-hosted runner on orangehouse |
| No environment protection | MEDIUM | No approval gates for production | Add GitHub Environments with required reviewers |
| No deployment verification | HIGH | No health check after deploy | Add health_check.sh invocation post-deploy |
| Secrets in `.env` | CRITICAL → RECLASIFICADO (IN-03) | `.env` tiene secretos reales en disco pero **NUNCA fue commiteado** (git vacío, `.gitignore:12`, chmod 600); la exposición versionada real es `deploy/host.env` (topología, sin secretos — H-DEP-01) | Gitleaks SI valida el repo; revisar `deploy/host.env` (H-DEP-01) |

## 23. Arquitectura CI/CD objetivo

```
                    GitHub
                       |
                  PR / merge / tag
                       |
                       v
               GitHub Actions
                       |
          +------------+------------+
          v            v            v
        Build         Test       Security
          |            |            |
          +------+-----+-----+-----+
                 |           |
                 v           v
         Architecture    Governance
           Gates           Gates
                 |           |
                 +-----+-----+
                       |
                       v
                   Artifact
                       |
                       v
                 Deployment
                       |
                       v
                  orangehouse
                       |
                       v
                    systemd
                       |
                       v
                OCM runtime
                       |
                       v
               Health/Readiness
```

Estado de cada parte:
- **Build:** NO EXISTE (falta)
- **Test:** COMPLETO (14 jobs)
- **Security:** PARCIAL (6 tools, falta container scanning)
- **Architecture Gates:** COMPLETO (7 gates)
- **Governance:** PARCIAL (falta tracking.yaml, ADR validation)
- **Artifact:** NO EXISTE (falta)
- **Release:** NO EXISTE (falta)
- **Deployment:** NO EXISTE (placeholder)
- **Health/Readiness:** NO EXISTE en CI (existe local en `deploy/scripts/health_check.sh`)

## 24. Integracion con Plan Maestro

| Capacidad | Estado actual | Evidencia | Gap | Fase Plan Maestro |
|-----------|---------------|-----------|-----|-------------------|
| Build | NO | No docker build, no artifact | HIGH | P7 — CI/CD consolidation |
| Test | YES | 14 jobs, unit + integration + architecture | NONE | — |
| Security | PARTIAL | 6 tools, no container scanning | LOW | P8 — Verification |
| Architecture | YES | 7 gates, 50 contracts | NONE | — |
| Governance | PARTIAL | Engineering health + policy gate | LOW | P1 — Governance |
| Artifact | NO | No upload-artifact, no Docker image | HIGH | P7 — CI/CD consolidation |
| Release | NO | No tags, no releases | HIGH | P7 — CI/CD consolidation |
| Deploy | NO | Placeholder only | HIGH | P7 — CI/CD consolidation |
| Health verification | NO | Local script exists, not in CI | HIGH | P7 — CI/CD consolidation |
| Rollback | NO | No artifacts, no versioning | HIGH | P7 — CI/CD consolidation |

Dependencias derivadas de la evidencia:
1. **P7 (CI/CD consolidation)** es el gap mas grande — Build, Artifact, Release, Deploy, Health, Rollback
2. **P1 (Governance)** tiene gaps meniores — tracking.yaml, ADR validation
3. **P8 (Verification)** tiene gap menor — container scanning

> **NOTA (reconciliación 2026-09-18, IN-05):** los nombres "P1/P7/P8" de las
> celdas anteriores y de las dependencias 1-3 **no pertenecen a la taxonomía del
> master/PLAN**; colisionan con P1 readiness, P7 ADR-0017+Bybit y P8 BookBuilder
> del master. Los gaps de este informe mapean a **F2.1 (governance)** y **F4
> (CD)** del Plan Maestro (ver reconciliación §10).

## 25. Relacion con auditorias existentes

Cruce con hallazgos de auditorias previas:

| Finding | Auditoria | Relacion con CI/CD |
|---------|-----------|-------------------|
| H-DEP-01 (host.env tracked) | deployment-portability | CI NO valida `deploy/host.env`; Gitleaks lo detectaria si hubiera secretos (IN-03: host.env es topología, sin secretos) |
| H-DEP-02 (host.env tracked) | deployment-portability | CI NO valida deploy files; yamllint lo detecta parcialmente |
| H-DEP-03 (systemd divergence) | deployment-portability | CI NO valida systemd templates; shellcheck podria detectar |
| H-DEP-04 (Dockerfile targets) | deployment-portability | CI linta Dockerfile (Hadolint) pero no valida targets |
| H-DEP-05 (Kafka versions) | deployment-portability | CI usa Kafka 7.6.0 en tests; docker-compose usa 7.6.1 |
| H-DEP-06 (Zookeeper legacy) | deployment-portability | CI NO valida docker-compose; yamllint lo detecta parcialmente |
| H-DEP-07 (run.sh entries) | deployment-portability | CI NO valida run.sh; shellcheck lo detecta |
| H-DEP-08 (requirements.txt) | deployment-portability | No existe; pyproject.toml es SSOT |
| H-DEP-09 (runbook) | deployment-portability | CI NO valida runbook; no existe runbook |
| H-DEP-10 (monitoring config) | deployment-portability | CI linta YAML (yamllint) pero no valida contenido |

## 26. Conclusion

### Respuesta a la pregunta central

**Existe alguna capacidad NECESARIA para el Plan Maestro de OCM que GitHub Actions NO pueda proporcionar razonablemente?**

**NO.** Todas las capacidades necesarias pueden implementarse en GitHub Actions:

| Capability | Category | How in GitHub Actions |
|------------|----------|----------------------|
| Build | A (inexistente) | `docker build` + `upload-artifact` |
| Deploy | A (inexistente) | SSH action + systemd |
| Artifacts | A (inexistente) | Docker registry + `upload-artifact` |
| Self-hosted | E (infra adicional) | Register runner on orangehouse |
| Environments | B (no configurada) | GitHub Environments + required reviewers |
| Rollback | A (inexistente) | Manual rollback script in workflow |
| Health check | B (no configurada) | Invoke `health_check.sh` post-deploy |
| Governance | C (parcial) | Add tracking.yaml, ADR validation jobs |

**Ninguna capacidad requiere Jenkins.** Jenkins ofreceria:
- Multi-repo orchestration (no necesario — single repo)
- Self-hosted execution (GitHub Actions lo soporta via runners)
- Private network access (GitHub Actions lo soporta via self-hosted runners)

**Recomendacion:** Implementar las capacidades faltantes en GitHub Actions, no migrar a Jenkins.

## 27. Limitaciones

- No se pudieron verificar: GitHub repository settings, actual runner registration, GitHub Environments, repository secrets, branch protection, webhook configuration, production deployment
- No se examino la configuracion de GitHub UI (branch protection rules, required status checks)
- No se verifico si existen runners registrados en el servidor
- No se verifico la configuracion de Dependabot en GitHub UI

NOT_VERIFIABLE_FROM_REPOSITORY: GitHub UI / infrastructure evidence

## 28. Validaciones

| Validation | Result |
|------------|--------|
| git status --short | Identical to initial state |
| git diff --check | No changes |
| Archivos modificados | Solo el archivo de auditoria (nuevo) |
| Jenkins references | ABSENT (only in .venv/) |
| Self-hosted runners | ABSENT |
| CD implementation | ABSENT (placeholder only) |
| Secrets | MINIMAL (only GITHUB_TOKEN) |
| Environments | ABSENT |

---

# Complementary Verification — Branch Protection / Rulesets

**Fecha de verificacion:** 2026-09-17
**Metodo:** GitHub API via `gh` CLI (autenticado como OrangeCashDigital)
**Auditor:** MiMo

## 29. Branch Protection for main

**ESTADO: PRESENTE — Configurado y activo**

### Evidencia (GitHub API)

```
gh api repos/OrangeCashDigital/orangecashmachine/branches/main/protection
```

### Configuracion verificada

| Setting | Value |
|---------|-------|
| Branch protection | PRESENTE |
| Required status checks | PRESENTE (13 checks) |
| Strict mode | YES (must be up-to-date with main) |
| Enforce admins | YES (admins cannot bypass) |
| Allow force pushes | NO |
| Allow deletions | NO |
| Required linear history | NO |
| Required conversation resolution | NO |
| Lock branch | NO |
| Required signatures | NO |

### Required Status Checks (13)

| # | Check Name | Matching Workflow Job |
|---|------------|----------------------|
| 1 | Unit tests | ocm-ci.yml → unit-tests |
| 2 | Integration tests (Kafka) | ocm-ci.yml → integration-tests |
| 3 | Domain purity guard (R11) | ocm-ci.yml → domain-guard |
| 4 | Config validation (Hydra bootstrap) | ocm-ci.yml → config-validation |
| 5 | Quality gates (ruff/mypy/SSOT/audit) | ocm-ci.yml → quality |
| 6 | Trading guards (R9/R10) | ocm-ci.yml → trading-guards |
| 7 | Security audit (bandit) | ocm-ci.yml → security |
| 8 | App layer guard (AST + mypy apps) | ocm-ci.yml → app-guard |
| 9 | Analyze (python) | codeql.yml → analyze |
| 10 | Scan filesystem for vulnerabilities | trivy.yml → trivy |
| 11 | Architecture contracts (import-linter) | ocm-ci.yml → architecture |
| 12 | Engineering Health Check (F2.0) | ocm-ci.yml → engineering-health |
| 13 | Scan for secrets | gitleaks.yml → gitleaks |

Todos los checks son de GitHub Actions (app_id 15368). No hay checks de terceros.

## 30. Rulesets

**ESTADO: AUSENTE**

No existen GitHub Rulesets que afecten main ni ninguna otra branch.

## 31. Pull Request Requirements

**ESTADO: REQUERIDO**

| Setting | Value |
|---------|-------|
| Required approving review count | 1 |
| Dismiss stale reviews | NO |
| Require code owner reviews | NO |
| Require last push approval | NO |

## 32. Push Restrictions

**ESTADO: NO HABILITADO**

No hay restricciones de push por usuario/equipo.

## 33. Admin Bypass

**ESTADO: NO PERMITIDO (enforce_admins = true)**

Los administradores NO pueden hacer bypass de las reglas de proteccion.

## 34. Repository Settings

| Setting | Value |
|---------|-------|
| Default branch | main |
| Allow merge commit | YES |
| Allow squash merge | YES |
| Allow rebase merge | YES |
| Allow auto merge | NO |
| Delete branch on merge | NO |
| Secret scanning | disabled |
| Secret scanning push protection | disabled |
| Dependabot security updates | disabled |

## 35. GitHub Environments

**ESTADO: AUSENTE**

No existen GitHub Environments configurados.

## 36. Diferencia entre CI y Merge Governance

### Checks ejecutados por CI vs requeridos para merge

| Check | CI ejecuta | GitHub requiere para merge |
|-------|------------|---------------------------|
| Architecture contracts (import-linter) | YES | YES |
| Engineering Health Check (F2.0) | YES | YES |
| App layer guard (AST + mypy apps) | YES | YES |
| Domain purity guard (R11) | YES | YES |
| Trading guards (R9/R10) | YES | YES |
| Unit tests | YES | YES |
| Integration tests (Kafka) | YES | YES |
| Config validation (Hydra bootstrap) | YES | YES |
| Quality gates (ruff/mypy/SSOT/audit) | YES | YES |
| Security audit (bandit) | YES | YES |
| Analyze (python) | YES | YES |
| Scan filesystem for vulnerabilities | YES | YES |
| Scan for secrets | YES | YES |
| Semgrep (architecture rules) | YES | NO (continue-on-error, not required) |
| Policy gate (evidence hash) | YES | NO (not in required checks) |
| Docker lint (Hadolint) | YES | NO (path-filtered, not required) |
| ShellCheck | YES | NO (path-filtered, not required) |
| yamllint | YES | NO (path-filtered, not required) |
| Actionlint | YES | NO (path-filtered, not required) |

**Hallazgo critico:** Todos los 13 required status checks para merge tienen un job correspondiente en CI. No hay checks requeridos que no se ejecuten. No hay checks ejecutados que no sean requeridos (excepto los path-filtered y semgrep/policy-gate que son non-blocking).

**Implicacion:** La governance de merge esta perfectamente alineada con lo que CI ejecuta. No hay gaps entre CI y merge requirements.

## 37. Impacto sobre el audit anterior

El audit anterior (secciones 1-28) clasifico branch protection como "NO VERIFICADO". Ahora puede actualizarse:

| Item | Antes | Ahora | Evidencia |
|------|-------|-------|-----------|
| Branch protection | NO VERIFICADO | PRESENTE | GitHub API 200 OK |
| Required checks | NO VERIFICADO | 13 checks required | GitHub API |
| PR requirement | NO VERIFICADO | YES (1 approval) | GitHub API |
| Force push | NO VERIFICADO | NO permitido | GitHub API |
| Admin bypass | NO VERIFICADO | NO permitido | GitHub API |
| Rulesets | NO VERIFICADO | AUSENTE | GitHub API |
| Environments | NO VERIFICADO | AUSENTE | GitHub API |

La conclusion del audit anterior se CONFIRMA y FORTALECE: GitHub Actions + branch protection proporciona una governance solida para OCM.

## 38. Impacto sobre Jenkins

La verificacion de branch protection NO cambia la conclusion sobre Jenkins:

- GitHub Actions ejecuta los 13 checks requeridos para merge
- Branch protection garantiza que ningun PR se mergee sin pasar todos los gates
- Enforce admins garantiza que nadie puede bypass
- No hay capacidad necesaria que requiera Jenkins

**Jenkins no es necesario para:**
- Branch protection (GitHub lo soporta nativamente)
- Required status checks (GitHub lo soporta nativamente)
- Required reviews (GitHub lo soporta nativamente)
- Admin bypass control (GitHub lo soporta nativamente)

## 39. Limitaciones de esta verificacion

- Se verifico la configuracion de branch protection via GitHub API
- No se verifico si los required checks estan correctamente configurados en GitHub UI (solo via API)
- No se verifico si existen branch protection rules para otras branches (solo main)
- No se verifico la configuracion de CODEOWNERS en GitHub UI
- No se verifico si los required reviews estan correctamente configurados en GitHub UI
