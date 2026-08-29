# OCM — AUDIT FINDINGS REGISTER — Policy Layer Adversarial Validation

**Ejecución de auditoría adversarial:** 2026-08-19 (baseline `a4d82983f629ef933a155ee7863ab5b2d3a56ae9`, branch `main`)
**Fuente primaria:** `docs/audits/AUDIT_OCM_POLICY_LAYER_COMPLEMENTARY_2026-08-19.md`
**Alcance:** segunda opinión adversarial sobre la auditoría de viabilidad previa (2026-08-19) y la OCM Constitution, con reevaluación obligatoria de Semgrep/SonarQube contra evidencia real del repositorio.
**Estado de este registro:** OPEN

Resumen: CRITICAL 1 · HIGH 3 · MEDIUM 4 · LOW 1 · INFO 0 · **total 9**.

Clasificación (taxonomía del protocolo de auditoría de OCM):
- NUEVO: 4 — F-PLA-01, F-PLA-02, F-PLA-05, F-PLA-09
- REVALIDADO: 2 — F-PLA-04, F-PLA-06
- CONTRADICCIÓN: 2 — F-PLA-03, F-PLA-08
- RECOMENDACIÓN: 1 — F-PLA-07
- REGRESIÓN: 0
- CERRADO: 0
- NO_VERIFICADO: 0

Deduplicación (regla §H):
- F-PLA-01 es NUEVO: **el hallazgo de que ruff solo habilita E/F/I** (no C901 complexity, PLR, SIM, DUP) contradice la afirmación de F-PL-08 de que "SonarQube duplicaría ruff (complexity/duplication ya cubiertos)".
- F-PLA-02 es NUEVO: vulture instalado pero nunca ejecutado en CI/pre-commit (dead code detection no enforced).
- F-PLA-03 es SIN CONTRADICCIÓN con F-PL-08: CodeQL se ejecuta en push/PR + weekly.
- F-PLA-04 REVALIDA F-PL-07 (HashiCorp: confirmado no introducir, sin necesidad demostrable).
- F-PLA-05 es NUEVO: check_production_gates.py ausente (ya señalado F-PL-04, aquí se extiende al Policy Gate completo).
- F-PLA-06 REVALIDA F-PL-02 (pip-audit 4 vulns activas).
- F-PLA-07 RECOMENDACIÓN: Semgrep como non-blocking inicial (coste ~0, sin gap material de seguridad pero valor arquitectónico).
- F-PLA-08 CONTRADICCIÓN con F-PL-08 (SonarQube): **SonarQube sí aportaría una señal longitudinal de maintainability** que ruff/mypy/pytest no proveen con la configuración actual — pero su coste operacional en OrangeHouse lo hace NO JUSTIFIED igualmente.
- F-PLA-09 es NUEVO: la cadena RULE→CI→EVIDENCE no está completa para ninguna regla (falta hash de evidencia + waiver + expiración + ownership).

---

## F-PLA-01 — Ruff solo habilita E/F/I: complejidad, duplicación y cognitive complexity NO cubiertas

Severity: HIGH
Status: OPEN
Classification: NUEVO
Control: Quality Tooling
Source: pyproject.toml `[tool.ruff.lint]`

Evidence:
- `pyproject.toml:189` → `[tool.ruff.lint]` `select = ["E", "F", "I"]`, `ignore = []` — **solo errores, pyflakes e imports**
- `uv run ruff check /tmp/opencode/test_c901.py --select E,F,I,C901` → "All checks passed!" (la función con complejidad ciclomática >10 NO se detecta con las reglas activas; archivo temporal de prueba en /tmp, no existe en el repo)
- `uv run ruff check /tmp/opencode/test_c901.py` (reglas por defecto) → "All checks passed!" (mismo archivo temporal de /tmp; no existe en el repo)
- NO hay C901 (mccabe/complexity), PLR (refactor), SIM (simplify), DUP (duplication), ANN (annotations), TID, PT en `select`
- F-PL-08 (auditoría previa) afirmó: "SonarQube duplicaría ruff+mypy+pytest (complexity/duplication/coverage ya cubiertos)" — **FALSO para complexity/duplication con la config actual**

Impact:
- La afirmación central de F-PL-08 para descartar SonarQube se basa en cobertura de complexity/duplication que **no existe** en la configuración real de ruff.
- El gap de maintainability (complexity, cognitive complexity, duplication, long methods, coupling) está **NOT COVERED** hoy por ninguna herramienta ejecutada.
- Esto NO implica adoptar SonarQube (coste operacional alto, F-PLA-08), pero sí que el gap es real y debe cerrarse (activando reglas C901/PLR/SIM en ruff es la alternativa de coste ~0).

Required human decision:
- D-PLA-01: ¿Activar reglas de maintainability en ruff (C901, PLR091*, SIM, PLW) como gate, o aceptar el gap y evaluar SonarQube/alternativa?

Recommended remediation:
- RECOMMENDATION ONLY (no implementar en auditoría): activar `C901` + subconjunto `PLR` + `SIM` en `select` de ruff; `mccabe` max-complexity configurable; calibrar con `# noqa` o per-file-ignores para no romper el baseline inmediato.

Verification required:
- `uv run ruff check . --select C901,PLR0915,PLR0912` → inventario de violaciones; decidir baseline/umbral.

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: pyproject.toml · Tests: NOT_TRACED · CI: quality job · Evidence: E-PLA-01 · Closure: OPEN

---

## F-PLA-02 — vulture instalado pero nunca ejecutado (dead code detection no enforced)

Severity: LOW
Status: OPEN
Classification: NUEVO
Control: Quality Tooling
Source: pyproject.toml + CI + pre-commit

Evidence:
- `pyproject.toml:188` → `"vulture>=2.16"` en dependency-groups.dev
- `vulture --version` → 2.16 instalado
- `.pre-commit-config.yaml` → 9 hooks (ruff, gitleaks, import-linter, mypy-shared, ssot-enums, bandit, readme-size-guard, pytest-pre-push) — **vulture NO está**
- `.github/workflows/ocm-ci.yml` → 10 jobs — **vulture NO está en ningún job**
- `scripts/check_ssot_enums.py` y `scripts/engineering_health_check.py` (ambos en `scripts/`) → no invocan vulture

Impact:
- Dead code (unreachable, unused functions/classes/imports) no se detecta automáticamente. Crecimiento de superficie de mantenimiento sin señal.
- Gap de maintainability adicional a F-PLA-01: vulture resolvería parte del "dead code" de SonarQube a coste ~0 (ya instalado).

Required human decision:
- D-PLA-02: ¿Añadir vulture al pre-commit/CI (gate no-blocking inicial, luego blocking) o eliminar la dependencia no usada?

Recommended remediation:
- RECOMMENDATION ONLY: hook pre-commit `vulture` con `--min-confidence 100` (solo muertes seguras) non-blocking; calibrar baseline.

Verification required:
- `uv run vulture . --min-confidence 100` → inventario de dead code.

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: pyproject.toml · Tests: NOT_TRACED · CI: NOT_TRACED · Evidence: E-PLA-02 · Closure: OPEN

---

## F-PLA-03 — CodeQL se ejecuta en PR (no solo semanal): corrección a F-PL-08

Severity: MEDIUM
Status: OPEN
Classification: CONTRADICCIÓN
Control: Security Tooling
Source: .github/workflows/codeql.yml

Evidence:
- `.github/workflows/codeql.yml` triggers: `push: branches: ["main"]` + `pull_request: branches: ["main"]` + `schedule: cron '23 4 * * 1'`
- Es decir: CodeQL corre en **cada push/PR a main** Y push/PR + weekly — no solo semanal como afirmó la auditoría previa ("CodeQL semanal", F-PL-08)
- build-mode: none (sin compilación) → análisis de dataflow disponible en PR
- Trivy: `.github/workflows/trivy.yml` triggers push + PR + schedule semanal — igualmente en PR

Impact:
- La cobertura real de CodeQL/Trivy es **mejor** de lo que F-PL-08 declaró. Esto refuerza el argumento de que Semgrep no es necesario para seguridad (CodeQL en PR + dataflow).
- Corrección factual de la auditoría previa: no contradice la conclusión final, pero corrige la evidencia.

Required human decision:
- D-PLA-03: Ninguna acción (corrección documental); confirmar que CodeQL/Trivy en PR es la decisión correcta de coste.

Recommended remediation:
- Mantener CodeQL/Trivy en PR + weekly. No mover a solo-nightly (evidencia: ya en PR).

Verification required:
- `git log --oneline -1 -- .github/workflows/codeql.yml` → confirmar triggers.

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: .github/workflows/codeql.yml, trivy.yml · Tests: NOT_TRACED · CI: codeql, trivy jobs · Evidence: E-PLA-03 · Closure: OPEN

---

## F-PLA-04 — HashiCorp: confirmado no introducir (sin necesidad demostrable)

Severity: MEDIUM
Status: OPEN
Classification: REVALIDADO
Control: Infrastructure
Source: FASE 14 del informe complementario

Evidence:
- Infra real: Docker Compose single-host (621 líneas, 10+ servicios), sin VMs, sin self-hosted runners, sin multi-host
- 0 matches Terraform/Vault/Consul/Nomad/Packer/Boundary en repo
- F2.6d (CERRADA): proceso único systemd + Kafka local suficiente; canary 30min CPU 0.00% / RAM 40.4MB
- Secretos: `.env` gitignored + SecretStr Pydantic + fail-fast `${VAR:?}` + `.dockerignore` (verificado)
- `alertmanager.yml:11` → "Reabrir cuando exista un secret manager" (deuda documentada, no bloqueante)
- **No hay necesidad demostrable** de Terraform (IaC = compose declarativo), Vault (no rotación, no multi-host, no cluster), Consul (DNS Docker basta), Nomad (compose+systemd satisfacen), Packer (imagen Docker reproducible), Boundary (sin superficie admin remota)

Impact:
- F-PL-07 se confirma (REVALIDADO). Introducir cualquier herramienta HashiCorp violaría no-sobrediseño y F2.6d.

Required human decision:
- D-PLA-04: Reafirmar no-introducir HashiCorp. Re-evaluar Vault solo si OCM opera live con capital real (rotación de secrets) — NO es decisión de esta auditoría.

Recommended remediation:
- Ninguna (decisión de no-cambio).

Verification required:
- Ninguna.

Traceability:
- Tracking: NOT_TRACED · ADR: ADR-0022, ADR-0024 · Implementation: docker-compose.yml · Tests: NOT_TRACED · CI: NOT_TRACED · Evidence: E-PLA-04 · Closure: OPEN

---

## F-PLA-05 — Policy Gate binario inexistente: check_production_gates.py ausente (extensión de F-PL-04)

Severity: HIGH
Status: OPEN
Classification: NUEVO
Control: Policy Gate
Source: FASE 11 del informe complementario

Evidence:
- `scripts/check_production_gates.py` **no existe** (ls scripts/ → 8 scripts, ninguno)
- Plan Maestro §1/§6/§10 lo cita como mecanismo existente → F-PL-04 (CONTRADICCIÓN) ya lo señaló
- Esta auditoría extiende: el **Policy Gate** binario (PASS/BLOCK con lista de reglas que provocaron el resultado) no tiene implementación ni diseño
- Los gates actuales están distribuidos en 10 jobs CI separados (import-linter, bandit, mypy, pytest, ruff, ssot, config-validation, engineering-health) sin un **veredicto agregado normativo** que un agente de IA pueda consumir
- `scripts/engineering_health_check.py` valida coherencia Plan↔tracker↔ADR↔contratos↔CI (F2.0) pero NO produce un veredicto de production-readiness G1..G11

Impact:
- Sin Policy Gate binario, la OCM Constitution no tiene su punto de enforcement central: "POLICY GATE → CI → BLOCK/PASS". Un agente de IA no puede demostrar conformidad con un comando.
- El artifact SHA/digest no tiene qué validar agregadamente.

Required human decision:
- D-PLA-05: ¿Implementar `scripts/check_production_gates.py` (G1..G11 binario sobre evidencia mecánica) como primer gate del Policy Registry, o declarar engineering_health + jobs CI como el gate vigente?

Recommended remediation:
- RECOMMENDATION ONLY: implementar como parte del Policy Registry (F-PL-06/F-PLC-05), veredicto PASS/BLOCK + lista de reglas violadas + evidencia por regla.

Verification required:
- `scripts/check_production_gates.py` → PASS/FAIL binario sobre los 11 checks.

Traceability:
- Tracking: NOT_TRACED · ADR: ADR-0020 · Implementation: scripts/ (ausente) · Tests: NOT_TRACED · CI: NOT_TRACED · Evidence: E-PLA-05 · Closure: OPEN

---

## F-PLA-06 — pip-audit: 4 vulnerabilidades activas (revalidación F-PL-02)

Severity: CRITICAL
Status: OPEN
Classification: REVALIDADO
Control: Dependency Security
Source: ocm-ci.yml / quality job

Evidence:
- `uv run pip-audit . --ignore-vuln PYSEC-2026-113 --ignore-vuln PYSEC-2026-1325` → exit 1
- aiohttp 3.14.1 → PYSEC-2026-3545/3546/3547 (fix ≥3.14.2/3.14.3); cryptography 49.0.0 → PYSEC-2026-3552 (fix 50.0.0)
- Los 2 ignores del risk-accept (2026-08-03) no cubren estas 4
- Ya registrado: F-CI-01 (2026-08-18), F-PL-02 (2026-08-19) — misma causa raíz

Impact:
- Gate de seguridad CI rojo; merge bloqueado; aiohttp (HTTP) y cryptography (crypto) sin mitigar.

Required human decision:
- D-PLA-06: bump aiohttp ≥3.14.3 y cryptography ≥50.0.0 con validación staging, o risk-accept formal (ADR).

Recommended remediation:
- Actualizar dependencias y validar; prohibido ampliar ignore-list sin aprobación.

Verification required:
- `uv run pip-audit . --ignore-vuln PYSEC-2026-113 --ignore-vuln PYSEC-2026-1325` → exit 0.

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: pyproject.toml · Tests: NOT_TRACED · CI: quality job · Evidence: E-PLA-06, E-PL-02, E-012 · Closure: OPEN

---

## F-PLA-07 — Semgrep: NO ADOPT como blocking; valor arquitectónico real pero sin gap material de seguridad

Severity: MEDIUM
Status: OPEN
Classification: RECOMENDACIÓN
Control: Security Tooling
Source: FASE 3 del informe complementario

Evidence:
- **Patrones peligrosos reales del repo** (busca exhaustiva):
  - `eval(`/`exec(`: **0 ocurrencias** (apps, ocm, packages, shared)
  - `pickle.`/`yaml.load`/`shelve`/`marshal`: **0 ocurrencias**
  - `shell=True`: **0 ocurrencias**
  - `subprocess.run`: solo en `ocm/runtime/lineage.py:60` y `shared/utils/repo.py:50` — ambos **infraestructura** (git rev-parse / repo root), no domain
  - `os.environ`/`os.getenv`: en `ocm/config/*` (SSOT env_vars) + infrastructure + apps/research — **0 en domain/application**
  - `random.*`: solo jitter/backoff en adapters (`ohlcv_fetcher.py:414`, `ccxt_adapter.py:773`) — legítimo no-crypto; `hashlib.sha256` para config hashing (legítimo)
  - `sqlite3`: en `ocm/runtime/registry.py` (B608 nosec, queries parametrizadas, verificado) + `infrastructure/lineage/tracker.py` — **0 en domain**
  - `requests`/`httpx`/`aiohttp`: en adapters (aiohttp.ClientSession) + observability (httpx.Client) — **capas correctas**
  - Logging de secrets: `logger.*api_key/secret/password` → 0; cursor_store loguea "key" (cursor keys, no secrets); processors.py:55 redacta api_key
- **Conclusión**: NO EVIDENCE OF MATERIAL GAP de seguridad. Bandit (0 Med/High) + CodeQL (PR, dataflow) + Gitleaks (PR) + Trivy (PR+weekly) cubren la superficie real.
- **Valor arquitectónico potencial** de Semgrep: patrones de uso (os.environ en domain, logging secrets, crypto inseguro) expresables en YAML declarativo, complementando AST Guards (hardcodeados en Python). Pero **el repo no tiene hoy los patrones prohibidos** — el valor es preventivo, no correctivo.

Impact:
- Semgrep aporta **valor preventivo** (reglas declarativas para invariantes arquitectónicas) pero no resuelve un gap material actual. El coste es ~0 (CLI, sin servidor, ~500ms/PR).
- Recomendación: ADOPT COMO NON-BLOCKING inicialmente (informational en PR, baselined), evaluando reglas R11..R16 migradas a YAML. NO blocking hasta tener baseline estable.

Required human decision:
- D-PLA-07: ¿Adoptar Semgrep non-blocking (rules en policies/semgrep/, job informational en PR) o posponer hasta que exista un patrón real que detecte?

Recommended remediation:
- RECOMMENDATION ONLY: si se adopta, empezar non-blocking con `--baseline` y reglas propias de arquitectura (no el ruleset default de seguridad, ya cubierto por Bandit/CodeQL).

Verification required:
- `semgrep --config=policies/semgrep/ --baseline <main> .` → 0 violaciones sobre baseline; CI job informational.

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: NOT_TRACED · Tests: NOT_TRACED · CI: NOT_TRACED · Evidence: E-PLA-07 · Closure: OPEN

---

## F-PLA-08 — SonarQube: aportaría señal longitudinal de maintainability, pero coste operacional lo hace NO JUSTIFIED

Severity: MEDIUM
Status: OPEN
Classification: CONTRADICCIÓN
Control: Quality Tooling
Source: FASE 4 del informe complementario

Evidence:
- **Contradicción con F-PL-08**: la afirmación "SonarQube duplicaría ruff (complexity/duplication ya cubiertos)" es **falsa** — ruff solo E/F/I (F-PLA-01). SonarQube SÍ aportaría:
  - **Complexity/cognitive complexity** (NOT COVERED hoy — F-PLA-01)
  - **Duplication** (NOT COVERED — ruff sin DUP)
  - **Long methods / excessive coupling / code smells** (NOT COVERED)
  - **Trend histórico de maintainability** (señal longitudinal que ni ruff/mypy/pytest ni Git+audit_validator proveen de forma agregada)
  - **Quality Gate / Quality Profile** (si se configura con umbrales OCM, no el default)
- **Sin embargo, el coste operacional real en OrangeHouse lo hace NO JUSTIFIED**:
  - Requiere **PostgreSQL** (no existe en docker-compose.yml)
  - ~1-2 GB RAM + 1 CPU adicionales en single-host
  - **Backup/DR**: no existe (F-PL-10: ni Grafana tiene) → SonarQube añade estado que requiere persistencia
  - **Actualizaciones** Java/SonarQube manuales recurrentes
  - **Autenticación** local (admin token) + **superficie de ataque** (Web UI + API + DB + webhook) en un host ya crítico
  - **Reproducibilidad**: estado en DB + UI, no "config as code" (viola principio)
  - **Maintenance**: 2-4 h/mes sin equipo ops dedicado
- **Alternativa de coste ~0** que cubre el mismo gap: activar reglas C901/PLR/SIM en ruff + vulture (F-PLA-01/F-PLA-02)

Impact:
- La conclusión final de F-PL-08 (no SonarQube) se **mantiene**, pero la justificación correcta es el **coste operacional en OrangeHouse**, NO la duplicación con ruff (que era incorrecta).
- La señal longitudinal de maintainability que SonarQube proveería puede obtenerse con ruff extendido + vulture + trend en CI a coste ~0.

Required human decision:
- D-PLA-08: Confirmar no-SonarQube; adoptar la alternativa ruff extendido + vulture como gate de maintainability.

Recommended remediation:
- RECOMMENDATION ONLY: activar C901/PLR/SIM en ruff; añadir vulture a CI (non-blocking → blocking); documentar trend de complexity en nightly compliance report.

Verification required:
- Ruff con reglas extendidas + vulture en CI verdes con baseline calibrado.

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: pyproject.toml · Tests: NOT_TRACED · CI: quality job · Evidence: E-PLA-08 · Closure: OPEN

---

## F-PLA-09 — Cadena RULE→CI→EVIDENCE incompleta: sin hash de evidencia, waiver ni ownership

Severity: HIGH
Status: OPEN
Classification: NUEVO
Control: Policy Governance
Source: FASE 2, 10 del informe complementario

Evidence:
- **Ninguna regla (R1..R16) tiene la cadena completa**:
  - Rule ID: implícito (R11..R16 en tracking.yaml con id)
  - Description: tracking.yaml `descripcion` ✔
  - Scope: parcial (tracking no declara `scope:` explícito)
  - Severity: tracking.yaml sin `severity:` (R1..R16 no la tienen)
  - Owner: **ninguna regla declara owner humano** (necesario anti-captura IA)
  - Enforcement: tracking `mecanismo` + `activada_en_ci` ✔
  - Tests positivos: tracking `backtest` parcial (R11 sin backtest — F-B20-03)
  - Tests negativos: solo en archivos de test, no declarados en tracking
  - Evidencia reproducible: **sin hash/digest de la evidencia** (solo git_hash inyectado, B-20)
  - CI gate: `activada_en_ci` ✔ (verificado por engineering_health)
  - Master Plan: parcial (R11→F2.5; R12..R16→AUDIT-apps)
  - ADR: parcial (ADR-0006/0015 en docstrings, no en tracking)
  - Estado: **no declarado** (ACTIVE/DEPRECATED/SUPERSEDED)
  - Waiver: **no existe** mecanismo de waiver
  - Expiración de waiver: **no existe**
  - Historial: solo git log
  - Reporting: no existe compliance report
- **AI-agent resistance**: sin hash de evidencia + waiver expirado detectable + ownership, un agente puede (Caso B/D/E) modificar guard/test/registry/CI y obtener PASS (vector demostrado en FASE 9 del informe)

Impact:
- La OCM Constitution no tiene enforcement completo: la cadena termina en "CI job pasa" pero no en "evidencia verificable + waiver gobernado + ownership humano". El riesgo de captura por agente IA es real.

Required human decision:
- D-PLA-09: Completar la cadena: (1) esquema de registry con todos los campos (F-PL-06/F-PLC-05); (2) hash de evidencia en CI artifact; (3) waiver con expiración + ADR; (4) ownership por regla + CODEOWNERS + branch protection.

Recommended remediation:
- RECOMMENDATION ONLY: extender M1..M20 con M21..M25 (tests obligatorios, enforcement, dead rules, waiver expirado, ADR huérfano); registry YAML; hash de guards+config en artifact inmutable.

Verification required:
- Registry parseable con M21..M25; hash de evidencia verificado en CI; waiver expirado → FAIL.

Traceability:
- Tracking: R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11, R12, R13, R14, R15, R16 · ADR: ADR-0015, ADR-0020 · Implementation: NOT_TRACED · Tests: NOT_TRACED · CI: NOT_TRACED · Evidence: E-PLA-09 · Closure: OPEN