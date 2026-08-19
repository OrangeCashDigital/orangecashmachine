# ADR-0034: Semgrep adoption (non-blocking, arquitectura/policy)

**Estado:** Propuesto
**Fecha:** 2026-08-19
**Bounded context(s) afectado(s):** ocm (plataforma), CI/CD

## Contexto

La auditoría de viabilidad (F-PL-08) concluyó "No introducir Semgrep" argumentando que "import-linter + AST guards + Bandit + CodeQL ya cubren". La auditoría complementaria (F-PLC-02) **contradijo** esta conclusión demostrando que **Semgrep aporta valor marginal real** para arquitectura/policy que ninguna herramienta actual cubre:

| Patrón | AST Guard | import-linter | Bandit | CodeQL | Semgrep |
|---|---|---|---|---|---|
| imports prohibidos cross-layer (domain→infra) | R11 (subprocess) | BC-03/BC-09 (grafo estático) | NO | SÍ (dataflow) | **SÍ** (pattern `import infra.*` en `domain/`) |
| llamadas prohibidas (argparse en use_cases) | R12 (argparse) | BC-53 (grafo estático) | NO | SÍ | **SÍ** |
| acceso directo capa a capa (adapters→application) | R13 (getattr) | BC-05/BC-06 (grafo estático) | NO | SÍ | **SÍ** |
| APIs deprecated (pandas.DataFrame.append) | NO | NO | NO | SÍ | **SÍ** |
| os.environ directo en domain/application | NO | NO | NO | SÍ | **SÍ** |
| subprocess fuera de infrastructure | R11 (domain) | NO (no es import) | SÍ (subprocess) | SÍ | **SÍ** |
| filesystem directo en domain | NO | NO | NO | SÍ | **SÍ** |
| librerías concretas en domain (ccxt, pyiceberg) | NO | BC-09 (modules) | NO | SÍ | **SÍ** |
| crypto inseguro (md5/sha1/random en crypto ctx) | NO | NO | SÍ (random) | SÍ | **SÍ** |
| logging de secrets | NO | NO | NO | NO | **SÍ** |
| llamadas prohibidas en capas (time.sleep) | NO | NO | NO | NO | **SÍ** |

**Hallazgo clave (F-PLC-02):** AST Guards cubren 6/11; import-linter 5/11 (solo grafo estático); **Semgrep cubre 11/11 con reglas declarativas mantenibles**.

La auditoría adversarial (F-PLA-07) confirmó: **búsqueda exhaustiva de patrones peligrosos (eval/exec, pickle, shell=True, subprocess, os.environ, crypto, SQL, logging secrets, deserialización) → NO EVIDENCE OF MATERIAL GAP de seguridad**. Bandit (0 Med/High) + CodeQL (PR, dataflow) + Gitleaks + Trivy cubren la superficie real.

**Conclusión integrada:** Semgrep NO es necesario para seguridad (gap material = 0), pero **SÍ tiene valor preventivo para arquitectura** (reglas declarativas YAML para invariantes que hoy están hardcodeadas en AST Guards).

## Alternativas evaluadas

1. **NO adoptar Semgrep** — Ventaja: cero superficie, cero coste. Desventaja: pierde valor preventivo arquitectura; AST Guards siguen hardcodeados en Python.
2. **Adoptar Semgrep como BLOCKING en CI** — Ventaja: enforcement inmediato. Desventaja: reglas default generan falsos positivos; sin baseline, bloquearía merges legítimos; gap material seguridad = 0 no justifica blocking.
3. **Adoptar Semgrep como NON-BLOCKING (opcional)** — Ventaja: coste ~0 (CLI, ~500ms, sin servidor); reglas propias en `policies/semgrep/`; `--baseline` para suprimir ruido; migración progresiva R11..R16 a YAML. Desventaja: requiere mantenimiento de reglas.

## Decisión

**NO adoptar Semgrep como blocking. ADOPTAR opcionalmente como NON-BLOCKING inicialmente.**

- Crear `policies/semgrep/` con reglas propias (NO ruleset default)
- Migrar R11..R16 a Semgrep YAML progresivamente (patrón: `import infra.*` en `domain/`, `os.environ[` en `domain/|application/`, `subprocess.run` fuera de `infrastructure/`, `logger.*api_key`, `hashlib.md5`, `random.random` + contexto crypto)
- Job CI `semgrep` en stage ARCHITECTURE (paralelo a import-linter, <1 min, non-blocking)
- `--baseline` para suprimir hallazgos existentes; solo nuevos violations reportados
- Re-evaluar a blocking SOLO si: (a) reglas propias estables, (b) baseline limpio, (c) gap real detectado que Semgrep cubre y AST Guards no

## Justificación técnica

- **No gap material de seguridad** → blocking injustificado (F-PLA-07)
- **Valor preventivo arquitectura**: reglas declarativas complementan AST Guards (hardcodeados) e import-linter (grafo estático)
- **Coste ~0**: CLI-only, sin servidor, sin DB, sin auth, sin backup, ~500ms/PR
- **Migración progresiva**: R11..R16 a YAML reduce deuda de hardcodeo; backtest existente valida equivalencia
- **Compatible** con defensa en profundidad (ADR-0032): Semgrep crítico para Caso B (detecta patrones que guard modificado ocultaría)

## Consecuencias

- **Más fácil:** Reglas de arquitectura versionadas en YAML, revisables, testeables, sin deploy Python
- **Deuda aceptada:** Reglas default Semgrep no se usan (falsos positivos); requiere inversión inicial en reglas propias
- **Contratos que hacen cumplir:** CI job `semgrep` non-blocking; `audit_validator` M21..M25 validarán tests/evidence de reglas Semgrep en registry
- **Relación con ADR-0031/0032:** Reglas Semgrep en `policies/semgrep/` → registry con owner/severity/evidence; branch protection protege cambios

## Referencias

- Código: `policies/semgrep/` (propuesto), `.github/workflows/ocm-ci.yml` (job semgrep)
- Hallazgos: B-53 (tracking.yaml)
- ADRs relacionados: ADR-0015, ADR-0031, ADR-0032
- Auditorías: `AUDIT_OCM_POLICY_LAYER_FEASIBILITY_2026-08-19.md` (F-PL-08), `AUDIT_OCM_POLICY_LAYER_COMPLEMENTARY_2026-08-19.md` (F-PLC-02), `AUDIT_OCM_POLICY_LAYER_COMPLEMENTARY_2026-08-19.md` (adversarial, F-PLA-07)