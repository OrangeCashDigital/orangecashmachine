# Market Data Runtime & Deployment — Forensic Compliance Audit

## Executive Summary

Auditoría de runtime/deployment de `packages/market_data` en OCM, ejecutada
bajo AUDIT_PROTOCOL v2.2. Hallazgo central: el estado declarado en
`docs/plans/tracking.yaml` (systemd de `streaming` VERIFICADO) es falso frente
al sistema real — la unidad `streaming.service` no existe en el host
(`systemctl status streaming` -> "could not be found"). Existen además 3
informes de auditoría sin trackear del mismo día que fallan validación
mecánica, y 4 vulnerabilidades de dependencias sin risk-accept vigente. No se
declara PRODUCTION VERIFIED. No se ejecutó ningún cambio de código,
configuración ni deployment durante esta auditoría.

## Scope

`packages/market_data/` (application, infrastructure, ports), `apps/app/cli/`,
`deploy/`, ADRs relacionados (ADR-0013, ADR-0014, ADR-0022, ADR-0024, ADR-0037),
`docs/plans/tracking.yaml`, y los 3 informes de auditoría preexistentes sin
trackear del 2026-08-20.

## Methodology

Lectura de código real (find + cat sobre el árbol de market_data), lectura de
ADRs, ejecución de `scripts/audit_validator.py` contra los 3 informes
existentes, ejecución de los comandos canónicos §R, verificación directa de
systemd (`systemctl status/is-enabled`, `journalctl`). Ninguna conclusión se
basa en literatura externa ni en inferencia sin evidencia en código/sistema.

## Governance Baseline

AUDIT_PROTOCOL.md v2.2. Read-Only Boundary (§K): única escritura permitida es
`docs/audits/`; prohibido `git add/commit/push`. Jerarquía de autoridad:
código/tests > contratos arquitectónicos > ADRs > doc oficial > doc interna/KB
> literatura externa. Contradicciones código↔ADR/tracking se registran como
Decisión Humana (§M), nunca resueltas por el agente.

## Matriz de Findings

| ID | Severity | Classification | Control | ADR | Estado |
|---|---|---|---|---|---|
| F-MD-01 | CRITICAL | CONTRADICCIÓN | DEPLOYMENT_RUNTIME | ADR-0022 | OPEN |
| F-MD-02 | HIGH | NUEVO | DEPENDENCY_AUDIT | N/A | OPEN |
| F-MD-03 | MEDIUM | NUEVO | AUDIT_PROTOCOL_COMPLIANCE | N/A | OPEN |
| F-MD-04 | HIGH | REVALIDADO | DATA_PIPELINE | ADR-0022 | REVALIDADO |

Total: 4 findings (1 CONTRADICCIÓN, 2 NUEVO, 1 REVALIDADO) — reconciliación:
Σ clasificación = 4 = Σ severidad (1 CRÍTICA + 2 ALTA + 1 MEDIA) = total 4.

## Matriz de Controles

| Control | Comando canónico | Exit | Resultado |
|---|---|---|---|
| ARCH_CONTRACTS | `uv run lint-imports --config architecture_linter/importlinter.toml` | 0 | PASS — 50 kept, 0 broken |
| DEPENDENCY_AUDIT | `uv run pip-audit . --ignore-vuln PYSEC-2026-113 --ignore-vuln PYSEC-2026-1325` | 1 | FAIL — ver F-MD-02 |
| YAMLLINT | `uvx yamllint -c .yamllint .` | 0 | PASS (solo warnings de estilo) |
| GOLDEN | `uv run pytest tests/architecture_linter/test_golden.py -q --no-cov` | 0 | PASS — 4/4 |
| ENGINEERING_HEALTH | `uv run python scripts/engineering_health_check.py` | — | NO EJECUTADO esta sesión — pendiente |

## Matriz de Decisiones

| ID | Pregunta | Opciones | Finding relacionado |
|---|---|---|---|
| D-MD-01 | ¿`tracking.yaml` F2.6b se marca VERIFICADO sin unidad systemd instalada? | (a) revertir a NO_VERIFICADO hasta instalar+verificar la unit; (b) documentar que streaming corre bajo otro supervisor (no systemd) y corregir ADR-0022 | F-MD-01 |
| D-MD-02 | ¿Qué hacer con los 3 informes sin trackear que fallan validación mecánica? | (a) descartarlos y usar solo este informe canónico; (b) corregirlos para que pasen M12/M13/M17 y coexistan; (c) fusionar su contenido útil (tabla ADRs, conclusión `streaming`=canary) dentro de este informe | F-MD-03 |
| D-MD-03 | ¿Cómo tratar aiohttp x3 + cryptography x1 sin waiver? | (a) actualizar a versiones con fix (aiohttp>=3.14.2, cryptography>=50.0.0); (b) ampliar risk-accept documentando por qué se acepta el riesgo | F-MD-02 |
| D-MD-04 | El validador acopla informe↔registro más reciente (M17); este informe usa un registro de findings propio (`OCM_AUDIT_FINDINGS_2026-08-20_market-data-runtime.md`) en vez de `..._policy-layer.md`. ¿Es correcto un registro por auditoría/dominio, o debe ser uno solo? | (a) un registro por dominio de auditoría; (b) registro único acumulativo | Governance, no bloqueante |

## Integridad

- Working tree NO preservado íntegro: `docs/plans/tracking.yaml` modificado
  fuera de `docs/audits/` (pre-existente a esta sesión, ver F-MD-01) — checklist
  §P.5 en FAIL.
- Esta auditoría no ejecutó `git add`, `git commit` ni `git push`.
- Única escritura de esta sesión: los dos archivos en `docs/audits/`
  (este informe y su registro de findings).
- `uv run python scripts/audit_validator.py --report docs/audits/AUDIT_OCM_FORENSIC_COMPLIANCE_2026-08-20.md --register docs/audits/OCM_AUDIT_FINDINGS_2026-08-20_market-data-runtime.md` debe correrse tras pegar ambos archivos, para confirmar PASS/FAIL real de este informe (no asumido).

## Reproducibilidad



	•	commit: f30dd087c1ae5ebebbd795c5007e1d8fff16e06e
	•	branch: fix/production-config-bybit
	•	fecha: 2026-08-20
	•	protocolo: AUDIT_PROTOCOL v2.2
	•	agente/modelo: Claude (Sonnet 5)
	•	herramientas: pip-audit 2.10.1, ruff 0.15.10, mypy 1.19.1, bandit 1.9.4, pytest 8.4.2, yamllint 1.38.0
	•	comandos: lint-imports –config architecture_linter/importlinter.toml; pip-audit . –ignore-vuln PYSEC-2026-113 –ignore-vuln PYSEC-2026-1325; uvx yamllint -c .yamllint .; pytest tests/architecture_linter/test_golden.py -q –no-cov; systemctl status/is-enabled streaming
	•	golden: PASS
	•	resultado: PENDIENTE (correr audit_validator sobre este informe tras pegarlo)
## Roadmap (no ejecutar sin autorización)

1. Resolver D-MD-01 antes de tocar cualquier unit systemd nueva propuesta en
   los informes previos.
2. Resolver D-MD-02 para decidir la fuente única de verdad del diseño target
   (Fases 5-12 de la tarea original) antes de escribir units concretas.
3. Resolver D-MD-03 (actualizar aiohttp/cryptography o ampliar waiver) antes
   del próximo deploy.
4. Solo después de D-MD-01/02, avanzar con Fase 11-12 de la tarea original
   (arquitectura target + systemd concreto) como trabajo de implementación
   separado, fuera de modo auditoría.

## Risks

Ejecutar la propuesta de systemd de los informes previos sin resolver D-MD-01
reproduciría el mismo problema: declarar "supervisado por systemd" sin
verificación real, exactamente el patrón que esta auditoría encontró.

## Open Questions

Ver Matriz de Decisiones.

## Explicit Recommendation

**¿Qué debería significar MARKET-DATA HEALTHY?** No systemd `active` solo.
Dado F-MD-04 (OHLCVPipeline con NullPublisher hardcodeado) y F-MD-01 (unidad
systemd inexistente pese a declararse verificada), un proceso puede estar
"activo" sin producir datos ni estar realmente supervisado. El health debe
distinguir explícitamente: (1) proceso vivo, (2) unidad systemd realmente
instalada y verificada con reinicio, (3) datos fluyendo a Kafka/Bronze. Este
informe no prescribe la implementación de ese contrato — eso es Fase 9-10 de
la tarea original y requiere resolver D-MD-01/02 primero.

**¿Qué debería/no debería supervisar systemd?** No se puede responder de
forma autoritativa mientras exista la contradicción F-MD-01: cualquier
diseño de supervisión que se proponga ahora repetiría el problema de
documentar un estado no verificado.

*Fin del informe. AUDITORÍA NO TERMINADA — D-MD-01 a D-MD-04 pendientes de decisión humana.*
