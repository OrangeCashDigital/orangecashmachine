# OCM Audit Findings — Market Data Runtime & Deployment (2026-08-20)

## F-MD-01 — CONTRADICCIÓN: tracking.yaml declara systemd VERIFICADO pero la unidad no existe en host
Classification: CONTRADICCIÓN
Severity: CRITICAL
Control: DEPLOYMENT_RUNTIME
ADR: ADR-0022
Related: F-PL-11
Evidence:
- git diff docs/plans/tracking.yaml (working tree, no commiteado): F2.6b systemd_reinicia_correctamente NO_VERIFICADO -> VERIFICADO
- systemctl status streaming --no-pager -> "Unit streaming.service could not be found."
- systemctl is-enabled streaming -> "not-found"
- ADR-0022:323 (histórico) ya documentaba "Sin unit systemd activo"
- F-PL-11 (registro 2026-08-19, aún OPEN) exige como criterio de cierre: systemctl status streaming -> active + test de reinicio documentado
Impact:
- El estado declarado en tracking.yaml no está respaldado por el sistema real; un reinicio de orangehouse hoy dejaría streaming caído sin supervisión.
Required human decision: D-MD-01
Traceability: Tracking: docs/plans/tracking.yaml (diff no commiteado) · ADR: ADR-0022 · Tests: NOT_TRACED · CI: NOT_TRACED · Closure: OPEN

## F-MD-02 — NUEVO: vulnerabilidades pip-audit fuera del risk-accept vigente
Classification: NUEVO
Severity: HIGH
Control: DEPENDENCY_AUDIT
ADR: N/A
Related: N/A
Evidence:
- Comando canónico: uv run pip-audit . --ignore-vuln PYSEC-2026-113 --ignore-vuln PYSEC-2026-1325 -> Exit 1
- Found 4 known vulnerabilities, ignored 2 in 2 packages: aiohttp 3.14.1 PYSEC-2026-3545/3546/3547 (fix 3.14.2/3.14.3), cryptography 49.0.0 PYSEC-2026-3552 (fix 50.0.0)
- El risk-accept documentado 2026-08-03 cubre solo PYSEC-2026-113/1325 — no cubre ninguno de estos 4 IDs
Impact:
- 3 CVEs simultáneos en aiohttp (dependencia directa de mercado/streaming) y 1 en cryptography sin waiver vigente.
Required human decision: D-MD-03
Traceability: Tracking: NOT_TRACED · ADR: N/A · Tests: NOT_TRACED · CI: verificar ocm-ci.yml vigente · Closure: OPEN

## F-MD-03 — NUEVO: tres informes de auditoría sin trackear fallan validación mecánica
Classification: NUEVO
Severity: MEDIUM
Control: AUDIT_PROTOCOL_COMPLIANCE
ADR: N/A
Related: N/A
Evidence:
- git status --short (untracked, mismo día 2026-08-20): AUDIT_OCM_infrastructure-runtime-architecture_2026-08-20.md, AUDIT_OCM_market-data-runtime-deployment_2026-08-20.md, AUDIT_OCM_realtime-operational_2026-08-20.md
- uv run python scripts/audit_validator.py --report <cada uno> -> EXIT 1 en los 3
- Fallas comunes: M12 (faltan Matriz de Findings/Controles/Decisiones, Integridad), M13 (no citan lint-imports/pip-audit ./uvx), M17 (F-PL-01..11 en registro pero ausentes del informe)
Impact:
- Ninguno de los tres puede tratarse como informe canónico compliant tal cual está; contienen análisis útil pero no cumplen §O/§P del protocolo.
Required human decision: D-MD-02
Traceability: Tracking: NOT_TRACED · ADR: N/A · Tests: NOT_TRACED · CI: NOT_TRACED · Closure: OPEN

## F-MD-04 — REVALIDADO: OHLCVPipeline no persiste datos (NullPublisher hardcodeado)
Classification: REVALIDADO
Severity: HIGH
Control: DATA_PIPELINE
ADR: ADR-0022
Related: F-031, B-46
Evidence:
- ADR-0022, nota de discrepancia 2026-08-10 (F-031/B-46): OHLCVPipeline hardcodea NullPublisher() (ohlcv_pipeline.py:248)
- _chunk_converter no se inyecta (runtime.py:298)
- incremental.py:106 y backfill.py:427 lanzan RuntimeError en get_chunk_converter() antes de publish_chunk()
Impact:
- market_data.main puede estar systemd active sin que ningún evento llegue a ohlcv.raw ni a Bronze/Iceberg vía esta ruta.
Required human decision: N/A (ya trackeado en tracking.yaml F-031/B-46)
Traceability: Tracking: docs/plans/tracking.yaml F-031/B-46 · ADR: ADR-0022 · Closure: REVALIDADO
