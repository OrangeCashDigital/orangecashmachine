# OCM — Forensic Compliance Audit — Data Plane Streaming (production)

**Versión:** AUDIT_PROTOCOL v2.1 · **Fecha:** 2026-08-28
**Auditor:** Lead Engineer / SRE (agente autónomo, read-only)
**Alcance:** DATA PLANE. Causa raíz exacta de `ocm-streaming.service` FAILED y qué bloquea el market-data realtime en `production`. Trading/live/paper/portfolio fuera de alcance (solo aislamiento).
**Régimen:** read-only (AUDIT_PROTOCOL §K). Única escritura: `docs/audits/`.
**Estado:** OPEN
**Registro de hallazgos:** `OCM_AUDIT_FINDINGS_2026-08-28_data-plane-streaming.yaml.md`

## REPRODUCIBILIDAD

```
- commit: 893773558522188545c9c78d294f770d00c605d1
- branch: feat/b56-ci-stage-ordering
- origin/main: 44034eaa1f55ed9518364ac042fbb3592c85c68e
- fecha: 2026-08-28
- protocolo: AUDIT_PROTOCOL v2.1
- agente/modelo: opencode (big-pickle)
- herramientas: uv sync (prod); uv run python scripts/audit_validator.py;
  git; systemctl/journalctl; grep; mypy/ruff no ejecutados (no aplican)
- comandos canónicos (AUDIT_PROTOCOL §R): lint-imports = `uv run lint-imports
  --config architecture_linter/importlinter.toml`; DEPENDENCY_AUDIT = `uv run
  pip-audit .`; YAMLLINT = `uvx yamllint -c .yamllint .`
- golden: no aplica (no se ejecutó linter arquitectónico en esta auditoría data-plane)
- resultado: ver sección Validación mecánica (M1..M20)
```

## Executive Summary

**El market-data realtime en `production` está caído.** `ocm-streaming.service` está en estado **failed** de forma persistente y reproducible. Causa raíz determinista: en `production`, la cascada Hydra deja **los 3 exchanges `enabled: false`** (`config/exchanges/*.yaml:5`) y `config/env/production.yaml` **no redefine `exchanges:`** → lista vacía → `AppConfig.validate_exchanges` (schema.py:865) lanza `At least one exchange must be enabled` → `ConfigPipelineError[VALIDATED]` → `_load_config` → None → exit 1 → systemd FAILED.

No hay **ningún exchange habilitado en producción** por diseño de los YAML base (`development`/`test` sí habilitan bybit). **No existe un cambio autónomo seguro** para operar el data-plane en producción: habilitar exchange exige decisión humana de exchange + credenciales + permisos (fuera del límite read-only).

Hallazgos: **2** (F-DPL-01 REGRESIÓN CRITICAL · F-DPL-02 RECOMENDACIÓN MEDIUM).

## Governance Baseline (§D)

- Branch `feat/b56-ci-stage-ordering` @ HEAD `8937735` (VOLÁTIL entre sesiones; sesión previa HEAD `1bf93ad`).
- origin/main `44034eaa` (= PR #27, B-51 merged; incluye B-60).
- Working tree: 1 cambio untracked `deploy/systemd/rendered/` (NO tocado; `.gitignore` sensible externo, no modificado).
- B-53..59 no merged; PR #20 (B-59) stale/conflicting.

## Escala de Evidencia y Metodología

Evidencia: servidor (`systemctl`, `journalctl`) > código/config ejecutable > tests > ADRs/docs.
MACHINE CHECKS FIRST (§Q): `uv run python scripts/audit_validator.py --register ... --report ...`.

## Matriz de Findings

| ID | Severity | Clasificación | Control | Estado |
|---|---|---|---|---|
| F-DPL-01 | CRITICAL | REGRESIÓN | Streaming Realtime Data Plane | FAIL |
| F-DPL-02 | MEDIUM | RECOMENDACIÓN | Observability | FAIL |

### F-DPL-01 — `ocm-streaming.service` FAILED en producción (REGRESIÓN · CRITICAL)

- `systemctl is-active ocm-streaming.service` → `failed`.
- Journal (live 14:29:05): `ocm.config.pipeline.ConfigPipelineError: [ConfigPipeline:VALIDATED] ... Value error, At least one exchange must be enabled. [type=value_error ...]`.
- Cadena: `streaming --env production` → `hydra_loader.py:271` → `hydra_cfg_to_appconfig` → `pipeline.py:131 _l4_validate` → `pipeline.py:273 raise ConfigPipelineError`.
- `config/exchanges/{bybit,kucoin,kucoinfutures}.yaml:5` → `enabled: false`.
- `config/env/production.yaml` → sin sección `exchanges:`.
- `config/env/{development,test}.yaml` → `exchanges: bybit: enabled: true` (override).
- `ocm/config/schema.py:865` → `validate_exchanges`: `At least one exchange must be enabled`.
- Contradice `OCM_AUDIT_FINDINGS_2026-08-20_market-data-runtime.md` (declaró ACTIVE/HEALTHY). Relacionado B-59 PENDIENTE; ADR-0022/0016/0013/0014.
- **Impacto:** market-data realtime en producción caído; sin DATA PLANE observable; bloquea B-59 y `systemd_reinicia_correctamente`.
- **Decisión requerida (D-DPL-01):** qué exchange(s) habilitar en `production` y con qué credenciales/permisos (Decisión Humana §N).

### F-DPL-02 — Observabilidad del data-plane NO desplegada (RECOMENDACIÓN · MEDIUM)

- `systemctl is-active prometheus|grafana|loki|alertmanager|promtail` → todos `inactive`.
- Configs versionadas en main: `deploy/monitoring/prometheus.yml`, `alertmanager.yml`, `alerts.yml`, `loki/loki.yml`, `promtail/promtail.yml`.
- Dashboards/datasources Grafana solo en rama de trabajo `b56` (NO en main).
- `deploy/scripts/health_check.sh` en main pero NO integrado en el unit.
- Infra data-plane (Kafka/Redis/ZooKeeper/Pushgateway) UP healthy (`Up 2 days`).
- **Impacto:** sin visibilidad del data-plane; B-58 (Grafana provisioning HECHO) sin runtime observability.
- **Decisión requerida (D-DPL-02):** desplegar stack observabilidad (systemd vs Docker) y conectar `health_check.sh`.

## Matriz de Controles

| Control | Dominio | Evidencia | Estado |
|---|---|---|---|
| STREAMING_RUNTIME | Data Plane | `systemctl is-active` failed; journal `At least one exchange must be enabled` | **FAIL** |
| CONFIG_PRODUCTION_EXCHANGES | Config | 3 × `enabled: false`; `production.yaml` sin `exchanges:` | **FAIL** |
| INFRA_DATA_PLANE | Infra | Kafka/Redis/ZooKeeper/Pushgateway UP healthy | **PASS** |
| OBSERVABILITY | Obs | Prometheus/Grafana/Loki/Alertmanager/Promtail inactive | **FAIL** |
| TRADING_ISOLATION | Seguridad | `streaming` usa `DATASOURCE_REPLAY`; sin imports trading/live/paper/oms/portfolio | **PASS** |
| SECURITY_CREDENTIALS | Seguridad | `.env` 600, no versionado, solo nombres; sin secretos en código | **PASS** |

Controles = PASS(3) + FAIL(3) = 6

### Evidencia de controles

- `STREAMING_RUNTIME` FAIL: `systemctl is-active` failed; journal `At least one exchange must be enabled`.
- `CONFIG_PRODUCTION_EXCHANGES` FAIL: 3 × `enabled: false`; `production.yaml` sin `exchanges:`.
- `INFRA_DATA_PLANE` PASS: Kafka/Redis/ZooKeeper/Pushgateway UP healthy.
- `OBSERVABILITY` FAIL: Prometheus/Grafana/Loki/Alertmanager/Promtail `inactive`.
- `TRADING_ISOLATION` PASS: `streaming` usa `DATASOURCE_REPLAY`; sin imports trading/live/paper/oms/portfolio; Cryptofeed sin credenciales; fail-closed R1/R-IS_STUB.
- `SECURITY_CREDENTIALS` PASS: `.env` 600, no versionado, solo nombres; sin secretos en código.

## Matriz de Decisiones

| ID | Decisión | Bloqueo | Consecuencia si no se decide |
|---|---|---|---|
| D-DPL-01 | Elegir exchange(s) para `production` + autorizar credenciales/permisos | **SÍ (data-plane)** | Sin esta decisión no hay cambio autónomo seguro; data-plane sigue caído |
| D-DPL-02 | Aprobar despliegue de observabilidad (systemd vs Docker) y conectar `health_check.sh` | Parcial | Sin visibilidad del data-plane |
| D-DPL-03 | Resolver contradicción documental con informe 2026-08-20 (¿runtime verificado fue no-producción?) | Documental | Estado operacional previo desmentido queda sin corrección |

## Integridad

- Read-only preservado: NO se modificó código, tests, CI, ADRs, tracking.yaml, governance, pyproject.toml, ni `.gitignore`.
- Única escritura: `docs/audits/OCM_AUDIT_FINDINGS_2026-08-28_data-plane-streaming.yaml.md` y `docs/audits/AUDIT_OCM_data-plane-streaming_2026-08-28.md`.
- Working tree antes del informe: 1 untracked `deploy/systemd/rendered/` (preexistente, no tocado).
- No hubo `git add`/`git commit`/`git push` (prohibido §K).
- No se habilitó exchange ni se modificaron credenciales ni se reinició servicio.

## Risk Matrix

| Riesgo | Severidad Técnica | Estado Normativo |
|---|---|---|
| Data-plane realtime caído | CRITICAL | UNGOVERNED (B-59 PENDIENTE) |
| Observabilidad ausente | MEDIUM | UNGOVERNED (B-58 HECHO, runtime ausente) |
| Contradicción doc 2026-08-20 | HIGH (doc) | CONTRADICCIÓN (§M) |
| Trading accidental | LOW | fail-closed demostrado |

## Validación mecánica (M1..M20)

Comando: `uv run python scripts/audit_validator.py --register docs/audits/OCM_AUDIT_FINDINGS_2026-08-28_data-plane-streaming.yaml.md --report docs/audits/AUDIT_OCM_data-plane-streaming_2026-08-28.md`

**Resultado: PASS** — 2 findings, 25 reglas mecánicas, exit 0. WARN no bloqueantes: M20 (3 controles FAIL sin finding NUEVO — esperado y correcto por §H: F-DPL-01 es REGRESIÓN, no NUEVO) y M25 (ADR-0020 related_adrs preexistente).

## Roadmap / Cierre

BLOQUEADO por decisión humana (§Matriz de Decisiones). Sin autorización (exchange+credenciales+despliegue observabilidad) no procede cambio alguno. La integridad del working tree se preserva.

## Final Verification Checklist (§P)

1. ✅ Plan Maestro + GOVERNANCE + AGENTS + AUDIT_PROTOCOL descubiertos.
2. ✅ Cada FAIL contrastado con tracking.yaml (B-59), ADRs y auditoría 2026-08-20.
3. ✅ Taxonomía estricta (REGRESIÓN/RECOMENDACIÓN).
4. ✅ Contadores reconciliados (§Reconciliación del registro).
5. ✅ Integridad working tree: solo `docs/audits/` modificado.
6. ✅ Validador M1..M20 ejecutado (§Validación mecánica).

## Reconciliación (M10)

Hallazgos Σ = 2. Clasificación: REGRESIÓN 1 + RECOMENDACIÓN 1 = 2. Severidad: CRITICAL 1 + MEDIUM 1 = 2. Controles = 6 (3 PASS, 3 FAIL). **OK.**
