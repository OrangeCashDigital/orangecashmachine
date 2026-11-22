# OCM — AUDIT FINDINGS REGISTER — Data Plane Streaming (production)

**Ejecución de auditoría:** 2026-08-28 (branch `feat/b56-ci-stage-ordering` @ `893773558522188545c9c78d294f770d00c605d1`; origin/main `44034eaa`)
**Fuente primaria:** `docs/audits/AUDIT_OCM_data-plane-streaming_2026-08-28.md`
**Alcance:** DATA PLANE — causa raíz exacta de `ocm-streaming.service` FAILED y bloqueo del market-data realtime en `production`. Trading/live/paper/portfolio out-of-scope (solo aislamiento).
**Régimen:** read-only (AUDIT_PROTOCOL §K). Única escritura: `docs/audits/`.
**Estado de este registro:** OPEN

Resumen: CRITICAL 1 · MEDIUM 1 · **total 2**.

Clasificación (taxonomía AUDIT_PROTOCOL §G):
- REGRESIÓN: 1 — F-DPL-01
- RECOMENDACIÓN: 1 — F-DPL-02

Deduplicación (regla §H):
- F-DPL-01 es REGRESIÓN: **contradice** `OCM_AUDIT_FINDINGS_2026-08-20_market-data-runtime.md`, que declaró `ocm-streaming.service ACTIVE/HEALTHY` y Plan Maestro `OPERATIONALLY VERIFIED`. Hoy el servicio está FAILED con `At least one exchange must be enabled` (ningún exchange habilitado en `production`). Se contrasta con tracking B-59 (PENDIENTE) y ADR-0022/0016/0013/0014; NO es NUEVO (relacionado con B-59/F-PL-11).
- F-DPL-02 es RECOMENDACIÓN: observabilidad (Prometheus/Grafana/Loki/Alertmanager/Promtail) no desplegada; provisioning versionado (B-58 HECHO) pero sin runtime.

---

## F-DPL-01 — `ocm-streaming.service` FAILED en producción: ningún exchange habilitado en la cascada Hydra

Severity: CRITICAL
Status: OPEN
Classification: REGRESIÓN
Control: Streaming Realtime Data Plane

Evidence:
- `systemctl is-active ocm-streaming.service` → `failed` (verificado 2026-08-28)
- journal (live): `ocm.config.pipeline.ConfigPipelineError: [ConfigPipeline:VALIDATED] ... Value error, At least one exchange must be enabled. [type=value_error ...]` — cruce exacto `streaming --env production`
- `hydra_loader.py:271 load_appconfig_from_hydra` → `hydra_cfg_to_appconfig` → `pipeline.py:131 _l4_validate` → `pipeline.py:273 raise ConfigPipelineError([VALIDATED])`
- `config/exchanges/bybit.yaml:5` → `enabled: false`
- `config/exchanges/kucoin.yaml:5` → `enabled: false`
- `config/exchanges/kucoinfutures.yaml:5` → `enabled: false`
- `config/env/production.yaml` → sin sección `exchanges:`
- `config/env/development.yaml:52-54` → `exchanges: bybit: enabled: true` (override)
- `config/env/test.yaml:61-63` → `exchanges: bybit: enabled: true` (override)
- `ocm/config/schema.py:865` → `validate_exchanges`: `At least one exchange must be enabled`

Traceability:
- Tracking: B-59 (PENDIENTE)
- ADR: ADR-0022
- Implementation: config/exchanges + config/env/production.yaml + deploy/systemd/rendered/ocm-streaming.service
- Tests: NOT_TRACED
- CI: NOT_TRACED
- Evidence: E-DPL-01
- Closure: OPEN

---

## F-DPL-02 — Observabilidad del data-plane NO desplegada (Prometheus/Grafana/Loki/Alertmanager/Promtail inactivos)

Severity: MEDIUM
Status: OPEN
Classification: RECOMENDACIÓN
Control: Observability

Evidence:
- `systemctl is-active prometheus|grafana|loki|alertmanager|promtail` → todos `inactive`
- `deploy/monitoring/prometheus.yml`, `alertmanager.yml`, `alerts.yml`, `loki/loki.yml`, `promtail/promtail.yml` versionados en main
- Dashboards/datasources Grafana solo en rama de trabajo `b56` (NO en main)
- `deploy/scripts/health_check.sh` existe en main pero NO integrado en el unit
- infra data-plane (Kafka/Redis/ZooKeeper/Pushgateway) UP healthy (`Up 2 days`)

Traceability:
- Tracking: B-58 (HECHO, provisioning)
- ADR: ADR-0038
- Implementation: deploy/monitoring/grafana + deploy/monitoring/prometheus.yml
- Tests: NOT_TRACED
- CI: NOT_TRACED
- Evidence: E-DPL-02
- Closure: OPEN

---

## Reconciliación (M10)

Hallazgos: REGRESIÓN 1 (= CRITICAL 1) + RECOMENDACIÓN 1 (= MEDIUM 1) = total 2.
Clasificación: Σ = 1 + 1 = 2. Severidad: Σ = 1 + 1 = 2. **OK.**

## Matriz de Controles

| Control | Estado | Evidencia |
|---|---|---|
| STREAMING_RUNTIME | **FAIL** | `systemctl is-active` failed; journal `At least one exchange must be enabled` |
| CONFIG_PRODUCTION_EXCHANGES | **FAIL** | 3 × `enabled: false`; production.yaml sin `exchanges:` |
| INFRA_DATA_PLANE | **PASS** | Kafka/Redis/ZooKeeper/Pushgateway UP healthy |
| OBSERVABILITY | **FAIL** | Prometheus/Grafana/Loki/Alertmanager/Promtail inactive |
| TRADING_ISOLATION | **PASS** | `streaming` usa DATASOURCE_REPLAY; sin imports trading/live/paper/oms/portfolio |
| SECURITY_CREDENTIALS | **PASS** | `.env` 600, no versionado, solo nombres presentes |

Controles = PASS(3) + FAIL(3) = 6
