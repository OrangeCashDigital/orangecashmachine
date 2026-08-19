# ADR-0038: Grafana provisioning versionado

**Estado:** Propuesto
**Fecha:** 2026-08-19
**Bounded context(s) afectado(s):** ocm (plataforma), observabilidad

## Contexto

La auditoría de Policy Layer (F-PL-10) identificó que la observabilidad Grafana **no es reproducible**:

- `docker-compose.yml:237-238` monta `./deploy/monitoring/grafana/provisioning` y `./dashboards`
- `.gitignore:72-73` → **ambos directorios están gitignored**
- `ls deploy/monitoring/grafana/` → `provisioning/` y `dashboards/` **vacíos** (creados ago 9)
- `README.md:267/274` referencia dashboards "provisionados desde deploy/" — **no reproducible desde el repo**

**Impacto real:** un clon limpio del repo obtiene Grafana sin dashboards ni datasources. Violación del principio "qué partes son reproducibles" (FASE 2, pregunta 10) y de la reproducibilidad que exige la evidencia de auditoría.

**Estado actual de observabilidad:**
| Componente | Estado | Reproducible |
|---|---|---|
| Prometheus | Config versionado (`deploy/monitoring/prometheus.yml`) | ✅ |
| Alertmanager | Config versionado (`deploy/monitoring/alertmanager.yml`) | ✅ |
| Loki | Config versionado (`deploy/monitoring/loki/loki.yml`) | ✅ |
| Grafana dashboards | **Vacíos + gitignored** | ❌ |
| Grafana datasources | **Vacíos + gitignored** | ❌ |

## Alternativas evaluadas

1. **Versionar provisioning de Grafana** (dashboards JSON + datasources YAML) y quitar del `.gitignore` — Ventaja: observabilidad 100% reproducible; clon limpio = Grafana funcional. Desventaja: requiere exportar/crear dashboards JSON y datasources YAML.
2. **Eliminar montajes de directorios vacíos y documentar setup manual** — Ventaja: cero esfuerzo en repo; honesto sobre lo que no está automatizado. Desventaja: observabilidad no reproducible; onboarding manual propenso a error.
3. **Grafana como "sin provisioning" — solo métricas via Prometheus API** — Ventaja: simplifica stack. Desventaja: pierde valor de dashboards predefinidos; no resuelve reproducibilidad.

## Decisión

**Versionar el provisioning de Grafana** (opción 1) — observabilidad reproducible es requisito de producción (F4, Constitution).

### Implementación:
1. **Crear datasources YAML** en `deploy/monitoring/grafana/provisioning/datasources/datasources.yaml`:
   ```yaml
   apiVersion: 1
   datasources:
     - name: Prometheus
       type: prometheus
       url: http://prometheus:9090
       access: proxy
       isDefault: true
       editable: false
   ```
2. **Exportar/crear dashboards JSON** en `deploy/monitoring/grafana/dashboards/`:
   - Pipeline health dashboard (OCM pipeline status, throughput, latency)
   - Kafka dashboard (topics, partitions, lag, throughput)
   - Redis dashboard (memory, commands, latency)
   - System dashboard (CPU, RAM, disk, network)
   - Trading dashboard (positions, P&L, orders, risk)
3. **Dashboard provisioning YAML** en `deploy/monitoring/grafana/provisioning/dashboards/dashboards.yaml`:
   ```yaml
   apiVersion: 1
   providers:
     - name: 'OCM Dashboards'
       orgId: 1
       folder: 'OCM'
       type: file
       disableDeletion: false
       updateIntervalSeconds: 30
       allowUiUpdates: true
       options:
         path: /etc/grafana/provisioning/dashboards
   ```
4. **Actualizar `docker-compose.yml`**: mantener montajes, **quitar de `.gitignore`** las líneas 72-73
5. **Actualizar `README.md`**: corregir referencias a "provisionados desde deploy/" → "versionados en repo, auto-provisionados"

### Criterios de aceptación:
- `git clone <repo> && docker compose up -d` → Grafana accesible en :3000 con dashboards y datasources pre-cargados
- `ls deploy/monitoring/grafana/provisioning/datasources/` → `datasources.yaml` existe
- `ls deploy/monitoring/grafana/dashboards/` → ≥4 archivos `.json` existen
- `docker compose up -d grafana` → logs muestran "provisioning dashboards from /etc/grafana/provisioning/dashboards"

## Justificación técnica

- **Reproducibilidad completa**: clona → `docker compose up` → observabilidad funcional (sin pasos manuales)
- **Config as code**: dashboards y datasources versionados, revisables, testeables
- **Cero coste operacional**: archivos estáticos, sin servidor adicional, sin DB
- **Compatible con Constitution**: F4 observabilidad reproducible, artifact integrity
- **Onboarding cero-fricción**: nuevo ingeniero ve dashboards inmediatos

## Consecuencias

- **Más fácil:** Observabilidad reproducible desde repo limpio; dashboards versionados = history + review
- **Deuda aceptada:** Inversión inicial exportando/creando dashboards JSON (~4-8 dashboards); mantenimiento menor (actualizar dashboards cuando cambian métricas)
- **Contratos que hacen cumplir:** `docker-compose.yml` healthchecks + provisioning montajes; `.gitignore` limpio; `deploy_ocm.sh` (ADR-0037) verifica Grafana health post-deploy
- **Relación con ADR-0037:** CD verify incluye Grafana health check

## Referencias

- Código: `docker-compose.yml`, `deploy/monitoring/grafana/`, `.gitignore`
- Hallazgos: B-58 (tracking.yaml)
- ADRs relacionados: ADR-0022, ADR-0037
- Auditorías: `AUDIT_OCM_POLICY_LAYER_FEASIBILITY_2026-08-19.md` (F-PL-10)