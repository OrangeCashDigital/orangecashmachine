# OCM — Triage y Priorización: Estrategia Dual de Observabilidad

**Fecha:** 2026-09-08
**Tipo:** Triage / Priorización de backlog técnico (NO implementación).
**Baseline audit previa:** Auditoría observabilidad (análisis manual, 2026-09-08) — hallazgos G1..G13.
**Alcance del triage:** G2, G3, G4, G5, G6, G8, G10, G11, G12.
**Estado:** Solo lectura + escritura en `docs/audits/`. Sin cambios de código, tests, tracking, ADRs, CI ni commits.

---

## REPRODUCIBILIDAD

```
- commit: 0c8b4ff4ded716f3c39d0113824bc224f79955f0
- branch: fix/iceberg-commitfailed
- fecha: 2026-09-08
- protocolo: AUDIT_PROTOCOL v2.1 (aplicación: fase de triage read-only)
- agente/modelo: OpenCode (auto/claude-sonnet)
- herramientas: grep/rg (verificación estática), lectura directa de archivos
- comandos canónicos usados (verificación): 
   - rg -n "loki" config/ -i            → 0 (G2)
   - rg -n "format" config/...          → production/test/logging.yaml (G4)
   - rg -n "MetricsPort|Counter|Gauge|Histogram|push_metrics" packages/trading → 0 (G8)
   - rg -n "pipeline_filter" → solo filters.py + tests (G5)
- working tree: SUCIO con cambios pre-existentes ajenos a esta auditoría
  (AGENTS.md, README.md, docs/*, 3 packages/trading/risk/models.py, scripts/*,
   shared/contracts/boundaries.py, tests/*). No modificados por esta fase.
- validador M1..M20: NO ejecutado (documento de triage no-canónico; no se declara
  como informe forense de cumplimiento).
```

---

## Metodología

1. **Verificación en HEAD** de cada hallazgo mediante lectura de archivos y `rg`.
2. **Clasificación** conforme a la taxonomía de `AUDIT_PROTOCOL.md` §G (NUEVO / REVALIDADO / etc.) y tipología de problema solicitada por el owner.
3. **Severidad** P0..P3 conforme al modelo de severidad §J (impacto técnico y de negocio).
4. **Contraste** contra `tracking.yaml`, ADRs y `docs/audits/` históricos para evitar `CONTROL FAIL ≠ FINDING NUEVO`.
5. **Sin implementación** — solo priorización y propuesta mínima.

Deduplicación L1 aplicada: **G6 y G12 comparten causa raíz** (valores de contexto pasados como argumentos posicionales a loguru → reiterados en `text`, ausentes de `extra`). Se recomienda tratarlos como **un único ítem de backlog**; se documentan por separado por pedido del owner.

---

## Ficha por hallazgo

### G2 — `loki_url` no configurado en ninguna config

1. **Problema:** El `LokiSink` (JSON push → Grafana Loki) existe e implementado, pero la cadena de configuración nunca lo activa: `LoggingConfig.loki_url` default es `None` y ningún YAML de `config/` define `loki_url`. Los logs de máquina se quedan en disco local (`app_*.log`); no hay shipping remoto out-of-the-box.
2. **Archivos/ubicación:** `ocm/observability/logger.py:358-412` (sink dormido, rama `if loki_url:`); `ocm/observability/config.py:43` (default `None`); `ocm/config/structured/observability.py:37` (default `None`); `config/observability/logging.yaml` (sin campo).
3. **Válido en HEAD:** **SÍ** — `rg -n "loki" config/ -i` → 0 matches en commit `0c8b4ff4`.
4. **Tipo:** Problema de configuración.
5. **Impacto:** **P2** — no pérdida funcional (los sinks locales funcionan), pero la retención remota y el análisis cross-run quedan desactivados por omisión. La infraestructura Loki ya existe y está healthy (F-025 cerrado, 2026-08-09).
6. **Valor para OCM:** Habilitar shipping JSON estructurado a Loki permite forense multi-run, consultas por `run_id`/`exchange`/`symbol`, y respaldo a Alertmanager/Grafana sin depender del disco local ni de los jobs de texto de promtail.
7. **Dependencias:** Precede a G3 en la cadena. No depende de ningún hallazgo; su valor depende de que Loki esté desplegado (satisfecho).
8. **Requiere ADR:** No. Es config operativa con diseño fail-soft ya existente.
9. **Corrección mínima:** Añadir en `config/observability/logging.yaml`:
   ```yaml
   observability:
     logging:
       loki_url: ${oc.env:LOKI_URL,http://loki:3100/loki/api/v1/push}
       loki_labels:
         app: orangecashmachine
   ```
   (El sink ya es best-effort y no-bloqueante; no cambia comportamiento de fallo.)
10. **Tests/gates:** `OCM_VALIDATE_ONLY=true uv run python -m app.cli.main` PASS; test de configuración que verifique que `LoggingConfig` acepta `loki_url` con/ sin env var; verificación e2e con docker compose de que labels `env/run_id/service` llegan a Loki.

---

### G3 — Promtail ingiere TEXTO (humano), no el JSON SSOT

1. **Problema:** `promtail.yml` scrapea `orangecashmachine_*.log` y `errors_*.log` —ambos en formato `FILE` texto— mientras que el sink máquina `app_*.log` (`serialize=True`, JSON, "SSOT único para análisis") no se transporta a Loki. La cadena "máquina" queda disconectada: los fields `extra` (run_id, env, service, contexto ligado) se pierden porque nunca se envían como JSON.
2. **Archivos/ubicación:** `deploy/monitoring/promtail/promtail.yml:22-40` (jobs `ocm_app_logs` y `ocm_error_logs`); sink JSON en `ocm/observability/logger.py:319-328`; docstring de `ocm/observability/formats.py:17-22` (JSON canónico).
3. **Válido en HEAD:** **SÍ** — archivo verificado en HEAD.
4. **Tipo:** Problema de configuración (cadena de observabilidad).
5. **Impacto:** **P2** — la información estructurada que OCM produce se degrada a texto plano antes de llegar al query plane.
6. **Valor para OCM:** Que el JSON estructurado llegue a Loki habilita consultas por campos, agregación por `level`/`service`/`run_id`, y alertas sobre campos (no sobre substrings). Cierra la estrategia dual end-to-end.
7. **Dependencias:** Coordinación conceptual con G2 (misma cadena Loki); la infra Loki ya está healthy.
8. **Requiere ADR:** No.
9. **Corrección mínima:** En `promtail.yml`, sustituir/añadir job primario sobre JSON:
   ```yaml
   - job_name: ocm_app_json_logs
     static_configs:
       - targets: [localhost]
         labels: { job: ocm_app_json, env: ${ENV}, __path__: /logs/app_*.log }
   ```
   *(Opcional: conservar `ocm_error_logs` para el job humano.)*
10. **Tests/gates:** Verificación e2e docker compose: stream `ocm_app_json` presente con campo estructurado visible en Loki; validación de sintaxis con `uvx yamllint -c .yamllint`; test de contrato de targets si se añade un guard.

---

### G4 — Campos `format` de config muertos (`text|json`)

1. **Problema:** `LoggingConfig.format` ("text"|"json") existe en los dos esquemas y `production.yaml:22`/`test.yaml:23` declaran `format: json`, pero `_resolve_config()` en `logger.py` **nunca lee `cfg.format`**. La declaración de producción es un no-op: el JSON del `app_*.log` siempre está activo y el texto de `orangecashmachine_*.log` siempre es texto. La superficie de configuración miente: un operador creería que `format: json` cambia la salida.
2. **Archivos/ubicación:** `ocm/observability/config.py:40`; `ocm/config/structured/observability.py:27`; `config/env/production.yaml:22`; `config/env/test.yaml:23`; `config/observability/logging.yaml:9`; consumidor ausente en `ocm/observability/logger.py:150-187`.
3. **Válido en HEAD:** **SÍ** — `rg "cfg.format|resolved\[\"format\"\]|\.format"` en consumidores de `LoggingConfig` = 0.
4. **Tipo:** Problema de configuración / deuda técnica de superficie.
5. **Impacto:** **P3** — sin bug funcional, pero config engañosa y superficie de configuración no verificable.
6. **Valor para OCM:** Honestidad de la superficie de configuración ("toda config tiene efecto" como invariante de gobernanza); menos campos que mantener y que testear.
7. **Dependencias:** Ninguna.
8. **Requiere ADR:** No (no toca arquitectura; tocar ambos esquemas de config es mecánico y sus tests de parity actuales no referencian `format`).
9. **Corrección mínima:** Eliminar el campo:
   - `ocm/observability/config.py` (Pydantic) y `ocm/config/structured/observability.py` (Hydra dataclass).
   - `format: json|text` en `production.yaml:22`, `test.yaml:23`, `logging.yaml:9`.
   Verificado: `tests/ocm/config/test_structured_parity.py` no referencia `format`.
10. **Tests/gates:** `OCM_VALIDATE_ONLY=true ...` PASS; grep gate (`rg "format:" config/` sin hits bajo `observability.logging`); suite de config verde.

---

### G5 — `filters.py` (pipeline_filter/strict_pipeline_filter) huérfano

1. **Problema:** Los filtros fueron diseñados para el sink `pipeline_*.log`, eliminado deliberadamente en v0.2.0 (ver docstring `logger.py:25-30`). Solo `tests/logging/test_filters.py` los importa; ningún sink los usa. Con prefijos de módulo obsoletos (`market_data.ingestion.`, `ocm.control_plane.`). Código muerto con superficie que sugiere una funcionalidad activa que no existe.
2. **Archivos/ubicación:** `ocm/observability/filters.py` (completo); tests: `tests/logging/test_filters.py`.
3. **Válido en HEAD:** **SÍ** — `rg "pipeline_filter|from ocm.observability.filters"` → solo `filters.py`, `test_filters.py` e `__init__.py` (que no lo re-exporta).
4. **Tipo:** Deuda técnica (código muerto).
5. **Impacto:** **P3**.
6. **Valor para OCM:** Claridad del SSOT de observabilidad; elimina el riesgo de que un operador instale un sink "de pipeline" basado en filtros obsoletos.
7. **Dependencias:** Ninguna.
8. **Requiere ADR:** No.
9. **Corrección mínima:** Eliminar `ocm/observability/filters.py` y `tests/logging/test_filters.py`. (Alternativa descartada por YAGNI: reconectar a un sink de alertas nuevo; reintroduce duplicación en disco que se eliminó en v0.2.0.)
10. **Tests/gates:** Grep gate: 0 imports de `ocm.observability.filters`; suite de tests verde; bandit/ruff sin referencias.

---

### G6 — Use cases hornean contexto en el mensaje en vez de `bind()`

1. **Problema:** En loguru, los argumentos posicionales `{}` se formatean **dentro** del campo `text`/`message`. Solo `extra` (de `logger.bind()`) se serializa como claves JSON separadas en `app_*.log` y en el puente Loki. Los use cases pasan `request`/objetos completos como argumentos → `exchange`, `symbol`, `pipeline`, `mode`, `market_type` quedan enterrados en el string humano y no son consultables en el JSON.
2. **Archivos/ubicación:**
   - `packages/market_data/application/use_cases/pipeline_orchestrator.py:239` (`"PipelineOrchestrator.run | {}"`), `:251`, `:256`, `:282`.
   - `packages/market_data/application/use_cases/resample_ohlcv.py:200` (`"ResampleUseCase.execute | {}"`), `:206-209`, `:243-257`.
   - `apps/app/use_cases/execute_live.py:225-226` (`"Engine live listo | {}"` con objeto completo), `execute_paper.py:308-309`.
   - `packages/trading/execution/paper_executor.py:60-69` (order_id/side/symbol/size/qty horneados con formatos `:+.2f`).
   - `apps/app/cli/_bootstrap.py:218-280` (`log_cycle_result` — todos los campos de orden/performance en strings de consola).
   El estándar interno ya normado está en `ohlcv_pipeline.py:325-331` (bind de `error`, `error_type`, `is_transient`).
3. **Válido en HEAD:** **SÍ** — líneas verificadas.
4. **Tipo:** Mejora de observabilidad (semi-estructurado → estructurado).
5. **Impacto:** **P2** — los datos existen pero no son queryables; diagnóstico por `exchange` o `pipeline` requiere grep de substring.
6. **Valor para OCM:** Campos consultables (filtrado/agregación) en `app_*.log` y Loki; correlación `run_id+exchange+symbol`; consistencia con el patrón ejemplar ya existente.
7. **Dependencias:** Ninguna técnica (el backend ya soporta `extra`).
8. **Requiere ADR:** No — es el patrón ya establecido en la propia codebase.
9. **Corrección mínima:** Sustituir argumentos posicionales por `logger.bind(exchange=..., symbol=..., pipeline=..., mode=..., run_id=...).info("event_name_snake_case")` en los sitios listados. Mantener campos en snake_case.
10. **Tests/gates:** Tests por sitio capturando `record["extra"]` y verificando claves; alternativa pragmática: snapshot/línea JSON del sink `app_*.log`; gate de linter (opcional) que exija `bind` en use cases.

---

### G8 — BC trading sin métricas de negocio (0 instrumentación)

1. **Problema:** `packages/trading/` no tiene ningún puerto ni adaptador de métricas: `rg` en HEAD = **0** imports de `MetricsPort`, `Counter`, `Gauge`, `Histogram`, `push_metrics`. Las métricas numéricas de trading (órdenes, fills, P&L, latencia de ciclo, OMS) solo existen como texto en logs (`log_cycle_result`, `paper_executor`, OMS). `ocm/observability/metrics.py:9-14` es un placeholder con TODO v0.2.0 sin implementar. Imposible construir dashboards/alertas/SLOs de ejecución.
2. **Archivos/ubicación:** `packages/trading/` (estructura actual: `analytics`, `bootstrap`, `execution`, `risk`, `strategies` — sin `infrastructure/observability/`); `ocm/observability/metrics.py:9-14` (placeholder); números horneados en `apps/app/cli/_bootstrap.py:218-280`, `packages/trading/execution/paper_executor.py:60-69`.
3. **Válido en HEAD:** **SÍ** — estructura y grep verificados en HEAD. Matiz: `live_executor` está parcialmente stub (tracking B-24/ADR-0016; bloque CCXT comentado), por lo que la relevancia operativa inmediata es menor, pero el gap estructural es real y pre-existe al desbloqueo del path live.
4. **Tipo:** Mejora de arquitectura (DIP/ports) + mejora de observabilidad. **Marcado parcialmente como "requiere investigación"** para la taxonomía de métricas y el estado real del path live.
5. **Impacto:** **P1** (para live: sin alertas de órdenes rechazadas, divergencia de fills, P&L ni latencia; forense de decisiones de capital). Hoy, con ejecución paper/stub en el path activo, impacto operativo inmediato **P2**.
6. **Valor para OCM:** Dashboards Grafana de ejecución, alertas de salud (rechazos, divergencias), SLO de latencia de ciclo, forense de P&L y decisiones de capital — el mismo nivel instrumental que ya tiene market_data.
7. **Dependencias:** Ninguna hard. Enriquecible después de G6/G10 (mismas capas de uso). Contexto: ADR-0016 (live executor), G-09 histórico (métricas live).
8. **Requiere ADR:** No obligatorio — replica el patrón de ports/adapters ya normado en market_data y coherente con ADR-0005/0007. Recomendable una **Decisión Humana (D-XXX)** para aprobar la taxonomía de métricas `ocm_trading_*` si cruza BC (shared/contracts/boundaries). Enfocado: investigación previa (ítem 4).
9. **Corrección mínima:**
   - Nuevo `packages/trading/ports/outbound/metrics.py` con `TradingMetricsPort` (order_submitted/filled/rejected, position_opened, pnl_observed, cycle_latency_observe) + `NullTradingMetrics`.
   - Nuevo `packages/trading/infrastructure/observability/metrics.py` con counters/histograms `ocm_trading_*`.
   - Adapter + wiring en `packages/trading/bootstrap/composition_root.py`; llamadas en `engine.run_once()`, `oms.py`, `executor`.
   - Cumplir import-linter BC-05 (dominio no importa infra).
10. **Tests/gates:** Test unitario con `prometheus_client.REGISTRY` real: `run_once()` incrementa counters; test de composition root que el port inyectado no es `NullTradingMetrics` en producción; gate de presencia de `ocm_trading_*` en `/metrics`.

---

### G10 — Se pierden tracebacks en use cases de trading

1. **Problema:** Los catch-all de `execute_live`/`execute_paper` capturan `except Exception` y loguean solo `"{} -- {}"` (tipo+str) **sin** `opt(exception=True)`. El stack completo se pierde; solo queda el mensaje. En un ciclo live (capital real), el diagnóstico de un fallo de OMS/executor queda a medias.
2. **Archivos/ubicación:** `apps/app/use_cases/execute_live.py:217-223`, `:246-252`, `:256-265`; `apps/app/use_cases/execute_paper.py:304-306`, `:311-315`, `:317-326`.
3. **Válido en HEAD:** **SÍ** — líneas verificadas.
4. **Tipo:** Mejora de observabilidad/diagnóstico.
5. **Impacto:** **P2** — los fallos se reportan pero no son debuggeables remotamente sin consola interactiva.
6. **Valor para OCM:** Stack exacto en `app_*.log`/Loki para diagnosis de fallos de ejecución; requisito previo para un on-call eficiente en live.
7. **Dependencias:** Ninguna; complementario a G6/G12 (misma capa).
8. **Requiere ADR:** No.
9. **Corrección mínima:** Añadir `.opt(exception=True)` al logger en los 6 catch-all, conservando `error`/`error_type` como campos bind.
10. **Tests/gates:** Test unitario capturando el record de loguru y verificando `record["exception"]` no nulo en paths de error de `execute_*`; o gate `rg "opt(exception=True)"` en `apps/app/use_cases/`.

---

### G11 — ResampleUseCase pierde tipo y traceback en errores

1. **Problema:** En este use case de market_data los errores se loguean `"... err={}"` con `exc` horneado, sin `opt(exception=True)` ni `error_type` como campo, y se retorna `error=str(exc)` perdiendo el tipo de excepción. No es posible distinguir transitorio/fatal en el log ni reconstruir el origen.
2. **Archivos/ubicación:** `packages/market_data/application/use_cases/resample_ohlcv.py:205-217`, `:243-257`, y paths de ejecución subsiguientes (+~:260-280).
3. **Válido en HEAD:** **SÍ** — líneas verificadas.
4. **Tipo:** Mejora de observabilidad.
5. **Impacto:** **P2** — afecta diagnóstico de calidad/resampling (gap de datos).
6. **Valor para OCM:** Categorización consultable de errores de resample (`error_type`), correlación con `exchange`/`market_type` ya bindeables; diagnóstico de fallos de calidad.
7. **Dependencias:** Ninguna; comparte archivo con G6 (posible single-pass).
8. **Requiere ADR:** No.
9. **Corrección mínima:** En cada `except`: `logger.bind(exchange=..., market_type=..., error_type=type(exc).__name__, error=str(exc)).opt(exception=True).error("ResampleUseCase.<fase>_error")`.
10. **Tests/gates:** Test unitario del path de fallo verificando `extra.error_type` presente y `record["exception"]` con traceback.

---

### G12 — Tipos de excepción horneados como TEXTO, no como campo `error_type`

1. **Problema:** En los logs de error de la capa de aplicación, el tipo de excepción aparece como substring del mensaje (`"... | {} -- {}"` con `type(exc).__name__`), no como campo `extra`. No es filtrable/agrupable por categoría de error en JSON/Loki. **Causa raíz compartida con G6** (valores como argumentos posicionales).
2. **Archivos/ubicación:** `apps/app/use_cases/execute_live.py:217-252`; `execute_paper.py:304-326`; `packages/market_data/application/use_cases/pipeline_orchestrator.py:256,282`; `resample_ohlcv.py:205-257`. Contrapunto normativo: `ohlcv_pipeline.py:325-331` (bind `error_type`).
3. **Válido en HEAD:** **SÍ**.
4. **Tipo:** Mejora de observabilidad (overlap con G6 — candidato a merge L1).
5. **Impacto:** **P2** (en conjunto con G6).
6. **Valor para OCM:** Agrupación/filtrado de errores por `error_type` en Loki/Grafana; correlación con `run_id`; reducción de ruido en troubleshooting multi-service.
7. **Dependencias:** G6 (raíz común); G10/G11 fijan sitios concretos.
8. **Requiere ADR:** No.
9. **Corrección mínima:** Unificar con G6 en un único ticket de "estructuración de contexto en capa de aplicación": adoptar `bind(error_type=..., error=...)` + `opt(exception=True)` como norma.
10. **Tests/gates:** Gate compartido con G6/G10/G11: tests de captura que verifican `error_type` en `extra` para paths de error; opcional AST guard en `scripts/` exigiendo `bind` + `opt(exception=True)` en `except Exception`.

---

## Tabla maestra

| ID | Problema | Estado actual | Severidad | Valor para OCM | Dependencias | ADR necesario | Acción recomendada |
|---|---|---|---|---|---|---|---|
| G2 | `loki_url` no configurado (LokiSink dormido) | NUEVO · válido en HEAD | P2 | Shipping JSON a Loki: forense cross-run, consultas por run_id/exchange | Precede a G3 | No | Config `loki_url` + labels en `logging.yaml` |
| G3 | Promtail scrapea TEXTO, no `app_*.log` JSON | NUEVO · válido en HEAD | P2 | Que el SSOT JSON llegue al query plane | Coordinación con G2; infra Loki healthy | No | Scrape job sobre `app_*.log` |
| G4 | Campo `format` muerto (`text\|json`) en 2 esquemas + prod/test | NUEVO · válido en HEAD | P3 | Superficie de configuración honesta y verificable | Ninguna | No | Eliminar campo en schemas y YAMLs |
| G5 | `filters.py` huérfano (pipe filter eliminado en v0.2.0) | NUEVO · válido en HEAD | P3 | Claridad del SSOT; sin superficie engañosa | Ninguna | No | Eliminar archivo + tests |
| G6 | Use cases hornean contexto en el mensaje (no `bind`) | NUEVO · válido en HEAD | P2 | Campos consultables en `app_*.log`/Loki | Ninguna | No | `logger.bind(...)` en sitios listados |
| G8 | BC trading sin métricas (0 instrumentación) | REVALIDADO parcial (G-09 histórico) · válido en HEAD · **requiere investigación** (taxonomía + live path) | P1 hoy / P2 con path stub | Alertas/dashboards/SLOs de ejecución; forense de capital | G6/G10 (misma capa); contexto ADR-0016 | No obligatorio; **Decisión Humana** recomendada | Port `TradingMetricsPort` + adapters + wiring |
| G10 | Tracebacks perdidos en catch-all de `execute_live/paper` | NUEVO · válido en HEAD | P2 | Stack en logs para debug de fallos de capital | Complementario a G6 | No | `.opt(exception=True)` en 6 catch-all |
| G11 | `ResampleUseCase` pierde tipo/traceback en errores | NUEVO · válido en HEAD | P2 | Diagnóstico de calidad/resample por `error_type` | Comparte archivo con G6 | No | `bind(error_type=...)` + `opt(exception=True)` |
| G12 | Tipos de excepción como TEXTO, no campo `error_type` | NUEVO · válido en HEAD | P2 | Agrupación de errores en Loki/Grafana | G6 (raíz común); G10/G11 | No | Merge con G6: norma `bind+exception` |

---

## Prioridad para el siguiente ciclo de trabajo

Orden propuesto por **valor ÷ esfuerzo × dependencias** (quick wins primero, ítem estratégico con investigación programada):

1. **G6 + G12 (unificados)** — quick win P2: estructuración de contexto en use cases (market_data + trading). Alto valor de diagnóstico, esfuerzo pequeño, patrón ya normado en la codebase (`ohlcv_pipeline.py:325`). Se puede acompañar del gate de tests por sitio.
2. **G2 → G3** — activación de la cadena Loki (config + promtail): desbloquea la mitad "máquina remota" de la estrategia dual ya implementada pero dormida. G2 primero (config), G3 después (transporte del JSON).
3. **G10** — tracebacks en use cases trading: 6 sitios, 1 línea cada uno; cierra el diagnóstico remoto de fallos de ejecución.
4. **G11** — diagnóstico de errors en resample: 1 archivo, single-pass junto a G6 en el mismo archivo.
5. **G8** — métricas de negocio trading: ítem estratégico P1 programado tras los quick wins, con **fase de investigación primero** (taxonomía `ocm_trading_*`, estado real del path live/ADR-0016, bounded-boundary si aplica) y **Decisión Humana** para la taxonomía. No debe empezar sin ese research.
6. **G4** — dead-config `format`: higiene de configuración; trivial y sin riesgo (verificado que ningún test lo consume).
7. **G5** — código muerto `filters.py`: limpieza; trivial.

---

## Notas de gobernanza

- **Siguiente paso formal:** registrar los hallazgos nuevos (G2, G3, G4, G5, G6, G10, G11, G12 y la extensión de G-09/G8) en `docs/plans/tracking.yaml` **solo tras decisión humana**, conforme a la cadena de adopción del `AUDIT_PROTOCOL.md` §B (§ not modified en esta fase por instrucción explícita del owner).
- **`requires investigation` aplica únicamente a G8** (taxonomía de métricas y estado real del path live). El resto tiene evidencia suficiente en HEAD.
- **Sin hallazgos falsos ni resueltos** en el conjunto G2–G6/G8/G10–G12 en el commit `0c8b4ff4`.
- Read-only respetado: única escritura en `docs/audits/`. Working tree con cambios pre-existentes intactos.