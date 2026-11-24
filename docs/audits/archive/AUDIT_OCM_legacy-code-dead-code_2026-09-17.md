# AUDITORÍA — LEGACY CODE / DEAD CODE / OBSOLETE ARTIFACTS
# OrangeCashMachine (OCM)

- **Fecha:** 2026-09-17
- **Modo:** READ-ONLY / FORENSE / INVENTARIO / PLANIFICACIÓN
- **Estado de Git durante la auditoría:** rama `feat/adr0028-bookbuilder`, HEAD `cb6c6d7c`.
- **Working tree pre-existente:** `M docs/plans/tracking.yaml` (preservado), 3 archivos de auditoría de otro agente (preservados).
- **Protocolo:** AUDIT_PROTOCOL v2.1

> **NOTA DE RECONCILIACIÓN (2026-09-18):** Este documento fue contrastado contra
> `AUDIT_OCM_reconciliacion-IN-01-06_2026-09-18.md` (IN-02/IN-03). Correcciones
> aplicadas en esta fecha:
>
> 1. **IN-02 (conteos):** la base correcta es la **matriz L-01..L-40** (40 filas):
>    **18 CONFIRMADO_DEAD, 2 PROBABLE, 19 POSIBLE, 1 EN_USO (L-30 signum)**. El
>    total "67 / 8 CONFIRMADO_DEAD" de §2 usa otra base (suma por categorías, no
>    filas de la matriz); §9 quedó alineado con la matriz (ver §9).
> 2. **IN-03 (`.env`):** el aserto "`.env` commiteado" era FALSO — `.env` nunca
>    fue trackeado (`git ls-files`/`git log --all -- .env` vacíos), está en
>    `.gitignore:12` y en disco con chmod 600. La exposición versionada real es
>    `deploy/host.env` (topología, H-DEP-01), sin secretos. F-LEGACY-001
>    reclasificado (ver §4); `git rm --cached .env` es inaplicable.

---

## REPRODUCIBILIDAD

```
commit: cb6c6d7c0fe0b8702534df6446b5f6f1c872ff5b
branch: feat/adr0028-bookbuilder
fecha: 2026-09-17
protocolo: AUDIT_PROTOCOL v2.1
agente/modelo: OpenCode / mimo-v2-free
herramientas: ruff, vulture, grep, git, import-linter
comandos: ruff check, vulture, grep, git log, git diff
golden: N/A (auditoría de legacy, no de arquitectura)
resultado: N/A (sin validador de auditoría legacy específico)
```

---

## 1. OBJETIVO DE LA AUDITORÍA

Realizar una auditoría integral del repositorio OCM para identificar:
1. Código legacy
2. Código muerto / dead code
3. Módulos sin referencias
4. Entry points obsoletos
5. Implementaciones duplicadas o reemplazadas
6. Compatibilidad heredada que ya no sea necesaria
7. Scripts legacy
8. Configuraciones legacy
9. Docker/Compose legacy
10. Archivos .bak/.old/.tmp u otros artefactos obsoletos
11. Documentación que describe arquitectura ya reemplazada
12. Tests de componentes eliminados
13. ADRs o planes que quedaron obsoletos
14. APIs internas duplicadas
15. Código perteneciente a arquitecturas anteriores
16. Dependencias Python aparentemente ya no utilizadas
17. Configuraciones o rutas que apuntan a componentes eliminados
18. Código que parece muerto pero que en realidad tiene uso indirecto

---

## 2. RESUMEN EJECUTIVO

### Inventario General

| Categoría | Candidatos | CONFIRMADO_DEAD | PROBABLE_LEGACY | POSIBLE_LEGACY | EN_USO | NO_DETERMINABLE |
|-----------|------------|-----------------|-----------------|----------------|--------|-----------------|
| Código Python | 6 | 2 | 4 | 0 | 0 | 0 |
| Configuración | 13 | 1 | 4 | 8 | 0 | 0 |
| Deployment | 8 | 1 | 2 | 5 | 0 | 0 |
| Dependencias | 11 | 4 | 4 | 3 | 0 | 0 |
| Tests | 5 | 0 | 1 | 4 | 0 | 0 |
| Documentación | 9 | 0 | 2 | 7 | 0 | 0 |
| Archivos raíz | 8 | 0 | 5 | 3 | 0 | 0 |
| Duplicación arquitectónica | 7 | 0 | 1 | 6 | 0 | 0 |
| **TOTAL** | **67** | **8** | **23** | **36** | **0** | **0** |

> **NOTA (reconciliación 2026-09-18):** la tabla anterior suma candidatos por
> categoría y NO coincide con la matriz L-01..L-40 (40 filas, §3). Estados reales
> de la matriz: **18 CONFIRMADO_DEAD, 2 PROBABLE, 19 POSIBLE, 1 EN_USO (L-30)**.
> La contradicción entre §2/§9 fue corregida en §9 (ver IN-02).

### Hallazgos Principales

1. **RECHECK (reclasificado 2026-09-18):** `.env` tiene secretos reales en disco, pero **nunca fue commiteado** (IN-03) — la exposición versionada es `deploy/host.env` (topología). Ver F-LEGACY-001.
2. **HIGH:** 4 dependencias declaradas pero nunca importadas (`duckdb`, `lz4`, `aiometer`, `pybreaker`)
3. **MEDIUM:** Documentación `DOMAIN.md` referencia componentes eliminados (`FillHandler`, `TradeHistory`)
4. **MEDIUM:** Archivos `.bak` y `.swp` en workspace (no trackeados, pero confusos)
5. **LOW:** Directorio `infrastructure/` vacío (legacy skeleton)
6. **LOW:** Variables de entorno sin consumidores en `.env`

---

## 3. MATRIZ DE CANDIDATOS

| ID | Archivo/Símbolo | Tipo | Evidencia | Consumidor | Estado | Acción propuesta |
|----|-----------------|------|-----------|------------|--------|------------------|
| L-01 | `infrastructure/__init__.py` | DEAD_CODE | Directorio vacío, solo `__init__.py` con docstring | Ninguno | CONFIRMADO_DEAD | ELIMINAR |
| L-02 | `duckdb` (pyproject.toml) | LEGACY_DEPENDENCY | `rg "import duckdb"` → 0 resultados | Ninguno | CONFIRMADO_DEAD | ELIMINAR |
| L-03 | `lz4` (pyproject.toml) | LEGACY_DEPENDENCY | `rg "import lz4"` → 0 resultados | Ninguno | CONFIRMADO_DEAD | ELIMINAR |
| L-04 | `aiometer` (pyproject.toml) | LEGACY_DEPENDENCY | `rg "import aiometer"` → 0 resultados | Ninguno | CONFIRMADO_DEAD | ELIMINAR |
| L-05 | `pybreaker` (pyproject.toml) | LEGACY_DEPENDENCY | `rg "import pybreaker"` → 0 resultados | Ninguno | CONFIRMADO_DEAD | ELIMINAR |
| L-06 | `.env` (secrets) | LEGACY_CONFIG | Secretos reales en disco, **NUNCA commiteado** (IN-03: `.gitignore:12`, chmod 600, git vacío) | Runtime | CONFIRMADO_DEAD (vars legacy) | ROTAR si se expone + revisar `deploy/host.env` |
| L-07 | `docker-compose.yml.bak` | WORKSPACE_ARTIFACT | Backup con config antigua | Ninguno | CONFIRMADO_DEAD | ELIMINAR |
| L-08 | `pyproject.toml.bak` | WORKSPACE_ARTIFACT | Backup con merge conflict | Ninguno | CONFIRMADO_DEAD | ELIMINAR |
| L-09 | `docs/DOMAIN.md:87-88` | LEGACY_DOC | Referencia `FillHandler`/`TradeHistory` eliminados | Ninguno | PROBABLE_LEGACY | DOCUMENTAR |
| L-10 | `docs/DOMAIN.md:142` | LEGACY_DOC | Referencia decisión ya resuelta | Ninguno | PROBABLE_LEGACY | DOCUMENTAR |
| L-11 | `storage/gold/feature_engineer.py` | LEGACY_CODE | Shim deprecated, tests usan `GoldTransformer` | Tests | POSIBLE_LEGACY | INVESTIGAR |
| L-12 | `domain/policies/base.py` classify_error | DUPLICATE | Re-export de `pipeline/runtime.py` | Importers | POSIBLE_LEGACY | MIGRAR |
| L-13 | `OCM_DATA_LAKE_PATH` | LEGACY_CONFIG | Marcada DEPRECATED en `env_vars.py` | `paths.py` | POSIBLE_LEGACY | MANTENER |
| L-14 | `config/risk/risk.yaml` | LEGACY_CONFIG | "valores referencia — no se aplican en runtime" | Hydra | POSIBLE_LEGACY | MANTENER |
| L-15 | `config/features.yaml` | LEGACY_CONFIG | Flags sin consumidores Python | Hydra | POSIBLE_LEGACY | INVESTIGAR |
| L-16 | `deploy/monitoring/grafana/` | LEGACY_DEPLOYMENT | Directorios vacíos | Docker | POSIBLE_LEGACY | POBLAR |
| L-17 | Zookeeper (docker-compose) | LEGACY_DEPLOYMENT | KRaft disponible desde Kafka 3.3+ | Docker | POSIBLE_LEGACY | MIGRAR |
| L-18 | `tests/features/test_feature_engineer*.py` | LEGACY_TEST | Nombres antiguos, tests correctos | pytest | POSIBLE_LEGACY | RENOMBRAR |
| L-19 | `docs/PLAN-Maestro-Ingenieria.md.bak` | WORKSPACE_ARTIFACT | Merge conflict sin resolver | Ninguno | CONFIRMADO_DEAD | ELIMINAR |
| L-20 | `docs/audits/*.bak` (3 archivos) | WORKSPACE_ARTIFACT | Backups de auditorías | Ninguno | CONFIRMADO_DEAD | ELIMINAR |
| L-21 | `.docs.swp` | WORKSPACE_ARTIFACT | Vim swap file | Ninguno | CONFIRMADO_DEAD | ELIMINAR |
| L-22 | `feedhandler.log` | WORKSPACE_ARTIFACT | Log de cryptofeed | Ninguno | CONFIRMADO_DEAD | ELIMINAR |
| L-23 | `OLLAMA_HOST` (.env) | LEGACY_CONFIG | Sin consumidores Python | Ninguno | CONFIRMADO_DEAD | ELIMINAR |
| L-24 | `KAFKA_MAX_REQUEST_SIZE` (.env) | LEGACY_CONFIG | Sin consumidores Python | Ninguno | CONFIRMADO_DEAD | ELIMINAR |
| L-25 | `METRICS_PORT` | LEGACY_CONFIG | En YAML pero no en `env_vars.py` SSOT | Hydra | POSIBLE_LEGACY | AGREGAR |
| L-26 | `docker-compose.override.yml` | LEGACY_DEPLOYMENT | Bloques comentados, env var sin consumidor | Docker | POSIBLE_LEGACY | LIMPIAR |
| L-27 | `ccxt` comment (pyproject.toml) | LEGACY_CONFIG | Comentario dice "pinneado 4.3.58" pero rango es `>=4.5.74` | Ninguno | CONFIRMADO_DEAD | ACTUALIZAR |
| L-28 | `pyproject.toml` description | LEGACY_DOC | Dice "DuckDB" pero no se importa | Ninguno | CONFIRMADO_DEAD | ACTUALIZAR |
| L-29 | `sphinx` deps (pyproject.toml) | LEGACY_DEPENDENCY | Sin `docs/` con Sphinx conf.py | Ninguno | CONFIRMADO_DEAD | ELIMINAR |
| L-30 | `signum` variables (bootstrap.py) | DEAD_CODE | Parámetros de signal handler, requeridos por API | Python | EN_USO | MANTENER |
| L-31 | `skip_versioning` (iceberg_storage) | DEAD_CODE | Parámetro en firma pero no usado en cuerpo | Ninguno | POSIBLE_LEGACY | INVESTIGAR |
| L-32 | `until` (iceberg_storage) | DEAD_CODE | Parámetro en firma pero no usado en cuerpo | Ninguno | POSIBLE_LEGACY | INVESTIGAR |
| L-33 | Hydra schema files | LEGACY_CONFIG | Defaults referencian schemas inexistentes | Hydra | POSIBLE_LEGACY | CREAR |
| L-34 | Alerting webhook vars (.env.example) | LEGACY_CONFIG | Comentadas, sin consumidores | Ninguno | CONFIRMADO_DEAD | ELIMINAR |
| L-35 | `risk/risk.yaml` | LEGACY_CONFIG | Documentado como "referencia" | Hydra | POSIBLE_LEGACY | MANTENER |
| L-36 | `docker-compose.yml` trading/portfolio | LEGACY_DEPLOYMENT | "SCAFFOLDING FUTURO — NO EJECUTABLE" | Docker | POSIBLE_LEGACY | MANTENER |
| L-37 | `health_check.sh` orderbook.raw | LEGACY_DEPLOYMENT | Referencia topic puede no existir | Scripts | POSIBLE_LEGACY | CONDICIONAL |
| L-38 | Prometheus config | LEGACY_DEPLOYMENT | Sin scrape de app metrics | Docker | POSIBLE_LEGACY | DOCUMENTAR |
| L-39 | `vulture` (pyproject.toml dev) | LEGACY_DEPENDENCY | En dev deps pero sin evidencia de uso en CI | CI | POSIBLE_LEGACY | VERIFICAR |
| L-40 | `trading.risk.risk.yaml` | LEGACY_CONFIG | Valores referencia sin apply | Hydra | POSIBLE_LEGACY | MANTENER |

---

## 4. HALLAZGOS DETALLADOS

### F-LEGACY-001: `.env` con Secretos en Disco — NO Commiteado (RECLASIFICADO 2026-09-18, de CRITICAL → RECHECK)

**Ubicación:** `.env` (raíz del repo, solo en disco local)
**Evidencia:** Líneas 12-13 (`BYBIT_API_KEY`, `BYBIT_API_SECRET`), L26 (`GRAFANA_PASSWORD`), L58 (`OCM_API_JWT_SECRET`), L61-62 (`KAFKA_UI_PASSWORD`), L62 (`OLLAMA_HOST`)
**Impacto original (hoy revisado):** el hallazgo afirmaba fuga de credenciales por commit; **es falso** (IN-03: `git ls-files .env` y `git log --all -- .env` vacíos; `.gitignore:12`; chmod 600 en disco). No hay fuga vía Git. La exposición versionada es de **topología** (`deploy/host.env` tracked, H-DEP-01), no de secretos.
**Recomendación actualizada:** no aplica `git rm --cached .env` (no está en el índice). Riesgo residual: secretos en disco local con permisos (600 OK) y `deploy/host.env` versionado (topología, sin secretos — deployment-portability §9). Rotar credenciales sigue siendo buena práctica defensiva, no urgente.
**Categoría de evidencia:** RECLASIFICADO (aserto "commiteado" refutado por git)
**Nivel de certeza:** ALTO

### F-LEGACY-002: 4 Dependencias Nunca Importadas (HIGH)

**Ubicación:** `pyproject.toml:109-110,95,100`
**Evidencia:**
- `duckdb>=1.5.1,<2.0` → `rg "import duckdb"` → 0 resultados
- `lz4>=4.4.5` → `rg "import lz4"` → 0 resultados
- `aiometer>=0.10.0` → `rg "import aiometer"` → 0 resultados
- `pybreaker==1.4.1` → `rg "import pybreaker"` → 0 resultados (comentario dice "used deliberately" pero no lo está)
**Impacto:** Dependencias innecesarias aumentan tiempo de instalación, superficie de ataque, y maintenance burden.
**Recomendación:** Eliminar las 4 dependencias.
**Categoría de evidencia:** CONFIRMADO
**Nivel de certeza:** ALTO

### F-LEGACY-003: Documentación DOMAIN.md Referencia Componentes Eliminados (MEDIUM)

**Ubicación:** `docs/DOMAIN.md:87-88,142`
**Evidencia:**
- L87-88: Tabla §4 lista `FillHandler` y `TradeHistory` como protocols en `boundaries.py`
- L142: "Decidir si `RiskGate`/`FillHandler`/`TradeHistory` se implementan de verdad o se eliminan"
- ADR-0009 (aceptado) eliminó ambos de `boundaries.py`
- `boundaries.py` actual solo contiene `FeatureSource`, `SignalProtocol`, `RiskGate`, `RebalancePort`
**Impacto:** Desarrolladores nuevos siguen documentación incorrecta.
**Recomendación:** Actualizar DOMAIN.md §4 y §6 para reflejar estado actual.
**Categoría de evidencia:** CONFIRMADO
**Nivel de certeza:** ALTO

### F-LEGACY-004: Directorio `infrastructure/` Vacío (LOW)

**Ubicación:** `infrastructure/__init__.py`
**Evidencia:** Solo contiene docstring. Sin código funcional. Remnant de arquitectura anterior.
**Impacto:** Ruido en el repositorio.
**Recomendación:** Eliminar directorio completo.
**Categoría de evidencia:** CONFIRMADO
**Nivel de certeza:** ALTO

### F-LEGACY-005: Archivos `.bak` y `.swp` en Workspace (LOW)

**Ubicación:** Raíz del repo y `docs/`
**Evidencia:**
- `pyproject.toml.bak` — backup con merge conflict
- `docker-compose.yml.bak` — backup con config antigua
- `docs/PLAN-Maestro-Ingenieria.md.bak` — backup con merge conflict sin resolver
- `docs/audits/*.bak` (3 archivos) — backups de auditorías
- `.docs.swp` — Vim swap file
- `feedhandler.log` — log de cryptofeed
**Impacto:** Confusión, occupancy de espacio.
**Recomendación:** Eliminar todos (no están trackeados en Git).
**Categoría de evidencia:** CONFIRMADO
**Nivel de certeza:** ALTO

### F-LEGACY-006: `duckdb` Referenciado en Descripción de Proyecto (LOW)

**Ubicación:** `pyproject.toml:27`
**Evidencia:** Descripción dice "data lakehouse Iceberg/DuckDB" pero `duckdb` nunca se importa.
**Impacto:** Descripción inexacta.
**Recomendación:** Actualizar descripción.
**Categoría de evidencia:** CONFIRMADO
**Nivel de certeza:** ALTO

### F-LEGACY-007: Comentario `ccxt` Stale en pyproject.toml (LOW)

**Ubicación:** `pyproject.toml:87`
**Evidencia:** Comentario dice "pinneado en 4.3.58" pero especificador es `>=4.5.74` (rango abierto).
**Impacto:** Confusión sobre versión real.
**Recomendación:** Actualizar comentario.
**Categoría de evidencia:** CONFIRMADO
**Nivel de certeza:** ALTO

### F-LEGACY-008: `sphinx` Dependencies Sin Uso (LOW)

**Ubicación:** `pyproject.toml:138-141`
**Evidencia:** Opcional deps `docs` declaradas pero sin `docs/` con Sphinx conf.py o archivos RST.
**Impacto:** Dependencias innecesarias.
**Recomendación:** Eliminar o crear documentación Sphinx.
**Categoría de evidencia:** CONFIRMADO
**Nivel de certeza:** ALTO

### F-LEGACY-009: Variables `.env` Sin Consumidores (LOW)

**Ubicación:** `.env`, `.env.example`
**Evidencia:**
- `OLLAMA_HOST` — 0 consumidores Python
- `KAFKA_MAX_REQUEST_SIZE` — 0 consumidores Python
- Alerting webhooks (comentados) — 0 consumidores
**Impacto:** Configuración obsoleta.
**Recomendación:** Eliminar variables sin consumidores.
**Categoría de evidencia:** CONFIRMADO
**Nivel de certeza:** ALTO

### F-LEGACY-010: Grafana Dashboards Vacíos (MEDIUM)

**Ubicación:** `deploy/monitoring/grafana/provisioning/`, `deploy/monitoring/grafana/dashboards/`
**Evidencia:** Docker-compose monta estos directorios pero están vacíos (0 archivos). `GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH` apunta a `/var/lib/grafana/dashboards/ocm_pipeline.json` que no existe.
**Impacto:** Grafana arranca sin dashboards ni provisioning.
**Recomendación:** Poblar directorios o eliminar mounts.
**Categoría de evidencia:** CONFIRMADO
**Nivel de certeza:** ALTO

### F-LEGACY-011: `feature_engineer.py` Shim Deprecated (LOW)

**Ubicación:** `packages/market_data/infrastructure/storage/gold/feature_engineer.py`
**Evidencia:** Shim documentado como "compatibilidad backward hasta v3.0.0". Tests ya usan `GoldTransformer` exclusivamente.
**Impacto:** Dead weight antes de v3.0.0.
**Recomendación:** Evaluar eliminación antes de v3.0.0.
**Categoría de evidencia:** CONFIRMADO
**Nivel de certeza:** MEDIO

### F-LEGACY-012: `classify_error()` Re-export en `domain/policies/base.py` (LOW)

**Ubicación:** `packages/market_data/domain/policies/base.py`
**Evidencia:** Re-export de `application/pipeline/runtime.py`. Código dice: "se eliminará esta al completar la migración de todos los importadores".
**Impacto:** Duplicación conocida, migración pendiente.
**Recomendación:** Completar migración de importadores.
**Categoría de evidencia:** CONFIRMADO
**Nivel de certeza:** ALTO

### F-LEGACY-013: Hydra Schema Files Inexistentes (MEDIUM)

**Ubicación:** `config/config.yaml:19-20`
**Evidencia:** Defaults list `- pipeline: schema` y `- observability: schema` pero no existen archivos `config/pipeline/schema.yaml` ni `config/observability/schema.yaml`.
**Impacto:** Hydra sin validación de tipos para estas secciones.
**Recomendación:** Crear schemas o eliminar defaults.
**Categoría de evidencia:** CONFIRMADO
**Nivel de certeza:** ALTO

### F-LEGACY-014: Zookeeper Legacy para Kafka (LOW)

**Ubicación:** `docker-compose.yml:473-493`
**Evidencia:** Zookeeper usado como metadata coordinator. Kafka 7.6.1 soporta KRaft. Compose file nota: "En producción considerar KRaft mode".
**Impacto:** Overhead operacional innecesario.
**Recomendación:** Planificar migración a KRaft.
**Categoría de evidencia:** CONFIRMADO
**Nivel de certeza:** ALTO

### F-LEGACY-015: Trading/Portfolio Services Scaffolding (LOW)

**Ubicación:** `docker-compose.yml:381-466`
**Evidencia:** Marcados como "SCAFFOLDING FUTURO — NO EJECUTABLE". Detrás de `[microservices]` profile.
**Impacto:** Maintenance burden sin funcionalidad.
**Recomendación:** Mantener hasta confirmar extracción a microservicios.
**Categoría de evidencia:** CONFIRMADO
**Nivel de certeza:** ALTO

---

## 5. CANDIDATOS QUE NO DEBEN ELIMINARSE TODAVÍA

| ID | Archivo/Símbolo | Razón | Dependencia |
|----|-----------------|-------|-------------|
| L-11 | `feature_engineer.py` | Shim deprecated pero aún importable | Backward compat hasta v3.0.0 |
| L-12 | `classify_error()` re-export | Migración pendiente de importadores | `application/pipeline/runtime.py` |
| L-13 | `OCM_DATA_LAKE_PATH` | Deprecación controlada con fallback | `ocm/config/paths.py` |
| L-14 | `risk/risk.yaml` | Documentado como "valores referencia" | Hydra config |
| L-15 | `features.yaml` | Flags pueden ser consumidos por Hydra | `ocm/config/` |
| L-17 | Zookeeper | KRaft migration requiere planificación | Kafka version |
| L-18 | Test filenames | Tests correctos, solo nombres antiguos | `GoldTransformer` |
| L-25 | `METRICS_PORT` | En YAML pero no en SSOT — gap menor | Hydra |
| L-26 | `docker-compose.override.yml` | Local dev convenience | Developer workflow |
| L-33 | Hydra schema files | Puede ser intencional (sin validación) | Hydra |
| L-36 | Trading/portfolio services | Detrás de profile, scaffolding futuro | Microservice extraction |
| L-37 | `health_check.sh` orderbook | Puede ser condicional | `datasets.orderbook` |
| L-38 | Prometheus config | Push model es diseño válido | Pushgateway |
| L-39 | `vulture` dev dep | Puede ser usado manualmente | Developer workflow |
| L-40 | `risk.yaml` | Valores referencia, no runtime | Trading maturity |
| L-30 | `signum` variables | Requeridas por Python signal API | `signal.signal()` |
| L-31/32 | `skip_versioning`, `until` | Pueden ser usados por overrides | Subclasses |

---

## 6. PLAN DE LIMPIEZA POR FASES

### FASE 1: Artefactos Accidentales (RIESGO: BAJO)
**Candidatos:** L-07, L-08, L-19, L-20, L-21, L-22
**Acción:** Eliminar archivos `.bak`, `.swp`, `.log` del workspace
**Dependencias:** Ninguna
**Pruebas:** `git status --short` debe mostrar solo cambios preexistentes
**Criterio de aceptación:** Workspace limpio de artefactos
**Rollback:** No aplica (no están trackeados)

### FASE 2: Dead Code Confirmado (RIESGO: BAJO)
**Candidatos:** L-01 (infrastructure/)
**Acción:** Eliminar directorio `infrastructure/`
**Dependencias:** Verificar que ningún import lo referencia
**Pruebas:** `uv run ruff check .`, `uv run lint-imports`
**Criterio de aceptación:** Sin imports rotos
**Rollback:** `git checkout infrastructure/`

### FASE 3: Dependencias Eliminables (RIESGO: MEDIO)
**Candidatos:** L-02, L-03, L-04, L-05, L-08 (sphinx)
**Acción:** Eliminar de `pyproject.toml`, ejecutar `uv lock`
**Dependencias:** Verificar que no hay imports dinámicos
**Pruebas:** `uv run pytest tests/ -x -q`, `uv run ruff check .`
**Criterio de aceptación:** Tests pasan, sin imports rotos
**Rollback:** Restaurar `pyproject.toml` y `uv.lock`

### FASE 4: Configuración Legacy (RIESGO: MEDIO)
**Candidatos:** L-06 (vars legacy en `.env`), L-23, L-24, L-27, L-28, L-34
**Acción:** Limpiar vars sin consumidores en `.env` (`.env` NO está commiteado — IN-03); revisar `deploy/host.env` versionado (H-DEP-01)
**Dependencias:** Decisión sobre `deploy/host.env` (H-DEP-01); no requiere `git rm --cached .env` (inaplicable)
**Pruebas:** `uv run ocm --cfg job`
**Criterio de aceptación:** Config válida
**Rollback:** Restaurar archivos

### FASE 5: Documentación Legacy (RIESGO: BAJO)
**Candidatos:** L-09, L-10
**Acción:** Actualizar `docs/DOMAIN.md` para reflejar ADR-0009
**Dependencias:** Ninguna
**Pruebas:** Review manual
**Criterio de aceptación:** Documentación consistente con código
**Rollback:** `git checkout docs/DOMAIN.md`

### FASE 6: Deployment Legacy (RIESGO: MEDIO)
**Candidatos:** L-16, L-26
**Acción:** Poblar Grafana dashboards o eliminar mounts; limpiar override
**Dependencias:** Decisión sobre monitoreo
**Pruebas:** `docker compose config`
**Criterio de aceptación:** Config Docker válida
**Rollback:** Restaurar archivos

### FASE 7: Test Naming (RIESGO: BAJO)
**Candidatos:** L-18
**Acción:** Renombrar `test_feature_engineer*.py` → `test_gold_transformer*.py`
**Dependencias:** Actualizar references en CI si existen
**Pruebas:** `uv run pytest tests/features/ -v`
**Criterio de aceptación:** Tests pasan con nuevos nombres
**Rollback:** `git mv`

### FASE 8: Migraciones Pendientes (RIESGO: MEDIO)
**Candidatos:** L-11, L-12, L-13
**Acción:** Eliminar shim `feature_engineer.py`, completar migración `classify_error()`, deprecar `OCM_DATA_LAKE_PATH`
**Dependencias:** Verificar todos los consumidores
**Pruebas:** `uv run pytest tests/ -x -q`, `uv run lint-imports`
**Criterio de aceptación:** Sin imports rotos, tests pasan
**Rollback:** Restaurar archivos

---

## 7. RELACIÓN CON AUDITORÍAS EXISTENTES

### vs AUDIT_OCM_deployment-portability_2026-09-17.md
- **H-DEP-01** (host.env tracked) → Relacionado con F-LEGACY-001 (secrets en disco, host.env versionado)
- **H-DEP-03** (SSOT entrypoints roto) → Relacionado con entry points audit (Fase 5)
- **H-DEP-06** (Kafka port inconsistente) → Relacionado con F-LEGACY-009 (variables sin consumidores)
- **H-DEP-09** (no runbook) → Confirmado en documentación legacy

### vs AUDIT_OCM_streaming-incident-recovery_2026-09-17.md
- B-59 (Kafka readiness) → Relacionado con F-LEGACY-014 (Zookeeper legacy)
- Systemd divergence → Relacionado con F-LEGACY-003 (docs desactualizadas)

### Hallazgos Comunes
1. Secretos de API en `.env` en disco (NO commiteado — IN-03) — ambos reportan
2. `host.env` tracked — ambos reportan
3. Systemd divergence — ambos reportan
4. Falta de runbook — ambos reportan

---

## 8. VALIDACIÓN

### audit_validator
No ejecutado — este es un formato de auditoría diferente (legacy/dead code vs compliance). El protocolo OCM no define un validador específico para auditorías de legacy.

### git status --short (final)
```
 M docs/plans/tracking.yaml
?? docs/audits/AUDIT_OCM_deployment-portability_2026-09-17.md
?? docs/audits/AUDIT_OCM_legacy-code-dead-code_2026-09-17.md
?? docs/audits/AUDIT_OCM_streaming-incident-entrypoints-architecture-2026-09-17.md
?? docs/audits/AUDIT_OCM_streaming-incident-recovery_2026-09-17.md
```

### Confirmación
```
NO SE ELIMINÓ NINGÚN ARCHIVO
NO SE MODIFICÓ CÓDIGO
NO SE MODIFICÓ CONFIGURACIÓN
NO SE MODIFICÓ SYSTEMD
NO SE MODIFICÓ DOCKER
NO SE MODIFICÓ TRACKING
NO SE HICIERON COMMITS
NO SE HICIERON PUSHES
```

---

## 9. RESUMEN FINAL

| Métrica | Valor |
|---------|-------|
| Total candidatos | 40 |
| CONFIRMADO_DEAD | 18 |
| PROBABLE_LEGACY | 2 |
| POSIBLE_LEGACY | 19 |
| EN_USO | 1 |
| NO_DETERMINABLE | 0 |
| Hallazgos CRITICAL | 0 |
| Hallazgos HIGH | 1 |
| Hallazgos MEDIUM | 3 |
| Hallazgos LOW | 10 |

> **NOTA (reconciliación 2026-09-18):** conteos alineados con la matriz L-01..L-40
> (IN-02). Los valores anteriores (8/4/28/0) no coincidían con la matriz;
> "67 candidatos" era la suma por categorías de §2, no filas de matriz.
> Hallazgos CRITICAL: 0 tras reclasificar F-LEGACY-001 (ver §2 y §4).
> EN_USO = L-30 (signum, bootstrap.py).
| Archivos eliminados | 0 |
| Archivos modificados | 0 |

### Principales Candidatos de Eliminación
1. `duckdb`, `lz4`, `aiometer`, `pybreaker` (HIGH)
3. `infrastructure/` directorio vacío (LOW)
4. Archivos `.bak` y `.swp` (LOW)
5. Variables `.env` sin consumidores (LOW)

### Principales Duplicaciones
1. `classify_error()` re-export (conocido, migración pendiente)
2. `feature_engineer.py` shim (deprecated, eliminación pendiente)

### Principales Configuraciones Legacy
1. Vars legacy en `.env` (NO commiteado; existencia = vars sin consumidores) y `deploy/host.env` versionado
2. Variables sin consumidores
3. Hydra schemas inexistentes
4. Grafana dashboards vacíos

### Principales Dependencias Potencialmente Eliminables
1. `duckdb` (nunca importado)
2. `lz4` (nunca importado)
3. `aiometer` (nunca importado)
4. `pybreaker` (nunca importado)
5. `sphinx` (sin docs/)

### Principales Tests Legacy
Ninguno funcional. Solo naming artifact en `tests/features/`.

### Principales Documentos Obsoletos
1. `docs/DOMAIN.md` (referencia FillHandler/TradeHistory eliminados)
2. `docs/PLAN-Maestro-Ingenieria.md.bak` (merge conflict)

### Artefactos Accidentales del Workspace
1. `pyproject.toml.bak`
2. `docker-compose.yml.bak`
3. `.docs.swp`
4. `feedhandler.log`
5. `docs/audits/*.bak` (3 archivos)

### Elementos que NO Deben Eliminarse Todavía
Ver §6 "CANDIDATOS QUE NO DEBEN ELIMINARSE TODAVÍA" (17 elementos)

### Decisiones que Requieren Revisión Humana
1. ¿Eliminar `duckdb` o planificar uso futuro?
2. ¿Poblar Grafana dashboards o eliminar la integración?
3. ¿Migrar a KRaft o mantener Zookeeper?
4. ¿Eliminar `feature_engineer.py` shim antes de v3.0.0?
5. ¿Completar migración `classify_error()`?
