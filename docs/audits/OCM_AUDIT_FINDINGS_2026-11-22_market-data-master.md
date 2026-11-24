# OCM — AUDIT FINDINGS REGISTER

**Ejecución de auditoría:** 2026-11-22 (baseline final `a1ba0fd4b20460bf2cd2af02e37f1990894d8bea`)
**Fuente primaria:** `docs/audits/AUDIT_OCM_FORENSIC_COMPLIANCE_2026-11-22.md`
**Estado de este registro:** OPEN (control read-only; ninguna corrección aplicada)
**Nota de integridad:** durante la sesión se observó deriva concurrente del árbol (`41c2a312…` → `5d06633…` → `119ee4b3…` → `a1ba0fd4…`) y artefactos temporales no gobernados; el baseline final y el estado limpio se revalidaron antes de emitir este registro.

Resumen: CRITICAL 1 · HIGH 5 · MEDIUM 3 · LOW 0 · INFO 0 · **total 9**.

Clasificación (taxonomía cerrada de `AUDIT_PROTOCOL.md` v2.1):
- NUEVO: 0
- REVALIDADO: 3 — F-MD-01, F-BAL-01, F-DATA-01
- REGRESIÓN: 1 — F-QA-01
- CERRADO: 1 — F-AUD-01
- CONTRADICCIÓN: 3 — F-GOV-01, F-FRESH-01, F-CFG-01
- RECOMENDACIÓN: 0
- NO_VERIFICADO: 1 — F-OPS-01

Deduplicación (regla §H):
- B-MD-009 + `ARCH-004` + ADR-0030 → un único finding (F-BAL-01); `ARCH-004 FAIL` es deuda gobernada, no hallazgo nuevo.
- B-MD-004 + ADR-0044 + staged port → un único finding (F-MD-01); `ARCH-006 PASS` testifica cobertura staged, no ausencia de riesgo.
- `scripts/expire_bronze_snapshots.py` + B-02 → un único finding de regresión (F-QA-01); la causa raíz ya había sido corregida en `pipeline_factory.py`.
- Hallazgos de ejecución (validador sin informe, cobertura) se separan de los hallazgos de producto: F-AUD-01 es el único finding de tooling y está CERRADO por la creación del informe canónico.

---

## F-GOV-01 — Cierre prematuro de B-25 en tracking

Severity: HIGH
Status: OPEN
Classification: CONTRADICCIÓN
Control: GOVERNANCE_TRACE (trazabilidad Hallazgo→Tracking→ADR→Implementación→Tests→CI→Cierre)
Source: `docs/plans/tracking.yaml` / `docs/architecture/decisions/ADR-0028-draft-bookbuilder-marketstate.md`

Evidence:
- `docs/plans/tracking.yaml` declara B-25 `estado: HECHO`, `estado_auditoria: CONFIRMADO` y `fecha_cierre: '2026-11-15'`.
- `docs/plans/tracking.yaml` declara en la misma ficha `cadena.implementacion.estado: PARCIAL`, `cadena.tests.estado: NO_APLICA`, `cadena.ci.estado: NO_APLICA` y `cadena.cierre.estado: PENDIENTE`.
- La propia cadena indica 'D3 market.gaps + D6 fix-dedup-u pendientes' y 'governance closure requiere D3/D6 implementados y decisión humana'.
- `docs/architecture/decisions/ADR-0028-draft-bookbuilder-marketstate.md` registra D3=market.gaps y D6=fix-dedup-u dentro del alcance pendiente.
- `tests/market_data/infrastructure/kafka/test_book_builder_gap_publishing.py` contieneTests para gap 100→105, stale, contiguo, lifecycle y header `x-ocm-dataset=orderbook`, evidencia de implementación parcial superior a la declarada en tracking.

Impact:
- El estado agregado del sistema es engañoso: un cierre declarado sin cadena completa permite reportar B-25 como terminado mientras quedan decisiones D3/D6 y cierre gobernado pendiente.

Required human decision:
- D-001: mantener `HECHO` corrigiendo la cadena y documentando por qué D3/D6 están fuera de alcance, **o** revertir a `PENDIENTE/EN_CURSO` hasta completar D3/D6 y la decisión humana de cierre.

Recommended remediation:
- Alinear los siete eslabones de la cadena de B-25 con la evidencia real: implementación PARCIAL, tests PASSED para el subconjunto cubierto, CI declarada si aplica y cierre PENDIENTE; no cerrar sin decisión humana.

Verification required:
- `docs/plans/tracking.yaml` sin contradicción entre cabecera y `cadena`, con D3/D6 y cierre gobernado reflejados de forma inequívoca.

Traceability:
- Tracking: B-25 (HECHO/CONTRADICCIÓN) · ADR: ADR-0028 · Implementation: PARCIAL · Tests: PARCIAL · CI: NO_APLICA · Evidence: F-GOV-01 · Closure: PENDIENTE

---

## F-MD-01 — MarketDataViewPort staged sin consumidor real

Severity: HIGH
Status: OPEN
Classification: REVALIDADO
Control: ARCH-006 (Orphaned Contract / Port — cobertura staged)

Evidence:
- `docs/plans/tracking.yaml` mantiene B-MD-004 `PENDIENTE`, con riesgo residual 'Alto' y fecha_cierre null.
- `policies/registry.yaml` registra `MarketDataViewPort` como staged port con consumidor previsto B-MD-004 y expiración `2026-12-31`.
- `docs/architecture/decisions/ADR-0044-staged-ports-gobernados.md` gobierna la fase intermedia del puerto.
- `tests/architecture_linter/test_golden.py` fija `ARCH-006: Status.PASS` con el comentario 'ports huérfanos eliminados o cableados', mientras la ejecución real de `uv run python -m architecture_linter --root . --json` indica '0 consumidores fuera del archivo de definición' y PASS por 'cobertura staged válida (ADR-0044)'.
- No hay evidencia de consumo de `MarketDataViewPort` por B-MD-004 en la configuración inspeccionada.

Impact:
- No existe validación de mercado (is_ready, spread, liquidez/depth, stale_threshold_ms) previa al envío de órdenes; el PASS del linter no debe interpretarse como conformidad funcional.

Required human decision:
- D-002: conservar el waiver staged hasta `2026-12-31` con riesgo explícito, **o** autorizar la fase de implementación de B-MD-004 y retirar el staged.

Recommended remediation:
- Cablear el consumidor real y actualizar golden/registry cuando el port deje de ser staged;否则 mantener el waiver con seguimiento de expiración.

Verification required:
- `ARCH-006 PASS` por consumidor real, no por staged coverage; registry sin entrada activa para `MarketDataViewPort`.

Traceability:
- Tracking: B-MD-004 (PENDIENTE) · ADR: ADR-0044 · ADR: ADR-0028 · Implementation: PENDIENTE · Tests: PENDIENTE · CI: GOBERNADA · Evidence: F-MD-01 · Closure: PENDIENTE

---

## F-BAL-01 — Balance real no implementado (ARCH-004 FAIL)

Severity: CRITICAL
Status: OPEN
Classification: REVALIDADO
Control: ARCH-004 (Balance State — configured vs exchange)

Evidence:
- `tests/architecture_linter/test_golden.py` fija `ARCH-004: Status.FAIL` como deuda P1 LIVE-blocker y referencia ADR-0030 / B-MD-009.
- `uv run python -m architecture_linter --root . --json` reporta ARCH-004 FAIL: 'No existe balance real consultado al exchange ... Risk/Execution computa sizing/drawdown contra capital_usd configurado'.
- `docs/plans/tracking.yaml` mantiene B-MD-009 `PENDIENTE`, prioridad `CRITICA`, con implementación/tests/CI/cierre PENDIENTE.
- `docs/architecture/decisions/ADR-0030-balance-real-reconciliacion-patrimonial.md` está aceptada pero no implementada.

Impact:
- En ejecución real (producción sobrescribe `dry_run: false`), sizing y drawdown usan capital configurado, no saldo del exchange; fees, transferencias y fills externos pueden producir exposición incorrecta.

Required human decision:
- D-003: autorizar la implementación de ADR-0030 (BalancePort + reconciliación) antes de cualquier habilitación live, o mantener el bloqueo documentado.

Recommended remediation:
- Implementar BalancePort en composition root de portfolio, consumo por RiskManager con frescura, gate de arranque/restart y bloqueo ante discrepancia material.

Verification required:
- ARCH-004 PASS, tests de integración fetch_balance→portfolio→risk y política de discrepancia; cierre de B-MD-009 con evidencia.

Traceability:
- Tracking: B-MD-009 (PENDIENTE) · ADR: ADR-0030 · Implementation: PENDIENTE · Tests: PENDIENTE · CI: PENDIENTE · Evidence: F-BAL-01 · Closure: PENDIENTE

---

## F-FRESH-01 — Contradicción de estado de B-MD-001 / freshness chain

Severity: HIGH
Status: OPEN
Classification: CONTRADICCIÓN
Control: FRESHNESS_CHAIN (FreshnessPort → enforcement → consumo real)

Evidence:
- `docs/plans/tracking.yaml` mantiene B-MD-001 `PENDIENTE` y su campo `evidencia` afirma 'El linter reporta ARCH-005 FAIL; no existe freshness como estado consultable'.
- El mismo registro, en `notes`, afirma que `FreshnessPort` y `FreshnessSnapshot` ya existen, `GapAwareStream` implementa el port y los niveles 1, 2 y 4 del linter PASS.
- `tests/architecture_linter/test_golden.py` fija `ARCH-005: Status.PASS` con comentario 'freshness chain completa: contract + enforcement + lag_ms en TradesSourceProtocol'.
- La ejecución real de `uv run python -m architecture_linter --root . --json` confirma ARCH-005 PASS en todos los niveles.

Impact:
- La contradicción impide saber si la deuda pendiente es de implementación, de consumo real en trading/risk o solo de documentación; bloquea la priorización correcta de un requisito live.

Required human decision:
- D-004: actualizar tracking para reflejar ARCH-005 PASS y limitar lo pendiente a niveles 3/5/6, **o** reabrir la cadena de implementación si el PASS del linter resulta insuficiente.

Recommended remediation:
- Separar en tracking los estados de contrato, enforcement y consumo real; eliminar la afirmación 'ARCH-005 FAIL' del campo de evidencia si no corresponde al baseline actual.

Verification required:
- tracking coherente con golden y con salida real del linter; B-MD-001 con alcance pendiente inequívoco.

Traceability:
- Tracking: B-MD-001 (PENDIENTE/CONTRADICCIÓN) · ADR: ADR-0059 · Implementation: PARCIAL · Tests: PARCIAL · CI: GOBERNADA · Evidence: F-FRESH-01 · Closure: PENDIENTE

---

## F-QA-01 — Regresión de import `get_catalog` en script de expiración

Severity: HIGH
Status: OPEN
Classification: REGRESIÓN
Control: TYPE_SAFETY (mypy gate)

Evidence:
- `uv run mypy . --no-incremental` → `scripts/expire_bronze_snapshots.py:6: error: Module "market_data.infrastructure.storage.bronze.bronze_storage" does not explicitly export attribute "get_catalog" [attr-defined]`; `Found 1 error in 1 file (checked 422 source files)`.
- `scripts/expire_bronze_snapshots.py` importa `get_catalog` desde `market_data.infrastructure.storage.bronze.bronze_storage`.
- `docs/plans/tracking.yaml` documenta B-02 como resuelto: 'fix pipeline_factory.py:49 -> iceberg.get_catalog (3affa2)' y solución 'Apuntar a market_data.infrastructure.storage.iceberg.catalog.get_catalog'.
- La referencia normativa de tracking apunta a `packages/market_data/infrastructure/storage/iceberg/catalog.py`, que existe en el repo.

Impact:
- El gate de tipado está rojo; el mismo error de ruta de import previamente corregido reaparece en un archivo nuevo, bloqueando merge y degradando la confianza en la corrección de B-02.

Required human decision:
- D-005: corregir el import a la fuente canónica `iceberg.catalog.get_catalog`, **o** retirar el script si ya no es operativo.

Recommended remediation:
- Aplicar el mismo patrón de B-02 en `scripts/expire_bronze_snapshots.py`, con test de import/gate de mypy.

Verification required:
- `uv run mypy . --no-incremental` → 0 errores; gate `quality` en CI verde.

Traceability:
- Tracking: B-02 (HECHO/REGRESIÓN) · ADR: NOT_TRACED · Implementation: REGRESIÓN · Tests: PENDIENTE · CI: FAIL · Evidence: F-QA-01 · Closure: OPEN

---

## F-AUD-01 — Validador de auditoría sin informe emparejado

Severity: MEDIUM
Status: CLOSED
Classification: CERRADO
Control: AUDIT_TOOLING (audit validator ↔ canonical report)

Evidence:
- Antes de este informe, `uv run pytest tests/architecture/test_audit_validator.py::test_full_repo_audit_validator_passes -q --no-cov` fallaba con AssertionError: sin informe emparejado para OCM_AUDIT_FINDINGS_2026-08-18_audit.md en `tests/architecture/test_audit_validator.py:249`.
- El registro canónico precedente apuntaba a `docs/audits/AUDIT_OCM_TECHNICAL_COMPLIANCE_2026-08-18.md`, ausente tras la archivación de auditorías históricas.
- El validador mecánico con registro por defecto pasaba pero con `skipped 8` por ausencia de informe.
- Este registro declara `Fuente primaria: docs/audits/AUDIT_OCM_FORENSIC_COMPLIANCE_2026-11-22.md`, que existe.

Impact:
- Sin emparejamiento registro↔informe, las reglas M12..M20 del validador se omitían y la suite unitaria completa quedaba roja.

Required human decision:
- Ninguna; hallazgo de tooling cerrado por la creación del artefacto canónico permitido.

Recommended remediation:
- Mantener `Fuente primaria` declarada en todo registro y verificar `test_full_repo_audit_validator_passes` en cada cierre de auditoría.

Verification required:
- `uv run pytest tests/architecture/test_audit_validator.py -q --no-cov` → PASS; `uv run python scripts/audit_validator.py` → PASS sin skipped M12..M20.

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: CERRADO · Tests: PASS · CI: PASS · Evidence: F-AUD-01 · Closure: CLOSED

---

## F-DATA-01 — Contrato canónico de 6 datasets vs activación real

Severity: MEDIUM
Status: GOVERNED
Classification: REVALIDADO
Control: DATASET_CONTRACT_SCOPE (ADR-0041 ↔ configuración habilitada)

Evidence:
- `docs/architecture/decisions/ADR-0041-canonical-market-data-contract.md` define alcance B para seis datasets con wire y declara 'Pending: adaptación de los adapters/producers de los 6 datasets del scope B'.
- `config/datasets.yaml` habilita únicamente `ohlcv: true`; `trades`, `orderbook`, `funding_rate`, `open_interest` y `liquidations` están en `false`.
- `config/market_data/feeds.yaml` usa `ingestion_mode: rest` y solo Bybit habilitado; KuCoin deshabilitado.
- ADR-0041 excluye explícitamente de su alcance la implementación de adapters/producers y la corrección de schema drift, por lo que la brecha está gobernada y no es una contradicción normativa.

Impact:
- La superficie operativa real es OHLCV/REST/Bybit; los otros cinco datasets del contrato canónico no están activos, limitando cobertura de Strategy/MarketState y validación cross-dataset.

Required human decision:
- D-007: secuenciar la activación de los cinco datasets pendientes o confirmar que el contrato canónico es solo especificación diferida.

Recommended remediation:
- Vincular cada dataset de scope B a un ítem de tracking con criterio de activación, adapters y tests de contrato.

Verification required:
- `config/datasets.yaml` y tracking/ADR alineados; activación progresiva con evidencia por dataset.

Traceability:
- Tracking: NOT_TRACED · ADR: ADR-0041 · Implementation: PARCIAL · Tests: PARCIAL · CI: NO_APLICA · Evidence: F-DATA-01 · Closure: GOVERNED

---

## F-CFG-01 — KAFKA_ENABLED: documento obligatorio vs default falso

Severity: MEDIUM
Status: OPEN
Classification: CONTRADICCIÓN
Control: CONFIG_FAILFAST (configuración de producción ↔ documentación)

Evidence:
- `README.md` indica 'KAFKA_ENABLED=true en producción — obligatoria (regla L5, F-031): production no arranca sin publisher Kafka (config-load fail-fast)'.
- `config/env/production.yaml` define `enabled: ${oc.env:KAFKA_ENABLED,false}` con default `false`.
- `ocm/config/env_vars.py` declara `KAFKA_ENABLED` y su docstring indica que en producción debe sobreescribirse a `true`.
- `docs/plans/tracking.yaml` B-43 registra que la búsqueda exhaustiva no encontró consumidores reales de `KAFKA_ENABLED` y que el flag permanece huérfano.
- La búsqueda directa en `apps/` y `packages/` no encontró uso de `KAFKA_ENABLED`.

Impact:
- La documentación afirma un fail-fast que la configuración por defecto no garantiza; un despliegue productivo sin la variable tendría Kafka deshabilitado sin error de carga.

Required human decision:
- D-006: hacer `KAFKA_ENABLED=true` obligatorio/fail-fast en producción y conectar el flag al publisher real, **o** corregir README para reflejar que es opt-in y documentar B-43.

Recommended remediation:
- Resolver conjuntamente con F-031/B-46: eliminar variable huérfana o gobernar `integrations.kafka.enabled` desde `AppConfig` con validación productiva.

Verification required:
- Validación de configuración de producción sin `KAFKA_ENABLED` → fallo explícito si la regla L5 sigue vigente; tracking B-43 actualizado.

Traceability:
- Tracking: B-43 (PENDIENTE) · ADR: NOT_TRACED · Implementation: PARCIAL · Tests: PENDIENTE · CI: NO_APLICA · Evidence: F-CFG-01 · Closure: OPEN

---

## F-OPS-01 — Preparación operacional post-incidente no verificable en auditoría estática

Severity: HIGH
Status: OPEN
Classification: NO_VERIFICADO
Control: OPS_READINESS (observabilidad y continuidad)

Evidence:
- `docs/incidents/2026-11-21-audit.md` documenta P0 `ocm-market-data.service` caído 23 h y `streaming_hydra` 34 h enviando tráfico a Kafka por confusión de puertos 9093/9094.
- El mismo documento reporta observabilidad 0/5, dependencia `pyiceberg_core` no reproducible y banner con puertos incorrectos.
- La auditoría es read-only y no ejecuta levantamiento de infraestructura, consulta de servicios ni verificación remota; las correcciones documentadas no pudieron confirmarse en el entorno.
- Quedan pendientes explícitos: refactor `DlqMixin` (Issue #180), limpieza de `ocm.dlq`, decisión sobre `safety.dry_run=false` y actualización de BIOS.

Impact:
- No puede afirmarse que la continuidad operativa y la observabilidad estén restauradas solo con base documental; el riesgo es de incertidumbre, no un fallo confirmado.

Required human decision:
- D-008: designar una verificación operativa (humana o CI de infra) de servicios, puertos, alertas y DLQ antes de declarar readiness.

Recommended remediation:
- Ejecutar comprobación controlada de servicios y observabilidad; cerrar pendientes del post-mortem con evidencia.

Verification required:
- Evidencia de 6/6 servicios observables, alertmanager en :9094, `ocm-market-data.service` activo y DLQ limpio.

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: NO_VERIFICADO · Tests: NO_VERIFICADO · CI: NO_VERIFICADO · Evidence: F-OPS-01 · Closure: OPEN
