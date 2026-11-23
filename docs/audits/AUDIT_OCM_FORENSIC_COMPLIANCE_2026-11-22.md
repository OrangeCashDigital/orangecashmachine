# AUDIT OCM — AUDITORÍA MAESTRA INTEGRAL DE MARKET DATA

**Fecha:** 2026-11-22
**Baseline final observado:** `a1ba0fd4b20460bf2cd2af02e37f1990894d8bea` (rama `chore/coverage-audit-2026-11-22`)
**Registro asociado:** `docs/audits/OCM_AUDIT_FINDINGS_2026-11-22_market-data-master.md`
**Protocolo:** `docs/governance/AUDIT_PROTOCOL.md` v2.1 — `MACHINE CHECKS FIRST. LLM JUDGMENT SECOND.`
**Regla de ejecución:** READ-ONLY. No se aplicaron correcciones de código, configuración, tests, CI, ADRs ni tracking. Única escritura permitida: `docs/audits/`.

---

## A. Executive Summary

**Veredicto final: `DEGRADED`.**

La auditoría maestra de market data encuentra un sistema con contratos import-linter en verde (`54 kept / 0 broken`), golden de deuda gobernada reproducible (`4 passed`), tipo y formato conformes salvo una regresión puntual de import, y un registro normativo amplio. No obstante, existen una deuda CRITICAL gobernada pero no implementada (balance real, B-MD-009 / ARCH-004), tres contradicciones de trazabilidad o configuración, una regresión de tipado que deja el gate `quality` rojo y una preparación operacional no verificable en modo read-only. Por ello el sistema no es `HEALTHY`; tampoco hay evidencia de ejecución live con capital real ni de pérdida patrimonial, por lo que no se declara `CRITICAL`.

Cifras consolidadas: **9 findings** (CRITICAL 1 · HIGH 5 · MEDIUM 3), **20 controles** (PASS 10 · FAIL 5 · PARTIAL 4 · NO_VERIFICADO 1), **8 decisiones humanas** pendientes y **0 correcciones aplicadas**.

La contradicción más relevante no es un fallo nuevo: B-25 está marcada `HECHO/CONFIRMADO` en la cabecera de tracking mientras su propia cadena declara implementación PARCIAL, tests NO_APLICA y cierre PENDIENTE. Bajo el protocolo, esto se clasifica `CONTRADICCIÓN`, no `NUEVO`. Del mismo modo, `ARCH-004 FAIL` y `ARCH-006 PASS` son deuda ya registrada: el golden los fija como estado esperado y no deben reemitirse como hallazgos nuevos.

---

## B. Alcance y baseline

Se auditó el bounded context `market_data` y sus fronteras con `shared`, `ocm`, `trading` y `portfolio`, incluyendo:

- Gobernanza: Plan Maestro, `AUDIT_PROTOCOL.md`, `docs/plans/tracking.yaml`, ADRs y `policies/registry.yaml`.
- Contratos: `architecture_linter/importlinter.toml` y linter AST `ARCH-001..010`.
- Configuración: `config/base.yaml`, `config/env/production.yaml`, `config/env/test.yaml`, `config/datasets.yaml`, `config/market_data/feeds.yaml` y `ocm/config/env_vars.py`.
- Runtime y datos: composition roots, `BronzeWriter`, `BookBuilder`, `MarketDataViewPort`, topics Kafka y expiración de snapshots.
- Calidad y seguridad: ruff, mypy, pytest, bandit, pip-audit, SSOT de enums y yamllint.
- Incidentes: post-mortem `2026-11-21-audit.md`.
- Referencias externas no normativas: Nautilus, Freqtrade y LEAN.

Durante la sesión se observó deriva concurrente del árbol de trabajo (`41c2a31271c5…` → `5d06633…` → `119ee4b3…` → `a1ba0fd4…`) y artefactos temporales under `.audit/2026-11-22/`. El baseline final se revalidó limpio antes de emitir este informe. No se inspeccionaron stashes porque el permiso está denegado; su existencia se reporta solo como不确定性 observada, no como evidencia de cambios no gobernados.

---

## C. Metodología FASE 0→9

Se aplicó la secuencia FASE 0→9 solicitada:

0. Verificación de permisos y modo read-only.
1. Lectura de gobernanza: Plan Maestro → protocolo → tracking → ADRs → registry → CI.
2. Ejecución de tooling mecánico antes de juicio LLM.
3. Recolección de evidencia por dominio: gobernanza, arquitectura, configuración, datos, calidad, seguridad y operaciones.
4. Reconciliación de contadores del registro y del informe.
5. Clasificación cerrada y deduplicación por causa raíz.
6. Formulación de decisiones humanas `D-001..D-008`.
7. Contrastación con referencias externas no normativas.
8. Verificación final de gates y alcance de escritura.
9. Emisión de este informe y del registro canónico.

No se encontró una Tabla A-J normativa en el repositorio; se adopta explícitamente esta estructura A-J y se declara en la sección de Integridad.

---

## D. Hallazgos por dominio

### D.1 Gobernanza y trazabilidad

El hallazgo F-GOV-01 sintetiza la contradicción de B-25. La evidencia es directa: la cabecera declara cierre confirmado, pero la cadena interna declara `PARCIAL/NO_APLICA/PENDIENTE` y nombra D3 y D6 como pendientes. La decisión requerida es corregir la cabecera o completar la cadena; no corresponde al auditor cerrar el hallazgo.

F-FRESH-01 presenta la misma forma estructural en B-MD-001: el campo `evidencia` afirma `ARCH-005 FAIL` mientras `notes` y el golden afirman `ARCH-005 PASS`. La causa de fondo es una mezcla de estados de implementación, enforcement y consumo real.

### D.2 Arquitectura y contratos

Los contratos import-linter pasan y el linter AST es reproducible contra el golden. `ARCH-004 FAIL` se mantiene como deuda P1 de live-readiness; `ARCH-005 PASS` y `ARCH-006 PASS` son estados esperados, no prueba de conformidad funcional. La distinción es crítica: el golden es un detector de no-regresión, no un certificado de calidad.

F-MD-01 documenta que `ARCH-006 PASS` se sostiene por cobertura staged bajo ADR-0044, no por un consumidor real de `MarketDataViewPort`. Por tanto no se emite un hallazgo de contrato huérfano nuevo: se revalida la deuda ya gobernada.

### D.3 Configuración y datasets

El contrato canónico de ADR-0041 define seis datasets, pero la activación real es OHLCV sobre Bybit en modo REST. La brecha está declarada como pendiente en el propio ADR, por lo que F-DATA-01 es `REVALIDADO`, no `NUEVO`.

F-CFG-01 contrasta la afirmación del README de fail-fast con el default `${oc.env:KAFKA_ENABLED,false}` en producción y con la ausencia de consumidores del flag. La consecuencia operativa es que la documentación promete una garantía que la configuración actual no implementa.

### D.4 Calidad y regresiones

F-QA-01 es una regresión exacta de B-02: el mismo error de import `get_catalog` corregido en `pipeline_factory.py` reaparece en `scripts/expire_bronze_snapshots.py`. La severidad HIGH se justifica porque `quality` es gate de merge y porque la corrección previa no se propagó a un archivo nuevo.

### D.5 Operaciones

F-OPS-01 se clasifica `NO_VERIFICADO`, no `NUEVO`: el post-mortem documenta dos P0, puertos incorrectos y observabilidad 0/5, pero esta auditoría no levanta infraestructura ni consulta servicios remotos. AFIRMAR que están corregidos sería especulación.

---

## E. Reproducibilidad y controles mecánicos

### E.1 Comandos canónicos ejecutados

- `uv run python scripts/audit_validator.py`
- `uv run python scripts/engineering_health_check.py`
- `uv run lint-imports --config architecture_linter/importlinter.toml`
- `uv run pytest tests/architecture_linter/test_golden.py -q --no-cov`
- `uv run ruff check .`
- `uv run ruff format . --check`
- `uv run mypy . --no-incremental`
- `uv run python scripts/check_ssot_enums.py`
- `uv run pip-audit --ignore-vuln PYSEC-2026-113`
- `uv run bandit -r apps ocm packages shared infrastructure -ll`
- `uv run pytest tests/ -x -q -m "not integration"`
- `uvx yamllint -c .yamllint .`
- `uv run python -m architecture_linter --root . --json`

### E.2 Versión de herramientas

| Herramienta | Versión observada |
|---|---|
| pip-audit | pip-audit 2.10.1 |
| ruff | ruff 0.15.10 |
| mypy | mypy 1.19.1 (compiled: yes) |
| bandit | bandit 1.9.4 |
| pytest | pytest 9.0.3 |
| yamllint | yamllint 1.38.0 |

### E.3 Resultados breve

- `audit_validator`: PASS mecánico; antes de este informe con 8 skipped por ausencia de report, cerrados al crear el par registro↔informe.
- `engineering_health_check`: PASS.
- `lint-imports`: PASS, 54 contratos mantenidos, 0 rotos.
- `golden`: PASS, 4 tests.
- `ruff check` y `ruff format --check`: PASS.
- `mypy`: FAIL con 1 error en `scripts/expire_bronze_snapshots.py`.
- `SSOT enums`: PASS.
- `pip-audit`: PASS; única exclusión formal `PYSEC-2026-113` y paquete local no publicado en PyPI.
- `bandit -ll`: PASS en umbral Med/High; hallazgos Low informativos no bloquean.
- `pytest unit`: FAIL por cobertura 37.40% < 50% y, en la pasada previa al informe, por ausencia de report emparejado.
- `yamllint`: PASS con avisos de línea larga en documentación.

---

## F. Análisis de mercado y benchmark externo

Las fuentes externas se usan solo como hipótesis de comparación, nunca como norma:

1. `https://github.com/nautechsystems/nautilus_trader/blob/develop/docs/concepts/message_bus.md`
2. `https://github.com/nautechsystems/nautilus_trader/blob/develop/crates/common/src/clients/data.rs`
3. `https://www.freqtrade.io/en/stable/data-download`
4. `https://github.com/quantconnect/documentation/blob/master/06%20LEAN%20Engine/02%20Contributions/02%20Brokerages/02%20Laying%20the%20Foundation/03%20Lay%20the%20Foundation.html`
5. `https://github.com/quantconnect/documentation/blob/master/single-page/Quantconnect-Writing-Algorithms-Python.pdf`

Nautilus separa topics live de replay y desacopla `DataClient` del `DataEngine`; Freqtrade hace explícitos `pairs`, `timerange` y formato de datos; LEAN usa plugins de brokerage/data-feed. OCM ya posee equivalentes estructurales (`shared/kafka/schemas`, puertos, composition roots, datasets), pero la brecha no es de abstracción sino de activación y consumo real. En consecuencia no se recomienda introducir nuevas capas por analogía.

---

## G. Matriz de Findings

| ID | Severidad | Clasificación | Control | Resumen | Estado |
|---|---|---|---|---|---|
| F-GOV-01 | HIGH | CONTRADICCIÓN | GOVERNANCE_TRACE | B-25 cerrada en cabecera con cadena PARCIAL y D3/D6 pendientes | OPEN |
| F-MD-01 | HIGH | REVALIDADO | ARCH-006 | MarketDataViewPort staged sin consumidor real | GOVERNED |
| F-BAL-01 | CRITICAL | REVALIDADO | ARCH-004 | Balance real pendiente, sizing contra capital configurado | OPEN |
| F-FRESH-01 | HIGH | CONTRADICCIÓN | FRESHNESS_CHAIN | B-MD-001 mezcla ARCH-005 FAIL y PASS | OPEN |
| F-QA-01 | HIGH | REGRESIÓN | TYPE_SAFETY | Reaparición del error de import get_catalog de B-02 | OPEN |
| F-AUD-01 | MEDIUM | CERRADO | AUDIT_TOOLING | Validador sin informe emparejado | CLOSED |
| F-DATA-01 | MEDIUM | REVALIDADO | DATASET_CONTRACT_SCOPE | Seis datasets contratados, solo OHLCV activo | GOVERNED |
| F-CFG-01 | MEDIUM | CONTRADICCIÓN | CONFIG_FAILFAST | README exige Kafka en producción, default es false | OPEN |
| F-OPS-01 | HIGH | NO_VERIFICADO | OPS_READINESS | Post-mortem P0 y observabilidad no verificables en read-only | OPEN |

### G.1 Detalle de hallazgos

#### F-GOV-01 — Cierre prematuro de B-25

La cabecera de `docs/plans/tracking.yaml` declara `estado: HECHO`, `estado_auditoria: CONFIRMADO` y fecha de cierre. La misma ficha declara implementación PARCIAL, tests NO_APLICA, CI NO_APLICA y cierre PENDIENTE, además de D3 y D6 pendientes. Existen tests de gap publishing, pero no cubren la totalidad de la decisión. Requiere D-001.

#### F-MD-01 — Staged port sin consumidor

`policies/registry.yaml` mantiene `MarketDataViewPort` como staged con expiración 2026-12-31. `ARCH-006 PASS` depende de la cobertura staged permitida por ADR-0044. No hay consumidor real de B-MD-004 en la configuración inspeccionada. Requiere D-002.

#### F-BAL-01 — Balance real

`ARCH-004 FAIL` es estado esperado en el golden y prioridad CRITICA en B-MD-009. ADR-0030 está aceptado pero no implementado. En producción `dry_run: false`, el sizing consume capital configurado en lugar del saldo del exchange. Requiere D-003 antes de habilitar live.

#### F-FRESH-01 — Freshness contradicha

`docs/plans/tracking.yaml` mantiene B-MD-001 PENDIENTE y en `evidencia` afirma `ARCH-005 FAIL`, mientras `notes`, el golden y la ejecución real afirman `ARCH-005 PASS`. La decisión debe separar contrato, enforcement y consumo real. Requiere D-004.

#### F-QA-01 — Regresión de import

`uv run mypy . --no-incremental` reporta `scripts/expire_bronze_snapshots.py:6` importando `get_catalog` desde `bronze_storage`. B-02 documenta la solución correcta apuntando a `iceberg.catalog.get_catalog`. Requiere D-005.

#### F-AUD-01 — Emparejamiento registro↔informe

El test `test_full_repo_audit_validator_passes` fallaba porque el registro más reciente apuntaba a un informe archivado ausente. Este informe y su registro declaran la fuente primaria cruzada, cerrando el hallazgo sin tocar código.

#### F-DATA-01 — Alcance de datasets

ADR-0041 declara explícitamente pendiente la adaptación de adapters y producers de los seis datasets de scope B. `config/datasets.yaml` solo habilita OHLCV. Es deuda gobernada; requiere D-007 para secuenciación.

#### F-CFG-01 — KAFKA_ENABLED

El README describe fail-fast productivo, pero `config/env/production.yaml` mantiene default `false` y la búsqueda no encontró consumidores del flag. Requiere D-006.

#### F-OPS-01 — Preparación operacional

El post-mortem documenta servicios caídos y puertos erróneos. Sin acceso a infraestructura, el estado actual es no verificado. Requiere D-008.

---

## H. Matriz de Controles

| ID | Objetivo | Evidencia o comando | Estado |
|---|---|---|---|
| GOVERNANCE_TRACE | Trazabilidad completa hallazgo a cierre | F-GOV-01 en tracking.yaml | FAIL |
| STAGED_PORT | Puertos huérfanos gobernados | F-MD-01, ADR-0044, registry.yaml | PARTIAL |
| BALANCE_REAL | Capital desde exchange | F-BAL-01, ARCH-004 FAIL | FAIL |
| FRESHNESS_CHAIN | Contrato, enforcement y consumo real | F-FRESH-01, ARCH-005 PASS | PARTIAL |
| DATASET_SCOPE | Contrato canónico vs activación | F-DATA-01, datasets.yaml | PARTIAL |
| CONFIG_FAILFAST | Configuración productiva coherente | F-CFG-01, production.yaml | FAIL |
| OPS_READINESS | Servicios, puertos y observabilidad | F-OPS-01, post-mortem | NO_VERIFICADO |
| AUDIT_TOOLING | Registro e informe emparejados | F-AUD-01, test_audit_validator.py | PASS |
| ARCH_CONTRACTS | Fronteras BC intactas | uv run lint-imports --config architecture_linter/importlinter.toml | PASS |
| ENGINEERING_HEALTH | Plan, tracker, ADR, contratos y CI alineados | engineering_health_check.py | PASS |
| ARCH_LINTER | Detector AST reproducible | ARCH-004 FAIL gobernado, golden PASS | PARTIAL |
| GOLDEN | No regresión de deuda esperada | test_golden.py, 4 passed | PASS |
| RUFF_CHECK | Lint de estilo | uv run ruff check . | PASS |
| RUFF_FORMAT | Formato determinista | uv run ruff format . --check | PASS |
| MYPY | Gate de tipado | 1 error en expire_bronze_snapshots.py | FAIL |
| SSOT_ENUMS | Literales canónicos | check_ssot_enums.py | PASS |
| DEPENDENCY_AUDIT | Vulnerabilidades conocidas | uv run pip-audit --ignore-vuln PYSEC-2026-113 | PASS |
| BANDIT | Vulnerabilidades Med/High | bandit -r apps ocm packages shared infrastructure -ll | PASS |
| UNIT_TESTS | Suite unitaria y cobertura | cobertura 37.40% menor que 50% | FAIL |
| YAMLLINT | Sintaxis YAML | uvx yamllint -c .yamllint . | PASS |

Controles = PASS(10) + FAIL(5) + PARTIAL(4) + NO_VERIFICADO(1) = 20

---

## I. Matriz de Decisiones

| ID | Pregunta | Opciones | Consecuencias | Evidencia | Finding |
|---|---|---|---|---|---|
| D-001 | Cómo cerrar B-25 | Corregir cadena o revertir estado a pendiente | Evita cierre falso; puede reabrir work item | tracking.yaml, ADR-0028 | F-GOV-01 |
| D-002 | Mantener waiver de MarketDataViewPort | Extender hasta 2026-12-31 o implementar B-MD-004 | Riesgo de mercado sin validación si se extiende | registry.yaml, ADR-0044 | F-MD-01 |
| D-003 | Autorizar ADR-0030 antes de live | Bloquear live o implementar BalancePort | Sizing con saldo real vs capital configurado | ADR-0030, ARCH-004 | F-BAL-01 |
| D-004 | Corregir B-MD-001 o reabrir implementación | Actualizar estado a PASS parcial o reabrir niveles 3/5/6 | Define si falta documento o consumo real | tracking.yaml, golden | F-FRESH-01 |
| D-005 | Corregir o retirar script de expiración | Apuntar a iceberg.catalog o eliminar script | Desbloquea gate mypy | expire_bronze_snapshots.py, B-02 | F-QA-01 |
| D-006 | Hacer Kafka obligatorio | Fail-fast en producción o corregir README | Alinea L5 con configuración real | README.md, production.yaml | F-CFG-01 |
| D-007 | Secuenciar datasets scope B | Activar por dataset o confirmar especificación diferida | Define cobertura de Strategy y MarketState | ADR-0041, datasets.yaml | F-DATA-01 |
| D-008 | Verificar operaciones post-incidente | Check controlado de servicios y puertos o aceptar riesgo | Cierra incertidumbre de observabilidad | post-mortem 2026-11-21 | F-OPS-01 |

---

## J. Integridad, roadmap y veredicto

### J.1 Control FAIL no equivale a finding nuevo

Todo FAIL detectado se contrastó contra tracking, ADRs, golden y auditorías históricas. `ARCH-004 FAIL`, `ARCH-005 PASS`, `ARCH-006 PASS`, B-25, B-MD-004, B-MD-009 y B-MD-001 ya están registrados. No se emitió ningún finding `NUEVO`. La única regresión es F-QA-01 porque B-02 ya había sido corregida y reapareció en otro archivo.

### J.2 Deduplicación

- B-MD-009 + ARCH-004 + ADR-0030 → F-BAL-01.
- B-MD-004 + ADR-0044 + ARCH-006 → F-MD-01.
- B-25 + cadena contradictoria → F-GOV-01.
- B-MD-001 + ARCH-005 → F-FRESH-01.
- B-02 + script de expiración → F-QA-01.
- B-43 + KAFKA_ENABLED → F-CFG-01.

### J.3 Límites de la auditoría

No se ejecutaron pruebas con infraestructura real, no se consultaron servicios desplegados, no se inspeccionaron stashes, no se imprimieron secretos y no se modificó código. El post-mortem y la KB se trataron como documentación, no como autoridad normativa. Ante conflicto, prevalecieron código, contratos y ADRs.

### J.4 Veredicto

`DEGRADED` es el veredicto honesto: la base arquitectónica y contractual es sólida, pero existen una deuda CRITICAL de live-readiness, cinco controles FAIL y contradicciones de gobernanza que impiden declarar salud operativa. El bloqueo para cualquier habilitación live se mantiene hasta F-BAL-01 y F-QA-01; la corrección de documentación y tracking debe pasar por decisión humana.

### J.5 Estructura A-J

No se localizó una tabla A-J normativa en el repositorio. Se adoptó y declaró esta estructura: A resumen, B alcance, C metodología, D dominios, E reproducibilidad, F benchmark externo, G findings, H controles, I decisiones, J integridad y veredicto. Esta declaración evita presentar una estructura inferida como norma.

---

**Cierre:** este informe y su registro son los únicos artefactos creados. La auditoría queda lista para revisión humana; el merge y cualquier corrección permanecen fuera del alcance del agente.
