# Informe — Architecture Governance Linter para OCM

- **Fecha**: 2026-08-16
- **Tipo**: Implementación + auditoría automatizada de gobernanza arquitectónica
- **Entregable**: `architecture_linter/` + `tests/architecture_linter/` + este informe
- **Estado**: EJECUTADO Y VERIFICADO

---

## 1. Resumen ejecutivo

Se implementó un **linter de gobernanza arquitectónica real, ejecutable y mantenible**
para OCM, basado en análisis **AST** (no grep), stdlib-only, que detecta violaciones
arquitectónicas en 10 invariantes (ARCH-001…ARCH-010). Se ejecutó contra el repositorio
real: **9 reglas en FAIL (hallazgos reales verificados), 1 en PASS** (capas BC-08 respetadas).

El linter reprodujo **todos los hallazgos forenses golden** (F1–F4) de la falsación
dirigida y confirmó 27 hallazgos primarios, además de detectar 8 ports huérfanos y
2 stubs de producción. No se modificó ningún ADR ni `tracking.yaml`; el código de
negocio queda intacto (deliverables son archivos nuevos).

Veredicto global: **VALIDADO** — el linter funciona, es reproducible, está testeado
(27 tests) y sus resultados coinciden con la evidencia forense previa.

## 2. Objetivos

1. Detectar violaciones arquitectónicas con **evidencia primaria** (ruta/línea/símbolo reales),
   no con nombres sueltos ni heurísticas de texto.
2. Correlacionar **declaración → mutación → lecturas → consumidores** para cada concepto.
3. Reportar PASS/FAIL/UNKNOWN con razón concreta; **nunca convertir inferencia en FAIL**.
4. Ser configurable sin modificar reglas (severidad, activación, excepciones justificadas).
5. Reproducir el estado actual del repo como **golden** y quedar protegido por tests.
6. Emitir salida humana y JSON (CI) + SARIF opcional.

## 3. Metodología

- **Mecanismo principal: AST** (`ast` de stdlib). Cada módulo se analiza una vez y se
  indexa (clases, atributos de instancia, mutaciones, lecturas, imports, referencias).
- **Correlación**: para estado mutable se exige *anotación/uso dict-like* + *mutaciones reales*
  (subscript write, pop, update, setdefault, del) + *patrón de nombre o tipo*. El nombre
  solo nunca dispara.
- **Regla de verificación**: antes de escribir cualquier hallazgo se inspeccionó el código
  real (rutas, líneas, imports, docstrings). Lo que no es verificable estáticamente → UNKNOWN.
- **Fuentes de verdad (prioridad)**: código actual → config real → tests existentes →
  ADR-0029/ADR-0030 → informes forenses → ZIPs de bots → inferencia.
- **Comparación golden**: cada veredicto se contrastó contra los hallazgos F1–F4 de
  `2026-08-16-falsacion-dirigida-final.md`.

## 4. Estado del repositorio (baseline)

- **HEAD**: `54b64ff docs(adr): ADR-0029 (cancelación real) y ADR-0030 (balance real)`.
- **Sin cambios**: ADR-0029, ADR-0030, `docs/plans/tracking.yaml`, código de negocio.
- **Untracked previos**: 7 informes de auditoría en `docs/audits/` + PDF externo.
- **Nuevos (este trabajo)**: `architecture_linter/`, `tests/architecture_linter/`,
  `architecture/architecture_linter.toml`, `docs/audits/2026-08-16-architecture-linter.md`.
- **Entorno**: Python 3.13.5 vía `uv`; 353 archivos .py productivos.

## 5. Diseño del linter

```
architecture_linter/
├── __init__.py           # export público (engine, models)
├── __main__.py           # uv run python -m architecture_linter
├── cli.py                # CLI (--json/--sarif/--rules/--root/--config), exit codes 0/1/2
├── config.py             # carga TOML (architecture/architecture_linter.toml)
├── engine.py             # RepoContext (índice AST cacheado) + LinterEngine
├── models.py             # Severity, Status, Evidence, Finding, RuleResult, PositionStore, OrderStore
├── analyzers/
│   ├── ast_walk.py       # inventario estructural por módulo (clases, attrs, mutaciones, imports)
│   └── mutable_state.py  # correlación declaración→mutación→lectura para posición/órdenes
├── rules/
│   ├── base.py           # Rule ABC + helpers de finding/evidencia
│   ├── registry: arch_001.py … arch_010.py
├── reporters/            # humano, JSON, SARIF 2.1.0
```

Propiedades: **stdlib-only** (no importa nada de OCM), reutiliza el patrón de los
guards AST existentes en `scripts/` (app_layer_guard, check_ssot_enums) sin duplicarlos.

## 6. Configuración

`architecture/architecture_linter.toml` (sin config → defaults y todas las reglas activas):

- `roots`, `exclude_dirs`.
- `[linter.severity]` — ajuste por regla.
- `[linter.allow]` — **excepciones justificadas por símbolo**:
  - `CompositionRoot` (ARCH-007): cada bounded context tiene su propio Composition
    Root (ADR-0003/BC-17) — homónimo esperado.
  - `RiskConfig`, `ConfigurationError`, `CursorStore` (ARCH-007): homónimos por BC
    intencionales con responsabilidades distintas (verificado).
- Patrón espejo Hydra→Pydantic (`ocm/config/structured/` ↔ `schema.py`/`observability/config.py`)
  excluido estructuralmente en la regla (no es duplicación accidental; los docstrings lo declaran).

## 7. Reglas implementadas

| Regla | Nombre | Criterio de evidencia |
|-------|--------|------------------------|
| ARCH-001 | Multiple Position State Owners | Owners mutables de posición correlacionados (declaración+mutación+lectura); distingue SSOT portfolio. |
| ARCH-002 | Position Semantic Divergence | WAC/acumulación vs reemplazo/pyramid y reduce vs pop incondicional entre owners. |
| ARCH-003 | Order State Without Reconciliation | Estado de órdenes en memoria sin gestión de órdenes abiertas (fetch/manage_open_orders). |
| ARCH-004 | Balance State | Balance real consultado (fetch_balance/get_balance/BalancePort) vs capital_usd estático. |
| ARCH-005 | Market Data Freshness Boundary | Cadena de 6 niveles: detección→recovery→port→contrato→propagación→enforcement. |
| ARCH-006 | Orphaned Contract / Port | Protocol sin consumidores reales; implementaciones citadas inexistentes; inbound/outbound contradictorio. |
| ARCH-007 | Duplicate / Homonymous Contracts | Mismo nombre en módulos distintos; compara miembros/valores; excluye mirrors y allowlist. |
| ARCH-008 | False Capability / Stub | Marcador NOT IMPLEMENTED en docstring/cuerpo + NotImplementedError/StopAsyncIteration. |
| ARCH-009 | Layer Violation (BC-08) | Reproduce contrato de capas de import-linter (infrastructure→…→domain) vía AST + ignore_imports. |
| ARCH-010 | Duplicated Mutable State | Estado mutable de conceptos críticos (position/order) duplicado en ≥2 almacenes. |

## 8. Resultados contra OCM

```
Resumen: 10 reglas | PASS=1 FAIL=9 PARTIAL=0 UNKNOWN=0
findings_total=28  failed_findings=27
```

| Regla | Estado | Hallazgos |
|-------|--------|-----------|
| ARCH-001 | FAIL | 1 |
| ARCH-002 | FAIL | 2 |
| ARCH-003 | FAIL | 1 |
| ARCH-004 | FAIL | 1 |
| ARCH-005 | FAIL | 1 |
| ARCH-006 | FAIL | 8 |
| ARCH-007 | FAIL | 9 |
| ARCH-008 | FAIL | 2 |
| ARCH-009 | PASS | 1 (sin violaciones) |
| ARCH-010 | FAIL | 2 |

## 9. Hallazgos golden confirmados

Correspondencia con la falsación dirigida final (F1–F4):

- **F1 / ARCH-008 — WSTradesSource stub** (`packages/market_data/adapters/inbound/websocket/ws_trades_source.py:32`):
  docstring/cuerpo declara NOT IMPLEMENTED + `raise StopAsyncIteration`. Confirmado.
  Además se detectó **InfraMetricsKafkaProducer** (`infra_metrics_producer.py:31`) como stub real.
- **F2 / ARCH-005 — cadena de freshness rota**: niveles 1-2 (detección `gap_aware_stream.py:259` y
  recovery `_handle_silence_gap`/`_run_recovery`) presentes; niveles 3-6 ausentes
  (estado consultable en port, contrato, propagación a trading/portfolio, enforcement pre-orden).
- **F3 / ARCH-006 — MarketDataSourcePort huérfano**
  (`ports/outbound/market_data_source.py:32`): 0 consumidores reales; docstring referencia
  `CcxtRestAdapter`/`ReplayAdapter` inexistentes; se autodeclara INBOUND en `ports/outbound/`.
- **F4 / ARCH-001+002+010 — posición**: 5 owners mutables además del SSOT portfolio
  (TradeTracker._open_positions, OMS._open/_entry_positions, RiskManager._open_positions/_positions);
  divergencia semántica WAC (OMS/PortfolioService) vs reemplazo+pop (TradeTracker, `trade_tracker.py:146,158`);
  estado de posición duplicado en 6 almacenes y de órdenes en 4.
- **ARCH-003** — OMS._orders/_open (`oms.py:169-170`) sin `manage_open_orders`/`fetch_open_orders`;
  cancel local-only; `fetch_state` puntual (submit-time) sin loop de reconciliación.
- **ARCH-004** — sin `fetch_balance` en todo el repo; sizing/drawdown contra `capital_usd`
  estático (`config/portfolio/portfolio.yaml:15`, `portfolio/models/position.py:146`).

## 10. Hallazgos nuevos detectados por el linter (verificados)

Además de los golden, el linter descubrió hallazgos reales (verificados manualmente):

- **ARCH-006 — 7 ports huérfanos adicionales** (0 imports en todo el repo):
  `EventConsumerPort` (event_consumer.py:38), `OrderBookSourceProtocol` (trades_source.py:133),
  `EventPublisherPort` (event_publisher.py:36), `DerivativesFetcherPort` (fetcher.py:65),
  `CircuitBreakerPort` (resilience.py:64), `BronzeStoragePort` (storage.py:231),
  `ExternalMetricsPort` (metrics.py:416, implementación citada inexistente).
  Nota: `ReplayPort` se detectó inicialmente pero es re-exportado por `ports/inbound/external/__init__.py`
  → **falso positivo corregido** al indexar imports de re-exportación.
- **ARCH-007 — 9 pares duplicados/homónimos** (todos golden o verificados): OrderStatus
  (order.py:55 vs transport.py:53, valores distintos), StorageFactoryPort (storage_factory.py:40 vs
  storage.py:270, sin consumidores el segundo), AnomalyRegistryPort (anomaly_registry.py:33 vs
  quality.py:32), QualityPipelineResult (quality_pipeline.py:48 vs application/quality/pipeline.py:84),
  ExchangeCircuitOpenError (exceptions:227 vs resilience.py:23), RetryExhaustedError
  (exceptions:105 vs adapters resilience:51), SchemaVersionError (exceptions:149 vs
  shared/kafka/schemas/_base.py:86), _TransientProxy (policies/base.py:122 vs runtime.py:159),
  PipelineContext (application/context.py:30 vs application/pipeline/runtime.py:219).

## 11. Falsos positivos detectados y corregidos (proceso)

El proceso de calibración sobre el repo real encontró y corrigió:

1. **ARCH-008 (234→2)**: marcadores `TODO`/`stub` genéricos disparaban cientos de falsos
   positivos (comentarios de typing-stubs de pyiceberg/redis). Se restringió a marcadores
   fuertes `NOT_IMPLEMENTED`/`NOT IMPLEMENTED` en docstring o cuerpo.
2. **ARCH-007 (21→9)**: los modelos Hydra `ocm/config/structured/` son un **patrón espejo
   documentado** del Pydantic SSOT (`schema.py`), no duplicados accidentales. Excluidos
   estructuralmente; homónimos por BC permitidos vía allowlist.
3. **ARCH-006 ReplayPort**: port re-exportado por su `__init__.py` → no es huérfano.
   Corregido indexando `ImportFrom`/`Import` como referencias.
4. **ARCH-004 equity**: curva de equity computada desde capital no es balance de exchange.
   Eliminado del set de símbolos de balance.
5. **ARCH-009 (8→0)**: `adapters → infrastructure` aparece como deuda técnica *documentada*
   en `ignore_imports` de BC-08 (SSOT). La regla ahora lee el contrato de import-linter
   en vez de aristas inventadas.

## 12. Tests y verificación

- **27 tests** en `tests/architecture_linter/`:
  - `test_rules.py` (23): por regla — PASS, FAIL y falso positivo, con fixtures aisladas.
  - `test_golden.py` (4): contra el repo OCM real — pin exacto de los 10 veredictos,
    y aserciones de símbolos concretos (ports huérfanos, duplicados ARCH-007, stubs ARCH-008).
- **Gates**:
  - `ruff check` — limpio (25 archivos).
  - `ruff format --check` — formateado.
  - `mypy architecture_linter` — sin issues (22 fuentes).
  - `bandit -r architecture_linter` — 0 issues.
  - `lint-imports --config architecture/importlinter.toml` — **50 contratos KEPT, 0 broken**.
  - `pytest tests/ -m "not integration"` — **1136 passed**, coverage 50.37% (fail_under 40).
  - CLI: `--json` válido; `--rules` filtra; exit code 1 con FAIL, 0 sin FAIL.

## 13. Limitaciones y UNKNOWN

- El análisis es estático (AST): dependencias dinámicas (imports dentro de `if TYPE_CHECKING`,
  imports lazy interpretables) se tratan según lo visible. Los imports lazy **sí** se detectan
  (walk completo), ventaja frente al grafo de import-linter.
- `_impl_exists` (ARCH-006) valida por nombre de clase, no por ruta módulo exacta.
- Refs por nombre (ARCH-004/005/007) pueden incluir coincidencias homónimas; cada hallazgo
  se marcó con `confidence < 1.0` cuando procede.
- No hay ningún UNKNOWN en la ejecución actual: todos los invariantes fueron verificables.

## 14. Conclusiones

El linter confirma y automatiza los hallazgos forenses previos (F1–F4) y añade evidencia
nueva verificada. Los resultados **no simulan**: cada ruta/línea fue verificada contra el
código actual. El veredicto global del estado arquitectónico de OCM es **NO CONFORME en 9
de 10 invariantes de gobernanza**, con deuda técnica ya catalogada (ports huérfanos,
contratos duplicados, stubs, estado mutable distribuido, balance estático).

## 15. Recomendaciones y próximos pasos

1. Integrar `uv run python -m architecture_linter` en CI como gate (JSON + exit code).
2. Resolver los 9 pares duplicados de ARCH-007 (SSOT único por contrato).
3. Mapear los 8 ports huérfanos: eliminar, implementar o reservar con doc explícita.
4. Sustituir `WSTradesSource`/`InfraMetricsKafkaProducer` por implementaciones reales o
   marcarlos explícitamente fuera de ruta de producción.
5. Consolidar el ownership de posición en un único SSOT (portfolio) y derivar vistas.
6. Introducir balance real vía `BalancePort` (ADR-0030) y gestionar órdenes abiertas
   periódicamente (ADR-0029).
7. Extender la cadena de freshness (niveles 3-6) al contrato `TradesSourceProtocol` y al
   enforcement pre-orden.
8. Golden test como guardián: si un veredicto cambia, o el código cambió (revisar) o la
   regla introdujo un falso positivo/negativo (corregir).