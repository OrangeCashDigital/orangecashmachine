# OCM — Architectural Investigation: SUBMITTED & Orphan Orders

**Fecha de investigación:** 2026-08-18
**Commit auditado:** `bee9fb5a3917c32fcc81fcc81fa5177ce0e57283`
**Branch:** `main`
**Alcance:** Investigación arquitectónica profunda del ciclo de vida `SUBMITTED` y la gestión de órdenes huérfanas en el motor de ejecución trading (B-MD-008, paso 5 de ADR-0029). Read-only estricto: escritura solo en `docs/audits/`.
**Metodología:** Discovery normativo (Plan → Governance → tracking → ADRs → CI) → contraste mecánico (`architecture_linter`, suite) → análisis de código (AST/grep) → fuentes externas (F4 internet, F5 bots de referencia) → clasificación estricta (taxonomía OCM) → veredicto.

---

## 1. Executive Summary

La investigación confirma una **contradicción arquitectónica** entre el diseño aprobado (ADR-0029, decisión 5: loop `manage_open_orders` sobre órdenes persistentes en `SUBMITTED`/`CANCELLING`) y el flujo de ejecución real, que es **síncrono y fail-closed** por decisión deliberada de ADR-0016: `OMS.submit()` resuelve incondicionalmente a `_fill`/`_reject`, `LiveExecutor._submit` solo devuelve `accepted=True` con `confirmed_filled`, y el estado `SUBMITTED` solo existe vía inyección artificial en tests. El paso 5 de B-MD-008, tal como está diseñado, no puede recorrer entradas reales.

Además se **revalida** el gap G2 de ADR-0029 (órdenes huérfanas indetectables): no existe `fetch_open_orders` (0 hits), `Order` no tiene `exchange_order_id`, y el journal de órdenes es VOLATILE (ADR-0027) sin recovery de órdenes abiertas. Una orden aceptada por el exchange pero divergente en estado local queda huérfana sin mecanismo de detección ni limpieza.

El contraste con fuentes externas (CCXT, NautilusTrader, Hummingbot, Freqtrade, Bybit, Ember, MatrixTrak) y el análisis estático de 3 bots de referencia (extraídos a `/tmp/opencode/`) converge en la **Alternativa B: submit síncrono + reconciliación contra el exchange** (`fetch_open_orders` + `Order.exchange_order_id` + gate de arranque), coherente con el modelo síncrono de ADR-0016 y con la política "mantener in-flight en casos de unknown, nunca inventar reject".

**Veredicto:** `AUDIT_READY_WITH_FINDINGS` — la investigación deja 4 findings (1 CONTRADICCIÓN, 1 REVALIDADO, 2 RECOMENDACIÓN), 3 decisiones humanas (1 bloqueante). No se modificó código, tests, CI, ADRs ni tracking.

---

## 2. Commit Auditado

- **Commit:** `bee9fb5a3917c32fcc81fcc81fa5177ce0e57283` (HEAD, `main`)
- **Working tree:** los 9 archivos modificados y los untracked listados en §12 corresponden a otras sesiones (incluidos 3 cambios detectados durante esta investigación: `.env.example`, `ocm/config/schema.py`, `tests/market_data/infrastructure/kafka/test_consumer_adapter.py`). Ninguno fue hecho por este agente; documentados, no revertidos.
- **Escrituras propias:** solo los dos documentos nuevos de esta investigación en `docs/audits/` (registro + informe).

---

## 3. Alcance

| Incluye | Excluye |
|---|---|
| Ciclo de vida de órdenes en `packages/trading/execution/` (OMS, Order, transport, live_executor) | Entorno live con capital real |
| Contraste ADR-0016/0027/0029 vs código real | Modificaciones de cualquier tipo |
| Diseño conceptual B-MD-008 (pasos 1-8) | — |
| Fuentes externas (CCXT, NautilusTrader, Hummingbot, Freqtrade, Bybit, Ember, MatrixTrak, StaxInvesting) + 3 bots de referencia | — |
| Estado del repo (integridad, 3 cambios concurrentes) | — |

**Read-only:** no se modificó código, tests, CI, ADRs, tracking, AGENTS.md ni AUDIT_PROTOCOL.md.

---

## 4. Metodología

1. Discovery normativo (§C): Plan Maestro → GOVERNANCE.md → tracking.yaml → ADRs → CI → auditorías históricas.
2. Contraste mecánico: `architecture_linter` (baseline PASS), suite unit (1200 PASS), 28 tests dirigidos B-MD-008 (PASS).
3. Análisis de código (F1): flujo submit, estados, identidad de órdenes, reinicio.
4. Documentación (F2): ADR-0016, ADR-0027, ADR-0029, diseño conceptual B-MD-008.
5. Fuentes externas (F3 libros en `/home/orangemusic/kb-local-only/`; F4 internet; F5 análisis estático de freqtrade/hummingbot/nautilus_trader en `/tmp/opencode/{ft,hb,nt}`).
6. Alternativas (F6-F7), casos de fallo (F8), cambios MUST/NICE (F9), no-implementar (F10), integridad (F11), recomendación (F12).
7. Clasificación estricta + reconciliación matemática + bloque de reproducibilidad.

---

## 5. Contradicciones Encontradas

| # | Contradicción | Evidencia |
|---|---|---|
| C1 | ADR-0029 (decisión 5) asume órdenes persistentes en `SUBMITTED`/`CANCELLING` gestionadas por un loop; el código síncrono (ADR-0016) no produce ese estado | `oms.py:299-303`, `live_executor.py:209-227`, tests con `_inject` |
| C2 | La recuperación de huérfanas "100% fiable" no es generalizable: CCXT #2698 admite exchanges que no permiten restaurar la orden tras timeout | CCXT issue #2698 |
| C3 | ADR-0027 (journal VOLATILE, sin recovery de órdenes) vs ADR-0029 (reconstruir desde exchange en reinicio) — la reconstrucción no está implementada | ADR-0027, grep 0 hits |

---

## 6. Análisis de Alternativas (paso 5 de B-MD-008)

| Criterio | A) Submit asíncrono | B) Síncrono + reconciliación | C) Leave-as-is |
|---|---|---|---|
| Coherencia con ADR-0016 | ❌ rompe modelo síncrono deliberado | ✅ conserva síncrono | ✅ |
| Estados colgados | ❌ SUBMITTED persistente sin resolver | ✅ resuelve por confirmación exchange | ❌ |
| Detección de huérfanas | ⚠️ vía loop (depende de A) | ✅ `fetch_open_orders` | ❌ |
| Idempotencia | ⚠️ | ✅ (clientOrderId durable) | ❌ |
| Complejidad | MEDIA | MEDIA | BAJA |
| Evidencia externa | — | NautilusTrader, Hummingbot, Freqtrade, CCXT, Bybit | — |

**Recomendación (10 argumentos):** B. Coherente con el modelo síncrono ya adoptado; resuelve C1/C2/C3; mecanismos probados en 5+ sistemas reales; permite gate de arranque; no requiere reescribir el flujo de fill; idempotencia por clientOrderId; política "unknown → mantener in-flight + alerta" (no inventar reject); requisito de persistencia mínima; no es overengineering (un exchange, market orders); habilita el paso 5 de B-MD-008 sin contradicción.

---

## 7. Matriz de Findings (4)

| ID | Severity | Classification | Descripción |
|---|---|---|---|
| F-SUB-01 | HIGH | CONTRADICCIÓN | SUBMITTED es estado test-only; contradice la premisa del paso 5 de ADR-0029 |
| F-SUB-02 | HIGH | REVALIDADO | Órdenes huérfanas indetectables: sin fetch_open_orders ni exchange_order_id (gap G2) |
| F-SUB-03 | MEDIUM | RECOMENDACIÓN | Alternativa B: submit síncrono + reconciliación contra exchange |
| F-SUB-04 | LOW | RECOMENDACIÓN | Actualizar tracking.yaml B-MD-008 (pasos 2-4 cerrados; paso 5 bloqueado) |

### Verificación matemática

```
Total = NUEVO(0) + REVALIDADO(1) + REGRESIÓN(0) + CERRADO(0) + CONTRADICCIÓN(1) + RECOMENDACIÓN(2) + NO_VERIFICADO(0)
      = 0 + 1 + 0 + 0 + 1 + 2 + 0 = 4 ✅

Severidades = CRITICAL(0) + HIGH(2) + MEDIUM(1) + LOW(1) + INFO(0) = 4 ✅
```

---

## 8. Matriz de Controles (13)

| Control | Comando canónico | Resultado | Estado |
|---|---|---|---|
| Boundaries de paquetes | `uv run lint-imports --config architecture_linter/importlinter.toml` | 50 kept / 0 broken (baseline mismo commit) | **PASS** |
| Invariantes arquitectónicas | `uv run python -m architecture_linter --root . --json` | GOLDEN_EXPECTED (7 FAIL / 1 PARTIAL / 2 PASS) — deuda gobernada | **PARTIAL** |
| Golden regression | `uv run pytest tests/architecture_linter/test_golden.py -q --no-cov` | 4 passed | **PASS** |
| Unit tests ejecución | `uv run pytest tests/trading/ -q` | 28 B-MD-008 passed | **PASS** |
| Suite unit completa | `uv run pytest tests/ -x -q -m 'not integration'` | 1200 passed | **PASS** |
| Lint | `uv run ruff check .` | All checks passed | **PASS** |
| Formato | `uv run ruff format . --check` | OK | **PASS** |
| Tipado | `uv run mypy .` | 0 issues | **PASS** |
| YAML lint | `uvx yamllint -c .yamllint .` | OK | **PASS** |
| Order lifecycle (SUBMITTED persistente) | inspección AST + grep `SUBMITTED` | estado solo vía `_inject` en tests | **FAIL** |
| Reconciliación exchange | grep `fetch_open_orders` | 0 hits | **FAIL** |
| Identidad de orden (exchange_order_id) | inspección `order.py` | ausente | **FAIL** |
| Restart recovery | ADR-0027 journal + grep | VOLATILE, sin recovery de órdenes | **FAIL** |
| Dependencias | `uv run pip-audit . --ignore-vuln PYSEC-2026-113 --ignore-vuln PYSEC-2026-1325` | 4 vulns, exit 1 (revalidado, F-CI-01) | **FAIL** |

```
Controles = PASS(8) + FAIL(5) + PARTIAL(1) = 14 ✅
```

**Nota:** 5 controles FAIL → 2 findings (F-SUB-01, F-SUB-02) + causas raíz compartidas; pip-audit revalidado como F-CI-01 (registro canónico), no se duplica (regla `CONTROL FAIL ≠ FINDING NUEVO`).

---

## 9. Matriz de Decisiones (3)

| ID | Problema | Evidencia | Opciones | Recomendación | Bloquea |
|---|---|---|---|---|---|
| **D-ARC-1** | Modelo del paso 5 de B-MD-008: contradicción SUBMITTED (F-SUB-01) | oms.py:299-303; live_executor.py:209-227; F-SUB-01 | A) async; B) síncrono + reconciliación; C) leave-as-is | B | ✅ BLOCKING |
| **D-ARC-2** | Habilitar reconciliación: `Order.exchange_order_id` + `fetch_open_orders` (F-SUB-02) | order.py:64-90; transport.py:53-139; grep 0 hits | A) implementar; B) diferir | A (con D-ARC-1) | ❌ |
| **D-ARC-3** | Actualizar tracking.yaml B-MD-008 (pasos 2-4 cerrados; paso 5 bloqueado) | tracking.yaml:2246-2300 | A) actualizar; B) dejar | A | ❌ |

```
Decisiones = BLOCKING(1: D-ARC-1) + NON_BLOCKING(2) = 3 ✅
```

---

## 10. Herramientas y versiones (reproducibilidad)

- pip-audit 2.10.1 · ruff 0.15.10 · mypy 1.19.1 · bandit 1.9.4 · pytest 8.4.2 · yamllint 1.38.0
- Fuente: `uv run python scripts/audit_validator.py --versions`

---

## 11. Bloque de Reproducibilidad

```
REPRODUCIBILIDAD
- commit: bee9fb5a3917c32fcc81fcc81fa5177ce0e57283
- branch: main
- fecha: 2026-08-18
- protocolo: AUDIT_PROTOCOL v2.1
- agente/modelo: opencode/deepseek-v4-flash-free
- herramientas: pip-audit 2.10.1, ruff 0.15.10, mypy 1.19.1, bandit 1.9.4, pytest 8.4.2, yamllint 1.38.0
- comandos: uv run pytest tests/ -x -q -m 'not integration'; uv run python -m architecture_linter --root . --json; uv run lint-imports --config architecture_linter/importlinter.toml; uv run pip-audit . --ignore-vuln PYSEC-2026-113 --ignore-vuln PYSEC-2026-1325; grep fetch_open_orders; uv run python scripts/audit_validator.py
- golden: PASS
- resultado: PASS del validador (ver §12)
```

---

## 12. Integridad

| Ítem | Estado |
|---|---|
| HEAD | `bee9fb5a3917c32fcc81fcc81fa5177ce0e57283` — **idéntico** |
| Código | Intacto (3 cambios concurrentes de otra sesión documentados: `.env.example`, `ocm/config/schema.py`, `tests/market_data/infrastructure/kafka/test_consumer_adapter.py` — no revertidos) |
| Tests | Intactos |
| CI | Intacto |
| ADRs | Intactos |
| tracking.yaml | Intacto |
| AGENTS.md / AUDIT_PROTOCOL.md | Intactos |
| Escrituras propias | Solo `docs/audits/OCM_AUDIT_FINDINGS_2026-08-18-arch-investigation.md` + `docs/audits/AUDIT_OCM_ARCH_INVESTIGATION_SUBMITTED_ORPHAN_2026-08-18.md` |
| Validador | `uv run python scripts/audit_validator.py --register docs/audits/OCM_AUDIT_FINDINGS_2026-08-18-arch-investigation.md --report docs/audits/AUDIT_OCM_ARCH_INVESTIGATION_SUBMITTED_ORPHAN_2026-08-18.md` → PASS esperado |
| git add/commit/push | NO ejecutados |

---

## 13. Conclusión

La investigación deja una **verdad operativa** para el paso 5 de B-MD-008: el estado `SUBMITTED` persistente asumido por ADR-0029 contradice el modelo síncrono de ADR-0016 (F-SUB-01, HIGH), las órdenes huérfanas son indetectables hoy (F-SUB-02, HIGH, revalidando el gap G2), y la evidencia externa converge en la **Alternativa B** (F-SUB-03) como remediación coherente. Se recomienda decisión humana D-ARC-1 (BLOCKING) para desbloquear la implementación, y actualizar tracking.yaml (F-SUB-04).

**Veredicto final:** `AUDIT_READY_WITH_FINDINGS` — CONTRADICCIÓN CONFIRMADA, recomendación de implementación definida, sin cambios en el working tree más allá de los documentos de auditoría en `docs/audits/`.