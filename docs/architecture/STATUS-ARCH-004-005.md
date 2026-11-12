# Estado de ARCH-004 y ARCH-005

> Actualizado: 2026-11-09

## ARCH-004 — Balance real (ADR-0030 / B-MD-009)

**Issue:** #121  
**Estado:** PARCIALMENTE IMPLEMENTADO  
**Bloqueante de live:** SÍ (P1)

### Roadmap ADR-0030 — estado actual

| Step | Qué | Estado |
|---|---|---|
| 1 | `CCXTAdapter.fetch_balance` (UNIFIED, totalAvailableBalance) | ❌ Pendiente (requiere sandbox Bybit) |
| 2 | `BalancePort` contract en `boundaries.py` | ✅ **En main** (PR #124) |
| 3 | `_BybitBalanceSource` en portfolio bootstrap (BC-50) | ❌ Pendiente (depende de step 1) |
| 4 | `PortfolioReconciler` | ❌ Pendiente |
| 5 | `RiskManager` consume `BalancePort` | ❌ Pendiente |
| 6 | Gate arranque + loop + política | ❌ Pendiente |

### Bloqueantes

- **Sandbox Bybit:** requerido para step 1.
- **Owner del ADR-0030:** debe firmar implementación (tracking B-MD-009).
- **Cambios de producción:** steps 3-6 requieren revisión.

### Test de contrato

`tests/architecture/test_balance_port_contract.py` valida la forma del contract. No valida el adapter real (no existe todavía).

## ARCH-005 — Freshness chain (B-MD-001)

**Issue:** #122  
**Estado:** DISEÑO EN REVIEW  
**Bloqueante de live:** SÍ (P1)

### Estado

- ✅ **ADR-0059 redactado** (DRAFT PR #142) — esperando aprobación del owner
- ❌ `FreshnessPort` en `boundaries.py` — bloqueado por aprobación del ADR
- ❌ Propagación a trading/portfolio — bloqueado
- ❌ Enforcement pre-orden — bloqueado

### Decisiones que esperan al owner

1. Aprobar ADR-0059 (Propuesto → Aceptado)
2. Confirmar umbrales por defecto (WS 5-10s, REST 60s)
3. Registrar B-MD-001 en `tracking.yaml`
4. Autorizar `FreshnessPort` en `boundaries.py`
5. Autorizar cambios en `OMS.submit()`
6. Autorizar gate de arranque live

### Próximo paso

Owner revisa PR #142 → si aprueba, se implementa el contract puro (como PR #124 para BalancePort).

## Restricciones del entorno (para futuras sesiones)

- **No se pueden ejecutar** `git stash list`, `uv run pytest`, `git push --delete` en este entorno.
- **Sí se pueden ejecutar** `gh`, `cat`, `grep`, `git log`, `git worktree list`, edición de archivos en el repo.
- **Tests de contrato** ejecutados en sesiones anteriores: PASS.

## Acoplamiento ARCH-004 ↔ ARCH-005 (2026-11-12)

**Hallazgo de auditoría:** ARCH-005 (Freshness Chain) no puede cerrarse de forma legítima sin ARCH-004 (Balance Port / sandbox Bybit).

### Estado por nivel (verificado 2026-11-12)

El linter `architecture_linter/rules/arch_005.py` mide 6 niveles y falla si cualquiera falta:

| Nivel | Archivo/área medida | Estado | Notas |
|---|---|---|---|
| 1 · Detección de silencio | `packages/market_data/adapters/inbound/websocket/` | PASS | `GapAwareStream._handle_silence_gap`, `gap_threshold_ms` |
| 2 · Recovery | `packages/market_data/` | PASS | `_run_recovery`, `_restart_source`, `GapRecoveryFetcher` |
| 3 · Estado consultable en port | `packages/market_data/ports/inbound/trades_source.py` | **FAIL** | No menciona `FRESHNESS_SYMBOLS`. Accionable sin ARCH-004 |
| 4 · Contrato en boundaries/ports | `shared/contracts/boundaries.py` + `packages/market_data/ports` | PASS | Docstring de `FreshnessSnapshot` contiene símbolos |
| 5 · Propagación a trading/portfolio | `packages/trading/` + `packages/portfolio/` | **FAIL** | Requiere lógica real que consuma `FreshnessPort.get_freshness()` |
| 6 · Enforcement pre-orden | `packages/trading/application/` + `packages/trading/risk/` | **FAIL** | Requiere validar freshness contra el exchange antes del submit → **bloqueado por ARCH-004** |

### Por qué no se cierra hoy

- **Nivel 3:** accionable (imports/referencias en `trades_source.py`), pero cerrarlo solo no hace pasar ARCH-005 (fallan 5 y 6).
- **Nivel 5:** requiere que `packages/trading` y `packages/portfolio` consuman `FreshnessPort`. Sin conexión real al exchange, cualquier consumo sería un stub → viola ARCH-008 (stubs).
- **Nivel 6:** requiere enforcement pre-orden. Sin sandbox Bybit (#121), no se puede validar contra el exchange. Enforcement stub = deuda disfrazada.

### Conclusión

`GapAwareStream` y `FreshnessPort` **ya están implementados correctamente**. Lo que falta es **consumo real** por parte de `trading`/`risk`, y eso depende de ARCH-004.

**B-MD-001 permanece bloqueado por ARCH-004.** No es un fallo de implementación; es diseño correcto: no tiene sentido enforcement sin datos reales.

