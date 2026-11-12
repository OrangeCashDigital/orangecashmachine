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
- **Linter:** PASS=8 FAIL=2 (pre-existente).
