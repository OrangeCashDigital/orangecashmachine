# ADR-0030: Balance real y reconciliación patrimonial — Portfolio como dueño del estado financiero (B-MD-009)

> **ESTADO: ACEPTADA** — decisión aprobada por el owner el 2026-08-16. NO implementada todavía: los contratos cambian solo cuando la implementación (tracking B-MD-009, cadena `implementacion`) se ejecute y los gates pasen.
> **Corresponde a:** A-MD-005 (etiqueta interna en tracking.yaml, B-MD-009), diseño conceptual en `docs/audits/2026-08-15-b-md-008-009-diseno-conceptual.md` §4-6.

**Estado:** Aceptado
**Fecha:** 2026-08-15
**Bounded context(s) afectado(s):** portfolio (patrimonio/reconciliación), trading (risk consume), market_data (adapter CCXT)

## Contexto

OCM **no conoce el saldo real del exchange** (VERIFIED, F-BMD9-01): grep exhaustivo de `fetch_balance`/`get_balance`/`balance`/`wallet`/`free`/`equity` en `packages/`, `shared/`, `apps/`, `ocm/` = solo "rebalance" (falso positivo). `RiskManager` recibe `capital_usd` por constructor (`packages/trading/risk/manager.py:112-118`, default 10_000) y computa sizing (`:394-401`) y drawdown (`:374-386`) contra ese número fijo. `PortfolioService` recibe `capital_usd` por constructor (`packages/portfolio/services/portfolio_service.py:63-80`) y lo reporta en `PortfolioState.capital_usd` (`:281-284`). `PositionStore` (InMemory/Redis) es el SSOT de posiciones (BC-43, solo instanciable en portfolio bootstrap, `packages/portfolio/bootstrap/composition_root.py:13-24`). `CCXTAdapter` no expone `fetch_balance` (`ccxt_adapter.py:405` create_order, `:462` fetch_order).

**Hechos del exchange (OFFICIAL, investigación 2026-08-15):**
- **`wallet-balance` con `accountType=UNIFIED`** expone: `walletBalance` (equity total en USD), `equity`, `locked`, `totalOrderIM`, `totalPositionIM`, `unrealisedPnl`, `totalAvailableBalance` (USD) — la cantidad realmente disponible para nueva exposición.
- **`free`/`availableToWithdraw` están DEPRECADOS para cuentas UNIFIED desde 2025-01-09** (docs oficiales Bybit). El campo operativo correcto es `totalAvailableBalance` (USD) / `availableBalance` por moneda.
- **CCXT `fetch_balance`** (`bybit.py:3525`): para UNIFIED llama a `wallet-balance` y parsea `info.walletBalance`; el `free` de CCXT mapea a `availableToWithdraw` (deprecado) — **no debe usarse como "disponible operativo"**.
- Aviso oficial de Bybit: en volatilidad extrema la respuesta de balance puede experimentar latencia/delays — la lectura no es un instante atómico del mercado.
- Rate limits: `wallet-balance` 50/s; la conexión IP global 600/5s (exceso → 403 ban ~10 min).

## Alternativas evaluadas

1. **Trading posee su propio balance** (`fetch_balance` directo en trading + store propio). Rechazada: crea 2ª/3ª fuente de verdad (exchange real + trading + portfolio) y viola la direccionalidad BC-50/BC-43 (trading no debería poseer estado patrimonial duplicado). Audit §12/§4B: riesgo de segunda fuente de verdad = real.
2. **RiskManager lee el saldo del exchange directamente en cada validate().** Rechazada: añade latencia a cada submit (período 3 del análisis, alto coste), no se puede verificar el estado patrimonial completo (posiciones del portfolio), y acopla trading a I/O de exchange en el dominio (BC-09).
3. **Portfolio como dueño del estado patrimonial (posiciones vía PositionStore BC-43 + saldo derivado vía reconciler) y trading consume vía port. ELEGIDA.** Modelo `exchange → [reconcile] → portfolio (PositionStore + saldo) → trading (lee vía port)`. Un solo punto de verdad patrimonial; trading ya depende de portfolio (BC-43, inyección en `TradingCompositionRoot`) — no se añade frontera nueva.
4. **Fetch sincrónico antes de cada orden + fetch periódico.** Combinar ambos. Se elige **no por defecto** el fetch en cada orden (ver Decisión: saldo cacheado + frescura B-MD-001); el gate de arranque y el loop periódico sí.

## Decisión

1. **Source of Truth del estado patrimonial: portfolio.** Portfolio ya es dueño de posiciones (ADR-0006, BC-43). El saldo es la otra mitad del estado patrimonial → **mismo dueño natural** (no se crea segundo SoT). El balance del exchange es la fuente primaria; portfolio lo materializa; trading lo consume vía port.
2. **Nuevo `BalancePort` (contract en `shared/contracts/boundaries.py`)** — consulta por activo: total, disponible operativo, locked, unrealisedPnL, timestamp de lectura, freshness. `RiskManager` consume el saldo con frescura (vínculo B-MD-001) para sizing/drawdown; **nunca segunda fuente**.
3. **Adapter `fetch_balance` en el composition root de portfolio** (único punto autorizado a importar market_data/CCXT — mismo criterio BC-50 que trading): `_BybitBalanceSource` en `packages/portfolio/bootstrap/composition_root.py` → `CCXTAdapter.fetch_balance`. Lectura UNIFIED con **`totalAvailableBalance`** (no `free`/`availableToWithdraw`).
4. **Cuándo reconciliar:**
   - **Gate en arranque/restart (obligatorio live):** verificar que el capital configurado coincide con el real; discrepancia material → no habilitar live.
   - **Loop periódico asíncrono** (heartbeat): detecta fugas, fees, transferencias externas, fills fuera de OCM.
   - **Tras cada fill y tras error/desconexión/reconexión:** asíncrono, no en el camino del submit.
   - **Antes de cada orden: NO por defecto.** Usar saldo cacheado con freshness en vez de fetch sincrónico (evita latencia en cada submit sin ganancia proporcional de seguridad).
5. **Política de discrepancia (fail-closed):** tolerancias **configurables** (origen = política de riesgo, no heurística; default conservador fijado en el ADR al aceptarse). Clasificación:
   - Redondeo/precisión (≤ `tol.rounding`) → log debug; sin acción.
   - Fees pequeñas (≤ `tol.fees`) → log info; ajuste opcional del capital interno con audit log.
   - Discrepancia pequeña (< `tol.tolerance_pct`) → alerta info; monitorizar.
   - **Material (> `tol.tolerance_pct`) → bloquear nuevas órdenes + alerta + estado degradado.**
   - Balance insuficiente para órdenes planificadas → bloquear órdenes de ese símbolo + alerta.
   - Posición en exchange sin reflejo en OCM (o viceversa) → **crítico: NO auto-corregir**, intervención humana.
   - Estado imposible (negativo, qty inconsistente) → **halt global (`ExecutionGuard`) + alerta + humana**.
   - **Nunca auto-corregir posiciones** contra el exchange sin decisión humana (la posición es SSOT en Portfolio, BC-43).
6. **Recovery:** al reiniciar, portfolio materializa el saldo desde el exchange (gate de arranque) y rehidrata posiciones desde `PositionStore` (ADR-0027); risk reconstruye su estado desde los SSOT patrimoniales.

## Justificación técnica

- **Un solo punto de verdad patrimonial** es la regla arquitectónica (audit §12): el balance del exchange es primario, portfolio lo materializa, trading lo consume. Evita el escenario de 3 fuentes (exchange + trading + portfolio) que el audit §4B marca como riesgo real.
- **Portfolio es el dueño natural:** ya posee posiciones (ADR-0006/BC-43); el saldo es la otra mitad del estado patrimonial. Trading ya depende de portfolio (BC-43) → no hay frontera nueva, solo una dependencia en la dirección existente.
- **UTA semantics correctas:** usar `totalAvailableBalance` (no `free`) porque `availableToWithdraw` está deprecado para UNIFIED desde 2025-01-09; un sizing contra `free` puede sobre/sub-estimar el capital operativo real (fees, órdenes reservadas, margen). El campo operativo Bybit es `totalAvailableBalance` (USD).
- **Separación conceptual (patrimonio ≠ balance disponible ≠ buying power ≠ capital máximo de riesgo):** `totalAvailableBalance` es un dato operativo de disponibilidad/buying power de la cuenta; NO debe interpretarse automáticamente como "capital máximo de riesgo". `RiskManager` puede usarlo como entrada para sizing/risk, pero el límite de riesgo es una decisión del dominio de Risk (política separada), no un valor derivado del exchange.
- **REST `wallet-balance` es la autoridad recuperable del balance.** Un futuro WebSocket privado de wallet sería incremental (latencia/observabilidad), no sustituye la reconciliación REST ni la lectura recuperable de arranque; no es el SoT patrimonial.
- **Gate en arranque + loop + post-fill/error** cubren los cuatro puntos ciegos del audit §4 (saldo real, fees acumuladas, transferencias externas, fills externos) sin añadir latencia al submit.
- **Fail-closed coherente con OCM:** ante discrepancia material se bloquean órdenes y se alerta (mismo patrón que `ExecutionGuard`/`_reconcile`); nunca se auto-corrige una posición (SSOT patrimonial en portfolio).
- **Veredicto LIVE:** bloqueante P1 (audit §16). Sin saldo real, sizing/exposición se computan contra capital configurado → decisiones incorrectas con capital real.

## Consecuencias

- **Más fácil:** riesgo calculado contra saldo real (sizing/drawdown correctos); detección temprana de fugas/fees/transferencias; base para stop-loss/limit sizing seguro; align con ADR-0027 (recovery) y B-MD-001 (freshness).
- **Deuda aceptada:** la reconciliación de saldo es asíncrona → siempre existe una ventana de staleness entre lecturas (mitigada por freshness y por el bloqueo ante discrepancia material); las tolerancias son configurables pero su calibración fina es política de riesgo posterior; fee_currency sigue None (GAP F7, ADR-0026) — se vincula, no se rehace.
- **Contratos BC-NN que lo hacen cumplir:**
  - `BC-43`/`BC-44` — PositionStore y el nuevo BalanceStore solo instanciables en portfolio bootstrap.
  - `BC-50` — el adapter CCXT `fetch_balance` vive en el composition root de portfolio (único punto autorizado a importar market_data).
  - `BC-12` — risk no importa execution; consume saldo vía port, no posee estado patrimonial.
  - `BC-13` — portfolio nunca importa trading (el bridge es fill_sync + inyección, ADR-0016/0027).
- **Riesgo residual:** lectura de balance puede sufrir latencia en volatilidad extrema (aviso oficial Bybit) → el saldo cacheado puede estar stale; mitigado por freshness (B-MD-001) y por bloqueo ante discrepancia material (nunca se opera con un saldo sospechoso).

## Relación con BookBuilder / market data

- **Independiente de la cadena B-MD-003→002→004** (VERIFIED, audit §8): el balance no necesita order book.
- **Dependencia débil con B-MD-001 (freshness):** el saldo cacheado necesita saber si está stale antes de usarse para sizing. No bloquea; es una mejora.
- **Política de freshness es requisito pre-LIVE, no optimización opcional:** el mecanismo de staleness/frescura, el umbral de antigüedad aceptable y el comportamiento fail-closed ante discrepancias son **requisito de Fase 2 / pre-LIVE**, no una optimización. El valor concreto del umbral NO se fija en este ADR (no se inventan valores numéricos); se determina por medición/pruebas en Sandbox antes de LIVE.
- **No se fuerza ninguna dependencia artificial** con BookBuilder (ADR-0028).

## Roadmap

- **Fase 3 (Business Rules / Trading)**, sin mover trabajo de Fase 1/2 (VERIFIED, audit §15). El paso de infraestructura de soporte (adapter CCXT `fetch_balance`) se ejecuta dentro de Fase 3.
- Orden sugerido (conceptual): exponer `fetch_balance` en CCXTAdapter → `BalancePort` en portfolio + `_BybitBalanceSource` en portfolio root → `PortfolioReconciler` (comparar PositionStore + balance vs exchange) → RiskManager consume con freshness → política de discrepancia + gate de arranque live → tests.
- **Bloqueante de live** (P1), junto a B-MD-008, B-MD-001, B-MD-004.

## Security scenarios

| Escenario | Garantía |
|---|---|
| A. Saldo configurado ≠ saldo real al arrancar | Gate de arranque live: discrepancia material → no habilitar live + alerta |
| B. Fuga/fee/transferencia durante operación | Loop periódico asíncrono detecta; discrepancia material → bloqueo de nuevas órdenes + alerta |
| C. Sizing contra saldo stale | Freshness (B-MD-001): si el saldo cacheado supera el umbral de edad, se refresca antes de usar o se bloquea (fail-closed) |
| D. Posición del exchange sin reflejo en OCM | Crítico: alerta, NO auto-corregir; intervención humana (SSOT patrimonial intacto) |

## ¿Por qué ADR separadas para B-MD-008 y B-MD-009?

Se recomienda **mantenerlas separadas** (ADR-0029 y ADR-0030), no combinarlas:

1. **Concern distinto:** B-MD-009 es **estado patrimonial** (saldo/posiciones, reconciliación contra el exchange); B-MD-008 es **control de órdenes** (estado de ejecución, máquina de estados CANCEL/FILL). Un ADR conjunto mezclaría dos ciclos de vida con ciclos de decisión diferentes.
2. **Dueños de BC distintos:** B-MD-009 vive en `portfolio` (patrimonio) con `trading/risk` como consumidor; B-MD-008 vive en `trading` (execution). La frontera BC se decide con claridad solo en documentos separados.
3. **Gates de aceptación independientes:** cada uno tiene tests, adapter CCXT y riesgos propios; uno puede aprobarse sin bloquear al otro (ambos son independientes entre sí, VERIFIED).
4. **Roadmap independiente:** aunque ambos son F3 y bloqueantes de live, se implementan en paralelo y comparten solo el paso de infraestructura (exponer métodos en CCXTAdapter), que no justifica fusionar los ADRs.

Si el owner prefiriera una sola ADR, el costo sería: menor granularidad de decisión, acoplamiento innecesario entre patrimonio y ejecución, y dificultad para reabrir/rechazar uno sin arrastrar el otro. **Recomendación: mantener separadas.**

## Decision Summary

| Campo | Valor |
|---|---|
| Problema | No existe `fetch_balance` en el repo (grep F-BMD9-01); RiskManager/PortfolioService usan `capital_usd` configurado (`manager.py:112-118`, `portfolio_service.py:63-80`) |
| Solución propuesta | Portfolio dueño del estado patrimonial (PositionStore BC-43 + saldo derivado vía `BalancePort`/`PortfolioReconciler`); adapter `CCXTAdapter.fetch_balance` (UTA, `totalAvailableBalance`) en portfolio root; gate de arranque + loop periódico + tras fill/error; discrepancia material → bloquear órdenes + alerta |
| BC responsable | portfolio (patrimonio/reconciliación), trading (risk consume vía port), market_data (adapter CCXT) |
| Bloquea LIVE | Sí (P1) — sizing/exposición contra capital configurado, no real |
| Fase | F3 |
| ADR | ADR-0030 (esta ADR; etiqueta A-MD-005 en tracking.yaml) |

## Implementation Roadmap (conceptual — no implementado)

1. **Adapter (`market_data`, S):** exponer `fetch_balance` en `CCXTAdapter` con lectura UNIFIED (`totalAvailableBalance`), compartido con ADR-0029 (paso adapter). Evidencia: test del adapter contra docs oficiales + sandbox.
2. **Port (`shared/contracts/boundaries.py`, S):** `BalancePort` (total/disponible/locked/unrealised por activo + timestamp/freshness). Evidencia: `tests/architecture/` (contrato).
3. **Portfolio (M):** `BalanceStore`/estado de saldo materializado + `PortfolioReconciler` (compara PositionStore + balance vs exchange; MATCH/MISMATCH) + `_BybitBalanceSource` en `portfolio/bootstrap/composition_root.py`. Evidencia: test de reconciliación real → portfolio materializa → risk consume.
4. **Risk consume (M):** RiskManager lee saldo disponible vía port con freshness para sizing/drawdown; nunca segunda fuente. Evidencia: test de sizing contra saldo real (no configurado).
5. **Política de discrepancia + gate de arranque live (M):** tolerancias configurables; bloqueo ante discrepancia material; halt global ante estado imposible. Evidencia: test política de discrepancia (bloqueo ante discrepancia material).
6. **Recovery (M):** gate de arranque/restart materializa saldo; rehidratación de posiciones (ADR-0027). Evidencia: test de restart.
7. **Tests/CI:** ruff + import-linter como gate; `pytest`.

## Referencias

- Código: `packages/trading/risk/manager.py:112-118,374-401`, `packages/portfolio/services/portfolio_service.py:63-80,281-284`, `packages/portfolio/bootstrap/composition_root.py:13-24`, `packages/market_data/adapters/outbound/exchange/ccxt_adapter.py:405,462`, `ocm/runtime/guard.py`, `shared/contracts/boundaries.py`.
- Docs: `docs/audits/2026-08-15-b-md-008-cancel-b-md-009-balance-audit.md` (F-BMD9-01..05), `docs/audits/2026-08-15-b-md-008-009-diseno-conceptual.md` §4-6, `docs/plans/tracking.yaml` B-MD-009 (no editado).
- ADRs relacionados: ADR-0006 (portfolio posee posiciones), ADR-0027 (recovery/SSOT), ADR-0025 (cost basis), ADR-0026 (fees), ADR-0016 (reconciliación). Complementa: ADR-0029 (B-MD-008).
- Doc oficial Bybit: `wallet-balance` (accountType=UNIFIED; `walletBalance`/`equity`/`locked`/`totalOrderIM`/`totalPositionIM`/`unrealisedPnl`/`totalAvailableBalance`); aviso de latencia en volatilidad extrema; `free`/`availableToWithdraw` deprecados para UNIFIED desde 2025-01-09; rate limit wallet-balance 50/s.
- CCXT (fuente descargada, `/tmp/opencode/ccxt-bybit/bybit.py`): `fetch_balance` :3525, `parse_balance` :3363 (`free` → `availableToWithdraw` deprecado).
