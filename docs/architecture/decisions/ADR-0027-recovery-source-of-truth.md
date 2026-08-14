# ADR-0027: Recovery y source of truth — journal OCM persistido + reconciliación con el exchange

**Estado:** Aceptado
**Fecha:** 2026-08-14
**Bounded context(s) afectado(s):** trading (execution, analytics), portfolio (models), shared
**Referencia de dominio:** decisión aprobada en S1.2/S1.3/S1.4 (etiqueta interna "ADR-3")

## Contexto

El estado financiero de OCM se pierde o se duplica entre reinicios:

- `OMS._open` y `OMS._entry_prices` son dicts en memoria (`packages/trading/execution/
  oms.py:126-130`); tras un restart no existe rehidratación → el P&L económico se
  desvanece (F6).
- `TradeTracker._open_positions` y `TradeTracker._closed` son estructuras en memoria
  (`packages/trading/analytics/trade_tracker.py:58-61`); **no existe TradeStore
  persistido** (ni Redis ni Iceberg en trading/portfolio).
- `RiskManager` guarda `_open_positions`, `_daily_pnl_pct`, `_total_pnl_pct` y `_halted`
  en memoria (`packages/trading/risk/manager.py:94-99`); `reset_total()` (155-166) borra
  el drawdown.
- Las posiciones sí se persisten (`RedisPositionStore`, TTL 7 días,
  `packages/portfolio/infra/redis_store.py`), pero sin quantity ni avg_entry (ADR-0025),
  y no hay wiring de rehidratación hacia OMS/risk.
- El P&L realizado se calcula en dos vías distintas (oms.py:331 para risk; trade_record
  para analytics) → semántica financiera duplicada.
- `fill_sync.py:33-35` documenta explícitamente la unificación de ownership del estado de
  posiciones como "decisión arquitectónica pendiente, no resuelta".

El exchange expone estado recuperable de forma oficial: `GET /v5/position/list` devuelve
`size`, `avgPrice`, `unrealisedPnl`, `cumRealisedPnl`; `closed-pnl` reporta `closedSize`,
`openFee`, `closeFee`, `avgEntryPrice`, `avgExitPrice`, `closedPnl`.

## Alternativas evaluadas

1. **Exchange como SSOT primario del estado interno OCM.** Rechazada: el exchange no
   conoce la semántica OCM (size_pct, timeframe, net_pnl_pct, fees estimadas, slippage,
   funding de paper); no debe gobernar el estado interno.
2. **Solo journal OCM persistido, sin reconciliación.** Rechazada: ante fills perdidos o
   fees UNKNOWN no hay referencia externa de verificación.
3. **Híbrido: journal OCM persistido (primario) + reconciliación con el exchange.
   Elegida.** OCM conserva su propia verdad para auditoría/reproducibilidad; el exchange
   es referencia de reconciliación y fuente de datos faltantes (fee UNKNOWN→FINAL,
   avgPrice).

## Decisión

Para cada estado financiero se define **SOURCE OF TRUTH**, **PERSISTENCE** y **RECOVERY
METHOD**:

### POSITION STATE
- **Source of truth:** **portfolio** — `PortfolioService` + `PositionStore` (ADR-0006:
  portfolio es el único dueño del estado de posiciones).
- **Persistence:** almacén de posiciones existente (`RedisPositionStore` producción /
  `InMemoryPositionStore` paper; BC-43). Pendiente: quantity + avg_entry (ADR-0025, F4a/
  F4b).
- **Recovery method:** al arrancar, PortfolioService rehidrata desde el almacén. Los
  duplicados VOLATILE de trading (`OMS._open`, `TradeTracker._open_positions`) dejan de
  ser fuente; el contador de risk se reconstruye desde aquí (ver RISK STATE).

### TRADE HISTORY
- **Source of truth:** **journal OCM persistido** de trades cerrados (append-only).
- **Persistence:** **UNKNOWN — no se decide aquí la tecnología** (no Redis, no Iceberg,
  no Kafka, ni otra, en este ADR). La elección es decisión de implementación posterior
  (F6a). Hoy `TradeTracker._closed` es VOLATILE.
- **Recovery method:** recarga del journal al arrancar.

### REALIZED P&L
- **Source of truth:** **única vía de asentamiento** = `OMS._fill` (el settlement del
  exchange), consumida por risk y analytics. Se elimina la semántica duplicada.
- **Persistence:** derivado del journal de trades (no se persiste como estado separado).
- **Recovery method:** recomputado desde fills/trades cerrados; fees UNKNOWN→FINAL vía
  reconciliación con `closed-pnl` (ADR-0026).

### UNREALIZED P&L
- **Source of truth:** derivado on-demand de `quantity × (valuation_price − avg_entry)`,
  con valuation = mark price (decisión Q-C, S1.3).
- **Persistence:** ninguna (valor derivado).
- **Recovery method:** recomputado; no se persiste.

### RISK STATE
- **Source of truth:** **reconstruido** desde los SSOT persistidos (posiciones del
  portfolio + journal de realized P&L). No es estado primario.
- **Persistence:** ninguna (hoy VOLATILE: manager.py:94-99).
- **Recovery method:** en el bootstrap, `open_count` desde el almacén de posiciones y
  drawdown desde el journal persistido.

### EXCHANGE STATE
- **Source of truth:** el exchange (autoridad externa). **Nunca SSOT del estado interno
  OCM**; es la referencia de reconciliación.
- **Persistence:** reconstruible vía API (`position/list`, `closed-pnl`).
- **Recovery method:** consulta a la API para reconciliar posiciones y confirmar fees/
  fills.

**Flujo de recovery:** restart → PortfolioService rehidrata posiciones → OMS/Risk
reconstruyen (`open_count` desde store; drawdown desde journal) → reconciliación con
`position/list` (size y avgPrice dentro de tolerancia; discrepancia → alerta) → fees
UNKNOWN→FINAL vía `closed-pnl`.

## Justificación técnica

- **Demostración con arquitectura actual (no se asume "portfolio es SSOT"):** portfolio
  es SSOT de POSITION por ADR-0006 y por el almacén persistido existente; **no** lo es de
  TRADE HISTORY ni de REALIZED P&L (hoy VOLATILE/duplicado) — por eso este ADR añade el
  journal, no lo asume.
- **Convergencia con el exchange:** `position/list` y `closed-pnl` permiten verificar y
  completar el estado OCM sin que el exchange gobierne la semántica interna (no conoce
  size_pct/timeframe/net_pnl_pct).
- **Recuperabilidad:** el único dato realmente persistido hoy (posiciones) es insuficiente
  para reconstruir P&L; el journal cierra el gap (F6a).
- Es decisión de arquitectura OCM (el orden de autoridad de la KB: código/ADRs > doc
  oficial > libros; los libros son neutrales sobre restart de proceso).

## Consecuencias

- **Más fácil:** restart sin pérdida de posiciones ni de P&L; verificación externa
  (reconciliación); fees UNKNOWN→FINAL.
- **Deuda aceptada:** el journal de trades aún no existe (F6a) y su tecnología queda
  **UNKNOWN** a propósito; la reconciliación depende de la disponibilidad de la API del
  exchange.
- **Contratos BC-NN que lo hacen cumplir:**
  - `BC-13` — el settlement trading→portfolio sigue por el puente existente (fill_sync +
  inyección); portfolio nunca importa trading.
  - `BC-43`/`BC-44` — el almacén de posiciones se instancia solo en el composition root
    de portfolio.
  - `BC-12` — risk no importa execution.
  - `BC-35` — los wire payloads (ordenes/posiciones) siguen en `shared/kafka/schemas/`.
- **Elimina:** `TradeTracker._open_positions` como fuente de posiciones; el P&L duplicado
  oms vs trade_record.

## No-objetivos (Non-goals)

- No se crea nuevo bounded context, ni nuevo event bus, ni infraestructura.
- No se decide la tecnología del TradeStore (Redis/Iceberg/Kafka u otra) — queda UNKNOWN.
- No se hace del exchange el SSOT del estado interno OCM.
- No se implementa el flujo de recovery en este ADR (solo semántica y ownership).

## Invariantes financieros

- **INV-09** — un restart no destruye una posición económicamente existente: posiciones
  y P&L realizado sobreviven al reinicio. Testeable (test de recovery).
- **INV-10** — execution price y valuation price son conceptos diferentes (el fill nunca
  se sustituye por mark ni por señal). Testeable.
- Adicionales: tras restart, las posiciones abiertas conservan qty+avg (testeable); el
  realized P&L persiste tras restart (testeable); una discrepancia posición OCM↔exchange
  genera alerta (testeable); la quantity de un partial fill nunca se pierde (testeable).

## Evidencia y autoridad (Evidence & Authority)

- **OFFICIAL_EXCHANGE_FACT:** `GET /v5/position/list` (`size`, `avgPrice`,
  `unrealisedPnl`, `cumRealisedPnl`); `GET /v5/position/closed-pnl` (`closedSize`,
  `openFee`, `closeFee`, `avgEntryPrice`, `avgExitPrice`, `closedPnl`)
  (`bybit-exchange.github.io/docs/v5/position`; `docs/knowledge/notes/bybit-perpetuals-
  reference.md` §6.4, §7.6).
- **CONCEPTUAL_KNOWLEDGE:** Aldridge 2010 (retornos netos auditables); Bybit §7.6 (el
  create-order es acuse asíncrono → el estado real se confirma/reconcilia con el
  exchange). Los libros son neutrales sobre el restart de un proceso OCM (no lo abordan).
- **OCM_EVIDENCE:** ADR-0006; `fill_sync.py` (único `on_fill_composite`); `oms.py:126-130`
  (estado en memoria); `trade_tracker.py:58-61` (sin persistencia); `manager.py:94-99`
  (risk en memoria); `redis_store.py` (posiciones persistidas sin qty).
- **OCM_RESEARCH_HYPOTHESIS:** la variante de fórmula de mark price de los símbolos
  target (candidata R2) — no bloquea este ADR.
- **UNVERIFIED:** `closed-pnl` como fuente histórica completa (pregunta abierta Q5);
  tolerancia de reconciliación de `cumRealisedPnl` (rounding/fees del exchange); mapeo
  `raw.get("average")` de CCXT (UNVERIFIED, ver ADR-0025).
- **Convergencia:** el exchange ofrece el estado de reconciliación; OCM aporta su journal
  para la semántica que el exchange no conoce. La decisión es arquitectónica OCM.

## Relación con la Knowledge Base

- **Influyó:** `bybit-perpetuals-reference.md` (§6.4 avg entry, §7.6 confirmación
  asíncrona, §5.2 unrealized mark-based); Aldridge (retornos auditables).
- **NO utilizado:** libros sobre restart de procesos (ninguno lo cubre — se declara el
  gap explícitamente); Kaufman (`COVERAGE_UNVERIFIED`); Oxford; los libros de
  data-engineering (no aportan a la semántica de recuperación financiera).
- **Decisión que NO puede derivarse de libros:** el SSOT del estado financiero interno y
  el flujo de recovery son decisiones de arquitectura OCM (ADR-0006 + fill_sync + gap de
  persistencia actual), no derivables de literatura.
- **Documentación oficial con precedencia:** Bybit (estado de reconciliación disponible)
  sobre libros.
- **Regla:** BOOK ≠ CONTRACT; BOOK ≠ ADR; BOOK ≠ OCM_EVIDENCE.

## Implicaciones de implementación

- `packages/portfolio/models/position.py` + stores: quantity y avg_entry persistidos
  (F4a/F4b, ADR-0025).
- **TradeStore (journal de trades cerrados): tecnología UNKNOWN por decisión explícita**
  (F6a). Semántica y ownership definidos aquí; la tecnología se decide en implementación.
- `packages/trading/bootstrap/composition_root.py` / portfolio root: wiring de
  rehidratación de posiciones y reconstrucción de risk (F6a).
- Adapter live: reconciliación con `position/list` y `closed-pnl` (F6b); discrepancia →
  alerta.
- Eliminar duplicados de trading (`TradeTracker._open_positions`, `OMS._entry_prices`).

## Referencias

- Código: `packages/trading/execution/oms.py`, `packages/trading/execution/fill_sync.py`,
  `packages/trading/analytics/trade_tracker.py`, `packages/trading/risk/manager.py`,
  `packages/portfolio/models/position.py`, `packages/portfolio/infra/redis_store.py`,
  `packages/trading/bootstrap/composition_root.py`.
- ADRs relacionados: ADR-0006 (portfolio posee el estado de posiciones), ADR-0025 (cost
  basis), ADR-0026 (fees), ADR-0016 (live executor real), ADR-0012 (composition root),
  ADR-0024 (dirección microservicios).
- Doc oficial: `https://bybit-exchange.github.io/docs/v5/position`.
- KB: `docs/knowledge/notes/bybit-perpetuals-reference.md` (§5.2, §6.4, §7.6).