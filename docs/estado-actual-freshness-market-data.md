# Estado actual de freshness de datos de mercado (B-MD-001 / ARCH-005)

**Fecha:** 2026-11-09
**Propósito:** Documentar qué existe y qué falta para que la cadena Market Data → Strategy → Risk → Execution tenga freshness como estado consultable, propagación y enforcement pre-orden. Solo lectura; no implementa nada.

## Qué EXISTE hoy (verificado en código)

### Detección de silencio (nivel 1 de ARCH-005) — PRESENTE

`packages/market_data/adapters/inbound/websocket/gap_aware_stream.py`:
- `GapAwareStream` — wrapper de resiliencia sobre `TradesSourceProtocol` (línea 95).
- `_DEFAULT_GAP_THRESHOLD_MS = 30_000` — 30 s de silencio → gap (línea 85).
- `asyncio.wait_for` con timeout en `_next_from_source()` (líneas 258-263) — detecta silencio.
- `asyncio.TimeoutError` capturado en `__anext__()` (línea 214) → trigger `_handle_silence_gap()`.
- `_handle_silence_gap()` (línea 265) — calcula rango `[last_trade_ms + 1, now_ms]` y ejecuta recovery.
- `_last_trade_ms` (línea 142) — cursor del último trade visto.

### Recovery (nivel 2 de ARCH-005) — PRESENTE

- `_run_recovery(gap_start_ms, gap_end_ms)` (línea 314) — instancia `GapRecoveryFetcher` vía `recovery_factory` y rellena buffer.
- `_handle_disconnection()` (línea 287) — recovery por desconexión WS + reinicio.
- `_restart_source()` (línea 346) — reconexión con backoff exponencial (máx. 3 intentos, `_RESTART_BACKOFF_S = 2.0`).
- `RecoveryFactory = Callable[[int, int], TradesSourceProtocol]` (línea 78).
- `GapRecoveryFetcher` construido en `packages/market_data/infrastructure/bootstrap/pipeline_factory.py` (líneas 433, 460-462, 475).

## Qué FALTA hoy (verificado en código)

### Estado consultable en el port (nivel 3 de ARCH-005) — AUSENTE

`packages/market_data/ports/inbound/trades_source.py` (`TradesSourceProtocol`) no declara ninguno de: `freshness`, `stale`, `lag_ms`, `last_trade_ms`, `age_ms`, `stale_ms`, `sequence_gap`. Solo declara `__aiter__`, `__anext__`, `stop`, `is_running`, `source_id`.

### Contrato en boundaries/ports (nivel 4 de ARCH-005) — AUSENTE

`shared/contracts/boundaries.py` no declara ningún contrato de freshness para datos de mercado. Solo existe `age_ms()` en `BalanceSnapshot` (línea 188), pero pertenece a balance (B-MD-009), no a freshness de market data.

### Propagación a trading/portfolio (nivel 5 de ARCH-005) — AUSENTE

`packages/trading/` y `packages/portfolio/` no hacen referencia a ningún símbolo de freshness. `RiskManager` recibe `capital_usd` por constructor, no consulta freshness.

### Enforcement pre-orden (nivel 6 de ARCH-005) — AUSENTE

`packages/trading/application/` y `packages/trading/risk/` no hacen referencia a ningún símbolo de freshness. `OMS.submit()` no consulta freshness antes de enviar.

## Tracking

`B-MD-001` **no aparece** en `docs/plans/tracking.yaml`.

## Linter ARCH-005

`architecture_linter/rules/arch_005.py` reporta `FAIL`:
- Presentes: 1-detención interna, 2-recovery.
- Ausentes: 3-estado consultable en port, 4-contrato (boundaries/ports), 5-propagación a trading/portfolio, 6-enforcement pre-orden.

## ADR relacionado

- `docs/architecture/decisions/ADR-0059-freshness-market-data.md` — ADR Propuesto que define `FreshnessPort`, umbrales, propagación y enforcement pre-orden.

## Decisiones que necesitan al owner

1. Aprobar ADR-0059.
2. Confirmar umbrales por defecto por tipo de stream.
3. Registrar `B-MD-001` en `docs/plans/tracking.yaml`.
4. Autorizar implementación en `gap_aware_stream.py` (fase 2) y declaración en `shared/contracts/boundaries.py` (fase 3).
5. Autorizar cambios en `OMS.submit()` para enforcement pre-orden (fase 5).
