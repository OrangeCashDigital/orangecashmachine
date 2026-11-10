# ADR-0059: Freshness de datos de mercado — contrato, propagación y enforcement pre-orden

**Estado:** Propuesto
**Fecha:** 2026-11-09
**Bounded context(s) afectado(s):** market_data (propietario del contrato), trading (consumidor), portfolio (consumidor), shared (contratos cruzados)

## Contexto

La regla del linter `ARCH-005` reporta `FAIL` porque la cadena Market Data → Strategy → Risk → Execution tiene detección de silencio y recovery, pero **no tiene freshness como estado consultable ni propagación ni enforcement pre-orden** (issue #122 / B-MD-001).

Hechos verificables en `main`:

- **Nivel 1 (detección interna) — PRESENTE:** `packages/market_data/adapters/inbound/websocket/gap_aware_stream.py` implementa `wait_for` (línea 259), `asyncio.TimeoutError` (línea 214), `_handle_silence_gap()` (línea 265), `gap_threshold_ms` con default 30_000 ms (línea 85), y `_restart_source()` con backoff exponencial (línea 346).
- **Nivel 2 (recovery) — PRESENTE:** `GapRecoveryFetcher` instanciado vía `recovery_factory` (`gap_aware_stream.py` líneas 21, 78, 314-344) y construido en `packages/market_data/infrastructure/bootstrap/pipeline_factory.py` (líneas 433, 460-462, 475).
- **Nivel 3 (estado consultable en port) — AUSENTE:** `packages/market_data/ports/inbound/trades_source.py` (`TradesSourceProtocol`) no declara ninguno de `freshness`, `stale`, `lag_ms`, `last_trade_ms`, `age_ms`, `stale_ms`, `sequence_gap`.
- **Nivel 4 (contrato en boundaries/ports) — AUSENTE:** `shared/contracts/boundaries.py` no declara ningún contrato de freshness para datos de mercado. (Solo existe `age_ms()` en `BalanceSnapshot` línea 188, pero es para balance B-MD-009, no para freshness de market data.)
- **Nivel 5 (propagación a trading/portfolio) — AUSENTE:** `packages/trading/` y `packages/portfolio/` no hacen referencia a ningún símbolo de freshness.
- **Nivel 6 (enforcement pre-orden) — AUSENTE:** `packages/trading/application/` y `packages/trading/risk/` no hacen referencia a ningún símbolo de freshness.

`B-MD-001` **no aparece** en `docs/plans/tracking.yaml`.

ADR-0022 (Lifecycle de `realtime_feeds`) menciona freshness/heartbeat como trabajo futuro, pero no define contrato ni enforcement.

## Problema

Sin un estado de freshness consultable, un feed congelado (stale) es indistinguible de un mercado quieto. `RiskManager` computa sizing y drawdown, y `OMS` envía órdenes, sin saber si los datos subyacentes están actualizados. Esto puede disparar decisiones incorrectas con capital real en modo live.

## Decisión propuesta

Definir `FreshnessPort` como contrato de estado consultable de freshness de datos de mercado, ubicado en `packages/market_data/ports/outbound/freshness_port.py` (capa `ports/outbound` de `market_data`, respetando el orden `domain < ports < application < adapters < infrastructure` y el contrato BC-08). `market_data` es el productor que expone freshness hacia afuera, por tanto es un puerto outbound desde la perspectiva de `market_data` y un puerto inbound desde la perspectiva de `trading`/`portfolio`.

El `FreshnessPort` define:

- **Firmas y tipos:** `last_seen_ms: int`, `lag_ms: int`, `is_stale: bool`, umbral configurable por tipo de stream, `staleness_reason: str | None`.
- **Errores:** `FreshnessError` (base), `FreshnessUnknownError` (no hay dato de referencia), `FreshnessThresholdExceededError` (datos obsoletos).
- **Umbrales de freshness:** por tipo de stream (WS trades, REST backfill, recovery, replay), configurables vía Hydra, con valores por defecto justificados (ej. WS trades líquido: 5-10 s; WS bajo liquidez: 30 s; REST: 60 s).
- **Propagación:** `market_data` expone `FreshnessPort`; `trading` y `portfolio` lo consumen a través de sus propios puertos/contratos, sin importar directamente la implementación de `market_data` (respeta BC-10, BC-13).
- **Enforcement pre-orden:** `OMS.submit()` consulta `FreshnessPort` antes de enviar; si `is_stale`, bloquea la orden y emite alerta (fail-closed). **Esta fase solo define el contrato; no se implementa el bloqueo en `OMS`.**
- **Métricas:** `last_seen_ms` por símbolo expuesto a Prometheus con alerta de staleness.

El `FreshnessPort` se declara en `shared/contracts/boundaries.py` como protocolo cruzado (item 4 de ARCH-005), y su implementación concreta vive en `market_data/adapters/inbound/websocket/gap_aware_stream.py` (ya calcula `_last_trade_ms` y `gap_threshold_ms`).

## Alternativas evaluadas

1. **A — FreshnessPort dedicado en `market_data/ports/outbound/` + contrato en `shared/contracts/boundaries.py`.** Ventaja: separación clara, cumple BC-08 y BC-10. Costo: nuevo archivo de contrato + implementación en dos capas.
2. **B — Extender `TradesSourceProtocol` con campos de freshness.** Ventaja: un solo puerto. Costo: `TradesSourceProtocol` es inbound (stream de trades); mezclar freshness metadata con el stream de datos viola SRP e ISP, y el linter ARCH-005 distingue explícitamente el nivel 3 (port trades_source) del nivel 4 (contrato boundaries/ports).
3. **C — Freshness como métrica Prometheus sin contrato de código.** Ventaja: cero cambio de contrato. Costo: no es consultable desde `OMS`/`RiskManager`, no cumple enforcement pre-orden (item 6 de ARCH-005).

Se elige **A**.

## Consecuencias

- Queda más fácil: `RiskManager` y `OMS` pueden validar freshness antes de operar; alertas de staleness en Prometheus.
- Deuda aceptada conscientemente: el contrato se define pero no se implementa en esta fase; el enforcement pre-orden queda bloqueado hasta que el owner apruebe el ADR y se implemente.
- Contratos BC-NN que hacen cumplir esta decisión: BC-08 (orden de capas market_data), BC-10 (market_data no importa sibling BCs), BC-13 (portfolio aislado de trading).

## Mapeo a los items de ARCH-005

| Item ARCH-005 | Estado actual | Qué define este ADR |
|---------------|---------------|---------------------|
| 1-detección interna | PRESENTE (GapAwareStream) | No cambia |
| 2-recovery | PRESENTE (GapRecoveryFetcher) | No cambia |
| 3-estado consultable en port | AUSENTE | `FreshnessPort` en `market_data/ports/outbound/freshness_port.py` expone `last_seen_ms`, `lag_ms`, `is_stale` |
| 4-contrato (boundaries/ports) | AUSENTE | Protocolo `FreshnessPort` en `shared/contracts/boundaries.py` |
| 5-propagación a trading/portfolio | AUSENTE | `trading` y `portfolio` consumen `FreshnessPort` vía sus propios puertos, sin importar implementación de `market_data` |
| 6-enforcement pre-orden | AUSENTE | `OMS.submit()` consulta `FreshnessPort` antes de enviar; si `is_stale` → bloquear + alerta (fail-closed) |

## Plan de implementación por fases

1. **Fase 1 (este ADR):** Definir `FreshnessPort` contrato + umbrales + documentación de estado actual. No tocar código de producción.
2. **Fase 2:** Implementar `FreshnessPort` en `gap_aware_stream.py` (exponer `_last_trade_ms` como `last_seen_ms`, calcular `lag_ms`, `is_stale`).
3. **Fase 3:** Declarar protocolo en `shared/contracts/boundaries.py`; importar-linter BC-10/BC-13.
4. **Fase 4:** Propagar a `trading/` y `portfolio/` vía puertos propios; `OMS` consulta pre-submit.
5. **Fase 5:** Enforcement pre-orden (bloquear orden si stale) + métricas Prometheus + alerta.
6. **Fase 6:** Tests de contrato + gate de arranque live.

## Cambios pendientes de registro

- `docs/plans/tracking.yaml`: registrar `B-MD-001` (freshness) con `estado: PENDIENTE`, `prioridad: CRITICA`, `fase: F3`.
- `policies/registry.yaml`: si aplica, registrar política de enforcement pre-orden.
- Estas entradas se agregan solo cuando el owner apruebe el ADR y se autorice la implementación. **Este ADR no modifica esos archivos.**

## Referencias

- Código: `packages/market_data/adapters/inbound/websocket/gap_aware_stream.py` (detección + recovery existentes)
- Código: `packages/market_data/ports/inbound/trades_source.py` (TradesSourceProtocol, sin freshness)
- Código: `shared/contracts/boundaries.py` (contratos cruzados, sin freshness de market data)
- Código: `packages/trading/risk/manager.py` (RiskManager, consume capital_usd, no freshness)
- Código: `packages/trading/application/` (OMS, sin enforcement de freshness)
- ADR relacionados: ADR-0022 (lifecycle realtime_feeds, freshness como trabajo futuro), ADR-0030 (balance real B-MD-009, patrón similar de contrato pendiente)
- Regla linter: `architecture_linter/rules/arch_005.py` (ARCH-005, 6 niveles)
- Issue: #122 (ARCH-005: freshness chain)
- Tracking: `B-MD-001` no encontrado en `docs/plans/tracking.yaml`

## Decisiones que necesitan al owner

1. Aprobar este ADR (cambiar estado de "Propuesto" a "Aceptado").
2. Confirmar valores por defecto de umbrales de freshness por tipo de stream.
3. Autorizar registro de `B-MD-001` en `tracking.yaml`.
4. Autorizar modificación de `OMS.submit()` para enforcement pre-orden (fase 5).
5. Autorizar gate de arranque live condicionado a freshness (fase 6).
