# Falsación Dirigida Final — OCM

**Fecha:** 2026-08-16
**Tipo:** auditoría forense independiente, SOLO LECTURA.
**Fuente de verdad:** estado actual del repositorio (verificado por inspección directa). No se confió en informes previos ni en memoria.

---

## 1. Alcance

Claims falsados (F1–F4):

- **F1** — `WSTradesSource` es stub/no implementado; clase en `ws_trades_source.py:32`; NOT_IMPLEMENTED ≈ línea 14; `is_running` permanece falso porque `_running` no pasa a True.
- **F2** — Existe detección de silencio y recovery en `GapAwareStream`, pero no existe estado de freshness/stale-data consultable ni política contractual/enforcement entre Market Data y Execution.
- **F3** — `MarketDataSourcePort` en `ports/outbound/` es huérfano: sin consumidores reales y docstring referencia implementaciones inexistentes.
- **F4** — La posición no solo está duplicada: hay divergencia semántica real entre representaciones (TradeTracker reemplaza en pyramid sin WAC; pop incondicional en SELL; OMS/Risk/Portfolio usan otra semántica).

Fuera de alcance: Parte 3/4, Gap Analysis, Roadmap, veredicto arquitectónico global, contraste con otros ADR, y todo claim no listado arriba.

---

## 2. Método de falsación

Inspecciones realmente ejecutadas (comandos sobre el estado actual del repo):

- `ls -la` / `sed -n` / `grep -n` sobre `ws_trades_source.py`, `gap_aware_stream.py`, `trades_source.py`, `market_data_source.py` (inbound y outbound), `trade_tracker.py`, `oms.py`, `manager.py`, `portfolio_service.py`, `fill_sync.py`, `settlement.py`, `trade_record.py`, `engine.py`, `feed_registry.py`, `feed_orchestrator.py`, `bybit_cryptofeed_runner.py`, `feed_runner_protocol.py`, `invariants.py`, `metrics.py`, `boundaries.py`, `composition_root.py` (trading y market_data).
- Búsquedas globales con `grep -rn` en `packages apps shared` (y `.` con `--include`) de: `MarketDataSourcePort`, `CcxtRestAdapter`, `ReplayAdapter`, `RawOHLCV`, `check_dataset_invariants`, `ocm_silver_freshness_seconds`, `freshness|stale|lag` en trading/portfolio, `_running`, `_open_positions`, `_entry_positions`, `Settlement.compute`.
- Validación Git: `git status --short`, `git diff --stat`, `git diff --cached --stat`, `git diff -- <ADR-0029>`, `git diff -- <ADR-0030>`, `git diff -- docs/plans/tracking.yaml`.

Cada conclusión se escribió después de su inspección; ninguna antes.

---

## 3. F1 — WSTradesSource

**Claim original:**
`WSTradesSource` está implementado como stub/no implementado; la clase está en `ws_trades_source.py:32`; existe indicación NOT_IMPLEMENTED ≈ línea 14; `is_running` permanece falso porque `_running` no pasa a True.

**Intento de falsación:**
Búsqueda de un camino real que ponga `_running = True`, subclases funcionales, implementaciones alternativas y callers de start/stop.

**Evidencia primaria:**
- Clase real: `class WSTradesSource` en `packages/market_data/adapters/inbound/websocket/ws_trades_source.py:32` (verificado con `grep -n`).
- NOT_IMPLEMENTED: línea **14** — "NOT_IMPLEMENTED — emite StopAsyncIteration inmediatamente." (docstring del módulo, L12-15: "Estado actual").
- Asignaciones reales de `_running` (verificado `grep -n "_running"`):
  - `:58` — `self._running = False` (en `__init__`).
  - `:96` — `self._running = False` (en `stop()`).
  - `:79` — `return self._running` (en `is_running`).
  - **Ninguna asignación a True.** `__aiter__` (`:81-82`) solo hace `return self`; NO setea `_running`.
- No existe método `start()` en la clase (verificado `grep -n "def start"` → sin match). El ciclo de vida es `__aiter__`/`__anext__`/`stop`.
- `__anext__` (`:84-90`): loguea warning y `raise StopAsyncIteration` (TODO "implementar conexión WS real" en `:85`).
- `is_running` (`:77-79`) siempre devuelve `False` (inicializado False, nunca True).
- Callers encontrados:
  - Instanciado en `packages/market_data/infrastructure/bootstrap/pipeline_factory.py:417` (`ws_source = WSTradesSource(...)`).
  - Envuelto en `GapAwareStream` (`pipeline_factory.py:437`).
  - Referenciado en docstrings/`__all__`: `websocket/__init__.py:12,14`, `rest/__init__.py:25,28`, `source_manager.py:32,39`, `ports/inbound/trades_source.py:20,75`.
  - No hay subclases (`grep "WSTradesSource)"` → solo comentarios/docstrings; ninguna subclasificación).
- Implementación funcional alternativa REAL (contraclaim potencial): `BybitFeedAdapter` (`bybit_feed_adapter.py:43`) + `BybitCryptofeedRunner` (`bybit_cryptofeed_runner.py:34`) usan cryptofeed real (`FeedHandler`, `Bybit`, `TRADES`, `:61-76`) y producen `NormalizedTrade` vía `_translate_and_dispatch` (`:78-108`). Están registrados en `feed_registry.py:20-21` ("bybit" → `BybitFeedAdapter`, "kucoin" → `KuCoinFeedAdapter`) y cableados por `FeedOrchestrator` (`feed_orchestrator.py:139-157`, `composition_root.py:149-221`).

**Contraevidencia encontrada:**
- No existe camino que ponga `_running = True`. El claim literal sobre `WSTradesSource` no se refuta.
- Matiz: la afirmación de que el sistema no tiene WS funcional sería FALSA — existe la vía `BybitFeedAdapter`→`BybitCryptofeedRunner` (cryptofeed), independiente de `WSTradesSource`. Pero el claim F1 se refiere exclusivamente a la clase `WSTradesSource`, no al sistema WS en general.

**Análisis:**
El claim sobre la clase `WSTradesSource` es exacto y reproducible. La indicación NOT_IMPLEMENTED está en L14 (no "alrededor de la línea 14"; exactamente en L14). La línea de la clase es :32. `is_running` siempre False. La existencia de la vía alternativa funcional no contradice el claim, pero limita su severidad: el stub es una vía del pipeline (`source_manager`/`GapAwareStream`), no la única vía WS del sistema.

**Clasificación final:** **VERIFIED.**

---

## 4. F2 — Freshness / Silence / Recovery

**Claim original:**
Existe detección de silencio y recovery en `GapAwareStream`, pero no existe un estado de freshness/stale-data consultable y utilizable como política contractual/enforcement entre Market Data y Execution.

**Intento de falsación:**
Búsqueda de estado consultable, contrato o enforcement de freshness bajo cualquier nombre (freshness/stale/timestamp/age/lag/sequence/silence/recovery), callers reales de `check_dataset_invariants`, emisión de `ocm_silver_freshness_seconds`, y enforcement previo a orden.

**Evidencia primaria:**

1. **Detección interna: EXISTE.**
   - `gap_aware_stream.py:258-263`: `_next_from_source` envuelve `self._source.__anext__()` con `asyncio.wait_for(timeout=gap_threshold_ms/1000)` si `gap_threshold_ms > 0`.
   - `:214-216`: `except asyncio.TimeoutError:` → `await self._handle_silence_gap()`.
   - `:265-285`: `_handle_silence_gap` calcula `[last_trade_ms+1, now_ms]` y ejecuta recovery.
   - `:85`: default `_DEFAULT_GAP_THRESHOLD_MS = 30_000`; `:56` `0` deshabilita.
2. **Recuperación: EXISTE.**
   - `:314-344`: `_run_recovery` instancia `GapRecoveryFetcher` vía `recovery_factory`, valida `source is TradeSource.REST_RECOVERY` (`:325`), buffer; fail-soft (`:338-344`).
   - `:287-312`: `_handle_disconnection` → recovery + restart.
   - `:346-380`: `_restart_source` con backoff (`_MAX_RESTART_ATTEMPTS=3`, `_RESTART_BACKOFF_S=2.0`).
   - `pipeline_factory.py:421-433`: `_make_recovery_factory` → `GapRecoveryFetcher`.
   - OHLCV: `ohlcv_fetcher.py:476-487` "Stale window — aborting"; `ccxt_adapter.py:563,574` `is_healthy()` consumido en `ohlcv_fetcher.py:276`.
3. **Estado consultable: NO EXISTE.**
   - `TradesSourceProtocol` (`ports/inbound/trades_source.py:89-118`): miembros `__aiter__`, `__anext__`, `stop`, `is_running`, `source_id`. **Ningún campo de freshness** (ni last_trade_ms, stale, lag, age).
   - `GapAwareStream` expone solo `is_running` (`:159-161`) y `source_id` (`:163-166`). `_last_trade_ms` (`:142`) es privado y solo se usa internamente (`:211,272,299`).
   - `TradesSourceManager.is_running` (`source_manager.py:142-143`) delega en el flag del stream; no es freshness.
   - `_check_dataset_lag`/`check_dataset_invariants` (`domain/quality/invariants.py:133-161,176-240`): definidos. **Callers reales: NINGUNO en producción.** `grep check_dataset_invariants` → solo su propio `__init__.py:13` (reexport) y docstring de uso (`invariants.py:20-22`). El lag check (`:233-238`) se marca como warning, y no hay código que lo invoque.
   - Métrica `ocm_silver_freshness_seconds` (`infrastructure/observability/metrics.py:40`): solo comentario documental; `grep ocm_silver_freshness_seconds` → 1 match (el comentario). **Nunca se emite.**
4. **Contrato: NO EXISTE.**
   - `FeatureSource` (`shared/contracts/boundaries.py:21-37`): `load_features(exchange, symbol, timeframe, market_type, **kwargs)` — sin parámetro ni gate de freshness.
   - `GoldReader.load_features` (`adapters/outbound/storage/gold_reader.py:128`), `feature_reader.py:52`, `_GoldFeatureSource.load_features` (`trading/bootstrap/composition_root.py:156-168`): ninguno verifica staleness.
5. **Propagación: NO EXISTE.**
   - `RawTrade` (`domain/value_objects/raw_trade.py:118-145`): campos exchange/market_type/symbol/trade_id/timestamp_ms/price/amount/side/source/cost/timestamp_utc. Sin campo de freshness.
   - `grep freshness|stale|lag|age` en `packages/trading` y `packages/portfolio` → **0 matches** (frontera de ejecución sin rastro de freshness).
   - `engine.py:332-346` `_load_data()`: llama `load_features` sin check; `_last_timestamp()` (`:325-331`) devuelve `datetime.now()`, no deriva del dato.
6. **Enforcement antes de ejecutar órdenes: NO EXISTE.**
   - `OMS.submit` (`oms.py:194-235`): solo guard (kill switch) + `risk.validate` + sizing. Sin check de freshness de datos.
   - `RiskManager.validate` (`manager.py:329-367`): reglas de entrada (max positions, order usd) sobre estado propio; sin freshness de datos de mercado.

**Contraevidencia encontrada:**
- La detección y recovery operativos existen (no se puede afirmar "no existe freshness" en absoluto). El claim F2 NO dice eso: el claim exacto separa detección/recovery (existen) de estado consultable/contrato/enforcement (no existen) — y esa separación es correcta y verificada.

**Análisis:**
El claim sobrevive íntegramente: la separación en 6 niveles es reproducible desde el código. Específicamente: (1) detección interna = existe; (2) recuperación = existe; (3) estado consultable = no existe (sin getter de last_trade_ms en el port, sin campo de freshness en RawTrade/DTOs); (4) contrato = no existe (TradesSourceProtocol y FeatureSource sin freshness); (5) propagación = no existe (0 matches en trading/portfolio); (6) enforcement pre-ejecución = no existe (OMS.submit y Risk.validate sin freshness).

**Clasificación final:** **VERIFIED.**

---

## 5. F3 — MarketDataSourcePort

**Claim original:**
`MarketDataSourcePort` ubicado en `ports/outbound/` es huérfano: no tiene consumidores reales y su docstring referencia implementaciones inexistentes.

**Intento de falsación:**
Búsqueda global de definición, imports, implementaciones, instanciaciones, inyección de dependencias, composition roots, adapters, callers, tests, reexports y referencias indirectas (incl. `RawOHLCV` y `MarketDataSourcePort` con o sin prefijo de módulo).

**Evidencia primaria:**
- **Definición:** `class MarketDataSourcePort(Protocol)` en `packages/market_data/ports/outbound/market_data_source.py:32`; `@runtime_checkable` (`:31`); único método `fetch_ohlcv` (`:42-48`); `RawOHLCV = Sequence[Sequence]` (`:28`); `__all__ = ["RawOHLCV", "MarketDataSourcePort"]` (`:65-68`).
- **Referencias globales:** `grep -rn "MarketDataSourcePort" . --include="*.py" --include="*.md" --include="*.yaml" --include="*.toml"` → solo aparece en su propio archivo (`:32,:67`) y en los informes de auditoría previos (documentales). **0 referencias de código fuera del propio archivo.**
- **Imports:** `grep "ports.outbound.market_data_source"` → sin matches en `packages apps` (vacío).
- **Implementaciones citadas en docstring (`:36-40`):** `market_data.adapters.inbound.rest.ccxt_adapter.CcxtRestAdapter` y `market_data.adapters.inbound.stream.replay_adapter.ReplayAdapter (futuro)`. **No existen:** `grep "class CcxtRestAdapter|class ReplayAdapter"` → sin match; `ls packages/market_data/adapters/inbound/rest/` → no hay `ccxt_adapter.py` (el real está en `outbound/exchange/ccxt_adapter.py`); no existe carpeta `adapters/inbound/stream/` ni ningún `replay_adapter.py`.
- **Instanciaciones/wiring:** ninguna (`grep` global → 0).
- **Tests:** 0.
- **Reexports:** `ports/__init__.py`, `ports/outbound/__init__.py`, `ports/inbound/__init__.py` → sin referencias a `market_data_source` (grep → vacío). `RawOHLCV` no se usa en ningún otro archivo (`grep RawOHLCV` → solo en su propio archivo).
- **Contraste inbound:** `ports/inbound/market_data_source.py:24` define `class MarketDataSource(Protocol)` con `TradeCallback` (`:20`) y tiene **8 consumidores reales**: `feed_runner_protocol.py:28`, `bybit_cryptofeed_runner.py:31`, `kucoin_cryptofeed_runner.py:26`, `bybit_feed_adapter.py:37`, `kucoin_feed_adapter.py:21`, `feed_orchestrator.py:32`, `feed_registry.py:14`.
- **Contradicción interna adicional:** el docstring del archivo outbound (`market_data_source.py:6`) se autodeclara "Puerto INBOUND (driving side)" pese a vivir en `ports/outbound/`.

**Contraevidencia encontrada:**
- Ninguna. No existe uso estructural indirecto (ningún signature usa `MarketDataSourcePort` como tipo), ni duck-typing invocable, ni implementación nominal, ni reexport.

**Análisis:**
El claim es exacto y reforzado: 0 consumidores reales, 0 implementaciones, docstring con rutas inexistentes, y contradicción inbound/outbound en el propio archivo.

**Clasificación final:** **VERIFIED.**

---

## 6. F4 — Position State

**Claim original:**
La posición no solo está duplicada: existe divergencia semántica real entre representaciones. `TradeTracker` reemplaza la posición durante pyramid (sin WAC) y hace pop incondicional en SELL, mientras OMS/Risk/Portfolio usan otra semántica.

**Intento de falsación:**
Inspección de `trade_tracker.py`, `oms.py`, `manager.py`, `portfolio_service.py`, `fill_sync.py`, `settlement.py`, `trade_record.py`; verificación de pyramid/WAC/pop/close_position y construcción de un escenario reproducible.

**Evidencia primaria:**

**Escritores reales (4 estructuras):**
1. `OMS._entry_positions` — `oms.py:177` `dict[str, tuple[float, float]]`. BUY: WAC (`:414-419`): `new_qty = prev_qty + filled_qty`; `new_avg = (prev_qty*prev_avg + filled_qty*fill_price)/new_qty` si `prev_qty>0`, si no `fill_price`. SELL: `:439-451` — `closed_qty = min(filled_qty, prev_qty)`; `remaining = prev_qty - closed_qty`; `remaining>0` → `(remaining, avg)`; si no → `pop`.
2. `RiskManager._positions` — `manager.py:135` `dict[str, tuple[float, Optional[float]]]`. Push desde OMS via `record_position` (`manager.py:190,193`; llamado en `oms.py:421,423,456,464`). SELL: `remaining` o `pop` si ≤0.
3. `TradeTracker._open_positions` — `trade_tracker.py:59` `dict[str, Order]` (guarda el objeto `Order` completo, no tupla).
4. `PositionStore` (persistido) — `portfolio_service.py:81` (`self._store`). WAC multi-entry (`:120-148`): `existing.quantity + quantity`; `new_avg = (existing.quantity*existing.avg_entry + quantity*avg_entry)/new_qty`. SELL: `close_position` (`:177-260`) — `get(order_id)`; si `remaining <= 1e-12` → `delete` (`:234`); si no → `save(updated)` (`:255`); si no existe o falla → `(None, 0.0)` (`:210-213,258-259`).

**Verificación específica de los claims:**
1. **Pyramid en TradeTracker: REEMPLAZA (no WAC).** `trade_tracker.py:137-146`: "if order.symbol in self._open_positions: ... (estrategia pyramid no soportada aún)" → `self._open_positions[order.symbol] = order`. Sobrescribe el `Order` de apertura; no acumula cantidad ni calcula WAC. El `Order` reemplazado conserva su propio `fill_price` (del último BUY), no el WAC.
2. **¿TradeTracker calcula WAC?** No. No hay ninguna operación WAC en `trade_tracker.py` (grep `new_avg|weighted|avg` → solo en comentarios de docstring del módulo).
3. **SELL en TradeTracker: pop incondicional.** `trade_tracker.py:158`: `entry_order = self._open_positions.pop(order.symbol, None)`. Elimina el símbolo pase lo que pase (cierre total o parcial).
4. **PortfolioService reduce parcialmente.** `portfolio_service.py:238-255`: `remaining = position.quantity - closed_qty`; si `remaining > 1e-12` → `save(updated)` conservando la posición abierta.
5. **Divergencia demostrable — escenario reproducible:**

   **Escenario 1 — SELL parcial (misma secuencia de fills):**
   - BUY qty=2, fill_price=100 → OMS `(2, 100)`; Risk `(2, 100)`; TradeTracker `_open_positions[sym]=Order(BUY)`; PositionStore `qty=2, avg=100`.
   - SELL qty=1, fill_price=150 → OMS `(1, 100)` (reduce); Risk `(1, 100)`; TradeTracker `pop` → `_open_positions[sym]` **desaparece**; PositionStore `qty=1, avg=100` (permanece).
   - Estado resultante: **TradeTracker reporta posición cerrada (sin símbolo) mientras PositionStore mantiene qty=1.** Un segundo SELL del símbolo: TradeTracker → "SELL sin BUY correspondiente" (`:161-163`), ignorado; `fill_sync` aún tiene `open_order_ids[sym]` (se conserva porque `remaining>0`, `fill_sync.py:152-155` no hace pop) → `close_position` se llamaría de nuevo sobre un estado que TradeTracker ya no ve.

   **Escenario 2 — Pyramid (2 BUY + 1 SELL):**
   - BUY1 qty=1 @100 → TradeTracker `_open_positions[sym]=Order1`; OMS/Risk/Portfolio `(1, 100)`.
   - BUY2 qty=1 @200 → TradeTracker `_open_positions[sym]=Order2` (REEMPLAZA; "pyramid no soportada"); OMS `(2, 150)` WAC; Risk `(2, 150)`; Portfolio `qty=2, avg=150` (merge `:120-148`).
   - SELL qty=1 @250 → OMS `Settlement.compute(avg_entry_price=avg)` con **avg=150 (WAC)** (`oms.py:459-463`); `order.settlement` set. TradeTracker `_register_close` → `entry_order = Order2` (reemplazado); en el **path canónico F3** consume `settlement` (`trade_tracker.py:174-181` → `TradeRecord.from_settlement`, `trade_record.py:165-200`) → P&L correcto (WAC 150). En el **path legacy** (settlement None, `:191-220`) → `avg_entry_price = entry_order.fill_price` (`:197`) = **200 (precio del BUY2, no WAC)** → P&L divergente.

6. **Matización importante:** en el camino canónico (settlement siempre presente, ADR-0026/F3), la economía del P&L proviene del `Settlement` (WAC correcto), no del Order de TradeTracker; por lo tanto la divergencia de **precio/P&L** por pyramid solo materializa en el path legacy. La divergencia de **estado de posición** (presencia/ausencia del símbolo) SÍ materializa siempre en el SELL parcial (Escenario 1), independiente del settlement.

**Contraevidencia encontrada:**
- El path canónico (F3) mitiga la divergencia de P&L por pyramid (usa Settlement con WAC). Esto matiza la severidad del claim en su componente de "cálculo", pero NO elimina: (a) la divergencia de estado en SELL parcial (Escenario 1, siempre), (b) la divergencia de representación durante pyramid (`Order2` reemplaza `Order1`; TradeTracker expone `open_positions()[sym]` como el Order del último BUY — `trade_tracker.py:94-101` — con `fill_price`=200, no WAC).

**Análisis:**
El claim de divergencia semántica es DEMOSTRABLE mediante los dos escenarios reproducibles. La formulación es precisa en "reemplaza en pyramid (sin WAC)" y "pop incondicional en SELL" vs "WAC en OMS/Risk/Portfolio". Único matiz: el settlement canónico corrige el P&L por pyramid; la divergencia persistente es la de estado de posición abierta (SELL parcial) y la de representación expuesta.

**Clasificación final:** **VERIFIED** (con matiz: la divergencia de P&L por pyramid queda mitigada por el path canónico settlement; la divergencia de estado y de representación persiste).

---

## 7. Matriz de falsación

| Claim | Evidencia primaria | Contraevidencia | Resultado | Severidad |
|---|---|---|---|---|
| F1 — WSTradesSource stub; clase :32; NOT_IMPLEMENTED :14; `_running` nunca True | `ws_trades_source.py:32,14,58,79,84-90`; `grep _running` (solo False); callers `pipeline_factory.py:417,437` | Vía WS funcional alternativa `BybitFeedAdapter`+`BybitCryptofeedRunner` (no refuta el claim sobre la clase) | **VERIFIED** | Baja |
| F2 — detección/recovery internos existen; estado consultable/contrato/enforcement no existen | `gap_aware_stream.py:258-263,265-285,314-380`; `trades_source.py:89-118` sin freshness; `boundaries.py:21-37` sin gate; `engine.py:332-346`; `oms.py:194-235`; `check_dataset_invariants` sin caller; `ocm_silver_freshness_seconds` sin emisión | No hay contraevidencia que refute la distinción de niveles | **VERIFIED** | Media |
| F3 — MarketDataSourcePort outbound huérfano; docstring con implementaciones inexistentes | `ports/outbound/market_data_source.py:32`; grep global → 0 refs de código; `CcxtRestAdapter`/`ReplayAdapter` inexistentes; inbound con 8 consumidores; autodeclaración "INBOUND" en `:6` | Ninguna | **VERIFIED** | Media |
| F4 — divergencia semántica real (pyramid reemplaza; pop incondicional; OMS/Risk/Portfolio otra semántica) | `trade_tracker.py:137-146,158`; `oms.py:414-419,439-451,459-463`; `manager.py:190,193`; `portfolio_service.py:120-148,238-255`; escenarios 1 y 2 reproducibles | Path canónico settlement mitiga P&L por pyramid (no la divergencia de estado ni de representación) | **VERIFIED** (con matiz) | Alta |

---

## 8. Claims falsados

- **Ninguno.** No se encontró evidencia que refutara ninguno de los cuatro claims (F1–F4). El intento activo de falsación fracasó en los cuatro.

---

## 9. Claims sobrevivientes

1. **F1** — `WSTradesSource` es stub: clase en `ws_trades_source.py:32`, NOT_IMPLEMENTED en `:14`, `is_running` siempre False (`_running` solo se asigna False en `:58,:96`, nunca True). Sobrevivió porque el intento de hallar un camino a `_running=True`, subclases funcionales o una implementación alternativa que haga funcional a esta clase específica fracasó.
2. **F2** — La distinción de niveles sobrevive: detección interna y recovery existen en `GapAwareStream`; estado consultable, contrato (`TradesSourceProtocol`/`FeatureSource`), propagación y enforcement pre-ejecución NO existen. Sobrevivió porque la búsqueda bajo nombres alternativos (stale/age/lag/sequence/silence/heartbeat) no encontró estado, contrato ni enforcement en la frontera Market Data → Execution.
3. **F3** — `MarketDataSourcePort` outbound es huérfano: 0 consumidores, 0 implementaciones, 0 wiring, 0 tests, 0 reexports; docstring con implementaciones inexistentes. Sobrevivió porque la búsqueda global (incl. `RawOHLCV`, reexports, `runtime_checkable`, composition roots) no halló ningún uso.
4. **F4** — Divergencia semántica demostrable con 2 escenarios reproducibles. Sobrevivió (con matiz) porque el pop incondicional de `TradeTracker._register_close` y el reemplazo en pyramid son código real, y la divergencia de estado (SELL parcial) y de representación (pyramid) son reproducibles.

---

## 10. UNKNOWN

- **Ninguno.** Los 4 claims son resolubles mediante inspección estática del estado actual del repositorio. No falta evidencia.
- Nota metodológica (no es UNKNOWN): no se ejecutó runtime; el comportamiento observado de los caminos (p. ej. qué ocurre con un fill real de SELL parcial en producción) se infiere de código y docstrings, no de observación en ejecución. Esta limitación aplica por igual a cualquier conclusión aquí, y no impide la resolución de los claims.

---

## 11. Correcciones a informes anteriores

Solo errores factuales demostrados en el estado actual del repo:

1. **`WSTradesSource` en `:58` (informe Parte 2 original) → `:32`.** La clase está en `ws_trades_source.py:32`; la línea `:58` es `self._running = False` (dentro de `__init__`). Demostrado por `grep -n "class WSTradesSource"` → `32` y `sed -n '54,60p'` → `self._running = False`.
2. **Docstring "L9-11" (informe Parte 2 original) → `L14`.** La afirmación "NOT_IMPLEMENTED — emite StopAsyncIteration inmediatamente." está en `ws_trades_source.py:14`. Demostrado por `sed -n '9,16p'` (L14 contiene la afirmación).
3. **Confirmación adicional (ya señalada en falsaciones previas, re-verificada aquí):** el docstring de `ports/outbound/market_data_source.py:6` se autodeclara "Puerto INBOUND (driving side)" pese a residir en `ports/outbound/`. Contradicción interna demostrada por lectura del archivo.
4. **Confirmación adicional:** `CcxtRestAdapter` y `ReplayAdapter` (implementaciones de referencia citadas en el docstring del port outbound) NO existen en el repositorio; tampoco existe la carpeta `adapters/inbound/stream/`. Demostrado por `grep` global y `ls`.

No se registran más errores. Ninguna corrección especulativa.

---

## 12. Validación final

Ejecutado al final de la falsación:

```
git status --short
?? docs/audits/2026-08-15-auditoria-integral-market-data-naming-estructura.md
?? docs/audits/2026-08-15-auditoria-ocm-parte-1-market-data.md
?? docs/audits/2026-08-15-auditoria-ocm-parte-2-naming-responsibility.md
?? docs/audits/2026-08-15-benchmark-complementario-lean-vnpy-ccxtpro.md
?? docs/audits/2026-08-16-falsacion-dirigida-parte-2.md
?? docs/audits/2026-08-16-verificacion-forense-parte-2-naming-responsibility.md
?? docs/audits/2026-08-16-falsacion-dirigida-final.md   <-- este entregable

git diff --stat
(sin output — código tracked sin modificaciones)

git diff --cached --stat
(sin output — sin staged changes)

git diff -- docs/architecture/decisions/ADR-0029-cancelacion-real-gestion-ordenes-abiertas.md
(sin output — 0 líneas)

git diff -- docs/architecture/decisions/ADR-0030-balance-real-reconciliacion-patrimonial.md
(sin output — 0 líneas)

git diff -- docs/plans/tracking.yaml
(sin output — 0 líneas)
```

Confirmación explícita:

```
CÓDIGO MODIFICADO: NO
ADR-0029 MODIFICADO: NO
ADR-0030 MODIFICADO: NO
TRACKING MODIFICADO: NO
COMMIT: NO
PUSH: NO
```

---

## 13. Veredicto de falsación

**C — VALIDADO**

Derivación exclusiva de F1–F4:

- F1: VERIFIED — no se encontró camino que refute el stub.
- F2: VERIFIED — la distinción de niveles (detección/recovery existen; estado/contrato/enforcement no existen) es reproducible.
- F3: VERIFIED — huérfano demostrado con búsqueda global.
- F4: VERIFIED (con matiz) — divergencia semántica demostrable con escenarios reproducibles.

El intento de falsación fracasó en los 4 claims: ninguno fue refutado. Todos se confirman con evidencia primaria del estado actual del repositorio. La calidad exigida (reproducibilidad por un tercero) se cumple: cada afirmación cita ruta, símbolo, líneas y comando de verificación.

---

### Anexo de validación del entregable

- **ARCHIVO CREADO:** `docs/audits/2026-08-16-falsacion-dirigida-final.md` (este archivo).
- **ARCHIVOS MODIFICADOS:** ninguno más.
- **CÓDIGO MODIFICADO:** NO.
- **ADRs MODIFICADOS:** NO.
- **tracking.yaml MODIFICADO:** NO.
- **INFORMES PREVIOS MODIFICADOS:** NO.
- **COMMIT:** NO.
- **PUSH:** NO.