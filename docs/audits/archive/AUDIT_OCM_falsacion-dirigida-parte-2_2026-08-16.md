# Falsación Dirigida — Parte 2/4 OCM (Naming / Responsibility / State Ownership)

**Fecha:** 2026-08-16
**Objeto falsado:** `docs/audits/2026-08-16-verificacion-forense-parte-2-naming-responsibility.md` (171 líneas).
**Informe base auditado (referencia):** `docs/audits/2026-08-15-auditoria-ocm-parte-2-naming-responsibility.md`.
**Rol:** falsación forense independiente, SOLO LECTURA. No se modificó ningún archivo del repositorio salvo la creación de este entregable.
**Resultado global:** 3 claims VERIFIED, 1 claim PARTIALLY VERIFIED, 0 FALSE, 0 UNKNOWN.

---

## 1. Objetivo y alcance

- Falsar de forma independiente los 4 claims que el informe forense anterior dejó como potencialmente incorrectos, incompletos o demasiado fuertes:
  - **F1 — WSTradesSource** (ubicación/citas de líneas).
  - **F2 — Freshness / stale-data** (claim literal: "no existe política de freshness").
  - **F3 — MarketDataSourcePort** (huérfano / sin consumidores / sin implementaciones).
  - **F4 — Position State** (múltiples copias del estado de posición; pyramiding/WAC/pop/close_position).
- Cada claim se inspeccionó buscando evidencia a favor Y en contra. Solo se concluyó tras la inspección correspondiente.
- No se ejecutó runtime: la vida del estado se infiere de código (misma limitación del informe base).

---

## 2. Metodología de falsación

1. **Regla central:** no aceptar ninguna afirmación del informe anterior como verdadera por defecto.
2. Para cada claim: inspección de archivos con `ls`/`sed`/`grep -n`, buscando activamente contra-evidencia.
3. Toda afirmación factual con: ruta real, símbolo real, línea exacta y comando de verificación.
4. Si la evidencia no permitía concluir → UNKNOWN con motivo explícito.
5. Diferenciación estricta en F2 entre: detección interna / recuperación / estado consultable / política en contrato / enforcement antes de ejecutar.
6. Diferenciación estricta en F4 entre: estado mutable duplicado / cache / snapshot / derivación / SSOT.
7. Sin modificación de código, ADRs, tracking.yaml ni informes existentes; sin git add/commit/push.

---

## 3. F1 — WSTradesSource

### Claim (informe forense anterior)
- "Stub CONFIRMADO: docstring NOT_IMPLEMENTED en `ws_trades_source.py:14`, clase en `:32`, `__anext__` levanta `StopAsyncIteration` (`:90`)".
- ERROR-01: "la clase NO está en `:58` (esa línea es `self._running = False` dentro de `__init__`)".
- ERROR-02: "la sección NOT_IMPLEMENTED está en L14" (no L9-11).

### EVIDENCIA A FAVOR (confirmación del stub y de las correcciones)
| Comando | Resultado |
|---|---|
| `grep -n "class WSTradesSource" ws_trades_source.py` | clase en **L32** |
| `sed -n '1,20p'` | docstring módulo L1-20; línea **L14** = "NOT_IMPLEMENTED — emite StopAsyncIteration inmediatamente." |
| `grep -n "_running"` | `:58` `self._running = False`; `:78-79` property `is_running` → `return self._running`; `:96` `self._running = False` (en stop); **nunca se setea True** |
| `sed -n '30,95p'` | `__anext__` (L84) hace `logger.warning` + `raise StopAsyncIteration` (L90) |
| `grep -rn "WSTradesSource"` | consumido por: `rest/__init__.py:25`, `websocket/__init__.py:12,14`, `gap_aware_stream.py:10,30,37,102`, `source_manager.py:32,39`, `pipeline_factory.py:407,417`, `trades_source.py:20,75` — el stub es instanciado en `pipeline_factory.py:417` y envuelto en `GapAwareStream` |

### EVIDENCIA EN CONTRA (intentos de refutación)
- ¿`is_running` podría ser True alguna vez? No: `_running` solo se inicializa a False (L58) y se resetea a False (L96). El stub nunca produce trades y nunca "corre". Verificado con `grep -n "_running"` → 4 matches, ninguno asigna True.
- ¿La clase podría estar en otra línea (ej. :58)? No: `:58` es `self._running = False` (confirmado por `sed -n '54,60p'`). La cita `:58` del informe Parte 2 era incorrecta.
- ¿El docstring citado como "L9-11" sería correcto? El docstring del módulo empieza en L9 ("Fuente WebSocket — stub estructural" está en L10), pero la sección "Estado actual / NOT_IMPLEMENTED" está en **L14**. La cita L9-11 apunta al rango correcto del docstring pero no a la línea de la afirmación clave.
- ¿`is_running:78-79` citado por el informe Parte 2? Correcto: `def is_running` en `:78`, `return self._running` en `:79`.

### RESULTADO
La corrección del informe forense (ERROR-01/02) es **correcta y reproducible**; el hecho central (stub estructural) se confirma.

### SEVERIDAD
Media (en el informe base Parte 2); **cero** en el informe forense falsado, cuyas correcciones son exactas.

### CONCLUSIÓN
**VERIFIED.** El claim F1 del informe forense (clase en :32, NOT_IMPLEMENTED en :14, is_running en :78, línea :58 es `_running=False`, nunca True) se reproduce de forma independiente. El claim del informe Parte 2 original (clase en :58) queda **FALSE** en su cita de línea, pero el hecho central (stub con nombre de fuente real) queda **VERIFIED**.

---

## 4. F2 — Freshness / stale-data

### Claim (informe forense anterior, OWN-05 corregido)
- La afirmación original del Parte 2 "sin política de freshness en flujo WS→execution" era **demasiado fuerte**.
- Corrección del forense: "Freshness OPERATIVA SÍ existe (GapAwareStream detecta silencio con `wait_for` + recovery); como ESTADO CONSULTABLE no existe (solo `is_running`/`source_id`); la conclusión de fondo (falta de estado consultable) se mantiene".

### Evidencia primaria (flujo completo inspeccionado)

**1. Detección interna (silencio): EXISTE**
- `gap_aware_stream.py:258-263`: `_next_from_source` envuelve `self._source.__anext__()` con `asyncio.wait_for(timeout=gap_threshold_ms/1000)` cuando `gap_threshold_ms > 0`.
- `gap_aware_stream.py:214-216`: `except asyncio.TimeoutError:` → `await self._handle_silence_gap()`.
- `gap_aware_stream.py:265-285`: `_handle_silence_gap` calcula rango `[last_trade_ms+1, now_ms]` y ejecuta recovery.
- `gap_aware_stream.py:85`: default `_DEFAULT_GAP_THRESHOLD_MS = 30_000` (30s); `:107-108` documentado; `:131-132` validación `>= 0`; `:56` `0 = deshabilitar`.
- `pipeline_factory.py:416`: `gap_threshold_ms = getattr(request, "gap_threshold_ms", 30_000)` — configurable vía request.

**2. Recuperación (recovery): EXISTE**
- `gap_aware_stream.py:314-344`: `_run_recovery` instancia `GapRecoveryFetcher` vía `recovery_factory`, verifica `source is TradeSource.REST_RECOVERY` (SSOT, `:325`), rellena `_recovery_buffer`; fail-soft (`:338-344`).
- `gap_aware_stream.py:287-312`: `_handle_disconnection` ejecuta recovery + restart.
- `gap_aware_stream.py:346-380`: `_restart_source` con backoff (`_MAX_RESTART_ATTEMPTS=3`, `_RESTART_BACKOFF_S=2.0`).
- `pipeline_factory.py:421-433`: `_make_recovery_factory` → `GapRecoveryFetcher`.
- `source_manager.py:39`: describe el cableado como "live stream con auto-recovery".
- OHLCV adicional: `ohlcv_fetcher.py:476-487` "Stale window — aborting"; `ccxt_adapter.py:563,574` `is_healthy()` O(1) sin red, consumido en `ohlcv_fetcher.py:276`.

**3. Estado consultable: NO EXISTE**
- `TradesSourceProtocol` (`ports/inbound/trades_source.py:89-107`): solo `__aiter__`/`__anext__`/`stop`. **No declara ningún campo/método de freshness** (`last_trade_ms`, staleness, lag).
- `GapAwareStream` expone solo `is_running` (`:159-161`) y `source_id` (`:163-166`). `_last_trade_ms` es privado (`:142`), solo lectura interna.
- `is_running` en `GapAwareStream` es un flag de ciclo de vida (True solo en `__aiter__` L169), NO de freshness.
- `TradesSourceManager.is_running` (`source_manager.py:142-143`) delega en el flag del stream — tampoco es freshness.
- `_check_dataset_lag`/`check_dataset_invariants` (`domain/quality/invariants.py:133-161,176`): definidos pero **sin caller en producción** — `grep check_dataset_invariants` solo arroja su propio `__init__` (`domain/quality/__init__.py:13`) y docstrings. El lag check no está cableado a ningún pipeline.
- Métrica `ocm_silver_freshness_seconds` (`infrastructure/observability/metrics.py:40`): solo comentario documental; `grep freshness` no muestra emisión en ningún lado.

**4. Política en contrato: NO EXISTE**
- `FeatureSource` (`shared/contracts/boundaries.py:21-37`): `load_features(exchange, symbol, timeframe, market_type, **kwargs)` — sin parámetro ni gate de freshness.
- `GoldReader.load_features` (`adapters/outbound/storage/gold_reader.py:128`), `feature_reader.py:52`, `composition_root.py:156-168`: ninguno verifica staleness antes de devolver datos.

**5. Enforcement antes de ejecutar órdenes: NO EXISTE**
- `TradingEngine._load_data` (`engine.py:337-344`): llama `load_features` directo; sin chequear freshness.
- `engine.py:325-331` `_last_timestamp()`: devuelve `datetime.now()`, no deriva del dato.
- Frontera Market Data → Strategy → Risk → Execution: `engine.py:338` consume `FeatureSource` (pull de Gold/Iceberg), produce señal → `OMS.submit` (`engine.py` → `oms.py`) → risk `validate` (`manager.py`) — ningún paso consulta freshness del dato.

### EVIDENCIA EN CONTRA (intentos de refutación del corrección)
- ¿Existiría algún estado de freshness consultable en otra parte? Búsqueda `grep -rn "last_trade_ms|freshness|is_running"` en application/infrastructure: solo `source_manager.py:138,143` (source_id/is_running), `metrics.py:40` (comentario), y el estado interno del stream. Ningún port/contract de freshness.
- ¿El invariante de lag estaría cableado en algún consumer? `grep check_lag|invariants` → solo definición y self-imports; los pipelines (`ohlcv_pipeline.py:470-471`) delegan gap detection a consumer no presente.
- ¿`is_healthy` sería un proxy de freshness consultable? Es health de la conexión/adapter (`ccxt_adapter.py:563`, O(1), sin red), no de la frescura de los datos consumidos por execution.

### RESULTADO
La corrección del informe forense es **correcta**. La distinción entre "existe detección/recovery interna" (VERDADERO) y "no existe estado consultable ni enforcement en contrato/frontera" (VERDADERO) es precisa y verificable.

### SEVERIDAD
Baja (error de caracterización en el Parte 2 original; corrección del forense sin error).

### CONCLUSIÓN
**VERIFIED** (claim corregido del forense). Se confirma la falsación del claim literal "no existe política de freshness": debe separarse en (a) detección interna + recovery operativos — existen, y (b) política de freshness en contrato/estado consultable/enforcement pre-ejecución — no existen.

---

## 5. F3 — MarketDataSourcePort

### Claim (informe forense anterior)
- "MarketDataSourcePort es HUÉRFANO — 0 consumidores en todo el repo; su docstring referencia implementaciones inexistentes (`adapters/inbound/rest/ccxt_adapter.py`, `adapters/inbound/stream/replay_adapter.py`)."

### EVIDENCIA A FAVOR
- `grep -rn "MarketDataSourcePort"` en todo el repo (py/md/yaml/toml, excl. `.venv`/`__pycache__`): solo aparece en su propio archivo (`ports/outbound/market_data_source.py:32,67`) y en los 2 informes. **0 consumidores de código.**
- Docstring del archivo (`ports/outbound/market_data_source.py:36-40`): "Implementaciones de referencia: `market_data.adapters.inbound.rest.ccxt_adapter.CcxtRestAdapter` y `market_data.adapters.inbound.stream.replay_adapter.ReplayAdapter (futuro)`".
- `grep -rn "CcxtRestAdapter|ReplayAdapter"`: solo el docstring mismo. **No existen**.
- `ls packages/market_data/adapters/inbound/`: no existe carpeta `stream/`; no existe `replay_adapter.py` (solo `rest/`, `websocket/`, `external/`, y adapters sueltos).
- Contradicción interna adicional: el docstring del propio archivo se autodeclara "Puerto INBOUND (driving side)" (`market_data_source.py:6`) pese a vivir en `ports/outbound/`.
- Los 4 `async def fetch_ohlcv` del repo (`ccxt_adapter.py:283`, `exchange.py:99`, `market_data_source.py:42`, `historical_fetcher.py:86`) pertenecen a contratos/adapters distintos; ninguno implementa `MarketDataSourcePort` nominalmente.
- Los use cases de backfill/fetch no importan `ports.outbound.market_data_source` (`grep "from market_data.ports.outbound.market_data_source"` → vacío).
- El outbound `MarketDataSource` INBOUND (`ports/inbound/market_data_source.py:24`) SÍ tiene implementaciones y consumidores: `bybit_feed_adapter.py:5,59`, `kucoin_feed_adapter.py:5,39`, `feed_orchestrator.py:32,72,77,139`, `feed_registry.py:14`.

### EVIDENCIA EN CONTRA (intentos de refutación)
- ¿Uso estructural/duck-typing indirecto (runtime_checkable)? El contrato outbound no se usa como tipo en ningún signature de función del repo. Ningún objeto se declara ni se valida como `MarketDataSourcePort`.
- ¿Algún adapter implementaría el protocolo sin nombrarlo? Los adapters OHLCV implementan `ExchangeAdapter`/`ExchangeClientPort`/`HistoricalFetcherPort`, no este protocolo; el único structural subtyping explícito citado es `BybitFeedAdapter`/`KuCoinFeedAdapter` → `MarketDataSource` (inbound).
- ¿Uso en tests? `grep "MarketDataSourcePort" tests` → 0.

### RESULTADO
El claim del informe forense se confirma íntegramente y se refuerza con la contradicción inbound/outbound del docstring.

### SEVERIDAD
Media (hallazgo de naming/ownership muerto); severidad factual del forense: **cero** (correcto).

### CONCLUSIÓN
**VERIFIED.** `MarketDataSourcePort` (outbound) es huérfano: 0 consumidores, 0 implementaciones, docstring con rutas inexistentes y autodeclaración "inbound" contradictoria.

---

## 6. F4 — Position State

### Claim (informe forense anterior)
- "4 copias del mismo hecho (posición abierta con qty/avg); solo `PositionStore` es persistido; divergencia documentada en `fill_sync.py:38-40`".
- "Divergencia real demostrada: `TradeTracker._register_open` REEMPLAZA la entrada en pyramid (no WAC) vs WAC acumulativo en OMS/Risk/Portfolio; `TradeTracker` hace `pop` incondicional en SELL vs `close_position` SafeOps que puede devolver `(None, 0.0)` dejando posición fantasma".
- Subestimación: "la divergencia es más fuerte que '4 copias' — son 4 representaciones con semánticas distintas".

### Inspección (escritores y lectores reales)

**Estructura 1 — `OMS._entry_positions` (memoria, espejo local)**
- Declaración: `oms.py:177` `dict[str, tuple[float, float]]`.
- Escritura BUY (WAC): `oms.py:414-419` — `new_qty = prev_qty + filled_qty`; `new_avg = (prev_qty*prev_avg + filled_qty*fill_price)/new_qty` si `prev_qty>0`, si no `fill_price`. Solo si `filled_qty>0` y `fill_price>0`.
- Escritura SELL: `oms.py:439-451` — `closed_qty = min(filled_qty, prev_qty)`; `remaining = prev_qty - closed_qty`; `remaining>0` → `(remaining, avg)` (preserva WAC); si no → `pop`.
- Lectura: `oms.py:233` (sizing SELL), `:381` (cost basis).
- Docstring del OMS la declara "espejo local de la SSOT en PortfolioService" (`oms.py:39`).

**Estructura 2 — `RiskManager._positions` (memoria, espejo económico)**
- Declaración: `manager.py:135` `dict[str, tuple[float, Optional[float]]]`.
- Escritura (push del OMS): `manager.py:190,193` vía `record_position` (llamado en `oms.py:419,423` BUY y `:456`/`:464` SELL). UNKNOWN price → `(qty, None)` (`:193`, `manager.py:190` pop si qty≤0).
- Lectura: `manager.py:289,295,304-305,316` (exposure/unknown/state), `:275`.
- Docstring `manager.py:23`: "es el espejo económico real". Riesgo NO posee estado patrimonial duplicado por propia decisión BC-12 (`ADR-0030:67`).

**Estructura 3 — `TradeTracker._open_positions` (memoria, analítica, guarda Order completo)**
- Declaración: `trade_tracker.py:59` `dict[str, Order]` — NO `(qty, avg)`: guarda el objeto `Order` completo.
- Escritura BUY (REEMPLAZO, no WAC): `trade_tracker.py:137-146` — "Posición ya abierta para {} — reemplazando ... (estrategia pyramid no soportada aún)"; `self._open_positions[order.symbol] = order` (sobrescribe el Order anterior).
- Escritura SELL (pop incondicional): `trade_tracker.py:158` — `self._open_positions.pop(order.symbol, None)`; si no había BUY → warning e ignorado (`:161-165`).
- Lectura: `trade_tracker.py:101` (property `open_positions`), `:118-119` (state), `:175+` (registro close).

**Estructura 4 — `PositionStore` vía `PortfolioService` (persistido, SSOT declarado)**
- Declaración del store: `portfolio_service.py:81` `self._store = store` (PositionStore Protocol).
- Escritura BUY multi-entry (WAC): `portfolio_service.py:120-148` — `existing = next(p for p in self._store.all() if symbol==p.symbol and side==p.side)`; `new_qty = existing.quantity + quantity`; `new_avg = (existing.quantity*existing.avg_entry + quantity*avg_entry)/new_qty`. La clave (order_id/entry_at/size_pct) queda la de la pierna de apertura.
- Escritura SELL (cierre parcial/completo SafeOps): `portfolio_service.py:177-260` — `position = self._store.get(order_id)`; si None → warning + `(None, 0.0)`; `remaining<=1e-12` → `delete`; si no → `save(updated)` con `remaining`; excepción → `(None, 0.0)`.
- Lectura: `portfolio_service.py:281` (snapshot), `:297` (open_count), `:305` (total_exposure).
- Instanciación: solo en `portfolio/bootstrap/composition_root.py` (BC-43, `:13-24,136-187`); trading lo RECIBE ya ensamblado (`trading/bootstrap/composition_root.py:338,362,456,466`).

**Cableado (quién escribe cada espejo):**
- `fill_sync.py:110-155`: `on_fill_composite` → 1º `tracker.on_fill(order)` (TradeTracker); 2º si BUY `open_order_ids.setdefault` + `portfolio.open_position`; si SELL `portfolio.close_position(buy_order_id, qty)`. Mapeo `symbol→buy_order_id` único en `open_order_ids` (`fill_sync.py:73-77`).
- Divergencia documentada: `fill_sync.py:38-40` "múltiples fuentes de verdad: TradeTracker._open_positions vs PortfolioService ... decisión arquitectónica pendiente, no resuelta aquí".
- Posición fantasma: `fill_sync.py:50-66,126-137` (B-15/H-09): `logger.critical POSITION_CLOSE_UNCONFIRMED` si `closed is None` — TradeTracker ya hizo pop (`:158`) mientras PortfolioService/PositionStore puede retenerla.

### Evidencia de divergencias semánticas reales (no solo nombres)
1. **Pyramid: TradeTracker REEMPLAZA vs OMS/Risk/Portfolio acumulan WAC.** En un segundo BUY del mismo símbolo: OMS acumula (`oms.py:414-419`), Risk acumula (`manager.py:193`), PortfolioService acumula WAC (`portfolio_service.py:120-148`), pero TradeTracker SOBRESCRIBE el Order de apertura (`trade_tracker.py:146`) y pierde el cost basis de la primera pierna. La propiedad `open_positions` (`:94-101`) devuelve el Order del último BUY, no el WAC.
2. **Cierre parcial: TradeTracker pop total vs los demás reducción parcial.** `_register_close` hace `pop` incondicional (`:158`): un SELL parcial deja `_open_positions` sin la posición, mientras OMS reduce (`oms.py:449`), Risk reduce (`manager.py:190,193`) y PortfolioService conserva `remaining` (`portfolio_service.py:241-255`). Un segundo SELL del símbolo en TradeTracker caería en "SELL sin BUY correspondiente" (`:161-163`).
3. **Unidad de estado distinta:** TradeTracker guarda `Order` (no `(qty, avg)`); OMS/Risk guardan tuplas `(qty, avg)`; Portfolio guarda `PositionSnapshot` persistido. "4 copias de `(qty, avg)`" es inexacto: son 3 tuplas + 1 Order.
4. **Persistencia:** solo `PositionStore` persiste (InMemory paper / Redis prod, `portfolio/bootstrap/composition_root.py:136-187`). OMS/Risk/TradeTracker son efímeros.

### RESULTADO
La subestimación del forense es **correcta y demostrada**: no son 4 copias equivalentes, sino 4 representaciones con semánticas distintas (WAC vs reemplazo vs Order completo vs PositionSnapshot) sincronizadas por un mismo evento (`on_fill_composite`) con comportamiento asimétrico en pyramid y cierre parcial.

### SEVERIDAD
Alta (en el Parte 2 original el riesgo era "4 copias"; la realidad es divergencia semántica demostrable).

### CONCLUSIÓN
**VERIFIED.** El claim del forense sobre divergencia semántica real (pyramid: reemplazo vs WAC; SELL: pop incondicional vs reducción SafeOps; unidad: Order vs tupla) se reproduce independientemente con líneas exactas.

---

## 7. Matriz final de resultados

Formato: F | CLAIM | EVIDENCIA A FAVOR | EVIDENCIA EN CONTRA | RESULTADO | SEVERIDAD | CONCLUSIÓN

| F | CLAIM (forense falsado) | EVIDENCIA A FAVOR | EVIDENCIA EN CONTRA | RESULTADO | SEVERIDAD | CONCLUSIÓN |
|---|---|---|---|---|---|---|
| F1 | `WSTradesSource` stub en :32; NOT_IMPLEMENTED en :14; `is_running` en :78 nunca True; `:58` es `_running=False` | `grep -n` clase L32; `sed 1-20` L14; `grep _running` L58/78-79/96 sin True; `sed 30-95` raise SAI L90; consumido en 6 módulos | `_running` jamás True; clase no está en :58; "L9-11" no contiene la frase clave | **VERIFIED** | Media (citas Parte 2) / cero (forense) | Correcciones exactas y reproducibles |
| F2 | Existe detección+recovery interna; NO existe estado consultable ni política en contrato ni enforcement pre-ejecución | `gap_aware_stream.py:258-263,214-216,265-285,314-344,287-312,346-380`; `pipeline_factory.py:416,421-433`; `source_manager.py:39`; contrato sin freshness `trades_source.py:89-107`; `FeatureSource` sin gate `boundaries.py:21-37`; `engine.py:337-344` sin check; `check_dataset_invariants` sin caller | `is_healthy` solo health de adapter; lag invariant sin cablear; métrica `freshness` sin emisión | **VERIFIED** | Baja | Falsación del claim literal "no existe freshness" correcta; reformulación exacta |
| F3 | `MarketDataSourcePort` outbound huérfano; docstring con implementaciones inexistentes | grep `MarketDataSourcePort` solo en su archivo+informes; `CcxtRestAdapter`/`ReplayAdapter` inexistentes; sin `stream/` ni `replay_adapter.py`; autodeclaración "INBOUND" (`:6`) en `ports/outbound/`; inbound `MarketDataSource` SÍ con 4 consumidores | Sin uso estructural indirecto; sin tests; sin implementadores | **VERIFIED** | Media | Confirmado íntegro + contradicción inbound/outbound adicional |
| F4 | 4 estructuras con divergencia semántica real (reemplazo vs WAC; pop incondicional vs SafeOps; Order vs tupla) | `oms.py:414-419,439-451`; `manager.py:190,193`; `trade_tracker.py:137-146,158`; `portfolio_service.py:120-148,177-260`; `fill_sync.py:38-40,50-66,110-155` | Ninguna: la asimetría es demostrable en código (pyramid y cierre parcial) | **VERIFIED** | Alta | Subestimación confirmada; no son 4 copias equivalentes |

---

## 8. Errores confirmados

1. **ERROR-01 (confirmado, ya en forense):** informe Parte 2 citó la clase `WSTradesSource` en `ws_trades_source.py:58`; la línea real es `:32`. `:58` es `self._running = False`. Reproducible.
2. **ERROR-02 (confirmado, ya en forense):** informe Parte 2 citó "docstring L9-11" como evidencia de NOT_IMPLEMENTED; la línea de la afirmación clave es `:14`. Reproducible.
3. **ERROR-03 (confirmado, ya en forense):** informe Parte 2 afirmó "sin política de freshness en flujo WS→execution"; la detección de silencio y el recovery operativos SÍ existen en `GapAwareStream`. La parte correcta: no existe estado consultable ni política en contrato ni enforcement. Reproducible.
4. **ERROR adicional (nuevo en esta falsación):** el docstring de `ports/outbound/market_data_source.py` se autodeclara "Puerto INBOUND (driving side)" (`:6`) aunque el archivo vive en `ports/outbound/`. Contradicción interna que refuerza NAMING-N02/F3.
5. **Matiz adicional (nuevo):** la formulación "4 copias del mismo hecho (posición abierta con qty/avg)" es inexacta porque `TradeTracker._open_positions` guarda el objeto `Order` completo, no la tupla `(qty, avg)`; y la asimetría pyramid/cierre parcial hace que ni siquiera sean "copias equivalentes".

---

## 9. Claims que sobreviven a la falsación

1. **F1 — stub WSTradesSource**: sobrevive (stub estructural confirmado, nunca corre, `is_running` siempre False).
2. **F1 — correcciones de líneas del forense**: sobreviven exactas (:32, :14, :78-79, :58).
3. **F2 — corrección de OWN-05**: sobrevive; distinción detección/recovery (existen) vs estado/política/enforcement (no existen) es exacta.
4. **F3 — `MarketDataSourcePort` huérfano**: sobrevive íntegro; refuerzo con docstring "INBOUND" en `ports/outbound/`.
5. **F4 — divergencia semántica de Position State**: sobrevive y se demuestra más fuerte que "4 copias".

---

## 10. Claims que deben corregirse

1. **Claim literal del Parte 2 "no existe política de freshness en flujo WS→execution"** → debe formularse como: "existe detección de silencio y recovery operativos en `GapAwareStream`, pero no existe estado de freshness consultable, ni política en contrato (`TradesSourceProtocol`/`FeatureSource`), ni enforcement antes de ejecutar órdenes".
2. **Claim del Parte 2 "4 copias del mismo hecho (posición abierta con qty/avg)"** → debe formularse como: "4 representaciones del estado de posición con semánticas distintas — `(qty, avg)` en OMS y Risk (WAC), `Order` completo con reemplazo en TradeTracker, `PositionSnapshot` persistido en PortfolioService — con asimetría demostrable en pyramid y cierre parcial".
3. **Claim del Parte 2 (citas de WSTradesSource)**: ubicación `:58` → `:32`; docstring `L9-11` → `L14`.

---

## 11. UNKNOWN pendientes

- Ninguno dentro del alcance F1–F4. Todas las afirmaciones son reproducibles por lectura de código con rutas y líneas exactas.
- Limitación declarada (no es UNKNOWN): no se ejecutó runtime; la vida del estado (p. ej. si un SELL parcial real dispara el camino SafeOps de `close_position`) se infiere de código, no de observación.

---

## 12. Validación final del repositorio

Ejecutado al final de la falsación:

```
git status --short
?? docs/audits/2026-08-15-auditoria-integral-market-data-naming-estructura.md
?? docs/audits/2026-08-15-auditoria-ocm-parte-1-market-data.md
?? docs/audits/2026-08-15-auditoria-ocm-parte-2-naming-responsibility.md
?? docs/audits/2026-08-15-benchmark-complementario-lean-vnpy-ccxtpro.md
?? docs/audits/2026-08-16-verificacion-forense-parte-2-naming-responsibility.md
?? docs/audits/2026-08-16-falsacion-dirigida-parte-2.md   <-- este entregable

git diff --stat
(sin output — código tracked sin modificaciones)

git diff --cached --stat
(sin output — sin staged changes)

git diff HEAD -- docs/architecture/decisions/ADR-0029-cancelacion-real-gestion-ordenes-abiertas.md
0 líneas

git diff HEAD -- docs/architecture/decisions/ADR-0030-balance-real-reconciliacion-patrimonial.md
0 líneas

git diff HEAD -- docs/plans/tracking.yaml
0 líneas
```

Confirmación explícita:

```
CÓDIGO MODIFICADO: NO
ADRs MODIFICADOS: NO
TRACKING MODIFICADO: NO
COMMIT: NO
PUSH: NO
```

---

### Anexo de validación del entregable

- **ARCHIVO CREADO:** `docs/audits/2026-08-16-falsacion-dirigida-parte-2.md` (este archivo).
- **ARCHIVOS MODIFICADOS:** ninguno más.
- **CÓDIGO MODIFICADO:** NO.
- **ADRs MODIFICADOS:** NO (ADR-0029, ADR-0030 intactos).
- **tracking.yaml MODIFICADO:** NO.
- **INFORMES PARTE 2 Y FORENSE MODIFICADOS:** NO.
- **COMMIT:** NO.
- **PUSH:** NO.