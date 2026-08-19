# Verificación Forense Independiente — Parte 2/4 (Naming / Responsibility / State Ownership)

**Fecha:** 2026-08-16
**Objeto auditado:** `docs/audits/2026-08-15-auditoria-ocm-parte-2-naming-responsibility.md` (321 líneas / 28 641 bytes).
**Rol:** verificación independiente, SOLO LECTURA. No se modificó ningún archivo del repositorio salvo la creación de este informe.
**Método:** "NO CONFÍES EN EL INFORME. INTENTA REFUTARLO." Cada afirmación se contrastó contra el código real del repositorio (rutas + líneas), no contra el texto del informe ni contra docstrings como prueba de comportamiento.
**Cadena de trazabilidad por claim:** AFIRMACIÓN → INSPECCIÓN REAL → EVIDENCIA → RESULTADO → CLASIFICACIÓN → IMPACTO.
**Clasificaciones usadas:** VERIFIED / PARTIALLY VERIFIED / FALSE / UNSUPPORTED / UNKNOWN.

---

## 1. Metodología

1. Cada afirmación del informe (NAMING-N01..N07, OWN-01..05, matriz de estados §6.3, contraste ADR §6.4) se auditó contra el archivo fuente real citado.
2. Los números de línea citados por el informe se re-verificaron con `grep -n`/`sed`, no se asumieron.
3. Búsqueda de contra-evidencia obligatoria en cada claim de ausencia (Balance State, `fetch_balance`, persistencia de órdenes, freshness).
4. No se confundió ausencia con inexistencia: para Balance State se revisaron 7 categorías (A–G); para Position State se probó divergencia semántica real, no solo "varias estructuras".
5. Los errores encontrados se registran en §4 con formato ERROR-ID. No se corrigió silenciosamente nada.
6. Regla anti-fallo: todo lo concluido aquí es reproducible por un tercero leyendo código + rutas/líneas citadas.

---

## 2. Matriz final de verificación

Formato: ID | Afirmación del informe | Evidencia primaria | Resultado | Clasificación | Severidad del error | Impacto

| ID | Afirmación del informe | Evidencia primaria (repo) | Resultado | Clasificación | Severidad | Impacto |
|---|---|---|---|---|---|---|
| NAMING-N01 | Dos `OrderStatus` con vocabularios solapados; `ERROR` solo en transporte | `order.py:55-60` (PENDING/SUBMITTED/FILLED/REJECTED/CANCELLED); `transport.py:53-61` (SUBMITTED/FILLED/CANCELLED/REJECTED/ERROR). Imports divididos: `engine.py:46`, `oms.py:96`, `execution/__init__.py:3` desde `order`; `composition_root.py:47`, `paper_executor.py:19`, `live_executor.py:53-58` desde `transport`. Ningún módulo importa ambos en el mismo scope. Sin conversión explícita (puente vía `OrderResult.accepted`/`OrderState.confirmed_filled`); `OMS._fill` usa `order.OrderStatus.FILLED` independiente del status del transporte. | Confirmado | VERIFIED | Ninguna | Hallazgo sostenido |
| NAMING-N02 | Dos archivos `market_data_source.py` (inbound/outbound) con clases casi homónimas | `ports/inbound/market_data_source.py:24` (`MarketDataSource`); `ports/outbound/market_data_source.py:32` (`MarketDataSourcePort`). **Hallazgo adicional:** `MarketDataSourcePort` es HUÉRFANO — 0 consumidores en todo el repo; su docstring referencia implementaciones inexistentes (`adapters/inbound/rest/ccxt_adapter.py`, `adapters/inbound/stream/replay_adapter.py`). | Confirmado; hallazgo adicional refuerza | VERIFIED | Ninguna | Hallazgo sostenido (+ severidad potencial mayor: contrato outbound muerto) |
| NAMING-N03 | `publisher.py` es re-export; doble ruta de import; SSOT en `publisher_port.py` | `publisher.py:1-20` docstring "Re-export desde publisher_port.py (SSOT)". Ambas rutas consumidas: `ohlcv_pipeline.py:43`, `backfill.py:51`, `incremental.py:29`, `runtime.py:58`, `pipeline_factory.py:251` (desde `publisher`); `backfill.py:437`, `incremental.py:116`, `ohlcv_publisher.py:44` (desde `publisher_port`). | Confirmado | VERIFIED | Ninguna | Hallazgo sostenido |
| NAMING-N04 | Carpetas `pipeline/` vs `pipelines/` homónimas | `application/pipeline/` (solo `runtime.py`) y `application/pipelines/` (5 pipelines + `_worker_pool.py`) ambas existen y ambas son consumidas por `pipeline_factory.py`. | Confirmado | VERIFIED | Ninguna | Hallazgo sostenido |
| NAMING-N05 | `WSTradesSource` es un stub con nombre de fuente real; ubicación citada `:58`, evidencia `docstring L9-11`, `is_running:78-79` | Stub CONFIRMADO: docstring "NOT_IMPLEMENTED — emite StopAsyncIteration" (`ws_trades_source.py:14`), clase en `:32`, `__anext__` levanta `StopAsyncIteration` (`:90`), consumido por `source_manager.py`, `pipeline_factory.py`, `gap_aware_stream.py`. **ERROR factual:** la clase NO está en `:58` (esa línea es `self._running = False` dentro de `__init__`); la evidencia "L9-11" apunta al docstring correcto pero la sección NOT_IMPLEMENTED está en L14. `is_running:78-79` correcto (def en `:78`). | Hecho central confirmado; 2 errores de cita de línea | PARTIALLY VERIFIED | Media | ERROR-01 / ERROR-02 |
| NAMING-N06 | Nombres que fijan tecnología; `pandas_to_domain` desincronizado (importa polars) | `bybit_cryptofeed_runner.py:25-27`, `kucoin_cryptofeed_runner.py:20-22`, `cryptofeed_orderbook_stream.py:49-51` (imports cryptofeed). `pandas_to_domain.py:25` importa polars (no pandas); cuerpo usa `pl.*` (`:47,95,98-101`); docstring habla de `pd.DataFrame`. | Confirmado | VERIFIED | Ninguna | Hallazgo sostenido |
| NAMING-N07 | Dos contratos de exchange coexisten sin relación nominal | `exchange.py:35` `ExchangeAdapter(ABC)` solo heredado por `CCXTAdapter` (`ccxt_adapter.py:96`). `exchange_client.py:20` `ExchangeClientPort(Protocol)` ACTIVO: consumido por `derivatives_pipeline.py:39,129`, `trades_pipeline.py:36,123`, `ohlcv_pipeline.py:38,182`, `pipeline_factory.py:228,248,260`. | Confirmado; matiz: `ExchangeClientPort` es claramente el contrato vigente, la ambigüedad es nominal, no funcional | VERIFIED | Ninguna | Hallazgo sostenido con matiz |
| OWN-01 | Order State solo en memoria de proceso; sin rehidratación del exchange ni loop `manage_open_orders` | `oms.py:169-170` (`_orders`/`_open` dicts), `oms.py:300-317` cancel local-only sin contacto con exchange. `Order` dataclass (`order.py:82-142`) sin serialización. grep en todo `packages/trading` de `fetch_open_orders|hydrat|recovery|deserializ|pickle|json.dump|sqlite` = 0 matches funcionales (solo docstrings). No existe caller de `manage_open_orders`. | Confirmado | VERIFIED | Ninguna | Hallazgo sostenido |
| OWN-02 | 4 copias del mismo hecho (posición qty/avg) con divergencia documentada (`fill_sync.py:38-40`) | `oms.py:177` (`_entry_positions`), `manager.py:135` (`_positions`), `trade_tracker.py:59` (`_open_positions`), `PositionStore` (`portfolio_service.py:143,164,234,255`). `fill_sync.py:38-40` confirma "múltiples fuentes de verdad... decisión arquitectónica pendiente". **Divergencia real demostrada:** `TradeTracker._register_open` REEMPLAZA la entrada en pyramid (`trade_tracker.py:141-146`, "pyramid no soportada aún") vs WAC acumulativo en OMS/Risk/Portfolio; `TradeTracker` hace `pop` incondicional en SELL (`:158`) vs `close_position` SafeOps que puede devolver `(None, 0.0)` dejando posición fantasma. | Confirmado; divergencia real es más fuerte que "4 copias" (son 4 representaciones con semánticas distintas) | VERIFIED | Ninguna | Hallazgo sostenido (subestimado: divergencia semántica real) |
| OWN-03 | Balance State AUSENTE; solo `capital_usd` configurado | grep `fetch_balance|get_balance|wallet|available_balance|BalanceStore|PortfolioReconciler` en packages/apps/shared/ocm/config = 0 (solo falsos positivos: rebalance, imbalance, load balancer). `ccxt_adapter.py` no expone `fetch_balance` (solo `create_order:405`, `fetch_order:462`). `config/portfolio/portfolio.yaml:15` `capital_usd: 10000.0`; `manager.py:112-118` default 10_000; `portfolio_service.py:63-80` constructor. Equity curve (`performance.py:201-260`) es derivada, no balance. Categorías A–G: ninguna implementada como estado. | Confirmado | VERIFIED | Ninguna | Hallazgo sostenido |
| OWN-04 | Order Book State sin `sequence` en payload | `cryptofeed_orderbook_stream.py:158-161` documenta `book.sequence_number` pero `on_snapshot` solo pasa `checksum` (`:201`); `on_delta` no recibe sequence. `shared/kafka/schemas/orderbook.py` `OrderBookSnapshotPayload` (`:63-100`) y `OrderBookDeltaPayload` (`:131-165`) sin campo sequence. Domain `order_book.py` sin sequence. BookBuilder reconstruye "en memoria" (docstring payload `:71-72`). | Confirmado | VERIFIED | Ninguna | Hallazgo sostenido |
| OWN-05 | Freshness "no consolidada como estado consultable en la frontera de ejecución"; evidencia `feed_orchestrator.py:58`, `source_manager.py:82`, `gap_aware_stream.py:160` (`is_running`) | **Freshness OPERATIVA SÍ existe:** `GapAwareStream` detecta silencio con `asyncio.wait_for` timeout (`gap_aware_stream.py:262-271`), `_handle_silence_gap`, `_handle_disconnection`, `_run_recovery` (`:265-320`); `source_manager.py:39` lo describe como "live stream con auto-recovery". OHLCV: "Stale window — aborting" (`ohlcv_fetcher.py:476-487`); lag invariant `_MAX_LAG_CANDLES=48` (`invariants.py:75,138-155`). Sin embargo, como ESTADO CONSULTABLE: solo `is_running`/`source_id` expuestos (`gap_aware_stream.py:159-165`), `_last_trade_ms` privado, sin watchdog central ni heartbeat de stream, y TradingEngine no consulta freshness. La afirmación "sin política de freshness en flujo WS→execution" es demasiado fuerte. | Parcialmente confirmado | PARTIALLY VERIFIED | Baja | ERROR-03 (subestimación de capacidad existente; conclusión de fondo correcta) |
| §6.4 Order State vs ADR-0029 | Clasificación CONSISTENTE: código implementa el problema que el ADR diagnostica | ADR-0029 L12 (VERIFIED): `oms.py:300-317` cancel local-only, `transport.py:96-128` sin cancel, `ccxt_adapter.py:405,462` sin `cancel_order`, "OMS.cancel() nunca se invoca". ADR en ESTADO: PROPUESTA (L3, L6). | Confirmado | VERIFIED | Ninguna | Clasificación sostenida |
| §6.4 Balance vs ADR-0030 | Clasificación CONSISTENTE; código coincide con diagnóstico F-BMD9-01 | ADR-0030 L12 (VERIFIED): sin `fetch_balance`, `manager.py:112-118` default 10_000, `portfolio_service.py:63-80`, grep = solo "rebalance". ADR en ESTADO: PROPUESTA (L3, L6). | Confirmado | VERIFIED | Ninguna | Clasificación sostenida |

---

## 3. Resumen ejecutivo de resultados

- **12 claims principales auditados:** 10 VERIFIED, 2 PARTIALLY VERIFIED, 0 FALSE, 0 UNSUPPORTED, 0 UNKNOWN.
- **Ninguna conclusión central del informe se refutó.** Las afirmaciones de fondo (estado en memoria, balance ausente, múltiples fuentes de verdad, sequence no serializado, homonimias) son todas reproduciibles desde el código.
- **3 errores factuales menores de cita de línea/docstring** (ERROR-01..03), ninguno invalida la conclusión del hallazgo afectado.
- **1 hallazgo adicional** (huérfano `MarketDataSourcePort` con docstring de implementaciones inexistentes) que el informe no capturó — refuerza NAMING-N02.
- **1 subestimación** (OWN-02): la divergencia de Position State es semántica real (TradeTracker reemplaza vs WAC; pop incondicional vs SafeOps), más fuerte que "4 copias del mismo hecho".
- **1 subestimación** (OWN-05): la freshenes operativa del flujo WS SÍ existe (GapAwareStream + recovery), aunque no como estado consultable.

---

## 4. ERRORES FACTUALES ENCONTRADOS

### ERROR-01 — NAMING-N05: ubicación de clase incorrecta
- **Afirmación original:** "Ubicación: `packages/market_data/adapters/inbound/websocket/ws_trades_source.py:58`" (fila de tabla §5.2 y hallazgo §5.3).
- **Qué dice el repo:** la clase `WSTradesSource` está en `ws_trades_source.py:32`. La línea `:58` es `self._running = False` dentro de `__init__`.
- **Evidencia:** `grep -n "class WSTradesSource"` → `32`; `sed -n '54,60p'` → `self._running = False`.
- **Por qué importa:** una cita de línea incorrecta rompe la trazabilidad al reproducir la verificación (regla anti-fallo). El hecho central (stub) es correcto.
- **Impacto:** bajo sobre la conclusión (hallazgo sostenido); alto sobre la precisión de la evidencia citada.

### ERROR-02 — NAMING-N05: evidencia de docstring imprecisa
- **Afirmación original:** "evidencia: `ws_trades_source.py` docstring L9-11".
- **Qué dice el repo:** el docstring comienza en L9, pero la línea "NOT_IMPLEMENTED — emite StopAsyncIteration inmediatamente." está en **L14**.
- **Evidencia:** `sed -n '9,16p'` → L14 contiene la afirmación clave.
- **Por qué importa:** la cita parcial desvía la búsqueda de un verificador. La evidencia en sí es correcta en sustancia.
- **Impacto:** bajo.

### ERROR-03 — OWN-05: afirmación "sin política de freshness en flujo WS→execution (Parte 1)" demasiado fuerte
- **Afirmación original:** "sin política de freshness en flujo WS→execution (Parte 1)"; "freshness no consolidada como estado consultable en la frontera de ejecución".
- **Qué dice el repo:** existe freshenes operativa real en el flujo WS: `GapAwareStream` con `asyncio.wait_for` timeout de silencio (`gap_aware_stream.py:262-271`), `_handle_silence_gap` (`:265`), `_handle_disconnection` (`:287`), `_run_recovery` (`:314`), y es descrito en `source_manager.py:39` como "live stream con auto-recovery". Además `ohlcv_fetcher.py:476-487` aborta ventanas stale y `invariants.py:138-155` marca lag de dataset.
- **Evidencia:** sed de `gap_aware_stream.py:251-320`; `source_manager.py:39`; grep freshness/staleness.
- **Por qué importa:** la formulación niega la existencia de mecanismo de freshness cuando sí existe; la parte correcta es que NO está consolidada como estado consultable (solo `is_running`/`source_id` expuestos; `_last_trade_ms` privado).
- **Impacto:** medio sobre la caracterización; la conclusión de fondo (falta de estado consultable de freshness en la frontera de ejecución) se mantiene.

### NINGÚN OTRO ERROR FACTUAL DETECTADO EN EL ALCANCE VERIFICADO.

---

## 5. HALLAZGOS CONFIRMADOS

Estos hallazgos del informe fueron refutados sin éxito (intento de falsación fallido) y se confirman:

1. **NAMING-N01** — dos `OrderStatus` homónimos con vocabularios solapados; `ERROR` no modelable en dominio. Imports divididos por módulo; puente por bools, no por enum. (VERIFIED)
2. **NAMING-N02** — dos `market_data_source.py`; confusión inbound/outbound real. PLUS: `MarketDataSourcePort` huérfano con docstring de implementaciones inexistentes (no capturado por el informe). (VERIFIED + adicional)
3. **NAMING-N03** — re-export `publisher.py` con doble ruta activa. (VERIFIED)
4. **NAMING-N04** — `pipeline/` vs `pipelines/` homónimas y activas. (VERIFIED)
5. **NAMING-N06** — nombres anclados a librerías; `pandas_to_domain` ya es polars-native. (VERIFIED)
6. **NAMING-N07** — dos contratos de exchange; `ExchangeClientPort` es el vigente, `ExchangeAdapter` solo base de `CCXTAdapter`. (VERIFIED con matiz)
7. **OWN-01** — Order State solo en memoria; sin persistencia/hydration/recovery; cancel local-only; sin caller de `manage_open_orders`. (VERIFIED)
8. **OWN-02** — 4 estructuras de posición con divergencia real, más fuerte que lo afirmado (TradeTracker reemplaza vs WAC; pop incondicional vs SafeOps). (VERIFIED)
9. **OWN-03** — Balance State AUSENTE en todas las categorías A–G; sin `fetch_balance` en todo el repo. (VERIFIED)
10. **OWN-04** — Order Book sin campo sequence en payloads ni en dominio; `sequence_number` descartado en el stream. (VERIFIED)
11. **§6.4 contraste ADR-0029** — clasificación CONSISTENTE correcta; ADR PROPUESTA, código implementa exactamente el problema diagnosticado. (VERIFIED)
12. **§6.4 contraste ADR-0030** — clasificación CONSISTENTE correcta; ADR PROPUESTA, sin BalanceStore ni PortfolioReconciler. (VERIFIED)

---

## 6. HALLAZGOS NO DEMOSTRADOS

1. **OWN-05 (freshness consultable)** — la parte de "no consolidada como estado consultable en la frontera de ejecución" NO se demostró como inexistencia de mecanismo de freshness; lo demostrable es (a) que el mecanismo operativo existe (GapAwareStream) y (b) que no se expone como estado consultable por `TradingEngine`/consumers. La ausencia de estado consultable SÍ se verificó (solo `is_running`/`source_id`). → PARTIALLY VERIFIED con la afirmación reformulada.

---

## 7. Matriz de integridad

Formato: Área | Claims auditados | VERIFIED | PARTIALLY VERIFIED | FALSE | UNSUPPORTED | UNKNOWN

| Área | Claims auditados | VERIFIED | PARTIALLY VERIFIED | FALSE | UNSUPPORTED | UNKNOWN |
|---|---|---|---|---|---|---|
| FASE 5 — Naming (NAMING-N01..N07) | 7 | 6 | 1 (N05: stub confirmado, citas de línea erróneas) | 0 | 0 | 0 |
| FASE 6 — State Ownership (OWN-01..05) | 5 | 4 | 1 (OWN-05) | 0 | 0 | 0 |
| Matriz §6.3 (9 estados) | 9 | 9 (contenido verificado por líneas citadas) | 0 | 0 | 0 | 0 |
| Contraste ADR-0029/0030 (§6.4) | 2 | 2 | 0 | 0 | 0 | 0 |
| **TOTAL** | **23** | **21** | **2** | **0** | **0** | **0** |

---

## 8. Autoauditoría de esta verificación

1. **Relectura completa:** realizada (este archivo).
2. **Rutas citadas:** todas verificadas con `ls`/`find`/`sed`/`grep -n`.
3. **Números de línea:** cada línea citada como evidencia fue re-verificada en el repo (incluidas las que el informe citó mal).
4. **Contra-evidencia buscada:** sí — persistencia de órdenes (grep amplio), fetch_balance (repo completo), balance (7 categorías A–G), freshness (watchdog/heartbeat/last_seen), lectores de `MarketDataSourcePort` y `PositionStore`.
5. **No se modificó:** código, ADR-0029, ADR-0030, `docs/plans/tracking.yaml`, ni el informe Parte 2.
6. **Placeholders:** `rg "TODO|TBD|PLACEHOLDER|por determinar"` en este informe → sin matches.
7. **Reproducibilidad:** todas las rutas + líneas citadas apuntan a archivos reales verificados.
8. **No confundir ausencia con inexistencia:** aplicado en Balance (A–G), Position (divergencia semántica probada), Order (contra-evidencia de persistencia buscada), freshness (capacidad operativa encontrada).
9. **No aceptar inferencias como hechos:** el matiz de NAMING-N07 (ExchangeClientPort vigente vs ExchangeAdapter base) y la corrección de OWN-05 reflejan el rechazo de inferencias no demostrables.

---

## 9. Validación Git

Resultado (verificado al final, como tercero):
- Código tracked sin modificaciones: `git --no-pager diff --stat` vacío.
- Sin staged changes: `git --no-pager diff --cached --stat` vacío.
- ADR-0029 sin modificaciones: `git --no-pager diff HEAD -- <ADR-0029>` = 0 líneas.
- ADR-0030 sin modificaciones: idem = 0 líneas.
- `docs/plans/tracking.yaml` sin modificaciones: idem = 0 líneas.
- Untracked (4, preexistentes): `?? docs/audits/2026-08-15-auditoria-integral-market-data-naming-estructura.md`, `?? ...-parte-1-market-data.md`, `?? ...-parte-2-naming-responsibility.md`, `?? ...-benchmark-complementario-...md`.
- Nuevo informe como untracked: `?? docs/audits/2026-08-16-verificacion-forense-parte-2-naming-responsibility.md`.
- Sin `git add`, sin `commit`, sin `push`.

---

## 10. Veredicto

**VEREDICTO: B — PARCIALMENTE VALIDADO.**

Justificación:
- Las 12 conclusiones centrales del informe Parte 2 se confirmaron con evidencia primaria del repositorio (veredicto sobre fondo: VALIDADO).
- No se encontró ninguna afirmación FALSE ni UNSUPPORTED; 21/23 claims en VERIFIED.
- El grado de parcialidad proviene de **3 errores factuales de cita** (ERROR-01/02: línea y docstring de `ws_trades_source.py`; ERROR-03: subestimación de la freshness operativa existente en OWN-05) y de **2 subestimaciones** (OWN-02 divergencia semántica más fuerte que "4 copias"; NAMING-N02 huérfano no detectado).
- Estos errores NO invalidaan ninguna conclusión de fondo del informe, pero impiden la calificación A porque la trazabilidad exacta (ruta:línea) que el informe exige a sí mismo tiene fallas menores y una caracterización (OWN-05) es parcialmente inexacta.

---

### Anexo de validación del entregable

- **ARCHIVO CREADO:** `docs/audits/2026-08-16-verificacion-forense-parte-2-naming-responsibility.md` (este archivo).
- **ARCHIVOS MODIFICADOS:** ninguno más.
- **CÓDIGO MODIFICADO:** NO.
- **ADRs MODIFICADOS:** NO (ADR-0029, ADR-0030 intactos).
- **tracking.yaml MODIFICADO:** NO.
- **INFORME PARTE 2 MODIFICADO:** NO.
- **COMMIT:** NO.
- **PUSH:** NO.