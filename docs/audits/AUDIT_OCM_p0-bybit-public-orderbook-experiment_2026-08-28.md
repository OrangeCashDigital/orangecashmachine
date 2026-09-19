# P0 — BYBIT PUBLIC MARKET DATA EXPERIMENT

> **Autor:** Lead Market-Data Engineer + SRE + Software Architect (OCM)
> **Fecha:** 2026-08-28 · **Branch:** `feat/p0-bybit-public-market-data`
> **Trading = BLOQUEADO.** Solo PUBLIC MARKET DATA. Sin auth. Sin credenciales. Read-only.

## 1. Executive Summary

Ejecuté el P0 experimental contra el **WebSocket público** de orderbook de Bybit
(`wss://stream.bybit.com/v5/public/linear`, topic `orderbook.50.BTCUSDT`), **sin
autenticación ni credenciales**, capturando evidencia reproducible (60 s, 1283
mensajes). Resultados empíricos decisivos:

1. **`u` (Update ID) es estrictamente contiguo (+1)** para el símbolo en la ventana observada
   (1279/1279 deltas `+1`; 0 saltos; 0 duplicados). Es el identificador natural de orden/gap **por libro**.
2. **`seq` (Cross sequence) NO es contiguo** (1279/1279 deltas `>1`; min salto=9, max=7593).
   `seq` es **global por exchange**, NO por símbolo → **usar `seq+1` para detección de gaps produce 100% falsos gaps**.
3. **Los deltas son MULTINIVEL** — 75.1% de los mensajes traen **>1 nivel** (hasta 88 en uno).
   Cada mensaje delta de Bybit es una **unidad atómica multinivel**, no 1 nivel.
4. **No hay checksum** en el orderbook WS de Bybit. Integridad vía `u` + snapshot-restore + invariantes + delete-semantics.
5. **Snapshot = reset**: al suscribir se recibe un snapshot completo (50 bids + 50 asks = 100 niveles); deltas después. `u` del snapshot = base; `u==1` (no observado en esta ventana) = reinicio → overwrite.
6. **Delete semantics confirmado**: niveles con `size="0"` en deltas = eliminar.
7. **Freshness**: latencia local p50 ≈ **163 ms** (msgs por cambio, ~20ms push L50 por oficial).

**Veredicto: `P0 COMPLETE — IMPLEMENTATION REQUIRED`.** El P0 confirma empíricamente las
decisiones D-7a (delta multinivel atómico) y D-7b (gap por `u`, no por `seq`) que ya señalaba
el diseño ADR-0028. NO se declara MARKET DATA READY.

---

## 2. Estado inicial de OCM (reconstruido, read-only)

- **Git:** HEAD `3897be0`; al crear branch dedicado `feat/p0-bybit-public-market-data` desde HEAD.
  `origin/main` = `44034ea` (B-51). Working tree con `M uv.lock` + untracked previos (audits,
  rendered/, proposals/). Sin staged.
- **systemd:** `ocm-streaming.service` **FAILED** (`Result=exit-code`, `ExecMainStatus=1`,
  `NRestarts=3`). No lo arrancé/paré.
- **Procesos:** `market_data.main` RUNNING (pid 1051). No hay streaming ni BookBuilder.
- **Kafka:** `orderbook.raw` ~23M (sin mensajes frescos), `trades.raw`=0, `book.snapshot`=`book.delta`=0,
  `ocm.dlq`=157/156/165.
- **Docker:** Kafka/ZK/Redis/pushgateway Up; **Prometheus/Grafana/Alertmanager `Created`** (obs NO operacional).
- **Market-data pipeline:** produce `orderbook.raw` (stale) pero no hay consumidor/BookBuilder.

## 3. Estado de systemd

`ocm-streaming.service` = **failed / enabled**, root cause `At least one exchange must be enabled`
(`ocm/config/schema.py:865`). No lo modifiqué. (systemd health ≠ data health.)

## 4. Estado de Kafka

Véase §2. `orderbook.raw` con torcidos (P1=0), sin frescura; `book.*` y `trades.raw` vacíos;
`ocm.dlq` con mensajes.

## 5. Estado de Market Data

No production-grade. Código/infra presentes, data-plane NO operativo (service failed, feed stale,
sin BookBuilder, sin validación, sin observabilidad).

---

## 6. Fuentes científicas/literatura consultadas

| Fuente | Principio relevante | Aplicación al P0 / BookBuilder |
|---|---|---|
| Lehalle & Laruelle, *Market Microstructure in Practice* (KB TIER_1) | L2 coherente: snapshot + deltas encadenados por UID/seq; aplicar delta solo sobre snapshot correcto; detectar gaps | El P0 verifica que `u` (por libro) es el UID de encadenamiento correcto para Bybit; `seq` no sirve como UID+1 |
| Buzzelli, *Data Quality Engineering in Financial Services* (KB TIER_1) | calidad = validación + detección de missing/duplicate/gap + trazabilidad + monitoreo | El P0 define la base empírica para gaps (missing) y duplicados; sustenta métricas `ocm_book_*` |
| Referencias de implementación (Hummingbot, Nautilus — zips en docs/) | delta multinivel atómico; snapshot-restore + reject-stale como recovery (no gap `+1` estricto) | Refuerzan el modelo M-C (empírico): `u` contiguo observado + snapshot-restore |
| **ADR-0017 Protocol Discovery Framework** | metodología de discovery: PROTOCOL/DOCUMENTATION/... + Promotion Rule; Bybit = primer profile | El P0 es el **WebSocket Discovery** (componente 5 de ADR-0017) del profile Bybit; los schemas orderbook ya están `PROTOCOL` (promovidos) |

> **Discrepancia / gap documental:** las fuentes TIER_1 de la KB no son artefactos legibles en el
> repo (metadata only) → se usan como motivación, no como autoridad normativa. La autoridad técnica
> de semántica del orderbook es la doc oficial Bybit (F1) + el P0 observado.

## 7. Documentación oficial de Bybit consultada (F1)

- **/docs/v5/ws/connect**: públicos NO requieren auth; heartbeat `ping` cada ~20 s; corte tras 10 min
  sin ping-pong/datos; reconnect ASAP; límite 500 conex/5 min por dominio.
- **/docs/v5/websocket/public/orderbook**: depths L1 10 ms / L50 20 ms / L200 100 ms / L1000 200 ms;
  snapshot inicial → deltas por cambio; **nuevo snapshot ⇒ reset local**; delta: size 0=delete,
  no-existe=insert, existe=update; campos `b`(desc)/`a`(asc)/`u`/`seq`/`cts`/`ts`; **sin checksum**;
  resync por re-snapshot (la garantizado latest).

## 8. Metodología experimental

- **Herramienta:** `docs/audits/p0_bybit/p0_bybit_orderbook.py` (venv OCM, `websockets` 17.0, Python 3.13.5).
- **Endpoint:** `wss://stream.bybit.com/v5/public/linear` (mainnet, público).
- **Topic:** `orderbook.50.BTCUSDT`. **Duración:** 60 s. **Heartbeat:** ping cada 20 s.
- **Diseño:** conecta SIN auth; suscribe; persiste `raw.jsonl` (evidence); envía ping; registra
  reconnects; calcula stats (`u`/`seq` contigüidad, niveles/mensaje, duplicados, latencia).
- **Aislamiento:** solo lectura público; sin credenciales; sin tocar producción/systemd/trading.
- **Salvaguarda:** detecto `publicTrade` no suscrito aquí (opcional) — solo orderbook en esta ejecución.
- Evidence: `docs/audits/p0_bybit/evidence/20260828T220545Z/{raw.jsonl,summary.json}` (296 KB).

## 9. Resultados (60 s, 1283 mensajes)

- `type`: snapshot=1, delta=1279. Reconnects=0.

## 10. Snapshot semantics (OBSERVADO + DOCUMENTADO)

- **1 snapshot** recibido al suscribir. `u`=101054915, `seq`=800573227279, `ts`=1787954747128,
  **50 bids (desc) + 50 asks (asc) = 100 niveles**.
- **DOCUMENTADO Bybit:** tras suscribir se recibe snapshot; un **nuevo snapshot = reset local**;
  `u==1` (reinicio Bybit) ⇒ overwrite. El P0 no observó `u==1` en esta ventana (ventana estable).
- **Semántica del snapshot para OCM:** es la base de reconstrucción; BookBuilder `is_ready=False`
  hasta snapshot válido; cualquier snapshot nuevo ⇒ reemplaza estado (overwrite).

## 11. Delta semantics (OBSERVADO + DOCUMENTADO)

- **MULTINIVEL CONFIRMADO: 75.1% de deltas con >1 nivel; máx 88 niveles en un mensaje.**
  Distribución: nivel2=347, nivel1=319, nivel3=208, nivel4=116, nivel5=91, ...
- Un delta modifica varias bids Y/0 asks en el mismo mensaje → **debe tratarse como unidad atómica**
  (aplicar todo o nada), no por nivel.
- **Delete semantics (DOCUMENTADO + OBSERVADO):** niveles con `size`="0" (ej. `["77355.00","0"]`) = eliminar;
  no-existe ⇒ insertar; existe ⇒ actualizar.
- **Implicación OCM (D-7a):** el wire v1 que aplana 1 nivel/msg **destruye** esta atomicidad y
  multiplicaría el volumen ~2-4× (media ~2.2 niveles/delta). Requiere **schema v2 multinivel atómico**.

## 12. Sequence / Update-ID semantics (OBSERVADO — definitorio)

| Metrica (60 s, 1280 msgs) | `u` (Update ID) | `seq` (Cross sequence) |
|---|---|---|
| deltas | eq1=**1279**, neq1=**0**, min=max=1 | eq1=**0**, neq1=**1279**, min=9, max=7593, p50=57 |
| distinct / dup | 1280 / 0 | 1280 / 0 |
| ejemplos | 101054915→…→101054924 incrementa de 1 en 1 | 800573227279→…328→…356→…392 (saltos grandes) |

**CONCLUSIÓN EMPÍRICA (D-7b):**
- `u` = **Update ID por libro**, **estrictamente contiguo (+1)** en la ventana observada → es el
  **identificador de gap/orden primario** correcto para Bybit.
- `seq` = **cross-sequence global por exchange** (compara niveles entre sí, no es +1 por libro) →
  **NO usar `seq+1` para gap** (100% falsos gaps).
- **No se impone `+1` como garantía universal**: se observó contiguo en esta ventana; el modelo debe
  tolerar saltos legítimos de `u` (p. ej. al re-snapshot con `u==1`, o gaps de entrega) y validarlo
  con snapshot-restore (Modelo C, §14), no con aritmética estricta ciega.

## 13. Checksum

- **OBSERVADO + DOCUMENTADO:** **NO existe checksum** en el orderbook WS de Bybit (ni campo en los
  mensajes). → Para Bybit, la integridad se valida por `u`/`seq`/invariantes/delete-semantics y
  **snapshot-restore**; NO por checksum. (Checksum genérico queda como mecanismo por-exchange para
  Binance etc., formalmente diferido para Bybit.)

## 14. Gap semantics (DECISIÓN → EVIDENCIA)

**Criterio de gap validado empíricamente (Bybit):**
- **Gap por `u`**: si el `u` esperado no es el contiguo respecto al último estado válido, hay
  inconsistencia → **invalidar estado, no publicar**, emitir `GapDetectedEvent`, solicitar snapshot
  fresco y **restaurar desde snapshot** (overwrite). Modelo **C** (modo preferido): `u`-validation +
  stale-rejection + **snapshot restoration**.
- `seq` se usa para **correlación/ordenamiento** (y con `cts`↔`T` de trades), no para gap `+1`.
- No se fija contigüidad universal sin evidencia; el P0 sí observó contigüidad de `u` en esta ventana,
  pero el diseño debe manejar re-snapshot (`u==1`) y reordenamiento como condición normal.

## 15. Reconnection / resynchronization

- **OBSERVADO:** 0 reconnects en la ventana (estable).
- **DOCUMENTADO Bybit:** reconnect ASAP; al reconectar **se reenvía snapshot (garantizado latest)**;
  si falta, envían snapshot y `u==1` (overwrite).
- **Modelo de recovery OCM (ADR-0028 §11, coherente con P0):** detector de desconexión/reintento en el
  productor; en BookBuilder, al detectar lag/reconnect ⇒ `is_ready=False` hasta **snapshot fresco** ⇒
  overwrite. Reutilizar infra Kafka (replay/seek) como backstop; la visibilidad de `u==1` confirma la
  resincronización.

## 16. Data freshness

- `latency_ms` (local-ts): p50 ≈ 163 ms, min 162, max 528 (60 s, 1279 deltas).
- `ts` rango: [1787954747128, 1787954806048] (~59.3 s de datos).
- Bybit L50 push ~20 ms; ~21 msgs/seg orderbook en este símbolo/ventana.
- **Implicación:** métricas `ocm_book_snapshot_age_seconds` y `ocm_book_latency_ms` deben existir para
  distinguir stale; no basta "proceso activo".

## 17. Comparación contra OCM actual

| Atributo | Bybit real (P0+F1) | OCM hoy (CÓDIGO) | Gap |
|---|---|---|---|
| `u` (Update ID) por libro, contiguo | requerido para gap/orden | **NO en wire v1** (snapshot/delta sin `u`) | CRÍTICO |
| `seq` (cross) | presente, NO contiguo | **NO en wire v1** | CRÍTICO |
| `cts` | presente (correlación trades) | NO en wire v1 | MEDIO |
| delta **multinivel** (unidad atómica) | 75% >1 nivel | **aplanado 1 nivel/msg** (`cryptofeed_orderbook_stream.py:208-218`) | CRÍTICO |
| snapshot = reset/overwrite | sí | productor emite snapshot; consumidor NO existe | CRÍTICO |
| delete size=0 | sí | representado vía `size="0"`? VO `is_delete` qty=0 | VERIFICAR |
| checksum | NO existe | campo `checksum: Optional` nunca validado (producer) | BAJO (quitar para Bybit) |
| burst contigüidad | `u` contiguo observado | no hay consumidor | — |
| resync re-snapshot | sí | no hay consumidor | CRÍTICO |

## 18. Defectos / hallazgos

- **F-H1 (crítico):** v1 **destruye** el multinivel atómico y **descarta `u`/`seq`/`cts`** → imposible
  reconstruir/validar/resinc/ordenar el libro desde `orderbook.raw` actual.
- **F-H2 (crítico):** v1 desconoce `u`/`seq` → no puede detectar gaps ni rechazar stale.
- **F-H3:** no existe BookBuilder/consumer → `book.*` vacíos.
- **F-H4:** `seq` NO debe usarse como `+1` (demostrado). Riesgo de falsos gaps si se implementa mal.
- **F-H5:** `checksum` en wire para Bybit sin soporte real → confusión; debe quedar `None`/genérico.
- **F-H6:** service FAILED por config; data-plane stale; obs no operacional (bloqueos transversales).

## 19. Impacto sobre ADR-0028

El P0 **valida** la propuesta ADR-0028 y **reduce la incertidumbre de D-7b**: ahora hay evidencia
empírica de que, en Bybit, `u` (por libro) es contiguo y `seq` (cross) no lo es. La propuesta ya
recomendaba `u` como gap primario y `seq` para correlación; el P0 lo **confirma con datos**. Se
confirma también el delta multinivel atómico (D-7a) y el modelo de recovery por snapshot (D-7b Modelo C).
**No se convierte la propuesta en ADR aceptado**; queda pendiente D-7 / D-7a / D-7c / D-7d y la
implementación.

## 20. Recomendación de BookBuilder

1. **Schema v2** (`shared/kafka/schemas/orderbook.py`): snapshot aditivo `+u/+seq/+cts`; delta **multinivel
   atómico** `{u, seq, cts, bids:[(p,q)..], asks:[(p,q)..]}` (size 0 = delete). `checksum=None` para Bybit.
2. **Gap primario por `u`** (por libro); `seq` para correlación/ordenar; `cts`↔`T`.
3. **Snapshot = overwrite/reset**; `u==1` (o evento de re-snapshot) ⇒ `is_ready` se re-inicializa y se
   reconstruye. Deltas solo si hay snapshot válido y `u` coherente.
4. **Modelo C**: `u`-validation + stale-rejection + **snapshot restoration**; gap ⇒ `GapDetectedEvent` +
   `is_ready=False` + no publicar estado inválido.
5. **Estructura:** aplicar delta completo (todo o nada) — no por nivel.
6. **Observabilidad:** `ocm_book_*` (ready, gaps, duplicate, out-of-order, stale, snapshot_age, latency) +
   consumer lag.
7. **Separación**: BookBuilder = consumidor Kafka de `orderbook.raw` (no GapAwareStream; ese sigue en
   trades WS). BookBuilder expone `MarketDataViewPort` (outbound).

## 21. Qué debe implementarse

- Schema v2 (multinivel + `u/seq/cts`); productor propaga `u/seq/cts` y **no aplana** (paquete atómico);
  `KafkaConsumerAdapter.for_book_builder()` + `GROUP_BOOK_BUILDER`; BookBuilder + `BookState` +
  buffer + gap/resync; `MarketDataViewPort` (según D-7d); métricas `ocm_book_*`; contrato BC-56;
  tests unit/integration; fixtures congeladas del P0 (evidence) como casos; doble-raíl de promo (ADR-0017
  componente 12/13: fixtures + tests de linaje). **Depende de D-7/D-7a/D-7c/D-7d.**

## 22. Qué NO debe implementarse

- **No** usar `seq+1` como gap (demostrado falso).
- **No** checksum para Bybit como mecanismo de integridad.
- **No** reutilizar/malar GapAwareStream como BookBuilder.
- **No** habilitar trading/execution/credenciales; no tocar producción/systemd/config en esta fase.
- **No** declarar MARKET DATA READY sin la cadena completa observada y validada.

## 23. Riesgos

| ID | Riesgo | Sev. |
|---|---|---|
| R1 | Implementar con `seq+1` → falsos gaps/resync continuos | ALTO (refutado hoy) |
| R2 | Wire v1 aplanado sin `u/seq` → libro corrupto silencioso | ALTO |
| R3 | Publicar estado inválido tras gap sin snapshot-restore | ALTO |
| R4 | Habilitar Bybit con credenciales para datos públicos | CRÍTICO (evitable) |
| R5 | Declarar READY por proceso/activo sin data-plane | CRÍTICO |
| R6 | Confiar en contigüidad `u` de una ventana de 60 s sin monitoreo continuo | MEDIO |

## 24. Decisiones humanas requeridas

1. **D-2/D-1 (Bybit en producción?):** habilitar Bybit **PUBLIC MD ONLY** en config para que el streaming
   y el pipeline (y el futuro BookBuilder) tengan flujo real → DECISIÓN de producción, humana.
2. **D-7 (gobierno):** aprobar ADR-0028 + schema v2 para implementar.
3. **D-7a:** delta **multinivel atómico** (recomendado, confirmado por P0) — decisión de schema.
4. **D-7b:** gap por `u` + snapshot-restore (Modelo C) → confirmado empíricamente; formalizar como política.
5. **D-7c:** frontera Decimal/float en BookState/mid-spread.
6. **D-7d:** alcance Fase 1 (viewport/replay) del BookBuilder.
7. **Descubrimiento (ADR-0017):** definir si se institucionaliza el **Discovery Profile de Bybit** como
   artefacto formal (evidencia+validación+promoción+limitaciones) al implementar el pipeline; hoy el
   framework existe (ADR-0017) pero **sus componentes 4-8 no están implementados en código** (solo
   Contract Provenance y el orderbook `PROTOCOL`).

## 25. Evidencia reproducible

- Script: `docs/audits/p0_bybit/p0_bybit_orderbook.py`.
- Evidence raw: `docs/audits/p0_bybit/evidence/20260828T220545Z/raw.jsonl` (1283 mensajes).
- Summary: `docs/audits/p0_bybit/evidence/20260828T220545Z/summary.json`.
- **Comando reproducible:** `.venv/bin/python docs/audits/p0_bybit/p0_bybit_orderbook.py --duration 60`.
- **Análisis de evidencia (riguroso, 1280 msgs):** `u` eq1=1279/neq1=0; `seq` eq1=0/neq1=1279;
  deltas >1 nivel=75.1%; snapshot 100 niveles; latencia p50 163 ms; 0 duplicados; 0 reconnects.

## 26. Estado de Trading

**TRADING = BLOCKED.** No se conectó nada privado, no se envió ninguna orden, no se tocó balances/
posiciones/permisos/credenciales. Solo WS público read-only.

## 27. Veredicto

**`P0 COMPLETE — IMPLEMENTATION REQUIRED`**

No es MARKET DATA READY. El P0 aporta la evidencia empírica que faltaba para fijar D-7a y D-7b, valida
el diseño ADR-0028 y desbloquea la fase de implementación (schema v2 + BookBuilder) sujeto a D-7/D-1
(habilitar Bybit PUBLIC MD) y D-7c/D-7d. Trading permanece bloqueado.

---

## ANEXO A — FASE DISCOVERY (obligatoria, previa a interpretación)

### ¿Existe contrato formal de discovery en OCM?

**SÍ existe (ADRs):**
- **ADR-0017 (Protocol Discovery Framework, Aceptado 2026-08-06):** metodología formal única de
  discovery/validación/modelado de protocolos externos. 14 componentes: Objetivo, Principios, Tipos de
  evidencia (PROTOCOL/DOCUMENTATION/UPSTREAM_LIBRARY/DOMAIN/ASSUMED), **REST Discovery**, **WebSocket
  Discovery**, Execution/Funding/Liquidation Discovery, **Contract Provenance**, Normalización,
  Validación, **Fixtures**, **Tests**, **Promotion Rule**.

**¿Qué hay implementado en código?**
- `shared/kafka/provenance.py` = **Contract Provenance** (componente 9) implementado: taxonomía,
  `_PROMOTED_STATES`, `is_promoted()`, `require_promoted()`. Los schemas `OrderBookSnapshotPayload` /
  `OrderBookDeltaPayload` ya están registrados como **PROTOCOL** ("WS Bybit observado (cryptofeed)") → **promovidos a SSOT**.
- `tests/kafka/test_schema_provenance.py` = semilla de tests de linaje (componente 13).
- **NO existe** módulo/directorio `discovery` ni `discovery_profile` en `packages/`/`shared/` → los
  componentes 4-8 (REST/WS/Execution/Funding/Liquidation Discovery) **NO están implementados como perfiles**.
- El port `MarketDataSource` solo expone `subscribe_trades/start/stop`; **no hay API de discovery de
  instrumentos/capabilities/channels**.

**Cumplimiento ADR-0017 del P0:** el P0 ES la ejecución del **WebSocket Discovery** (componente 5)
del "primer profile" (Bybit) que ADR-0017 designa. Con su evidence (fixtures raw) y el provenance
`PROTOCOL` ya fijado, el P0 satisface los componentes 5, 9, 12, 13 del PDF para el orderbook Bybit.

**Discrepancia formal (reportada, no corregida):** ADR-0017 es "Accepted" y define el framework, pero
sus componentes de discovery operativo (4-8) no tienen implementación ni profile formal en el repo
(excepto provenance y observación parcial por cryptofeed). No se crea un ADR nuevo en esta fase.

### Discovery por dimensiones

| Dimensión | Hallazgo | Clasificación |
|---|---|---|
| 1. CAPABILITY DISCOVERY | OCM: no hay API de capabilities; frameworks: cryptofeed/cryptofeed.ttls expone channels del exchange. Bybit D50/100/200/1000; trades/kline/etc. | DOCUMENTADO Bybit + INFERIDO OCM (no formalizado) |
| 2. INSTRUMENT DISCOVERY | Bybit GET /v5/market/instruments-info (category=linear) lista símbolos. OCM usa `symbols` hardcodeadas en config (BTCUSDT/ETHUSDT/SOLUSDT); sin discovery dinámico. | DOCUMENTADO Bybit + OBSERVADO (config estática OCM) |
| 3. CHANNEL DISCOVERY | Bybit tópicos `orderbook.{d}.{s}`, `publicTrade.{s}`, `kline.{i}.{s}`, `tickers.{s}`; OCM suscribe por config/runner, no por discovery de channels. | DOCUMENTADO Bybit + INFERIDO OCM |
| 4. PROTOCOL DISCOVERY | handshake: subscribe (`{"op":"subscribe","args":[...]}`) → snapshot+COMMAND_RESP; ping/pong 20 s. OBSERVADO en P0. | OBSERVADO (P0) + DOCUMENTADO Bybit |
| 5. SEMANTIC DISCOVERY | snapshot=reset; delta multinivel; `u` contiguo (gap por libro); `seq` cross (no +1); delete size=0; sin checksum. OBSERVADO. | OBSERVADO (P0) + DOCUMENTADO Bybit |
| 6. RECOVERY DISCOVERY | reconnect → re-snapshot (latest) / `u==1` overwrite; snapshot-restore. OBSERVADO parcial (ventana estable) + DOCUMENTADO. | DOCUMENTADO Bybit + OBSERVADO parcial |
| 7. CONTRATO FORMAL OCM | ADR-0017 (PDF) Accepted; provenance implementado; perfiles operativos NO implementados. | DEFINIDO por ADR + parcialmente implementado |

### Nota de no-suposición
No se confunden "configurar un exchange" con "discovery". OCM tiene **configuración** (config/exchanges)
y un **framework formal** (ADR-0017) con **provenance** implementado, pero **no** un perfil de discovery
operativo en código para Bybit ni una API de discovery de instrumentos/capabilities/channels. Eso queda
declarado, no inventado ni ocultado.
