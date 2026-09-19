# AUDITORÍA DE RECONCILIACIÓN — ADR-0017 → BYBIT DISCOVERY → P0 → ADR-0028 → BOOKBUILDER

- **Fecha:** 2026-08-28 · **Modo:** READ-ONLY (sin cambios de código/producción/ADR/tracking/CI/merge) · **Trading:** BLOQUEADO
- **Mandato:** Extraer y documentar los **requisitos** de ADR-0017, ADR-0028, Plan Maestro y evidencia P0 **antes** de implementar BookBuilder. No implementar a ciegas.
- **Fuentes (jerarquía):** evidencia primaria > doc oficial Bybit > código > tests > ADR > docs proyecto > literatura > opinión agente.

---

## 0. Decisión humana tomada (contexto)

**DH-1/D-1 (AUTORIZADO):** Bybit = primer exchange de prueba, **SOLO PUBLIC MARKET DATA**, sin auth-trading, sin órdenes, sin ejecución, sin promoción a live. **Canary de Market Data, NO habilitación del motor de trading.** Trading continúa BLOQUEADO.
→ Esto habilita ejecutar el canary de data-plane; **no** autoriza schema v2/BookBuilder (pendiente D-7/DH-3) ni producción de trading.

---

## 1. Marco documental — qué define cada fuente

### 1.1 ADR-0017 (Protocol Discovery Framework) — Aceptado
- **Metodología única y permanente (PDF, 14 componentes):** 1 Objetivo, 2 Principios, 3 Tipos de evidencia (PROTOCOL/DOCUMENTATION/UPSTREAM_LIBRARY/DOMAIN/ASSUMED), 4 REST Discovery, 5 WebSocket Discovery, 6 Execution, 7 Funding, 8 Liquidation, 9 Contract Provenance, 10 Normalización, 11 Validación, 12 Fixtures, 13 Tests, 14 Promotion Rule. (ADR-0017:14-64)
- **Contrato de Discovery Profile (la "hoja"):** cada fuente declara **explícitamente**:
  1. **Fuentes de evidencia** — qué mensajes/endpoints provee (REST, WS, callbacks).
  2. **Validación** — qué invariantes comprueba sobre los mensajes antes de normalizar.
  3. **Criterios de promoción** — qué contratos promueve a SSOT y con qué evidencia.
  4. **Limitaciones** — qué no observó, qué está ASSUMED, qué campos son especulativos. (ADR-0017:83-89)
- **Bybit = primer profile** (ADR-0017:93). Prueba por profile, no rediseño del framework.
- **Promotion Rule (14):** un contrato se promueve a SSOT solo con provenance estable; **ASSUMED bloquea live** (gate F2.5 / ADR-0020). (ADR-0017:64, 102-104)
- **BC-NN que lo hacen cumplir:** BC-29 (wire en `shared.kafka`), BC-09 (dominio framework-agnostic; PDF en adapters/infra), `test_schema_provenance.py` (linaje). (ADR-0017:115-118)
- **Deuda aceptada:** puntos 4-8 implementados **por profile**; hoy solo Bybit tiene observación parcial (orderbook vía cryptofeed). (ADR-0017:112-114)

### 1.2 ADR-0013 (Modelo unificado ingestión) — **Aceptado**
- Fuente ≠ mecanismo ≠ feed (ortogonales); todo mecanismo converge al mismo modelo de eventos de dominio. (ADR-0013:37-46)

### 1.3 ADR-0014 (Diseño interno market_data) — **Propuesto**
- Capabilities internas: `realtime_feeds`, `external_ingestion`, `normalization`, `kafka boundary` (todo termina en Kafka SSOT). `data_quality` reservada: **timestamp validation, missing/duplicate detection**. (ADR-0014:71-91)
- NOTA: es **Propuesto**, no Aceptado → su "deber ser" es guía normativa de menor jerarquía que un ADR aceptado, pero está citado en ADR-0028 como requisito F3.

### 1.4 ADR-0023 / B-25
- Gap detection diferido **product/consumer-side de `orderbook.raw`** "hasta que exista un consumidor real" con sequence + DLQ. BookBuilder es ese consumidor; al aprobarse **reabre B-25**. (ADR-0028 proposal:8-10, 77)

### 1.5 ADR-0028 / propuesta — **Propuesto / NO aprobado**
- Estado: **PROPUESTA** (línea 3). Diseña el **BookBuilder** = consumidor+constructor+validador del L2 en memoria por `(exchange, symbol)`, detecta gaps, resync, expone **viewport**; reabre B-25 y habilita B-MD-004 (market validity) en Fase 2. (propuesta:42)
- Requisitos que **ya están documentados en la propuesta** y que la implementación debe respetar:
  - schema v2 con `seq`/`u`/`cts` (gap: wire v1 sin ellos, propuesta:94).
  - delta **multinivel atómico** (no aplanar, propuesta:99 → riesgo estados intermedios).
  - **D-7b gap:** primario por `u` por-book (recomendado) VS `seq` contiguo; **NO se asume `seq+1`** — requiere P0 empírico (R1 alto, propuesta:152, 243, 256).
  - `u==1` ⇒ snapshot/reinicio ⇒ overwrite libro; resync por re-snapshot; `seq` para ordenar/correlacionar con `cts`/`T`; **sin checksum** en Bybit (propuesta:58-62).
  - **Precisión (D-7c):** wire str(Decimal) vs dominio float; frontera de conversión para no perder precisión (falso mid/spread) (propuesta:68, 100, 191, 257).
  - **D-7d (Fase 1):** si incluir `MarketDataViewPort` + `seek_to_beginning`/replay (propuesta:258).
  - Unit tests: snapshot; deltas `101→102→103`; gap `101→102→104`; duplicado; out-of-order; snapshot inválido (crossed/sorting); overwrite `u==1`; delete qty=0; precisión; atomicidad multinivel (propuesta:217).
  - Operational verification / DATA-PLANE HEALTH criterio §17 (propuesta:223).

### 1.6 Plan Maestro (F2.5 / F2.6 / gate)
- **F2.5** cerrada: ADR-0017 aceptada = gate normativo antes de capital. (Plan:177-190)
- **F2.6b** (Streaming Entrypoint MVP) HECHO 2026-08-08. **F2.6c** = canary bajo systemd. (Plan:236-256)
- **F3** (trading live) requiere gate de capital; **NO es la ruta actual** → "NO NEXT MILESTONE DEFINED para Market Data post-F2.6" (documentado, no invento B-61+).

---

## 2. Matriz de reconciliación ADR-0017 → Requisito → Estado

| # | Requisito (fuente) | Definido por | Implementado | Evidencia | Estado |
|---|---|---|---|---|---|
| R1 | Profile declara **fuentes de evidencia** (mensajes/endpoints) | ADR-0017:86 | No perfil Bybit | sin módulo `discovery` | **FALTA** |
| R2 | Profile declara **validación** de invariantes | ADR-0017:87 | Parcial (VOs domain) | `order_book.py` invariantes | **PARCIAL** |
| R3 | Profile declara **criterios de promoción** (SSOT) | ADR-0017:88 | Contract Provenance + Promotion | `provenance.py` | **PASS** |
| R4 | Profile declara **limitaciones** (ASSUMED/observado) | ADR-0017:89 | No perfil Bybit | — | **FALTA** |
| R5 | Contract Provenance (comp. 9) | ADR-0017:56-64 | Sí | `provenance.py` | **PASS** |
| R6 | Promotion Rule (comp. 14) — ASSUMED bloquea live | ADR-0017:64,102-104 | Sí | `require_promoted` | **PASS** |
| R7 | RD4-8 REST/WS/Execution/Funding/Liquidation por profile | ADR-0017:112-114 | No (Bybit parcial orderbook WS) | P0 evidence | **PARCIAL** (solo WS orderbook) |
| R8 | WS Discovery (comp. 5) para Bybit | ADR-0017:5 | Operativo read-only P0 | P0 evidence | **PARCIAL** (perfil no formalizado) |
| R9 | Fixtures (comp. 12) | ADR-0017:61 | No congeladas como contrato | P0 evidence raw | **FALTA formalizar** |
| R10 | Tests linaje (comp. 13) | ADR-0017:62 | Sí | `test_schema_provenance.py` | **PASS** |
| R11 | Normalización SSOT (comp. 10) | ADR-0017:59,113 | Sí | `normalization.py` | **PASS** |
| R12 | Wire en `shared.kafka` (BC-29) | ADR-0017:116 | Sí (v1) | `orderbook.py` | **PASS (v1)** / schema v2 pendiente |
| R13 | Dominio framework-agnostic (BC-09) | ADR-0017:117 | Sí | domain sin pandas/protocolo | **PASS** |

## 3. Matriz de reconciliación P0 → ADR-0028 (D-7)

| D-7 sub | Pregunta | Evidencia P0 (empírica) | Resolución |
|---|---|---|---|
| **D-7b** | gap primario por `u` por-book vs `seq` contiguo | `u` estrictamente +1 (eq1=1279/1279, 0 dupes); `seq` NO contiguo (min gap 9, max 7593) → `seq+1` = 100% falsos gaps | **Resuelto: gap primario por `u` por-book; `seq` global solo orden/correlación.** (confirma recomendación de propuesta; no se asume `seq+1`) |
| D-7a | delta multinivel atómico | 75.1% msgs multinivel; máx 88 niveles | **Confirmado: unidad atómica multinivel** (no aplanar) |
| — | checksum Bybit | **ausente** en todos los WS msgs | **No implementar checksum** (semántica Bybit: no existe) |
| — | snapshot reset / `u==1` | snapshot=reset (100 niveles); `u==1`=0 en ventana | snapshot-restore ante `u==1`/re-snapshot |
| D-7c | precisión | wire str(Decimal), dominio float | **decisión pendiente** (frontera de conversión) |
| D-7d | viewport/replay Fase 1 | — | **decisión pendiente** (alcance) |

**Estado de D-7:** D-7a y D-7b **resueltos con evidencia P0**; D-7c y D-7d **siguen pendientes** (requieren decisión humana/arquitectura).

---

## 4. Cadena del data-plane — por etapa

| Etapa | ¿Existe? | ¿Implementada? | ¿Conectada? | ¿Probada? | ¿Ejecutándose? | ¿Evidencia operacional? | Falta |
|---|---|---|---|---|---|---|---|
| Bybit public WS | Sí | CryptofeedOrderBookStream (ACL) | No en runtime | canary v1 | No | P0 aislado | canary real |
| Adapter (MarketDataSource) | Sí | BybitFeedAdapter (trades) | No orderbook | unit | No | — | ordenbook via adapter |
| Normalización | Sí | normalization.py SSOT | Parcial | tests | — | — | — |
| Schema wire | Sí (v1) | orderbook.py | Definición | tests wire | — | — | schema v2 `u/seq/cts`+multinivel |
| Kafka producer | Sí | orderbook_producer.py | No runtime | tests | No | orderbook.raw STALE ~7d | feed vivo |
| BookBuilder | **No** | — | — | — | — | — | implementación (tras D-7/DH-3) |
| Estado orderbook | No | — | — | — | — | — | BookBuilder |
| Observabilidad | Parcial | plantillas/métricas | No | No | No | stack no healthy | stack+datos+alertas |
| Replay/Recovery | No | — | — | — | — | — | — |

**Conclusión §4:** hay **código**, pero **no está operativo end-to-end**; `ingestion_mode: rest` + ningún exchange habilitado en producción (F-002/F-003).

---

## 5. Config / Systemd — estado y discrepancia

- **Config doble gate (F-003):** `streaming_hydra` valida `feeds.feeds.<>.enabled` (SSOT `config/market_data/feeds.yaml`, bybit true) vs schema valida `exchanges.*.enabled` (SSOT `config/exchanges/bybit.yaml`, false). → **DECISIÓN de diseño** para unificar (no se resuelve silenciosamente).
- **DH-1/D-1** autoriza Bybit MD en producción → requiere setear un exchange `enabled: true` de forma controlada (solo data-plane).
- **Systemd (F-004):** 3 variantes divergentes (instalada `/etc` vs `rendered/` gitignored vs `templates/`). El unit desplegado **no** corresponde exactamente al SSOT versionado. → reconciliar en una unit versionada (base PR #20), test restart (B-59). **No se declara B-59 cerrado sin evidencia.**
- **Root cause F-002:** producción sin exchange habilitado → schema valida → exit 1.

---

## 6. Observabilidad — CONFIGURADO vs DESPLEGADO vs ACTIVO

- **B-58:** Grafana provisioning gitignored/vacío; Prometheus/Grafana/Loki/Alertmanager/Promtail **no healthy** en Docker. → **SOLO CONFIGURADO/parcial; NO activo, NO recibiendo datos, NO generando alertas.** No se declara B-58 verificado.
- Pushgateway Up (healthy) pero no prueba data-plane.

---

## 7. Discrepancias formales (ACTUAL vs ESPERADO vs EVIDENCIA vs DECISIÓN)

| Discrepancia | ACTUAL | ESPERADO (fuente) | EVIDENCIA | DECISIÓN |
|---|---|---|---|---|
| Wire v1 sin `u/seq/cts`, delta aplanado | wire v1 | schema v2 multinivel atómico (ADR-0028/D-7a) | schema v1 (F-006); propuesta:94 | D-7/DH-3 |
| `seq+1` como regla universial | no aplicado en código | NO usar `seq+1`; usar `u` | P0 (D-7b resuelto) | confirmado |
| Checksum capturado y publicado, nunca contrastado | checksum Optional en v1 | Bybit no tiene checksum → no validar checksum | propuesta:65 | D-7 (no checksum) |
| Doble gate exchange/feed | streaming valida feeds; schema valida exchanges | una SSOT | feeds.yaml vs exchanges/bybit.yaml | diseño F-003 |
| Discovery Profile no existe | sin perfil Bybit | perfil formalizado (ADR-0017:83-89) | F-005; ADR-0017 | DH-2 |
| systemd divergente | 3 variantes | unit versionado == instalado | F-004 | B-59/DH-4 |
| Observabilidad no activa | plantillas | stack + datos + alertas | F-007/B-58 | B-58 |
| BookBuilder inexistente | — | consumidor+validador (ADR-0028) | — | D-7/DH-3 |

---

## 8. Gaps

1. Schema v2 no implementado (necesario para BookBuilder).
2. Discovery Profile de Bybit no formalizado (ADR-0017 core).
3. Fixtures del P0 no formalizadas como contrato (comp. 12).
4. Ningún exchange habilitado en producción (data-plane no corre).
5. systemd divergente; B-59 sin verificación operacional.
6. Observabilidad no activa (B-58).
7. Sin replay/recovery.
8. D-7c (precisión) y D-7d (viewport/replay) pendientes de decisión.

---

## 9. Riesgos

- **R1 (mitigado):** asumir `seq+1` → **resuelto por P0** (D-7b). *antes: propuesta R1 alto.*
- **R2 (abierto):** Decimal→float pierde precisión → D-7c pendiente.
- **R4 (aceptado):** retención `orderbook.raw` 1h limita replay; snapshot fresco al arranque.
- **Operacional:** habilitar exchange en producción es decisión (DH-1 ya dada para MD público); no debe escalar a trading.

---

## 10. Decisiones humanas (solo las reales)

| ID | DECISIÓN | OPCIONES | RECOMENDACIÓN | EVIDENCIA | RIESGO | IMPACTO | QUÉ CAMBIA | QUÉ NO CAMBIA |
|---|---|---|---|---|---|---|---|---|
| **DH-2** | Formalizar **Discovery Profile de Bybit** (fuentes/validación/promoción/limitaciones) + P0→fixtures/provenance | (a) ahora; (b) junto a schema v2 | (a) ahora | ADR-0017:83-89; F-005 | bajo | Cumple ADR-0017 core | Docs/artefactos discovery | prod/trading |
| **DH-3 / D-7** | Aprobar ADR-0028 + **schema v2** (`u/seq/cts`+multinivel) + registrar PROVENANCE + BookBuilder consumer | (a) schema v2 + BookBuilder; (b) solo schema v2; (c) diferir | (a) tras canary vivo | ADR-0028 propuesta; D-7a/b resueltos | medio (contrato irreversible) | Desbloquea integridad orderbook | schema + BookBuilder | trading |
| **DH-3b / D-7c** | Frontera de precisión Decimal→float en BookBuilder | mantener Decimal en BookState vs float | mantener Decimal en BookState (evitar falso mid) | propuesta R2/191 | medio | precisión | BookBuilder | — |
| **DH-3c / D-7d** | Alcance Fase 1: viewport + replay/seek | incluir vs diferir | incluir `MarketDataViewPort`; replay diferido | propuesta:258 | medio | requerimientos Fase 1 | viewport | — |
| **B-59** | Criterio de cierre systemd | cerrar solo con evidencia (arranque+datos) vs unit basta | exigir evidencia operacional | B-59/ADR-0022 | bajo | DoD honesto | estado tracking | — |
| **B-58** | Cierre observabilidad | con stack+datos activos vs provisioning | exigir stack activo + datos | F-007 | bajo | DoD honesto | estado tracking | — |

**No requiere decisión humana (autónomo/mecánico), tras DH-1:** habilitar config MD en producción de forma controlada siguiendo SSOT SÍ es producción → **depende de DH-1 ya otorgada** (pero ejecutar el canary requiere reconciliar systemd F-004 — mecánico); B-60 (documental) — mecánico.

---

## 11. Recommended next steps (orden determinista, MARKET-DATA-FIRST)

1. **[Bloqueante — D-7/DH-3]** Aprobar ADR-0028 + alcance de schema v2 (D-7c/D-7d). *P0 ya resolvió D-7a/D-7b.*
2. **[Autónomo tras ello]** Implementar `schema v2` (`u/seq/cts` + delta multinivel atómico) en branch dedicado, con registro `PROVENANCE` + fixtures del P0 (comp. 12/14). No aplanar; no `seq+1`; no checksum.
3. **[Autónomo]** Implementar `BookBuilder` consumer (`GROUP_BOOK_BUILDER`), validador, viewport, replay/seek según D-7d; reabrir B-25. **No reutilizar GapAwareStream** (responsabilidad separada).
4. **[Producción — DH-1 ya dada]** Reconciliar systemd (F-004) e iniciar canary MD real; validar Kafka (offsets/freshness/frescura/DLQ/continuidad).
5. **[Operacional]** Observabilidad activa (B-58): stack + dashboards versionados + freshness/integrity; health checks.
6. **[Verificación]** quality gates (tests/ruff/mypy/importlinter/audit_validator) + evidencia reproducible + tracking/audit/Plan si corresponde; closure solo con DoD completo.
7. **Trading permanece BLOQUEADO** hasta Market Data READY formal.

---

## 12. Criterio MARKET DATA READY (por verificar — ninguna casilla marcada aún)

Todos los ítems de la checklist del mandato §11 (systemd, estabilidad, bybit conecta, discovery/profile documentado, instrumento/canal id, datos llegan, Kafka reciente, snapshot/delta correctos, multinivel atómico, stale detectado, gap manejado, snapshot recovery, checksum, provenance, BookBuilder consume, orderbook coherente, observabilidad, health, restart, evidencia reproducible, tests/ruff/mypy/importlinter/audit_validator, trading bloqueado) **quedan abiertos** y deben cerrarse con evidencia operacional reproducible antes de declarar READY.

---

## 13. Tests / Commands / Evidencia

- **Tests:** baseline documentado 900 passed / 49/49 BC (tracking). BookBuilder unit list en propuesta:217 (a implementar). `test_schema_provenance.py` existe.
- **Commands:** git/log/branch/status; systemctl; journalctl; docker ps; GetOffsetShell (orderbook.raw/book.*/trades.raw); ps/cat /proc; cat config/*.yaml; sed schema.py/streaming_hydra.py; rg ADR-0017/0013/0014/0028/propuesta/tracking/Plan.
- **Evidencia primaria:** P0 (1280 msgs, 20260828T220545Z), systemd failed + journalctl root cause, Kafka offsets, Docker.

## 14. Commit/branch/PR refs

- Branch: `feat/p0-bybit-public-market-data` (HEAD `3897be0`); origin/main `44034ea`; merge-base `c392f8f`.
- PR #20 `feat/deploy-systemd-adr0022` (systemd/B-59) — base recomendada para reconciliar unit.
- PR #21 `chore/gitignore-deploy-secrets` (gitignore host.env/rendered).
- No se crearon commits/PRs/merges ni se modificó main.

---

*Fin de la auditoría de reconciliación. Read-only. Sin cambios de código/producción/ADR/tracking/CI/merge/.gitignore. Trading BLOQUEADO.*
