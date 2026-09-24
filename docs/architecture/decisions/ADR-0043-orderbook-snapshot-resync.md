# ADR-0043 — OrderBook Snapshot / Resync Architecture

**Estado:** Propuesto
**Fecha:** 2026-09-21
**Bounded context(s) afectado(s):** market_data

## 1. Contexto

La auditoría maestre ha confirmado:

- `OB-SNAPSHOT-01` = capacidad REST independiente para obtener un snapshot (pattern confirmado en Hummingbot/Nautilus; adapter OCM FALTANTE)
- `OB-RESYNC-01` = bloqueado: pending OB-SNAPSHOT-01 implementation + race condition resolution
- `Reconnect + snapshot (con disconnect)` = IMPLEMENTADO (Cryptofeed FeedHandler reconnect → delta None → on_snapshot → reset last) (OB-RESYNC-001:Scenario B)
- `Connected u-jump sin disconnect` = BLOQUEADO — gap detected → has_snapshot=False → nadie obtiene snapshot → STALE hasta próximo WS snapshot natural (OB-RESYNC-001:Scenario C)
- `snapshot/delta race condition` = PARTIALLY DEFINED — DELTA_BEFORE_SNAPSHOT descarta (book_builder.py:207); replay/no replay NOT DEMONSTRATED
- `Hummingbot` y `Nautilus` confirman pattern REST snapshot + WS deltas + sequence validation + gap → REST snapshot → rebuild → resume
- `OrderBook` issues son independientes de `Bronze` schema drift (Big Pickle finding: 11 vs 12 columnas, run_id)

El problema arquitectónico: OCM tiene detection y reconnect recovery implementados, pero el resync determinista para u-jump sin disconnect y el snapshot REST active durante WS life no están implementados.

## 2. Problema

Definir la arquitectura OrderBook para OCM que aclare:

- Snapshot REST active durante vida WS vs snapshot pasivo después reconnect
- Sequence semantics (u monótono +1 vs u-jump sin disconnect)
- Gap detection → REST snapshot → rebuild → resume pathway
- Disconnect recovery vs connected u-jump recovery
- Buffering/replay/deduplication de deltas during in-flight snapshot
- Canonical OrderBook contract (o UNKNOWN)

## 3. Drivers de decisión

Las siguientes propiedades importan:

- **Snapshot REST during WS life**: ¿should snapshot be actively fetched during WS life, or only on reconnect?
- **Sequence semantics**: u monótono +1 es Bybit-specific; canonical status UNKNOWN; u-jump sin disconnect requiere REST snapshot para determinismo
- **Reconnect recovery**: implementado (scenario B); disconnected u-jump recovery: BLOCKED
- **Race condition t0→t4**: gap detected → REST snapshot requested → WS continues deltas → snapshot arrives → deltas before snapshot discarded (book_builder.py:207) — necesidad de buffering/replay definido o no
- **Canonical OrderBook contract**: UNKNOWN en OCM; Hummingbot/Nautilus patterns confirmados pero requieren adaptación
- **Disconnect vs connected recovery**: disconnect recovery (reconnect + snapshot) = confirmado; connected u-jump recovery = BLOCKED

## 4. Consideradas alternativas

**A. Mantener separación disconnect/resync tal como están** (recomendado)
- *Ventaja*: Separación clara: reconnect recovery (confirmado) vs resync (pending); no introducir dependencias artificiales entre OHLCV y OrderBook; evidencia actual respalda esta separación
- *Desventaja*: OB-RESYNC-01 permanece BLOCKED hasta que se implemente OB-SNAPSHOT-01; race condition t0→t4 sin resolver

**B. Implementar snapshot REST active durante vida WS**
- *Ventaja*: Resuelve scenario C u-jump sin disconnect; path gap→REST→rebuild→resume queda definido
- *Desventaja*: Requiere adapter OrderBookSnapshotFetcher (NO existe en OCM); race condition t0→t4 necesita resolución (buffering/replay definido o no); depende de decisión ADR-0041 (contrato canónico)

**C. Fundir disconnect recovery y resync en un solo componente**
- *Ventaja*: Arquitectura más simple, menos componentes
- *Desventaja*: Rompe separación de concerns documentada en OB-RESYNC-001; OrderBook disconnect/reconnect y resync tienen propósitos distintos; evidencia actual los mantiene separados; riesgo de complicación innecesaria

## 5. Decisión

**Mantener separación explícita entre disconnect recovery y resync architecture**, con decisión deferred sobre snapshot REST active durante vida WS hasta que:

1. Se implemente OB-SNAPSHOT-01 (adapter `orderbook_snapshot_fetcher.py` + contrato `OrderBookSnapshotPayload`)
2. Se resuelva la race condition t0→t4 (mecanismo de replay/buffer para deltas during in-flight snapshot)
3. Se defina canonical OrderBook contract (o se determine UNKNOWN)

La arquitectura actual distingue:

- **DISCONNECT → RECONNECT → SNAPSHOT → REBUILD → RESUME** (IMPLEMENTADO: Cryptofeed reconnect → delta None → OnSnapshot → reset last)
- **CONNECTED WS → SEQUENCE GAP / u-JUMP → GAP DETECTED → ?** (BLOQUEADO: has_snapshot=False, nadie obtiene snapshot REST)
- **REST snapshot active durante WS life** → DEFERRED pending OB-SNAPSHOT-01 implementation

## 5. Consecuencias

### Consecuencias positivas

- Separación clara de concerns: disconnect recovery (confirmado) vs resync (pending)
- Sin dependencias artificiales entre OHLCV recovery y OrderBook resync (confirmado por evidencia)
- Evidence-based: Hummingbot/Nautilus patterns confirmados para pattern REST snapshot + WS deltas
- Race condition explicitly acknowledged (t0→t4: gap→REST→snapshot→deltas discarded)
- OrderBook issues independientes de Bronze schema drift (Big Pickle)

### Consecuencias negativas

- OB-RESYNC-01 permanece BLOCKED hasta que se implemente OB-SNAPSHOT-01 + race condition resolution
- Scenario C u-jump sin disconnect queda BLOCKED (STALE hasta próximo WS snapshot natural)
- No se implementa snapshot REST active durante vida WS (deferred)
- No se define buffering/replay de deltas during in-flight snapshot (PARTIALLY DEFINED: DESCARTE)

### Consecuencias operacionales

- Los equipos saben disconnect recovery (reconnect + snapshot) funciona; resync pending
- Los equipos saben scenario C u-jump sin disconnect no tiene recovery determinista actualmente
- Los gapes de OrderBook detected requieren intervención manual hasta que OB-SNAPSHOT-01 + resync resueltos
- No se asume capability que no está demostrada

## 6. Alternativas Rechazadas

- **Implementar snapshot REST active durante WS life ahora**: rechazado porque OB-SNAPSHOT-01 adapter no existe en OCM; race condition t0→t4 sin resolver; deferred a fase posterior después de ADR-0041 y adapter implementation
- **Fundir disconnect recovery y resync**: rechazado porque separa concerns documentados en OB-RESYNC-001; evidencia actual los mantiene separados; riesgo de complicación innecesaria
- **Asumir u es contrato canonical universal**: rechazado; u es Bybit-specific; canonical status UNKNOWN; decisión requiere ADR-0041 (contrato canónico)

## 6. Alcance

- **Dentro**: definición de architecture OrderBook snapshot/resync con separación disconnect/reconnect vs connected u-jump; race condition t0→t4 análisis; canonical contract status; dependencia/índependencia de OHLCV recovery
- **Fuera**: implementar OrderBookSnapshotFetcher adapter; implementar orchestración resync; modificar main.py ingestion loop; cambiar systemd units; rebuild Docker image

## 7. Dependencias

- Requiere ADR-0041 (contrato canónico) para definición completa de canonical OrderBook contract — pero separación disconnect/resync es independiente de contrato canonical specifics
- Requiere evidencia de Hummingbot/Nautulus confirmada ✅
- Requiere estado actual: OB-SNAPSHOT-01 CONDITIONAL GO, OB-RESYNC-01 BLOCKED ✅
- Requiere análisis race condition t0→t4 definido (DESCARTE vs REPLAY) ✅

## 7. Frontera de implementación

Esta decisión define la arquitectura y clasificación; la implementación (adapter OrderBookSnapshotFetcher, race condition resolution, canonical contract details) es fase posterior. La frontera indica:

- **Definición**: arquitectura OrderBook snapshot/resync con separación explícita de disconnect/reconnect vs connected u-jump; race condition acknowledgment; canonical contract status UNKNOWN
- **Pendiente**: implementar OB-SNAPSHOT-01 (adapter + contrato); resolver race condition t0→t4; definir canonical contract specifics
- **Future**: snapshot REST active durante vida WS; buffering/replay definido; resync orchestration

## Referencias

- AUDIT_OCM_orderbook-resync-architectural-decision_2026-09-21.md (OB-RESYNC-001)
- AUDIT_OCM_orderbook-reference-models-comparative-study_2026-09-21.md (Hummingbot/Nautilus patterns)
- AUDIT_OCM_reference-models-recovery-resync-comparative-study_2026-09-21.md (comparative evidence)
- AUDIT_OCM_ohlcv-repair-backfill-causality_2026-09-21.md (MD-RECOVERY-002: DETECTION ≠ RECOVERY)
- AUDIT_OCM_market-data-e2e-incompleteness_2026-09-21.md (runtime reality)
- Big Pickle audit: OrderBook independiente de drift schema Bronze

