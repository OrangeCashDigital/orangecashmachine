# Auditoría arquitectónica transversal (2026-08-03)

**Alcance:** principios estructurales de los bounded contexts (market_data,
trading, portfolio) y la plataforma (ocm, apps, shared).
**Tipo:** auditoría de calidad, no de bugs — verificación de cumplimiento.
**Método:** solo lectura; evidencia `ruta:línea`.
**Contexto:** ejecutada al cierre de la integración del composition root de
trading (eliminación de PaperBot y de los factories
`TradingEngine.build_live()/build_paper()`, ADR-0012).

---

## Checklist de cumplimiento

| Principio | Estado |
|---|---|
| 1. Un Composition Root por BC | ⚠️ PARCIAL — `OCMContainer` huérfano/roto en market_data; tercer ensamblado en `main.py` |
| 2. Ningún adapter concreto fuera de su root | ⚠️ PARCIAL — GoldReader en research; Redis en ocm y API |
| 3a. Contratos de capas / DIP | ⚠️ PARCIAL — trading sin contrato `layers` ni DIP de dominio |
| 3b. Dominio no importa infra | ✅ CUMPLE |
| 4a. Redis solo portfolio | ❌ NO CUMPLE — 4 dueños |
| 4b. PositionStore solo portfolio/bootstrap | ✅ CUMPLE |
| 4c. Gold solo trading-root + infra market_data | ⚠️ PARCIAL — + research |
| 4d. Kafka wire schemas solo shared/kafka | ✅ CUMPLE |
| 5. shared/ sin imports internos | ✅ CUMPLE |

---

## HALLAZGOS

### MAYORES

1. **`OCMContainer` muerto y roto en market_data.**
   `packages/market_data/infrastructure/bootstrap/container.py:119`. Imports
   fantasma `infrastructure.*` que no existen (`:64` `from infrastructure.timeouts
   import Timeouts`, `:98` cursor_store, `:108` snapshot_manager — el paquete raíz
   `infrastructure/` solo contiene `redis/redis_stream.py`). Cero imports reales
   en el repo (solo auto-referencias y docstrings en `application/strategies/
   repair.py:95`, `application/consumers/quality_consumer.py:77`,
   `adapters/outbound/kafka_gap_publisher.py:85`).
   **ACCIÓN (2026-08-03):** eliminado + limpieza del contrato BC-07 en
   `architecture/importlinter.toml`.

2. **Trading sin contrato `layers` ni DIP de dominio.**
   `architecture/importlinter.toml` tiene 4 contratos `layers` (BC-08
   market_data `:209`, BC-30 medallion `:275`, BC-26 ocm `:689`, BC-44
   portfolio `:856`). Trading solo tiene aislamientos puntuales (BC-36, BC-12,
   BC-50) — **no hay contrato que prohíba a un núcleo de trading importar
   infraestructura ni orden de capas**. Trading carece de `domain/ports/
   adapters/services` (solo `analytics, bootstrap, engine, execution, observers,
   risk, strategies`).
   **DEUDA PENDIENTE** — decisión de diseño (ADR-0007 ya la señala).

3. **Redis con múltiples dueños.**
   - `packages/portfolio/infra/redis_factory.py:39` — dueño legítimo (solo lo usa
     `portfolio/bootstrap/composition_root.py:159-168`). ✓
   - `ocm/runtime/state/cursor_store.py:154` (`RedisCursorStore`) — plataforma ocm.
   - `apps/api/deps.py:39` (`_redis_pool` → aioredis) + `apps/api/main.py:65`.
   - `packages/market_data/infrastructure/bootstrap/container.py:71` — código muerto
     (eliminado 2026-08-03).
   - Además, adapters de market_data construyen conexiones indirectamente vía
     factories de ocm: `adapters/inbound/rest/trades_fetcher.py:128,144`,
     `derivatives_fetcher.py:53,160`, `ohlcv_fetcher.py:208-210`,
     `adapters/inbound/rest/_cursor_factory.py:46`,
     `infrastructure/bootstrap/pipeline_factory.py:215,227` (BC-22 usa
     `allow_indirect_imports` y no prohíbe `ocm.runtime.state`).
   Contradice el "SSOT: un único dueño de la conexión por ejecución" declarado en
   `execute_live.py`.
   **DEUDA PENDIENTE.**

### MENORES

4. **`apps/research/data/data_access.py:40,221`** instancia `GoldReader`
   (alias `GoldLoader`) fuera de los roots autorizados. Consumer de solo lectura
   (BC-20 no lo prohíbe), pero rompe el inventario de propiedad Gold.
   **DEUDA PENDIENTE.**

5. **`packages/market_data/main.py:336-340`** instancia `IcebergStorageFactory()`
   directamente — tercer punto de ensamblado de market_data, coexistiendo con el
   root formal y el OCMContainer (eliminado).
   **DEUDA PENDIENTE.**

6. **AGENTS.md desactualizado:** declaraba "39 import-linter contracts" (hay 44)
   y "research not importable as package" (sí es paquete, en `root_packages`).
   **ACCIÓN (2026-08-03):** corregido.

### INFORMATIVOS

7. **Naming drift cursor store:** `ocm/runtime/state/cursor_store.py`
   (`RedisCursorStore`) es el cursor store real, pero `portfolio/bootstrap/
   composition_root.py:26` lo describe como "el cursor store de market_data".
8. **`apps/research` es importable como paquete** (`tests/research/
   test_data_access.py:30`), pese a AGENTS.md.
9. **`trading/execution/__init__.py` re-exporta `PaperExecutor`** — símbolo
   expuesto a import-time; nadie lo instancia fuera del root.

---

## Verificados limpios

- Un root formal por BC (trading/portfolio/market_data) que ensambla solo su BC.
- `PositionStore` concreto solo en `portfolio/bootstrap` (BC-43/44).
- `LiveExecutor`/`PaperExecutor` solo en `trading/bootstrap/composition_root.py`.
- Dominio/application de market_data no importa infra (BC-03/05/09 + guard AST).
- `shared/` sin imports hacia ocm/packages/apps (BC-01, BC-33..48).
- Wire schemas Kafka solo en `shared/kafka/schemas/`; producer/consumer solo en
  market_data (BC-35, BC-29). trading/portfolio/apps no usan Kafka.
- ocm sin dependencias de BCs (BC-14); market_data no importa trading/portfolio
  (BC-10); portfolio no importa trading (BC-13).

---

## Acciones tomadas (2026-08-03)

- Hallazgo 1: eliminado `packages/market_data/infrastructure/bootstrap/container.py`
  + limpieza de BC-07 (ignore_imports y comentario del OCMContainer).
- Hallazgo 6: AGENTS.md corregido (44 contratos; research sí es paquete).

## Deuda pendiente (decisión posterior)

- Contrato `layers`/DIP de dominio para trading (hallazgo 2).
- Unificación de ownership Redis (hallazgo 3).
- GoldReader en research (hallazgo 4).
- Tercer punto de ensamblado en `main.py` (hallazgo 5).
