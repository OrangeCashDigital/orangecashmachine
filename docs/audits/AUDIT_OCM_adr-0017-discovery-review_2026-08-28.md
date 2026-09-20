# AUDITORÍA ADR-0017 — DISCOVERY (antes de ADR-0028)

- **Fecha:** 2026-08-28 · **Modo:** READ-ONLY (sin código/producción/merge)
- **Objetivo:** Determinar exactamente qué significa "Discovery" en la arquitectura formal de OCM
  y si la implementación actual cumple ADR-0017. **No implementar todavía.**
- **Fuentes:** ADR-0017 (full), `MarketDataSource` port + adapters (Bybit/KuCoin), `feed_registry`,
  `composition_root`, `ccxt_adapter`, `ws_trades_source`, `provenance.py`, `tracking.yaml`,
  `PLAN-Maestro-Ingenieria.md`, P0 Bybit.

---

## 1. ADR-0017 — Extracción (propósito, scope, contrato)

**Propósito:** Elevar la procedencia implícita de schemas a una **metodología única, permanente y
multi-fuente** sobre cómo se **descubre, valida y modela cualquier protocolo externo** (exchanges,
blockchains, brokers) antes de incorporarlo al dominio. Corrige el contexto donde "la mayoría de los
schemas eran ASSUMED; solo el orderbook venía de un protocolo observado".

**SCOPE / responsabilidades (decisión):**
- **Protocol Discovery Framework (PDF)** — metodología (14 componentes), **nunca cambia**.
- **Discovery Profiles** — implementación concreta **por integración** (Bybit = primer profile); **lo
  que cambia**. Cada profile declara: *fuentes de evidencia, validación, criterios de promoción,
  limitaciones*.

**Componentes del PDF (en orden):**
1. Objetivo · 2. Principios · 3. Tipos de evidencia (PROTOCOL/DOCUMENTATION/UPSTREAM_LIBRARY/DOMAIN/ASSUMED)
4. **REST Discovery** · 5. **WebSocket Discovery** · 6. Execution Discovery · 7. Funding Discovery ·
8. Liquidation Discovery · 9. **Contract Provenance** · 10. Normalización · 11. Validación ·
12. Fixtures · 13. Tests · 14. **Promotion Rule**.

**Principios explícitos:**
- Evidencia > suposición; **linaje obligatorio**; no-SSOT hasta validar; **el dominio nunca depende
  directamente del protocolo externo** — el protocolo se descubre, valida y luego se *proyecta* al dominio.
- **Promotion Rule (14):** un contrato se promueve a **SSOT** solo si su provenance es estable
  (PROTOCOL/DOCUMENTATION/UPSTREAM_LIBRARY/DOMAIN) y pasa validación; **ASSUMED permanece provisional**.
  Aplica a **todo contrato crítico para capital** (balances, posiciones, funding, liquidaciones,
  orderbook, trades, metamodelos).

**Contratos BC-NN:** `BC-29` (schemas wire en `shared.kafka`); `BC-09`/guards (dominio framework-agnostic;
PDF vive en adapters/infra); `tests/kafka/test_schema_provenance.py` (guard de linaje).

**Actores/interfaces/entradas/salidas:** no define una API formal de Discovery en el código; define una
**metodología** más el **contrato de profile** (evidencia + validación + promoción + limitaciones).
"No es un kit de scripts: es una metodología." **No inventar más allá de lo escrito.**

**Criterios de aceptación / estado:** F2.5 (Plan Maestro:177-188) — "ADR-0017 aceptada y committeada";
**gate normativo antes de capital** (F3); backlog en F4. tracking.yaml `prerequisito_f3_discovery`: estado
**activo (F2.5)**, "institucionalizacion: PDF + Discovery Profiles (Bybit = primero)"; "cambian los
Discovery Profiles, nunca [el framework]".

**Relación con MarketDataSource / exchanges / instrumentos / channels / capabilities / config / runtime:**
- ADR-0017 **no** referencia `MarketDataSource` por nombre.
- El framework habla de "descubrir" mensajes/endpoints **por exchange y por contrato** (REST/WS/execution/funding/liquidation) y de "capabilities" **implícitas** en cada profile (qué mensajes provee la fuente).
- Instrumentos/channels se descubren **por profile** (componente 4-5) — no especifica que un port deba exponerlos.

---

## 2. Implementación actual (auditada)

### `MarketDataSource` port (`ports/inbound/market_data_source.py`)
- Protocol (structural) con **3 métodos**: `subscribe_trades(symbols, callback)`, `start()`, `stop()`.
- Atributo `exchange: str`.
- **NO expone** `instruments`, `symbols` (getter), `channels`, `capabilities`, `discovery`,
  `handshake`, `profile`, ni ningún método de discovery. **No hay API de discovery.**

### `BybitFeedAdapter` / `KuCoinFeedAdapter` (implementadores)
- Implementan el port: `subscribe_trades` (registra símbolos **pasados por el caller**), `start`
  (delega al runner), `stop`.
- **Símbolos hardcoded/venidos de configuración**, NO descubiertos. `bybit_feed_adapter` usa
  `FeedRunnerProtocol.run_until_stopped(symbols=self._symbols, ...)`; los `symbols` los inyecta el
  Composition Root desde config.
- **No hay** handshake de capabilities, ni listado de instrumentos, ni negociación de channels.

### Cadena de símbolos (config → adapter)
- `streaming_hydra.py:86-97`: CLI `--symbols` con default `config.feeds.feeds.<exchange>.symbols` (SSOT);
  `--exchange` default `bybit`.
- `composition_root.py:193,297`: construye adapters con `entry.symbols` / `cfg.symbols` desde config.
- **→ Los instrumentos son configuración estática, no descubrimiento.**

### `feed_registry.py` (infrastructure/bootstrap)
- SSOT **hardcoded** `_ADAPTER_CLASSES = {"bybit": ..., "kucoin": ...}` → mapa exchange→clase. Lazy import.
- No descubre adapters dinámicamente; es un registro estático de wiring.

### `ccxt_adapter.py` (outbound)
- Tiene `load_markets()` (374-377) = **discovery de instrumentos a nivel librería CCXT**, usado
  internamente para OHLCV. **No está expuesto** por `MarketDataSource` ni abstraído como port OCM de discovery.

### `ws_trades_source.py`
- **STUB** (línea 85: "TODO: implementar conexión WS real"). El "WS Discovery de trades" que ADR-0017
  referencia no está operativo.

### `provenance.py` (Contract Provenance, componente 9)
- **Implementado:** taxonomía PROTOCOL/DOCUMENTATION/UPSTREAM_LIBRARY/DOMAIN/ASSUMED; `_PROMOTED_STATES`;
  `is_promoted()`, `require_promoted()`.
- **Solo orderbook** (`OrderBookSnapshotPayload`, `OrderBookDeltaPayload`) es **PROTOCOL** (observado);
  `TradeSeriesPayload` y diversos son **ASSUMED/orphan**. → coherente con ADR-0017: solo el orderbook
  tiene procedencia de protocolo real observado.

### `tests/kafka/test_schema_provenance.py`
- Guard de linaje (semilla componente 13) implementado.

---

## 3. GAP ANALYSIS (matriz)

| Requisito ADR-0017 | Implementación | Evidencia | Estado |
|---|---|---|---|
| 1. Objetivo (metodología) | Documentado en ADR-0017 | adr file | PASS (metodología definida) |
| 2. Principios (evidencia>suposición, linaje, proyección) | Parcial: `provenance.py` linaje; dominio no importa protocolo (BC-09) | código + ADR-0017 | PARTIAL |
| 3. Tipos de evidencia | `provenance.py` taxonomía | código | PASS |
| 4. REST Discovery | Solo `load_markets()` CCXT interno; **no profile** | ccxt_adapter | FAIL/PARTIAL (no como perfil) |
| 5. WebSocket Discovery | **P0 ejecutado (evidence)**; operación manual; **no profile formal** | P0 evidence + p0_bybit | PARTIAL (evidencia sí; perfil no) |
| 6. Execution Discovery | — | — | NOT APPLICABLE aún (trading bloqueado) |
| 7. Funding Discovery | — | — | NOT APPLICABLE / UNKNOWN |
| 8. Liquidation Discovery | — | — | NOT APPLICABLE / UNKNOWN |
| 9. Contract Provenance | `provenance.py` (+orderbook PROTOCOL) | código | PASS |
| 10. Normalización | `shared/ports/outbound/normalization.py` | código | PASS (existe SSOT) |
| 11. Validación | config/schema + tests wire; no per-profile | código | PARTIAL |
| 12. Fixtures | **P0 evidence raw** (empieza); no fixtures congeladas de contrato | p0_bybit/evidence | PARTIAL |
| 13. Tests | `test_schema_provenance.py`, tests de wire | código+tests | PASS (parcial per-profile) |
| 14. Promotion Rule | `require_promoted()`; orderbook promovido; ASSUMED bloqueado | código | PASS |
| **Discovery Profiles (contrato core)** | **NO implementado** (sin módulo/dir `discovery`/`profile`) | búsqueda exhaustiva | **FAIL** |
| API de Discovery en port | No existe | MarketDataSource | NOT APPLICABLE (ADR no la exige) |
| WebSocket/trades source | **STUB** | ws_trades_source | FAIL (para trades WS) |

**No convertir UNKNOWN en FAIL sin evidencia:** las dimensiones Execution/Funding/Liquidation están
formalmente en backlog F4 y trading bloqueado → NOT APPLICABLE/UNKNOWN, no FAIL.

---

## 4. Relación P0 Bybit ↔ ADR-0017

```
BYBIT (public WS)
  → DISCOVERY  : ADR-0017 componente 5 (WebSocket Discovery) — OBSERVADO en P0 (evidence raw PROTOCOL)
  → ADAPTER    : BybitFeedAdapter (MarketDataSource port) — SOLO trades subscribe; orderbook NO via este port
  → MARKET DATA: orderbook.raw (wire v1 aplanado, sin u/seq/cts — DEFECTO vs protocolo)
  → BOOKBUILDER: consumidor Kafka (pendiente D-7, ADR-0028) — NO existe
```

Dónde existe cada contrato:
- **Discovery (metodología):** ADR-0017 (normativo). Evidencia de protocolo: P0 (fixtures raw candidatas).
- **Exchange Profile / Discovery Profile:** ADR-0017 lo EXIGE como contrato por integración; **no existe en código** (FAIL de implementación).
- **Adapter:** `feed_registry` (mapa estático) + `MarketDataSource` (3 métodos, sin discovery).
- **Schema v2 / BookBuilder:** ADR-0028 (propuesta, pendiente D-7).

---

## 5. Decisión arquitectónica (respuesta explícita)

**A. ¿ADR-0017 requiere una API formal de Discovery?**
**No textualmente.** ADR-0017 define una **metodología** (PDF) y un **contrato de Discovery Profile**
(evidencia + validación + promoción + limitaciones), no una API/port de código. "No es un kit de scripts."
→ La "API formal" NO es un requisito textual; el **Discovery Profile** SÍ es un requisito formal del framework.

**B. ¿MarketDataSource debería exponer Discovery?**
**No necesariamente derivado de ADR-0017.** ADR-0017 no referencia `MarketDataSource`. El discovery de
instrumentos/capabilities bien puede vivir en un **port/perfil separado** o como **onboarding** (config +
fixtures + evidencia), no obligatoriamente en el flujo de streaming. Decisión de diseño, no impuesta por ADR-0017.

**C. ¿Discovery runtime u onboarding/config?**
ADR-0017 es esencialmente **onboarding + validación + promoción (estática)**: se descubre/valida el
protocolo una vez, se fijan fixtures/tests y el contrato se promueve a SSOT. **No exige discovery runtime
dinámico** (p.ej. listar símbolos en vivo cada vez). El runtime usa config + contratos promovidos.

**D. ¿Debe existir un ExchangeProfile/DiscoveryProfile?**
**SÍ — es el requisito central de ADR-0017.** Es el "contrato de profile" (evidencia, validación,
promoción, limitaciones). Es lo que falta en código (FAIL del gap analysis).

**E. ¿Debe Discovery devolver capabilities/instruments/channels/protocol/seq/checksum?**
El framework espera que el **profile** describa: fuentes de evidencia (endpoints/channels), validación,
promoción y limitaciones. **A nivel de profile** (documental/artefacto) sí debe capturar esas semánticas
(instrumentos, channels, seq/checksum observados). **No** se exige una función runtime que las devuelva
dinámicamente.

**F. ¿Discovery pertenece al dominio o al adapter/infra?**
ADR-0017: BC-09 — "el PDF vive en adapters/infra"; el dominio **nunca depende directamente del protocolo**.
→ Discovery es **infrastructure/adapter + onbording**, proyectado luego al dominio **proyectado con
normalización/validación** (`shared/ports/outbound/normalization.py`). No es dominio.

**G. ¿Qué parte se persiste/versiona para auditoría?**
Fixtures (muestras del mensaje observado, componente 12), tests de linaje (13), provenance (9), y el
**Discovery Profile** (evidencia+validación+promoción+limitaciones). → Lo versionable: **fixtures raw +
tests + provenance + el profile documental**. (El P0 ya genera fixtures raw en `docs/audits/p0_bybit/evidence`.)

---

## 6. Relación con ADR-0028 (no se modifica ADR-0028)

Determinar si ADR-0017 impone requisitos que afecten a ADR-0028:

- **SÍ impone un requisito transversal:** ADR-0028 (BookBuilder sobre orderbook) opera sobre un contrato
  wire (schema v2) que, según ADR-0017, debe tener **provenance estable (PROTOCOL)** y **paso por el
  Discovery Profile de Bybit** (validación + promoción). El orderbook ya está `PROTOCOL` en provenance →
  base sólida; pero el **schema v2** (multinivel + `u/seq/cts`) es un contrato **nuevo** que debería
  registrarse/validarse con fixtures del P0 antes de promoverse a SSOT.
- **NO cambia la arquitectura prevista** de BookBuilder: la cadena
  `Discovery → Exchange Profile → Orderbook Adapter → Schema v2 → BookBuilder` **es coherente** con los
  ADR existentes (ADR-0017 PDF → profile; ADR-0013/0014 ingestión/normalización; ADR-0028 consumidor).
- **Impacto práctico en ADR-0028 (sin modificar el doc):** conviene que la implementación de schema v2
  añada **fixtures congeladas del P0** (componente 12) y **registro de provenance PROTOCOL** para el
  delta multinivel, de modo que el BookBuilder opere sobre contratos promovidos y auditables. Esto NO
  bloquea el diseño; refuerza la trazabilidad que ADR-0017 exige.

**¿La cadena es la prevista por los ADR?** Sí — ADR-0017 (descubrimiento+provenance) → ADR-0013/0014
(ingesta/normalización) → ADR-0028 (BookBuilder consumidor). No hay conflicto; hay un gap de
implementación (faltan los Discovery Profiles operativos) que es independiente del diseño de BookBuilder.

---

## 7. Decisiones humanas

Solo las reales (no mecánicas):

- **DH-1 (alcance Discovery):** ¿institucionalizar el **Discovery Profile de Bybit** como artefacto formal
  (documento/módulo con evidence+validación+promoción+limitaciones) como parte del onboarding del
  pipeline, cerrando el gap de los componentes 4-5 de ADR-0017? (decisión de arquitectura/gobernanza).
- **DH-2 (schema v2 + provenance):** al implementar ADR-0028, ¿registrar el **delta multinivel v2** como
  contrato PROTOCOL con fixtures del P0 (cumplir ADR-0017 componente 12/14) — o diferirlo? (recomendado: registrarlo).
- **DH-3 (onboarding vs runtime):** confirmar que el discovery de instrumentos/capabilities sea
  **onboarding/config + fixtures** (recomendado, coherente con ADR-0017) y no una API runtime dinámica.
- **DH-4 (Bybit en producción PUBLIC MD):** decisión de producción separada (D-1) pendiente.

No requiere decisión humana: el análisis indica que **no hay conflicto arquitectónico** entre ADR-0017 y
ADR-0028; solo falta implementar los Discovery Profiles (backlog F4) y registrar provenance/schema.

---

## 8. Veredicto

**B) ADR-0017 está definida pero parcialmente implementada.**

**Por qué:**
- **Implementado (PASS):** metodología documentada (ADR-0017), Contract Provenance (`provenance.py`),
  Promotion Rule (`require_promoted`), orderbook como `PROTOCOL` promovido, normalización SSOT, tests de
  linaje, y ahora **evidencia WebSocket real (P0)**.
- **Definido pero NO implementado (FAIL/PARTIAL):** los **Discovery Profiles** (contrato central del
  framework, componente por integración) **no existen en el repo**; los componentes operativos 4-8
  (REST/WS/Execution/Funding/Liquidation Discovery) no tienen perfil; `ws_trades_source` es STUB;
  instrumentos/channels vienen de configuración estática, no de discovery; no hay fixtures congeladas de
  contrato (aunque el P0 ya las engendra).
- **Coherente con el backlog:** tracking.yaml lo marca fase F2.5 activa y DOR F2.5 = "ADR-0017 aceptada"
  (cumplida) con institucionalización/profile pendiente en F4.

**Impacto sobre BookBuilder:** ADR-0017 **no bloquea** el diseño de ADR-0028 y **no impone una API formal**
de Discovery en `MarketDataSource`. Exige únicamente que el contrato wire (schema v2) tenga **provenance
estable + fixtures + validación** — algo que el P0 ya produce y que se debe cristalizar al implementar.
No cambiar la arquitectura de BookBuilder por esto.

---
*Archivo: `docs/audits/AUDIT_ADR-0017-DISCOVERY_2026-08-28.md`. Read-only; sin commits/PRs/merge/producción.
Trading permanece BLOQUEADO.*
