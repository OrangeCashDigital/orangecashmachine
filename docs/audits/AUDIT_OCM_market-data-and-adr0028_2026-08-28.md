# AUDITORÍA TÉCNICA — ESTADO REAL DE MARKET DATA + PROPUESTA ADR-0028

- **Fecha:** 2026-08-28
- **Modo:** READ-ONLY. Cero modificaciones a código/producto/producción/Bybit/ADR-0028/sistema.
- **Autor:** Lead Engineer + System Architect + Trading-System Auditor (OCM)
- **Objetivo:** Determinar si OCM tiene un Market Data production-grade; si no, qué falta exactamente; y auditar la propuesta ADR-0028 (BookBuilder) contra la evidencia antes de cualquier cambio irreversible.

## A. Regla de evidencia aplicada

`EVIDENCIA > FUENTES PRIMARIAS > PAPERS/LIBROS > ADR > CÓDIGO > TESTS > DOCUMENTACIÓN > OPINIÓN DEL AGENTE`

Cada conclusión importante indica su fuente. No se presentan inferencias del agente como hechos.

---

## 1. Executive Summary

**OCM NO tiene un Market Data production-grade.** El data-plane está **roto y tiene datos STALE de ~7 días**; el servicio systemd está **FAILED** por configuración; **no existe BookBuilder** (solo una constante reservada); la observabilidad **no está operacional** (contenedores `Created`, no running). La infraestructura (Kafka, Redis, procesos) está viva, pero **un proceso vivo ≠ data-plane válido**.

Respuesta a la pregunta central: **NO.** Y a continuación, qué falta exactamente (ver §22).

Puntos críticos confirmados con evidencia operacional primaria:
1. `orderbook.raw` tiene ~23M mensajes pero **el último es de 2026-08-21T16:56Z** (hoy 2026-08-28) → **STALE 7 días**.
2. `book.snapshot` y `book.delta` (tópicos de salida del BookBuilder) tienen **offsets 0** → **BookBuilder nunca produjo**.
3. `trades.raw` está **vacío (0 mensajes)** → el feed de trades de Bybit no entrega datos.
4. `ocm-streaming.service` **FAILED** (`Result=exit-code`, `ExecMainStatus=1`, `NRestarts=3`), root cause en `journalctl`: `Value error, At least one exchange must be enabled` en `_l4_validate` del pipeline de config → **clasificación: configuración**.
5. Observabilidad: Prometheus, Grafana, Alertmanager, Loki, Promtail en estado **`Created` (no running)**; solo pushgateway/kafka/zookeeper/redis Up. → configuración versionada ≠ observabilidad operacional.

Trading permanece **BLOQUEADO** (sin ruta de ejecución; `LiveExecutor.IS_STUB=True` por ADR-0016). No se introdujo ningún cambio.

---

## 2. Estado real de Git

**Fuente: RUNTIME (comandos git)**

| Ítem | Estado |
|---|---|
| Branch actual (HEAD local) | `dependabot/pip/structlog-gte-24.1.0-and-lt-27.0` (b dependabot, NO la rama de trabajo B-56) |
| HEAD commit | `c998328da` (estructurado: rama dependabot) |
| origin/main | `44034ea` (`docs(tracking): close B-51 — deprecate tracking.yaml rules (Option 3) (#27)`) |
| Divergencia | HEAD local está en rama dependabot, divergente de main |
| Working tree | `M uv.lock`, `?? deploy/systemd/rendered/`, `?? docs/audits/AUDIT_OCM_data-plane-streaming_2026-08-28.md`, `?? docs/audits/OCM_AUDIT_FINDINGS_2026-08-28_data-plane-streaming.yaml.md`, `?? docs/proposals/` |
| PRs B-50..B-60 | Ver tabla §2.1 |

**Hallazgo clave:** El estado Git es **volátil**. El HEAD local NO está en una rama de feature B-* ni en main: está en `dependabot/pip/structlog-...`. El `?? docs/audits/AUDIT_OCM_data-plane-streaming_2026-08-28.md` (sin trackear) es de una sesión previa y NO está en main ni en rama alguna → **la auditoría operacional previa de data-plane no está versionada ni integrada**.

Además, la plantilla `ocm-streaming.service.template` existe en `origin/main:deploy/systemd/templates/` **pero está AUSENTE en el working tree (ramas dependabot/actual)** — solo queda `ocm-market-data.service.template`. Esto es una divergencia de contenido: el branch actual ha eliminado/renombrado plantillas que sí están en main. Confirmado por `git show origin/main:deploy/systemd/templates/` (2 archivos) vs `deploy/systemd/templates/` en working tree (1 archivo). **El contenido del árbol de trabajo (dependabot) no refleja main.**

### 2.1 PRs B-50 → B-60 (fuente: gh + git rev-list)

| Backlog | PR | Branch | Estado |
|---|---|---|---|
| B-50 | #26 close vuln tracking | feat/b50-close-vuln-tracking | **OPEN** (1 commit ahead) |
| B-51 | #27 close B-51 | feat/b51-deprecate-tracking-rules | **MERGED** (en main `44034ea`) |
| B-52 | #28 governance ADR-0032 | feat/b52-governance-controls | OPEN |
| B-53 | #29 semgrep non-blocking | feat/b53-semgrep-nonblocking | OPEN |
| B-54 | #30 ruff C901/PLR/SIM+vulture | feat/b54-maintainability-strategy | OPEN |
| B-55 | #31 AST Guards policy | feat/b55-ast-guards-formalization | OPEN |
| B-56 | #32 POLICY GATE CI | feat/b56-ci-stage-ordering | OPEN (8 commits ahead) |
| B-58 | observabilidad/provisioning | (ADR-0038, no PR integrado operacional) | config present, **no ejecutándose** |
| B-59 | systemd/operación streaming | feat/deploy-systemd-adr0022 (#20) | **OPEN** — infraestructura, no operacional |
| B-60 | — | — | no confirmado integrado |

**Conclusión Git:** La mayoría de B-50..B-60 están en ramas/PRs **OPEN**, no en main. Lo único integrado es B-51 (y B-22 master-plan, B-19 auditoría, B-18 políticas). **No asumir que el trabajo está "hecho" porque existen PRs** — están sin merge.

---

## 3. Estado real de systemd

**Fuente: RUNTIME (systemctl) + CÓDIGO + main template**

### 3.1 Unit instalado (`/etc/systemd/system/ocm-streaming.service`)

```
User=orangemusic
WorkingDirectory=/home/orangemusic/trading/orangecashmachine
Environment=PYTHONUNBUFFERED=1
Environment=OCM_ENV=production
Environment=KAFKA_BOOTSTRAP_SERVERS=localhost:9093
EnvironmentFile=/home/orangemusic/trading/orangecashmachine/.env
ExecStart=/home/orangemusic/trading/orangecashmachine/.venv/bin/streaming --env production
Restart=on-failure / RestartSec=10
StartLimitIntervalSec=60 / StartLimitBurst=3 / StartLimitAction=none
KillSignal=SIGTERM / TimeoutStopSec=30
Type=simple / After=network-online.target, Wants=network-online.target
No SafeOps (NoNewPrivileges/ProtectSystem/PrivateTmp AUSENTES)
```

### 3.2 Template versionado (main) — `deploy/systemd/templates/ocm-streaming.service.template`

```
ExecStart=.../.venv/bin/python -m app.cli.streaming_hydra
EnvironmentFile=deploy/host.env
KillSignal=SIGINT
After=network-online.target docker.service
SafeOps: NoNewPrivileges=true, ProtectSystem=strict, ReadWritePaths=..., PrivateTmp=true
```

**Divergencia unit instalado vs template main (CÓDIGO):**

| Atributo | Instalado | Template main | Impacto |
|---|---|---|---|
| ExecStart | `.venv/bin/streaming` (entrypoint) | `.venv/bin/python -m app.cli.streaming_hydra` | Distinto entrypoint |
| EnvironmentFile | `/.env` | `deploy/host.env` | Variables distintas |
| KillSignal | SIGTERM | SIGINT | Graceful shutdown distinto |
| After | network-online | network-online + docker | No espera Kafka/docker |
| SafeOps | AUSENTE | Presente | Endurecimiento ausente en prod |
| Limit storm | 3/60s + none | (template no lo declara) | Parada tras 3 intentos |

Además, en el working tree actual (dependabot) **la plantilla `ocm-streaming.service.template` NO existe** — el archivo versionado que se audita difiere según se lea main o el working tree. El artefacto `deploy/systemd/rendered/ocm-streaming.service` (untracked, del working tree) coincide con el template de main. **El árbol de trabajo no refleja main; hay discrepancia de 3 vías: versionado/main vs working-tree vs unit instalado.**

### 3.3 ¿Quién ejecuta systemd?

- **Usuario:** `orangemusic` (no root, no servicio dedicado).
- **WorkingDirectory:** `/home/orangemusic/trading/orangecashmachine`.
- **ExecStart:** `.venv/bin/streaming --env production` (entrypoint `streaming = app.cli.streaming_hydra:main`).
- **Environment:** carga `.env` (incluye `BYBIT_API_KEY/SECRET`).
- **Config:** `OCM_ENV=production` → cascade Hydra base→env=production→...→CLI.

### 3.4 ¿Qué pasa tras reiniciar?

`Restart=on-failure`: tras fallo vuelve a intentar con `RestartSec=10`, **máx. 3 reinicios en 60s**, tras lo cual `StartLimitAction=none` lo deja en `failed` (intervención manual). En cada intento falla igual (mismo error de config) → estado `failed` persistente.

---

## 4. Estado real del servicio

**Fuente: RUNTIME (journalctl)**

`systemctl status ocm-streaming.service` → **`failed`** (`ActiveState=failed`, `SubState=failed`, `Result=exit-code`, `ExecMainStatus=1`, `NRestarts=3`).

### 4.1 Causa raíz (journal)

```
pydantic_core.ValidationError: 1 validation error for AppConfig
  Value error, At least one exchange must be enabled.
  ...
File ".../apps/app/cli/streaming_hydra.py", line 134, in _load_config -> load_appconfig_standalone
File ".../ocm/config/hydra_loader.py", line 271 -> hydra_cfg_to_appconfig
File ".../ocm/config/pipeline.py", line 273, in _l4_validate -> raise ConfigPipelineError(ConfigStage.VALIDATED)
```

**Evidencia de fondo (config)**
- `config/exchanges/bybit.yaml`, `kucoin.yaml`, `kucoinfutures.yaml` → `enabled: false` (todos).
- `config/env/production.yaml` → **sin sección `exchanges:`**.

**Clasificación de causa raíz: CONFIGURACIÓN** (validación AppConfig: ningún exchange habilitado). NO es bug de código, credenciales, red, Kafka, exchange, systemd ni entorno: el pipeline de config rechaza arrancar sin un exchange `enabled: true`.

> **Nota (decisión no automática):** Habilitar un exchange en `production.yaml` es una **decisión de producción** (D-1: Bybit market-data public only, sin permisos). No se corrige automáticamente. Se reporta como **DECISIÓN HUMANA REQUERIDA** (§18.D1).

---

## 5. Estado de Kafka

**Fuente: RUNTIME (docker + kafka-cli)**

- Brokers: `ocm_kafka` (`confluentinc/cp-kafka:7.6.1`) **Up 2 days (healthy)**; `ocm_zookeeper` Up healthy; `ocm_redis` Up healthy.
- Listener: INTERNAL :9092, EXTERNAL localhost:9093.
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"` (productivo).

### 5.1 Tópicos y offsets (read-only)

| Topic | offset P0/P1/P2 | Interpretación |
|---|---|---|
| **orderbook.raw** | 23025555 / 0 / 2025869 | **Datos presentes (~23M)** |
| **book.snapshot** | 0 / 0 / 0 | **VACÍO — BookBuilder nunca emitió** |
| **book.delta** | 0 / 0 / 0 | **VACÍO — BookBuilder nunca emitió** |
| **trades.raw** | 0 / 0 / 0 | **VACÍO — no llegan trades** |
| ohlcv.raw | 3568 / 2652 / 2989 | Datos presentes |
| ocm.dlq | presente | canal de corruptos/fallidos |

### 5.2 Frescura real

- Mensaje más reciente de `orderbook.raw` (partición 0): **`CreateTime 2026-08-21T16:56:04Z`**, payload `occurred_at 2026-08-21`, `timestamp_ms 2026-08-20T21:32:39Z`. Hoy **2026-08-28** → **STALE ~7 días**. La partición 2 no devolvió mensajes frescos al sondear offset -1 con timeout.
- **Conclusión:** los offsets existen pero **NO avanzan en tiempo real**. Existe un topic → no significa data-plane funcional.

### 5.3 Consumer groups

Listados `dlq-*`, `replay-*` existentes. **NO aparece `ocm-book-builder`** porque `GROUP_BOOK_BUILDER="ocm-book-builder"` es una **constante reservada sin uso** (`shared/kafka/topics.py:179`) — no hay consumidor registrado.

---

## 6. Estado de Bybit

**Fuente: CÓDIGO + DOCUMENTACIÓN OFICIAL**

- **Configurado:** `config/exchanges/bybit.yaml` → `enabled: false`. (No habilitado; D-1 pendiente.)
- **Adapter público (reads):** `bybit_cryptofeed_runner.py` usa `cryptofeed.exchanges.Bybit` con `channels=[TRADES]`, `symbols=["BTC-USDT-PERP"]` (default). Es **Anti-Corruption Layer** de cryptofeed para Bybit.
- Protocolo WS real Bybit: `wss://stream.bybit.com/v5/public/linear`, tópicos `orderbook.{depth}.{symbol}` y `publicTrade.{symbol}`; **público SIN autenticación** (no requiere API key/secret) — DOCUMENTACIÓN OFICIAL.
- **Credenciales:** el unit carga `.env` con `BYBIT_API_KEY/SECRET`, **pero el feed público de market-data NO las necesita**. Para PUBLIC MARKET DATA ONLY no deben reutilizarse credenciales de trading. → ver §18.D1.

**Nota de seguridad:** No imprimir secretos. No modificar `.env`. No tocar permisos de API sin autorización. (El unit ya carga `.env` con `EnvironmentFile`, pero eso no habilita trading.)

---

## 7. Flujo completo de Market Data (estado real explorado)

```
Bybit WS (public) ──► cryptofeed ──► BybitCryptofeedRunner ──► NormalizedTrade / OnDelta hooks
     │                    │  Anti-Corruption Layer (bybit_cryptofeed_runner.py)
     │                    ▼
     │              orderbook_producer.py  (cryptofeed_orderbook_stream.py)
     │                 ├─ on_snapshot → OrderBookSnapshotPayload → orderbook.raw  ✅ (lo escribe)
     │                 └─ on_delta    → OrderBookDeltaPayload   → orderbook.raw  ✅ (lo escribe, 1 nivel/msg)
     ▼                    ▼
     Kafka orderbook.raw (23M msgs, STALE desde 2026-08-21)   │
                                                               ▼
        BookBuilder (consumidor)  == NO EXISTE (solo constante GROUP_BOOK_BUILDER reservada)
                                                               ▼
        validated order book  == NO EXISTE (book.snapshot/book.delta VACÍOS)
                                                               ▼
        consumidores  == NO EXISTEN en Fase 1
```

**Evidencia CÓDIGO:**
- `bybit_cryptofeed_runner.py` → `channels=[TRADES]` (no suscribe orderbook) — por eso `trades.raw` está vacío si el runner actual es el de trades.
- `cryptofeed_orderbook_stream.py:208-211` aplanamiento de deltas: `for side_key... for price, size in delta.get(side_key, [])` → **cada nivel = un mensaje Kafka** (v1).
- `orderbook_producer.on_delta` serializa y publica **un `OrderBookDeltaPayload` por $(side,price,size)$** → no preserva la unidad multinivel del mensaje Bybit.
- `shared/kafka/topics.py:179` define `GROUP_BOOK_BUILDER` **sin ningún uso** → símil de un libro de direcciones pero sin el consumidor.

**Qué existe realmente:** transporte WS + crawler cryptofeed + productor Kafka (`orderbook.raw`). **Qué NO existe:** BookBuilder, snapshot/delta validados, viewport, consumidores. **Qué aparenta existir pero está roto/stale:** `orderbook.raw` (stale), feed de trades (vacío).

---

## 8. Estado de orderbook.raw

**Fuente: RUNTIME (kafka-cli)**

- Tópico con ~23M mensajes repartidos en particiones 0 y 2 (partición 1 = 0 → skew de particionado posible).
- **STALE: último mensaje 2026-08-21T16:56Z, hoy 2026-08-28.**
- Mensajes de tipo `delta` con `payload_type: "delta"`, `exchange: "bybit"`, `symbol: "BTC-USDT-PERP"`, `side` por nivel, `price`/`size` como `str` (Decimal preservado en wire).
- **No se puede reconstruir un libro válido desde orderbook.raw v1** porque: (a) cada nivel es un mensaje separado sin agrupación por `u`/`seq`; (b) no hay `u`/`seq`/`cts` en el payload (solo `timestamp_ms`); (c) sin snapshot vs delta coherente garantizado por secuencia.

---

## 9. Estado de BookBuilder

**Fuente: CÓDIGO**

- `GROUP_BOOK_BUILDER` definido pero **0 usos** → **no implementado**.
- `ports/outbound/kafka_consumer.py` tiene `KafkaConsumerPort` con `poll/commit/seek_to_beginning/start/close`, pero **no existe `for_book_builder()`** en `infrastructure/kafka/consumer.py`.
- No existe dominio de BookState fuera del borrador/propuesta.
- `book.snapshot` y `book.delta` tópicos **VACÍOS** → el BookBuilder **nunca** ha corrido ni emitido.

**Conclusión:** BookBuilder es **estado futuro/disefio (ADR-0028 propuesta)**, NO código operativo.

---

## 10. Integridad de sequence/update IDs

**Fuente: DOCUMENTACIÓN OFICIAL + CÓDIGO**

- El wire v1 de OCM **noCarries** `u`/`seq`/`cts` de Bybit (solo `timestamp_ms`). → no es posible validar secuencia/continuidad en `orderbook.raw` actual.
- Bybit oficial: `u` = Update ID del libro (`u==1` ⇒ snapshot → overwrite); `seq` = Cross sequence (**global por exchange**, "smaller seq = generated earlier"); `cts` = correlación con `T` de trades. **La doc NO garantiza contigüidad `+1` de `u` ni `seq` por-símbolo.**
- **D-7b sigue siendo decisión EMPÍRICA.** No imponer `sequence==+1` sin evidencia (regla del encargo). `seq` cross-exchange ⇒ `seq==last+1` provoca falsos positivos.

---

## 11. Estado de checksum

**Fuente: DOCUMENTACIÓN OFICIAL**

- Bybit **NO tiene campo checksum** en orderbook WS (a diferencia de Binance). Confirmado.
- Para Bybit la integridad se valida por: `u`/`seq` monotónicos + snapshot-reset + invariantes estructurales (bids desc/asks asc/no-crossed). Igual que la propuesta.

---

## 12. Gap / recovery

**Fuente: CÓDIGO + DOCUMENTACIÓN OFICIAL + referencias**

- **GapAwareStream** existe (`adapters/inbound/websocket/gap_aware_stream.py`, `ports/inbound/trades_source.py`) → pertenece a **resiliencia/transporte del WS** (rest). **NO debe reutilizarse como BookBuilder** (confirmado como línea de diseño: BookBuilder consumidor Kafka independiente).
- Hay consumidores `replay-*` y `dlq-*` (parte del mecanismo de replay/corrección de OCM).
- Referencias profesionales (CBDC): Hummingbot usa **snapshot-restore + reject-stale + reapply-window**, NO gap estricto `+1`. Nautilus usa `sequence` para **ordenar/rechazar** y `clear`/`clear_stale_levels`. ⇒ el gap estricto no es el mecanismo primario en la industria; es **alerta → invalidar → snapshot restore**. (Coherente con la conclusión D-7b previamente emitida.)

**Recovery tras reconnect (oficial Bybit):** heartbeat ~20s; corte tras 10 min sin ping-pong/datos; "reconnect as soon as possible" + resubscribe; Bybit reenvía snapshot ⇒ BookBuilder overwrite (`u==1`, `is_ready`).

---

## 13. Observabilidad

**Fuente: RUNTIME (docker/systemctl)**

| Componente | docker status | systemctl | Interpretación |
|---|---|---|---|
| prometheus | **Created** | inactive | NO running |
| grafana | **Created** | inactive | NO running |
| alertmanager | **Created** | inactive | NO running |
| loki | (en compose) | inactive | NO running |
| promtail | (en compose) | inactive | NO running |
| pushgateway | **Up healthy** | — | running (único observ abst. activo) |
| kafka/zookeeper/redis | Up healthy | — | infra |

**Hallazgo clave:** La observabilidad está **configurada en `docker-compose.yml`** (servicios `prometheus/alertmanager/grafana/loki/promtail` definidos) pero **NO operacional** — los contenedores están en `Created` (nunca arrancados). Solo `pushgateway` corre. `streaming_hydra.py:148-150` envía métricas a Pushgateway **solo si `observability.metrics.enabled=true`** y con fallo-soft (Noop si no). Sin Prometheus scrapiando ni Grafana, no hay dashboards ni alertas activas. → **configuración versionada ≠ observabilidad operacional**. B-58 no está cumplido operativamente.

---

## 14. Comparación ADR ↔ código ↔ runtime

| ADR | Requisito | Código actual | Runtime | Estado |
|---|---|---|---|---|
| ADR-0013 | Ingestión unificada | adapters cryptofeed/ccxt | orderbook.raw con datos (stale) | Parcial (estale) |
| ADR-0014 | Market Data Platform + data_quality | quality/ presente | no verificado en pipeline activo | Parcial |
| ADR-0022 | Ciclo de vida streaming (supervisión process health) | `streaming_hydra.py` | servicio FAILED | **NO cumplido** |
| ADR-0023 | Deferral / gap detection | consumer replay/dedup + GapAwareStream | no BookBuilder | Parcial |
| ADR-0028 (propuesta) | BookBuilder como consumidor Kafka | **no implementado** | book.snapshot/delta vacíos | **NO** |
| ADR-0016 | Live executor stub | `IS_STUB=True` | trading OFF | Cumplido (aislamiento) |
| ADR-0038 | Grafana provisioning | compose definido | contenedor Created | **NO operacional** |

---

## 15. Comparación contra papers/libros/documentación formal

**Fuente: KB (manifest.yaml) + referencias en docs/**

- **Doc científico en repo:** `docs/knowledge/` referencia Lehalle & Laruelle (*Market Microstructure in Practice*, TIER_1), Buzzelli (*Data Quality Engineering*, TIER_1), Reis & Housley (*Fundamentals of Data Engineering*). **No hay PDFs legibles del contenido** (solo metadata/notes). → gap documental: las fuentes de autoridad TIER_1 no son verificables en esta sesión por ausencia de artefacto; el borrador ADR-0028 las cita como motivación, no como norma.
- **Referencias de implementación (zips)**: Hummingbot y Nautilus (con conectores Bybit reales) son la **evidencia de ingeniería más fuerte** y verificable. Validan: multinivel atómico (D-7a), snapshot-restore como recovery, rechazo de stale por monotonicidad, gaps como alerta (no camino único), uso de `u`/`seq` para ordenar.
- Precedencia aplicada: **código/de referencia real > ADR/draft > audit interna > opinión**. (La interpretación previa en la auditoría interna del patrón `first_update_id != last_diff_uid+1` quedó **refutada** por la lectura directa del código Hummingbot: `first_update_id` NO se fija ni se comprueba en la ruta Bybit.)

---

## 16. P0 Experimental propuesto (Bybit, PUBLIC MARKET DATA ONLY)

**NO ejecutar sobre producción sin autorización** (D-1/D-7b). Diseño reproducible, aislado, sin trading, sin credenciales de trading, evidencias persistentes, auditable.

- **Entorno:** script independiente (p. ej. bajo `scripts/` o notebook) que abre `wss://stream.bybit.com/v5/public/linear`, **sin auth**, suscribe `orderbook.50.BTCUSDT` + `publicTrade.BTCUSDT`.
- **Objetivos empíricos:**
  1. Semántica de `u`/`seq`/`cts` (tipos, valores, contigüidad real por-símbolo).
  2. Comportamiento de snapshot (`type=snapshot`, `u==1`? frecuencia).
  3. Comportamiento de deltas (multinivel en un mensaje; cuántos niveles por `u`).
  4. Posibilidad real de gaps (`u` salta? `seq` salta por cross-exchange?).
  5. Duplicados (¿mensajes con mismo `u`/`seq`?).
  6. Orden dentro de un mensaje y entre mensajes.
  7. Checksum (ausencia confirmada para Bybit).
  8. Comportamiento tras disconnect/reconnect (¿resnapshot automático?).
  9. Mecanismo correcto de resync (overwrite en `u==1` vs seq).
- **Métricas de salida persistidas:** directorio `data_platform/p0/<timestamp>/` con JSONL crudo, resumen CSV de gaps/duplicados/contigüidad, y un reporte markdown auditable.
- **Éxito P0 en D-7b:** decidir con datos si el gap se detecta por `u`, por `seq`, o por otro mecanismo; no se fija implementación sin esto.
- **Salvaguardas:** solo lectura de WS público; sin credenciales; sin ordenes; sin tocar balance/permisos; se detiene si detecta auth/private.

---

## 17. Riesgos

| ID | Riesgo | Severidad | Estado |
|---|---|---|---|
| R1 | **orderbook.raw stale (7d)** → data-plane no válido aunque el proceso esté vivo | ALTA | CONFIRMADO |
| R2 | `ocm-streaming.service` FAILED por config (ningún exchange enabled) | ALTA | CONFIRMADO |
| R3 | BookBuilder inexistente (solo constante) → sin libro validado | ALTA | CONFIRMADO |
| R4 | Feed de trades vacío (`trades.raw`=0) | MEDIA-Alta | CONFIRMADO |
| R5 | Observabilidad no operacional (Created) → sin scraping/alertas | MEDIA | CONFIRMADO |
| R6 | Divergencia 3-vías de unit systemd (instalado vs template main vs working-tree) | MEDIA | CONFIRMADO |
| R7 | Estado Git volátil: HEAD en rama dependabot, PRs B-50..B-60 sin merge, auditoría previa no versionada | MEDIA | CONFIRMADO |
| R8 | Gap estricto `+1` no garantizado por exchange → falsos positivos si se impone | ALTA (si se implementa mal) | CONFIRMADO (doc oficial) |
| R9 | Habilitar bybit sin confirmar que es PUBLIC MARKET DATA ONLY y sin permisos de trading | CRÍTICA | Controlado (bloqueado) |
| R10 | Confundir "infra viva" con "data-plane válido" | CRÍTICO (conceptual) | Gestionado por esta auditoría |

---

## 18. Decisiones humanas requeridas

### D-1 — Habilitar Bybit (PUBLIC MARKET DATA ONLY)
- **Qué:** habilitar `enabled: true` para bybit en config producción/directa, solo para market-data público.
- **Por qué:** es la decisión de producción que desbloquearía el streaming (causa raíz R2). Requiere confirmar que el adapter usa solo el WS público sin auth y sin permisos de trading.
- **Opciones:** (a) habilitar bybit con símbolo `BTC-USDT-PERP`, channels market-data; (b) no habilitar y corregir config para que el servicio no falle (p. ej. dejar claro el estado); (c) usar otro exchange.
- **Recomendación:** habilitar solo PUBLIC MD con símbolos y depth L50, sin credenciales de trading, validando primero con P0. **Evidencia:** protocols públicos sin auth (Bybit), but D-1 no ejecutado sin autorización.
- **Riesgo:** si se habilitara trading por error → crítico; por eso se exige modo público + sin permisos.

### D-2 — Adoptar el modo D-7b (modelo de gap) — requiere P0
- **Qué:** decidir si el gap se detecta por `u`, por `seq`, o verificando contigüidad real.
- **Por qué:** doc oficial no garantiza `+1`; `seq` es cross-exchange.
- **Recomendación:** ejecutar P0 antes de fijar; no fijar `+1` como único mecanismo. El gap debe: alertar → invalidar estado → snapshot restore/resync.

### D-3 — Determinar el alcance de la Fase 1 del BookBuilder (D-7d)
- **Qué:** MUST (schema v2 multinivel + productor seq + BookBuilder + is_ready + metrics + BC + tests) vs SHOULD (viewport/replay) vs FUTURE (cts↔T).
- **Por qué:** definir qué entrega el primer release de MD sin trading.

### D-4 — Modelo de precisión (D-7c)
- **Qué:** fijar Decimal vs float en BookState/mid-spread. Recomendación: Decimal en wire/invariantes/BookState; float solo a escala entera (tick-size) si rendimiento lo exige.

### D-5 — Aprobar la propuesta ADR-0028 con las correcciones D-7a (multinivel atómico = A) y D-7b (gap = empírico, no estricto `+1`)
- **Qué:** dar por buena la dirección del diseño (consumidor Kafka independiente de GapAwareStream) y corregir los dos puntos señalados.

### D-6 — Definir qué se entrega como "Market Data READY" según la DoD (§20)
- **Qué:** aceptar formalmente la Definition of Done del §20 como compuerta de salida.

---

## 19. Recomendación de arquitectura

1. **BookBuilder es consumidor Kafka independiente** — NO reutilizar/n compartir con GapAwareStream (ya decidido: gap_aware_stream es resiliencia de transporte WS). Se mantiene la decisión arquitectónica.
2. **Delta multinivel atómico (schema v2)** con `u`/`seq`/`cts`: cada mensaje Bybit = una unidad (props: Hummingbot/Nautilus/protocolo). Corrige el defecto v1 de aplanar un nivel/mensaje.
3. **Modelo de integridad (D-7b tras P0):** monotonic (reject-stale) + buffering pre-snapshot + **snapshot-restore** como recovery; gap estricto solo como alerta/salida de resincronización, **no** como único mecanismo.
4. **Recovery/reconnect (oficial Bybit):** reconectar + resubscribe + snapshot fresco → overwrite (`u==1`, `is_ready=True`).
5. **Checksum:** genérico por-exchange; Bybit no lo tiene → validar con invariantes estructurales + seq/u.
6. **Decimal** en wire/invariantes/BookState; conversión solo en frontera de cómputo de alta frecuencia si se justifica por rendimiento (escala a enteros).
7. **Seguridad:** BookBuilder y viewport pertenecen a `market_data`; port outbound sin dependencia de trading/oms/execution; compuerta `is_ready=False` fail-closed. Trading permanece bloqueado (`LiveExecutor` stub).
8. **Fase 0 obligatoria:** antes de fijar D-7b, ejecutar P0 (§16). Antes de habilitar Bybit en prod, confirmar PUBLIC MARKET DATA ONLY + D-1.

---

## 20. Definition of Done — Market Data READY

No declarar READY hasta demostrar con **evidencia operacional reproducible**:

1. Bybit market-data funciona (WS público, sin trading). *(hoy NO — bybit disabled)*
2. WebSocket recibe datos reales y frescos. *(hoy NO verificable — service FAILED; orderbook.raw stale)*
3. Kafka recibe datos. *(parcial: orderbook.raw tiene msgs, pero stale)*
4. orderbook.raw recibe datos **frescos** (offsets avanzando, timestamps recientes). *(NO — stale 7d)*
5. BookBuilder procesa correctamente. *(NO — inexistente)*
6. Snapshot + delta producen **estado consistente** (is_ready valid). *(NO)*
7. Sequence/gap semantics **demostradas** (P0). *(NO — sin P0)*
8. Checksum validado cuando corresponda (Bybit: invariantes + seq/u). *(Parcial/NA)*
9. Recovery/resync funciona. *(NO)*
10. Datos stale son detectados (métricas/flags). *(NO)*
11. Métricas existen. *(parcial: pushgateway)*
12. Prometheus scrapea correctamente. *(NO — container Created)*
13. Alertas críticas existen. *(NO)*
14. systemd reinicia correctamente (ACTIVE sostenido). *(NO — FAILED)*
15. Logs suficientes para auditoría. *(parcial)*
16. Todo reproducible. *(NO — procesos manuales, estado volátil)*
17. Trading permanece **BLOQUEADO**. *(SÍ — cumplido)*

**Veredicto DoD:** **NO READY.** Solo el ítem 17 se cumple; los demás están pendientes o negados.

---

## 21. Evidencia / comandos utilizados

```
git rev-parse --abbrev-ref HEAD / HEAD / origin/main
git log --oneline origin/main..<feat/b5x>        # diverge PRs
git show origin/main:deploy/systemd/templates/   # plantillas en main (2)
ls deploy/systemd/templates/                     # working tree (1)
cat /etc/systemd/system/ocm-streaming.service    # unit instalado
cat deploy/systemd/rendered/ocm-streaming.service
systemctl {is-enabled,is-active,status,show,list-dependencies} ocm-streaming.service
journalctl -u ocm-streaming.service -n 25        # root cause ValidationError
grep -n enabled config/exchanges/*.yaml ; grep -n exchanges config/env/production.yaml
docker ps -a --format '{{.Names}}\t{{.Status}}'  # observabilidad Created
docker exec ocm_kafka kafka-topics --list
docker exec ocm_kafka kafka-get-offsets --topic {orderbook.raw,trades.raw,ohlcv.raw,book.snapshot,book.delta}
docker exec ocm_kafka kafka-console-consumer --topic orderbook.raw --partition 0 --from-beginning --max-messages 1 --property print.timestamp=true
ps aux | grep market_data.main                  # pid 1051 vivo
tr '\0' ' ' < /proc/1051/cmdline; readlink /proc/1051/cwd
python3 -c "... datetime.fromtimestamp(ms/1000) ..."  # 1787331364273 → 2026-08-21T16:56Z
rg -n GROUP_BOOK_BUILDER packages shared apps
sed -n '179,225p' orderbook_producer.py; sed -n '208,211p' cryptofeed_orderbook_stream.py
gh pr list --state all --limit 40
```

**Fuentes por conclusión:** §2-6 RUNTIME/CÓDIGO; §5-8 RUNTIME (kafka-cli); §10-12 DOCUMENTACIÓN OFICIAL + CÓDIGO + referencias; §13 RUNTIME; §14-15 ADR + CÓDIGO + referencias.

---

## 22. Respuesta directa: ¿OCM tiene Market Data production-grade?

**NO.**

**Qué falta exactamente (lo que impide production-grade):**

1. **Byte degeña:** Bybit no habilitado (PUBLIC MARKET DATA ONLY) → `ocm-streaming.service` FAILED por config.
2. **Frescura:** el data-plane está STALE (orderbook.raw detenido 2026-08-21). Falta que fluya y que se verifique frescura/offsets avanzando.
3. **BookBuilder:** no existe; sin él no hay libro validado ni snapshot/delta (tópicos vacíos).
4. **Schema v2:** el wire v1 aplanado (1 nivel/msg) impide reconstrucción atómica y correlación `u`/`seq`/`cts`.
5. **Integridad/gap:** sin P0 no se puede fijar D-7b (gap estricto `+1` NO está garantizado por exchange).
6. **Stale-detection:** sin BookBuilder ni metrics no se detecta que los datos llevan 7 días congelados.
7. **Observabilidad operacional:** Prometheus/Grafana/Alertmanager/Loki en `Created` (no running); B-58 no cumplido.
8. **systemd operativo:** servicio FAILED; además divergencia 3-vías del unit (instalado vs template main vs working-tree).
9. **Estado Git/reproducibilidad:** HEAD en rama dependabot; B-50..B-60 en PRs OPEN; auditorías previas sin versionar.
10. **Feed de trades:** `trades.raw` vacío.

Lo que SÍ existe y está sano: infraestructura (Kafka, Redis, ZK), `market_data.main` proceso vivo, productor `orderbook.raw` (aunque stale), aislamiento de trading (LiveExecutor stub), y una **propuesta de diseño sólida** (ADR-0028) que apunta a resolver 2-5 con condicionantes (D-7a/D-7b).

**Trading permanece BLOQUEADO.** No se avanzó a trading ni se habilitó exchange.

---

## 23. Nota de cierre / próxima acción

Detenido por **REGLA DE PARADA** antes de cualquier cambio irreversible. Siguientes pasos (todos requieren decisión humana/ejecución autorizada):
1. **D-1** decisión: habilitar Bybit PUBLIC MD ONLY (nuevo env instrucciones).
2. **P0 experimental** (§16) para fijar D-7b.
3. **Correcciones de propuesta ADR-0028** sugeridas pero NO aplicadas (D-7a multinivel atómico, y D-7b no-estricto-`+1` / snapshot-restore).
4. Estabilizar **estado Git** (HEAD a main/rama real, integrar PRs pendientes) — decisión de orden de trabajo.

Trading sigue bloqueado hasta cumplir formalmente la DoD (§20).
