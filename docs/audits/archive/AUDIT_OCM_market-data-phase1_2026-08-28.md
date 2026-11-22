# AUDITORÍA FASE 1 — RECONSTRUCCIÓN DE ESTADO REAL DE MARKET DATA (OCM)

- **Fecha:** 2026-08-28 (sesión fresh)
- **Modo:** READ-ONLY. Cero modificaciones a código/producto/producción/Bybit/ADR-0028/sistema.
- **Motivo:** La directiva exige reconstruir el estado REAL del sistema (prioridad: evidencia operacional primaria > comportamiento de código > tests > config > ADR > doc > opinión). No confiar en tracking.yaml/Plan Maestro/ADRs/comentarios/informes previos.
- **Entregable:** diagnóstico completo (formato §18) + diseño de P0 experimental (no ejecutado).

---

## 1. ESTADO ACTUAL (reconstruido, evidencia primaria)

### A. Git (RUNTIME)

| Ítem | Valor |
|---|---|
| Branch actual (HEAD) | `dependabot/pip/redis-gte-7.4.0-and-lt-9.0` |
| HEAD commit | `3897be0` (`fix(yaml): añadir newline al final de deploy/monitoring/alerts.yml`) |
| origin/main | `44034ea` (`docs(tracking): close B-51 — ... (#27)`) |
| merge-base(HEAD,main) | `c392f8f` |
| staged | ninguno |
| working tree | `M uv.lock`; `?? deploy/systemd/rendered/`; `?? docs/audits/{AUDIT_OCM_data-plane-streaming_2026-08-28, AUDIT_OCM_market-data-and-adr0028_2026-08-28, OCM_AUDIT_FINDINGS_2026-08-28_data-plane-streaming.yaml}.md`; `?? docs/proposals/` |

**Hallazgo clave:** El HEAD local está en una **rama dependabot** (esta vez `redis-...`; la sesión previa estaba en `structlog-...`). **El estado Git es volátil** y NO está en main ni en una rama de feature B-*. origin/main = `44034ea` (B-51 cerrado por PR #27). Por lo tanto, la mayor parte de B-50..B-60 vive en **PRs OPEN**, no en main.

**B-50 → B-60 ¿qué está realmente en main?** (gh + git rev-list)
- B-51 → **MERGED** (#27, en main `44034ea`).
- B-22 master-plan (#22), B-19 audit-validator M22-M25 (#19), B-18 policy-registry (#18) → MERGED.
- B-50 (#26 OPEN), B-52 (#28 OPEN), B-53 (#29 OPEN), B-54 (#30 OPEN), B-55 (#31 OPEN), B-56 (#32 OPEN), B-59/ADR-0022 deploy-systemd (#20 OPEN).
- **Conclusión:** la mayoría de B-50..B-60 **no está integrado en main**. "HECHO en tracking" ≠ "integrado". No se mergea ni se decide integración en esta fase (decisión humana).

### B. systemd (RUNTIME + CÓDIGO)

- `ocm-streaming.service`: **FAILED** (`Result=exit-code`, `ExecMainStatus=1`, `NRestarts=3`, `ActiveState=failed`, `SubState=failed`).
- Unit instalado (`/etc/systemd/system/ocm-streaming.service`):
  - `Type=simple`, `User=orangemusic`, `WorkingDirectory=/home/orangemusic/trading/orangecashmachine`
  - `Environment=PYTHONUNBUFFERED=1`, `OCM_ENV=production`, `KAFKA_BOOTSTRAP_SERVERS=localhost:9093`
  - `EnvironmentFile=/home/orangemusic/trading/orangecashmachine/.env`
  - `ExecStart=/home/orangemusic/trading/orangecashmachine/.venv/bin/streaming --env production`
  - `Restart=on-failure` / `RestartSec=10` / `KillSignal=SIGTERM` / `StartLimitBurst=3`
- **Divergencia vs template `origin/main:deploy/systemd/templates/ocm-streaming.service.template`:** template usa `python -m app.cli.streaming_hydra`, `deploy/host.env`, `KillSignal=SIGINT`, `After=... docker.service`, `NoNewPrivileges`/`ProtectSystem=strict`/`PrivateTmp=true` (SafeOps) — el unit **instalado NO tiene SafeOps** y difiere en entrypoint/env. En el working tree actual (rama dependabot), la plantilla `ocm-streaming.service.template` **no está presente** (solo `ocm-market-data.service.template`).
- `list-dependencies` normal (network, mounts, sysinit).

### C. Procesos (RUNTIME)

- **`market_data.main` RUNNING** pid `1051` (`/home/orangemusic/trading/orangecashmachine/.venv/bin/python -m market_data.main`, user orangemusic, cwd=repo). Lleva ~2 días de uptime.
- `litellm-proxy.service` RUNNING (proxy LLM, ajeno a MD).
- **NO hay** proceso `streaming`, `BookBuilder`, ni consumidor `ocm-book-builder`.
- tmux sesión `orangecashmachine` (proceso interactivo).

### D. Docker (RUNTIME)

| Contenedor | Estado | Imagen |
|---|---|---|
| ocm_kafka | **Up (healthy)** | cp-kafka:7.6.1 |
| ocm_zookeeper | **Up (healthy)** | cp-zookeeper:7.6.1 |
| ocm_redis | **Up (healthy)** | redis:7.2-alpine |
| ocm_pushgateway | **Up (healthy)** | prom/pushgateway:v1.8.0 |
| ocm_prometheus | **Created** (NO running) | prometheus:v2.51.2 |
| ocm_grafana | **Created** (NO running) | grafana:10.4.2 |
| ocm_alertmanager | **Created** (NO running) | alertmanager:v0.27.0 |
| ocm_config_guard | Exited(0) 7d | busybox |

**Hallazgo:** Observabilidad definida en compose pero **NO operacional**. Solo pushgateway de la pila obs. está arriba. Prometheus/Grafana/Alertmanager en `Created`.

### E. Kafka (RUNTIME)

| Tópico | offset P0/P1/P2 | Lectura |
|---|---|---|
| orderbook.raw | 23025555/0/2025869 | Datos (~23M), **con skew** (P1=0) |
| trades.raw | 0/0/0 | **VACÍO** |
| book.snapshot | 0/0/0 | **VACÍO** (BookBuilder nunca produjo) |
| book.delta | 0/0/0 | **VACÍO** (BookBuilder nunca produjo) |
| ohlcv.raw | 3568/2652/2989 | Datos |
| ocm.dlq | 157/156/165 | **Hay fallos en DLQ** (pendiente de inspección en Phase 2) |

**Frescura:** el sondeo del último mensaje de `orderbook.raw` (partición 0) no devolvió mensaje fresco dentro del timeout en esta sesión; la sesión previa fijó el último `CreateTime` en **2026-08-21T16:56Z** → **STALE ~7 días**. **El data-plane no fluye en tiempo real.** `book.*` vacíos ⇒ **no hay BookBuilder emitiendo**; `trades.raw` vacío ⇒ **no llegan trades**.

**Consumer groups activos:** `dlq-*`, `replay-*`. **NO** existe `ocm-book-builder` (constante `GROUP_BOOK_BUILDER` sin uso).

### F. Bybit (CÓDIGO + DOCUMENTACIÓN OFICIAL)
- `config/exchanges/bybit.yaml`: `enabled: false`. `production.yaml`: **sin sección `exchanges:`**.
- Protocolo WS público: `wss://stream.bybit.com/v5/public/linear`, tópicos `orderbook.{depth}.{symbol}`, `publicTrade.{symbol}`, `kline.{i}.{s}`, `tickers.{s}`, `allLiquidation.{s}`, `insurance.{s}`.
- **Autenticación (oficial Bybit /ws/connect):** "**Public topics do not require authentication.**" → El feed público de MD **NO necesita API key**.
- `.env` contiene `BYBIT_API_KEY`/`BYBIT_API_SECRET` (y KuCoin) — **NO deben usarse para la prueba pública**.

---

## 2. EVIDENCIA PRIMARIA (fuente por conclusión)

- **Git/systemd/process/docker/kafka/config:** RUNTIME (comandos múltiples; ver §EVIDENCIA-COMANDS al final).
- **F1 Bybit official (orderbook WS):** depths L1 10ms / L50 20ms / L200 100ms / L1000 200ms; tras suscribir se recibe `snapshot`, luego `delta` por cambio; **nuevo `snapshot` ⇒ reset ordenado local**; delta: size 0=delete, no-existe=insert, sí-existe=update; campos `b`(desc)/`a`(asc)/`u`(Update ID, `u==1`=restart→overwrite)/`seq`(cross sequence, orden)/`cts`(matching-engine ts, correlacionable con `T`)/`ts`; **NO hay checksum**. Ejemplo oficial muestra **delta multinivel** (varios niveles en `b`+`a` por mensaje).
- **F1 Bybit official (/ws/connect):** public no-auth; heartbeat ping ~20s; corte tras 10 min sin ping-pong/datos; reconnect ASAP; límite WS por dominio 500 conex/5 min.
- **Código (schema v1):** `shared/kafka/schemas/orderbook.py` — snapshot con `checksum: Optional[int]` **sin `u/seq/cts`**; delta de **un nivel** (`side`, `price`, `size`) **sin `u/seq`**.
- **Código (stream):** `cryptofeed_orderbook_stream.py:208-218` aplana delta: `for side_key... for price,size in delta.get(side_key,[])` → **1 mensaje nivel**. `book.sequence_number` y `book.checksum` son atributos fijados por cryptofeed `book_callback`; **solo `checksum` se propaga (snapshot), `sequence_number` NO**.
- **Código (config):** `ocm/config/schema.py:862-865` — `parse_exchanges` filtra `enabled:false`; `validate_exchanges` lanza si lista vacía.
- **Código (BookBuilder):** `GROUP_BOOK_BUILDER` definido (`shared/kafka/topics.py:179`), **0 usos** → no implementado.

---

## 3. HALLAZGOS

1. **Data-plane STALE:** `orderbook.raw` sin datos frescos (~7d); `trades.raw` vacío; `book.*` vacíos.
2. **service FAILED por CONFIGURACIÓN:** `At least one exchange must be enabled` (`schema.py:865`) — ningún exchange `enabled:true` (config SSOT). No es bug de código.
3. **No existe BookBuilder:** solo constante reservada; tópicos de salida vacíos; sin consumidor `ocm-book-builder`.
4. **Wire v1 aplanado/destructivo:** 1 nivel/msg; **sin `u/seq/cts`**; multiplica mensajes y rompe atomicidad/orden del protocolo.
5. **`seq` dropped por el stream:** cryptofeed entrega `book.sequence_number` pero el productor NO lo publica; `checksum` publicado pero nunca validado.
6. **Checkbox:** Bybit **sin checksum** → validar por `u`/`seq`/invariantes (corrige supuesto previo).
7. **Observabilidad NO operacional:** Prometheus/Grafana/Alertmanager `Created`.
8. **SKEW de particiones** en orderbook.raw (P1=0) — posible asimetría de routing.
9. **Git volátil / PRs sin integrar:** B-50..B-60 mayormente OPEN; HEAD en rama dependabot.
10. **Credenciales presentes en `.env`** pero el feed público NO las requiere.

---

## 4. RIESGO

| ID | Riesgo | Sev. |
|---|---|---|
| R1 | Declarar MD "READY" por proceso vivo / topic con datos | CRÍTICO |
| R2 | Implementar BookBuilder con `seq` contiguo `+1` → falsos gaps | ALTO |
| R3 | Wire v1 (aplanado, sin `u/seq`) imposibilita reconstrucción correcta | ALTO |
| R4 | Habilitar Bybit con credenciales de trading para datos públicos | CRÍTICO (evitable) |
| R5 | Observabilidad no operacional → sin alertas/freshness | MEDIO |
| R6 | DLQ con mensajes (157/156/165) — si no se inspecciona, corrupción silenciosa | MEDIO |
| R7 | Estado Git divergente → integrar/enviar PR sobre base equivocada | MEDIO |

---

## 5. CÓDIGO vs OPERACIONALMENTE VERIFICADO

- **Código:** existe adapter WS (cryptofeed), productor Kafka, schema v1, consumer infra (poll/commit/seek), config pipeline. **OK en diseño.**
- **Operacionalmente VERIFICADO:** NADA del data-plane end-to-end. El servicio no arranca (config); el feed no entrega datos frescos; no hay consumidor; no hay métricas scrapeadas. **La infraestructura (Kafka/Redis/proceso) vive; el data-plane no.**

---

## 6. RELACIÓN CON ADRs

- **ADR-0022:** exige separar "proceso vivo" de "datos aptos" → no se declara happy. Incumplido operativamente (service FAILED).
- **ADR-0014:** `data_quality` (timestamp/missing/duplicate) → no operativo.
- **ADR-0023/B-25:** gap detection diferido hasta consumidor real → sin consumidor, sigue diferido.
- **ADR-0028 (propuesta docs/proposals):** coherente con la evidencia (u/seq/multinivel/checksum-none/resync); las decisiones D-7a/D-7b/D-7c/D-7d permanecen. La implementación NO existe.
- **ADR-0038:** provisioning Grafana — config presente, despliegue NO (containers Created).

---

## 7. RELACIÓN CON PAPERS/LIBROS/DOC OFICIAL

- **Bybit oficial (F1)** = fuente primaria técnica para el orderbook. Coincide con la propuesta ADR-0028 en: multicanal multinivel, snapshot-reset, `u==1` overwrite, `seq` cross, sin checksum, resync por re-snapshot.
- **KB TIER_1** (Lehalle & Laruelle *Market Microstructure in Practice*; Buzzelli *DQE*): motivación/concepto para coherencia L2 y data quality; **informativos, no normativos** (jerarquía KB: ADR/código > doc oficial > KB > libros). No hay artefactos legibles en esta sesión (metadata only).
- Las **referencias de implementación** (hummingbot/nautilus) validan: delta multinivel atómico y snapshot-restore + reject-stale como recovery (no gap estricto `+1`).

**No hay contradicción** entre doc oficial y el diseño propuesto al nivel de hechos de protocolo; la única tensión es que la propuesta **marca explícitamente D-7b como empírico** (correcto) y NINGUNA fuente garantiza contigüidad `+1` de `u`/`seq`.

---

## 8. CAMBIOS REALIZADOS
- **Ninguno** en código, configuración, producción, systemd, Bybit, ADR-0028, git HEAD.
- Reporte de auditoría creado en `docs/audits/` (documentación, permitida).

## 9. CAMBIOS NO REALIZADOS
- No se habilitó exchange. No se modificó `.env`. No se ejecutó P0 en vivo. No se implementó BookBuilder. No se hicieron commits/PRs. No se tocó trading. No se arregló documentación para ocultar discrepancias.

## 10. TESTS
- No ejecutados (read-only). Los relevantes existen para kafka/orderbook/architecture y deberán correrse en Phase 2 al tocar código.

## 11. DATA-PLANE STATUS
- **NO HEALTHY, NO READY.** Proceso `market_data.main` vivo + topic con datos, pero sin frescura demostrada, sin consumidor/BookBuilder, sin validación, sin métricas. (Iría §8/§16 DoD: solo se satisface "trading bloqueado".)

## 12. TRADING STATUS
- **BLOQUEADO** (sin ejecutor real; `LiveExecutor.IS_STUB=True` por ADR-0016). No se habilitó nada.

---

## 13. DECISIONES HUMANAS REQUERIDAS

**D-1 (config/producción):** ¿Habilitar Bybit en `production.yaml`/`config` para PUBLIC MARKET DATA ONLY (sin credenciales de trading)? Necesario para el streaming y para verificar operacionalmente. No se hace sin autorización.

**D-2 (ejecución de P0):** ¿Autorizar conexión READ-ONLY al WS público de Bybit (`wss://stream.bybit.com/v5/public/linear`, sin auth) para ejecutar el P0 y fijar D-7b? Queda fuera de alcance hasta aprobación (conexión externa a exchange).

**D-7 (gobierno):** ¿Aprobar diseño ADR-0028 para implementación? (con D-7a/D-7b/D-7c/D-7d resueltos).

**D-7a (schema):** delta **multinivel atómico** (recomendado, alineado con protocolo) vs un-nivel agrupado por `u`.

**D-7b (gap):** criterio empírico tras P0 (`u` por-book vs `seq` cross), **no** imponer `+1`.

**D-7c (precisión):** frontera Decimal→float en BookState/mid-spread.

**D-7d (alcance Fase 1):** incluir viewport/replay en Fase 1 o diferirlos.

**D-trabajo (git):** qué branch usar para la implementación (no la actual dependabot); integrar/cerrar PRs B-50..B-60 sin merge aquí.

---

## 14. SIGUIENTE ACCIÓN SEGURA (tras aprobación D-2)

1. **P0 experimental** (read-only WS público bybit, sin auth/sin creds/sin trading) → fijar D-7b. → REQUIERE D-2.
2. Documentar divergencia systemd (instalado vs template main vs working-tree) y proponer la reconciliación (mecánica, separada de decisiones arquitectónicas).
3. Inspeccionar `ocm.dlq` (157/156/165) para caracterizar corrupción existente.
4. Resolver D-1/D-7a/D-7c/D-7d, luego implementar (branch dedicada) y correr gate (tests/mypy/ruff/importlinter).
5. Desplegar observabilidad (Prometheus/Grafana/Loki) y re-verificar data-plane (§8 DoD).

---

## ANEXO — DISEÑO DEL P0 EXPERIMENTAL (Bybit, PUBLIC MD, READ-ONLY) — NO EJECUTADO

**Objetivo:** determinar empíricamente la semántica real del orderbook (D-7b) y documentar evidencia reproducible.

**Endpoints candidato** (oficial Bybit, mainnet public, sin auth):
- `wss://stream.bybit.com/v5/public/linear`
- Suscribir: `orderbook.50.BTCUSDT`, `publicTrade.BTCUSDT` (opcional `orderbook.200.BTCUSDT`).

**Diseño reproducible (aislado, auditable):**
- Script Python stdlib (`websockets` o `websocket-client`) que: conecta sin args de auth; envía `{"op":"subscribe","args":["orderbook.50.BTCUSDT"]}`; envía `ping` cada 20 s; persiste JSONL crudo a `data_platform/p0/<ts>/raw.jsonl`; corre T segundos.
- **Mediciones / salida (reporte markdown + CSV):**
  1. formato real snapshot vs delta (`type`), campos `s,b,a,u,seq,cts,ts`.
  2. **niveles por mensaje** en deltas (`len(b)`, `len(a)`) → confirma multinivel.
  3. **contigüidad de `u`** por-símbolo (¿+1? ¿saltos?); **contigüidad de `seq`** (¿+1? ¿saltos cross-exchange?).
  4. eventos `u==1` (re-snapshot/restart).
  5. duplicados (mismo `u`/`seq` duplicados) y reordenamiento.
  6. gaps: `u_next - u_prev != 1`; `seq_next - seq_prev` distribución.
  7. checksum: ausencia confirmada.
  8. comportamiento tras disconnect (nota: en script de corta duración registrar nº de reconnects/snapshots).
  9. correlación `cts`↔`T` si se suscribe también trades.
- **Reglas de parada del script:** solo lee; si detecta petición de auth/private → aborta; nunca envía órdenes; nunca usa credenciales; sin tocar balances/permissiones.

**Criterio de éxito P0:** caracterizar distribución de `u`/`seq` del símbolo y decidir D-7b con datos (gap primario por `u` vs `seq` vs monotonic+snapshot-restore).

**Estado:** DISEÑADO, en espera de autorización D-2 para ejecutar.

---

## EVIDENCIA / COMANDOS UTILIZADOS

```
git rev-parse --abbrev-ref HEAD; git rev-parse HEAD; git log --oneline -1
git rev-parse origin/main; git log --oneline -1 origin/main; git merge-base HEAD origin/main
git status --short; git diff --cached --name-only
systemctl is-enabled/is-active/status/show ocm-streaming.service
systemctl list-units --type=service | grep ocm
journalctl -u ocm-streaming.service -n 20
ps -eo pid,user,etime,args | grep -iE "market_data|streaming|uv run"
docker ps -a --format '{{.Names}}\t{{.Status}}\t{{.Image}}'
docker exec ocm_kafka kafka-topics --bootstrap-server localhost:9092 --list
docker exec ocm_kafka kafka-get-offsets --bootstrap-server localhost:9092 --topic <T>
grep enabled config/exchanges/bybit.yaml; grep exchanges config/env/production.yaml
rg -n "At least one exchange" ocm/config packages   # schema.py:865
sed -n '840,880p' ocm/config/schema.py; sed -n '100,145p' apps/app/cli/streaming_hydra.py
grep -oE "^[A-Z_]+=" .env | sed 's/=$//'            # solo nombres, secretos redactados
rg -n "GROUP_BOOK_BUILDER" shared/kafka/topics.py packages apps
sed -n '140,219p' packages/market_data/adapters/inbound/websocket/cryptofeed_orderbook_stream.py
webfetch bybit-exchange.github.io/docs/v5/ws/connect
webfetch bybit-exchange.github.io/docs/v5/websocket/public/orderbook
```
