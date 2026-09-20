# AUDITORÍA READ-ONLY — Estado Actual & Cadena de Evidencia End-to-End

- **Fecha:** 2026-08-28 · **Modo:** READ-ONLY (sin modificaciones de código, producción, CI, ADR, tracking ni merge)
- **Rol:** Lead Engineer + Principal Architect + Forensic Auditor
- **Objetivo:** Reconstrucción auditable de la cadena `Plan Maestro → ADRs → Discovery → Bybit P0 → Adapter → Schema → Kafka → BookBuilder → Systemd → Observabilidad → Market Data readiness → Trading gate`, detectar discrepancias y priorizar findings, respetando la jerarquía de evidencia de OCM.

---

## Executive Summary

- **Ninguna decisión de producción tomada; trading permanece BLOQUEADO; ninguna implementación ejecutada.**
- El siguiente milestone del Plan Maestro NO está explícitamente definido para la ruta Market Data en este momento: F2 (calidad), F2.5 (Discovery) y F2.6 (Capacity + Streaming MVP) han sido **formalmente cerrados** en tracking; el milestone "siguiente" nominal del plan es **F3 (trading live)**, pero ese es (a) bloqueado por el gate de capital (B-23, grado ADR-0016) y (b) NO la prioridad (Market Data first). → **NO NEXT MILESTONE DEFINED para Market Data post-F2.6 en el Plan Maestro**; esto se documenta (no se inventa B-61+).
- El camino real no bloqueado y prioritario es **cerrar B-59 (systemd, F2.6b) y B-58 (observabilidad Grafana, F4)** + institucionalizar el **Discovery Profile de Bybit (ADR-0017)** + validar el **data-plane end-to-end** para desbloquear Market Data READY bajo DoD verificable.
- Discrepancias críticas detectadas entre **código vs ADR vs config actual** (ver Findings): el data-plane no está operativo end-to-end; systemd FAILED; observabilidad no operativa; config con doble gate exchange/feed.
- **Veredicto: Market Data NO es READY.** No hay evidencia operacional suficiente (solo P0 empírico en WS público aislado).

---

## Scope

- Cadena completa de Market Data pública (Bybit) a través del plan, ADRs, código, config, Kafka, systemd, observabilidad.
- **Fuera de scope:** trading (bloqueado), portfolio, cambios de código, producción.
- Revisión de: `docs/PLAN-Maestro-Ingenieria.md`, `docs/plans/tracking.yaml`, ADR-0003/0013/0014/0016/0017/0022/0028, `config/market_data/feeds.yaml`, `config/exchanges/*.yaml`, `config/env/{production,development,test}.yaml`, `ocm/config/schema.py`, `apps/app/cli/streaming_hydra.py`, `market_data.main`, adapters WS, sistema Debian (systemd, procesos, Docker/Kafka), P0 Bybit.

---

## Evidence hierarchy aplicada

1. **Evidencia primaria** (systemd estado, journalctl, procesos, Kafka offsets, Docker) — usada como base.
2. **Código real** (schema.py, streaming_hydra.py, feed_registry, adapters, provenance).
3. **Tests** (baseline documentado: 900 passed, 49/49 BC, reportado en tracking).
4. **ADRs** (0013/0014/0016/0017/0022/0028).
5. **Docs de proyecto** (Plan Maestro, tracking).
6. **Fuentes externas** (documentación oficial Bybit — ya verificada en P0; papers en KB referenciados, no usados como autoridad).
7. **Opinión del agente** (última, marcada explícitamente).

---

## Current State (git / sistema)

### Git
- **Branch actual:** `feat/p0-bybit-public-market-data` (HEAD `3897be0`, basado en merge-base `c392f8f`).
- **origin/main:** `44034ea` (delante de la rama P0 en 1 commit: "close B-51 #27").
- La rama P0 **no está merged** sobre main; contiene solo nuestros archivos untracked de la auditoría/P0.
- **Open PRs:** 22 (B-52..B-56, deploy-systemd-adr0022 #20, kaf-001 #16, dependabot varios). **#20 (`feat/deploy-systemd-adr0022`) ES el PR abierto de plantillas systemd + instalador** — relevante para B-59.
- **Cambios preexistentes NO tocados:** `M uv.lock` (pre-existente); `?? deploy/systemd/rendered/` (untracked); `?? docs/audits/*2026-08-28*.md`; `?? docs/proposals/`; `?? docs/audits/p0_bybit/`. **No se modificaron, limpiaron ni mezclaron.**

### Sistema
- `ocm-streaming.service` → **failed** (Result=exit-code), arrancado 2026-08-28 14:29:15, en 2h58min. Causa root en journalctl: **"At least one exchange must be enabled"** (AppConfig validation) → `config/exchanges/*.yaml` TODO `enabled: false` y **producción NO tiene override de `exchanges`**.
- `market_data.main` PID 1051 **running** (2d10h), cwd repo, `python -m market_data.main`. Con `feeds.yaml` `ingestion_mode: rest` → **NO streamea orderbook WS** ("feed_orchestrator_skipped") — es el servicio REST/OHLCV (pip 8001).
- Docker: `ocm_kafka`, `ocm_zookeeper`, `ocm_redis`, `ocm_pushgateway` **Up (healthy)**. Prometheus/Grafana/Loki/Alertmanager/Promtail **NO en la lista healthy** → observabilidad NO operativa.
- Kafka topics: `orderbook.raw` p0=23025555, p1=0, p2=2025869 (STALE ~7d, sin datos frescos); `book.snapshot`/`book.delta`/`trades.raw` = **0** en todas las particiones.

---

## Architecture (jerarquía y cadenas)

Cadena prevista (ADR-0017 + ADR-0013/0014 + ADR-0028):
```
Bybit (public WS) → Discovery(ADR-0017) → Exchange/Discovery Profile → Adapter
 → Normalización → Schema (wire) → Kafka → BookBuilder/consumidor → Estado válido orderbook
 → Observabilidad → Freshness/Integrity → Replay/Recovery → MARKET DATA READY → (F3/trading)
```

Dependencias de capas (base): `shared → ocm → domain → ports → application → adapters → infrastructure`.

---

## Discovery / ADR-0017 (verificado documentalmente)

**Qué define ADR-0017:** Protocol Discovery Framework (PDF, 14 componentes) — **metodología**, no API de código. Componentes: 1 Objetivo, 2 Principios, 3 Tipos de evidencia, 4 REST Discovery, 5 WebSocket Discovery, 6 Execution, 7 Funding, 8 Liquidation, 9 Contract Provenance, 10 Normalización, 11 Validación, 12 Fixtures, 13 Tests, 14 Promotion Rule.

- **Qué está implementado:** Contract Provenance (`shared/kafka/provenance.py`, componente 9) + Promotion Rule (`require_promoted`, 14) + orderbook wire como `PROTOCOL` (promovido); normalización SSOT (10); tests de linaje (13).
- **Qué está solo diseñado:** componentes 4-8 (REST/WS/Execution/Funding/Liquidation Discovery) — definidos en paper, **sin perfil operativo** implementado.
- **Qué falta:** **Discovery Profiles** (by integración, Bybit = primero) NO existen en repo; `ws_trades_source.py` es STUB; el `MarketDataSource` port NO expone discovery. (Confirmado previo: `AUDIT_ADR-0017-DISCOVERY_2026-08-28.md` → veredicto "parcialmente implementada").
- **Encaje Bybit:** Bybit es el primer profile designado; el **P0** proporciona la evidencia PROTOCOL observada para ese profile (fixtures candidatas).
- **P0 → fixture/provenance:** **SÍ** — el P0 debe convertirse en **fixture congelado** (componente 12) + registro de **provenance PROTOCOL** del delta multinivel/`u`/`seq`/checksum antes de promover schema v2.
- **Discovery Profile institucionalizar:** **SÍ, recomendado** — es el requerimiento central de ADR-0017 aún pendiente (decisión humana DH-1).
- **Qué entra en SSOT:** schema v2 + fixtures + provenance del feed de orderbook.
- **Qué requiere nueva decisión humana:** formalizar el Discover Profile; registrar schema v2 como PROTOCOL (DH-1/DH-2); habilitar Bybit en producción (DH-4/D-1).

---

## Bybit P0 (prueba experimental — NO trading)

Confirmado read-only desde escenario previo (reporte `P0-BYBITS-PUBLIC-ORDERBOOK-EXPERIMENT-2026-08-28.md`, evidence `docs/audits/p0_bybit/evidence/20260828T220545Z/`):

- 60 s, `wss://stream.bybit.com/v5/public/linear` topic `orderbook.50.BTCUSDT`, **sin auth/keys**, 1280 orderbook msgs, 0 reconnects.
- Snapshot `u=101054915`, `seq=800573227279`, 100 niveles (50/50).
- **`u` estrictamente +1 (eq1=1279/1279, 0 dupes)** → sequence/update-id por libro.
- **`seq` NO contiguo (min gap 9, max 7593, p50 57)** → cross-sequence global; **no imponer `seq+1` universal**.
- Deltas multinivel: 75.1% >1 nivel (máx 88) → **unidad atómica**.
- Delete `size=0` confirmado; snapshot=reset; **sin checksum** en mensajes.
- Freshness p50 ≈ 163 ms.
- Implicaciones: **stale/gap por `u` (no `seq`)**; snapshot-restore ante `u==1`/reconnect; rechazos estructurales (crossed/out-of-order) por invariantes; conservar provenance de `u/seq/cts/checksum-ausente`.
- **No se asumió `+1` universal; se determinó experimentalmente** (correcto).

---

## Adapter / Schema / Kafka / BookBuilder (por etapa)

| Etapa | ¿Existe? | ¿Implementada? | ¿Conectada? | ¿Probada? | ¿Ejecutándose? | ¿Evidencia operacional? |
|---|---|---|---|---|---|---|
| Bybit public WS | Sí | CryptofeedOrderBookStream (ACL) | No (cfg inactivo) | Canary F2.6b/v1 | No | P0 aislado (no pipeline) |
| Adapter | Sí | BybitFeedAdapter (trades) + orderbook stream | No para orderbook | unit | No | — |
| Normalización | Sí | `shared/ports/outbound/normalization.py` | Parcial | tests | — | — |
| Schema wire | Sí | v1 (`orderbook.py`) PROTOCOL | Sí (definición) | tests wire | — | — |
| Kafka producer | Sí | `orderbook_producer.py` (+`cryptofeed_orderbook_stream`) | No en runtime | tests | No | orderbook.raw STALE ~7d |
| BookBuilder | **No** | No (ADR-0028 Propuesto) | No | No | No | — |
| Estado orderbook | No | No | — | No | No | — |
| Observabilidad | Parcial | plantillas/métricas | No | No | **No** | Prometheus/Grafana no healthy |
| Replay/Recovery | No | No | — | No | No | — |

**Conclusión:** existe **código** (producer, stream, schemas, metrics) pero **NO está operativo end-to-end** ni probado en runtime real. `orderbook.raw` stale ≠ feed vivo.

---

## Systemd (capa de operación — no prueba de correctness)

- **Unit versioneda:** `deploy/systemd/templates/ocm-market-data.service.template` (template) — **NO es el unit instalado**.
- **Unit realmente instalado:** `/etc/systemd/system/ocm-streaming.service` (root, mtime ago 20 13:55) — difiere de la template y del `rendered`.
- **Rendered (untracked)** `deploy/systemd/rendered/ocm-streaming.service` usa `deploy/host.env` (gitignored) — tercera variante.
- **Tres fuentes que NO coinciden** entre sí (instalado vs rendered vs template):
  - Instalado: `ExecStart=.venv/bin/streaming --env production`, `EnvironmentFile=.env`, `User=orangemusic`, `After=network-online.target`, `Restart=on-failure`, **sin hardening SafeOps**.
  - Rendered: `ExecStart=python -m app.cli.streaming_hydra` (sin `--env`), `EnvironmentFile=deploy/host.env`, `KillSignal=SIGINT`, **con hardening SafeOps**, `After=...docker.service`.
  - Template: (no inspeccionado a nivel unit instalado).
- **Root cause del FAIL:** config `exchanges.*.enabled` = false y producción sin override → schema validator falla. **No es un problema de systemd**, sino de **config de producción que no ha habilitado ningún exchange**.
- **Coherencia systemd ↔ ADR-0022:** ADR-0022 §7 asume supervisión; el unit installado cumple parcialmente, pero el modelo operativo declarado (systemd supervisando streaming) **no está verificado** (B-59 PENDIENTE). → **B-59 NO puede cerrarse** solo porque exista unit; requiere que el proceso arranque Y permanezca vivo Y produzca datos válidos.

---

## Observabilidad (separar CONFIGURADO vs DESPLEGADO vs ACTIVO)

- **Prometheus / Grafana / Loki / Alertmanager / Promtail:** provisioning **versionado/plantillas** existen (deploy/monitoring), pero los contenedores **NO están healthy/arrancados** en el estado actual de Docker (`docker ps` muestra solo kafka/zookeeper/redis/pushgateway Up). → **B-58 (Grafana provisioning versionado) NO verificado operacionalmente.** B-58 está PENDIENTE en tracking (F-PL-10, fase F4). `deploy/monitoring/grafana/provisioning/` y `dashboards/` están **gitignored y vacíos** (evidencia B-58).
- **Pushgateway:** Up (healthy) — pero Pushgateway operativo no prueba Market Data (lo confirma el enunciado).
- Config `observability.metrics.enabled: true`, exporter prometheus, puerto 8000 en producción — pero sin procesos exportando métricas de streaming real (el canary no corre).

---

## Orderbook integrity / BookBuilder

- **BookBuilder:** ADR-0028 es **Propuesto** (no aceptado); propuesta en `docs/proposals/ADR-0028-bookbuilder-design.md`.
- Coherencia con principios (verificado en propuesta): delta multinivel atómico ✓; no aplanar ✓; `u`-por-libro para gaps (no `seq+1`) ✓ (validado P0); snapshot-restore ante pérdida ✓; provenance ✓; **BookBuilder NO reutiliza GapAwareStream** (explícito en propuesta §18) ✓; separación transporte WS de validación ✓; vista/replay Fase 1 ✓; estado nunca válido solo porque el proceso viva ✓.
- **Falta:** que el schema v2 `u/seq/cts` exista en `shared/kafka/schemas/orderbook.py` (v1 no lo tiene), que se registre provenance del delta v2, fixtures del P0, y el consumidor `GROUP_BOOK_BUILDER` (reservado en topics.py, 0 usos).

---

## Tests

- Baseline documentado en tracking: **900 passed · 49/49 BC · mypy · 44% cobertura global** (F0/F1 gates). No se re-ejecutó suite completa (auditoría read-only); se cita el baseline de tracking como evidencia documentada.
- Tests de schema/provenance presentes (`tests/kafka/test_schema_provenance.py`, `test_schemas_orderbook.py`).

---

## ADR compliance

- **ADR-0017:** parcialmente implementado (provenance+promotion sí; Discovery Profiles no).
- **ADR-0028:** Propuesto (diseño coherente con principios y con evidencia P0).
- **ADR-0022:** entrypoint `streaming` existe; modelo lifecycle systemd **no verificado** (B-59).
- **Gate de capital (ADR-0016/B-23):** live/paper bloqueados — **no se inició trading**.
- **Cumplimiento documental:** no se modificaron ADRs ni se crearon ADRs nuevos.

---

## Scientific / industry references existentes

- KB (`docs/knowledge/manifest.yaml`) registra **9 fuentes con `status: needs_verification`** (no confirmadas) y tier de autoridad. Relevantes a microestructura: *Market Microstructure in Practice (2e)*, *Trading and Exchanges (Harris)*, *High-Frequency Trading (Aldridge)*, *Data Quality Engineering in Financial Services*, *Financial Data Engineering*. Todas **TIER_1/TIER_2, no normativas**; no se usan como autoridad arquitectónica; referencias existentes (no se crean).
- Se cita la **documentación oficial de Bybit** como autoridad de semántica WS (ya verificada en P0).

---

## Findings (ID, SEVERITY, DESCRIPTION, EVIDENCE, EXPECTED, ACTUAL, IMPACT, RECOMMENDATION, STATUS)

### F-001 — [CRITICAL] Data-plane orderbook NO operativo end-to-end
- **EVIDENCE:** `book.snapshot`/`book.delta`/`trades.raw` = 0 (Kafka); `orderbook.raw` stale ~7d; no hay consumidor BookBuilder; `ingestion_mode: rest`.
- **EXPECTED:** feed WS vivo → orderbook.raw fresco → BookBuilder valida → estado válido.
- **ACTUAL:** no hay stream en runtime; sin datos frescos.
- **IMPACT:** Market Data no puede considerarse READY; sin base para Fase 2.
- **RECOMMENDATION:** habilitar Bybit PUBLIC MD + correr canary streaming bajo revisión y verificar offsets/estado.
- **STATUS:** OPEN (bloqueado por DH/D-1).

### F-002 — [HIGH] Systemd FAILED por config (no exchange habilitado)
- **EVIDENCE:** `systemctl status` failed; journalctl `At least one exchange must be enabled`; `config/exchanges/*.yaml` enabled:false; production.yaml sin `exchanges:`.
- **EXPECTED:** servicio ACTIVE; proceso vivo.
- **ACTUAL:** failed (exit 1) en 1.068s; restart storm limitada.
- **IMPACT:** B-59 no cerrable; sin supervisión operativa.
- **RECOMMENDATION:** decidir/auto-habilitar exchange en producción (DH/D-1), verificar arranque, aplicar unit consistente.
- **STATUS:** OPEN.

### F-003 — [HIGH] Config de doble gate (exchanges.* vs feeds.*) — ambigüedad
- **EVIDENCE:** `streaming_hydra.py:main` valida `feeds.feeds.<ex>.enabled`; schema.valida `exchanges.*.enabled`; `feeds.yaml` bybit.enabled=true pero `exchanges/bybit.yaml` enabled=false.
- **EXPECTED:** una sola SSOT de habilitación de feed.
- **ACTUAL:** dos gates independientes que producen fallo desigual.
- **IMPACT:** confusión/fallas de arranque; corrección no obvia.
- **RECOMMENDATION:** alinear/unificar la habilitación (decisión de diseño; documentar).
- **STATUS:** OPEN.

### F-004 — [HIGH] Tres variantes de unit systemd no consistentes
- **EVIDENCE:** `/etc/systemd/system/ocm-streaming.service` vs `rendered/` vs `templates/` difieren (ExecStart, EnvironmentFile, hardening, KillSignal).
- **EXPECTED:** una sola unit SSOT versionada e instalada.
- **ACTUAL:** 3 fuentes divergentes; rendered gitignored.
- **IMPACT:** arranque no reproducible; advertencias de seguridad dispares.
- **RECOMMENDATION:** reconciliar en una unit versionada (base PR #20), documented, installada; test restart (B-59).
- **STATUS:** OPEN.

### F-005 — [HIGH] Discovery Profile (ADR-0017) no implementado
- **EVIDENCE:** sin módulo/perfil `discovery`; `ws_trades_source.py` STUB; port sin discovery; ver ADR-0017 + previo.
- **EXPECTED:** perfil Bybit (evidence+validación+promoción+limitaciones).
- **ACTUAL:** no existe.
- **IMPACT:** ADR-0017 parcialmente implementado; riesgo de asumir semántica sin profile.
- **RECOMMENDATION:** institucionalizar Discovery Profile Bybit (DH-1); convertir P0 en fixtures/provenance.
- **STATUS:** OPEN.

### F-006 — [MEDIUM] Schema v1 carece de `u/seq/cts`; delta plano
- **EVIDENCE:** `shared/kafka/schemas/orderbook.py` v1 (sin u/seq/cts; delta un nivel); `orderbook_producer.on_delta` por nivel (aplanado); `cryptofeed_orderbook_stream` no propaga sequence.
- **EXPECTED:** schema v2 multinivel con `u/seq/cts`, atómico.
- **ACTUAL:** v1 destruye atomicidad y pierde marcadores de integridad.
- **IMPACT:** BookBuilder no puede validar/recuperar correctamente.
- **RECOMMENDATION:** implementar schema v2 + propagación (tras D-7/DH-2) con fixtures/provenance del P0.
- **STATUS:** OPEN.

### F-007 — [MEDIUM] Observabilidad no operativa (B-58)
- **EVIDENCE:** Docker sin Prometheus/Grafana/Loki/Alertmanager/Promtail healthy; provisioning gitignored/vacío.
- **EXPECTED:** observabilidad activa con dashboards versionados.
- **ACTUAL:** solo plantillas; sin datos/alertas.
- **IMPACT:** B-58 no cerrable; sin freshness/integrity observable.
- **RECOMMENDATION:** versionar provisioning + levantar stack y verificar datos/alertas.
- **STATUS:** OPEN.

### F-008 — [MEDIUM] B-60 (documental) pendiente
- **EVIDENCE:** tracking if B-60 PENDIENTE (correction CodeQL/Trivy frequency); B-58/B-59/B-60 todos PENDIENTE.
- **EXPECTED:** corrección documental.
- **ACTUAL:** pendiente.
- **IMPACT:** bajo.
- **RECOMMENDATION:** corrección factual en docs.
- **STATUS:** OPEN.

### F-009 — [INFO] P0 es evidencia fuerte pero aislada
- **EVIDENCE:** P0 60s 1280 msgs; no conectado a pipeline; fixtures raw son candidatas.
- **EXPECTED:** feed perpetuo validado.
- **ACTUAL:** ventana puntual.
- **IMPACT:** no confunde con pipeline operativo.
- **RECOMMENDATION:** usar P0 → fixtures/provenance; repetir ventanas mayores al validar.
- **STATUS:** OPEN/EOF.

---

## Gaps

1. No hay BookBuilder ni estado de orderbook.
2. No hay schema v2 (u/seq/cts / multinivel).
3. No hay Discovery Profile de Bybit formalizado.
4. No hay start/stop/tests operativos de streaming end-to-end verificado.
5. No hay observabilidad activa que certifique freshness/integrity.
6. No hay replay/recovery probado.
7. Config de producción sin exchange habilitado (F-002/F-003).

---

## Human decisions (solo las reales)

| ID | DECISIÓN | OPCIONES | RECOMENDACIÓN | EVIDENCIA | RIESGO | IMPACTO | QUÉ CAMBIA | QUÉ NO CAMBIA |
|---|---|---|---|---|---|---|---|---|
| **DH-1 / D-1** | Habilitar **Bybit PUBLIC MD ONLY** en producción para el canary streaming real | (a) habilitar solo `exchanges.bybit.enabled=true` en config de producción; (b) no habilitar y quedarse en P0 aislado; (c) otro exchange | (a) habilitar SOLO MD público, sin trading | F-002/F-003; config sin override | Autoridad: es decisión de producción (no mecánica) | Desbloquea data-plane end-to-end y B-59 | Config producción (Bybit MD) | Trading, API keys, credenciales, permisos |
| **DH-2** | Institucionalizar **Discovery Profile de Bybit** (ADR-0017, componente 4-5) y convertir P0 → fixture/provenance | (a) formalizar ahora; (b) diferir a la implementación de schema v2 | (a) formalizar como parte del onboarding del data-plane | F-005; ADR-0017 | bajo (documentación) | Satisfacción del requerimiento central de ADR-0017 | Docs/artefactos de discovery | Código de producción |
| **DH-3** | Aprobar **ADR-0028 / schema v2 / D-7 (BookBuilder)** | (a) aprobar diseño y proceder a implementar schema v2+BookBuilder en branch dedicado; (b) solo aprobar schema v2; (c) diferir | (a) avanzar tras D-1/DH-1 | F-006; ADR-0028 propuesta | medio (arquitectura irreversible de contrato wire) | Desbloquea validador de integridad | Schema v2 + BookBuilder | Trading |
| **DH-4** | **B-59 cierre** (systemd verificado) | (a) cerrar solo tras arranque+datos válidos; (b) cerrar por unit existente | (a) NO cerrar hasta evidencia operacional | B-59 PENDIENTE; F-002/F-004 | bajo | Definición de DoD honesto | Estado B-59 en tracking | — |

**Acciones autónomas que NO requieren decisión humana** (mecánicas/reversibles, tras D-1): reconciliar unit systemd vs template (F-004), añadir fixtures/provenance del P0 (DH-2 ya recomendado), B-60 documental. **NO se ejecutan en esta auditoría read-only.**

---

## Recommended next steps (orden)

1. **PARAR — decisión humana DH-1 (D-1):** ¿habilitar Bybit PUBLIC MD ONLY en producción y autorizar el canary streaming real? (no mecánico; producción).
2. Tras DH-1: correr el canary bajo revisión (branch dedicado, sin trading), verificar `orderbook.raw` fresco + offsets avanzando.
3. Reconciliar unit systemd (F-004) y validar arranque/restart (B-59) con evidencia.
4. Institucionalizar Discovery Profile Bybit (DH-2) + fixtures/provenance del P0.
5. Implementar schema v2 (u/seq/cts + multinivel atómico) + registro PROVENANCE + BookBuilder `GROUP_BOOK_BUILDER` (DH-3) en branch dedicado con tests/lint/gates; separadamente de systemd.
6. Observabilidad activa (F-007/B-58): versionar provisioning + stack + freshness/integrity.
7. Definir/validar replay/recovery.
8. Solo tras evidencia completa: considerar Market Data READY bajo DoD verificable; luego estudio Fase 2 (trading). Trading permanece BLOQUEADO.

---

## Definition of Done (para Market Data READY — pendiente de verificación)

- [ ] Bybit PUBLIC MD habilitado en producción y canary real corriendo (dato fresco continuo).
- [ ] `orderbook.raw` + `book.*` con offsets avanzando y frescura verificada.
- [ ] Schema v2 `u/seq/cts` + multinivel atómico con PROVENANCE PROTOCOL y fixtures P0.
- [ ] BookBuilder `GROUP_BOOK_BUILDER` construye estado válido (no crossed, invariantes), con gap-by-`u`/snapshot-restore.
- [ ] Observabilidad confirma freshness/integrity (métricas + dashboards versionados, stack activo).
- [ ] Replay/recovery probado.
- [ ] Tests + ruff + mypy + import-linter 49/49 + auditoría.
- [ ] ADR-0017 Discovery Profile Bybit institucionalizado.
- [ ] ADR-0028 aprobado/acreditado con evidencia P0.
- [ ] B-58/B-59/B-60 cerrados con evidencia operacional.
- [ ] DoD verificable; **Market Data READY** declarado formalmente.

---

## Commands / evidence used

- `git rev-parse --abbrev-ref HEAD; git log --oneline -1; git log --oneline origin/main -1; git branch -a; git status --short; git check-ignore ...`
- `systemctl status ocm-streaming.service --no-pager -l`
- `journalctl -u ocm-streaming.service --no-pager -n 25`
- `cat /etc/systemd/system/ocm-streaming.service` vs `deploy/systemd/rendered/ocm-streaming.service` vs `deploy/systemd/templates/`
- `docker ps --format; docker exec ... kafka-run-class GetOffsetShell` (orderbook.raw, book.*, trades.raw)
- `ps aux; cat /proc/1051/cmdline; readlink /proc/1051/cwd`
- `cat config/env/production.yaml config/exchanges/*.yaml config/market_data/feeds.yaml`
- `sed -n '854,880p' ocm/config/schema.py`; `sed -n '175,290p' apps/app/cli/streaming_hydra.py`
- `rg` sobre tracking.yaml (B-57..B-60), Plan Maestro (F2.5/F2.6/F3), provenance, adapters.
- `gh pr list --state all`

## Exact commit/branch/PR references

- Branch: `feat/p0-bybit-public-market-data` (HEAD `3897be0`); no merged.
- origin/main: `44034ea` (#27).
- PRs abiertas relevantes: **#20** `feat/deploy-systemd-adr0022` (systemd/ADR-0022/B-59); **#21** `chore/gitignore-deploy-secrets` (gitignore host.env/rendered); #16 kaf-001; #26/#28..#32 (B-50..B-56).
- Merge-base P0 ↔ main: `c392f8f`.

---

*Fin de la auditoría read-only. No se modificó código, CI, ADR, tracking, producción ni .gitignore. No se crearon commits/PRs/merges. No se inició trading.*
