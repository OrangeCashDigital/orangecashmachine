# MASTER CONSOLIDATION REPORT — OrangeCashMachine (OCM)

- **Fecha:** 2026-09-17
- **Modo:** READ-ONLY / FORENSE / CONSOLIDACIÓN DOCUMENTAL (sin escritura de código)
- **Estado de Git durante la auditoría:** rama `feat/adr0028-bookbuilder`, HEAD `cb6c6d7c`.
- **Protocolo:** AUDIT_PROTOCOL v2.1 (`docs/governance/AUDIT_PROTOCOL.md`).
- **Sujeto:** consolidación transversal de TODAS las auditorías OCM con foco en el incidente de streaming
  del 2026-09-17; deduplicación por causa raíz; cierres documentales solo con evidencia suficiente.

---

> **NOTA DE RECONCILIACIÓN (2026-09-18)** — correcciones aplicadas por evidencia reproducible según
> `docs/audits/AUDIT_OCM_reconciliacion-IN-01-06_2026-09-18.md`:
> 1. `scripts/check_production_gates.py` **SÍ existe** (b2ffe0ba 2026-08-23, PR #25) y corre en CI
>    (ocm-ci.yml:362, gate-ci). F-MC-18 (como OPEN) queda **resuelto por B-49** — ver §6, §17, D-5.
> 2. Contratos import-linter = **50**, no 49 (§1, §3, §20).
> 3. `.env` **nunca fue commiteado** (git vacío, `.gitignore:12`, chmod 600); lo versionado es
>    `deploy/host.env` (topología). F-MC-11 corregido.
> 4. Inventario legacy = matriz L-01..L-40 (40 filas): 18 CONFIRMADO_DEAD, 1 EN_USO (L-30) — no
>    "67 candidatos / 8 CONFIRMADO_DEAD" (§14, F-MC-11).
> 5. Fuente `AUDIT_OCM_CI-CD_github-actions_2026-09-17.md` añadida al inventario de fuentes (§5).

## 1. Executive Summary

OCM se audita a sí mismo con un ecosistema documental maduro: 33 ADRs, 80+ informes en
`docs/audits/`, reglas mecánicas `M1..M25` (validador), linter arquitectónico (ARCH-001..010) y 50
contratos import-linter. Esta consolidación cruza **todos los registros relevantes** —incidente
streaming 09-17, entrypoints/SSOT, deployment portability, legacy/dead-code, data-plane 08-28,
canary 08-08, kafka-topology 08-18, AI-agents, DOCUMENTATION_TOOLING, política/CI 08-18/08-19,
ADR-0017 discovery, P0 Bybit/BookBuilder— y deduplica por **causa raíz**.

**Veredicto global:** el sistema NO es production-grade para el data plane de streaming.
El incidente del 09-17 (crash-loop de `ocm-streaming.service`, ~641 073 errores de
deserialización en 24 h, recuperación manual) es un síntoma de causas raíz ya registradas y
parcialmente no remediadas, no un fallo nuevo aislado. La causa raíz de mayor rango es la
**cadena de configuración en cascada** (F-DPL-01, CRITICAL REGRESIÓN): `config/exchanges/*.yaml`
con `enabled:false` sin redefinición en `config/env/production.yaml` → lista de exchanges vacía →
pipeline sin feeds, contradiciendo el estado ACTIVE/HEALTHY declarado en el registro de 08-20.

**Consolidado en 20 findings maestros (F-MC-01..F-MC-20),** deduplicados por causa raíz:
3 CRITICAL · 5 HIGH · 10 MEDIUM · 2 LOW. Classification: 13 REVALIDADO, 1 REGRESIÓN,
1 RECOMENDACIÓN, 3 CONTRADICCIÓN, 2 NUEVO.

**Cierres documentales (CONFIRMADO_CERRABLE) aplicados:** B-45 (puerto Kafka, F-029), B-46
(Kappa OHLCV fail-fast, F-031), B-20 (forensics, VERIFIED WITH MINOR ISSUES), kafka-topology P1
(auto-create topics resuelto `fa98b32`), canary F-020/F-021/F-023/F-024/F-029 (resueltos), F-RT-01
(polars bridge, fix `61da7a9`). Se distinguen resolución técnica (implementación) de cierre
documental (evidencia verificable); ambos listados en §8.

**Siguiente tramo del Plan Maestro:** fases P0..P8 definidas en §19, sujetas a validación del
equipo (P5) antes de cualquier decisión de escala o capital.

---

## 2. Alcance, modo de operación y límites

- **READ-ONLY:** ninguna escritura de código, tests, CI, ADRs, tracking, systemd, Docker, Kafka,
  schemas, config, pyproject ni producción.
- **Escritura permitida:** únicamente `docs/audits/` (este informe) + actualizaciones de
  `docs/PLAN-Maestro-Ingenieria.md` y `docs/plans/tracking.yaml` **solo con evidencia** (§21); y
  `scripts/` NO (relación con el validador: solo ejecutar, no modificar).
- **Prohibiciones respetadas:** sin `git add .`, sin commit/push, sin reset/rebase/cherry-pick,
  sin borrar archivos legacy (`.bak`, logs), sin tocar la unidad systemd instalada.
- **Working tree:** preservado intacto — `M docs/plans/tracking.yaml` (diff preexistente) y
  auditorías 09-17 no trackeadas de la serie streaming.

---

## 3. Reproducibility Block

```
commit:   cb6c6d7c0fe0b8702534df6446b5f6f1c872ff5b
branch:   feat/adr0028-bookbuilder
fecha:    2026-09-17
protocolo: AUDIT_PROTOCOL v2.1
agente/modelo: OpenCode / big-pickle (síntesis final) — serie MiMo
herramientas: uv(python), pip-audit, ruff, mypy, bandit, pytest, yamllint,
              architecture_linter, import-linter, audit_validator, git
comandos:
  - uv run lint-imports --config architecture_linter/importlinter.toml
  - uv run python -m architecture_linter --root . --json
  - uv run pip-audit .
  - uvx yamllint -c .yamllint .
  - uv run python scripts/audit_validator.py --register docs/audits/AUDIT_OCM_master-consolidation_2026-09-17.md --report docs/audits/AUDIT_OCM_master-consolidation_2026-09-17.md --tracking docs/plans/tracking.yaml --adrs docs/architecture/decisions
golden:   tests/architecture_linter/test_golden.py (4 passed)
```

### Versiones de herramientas (2026-09-17)

| Herramienta | Versión | Gate |
|---|---|---|
| pip-audit | 2.10.1 | M14/Dependencias |
| ruff | 0.15.10 | M13/Lint |
| mypy | 1.19.1 | M13/Typing |
| bandit | 1.9.4 | M13/Security |
| pytest | 8.4.2 | M13/Tests |
| yamllint | 1.38.0 | M13/YAML |
| import-linter | 2.x | 50 contratos BC |

---

## 4. Protocolo de auditoría y jerarquía de fuentes

Orden de descubrimiento de esta consolidación (AUDIT_PROTOCOL):
1. `docs/PLAN-Maestro-Ingenieria.md` (SSOT de fases; F2.6 cerrada 08-10; F3/F4 avanzadas).
2. `docs/architecture/GOVERNANCE.md` (reglas de ADR, artefactos críticos, dueño de estado).
3. `docs/plans/tracking.yaml` (78 IDs: R1..R16 + B-01..B-60).
4. ADRs `docs/architecture/decisions/` (33 vigentes + template).
5. Informes/registros de `docs/audits/`.

Fuentes externas de conocimiento (libros/PDF en `docs/knowledge/`) **no son normas**; solo
referencia. Cualquier obligación requiere la cadena Conocimiento → Decisión → ADR/Governance → Control.

---

## 5. Inventario de fuentes revisadas (reguladas vs forenses)

| Fuente | Tipo | Estado | Hallazgos |
|---|---|---|---|
| `AUDIT_OCM_streaming-incident-entrypoints-architecture_2026-09-17.md` | Informe+Registro | PASS (14 findings) | F-01..F-14 |
| `AUDIT_OCM_deployment-portability_2026-09-17.md` | Informe forense | — | H-DEP-01..10, NV-1, NV-2 |
| `AUDIT_OCM_CI-CD_github-actions_2026-09-17.md` *(añadido en reconciliación 09-18)* | Informe forense | — | CI/CD: 9 workflows, branch protection PRESENTE (13 checks), Jenkins innecesario |
| `AUDIT_OCM_streaming-incident-recovery_2026-09-17.md` | Informe forense | — | narrativa incidente |
| `AUDIT_OCM_legacy-code-dead-code_2026-09-17.md` | Informe forense | inventario | L-01..L-40 / F-LEGACY-01..15 |
| `OCM_AUDIT_FINDINGS_2026-08-28_data-plane-streaming.yaml.md` | Registro | — | F-DPL-01 (CRITICAL REGRESIÓN), F-DPL-02 |
| `AUDIT_OCM_data-plane-streaming_2026-08-28.md` | Informe | — | D-1..D-6 |
| `AUDIT_OCM_market-data-and-adr0028_2026-08-28.md` | Informe | — | D-1..D-6, ADR-0028 estado |
| `AUDIT_OCM_state-end-to-end_2026-08-28.md` | Informe | — | No next milestone post-F2.6 |
| `AUDIT_OCM_p0-bybit-public-orderbook-experiment_2026-08-28.md` | Informe | P0 COMPLETE | P0 verdict |
| `AUDIT_OCM_adr-0017-discovery-review_2026-08-28.md` | Informe | Discovery FAIL | ADR-0017 gaps |
| `OCM_AUDIT_FINDINGS_2026-08-20_market-data-runtime.md` | Registro | — | F-RT-01 (CRITICAL, fix `61da7a9`), F-RT-02 (HIGH) |
| `AUDIT_OCM_forensic-compliance-b20_2026-08-18.md` | Informe | PASS | B-20 VERIFIED WITH MINOR ISSUES |
| `OCM_AUDIT_FINDINGS_2026-08-18_kafka-replay.md` | Registro | — | F-KAFKA-01..04 |
| `OCM_AUDIT_FINDINGS_2026-08-18_audit.md` | Registro | — | F-CI-01..03, F-ARCH-01..06, F-GOV-01..05, F-SC-01/02 |
| `OCM_AUDIT_FINDINGS_2026-08-19_policy-layer.md` | Registro | — | F-PL-01..11 |
| `AUDIT_OCM_kafka-topology-audit_2026-08-18.md` | Informe | P1 resuelto; P2 open | AUTO_CREATE / DLQ |
| `AUDIT_OCM_streaming-canary-audit_2026-08-08.md` | Informe | — | F-001..F-031 |
| `AUDIT_OCM_agent-audit-system_2026-08-18.md` | Informe | AUDIT_READY | — |
| `AUDIT_OCM_DOCUMENTATION_TOOLING_2026-08-19.md` | Informe | FAIL 11 mecánicos | F-DOC-01..07 |
| `docs/architecture/decisions/ADR-0022-lifecycle-proceso-realtime-feeds.md` | ADR | PROPUESTO | build_ws_producers huérfana |
| `scripts/audit_validator.py` | Tool | M1..M25 | reglas mecánicas |

---

## 6. Matriz Maestra consolidada (deduplicación por causa raíz)

> Cada fila agrupa findings de distintas fuentes bajo UNA causa raíz. La fila maestra hereda la
> severidad más alta y la clasificación que mejor describe el estado consolidado.

### Matriz de Findings

| ID | Severity | Classification | Consolida (fuente) | Causa raíz |
|---|---|---|---|---|
| F-MC-01 | CRITICAL | REGRESIÓN | F-DPL-01 | Exchanges deshabilitados en cascada → lista vacía → pipeline sin feeds |
| F-MC-02 | CRITICAL | REVALIDADO | D-1..D-6 (data-plane 08-28) | Data plane NO production-grade: snapshot/delta offsets 0, trades vacío, orderbook stale |
| F-MC-03 | HIGH | REVALIDADO | F-01, H-DEP-05, F-PL-11, B-59, incidente | Readiness Kafka ausente → crash-loop |
| F-MC-04 | HIGH | REVALIDADO | F-02, F-13, incidente, F-DPL-02 | Migración schema v1→v2 sin ventana → DLQ masivo |
| F-MC-05 | HIGH | REVALIDADO | adr-0017-discovery-review | Descubrimiento dinámico NO implementado (perfil Bybit ausente; ws_trades STUB) |
| F-MC-06 | MEDIUM | REVALIDADO | F-07, F-08, H-DEP-03 | SSOT entrypoints roto (run.sh/systemd/pyproject divergen) |
| F-MC-07 | MEDIUM | RECOMENDACIÓN | F-09, ADR-0022 | Streaming cableado en entrypoint, no en CompositionRoot |
| F-MC-08 | MEDIUM | REVALIDADO | F-04 | Stale de libro por umbral 2000 ms vs jitter del feed |
| F-MC-09 | MEDIUM | REVALIDADO | F-14, F-KAFKA-03, H-DEP-06, F-13 | Puertos Kafka 9093/9094 + DLQ inconsistente |
| F-MC-10 | MEDIUM | REVALIDADO | F-11, F-RT-02, F-017 | Observabilidad streaming insuficiente (pushgateway no operativo, sin métrica) |
| F-MC-11 | MEDIUM | REVALIDADO | F-LEGACY series (L-01..40) | Legacy: secretos en `.env`, `.bak`, deps sin importar, docs obsoletos |
| F-MC-12 | MEDIUM | REVALIDADO | F-06, H-DEP-01 | Env L2 roto (host.env OCM_* incongruente → malformed_key) |
| F-MC-13 | MEDIUM | CONTRADICCIÓN | F-10, F-05, F-025 | Drift docs↔código (retention 1h vs 168h; alertmanager comment) |
| F-MC-14 | HIGH | REVALIDADO | F-PL-01, F-ARCH-01..06 | Deuda de arquitectura gobernada por golden (7/10 FAIL, 19 findings) |
| F-MC-15 | CRITICAL | REVALIDADO | F-PL-02, F-CI-01 | 4 vulnerabilidades pip-audit (PYSEC-2026-3545/46/47/3552) |
| F-MC-16 | HIGH | CONTRADICCIÓN | F-GOV-05 | Licencia: LICENSE (PolyForm) ≠ pyproject (MIT) |
| F-MC-17 | LOW | REVALIDADO | F-PL-03, F-CI-02 | yamllint `deploy/monitoring/alerts.yml` |
| F-MC-18 | MEDIUM | CONTRADICCIÓN | F-PL-04 → resuelto por B-49 (09-18) | `check_production_gates.py` SÍ existe y corre en CI; aserto "inexistente" falso |
| F-MC-19 | MEDIUM | NUEVO | F-12, H-DEP-04 | Units systemd divergentes (template↔rendered↔instalado, `.env`) |
| F-MC-20 | LOW | NUEVO | F-GOV-01, F-025 | INVENTORY.md pendiente; riesgo_aceptado drift v1/v2 |

---

## 7. Causas raíz y separación FACT / INFERENCE / HYPOTHESIS

**FACT (evidencia reproducible):**
- Incidente: 641 073 `book_builder_deserialize_error` en 24 h (~1 msg/s en v1), offset 15 509 632 en
  `orderbook.raw` consumido, crash-loop `KafkaConnectionError: Unable to bootstrap from
  [('localhost', 9093, ...)]`, MainPID 9371 a las 17:20:02, streaming_started 17:20:13.
- `_ADAPTER_CLASSES` = registro estático (sin descubrimiento). `ws_trades_source.py` = STUB.
- `config/exchanges/*.yaml:5` `enabled:false` ×3; `config/env/production.yaml` sin `exchanges:`.
- `cryptofeed_orderbook_stream.py:75` (adapter) instanciado en `streaming_hydra.py:212`, no en CR.
- B-46: Kappa OHLCV fail-fast implementado (RuntimeError estructural) — riesgo de pérdida silenciosa
  mitigado con `NullPublisher` flip revertido (`fe4525f` documentado).
- ADR-0022 PROPUESTO: `build_ws_producers()` huérfana — "Usado por main.py" desactualizado.

**INFERENCE (no afirmada como causalidad):**
- Atribución cuantitativa del 100% de los 641 073 errores a la migración v1→v2 (correlación
  sólida por ptype/offset, pero la causa raíz última de arranque del flujo no se fija solo con logs).
- "El crash-loop causó el DLQ" — el DLQ depende del consumidor; correlación ≠ causalidad (F-01 no
  implica F-02); se registraron como findings independientes.

**HYPOTHESIS (requiere validación):**
- Si se remedia solo el converter en backfill (sin DI de chunk_converter), `NullPublisher` retorna
  `True` → pérdida silenciosa de OHLCV (mitigado por fail-fast B-46, verificar en release).

---

## 8. Decisiones de cierre documental (CONFIRMADO_CERRABLE) y resolución técnica

Regla aplicada: cierre documental solo si evidencia verificable y reproducible, sin depender de
implementación futura, y sin contradecir otro documento. Se distingue **resolución técnica**
(código/implementación) de **cierre documental** (evidencia en repo).

| Contexto (B/Audit) | Resolución técnica | Cierre documental | Evidencia |
|---|---|---|---|
| B-45 / F-029 (puerto Kafka) | `KAFKA_HOST_PORT=9094` + `.env`/`.env.example` | **CERRADO** 2026-08-09 | `docker compose ps` 8 servicios |
| B-46 / F-031 (Kappa OHLCV) | Fail-fast estructural en `OHLCVPipeline.__init__` + `pipeline_factory._build_ohlcv` | **CERRADO** 2026-08-14 | RuntimeError reproducido + test |
| B-20 (forensics) | — | **VERIFIED WITH MINOR ISSUES** | AUDIT 08-18 PASS |
| kafka-topology P1 | `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` (`fa98b32`) + `ALL_TOPICS` (`10ec308`) | **CERRADO (P1)** | docker-compose + provision script |
| Canary F-020/F-021/F-023/F-024/F-029 | clase/legacy eliminados; enum; puerto | RESUELTOS | AUDIT 08-08 |
| Polars bridge (F-RT-01) | fix `61da7a9` | **CERRADO** | AUDIT 08-20 register |

**NO cierre (permanecen abiertos):** F-DPL-01 (config), B-58 (Grafana gitignored), P0 Bybit
(implementación pendiente), ADR-0017 (Discover Profile), F-MC-14 (deuda architecture linter bajo
golden — requiere ADR-0021/0030), DLQ/retention y todas las cadenas de streaming con software.

---

## 9. Incidente de streaming 2026-09-17 (síntesis forense)

- **Crash-loop:** arranque vía systemd sin precondición activa de Kafka (`After=network-online` no
  garantiza broker); `Restart=on-failure` + `RestartSec=10` → reintento infinito, ~11 s por ciclo.
- **Ventana sin streaming:** ~5 h (hasta recuperación manual 17:20). Recuperación: `Restart`,
  MainPID confirmado, heartbeat 17:20:13.
- **Métricas:** errores 641 073 acumulados; última lectura de libro pre-incidente: orderbook.raw
  23M msgs con lag previo de 5 días (§5 de recovery audit).
- **Resiliencia:** no hubo dead-letter playback ni escalado manual; el incidente se recuperó por la
  naturaleza idempotente del sistema (re-procesado), no por mecanismo de DLQ automático (F-MC-04).

Consolidación: el incidente es **síntoma** de F-MC-01+03+04 (config vacía + readiness ausente +
migración sin ventana). Nada nuevo respecto a findings previos; se REVALIDA F-01/F-02/F-13.

---

## 10. Estado del data plane / market data (post-mapa 08-28)

- `orderbook.raw`: ~23 392 027 msgs; último lag de 5 días; en 09-17 con errores de schema v1 masivos.
- `book.snapshot`/`book.delta`: offset **0** — BookBuilder NUNCA produjo; ADR-0028 permanece
  PROPUESTO a pesar de shims.
- `trades.raw`: tapado (consumer deshabilitado).
- **Veredicto:** data plane NO production-grade (F-MC-02 CRÍTICO). No hay NEXT MILESTONE
  post-F2.6 en state-end-to-end.

---

## 11. P0 Bybit: evidencia empírica (07/28 ago)

- **Protocolo:** `wss://stream.bybit.com/v5/public/linear`, `orderbook.50.BTCUSDT`, 60 s, 1283 msgs,
  sin autenticación. `u` contiguo (+1 en 1279/1279) → gap = `u` salto; `seq` NO contiguo
  (usar `seq+1` → 100% falsos gaps). Deltas multinivel (75.1% >1 nivel), 5.5% con delete
  (`size="0"`), 2.4% snapshot-reset, **sin checksum session**.
- **Veredicto:** `P0 COMPLETE — IMPLEMENTATION REQUIRED`. El protocolo de descubrimiento
  (ADR-0017) debe alimentar al perfil Bybit operativo antes de capital (F-MC-05).

---

## 12. Entrypoints y SSOT operacional

- SSOT: `run.sh`, `pyproject.toml [project.scripts]`, systemd (`ExecStart`), supervisor. Los tres
  divergen: `run.sh` NO expone streaming; systemd usa `.venv/bin/python -m app.cli.streaming_hydra`
  vs script registrado. `pyproject.toml:181` `streaming = "app.cli.streaming_hydra:main"`.
- `*_hydra.py` filtra Hydra en el nombre (F-08) — naming hygiene, no bloqueante.
- Patrón `python -m app.cli.X` para systemd no está scripteado en `[project.scripts]` para
  `streaming` (F-07 MEDIUM).

---

## 13. Deployment portability (H-DEP-01..10) — revalidación

- H-DEP-01: host.env malformed_key `OCM_HOST_USER`/`OCM_REPO_ROOT` (F-MC-12).
- H-DEP-03: SSOT deploy (F-MC-06). H-DEP-04: units divergentes (F-MC-19).
- H-DEP-05: readiness/crash-loop (F-MC-03). H-DEP-06: puerto Kafka (F-MC-09).
- H-DEP-07..10 (instalador, heredado, runbook, hardcoded): sin cambio en esta consolidación;
  revalidados como PENDIENTES salvo evidencia contraria.
- NV-1/NV-2 no verificables → NO se cierran.

---

## 14. Legacy / dead code

`AUDIT_OCM_legacy-code-dead-code_2026-09-17.md` (F-LEGACY-01..15 + matriz L-01..40, 40
candidatos — reconciliado 09-18): 18 CONFIRMADO_DEAD, 2 PROBABLE, 19 POSIBLE, 1 EN_USO (L-30),
incl. `.env` (secretos en disco chmod 600, NO commiteado), `infrastructure/` vacío, `duckdb/lz4/
aiometer/pybreaker` no importadas, `sphinx`. Consolidado en F-MC-11 (MEDIUM). **Alerta:** no
borrar nada sin autorización separada; `.bak` ocupan espacio pero son trazabilidad temporal.

---

## 15. Kafka / topics / DLQ

- `shared/kafka/topics.py` SSOT (25 constantes). DLQ de schema v1 (318 744 msgs) retenido desde
  09-07 sin reconciliación; decay 336 h en `orderbook.raw` vs retención 168 h configurada (drift).
- AUTO_CREATE deshabilitado (P1 resuelto); `scripts/provision_kafka_topics.py` idempotente.
- P2 DLQ: diseño por-tópico vs global sigue abierto (F-MC-09/F-MC-04).

---

## 16. Observabilidad / métricas

- `rows_ingested_inc` para streaming AÚN sin métrica dedicada (F-RT-02 HIGH → F-MC-10 REVALIDADO).
- pushgateway durante el incidente NO operativo (sin scrape), Grafana sin dashboards (B-58 open).
- Stale de libro (F-MC-08): umbral 2 s vs p50 feed 163 ms → verifica `stale_ms` en AppConfig.

---

## 17. CI / gates / Policy Layer

- 4 vulns pip-audit (F-MC-15 CRITICAL) → tracking B-50 (existe).
- yamllint alerts.yml (F-MC-17 LOW). Production gate script (`check_production_gates.py`)
  **existe** (b2ffe0ba 2026-08-23; PR #25) y corre en CI (`ocm-ci.yml:362`, gate-ci) → F-MC-18 resuelto por B-49 (reconciliación 09-18).
- Licencia: `LICENSE` PolyForm vs `pyproject.toml` MIT → F-MC-16 HIGH CONTRADICCIÓN.
- Linter arquitectónico: golden 4 passed; 7/10 reglas FAIL → 19 findings/16 failed (F-MC-14 HIGH):
  gobernado por golden + track B-21 (position) y ADR-0021/0030.

---

## 18. ADR y governance (estado)

| ADR | Estado | Nota de esta consolidación |
|---|---|---|
| ADR-0022 (lifecycle realtime) | PROPUESTO | `build_ws_producers()` huérfana; docstring desactualizado |
| ADR-0028 (BookBuilder) | PROPUESTO (draft) | BookBuilder sin producción; requiere ACEPTAR tras P0/P1 |
| ADR-0017 (discovery) | PROPUESTO | implementación ausente (F-MC-05) |
| ADR-0021/0030 (posición/balance) | ACEPTADO | deuda de arquitectura rastreada |
| INVENTORY.md | PENDIENTE | obligación GOVERNANCE §6 no cumplida (F-MC-20) |

---

## 19. Plan Maestro — siguiente tramo: fases P0..P8

Planificación consolidada del siguiente tramo (documental; requiere autorización por fase):

| Fase | Ámbito | DOR (salidas clave) | Criterio de salida |
|---|---|---|---|
| **P0** | Estado del cluster y data plane (orderbook/snapshot/delta/trades) | doc baseline data-plane; F-MC-02 verificado | métricas verificables de producción |
| **P1** | Readiness + crash-loop (B-59) | F-MC-03 → fail-fast + readiness pump | `systemctl` active sin crash-loop sostenido |
| **P2** | SSOT entrypoints/units (F-MC-06/19) | run.sh=pyproject=systemd | diff limpio |
| **P3** | Config exchanges + env L2 (F-MC-01/12) | validación lista exchange, sin cascade | config validation green |
| **P4** | Schema v1→v2 evolución con ventana + DLQ (F-MC-04/09) | dual-read + replay/DTAP probado | reconciliación v1/v2 completa |
| **P5** | **GATE de validación del equipo** — revisión humana de P0..P4 | sign-off Ops/QA del tramo | acta/PR de validación |
| **P6** | Observabilidad (F-MC-08/10, B-58) | métricas streaming + dashboards | alertas operativas |
| **P7** | ADR-0017 discovery + perfil Bybit (F-MC-05, P0) | Discovery Profile operativo | tests de gap/checksum |
| **P8** | BookBuilder producción (ADR-0028) | schema aceptado → producer → snapshot/delta | offsets > 0 + fixture e2e |

> La P5 es bloqueante: ninguna decisión de escala (F5 del PLAN) ni de capital se toma sin sign-off.

---

## 20. Readiness (10 preguntas — respuestas consolidadas)

1. ¿Entrada única core? `uv run ocm` → `app.cli.main` (no hay main.py en raíz). `streaming` en
   scripts pero sin coincidencia con systemd exacta. → **PARTIAL**.
2. ¿Un systemd unit por servicio + arranque ordenado? units existen pero con drift y sin readiness
   de Kafka. → **FAIL**.
3. ¿Config single-source (Hydra) validada? sí `OCM_VALIDATE_ONLY`; pero cascade de exchanges
   vacía no falla en boot. → **PARTIAL**.
4. ¿DLQ con SQL/plan de reproceso? DLQ existe, reconciliación NO. → **FAIL**.
5. ¿Métricas de pipeline visibles? pushgateway inoperativo. → **FAIL**.
6. ¿Alertas operativas (alertmanager)? config present, Grafana vacía, no operativa durante
   incidente. → **FAIL**.
7. ¿Runbook de recuperación documentado? informes forenses sí; runbook en repo NO. → **FAIL**.
8. ¿Schema evolution con ventana? v1→v2 sin ventana → **FAIL**.
9. ¿Adr/contracts/tests en verde? contract-linter 50/50 y tests rutas verdes; arch-linter 7/10
   gobernado por golden. → **PARTIAL**.
10. ¿Composability del CI (fail-fast)? architecture (linter) → tests+config. Correcto. → **PASS**.

**Score: PASS 1 · PARTIAL 3 · FAIL 6.**

---

## Matriz de Controles

| Control | Comando canónico | Exit esperado | Resultado real |
|---|---|---|---|
| ARCH_CONTRACTS | `uv run lint-imports --config architecture_linter/importlinter.toml` | 0 | PASS |
| ARCH_LINTER | `uv run python -m architecture_linter --root . --json` | 0 (golden) | PASS |
| UNIT_TESTS | `uv run pytest tests/ -q` | 0 | PASS |
| ENGINEERING_HEALTH | `uv run python scripts/engineering_health_check.py` | 0 | PASS |
| GOLDEN | `uv run pytest tests/architecture_linter/test_golden.py -q --no-cov` | 0 | PASS |
| DEPENDENCY_AUDIT | `uv run pip-audit .` | 0 | FAIL |
| YAMLLINT | `uvx yamllint -c .yamllint .` | 0 | FAIL |
| DATA_PLANE_STREAMING | tópicos `book.snapshot`/`book.delta` offsets | > 0 | FAIL |
| CONFIG_PRODUCTION | `OCM_VALIDATE_ONLY` + cascade validation | lista exchange no vacía | FAIL |
| OPS_LIFECYCLE | `systemctl status ocm-streaming` | active sin crash-loop | FAIL |
| SCHEMA_GOVERNANCE | DLQ v1 counts / dual-read | 0 errores schema | FAIL |
| ENTRYPOINT_SSOT | `diff run.sh pyproject systemd` | idéntico | FAIL |
| DEPLOY_SSOT | `diff template rendered instalado` | idéntico | FAIL |
| OBSERVABILITY | scrape pushgateway / métricas por símbolo | presente | PARTIAL |
| DOCUMENTATION | INVENTORY.md + doc↔código | coherente | PARTIAL |

Controles = PASS(5) + FAIL(8) + PARTIAL(2) + NO_VERIFICADO(0) = 15

---

## Matriz de Decisiones

| D- | Pregunta | Opciones | Decisión de consolidación | Finding |
|---|---|---|---|---|
| D-1 | ¿Readiness en app o systemd? | app-backoff / ExecStartPre / ambos | Recomendar P1 (B-59) — decisión formal del equipo | F-MC-03 |
| D-2 | ¿Ventana dual v1/v2 o reset controlado? | dual-read / reset | dual-read preserva evidencia; P4 | F-MC-04 |
| D-3 | ¿Mover stream a CompositionRoot? | CR-build / entrypoint-build | CR-build (coherencia); P2 | F-MC-07 |
| D-4 | ¿Grafana gitignored (B-58)? | volver provisionar / mantener | volver provisionar; P6 | F-MC-10 |
| D-5 | ¿check_production_gates.py? | crear script / usar CI | **Resuelto por B-49 (2026-08-23)**: script existe (b2ffe0ba, PR #25) y corre en CI gate-ci — sin acción pendiente | F-MC-18 |
| D-6 | ¿Licencia PolyForm vs MIT? | alinear LICENSE | alinear a pyproject | F-MC-16 |
| D-7 | ¿Discovery Bybit a producción? | implementar ADC | P7 tras P5 sign-off | F-MC-05 |

---

## Integridad

- Escrituras durante esta auditoría: **solo este documento** + actualización de
  `docs/plans/tracking.yaml` (evidencia B-45/B-58, nota) y `docs/PLAN-Maestro-Ingenieria.md`
  (§ fases P0..P8). Sin commits, sin pushes, sin servicios.
- Working tree preservado: ` M docs/plans/tracking.yaml` (diff preexistente, NO revertido).
- Commits: 0 · Pushes: 0 · Servicios: 0.
- Resultado del validador: véase §22.

---

## 21. Actualizaciones documentales con evidencia

1. `tracking.yaml` B-45: evidencia ya presente (cierre 08-09) → solo NOTA de revalidación.
2. `tracking.yaml` B-58 (Grafana): anotación de evidencia confirmada en esta consolidación.
3. `docs/PLAN-Maestro-Ingenieria.md`: sección §4 `Fases siguientes — P0..P8` (nuevo), enlazando la
   tabla del §19 de este informe como referencia.
4. `docs/audits/AUDIT_OCM_master-consolidation_2026-09-17.md` (este documento, registro+informe).

Ninguna otra escritura. Cualquier modificación a código, CI, ADR, systemd, Docker o producción
queda fuera del alcance y se declara como trabajo futuro (P0..P8).

---

## 22. Resultado de la auto-auditoría / validador

```
uv run python scripts/audit_validator.py --register docs/audits/AUDIT_OCM_master-consolidation_2026-09-17.md --report docs/audits/AUDIT_OCM_master-consolidation_2026-09-17.md --tracking docs/plans/tracking.yaml --adrs docs/architecture/decisions
```

Comandos complementarios ejecutados (evidencia M13/M14):
`uv run lint-imports --config architecture_linter/importlinter.toml` · `uv run python -m
architecture_linter --root . --json` · `uv run pip-audit .` · `uvx yamllint -c .yamllint .` ·
`uv run pytest tests/architecture_linter/test_golden.py -q --no-cov`.

---

## Resumen del registro (mismo cálculo §6)

Resultado declarado en Matriz §6:

- CRITICAL: 3
- HIGH: 5
- MEDIUM: 10
- LOW: 2
- INFO: 0
- total 20

Clasificación (taxonomía AUDIT_PROTOCOL §Q.10): REVALIDADO (13) · REGRESIÓN (1) · RECOMENDACIÓN (1) ·
CONTRADICCIÓN (3) · NUEVO (2).

```
- NUEVO: 2
- REVALIDADO: 13
- REGRESIÓN: 1
- CONTRADICCIÓN: 3
- RECOMENDACIÓN: 1
Controles = PASS(5) + FAIL(8) + PARTIAL(2) + NO_VERIFICADO(0) = 15
```

---

## F-MC-01 — Cascade de configuración: exchanges deshabilitados → pipeline sin feeds

Severity: CRITICAL
Status: OPEN
Classification: REGRESIÓN
Control: CONFIG_PRODUCTION (Hydra)

Evidence:
- `config/exchanges/*.yaml:5` `enabled:false` ×3 (Bybit/KuCoin) — sobreescritura sin redefinir lista en `config/env/production.yaml`
- `AppConfig.validate_exchanges` acepta lista vacía → boot sin feeds
- `OCM_AUDIT_FINDINGS_2026-08-28_data-plane-streaming.yaml.md F-DPL-01 (CRITICAL REGRESIÓN)`
Traceability:
- Tracking: NOT_TRACED · ADR: ADR-0028 · ADR: ADR-0017 · Closure: OPEN

---

## F-MC-02 — Data plane NO production-grade (snapshot/delta/trades sin producción)

Severity: CRITICAL
Status: OPEN
Classification: REVALIDADO
Control: DATA_PLANE_STREAMING

Evidence:
- `book.snapshot` / `book.delta` offsets 0 en 08-28; `trades.raw` vacío
- `orderbook.raw` ~23 392 027 msgs con lag 5 días + errores v1 09-17
- `AUDIT_OCM_data-plane-streaming_2026-08-28.md D-1..D-6`; `AUDIT_OCM_market-data-and-adr0028_2026-08-28.md`
Traceability:
- Tracking: NOT_TRACED · ADR: ADR-0028 · Closure: OPEN

---

## F-MC-03 — Readiness/restart: crash-loop de ocm-streaming.service

Severity: HIGH
Status: OPEN
Classification: REVALIDADO
Control: OPS_LIFECYCLE (systemd/readiness)

Evidence:
- Incidente 09-17: `KafkaConnectionError: Unable to bootstrap from [('localhost', 9093, ...)]`, RestartSec=10, ~11s/cycle, recuperación manual 17:20:02
- `AUDIT_OCM_streaming-incident-entrypoints-architecture_2026-09-17.md F-01` · H-DEP-05 · F-PL-11
- template systemd: `After=network-online.target docker.service` SIN precondición de broker
Traceability:
- Tracking: B-59 · ADR: ADR-0022 · Closure: OPEN

---

## F-MC-04 — Migración schema v1→v2 sin ventana → DLQ masivo

Severity: HIGH
Status: OPEN
Classification: REVALIDADO
Control: SCHEMA_GOVERNANCE

Evidence:
- 641 073 `book_builder_deserialize_error` en 24 h; 318 744 msgs DLQ retenidos desde 09-07
- break commit `3706d61c`; v1→v2 sin período dual-read
- `entrypoints F-02/F-13`; `OCM_AUDIT_FINDINGS_2026-08-28_data-plane-streaming.yaml.md F-DPL-02`
Traceability:
- Tracking: NOT_TRACED · ADR: ADR-0028 · ADR: ADR-0013 · Closure: OPEN

---

## F-MC-05 — Descubrimiento dinámico NO implementado (ADR-0017)

Severity: HIGH
Status: OPEN
Classification: REVALIDADO
Control: PROTOCOL_DISCOVERY

Evidence:
- `_ADAPTER_CLASSES` registro estático; `ccxt_adapter.load_markets()` no expuesto como port
- `ws_trades_source.py` STUB ("TODO: implementar conexión WS real")
- `AUDIT_OCM_adr-0017-discovery-review_2026-08-28.md` (Discovery Profiles = perfil Bybit ausente)
Traceability:
- Tracking: NOT_TRACED · ADR: ADR-0017 · ADR: ADR-0028 · Closure: OPEN

---

## F-MC-06 — SSOT entrypoints roto (run.sh / pyproject / systemd divergen)

Severity: MEDIUM
Status: OPEN
Classification: REVALIDADO
Control: ENTRYPOINT_SSOT

Evidence:
- `run.sh` sin target streaming; systemd `ExecStart=.venv/bin/python -m app.cli.streaming_hydra` vs script único
- `pyproject.toml:181` `streaming = "app.cli.streaming_hydra:main"`
- `entrypoints F-07/F-08`; `deployment-portability H-DEP-03`
Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Closure: OPEN

---

## F-MC-07 — Streaming cableado en entrypoint, no en CompositionRoot (ADR-0022)

Severity: MEDIUM
Status: OPEN
Classification: RECOMENDACIÓN
Control: COMPOSITION_ROOT

Evidence:
- `cryptofeed_orderbook_stream.py:75` construido en `streaming_hydra.py:212`, no en CR
- `build_ws_producers()` / `WSProducerBundle` presentes y testeados pero HUÉRFANOS (grep apps/ vacío)
- `docs/architecture/decisions/ADR-0022-lifecycle-proceso-realtime-feeds.md` docstring "Usado por main.py" falso
Traceability:
- Tracking: NOT_TRACED · ADR: ADR-0022 · Closure: OPEN

---

## F-MC-08 — Stale de libro por umbral 2000 ms vs jitter del feed

Severity: MEDIUM
Status: OPEN
Classification: REVALIDADO
Control: BOOKBUILDER_STALE

Evidence:
- 304 eventos con 2013–2288 ms (p50 feed 163 ms); umbral `stale_ms` 2000 ms en `AppConfig`
- `entrypoints F-04`
Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Closure: OPEN

---

## F-MC-09 — Puertos Kafka (9093/9094) + DLQ inconsistente

Severity: MEDIUM
Status: OPEN
Classification: REVALIDADO
Control: KAFKA_TOPOLOGY

Evidence:
- `conftest`/`host.env`/`.env.example` 9093 vs 9094 (cerrado B-45 en 08-09, pero drift en tests)
- DLQ v1 retenido sin reconciliación (F-13)
- `F-14`; `F-KAFKA-03`; `H-DEP-06`
Traceability:
- Tracking: B-45 · ADR: NOT_TRACED · Closure: OPEN

---

## F-MC-10 — Observabilidad streaming insuficiente

Severity: MEDIUM
Status: OPEN
Classification: REVALIDADO
Control: OBSERVABILITY

Evidence:
- pushgateway NO operativo durante el incidente (sin scrape); Grafana sin dashboards (B-58)
- `rows_ingested_inc` dedicada ausente (F-RT-02 HIGH 08-20)
- `F-11`; canary `F-017` (pushgateway hardcodeo)
Traceability:
- Tracking: B-58 · ADR: ADR-0022 · Closure: OPEN

---

## F-MC-11 — Legacy / dead code consolidado

Severity: MEDIUM
Status: OPEN
Classification: REVALIDADO
Control: LEGACY

Evidence:
- `.env` con secretos en disco local (chmod 600, `.gitignore:12`, **NUNCA commiteado** — L-06); la exposición versionada es `deploy/host.env` (topología, H-DEP-01); `infrastructure/` vacío (L-01)
- `duckdb/lz4/aiometer/pybreaker` sin imports (L-02..L-05); `sphinx` (L-29)
- docs obsoletas `docs/DOMAIN.md` (L-09/L-10); `*.bak` (L-07/08/19/20, 3 en `docs/audits/`)
- `AUDIT_OCM_legacy-code-dead-code_2026-09-17.md` F-LEGACY-001..015, 40 candidatos (matriz L-01..40: 18 CONFIRMADO_DEAD, 1 EN_USO — reconciliado 09-18)
Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Closure: OPEN

---

## F-MC-12 — Env L2 roto: host.env con OCM_* incongruente

Severity: MEDIUM
Status: OPEN
Classification: REVALIDADO
Control: CONFIG_ENV_L2

Evidence:
- `host.env` `malformed_key=OCM_HOST_USER/OCM_REPO_ROOT` (no cumple protocolo L2) → warning en config
- `entrypoints F-06`; `deployment-portability H-DEP-01`
Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Closure: OPEN

---

## F-MC-13 — Drift documentación ↔ código (retention, alertmanager)

Severity: MEDIUM
Status: OPEN
Classification: CONTRADICCIÓN
Control: DOCUMENTATION

Evidence:
- docstring retención `orderbook.raw` "1h" vs compose 168h (F-10)
- `deploy/monitoring/alertmanager.yml` comment asume observabilidad no operativa (F-025)
- F-05: topic reset/recreación no verificable
Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Closure: OPEN

---

## F-MC-14 — Deuda de arquitectura gobernada por golden

Severity: HIGH
Status: OPEN
Classification: REVALIDADO
Control: ARCH_LINTER

Evidence:
- `uv run python -m architecture_linter --root . --json` → 10 reglas: passed 3 / failed 7; findings 19, failed 16
- jerarquía: golden test 4 passed lo blindan como deuda conocida
- `F-PL-01` == `F-ARCH-01..06` (multi-owner position → ADR-0021; loop órdenes → ADR-0029; balance → ADR-0030)
Traceability:
- Tracking: B-21 · ADR: ADR-0021 · ADR: ADR-0030 · Closure: OPEN

---

## F-MC-15 — 4 vulnerabilidades de dependencias (pip-audit)

Severity: CRITICAL
Status: OPEN
Classification: REVALIDADO
Control: DEPENDENCY_AUDIT

Evidence:
- `uv run pip-audit .` (2.10.1): PYSEC-2026-3545, PYSEC-2026-3546, PYSEC-2026-3547, PYSEC-2026-3552
- `F-PL-02` == `F-CI-01` (single finding, 4 advisories)
Traceability:
- Tracking: B-50 · ADR: ADR-0020 · Closure: OPEN

---

## F-MC-16 — Licencia inconsistente (LICENSE vs pyproject)

Severity: HIGH
Status: OPEN
Classification: CONTRADICCIÓN
Control: LEGAL

Evidence:
- `LICENSE` = PolyForm Noncommercial 1.0.0; `pyproject.toml[project]` license = MIT
- `OCM_AUDIT_FINDINGS_2026-08-18_audit.md F-GOV-05`
Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Closure: OPEN

---

## F-MC-17 — yamllint `deploy/monitoring/alerts.yml`

Severity: LOW
Status: OPEN
Classification: REVALIDADO
Control: YAMLLINT

Evidence:
- `uvx yamllint -c .yamllint .` (1.38.0): 16 exp en alerts.yml
- `F-PL-03` == `F-CI-02`
Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Closure: OPEN

---

## F-MC-18 — `check_production_gates.py` inexistente vs ADR-0020

Severity: MEDIUM
Status: RESUELTO (corregido 2026-09-18)
Classification: ~~CONTRADICCIÓN~~ → RESUELTO
Control: PRODUCTION_GATE

Evidence (original — F-PL-04, 2026-08-19):
- `scripts/check_production_gates.py` no existía en repo; ADR-0020 lo declara obligatorio
- `F-PL-04` (CONTRADICCIÓN con `OCM_AUDIT_FINDINGS_2026-08-19_policy-layer.md`)

Evidence (corrección — reconciliación 2026-09-18):
- `scripts/check_production_gates.py` **EXISTE** (18595 bytes, commit `b2ffe0ba`)
- Commit `b2ffe0ba`: "feat(quality): fix and integrate check_production_gates.py (B-49)"
- `.github/workflows/ocm-ci.yml:362`: `run: uv run python scripts/check_production_gates.py --mode gate-ci`
- Branch protection required check: "Quality gates (ruff/mypy/SSOT/audit)" incluye production gates
- `AUDIT_OCM_CI-CD_github-actions_2026-09-17.md` §7: Production gates (G1/G2/G3/G10/G11) = YES, blocking

Traceability:
- Tracking: B-49 · ADR: ADR-0020 · Closure: RESUELTO
- Resolución: script implementado en `b2ffe0ba` (2026-08-23), integrado en CI como required check

---

## F-MC-19 — Units systemd divergentes (template ↔ rendered ↔ instalado)

Severity: MEDIUM
Status: OPEN
Classification: NUEVO
Control: DEPLOY_SSOT

Evidence:
- `deploy/systemd/templates/ocm-streaming.service.template` vs `rendered/` vs unidad instalada difieren (EnvironmentFile `.env`)
- `entrypoints F-12`; `deployment-portability H-DEP-04`
Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Closure: OPEN

---

## F-MC-20 — INVENTORY.md pendiente + riesgo_aceptado drift

Severity: LOW
Status: OPEN
Classification: NUEVO
Control: GOV_DOCS

Evidence:
- `docs/architecture/GOVERNANCE.md` §6 obliga INVENTORY.md → no existe
- `F-GOV-01` (INVENTORY); `F-025` (alertmanager comment)
Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Closure: OPEN