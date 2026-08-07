# ADR-0016: Lifecycle de proceso para `realtime_feeds` — entrypoint de streaming y separación process health / trading data eligibility

## Estado

Propuesto (auditado contra código real el 7-ago-2026).

## Contexto

ADR-0013 estableció `market_data` como bounded context propietario único de
toda adquisición de datos (streaming y no-streaming). ADR-0014 confirmó
`market_data` como **Market Data Platform**: un único dominio que posee
adquisición, normalización, distribución y calidad, con capacidades internas
cohesivas — `realtime_feeds`, `external_ingestion`, `normalization`,
`data_quality` — gobernadas por **un único composition root**, separadas
"por responsabilidad y ciclo de vida, nunca por protocolo". ADR-0014 no
decide qué proceso del sistema operativo ejecuta `realtime_feeds`.

Evidencia verificada en código (no hipótesis):

- `CompositionRoot.build_ws_producers()` (método de instancia, no función
  standalone) y `WSProducerBundle` existen completos en
  `packages/market_data/infrastructure/bootstrap/composition_root.py`,
  construyendo cuatro producers reales sobre `KafkaProducerAdapter`:
  `orderbook` (→ `orderbook.raw`), `funding` (→ `funding.raw`), `oi`
  (→ `oi.raw`), `liquidations` (→ `liquidations.raw`).
- El docstring de `WSProducerBundle` afirma "Usado por `main.py` para
  gestionar el lifecycle... en un único punto" — **verificado como
  desactualizado**: `grep -rn "build_ws_producers\|WSProducerBundle" apps/`
  no devuelve resultados. `main.py` (modo `ocm`) ejecuta únicamente
  `OHLCVPipeline` batch/histórico vía Hydra. La capacidad está
  implementada, testeada y **huérfana**.
- Existen **seis** adapters con lifecycle `start()`/`close()` bajo
  `adapters/inbound/websocket/`: `orderbook_producer`, `funding_producer`,
  `oi_producer`, `liquidations_producer`, `onchain_producer` e
  `infra_metrics_producer`. Solo los primeros cuatro están wireados en
  `WSProducerBundle`. `onchain` e `infra_metrics` quedan fuera del alcance
  de este ADR (ver "Fuera de alcance").
- `KafkaProducerAdapter` (`infrastructure/kafka/producer.py:32`) satisface
  `KafkaProducerPort` (`ports/outbound/kafka_producer.py:35`, `Protocol`)
  estructuralmente, sin herencia declarada — deuda ya conocida, ortogonal
  a este ADR.
- `run.sh` y `[project.scripts]` en `pyproject.toml` siguen el patrón SSOT
  `ocm|live|paper` → `exec uv run python -m app.cli.<modo>` /
  `<modo> = "app.cli.<modo>:main"`.
- **BC-51** (`architecture/importlinter.toml:897`) prohíbe importar
  `hydra`/`omegaconf` directamente desde cualquier módulo salvo la
  excepción documentada `app.cli.main` (frontera del framework, ADR-0006).
  Un nuevo entrypoint `streaming_hydra.py` que use `@hydra.main` **necesita
  añadirse explícitamente a esa excepción**, o bien construir su config
  exclusivamente vía `load_appconfig_from_hydra()` sin tocar `DictConfig`
  crudo, igual que hace `main.py`.
- Existe un guard AST (`scripts/app_layer_guard.py`, regla R14, contrato
  H8) que exige que `live_hydra.py`/`paper_hydra.py` importen su scaffolding
  desde `app.cli._bootstrap` — import-linter 2.x no puede expresar
  "must_import", por eso vive fuera del `.toml`. `streaming_hydra.py`
  debería seguir el mismo patrón y añadirse a esa regla.
- El precedente de shutdown en `live_hydra.py` es
  `try/except (KeyboardInterrupt, SystemExit) / finally: close()` — patrón
  válido para un CLI batch/acotado que ya iba a terminar solo. **No es
  aplicable tal cual** a un proceso de larga duración: `streaming_hydra.py`
  necesita esperar activamente señales (`asyncio.Event()` +
  `loop.add_signal_handler()`), no envolver una ejecución que termina sola.
- `require_promoted()` (`shared/kafka/provenance.py`) referencia
  literalmente "Promotion Rule (ADR-0017 §14)" en su docstring, equivalente al ticket B-23;
  su caso de uso documentado son guards de arranque para payloads de
  *ejecución de trading* (`OrderFilledPayload`, `OrderRejectedPayload`), no
  para eventos de `realtime_feeds`. Extenderlo a provenance de market data
  es una decisión adicional, no un hecho ya establecido.
- `DataQualityCheckerPort.check()` (`ports/outbound/data_quality_checker.py`)
  opera sobre `pl.DataFrame` — es **batch/columnar**, no evento-a-evento.
  No aplica directamente a mensajes streaming individuales sin un
  adaptador nuevo (por evento o por ventana corta), lo cual sería
  ownership/contrato nuevo, no una simple reutilización.

Existe además una distinción conceptual que este ADR fija explícitamente
porque afecta la seguridad de capital: que un proceso esté "vivo" no
implica que los datos que produce sean aptos para operar.

## Alternativas evaluadas

1. **Arrancar `realtime_feeds` dentro de `ocm` (modo batch actual).**
   Descartada. Mezclaría el lifecycle de un pipeline batch (ejecución
   acotada) con el de un proceso de streaming persistente.

2. **Arrancar `realtime_feeds` dentro de `trading` (`live_hydra.py`/
   `paper_hydra.py`).** Descartada explícitamente. Acoplaría `trading` a
   websockets/Cryptofeed, violando la separación que ADR-0013/ADR-0014
   establecen. `trading` debe seguir sabiendo únicamente que consume de
   Kafka.

3. **Crear un composition root nuevo, separado, solo para streaming.**
   Descartada. Contradice el texto literal de ADR-0014 (composition root
   único). Composition root (dónde se ensamblan los objetos) y process
   entrypoint (dónde arranca un proceso del SO) son conceptos distintos.

4. **Cuarto modo de ejecución `streaming`, entrypoint propio
   (`apps/app/cli/streaming_hydra.py`), reutilizando el `CompositionRoot`
   existente vía `build_ws_producers()`, supervisado como unidad `systemd`
   independiente.** **Elegida.**

## Decisión

### A. Modelo operativo de `realtime_feeds` (despliegue y lifecycle — no arquitectura de dominio)

Este ADR no crea ni modifica ningún bounded context, ni redefine la
propiedad de dominio fijada por ADR-0013/ADR-0014. Decide únicamente cómo
se instancian y supervisan en ejecución las capacidades ya definidas.

run.sh
├── ocm         → market data pipeline batch/histórico (Hydra) — existente
├── paper       → paper trading — existente
├── live        → live trading ⚠️ capital real — existente
└── streaming   → market data realtime_feeds (Hydra) — NUEVO 
Entrypoint dedicado (`apps/app/cli/streaming_hydra.py`, nombre a confirmar
en implementación). Este entrypoint:

1. Construye la instancia de `CompositionRoot` de `market_data` (el mismo
   composition root existente — no se crea ninguno nuevo) siguiendo el
   patrón de registro de Structured Configs ya usado en `main.py`
   (`_register_structured_configs()` antes de `@hydra.main`).
2. Se añade a la excepción documentada de **BC-51**, o construye su config
   exclusivamente vía `load_appconfig_from_hydra()` sin importar
   `hydra`/`omegaconf` fuera de esa frontera.
3. Importa su scaffolding desde `app.cli._bootstrap`, cumpliendo la misma
   regla que R14/H8 exige a `live_hydra.py`/`paper_hydra.py`; el guard AST
   (`scripts/app_layer_guard.py`) se actualiza para cubrir también este
   entrypoint.
4. Invoca `composition_root.build_ws_producers()` para obtener el
   `WSProducerBundle` (los cuatro producers: `orderbook`, `funding`, `oi`,
   `liquidations`).
5. Corre un loop de vida propio del proceso mediante espera activa de
   señales (`asyncio.Event()` + `loop.add_signal_handler()`) — no el
   patrón `try/except KeyboardInterrupt` de `live_hydra.py`, que asume una
   ejecución acotada.
6. Maneja `SIGTERM`/`SIGINT` para cierre ordenado, delegando `close()` de
   cada uno de los cuatro producers a los mecanismos de lifecycle que ya
   existen en los adapters.
7. Queda supervisado por su propia unidad `systemd`, independiente de
   `ocm`/`live`/`paper` (no existen unidades `.service` previas en el
   sistema para replicar convención — se parte de cero).

Como parte de la implementación, se corrige el docstring de
`WSProducerBundle` (referencia desactualizada a `main.py` como consumidor).

`trading` (live y paper) sigue sin conocer websockets, Cryptofeed, ni el
composition root de `market_data`. Su única relación con `realtime_feeds`
es indirecta, vía Kafka.

### B. Process health/readiness (capa 1 — "¿el proceso está vivo?")

Ámbito: propiedad exclusiva del proceso `streaming` y su supervisor
(`systemd`). Responde: ¿el proceso arrancó y sigue corriendo?, ¿los WS
producers están conectados?, ¿la conexión con Kafka está operativa?, ¿el
proceso puede seguir ejecutando su lifecycle?

Mecanismo concreto de exposición (log estructurado, métrica, endpoint) se
define en implementación, no aquí. **Este health es necesario pero no
suficiente** para autorizar que `trading` opere con esos datos.

### C. Trading data eligibility / capital safety (capa 2 — "¿los datos sirven?")

Cadena conceptual:

Process health     →  streaming (proceso, systemd)
Feed health         →  realtime_feeds (adapter, conexión al exchange)
Data quality flags  →  data_quality (capacidad YA implementada en
market_data: domain/quality/, ports/outbound/
data_quality_checker.py, infrastructure/quality/
ge_checker.py) — freshness, provenance, duplicados,
outliers, adjuntados como flags al evento canónico
Trading eligibility →  trading lee esos flags y decide si opera; NO
recalcula freshness/provenance por su cuenta 
Corrección respecto a versiones previas de este ADR: `data_quality` **no
es una capacidad reservada/futura** — ya tiene implementación real
(`DataQualityCheckerPort`, `NullChecker`, `ge_checker.py`, políticas de
dominio). Pero su puerto actual opera sobre `pl.DataFrame` batch, no
evento-a-evento streaming — por lo que **no puede asumirse que ya cubre
`realtime_feeds` tal cual**. Adaptarlo a streaming (evento a evento o por
ventana corta) es trabajo adicional, explícitamente fuera de alcance aquí.

`require_promoted()` (`shared/kafka/provenance.py`, Promotion Rule
ADR-0017 §14) cubre exclusivamente provenance de schema para payloads de
*ejecución de trading* ya registrados en `PROVIDENCE`. Extenderlo a
payloads de `realtime_feeds` es una decisión nueva, no un hecho ya
establecido — no debe citarse como si ya resolviera esta capa.

Este ADR fija la existencia de esta capa como necesaria y su independencia
respecto a process health, pero no diseña el mecanismo concreto. Eso queda
fuera de alcance — ver más abajo — y debería resolverse en un ADR
posterior, una vez que `realtime_feeds` esté corriendo y haya evidencia
operativa real de patrones de staleness en producción.

## Justificación técnica

- Mantiene el composition root único de `market_data` intacto, invocado
  desde un entrypoint de proceso distinto — composition root y process
  entrypoint son conceptos ortogonales.
- Preserva la separación de responsabilidades de ADR-0013/ADR-0014:
  `trading` sigue sin importar nada de websockets/Cryptofeed.
- Respeta el capability boundary ya fijado en ADR-0014 para `data_quality`:
  la evaluación de calidad de datos vive en `market_data`, no se
  reinventa parcialmente dentro de `trading`.
- Sigue el patrón SSOT ya validado (`run.sh`/`[project.scripts]`
  idénticos, mismo patrón usado al añadir `live_hydra.py`/`paper_hydra.py`).
- Respeta BC-51 (hydra/omegaconf encapsulados) y H8/R14 (scaffolding vía
  `app.cli._bootstrap`) en vez de crear un patrón de entrypoint paralelo.
- Evita el antipatrón de tratar `health/readiness` como proxy de `trading
  eligibility`: un proceso systemd-healthy con datos stale es exactamente
  el escenario que un gate de una sola capa no detectaría.

## Consecuencias

- **Más fácil:** `market_data` streaming se puede desplegar, reiniciar y
  supervisar independientemente del ciclo de vida de `trading` o de `ocm`.
- **Deuda consciente:** el mecanismo concreto de health/readiness y el de
  data eligibility streaming quedan como trabajo posterior, explícitamente
  fuera de alcance.
- **Nuevo requisito operativo:** unidad `systemd` adicional para
  `streaming`, sin convención previa que replicar.
- **Ningún BC ni composition root nuevo.** Sin impacto en los contratos
  import-linter existentes relacionados con `market_data` (salvo la
  extensión necesaria de la excepción BC-51 y de la regla AST R14/H8 para
  cubrir el nuevo entrypoint).

## Alcance

- Decisión de que existe un cuarto modo de proceso (`streaming`), su
  entrypoint, y que reutiliza el composition root único de `market_data`.
- Decisión de que `process health` y `trading data eligibility` son capas
  conceptualmente distintas, con dueños distintos (`streaming`/`systemd`
  vs. `market_data.data_quality` + `trading`).
- Contrato de alto nivel de shutdown (`SIGTERM`/`SIGINT` → cierre ordenado
  delegado a los cuatro producers existentes vía espera activa de señales).
- Extensión de BC-51 (excepción hydra/omegaconf) y de la regla AST R14/H8
  para cubrir `streaming_hydra.py`.

## Fuera de alcance

- Mecanismo concreto de exposición de health/readiness (log, métrica,
  endpoint) — se define en implementación.
- Diseño del mecanismo de `data freshness`/heartbeat streaming para
  trading eligibility, incluida la adaptación de `DataQualityCheckerPort`
  a eventos individuales — ADR posterior.
- Decisión sobre si `onchain_producer` e `infra_metrics_producer` (con
  lifecycle completo pero no wireados en `WSProducerBundle`) pertenecen
  conceptualmente a `realtime_feeds` y deben incorporarse al modo
  `streaming`.
- Decisión sobre si `require_promoted()`/Promotion Rule (ADR-0017 §14) se
  extiende a payloads de `realtime_feeds`.
- Autoscaling, multi-instancia, o alta disponibilidad del proceso
  `streaming`.
- Definición del nombre final del archivo/comando (`streaming_hydra.py` es
  tentativo).

## Criterios de aceptación (para implementación posterior)

1. `run.sh streaming` y la entrada correspondiente en `[project.scripts]`
   son idénticas al patrón SSOT ya usado por `ocm`/`live`/`paper`.
2. El entrypoint construye la instancia existente de `CompositionRoot` de
   `market_data` sin duplicarla ni crear una nueva.
3. `build_ws_producers()` se invoca únicamente sobre la instancia de
   `CompositionRoot` construida en este entrypoint — no desde `ocm`,
   `paper` o `live`, y no existen composition roots paralelos.
4. `SIGTERM`/`SIGINT` producen cierre ordenado verificable en tests
   (arranque → señal → verificación de `close()` en los cuatro producers:
   `orderbook`, `funding`, `oi`, `liquidations`), usando espera activa de
   señales, no el patrón `try/except KeyboardInterrupt` de `live_hydra.py`.
5. `trading` (`live_hydra.py`/`paper_hydra.py`) no gana ningún import
   nuevo hacia `market_data.infrastructure.bootstrap` ni hacia websockets.
6. Import-linter se mantiene en verde; BC-51 se extiende explícitamente
   para incluir `streaming_hydra.py` en la excepción documentada, sin
   relajar la prohibición general.
7. El guard AST (`scripts/app_layer_guard.py`, regla R14) se actualiza
   para exigir que `streaming_hydra.py` importe su scaffolding desde
   `app.cli._bootstrap`, igual que `live_hydra.py`/`paper_hydra.py`.
8. Existe al menos un mecanismo mínimo de exposición de health del
   proceso `streaming`, documentado como tal — sin pretender que sustituya
   trading eligibility.
9. El docstring de `WSProducerBundle` se corrige para reflejar el
   consumidor real (`streaming_hydra.py`), no `main.py`.

## Relación con otros ADRs y hallazgos

- **ADR-0013 / ADR-0014**: no se modifican. El composition root único se
  respeta explícitamente; este ADR resuelve la pregunta de proceso que
  ADR-0014 dejó abierta, y aclara que `data_quality` ya existe como
  implementación (no solo reservada) aunque no cubra streaming aún.
- **ADR-0006**: gobierna la frontera hydra/omegaconf (BC-51); este ADR
  extiende su excepción documentada, no la reinterpreta.
- **ADR-0017 §14 / Promotion Rule (`require_promoted()`)**: se referencia
  como mecanismo existente para provenance de payloads de trading; se dejó
  constancia expresa de que no cubre hoy eventos de `realtime_feeds`.

## Referencias

- `docs/architecture/decisions/ADR-0013-modelo-unificado-ingestion-datos.md`
- `docs/architecture/decisions/ADR-0014-diseno-interno-market-data.md`
- `architecture/importlinter.toml` (BC-51, línea 897)
- `scripts/app_layer_guard.py` (regla R14, contrato H8)
- `shared/kafka/provenance.py` (`require_promoted()`, Promotion Rule ADR-0017 §14)
- `packages/market_data/ports/outbound/data_quality_checker.py`
- `packages/market_data/infrastructure/bootstrap/composition_root.py`
  (`build_ws_producers()`, `WSProducerBundle`)
- `apps/app/cli/main.py`, `apps/app/cli/live_hydra.py`,
  `apps/app/cli/paper_hydra.py`, `run.sh`, `pyproject.toml`
  (`[project.scripts]`)
