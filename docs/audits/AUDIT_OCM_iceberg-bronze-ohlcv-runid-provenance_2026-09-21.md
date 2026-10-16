# AUDIT — Iceberg Bronze OHLCV `run_id` Provenance (2026-09-21)

## §0. SAFOPS Header

```
MODIFIED         : NO
PROCESSES_KILLED : NO
CONTAINERS       : NO (solo inspeccionar `docker ps`/`docker images`)
SYSTEMD          : NO (solo `systemctl status`, sin arranque/parada)
KAFKA            : NO consumo de mensajes que altere offsets (solo GetOffsetShell/describe).
                   NO `provision`/`reset`/escritura. `ocm.dlq` observado, no tocado.
ICEBERG          : NO ALTER / no overwrite / no delete / no ingest de control.
                   Solo lectura de JSON de metadata, manifests y parquet (fs).
GIT              : NO commits, NO reset, NO checkout, NO stash apply/pop. `git log/show/branch` read-only.
MASTER PLAN      : NO MODIFICADO.
ESCRITURA ALLOWED: docs/audits/AUDIT_OCM_iceberg-bronze-ohlcv-runid-provenance_2026-09-21.md (este informe)
```

## §1. Scope

Determinar la **procedencia de la evolución de esquema `run_id`** en `bronze.ohlcv`
(Iceberg sql-catalog, `data_platform/iceberg_warehouse`), referida por la traza como
"versión 2160 ~2026-09-02 22:50". Objetivos:

1. Fijar con evidencia qué metadata file y mecanismo introdujeron `run_id`.
2. Clasificar cada hallazgo como `OBSERVATION` / `CORRELATION` / `CAUSALITY - DEMONSTRATED`,
   y cada afirmación como `CONFIRMED` / `CONTRADICTED` / `PARTIAL` / `NOT DEMONSTRATED` / `UNKNOWN`.
3. Verificar independencia de `OB-SNAPSHOT-01` (PR-A) y `OB-RESYNC-01` (PR-B) frente a
   OHLCV Recovery y al drift `run_id`.
4. No modificar nada (ver §0).

## §2. Metodología (read-only)

- Lectura de `data_platform/iceberg_warehouse/bronze/ohlcv/metadata/02*.metadata.json`
  (02159, 02160, 02161, 02162) + catálogo `data_platform/iceberg_catalog/catalog.db` (URI `mode=ro`).
- Inspección de parquet `/data/...` con pyarrow (`ParquetFile`, sin escribir).
- Git forense: `git log --all -S run_id`, `git show`, `git branch --contains`, `git reflog`,
  stashes, worktrees. Todo read-only.
- `$HOME/.zsh_history` (solo lectura) acotado a la ventana temporal de la evolución.
- Contraste obligatorio con `docs/audits/AUDIT_OCM_master-market-data-implementation-order_2026-09-21.md`.
- Ninguna escritura salvo este informe.

## §3. Convención de numeración Iceberg (evitando la ambigüedad "2160")

- Nombre de metadata file: `NNNNN-<uuid>.metadata.json`. Es un **contador de metadata versions**,
  no igual a `last-sequence-number`.
- `last-sequence-number` SOLO avanza al crear un snapshot con datos. Un update **metadata-only**
  (schema evolution) produce un metadata file nuevo SIN avanzar secuencia.
- Evidencia observada:

| metadata file | last-seq | current-schema-id | snapshots# | current-snapshot-id | op del último snap | added-records | timestamp-ms (UTC) | mtime local (America/…-05:00) |
|---|---|---|---|---|---|---|---|---|
| 02159 | 2159 | 0 | 2159 | 2698038907058533070 | append | 1501 | 03:39:12 | 09-02 22:39:13 |
| 02160 | 2160 | 0 | 2160 | 2558390325706810625 | append | 1500 | 03:44:44 | 09-02 22:44:44 |
| **02161** | **2160** (=prev) | **1** | **2160** (=prev) | **2558390325706810625** (=prev) | — | — | — | **09-02 22:50:39** |
| 02162 | 2161 | 1 | 2161 | 8929920605883474926 | append | 1 | 03:51:17 | 09-02 22:51:18 |

**Hallazgo decisivo:** el file `02161-8016c77e-e93d-40e1-9eda-4d71a898b20a` tiene el MISMO
`last-sequence-number` (2160), el MISMO `current-snapshot-id` y el MISMO nº de snapshots que
`02160`, pero `current-schema-id` pasa de `0` a `1` y la lista de schemas pasa de
`[schema0: 11 campos]` a `[schema0, schema1(+run_id)]`. **Es un UPDATE DE ESQUEMA metadata-only**
(pyiceberg `update_schema().add_column(...).commit()`), NO un append. No genera snapshot ni datos.

La "versión 2160 ~22:50 del 02-sep" de la traza corresponde exactamente al mtime de este file
`02161` (2026-09-02 22:50:39 local = 03:50:39 UTC).

## §4. Timeline maestro

| Cuando (local -05:00) | Qué ocurrió | Evidencia |
|---|---|---|
| 09-02 21:00-22:44 | Appends normales de producción schema0 (1500-1501 filas/batch, `exchange=kucoin`) | 02159-02160 |
| **09-02 22:50:39** | **Evolución de esquema: ADD COLUMN `run_id` (metadata-only)** | 02161 |
| 09-02 22:51:16 | Append test de **1 fila** en `exchange=test`, `symbol=BTC/USDT`, `run_id="test-run-id-123"` | 02162 + parquet `00000-0-216ad290…` |
| 09-03 00:46–01:32 | Más appends de prueba en `exchange=kucoin`: `1214bcdb…` (uuid), `test-zero-volume`, `test-batch-1500` (1500 filas con run_id de prueba), `debug-test-123` | parquet mtimes + run_id values |
| 09-03 23:30 → 09-06 18:48 | **Producción real con `run_id` UUID por batch** (1500~1501 filas, cadencia ~6-8 min) | parquets: run_id UUIDs (`706d2b18…`, `c1347da3…`) |
| 09-06 18:48 | Última metadata de `bronze.ohlcv` (03161→03315), última escritura | 03315 18:48:34; `catalog.db` mtime 18:48:49 |
| 09-06 18:48:49 | Última metadata `bronze.orderbook_delta` (final del mismo periodo de escritura) | 11874 |
| 09-07 12:12 | Commit `0005eec0` (fix/iceberg-commitfailed) autor `run_id` en `_BRONZE_COLS` ("serialize Iceberg commits"); el 09-09 un pull/pick lo re-aplica como `ddbcd270` en `main` **local** (committer 09-09; árbol distinto: actionlint.yml + pyproject.toml) | git |
| Hoy (09-21) | Host (branch `feat/m22b-governance`, HEAD `857cfe60`, SIN fix) emite 11 cols y falla contra tabla 12 cols; `origin/main` (f4814fb7) también SIN fix | `_BRONZE_COLS` + `bronze_write_error` |

## §5. Evidencia Iceberg detallada (versiones 2159/2160/2161/2162)

- `02159`: append, `added-records=1501`, `total-files=2159`, `ts=1788406752478ms` (03:39:12Z), schema0.
- `02160`: append, `added-records=1500`, `total-files=2160`, `ts=1788407084022ms` (03:44:44Z), schema0.
- `02161`: **sin snapshot nuevo**; `schemas` = `[(0, [timestamp,…,ingestion_ts]), (1, [timestamp,…,ingestion_ts, run_id])]`;
  `current-schema-id=1`; `mtime=2026-09-02 22:50:39.060417` (03:50:39Z).
- `02162`: append `added-records=1`, `ts=1788407477737ms` (03:51:17Z), schema1. Data file
  `data/…/exchange=test/market_type=spot/symbol=BTC%2FUSDT/timeframe=1m/ts_month=2026-09/00000-0-216ad290….parquet`
  con 12 campos (`…, ingestion_ts, run_id`), `run_id='test-run-id-123'`, `ingestion_ts=03:51:15.723157Z`,
  `created_by=parquet-cpp-arrow version 19.0.1`.
- Estado final actual: metadata 03315 (seq 3314, parquet-mtime 09-06 18:48), `current-schema-id=1`,
  recuento acumulado `added-records=7,067,625`, `total-data-files=3838`, último snap
  `4150445708348474244` (append 1500).

## §6. Mecanismo de la evolución

- **`OBSERVATION` (CE-CONFIRMED):** 02161 es un `table.UpdateSchema` con `add_column('run_id')`,
  commiteado contra `data_platform/iceberg_catalog/catalog.db` (SQL catalog). El catalog sqlite solo
  guarda `metadata_location` + `previous_metadata_location` (sin bitácora de actor ni de operación);
  su mtime global es 09-06 18:48 (última escritura), por lo que **el catálogo no fecha ni atribuye** la evolución.
- **`CORRELATION` (CE-HIGH):** 02161 queda a **36 s** del append-test 02162 y a **~6 min** del último
  append normal previo (02160). La secuencia schema-evolución → append-test con `run_id` en el mismo
  minuto indica **una misma sesión manual de desarrollo**, no un comportamiento periódico del pipeline.
- **`CAUSALITY - PARTIAL (indirecta):** el código que acabó escribiendo `run_id` en Bronze se commiteó
  SOLO hasta `ddbcd270` (09-07 12:12, en `main`). No existe ningún commit anterior a 09-07 con
  `run_id` en `_BRONZE_COLS`/`BRONZE_SCHEMA`. Por tanto, la evolución la produjo **código en desarrollo
  (working-tree WIP) que corría en el host**, anterior a su commit, que evolucionó la tabla viva al
  vuelo antes de dejar constancia en git. `NOT DEMONSTRATED` la identidad del actor/script.

## §7. Registro `test-run-id-123`

- Fila: `timestamp=2026-09-02T12:00:00Z`, `exchange=test`, `market_type=spot`, `symbol=BTC/USDT`
  `timeframe=1m`, `ingestion_ts=2026-09-03T03:51:15.723157Z`, `run_id='test-run-id-123'`.
- Firma inequívoca de **write de prueba manual** (exchange inventado `test`, run_id literal).
- No aparece `test-run-id` en ningún archivo del repo (rg vacío): es dato, no código.

## §8. Producción con run_id (09-03 → 09-06)

- A partir de 09-03 00:46-01:32 se observan appends en `exchange=kucoin` con run_id de prueba
  (`test-zero-volume`, `test-batch-1500`, `debug-test-123`), incluido un batch de 1500 filas con
  `test-batch-1500` (09-03 01:23) → el WRITER, no solo un script suelto, ya emitía `run_id`.
- Desde 09-03 23:30 los `run_id` son UUID reales por batch (cadencia ~6-8 min, 1500 filas/batch),
  continuando sin interrupción hasta la última escritura el **09-06 18:48**.
- **`CAUSALITY - DEMONSTRATED (secundaria):** este periodo de escritura fue ejecutado por una versión
  del writer con `run_id` en su camino de escritura, coherente con el contenido posterior de `ddbcd270`.

## §9. Cese de escritura (09-06 18:48)

- Última metadata `03315-afb8ee9a…` mtime 09-06 18:48:34; `catalog.db` mtime 09-06 18:48:49;
  `bronze.orderbook_delta` último mtime 09-06 18:48:49. Todos **cesan en la misma ventana**.
- `NOT DEMONSTRATED` la causa del cese (parada manual, reinicio, fallo de otra capa). No hay logs
  locales previos a 09-14 (retención de `logs/app_*.log*` empieza en 09-14) para correlacionar.

## §10. Drift esquema vs rama activa (la causa del fallo actual)

- Tabla viva: `current-schema-id=1` → **12 columnas** (incluye `run_id`).
- `_BRONZE_COLS` en HEAD/branch activa `feat/m22b-governance` (`857cfe60`): **11 columnas sin `run_id`**.
- Writer hoy: `prepared.to_arrow().cast(self._table.schema().as_arrow())` (`bronze_storage.py:182`)
  → `PARQUET`: "Target schema's field names are not matching…" → `bronze_write_error` (3407 hoy).
- `ddbcd270` (que añade `run_id` a `_BRONZE_COLS`) pertenece a `main`; **NO es ancestro de HEAD actual**
  (`git merge-base --is-ancestor ddbcd270 HEAD` = NO). La rama activa quedó desacoplada del fix.
- **`CAUSALITY - DEMONSTRATED`:** drift `run_id` = causa directa del fallo de escritura actual.

## §11. Forense git

- `BRONZE_SCHEMA` (schemas.py) **nunca** contuvo `run_id` en ninguna revisión (solo `GOLD_SCHEMA`).
- `_BRONZE_COLS` con `run_id`: únicamente `0005eec0` (09-07 12:12, rama `fix/iceberg-commitfailed`) y su
  cherry-pick local `ddbcd270` (committer 09-09, `main` local). Ambos con el mismo mensaje/fecha de autor pero
  **árboles distintos** (diff `ddbcd270` vs `0005eec0`: actionlint.yml + pyproject.toml).
- **`ddbcd270` NO existe en `origin/main`** (`git merge-base --is-ancestor` = NO). `origin/main` (f4814fb7) y
  `feat/m22b-governance` (857cfe60) tienen `_BRONZE_COLS` = 11 cols sin `run_id`. Local `main` (7e693fa6) está
  8 ahead / 47 behind de `origin/main`. El fix quedó **solo en una rama local y en una rama fix nunca fusionada**.
- HEAD activo `857cfe60` (feat/m22b-governance): no contiene `ddbcd270`; `git status` limpio →
  el writer actual es exactamente el del HEAD.
- Stashes: ninguno con `run_id` en `bronze_storage.py`. Worktrees `/tmp/*` prunable, sin esos archivos.
- No existen otros `bronze_storage.py` en disco (`find /tmp /home/...` vacío).
- No hay commits tocando Bronze/schemas entre 08-25 y 09-05 en ninguna rama.
- **Conclusión:** la evolución corrió con **código WIP no commiteado** anterior a `ddbcd270`.

## §12. Catalogo SQLite y actores

- `iceberg_tables(catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location)`:
  `bronze.ohlcv` → current `03315-afb8ee9a…`, previous `03314-64d3e932…`.
  No almacena usuario, ni timestamp de operación, ni historial completo de versions.
- `iceberg_namespace_properties`: `(ocm, bronze|silver|gold, exists, true)`.
- Por tanto el catálogo **no permite atribuir actor** (humano/script/proceso). Se usa solo como
  corroboración del estado final (schema-id 1 en current).

## §13. Historial de shell (ventana 22:39–22:51 local 09-02)

- `$HOME/.zsh_history` filtrado a `epoch∈[1788406000,1788425000]` (≈ 09-03 03:26:40Z–08:43:20Z):
  **sin entradas** en esa ventana.
- `CONTRASTADO`: no hay comando CLI registrado (ni `python`, `ALTER`, `update_schema`, `sqlite3`,
  `kafka-*`) en ese intervalo. → La operación se ejecutó programáticamente (proceso/Python ya en
  ejecución o lanzado sin dejar traza de shell persistente en este historial). `NOT DEMONSTRATED`.

## §14. Otras tablas Bronze (`run_id` endógeno)

- `bronze.orderbook_delta` y `bronze.orderbook_snapshot`: **desde su creación (09-04 17:44 / 17:40 local) ya traen
  `run_id` en schema 0** (primera metadata `00000-2529f15c…` mtime 09-04 17:44, `00000-61ed7112…` mtime 09-04 17:40).
  → Son tablas nuevas creadas con el código WIP (con `run_id`); su schema nunca "evolucionó" (current-schema-id=0,
  si bien `orderbook_snapshot` cesa de escribir 09-05 23:35 y `orderbook_delta` sigue hasta 09-06 18:48).
- Implicación: `run_id` es un **patrón coherente del WIP** aplicado a las tablas nuevas desde el
  principio, y a `ohlcv` como evolución del esquema existente en 09-02 22:50.

## §15. Independencia OrderBook — OB-SNAPSHOT-01 / OB-RESYNC-01

- `rg` en todo el repo: **NO existe** `OrderBookSnapshotFetcher`, `fetch_snapshot`, `fetch_order_book_snapshot`.
  PR-A no está implementado en código. Solo existe documentación de diseño.
- `BookBuilderConsumer` (`book_builder_consumer.py`): stream processor `orderbook.raw → book.snapshot/book.delta`,
  sin relación con OHLCV ni con `run_id` (provenance OHLCV). No consume `ohlcv.*`, no usa `run_id`.
- Cross-dependencia según `AUDIT_OCM_orderbook-resync-causality-audit_2026-09-21.md`: "No dependency OHLCV vs OrderBook
  (different Kafka topics, different Bronze)"; OrderBook **no** afecta al dataset OHLCV Bronze.
- `bronze.ohlcv` (run_id drift) NO toca las tablas de orderbook y viceversa.
- **Veredicto independencia:** `CONFIRMED` — ambos son independientes de OHLCV Recovery y del drift `run_id`.
  Condiciones de ese resultado:
  - `OB-SNAPSHOT-01` (PR-A): requiere implementación nueva (`OrderBookSnapshotFetcher` REST Bybit limit 50);
    su dependencia es el contrato `OrderBookSnapshotPayload` y el acceso REST Bybit, **no** OHLCV/run_id.
  - `OB-RESYNC-01` (PR-B): depende de PR-A (fetcher) + continuidad `u` de Bybit + dataset orderbook; **no**
    de OHLCV/run_id.

## §16. Causalidad (síntesis)

| Cadena | Estado |
|---|---|
| ¿02161 evolucionó el esquema a `run_id`? | OBSERVATION — CONFIRMED (schemas, mtime, misma seq/snapshot) |
| ¿La evolución es metadata-only (sin datos)? | CONFIRMED (02161 sin snapshot nuevo) |
| ¿Quién la hizo? | CORRELATION high (sesión manual de test con `run_id`) — actor NOT DEMONSTRATED |
| ¿El writer de producción escribió `run_id` desde 09-03? | CONFIRMED (parquets con UUID/batch) |
| ¿El código WIP se commiteó después (0005eec0 09-07 → cherry-pick local ddbcd270)? | CONFIRMED |
| ¿La evolución causa el fallo actual del writer (11 vs 12)? | CAUSALITY — DEMONSTRATED |
| ¿Ese fix llegó a rama remota/activa? | REFUTADO — NO está en `origin/main` ni en `857cfe60`

## §17. Impacto en Master Plan (INFORMATIVO, sin editar el master)

Relación con `AUDIT_OCM_master-market-data-implementation-order_2026-09-21.md`:

- **MD-INFRA-01 (runtime correctness):** IMPACTADO — la corrección del runtime debe alinear rama activa y esquema
  (el fix `0005eec0`/`ddbcd270` vive solo en `main` local y en `fix/iceberg-commitfailed`; ausente en
  `feat/m22b-governance` y en `origin/main`).
- **MD-INFRA-02 (Kafka/Bronze operational, MD-BRONZE-01):** BLOQUEADO por el drift (causa del `bronze_write_error`).
  Reconciliar 11 vs 12 columnas (o portar `ddbcd270`) es prerrequisito.
- **MD-DATASET-01 (dataset observable):** CONDICIONAL — sin Bronze operativo, dataset no se actualiza.
- **OH-RECOVERY-01 (recovery contracts):** UNCHANGED — independiente de `run_id`.
- **OB-SNAPSHOT-01 (PR-A):** INDEPENDIENTE de OHLCV/run_id; **implementación ausente** en código (solo diseño).
- **OB-RESYNC-01 (PR-B):** INDEPENDIENTE de OHLCV/run_id; depende de PR-A + contrato `u`.
- **NUEVA observación (no modifica master):** el drift `run_id` es un hallazgo de data-governance
  (esquema de tabla viva sin historial reproducible de versión) → candidato a work-item de gobernanza.

## §18. Respuestas Q1–Q24

1. **Q1 ¿Existe realmente "Iceberg snapshot/versión 2160"?** PARTIAL — existe metadata file `02161` con
   `last-sequence-number=2160` heredado (metadata-only). No es un snapshot nuevo; el snapshot current de 02160 es idéntico.
2. **Q2 ¿La evolución de esquema fue un append?** NO — fue un update metadata-only (sin snapshot, sin datos).
3. **Q3 ¿Timestamp exacto?** 2026-09-02 22:50:39 local (−05:00) = 03:50:39Z (mtime `02161`).
4. **Q4 ¿Qué campos añadió el schema 1?** `run_id` (StringType, required=False) tras `ingestion_ts`; 11→12 campos.
5. **Q5 ¿Qué snapshot era current en ese instante?** `2558390325706810625` (append seq 2160, 03:44:44Z).
6. **Q6 ¿Qué escribió primero con run_id?** el append-test `02162` (`exchange=test`, 1 fila, `run_id='test-run-id-123'`).
7. **Q7 ¿Era producción?** NO — exchange `test` + run_id literal: write de prueba manual.
8. **Q8 ¿Qué código la escribió?** WIP del writer con `run_id` (commiteado después como `0005eec0`/`ddbcd270`); identidad exacta NOT DEMONSTRATED.
9. **Q9 ¿Cuándo arrancó producción con run_id?** 09-03 00:46–01:32 (pruebas), producción real (UUIDs) desde 09-03 23:30.
10. **Q10 ¿El periodo 09-03 00:46–01:32 tiene test ids?** CONFIRMED (`test-zero-volume`, `test-batch-1500`, `debug-test-123`).
11. **Q11 ¿Hasta cuándo siguió la producción con run_id?** hasta 09-06 18:48 (última metadata 03315).
12. **Q12 ¿run_id estuvo en `_BRONZE_COLS`/BRONZE_SCHEMA antes de 09-07?** NO en git (solo GOLD_SCHEMA; bronze desde `ddbcd270`).
13. **Q13 ¿Qué commit introdujo run_id en `_BRONZE_COLS`?** `0005eec0` (09-07 12:12, rama `fix/iceberg-commitfailed`); luego su cherry-pick `ddbcd270` en `main` local (committer 09-09). Aclaración adversarial: `ddbcd270` ≠ `0005eec0` (árboles distintos); **ninguno de los dos está en `origin/main`**.
14. **Q14 ¿Está ese commit en la rama activa?** NO — HEAD `857cfe60` (feat/m22b-governance) no lo contiene.
15. **Q15 ¿Existe código capaz de `update_schema`/`add_column` en el repo actual?** NO (rg vacío en packages/shared/apps).
16. **Q16 ¿Quién ejecutó la evolución?** NOT DEMONSTRATED — sesión WIP local; sin bitácora de catálogo ni shell.
17. **Q17 ¿Hay comandos shell en la ventana?** NO en `$HOME/.zsh_history` (ventana 1788406000–1788425000).
18. **Q18 ¿El catálogo SQLite registra el cambio?** Solo current/previous `metadata_location`; sin historial de versiones ni actor.
19. **Q19 ¿El drift afecta a otras tablas?** NO aplica igual — orderbook_delta/snapshot nacieron ya con run_id (endógeno).
20. **Q20 ¿Cuándo cesó la escritura en Bronze?** 09-06 18:48 (ohlcv 18:48:34, orderbook_delta 18:48:49, catalog 18:48:49).
21. **Q21 ¿El runtime actual (host) escribe 11 o 12 columnas?** 11 (sin `run_id`) en la rama activa `857cfe60`.
22. **Q22 ¿El `bronze_write_error` actual es consecuencia directa del drift?** SÍ — `to_arrow().cast(schema 12)` vs df 11 → mismatch.
23. **Q23 ¿OB-SNAPSHOT-01 es independiente de OHLCV/run_id?** CONFIRMED — sí (tipo de dato, topics y almacenes distintos; fetcher aún no implementado).
24. **Q24 ¿OB-RESYNC-01 es independiente de OHLCV/run_id?** CONFIRMED — sí; dependencias propias PR-A + contrato `u`.

## §19. Matriz de evidencia

| # | Evidencia | Fuente | Estado | Causalidad |
|---|---|---|---|---|
| E1 | 02161 = metadata-only, schema 0→1, run_id añadido | metadata JSON 02161-8016c77e | CONFIRMED | OBSERVATION |
| E2 | mtime 02161 = 09-02 22:50:39 local | fs mtime | CONFIRMED | OBSERVATION |
| E3 | append-test 02162 con `run_id='test-run-id-123'` en `exchange=test` a 36s | parquet + metadata 02162 | CONFIRMED | CORRELATION (test manual) |
| E4 | producción con UUIDs 09-03 23:30→09-06 18:48 | parquets kucoin run_id | CONFIRMED | CORRELATION |
| E5 | `_BRONZE_COLS` con run_id solo desde `0005eec0`/`ddbcd270` (09-07; main local / fix branch) | git | CONFIRMED | CORRELATION |
| E5b | `ddbcd270` NO está en `origin/main`; origin/main (f4814fb7) y 857cfe60 → `_BRONZE_COLS`=11 cols | git `merge-base` + árbol | CONFIRMED | (desmiente propagación del fix) |
| E6 | HEAD activo no contiene `ddbcd270`; `_BRONZE_COLS` 11 cols | git + worktree | CONFIRMED | CAUSALITY (del fallo actual) |
| E7 | writer actual hace cast a tabla 12 cols | `bronze_storage.py:182` | CONFIRMED | CAUSALITY — DEMONSTRATED |
| E8 | catálogo sqlcatalog no guarda historial/actor | catalog.db schema | CONFIRMED | — |
| E9 | sin shell history en ventana | .zsh_history | CONFIRMED (ausencia) | NOT DEMONSTRATED |
| E10 | orderbook tables con run_id desde creación | metadata 00000 orderbook | CONFIRMED | CORRELATION (patrón WIP) |
| E11 | OB-SNAPSHOT-01/OB-RESYNC-01 sin dependencia OHLCV | código + OB-RESYNC-001 | CONFIRMED | INDEPENDENCE |
| E12 | `OrderBookSnapshotFetcher` no existe en código | rg repo | CONFIRMED | — (implementación pendiente) |
| E13 | última escritura 09-06 18:48 (drift run_id no detuvo escritura hasta esa fecha) | metadata 03315 | CONFIRMED | — |
| E14 | sin logs locales previos a 09-14 | logs/app_*.log* | CONFIRMED | limitación |

## §20. Contradicciones / ajustes vs auditorías previas

- La reconciliación previa decía "run_id necesita fix": ahora se precisa **CAUSALIDAD**: drift = evolución
  manual WIP 09-02 22:50 commiteada tarde (0005eec0 09-07 / ddbcd270 main-local 09-09) y no propagada a rama
  activa ni a `origin/main`. No fue "magia" ni el pipeline.
- El "fallo desde 09-02" a corregir: matiz — el writer con `run_id` (WIP) escribió OK hasta 09-06 18:48;
  el fallo actual (11 cols) aparece al correr la rama SIN `ddbcd270` hoy. La ventana de fallo es el runtime activo actual,
  no todo el periodo post-09-02.

## §22. Revisión adversarial del informe (refutación BIG PICKLE)

Objetivo: intentar REFUTAR las afirmaciones/causalidad del informe. Ejecutado read-only (schema de catálogo no
alterado; solo lecturas de metadata JSON, árboles git, reflog, `git log -S/-G`, `git fsck`, parquets y sqlite).
Resultado: **ninguna afirmación central fue refutada**; dos precisiones de hecho y una corrección cronológica.

### §22.1 Clasificación por flecha de causalidad

Cadena `WIP → EVOLUCIÓN TABLA → ESCRITURAS run_id → COMMIT TARDÍO → NO PROPAGADO → 11 cols → 12 cols → FALLO`:

| Flecha | Estado | Justificación adversarial |
|---|---|---|
| A. WIP LOCAL → EVOLUCIÓN (02161, 09-02 22:50) | NOT DEMONSTRATED (actor) | La evolución existió (CONFIRMED metadata-only); el actor es WIP humano/manual por exclusión (sin shell, sin commit previo, sin `update_schema` en repo) — correlación, no causalidad demostrada. MARKET: existe evidencia negativa fuerte (rg −G update_schema/add_column vacío en HEAD y en toda la historia, incluidos fsck/dangling/stashes) pero sin bitácora. |
| B. EVOLUCIÓN → ESCRITURAS con `run_id` (09-03 23:30→09-06) | CONFIRMED | El writer (no un script suelto) emitió `run_id` (batch 1500 con `test-batch-1500`); 36s tras 02161 está el write test `test-run-id-123`. |
| C. WIP → COMMIT TARDÍO (0005eec0/ddbcd270) | CONFIRMED | Commit autor-09-07 12:12 en fix/iceberg-commitfailed; `ddbcd270` = cherry-pick del 09-09 en `main` local (author=committer ≠). No hay commit *anterior* a 09-07 con `run_id` en `_BRONZE_COLS` (git log -S en toda la historia: solo GOLD_SCHEMA). |
| D. COMMIT → NO PROPAGADO a rama activa | CONFIRMED | `merge-base --is-ancestor ddbcd270 857cfe60` = NO. `origin/main` (f4814fb7) y 857cfe60: `_BRONZE_COLS` = 11 cols. Fix solo en `main` local y en la rama fix nunca fusionada. |
| E. 11 cols (código) vs 12 cols (tabla viva) → FALLO | CONFIRMED — CAUSALITY | `bronze_storage.py:182` cast `.to_arrow().cast(table.schema().as_arrow())`; pyarrow error "Target schema's field names are not matching"; `bronze_write_error` (3407 hoy). Es el mecanismo directo. |

Ninguna flecha se clasificó como REFUTED. La única degradación posible respecto al informe original es la
flecha A: "actor = WIP local" se mantiene NOT DEMONSTRATED (correlación de exclusión), no CONFIRMED.

### §22.2 Procedencia git de `ddbcd270` (corrección)

- `ddbcd270e20abc6a…` : `P=9314217ecb92…`, author 2026-09-07 12:12:17 −0500, committer **2026-09-09 00:07:28 −0500**,
  tree=8869dbe… (rama `main` local 7e693fa6, **no** en `origin/main`).
- `0005eec0be111f1d…` : `P=2d241560cf81…`, author 2026-09-07 12:12:17, committer **2026-09-07 12:12:17** (rama
  `fix/iceberg-commitfailed` + `remotes/origin/fix/iceberg-commitfailed`), tree=e5a695308…
- `diff ddbcd270 0005eec0`: actionlint.yml + pyproject.toml → **mismo cambio de fondo, árboles distintos, historias
  distintas**. La afirmación "ddbcd270 (=0005eec0) en main" del §4/§19 se corrige aquí: son dos commits del mismo
  work, en ramas distintas, y el que quedó en `main` es un cherry-pick local nunca publicado.
- El commit que "tradujo" el fix a la rama main-local llegó junto a `7e693fa6/bb0b557f/fbc772f6/…` (pull/pick 09-09,
  reflog `refs/heads/main@{...}`), todos apuntando a la secuencia posterior a `caefa0db`.

### §22.3 Tabla Iceberg viva vs código (refutación: ¿el esquema nació con run_id?)

- **REFUTADO que la tabla siempre tuvo `run_id`:** metadata `00000-efd2eaa4…` (mtime 2026-08-20 23:10 local,
  `current-schema-id=0`, `last-seq=0`, 0 snapshots) tiene **11 campos SIN `run_id`** ([timestamp, open, high, low,
  close, volume, exchange, market_type, symbol, timeframe, ingestion_ts]). La tabla nació sin run_id; `run_id`
  (schema 1) entra SOLO en 02161 (09-02 22:50:39). Tabla ≠ código se demuestra: el primer metadata y el código
  previo a 09-07 coinciden en 11 cols; la divergencia es posterior y exclusivamente introducida por 02161.
- Estado final vivo (03315): `current-schema-id=1`, 12 cols con `run_id`, `last-seq=3314`, snapshots 3310-3314.
- `BRONZE_SCHEMA` (schemas.py) nunca tuvo `run_id` (solo `GOLD_SCHEMA`): confirmado con `git log -S"run_id"` sobre
  schemas.py en toda la historia (commits 09f6419a/dc35188c añaden run_id a gold, ninguno a bronze).

### §22.4 Writer (refutación: ¿el writer actual podría escribir 12 cols?)

- `_BRONZE_COLS` en 857cfe60 y en origin/main: **11 sin run_id** (grep del árbol de cada ref).
- `append()` (línea 116) → `prepared.to_arrow().cast(self._table.schema().as_arrow())` (línea 182). No hay
  `update_schema`/`add_column`/auto-evolución de esquema en ningún commit (git log -S en packages/shared/apps:
  vacío; fsck/stash: sin rastro). El writer **no puede** solucionarse solo: sin contrato de compatibilidad, el
  cast falla silenciosamente al primer desajuste de campo.

### §22.5 Provenance vs decisión arquitectónica

- El cambio de producir `run_id` NO fue una decisión arquitectónica con ADR: es evolución de esquema viva manual
  (WIP), sin gobernanza de migración ni contrato de compatibilidad código↔esquema.
- Separación INCIDENTE vs DECISION: el incidente (drift) y el trabajo pendiente (ADR candidates A-D) son
  independientes: el incidente expone la brecha, no la cubre.

### §22.6 ADR candidates A-D (evaluación)

| Candidato | Veredicto | Razonamiento |
|---|---|---|
| A. Iceberg schema evolution governance | **MERGE (con B) / DEFER standalone** | A solo no añade valor: la gobernanza de evolución de esquema YA es parte viva de la herramienta estándar (update_schema). Lo que faltó es el contrato; fusionar A dentro de B. |
| B. Code/schema compatibility contract | **KEEP** | Es la decisión mínima que evita el fallo: validar/castear el writer contra el esquema de tabla diana (o contrato versionado) ANTES del append. Da cobertura directa a `bronze_storage.py:182`. |
| C. Provenance `run_id` ownership | **KEEP** | Define SSOT de `run_id` (formato UUID, ingestión, cardinalidad), desacoplado del mecanismo de escritura. Complemento de B (qué es el dato) vs mecanismo (cómo se escribe). |
| D. Production data lake schema migration policy | **MERGE (Dentro de B) / DEFER** | D sin B es proceso sin gobernanza de compatibilidad; la política de migraciones en producción se resuelve con el contrato B + tests de compatibilidad. D como work-item de proceso (opcional), no como ADR de arquitectura. |

Síntesis: **una sola decisión arquitectónica real** = "contrato de compatibilidad código↔esquema Iceberg para escritura
Bronze" (B), con A y D absorbidos, y una decisión de datos separada = "ownership/provenance de `run_id`" (C).

### §22.7 Relación con candidatos ADR de reconciliación (Nemotron)

- "Bronze / idempotency / dedup" y "Provenance / run_id": **NO son la misma decisión que B**.
  - B (compatibilidad) responde a "¿cómo puede evolucionar el esquema sin romper el writer?".
  - Provenance/run_id responde a "¿qué identifica una escritura y qué trazabilidad exige?".
  - Idempotency/dedup (Bronze) responde a "¿cómo se evita duplicado ante re-intentos?" — usa `run_id`, pero no es
    la evolución de esquema.
  - Lo correcto: evolución de esquema NO debe involucrar el motor (pipeline/writer) en la creación del esquema; debe
    ir por contrato (B). Schema evolution es **implementation-governance del contrato**, no un ADR de dominio Bronce.
- Conclusión para composición final: A+D ∈ B (un ADR), C separado (datos), idempotency/provenance como candidatos
  ya existentes del reconciliador (independientes entre sí y de B). Nada nuevo de dominio.

### §22.8 SAFOPS adversarial

```
REVIEW        : adversarial refutation — READ ONLY. sin modificar catálogo/ESQUEMA/ICEberg (291 metadata files intactos).
RESULT        : 0 afirmaciones refutadas. Section 22.5/22.6: B KEEP, C KEEP, A=MERGE→B, D=MERGE→B (o DEFER).
CORRECCIONES  : ddbcd270 ≠ 0005eec0 (árboles y ramas distintas; ddbcd270 no está en origin/main); orderbook_snapshot sch0 run_id desde 09-04 17:40 y orderbook_delta 09-04 17:44 (no 09-05).
CAUSALITY     : flechas B,C,D,E CONFIRMED; flecha A actor NOT DEMONSTRATED (exclusión) — no refutado.
STOP
```

## §21. SAFOPS FINAL

```
OB-SNAPSHOT-01 : INDEPENDENT of OHLCV Recovery/run_id. Implementation NOT PRESENT in repo (REST Bybit fetcher, limit 50) — diseño/documentación solamente.
OB-RESYNC-01   : INDEPENDENT of OHLCV Recovery/run_id. Depends on PR-A fetcher + Bybit u-continuity + orderbook dataset. Implementation NOT PRESENT.
RUN_ID DRIFT   : CONFIRMED — causado por evolución de esquema metadata-only (02161, 2026-09-02 22:50:39 local) con código WIP anterior a 0005eec0/ddbcd270.
                 CAUSALITY root cause of current bronze_write_error (writer 11 cols vs tabla 12 cols) en la rama activa 857cfe60 (sin fix).
                 ADVERSARIAL: el fix NO está en origin/main (f4814fb7) ni en 857cfe60; solo en main local (ddbcd270) y fix/iceberg-commitfailed (0005eec0). ddbcd270 ≠ 0005eec0.
CAUSALITY      : For the failure today — DEMONSTRATED. Actor identity of the 09-02 schema evolution — NOT DEMONSTRATED (forzado por ausencia de bitácora en catálogo/shell).
MASTER PLAN    : NOT MODIFIED. Impact flags: MD-INFRA-02 BLOCKED (drift), MD-INFRA-01 impacted (branch alignment), OH-RECOVERY/OB-* UNCHANGED, OB-* INDEPENDENT.
FINAL STATUS   : The Bronze OHLCV run_id drift origin is identified (manual/WIP schema evolution, committed late, not propagated to active branch).
                 Recommendations (NOT executed, next safe read-only steps): review branch alignment to main for ddbcd270; confirm no data relies on 11-col casts; audit WIP-era processes.
DOCUMENTATION  : docs/audits/AUDIT_OCM_iceberg-bronze-ohlcv-runid-provenance_2026-09-21.md
STOP
```

## §23. Consolidación ADR-0041 (registro de reconciliación, 2026-09-21)

Reconciliación de ADR-0041 (Propuesto, 2026-09-21, UNIX `??` sin commitear) frente a
los candidatos A/B/C/D y del incidente `11 vs 12`. Solo lectura salvo este registro.

### §23.1 Auditoría de ADR-0041 (cubre / no cubre)

- **Qué decisión toma** (L55-66): adoptar un contrato canónico de Market Data
  (Dataset × Exchange × Transport × Purpose × Identidad de evento × Deduplicación),
  incluida estructura Kafka y "Bronze/Silver/Gold: esquema canónico y evolución de schema" (L66).
- **Qué problema resuelve**: integración ad hoc entre datasets/transports/exchanges;
  cita el drift `11 vs 12`, run_id (L14) y la independencia de OrderBook (L15).
- **Alcance (L101-102)**: Dentro = definición del contrato, identidades de evento,
  dedup, topics Kafka, esquemas Bronze/Silver/Gold. Fuera = adapters REST/WS,
  migración de datos, **corrección de schema drift en Bronze (tarea separada)**,
  recuperación de gaps (L102).
- **Validación runtime (L117)**: SOLO "**Future**: validación runtime del contrato;
  evolución de schema controlada". No es una cláusula de decisión: es backlog.
- **Coverage writer → schema Iceberg (columnas/run_id/fail-fast)**: NO cubre.
  No define: (a) verificación de compatibilidad de columnas antes de persistir,
  (b) presencia/ausencia de `run_id`, (c) fail-fast pre-append, (d) ownership/SSOT
  del schema de tabla, (e) detección/enforcement de schema drift. El código confirma
  la brecha: `REQUIRED_COLUMNS` = solo OHLCV básico; `_BRONZE_COLS` = 11 sin `run_id`;
  `append()` castea en `:182` con `except → BronzeWriteError` (post-fallo, no fail-fast).

**Conclusión:** ADR-0041 cubre el *contrato* (nivel de decisión) pero NO la *frontera de
enforcement* (B). La brecha exacta = aritmética de columnas + presencia `run_id` +
fail-fast pre-persist + ownership del schema. Sin estos, el drift puede reaparecer.

### §23.2 Reconciliación A/B/D

| Candidato | Problema | Decisión arquitectónica | Evidencia | Relación con ADR-0041 | Resultado |
|---|---|---|---|---|---|
| A (schema evolution governance) | 02161 metadata-only sin bitácora; actor NOT DEMONSTRATED (E8/E9/E1) | quién/por-qué proceso puede evolucionar el schema de tabla viva; parte arquitectónica = gate de compatibilidad previo | E8 catálogo sin historial; E9 sin shell; Q15 no existe update_schema en repo | L66 (evolución de schema) + L117 (Future) — proceso sin gate no previene drift | **A → B VALIDATED** (parte arquitectónica); componente organizativo = gobernanza/incidente |
| B (code/schema compatibility contract) | fallo actual 11 vs 12, CAUSALITY-DEMONSTRATED (E6/E7) | enforcement verificable de compatibilidad (columnas/run_id/fail-fast) antes de persistir | :182 cast → BronzeWriteError; E5b fix ausente en origin/main+857cfe60 | L117 "validación runtime del contrato" = la declaración de B; B = su frontera de implementación | **B → ADR-0041 VALIDATED** (como cláusula de enforcement; requiere revisión de ADR-0041) |
| D (producción migration policy) | 02161 aplicada a tabla viva sin política | mecanismo (B) + proceso operativo (política); el enforcement es B | E1/E2 mtime 09-02 22:50; Q16 actor NOT DEMONSTRATED | Sin B la política no se puede hacer cumplir | **D → B VALIDATED** (enforcement); resto de política = proceso, no ADR |

**Estructura validada:** A→B→ADR-0041 y D→B→ADR-0041. Corregiría mi §22.6 anterior
(B KEEP standalone): ADR-0041 (L117) declare "validación runtime del contrato", por lo
que B no requiere ADR propio; requiere que ADR-0041 pase esa cláusula de Future a Decisión
con requisitos de enforcement (fail-fast, columnas, run_id, ownership).

### §23.3 Incidente vs ADR

- **Incidente ("11 vs 12")**: incident + technical debt. Corrección = tarea plan
  (MD-INFRA-01/02), NO ADR.
- **Finding de auditoría**: drift run_id origin (02161) + fix no propagado (E5b).
- **Decisión arquitectónica**: contrato canónico + enforcement (ADR-0041).
- **Implementación futura**: enforcement de compatibilidad en writer; evolución de schema controlada.
- **Evidencia histórica**: 02161/02162, parquets, timeline §4.
- **Documentación**: este informe (§0-§23).
- **ATRIBUCIÓN**: NOT DEMONSTRATED (no convertir §6/§7 "WIP por exclusión" en hecho;
  es OBSERVATION + CORRELATION, no CAUSALITY de actor).