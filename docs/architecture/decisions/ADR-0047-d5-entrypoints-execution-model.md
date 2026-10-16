# ADR-0047: Execution Model & Entrypoints (D5)

**Estado:** ACEPTADO CON ACLARACIONES (G4.3 — 2026-09-28)
**Decisión humana asociada:** D5 = DECIDED — HUMAN — 2026-09-28 + G4.3 = ACCEPT WITH CLARIFICATIONS — 2026-09-28
**Materialización:** REALIZADA VÍA D5 — PENDIENTE DE HUMAN REVIEW (RESUELTO: ACEPTADO CON ACLARACIONES, G4.3 — 2026-09-28)
**Fecha:** 2026-09-28
**Bounded context(s) afectado(s):** ocm (plataforma), market_data, trading, portfolio, apps, shared, CI/CD

## 1. Contexto

OCM no tenía una decisión arquitectónica formal sobre su modelo de ejecución ni sobre
la frontera PUBLIC/INTERNAL de sus entrypoints. Existía un conjunto de hechos verificados
en el repositorio:

- `[project.scripts]` en `pyproject.toml` registra cinco entrypoints: `ocm`
  (`app.cli.main:main`), `ocm-api` (`api.main:serve`), `live`
  (`app.cli.live_hydra:main`), `paper` (`app.cli.paper_hydra:main`), `streaming`
  (`app.cli.streaming_hydra:main`).
- `app.cli.main` es un CLI batch Hydra de una pasada, usado por CI para validación
  (`OCM_VALIDATE_ONLY=true uv run python -m app.cli.main`).
- `market_data.main` es un service entrypoint (FastAPI en `:8001`, ingestion loop,
  Kafka → Iceberg) invocado por Dockerfile CMD (`python -m market_data.main`),
  `docker-compose.yml` (`command: python -m market_data.main`) y systemd
  (`ocm-market-data.service`: `python -m market_data.main`). **No está registrado en
  `[project.scripts]` ni en `run.sh`.**
- `trading` y `portfolio` son bounded contexts Nivel 2 (ADR-0024): componentes
  EMBEDDED ejecutados via `live`/`paper` → `app.cli.live_hydra`/`app.cli.paper_hydra`
  → `execute_live.py`/`execute_paper.py`. No existe `trading/main.py` ni
  `portfolio/main.py`.
- `docker-compose.yml` referencia `python -m trading.main` y `python -m portfolio.main`,
  módulos inexistentes (comentarios del propio Compose advierten "NO existe trading.main").
- `run.sh` expone solo `ocm | live | paper`, con cabecera que declara "SSOT: los
  entrypoints aquí deben ser idénticos a [project.scripts]" — contradicción factual
  (faltan `ocm-api` y `streaming`).
- `ocm-streaming.service` (systemd) ejecuta `python -m app.cli.streaming_hydra`.
- No existe un modo backtest de trading en OCM: solo `scripts/backtest_app_guard.py`,
  un gate de CI sobre apps, no un modo de ejecución.
- ADR-0024 define los niveles de madurez de bounded contexts; ADR-0039 establece
  systemd como runtime canónico de aplicaciones y Docker/Compose como infraestructura.
- Las decisiones humanas H1/H2/H3 (ADR-0045, ADR-0046) ya están materializadas:
  modelo de autoridad CUSTOM/HYBRID, H2-E fail-closed + human gate, y contrato de
  materialización de decisiones (DECIDED ≠ MATERIALIZED ≠ ACCEPTED).

## 2. Problema

Sin una decisión formal, los artefactos de deployment (Docker, Docker Compose,
systemd, `run.sh`, CI) actuaban como fuentes de autoridad implícitas sobre qué se
puede ejecutar: la mera existencia de un módulo, un `main.py`, una invocación
`python -m`, una referencia de infraestructura o de tests podía interpretarse como
"entrypoint público". Esto genera ambigüedad sobre la frontera de ejecución, sobre
el rol de `app.cli.main` vs `market_data.main`, sobre qué es un entrypoint público
de Market Data, y sobre cuál es la fuente normativa de los entrypoints.

El humano decidió (D5, 2026-09-28) resolver estas cuatro preguntas y fijar el modelo
de ejecución de OCM.

## 3. Decisión

OCM adopta el siguiente modelo arquitectónico de ejecución:

```
                     OCM
                      │
                MISMO DOMINIO
                      │
          ┌───────────┼───────────┐
          ▼           ▼           ▼
      BACKTEST       LIVE        PAPER
          │           │           │
          └───────────┼───────────┘
                      ▼
                 APPLICATION
                      ▼
                   DOMAIN
                      │
             ┌────────┴────────┐
             ▼                 ▼
          trading          portfolio
          embedded          embedded
                market_data
                     │
                     ▼
                  SERVICE
                     │
                     ▼
                Kafka/Iceberg
```

Interpretación normativa:

1. BACKTEST, LIVE y PAPER son modos de ejecución del mismo dominio/aplicación OCM.
2. BACKTEST es parte del modelo arquitectónico objetivo, pero actualmente NO está
   implementado. El ADR distingue decisión arquitectónica, capacidad objetivo, y
   estado actual de implementación.
3. LIVE y PAPER existen como modos/runtimes actuales y deben conservarse.
4. `trading` permanece como BC Nivel 2 y componente EMBEDDED.
5. `portfolio` permanece como BC Nivel 2 y componente EMBEDDED.
6. `market_data` es BC Nivel 1 y posee un RUNTIME/SERVICE independiente.
7. No se establece que cada BC necesite un proceso independiente.
8. Invariantes: **BC ≠ PROCESS**, **BC ≠ ENTRYPOINT**, **MODE ≠ BC**,
   **DEPLOYMENT MECHANISM ≠ ARCHITECTURE**.
9. Docker, Docker Compose, systemd, `run.sh` y CI son CONSUMIDORES del execution
   contract.
10. Estos mecanismos NO constituyen por sí mismos la autoridad arquitectónica.
11. `trading.main` y `portfolio.main` NO deben crearse como consecuencia de esta
    decisión.
12. La discrepancia actual de docker-compose respecto a `trading.main`/`portfolio.main`
    queda registrada como deuda/discrepancia pendiente de resolución posterior.

## 4. D5-01 — PUBLIC vs INTERNAL (Decisión: D5-01 = OPCIÓN D)

"PUBLIC" significa que el modo/entrypoint está explícitamente registrado en el
execution contract.

La mera existencia de:

- un módulo Python;
- un archivo `main.py`;
- una función `main()`;
- una invocación `python -m`;
- una referencia desde Docker/systemd;
- una referencia desde tests;

NO convierte automáticamente ese elemento en un public entrypoint.

El execution contract es la frontera conceptual entre PUBLIC e INTERNAL.

Debe quedar explícito:

- **PUBLIC ≠ cualquier módulo ejecutable.**
- **INTERNAL = implementación interna no declarada como interfaz de ejecución pública.**

## 5. D5-02 — app.cli.main vs market_data.main (Decisión: D5-02 = OPCIÓN A)

Clasificar ambos como entrypoints públicos con roles distintos:

| Entrypoint | Rol |
|---|---|
| **`app.cli.main`** | APPLICATION BATCH ENTRYPOINT |
| **`market_data.main`** | MARKET DATA SERVICE ENTRYPOINT |

- No fusionarlos.
- No sustituir uno por otro.
- No convertir `market_data.main` en una variante de `app.cli.main`.
- No convertir `app.cli.main` en el runtime del servicio Market Data.

La arquitectura debe permitir que ambos coexistan porque pertenecen a fronteras de
ejecución diferentes:

```
APPLICATION EXECUTION
vs
MARKET DATA SERVICE EXECUTION
```

## 6. D5-03 — Market Data Entrypoint (Decisión: D5-03 = OPCIÓN B)

El service entrypoint público de Market Data es conceptualmente:

```
python -m market_data.main
```

Importante:

- La decisión arquitectónica es: **"`market_data.main` es el service entrypoint
  público de Market Data."**
- La sintaxis `-m` es el mecanismo de invocación actual.
- No convertir esta decisión en una obligación de que todos los demás entrypoints de
  OCM deban utilizar `-m`.
- No registrar `trading.main` ni `portfolio.main`.
- No crear nuevas interfaces.

## 7. D5-04 — Fuente normativa (Decisión: D5-04 = OPCIÓN D)

Adoptar un modelo de tres capas:

1. **ADR / ARCHITECTURAL AUTHORITY**
2. **EXECUTION REGISTRY**
3. **EXECUTION CONSUMERS**

```
          ADR
           │
           ▼
 EXECUTION CONTRACT
           │
           ▼
   EXECUTION REGISTRY
           │
   ┌───────┼────────┐
   ▼       ▼        ▼
pyproject systemd  Docker
   │       │        │
   └───────┼────────┘
           ▼
       CONSUMERS
```

Reglas:

- ADR define la decisión arquitectónica/normativa.
- Registry representa el contrato ejecutable/registrable.
- Consumers implementan/consumen ese contrato.
- Un consumer NO puede crear una nueva autoridad arquitectónica por el mero hecho
  de existir.
- `pyproject.toml` no es automáticamente autoridad arquitectónica.
- `run.sh` no es automáticamente autoridad arquitectónica.
- `Dockerfile` no es automáticamente autoridad arquitectónica.
- systemd no es automáticamente autoridad arquitectónica.
- CI verifica/aplica; no crea autoridad normativa.
- La existencia de un artefacto no le confiere autoridad.

## 8. Execution Model

Ver diagrama y puntos 1-12 de la sección 3. Este ADR NO inventa una nueva estructura
arquitectónica: formaliza el modelo que el humano adoptó para ejecución/entrypoints,
distinto del modelo de niveles de madurez de ADR-0024 (ver sección 13).

## 9. Public/Internal boundary

- PUBLIC = registro explícito en el execution contract (D5-01).
- INTERNAL = implementación interna no declarada como interfaz de ejecución pública.
- El execution contract es la frontera conceptual entre ambos.
- Ningún artefacto (módulo, `main.py`, `main()`, `python -m`, referencia de
  Docker/systemd/tests) se convierte automáticamente en público por existir.

## 10. Application entrypoint

`app.cli.main` = APPLICATION BATCH ENTRYPOINT (D5-02, opción A). Es un CLI batch
Hydra de una pasada, registrado en `[project.scripts]` como `ocm`, y usado por CI
para validación de config.

## 11. Market Data service entrypoint

`market_data.main` = MARKET DATA SERVICE ENTRYPOINT (D5-02, opción A); entrypoint
público de Market Data conceptualmente `python -m market_data.main` (D5-03, opción
B). Posee un runtime/service independiente como BC Nivel 1 (ADR-0024; sección 13).

## 12. Embedded BCs

- `trading` = BC Nivel 2, componente EMBEDDED, ejecutado via `live`/`paper`. No se
  crea `trading.main` como consecuencia de D5.
- `portfolio` = BC Nivel 2, componente EMBEDDED, ejecutado via `live`/`paper`. No se
  crea `portfolio.main` como consecuencia de D5.
- No se establece que cada BC necesite un proceso independiente.

## 13. BC/process distinction

Relación con ADR-0024:
- ADR-0024 define niveles de madurez de bounded contexts.
- D5 NO cambia automáticamente: market_data = Nivel 1, trading = Nivel 2,
  portfolio = Nivel 2.
- D5 solamente define el modelo de ejecución/entrypoints.
- **"Promover un BC a un proceso/runtime independiente requiere una decisión
  arquitectónica posterior y no se deriva automáticamente de D5."**
- Invariantes: BC ≠ PROCESS, BC ≠ ENTRYPOINT, MODE ≠ BC, DEPLOYMENT MECHANISM ≠
  ARCHITECTURE.

## 14. Execution registry

El modelo D5-04 establece que el execution registry representa el contrato
ejecutable/registrable. `[project.scripts]` es el registro ejecutable vigente y
candidato a materializar el execution contract en fases posteriores, PERO no es
automáticamente autoridad arquitectónica (D5-04). El ADR materializado es la
autoridad; el diseño concreto del execution contract es un Human/Build Gate posterior.

## 15. Consumers

Docker, Docker Compose, systemd, `run.sh` y CI son consumidores del execution
contract. Estas plataformas:

- consumen e implementan el contrato;
- NO definen el modelo arquitectónico por el mero hecho de existir;
- se alinearán con el execution contract materializado en la fase posterior
  (migración de consumers), NO en esta fase de documentación.

El servicio Market Data puede ser consumido por Docker, systemd, Compose u otros
consumidores autorizados, pero esos consumidores no definen el modelo arquitectónico.

## 16. Current-state discrepancies

Registradas como deuda/discrepancias pendientes de resolución posterior. NO se corrigen
en esta fase.

1. **`run.sh`**: `pyproject.toml` registra `ocm ocm-api live paper streaming`;
   `run.sh` expone `ocm live paper` y su cabecera declara "SSOT idéntico a
   [project.scripts]". **Contradicción factual** → FINDING/DEUDA DE MIGRACIÓN.
2. **`docker-compose.yml`**: referencia `trading.main` y `portfolio.main`, módulos
   inexistentes. Arquitectura decidida: LIVE/PAPER → APPLICATION (trading + portfolio
   embedded). Clasificación: **ARCHITECTURAL DISCREPANCY / DEAD RUNTIME REFERENCE**.
   NO resolver creando `trading.main` ni `portfolio.main`; la corrección futura
   alineará Compose con el execution contract aprobado.

## 17. Backtest status

- **BACKTEST = modo de ejecución adoptado como parte del modelo arquitectónico
  objetivo.**
- Estado actual: **NOT YET IMPLEMENTED / PENDING DESIGN AND IMPLEMENTATION.**
- No se documenta Backtest como runtime actualmente existente: la evidencia no lo
  demuestra (solo existe `scripts/backtest_app_guard.py`, un gate de CI sobre apps).
- Separado: **DECIDED ARCHITECTURE** vs **IMPLEMENTED CAPABILITY**.

## 18. Consecuencias

- Política clara PUBLIC/INTERNAL: la existencia de un módulo no confiere el estatus
  de entrypoint público.
- Coexistencia formal de `app.cli.main` (application batch) y `market_data.main`
  (market data service) en fronteras de ejecución distintas.
- Frontera normativa de tres capas (ADR → Registry → Consumers) que impide que
  Docker/systemd/run.sh/CI legislen por existencia.
- Confirmación de trading/portfolio como Nivel 2 embedded (sin `main.py`).
- Backtest formalmente parte del modelo objetivo, sin implementar.
- Se registran las discrepancias de `run.sh` y `docker-compose` como deuda sin
  resolver en esta fase.
- Los contratos BC-NN actuales permanecen intactos; este ADR no crea contratos
  técnicos nuevos.

## 19. Non-goals

Este ADR NO:

- implementa el execution contract;
- modifica código funcional;
- modifica Dockerfile, docker-compose, systemd, `run.sh` ni `[project.scripts]`;
- crea/elimina entrypoints de runtime;
- mueve archivos ni reorganiza directorios;
- crea `trading.main` ni `portfolio.main`;
- implementa Backtest;
- corrige las discrepancias detectadas (run.sh, docker-compose, Dockerfile);
- modifica contratos de arquitectura existentes;
- autoriza automáticamente la implementación.

## 20. Migration implications

La migración de consumidores (pyproject, app.cli, market_data runtime, Docker,
Compose, systemd, run.sh, CI, refs legacy) es un **Human/Build Gate separado** que
dependerá del execution contract concreto. Este ADR define solo la decisión
arquitectónica; nada se migra en esta fase.

## 21. Explicit deferred decisions

- Diseño materializado del Execution Contract (12 campos + matrices).
- Migración de consumers (fases A-L: contrato, pyproject, app.cli, market_data
  runtime, Docker, Compose, systemd, run.sh, tests de paridad, CI, refs legacy,
  validación).
- Implementación del modo BACKTEST (capacidad objetivo; requiere diseño + human gate).
- Corrección de discrepancias `run.sh` y `docker-compose`.
- Resolución del hallazgo M21 (naming canónico de auditorías en `docs/audits/`).
- Todo lo ya marcado como fuera de alcance en ADR-0045/ADR-0046 (H4, H5, B4, B5,
  M26, `adr:null`, precedencia registry↔ADR, etc.) permanece sin resolver por este ADR.

## 22. Authority/acceptance state

- **D5 = DECIDED — HUMAN** (2026-09-28).
- D5-01 = DECIDED — D; D5-02 = DECIDED — A; D5-03 = DECIDED — B; D5-04 = DECIDED — D.
- **DECISION ≠ MATERIALIZATION ≠ IMPLEMENTATION ≠ ACCEPTANCE.**
- Este ADR es el registro normativo (Decision Record) de D5; materializa la decisión
  humana, no la sustituye, no la amplía ni la reinterpreta.
- **Aceptación humana: G4.3 = ACCEPT WITH CLARIFICATIONS (2026-09-28).**
  La materialización de D5 queda ACEPTADA CON ACLARACIONES; las aclaraciones formales
  se incorporan en la sección 23 y en el registro. La aceptación NO autoriza
  implementación funcional ni migración (sección 23, punto 6).
- Este ADR respeta H1/H2/H3: no precedencia universal implícita, SSOTs
  especializados, evidence ≠ authority, CI ≠ authority, existencia ≠ authority,
  autoridad indeterminada → human gate.

## 23. Aclaraciones formales de aceptación (G4.3 — 2026-09-28)

El humano aceptó este ADR CON ACLARACIONES (G4.3, 2026-09-28). Estas aclaraciones son
estrictamente documentales/arquitectónicas: **no introducen nuevas decisiones de
diseño** y no alteran el contenido técnico de D5 más allá de lo siguiente.

### 23.1 Separación semántica de capas

| Capa | Rol |
|---|---|
| **ADR** | WHY — decisión arquitectónica |
| **CONTRACT** | WHAT MUST BE TRUE — invariantes/condiciones requeridas |
| **REGISTRY** | representación ejecutable/registrable de la decisión aprobada |
| **IMPLEMENTATION** | HOW — realización técnica |
| **VALIDATION** | HOW KNOWN TRUE — verificación de conformidad |
| **EVIDENCE** | WHAT WAS OBSERVED — observaciones registradas |

Ninguna capa inferior crea autoridad sobre las superiores. En particular:
- el Execution Registry representa la decisión aprobada, pero **no crea autoridad**;
- CI verifica/aplica, **no legisla**;
- la existencia de un artefacto (módulo, `main.py`, `python -m`, consumer) **no
  confiere autoridad**.

### 23.2 Invariantes formales (I-ENTRY)

Formalización explícita, sin duplicar formulaciones ya presentes en el ADR:

1. **I-ENTRY-01 — ENTRYPOINT ≠ BC.** Implícito en ADR-0047 §3.8 (`BC ≠ ENTRYPOINT`);
   se formaliza aquí con nombre estable.
2. **I-ENTRY-02 — ENTRYPOINT ≠ PROCESS.** Un BC no implica un proceso; un entrypoint
   tampoco. Un proceso puede ejecutar varios entrypoints (EP-002/EP-003 embedded).
3. **I-ENTRY-03 — ENTRYPOINT ≠ SERVICE.** Coexisten en fronteras distintas:
   `app.cli.main` = APPLICATION BATCH ENTRYPOINT; `market_data.main` = MARKET DATA
   SERVICE ENTRYPOINT. El "service entrypoint" no es el "service" en sí.
4. **I-ENTRY-04 — ENTRYPOINT ≠ DEPLOYMENT UNIT.** Docker/Compose/systemd son
   deployment consumers; no definen entrypoints ni arquitectura.
5. **I-ENTRY-05 — PUBLIC requiere registro explícito.** PUBLIC ≠ "existe main.py";
   requiere registro en el Execution Contract/Registry (D5-01).
6. **I-ENTRY-06 — APPLICATION MODE no implica BC.** BACKTEST/LIVE/PAPER/STREAMING/
   SERVICE son modos de ejecución, no bounded contexts nuevos (MODE ≠ BC).
7. **I-ENTRY-07 — BACKTEST/PAPER/LIVE comparten contratos app/dominio** donde el
   contrato sea aplicable; las diferencias de runtime quedan detrás de ports/adapters.
8. **I-ENTRY-08 — Venue-specific behavior detrás de adapters.** Sin imports directos
   cross-BC; venue ≠ contrato de dominio.
9. **I-ENTRY-09 — Application Core no depende de venue/deployment infra.** Core
   agnóstico del venue/deployment; adapters implementan ports.
10. **I-ENTRY-10 — Venue-specific behavior detrás de adapters** (formulación
    equivalente a I-ENTRY-08; se conserva la formulación de I-ENTRY-08 como única
    fuente, evitando duplicado).
11. **I-ENTRY-11 — ENTRYPOINT ≠ PROCESS ≠ SERVICE ≠ BC ≠ DEPLOYMENT UNIT**
    (superconjunto de I-ENTRY-01..04).
12. **I-ENTRY-12 — Fail-closed para autoridad/contrato/seguridad; fail-soft solo en
    zonas no críticas cuando sea apropiado.**

Nota de no-duplicación: I-ENTRY-10 reproduce el texto de I-ENTRY-08; I-ENTRY-11 es el
compuesto de I-ENTRY-01..04. No se crean invariantes nuevas no previstas; la
formalización usa la redacción existente del ADR.

### 23.3 Confirmación de la frontera de autoridad

- La autoridad sigue determinada por la gobernanza aprobada / H2-E: ante ausencia de
  regla aprobada → FAIL-CLOSED → human gate.
- La aceptación de D5 NO autoriza por sí misma: implementación funcional, migración
  de consumers, ni continuación automática de las fases B-L (CADA fase requiere su
  propio gate, ADR-0048 §9).

## Relación con H1/H2/H3

- H1 = DECIDED — CUSTOM/HYBRID (MODEL D + B + C).
- H2 = DECIDED — H2-E CUSTOM (ADR-0045).
- H3 = DECIDED — HUMAN (ADR-0046).
- H3-M1 = MATERIALIZED + COMMITTED.
- Este ADR no modifica H1/H2/H3 ni ADR-0045/ADR-0046.

## Relación con ADR-0024 y ADR-0039

- ADR-0024: conserva los niveles market_data = Nivel 1, trading = Nivel 2,
  portfolio = Nivel 2. D5 SOLO define ejecución/entrypoints.
- ADR-0039 (systemd como runtime canónico de aplicaciones; Docker/Compose como
  infraestructura): D5 es coherente con esa jerarquía — systemd/Docker/Compose son
  consumidores, no autoridad.

## Referencias

- Decisión humana: D5 = DECIDED — HUMAN — 2026-09-28 (D5-01=D, D5-02=A, D5-03=B,
  D5-04=D; autorización explícita D5 Human Decision → ADR + tracking/registry).
- ADR-0024: Dirección arquitectónica hacia microservicios — niveles de madurez.
- ADR-0039: Runtime canónico systemd.
- ADR-0045: Authority Conflict Resolution — Hybrid (H2-E Custom).
- ADR-0046: Decision Records & Materialization (H3).
- Código: `pyproject.toml` `[project.scripts]` (líneas 176-181), `packages/market_data/main.py`,
  `apps/app/cli/*.py`, `Dockerfile:40`, `docker-compose.yml` (líneas 359/407/451),
  `deploy/systemd/templates/ocm-market-data.service.template`,
  `deploy/systemd/templates/ocm-streaming.service.template`, `run.sh` (cabecera + caso ocm/live/paper).