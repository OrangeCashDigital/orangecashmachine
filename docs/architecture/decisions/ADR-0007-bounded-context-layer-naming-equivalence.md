# ADR-0007: Equivalencia de capas por bounded context (no forzar naming uniforme)

**Estado:** Aceptado
**Fecha:** 2026-08-02
**Bounded context(s) afectado(s):** market_data, trading, portfolio

## Contexto

Auditoría de estructura real de carpetas (paso 2 del orden priorizado de
Fase 0) contra el patrón de referencia domain/ports/adapters/services/bootstrap:

| Bounded context | domain | ports | adapters | application/services | bootstrap |
|---|---|---|---|---|---|
| market_data | domain/ | ports/ | adapters/ | application/ | infrastructure/bootstrap/ (anidado) |
| portfolio | models/ | ports/ | infra/ | services/ | bootstrap/ (top-level) |
| trading | ninguno (organizado por capacidad: execution/, risk/, strategies/, analytics/) | ninguno | ninguno | ninguno | bootstrap/ (perdido, ver decisions/ADR-0003..0006) |

Ningún contexto usa literalmente los cinco nombres canónicos. market_data
y portfolio separan capas con nombres propios pero equivalentes en
espíritu (models≈domain, infra≈adapters, application≈services). trading
no separa por capa en absoluto — es la única deuda estructural real.

## Alternativas evaluadas

1. **Renombrar carpetas de market_data y portfolio al naming canónico**
   (domain/, adapters/, services/) para uniformidad total. Alto costo
   (imports rotos en todo el árbol, riesgo de regresión) para un
   beneficio puramente cosmético — viola KISS y SafeOps (cambio grande
   sin valor funcional).
2. **No tocar market_data ni portfolio; documentar la equivalencia como
   SSOT.** Costo cero, preserva el trabajo ya validado (38 contratos de
   import-linter, tests pasando). Formaliza el mapeo para que futuras
   auditorías no vuelvan a plantear la pregunta desde cero.
3. **Dejar trading sin capas también**, evitando la reconstrucción con
   estructura hexagonal. Rechazada: trading es el único contexto sin DIP
   real entre dominio e infraestructura — perpetuar eso bloquea Clean
   Architecture y BC-47.

## Decisión

- **market_data y portfolio no se renombran.** Su estructura actual es
  Clean Architecture / Hexagonal válida bajo naming propio. Este ADR
  formaliza la tabla de equivalencia de §Contexto como SSOT — no hay
  ambigüedad futura sobre si domain/ vs models/ es una carpeta "correcta".
- **trading SÍ debe adoptar domain/ports/adapters/services/bootstrap**
  al reconstruirse (ver docs/architecture/recovered/trading-bootstrap-forensic-analysis.md
  §7, Plan de reconstrucción). No es opcional: sin esa separación, BC-47
  y el resto de contratos DIP no tienen dónde aplicarse.

## Justificación técnica

Fail-fast donde importa (DIP roto en trading bloquea reconstrucción
correcta del bootstrap) y fail-soft donde conviene (no forzar rename
de dos contextos ya estables y probados). SSOT no exige naming idéntico
entre bounded contexts — exige que, dentro de cada contexto, exista una
única fuente de verdad para su propia estructura, documentada y
auditable. Consistente con DDD: cada bounded context puede tener su
propio lenguaje ubicuo, incluyendo nombres de carpeta, siempre que el
principio arquitectónico subyacente (separación domain/infra, DIP) se
cumpla.

## Consecuencias

- Ninguna migración de carpetas en market_data ni portfolio.
- La reconstrucción de trading (decisions/ADR-0003..0006) queda obligada a crear
  domain/, ports/, adapters/, services/ además de bootstrap/ — no solo
  recuperar composition_root.py suelto.
  **Estado (2026-08-03):** el bootstrap de trading se reconstruyó
  (`packages/trading/bootstrap/composition_root.py`, interfaz v3 de
  ADR-0003 enmendado) y quedó como único punto de ensamblado del contexto
  (ADR-0012). El trabajo de separación en domain/ports/adapters/services/
  de trading sigue pendiente como deuda estructural documentada.
- GOVERNANCE.md §7 mantiene la tabla de equivalencia actualizada; si un
  bounded context cambia su estructura, este ADR se marca Reemplazado.

## Addendum 2026-08-09 — coexistencia de `infrastructure/` raíz y `packages/*/infrastructure/`

**Origen:** F-011 (auditoría streaming canary). Existen DOS rutas `infrastructure/`
con significados distintos:

- **`infrastructure/` en la raíz del repo** (junto a `shared/`, `ocm/`,
  `packages/`, `apps/`). Contiene únicamente `redis/redis_stream.py` (código de
  streams Redis) y `__init__.py`. Documentado en pyproject.toml (packages
  remapped) y referenciado por import-linter (contrato de layers) — no es un
  artefacto huérfano.
- **`packages/<bc>/infrastructure/`** — capa de infraestructura del bounded
  context, anidada bajo cada paquete (p.ej. `packages/market_data/infrastructure/`
  con `timeouts.py`, `bootstrap/`, etc.).

Decisión: **no se renombra ni se mueve la raíz `infrastructure/`.** Los dos
árboles coexisten: la raíz aloja infraestructura transversal de plataforma
fuera de cualquier bounded context, mientras que `packages/*/infrastructure/`
es la capa hexagonal del contexto. Cualquier import canónico debe usar la ruta
completa (`market_data.infrastructure.timeouts`, no `infrastructure.timeouts` —
ver F-011; el ejemplo `from infrastructure.timeouts import ...` del docstring de
timeouts.py apuntaba a un módulo inexistente y ya fue corregido). Candidato a
futura unificación del código de streams bajo un único hogar canónico, según
resolución de F-024.

## Referencias

- docs/architecture/recovered/trading-bootstrap-forensic-analysis.md
- ADR-0003, ADR-0004, ADR-0005, ADR-0006 (serie decisions/)
- GOVERNANCE.md §7 (inventario de estructura real)
