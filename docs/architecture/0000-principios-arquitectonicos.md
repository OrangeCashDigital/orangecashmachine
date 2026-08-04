# ADR-0000: Principios arquitectónicos de OrangeCashMachine

> **SERIE HEREDADA (deprecada 2026-08-03).** La serie canónica de ADRs es
> `docs/architecture/decisions/ADR-NNNN-*` (ver `GOVERNANCE.md §3` y §9).
> Este documento se conserva como registro histórico; su numeración no debe
> usarse para referencias nuevas.

**Estado:** Aceptado
**Fecha:** 2026-08-01
**Contexto del bounded context:** Todo el repositorio — este documento no cambia con la tecnología, solo con la filosofía del proyecto.

## Contexto

OCM ha ido acumulando principios de facto a través de auditorías (mayo,
agosto) y ADRs técnicos (0002 event-driven/Kappa, 0003 Composition Root),
pero nunca se formalizó qué significan esos principios *en este proyecto
específico*. Términos como SSOT, Fail-Soft o Clean Architecture se usan de
forma consistente en commits y hallazgos, pero sin una definición ancla a la
que apelar cuando hay ambigüedad. Este documento es esa ancla.

Este ADR no habla de Kafka, Redis, Hydra ni de ningún detalle técnico —
esos pertenecen a `ADR-0001` (arquitectura global) y `ADR-0002` en adelante
(decisiones técnicas puntuales). Este documento prácticamente nunca cambia.

## Decisión

### Principio 1 — SSOT (Single Source of Truth)

Cada pieza de conocimiento del sistema vive en exactamente un lugar. Un
tópico Kafka no tiene dos nombres (`market.trades.raw` vs `trades.raw` fue
una violación real, corregida). Una regla de negocio no se duplica entre
`execute_live.py` y `execute_paper.py` (violación real: `on_fill_composite`,
corregida vía `build_fill_sync()`). Cuando SSOT se rompe, el síntoma no es
un error inmediato — es que un fix aplicado en un lugar no se propaga al
otro, y el bug queda vivo a medias sin que ningún test lo note.

### Principio 2 — DIP (Dependency Inversion) — "todo depende de interfaces"

Los módulos de alto nivel no dependen de implementaciones concretas de bajo
nivel; ambos dependen de abstracciones (`Protocol`, `runtime_checkable`).
Ejemplo ya implementado: `build_fill_sync(tracker, portfolio)` recibe sus
colaboradores vía `Protocol`, no importa `portfolio` directamente — respeta
el aislamiento entre bounded contexts (contrato BC-13). El día que
`portfolio` cambie de Redis a PostgreSQL, nada fuera de su Composition Root
debe enterarse.

### Principio 3 — Clean Architecture / aislamiento de bounded contexts

Cada bounded context (`market_data`, `trading`, `portfolio`,
`control_plane`) es una unidad con fronteras explícitas, verificadas
mecánicamente por `lint-imports` (37 contratos activos al momento de este
ADR). Un bounded context no importa detalles internos de otro; solo
consume lo que el otro expone deliberadamente. `ADR-0003` (Composition Root
jerárquico) es la aplicación directa de este principio al problema de
ensamblaje: el Composition Root General conoce que existen los bounded
contexts, nunca cómo se construyen por dentro.

### Principio 4 — Fail-Soft / SafeOps

El sistema prioriza degradar sin caerse por completo antes que fallar de
forma catastrófica, especialmente en rutas que tocan capital real. Ejemplo
ya implementado: `LiveEngineResources.shutdown()` cierra cada recurso de
forma aislada — el fallo al cerrar `redis_client` no impide intentar cerrar
`kafka_producer`. SafeOps es la aplicación de Fail-Soft específicamente a
operaciones con dinero: nunca dejar una conexión abierta ni un estado a
medias ante `SIGINT`/`SIGTERM`/excepción.

### Principio 5 — Event First

Cuando una funcionalidad puede modelarse como reacción a un evento en vez
de como llamada directa, se modela como evento. Esto incluye adelantar
contratos antes que su consumidor exista: `RiskGate` es un contrato
publicado a propósito, en espera de `RiskGateConsumer` (parte de la
migración event-driven de `ADR-0002`) — no es código huérfano, es diseño
adelantado deliberado. La distinción entre "contrato adelantado a
propósito" y "código huérfano sin plan" (como sí lo era `rebalance.py`) es
si existe una decisión documentada de hacia dónde va.

### Principio 6 — KISS (Keep It Simple)

Se prefiere la solución más simple que resuelve el problema real, incluso
cuando existe una alternativa más "correcta" en abstracto. Ejemplo ya
decidido: `ADR-0003` rechaza explícitamente un framework de DI de terceros
para el Composition Root — no porque no funcionaría, sino porque OCM no usa
contenedores DI en ningún otro punto del código, y el problema real no era
la mecánica de inyección sino la falta de una frontera clara entre niveles.
KISS no es "escribir menos código"; es no introducir maquinaria que el
proyecto no necesita todavía.

### Principio 7 — DRY (Don't Repeat Yourself), subordinado a SSOT

La duplicación de código es un síntoma, no el problema en sí — el problema
de fondo casi siempre es una violación de SSOT (dos lugares que deberían
ser uno). DRY se aplica como consecuencia de perseguir SSOT, no como
objetivo aislado: extraer una función compartida sin resolver por qué había
dos fuentes de verdad solo mueve el problema.

## Consecuencias

- Cualquier ADR posterior (`0001` en adelante) hereda estos principios sin
  necesidad de re-justificarlos; puede citarlos por nombre.
- Cuando una auditoría futura encuentre una violación, debe nombrar cuál de
  estos 7 principios se rompe — no basta con "esto está mal", el hallazgo
  debe ser accionable contra un principio nombrado.
- Este documento se actualiza solo si el proyecto adopta o abandona un
  principio fundacional completo, no por cada decisión técnica puntual.

## Alternativas consideradas

- **No formalizar principios, dejarlos implícitos en la cultura del
  proyecto**: rechazado — ya se demostró en la práctica que sin ancla
  escrita, términos como "SSOT" se invocan de forma consistente pero sin
  definición común, dificultando arbitrar desacuerdos futuros.

Principios: (este documento es la fuente de los principios, no los aplica sobre sí mismo)
