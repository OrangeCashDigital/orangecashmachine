# ADR-0008: Contrato de capas para portfolio (bootstrap → infra → services → ports → models)

**Estado:** Aceptado
**Fecha:** 2026-08-02
**Bounded context(s) afectado(s):** portfolio

## Contexto

Auditoría del paso 3 del orden priorizado (revisión de import-linter
contra el patrón de capas, ver ADR-0007) encontró que market_data tiene
un contrato layers formal (BC-08) que hace cumplir su orden de capas en
CI, pero portfolio no tiene contrato equivalente pese a tener las cinco
capas bien separadas (models/, ports/, infra/, services/, bootstrap/).
Solo existen contratos forbidden puntuales (BC-13, BC-43) que cubren
casos específicos, no la totalidad del grafo de dependencias interno.

## Alternativas evaluadas

1. Dejar portfolio sin contrato layers, confiando en que BC-13/BC-43 y
   la disciplina del equipo alcanzan — riesgo de import invertido sin
   detección automática (ej. models importando infra).
2. Agregar un contrato layers formal, mismo tipo que BC-08 en
   market_data, con la dirección bootstrap → infra → services → ports →
   models.

## Decisión

Se agrega BC-44: contrato layers para portfolio con capas, de externa a
interna: bootstrap, infra, services, ports, models.

- bootstrap puede importar todo (es el composition root, DIP se resuelve ahí).
- infra puede importar services, ports, models.
- services puede importar únicamente ports y models — nunca infra
  directamente (ya validado en código: PortfolioService depende de
  PositionStore como Protocol, no de RedisPositionStore).
- ports puede importar únicamente models.
- models no importa ningún módulo del propio bounded context portfolio.

## Justificación técnica

DIP explícito: services depende de la abstracción (ports), nunca de la
implementación concreta (infra) — RedisPositionStore/InMemoryPositionStore
implementan PositionStore, y solo bootstrap las conecta. Este contrato
convierte esa disciplina, ya presente en el código desde Fase 3, en una
regla ejecutable en CI (fail-fast: un import invertido rompe el build,
no se descubre en producción). Mismo principio que BC-08 aplicado a un
bounded context distinto — SSOT de la regla es el propio código, no la
memoria del equipo.

## Consecuencias

- Cualquier import de portfolio.infra desde portfolio.services queda
  bloqueado en CI, no solo desalentado por convención.
- BC-43 (forbidden puntual sobre adapters concretos) queda como caso
  particular ya cubierto por BC-44 (layers) — se mantiene por
  redundancia documental, no es necesario eliminarlo.
- Verificar con `lint_imports` local antes de mergear (ver comando en
  el commit de este ADR) por si algún import existente ya viola el
  orden y requiere fix previo.

## Referencias

- architecture/importlinter.toml — BC-08 (mismo patrón en market_data),
  BC-13, BC-43
- packages/portfolio/services/portfolio_service.py (PositionStore por
  constructor, DIP ya validado en Fase 3)
- ADR-0007 (equivalencia de capas por bounded context)
