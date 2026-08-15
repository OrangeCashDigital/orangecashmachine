# Knowledge Base — OrangeCashMachine

## Qué es esto

Colección auditada y clasificada de libros, papers y material de curso usados
como referencia técnica para OCM. Ver `manifest.yaml` como punto de entrada.

## Qué NO es

Esta Knowledge Base **no sustituye** ADRs ni código. La arquitectura normativa
de OCM está definida por el código, los contratos de import-linter y los ADRs
adoptados en `docs/architecture/decisions/`, no por los libros de esta carpeta.

## Cómo consultar manifest.yaml

Cada entrada tiene `id`, `type`, `status`, `authority`, `ocm_domains`, `path`,
`notes`, `provenance`. Empieza siempre por `manifest.yaml` — no recorras los
PDFs directamente sin verificar su `status`.

## Interpretar `authority`

- **TIER_0** — Código / ADR / arquitectura normativa de OCM
- **TIER_1** — Referencia técnica primaria
- **TIER_2** — Referencia secundaria
- **TIER_3** — Histórica / exploratoria
- **TIER_4** — Contextual / material de curso

Un libro no es automáticamente TIER_1: la autoridad refleja su función dentro de OCM.

## Interpretar `status`

- `active` — utilizable como referencia
- `needs_verification` — metadata bibliográfica no confirmada
- `needs_provenance_review` — origen del archivo no investigado
- `needs_attribution_review` — autoría/traducción incierta
- `needs_legal_review` — alerta de procedencia (ej. Z-Library)
- `historical` — draft o edición no definitiva
- `non_normative` — no debe citarse como autoridad técnica

## Regla explícita

> La Knowledge Base proporciona contexto y referencias técnicas. La arquitectura
> normativa de OCM está definida por el código, los contratos y los ADRs
> adoptados, no por los libros de esta carpeta.

Metadata no verificada (`status: needs_verification` o similar) **no debe
convertirse en hecho** al citarla. Verifica siempre `status` antes de usar una fuente.
