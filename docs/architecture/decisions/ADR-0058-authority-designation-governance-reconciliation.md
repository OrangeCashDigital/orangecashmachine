# ADR-0058: Designación de autoridad para reconciliación de gobernanza

**Estado:** ACEPTADO — aceptación documental autorizada 2026-10-27 (decisión humana: designación de autoridad decisoria para C1–C4/D12/T-PLAN/VCOM-1–VCOM-5; no autoriza por sí misma el levantamiento de GATE C, IMPLEMENTATION ni Q2)
**Fecha:** 2026-10-27
**Bounded context(s) afectado(s):** ocm (plataforma), market_data | trading

## Contexto

La auditoría de gobernanza del 2026-10-27 concluyó que no existe autoridad designada explícitamente para resolver las contradicciones C1, C2, C3, C4/D12, T-PLAN ni VCOM-1..VCOM-5, ni para levantar GATE C, autorizar IMPLEMENTATION ni decidir Q2.

ADR-0032 (AI Agent Governance) regula branch protection, CODEOWNERS, evidence hash y waiver/expiración, pero no designa autoridad decisoria para estas contradicciones. ADR-0045 (H2-E) establece el mecanismo FAIL-CLOSED → HUMAN DECISION GATE, pero no nombra al decisor competente cuando no existe autoridad de dominio formalmente determinada. ADR-0055 resuelve Q1 (autoridad normativa para D19-01/D20b-01..06) pero no designa autoridad para el resto. ADR-0057 consolida decisiones humanas G1 sin resolver C1–C4 ni designar autoridad.

El repositorio OrangeCashDigital/orangecashmachine pertenece a la cuenta de usuario OrangeCashDigital (id 207955556, login OrangeCashDigital), verificada como titular y owner del repositorio. `gh api orgs/OrangeCashDigital` devuelve 404, confirmando que no es una organización ni existe un comité con competencia superior documentada.

## Decisión

Se designa como **autoridad decisoria** para las contradicciones C1, C2, C3, C4/D12, T-PLAN y VCOM-1..VCOM-5 al titular de la cuenta OrangeCashDigital (owner del repositorio, id 207955556).

## Fuente de competencia

Titularidad de la cuenta de usuario OrangeCashDigital en GitHub (id 207955556, login OrangeCashDigital), que otorga control administrativo sobre el repositorio. Verificado el 2026-10-27 con `gh api user` y `gh repo view OrangeCashDigital/orangecashmachine --json owner`.

No hay organización, estatutos ni comité con competencia superior documentada. `gh api orgs/OrangeCashDigital` devuelve 404, confirmando que no es una organización.

## Nota meta-normativa (bootstrap consciente)

El corpus normativo revisado (GOVERNANCE.md, ADR-template.md, AUDIT_PROTOCOL.md, PLAN-Maestro-Ingenieria.md, ADR-0045, ADR-0046) no contiene una regla preexistente que transfiera autoridad normativa al titular administrativo de la cuenta de GitHub. La sección "Autoridad decisoria" de GOVERNANCE.md es producto de este mismo ADR-0058, por lo que no puede invocarse como fuente independiente.

El titular de la cuenta OrangeCashDigital declara explícitamente asumir la autoridad normativa de OCM por vía de los hechos, en ausencia documentada de una instancia superior con competencia establecida. No existe organización, comité, estatutos ni documento de gobernanza externo al repositorio que designe una autoridad alternativa.

Esta declaración no infiere autoridad de fuentes que no la contienen: la ejerce como titular administrativo único del repositorio, con reconocimiento explícito de que se trata de un bootstrap normativo y no de una derivación de una regla preexistente.

Si en el futuro se crea una organización con estatutos o se designa una autoridad superior, este bootstrap quedará sujeto a revisión según la sección "Vigencia y revisión" de este ADR.

## Alcance

Competencias delegadas, cada una **independiente**:

- Resolver C1, C2, C3, C4/D12, T-PLAN.
- Resolver VCOM-1, VCOM-2, VCOM-3, VCOM-4, VCOM-5.
- Aprobar cambios normativos en ADRs, GOVERNANCE.md, AUDIT_PROTOCOL.md, PLAN-Maestro-Ingenieria.md.
- Actualizar tracking.yaml.
- Levantar GATE C.
- Autorizar IMPLEMENTATION.
- Decidir Q2 / materialización de D20b-01..06.

Aprobar una competencia **NO implica** aprobar las demás. Cada autorización requiere decisión explícita y separada.

## Procedimiento de registro

Cada decisión se registra mediante:

- ADR nuevo, o
- Enmienda a ADR existente, o
- Entrada D-XXX en AUDIT_PROTOCOL §N, o
- Entrada con fecha en tracking.yaml.

Toda decisión debe incluir identificador, evidencia, fecha y firma del titular (o commit firmado con GIT_HUMAN_OVERRIDE=1 si opera directamente).

## Vigencia y revisión

Sin fecha de caducidad. Revisable si cambia la titularidad de la cuenta OrangeCashDigital o si se crea una organización con estatutos.

## Consecuencias

- Desbloquea la resolución de C1, C2, C3, C4/D12, T-PLAN, VCOM-1, VCOM-4, VCOM-5 (VCOM-2 y VCOM-3 quedan DECIDED por este ADR).
- Permite solicitar el levantamiento de GATE C.
- Permite solicitar autorización de IMPLEMENTATION y decisión de Q2.
- NO desbloquea por sí solo ninguna de esas autorizaciones: cada una requiere decisión explícita y separada.

## Referencias

- ADR-0032: AI Agent Governance (branch protection, CODEOWNERS, evidence hash, waiver/expiración).
- ADR-0045: Authority Conflict Resolution — Hybrid (H2-E Custom).
- ADR-0055: Registrar la autoridad normativa de D19-01 y D20b-01..06 (D-OCM-01 Q1).
- ADR-0057: Consolidación de decisiones humanas G1: identidad canónica, DefinitionVersion y VenueConstraints.
- docs/governance/AUDIT_PROTOCOL.md §N (estructura D-XXX para decisiones humanas).
- docs/plans/tracking.yaml (bloque vcom_designacion_autoridad, VCOM-2 = DECIDED).
- docs/architecture/GOVERNANCE.md (sección "Autoridad decisoria").
