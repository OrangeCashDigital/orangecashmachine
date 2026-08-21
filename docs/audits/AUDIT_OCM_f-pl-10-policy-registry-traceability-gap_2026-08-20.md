# F-PL-10 — Pérdida de trazabilidad regla↔hallazgo en migración a Policy Registry

**Fecha:** 2026-08-20
**Contexto:** migración de `engineering_health_check.py` de `tracking.yaml:reglas` a `policies/registry.yaml` como SSOT (ADR-0031).

## Matriz de Findings

| ID | Clasificación | Severidad | Control | ADR | Estado |
|---|---|---|---|---|---|
| F-PL-10 | NUEVO | MEDIA | POLICY_REGISTRY_SCHEMA | ADR-0031 | OPEN |

## Evidence

- `tracking.yaml:reglas` (legacy) modelaba cada regla con un campo `hallazgo: B-NNN` (o `H-NNN`), vinculándola al backlog/hallazgo que la originó.
- `policies/registry.yaml` (SSOT vigente post ADR-0031) no tiene campo equivalente en el schema de sus 16 reglas (R1-R16): no existe `hallazgo`, `backlog_ref`, ni `source_finding`.
- `engineering_health_check.py`, en su versión pre-migración, validaba esta referencia (`check_coherence`: `"referencia hallazgo {hid} inexistente en backlog"`). Esa validación fue retirada como parte de la migración a `registry.yaml` (2026-08-20), sin reemplazo funcional.

## Impact

- Ya no hay forma automatizada de responder "¿qué hallazgo/decisión de negocio originó esta regla de CI?" a partir del registry — la trazabilidad regla→causa-raíz se perdió en la migración, aunque las 16 reglas migradas SÍ preservan `master_plan` y `adr` como referencias arquitectónicas.
- Riesgo de deuda de gobernanza: futuras auditorías (M17-M20 en `audit_validator.py`) no podrán cruzar automáticamente reglas del registry contra el backlog de hallazgos en `tracking.yaml`.

## Decisión Humana requerida (§M AUDIT_PROTOCOL)

1. ¿Se acepta la pérdida de este campo como trade-off deliberado de ADR-0031 (`registry.yaml` es "reglas de gobernanza puras", desacopladas del backlog operativo)?
2. Si no se acepta: ¿se añade un campo `source_finding:` opcional al schema de `registry.yaml` y se re-cablea un check nuevo (`check_registry_traceability`) en `engineering_health_check.py`?

## Estado

OPEN — no resuelto por el agente, requiere decisión de Solano.
