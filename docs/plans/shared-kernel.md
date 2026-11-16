# Plan de desarrollo profesional de shared — OrangeCashMachine

## Objetivo
Convertir `shared/` en un Shared Kernel de calidad industrial (alineado con NautilusTrader / Hummingbot), listo para paper-to-live y event-driven.

## Reglas inviolables
- `shared/` solo puede depender de stdlib (+ typing). Cero imports de BCs, ocm, infrastructure, pandas, CCXT, etc.
- Todo value object debe ser `frozen=True`.
- Nunca usar `float` para dinero, precio o cantidad económica.
- Los contratos de `boundaries.py` son SSOT. Ningún BC define sus propios protocols de frontera.

## Checklist de Prioridades
- [ ] P1: Value Objects (Price, Quantity, Money) + tests + tests golden.
- [ ] P2: Jerarquía de Domain Events (Base `DomainEvent`).
- [ ] P3: Entidades de dominio (OrderSnapshot, PositionSnapshot).
- [ ] P4: Limpieza de huérfanos (boundaries.py).
- [ ] P5: Enums como SSOT absoluto.
- [ ] P6: Kafka schemas (wire contracts versionados).
- [ ] P7: Gobernanza (refuerzo import-linter, tests golden, mypy strict).
- [ ] P8: Documentación viva (DOMAIN.md / ADRs).

## Estado actual (2026-11-16)
- Plan creado.
- Bug StreamIdentity (validación en construcción directa): **BLOQUEADO por PR #148** (`wip/freshness-enforcement` toca `shared/contracts/stream_identity.py`). Cambio propuesto pendiente de merge del PR antes de tocar.
- Bug serializer (None en campo obligatorio -> `MissingField`): pendiente de definición de contrato/schema en implementación actual (serializer actual solo hace `payload.to_dict()`/`from_dict` sin validación de campos requeridos).
