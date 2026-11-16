# Plan de desarrollo profesional de shared — OrangeCashMachine

## Objetivo
Convertir `shared/` en un Shared Kernel de calidad industrial (alineado con NautilusTrader / Hummingbot), listo para paper-to-live y event-driven.

## Reglas inviolables
- `shared/` solo puede depender de stdlib (+ typing). Cero imports de BCs, ocm, infrastructure, pandas, CCXT, etc.
- Todo value object debe ser `frozen=True`.
- Nunca usar `float` para dinero, precio o cantidad económica.
- Los contratos de `boundaries.py` son SSOT. Ningún BC define sus propios protocols de frontera.

## Checklist de Prioridades
- [~] P1: Value Objects (Price, Quantity, Money) + tests + tests golden.
  - [x] Value objects `Price`/`Quantity`/`Money` (`shared/types/values.py`) con `Decimal`, `frozen=True, slots=True`, rechazo de `float`, `tick_size`/`step_size` opcionales y aritmética de `Money` con divisa homogénea.
  - [x] Tests golden en `tests/shared/types/test_values.py` (`0.1 + 0.2 == 0.3`, inmutabilidad, igualdad/hash, múltiplos tick/step).
  - [ ] Migrar `Signal.price`/`Signal.quantity` a `Price`/`Quantity` (fuera de esta sesión).
- [ ] P2: Jerarquía de Domain Events (Base `DomainEvent`).
- [ ] P3: Entidades de dominio (OrderSnapshot, PositionSnapshot).
- [ ] P4: Limpieza de huérfanos (boundaries.py).
- [ ] P5: Enums como SSOT absoluto.
- [ ] P6: Kafka schemas (wire contracts versionados).
- [ ] P7: Gobernanza (refuerzo import-linter, tests golden, mypy strict).
- [ ] P8: Documentación viva (DOMAIN.md / ADRs).

## Estado actual (2026-11-16)
- Plan creado.
- **P1 parcial:** value objects implementados y testeados; migración de `Signal` a `Price`/`Quantity` pendiente para otra sesión.
- Bug StreamIdentity (validación en construcción directa): **RESUELTO** en `wip/freshness-enforcement` (`shared/contracts/stream_identity.py` valida en `__post_init__`). PR previsto: #152.
- Bug serializer (None en campo obligatorio -> `MissingField`): **RESUELTO** (`BasePayload.from_dict` valida `event_id` requerido).
- Pureza del kernel: `tests/shared/test_kernel_purity.py` (AST) falla si `shared/` importa algo distinto de stdlib/`shared.*`.
- Nota: `git stash` es competencia exclusiva del humano; el agente no lo inspecciona ni aplica.
