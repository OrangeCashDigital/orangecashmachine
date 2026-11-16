# HANDOFF — Shared Kernel (P1 parcial)

Fecha: 2026-11-16
Rama: `wip/freshness-enforcement`

## Estado actual

- **P1 parcialmente completado.** `shared/types/values.py` implementa `Price`, `Quantity`
  y `Money` según la spec confirmada:
  - `@dataclass(frozen=True, slots=True)`.
  - Aceptan `Decimal`, `int` y `str` (coerción a `Decimal`); `float` → `TypeError`.
  - `Price.value > 0`; `Quantity.value >= 0`; `Money.amount` puede ser negativo.
  - `tick_size` / `step_size` opcionales; si se aportan, múltiplo exacto obligatorio.
  - `Money` normaliza `currency` a mayúsculas y exige divisa homogénea para `+`/`-`.
- **Tests golden** en `tests/shared/types/test_values.py`: rechazo de `float`,
  inmutabilidad, igualdad/hash, precisión decimal (`0.1 + 0.2 == 0.3`), múltiplos
  tick/step, aritmética de `Money`.
- **Pureza del kernel**: `tests/shared/test_kernel_purity.py` (AST) falla si `shared/`
  importa algo distinto de stdlib/`shared.*`.
- Bugs previos de esta rama ya resueltos y commiteados: validación de `StreamIdentity`
  en construcción directa y `MissingField` para `event_id` en `BasePayload.from_dict`.
- **Pendiente**: migración de `Signal.price` / `Signal.quantity` a `Price` / `Quantity`
  (fuera de esta sesión).
- `tests/shared` no requiere Kafka. La suite completa del repo no se ejecutó en esta
  sesión.
- El árbol de trabajo contiene cambios no relacionados (`packages/market_data`,
  `packages/trading`, `tests/trading`, nuevos ficheros de freshness) que **no** se
  tocaron ni se incluyeron en estos commits.

## Commits hechos (más recientes primero)

- `b9c4b1b` `docs(shared): marcar P1 parcial (value objects, Signal pendiente)`
- `6a419e5` `test(shared): verificar pureza stdlib del kernel`
- `97445df` `feat(shared): Price, Quantity, Money con Decimal`
- `63295e0` `feat(shared): add Price, Quantity, Money value objects`
- `d4b19a6` `fix(shared): validar event_id requerido en BasePayload.from_dict`
- `3f6e798` `fix(shared): validar StreamIdentity en construcción directa`
- `fbd8148` `docs(shared): crear plan maestro de shared kernel`

## Siguiente tarea — migración de `Signal` (P1 restante)

1. Migrar `shared/types/signal.py`: `price: float` → `Price` y
   `quantity: Optional[float]` → `Quantity | None`; ajustar `__post_init__`
   (las invariantes de positividad/no-negatividad pasan a los VOs).
2. Buscar consumidores rotos:
   - `grep -rn "Signal(" --include='*.py' .`
   - `grep -rn "\.price" shared packages apps tests --include='*.py'`
   - `grep -rn "\.quantity" shared packages apps tests --include='*.py'`
3. Ejecutar la **suite completa** para listar roturas reales:
   `uv run pytest tests -q --no-cov` (o `-x` para fail-fast).
4. Arreglar consumidores uno a uno (sin `float` para precio/cantidad; sin
   `as any`/type-ignore para silenciar). Reportar cualquier rotura que requiera
   decisión de diseño en lugar de asumirla.

## Comandos de verificación

```bash
PYTHONPATH=/home/orangemusic/trading/orangecashmachine uv run pytest tests/shared -q --no-cov
uv run pytest tests -q --no-cov          # suite completa (incluye integración; Kafka aparte)
uv run ruff check .
uv run lint-imports --config architecture_linter/importlinter.toml
```

## Notas operativas

- **`git stash` es competencia exclusiva del humano.** El agente no ejecuta
  `git stash`, ni inspecciona/aplica stashes. Existen refs/logs de stash en `.git/`;
  su contenido y gestión corresponden al humano.
- `git push` y `gh pr create` requieren confirmación humana explícita. El merge lo
  realiza siempre el humano.
- **Paso 4 (typo) — no aplicado:** `grep -n "cuandose" shared/types/signal.py` no
  encuentra coincidencias y `git log -S "cuandose" -- shared/types/signal.py` tampoco.
  La línea 98 ya dice `cuando se especifica` (correcto). No se hizo commit para no
  inventar un cambio inexistente.
