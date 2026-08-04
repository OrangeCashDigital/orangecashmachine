# Gobernanza de arquitectura — OrangeCashMachine

Fase 0: preservación de conocimiento. SSOT de reglas sobre cómo se
documenta, respalda y decide arquitectura en OCM.

## 1. Archivos y artefactos críticos

- architecture/importlinter.toml
- **/bootstrap/composition_root.py de cada bounded context
- ocm/config/schema.py
- shared/contracts/boundaries.py
- docs/architecture/decisions/
- docs/architecture/recovered/

Regla dura: ningún archivo bajo **/bootstrap/ se considera terminado
hasta estar committeado a git.

## 2. Cuándo un cambio requiere ADR

- Cambiar la firma del constructor de cualquier Composition Root.
- Agregar o eliminar un contrato de architecture/importlinter.toml.
- Cambiar quién es dueño de un estado mutable compartido.
- Documentar y posponer deuda técnica en vez de resolverla ya.

No requiere ADR: bugfixes, refactors internos sin cambio de contrato
público, cambios de test.

## 3. Sistema de ADR

- Ubicación: docs/architecture/decisions/ADR-NNNN-titulo-slug.md
- Template: docs/architecture/decisions/ADR-template.md
- Numeración secuencial, nunca se reutiliza un número eliminado.

## 4. Documentación de recuperaciones forenses

1. Backup inmediato fuera del working tree antes de tocar nada.
2. Documento con secciones separadas: evidencia objetiva, comparación
   con estado actual, decisiones de arquitectura.
3. No se reconstruye código roto solo para completar lo perdido.

## 5. Backups

Cualquier artefacto irremplazable se respalda fuera del repo git antes
de cualquier otra acción. Los backups no reemplazan el commit a git.

## 6. Inventario de activos arquitectónicos

Ver docs/architecture/INVENTORY.md (pendiente de crear): bounded
contexts, composition roots, ports, adapters, contratos BC-NN activos.

## 7. Estructura real por bounded context (equivalencia de capas)

Auditoría verificada por estructura de carpetas (paso 2, Fase 0). Ver
ADR-0007 para la decisión de no forzar naming uniforme.

| Bounded context | domain | ports | adapters | application/services | bootstrap | Estado |
|---|---|---|---|---|---|---|
| market_data | domain/ | ports/ | adapters/ | application/ | infrastructure/bootstrap/ | Completo (naming propio) |
| portfolio | models/ | ports/ | infra/ | services/ | bootstrap/ | Completo (naming propio) |
| trading | ninguno (naming propio: analytics/, execution/, risk/, strategies/, data/) | | | bootstrap/ | En desarrollo — capas execution/, risk/, strategies/, analytics/, data/ + composition_root |

Regla derivada: cualquier bounded context nuevo, o `trading` al
reconstruirse, debe implementar las cinco capas explícitamente
(domain/ports/adapters/services|application/bootstrap), no
necesariamente con esos nombres literales, pero sí con la separación
de responsabilidades que representan.

## 8. Gobernanza automatizada (ADR-0010)

La arquitectura del kernel no solo se documenta: se hace cumplir en
CI y en el commit local. Ver ADR-0010 para la decisión completa.

### Gates de CI (`.github/workflows/ocm-ci.yml`)

- `architecture` (import-linter, 43 contratos) — fail-fast, bloquea el merge si un BC-NN se rompe.
- `quality` — ruff check, ruff format, mypy shared/, SSOT literales, pip-audit.

### Scripts de gobernanza (`scripts/`)

- `check_ssot_enums.py` — verifica que los literales de dominio (OrderSide, PositionSide,
  SignalDirection, DataSource) solo se definan en `shared/enums.py`. Falla en CI si se duplica.
- `metrics_report.py` — genera `architecture/metrics.json` (contratos KEPT/BROKEN, errores mypy,
  tests passed, vulnerabilidades). En CI se sube como artifact, no se commitea.

### Contratos del kernel (shared)

BC-01 (dependency-free) · BC-32 (SSOT del bus) · BC-33 (schemas aislados del dominio) ·
BC-34 (neutralidad) · BC-35 (sin duplicación de wire) · BC-45 (types/contracts no importan
kafka) · BC-46 (`enums` stdlib-only) · BC-47 (`kafka` no importa dominio) · BC-48 (`utils` genérico).

### SafeOps

- `pip-audit --requirement pyproject.toml` en CI (vulnerabilidades conocidas).
- Dependabot semanal (ecosistema `pip`).
- Pre-commit: ruff, import-linter, mypy shared/, SSOT — detección local antes del commit.

## 9. Series de ADR — serie canónica y serie heredada

La serie canónica vive en `docs/architecture/decisions/ADR-NNNN-*.md`
(§3). La serie raíz `docs/architecture/0000-0005*.md` quedó **deprecada
2026-08-03** (auditoría de composition roots, hallazgo H5 — colisión de
numeración). Se conserva como registro histórico con banner de deprecación
en cada archivo; **no usar su numeración para referencias nuevas**.

### Mapa serie heredada → serie canónica

| Serie heredada (`docs/architecture/`) | Serie canónica (`docs/architecture/decisions/`) |
|---|---|
| `0000-principios-arquitectonicos` | — (sin equivalente; principios base) |
| `0001-bounded-contexts-composition-root` | — (sin equivalente directo; lectura histórica) |
| `0002-event-driven-kappa-architecture` | — (sin equivalente; supersedida en notas) |
| `0003-composition-root-jerarquico` | `ADR-0003-trading-composition-root-narrow-constructor` (tema distinto) |
| `0004-rebalance-service-capacidad-adelantada` | `ADR-0004-bc47-market-data-import-boundary` (tema distinto) |
| `0005-hydra-reemplaza-cli-legado` | `ADR-0005-trading-engine-internal-external-split` (tema distinto) |

Nota: el log de verificación `0006-verificacion-adrs-vs-codigo` se reubicó
en `docs/architecture/logs/verificacion-adrs-vs-codigo-2026-08-02.md`
(no es un ADR y colisionaba con `decisions/ADR-0006`).

Regla: referencias nuevas a decisiones de arquitectura apuntan a
`decisions/ADR-NNNN-*`. La serie heredada solo se cita como contexto
histórico y nunca por su número de forma ambigua.
