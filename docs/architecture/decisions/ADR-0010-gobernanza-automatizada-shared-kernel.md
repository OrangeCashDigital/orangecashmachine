# ADR-0010: Gobernanza automatizada del Shared Kernel

**Estado:** Aceptado
**Fecha:** 2026-08-03
**Bounded context(s) afectado(s):** shared | ocm (CI) | infraestructura de gobernanza

## Contexto

El Shared Kernel pasó de tener literales duplicados entre `shared/kafka/schemas/_base.py`
y `shared/types/` a una estructura limpia (ADR-0009 → refactor `dfe9c24`): el vocabulario de
dominio vive en `shared/enums.py` y el envelope wire re-exporta por compatibilidad (BC-45).

Hasta ahora la arquitectura estaba **documentada** pero no **gobernada**: la única verificación
automática de fronteras era el job `architecture` de import-linter en CI. No había verificación
de SSOT (un literal podía redefinirse fuera de `shared/enums.py` sin que el CI fallara), ni
seguridad de cadena de suministro en CI (pip-audit), ni detección local previa al commit
(import-linter y mypy solo corrían en CI o manualmente), ni métricas de salud del kernel.

## Alternativas evaluadas

1. **Gobernanza automatizada completa** (elegida) — contratos BC-46/47/48 + scripts de SSOT y
   métricas + job `quality` en CI + dependabot + pre-commit hooks. Costo: mantenimiento de
   scripts y dependencia nueva (`pip-audit`). Beneficio: cualquier violación de frontera o
   duplicación rompe el CI de forma inmediata y accionable.
2. **pydeps como gate de ciclos en CI** — descartado: pydeps invoca un visor headless y falla con
   `CalledProcessError` en runners sin visor; además `--show-cycles` no ofrece un exit-code fiable.
   Las direcciones de dependencia que crearían ciclos ya están cubiertas por BC-01/33/34/45.
3. **JSON nativo de herramientas para métricas** — descartado: ni import-linter 2.6 ni mypy 1.19.1
   soportan salida JSON (verificado con `--help`). Se parsea texto verificado.
4. **Crear un workflow `ci.yml` separado** — descartado: `ocm-ci.yml` ya orquesta 4 jobs; se
   extiende con un job `quality` siguiendo el patrón existente.

## Decisión

1. **Contratos BC-46/47/48** en `architecture/importlinter.toml`: `shared.enums` es stdlib-only
   (sin deps internas), `shared.kafka` no importa dominio, `shared.utils` es genérico.
2. **Unificar literales** `OrderSide`/`PositionSide` en `shared/enums.py` (se elimina la
   duplicación en `types/order_events.py` y `types/position_events.py`).
3. **Scripts de gobernanza** en `scripts/`: `check_ssot_enums.py` (SSOT enforcement) y
   `metrics_report.py` (contratos/mypy/pytest/pip-audit → `architecture/metrics.json`).
4. **CI**: job `quality` en `ocm-ci.yml` con ruff, format, mypy, SSOT y pip-audit.
5. **SafeOps**: `pip-audit` en `[dependency-groups] dev` + Dependabot (ecosistema `pip`).
6. **Pre-commit**: hooks locales import-linter, mypy shared/, SSOT.
7. **pydeps queda fuera del CI** como gate; la visualización del grafo (si se quiere) es un
   artifact opcional con `pydeps ... -o shared.svg --noshow`.

## Justificación técnica

- El SSOT check fallaba al diseñarse: `OrderSide`/`PositionSide` estaban duplicados en
  `shared/types/order_events.py:35` y `position_events.py:35`. Se unifica en la raíz (opción
  recomendada, no ignore-list) para que el script pase limpio y la regla sea honesta.
- BC-46/47/48 pasan desde el primer run (verificado por inventario de imports: kafka y utils
  solo importan stdlib/`_base`), por lo que son gates de *regresión* sin deuda previa.
- BC-46/48 usan solo dependencias internas de `shared`; los BCs externos ya los bloquea BC-01
  (evita duplicación de mantenimiento).
- El parseo de texto de lint-imports/mypy fue validado contra la salida real de las versiones
  instaladas (import-linter 2.6, mypy 1.19.1).

## Consecuencias

- **Más fácil:** detectar en el commit y en CI cualquier violación de frontera del kernel,
  duplicación de literales, fallo de tipado o vulnerabilidad de dependencias.
- **Deuda aceptada:** `pip-audit .` en CI bloquea si aparecen vulnerabilidades nuevas en las deps
  declaradas. Risk-accept inicial (2026-08-03): `pyarrow` 19.0.1 (`PYSEC-2026-113`) y `ecdsa`
  0.19.2 transitiva de `python-jose` (`PYSEC-2026-1325`) — se ignoran con comentario en el
  workflow hasta validar la subida de pin en staging. El reporte de métricas usa `pip-audit -l`
  (entorno completo) y es informativo. Los scripts quedan bajo ruff/mypy globales (más superficie
  a mantener).
- **Contratos BC-NN:** BC-46, BC-47, BC-48 hacen cumplir la gobernanza; BC-01/33/34/45 permanecen.

## Referencias

- Código: `shared/enums.py`, `shared/types/order_events.py`, `shared/types/position_events.py`,
  `architecture/importlinter.toml`, `scripts/check_ssot_enums.py`,
  `scripts/metrics_report.py`, `.github/workflows/ocm-ci.yml`, `.github/dependabot.yml`,
  `.pre-commit-config.yaml`
- ADRs relacionados: ADR-0007 (naming de capas), ADR-0009 (kernel)
- Plan: `PLAN_REORGANIZACION_SHARED.md` → FASE 2
