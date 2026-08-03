# Plan del Shared Kernel — OrangeCashMachine

Documento raíz de planificación del kernel compartido (Shared Kernel) de OCM. Contiene dos fases:

| Fase | Estado | Contenido |
|---|---|---|
| **Fase 1 — Reorganización** | ✅ COMPLETADA (`dfe9c24`) | Separación del vocabulario de dominio del envelope wire Kafka + BC-45 |
| **Fase 2 — Gobernanza Automatizada** | 🚧 EN EJECUCIÓN | BC-46/47/48, scripts SSOT/métricas, job `quality` en CI, dependabot, pre-commit, ADR-0010 |

**Área:** Shared Kernel · **Fecha:** 2026-08-03

---

# FASE 1 — Reorganización del Shared Kernel (COMPLETADA)

> **Ejecutado y commiteado:** `dfe9c24` — gate verde (40 KEPT · 748 tests · ruff/mypy/pydeps OK).
> Plan original: extraer el vocabulario de dominio de `shared/kafka/schemas/_base.py` a `shared/enums.py`,
> re-exportar por compatibilidad, y blindar la dirección con BC-45. Checklist de pasos al final de esta fase.

---

## 1. Resumen Ejecutivo

`shared/kafka/schemas/_base.py` mezcla dos responsabilidades con orígenes y destinos distintos:

| Grupo | Contenido | Destino real |
|---|---|---|
| **A — Envelope wire Kafka** | `SchemaVersionError`, `KappaSourceMixin`, `BasePayload`, `_VALID_*` | Únicamente `shared/kafka/schemas/*` y `shared/kafka/serializer.py` |
| **B — Vocabulario de dominio** | `SignalDirection`, `OrderSide`, `PositionSide`, `DataSource`, `DATASOURCE_*` | Schemas **y** `shared/types/signal.py`, `shared/contracts/boundaries.py` |

Esta mezcla obliga a módulos de dominio (`types`, `contracts`) a importar desde `shared.kafka`, invirtiendo la jerarquía natural del kernel. Este plan extrae el Grupo B a un nuevo `shared/enums.py` (raíz, sin underscore, público), deja el Grupo A en `_base.py` (que re-exporta el vocabulario por compatibilidad), y blinda la dirección corregida con un contrato nuevo de import-linter (**BC-45**). Se corrige además un error documental (`BC-09` → `BC-01`) en `shared/__init__.py`.

Blast radius: **4 archivos de código** + 2 docstrings de estructura + 1 contrato.

---

## 2. Contexto

`shared/` es el Shared Kernel (DDD) de OrangeCashMachine: tipos canónicos (`types/`), contratos inter-BC (`contracts/`), excepciones (`exceptions/`), el SSOT del bus (`kafka/` con schemas, topics, serializer) y utilidades (`utils/`). Es el nivel más bajo del grafo (BC-01: dependency-free, solo stdlib + third-party).

### 2.1 Estado previo del área (plan de 8 fases, ya completado)

| Fase | Commit | Contenido |
|---|---|---|
| 1 | `dde5292` | `SchemaVersionError` único + literales SSOT en `_base` |
| 2 | `a4789c7` | `SCHEMA_VERSION` como `ClassVar`, SSOT de versión |
| 3 | `c4817d7` | Eliminación de huérfanos (`OHLCVBar`, `Rebalance*`, `OrderCancelled`) |
| 4 | `7c3fce9` | Eliminación de shims y alias `make_routing_key` |
| 5 | `fac3f6e` | Documentación alineada al estado real |
| 6 | `8ab4a96` | Docstring de `Signal` corregido |
| 7 | `f9c6d57` | `Signal` congelada (`frozen=True`) |
| 8 | `7d3e61f` | Cobertura de tests de schemas (108 nuevos; total 748) |

Esta reorganización es la continuación lógica: la Fase 1 centralizó los literales en `_base.py` por necesidad del bus; ahora se les da su hogar definitivo de dominio.

---

## 3. Diagnóstico y Evidencia

### 3.1 Dos responsabilidades en `_base.py` (evidencia empírica)

```python
# shared/kafka/schemas/_base.py

# Grupo A — Envelope y utilidades de Kafka (solo wire)
class SchemaVersionError(ValueError): ...
class KappaSourceMixin: ...            # source, is_live, is_backfill, is_replay
class BasePayload: ...                 # event_id, occurred_at, SCHEMA_VERSION, to_dict()
_VALID_SOURCES, _VALID_SIGNAL_DIRECTIONS

# Grupo B — Literales de dominio (neutros, sin dependencias externas)
SignalDirection = Literal["buy", "sell", "hold"]
OrderSide      = Literal["buy", "sell"]
PositionSide   = Literal["long", "short"]
DataSource     = Literal["live", "backfill", "replay"]
```

`BasePayload`/`KappaSourceMixin`/`SchemaVersionError` son **mecanismo de transporte wire** (uuid, datetime, `to_dict`/`from_dict` JSON, versionado). Los literales son **vocabulario de dominio** que no debería conocer Kafka.

### 3.2 Consumidores reales (inventario completo con `grep`)

Referencias a `shared.kafka.schemas._base` — **12 imports absolutos, 0 relativos, 0 alternativos** (`from ._base`, `import shared.kafka.schemas as ...` → ninguno):

| Archivo | Importa | Grupo |
|---|---|---|
| `shared/kafka/serializer.py:37` | `BasePayload` | A |
| `shared/kafka/schemas/ohlcv.py:50` | `BasePayload`, `SchemaVersionError`, `DataSource`, `DATASOURCE_*`, `_VALID_SOURCES` | A+B |
| `shared/kafka/schemas/signals.py:43` | `SignalDirection`, `_VALID_SIGNAL_DIRECTIONS`, `SchemaVersionError` | A+B |
| `shared/kafka/schemas/orders.py:55` | `BasePayload`, `OrderSide`, `SchemaVersionError` | A+B |
| `shared/kafka/schemas/positions.py:39` | `BasePayload`, `PositionSide`, `SchemaVersionError` | A+B |
| `shared/kafka/schemas/trades.py:42` | `BasePayload`, `DataSource`, `DATASOURCE_*`, `_VALID_SOURCES`, `KappaSourceMixin` | A+B |
| `shared/kafka/schemas/orderbook.py:40` | `BasePayload`, `SchemaVersionError` | A |
| `shared/kafka/schemas/funding.py:37` | `BasePayload`, `SchemaVersionError` | A |
| `shared/kafka/schemas/oi.py:38` | `BasePayload`, `SchemaVersionError` | A |
| `shared/kafka/schemas/liquidations.py:38` | `BasePayload`, `SchemaVersionError` | A |
| `shared/types/signal.py:32` | `SignalDirection` | **B — dominio** |
| `shared/contracts/boundaries.py:23` | `SignalDirection` | **B — dominio** |

**Conclusión:** el vocabulario (B) se usa fuera de Kafka; el envelope (A) solo dentro. Los 9 schemas + serializer mantienen su import a `_base` sin cambios.

### 3.3 Violación de la jerarquía del kernel

```python
# shared/types/signal.py:32
from shared.kafka.schemas._base import SignalDirection

# shared/contracts/boundaries.py:23
from shared.kafka.schemas._base import SignalDirection
```

Un módulo fundamental del kernel (`types`, `contracts`) depende de un módulo de transporte (`kafka`). Nota de precisión: `shared/kafka` es parte del Shared Kernel (no "infraestructura de BC" en sentido Clean Architecture), por lo que la corrección se encuadra como **inversión de jerarquía dentro del kernel** (núcleo → detalle de transporte), no como violación de DIP entre capas de un BC.

### 3.4 Hallazgos documentales

1. **`shared/__init__.py:16`** dice `Regla de dependencia (BC-09 en pyproject.toml)`. La regla de dependency-free de `shared` es **BC-01** (`architecture/importlinter.toml:52`). BC-09 es otra cosa (`market_data.domain` no importa third-party de infra). Error documental real → se corrige.
2. **`BC-33` protege la dirección contraria**: prohíbe `shared.kafka.schemas → shared.types`, pero **nada impide** `shared.types → shared.kafka`. Sin un contrato nuevo, el fix quedaría como evidencia puntual, no como guardrail. → Se añade **BC-45**.

### 3.5 Herramientas de medición

`arch_metrics --json` **no existe** en el repositorio (`uv run arch_metrics` → `Failed to spawn`). La evidencia cuantitativa se obtiene con las herramientas reales del proyecto:
- `uv run pydeps shared --max-bacon 4` (dev-deps) — grafo de dependencias, detección de ciclos.
- `uv run lint-imports --config architecture/importlinter.toml` — gate de contratos.
- Suite de tests completa.

---

## 4. Decisión de Diseño

### 4.1 Por qué separar (y no "mover todo a la raíz")

La opción de mover `_base.py` completo a `shared/base.py` fue evaluada y **descartada**: arrastraría `BasePayload`/`KappaSourceMixin`/`SchemaVersionError` (serialización Kafka) al nivel más fundamental del kernel, contaminando el núcleo con mecanismos de transporte. La separación resuelve el problema real (acoplamiento `types`/`contracts` → kafka) con el menor blast radius y sin degradar la pureza del envelope.

### 4.2 Arquitectura destino

```
shared/
  enums.py                      ← NUEVO: vocabulario de dominio (SSOT). Solo stdlib.
  types/signal.py               → importa de shared.enums
  contracts/boundaries.py       → importa de shared.enums
  kafka/schemas/_base.py        → envelope + re-export de enums (compatibilidad)
  kafka/schemas/*.py            → SIN cambios (siguen importando de _base)
  kafka/serializer.py           → SIN cambios
```

**Regla de direcciones:** `enums.py` no importa nada de `shared` (solo stdlib, BC-01). `_base.py` importa de `enums` (dirección permitida). `types`/`contracts` importan de `enums`. `kafka/schemas` solo importa de `_base`.

### 4.3 Principios aplicados

| Principio | Cumplimiento |
|---|---|
| **SSOT** | `enums.py` es el único punto de definición; `_base.py` re-exporta (alias, no redefine). |
| **DIP** | Dominio (`types`/`contracts`) depende de `enums` (abstracción sin deps), no de `kafka` (detalle). |
| **Clean Architecture** | El núcleo del kernel no conoce detalles de mensajería. |
| **DRY** | Elimina la fragmentación del vocabulario cross-BC. |
| **KISS** | Blast radius mínimo: 4 archivos de código. |
| **Fail-fast** | Imports inválidos se detectan en tiempo de carga. |
| **Guardrail permanente** | BC-45 enforced por CI. |
| **SafeOps** | Validación con linters + tests + pydeps. |

---

## 5. Plan de Ejecución Detallado

### Checklist de estado

| Paso | Acción | Estado |
|---|---|---|
| 0 | Baseline (pydeps, lint-imports 39 KEPT, pytest 748) | ✅ 39 KEPT · 748 passed · pydeps sin ciclos |
| 1 | Crear `shared/enums.py` | ✅ `shared/enums.py` creado |
| 2 | Modificar `_base.py` | ✅ Re-export desde `shared.enums` + `_VALID_*` en `__all__` |
| 3 | Actualizar imports en `signal.py` y `boundaries.py` | ✅ `from shared.enums import SignalDirection` |
| 4 | Corregir docstrings (`shared/__init__.py`, `shared/kafka/__init__.py`) | ✅ BC-01 + estructura + re-export |
| 5 | Añadir BC-45 en `importlinter.toml` + comentario BC-33 | ✅ `[[tool.importlinter.contracts]]` nuevo |
| 6 | Validación completa (ruff, mypy, lint-imports 40 KEPT, pytest, pydeps, residuales) | ✅ 40 KEPT · 748 passed · ruff/mypy/pydeps OK · residuales 0 |
| 7 | Commit atómico | ✅ `dfe9c24` — pre-commit hooks OK (ruff check + format) |

### Paso 0 — Baseline (evidencia previa)

```bash
uv run pydeps shared --max-bacon 4        # capturar grafo previo (sin ciclos)
uv run lint-imports --config architecture/importlinter.toml   # 39 KEPT
uv run pytest tests/ -x -q                # 748 passed
```

### Paso 1 — Crear `shared/enums.py` (nuevo)

Contenido íntegro propuesto:

```python
# -*- coding: utf-8 -*-
"""
shared/enums.py
================

Literales de dominio neutros (sin dependencias internas de shared/).
SSOT de las enumeraciones compartidas entre bounded contexts.

Independiente de transporte: NO importa nada de shared.kafka ni de
ningún bounded context — solo stdlib (typing). BC-01 safe.
"""

from typing import Literal

SignalDirection = Literal["buy", "sell", "hold"]
"""Dirección de señal de trading: 'buy' | 'sell' | 'hold'."""

OrderSide = Literal["buy", "sell"]
"""Lado de una orden: 'buy' | 'sell'."""

PositionSide = Literal["long", "short"]
"""Lado de una posición: 'long' | 'short'."""

DataSource = Literal["live", "backfill", "replay"]
"""Origen Kappa de un dato de mercado."""

DATASOURCE_LIVE: DataSource = "live"
DATASOURCE_BACKFILL: DataSource = "backfill"
DATASOURCE_REPLAY: DataSource = "replay"

_VALID_SOURCES: frozenset[str] = frozenset({"live", "backfill", "replay"})
_VALID_SIGNAL_DIRECTIONS: frozenset[str] = frozenset({"buy", "sell", "hold"})

__all__ = [
    "SignalDirection",
    "OrderSide",
    "PositionSide",
    "DataSource",
    "DATASOURCE_LIVE",
    "DATASOURCE_BACKFILL",
    "DATASOURCE_REPLAY",
    "_VALID_SOURCES",
    "_VALID_SIGNAL_DIRECTIONS",
]
```

Notas:
- Los docstrings de una línea de cada literal se conservan (existían en `_base.py`).
- Solo `typing` de stdlib → BC-01 intacto.
- Sin imports de `shared/` → cero riesgo de ciclos.

### Paso 2 — Modificar `shared/kafka/schemas/_base.py`

**4 sub-cambios (refinado tras la revisión):**

**a) Eliminar las definiciones del Grupo B** (bloque `SignalDirection` … `_VALID_SIGNAL_DIRECTIONS`).

**b) Añadir el re-export desde `shared.enums`:**

```python
from shared.enums import (
    DATASOURCE_BACKFILL,
    DATASOURCE_LIVE,
    DATASOURCE_REPLAY,
    DataSource,
    OrderSide,
    PositionSide,
    SignalDirection,
    _VALID_SIGNAL_DIRECTIONS,
    _VALID_SOURCES,
)
```

**c) Reemplazar el bloque de comentario actual (líneas 70–76)** — explica la dirección ANTIGUA (`types/contracts → kafka.schemas._base`) que BC-45 invierte. Si se deja, queda con información falsa. Sustituir por:

```python
# =============================================================================
# Literales cross-wire — re-exportados desde shared.enums (SSOT real)
# =============================================================================
# BC-45 exige que shared.types y shared.contracts importen desde shared.enums,
# nunca desde este módulo. _base.py re-exporta por compatibilidad con los 9
# schemas y el serializer, que siguen importando solo de aquí (BC-33).
```

**d) Añadir `"_VALID_SOURCES"` y `"_VALID_SIGNAL_DIRECTIONS"` al `__all__` de `_base.py`** — **bloqueante de gate**: al no estar en `__all__` y no usarse internamente en `_base.py`, ruff **F401** los marcaría como "imported but unused". Añadirlos a `__all__` documenta el re-export intencional (BC-33: los schemas solo importan de `_base`) y evita F401. Los literales públicos (`SignalDirection`, `OrderSide`, `PositionSide`, `DataSource`, `DATASOURCE_*`) **ya están en `__all__`** → sin cambios para ellos.

- Conservar `SchemaVersionError`, `KappaSourceMixin`, `BasePayload`.
- Actualizar el docstring del módulo (encabezado, líneas 1–38): `_base.py` = envelope del bus; los literales viven en `shared.enums` (raíz) y se re-exportan por compatibilidad.

**Impacto cero en consumidores:** los 9 schemas y `serializer.py` no se tocan; siguen importando desde `_base`.

### Paso 3 — Actualizar imports de dominio

```bash
# shared/types/signal.py
sed -i 's/from shared\.kafka\.schemas\._base import SignalDirection/from shared.enums import SignalDirection/g' shared/types/signal.py

# shared/contracts/boundaries.py
sed -i 's/from shared\.kafka\.schemas\._base import SignalDirection/from shared.enums import SignalDirection/g' shared/contracts/boundaries.py
```

Además, en `shared/types/signal.py` actualizar el comentario (líneas 28–31), que aún referencia `_base`:
- Antes: `# SignalType re-exporta SignalDirection desde _base (BC-33: el SSOT de los literales wire vive en shared.kafka.schemas._base; types solo re-expone).`
- Después: `# SignalType re-exporta SignalDirection desde shared.enums (BC-45: el SSOT del vocabulario de dominio vive en la raíz del kernel; types solo re-expone).`

### Paso 4 — Docstrings de estructura

- **`shared/__init__.py`**:
  - Corregir `Regla de dependencia (BC-09 en pyproject.toml):` → `Regla de dependencia (BC-01 en architecture/importlinter.toml):`.
  - Añadir `enums.py` a la estructura del kernel, p. ej.: `enums.py     — vocabulario de dominio cross-BC (literales, SSOT de enums)`.

- **`shared/kafka/__init__.py`**: actualizar la línea de estructura del paquete `schemas/`:
  - Antes: `_base.py — BasePayload, SchemaVersionError, KappaSourceMixin, literales`
  - Después: `_base.py — envelope wire (BasePayload, SchemaVersionError, KappaSourceMixin); literales re-exportados desde shared.enums`

- **`shared/kafka/schemas/__init__.py`**: sin cambios (no referencia `_base`).

### Paso 5 — Contratos de arquitectura (`architecture/importlinter.toml`)

**5.1 Contrato NUEVO BC-45** (formato real del archivo, con la clave completa):

```toml
[[tool.importlinter.contracts]]
# shared.types y shared.contracts son vocabulario de dominio neutro — el SSOT de
# literales cross-BC vive en shared.enums (raíz del kernel). No pueden depender
# del transporte Kafka (shared.kafka). La dirección correcta es: types/contracts
# → shared.enums → (kafka/schemas re-exporta desde enums).
name = "BC-45: shared.types/contracts do not import shared.kafka (vocabulary isolated from transport)"
type = "forbidden"
source_modules = ["shared.types", "shared.contracts"]
forbidden_modules = ["shared.kafka"]
```

Tras el Paso 3, `types`/`contracts` quedan sin imports de `shared.kafka` → BC-45 pasa desde el primer run.

**5.2 BC-33 — actualizar comentario del docstring** (el contrato en sí no cambia): la "regla de oro" pasa a admitir que `_base.py` re-exporta desde `shared.enums`. Los schemas siguen importando solo de `_base` (cumplen BC-33).

**5.3 Contratos no afectados:** BC-01 (`shared` dependency-free — `enums.py` solo stdlib), BC-34 (`shared` neutral tooling), BC-32 (`shared.kafka` no importa `market_data.infrastructure`), BC-35 (los BCs siguen sin poder importar `_base`; `shared.enums` queda legítimamente accesible como vocabulario de dominio). **BC-35 sin cambios.**

### Paso 6 — Validación y evidencia

```bash
# Evidencia de acoplamiento (sustituye a arch_metrics, que no existe)
uv run pydeps shared --max-bacon 4        # post: mismo grafo, sin ciclos

# Gate completo
uv run ruff check .
uv run ruff format . --check              # excepción conocida: packages/market_data/infrastructure/runtime/supervisor.py (drift pre-existente, untracked)
uv run lint-imports --config architecture/importlinter.toml   # 40 KEPT / 0 broken
uv run mypy shared/                        # Success
uv run pytest tests/ -x -q                 # 748 passed

# Comprobación residual
rg "shared.kafka" shared/types shared/contracts    # → 0 matches
rg "kafka.schemas._base" --glob '*.py' .            # → solo docstrings/comentarios internos de shared/kafka
rg "_VALID_SOURCES|_VALID_SIGNAL_DIRECTIONS" tests/  # → 0 matches (re-export seguro)
```

**Evidencia cerrada en la revisión:**
- `pyproject.toml:244` → `strict = false` → `no_implicit_reexport` **inactivo** (solo se activa con strict). El re-export implícito en `_base.py` es **mypy-compatible**; el gate `mypy shared/` lo confirma.
- `pyproject.toml [tool.ruff]` sin `preview = true` → la regla **PLC2701** (private import) **no bloquea CI** (el sub-cambio d del Paso 2 se hace por F401, no por PLC2701).
- **0 tests** importan `_VALID_*` directamente → el re-export en `_base.py` es seguro y suficiente.

### Paso 7 — Commit atómico

```bash
git add shared/ architecture/importlinter.toml
git commit -m "refactor(shared): separar vocabulario de dominio (shared/enums.py) del envelope Kafka

- Crea shared/enums.py con los literales cross-BC (SignalDirection, OrderSide,
  PositionSide, DataSource, DATASOURCE_*, _VALID_*) — solo stdlib (BC-01).
- shared/kafka/schemas/_base.py conserva el envelope wire (BasePayload,
  KappaSourceMixin, SchemaVersionError) y re-exporta los literales desde
  shared.enums por compatibilidad (los 9 schemas y serializer no cambian).
- shared/types/signal.py y shared/contracts/boundaries.py importan desde
  shared.enums — se elimina el acoplamiento dominio → kafka.
- BC-45: prohíbe a shared.types/contracts importar shared.kafka (guardrail).
- Corrige docstring de shared/__init__.py (BC-09 → BC-01) y actualiza
  estructura del kernel (enums.py).
- Blast radius: 4 archivos de código. Verificado con pydeps + lint-imports
  (40 KEPT) + pytest (748)."
```

---

## 6. Criterios de Aceptación

1. `shared.enums` es el **único punto de definición** del vocabulario (SSOT).
2. **Cero imports** de `shared.kafka` en `shared/types/` y `shared/contracts/`.
3. Los **9 schemas + serializer** siguen importando de `_base` **sin modificaciones** (compatibilidad total).
4. **BC-45 pasa** desde el primer run y queda enforced en CI.
5. Contratos existentes intactos: **40 KEPT / 0 broken** (BC-01, BC-33, BC-34, BC-35, BC-32).
6. **748 tests passed**, `mypy shared/` Success, ruff limpio, pydeps sin ciclos.
7. Documentación coherente: `shared/__init__.py` (BC-01 + estructura), `shared/kafka/__init__.py` (re-export).

---

## 7. Riesgos y Mitigación

| Riesgo | Probabilidad | Mitigación |
|---|---|---|
| Import con formato alternativo no capturado por sed | Baja | Inventario completo previo con `rg` (12 imports, todos absolutos); comprobación residual post-cambio. |
| Romper compatibilidad de los schemas | Baja | Re-export en `_base.py` mantiene el namespace; los schemas no se tocan; tests de Fase 8 cubren round-trip/versión. |
| Ciclos de import | Nula | `enums.py` no importa nada de `shared`; `_base → enums` es unidireccional. |
| BC-35 roto | Baja | `shared.enums` no es `shared.kafka`; los BCs no importan `_base` (verificado). |
| Reintroducción futura de la dependencia | Media | **BC-45** la bloquea en CI. |
| Regresión de cobertura | Baja | 108 tests de schemas (Fase 8) validan los literales vía `_base` re-exportado. |

---

## 8. Fuera de Alcance (no se ejecuta en este plan)

1. **Duplicación de `OrderSide`/`PositionSide`**: `shared/types/order_events.py:35` y `shared/types/position_events.py:35` definen literales propios, y `shared/types/__init__.py` exporta esos (no los de `_base`). Unificar en `shared.enums` es un refactor **separado** (amplía el blast radius y toca decisiones previas de `shared.types`). Se documenta como trabajo futuro.
2. **Enum de `trading/execution/order.py`**: `class OrderSide(str, Enum)` es un modelo distinto del BC `trading`; queda fuera del alcance del kernel.
3. **Drift pre-existente**: `packages/market_data/infrastructure/runtime/supervisor.py` (formato + 2 errores mypy) y `packages/market_data/application/strategies/incremental.py` (2 errores mypy) — no se tocan en este cambio (archivos ajenos a `shared/`).

---

## 9. Archivos Afectados

| Archivo | Acción |
|---|---|
| `shared/enums.py` | **Nuevo** |
| `shared/kafka/schemas/_base.py` | Modificar (quitar Grupo B, re-export, bloque comentario 70–76, `_VALID_*` en `__all__`) |
| `shared/types/signal.py` | Modificar (import + comentario) |
| `shared/contracts/boundaries.py` | Modificar (import) |
| `shared/__init__.py` | Modificar (BC-09→BC-01 + estructura) |
| `shared/kafka/__init__.py` | Modificar (estructura) |
| `architecture/importlinter.toml` | Modificar (BC-45 + comentario BC-33) |

---

## Anexo A — Decisiones registradas (trazabilidad)

- 2026-08-03: Se descarta "mover `_base.py` completo a `shared/base.py`" (arrastraría serialización Kafka al núcleo). Se adopta la **separación** vocabulario/envelope con re-export por compatibilidad.
- 2026-08-03: Nombre del módulo raíz: **`shared/enums.py`** (no `literals.py`).
- 2026-08-03: Evidencia cuantitativa: **`pydeps` + import-linter** (no `arch_metrics`, inexistente en el repo).
- 2026-08-03: Hallazgo documental: `shared/__init__.py` cita `BC-09`; la regla real es `BC-01` → se corrige.
- 2026-08-03 (revisión): **`_VALID_*` en el `__all__` de `_base.py`** — necesario para evitar ruff F401 (imports re-exportados sin uso interno). No es por PLC2701 (regla preview, no bloquea CI).
- 2026-08-03 (revisión): **No se renombra `_VALID_*` → `VALID_*`** en `enums.py`. Es cosmético (PLC2701 no bloquea sin `--preview`), y obligaría a tocar los 3 schemas que los importan vía `_base`. Queda como opción futura.
- 2026-08-03 (revisión): **No se usa `get_args(DataSource)`** para SSOT de los valores — sin precedente en el repo (0 matches). Se mantiene el idioma actual (`Literal` + frozenset de validación).
- 2026-08-03 (revisión): **`mypy` con `strict=false`** → `no_implicit_reexport` inactivo; el re-export implícito es compatible. Confirmado con `pyproject.toml:244`.
- 2026-08-03 (revisión): **0 tests** importan `_VALID_*` directamente → el re-export por compatibilidad es seguro y suficiente.

---

# FASE 2 — Gobernanza Automatizada del Shared Kernel (EJECUTADA — pendiente commits)

> Continuación de la Fase 1: pasar de una arquitectura *documentada* a una arquitectura *gobernada por
> automatización* — reglas ejecutables en CI, contratos de arquitectura, SSOT enforcement, seguridad de
> cadena de suministro y métricas permanentes.

**Estado:** Ejecutada y validada (Fases 1–8 completadas, Fase 9 validada) · **Fecha:** 2026-08-03
**Gate de aceptación:** ruff ✅ · format ✅ · import-linter (**43 KEPT / 0 broken**) ✅ · mypy (shared/ + scripts/ limpio) ✅ · pytest (**748 passed**) ✅ · SSOT ✅ · pip-audit ✅ (2 risk-accept) · bandit ✅ (1 Medium pre-existente, 0 nuevos)

### Checklist de estado

| Fase | Acción | Estado |
|---|---|---|
| 1 | Unificar `OrderSide`/`PositionSide` en `shared/enums.py` (+ `_VALID_*`) | ✅ consumidores importan de `shared.enums`; ruff/mypy OK |
| 2 | Contratos BC-46/47/48 en `importlinter.toml` | ✅ 40 → **43 KEPT / 0 broken** |
| 3 | `pip-audit` en `[dependency-groups] dev` + `uv.lock` | ✅ `pip-audit>=2.7,<3.0` (2.10.1 instalada) |
| 4 | Scripts `check_ssot_enums.py` + `metrics_report.py` | ✅ creados, tipados, ruff OK; `check_ssot` pasa; `metrics_report` genera `architecture/metrics.json` |
| 5 | Job `quality` en `ocm-ci.yml` | ✅ añadido (ruff · format · mypy shared · SSOT · pip-audit) |
| 6 | `.github/dependabot.yml` | ✅ ecosistema pip, weekly, grupo dev-dependencies |
| 7 | Pre-commit: import-linter + mypy-shared + ssot-enums | ✅ hooks locales añadidos |
| 8 | ADR-0010 + `GOVERNANCE.md` §8 + README | ✅ documentados |
| 9 | Validación final | ✅ ruff/format/lint-imports/pytest/SSOT/pip-audit/bandit; `mypy .` → 21 errores pre-existentes fuera de alcance (ninguno en `shared/` ni `scripts/`) |

### Incidencias encontradas en ejecución (Fase 9)

| # | Problema | Resolución |
|---|---|---|
| A | `pip-audit --json` y `--requirement pyproject.toml` NO existen/fallan | CI usa `pip-audit .` (PEP 621); métricas usan `pip-audit -l -f json` |
| B | JSON de pip-audit anida vulns en `dependencies[].vulns` | `_audit_vulns()` suma por dependencia (56 vulns en entorno, informativo) |
| C | `pip-audit .` detecta 2 vulns (`pyarrow`, `ecdsa`) | Risk-accept con `--ignore-vuln` documentado en el workflow + ADR-0010 |

---

## 1. Resumen Ejecutivo

Ocho fases que blindan el kernel con automatización:

| Fase | Contenido | Resultado |
|---|---|---|
| 1 | Unificar literales duplicados `OrderSide`/`PositionSide` en `shared/enums.py` | Desbloquea el SSOT check |
| 2 | Contratos BC-46/47/48 | 40 → **43 KEPT** |
| 3 | Añadir `pip-audit` a dev deps (+ `uv.lock`) | SafeOps en CI |
| 4 | Scripts `scripts/check_ssot_enums.py` + `scripts/metrics_report.py` | SSOT enforcement + métricas |
| 5 | Job `quality` en `ocm-ci.yml` | ruff · format · mypy · SSOT · pip-audit |
| 6 | `.github/dependabot.yml` | Actualización de deps automatizada |
| 7 | Pre-commit: hooks de arquitectura y SSOT | Detección local pre-commit |
| 8 | ADR-0010 + `GOVERNANCE.md` + README | Documentación |

## 2. Estado actual (baseline verificado)

Verificado empíricamente antes de redactar este plan (nada asumido):

- `uv run lint-imports --config architecture/importlinter.toml` → **40 kept / 0 broken**.
- `uv run pytest tests/ -x -q` → **748 passed**.
- `uv run mypy shared/` → `Success: no issues found in 26 source files` (mypy **1.19.1**).
- `uv run ruff check .` limpio (salvo drift pre-existente `supervisor.py`, untracked, fuera de alcance).
- `.github/workflows/` ya contiene `ocm-ci.yml` (4 jobs) y `ocm-cd.yml` (placeholder).
- No existe directorio `scripts/`. `pip-audit` no está en `[dependency-groups] dev`.
- `docs/architecture/GOVERNANCE.md` existe (66 líneas); ADRs hasta `ADR-0009`; existe `ADR-template.md`.

### 2.1 Estructura final de `shared/` (confirmada tras la Fase 1)

```
shared/
├── enums.py          # Vocabulario puro de dominio (SSOT). Solo stdlib (BC-01).
├── types/            # Value objects, entidades, eventos de dominio.
├── contracts/        # Protocolos (interfaces) entre bounded contexts.
├── exceptions/       # Excepciones base compartidas.
├── kafka/            # Envelope wire, schemas, serializer, topics.
│   └── schemas/
│       ├── _base.py  # Envelope (BasePayload, KappaSourceMixin, SchemaVersionError)
│       └── ...       # 9 schemas (ohlcv, signals, orders, positions, trades, …)
└── utils/            # Utilidades genéricas (repo_root, etc.).
```

Regla de dependencias (definitiva):
- `enums.py` → 0 dependencias internas (solo stdlib).
- `types/`, `contracts/`, `exceptions/`, `utils/` → pueden importar de `enums` (+ stdlib/third-party).
- `kafka/` → puede importar de `enums` (vía `_base`), `exceptions`, `utils`; nunca de `types/` ni `contracts/` (BC-47).
- Ningún módulo de `shared/` importa de BCs externos (BC-01).

## 3. Hallazgos que corrigieron el plan original

Auditoría read-only previa (todo verificado):

| # | Supuesto del plan original | Realidad verificada | Corrección aplicada |
|---|---|---|---|
| 1 | Crear `.github/workflows/ci.yml` | Ya existe `ocm-ci.yml` con 4 jobs | **Modificar** `ocm-ci.yml` (job `quality` nuevo) |
| 2 | Script SSOT pasa en CI | **Falla de inmediato**: `OrderSide` duplicado en `shared/types/order_events.py:35` y `PositionSide` en `shared/types/position_events.py:35` | **Fase 1**: unificar en `shared.enums` |
| 3 | `lint-imports --json` | No existe en import-linter 2.6 | Parsear texto: `Contracts: (\d+) kept, (\d+) broken.` |
| 4 | `mypy --json` | **Confirmado: no existe** en mypy 1.19.1 (`mypy shared/ --help \| grep -i json` → sin coincidencias) | Parsear texto: `Success: no issues found` / `Found N errors` |
| 5 | `uv run pip-audit` | `pip-audit` **no está** en `[dependency-groups] dev` | **Fase 3**: añadir dep + `uv.lock` |
| 6 | BC-46 con `"apps"` en forbidden_modules | `"apps"` no es root_package (los reales: `app`, `api`, `research`); externos ya cubiertos por BC-01 | BC-46/48 usan **solo dependencias internas** de `shared` |
| 7 | pydeps como gate de bloqueo | El visor headless rompe (CalledProcessError) sin `--no-show`; `--no-output` **implica** `--no-show` pero `--show-cycles` no tiene exit-code fiable verificado | **Eliminar pydeps del CI** como gate. Opcional: artifact SVG con `--noshow` |
| 8 | `uv sync --frozen` en CI | Los 4 jobs usan `uv sync` sin frozen | Fuera de alcance (ver §8) |

## 4. Fases de ejecución detalladas

### Fase 1 — Unificar literales duplicados (desbloquea SSOT)

**Por qué:** el script SSOT detecta definiciones de literales fuera de `shared/enums.py`. Hoy hay 2 violaciones reales (deuda documentada en la Fase 1 del plan). Se resuelve la raíz, no se parchea el script.

| Archivo | Cambio |
|---|---|
| `shared/enums.py` | Añadir `_VALID_ORDER_SIDES: frozenset[str]` y `_VALID_POSITION_SIDES: frozenset[str]` + incluirlos en `__all__` |
| `shared/types/order_events.py` | Eliminar `OrderSide = Literal[...]` y `_VALID_ORDER_SIDES` locales (líneas 34–36) → `from shared.enums import OrderSide, _VALID_ORDER_SIDES`; quitar `Literal` del import de `typing` |
| `shared/types/position_events.py` | Idem con `PositionSide`, `_VALID_POSITION_SIDES` (conservar `Optional`, sigue en uso) |

**Seguridad del cambio (verificado):**
- `packages/trading/*` importa `OrderSide`/`PositionSide` vía `shared.types` (re-export) → sin romper.
- `shared/types/__init__.py` no cambia (sigue re-exportando desde `order_events`/`position_events`).
- Schemas `orders.py`/`positions.py` importan de `_base` (re-export desde `enums`) → sin cambios.
- **No se toca `_base.py`**: los `_VALID_ORDER_*` no se usan en el wire (verificado con `rg`).

**Validación:** `ruff check .` · `mypy shared/` · `pytest tests/ -x -q` (748+) · lint-imports (40 KEPT, sin cambio).

### Fase 2 — Contratos BC-46/47/48 (`architecture/importlinter.toml`)

Se insertan tras BC-35 (fin del bloque TIER 2 de shared):

```toml
[[tool.importlinter.contracts]]
# BC-46: shared.enums es vocabulario puro — cero dependencias internas de shared.
# Los BCs externos ya los cubre BC-01 (fuente shared completa).
name = "BC-46: shared.enums is stdlib-only (no internal shared deps)"
type = "forbidden"
source_modules = ["shared.enums"]
forbidden_modules = [
    "shared.types", "shared.contracts", "shared.exceptions",
    "shared.kafka", "shared.utils",
]

[[tool.importlinter.contracts]]
# BC-47: shared.kafka es transporte — no conoce dominio (types/contracts).
# Verificado: pasa desde el primer run (kafka solo importa stdlib + _base).
name = "BC-47: shared.kafka does not import domain (types/contracts)"
type = "forbidden"
source_modules = ["shared.kafka"]
forbidden_modules = ["shared.types", "shared.contracts"]

[[tool.importlinter.contracts]]
# BC-48: shared.utils es genérico — sin dominio ni negocio.
name = "BC-48: shared.utils is generic (no domain/business)"
type = "forbidden"
source_modules = ["shared.utils"]
forbidden_modules = [
    "shared.types", "shared.contracts", "shared.kafka",
    "market_data", "trading", "portfolio",
]
```

**Resultado: 43 KEPT / 0 broken** (BC-46, BC-47, BC-48 pasan desde el primer run — verificado con `rg` de imports reales).

### Fase 3 — Dependencia `pip-audit` (dev)

- `pyproject.toml` → `[dependency-groups] dev`: añadir `"pip-audit>=2.7,<3.0"` (junto a bandit).
- `uv lock` → actualiza `uv.lock`. **Nota:** es un cambio de dependencia real (no el caso prohibido de AGENTS.md), commiteable.
- Uso: `uv run pip-audit --requirement pyproject.toml`.

### Fase 4 — Scripts de gobernanza (`scripts/`, directorio nuevo)

**Restricciones aplicables (verificadas):** ruff `select = ["E","F","I"]` con `line-length = 120`, y `uv run mypy .` cubre `scripts/` (mypy no excluye `scripts/`). Ambos scripts deben ser compatibles.

**4.1 `scripts/check_ssot_enums.py`** — verifica que los literales de dominio solo se definan en `shared/enums.py`:

```python
#!/usr/bin/env python3
"""SSOT enforcement: literales de dominio solo se definen en shared/enums.py."""
import re
import sys
from pathlib import Path

LITERALS = ["OrderSide", "PositionSide", "SignalDirection", "DataSource"]
ENUMS_FILE = Path("shared/enums.py")

errors: list[str] = []
for lit in LITERALS:
    pattern = re.compile(rf"^{lit}\s*=", re.MULTILINE)
    for py_file in Path("shared").rglob("*.py"):
        if py_file == ENUMS_FILE:
            continue
        if pattern.search(py_file.read_text(encoding="utf-8")):
            errors.append(f"SSOT violado: {lit} definido también en {py_file}")

if errors:
    print("\n".join(errors))
    sys.exit(1)
print("OK: todos los literales viven en shared/enums.py")
```

Tras la Fase 1 pasa limpio (verificado por simulación: solo encuentra las 2 violaciones que la Fase 1 elimina).

**4.2 `scripts/metrics_report.py`** — informe de salud del kernel. **Corregido** respecto al plan original: ni `lint-imports` ni `mypy` tienen salida JSON, se parsea texto:

```python
#!/usr/bin/env python3
"""Informe de salud del Shared Kernel: contratos, mypy, pytest, pip-audit."""
import json
import re
import subprocess
from pathlib import Path

def _run(cmd: list[str]) -> str:
    r = subprocess.run(cmd, capture_output=True, text=True)
    return r.stdout + r.stderr

def _contracts() -> tuple[int, int]:
    out = _run(["uv", "run", "lint-imports", "--config", "architecture/importlinter.toml"])
    m = re.search(r"Contracts: (\d+) kept, (\d+) broken\.", out)
    return (int(m.group(1)), int(m.group(2))) if m else (0, 0)

def _mypy_errors() -> int:
    out = _run(["uv", "run", "mypy", "shared/"])
    if "Success: no issues found" in out:
        return 0
    m = re.search(r"Found (\d+) errors?", out)
    return int(m.group(1)) if m else -1

def _pytest_passed() -> int:
    out = _run(["uv", "run", "pytest", "tests/", "-q", "--no-header"])
    m = re.search(r"(\d+) passed", out)
    return int(m.group(1)) if m else 0

def _audit_vulns() -> int:
    r = subprocess.run(
        ["uv", "run", "pip-audit", "--requirement", "pyproject.toml", "--json"],
        capture_output=True, text=True,
    )
    try:
        data = json.loads(r.stdout or r.stderr)
        return len(data.get("vulnerabilities", []))
    except json.JSONDecodeError:
        return -1

def main() -> None:
    kept, broken = _contracts()
    report = {
        "contracts_kept": kept,
        "contracts_broken": broken,
        "mypy_errors": _mypy_errors(),
        "pytest_passed": _pytest_passed(),
        "vulnerabilities": _audit_vulns(),
    }
    print(json.dumps(report, indent=2))
    Path("architecture/metrics.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )

if __name__ == "__main__":
    main()
```

**4.3 `.gitignore`**: añadir `architecture/metrics.json` y `shared.svg` (el gráfico generado por pydeps nunca debe commitearse).

### Fase 5 — CI: job `quality` en `ocm-ci.yml` (modificar, NO crear `ci.yml`)

Añadir job nuevo, siguiendo el patrón existente (`needs: architecture`, `uv sync --group dev`):

```yaml
  # ──────────────────────────────────────────────────────────────────────────
  # JOB 4: Quality gates
  # ruff + format + mypy + SSOT + pip-audit. Corre tras architecture (fail-fast).
  # Cualquier fallo aquí impide el merge.
  # ──────────────────────────────────────────────────────────────────────────
  quality:
    name: Quality gates (ruff/mypy/SSOT/audit)
    runs-on: ubuntu-latest
    needs: architecture

    steps:
      - uses: actions/checkout@v4

      - name: Install uv
        uses: astral-sh/setup-uv@v4
        with:
          version: "latest"

      - name: Install dev dependencies
        run: uv sync --group dev

      - name: Ruff lint
        run: uv run ruff check .

      - name: Ruff format check
        run: uv run ruff format . --check

      - name: Mypy (shared)
        run: uv run mypy shared/

      - name: SSOT literales
        run: uv run python scripts/check_ssot_enums.py

      - name: Vulnerabilidades (pip-audit)
        run: uv run pip-audit --requirement pyproject.toml
```

**pydeps NO entra como gate** (ver §3, hallazgo 7). Visualización opcional no bloqueante:

```yaml
      - name: Grafo shared/ (artifact)
        run: uv run pydeps shared --max-bacon 4 -o shared.svg --noshow
      - name: Upload grafo
        uses: actions/upload-artifact@v4
        with:
          name: shared-graph
          path: shared.svg
```

> `--noshow` / `--no-output` son los flags que evitan el visor headless. **Nunca** invocar pydeps sin uno de los dos (falla con CalledProcessError en runners sin visor).

### Fase 6 — Dependabot (`.github/dependabot.yml`, nuevo)

```yaml
version: 2
updates:
  - package-ecosystem: "pip"
    directory: "/"
    schedule:
      interval: "weekly"
    groups:
      dev-dependencies:
        patterns: ["pytest", "ruff", "mypy", "pydeps", "pip-audit"]
```

### Fase 7 — Pre-commit (`.pre-commit-config.yaml`)

Añadir hooks locales (mantener ruff-pre-commit v0.15.10 y el readme-size-guard actuales):

```yaml
  - repo: local
    hooks:
      - id: import-linter
        name: Import-linter (arquitectura)
        entry: uv run lint-imports --config architecture/importlinter.toml
        language: system
        pass_filenames: false
      - id: mypy-shared
        name: Mypy (shared)
        entry: uv run mypy shared/
        language: system
        pass_filenames: false
      - id: ssot-enums
        name: SSOT literales
        entry: uv run python scripts/check_ssot_enums.py
        language: system
        pass_filenames: false
```

### Fase 8 — Documentación

- **ADR-0010** (`docs/architecture/decisions/ADR-0010-gobernanza-automatizada-shared-kernel.md`, siguiendo `ADR-template.md`): decisión de pasar de arquitectura documentada a gobernada por CI — contratos, SSOT enforcement, seguridad de cadena de suministro, métricas.
- **`GOVERNANCE.md`** (existe): nueva sección "Gobernanza automatizada" (CI gates, scripts, métricas).
- **README.md**: mención breve de los gates de calidad.

### Fase 9 — Validación final y commits

**Comandos de verificación completos:**

```
uv run ruff check .
uv run ruff format . --check
uv run lint-imports --config architecture/importlinter.toml   # 43 KEPT
uv run mypy .                                                 # incluye scripts/ y shared/
uv run pytest tests/ -x -q                                   # 748+
uv run python scripts/check_ssot_enums.py                     # OK
uv run python scripts/metrics_report.py                       # genera architecture/metrics.json
uv run bandit -r apps ocm packages shared infrastructure      # intacto
```

**Commits atómicos propuestos (Conventional Commits):**

| # | Commit | Fases |
|---|---|---|
| 1 | `refactor(shared): unificar literales OrderSide/PositionSide en shared.enums` | 1 |
| 2 | `chore(arch): blindar kernel con BC-46/47/48 (vocabulario, transporte, utils)` | 2 |
| 3 | `chore(governance): pip-audit + scripts SSOT/métricas + CI quality + dependabot + pre-commit` | 3–7 |
| 4 | `docs(architecture): ADR-0010 gobernanza automatizada + GOVERNANCE` | 8 |

El drift pre-existente (`packages/market_data/infrastructure/runtime/`) queda intacto y fuera de los commits.

## 5. Criterios de Aceptación

1. `shared/enums.py` es el **único** punto de definición del vocabulario — `check_ssot_enums.py` pasa en local y CI.
2. **43 KEPT / 0 broken** — BC-46/47/48 enforced en CI desde el primer run.
3. Job `quality` en `ocm-ci.yml` ejecuta ruff, format, mypy, SSOT y pip-audit.
4. Pre-commit detecta violaciones de arquitectura y SSOT antes del commit.
5. `pip-audit` en dev deps, `uv.lock` actualizado con el cambio real de dependencia.
6. Dependabot activo (ecosistema `pip`).
7. ADR-0010 + `GOVERNANCE.md` documentan la gobernanza automatizada.
8. 748+ tests · mypy limpio · ruff limpio · bandit intacto.

## 6. Riesgos y Mitigación

| Riesgo | Probabilidad | Mitigación |
|---|---|---|
| Unificación rompe consumidores de `OrderSide`/`PositionSide` | Baja | Consumidores verificados (trading vía `shared.types` re-export); gate lo confirma |
| Scripts rompen `mypy .` / ruff | Baja | Scripts tipados y ≤120 cols; validación en Fase 9 |
| `pip-audit` detecta vulnerabilidades y bloquea CI | Media | Es el objetivo (SafeOps); se corrige la dep o se documenta el risk-accept |
| pydeps reintroducido como gate roto | Baja | Documentado en §3; solo artifact opcional con `--noshow` |
| `uv.lock` rechazado en pre-commit | Baja | Cambio de dep real (pip-audit) — el único `uv.lock` permitido de AGENTS.md |
| BC-46/48 redundantes con BC-01 | Nula | Redundancia intencional eliminada (solo internos de `shared`); BC-01 cubre externos |

## 7. Decisiones registradas (trazabilidad)

- 2026-08-03: **Unificar** `OrderSide`/`PositionSide` en `shared.enums` (no ignore-list en el script SSOT).
- 2026-08-03: **Modificar** `ocm-ci.yml` (job `quality`); no crear `ci.yml` duplicado.
- 2026-08-03: `pip-audit` **añadida a `[dependency-groups] dev`** (no `uvx`).
- 2026-08-03: **pydeps fuera del CI como gate** (visor headless rompe; exit-code de ciclos no fiable). Opcional: artifact `shared.svg` con `--noshow`.
- 2026-08-03: Métricas **sin JSON de herramientas** (lint-imports y mypy no lo soportan en 2.6/1.19.1) → parseo de texto verificado.
- 2026-08-03: BC-46/48 usan solo dependencias internas de `shared` (externos ya en BC-01); se omite `"apps"` (no es root_package).
- 2026-08-03 (ejecución): **pip-audit NO soporta `--json` ni `--requirement pyproject.toml`** (trata el pyproject como requirements pip → error). El CI usa `pip-audit .` (proyecto PEP 621); las métricas usan `pip-audit -l -f json` (entorno completo, informativo).
- 2026-08-03 (ejecución): **Risk-accept de 2 vulnerabilidades** en CI: `pyarrow` 19.0.1 (`PYSEC-2026-113`) y `ecdsa` transitiva de `python-jose` (`PYSEC-2026-1325`) → `--ignore-vuln` documentado en el workflow.
- 2026-08-03 (ejecución): el JSON de pip-audit anida vulns en `dependencies[].vulns` (no hay clave top-level) → el script las suma por dependencia.
- 2026-08-03 (ejecución): `mypy .` global tiene 21 errores **pre-existentes fuera de alcance** (apps/api, apps/research, infrastructure/redis, market_data drift, trading/paper_bot) — ninguno en `shared/` ni `scripts/`, que son los que este plan gobierna.

## 8. Fuera de Alcance (no se ejecuta en este plan)

1. `uv sync --frozen` en los 4 jobs de CI existentes (SafeOps adicional; requiere alinear `uv.lock` y los workflows — decisión separada).
2. Dashboard/Grafana de métricas (el script genera `architecture/metrics.json`; la visualización se decide después).
3. Añadir bandit al job `quality` (ya existe como comando manual; se puede integrar en una iteración futura).
4. Unificar otros literales (SignalDirection/DataSource ya son SSOT en `enums`; no hay más duplicaciones verificadas).
5. El drift pre-existente de `packages/market_data/infrastructure/runtime/`.
