# -*- coding: utf-8 -*-
"""
ocm/runtime/state/__init__.py
==============================

API pública del subpaquete ``ocm.runtime.state``.

Superficie de acceso canónica para stores de estado persistente del runtime.
Los consumidores importan desde aquí — nunca desde los módulos internos.

Consumo canónico
----------------
Composition root (main.py / entrypoints)::

    from ocm.runtime.state import (
        build_cursor_store,
        build_gap_registry,
        build_lateness_calibration_store,
    )

Uso en pipelines (inyección de dependencias)::

    from ocm.runtime.state import (
        RedisCursorStore,
        InMemoryCursorStore,
        GapRegistry,
        LatenessCalibrationStore,
    )

Contratos de port (para typing / DI sin instanciar)::

    from ocm.runtime.state import CursorStorePort, GapStorePort

Testing (in-memory stubs sin Redis real)::

    from ocm.runtime.state import InMemoryCursorStore, InMemoryGapStore

Reglas
------
  · Solo ``main.py`` y entrypoints instancian via ``build_*``.
  · Los pipelines reciben el store ya construido por DI — no lo construyen.
  · Los tests usan las implementaciones InMemory* directamente.
  · Ningún módulo externo importa de submódulos internos de este paquete.

Contratos BC: BC-22, BC-23, BC-27 (ver pyproject.toml).

Principios: Clean Architecture (facade) · DIP · SRP · Fail-Fast en import time.
"""

from __future__ import annotations

# ── Protocols / Ports ─────────────────────────────────────────────────────────
from ocm.runtime.state.cursor_store import (
    CursorStore,
    InMemoryCursorStore,
)

# ── Implementaciones concretas ────────────────────────────────────────────────
from ocm.runtime.state.cursor_store import (
    CursorStore as RedisCursorStore,
)

# ── Encoding — uso excepcional documentado ────────────────────────────────────
from ocm.runtime.state.encoding import encode_redis_key

# ── Factories — composition root únicamente ───────────────────────────────────
from ocm.runtime.state.factories import (
    build_cursor_store,
    build_gap_registry,
    build_lateness_calibration_store,
)
from ocm.runtime.state.gap_registry import GapRegistry
from ocm.runtime.state.gap_store import (
    GapStorePort,
    InMemoryGapStore,
    RedisGapStore,
)
from ocm.runtime.state.lateness_calibration import LatenessCalibrationStore

__all__ = [
    "CursorStore",
    "GapStorePort",
    "RedisCursorStore",
    "InMemoryCursorStore",
    "RedisGapStore",
    "InMemoryGapStore",
    "GapRegistry",
    "LatenessCalibrationStore",
    "build_cursor_store",
    "build_gap_registry",
    "build_lateness_calibration_store",
    "encode_redis_key",
]
