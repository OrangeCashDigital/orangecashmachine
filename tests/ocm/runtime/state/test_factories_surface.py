# -*- coding: utf-8 -*-
"""
tests/ocm/runtime/state/test_factories_surface.py
===================================================

No-regresión de la superficie pública de ocm.runtime.state.factories tras
la eliminación del dead code de RedisStreams (B-40).

En el rename ocm_platform→ocm (66cd1c4) quedaron en el árbol dos copias
rotas de los stream helpers:
  · build_stream_publisher / build_stream_source en state/factories.py que
    importaban 'ocm.runtime.state.redis_stream' (módulo inexistente).
Estado verificado: cero callers, cero imports, cero tests — dead code.

Estos tests protegen las propiedades reales que NO deben regresionar:
  1. El módulo factories importa limpio y expone los builders canónicos.
  2. No expone los símbolos de stream (eliminados) — si alguien reintroduce
     un import a un módulo inexistente, el smoke de import lo detecta.
"""

import importlib


def test_factories_imports_clean_and_canonical_exports() -> None:
    factories = importlib.import_module("ocm.runtime.state.factories")
    for name in (
        "build_cursor_store",
        "build_gap_registry",
        "build_lateness_calibration_store",
    ):
        assert callable(getattr(factories, name)), f"faltó export: {name}"


def test_stream_builders_removed() -> None:
    """B-40: los builders de stream (dead code) ya no existen."""
    factories = importlib.import_module("ocm.runtime.state.factories")
    assert not hasattr(factories, "build_stream_publisher")
    assert not hasattr(factories, "build_stream_source")


def test_facade_public_api_unchanged() -> None:
    """La fachada ocm.runtime.state.__init__ no expone stream builders."""
    export = importlib.import_module("ocm.runtime.state")
    assert not hasattr(export, "build_stream_publisher")
    assert not hasattr(export, "build_stream_source")
    for name in (
        "build_cursor_store",
        "build_gap_registry",
        "build_lateness_calibration_store",
    ):
        assert callable(getattr(export, name)), f"faltó export: {name}"
