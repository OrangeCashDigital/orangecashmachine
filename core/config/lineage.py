"""Shim: alias de ocm_platform.config.lineage via sys.modules."""
import sys as _sys
import importlib as _importlib

_real = _importlib.import_module("ocm_platform.config.lineage")
_sys.modules[__name__] = _real
