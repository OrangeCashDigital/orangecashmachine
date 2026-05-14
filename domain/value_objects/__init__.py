# -*- coding: utf-8 -*-
# DEPRECATED — usar shared.types
import warnings as _w
_w.warn(
    "domain.value_objects está deprecado. Usar shared.types.",
    DeprecationWarning, stacklevel=2,
)
from shared.types.signal import Signal, SignalType  # noqa: F401
from shared.types.timeframe import Timeframe         # noqa: F401
