# -*- coding: utf-8 -*-
# DEPRECATED — usar shared.types.signal
import warnings as _w
_w.warn(
    "domain.value_objects.signal está deprecado. Usar shared.types.signal.",
    DeprecationWarning, stacklevel=2,
)
from shared.types.signal import *  # noqa: F401, F403
from shared.types.signal import Signal, SignalType  # noqa: F401
