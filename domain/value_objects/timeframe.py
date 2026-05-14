# -*- coding: utf-8 -*-
# DEPRECATED — usar shared.types.timeframe
import warnings as _w
_w.warn(
    "domain.value_objects.timeframe está deprecado. Usar shared.types.timeframe.",
    DeprecationWarning, stacklevel=2,
)
from shared.types.timeframe import *  # noqa: F401, F403
from shared.types.timeframe import Timeframe  # noqa: F401
