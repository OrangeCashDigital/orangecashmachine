# -*- coding: utf-8 -*-
# DEPRECATED — usar shared.types.ohlcv
import warnings as _w
_w.warn(
    "domain.entities.ohlcv está deprecado. Usar shared.types.ohlcv.",
    DeprecationWarning, stacklevel=2,
)
from shared.types.ohlcv import *  # noqa: F401, F403
from shared.types.ohlcv import OHLCVBar  # noqa: F401
