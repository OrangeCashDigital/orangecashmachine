# -*- coding: utf-8 -*-
# DEPRECATED — usar shared.types
import warnings as _w
_w.warn(
    "domain.entities está deprecado. Usar shared.types.",
    DeprecationWarning, stacklevel=2,
)
from shared.types.ohlcv import OHLCVBar  # noqa: F401
