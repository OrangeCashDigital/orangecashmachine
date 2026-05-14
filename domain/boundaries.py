# -*- coding: utf-8 -*-
# DEPRECATED — usar shared.contracts.boundaries
import warnings as _w
_w.warn(
    "domain.boundaries está deprecado. Usar shared.contracts.boundaries.",
    DeprecationWarning, stacklevel=2,
)
from shared.contracts.boundaries import *  # noqa: F401, F403
from shared.contracts.boundaries import (  # noqa: F401
    FeatureSource, SignalProtocol, FillHandler, TradeHistory, RiskGate,
)
