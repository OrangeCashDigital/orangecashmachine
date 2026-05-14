# -*- coding: utf-8 -*-
# DEPRECATED — usar shared.types
import warnings as _w
_w.warn(
    "domain.events está deprecado. Usar shared.types.",
    DeprecationWarning, stacklevel=2,
)
from shared.types.order_events import OrderFilled, OrderRejected, OrderCancelled  # noqa: F401
from shared.types.position_events import PositionOpened, PositionClosed          # noqa: F401
from shared.types.rebalance_events import RebalanceTriggered, RebalanceCompleted  # noqa: F401
