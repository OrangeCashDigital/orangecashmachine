# -*- coding: utf-8 -*-
"""
dagster_assets/ — DEPRECATED. Usa infrastructure/dagster/assets/.

Re-exporta para compatibilidad con imports existentes en tests.
"""
import warnings
warnings.warn(
    "dagster_assets/ está deprecated. Usa infrastructure.dagster.assets",
    DeprecationWarning,
    stacklevel=2,
)
from infrastructure.dagster.assets import (  # noqa: F401
    BRONZE_OHLCV_ASSETS,
    REPAIR_OHLCV_ASSETS,
    RESAMPLE_OHLCV_ASSETS,
    ALL_ASSET_CHECKS,
)
