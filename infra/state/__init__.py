"""Shim de compatibilidad: re-exporta desde ocm_platform.infra.state.

TEMPORAL: eliminar cuando todos los callers apunten a ocm_platform.infra.state.*

Callers actuales:
  market_data/: build_cursor_store, build_gap_registry,
                build_lateness_calibration_store, build_stream_publisher,
                build_stream_source, cursor_store symbols
"""
from ocm_platform.infra.state.cursor_store          import *  # noqa: F401, F403
from ocm_platform.infra.state.factories             import *  # noqa: F401, F403
from ocm_platform.infra.state.gap_registry          import *  # noqa: F401, F403
from ocm_platform.infra.state.lateness_calibration  import *  # noqa: F401, F403
from ocm_platform.infra.state.redis_stream          import *  # noqa: F401, F403
