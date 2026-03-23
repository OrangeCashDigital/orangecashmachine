from __future__ import annotations

"""core/config/loader/metrics.py — Métricas Prometheus para el loader."""

try:
    from prometheus_client import Counter, Histogram
    _PROMETHEUS_AVAILABLE = True
    CONFIG_LOAD_COUNT    = Counter("config_loads_total",            "Config load attempts", ["env", "status"])
    CONFIG_LOAD_DURATION = Histogram("config_load_duration_seconds", "Config load duration", ["env"])
    CONFIG_CACHE_HITS    = Counter("config_cache_hits_total",       "Cache hits",            ["env"])
except ImportError:
    _PROMETHEUS_AVAILABLE = False

    class _NoOpMetric:
        def labels(self, **_): return self
        def inc(self, _=1):    pass
        def observe(self, _):  pass

    CONFIG_LOAD_COUNT    = _NoOpMetric()  # type: ignore[assignment]
    CONFIG_LOAD_DURATION = _NoOpMetric()  # type: ignore[assignment]
    CONFIG_CACHE_HITS    = _NoOpMetric()  # type: ignore[assignment]

CONFIG_RELOAD_TIME = CONFIG_LOAD_DURATION
