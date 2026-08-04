# ==============================================================================
# api/middleware/__init__.py — constantes compartidas del paquete middleware
# ==============================================================================
#
# Paths de observabilidad excluidos de logging y rate limit (H6,
# AUDIT-apps-2026-08-03). SSOT compartido: RequestLoggingMiddleware y
# RateLimitMiddleware los leen de aquí — las probes de infraestructura no
# representan carga de cliente y no deben poder degradar la rotación del nodo.
# ==============================================================================

SILENT_PATHS: frozenset[str] = frozenset({"/health", "/ready", "/metrics"})
