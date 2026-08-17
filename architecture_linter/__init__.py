"""architecture_linter — Architecture Governance Linter para OCM.

Herramienta independiente (stdlib-only) que detecta violaciones
arquitectónicas reales por análisis AST: owners de estado, ports huérfanos,
stubs, contratos duplicados, capas, freshness y balance.
"""

__version__ = "0.1.0"

from architecture_linter.engine import LinterEngine, RepoContext
from architecture_linter.models import Severity, Status

__all__ = ["LinterEngine", "RepoContext", "Severity", "Status", "__version__"]
