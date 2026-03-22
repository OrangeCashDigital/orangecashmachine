"""
core/logging/filters.py
=======================
Filtros reutilizables para sinks de Loguru.
"""

from typing import Any


def pipeline_filter(record: dict[str, Any]) -> bool:
    """
    Acepta solo logs de módulos propios del pipeline que tengan
    contexto bind completo (exchange + dataset).
    """
    return (
        record["name"].startswith((
            "market_data.",
            "services.exchange.",
            "services.state.",
        ))
        and bool(record["extra"].get("exchange"))
    )
