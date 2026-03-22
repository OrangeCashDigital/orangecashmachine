from __future__ import annotations

"""core/config/loader/audit.py — Registro inmutable de recargas."""

from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.config.schema import AppConfig


def record(config: "AppConfig", cache_key: str, h: str, source: str) -> "AppConfig":
    from core.config.schema import AuditEntry
    entry = AuditEntry(
        timestamp=datetime.now(timezone.utc),
        cache_key=cache_key,
        hash=h,
        source_file=source,
    )
    return config.model_copy(update={
        "audit_log":   [*config.audit_log, entry],
        "last_reload": datetime.now(timezone.utc),
    })
