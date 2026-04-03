from __future__ import annotations

"""core/config/loader/audit.py — Registro inmutable de recargas."""

from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.config.schema import AppConfig


def record(config: "AppConfig", cache_key: str, h: str, source: str) -> "AppConfig":
    from core.config.schema import AuditEntry
    # Capturar el instante una sola vez — AuditEntry.timestamp y
    # AppConfig.last_reload deben ser el mismo momento exacto.
    now = datetime.now(timezone.utc)
    entry = AuditEntry(
        timestamp=now,
        cache_key=cache_key,
        hash=h,
        source_file=source,
    )
    return config.model_copy(update={
        "audit_log":   [*config.audit_log, entry],
        "last_reload": now,
    })
