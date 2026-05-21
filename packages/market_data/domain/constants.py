"""
market_data/domain/constants.py
================================

Constantes de dominio puras — sin dependencias externas.

SSOT: todos los módulos (application, adapters, infrastructure)
importan desde aquí. Ninguno redefine estas constantes localmente.

Principios: SSOT · KISS · sin side-effects al importar.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Paginación histórica
# ---------------------------------------------------------------------------

#: Tamaño de chunk por defecto para paginación hacia atrás en backfill.
#: Valor conservador válido para Bybit, KuCoin y KuCoinFutures.
#: Override por exchange vía exchange_quirks en la capa adapter.
DEFAULT_CHUNK_LIMIT: int = 500

#: Safety cap: máximo de chunks por sesión de backfill.
#: Protege contra loops infinitos ante datos corruptos en exchanges.
MAX_BACKFILL_CHUNKS: int = 100_000
