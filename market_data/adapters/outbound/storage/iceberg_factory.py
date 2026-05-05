# -*- coding: utf-8 -*-
"""
market_data/adapters/outbound/storage/iceberg_factory.py
=========================================================

IcebergStorageFactory — implementación concreta de StorageFactoryPort.

Responsabilidad única
---------------------
Crear y cachear instancias de IcebergStorage por (exchange, market_type).
Una instancia por par — IcebergStorage es stateful respecto al catalog
pero thread-safe gracias al singleton de SqlCatalog.

Cache policy
------------
Instancias se crean una vez (lazy) y se reutilizan.
IcebergStorage es thread-safe → no requiere lock adicional.
La duración del cache es la del proceso (lifespan del servicio).

Registro en composition root (main.py / lifespan)
--------------------------------------------------
    from market_data.adapters.outbound.storage.iceberg_factory import IcebergStorageFactory
    _state.storage_factory = IcebergStorageFactory()

Principios: DIP · SRP · SSOT · SafeOps · Lazy init
"""
from __future__ import annotations

from market_data.ports.storage import OHLCVStorage


class IcebergStorageFactory:
    """
    Fábrica con cache de instancias IcebergStorage.

    Satisface StorageFactoryPort estructuralmente (Protocol).
    No hereda explícitamente para evitar acoplamiento — duck typing.

    Thread-safety
    -------------
    Python GIL protege el dict en CPython para operaciones simples.
    Para producción multi-threaded real usar threading.Lock si se migra
    fuera de asyncio.to_thread.
    """

    def __init__(self) -> None:
        # Clave de cache: (exchange, market_type, dry_run)
        # dry_run=True y dry_run=False son instancias semánticamente distintas
        # (escrituras habilitadas vs deshabilitadas) — no deben compartir cache.
        self._cache: dict[tuple[str, str, bool], OHLCVStorage] = {}

    def get_storage(
        self,
        exchange:    str,
        market_type: str  = "spot",
        dry_run:     bool = False,
    ) -> OHLCVStorage:
        """
        Retorna IcebergStorage para (exchange, market_type, dry_run), creándola
        si no existe en cache.

        Fail-Fast: lanza RuntimeError si IcebergStorage no puede inicializarse.
        El caller recibe 500 limpio en lugar de AttributeError críptico.

        Cache key
        ---------
        Incluye dry_run porque IcebergStorage con dry_run=True no persiste datos
        — son instancias semánticamente distintas que no deben compartir estado.
        """
        key = (exchange, market_type, dry_run)
        if key not in self._cache:
            # Import lazy — adaptador concreto no se carga en import time (DIP)
            from market_data.storage.iceberg.iceberg_storage import IcebergStorage
            try:
                self._cache[key] = IcebergStorage(
                    exchange    = exchange,
                    market_type = market_type,
                    dry_run     = dry_run,
                )
            except Exception as exc:
                raise RuntimeError(
                    f"IcebergStorageFactory: no se pudo crear storage para "
                    f"exchange={exchange!r} market_type={market_type!r} "
                    f"dry_run={dry_run}: {exc}"
                ) from exc
        return self._cache[key]

    def __repr__(self) -> str:  # pragma: no cover
        keys = [f"{e}/{m}" for e, m in self._cache]
        return f"IcebergStorageFactory(cached={keys})"
