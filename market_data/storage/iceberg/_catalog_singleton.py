# -*- coding: utf-8 -*-
"""
market_data/storage/iceberg/_catalog_singleton.py
===================================================

Singleton del catálogo Iceberg — única responsabilidad: crear y devolver
la instancia SqlCatalog compartida por proceso.

Privado al paquete — importar via catalog.py, no directamente.

Ref: https://py.iceberg.apache.org/configuration/#sql-catalog
"""
from __future__ import annotations

from typing import Optional

from pyiceberg.catalog.sql import SqlCatalog

from core.utils import repo_root

_CATALOG: Optional[SqlCatalog] = None


def get_catalog() -> SqlCatalog:
    """Devuelve el catálogo Iceberg compartido, creándolo una sola vez.

    Paths resueltos desde repo_root() — independiente del cwd.
    SqlCatalog SQLite: adecuado para single-node. Para multi-nodo
    usar REST catalog o Hive Metastore.
    """
    global _CATALOG
    if _CATALOG is None:
        base      = repo_root() / "data_platform"
        cat_path  = base / "iceberg_catalog"
        ware_path = base / "iceberg_warehouse"
        cat_path.mkdir(parents=True, exist_ok=True)
        ware_path.mkdir(parents=True, exist_ok=True)
        _CATALOG = SqlCatalog(
            "ocm",
            **{
                "uri":       f"sqlite:///{cat_path}/catalog.db",
                "warehouse": str(ware_path.absolute()),
            },
        )
    return _CATALOG
