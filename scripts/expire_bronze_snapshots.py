"""Expira snapshots de bronze.ohlcv con más de 1 día. Reintenta si pierde la carrera de commit."""

import time
from datetime import datetime, timedelta

from market_data.infrastructure.storage.iceberg.catalog import get_catalog
from pyiceberg.exceptions import CommitFailedException

for attempt in range(5):
    try:
        t = get_catalog().load_table("bronze.ohlcv")
        t.maintenance.expire_snapshots().older_than(datetime.now() - timedelta(days=1)).commit()
        n = len(get_catalog().load_table("bronze.ohlcv").metadata.snapshots)
        print(f"ok snapshots={n}")
        break
    except CommitFailedException:
        time.sleep(5 * (attempt + 1))
