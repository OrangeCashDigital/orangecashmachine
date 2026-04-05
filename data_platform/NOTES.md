
## TODO: Expirar snapshots de migración
- pyiceberg 0.8.1 no expone expire_snapshots en la API pública
- Upgrading a pyiceberg >= 0.9 habilita: table.expire_snapshots().expire_older_than(now_ms).commit()
- Snapshots actuales: 989 (solo metadata en catalog.db, no afecta performance)
- Snapshot de producción actual: 6436016868059628227
