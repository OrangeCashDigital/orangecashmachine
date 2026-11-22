# Known false positives — Code scanning

## CodeQL alert #65 — py/weak-sensitive-data-hashing

- **File**: `packages/trading/adapters/outbound/bybit/adapter.py::_sign`
- **Status**: dismissed as false positive (2026-11-21)
- **Rule**: `py/weak-sensitive-data-hashing`
- **Reason**: HMAC-SHA256 es el algoritmo requerido por Bybit V5 para
  firmar REST requests (message authentication code). `api_secret` actúa
  como HMAC key, no como password a hashear con KDF. La regla apunta a
  password hashing (bcrypt/argon2/scrypt), no a MACs.
- **Re-validation**: si se migra a otro exchange, revisar el algoritmo
  de firma requerido por ese exchange.
- **Refs**: ADR-0016, ADR-0030, ADR-0048
