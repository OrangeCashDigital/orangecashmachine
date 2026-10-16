# ADR-0051: Sustitución de `python-jose` por `PyJWT` en apps/api

**Estado:** ACEPTADO por decisión humana D-OCM-07b (2026-10-07) — implementación pendiente de aplicación autorizada
**Decisión humana asociada:** D-OCM-07b = OPCIÓN A — sustitución `python-jose` → `PyJWT`
**Fecha:** 2026-10-07
**Bounded context(s) afectado(s):** apps/api (gateway experimental)

## Contexto

`python-jose` (pin `python-jose[cryptography]>=3.3.0,<4.0`, `pyproject.toml:118`) presenta
**CVE-2026-85394** — hallazgo F-OCM-06 (auditoría 2026-10-07) — **sin fix disponible**:

- Advisory GitHub `GHSA-3qf3-8w2g-rqmx` (Crítica, Unreviewed); CVSS 3.1 = **9.1**, CVSS 4.0 = **9.3** (CNA VulnCheck; mismos datos en NVD, Tenable, SUSE, Armis).
- Afecta python-jose `0 <= x <= 3.5.0`; **no existe versión parcheada** (último release 3.5.0, 2025-05-28).
- Vulnerabilidad "algorithm confusion": claves públicas DER sin armadura PEM/SSH aceptadas como secreto HMAC; forja de tokens HS256 si `algorithms` no está restringido. Es un *fix incompleto* de CVE-2024-33663.
- Detalle reportado en `mpdavis/python-jose#414`; PR (private vulnerability reporting) deshabilitado, sin SECURITY.md (también `#398`, `#419` abiertos sin respuesta desde ~junio 2026).
- SUSE clasifica la secuencia como "Won't fix" para las distribuciones listadas.

Matiz de explotabilidad en OCM: el único sitio de `decode` (`apps/api/auth/jwt.py:71`) ya pasa
`algorithms=[algorithm]` de forma explícita y el algoritmo es HS256 simétrico (sin claves públicas
en el path) → el vector principal del advisory **no es alcanzable** tal como está configurado. Aun
así, mantener una dependencia sin fix crítico ni actividad upstream, y con la transitiva `ecdsa`
ya bajo risk-accept previo (`PYSEC-2026-1325`, `ocm-ci.yml:353-365`), no es defendible si existe
sustitución directa y de bajo costo.

### Uso real verificado (único consumidor en el repo)

- `apps/api/auth/jwt.py:19` — `from jose import JWTError, jwt`.
- `apps/api/auth/jwt.py:23-47` — `create_token()`: `jwt.encode(data, secret, algorithm=algorithm)` (HS256 simétrico).
- `apps/api/auth/jwt.py:50-74` — `verify_token()`: `jwt.decode(token, secret, algorithms=[algorithm])`, `except JWTError` → `None` (fail-soft).
- Consumidores: `apps/api/deps.py:19,79` (`verify_token`, contrato `dict[str, Any] | None`).
- No se usan `jwe` / `jwk` / `jws` ni criptografía asimétrica de python-jose.
- Gateway `uv run ocm-api` es **experimental** (CD placeholder; sin exposición pública en producción).

## Alternativas evaluadas

1. **Sustituir por `PyJWT[crypto]`** (esta propuesta) — API compatible para encode/decode HS256; mantenimiento activo; te obliga a pasar `algorithms` explícito (no regresión). Coste: refactor de un módulo de 74 líneas + tests.
2. **Sustituir por `joserfc`** — sucesor moderno del ecosistema authlib (JWS+JWE, type hints); usado por MAAS (Ubuntu) al migrar de python-jose (validación humana D-OCM-07b). Descarte razonado: cambio de API mayor sin necesidad para el caso HS256; no cubierto por este ADR (quedó como opción C en el análisis).
3. **Risk-accept formal (ADR-0052)** — gobernaba la CVE sin eliminarla. **RECHAZADA por decisión humana D-OCM-07b**: para una CVE crítica sin fix ni respuesta upstream y con sustituto barato, el risk-accept no es defendible. ADR-0052 queda **OBSOLETO**.
4. **Esperar fix upstream** — sin actividad del mantenedor desde el issue #414; rechazada por la política de riesgo registrado (ADR-0010).

## Análisis de compatibilidad de API (python-jose → PyJWT)

| Capacidad usada | python-jose | PyJWT | Compatible |
|---|---|---|---|
| Firma HS256 | `jwt.encode(data, secret, algorithm="HS256")` | `jwt.encode(data, secret, algorithm="HS256")` | **Sí** (idéntica firma) |
| Verificación HS256 | `jwt.decode(token, secret, algorithms=["HS256"])` | `jwt.decode(token, secret, algorithms=["HS256"])` | **Sí** |
| Excepción | `JWTError` | `PyJWTError` (raíz de `InvalidTokenError`, `ExpiredSignatureError`, `DecodeError`…) | Renombrar import (`jwt.py:19`) |
| Payload `iat`/`exp` | acepta `datetime` tz-aware | acepta `datetime` tz-aware y serializa a epoch UTC | **Sí** |
| Claims arbitrarios (`sub`, roles…) | dict | dict | **Sí** |

Notas finas:

- PyJWT exige `algorithms` en `decode` (desde 2.x, tras CVE-2022-29217); el código ya lo pasa (`jwt.py:54,71`) — **mejora la seguridad** respecto a python-jose (`#419`: `algorithms=None` sigue permitido).
- No cambia el formato del token HS256: tokens emitidos por python-jose se verifican con PyJWT y viceversa (mismo secreto/algoritmo) — sin revocar sesiones.
- El único `except JWTError` (`jwt.py:72`) se preserva renombrando a `PyJWTError` (convierte a `None`, fail-soft, SAME contrato).

## Referencias de decisión — validación humana + industria

- FastAPI docs oficiales usan PyJWT en los ejemplos JWT: https://fastapi.tiangolo.com/tutorial/security/oauth2-jwt/ (tras `fastapi/fastapi#9587` y PR `#11589`).
- Litestar retiró python-jose por "unresolved security issues" y eligió PyJWT: `litestar-org/litestar#3549`, PR `#3684`.
- Guía oficial de migración python-jose → PyJWT: `jpadilla/pyjwt#942`.
- SEMICeu/sdep migró python-jose → PyJWT ("unmaintained since 2022") y MAAS (Ubuntu) → joserfc ("deprecated") — validación humana D-OCM-07b.
- Motores de referencia en `docs/`: Hummingbot y Freqtrade ya usan PyJWT; Nautilus no usa ninguna lib JWT (análisis B, decisión D-OCM-07b).

## Plan de migración

1. **pyproject.toml:118** — `"python-jose[cryptography]>=3.3.0,<4.0"` → `"pyjwt[crypto]>=2.8.0,<3.0"`. Elimina de paso la transitiva `ecdsa` (cierra `PYSEC-2026-1325`).
2. **uv.lock** — regenerar (`uv lock / uv sync`) SOLO en el paso de aplicación autorizado.
3. **apps/api/auth/jwt.py** — adaptar import + call sites (`import jwt as _jwt`, `PyJWTError` alias).
4. **apps/api/deps.py** — **sin cambios**: importa solo `verify_token` (`deps.py:19`); contrato intacto (`deps.py:79`).
5. **tests/api/test_jwt.py** — tests nuevos de round-trip HS256 (deuda de cobertura 0% documentada).
6. **Ignore `PYSEC-2026-1325`** — la CVE desaparece del grafo al salir `ecdsa`; el ignore canónico (AUDIT_PROTOCOL §R `DEPENDENCY_AUDIT`) queda inerte y puede retirarse en una revisión posterior (no se retira en esta implementación).
7. **Verificación:** `uv run ruff check . && uv run mypy . && uv run pytest tests/api/ -q` + `uv run pip-audit . --ignore-vuln PYSEC-2026-113 --ignore-vuln PYSEC-2026-1325`.

## Riesgos

- **Compatibilidad API:** mitigada por la tabla anterior; round-trip HS256 estándar.
- **Tokens existentes:** no se revocan (mismo secret/algoritmo); transición transparente.
- **Naming collision** `api/auth/jwt.py` vs paquete `jwt`: mitigado con alias local `import jwt as _jwt`.
- **Cobertura de tests del módulo:** hoy 0%; se añade `tests/api/test_jwt.py` en el mismo cambio.
- **Gateway experimental:** cambio reversible y de bajo riesgo (CD placeholder, sin superficie pública).

## Rollback

1. `git revert` del commit de migración restaura python-jose + `uv.lock` previo.
2. Compatibilidad binaria: tokens HS256 firmados durante la ventana PyJWT se verifican con python-jose (mismo secreto/algoritmo).
3. Sin migración de datos ni cambio de esquema — rollback inmediato.

## Consecuencias

- Se elimina del grafo **CVE-2026-85394** (python-jose) y **PYSEC-2026-1325** (ecdsa transitiva).
- Refactor acotado: 1 módulo de 74 líneas + 2 imports + tests nuevos. Sin cambios en ports/dominio (BC-09 intacto; el cambio vive en `apps/api/`).
- ADR-0052 (risk-accept python-jose) queda **OBSOLETO** por esta decisión (D-OCM-07b).

## ADRs relacionados

- **ADR-0052** — risk-accept CVE-2026-85394 (alternativa B) → **OBSOLETO por decisión D-OCM-07b**.
- **ADR-0053** — patrón de sustitución por fix real (override-dependencies, multidict) que sirve de espejo a esta decisión: cuando existe solución de bajo costo, se sustituye en lugar de aceptar el riesgo.
- **ADR-0050** — risk-accept multidict → **OBSOLETO** (rechazado 2026-10-07); mismo razonamiento que justifica retirar el risk-accept de python-jose.
- **ADR-0010:63-66** — precedente de risk-accept inicial (pyarrow/ecdsa, 2026-08-03).

## Referencias

- Hallazgo: `docs/audits/AUDIT_OCM_high-impact-improvements_2026-10-07.md` (F-OCM-06).
- CVE-2026-85394: GHSA-3qf3-8w2g-rqmx · https://nvd.nist.gov/vuln/detail/CVE-2026-85394 · https://www.tenable.com/cve/CVE-2026-85394 · https://github.com/mpdavis/python-jose/issues/414.
- Código: `pyproject.toml:118`; `apps/api/auth/jwt.py:19,41-47,50-74`; `apps/api/deps.py:19,79`.
- Migración: https://github.com/jpadilla/pyjwt/issues/942.