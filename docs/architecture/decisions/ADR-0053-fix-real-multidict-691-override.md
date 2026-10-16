# ADR-0053: CVE-2026-104874 · Fix real vía override-dependencies (multidict 6.9.1), alineado con ccxt #30764

**Estado:** Propuesto
**Fecha:** 2026-10-07
**Bounded context(s) afectado(s):** ocm (CI/seguridad de dependencias) | todo el runtime (dependencia transitiva)

## Contexto

La auditoría 2026-10-07 (F-OCM-06, HIGH/NUEVO) detectó `multidict 6.7.1` con
CVE-2026-104874 (GHSA-54p9-h82j-f925, CWE-401, DoS por fuga de referencia en
`CIMultiDict/MultiDict` items-view union/subtraction; `>=6.7.0, <=6.9.0`). Fix
disponible en `6.9.1` (commit 350b4a07).

La resolución directa `multidict>=6.9.1` es **insatisfacible** con el grafo vigente
(sub-hallazgo F-OCM-06b): todas las versiones publicadas de `ccxt` (4.5.74 → 4.5.85)
declaran `multidict==6.7.1` con pin exacto (`uv.lock:356-371`, `pyproject.toml:84-87`,
`ccxt>=4.5.74`).

Hallazgo decisivo de la investigación: **ccxt ya corrigió el pin upstream** — PR
`ccxt/ccxt#30764` "chore(deps): bump multidict from 6.7.1 to 6.9.1"
(https://github.com/ccxt/ccxt/pull/30764), commit `bb13357f9f`, merged 2026-10-07;
`pyproject.toml` de `master` ya declara `multidict==6.9.1` (línea 83). No publicado
aún en PyPI (latest = 4.5.85).

`ccxt` no importa `multidict` en runtime (0 hits en todo el paquete): la dependencia
es metadata transitiva consumida realmente por `aiohttp`/`yarl`, ambos compatibles
con 6.9.1 (`aiohttp` permite `multidict>=4.5,<7.0`; `yarl` `>=4.0`; metadata
instalada verificada).

## Alternativas evaluadas

1. **Fix real vía `[tool.uv] override-dependencies`** (esta decisión) — fuerza
   `multidict==6.9.1` en resolución, cerrando la CVE sin `--ignore-vuln`, sin
   risk-accept y sin tocar ccxt. Costo-riesgo: muy bajo; reversible en una línea.
2. **Risk-accept formal con ADR + tracking + ignore canónico** (ADR-0050) —
   **rechazado** por decisión humana 2026-10-07: hay fix disponible y viable.
3. **Esperar ccxt 4.5.86+** (release que hereda el pin 6.9.1 de master) — válido
   como complemento, no como solución inmediata: deja la CVE abierta hasta la
   publicación. Se adopta a medio plazo (retirar el override cuando llegue).
4. **Sustitución de ccxt** — descartada: `ccxt>=4.5.74` está pinado por razones
   conocidas (cambios de firma `fetch_ohlcv()` en el adapter); coste alto,
   desproporcionado para un CVE de DoS de dependencia transitiva.

## Decisión

Forzar `multidict==6.9.1` vía **`[tool.uv] override-dependencies`** en
`pyproject.toml`:

```toml
[tool.uv]
override-dependencies = ["multidict==6.9.1"]
```

`uv` trata los overrides como **absolutos**: reemplazan por completo los requirements
de los paquetes constituyentes (docs.astral.sh/uv/reference/settings/), por lo que el
pin exacto `multidict==6.7.1` de ccxt queda sobreescrito en la resolución.

**NO** se añade `--ignore-vuln CVE-2026-104874` en ningún sitio. **NO** se bumpea ni
toca `ccxt`. **NO** hay risk-accept. **NO** se materializa ADR-0050.

## Justificación técnica

- El override selecciona una **versión parcheada** (6.9.1) de una dependencia que
  ccxt declara pero no usa en runtime → no cambia el comportamiento de ccxt.
- Alineado con el fix oficial upstream (ccxt #30764, ya mergeado): el override
  reproduce en OCM exactamente lo que ccxt aplicará en su próxima release.
- Compatible con el resto del grafo: `aiohttp 3.14.3` (`uv.lock:52`) acepta
  `<7.0,>=4.5`; `yarl 1.24.2` (`uv.lock:3361`) acepta `>=4.0`. 6.9.1 es `<7.0`
  (evita el breaking de `istr` de 7.0.0) y requiere Python `>=3.10` (OK).
- El override no instala multidict por sí mismo (debe estar pedido en el grafo), y lo
  está: es transitiva de `aiohttp`/`yarl`/`ccxt`.
- Sin cambios en ports/dominio: cero impacto en capas arquitectónicas.

## Consecuencias

- CVE-2026-104874 queda **cerrada en el grafo resuelto** (multidict 6.9.1), sin
  `--ignore-vuln` ni risk-accept.
- `DEPENDENCY_AUDIT` sigue verde con los 2 ignores vigentes (`PYSEC-2026-113`,
  `PYSEC-2026-1325`), sin añadir ninguno.
- Deuda técnica mínima y **gobernada**: el override es redundante cuando ccxt
  4.5.86+ publique; retirarlo en ese momento (o conservarlo como
  belt-and-suspenders) queda registrado como tarea de monitoreo.
- ADR-0050 (risk-accept multidict) queda **OBSOLETO**, sustituido por este.

## Rollback

Retirar la sección `[tool.uv]` de `pyproject.toml` y re-lockear (`uv lock`) →
vuelve al pin de ccxt de la release en curso (heredará 6.9.1 si ya publicada).

## Validación

En fase de aplicación autorizada (NO ejecutada en auditoría):

- `uv lock` → `multidict 6.9.1` en `uv.lock`.
- `uv run pip-audit .` → sin CVE-2026-104874.
- `uv run ruff check . && uv run lint-imports --config architecture_linter/importlinter.toml && uv run pytest tests/ -q`

## Referencias

- Hallazgo: `docs/audits/AUDIT_OCM_high-impact-improvements_2026-10-07.md`
  (F-OCM-06, F-OCM-06b, D-OCM-07a/b).
- Advisory: GHSA-54p9-h82j-f925; OSV CVE-2026-104874 (fixed `6.9.1`);
  commit `aio-libs/multidict` `350b4a07`.
- Upstream ccxt: PR `ccxt/ccxt#30764` (commit `bb13357f9f`, merged 2026-10-07):
  https://github.com/ccxt/ccxt/pull/30764; `pyproject.toml` master línea 83
  (`multidict==6.9.1`).
- Código local: `pyproject.toml:84-87` (`ccxt>=4.5.74`);
  `uv.lock:356-371` (ccxt pin multidict==6.7.1), `uv.lock:52` (aiohttp),
  `uv.lock:3361` (yarl).
- Docs uv: https://docs.astral.sh/uv/reference/settings/ (`override-dependencies`).
- ADRs relacionados: ADR-0050 (OBSOLETO), ADR-0051 (python-jose→PyJWT).