# ADR-0050: [OBSOLETO] Risk-accept multidict (rechazado por decisión humana 2026-10-07)

**Estado:** Obsoleto
**Fecha:** 2026-10-07
**Bounded context(s) afectado(s):** ocm (CI/seguridad de dependencias) | todo el runtime (dependencia transitiva)

> Este ADR queda OBSOLETO. Rechazado por decisión humana del 2026-10-07. Sustituido por ADR-0053 (fix real vía override-dependencies).

## Contexto

La auditoría 2026-10-07 (F-OCM-06, HIGH/NUEVO) detectó `multidict 6.7.1` con CVE-2026-104874
(fix `6.9.1`). El fix está formalmente disponible, pero el bump directo a `>=6.9.1` es
**insatisfacible** con el grafo de dependencias vigente (sub-hallazgo F-OCM-06b,
HIGH/CONTRADICCIÓN): **todas** las versiones publicadas de `ccxt` (4.5.74, 4.5.75, 4.5.76)
declaran `multidict==6.7.1` con pin exacto (`uv.lock:356-371`). `ccxt` es dependencia directa
pinada (`pyproject.toml:132-133`, `ccxt>=4.5.74`).

Verificación mecánica (read-only, `uv lock --dry-run`): resolver `multidict>=6.9.1` con cualquiera
de las tres versiones de ccxt devuelve el error de resolución `… your project's requirements are
unsatisfiable`. `aiohttp 3.14.3` (`uv.lock:52`) y `yarl 1.24.2` (`uv.lock:3361`) toleran rangos
abiertos de multidict; el único dependiente que lo pinea a 6.7.1 es `ccxt`.

## Alternativas evaluadas

1. **Risk-accept formal con ADR + tracking + ignore canónico** (esta decisión) — mantiene
   `multidict 6.7.1` operativo con la CVE registrada y monitoreo activo. Costo-riesgo: bajo;
   deuda gobernada.
2. **Bump de `multidict>=6.9.1` en `[project.dependencies]`** — insatisfacible hoy: rompe la
   resolución de `uv lock` (error de resolución reproducible). Descartado por restricción
   técnica, no por elección.
3. **Espera pasiva del fix de ccxt** — no es una "opción": deja la CVE sin gestión formal.
   Contradice el protocolo §B (decisión humana → ADR → control). Descartada por la humana.
4. **Sustitución de ccxt** — fuera de alcance: `ccxt>=4.5.74` está pinado por razones conocidas
   (cambios de firma `fetch_ohlcv()` en el adapter). Descartada por la humana.

## Decisión

Risk-accept temporal de **CVE-2026-104874** (`multidict 6.7.1 -> 6.9.1`) en OCM, gobernado por:

- documento de ADR (este), estado `PROPUESTO` → `ACEPTADO` tras revisión humana;
- item de tracking en `docs/plans/tracking.yaml` (id `B-61`, `estado: PENDIENTE`,
  `cadena: {origen: F-OCM-06, adr: ADR-0050}`) con la tarea de monitoreo;
- update del comando canónico `DEPENDENCY_AUDIT` en `scripts/audit_validator.py` (CANONICAL_COMMANDS)
  para incluir `CVE-2026-104874` en `--ignore-vuln`, junto a los ignores vigentes
  `PYSEC-2026-113` y `PYSEC-2026-1325`.

**NO** se bumpea `multidict`. **NO** se toca `ccxt`. **NO** se sustituye ccxt.

## Condiciones de salida

Reevaluar el risk-accept (y subir `multidict>=6.9.1`) **en cuanto ccxt publique una versión que
permita `multidict>=6.x` con `x>=6.9.1`**, o inmediatamente en el próximo bump de ccxt que se
valide en staging. Disparador del item B-61: revisión mensual de releases de ccxt hasta que el
pin de multidict deje de estar fijado a 6.7.1.

## Justificación técnica

- `multidict` es transitiva (vía `aiohttp`, `yarl`, `ccxt`); el peligro CVE-2026-104874 se
  manifiesta en código que OCM no invoca directamente en el path de trading (usos re-exportados
  internos de aiohttp/yarl).
- El pin exacto de ccxt es una restricción dura del resolver: no existe suavización local sin
  overrides de dudoso valor (un override forzaría una build que el resolver rechaza → riesgo de
  inconsistencia en producción mayor que la CVE en sí).
- Precedente consistente: `PYSEC-2026-113` (pyarrow) y `PYSEC-2026-1325` (ecdsa) ya tienen
  risk-accept en `ocm-ci.yml:353-365` (2026-08-03). Añadir `CVE-2026-104874` respeta el mismo
  patrón sin ampliar el ignore-list "por defecto" (solo con ADR + tracking).

## Consecuencias

- `DEPENDENCY_AUDIT` gatea verde con los 2 ignores existentes + `CVE-2026-104874`.
- Riesgo aceptado de forma explícita, NO eliminado: la CVE queda documentada en ADR y con item
  de tracking de monitoreo.
- El gate de seguridad NO se falsea: el ignore está justificado por ADR aprobado + cadena §B.
- Deuda gobernada: la actualización queda bloqueada a disponibilidad de ccxt compatible.

## Referencias

- Hallazgo: `docs/audits/AUDIT_OCM_high-impact-improvements_2026-10-07.md` (F-OCM-06, F-OCM-06b).
- Código: `pyproject.toml:132-133` (`ccxt>=4.5.74`, `multidict>=6.9.1` no aplicado);
  `scripts/audit_validator.py:140-145` (CANONICAL_COMMANDS `DEPENDENCY_AUDIT`);
  `uv.lock:356-371` (ccxt pin multidict==6.7.1).
- Risk-accepts previos: `.github/workflows/ocm-ci.yml:353-365`.
- ADRs relacionados: ADR-0051 (sustitución python-jose→PyJWT), ADR-0052 (risk-accept python-jose).
- Item tracking: `docs/plans/tracking.yaml` → `B-61`.