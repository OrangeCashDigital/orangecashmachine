# ADR-0052: [OBSOLETO] Risk-accept formal de CVE-2026-85394 (python-jose) — rechazado por decisión humana D-OCM-07b

**Estado:** Obsoleto
**Fecha:** 2026-10-07
**Bounded context(s) afectado(s):** apps/api (gateway experimental) | ocm (CI/seguridad)

> Este ADR queda OBSOLETO. Rechazado por decisión humana D-OCM-07b del 2026-10-07: para una CVE
> crítica sin fix upstream ni actividad del mantenedor, y con sustituto directo de bajo costo
> (PyJWT), el risk-accept no es defendible. Sustituido por **ADR-0051** (sustitución
> python-jose → PyJWT).

## Contexto

`python-jose 3.5.0` (pin `python-jose[cryptography]>=3.3.0,<4.0`, `pyproject.toml:118`) presenta
**CVE-2026-85394** (HIGH, NUEVO — F-OCM-06, auditoría 2026-10-07) **sin fix listado** en el
advisory upstream. Este borrador fue la **alternativa B** de D-OCM-07b: gobernar el riesgo sin
sustituir la librería, en lugar de la migración a PyJWT del ADR-0051.

Precedente consistente en el repo: `PYSEC-2026-113` (pyarrow) y `PYSEC-2026-1325` (ecdsa,
transitiva del mismo python-jose) tienen risk-accept documentado en
`.github/workflows/ocm-ci.yml:353-365` (2026-08-03), gestionado por ignore-list canónica.
`ecdsa` — transitiva de python-jose — ya está bajo risk-accept: aceptar la CVE del paquete raíz
mantenía el mismo criterio en toda la cadena.

## Superficie de exposición real (solo hechos verificables)

- **Único consumidor:** `apps/api/auth/jwt.py:19-74` — firma y verificación HS256 del bearer
  token del gateway API.
- **Consumidores del módulo:** `apps/api/deps.py:19,79` (`verify_token` → 401 limpio).
- **Exposición:** gateway API **experimental** (`uv run ocm-api`), CD placeholder, sin
  exposición pública en producción (ADR-0016 `IS_STUB` fail-closed). Tokens generados y
  verificados con secreto local (`ApiSettings.jwt_secret`, `apps/api/settings.py:52`).
- **Algoritmo:** HS256 simétrico — sin uso de `jwe`/`jwk`/`jws`/RS256 (módulos no invocados).
- **No hay emisión externa de tokens ni integración con proveedor de identidad.**

Evaluación: la CVE es un problema de la librería en general (parece no tener fix upstream), pero
el path de uso real (HS256 interno, gateway experimental, secreto single-operator) limita la
explotabilidad material. No se verifica exploitabilidad activa en el código de OCM.

## Mitigaciones compensatorias

1. Secreto de firma: SSOT en `ApiSettings.jwt_secret` (`apps/api/settings.py:52`), fuera de logs
   y con validación de no-default en arranque (field_validator :113).
2. Gateway no expuesto en producción; si se expusiera, el gate G1+ y la revisión de esta decisión
   son precondición (§ condiciones de revisión).
3. Tokens de corta vida: TTL 60 min por defecto (`jwt.py:27`, `settings.py:59`), reduciendo la
   ventana de explotación de un token robado.
4. Fail-soft en verificación con logging DEBUG (`jwt.py:58-74`): sin traceback/stack leak al
   cliente.
5. Sin entrada de red directa previa al gateway (rate-limit middleware testado:
   `tests/api/test_rate_limit_middleware.py`).
6. Monitoreo de dependencias: pip-audit en CI (`DEPENDENCY_AUDIT`) con la CVE visible como
   `--ignore-vuln` **justificado** (no oculto).

## Alternativas evaluadas

1. **Risk-accept formal** (este ADR) — gobernado con ADR+tracking, vigencia y re-evaluación;
   no elimina la CVE pero la visibiliza y la mitiga por path de uso. Costo-riesgo: bajo, exige
   disciplina de renovación.
2. **Sustitución por PyJWT** — ver ADR-0051: elimina la CVE a costa de refactor en
   `apps/api/auth/jwt.py` + retirar `PYSEC-2026-1325` (ecdsa). **Opción elegida** (D-OCM-07b,
   opción A): compatibilidad de API confirmada en ADR-0051 (cuadro de compatibilidad).
3. **Esperar fix upstream** — CVE abierta sin gestión formal; rechazada por la política de
   riesgo registrado.

## Decisión

**RECHAZADO — este ADR NO se aplica.** La decisión humana D-OCM-07b (2026-10-07) eligió la
**opción A** (sustitución python-jose → PyJWT, ADR-0051): para una CVE crítica sin fix ni
respuesta upstream y con sustituto directo y de bajo costo, mantener la dependencia vía
risk-accept no es defendible. La materialización de ADR-0051 elimina la CVE del grafo y cierra
de paso `PYSEC-2026-1325` (ecdsa transitiva).

De haberse aceptado este documento, habría implicado:

- Añadir `CVE-2026-85394` a los `--ignore-vuln` del comando canónico `DEPENDENCY_AUDIT`
  (`scripts/audit_validator.py:140-145`), junto a `PYSEC-2026-113`, `PYSEC-2026-1325`.
- Crear item de tracking `B-62` en `docs/plans/tracking.yaml`:
  `estado: PENDIENTE`, `descripcion: "Re-evaluar CVE-2026-85394 (python-jose)…"`,
  `cadena: {origen: F-OCM-06, adr: ADR-0052}`.

Ninguno de esos cambios se ejecutó; el ADR queda como registro normativo del rechazo
(referencia §B: conocimiento → decisión humana → ADR → control).

## Condiciones de revisión

Innecesarias — ADR OBSOLETO. La sustitución (ADR-0051) elimina la superficie de riesgo; la
vigilancia se traslada a la revisión semestral de dependencias del gateway.

## Justificación del rechazo

- El exploit potencial de CVE-2026-85394 no tiene fix upstream y el mantenedor no responde
  (#414 desde ~junio 2026, sin SECURITY.md).
- SUSE lo clasifica "Won't fix" para sus distribuciones; python-jose sin actividad desde su
  último release 3.5.0 (2025-05-28).
- Existe sustituto directo (PyJWT) con API idéntica para el caso de uso real (HS256), validado
  por cuadro de compatibilidad (ADR-0051) y adoptado por el ecosistema (FastAPI docs, Litestar,
  SEMICeu/sdep).
- Patrón espejo: igual que ADR-0050 (risk-accept multidict) quedó **OBSOLETO** en favor de un
  fix real (ADR-0053, override-dependencies), el risk-accept de python-jose se descarta en favor
  de la sustitución.

## Consecuencias

- **No** se añade `CVE-2026-85394` a la ignore-list canónica.
- El gate `DEPENDENCY_AUDIT` mantiene los 2 ignores vigentes (`PYSEC-2026-113`,
  `PYSEC-2026-1325`); el segundo desaparecerá del grafo con la materialización de ADR-0051.
- La CVE queda documentada como rechazada (registro normativo), con trazabilidad a la decisión
  humana D-OCM-07b.

## Referencias

- Hallazgo: `docs/audits/AUDIT_OCM_high-impact-improvements_2026-10-07.md` (F-OCM-06).
- Análisis bibliográfico: FastAPI docs JWT (fanjwt); `mpdavis/python-jose#414`; migración
  `jpadilla/pyjwt#942`; Litestar `#3549`/`#3684`; SEMICeu/sdep; MAAS → joserfc (D-OCM-07b).
- Código: `pyproject.toml:118`; `apps/api/auth/jwt.py:19-74`; `apps/api/deps.py:19,79`;
  `apps/api/settings.py:52,59,113`; `scripts/audit_validator.py:140-145`; `ocm-ci.yml:353-365`.
- ADRs relacionados: **ADR-0051** (sustitución python-jose→PyJWT, la adoptada); **ADR-0053**
  (fix real multidict, patrón de sustitución); **ADR-0050** (risk-accept multidict, OBSOLETO).