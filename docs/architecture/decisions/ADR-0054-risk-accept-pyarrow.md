# ADR-0054: Risk-accept formal de PYSEC-2026-113 (pyarrow 19.0.1) por riesgo de migración 19→23

**Estado:** PROPUESTO
**Fecha:** 2026-10-08
**Bounded context(s) afectado(s):** ocm (CI/seguridad de dependencias) | market_data.infrastructure.storage (Iceberg/Gold)

## Contexto

pyarrow 19.0.1 es dependencia runtime vía `pyiceberg[pyarrow]`. Se usa directo en
`packages/market_data/infrastructure/storage/iceberg/iceberg_storage.py:38` (`pyarrow.compute`,
`pc.max()/pc.min()` post-scan, líneas 321 y 356) y en
`packages/market_data/infrastructure/storage/gold/gold_storage.py:41` (`pa.table(...)`, línea 258).
Uso: escritura/lectura de tablas Arrow en los storages propios Iceberg (SoT Gold/features). OCM **no
ingiere archivos IPC de fuentes no confiables**.

`pip-audit` reporta **PYSEC-2026-113** (alias **CVE-2026-25087**, **GHSA-rgxp-2hwp-jwgg**):
Use-After-Free en Arrow C++ que afecta `15.0.0..23.0.0`. La condición explotable requiere
pre-buffering (`RecordBatchFileReader::PreBufferMetadata`) sobre un IPC *file*; la funcionalidad
**no está expuesta en los bindings Python** (los bindings Python/Ruby/GLib no son vulnerables, según
el propio advisory). OCM no ingiere IPC files de terceros.

**Fix disponible: pyarrow 23.0.1** (publicada 2026-02-16; el rango `>=17,<26` del pyproject lo
admite). **Este risk-accept NO se justifica por ausencia de fix.**

## Decisión

**Risk-accept formal** de PYSEC-2026-113 en pyarrow 19.0.1, justificado por **riesgo de migración
19→23**:

- Cuatro majors intermedias (20, 21, 22, 23) con breaking changes de Arrow C++/PyArrow.
- Superficie de cambio concreta en OCM: `iceberg_storage.py` y `gold_storage.py` (exigencia de
  schema exacto con `.cast(schema.as_arrow())` y nullability estricta, `pc.max()/pc.min()` sobre
  columnas de timestamp, generación de datasets Gold) — requiere ventana de testing específica
  (smoke Iceberg + Gold + ejecución real del pipeline Bronze→Gold), no disponible en esta fase.
- Exposición real baja: ninguna ingestión de IPC/Arrow de fuentes no confiables + bindings Python
  no vulnerables según el advisory.

Mitigaciones compensatorias:

- Único camino de datos Arrow = storages propios sobre Iceberg (dataset controlado, schema fijo de
  `market_data.domain`); no hay entrada externa de IPC.
- CI mantiene `pip-audit` (modo entorno) como gate; el ignore de esta CVE queda documentado por
  este ADR (antes: risk-accept implícito del 2026-08-03).
- Monitoreo de releases pyarrow para detectar ventana de migración.

## Consecuencias

- L12e': `--ignore-vuln PYSEC-2026-113` se mantiene en el paso DEPENDENCY_AUDIT de CI, ahora
  respaldado por risk-accept formal.
- El riesgo queda aceptado y documentado, no eliminado.
- Plan de salida: tracking **B-61** (migrar `pyarrow>=23.0.1` cuando exista ventana de testing
  dedicada; retirar el ignore en CI).

## Condición de salida

- **Trigger de reevaluación:** ventana de mantenimiento mayor siguiente (estimada **Q1 2027**), o
  cualquier cambio que introduzca ingestión de IPC/Arrow desde fuentes no confiables.
- **Criterio de cierre:** migrar a `pyarrow>=23.0.1` con suite y smoke Iceberg/Gold verdes;
  **retirar** `--ignore-vuln PYSEC-2026-113` en `ocm-ci.yml` y `scripts/audit_validator.py`;
  recalcular el hash de evidence; cerrar el item B-61.

## Rollback

No aplica rollback de dependencia (pyarrow permanece 19.0.1). Si se decide migrar antes del
trigger: `uv lock -P pyarrow==23.0.1`, retirar el ignore en `ocm-ci.yml`/`audit_validator.py`,
recalcular el evidence hash y cerrar B-61.

## Referencias

- Hallazgo origen: risk-accept histórico PYSEC-2026-113 (2026-08-03; `ocm-ci.yml:400-404`,
  `scripts/audit_validator.py:140-145`, tracking). La auditoría 2026-10-07 revalidó el riesgo
  (DEPENDENCY_AUDIT FAIL = F-OCM-06, python-jose+multidict); pyarrow ya estaba cubierto por el
  risk-accept previo y no se reabrió. La causa del risk-accept es la propia CVE.
- Código afectado: `packages/market_data/infrastructure/storage/iceberg/iceberg_storage.py:38,321,356`;
  `packages/market_data/infrastructure/storage/gold/gold_storage.py:41,258`.
- ADR relacionados: ADR-0053 (patrón de fix real de dependencias); ADR-0050/0052 (risk-accepts
  previos).
- Fuentes externas: https://nvd.nist.gov/vuln/detail/CVE-2026-25087 ·
  https://github.com/advisories/GHSA-rgxp-2hwp-jwgg · https://arrow.apache.org/release/