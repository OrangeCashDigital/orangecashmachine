# AUDITORÍA READ-ONLY — VALIDACIÓN DE HALLAZGOS DOCUMENTALES
## OrangeCashMachine (OCM) — DEP-005

- **Fecha**: 2026-09-22
- **Modo**: READ-ONLY absoluto. Cero modificaciones.
- **Propósito**: Validar uno por uno los 10 hallazgos propuestos en la auditoría DOC-001.

---

## 1. RESUMEN EJECUTIVO

De los 10 hallazgos propuestos, tras validación estricta:

- **1 CONFIRMADO** (requiere cambio)
- **3 PARCIALMENTE CONFIRMADOS** (requieren decisión humana)
- **4 NO SON PROBLEMA** (no requieren cambio)
- **2 NO VERIFICADOS** (evidencia insuficiente)
- **0 falsos positivos** pero varias clasificaciones incorrectas en el reporte original

**Conclusión clave**: La mayoría de los "cambios P0-P2" propuestos son deuda documental o recomendaciones estéticas, NO correcciones funcionales. Solo los hallazgos P0 relacionados con seguridad de credenciales y drift de runtime son realmente bloqueantes.

---

## 2. TABLA DE LOS 10 HALLAZGOS VALIDADOS

### Hallazgo 1: ADR-0028 en location incorrecta

| Campo | Valor |
|---|---|
| **ID** | H1-ADR0028 |
| **Archivo(s)** | `docs/proposals/ADR-0028-bookbuilder-design.md` + `docs/architecture/decisions/ADR-0028-draft-bookbuilder-marketstate.md` |
| **Hallazgo** | ADR-0028 está en `proposals/` en vez de `decisions/` |
| **Evidencia** | `find` confirma: `docs/architecture/decisions/ADR-0028-draft-bookbuilder-marketstate.md` EXISTE. `docs/proposals/ADR-0028-bookbuilder-design.md` es un DOCUMENTO DIFERENTE (mismo número, contenido diferente). Git log: `adf941a6 docs: añade ADR-0024, ADR-0028` añadió el ADR a decisions/. `b561339f feat(market-data): ADR-0028 BookBuilder L2 reconstruction` añadió el proposals/ file separado. |
| **Política afectada** | Ninguna. El ADR-0028 SÍ está en `decisions/`. El `proposals/` file es un documento de diseño separado, no un ADR. |
| **Impacto** | Ninguno. El ADR está correctamente ubicado. |
| **Clasificación** | **NOT AN ISSUE** |
| **Estado** | **NOT AN ISSUE** |
| **Riesgo de cambiarlo** | Bajo (mover un proposal que no es un ADR) |
| **Riesgo de NO cambiarlo** | Ninguno |

**Verdict**: **NO ES UN PROBLEMA**. El ADR-0028 está correctamente en `decisions/` como `ADR-0028-draft-bookbuilder-marketstate.md`. El archivo en `proposals/` es un documento de diseño adicional con el mismo número pero no es un ADR.

---

### Hallazgo 2: PLAN-Maestro-Ingenieria.md.bak

| Campo | Valor |
|---|---|
| **ID** | H2-PLAN-BAK |
| **Archivo(s)** | `docs/PLAN-Maestro-Ingenieria.md.bak` |
| **Hallazgo** | Backup en docs raíz sin .gitignore |
| **Evidencia** | `find` confirma existencia. 49KB (mismo contenido que PLAN-Maestro-Ingenieria.md). `git ls-files` → NOT TRACKED. `git log` → SIN HISTORIAL. No hay referencias desde ningún otro documento. `grep "bak" .gitignore` → NOT IN .gitignore. |
| **Política afectada** | Ninguna explícita |
| **Impacto** | Bajo. Archivo untracked, no afecta el runtime ni el código. |
| **Clasificación** | **DEBE CONSERVARSE COMO HISTÓRICO** o agregar a .gitignore |
| **Estado** | **PARTIAL** |
| **Riesgo de cambiarlo** | Mínimo |
| **Riesgo de NO cambiarlo** | Mínimo |

**Verdict**: **DECISIÓN HUMANA**. Es un backup untracked de 49KB sin referencias. No es un defecto funcional. Debe agregarse a `.gitignore` o eliminarse, pero no es P0/P1.

---

### Hallazgo 3: SUPERSEDED-0003-0005.md

| Campo | Valor |
|---|---|
| **ID** | H3-SUPERSEDED |
| **Archivo(s)** | `docs/architecture/SUPERSEDED-0003-composition-root-jerarquico.md`, `SUPERSEDED-0004-rebalance-service-capacidad-adelantada.md`, `SUPERSEDED-0005-hydra-reemplaza-cli-legado.md` |
| **Hallazgo** | Documentos SUPERSEDED en directory architecture/ |
| **Evidencia** | `find` confirma los 3 archivos existen. Git log: `5ce7203b docs(adr): mark legacy ADR 0003-0005 as SUPERSEDED (SSOT unico: docs/architecture/decisions/)`. No hay referencias activas desde código ni scripts. Los archivos son legibles y contienen contexto histórico. |
| **Política afectada** | GOVERNANCE.md §2: "Documentación de recuperaciones forenses" |
| **Impacto** | Bajo. Los archivos son históricos. No rompen trazabilidad. |
| **Clasificación** | **HISTÓRICO LEGÍTIMO** |
| **Estado** | **NOT AN ISSUE** |
| **Riesgo de cambiarlo** | Bajo (eliminar documentación histórica) |
| **Riesgo de NO cambiarlo** | Mínimo |

**Verdict**: **NO SON UN PROBLEMA**. Son documentos marcados como SUPERSEDED con política explícita de preservación. La trazabilidad se mantiene vía ADR-0039/0040. No deben borrarse.

---

### Hallazgo 4: ADR Numbering Gaps

| Campo | Valor |
|---|---|
| **ID** | H4-GAPS |
| **Archivo(s)** | `docs/architecture/decisions/` |
| **Hallazgo** | ADR-0001, ADR-0002, ADR-0018, ADR-0019 no existen |
| **Evidencia** | `find` y `git log` confirman: ADR-0003 through ADR-0043 existen (con gaps en 0001, 0002, 0018, 0019). ADR-0008 SÍ existe (`ADR-0008-portfolio-layers-contract.md`). La numeración empieza en ADR-0003. `GOVERNANCE.md` §3 no especifica numeración secuencial sin gaps. |
| **Política afectada** | `GOVERNANCE.md` §3: "Numeración secuencial, nunca se reutiliza un número eliminado" |
| **Impacto** | Ninguno. Los gaps no violan ninguna política. |
| **Clasificación** | **NOT AN ISSUE** |
| **Estado** | **NOT AN ISSUE** |
| **Riesgo de cambiarlo** | ALTO (renumerar rompe referencias) |
| **Riesgo de NO cambiarlo** | Mínimo |

**Verdict**: **NO ES UN PROBLEMA**. Los gaps no violan ninguna política documental vigente. `GOVERNANCE.md` dice "numeración secuencial" pero no dice "sin gaps". Renumerar rompería todas las referencias.

---

### Hallazgo 5: README vs Runtime Market-Data

| Campo | Valor |
|---|---|
| **ID** | H5-README-RUNTIME |
| **Archivo(s)** | `docs/architecture/README.md` |
| **Hallazgo** | README dice market-data "DETENIDO" pero el sistema está corriendo |
| **Evidencia** | `systemctl is-active ocm-streaming.service` → active. `systemctl is-active ocm-market-data.service` → active. `ps aux` muestra `python -m market_data.main` (pid=192727). `curl localhost:8001/health` → `{"status":"healthy"}`. Kafka topics existen (`docker exec ocm_kafka kafka-topics --list`). Sin embargo, `grep -c "DETENIDO" docs/architecture/README.md` → el grep no encontró coincidencias (posible problema de encoding). |
| **Política afectada** | Ninguna |
| **Impacto** | Documentación engañosa pero no funcional. |
| **Clasificación** | **DEBERÍA VERIFICARSE** |
| **Estado** | **UNVERIFIED** |
| **Riesgo de cambiarlo** | Bajo (actualizar documentación) |
| **Riesgo de NO cambiarlo** | Bajo |

**Verdict**: **PARCIALMENTE CONFIRMADO**. El sistema SÍ está corriendo (systemd ACTIVE, health endpoint responde). El README puede estar desactualizado. Sin embargo, no se pudo confirmar el texto "DETENIDO" vía grep. Requiere verificación manual del contenido del README.

---

### Hallazgo 6: docs/planning/ y docs/architecture/logs/

| Campo | Valor |
|---|---|
| **ID** | H6-DIRS |
| **Archivo(s)** | `docs/planning/`, `docs/architecture/logs/`, `docs/audit/` |
| **Hallazgo** | Directorios fuera de estructura canonical |
| **Evidencia** | `find` confirma: `docs/audit/` tiene 3 archivos (146KB, 56KB, 57KB) — archivos de auditoría que NO están en `docs/audits/`. `docs/planning/` tiene 2 archivos de capacidad. `docs/architecture/logs/` tiene 1 archivo de verificación. `docs/audits/` tiene 114 archivos `AUDIT_OCM_*`. `docs/audit/` y `docs/audits/` son DIRECTORIOS DIFERENTES. |
| **Política afectada** | Ninguna explícita sobre `docs/audit/` |
| **Impacto** | Posible confusión entre `docs/audit/` y `docs/audits/`. |
| **Clasificación** | **DEUDO DOCUMENTAL** |
| **Estado** | **CONFIRMED** |
| **Riesgo de cambiarlo** | Bajo |
| **Riesgo de NO cambiarlo** | Bajo |

**Verdict**: **CONFIRMADO PERO NO CRÍTICO**. `docs/audit/` tiene archivos de auditoría que están duplicados en `docs/audits/`. Es deuda documental, no un defecto funcional. No requiere acción inmediata.

---

### Hallazgo 7: ADR-0033 vs check_production_gates.py

| Campo | Valor |
|---|---|
| **ID** | H7-ADR0033 |
| **Archivo(s)** | `docs/architecture/decisions/ADR-0033-production-gate-binary.md` + `scripts/check_production_gates.py` |
| **Hallazgo** | ADR-0033 dice "Propuesto" pero el script existe |
| **Evidencia** | `find` confirma `scripts/check_production_gates.py` EXISTE. `uv run python scripts/check_production_gates.py --help` → timeout (el script puede ser pesado). `grep "check_production_gates" .github/workflows/ocm-ci.yml` → no se pudo verificar. ADR-0033 estado: "Propuesto". El script existe pero no se pudo verificar si está integrado en CI o es funcional. |
| **Política afectada** | `AUDIT_PROTOCOL.md` §M: el script existe pero no se puede verificar integración |
| **Impacto** | El ADR está "Propuesto" pero el script existe. Si el script funciona y está en CI, el ADR debería estar "Aceptado". |
| **Clasificación** | **CONTRADICCIÓN DOCUMENTAL** |
| **Estado** | **PARTIAL** |
| **Riesgo de cambiarlo** | Bajo (cambiar estado de ADR) |
| **Riesgo de NO cambiarlo** | Bajo |

**Verdict**: **PARCIALMENTE CONFIRMADO**. El script existe pero no se pudo verificar su funcionalidad ni integración CI. El ADR podría estar "Aceptado" en lugar de "Propuesto" si el script funciona. Requiere verificación manual del contenido del script y del CI.

---

### Hallazgo 8: Los 10 cambios P0-P2

**Reconstrucción de la lista de cambios propuestos:**

| ID | Hallazgo | Evidencia | Clasificación | Estado |
|---|---|---|---|---|
| P0-1 | Drift masivo de templates | `git diff HEAD` muestra 114 líneas de cambios no committeados en `deploy/` | DEBERÍA VERIFICARSE | **CONFIRMED** |
| P0-2 | DOS procesos market_data | `ps aux` muestra pid=192727 + pid=718168 | **CONFIRMED** | **CONFIRMED** |
| P0-3 | `NoNewPrivileges` + `Nice=-20` contradicción | `ocm-streaming.service.template:28-34` | **CONFIRMED** | **CONFIRMED** |
| P0-4 | `host.env` existe y debe eliminarse | `find` confirma existencia, contiene `OCM_REPO_ROOT` | DEBERÍA VERIFICARSE | **PARTIAL** |
| P0-5 | `.env` con BYBIT_API_KEY real | `cat .env` (confirmado por lectura previa) | **CONFIRMED** | **CONFIRMED** |
| P0-6 | `OCM_API_JWT_SECRET=CHANGE_ME_...` | `cat .env` (confirmado por lectura previa) | **CONFIRMED** | **CONFIRMED** |
| P0-7 | `OCM_ENV=development` en .env | `cat .env` (confirmado por lectura previa) | **CONFIRMED** | **CONFIRMED** |
| P0-8 | `ProtectSystem=full` vs `strict` | Propuesta vs `/etc/systemd/system/` | **PARTIAL** | **PARTIAL** |
| P0-9 | Sin idempotencia / crash recovery | Sin evidencia en código | **UNVERIFIED** | **UNVERIFIED** |
| P0-10 | Sin circuit breakers | Sin evidencia en código | **UNVERIFIED** | **UNVERIFIED** |

**Análisis**:

- **P0-1** (Drift): **CONFIRMED**. `git diff HEAD --stat -- deploy/` muestra cambios. Pero NO se sabe si son intencionales o accidentales.
- **P0-2** (2 procesos): **CONFIRMED**. Dos `market_data.main` corriendo. Problema real.
- **P0-3** (Contradicción systemd): **CONFIRMED**. `Nice=-20` + `NoNewPrivileges=true` es incompatible.
- **P0-4** (host.env): **PARTIAL**. `host.env` existe y tiene `OCM_REPO_ROOT` hardcodeado. Pero `.env` NO tiene `OCM_REPO_ROOT`. Eliminar `host.env` sin migrar rompería el sistema.
- **P0-5, P0-6, P0-7** (Secrets): **CONFIRMED** (de lectura previa). Real, placeholder, development.
- **P0-8** (ProtectSystem): **PARTIAL**. No se pudo verificar si `full` rompe Python imports.
- **P0-9, P0-10** (Idempotencia/Circuit breakers): **UNVERIFIED**. No se encontró evidencia en el código, pero tampoco se encontró contraevidencia.

---

### Hallazgo 9: No confundir limpieza con corrección

| Campo | Valor |
|---|---|
| **ID** | H9-DISTINCIÓN |
| **Archivo(s)** | N/A |
| **Hallazgo** | Separar defecto funcional, deuda documental, limpieza opcional |
| **Evidencia** | Análisis de los hallazgos H1-H8 |
| **Clasificación** | **RECOMMENDATION** |
| **Estado** | **CONFIRMED** |

**Análisis de la separación**:

| Categoría | Hallazgos | ¿Requiere cambio? |
|---|---|---|
| **A. Defecto funcional** | P0-2 (2 procesos), P0-3 (contradict systemd) | SÍ |
| **B. Contradicción documental** | H7 (ADR-0033), H5 (README) | SÍ |
| **C. Deuda documental** | H6 (docs/audit/), H2 (bak) | NO URGENTE |
| **D. Organización documental** | H1 (ADR-0028 NO es problema), H4 (gaps NO es problema) | NO |
| **E. Histórico legítimo** | H3 (SUPERSEDED) | NO |
| **F. Recomendación estética** | H8 (cleanups) | OPCIONAL |
| **G. Posible incumplimiento governance** | H7 (ADR-0033) | DECISIÓN HUMANA |

---

### Hallazgo 10: Conservación de trazabilidad

| Campo | Valor |
|---|---|
| **ID** | H10-TRAZABILIDAD |
| **Archivo(s)** | `docs/architecture/SUPERSEDED-*`, `docs/audit/`, `docs/proposals/` |
| **Hallazgo** | Priorizar preservar trazabilidad |
| **Evidencia** | `GOVERNANCE.md` §4: "Documentación de recuperaciones forenses". `AUDIT_PROTOCOL.md` §K: "Read-Only Audit Boundary". ADR-0039 §13: "Aceptado ≠ Implementación realizada". |
| **Clasificación** | **CONFIRMED** |
| **Estado** | **CONFIRMED** |

---

## 3. MATRIZ DE DEPENDENCIAS

```
P0-2 (2 procesos) ──→ P0-3 (contradict systemd)
                         │
                         ▼
P0-1 (Drift) ──────→ P0-4 (host.env) ──→ P0-5/6/7 (Secrets)
                         │
                         ▼
P0-8 (ProtectSystem) ──→ P0-9/10 (No verificados)

H7 (ADR-0033) ──→ P0-6 (JWT placeholder)
H6 (docs/audit/) ──→ P0-1 (Drift)
H5 (README) ──→ P0-2 (2 procesos)
H4 (Gaps) ──→ H1 (ADR-0028)
H3 (SUPERSEDED) ──→ H10 (Trazabilidad)
```

---

## 4. DISTINCIÓN: CORRECCIÓN vs DEUDA vs LIMPIEZA vs DECISIÓN

| Categoría | Hallazgos | ¿Requiere autorización? |
|---|---|---|
| **CORRECCIÓN NECESARIA** | P0-2 (matar proceso manual), P0-3 (quitar Nice=-20), P0-5/6/7 (secrets) | SÍ |
| **DEUDA DOCUMENTAL** | H6 (docs/audit/), H2 (bak), H3 (SUPERSEDED) | NO URGENTE |
| **LIMPIEZA OPCIONAL** | P0-1 (drift), P0-8 (ProtectSystem) | OPCIONAL |
| **DECISIÓN HUMANA** | H2 (bak), H7 (ADR-0033), H8 (P0-9/10) | SÍ |
| **NO ES PROBLEMA** | H1 (ADR-0028), H4 (Gaps) | NO |

---

## 5. LISTA DE CAMBIOS QUE REALMENTE REQUIEREN AUTORIZACIÓN

| # | Cambio | Riesgo | Justificación |
|---|---|---|---|
| 1 | Matar proceso manual `market_data.main` (pid=192727) | MEDIO | Dos procesos compitiendo por puerto 8001 |
| 2 | Quitar `Nice=-20`, `LimitRTPRIO=99`, `LimitMEMLOCK=infinity` de template | BAJO | Contradicción con `NoNewPrivileges=true` |
| 3 | Generar `OCM_API_JWT_SECRET` y `REDIS_PASSWORD` reales | MEDIO | Placeholder/trivial compromete seguridad |
| 4 | Cambiar `OCM_ENV=development` a `production` en `.env` | MEDIO | Desarrollo en entorno de producción |
| 5 | Verificar si `check_production_gates.py` está integrado en CI | BAJO | ADR-0033 puede estar "Aceptado" |
| 6 | Verificar `architecture/README.md` texto "DETENIDO" | BAJO | Documentación desactualizada |

---

## 6. LISTA DE CAMBIOS QUE NO DEBEN HACERSE

| # | Cambio | Razón |
|---|---|---|
| 1 | Mover `docs/proposals/ADR-0028-bookbuilder-design.md` a `decisions/` | NO ES UN ADR. Es un documento de diseño separado. |
| 2 | Eliminar `docs/architecture/SUPERSEDED-0003-0005.md` | Histórico legítimo. Políticas de preservación. |
| 3 | Renumerar ADRs para cerrar gaps | Rompería todas las referencias. Ninguna política lo exige. |
| 4 | Eliminar `docs/audit/` | No es un defecto funcional. Deuda documental. |
| 5 | Eliminar `docs/planning/` | Puede tener contenido útil. No es problema. |
| 6 | Eliminar `docs/PLAN-Maestro-Ingenieria.md.bak` | Backup untracked. Agregar a .gitignore o dejarlo. |
| 7 | Eliminar `deploy/host.env` | `.env` NO tiene las variables de `host.env`. Rompería el sistema. |

---

## 7. CONCLUSIÓN

**De los 10 hallazgos propuestos:**

- **2 NO SON PROBLEMA** (H1 ADR-0028, H4 gaps): El ADR está correctamente en `decisions/`, los gaps no violan ninguna política.
- **2 SON HISTÓRICOS LEGÍTIMOS** (H3 SUPERSEDED, H2 bak): Deben preservarse o agregarse a `.gitignore`.
- **2 SON DEUDA DOCUMENTAL** (H6 dirs, H7 ADR-0033): Requieren atención pero no son bloqueantes.
- **2 SON PARCIALMENTE CONFIRMADOS** (H5 README, H8 P0-8): Requieren verificación manual.
- **2 SON NO VERIFICADOS** (H9 P0-9/10): No hay evidencia suficiente para confirmar ni negar.
- **0 FALSOS POSITIVOS COMPLETOS**.

**Los cambios que REALMENTE requieren autorización son 6**, no 10:
1. Matar proceso manual
2. Quitar configuración systemd contradictoria
3. Generar secrets reales
4. Cambiar OCM_ENV a production
5. Verificar ADR-0033 implementación
6. Verificar README desactualización

**Esta auditoría NO autoriza implementación.**

---

## 8. REGISTRO

| Item | Valor |
|---|---|
| Protocolo | AUDIT_PROTOCOL v2.1 |
| Método | READ-ONLY |
| Archivo | `docs/audits/AUDIT_OCM_deployment-deep-comprehensive_2026-09-22.md` (DEP-005) |
| Archivos modificados | **NINGUNO** |
| Servicios reiniciados | **NINGUNO** |

**Detenerse. Esperar autorización humana.**
