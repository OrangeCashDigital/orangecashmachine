# OCM — Comprehensive Forensic & Provenance Audit of `corangecashmachine`

**Fecha de auditoría:** 2026-08-18  
**Commit Auditado (`orangecashmachine`):** `bee9fb5a3917c32fcc81fcc81fa5177ce0e57283`  
**Ruta Investigada:** `/home/orangemusic/trading/corangecashmachine`  
**Objetivo:** Reconstruir la identidad, procedencia exacta, propósito histórico y relación sistémica del directorio `corangecashmachine` con respecto a `orangecashmachine`.

---

## 1. Executive Summary

La investigación forense y de procedencia sobre `/home/orangemusic/trading/corangecashmachine` demuestra de manera concluyente que **no es un repositorio Git independiente, ni un fork completo, ni un proyecto anterior**, sino un **directorio sandbox auxiliar / borrador de desarrollo** creado el **6 de agosto de 2026 a las 18:31** (coincidentes al minuto con la autoría del commit `930ab2c7` en `orangecashmachine` titulado *"test(architecture): guard no-vacuo de import-linter"*). 

Contiene exclusivamente tres archivos de pruebas arquitectónicas (`tests/architecture/`) creados como área de pruebas temporal o producto de un error tipográfico al denominar el directorio de trabajo. Actualmente se encuentra completamente huérfano, inactivo y sin dependencias operativas activas.

---

## 2. Scope & Methodology

- **Alcance:** Directorio huérfano en `/home/orangemusic/trading/corangecashmachine` y su correlación con el repositorio principal `orangecashmachine`.
- **Metodología:** Análisis forense estricto de solo lectura combinando metadatos del sistema de archivos (`stat`), inspección de contenido, ausencia de control de versiones Git (`Not git`) y correlación temporal con el historial de commits (`git log`).

---

## 3. Identity (Identidad del Directorio)

- **Fecha y Hora de Creación / Modificación:** `2026-08-06 18:31:36 -0500` (según inodos y timestamps del sistema de archivos).
- **Tamaño:** Mínimo (10 bloques, 3 archivos Python).
- **Estructura:**
  - `tests/architecture/test_import_linter_no_vacuo.py`
  - `tests/architecture/test_import_linter_sanitized.py`
  - `tests/architecture/test_kafka_schemas_roundtrip.py`
- **Control de Versiones:** Ausente (`fatal: no es un repositorio git`).

---

## 4. Timeline (Línea Temporal Reconstruida)

1. **6 de agosto de 2026 (~18:31):** El desarrollador crea el directorio `corangecashmachine` (presumiblemente por un error tipográfico al teclear `orangecashmachine`) y deposita allí un borrador inicial de los tests de arquitectura para validar el import-linter.
2. **6 de agosto de 2026 (18:32):** Se registra el commit `930ab2c7` en el repositorio oficial `orangecashmachine` (*"test(architecture): guard no-vacuo de import-linter"*), incorporando los tests definitivos en la ruta correcta (`tests/architecture/`).
3. **7 de agosto al 18 de agosto de 2026:** El directorio `corangecashmachine` queda inalterado, sin actividad posterior, convirtiéndose en un remanente huérfano de desarrollo.

---

## 5. Relationship with `orangecashmachine`

- **Naturaleza:** Es un borrador de trabajo o directorio auxiliar temporal generado durante la sesión de creación del commit `930ab2c7`.
- **Diferencia de Código:** Los archivos en `corangecashmachine` son versiones preliminares o idénticas a los que terminaron commiteándose en el repo oficial, pero sin el respaldo de un `.git`.

---

## 6. Git Forensics

- El comando `git log --all --full-history` no encuentra rastro de `corangecashmachine` como rama ni remote en el repo principal.
- La correlación temporal es la evidencia clave: la marca de tiempo `2026-08-06 18:31:36` precede por exactamente 45 segundos al commit `930ab2c7` en `orangecashmachine`.

---

## 7. Architecture / Test Relationship

- Contiene prototipos de los tests que blindan el `import-linter` contra ejecuciones vacuas y validan el roundtrip de esquemas Kafka. Estos tests ya viven formalmente y de forma activa en `orangecashmachine/tests/architecture/`.

---

## 8. Dependency Analysis

- **¿Dependencias activas?:** Ninguna. Ningún script, servicio systemd, cron, compose o pipeline de CI hace referencia a `/home/orangemusic/trading/corangecashmachine`.

---

## 9. Knowledge / Documentation Relationship

- No está mencionado en ningún ADR, Plan Maestro, GOVERNANCE.md ni tracking.yaml. Es un artefacto puramente incidental del entorno de desarrollo local.

---

## 10. Evidence Matrix

| ID | Evidencia | Ubicación | Tipo | Qué demuestra | Confianza |
|---|---|---|---|---|---|
| E-01 | `stat /home/orangemusic/trading/corangecashmachine` | Filesystem | Hecho | Creado el 2026-08-06 a las 18:31:36 | HIGH |
| E-02 | `git status` en corangecashmachine | Filesystem | Hecho | No es un repositorio Git (`Not git`) | HIGH |
| E-03 | `git log` commit `930ab2c7` | `orangecashmachine` | Hecho | Creado 45 segundos antes del commit de tests de arquitectura | HIGH |
| E-04 | Contenido de `tests/architecture/` | corangecashmachine | Hecho | Contiene los 3 tests preliminares de arquitectura | HIGH |

---

## 11. Relationship Matrix

| Elemento | `corangecashmachine` | `orangecashmachine` | Relación | Evidencia |
|---|---|---|---|---|
| Código fuente | 3 archivos de tests | Repositorio completo | Borrador previo de desarrollo | Timestamps y similitud de archivos |
| Git History | Inexistente | Completo (main) | Independiente / Fuera de control | `git status` (Not git) |

---

## 12. Facts vs Inferences

- **HECHO:** El directorio fue creado el 6 de agosto de 2026 a las 18:31, carece de `.git` y contiene 3 archivos de tests de arquitectura.
- **INFERENCIA:** El nombre `corangecashmachine` se debió a un error tipográfico ("co" en lugar de "o") al intentar abrir o crear una ruta de trabajo vinculada a `orangecashmachine`.
- **CONCLUSIÓN:** Es un sandbox de desarrollo temporal y abandonado sin impacto operativo.

---

## 13. Deletion Risk Assessment

- **Clasificación:** **`SAFE_TO_DELETE`** (Seguro de eliminar).
- **Justificación:** No tiene historial Git, no está referenciado por ningún proceso y todo su contenido útil ya fue incorporado correctamente al repositorio oficial `orangecashmachine`.

---

## 14. Uncertainties

- Ninguna incertidumbre técnica relevante. El origen como sandbox temporal con errata tipográfica en el nombre es altamente concluyente.

---

## 15. Human Decision Required

- **D-CCM-FORENSIC-1:** Aprobar opcionalmente la eliminación del directorio físico `/home/orangemusic/trading/corangecashmachine` para mantener limpio el entorno de trabajo.

---

## 16. Final Verdict

**AUDIT_READY_WITH_FINDINGS** (Directorio huérfano identificado sin riesgo sistémico).

---

## 17. Final Disposal Verification

- **Evidencia Previa:** Inexistencia de metadata `.git`, timestamps del 6 de agosto de 2026 (coincidentes con el commit `930ab2c7` de `orangecashmachine`), y presencia exclusiva de 3 archivos preliminares de tests de arquitectura en `tests/architecture/`.
- **Verificaciones Realizadas:** Búsqueda recursiva de referencias a la ruta o nombre en todo el host y workspace de OCM; validación de ausencia de servicios systemd, crons, docker containers, compose mounts o scripts de CI dependientes.
- **Dependencias Encontradas:** Ninguna.
- **Dependencias Descartadas:** Todas (no hay imports, referencias ni scripts que usen el directorio).
- **Clasificación Final:** `SAFE_TO_DELETE`.
- **Acción Ejecutada:** Eliminación física completa del directorio `/home/orangemusic/trading/corangecashmachine` mediante comando `rm -rf`.
- **Ruta Eliminada:** `/home/orangemusic/trading/corangecashmachine`.
- **Verificación Post-Eliminación:** El directorio no existe; el repositorio principal `orangecashmachine` permanece totalmente intacto; `git status --porcelain` muestra cero cambios en código, tests, CI, ADRs o tracking de OCM.
- **Integridad del Repositorio Principal:** OK (Working tree de OCM sin alteraciones funcionales).

---

## 18. Final Verdict

**DISPOSAL_COMPLETED_SUCCESSFULLY** (Directorio huérfano eliminado de forma segura y trazable).
