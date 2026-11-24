# OCM — PAQUETE DE DECISIONES HUMANAS MULTIMERCADO

**Fecha:** 2026-10-19  
**Expediente base:** AUDIT_OCM_MULTIMARKET_GOVERNANCE_AUTHORITY_INVESTIGATION_2026-10-19.md  
**Matriz de evidencia:** AUDIT_OCM_MULTIMARKET_GOVERNANCE_EVIDENCE_MATRIX_2026-10-19.md  
**Ámbito:** Solo lectura/exclusivamente en docs/audits/  
**Límites:** No resuelve decisiones. No modifica normativos. No materializa arquitectura.

## 1. RESUMEN EJECUTIVO

Este paquete prepara DH-1, DH-2 y DH-3 con evidencias, alternativas, consecuencias, fuentes afectadas y autorizaciones necesarias. Las contradicciones C1–C4/D12 permanecen sin resolver por inferencia (ADR-0045 r9).

## 2. DH-1 — UBICACIÓN DOCUMENTAL DEL ROADMAP MULTIMERCADO (PREGUNTAS SEPARADAS)

**Principio:** **Ubicación (DH-1) ≠ aprobación (DH-3).** Investigar ubicación documental NO implica aprobar estrategia, diseño arquitectónico ni autorizar implementación.

### Preguntas independientes (NO combinar)

#### DH-1A: Salir del modo auditoría
**Pregunta exacta:** ¿Se autoriza salir del modo de auditoría para tramitar un cambio normativo relacionado con la ubicación del roadmap multimercado?

**Alternativas:**
- **A)** Sí — autorizar salida del modo auditoría (solo para tramitar; NO implica aprobar contenido)
- **B)** No — mantener en docs/audits/ hasta tener otra vía autorizada
- **C)** Otro (especificar)

**Evidencia:** `AUDIT_PROTOCOL.md §K` (escritura solo `docs/audits/` durante auditoría).  
**Consecuencias:** A permite tramitar cambios fuera de auditoría; B mantiene restricciones.  
**Nota:** Autorizar salida **NO demuestra** que exista un procedimiento formal de modificación de N1.  
**Autorización:** Decisión humana explícita.

#### DH-1B: Procedimiento formal para modificar N1
**Pregunta exacta:** ¿Existe un procedimiento formal verificable para modificar el Plan Maestro N1 en este caso? Si existe, ¿quién es la autoridad competente?

**Alternativas:**
- **A)** Sí — existe procedimiento verificable (indicar documento/sección/autoridad)
- **B)** No — no se ha localizado procedimiento verificable en el repositorio
- **C)** Parcial — existe práctica pero no procedimiento normativo demostrable
- **D)** Necesita aclaración antes de decidir

**Evidencia:** `docs/PLAN-Maestro-Ingenieria.md` N1, `docs/architecture/GOVERNANCE.md` §2 (4 casos ADR), `docs/governance/AUDIT_PROTOCOL.md`. **No se ha localizado** procedimiento explícito de modificación de N1 en la inspección realizada.  
**Consecuencias:** Si B/C/D → no se puede asumir que alternativa B de ubicación sea factible sin aclaración.  
**Autorización:** Requiere verificación/aclaración humana.  
**Nota:** Práctica observada ≠ procedimiento normativo demostrable.

#### DH-1C: Ubicación documental
**Pregunta exacta:** ¿Qué ubicación documental se propone para el roadmap multimercado (si se autoriza)?

**Alternativas:**
- **A)** Mantener solo en `docs/audits/` (exploratorio, no normativo)
- **B)** Preparar propuesta para N1 §14 en `docs/PLAN-Maestro-Ingenieria.md` (pendiente DH-1A + DH-1B)
- **C)** Usar `docs/plans/` u otro documento no normativo (verificar compatibilidad)
- **D)** Otro (especificar con justificación)

**Evidencia:** `PLAN-Maestro-Ingenieria.md` N1 §14; `AUDIT_PROTOCOL §K`; estructura actual.  
**Dependencias:** Requiere DH-1A y DH-1B si se elige **B**.  
**Autorización:** Depende de A/B.

#### DH-1D: Alcance — ubicación vs estrategia
**Pregunta exacta:** ¿Se aprueba ÚNICAMENTE la ubicación documental, o se aprueba también alguna estrategia multimercado?

**Alternativas:**
- **A)** ÚNICAMENTE ubicación documental (separado de DH-3)
- **B)** Ubicación + estrategia (requiere decisión DH-3 explícita por separado)
- **C)** Ni ubicación ni estrategia ahora
- **D)** Otro

**Principio:** **La ubicación NO debe implicar aprobación estratégica.** Si se elige B, debe tramitarse como **DH-3** por separado con sus propias preguntas.  
**Autorización:** Decisión humana explícita.

## 3. DH-2 — RECONCILIACIÓN FORMAL C1–C4/D12 (PREGUNTAS SEPARADAS)

**Principio:** No resolver por inferencia (ADR-0045 r9). No convertir deliberativa en formalmente adoptada, histórico en vigente, decidida en autorizada.

### Vía 1 vs Vía 2 (separadas)

#### VÍA-2A: Resolver individualmente (caso por caso)
**Descripción:** Tomar decisiones humanas documentadas para C1, C2-C3 y C4/D12 por separado.  
**Ventajas:** Focalizado, mínimo impacto, respeta trazabilidad.  
**Riesgos:** Puede dejar incoherencias si no se documenta relación entre decisiones.  
**Dependencias:** Decisiones humanas explícitas.  
**Autorización:** Por cada sub-decisión.

#### VÍA-2B: Procedimiento general primero
**Descripción:** Proponer y tramitar primero el procedimiento "Corrección de contradicciones documentales" (NO VIGENTE) antes de resolver C1-C4/D12.  
**Ventajas:** Sistemático, prevence inferencia, aplicable futuro.  
**Riesgos:** Más tiempo, requiere autorización para introducir procedimiento, puede retrasar decisiones urgentes.  
**Dependencias:** Decisión de adoptar procedimiento + autoridad competente.  
**Autorización:** Requiere aprobación explícita para proponer/adoptar procedimiento (fuera modo resolución por inferencia).  
**Nota:** **NO presentar como vigente.** Requiere decisión humana separada.

**Pregunta:** ¿VÍA-2A o VÍA-2B? (pueden combinarse parcialmente con justificación)

---

### DH-2A: C1 — Contradicción interna ADR-0049 (L53-54 vs L544-560)
**Pregunta exacta:** ¿Cómo tratar la contradicción interna entre `ADR-0049 L53-54` y `ADR-0049 L544-560`?

**Evidencia:**
- `docs/architecture/decisions/ADR-0049-canonical-instrument-identity-g1.md:53-54` vs `:544-560` (unificado)
- Sin texto explícito de sustitución/supersession
- Ambos conviven en mismo ADR

**Alternativas:**
- **A)** Marcar uno como superseded **SOLO SI** autoridad competente determina explícitamente cuál prevalece Y existe mecanismo para hacerlo
- **B)** Añadir nota de reconciliación preservando ambos pasajes y registrando decisión formal
- **C)** Añadir regla de interpretación explícita aprobada por autoridad competente (mantener ambos)
- **D)** Dejar inventariado con aclaración (FAIL-CLOSED) sin modificar ADR ahora
- **E)** Otro (especificar)

**Consecuencias:** A elimina ambigüedad pero requiere autoridad + mecanismo; B preserva trazabilidad; C/D controlan ambigüedad.  
**Autoridad competente:** A determinar por decisión humana (no inferida).  
**Fuentes a actualizar si procede:** `ADR-0049` (solo si A/B/C); `tracking.yaml` contexto si aplica.  
**Laguna:** **Falta mecanismo conocido** para marcar texto obsoleto/superseded sin rehacer ADR completo — esto es **laguna no resuelta**, no prueba de inexistencia absoluta.

### DH-2B: C2–C3 — Estados históricos D19 vs fuentes actuales
**Pregunta exacta:** ¿Cómo tratar los estados contradictorios de D19 (pack/Industry BOW vs ADR-0049 vigente)? ¿Preservar como histórico, reconciliar materialmente o dejar inventariado?

**Evidencia:**
- Blob `4ad03eba:AUDIT_OCM_d19-human-decision-pack_2026-09-30.md:L14,L2262-2291` (DECIDED + IMPLEMENTATION NOT AUTHORIZED; también referencias históricas)
- Blob `4ad03eba:AUDIT_OCM_d19-01-industry-bow-review_2026-10-03.md:L32,L33,L35` (PENDING/BLOCKED, HUMAN DECISION REQUIRED, DECISION REGISTERED NO)
- `ADR-0049:542-560` (D19-01..10 DECIDED, D20b-01..06 DECIDED, IMPLEMENTATION NOT AUTHORIZED)
- `ADR-0055` Q1 (jerarquía parcial) — **NO reconcilia textos**

**Alternativas:**
- **A)** Preservar textos históricos con notas de alcance (deliberativo/registro histórico) sin modificarlos
- **B)** Reconciliar materialmente textos para reflejar estado vigente con trazabilidad completa
- **C)** Dejar inventariado, aclarar estado vigente en fuentes normativas sin eliminar histórico
- **D)** Otro (especificar)

**Consecuencias:** A mínimo cambio; B mayor claridad; riesgo alterar registro histórico incorrectamente.  
**Autoridad:** Decisión humana explícita.  
**Fuentes a actualizar si procede:** `ADR-0049` (contexto/notas), `tracking.yaml`, auditorías  
**Regla:** **NO** asumir sustitución formal sin evidencia explícita.

### DH-2C: C4/D12 — Estados contradictorios
**Pregunta exacta:** ¿Cómo resolver formalmente la contradicción de D12 (decision-support OPEN / pack DECIDED Alt5+Alt3 / ADR-0049 C/RESTRICTED)?

**Evidencia:**
- Blob decision-support: `D12 = OPEN` (referenciado ADR-0057 C4)
- Blob pack: `D12: DECIDED — Alternative 5 + Alternative 3` (L13)
- `ADR-0049:335` (`G1-D12 = C/RESTRICTED`, catálogo exhaustivo pendiente)
- `ADR-0057` C4: **D12 fuera del alcance decisorio de ADR-0057** — **NO resuelve contradicción**
- `docs/plans/tracking.yaml`: inventariado

**Alternativas:**
- **A)** Determinar precedencia con regla explícita aprobada (identificar autoridad)
- **B)** Tratar D12 como decidido pero restringido/condicionado (registrar restricciones)
- **C)** Preservar con aclaración controlada (FAIL-CLOSED) e inventariar
- **D)** Otro (especificar)

**Consecuencias:** C respeta `ADR-0045 r9/r10` (FAIL-CLOSED); A requiere autoridad + justificación.  
**Autoridad:** Decisión humana explícita.  
**Fuentes a actualizar si procede:** `ADR-0049` (aclaraciones), `ADR-0057` contexto, `tracking.yaml`  
**Nota:** Exclusión de alcance **NO equivale** a resolución.

## 4. DH-3 — ESTRATEGIA MULTIMERCADO (SEPARADA DE UBICACIÓN)

**Principio fundamental:** **Ubicación (DH-1) ≠ Aprobación (DH-3).**  
Distinguir claramente:
- **Explorar mercados** — investigación
- **Aprobar una estrategia** — decisión estratégica humana
- **Aprobar un diseño arquitectónico** — requiere procedimiento (posiblemente ADR según GOVERNANCE §2)
- **Autorizar implementación** — requiere autorización explícita (actualmente NO para D19/D20b)

**Pregunta exacta:** ¿Debe OCM adoptar una estrategia de expansión a mercados no cripto? Si sí, ¿con qué alcance y restricciones iniciales?

**Restricciones vigentes (OBLIGATORIO CONSIDERAR):**
- **G1-D04 / InstrumentType:** Cerrado a `SPOT|PERPETUAL|FUTURE|OPTION` (`packages/market_data/domain/value_objects/instrument_type.py`). Extensión es identity-defining.
- **IMPLEMENTATION NOT AUTHORIZED:** D19/D20/D20a/D20b con `IMPLEMENTATION = NOT AUTHORIZED` (`ADR-0049:560`, `tracking.yaml`)
- **Gate C:** **BLOCKED** (`ADR-0057 §17.4`, `tracking.yaml`)
- **Q2 (ADR-0055):** **UNAUTHORIZED** / no decidida para materialización
- **Contradicciones C1-C4/D12:** Sin resolver — afectan claridad sobre identidad/taxonomía

**Alternativas (SIN SELECCIONAR):**
- **A)** Crypto-first estricto (no autorizar extensión ahora) — mantener restricciones vigentes
- **B)** Brokers-later exploratorio (solo investigación, sin aprobar estrategia ni implementación)
- **C)** Hybrid controlado con gates explícitos (requiere resolver C1-C4/D12 y posibles decisiones arquitectónicas)
- **D)** Otro (especificar con justificación)

**Consecuencias:** Dependen de DH-2 (reconciliación). La ubicación DH-1 **no** es prerrequisito.  
**Autorización:** Decisión estratégica humana. Puede requerir ADR si implica cambios normativos/arquitectónicos (GOVERNANCE §2).  
**Nota:** No se decide aquí si expandirse. Solo se prepara el marco de decisión.

## 5. PROCEDIMIENTO PROPUESTO (NO VIGENTE) — “CORRECCIÓN DE CONTRADICCIONES DOCUMENTALES”
**Objetivo:** cubrir lagunas sin permitir inferencia.

**Elementos:**
1. Inventario fuentes con ubicaciones exactas
2. Autoridad competente identificada
3. Estado cada fuente (deliberativa/adoptada formalmente/registro histórico/vigente)
4. Decisión humana registrada y trazable
5. Tratamiento textos históricos (preservar/marcar superseded con nota)
6. Actualización coordinada fuentes normativas afectadas
7. Verificación post-corrección consistencia
8. Autorización implementación si procede
9. Prevención inferencia (recordar ADR-0045 r9)

**Ámbito afectable:** GOVERNANCE (aclaración), AUDIT_PROTOCOL (aclaración), ADRs afectados, tracking.yaml. **Requiere aprobación humana explícita.**

## 6. PREGUNTAS DE DECISIÓN HUMANAS RESUMIDAS
1. **DH-1:** Ubicación roadmap (A–D)? Justificar. ¿Implica salir modo auditoría?
2. **DH-2:** Tratamiento C1 (A–D), C2–C3 (A–D), C4/D12 (A–D)? ¿Usar procedimiento propuesto?
3. **DH-3:** ¿Adoptar estrategia multimercado? Alcance/restricciones iniciales (A–D)?

## 7. REFERENCIAS
- ADR-0045 r9/r10, ADR-0049, ADR-0055 Q1, ADR-0057 §17.4, ADR-0054 MD-A2
- AUDIT_PROTOCOL §B,K, GOVERNANCE §2
- PLAN-Maestro-Ingenieria N1 §14, tracking.yaml
- Blobs 4ad03eba (packs/decision-support/industry-BOW)
