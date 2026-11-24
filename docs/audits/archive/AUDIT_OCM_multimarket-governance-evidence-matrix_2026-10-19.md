# AUDIT_OCM_MULTIMARKET_GOVERNANCE_EVIDENCE_MATRIX_2026-10-19

**ID:** AUDIT-OCM-MULTIMARKET-EVIDENCE-2026-10-19  
**Tipo:** Matriz de evidencia y contradicciones detallada  
**Pertenece a:** AUDIT_OCM_MULTIMARKET_GOVERNANCE_AUTHORITY_INVESTIGATION_2026-10-19  
**Modo:** READ-ONLY / WRITE-ONLY-IN-DOCS-AUDITS  
**Estado:** EN PROGRESO  
**Fecha:** 2026-10-19  
**HEAD:** df9f0b6c5ac40bcb8130c7f290d68b0f43fe4376  

---

## Propósito
Registro detallado de evidencia (fuente, ubicación, texto relevante, tipo, impacto, procedimiento resolución, decisión humana requerida) para cada hallazgo. No resuelve contradicciones.

---

## 1. Matriz de contradicciones detallada

| Hallazgo ID | Tema | Fuente A | Ubicación A | Fuente B | Ubicación B | Texto/referencia relevante (fiel) | Tipo discrepancia | Estado verificación | Impacto | Procedimiento resolución | Decisión humana requerida | Dependientes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **H-C1** | D19-03..D19-10 + D20-D24 | `ADR-0049-canonical-instrument-identity-g1.md` | L53-54 | `ADR-0049-canonical-instrument-identity-g1.md` | L544-560 | L53-54: *"D19-03..D19-10 permanecen pendientes. NO se anticipa D20–D24."* vs L545-551: *"D19-03..10 = DECIDED (A/B/A/A/D/B/C)..."*, L554-560: *"D20b-01..06 = DECIDED"* | **Interna (mismo doc)** | **VERIFICADO** | Confusión estado; rigor roadmap | HUMAN DECISION (ADR-0045 r9). No resolver por inferencia. | ¿Corregir L53-54 como obsoletas (marcar superseded), añadir reconciliación, o mantener? | DH-2 |
| **H-C2** | Pack D19 estado D19-06..10 | `AUDIT_OCM_d19-human-decision-pack_2026-09-30.md` (blob `4ad03eba`) | L841 | Idem | L2262-2291 | L841: *HUMAN DECISION REQUIRED/BLOCKED* (contexto D19-06..10) vs L2262-2291: tabla ejecutiva *DECIDED* + registration | **Histórica** | **VERIFICADO por inventario** (contenido completo pendiente lectura detallada - V-3) | Divergencia fuentes históricas | Preservar contradicción; decidir reconciliación material | ¿Reconciliar texto pack, mantener histórico, o añadir aclaración? | DH-2 |
| **H-C3** | INDUSTRY/KN_BASE vs ADR-0049 | `AUDIT_OCM_d19-01-industry-bow-review_2026-10-03.md` | L32-35,L167-169 | `ADR-0049` | L542+ | INDUSTRY: *IMPLEMENTATION NOT AUTHORIZED, DECISION REGISTERED NO* / *D19-01 HUMAN DECISION REQUIRED, DECISION NOT YET DECIDED, D19 PENDING/BLOCKED* + `KNOWLEDGE_BASE_CONTRACT.md` L166 | **Jerárquico (parcialmente resuelto)** | **VERIFICADO** | Q1 resuelve jerarquía (ADR-0055 L106-109,L185); **textos contradictorios permanecen** | Reconciliación material entre vigentes/históricos | ¿Actualizar textos históricos con nota referencia a Q1/ADR-0055/ADR-0057? | DH-2 |
| **H-C4** | D12 triple discrepancia | decision-support: `AUDIT_OCM_decision-support-hd-venue-constraints-identity_2026-09-29.md` | L24,L813,L1046 | pack `4ad03eba` | L13,L34 | decision-support: *D12 = OPEN, D19 = PENDING, GATE C = BLOCKED*; pack: *D12: DECIDED — Alternative 5 + Alternative 3*; ADR-0049 L335/361: *G1-D12 = C/RESTRICTED, NO decidir todavía (catálogo exhaustivo pendiente)* | **Triple** | **VERIFICADO** | Impacto rigor contenido | HUMAN DECISION (fuera alcance ADR-0057 C4). No inferir. | ¿Cuál interpretación canónica? Abrir D12 o mantener estado? | DH-2 |
| **H-T1** | PLAN vs AUDIT_PROTOCOL §K | `PLAN-Maestro-Ingenieria.md` | preamble/§14 | `AUDIT_PROTOCOL.md` | §K L88-99 | Orden inviolable + "documento operativo vivo" + §14 historial vs §K: *prohibido modificar... únicamente docs/audits/ en auditoría* | **Tensión alcance (modo vs inmodificabilidad)** | **VERIFICADO** | Procedimiento modificación N1 ambiguo | Diferenciar modo auditoría vs fuera; si persiste → HUMAN DECISION | ¿Aclarar procedimiento modificación PLAN fuera modo auditoría? | DH-1, DH-8 |

---

## 2. Matriz estado canónico detallado

| Elemento | Fuente canónica | Ubicación | Estado (verificado hoy) | Certeza | Notas |
|---|---|---|---|---|---|
| G1-D01-D13 | ADR-0049 | L538-539,L565-570 | DECIDED+MATERIALIZED+ACCEPTED WITH CLARIFICATIONS | HECHO VERIFICADO | — |
| G1-D14 | ADR-0049 | L540,L571-576 | DECIDED+MATERIALIZED+ACCEPTED (A, 2026-09-28) | HECHO VERIFICADO | — |
| G1-D15 | ADR-0049 | L541,L577-586 | DECIDED+MATERIALIZED+ACCEPTED (A, 2026-09-29); NO autoriza implementación | HECHO VERIFICADO | — |
| G1-D04 | `instrument_type.py` | L9-50 | **CERRADO**: SPOT/PERPETUAL/FUTURE/OPTION; prohíbe extensión | HECHO VERIFICADO | Identity-defining |
| G1-D05 | `contract_terms.py`, `listing_identity.py` | contract_terms L16,58,100; listing_identity L27 | Presente como moneda/término identity-defining | HECHO VERIFICADO | 2ª semántica settlement |
| D19-01 | ADR-0049 + ADR-0055 | L542 + L106-109,L185 | DECIDED (C+) | HECHO VERIFICADO | Jerarquía aclarada por Q1 |
| D19-02..10 | ADR-0049 | L543-551 | DECIDED (A/A/B/A/A/D/B/C) | HECHO VERIFICADO | **CONTRADICCIÓN C1** con L53-54 |
| D20/D20a | ADR-0049 | L552-553 | DECIDED | HECHO VERIFICADO | — |
| D20b-01..06 | ADR-0049 | L554-560 | DECIDED | HECHO VERIFICADO | — |
| D21-D24 | ADR-0049 | L561 | BLOCKED | HECHO VERIFICADO | — |
| IMPLEMENTATION | ADR-0049, ADR-0055 | L561,L212 | **NOT AUTHORIZED** | HECHO VERIFICADO | Global |
| D12 | ADR-0057 C4 + ADR-0049 + packs | ADR-0057 L148-155 | **DISCREPANTE** | NO VERIFICADO CANÓNICAMENTE | Fuera alcance ADR-0057 |
| C1-C4 | ADR-0057 | L118-155,L189 | **ABIERTAS** | HECHO VERIFICADO | C4 incluye D12 |
| GATE C / §17.4 | ADR-0057 | L109,L182 | **BLOCKED / fuera alcance** | HECHO VERIFICADO | Requiere autorización humana |
| Q2 | ADR-0055, tracking | L134,L212,L3843 | **NO decidida / UNAUTHORIZED** | HECHO VERIFICADO | Materialización D20b |
| MD-A4 | ADR-0054 | — | **Propuesto** (MD-A2 aceptado) | HECHO VERIFICADO | Precedencia |
| ADR-0045 | — | — | **Propuesto** (reglas r9,r10) | HECHO VERIFICADO | Conflictos/fail-closed |
| ADR-0028 | ADR-0028, tracking | L3,L133,L138 | **ACEPTADO CON REVISIÓN (2026-09-24)**; ND/F6 pendientes | HECHO VERIFICADO | Market data BookBuilder |

---

## 3. Investigación arquitectónica multimercado (evidencia)

| Concepto | Existente | Ubicación | Notas |
|---|---|---|---|
| Trade/Execution Settlement | SÍ | `packages/trading/execution/settlement.py`, `oms.py:537-590`, `order.py:152` | Fees, net_realized_usd, gross_realized_usd, fee_status |
| Contractual Settlement (moneda) | SÍ | `contract_terms.py:16,58,100`, `listing_identity.py:27` | G1-D05 identity-defining (1ª distinción) |
| Market/delivery Settlement (fecha/ciclo) | **NO** | — | FX T+2, equity settlement, delivery futuro — AUSENTE |
| MarketCalendar/TradingCalendar/Holiday/BusinessDay | **NO** | — | grep sin hits relevantes |
| MarketSession/TradingSession/RTH/ETH/pre-post | **NO** | — | grep sin hits relevantes |
| Kafka session_timeout_ms | SÍ (infra) | `packages/market_data/infrastructure/kafka/consumer.py:73,104,138-249` | **NO calendario mercado** |
| `"session is closed"` | SÍ (strings) | `runtime.py:112`, `base.py:105` | operacional |
| Symbol runtime | Aún `symbol: str` | composición por lectura previa | pendiente materialización identidad runtime |

---

## 4. Verificaciones pendientes detalladas

| ID | Verificación | Método (read-only) | Estado | Limitación | Acción próxima |
|---|---|---|---|---|---|
| **V-1** | `scripts/audit_validator.py` (M1-M20, efectos secundarios) | `cat scripts/audit_validator.py` (lectura) | **NO VERIFICADO** | ¿genera artefactos/cachés? No confirmado | Leer completo; si sin efectos secundarios → documentar comandos/resultado |
| **V-2** | GitHub Settings (branch protection/rulesets/required checks) | `gh` CLI | **NO VERIFICADO** | `gh` no disponible localmente → no verificable | Registrar limitación; no instalar |
| **V-3** | Blob `4ad03eba` completo | `git show --no-patch 4ad03eba` + `git show 4ad03eba -- "docs/audits/AUDIT_OCM_d19-human-decision-pack_2026-09-30.md"` selectivo | **PARCIAL** | Solo log conocido; lectura completa no ejecutada | Ejecutar read-only selectivo si posible |
| **V-4** | `.github/workflows/ocm-ci.yml` completo | `cat` | **PARCIAL** | Mencionado (16 jobs); lectura íntegra pendiente | Leer completo; comparar workflow vs required checks |
| **V-5** | §17.4 completo en ADR-0057 + contexto | `sed -n` selectivo | **PARCIAL** | Mencionado L109,L182 | Leer contexto completo |
| **V-6** | Usos exhaustivos `InstrumentType` | `grep -rn "InstrumentType"` acotado | **PARCIAL** | Solo revisión inicial | Mapear usos completos sin modificar |
| **V-7** | Otras ADR instrumentos/identidad/calendario/settlement/BC | `grep -rn` | **PARCIAL** | No exhaustivo | Búsqueda por keywords: calendar, session, settlement, multimercado, forex, equity, broker, bounded context |
| **V-8** | Estado D12 canónico cruzado | lectura cruzada | **CONTRADICTORIO** | C4 mantiene discrepancia | Comparar sistemáticamente; **NO RESOLVER** |

---

## 5. Decisiones humanas pendientes (detalle accionable)

| ID | Pregunta exacta | Alternativas | Consecuencias | Dependencias | Evidencia necesaria |
|---|---|---|---|---|---|
| **DH-1** | ¿Incorporar roadmap multimercado a **Plan Maestro** como sección (fuera modo auditoría), o mantener fuera? ¿ADR previo o directo? | A) Sección N1 B) docs/plans/ C) Solo ADR D) Otro | A) Sigue orden Plan→Tracking→ADR B) Menor impacto normativo C) Menos visible D) Indeterminado | Cadena adopción, AUDIT_PROTOCOL §K, T-PLAN | Decisión registrada explícitamente |
| **DH-2** | ¿Reconciliar **C1-C4/D12**? Si sí, cómo por cada uno? | A) Corregir ADR-0049 L53-54 B) Añadir notas reconciliación C) Dejar histórico preservado D) Caso por caso | A) Elimina C1 B) Preserva historia C) Mantiene ambigüedad D) Complejo pero correcto | ADR-0045 r9/r10 (no inferir), ADR-0057 | Decisión por cada elemento; NO resuelto por agente |
| **DH-3** | ¿Abrir **G1-D04** (extensión InstrumentType) o mantener cerrado a cripto? | A) Mantener cerrado B) Abrir con ADR nuevo C) Extensión gradual D) Modelo separado | A) Respeta estado B) Requiere ADR + cascada D19 C) Requiere criterio D) Evita enum pero añade complejidad | Identidad G1, D19 | Decisión estrategia + impacto identidad |
| **DH-4** | ¿Separar/unificar **trade/contractual/market settlement** (fecha/ciclo)? | A) 3 separados B) Unificar C) 2+1 D) Delegar adaptador | A) Semánticamente correcto B) Riesgo confusión C) Intermedio D) Riesgo fuga semántica | Contratos identidad/trading | Definición semántica precisa |
| **DH-5** | ¿Necesita OCM calendario/sesiones canónico? ¿Dominio/BC? | A) Delegar adaptador B) market_data C) Nuevo BC D) trading E) Solo puertos | A) Mínimo cambio B) Natural para datos C) Justificación fuerte D) Dudoso E) Preparación | Fronteras BC, import-linter | Evidencia necesidad |
| **DH-6** | ¿Autorizar **Q2/materialización D20b-01..06** y avanzar hacia **GATE C**? | A) No por ahora B) Sí con plan C) Parcial D) Tras C1-C4/D12 | A) Respeta estado B) Desbloquea identidad C) Complejo D) Más seguro | ADR-0055 Q2, ADR-0057 | Cierre contradicciones + plan materialización |
| **DH-7** | ¿BC para calendario/settlement multimercado? | A) Extender market_data B) Extender trading C) Nuevo BC D) Solo puertos | A) Mínimo B) Solo trade settlement C) Probablemente requiere ADR D) Temporal | GOVERNANCE §2, import-linter | Justificación responsabilidades/invariantes |
| **DH-8** | ¿Formalizar procedimiento corrección contradicciones/modificación documentos LEVEL 4? | A) Añadir a GOVERNANCE B) Nuevo ADR C) Actualizar AUDIT_PROTOCOL D) Caso a caso | A) Centraliza B) Formal C) Específico modo auditoría D) Ambiguo | AUDIT_PROTOCOL §B,K, ADR-0045 | Aprobar propuesta §10 del principal |

**Recomendación:** tratar **DH-1 + DH-2** como **preguntas independientes** (no interpretar ubicación como aprobación estrategia).

---

## 6. Referencias cruzadas

| Referencia | Relación |
|---|---|
| Informe principal | `AUDIT_OCM_MULTIMARKET_GOVERNANCE_AUTHORITY_INVESTIGATION_2026-10-19.md` |
| ADR-0049 | C1, D19, G1 |
| ADR-0055 | Q1/Q2, jerarquía C3 |
| ADR-0057 | C1-C4, D12, Gate C/§17.4 |
| ADR-0054 | MD-A2/MD-A4 |
| ADR-0045 | r9/r10 (fail-closed) |
| ADR-0028 | market data contratos |
| AUDIT_PROTOCOL §K | T-PLAN |
| PLAN preamble/§14 | T-PLAN |
| GOVERNANCE §2 | cuándo requiere ADR |
| tracking.yaml | referencias cruzadas |

---

## 7. Estado expediente

- **Matriz de evidencia:** **EN PROGRESO** (completa para lo verificado)
- **Verificaciones pendientes:** V-1-V-8
- **Contradicciones abiertas:** C1-C4, T-PLAN (tensión)
- **Decisiones humanas pendientes:** DH-1-DH-8
- **Sin modificaciones normativas/arquitectónicas:** **CONFIRMADO**
- **Solo `docs/audits/`:** **CONFIRMADO**
- **Sin operaciones Git mutativas:** **CONFIRMADO**

---

**Actualización:** 2026-10-19  
**Confirmación:** Archivo creado read-only, trazable, complementario al informe principal. No resuelve contradicciones ni materializa decisiones.

---

## FASE 3 — INVESTIGACIÓN RECONCILIACIÓN FORMAL (2026-10-19)

### C1: contraste L53-54 vs L545-551 (D19-03..10, D20-D24)
**Hallazgo H-C1 confirmado (fase 2):**
- **L53-54:** `"D19-03..D19-10 permanecen pendientes. NO se anticipa D20–D24."` (estado puntual dentro del texto)
- **L545-551:** tabla con `D19-03..10 = DECIDED (A/B/A/A/D/B/C...)`, `D20b-01..06 = DECIDED`; `D21–D24 = BLOCKED`, `IMPLEMENTATION = NOT AUTHORIZED`
- **Contexto ADR-0049:** líneas 53-54 aparecen en sección de reglas/no-inferencia/estado inicial en el cuerpo; líneas 544-562 en bloque "Estado posterior de la serie G1". 
- **Conclusión (HECHO, SIN RESOLVER):** discrepancia interna. No demuestra sustitución formal (no hay texto explícito "se actualizan L53-54"). **No puede resolverse por inferencia** (ADR-0045 r9). **Requiere HUMAN DECISION**: corregir L53-54 como obsoletas/superseded, añadir reconciliación, o mantener ambas y explicitar preeminencia — sin alterar significado.

### C4/D12: triple discrepancia
- **decision-support (blob del backup, decisión-support-hd-venue-constraints-identity):** D12 = OPEN en referencias deliberativas (L24/L813/L1046 según ADR-0057)
- **pack D19 (blob 4ad03eba):** `D12: DECIDED — Alternative 5 + Alternative 3` (L13)
- **ADR-0049 vigente:** `G1-D12 = C/RESTRICTED` (L335) con aclaraciones — catálogo exhaustivo pendiente; líneas 5,13,18,57,335,410,438,602,642 confirman G1-D12 adoptado en deliberaciones G1, pero con restricciones
- **Verificado:** **D12 fuera de alcance decisorio de ADR-0057** (ADR-0057 C4). Pack afirma DECIDED con alternativa; decision-support afirma OPEN. ADR-0049 presenta C/RESTRICTED. **Sin regla formal de precedencia automática**. **HUMAN DECISION requerida**.

### §17.4 (V-5 completado)
- Apariciones: L55, L109, L182. 
- Regulación: formalización §17.3 como ADR nuevo + registro en `tracking.yaml` + contrato/linter para activar GATE C.
- **NO cubre** resolución C1-C4/D12. Requiere autorización humana independiente. GATE C BLOCKED, Q2 UNAUTHORIZED.

### Lacuna de gobernanza (PROPUESTA, NO VIGENTE)
Falta mecanismo explícito para:
1. Corregir discrepancias internas de un ADR (C1) sin crear nuevo ADR
2. Reconciliar discrepancias entre documentos históricos/backup y ADR vigente (C2-C3)
3. Resolver estado contradictorio D12 cuando no cubierto por ADR-0057
4. Determinar precedencia cuando fuentes deliberativas divergen de ADR vigente
Se propone (no materializado): añadir a GOVERNANCE sección "Corrección de contradicciones documentales" — inventariar, HUMAN DECISION, no inferir (ADR-0045 r9), registrar decisión. También aclarar alcance modo auditoría vs fuera (T-PLAN).
