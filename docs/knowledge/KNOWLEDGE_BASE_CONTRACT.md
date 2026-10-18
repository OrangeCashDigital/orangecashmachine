# Knowledge Base Contract — OCM

**Estado:** CONTRATO VIGENTE (mecanismo de conocimiento; NO es decisión arquitectónica de dominio)
**Relaciona:** `docs/knowledge/manifest.yaml` (SSOT de metadatos) ↔ corpus físico local ↔ `scripts/knowledge_integrity.py` (validador) ↔ `docs/knowledge/integrity.yaml` (baseline derivado)
**Gobernanza:** Este documento formaliza CÓMO OCM descubre, identifica, verifica y usa conocimiento documental. NO decide ninguna cuestión de dominio (D12, D19, G1, HD-*).

---

## 1. Separación de capas (obligatoria)

| Capa | Ubicación | Rol |
|---|---|---|
| **Catálogo lógico** | `docs/knowledge/` (`manifest.yaml`, `mappings/`, `notes/`) | SSOT de metadatos documentales: identidad lógica (`id`), bibliografía, `status`, `authority`, provenance. Vive en el repo (Git). |
| **Corpus físico local** | `/home/orangemusic/kb-local-only/` | Artefactos físicos (PDFs). NO forma parte del repo. |
| **Integridad** | `docs/knowledge/integrity.yaml` | Índice DERIVADO (no SSOT competitivo): sha256 + size por document_id catalogado. |
| **Validador/Preflight** | `scripts/knowledge_integrity.py` | Mecanismo determinista de verificación. |

**Reglas:**
- **NO copiar, mover, renombrar ni modificar** PDFs/ZIPs del corpus ni del repo como parte de ninguna tarea (copyright; integridad; derechos de distribución).
- El repo almacena **metadata + identifier + source + hash + clasificación + provenance + location contract**, no el contenido de artefactos con copyright.
- `manifest.yaml.path` es un **indicio bibliográfico** (layout aspiracional `books/…`, `papers/…`). El mecanismo de resolución canónico es el del §3 (no depende de que ese layout exista físicamente).

## 2. Single Source of Truth

| Responsabilidad | SSOT |
|---|---|
| Identidad lógica de documento | `manifest.yaml → resources[].id` |
| Metadatos documentales (title/status/authority/provenance) | `manifest.yaml` |
| Identidad física de artefacto | `document_id` = `sha256:<contenido>` (fingerprint; no es registro competitivo) |
| Integridad (esperado vs observado) | `docs/knowledge/integrity.yaml` (derivable desde catálogo+corpus) |
| Clases/contratos de dominio OCM | código + ADRs (fuera de este contrato; ver `docs/architecture/GOVERNANCE.md`) |

No existe (ni se creará) un segundo registro/JSON/DB de conocimiento. `integrity.yaml` es **índice derivado**: regenerable y re-verificable; si diverge del contenido real, el validador lo marca `INTEGRITY_FAILURE`.

## 3. Mecanismo de resolución catálogo ↔ corpus

Determinista y reproducible:

1. Descubrir corpus: todos los archivos regulares bajo la raíz del corpus, **ordenados por relpath**.
2. Indexar por **basename** (nombre de archivo).
3. Para cada entrada del catálogo: `basename = basename(manifest.path)`.
   - **0 coincidencias** → `MISSING` (error, BLOCKED).
   - **1 coincidencia** → resuelto.
   - **>1 coincidencias con contenido distinto** → `AMBIGUOUS` (error, BLOCKED).
   - **>1 coincidencias con contenido idéntico** → resuelto al relpath ordenado + `DUPLICATE_PLACEMENT` (advertencia).
4. Artefacto del corpus no consumido por ningún catálogo → `UNCATALOGUED` (advertencia; **REVIEW_REQUIRED**; nunca se elimina ni se ignora silenciosamente).

**No se inventa identidad semántica** cuando el corpus no la permite: si la resolución es ambigua, el resultado es `AMBIGUOUS` y el preflight es FAIL-CLOSED.

## 4. Document Identity

- **Identidad lógica:** `id` del manifest (asignado por el humano; SSOT del catálogo).
- **Identidad física (document_id):** `sha256:<hash del contenido>`.
  - Determinista; estable para el mismo documento.
  - **NO** depende de: ruta absoluta, orden de descubrimiento, timestamps del filesystem.
  - Detecta sustitución/modificación (el hash cambia si el contenido cambia).
  - **NO** es un segundo SSOT: es fingerprint de verificación; la identidad canónica documental sigue siendo el `id` del manifest.

**Política de casos límite:**

| Caso | Comportamiento |
|---|---|
| Dos archivos con el mismo nombre y contenido distinto | `AMBIGUOUS` (BLOCKED) |
| Dos archivos con el mismo nombre y contenido idéntico | `DUPLICATE_PLACEMENT` (advertencia; mismo `document_id`) |
| El mismo contenido en rutas diferentes | `DUPLICATE_PLACEMENT` (advertencia) |
| El catálogo apunta a un documento inexistente | `MISSING` (BLOCKED) |
| PDF no catalogado | `UNCATALOGUED` / REVIEW_REQUIRED (advertencia explícita) |

## 5. Integridad

- El validador calcula `sha256` + `size` del artefacto resuelto y los compara contra `integrity.yaml`.
- **No hay baseline para un `id`** → `UNVERIFIED` (advertencia; integridad no demostrable → el preflight FAIL-CLOSED para ese documento si es requerido).
- **Hash o size distintos** → `INTEGRITY_FAILURE` (error, BLOCKED).
- Ningún campo de integridad se infiere: si no se puede calcular (archivo ilegible) → `READ_ERROR` (error, BLOCKED).

## 6. Decision Preflight

**Cadena (obligatoria):**

```
Decision
   ↓
Required Knowledge (ids de catálogo requeridos)
   ↓
Document Resolution (§3)
   ↓
Integrity Verification (§5)
   ↓
Evidence Availability
   ↓
Decision Preflight (verdict)
```

**Verdicts:** `PASS` | `PASS_WITH_WARNINGS` | `BLOCKED`.

**Condiciones de error (producen BLOCKED):**
`MISSING`, `INTEGRITY_FAILURE`, `CATALOG_MISMATCH`, `CATALOG_INVALID`, `CORPUS_UNAVAILABLE`, `AMBIGUOUS`, `READ_ERROR`.

**Condiciones de advertencia (PASS_WITH_WARNINGS):**
`UNVERIFIED`, `UNCATALOGUED`, `DUPLICATE_PLACEMENT`, `STATUS_NEEDS_REVIEW`
(status de catálogo no `active` → la metadata no verificada **no debe citarse como hecho**).

**Regla FAIL-CLOSED:** un documento requerido que no existe, no se localiza de forma inequívoca, no se verifica o no está catalogado → el preflight es `BLOCKED`. **No se continúa como si el conocimiento estuviera disponible.** La ausencia de evidencia **no** es evidencia negativa.

**Distinguimos estados de conocimiento:**
- `KNOWLEDGE UNAVAILABLE` (corpus no disponible / archivo ilegible) ≠
- `KNOWLEDGE ABSENT` (artefacto faltante del corpus, catalogado) ≠
- `KNOWLEDGE CONFLICTING` (contenido distinto donde se esperaba uno) ≠
- `KNOWLEDGE UNVERIFIED` (catalogado, resuelto, sin baseline).

**Uso:**
```
uv run python scripts/knowledge_integrity.py                          # check integral
uv run python scripts/knowledge_integrity.py --json                   # salida determinista
uv run python scripts/knowledge_integrity.py --require <id1,id2>      # preflight de decisión
```
Exit codes: `0` = PASS / PASS_WITH_WARNINGS · `1` = BLOCKED · `2` = error de ejecución.

## 7. Jerarquía de autoridad y flujo de decisión

```
Knowledge   →  documento/estándar/paper/bot/model (CONOCIMIENTO, no norma)
Evidence    →  hecho localizable e íntegro que puede apoyar una afirmación
Analysis    →  razonamiento sobre evidencia
Decision Support →  Decision Support Pack (informa; NO decide)
HUMAN DECISION →  el humano decide (única vía a DECIDED)
Decision Record →  registro normativo (ADR/tracking) bajo autorización humana
Implementation →  materializa una decisión registrada (no la redefine)
Verification  →  pruebas/linters/CI confirman la implementación
```

**Invariante:** `Knowledge ≠ Evidence ≠ Analysis ≠ Recommendation ≠ Decision ≠ Implementation`.
Un PDF no decide una arquitectura. Un BOW no decide. Un agente no decide. Una recomendación no equivale a una decisión humana. El Preflight solo determina disponibilidad/integridad de evidencia.

## 8. SafeOps — clasificación de mutaciones

Categorías **independientes** (Git Read-Only ≠ Filesystem Read-Only):

| Categoría | Incluye |
|---|---|
| **GIT_MUTATION** | commit, push, merge, rebase, reset, clean, stash, add, tag, branch-delete |
| **FILESYSTEM_REPOSITORY_MUTATION** | create/modify/delete/rename/move/copy dentro del repo |
| **KNOWLEDGE_CORPUS_MUTATION** | create/modify/delete/rename/move/copy dentro del corpus (`/home/orangemusic/kb-local-only`) |
| **EXTERNAL_ARTIFACT_MUTATION** | mutación sobre cualquier otro path externo al repo y al corpus |

**SAFEOPS STRICT READ-ONLY** = las cuatro categorías PROHIBIDAS, salvo autorización explícita y acotada. El mecanismo `classify_mutation` / `read_only_violation` de `scripts/knowledge_integrity.py` implementa la clasificación y la política.

## 9. Provenance

Para cada verificación, el resultado preserva: `document_id` (fingerprint), `id` de catálogo, título, `status`/`authority`, ruta física relativa del artefacto, hash, size, condición de integridad. Lo que no se puede determinar se declara `UNKNOWN`; **nunca** se rellena por inferencia. Las fechas de descubrimiento NO forman parte de la salida determinista (reproducibilidad).

## 10. Artefactos ZIP / bots / modelos

Los ZIPs bajo `docs/` (p. ej. snapshots de framework/bot) se clasifican **descriptivamente**: `DOCUMENT | REFERENCE | BOT/MODEL ARTIFACT | ARCHIVE | CODE ARTIFACT | UNKNOWN`. Ninguno tiene autoridad arquitectónica automática. Cadenas: `BOT/MODEL OUTPUT → EVIDENCE/ANALYSIS → HUMAN REVIEW → DECISION`.

## 11. Reproducibilidad

Requisitos para que otra máquina reconstruya el mismo resultado:
- repo OCM + corpus en `/home/orangemusic/kb-local-only` (o vía `--corpus`).
- `uv run python scripts/knowledge_integrity.py --json` → salida byte-idéntica
  (sin timestamps; ordenamiento canónico de checks; hashes del contenido).
- Errores mecánicos primero; el LLM solo juzga donde no hay validación mecánica.

## 12. Estado de gobernanza (no modificado por este contrato)

D19-01 = HUMAN DECISION REQUIRED · D19 = PENDING / BLOCKED · GATE C = BLOCKED ·
D19 IMPLEMENTATION = NOT AUTHORIZED · DECISION REGISTERED = NO.

Este mecanismo **no decide**, no registra decisiones y no desbloquea GATE C.
