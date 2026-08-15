# Auditoría de la Knowledge Base de OCM — 2026-08-13

**Fase:** READ ONLY / AUDIT — sin ejecución. Ningún archivo ha sido movido, renombrado ni eliminado.
**Metodología:** cada afirmación está marcada `[VERIFICADO]` (confirmado directamente contra el repo/filesystem en esta sesión), `[HEREDADO-SIN-VERIFICAR]` (proviene de un informe externo previo y no se ha confirmado independientemente) o `[CORREGIDO]` (el informe externo lo afirmaba y resultó ser incorrecto o impreciso).

---

## A. Estado actual `[VERIFICADO]`

\```
docs/
├── architecture/
│   ├── 0000-principios-arquitectonicos.md
│   ├── 0001-bounded-contexts-composition-root.md
│   ├── 0002-event-driven-kappa-architecture.md
│   ├── BLINDAJE-APPS-2026-08-06.md
│   ├── decisions/            (20 archivos: ADR-0003 a ADR-0024, sin 0018/0019/0021 + ADR-template.md)
│   ├── feed-model.md
│   ├── GOVERNANCE.md
│   ├── logs/
│   ├── README.md             (sección "## Referencias" = cross-refs a ADRs, NO bibliografía externa)
│   ├── recovered/
│   └── SUPERSEDED-000{3,4,5}-*.md
├── audits/                   (8 archivos, incluye 2026-08-13-auditoria-arquitectonica.md)
├── DOMAIN.md
├── Libros_Referencia/        ← objeto de esta auditoría, 45M, TODO sin trackear en git (`??`)
│   ├── Advances in Financial Machine Learning — López de Prado.pdf   (782K, 61 págs)
│   ├── Bill Williams Trading Chaos -castellano.pdf                    (6.6M, 219 págs)
│   ├── Data Quality Engineering in Financial Services.pdf             (7.5M, 177 págs)
│   ├── Financial Data Engineering — Tamer Khraisha.pdf                (3.1M, 144 págs)
│   ├── Fundamentals of Data Engineering (...) (Z-Library).pdf         (8.5M, 445 págs)
│   ├── market-microstructure-in-practice-...pdf                       (9.0M, 339 págs)
│   ├── Oxford_Said_Bussines_School/            (17 PDFs: lecciones, notas, transcripciones)
│   └── Trading and Exchanges — Larry Harris.pdf                       (608K, 113 págs)
├── PLAN-Maestro-Ingenieria.md    ← suelto en raíz de docs/, no en plans/ ni planning/
├── planning/                     ← f2_6a-capacity-teorico.md, fase3.5c-capacity-empirico.md
└── plans/                        ← backlog-priorizado-*.md, engineering-guardrails.md, tracking.yaml
\```

Contexto para agentes en raíz del repo: existe `AGENTS.md` (7.6K) y `README.md` (19K). **No existen** `CLAUDE.md` ni `CONTRIBUTING.md`.

No hay ningún índice/manifiesto de conocimiento preexistente. `docs/plans/tracking.yaml` se investigó como candidato y se descartó: sus menciones a "libros" son jerga de order books de microestructura, no bibliografía.

### A.1 Procedencia — hallazgo relevante `[VERIFICADO]`

Todo el contenido de `Libros_Referencia/` (los 7 PDFs + los 17 de Oxford) tiene fecha de modificación del **mismo día, 2026-08-13, entre 19:57 y 21:03** (~66 min), coincidiendo con una sesión SSH activa 20:03–21:11. Existen snapshots de **OpenCode** en `~/.local/share/opencode/snapshot/` de ese entorno. La carpeta `Libros_Referencia/` en sí (el inodo) fue creada a las 21:03, la misma hora que el último archivo.

Conclusión razonable: una sesión de agente (OpenCode, con acceso a red/herramientas en la máquina local, a diferencia de este entorno) generó/descargó todo el corpus en una sola sesión esta noche. **La procedencia exacta (URLs, prompt usado) no se investigó** — decisión explícita del usuario, riesgo queda abierto (ver sección J).

---

## B. Problemas detectados

| # | Problema | Estado | Severidad |
|---|---|---|---|
| 1 | Carpeta en español (`Libros_Referencia`), inconsistente con resto del repo en inglés | `[HEREDADO]` razonable | Media |
| 2 | Filenames inconsistentes (guiones, paréntesis, sin año) | `[HEREDADO]` razonable | Alta |
| 3 | Procedencia Z-Library en filename de Reis/Housley | `[VERIFICADO]` (confirmado en el nombre) | Crítica (legal) |
| 4 | **El PDF "López de Prado" NO es el libro** — es el paper corto de SSRN ("SSRN_AFML.pdf", autor "mldp", 61 págs, Adobe Distiller 2018) | `[CORREGIDO]` — el informe externo lo catalogó como el libro completo (445 págs esperadas) con ISBN de Wiley | Alta — metadata falsa si se publica tal cual |
| 5 | **El PDF "Bill Williams Trading Chaos -castellano" no es el original** — metadata interna dice título "Como entender el trading y vivir de él", autor "Darío Redes" (2011). El índice temático (fractales, teoría del caos) sí coincide con *Trading Chaos*, consistente con una traducción/adaptación no oficial al español | `[CORREGIDO]` — el informe externo le asignó ISBN y año de la edición original en inglés sin verificar | Alta — atribución de autoría incorrecta si no se corrige |
| 6 | Harris es un **"Draft Copy" de marzo 2002**, pre-publicación ("Forthcoming Fall 2002"), no necesariamente idéntico a la edición final de Oxford UP 2003 | `[VERIFICADO]`, nuevo hallazgo no detectado por el informe externo | Media |
| 7 | Khraisha y Lehalle/Laruelle generados **hoy mismo vía Calibre** (herramienta no instalada actualmente en la máquina) | `[VERIFICADO]`, nuevo hallazgo, ligado a A.1 | Media-Alta (procedencia) |
| 8 | Oxford: typo "Bussines", es material de curso (lecciones/notas/transcripciones de video), no un libro | `[VERIFICADO]` | Media |
| 9 | Sin índice central (`index.yaml`/`manifest.yaml`/`README.md` dentro de la carpeta) | `[VERIFICADO]` | Alta |
| 10 | Todo el conocimiento en PDFs binarios, sin notas extraídas | `[VERIFICADO]` | Media |
| 11 | Sin mapeo a bounded contexts | `[VERIFICADO]` | Alta |
| 12 | Sin metadatos estructurados | `[VERIFICADO]` | Alta |
| 13 | `docs/planning/` vs `docs/plans/` — nombres casi duplicados, no detectado por el informe externo | `[VERIFICADO]`, fuera de scope estricto de KB pero anotado | Baja-Media |
| 14 | `docs/PLAN-Maestro-Ingenieria.md` suelto en raíz de `docs/` en vez de estar en `plans/` | `[VERIFICADO]` | Baja |

---

## C. Arquitectura propuesta (misma dirección que el informe externo, ajustada)

\```
docs/
└── knowledge/
    ├── README.md                # guía humanos + agentes
    ├── manifest.yaml             # índice maestro de metadatos
    ├── books/
    │   ├── data-engineering/     # Reis, Khraisha
    │   ├── financial-data-quality/  # Buzzelli
    │   ├── market-microstructure/   # Harris (draft), Lehalle/Laruelle
    │   └── historical/           # "Trading Chaos" (adaptación ES), marcado no-normativo
    ├── papers/
    │   └── lopez-de-prado-afml-ssrn.pdf   # reclasificado como paper, no libro
    ├── courses/
    │   └── oxford-said-algo-trading/      # typo corregido, 17 archivos
    ├── notes/                    # resúmenes extraídos, no copias de los libros
    └── mappings/
        ├── ocm-domains.yaml
        └── provenance-log.md     # NUEVO: registra el hallazgo de A.1 explícitamente
\```

Diferencia clave frente al informe externo: se separa `books/` de `papers/` (López de Prado no es un libro), y se añade `provenance-log.md` para no perder el rastro de que este corpus llegó vía agente sin curación manual — información que un futuro agente necesita para no tratar el corpus como "cuidadosamente seleccionado por el usuario".

---

## D. Metadata schema — con datos verificados

Solo los campos confirmados vía `pdfinfo`/`pdftotext`. Autor/año se dejan `null` donde no hay evidencia directa (Harris, "Bill Williams").

\```yaml
entries:
  - id: reis_housley_fundamentals_data_engineering
    title: "Fundamentals of Data Engineering"
    authors: ["Joe Reis", "Matt Housley"]
    publication_year: 2022        # [VERIFICADO] CreationDate PDF
    type: book
    pages: 445
    provenance: "filename indica origen Z-Library — riesgo legal, ver J"
    ocm_authority: PRIMARY_TECHNICAL_REFERENCE   # [HEREDADO, razonable]
    status: needs_legal_review

  - id: khraisha_financial_data_engineering
    title: "Financial Data Engineering"
    authors: ["Tamer Khraisha"]
    publication_year: null        # [PENDIENTE] título interno trae artefacto "(for . .)"
    type: book
    pages: 144
    provenance: "generado vía Calibre 2026-08-13 20:22, misma sesión que el resto del corpus — ver A.1"
    status: needs_provenance_review

  - id: lehalle_laruelle_market_microstructure_practice
    title: "Market Microstructure In Practice (Second Edition)"
    authors: ["Charles-Albert Lehalle", "Sophie Laruelle"]
    publication_year: null        # [PENDIENTE] no confirmado en metadata, ISBN en filename sin verificar contra portada
    type: book
    pages: 339
    provenance: "creado 2026-08-13 21:03, misma sesión — ver A.1"
    status: needs_provenance_review

  - id: buzzelli_data_quality_engineering_financial_services
    title: "Data Quality Engineering in Financial Services"
    authors: ["Brian Buzzelli"]
    publication_year: 2022        # [VERIFICADO]
    type: book
    pages: 177
    ocm_authority: PRIMARY_TECHNICAL_REFERENCE
    status: active

  - id: harris_trading_and_exchanges_draft
    title: "Trading and Exchanges: Market Microstructure for Practitioners (Draft Copy)"
    authors: ["Larry Harris"]
    publication_year: 2002        # [VERIFICADO] "Draft: March 1, 2002"; edición final 2003 NO confirmada como idéntica
    type: book
    pages: 113
    notes: "Es un borrador pre-publicación, no la edición final de Oxford UP 2003. No asumir paginación/contenido idéntico al citar."
    ocm_authority: SECONDARY_REFERENCE
    status: active

  - id: lopez_de_prado_afml_ssrn_paper
    title: "Advances in Financial Machine Learning (SSRN preprint)"
    authors: ["Marcos López de Prado"]
    publication_year: 2018        # [VERIFICADO]
    type: paper                   # [CORREGIDO] — no es el libro de Wiley
    pages: 61
    notes: "Este archivo es el paper corto de SSRN, NO el libro completo (~400 págs). Si se quiere el libro, hay que adquirirlo aparte."
    ocm_authority: PRIMARY_TECHNICAL_REFERENCE
    status: active

  - id: trading_chaos_adaptacion_es
    title: "Trading Chaos (adaptación/traducción al español, atribución incierta)"
    authors: ["Bill Williams (obra original)", "Darío Redes (adaptación, según metadata del PDF)"]
    publication_year: null        # [PENDIENTE] metadata interna dice 2011, original es de 2000 — no coinciden, no asumir
    type: book
    pages: 219
    notes: "Metadata interna del PDF no coincide con el filename. Verificar edición/traductor antes de citar formalmente."
    ocm_authority: HISTORICAL_EXPLORATORY
    status: needs_attribution_review

  - id: oxford_said_algo_trading_course
    title: "Oxford Saïd Business School — Algorithmic Trading (materiales de curso)"
    authors: ["Oxford Saïd Business School"]
    type: course_material
    files: 17
    ocm_authority: CONTEXTUAL_REFERENCE
    status: active
\```

---

## E. Agent discovery

Igual que lo propuesto en el informe externo (punto de entrada obligatorio en `manifest.yaml`, filtrado por `ocm_domains`, jerarquía de autoridad, citas con id), con un añadido:

- Antes de citar `khraisha_financial_data_engineering` o `lehalle_laruelle_market_microstructure_practice`, el agente debe advertir que el año de publicación no está confirmado (`publication_year: null`) y no debe inventarlo.
- Un agente no debe tratar el corpus completo como "seleccionado y revisado por el usuario" — debe saber que llegó vía una sesión de agente sin curación manual (ver `mappings/provenance-log.md`).

## F. Modelo de autoridad

Se mantiene el esquema Tier 0–4 propuesto en el informe externo (Código/ADRs > Primary > Secondary > Historical > Contextual/Academic), sin cambios sustanciales — es razonable y no depende de los datos corregidos en este informe.

## G. Mapping a bounded contexts

La matriz PRIMARY/SECONDARY/CONTEXTUAL/NONE del informe externo (sección G del documento 7) es plausible en su forma, pero **no fue verificada línea por línea en esta sesión** — se basa en los temas generales de cada libro, no en lectura de contenido. Se hereda como punto de partida, marcada `[HEREDADO-SIN-VERIFICAR]`, para revisión humana antes de adoptarla como definitiva.

## H. Gaps de conocimiento

Se mantienen los gaps identificados por el informe externo (portfolio/risk, on-chain data, streaming en profundidad, data lineage) — son coherentes con el propósito de OCM y no dependen de los datos corregidos aquí.

## I. Plan de migración (propuesta, NO ejecutar sin aprobación)

1. Crear `docs/knowledge/{books,papers,courses,notes,mappings}/` con subcarpetas por tema (no por tier — la clasificación de tier vive en el manifest, no en el path, para evitar mover archivos si cambia la clasificación).
2. Mover archivos con nombres corregidos (ver sección D para los ids), **sin renombrar el contenido interno de los PDFs**.
3. Crear `manifest.yaml` con el schema de la sección D, marcando explícitamente `status: needs_provenance_review` / `needs_attribution_review` donde aplique — no dejarlo como "active" sin más.
4. Crear `mappings/provenance-log.md` documentando el hallazgo A.1 (corpus generado por agente el 2026-08-13, procedencia exacta no investigada por decisión del usuario).
5. Actualizar `AGENTS.md` con sección "Knowledge Base" y las reglas de la sección E.
6. Buscar y actualizar referencias a `docs/Libros_Referencia/` en `AGENTS.md`, `README.md`, `PLAN-Maestro-Ingenieria.md`, `DOMAIN.md`.
7. Eliminar `docs/Libros_Referencia/` vacía tras confirmar la migración.

## J. Riesgos

| Riesgo | Nivel | Nota |
|---|---|---|
| Procedencia del corpus sin investigar (A.1) | Alto, abierto | Decisión explícita del usuario de no investigar logs de OpenCode. Documentado, no resuelto. |
| Z-Library en Reis/Housley | Crítico (legal) | `[VERIFICADO]` en filename |
| "López de Prado" catalogado como libro cuando es un paper de 61 págs | Alto si se publica sin corregir | `[CORREGIDO]` en este informe |
| Atribución incierta en "Bill Williams" (metadata interna no coincide) | Medio-Alto | `[CORREGIDO]` en este informe |
| Harris es un draft de 2002, no la edición final citable con el ISBN de 2003 | Medio | `[VERIFICADO]`, nuevo |
| PDFs binarios en git sin LFS, 45M total | Medio | Sin cambios respecto al informe externo |
| Metadata de Khraisha/Lehalle incompleta (año null) | Medio | No inventar para rellenar el manifest |

## K. Recomendación final

La estructura y el modelo de autoridad de tiers del informe externo (secciones C, E, F) son razonables y se pueden adoptar como base. **Pero el manifest.yaml no debe copiarse tal cual del informe externo**: contiene al menos dos errores de clasificación de tipo/autoría (López de Prado, "Bill Williams") y varios datos de edición/ISBN inventados sin verificación contra los PDFs reales. Antes de ejecutar la migración, recomiendo:

1. Adoptar el schema de la sección D de este informe (con los `status: needs_*_review` explícitos) en vez del de la sección D del informe externo.
2. Decidir explícitamente qué hacer con la pregunta de procedencia abierta en A.1 antes de considerar este corpus "normativo" para agentes — aunque no se investigue ahora, no debería tratarse como fuente confiable sin reservas mientras el origen no esté claro.
3. Ejecutar la migración solo tras tu aprobación explícita de este documento (regla del spec original — seguimos en fase STOP).

---

*Generado en sesión de auditoría 2026-08-13, con verificación directa vía `pdfinfo`/`pdftotext`/`stat`/`find` sobre el repositorio real, corrigiendo datos del informe de knowledge base recibido previamente donde se detectaron discrepancias.*
