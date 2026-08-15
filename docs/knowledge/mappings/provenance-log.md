# Provenance Log — Knowledge Base Migration

**Fecha de auditoría:** 2026-08-13
**Alcance:** `docs/Libros_Referencia/` → `docs/knowledge/`

## Contexto de aparición del corpus

El corpus completo apareció agrupado en `docs/Libros_Referencia/` sin manifiesto ni
clasificación previa. La procedencia exacta de adquisición de cada archivo (descarga,
compra, fuente) **no fue investigada** salvo cuando el propio filename o metadata
interna del PDF lo revela explícitamente. Esto NO implica que el usuario haya
revisado manualmente cada fuente antes de esta auditoría.

## Hallazgos por archivo

### Advances in Financial Machine Learning — López de Prado.pdf
- **Tamaño:** 768K (anómalo para libro Wiley de 400+ páginas)
- **Evidencia:** watermark interno `Electronic copy available at: https://ssrn.com/abstract=3104847`
- **Conclusión:** CONFIRMADO — es el preprint/paper de SSRN (abstract 3104847), NO el libro
  completo publicado por Wiley.
- **Riesgo registrado:** el libro completo de López de Prado NO está presente en el corpus.
  No debe presentarse como tal.

### Trading and Exchanges — Larry Harris.pdf
- **Evidencia:** portada dice literalmente "Draft Copy", "Draft: March 1, 2002",
  "©2002 Oxford University Press"
- **Conclusión:** CONFIRMADO — es un draft pre-publicación (2002), no necesariamente
  idéntico a la edición final de Oxford University Press (2003).
- **Estado:** `historical` / draft, no `active` como edición definitiva.

### Bill Williams Trading Chaos -castellano.pdf
- **Evidencia parcial:** filename indica "-castellano" (traducción); índice interno en
  español confirma que es una edición/adaptación traducida, no el original en inglés.
- **Pendiente:** identidad del traductor/editorial no confirmada aún — inspección de
  páginas 3-6 en curso.
- **Estado:** `needs_attribution_review`.

### Fundamentals of Data Engineering (Reis, Joe Housley, Matt) (Z-Library).pdf
- **Evidencia:** el filename contiene explícitamente "(Z-Library)".
- **Conclusión:** se conserva la alerta de procedencia. No se afirma legalidad ni
  ilegalidad del archivo.
- **Estado:** `needs_legal_review`.

### Oxford_Said_Bussines_School/ (18 archivos)
- **Evidencia:** todos los archivos siguen el patrón `OXF ALG M{módulo}U{unidad}
  [Lesson|Notes|Video Transcript|Infographic Transcript]`.
- **Conclusión:** CONFIRMADO — material de curso (Oxford Saïd Business School,
  programa de Algorithmic Trading), no bibliografía normativa.
- **Estado:** `course_material`, `authority: TIER_4`.

### market-microstructure-in-practice-second-edition...pdf
- **Evidencia:** generado vía Calibre 3.32.0, `Custom Metadata: no`,
  `CreationDate` coincide con fecha de la sesión de auditoría (no con publicación real).
- **Conclusión:** metadata de título/autor inyectada por Calibre; tratar como
  NO verificada hasta inspección de contenido/portada.
- **Estado:** `needs_verification`.

## Archivos pendientes de inspección directa

- Data Quality Engineering in Financial Services.pdf — pdfinfo pendiente
- Financial Data Engineering — Tamer Khraisha.pdf — pdfinfo pendiente
- Bill Williams (atribución/traductor) — pdftotext páginas 3-6 pendiente

## Actualización — inspección adicional

### Bill Williams Trading Chaos -castellano.pdf
- Búsqueda de "traduc|copyright|©|isbn|derechos|editorial" en el texto completo
  (7231 líneas) NO arrojó información de traductor, editorial ni ISBN.
- **Conclusión:** atribución sigue sin poder verificarse. Se mantiene
  `needs_attribution_review`. No se inventa editorial ni traductor.

### Data Quality Engineering in Financial Services.pdf
- pdfinfo confirma: Title="Data Quality Engineering in Financial Services",
  Author="Buzzelli, Brian;" (punto y coma final sugiere lista truncada),
  generado vía Antenna House PDF Output Library (pipeline editorial real).
- **Conclusión:** identidad del libro confirmada. Lista completa de autores
  queda `needs_verification`.

## Pendiente

- pdfinfo de "Advances in Financial Machine Learning — López de Prado.pdf" — no obtenido aún
- pdfinfo de "Fundamentals of Data Engineering (Z-Library).pdf" — no obtenido aún
