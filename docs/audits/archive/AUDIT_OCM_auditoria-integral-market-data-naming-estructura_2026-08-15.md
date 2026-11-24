# PROMPT MAESTRO — AUDITORÍA ARQUITECTÓNICA INTEGRAL OCM

## 0. OBJETIVO

Realiza una auditoría arquitectónica integral, estrictamente de lectura, del repositorio OCM (OrangeCashMachine).

El objetivo es obtener una fotografía rigurosa y verificable de:

1. La arquitectura REAL actualmente implementada de Market Data.
2. La organización REAL del repositorio.
3. La consistencia de carpetas, módulos y naming.
4. Los ports, adapters, contracts e interfaces existentes.
5. La distribución real de responsabilidades entre dominio, aplicación e infraestructura.
6. Las fuentes de verdad existentes para estado, datos y patrimonio.
7. Las diferencias entre arquitectura diseñada y arquitectura realmente implementada.
8. Los gaps funcionales y arquitectónicos existentes.
9. Los patrones relevantes de los bots benchmark que podrían beneficiar a OCM.
10. Los patrones de otros proyectos que deliberadamente NO deberían incorporarse.
11. Qué debe corregirse antes de continuar con la implementación.
12. Qué debe convertirse posteriormente en trabajo concreto.
13. Qué cuestiones siguen siendo UNKNOWN y requieren evidencia adicional o Sandbox.

REGLA CENTRAL

NO REDISEÑES OCM DURANTE LA AUDITORÍA.

Primero audita.

No implementes.

No refactorices.

No muevas archivos.

No renombres archivos.

No modifiques código.

No modifiques ADRs.

No modifiques tracking.yaml.

No modifiques configuración.

No modifiques tests.

No hagas commit.

No hagas push.

No instales dependencias.

No copies código de proyectos externos.

No conviertas una inferencia en VERIFIED.

No conviertas documentación en evidencia de implementación.

No conviertas "diseñado" en "implementado".

No conviertas "existe en otro bot" en "OCM debería implementarlo".

Si falta evidencia:

UNKNOWN — evidencia insuficiente.

⸻

1. ESTADO DE LOS TRABAJOS PREVIOS

Antes de comenzar, lee obligatoriamente:

* docs/architecture/decisions/ADR-0029-cancelacion-real-gestion-ordenes-abiertas.md
* docs/architecture/decisions/ADR-0030-balance-real-reconciliacion-patrimonial.md
* docs/audits/2026-08-15-b-md-008-009-diseno-conceptual.md
* docs/audits/2026-08-15-bot-benchmark-b-md-008-009.md
* docs/audits/2026-08-15-verificacion-arquitectonica-cryptofeed-bybit-ws-no-ccxtpro.md

Estos documentos constituyen contexto y evidencia previa, pero NO deben considerarse infalibles.

La auditoría actual debe:

* reutilizar sus conclusiones cuando sigan respaldadas;
* evitar repetir innecesariamente análisis ya cerrados;
* verificar contra código actual cuando la pregunta sea sobre implementación;
* detectar cualquier contradicción nueva;
* distinguir siempre entre diseño y realidad implementada.

CONTEXTO PREVIO RELEVANTE

Las rondas anteriores concluyeron, entre otras cosas:

* cryptofeed está destinado a Market Data público;
* CCXT estándar/REST cubre las operaciones REST de trading/account;
* CCXT Pro fue descartado;
* Private WebSocket de Bybit se considera opcional y futuro;
* B-MD-008 fue diseñado alrededor de CANCELLING y resolución mediante estado del exchange;
* B-MD-009 utiliza totalAvailableBalance como dato operativo para cuentas UTA, con Portfolio como SSOT patrimonial;
* B-MD-008 y B-MD-009 fueron considerados aptos para avanzar hacia implementación/Sandbox, pero no declarados LIVE-ready.

IMPORTANTE: verifica cualquier afirmación anterior contra la documentación y el código. No la trates como verdad únicamente porque aparece escrita aquí.

Si encuentras contradicción:

FUENTE A
FUENTE B
CONTRADICCIÓN
FUENTE MÁS AUTORITATIVA
VEREDICTO

⸻

2. REGLA DE EVIDENCIA

Cada conclusión debe clasificarse:

VERIFIED

Demostrada directamente por:

* código;
* estructura real del repositorio;
* documentación explícita;
* configuración real;
* tests;
* evidencia reproducible.

INFERENCE

Conclusión razonable derivada de evidencia, pero no demostrada directamente.

UNKNOWN

No existe evidencia suficiente.

Usa literalmente:

UNKNOWN — evidencia insuficiente.

Nunca hagas:

INFERENCE → VERIFIED
DISEÑADO → IMPLEMENTADO
DOCUMENTADO → IMPLEMENTADO
EXISTE EN OTRO BOT → NECESARIO EN OCM

⸻

3. FASE 0 — INVENTARIO REAL DEL REPOSITORIO

Antes de analizar arquitectura, inspecciona el árbol REAL.

No asumas que existen:

apps/
src/
packages/
shared/
config/
tests/
docs/
scripts/
tools/
adapters/
infrastructure/
domain/
application/

Si una ruta no existe, indícalo.

Construye:

PATH	RESPONSABILIDAD REAL	MÓDULOS	CONSUMIDORES	CAPA	PROBLEMAS

Determina:

1. ¿La estructura refleja realmente la arquitectura?
2. ¿Hay responsabilidades mezcladas?
3. ¿Hay duplicación conceptual?
4. ¿Hay módulos ambiguos?
5. ¿Hay módulos demasiado grandes?
6. ¿Hay adapters con lógica de dominio?
7. ¿Hay infraestructura filtrándose al dominio?
8. ¿Hay composition roots?
9. ¿Están correctamente aislados?
10. ¿Hay dependencias circulares o acoplamiento sospechoso?
11. ¿Hay módulos que parecen pertenecer a otra capa?
12. ¿Hay nombres de carpetas que contradicen su contenido?

No propongas movimientos todavía.

Primero documenta la realidad.

⸻

4. FASE 1 — MARKET DATA: MAPA REAL COMPLETO

Reconstruye desde código el flujo REAL de Market Data.

No asumas que es:

Bybit
 ↓
cryptofeed
 ↓
callback
 ↓
normalización
 ↓
Order Book
 ↓
Market Data Port
 ↓
Strategy / Risk / Execution

Descubre cuál es realmente.

Determina:

1. dónde se instancia cryptofeed;
2. quién configura los feeds;
3. quién inicia el feed;
4. quién detiene el feed;
5. qué exchanges utiliza;
6. qué canales utiliza;
7. qué símbolos utiliza;
8. qué callbacks recibe;
9. qué estructura de datos recibe;
10. cómo procesa L2;
11. cómo procesa TRADES;
12. qué timestamp proporciona el exchange;
13. qué timestamp proporciona cryptofeed;
14. si existe receipt timestamp;
15. cómo se identifica exchange;
16. cómo se identifica symbol;
17. cómo se identifica event type;
18. si existe sequence;
19. cómo se procesan duplicados;
20. cómo se procesan eventos fuera de orden;
21. cómo se detectan gaps;
22. cómo se recuperan gaps;
23. si existe snapshot inicial;
24. si existe resync;
25. cómo funciona reconexión;
26. qué ocurre ante datos corruptos/incompletos;
27. si existe backpressure;
28. si existen bounded queues;
29. qué ocurre cuando un consumidor se retrasa;
30. si existe lag metric;
31. si existe freshness;
32. si existe health state;
33. si existe circuit breaker;
34. si existe kill switch;
35. qué ocurre cuando Market Data se congela;
36. qué ocurre ante timestamps antiguos;
37. qué ocurre ante un Order Book inconsistente.

Clasifica cada capacidad:

* IMPLEMENTADO
* PARCIAL
* DISEÑADO
* AUSENTE
* UNKNOWN

⸻

4. FASE 2 — MARKET DATA CONTRACTS

Localiza todos los ports, protocols, interfaces, DTOs y contracts relacionados con Market Data.

No inventes abstracciones.

Usa los nombres reales encontrados.

Determina si existen conceptos equivalentes a:

* Market Data
* Market Event
* Trade
* Ticker
* Order Book
* Snapshot
* Delta
* Sequence
* Exchange Timestamp
* Receipt Timestamp
* Freshness
* Market Data Source
* Market Data Consumer

Para cada uno:

NOMBRE REAL
UBICACIÓN
RESPONSABILIDAD
CONSUMIDORES
PROVEEDOR
CAPA
PROBLEMAS

Determina:

A. ¿El dominio conoce cryptofeed?

B. ¿El dominio conoce tipos concretos de Bybit?

C. ¿Existe leakage de infraestructura?

D. ¿Los adapters aíslan correctamente las dependencias externas?

E. ¿La normalización pierde información relevante?

F. ¿Se conservan timestamps suficientes?

G. ¿Se conserva sequence information?

H. ¿Existe trazabilidad desde evento externo hasta consumidor?

I. ¿Los contracts son demasiado genéricos?

J. ¿Los contracts son excesivamente específicos del exchange?

⸻

5. FASE 3 — MARKET DATA → STRATEGY → RISK → EXECUTION

Audita la frontera:

Market Data
     ↓
 Strategy
     ↓
   Risk
     ↓
 Execution

Determina:

* quién consume Market Data;
* si Strategy conoce infraestructura;
* si Risk conoce cryptofeed;
* si Execution consume directamente Market Data;
* si existe un port intermedio;
* qué precio se utiliza para sizing;
* si ese precio es trazable;
* si existe freshness protection;
* si existe stale-data protection;
* si existe Market Data health;
* qué ocurre cuando Market Data deja de ser confiable.

Pregunta crítica

¿Puede OCM actualmente ejecutar una orden utilizando:

* datos congelados;
* datos stale;
* datos fuera de secuencia;
* un Order Book inconsistente;
* un timestamp inválido;
* un símbolo incorrectamente normalizado?

Si sí, documenta exactamente dónde puede suceder.

No propongas todavía una solución.

⸻

6. FASE 4 — NAMING AUDIT

Audita sistemáticamente:

* carpetas;
* archivos;
* clases;
* Protocols;
* interfaces;
* métodos;
* variables;
* eventos;
* DTOs;
* ports;
* adapters;
* services;
* managers;
* runners;
* factories;
* repositories;
* transports;
* composition roots.

Busca:

1. manager, service, helper, utils excesivamente genéricos.
2. Un mismo concepto con múltiples nombres.
3. Un mismo nombre para conceptos diferentes.
4. Métodos cuyo nombre no representa su comportamiento.
5. Singular/plural inconsistente.
6. Verbos inconsistentes.
7. Terminología inconsistente entre capas.
8. Naming heredado de CCXT que no representa el dominio.
9. Naming de Bybit filtrándose al dominio.
10. Abstracciones genéricas que en realidad son adapters.
11. Adapters que parecen servicios de dominio.
12. Nombres que mezclan transporte, dominio y proveedor.

Tabla obligatoria:

Elemento	Ubicación	Nombre actual	Responsabilidad real	Problema	Severidad	Nombre sugerido	Motivo

No renombres nada.

⸻

7. FASE 5 — RESPONSIBILITY / STATE OWNERSHIP AUDIT

Para cada estado importante determina:

STATE
OWNER
STORAGE
UPDATE SOURCE
READERS
RECONCILIATION
PROBLEMA
EVIDENCIA

Como mínimo:

* Order State
* Balance State
* Portfolio State
* Market Data State
* Order Book State
* Risk State
* Execution State
* Exchange State

Busca:

* múltiples fuentes de verdad;
* caches actuando como SSOT;
* WS actuando como autoridad sin justificación;
* REST tratado como evento;
* Portfolio duplicado;
* OMS duplicado;
* Risk manteniendo balance paralelo;
* Market Data duplicado;
* estado de exchange almacenado innecesariamente en múltiples capas.

Para Order State y Balance State, contrasta únicamente contra ADR-0029/ADR-0030 y reporta:

* consistente;
* parcialmente consistente;
* contradictorio.

No repitas todo el análisis previo salvo que aparezca evidencia nueva.

⸻

8. FASE 6 — BENCHMARK DE BOTS

Los ZIP de referencia están fuera del repositorio.

Primero verifica su ubicación real.

Esperados:

freqtrade-develop.zip
hummingbot-master.zip
nautilus_trader-develop.zip

Ubicación esperada:

~/kb-local-only/

Si no están ahí:

UNKNOWN — evidencia insuficiente.

No inventes rutas alternativas.

Si existen, descomprímelos exclusivamente fuera del repositorio, por ejemplo:

/tmp/ocm-bot-refs/

Nunca dentro de:

orangecashmachine/
docs/
src/

Limpia el temporal al terminar.

IMPORTANTE

No repitas el benchmark completo de B-MD-008/B-MD-009.

Ya existe:

docs/audits/2026-08-15-bot-benchmark-b-md-008-009.md

Utiliza ese documento para las conclusiones ya cubiertas.

El benchmark nuevo debe concentrarse en:

* Market Data;
* Order Book;
* snapshots;
* sequencing;
* gap recovery;
* freshness;
* backpressure;
* observability;
* testing;
* repository organization;
* naming;
* separación de responsabilidades.

No copies código.

No copies snippets.

No copies estructuras de implementación.

Solo extrae patrones conceptuales.

Para cada patrón:

BOT	PATRÓN	PROBLEMA QUE RESUELVE	EVIDENCIA	EQUIVALENTE OCM	ESTADO OCM	BENEFICIO	PRIORIDAD	RIESGO

Clasifica beneficio:

* NO APLICABLE
* ÚTIL PARCIALMENTE
* BENEFICIO CLARO
* REQUIERE MÁS EVIDENCIA

No uses "mejor arquitectura" como argumento suficiente.

⸻

9. FASE 7 — GAP ANALYSIS

Después de la auditoría y benchmark:

CAPACIDAD	OCM ACTUAL	EVIDENCIA	GAP	IMPACTO	PRIORIDAD

Prioridades:

P0

Necesario para corrección.

P1

Necesario antes de LIVE.

P2

Robustez importante.

P3

Optimización.

P4

Nice-to-have.

Sé conservador.

No conviertas automáticamente una ausencia en P0.

⸻

10. FASE 8 — PROBLEMAS ACTUALES

Clasifica cada hallazgo exactamente como:

* BUG ARQUITECTÓNICO
* RIESGO
* DEUDA TÉCNICA
* INCONSISTENCIA DOCUMENTAL
* NAMING
* ORGANIZACIÓN
* GAP FUNCIONAL
* UNKNOWN

Para cada uno:

ID
CATEGORÍA
UBICACIÓN
EVIDENCIA
PROBLEMA
IMPACTO
SEVERIDAD
PRIORIDAD
VERIFIED / INFERENCE / UNKNOWN

⸻

11. FASE 9 — QUÉ NO DEBEMOS IMPORTAR

Busca explícitamente patrones de los benchmarks que serían contraproducentes para OCM.

Ejemplos posibles:

* dependencia comercial innecesaria;
* WS como SSOT;
* duplicación de estado;
* abstracciones excesivas;
* event buses prematuros;
* microservicios prematuros;
* complejidad innecesaria;
* arquitectura copiada de otro dominio;
* dependencia de un framework cuando OCM no la necesita.

No asumas que estos ejemplos son realmente problemas.

Demuestra cada uno.

Tabla:

PATRÓN	PROYECTO	EVIDENCIA	POR QUÉ APARECE	POR QUÉ NO APLICA A OCM	RIESGO

⸻

12. FASE 10 — ARQUITECTURA OBJETIVO

Solo después de completar la auditoría, describe conceptualmente una arquitectura objetivo.

Máximo 25 líneas.

Debe incluir:

* Market Data;
* Order Book;
* Strategy;
* Risk;
* Execution;
* OMS;
* Portfolio;
* adapters;
* ports;
* REST;
* cryptofeed;
* Bybit;
* reconciliation;
* testing.

No introduzcas componentes nuevos salvo que exista evidencia concreta de necesidad.

Esta arquitectura debe ser compatible con ADR-0029/ADR-0030.

Si no lo es:

CONTRADICCIÓN ARQUITECTÓNICA — requiere decisión humana.

⸻

13. FASE 11 — PROPUESTA DE REORGANIZACIÓN

No ejecutes ningún movimiento.

Solo propón.

Para cada cambio:

FROM
TO
MOTIVO
EVIDENCIA
DEPENDENCIAS
RIESGO
PRIORIDAD

Incluye:

1. carpetas que deben permanecer;
2. carpetas posiblemente mal ubicadas;
3. archivos que podrían moverse;
4. nombres que podrían cambiar;
5. módulos que podrían dividirse;
6. módulos que podrían fusionarse.

Si no existe suficiente evidencia para recomendar un movimiento:

UNKNOWN — evidencia insuficiente.

⸻

14. FASE 12 — ROADMAP

Ordena el trabajo por dependencia.

FASE 0

Correcciones documentales/naming de bajo riesgo.

FASE 1

Correcciones arquitectónicas necesarias.

FASE 2

Market Data robustness.

FASE 3

B-MD-008.

Referenciar ADR-0029.

No rediseñarlo.

FASE 4

B-MD-009.

Referenciar ADR-0030.

No rediseñarlo.

FASE 5

Sandbox.

FASE 6

LIVE hardening.

No mezcles:

CORRECCIÓN
ROBUSTEZ
OPTIMIZACIÓN

⸻

15. FASE 13 — FALSACIÓN

Intenta demostrar que la arquitectura actual es incorrecta.

Busca contraejemplos en:

1. Market Data stale.
2. Order Book gap.
3. Out-of-order.
4. Duplicate.
5. Disconnect.
6. Reconnect.
7. REST unavailable.
8. Cancel/FILL race.
9. Balance stale.
10. Restart.
11. Partial response.
12. Rate limit.
13. Exchange degradation.
14. Incorrect normalization.
16. Incorrect timestamp.
17. Incorrect symbol mapping.
18. Multiple sources of truth.
19. Consumer slower than producer.
20. Missing snapshot.
21. Missing resync.

Para cada uno:

ESCENARIO
EVIDENCIA
IMPACTO
MITIGACIÓN EXISTENTE
GAP
VERIFIED / INFERENCE / UNKNOWN

Para B-MD-008/B-MD-009 puedes reutilizar el benchmark anterior salvo que exista evidencia nueva.

⸻

16. FASE 14 — CONTRADICCIONES

Si existe contradicción entre:

Código
ADR
Auditoría previa
Benchmark
Configuración
Tests

NO la resuelvas silenciosamente.

Registra:

FUENTE A	FUENTE B	CONTRADICCIÓN	AUTORIDAD	VEREDICTO

La autoridad debe justificarse.

⸻

17. FASE 15 — RECOMENDACIÓN FINAL

Debe responder explícitamente:

A.

¿La arquitectura actual de Market Data es coherente?

B.

¿cryptofeed está correctamente aislado?

C.

¿Existe leakage de infraestructura hacia dominio?

D.

¿Existe alguna fuente de verdad duplicada?

E.

¿La estructura de carpetas representa realmente la arquitectura?

F.

¿El naming es consistente?

G.

¿Cuáles son los 5 problemas de mayor prioridad?

H.

¿Cuáles son las 5 mejoras con mayor retorno?

I.

¿Qué patrones de los bots deberíamos adoptar?

J.

¿Qué patrones deberíamos rechazar?

K.

¿Qué debe hacerse antes de continuar con B-MD-008/B-MD-009?

L.

¿Qué debe probarse en Sandbox?

M.

¿Qué NO debemos implementar todavía?

⸻

18. VEREDICTO FINAL

Entrega exactamente estas secciones:

1. ESTADO ACTUAL DE OCM
2. ARQUITECTURA REAL DE MARKET DATA
3. GAPS CRÍTICOS
4. GAPS IMPORTANTES
5. GAPS OPCIONALES
6. PROBLEMAS DE NAMING
7. PROBLEMAS DE ESTRUCTURA
8. PROBLEMAS DE CONTRATOS
9. PATRONES ÚTILES DE LOS BOTS
10. PATRONES QUE DEBEMOS DESCARTAR
11. QUÉ FALTA IMPLEMENTAR
12. QUÉ DEBE CORREGIRSE
13. QUÉ NO DEBE TOCARSE
14. ARQUITECTURA OBJETIVO
15. PROPUESTA DE REORGANIZACIÓN
16. ROADMAP
17. RIESGOS
18. UNKNOWN
19. CONTRADICCIONES CON TRABAJO PREVIO
20. RECOMENDACIÓN FINAL

⸻

19. RECOMENDACIÓN FINAL

Debe responder explícitamente:

A.

¿La arquitectura actual de Market Data es coherente?

B.

¿cryptofeed está correctamente aislado?

C.

¿Existe leakage de infraestructura hacia dominio?

D.

¿Existe alguna fuente de verdad duplicada?

E.

¿La estructura de carpetas representa realmente la arquitectura?

F.

¿El naming es consistente?

G.

¿Cuáles son los 5 problemas de mayor prioridad?

H.

¿Cuáles son las 5 mejoras con mayor retorno?

I.

¿Qué patrones de los bots deberíamos adoptar?

J.

¿Qué patrones deberíamos rechazar?

K.

¿Qué debe hacerse antes de continuar con B-MD-008/B-MD-009?

M.

¿Qué NO debemos implementar todavía?

⸻

20. ENTREGABLE

Crear únicamente:

docs/audits/2026-08-15-auditoria-integral-market-data-naming-estructura.md

Este es el único archivo que puede crearse o modificarse durante esta auditoría.

No modificar ningún otro archivo.

No modificar:

* código;
* ADRs;
* tracking.yaml;
* configuración;
* tests;
* estructura del repositorio;
* nombres;
* dependencias;
* documentación existente.

No hacer commit.

No hacer push.

No instalar dependencias.

No copiar código de los bots.

No implementar recomendaciones.

⸻

21. VALIDACIÓN OBLIGATORIA ANTES DE TERMINAR

Ejecuta:

git --no-pager status --short

y confirma qué cambios existían antes de comenzar y cuáles produjo esta auditoría.

Después verifica:

git --no-pager diff -- docs/audits/2026-08-15-auditoria-integral-market-data-naming-estructura.md

Confirma explícitamente:

ARCHIVOS MODIFICADOS:
ARCHIVO CREADO:
CÓDIGO MODIFICADO: NO
ADRs MODIFICADOS: NO
tracking.yaml MODIFICADO: NO
CONFIGURACIÓN MODIFICADA: NO
TESTS MODIFICADOS: NO
COMMIT: NO
PUSH: NO
DEPENDENCIAS INSTALADAS: NO

Si aparecen modificaciones adicionales no producidas por esta tarea, no las reviertas. Repórtalas como cambios preexistentes.

⸻

21. VALIDACIÓN OBLIGATORIA DESPUÉS DE TERMINAR

Ejecuta:

git --no-pager status --short

y confirma qué cambios existían antes de comenzar y cuáles produjo esta auditoría.

Después verifica:

git --no-pager diff -- docs/audits/2026-08-15-auditoria-integral-market-data-naming-estructura.md

Confirma explícitamente:

ARCHIVOS MODIFICADOS:
ARCHIVO CREADO:
CÓDIGO MODIFICADO: NO
ADRs MODIFICADOS: NO
tracking.yaml MODIFICADO: NO
CONFIGURACIÓN MODIFICADA: NO
TESTS MODIFICADOS: NO
COMMIT: NO
PUSH: NO
DEPENDENCIAS INSTALADAS: NO

Si aparecen modificaciones adicionales no producidas por esta tarea, no las reviertas. Repórtalas como cambios preexistentes.

⸻

22. REGLA DE CALIDAD

No sacrifiques precisión por volumen.

Si una fase no puede completarse con evidencia suficiente:

UNKNOWN — evidencia insuficiente.

Si el análisis completo no puede terminarse en una sola pasada con calidad:

1. detente;
2. guarda únicamente las secciones completadas en el entregable;
3. marca claramente las fases pendientes;
4. explica qué evidencia falta;
5. no inventes conclusiones para completar el documento.

La auditoría debe preferir un UNKNOWN correcto antes que una conclusión falsa.

Al terminar, entrega el diagnóstico.

No preguntes si debe implementarse.

La decisión de implementación se tomará después de revisar esta auditoría.
