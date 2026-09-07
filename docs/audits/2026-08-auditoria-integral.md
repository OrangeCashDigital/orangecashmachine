
### CLI-003 — setup_observability() / configure_logging()

Estado: JUSTIFIED / NO ACTION

Principios aplicados: Fail-soft donde conviene, SafeOps, Resiliencia,
Desacoplamiento (el fallo de un subsistema de observabilidad no debe
acoplar su disponibilidad a la del pipeline de trading).

A) setup_observability()
   - init_metrics_runtime() delega el fail-soft al propio MetricsRuntime
     (SSOT del comportamiento: un solo lugar decide qué pasa si falla).
   - init_tracing() construye TracingRuntime.from_config(cfg) y llama a
     .start(). TracingRuntime.start() ya captura Exception internamente
     (tracing.py:138), loguea WARNING y devuelve False sin propagar —
     este es el "fail-fast donde importa, fail-soft donde conviene"
     aplicado correctamente en la capa interna.
   - El except Exception externo NO duplica esa protección: cubre la
     superficie PREVIA a .start() — getattr(cfg, "observability", None),
     obs.tracing is None, y las conversiones de tipo en from_config()
     (bool(), float() sobre atributos potencialmente ausentes o mal
     tipados). Es una capa de defensa distinta, no redundante.
   - Angostar el except aquí violaría KISS sin beneficio: no existe una
     jerarquía de excepciones cerrada para "config de observabilidad
     parcialmente inválida"; inventar una ad-hoc sería complejidad
     especulativa sin caso de uso real que la exija.

B) configure_logging()
   - run_application() declara el contrato fail-soft explícitamente en
     su propio comentario ("un fallo de logging no mata el pipeline") —
     el contrato vive donde se consume, no se infiere.
   - _install_sinks() es la superficie real de riesgo y es heterogénea
     por diseño (Desacoplamiento correcto: consola, Prometheus,
     filesystem, Loki no comparten una causa de fallo común): I/O de
     disco (rotación/retención), sockets de red (Loki), y dependencias
     de terceros (Loguru, cliente Loki) cuyas excepciones no están
     documentadas de forma cerrada.
   - Sustituir except Exception por una lista concreta (OSError,
     ConnectionError, etc.) sería fail-fast donde NO importa: rompería
     la Resiliencia del sistema de trading por un fallo de un
     subsistema no crítico (logging), violando el propio contrato ya
     declarado en el código.

Conclusión: el broad catch en ambos puntos es deliberado, está acotado
a subsistemas no críticos para la ejecución del pipeline, y está
respaldado por un contrato fail-soft ya documentado. No se modifica
comportamiento. No se abre ningún hallazgo nuevo para CLI-003.
