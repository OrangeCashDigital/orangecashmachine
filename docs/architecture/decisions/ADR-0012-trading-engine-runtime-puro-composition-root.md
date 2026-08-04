# ADR-0012: TradingEngine es un objeto runtime puro; el Composition Root ensambla todo

**Estado:** Aceptado
**Fecha:** 2026-08-03
**Bounded context(s) afectado(s):** trading
**Supersedes:** ADR-0005 (reemplazado)

## Contexto

ADR-0005 estableció un split: TradingEngine.build_live()/build_paper()
construyen las dependencias internas del ciclo estrategia-riesgo-ejecución
(Strategy, RiskManager, Executor, OMS) y el Composition Root solo las
externas. La justificación era DRY: no duplicar el ensamblaje que los
factories ya resolvían.

Tras la auditoría de composition roots (2026-08-03), TradingCompositionRoot
v3 se convirtió en el único punto de ensamblado del bounded context
(ADR-0003 enmendado). PaperBot — el único consumidor de build_paper() fuera
del root — fue eliminado. Quedó un solo caller de los factories: el propio
root. Los factories eran entonces una indirección sin propósito:
TradingEngine.build_live()/build_paper() no tenían consumidores externos
demostrables (grep en repositorio completo: README, ejemplos, scripts,
docs, notebooks, research) y no había tests que los invocaran directamente.

## Alternativas evaluadas

1. **Privatizar los factories** (`_build_live`/`_build_paper`): siguen
   existiendo físicamente; solo desaparecen del API público. ADR-0005
   seguiría siendo válido. Menor cambio, pero preserva la indirección y la
   responsabilidad de construcción fuera del root.
2. **Inline completo en el root + eliminar los factories**: TradingEngine
   pierde la sección Factory y queda como objeto runtime puro
   (constructor directo). El root construye Strategy + RiskManager +
   Executor + OMS. Eliminación física del camino antiguo.

## Decisión

Se adopta la alternativa 2. TradingCompositionRoot.assemble_live()/
assemble_paper() construyen las dependencias internas y externas del engine.
TradingEngine.build_live()/build_paper() se eliminan; TradingEngine queda
con su constructor, run_once(), validate_signal(), oms_summary y privados.

## Justificación técnica

- **DIP/SRP:** el Composition Root es, por definición, el único responsable
  de ensamblar dependencias. Eliminar los factories elimina la única
  duplicación de autoridad que justificaba ADR-0005.
- **Sin cambio de comportamiento observable:** el inline es relocación 1:1.
  Mismo Strategy (`StrategyRegistry.get(name)(**cfg)`), mismo RiskManager
  (`config`, `capital_usd`), mismo Executor (Live/Paper), mismo OMS
  (`risk_manager, executor, guard, on_fill, on_reject=None`), mismo guard,
  mismo TradeTracker, mismos callbacks on_fill. Los fail-fast de live
  (guard/risk obligatorios) ya los aplica assemble_live() antes de
  construir; el default de paper se conserva vía _resolve_risk_config().
- **DRY preservado:** una sola fuente de ensamblado — el root — sin
  delegación a un colaborador sin otro consumidor.
- **Efecto colateral positivo:** eliminar paper_bot.py (que importaba
  trading.engine en top-level) rompe el ciclo de imports latente
  `trading.engine ↔ trading.execution`.

## Consecuencias

- TradingEngine no construye sus dependencias. No se deben reintroducir
  factories en el engine; el ensamblado vive en el root (ADR-0003).
- Cambios en la construcción de internas se hacen en assemble_live()/
  assemble_paper() del root, no en el engine.
- Los imports de LiveExecutor/PaperExecutor/OMS/RiskManager/StrategyRegistry
  son lazy dentro de los métodos assemble_* (mismo patrón que tenían los
  factories) — evita ciclos a nivel de módulo.
- El engine se prueba vía el root (assemble_paper → run_once), como en
  tests/trading/test_composition_root.py.

## Referencias

- ADR-0005 (reemplazado por este ADR)
- ADR-0003 (enmendado 2026-08-03) — interfaz del root
- packages/trading/bootstrap/composition_root.py (assemble_live/assemble_paper)
- packages/trading/engine.py (runtime puro)
- docs/architecture/recovered/trading-bootstrap-forensic-analysis.md
