# ADR-0021: Estado de posición con un solo dueño mutable — unificar en PortfolioService

> **ESTADO: PROPUESTA** — borrador de diseño para decisión humana. NO aprobado. NO implementado. Ningún contrato cambia hasta su aceptación formal (proceso ADR + tracking.yaml).
> **Corresponde a:** B-15 (tracking.yaml), hallazgo H-09 de `docs/audits/2026-08-auditoria-integral.md`. Sustituye la nota "ADR-0021 (pendiente de redactar)" en tracking B-15.

**Estado:** Propuesto
**Fecha:** 2026-08-16
**Bounded context(s) afectado(s):** trading (execution, analytics), portfolio (models)

## Contexto

El estado de posición está repartido en **tres representaciones mutables** (verificado en F0 2026-07-29, B-15):

1. `open_order_ids` en `packages/trading/execution/fill_sync.py:75` — registro de pares buy/sell de fills.
2. `_open_positions` en `packages/trading/application/analytics/trade_tracker.py:58` — cache mutable de posiciones del trade tracker.
3. `PositionStore` detrás de `PortfolioService` (`packages/portfolio/`) — SSOT formal (ADR-0006, BC-43).

**Riesgo verificado:** `close_position` en `packages/portfolio/services/portfolio_service.py:126-152` traga la excepción (patrón `except -> None`, nunca lanza); `fill_sync` descarta el retorno (protocolo `fill_sync.py:46-49`, rama SELL `:96-98`). Un cierre de posición puede **fallar en silencio** sin propagación: el estado real del exchange/portfolio queda divergente de la vista de trading.

**Mitigación existente (PARCIAL, 2026-08-12):** el SELL path de `build_fill_sync` ahora captura el retorno de `close_position()` y emite `logger.critical 'POSITION_CLOSE_UNCONFIRMED'` si es `None` — el fallo silencioso es observable en operación, sin cambiar el contrato SafeOps ni hacer que el flujo lance. Tests: `tests/trading/test_fill_sync_close_divergence.py` (5 tests).

**Raíz sin resolver:** tres dueños mutables del mismo hecho (posición). Un solo dueño mutable elimina la clase de bugs de estado divergente (mismo principio que ADR-0006).

## Alternativas evaluadas

1. **Unificar todo el estado de posición en PortfolioService (ELEGIDA).** Portfolio ya es SSOT formal de posiciones (ADR-0006, BC-43, solo instanciable en portfolio bootstrap). `fill_sync`/`TradeTracker` pasan a **consumir eventos/lectura** del SSOT, nunca estado mutable propio. Ventaja: un único dueño mutable; la divergencia se vuelve estructuralmente imposible. Costo: trading deja de tener cache mutable propio (`_open_positions`, `open_order_ids` pasan a ser derivados de lectura); exige limpiar las dependencias que hoy escriben estado en trading (BC-13 prohíbe portfolio→trading; la dirección trading→portfolio ya existe por inyección, ADR-0016/0027).
2. **Mantener caches mutables pero sincronizados (estado actual).** Ventaja: nada que tocar en trading. Costo-riesgo: es exactamente el bug actual — cada sync es un punto de divergencia; la mitigación (log crítico) solo hace visible el fallo, no lo elimina. Rechazada como solución definitiva.
3. **Portfolio emite eventos de cambio de posición y trading se suscribe.** Compatible con (1) como mecanismo de propagación (fill_sync consume eventos), pero exige un event bus que hoy no existe en trading (100% síncrono/callback, verificado). No es prerrequisito: la lectura por inyección del SSOT cubre el caso sin bus (mismo patrón que ADR-0029 §Decision 6).

## Decisión

1. **Un solo dueño mutable del estado de posición: PortfolioService** (refuerza y completa ADR-0006). `fill_sync` y `TradeTracker` dejan de ser dueños de estado de posición: `open_order_ids` y `_open_positions` pasan a ser **derivados de lectura** (consulta al SSOT) o se eliminan.
2. **`close_position` deja de tragar la excepción** en silencio: el contrato SafeOps se conserva en el nivel de transporte (nunca lanza hacia arriba en el camino del fill), pero el retorno debe distinguir "posición inexistente" de "fallo de persistencia" (hoy ambos son `None` — limitación documentada en `fill_sync.py`). La propagación del error al dominio queda explícita y observable.
3. **`fill_sync` no descarta el retorno de `close_position`**: cualquier fallo de cierre (confirmado o no) se registra y, si el destino no se corrige automáticamente, queda visible para reconciliación (manteniendo la mitigación de observabilidad ya implementada como comportamiento permanente).
4. **Contratos BC-NN**: BC-13 (portfolio no importa trading), BC-43 (stores solo en portfolio bootstrap) y BC-50 (trading no importa market_data fuera del CR) siguen siendo la red que hace cumplir el ownership; trading consume portfolio vía inyección (patrón ADR-0006/0016/0027), nunca por import.

## Justificación técnica

- **La raíz del bug B-15 es el multi-ownership mutable**, no el `except -> None`. La mitigación (log crítico) convirtió el fallo silencioso en observable; esta decisión elimina la causa: si hay un solo dueño, "cierre falló" ya no puede coexistir con "vista de trading lo da por cerrado".
- **Es la misma regla que ADR-0006** (un solo dueño de estado mutable elimina la clase de bugs de estado divergente), aplicada al flujo de cierre (SELL) que la Fase 3 de ADR-0006 dejó con caches en trading.
- **No introduce event bus**: la propagación por lectura/inyección del SSOT es suficiente para LIVE; el event bus (si llega) se alinea luego (mismo criterio que ADR-0029).
- **Veredicto:** no bloquea LIVE por sí misma (la mitigación de observabilidad ya está), pero es la deuda arquitectónica raíz de B-15; resolverla reduce el riesgo residual de posición fantasma.

## Consecuencias

- **Más fácil:** un solo lugar donde muta la posición; la divergencia fill_sync/TradeTracker/Portfolio se vuelve estructuralmente imposible; el "cierre fallido" es detectable y reconciliable.
- **Deuda aceptada:** requiere migrar el cache de trading (`_open_positions`, `open_order_ids`) a lectura del SSOT; los CLIs legados y el `portfolio_service=None` opcional (ADR-0006, Consecuencias) siguen pendientes de eliminar; no se introduce event bus en esta fase.
- **Contratos BC-NN que lo hacen cumplir:** BC-13 (dirección portfolio↔trading), BC-43 (stores solo en portfolio bootstrap), BC-50 (CR único), BC-09 (dominio sin framework).
- **Riesgo residual:** mientras no se unifique, la mitigación de observabilidad (log crítico `POSITION_CLOSE_UNCONFIRMED`) sigue activa como red de seguridad; la posición fantasma solo puede persistir hasta reconciliación manual.

## Referencias

- Código: `packages/trading/execution/fill_sync.py:46-49,75,96-98`, `packages/trading/application/analytics/trade_tracker.py:58`, `packages/portfolio/services/portfolio_service.py:126-152`, `packages/portfolio/bootstrap/composition_root.py`.
- Tests: `tests/trading/test_fill_sync_close_divergence.py` (5 tests, mitigación).
- Tracking: `docs/plans/tracking.yaml` B-15 (EN_CURSO, mitigación PARCIAL; raíz pendiente ADR-0021).
- ADRs relacionados: ADR-0006 (portfolio dueño de posiciones — base de esta decisión), ADR-0016 (reconciliación de fills), ADR-0027 (recovery/SSOT), ADR-0029 (callbacks al flujo de fill, sin event bus), ADR-0030 (portfolio dueño del estado patrimonial).