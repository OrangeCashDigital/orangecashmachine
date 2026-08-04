# ADR-0004: TradingCompositionRoot es el único punto autorizado a importar market_data (BC-50)

**Estado:** Aceptado — contrato de import-linter formalizado como BC-50
**Fecha:** 2026-08-02 (enmendado 2026-08-03)
**Bounded context(s) afectado(s):** trading, market_data

## Contexto

El bytecode recuperado muestra el comentario literal "Único punto de
trading autorizado a importar market_data (BC-47)" en
_build_feature_reader. GoldReader implementa FeatureReaderPort
estructuralmente (Protocol).

**Corrección 2026-08-03:** el número BC-47 ya estaba ocupado en
`architecture/importlinter.toml` (`shared.kafka does not import domain`).
La frontera trading→market_data se formaliza como **BC-50**. `RedisFactory`
NO se recrea (obsoleto — portfolio es dueño de Redis, ver ADR-0003).

## Alternativas evaluadas

1. Permitir que cualquier módulo de trading importe market_data
   directamente — rompe BC-10 y esparce el acoplamiento.
2. Restringir el import a TradingCompositionRoot únicamente.

## Decisión

Solo trading.bootstrap.composition_root puede importar market_data.
El resto de trading depende de FeatureReaderPort / FeatureSource, nunca de
GoldReader directamente.

## Justificación técnica

Mismo principio que BC-38/BC-42: concentra el acoplamiento externo en
un solo archivo auditable.

## Consecuencias

- BC-50 formalizado en `architecture/importlinter.toml` como contrato
  forbidden (sin `ignore_imports`: import-linter analiza el grafo estático y
  el import de GoldReader dentro del root es lazy — no genera arista. La
  excepción del composition root se gobierna por convención/documentación).
- `trading/data/gold_adapter.py` retirado en la auditoría 2026-08-03 (B6):
  su import de market_data violaba la intención de la frontera. El adaptador
  de Gold a FeatureSource vive ahora dentro del composition root.

## Referencias

- docs/architecture/recovered/trading-bootstrap-forensic-analysis.md
- packages/market_data/ports/outbound/feature_reader.py (FeatureReaderPort)
- Contratos análogos: BC-10, BC-38, BC-42
