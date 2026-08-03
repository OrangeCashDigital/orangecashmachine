# ADR-0004: TradingCompositionRoot es el único punto autorizado a importar market_data (BC-47)

**Estado:** Aceptado en diseño — contrato de import-linter aún no formalizado
**Fecha:** 2026-08-02
**Bounded context(s) afectado(s):** trading, market_data

## Contexto

El bytecode recuperado muestra el comentario literal "Único punto de
trading autorizado a importar market_data (BC-47)" en
_build_feature_reader. BC-47 no existe hoy en architecture/importlinter.toml.
GoldReader implementa FeatureReaderPort estructuralmente (Protocol).

## Alternativas evaluadas

1. Permitir que cualquier módulo de trading importe market_data
   directamente — rompe BC-10 y esparce el acoplamiento.
2. Restringir el import a TradingCompositionRoot únicamente.

## Decisión

Solo trading.bootstrap.composition_root puede importar market_data.
El resto de trading depende de FeatureReaderPort, nunca de GoldReader
directamente.

## Justificación técnica

Mismo principio que BC-38/BC-42: concentra el acoplamiento externo en
un solo archivo auditable.

## Consecuencias

- Acción pendiente: formalizar BC-47 en architecture/importlinter.toml
  como contrato forbidden.
- Hasta entonces es solo convención documentada, no bloquea merges.

## Referencias

- docs/architecture/recovered/trading-bootstrap-forensic-analysis.md
- packages/market_data/ports/outbound/feature_reader.py (FeatureReaderPort)
- Contratos análogos: BC-10, BC-38, BC-42
