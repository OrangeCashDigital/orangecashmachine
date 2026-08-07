# ADR-0017: Protocol Discovery Framework (PDF)

**Estado:** Aceptado
**Fecha:** 2026-08-06
**Bounded context(s) afectado(s):** OCM (plataforma), market_data (ingesta), trading (execution), shared (kafka/schemas)

## Contexto

OrangeCashMachine integra protocolos externos (exchanges, blockchains, brokers) de los que deriva
modelos de dominio: OHLCV, orderbook, trades, funding, liquidaciones, órdenes y fills. Hasta F2.3
la procedencia de esos contratos era implícita: un schema wire existía y se presumía "correcto",
pero nadie podía responder *con evidencia* qué mensaje real del exchange había dado forma a cada
campo. La auditoría de contract origins (2026-08) demostró que la mayoría de los schemas eran
ASSUMED (deducidos de docs o adivinados), y que solo el orderbook venía de un protocolo observado
de verdad.

F2.3 dejó la semilla: `tests/kafka/test_schema_provenance.py` registra el linaje de cada schema con
la taxonomía `PROTOCOL | DOCUMENTATION | UPSTREAM_LIBRARY | DOMAIN | ASSUMED`. Falta elevarlo a
**metodología**: un Framework único, permanente y multi-fuente que gobierne cómo se descubre,
valida y modela cualquier protocolo externo antes de incorporarlo al dominio.

No es un "kit" de scripts: es una metodología de ingeniería. El Framework nunca cambia; lo que
cambia son los **Discovery Profiles** — una implementación concreta por integración (Bybit,
Binance, Hyperliquid, Ethereum, Solana, un broker...).

## Alternativas evaluadas

1. **Kit de scripts ad-hoc por exchange.** Descartada. Duplica lógica, rompe la consistencia de
   evidencia entre integraciones y es desechable cada vez que llega un exchange nuevo.
2. **Descubrimiento por observación pura sin catálogo.** Descartada. Capturar sin registrar qué
   campos se observaron no produce evidencia reutilizable; el linaje vuelve a perderse.
3. **Framework único + Discovery Profiles. Elegida.** Metodología permanente, generativa y
   verificable; cada integración implementa un perfil sin alterar el Framework. Contract
   Provenance es un componente del Framework, no el objetivo principal.

## Decisión

**Protocol Discovery Framework (PDF)** — metodología única de OrangeCashMachine para descubrir,
validar y modelar protocolos externos. Componentes (orden de aplicación):

1. **Objetivo** — definir el contrato de entrada/salida del discovery para la integración.
2. **Principios** — evidencia sobre suposición; linaje obligatorio; no-SSOT hasta validar;
   diseño multi-fuente desde el inicio.
3. **Tipos de evidencia** — PROTOCOL (mensaje observado del wire), DOCUMENTATION (documentación
   oficial), UPSTREAM_LIBRARY (librería verificada, p.ej. CCXT), DOMAIN (evento propio OCM),
   ASSUMED (provisional, sin fuente).
4. **REST Discovery** — captura/observación de endpoints REST (OHLCV, meta, etc.).
5. **WebSocket Discovery** — captura/observación de streams WS (orderbook, trades, funding).
6. **Execution Discovery** — observación del ciclo orden→fill→estado en el exchange.
7. **Funding Discovery** — observación de mensajes de funding/interest.
8. **Liquidation Discovery** — observación de mensajes de liquidaciones.
9. **Contract Provenance** — registro del linaje de cada contrato (Schema/DTO/Evento/VO) con la
   taxonomía del punto 3; solo los de provenance estable califican SSOT.
10. **Normalización** — transformación del mensaje observado al modelo interno OCM (puerto
    outbound; hoy `shared`/`ports/outbound/normalization.py`).
11. **Validación** — invariantes de campo, tipos, rangos y compatibilidad backward sobre el
    contrato normalizado.
12. **Fixtures** — muestras congeladas de los mensajes observados reales para reproducibilidad.
13. **Tests** — pruebas de linaje (semilla: `tests/kafka/test_schema_provenance.py`), normalización
    y validación.
14. **Promotion Rule** — un contrato se **promueve a SSOT** solo si su provenance es estable
    (PROTOCOL/DOCUMENTATION/UPSTREAM_LIBRARY/DOMAIN) y pasa validación; los ASSUMED permanecen
    provisionales y se resuelven en la fase que los haga observables.

Estructura del Framework:

```
Protocol Discovery Framework (PDF)
├── Exchange        └── Blockchain        └── Broker
│   ├── Bybit           ├── Ethereum          ├── (brokers)
│   ├── Binance         ├── Solana
│   ├── Hyperliquid     ├── Arbitrum
│   └── OKX             └── Base
```

Cada hoja es un **Discovery Profile**: implementa los 14 componentes para esa fuente sin tocar el
Framework. Bybit es el primer profile (por ser el exchange operativo en F3), pero el diseño es
agnóstico.

## Justificación técnica

- **Framework único** evita que cada integración invente su propio criterio de evidencia; la
  calidad de un contrato deja de depender de quién lo escribió.
- **Contract Provenance como componente** (punto 9) en lugar de objetivo principal: el objetivo es
  *descubrir protocolos con evidencia*; el provenance es la disciplina que lo hace verificable.
  Esto corrige el sesgo de F2.3, donde el provenance se veía como fin en sí mismo.
- **Promotion Rule (14)** materializa el gate normativo de F2.5: el capital real (F3) solo opera
  con contratos promovidos, nunca con ASSUMED. Un schema ASSUMED bloqueado a live = release
  bloqueado por el Production Gate (ADR-0020).
- La semilla de F2.3 (`test_schema_provenance.py`) es el arranque operativo de los componentes 9 y
  13; la fase F2.5 los institucionaliza sin esperar a F3.

## Consecuencias

- **Más fácil:** añadir un exchange/blockchain nuevo = escribir un Discovery Profile; el Framework
  (14 pasos) es inmutable y da checklist.
- **Deuda aceptada:** los puntos 4–8 (REST/WS/Execution/Funding/Liquidation Discovery) se
  implementan por profile; hoy solo Bybit tiene observación parcial (orderbook, vía cryptofeed). El
  resto se completa conforme cada fuente se integre (F3+).
- **Contratos BC-NN que lo hacen cumplir:**
  - `BC-29` — schemas wire en `shared.kafka` (los contratos descubiertos pasan por ahí).
  - `BC-09` / guards — el dominio sigue framework-agnostic; el PDF vive en adapters/infra.
  - `tests/kafka/test_schema_provenance.py` — guard de linaje (F2.3, semilla del punto 13).

## Referencias

- Código: `tests/kafka/test_schema_provenance.py` (registro de linaje, semilla F2.3),
  `packages/market_data/adapters/inbound/websocket/ws_trades_source.py` (fuente WS, STUB en F2.5),
  `packages/market_data/adapters/outbound/exchange/ccxt_adapter.py` (adapter CCXT).
- Hallazgos: H-15 (schemas Kafka sin cobertura/linaje, B-18), H-20 (conteo de contratos, B-10).
- ADRs relacionados: ADR-0013 (modelo de ingestión), ADR-0018 (Schema Registry), ADR-0020
  (Production Gate — promoción de contratos como gate de release). Renumeración: ADR-0021 asume el
  antiguo número 0017 (Unificación del estado de posiciones).
