# Internal derived reference from official Bybit documentation

**Tipo:** Referencia interna derivada (no fuente primaria, no contrato).
**Dominio:** Derivados perpetuos lineales Bybit (category `linear`), con enfoque en BTC/USDT, ETH/USDT, SOL/USDT perpetuals.
**Fecha de consulta:** 2026-08-14.
**Estado:** `needs_verification` (referencia derivada de fuentes oficiales; los números de contrato cambian y deben re-verificarse antes de cualquier uso operativo).
**Clasificación de afirmaciones:**
- `OFFICIAL_BYBIT_FACT` — afirmación tomada literalmente de documentación oficial Bybit (URL de la fuente).
- `CONCEPTUAL_KNOWLEDGE` — conocimiento conceptual general del dominio (crypto perpetuals), no afirmado por una página concreta de Bybit.
- `OCM_RESEARCH_HYPOTHESIS` — hipótesis de research de OCM, no evidencia.
- `OCM_EVIDENCE` — evidencia reproducible generada por OCM (nada en este documento).

---

## 1. Resumen ejecutivo

Este documento es una referencia interna derivada **exclusivamente** de documentación oficial de Bybit (API docs v5 `bybit-exchange.github.io/docs/v5/*` y Help Center `bybit.com/en/help-center/*`), consultada el 2026-08-14. No modifica arquitectura, no es un contrato, no autoriza implementaciones. Su propósito es sustentar investigación futura (R2–R6) y definir requisitos de conocimiento para backtesting y paper execution sobre derivados perpetuos.

Hechos núcleo verificados de fuentes oficiales:
- Bybit expone la familia completa de datos de mercado derivados vía REST/WS: tickers con `indexPrice`, `markPrice`, `fundingRate`, `nextFundingTime`, `openInterest`, `basisRate`; klines de precio, mark e index; histórico de funding; orderbook depth y full; trades públicos con `seq`; liquidaciones en streaming.
- El funding no es fijo: se recalcul cada minuto y se aplica en el `fundingRateTimestamp` (intervalo típico 8 h para los símbolos objetivo). La fórmula y sus límites están publicados (ver §4).
- La liquidación se dispara con **Mark Price** (no last price) en todos los modos de margen; el precio de quiebra cierra la posición. El mark price se calcula con fórmulas publicadas que mezclan index price, funding decay y basis del order book (ver §5).
- Los límites de orden y ejecución, tipos de orden (Market/Limit/conditional, TIF, reduce-only, post-only), el concepto de `slippageTolerance` para market orders y los límites de rate limit están publicados (ver §7 y §9).

**Puntos abiertos para OCM (no resueltos aquí):** tarifas de fee vigentes no confirmadas desde una página oficial de fee schedule (la página `bybit.com/en/fee-rate/` no renderiza para captura automatizada); parámetros de contract specs de ETH/SOL específicos; detalles del índice de precio por símbolo (composición/ponderación de exchanges); mecánica exacta de insolvency/ADL (solo señalada). Ver §12.

---

## 2. Especificación conceptual del perpetual

### 2.1 Qué es un perpetual linear
`CONCEPTUAL_KNOWLEDGE`: Un contrato de futuros perpetuos no tiene fecha de vencimiento. Los traders abren posiciones long/short con margen y apalancamiento; el precio de referencia es un índice spot global; la convergencia del precio del contrato con el índice se incentiva mediante el mecanismo de funding (pagos periódicos entre longs y shorts).

### 2.2 Categoría y símbolos
`OFFICIAL_BYBIT_FACT` — `GET /v5/market/instruments-info`, parámetro `category=linear` (cubre USDT perpetuals / USDT futures) y WS pública `wss://stream.bybit.com/v5/public/linear`. Símbolos de interés OCM (verificados habilitados en config OCM): `BTCUSDT`, `ETHUSDT`, `SOLUSDT`.

### 2.3 Campos del contrato (contract specs)
`OFFICIAL_BYBIT_FACT` — `GET /v5/market/instruments-info`. Campos devueltos por símbolo (ejemplo BTCUSDT, consultado 2026-08-14):
- `priceFilter.tickSize`: 0.10
- `lotSizeFilter.minOrderQty`: 0.001, `qtyStep`: 0.001, `maxOrderQty`: 1190
- `marketOrderParams.maxMktOrderQty`: 500
- `lotSizeFilter.minNotional`: 5
- `fundingInterval`: 480 (minutos = 8 h)
- `upperFundingRate` / `lowerFundingRate`: 0.00375 / -0.00375
- `leverageFilter`: min 1, max 100
- `priceFilter.priceLimitRatioX` / `priceLimitRatioY` (para límites de precio en market orders)
- `takerFeeRate` / `makerFeeRate`: expuestos por instrumento (los valores de los symbols target deben leerse del endpoint; no se consigna aquí un valor numérico como hecho, ver §9.1).

`OCM_RESEARCH_HYPOTHESIS`: Estos límites de tamaño son relevantes para paper execution (una estrategia que ordene qty > `maxOrderQty` será rechazada o dividida; ver §7.7).

---

## 3. Datos de mercado derivados disponibles (mapa REST/WS)

### 3.1 REST — `GET /v5/market/*`
`OFFICIAL_BYBIT_FACT` (por URL de cada endpoint):
- `tickers` — snapshot por símbolo/categoría con `lastPrice`, `indexPrice`, `markPrice`, `openInterest`, `openInterestValue`, `turnover24h`, `volume24h`, `fundingRate`, `nextFundingTime`, `basisRate`, `basis`, `bid1Price`, `ask1Price` (y más).
- `kline` — OHLCV del precio de contrato.
- `mark-kline` — OHLCV del mark price.
- `index-kline` — OHLCV del index price.
- `premium-index-kline` — OHLCV del premium index.
- `orderbook` (1/50 niveles) y `full-ob`.
- `recent-trade` — trades públicos con `execId`, `price`, `size`, `side` (lado del taker), `time`, `isBlockTrade`, `seq` (cross sequence).
- `history-fund-rate` — histórico de funding (`fundingRate`, `fundingRateTimestamp`).
- `open-interest` — OI histórico.
- `index-components` — componentes del índice por símbolo.
- `iv` — volatilidad histórica.

`OFFICIAL_BYBIT_FACT` — `GET /v5/market/funding/history` (misma página): parámetros `category` (linear/inverse), `symbol`, `startTime`/`endTime` (ms), `limit` [1,200]. Nota: pasar solo `startTime` devuelve error; pasar solo `endTime` devuelve 200 registros hasta ese instante; ninguno devuelve 200 hasta el momento actual. Cada símbolo tiene intervalo de funding distinto (consultar instruments-info).

### 3.2 WebSocket — tópicos públicos
`OFFICIAL_BYBIT_FACT` — `docs/v5/ws/connect` y páginas de cada tópico:
- `wss://stream.bybit.com/v5/public/linear` (mainnet) para USDT/USDC perpetuals y USDT futures.
- Tópicos: `orderbook.<depth>.{symbol}`, `publicTrade.{symbol}`, `kline.{interval}.{symbol}`, `tickers.{symbol}`, `allLiquidation.{symbol}`, `insurance.{symbol}`, `orderbook price limit`, `adlAlert`.
- Heartbeat: enviar `{"op": "ping"}` cada ~20 s; el servidor responde pong. Recomendado para mantener la conexión (se corta a los 10 min sin ping-pong ni datos).
- `max_active_time`: para private streams se puede configurar la duración de vida (30s–600s).
- Límites de conexión: no exceder 500 conexiones en 5 minutos por dominio WS.

`OCM_EVIDENCE` (contexto OCM): los producers WS en OCM (`ws_trades_source.py:85 TODO`) son en parte stubs; la integración real de estos tópicos es trabajo pendiente de investigación/implementación (fuera de alcance de R1).

---

## 4. Funding rate

### 4.1 Definición oficial
`OFFICIAL_BYBIT_FACT` — Help Center, *"Introduction to Funding Rate"* (`bybit.com/en/help-center/article/Introduction-to-Funding-Rate`):

> "The funding rate is not fixed. It is updated every minute."

El funding rate se compone de dos elementos:
- **Interest Rate (I):** la tasa base entre las monedas (quote/base).
- **Premium Index (P):** la diferencia entre el precio del contrato y el índice spot, medida con precios "impact" (no con el mid del book simple).

### 4.2 Fórmulas publicadas
`OFFICIAL_BYBIT_FACT` (misma página):

```
Funding Rate (F) = clamp[ P + clamp( I − P, 0.05%, −0.05% ), upper limit, lower limit ]
Premium Index (P) = [ Max(0, Impact Bid Price − Index Price) − Max(0, Index Price − Impact Ask Price) ] / Index Price
```

- Impact Bid/Ask Price = precio medio de ejecución necesario para llenar un **Impact Margin Notional** en el lado correspondiente del book (protege contra manipulación del mid).
- `I` y `P` se calculan cada minuto y se promedian por **TWAP de N horas** hasta el momento de funding.
- A mayor proximidad al timestamp de funding, mayor es el coeficiente del componente de premium index.
- El **funding fee** se aplica al **valor de la posición** en el timestamp de funding (`nextFundingTime`); los long pagan a los short si F>0 y viceversa.

`OFFICIAL_BYBIT_FACT` — `GET /v5/market/instruments-info`: para BTCUSDT, `fundingInterval` = 480 min (8 h) y `upperFundingRate`/`lowerFundingRate` = ±0.00375 (0.375 %).

### 4.3 Histórico
`OFFICIAL_BYBIT_FACT` — `GET /v5/market/funding/history` devuelve por símbolo `fundingRate` + `fundingRateTimestamp` (ms) por asentamiento.

`OCM_RESEARCH_HYPOTHESIS`: Para backtest de estrategias en perpetuals, el funding es un coste/ingreso recurrente que debe modelarse por settlement (8 h), no continuamente; la magnitud típica y su deriva requieren estudio empírico (candidato R2).

---

## 5. Mark Price e Index Price

### 5.1 Índice de precio (Index Price)
`CONCEPTUAL_KNOWLEDGE`: El index price es un agregado ponderado del precio spot en varios exchanges (designado para resistir manipulación de un solo venue).
`OFFICIAL_BYBIT_FACT` — `GET /v5/market/index-components` devuelve los componentes/ponderaciones por símbolo (a verificar por símbolo target; no se consigna composición aquí).
`OFFICIAL_BYBIT_FACT` — `GET /v5/market/tickers` expone `indexPrice` en tiempo real.

### 5.2 Mark Price (perpetual)
`OFFICIAL_BYBIT_FACT` — Help Center, *"Mark Price (Perpetual and Expiry Contracts)"* (`bybit.com/en/help-center/article/Mark-Price-Calculation-Perpetual-Expiry-Contracts`):

> "Bybit uses Mark Price as a trigger for liquidation and to measure unrealized profit and loss."

Para la mayoría de perpetuals:

```
Mark Price = Median( Price1, Price2, Last Traded Price )
Price1 = Index Price × [ 1 + Last Funding Rate × (Time Until Funding / 8) ]
Price2 = Index Price + MovingAverage( 2.5-min Basis )
   Basis = ( Bid1 + Ask1 ) / 2 − Index Price   (medido cada segundo)
```

Para una selección de perpetuals (fórmula alternativa publicada en la misma página):

```
Mark price = Price3 × C + Index price × (1 − C)
Price3 = Index price + MovingAvg(DeltaPrice)
DeltaPrice = (Bid1 + Ask1) ÷ 2 − Index price   (cada segundo)
C = clamp( DeltaPrice ÷ MaxDeltaPrice, 0.3, 0.7 )
MaxDeltaPrice = R-minute maximum basis (cada segundo, excluyendo el dato más reciente)
```

TradFi Perpetuals (no aplicable a los símbolos target, pero documentado):

```
TradFi Mark Price = clamp[ Perp MarkPrice, Index × (1 − 3%), Index × (1 + 3%) ]
```

**Reglas de degradación (fallback)** — `OFFICIAL_BYBIT_FACT` (misma página):
1. Si el index price de algún spot exchange es anómalo o no disponible → mark price se calcula con el last traded price de Bybit.
2. Si no hay datos suficientes para la MA de 2.5 min → mark price = last traded price de Bybit.

### 5.3 Uso en OCM (contexto)
`OCM_EVIDENCE`: En OCM, el provenance actual (`shared/kafka/provenance.py`) registra que `l_value`/`mark_price` se derivaron sin fuente y que CCXT solo entrega `ts`+`open_interest`. R1 confirma que Bybit expone mark/index/funding de forma nativa y con semántica publicada → es insumo para R6 (modelo de datos de derivados) y R2 (estudio de funding/mark).

---

## 6. Liquidación y margen

### 6.1 Mecánica general
`OFFICIAL_BYBIT_FACT` — Help Center, *"Trading Rules: Liquidation Process (Unified Trading Account)"*:
- **Isolated Margin:** la liquidación se dispara cuando el **Mark Price** alcanza el liquidation price de la posición. La liquidación de una posición no afecta a las demás.
- **Cross Margin / Portfolio Margin (UTA):** el riesgo se evalúa a nivel de cuenta; la liquidación se dispara cuando la **Account Maintenance Margin Rate (MMR) llega al 100 %**.
- El valor mostrado de "liquidation price" es el precio de disparo real en Isolated, pero en Cross es estimación/referencia (el disparo real es cuando MMR=100 %).

### 6.2 Precio de quiebra (Bankruptcy Price)
`OFFICIAL_BYBIT_FACT` — Help Center, *"Bankruptcy Price (USDT Contract)"*:
- Indica el nivel de precio en el que se pierde todo el margen inicial.
- Al liquidar, la posición se cierra en el bankruptcy price.
- Si el precio final de liquidación es mejor que el bankruptcy → el exceso va al **Insurance Fund**. Si es peor → el Insurance Fund cubre la diferencia.
- Isolated: `Bankruptcy Price (Long) = Entry Price × (1 − IMR)`; `Bankruptcy Price (Short) = Entry Price × (1 + IMR)`, con `IMR = 1 / Leverage`.

### 6.3 Laddered liquidation (reducción progresiva)
`OFFICIAL_BYBIT_FACT` — *"Trading Rules: Liquidation Process (UTA)"*:
1. **Cancelar órdenes activas** que aumentarían el tamaño de la posición (libera margen).
2. **Cierre parcial:** si aún no se cumple el maintenance margin, se cierra parcialmente con una orden **IOC** por la diferencia entre el valor actual y el del tier inferior.
3. **Cierre total:** si persiste el déficit, la posición se liquida y se cierra al bankruptcy price.

### 6.4 Margin / leverage
`OFFICIAL_BYBIT_FACT` — Help Center, *"Liquidation Price Calculation under Isolated Mode (Unified Trading Account)"* (fórmulas para USDT perpetual, USDC perpetual e inverse):
```
Position Value = Contract Quantity ÷ Mark Price
Initial Margin = (Position Value / Leverage) + Estimated Fee to Close Position
Maintenance Margin = (Position Value × MMR) − MM Deduction + Estimated Fee to Close Position
Estimated Fee to Close Position = Position Size / Avg Entry Price × (1 ± 1/Leverage) × Taker Fee Rate
Liquidation Price (Long) = [Position Size × (MMR + 1)] ÷ [ (Position Size ÷ Entry Price) + (Position Size ÷ Entry Price ÷ Leverage) + (Extra Margin ÷ (1 + Taker Fee Rate)) + MM Deduction ]
Liquidation Price (Short) = [Position Size × (1 − MMR)] ÷ [ (Position Size ÷ Entry Price) − (Position Size ÷ Entry Price ÷ Leverage) − (Extra Margin ÷ (1 − Taker Fee Rate)) − MM Deduction ]
```
- `MMR` depende del tier de risk limit del símbolo.
- **Auto-Margin Replenishment (AMR)** (Isolated, USDT/USDC perpetual): añade margen automáticamente desde el saldo disponible cuando el margen se acerca al umbral de liquidación, con tope en 1× apalancamiento. No garantiza evitar la liquidación.

### 6.5 Streaming de liquidaciones
`OFFICIAL_BYBIT_FACT` — `docs/v5/websocket/public/all-liquidation`:
- Tópico `allLiquidation.{symbol}`, push cada **500 ms**.
- Campos: `T` (updated timestamp), `s` (símbolo), `S` (lado de la posición: un `Buy` = se liquidó un long), `v` (tamaño ejecutado), `p` (bankruptcy price).

`OCM_EVIDENCE`: OCM ya dispone de clase `LiquidationsKafkaProducer` y tópico `liquidations.raw`; R1 confirma la semántica oficial de los campos que esa clase debe mapear.

---

## 7. Semántica de órdenes y ejecución

### 7.1 Tipos de orden
`OFFICIAL_BYBIT_FACT` — `POST /v5/order/create` (Place Order):
- `orderType`: `Market` | `Limit`.
  - **Market order:** se ejecuta al mejor precio del book de Bybit hasta completarse; el precio puede omitirse. El motor convierte la market order en una **IOC limit order** para proteger de slippage severo; si no hay entradas dentro del límite de slippage, no se ejecuta; si la liquidez es insuficiente, se cancela. El umbral de slippage es un porcentaje de desviación respecto del mark price.
  - **Limit order:** requiere `price` y `qty`.
- `timeInForce`: `GTC` (default), `IOC`, `FOK`, `PostOnly` (si se llenaría de inmediato → cancelada), `RPI` (solo market makers asignados).
- **Conditional orders:** si se pasa `triggerPrice`, la orden se convierte en condicional; no ocupa margen hasta que se dispara; si no hay margen suficiente al dispararse, se cancela. `triggerDirection`: `1` (sube a triggerPrice) / `2` (baja a triggerPrice). `triggerBy`: `LastPrice` | `IndexPrice` | `MarkPrice`.
- **TP/SL:** se pueden fijar al colocar la orden o modificar la posición; `tpTriggerBy`/`slTriggerBy`: `MarkPrice` | `IndexPrice` (default `LastPrice`).

### 7.2 Reduce-only y close-on-trigger
`OFFICIAL_BYBIT_FACT` — `POST /v5/order/create`:
- `reduceOnly=true`: la orden solo puede reducir la posición (obligatorio para cerrar/reducir). Si `reduceOnly=true` y `qty > maxOrderQty`, la orden se **divide automáticamente** en varias. No se puede combinar con TP/SL.
- `closeOnTrigger=true`: orden de cierre que solo reduce; si hay saldo insuficiente al dispararse, se cancelan/reducen otras órdenes activas del mismo contrato. Garantiza que el stop loss reduzca la posición aunque no haya margen disponible.

### 7.3 Slippage tolerance (market orders)
`OFFICIAL_BYBIT_FACT` — `POST /v5/order/create`:
- `slippageToleranceType`: `TickSize` | `Percent`.
  - TickSize: `precio máximo Buy = ask1 + slippageTolerance × tickSize`; `precio mínimo Sell = bid1 − slippageTolerance × tickSize`.
  - Percent: `precio máximo Buy = ask1 × (1 + slippageTolerance × 0.01)`; análogo para Sell.
- Rangos: TickSize [1,10000] entero; Percent [0.01,10] con 2 decimales. No aplica a TP/SL ni conditional orders.

### 7.4 Posición / modo
`OFFICIAL_BYBIT_FACT` — `POST /v5/order/create`:
- `positionIdx`: `0` one-way; `1` hedge-mode Buy; `2` hedge-mode Sell (requerido en hedge mode).
- Perps & futures siempre ordenan **por cantidad** (`qty`), no por valor.
- Cerrar posición completa: `qty="0"` + `reduceOnly=true` + `closeOnTrigger=true` cierra hasta `maxMktOrderQty` o `maxOrderQty` del símbolo.

### 7.5 Límites de órdenes activas
`OFFICIAL_BYBIT_FACT` — `POST /v5/order/create`:
- Perps & Futures: máx **500 órdenes activas** por símbolo; máx **10 órdenes condicionales** activas por símbolo por cuenta.

### 7.6 Confirmación de ejecución
`OFFICIAL_BYBIT_FACT` — `POST /v5/order/create`: la respuesta de create es solo **acuse de recibo (asíncrona)**; el estado real de la orden debe confirmarse vía WebSocket (private stream) o consulta de órdenes.

### 7.7 Implicaciones paper execution (por confirmar con datos reales)
`OCM_RESEARCH_HYPOTHESIS`:
- Límites de tamaño (`maxOrderQty`/`maxMktOrderQty`) → el paper executor debe split/verificar qty, no rechazar silenciosamente.
- Market orders usan IOC con slippage tolerance → medir slippage real y rechazo por límite requiere modelar el book (slippage de Bybit ≠ slippage de un backtest con mid price).
- `reduceOnly` y `closeOnTrigger` → el cierre de posición debe usar estas semánticas para no abrir/revertir la posición.
- Rate limits de orden (ver §9) → el executor debe respetar bucket de `create-order`.

---

## 8. Semántica de market data (timestamp / secuencia / actualización)

### 8.1 Trades (WS y REST)
`OFFICIAL_BYBIT_FACT` — `docs/v5/websocket/public/trade` y `GET /v5/market/recent-trade`:
- WS `publicTrade.{symbol}`: campos `T` (timestamp), `s`, `S` (side), `v` (qty), `p` (price), `L` (tickDirection), `i` (trade id), `BT` (block trade flag), `seq`. Hasta 1024 trades por mensaje.
- REST `recent-trade`: `execId`, `price`, `size`, `side` (lado del taker), `time`, `isBlockTrade`, `isRPITrade`, `seq` (cross sequence). Límites: spot [1,60]; otros [1,1000] (default 500).
- Archivo histórico de trades descargable: `bybit.com/en/derivative-activity/history-data`.

### 8.2 Order book (WS)
`OFFICIAL_BYBIT_FACT` — `docs/v5/websocket/public/orderbook` y `full-ob`:
- Snapshot + deltas (modelo de actualización diferencial). Profundidades disponibles vía `orderbook.{depth}.{symbol}`.

### 8.3 Klines
`OFFICIAL_BYBIT_FACT` — `docs/v5/websocket/public/kline` y `GET /v5/market/kline`:
- WS `kline.{interval}.{symbol}`: `start`, `end`, `interval`, OHLC, `volume`, `turnover`, `confirm` (flag de barra finalizada), `timestamp`.
- Endpoints REST adicionales: `mark-kline`, `index-kline`, `premium-index-kline`.

### 8.4 Tickers
`OFFICIAL_BYBIT_FACT` — `GET /v5/market/tickers` (y WS `tickers.{symbol}`):
- Expone conjuntamente `lastPrice`, `indexPrice`, `markPrice`, `fundingRate`, `nextFundingTime`, `openInterest`, `basisRate`, `basis`, `turnover24h`, `volume24h`. Útil como snapshot consolidado para backtests y monitoreo.

### 8.5 Semántica de tiempo
`OFFICIAL_BYBIT_FACT` — `GET /v5/market/time` (endpoint listado en la navegación de market data): el servidor de Bybit publica su tiempo; los timestamps de los tópicos son en ms UTC.

`OCM_RESEARCH_HYPOTHESIS`: para reconstruir un feed contiguo de trades/orderbook, la secuencia correcta es: `seq` (cross sequence) en trades, y snapshot+delta con verificación de `cts`/`seq` del orderbook; el `T` de liquidation es timestamp de actualización, no de ocurrencia exacta (misma granularidad que el push de 500 ms).

---

## 9. Fees y rate limits

### 9.1 Fees
`OFFICIAL_BYBIT_FACT` (parcial) — `GET /v5/market/instruments-info` expone `takerFeeRate`/`makerFeeRate` **por instrumento**; y Help Center *"Liquidation Price Calculation…"* usa `Taker Fee Rate` en la fórmula del estimated fee to close.
`UNVERIFIED` — La página de fee schedule pública (`bybit.com/en/fee-rate/`) no renderiza para captura automatizada en esta consulta; **no se consigna aquí ningún valor numérico de fee como hecho**. Los valores usados en research deberán leerse del endpoint de instruments-info (o verificarse manualmente) en el momento de uso.

### 9.2 Rate limits
`OFFICIAL_BYBIT_FACT` — `docs/v5/rate-limit` (Rate Limit Rules):
- Modelo de buckets por IP / clave API / endpoint; el límite se restablece en ventanas de tiempo (por ejemplo, buckets de 5 s / 1 min).
- Regla general: no exceder **600** requests por IP cada **5 s** (límite de IP general) y los límites por endpoint específicos se listan en la tabla de la página.
- WS: no exceder **500 conexiones en 5 minutos** por dominio WS; y límites de suscripción (longitud de `args` por conexión: ≤ 21.000 caracteres; spot hasta 10 args por request; sin límite de args declarado para futures/linear).
- Endpoints de mercado típicos (kline, orderbook, recent-trade, etc.) tienen buckets específicos (p. ej., 120 req/s para kline; 1000 req/5 min para recent-trade) — los valores exactos por endpoint se leen de la tabla `docs/v5/rate-limit`.

`OCM_EVIDENCE`: config OCM actual para Bybit: `max_concurrency: 8`, `max_rate: 15` (archivo de config de exchange); coherente con los buckets de IP, pero debe validarse contra la tabla vigente por endpoint al implementar ingestion de klines/trades.

---

## 10. Implicaciones para backtesting (no implementar aquí)

`OCM_RESEARCH_HYPOTHESIS` — requisitos de conocimiento que un backtester de perpetuals sobre datos Bybit debe incorporar (candidato R4):

1. **Mark vs Last vs Index:** el P&L no realizado y la liquidación se rigen por mark price; un backtest que use last price para liquidación sobreestimará/ subestimará el riesgo. Se necesita el mark price histórico (endpoint `mark-kline` / `tickers`).
2. **Funding periódico:** el coste/ingreso de funding se aplica por settlement (8 h típico). Modelar: `fundingRate × position value` en cada `fundingRateTimestamp`. Los datos históricos están en `history-fund-rate`.
3. **Liquidación:** simular con bankruptcy price + insurance fund + MM deduction; el disparo es mark price (isolated) o MMR de cuenta (cross). Para simplificación inicial: isolated margin por posición.
4. **Slippage y ejecución:** market orders en Bybit son IOC con slippage tolerance sobre ask1/bid1; limit orders respetan tickSize/minNotional/qtyStep; una simulación con precio de transacción = mid o last ignora el spread y el impacto.
5. **Fees:** usar `takerFeeRate`/`makerFeeRate` del símbolo (leer del endpoint en momento de uso; no hay valor canónico aquí, §9.1).
6. **Fallas de ejecución:** órdenes condicionales que se cancelan por margen insuficiente; `maxOrderQty` y split de `reduceOnly`.
7. **Continuidad 24/7:** el mercado de perpetuals no cierra; el backtester no debe aplicar cortes de sesión estilo spot salvo que el dato lo tenga.
8. **Tamaño del tick / granularidad:** los trades tienen `seq` y timestamps en ms; los klines tienen flag `confirm` para saber si la barra está cerrada.

---

## 11. Implicaciones para paper execution (no implementar aquí)

`OCM_RESEARCH_HYPOTHESIS` — requisitos de conocimiento que un paper executor sobre Bybit (testnet o mainnet) debe incorporar:

1. **Confirmación asíncrona:** create-order devuelve acuse; el estado real viene por WS private o consulta. El paper executor debe esperar confirmación, no asumir fill.
2. **Lógica de cierre:** usar `reduceOnly`/`closeOnTrigger` y `positionIdx` correcto para no revertir posiciones.
3. **Límites de orden y rate limit:** respetar 500 activas/símbolo y 10 condicionales/símbolo, y el bucket de create-order.
4. **Slippage/impacto:** en paper trading se mide el slippage implícito entre la señal y el fill (o el rechazo por slippage tolerance); con testnet el matching es sobre un book de testnet, no el real.
5. **Funding como coste:** el paper executor debe registrar el funding aplicado en cada settlement para no sesgar el P&L paper frente al real.
6. **Estados de mercado:** liquidaciones (stream) y ADL son eventos que pueden alterar posiciones de otras cuentas; para paper no aplican, pero sí para el diseño de research de eventos (R3).

---

## 12. Gaps, incertidumbres y notas de verificación

1. **Fees vigentes** (`UNVERIFIED`): el fee schedule público no fue capturable automáticamente. No hay valor numérico de fee consignado como hecho en este documento.
2. **Contract specs de ETH/SOL** (`needs_verification`): los números de BTCUSDT se leyeron del endpoint; ETH/SOL pueden diferir (tickSize, minQty, límites, funding interval). Re-verificar por símbolo.
3. **Composición del index price** (`needs_verification`): el endpoint `index-components` existe pero no se inspeccionó su contenido; la ponderación y lista de exchanges por símbolo no está consignada aquí.
4. **Mecánica exacta de ADL / Auto-Deleveraging** (`needs_verification`): solo señalado como tópico `adlAlert` en WS; su fórmula y condiciones no se desarrollaron aquí (fuera del alcance mínimo de R1; proponer ampliación si se requiere para R3).
5. **Mecánica de insolvency del Insurance Fund** (`needs_verification`): solo conceptual (el fund cubre el gap si el cierre es peor que el bankruptcy price).
6. **Settlements de USDC perpetual (Session Settlement):** mencionados en el Help Center para USDC; no aplican a los símbolos USDT target, pero si OCM amplía a USDC deben revisarse.
7. **RPI orders:** tópico `isRPITrade` y TIF `RPI` existen; su mecánica (Retail Price Improvement) no es relevante para los símbolos/finalidad actual de OCM, pero puede afectar la lectura de trades (marcar y posiblemente filtrar).
8. **Fechas/cambios:** la documentación oficial cambia; todo `OFFICIAL_BYBIT_FACT` de este documento tiene fecha de consulta 2026-08-14 y debe re-verificarse antes de uso operativo o de decisión arquitectónica.
9. **Fórmula de mark price "selected perpetuals":** no se confirmó si BTCUSDT/ETHUSDT/SOLUSDT usan la fórmula `Median(...)` o la fórmula `Price3 × C + ...`. `OCM_RESEARCH_HYPOTHESIS`: determinarlo empíricamente comparando mark de tickers con la fórmula (candidato R2).

---

## 13. Fuentes oficiales consultadas

Todas consultadas el 2026-08-14. API docs v5:

- https://bybit-exchange.github.io/docs/v5/market/instruments-info — Get Instruments Info
- https://bybit-exchange.github.io/docs/v5/market/tickers — Get Tickers
- https://bybit-exchange.github.io/docs/v5/market/kline — Get Kline
- https://bybit-exchange.github.io/docs/v5/market/mark-kline — Get Mark Price Kline
- https://bybit-exchange.github.io/docs/v5/market/index-kline — Get Index Price Kline
- https://bybit-exchange.github.io/docs/v5/market/premium-index-kline — Get Premium Index Price Kline
- https://bybit-exchange.github.io/docs/v5/market/orderbook — Get Orderbook
- https://bybit-exchange.github.io/docs/v5/market/recent-trade — Get Recent Public Trades
- https://bybit-exchange.github.io/docs/v5/market/history-fund-rate — Get Funding Rate History
- https://bybit-exchange.github.io/docs/v5/market/index-components — Get Index Price Components
- https://bybit-exchange.github.io/docs/v5/order/create-order — Place Order
- https://bybit-exchange.github.io/docs/v5/rate-limit — Rate Limit Rules
- https://bybit-exchange.github.io/docs/v5/ws/connect — WebSocket Connect
- https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook — Orderbook (WS)
- https://bybit-exchange.github.io/docs/v5/websocket/public/trade — Trade (WS)
- https://bybit-exchange.github.io/docs/v5/websocket/public/kline — Kline (WS)
- https://bybit-exchange.github.io/docs/v5/websocket/public/all-liquidation — All Liquidation (WS)

Help Center:

- https://www.bybit.com/en/help-center/article/Introduction-to-Funding-Rate
- https://www.bybit.com/en/help-center/article/Mark-Price-Calculation-Perpetual-Expiry-Contracts
- https://www.bybit.com/en/help-center/article/UTA-Trading-Rules (Trading Rules: Liquidation Process)
- https://www.bybit.com/en/help-center/article/Liquidation-Price-Calculation-under-Isolated-Mode-Unified-Trading-Account
- https://www.bybit.com/en/help-center/article/Bankruptcy-Price-USDT-Contract (Bankruptcy Price)
- https://www.bybit.com/en/help-center/article/Auto-Margin-Replenishment

---

## 14. Riesgos de interpretación

1. **Documentación oficial ≠ contrato operativo.** Este documento es una **referencia derivada**; no es un contrato OCM, ni un ADR, ni una decisión arquitectónica. Cualquier cambio de arquitectura que derive de esta información debe pasar por el mecanismo formal (ADR/BC/contrato).
2. **Números de contrato volátiles.** tickSize, límites de qty, funding interval, upper/lower funding y fees cambian con el tiempo y por símbolo. Ningún valor aquí debe ser tratado como constante de código.
3. **Fuentes de menor autoridad.** No se usaron blogs, Reddit ni terceros como autoridad; cualquier afirmación de este documento que no tenga URL oficial es `CONCEPTUAL_KNOWLEDGE` o `OCM_RESEARCH_HYPOTHESIS`, no hecho.
4. **No es evidencia de edge.** Nada en este documento constituye evidencia de que una estrategia de perpetuals tenga alpha en OCM; es conocimiento de mecanismo de mercado (base para investigación reproducible).
5. **Jerarquía.** Ante conflicto, prevalece: código/ADRs/contratos OCM > documentación oficial de Bybit > esta referencia > libros/literatura.
