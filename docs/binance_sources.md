# Binance 官方来源

## S-BMDR C1

- Spot WS 官方目录定义 `btcusdt@depth@100ms` 的 combined 连接与事件字段。C1 保留整个 combined payload，提取 BTCUSDT、交易所时间和序列范围。[Spot WebSocket Market Streams](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/ws-streams/~)
- Spot REST 官方目录列出公开 `GET /api/v3/exchangeInfo` 及 `symbol` 参数和 20 weight。C1 公开模式只请求 BTCUSDT，每小时一次，封存一份响应主体。[Spot REST General](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/general)
