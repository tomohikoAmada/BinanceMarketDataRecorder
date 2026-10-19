# R3 / G3 共享出口预算：离线 inventory 与待关闭项

2026-10-19。仅代码、官方文档与静态算术；没有 Binance API 调用或 VPS 操作。
此表完成 R3 的端点盘点/负面容量裁决，**BMD-RL-01 联合恢复资格仍 OPEN**。
Spot 与 USD-M 的 IP weight 分开，专门次数窗再单独累计。
官方来源、检索表示和 SHA 在 [来源记录](../milestone_evidence/r3-official-sources-20261019.json)。
REST 分钟额度6000/2400只是官方 exchangeInfo 示例/当前代码预算，正式准入必须读取真实
metadata 的 rateLimits 与 response used-weight，不把文档示例当永久额度。

## 当前启用端点和正常开销

| 市场/端点 | Recorder（10+10，全部功能） | Gateway（20） |
|---|---|---|
| Spot `/api/v3/depth` | limit1000，50 weight/snapshot，十产品全恢复500 | limit5000，250/snapshot，十产品全恢复2500 |
| Spot `/api/v3/exchangeInfo` | 逐产品，每3600s，共10×20 weight/h | 启动时一次共享全市场response，20 weight |
| USD-M `/fapi/v1/depth` | limit1000，20/snapshot，十产品200 | limit1000，十产品200 |
| USD-M `/fapi/v1/premiumIndex` | 每产品60s，10次/min | 无 |
| USD-M `/fapi/v1/openInterest` | 每产品60s，10次/min | 无 |
| USD-M `/fapi/v1/exchangeInfo` | 全局一次/3600s | 启动时一次共享response |
| USD-M `/fapi/v1/fundingRate` | 每产品300s，10次/5min，另有重试 | 无 |
| USD-M `/fapi/v1/fundingInfo` | 全局3600s，与 fundingRate 共享500次/5min/IP | 无 |
| USD-M `/futures/data/openInterestHist` | 每产品300s、500条上限；稀疏窗口可合法为空 | 无 |
| USD-M `/futures/data/takerlongshortRatio` | 每产品300s、分页补采 | 无 |
| USD-M `/futures/data/globalLongShortAccountRatio` | 同上 | 无 |
| USD-M `/futures/data/topLongShortAccountRatio` | 同上 | 无 |
| USD-M `/futures/data/topLongShortPositionRatio` | 同上 | 无 |
| USD-M `/futures/data/basis` | 同上（pair + PERPETUAL） | 无 |

后六项共60个五分钟 cursor。文档对其中五个统计路由标出1000次/5min/IP；
basis不能仅因weight0而推断无限次，联合准入先按更严格共同次数预算计算。
funding两接口另有独立窗口，不能用weight0绕过。
依据：[Spot depth](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/market#order-book)、
[Spot metadata](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/general#exchange-information)、
[USD-M market data](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data)。

## 可复算的准入与负面情境

| 情境 | 静态算术/裁决 |
|---|---|
| 两进程普通冷启动，各产品一次 snapshot | Spot 500+200+2500+20=3220 weight；USD-M depth400加metadata/侧请求。单次不等于一分钟内任意重试都可承受 |
| 一个 Spot Key 两进程同时恢复 | 50+250=300 weight；还必须合并其他Key与metadata |
| 全部Spot产品两轮恢复、同分钟metadata | 2×3000+220=6220>6000。模型负面条件成立；不能给每个进程独立一整份IP额度 |
| 正常五分钟side轮询 | 60次统计请求，加fundingRate10次及最多一个fundingInfo；冷启动补采、失败重试另外算 |
| 冷启动历史补采 | 每owner默认每attempt最多2页，各页释放共享lock；60 owners可产生120请求/attempt。lock不限制5分钟成功请求总数，不能证明不会跨越次数窗 |
| 短Live后反复恢复 | 连续失败计数可重置；有限指数退避不构成任何5分钟窗口的长期配额证明 |
| 429/418、Retry-After与退出 | 现有Spot与USD-M离线测试覆盖已排队请求二次冷却核查、较短冷却不能缩短已知期限、ban fallback、取消/stop时无新增wire调用；仍只证明进程内行为 |

Gateway生产路径 Spot 每产品一条combined连接承载三流（`g11/multi_market_runtime.cpp`、
`g4/spot_transport.cpp`），USD-M每产品一条depth。
Recorder Spot每产品三条、USD-M每产品三条核心加mark/liquidation两条。
因此常态候选 **Spot40、USD-M60，共100个逻辑连接**，不是102 REST owners或gRPC contexts。
蓝绿重叠与重试要另计连接尝试。Spot按300次/5min/IP预算时，八轮40连接尝试为320，
已超过该窗口；这是静态拒绝情境，不是实际发生的限流。
每连接客户端控制消息按Spot5/s、USD-M10/s分别核对；Binance下行行情不计为客户端控制消息。
[Spot WS](https://developers.binance.com/en/docs/products/spot/web-socket-streams)、
[USD-M WS](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect)。

## 收口边界

- [x] 端点/次数/权重/普通start/集中恢复/补采/WS尝试盘点，公开可复算负面情境。
- [x] 复用并执行真实Spot/USD-M进程内冷却、FIFO、429/418/Retry-After、取消和停止测试。
- [ ] R3-b/G3-b 共同确定成功请求窗口的静态分账与恢复错峰，在真实两进程路径覆盖
  同启、单Key/全Key恢复、短Live反复恢复及冷启动补采；若静态负荷仍不足再最小改进程内limiter。
- [ ] 用户确认最终产品名单后，用授权普通请求观测实际metadata/used-weight；不制造真实封禁。

不新增协调进程或Redis，不为了本机合成PASS降低14功能。
本轮没有改Gateway源码、生产限流或正在运行的R2环境；这里的OPEN是联合阶段的真实阻断。
