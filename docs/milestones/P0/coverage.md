# P0 首版公开数据覆盖表

核对日期：2026-10-26；保存表示取舍更新：2026-10-27（[ADR-0053](../../adr/0053-single-rest-representation-and-required-information.md)）。此表是 P2 的范围合同，不是已实现清单。P4 开始前将本表展开为实际产品/源清单并冻结；每个必须源分别进入连续性分母。P0 不发起生产采集。

官方依据：[Spot 流](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/ws-streams/~)、[Spot 市场 REST](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/market)、[Spot 通用 REST](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/general)、[USD-M REST](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data)、[USD-M 流](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public)、[COIN-M REST](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-coin-m-futures/api/rest-api/market-data)、[COIN-M 流](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-coin-m-futures/api/ws-streams/~)。抓取哈希与无效网页回退见 [official-sources.json](official-sources.json)；不能把返回 HTML 壳的 `.md` 地址当正文证据。

产品选择：配置是 underlying assets 列表，至少支持 10 项；首版 BTC、ETH。S 是 `baseAsset` 为所选标的且允许现货交易的所有所选报价对（含非 USDT 报价）；不把 BTC 作为 quote 的所有其他币种引进来。U 是这些标的在 USD-M 的永续与交割，C 是 COIN-M 的永续与交割，D 表示 U 和 C **分别实例化**。身份包含市场、symbol、pair、base/quote/margin、contractType、contractSize、上市/到期时间及状态，不能只以 symbol 或 `st=1` 判断。交易所没有某合约时记 `EXCHANGE_NOT_OFFERED`，附当次目录快照，不能凭想象订阅。尚未到期的已选产品一直纳入，不因故障而移除。

下表“字段”是所需信息的起点，P2 按具体源补齐映射、表示 / schema、适用 SDK 版本和已知不保留项；不能仅以 SDK 未建模为由删掉已要求的字段。WS 继续保留完整应用层 payload；REST 每个响应只保存一种主体表示，正文或能保留所需信息的 SDK 模型均可，不再统一要求未知字段、JSON 排版及 HTTP 字节原样留存。价格 / 数量精度、交易所 / 接收时间、产品身份、必要序列、缺口及有语义的 null / 缺失区别须保留。表内 REST “原文”指相应字段信息，不增加逐字节留存要求；“正文哈希”若用于派生去重，应明确哈希实际表示，不把模型摘要冒充 HTTP 正文摘要。

权限 P=无需密钥，K=官方要求 API key（不请求），A=账户/签名（排除）。周期是本项目拟采用的采样/轮询合同，括号内为官方发布频率；P2 须在共享预算下落实，不声称 REST 连续事件真值。`F` 为 REST 新鲜度：首次请求或上次成功 + 轮询周期 + 最多 10s 请求预算；过期起算缺口，失败不得刷新成功时间。历史/周期型源还要求对应已关闭数据周期，允许官方合法空值，按声明表示保留该次空观测及查询范围，不捏造零值。长期静态元数据允许内容相同，仍需新一次成功响应。

WS 缺口 W：断连/心跳失效、已接收未持久化停滞、无法解释的序列断裂，至有效恢复；只有观测上界时把上界前无法确认区间记 unknown。序列型深度须重新获得有效快照并接续后才恢复。稀疏流无新事件不判掉线。去重只用于派生层，Raw 不删除重复响应或重连重复帧。默认键前缀均含市场/产品；共享源使用自身 scope 并关联产品。

## 2026-10-27 信息优先级补充

[ADR-0053](../../adr/0053-single-rest-representation-and-required-information.md)与[计划的信息优先级](../../production_milestone_plan.md#信息优先级与资源不足时的顺序)区分完整目标和实际资源配置。下列53行保留最初目标，不改写历史证据；P2逐源补充优先级、保留字段、实际周期 / 频率及缩减原因。不能按假定研究策略删除独有事实。最低必需源及冻结配置内全部启用源按99.5%验收，未启用观察细节单列未采集，不视为覆盖成功。

S11/S12、D21–D23的全部未收盘更新仍是完整目标；受限配置可以明确减少部分周期的形成过程，原生收盘值及价格类型 / 日界等语义不能随意替代。D06未来可按同ADR评估“新增 / 变化成交 + 每次轮询记录”的有版本表示；收益不足就保持每次完整响应。此例外不自动扩展到WS或历史Raw。表中“正文哈希”是旧派生去重候选写法，不要求P2新增逐事件SHA；自然ID与字段比较足够时优先使用，必要摘要的用途和实际表示须明确。

## Spot

| ID | 市场/产品 | 源 | 频率 | 字段 | 共享范围 | 权限 | 派生去重键 | 缺口定义 | 必须/当前归宿 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| S01 | S | `/api/v3/exchangeInfo` | 启动、每小时；发现失败重试 | symbols、filters、permissions、status、rateLimits | 市场一次抓取后筛 base | P | 快照正文哈希+观测时间 | F，目录未知不算健康 | 是；已有按 symbol 调用，P2 合并目录 |
| S02 | S | `@depth@100ms` + `/api/v3/depth` | 100ms；启动/断序快照 | U/u、b/a、lastUpdateId | 产品 | P | update区间；快照请求ID | W，快照覆盖范围以外不推断 | 是；已有 |
| S03 | S | `@aggTrade` | 实时 | a/p/q/f/l/T/m | 产品 | P | a | W+可观测ID跳跃 | 是；已有 |
| S04 | S | `@trade` | 实时 | t/p/q/T/m | 产品 | P | t | W+可观测ID跳跃 | 是；P2，不能从 aggTrade 逆推逐笔 |
| S05 | S | `@bookTicker` | 最佳价量变动 | u/b/B/a/A | 产品 | P | u+正文哈希 | W；无全事件连续序号承诺 | 是；已有 |
| S06 | S | `@blockTrade` | 事件触发 | id/price/qty/time、买卖方向 | 产品 | P | block id | W；静默合法 | 是；P2 |
| S07 | S | `@referencePrice`；`/api/v3/referencePrice` | 流；REST 启动/恢复 | referencePrice、时间 | 产品 | P | 时间+正文哈希 | W/F，不能拿成交价替代 | 是（交易所适用时）；P2 |
| S08 | S | `/api/v3/referencePrice/calculation` | 启动、每小时 | 计算参数、组成与约束原文 | 产品/响应实际scope | P | 快照哈希+观测时间 | F；缺配置可解释但不能杜撰 | 是（适用时）；P2 |
| S09 | S | `/api/v3/executionRules` | 启动、每小时 | executionRules、价格范围参数 | 市场或批次一次 | P | 产品+规则快照哈希 | F | 是（适用时）；P2 |
| S10 | S | `@avgPrice` 或 `/api/v3/avgPrice` | 选择1000ms；REST仅恢复 | w/interval、价格、last trade时间 | 产品 | P | 观测时间+窗口 | W/F，窗口均价非最后成交价 | 是；P2 |
| S11 | S | `@kline_{interval}`（UTC）；`/api/v3/klines` | 1s interval每1000ms，其他每2000ms；保存未收盘更新；启动补历史 | k.t/T/i/o/h/l/c/v/q/n/V/Q/x | 产品+interval | P | openTime+更新时间+closed | W；缺 bar单独记范围 | 是；P2；历史导入不等于直播 |
| S12 | S | `@kline_{interval}@+08:00` | 同UTC推送频率；独立日界语义 | 同上及timezone | 产品+timezone | P | timezone+openTime+更新 | W | 是；P2，独立时区日界的原生更新须保留 |
| S13 | S | `@ticker` | 1s | 24h窗口、OHLC、数量、n、首末ID | 产品 | P | 窗口时间+正文哈希 | W | 是；P2 |
| S14 | S | `@ticker_1h` / `_4h` / `_1d` | 1s | 滚动窗口OHLC、v/q、首末时间 | 产品+window | P | window+窗口时间+哈希 | W，不以24h替代1h | 是；P2，三种窗口分别记录 |
| S15 | S | `/api/v3/ticker/tradingDay` | 60s，UTC日界 | 当日OHLC、成交数、首末时间 | 产品+timezone | P | 日界+响应时间+哈希 | F | 是；P2；与24h滚动不同 |
| S16 | S | `@miniTicker`、全市场 ticker/mini/rolling、REST ticker/price/bookTicker/24hr/ticker | 等价传输择一 | ticker字段子集、last price、BBO | 同信息家族一次 | P | 对应 S05/S13/S14 | 使用选定主源 W | 等价不重复必需；last price来自完整逐笔 |
| S17 | S | `@depth{5,10,20}` | 100/1000ms | Top-N 快照 | 产品 | P | S02 派生Top-N | S02；派生受重建质量约束 | 选择 S02，不把有限深度称全量订单簿 |
| S18 | S | `/api/v3/trades`、`/api/v3/aggTrades` | 仅启动/有界修复 | trade id或agg id及成交字段 | 产品 | P | 对应 S03/S04 | 超限无法修复区间保留缺口 | 等价补源，不另计必需流 |
| S19 | S | `/api/v3/historicalTrades`、`historicalBlockTrades` | 有界历史查找 | 历史成交/大宗字段 | 产品 | 当前Spot页面未列key；P2核对公开实样 | 对应成交ID | 有界历史超限仍保留缺口 | 等价历史补源；不能沿用U/C的key假设，无法补原接收时间 |
| S20 | S | `/api/v3/uiKlines` | 可视化修改的bar | UI bar | 产品 | P | 独立UI语义 | 不用其填原始kline缺口 | 非研究必需；明确排除UI展示变体 |

## USD-M / COIN-M 永续和交割

REST U 前缀 `/fapi/v1`，C 为 `/dapi/v1`；`/futures/data` 的 host 分别为 fapi/dapi。**不是将路径机械互换**：每行明确仅 U/C 的例外。连接路由遵循当日 [U Connect](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect.md) / [C Connect](https://developers.binance.com/en/docs/products/derivatives-trading-coin-futures/websocket-market-streams/Connect.md)。整合后的相同 payload 可能在两个 host 出现；按 `st`、目录身份和pair路由，同一发布只选一个主源，不双算覆盖。

| ID | 市场/产品 | 源 | 频率 | 字段 | 共享范围 | 权限 | 派生去重键 | 缺口定义 | 必须/当前归宿 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| D01 | U/C | `exchangeInfo` + `!contractInfo` | REST启动/小时；WS生命周期事件 | symbol/pair、st、contractType/Size、base/quote/margin、deliveryDate/status | 市场目录一次；事件按产品关联 | P | 市场+symbol+版本时间 | F/W；新增产品尚未订阅记未覆盖 | 是；U REST已有，其他P2 |
| D02 | U/C | `@depth@100ms` + `depth` | 100ms；启动/断序快照 | E/T/U/u/pu、b/a、st/ps、lastUpdateId | 产品 | P | 更新区间/前序 | W，快照接续前无有效L2 | 是；U永续已有，C/交割P2 |
| D03 | U | `@rpiDepth@500ms` + `rpiDepth` | 500ms；启动/恢复快照 | 含RPI价格层、U/u/pu等原文 | 产品 | P | 独立RPI更新区间 | W，普通depth排除RPI不能替代 | 是；P2 |
| D04 | C | RPI 专用源 | 官方 C 目录未列 | 无可确认专用接口 | 产品 | 无已确认源 | 不适用 | EXCHANGE_NOT_OFFERED | 不伪造；P2复核当时官方变化 |
| D05 | U/C | `@aggTrade` | 100ms聚合 | a/f/l/p/q/T、nq(若提供)、st | 产品 | P | a | W+可观测ID跳跃；不逆推个别成交 | 是；U永续已有 |
| D06 | U/C | `trades` | 每1s有界轮询（最近成交列表） | id/p/q/quoteQty/time/isBuyerMaker | 产品 | P | id；Raw保存每次响应 | F+ID跳跃；窗口溢出永久缺口 | 是；P2；官方未列独立 `@trade`，不能虚构 |
| D07 | U/C | `historicalTrades` | 有界历史回补 | 逐笔成交字段 | 产品 | K | id | 无钥不能任意回补 | 不调用；D06及官方历史trades文件只补交易所时间 |
| D08 | U/C | `@bookTicker` | 实时价量变动 | E/T/u/b/B/a/A、st/ps | 产品 | P | u+正文哈希 | W；不存在全市场事件真值 | 是；U永续已有 |
| D09 | U/C | `@markPrice@1s`；`premiumIndex` | 1s；REST60s | mark/index/estimatedSettle/fundingRate/nextFundingTime、st | 产品；pair index关联同标的合约 | P | 产品+发布时间+哈希 | W/F；交割无funding不能强填 | 是；U永续已有 |
| D10 | C | `{pair}@indexPrice@1s` | 1s | indexPrice、E、pair | pair一次 | P | pair+E+哈希 | W，不能假定U mark中的index与C完全相同 | 是；P2 |
| D11-U | U | `@forceOrder` / 等价全市场发布 | 1000ms代表性快照；选择规则存在官方冲突 | o.s/S/o/f/q/p/ap/X/l/z/T、st | 产品；全市场传输只选一次 | P | 观测身份（来源/连接/chunk/ordinal）；Raw不去重，不按最新/最大假设合并 | W；无事件可合法静默；非每笔全集 | 是；U已有原文采集；参考页latest与2026-04-10公告largest冲突，语义未解决，见下文 |
| D11-C | C | `@forceOrder` / 等价全市场发布 | C参考页：1000ms代表性快照 | 原文含o及ps/st等实际字段 | C产品；不以U代替C身份 | P | 观测身份（来源/连接/chunk/ordinal）；不据U规则合并 | W；稀疏流，不证明每笔完整 | P2；C参考页称latest，没有此次U公告对C适用的证据；合同不依赖latest/最大选择假设 |
| D12 | U/C永续 | `fundingRate` | 300s有界补查；发布间隔依元数据 | symbol/fundingTime/rate/markPrice | 产品 | P | fundingTime+内容 | F；事件周期按配置不固定8h | 是；U已有，交割不适用 |
| D13 | U/C永续 | `fundingInfo` | 启动/小时 | cap/floor/intervalHours等调整 | 市场一次，稀疏清单 | P | 产品+快照哈希+时间 | F；空表是合法新观测，不捏造默认调整 | 是；U已有，C P2 |
| D14 | U/C | `openInterest` | 60s | symbol/pair、OI、time、contractType | 产品 | P | 产品+响应time+内容 | F，无前向填充 | 是；U永续已有 |
| D15 | U/C | `/futures/data/openInterestHist` | 300s；已关闭5m | sumOpenInterest/Value、timestamp；C按pair/contractType | U产品；C查询scope共享一次 | P | scope+period+timestamp | F+闭周期；官方稀疏空窗保留evaluated范围 | 是；U已有，C P2 |
| D16 | U/C | `/futures/data/takerlongshortRatio` (U) / `takerBuySellVol` (C) | 300s，5m | buy/sell volume、ratio、timestamp | U产品；C pair/contractType | P | scope+period+timestamp | F+闭周期/查询边界 | 是；U已有，C P2 |
| D17 | U/C | `/futures/data/globalLongShortAccountRatio` | 300s，5m | long/shortAccount、ratio、timestamp | U产品；C pair+contractType（如参数提供） | P | scope+period+timestamp | F+闭周期 | 是；U已有，C P2 |
| D18 | U/C | `/futures/data/topLongShortAccountRatio` | 官方支持5m等周期 | top账户多空比例、timestamp | U产品；C pair+contractType（如参数提供） | U官方当前要求K；C页面未列key | scope+period+timestamp | U不得声称无钥覆盖；C按F+闭周期 | U受限并显式排除无钥首版；无等价公开替代；C P2按公开接口实现 |
| D19 | U/C | `/futures/data/topLongShortPositionRatio` | 官方支持5m等周期 | top持仓多空比例、timestamp | U产品；C pair+contractType（如参数提供） | U官方当前要求K；C页面未列key | scope+period+timestamp | 同D18，不能以global ratio伪装top | U受限；C公开时必须；现有U无钥代码不证明可访问 |
| D20 | U/C | `/futures/data/basis` | 300s，5m | pair、contractType、basis/rate、index/futures价、timestamp | pair+contractType一次 | P | pair+contractType+period+timestamp | F+闭周期；交割/永续各自独立 | 是；U永续已有，其余P2 |
| D21 | U/C | `@kline_{interval}`；REST `klines` | 250ms（有更新时）；保存未收盘和收盘 | OHLC/V/Q/n、t/T/x | 产品+interval | P | openTime+更新time+closed | W；closed bar缺口 | 是；P2 |
| D22 | U/C | `{pair}_{contractType}@continuousKline_{interval}`；`continuousKlines` | 250ms（有更新时） | 连续合约kline、pair/contractType | pair+contractType | P | 连续类型+openTime+更新 | W；换月语义不能以单合约替代 | 是；P2 |
| D23 | U/C | REST `markPriceKlines`/`indexPriceKlines`/`premiumIndexKlines`；C另有mark/index kline流 | 闭bar后60s内REST补查；C原生mark/index kline250ms | 各价格OHLC、open/close time | mark/premium产品；index pair一次 | P | 价格类型+scope+openTime | F+闭bar；每秒样本不能还原bar极值 | 是；P2；保留各类型独立字段 |
| D24 | U/C | `@ticker` | 官方周期更新（通常500ms） | 24hOHLC、成交量/数量、时间 | 产品 | P | 窗口+发布时间+哈希 | W | 是；P2 |
| D25 | U/C | miniTicker/全市场ticker、bookTicker聚合、REST ticker/price(v1/v2)/24hr/bookTicker、Top-N depth | 对应主源等价传输 | 对应D02/D08/D24子集、last价 | 只选产品级主源；不重复全市场 | P | 主源语义键 | 主源W | 等价不另必需；不能混淆RPI版本 |
| D26 | U/C | `constituents`；U另 `indexInfo`/`@compositeIndex` | 成分REST启动/小时；U复合指数流1000ms | 组成exchange/symbol/weight、成分价格、指数定义 | index或pair一次，映射产品 | P | scope+时间+正文哈希 | F/W；普通index价不能替代组成权重 | 是（所选产品适用时）；P2 |
| D27 | U | `assetIndex` 或 `!assetIndex@arr` | 选官方流（约1s）；REST恢复 | asset/index/bid/ask及整合后结算资产信息 | 全局一次，关联所选margin资产 | P | asset+发布时间+哈希 | W/F | 是；P2，不能按每产品重复抓取 |
| D28 | U | `symbolAdlRisk` | 1800s轮询（官方30分钟更新） | symbol/adlRisk/updateTime原文 | 产品/可批量scope | P | 产品+响应时间+哈希 | F | 是；P2，区别账户adlQuantile |
| D29 | U | `insuranceBalance` | 60s查询（发布快照时间独立保留） | 保险基金余额/资产/时间原文 | 真实fund/pool scope一次 | P | pool+发布时间+内容 | F；新响应与数据发布时间分别判断 | 是；P2，勿假造C同名端点 |
| D30 | C | 保险基金/公共ADL独立REST | 当前C公开目录未列对应接口 | 无 | 全局/产品 | 无已确认源 | 不适用 | EXCHANGE_NOT_OFFERED | 不要求不存在端点；U全局共享若实际包含C可关联并标来源 |
| D31 | U交割 | `/futures/data/delivery-price` | 到期后每60s查到发布，之后封存 | pair、deliveryTime、deliveryPrice | pair+到期日 | P | pair+deliveryTime | 到期到发布期限；缺发布明确unknown | 是（实际提供交割时）；P2；C未列同名源不得照搬 |
| D32 | U | `tradingSchedule`/`tradingSession` | 交易时段元数据/事件 | sessionType、交易日历 | TradFi标的 | P | 产品+session时间 | 不适用于BTC/ETH加密标的 | 不适用；未来扩展到TradFi才纳入 |

## 共用边界与不能覆盖的事实

- Spot/U/C `ping`、`time` 为健康/时钟辅助，启动和每60s可观测；不是独立市场数据家族，也不代替持久化进度。WebSocket API 的同名 depth/ticker、JSON/SBE/FIX 是传输候选；只选择无钥 JSON 公开源。SBE/FIX 需要密钥/会话时不引入认证依赖。
- 各市场 aggTrades REST 是流的补源；官方日/月 kline、aggTrades、trades 与适用 funding 档案由已有 backfill 独立导入，使用 archive-source 时钟，不能抹去实时接收缺口。Kline的interval集合按各市场官方枚举展开（Spot包括1s；U/C以各自枚举为准），UTC和+08:00独立日界、连续合约和不同价格类型不能相互冒充。若同一interval的REST最终bar与已收完整原生流等价，可仅用作补源；离线长周期聚合不冒充原生未收盘推送。独立价格类型、滚动窗口及连续合约语义以上均保留。
- Spot UIKlines 是显示变体；成交/挂单账户接口、private user streams、账户ADL量化、保证金/杠杆/手续费个人配置、订单、资金划转、期权、期权大宗、Alpha、股票等独立产品不属于本次范围。公共元数据中的交易规则仍保留。
- U/C 清算流发布代表性快照；具体选择语义按下文分别披露，不能声称每笔全集。最近成交REST是有限窗口；本项目不能承诺捕获被交易所省略的事件。盘口是L2，非L3、非所有隐藏/RPI订单的完整队列。
- 权限是当前官方合同，SDK方法存在或历史成功不改变当前要求。U top两种ratio的缺失**没有无钥等价替代**，必须在P2/交付报告保留受限标记；global ratio、官方历史包都不能冒充top ratio。若新官方合同解除权限限制，再在P2记录依据后纳入，不私加key。
- REST 现实现保存 SDK model provenance 的来源继续如实标注模型边界。P2 按 ADR-0053 实现单份主体、所需字段及必要状态 / headers；SDK 模型满足字段与精度约定即可计入覆盖，不以取得所有原始 HTTP 正文为前提。现货盘口模型 / Base64 重复表示在 P2 去掉，变化的 payload 明确版本并保留旧 reader；历史记录不改写。
- 运行前的源合同须固定：source_id、完整产品或共享scope、选定传输、请求参数、发布/轮询周期、成功期限、合法空值、数据周期、新鲜度、恢复条件。上表中“官方周期”的流须在P2按具体endpoint参数展开；不能用一个统一超时替代所有语义。实现/官方实样证明在P2逐行填写，P0只完成定义与归宿。

## D11 的官方语义冲突（R4，2026-10-26）

U参考页在单产品和全市场清算流都仍使用“latest”；[官方USD-M changelog](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/change-log) 的2026-04-10条目（2026-04-14生效）则改述为“largest”。两者均已重新核对；changelog响应SHA与原研究相同。此处**未解决交易所实际选择规则**，不能任选其一当确定合同。

C的[独立参考页](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-coin-m-futures/api/ws-streams/~)也写latest，但上述改述明确属于USD-M，不能据此替C断言largest，亦不把U冲突宣称为已经发现C同样变更。U/C分别保留原文、来源、接收时间、连接与物理ordinal，按交易所实际发布的代表性快照解释。未取得一致资料前，不将最新/最大假设用于完备性、去重、总清算数量或派生统计；不能模拟未发布的清算全集。当前仍只承诺采集已发布消息，不承诺每笔清算完整。

[R4来源记录](corrections-20261026/official-sources.json)列出检索时间、可取得的正文摘要及表示限制。此次把同一家族D11按U/C拆为两行，因此现表仍为52个家族、53行，并未增加市场范围。该合同披露不要求重跑VPS；本次新增测量是为R1–R3校验脚本修正。

## P2 implementation and field mapping — 2026-10-27

The original target and P0 evidence above remain unchanged. P2's concrete source
planner is `binance/public_sources.py`; selected-base discovery is
`binance/products.py`, and runtime ownership is `collector/coverage.py`.
[ADR-0054](../../adr/0054-selected-asset-capture-and-public-observations.md)
defines the additive product/shared identities and versioned observation formats.
[P2 result](../P2/README.md) records actual samples, exceptions and costs.

For every selected source, `Source.public_dict()` records its market/scope,
request parameters, exact native intervals, cadence, priority, required fields,
representation, adapter schema and SDK versions. A directory observation records
the expanded source plan in Catalog. The default tries every interval above;
explicit interval subsets, quote filters and admission budgets remain visible.
New/changed products are admitted under these bounds; missing or failed sources
are not retrospectively removed. U/C weight shares one process/IP budget.

WS mapping below is into the **unchanged received combined message** `data`;
metadata is extracted separately without reencoding the raw bytes. `W1` means
`utf-8-json-combined.v1` / `binance.public.websocket.v1`, generic websockets 15.0.1,
no SDK model conversion. `J1` means `binance-public-rest-provenance.v1`, one
`sdk-parsed-json.v1` body under `response.model`, through Spot SDK 10.0.0's common
unsigned transport (the exact common version is in every record and source plan).
It preserves supplied JSON strings, integers and null/missing distinctions; it
makes no unknown-field/HTTP-format promise. `B1` means one exact SDK-received body,
Base64 only, with decimal JSON numbers exposed as exact text by the reader.
Legacy Spot snapshot v3 also selects B1 when available, otherwise one model;
historical v1/v2 records still read. All clocks and request/status/header evidence
are retained. Model conversion is not described as original HTTP.

| ID | Implemented mapping / source selection | Version, cadence and priority | Evidence / explicit boundary |
| --- | --- | --- | --- |
| S01 | `response.model.symbols[*]`, filters/permissions/status/rateLimits; filter selected base after saving one market response | J1; startup/hour; unique metadata | Live selected-directory fixture; all quote pairs, not quote-asset expansion |
| S02 | WS `data.U/u/b/a`; REST lastUpdateId/bids/asks; snapshot sequence reader | W1 + J1 (expanded), legacy B1 v3; 100ms + on-demand; unique | Live bytes, independent Raw read, old/new snapshot tests |
| S03 | `data.a/p/q/f/l/T/m` | W1; venue publication; unique | Live Spot sample; no inversion to individual fills |
| S04 | `data.t/p/q/T/m` and all supplied fields | W1; real time; unique | Live Spot sample |
| S05 | `data.u/b/B/a/A` | W1; best-price changes; unique | Live Spot sample; no global sequence guarantee |
| S06 | `data.e/E/s/t/p/q/T/m` (native `t`, not invented block ID) | W1; sparse real time; unique | Official block-trade example; 15s sample had no block event |
| S07 | WS `data.r/t/s` including null; REST symbol/referencePrice/timestamp | W1 + J1 startup; 1s; unique | Live WS/REST sample; explicit null regression |
| S08 | model symbol/calculationType/bucketCount/bucketWidthMs and supplied parameters | J1; startup/hour; unique | Live public sample |
| S09 | model execution rules for selected-symbol batches | J1; startup/hour; unique | Live public batch sample; one request per <=100 symbols |
| S10 | `data.i/w/T/E` and supplied fields | W1; 1s; unique | Live average-price sample; WS reconnect is primary recovery |
| S11 | `data.k` including t/T/i/OHLC/v/q/n/V/Q/x; REST native array positions retained | W1 + J1 startup seed; every native interval, 1s/2s updates; forming bars | Full-interval native-bar sample, UTC |
| S12 | Same native kline fields, distinct `@+08:00` wire identity and REST timeZone=8 | W1 + J1 seed; every native interval; forming bars | Live timezone sample; no UTC substitution |
| S13 | Native 24h ticker fields including time/volume/count/first-last IDs | W1; 1s; native statistics | Live Spot ticker |
| S14 | Separate `ticker_1h`, `ticker_4h`, `ticker_1d` payloads | W1; 1s each; native statistics | Live three-window sample |
| S15 | model native trading-day OHLC/count/open-close clocks, UTC timeZone=0 | J1; 60s; native statistics | Live public sample; distinct from rolling ticker |
| S16 | Select S04/S05/S13/S14 primary publications | Equivalent, not additional enabled sources | No duplicate all-market/mini/ticker transports |
| S17 | Select S02 depth snapshot+diff | Equivalent, not additional enabled sources | No claim of L3 or a full hidden-order book |
| S18 | Existing historical import/selected live trade primary; recent/agg repair alternatives are not independent streams | Equivalent repair, not separately enabled | No promise that a live receive gap is erased by historical data |
| S19 | Historical/block lookup is an equivalent repair alternative, not an enabled source in this candidate | Not additionally implemented/enabled | New block information uses S06; no fabricated key requirement or repair success |
| S20 | UI-only modified bars excluded | Explicit non-goal | Not used to fill native bars |
| D01 | Full exchangeInfo plus one merged `!contractInfo`; directory Product maps pair/base/quote/margin/type/size/onboard/delivery/status | J1 hourly + W1 lifecycle; unique | Live U/C directories; shared UM/CM lifecycle transport once |
| D02 | `data.E/T/U/u/pu/b/a/st/ps`; model lastUpdateId/bids/asks | W1 + J1 on-demand; 100ms; unique | Live U/C perpetual/dated samples; st/pair regression |
| D03 | Separate `rpiDepth` sequence and snapshot model | W1 + J1; 500ms + on-demand; unique | Live U RPI stream and valid limit=1000 snapshot; independent sequence bridge |
| D04 | No confirmed dedicated C REST snapshot/RPI pair in the current C catalog | Not offered/confirmed as a C-specific source | Integration notice's st field does not invent an undocumented C snapshot |
| D05 | `data.a/f/l/p/q/T/m`, nq and st when supplied | W1; 100ms; unique | Live U/C aggregate trades |
| D06 | Native trade id/price/qty/time/isBuyerMaker; U quoteQty or C baseQty retained; direct field equality | Compact v1 by default; 1s; unique | Live U/C responses; matched CPU/size comparison; late IDs, changes, empty/failure, overflow, eviction/restart tests |
| D07 | Key-restricted historical lookup not called | Permission-limited, excluded | No credentials or substitute completeness claim |
| D08 | `data.E/T/u/b/B/a/A/st/ps` | W1; best-price changes; unique | Live U/C perpetual/dated samples |
| D09 | Native mark/index/estimated-settle/funding/next-funding fields, including dated empty/zero values | W1 1s + J1 60s; unique | Live U/C WS/REST; no funding values invented for delivery |
| D10 | C pair `data.i/E/p`, collected once per pair | W1; 1s; unique | Official index-price example; 15s live sample received no event, no coverage success inferred |
| D11-U | Entire representative forceOrder observation, connection/group/physical ordinal | W1; sparse representative publication; unique | Live U observation; latest/largest conflict remains unresolved |
| D11-C | Entire native `data.o` plus supplied root fields | W1; sparse representative publication; unique | Official C example; no U sampling-rule substitution |
| D12 | model symbol/fundingTime/fundingRate/markPrice | J1; 300s, perpetual only; unique | Live U/C response |
| D13 | Sparse model symbol/cap/floor/fundingIntervalHours records; legal empty retained | J1; startup/hour, once per venue; unique | Live U/C response |
| D14 | model symbol/pair/openInterest/time/contractType as supplied | J1; 60s; unique | Live U/C response |
| D15 | model sumOpenInterest/sumOpenInterestValue/timestamp; C pair+contractType | J1; closed 5m query/300s; unique | Live U/C response, bounded request range retained; no dense fill |
| D16 | U buy/sell volume/ratio versus C takerBuyVol/takerSellVol and supplied fields | J1; closed 5m query/300s; unique | Distinct endpoint/parameter maps, live U/C samples |
| D17 | model longShortRatio/longAccount/shortAccount/timestamp; C pair-only scope | J1; closed 5m query/300s; unique | Live U/C samples; no invented C contractType parameter |
| D18 | C top-account model ratio/account/timestamp; U excluded | J1 C only; closed 5m/300s; unique | C live success; U key restriction remains disclosed |
| D19 | C top-position model ratio/account/timestamp; U excluded | J1 C only; closed 5m/300s; unique | C live success; global ratio is not a replacement |
| D20 | model pair/type/basis/rate/index/futures/annualized/timestamp | J1; pair+contract-role once, closed 5m/300s; unique | Live U/C samples; roles remain distinct |
| D21 | Native per-symbol kline payload plus bounded native REST seed | W1 + J1; all 15 futures intervals, 250ms when published; forming bars | Full-interval sample; no locally aggregated replacement |
| D22 | Native ps/ct/k continuous bars per pair+contract role | W1 + J1 seed; all 15 intervals, 250ms; forming bars | Live continuous samples; role scopes and directory changes retained |
| D23 | Distinct mark/index/premium native arrays; C mark/index kline streams; index scope once per pair | W1 + J1, query each native closed period within 60s, all intervals; native/forming bars | U/C successful samples except **C dated premium**: endpoint returns -1121; unresolved applicability, kept failed/uncovered, never replaced with perpetual bars |
| D24 | Full native 24h ticker fields | W1; 500ms; native statistics | Live U/C samples |
| D25 | Select D02/D08/D24 primary publications | Equivalent, not additionally enabled | RPI is not folded into ordinary depth |
| D26 | model constituents exchange/symbol/price/weight; indexInfo and compositeIndex only for directory INDEX products | J1 hour + applicable W1 1s; unique | Live U/C constituents; BTC/ETH composite-index sources not applicable |
| D27 | Whole merged assetIndex array, once globally | W1; ~1s; unique | Official integration contract; no per-product duplicate |
| D28 | model symbol/adlRisk/updateTime | J1; 1800s; unique | Live unsigned sample; never account adlQuantile |
| D29 | Whole insurance pool symbols/assets/balances/times response | J1; 60s, global pool once; unique | Live unsigned sample; no C endpoint invented |
| D30 | No C-specific insurance/ADL endpoint assumed | Not offered/confirmed separately | U's actual shared response may be associated with its listed pool |
| D31 | Exact numeric-price body; deliveryTime/price, pair and request/receive clocks | B1; 60s only after delivery until observed publication; unique | Live body fixture; high-precision numeric regression; directory lifecycle keeps due deliveries |
| D32 | TradFi schedule/session excluded for selected crypto products | Not applicable | No account or TradFi scope expansion |

The source planner's configured native intervals are complete **targets**, not a
claim that every stream emitted within the short sample. The 30s sample observed
130 of 197 configured native-bar streams; others remain unobserved in that sample.
No source is dropped after failure, and no 99.5% continuity is claimed here.
Current full BTC/ETH discovery also produces a REST demand above the existing
reserved request share; the explicit limiter waits and reports overdue sources.
This is a qualification/resource issue for P3/P4, not permission to silently shed
D06 or to describe a smaller workload as a code speedup.

### P2 measured D06 / daily supplement update — 2026-10-28

The full53-row target above and its original samples remain unchanged. Current
[D06 configuration and losses](../P2/closeout-20261028/measured-budget.json) explicitly
use2s for BTCUSDT/ETHUSDT UM perpetual,3s for BTCUSDC/ETHUSDC UM perpetual and30s for
other selected derivatives. The entire family/products remain; finite windows can
miss individual observations. Generic1s stays available; no dynamic shedding.
Official derivative daily trades are a separate delayed supplement, not keyed REST
historicalTrades, live receive recovery or all-field equivalence. Native id/price/qty,
UM quote_qty or CM base_qty, millisecond time and maker flag are typed without float
loss. isRPITrade and live observation/timing are unavailable.17 actual files parsed;
8 imported/read independently; two CM next-quarter files404 and file ID holes remain
unknown, not zero activity. [Reader and conflict rules](../../adr/0054-selected-asset-capture-and-public-observations.md#implementation-of-the-approved-daily-supplement-2026-10-28)
keep equal overlapping facts once and conflicting source records separate.
D23 now has observed failures for all8 tested U/C dated premium symbols. This adds
UM to the R2 unknown/failed applicability boundary; do not drop them or substitute a
perpetual. P2 is not passed until applicability and independent review are resolved.

### D23 R2 follow-up — 2026-10-29

[New bounded evidence](../P2/r2-20261028/README.md) confirms the same eight dated
identities are currently TRADING. Native HTTP premiumIndexKlines still fails for
all eight; four perpetual controls succeed, and two explicit closed-range queries
also fail. The official Python method parameters do not supply an alternate dated
identity, and official documentation still lacks an applicability rule. This remains
**UNKNOWN / failed**, not exchange-not-offered or an equivalent shared/perpetual series.
All 120 affected interval sources and retry costs remain. No implementation boundary
changes until the [official clarification](../P2/r2-20261028/binance-clarification-draft.md)
has an authoritative answer. Historical measurements and scope rows above are retained.
