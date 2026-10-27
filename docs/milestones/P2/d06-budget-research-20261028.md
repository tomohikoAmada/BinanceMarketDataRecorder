# D06 请求预算与成交保留方案研究

日期：2026-10-28（上海）；检索发生于 2026-10-27 UTC。
审查对象：`e73068b`。本文是建议及修正输入，**不批准或启用 6 秒采样**。
当前默认仍为 1 秒；研究用途不决定 Recorder 能记录哪些市场事实。

## 结论与预算复核

不建议直接在“全产品 1 秒”与“全产品 6 秒”之间二选一。应先比较按产品分配
轮询频率，并核验官方日度逐笔文件能否作为独立的延迟补充。它们都不能保证
消除所有信息损失；改变实时观察范围仍需 owner 决定。

当前保存的目录有 50 个 TRADING 产品，其中 19 个合约产品。直接用审查提交的
`source_plan` 复算得到 2,154 个 WS 订阅、2,627 个 REST 源/seed。
两种配置的名义需求与原交付估算一致：

| 轮询候选 | D06 weight/min | 其余名义常驻请求 | 合计 | 结论 |
| --- | ---: | ---: | ---: | --- |
| 19 产品均 1 秒 | 5,700 | 128.653 | 5,828.653 | 不可持续 |
| 19 产品均 6 秒 | 950 | 128.653 | 1,078.653 | 仅名义均值低于 Recorder 1,200 配额 |
| **算术示例**：2 产品 1 秒，其余 17 产品 15 秒 | 940 | 128.653 | 1,068.653 | 相近预算可以有不同分配；未经活跃度验证，不能直接启用 |

计算为 `sum(weight × 60 / interval_seconds)`。不含一次性 seed、重连快照、重试、
尚未到期的 D31 发布查询和其他 IP 用户。若两个 D31 pair 都进入每分钟查询，另加
2/min。6 秒方案只余约 121/min，低频源同时到期、初始化和持续失败仍可能排队。
实际轮询还受请求耗时及本地调度影响，配置 6 秒不等于每 6 秒准时收到响应。

官方 [CM/UM 整合说明](https://developers.binance.com/en/docs/products/derivatives-trading-coin-futures/Important-CM-UM-Integration-Notice)
规定二者共享每 IP 2,400 weight/min；Recorder 现有份额为 1,200，另有 Gateway 和
预留余量。不能将 UM、CM 或两个域名当作独立配额，也不能擅自占用其他服务份额。
减少 JSON、压缩、换语言能省本机成本，不能增加交易所的请求配额。

## 为什么不能把 6 秒说成“完整逐笔”

当前 [USD-M REST 文档](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data)
和 [COIN-M REST 文档](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-coin-m-futures/api/rest-api/market-data)
给出 recent trades 的 weight=5、limit 最大 1,000；该接口没有向前翻页参数。
两次成功请求之间若超过 1,000 笔新成交，早期成交可能已离开窗口。6 秒对应的简单
平均边界约为 166.7 笔/秒，但突发、发布延迟和请求失败都会降低实际余量；1 秒也
不构成绝对完整保证。应比较成功响应之间的成交 ID 覆盖、窗口时间跨度和突发，
不能只看全天平均速率或 HTTP 成功率。

`historicalTrades` 虽有 `fromId`，当前官方文档要求 API Key，不属于本项目无凭据
范围。[合约 aggTrade](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/market)
按相同价格及主动方聚合，不能反推出每笔成交的数量与时间。保留它很有价值，
但不能将它登记为已获得完整的 D06 逐笔信息。

## 可以借鉴的实现与研究

| 依据 | 可取做法 | 在 Recorder 的边界 |
| --- | --- | --- |
| [Binance Public Data README](https://github.com/binance/binance-public-data/blob/d571690eb66760763b0bd6e27e8cd35603c7e244/README.md) 与 [download-trade.py](https://github.com/binance/binance-public-data/blob/d571690eb66760763b0bd6e27e8cd35603c7e244/python/download-trade.py) | 按市场、产品、日期获取日/月文件及校验文件；日数据次日发布 | 可以在采集进程之外补充历史成交事实，限定 BTC/ETH 产品和缺失日期；不照搬默认全市场/全历史下载。源码只读，未执行 |
| [CCXT throttler.py，固定 P0 研究版本](https://github.com/ccxt/ccxt/blob/d3159f7036f93db086cd0414642132fba5401710/python/ccxt/async_support/base/throttler.py) | 按每次请求的实际 cost 计算滚动窗口并延后调度 | 当前 RequestBudget 已有同类能力，继续复用；不引入整个 CCXT，也不从它推断 Binance 配额。排队只能限制请求，不能实现超额的目标频率 |
| [Akidau 等，VLDB 2015，Dataflow Model](https://www.vldb.org/pvldb/vol8/p1792-Akidau.pdf)，§1.3、§2 | 区分事件发生时间和处理时间，明确正确性、延迟与成本的取舍 | 次日获得昨日成交，能补充事件事实，不能伪造昨日的本机接收时间或抹平实时缺口；无需引入 Beam、分布式流平台或另一套实时计算系统 |

官方批量文件仍有边界：README 列出的合约 trades 字段包含 ID、价格、数量、
成交额/基础币数量、成交时间和主动方，但没有列出 REST USD-M 的 `isRPITrade`。
实际文件是否有新增列、特定交割产品是否可用、时间单位、缺日及后续修订，都需用
选定产品的文件核验。不能仅凭 README 的“支持全部 symbol”宣称已补齐全部字段。
该服务不是 Recorder 的原始现场观察；文件校验和证明文件内容一致，不证明交易所
发布了每一笔成交。本次没有下载市场数据文件，也未证明其实际覆盖率。

## 建议团队在 P2-R1 完成的有限工作

1. 保留现有 WS、市场范围及紧凑 D06 表示。紧凑缓存只减少重复保存，不算请求预算
   优化，也不能当作减少独有成交信息的授权。
2. 对所选合约做一份小型比较报告：最近 1,000 笔窗口跨度、相邻轮询新增 ID、已知
   缺口、空/失败次数、请求延迟及 weight。可以先用官方日文件做不同间隔的离线
   模拟，但须标为理想无网络延迟估计，再对少量代表产品做有总预算上限的公开样本。
   不同时以 1 秒轮询全 19 产品来“测一下是否会限流”，不建立通用容量测试平台。
3. 比较统一 6 秒和少量固定频率档位；依据实际窗口变化分配额度，不预设只有两个
   USDT 永续活跃。若有收益，给产品配置简单覆盖项并在运行前冻结。新产品仍显式
   发现和报告预算不足；不在运行中偷偷降频或删掉失败源。
4. 独立核验官方日文件候选。若 owner 接受延迟补充，再落实小型按日下载/导入工具，
   尽量在研究/归档机器运行。保留来源、实际获取时间和必要文件校验，重用现有存储
   边界，不在采集回调扫描全历史，不增加逐笔哈希；冲突记录不覆盖原现场观察。
5. 同时交付字段/时间差异、预期观察损失、实际文件可用性、请求余量和本机资源开销，
   再选上线配置。整个 D06 家族或产品仍在，不等于所有成交已经完整。

需要 owner 决定的是：是否接受部分逐笔事实延迟到次日补充，以及实时采样的损失
边界。该建议尚未成为已批准实现；默认 1 秒与原 6 秒提案均保持原状。99.5% 实时
连续性与延迟数据的事实覆盖分别报告，不能以补充文件给实时采集追补通过时长。

CM 交割 premiumIndexKlines 的 `-1121` 是独立的 P2-R2。重新核对的官方接口页和
整合说明仍未明确所有交割合约的适用规则，不能用永续价格序列或经济学推断替代。
继续保留未知/失败证据，待规则明确；本研究没有关闭该问题。

检索 URL、UTC 边界、内容摘要及表示见 [review-sources.json](review-sources.json) 和
[官方来源登记](../../binance_sources.md#p2-独立复审与-d06-预算研究2026-10-28)。
