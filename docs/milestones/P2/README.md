# P2：产品和数据范围实施记录

**2026-10-28 独立审查：暂不通过。** [审查记录](review-20261028.md)与
[D06 预算研究](d06-budget-research-20261028.md)已写回计划；原 R1/R2 继续开放，
新增 R4：REST 未遵守 Retry-After。复跑全仓 2,314 项及相关 314 项通过，
新限流复现六例均失败。以下是原交付证据，保留其测量和当时边界。

**2026-10-28 owner 决定：已选择选项一。** 后续按“按产品分配实时轮询频率＋官方
日度逐笔文件独立延迟补充”推进；先核验具体产品、日期、字段及观察损失，再实现
小型下载/导入路径和实测频率配置。方向已确定，R1 的实现与验证尚未完成，R2/R4
仍开放。当前默认仍为1秒，原6秒提案未被直接选定。详见[计划](../../production_milestone_plan.md#p2补齐产品和数据范围)
及[数据边界](../../adr/0054-selected-asset-capture-and-public-observations.md#owner-decision-live-d06-and-daily-supplement-2026-10-28)。

2026-10-27。基线 `8c2cf72114b55ea193316189e7ce9a178bd96393`，工作分支
`codex/p2-market-coverage`；起始工作区干净。未部署、未改变 VPS 服务，未访问生产数据。

**代码及本机验证已交付，P2 的全部通过条件尚未闭合。** 保留完整目标的
1s D06 配置，不用限流等待或减少工作冒充 99.5% 通过。当前公开接口预算无法满足
这份 BTC/ETH 完整配置，正在请 owner 决定是否允许明确的采样范围调整。
C 交割 premium-index 的适用性也仍有未覆盖记录，不能用永续值替代。
P3–P6 未开始；production readiness 仍为 NO。

## 已实施

- 明确 Spot、USD-M / COIN-M 永续和交割身份；保存 pair、base/quote/margin、
  合约倍率、上市/交割时间与状态。共享源使用独立 shared 身份。
- 按配置 underlying assets 发现 base 对应的全部 Spot 报价对及 U/C 合约；
  处理新产品、到期、目录缺失、状态变化及显式资源准入。不是订阅全交易所。
  保留旧显式 symbol 配置模式及旧记录读取。
- 将 [53 行覆盖表](../P0/coverage.md#p2-implementation-and-field-mapping--2026-10-27)
  落实到具体 Source 计划，逐项记录字段、scope、请求参数、原生周期、优先级、
  schema、SDK 版本及例外。目录、pair/contract-role 数据和全局数据按实际 scope 抓一次。
- 复用 P1 有界长寿命接收与批写，采用 Binance 官方 combined 订阅。保存完整收到的
  combined 应用消息字节，保留逻辑源标签；订阅成员的单次身份摘要绑定断线记录。
  普通深度与 RPI 的快照/序列桥接独立，接收不等待完整盘口。
- REST 通过已安装的官方 SDK common unsigned transport，保留真实完成接收时钟、
  状态和允许的 headers。解析型每次只存一份 JSON 信息，保留 null/缺失和十进制字符串；
  数字型结算价格使用已经收到的单份正文，避免 SDK float 精度损失。
  Spot 旧快照写入改为 v3 单份表示，v1/v2 仍可读。
- D06 有界 2,000-ID 缓存直接比较 ID 和字段，保存新增、同 ID 变化、首次观察时点、
  每次轮询计数/空值/ID 与时间边界、窗口溢出。失败轮询保留证据且不刷新成功。
  重启/驱逐允许重复；不承诺重建每次原返回列表。不改 WS 或历史 Raw。
- 新 public-observation 规范化 schema 及现有 ManifestCatalog/replay 可读；
  保留原始保存表示，未知有效版本拒绝猜测。Raw v1、CRC、文件/归档完整性边界不改。

协议与兼容约定见 [ADR-0054](../../adr/0054-selected-asset-capture-and-public-observations.md)。
`data_freshness_evaluated=false` 和 `continuity_qualified=false` 如实保留：当前源状态
用于接入/请求诊断，P3 的持久逐源时间区间、新文件验证和后台资格工作尚未完成。

## 实际证据

| 项目 | 结果及边界 |
| --- | --- |
| 真实目录 | Spot/U/C 三市场成功，BTC/ETH 有 50 个 TRADING 产品：Spot 31、U 永续 9 / 交割 4、C 永续 2 / 交割 4；含 ETHBTC 和非 USDT 报价对。另有非交易历史目录项，未伪造成活跃产品 |
| REST 小样本 | 已保存公开响应及失败，覆盖各实际 REST 家族；无账户/订单/密钥请求。初次探测为缩小样本把 depth limit 改成 3/5 的失败原样保留；有效深度及 RPI limit=1000 后续样本单列，未覆盖旧文件 |
| WS 小样本 | 五种产品身份的真实公开字节；稀疏 block/C liquidation、未观测的 C index 用官方样例补充。短窗口没有消息不等于故障，也不等于已验证其生产覆盖 |
| 全周期原生 bar 成本 | [固定 30s 接收样本](native-bars-sample.json)：5 个代表产品，197 条配置流，实际观察 130 条；3,194 帧 / 1,169,630 payload bytes，独立完整读取的字节和物理顺序一致，停止队列为 0。含连接/排空总墙钟 39.335s，进程 CPU 2.691s，压缩 Raw 149,224 bytes。未观察的 67 条不记成功。非 VPS、非稳态/全功能 A/B、非 99.5% 资格 |
| Spot 单份/双份对照 | [同输入交错测量](representation-benchmark.json)：100 次相同 1,000 档快照，编码 CPU 中位 37.127→14.528ms，压缩字节 8,724→5,508；字段及正文一致。只证明该本机编码工作 |
| D06 表示对照 | 同 52 次有重叠/变化/空窗口，编码 CPU 26.086→12.895ms，压缩字节 13,726→5,824；新增/变化事实及首次观察时点相同。选紧凑 v1；不把减少重复列表当作减少真实成交 |
| 离线验证 | 全仓 2,292 passed、24 skipped、5 deselected；随后受影响的 P2 / 服务 / 规范化 / 盘口定向回归单独补跑。Ruff、strict mypy、M0、Go Raw golden、sdist/wheel 构建通过。最终定向数量见 [checks.json](checks.json) |
| 实际落盘与读取 | 五类身份经真实有界接收/Raw writer/封存，独立 Raw 读取、规范化及 ManifestCatalog replay；精度、旧快照、迟到 ID、同 ID 变化、空/失败轮询、配置、目录丢失、资源准入与 10 标的/50 产品计划均有回归；另验证实际服务入口、缓冲溢出恢复、已知序列缺口标记和正常轮询不制造断线缺口 |

目录及 REST/WS 小样本在 `tests/fixtures/binance/p2/`；
[fixture 清单](fixture-manifest.json)绑定用于验证的具体文件；初始 D31 SDK 模型样本
仅是接入诊断，数字精度以 `usdm-delivery-price-exact-body.json` 和高精度回归为准。
[官方来源](official-sources.json)区别下载原文、WAF/HTML 失败与渲染摘录；未下载 llms-full。
[原生 bar 样本源码](native-bars-sample-source.py.txt)保留当次执行，测试 Raw 位于结果中的
临时测试根，未清理或改动生产/外置盘。

## 尚未闭合的通过条件

1. **明确的请求预算冲突。** [当前目录完整计划](coverage-summary.json)有 2,154 WS
   订阅及 2,627 REST 源/seed。D23 已改为每个原生 bar 关闭后查询（不是把月线每分钟
   重复抓）；即使如此，正常 REST 计划仍约消耗 5,829 UM/CM weight/min，其中 D06
   19 产品 × 5 weight × 60 次 = **5,700/min**。现有 Recorder 份额为 1,200/min，
   官方整合池总量 2,400/min。初始 seed、重试和其他 IP 用户还要额外消耗。
   限流器不会违规绕过配额；它等待，逾期仍可见。不能以这种等待宣称通过。
   6s 的显式有限窗口采样在名义稳态约为 1,079/min，可能减少高峰时观察到的成交，
   尚未获得 owner 的范围变更决定，未改默认 1s。可审阅的
   [6s 配置提案](recorder.resource-proposal.toml)和[同源需求估算](resource-proposal.json)均明确标为未批准，未运行资格窗口。
2. **C 交割 premium-index 未覆盖。** 当前目录列有 BTCUSD_261225 / ETHUSD_261225，
   但 `/dapi/v1/premiumIndexKlines` 对所测交割 symbol 返回 -1121。依官方整合说明
   再查同名 FAPI 路径也同样返回 -1121；相同路径的 perpetual symbol 成功。
   当前文档的例子 pair=BTCUSD 自身也返回 -1121。这些是实测边界，尚不能据此推导
   所有合约的永久不提供规则。停止猜测这条源的适用语义，保留失败/未知状态及
   风险登记，不改成永续或其他价格类型，不删除失败源以通过资格。
3. **U dated ticker 的 ps 不可泛化。** 小样本 ticker.ps 等于完整 dated symbol，
   depth/bookTicker/rpiDepth.ps 则为目录 pair。保留实际字段，产品身份取目录；
   不再用一个全局 ps 规则解释所有流。U 清算 latest/largest 的既有冲突仍未解决。
4. **资格工作未做。** 67 条 bar 流在短样本中未观察，C index 小样本无事件；
   不授予成功 credit。P3 的逐源新鲜度/连续性和后台验证、GreenCloud 长跑、双平台
   新候选 CI 与实际容量均未获本次证明。没有 P4–P6 时长或生产通过。

P2 的实现可以审阅，但在预算/采样范围决定及源适用边界明确前，计划仍标记
“实施中，未全部通过”。这是当前合同要求的阻塞，不新增私人审批链。
