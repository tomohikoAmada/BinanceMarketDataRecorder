# P2-R2：交割 premium 适用性调查

**后续owner决定（2026-10-29）：** 首版仅暂缺U/C交割premium K线已获批准，
永续和其他范围不变；[官方issue #585](https://github.com/binance/binance-connector-python/issues/585)
已提交。适用性仍未知，但等待回复不再阻塞首版验收；团队继续落实
[P2清单](../../../production_milestone_plan.md#p2补齐产品和数据范围)中的源选择、
覆盖/预算和验证。以下调查保留原义，旧的未授权/未发送及等待规则门槛已被
[ADR-0055](../../../adr/0055-initial-production-dated-premium-exception.md)替代。

2026-10-29；观测发生于2026-10-28 UTC。本轮基线为 `e3f1dd7`；R1、R4的独立复审通过结论保留。
**R2仍未闭合，P2仍未通过。** 没有新的官方适用规则，故按AGENTS的官方来源
要求停止该项实现，不把重复失败转换成“不提供”，不缩减目标或改用另一价格序列。

## 新增证据

- 先读取当前Agent Native index，再核对整合说明、UM/CM changelog和当前官方
  Python connector源码。整合说明C.3列出两条premiumIndexKlines路径；SDK只有
  `symbol`、`interval`及可选时间/limit，没有额外的交割合约参数。两者均未明确
  perpetual/dated适用规则。共享changelog的两个URL返回同一内容，不算两份独立证明。
- [14次有界HTTP诊断](raw-http-diagnostic.json)使用原生host/symbol、1m/limit2：
  8个U/C交割合约均400/-1121，4个永续对照均200/两行；BTC的U/C交割额外指定
  已收盘的两分钟时间段，仍400/-1121。每次保留真实请求/接收UTC及单份响应正文。
  这是隔离SDK解码影响的诊断，不是新增生产HTTP接入，也不是连续性或全周期证明。
- [两次新目录检查](directory-validation.json)确认上述8个交割原生symbol均仍TRADING，
  对应当季/次季。JSON明确是所选身份字段投影，不冒充完整HTTP响应；完整目录正文
  留在所列本地诊断目录。目录有效也不能证明premium端点必须支持该产品。
- [来源记录](official-sources.json)列出URL、获取UTC、正文/解码文本SHA-256及结论；
  不执行下载内容，不读取llms-full。`premiumIndex`快照中交割funding字段为空的规则
  属于不同端点，不能用于关闭premiumIndexKlines适用性。

新增市场数据与目录诊断共16次公开无凭据GET：14次premium诊断、2次元数据。无生产采集或部署。
原8个失败产品、120个周期源及其重试预算继续保留，历史观测未修改。

## 闭合需要的外部事实

[2026-10-29权限与重要性补充](../r2-20261029-access/README.md)核对API Key/VIP疑问：
六次匿名请求中两个BTC永续premium成功，两个交割premium失败但各自盘口成功。
官方接口说明未列密钥或VIP门槛；这项补充没有解决交割适用规则，R2保持开放。

已整理[可提交的官方澄清问题稿](binance-clarification-draft.md)，尚未发送。
官方connector README提供GitHub issue讨论入口；提交外部消息需要owner明确授权。
这不是重新确认已批准的D06方向或新增验收层级。

收到明确的官方规则后，按以下实际情况继续，不能靠本轮负例代替规则：

| 官方结论 | 后续实现 / 验证 |
| --- | --- |
| 明确仅永续适用 | 在既有覆盖表记录交割不适用及官方依据；显式修改源生成/预算，保留旧失败Raw，验证永续与交割边界、读兼容及预算，然后独立复审 |
| 交割适用且给出有效原生查询 | 修正受影响参数/身份接入，保留交割原生信息；验证实际样本及落盘/读取，补负回归和最终代码CI，然后独立复审 |
| 确认服务故障或仍无规则 | 保留UNKNOWN/失败源和实际重试成本；P2不能按完成打勾 |

本轮未改运行时。未重跑已通过R1/R4的历史审计、全套测试或VPS测量；已有运行时
测试/CI仍只归属于原提交。受影响的文档链接、证据结构及合同检查见
[检查记录](checks.json)。PR #88保持Draft，开发分支保留，P3未开始。
