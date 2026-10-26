# Capacity 两个UNBOUND draft gate独立简审

**ACCEPT，仅LOCAL/OFFLINE draft；无范围内阻断项，不授予stage launch。** 未读取正在运行native的状态，未SSH/Raw，也未重审九runtime。

aux `79435a68…`已去固定四产品限制、expected count取完整config-derived map长度；其余原flags/freshness/cursor条件保留。monitor `007d9344…`要求外部expected_contexts并按完整keys校验，原BLOCK、cadence和rate语义保留。N1/N2/N4合成正例分别14/26/50contexts；独立两个反例均BLOCK：移除singleton global exchange_info，即使core_ready和所有产品仍ready也拒绝；global funding_info最后成功恰超(grace+interval)1ns，也在其他字段全部正常时拒绝。同数量错context名的monitor replay另行拒绝。

candidate BTC TOML SHA`e2f81312…`解析为两市场仅BTC且14flags全开。只把这两行恢复为BTC+ETH即可逐字节复现旧config SHA`5b73db1b…`，独立支持“只改两list”。topology按原enum/core/snapshot推导Spot5、USD-M15、global单例2个producer，因此N1/2/4 spools=22/42/82；辅助contexts=12N+2=14/26/50。合成N4符号仅测试fixture，不选择实际cohort或声称市场资格。

common/forecast/旧历史锚/controller调用/terminal及actual Linux/physical仍待绑定；尤其monitor CLI从目标strict_endpoint取keys，未来必须独立建立其exact config/source/identity权威，不能把记录自报keys视为配置证明。当前cohort真实rates与actual admission也未完成。父2880行合成结果只是fixture，不是实际24h或VPS容量证据。
