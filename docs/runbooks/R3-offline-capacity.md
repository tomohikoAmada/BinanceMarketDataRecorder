# R3 二十产品离线容量与系统盘留存

2026-10-19。适用范围：10 Spot + 10 USD-M 的研究候选配置与本机离线验证。
R2 正在运行的四产品 24h、冻结配置/产物、原生控制和自动归档均未操作。
真实生产名单、metadata eligibility 和同 VPS 联合负载资格仍待后续确认。

## 已验证的链路

复用既有 runtime/Collector 测试，保持全部 14 项功能：20 个私有 Collector、
60 条核心逻辑 WS 流、20 条辅助 WS、102 个 REST owner，共 122 个辅助 owner，
其中 USD-M fundingInfo/exchangeInfo 仍各只有一个全局 owner。
全体首次 core-ready、辅助数据落盘、60 个独立五分钟 cursor、停止 drain/seal
和准确产品身份均已核对；LTC 的空 OI 响应保留原始空数组与已评估范围证据，
没有制造密集时间网格或假数据。

真实核心 Collector 的 20 产品混合背压测试同时覆盖默认 executor 和六个 worker；
一个 USD-M aggTrade writer 受阻时其余 59 流继续落盘，释放后恢复，边界帧保留。
另测全 60 身份归档、目标缺失保留、重试幂等和全产品硬余量停止。
旧四产品 qualification gate 未修改，未新增线上控制器。

## 存储实验与解释

工具：`python -m tools.benchmark_r3_storage --parent <已存在的临时目录>`。
每次只新建一个 fixture 子目录；它不访问网络、不运行服务命令、不清理旧数据。
保持默认 60s/128MiB 的按产品相位轮转及 1s durability；每轮结束另显式 seal。
轮数不等于实际 60 秒时长，自动轮转后的空 close 是正常结果。
流量按 symbol 分层，depth 每侧 5–41 档，三种核心流数量不同；是构造的负载，
并非所测 Binance 到达率。全 14 项功能另由上述 runtime 测试覆盖。

| 本机 macOS arm64 / Python 3.12.9 | 普通档 | 突发档 |
|---|---:|---:|
| 产品 / 核心流 / rounds | 20 / 60 / 3 | 20 / 60 / 3 |
| 已保存并回放帧 | 27,360 | 82,080 |
| sealed chunks | 180 | 180 |
| decoded / stored Raw bytes | 20,025,063 / 1,138,396 | 60,009,427 / 3,065,445 |
| 目标总文件 bytes（含外部 manifest/注册标记） | 1,867,921 | 3,795,988 |
| Catalog checkpoint 后 bytes | 1,241,088 | 1,245,184 |
| 系统盘 retained manifest bytes | 254,886 | 255,283 |
| 完整归档 wall seconds | 0.567 | 0.688 |
| Normalize wall seconds | 5.353 | 15.945 |
| 进程 RSS 高水位（含 Parquet/Replay） | 301.45 MiB | 527.00 MiB |

两档均在 archive target 缺失时保留全部 sealed Raw；恢复后 backlog=0，
180/180 完整外部提交验证通过、源文件安全退场。
Normalize 从明确 storage_id 的外部副本读取，Replay 覆盖全部 60 身份，逐身份帧数一致；
人为加入的 reconnect_gap 使两档各 480/1,440 行保持 unreliable。
这些不是正式双 LIVE 审计收据，也不是交易所事件捕获率。
临时目录与原始结果哈希见 [实验 JSON](../milestone_evidence/r3-offline-storage-20261019.json)。
首个突发尝试因工具错误假设每次 close 都必须新 seal 而失败，其目录保留；
修正仅影响工具，另有强制每帧自动轮转回归。

这里只量到本机 unpaced 存储路径；没有量到 VPS 的 ingest arrival、共同 CPU、
event-loop lag、FD、Swap、持续 WAL/backlog slope 或真实恢复追赶。
不据此调长轮转、扩大队列、删 fsync/读回或声称 20 产品实机 PASS。
60 个核心 receipt queue 的 64MiB owned-byte 上限合计已达 3.75GiB；这是理论上限，
不是已占内存，不能当作 5.7GiB 主机可用余量。按实际拥塞同时度测量后再决定参数。

## 系统盘留什么、释放什么

| 类型 | 留存与预算 |
|---|---|
| active `.partial` | 系统盘写入；计峰值、最大帧超限增量和正常轮转/停止期间占用 |
| seal 源与压缩临时文件 | 转换期同时存在；在 active 之外计临时峰值 |
| SEALED 与 archive backlog | 直到目标 fsync、完整读回、外部提交、原删除授权完成；无固定最短留存期 |
| 内部 manifest、Catalog/lifecycle/归档记录 | Raw 删除后继续保留，不随归档自动回收；按 chunks/s 与控制事件另算长期增长 |
| SQLite WAL/SHM、日志、checkpoint、审计临时文件 | 单独预留峰值与持续增长；不把 checkpoint 后的小 DB 当作运行期 WAL 预算 |
| 外部 Raw 与 archive-manifest | 数据盘可能是唯一副本；两种文件都占长期容量，不自动删旧归档 |
| Normalize/Replay | 按需离线派生，不是常驻 writer 工作；如果同盘执行，另计 scratch、Parquet 和并存峰值 |

现有源释放协议无需重写。没有新加定时删 Raw/manifest/Catalog/日志的策略。
57GiB 是计划中的总系统盘规模，不能直接当作 free bytes；需扣除现有 OS、软件、历史证据
及其他服务。归档盘约 2.16TB 也不是当前可用空间，更不构成异地备份。

只算 60 核心流持续活跃时约 3,600 chunks/hour 会漏掉辅助数据。
在默认配置下，额外 mark/liquidation 各最多按 10/60 chunks/s、两个每分钟 REST 各
10/60、funding history 10/300、六项五分钟 REST 合计 60/300、Spot metadata 10/3600、
两个全局 metadata/info 2/3600 计算，合计约 **6,852 chunks/hour**。
这是持续活跃、时间轮转主导的静态估算；低频无事件、分页、首次连接、重同步及 size
轮转会改变数量。不要把它写成实测保证或硬性上界。

## 可重复的容量预算

`python -m tools.r3_capacity --input docs/milestone_evidence/r3-disk-budget-example-input.json`
只做纯计算；它不读取主机空间、Catalog 或服务，也不作线上 admission。
所有关键输入显式提供，输出同时保留输入：

- writer 当前 free、数据盘当前 free、规划 horizon、归档不可用时间和初始积压；
- 同一混合负载的 stored bytes/s、chunks/s、完整归档退休 bytes/work-second 与 files/work-second；
- 真实 wall-clock cycle、可工作时长和每轮 max-files（默认模板为 60s cadence、10s jitter、
  50s runtime、1000 files；runtime 在事务间检查，单笔事务可能超时，须使用实测 cycle）；
- writer 持续元数据/其他增长、外部 manifest bytes/chunk，active、seal、归档临时、WAL、
  审计/派生/其他临时峰值。

文件吞吐与 bytes 吞吐都必须高于流入；每轮 max-files 也约束有效服务能力。
峰值保守加入一整个 cycle 的等待，覆盖 worker 提前退出；旧小块按同流入混合的等价空间
向上计费，避免把文件处理时间冒充已释放的新大块 bytes。
所有声明峰值与 horizon 末持续增长同时扣除，可能偏保守；它不模拟真实队列调度。
恢复后还须在 horizon 内清完积压。writer 下界必须严格大于原 **10GiB**，目标空间也须为正。
外部 manifest 按初始与新增 chunks 一并计入；本机普通档它们已约占 stored Raw 的 64%，不可忽略。

[示例输入/结果](../milestone_evidence/r3-disk-budget-examples-20261019.json)全部是演示假设，
**不是当前 VPS 观测、不是 R2 的 24h/48h 预算**：writer free=20GiB、archive free=1800GiB，
sealed 200,000B/s、2chunks/s、writer retained metadata 32,768B/s、其他增长1000B/s，
归档1000files/cycle、50/70s、1,000,000B/work-s 和20files/work-s，
另计 active/temp/WAL 共2.375GiB、外部 metadata 8KiB/chunk。
1h 归档不可用的 1d 情境可以容纳；7d 情境被持续系统盘元数据增长拒绝；
整天无归档和低 max-files 情境也被拒绝。持续元数据仍增长，所以“归档清空”不等于
系统盘能永久留存。准确数值见结果 JSON；上线前必须换成实际空闲、真实分布与恢复速率。

## 联合配额的离线裁决

端点、两进程开销、特殊窗口和负面情境见 [BMD-RL-01 inventory](R3-shared-ip-budget.md)。
普通冷启动的静态合计可容纳，但两轮集中 Spot 恢复加 metadata 就可能超过预算。
现有进程各自的 limiter/cooldown 没有构成跨进程总量保证；联合恢复/冷启动补采窗口
仍 OPEN，留给 R3-b/G3-b 的最小静态分账/错峰改动，不能把本轮 Collector 测试当作它的 PASS。
本轮未启动 Gateway/VPS 或制造真实 429/418。
