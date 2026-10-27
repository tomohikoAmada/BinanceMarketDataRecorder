# P1：缩短原始数据写入路径

**状态：完成（本地实现、离线验证与交错测量；生产资格未完成）。**

2026-10-27。开发起点 `6f8fe1e802d1e2b99e4f0a38a1f5cacfe707079a` 已先与
GitHub main 同步；工作区无未提交改动。实现为 `ce1826b8a8fa9057783dfe035596332c8c6d67d8`，
运行时 `src` tree 为 `0a173540835c6eb78de26eab9d06d165cc4902aa`。
后续文档、测试和测量工具提交不改变该运行时身份。

## 实现

- 接入仍在解析前记录原文及 UTC / monotonic 接收时间。新增字段可兼容；格式错误、
  schema 不符和过深 JSON 保留原字节并标为 `malformed`。价格派生计算不再阻断 Raw。
- 每连接保留一个接收任务及停止 / 轮换控制，逐帧用协作式让出调度，不再创建和取消
  recv / stop / shutdown 任务。正常停止与 socket 异常竞态通过控制清理后仲裁。
- 原有批量线程移交继续使用；新增真正的 POSIX `writev` 批量追加，短写、EINTR、
  无进展、部分失败、证据 / fsync 失败仍按明确规则处理。没有 `writev` 时使用有界
  拼接与完整写循环。现有同步每帧配置保留原语义。
- 每批至多 256 帧 / 1 MiB 加一最大帧；receipt 保持原有 64 MiB payload ownership
  预算，spool 新增 64 MiB 的 payload + 保守元数据预算，并保留条数限制。批次在队列
  外的持有量另有界。writer 区分完成追加和成功 fsync，诊断显示两种进度及字节积压。
- readiness 只保存 U/u/pu 和接收时间，保留原 snapshot bridge、重复 / 过期、缺口及
  bootstrap 溢出处理；完整盘口、Decimal / 最优价 / 交叉盘计算仍可由离线消费者执行。
  公开状态明确 `capture-depth-sequence.v1` 和 `full_orderbook_evaluated=false`；
  旧 `orderbook_synchronized` 字段作为序列桥接兼容别名保留。异常 depth 已保存后会使
  序列健康失效。旧历史记录不重解释。
- metrics 复用已有 aggregate，避免每次查找都构造一个随后丢弃的新实例。

[ADR-0052](../../adr/0052-bounded-raw-batches-and-sequence-only-capture-health.md)
及[数据合同](../../data_contract.md#p1-capture-and-progress-contract-2026-10-27)
明确这些边界，P0 记录也追加其适用关系。Raw v1、SQLite、现有 reader、CRC、canonical
encoding、原有内容摘要、封存与归档读回 / 删除授权不变；没有历史转换、生产部署或掉电承诺。

## 验证与测量

最终离线全套 **2,263 passed / 24 skipped / 5 deselected**（188.61s），
Ruff、strict mypy（324 文件）、M0、Go Raw v1 golden、diff whitespace、wheel 构建与
隔离 wheel 导入通过，见 [checks](checks.json)、[完整测试日志](pytest.log)及[构建日志](build.log)。
新增回归覆盖单帧 / 批量精确字节、source 时间 / 序列 / connection transition、
短 writev / EINTR、部分失败 / 无进展、fsync / observer 失败与 poison、别名突变、
可移植 fallback、轮换计数 / header 字节、字节预算，以及序列桥接与离线算法对照、
异常原文、未知字段、无完整盘口依赖和 malformed depth 失效。原有停止 / 重连 / 背压
集成用例保留；没有降低断言以迁就优化。13 条 warning 来自保留的 macOS fork 测试。
未运行 opt-in 在线、百万帧 stress、VPS 对照 / 长跑、新掉电专项、其他平台服务或真实外卷测试。


测量原始结果见下表及 [benchmark](benchmark/summary.json)，最终检查见 [checks](checks.json)。所有新数据均为隔离临时目录中的离线 fixture，
未访问真实外卷、账户 / 密钥、生产 VPS 或生产数据。生产运行状态没有重新观察；
[上一次 VPS 观察](../P0/corrections-20261026/benchmark/completion.json)仍是历史记录。

比较沿用 P0 的固定输入、all14 设置、20s warmup / 120s 观察、v3 计数和 v4 分类器。
A 为起点、B 为 P1 实现，按 A/B/B/A 顺序交错。逐源窗口工作、尾部和后台量分列，
NON_STEADY 保留且不可作为 CPU 性能成绩。另用原有十标的核心 fixture 交错观察并发、
资源和正常排空；writer 单项测量采用相同帧 / 固定头及独立全读，比较字节、摘要和实际
系统调用。均是 macOS 本机结果，不替代旧 VPS 基线，也不声称 VPS 容量或生产收益。


### 本机结果

完整[汇总](benchmark/summary.json)保留逐源窗口量、尾部及各轮资源；原 JSON / 日志
均保留，未覆盖 P0 或任何旧测量。两侧运行依赖、全功能配置、固定 WS 输入摘要相同；
四轮每条 WS 的窗口 sent / received / written 条数和 written payload 字节完全相同。
每轮 32,214 条 WS，物理顺序 / 原文 / CRC / 摘要及实际 fsync 数量回读核对均通过。

| 轮次 | 分类 | 窗口 CPU (s) | 全 Raw 帧 | 停止排空 (s) | 归档片 / 终点积压 | 规范化行 / 回放 aggTrade |
| --- | --- | ---: | ---: | ---: | --- | --- |
| [1-A](benchmark/all-features-1-A.json) | STEADY | 122.628 | 32250 | 0.109 | 98 / 0 | 9804 / 213 |
| [2-B](benchmark/all-features-2-B.json) | NON_STEADY | 122.582 | 32251 | 0.119 | 99 / 0 | 7939 / 123 |
| [3-B](benchmark/all-features-3-B.json) | STEADY | 122.778 | 32250 | 0.120 | 99 / 0 | 6944 / 153 |
| [4-A](benchmark/all-features-4-A.json) | NON_STEADY | 122.985 | 32251 | 0.137 | 97 / 0 | 7992 / 181 |

四轮观察均为约 120s，输入尾部补发均不足 0.001s；完整后台等待和各源 fsync 进度
见 JSON。全 Raw 的 1 帧差异来自 SDK snapshot，未包装成相同完整工作量。
B2 未同步边界为 `28 → 4 → 33 → 5 → 35`；A4 为 `32 → 11 → 44 → 23 → 45`，
v4 因此分别报告对应 bookTicker 源的增长。正常停止后均已同步，但尾部不能改判。
后台消费者量不同，且原版 / 优化版各有 NON_STEADY；**没有证实全功能 CPU 收益**，
也不据此声称持续积压已解决。P3 / 后续真实观察仍须验证这些进度和后台工作的长期行为。

十标的核心 fixture 以同一 A/B/B/A 顺序执行，每轮 20 产品 / 60 流、42,600 条 WS，
原文 / 数量 / CRC / 摘要核对通过，停止后 receipt owned bytes / depth 全部归零。

| 轮次 | CPU (s) | 采集含排空墙钟 (s) | RSS 峰值 MiB | receipt 条数 / payload 字节峰值 |
| --- | ---: | ---: | ---: | --- |
| [1-A](benchmark/core-ten-assets-1-A.json) | 4.110 | 4.672 | 159.22 | 268 / 34726 |
| [2-B](benchmark/core-ten-assets-2-B.json) | 2.259 | 4.733 | 162.36 | 1024 / 140897 |
| [3-B](benchmark/core-ten-assets-3-B.json) | 2.315 | 4.677 | 166.06 | 1024 / 163091 |
| [4-A](benchmark/core-ten-assets-4-A.json) | 4.101 | 4.678 | 152.05 | 283 / 34338 |

CPU 下降是这组固定 WS / 当前核心路径的本机观察；B2 多一次 SDK snapshot，完整量
单列，不能外推全功能容量。墙钟并未加快。RSS 含预分配 fixture，优化版峰值未下降；
更快接收使 receipt 队列达到配置条数上限 1024，但 payload 峰值仍低于 164 KiB，
未突破字节预算。该有限输入不证明长期资源有界，也不证明满预算同时饱和可被 VPS 承受。
14 / 20 产品、默认 / 六线程 executor 的背压集成回归另行通过。

writer 使用 A/B/B/A/A/B/B/A，8 轮各 20,000 帧、固定头和相同元数据：

| 项目 | 原版 A（4 轮范围；中位数） | P1 B（4 轮范围；中位数） |
| --- | --- | --- |
| 追加含 final sync 墙钟 | 0.311–0.325s；0.316814s | 0.279–0.282s；0.279373s |
| CPU | 0.311–0.325s；0.316217s | 0.278–0.281s；0.278768s |
| 写 API 调用 | 20,000 次 `os.write` | 79 次 `os.writev` |

中位墙钟减少 **11.82%**、CPU 减少 **11.84%**；独立扫描全部通过，8 份文件内容 SHA-256
均为 `6c07846a08c261f268a719ec9d22e8d05468efc57cb0b2780805e07d1d71badf`。
该摘要用于固定测试输出身份 / 字节等价，未加入生产逐事件哈希。逐轮耗时、调用数、
字节数和源码路径见 [summary](benchmark/summary.json) 的 writer 行及各轮原 JSON。
因此保留实际批量写实现；不把减少调用数单独当作收益证据。

复测使用已准备好的两个源码目录和同一依赖环境；不修改它们的 Git / 安装状态：

```bash
/absolute/path/to/python docs/milestones/P1/compare.py \
  --baseline /absolute/path/to/before --baseline-revision 6f8fe1e \
  --candidate /absolute/path/to/candidate --candidate-revision ce1826b \
  --python /absolute/path/to/python --output /var/tmp/new-p1-comparison
```

输出目录须不存在，工具会在任何子进程 / 写文件前拒绝旧路径；每轮独立日志及 JSON
用独占创建。`compare.py` / `writer_benchmark.py` 只服务本次有限对照，不是新的通用容量
或故障平台。原 P0 计量入口、分类器、fixtures 和历史证据保持原样。

## 下阶段边界

P2 补产品身份和覆盖，含 COIN-M、交割及新增信息家族；本次十标的测试只覆盖当前
Spot / USD-M 产品和流，不算 P2 覆盖完成。P3 继续实现按源连续性、独立新文件验证队列、
后台调度 / 恢复和运营命令。现有 archive / report / normalize / replay 在比较中照常运行；
不把本次有限 fixture 的正确性或队列上限当成长期有界证明。P4–P6 尚未开始。
