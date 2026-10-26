# P0 审查修正与复测

2026-10-26；对应 [R1–R4](../review-20261026.md)。四项修正完成，P1 未开始。原 baseline 和最初研究保留；本目录只追加修正证据。运行时 `src` tree 仍为 `392154f9fc26923f88a465800d4a0a89b1fac2df`，依赖锁和 CI workflow 未改。

## 修正

- R1：用 Catalog 的 ACTIVE / SEALED transition ID 确定分片生命周期次序，在每片内按物理 ordinal 读取；先检查同连接接收单调时间不倒退，再按该物理顺序核对固定输入摘要。没有重新排序帧。真实 Raw 的 2、1、3、跨片倒序和改写时间但原文仍乱序的负例均拒绝；正确跨片及倒退墙钟通过。
- R2：同一短观察边界记录每源 sent / received / enqueued / written / persisted 条数及 payload 字节，另记真实 receipt 队列、spool 队列和未同步写入。written 在真实 Raw 追加后计数，persisted 只在真实 fsync 成功后确认，终点再与独立文件回读核对。REST / snapshot 从 Raw 模型入口计数，明确不表示观测到原始 HTTP。开始、三个四分点和结束共五个边界；尾部补发、正常排空、后台等待分列。逐源检查持续增长，不能由另一源下降抵消；输入逾期或持续积压则 NON_STEADY。短负例在尾部补齐后仍保留非稳态结论。
- R3：父进程在任何写入和启动前检查全部七类输出冲突，包括 dangling symlink；execution receipt 和日志独占创建。早退记录 FAILED、退出码和清理状态。七类单独冲突及全部旧文件并存的回归均要求原字节不变。
- R4：D11-U / D11-C 分开（52 个信息家族、53 行）。U 参考页与 2026-04-10 changelog（4 月 14 日生效）冲突仍未解决；C 按其自身参考，不外推 U 的变更。仅保存代表性清算原文，不用 latest / largest 假设判完整性、去重或推算清算总量。同步数据合同、覆盖说明、风险和[官方来源记录](official-sources.json)。参考页的短摘录 SHA 明确是渲染摘录，不冒充原始 HTTP；`.md` 探针返回 HTML shell，已标不可用于语义。

## 新 VPS 观察

独立目录 `/var/tmp/bmdr-p0-review-20261026-3KRfsxId`，固定输入、all14、20s warmup / 120s 窗口及后台工作频率同原协议。两轮均 nice 10；无部署、生产采集、历史 Raw 扫描或服务状态修改。实际[执行源](measurement-source.json)固定为 `7c07157`；[执行入口原文](benchmark/runner-source.py.txt)与 receipts 保留。之后 `a773339` 只将积压判定从跨流总量改为逐源，采集、计时字段和物理 / fsync 校验均未改变。[独立汇总](summary.json)按该最终分类器解释已存五个边界，两轮分类与 producer 相同；没有改写 producer JSON 或虚称测量了后来的 HEAD。

| 项目 | [无 profiler](benchmark/steady-n2.json) | [独立 profile](benchmark/profile-steady-n2.json) |
| --- | ---: | ---: |
| 分类 | STEADY | **NON_STEADY，仅诊断** |
| 窗口墙钟 / Recorder CPU | 120.001s / 70.357s | 120.016s / 63.928s（不可比较） |
| 结束 sent / received / written / persisted（全 Raw 源） | 32250 / 32250 / 32250 / 32113 | 32242 / 32242 / 32237 / 32091 |
| 尾部补发 / 正常停止排空 / 后台等待 | 0.000118 / 0.369771 / 3.280942s | 0.053958 / 1.608692 / 0.002833s |
| 最终独立读取 WS / 全 Raw | 32214 / 32250 | 32214 / 32250 |
| 物理顺序、原文、数量、CRC / 哈希及 fsync 数量 | 通过，99 个分片 | 通过，99 个分片 |
| 辅助源实例 | 26 个全部 accepted>0 / failures=0 | 同左 |
| 规范化行 / 实际 aggTrade 回放 | 9379 / 205 | 6471 / 143 |
| 最终归档积压 | 0 | 0 |

无 profiler 结束仍有 137 帧未获周期 fsync 确认，其正常停止后已持久化，明确不计作结束时已持久化。profile 结束还少送 8 帧，四条 bookTicker 流未按协议完成，尾部最终正确不能使计时窗口合格。两列后台处理量也不同，不能由 CPU 差异推导加速或采样开销。P1 必须比较逐源 `window_work`、尾部及后台工作量，交错复测并报告波动；单轮起点不证明容量或性能收益。

[采样日志](benchmark/profile-steady-sampler.stdout)为 10Hz / 110s、1591 样本、2 次采样错误；[execution](benchmark/profile-steady-n2.execution.json)确认采样在 STEADY_END 前结束。[诊断火焰图](benchmark/flamegraph.svg)只解释本次 NON_STEADY 运行，不能作为合格性能成绩；未为取得更好分类重新挑选轮次。

[结束观察](benchmark/completion.json)于 2026-10-26T15:06:33Z 记录生产 Recorder inactive / disabled / PID0 / NRestarts0。此前 08:41 的安装源核对仍是历史证据，本轮没有重复安装目录审计。

## 验证和范围

[定向测试](../../../../tests/unit/test_p0_benchmark_review.py) 19 passed；Ruff、针对该测试及其 helper 的 mypy、M0 合同和文档链接检查通过，详见 [checks](checks.json)。双平台 CI 绑定实际可执行提交，见该记录；本地不再重复全套历史测试。未运行在线、百万帧压力、平台部署或生产长跑。

复测须同时带上 `steady_baseline.py`、`benchmark_checks.py`、`profile_steady.py`、原固定输入生成器及其必要 fixtures；使用新的隔离输出目录和相同运行依赖。协议 v3 的额外计数会产生开销，P1 两侧都必须采用相同仪表。旧 v2 的 CPU 不能直接当 v3 的优化前对照。数据格式、生产运行时与依赖均未因这些修正改变。
