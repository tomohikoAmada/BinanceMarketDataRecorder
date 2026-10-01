# 计划、现有实现与对话判断复核

日期：2026-10-02（Asia/Shanghai）。本次范围：`PLAN_REVIEW_AND_PUBLICATION`。
GitHub main 核对为 `d6f37576f5b044bf578501cba291fe889e45a919`；复核优化候选
`78770822ad95f97617d28a0fba6df761e8e422fa`，其前一审查提交为 `befe152`。
这是作者自复核，不冒充独立实施审查，不关闭已有 P1 或云端资格门。

## 结论和审查范围

保留 Python、现有单进程多产品边界和 SQLite/Raw/Archive 事务。此前两份审查的
主要发现和已测优化方向成立；本次没有发现要求整仓重写或立即升级服务器的证据。
当前仍有必须在冻结发布前处理的正确性问题，并发现一项辅助健康摘要遗漏。
最终路线为小修复 → 审查/发布 → 安全清理/新范围/云端预热/基线 → 2h → 12h →
24h。唯一当前计划入口是 [milestone_plan.md](../milestone_plan.md)；旧 24h 文件
只保留跳转，历史章节明确不再是执行指令。

复核沿用已有全仓模块盘点、专项复现和全离线测试，再重读实际调用链：runtime
启动/等待/退出与辅助状态、normalized identity/dedup/merge、orderbook audit/
checkpoint、V5 start/delta/replay/resume/freeze/audit/finalize、Catalog 摘要与分页、
Raw 扫描、CLI verify 以及 CI/构建门。未声称逐行独立证明全仓正确；未连接或改动
VPS，未读凭据，未清理、部署、启用监控或创建 Formal T0。

## 代码与计划发现

| 项目 | 复核结果 | 最小处置 |
|---|---|---|
| F1 / 单页吞吐 | 原问题成立；四页候选及字节/引用边界有离线证据，尚未证明完整云端 observation 可持续 | Q2 独立审查，Q3 实测核心+辅助+大小封存/重连，不直接关闭 |
| F2 / heartbeat | 实际 wait 集合仍缺 heartbeat；finally 收走异常，启动恢复阶段也须考虑任务已失败 | Q1 用既有等待/停止结构统一必需任务终态，覆盖启动和稳态 |
| F3 / normalized identity | snapshot/shutdown 隔离缺陷仍在；Raw 未因此被修改 | Q1 统一身份命名空间，版本化 build/dedup，保持旧输出可读 |
| F4 / 外部归并 | 全部 run 同时打开的风险仍在；当前 VPS live 不跑 normalization | 38h 后独立离线修复，不引入 VPS 新服务 |
| F5 / audit history | `audits` 在启用 observer 时仍 append；checkpoint 另存 unreliable intervals，不能把整个质量状态一刀截断 | 冻结前测 live 增长；必要时只限制近期诊断历史，保留持久事实 |
| F6 / cwd | optional Git discovery 的 PermissionError 风险仍在；Path.cwd 本身失败也需覆盖 | Q1 跳过不可用发现，实际数据权限不放宽 |
| F7 / 辅助 FAILED 摘要 | **新增 P2，离线复现。** enabled auxiliary FAILED 未列入总览 DEGRADED 条件 | Q1 小健康修复；保留 core_ready 和辅助隔离语义 |

F7 位于 `service/runtime.py` 的 `_state_document`：side-items 的退化集合只有
RETRYING/STALE。注入两个 READY core Collector 和一个 enabled/FAILED global
side kind 后，总览仍是 `network_status=ALL_MARKETS_READY`，详细辅助状态为 FAILED。
这是摘要不一致，不是辅助失败必须停止核心或已发生生产数据丢失。复现见
[脚本](2026-10-02-plan-recheck-probes.py) 和 [输出](2026-10-02-plan-recheck-probe-results.json)。
全局辅助 owner 的意外退出应在同一修复中显式反映；已有 SideDataSupervisor 的
隔离/retry/terminal 机制复用即可，不再建立一套通用 task framework。

Raw 优化仍逐帧做 canonical CBOR、类型、CRC、双 hash 与统计验证；只延迟未使用
的 FrameIdentity/摘要。Catalog 默认明细 API 未变；CLI 默认输出兼容性确实改变，
使用 transactions 的操作脚本必须改成显式分页。现有 `ArchiveManager.status`
库接口仍加载明细，因此不把此次 CLI 收益推广为所有库调用都已优化。

## 对前述性能判断的限定

- Raw 的约 21% 耗时下降来自六个真实 chunk、五种 market/stream 组合、每种两轮
  测量；不是全数据集、峰值负载或完整 observation 的改善比例。缺少 USD-M depth
  和多个辅助类型。约 44 倍的 status 对比是一次有完成退出码的 A/B，加 serialization
  的固定数据样本，不是稳定 p95 保证。原始 JSON 保持不变。
- 4 核/约 6 GB、无限 unit quota 是云端事实。旧 0.9255 核与约 0.49 GB sampled
  RSS 来自 **Recorder 停止时的 baseline 审计流程**，不能当作 live Collector 余量。
  cgroup MemoryPeak 还含 page cache。结论应是“未证实硬件是主因”，不是“服务器
  已证明足够”。Q3 必须测 Recorder、Observer/worker 和合并压力。
- Go/C++/Rust 局部 scanner、两个 Python Raw workers 都是有依据的备选，尚无本项目
  的收益数据。先减少重复工作，再用同一云端 Raw 做端到端等价与收益比较；现有 codec/
  validator 已有原生实现，不承诺语言替换的固定倍数。不要仅由 CPU 空闲推断更多核
  必有用，也不要仅由 CPU 时间推断不存在磁盘/steal 干扰。

本次重新核对 [Python 3.12 multiprocessing](https://docs.python.org/3.12/library/multiprocessing.html)、
[python-zstandard](https://github.com/indygreg/python-zstandard) 和
[Pydantic 原理](https://pydantic.dev/docs/validation/latest/get-started/why/)；它们支持
可行性与既有原生库判断，不代替本项目云端测量。未修改 Binance transport，未据此
引入网络语义变化或新增依赖。

## 修正后的执行顺序与成本

1. **预热在权威基线之前。** 原提案先为空范围建立 baseline，再做多窗口预热。
   `V5AcceptanceObserver.start` 从 predecessor cursor 捕获首个增量，因此会把预热
   全部行交给首次 T0 的有限批次。现有实现允许 pending，不代表必然失败，但徒增
   积压和排障。改成同一个新 corpus 中非正式预热 → 停止/归档/静止 → 完整 baseline
   → fresh readiness → 及时 T0；不再次清空预热数据。
2. **每个阶段终审是累计范围。** 三次终审分别覆盖至少约 2h、14h、38h 加预热和
   其他捕获，不是独立只扫本阶段 2h/12h/24h。实际失败尝试也保留在范围。38h 信用
   与完成墙钟区别必须写清。按真实分流事件/字节增长预测两遍 full LIVE scan，加
   freeze、SQL、shards 和 control verification 成本；没有人为两小时终审上限。
3. **不增加第三遍 full Raw。** `run_audit` 在 root 发布前已经调用 live
   `verify_audit`；随后 CLI `acceptance verify` 是历史 control/proof reconstruction。
   它仍是独立证据复核，不是再次审计当前物理 Raw。不可把它宣传为异构算法证明，
   也不用为了“独立”字样再触发一次完整 LIVE audit。既有两遍要求继续保留。
4. **清理不绑定到重复旧库全审。** 用户授权新范围；保留旧 archive/evidence 和
   Catalog/manifest/registration 的核验可恢复副本，旧未完成审计风险保持未决。
   在停止且短暂暂停项目 archive mutator 时做一致 custody，限定物理删除对象。
   新捕获后禁止拿旧 Catalog 覆盖新范围；回滚先停并保留新数据。
5. **计时链与状态精确对应。** 38h 明确为三阶段，而非连续 38h；每个 final 的
   PASS_CANDIDATE/eligible/无 blocker 与 completed verify/review 共同形成计划 PASS。
   每阶段进程身份固定，阶段之间允许受控重启。工件/配置/范围身份改变则重新起链。
6. **没有额外架构门。** F4 可在后续 offline 修复；F5 必须在冻结前获得实际处置。
   不要求整仓重写、第三审计实现、通用多 root、额外 soak 或新的状态机/服务。
7. **维护窗口涵盖身份敏感终审。** 只保护在线计时、target 后马上允许更新，仍可能
   在长终审中改变 pinned Python/依赖或重新激活 mutator。窗口按实测计划设定有限
   结束/恢复程序，覆盖基线或计时/终审至发布；完整阶段之间恢复维护。更新若改变
   工件身份必须重新审查/起链，不能偷偷转移旧信用，也不新增常驻维护服务。

## 验证和发布范围

本次 runtime 未改动。复现工具只在 TemporaryDirectory 使用实际状态构建和注入
的 readiness/auxiliary 夹具；输出不是 Formal 证据。本次最终本地检查使用原 workspace
只读复用的 Python 3.12.9 环境，`PYTHONPATH=src:.` 指向 review worktree：

| 检查 | 本次结果 |
|---|---|
| `CI=true M0_CONTRACT_ROOT="$PWD" PYTHONPATH=src:. <python> -m pytest -q` | 1852 PASS，24 skipped，4 stress deselected，145.81s；13 个已有 fork warnings |
| `<python> -m ruff check .` | PASS |
| `PYTHONPATH=src <python> -m mypy` | PASS，278 source files |
| `CI=true M0_CONTRACT_ROOT="$PWD" <python> tests/verify_m0_contracts.py` | PASS |
| `go run tools/verify_raw_chunk_golden.go` | PASS |
| 复现脚本、修改文档本地链接/JSON、`git diff --check` | PASS |

GitHub 双平台 CI/build/fresh-wheel 以推送 PR 的实际运行记录为准，不冒用 PR #78
或之前的结果。online/stress 不在本轮离线审查执行：生产部署未改变，相关 opt-in
发布门保留在 Q2/Q3；本次不创建现场验收或外部卷写入测试。

本次推送范围包含之前尚未发布的 review/优化候选以及这次计划复核。生产部署和
Q1 实施不在此次发布操作中。双平台 CI 包含 build/fresh-wheel/dependency gates；
独立实施审查、online/stress、完整云端预热/基线与 38h 正式阶段按 Q1–Q6 执行。
GitHub 发布不关闭未完成门，不启动暂停的自动化。后续开发只从当前 Q1 顺序推进。
