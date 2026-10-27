# P0：整理起点（对齐代码和验收口径）

日期：2026-10-26；更新：2026-10-27。状态：**R1 / R3 / R4复审通过，R2剩余分类缺陷已修正并验证，P0完成**。[复审记录](review-20261026.md#pr-87-复审2026-10-26)及[R2补充修正](review-20261026.md#r2补充修正2026-10-27)保留复现与闭合证据。原19项回归保留，新增7项批量涨落用例，共26项通过；现有五个边界按v4追加解释，未重新访问VPS。P1未开始。

- [x] R1：按物理 ordinal 和 Catalog 生命周期独立验证写入顺序，乱序负例失败。
- [x] R2：保留逐源新高历史，不因中途小幅回落漏判；增长负例及有界波动正例通过，[v4重新分类](corrections-20261026/r2-reclassification.json)已追加。
- [x] R3：全部七类输出先检查冲突，独占创建，失败保留 receipt；旧文件字节不变。
- [x] R4：D11 区分 U/C；USD-M 官方采样规则冲突明确未解决，不再假设 latest / largest。

新无 profiler 运行 STEADY；独立采样运行因输入逾期为 **NON_STEADY，仅供诊断**。两轮各 32,214 WS / 32,250 全源 Raw 的物理顺序、原文、数量和 fsync 回读均通过。不覆盖原结果，不以采样运行较少的 CPU 声称优化。

## 唯一开发起点与保留的工作

- 进入任务时本地 `main=9e8ec373a5e9e8bede36cd6f5ff9a21cd355fb3b`，落后已接受远端 main 108 commits。
- 所有原有已跟踪和未跟踪更改（含旧评审bundle、研究、旧计划退役和新plan）完整提交为 `d81e7d2`，先推送 `codex/p0-starting-point-20261026`，之后才对齐代码；没有 reset/stash 覆盖用户工作。
- 当时最新远端 `01d74212793e2bf1895f903a01868d6832b0b2cb` 合并为 `113cdb3d18f36c98408ca5e6e9e42297e7c430f3`。本次 P0 不改运行时源文件或依赖锁；其 `src` Git tree 为 `392154f9fc26923f88a465800d4a0a89b1fac2df`，与该已接受远端相同。
- 接受已有修复：USD-M E/T时间语义、有界owned ingress、stop优先级、稀疏OI evaluated windows、共享IP请求预算、独立日报进程、WebSocket断连诊断。没有把远端私有验收helpers当新架构依赖。
- [包/源对应清单](baseline/package-source.json) 逐个绑定126个运行时文件（125 Python + py.typed）；干净构建wheel逐项比较一致。wheel只在隔离开发环境测试，未部署。VPS已安装源仍是独立的 `20e57f3`；原核心路径基准正是该安装源，不能冒充本次开发基线。

## 数据和流程合同

[52个信息家族覆盖表（D11拆分后53行）](coverage.md) 明确市场/产品、端点/流、频率、字段、共享scope、权限、去重、缺口、必须性及实现归宿；[当前官方资料](official-sources.json)及[R4冲突记录](corrections-20261026/official-sources.json)约束API语义；官方冲突未解决前不猜测采样规则。USD-M两种top trader ratio当前要求key，无法无钥等价替代；如实列为权限受限。COIN-M/交割、新数据家族和原始HTTP正文属于P2，没有以文档宣布已实现。

[ADR-0051](../../adr/0051-production-continuity-and-incremental-verification.md) 已落实到数据、存储、项目合同与执行入口：逐必需源99.5%连续性，unknown计缺口，窗口并集不重复扣时；每次新封存文件独立完整读取校验，终点只补尚未验证的新文件。归档目标实际回读和删除授权保留。P1–P3实现后才可用于P4–P6，旧采样pass率不能冒充新覆盖率。

`AGENTS.md`、当前生产状态、handoff不再包含大段活动旧指令。对齐前的完整入口和两份退役计划保存在 `docs/history/pre-P0-*`，原始本地状态可从 `d81e7d2`还原；旧数据/失败/验收材料没有改成新计划通过证据。历史Python证据排除lint改写；运行时、tests、tools及本轮基准仍受检查。M0改查新plan并跳过安装依赖/已分类历史材料，保留原身份与数据合同检查。

## 性能起点与比较限制

[修正后 v3 基准](corrections-20261026/README.md)、[PR #86 原始测量](baseline/README.md) 与 [最初核心通路基准](../../research/2026-10-26/baseline/README.md) 分列。P0测量使用选定源、独立目录、固定paced WS输入、固定REST fixture/响应生成规则，真实现有Collector/服务组装和后台消费。原核心基准目录逐字保留。新市场、真实TLS/交易所网络及真实归档盘容量不在这个离线模型结论内。

## 验证与下阶段入口

以下是 PR #86 的历史验证，原成绩不改写为本轮：本地 macOS / Python3.12：2218 passed，24 skipped，5 deselected（189.17s）；mypy320文件、Ruff、M0、Go Raw v1 golden、sdist/wheel构建和干净wheel `--version/doctor/status`全部通过。未运行opt-in在线/压力/生产长跑。P0真实VPS测量两轮共64,428条WS原文核对通过，26个辅助源实例每轮均零失败，非空规范化/回放/日报及归档排空完成；低频profile1,590样本/2采样错误，限制如实保留。最终服务inactive/disabled/PID0，已安装20e57f3源未变。双平台CI在可执行修改提交8506262通过，最终只补结果/说明文档，不重复全仓运行。详见[验证记录](verification.json)。

本轮 19 项定向测试、Ruff、针对 helper 的 mypy、M0 和文档检查，以及修正提交的双平台 CI 见[修正验证](corrections-20261026/checks.json)。本轮未重复本地全套测试、生产数据审计或长跑。

R2补充验证及对应CI见[r2-checks](corrections-20261026/r2-checks.json)；原checks及测量文件保留。

P1从包含修正的main开始，只缩短Raw接收→落盘通路：当前已有批量线程移交，要实测真正批量系统调用、减少重复编码/任务创建和派生耦合。保留Raw/SQLite/校验兼容、有界缓冲、明确持久化进度、写失败传播和正常排空。用修正后的同口径输入交错A/B，报告snapshot/chunk数量、波动和资源差异；无实测收益的复杂化不保留。本任务不启动P1、P2或生产资格长跑。

## P1 contract application (2026-10-27)

P1 adopts [ADR-0052](../../adr/0052-bounded-raw-batches-and-sequence-only-capture-health.md)
and the [P1 data/progress contract](../../data_contract.md#p1-capture-and-progress-contract-2026-10-27).
Capture health is now a bounded snapshot/sequence bridge, explicitly not full-book
quality; append and fsync progress are separate. Raw v1 and archive readback/
retirement rules are unchanged. These contracts govern the P1 candidate and its
comparisons; the retained P0 runs, original readiness fields and v4 classification
are not rewritten or given new production credit. P1 measurements use the same
fixed input, instrumentation and classifier on both sides, with local machine
results identified separately from the preserved VPS baseline.
