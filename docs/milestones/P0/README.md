# P0：整理起点（对齐代码和验收口径）

日期：2026-10-26。状态：**COMPLETE**。P0四项交付均完成；P1未开始，生产资格未授予。

## 唯一开发起点与保留的工作

- 进入任务时本地 `main=9e8ec373a5e9e8bede36cd6f5ff9a21cd355fb3b`，落后已接受远端 main 108 commits。
- 所有原有已跟踪和未跟踪更改（含旧评审bundle、研究、旧计划退役和新plan）完整提交为 `d81e7d2`，先推送 `codex/p0-starting-point-20261026`，之后才对齐代码；没有 reset/stash 覆盖用户工作。
- 当时最新远端 `01d74212793e2bf1895f903a01868d6832b0b2cb` 合并为 `113cdb3d18f36c98408ca5e6e9e42297e7c430f3`。本次 P0 不改运行时源文件或依赖锁；其 `src` Git tree 为 `392154f9fc26923f88a465800d4a0a89b1fac2df`，与该已接受远端相同。
- 接受已有修复：USD-M E/T时间语义、有界owned ingress、stop优先级、稀疏OI evaluated windows、共享IP请求预算、独立日报进程、WebSocket断连诊断。没有把远端私有验收helpers当新架构依赖。
- [包/源对应清单](baseline/package-source.json) 逐个绑定126个运行时文件（125 Python + py.typed）；干净构建wheel逐项比较一致。wheel只在隔离开发环境测试，未部署。VPS已安装源仍是独立的 `20e57f3`；原核心路径基准正是该安装源，不能冒充本次开发基线。

## 数据和流程合同

[52项覆盖表](coverage.md) 明确市场/产品、端点/流、频率、字段、共享scope、权限、去重、缺口、必须性及实现归宿；[当前官方资料](official-sources.json) 是唯一API语义来源。USD-M两种top trader ratio当前要求key，无法无钥等价替代；如实列为权限受限。COIN-M/交割、新数据家族和原始HTTP正文属于P2，没有以文档宣布已实现。

[ADR-0051](../../adr/0051-production-continuity-and-incremental-verification.md) 已落实到数据、存储、项目合同与执行入口：逐必需源99.5%连续性，unknown计缺口，窗口并集不重复扣时；每次新封存文件独立完整读取校验，终点只补尚未验证的新文件。归档目标实际回读和删除授权保留。P1–P3实现后才可用于P4–P6，旧采样pass率不能冒充新覆盖率。

`AGENTS.md`、当前生产状态、handoff不再包含大段活动旧指令。对齐前的完整入口和两份退役计划保存在 `docs/history/pre-P0-*`，原始本地状态可从 `d81e7d2`还原；旧数据/失败/验收材料没有改成新计划通过证据。历史Python证据排除lint改写；运行时、tests、tools及本轮基准仍受检查。M0改查新plan并跳过安装依赖/已分类历史材料，保留原身份与数据合同检查。

## 可比较的性能起点

[稳态全现有功能基准](baseline/README.md) 与 [最初核心通路基准](../../research/2026-10-26/baseline/README.md) 分列。P0测量使用选定源、独立目录、固定paced WS输入、固定REST fixture/响应生成规则，真实现有Collector/服务组装和后台消费。原核心基准目录逐字保留。新市场、真实TLS/交易所网络及真实归档盘容量不在这个离线模型结论内。

## 验证与下阶段入口

本地 macOS / Python3.12：2218 passed，24 skipped，5 deselected（189.17s）；mypy320文件、Ruff、M0、Go Raw v1 golden、sdist/wheel构建和干净wheel `--version/doctor/status`全部通过。未运行opt-in在线/压力/生产长跑。P0真实VPS测量两轮共64,428条WS原文核对通过，26个辅助源实例每轮均零失败，非空规范化/回放/日报及归档排空完成；低频profile1,590样本/2采样错误，限制如实保留。最终服务inactive/disabled/PID0，已安装20e57f3源未变。双平台CI在可执行修改提交8506262通过，最终只补结果/说明文档，不重复全仓运行。详见[验证记录](verification.json)。

P1从当前分支/合并后的main开始，只缩短Raw接收→落盘通路：当前已有批量线程移交，要实测真正批量系统调用、减少重复编码/任务创建和派生耦合。保留Raw/SQLite/校验兼容、有界缓冲、明确持久化进度、写失败传播和正常排空。用同口径输入交错A/B，报告snapshot/chunk数量、波动和资源差异；无实测收益的复杂化不保留。本任务不启动P1、P2或生产资格长跑。
