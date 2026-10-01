# 全仓架构审查与 24h MVP 推进建议

日期：2026-10-01（Asia/Shanghai）。本次任务为 `ARCHITECTURE_REVIEW_AND_24H_PLAN`。

## 结论

保留现有 Python、单进程多 ProductKey、Raw v1、SQLite Catalog、原生 systemd
和验证后归档的架构。当前需要修复的主要问题是验收吞吐与负载不匹配、关键后台任务
未被监督，以及派生数据的产品隔离漏洞。无需重写 Recorder，也无需引入消息队列、
新的服务框架或原生语言引擎。

最短有效路线：修复已复现缺陷 → 使选定负载下的观测/完整审计可完成 → 冻结工件 →
完成 baseline/readiness → 独立 2h、12h、24h。每次阶段通过须包含 terminal audit
和独立验证；“进程运行够时间”不是 PASS。24h 是本任务终点，仍不等于 Production Ready。

## 审查基准与方法

- GitHub main 实时读取并 fetch 后固定为 `d6f37576f5b044bf578501cba291fe889e45a919`，
  tree `bc5a5f16964ddbf83e3871735e240c50845318bd`。
- 最新 runtime merge 为 PR #78 / `89bc6b41c0cb7d270ca6e4d2dec9a375137c028e`。
  原本地 WIP 位于 `354f5199…`，落后于主线；审查在独立 worktree 中进行，未修改原 WIP。
- 盘点全部 120 个 Python 源文件，共 52,466 行；进行模块级静态审查、关键调用链深读，
  配合现有全套离线测试与专项复现。不是每一行都获得独立正确性证明。
- 读过当前 handoff、production state、project contract、milestone plan、developer guide，
  以及 Raw、部署、readiness 与 V5 的相关契约。
- 本次未连接 VPS、未启用在线 Binance 测试、未启动 Recorder、未变更数据或部署。

| 审查域 | 源文件 / 行数 | 重点 |
|---|---:|---|
| Binance / network / collectors | 23 / 7,651 | public transport、原字节、代理、REST shared gate、重连与取消 |
| Raw spool / orderbook / supervisor | 18 / 6,485 | 有界队列、fsync、clean seal、恢复、序列与产品隔离 |
| Catalog / storage | 14 / 7,621 | 事务、生命周期、容量、注册路径、delta 查询 |
| archive | 11 / 6,841 | copy/readback/retirement、remote receipts、custody |
| service | 21 / 13,186 | runtime supervision、部署身份、V1–V5、terminal audit |
| normalize / replay / backfill | 13 / 3,865 | 去重、版本化输出、外部排序、确定性读取 |
| CLI / domain / metrics / soak 等 | 20 / 6,817 | 操作可用性、状态成本、可观测性与配置边界 |

## 已确认的缺陷

### F1 — P1：V5 单页消费速率不足以承载四产品常规封存/归档

位置：`storage/acceptance_delta.py:113`、`service/acceptance_v5_raw.py:386`、
`service/acceptance_v5_online.py:283`、`service/acceptance_v5_delta.py:284`。

每次 observation 每个 cursor family 只抓取一页、最多 256 行；replay 也拒绝超过
256 行。不是只存在于 SQL 的批次大小，而是一次样本的吞吐上限。ADR-0034 本身
规定了每 family 一页，因此这是负载/验收政策设计缺陷，不能只在实现中偷偷放大。

一个正常封存并本地归档至 `LOCAL_DELETED` 的 chunk 产生 3 个封存生命周期 transition
及 5 个 archive chunk transition，共 8 个 chunk transition、5 个 archive event。
四产品 × 三核心流 × 五分钟/一分钟封存，光核心就约 60 个 chunk/窗口，即
480 个 chunk transition、300 个 archive event；不包含辅助流、大小触发封存和重连。

离线用真实 RawChunkWriter、seal_partial、Catalog、ArchiveManager，两轮各生成 60 个
正常 chunk；仅时钟、OS/readiness、baseline reference 与进程边界使用现有合成夹具：

| 样本 | chunk high-water / processed | archive high-water / processed | pending |
|---|---|---|---|
| 300s | 480 / 256 | 300 / 256 | true |
| 600s | 960 / 512 | 600 / 512 | true |

没有 integrity finding，独立 online replay 接受证据；处理仍持续落后。持续该负载时
正常 timed target 会留下 `target_delta_pending`，无法获得可推进的最终结果。
这不是已发生的 V5 live failure：V5 尚未开始任何 Formal stage。

最小修正必须同时考虑行吞吐、8 MiB document、240s 工作预算、跨游标 pending references。
不要简单把 256 改成一个很大的常量。先用真实负载测试判断：调整已有 rotation 参数能否
在保持 1s durability、128 MiB 大小上限和完整性语义下充分降低行率；否则用一个明确
ADR 修订为预算内的有界分批消费，复用已有 shard/chain 工具，保持同一 snapshot 边界
和连续游标确认。不需要再建通用事件框架。

### F2 — P1：heartbeat 异常未被 runtime 主流程监督

位置：`service/runtime.py:566`、`:761`、`:859`、`:891`。

heartbeat 被 create_task 后，主等待集合只有 collectors/capacity。heartbeat 发生
state publication 或相关异常时，采集继续；finally 用 `gather(return_exceptions=True)`
取走异常，却没有改变 failure 状态。离线注入 OSError 后两个 FakeCollector 仍运行，
外部请求正常停止后最终状态是 `STOPPED`。不是已证实 Raw 数据丢失，但健康证据链
失效不应被写成正常服务结局，也不能靠外部 observer 才发现。

最小修正：把 heartbeat 作为必需任务纳入已有 supervisor 等待，unexpected return 和
异常统一触发有序 stop、保留原因并传播失败；正常 stop 不制造失败。没有必要实现新的
supervisor 框架。全局辅助任务的终态需显式反映 degraded 状态，不必因此停止核心采集。

### F3 — P1：部分 normalized 去重键不含产品身份，会合并不同产品

位置：`normalize/parser.py:198`（depth_snapshot）和 `:123`（server_shutdown），
`normalize/pipeline.py:278`、`:430`。

depth_snapshot 的 semantic key 只有 market、update ID、model hash；logical hash
也不含 symbol。两个不同交易对具有相同 snapshot 模型时，去重器把它们当成同一事件。
离线把 BTCUSDT、ETHUSDT 两个有效快照交给真实 candidate/dedup 路径，输出只剩
BTCUSDT，`duplicate_count=2`。serverShutdown 键只含事件时间，代码中同样缺少
market/symbol/stream 隔离；此分支的风险由代码确认，本次没有另做端到端复现。

最小修正：在所有 semantic identity 的统一边界纳入 venue/market/symbol/stream；
补充跨产品/跨流同值夹具并保留真正重复的合并。评估 dedup version/build ID 的变化，
既有 immutable build 保留原语义，不能原地改写。此缺陷影响离线消费，不等于 Raw
采集错误；修复不应顺带重写整个 normalized contract。

### F4 — P2：normalization 外部排序同时打开全部 runs

位置：`normalize/pipeline.py:378`、`:396`。

每 10,000 行生成一个 run，然后为所有 run 初始化迭代器；逐个预读第一行时全部文件
保持打开，heap 与句柄数量随总输入增长。专项复现将 run size 缩为 1，以 40 行生成
40 个 run，观测到 40 个 reader 同时打开。大输入可超过 RLIMIT_NOFILE；当前 replay
模块已经有 32 路分层归并模式，可以直接采用同类结构。去重组也可能随重复次数增长，
应另行度量；本次不把未复现的 OOM 记作新缺陷。

该路径不在 VPS live capture 内，可排在 24h 后，不必扩大本轮发布范围。

### F5 — P2：内存保留全部 quality audits

位置：`orderbook/reconstructor.py:110`、`:115`、`:244`。

即便提供 audit_observer，仍把每个 audit 追加到 list。同步后反复输入 10,000 个
重复 depth update，保留 10,000 个 audit。生产 readiness 会在重建实例时重置对象，
所以不是所有历史跨重启无限累积；稳定会话内仍会随重复/异常事件数增长。

最小修正：生产模式保留有界 recent ring 和计数，仍把需要保留的完整性事实写入已有
持久路径；兼容离线诊断和 checkpoint 的 interval 语义。不能直接截断唯一的 gap 证据。
无需因此引入新的日志存储系统。24h 前只有实际持续增长证据或已启用生产有界模式才
要求改动；否则列为资源 watch，不据此断言当前生产已 OOM。

### F6 — P2：repository discovery 对无权限 cwd 的异常会击穿 CLI

位置：`paths.py:37`–`:45`，`version.py:31`、`cli.py:209`。

`discover_repository_root` 遍历 cwd ancestors 的 `.git`，未处理 PermissionError；
parser 构建提前调用 version_string，无法只依靠后面的 command error handling。
离线注入该异常会直接传播；最新 owner-stop 文档也记录了 runuser 继承 `/root` cwd
导致 archive status 失败，而换到可访问 data root 后成功。

最小修正：不可访问的 discovery candidate 跳过，继续 module path candidate；Git
revision 不可用时返回 unknown。实际 data-root 的包含性/权限校验仍保留。补一个
installed CLI + unreadable cwd 回归即可。

## 明显可安全优化的部分

| 建议 | 依据 | 安全边界 / 优先级 |
|---|---|---|
| 常规 archive status 默认 aggregate，明细显式分页或 --details | `archive/manager.py:145` 当前加载/序列化全部 transactions；现有 Catalog 已提供 aggregates | 保持详细接口可用；日常 monitor 不再搬运 143k 历史行。24h 前完成 |
| scan_raw 用一个 descriptor/压缩流同时累计 stored SHA 与解压扫描 | `acceptance_v5_raw.py:88` 先整文件 hash，再 reopen/decompress | 两种 SHA、CRC、统计、fd/path 稳定性都保留；需覆盖 zstd 尾部与读取消耗。先离线比较再优化 |
| 提高现有 rotation_seconds 作为选定 profile 的候选参数 | 默认 60s 造成大量小 chunk；size cap 与 1s fsync 独立 | 属于配置/部署身份变更；须实测 chunk 行率、恢复和 gap 粒度，不用它掩盖未承载任意配置的问题 |
| 取消热路径重复环境/包全量扫描，只保留适当周期的完整核验 | `_live` 与 evaluator 都会调用 identity verification；每次校验 installed RECORD / lock / Wheel | 先量化成本。缓存绑定精确文件身份、失效规则和边界；不要无条件永久相信第一次结果 |
| normalize 限定 merge fan-in | F4；replay 已采用分层归并 | 离线 consumer 迭代，24h 后即可 |

目前没有完成生产 profiling 的新证据支持线程池扩张、异步 seal、CRC/SHA 删除、改浮点、
C++/Go 重写或更长 durability。它们不进入 24h 计划。

## 过度设计 / 过度防御的判断

1. **验收系统的重复工作已成为主瓶颈。** V5 first pass 扫完所有 Raw，然后
   `acceptance_v5_finalize.py:386` 再用同一个 AuditRecords/scan_raw 实现做完整 LIVE
   reconstruction。它是从原始权威重新计算，不是不同算法的独立实现。再次读取可以
   检出两次扫描间变化，但成本不能用“独立”一词免于评估。现有契约要求两遍，当前不能
   悄悄删掉；先合并同一遍中的重复 I/O，再决定是否用明确的 ADR 改为单次完整 Raw
   qualification + 独立证据/control replay，并讲清对扫描间变化检测能力的取舍。
2. **把所有旧数据反复审计绑定到每次新运行验收，不适合快速 MVP 迭代。** 停止的基线
   超过 14h，只发布 87,423 / 143,362 个首轮 manifest record；独立 LIVE 复核尚未开始。
   这是 partial measurement，不是 integrity FAIL，也不能据此宣称完成 throughput。
   最终计划应区分新工件的 bounded qualification 和旧 archive custody/audit；旧数据仍需
   完整审计，但不要让它变成每次修一个代码缺陷都必须重跑的历史成本。
3. **过多历史流程掺在当前计划中。** 当前 milestone_plan 仍很长，早期 TIME-LOCAL
   NEXT 有可能误导操作；新增当前计划用短的 scope/gates/next 表，历史材料保留。无需
   在同一次修复里重新整理整个文档体系。
4. **固定源仓库路径断言不是软件正确性门。** 本次全套测试只有 M0 的 checkout-path
   断言失败，使用文档现有 CI override 即通过。未来可将 portable source/build 检查
   和生产 data-root 约束分开；后者有实际保护意义。
5. **大文件或检查较多，本身不证明过度设计。** Raw byte fidelity、1s durability、
   crash reconciliation、archive readback-before-delete、ProductKey identity、gap
   evidence 和存储边界均有实际故障案例与合同支撑，应保留。V1–V4 reader 是历史
   兼容能力，可隔离维护；不用在 24h 前删除或重写。

## 验证结果

解释器为原 WIP 的只读复用 `.venv-ms2/bin/python`（Python 3.12.9），source 通过
`PYTHONPATH=src` 指向当前 main worktree。未改写该环境或安装包。

| 命令 | 结果 |
|---|---|
| `PYTHONPATH=src <python> -m pytest -q` | 1,837 PASS；1 个 M0 checkout-path FAIL；24 skip；4 stress deselect；150.28s |
| `CI=true M0_CONTRACT_ROOT="$PWD" PYTHONPATH=src <python> -m pytest -q tests/test_m0_contracts.py` | 1 PASS；上述唯一失败已用文档支持的 sibling-checkout 参数解决 |
| `<python> -m ruff check .` | PASS（初始主线及最终新增文件检查） |
| `PYTHONPATH=src <python> -m mypy` | PASS，276 source files |
| `CI=true M0_CONTRACT_ROOT="$PWD" <python> tests/verify_m0_contracts.py` | PASS |
| `go run tools/verify_raw_chunk_golden.go` | PASS |
| `PYTHONPATH=src:. <python> docs/reviews/2026-10-01-review-probes.py` | 六项诊断完成；结果见 JSON |

13 个已有 fault-test fork warnings 仍存在；不据此推断生产 spawn worker deadlock。
裸 python3.12 缺 pytest/Ruff/mypy 后改用已有环境，上表才是有效检验。
在线 smoke、stress、Linux/systemd 现场、clean-wheel rebuild、生产 Raw 审计与长跑未执行：
本次是审查/计划，未改变 runtime；后续发布必须单独完成 exact artifact checks。

复现脚本是诊断工件，不是新的生产模块，不伪造 Formal credit。它对 F1 使用真实
文件/数据库/归档路径，对 OS、时钟与 predecessor 采用现有测试夹具；不能替代 VPS
整体资格证明。见 [脚本](2026-10-01-review-probes.py)、
[诊断输出](2026-10-01-review-probe-results.json) 和
[推进计划](../qualification_to_24h_plan.md)。

本次 runtime、Raw、Catalog/schema、依赖锁、配置、service units 与 deployed identity
均未改变；审查后的代码修复和架构取舍是后续任务。
