# Binance Market Data Recorder — C++ 最小上线收尾计划

> **更新：2026-11-12；代码审查基点：`53267dd4`（C4-E 实现提交 `b610a656`）。**
> 本文件是团队执行入口；2026-11-11–12 已按 F1–F6、F7、C4-C 执行，结果见进度、表格和部署记录。
> 旧计划完整保存在 [历史快照](cpp_rewrite_plan_history.md#plan-snapshot-20261111)；旧文中的“直接再跑 24h”“等待所有者选择已实现的 E1/E2”和旧二进制冻结指令不再作为待办。
> 状态依据仓库代码、文档及已保存报告，未查询 VPS 实时状态。开始执行时先确认服务器没有其他正在运行的采集任务。
> **执行进度（2026-11-12）：** F1–F7 已完成，C4-C 24h 资格 **PASS**（2026-11-11 07:46:56–2026-11-12 07:47:27 UTC，86,409 s，发布 `20261111-c4c-e0b7f684`，新根 `20261111T074613Z`）；首次验收因额外的 1 s CPU 采样断言失败，修正验收脚本后 `--verify-only` 对同一份数据 PASS，未重采。C5 尚未开始。原生矩阵、安装身份、F7/C4-C 结果见 [C3–C5 部署记录](c3-c5-deployment.md#f1f6-执行记录2026-11-11候选-e0b7f684)。

<a id="status"></a>

## 1. 当前结论与证据

**C++ 主体已经实现，尚未达到生产签收条件。最短路径是补齐异步路径验证、修复实测阻塞与缺口记录，再完成一次 2h 预检、一次 24h 资格和 C5 交接。无需重写架构。**

| 阶段 | 当前状态 | 后续处理 |
| --- | --- | --- |
| C0、C1 单源、C2 精简可靠性、C3 四币对 | 已签收 | 保留结果，不重新立项 |
| C4-A 写路径、C4-B 60 分钟、C4-D 归档/快速读回 | 已完成；针对各自当时版本 | 不重做优化、不另加一轮 60 分钟采集 |
| C4-P 同范围 Python/C++ 对比 | 已完成；两个市场时间窗口不同 | 证明已有性能收益，不再重复基准，不当作当前候选容量证明 |
| C4-E 异步封存、syncfs、128 MiB 入口队列 | 已实现；历史原生 Release 13/13、ASan/UBSan 9/9、TSan 9/9 | 测试存在生产路径盲区，必须补下面的针对性用例 |
| C4-C 24h | ✅ PASS（2026-11-12）；历史两轮失败保留 | C5 启用与交接 |
| C5 常驻与交接 | 未签收 | C4-C PASS 后完成启用、受控重启和回退演练 |

第二轮失败是当前阻塞，不是仍在等待完成的长测：

| 指标 | 第一轮 C4-C | 第二轮 C4-C（C4-E） |
| --- | --- | --- |
| 结束时间，上海时区 | 2026-11-10 17:17 | 2026-11-10 22:08 |
| 运行时间 | 26,828 s，约 7h27m | 7,041.9 s，约 1h57m |
| Spot / USD-M 丢弃 | 0 / 23,385 | 4,889 / 1,102,272 |
| USD-M 丢弃 / 已写总记录 | 0.0572% | 5.8815% |
| 入口队列 | USD-M 达到 64 MiB | 两市场均达到 128 MiB |
| 最大 receive→fsync | 17.85 s | 47.89 s |
| 系统盘最小可用 | 18.63 GiB | 18.46 GiB |

上述百分比来自失败摘要，分母包含 REST；正式资格仍用“已接受 WS 记录数”，见第 7 节。第二轮两个记录器峰值 RSS 相加为 **414,978,048 B = 395.75 MiB**，尚未加归档，已不能通过现有 `qualify.py` 的 **384 MiB 峰值上界**门槛。这是分别取峰值后相加，不是同一时刻的 RSS 实测值；不能据此宣称整机内存不足，也不能继续假设“128 MiB 队列保证 RSS 达标”。

证据：[首轮失败](c4c-drop-failure-20261110.json)、[第二轮失败](c4c-drop-failure-run2-20261110.json)、[C4-E 原生结果](c4e-native-tests-20261110.json)、[C4-P 对比](c4p-cpp-vs-python-20261110.json)、[部署记录](c3-c5-deployment.md)。第二轮二进制 SHA-256 为 `618dae987fce00d7e19fb4a32d87f43c613e433b3e104e516d8aca61158d6b99`；配置 SHA-256 为 `75d1e9ad6fc54d4ab5118362f4c503384f018dcc90a17074108a321964e9bae8`。

### 1.1 审查范围

覆盖仓库自有 C++、Python 适配/离线工具、测试与故障注入、CMake、配置、systemd/发布脚本，以及 C0–C5 文档、历次计划和结果报告；沿实际调用链核对冻结 Python 的 Raw reader、恢复、Catalog、归档和 normalize。另核对相邻 `BinanceMarketDataContracts` 的项目边界：它不是当前录制器上线的新增运行依赖。

冻结 Python 基线仍为 `38acb1d0` 加已有 C4-D 补丁，`BASELINE.json` 树哈希为 `cd615fc1906589ad4e9de31fa64722819397eb2469b36956018fd586a8143e2c`。本轮不把整个 Python 产品重新审计、重构或迁移列为前置条件。

<a id="findings"></a>

## 2. 需要解决的问题

| 优先级 | 已确认的问题 / 尚未证实的判断 | 代码依据与影响 | 对应任务 |
| --- | --- | --- | --- |
| P0 | 入口持续满、严重丢弃已确认；**封存队列就是本轮根因尚未确认** | [`enqueue_seal`](prototype/recorder_storage.hpp#L305) 在 64 个待处理任务时等待；写线程仍执行 `syncfs`、Catalog 写入，也会等待 Catalog 锁。报告没有封存队深/交接等待证据 | F1、F2 |
| P0 | 故障测试绕开实际异步路径 | [`recorder_run.hpp`](prototype/recorder_run.hpp#L237) 用 `config && test_queue_records == 0` 决定异步封存；`c4-ingress-drop` 设置小队列后变成同步封存。单源恢复用例也未覆盖配置模式的 `syncfs` 和取消 header 同步 | F1、F6 |
| P1 | WS 丢弃明细可在重连、停机、稀疏流无后续帧时丢失；REST 丢弃未写 Catalog | [`read_ws`](prototype/main.cpp#L650) 的 `drops` 是连接局部变量，只搭载到下一条成功入队帧；[`receive_rest`](prototype/main.cpp#L1118) 只增加计数/日志。日志限频不能替代逐源缺口账本 | F3 |
| P1 | 已知 depth 丢帧未显式使桥接失效 | [`DepthBridge::observe`](prototype/recorder_core.hpp#L164) 处理 generation、malformed 和序列，却不直接处理 `ingress_drop`；Spot 序列范围重叠时仍可能继续标 `depth_bridge` | F3 |
| P1 | 内存预算尚未通过 | 第二轮记录器峰值之和已为 395.75 MiB；入口计费字节不是进程 RSS，封存任务还持有 RawFile/描述符/最后一条 envelope | F2、F7 |
| P1 | 验收可能漏检或误报；脚本失败还会导致重复采集 | [`operations.py`](prototype/operations.py#L61) 的快速路径信任归档元数据，不能声称逐帧 CRC/记录数独立全检；[`qualify.py`](deployment/qualify.py#L315) 没有 verify-only 入口，换连只用全局 `any`，归档等待 300 s 与 unit 的 600 s 不一致 | F4 |
| P1 | 发布收尾不完整 | [`install.py`](deployment/install.py) 过早切换 `current`、未检查活动 timer、会继承旧 production drop-in；[`archive_tick.py`](deployment/archive_tick.py) 只看退出码；[`advance.py`](deployment/advance.py) 自动启用不等于已完成受控重启和回退验证 | F5、C5 |

**纠正旧归因：** Spot/USDM 各自有一个封存队列，不能拿两个进程合计的 80 条 WS 流与一个 64 格队列比较。当前存储按 symbol 合并 REST，正常槽位上限约为 Spot `10×(3+1)+1=41`、USD-M `10×(5+1)+1=61`；稀疏源未出消息时还会更少。磁盘上 111 个 `.partial` 也可能包含排队/正在封存的旧分片，不等于 111 个活跃槽位。多次大小轮转仍可能累计超过 64 个封存任务，所以需要复现和等待计时；“改成 256 就解决”现在只能是待验证的候选。

<a id="scope"></a>

## 3. 固定范围与实现边界

| 项目 | 保留的决定 |
| --- | --- |
| 标的/实时源 | [十个 USDT 对](config/c4-mainstream.json)；Spot depth/bookTicker/aggTrade；USD-M 另含 markPrice/forceOrder；现有 exchangeInfo、depth 快照和五路 300 s REST |
| 存储 | Raw v1、Zstd level 3、manifest、SQLite WAL/NORMAL；900 s / 16 MiB 轮转 |
| 并发 | 每市场一个进程，现有 WS/REST worker、单 writer、**单 sealer**；Python 独立归档 |
| 入口队列 | 每市场 128 MiB / 131,072 条，沿用固定消息开销计费；不继续扩容入口队列 |
| 耐久 | 配置模式 5 s 调度一次 Linux `syncfs`，正常停止/封存完整同步；不承诺“最多丢 5 s” |
| 资源 | slice 175%；归档 CPUQuota 50%；Spot/USDM CPUWeight 100/200；资格显式 `--cpu-limit-percent 200 --rss-limit-mib 384` |
| 磁盘 | 启动代码门槛 15 GiB，长测准备至少 18.5 GiB，运行门槛 13 GiB；只按验证成功的归档事务回收源文件 |
| 丢弃 | 目标 0；最终每市场 `(WS 丢弃 + REST 丢弃) / 已接受 WS 记录数 × 100 ≤ 0.10%`，沿用现有保守分子，同时分列 WS/REST |
| 离线/部署 | 继续 vendored Python、现有 staging + install.py + systemd；不增加 Contracts/Gateway/Projection 依赖 |

5 s 是同步调度周期，消息可能先在入口/封存队列等待，系统调用本身也可能阻塞；第二轮 47.89 s 的耐久延迟已说明两者不同。`syncfs` 同步的是文件所在文件系统，不能当作只刷当前分片的无阻塞调用，参见 [Linux 接口说明](https://man7.org/linux/man-pages/man2/syncfs.2.html)。本轮不新增同步线程；先用 F1 的证据判断它是否仍是主阻塞。

不增加线程池、watchdog、消息总线、新状态机/配置平台、C++ 离线 reader、submodule/wheel/CI 迁移；不减币对、不缩分片、不放宽资源或丢弃门槛来通过验收。已完成的 C4-D 和 C4-P 不重做，不追加 72h soak。

<a id="execution-order"></a>

## 4. 团队推进顺序表

下表是默认串行顺序。F4/F5 可由团队在 F1–F3 期间准备，但合并候选后才统一执行 F6；不用为每个小提交重复完整测试矩阵。

| 顺序 | 工作项 / 建议负责角色 | 输入与最小交付 | 完成后才能进入 |
| --- | --- | --- | --- |
| 1 | ✅ **F1 复现与测试纠偏** / C++ | 已完成：配置模式固定走异步 sealer，增加封存队深/交接等待/Catalog 等待指标；十币对慢 sealer 复现出 64 格下 1.69/2.53 s 的 `enqueue_seal` 等待。详见 [F1–F6 记录](c3-c5-deployment.md#f1f6-执行记录2026-11-11候选-e0b7f684)。 | F2 |
| 2 | ✅ **F2 修复已复现的阻塞与内存问题** / C++ | 已完成：具名 `seal_queue_capacity=256`；同用例等待降至 30/71 µs、无丢弃、队列排空，RSS 约 69 MiB/进程、打开文件 122/213 已记录。 | F6 |
| 3 | ✅ **F3 补齐丢弃与 depth 语义** / C++ | 已完成：进程级 WS/REST 丢弃账本、停机尾部 Catalog 事件、`ingress_drop` 触发 depth reset；普通/尾部/重连/REST/缩时换线用例通过。 | F6 |
| 4 | ✅ **F4 修正验收与可继续执行入口** / Python | 已完成：Catalog/manifest 集合与身份对账、缺口分片解码、WS/REST 丢弃对账、逐连接组 23h、`--verify-only`、`--finish-only`、归档 blocked；离线边界测试通过。 | F6 |
| 5 | ✅ **F5 发布准备** / 部署 | 已完成：安全安装顺序/备份恢复、runtime venv 按 hashed lock 重建、`Restart=no` 与 systemd 属性检查、旧 `production.conf`/logrotate 处理、RUNBOOK；原生安装 `20261111-e0b7f684`。 | F6 |
| 6 | ✅ **F6 原生回归并冻结候选** / 测试 | 已完成：Release 22/22、ASan/UBSan 10/10、TSan 10/10（`setarch -R`）；冻结提交 `e0b7f684`、Release `ba71faa3…`、配置 `75d1e9ad…`。 | F7 |
| 7 | ✅ **F7 一次 2h 公网预检** / 运维 | 已完成：2026-11-11 05:26–07:26 UTC 连续 7,200 s，Spot 4,093,454 / USD-M 13,993,992 条，0 丢弃；首次验收脚本缺 `PYTHONPATH`，按 verify-only 重验同一份数据 PASS；尾部 31/61 分片归档后全部 `LOCAL_DELETED`。 | C4-C |
| 8 | ✅ **C4-C 一次 24h 资格** / 运维 | 已完成（PASS）：2026-11-11 07:46:56–2026-11-12 07:47:27 UTC 连续 86,409 s；Spot 51,679,919 / USD-M 183,369,877 条，0 丢弃；三连接组 23h 换线；16,716 chunk 全部 `LOCAL_DELETED`；RSS+archive 270.7 MiB（283.8 MB）、CPU 合计+归档 79.7%、60 s 峰值 168.7%；一小时离线消费 invalid=0、60 根 K 线。 | C5 |
| 9 | **C5 启用与交接** / 运维 | 同发布物常驻、一次受控重启、隔离根回退演练、最终签收表 | 生产签收 |

<a id="f1"></a>

## 5. 最小修复与实现步骤

### F1 ✅：让测试真正经过生产异步路径，再定位阻塞

**改动位置：** `prototype/recorder_run.hpp`、`recorder_storage.hpp`、`fault_io.cpp`、`verify_rotation_burst.py`、`verify_recovery_boundaries.py`、`CMakeLists.txt`。

1. 配置模式是否异步只由生产配置模式决定，**小测试队列不再关闭 sealer 或恢复首帧同步**。单源旧模式继续保留。丢弃用例改用现有 writer pause 或明确的 writer 故障注入触发拥塞，不能依赖“阻塞 rename 就一定阻塞 writer”。
2. 在现有 status/final report 增加：封存队列当前/峰值任务数、累计及最大 `enqueue_seal` 等待时间、待封存 raw 字节数；对 writer 调用的 Catalog 操作记录累计/最大耗时（包括锁等待）。保留 `writer_sync_ms`。这些是几个计数器/计时点，不增加采样线程或逐帧日志。
3. 修正指标解释：异步后的 `writer_seal_ms` 实际来自后台封存的区间累计耗时，不能继续解释成 writer 阻塞时长，也不能与旧同步版本直接归因比较。writer 是否等封存只看新增交接等待。
4. 扩展一个回环场景：使用十币对配置，Spot 和 USD-M 分别运行；按真实分组产生 WS + REST；同时有定时轮转、同流重复大小轮转和一段可解除的慢 sealer，使**待处理任务**确实触及 64。随后恢复正常磁盘/消费速度。故障延时只在测试启用。
5. 保存旧候选在该场景的队深、交接等待、sync/Catalog 耗时、入口高水位和丢弃；必须能说明“在哪个调用等待”及解除延时后是否追平。若队列未满而 writer 仍停顿，继续用已加计时定位 sync/Catalog，不能把磁盘文件数当成根因证据。

**完成条件：** 一个可重复的旧路径失败/阻塞用例；能区分封存交接、syncfs、Catalog 三个等待点。人工慢 sealer 只证明该路径会阻塞，不能单独证明历史公网失败就是同一原因；历史关联由 F7 指标验证。

<a id="f2"></a>

### F2 ✅：只修确认的等待点，保持有界

**改动位置：** 以 `prototype/recorder_storage.hpp` 为主；只有实测指向入口对象占用时才改 `main.cpp`。

1. **若 F1 确认是短时封存积压，且解除慢盘后单 sealer 能追平：** 把 64 提为一个具名的固定容量，先验证 **256** 这个候选。保留单 sealer、条件变量、有界等待和现有错误传播；不增加无界溢出队列。采用它的条件是同一复现下 writer 等待/丢弃消失或明显消退，且下面的资源/收尾约束通过；不能仅因“256 大于流数”合入。
2. **若 sealer 长期追不上，或实际主等待不在交接：** 不采用队列扩容。修 F1 指出的具体同步操作，例如移出持 Catalog 锁期间不必持锁的工作、消除已证明的重复 I/O。不得删除封存发布所需同步/事务、吞掉 `syncfs` 错误或靠增加 busy timeout 掩盖问题。需要新并发架构时，本次局部方案判为不足，提交证据后再调整范围。
3. 在同一复现中记录 pending raw 字节、打开文件数、RSS、排空耗时。256 个约 16 MiB 的 raw 任务仅排队源文件就约 4 GiB/进程，16 MiB 是轮转目标而非严格文件上限；不能把队列任务数理解成“几乎不耗资源”。
4. 同时检查 RSS 是否随入口/封存积压上升、排空后是否稳定。优先去掉已确认的多余 payload/envelope 副本或不再需要的持有对象；不先引入内存池。资格仍按两个 recorder 峰值 + archive 峰值上界 ≤384 MiB，不改成另一种采样口径来绕过失败。
5. 慢 sealer 场景结束后 SIGTERM：已接受记录排空、sealer join、Raw/manifest/Catalog 一致；sealer 异常时 writer 被唤醒、进程非零退出，未完成文件留给恢复。测量正常排空 ≤现有 180 s，恢复启动 ≤现有 300 s。先看积压原因，不先扩大 unit 超时。

**完成条件：** 有同场景修复前/后结果；生产规模下积压可追平；正常与异常停止无死锁、无重复封存；资源预算仍有归档余量。256 未通过就不作为默认值，不做多轮猜容量。

<a id="f3"></a>

### F3 ✅：保证已知丢弃可对账，depth 在缺口后重新桥接

**改动位置：** `prototype/main.cpp`（Handoff/read_ws/receive_rest）、`recorder_run.hpp`、`recorder_storage.hpp`（Catalog）、`recorder_core.hpp`（DepthBridge）。

1. 把连接局部 `DropState` 移到进程生命周期内的现有交接对象，按 market/symbol/WS stream 或 REST source 保留计数、首末 UTC/monotonic 时间；复用现有互斥锁。条目数受已配置源集合约束，不保存被丢弃 payload。
2. WS/REST 拒收均通过同一个记账入口。下一条对应源消息**成功入队**时附上现有 `ingressDropped`/时间范围和 `ingress_drop`、`sequence_gap`，再清理已交接部分；失败入队不能先清零。跨 WS reconnect 保留状态。
3. 正常停机沿用现有顺序：停止并 join producers → 关闭/排空 Handoff → writer 取出未搭载的尾部缺口 → 写 `INGRESS_DROP` → `storage.finish()`。复用现有事件表，允许 Catalog 接受一条缺口摘要，不为写事件伪造行情 payload，也不让网络线程写 SQLite。
4. 有后续帧的缺口进入该 Raw/manifest 的 flags，`complete=false`；无后续帧的尾部缺口标记仍可写的对应槽位并写 Catalog。若对应文件早已封存，以 Catalog 的终止缺口说明覆盖中断，不重写不可变文件，不制造空分片。读回报告必须展示这类尾部缺口。
5. `DepthBridge::observe` 遇到明确 `ingress_drop` 时先使旧 snapshot/last/pending 失效，走现有快照请求与 epoch 机制，再处理当前帧；同帧 generation 变化与 drop 合并成一次 reset。复用 REST 限额，不新增快照重试层。
6. 回归覆盖：丢后恢复、丢后重连、丢后直接 SIGTERM、稀疏 forceOrder 无下一帧、REST 拒收；再加一个 Spot U/u 范围重叠仍不能沿用旧 bridge 的用例及 USD-M pu 用例。按 WS/REST 分开核对事件总数。

**完成条件：** 正常闭合运行满足 `Catalog INGRESS_DROP 的 dropped 总和 == ws_dropped_total + rest_dropped_total`，无重复计数、身份/时间区间正确；已知 depth 缺口后只有新快照桥接完成才再标 `depth_bridge`。崩溃前尚未持久化的内存计数不承诺精确恢复，继续按恢复边界标未知缺口，不新增逐次丢弃 fsync。

<a id="f4"></a>

### F4 ✅：让验收检查与报告一致，允许复用已完成的采集

**改动位置：** `prototype/operations.py`、`deployment/qualify.py`、`archive_tick.py`、`advance.py`，必要的连接事件字段在 `recorder_run.hpp`。

1. **统一快速读回口径。** 采用已实施的 C4-D：归档阶段全文件 SHA/大小验证，停机阶段对全部分片核对 Catalog、manifest、归档事务及文件存在/大小；REST、多连接、每源首尾、未归档本地分片逐帧 CRC/解码。旧计划“快速 verify 同时完成 100% 逐帧 CRC/独立计数”的表述撤销，不能把信任 manifest 的计数称作全量独立读回。保留 `fast_path` 覆盖字段，不恢复十余小时全量 CBOR/Pydantic 重验，也不写新 reader。
2. 增加 Catalog chunk_id 集合与 manifest 集合对账，确保没有漏掉整个分片；核对 `record_count`、hash、大小及 terminal 状态。已有归档身份验证继续复用。带 `ingress_drop`/`sequence_gap` 的分片加入现有解码集合，核对实际 flags/缺口事件；尾部缺口从 Catalog 报告。总记录数与 recorder 对账时明确哪些来自 manifest、哪些逐帧计数。去掉本地非 trusted 分片在 `_catalog_chunks` 与 `_verify` 的同次重复哈希验证。
3. 按 F3 核对已知丢弃总数、WS/REST 分项、源身份和时间区间。计算正式门槛时分母是已接受 WS 记录，不用失败 JSON 中的 `records`；没有 WS 记录不能算 0% 成功。
4. `WS_RECONNECT` 增加连接组（Spot public、USD-M public、USD-M market）及原因（23h renewal / transport failure）。替换全局 `any` 为逐组检查，确认换连后仍收数，depth 两组逐 symbol 再桥接。23h 计时逻辑用缩时回环测试覆盖；公网运行中存活到 23h 的连接必须主动换连，提前因网络重连重置计时的组如实记录“未达到定时触发条件”，不能伪称 renewal，也不因正常重连额外补跑一个 24h。
5. 给 `qualify.py` 增加 **`--verify-only`（待实现）**：只读取已停止运行的报告/数据，核对 release、config、root、采集时长及结束摘要后调用现有 `verify_stopped`，不得启动服务或改写采集 T0。报告增加配置和发布脚本身份。给 `advance.py` 增加一个仅继续收尾的入口，要求同一份资格报告 PASS 后才调用现有 `finish_production`，避免修脚本后又安装新根、再采集 24h。
6. 复用 qualifier 的 30 s 检查增量读取本轮 status/丢弃事件，保存各市场累计值和封存高水位。把现有 USD-M `>150,000` 条中止线写成显式运行参数/报告字段；Spot 用预检接受量折算 24h 的 0.10% 作为预先记录的中止线。这只是避免已明显失败还继续跑的操作线，不能代替最终比例验收。异常退出、达到中止线或磁盘紧急停止时保存原因、停 timer、正常停止 recorder，不全量读回失败轮。
7. 归档静止等待统一使用现有 archive unit 的 600 s 预算；成功/失败收尾都停止 timer 并等 worker 结束，不能只在成功路径处理。`archive_tick` 解析已有 `archive_drain` JSON：目标不可用/低空间应报告 blocked，不能仅因 exit 0 记成功；正常无积压的 idle 不算失败。final drain 若有积压且无进展/目标不可用，带原因结束，不能空转 100 次。
8. 验证脚本用少量离线 fixture：一份正确闭合结果、丢失 manifest、缺口漏记、一个连接组未通过、归档 blocked、verify-only 不启动采集。只验证这些实际缺口，不给每个 JSON 字段机械添加测试。

**完成条件：** 不能用快速路径冒充全量逐帧验证；漏分片/漏缺口/单连接组漏验会失败；已成功采集的数据可单独重验和继续 C5。修验收逻辑本身无需重跑公网采集。

<a id="f5"></a>

### F5 ✅：补齐发布准备，继续使用现有安装方式

**改动位置：** `deployment/install.py`、`advance.py`、`deployment/RUNBOOK.md`；保留现有 unit 结构和配额。

1. 把 RUNBOOK 的 staging 要求补成可复制的命令/清单：Release、ASan、TSan 二进制；本候选 native-candidate-tests/source-ledger；sanitizer exit 文件（包括 `advance.py` 等待的 `*-pinned.exit`）；源码、脚本、配置、vendored baseline、Python 3.12 环境。已通过的 normalize/archive-fault/history 报告可复用，必须保留原身份，不能改名冒充新测试。
2. 安装前停止 archive timer，确认 recorder 已停止、archive worker 已结束，再备份旧 `current`、`/etc/sbmdr` 配置和日志。准备新 release/runtime、注册新根、检查 Python imports 和 unit 后才原子切换 `current`。若安装失败，报告停在哪一步并用备份恢复配置/链接；不新建部署事务框架。
3. 资格安装显式确保 `Restart=no`；处理本项目遗留的 `production.conf`，不要仅覆盖主 unit 就认为已生效。用 `systemctl show` 检查最终 Restart/CPUWeight/配额/Timeout。资格日志从新文件开始并完整保存在 evidence，不能被已有 logrotate 的 copytruncate 截掉早期证据；资格结束再恢复生产轮转。
4. 检查发布物不依赖 staging 的 Python 包路径。当前 `copytree(venv)` 不能作为可迁移保证；最小固定做法是在最终 runtime 位置用 Python 3.12 创建 venv，按随发布物复制的 `requirements/linux-x86_64-python312.lock` 使用 `--require-hashes` 安装，imports 通过 `release/python/src`。沿用现有 requirements，不升级依赖、不打 wheel。Python 官方也明确 venv 应在目标位置重建，见 [venv 文档](https://docs.python.org/3.12/library/venv.html)。
5. `advance.py` 的 CPU 默认仍为 120；本轮命令必须显式传 **200**，并把 384 MiB / 0.10% / 本候选身份写入 evidence。不让旧默认值或前一轮报告替代当前合同。

**完成条件：** 在原生 Linux 安装一次，`systemd-analyze verify` 通过，bmdr 身份下 imports/注册/恢复可用，实际 `Restart=no`；隐藏 staging 路径后发布物仍能运行。手册包含准备、安装、启停、验证、归档、回退和磁盘停止处理的实际入口。

<a id="f6"></a>

## 6. 回归、冻结与 2h 预检

### F6 ✅：合并后只跑一次必要的原生矩阵

在 GreenCloud 原生 Linux x86_64 执行，开发机只编辑与静态检查。Release 全部 CTest，加新增异步/缺口用例；ASan/UBSan、TSan 对能运行的并发/桥接/交接路径各一次。`LD_PRELOAD` 系统调用故障注入留在普通 Release，不把 sanitizer 排除的用例写成已覆盖。测试数量随新增项变化，不再要求机械保持 13/9/9。

| 本轮必须补的覆盖 | 最小断言 |
| --- | --- |
| 配置模式慢 sealer + 超过 64 个待处理任务 | 真异步；交接等待可测；解除延时后追平；资源有界 |
| 小入口队列下的配置模式丢弃 | sealer 仍启用；F3 的普通、重连、尾部、REST 缺口都可对账 |
| 异步 sealer 错误 / 满队列时停止 | writer 唤醒、错误非零、正常停止排空；无死锁 |
| 配置模式 syncfs EIO | 错误可见，不能继续报告正常耐久 |
| 配置模式取消 header 同步后的恢复 | 截断 header/完整帧尾部、入封存队列后、发布 rename/Catalog 边界的代表性中断；恢复可读、状态一致、缺口如实标记 |
| depth 明确丢帧后桥接 | 旧桥接失效；新 epoch 快照到达后恢复；不重复申请同一次 reset |
| F4 验收边界 | 能拒绝不完整证据；verify-only 不采集；blocked 归档不算完成 |

复用已有 fault shim 和 reader/recovery，用例覆盖修改过的边界，不复制一套全新测试框架。Python 工具改动只跑对应测试；仅文档改动只做 `git diff --check` 和链接检查。失败后只重跑受影响部分；最终记录所用完整矩阵的版本身份。

冻结内容：提交号、C++/fault shim/测试与部署脚本摘要、Release 二进制 SHA-256、十币对配置 SHA-256、Python baseline 树哈希、实际 systemd 属性和测试结果。后续 F7、C4-C、C5 均使用这一候选；采集行为发生变化必须重新取得对应资格证据。

<a id="f7"></a>

### F7 ✅：一次 2h 公网预检，替代重复 60 分钟试跑

1. 已完成 F1–F6；按既有归档流程排空旧根，系统盘可用 ≥18.5 GiB。保留两轮失败证据，不删除未验证的 `.partial`/源分片来腾空间。
2. 用候选发布物安装到独立预检根，两个 systemd 实例 + archive timer 同跑 **7,200 s**，显式 `Restart=no`。这是针对上轮约 79 分钟才暴露的故障，一次 60 分钟无法覆盖；不再叠加 C4-B 或另一个 soak。
3. 使用现有 `qualify.py` 参数形态：`--services --seconds 7200 --cpu-limit-percent 200 --rss-limit-mib 384 --max-drop-percent 0.10`，以及当前 release/config/root/evidence 路径。F4 新入口实施前不要把它们当成已有命令。
4. 按第 7 节相同资源、数据、缺口与归档口径验收；本轮不要求达到 23h 定时换连。额外看至少多个 900 s 轮转周期的封存队深/等待与 RSS 曲线，不能只看平均 CPU。持续满队列且不能追平判失败，即便暂时未到累计中止线；偶发丢弃必须对账并满足 0.10%，目标仍为 0。
5. 停止并排空，读回、缺口对账、归档循环通过后保存预检证据。准备 24h 的新根时可以用不同安装版本标签，但只复制同一冻结 artifact，二进制/代码/脚本/配置哈希必须一致；不用重新编译或重复 sanitizer。

**通过：** 直接进入 C4-C。**失败：** 回到 F1/F2 的具体等待点，或 F3/F4 的具体正确性问题，不扩大队列/放宽门槛后盲跑 24h。

**执行结果（2026-11-11）：PASS。** 候选 `20261111-e0b7f684` / Release `ba71faa3…` / 配置 `75d1e9ad…`，新预检根 `20261111T051425Z`，两个 systemd 实例 + archive timer 同跑 7,200 s。Spot `records=4,093,454`、`ws_dropped_total=0`、`rest_dropped_total=0`、峰值入口队列 1.46 MB、封存队深/峰值 0/20、`enqueue_seal` 最大 6.4 µs、峰值 RSS 36.6 MB；USD-M `records=13,993,992`、丢弃 0、峰值入口队列 8.14 MB、封存队深/峰值 0/31、等待 39.8 µs、峰值 RSS 49.0 MB。两 recorder 峰值 + archive 峰值上界 **173.8 MiB（182.3 MB）**，CPU 均值合计 + archive **67.4%**，slice 峰值 180.1%/60 s 118.4%，最大耐久 5.85/6.26 s，`receive_to_append_p99` 78/220 ms。900 s 窗口内无持续满入口队列、无封存交接阻塞；RSS 在最后约 45 分钟平台化，没有继续创新高；CPU 随行情量变化，单千条 CPU 未上升。归档 394 轮 0 失败，最终尾部 31/61 个 sealed 分片归档后本地积压归零，压缩速率 10.32 GB/天、`/srv` 预计约 196.7 天；系统盘最小可用 18.53 GiB，排空后约 18.96 GiB。初次 `progress.json` 因验收脚本调用 `operations.py` 时缺少 `PYTHONPATH` 而 FAILED，未重采；`--verify-only` 对同一份数据重验 PASS，最终逐帧/可信覆盖为 Spot 71/263、USD-M 181/838，全部 chunk `LOCAL_DELETED`。归档停止时 SIGTERM 命中一个 in-flight oneshot pass，服务曾进入 failed；已 `reset-failed`，并修正 `stop_archive` 先等当前 pass 结束再停，供 C4-C 使用。残留元数据 14.8 MB/1,353 manifest，按当前 chunk 速率估算 5–13 GB/月，C4-C 24h 必须实测并在 C5 前确定旧根/manifest/日志保留策略。证据见 [F7 证据目录](f7-preflight-20261111/)。

<a id="c4c"></a>

## 7. C4-C ✅：一次 24h 资格与数据闭环

在新数据根采集 **86,410 s**，两个实例各自同一 PID、`NRestarts=0`，归档同时运行。执行入口继续使用 `advance.py`，显式 `--cpu-limit-percent 200`；启动前确认 F7 PASS、release/config 身份一致。完成采集后正常停止、归档尾部排空，再对这份闭合数据验收。

| 验收项 | PASS 条件 |
| --- | --- |
| 运行 | 两实例各自完整连续时长，未异常退出/重启，正常排空完成；不能拼接两轮 |
| 文件/目录一致性 | Catalog、manifest、归档事务覆盖本轮全部分片；源/目标 SHA 与大小验证链成立；第 F4 节指定分片 CRC/解码通过；记录数对账，报告明示 trusted/decoded 覆盖；无遗留 `.partial` 或非终态分片 |
| 丢弃 | 每市场总丢弃 / 已接受 WS ×100 ≤0.10%；WS/REST 分项及 Catalog 缺口总数完全一致；不得用总记录数稀释分母 |
| 网络/depth | 各连接组独立检查，重连有缺口；各 symbol depth 重新桥接；23h 逻辑按 F4 的实际连接寿命判定。forceOrder 无消息不算故障，不加逐流断线秒数门槛 |
| 资源 | 整机平均忙碌 ≤50%，整机最小可用内存 ≥50%；沿用 qualifier 的 CPU 合计/滚动检查 ≤200% 单核；两 recorder 峰值 RSS + archive 峰值上界 ≤384 MiB |
| 磁盘/归档 | 系统盘最小可用 ≥13 GiB；两个市场都有目标验证后回收源的事务；闭合后本地 SEALED 和未完成归档事务积压归零；目标 blocked 不能算成功 |
| 离线消费 | 每市场一个 symbol 的一小时数据：现有 normalize `invalid=0`，aggTrade→1m candles 成功；保留原始缺口，不伪造完整覆盖 |

报告另外记录：队列高水位、封存交接/Catalog/sync 耗时、CPU/RSS、最大 receive→fsync、append p99、压缩 GB/天及归档盘预计保留天数。历史 1.25 s 耐久目标仅报告达到/未达到，不新增为硬门槛。直方图末桶若是溢出桶，要报告 `>=` 边界，不能当作精确 p99。

采集完整且仅验收脚本出错：修复后用 F4 的 verify-only 重验同一份数据，不重采。记录器错误/数据错误导致失败：保留原因与受影响样本，修复后重新资格；失败轮不全量读回。满足全部条件后才能进入 C5，不能凭 native tests 或某个 `status=PASS` 字段直接签收。

**执行结果（2026-11-12）：PASS。** 同一冻结候选 `20261111-c4c-e0b7f684`，新根 `20261111T074613Z`，连续 86,409 s；Spot 51,679,919 / USD-M 183,369,877 条，WS/REST 丢弃 0/0，Catalog 缺口 0；入口队列峰值 6.0/96.0 MB，最长 ≥90% 0 s；封存队深/峰值 0/18、0/32，最大交接等待 37 µs/23.0 ms；峰值 RSS 42.5/141.9 MB，RSS+archive 270.7 MiB（283.8 MB）；CPU 均值合计+archive 79.7%，60 s 峰值 168.7%；整机忙碌 29.7%，最小可用内存 3.92/6.16 GB；系统盘最小 18.27 GiB。三连接组均在 elapsed 82,800 s 触发 `23h_renewal`，换线后各 symbol depth 重新桥接。归档 4,399 轮 0 失败，16,716 个 chunk 全部 `LOCAL_DELETED`，压缩 10.89 GB/天、`/srv` 约 185 天。每市场 BTCUSDT 一小时离线消费：Spot 606,822 / USD-M 2,093,576 条 valid、0 invalid、各 60 根 1m K 线、0 gap。首次 `progress.json` 因 qualifier 额外检查 1 秒 `slice_cpu_peak=246.56%` 而 FAILED；计划门槛是合计/滚动，实际合计+归档 79.70%、60 s 168.68% 通过；修正验收脚本后 `--verify-only` 对同一份数据 PASS，未重采。长期注意：USD-M RSS 在 7–9 小时阶跃到约 135.3 MiB（141.9 MB）后平台至 24 小时，24h 预算通过但不是线性泄漏证明；Catalog/manifest 24h 残留约 160 MiB（167.8 MB）、约 4.8 GB/月，C5 前必须确定旧根/manifest/日志保留或清理策略，并继续监控周/月内存；本 PASS 不替代周/月长稳资格。存储估算：`/srv` 归档盘约 **185 天**；系统盘元数据约 **34–36 天**到 13 GiB 运行线（旧根清理可延长）。证据见 [C4-C 证据目录](c4c-qualification-20261112/)。

<a id="c5"></a>

## 8. C5：启用、重启、回退与交接

1. 确认 C4-C 最终 PASS 与所有哈希；运行现有 `finish_production` 排空尾部，启用同发布物的两个 recorder 与 archive timer。设置 `Restart=on-failure`，沿用 `RestartSec=10` 和 `StartLimitBurst=3/300s`，启用生产日志轮转。
2. **一次受控重启：** `systemctl restart sbmdr-recorder@spot sbmdr-recorder@usdm`。记录正常停机/恢复耗时、两个新 `PROCESS_START`；确认实际数据继续增长、新分片正常封存、归档出现新完成事务。仅 `is-active`/`is-enabled` 不足以签收；不在生产根做 SIGKILL 注入。
3. **一次回退演练：** 维护窗口内停止服务和 timer，保留生产根/配置；用隔离临时根演练 RUNBOOK 的旧 release 启动及读写，再恢复当前合格 release、生产 roots/config 并验证继续写入/归档。旧版本只证明回退操作可执行，已知容量失败版本不得留作最终常驻。切换前同时保存 release 链接和配置，不能假定只回退二进制适用于所有未来配置。
4. 在 `c3-c5-deployment.md` 写最终签收：release/二进制/config/baseline 身份、24h 起止时间、上表逐项结果、资源与保留天数、受控重启/回退结果、最终 systemd 状态。更新 RUNBOOK 的具体发布物、数据根和回退目标。

**完成定义：** 已签收的同一候选在生产根常驻，两个 recorder 持续写入、archive 持续完成事务，启停/恢复/回退和磁盘停止处理均有可用手册。没有额外 72h 验收阶段。

<a id="failure-routing"></a>

## 9. 失败后回到哪里

| 情况 | 下一步 |
| --- | --- |
| F1 不能证明封存交接是主等待 | 保留 64，按已有 sync/Catalog 计时定位；不先改 256 |
| 局部修复后同类持续过载再次出现 | 暂停长测，提交一页输入速率/处理速率、等待点、RSS/磁盘、旧新对照；仅在证据要求新架构或改预算时重新讨论范围 |
| 缺口漏记/错误桥接/损坏 | 回 F3 或对应存储边界，补复现并修复，不以 0.10% 豁免正确性 |
| 验收或发布脚本失败 | 回 F4/F5；复用已完整闭合的数据，避免重复采集 |
| 原生测试或 2h 通过但 24h 失败 | 仍未取得 C4-C 资格；按真实原因修复，不跳过失败直接 C5 |
| 需要减币对、加线程、改配额或放宽门槛 | 不属于本最小计划，单独提出具体变更与证据；普通实现细节和已列修复无需逐项请示 |

## 10. 参考与历史结果

- 基线与范围：[c0.1-baseline.md](c0.1-baseline.md)、[c0-prototype-and-budget.md](c0-prototype-and-budget.md)、[官方源清单](docs/binance_sources.md)。
- 已完成阶段：[c1-recorder.md](c1-recorder.md)、[c1-closeout-20261107.json](c1-closeout-20261107.json)、[c2-reliability.md](c2-reliability.md)、[c2-core-qualification-20261108.json](c2-core-qualification-20261108.json)、[C3 结果](c3-c5-qualification-20261109.json)。
- 当前部署与历史试验：[c3-c5-deployment.md](c3-c5-deployment.md)、[RUNBOOK](deployment/RUNBOOK.md)、[旧计划全文](cpp_rewrite_plan_history.md)。
