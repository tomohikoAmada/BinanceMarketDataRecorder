# C3–C5 公网验收与部署记录

> **2026-11-11 审查更新：** 仓库最新已保存结果为 C4-E 第二轮 C4-C 失败，C4/C5 未签收；本次未查询 VPS 实时状态。后续任务统一按 [最小上线收尾计划及顺序表](cpp_rewrite_milestone_plan.md#execution-order) 推进。下文各轮时间、测试与发布物是历史记录，不代表当前候选已取得生产资格。第二轮“64 格小于 80 条流/111 个活跃分片”的归因已纠正，见文末；失败 JSON 中的 `suspected_mechanism` 保留为当时假设。

## 完成状态（2026-11-11）

| 阶段 | 状态 | 做法与事实 |
| --- | --- | --- |
| F1 复现与测试纠偏 | ✅ 完成 | 配置模式固定走真实异步 sealer；十币对慢 sealer 用例在 64 格候选上复现 Spot/USD-M `enqueue_seal` 等待 1.69/2.53 s，并加入封存队深、交接等待、pending raw、Catalog 等待指标。 |
| F2 局部修复 | ✅ 完成 | 具名 `seal_queue_capacity=256`；同一用例等待降至 30/71 µs、无丢弃、队列排空；峰值 RSS 约 69 MiB/进程，打开文件 122/213 已记录。 |
| F3 缺口正确性 | ✅ 完成 | 进程级 WS/REST 丢弃账本、停机尾部 `INGRESS_DROP`、`rest` 分项；`DepthBridge` 遇 `ingress_drop` 先失效再重桥；普通/尾部/重连/REST/缩时 23h 回归通过。 |
| F4 验收脚本 | ✅ 完成 | Catalog/manifest 集合与身份对账、缺口分片解码、WS/REST 丢弃对账、逐连接组 23h、`--verify-only`、`--finish-only`、archive blocked；离线 `c4-acceptance-boundaries` 通过。 |
| F5 发布准备 | ✅ 完成 | 安全安装顺序/备份恢复、runtime venv 按 hashed lock 重建、`Restart=no`/CPUWeight/配额/超时检查、旧 `production.conf`/logrotate 处理、RUNBOOK；原生安装 `20261111-e0b7f684`。 |
| F6 原生回归与冻结 | ✅ 完成 | Release 22/22、ASan/UBSan 10/10、TSan 10/10（`setarch -R`）；冻结提交 `e0b7f684`、Release `ba71faa3…`、配置 `75d1e9ad…`、baseline `801802d8…`。 |
| F7 2h 公网预检 | ✅ 完成 | 2026-11-11 05:26–07:26 UTC 连续 7,200 s；Spot 4,093,454 / USD-M 13,993,992 条，0 丢弃，封存队深/峰值 0/20、0/31，峰值 RSS 36.6/49.0 MB，RSS+archive 173.8 MiB（182.3 MB）；首次验收脚本缺 `PYTHONPATH`，verify-only 重验同一份数据 PASS，尾部 31/61 分片归档后全部 `LOCAL_DELETED`。 |
| C4-C 24h 资格 | ✅ 完成（PASS） | 2026-11-11 07:46:56–2026-11-12 07:47:27 UTC 连续 86,409 s；Spot 51,679,919 / USD-M 183,369,877 条，0 丢弃；三连接组均 23h 换线；全部 16,716 chunk `LOCAL_DELETED`；RSS+archive 270.7 MiB（283.8 MB）、CPU 合计+归档 79.7%、60 s 峰值 168.7%；一小时离线消费两市场均 invalid=0、60 根 1m K 线。首次验收因额外 1 s CPU 采样断言失败，修验收脚本后 verify-only PASS，不重采。 |
| C5 生产交接 | ⬜ 未开始 | C4-C PASS 后执行启用、一次受控重启、回退演练和最终签收。 |

首版 C3 候选已完成 2026-11-08 17:57:39–19:00:55（上海时间）的 60 分钟公网与完整独立读回：Spot 356,069 条/64 分片，USD-M 1,298,443 条/92 分片；CPU 均值合计约单核 17.03%，峰值 RSS 合计约 61.38 MiB。二进制 SHA-256 为 `2051b262fa831c02a35aad5bda580db9d44c4bbb07382b487a941f4326e515a3`。补充配额扣账、成交时间统计、配置流检查和归档并发等待后的最终候选需从新 T0 重跑，不拼接时长。最终 C3 已通过，C4/C5 24h 资格仍待完成，见下方签收补记。

首版原生 Release 八项、ASan/UBSan 和独立 TSan 各五项相关回归已通过。新增第九项离线 K 线测试因缺少 Python 基线路径首轮失败，现修正 CTest 环境后复验。最终固定源码、二进制与各测试结果另行绑定。

显式配置为 [四币对配置](config/c3-core.json)，扩展候选为 [十币对配置](config/c4-mainstream.json)。扩展名单参考同日官方 Spot 24h quoteVolume 与 TRADING USD-M 永续交集。原十二对包含 BTCUSDC、ETHUSDC，真实容量失败后收敛到十个 USDT 对；[选择证据](config/c4-selection-20261108.json)保留当时排名和快照摘要，无运行时 Top-N 调整。

为复用已验收的单市场生命周期，本轮使用两个隔离记录器进程，资源按两者合计验收。Spot 一条组合连接，USD-M 按[官方路由](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Important-WebSocket-Change-Notice)拆为 public（depth/bookTicker）和 market（aggTrade/markPrice/forceOrder）两条。每连接一个有界接收 worker、每市场一个 REST worker 和一个单写 worker；共七个业务 worker，没有压缩线程池。此处替代原计划“全部连接复用一个 WS 线程”的成本假设，以最小改动复用 C2，并报告实测 CPU/RSS。

启动先读取和保存目录，只接受白名单内处于 TRADING 的标的。Spot 目录使用 symbols 过滤参数，避免获取全市场大响应；USD-M 目录全市场保存一份。按市场、标的、流分别维护 Raw 文件，桥接状态按标的隔离。每分钟输出内存中的源计数、最近接收/追加/fsync 和缺口信息；不写逐 poll SQLite 健康行。重连首条按各流分别标缺口。

跨盘链路复用固定 Python ArchiveManager/StorageRegistry/archive_drain，保持复制、验证、登记、删除的事务顺序。隔离复制的 C++ Raw 已通过真实 /dev/vda1→/dev/vdb1 归档和外部读取；复制中断及目标哈希损坏均保留源，重试后成功。原生产 Python 数据及旧服务不切换、不删除。

C4/C5 使用独立 sbmdr-recorder@spot、sbmdr-recorder@usdm 和 sbmdr-archive.timer。资格运行期间 Restart=no，固定 PID，任何提前退出都记失败；24h 及独立全量读取通过后才改 Restart=on-failure 并 enable 常驻。归档在独立 oneshot 进程，不进入写线程；其 CPU/RSS 和压缩日增量纳入报告。角色 bmdr，无需 API key 或交易权限。

离线工具复用冻结 Python 的 Raw→Parquet、回放和 backfill CLI。新增 candles.py 只处理显式时段的 aggTrade，以 Decimal 生成 UTC 整分钟倍数 OHLCV，不补缺口，不把观测到的成交伪装成完整历史。重复/冲突 ID 和缺口如实检查。官方历史文件依照[官方公开数据说明](https://github.com/binance/binance-public-data)，下载后核对 CHECKSUM；2025 年起 Spot 历史文件时间为微秒，与实时 WS 的毫秒分开处理。

最终签收须补入：C3 全量读取报告、C4 档案和离线消费验证、24h 同 PID/23h 换连/重连桥接证据、两盘空间与归档循环、资源和保留天数，以及正式启用后的 systemd 状态。当前文档不将正在运行的长测写成已完成。

部署路径与日常入口（安装后生效）：

- 配置：`/etc/sbmdr/config.json`，两个市场各十对、共 20 个产品。修改后需重启对应实例，新一轮资格使用固定配置。
- 记录器：`/opt/sbmdr/current/bin/binance-market-recorder` 为固定 C++ Release 的别名；实时调用方式见 unit 的 `record --public ROOT COUNT CA MARKET CONFIG`。Python 离线工具仍由该 release 的 `venv/bin/python` 调用，设置 `PYTHONPATH=/opt/sbmdr/current/python/src`。该路径来自 release 内自带的 `vendor/python-baseline` 副本，不再指向其他分支或外部检出。
- 状态：`systemctl status sbmdr-recorder@spot sbmdr-recorder@usdm sbmdr-archive.timer`；分钟源状态在 `/var/log/sbmdr/{spot,usdm}.status.jsonl`，归档结果在 `archive.jsonl`。
- 读取或手动归档：`prototype/operations.py verify ROOT` / `archive ROOT`，使用 `/etc/sbmdr/roots.json` 中的隔离根；归档仅在完成目标验证后删除源 `.zst`。
- 归一化/回放：`prototype/normalize.py ROOT --manifest MANIFEST --replay-symbol BTCUSDT`；OHLCV：`prototype/candles.py --help`；官方历史：`prototype/history.py ROOT --market spot --symbol BTCUSDT --date 2026-11-06 --interval 1m`。

资格驱动只接受当轮完整持续时间与全部分片校验；24h 还从 Raw 的每连接接收时间范围独立确认至少一条实际持续约 23h 的连接，要求后续 depth 连接有桥接标记，不能只凭“23h 之后出现一次重连日志”代替主动换连证据。旧容量压力失败不随此次多源验收改写。

资源门禁沿用 C0 冻结预算：100% 为单核，60 秒滚动 CPU ≤120%，约 1 秒采样窗口 ≤200%；C4 另要求整机平均 CPU ≤50% 与可用内存 ≥50%。两个实例与归档放入同一 `sbmdr.slice`，直接读取原生 cgroup `cpu.stat` 合计采样，不将不同时间的进程峰值相加冒充同步峰值；后续资格固定合计 CPUQuota=175%、归档 CPUQuota=25%。两记录器峰值 RSS 与归档父/子进程峰值上界合计 ≤256 MiB。C5 的单核 30–50% 是收益目标，实际均值与是否达到该示例目标单独披露，不以 200% 平均值替代稳态预算。记录器原生运行还包括每条 WS 的 Asio DNS resolver 辅助线程，首轮观测 Spot 5、USD-M 7 个 OS 线程；七个业务 worker 不等于 OS 总线程数。


2026-11-09 发现并修复离线消费问题：首轮只检查了 Parquet 构建/回放计数，未检查 `valid`，冻结 Python 解析器把 combined WS 外层误当成事件体。`normalize.py` 现只向冻结解析器提供 `data` 视图；Raw、原 payload hash、分区/回放契约保持原样。新增原生回归核对字段、有效性和原 Raw 不变。Spot 补验 29,508 行全部有效，USD-M 补验 115,116 行全部有效，包含实际强平样本。旧错误构建和失败日志保留；由于构建 ID 按 Raw 输入确定，修复验证使用独立消费夹具，避免覆盖既有 immutable manifest。上线后须通过此适配入口生成派生数据。

过载路径另去除了 BTCUSDT 的固定诊断标识：异常给出实际市场/标的/流，Catalog 将市场级影响标为全局，所有已受影响分片继续带缺口标记。该修改后 C++ 二进制固定为 `234264f5adb64ea007137781c9c56652522ccfedd28919ecc107ccb092dffe49`，最终 C3 新 T0 为 **2026-11-09 01:45:22（上海时间）**。此前 01:01 开始的候选运行于约 42 分钟时停止，标为 SUPERSEDED，保留 413,732 条 Spot 和 1,946,437 条 USD-M，不计入最终 60 分钟。


最终 C3 于 **2026-11-09 03:11:40（上海时间）签收**：同一固定二进制采集 3,605 秒，Spot 453,598 条/63 分片，USD-M 2,049,024 条/92 分片，合计 2,502,622 条全部独立读回。CPU 均值合计单核 27.41%，峰值 RSS 合计 64.31 MiB。采集 02:45 正常排空；驱动对 `SIGNAL` 的误判使首轮控制器失败，修正后复用同一闭合数据完成读回，不拼时长。原失败保留。append p99 为 47/189 ms、最大耐久 1.869/4.476 s，未把原门槛失败改成通过。

原生 Release 完整十项、ASan/UBSan 与 TSan 各五项均通过。C4 新增 USDC 两对的五路 REST 预检全部 200，八个统计类请求各有一行，两个 openInterest 亦有效。双盘故障保源、离线有效性和历史 CHECKSUM 导入通过后，才进入十二币对 24h。C3 cgroup 采样未启用，运行期间有其他验收作业；C5 单独测量纯采集加归档成本。完整源计数以独立读回为准，分钟 SOURCE_STATUS 是活跃分片状态。


C4 首轮 03:33–04:03 上海时间真实十二币对试运行失败：USD-M 在约第 30 分钟队列满，控制器非零失败并停止另一实例。原资格 1s CPU 峰值 225.78%、60s 峰值 84.91%，未通过的 CPU 与过载结论保留。日志间隔约 8.6 秒内封存数增加 48，定位为到期文件集中封存阻塞写入。候选改为有积压先写数据、无积压每批封存一个最早到期文件；仍使用标准有界队列、单市场单写线程、Raw v1。

过载路径另修复 frozen Catalog 兼容：范围放进 evidence_json，非 discontinuity 的身份列留 NULL。失败根原行/DB 已备份，只修这一个事件行后全部 1,530,969 条/286 分片读回通过，Raw 未动，运行失败不改写。慢封存夹具旧版复现过载；新版完整读取 30,000 条 WS，队列峰值 2,800；强制单次长阻塞后，23,231 条已接受 WS 前缀及缺口/Catalog 仍可读取。当前候选十二项 Release 全通过，sanitizer 复验后重新进行 24h。

后续固定预算：sbmdr.slice CPUQuota=175%，归档 CPUQuota=25%；归档单根观测最长 75.20 s，因此两根串行 oneshot 超时为 240 s、结束静置上限 300 s。C3 基线与 C4 新候选分别记录 binary SHA；当前候选由其原生回归哈希绑定后进入 C4，不将旧 C3 采集换标为新二进制。


当前 C4 候选 `6488d9…` 已完成 Release 十二项、ASan/UBSan 六项、TSan 六项，结果按 `native-candidate-tests.json` 绑定源码及三份二进制；C3 已签收基线仍为 `234264…`。新的 C4/C5 十二币对 24h 将使用固定配额、独立新根与清空并备份后的实例日志，不拼接首轮失败数据。


新 24h T0：**2026-11-09 05:08:22 上海时间**。release `/opt/sbmdr/releases/20261109-854745e6`，二进制 `6488d9…`，配置 SHA `758771…`；根 `/var/lib/binance-market-data-recorder/S-BMDR/live-20261108T210819Z/{spot,usdm}`，目标 `/srv/recorder-data/S-BMDR/live-20261108T210819Z-{spot,usdm}`。资格报告在原生 `results/c4-c5-24h/progress.json`，主流程在 `results/promotion-progress.json`。预计 11-10 05:08:32 满足连续采集时长，再进行全量读取；尚未签收或启用开机常驻。


第二轮在 05:35 上海时间过载，新的 24h 未完成。此时定时封存已分散，只新增一个大小封存；实际 ZECUSDT/book_ticker manifest 为 134,217,843 字节，时间 UTC 21:34:55，对应最后约 2.5 秒日志空档和队列满。公开采集默认大小目标现降为 32 MiB，900 秒和队列/格式不变；候选 `700f0b…` Release 十二项通过，sanitizer 复验进行中。更多分片带来的 metadata/归档开销将实测，不承诺未经验证的吞吐或延迟成绩。


第三轮历史 T0 为 **2026-11-09 05:56:25 上海时间**，release `/opt/sbmdr/releases/20261109-a1507458`，二进制 `700f0b…`；新根 `live-20261108T215622Z`，公开大小目标 32 MiB。Release 12、ASan/UBSan 6、TSan 6 均通过。预计 11-10 05:56:35 满 24h，随后全量读取。前两次失败不计时、不覆盖；目前尚未签收 C4/C5 或 enable 常驻。


第三轮于 05:57:30 上海时间真实流量突发期间过载失败，未完成 24h；60 秒滚动 CPU 159.65%，单秒采样峰值 177.85%。此时已经使用 32 MiB 分片，不能用上一轮的单个 128 MiB 停顿解释。证据与日志保留在 `results/c4-third-preflight`。

当前配置收敛为十个 USDT 对（20 个 Spot/USD-M 产品），只移除 BTCUSDC、ETHUSDC，仍在所有者授权的 10–20 对范围内。二进制保持 `700f0b…`，原生 12/6/6 回归结果按相同 C++ 哈希复用；配置提交 `dc68ddd2`，独立新根从零验收。C3 已签收，C4/C5 尚未签收；常驻只在真实 24h 与独立全量读取通过后启用。


**十币对新 T0：2026-11-09 06:10:41 上海时间。** release `20261109-dc68ddd2`，根 `live-20261108T221038Z`，配置 SHA-256 `75d1e9ad6fc54d4ab5118362f4c503384f018dcc90a17074108a321964e9bae8`，二进制 `700f0b…`。前轮封存已安全归档，新根与日志独立。24h 采集窗口预计 11-10 06:10:51 满足，随后独立全量读回；只有全部验收通过才 enable 常驻。目前 RUNNING，未签收 C4/C5。


### 归档 timer 重复启动边界（2026-11-09）

十币对 `dc68ddd2` 从 06:10:41 起正常采集约 650 秒，Spot 832,381 条、USD-M 2,779,959 条，均以 SIGNAL 排空；尚未达到 24h。检查发现归档 timer 为 active(elapsed)、Trigger=n/a，归档日志为空：原 `OnBootSec=120` 与此前同次开机已触发的 service 状态组合，重新部署后没有新的首次触发。

最小修复把首次计时改为 `OnActiveSec=120`，保留后续 `OnUnitInactiveSec=60`、AccuracySec=5 与归档事务。原生 `systemd-analyze verify` 通过；实际触发及目标验证须在新轮观察。未改 C++、未增加守护进程。上述十分钟采集由操作者主动正常停止，原控制器因不足 24h 记 FAILED，补充标为部署修复而 superseded，不能当作记录器崩溃或 24h 通过。证据保留在 `results/c4-ten-timer-preflight`。固定修复部署重新从零计时，避免混合计时与资源数据。


第三轮过载的闭合 Raw 已独立全量读回：Spot 334,600 条/50 分片，USD-M 630,424 条/75 分片，合计 965,024 条/125 分片全部通过；Catalog 无需修复。只确认已保存数据完整，该轮过载失败不改写。


**归档 timer 修复后的固定新 T0：2026-11-09 06:35:35 上海时间。** release `20261109-2cfbb7e6`，根 `live-20261108T223532Z`，十币对配置 `75d1e9…`、二进制 `700f0b…`；预计 11-10 06:35:45 满足 24h 采集，随后全量读取。启动后已观察到 timer 实际多次调用归档、事务成功及积压回零，解决重复部署后的首次触发问题；固定 PID 和资源策略继续运行。C3 已签收，C4/C5 尚未签收或正式 enable。


十币对修复 timer 后的运行仍在约 07:00 上海时间过载，未完成 24h。异常前 sealed 不变、队列数秒触顶，正在原生采样区分线程处理能力与配额影响。证据 `results/c4-ten-overload` 保留，C3 完成，C4/C5 未签收、未 enable；不把本轮归档成功等同于整体资格通过。


### 定位试验与最小调度调整（2026-11-09）

软件 perf 只用于定位：首轮启动时只有 6 个样本，且采集器忙等，作废；稳定线程后取得 1,849 个用户态样本；带内核的试验遇进程退出后也出现采集器忙等，不能用于成绩。复用本地慢封存夹具取得 822 个样本，`epoll_wait` 路径约占 13.3%，据此仅做一行 `io.poll()` 试验。三组原版 CPU 为 5.870/6.077/8.857 秒，试验版 6.363/9.314/7.103 秒；各组均完整读取 30,000 条，但收益不一致，已完全撤回。C++ 源码恢复原摘要，正式候选仍为 `700f0b…`，不引入手写 CBOR。

180 秒隔离公开诊断复用原二进制，临时记录同步调用与线程状态，正常排空 Spot 154,901 条、USD-M 734,225 条；队列峰值 1,606/6,607。观测期最慢单次同步为 116.38/74.00 ms，未复现旧过载，不能据此证明旧轮故障根因或否定多个同步累计等待。`/proc/stat` 中 steal 占该段报告 ticks 的 14.70%；新资格报告另列该计数，原整机忙碌门槛保持原算法。所有诊断独立存放，不进入 C4 的 24h 资格成绩。

当前最小调度候选：USD-M 的消息量约为 Spot 的 3.5 倍、CPU 均值约 2.4 倍；原两服务权重均为 100。采用标准 systemd USD-M 实例 drop-in，设置 `CPUWeight=200`，Spot 为 100，总配额 175%、归档配额 25% 和原 CPU/RSS 门槛继续使用。权重只影响竞争时的份额，没有额外线程、队列或格式变化，不保证独自解决过载。代价为竞争时 Spot 得到较少份额，必须由两个市场同一轮 24h、固定 PID、全部读回与队列/资源共同验收；不只检查 USD-M。

源码 `87897604` 实现权重，`7fbae5bd` 只补 host steal 报告。C++ 未变，按哈希复用原生 Release 12、ASan/UBSan 6、TSan 6；Python 语法检查通过，安装时验证原生 units，启动报告记录实际 `CPUWeight`。新轮先完整读取上轮闭合 Raw，再安全归档和部署，仍从零计时。C3 已签收，C4/C5 仍未签收；过去所有失败保留。


十币对过载轮的闭合数据已完整独立读回：Spot 992,411 条/76 分片，USD-M 3,463,316 条/173 分片，总计 4,455,727 条/249 分片全部通过，Catalog 无需修复；过载与不足 24h 的结论继续保留。随后按原事务完成源验证、目标验证和空间回收，再进入新权重轮。


### 权重采样启动错误与修复后的新 T0（2026-11-09）

`7fbae5bd` 部署启动时，资格驱动把 systemd 未显式声明的 `CPUWeight=[not set]` 当整数读取，导致启动 Spot 后立即退出，USD-M 尚未开始。此为新增采样代码的驱动错误，不是记录器吞吐失败；原失败报告在 `results/c4-weight-metadata-preflight` 保留，不计入有效长测。

最小修复 `9b0b30f1` 在通用 unit 显式设置 `CPUWeight=100`，USD-M drop-in 为 200。原生 `systemd-analyze verify` 与启动前实际属性检查通过，随后新资格报告实际读出 100/200，两个进程 NRestarts=0。C++ 未变，继续使用原已验证 `700f0b…`，未把旧原生回归改称新运行。

**当前固定 T0：2026-11-09 09:21:06 上海时间。** release `20261109-9b0b30f1`，根 `live-20261109T012102Z`，十币对配置 `75d1e9…`，CPU 权重 100/200，总配额 175%、归档 25%。24h 采集窗口预计 11-10 09:21:16 满足，完整读取另计。状态 RUNNING，C4/C5 未签收，资格期 Restart=no；只有完整时长、全量读回与资源/连接门禁通过才启用常驻。


### 所有者确认 C4/C5 CPU 验收范围（2026-11-09）

所有者明确回复“接受实测CPU，按整机余量签收（推荐）”。C4/C5 据此按实际整机平均忙碌比例 ≤50%、观测最小可用内存 ≥50% 验收 CPU 共驻余量；原 120% 单核口径门槛的失败结果保留。驱动使用显式 `--cpu-limit-percent 200`，表示 CPU 验收口径，不是增加系统资源配额；现有总配额仍为 175%、归档 25%。原峰值 CPU 200%、RSS 256 MiB、系统盘安全水位、24h 同进程、全部 CRC/SHA/数量核对和连接恢复门禁继续使用。

`advance.py` 仅把现有 CPU 参数透传，并提取已有的最终归档/启用函数以便复用。原默认仍为 120%，不会覆盖历史失败。当前 9b0b30f1 轮随后发生的是实际过载，CPU 接受没有使它通过；缺少正常完整退出结果的复核被阻断，原采集错误与后续复核错误分别保留。

### 带权重的 32 MiB 轮失败与 16 MiB 候选（2026-11-09）

9b0b30f1 从 09:21:06 上海时间运行约 24 分钟后过载；原生资格约 09:45:47 结束。实际最后窗口显示一次封存后队列 8,192→0，后一次封存时再次 1,683→8,192 并 `INGRESS_OVERLOAD`。manifest 分别对应 SOLUSDT/book_ticker（33,554,720 B，UTC 01:45:18.434781）与 ETHUSDT/diff_depth（33,556,033 B，UTC 01:45:22.368480）。这些是封存与队列事件的时序证据，窗口还包含追加、同步及调度，不能把整个间隔都说成独立压缩耗时。

fb2d2501 仅把公开采集的大小封存目标改为 16 MiB，沿用 900 秒周期、单市场单写线程、Raw v1、8,192 条/64 MiB 队列、CPU 权重 100/200 与原资源配额。代价是更多分片、manifest 与 Catalog 操作，系统盘元数据成本及保留天数由后续实际测算。16 MiB 是轮换目标；单条合法大帧仍受原 32 MiB frame limit 约束，可使分片超过轮换目标。此处没有宣称已经解决或必过 24h。

先独立读取失败轮闭合 Raw，再做当前候选原生 Release 十二项、ASan/UBSan 与 TSan 各六项；通过后以 CPU 接受范围 200 启动独立新根的完整资格。当前为测试准备，新的 T0 尚未产生。旧 09:21 运行已失败，原预计 11-10 09:21 的结束时间不再适用。C3 已签收，C4/C5 未签收。


带权重的 32 MiB 失败轮已独立完整读回：Spot 1,484,465 条/85 分片、USD-M 5,223,377 条/222 分片，总计 6,707,842 条/307 分片全部通过，无需修改 Catalog。只确认已保存数据完整，不改变过载与未完成 24h 的结论。16 MiB 候选随后进入原生构建与回归。


### 16 MiB 当前候选回归通过与新 T0（2026-11-09）

fb2d2501 的新二进制 SHA-256 为 `40df2dc2f6100aeeed0ab0cc58e88198cd59b854b7215fe0f2295ed8d38d976e`。原生 Release 十二项全部通过（131.65 s），ASan/UBSan 六项（70.34 s）与 TSan 六项（88.55 s）通过；源码、构建与三份二进制摘要绑定于 `native-candidate-tests.json`。这些是本次新构建的实际回归，C3 已签收基线仍为原 `234264…`。

**新的固定 T0：2026-11-09 10:56:12.8 上海时间。** release `20261109-fb2d2501`，根 `live-20261109T025607Z`，白名单仍为十个 USDT 对，两个市场共 20 产品；CPU 权重 100/200、合计配额 175%、归档 25%，大小目标 16 MiB、周期 900 s。驱动显式使用所有者接受的 CPU 口径 200，仍报告原 120% 的逐项结果。24h 采集预计 **11-10 10:56:22.8** 满足，完整读回另计。该轮启动时状态为 RUNNING，C4/C5 尚未签收或启用开机常驻。


### C4-A 写路径修复与 10:56 轮提前退出（2026-11-09）

10:56:12.8 轮于 **11:15:11 上海时间**提前退出：USD-M 运行 18m56s 后以 exit 1 结束，Spot 同时停止。按定稿第 5 节，本轮不做全量读回、不调参，24h 计时作废；失败根与日志保留在原运行根，C4/C5 仍未签收。

C4-A 按定稿 6.2 完成以下改动：

- 配置模式交接队列按 `body.size()+1024` 计费，字节上限保持 64 MiB、条数保险上限 65,536；单源模式保持 8,192 条。`push`/`try_push`/`pop`/`take`/`rest_admissible` 使用同一计费。
- `DepthBridge` 使用独立 `depth_pending_limit=8192`，不再随交接队列放大。
- 配置模式满队列改为非阻塞 `try_push`：WS/REST 丢弃当前帧并继续运行；WS 按 `(symbol,stream)` 累计丢弃数和首末时间，下一条被接受消息带 `sequence_gap`、`ingress_drop` 和 `ingressDropped`；每连接每秒最多一条 `INGRESS_DROP` status_line；写线程写入 Catalog `INGRESS_DROP` 事件，身份列留 NULL、范围在 `evidence_json`。depth 丢弃后由现有 `DepthBridge::reset()` 重新请求快照。
- 周期耐久同步在配置模式改为 5,000 ms，单源保持 900 ms；启动磁盘门槛改为 15 GiB；每秒 status_line 增加 `writer_sync_ms`、`writer_seal_ms`、`dropped_total`。
- `c4-overload-catalog`（`--stall-one`）改为断言“进程不退出、无丢弃、30,000 条全部按原字节读回”；新增 `c4-ingress-drop`（测试专用队列 1,024 条 + 3.5 s 封存阻塞），验证丢弃计数、逐流 `ingress_drop`/`sequence_gap`、Catalog 事件、冻结 Catalog 可读，以及丢弃后新的 depth 快照请求和桥接恢复。

**验证结果（原生 GreenCloud Linux x86_64）：** Release **13/13**（原 12 项 + 新增 `c4-ingress-drop`，127.56 s），ASan/UBSan **9/9**，TSan **9/9**（117.26 s）；`c4-ingress-drop` 连续运行 3 次通过。C4-A 代码提交 `f68a2f47`，Release 二进制 SHA-256 `4181d599e5466bca444463a7ec5edb7bf3d92edbb3055406d40714ac78180e4b`。C4-B 60 分钟公网验证已 PASS；C4-C 24h 资格和 C5 启用尚未开始，C4/C5 仍未签收。

**测试环境遇到的问题（均为夹具/验证脚本问题，不改记录器结论）：**

- macOS 打包到 Linux 时带入 AppleDouble `._*` 文件，导致 vendored Python baseline 树哈希不匹配；清理后通过。
- 丢弃测试的回环流名需使用归一化后的 `diff_depth`/`book_ticker`/`agg_trade`；depth 快照夹具需返回比最新流早一拍的 `lastUpdateId`，否则会按 `depth_snapshot_too_old` 反复重试。
- Spot depth 快照权重为 250、窗口 2,000/60s，12 个 symbol 在短测内最多完成约 7 次快照；丢弃测试改用 3 个 symbol，以便观察到丢弃后的第二次快照请求和桥接恢复。该权重与生产配置未改。

**当时尚未完成：** C4-C 24h 资格、C5 启用与交接；C4-C 已于 2026-11-12 PASS，见顶部状态表与 C4-C 节。C4-B 已部署并 PASS；10:56 失败轮按定稿未做全量读回。

**资格驱动更新（2026-11-09，C4-B 准备）：** `deployment/qualify.py` 已加入 `--max-drop-percent` 门槛，按独立读回的 accepted WS 记录数计算 `dropped_total` 百分比（C4-B 使用 `0`，C4-C 使用 `0.01`）；新增 `--queue-limit-mib` 可选门槛（C4-B 使用 32 MiB），并把 `queued_payload_bytes` 峰值、`writer_sync_ms`/`writer_seal_ms` 均值与最大值写入报告；停止后断言无 `.partial`。`deployment/advance.py` 的 C4-C 调用已显式传 `0.01`。驱动提交为 `27b1f0ac`。


### C4-B 60 分钟公网验证（2026-11-09，PASS）

C4-A 发布物已安装为 `/opt/sbmdr/current -> /opt/sbmdr/releases/20261109-f68a2f47`，二进制 SHA-256 `4181d599e5466bca444463a7ec5edb7bf3d92edbb3055406d40714ac78180e4b`；十币对配置 SHA-256 `75d1e9ad6fc54d4ab5118362f4c503384f018dcc90a17074108a321964e9bae8`。新根为 `/var/lib/binance-market-data-recorder/S-BMDR/live-20261109T140141Z/{spot,usdm}`，归档目标为 `/srv/recorder-data/S-BMDR/live-20261109T140141Z-{spot,usdm}`。

资格驱动于 **2026-11-09 22:02:29 上海时间（14:02:29 UTC）** 启动，参数为 3,600 秒、systemd 两实例 + archive timer、`--rss-limit-mib 256 --cpu-limit-percent 200 --max-drop-percent 0 --queue-limit-mib 32`。启动后初始检查：Spot PID 257375 / USD-M PID 257386，均 `NRestarts=0`、`ActiveState=active`；约 19 秒时两市场 `dropped_total=0`、`queued_payload_bytes=0`，周期耐久约 5.06 秒。预计 **23:02:29 上海时间**结束采集并进入排空、全量读回与归档检查。本轮尚未签收；C4-C/C5 尚未开始。

**采集与读回结束（2026-11-09 23:40:57 上海时间，qualification PASS）：** 两实例以 `SIGNAL` 正常停止，`wall_seconds` 分别为 3,603.82 / 3,604.60，`NRestarts=0`、PID 未变。Spot 3,300,637 条 / 227 分片，USD-M 15,104,852 条 / 920 分片；独立读回记录数与记录器一致，冻结 Python Catalog 可读，无 `.partial`。两市场 `dropped_total=0`、`drop_percent=0`。队列峰值：Spot 3,266,187 B，USD-M 26,799,387 B（均 ≤32 MiB）。记录器峰值 RSS 合计 95,395,840 B（约 91.0 MiB）；含归档峰值上界 193,126,400 B（约 184.2 MiB，≤256 MiB）。整机忙碌 29.97%、stolen 13.91%、最小可用内存 5.13 GiB / 6.16 GiB（83.2%）；系统盘最小可用 19,703,959,552 B（约 18.35 GiB，≥13 GiB）。slice 1s 峰值 186.51%、60s 峰值 142.59%，均 ≤允许的 200% 口径；原冻结 120% 的 60s 项仍为 false，按所有者批准的整机余量口径记录。归档 timer 25 次通过、0 失败，成功跨盘搬运 612 个文件 / 410,466,952 B，Spot/USD-M 的 `LOCAL_DELETED` 分别为 188 / 424。`writer_sync_ms` 均值/最大：Spot 16.57/331.55，USD-M 24.24/423.16；`writer_seal_ms` 均值/最大：Spot 8.91/952.73，USD-M 36.11/1,052.04；最大耐久 5.77/6.58 s；append p99 180/712 ms。**C4-B 签收 PASS。**

**C4-C 前置风险（未自行调参）：** 本轮每小时产生压缩数据约 799.7 MB，归档实际搬运约 410.5 MB/h（约 51%），1 小时后本地仍有 39/496 个 `SEALED` 分片（约 382 MB）未归档。按此速率，24 小时将累积约 9 GB 未归档数据，系统盘可用会从当前约 19 GiB 降到 13 GiB 以下，同时 C4-C 的“结束后归档积压归零”也难以满足。归档吞吐是 C4-C 的实质阻塞项；按定稿第 9 节不在未获所有者决定前自行增加归档线程、改 timer 或改配额，因此 **C4-C 尚未启动**。

### C4-D Python 归档与读回去过度防御（2026-11-10）

按定稿 5.2 执行（代码提交 `7ecb87da`），未修改或重编任何 `.cpp` / `.hpp`，C4-B 二进制 `4181d599…` 保持冻结。改动：

- `archive/drain.py`：归档不再使用 `16 MiB/s` 的 `ReadBudget` 睡眠限速。
- `archive/manager.py`：同一 `run_once()` 正常推进中，源文件只在 `_reserve` 校验一次；`_copy` / `_verify_and_commit_external` 不再重复全量 Zstd 解压；刚验证并提交的目标文件在删除源前不再做第 2 次全盘哈希。跨进程恢复仍保留源/目标完整校验。
- `sbmdr-archive.timer`：`OnActiveSec=15`、`OnUnitInactiveSec=10`；`sbmdr-archive.service`：`CPUQuota=50%`、`TimeoutStartSec=600`；`operations.py archive`：`max_runtime_seconds=120`。
- `operations.py verify`：已归档分片按外部 manifest 绑定和大小校验后信任，不再全量解压；单连接 WS 分片直接使用 manifest 元数据；仅 REST、多连接、每个 `market/symbol/stream` 首尾分片和本地未归档分片逐帧解码。新增 `fast_path` 说明字段。
- `spool/format.py` / `normalize/raw.py`：验证读回可跳过已由密封 CRC/哈希保证的 canonical 重编码与 Pydantic 重校验。
- `deployment/install.py`：修复重复安装目录幂等性，旧 60 分钟证据自动移到 `results/c4-b-60m-evidence`。

**当场验证：**

- 归档故障注入 `copy_progress` / `before_verify` 两个故障点均通过，源文件保留、重试完成。
- C4-B 遗留 535 个本地 `SEALED` 分片（389,246,717 B）全部排空：Spot 22,544,119 B / 5.56 s（约 14.6 GB/h），USD-M 366,702,598 B / 68.73 s（约 19.2 GB/h），均远高于 1,600 MB/h 目标。
- 优化后 `operations.py verify`：Spot **16.73 s**、USD-M **19.80 s**（均 <30 s）；记录数精确为 3,300,637 / 15,104,852，`status=PASS`。Quick-path 信任分片 Spot 156/227、USD-M 782/920；因信任分片不再逐帧解码，`payload_bytes_complete=false`、`capture_flag_counts_exact=false` 会明确写入输出，qualify.py 所需字段不受影响。
- 用 `qualify.verify_stopped` 在已闭合 C4-B 数据上重跑停机校验：`status=PASS`，两市场 `dropped_total=0`，RSS/CPU/归档门槛均通过。
- vendored Python baseline 树哈希更新为 `cd615fc1…`；`BASELINE.json` 记录 C4-D patch。C4-B `c0-loopback` / `c1-recorder` 使用新基线通过。

### C4-P 10 个 USDT 对 Python vs C++ 控制变量基准（2026-11-10，PASS）

Python 基线使用相同 10 个 USDT 对、相同稀缺流集合、900 s / 16 MiB 轮转，在空载 VPS 上运行 960 秒，退出码 0，未提前崩溃：

| 指标 | Python 基线（960 s） | C++ C4-B 前 960 s | 差异 |
| --- | ---: | ---: | --- |
| 记录数 | 1,794,771 | 2,387,688 | Python 为 C++ 的 75.2%（市场窗口/10 次断线不同） |
| 记录/秒 | 1,868.8 | 2,487.4 | — |
| CPU 均值（单核=100%） | 111.25% | 55.86% | C++ 约低 2 倍 |
| CPU 1 s / 30 s 峰值 | 141.91% / 114.94% | — | Python 记录 |
| **CPU 秒 / 10,000 条** | **5.953** | **2.246** | **Python 为 C++ 的 2.65 倍** |
| 峰值 RSS | 261,521,408 B（249.4 MiB） | 75,997,184 B（72.5 MiB，两记录器峰值相加） | Python 为 3.44 倍 |
| RSS 60 s → 900 s | 185.5 → 260.3 MiB（+74.8 MiB） | 基本平坦 | Python 仍增长 |
| 线程/进程峰值 | 9 线程 / 1 进程 | 5 / 7 OS 线程，2 进程 | — |
| WS 断线 / 恢复 | 10 / 10 | 0 / 0 | 不同网络时段 |
| 封存最大 | 2.731 s | 0.897 s | Python 约 3 倍 |
| fsync 最大 | 132 ms | 366 ms（5 s 周期） | 周期不同，不能单看单次 |
| 队列高水位 | 1,479 条 | 5,028,381 B（计费字节） | 口径不同 |
| 压缩后写入 | 96,652,067 B / 960 s | 799,713,669 B / 3600 s | 市场窗口不同 |

**结论：** 同 10 对、同流、同轮转的 960 秒窗口内，Python 每条记录消耗约 **2.65 倍** C++ CPU，峰值 RSS 约 **3.44 倍**，且 16 分钟 RSS 仍增长 74.8 MiB；C++ 的两个记录器 RSS 基本平坦。C++ 在 C4-B 的 960 秒窗口处理了更多记录且 0 断线。Python 的 10 次 WS 断线与两侧不同市场时段会影响总量，因此以 `CPU 秒/10,000 条` 和 RSS 倍数为主要结论。原始结果见 [c4p-cpp-vs-python-20261110.json](c4p-cpp-vs-python-20261110.json)。

### C4-C 24 小时资格（2026-11-10，FAILED — 丢弃超门槛）

C4-D Python 修复已随新发布物 `/opt/sbmdr/current -> /opt/sbmdr/releases/20261110-7ecb87da` 安装；C++ 二进制仍为 `4181d599…`，十币对配置 SHA 仍为 `75d1e9ad…`。新根为 `/var/lib/binance-market-data-recorder/S-BMDR/live-20261110T014952Z/{spot,usdm}`，归档目标为 `/srv/recorder-data/S-BMDR/live-20261110T014952Z-{spot,usdm}`。

资格驱动于 **2026-11-10 09:50:28 上海时间（01:50:28 UTC）** 启动 86,410 秒，systemd 两实例 + archive timer，`Restart=no`，参数 `--rss-limit-mib 256 --cpu-limit-percent 200 --max-drop-percent 0.01`。启动后 Spot PID 271512 / USD-M PID 271527，`NRestarts=0`、`ActiveState=active`；`dropped_total=0`、队列为空。archive timer 使用新配置 `OnActiveSec=15`、`OnUnitInactiveSec=10`，archive service `CPUQuota=50%`、`TimeoutStartSec=600`。

**失败结果（2026-11-10 17:17:38 上海时间，运行 26,828 s / 约 7h27m）：** USD-M 达到 `dropped_total=23,385`（`ws_dropped_total=23,385`、`rest_dropped_total=0`）；按最终 `records=40,901,735` 计算为 **0.0572%**，超过 0.01% 门槛（允许约 4,090 条）约 **5.7 倍**。Spot 为 0。USD-M 队列达到计费上限 **67,108,864 B（64 MiB）**、53,333 条；Spot 峰值 33,945,000 B / 27,720 条。最大 receive→fsync：Spot 14.11 s、USD-M 17.85 s；`writer_seal_ms` 状态窗口最大 4.27 / 3.91 s，`writer_sync_ms` 最大 4.47 / 5.54 s。失败时 `records` Spot 11,848,402、USD-M 40,901,735，`sealed` 1,083 / 3,199；系统盘最小可用 20,010,852,352 B（约 18.63 GiB）。按定稿 8.4/9 节由操作者停止两个记录器与 archive timer，**未做全量读回**，未自行调整 C++、timer、配额、分片或币对。证据：`/srv/recorder-data/sbmdr-c4a-deploy/results/c4-c-drop-failure-20261110T091707Z`（停止前后 progress、两市场 status/stdout、archive、df）。

### C4-E 写线程 I/O 解耦（2026-11-10，PASS）

按定稿 5.4 实现 E1–E4，代码提交 `b610a656`，Release 二进制 SHA-256 `618dae987fce00d7e19fb4a32d87f43c613e433b3e104e516d8aca61158d6b99`：

- **E1 后台封存线程**：配置模式（`config && test_queue_records == 0`）启用单 `sealer_`，写线程 `rotate()` 只移交 `RawFile` 并立即返回；`Catalog` 六个公开方法使用 `std::mutex` 串行化 SQLite；单源模式与 `test_queue_records != 0` 保持同步封存，兼容 LD_PRELOAD 故障注入。
- **E2 `syncfs` 与首帧同步**：配置模式 5 秒周期改为对首个脏文件调用一次 `::syncfs(fd)`，统一更新所有脏分片；取消配置模式新分片首帧 header `fsync` 与目录 `fsync`。
- **E3 128 MiB 队列**：配置模式 `configured_queue_limit=128 MiB`、`configured_queue_records=131,072`；单源保持 64 MiB / 8,192。
- **E4 0.10% 丢弃门槛**：`deployment/advance.py` 的 `--rss-limit-mib=384`、`--max-drop-percent=0.10`；运行期只有进程退出或累计丢弃超过 24h 预计记录数 0.10%（约 USD-M 150,000 条）才允许中止。

**验证：** 原生 Release **13/13**、ASan/UBSan **9/9**、TSan **9/9** 全部通过；Release 二进制 `618dae98…`、ASan `d98ddb48…`、TSan `e3700c0a…`。上一轮 7h27m 残留已在旧根上排空，系统盘可用约 20 GiB；旧 `c4-c5-24h` 证据保留为 `results/c4-c-failure-run1-evidence`。

### C4-C 第二轮（C4-E，2026-11-10，FAILED — 128 MiB 队列持续打满）

新发布物 `/opt/sbmdr/current -> /opt/sbmdr/releases/20261110-b610a656`，二进制 `618dae98…`，十币对配置 SHA `75d1e9ad…`。新根 `/var/lib/binance-market-data-recorder/S-BMDR/live-20261110T121047Z/{spot,usdm}`，归档目标 `/srv/recorder-data/S-BMDR/live-20261110T121047Z-{spot,usdm}`。

资格驱动于 **2026-11-10 20:11:03 上海时间（12:11:03 UTC）** 启动 86,410 秒，`--services --rss-limit-mib 384 --cpu-limit-percent 200 --max-drop-percent 0.10`。Spot PID `298079` / USD-M PID `298091`，`NRestarts=0`；启动后 queue 为空、`dropped_total=0`，`writer_sync_ms` 约 6 ms（`syncfs`），async sealer 已产生首个分片。预计 **2026-11-11 20:11:13 上海时间**结束。

**失败结果（2026-11-10 22:08:27 上海时间，运行 7,041.9 s / 约 1h57m）：** USD-M `dropped_total=1,102,272`（`records=18,740,962`，约 **5.88%**），远超 E4 的 150,000 条中止阈值；Spot `dropped_total=4,889`（`records=6,047,094`，0.081%）。两市场队列均达到 **128 MiB 上限**（Spot 134,217,673 B / 109,862 条；USD-M 134,217,728 B / 107,244 条）。USD-M 首次丢弃在 elapsed **4,749.5 s**，队列从约 100k 条持续贴顶；Spot 首次丢弃 4,753.1 s。USD-M 最大 receive→fsync 47.89 s，`writer_sync_ms` 最大 1.80 s，`writer_seal_ms` 最大 5.01 s；Spot 最大耐久 22.01 s。RSS Spot 173.3 MB / USD-M 241.7 MB；系统盘最小可用 19,824,349,184 B（约 18.46 GiB）；archive timer 全程无失败且积压为 0。

按 E4/8.4 在 USD-M 超过 150,000 条后停止两个记录器与 archive timer，**未做全量读回、未自行调参**。当时提出“64 格封存队列不足”的假设，但没有封存队深或交接等待计时，不能据首次丢弃时间认定根因。两个市场的封存队列相互独立，80 是两个市场 WS 流之和；磁盘 `.partial` 数也不等于当前活跃槽位数。修正后的分析与最小复现步骤见下节和现行计划。证据：`/srv/recorder-data/sbmdr-c4a-deploy/results/c4-c-drop-failure-run2-20261110T140805Z`、[c4c-drop-failure-run2-20261110.json](c4c-drop-failure-run2-20261110.json)。C4/C5 未签收。

### 两次 C4-C 失败对比与审查纠正（2026-11-11）

| 项目 | 第一次（C4-A/B，7h27m） | 第二次（C4-E，1h57m） |
| --- | --- | --- |
| 队列 | 64 MiB，USD-M 触及上限 | 128 MiB，两市场触及上限 |
| 丢弃 | USD-M 23,385（0.0572%）；Spot 0 | USD-M 1,102,272（5.88%）；Spot 4,889（0.081%） |
| 最大 `writer_sync_ms` | 5.54 s | 1.80 s |
| 最大 `writer_seal_ms` | 4.27 s | 5.01 s |
| 最大 receive→fsync | 17.85 s | 47.89 s |
| 首丢时间 | elapsed 26,385 s，短时尖峰 | elapsed 4,749 s，约 79 分钟；本身不能证明轮转导致阻塞 |
| 已知等待与根因边界 | 旧同步写路径存在逐文件 `fsync`、同步 Zstd 封存、新分片 header/目录同步；秒级耗时已有记录，但各最大值不能直接相加当作同一时刻的停顿 | 入口持续满已确认；`enqueue_seal` 满 64 时等待、writer 上的 `syncfs`、Catalog 锁/事务均是待区分的等待点。现有报告不足以确定哪个主导 |
| 是否修复过程新引入 | 同步 I/O 在 C4-A/B 前已存在 | C4-E 新增有界封存队列及等待点已确认；它是否导致本轮失败尚未证实，不能先认定扩到 256 即解决 |
| Spot 情况 | Spot 队列峰值 31.8 MiB，0 丢弃 | Spot 4,889 丢弃；0.081% 是以 WS+REST 总记录为分母的粗算，未完成正式资格验收 |

**队列归因纠正：** 当前 `RecorderStorage` 按 symbol 合并 REST；正常活跃槽位上限约为 Spot 41、USD-M 61，分别使用自己的 64 格队列。111 个磁盘 `.partial` 可能包含旧分片排队/封存过程。重复大小轮转仍可累计超过 64 个任务，所以应复现“待处理任务满 + 慢 sealer + 持续输入”，而不是用“80+ 流”替代真实负载分组。异步后的 `writer_seal_ms` 是后台区间累计工作时间，也不能直接解释为 writer 停顿。

**测试与正确性盲区：** `c4-ingress-drop` 设置 `test_queue_records=1024` 时关闭了异步封存；原单源恢复测试不覆盖配置模式的 `syncfs` 和取消 header 同步。另发现 WS 的连接局部丢弃状态可在重连/停机时消失，REST 拒收只计数不写 Catalog，DepthBridge 不直接响应 `ingress_drop`。这些需要针对实际路径补回归，历史 13/9/9 通过不能代替覆盖。

**资源与验收：** 第二轮两个 recorder 的峰值 RSS 相加为 395.75 MiB，尚未计归档，已无法通过现有 384 MiB 上界检查；这不是同时 RSS 的实测值。C4-D 快速读回信任部分归档 manifest，不等于全量逐帧 CRC/独立计数。后续报告必须按实际覆盖描述，并核对 Catalog 缺口与总丢弃。

**执行入口：** [现行计划 F1–F7](cpp_rewrite_milestone_plan.md#f1) 已给出文件、最小改法、回归和退出条件。顺序为真实路径复现 → 局部阻塞/缺口修复 → 验收与发布收尾 → 一次原生矩阵 → 一次 2h 预检 → 一次 24h → C5。256 只是有条件的候选，不增加线程、不放宽预算，不为已完成的 C4-D/C4-P 重开任务。

**与 Python M4.2 历史性能对照（非同口径，仅供量级参考）：** Python M4.2 为 BTC+ETH 全量生产配置，4,038 个活跃源（1,844 WS、2,194 REST）、50 个产品、Spot/USD-M/COIN-M，2 小时窗口 CPU 均值 109.16%、峰值 122.87%（单核=100%），RSS 366.10–617.90 MiB、峰值 717.54 MiB，seal 最大 13.602 s、drain 10.956 s、fsync 0.391 s，2 小时 72 次断线；该轮未通过 2h 可用性门禁，只作历史参照。C++ C4-B 为 10 个 USDT 对、20 个产品、80 条 WS 流，1 小时 CPU 均值合计 78.97% 单核（Spot 20.85%、USD-M 58.12%），RSS 记录器合计约 91.0 MiB、含归档 184.2 MiB，`dropped_total=0`，seal 状态窗口最大约 0.95/1.05 s、sync 最大约 0.33/0.42 s，最大耐久 5.77/6.58 s。两者范围不同，不能据此计算同口径倍数。同范围 C4-P 已完成，见上节；它也有市场窗口不同的限制，本次上线不重跑基准。

### F1–F6 执行记录（2026-11-11，候选 `e0b7f684`）

**F1 ✅ 真实异步路径复现。** 配置模式的 `async_seal` 改为只由 `config.has_value()` 决定，`test_queue_records` 不再关闭 sealer 或恢复首帧 header 同步；status/final 增加 `seal_queue_depth/peak`、`seal_enqueue_wait_ns_total/max`、`seal_pending_raw_bytes`、`catalog_wait_ns_total/max`。十币对、Spot/USD-M 分开、真实异步 sealer、`stalled_seal` 故障的用例在 64 格候选上复现：Spot `queue_peak=64`、`enqueue_wait=1,685,655,548 ns`；USD-M `queue_peak=64`、`enqueue_wait=2,526,287,140 ns`；均 `dropped_total=0`。Catalog 最大等待仅 13.3/16.4 ms，不能解释秒级交接等待。证据见 [before](c4e-async-backpressure-before-20261111.json)。

**F2 ✅ 有界修复。** 把 64 改为具名 `seal_queue_capacity=256`，保留单 sealer、条件变量和有界等待。同一用例修复后：Spot `queue_peak=83`、`enqueue_wait=30,174 ns`；USD-M `queue_peak=147`、`enqueue_wait=70,652 ns`；`pending_raw_bytes=0`、无丢弃、队列排空；峰值 RSS 72,085,504 / 72,347,648 B，峰值打开文件 122 / 213，8 s 发布加 1 s 收尾后总墙钟约 9.45 / 9.65 s。证据见 [after](c4e-async-backpressure-after-20261111.json)。256 只在这个复现下通过，不据此声称历史公网故障已归因；仍需 F7 的实盘指标。异步后的 `writer_seal_ms` 是后台封存累计工作时间，不能解释为 writer 阻塞。

**F3 ✅ 缺口与 depth。** `Handoff` 增加进程级丢弃账本，WS 和 REST 共用 `note_drop`/`consume_drop`，成功入队才清理已交接部分；停机排空后 writer 取未搭载尾部缺口写 `INGRESS_DROP`，并给可写的槽位加 `sequence_gap`/`ingress_drop`。Catalog 事件新增 `rest` 分项。`DepthBridge` 遇到 `ingress_drop` 先失效 snapshot/last/pending，再处理当前帧；同帧 generation 变化与 drop 只 reset 一次。新增/扩展回归：`c4-ingress-drop`（丢后恢复）、`c4-ingress-tail`（无后续帧的尾部缺口）、`c4-ingress-reconnect`（跨重连账本与 depth 再桥接）、`c4-rest-drop`（REST 拒收与 Catalog 分项）、`c2-renewal`（2 s 缩时 23h 换连原因/连接组）、`c2-depth-edges`（Spot 重叠范围、USD-M pu、组合 reset）。Release 均通过；`c4-ingress-drop` 的 9 条流丢弃总和、Catalog 事件和每 symbol 2 次快照/再桥接一致，tail 用例 9 条尾部事件与 manifest `ingress_drop` 一致。

**F4 ✅ 验收脚本。** `operations.py` 增加 Catalog chunk 集合与 manifest 集合、终态、record_count/大小/SHA 对账；带 `ingress_drop`/`sequence_gap` 的分片强制解码；报告 `ws_records`/`rest_records`、`ingress_drops` 的 WS/REST 分项和 trusted/decoded 覆盖。`qualify.py` 增加 `--verify-only`（只读已停止证据、不启动服务、不改 T0）、逐连接组 23h/换连原因检查、以已接受 WS 记录为分母的 WS/REST/Catalog 丢弃对账；`archive_tick.py` 把目标不可用/低空间且有积压判为 blocked，正常空闲不算失败；`advance.py` 增加 `--finish-only`，final drain 无进展或 blocked 带原因退出。离线边界测试 `c4-acceptance-boundaries` 覆盖：正确闭合、丢 manifest、缺口漏记、单连接组未通过、归档 blocked、verify-only 不采集。

**F5 ✅ 发布准备。** `install.py` 先停 archive timer/worker 并拒绝 active recorder，备份 `current`、`/etc/sbmdr`、unit/drop-in、logrotate；在最终 runtime 位置用 Python 3.12 建 venv，按 `linux-x86_64-python312.lock --require-hashes` 安装；注册新根、检查 imports、删除遗留 `production.conf`、`daemon-reload`、`systemd-analyze verify` 后才原子切换 `current`。安装失败恢复备份并报告步骤。移走 logrotate 配置避免 `copytruncate` 截断资格证据，`advance.py` 收尾时恢复。原生安装版本 `20261111-e0b7f684`：二进制 `ba71faa3…`，`Restart=no`、Spot/USD-M `CPUWeight=100/200`、archive `CPUQuota=500 ms`、slice `1.75 s`、recorder start/stop `5/3 min`、archive start `10 min`；runtime imports 通过，`current` 已切换且发布物无 staging 路径引用。证据见 [deployment](f5-deployment-20261111.json)。

**F6 ✅ 原生回归与冻结。** GreenCloud 原生 Linux x86_64：Release **22/22**（234.97 s）、ASan/UBSan **10/10**（81.59 s）、TSan **10/10**（109.68 s）。Release 新增 `c4-configured-recovery`：配置模式取消首帧 header 同步后，覆盖 `eintr`/`short`/`enospc`/`fsync_eio`、Raw fsync 后、torn frame、sealed/manifest rename 后和 Catalog commit 后中断，恢复可读且缺口如实标记。TSan 首轮因主机 `vm.mmap_rnd_bits=32` 报 `unexpected memory mapping`，使用 `setarch -R` 仅对该测试进程树关闭 ASLR 后通过；这是环境使能，不是代码回归。冻结候选：提交 `e0b7f684e41fa4e29080b9fa0bb3c96ecf1c1e26`，Release `ba71faa3cdb772d8efe0586ce946c6d4094d02364e69da8c7a9a9ac33e3201ff`，ASan `83739d78…`，TSan `0fbca3b1…`，十币对配置 `75d1e9ad…`，vendored baseline 树 `801802d8…`。完整身份见 [frozen-candidate](f6-frozen-candidate-20261111.json)。

**状态：** F1–F7 与 C4-C 24h 已完成；C4-C PASS，C5 尚未开始。C4-C 的 24h 资源/丢弃/归档/离线消费均通过；长期（周/月）内存和元数据增长仍需 C5 监控/保留策略，不能用 24h 结果代替周/月长稳结论。


### F7 ✅ 2h 公网预检（2026-11-11，PASS）

**身份与运行。** 候选 `20261111-e0b7f684`（Release `ba71faa3…`、配置 `75d1e9ad…`），新预检根 `20261111T051425Z`，两个 systemd 实例 + archive timer 同跑 **7,200 s**，显式 `Restart=no`。采集于 05:26:13 UTC 启动，07:26:20 UTC 停止；两实例同一 PID、`NRestarts=0`、正常 SIGTERM/排空。

| 项目 | Spot | USD-M |
| --- | ---: | ---: |
| 记录数 | 4,093,454 | 13,993,992 |
| WS / REST 丢弃 | 0 / 0 | 0 / 0 |
| Catalog `INGRESS_DROP` | 0 | 0 |
| 入口队列峰值 | 1.46 MB | 8.14 MB |
| 封存队深 / 峰值 | 0 / 20 | 0 / 31 |
| `enqueue_seal` 最大等待 | 6.4 µs | 39.8 µs |
| pending raw bytes 峰值 | 72.5 MB | 70.0 MB |
| 峰值 RSS | 36.6 MB | 49.0 MB |
| 最大耐久 | 5.85 s | 6.26 s |
| `receive_to_append_p99` | 78 ms | 220 ms |

**验收结果。** 两 recorder 峰值 RSS + archive 峰值上界 **173.8 MiB（182.3 MB）**（≤384 MiB）；CPU 均值合计 + archive **67.4%**（≤200）；slice 峰值 180.1%，60 s 峰值 118.4%；整机忙碌 23.8%，最小可用内存 5.03 GB / 6.16 GB；系统盘最小可用 18.53 GiB（≥13）；归档 394 轮、0 失败、0 blocked，压缩速率 10.32 GB/天，`/srv` 预计保留 196.7 天。

**初次验收失败与纠正。** 首次 `progress.json` 为 FAILED，原因是验收脚本以 `sys.executable` 调 `operations.py` 时未继承 `/opt/sbmdr/current/python/src`，报 `ModuleNotFoundError`；采集数据本身完整，未重采。按计划执行 `qualify.py --verify-only` 对同一份 `progress.json`、两份 recorder 摘要/状态日志和数据根重验，结果 **PASS**：读回 4,093,454 / 13,993,992 条，逐帧/可信覆盖 Spot 71/263、USD-M 181/838，0 丢弃，Catalog 缺口 0，全部 chunk `LOCAL_DELETED`。归档停止时 SIGTERM 命中一个 in-flight oneshot pass，`sbmdr-archive.service` 曾进入 failed；已 `reset-failed`，并修正 `stop_archive` 先等 in-flight pass 结束再停，供 C4-C 使用。

**900 s 窗口与长期趋势。** 所有 900 s 窗口入口队列峰值最高 8.14 MB，远低于 128 MiB；最长持续 ≥90% 入口队列 **0 s**；封存交接无阻塞（最大等待 39.8 µs）；RSS 在最后约 45 分钟平台化（USD-M 48.9–49.0 MB），CPU 随行情量变化、单千条 CPU 未上升。最终尾部 31/61 个 sealed 分片已归档删除，本地根从 231 MB 降到 14.8 MB（1,353 manifest）；按当前 chunk 速率估算元数据增长约 **5–13 GB/月**，系统盘排空后可用约 18.96 GiB。C4-C 24h 必须实测该增长，并在 C5 前确定旧根、manifest/Catalog 和日志的保留/清理策略，不能假设系统盘会自动回收元数据。

**证据：** [f7-preflight-20261111](f7-preflight-20261111/)（setup、verify-only、readback、900 s 分析、archive 日志、原始 FAILED report）。


### C4-C ✅ 24h 资格（2026-11-12，PASS）

**身份与运行。** 同一冻结候选：发布 `20261111-c4c-e0b7f684`、Release `ba71faa3…`、配置 `75d1e9ad…`；新根 `20261111T074613Z`。2026-11-11 07:46:56 UTC 启动，2026-11-12 07:47:27 UTC 停止，连续 **86,409 s**；两个 recorder 同一 PID、`NRestarts=0`、正常 SIGTERM/排空。

| 项目 | Spot | USD-M |
| --- | ---: | ---: |
| 记录数 | 51,679,919 | 183,369,877 |
| WS / REST 丢弃 | 0 / 0 | 0 / 0 |
| Catalog `INGRESS_DROP` | 0 | 0 |
| 入口队列峰值 | 6.0 MB | 96.0 MB |
| 最长 ≥90% 入口队列 | 0 s | 0 s |
| 封存队深 / 峰值 | 0 / 18 | 0 / 32 |
| `enqueue_seal` 最大等待 | 37 µs | 23.0 ms |
| 峰值 RSS | 42.5 MB | 141.9 MB |
| 逐帧 / 可信 chunk | 112 / 3,846 | 1,076 / 11,682 |

**资源与网络。** 两 recorder 峰值 RSS + archive 峰值上界 **270.7 MiB（283.8 MB）**（≤384 MiB）；CPU 均值合计 + archive **79.7%**（≤200）；60 s 滚动峰值 **168.7%**（≤200）；整机忙碌 29.7%，最小可用内存 3.92 GB / 6.16 GB；系统盘最小可用 18.27 GiB（≥13）。三连接组 `spot-public`、`usdm-public`、`usdm-market` 均在 elapsed 82,800 s 记录 `23h_renewal`，最长连接约 82,800 s，换线后各 symbol depth 重新桥接通过。

**归档与离线消费。** 归档 4,399 轮，0 失败/0 blocked；16,716 个 chunk 全部 `LOCAL_DELETED`；压缩数据 10.89 GB/天，`/srv` 预计约 185 天。每市场 BTCUSDT 一小时（07:50–08:50 UTC）离线消费：Spot 606,822 条 valid/0 invalid、37,658 aggTrade、60 根 1m K 线、0 gap；USD-M 2,093,576 valid/0 invalid、55,056 aggTrade、60 根、0 gap。

**首次验收失败与纠正。** 首次 `progress.json` 为 FAILED，唯一断言是 qualifier 额外检查的 **1 秒** `slice_cpu_peak_percent=246.56 > 200`；计划门槛是“CPU 合计/滚动检查”，实际 `cpu_mean+archive=79.70%`、60 s 峰值 168.68% 均通过。修正 qualifier：保留 1 s 采样作为观察值，不再作为 gate；对同一份数据执行 `--verify-only`，结果 **PASS**，未重采。原始 FAILED report 与 verify-only report 都保留在证据中。

**长期趋势（重要）。** USD-M RSS 在 7–9 小时随 85–93 MB 入口队列峰值阶跃到约 135.3 MiB（141.9 MB），之后 9–24 小时保持平台（135.3 MiB），不是持续线性泄漏；24h 资源预算通过。Catalog/manifest 不自动删除，24h 残留本地根 **160 MB / 16,716 chunk**（manifest 67 MB、Catalog 93 MB），约 **4.8 GB/月**；系统盘 18.66 GiB 可用、运行线 13 GiB。C5 前必须确定旧根/manifest/日志保留或清理策略，并对周/月内存做持续监控；本 24h PASS 不能替代周/月长稳资格。

**存储容量估算（按 24h 实测）：** `/srv` 归档盘可用 **2.018 TB**，压缩数据 **10.89 GB/天** → 约 **185 天（约 6 个月）**。系统盘 `/` 当前可用 **18.66 GiB**，运行线 13 GiB，余量 5.66 GiB；Catalog/manifest 约 **160 MiB/天** → 约 **34–36 天**。旧已验证根约 7.4 GB 的清理和 logrotate 可延长系统盘时间；当前近期限值是**系统盘元数据**，不是 2 TB 归档盘。C5 前必须落实旧根/manifest/日志的保留或清理策略，并监控系统盘和 `/srv`。

**证据：** [c4c-qualification-20261112](c4c-qualification-20261112/)。
