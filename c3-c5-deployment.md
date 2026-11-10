# C3–C5 公网验收与部署记录

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

**尚未完成：** C4-C 24h 资格、C5 启用与交接。C4-B 已部署并 PASS；10:56 失败轮按定稿未做全量读回。

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

**与 Python M4.2 历史性能对照（非同口径，仅供量级参考）：** Python M4.2 为 BTC+ETH 全量生产配置，4,038 个活跃源（1,844 WS、2,194 REST）、50 个产品、Spot/USD-M/COIN-M，2 小时窗口 CPU 均值 109.16%、峰值 122.87%（单核=100%），RSS 366.10–617.90 MiB、峰值 717.54 MiB，seal 最大 13.602 s、drain 10.956 s、fsync 0.391 s，2 小时 72 次断线；该轮未通过 2h 可用性门禁，只作历史参照。C++ C4-B 为 10 个 USDT 对、20 个产品、80 条 WS 流，1 小时 CPU 均值合计 78.97% 单核（Spot 20.85%、USD-M 58.12%），RSS 记录器合计约 91.0 MiB、含归档 184.2 MiB，`dropped_total=0`，seal 状态窗口最大约 0.95/1.05 s、sync 最大约 0.33/0.42 s，最大耐久 5.77/6.58 s（5 秒同步周期的有意取舍）。结论：在各自已测范围内，C++ 的 CPU、RSS 和封存停顿都明显更低；但 C++ 当前源范围远小于 Python M4，不能据此给出同口径倍数。严格的同范围比较仍需让冻结 Python 基线在相同 10 对/80 流配置下跑同一时段。
