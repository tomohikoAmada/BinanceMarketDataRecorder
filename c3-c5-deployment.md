# C3–C5 公网验收与部署记录

首版 C3 候选已完成 2026-11-08 17:57:39–19:00:55（上海时间）的 60 分钟公网与完整独立读回：Spot 356,069 条/64 分片，USD-M 1,298,443 条/92 分片；CPU 均值合计约单核 17.03%，峰值 RSS 合计约 61.38 MiB。二进制 SHA-256 为 `2051b262fa831c02a35aad5bda580db9d44c4bbb07382b487a941f4326e515a3`。补充配额扣账、成交时间统计、配置流检查和归档并发等待后的最终候选需从新 T0 重跑，不拼接时长。最终 C3 已通过，C4/C5 24h 资格仍待完成，见下方签收补记。

首版原生 Release 八项、ASan/UBSan 和独立 TSan 各五项相关回归已通过。新增第九项离线 K 线测试因缺少 Python 基线路径首轮失败，现修正 CTest 环境后复验。最终固定源码、二进制与各测试结果另行绑定。

显式配置为 [四币对配置](config/c3-core.json)，扩展候选为 [十二币对配置](config/c4-mainstream.json)。扩展名单参考同日官方 Spot 24h quoteVolume 与 TRADING USD-M 永续交集，固定选择十个熟悉的 USDT 主流对，加 BTCUSDC、ETHUSDC；[选择证据](config/c4-selection-20261108.json)保留当时排名和快照摘要，无运行时 Top-N 调整。

为复用已验收的单市场生命周期，本轮使用两个隔离记录器进程，资源按两者合计验收。Spot 一条组合连接，USD-M 按[官方路由](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Important-WebSocket-Change-Notice)拆为 public（depth/bookTicker）和 market（aggTrade/markPrice/forceOrder）两条。每连接一个有界接收 worker、每市场一个 REST worker 和一个单写 worker；共七个业务 worker，没有压缩线程池。此处替代原计划“全部连接复用一个 WS 线程”的成本假设，以最小改动复用 C2，并报告实测 CPU/RSS。

启动先读取和保存目录，只接受白名单内处于 TRADING 的标的。Spot 目录使用 symbols 过滤参数，避免获取全市场大响应；USD-M 目录全市场保存一份。按市场、标的、流分别维护 Raw 文件，桥接状态按标的隔离。每分钟输出内存中的源计数、最近接收/追加/fsync 和缺口信息；不写逐 poll SQLite 健康行。重连首条按各流分别标缺口。

跨盘链路复用固定 Python ArchiveManager/StorageRegistry/archive_drain，保持复制、验证、登记、删除的事务顺序。隔离复制的 C++ Raw 已通过真实 /dev/vda1→/dev/vdb1 归档和外部读取；复制中断及目标哈希损坏均保留源，重试后成功。原生产 Python 数据及旧服务不切换、不删除。

C4/C5 使用独立 sbmdr-recorder@spot、sbmdr-recorder@usdm 和 sbmdr-archive.timer。资格运行期间 Restart=no，固定 PID，任何提前退出都记失败；24h 及独立全量读取通过后才改 Restart=on-failure 并 enable 常驻。归档在独立 oneshot 进程，不进入写线程；其 CPU/RSS 和压缩日增量纳入报告。角色 bmdr，无需 API key 或交易权限。

离线工具复用冻结 Python 的 Raw→Parquet、回放和 backfill CLI。新增 candles.py 只处理显式时段的 aggTrade，以 Decimal 生成 UTC 整分钟倍数 OHLCV，不补缺口，不把观测到的成交伪装成完整历史。重复/冲突 ID 和缺口如实检查。官方历史文件依照[官方公开数据说明](https://github.com/binance/binance-public-data)，下载后核对 CHECKSUM；2025 年起 Spot 历史文件时间为微秒，与实时 WS 的毫秒分开处理。

最终签收须补入：C3 全量读取报告、C4 档案和离线消费验证、24h 同 PID/23h 换连/重连桥接证据、两盘空间与归档循环、资源和保留天数，以及正式启用后的 systemd 状态。当前文档不将正在运行的长测写成已完成。

部署路径与日常入口（安装后生效）：

- 配置：`/etc/sbmdr/config.json`，两个市场各十二对、共 24 个产品。修改后需重启对应实例，新一轮资格使用固定配置。
- 记录器：`/opt/sbmdr/current/bin/binance-market-recorder` 为固定 C++ Release 的别名；实时调用方式见 unit 的 `record --public ROOT COUNT CA MARKET CONFIG`。Python 离线工具仍由该 release 的 `venv/bin/python` 调用，设置 `PYTHONPATH=/opt/sbmdr/current/python/src`。
- 状态：`systemctl status sbmdr-recorder@spot sbmdr-recorder@usdm sbmdr-archive.timer`；分钟源状态在 `/var/log/sbmdr/{spot,usdm}.status.jsonl`，归档结果在 `archive.jsonl`。
- 读取或手动归档：`prototype/operations.py verify ROOT` / `archive ROOT`，使用 `/etc/sbmdr/roots.json` 中的隔离根；归档仅在完成目标验证后删除源 `.zst`。
- 归一化/回放：`prototype/normalize.py ROOT --manifest MANIFEST --replay-symbol BTCUSDT`；OHLCV：`prototype/candles.py --help`；官方历史：`prototype/history.py ROOT --market spot --symbol BTCUSDT --date 2026-11-06 --interval 1m`。

资格驱动只接受当轮完整持续时间与全部分片校验；24h 还从 Raw 的每连接接收时间范围独立确认至少一条实际持续约 23h 的连接，要求后续 depth 连接有桥接标记，不能只凭“23h 之后出现一次重连日志”代替主动换连证据。旧容量压力失败不随此次多源验收改写。

资源门禁沿用 C0 冻结预算：100% 为单核，60 秒滚动 CPU ≤120%，约 1 秒采样窗口 ≤200%；C4 另要求整机平均 CPU ≤50% 与可用内存 ≥50%。两个实例与归档放入同一 `sbmdr.slice`，直接读取原生 cgroup `cpu.stat` 合计采样，不将不同时间的进程峰值相加冒充同步峰值；无 CPU quota。两记录器峰值 RSS 与归档父/子进程峰值上界合计 ≤256 MiB。C5 的单核 30–50% 是收益目标，实际均值与是否达到该示例目标单独披露，不以 200% 平均值替代稳态预算。记录器原生运行还包括每条 WS 的 Asio DNS resolver 辅助线程，首轮观测 Spot 5、USD-M 7 个 OS 线程；七个业务 worker 不等于 OS 总线程数。


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


最新固定 T0 为 **2026-11-09 05:56:25 上海时间**，release `/opt/sbmdr/releases/20261109-a1507458`，二进制 `700f0b…`；新根 `live-20261108T215622Z`，公开大小目标 32 MiB。Release 12、ASan/UBSan 6、TSan 6 均通过。预计 11-10 05:56:35 满 24h，随后全量读取。前两次失败不计时、不覆盖；目前尚未签收 C4/C5 或 enable 常驻。
