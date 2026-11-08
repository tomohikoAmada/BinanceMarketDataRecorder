# C3–C5 公网验收与部署记录

首版 C3 候选已完成 2026-11-08 17:57:39–19:00:55（上海时间）的 60 分钟公网与完整独立读回：Spot 356,069 条/64 分片，USD-M 1,298,443 条/92 分片；CPU 均值合计约单核 17.03%，峰值 RSS 合计约 61.38 MiB。二进制 SHA-256 为 `2051b262fa831c02a35aad5bda580db9d44c4bbb07382b487a941f4326e515a3`。补充配额扣账、成交时间统计、配置流检查和归档并发等待后的最终候选需从新 T0 重跑，不拼接时长。C3 最终签收和 C4/C5 24h 资格仍待完成。

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


2026-11-09 发现并修复离线消费问题：首轮只检查了 Parquet 构建/回放计数，未检查 `valid`，冻结 Python 解析器把 combined WS 外层误当成事件体。`normalize.py` 现只向冻结解析器提供 `data` 视图；Raw、原 payload hash、分区/回放契约保持原样。新增原生回归核对字段、有效性和原 Raw 不变。Spot 补验 29,508 行全部有效，USD-M 补验进行中。旧错误构建和失败日志保留；由于构建 ID 按 Raw 输入确定，修复验证使用独立消费夹具，避免覆盖既有 immutable manifest。上线后须通过此适配入口生成派生数据。

过载路径另去除了 BTCUSDT 的固定诊断标识：异常给出实际市场/标的/流，Catalog 将市场级影响标为全局，所有已受影响分片继续带缺口标记。该修改后 C++ 二进制固定为 `234264f5adb64ea007137781c9c56652522ccfedd28919ecc107ccb092dffe49`，最终 C3 新 T0 为 **2026-11-09 01:45:22（上海时间）**。此前 01:01 开始的候选运行于约 42 分钟时停止，标为 SUPERSEDED，保留 413,732 条 Spot 和 1,946,437 条 USD-M，不计入最终 60 分钟。
