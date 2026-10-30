# P3：恢复与后台任务不再互相拖累

2026-10-30，本机开发交付。起点为合并 P2 的 main `ec46e32`，工作分支
`codex/p3-recovery-background`。用户本轮授权完成 P3；未连接 VPS、部署、启停生产服务，
未访问生产数据或真实外卷。P0–P2 的独立验收和历史证据保持不变。

## 实现

- 恢复继续由现有 WS 连接/退避、产品盘口快照/序列桥、公共 REST 调度负责。
  辅助 HTTP、超时、字段或发布期错误只使对应源不可用；Raw/Catalog 写失败升级至服务，
  正常停止/排空，平台 service 负责进程重启。没有新增全产品重启循环。
- `collector/availability.py` 与既有 coverage owner 持久化逐源区间和错误/恢复计数。
  断连、异常字节、盘口未桥接、REST 期限、旧/空闭 bar、统计查询期和交割发布分别可见。
  收到但未写入且10秒无进展的连接组不能算健康；安静的清算流不因没有事件被判掉线。
  快速恢复不删除旧缺口，重启从上次持久观察边界保守计未知；停止、生命周期和时钟不确定性有明确边界。
- SEALED 同事务登记新文件待校验项；独立子进程一次顺序读取完成存储/解压双摘要、
  header/帧/CRC/规范编码及产品、数量、时间、序列/缺口统计。成功证明不可变，绑定原文
  manifest 摘要和 verifier 版本；失败保留文件、最初失败证据和可重试状态，不重新采集替代。
- 一根目录一个 verifier 锁，32文件/512MiB/批次间30秒，实际读取16MiB/s。
  service 根据 rotation/max-frame 扩大字节预算和超时，单个文件不被误称为硬30秒内完成。
  路径解析位于storage层，verifier不导入规范化/PyArrow。两个后台家族各一子进程：日报和新文件校验；停止时终止并回收子进程。正常归档继续，
  既有 drain 锁/文件/时间限制外增加16MiB/s复制/校验节流。
- 所有 service 配置监控内部容量，显式 VPS profile 保留原规则，普通配置使用既有动态
  hard reserve。实际触及储备时记录事件、打开缺口并有序停采，绝不删除未归档唯一副本。
  日增量/净增长、归档积压和吞吐继续由已有 forecast/archive/daily 工具报告。
- 封存读回复用同一压缩文件读取获得双摘要；规范化同一 Raw 的子记录复用必要摘要，
  删除无实际消费方的普通 poll 模型摘要和临时分区名摘要，进程内复用 SDK 版本。
  完整性/公开身份摘要保留；没有 per-event hash chain，也没有 Raw v1 重写或旧数据迁移。
- 规范化只按需运行，支持产品、UTC接收日期、指定 chunk 或新封存时间筛选。
  受限构建有版本化选择对象、独立 build ID 和相应 replay 验证；旧构建/公开行字段保持兼容。

详细责任、观察边界和格式在 [ADR-0056](../../adr/0056-source-recovery-and-bounded-background-verification.md)。

## 本机同输入比较

[原始结果](processing-comparison.json)由 `tools/benchmark_p3_processing.py` 产生：
macOS ARM64 / Python3.12.14，64帧，16,805,234个 Raw 字节，16,805,635个压缩字节，
8轮交替 A/B、B/A。两种字节身份逐项相同。

| 路径 | 原实现 | P3 | 范围 |
| --- | ---: | ---: | --- |
| 压缩文件顺序读取 | 2次 / 33,611,270字节 | 1次 / 16,805,635字节 | 读取量减少50% |
| wall中位数 | 13.940 ms | 13.109 ms | 本机热缓存约降低5.96% |
| CPU中位数 | 13.938 ms | 13.105 ms | 本机约降低5.97% |
| Python峰值分配 | 2,229,201字节 | 2,230,284字节 | 基本相同，未声称内存收益 |
| 1MiB Raw / 128子记录摘要 | 128次 / 44.040 ms | 1次 / 0.338 ms | 仅摘要复用局部对照，非规范化整条路径加速 |

这是本机局部比较，**不证明全功能 CPU、VPS吞吐、双币容量或长期连续性**。
存储/解压摘要保护不同表示；归档写后读回、重试和删除前校验并未作为“重复”删掉。

复现仅创建并清理 Python 临时测试目录：

```bash
.venv/bin/python tools/benchmark_p3_processing.py --output <new-result.json>
```

## 稳定操作入口

以下是后续部署/运行手册的命令示例，本次没有执行生产操作。
全局 `--config <config>` 必须在子命令之前；保持既有安全的内部 data root。

```text
binance-market-recorder --config <config> systemd start
binance-market-recorder --config <config> systemd stop
binance-market-recorder --config <config> systemd status
binance-market-recorder --config <config> status
binance-market-recorder --config <config> verify status
binance-market-recorder --config <config> verify new
binance-market-recorder --config <config> verify new --through-utc-ns <enrollment_high_water_utc_ns>
binance-market-recorder --config <config> verify new --chunk-id <id>
binance-market-recorder --config <config> archive status
binance-market-recorder --config <config> archive drain --storage-id <registered-id> --max-files 32 --max-runtime-seconds 50
binance-market-recorder --config <config> storage forecast
binance-market-recorder --config <config> report daily --date <UTC-YYYY-MM-DD>
binance-market-recorder --config <config> report continuity --start-utc-ns <start> --end-utc-ns <end> --source-id <venue/scope/stream/transport>
binance-market-recorder --config <config> normalize run --product spot:BTCUSDT --start <YYYY-MM-DD> --end <YYYY-MM-DD>
binance-market-recorder --config <config> normalize run --sealed-since-utc-ns <start>
binance-market-recorder --config <config> normalize run --chunk-id <id>
```

macOS沿用现有 `launchd start/stop/status`。健康运行由原生 service 管理，SSH/聊天断开不结束它。
停止交接（stopped handoff） 须同时记录 `systemctl is-active` / `systemctl is-enabled`，授权启动前保持 disabled。

`status` 中逐产品 side_data/shared_public_sources 的 availability 包含源身份、当前缺口起点/秒数、
恢复次数和上次错误；`catalog.raw_verification` 包含新文件积压/最老年龄/错误。
`verify new` 每次只处理有界批次，返回0不等于全部完成；检查冻结边界下 `pending_files=0` 且
`failed_files=0`。当前文件不可分割；需更大 CLI 预算时显式传 `--max-bytes`。
service 会自动重试，catch-up 后不读取旧 Raw；已退休文件可从注册并就绪的本地归档读取，
不可用的远端/外卷副本保持 pending/failed，不能冒充已校验或由 receipt 代替帧检查。

`report continuity` 的默认源集合是已观察源。P4冻结配置时必须保存完整应提供服务的源ID并逐项
传入（`--source-id` 可重复），否则报告仅供诊断；预期但未发现源为 UNKNOWN_SOURCE。
报告按每源区间并集计算，不以行数或其他高流量源填平缺口。结束时间后尚未持久记录的尾部
计未知；等待后台补完冻结时点的证明，再读取相应结果。P2首版交割 premium 例外继续单列，
不能在结果出来后去除其他失败源。

## 检查与剩余边界

代码提交 `b72761a1b5eb6ee52d1b3a473079ac5d5b5ef23a` 的完整离线套件通过：
**2,404 passed / 24 skipped / 5 stress未选 / 13条既有fork警告**，188.42秒。
35项新增回归单独通过；Ruff、355文件strict mypy、M0、Go golden、sdist/wheel构建及
新wheel隔离安装后的CLI冒烟均通过。详见 [checks.json](checks.json) 与
[完整输出](pytest-b72761a.txt)。新增35项回归覆盖源间隔离、期限/发布期、盘口/稀疏源、
写入停滞与监控暂停/倒退时钟、正常停启与原文/进度/排空、持久校验/失败修复/并发预算、
归档源退役后验证、元数据/CRC/压缩损坏、读后变异、受限构建与旧 replay，以及普通配置储备停采。
并行回归实际运行持续收写、独立校验、正常归档、日报、规范化和 replay，检查保存字节/数量。
现有重连、快照、写失败、冷却、归档故障和 reserve 检查复用原测试；没有重做 P2 的历史审计。

当前结论是 **P3本机开发交付**。生产 readiness 仍为 NO。双平台CI、Linux ARM64运行、
真实VPS同负载性能、实际磁盘日增量/正常归档吞吐及P4–P6长跑均不从本机fixture取得信用。
下一阶段 P4 冻结 BTC 范围/配置/版本，在授权的真实运行中取得2h检查点并保持同一进程到24h；
正常归档、校验、宿主任务必须开启。P3不启动该运行。
