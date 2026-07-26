# Binance Market Data Recorder — 代码维护者指南

> 本文档面向需要理解、审查或修改此项目代码的开发者。
> 本文为纯文档追加，不改变任何运行行为、接口或数据格式。

## 1. 项目解决的问题

Binance Market Data Recorder 是一个独立的、无 API 密钥的 Binance 公共市场数据录制器。
它将 Binance Spot 和 USD-M 永续合约的 WebSocket 原始字节流（diff depth、agg trade、book ticker）
和 REST 快照（depth snapshot、exchange info 等）持久化为不可变的原始数据（Raw），
再派生为规范化的 Parquet 数据集和确定性重放（Replay）。

## 2. 数据流全景

```text
Binance WebSocket / REST API
        │
        ├─> binance.spot / binance.usdm （传输 + schema 模块）
        │       │
        │       v
        └─> collector （接收时间戳 → 有界队列 → JSON 解析 → EventEnvelope）
                │
                v
        spool （frame + CRC32C + CBOR → .partial 文件）
                │
                v
        spool.seal （扫描 → 压缩 → 校验 → 原子重命名 → manifest → Catalog 提交）
                │
                v
        Raw chunk （不可变 .bmdr.zst）
                │
        ┌───────┴──────────────────┐
        v                          v
  archive （外部介质）         normalize （Parquet）
        │                          │
  外部 volume                   replay （消费者合约）
        │                          │
  LOCAL_DELETED             任意消费者
```

## 3. Live vs Historical 的区别

| 维度 | Live | Historical |
|------|------|------------|
| 时钟 | receive UTC + monotonic | archive_event_time（无本地接收时间） |
| 来源 | WebSocket + REST SDK | data.binance.vision ZIP |
| 认证 | 无 | 无 |
| 校验 | CRC32C + SHA-256 + Zstd checksum | .CHECKSUM 文件 |
| L2 深度 | 有（diff depth 实时重建） | 无（官方归档不提供） |
| 自动混合 | 否 | 否 |
| 重放时钟 | receive-time 重放 | 不支持 receive-time 重放 |

Live 和 Historical 从不自动混合。Historical 行带有 `clock_semantics=archive_source`
标记。Replay 支持 `ReplayClock.RECEIVE_TIME` 和 `ReplayClock.EXCHANGE_TIME`，
所选时钟决定排序键。缺少 exchange time 时，`MissingExchangeTimePolicy` 为
`ERROR`、`EXCLUDE` 或 `FALLBACK_RECEIVE`；`GapPolicy` 为 `ERROR`、`INCLUDE`
或 `EXCLUDE`，依据 `source_gap` 和 `source_complete` 处理来源缺口。

## 4. Spot 和 USD-M 模块边界

| 市场 | Spot | USD-M Perpetual |
|------|------|-----------------|
| 序列语义 | `U/u`（first/final update ID） | `U/u/pu`（含 previous final update ID） |
| bootstrap 规则 | `U <= snapshot.last_update_id + 1 <= u`（R-034 Open 冲突） | `U <= snapshot.last_update_id <= u` |
| WebSocket 端点 | `stream.binance.com:443/ws` | `/public/ws/`, `/market/ws/` |
| side data | exchange_info | mark price, liquidation + 11 REST poll 类型 |

Spot 和 USD-M 各拥有独立的连接、队列、checkpoint 和 Catalog 指标。市场级
Depth Resync 相互隔离，side-data 失败不停止核心 L2。若任一核心 Collector 任务
终止，`MarketCollectorSupervisor` 会设置全部子 stop 事件、等待封口并抛出
`CoreMarketTerminalFailure`，由 launchd 重启整个进程。

## 5. Snapshot 与 Depth Resync

**Snapshot 生命周期：**
1. 三个 WebSocket 流（diff_depth、agg_trade、book_ticker）先于 snapshot 启动。
2. diff_depth 事件被缓冲到 `LocalBookReconstructor._buffer`（有界，默认 8192 条）。
3. 公共 REST depth snapshot（limit=1000）被获取并持久化。
4. snapshot 尝试桥接缓冲：Spot 接受
   `U <= snapshot.last_update_id + 1 <= u`，USD-M 接受
   `U <= snapshot.last_update_id <= u`。
5. 成功后，缓冲的 diff 被依次应用到 Book，然后进入实时 `_apply_live` 模式。
   USD-M 实时连续性要求下一事件的 `pu` 等于当前本地 Book 的 `update_id`。

**Depth Resync（ADR-0023）：**
- 触发条件：`unexpected_disconnect`、`planned_rotation`、`server_shutdown`、`sequence_gap`、`bootstrap_buffer_overflow`。
- Gap 触发 `RESYNC_REQUIRED`；offending update 保留为 `_buffer` 首项，
  非同步期间后续事件继续进入有界缓冲。
- Collector 的完整恢复周期随后可调用 `restart_bootstrap` 清除派生状态，
  停止当前 capture session → 带 jitter 回退 → 新连接 + 新 snapshot → 重新桥接。
- Spot 和 USD-M 的 resync 隔离：各自独立的 `DepthResyncCoordinator`。
- R-034 仍为 Open：官方 Spot bootstrap 文辞与官方 toolbox 示例存在冲突。代码使用 `L+1` 规则，不作官方纠正声明。
- M6 创建和验证本地订单簿 Checkpoint；M15 Normalizer 将经验证 Checkpoint 绑定到
  Normalized Build Manifest；M16 Replay 消费 Checkpoint。Checkpoint 临时文件后缀
  为 `.partial`，当前模块不负责清理此前遗留的临时文件。

## 6. Spool 和 Archive 事务

**Spool 状态：**
- `ACTIVE.partial` → `RECOVERED.partial`（可选，尾部截断）→ `SEALING` → `SEALED`。

**Seal 步骤：**
1. 扫描所有 frame，验证 CRC32C，计算统计信息和 uncompressed SHA-256。
2. Zstd level 3 压缩到 `sealed/<chunk_id>.bmdr.zst.partial`。
3. 解压回读，验证与原始 uncompressed SHA-256 匹配。
4. 原子重命名，fsync 目录。
5. 写入 manifest JSON（记录所有统计信息、双重哈希和 complete 标志）。
6. Catalog 提交 SEALED。**仅此后才删除 .partial 源。**

**Archive 事务（ADR-0015）：**
1. 保留最旧的 SEALED chunk。
2. 流式复制到外部 `.copying` 临时文件。
3. fsync + 完整 readback + SHA-256 验证。
4. 原子重命名为最终不可变文件名。
5. 提交外部 manifest（嵌入 Raw manifest base64）。
6. Catalog 提交 VERIFIED。
7. 单独授权内部源删除：**仅在所有前面步骤成功后**。

**kill -9 恢复：**
- 每个 frame 有独立的 CRC32C。尾部截断到最后一个有效 frame（`ftruncate`），标记 `RECOVERED`。
- 如果 chunk 处于 `SEALING` 状态且在 seal 期间崩溃，恢复过程重新执行 `seal_partial()`。
- 已写入 manifest 但未提交 Catalog 的已密封 chunk，通过 `reconcile_sealed()` 恢复。

## 7. Catalog 职责

`storage/catalog.py` 是所有状态转换和元数据的**唯一经久化点**。

- SQLite 仅存储生命周期元数据（chunk 状态、archive 事务、deployment 会话、metrics 聚合、side-data cursors、operational events）。
- **不存储**：Raw 负载字节、价格、数量、序列号、单个 market event 行。
- 所有写入使用 `BEGIN IMMEDIATE` 事务 + `RLock` 串行化。
- Idempotency keys 防止崩溃后重放同一转换。
- 线程安全（RLock），但不支持多进程并发写入（由 `service/lock.py` 的 kernel `flock` 保证单进程）。

## 8. 故障恢复

| 故障类型 | 恢复机制 |
|---------|---------|
| kill -9 进程 | 启动时 `recover_storage()` 恢复：截断尾部 frame、完成未完成的 seal、协调 manifest |
| 核心 market 异常退出 | `MarketCollectorSupervisor` 设置子 stop 事件，`CoreMarketTerminalFailure` → launchd 重启 |
| side-data 任务失败 | `SideDataSupervisor` 独立重启，不设置核心 stop 事件 |
| 外置 volume 消失 | `ArchiveManager` 记录 `DISAPPEARED_DURING_COPY`，保留内部源 |
| 磁盘空间耗尽 | `DiskEmergencyCoordinator`：WARNING → CRITICAL → EMERGENCY → hard reserve seal+stop |
| 网络断开 | WebSocket 自动重连 + snapshot resync |
| 进程/操作系统崩溃或掉电 | Catalog 的 WAL + `synchronous=FULL` 用于事务一致性和持久性；真正的 SQLite 文件损坏不宣称可自动恢复 |

## 9. 顶级 Python 包职责

| 包 | 负责 | 不负责 |
|---|------|--------|
| `binance.spot` | Spot 公开 schema/传输/REST 快照 | 账户、USD-M 策略 |
| `binance.usdm` | USD-M 公开 schema/传输/REST 快照 | 账户、Spot 策略 |
| `collector` | 连接/会话生命周期、接收时间戳、有界移交 | 压缩、Parquet、因子 |
| `spool` | Frame 追加、旋转、fsync、seal、崩溃恢复 | 外部挂载逻辑 |
| `storage` | 路径、Catalog、manifest、持久状态转换 | 市场策略语义 |
| `orderbook` | 官方序列验证、重建、gap/resync 证据 | 执行/队列填充 |
| `archive` | 最旧密封 chunk 复制/验证/提交/删除交易 | 注册文件夹之外的写入 |
| `storage.macos` | Disk Arbitration 观察、UUID 解析、探针、弹出 | 格式化/修复/root 守护进程 |
| `normalize` | 版本化 schema、确定性去重/分区、谱系 | Raw 的变异 |
| `replay` | 确定性事件时钟、寻道、gap 策略 | 策略/回测行为 |
| `metrics` | 计数器、延迟/运行时采样、每日 UTC 报告 | SQLite 中的 market-event 语料库 |
| `supervisor` | Collector readiness、blue/green 交接、紧急停止 | 隐藏 gap 或耦合市场 |
| `cli` | 本地控制/状态/报告/存储命令 | GUI、交易界面 |
| `backfill` | 官方 data.binance.vision 历史导入 | 实时数据 |
| `service` | launchd、进程锁、电源生命周期、运行时状态 | root 守护进程 |
| `domain` | EventEnvelope 数据模型 | 业务逻辑 |

## 10. 新开发者从哪里开始阅读

1. `AGENTS.md` — 项目身份和代理合约
2. `docs/architecture.md` — 架构概览
3. `docs/data_contract.md` — EventEnvelope v1、Raw chunk v1、normalized-dataset.v1 合约
4. `docs/storage_contract.md` — 存储不变式
5. `src/binance_market_data_recorder/domain/event.py` — EventEnvelope 数据模型
6. `src/binance_market_data_recorder/spool/format.py` — Raw chunk 字节格式
7. `src/binance_market_data_recorder/collector/spot.py` — 完整 Spot Collector 生命周期
8. `src/binance_market_data_recorder/orderbook/reconstructor.py` — 序列桥接和 gap 检测
9. `src/binance_market_data_recorder/storage/catalog.py` — 所有状态转换的协调点
10. `src/binance_market_data_recorder/normalize/pipeline.py` — Raw 到 Parquet 管道
11. `src/binance_market_data_recorder/archive/manager.py` — 外部归档交易

## 11. 修改关键模块时的必要测试

| 修改范围 | 必须运行的测试 |
|---------|-------------|
| spool/format.py, spool/seal.py, spool/writer.py | `python3.12 -m pytest tests/unit/test_raw_chunk_format.py` + `test_raw_chunk_golden.py` + `go run tools/verify_raw_chunk_golden.go` |
| orderbook/ | `python3.12 -m pytest tests/unit/test_orderbook_model.py tests/unit/test_orderbook_parser.py tests/unit/test_orderbook_reconstructor.py tests/fault_injection/test_orderbook_sequences.py` |
| collector/ | `python3.12 -m pytest tests/unit/test_collector_readiness.py tests/unit/test_market_supervisor.py tests/unit/test_m19_reliability.py` |
| storage/catalog.py | `python3.12 -m pytest tests/integration/test_catalog_and_seal.py tests/integration/test_archive_transaction.py` |
| archive/ | `python3.12 -m pytest tests/integration/test_archive_transaction.py tests/fault_injection/test_archive_kill9.py` |
| normalize/ | `python3.12 -m pytest tests/unit/test_normalized_parser.py tests/unit/test_m19_normalized_parser.py tests/integration/test_normalization_pipeline.py tests/integration/test_m19_normalization_pipeline.py` |
| backfill/ | `python3.12 -m pytest tests/unit/test_backfill.py` |
| 任何全局修改 | `python3.12 -m pytest -q`, `python3.12 -m ruff check .`, `python3.12 -m mypy`, `python3.12 tests/verify_m0_contracts.py`, `go run tools/verify_raw_chunk_golden.go` |

## 12. 当前仍 Open 的限制

- **系统尚未 production-ready 或 trading-ready**。本项目为独立数据录制器，不提供账户、交易或投资功能。
- **R-034（Open）**：官方 Global Spot bootstrap 文辞与 toolbox 观察边界冲突。代码使用 `lastUpdateId + 1`。
- **R-035（Open）**：72h/168h 长期运行验收尚未执行。
- **R-036（Open）**：USD-M 5 分钟统计在录制器离线期间可能错过，超出保留窗口即不可恢复。
- 无 Historical L2：data.binance.vision 不提供深度数据。
- 无 Live raw trades/klines 流。
- 仅支持 BTCUSDT。
- macOS Apple Silicon 为唯一认证平台；Ubuntu 尚待 M20 适配和长期测试。
- Live 和 Historical 数据集从不自动混合。

## 13. Durable Cursor 与实际持久状态

- **Side-data Cursor**：每种 5 分钟统计独立记录
  `last_persisted_period_timestamp`；Raw 完成排空和 fsync 后才推进，
  空响应不推进。
- **Raw**：使用 `ChunkState`、manifest 和 Catalog 状态转换协调恢复，不使用 Cursor。
- **Archive**：使用 `archive_transactions`、`ArchiveState` 和 `ChunkState`，
  不使用 Cursor。
- **Metrics**：使用稳定 `batch_id` 实现幂等批次提交，不使用 Cursor。

## 14. 关于本文档

本文档是 `docs/repository-code-commentary` 分支纯文档化 Code Pass 的交付物之一。
它不创建 API、GUI、新功能或配置更改。
本文中的所有事实均可从源代码、ADR 或测试中验证。
