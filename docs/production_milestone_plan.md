# Binance Market Data Recorder — 生产上线里程碑计划（M1–M4 详尽执行版）

> **状态声明（2026-11-02）**：原 2026-10-26 至 2026-11-01 的 `P0–P6` 里程碑计划已**正式废除（Abolished & Superseded）**，由本文件定义的 **`M1` → `M2` → `M3` → `M4`** 计划全面替代。
>
> 本计划综合了 Argon、Claude Opus 5.5 与 ChatGPT Astra 对代码库（`main` `f109ffc`）的交叉核查结论，并锁定了 Owner 拍板的全部数据范围与可靠性决策。本阶段**不进行跨语言重写**，全部工作在 Python 3.12 下完成。每个 Milestone 与子任务均按“目标、涉及代码位置、现状根因、具体改法、异常/边界处理、独立验收标准”编写，团队可按章独立推进。

---

## 一、 总体工程原则与已锁定的 Owner 决策

### 1.1 保留的基线成果（P0–P3 及 P4 准备实测）
以下已验收能力与契约作为本计划的起点保留，不推翻重写：
- **P0 (`113cdb3` / `43aab9f`)**：完成现货（Spot）、U 本位（USD-M）永续与交割、币本位（COIN-M）永续与交割共 53 个公开数据源盘点（[`docs/milestones/P0/coverage.md`](milestones/P0/coverage.md)）。
- **P1 (`b292d0d`, [`ADR-0052`](adr/0052-bounded-raw-batches-and-sequence-only-capture-health.md))**：采集热路径剥离全量 L2 订单簿重建，改用序列号连续性检查（Spot `U/u`、期货 `U/u/pu`）、长生命周期单接收协程与 `os.writev` 批量写入。
- **P2 (`1c1785c` / `464ed56`, [`ADR-0053`](adr/0053-single-rest-representation-and-required-information.md), [`ADR-0054`](adr/0054-d06-fixed-cadence-and-daily-trade-supplement.md), [`ADR-0055`](adr/0055-initial-production-dated-premium-exception.md))**：多币种产品动态发现、单份主 REST 响应存储、D06 `historicalTrades` 实测分档轮询（`2s` USDT / `3s` USDC / `30s` COIN-M）+ 官方日度成交文件独立补充导入、持久化 `await` 前同步触发 `418/429` `Retry-After` 冷却门、以及首版批准暂缺 U/C 交割合约 `premiumIndexKlines`（`UNKNOWN` 适用性披露）。
- **P3 (`30e8a4a` / `30ebbd6`, [`ADR-0051`](adr/0051-production-continuity-and-incremental-verification.md), [`ADR-0056`](adr/0056-source-recovery-and-bounded-background-verification.md))**：逐源可用性区间追踪、时钟回拨/跳变不确定窗口保护、以及新封存分片增量校验协议（`raw-independent.v2`）。
- **ADR-0057 ([`ADR-0057`](adr/0057-initial-production-dated-adl-exception.md))**：首版批准暂缺 USD-M 交割合约公共 `adlQuantile`（实测返回 HTTP `400 / -1121`），永续合约 ADL 正常保留。

### 1.2 已锁定的六项 Owner 关键决策（团队必须严格执行）

| 编号 | 决策项 | 锁定规则与合同边界 |
|---|---|---|
| **D1** | **WebSocket K 线形成过程过滤与漏收补齐（新立 `ADR-0058`）** | **只持久化收盘 K 线（`k.x == True`）+ 断线错过收盘时自动通过 REST 补齐**：<br>1. 收到合法未收盘 K 线帧（`k.x == False`）时：正常刷新该源的内存存活时间戳（`AvailabilityJournal` 视为有效观测），并累加该源的 `forming_filtered_count` 计数，但**不写入 Raw 分片**（为 `1s`/`1m` 保留可选配置开关，生产默认关闭未收盘写入）；<br>2. 收到畸形帧（缺少必填字段、JSON 解析失败、币种不匹配）时：**严禁当作未收盘帧静默丢弃**，必须按现有规则打上 `malformed` 标记落盘并计入故障；<br>3. 若因断线重连或序列跳变错过了某个周期应有的收盘时间点（Expected Close Boundary），后台自动通过共享限流器 `shared_request_budget()` 发起有界 REST `/klines` 补采，将补回的收盘 K 线打上 `capture_flags = ("closed_bar_backfill",)` 及**真实 REST 接收时间**写入 Raw，并在文档中明确披露“不再保存形成中的中间推送轨迹”。 |
| **D2** | **`S12`（`@+08:00` 东八区 K 线）10 个日内周期直接去重** | **直接停采数学上 100% 重合的 10 个周期，免除形式主义对照脚本**：<br>能整除 8 小时的 10 个周期（`1s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 8h`）在 UTC 与 `+08:00` 下开收盘边界完全一致，直接在 `S12` 默认配置中停采这 10 个周期（仅保留边界不同的 `6h, 12h, 1d, 3d, 1w, 1M`），并在 [`docs/data_coverage.md`](data_coverage.md) 标注由 `S11` 覆盖。 |
| **D3** | **SQLite `Catalog` 落盘强度与恢复（新立 `ADR-0059`）** | **日常事务采用 `WAL + PRAGMA synchronous = NORMAL`**：<br>1. 日常分片登记、状态更新、可用性水位提交使用 `PRAGMA synchronous = NORMAL`，消除每次小事务的磁盘 `fsync` 阻塞；<br>2. 仅在归档跨盘删除本地源文件（`LOCAL_DELETED`）前执行 `FULL` 同步；启动恢复（`spool/recovery.py`）自动将磁盘上已封存的 `.zst` + `.json` 对账补齐至 `Catalog`。 |
| **D4** | **Raw 分片二进制格式兼容性** | **100% 保持 Raw v1（`ADR-0010`）磁盘字节格式不变**，确保既有回放器、Parquet 归一化器及 `tools/verify_raw_chunk_golden.go` 零改动兼容。 |
| **D5** | **外围与历史功能处置（macOS 外接盘、远端归档、旧版验收）** | **功能保留但冻结，移出采集进程启动加载路径**：通过 `cli.py` 按子命令延迟导入（Lazy Import），确保采集守护进程启动时不加载 `pyarrow` 或上述外围模块。 |
| **D6** | **VPS 实测验收方式（`M4`）** | **直接以 `[recorder.coverage].underlying_assets = ["BTC", "ETH"]` 进行单次连续运行**，依次在 **`2h`、`12h`、`24h`** 三个时间点做不中断检查；任一检查点硬指标不达标，修复后从 `0h` 重新计时。 |
| **D7** | **彻底移除学院派过度防御（封存回读、内部字典防篡改拷贝、启动空文件）** | 1. **取消实时封存后的全文件解压回读（Readback）与后台重验**：写入时内存中流式算好 `CRC32C` + `SHA-256` 并 `fsync` 即完成封存，不再刚写完又从磁盘读回来解压一遍（全文件读取仅用于 `.partial` 崩溃恢复、跨盘归档拷贝和手动执行 `verify-raw` CLI）；<br>2. **取消进程内防篡改拷贝**：移除 `RawChunkWriter` 中针对“内部代码篡改字典”的 `_ImmutableEnvelopeSnapshot` 和多余字典拷贝；<br>3. **取消启动空文件与额外边界文件**：移除启动时 80+ 个 0 记录 `.zst` 分片，仅在 `Catalog` 和首分片 manifest 标记启动边界。 |

---

## 二、 代码事实校准表（纠正前期讨论中的偏差）

团队在实施 `M1–M4` 时，必须以下表核实过的代码事实为准，不得依赖未经证实的推算：

| 前期讨论中的不准确说法 | 核对当前代码（`main` `f109ffc`）后的准确事实 | 对本计划的指导意义 |
|---|---|---|
| “`logical_envelope()` 又做了一次 `json.loads`” | [`public_websocket.py:60`](../src/binance_market_data_recorder/binance/public_websocket.py#L60) 的 `logical_envelope()` 只读取 `envelope.source_sequence.get("sourceStream")` 并调用 `model_copy(update={"stream": stream})`，**没有**二次 JSON 解析。 | `M1.4` 应聚焦于减少 `model_copy` 和构造不可变内部记录，而非在 `logical_envelope` 里找 JSON 解析。 |
| “每次 REST 持久化都重新 `json.loads` 和 `json.dumps`” | [`coverage.py:735–756`](../src/binance_market_data_recorder/collector/coverage.py#L735-L756) 中，只有 `D16`（注入 `evaluated_period`）和 `418/429` 限流分支对 `raw_payload` 做了二次 JSON 解析；常规 REST 的主要额外开销是**每次轮询分两次进出线程池**（一次 `capture_public_response`，一次 `persist_rest`）。 | `M1.4` 只需把 `evaluated_period` 前置传入 `capture_public_response`、在 `RestCaptureResult` 暴露响应头，并合并/批量化 REST 写盘线程切换。 |
| “P4 Run C 每轮只刷 64 行；BTC 每分钟固定产生 80 个分片（11.5 万/天）” | 1. Run C（`8bdae20`）每轮刷 `128` 行；当前 `main`（`2a4fefd`，[`coverage.py:938`](../src/binance_market_data_recorder/collector/coverage.py#L938)）已改为每轮最多 `32` 批（`4,096` 行）或 `100ms`。<br>2. Run C 实测 55 分钟封存了 `874` 个分片（平均 **`15.9` 个/分钟，约 `2.3 万` 个/天**），因为空闲流在封存后只有收到新消息才会创建下一个 `.partial` 文件。 | `M1.1` 与 `M1.2` 的收益必须基于实测基线（如 ~15.9 个/分钟）做对照，从源头减少健康 checkpoint 产生速率与轮换频率。 |
| “`EventEnvelope` 已经是 `frozen=True`，可以直接删掉 `_ImmutableEnvelopeSnapshot`” | Pydantic 的 `frozen=True` 是浅冻结，`EventEnvelope.source_sequence` 仍是可变 `dict`。但由于采集全链路都在同一受控进程内，没有外部插件半途篡改字典，专门为此做防御拷贝属于过度防御。 | `M1.4` 直接移除 `_ImmutableEnvelopeSnapshot` 与多余字典拷贝，直接编码 CBOR 与累加统计。 |
| “刚写完分片 `fsync` 后还要再读回解压校验 2 遍” | 当前封存时 [`readback.py:89`](../src/binance_market_data_recorder/spool/readback.py#L89) 全文件解压读 1 遍，后台 [`verification.py:61`](../src/binance_market_data_recorder/service/verification.py#L61) 又全文件解压读 1 遍。写入时内存已算好 `CRC32C` 和 `SHA-256` 并 `fsync`，实时采集路径上立刻重读属于学院派过度防御。 | `M2.1` **直接取消实时封存后的全文件解压回读与后台重验**（仅在 `.partial` 崩溃恢复、跨盘归档拷贝和手动 `verify-raw` CLI 时执行全文件读取校验）。 |
| “旧单品种采集器是死代码，可直接删除” | [`service/runtime.py:963`](../src/binance_market_data_recorder/service/runtime.py#L963) 在 `self.config.coverage is None` 时仍会走旧的 `MarketCollectorSupervisor` 分支，且大量旧单元/集成测试依赖该入口。 | `M2.3` 先将旧配置（`spot_symbols`/`usdm_symbols`）无缝映射到统一采集实现并验证测试等价，再下线旧类。 |

---

## 三、 各 Milestone 详尽任务说明与验收标准

---

### Milestone 1（M1）：建立实测基线与解决核心性能/资源放大瓶颈

- **阶段目标**：在不破坏任何数据完整性契约的前提下，先固化修改前的性能基线，随后解决“60 秒高频切文件”、“SQLite 可用性日志写放大”、“K 线重复订阅与未收盘高频写盘”、“一次性种子任务空转”及“热路径重复对象拷贝与线程切换”五大核心问题。
- **前置条件**：当前 `main` 分支（`f109ffc`）工作区干净，现有测试套件通过。

---

#### 任务 M1.0：建立可重放的多流固定输入基线（Pre-M1 Baseline）
- **涉及文件**：
  - 新增基准脚本与夹具生成工具：`tools/benchmark_m1_m3_pipeline.py`
  - 参考现有实现：[`docs/research/2026-10-26/preplan_baseline.py`](research/2026-10-26/preplan_baseline.py)、[`docs/milestones/P1/compare.py`](milestones/P1/compare.py)
- **现状问题**：
  - 仓库现有的 `preplan_baseline.py` 仅覆盖旧版单品种 Spot/USD-M 核心流（94% 为 `bookTicker`），不包含 `SelectedAssetCapture` 的组合流、K 线流、多周期 REST 轮询及 `AvailabilityJournal` 刷新；若不先测当前 `main` 基线，后续优化无法区分“代码提速”与“少采数据带来的减负”。
- **具体实施内容**：
  1. 编写确定性多流基准脚本 `tools/benchmark_m1_m3_pipeline.py`，支持注入固定随机种子生成（或从样本提取）的 `BTC + ETH` 全家族消息流（包含 `bookTicker`、`aggTrade`、`diff_depth`、`rpi_depth`、多周期 `kline` 的 `x=False`/`x=True` 序列、以及模拟的 REST 响应和定时监控循环）。
  2. 在修改任何业务代码前，在当前 `main` 上运行该基准并保存 JSON 基线报告，记录以下 8 项核心指标：
     - ① 每帧平均 CPU 耗时（`cpu_us_per_frame`）与壁钟吞吐；
     - ② 每小时生成的封存 `.zst` 文件数与 `.json` manifest 数；
     - ③ SQLite `Catalog` 每分钟写事务数、总耗时及 WAL 增长字节数；
     - ④ `AvailabilityJournal.pending` 队列峰值长度与刷盘耗时；
     - ⑤ Spool 队列最老事件等待时间（`oldest_queue_wait_ms` p50/p99）；
     - ⑥ 写入批次耗时与 `fsync` / 封存（seal）耗时分布（p50/p99/max）；
     - ⑦ 进程常驻内存峰值（` peak_rss_bytes`）；
     - ⑧ 产出的 Raw 分片计数、帧计数与摘要清单。
- **独立验收标准**：
  - 脚本在离线沙箱环境可重复运行，连续两次运行的帧计数与逻辑摘要 100% 一致，产出修改前的基准文件 `baseline-main.json`。

- **完成状态（2026-11-02 独立复核后接续完成）**：
  - 原 `884dcdd` 交付的基准框架可运行，但原验收存在输入覆盖、内容摘要和指标口径缺口，不能按原报告直接签收；问题及修复见 [`M1.0 复核记录`](benchmarks/m1.0-review-20261102.md)。
  - 已修正 [`tools/benchmark_m1_m3_pipeline.py`](../tools/benchmark_m1_m3_pipeline.py)，继续使用真实 `SelectedAssetCapture`、`StreamSpool`、Writer、Catalog；**未修改生产业务代码**，`src/` 与 `f109ffc` 一致。
  - 固定输入包含 **50 个 BTC/ETH 产品、85 个 WS 组（含共享连续合约/指数/全局源）**，选用 `1m/5m` 两周期并包含形成/收盘序列；REST 按固定源配额执行 **206 次**请求，包含全部产品深度快照、各市场/stream 代表源及周期源重复轮询。离线配额调度采用压缩时间间隔，不代表真实生产请求频率。
  - 默认观测至少 **12 秒**，覆盖每秒可用性扫描和一次 10 秒健康 checkpoint；保留基线 `60s / 128 MiB`、`WAL + FULL`，仅将首次轮换期限固定为 writer 打开后一个完整周期，避免宿主机相位碰撞。此上限是**当前代码基线**，不是对后续 M1.1 配置的修改。
  - 连续两次运行均持久化 **13,966 帧**；输入 SHA-256 与**实际解码 Raw 内容摘要**均一致。停机条件检查真实 WS `received == persisted == expected` 和逐源 REST 完成数；计时结束后离线检查每份 Raw、manifest 及完整输入计数，不把校验耗时算入采集 CPU。
  - 新基线：[`docs/benchmarks/baseline-main.json`](benchmarks/baseline-main.json)，根目录 [`baseline-main.json`](../baseline-main.json) 是相同副本。完整八项指标、逐分片哈希、环境/脚本/源码身份、两次复现结果及测量边界均保存在 JSON 中。
  - 原两份数值不一致的 v1 报告已分别保留为 [`m1.0-v1-docs.json`](benchmarks/archive/m1.0-v1-docs.json) 和 [`m1.0-v1-root.json`](benchmarks/archive/m1.0-v1-root.json)，仅供历史追溯。
  - **口径限制**：短突发包括启动/停机封存，不能外推稳态 files/hour；小时速率仅为本次生命周期文件数的归一化观测值，明确不代表稳态轮换率。SQLite 写语句与已提交事务分别计数；队列等待、接收到写完的延迟、append/write、Raw fsync 与真实 seal 各自计量。短测不能证明长期积压或优化收益；M1.1 的轮换收益仍需跨轮换输入测试，M3.1 的 45–60 分钟三方验收保持不变。
  - 验证命令与结果见复核记录；新增回归覆盖固定种子/共享流/多周期有效负载、SQL 事务口径、离线双次回放及 Raw 内容完整性。

---

#### 任务 M1.1：Spool 分片轮换周期调优（`60s` → `600s–900s` + `128 MiB` 上限）
- **涉及文件与符号**：
  - [`src/binance_market_data_recorder/spool/stream.py`](../src/binance_market_data_recorder/spool/stream.py#L41-L48)（`RotationPolicy`）
  - [`src/binance_market_data_recorder/config.py`](../src/binance_market_data_recorder/config.py)（`RecorderConfig.rotation_seconds`、`rotation_max_bytes`）
  - [`src/binance_market_data_recorder/collector/coverage.py`](../src/binance_market_data_recorder/collector/coverage.py#L452-L517)（`StreamSpool` 实例化与错峰偏移）
  - [`src/binance_market_data_recorder/storage/capacity.py`](../src/binance_market_data_recorder/storage/capacity.py) 及 [`storage/emergency.py`](../src/binance_market_data_recorder/storage/emergency.py)（活跃区与紧急预留水位核算）
- **现状问题**：
  - 当前 `RotationPolicy.seconds` 默认为 `60.0` 秒。只要某个流在 1 分钟内收到至少 1 条消息，60 秒到点就会触发一次完整的封存生命周期（关闭 `.partial` → `fsync` → Zstd 压缩 → 独立回读 → 写 `.json` manifest → 目录 `fsync` → SQLite `Catalog` 状态流转 → 入队后台校验 → 入队归档拷贝）。P4 Run C 单 BTC 实测每分钟封存约 15.9 个文件（一天约 2.3 万个 `.zst` + 2.3 万个 `.json`），双币种 `BTC + ETH` 将达到每天 4.5 万+ 个小文件。
- **具体实施内容**：
  1. 将默认生产轮换时间 `rotation_seconds` 从 `60.0` 调整为 **`900.0` 秒（15 分钟，支持在 TOML 中配置为 `600.0`–`900.0`）**，同时保留单分片未压缩大小上限 `max_bytes = 128 * 1024 * 1024`（`128 MiB`）——高流量流（如 `BTCUSDT` 现货 `bookTicker`/`diff_depth`）若在 15 分钟内先达到 `128 MiB` 则按大小提前轮换，中低流量流按 15 分钟轮换。
  2. **保留并适配流间错峰（Phase Staggering）**：确保不同 `StreamSpool` 的初始轮换相位偏移（`rotation_phase_offset_ns`）按新的 `rotation_seconds` 均匀分布在 `[0, rotation_seconds)` 区间内，避免所有活跃流在同一秒同时做 Zstd 压缩。
  3. **保持独立的数据刷盘周期不变**：活跃 `.partial` 文件的周期性 `fsync`（`durability_interval_seconds`，默认 1 秒）保持独立运行，确保拉长封存周期不会增加进程崩溃时的未刷盘数据窗口。
  4. **核算活跃区空间与恢复耗时**：验证当多个流同时持有较大 `.partial` 文件（上限 `128 MiB`）且进行 Zstd 临时文件压缩时，所需的峰值临时空间仍在 `10 GiB` 紧急停机预留水位（`ADR-0016`）的安全边界内；测试对 `128 MiB` `.partial` 文件执行启动截断恢复（`scan_chunk`）的耗时。
  5. 现有显式测试 60 秒轮换行为的单元测试通过配置注入 `rotation_seconds=60.0`，保持测试语义不变。
- **独立验收标准**：
  - 在 `M1.0` 固定输入基准测试中，封存文件数量、封存触发的 SQLite 事务数及归档事务数相比 60 秒基线显著下降（中低频流下降约 90%+）；
  - 单次封存（最大 `128 MiB`）在 drain 线程中的耗时不导致 WebSocket 有界接收队列溢出（`IngressQueueFull == 0`）；
  - 模拟 `SIGKILL` 截断恢复测试全部通过。

- **完成状态（2026-11-02 独立复核并补齐验收后完成）**：
  - `5f32074` 已正确将 `RecorderConfig`、`load_config`、`RotationPolicy`、旧 Spot/USD-M settings 的默认值改为 **900s / 128 MiB**；原 identity 哈希错峰随周期缩放，**无需新增生产调度逻辑**。TOML `600.0` / `900.0` 和空闲流独立 `1s fsync` 已补测。
  - **原验收缺口**：93.3% 降幅原来只是算式；队列测试没有实际封存 128 MiB 或覆盖封存期间接收；空间核算的 30 MiB 压缩产物、5 流并发假设不足。原“数学与逻辑全覆盖”声明撤回，以本次证据替代。详见 [`M1.1 复核记录`](benchmarks/m1.1-review-20261102.md)。
  - **真实轮换对照**：新增 [`tools/benchmark_m1_1_rotation.py`](../tools/benchmark_m1_1_rotation.py)，复用 M1.0 的 13,966 条实际 WS/REST 输入并明确重复稀疏流观测；143 条物理流、两组各解码 **39,540 帧**，内容摘要完全相同。在 **2,700 秒逻辑 Writer 时间**中真实执行编码、fsync、压缩、Catalog 和归档预留：含停机的 `.zst` / manifest、SEALING / SEALED 提交、归档 COPYING 事务均由 **6,533 → 570（下降 91.28%）**，定时封存为 **6,435 → 429（下降 93.33%）**。报告：[`m1.1-rotation-20261102.json`](benchmarks/m1.1-rotation-20261102.json)。
  - **封存期间并发接收**：本机 **134,274,303 bytes** 不可压缩 Raw 实际大小轮换封存 **221.74ms**；mock socket 通过真实 collector `_receive_once`、有界 receipt queue、`_writer_loop` 持续交接，期间接收 **632 帧**，队列高水位 **632 / 8,192**，`IngressQueueFull = 0`、等待计数 `0`，最终全部落盘。
  - **恢复**：**135,194,321 bytes** `.partial` 扫描 **117.52ms**、截断恢复 **239.99ms**；新增实际子进程 `SIGKILL` 大文件半帧尾部恢复，保留全部 129 条有效帧、Catalog 为 `RECOVERED`。专项 13 项通过，原有故障恢复套件通过。数据与边界：[`m1.1-seal-recovery-20261102.json`](benchmarks/m1.1-seal-recovery-20261102.json)。
  - **空间核算纠正**：活跃 `.partial` 已计入磁盘已用量；紧急预留覆盖新增压缩临时文件。现有 Python 3.12 默认 executor 至多 32 workers，每份按 `(128 MiB + 16 MiB 最大帧 + 12 bytes 前缀) × 2` 预算，无需依赖压缩收益，约 **9 GiB < 10 GiB**。哈希错峰不承诺零碰撞；128 MiB 为轮换阈值，允许末条完整帧越过阈值。本核算针对默认最大帧与线程池，参数扩大时重新核算。
  - **验收边界**：跨轮换报告为测试时钟推进，非 45 分钟墙钟持续采集；启动空 marker 不计入轮换重放，归档只测真实预留次数，未测外盘拷贝/删除性能。封存/恢复时延与队列为本机离线负载结果，**不替代 M3.1 的 45–60 分钟三方整机对照或 M4 VPS 验收**。
  - **回归**：全量离线 `pytest -q` **2,464 passed / 24 skipped / 5 deselected**，`ruff`、`mypy`、M0 契约、Go Raw v1 golden、`git diff --check` 通过；最终 collector 专项更新后另行重跑通过。本次未进一步修改生产 `src/`，未提前实施 M1.2–M2。

---

#### 任务 M1.2：SQLite `Catalog` 落盘模式调整与 `AvailabilityJournal` 状态合并减负（含 `ADR-0059`）
- **涉及文件与符号**：
  - [`src/binance_market_data_recorder/collector/availability.py`](../src/binance_market_data_recorder/collector/availability.py#L56-L176)（`AvailabilityJournal`、`SourceAvailability`）
  - [`src/binance_market_data_recorder/collector/coverage.py`](../src/binance_market_data_recorder/collector/coverage.py#L938-L1030)（`_flush_availability`、`_observe_availability`）
  - [`src/binance_market_data_recorder/storage/catalog.py`](../src/binance_market_data_recorder/storage/catalog.py#L120-L210)（SQLite 连接 PRAGMA、事务锁、`persist_source_availability`、归档删除授权）
  - [`src/binance_market_data_recorder/spool/recovery.py`](../src/binance_market_data_recorder/spool/recovery.py)（启动时磁盘与 `Catalog` 状态对账恢复）
  - 新增架构决策记录：`docs/adr/0059-catalog-wal-normal-sync-and-watermark-checkpoint.md`
- **现状问题**：
  1. `AvailabilityJournal.valid()` 对每个健康源每满 10 秒就往 `self.pending` 队列追加一条完整快照字典（BTC 单币 2,233 个源 = 每 10 秒 2,233 条记录；`BTC + ETH` 约 4,660 个源 = 每 10 秒 4,660 条记录）。若刷盘稍有延迟，同一个 `source_id` 会在 `pending` 队列中堆积多条旧的健康 checkpoint。
  2. `Catalog` 对所有事务统一使用 `PRAGMA synchronous = FULL`，每次提交都触发底层 WAL `fsync`，在共享云盘（GreenCloud VPS）I/O 抖动时会放大锁等待时间。
- **具体实施内容**：
  1. **第一步：`AvailabilityJournal` 待写队列去重合并（Coalescing）与共同检查点水位（Shared Watermark）**：
     - **队列内合并**：在 `AvailabilityJournal` 中，将“状态跳变事件”（`unavailable` 开始、`recovery` 闭合、`failure` 计数更新、`lifecycle` 变更）与“周期性健康 checkpoint”区分管理。状态跳变事件严格按发生顺序保留在有序队列中，绝不丢弃或重排；而对于健康源的周期性存活刷新，同一 `source_id` 在尚未刷盘前只保留最新一条水位状态，直接消除队列长度随时间线性积压的放大效应。
     - **共同检查点水位（Monitor Watermark）**：当一轮 `_observe_availability(now)` 完成对当前冻结源集合（绑定 `source_selection_id`）的全部检查、且时间 $T$ 之前的所有状态跳变记录均已在同一或前置 SQLite 事务中持久化后，更新单行共同检查点水位记录 `source_availability_watermark(selection_id, evaluated_through_utc_ns = T)`，替代每 10 秒对数千个未发生跳变的健康源逐行执行 `INSERT ... ON CONFLICT DO UPDATE`。
     - **重启恢复语义**：服务重启加载 `Catalog` 时，任意正常源的已证实健康下界取 $\max(\text{该源最后一条持久化记录的 } observed\_at, \text{所属集合的 } evaluated\_through\_utc\_ns)$；从该时间点到新进程恢复观测之间的窗口按既有规则诚实记为 `unknown`（不超过一个 checkpoint 周期 + 停机时长）。
  2. **第二步：SQLite `WAL + NORMAL` 模式与不可逆删除 `FULL` 屏障（`ADR-0059`）**：
     - 将 `Catalog` 默认连接配置调整为 `PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;`（不捆绑未经单独验证的 `mmap_size` 或 `temp_store` 参数）。
     - **不可逆操作强制同步屏障**：在执行任何会授权或完成删除本地原始分片的操作（例如 `ArchiveManager` 本地源退休置 `LOCAL_DELETED`、远端删除授权提交）时，在事务前后显式执行 `PRAGMA synchronous = FULL` 与 `PRAGMA wal_checkpoint(PASSIVE)`（或文件级 `fsync`），确保删除授权不会因掉电回滚。
     - **掉电 WAL 尾部回滚恢复对账**：在 `spool/recovery.py` 启动恢复中，补充检查：若磁盘上已存在完整且通过校验的封存文件（`.zst` + `.json` manifest），但 SQLite `Catalog` 因模拟掉电回滚仍显示该 `chunk_id` 为 `ACTIVE` 或 `SEALING`（甚至缺少记录），恢复逻辑必须根据磁盘上已验证的 `.zst` + `.json` 自动前滚（Roll-forward）补齐 `Catalog` 的 `SEALED` 状态，而不是将其报错或误判为孤儿损坏文件。
- **独立验收标准**：
  - 在 `BTC + ETH`（~4,660 源）规模下运行，稳态健康期间 `AvailabilityJournal` 每 10 秒产生的 SQLite 写行数从 ~4,660 行降至个位数（水位更新 + 少量真实状态跳变），`pending` 队列稳态积压为 `0`；
  - 新增单元与集成测试：
    - ① 故障跳变与恢复边界在合并后与原实现 100% 精确一致；
    - ② 模拟进程崩溃及 WAL 尾部最后数秒事务回滚（截断未 checkpoint 的最后几帧 WAL 事务）后，启动恢复能正确将磁盘上已封存的 `.zst` 对账恢复为 `SEALED`，且可用性账本将丢失的数秒尾部记为 `unknown`，无静默漏记。
- **完成状态（2026-11-02 独立复核并补齐后完成）**：
  - 原实现 `ecf0302` 已完成 `WAL + NORMAL`、删除路径的 `FULL` 屏障、checkpoint 原地合并、共同水位表及已封存文件恢复前滚；这些机制保留，**没有重写采集架构**。原“严格验收完成”声明存在以下缺口，以本次复核证据替代。完整记录见 [`M1.2 复核记录`](benchmarks/m1.2-review-20261102.md)，规范见 [`ADR-0059`](adr/0059-catalog-wal-normal-sync-and-watermark-checkpoint.md)。
  - **水位顺序与失败重试修正**：原 `_flush_availability()` 在 `32` 批 / `100ms` 上限后仍可能提交水位，越过尚未提交的故障跳变；也会在事务成功前更新内存已提交时间。现仅在相关队列排空后提交水位，并在成功返回后确认其实际时间。水位到期与否都不再触发逐源健康写盘，独立 Journal 的旧式 checkpoint 接口保留。
  - **水位归属修正**：原 `source_availability()` 将某一集合水位套用于全部历史健康源。现每条源状态携带 `selection_id`，水位键绑定资产集合及本次 collector instance；读取时仅应用该记录所属水位。重启读取前一实例各源自己的已提交水位；旧版未绑定记录保守使用自身 `observed_at`。仅推进健康确认时间，不改写 `last_valid_utc_ns`。
  - **封存对账与 `.partial` 清理修正**：原新增清理直接按 Catalog 路径 unlink，未验证重复内容，兜底文件名还误用了带连字符 UUID。现按 Writer 实际的 UUID hex 文件名定位，并复用既有恢复校验与清理函数；无新增实时回读、后台校验或防御拷贝。
  - **删除屏障核验**：本地 `LOCAL_DELETE_PENDING` / `LOCAL_DELETED`、远端授权和远端终态均验证 `FULL → BEGIN → COMMIT → PASSIVE checkpoint → NORMAL`。远端终态保留既有专用事务，原“四项全部走 `_transaction(force_sync=True)`”描述不准确。另修正 `BEGIN` 失败时未还原 `NORMAL` 的边界。
  - **规模与写放大实测**：原测试只有 **200 个模拟源**，未实际覆盖文档声称的 4,660 源。本次专项使用 **4,660 个健康模拟 WS 源 + 3 个目录源**，逐秒观察 10 秒；SQLite trace 实测仅 **1 条 watermark INSERT/UPSERT、0 条逐源写入**，每轮刷新后 pending 为 `0`。健康 checkpoint 写行数相对逐源 4,660 行下降 **99.98%**；启动与真实跳变仍按原事务写入。
  - **真实 WAL 尾部截断恢复**：原测试通过 SQL 回改 `chunks` 和 watermark，并未截断 WAL；保留为状态回退测试。本次新增子进程 `SIGKILL` 后实际截断未 checkpoint 的 WAL 尾部，覆盖 Catalog **缺记录 / ACTIVE / SEALING × 有 / 无重复 `.partial`** 共 6 种组合：真实非空 `.zst` + manifest 自动恢复 `SEALED`，有效帧数保持 `1`，重复 `.partial` 清零，重复恢复幂等；丢失的 10 秒监控尾部及 2 秒停机完整计入 `unknown`。
  - **验证**：专项 [`test_m1_2_catalog_and_availability.py`](../tests/unit/test_m1_2_catalog_and_availability.py) **17 passed**；全量离线回归 **2,481 passed / 24 skipped / 5 deselected**，Ruff、mypy、M0 契约、Go Raw v1 golden、`git diff --check` 均通过；13 条既有 fork 警告及验收口径见复核记录。修正原专项测试遗留的 6 项 mypy 错误。故障跳变顺序、精确 gap 边界、生命周期和时钟回拨沿用并通过现有 P3 测试。
  - **验收边界**：以上为本机离线规模、真实 SQLite 和文件恢复测试；WAL 截断是可复现的掉电尾部丢失模型，**不是物理断电实验，也不是实际 Binance 双币种在线采集**。不替代 M3.1 的 45–60 分钟三方对照或 M4 VPS 实测；本次未实施 M1.3–M2。

---

#### 任务 M1.3：K 线收盘过滤 + 漏收 REST 自动补采、`S12` 直接去重与一次性 Seed 源退役（含 `ADR-0058`）
- **涉及文件与符号**：
  - [`src/binance_market_data_recorder/binance/public_sources.py`](../src/binance_market_data_recorder/binance/public_sources.py#L230-L515)（`S11`, `S12`, `D21`, `D22`, `D23` 及 `*_seed*` 源定义）
  - [`src/binance_market_data_recorder/binance/public_websocket.py`](../src/binance_market_data_recorder/binance/public_websocket.py#L68-L162)（`envelope_for_group` K 线字段提取）
  - [`src/binance_market_data_recorder/collector/coverage.py`](../src/binance_market_data_recorder/collector/coverage.py#L518-L920)（WS 消息分发、REST 轮询调度 `_poll_rest_loop`、可用性扫描 `_observe_availability`）
  - [`docs/data_coverage.md`](data_coverage.md) 及新增 `docs/adr/0058-kline-closed-bar-persistence-and-backfill.md`
- **现状问题**：
  1. **一次性 Seed 源常驻空转**：启动时用于抓取初始快照的 `kline_seed_*`、`kline_seed_*_utc8`、`continuous_seed_*`、`reference_price_seed`（单 BTC 就有 600+ 个，双币种超过 1,200 个）在 `cadence_seconds == 0` 完成唯一一次抓取后，仍留在活跃 `self.sources` 中，每秒被 `_poll_rest_loop` 和 `_observe_availability` 反复遍历。
  2. **`S12`（`@+08:00`）重复订阅**：`S12` 订阅了全部 16 个周期，其中能整除 8 小时的 10 个周期（`1s`–`4h`, `8h`）在数学上与 `S11`（UTC）的开收盘时间边界完全重合。
  3. **未收盘 K 线高频落盘**：交易所对每个 K 线流每 1～2 秒（合约最快 250ms）推送一次 `k.x == False` 的形成中更新，占用了绝大部分 K 线落盘帧数。
- **具体实施内容**：
  1. **一次性 Seed 源完成后退役（Seed Retirement）**：
     - 在 `SelectedAssetCapture` 中将 `self.sources` 拆分为“常驻活跃源（`_active_sources`）”与“已完成一次性种子源（`_completed_seed_sources`）”。
     - 当 `cadence_seconds == 0` 的一次性源成功完成请求并持久化后，将其移入 `_completed_seed_sources`：不再参与每秒的 `_poll_rest_loop` 排序和 `_observe_availability` 扫描，但在生成覆盖率报告与状态快照时仍保留其已完成记录。
  2. **`S12`（`@+08:00`）10 个重合日内周期直接停采（免除形式主义对照脚本）**：
     - 直接在 `public_sources.py` 的 `S12` 定义中默认只启用与 UTC 边界不同的 6 个周期（`6h, 12h, 1d, 3d, 1w, 1M`），并在 `docs/data_coverage.md` 中记录其余 10 个周期由 `S11`（UTC）直接覆盖（双币种直接减少 **600 个数据源**）。
  3. **WebSocket K 线只持久化收盘帧（`k.x == True`）+ 断线漏收自动 REST 补齐（`ADR-0058`）**：
     - 在 `envelope_for_group()` 解析 K 线消息（`data.get("e") == "kline"` 或 `"continuous_kline"`）时，校验 `data["k"]["x"]`（布尔值）与开收盘时间 `t, T`，存入 `source_sequence`（如 `klineClosed=1/0`, `openTime=t`, `closeTime=T`）。若消息格式非法或缺少必填字段，保留 `malformed` 标记并落盘。
     - 在 `SelectedAssetCapture` 的 WebSocket 接收路径上（配置项 `kline_forming_policy = "closed_only"`，默认开启）：
       - 收到合法 `k.x == False` 时：更新该 K 线源的内存存活时间，并递增 `forming_filtered_count` 指标，直接返回，不送入 `StreamSpool`。
       - 收到合法 `k.x == True` 时：正常写入 `StreamSpool`，并记录 `last_closed_open_time_ms = t`。
     - **漏收收盘 bar 检测与 REST 自动补采**：
       - 对每个启用的 K 线源维护 `last_closed_open_time_ms`。当新收到一根收盘 bar 的 `t` 大于上一根 `last_closed_open_time_ms + interval_ms`，或断线重连恢复后跨过预期收盘边界时，自动通过 `shared_request_budget()` 发起低优先级 REST 补采请求，拉取缺失区间的已收盘 K 线，打上 `capture_flags = ("closed_bar_backfill",)` 与真实 REST 接收时间写入对应的 Raw 流。
- **独立验收标准**：
  - 单元测试覆盖：① 正常 `k.x == False` 过滤计数且保持源 `VALID` 不超时；② 模拟 WebSocket 在整分收盘前断线、收盘后重连，系统自动通过 REST 补齐缺失的收盘 K 线；
  - 一次性 Seed 源在完成后不再出现在每秒调度循环中。

- **完成状态（2026-11-02 独立复核后补齐）**：
  - 原工作区已具备收盘过滤、S12 默认六周期与 Seed 退役框架，但补采仅一页、可能请求形成中 bar，失败或并发新缺口会被遗忘；UTC+8 月线、连续交割合约路由和 Seed 闭合水位也存在错误。新增异步测试依赖项目未安装的 pytest 插件，原交付未通过现有检查。
  - 接续修正采用现有 REST 调度器与共享请求预算：常规采集优先，每次最多 100 bar（长周期限制在 200 天内；月线每页最多五根），分页推进，空响应/失败五秒后重试；418/429 复用现有冷却门，失败响应也落 Raw，写盘异常向上抛出。待补区间可在状态中查看，WS 后续收盘不会抹掉尚未补齐的缺口。
  - 形成帧刷新内存有效观测和过滤计数；畸形帧继续带 `malformed` 落盘。修正 UTC+8 月线跨月/闰年、连续交割合约 `contractType`、仅用已收盘 Seed 初始化水位，以及 RPI 一次性快照按需重新激活。使用标准库 `ChainMap` 合并状态视图，移除自定义字典包装。
  - S12 默认保留六周期，已在 data coverage 与 ADR-0058 披露形成轨迹不再保存；基准工具显式选择完整旧范围以保持后续 M3 三方对照能力。
  - 验证与边界见 [`M1.3 复核记录`](benchmarks/m1.3-review-20261102.md)。本次不提前实施 M1.4 或 M2；补采游标仅在本进程内存中保存，不新增跨重启历史补采系统。

---

#### 任务 M1.4：移除热路径防御性拷贝与合并 REST 持久化线程交接
- **涉及文件与符号**：
  - [`src/binance_market_data_recorder/domain/event.py`](../src/binance_market_data_recorder/domain/event.py#L17-L44)（`EventEnvelope`）
  - [`src/binance_market_data_recorder/spool/writer.py`](../src/binance_market_data_recorder/spool/writer.py#L86-L135) 及 [`L271-L320`](../src/binance_market_data_recorder/spool/writer.py#L271-L320)（`_ImmutableEnvelopeSnapshot`、`_prepare_frame`、`append_many`）
  - [`src/binance_market_data_recorder/binance/public_rest.py`](../src/binance_market_data_recorder/binance/public_rest.py) 及 [`collector/coverage.py:725-785`](../src/binance_market_data_recorder/collector/coverage.py#L725-L785)（`capture_public_response`、`poll_once`、`persist_rest`）
- **现状问题**：
  1. 在 WebSocket 热路径上，每条消息构造 `EventEnvelope` 后，经过 `logical_envelope()`、`observe()` 等多次 `.model_copy()`，进入 `RawChunkWriter._prepare_frame()` 时又构造一次防内部篡改的 `_ImmutableEnvelopeSnapshot`（复制 `dict`、构造 `MappingProxyType`），紧接着 `.canonical_mapping()` 又新建一个 17 键 `dict` 传给 `cbor2.dumps()`。
  2. 在 REST 路径上，`poll_once()` 先通过 `run_owned_blocking_call` 进线程执行 HTTP 请求，回到事件循环后（若是 `D16` 或 `418/429` 还把刚序列化的 `raw_payload` 重新 `json.loads` + `json.dumps`），紧接着又调一次 `run_owned_blocking_call` 进线程执行 `persist_rest()` 写盘。
- **具体实施内容**：
  1. **直接移除 `_ImmutableEnvelopeSnapshot` 防御包装与多余字典拷贝**：
     - 在 `spool/writer.py` 的 `_prepare_frame()` 中，直接从 `EventEnvelope` 读取字段构造 CBOR 映射并累加统计，删除 `_ImmutableEnvelopeSnapshot` 及 `MappingProxyType` 防御包装；保留 `envelope_for_group()` 的物理组名 `stream=group.name`，消费端直接读取 `source_sequence["sourceStream"]`，省去正常 WS 逐帧路径上的 `logical_envelope()` 拷贝（分片头和帧身份必须一致）。
  2. **消除 REST `D16` / `418` / `429` 的二次 JSON 解析**：
     - 在 `capture_public_response()` 中直接支持传入 `evaluated_period` 参数，并在 `RestCaptureResult` 上直接保留 `response_headers: Mapping[str, str]` 字段，彻底删除 `coverage.py:735–756` 中的二次 `json.loads` 与 `json.dumps`。
  3. **合并/批量化 REST 写盘线程切换**：
     - 保持 `418/429` 冷却门在存储等待之前同步生效，将 REST 观测寫盘合并，减少线程池上下文切换次数。
- **独立验收标准**：
  - `418/429` 冷却顺序回归测试通过；
  - `tools/verify_raw_chunk_golden.go` 校验生成的 `.zst` 分片与 Raw v1 规范 100% 兼容。

- **完成状态（2026-11-02 再次独立复核后补齐）**：
  - 原 `9915c83` 已完成 D16 一次性 provenance 序列化、418/429 响应头直读、REST 并发合并与正常 WS 消费路径的逻辑流拷贝移除；但 Writer 仍在 `_prepare_frame()` 内复制 `source_sequence`，并通过 `_PreparedFrame` 重复转发字段和构造 CBOR 映射，原“直接编码 EventEnvelope、彻底移除拷贝”声明不成立。
  - 现删除残留字典拷贝、字段转发与重复映射实现，直接 `encode_frame(envelope)` / `ChunkStatistics.add(envelope)`。保留有界批次、完整写入后提交统计和摘要、短写/失败恢复；调用方在 append 完成前保持内部元数据稳定。旧的单帧/批量篡改防御测试改为验证 append 完成后复用对象不影响已提交 Raw 和统计。
  - 修正 REST 合并刷盘失败时主调用方遗留未读取异常 Future 的问题：仅等待共享结果的后续调用方分配 Future，主调用方直接接收异常；正常合并仍复用既有 spool 锁和 owned worker。原提交可稳定复现 `Future exception was never retrieved`，新增回归在原提交失败、修正后通过。418/429 冷却在持久化 await 前同步生效，响应头 Mapping 直接传递。
  - 兼容性选择保持不变：Raw 帧 `stream=group.name`，正常 WS readiness / persisted 直接读取 `sourceStream`；异常序列缺口标记和低频快照适配仍可执行必要的 `model_copy`，不宣称所有复制都已删除。
  - 原“3.7x”仅指字典构造微测，不能代表 Writer 或全链路收益。此次本机交错五轮、每轮 100,000 次 `_prepare_frame`，相对 `9915c83` 中位耗时 **1.016s → 0.884s（约 1.15x）**；不含写盘且与离线测试并行运行，仅为局部参考，不替代 M3.1。
  - 实际 Writer 和封存 `.zst` 解压后内容逐字节匹配冻结 Raw v1 golden。除仓库 Go golden 校验外，另将真实新封存分片解压内容（773 bytes / 1 frame）作为临时输入交给未修改的 Go 工具，framing/CRC 校验通过；canonical CBOR、统计和 manifest 由 Python 回归覆盖。
  - 验证、具体计数和边界见 [`M1.4 复核记录`](benchmarks/m1.4-review-20261102.md)。本次仅补齐 M1.4，不提前实施 M2、M3 或 VPS 部署。


---

### Milestone 2 (`M2`)：消除过度设计、取消实时回读重验与清理双栈包袱（De-bloating & Architecture Cleanup）

**里程碑总目标**：彻底移除学院派过度防御——取消实时采集封存后的全文件解压回读与后台重验循环，取消启动时每流生成空 `.zst` 分片及额外边界文件，统一新旧两套采集器栈，并通过按需延迟导入隔离外围与历史模块。

---

#### 任务 `M2.1`：取消实时封存后的全文件解压回读（Readback）与后台重验循环

- **现状与瓶颈定位**：
  - 当前 `spool/writer.py` 在写入时已经在内存中流式计算好了每帧 `CRC32C`、未压缩 `SHA-256` 和压缩后 `SHA-256` 并执行了 `fsync`。但封存（`seal()`）时仍调用 `spool/readback.py:89`（`verify_sealed_chunk_readback`）把刚写进磁盘的 `.zst` 文件完整读出来解压一遍；随后后台校验任务又调用 `service/verification.py:61` 再把同一个文件从磁盘读出来解压扫描一遍。这种防范“操作系统 `fsync` 刚返回磁盘就损坏”的做法在实时采集路径上属于严重的过度防御。
- **目标文件与入口**：
  - `src/binance_market_data_recorder/spool/writer.py`（`RawChunkWriter.seal`）
  - `src/binance_market_data_recorder/spool/readback.py`
  - `src/binance_market_data_recorder/service/verification.py` 及 `service/runtime.py`
- **具体改造要求**：
  1. **实时封存零回读**：在 `RawChunkWriter.seal()` 中，完成流式压缩写入、写入期 `CRC32C` / `SHA-256` 计算、`fsync` 及 `.zst` + `.json` 原子重命名后，直接将分片登记为已完成封存，**默认不再在实时采集路径上重新打开 `.zst` 文件做全文件解压回读**，同时关闭采集期间的后台周期性全文件重验循环。
  2. **按需保留独立读取校验的场景**：`readback.py` 与 `verification.py` 的全文件读取与逐帧校验函数仅在以下三个非实时热路径场景调用：
     - 进程异常崩溃重启后，对未正常封存的 `.partial` 残留文件做截断恢复（`spool/recovery.py`）；
     - 跨卷/外部硬盘归档（`archive`）拷贝至目标盘 `fsync` 后、删除本地源分片前的完整性校验；
     - 运维人员手动运行 `binance-market-recorder verify-raw` CLI 命令做离线抽检。
- **独立验收标准**：
  - 实时采集封存一个分片的磁盘全文件解压读取次数从 `2` 次直接降至 **`0` 次**（纯流式写盘 + `fsync`）；
  - 手动执行 `verify-raw` CLI 与崩溃 `.partial` 恢复测试依然 100% 检出任何损坏或截断帧。

- **完成状态（2026-11-02 再次独立复核并补齐）**：
  - 原 `b76eae6` 已实现正常封存零解压回读，并保留崩溃恢复、归档校验与 `verify-raw` CLI；但仍留有采集期后台重验配置开关、为旧测试模拟器保留的 `TypeError` 重试 / tuple 兼容 / 缺失哈希时回读，以及未真正跑到采集阶段的运行时测试，不能按“彻底移除”直接签收。
  - 已删除 `live_raw_verification_enabled` 配置、环境变量及运行时后台重验任务；删除封存的 `verify_readback` 开关和兼容回退。压缩结果改为普通 dataclass，manifest 必须直接接收压缩时得到的大小与摘要，不再用空摘要或零大小作默认值。旧测试模拟器随内部接口更新。
  - `_HashingReader` / `_HashingWriter` 在压缩所需的一次源读取和目标写入中计算 SHA-256；源摘要只与 Writer/恢复扫描证据比较一次。修复压缩目标短写：写满当前缓冲区后再累计摘要，写入无进展则保留源文件并报错；7 字节短写故障在原实现失败、修复后独立逐帧验证通过。正常封存新 `.zst` 的全文件解压读取次数为 **0**；压缩仍需读未压缩 `.partial`，不宣称所有磁盘读取为零。
  - 现有 `.zst` 的幂等崩溃恢复核对、`.partial` 截断/校验和恢复、归档复制验证与手动独立校验保留。`verify-raw` 默认有界消费待校验队列；复查已经校验过的分片使用 `verify-raw --chunk-id <UUID>`，并按分片大小配置已有预算参数。
  - 新验收直接拦截 `Path.open`：实际 `StreamSpool` 大小轮换与优雅停机期间不得打开压缩 Raw 读取，随后独立验证产物；运行时测试实际进入 `RUNNING` 并停止 FakeCollector，确认仅保留日报 worker、没有 Raw 重验任务；CLI 首次通过后损坏同一分片，再按 ID 复验必须检出并返回退出码 `2`。
  - 本次全量离线回归 **2,516 passed / 24 skipped / 5 deselected**（13 条既有 fork 弃用警告）；Ruff、mypy（367 文件）、M0 契约、Go Raw v1 golden 与 `git diff --check` 全通过。定向 114 项及详细边界见 [`M2.1 复核记录`](benchmarks/m2.1-review-20261102.md)。本次仅补齐 M2.1；下一项为 M2.2，未进行 VPS 部署。

---

#### 任务 `M2.2`：取消启动时每流 0 记录空 `.zst` 分片（更新 `ADR-0038`）

- **现状与瓶颈定位**：
  - `spool/stream.py:331`（`seal_capture_startup()`）在每次进程启动时，为全部 80+ 个打开的 `StreamSpool` 立即创建并封存一个 `record_count = 0` 的 `.zst` + `.json` 空分片，仅用于标记 `process_startup_boundary`。这在每次重启时产生上百个无效小文件与 `fsync` 风暴。
- **目标文件与入口**：
  - `src/binance_market_data_recorder/spool/stream.py`（`StreamSpool.seal_capture_startup`）
  - `src/binance_market_data_recorder/models/manifest.py`（`RawChunkManifest`）
  - `src/binance_market_data_recorder/replay/` 及 `normalization/`
- **具体改造要求**：
  1. **直接移除启动空文件**：删除启动时为每个流强制写出 0 记录 `.zst` 空分片的逻辑，也不再额外生成单独的磁盘边界文件。
  2. **数据库记录 + 首分片 manifest 标记**：进程启动时仅在 SQLite `Catalog` 中记录一条启动事件，并在每个流收到第一帧后封存的首个真实分片 `.json` manifest 中带上 `process_boundary_before_first_frame = True` 即可。
- **独立验收标准**：
  - 进程启动时磁盘上产生的空 `.zst` 分片与额外文件数量为 **`0`**；
  - 现有回放与规范化测试全部通过。

- **完成状态（2026-11-02 再次独立复核后补齐）**：
  - **事实模型校正**：计划提及的 `src/binance_market_data_recorder/models/manifest.py（RawChunkManifest）` 在代码库中并不存在（无 `models/` 目录）；manifest 的构建与严格 schema 校验均在 [`src/binance_market_data_recorder/spool/seal.py`](../src/binance_market_data_recorder/spool/seal.py) 中。已在 `spool/seal.py` 的 `MANIFEST_OPTIONAL_FIELDS` 中扩展 `process_boundary_before_first_frame`，并在 `parse_strict_manifest_bytes()` 中严格校验布尔类型，保留严格 schema 拒绝未知多余字段。
  - **消除启动空分片**：修改 [`src/binance_market_data_recorder/spool/stream.py`](../src/binance_market_data_recorder/spool/stream.py) 中的 `StreamSpool.seal_capture_startup()` 为无操作并返回 `{}`，不再向磁盘写入任何 0 记录分片或边界文件，启动阶段磁盘空分片与额外文件数为 **`0`**。
  - **首分片边界标记与流转**：`StreamSpool` 启动时置 `_process_boundary_pending = True`；首个真实数据帧封存时将 `process_boundary_before_first_frame = True` 写入该分片的 `.json` manifest 及 `Catalog` 的 `SEALING` 状态 transition evidence 中，成功后清除 pending 标记；次分片及后续分片均不包含该字段。崩溃截断恢复路径（`seal_partial`）保持该标记不丢。
  - **全局启动事件与重连分界兼容**：`ServiceRuntime.start()` 启动时已在 `Catalog` 记录全局 `SERVICE_STARTED` 事件。在 [`src/binance_market_data_recorder/audit/reconnect_boundaries.py`](../src/binance_market_data_recorder/audit/reconnect_boundaries.py) 的 `_classify_inter_chunk()` 中引入对新分片 `process_boundary_before_first_frame: True` 的识别，跨进程启动重连准确判定为 `EXPLICIT_SEQUENCE_GAP`，不再依赖 0 记录空分片，且进程内后续发生的未标记重连继续严格判定为 `UNMARKED_RECONNECT`。在 [`src/binance_market_data_recorder/service/acceptance_v5_reconnect.py`](../src/binance_market_data_recorder/service/acceptance_v5_reconnect.py) 中更新 `manifest_projection()` 保持字段传递。
  - **更新架构决策记录**：更新 [`docs/adr/0038-durable-capture-startup-boundary.md`](adr/0038-durable-capture-startup-boundary.md)，增补 M2.2 修正说明。
  - **验收与回归**：新增专项测试 [`tests/unit/test_m2_2_zero_empty_startup.py`](../tests/unit/test_m2_2_zero_empty_startup.py)（7 项全部通过）；更新已有启动与重连测试套件 [`tests/integration/test_capture_startup_boundary.py`](../tests/integration/test_capture_startup_boundary.py)（17 项全部通过）、[`tests/integration/test_spot_stream_collector.py`](../tests/integration/test_spot_stream_collector.py)（10 项全部通过）、[`tests/integration/test_usdm_stream_collector.py`](../tests/integration/test_usdm_stream_collector.py)（4 项全部通过）、[`tests/integration/test_ms3b_production_paths.py`](../tests/integration/test_ms3b_production_paths.py)（9 项全部通过）；回放与规范化测试套件全部通过（50 项 passed）。
  - 复核报告见 [`M2.2 复核记录`](benchmarks/m2.2-review-20261102.md)。本次仅完成 M2.2，未提前实施 M2.3–M2.4 或 VPS 部署。
  - **再次复核发现并修正**：原 `5afcb40` 在旧连接仅有一帧且带 `sequence_gap` 时，先返回 `UNKNOWN`，导致新首分片已有启动标记仍误报 `unknown_reconnect`。现优先消费新分片的明确启动标记；无该标记时保留原来的 `UNKNOWN` / `UNMARKED_RECONNECT` 区分。两项新增参数化回归在原分类器上均失败，修正后通过。
  - **补实验收证据**：新增 Spot / USD-M 真实 `StreamSpool → Raw → Normalizer → Replay` 测试，混读旧 manifest 与两个进程的首/次分片，核对帧、成交 ID、连接 ID、payload 摘要和原始文件不变；运行时验证单一 `SERVICE_STARTED` 事件及无数据时 Raw 目录零文件。专项单测现为 9 项，新增回放集成测试 2 项；最终完整检查结果见复核记录。

---

#### 任务 `M2.3`：抽取公共 WebSocket 底座并统一新旧两套采集器栈

- **现状与瓶颈定位**：
  - `service/runtime.py:963`（`_build_collector`）当前存在双轨分支：当配置开启 `coverage.enabled` 时使用新的 `CoverageRuntimeSupervisor`（`collector/coverage.py`），否则回退到旧的 `MarketCollectorSupervisor`（`collector/runtime.py`），导致两套采集、重连与监督代码并存维护。
  - 同时，`binance/spot/websocket.py:34` 直接从 `binance/usdm/websocket.py` 导入 `_PublicWebSocketStream` 和 `_CombinedWebSocketStream`，模块命名与层次耦合反常。
- **目标文件与入口**：
  - `src/binance_market_data_recorder/binance/public_websocket.py`
  - `src/binance_market_data_recorder/binance/usdm/websocket.py`
  - `src/binance_market_data_recorder/binance/spot/websocket.py`
  - `src/binance_market_data_recorder/service/runtime.py`（`_build_collector`）
  - `src/binance_market_data_recorder/collector/runtime.py` 与 `collector/coverage.py`
- **具体改造要求**：
  1. **公共 WS 底座解耦**：将 `_PublicWebSocketStream` 与 `_CombinedWebSocketStream` 正式提升至市场无关的 `binance/public_websocket.py`（公开命名为 `PublicWebSocketStream` 和 `CombinedWebSocketStream`），让 `spot`、`usdm`、`coinm` 平级依赖 `binance/public_websocket.py`，消除 `spot` 反向导入 `usdm` 私有类的耦合。
  2. **配置自动映射与单栈统一**：在 `service/runtime.py:963` 中提供兼容适配器，将传统的显式 `spot_symbols` / `usdm_symbols` 配置自动转换为统一的 `CoverageRuntimeSupervisor` 采集计划；验证所有现有单产品/多产品集成测试在新栈上行为等价后，退役重复的旧版采集循环。
- **边界与不变量**：
  - 现有配置文件格式（无论使用 `spot_symbols`/`usdm_symbols` 还是 `[recorder.coverage]`）均保持 100% 兼容，CLI 行为不变。
- **独立验收标准**：
  - `binance/spot/` 下不再存在任何对 `binance/usdm/` 的导入；
  - `service/runtime.py` 采集路径收敛为单一监督栈，全部单元与集成测试通过。

- **完成状态（2026-11-02 二次独立复核并接续补齐）**：
  - 原 `8ca9f40` 已抽出公共 WS 底座并清理 Spot 对 USD-M 的反向导入；但仍保留按配置选择的旧采集监督分支，**当时未满足 M2.3 的单栈退出门禁**。历史测试对内部类的依赖不构成该门禁的豁免。
  - 服务现在统一运行 `CoverageRuntimeSupervisor`。新增 `collector/explicit.py`，将传统显式币种配置及辅助源开关/间隔映射为受限 source plan；不扩大到全量 coverage，不增加未配置市场。删除服务中的旧采集器工厂与独立全局辅助管理任务。
  - 保留显式配置的精确产品就绪、单市场零额外市场请求、每进程全局源去重、5m 统计游标续采和稀疏 OI 语义、共享 `418/429` 冷却门、停机同步封存和故障原因传播。coverage 配置仍沿用原动态发现规则。
  - 公共 WS 回调改为一次明确调用，在公共层合入 receipt flags；取消捕获任意 `TypeError` 再试一次的热路径兼容逻辑。
  - 历史单/多产品用例迁移至统一栈的行为验证；服务生命周期故障注入同样运行生产监督循环。旧采集器类仅保留为直接调用的兼容/历史测试模块，服务、工具和示例不再实例化；延迟导入与模块隔离留给 M2.4。
  - 最终全量离线回归 **2528 passed / 24 skipped / 5 deselected / 0 failed**；`ruff`、`mypy`（372 个源文件）、M0 契约、Go Raw golden 与 diff 检查通过。13 条既有故障注入 fork 警告及在线/压力验收边界已列入复核记录。
  - 完整发现、修正和最终验证结果见 [`M2.3 复核记录`](benchmarks/m2.3-review-20261102.md)。本次为本机离线验收，不替代 M3 基准或 M4 VPS 在线资格验证。

---

#### 任务 `M2.4`：CLI 按需延迟导入（Lazy Import）与 `Catalog` 超大文件按领域拆分

- **现状与瓶颈定位**：
  - `cli.py` 在顶层一次性导入了 `pyarrow`（通过 `normalization`）、`service/acceptance*.py`、`audit/reconnect_boundaries.py`、`spool/legacy_reconnect.py`、`storage/macos/` 以及 `archive/remote_*.py`。这导致采集守护进程每次启动都加载全部重型分析库、历史里程碑验收器与跨平台外围模块，增大常驻内存（RSS）与启动开销。
  - `storage/catalog.py` 单文件膨胀至 `5,106` 行，混合了建表迁移、原始分片登记、可用性区间、归档状态机、规范化构建与日常报告查询。
- **目标文件与入口**：
  - `src/binance_market_data_recorder/cli.py`
  - `src/binance_market_data_recorder/storage/catalog.py`（拆分为 `storage/catalog/` 包或按领域子模块，保持原导入路径 `from ...storage.catalog import Catalog` 100% 兼容）
- **具体改造要求**：
  1. **CLI 子命令级延迟导入**：将 `cli.py` 中与实时采集无关的模块改为在对应子命令执行函数内部按需导入：
     - `normalize` / `replay` 子命令执行时才导入 `pyarrow` 与规范化模块；
     - `storage` / `archive` 子命令执行时才导入 `storage/macos/`、`storage/linux/` 及 `archive/remote_*.py`；
     - 冻结保留的旧里程碑验收与重连审计模块（`service/acceptance*.py`, `audit/reconnect_boundaries.py`, `spool/legacy_reconnect.py`）完全移出常规运行导入图。
  2. **`Catalog` 模块化拆分**：将 `storage/catalog.py` 按职责拆分为清晰的混入或子模块（如 `schema.py`、`chunks.py`、`availability.py`、`archive_ops.py`、`reports.py`），对外继续导出统一的 `Catalog` 类，不破坏任何现有调用方。
- **边界与不变量**：
  - 遵照已锁定决策 **D5**：保留并冻结 macOS 外置存储、远程归档及历史测试工具的代码与对应测试，不删除外围功能，仅将其隔离出采集进程的热路径与启动导入链。
- **独立验收标准**：
  - 新增单元测试验证：启动采集运行时后检查 `sys.modules`，确认未加载 `pyarrow`、`service.acceptance`、`storage.macos` 等非采集模块；
  - 拆分后的 `Catalog` 通过全部现有数据库、归档、回放与报告测试。

- **完成状态（2026-11-02 复核后接续完成）**：
  - 原提交 `cb4f074` 已完成 CLI 按需导入主体、指标报告隔离、平台适配器延迟导入和 `Catalog` 领域拆分；但**尚未通过完整退出门禁**：`spool.recovery` 顶层仍导入 `spool.legacy_reconnect`，`_service run` 的路径解析也加载该模块；原专项漏查此模块且没有真正启动服务。另有 Ruff 1 项、全量 mypy 2 项错误。
  - `Catalog` 保留原导入路径与全部 130 个既有方法，分为 `types/base/schema/chunks/availability/archive_ops/reports`；原方法 AST 的差异仅为相对导入与 `Self` 类型注解，不改数据库状态机、DDL 或事务策略。
  - 将操作员分类文件路径解析移至轻量 `paths.py`，旧路径继续重导出。恢复入口用只读 `EXISTS` 查询检查是否存在 reconnect seal intent 或断线生命周期记录；存在相关记录或操作员分类文件时才导入并执行原判定器。**历史恢复规则保持不变**，畸形生命周期与无匹配候选的分类文件仍走原检查，不因延迟加载跳过。
  - `status` 在没有注册归档目标时不加载外设模块、不枚举卷；已有注册目标时仍按原逻辑显示外盘状态。`storage`、`archive`、归一化及旧验收功能和测试继续保留。
  - 恢复 `_run_acceptance_stage` 的静态类型（`TYPE_CHECKING` 导入），移除冗余 cast 和未使用局部变量；保留既有 CLI 延迟解析与 monkeypatch 接口，不新增框架或兼容包装。
  - 专项增至 **11 项**：检查整个模块前缀；覆盖 CLI/runtime/coverage 导入、`config show`、`doctor`、无外盘目标的 `status`，以及真实 `_service run` → 恢复 → 统一监督循环 → Raw 写入/封存 → 停机 → 再启动；另覆盖无分片时的历史分类文件/畸形生命周期阻断。
  - 全量离线回归 **2537 passed / 24 skipped / 5 deselected / 0 failed**；随后追加的两项恢复阻断测试随最终 11 项专项通过。Ruff、全量 mypy（380 个源文件）、M0 契约、Go Raw golden 与 diff 检查均通过。13 条既有 fork 警告及具体边界见 [`M2.4 复核记录`](benchmarks/m2.4-review-20261102.md)。本次仅完成 M2.4，不替代 M3 的跨轮换基准、压力测试或 M4 VPS 在线验收。

---

### Milestone 3 (`M3`)：多轮换三方对照基准测试与全量回归收敛（Benchmark & Convergence）

**里程碑总目标**：用科学、可复现的跨多轮换周期（`>= 45–60` 分钟）对照实验，将“代码与 I/O 架构优化收益”与“K 线采集范围收敛收益”分别独立量化，并通过全套离线契约、100 万事件压力测试与故障恢复测试，冻结可上线的生产构建包。

---

#### 任务 `M3.1`：三方多轮换对照基准测试（Three-Way Multi-Rotation Benchmark）

- **为什么必须三方对照且覆盖多个轮换周期**：
  - 短于单次轮换周期（如仅跑几分钟）的测试无法触发 `15m` 定时封存、回读校验与 `Catalog` 批量提交峰值，会掩盖稳态瓶颈；
  - 若只对比优化前与最终版，则无法区分性能提升究竟来自“SQLite/写盘/对象分配优化”还是来自“减少 K 线中间推送”。
- **具体测试方案**：
  在相同硬件环境、相同 `BTC + ETH` 源集合配置（`[recorder.coverage].underlying_assets = ["BTC", "ETH"]`）下，分别运行至少 **45–60 分钟**（完整跨越 3–4 个 15 分钟轮换周期）的三组对照实验：
  1. **对照组 A（Baseline `main` `f109ffc`）**：未优化基线（保留全部 K 线实时推送、60s 轮换、旧版逐源刷盘与双次验证）。
  2. **实验组 B（Optimized Same-Scope）**：启用 `M1 + M2` 全部代码、SQLite、缓冲、单次回读与轮换优化，但**保持与基线完全相同的采集范围**（仍写入全部 `k.x=false` K 线推送及全部 `S12` 周期）。用于纯正度量代码与 I/O 架构优化的真实收益。
  3. **实验组 C（Optimized Production-Scope）**：在实验组 B 基础上，启用 **D1**（K 线仅落盘 `k.x=true` 收盘帧 + REST 自动补采）与 **D2**（`S12` 仅订阅 `6h, 12h, 1d, 3d, 1w, 1M`）。用于度量最终生产形态的资源占用。
- **必须采集并记录的核心指标**：
  - 进程平均 CPU 占用率（%）、轮换封存窗口峰值 CPU（%）、常驻内存 RSS（MiB）随时间曲线；
  - 每分钟接收总帧数、实际落盘帧数、`forming_filtered_count` 过滤帧数；
  - 每小时封存分片数（files/hour）、磁盘写入字节速率（MB/min）及 `fsync` 次数；
  - `AvailabilityJournal` 队列长度水位（`max_pending_rows`）、每轮 flush 耗时分布（p50 / p95 / p99）；
  - 落盘排队延迟 `persist_lag_ms`（p50 / p95 / p99）。
- **独立验收标准**：
  - 产出完整的三方对照基准报告（写入 `docs/benchmarks/`），实验组 B 相比对照组 A 在相同数据范围下显著消除 `AvailabilityJournal` 积压与轮换风暴，实验组 C 满足 VPS 2 vCPU / 4 GB RAM 的充裕余量要求（稳态队列 `0` 积压，内存平稳无泄漏）。

---

#### 任务 `M3.2`：全量离线测试、100 万事件压力测试与故障注入回归

- **具体测试与检查清单**：
  1. **静态与契约检查**：
     - `python3.12 -m ruff check .`（0 警告/错误）
     - `python3.12 -m mypy`（0 类型错误）
     - `python3.12 tests/verify_m0_contracts.py`（契约检查通过）
     - `go run tools/verify_raw_chunk_golden.go`（Go 独立验证器确认 Raw v1 字节格式 100% 兼容）
  2. **全量单元与集成测试**：
     - `python3.12 -m pytest -q` 全部通过。
  3. **100 万事件压力测试**：
     - `python3.12 -m pytest -m stress -q tests/stress/test_million_events.py` 顺利通过，记录写入与扫描耗时及峰值 RSS。
  4. **专项故障恢复测试**：
     - **SQLite WAL 尾部回滚对账测试**：模拟 `PRAGMA synchronous = NORMAL` 下掉电丢失最后几秒 `Catalog` 提交，而磁盘已存在 `.zst` + `.json` 分片或孤立 `.partial` 文件，验证启动恢复（`spool/recovery.py`）自动补齐 `Catalog` 记录并恢复一致状态；
     - **K 线断线跨整点 REST 补采测试**：模拟 WS 在整点收盘时刻断线重连，验证自动触发 REST K 线补采、写入 `capture_flags = ("closed_bar_backfill",)` 且收盘序列无缺口。
- **独立验收标准**：
  - 上述全部测试命令 100% 绿灯通过，无新增回归或未受控警告。

---

#### 任务 `M3.3`：生产构建包与 VPS 部署配置冻结

- **具体改造要求**：
  1. 构建纯净 Wheel 包并记录源码 Git Commit SHA 与构建产物 SHA-256。
  2. 准备并校验生产配置文件模板（显式设置 `[recorder.coverage].underlying_assets = ["BTC", "ETH"]`、15 分钟轮换阈值、验证通过的 K 线与 `Catalog` 策略）。
  3. 准备非侵入式外部观测脚本（定期采样 `systemctl show`、进程 CPU/RSS、`binance-market-recorder status`、磁盘可用空间与分片增长率）。
- **独立验收标准**：
  - Wheel 包在本地全新虚拟环境中安装后，`binance-market-recorder --version`、`config show`、`doctor` 均正常执行并通过检查。

---

### Milestone 4 (`M4`)：GreenCloud VPS 连续实战验收（`2h` → `12h` → `24h` Qualification）

**里程碑总目标**：在目标生产环境 `greencloud-tokyo-01`（Ubuntu 24.04 ARM64, 2 vCPU, 4 GB RAM）上，以 `BTC + ETH` 全量生产配置发起**单次连续运行**，依次在 `T0 + 2h`、`T0 + 12h`、`T0 + 24h` 三个检查点进行严格的硬性门禁核验。**若任一检查点未达标，必须定位修复后从 `0h` 重新起跑**。

---

#### 任务 `M4.1`：VPS 宿主机预检、安全部署与起跑基准记录（`T0`）

- **操作安全铁律（遵循 `AGENTS.md` 远程维护安全规范）**：
  - 严禁执行任何跨 SSH 变量拼接的递归删除指令；保留历史运行证据目录，使用新的运行目录或经核验的归档清理流程。
  - 操作前后必须明确记录 `systemctl is-active` 与 `systemctl is-enabled` 状态。
- **具体执行步骤**：
  1. 检查宿主机磁盘剩余空间（确保远高于硬性保留阈值 `hard_reserve` 并具备充足的 24h+ 写入余量）、内存与负载状态。
  2. 为防止 Ubuntu 后台定时更新在 24 小时验收期间抢占单盘 I/O 或重启组件，对 `apt-daily.service`、`apt-daily-upgrade.service` 及 `unattended-upgrades` 执行运行时临时屏蔽（`systemctl mask --runtime`），并在验收结束后恢复。
  3. 部署 `M3.3` 冻结的 Wheel 包与 `/etc/binance-market-data-recorder/recorder.toml`（确认 `[recorder.coverage].underlying_assets = ["BTC", "ETH"]`）。
  4. 启动 `binance-market-data-recorder.service`，记录起跑时刻 `T0`（UTC）、`MainPID`、`InvocationID` 及初始资源快照。
- **独立验收标准**：
  - 服务一次启动成功，所有受管市场（Spot / USD-M / COIN-M 的 BTC 与 ETH 活跃产品）全部进入 `AVAILABLE` 采集状态，无 `400/-1121` 或未捕获异常。

---

#### 任务 `M4.2`：第一阶段检查点：`T0 + 2h` 稳定性与零积压门禁

- **核验时机**：连续运行满 **2 小时**（已跨越约 8 个完整的 15 分钟分片轮换周期）。
- **硬性验收门禁（Hard Gates）**：
  1. **进程连续性**：`MainPID` 与 `InvocationID` 与 `T0` 完全一致，`NRestarts = 0`，无 OOM 或未捕获异常。
  2. **队列与状态机健康**：`AvailabilityJournal` 待刷盘队列无持续积压（稳态回落至 `0` 或单批水位以内），无 SQLite `database is locked` 错误，无 HTTP `418/429` 封禁风暴。
  3. **单源可用性门禁**：每个活跃数据源的可用率 `>= 99.5%`（2 小时窗口内累计 `UNAVAILABLE + UNKNOWN` 时间 `<= 36 秒`）。
  4. **分片完整性抽检**：对前 2 小时内已封存的全部 `.zst` + `.json` 分片冻结快照，确认增量校验 `100%` 通过（`0` 损坏、`0` 截断、`0` 规范编码错误、K 线闭合序列无未标记缺口）。
  5. **资源水位记录**：记录 2h 内的 CPU 均值/峰值、RSS 内存曲线及每小时分片生成速率。
- **判定规则**：全部达标则**不停止服务**，继续向 `12h` 检查点运行；任一门禁失败则停止服务、取证修复并回退至 `M4.1` 重新从 `0h` 起跑。

---

#### 任务 `M4.3`：第二阶段检查点：`T0 + 12h` 中期耐久与内存/存储平稳性门禁

- **核验时机**：同一进程连续运行满 **12 小时**（跨越约 48 个 15 分钟轮换周期及资金费率结算窗口）。
- **硬性验收门禁（Hard Gates）**：
  1. **进程连续性**：`MainPID` 与 `InvocationID` 仍与 `T0` 完全一致（`NRestarts = 0`）。
  2. **内存与性能无退化**：常驻内存 RSS 在 `2h -> 12h` 期间保持水平稳态（无持续单调增长的内存泄漏），`persist_lag_ms` 与 SQLite 提交耗时与 `2h` 阶段持平。
  3. **单源可用性门禁**：每个活跃数据源在 12 小时窗口内可用率 `>= 99.5%`（累计 `UNAVAILABLE + UNKNOWN` 时间 `<= 216 秒`）。
  4. **增量校验零错误**：前 12 小时内所有新封存分片 `100%` 通过校验，`0` 损坏分片，`0` 隔离（quarantined）分片。
- **判定规则**：全部达标则**保持服务继续运行**，进入最终 `24h` 验收阶段；否则修复后从 `0h` 重跑。

---

#### 任务 `M4.4`：第三阶段检查点：`T0 + 24h` 全周期生产签收（含 24h WS 轮换与 UTC 跨日任务）

- **核验时机**：同一进程连续运行满 **24 小时**（必须完整覆盖一个 UTC 自然日跨日边界及 Binance WebSocket 24 小时连接主动轮换窗口）。
- **硬性验收门禁（Hard Gates）**：
  1. **全周期进程连续性**：连续运行满 24 小时，`MainPID` 与 `InvocationID` 全程未变（`NRestarts = 0`）。
  2. **24 小时单源可用性门禁**：全部活跃数据源在 24 小时窗口内可用率 `>= 99.5%`（累计 `UNAVAILABLE + UNKNOWN` 时间 `<= 432 秒`）。
  3. **真实 24h WS 会话轮换验证**：日志与 `Catalog` 明确记录了连接在接近 24 小时上限时的主动平滑轮换重连，重连后深度快照同步（Depth Bridge）与 K 线收盘连续性完好无损。
  4. **UTC 跨日批处理与日报验证**：
     - 跨 UTC 00:00 后，`collector/daily_trades.py`（`D06` 日度逐笔成交补充任务）成功完成且未干扰实时 WS/REST 采集；
     - `binance-market-recorder report daily --date <UTC-date>` 成功生成完整的单日质量报告。
  5. **优雅停机与最终全量封存核验**：
     - 满 24 小时后执行受控停止（`systemctl stop`），确认所有活动 `.partial` 分片在停机过程中全部优雅封存为完整的 `.zst` + `.json` 分片（遗留未恢复 `.partial` 数量为 `0`）；
     - 停机后确认并记录 `systemctl is-active` 与 `systemctl is-enabled` 状态（按维护规范保持服务处于受控禁用状态直至正式投产指令）；
     - 最终汇总产出《`M4` 24 小时生产验收报告》（归档于 `docs/milestones/`）。

---

## 四、里程碑全景总览表

| 里程碑 | 阶段定位 | 子任务清单 | 核心交付物与退出门禁 |
| :--- | :--- | :--- | :--- |
| **`M1`** | **核心性能与写放大治理** | `M1.0` 多流固定输入基准 **`[COMPLETED]`**<br>`M1.1` 15m/128MiB 封存轮换 **`[COMPLETED]`**<br>`M1.2` `Catalog` WAL+NORMAL 与状态去重 **`[COMPLETED]`**<br>`M1.3` K 线收盘落盘 + 断线 REST 补采 + `S12` 直接去重 **`[COMPLETED]`**<br>`M1.4` 移除热路径防御拷贝与 REST 合并 **`[COMPLETED]`** | 分片生成速率下降 `~90%`；`AvailabilityJournal` 零积压；K 线断线自动补齐闭合 bar；`ADR-0058` / `ADR-0059` 落地并通过单测 |
| **`M2`** | **去过度设计与移除过度防御** | `M2.1` 取消实时封存全文件解压回读与后台重验 **`[COMPLETED]`**<br>`M2.2` 取消启动时 80+ 个空 `.zst` 分片 **`[COMPLETED]`**<br>`M2.3` 抽取公共 WS 底座并统一双采集栈 **`[COMPLETED]`**<br>`M2.4` CLI 延迟导入与 `Catalog` 拆分 **`[COMPLETED]`** | 实时封存磁盘重读从 2 次直接降至 **0 次**；启动 `0` 空 `.zst` 分片；消除 `spot->usdm` 反向依赖；守护进程不加载外围重模块 |
| **`M3`** | **多轮换三方基准与收敛** | `M3.1` 45–60m 三方对照基准测试<br>`M3.2` 全量测试 + 100 万事件压测 + 故障注入<br>`M3.3` 生产 Wheel 包与配置冻结 | 独立量化“代码优化收益”与“K 线收敛收益”；`pytest` / `ruff` / `mypy` / `verify_raw_chunk_golden.go` / `1M stress` 全绿 |
| **`M4`** | **VPS 连续 `2h→12h→24h` 验收** | `M4.1` 宿主机预检与 `T0` 部署<br>`M4.2` `T0 + 2h` 检查点<br>`M4.3` `T0 + 12h` 检查点<br>`M4.4` `T0 + 24h` 终验（含跨日与 24h 轮换） | `greencloud-tokyo-01` 上 `BTC + ETH` 单次连续跑满 `2h → 12h → 24h`，`MainPID` 不变，可用率 `>= 99.5%`，`0` 损坏分片 |
