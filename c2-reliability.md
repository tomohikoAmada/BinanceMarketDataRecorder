# C2 核心可靠性

**精简 C2 核心可靠性已于 2026-11-08 完成。** 当前版本、逐项结果及原始日志摘要见[原生验收报告](c2-core-qualification-20261108.json)。 本阶段只有停止唤醒、深度快照桥接、轻量多源 REST 和文件恢复边界。多周期 K 线补齐、旧桶重试和全量盘口均已移出范围。C1 已按[稳定单源收尾报告](c1-closeout-20261107.json)签收，完整容量压力仍为未通过。

## 实现与成本

| 核心项 | 行为 | 验证 |
| --- | --- | --- |
| SIGTERM / 退避唤醒 | 用普通 condition_variable 与显式 stop_callback 替代隐式 stop-token 等待。回调、谓词和通知使用同一 mutex；注册发生在加锁前，锁先于回调析构，涵盖 stop-before-wait 及等待交接。协调线程保留 50 ms 信号检查，在途 HTTP 保留既有 10 秒超时。 | 每构建 12 次回归，覆盖收到重连日志后立即、10/150/280 ms 发送 SIGTERM；健康 HTTP 下退出 <1.5 秒。另保留已启动 HTTP 的 12 秒退出上限用例，未放宽超时。 |
| 深度快照桥接 | 启动首帧、断线换代、malformed 或序列断层触发按预算的 REST depth。Spot 用快照 lastUpdateId 与 U/u 覆盖关系，USD-M 首次桥接后另检查 pu=前一 u。过期代次快照不污染新连接，旧快照重新请求，重复更新标 depth_stale。 | Spot/USD-M 真实 loopback，启动、断层、重连三次快照桥接；另覆盖过期响应、快照太旧和有界序列缓冲溢出。Raw v1 仍由冻结 Python reader 独立检查。 |
| 轻量 REST | 现有 REST 线程与持久 libcurl multi 串行工作。目录启动及每 24 小时一次；USD-M 五路按启动后 50/100/150/200/250 秒错峰，每 300 秒重复。depth 请求优先，所有请求遵守滚动请求、weight 与统计窗口和全局 429/418 冷却；半队列压力下延后入场。 | 使用 6 秒测试周期验证两轮错峰及连接复用；完整 418、截断 429 后保留冷却。第二轮 OI 的截断 429 会阻止其余所有源提前发出；合法旧桶和空数组原样记录，无快速重试。 |
| 文件恢复 | 复用共享根锁与冻结 Python 恢复/隔离/对账。I/O 故障可见，失败对账退出码 1，重复恢复幂等，保留不完整证据。 | Linux 专用 LD_PRELOAD 夹具实际注入 C++ 系统调用：EINTR、短写、ENOSPC、fsync EIO、成功 Raw fsync 后中断、第二帧撕裂、封存 rename 前/后、manifest 发布后、Catalog 提交后中断，共十例。 |

保持 WS、REST、存储三个工作线程、8,192 条 /64 MiB 队列、单存储线程和 Raw v1。桥接仅缓冲最多 8,192 份 U/u/pu（约 192 KiB 序列数据加容器开销），原始消息已按接收顺序落盘；不缓存盘口树，也不缓存第二份 Raw payload。快照与 WS 用 `depthSnapshotEpoch` 关联；WS 记录附 `snapshotLastUpdateId`，成功桥接记录带 `depth_bridge`，缺口不会因后续快照成功被改写。接收时间保持原应用边界。

Raw 的市场身份保持旧合同：Spot 为 spot，USD-M 永续为 um_perpetual，全市场目录为 um_shared；provenance 的 venue_market 使用 spot/usdm。目录、快照和五路响应保留请求参数、响应状态与原始 body。按市场/标的/流分开活跃文件，避免把 __GLOBAL__ 目录混入 BTCUSDT 分片。

## 运行

C3 白名单尚未实施。本阶段一次运行固定 BTCUSDT、一个市场；末尾市场参数可为 spot 或 usdm，默认 spot。公开入口启用 C2 核心路径，loopback 原 C1 入口保留用于历史范围回归。

```sh
root="$HOME/.local/share/BinanceMarketDataRecorder/S-BMDR/core-usdm"
"$build/sbmdr-c0" record --public "$root" 1000000 /etc/ssl/certs/ca-certificates.crt usdm
PYTHONPATH="$PWD/vendor/python-baseline/src" "$py" prototype/read_raw.py "$root"
```

五路轮询为 openInterest、topLongShortAccountRatio、topLongShortPositionRatio、globalLongShortAccountRatio、takerlongshortRatio，统计请求使用 period=5m、limit=1。未收到合法响应会保留失败响应及 source_failed；不把合法旧桶伪装成已补齐，也不实现旧桶重试。

[Spot 官方桥接说明](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#how-to-manage-a-local-order-book-correctly)与[USD-M U/u/pu 说明](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly)于本次复核；USD-M 使用 /public/stream 路由。Spot depth limit=5000（weight 250），USD-M limit=1000（weight 20），由既有分配池扣账。当前[官方 REST 目录](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data)将 topLongShortAccountRatio 标为需 API key；本阶段未引入密钥；本次真实无钥烟测的五路响应均返回 HTTP 200，与该文档标注的差异单列保留，不承诺端点持续可无钥访问。

## 恢复与验证

先停止目标根上的记录器，再运行：

```sh
PYTHONPATH="$PWD/vendor/python-baseline/src" "$py" prototype/recover_raw.py "$root"
PYTHONPATH="$PWD/vendor/python-baseline/src" "$py" prototype/read_raw.py "$root"
```

恢复可封存或隔离原 active，保留截尾和不完整标记。十个新边界案例之外，原 c1-recorder 仍验证封存失败后的半帧截尾、重复恢复，以及 artifact 缺失时退出 1、恢复 artifact 后退出 0。LD_PRELOAD 仅作用于独立 loopback 夹具子进程；测试库不是记录器依赖，不载入公开采集进程。系统全局 ASLR 和旧生产服务均保持原状态。

GreenCloud 原生 Release 七项检查通过；ASan/UBSan 与独立 TSan 的六项适用检查通过，12 次 SIGTERM 边界回归在三种构建均通过。Linux 系统调用夹具另以未插装的 Release 子进程运行，十例通过，未与 sanitizer 混用。TSan 仍仅对测试进程使用 `setarch x86_64 -R`，全局 ASLR 未改。

保留 TSan 首轮 5/6 通过及错峰断言失败的原日志：原夹具把 5 分钟压缩成 1.8 秒，首个轮询时隙仅 0.3 秒；实测 TSan 目录准备阻塞约 0.44 秒，首两路仅隔 0.139 秒。测试周期改为 6 秒、相位间距 1 秒后，保持 ≥0.2 秒间隔断言，Release/ASan/TSan 分别复验通过；生产 300 秒周期和所有退出时限均未改。该修正与业务调度或数据竞争结论分开记录。

真实公开烟测 Spot 20.09 秒独立读回 198 条；USD-M 310.21 秒独立读回 3,046 条，五路 REST 实际发出时间为启动后 50.001/100.000/150.001/200.001/250.001 秒，七种 REST 来源（含目录、depth）本次均 HTTP 200。两市场均验证深度桥接及 SIGTERM 排空，真实 Raw 保留。该烟测为功能证据，期间可能与 sanitizer 编译重叠，不作为生产性能基准。

23 小时主动换连与 24 小时生产连续运行留到 C5 的自然覆盖；C3–C5 尚未实现。旧生产服务保持 inactive/disabled；本轮采集进程已全部退出。
