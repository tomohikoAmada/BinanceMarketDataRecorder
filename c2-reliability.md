# C2 可靠性进度

**C2 仍为部分实现。** 本页早期 macOS sanitizer 通过记录属于当时版本。当前 GreenCloud 原生回归与 TSan/SIGTERM 复验见[最终收尾报告](c1-closeout-20261107.json)；故障矩阵、多源调度和补齐仍待完成。

| 项目 | 已实现与验证 | 待验收 |
| --- | --- | --- |
| C2.1 文件恢复 | `recover_raw.py` 持有与 C++ 记录器相同的锁，复用固定 Python 版本的 Raw 扫描、截尾、隔离和封存对账。C++ 封存故障后保留 active，夹具验证恢复有效帧、截去半帧、标记不完整和重复执行。对账失败返回退出码 1，产物恢复后返回 0。 | C++ 自身在写入、fsync、rename、manifest、Catalog 各点的崩溃和系统调用故障矩阵。 |
| C2.2 连接生命周期 | C++ 握手和读取有超时，按预算抖动重连，23 小时主动换代。连接 ID 随代次变化，重连首帧记录缺口。回环夹具在首个连接后连续失败六次，第八次恢复采集；不再因六次失败终止记录器。 | 真实 Pong timeout、主动换连及快照桥接夹具。 |
| C2.3 REST 调度 | Spot S01 在请求前检查滚动配额和队列压力。完整及响应体中断的 429/418 均保留已收到的冷却信息；普通失败使用有限快速重试。持久 libcurl multi 复用连接，夹具核对冷却时机与连接次数。 | 多源齐发、多窗口并发准入、D16 旧桶与迟发。 |
| C2.4 补齐与连续性 | 原型记录深度序列缺口与重连边界。 | C++ 深度快照桥接、闭合 K 线补齐和重启后的修复状态。 |
| C2.5 过载与保底 | REST 在半队列压力时延后入场。队列已满时入队最多等 250 ms，之后停止接收并排空已入队记录，记录受影响的源。饱和夹具核对拒绝新记录后原队列的顺序与计数。 | 原生进程低空间和 ENOSPC 故障注入。 |

## 离线恢复

先停止目标根上的记录器。使用 C0.1 冻结的 Python 工作树和依赖，恢复完成后可用现有只读工具检查产物，再启动记录器。

```sh
PYTHONPATH="$scratch/python/src" "$py" prototype/recover_raw.py "$root"
PYTHONPATH="$scratch/python/src" "$py" prototype/read_raw.py "$root"
```

恢复会写入 Catalog，并可能封存或隔离原 active 文件。截尾恢复的 manifest 保留恢复证据。对干净但尚未封存的 active，恢复封存时记录连续性缺口。恢复测试使用合成数据和进程级故障。

## 测试结果

macOS arm64 上的 Release、ASan/UBSan 和独立 TSan 构建均通过 4 项 CTest。冻结 Python 版本的恢复、SIGKILL、封存边界和 M3.2 故障测试共 26 项通过。这 26 项覆盖复用的 Python 路径及参考行为，C++ 待验收项见上表。

当前 `ce7e4943` 在 GreenCloud 原生 Ubuntu x86_64 上通过 Release 四项 CTest，以及 ASan/UBSan 和 TSan 各三项相关回归（`c1-recorder`、`c2-reliability`、`c2-handoff-limit`）。TSan 默认 ASLR 下在 golden 启动前报 `unexpected memory mapping`；使用 `setarch x86_64 -R ctest …` 仅关闭测试进程 ASLR 后，三项全部通过，系统全局设置未改。此前待定位的 SIGTERM 用例在原生 ASan/UBSan 和 TSan 下均通过；现有夹具允许已启动 REST 请求完成其 10 秒超时，不将这段等待归为 WS 退避或数据竞争。
