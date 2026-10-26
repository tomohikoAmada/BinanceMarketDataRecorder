# 首份 VPS 核心路径基准与火焰图

日期：2026-10-26。该基准提供优化前的固定输入参考，**不是完整生产负载、币种容量上限或长跑通过证明**。

## 测了什么

使用 GreenCloud 原 VPS 的已安装包：`20e57f30682e88b4090cc2f0da34aa26fca4de2e`，124 个 Python 源文件逐个与提交匹配。Python 3.12.3，Ubuntu 24.04 / Linux 6.8.0-146，x86_64，4 vCPU，约 5.74 GiB RAM，ext4。CPU 型号、最后服务状态和脚本 SHA 在 [environment-final.json](environment-final.json)；依赖版本在每份结果的 `dependencies` 中。

测试使用真实 Collector、队列、envelope、盘口同步、指标、Raw 编码、写盘、fsync、封存和 SQLite；通过注入固定 WS 输入与 REST snapshot 代替网络。**没有禁用这些核心功能来制造好成绩。** 已有生产 venv 只用于执行；py-spy 0.4.2 安装在独立临时目录。所有生成的测试 Raw 在专用临时子目录，完成核对后由脚本删除；生产数据未打开或改动。最后生产 Recorder 仍 inactive / disabled / PID 0。

每币包括 Spot + USD-M 永续两个产品，各有 bookTicker、aggTrade、diff depth，以及 depth snapshot。每产品输入数量为：`bookTicker=events`、`aggTrade=events//25`、`depth=events//40`。输入高频批量回放，没有按交易所真实到达间隔限速；约 94% 是 bookTicker。消息大小、价格、盘口变化和时间戳固定。这是可重复的模型负载，不代表市场行情分布。

**计时边界：** 导入、Collector 构造和 fixture 分配在计时外；从启动 Collector 到接收完、排空队列、停止和封存的时间在计时内。另存 `capture_wall_seconds` / `capture_cpu_seconds`，表示达到输入已处理且 ready 时的读数。随后完整读取生成的数据，逐帧对照原文、计数、CRC 和文件摘要；验证时间单独记录。内存数字包含预先分配的 fixture，不能直接当作生产内存预测。日归档 worker、外部报告进程、真实网络 / TLS、所有辅助频道与 REST 轮询，以及未实现的 COIN-M / 交割合约都不在本轮覆盖范围。

## 不带 profiler 的结果

| 负载 | WS 帧数 / 次 | 墙钟秒，含停止封存 | CPU 秒 | 帧 / 秒 | CPU 微秒 / 帧 | 采样峰值 RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 币，短跑 r1 | 4,260 | 2.873 | 3.327 | 1,483 | 781 | 134.6 |
| 1 币，短跑 r2 | 4,260 | 6.463 | 8.348 | 659 | 1,960 | 131.1 |
| 1 币，短跑 r3 | 4,260 | 3.391 | 4.065 | 1,256 | 954 | 132.3 |
| 2 币，短跑 r1 | 8,520 | 4.915 | 5.478 | 1,733 | 643 | 135.8 |
| 2 币，短跑 r2 | 8,520 | 10.805 | 13.443 | 789 | 1,578 | 136.1 |
| 2 币，短跑 r3 | 8,520 | 10.100 | 13.218 | 844 | 1,551 | 142.1 |
| 10 币，一次 | 42,600 | 38.225 | 45.814 | 1,114 | 1,075 | 186.3 |
| 1 币，较长输入一次 | 21,300 | 19.298 | 24.446 | 1,104 | 1,148 | 136.3 |
| 2 币，较长输入一次 | 42,600 | 35.735 | 44.544 | 1,192 | 1,046 | 155.2 |

9 次不带 profiler 的运行共 **144,840 / 144,840 条 WS 原文核对通过**，没有 malformed 标记。depth snapshot 是注入模型转换的响应，明确带有 `sdk_model_not_raw_http_body` / `injected_model_without_raw_http_body` 标记，不能冒充实际 HTTP 原始报文。重试次数及初始 snapshot 数随并发调度可能变化；WS 输入固定，因此严格的优化 A/B 还应报告 snapshot / chunk 数并控制这部分差异。

短跑顺序是 1、2 币交替重复三次，然后 10 币；较长输入在之后单独运行。结果文件为 `baseline-n{N}-r{R}.json` 和 `baseline-long-n{N}.json`，每个伴随 `.execution.json` 记录命令、开始 / 结束时间和退出码。短跑 1 币 CPU/帧中位数 954 μs，2 币 1,551 μs，但范围高度重叠，**不能从这三个重复推导二次复杂度或精确容量曲线**。

这些运行中，主机 `/proc/stat` 的 steal 占比约 **6.45%–28.79%**。steal 是虚拟机希望运行时宿主没有给到的 CPU 时间，反映共享主机因素；它不是 Recorder 自己消耗的 CPU，也不能单独证明提供商限流原因。较长输入的 1 / 2 币平均使用约 1.267 / 1.247 核，事件循环延迟 p99 约 72.5 / 104.8 ms；10 币短跑约 397 ms。延迟来自有限采样，不是网络延迟或漏数据率。

墙钟成绩和 CPU 成绩都存在波动。后续对比需要交错重复、记录 steal 和相同输入，不应拿这里最快一轮与未来最慢一轮比较，或反过来。

## 火焰图怎么看

- [完整火焰图](flamegraph-full.svg)：1,452 个 Python 线程栈样本，25 Hz，py-spy 报告 0 个采样错误。含导入、采集、启动 / 停止生命周期和事后验证。
- [采集及生命周期视图](flamegraph-capture.svg)：从同一批样本过滤导入、明确的验证栈以及线程池等待 / 未解析栈，保留 902 样本。仍含启动、封存和等待，不是纯稳态 CPU 图。
- [原始 folded 栈](profile-python25-n2.folded)、[采样运行结果](profile-python25-n2.json)、[执行命令](profile-python25-n2.execution.json)、[分类计数](profile-summary.json)。

SVG 可在浏览器中点击放大、搜索函数。横宽表示该栈的样本数，**不是精确 CPU 耗时百分比**。多线程、系统调用、锁等待和采样方式都会影响宽度；不能把父子栈百分比相加。

在完整 1,452 样本中，按互斥分类有：234 样本停在 Raw `_write_all` 调用（16.12%）；206 样本位于 Catalog 路径（14.19%，包含生命周期事务）；78 样本属于其余封存路径（5.37%）；25 样本位于 Raw fsync（1.72%）。另外 293 样本属于线程池等待 / 未解析位置，211 属于导入，46 属于明确的事后验证栈。分类顺序和限制见 `profile-summary.json`。这些数支持优先研究写入与元数据通路，**不支持断言磁盘占了 16% CPU 或 SQLite 占了 14% CPU**。

第一次尝试 `--native --rate 100` 出现持续 1–3 秒的采样滞后，最终 47,406 样本、423 个采样错误。该运行自身完成并核对 17,040 条消息，但 capture 墙钟受严重干扰，约 96.9 秒。保留 [原始压缩栈](profile-n2.folded.gz) 与 [运行结果](profile-n2.json)，只用于披露失败测量方式，**不作为性能成绩或可信开销分配**。随后降低到 Python 25 Hz 的独立运行核对同样数量的输入，capture 约 17.2 秒；也不混入上方无 profiler 表格。原生采样完成后才进行低频采样，没有同时运行两个基准。

## 据此优先验证的优化方向

1. 检查真实批量写入：当前接收 / 线程移交已经批量，但底层仍每帧执行 `os.write`。比较批量写系统调用、编码分配和统计回调的收益，保留短写处理、周期性 fsync 和恢复边界。
2. 分离生命周期成本：初始空分片、封存和 Catalog 事务在短跑中占比明显；下一轮测量增加稳态窗口，不能把启动费用外推成每小时恒定费用。
3. 保持 Raw 接收短通路：完整盘口重建、报表、规范化、历史全量审计应作为独立消费者；必要的序列观察与缺口标记仍应保留。
4. 更换语言作为后续有对照的实验：先做等价工作量和相同正确性要求的比较。当前没有 Cryptofeed / Rust 对比成绩，也没有证明语言是主要根因。

## 复现

脚本是上一级的 [preplan_baseline.py](../preplan_baseline.py)，SHA-256 `b5cf5a22635510ec3dc3af7563db8909d13308c8b59f367ebafee7867cb521a6`。它只注入本地 fixture，没有真实 WS / REST 网络请求。原模板 `tools/benchmark_usdm_ingress.py` 的模拟消息缺少当前解析器要求的 `st` / `ps`，本轮没有沿用其结果；新脚本明确提供这些字段并在结束时拒绝 malformed。

在可用的独立基准目录中、使用指定版本环境执行，例如：

```sh
nice -n 10 env PYTHONDONTWRITEBYTECODE=1 /path/to/venv/bin/python \
  /path/to/preplan_baseline.py --symbols 2 --events 10000 \
  --timeout 180 --temp-parent /var/tmp/recorder-benchmark \
  --output /var/tmp/recorder-benchmark/new-result.json
```

目录必须预先存在，结果路径必须未被使用。采用 `--events 2000` 复现短跑；`--symbols 10 --events 2000` 复现 10 币输入。应先核对运行包提交 / 依赖，不要直接在这个落后的本地工作区运行后与 VPS 成绩混为一谈。脚本读取的包由所选解释器决定。

低干扰采样命令保存在 `profile-python25-n2.execution.json`，核心形式为 `py-spy record --rate 25 --format raw ... -- python preplan_baseline.py ...`。火焰图由 [FlameGraph 固定版本](https://github.com/brendangregg/FlameGraph/blob/41fee1f99f9276008b7cd112fca19dc3ea84ac32/flamegraph.pl) 生成；完整图直接以 folded 文件为输入。过滤图排除含 importlib、`iter_source_records`、`load_source_chunks` 的栈、空栈及叶节点 `_worker (concurrent/futures/thread.py:89)`，不修改剩余权重。工具用法参考 [py-spy 官方仓库](https://github.com/benfred/py-spy)。

现有数据足以保存一个真实的核心路径起点。正式计划前的性能研究还缺：真实全功能运行的稳态样本、系统态 / 用户态与 I/O 等待的进一步拆分，以及候选实现的等价对比。此次未启动生产长跑，也未宣称已经完成这些工作。
