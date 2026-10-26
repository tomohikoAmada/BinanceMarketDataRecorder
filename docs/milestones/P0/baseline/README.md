# P0 全现有功能稳态基准

状态：**PR #86 历史原始测量，文件保留**。R1 / R3通过复审，R2边界记录已补齐但仍有一项分类漏判，见[PR #87复审](../review-20261026.md#pr-87-复审2026-10-26)及[v3测量](../corrections-20261026/README.md)。下列v2结果只支持原文内容 / 数量，不能由排序核对证明物理顺序，也不能由最终总量证明计时窗口工作相同。旧结果未重算或改写。v3原标签为无profiler STEADY / profile NON_STEADY，分类修正后另行解释，profile仅诊断。两版仪表开销不同，不直接比较CPU。与最初的 [20e57f3核心路径基准](../../../research/2026-10-26/baseline/README.md)分开保存。

选定源 `113cdb3`（runtime tree与已接受 `01d7421`一致），在原GreenCloud VPS独立源码目录执行，复用已安装运行依赖，不替换生产venv/包，不启用生产服务。每次创建独立临时数据根与模拟注册归档目录，正常退出后只清理自己创建的测试数据；生产Raw、Catalog、evidence和归档均不打开。真实归档代码在临时目录执行，其卷发现返回fixture，物理I/O仍发生在主机ext4系统盘，不代表2TB归档盘吞吐。

## 固定协议

双币BTCUSDT/ETHUSDT，每币Spot+USD-M永续。20s准备后测量120s稳态；60s默认轮换、1s周期同步，真实服务组装、队列、盘口、指标、Catalog、心跳、all14辅助开关和正常停止/排空均开启。输入按每产品bookTicker50/s、aggTrade2/s、depth5/s，另USD-M mark1/s、liquidation每20s一次。所有WS原文及顺序固定；REST模型fixtures固定，五分钟查询源时钟固定为 `1793000000000ms`，fixture游标预置到上一个已关闭周期之前一格（合成初始元数据，不是历史数据/时长信用），避免测成整月冷启动追补；接收和系统资源时钟仍是真实时钟。fixture内容/生成器和选定源/wheel对应关系在 [package-source.json](package-source.json)。REST模型重编码带原有非原始HTTP标签，不冒充原始正文。

后台归档/实际回读每10s一个有界批次；日常报告worker保留默认60s周期；65s后另在独立消费者进程对非空封存数据执行一次规范化、实际aggTrade回放和当天日报生成，输出成功路径与数量，不能让初始空分片算消费者已覆盖。10s归档及这次额外消费是固定测量配置，用于覆盖功能，不能称为生产默认负载。所有生产OS/归档timer保持原状。容量forecast使用真实可用空间；生产capacity-profile身份/硬储备停采、远程归档客户端和平台GUI等非稳态路径不在此测量内。

计时排除导入/fixture分配/初始准备和最终stop/全量核对。Raw核对在正常停止后完整读取本次生成的全部封存/归档文件，对WS数量、按接收时间排序后的原文摘要及CRC、存储/解压哈希做核对；只此有限fixture有输入真值。物理写入顺序尚缺独立检查，不能由该排序后的比较证明。输入接收/回放的完成及正常排空另有时间，不能拿120s当生产时长信用。内存含预分配fixture；root进程RSS和后台child峰值分别保留；profile另采 `/proc`后代RSS总和（含共享页重复，不是PSS）。

CPU是Recorder及已回收子进程分别计量；I/O/上下文切换、host steal/PSI、调度lag和原始样本均留存，子进程CPU在回收时计入可能略跨窗口，不能当精确逐时刻总CPU。低频profile单独执行，不混入无profiler成绩；10Hz attach仅在STEADY_BEGIN后开始，110s后在STEADY_END前结束，排除导入和最终核对。

## PR #86 原始实测结果（v2）

[完整汇总](summary.json)、[无profiler原始结果](steady-n2.json)、[独立profile运行](profile-steady-n2.json)及各自execution文件保留实际命令/时间/退出码。

| 测量 | 无profiler主结果 | 独立profile运行（不作性能对照） |
| --- | ---: | ---: |
| 稳态墙钟 | 120.003s | 120.002s |
| Recorder CPU | 67.507s / 0.563核 | 62.740s / 0.523核 |
| 已回收后台子进程CPU | 65.043s | 61.317s |
| 上述合计平均核数 | 1.105 | 1.034 |
| Recorder RSS范围 | 146.0–165.7MiB | 141.6–160.6MiB |
| 归档/消费者子进程峰值RSS | 292.8MiB | 315.1MiB |
| event loop lag p99 / max | 43.85 / 162.63ms | 66.70 / 264.38ms |
| 宿主steal | 13.15% | 14.70% |
| 正常停止后逐条核对WS | 32,214 / 32,214 | 32,214 / 32,214 |
| 全源Raw帧（包括REST/snapshot） | 32,250 | 32,250 |
| 必需辅助源实例 | 26个，全部accepted>0 / failures=0 | 同左 |
| 可选规范化实际行 / aggTrade回放 | 12,584 / 139 | 8,593 / 193 |
| 实际日报JSON / 终点待归档 | 1 / 0 | 1 / 0 |

两轮合计 **64,428 / 64,428 WS原文及数量核对通过**；原脚本的“顺序通过”字段基于接收时间重排，不能证明物理写入未乱序。全部新文件完整读取/CRC/存储及解压摘要检查通过；4个核心产品在停止前均ready。该有限fixture的正确性不是交易所每事件捕获率，也不是99.5%新连续性口径的生产资格结果。

无profiler主结果的Recorder `/proc/self/io`：`write_bytes=132,284,416`、`cancelled_write_bytes=89,272,320`增量，`read_bytes=0`（page cache并不等于没有逻辑读），`syscw=36,961`；自愿/非自愿上下文切换增量162,660/16,661。后台每次CPU/IO/RSS计数在原结果的archive_runs中，不能把上述父进程I/O称为所有子进程总量或最终介质写入量。

[稳态火焰图](flamegraph-steady.svg)来自[原始folded](profile-steady-n2.folded)：py-spy0.4.2、10Hz、110s，STEADY_BEGIN后attach、STEADY_END前结束；保留1,590个栈样本，**2次采样错误**。不是零错误采样，不提供精确CPU百分比分解。图含真实后台进程在稳态发生的导入/归档/派生计算；图宽表示采样栈数量。profile运行观测到Recorder后代RSS和峰值467.9MiB，共享页可能重复计算，不是PSS。

后台封存/消费者一次批次所包含的前缀随调度而变，实际处理行数已列出；profile运行与主结果的后台处理量不完全相同，且采样运行并非相同nice优先级的优化对照，不能据两列CPU差异声称加速或采样开销百分比。P1比较须控制/报告这些工作量、交错重复和宿主steal；本次没有选最快/最慢轮推导容量上限。

[最终环境观察](environment-final.json)在2026-10-26T08:41:57Z确认：生产Recorder inactive/disabled/PID0/NRestarts0；安装的124个Python源仍与原20e57f3参考逐个一致；选定隔离源码126个文件无差异，实际import指向隔离目录。没有部署、生产采集或历史Raw审计。

## 复测

当前脚本采用 v3：五个边界保存逐源写入 / fsync 和队列进度，单列尾部耗时；物理帧不排序，分片只按 Catalog 生命周期排列；输出冲突在写入前拒绝。将当前源、必要 fixtures、原固定输入生成器及三份脚本（包括 `benchmark_checks.py`）放到新的隔离目录，使用相同运行依赖；生产数据根不参与：

```bash
cd /path/to/isolated-checkout
PYTHONPATH=src:. /path/to/python3.12 docs/milestones/P0/steady_baseline.py \
  --repo "$PWD" --commit <selected-runtime-commit> \
  --symbols 2 --warmup 20 --seconds 120 --output /new/path/steady-n2.json
/path/to/python3.12 docs/milestones/P0/profile_steady.py \
  --root "$PWD" --commit <selected-runtime-commit> --profiler /path/to/py-spy
```

`profile_steady.py`启动自己的相同输入运行并在稳态窗口attach；全部七类输出路径必须未占用。运行文件在 [steady_baseline.py](../steady_baseline.py)、[benchmark_checks.py](../benchmark_checks.py)、[profile_steady.py](../profile_steady.py)。后续P1 A/B保持同输入和后台频率交错重复，报告snapshot/chunk/REST数量与宿主steal差异；这里单轮基线不能推断规模曲线、CPU瓶颈归因或精确优化百分比。

## 初测限制保留

[preliminary](preliminary/) 保留第一版1/2币测量：all14采集及Raw核对通过，但最初封存的空分片过早触发消费者，规范化/回放实际数据未充分进入测量；日报只发生周期探测，没有生成文件。这些数字不能充当最终全现有功能结果。随后的冷启动整月追补使消费者达到90s超时，相关原始失败也保留。另一次固定游标测量虽然完成Raw和消费者核对，但逐源检查发现taker fixture返回了不允许的尾部周期，触发SideDataSchemaError；该结果也移入preliminary/rejected-source-fixture，不能用作成功基准。最终fixture遵循已有CursorApi的前置周期语义，经六类周期响应预检，并要求全部已启用源accepted>0、failures=0才发布结果。以上均是测试输入/触发修正，没有更改运行时生产代码。最初核心基准目录37个文件逐字不变。

COIN-M、交割、新P2源、真实网络/TLS、24h发布周期/跨日恢复、生产容量和掉电不受此有限离线测量证明。
