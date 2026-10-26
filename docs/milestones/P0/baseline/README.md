# P0 全现有功能稳态基准

状态：正式v2结果收尾中。与最初的 [20e57f3核心路径基准](../../../research/2026-10-26/baseline/README.md) 分开保存，不能混合做容量或优化胜出结论。

选定源 `113cdb3`（runtime tree与已接受 `01d7421`一致），在原GreenCloud VPS独立源码目录执行，复用已安装运行依赖，不替换生产venv/包，不启用生产服务。每次创建独立临时数据根与模拟注册归档目录，正常退出后只清理自己创建的测试数据；生产Raw、Catalog、evidence和归档均不打开。真实归档代码在临时目录执行，其卷发现返回fixture，物理I/O仍发生在主机ext4系统盘，不代表2TB归档盘吞吐。

## 固定协议

双币BTCUSDT/ETHUSDT，每币Spot+USD-M永续。20s准备后测量120s稳态；60s默认轮换、1s周期同步，真实服务组装、队列、盘口、指标、Catalog、心跳、all14辅助开关和正常停止/排空均开启。输入按每产品bookTicker50/s、aggTrade2/s、depth5/s，另USD-M mark1/s、liquidation每20s一次。所有WS原文及顺序固定；REST模型fixtures固定，五分钟查询源时钟固定为 `1793000000000ms`，接收和系统资源时钟仍是真实时钟。fixture内容/生成器和选定源/wheel对应关系在 [package-source.json](package-source.json)。REST模型重编码带原有非原始HTTP标签，不冒充原始正文。

后台归档/实际回读每10s一个有界批次；日常报告worker保留默认60s周期；65s后另在独立消费者进程对非空封存数据执行一次规范化、实际aggTrade回放和当天日报生成，输出成功路径与数量，不能让初始空分片算消费者已覆盖。10s归档及这次额外消费是固定测量配置，用于覆盖功能，不能称为生产默认负载。所有生产OS/归档timer保持原状。容量forecast使用真实可用空间；生产capacity-profile身份/硬储备停采、远程归档客户端和平台GUI等非稳态路径不在此测量内。

计时排除导入/fixture分配/初始准备和最终stop/全量核对。Raw核对在正常停止后完整读取本次生成的全部封存/归档文件，对WS逐帧数量、顺序、原文摘要及CRC、存储/解压哈希做核对；只此有限fixture有输入真值。输入接收/回放的完成及正常排空另有时间，不能拿120s当生产时长信用。内存含预分配fixture；root进程RSS和后台child峰值分别保留；profile另采 `/proc`后代RSS总和（含共享页重复，不是PSS）。

CPU是Recorder及已回收子进程分别计量；I/O/上下文切换、host steal/PSI、调度lag和原始样本均留存，子进程CPU在回收时计入可能略跨窗口，不能当精确逐时刻总CPU。低频profile单独执行，不混入无profiler成绩；10Hz attach仅在STEADY_BEGIN后开始，110s后在STEADY_END前结束，排除导入和最终核对。

## 复测

将当前源、必要fixtures和这两份脚本放到隔离目录，使用相同运行依赖；生产数据根不参与：

```bash
cd /path/to/isolated-checkout
PYTHONPATH=src:. /path/to/python3.12 docs/milestones/P0/steady_baseline.py \
  --repo "$PWD" --commit <selected-runtime-commit> \
  --symbols 2 --warmup 20 --seconds 120 --output /new/path/steady-n2.json
/path/to/python3.12 docs/milestones/P0/profile_steady.py \
  --root "$PWD" --commit <selected-runtime-commit> --profiler /path/to/py-spy
```

`profile_steady.py`启动自己的相同输入运行并在稳态窗口attach；输出路径必须未占用。运行文件在 [steady_baseline.py](../steady_baseline.py)、[profile_steady.py](../profile_steady.py)。后续P1 A/B保持同输入和后台频率交错重复，报告snapshot/chunk/REST数量与宿主steal差异；这里单轮基线不能推断规模曲线、CPU瓶颈归因或精确优化百分比。

## 初测限制保留

[preliminary](preliminary/) 保留第一版1/2币测量：all14采集及Raw核对通过，但最初封存的空分片过早触发消费者，规范化/回放实际数据未充分进入测量；日报只发生周期探测，没有生成文件。这些数字不能充当最终全现有功能结果。v2只修基准触发与固定REST源时钟，未更改运行时生产代码。最初核心基准目录37个文件逐字不变。

COIN-M、交割、新P2源、真实网络/TLS、24h发布周期/跨日恢复、生产容量和掉电不受此有限离线测量证明。
