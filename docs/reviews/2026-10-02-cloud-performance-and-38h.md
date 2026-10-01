# 云服务器性能判断、已实现优化与 38h 终点

更新：2026-10-02（Asia/Shanghai）。本次任务：`CLOUD_PERFORMANCE_OPTIMIZATION_AND_38H_PLAN`。
代码基线为审查提交 `befe152`；本次实现为未部署的工程候选。

## 结论

先保留 Python 架构。已确认并优化了不必要的逐事件工作、全量状态输出和单页验收
吞吐上限。云端测量支持这些改动有收益；没有证据支持立即整仓改写或购买更高配置。
剩余 CPU 密集扫描很可能还能通过有限多进程或独立原生扫描器加速，但收益须在同一
云服务器、同一 Raw 上测量。不得用 Mac 测试耗时作为生产性能依据。

用户已选择：安全清理旧数据，使用新的独立验收数据范围。清理/重新部署列入准备阶段，
本次没有实际清理或启动服务。计划的完成标志是 **2h、12h、24h 三阶段全部通过，
累计至少 38h 有效 Formal 观测**；不是改完代码、微基准通过或进程跑满 38h。

## 云端证据

主机 `greencloud-tokyo-01`，Ubuntu 24.04 x86_64，安装环境 Python 3.12.3。
read-only SSH 检查使用已有主机别名，不读取 SSH 密钥或 credential stores。
候选函数只在独立诊断进程的内存执行，未替换安装包、迁移 Catalog、发布验收文档，
也未创建 Formal T0。扫描调用使用 `nice -n 10`，数据已有归档，双侧预热且交替顺序；
没有清 OS cache、执行写入型磁盘压测或控制其他服务。

实际逻辑 CPU 为 **4**，MemTotal **6,163,623,936 bytes**，约 5.74 GiB；
不是原 ADR-0028 最低目标的 2 vCPU / 4 GiB。一次只读快照的 MemAvailable 为
5,440,782,336 bytes。不能把暂停状态的空闲资源当作长跑余量证明。

Recorder unit 的 CPU quota、MemoryHigh/MemoryMax 均为 infinity；未发现 systemd
配额把它限制在一个核心。根文件系统可用约 34 GiB，归档文件系统可用约 2 TB。
这些是容量快照，不是 38h 写入量预测或磁盘吞吐证明。

旧 baseline 的 3,425 个资源样本显示：51,440.420371 秒墙钟期间使用
47,609.208961 CPU 秒，平均 **0.9255 个核心**，约四核总能力的 23%。
采样进程 RSS 同时求和的最大值为 **488,407,040 bytes**；cgroup MemoryPeak
4,941,856,768 bytes 含 page cache，不能称为 Python RSS。样本文件 SHA-256：
`8fe2939c981418d052d0b385a339d315ad50c3232302140559dda228f418610c`。
历史 CPU/RSS 样本不含完整磁盘延迟和 steal 时间分解；不声称排除了全部 I/O 或邻居干扰。

独立短时 cProfile 对三个高事件数 book_ticker chunk 共 118,456 帧的扫描显示：
扫描 CPU 接近墙钟；总累计约 22.9 秒，其中 decode_envelope 11.56 秒，
canonical CBOR re-encode 5.64 秒，statistics.add 3.19 秒，FrameIdentity 2.27 秒。
这些累计时间有包含关系，不能相加，剖析开销也不能用作普通吞吐。

不带 profiler 的完成对比见 [原始 JSON](2026-10-01-cloud-performance.json)：

| 操作 | 已安装代码 | 优化候选 | 解释 |
|---|---:|---:|---|
| 六个真实 chunk、113,325 帧、77,188,136 解压 bytes | 各 chunk 两轮的中位墙钟合计 16.2655s | 12.9009s | 速度 1.261 倍，耗时减少约 20.7%；完整 Raw proof 相等 |
| 143,362 笔交易的 archive status 加 JSON serialization | 7.7647s / 163,339,213 bytes | 0.1761s / 416 bytes | 约 44 倍；汇总字段完全相等，明细改为显式分页 |

该 Raw 微基准只覆盖六个 chunk、五种 market/stream 组合；不是全数据集测量。
首次探测在输出完整 JSON 后遭外部 100s 时限终止，未作为成功证据；延长仅诊断
进程时限后正常退出 0 的第二次测量才是上述保存结果。原 scanner 源码未再变动；
之后添加的同文件 batch capture 用单独云端检查验证。

[云端 delta snapshot JSON](2026-10-02-cloud-delta-snapshot.json) 的新候选在
0.4446s 内捕获 480 chunk / 300 archive / 8 operational rows，冻结高水位一致，
序列化 snapshot 约 5.01 MB，进程最大 RSS（Linux ru_maxrss）为 144,256 KiB。
这只是一次 SQL 捕获检查，且执行顺序有 cache 差异；不将旧/新时间比称为 SQL 加速比，
也不把它当作包含 Raw/identity/readiness 的完整在线 observation。

## 实现与安全边界

1. **Raw 扫描延后生成边界摘要。** 每帧仍做 canonical CBOR、EventEnvelope、CRC、
   双 SHA 与统计验证；只在首尾及实际连接切换时生成 FrameIdentity/payload hash。
   最多保留两个尾部 EventEnvelope，额外内存受两个最大 frame 大小约束。完整边界
   计数、时间区间、digest 和首尾 proof 保持一致。完整读的常见路径还避免 bytearray
   和重复拷贝，短读/截断行为不变；chunk UUID 文本只生成一次。
2. **archive status 默认只返回汇总。** 在 SQLite 做 GROUP BY / error aggregation，
   不把全部历史交易装成 Python dict 和巨大 JSON。明细用
   `archive status --details --limit 100 --offset 0`，单页最多 1000 条。
   CLI 的默认 `transactions` 数组不再输出，新增 `transactions_included`；消费该
   数组的脚本需显式使用明细分页。Catalog 原无参数明细 API 保留兼容。
3. **有限批次消费，而非无界追赶。** [ADR-0035](../adr/0035-v5-bounded-delta-batches.md)
   声明每 family 最多四个 256-row SQL pages，7 MiB delta-entry 预算，8 MiB 总文档，
   1024 causal references 和原 240s 工作预算。旧 start 缺少新 policy 时，继续使用
   原一页/256-reference/4 MiB 规则。新 verifier 同时读取两种政策；旧 verifier 不
   理解新政策，不得用于新工件资格证明。Raw、Catalog schema 和 300/600/900s 规则不变。

真实 writer/seal/archive 离线测试覆盖四 ProductKey、十二核心流、五个窗口及一次
漏掉 300s cadence。正常窗口无 pending；一次遗漏后的积压可因字节预算分多次追平，
在 catch-up 样本之后的两个正常样本内清空。追加提交不能污染四页的冻结 snapshot，policy 在 T0 后
不能改。旧政策读取仍限一页。小帧测试证明逻辑，不代替云端真实高事件量/辅助流测试。

## 其他语言与配置判断

压缩已经使用 Zstandard 原生实现，schema validation 已用 Rust pydantic-core；
此次云端 profiler 的 cbor2 codec 也是原生扩展。换语言不会自动减少重复工作。
官方说明见 [python-zstandard](https://github.com/indygreg/python-zstandard) 和
[Pydantic](https://docs.pydantic.dev/latest/why/)。

| 路线 | 对本项目的判断 |
|---|---|
| Python + 已有原生库 | 当前优先。已测得收益，兼容与维护成本低；进一步减少 Python 对象构建/重复扫描需等价性验证 |
| 有限 Python 多进程 | 多文件扫描可并行，很可能利用目前空闲核心；先测两个 workers 的原型。结果须按现有确定顺序合并，不能让子进程并发写 Catalog/证据 |
| 独立 Go scanner | 如果优化后仍 CPU 受限，较合适的备选：每 chunk 调用一次、返回小 proof，保持 Python 负责事务/编排。已有 Go golden 只验证 framing/CRC，尚不是生产 scanner |
| C/C++ scanner / extension | 很可能加快紧密解析循环，但要自己承担完整 CBOR/类型/边界协议和跨平台构建成本。只加速扫描器；不优先重写整个 Recorder |
| Rust / Cython 局部加速 | Rust 可做同类独立 scanner；Cython 可减少部分 Python 循环成本，但对已经在 C/Rust 中执行的 codec/validator 未必有明显收益 |

Python 多进程可绕过单进程 GIL，但线程不等价于多核纯 Python 并行：
[Python 3.12 multiprocessing](https://docs.python.org/3.12/library/multiprocessing.html)。
Go 可用现成 [fxamacker/cbor](https://github.com/fxamacker/cbor) 做确定性编码基础，
仍须证明与 Raw v1/cbor2 canonical bytes、严格字段语义完全一致；
[Cython 官方教程](https://cython.readthedocs.io/en/latest/src/tutorial/cython_tutorial.html)
也明确静态类型对加速的重要性。这些是可行性依据，不是本项目已测 native 加速结果。

若只加速一个占总耗时 25% 的部分，即使它无限快，整体最多约 1.33 倍。因此不承诺
“改 C++ 就快十倍”。对残余扫描，减少 canonical re-encode/模型对象成本或跨文件并行
更可能产生实质收益；删除验证不属于可接受优化。

硬件判断按云端 CPU、steal、iowait/磁盘延迟、RSS/PSI、事件率一起做：
CPU 满且可并行才考虑更多核；RSS/swap/PSI 持续恶化才考虑更多内存；I/O 等待主导才
考虑更快磁盘。旧 baseline 用不到一个核心、采样进程 RSS 不到约 0.5 GB 的证据来自
Recorder 停止时的审计，不能证明 live Collector 的余量；它不支持直接把当前问题
归因于“4 核/6 GB 不够”，也未证明服务器已足够。未采购/升级服务器，未引入原生依赖。

## 验证与剩余工作

本地检查只证明逻辑/兼容：全离线 pytest 1850 PASS、24 skip、4 stress deselect，
147.19s；之后新增的 snapshot/policy 回归所在目标组 20 PASS。Ruff、mypy
（278 source files）、M0 和 Go Raw golden 均 PASS。已有 13 个 fork warnings 保留。
在线 Binance smoke、stress、干净 wheel 构建、双平台 CI、云端部署/长跑未在本任务执行。
它们按更新后的计划在发布/资格阶段执行；没有以微基准代替阶段通过。

下一步是小正确性修复、独立审查/发布，然后执行已选择的旧活动数据安全清理及新
canonical corpus 初始化。保留历史 Catalog/manifest、现有已核验归档和验收证据的
可恢复副本；清理从验收范围移出旧数据，不隐含删除唯一归档副本。任何最终物理删除
须对应明确的可删除对象和核验记录。未解决的旧库 full-audit scalability R-078
仍独立保留，fresh corpus 通过不会替它关闭。详见 [38h 计划](../qualification_to_24h_plan.md)。

后续 [计划复核](2026-10-02-plan-recheck.md) 限定了上述小样本结论，并把唯一当前
计划合并到 [milestone_plan.md](../milestone_plan.md)。旧计划文件只保留兼容链接。
