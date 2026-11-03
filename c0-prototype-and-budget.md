# C0.2–C0.4 原型、格式与预算

## C0.2 本地原型

[原型](prototype/main.cpp)用 Boost.Asio/Beast 收 Spot BTCUSDT S02 combined depth，用 libcurl multi 收 S01 目录响应。simdjson 检查本场景所需的路由、时间、序列和字段。采集端将拥有型 payload 移交独立写入线程，队列同时限制记录数和 payload 字节数。WS 保留应用层原始字节，HTTP 保留 libcurl 交付的响应主体及请求路径、状态和 Content-Type；本地 HTTP 不启用内容解压，REST 主体只以现有 `http-body-base64.v1` 表示保存。Raw 使用确定顺序的 CBOR、Castagnoli CRC32C 及现有 v1 头与帧；CBOR 使用 nlohmann JSON，CRC 原型使用本机已有的 Abseil 实现。

[复用夹具的验收](prototype/verify.py)以 [C0.1 固定提交](c0.1-baseline.md) 的独立 Python reader 校验 Raw、payload、序列和来源字段。Python normalizer 可读取分组 observation。C++ 重编 golden 后逐字节与原向量相同。夹具覆盖五个场景：

| 场景 | 验收结果 |
| --- | --- |
| 正常 WS、截断 WS 和正常 HTTP | 两条 WS 和一条 REST 均读回，截断 WS 原字节保留并标记 `malformed` |
| HTTP 503 | 主体保留，标记 `source_failed` 和 `malformed` |
| 17,817,762 字节目录 | 以原 Spot 目录补 JSON 空白至 P4 历史大小，主体逐字节读回 |
| 突发及存储停顿 | 8,192 条 × 8 KiB WS，随后一次大目录，写线程启动延迟 250 ms；8,193 条全数读回，队列达到 8,192 条 / 64 MiB |
| 24 MiB HTTP | 超过本原型 body 上限，传输显式失败，未创建 Raw 文件 |

C0.2 使用同步 loopback WS 与单个 multi HTTP handle，顺序驱动两种输入并与写线程并行。只写未压缩的 v1 active 样本，结束时显式 fsync。TLS、正常运行的一秒 fsync、轮换与封存发布、Catalog、完整市场语义及恢复留给 C1/C2。24 MiB 附近的 HTTP body 限制由 32 MiB **编码后**帧预算和 base64 膨胀决定，给 provenance 和 CBOR 保留 4 KiB。当前大样本距上限仍有约 7 MiB。

Release、ASan/UBSan 和独立 TSan 的 CTest 均通过。固定 Python 提交的 Raw golden、帧格式及回放相关测试共 17 项通过。

### 实测

Apple M3、8 核、24 GiB、macOS arm64，AppleClang 21 Release。下表为最终实现三轮中位数，完整逐轮数据及依赖版本在 [测量记录](c0.2-measurements.json)。CPU 汇总原型进程线程。RSS 是原型进程峰值，排除夹具服务器。分配统计覆盖原型进程的非对齐 C++ `new` 请求，累计请求字节不等同峰值，C `malloc` 和对齐分配另计。

| 场景 | 墙钟 ms | CPU ms | 峰值 RSS MiB | C++ 分配次数 | C++ 请求 MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| 正常及畸形 | 4.43 | 3.25 | 14.92 | 1,328 | 17.15 |
| HTTP 503 | 1.17 | 0.52 | 11.05 | 645 | 0.05 |
| 大目录 | 81.48 | 77.96 | 145.70 | 670 | 527.66 |
| 突发加大目录 | 464.52 | 262.26 | 215.44 | 2,237,609 | 981.25 |

突发场景三轮最早接收至最终成功 fsync 的上界为 469 ms，含 250 ms 注入停顿。这是短测结束时的批量同步，生产耐久延迟使用后文单独门禁。

## C0.3 冻结选择

继续 **Raw v1 + 未压缩 active + 原封存流程的 Zstd level 3**，REST 继续 **libcurl multi**。同一夹具、编译器与硬件顺序各跑三轮。初版 CBOR 从空 vector 增长，然后将正文追加至帧头 buffer 再用 `write` 写出。保留方案预留正文缓冲，分别计算帧头和正文，使用支持短写重试的 `writev`。两版均通过 golden 和独立 Python 读回。

- 大目录峰值 RSS 从 **168.27 MiB 降至 145.70 MiB**，C++ 累计请求字节减少约 **68 MiB**。编码加写入耗时中位数 **66.37 → 65.82 ms**，进程 CPU **75.20 → 77.96 ms**。
- 突发峰值 RSS 从 **238.03 MiB 降至 215.44 MiB**，C++ 累计请求字节减少约 **263 MiB**。进程 CPU **266.82 → 262.26 ms**，两组范围重叠。
- 保留减少复制的实现。当前测量定位到大帧编码/写入的内存成本，尚未隔离封存压缩成本，也没有足够的 CPU 收益支持修改字节协议。封存沿用现有配置的 level 3、checksum 和内容大小记录。现有 Python 封存 golden 测试通过。生产封存按现行计划流式计算摘要、fsync 和原子 rename，运行中无需读回验证。

## C0.4 冻结预算

目标机为既有 GreenCloud Tokyo 实测档位 **Linux x86_64、4 vCPU Intel Broadwell @ 2.0 GHz、5,878 MiB RAM**。硬件、105.41 MiB/h 封存观测和 2.5 GiB 临时封存空间来自 C0.1 固定提交的 `docs/benchmarks/m4.1-review-20261103.md`。旧 Python M4 第二轮的 CPU 109.16%、峰值 RSS 617.90 MiB 和最长封存 13.602 秒作为历史参照。以下是后续阶段的验收上限，本轮通过的是 macOS loopback 短测。

| 项目 | 冻结值与依据 |
| --- | --- |
| 负载与队列 | 用 C0.1 固定输入逐条对照正确性。容量压测以每秒 1 MiB 输入、每帧 8 KiB 的稳态背景，加每 60 秒一次 64 MiB WS 突发及一条 17,817,762 B 目录，注入 250 ms 存储停顿。峰值为 8,192 WS 条 / 64 MiB 加一条大目录，合计约 81 MiB 测试输入。入口队列上限 8,192 条 / 64 MiB payload，编码后单帧上限 32 MiB，目录按可用字节准入。接收缓冲、在途 worker 和编码暂存单列，持续过载按 C2 的失败和重连语义处理。 |
| CPU | 单核为 100%。空闲 60 秒平均 ≤5%，稳态 60 秒滚动平均 ≤120%，突发/轮换任意 1 秒窗口 ≤200%。依据历史稳态约 109% 并预留调度余量。C5 的同口径收益仍须超过测量波动。 |
| RSS | 稳态 ≤512 MiB，突发/轮换峰值 ≤768 MiB，覆盖 TLS、队列、编码、Zstd、SQLite 和线程。依据原型约 215 MiB 突发峰值和现有 Python 约 618 MiB 现场峰值。cgroup 内存另报，含 page cache 上限 2 GiB。 |
| 本地延迟与耐久 | 稳态热流 receive → append p99 ≤100 ms。注入突发 receive → append p99 ≤500 ms，队列在停顿解除后 ≤1 秒排空。fsync 间隔配置 1 秒，任一已接受记录 receive → 成功 fsync ≤1.25 秒，含最多 250 ms 的调度/提交余量。分别记录 receive、enqueue、append 与同步完成时间。 |
| REST 调度与 IP | 已到期且可准入的请求 due → dispatch p99 ≤100 ms、最大 250 ms，本地调度失约 0。单次传输超时 10 秒，最多 8 个并发 REST 请求。沿用固定提交的滚动共享配额 Spot 2,000 weight/min、USD-M/COIN-M 共用衍生品池 1,200 weight/min（常态计划 ≤1,000）、funding 400/5 min、统计 800/5 min、Spot 与衍生品各 180 次建连/5 min。控制消息每连接本地上限 Spot 5/s、USD-M 10/s，含 ping/pong，COIN-M 在 C3 核实前采用 5/s 保守上限。配额等待、网络超时、发布迟发分别计量。 |
| 网络与磁盘 | 入口应用 payload 60 秒滚动均值 ≤3 MiB/s，容纳稳态加每分钟一次约 81 MiB 突发。封存净增长 ≤5 GiB/24h，约为历史观测的两倍。短测突发用于内存与排空门禁，24 小时净增长按完整源集现场测量。轮换 900 秒 / 128 MiB；连续运行启动时内部可用 ≥18 GiB，包含 10 GiB 硬保留、2.5 GiB 封存临时空间、64 MiB 排空与 5 GiB 一日增长。低于阈值先告警，到硬保留前停采排空。归档含目标校验的吞吐 ≥10 MiB/s，即 5 GiB 日增量可在约 9 分钟转移。分别记录应用写入和块设备写入、fsync 与 WAL；归档独立计量。 |

预算作为 C1–C5 固定验收口径。超预算须记录实测原因，范围或硬件变动按明确变更重新对照。

## 复跑

使用 C0.1 的独立 Python 工作树和具备其依赖的 Python 3.12。由 S-BMDR 根目录执行，`scratch` 为独立临时目录，`py` 为虚拟环境中 Python 的绝对路径。CMake 用 pkg-config 发现依赖，现有版本记录在测量 JSON 中。C1 再固定跨架构工具链与 vcpkg baseline。

```sh
cmake -S prototype -B "$scratch/c0-release" -DCMAKE_BUILD_TYPE=Release \
  -DPython3_EXECUTABLE="$py" -DPYTHON_BASELINE="$scratch/python"
cmake --build "$scratch/c0-release"
ctest --test-dir "$scratch/c0-release" --output-on-failure
for n in 1 2 3; do
  "$py" prototype/verify.py "$scratch/c0-release/sbmdr-c0" "$scratch/python" \
    > "$scratch/c0-release-$n.json"
done
```

ASan/UBSan 和 TSan 分开构建，使用 Debug，分别设置 `CMAKE_CXX_FLAGS` 为 `-fsanitize=address,undefined -fno-omit-frame-pointer -DSBMDR_NO_NEW_COUNTER` 和 `-fsanitize=thread -fno-omit-frame-pointer -DSBMDR_NO_NEW_COUNTER`，运行相同 CTest。sanitizer 结果用于正确性检查，预算比较使用 Release。
