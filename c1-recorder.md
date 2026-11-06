# C1 最小端到端记录器与读取器

本文的早期本机/长测结果属于历史版本。当前优化、GreenCloud 逐次结果与未关闭门槛见[计划第十一节](cpp_rewrite_milestone_plan.md#cpp-performance-review-20261104)和[2026-11-06 结果](c1-performance-20261106.json)。

## 实现

- 公开入口使用 Spot BTCUSDT S02 100 ms 深度流与 S01 `exchangeInfo?symbol=BTCUSDT`。WS 使用 Boost.Beast TLS，REST 使用 libcurl multi。两个连接均校验证书，使用只读公开市场端点。完整响应仅保留一份 base64 主体表示。公开模式每小时发起一次 REST 请求，并在 HTTP 418/429 后按服务端指示冷却。完整响应可封存，截断响应记录传输失败并保留冷却约束。REST 工作线程跨请求复用 libcurl 连接缓存。[官方端点记录](docs/binance_sources.md)
- WS、REST、存储各占一个工作线程。拥有型 payload 经 8,192 条 / 64 MiB 队列移交，写线程分批消费。网络线程不写 Raw。存储端用单调时钟在最多约 900 ms 间隔调度 fsync，并计算真实接收至成功同步的延迟。SIGINT/SIGTERM 停止接收，等待在途 HTTP 完成，排空队列后封存。
- 首条记录才打开同源 Raw active 文件。时间或大小先到即轮换，默认 **900 秒 / 128 MiB**。写入时计算 Raw SHA-256，封存时流式压缩为 Zstd level 3 并计算压缩 SHA-256。压缩文件、manifest 和 Catalog 依序发布，最后移除已封存的内部 active。封存中断时保留 active，启动检查会提示处理残留。崩溃恢复和重连后自动补齐在 C2。
- SQLite 使用 WAL + NORMAL，记录进程启动、chunk 生命周期与摘要。独立读取器在只读 Catalog 下复用冻结的 Python Raw reader，检查压缩和解压哈希、CBOR、CRC、记录数量与身份，按 chunk 导出 NDJSON。

## 构建与运行

在 S-BMDR 根目录执行。Python 使用 [C0.1 的固定工作树](c0.1-baseline.md)及其依赖。`scratch` 为独立临时目录，`py` 为 Python 3.12 虚拟环境的绝对路径。系统依赖包括 Boost、libcurl、simdjson、nlohmann JSON、Abseil CRC32C、OpenSSL、SQLite 和 Zstd。

```sh
cmake -S prototype -B "$scratch/c1-build" -DCMAKE_BUILD_TYPE=Release \
  -DPython3_EXECUTABLE="$py" -DPYTHON_BASELINE="$scratch/python"
cmake --build "$scratch/c1-build"
ctest --test-dir "$scratch/c1-build" --output-on-failure
```

`prototype` 提供 `release` 与 `vcpkg-release` CMake presets。vcpkg 方案由 `prototype/vcpkg.json` 固定 baseline。使用该方案时在 `prototype` 内设置 `VCPKG_ROOT`，运行 `cmake --preset vcpkg-release` 并传入相同的 Python 参数。

公开采集只接受 Recorder 内部数据根下的独立 `S-BMDR` 子目录。路径示例适用于 macOS，Linux 交互根使用 `~/.local/share/BinanceMarketDataRecorder`，系统服务根使用 `/var/lib/binance-market-data-recorder`。公开采集启动要求至少 18 GiB 可用，降至 13 GiB 时停止接收并排空，为 10 GiB 硬保留留下封存和队列空间。

```sh
root="$HOME/Library/Application Support/BinanceMarketDataRecorder/S-BMDR/c1"
"$scratch/c1-build/sbmdr-c0" record --public "$root" 1000 /etc/ssl/cert.pem
PYTHONPATH="$scratch/python/src" "$py" prototype/read_raw.py "$root"
```

最后两个参数分别是 WS 帧数上限和信任的 PEM CA bundle。每秒在 stderr 输出采集/封存计数和 CPU、RSS、耐久摘要，退出时在 stdout 输出最终 JSON。NDJSON 中的 `raw_payload_base64` 是原 Raw payload，REST payload 内含响应主体表示。

## 验收

短测走真实 loopback 网络，核对时间轮换、默认 128 MiB 大小轮换、SIGTERM 排空、畸形 WS 保留、封存故障以及 Python 独立读回。C0 golden 同时覆盖跨构建的 Raw v1 字节一致性。公开 TLS 烟测用真实 Binance 数据走过两个 WS 记录和一次 BTCUSDT 目录响应。

macOS arm64 的 Release、固定 vcpkg Release、ASan/UBSan 和独立 TSan 均通过。Ubuntu 24.04 的 arm64 与 x86_64 通过同一个 Raw golden 及两项 CTest，x86_64 在本机 Docker 中模拟执行。固定 vcpkg 构建使用 baseline `afa249f12f52ed8a3b919f195495ba8b330288d1` 和本机 CMake 4.3.2。GreenCloud 的持续运行资格在后续阶段验证。

长测使用 46 分钟真实墙钟，固定 10 Hz WS，每帧 8 KiB，以及每秒一次 mock REST 目录响应。默认轮换配置，独立读回每条记录并核对 payload、身份、时间和 Catalog；负载限于 C1 单 WS/REST 场景。

```sh
SBMDR_LONG_SECONDS=2760 "$py" prototype/verify_recorder.py \
  "$scratch/c1-build/sbmdr-c0" "$scratch/python" > "$scratch/c1-46m.json"
```


[验收数据](c1-qualification.json)保留长测的二进制与源文件摘要、轮换状态采样、短测及公开烟测结果。运行从 2026-11-03 23:34:42（上海时间）开始，同一进程运行 **2,760.11 秒**。两路共读回 **30,344 条**，其中 WS 27,601 条、REST 2,743 条。20 个封存分片来自 3 次时间轮换、15 次大小轮换和退出时的 2 个分片。独立读回与 Catalog 计数完全一致。

| C1 长测指标 | 实测 | C0 门槛 |
| --- | ---: | ---: |
| 平均 CPU / 最大约 60 秒窗口 | 1.42% / 1.71% | 120% |
| 最大约 1 秒 CPU 窗口 | 8.89% | 200% |
| 峰值 RSS | 23.13 MiB | 稳态 512 MiB |
| receive → append p99 | 9 ms | 100 ms |
| receive → 成功 fsync 最大值 | 994 ms | 1,250 ms |
| 峰值队列 | 2 条 / 589,489 B | 8,192 条 / 64 MiB |
