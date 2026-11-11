# S-BMDR 运行手册

本手册只使用现有发布物：`/opt/sbmdr/releases/<version>`、`/etc/sbmdr/{config.json,roots.json}`、systemd 单元 `sbmdr-recorder@{spot,usdm}` 与 `sbmdr-archive.timer`。不引入 submodule、wheel 或 CI。

## 1. 原生构建与测试

在仓库根目录执行，`py` 为带 WebSocket/pyarrow 等依赖的 Python 3.12：

```sh
cmake -S prototype -B build-release -DCMAKE_BUILD_TYPE=Release \
  -DPython3_EXECUTABLE="$py"
cmake --build build-release -j"$(nproc)"
ctest --test-dir build-release --output-on-failure
```

`PYTHON_BASELINE` 默认指向仓库内 `vendor/python-baseline`；需要对照外部检出时才显式覆盖。ASan/UBSan 与 TSan 使用独立构建目录和 `-fsanitize=...`，产物只用于正确性回归。`LD_PRELOAD` 故障注入用例只在普通 Release 运行。

## 2. staging 准备

staging 目录（以下称 `$S`）必须是最终候选，安装脚本会逐项校验身份：

```text
$S/release/sbmdr-c0                     Release 二进制
$S/asan/sbmdr-c0                        ASan/UBSan 二进制
$S/tsan/sbmdr-c0                        TSan 二进制
$S/results/native-candidate-tests.json  三套矩阵的退出码、二进制/源码摘要
$S/results/source-ledger.json           C++ 源文件清单
$S/results/ctest-asan.exit              ASan 退出码文件（内容必须为 0）
$S/results/ctest-tsan.exit              TSan 退出码文件（内容必须为 0）
$S/results/ctest-asan-pinned.exit       冻结候选的 ASan 退出码（advance.py 等待）
$S/results/ctest-tsan-pinned.exit       冻结候选的 TSan 退出码（advance.py 等待）
$S/prototype/ $S/deployment/ $S/config/
$S/vendor/python-baseline/              含 BASELINE.json、src/、requirements/
$S/venv/                                仅用于准备 staging，不复制到 runtime
```

已通过的 normalize、archive-fault、history 报告直接复用，保留原文件名和原身份，不改名冒充本轮测试。资格安装使用 `--cpu-limit-percent 200`、`--rss-limit-mib 384`、`--max-drop-percent 0.10`，并在证据中记录。

## 3. 安装

安装前确认没有仍在采集或归档的实例。`install.py` 会再次停止 archive timer 和 archive worker，并拒绝在 recorder 仍 active 时继续。

```sh
systemctl stop sbmdr-archive.timer
systemctl stop sbmdr-recorder@spot sbmdr-recorder@usdm
systemctl show sbmdr-archive.service --property=ActiveState --value

python3 "$S/deployment/install.py" \
  --source "$S" \
  --c3-report /path/to/c3-release-60m/progress.json \
  --version 20261111-<commit>
```

安装顺序：

1. 停止 archive timer/worker，确认 recorder、archive 均 inactive。
2. 备份 `/opt/sbmdr/current`、`/etc/sbmdr`、systemd 单元与 drop-in、`/etc/logrotate.d/sbmdr` 到 `$S/results/install-backup-<UTC>/`。
3. 在新 release 目录准备二进制、脚本、配置、vendored Python 和 requirements lock。
4. 在最终 runtime 位置用 `python3.12 -m venv` 创建环境，按 `python/requirements/linux-x86_64-python312.lock` 使用 `pip install --require-hashes` 安装；不复制 staging 的 venv。
5. 注册新数据根和归档目标，检查 `bmdr` 身份下 imports。
6. 安装 unit 与 CPUWeight drop-in，删除遗留的 `sbmdr-recorder@.service.d/production.conf`，`daemon-reload` 后执行 `systemd-analyze verify`。
7. 用 `systemctl show` 检查最终 `Restart=no`、Spot/USD-M `CPUWeight=100/200`、archive `CPUQuota=50%`、slice `CPUQuota=175%`、recorder start/stop 300/180 s、archive start 600 s。
8. 原子切换 `/opt/sbmdr/current`，从新路径再做一次 import 检查。

任一步失败时脚本写入 `$S/results/deployment-failed.json`，恢复 `current`、`/etc/sbmdr`、遗留 drop-in 与 logrotate，并再次 `daemon-reload`；新 release/runtime 会删除以便重试。不要把“已切换 current”当作安装成功，以 `deployment.json` 和最终 systemd 属性为准。

安装会清空运行日志并把旧日志备份到 `$S/results/logs-before-<version>/`，同时把 logrotate 配置改名为 `sbmdr.qualification-disabled`，避免 `copytruncate` 截掉资格证据。资格结束、进入生产时由 `advance.py` 恢复。

## 4. 资格运行

```sh
python3 "$S/deployment/advance.py" \
  --source "$S" \
  --version 20261111-<commit> \
  --c3-report /path/to/c3-release-60m/progress.json \
  --cpu-limit-percent 200
```

`advance.py` 显式传 200；120 只是旧默认值，不得用于本轮。资格入口为：

```sh
/opt/sbmdr/current/venv/bin/python /opt/sbmdr/current/deployment/qualify.py \
  --release /opt/sbmdr/current \
  --config /etc/sbmdr/config.json \
  --root /var/lib/binance-market-data-recorder/S-BMDR/<label> \
  --evidence "$S/results/c4-c5-24h" \
  --seconds 86410 \
  --services \
  --rss-limit-mib 384 \
  --cpu-limit-percent 200 \
  --max-drop-percent 0.10
```

验收内容：Catalog/manifest/chunk 集合完全一致且全部终态；WS/REST 丢弃分项与 Catalog `INGRESS_DROP` 总和一致，分母是已接受 WS 记录；逐连接组检查 23h 换连原因，未达到 23h 的连接如实记录；每个 symbol 的 depth 在最新连接上重新桥接；归档目标 blocked 且有积压即失败。

## 5. 仅重验已完成采集

采集完整、仅验收脚本出错时，不重新采集：

```sh
/opt/sbmdr/current/venv/bin/python /opt/sbmdr/current/deployment/qualify.py \
  --release /opt/sbmdr/current \
  --config /etc/sbmdr/config.json \
  --root /var/lib/binance-market-data-recorder/S-BMDR/<label> \
  --evidence "$S/results/c4-c5-24h" \
  --seconds 86410 \
  --services \
  --rss-limit-mib 384 \
  --cpu-limit-percent 200 \
  --max-drop-percent 0.10 \
  --verify-only
```

`--verify-only` 只读取 `progress.json`、两份 recorder 摘要/状态日志和已停止的数据根，核对 release、config、root、时长后重跑 `verify_stopped`。它不启动服务、不改写采集 T0，结果写入 `verify-only.json`。若 `progress.json` 身份与实际 release 不一致，会直接拒绝。

同一份 PASS 报告的收尾入口：

```sh
python3 "$S/deployment/advance.py" \
  --source "$S" \
  --version 20261111-<commit> \
  --finish-only \
  --release /opt/sbmdr/current \
  --qualification-report "$S/results/c4-c5-24h/progress.json"
```

该入口要求报告 `PASS`、报告二进制与 `--release` 一致、`deployment.json` 的 evidence 根一致，然后才排空归档尾部并恢复生产 logrotate/重启策略。

## 6. 启停与状态

```sh
systemctl start sbmdr-recorder@spot sbmdr-recorder@usdm
systemctl start sbmdr-archive.timer
systemctl status sbmdr-recorder@spot sbmdr-recorder@usdm sbmdr-archive.timer
systemctl show sbmdr-recorder@spot --property=MainPID,NRestarts,ActiveState,Restart
```

停止：

```sh
systemctl stop sbmdr-archive.timer
systemctl stop sbmdr-recorder@spot sbmdr-recorder@usdm
```

日志：

- 记录器 stdout：`/var/log/sbmdr/{spot,usdm}.stdout.jsonl`
- 记录器每秒状态：`/var/log/sbmdr/{spot,usdm}.status.jsonl`
- 归档结果：`/var/log/sbmdr/archive.jsonl`

`status.jsonl` 中的 `queued_payload_bytes` 是计费后的入口队列字节；`seal_queue_depth/peak`、`seal_enqueue_wait_ns_*`、`seal_pending_raw_bytes`、`catalog_wait_ns_*` 分别对应封存交接、待封存 raw 和 writer 的 Catalog 等待。异步封存后不要再用 `writer_seal_ms` 解释 writer 阻塞。

## 7. 读回与手动归档

```sh
export PYTHONPATH=/opt/sbmdr/current/python/src
/opt/sbmdr/current/venv/bin/python /opt/sbmdr/current/prototype/operations.py verify "$ROOT"
/opt/sbmdr/current/venv/bin/python /opt/sbmdr/current/prototype/operations.py archive "$ROOT"
```

`verify` 对已归档分片使用外部 manifest 绑定校验，对本地分片及 REST、多连接、首尾、带 `ingress_drop`/`sequence_gap` 的分片逐帧读回；同时核对 Catalog 与 manifest 的 chunk 集合、终态、记录数、大小和 SHA。`archive` 每个市场一次最多运行 120 秒，输出 `exit_reason`；`TARGET_ABSENT`/`TARGET_NOT_READY`/`TARGET_LOW_SPACE` 且有积压时计为 blocked。

## 8. 回退到上一个 release

```sh
ls -1 /opt/sbmdr/releases
ln -sfn /opt/sbmdr/releases/<previous-version> /opt/sbmdr/current.new
mv -Tf /opt/sbmdr/current.new /opt/sbmdr/current
systemctl restart sbmdr-recorder@spot sbmdr-recorder@usdm
systemctl restart sbmdr-archive.timer
```

回退只切换 `current` 链接；数据根和归档目标不删除。回退后确认两个实例 `ActiveState=active`、`NRestarts=0`，归档日志继续增长。生产重启策略由 `production.conf` 提供；资格候选必须保持 `Restart=no`。

## 9. 磁盘紧急停止

记录器在系统盘可用 ≤13 GiB 时停止接收、封存并退出。处理顺序：

1. `systemctl status sbmdr-recorder@spot sbmdr-recorder@usdm`
2. 确认 `/opt/sbmdr/current` 指向正确 release，`/etc/sbmdr/roots.json` 指向本次根。
3. 对两个根执行手动归档，直到 `operations.py verify` 能看到 `LOCAL_DELETED` 且本地 `SEALED` 归零；归档 `exit_reason` 为 blocked 时先处理目标盘，不得当作成功。
4. `df -h /` 确认可用 ≥15 GiB 后再启动记录器；归档 timer 必须同时启动。

不要删除未完成目标校验的源文件，不要为腾空间删除 `data/active` 或 `.partial`。
