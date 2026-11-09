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

`PYTHON_BASELINE` 默认指向仓库内 `vendor/python-baseline`；需要对照外部检出时才显式覆盖。ASan/UBSan 与 TSan 使用独立构建目录和 `-fsanitize=...`，产物只用于正确性回归。

## 2. 准备 staging 与安装

`install.py` 要求 staging 目录包含 `release/sbmdr-c0`、`asan/`、`tsan/`、`results/native-candidate-tests.json`、`results/source-ledger.json`、`results/ctest-asan.exit`、`results/ctest-tsan.exit`、`prototype/`、`deployment/`、`config/`、`vendor/python-baseline/` 与可解析的 `venv`。

```sh
python3 deployment/install.py \
  --source /path/to/staging \
  --c3-report /path/to/c3-release-60m/progress.json \
  --version 20261110-<commit>
```

安装会创建新 release、新数据根和归档目标，切换 `/opt/sbmdr/current`，安装 systemd 单元与 `CPUWeight` drop-in，并清空运行日志（旧日志备份到 staging 的 `results/logs-before-<version>/`）。

## 3. 启停与状态

```sh
systemctl start sbmdr-recorder@spot sbmdr-recorder@usdm
systemctl start sbmdr-archive.timer
systemctl status sbmdr-recorder@spot sbmdr-recorder@usdm sbmdr-archive.timer
systemctl show sbmdr-recorder@spot --property=MainPID,NRestarts,ActiveState
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

`status.jsonl` 中的 `queued_payload_bytes` 是计费后的队列字节，`dropped_total` 是 WS+REST 累计丢弃数。

## 4. 读回与手动归档

```sh
export PYTHONPATH=/opt/sbmdr/current/python/src
/opt/sbmdr/current/venv/bin/python /opt/sbmdr/current/prototype/operations.py verify "$ROOT"
/opt/sbmdr/current/venv/bin/python /opt/sbmdr/current/prototype/operations.py archive "$ROOT"
```

`verify` 对已归档分片使用外部 manifest 绑定校验，对本地分片和 REST/多连接/首尾分片逐帧读回；`archive` 每个市场一次最多运行 120 秒。

## 5. 回退到上一个 release

```sh
ls -1 /opt/sbmdr/releases
ln -sfn /opt/sbmdr/releases/<previous-version> /opt/sbmdr/current.new
mv -Tf /opt/sbmdr/current.new /opt/sbmdr/current
systemctl restart sbmdr-recorder@spot sbmdr-recorder@usdm
systemctl restart sbmdr-archive.timer
```

回退只切换 `current` 链接；数据根和归档目标不删除。回退后确认两个实例 `ActiveState=active`、`NRestarts=0`，归档日志继续增长。

## 6. 磁盘紧急停止

记录器在系统盘可用 ≤13 GiB 时停止接收、封存并退出。处理顺序：

1. `systemctl status sbmdr-recorder@spot sbmdr-recorder@usdm`
2. 确认 `/opt/sbmdr/current` 指向正确 release，`/etc/sbmdr/roots.json` 指向本次根。
3. 对两个根执行手动归档，直到 `operations.py verify` 能看到 `LOCAL_DELETED` 且本地 `SEALED` 归零。
4. `df -h /` 确认可用 ≥15 GiB 后再启动记录器；归档 timer 必须同时启动。

不要删除未完成目标校验的源文件，不要为腾空间删除 `data/active` 或 `.partial`。
