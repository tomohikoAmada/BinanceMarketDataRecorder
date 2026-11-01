# P4：BTC 全市场 2h → 24h

状态（2026-11-01）：**启动准备与实测修正中，当前无有效长跑；P4 未通过。**
本次用户已明确授权完成 P4，包含计划要求的 GreenCloud 实际运行。P0–P3
保持独立验收通过；不开始 P5，也不声称生产资格或10币容量。

## 当前候选与范围

开发分支 `codex/p4-single-asset`，基于已合并 main `ff865cd`。准备实测发现的
修正正在本分支验证，新安装版本会单独冻结。全部 BTC Spot base-asset 报价对、
USD-M/COIN-M 永续与交割；目录实测24个 TRADING 产品，原计划2,235条源。
沿用 P2 的2s USDT/3s USDC/30s其他导数 D06 cadence，无观察周期缩减。

除 ADR-0055 的 U/C 交割 premium K线外，owner在本聊天明确批准额外暂缺
**USD-M交割公共ADL**，见[ADR-0057](../../adr/0057-initial-production-dated-adl-exception.md)。
适用性仍 UNKNOWN；永续ADL继续，其他源不删除。新候选应为2,233条启用源，
另列62条明确未采集项（60条交割premium与2条交割ADL），以冻结目录为准。

## 保留的准备尝试

主机 `greencloud-tokyo-01`，Ubuntu24.04/Python3.12.3/x86_64，4vCPU/约5.74GiB RAM。
2026-11-01T11:46:17Z，原服务 inactive/disabled，原归档 timer active/enabled；
系统盘可用23,956,934,656B，归档盘可用2,046,270,267,392B。无旧服务或配置替换。

| 运行ID | 结果与保留证据 |
| --- | --- |
| `p4-20261101-btc` | 默认16MiB帧上限拒绝17,817,762B的Spot目录表示；服务报错退出。45个新封存文件独立v2校验全部完成，无校验错误。无正式T0。 |
| `p4-20261101-btc-c` | `8bdae20`在12:13:18.836073348Z建立正式T0，随后来源日志待写队列持续增至52,997条，实时报告出现大量未知尾部。主动正常停止；该窗口不获2h/24h通过。874个新封存文件全部v2独立校验，无错误。旧状态/样本/Raw保留。 |
| `p4-20261101-btc-b` | 帧上限32MiB；24产品全部接入。启动检查发现S09 JSON数组含空格被拒绝、D16包含前一周期导致窗口误判，以及2个交割ADL `Invalid symbol`。11:57UTC主动正常停止；337个新封存文件v2校验全部完成，无错误。无正式T0。 |

每次的内部数据/Catalog/证据在
`/var/lib/binance-market-data-recorder/<运行ID>/`；注册归档只在
`/srv/recorder-data/recorder-archive/<运行ID>/`，有独立卷UUID/marker/storage_id。
完整失败日志、冻结目录/源清单、请求参数/实际响应和校验结果均保留在各自`evidence/`。
原 Raw、Catalog、部署环境及其他本地 worktree 保留。准备服务为 runtime-only static unit，
停止后不会开机自启；原 Recorder 仍 inactive/disabled。

## P4 实测修正

- S09批次使用无空格JSON数组，并按官方每symbol2、上限40计入请求预算。
- D16复用既有taker窗口政策：请求多一个周期/limit3；保留完整响应与实际请求，
  显式记录实际评价的闭合区间。仅允许恰好前一周期的重叠；没有区间内记录的非空
  旧响应不判新鲜，任意更早/未来记录仍失败。USD-M与COIN-M均覆盖。
- 新配置显式关闭交割ADL，默认配置保留原完整目标；未采集项按ADR-0057披露。
- status公开已有WS group接收/持久化/待写边界计数，供长跑与正常排空核对，
  不增加逐帧扫描、哈希或消费者工作。

## 运行方法与续接

[`freeze.py`](freeze.py)、[`install_units.py`](install_units.py) 和
[`observe.py`](observe.py) 是本次单一P4运行的操作脚本，不是通用验收平台。
源清单须在启动前独立按完整产品目录生成，不能从已经成功的源反推。
安装锁来自`requirements/linux-x86_64-python312.lock`；wheel与选定源码及安装后的
Python文件逐字节核对。每次改源码/配置都使用新运行目录和计时窗口。

准备上限900s，有独立原生截止timer；正常开启独立新文件校验、报告、每分钟归档。
正式T0需全部预期源满足P3来源状态、目录/产品准入正确、depth snapshot bridge就绪；
安静流不以尚无首笔成交判失败。冻结源清单内每源分别99.5%，未发现源不能消失。
2h冻结新文件校验边界并检查资源，健康则同一进程继续；24h继续补齐终点边界，
正常停止/排空后记录尾部。额外25h原生截止防止聊天/observer异常导致无限运行。
后台校验异常只补验原文件，不因此重采一天。

当前聊天已建立15分钟自动跟进 `recorder-p4-btc-2h-24h`，只在检查点、故障、
需要决定或完成时通知。正式开始/2h/24h时间、最终结果与限制仍待实际证据补齐。

## 本机验证（新候选部署前）

`pytest -q`：2,450 passed /24 skipped /5 stress未选，189.68s；跳过的是显式online/platform项。
59项定向检查含15项新回归通过。Ruff、mypy（358文件）、M0合同和diff检查通过。
完整测试日志保留在`/private/var/tmp/recorder-p4-pytest-20261101.log`。这些离线结果不证明VPS容量/24h连续性。

## C窗口发现与后续候选

C的来源状态队列每轮只刷128条，无法跟上2,233源的定期checkpoint。停止排空后再形成的完整区间证据与在线未知尾部分别保留；不能把排空后的报告当成当时没有后台积压。新候选保留128条/事务，以32批或100ms为每轮上限（批间让出），顺序/失败证据不丢弃。另将既有MetricsRecorder接到新路径的physical Raw stream写入/操作/封存回调，每10秒批量刷新、正常停止最终刷新；按逻辑源的连续性仍由来源日志评价，不重复解析combined JSON。旧C日报只有归档/删除等指标，不能据其0输入宣称没有采集。

`8bdae20`精确提交双平台CI [41305329700](https://github.com/tomohikoAmada/BinanceMarketDataRecorder/actions/runs/41305329700)成功；它仍因实际后台积压不获P4通过。新候选须另行冻结/安装/计时。独立观察器已补RSS/归档积压、完整预期集合及scope检查；C曾按原T0恢复观察器，未重启采集，原样本与新增segment都保留。

后续候选的本机完整离线套件：2,451 passed /24 skipped /5 stress未选，189.10s；最终定向19项通过，Ruff/mypy/M0/diff通过。新日志在`/private/var/tmp/recorder-p4-backlog-pytest-20261101.log`。前一8bdae20 CI完整解码日志已保留在`/private/var/tmp/recorder-p4-ci-8bdae20-{0,1}.json`，macOS2450/Ubuntu2446，全步骤成功；不将其当作后续候选CI。
