# Official Binance Sources

## P2-R2 official applicability follow-up — 2026-10-29

The index was read first; observations below were retrieved on 2026-10-28 UTC.
[Source metadata](milestones/P2/r2-20261028/official-sources.json) records exact byte
counts, representations and local full-source paths. Digests identify downloaded
text or decoded connector files, not original HTTP wire bytes. No downloaded
content was executed and no llms-full was read.

| Source | Retrieved UTC | Content SHA-256 | Conclusion |
| --- | --- | --- | --- |
| [Agent Native index](https://developers.binance.com/en/docs/llms.txt) | 2026-10-28T15:16:15Z | `b7051e1b961a4b221a5e5fcd47439221d2ade5222cc0bad59c1dc84a505d5482` | Locates native paths; no dated eligibility rule. |
| [CM/UM integration C.3](https://developers.binance.com/en/docs/products/derivatives-trading-coin-futures/Important-CM-UM-Integration-Notice.md) | 2026-10-28T15:23:57Z | `7b2f3c49aec8d1ab2d9ca2d1295ae1c0c8a54a958ee01e8aa23ecd03568eaa98` | Both premium paths accept UM/CM symbols; contract-type eligibility unspecified. |
| [USD-M shared changelog](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/change-log.md) | 2026-10-28T15:23:58Z | `05cd6861a8210185f498ab6f7d2248c3d58ee0202d4707f1c80a877fc59a8bb8` | No premiumIndexKlines dated/perpetual rule found in the selected search. |
| [COIN-M shared changelog (same bytes)](https://developers.binance.com/en/docs/products/derivatives-trading-coin-futures/change-log.md) | 2026-10-28T15:23:58Z | `05cd6861a8210185f498ab6f7d2248c3d58ee0202d4707f1c80a877fc59a8bb8` | Same content as the USD-M URL, evaluated once. |
| [Agent Native discovery](https://developers.binance.com/en/docs/agent-native/overview.md) | 2026-10-28T15:23:59Z | `7848052093045381e874dcd826d3cd8a1fb73706f71ed10a8b6607fbcf701a22` | Discovery guidance only; not endpoint eligibility. |
| [COIN-M Python market API](https://github.com/binance/binance-connector-python/blob/master/clients/derivatives_trading_coin_futures/src/binance_sdk_derivatives_trading_coin_futures/rest_api/api/market_data_api.py) | 2026-10-28T15:27:20Z | `684f233e18a6440254daeedfa6f99ea2c5861c85fd15c5be867ba642a6ca650a` | symbol/interval + optional time/limit, no alternate pair/contractType or eligibility validation. |
| [USD-M Python market API](https://github.com/binance/binance-connector-python/blob/master/clients/derivatives_trading_usds_futures/src/binance_sdk_derivatives_trading_usds_futures/rest_api/api/market_data_api.py) | 2026-10-28T15:27:20Z | `d783bb54426876450e148c11c76d4f9784b45dd40d1026a7e81ceb0cc4268182` | Same parameter boundary; SDK behavior does not define market eligibility. |
| [Python connector README](https://github.com/binance/binance-connector-python/blob/master/README.md) | 2026-10-28T15:45:06Z | `381dc82aac2e997d63a785d8cfbcfaf718da605ea8bfb98dd48b8ddb6831eceb` | Contributing section provides an issue discussion/bug-report venue. |

The catalogue evidence already retained in [the earlier P2 source record](milestones/P2/closeout-20261028/official-sources.json)
also does not establish dated eligibility. Delivery-specific funding nulls on
premiumIndex belong to another endpoint. No unsupported-to-not-offered inference
or substitute series is authorized. [New finite public HTTP observations](milestones/P2/r2-20261028/raw-http-diagnostic.json)
and [current selected directory facts](milestones/P2/r2-20261028/directory-validation.json)
are diagnostic observations, not normative applicability. R2 stays OPEN; the
[clarification draft](milestones/P2/r2-20261028/binance-clarification-draft.md) is unsent.

## P2 独立复审与 D06 预算研究（2026-10-28）

### 日度文件存在性补充核验

2026-10-28（上海）重新读取官方 `binance/binance-public-data` README，并以无凭据
HEAD 核验以下历史样例 ZIP，GET 其对应 `.CHECKSUM`；全部返回 HTTP 200。
ZIP 正文未下载，下表 SHA-256 是实际取得的校验文本的摘要，不是对 ZIP 内容完成了校验。
这确认官方日度逐笔文件实际存在，不证明每个目标产品/日期/字段均完整可用。

| 官方样例 / 核验日期 | 请求 UTC | CHECKSUM 文本 SHA-256 | 结果 |
| --- | --- | --- | --- |
| [USD-M BTCUSDT 2025-01-01](https://data.binance.vision/data/futures/um/daily/trades/BTCUSDT/BTCUSDT-trades-2025-01-01.zip)、[校验文本](https://data.binance.vision/data/futures/um/daily/trades/BTCUSDT/BTCUSDT-trades-2025-01-01.zip.CHECKSUM) | 2026-10-27T16:21:52.799929Z | `c721f59b57c2c1c369d99063272b4cb93b812da45b104ae88e19f8da5fc39157` | HEAD 200，15,105,022 bytes；CHECKSUM GET 200 |
| [COIN-M BTCUSD_PERP 2025-01-01](https://data.binance.vision/data/futures/cm/daily/trades/BTCUSD_PERP/BTCUSD_PERP-trades-2025-01-01.zip)、[校验文本](https://data.binance.vision/data/futures/cm/daily/trades/BTCUSD_PERP/BTCUSD_PERP-trades-2025-01-01.zip.CHECKSUM) | 2026-10-27T16:21:52.800231Z | `9e01995b65e1007dc17809ddd7c0625b691e6e871c4e89a384d7276293cb1964` | HEAD 200，1,652,988 bytes；CHECKSUM GET 200 |

本次先读取 Agent Native index，再核对选定产品页、整合说明及官方批量数据源码。
下列 UTC、摘要及表示完整记录于 [P2 review-sources.json](milestones/P2/review-sources.json)；
检索发生于 2026-10-27 UTC，报告日期为上海时间次日。没有访问账户或市场采集接口、
执行下载代码或读取 llms-full。文档更新不授权 6 秒采样或生产运行。

| 来源 | 获取 UTC | SHA-256 / 表示 | 结论 |
| --- | --- | --- | --- |
| [当前 index](https://developers.binance.com/en/docs/llms.txt) | 2026-10-27T15:13:25.820554Z | `533716d1dfd4a505e9d4f52e3de850c6048f775b17e6c1d0bef0c169f532cdd4`，下载文本 | 定位 recent trades、premium bars 及 CM/UM 整合说明 |
| [Spot REST](https://developers.binance.com/en/docs/products/spot/rest-api.md) | 2026-10-27T15:54:35.269613Z | `25f9203680b6c2570113c54fb449a119dc7482f125b790a6a27ce572b4665dec`，下载 Markdown | 429/418 的 Retry-After 表示应等待秒数；固定 120 秒不能替代实际期限 |
| [CM/UM 整合说明](https://developers.binance.com/en/docs/products/derivatives-trading-coin-futures/Important-CM-UM-Integration-Notice.md) | 2026-10-27T15:54:35.225260Z | `7b2f3c49aec8d1ab2d9ca2d1295ae1c0c8a54a958ee01e8aa23ecd03568eaa98`，下载 Markdown | UM/CM 共用 IP 2,400 weight/min；部分跨市场路径可接受两类 symbol，但不能据此推导交割 premium 的适用规则 |
| [UM general info](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/general-info.md) | 2026-10-27T15:54:35.256107Z | `7026aa5ae78b20df3e15349dfdb37f92866d563131cb551ff6eb538e58343a65`，下载 Markdown | 请求按 IP 计量，429 应退避；持续违规可能封禁 |
| [UM REST](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data)、[CM REST](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-coin-m-futures/api/rest-api/market-data) | 2026-10-27T15:13:25Z–15:59:05Z | `1e70d3f2720438f80e612c7fc4408a616b9becb2a24f230c24afd85c924823f3`，组合 web 工具摘录文本，非 HTTP 原文 | recent trades 的 weight=5、limit≤1,000，无翻页参数；UM historicalTrades 需 API Key，不能引入本项目 |
| [UM WS](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/market)、[CM premium REST](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-coin-m-futures/api/rest-api/market-data) | 2026-10-27T15:13:25Z–15:59:05Z | `2dc2d19cde45ae5b42df82bbc085111b4a1ccf20808a3e7950b2924987bd2212`，组合 web 工具摘录文本，非 HTTP 原文 | aggTrade 是聚合流，不能反推逐笔；CM premium 页未明确所有交割产品的适用规则，R2 保持开放 |
| [官方批量文件 README](https://github.com/binance/binance-public-data/blob/d571690eb66760763b0bd6e27e8cd35603c7e244/README.md) | 2026-10-27T15:13:25Z–15:59:05Z | `2e133d9945a9263a02781369078e72805eff074680f305cd5460016471c6caad`，connector 解码文件文本 | 日文件次日发布，带文件校验；所列合约 trades 列未列 isRPITrade，实际产品/字段覆盖待验证 |
| [官方下载源码](https://github.com/binance/binance-public-data/blob/d571690eb66760763b0bd6e27e8cd35603c7e244/python/download-trade.py) | 2026-10-27T15:13:25Z–15:59:05Z | `9acd63d2dca09dd198949a28e00a0a54f13100a70f8b24ac932678e14c7e02b6`，connector 解码文件文本 | 可按产品、日期获取文件及校验文件；仅阅读，未运行或整仓引入 |

[研究结论](milestones/P2/d06-budget-research-20261028.md)区分源文档事实和方案推断：
按产品分配频率和日文件补充是候选，不是已验证的数据完整性保证。文件校验不证明
交易所逐笔发布完整，也不能替代实时接收时间或连续性。

## Retention discussion: candle boundaries and recent trades — 2026-10-27

Evidence for the retention discussion and subsequent plan update; no endpoint implementation.
The current Agent Native index was read before the selected product pages.

| Source / representation | Retrieval UTC | SHA-256 | Conclusion |
| --- | --- | --- | --- |
| [Agent Native index](https://developers.binance.com/en/docs/llms.txt), downloaded text, 174759 bytes | 2026-10-27T05:23:35.814051+00:00 | `81d0f32aabb5c313ed7e94cf61c5bc1b935a094d5d3cbf32274586bf4a0da77b` | Index lists the Spot kline streams and USD-M recent-trades family; it is not a substitute for endpoint semantics. |
| [Spot streams](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/ws-streams/~) and [USD-M market REST](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data), combined serialized web-tool excerpts with retrieval bounds, 10773 bytes | 2026-10-27T05:26:11Z–2026-10-27T05:26:14Z | `e2300533f129193a9c15b5dff42d2aabba94682330d6387110c4173b4c086253` | Spot kline has interval/start/close times and boolean `x` for whether that candle is closed. USD-M `/fapi/v1/trades` accepts symbol/limit (default 500, max 1000), returns recent records with IDs, and does not expose a fromId parameter on this endpoint. It is not an unlimited historical feed. |

The excerpt digest covers the [retained web-tool excerpts](research/2026-10-27/binance-retention-excerpts.json),
originally `/tmp/bmdr-retention-discussion-20261027-web.json`, not original HTTP bodies.
Direct HTTP downloads of the two catalogue pages returned empty bodies
and were excluded as semantic evidence; rendered official-page excerpts were used.
No `llms-full.txt`, account endpoint, market-data request or downloaded code was executed.
Polling overlap and reduced duplicate writes are design inferences; this check does
not establish a live capture guarantee or authorize dropping required observations.

## P0 PR #86 review: liquidation semantics — 2026-10-26

Review [R4](milestones/P0/review-20261026.md) finds conflicting USD-M sampling
descriptions: the current catalog says latest, while the 2026-04-10 changelog
(effective 2026-04-14) says largest within 1000ms. The changelog bytes match the
pre-milestone research snapshot. This is an unresolved coverage-contract issue,
not evidence of a dropped payload. Both sources describe a representative feed,
not an exhaustive liquidation ledger. Do not infer COIN-M behavior from the USD-M
change. D11 is now split into U/C observations with an explicit unresolved selection
rule. Raw observation identity is retained; latest/largest is not an authority
for completeness or derived statistics. See the [R4 correction](milestones/P0/coverage.md)
and [fresh source record](milestones/P0/corrections-20261026/official-sources.json).

| Source | Retrieval UTC | SHA-256 and representation |
| --- | --- | --- |
| [USD-M liquidation catalog](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/market) | 2026-10-26T14:19:55Z–14:19:59Z | `14cc2220cb690489af9d9729061f67f1d5347ed8c294f22ec9c42edc610b5f1f`, serialized web-tool excerpt JSON, not original HTTP bytes; `/private/tmp/bmdr-pr86-review-market-fresh.json` |
| [Shared derivatives changelog, USD-M entry](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/change-log.md) | 2026-10-26T09:58:39.936776+00:00 | `7da6939ae9e4e9b9c9dbdbabba6a4b582fa08d3af23c4bd952546f140e331863`, downloaded Markdown, 113183 bytes; `/private/tmp/bmdr-pr86-review-changelog.md` |

The P0 current-index record below selected the affected official sources. An empty
direct catalog HTTP response was excluded; it supplies no semantic evidence.
No downloaded text was executed and no exchange market-data endpoint was called.

## P0 official source refresh — 2026-10-26

The [52-family first-release coverage inventory (53 rows after D11 split)](milestones/P0/coverage.md) and
[retrieval record](milestones/P0/official-sources.json) map Spot/USD-M/COIN-M
families, frequencies, fields, sharing, permissions, deduplication and gaps.
Current rendered USD-M market documentation requires API keys for the two top-trader
ratios and historical trades; COIN-M requires a key for historical trades but its
top-ratio pages show public query parameters. Spot historical-trade pages currently
show no key header; do not transfer derivatives permissions by analogy. P2 must use
current per-endpoint public semantics. No authenticated request is introduced.

Selected `.md` catalogue URLs returned the same HTML fallback shell and were rejected
as semantic evidence. The current official index/full API summaries and rendered
endpoint pages were used instead; hashes and retrieval limitations are explicit.
Public ADL risk is symbol-level and published every 30 minutes, distinct from
account ADL quantiles. Original model-based REST provenance remains labelled until
P2 implements raw HTTP body capture. This refresh performs no production collection.

## Pre-milestone scope research — 2026-10-26

Owner scope: original public data for Spot, USD-M and COIN-M perpetual and dated futures; options deferred. This is documentation research, not endpoint runtime qualification. The current index was read first; selected response bytes were hashed. No whole `llms-full.txt` fetch.

Details: [research source manifest](research/2026-10-26/binance-sources.json). Temporary byte cache: `/private/tmp/bmdr-preplan-20261026-zcd_9dlb/sources/`.

| URL | Retrieved UTC | Response SHA-256 | Conclusion |
| --- | --- | --- | --- |
| https://developers.binance.com/en/docs/llms.txt | 2026-10-26T03:52:44.572133+00:00 | e669de0fc0ee2030e81b56ab086211f62a74f0ab48ccd190e05d11ddaf37d63b | Current index identifies Spot, USD-M and COIN-M public data families; options deferred by owner. |
| https://developers.binance.com/en/docs/products/derivatives-trading-coin-futures/common-definition.md | 2026-10-26T03:57:12.534030+00:00 | 2b1c74158ccaac85c0b0f2d2c4f9f1149180658f5e93a3c6c2c28eadbadac3e0 | COIN-M perpetual and quarterly dated contract identities/statuses require a separate adapter. |
| https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/common-definition.md | 2026-10-26T03:57:12.810787+00:00 | 52706283c94a7b6c80714aee009e4c9bf1b367979612903753e52bd935c878d1 | USD-M perpetual and dated contract types are distinct; do not match every contract by symbol==underlying pair. |
| https://developers.binance.com/en/docs/products/spot/web-socket-streams.md | 2026-10-26T03:57:13.099511+00:00 | 555f9ed488d94665fc6c6dc3e7645d90546d2d179d5083f0289a8a9a5f538a35 | Spot stream/connection lifecycle and raw capture baseline reference. |
| https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/change-log.md | 2026-10-26T03:57:13.517186+00:00 | 7da6939ae9e4e9b9c9dbdbabba6a4b582fa08d3af23c4bd952546f140e331863 | Shared derivatives changelog records stream routing/data-family changes; inspect exact endpoint before implementation. |
| https://developers.binance.com/en/docs/products/derivatives-trading-coin-futures/change-log.md | 2026-10-26T03:57:13.800877+00:00 | 7da6939ae9e4e9b9c9dbdbabba6a4b582fa08d3af23c4bd952546f140e331863 | Same shared changelog response bytes as USD-M URL; not independent corroboration. |

## Public data coverage review — 2026-10-04

Owner-requested verification of the “归档流程说明” conversation used the current
Agent Native index first, selected developer-portal pages and official public-data
documentation. Findings are in
[the coverage review](reviews/2026-10-04-data-coverage-gap-review.md).
Evidence cache:
`/Users/amada/Library/Caches/BinanceMarketDataRecorder/data-coverage-review-20261004`.

Hashes below bind the stated representation. “Extract” means the saved web
tool's extracted page text, not original HTTP bytes. Raw markdown/README and
index entries bind downloaded response bytes. Empty catalog responses and generic
HTML fallback pages were retained in the cache but are **not** semantic evidence.
The obsolete root `/llms.txt` returned404; the current index is `/en/docs/llms.txt`.
Only selected documentation was read; `llms-full.txt` was not downloaded.

| Source / representation | URL | Retrieved UTC | Content SHA-256 | Conclusion |
| --- | --- | --- | --- | --- |
| Agent Native index / bytes | https://developers.binance.com/en/docs/llms.txt |2026-10-04T05:03:08.881406Z|754db774d164d0490e777ae4f89b77bafce73468863327f856e2bfd34dc314e6|Spot trade/block/reference streams and USD-M REST trade/RPI/risk/index families exist; USD-M inventory does not establish `@trade`.|
| Spot changelog / bytes | https://developers.binance.com/en/docs/products/spot/CHANGELOG.md |2026-10-04T05:05:16.420840Z|45e96f38f0deeb44bc4e38975f74cd1ac44728fec37d68b450034ae8b326e8f2|May2026 introduced block-trade stream/history; finer SBE depth cadence is a separate transport capability.|
| USD-M changelog / bytes | https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/change-log.md |2026-10-04T05:05:16.460989Z|cfed5a72a9c270d1b5b2edf66d4d4de9908b82352cb87962bcfc291a907cfb3c|Normal depth excludes RPI; dedicated depth exists; `nq` is normal quantity in aggregates; liquidation description changed to largest per1000ms.|
| Official public-data README / bytes | https://github.com/binance/binance-public-data/raw/refs/heads/master/README.md |2026-10-04T05:05:17.575366Z|2e133d9945a9263a02781369078e72805eff074680f305cd5460016471c6caad|Daily/monthly Spot/futures trades and bars support separate historical imports. Documented futures CSV does not promise per-trade RPI flags or live receive clocks.|
| Spot price-range rule / bytes | https://developers.binance.com/en/docs/products/spot/faqs/price_range_execution_rules.md |2026-10-04T05:11:33.759294Z|7fd9ed8f0fd13f4bb997e022c2eca2009e96be4203c91b07a8f1b4d90e3f39c2|Reference price, calculation configuration and execution rules are separately published state; formulas require their defined inputs/rounding.|
| Spot stream reference / extract | https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/ws-streams/~ |2026-10-04T05:13:39.615Z|1a41287ceae85bd434c5eddd65f4fbc9f690a507368dab4b507df7b694298897|Individual trade, block-trade and reference-price streams are separate inputs.|
| USD-M market reference / extract | https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data |2026-10-04T05:13:39.625Z|939574755ab62a11f9c12c8745d1dba982f9655ba5d468c38bbe4d6ef0c3d5ed|Recent individual trades include `isRPITrade`; augmented RPI depth hides crossed levels; symbol-level ADL and normal aggregate quantity are independently defined.|
| Options market reference / extract | https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-options/api/rest-api/market-data |2026-10-04T05:13:39.625Z|5a2f963ea1136995909822372dcfe3508616f1aec8a5f74c8fda1659b649d91e|Option marks/IV/Greeks/OI are a separate unimplemented market family.|

Six bounded **unsigned public GETs** from the VPS used the installed M20
ProxyPolicy and the existing config's direct mode (environment proxies ignored).
No API-key header, account/order endpoint, credential store, stream subscription
or service control was used. These inspection responses were saved locally as
review evidence, not inserted into production Raw/Catalog.

| Public observation | URL | Retrieved UTC | Response-byte SHA-256 | Result |
| --- | --- | --- | --- | --- |
| BTC Spot reference |https://api.binance.com/api/v3/referencePrice?symbol=BTCUSDT|2026-10-04T05:10:37.133697Z|4205a383558724ab7eb453dd4bb62e7d81bdfbcbaceeeaf3d200da345a4e4a8a|HTTP200/non-null|
| BTC reference calculation |https://api.binance.com/api/v3/referencePrice/calculation?symbol=BTCUSDT|2026-10-04T05:10:37.204081Z|c88b4e24a8fac60c24d52f723918a10e5addc8fc3645811469ec558d75484ce1|ARITHMETIC_MEAN/80×3750ms|
| ETH Spot reference |https://api.binance.com/api/v3/referencePrice?symbol=ETHUSDT|2026-10-04T05:10:37.277966Z|5dd6f0afd71dd877d0b8984615ba5a61072fbbcfd77fa6c06ea6165a764ca656|HTTP200/non-null|
| ETH reference calculation |https://api.binance.com/api/v3/referencePrice/calculation?symbol=ETHUSDT|2026-10-04T05:10:37.361671Z|c5e237715cfadde6b38a91a34394369fbab9d5d5f7cccaec5ff8c27b26fcb01d|ARITHMETIC_MEAN/80×3750ms|
| BTC Spot execution rules |https://api.binance.com/api/v3/executionRules?symbol=BTCUSDT|2026-10-04T05:14:58.842435Z|e05d1c8f0221689f82a0f68e3d15e40991879c68ae1cd065dd78a57d5f72ac48|HTTP200/PRICE_RANGE|
| ETH Spot execution rules |https://api.binance.com/api/v3/executionRules?symbol=ETHUSDT|2026-10-04T05:14:58.911081Z|519b84ff8d200e60209201219a33b9c41b97fd9b5eed9c7d5eb05e2f560c556d|HTTP200/PRICE_RANGE|

Original immutable artifacts and Q5 processes were inspected read-only.
These observations grant no formal time credit or continuous completeness claim.

## Formal M22.9 2h eligibility refresh — 2026-09-11

Immediately before the owner-authorized Formal 2-hour start, direct unsigned
public market-data requests revalidated the exact configured symbols. The
record is preserved at
`/srv/recorder-data/recorder-archive/acceptance/m22.9/formal-2h-20260911T132845Z-5b4fd719-646792f2/operator-evidence/public-eligibility.json`,
SHA-256
`ef87ac01513d9f56be49226ab2308e9048756adb6f351f61e862e8ccac07faf9`.

| Source | URL | Retrieved UTC | Response SHA-256 | Conclusion |
| --- | --- | --- | --- | --- |
| Spot `exchangeInfo` | `https://api.binance.com/api/v3/exchangeInfo` | `2026-09-11T13:32:30.642704409Z` | `727f7ff505be55a1df1e1e638ef9571dfe99274771c6dde7ac61d1218adea3c3` | BTCUSDT and ETHUSDT both reported `TRADING`; eligible |
| USD-M `exchangeInfo` | `https://fapi.binance.com/fapi/v1/exchangeInfo` | `2026-09-11T13:32:30.752525992Z` | `2b31db1a4bdab613a7cf5af248b57694fa4bf5cfea27c2cc32f3025b873c74e1` | BTCUSDT and ETHUSDT both reported `TRADING`; eligible |

No account, order, key, credential, or private endpoint was accessed. This
eligibility evidence authorizes no retry and awards no duration credit; the
attempt's immutable result is recorded in
[`M22.9-2h acceptance`](milestone_acceptance/M22.9-2h.md).

## M22.9-P2 live eligibility preflight — 2026-09-11

Immediately before the bounded P2 Recorder start, direct unsigned public
market-data requests captured eligibility for the exact configured symbols. No
account, order, key, or credential endpoint was accessed. The response bytes
are preserved under the P2 VPS evidence bundle.

| Source | URL | Retrieval (UTC) | SHA-256 | Conclusion |
| --- | --- | --- | --- | --- |
| Spot `exchangeInfo` | `https://api.binance.com/api/v3/exchangeInfo?symbols=%5B%22BTCUSDT%22%2C%22ETHUSDT%22%5D` | `2026-09-11T09:49:16Z` | `ed547a4f2b90e5301d2b3cbe70ec5b0f9f2374f8490e37e228c39d8833f12182` | BTCUSDT and ETHUSDT reported `TRADING` |
| USD-M `exchangeInfo` | `https://fapi.binance.com/fapi/v1/exchangeInfo?symbols=%5B%22BTCUSDT%22%2C%22ETHUSDT%22%5D` | `2026-09-11T09:49:16Z` | `b46d606170205aec8576a325418c393d88f2bb1adefa9426739628ec7fa0c0a0` | BTCUSDT and ETHUSDT reported `TRADING`; both are `PERPETUAL` |

This evidence supports the P2 bounded readiness/interaction observation only;
it does not authorize Formal M22.9 or trading activity.

## MS2 configurable-product parameterization — 2026-09-09

MS2 reuses the pinned official sources below for parameterized symbol routes,
public REST symbol arguments, UM `st`/`ps`, and IP-wide cooldown semantics.
No endpoint, stream suffix, sequence algorithm, weight, or retry policy changed.
Global exchangeInfo validation no longer requires BTCUSDT to appear in its
all-symbol response; the durable BTCUSDT global sentinel is unchanged under
ADR-0032. No live Binance traffic or new external documentation retrieval was
performed. Existing URL/retrieval/SHA records retain their original dates.

## MS1 metadata-only identity work — 2026-09-04

MS1 changed only local durable identity and Catalog migration metadata. It did
not change Binance endpoint, stream, payload, rate-limit, or sequence
semantics, and it used no live Binance traffic or newly retrieved external
source. The existing pinned official-source inventory remains authoritative;
no new source hash is required for this milestone.

## M20 proxy transport verification — 2026-07-27T09:29:15+00:00

M20 did not change Binance endpoint/schema semantics. It revalidated transport
capabilities from primary project sources:

| Source | URL | SHA-256 | Conclusion |
| --- | --- | --- | --- |
| websockets 15 stable proxy guide | `https://websockets.readthedocs.io/en/stable/topics/proxies.html` | `4487fdd632e521549d60555a918483299928494112dae3ef216363c69f72c718` | `proxy=True` performs environment discovery including bypass behavior; `proxy=None` disables proxy; a URL selects an explicit proxy |
| Official Binance connector common REST configuration | `https://raw.githubusercontent.com/binance/binance-connector-python/master/common/src/binance_common/configuration.py` | `25a04806125bf73f0dbdfc92a1625594bafd301a90945e1806ba7f45e96a5c98` | `ConfigurationRestAPI` accepts an explicit proxy dictionary; M20 passes only unauthenticated host, port, and protocol |

Retrieval used text-only HTTPS and executed no downloaded content. The pinned
runtime distributions remain `binance-sdk-spot==10.0.0`,
`binance-sdk-derivatives-trading-usds-futures==14.0.0`, and
`binance-common==4.0.3`. ADR-0025 defines redaction and mode semantics.

This is the behavior-evidence inventory for Binance Market Data Recorder. The
project is independent and unofficial: it is not affiliated with, maintained
by, sponsored by, or endorsed by Binance. Official Binance sources establish
API behavior only; they do not make this Recorder an official Binance product.

## USD-M shared REST cooldown verification — 2026-08-31

The current Binance General Info rate-limit guidance was rechecked before the
USD-M side-data cooldown correction. It states that HTTP 429 means a request
rate-limit violation; after 429 clients must back off; repeated violations can
produce an automated IP-wide HTTP 418 ban; bans scale from 2 minutes to 3 days;
and the limits are based on IP rather than API key. It also states that
`Retry-After` gives the wait in seconds for 429 or the ban end for 418. IP
weight 0 on an individual route does not remove the IP-wide policy. Retrieval
was text-only and no online rate limit was induced.

| Source | URL | Retrieval | SHA-256 | Conclusion |
|---|---|---|---|---|
| USD-M General Info / rate limits | `https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/general-info` | 2026-08-31T02:32:22Z | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | 429 backoff, IP-wide 418 bans, 2-minute–3-day escalation, IP basis, and Retry-After semantics |

The pinned runtime source was also inspected locally: `binance-common==4.0.3`
raises `TooManyRequestsError` and `RateLimitBanError` with `status_code` and
`error_message`, but does not reliably preserve complete response headers.

## CM/UM integration notice refresh — 2026-08-23T07:08:24.249743+00:00

The repository-owned updater ran a minimal selection containing the current
Agent Native index and its listed CM/UM integration notice. It wrote response
bodies and `manifest.json` only under `/private/tmp`; no downloaded body was
committed or executed. Both entries below share the updater retrieval time and
are the exact response bytes recorded in that manifest.

| Source | URL | SHA-256 | Bytes |
| --- | --- | --- | ---: |
| Agent Native index | `https://developers.binance.com/en/docs/llms.txt` | `c5decbaaef780287cec46d394b34451e1bc56ffe6681ca74e2f38819fda4a12a` | 167471 |
| CM/UM integration notice | `https://developers.binance.com/en/docs/products/derivatives-trading-coin-futures/Important-CM-UM-Integration-Notice.md` | `7b2f3c49aec8d1ab2d9ca2d1295ae1c0c8a54a958ee01e8aa23ecd03568eaa98` | 12109 |

The notice's D.4 explicitly defines `st` as symbol type (`1 = UM`, `2 = CM`)
and lists `<symbol>@depth<levels>`, `<symbol>@aggTrade`, and
`<symbol>@bookTicker` among streams whose payloads gain `st`. D.5 separately
lists `<symbol>@depth<levels>`, `!bookTicker`, and `<symbol>@bookTicker` as
UM-side streams that gain `ps` (pair symbol), while omitting aggTrade. The
Recorder therefore requires strict `st == 1` for all three USD-M core streams,
requires `ps == BTCUSDT` for depth and individual bookTicker, and does not
invent a `ps` requirement for aggTrade. This source establishes the
stream-specific distinction; it is not evidence that a CM payload may be
stored under the USD-M identity.

The current catalog public/market pages were not used for these hashes: their
direct responses were blocked by an HTTP 202 CloudFront WAF challenge during
the same verification effort. The challenge response and generic `.md` portal
shell were deliberately not recorded as page bodies.

## M19 refresh — 2026-07-26T08:06:37.425492+00:00

The allowlisted updater fetched 20 sources, did not load `llms-full.txt`, and
did not execute remote content.

| Source | URL | SHA-256 |
|---|---|---|
| llms.txt | `https://developers.binance.com/en/docs/llms.txt` | `554ab6217738b845aba19e4bc3637424968d1885a8ec05755bd4dd186f9e4d62` |
| Spot WebSocket | `https://developers.binance.com/en/docs/products/spot/web-socket-streams.md` | `193aa07cd537b2ccc94662474fb3dda3cb774d550b1e117825919d99f91b725f` |
| Spot REST | `https://developers.binance.com/en/docs/products/spot/rest-api.md` | `3bfe5526b745c976ae2db7c6bffdee14f10663d5fe326d8aa54c8b5f12968775` |
| Spot official REST schema | `https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md` | `1f81a02d05822696de40a2b2be21867f2243fda766e0cbbfa7057ac72cf2f93d` |
| Spot official WebSocket schema | `https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md` | `8a127810e46793aa47b42e33ce0df963c9a169c3f27f89b97cbc6a603f0c823e` |
| USD-M general REST/rate limits | `https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/general-info.md` | `b2e647582fb3ae4cae3a79d6f6f6030d0c03cf403d05176117b24094a083521b` |
| USD-M official SDK REST methods | `https://github.com/binance/binance-connector-python/blob/master/clients/derivatives_trading_usds_futures/src/binance_sdk_derivatives_trading_usds_futures/rest_api/rest_api.py` | `c54a30a8778c335f71e7b83899b926eb1635b51e59d5bf31a439c201e2c36d6f` |
| Binance Public Data README | `https://github.com/binance/binance-public-data/blob/master/README.md` | `fe14fa39cbbbc7630f271492cd8d64b0d4709697114adb9a3bc1a13514f453e4` |
| Spot SDK package | `https://github.com/binance/binance-connector-python/blob/master/clients/spot/pyproject.toml` | `eb6f365ace01e8b3c8ae05456f61102bae077decc3f0be6e3a71950e97c38295` |
| USD-M SDK package | `https://github.com/binance/binance-connector-python/blob/master/clients/derivatives_trading_usds_futures/pyproject.toml` | `a33a6e20040b7e4228515a836c5fe08c9eea5919949feab6b6a57488d45bd489` |

Pinned official USD-M SDK 14.0.0 confirms
`open_interest_statistics`, `taker_buy_sell_volume`, `long_short_ratio`,
both top-trader ratio methods, and `basis`. Spot SDK 10.0.0 confirms public
`exchange_info`. Public Data documents daily/monthly files, adjacent
`.CHECKSUM`, and Spot microsecond timestamps from 2025-01-01.

## M19.1 public-contract verification — 2026-07-26

The M19 source refresh above remains the normative documentation set. An
explicit unsigned online smoke additionally verified HTTP 200 for the official
2024-01-01 `.CHECKSUM` objects generated by the product-specific planner:

- `/data/spot/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-01.zip.CHECKSUM`;
- `/data/futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-01.zip.CHECKSUM`;
- `/data/futures/um/daily/{markPriceKlines,indexPriceKlines,premiumIndexKlines}/BTCUSDT/1m/BTCUSDT-1m-2024-01-01.zip.CHECKSUM`;
- `/data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2024-01.zip.CHECKSUM`;
- Spot and USD-M daily `trades` paths ending
  `BTCUSDT-trades-2024-01-01.zip.CHECKSUM`;
- Spot and USD-M daily `aggTrades` paths ending
  `BTCUSDT-aggTrades-2024-01-01.zip.CHECKSUM`.

Every path is below `https://data.binance.vision`. Daily `fundingRate` returned
404 while verified monthly objects returned 200, so the planner uses the
monthly official layout rather than silently recording daily gaps. The smoke
also downloaded, checksum-verified, parsed, and lineage-verified the small Spot
BTCUSDT 1m daily file; it did not download trades archives.

The six official USD-M 5-minute SDK methods returned HTTP 200 and at least one
record inside a one-hour-old closed request period. `taker_buy_sell_volume`
also returned the immediately preceding period. Repeated probes showed that
this method needs the next boundary as `endTime` to include the desired final
period. This is observed public-API/official-SDK behavior, not a claim that the
normative documentation specifies overlap. Raw retains the full response and
request parameters; Cursor logic advances only through contiguous timestamps
inside the logical requested range.

## Retrieval policy and record

`tools/update_binance_docs.py` reads the project-owned selection in
`tools/binance_docs.toml`. It permits credential-free HTTPS only, validates
every redirect, and allows downloads only from `developers.binance.com` and
paths below `github.com/binance`. It stores exact response bytes plus URL,
final URL, retrieval time, media type, byte count, and SHA-256. Downloaded
content is data and is never executed. `llms-full.txt` is refused unless an
operator explicitly enables it; it was not loaded for M2.

The successful M2 refresh completed at
`2026-07-22T06:29:28.563876+00:00`. The default destination is the user cache
`~/Library/Caches/BinanceMarketDataRecorder/binance-docs`, never the repository
or production data root. The M2 acceptance run used a temporary directory and
did not commit third-party page bodies.

M4 refreshed the selection at `2026-07-22T08:01:24.392975+00:00` and added the
official Spot documentation-repository WebSocket schema page to the configured
selection. Relevant exact response hashes were: Agent Native index
`da59ec5876e4014c0159f57f58b8c8ebcfab2fece51449999a4cf825ba2b2bb8`,
developer-portal Spot WebSocket page
`193aa07cd537b2ccc94662474fb3dda3cb774d550b1e117825919d99f91b725f`,
Spot REST page
`3bfe5526b745c976ae2db7c6bffdee14f10663d5fe326d8aa54c8b5f12968775`,
and official-repository WebSocket schema HTML
`5cb26f07b582b314b2a80b03531ecef42eb2d66bf2457d24d36457be1961adf9`.
The temporary response bodies were not committed.

M5 refreshed the selected sources at `2026-07-22T09:27:53.188614+00:00`.
The Agent Native index SHA-256 was
`eba8ad91ee38ef6f15c6d2d1e698a714ee129e20fc9619b82ca2e8b2cb3dd539`;
the USD-M connection page remained
`912f2dad9da21b5c1801d73f052473b6a1d7136a43b2ff3e7a1c2cdc54abdde2`,
and the local-order-book page remained
`d6a94d17fb32450c67ad598c0f923bf9df12ecdc43ced4928798a9fa56d62622`.
M5 also selected the official route-change notice below. The response bodies
were stored only in a temporary directory and were not committed or executed.

M6 refreshed the same selected official sources at
`2026-07-22T10:10:16.759850+00:00`. The Agent Native index, Spot stream page,
and USD-M local-order-book page SHA-256 values were respectively
`eba8ad91ee38ef6f15c6d2d1e698a714ee129e20fc9619b82ca2e8b2cb3dd539`,
`193aa07cd537b2ccc94662474fb3dda3cb774d550b1e117825919d99f91b725f`,
and `d6a94d17fb32450c67ad598c0f923bf9df12ecdc43ced4928798a9fa56d62622`.
No full index or remote code was loaded.

M7 refreshed the selection at `2026-07-22T13:53:53.157858+00:00` and added
the official SDK REST and WebSocket market generated sources needed for side
data. Their GitHub-rendered response SHA-256 values were
`86d61768ae09b4bf0f46c884658c01e45f5cdadfaa7eaa839c758839ad1663c1`
and `14f9d94c8b1260be429b2065ee94c6841170d7db0d1009f032e0fdf93f2d247b`.
The Agent Native index remained
`eba8ad91ee38ef6f15c6d2d1e698a714ee129e20fc9619b82ca2e8b2cb3dd539`.
No full index or remote code was loaded.

M17 rechecked the current Spot stream sources at `2026-07-24T02:25:45Z`
after a public, credential-free fault/soak preflight could not establish Spot
order-book readiness. The developer-portal Markdown SHA-256 remained
`193aa07cd537b2ccc94662474fb3dda3cb774d550b1e117825919d99f91b725f`.
The raw official `binance/binance-spot-api-docs` Markdown SHA-256 was
`32bf73a0bed3b75e3ca981fdbaf48c53544bbdfb5944ef8ed1c4d7af9aceba0a`.
Both say to discard `u <= lastUpdateId` and require the first remaining event
to contain `lastUpdateId` in `[U,u]`; the same official procedure says a normal
next event has `U = previous u + 1` and treats only
`U > localUpdateId + 1` as a live gap.

The second M17 probe preserved 24 Spot snapshots and 355 Spot diff-depth
events. Its first snapshot/update pair was
`lastUpdateId=97799318536`, `[U,u]=[97799318545,97799318546]`. A later
snapshot was `97799318619`; after discarding covered events, the next event was
`[97799318620,97799318630]`. That is the normal adjacent boundary described by
the live rule, but it does not contain `lastUpdateId` as the bootstrap sentence
requires. The Recorder therefore did not change the rule to
`lastUpdateId + 1`, did not claim readiness, and stopped M17. Rapid semantic
retries during diagnosis also produced official `TooManyRequestsError` and
`RateLimitBanError`; no further public API probe or soak was started. This
unresolved official-source/observed-data conflict is R-034.

M17 then refreshed the selected sources at
`2026-07-24T08:57:51.459312+00:00`. Exact response hashes were:
Agent Native index
`7c0c8bf0a3c9200f370c11a54f00ef80496701953ab28eb3225fec93f2cbc3dd`,
Global Spot Markdown
`193aa07cd537b2ccc94662474fb3dda3cb774d550b1e117825919d99f91b725f`,
official Spot-repository GitHub page
`490c298efc4602a585fbf16ae667372a7a106b65b8dc883278580c4c65c0bc81`,
official toolbox GitHub page
`41b5eeac9d2e5b4bc7e997159120a07f15b027a3123be9a2d62cd94a88933216`,
and official connector common-transport GitHub page
`e02be5d57a308e8e33da95f09f3c61d0a405cffee48423ec2eeaefe31ab39b8f`.
GitHub-rendered HTML is not a stable source-code digest, so the embedded exact
source was independently extracted without execution. Toolbox commit
`51547845a9e3725b98e5a1bc55d4895c69ca0ca2` produced 4,187 bytes with SHA-256
`993498520ae240ccae03bc54ad451091e8e0a10c8ef5ec9447f9876058cb9f61`;
connector commit `15c2bfcbb9e9654d7186680a0dd32287a3285e11` produced 30,724 bytes with SHA-256
`7b7934fbde13c29b6b128e7898749874d0f04ebbc0132345ac462f2fd4985391`.
No `llms-full.txt` or remote code was loaded or executed.

The Global text still requires `lastUpdateId` inside the first `[U,u]`. It has
not been formally corrected. Separately, the official toolbox example uses
`U <= last_update_id + 1 <= u`. Together with the immutable observed
`L=97799318619`, `[U,u]=[97799318620,97799318630]`, this is the documented
basis for the versioned engineering decision in ADR-0011. The distinction
between normative text, example behavior, Raw observation, and inference is
intentional. A clarification draft exists locally and was not published.

The originally supplied `https://developers.binance.com/docs/llms.txt`
currently redirects to `/en/docs/docs/llms.txt` and returns portal HTML. The
working official text index is `https://developers.binance.com/en/docs/llms.txt`.
The updater uses the working URL and rejects HTML masquerading as a selected
Markdown page.

## Selected official pages

Hashes are of the exact M2 response bodies.

| Product/use | Official URL | SHA-256 | Bytes |
| --- | --- | --- | ---: |
| Agent Native index | `https://developers.binance.com/en/docs/llms.txt` | `0b3b9024da6e5ffe60129e9756843f847a6edff55565b5c182ecbd217e4d2be8` | 165963 |
| Spot WebSocket streams | `https://developers.binance.com/en/docs/products/spot/web-socket-streams.md` | `193aa07cd537b2ccc94662474fb3dda3cb774d550b1e117825919d99f91b725f` | 10370 |
| Spot REST API | `https://developers.binance.com/en/docs/products/spot/rest-api.md` | `3bfe5526b745c976ae2db7c6bffdee14f10663d5fe326d8aa54c8b5f12968775` | 33310 |
| USD-M WebSocket connection | `https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect.md` | `912f2dad9da21b5c1801d73f052473b6a1d7136a43b2ff3e7a1c2cdc54abdde2` | 2613 |
| USD-M WebSocket route change | `https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Important-WebSocket-Change-Notice.md` | `7711169f43066cb169fa40d90193731630ffca43dc2c04a2c753a5814b596f5c` | 4303 |
| USD-M local order book | `https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly.md` | `d6a94d17fb32450c67ad598c0f923bf9df12ecdc43ced4928798a9fa56d62622` | 1114 |
| USD-M general information | `https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/general-info.md` | `b2e647582fb3ae4cae3a79d6f6f6030d0c03cf403d05176117b24094a083521b` | 19261 |
| USD-M changelog | `https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/change-log.md` | `49bb7b194911c44e88793eca8da18822c28ffb8bdc207e9d6466d28fb4b0e532` | 98085 |
| USD-M SDK REST market-data source | `https://github.com/binance/binance-connector-python/blob/master/clients/derivatives_trading_usds_futures/src/binance_sdk_derivatives_trading_usds_futures/rest_api/rest_api.py` | `86d61768ae09b4bf0f46c884658c01e45f5cdadfaa7eaa839c758839ad1663c1` | 1001109 |
| USD-M SDK WebSocket market source | `https://github.com/binance/binance-connector-python/blob/master/clients/derivatives_trading_usds_futures/src/binance_sdk_derivatives_trading_usds_futures/websocket_streams/streams/market_api.py` | `14f9d94c8b1260be429b2065ee94c6841170d7db0d1009f032e0fdf93f2d247b` | 661925 |
| Python connector REST error source (M17) | `https://github.com/binance/binance-connector-python/blob/master/common/src/binance_common/utils.py` | `e02be5d57a308e8e33da95f09f3c61d0a405cffee48423ec2eeaefe31ab39b8f` | 775072 |
| Python connectors page | `https://developers.binance.com/en/docs/sdks-tools/connectors/python.md` | `b74fdc09f25dee3b03c4d17f76835ed4b7923b548b3cc940005dcb44a2c5b989` | 2964 |
| Official modular SDK repository README | `https://github.com/binance/binance-connector-python/blob/master/README.md` | `4f578165c03deb9b1426bd0ab2805018f7c6c3de80c8a44e6d85da083c4e01ef` | 300231 |
| Spot SDK package metadata | `https://github.com/binance/binance-connector-python/blob/master/clients/spot/pyproject.toml` | `60ec5346b5150b87e31c586d3563ba54f3a29ecf713daa33e49f2817c5acefc1` | 255068 |
| USD-M SDK package metadata | `https://github.com/binance/binance-connector-python/blob/master/clients/derivatives_trading_usds_futures/pyproject.toml` | `553710b453a5c6342736b921993e37effc29336fddf1a744f245235fd46f4fb7` | 256441 |
| Spot API changelog | `https://github.com/binance/binance-spot-api-docs/blob/master/CHANGELOG.md` | `950621641102e8e65922fe3bcf5f0f27d8126074e586be1b8753ee3fde6b158f` | 1304722 |
| Spot local-book toolbox example (M17) | `https://github.com/binance/binance-toolbox-python/blob/master/manage_local_order_book.py` | `41b5eeac9d2e5b4bc7e997159120a07f15b027a3123be9a2d62cd94a88933216` | 314804 |

The GitHub entries above are official repository web responses, so their hashes
include GitHub rendering around the source. Package selection is independently
verified from installed wheels and their source, not inferred from page text.

## Selected SDK packages

| Package | Version | Wheel SHA-256 | Decision |
| --- | ---: | --- | --- |
| `binance-sdk-spot` | `10.0.0` | `614d26671fa5aaa5402c0ffbcb13ff3168e03e58d09845db330472448d9a833b` | Accepted for unsigned public REST; WebSocket layer rejected |
| `binance-sdk-derivatives-trading-usds-futures` | `14.0.0` | `a39992c26dfc745f3193be3247d5c8a06fa21af35d207e1f68195097257cf7f6` | Accepted for unsigned public REST; WebSocket layer rejected |
| shared `binance-common` | `4.0.3` | `c64318d9141576e98f365bbb97f019948ce4a2d90cddeec21d169305d3fb1651` | Inspected by the capability probe |

The complete CPython 3.12/macOS arm64 resolution and hashes are in
`requirements/macos-arm64-python312.lock`. The deprecated Futures connector
and third-party `python-binance` are absent.

## Confirmed API semantics and transport conclusions

- Spot documents lowercase stream symbols, raw and combined stream forms, a
  24-hour connection lifetime, server ping every 20 seconds, pong within one
  minute with copied ping payload, and `U/u` local-book bridging rules.
- The official Spot repository documents `btcusdt@depth@100ms` with `E/U/u/b/a`,
  `btcusdt@aggTrade` with `E/a/f/l/T`, and `btcusdt@bookTicker` with `u` and no
  exchange event timestamp. M4 fixtures preserve those documented fields.
- Spot public depth snapshots use `GET /api/v3/depth`.
- USD-M documents routed `/market` aggregate-trade and `/public` depth and
  individual book-ticker streams, a 24-hour connection lifetime, server ping
  every three minutes, and pong within ten minutes. The route-change notice
  states that legacy unrouted Market streams stopped pushing after
  2026-04-23, so M5 uses only routed URLs.
- USD-M public depth snapshots use `GET /fapi/v1/depth`; local-book continuity
  requires each new event's `pu` to equal the prior event's `u`.
- USD-M mark-price data uses the routed market stream
  `/market/ws/btcusdt@markPrice@1s`. It supplies mark/index/estimated-settle
  prices, current funding rate, mark moving average and next funding time;
  `T` is not treated as transaction time.
- The symbol liquidation stream `/market/ws/btcusdt@forceOrder` is explicitly
  event-sparse: at most the latest liquidation snapshot inside 1000 ms is
  emitted, and no event is emitted in an interval without a liquidation. It is
  not documented as a complete order-by-order liquidation feed.
- Public SDK REST side data uses `/fapi/v1/premiumIndex` (weight 1 with symbol),
  `/fapi/v1/openInterest` (weight 1), `/fapi/v1/exchangeInfo` (weight 1), and
  `/fapi/v1/fundingRate` plus `/fapi/v1/fundingInfo` sharing 500 requests per
  five minutes per IP. Funding-info describes adjusted interval/cap/floor
  symbols only, so absence does not imply a fixed eight-hour cadence.
- The Global Spot page says bootstrap discards buffered
  `u <= lastUpdateId` and bridges the first remaining `[U,u]` over
  `lastUpdateId`; it treats `U > localUpdateId + 1` as missed events. The
  official toolbox example instead tests containment of `lastUpdateId + 1`.
  ADR-0011 records the unresolved distinction and versioned engineering rule;
  this inventory does not claim the Global page was corrected. USD-M discards buffered
  `u < lastUpdateId`, bridges `U <= lastUpdateId <= u`, and then uses `pu`
  continuity. Both products document absolute quantities and zero deletion;
  USD-M explicitly says deleting a missing level is normal.
- The pinned SDK models retain Spot `U/u` and USD-M `U/u/pu`, but the shared SDK
  receive loop JSON-decodes before callback, supplies neither original bytes nor
  socket-receipt time, runs callbacks inline, exposes no connection-factory
  injection, and schedules a fixed internal reconnect. It therefore fails the
  Recorder WebSocket evidence gate.
- `websockets==15.0.1` is selected only as the RFC WebSocket transport. Binance
  endpoint, payload, lifecycle, and sequence semantics continue to come from
  official Binance sources. A local probe verifies byte-identical
  whitespace-sensitive text-frame receipt with `recv(decode=False)`.
- An opt-in no-credential smoke on 2026-07-22 returned HTTP 200 and five
  bid/ask levels from both official SDK public depth methods. No account API was
  called and no credential was read.
- Successful SDK REST responses expose parsed models and response headers, not
  the exact original HTTP body. The pinned common transport discards response
  headers when raising 418/429, so it cannot preserve `Retry-After`. ADR-0022
  therefore replaces only Spot depth snapshots with a minimal unsigned HTTPS
  wrapper that records exact body/headers; USD-M retains the SDK boundary.

ADR-0008 records the general REST decision, ADR-0022 its Spot snapshot
exception, and ADR-0009 the WebSocket decision.
M4 revalidates Spot raw routing, `@100ms` fixtures, protocol Ping/Pong,
serverShutdown, reconnect and public live capture. M5 separately validates the
USD-M route split, `U/u/pu`, protocol Ping/Pong, reconnect, exact Raw retention,
and concurrent Spot/USD-M public capture.

## Changelog locations

- Spot API: `binance/binance-spot-api-docs` `CHANGELOG.md` and the corresponding
  developer-portal changelog.
- USD-M: developer portal `change-log.md` selected above.
- Modular Python SDKs: official `binance/binance-connector-python` releases,
  commits, and per-client package metadata.

## Known retrieval limitation

Some developer-portal catalog pages are readable in an interactive browser but
scripted retrieval currently receives a CloudFront/WAF HTTP 202 challenge with
an empty body. The updater treats non-200 responses as failures and does not
store a challenge as documentation. Those catalog pages are not in the
automated selection; the downloadable product Markdown, official generated SDK
source, public smoke, and official changelogs are the recorded evidence. M7
reproduced this challenge for the USD-M REST market-data catalog before
selecting the matching official SDK generated sources. See R-028. No unofficial
mirror or proxy is used.

## M21.4.11-R2.2 source impact

The R2.2 correction on 2026-08-11 changes only local crash ordering, Catalog
idempotency validation, zero-record marker provenance, and the read-only
historical audit. It changes no Binance endpoint, payload, sequence, timing,
rate-limit, or SDK behavior. No new external source was required or retrieved;
the existing official-source inventory and hashes remain authoritative.

## M22.9 post-close continuity correction source impact

The M22.9 correction changes only local bounded-handoff failure ordering,
manifest completeness forcing, and reuse of the existing ADR-0027 durable
gap/recovery authority. It changes no Binance endpoint, payload, sequence,
timing, rate-limit, or SDK behavior. No new external source was required or
retrieved; the existing official-source inventory and hashes remain
authoritative.

The later startup-liveness correction at local commit `2e8525f…` likewise
changes only local recovery scheduling, stable-sealed reconciliation, heartbeat
liveness, and cooperative shutdown. It changes no Binance API semantics and
requires no new official-source retrieval.

## MS4-C R3 official eligibility refresh — 2026-09-11

Immediately before the separate R3 recovery supplement, the Tokyo VPS captured
the following official source responses. Retrieval was unsigned public market
data only; no account, order, credential or trading endpoint was used.

| Source | URL | Retrieval (UTC) | SHA-256 | Conclusion |
| --- | --- | --- | --- | --- |
| Agent Native source index | `https://developers.binance.com/en/docs/llms.txt` | `2026-09-11T01:01:49.552554Z` | `d5bdebefe19b4976b55835f11ac5b43e8e3045103b6c8c7ab15b134b2122a103` | Official index captured for the R3 source-selection record; it is not itself a market eligibility assertion |
| Spot `exchangeInfo` | `https://api.binance.com/api/v3/exchangeInfo?symbols=%5B%22BTCUSDT%22,%22ETHUSDT%22%5D` | `2026-09-11T01:01:49.576772Z` | `396050c65df5d01baf107f4d4d76217333b4566f9da66502e6aef86dd7cba3e8` | `ELIGIBLE` for Spot BTCUSDT and ETHUSDT; both selected symbols reported `TRADING` |
| USD-M `exchangeInfo` | `https://fapi.binance.com/fapi/v1/exchangeInfo` | `2026-09-11T01:01:49.616402Z` | `2dc55e92a76cbd999f4e37478da2d43551181e794b2ec94f00295f31014f683d` | `ELIGIBLE` for USD-M BTCUSDT and ETHUSDT; both selected symbols reported `TRADING` |

These hashes and conclusions are the fresh eligibility evidence bound to the
R3 `NONFORMAL_MS4_RECOVERY_SUPPLEMENT`; they do not authorize a new live run,
Formal M22.9, or source retirement.

## Formal M22.9 2h retry eligibility refresh — 2026-09-12

Immediately before exact-artifact readiness, the Tokyo VPS captured unsigned
public official responses. No account, order, credential, or trading endpoint
was used.

| Source | URL | Retrieval (UTC) | SHA-256 | Conclusion |
| --- | --- | --- | --- | --- |
| Spot `exchangeInfo` | `https://api.binance.com/api/v3/exchangeInfo?symbols=%5B%22BTCUSDT%22%2C%22ETHUSDT%22%5D` | `2026-09-12T03:24:00.041682148Z` | `07e4f0ddb33965b90812bc178de39feedf428c64d1ff4bfbd09bd01572735120` | BTCUSDT and ETHUSDT both reported `TRADING` |
| USD-M `exchangeInfo` | `https://fapi.binance.com/fapi/v1/exchangeInfo` | `2026-09-12T03:24:00.041682148Z` | `70d17ae79f30e9017a90b8e2a70f0d4f272d5c2070421ffe10f8a9f9f0e6bad5` | BTCUSDT and ETHUSDT both reported `TRADING` and `PERPETUAL` |

The raw responses and compact conclusion are retained in the root-controlled
redeploy/readiness evidence bundle. This refresh supports the immediate 2-hour
retry gate only; it does not authorize 12 hours or Production Ready.

## Formal M22.9 2h retry immediate pre-T0 refresh — 2026-09-12

Immediately before the actual Formal retry T0, the Tokyo VPS refreshed the
same unsigned public official endpoints. No account, order, credential, API-key,
or private endpoint was used.

| Source | URL | Retrieval (UTC) | SHA-256 | Conclusion |
| --- | --- | --- | --- | --- |
| Spot `exchangeInfo` | `https://api.binance.com/api/v3/exchangeInfo?symbols=%5B%22BTCUSDT%22%2C%22ETHUSDT%22%5D` | `2026-09-12T05:25:28.069854051Z` | `1d16143356258a9b42d96c8134f74dd6f8a3a4a50b5d266024b737312281130b` | BTCUSDT and ETHUSDT both reported `TRADING` |
| USD-M `exchangeInfo` | `https://fapi.binance.com/fapi/v1/exchangeInfo` | `2026-09-12T05:25:28.069854051Z` | `fc562c75cf7513b1d08e386547053a43d252292f450ed9b620d56b2493e9094b` | BTCUSDT and ETHUSDT both reported `TRADING` and `PERPETUAL` |

The raw responses and compact conclusion are retained in the Formal acceptance
root. Eligibility passed, but the later host-maintenance interruption leaves
the stage incomplete and provides no 12-hour or Production Ready authority.

## All-enabled qualification health refresh — 2026-10-02

Only official public documentation was read; downloaded content was never executed.
Selected text/schema bytes are retained under the local cache
`~/Library/Caches/BinanceMarketDataRecorder/binance-docs-all-enabled-20261002/`.

| Source | URL | Retrieval UTC | Content SHA-256 | Conclusion |
|---|---|---|---|---|
| Agent Native index | `https://developers.binance.com/en/docs/llms.txt` | `2026-10-02T04:53:43.647725Z` | `7474dd19d22fc523ea20709c1dffa44d3fda9209006409ce3296d275b59a155c` | USD-M public REST/stream operations remain listed; API detail moved to catalog |
| Official USD-M WS schema1.0.0 | `https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/1.0.0/schema.yaml` | `2026-10-02T05:00:24.828598Z` | `b81aab5381fa00f25e86c9d8c7c24a822493edef2a291097f5e9319a460fb48f` | Symbol liquidation pushes latest snapshot within1000ms and no message without liquidation; mark price supports1s/3s; existing market/public routes and st fields retained |
| Official USD-M REST schema1.0.0 | `https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/1.0.0/schema.yaml` | `2026-10-02T05:01:02.225017Z` | `9c3f92d02d8b84f6c209286b601058bea66e98016c0f47757fb253337bd59d2c` | fundingInfo covers only adjusted symbols, so empty list is valid; shares500/5min/IP funding-history limit. Six statistical routes retain5m/500 limits and explicit endpoint timestamp semantics; Raw preserves source timestamps |

Old selected USD-M `.md` paths returned identical65,590-byte portal landing HTML
SHA-256 `1c9f731787466a2e31b03034fb707b97a1fe0c75e73bf46749618a4d77d11234`
at04:54:25–26UTC, not normative Markdown. Direct catalog/schema updater requests
received202 WAF challenges. Visible official browser navigation exposed the
catalog's exact Download schema links; downloading their inert text succeeded.
No CAPTCHA, downloaded code execution or nonallowlisted source was used. These
schema bytes establish the health semantics without substituting SDK examples.
The documentation updater's old selected detail URLs need a separate future
refresh; landing HTML must continue to fail validation.


## Q3 repeated-empty operational identity correction — October2,2026

No Binance endpoint, request range, SDK dependency, period/retention, schema or
rate-limit semantics changed. Reuse the official source inventory above.
The retained public Raw responses under Q3-retirement-48ca920 show leading
allowed overlap and requestedRecordCount0; they establish actual observed
responses, not a new normative publication-latency guarantee. The correction
changes local operational-event identity and explicitly bounded diagnostic
recovery; it never advances a missing-period cursor or substitutes different data.
No new external document retrieval or downloaded code execution occurred.

## R1 USD-M E/T semantic correction (2026-10-14)

USD-M public depth/bookTicker T is Transaction Time; market aggTrade T is
Trade Time. ADR-0044 maps these separately and versions Normalize rederivation
without changing old Raw. E remains Event Time. The current Agent Native
index selected the official USD-M streams, and their current catalog pages
were inspected through the browser source reader.

| Source | URL | Retrieval UTC | Content SHA-256 and representation |
| --- | --- | --- | --- |
| Agent Native index | https://developers.binance.com/en/docs/llms.txt | 2026-10-14T02:33:26.224998+00:00 | `b92c275f8d8aed4f403235a354d2ac6681261480e2f579d043b3c79ed4f00ef1` raw text response |
| Official USD-M stream page | https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public | 2026-10-14T02:37:02.366345+00:00 | `9a8c54bf50e85e99a4bd4fd5c6125cfbe5862fa1263032cd37d022dd261b67d2` combined browser-extracted E/T excerpts, not HTTP response bytes |
| Official USD-M stream page | https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/market | 2026-10-14T02:37:02.366345+00:00 | `9a8c54bf50e85e99a4bd4fd5c6125cfbe5862fa1263032cd37d022dd261b67d2` combined browser-extracted E/T excerpts, not HTTP response bytes |

Direct HTML/schema requests returned HTTP202 empty bodies, so their response hashes provide no semantic evidence. The nonempty browser-extracted official pages identify stream-specific T semantics and int64 types. Downloaded text was not executed. Shared Contracts fixture origin/check date and copied SHA remain in tests/fixtures/contracts/README.md.


## R3 offline capacity / shared-IP inventory — 2026-10-19

Agent Native index `https://developers.binance.com/en/docs/llms.txt` was checked
before selected Spot REST/WS and USD-M General/WS/market endpoint pages. Full
URL, UTC retrieval time, representation and content SHA-256 are recorded in
[machine-readable source ledger](milestone_evidence/r3-official-sources-20261019.json).
The documentation portal moved endpoint pages into its catalog. Direct HTTP
endpoint fetches returned empty bodies/site shells, so endpoint evidence is an
explicitly labelled Web-rendered excerpt snapshot, not a claimed upstream HTTP
digest; source snapshots remain under `/private/var/tmp/r3-binance-docs-8awe6o9v`.
No exchange market-data endpoint was called.

Conclusions: depth weights differ by market and requested limit; IP budgets
include both processes. Funding history/info share a separate request-count
window, while zero-weight statistics still require count-window budgeting.
WS client control and connection attempts are separate from incoming market
frames. The [R3 inventory](runbooks/R3-shared-ip-budget.md) records calculations,
process-local cooldown coverage and the still-open joint recovery gate.
Official example rateLimits must be replaced by actual metadata before live
admission; none of these sources qualifies twenty-product VPS capacity.


## 2026-10-19 R3-b/G3-b shared-IP admission

Re-read the Agent Native index first, then the selected official Spot REST/WS
and USD-M general/WS text. Exact URLs, UTC retrieval times, content SHA-256 and
bytes are in [the source record](milestone_evidence/shared-api-official-sources-20261019.json).
Selected current [USD-M market data](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data)
confirms depth1000 weight20, funding500/5min shared and statistics1000/5min;
[Spot market data](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/market)
retains the depth limit/weight tiers. The source record hashes downloaded
upstream text, not a hash claim for rendered excerpts. No Binance API was called.
Static role shares, reference6000/2400 admission limits and remaining live checks
are explicit in [the closeout](milestone_acceptance/R3-b-G3-b-shared-api-20261019.md).

## R4 correction adopted — 2026-10-26

The independent COIN-M catalog also describes the latest snapshot, but the
April10 changed-description entry is explicitly under USD-M. The U conflict
is disclosed rather than resolved by guessing; do not infer a C change.
The current contract preserves representative messages and does not use either
selection assumption in completeness, deduplication or derived liquidation totals.
Historical retrieval conclusions above remain their original observations and are
superseded for current D11 interpretation by the [coverage correction](milestones/P0/coverage.md).

The correction refresh follows the current index and preserves exact short U/C
rendered excerpts in the [source record](milestones/P0/corrections-20261026/official-sources.json).
Both were observed at 2026-10-26T15:14:16Z; each excerpt SHA-256 is
`1ac870d670e877cf3ed117f171b01b6081c3d5131594e44698083196b0565a6f`.
Equal short wording is not independent original-HTTP identity. The downloaded
changelog at 2026-10-26T14:48:32Z retains SHA-256
`7da6939ae9e4e9b9c9dbdbabba6a4b582fa08d3af23c4bd952546f140e331863`.
U/C `.md` probes returned an HTML shell and are explicitly unusable; no downloaded
content was executed and the index's full-text corpus was not loaded for this fix.

## P2 implementation sources — 2026-10-27

Current source inventory and exact representations are in [P2 official-sources.json](milestones/P2/official-sources.json). Direct catalogue/schema downloads returned WAF 202 and were excluded as semantic evidence. Rendered official-page excerpts are explicitly hashed as excerpts, not HTTP bodies. No downloaded code was executed.

| Source | Retrieval UTC | SHA-256 | Relevant conclusion |
| --- | --- | --- | --- |
| [index](https://developers.binance.com/en/docs/llms.txt) (downloaded text) | 2026-10-27T10:20:33.264476+00:00 | `d23560223c5726258114e892d31b86915d8f4de4a2bf65d6885e9c8c54800ab4` | Current index read before selected pages; no llms-full load. |
| [spot-streams](https://developers.binance.com/en/docs/products/spot/web-socket-streams.md) (downloaded document bytes) | 2026-10-27T10:20:53.964338+00:00 | `555f9ed488d94665fc6c6dc3e7645d90546d2d179d5083f0289a8a9a5f538a35` | Combined wrapper, exact application-message boundary, native connection limits and shutdown. |
| [spot-rest](https://developers.binance.com/en/docs/products/spot/rest-api.md) (downloaded document bytes) | 2026-10-27T10:20:53.971075+00:00 | `25f9203680b6c2570113c54fb449a119dc7482f125b790a6a27ce572b4665dec` | Public unsigned REST and supplied time/precision semantics. |
| [um-connect](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect.md) (downloaded document bytes) | 2026-10-27T10:20:53.900829+00:00 | `912f2dad9da21b5c1801d73f052473b6a1d7136a43b2ff3e7a1c2cdc54abdde2` | USD-M public/market routing and combined stream limit. |
| [cm-connect](https://developers.binance.com/en/docs/products/derivatives-trading-coin-futures/websocket-market-streams/Connect.md) (downloaded document bytes) | 2026-10-27T10:20:53.968097+00:00 | `d6963a3e320b7d9d86ab4cbc9e199d063926a6e27ecfddd4f07dac821f158762` | COIN-M dstream route and combined stream limit. |
| [integration](https://developers.binance.com/en/docs/products/derivatives-trading-coin-futures/Important-CM-UM-Integration-Notice.md) (downloaded document bytes) | 2026-10-27T10:20:53.899703+00:00 | `7b2f3c49aec8d1ab2d9ca2d1295ae1c0c8a54a958ee01e8aa23ecd03568eaa98` | UM/CM 2400/min shared IP weight; st=1/2; stream-specific ps; cross-host kline support. |
| [catalog-excerpts](https://developers.binance.com/en/docs/catalog) (serialized rendered official-page excerpts, not original HTTP bytes) | 2026-10-27T11:43:53+00:00 | `5ec6acf9c87a44aa127757efd39b9699d843c054b9d29aac583f19b174dd333e` | Spot blockTrade native t field; C indexPrice i/p; C premium symbol contract remains ambiguous; U top-ratio key restrictions retained. |

Live public samples, initial invalid-limit probes, C dated-premium failures and the successful perpetual comparison are separately retained in P2 fixtures. The SDK source evaluation used installed official Spot 10.0.0 / USD-M 14.0.0 / common 4.0.3; generated model null loss and the generic wrapper error are worked around within the installed unsigned transport. D31 uses the already-received body to preserve numeric decimal precision. See [P2 result](milestones/P2/README.md); failed/unobserved sources are not qualification successes.

RPI sequence semantics checked at 2026-10-27T12:52:15Z: [official public streams](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public) documents `pu` as the preceding stream event's final ID. The [short rendered excerpt](milestones/P2/rpi-sequence-source.json) SHA-256 is `fadee421d713a415c5e82d806d3170e09fd542dce58344ed5b607907bf632be8` (excerpt identity, not HTTP-body identity). P2 keeps an independent RPI cursor and marks a known jump before append; it does not infer order-book price correctness.

## P2 corrections and measured D06 — 2026-10-28

Read the [Agent Native index](https://developers.binance.com/en/docs/llms.txt) first at 2026-10-28T06:41:13Z; 174,964 bytes, SHA-256 `c5ce78c90244f0bf92fa1b6143a63807bdfb908a3de8c1a6e9379e73c911741f`. Selected old premium `.md` URLs returned an HTML shell, excluded as semantics. [Retrieval records and catalog excerpt](milestones/P2/closeout-20261028/official-sources.json) record UTC bounds/hashes. No delivery applicability rule was established, so R2 remains open.

The [official Public Data README](https://github.com/binance/binance-public-data/blob/master/README.md) was re-read in rendered form on October28. The pinned README hash above remains its original retrieval. Actual [file/date/checksum observations](milestones/P2/closeout-20261028/daily-file-availability.json), [content/field/ID study](milestones/P2/closeout-20261028/daily-window-study.json), and [import readbacks](milestones/P2/closeout-20261028/daily-import-sample.json) bind each official ZIP URL, supplied SHA-256 and actual acquisition time. All 17 available files were downloaded and SHA-verified; two CM next-quarter files returned 404 for this date. Six-column files lack isRPITrade/live timing; native ID holes are disclosed. These observations do not establish exchange completeness. No downloaded code was executed.

| Current rendered source | UTC retrieval bounds | SHA-256 of retained short excerpt | Conclusion |
| --- | --- | --- | --- |
| [premium-catalog](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-coin-m-futures/api/rest-api/market-data) | 2026-10-28T06:41:14Z to 2026-10-28T06:42:45Z | `87b48b046703bdb8193ba88d702ad580eb776857dbf11c79a2dcfc46266a784f` | No explicit perpetual/delivery applicability rule established; R2 remains open. |
| [public-data-readme](https://github.com/binance/binance-public-data/blob/master/README.md) | 2026-10-28T06:41:13Z to 2026-10-28T06:41:30Z | `8bb6fea7c93492f92f5937c1c39479547938d247453b90aa5b9447101bcc71e3` | Daily release is delayed; verify selected file/date/fields and revisions independently. |
