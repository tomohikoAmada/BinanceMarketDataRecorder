# Capacity 阶段私有绑定简审

结论：新计划与ADR0032及现有配置/产品/readiness API一致；**新BTC阶段不得原样运行旧四产品私有控制器**。本次只核绑定，不重审九生产文件、不实施、不SSH、不读取生产Raw或历史manifest。

新cohort的两个market必须显式采用同一组符号。全部14功能开启时，N=1/2/4的辅助上下文分别为14/26/50；完整上下文应由冻结exactconfig派生，不能只改一个数字。

必须最小调整四个新scope私有文件：

- `qualification_auxiliary_gate.py`：移除固定BTC+ETH/cohort及26假设，验证exact products与完整contexts；保留全部14flag、freshness/cursor/core条件。
- `qualification_monitor_rate.py`：将固定26改为独立绑定的expected完整集合；缺失、额外或同数量错名字均拒绝，原BLOCK和采样缺口语义保留。
- `scoped-terminal.py`：四产品assert改为当前stage start/boundary/config一致的完整ProductKey集合，selected审计不得漏掉已配置产品。
- `common.py`：全新source/config/identity/deploy/Q/unit/window绑定；旧2h/12h锚按旧身份独立验证并保持历史意义，不能要求等于新身份或继承时长。`forecast`固定42与旧四产品rates不能静默套用：N1可能保守过估，N4+可能低估；应以当前配置的producer/global singleton数量和冻结的当前cohort实测/保守rates作具体准入。

最小回归为N1/N2/N4 exact上下文及缺/多/错名；selected产品一致性；新身份下旧锚仍可按原身份核验但零时长继承；配置变化令reserve/rates输入变化，时空不足仍拒绝。当前无需增加框架或重建字节不变的生产源码；actual新配置、速率及admission材料到位后另行复核。

CI两处checkout ref（18/54行）均为`${{ github.event.pull_request.head.sha || github.sha }}`：PR选实际head SHA，main push选该push SHA。该变更避免PR默认merge混入main R3；最终仍核实际checkout git HEAD=欲发布exact branch head。这里只接受YAML绑定意图，不宣称远端CI已经通过。
