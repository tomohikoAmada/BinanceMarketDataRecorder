# R2 日报隔离：追加稳定性独立复核

**ACCEPT：仅最终九个生产文件的 LOCAL/OFFLINE 源码与合成控制；无剩余阻断项。** 本补充绑定最终125文件源码，替代首轮报告的候选源码绑定；首轮报告、初版失败及其16组控制原样保留。新源码的既有发布/迟到/authority/真实维护进程隔离/超时取消与exec重试控制均已重新执行通过。

追加范围限定于已改路径，没有重审R1/OI/ADR0048，也没有SSH或生产数据操作。发现并核实三项小修：

- 历史report全量list的解码内存随保留量增长。最终report使用原唯一排序的keyset页，每页metric最多512行、remote最多128行，fetch后关闭cursor并释放Catalog锁；原逐行manifest/descriptor/binding校验完整保留，无缓存和跳校验。
- 固定DESC first-due遇一个坏日报会饿死其他日报。初版真实maintenance连续三轮因22日输出故障失败，正常21日始终不生成。单个durable last-attempt指针修后，真实child依次“22失败→21完成→22失败”，故障移除后22完成；每个child仍最多一次尝试。
- 固定PID scratch可挡住同PID重试。UUID scratch及只清理本调用新建文件的finally通过实测：fsync失败保留旧report、旧PID failed scratch原字节不变、replace失败后同PID可重试。

独立分页控制使用2050个metric批次（当天513）及257条合成remote授权，三类iterator与原list全序列一致；实际JSON和CSV与ca760逐字节一致。实测最大fetch=512/128。最后authority页损坏仍拒绝build、不发新完成receipt；恢复后可重试。第一512行已过后插入排序落在旧页的新batch，第一次输出1030但token拒绝完成、返回RETRY，下一轮正确1031。页间另一thread能取得Catalog锁，SQLite不在跨页事务内。

保留真实边界：分页约束解码缓冲，不承诺总历史扫描CPU/I/O恒定或永不触及300s。真实VPS资源峰、安装来源/新wheel/精确CI/部署/新时长及saved-data/restoration仍需各自资格；现场无stack，日报不是已证明的唯一原故障原因。全部14功能及四产品的采集/配置、60s恢复和原StopSec未被本次最小修复削弱。

精确源码哈希、脚本、逐项JSON证据及本报告由 RESULT.json/MANIFEST.json 绑定。仅生成私有合成fixture，未修改仓库文件、生产Raw/历史manifest、失败证据或任何生产服务。
