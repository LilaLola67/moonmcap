# 增量查询与质量监控

两种输入明确区分：`RecordingTail` 只处理 MRSC1 清单已提交的完整分段，逐段检查
大小、SHA256、容器 CRC 和消息数；活动 `.part` 不可见。`GrowingTail` 处理稳定大小
快照中的完整物理记录和完整 Chunk，半个头或半个 body 等待，不推进偏移。
输入必须保持追加写语义；每次 poll 都重新散列已消费前缀，截断或改写立即拒绝。
这会增加前缀读取开销，不宣称只读取新增字节。

```sh
moon run cmd/payload --target js -- tail active.mcap examples/live/rules.json poll-1.json
# 后续规则增加 "snapshot":"poll-1.json"，输出到新的 poll-2.json。
```

规则 `mode` 为 file（默认）或 recording（INPUT 是录制目录）。identity 必填，用于
区分录制会话。topics、start/end（日志时间字符串）、decode、where 控制增量查询。
输出保留原始 data_hex 和信封；decode 或 where 使用标准载荷解码器。质量条件仅
解码其指定话题，不会因无关的原始图像话题而失败。

输出是单个原子 JSON 文件，包含 rows、evidence、quality、state。state 同时包含
游标与质量快照；后续读取上次输出中的 state。查询条件、模式和规则绑定，改变配置
必须建立新会话。watermark/finish 是可变控制项。保存失败后仍可从上一份输出重做。
外部系统收到结果后若在保存快照前崩溃，仍可能再次收到同批数据；库接口不承诺
外部副作用恰好一次。CLI 原子文件避免结果和快照分开提交。

部分文件 evidence 为 `partial-prefix`，仅已出现的 Chunk CRC 等检查可用，
chunks_with_crc 会记录实际提供 CRC 的块数。DataEnd/摘要/尾部齐全后执行完整容器
扫描，才升级为 `complete-container`；未提供 CRC 的区域没有凭空获得校验能力。
后期全文件 CRC 失败不会追认先前部分结果。已提交分段标记 `committed-segment`，
无新分段时标记 `no-new-committed-segment`。

GrowingTail 默认每次最多处理 32 条外层记录（units 可设 1–4096），批消息逻辑字节
上限 64 MiB，块解压受 Limits 限制，声明逻辑预算 3 MiB，单页消息数受 max_records
限制。块必须整体解码，单个块超过批预算报错，不会暗中拆成未经校验的小片段。
RecordingTail 每次最多一个完整分段，默认同样限制 64 MiB 消息逻辑字节。
快照最多 8 MiB，带 CRC；文件适配器使用 SHA256 检查来源。CLI 将整批转换为 JSON，
额外对象/十六进制输出内存另计，不将逻辑预算当成 RSS 上限。

## 质量与水位线

quality.rules 支持 topic、minimum_messages、max_gap、check_sequence、condition、warning。
quality.start/lateness 是 UInt64 纳秒字符串，pending_limit 默认 4096、最大 16384。
质量计算按事件时间排序；默认水位线为已观察最大时间减 lateness，也可显式 advance。
水位线为闭区间，之后到达的相等或更早消息计入 late_excluded 并排除，不追溯改写
已输出统计。相同时间的缓冲消息按到达顺序处理。未知话题不推进质量水位线。

最小数量和尾部静默只在 finish 时检查；运行中无违规状态为 monitoring。
已定稿消息检查间隔、条件和每通道 UInt32 连续序号（允许回绕），最多保存每规则
64 个详细问题和 1024 个序号通道。内存重排队列只保存已检查条件的轻量信封，
不保留原始载荷。队列满时明确报错，可先 advance 或缩小允许迟到范围。
结束后拒绝新消息；显式结束时间不得早于已观察数据或水位线。

`python tools/live_workflows.py` 用独立 writer 构造三种压缩的渐增文件，验证不完整
块等待、规则绑定、SHA256 前缀改写、续读不重复、质量状态一致及分段清单回退拒绝。
