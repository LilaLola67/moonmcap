# 跨文件统一查询（0.15.2）

```sh
moon run cmd/mcap --target js -- dataset-query segment0.mcap segment1.mcap --topic /imu --start 0 --end 1000000000 --limit 100 --format ndjson
python tools/dataset_workflows.py
```

将多个 RangeReader.query_source() 交给 open_dataset，使用 Dataset.cursor(query) 创建统一游标。next_record 返回 DatasetMessage（source_index 从输入列表 0 开始、message 使用全局 channel ID）；next 只返回消息；next_page 返回带来源的记录数组。支持主题/闭区间时间筛选、双向分页、seek、取消、进度和逐条 NDJSON。Dataset.channels/schemas 提供全局声明，按内容去重并重映射冲突 ID；不兼容 profile 拒绝。

不生成合并文件，逐源有序游标与堆头按 `(时间,输入文件编号,文件内物理位置)` 归并；倒序反转整个次序。默认保留全部重复消息，包括同一文件在输入列表中重复出现。offset/limit 只应用于全局结果，seek 在原始区间内清除 offset、重置 limit，取消/失败后不能重开。零 limit 不读取消息块。多个游标可以独立推进。

pending_bytes 默认 64 MiB：一半预算等分给子游标候选，其余供堆头及动态检查；小预算或高度重叠块可能显式失败。每个源还拥有自己的块/缓存/摘要预算；不是总进程 RSS 限额。默认最多 64 个输入，声明保留量受预算限制，总输出数量受 Limits.max_records 限制。输入需要保持同一快照，快速查询只验证读到的内容。CLI --full 对每个文件执行既有整文件预算内的完整验证。

无 ChunkIndex 的 RangeReader 提供方会在原 fallback_bytes 预算内先加载完整 Log，并在进度 materialized_sources 中明确计数；关闭 fallback 则拒绝。需要避免这条回退路径时，使用后续的侧车索引提供方。索引文件的快照稳定性由各输入适配器保证。

QuerySource/MessageStream 是可扩展的有序提供方接口，便于后续把持久索引接入同一个 Dataset。自定义提供方必须保证查询过滤、排序、声明、取消释放和负载计数正确；流异常会使整个 DatasetCursor 失败并取消其他源。输入数组及声明在构造时复制，来源编号始终对应原始列表，不随筛选或声明去重改变。
