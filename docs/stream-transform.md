# 大文件流式变换（0.15.1）

```sh
moon run cmd/mcap --target js -- stream-transform input.mcap --temp-dir ./runs --output result.mcap --rename /imu=/sensor/imu --sample-period 10000000 --align-start 0 --compression zstd
moon run cmd/mcap --target js -- query result.mcap --format ndjson --limit 100000
python tools/transform_stream_workflows.py
```

临时父目录须存在。stream_transform(source, storage, output, options, external~, cancelled~) 不保留整个 Log 的消息数组：先使用外部排序生成按时间排序的规范中间文件，再逐页计数采样结果，选择分页窗口、写出有界选中分段，最后按指定方向输出。可处理顺序/无索引和三种压缩输入；输入整体通过原有严格容器校验后才完成结果。多个扫描阶段以额外 I/O 换取有限消息缓冲，首条最终输出不保证低延迟。

语义与 transform_log 一致：按原始主题筛选，按时间升序及物理位置选择每个通道每个采样桶的第一条，再按方向应用 offset/limit；align_start 将选中消息最小 log_time 对齐目标，同时平移 publish_time 和保留附件的两个时钟，检查溢出/下溢。即使声明内容相同，也保留源通道 ID，避免合并通道改变采样。重命名同时应用，不串联；保留消息引用的声明，支持附件及元数据筛选。空选择可以输出合法空文件，空选择不能对齐。

临时排序段和两个 MCAP 中间文件统一受 spool_bytes 磁盘预算约束；排序段、归并头部、输入解压、输出块、摘要和声明另有预算。普通错误与协作取消清理已知临时文件；崩溃恢复不在本版范围。CLI 独占发布新输出，失败清理暂存文件；直接调用者必须丢弃失败输出。已有 transform_log 与 transform 命令仍为内存接口。

query --format ndjson 与 FileCursor.write_ndjson 每次向 sink 写一条 JSON 消息对象和换行，时间为无精度损失的字符串、负载为 data_hex，不解码业务内容。输出到 stdout 可接管道；stdout 已发送的行无法撤回，快速查询也不验证未读取内容。输出到文件使用同样的独占发布事务。

scan_source / SourceVisitor 提供物理顺序、单记录/单块缓冲的完整验证访问器，包含消息原始定位信息，供后续索引与工具复用。回调可能在最终 DataEnd CRC/摘要关系校验前执行；调用者只有在整个扫描成功后才能提交副作用。资源和记录预算生效，未知记录不进入语义变换结果。
