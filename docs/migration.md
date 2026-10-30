# 接口与行为迁移

0.16.10 将项目工具链基准与固定 CI 从 moonc 0.10.14 升至
`0.10.17+d971670cd`，满足 `moonc >= 0.10.16` 的要求。使用旧版本工具链的环境
应先升级编译器；公开 API 和 MCAP 格式保持兼容。下文旧版本条目保留当时的验证环境。

0.15.24 新增可选 Node.js WorkerPool、ParallelReader、decode_parallel、编译线程入口及 parallel 命令，核心增加有界展开与 feed_expanded 协议。旧查询路径默认不启动线程，输出语义不变；线程池须显式关闭，见 [并行处理](parallel-codecs.md)。

0.15.23 新增 resumable_chunk_pipeline、CJOB2 与 chunkmerge --checkpoint-final，checkpoint_run_ids/progress 识别组合状态；旧 CJOB1/MJOB1/MJOB2 保持兼容。新模式支持最终排序段与归并组复用，最终输出仍整体重写，见 [贯通恢复](chunk-pipeline.md)。

0.15.22 新增独立 MFLD1 字段范围侧车、build_field_index/open_field_index、FieldQueryPlan 与 field 命令；现有 MIDX1/MIDX2 和 JSON 条件行为不变。只排除可证明不命中的候选块，保留最终精确过滤，见 [字段索引](field-index.md)。

0.15.21 新增 AsyncDiskSidecarReader/open_disk_sidecar、asyncpaged 命令及核心索引审核/查询 poll-feed 协议；MIDX2 格式和同步入口不变。首次打开仍完整核对源指纹与全索引，见 [异步磁盘索引](async-disk-sidecar.md)。

0.15.20 新增 CdrOptions/CdrDecoder/cdr_decoder，standard_payload_decoder 增加可选 cdr 配置并自动识别 ros2msg/cdr，Protobuf 与 CDR 共用总缓存预算；新增 raw-cdr 命令。既有 Protobuf/JSON 模式保持兼容，见 [CDR 解码](cdr-decoding.md)。

0.15.19 增加 protobuf_json_options、ProtobufOptions.protojson/emit_defaults 与可选规范映射，默认数值模式保持不变；结构体建议用 protobuf_options() 更新字段。描述符更严格拒绝非法 oneof/optional、保留编号、重复字段名和包名，见 [消息解码](payload-decoding.md)。

0.15.18 增加可选 Node.js CachedHttpSource/open_cached_http，所有原 HTTP API 不变；新缓存须独立目录并在使用后 close，见 [HTTP 缓存](http-cache.md)。

0.15.17 增加 ProtobufDecoder、PayloadDecoder、同步/异步 decode_payload 和 cmd/decode；原容器 payload 保持原字节，显式适配后的声明为 JSON。默认 64 位整数输出精确 JSON 数字，可选字符串模式，见 [消息解码](payload-decoding.md)。

0.15.16 新增 resumable_chunk_merge、CJOB1 和 cmd/chunkmerge；checkpoint_run_ids/progress 增加识别新引擎，原 MJOB1/MJOB2 不变。新任务须独立校验的索引块，完成输入时仍整体校验，最终归并失败从副本重做。见 [Chunk 保存点](chunk-checkpoint.md)。

0.15.15 增加 aggregate_external 与 cmd/spill，逐组输出 NDJSON；max_groups 在新接口中约束缓冲段而非总输出。原内存接口签名不变，状态预算补计极值字符串，接近预算上限的调用可能更早明确失败。见 [外部聚合](external-aggregation.md)。

0.15.14 新增 MIDX2 分页磁盘索引、build_disk_sidecar/open_disk_sidecar、DiskSidecarReader/Cursor 和 cmd/paged；MIDX1 保持兼容，新格式采用新建索引。另修正引用 Zstd 解码器五字节 Huffman literal 头的 18 位长度掩码，增加真实驾驶日志验证，见 [磁盘索引](disk-sidecar.md)。

0.15.12 新增 diagnose_source、SourceScanEvent/scan_source(trace~)、BlockRecoveryEvidence/recover_blocks(on_block~)、recover_blocks_detailed 与 cmd/diagnose。原有签名增加可选参数，既有调用不变。diagnosis_json 的可选字段修正为标量或 null，曾依赖数组形状的调用方需调整。见 [大文件诊断](source-diagnostics.md)。

0.15.11 写出版本 2 检查点并继续读取版本 1；输入阶段增加逐文件保存点，新增 checkpoint_progress，job-inspect 展示已提交进度。resumable_merge 签名不变，自定义存储需允许 -2..-65 的专有输入辅助段。见 [恢复任务](resumable-merge.md)。

0.15.10 新增 AsyncQuerySource/AsyncDataset/AsyncSidecarReader、异步内容哈希和 NDJSON；SidecarCursor 增加 poll/feed，next 保持兼容；Dataset.remap_message/QuerySource.declaration_log 供提供方复用核心校验。见 [统一异步查询](async-dataset.md)。

0.15.9 新增 Replay、ReplayPoll/Progress 和 asyncio.playback，以及 cmd/replay 定时导出示例；QuerySource.declaration_log 返回独立声明副本。原查询和录制接口不变。见 [回放](replay.md)。

0.15.8 新增 DiffOptions/diff_sources 和 diff 命令，CommandOptions 增加 diff 字段。diff 以退出码 1 表示合法文件存在内容差异；错误仍为 2。见 [日志比较](semantic-diff.md)。

0.15.7 新增 AggregateOptions/aggregate_json 和 aggregate 命令；CommandOptions 增加 aggregate 字段，使用 parse_command 的旧调用保持兼容。平均值提供精确分数及明确舍入的小数，见 [聚合](aggregation.md)。

0.15.6 新增可显式关闭的 ManagedFile/FileScope；已有 open_file、异步文件接口和查询签名保持兼容。CLI 运行期间持有输入句柄，最多 1024 个；每文件默认 64 KiB 预读。见 [文件 I/O](file-io.md)。

0.15.5 新增 resumable_merge、checkpoint_run_ids 和 Node MergeJob；CommandOptions 新增 job_dir/resume_job，parse_command 原有用法保持兼容。持久任务需显式 --job，普通临时合并行为不变。见 [恢复任务](resumable-merge.md)。

0.15.4 新增 JSON 内容过滤和投影，CommandOptions 增加 content 可选字段（无过滤为 None）；Query 结构和既有查询不变。stream_transform 增加可选 content 参数，原始 MCAP payload 保持不变。见 [JSON 查询](json-query.md)。

0.15.3 新增 SidecarReader/SidecarCursor、IndexVerification、build_sidecar/open_sidecar，以及 Node SHA-256 和文件身份适配器。CommandOptions 新增 indexes、fast_index、index_bytes；手工构造完整参数结构体需补字段，parse_command 的既有参数保持兼容。默认侧车打开会完整读取源内容验证 SHA-256，只有显式 TrustIdentity/--fast-index 走文件属性信任模式。

0.15.1 新增 stream_transform、SourceVisitor/scan_source 和 FileCursor.write_ndjson，原接口继续可用。external_merge 新增 preserve_ids 可选参数，默认行为不变；true 仅允许单输入并保留独立通道身份。0.15.2 新增 QuerySource、Dataset/DatasetCursor 与 dataset-query，多文件 next_page 返回带来源编号的 DatasetMessage。后续功能按 0.15.x 递增发布。

原有内存读写、QueryIndex/QueryCursor、同步 StreamWriter、批量变换、连续前缀恢复和索引重建继续可用。MCAP 格式目标仍为版本 0，消息负载和三种压缩边界不变。没有自动以部分恢复替代严格读取的行为。

| 版本 | 新入口 | 替换或组合方式 |
| --- | --- | --- |
| 0.9 | RangeReader.cursor、FileCursor.poll/feed | 文件分页可替代重复 RangeReader.query；CLI query 已使用磁盘游标 |
| 0.10 | asyncio | 需要等待宿主 I/O 时使用 async 函数；原同步 API 保留 |
| 0.11 | asyncio.open_http | 使用强 ETag 的稳定 HTTP 对象作为 AsyncSource |
| 0.12 | external_merge | 乱序大文件合并替代先 read_log 再 merge_logs，显式提供临时存储和预算 |
| 0.13 | recover_blocks | 显式索引块部分提取，清单标明范围；recover_prefix 仍只返回连续前缀 |
| 0.14 | 质量与发行检查 | 增加固定种子差分、RSS 基准、归档文件检查及完整 CI |

CommandOptions 新增 external 和 temp_dir。手工构造完整结构体的调用者需补充这两个字段；命令行解析器的旧参数继续可用。新增 WriteSink.write 用于自定义临时排序存储，不能绕过 StreamWriter 的状态来向同一输出随意插入数据。

所有查询接口沿用确定次序：升序 `(时间,物理位置)`；倒序反转整个次序，同时间物理位置也反转。外部合并始终升序，时间相同时按输入文件列表顺序、各输入物理顺序保持重复消息。

FileCursor 进度不在未扫描时声明精确匹配总数；QueryCursor 的总数来自完整内存索引。这两个进度类型不能互换。HTTP 的初始探测和重试网络开销不属于 AsyncSource 的逻辑成功读取计数，实际传输量可由服务器请求日志核对。

Chunk 解码现在拒绝声明体中压缩数据后的尾随字节。此前可能误接纳的长度损坏输入会被拒绝；合法文件没有变化。运行库新增固定依赖 moonbitlang/async 0.20.2，已验证工具链仍为 moonc 0.10.14+7d59c7ec9。仅 JS/Node 提供异步文件和 HTTP 宿主。
