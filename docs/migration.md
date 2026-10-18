# 0.8 到 0.14 的接口与行为

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
