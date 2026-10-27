# MoonMCAP

统一组合查询入口与复现说明见 [异步查询管线](docs/pipeline.md)。

[可选工作线程](docs/parallel-codecs.md)在 Node.js 中并行解压和解码，保留排序、预算和背压；附耗时及进程内存实测。

[贯通恢复任务](docs/chunk-pipeline.md)把 Chunk 副本、最终排序段和归并组进度保存在同一任务中，支持进程中断后复用。

[字段范围索引](docs/field-index.md)通过块级数值/字符串摘要保守排除不命中块，并保留最后的精确筛选与完整结果。

[异步分页索引](docs/async-disk-sidecar.md)可组合本地/远程 MIDX2、持久 HTTP 缓存和消息解码，完整审核后按页查询。

HTTP 远程日志支持有界持久页缓存、并发预取、ETag 快照校验、校验失败重取与中断恢复，见[HTTP 缓存](docs/http-cache.md)。

消息结构解码：内置 JSON、Protobuf FileDescriptorSet 和 ROS 2 ros2msg / XCDR1 解码，可接入同步、异步筛选、投影与精确聚合，支持自定义插件。见[消息解码](docs/payload-decoding.md)和 [CDR 范围与样例](docs/cdr-decoding.md)。

[0.15.24 本地验证报告](examples/validation/release-0.15.24.json)记录 41 组回归、11 个语料案例、4719 条公开 CDR 消息的独立解码对照，以及 200 条消息的并行/串行对照，区分真实录制、转换数据与合成样例。

MoonBit 原生 **MCAP 0** 容器库，提供标准记录编解码、三种压缩模式、磁盘游标、异步录制、HTTP 范围查询、外部排序合并和诊断恢复。当前模块版本 **0.15.29**，见 [更新记录](CHANGELOG.md) 和 [迁移说明](docs/migration.md)。

## 安装与运行

[块级保存点](docs/chunk-checkpoint.md)为可独立校验的索引块保存持久副本，输入未完成时也能保留录入进度。

[外部聚合](docs/external-aggregation.md)在分组缓冲超限时溢写临时磁盘，精确归并并逐组输出 NDJSON。

[分页磁盘索引](docs/disk-sidecar.md)使用外部排序和有限页缓存，提供百万条实际构建与查询报告。

[公开数据验证](docs/corpus-validation.md)分别记录真实机械臂日志与复杂合成分布，提供来源、固定指纹和独立参考核对。

[大文件诊断](docs/source-diagnostics.md)按记录完整校验，提供首错范围、读取统计和实际恢复消息时间证据。

[统一异步查询](docs/async-dataset.md)组合本地/HTTP、侧车、JSON 条件、跨文件分页和 NDJSON 导出。

[时间回放](docs/replay.md)提供倍速、暂停、单步、跳转，以及有背压的异步消息回调。

[日志差异比较](docs/semantic-diff.md)区分消息、声明、附件和元数据变化，忽略压缩及索引布局差异。

[流式聚合](docs/aggregation.md)支持 JSON 数值统计、字段分组和时间窗口，累计十进制值保持精度。

[JSON 内容查询](docs/json-query.md)支持精确数字条件、字段投影、跨文件分页及流式转换筛选。

[可恢复合并任务](docs/resumable-merge.md)支持逐输入保存点、持久归并检查点、已完成排序段复用，以及任务查看和清理。

[文件 I/O](docs/file-io.md)提供直接字节桥接、可关闭文件句柄、有界预读及物理读取统计。

需要 `moonc >= 0.10.14`，已验证 `v0.10.14+7d59c7ec9`；文件操作示例需要 Node.js 22.3 及以上。工具链安装见 [MoonBit 官方下载页](https://www.moonbitlang.com/download/)。

```sh
git clone https://github.com/LilaLola67/moonmcap.git
cd moonmcap
moon update
moon check
moon build
moon test
moon run cmd/main
```

JS 目标：`moon test --target js`、`moon run cmd/main --target js`。内存示例录制 6 条消息和一个附件，再读取、统计、查询和裁剪，最后输出 `MCAP_HEX=` 数据用于互操作验证。

[可运行文件样例](examples/README.md)提供无压缩/LZ4/Zstd 的实际输入文件、裁剪/合并/恢复的预期输出及一键验证命令：`python examples/run.py`。

安装 Mooncakes 发行版本：

```sh
moon add LilaLola67/moonmcap@0.15.29
```

也可从 GitHub 克隆复现全部样例；[发布包校验](docs/distribution.md) 可生成经过文件清单检查的模块归档。运行库依赖固定版本的 `bikallem/compress 0.3.4`、`bikallem/blit 0.2.2`，异步包使用 `moonbitlang/async 0.20.2`；[第三方来源与许可证](third_party/README.md) 说明了来源。运行时不需要 Python，也不调用外部压缩进程。

## API 示例

安装依赖后，使用方在 `moon.pkg` 中导入 `"LilaLola67/moonmcap" @mcap`：

```moonbit
let recorder = @mcap.new_builder()
let channel = recorder.register_channel("/events", "json")
recorder.append(channel, 100UL, b"{\"value\":1}")
let bytes = recorder.finish(compression=@mcap.Zstd, messages_per_chunk=256)
let log = @mcap.read_log(bytes)
let query = { ..@mcap.all_query(), topics: ["/events"], start_time: 100UL, end_time: 200UL }
let messages = @mcap.query_file(bytes, query).messages
let sliced = @mcap.slice_log(log, query)
let output = @mcap.write_indexed(sliced, compression=@mcap.Lz4)
```

调用会抛出 `McapError`，应在声明该错误的函数或 `try/catch` 内使用。时间是 UInt64 纳秒，查询区间两端包含；升序按 `(时间,原始物理位置)` 排序，倒序反转整个次序，支持偏移和数量分页。`Uncompressed`、`Lz4`、`Zstd` 是三种写入模式。

连续读取时，可以复用同一个索引与游标：

```moonbit
let cursor = @mcap.build_query_index(log).cursor(query)
let first_page = cursor.next_page(32)
let second_page = cursor.next_page(32)
cursor.seek(150UL) // 包含时间边界；不重新应用 offset
let progress = cursor.progress()
cursor.cancel()
```

游标最多保留每个匹配通道的一个归并头部，不为每页重新构造完整候选数组。`offset`、`limit` 作用于整个遍历；`seek` 重置本次进度和 limit，取消后不能重开。索引仍持有完整内存快照。完整约定和示例见 [消息游标](docs/query-cursor.md)，运行 `moon run cmd/cursor` 或 `moon run cmd/cursor --target js`。

## 大文件与异步工作流

| 功能 | 行为与边界 |
| --- | --- |
| [磁盘游标](docs/file-cursor.md) | 逐页按需加载、处理时间重叠块、seek/取消；候选负载超预算显式失败 |
| [流式变换](docs/stream-transform.md) | 临时文件支持大文件排序、采样、分页及对齐；NDJSON 逐条导出 |
| [跨文件数据集](docs/dataset.md) | 冲突 ID 重映射、全局分页与跳转、来源编号；不生成合并文件 |
| [侧车索引](docs/sidecar-index.md) | 为无索引文件建立持久定位表，SHA-256 过期检查、显式快速身份模式、跨文件接入 |
| [异步读写与轮转](docs/async-io.md) | 实际异步宿主 I/O、等待输出接纳、任务取消；每个完成段独立可读 |
| [HTTP 查询](docs/http-source.md) | Range + 强 ETag + If-Match，超时、有限重试、活动请求中止；拒绝整文件回退 |
| [外部排序合并](docs/external-merge.md) | 乱序输入写临时排序段，多轮归并、稳定重复次序、声明重映射、临时空间预算及清理 |
| [独立块恢复](docs/block-recovery.md) | 显式跳过坏块、校验后续索引块并输出范围清单；始终标为部分提取 |

异步文件和 HTTP 宿主支持 JS/Node；同步核心、磁盘游标协议和排序算法支持 Wasm。原路径的解压/排序仍在当前线程运行，可选工作线程后端分担解压和负载解码；异步 I/O 本身不意味着多线程压缩。负载预算分别计账，不是总进程内存上限，见 [内存实测](docs/performance.md)。

## 容器与压缩

覆盖 MCAP 0 的全部 **15 类标准记录**，包括 Statistics、消息/块索引、附件与元数据索引和 SummaryOffset。读取时检查记录顺序、声明依赖、摘要副本、统计内容、物理偏移以及数据区、摘要、块和附件 CRC。可读取顺序、分块、有/无摘要、有/无摘要偏移、无消息索引的合法文件，并在重写时重新生成统计及索引。

- LZ4：标准帧读写；读取独立/依赖块、块校验及内容校验。
- Zstd：标准单帧读写；读取 raw/RLE/compressed 块、未知内容大小及可选校验；写端采用固定编码策略，不提供压缩等级调节。
- 每个 Chunk 独立判断压缩方式，允许同一文件混用三种模式。
- 默认记录大小上限 16 MiB、单块解压上限 64 MiB、字符串 1 MiB、记录数量 100,000，可通过 `Limits` 配置。新增的 `max_decompressed_bytes` 限制单块展开，嵌套记录也计入文件记录预算。

[兼容范围与可复现案例](docs/compatibility.md) 说明了具体边界。容器读写保留原始消息字节；可选解码适配器支持 JSON / Protobuf 和明确范围的 ROS 2 XCDR1 结构查询，完整 ROS profile 语义需要业务方处理。

## 日志操作

- 主题/时间裁剪，清理无引用声明并保留附件、元数据。
- 按内容去重 schema/channel，合并时安全重映射 ID 并保留重复消息。
- 平移消息和附件时间，检查 UInt64 溢出/下溢。
- 时序统计：消息数、数据量、时间间隔、乱序与序号异常；与标准 Statistics 记录分别提供 API。
- 查询游标：逐条/分批取消息、稳定分页、双向时间跳转、取消和进度；同一索引可共享给多个独立游标。
- 录制器提供声明去重、自动序号、快照、添加失败原子性及结束状态。
- 分片记录流解码与连续前缀恢复。压缩块完全解码并通过校验后才接纳消息，遇到损坏或截断就停止，不穿过坏字节重同步。

## 文件示例与验证

下面的 JS 命令将文件转为 Zstd 索引文件：

```sh
moon run cmd/interop --target js -- input.mcap output.mcap zstd rewrite
```

还支持 `slice`（筛选 `/imu` 的 20–30 秒闭区间）、`merge`（在参数末尾追加第二个文件路径）和 `recover`，用于复现完整工作流。该命令的文件访问使用 Node.js；核心编解码、压缩及变换在 Wasm 和 JS 上均可运行。

```sh
python -m pip install mcap==1.3.0 lz4==4.4.5 zstandard==0.25.0
python tools/interop.py
python tools/conformance.py
python tools/count_source.py
```

- MoonBit 单元/回归测试：Wasm 169 项、JS 190 项；JS 增加 Node 适配器和真实异步暂停、取消、背压、轮转状态测试。
- 真实文件验证：30 项；包含 `3 输入压缩 × 2 摘要布局 × 3 输出压缩 = 18` 组双向读写检查，以及顺序文件、无消息索引、无摘要偏移、混合压缩和查询/合并/恢复工作流。
- 真实文件验证还覆盖按需读取、三种流式输出、8 项 CLI、4 项批量变换、8 项诊断修复，以及 5 项异步录制、15 项 HTTP 故障、5 项磁盘排序、9 项独立块恢复、108 项固定种子差分检查和 4 组进程 RSS 测量。执行命令见下方，结果位于 `_interop/` 各子目录的 `report.json`。GitHub Actions 完整执行并上传报告。
- 新增 45 项大文件诊断与恢复故障用例，百万条消息与实际超过 2 GiB 文件的本地验证见 [规模报告](docs/performance.md)。常规 CI 使用缩小规模，完整规模测试可手动启用。

```sh
python tools/range_benchmark.py
python tools/stream_workflows.py
python tools/cli_workflows.py
python tools/batch_workflows.py
python tools/repair_workflows.py
python tools/async_workflows.py
python tools/http_workflows.py
python tools/external_workflows.py
python tools/block_workflows.py
python tools/transform_stream_workflows.py
python tools/dataset_workflows.py
python tools/sidecar_workflows.py
python tools/json_workflows.py
python tools/checkpoint_workflows.py
python tools/input_checkpoint_workflows.py
python tools/io_workflows.py
python tools/aggregate_workflows.py
python tools/diff_workflows.py
python tools/replay_workflows.py
python tools/async_dataset_workflows.py
python tools/source_diagnostic_workflows.py
python tools/large_source_workflows.py
python tools/fuzz_workflows.py
python tools/memory_benchmark.py
python tools/corpus_workflows.py
python tools/disk_index_workflows.py
python tools/disk_index_scale.py
python tools/aggregate_external_workflows.py
python tools/chunk_checkpoint_workflows.py
python tools/protobuf_workflows.py
python tools/cached_http_workflows.py
python tools/protojson_workflows.py
python tools/cdr_workflows.py
python tools/async_paged_workflows.py
python tools/field_index_workflows.py
python tools/chunk_pipeline_workflows.py
python tools/worker_workflows.py
python tools/worker_benchmark.py
python tools/package_audit.py
```

## 文件工具

[通用命令行](docs/cli.md) 提供 `inspect`、`query`、`slice`、`merge`、`external-merge`、`recover`、`recover-blocks`、`rewrite`、`transform`、`validate`、`reindex`。主题、UInt64 纳秒时间、分页、压缩和预算均可配置；查询支持 JSON/CSV，输出文件拒绝覆盖已有路径。

```sh
moon run cmd/mcap --target js -- query examples/files/zstd-summary.mcap --topic /imu --start 20000000000 --end 30000000000
moon run cmd/mcap --target js -- validate examples/files/zstd-summary.mcap
moon run cmd/mcap --target js -- reindex broken-summary.mcap --output rebuilt.mcap --compression zstd
```

[批量变换](docs/batch-transform.md) 提供主题映射、按通道时间窗采样、时间对齐、附件和元数据筛选。[结构化诊断与修复](docs/diagnostics.md) 区分数据区和索引/摘要问题，已知时给出偏移、记录类型与预期/实际值；只从完整有效的数据区重建索引，数据损坏可显式选择连续前缀恢复或索引块部分提取。

Python `mcap==1.3.0` 仅为开发阶段的官方参考工具，其包版本不是文件格式版本。生成测试不计入有效源码；`tools/count_source.py` 只统计根目录手写核心的非空、非注释 `.mbt` 行，排除测试、生成接口、示例及 `internal/zstd` 引用代码。

## 边界

不支持外部压缩字典、拼接帧、skippable frame 或自定义压缩算法。消息体可显式执行 JSON 条件和投影，Protobuf 和受支持的 ros2msg/XCDR1 可先按日志内定义解码；其他编码保留原始字节。原有文件 API 接收已加载的 Bytes；RangeReader 和 Node.js 文件适配器支持 [按需读取](docs/range-reader.md)，快速模式不校验未读取的数据。[StreamWriter](docs/stream-writer.md) 支持同步输出、增量校验和与分块/摘要预算；旧 LogBuilder 仍提供完整快照和原子添加。

`scan_file` 可取得未知原始记录；`Log` 是语义模型，重写会重建容器，不保留未知记录和扩展字段的原始字节。未提供 CRC 的内容无法仅凭解析证明未损坏。互操作结果限于已执行的矩阵与声明边界，不等于所有未来扩展格式都已验证。

## 原创与参考

MCAP 容器实现独立编写，依据 [官方规范](https://mcap.dev/spec)，参考生态为 [foxglove/mcap](https://github.com/foxglove/mcap)（MIT）。容器核心采用 MIT，引用的 Zstd 实现及压缩依赖遵循其 Apache-2.0 许可证，详见 [third_party](third_party/README.md)。[公开项目检索记录](docs/duplication-review.md) 说明了检索范围与相邻项目差异。
