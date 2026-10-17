# MoonMCAP

MoonBit 原生 **MCAP 0** 容器库，提供标准记录编解码、无压缩/LZ4/Zstd 分块读写、索引查询、日志变换和截断恢复。当前模块版本 **0.5.0**，支持按需读取、LRU 块缓存与有预算约束的流式录制，见 [更新记录](CHANGELOG.md)。

## 安装与运行

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

尚未发布 mooncakes.io，目前从 GitHub 复现。运行库依赖固定版本的 `bikallem/compress 0.3.4`、`bikallem/blit 0.2.2`；[第三方来源与许可证](third_party/README.md) 说明了引用代码及修改。运行时不需要 Python，也不调用外部压缩进程。

## API 示例

使用方在 `moon.pkg` 中导入 `"LilaLola67/moonmcap" @mcap`，通过本地模块依赖使用：

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

调用会抛出 `McapError`，应在声明该错误的函数或 `try/catch` 内使用。时间是 UInt64 纳秒，查询区间两端包含；同一时间按原始物理顺序稳定排序，支持倒序、偏移和数量分页。`Uncompressed`、`Lz4`、`Zstd` 是三种写入模式。

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

## 容器与压缩

覆盖 MCAP 0 的全部 **15 类标准记录**，包括 Statistics、消息/块索引、附件与元数据索引和 SummaryOffset。读取时检查记录顺序、声明依赖、摘要副本、统计内容、物理偏移以及数据区、摘要、块和附件 CRC。可读取顺序、分块、有/无摘要、有/无摘要偏移、无消息索引的合法文件，并在重写时重新生成统计及索引。

- LZ4：标准帧读写；读取独立/依赖块、块校验及内容校验。
- Zstd：标准单帧读写；读取 raw/RLE/compressed 块、未知内容大小及可选校验；写端采用固定编码策略，不提供压缩等级调节。
- 每个 Chunk 独立判断压缩方式，允许同一文件混用三种模式。
- 默认记录大小上限 16 MiB、单块解压上限 64 MiB、字符串 1 MiB、记录数量 100,000，可通过 `Limits` 配置。新增的 `max_decompressed_bytes` 限制单块展开，嵌套记录也计入文件记录预算。

[兼容范围与可复现案例](docs/compatibility.md) 说明了具体边界。消息负载保持原始字节，不负责 CDR、Protobuf 或 ROS profile 的业务解码。

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

- MoonBit 单元/回归测试：79 项，覆盖官方压缩帧、边界、损坏输入、游标对照、按需读取和缓存淘汰。
- 真实文件验证：30 项；包含 `3 输入压缩 × 2 摘要布局 × 3 输出压缩 = 18` 组双向读写检查，以及顺序文件、无消息索引、无摘要偏移、混合压缩和查询/合并/恢复工作流。
- 输出报告：`_interop/conformance/report.json`，记录参考输入 SHA-256 及各项结果。GitHub Actions 执行检查、构建、测试和这两个验证脚本，并上传报告。

Python `mcap==1.3.0` 仅为开发阶段的官方参考工具，其包版本不是文件格式版本。生成测试不计入有效源码；`tools/count_source.py` 只统计根目录手写核心的非空、非注释 `.mbt` 行，排除测试、生成接口、示例及 `internal/zstd` 引用代码。

## 边界

不支持外部压缩字典、拼接帧、skippable frame 或自定义压缩算法；不支持消息体语义解码。原有文件 API 接收已加载的 Bytes；RangeReader 和 Node.js 文件适配器支持 [按需读取](docs/range-reader.md)，快速模式不校验未读取的数据。[StreamWriter](docs/stream-writer.md) 支持同步输出、增量校验和与分块/摘要预算；旧 LogBuilder 仍提供完整快照和原子添加。

`scan_file` 可取得未知原始记录；`Log` 是语义模型，重写会重建容器，不保留未知记录和扩展字段的原始字节。未提供 CRC 的内容无法仅凭解析证明未损坏。互操作结果限于已执行的矩阵与声明边界，不等于所有未来扩展格式都已验证。

## 原创与参考

MCAP 容器实现独立编写，依据 [官方规范](https://mcap.dev/spec)，参考生态为 [foxglove/mcap](https://github.com/foxglove/mcap)（MIT）。容器核心采用 MIT，引用的 Zstd 实现及压缩依赖遵循其 Apache-2.0 许可证，详见 [third_party](third_party/README.md)。[公开项目检索记录](docs/duplication-review.md) 说明了检索范围与相邻项目差异。
