# MoonMCAP

MoonBit 原生 MCAP 日志基础库：有界二进制读写、CRC 校验、索引查询、日志变换和截断恢复。报名阶段已有可运行核心；后续验收完善计划见下方。

## 安装与运行

需要 `moonc >= 0.10.14`。已验证工具链：`moonc v0.10.14+7d59c7ec9`。安装方式见 [MoonBit 官方下载页](https://www.moonbitlang.com/download/)。核心仅依赖 MoonBit 标准库。

```sh
git clone https://github.com/LilaLola67/moonmcap.git
cd moonmcap
moon check
moon build
moon test
moon run cmd/main
```

JS 目标需要 Node.js：`moon test --target js`、`moon run cmd/main --target js`。示例实际录制 6 条消息和一个附件，写成索引文件，再读取、统计、按时间查询和裁剪，最后输出 `MCAP_HEX=` 十六进制数据供跨语言验证。

尚未发布到 mooncakes.io；报名阶段先从 GitHub 复现。发布后补充包管理器安装命令。

## API 示例

在使用方 `moon.pkg` 中导入 `"LilaLola67/moonmcap" @mcap`，通过本地模块依赖或发布后的包使用。完整可运行示例位于 [cmd/main/main.mbt](cmd/main/main.mbt)。

```moonbit
let recorder = @mcap.new_builder()
let channel = recorder.register_channel("/events", "json")
recorder.append(channel, 100UL, b"{\"value\":1}")
let bytes = recorder.finish(messages_per_chunk=256)
let log = @mcap.read_log(bytes)
let query = { ..@mcap.all_query(), topics: ["/events"], start_time: 100UL, end_time: 200UL }
let messages = @mcap.query_file(bytes, query).messages
let sliced = @mcap.slice_log(log, query)
let rewritten = @mcap.write_log(sliced)
```

以上调用会抛出 `McapError`，应在可抛出该错误的函数或 `try/catch` 内使用。时间为 UInt64 纳秒，查询区间两端包含，时间相同按原始物理顺序稳定排序；支持倒序、偏移及数量分页。ID 和声明依赖会校验，消息体作为不透明字节保留。

## 已完成

- Header、Schema、Channel、Message、Footer、Metadata、Attachment 编解码；小端整数、UTF-8、长度和记录预算检查。
- 顺序文件及无压缩 Chunk 文件读写；数据区、摘要、块及附件 IEEE CRC32 校验。
- MCAP MessageIndex、ChunkIndex、AttachmentIndex、MetadataIndex、SummaryOffset 的编解码；索引文件写入和物理偏移一致性验证。
- 内存二分时间索引；文件查询按块时间范围和主题过滤，再稳定排序、分页，并返回诊断计数。
- 主题/时间裁剪、声明裁减、内容去重合并与 ID 重映射；消息和附件时间平移及溢出检查。
- 分片记录流解码，支持任意输入边界、缓存预算、失败状态和截断检测。
- 每主题消息数、数据量、时间间隔、乱序及序号异常统计。
- 状态化录制器：声明去重、自动序号、添加失败原子性、快照和结束状态。
- 连续前缀恢复及重新写入：不会穿过损坏字节强行重同步；整块验证后才接纳块中的消息。

## 检查与证据

GitHub Actions 执行 Wasm/JS 的检查、构建、测试、示例，以及官方 MCAP 双向互操作检查。核心测试包括 CRC 损坏、长度预算、ID 依赖、时间溢出、任意分片、索引偏移、排序分页、录制状态、附件保留和截断恢复。

```sh
python -m pip install mcap==1.3.0
python tools/interop.py
python tools/count_source.py
```

互操作检查：官方 Python 读者验证 MoonBit 输出；官方 Python 生成顺序及索引 Chunk 夹具，由 MoonBit 验证。固定为无压缩格式，不能据此推断所有第三方文件均兼容。夹具在 `reference_fixture_test.mbt`，不计入实现源码。行数统计仅计算根目录手写核心 `.mbt` 的非空、非注释行，排除测试、示例、依赖和生成接口。

## 当前边界与后续验收

- 当前只支持无压缩块，遇到 LZ4/Zstd 会明确报错；压缩适配、磁盘按需读取和性能测量留待初审后的开发。
- 文件接口接收已加载的 Bytes；索引检查扫描整个文件，不宣称磁盘随机访问或仅解码候选块。录制器用复制验证保证原子性，尚非高吞吐流式写盘方案。
- `scan_file` 暴露未知原始记录；`Log` 是语义模型，读入后重新写出会重建索引且不保留未知/私有记录与扩展字段，不提供字节级无损编辑承诺。
- 恢复报告只说明可解析前缀，未提供校验和的数据无法证明未被篡改。压缩损坏块不恢复。
- 核心测试不等于完整验收已完成。初审后完善更广的外部夹具、性能/兼容测试与覆盖率说明，并发布 mooncakes.io；采用 MIT 开源许可证。

## 结构

`model`/`binary`/`codec` 定义格式；`reader`/`writer`/`chunk` 处理文件；`index_codec`/`indexed_writer`/`summary`/`attachment` 处理索引与附件；`query`/`indexed_query`/`transform`/`statistics` 提供操作；`stream`/`builder`/`recovery` 提供增量与恢复入口。`*_test.mbt` 为对应行为测试，`pkg.generated.mbti` 为自动生成公开接口。

## 原创与参考

本项目独立编写 MoonBit 实现，参考 [MCAP 公共规范](https://mcap.dev/spec) 和 [foxglove/mcap](https://github.com/foxglove/mcap)（MIT）的生态与测试行为，未移植其实现源码；不宣称 MCAP 格式本身原创。[公开项目查重记录](docs/duplication-review.md) 含检索范围与相邻项目差异。[项目申报书](docs/项目申报书.md) 留有个人信息空白。
