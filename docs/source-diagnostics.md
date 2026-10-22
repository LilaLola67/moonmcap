# 大文件诊断与恢复证据

`diagnose_source(ReadSource)` 按记录读取并执行完整结构、声明、索引、统计及已提供 CRC 的校验，不把整个文件或全部消息装入内存。同步核心支持 Wasm/JS；以下文件命令需要 Node.js 22.3+。

```sh
moon run cmd/diagnose --target js -- validate input.mcap
moon run cmd/diagnose --target js -- validate large.mcap --record-limit 3000000 --read-bytes 1048576
moon run cmd/diagnose --target js -- recover damaged.mcap --output extracted.mcap --max-ranges 10000 > recovery.json
python tools/source_diagnostic_workflows.py
python tools/large_source_workflows.py
python tools/large_source_workflows.py --large
```

构建后也可运行 `node _build/js/debug/build/cmd/diagnose/diagnose.js ...`。validate 有效退出 0，格式错误输出 JSON 并退出 1；参数和命令操作失败退出 2。底层读取失败也会成为首个诊断问题；取消和无效预算抛出错误。recover 使用新路径、临时文件和独占发布，失败不留下半成品。直接使用库的输出 sink 时，调用方须丢弃失败输出。

## 诊断证据

| 字段 | 含义 |
| --- | --- |
| valid / source_complete | 完整扫描通过；不代表未提供的 CRC 或密码学真实性也被验证 |
| data_valid | true：完整数据区通过；false：扫描数据时失败；null：元信息预检失败，尚不能判断数据区 |
| data_crc_verified | 非零 DataEnd CRC 已实际匹配；未提供时 false |
| observed_messages / observed_time_start/end | 已回调消息的数量及最小/最大时间；最终 CRC 失败时也只是已观察内容 |
| accepted_chunks | 已完整解析的块数量；仍可能随后发生全局校验失败 |
| parsed_data_prefix_end | 物理记录解析进度，不是已经证明无损的前缀 |
| failed_record_range | 已读完整物理帧的范围；不猜测块内损坏字节位置。帧头读取失败时可为 null |
| unchecked_range | 从解析进度到文件尾的未完成范围；之前可能已经读过元信息，不表示这些字节从未被访问 |
| issues | 首个失败及阶段、范围依据；全数据区 CRC 错误单独标明作用域与期望/计算值 |
| unknown_lost_messages | null；不推测消息丢失数量 |
| io | 成功读取字节数、底层请求次数、单次请求峰值和扫描到的最大物理帧 |

偏移、大小、消息数量及 UInt64 时间用十进制字符串，空值明确使用 JSON null。范围采用 `[start,end)`。先校验头尾与摘要，然后扫描数据，最后核对摘要语义；损坏摘要可能使扫描提前失败，不能据此断言数据区损坏或有效。遇首错停止，报告不是全部错误清单。数据 CRC 失败意味着整个数据区完整性未通过，不能把 DataEnd 所在帧当成唯一损坏位置。

## 资源限制

库可指定 `limits~`、`summary_bytes~`、`read_bytes~`、`cancelled~`。命令参数为 `--record-limit`、`--record-bytes`、`--decoded-bytes`、`--summary-bytes`、`--read-bytes`（仅 validate），默认分别为 100000、16 MiB、64 MiB、8 MiB、1 MiB。记录数包括块内记录，超过预算会明确失败，百万消息需要主动扩大数量预算。记录体与单块解压仍会完整驻留，声明与索引受独立预算约束；不同缓冲可能同时存在。这些预算不是进程 RSS 上限，也不承诺任意尺寸记录都可接纳。取消在记录和读取片段之间检查，解压计算不被抢占。

`scan_source` 新增可选 `trace` 回调及 `SourceScanEvent`，供自定义进度记录；RecordAccepted 仅说明当前记录已通过解析，后续全局检查仍可能失败。

## 恢复范围

`recover_blocks_detailed` 保留既有 `recover_blocks` 的严格接受条件，并提供有数量预算的逐块证据：

- `summary` 保留原恢复清单，始终声明部分提取，未检查整个源文件数据 CRC。
- `ranges` 列出源字节范围、恢复消息数、实际恢复消息时间极值、索引声明的时间范围和失败原因。
- 跳过块的实际时间及未知丢失消息数为 null；索引声明的时间范围只作为声明展示，不能证明恰好丢失了这些时间内的内容。

库支持 `max_ranges~`（默认 10000）；命令使用 `--max-ranges`。超预算失败，命令丢弃暂存输出。已有 `recover_blocks` 新增可选 `on_block` 回调，可逐块消费 `BlockRecoveryEvidence`；原返回结构与旧命令保持兼容。`diagnosis_json` 同时修正旧内存诊断的可选字段，使其按照文档输出标量或 null。

## 可复现验证

常规故障工作流覆盖三种压缩、索引/无索引/顺序布局、块 CRC、块长度、全数据 CRC、摘要 CRC、头部、截断、四类资源预算和恢复输出失败清理，使用官方 Python MCAP 工具交叉核对，检查输入 SHA-256 不变。CI 每次运行常规故障测试及缩小规模的资源测试。

`--large` 会实际写出 1,000,000 条消息和超过 2 GiB 的两个文件，不使用稀疏文件模拟；运行后逐个删除测试输入，保留 `_interop/large-source/report-large.json`。需要至少 3 GiB 空闲空间。报告记录字节数、SHA-256、消息数量、耗时和操作系统峰值 RSS，并由官方读取器逐条核对内容及 CRC。GitHub Actions 手动运行时勾选 `large_source` 可执行同等规模测试；普通 push 不执行 2 GiB 用例。观测结果见 [性能说明](performance.md)，不能外推为所有负载的性能保证。
