# 结构化诊断与索引修复

```sh
moon run cmd/mcap --target js -- validate input.mcap
moon run cmd/mcap --target js -- reindex damaged-summary.mcap --output rebuilt.mcap --compression zstd
python tools/repair_workflows.py
```

库接口：`diagnose_file(bytes)` 返回 FileDiagnosis；`rebuild_indexes(bytes, compression~, messages_per_chunk~)` 返回 IndexRepair（新文件 bytes、原始诊断 original、输出诊断 result）。诊断处理格式错误而不抛出 McapError；I/O 仍由宿主读取接口处理。资源上限沿用 Limits。

## 诊断字段

| 字段 | 含义 |
| --- | --- |
| valid | 完整文件通过结构、声明、索引、统计及所有已提供 CRC 校验 |
| data_valid | 找到完整 DataEnd，数据记录解析和已提供校验通过；不表示索引/摘要有效 |
| data_crc_verified | 完整数据区非零 CRC 已实际匹配；CRC=0 表示未提供，不标记已验证 |
| messages / chunks | 已解析并接纳的消息/完整块数量，不是推测的原始总数 |
| data_end | 有效 DataEnd 的位置；未找到或数据校验失败为 null |
| issues | 结构化问题列表，数据区遇首个错误停止，不能视为穷尽所有问题 |

每个问题含 code、message、offset、opcode、expected、actual。位置和数值在确定时提供，不确定则为 null；JSON UInt64 值使用十进制字符串。截断记录提供记录起点、期望长度和实际剩余长度，数据区 CRC 错误提供预期与计算值。全局索引关系错误不伪造单个精确偏移。

code 为 bad_envelope、bad_record、bad_declaration、bad_compression、bad_checksum、bad_index、bad_summary、truncated_input、limit_exceeded。代码来自已知检查阶段及底层错误上下文，message 保留具体原因。

## 修复边界

- 数据区完整且通过解析与已有 CRC 校验时，允许忽略损坏的摘要、索引或缺失的文件尾，重新生成容器、Statistics、索引、摘要与校验和。
- 数据区截断、消息声明关系错误、压缩块损坏或数据区 CRC 不匹配时，rebuild_indexes 拒绝；不会跳过损坏字节拼接后续数据。
- 如要挽救截断文件，使用 recover/recover_prefix，明确接受只取连续有效前缀。停止位置、原因和源文件诊断会报告，不能恢复的附件或消息不会被补造。
- CRC=0 的内容仍可解析，但没有相应完整性证明。data_valid 不是对原始数据从未损坏的保证。
- 重建保留当前 Log 模型支持的消息、声明、附件和元数据；不承诺未知记录、扩展字段或原始编码的逐字节保留，不解码消息负载。
- CLI 写入新路径，原文件不变；已有输出拒绝覆盖。validate 对有效文件退出 0，对无效文件返回诊断并退出 1；参数、I/O 或修复失败退出 2。

故障工作流分别覆盖三种输入压缩的坏 ChunkIndex、坏摘要 CRC、缺失尾部、截断数据和错误数据 CRC；修复输出由官方读者独立检查，失败路径核对原文件 SHA-256 未改变。
