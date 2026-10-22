# 独立块部分恢复

0.15.12 新增 `recover_blocks_detailed` 和 `cmd/diagnose recover`，给出实际恢复时间、索引声明时间及明确的未知丢失数量，支持范围数量预算。见 [恢复证据](source-diagnostics.md)。原命令和返回结构保持兼容。

`recover` 继续只恢复连续前缀，遇到损坏就停止。`recover-blocks` 是另一个显式操作：依靠已通过非零 CRC 校验的摘要 ChunkIndex 定位每个块，接受完整通过解压、非零 Chunk CRC、声明副本、时间/压缩属性和消息索引验证的块。坏块不会阻止检查后面的独立块，不扫描坏字节猜测下一条记录。

```sh
moon run cmd/mcap --target js -- recover-blocks damaged.mcap --output extracted.mcap --compression zstd > recovery-manifest.json
python tools/block_workflows.py
```

输出采用临时文件完成后独占发布；输入和已有输出不会被覆盖。失败时清理暂存文件。核心 API `recover_blocks(source, sink)` 支持自定义随机访问输入和流式输出；直接调用者负责失败输出的丢弃。

清单列出 `recovered` 和 `skipped` 的原始字节区间 `[start,end)`，包含对应消息索引区域，偏移用十进制字符串保存。成功块给出实际恢复消息数；失败块只给错误原因，不从摘要时间范围推断丢失消息数量或时间。输出重新生成有效索引、统计和 CRC。

这是**索引块范围的部分提取**：不接纳分块外消息、附件、元数据、未知记录，也不验证整个源文件 DataEnd CRC。`source_complete` 始终为 false，`source_data_crc_checked` 为 false；不能据此宣称完整恢复、损坏区域没有消息或来源具有密码学真实性。摘要/尾部损坏、摘要 CRC 未提供、没有索引块时拒绝此模式；缺少 Chunk CRC 的块列为跳过。没有任何可独立验证的块时失败。可按损坏类型另选连续前缀恢复或完整数据区索引重建。

单块记录/解压预算、摘要预算、总接纳记录预算和流式输出预算都生效，不积累整个恢复文件。示例测试覆盖三种压缩的中间块 CRC/长度损坏，以及摘要损坏拒绝；官方 Python MCAP 工具独立校验恢复后的文件。
