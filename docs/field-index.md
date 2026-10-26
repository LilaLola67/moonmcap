# 字段范围索引与保守块排除

MFLD1 是 MoonMCAP 自有的字段摘要侧车格式，不是 MCAP 标准记录或 MIDX2 替代品。它按物理块保存指定字段的数值/字符串最小值和最大值、出现性，以及是否包含无法解码的消息。索引不保存全部消息和 payload；源内容 SHA-256、源大小、解码语义标识和 CRC 用于版本匹配与损坏检测。

```sh
moon build --target js
node _build/js/debug/build/cmd/field/field.js build input.mcap input.mfld --field /temperature --field /device
node _build/js/debug/build/cmd/field/field.js query input.mcap input.mfld --where '{"path":"/temperature","op":">","value":80}' --select /temperature --limit 20 --output hot.ndjson
python tools/field_index_workflows.py
```

库入口是 build_field_index、FieldIndex.to_bytes/open_field_index、RangeReader.plan_field_query 和 FieldQueryPlan.query_source/info。计划返回的 provider 已组合标准解码与最终精确字段筛选，可直接加入 Dataset，再应用全局 offset/limit。JSON、Protobuf 和已支持的 ros2msg/XCDR1 都通过 PayloadDecoder 生成字段；自定义插件必须提供稳定、正确的 decoder_identity，查询需使用同一语义。CLI 固定使用默认标准数值模式及其语义标识。

## 哪些块能够排除

- 已索引路径上的数字和字符串支持 =、>、>=、<、<=，数字比较沿用精确十进制，不先转换为 Double。
- exists 可利用字段出现性；all/any 递归组合保守结论。
- not、!=、contains、未索引路径和无法证明不命中的情况保留正常扫描。字段缺失、null 与数值类型不同不会混为同一个数值范围。
- 块中存在解码失败或无法统计的值时，该块标记 unknown，整块保留，随后正常解码/筛选；不会通过范围摘要隐藏错误。
- 摘要缺失、位置跨度不匹配的块保留；无内置 ChunkIndex 的源继续走原有预算回退，不承诺加速。完全排除所有块时返回空流，避免误触发全文件回退。

索引只缩小候选块，不能替代最后逐条执行条件。时间、主题、稳定排序、正反序、offset/limit 的既有含义保持不变。

## 校验与信任

构建先完成严格容器扫描，失败不发布索引；默认标准解码失败会保留 unknown 标记。打开和计划准备验证源指纹及解码语义，CLI 对同一稳定打开文件复用一次实际计算的指纹。完整指纹检查会读完整个源，因此首次查询的总读取量不一定降低；块排除减少的是随后查询阶段的读取、解压和解码。info 单独报告查询阶段读取字节、候选/排除块和实际解压数量。

CRC 与源 SHA-256 不是对摘要正确性的签名：能够重写侧车的人也能伪造字段范围。因此 MFLD1 必须来自自己构建或可信来源，不应把不可信方生成的摘要当作可证明不遗漏的索引。这与普通索引的信任边界相同；需要独立核对时可重建索引并与无摘要查询比较。

查询仍为选择性校验，只检查读取块，不能宣称验证未读取的数据区 CRC。源和索引必须保持稳定快照。源指纹、解码器配置变化或侧车损坏明确拒绝；失败输出须丢弃，CLI 新文件输出原子发布且拒绝覆盖。

## 资源与范围

每个索引默认最多 16 个不同 JSON Pointer、10000 个物理区、8 MiB 估算摘要状态。单独的声明/消息/字符串/解压预算继续生效。编码默认最多 16 MiB，解码同时限制 JSON 深度 32 和节点 100000；编码前检查同样限制，超限报错，不产生自己不能读取的索引。逻辑状态预算不是 RSS 限制，极长的单字段极值仍可能超预算。

解码器缓存与索引摘要状态分别计账。未分块消息也能记录摘要，但当前加速计划只排除内置 ChunkIndex 对应块。首版提供同步 RangeReader 计划和 Node.js 文件命令，不提供异步字段摘要计划、Bloom filter、字符串子串索引或自动代价优化器。

验证含三种压缩、精确 64 位整数、30 组独立条件/分页对照、双向遍历、零命中零解压、未知块错误保留、CRC 破坏与独占输出。选择性条件在该固定测试分布排除超过 80% 的块，属于已测案例，不代表所有日志的加速比例。
