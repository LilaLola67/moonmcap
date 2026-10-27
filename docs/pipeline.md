# 统一异步查询

`asyncio.plan_query` 将原生 ChunkIndex/MIDX2、字段索引、工作线程解压、标准载荷解码和精确条件组合成 `AsyncQuerySource`，再交给异步 Dataset 合并和全局分页。源、索引、字段索引均可为本地文件或带强 ETag 的缓存 HTTP 数据源；调用者负责关闭这些句柄和工作线程池。

```powershell
moon build --target js
node _build/js/debug/build/cmd/query/query.js recording.mcap --index recording.midx2 --field-index recording.mfld --workers 2 --where '{"path":"/speed","op":">","value":10}' --select /speed --offset 5 --limit 20
```

`--index` 与 `--field-index` 可省略；提供时每个输入各对应一个。`--workers 0` 为默认值，正数启用工作线程，库用户需自行编译 worker 入口并传入路径，见 [工作线程](parallel-codecs.md)。`--http-cache EXISTING_PARENT` 为三个来源各分配独立缓存。存在内容条件时自动解码，`--decode` 可在无条件时主动解码。未选择字段时输出沿用 Dataset NDJSON 的 `data_hex`，可用 `--where true` 请求 decoded 输出。

字段索引要求 SHA256 与解码器身份一致。无法判定的区域保留，剪枝后仍逐消息执行精确过滤，再执行 offset/limit。没有内部索引时使用已有的有界完整载入回退，字段索引不会把无索引输入伪装成选择性读取。

stderr 的 `plans` 区分 `opening_source_bytes` 和 `query_source_bytes`。打开时可能扫描完整源文件计算 SHA256，并审计完整 MIDX2；冷缓存不能宣称只传输命中区域。`tracked_regions/skipped_regions` 是最多 10000 个不同物理区域的观测值，超过时截断统计，查询本身继续正确执行；Chunk 的缓存命中也可能省略该观测。工作线程内存与队列限制见独立文档，工作线程并非所有负载都更快。

复现：`python tools/pipeline_workflows.py` 使用官方 Python MCAP writer 生成 776 条合成消息，对照独立排序/过滤结果执行 11 组查询，检查三种压缩、两个索引后端、两种工作线程设置、升降序全局分页、三个远程缓存的热启动及损坏字段索引的原子失败。额外高压缩比夹具确认小压缩跨度也可触发实际 worker 解压，任务按该 Chunk 的真实声明展开大小预留缓冲。

## MFLD2 分页字段索引

```powershell
node _build/js/debug/build/cmd/field/field.js build-paged recording.mcap recording.mfld2 --field /speed --field /label --page-bytes 65536 --page-zones 32
node _build/js/debug/build/cmd/query/query.js recording.mcap --field-index recording.mfld2 --where '{"path":"/speed","op":">","value":10}' --select /speed
```

统一入口自动识别 MFLD1/MFLD2。MFLD2 包含绑定源 SHA256/解码器身份的有界头，以及固定长度、CRC 保护、物理顺序排列的页。构建时逐区域汇总，只保留当前区域和一页；超出页预算时明确失败，可降低 `--page-zones` 或增大 `--page-bytes`，不会偷偷扩大内存。输出通过临时文件校验完成后发布。

`asyncio.open_paged_fields` 打开时完整审计页 CRC、区域边界和顺序，保留至多指定页数（默认 4，允许 0..64）；查询二分查找页，页缓存采用 LRU。打开审计读取全索引，不是零扫描启动。字段页还须受调用者 HTTP 缓存预算约束。该格式使用定长页换取直接定位，稀疏页有空间开销。

每个字符串字段可选 256 位、4 哈希 Bloom（`--no-bloom` 关闭）。只用于字符串相等及保守 AND/OR 推断，否定等无法安全判断的条件保留；未知解码区域、缺失摘要、不同跨度也保留。精确过滤始终执行。CRC 和 SHA256 防误用与意外损坏，摘要并非不可信第三方提供的证明。

复现 `python tools/paged_field_workflows.py`：独立 writer 构建 12032 区域的合成大索引，超过旧格式默认一万区域限制；验证三个压缩后端、六组分页查询、最多四页缓存，以及损坏页原子失败。Bloom 的区间内缺失值与已存在值另有单元测试。
