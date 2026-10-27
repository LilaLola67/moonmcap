# 统一异步查询

`asyncio.plan_query` 将原生 ChunkIndex/MIDX2、字段索引、工作线程解压、标准载荷解码和精确条件组合成 `AsyncQuerySource`，再交给异步 Dataset 合并和全局分页。源、索引、字段索引均可为本地文件或带强 ETag 的缓存 HTTP 数据源；调用者负责关闭这些句柄和工作线程池。

```powershell
moon build --target js
node _build/js/debug/build/cmd/query/query.js recording.mcap --index recording.midx2 --field-index recording.mfld --workers 2 --where '{"path":"/speed","op":">","value":10}' --select /speed --offset 5 --limit 20
```

`--index` 与 `--field-index` 可省略；提供时每个输入各对应一个。`--workers 0` 为默认值，正数启用工作线程，库用户需自行编译 worker 入口并传入路径，见 [工作线程](workers.md)。`--http-cache EXISTING_PARENT` 为三个来源各分配独立缓存。存在内容条件时自动解码，`--decode` 可在无条件时主动解码。未选择字段时输出沿用 Dataset NDJSON 的 `data_hex`，可用 `--where true` 请求 decoded 输出。

字段索引要求 SHA256 与解码器身份一致。无法判定的区域保留，剪枝后仍逐消息执行精确过滤，再执行 offset/limit。没有内部索引时使用已有的有界完整载入回退，字段索引不会把无索引输入伪装成选择性读取。

stderr 的 `plans` 区分 `opening_source_bytes` 和 `query_source_bytes`。打开时可能扫描完整源文件计算 SHA256，并审计完整 MIDX2；冷缓存不能宣称只传输命中区域。`tracked_regions/skipped_regions` 是最多 10000 个不同物理区域的观测值，超过时截断统计，查询本身继续正确执行；Chunk 的缓存命中也可能省略该观测。工作线程内存与队列限制见独立文档，工作线程并非所有负载都更快。

复现：`python tools/pipeline_workflows.py` 使用官方 Python MCAP writer 生成 768 条合成消息，对照独立排序/过滤结果执行 10 组查询，检查三种压缩、两个索引后端、两种工作线程设置、升降序全局分页、三个远程缓存的热启动及损坏字段索引的原子失败。
