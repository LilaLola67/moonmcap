# 按需读取与块缓存

```moonbit
let source = @nodeio.open_file("input.mcap") // JS，导入 LilaLola67/moonmcap/nodeio
let reader = @mcap.open_reader(source, cache_bytes=8 * 1024 * 1024)
let messages = reader.query({ ..@mcap.all_query(), topics: ["/imu"], start_time: 20UL, end_time: 30UL, })
let diagnostics = reader.diagnostics()
reader.clear_cache()
reader.validate_all() // 显式完整读取并校验，受 fallback_bytes 预算约束
```

运行真实文件示例：

```sh
moon run cmd/range --target js -- examples/files/zstd-summary.mcap /imu 20000000000 30000000000
python tools/range_benchmark.py
```

`ReadSource` 的长度及偏移为 UInt64，回调必须提供稳定的同一份内容。`read_exact` 重试短读，零进展、越界和超量返回立即失败。可用 `new_read_source(size, callback)` 实现其他宿主适配器；以 `io_error(message)` 构造宿主错误。Node.js 适配器使用文件身份、大小和纳秒修改/状态时间检查变化；偏移受 Node 安全整数范围限制。其他适配器需自行保证快照稳定性；当前未提供 HTTP 实现。

打开时读取首尾 magic、Header、DataEnd 边界、Footer 和摘要，检查摘要 CRC、分组、SummaryOffset、ChunkIndex 范围和重叠。查询按索引选择主题/时间相交的块，核对读取到的 Chunk 属性、CRC、声明和 MessageIndex，再按时间及物理顺序稳定排序分页。无消息索引的块仍按时间定位，读取后筛选主题。摘要和索引作为定位依据；快速模式不证明全部数据记录都被索引覆盖，也不校验未读取块或全数据区 CRC。需要完整容器保证时执行 `validate_all`。

无 ChunkIndex 的文件回退至完整读取和 `read_log`，默认大小上限 64 MiB；`fallback_bytes=0` 可关闭回退。摘要默认上限 8 MiB。返回消息数受 Limits.max_records 约束，结果数组会保留本次匹配候选，当前不是逐块游标，也不承诺固定进程内存。

缓存默认 8 MiB，以解压后的记录字节数记账，超预算块照常解码但不缓存；空块不缓存。淘汰最久未使用的块，命中会刷新顺序。`cache_bytes`/`peak_cache_bytes` 是原始解压负载预算，不包含对象、返回结果、编码器临时空间或进程 RSS。`read_calls`、`bytes_read` 是 ReadSource 的累计计数；同一源共享给多个读者时也共享计数。`decoded_chunks`、`cache_hits` 是当前读者的累计计数。

基准固定使用官方 Python Writer 生成 2,048 条、每条 8,192 字节的无压缩合成消息，查询 1000–1009 后重复查询；检查时间戳、缓存命中、缓存预算与实际读取量，不把单次耗时当作通用性能承诺。
