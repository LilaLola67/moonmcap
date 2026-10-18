# 磁盘消息游标

```moonbit
let reader = @mcap.open_reader(@nodeio.open_file("input.mcap"))
let cursor = reader.cursor({ ..@mcap.all_query(), topics: ["/imu"], limit: 1000, })
let page = cursor.next_page(32)
cursor.seek(20000000000UL)
let progress = cursor.progress()
cursor.cancel()
```

RangeReader::cursor 复用摘要和解压缓存。按 ChunkIndex 起始时间（倒序时为结束时间）排序候选块；只有当未加载块不可能排在当前堆头之前时，才返回消息。时间边界相等时也先加载块，以保留原文件的同时间顺序。跨块时间重叠可能需要同时保留多个块的候选，不承诺任何分布都只读一个块。

pending_bytes 默认 64 MiB，按候选消息的 31 字节记录开销加 data 长度计数；span_bytes 默认 32 MiB，限制一次块及其消息索引跨度。超过预算进入 FileFailed，不丢弃或重排数据。该计数不包含缓存、元数据、对象和压缩器临时空间，不是进程 RSS 上限。缓存仍受 RangeReader 的独立预算限制。

offset/limit 作用于整个遍历；seek 在原查询闭区间内重启，不重用 offset，重置 limit/进度。取消和失败均不能重开；非法页大小和区间外跳转不推进游标。返回的页由调用者持有，不计入内部候选预算。

无索引文件使用已声明大小预算的完整扫描回退，progress.used_fallback 明确标记；零 limit 不触发回退。进度记录已加载候选块、返回/跳过消息数与峰值候选负载，不在未读完时虚构精确总匹配数。

poll() 不执行文件 I/O，返回 FileMessage、FileRead(ReadSpan) 或 FileDone。宿主异步读取 FileRead 所需的精确跨度后调用 feed(bytes)，再继续 poll。同步 next/next_page 是此协议的驱动器。feed 解码、校验和接纳整个块后才推进；错误使游标终止。快速模式的校验范围仍为摘要及读取到的块，不替代完整文件校验。
