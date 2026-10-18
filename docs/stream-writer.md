# 流式录制

```moonbit
let sink = @nodeio.create_sink("recording.mcap") // 独占创建，已有路径会报错
let writer = @mcap.new_stream_writer(sink, options={
  ..@mcap.stream_options(), compression: @mcap.Zstd,
  chunk_bytes: 256 * 1024, chunk_messages: 256, chunk_span: 1000000000UL,
})
writer.add_channel({ id: 1, schema_id: 0, topic: "/events", message_encoding: "raw", metadata: [], })
writer.append({ channel_id: 1, sequence: 0U, log_time: 100UL, publish_time: 100UL, data: b"event", })
writer.flush()
let result = writer.finish()
```

先注册 Schema 再注册 Channel，再添加 Message。ID 和 sequence 由调用者明确指定；无 schema 时用 schema_id=0。同 ID 冲突声明被拒绝，完全相同的声明不重复写入。附件和元数据在调用时输出，其数据体不保留在录制器历史中。

```sh
moon run cmd/stream --target js -- recording.mcap zstd
python tools/stream_workflows.py
```

示例逐条生成 1,000 条消息，不构建完整 Log。验证脚本在 none/lz4/zstd 下分别生成实际文件，用官方读者验证消息、索引、Statistics 和全部可用 CRC，并验证已有输出不会被覆盖。

## 刷新、预算与背压

- 达到消息数、待加入消息将超过字节数、加入后时间跨度将超限时，先刷新旧块。chunk_span=0 关闭时间阈值；乱序时间按当前块最大值减最小值计算。
- 单条消息必须装入 chunk_bytes，过大时拒绝，不静默增大预算。配置为压缩帧预留 128 字节，并分别检查实际编码记录及解压大小限制。
- chunk_bytes 约束待写记录负载；summary_bytes 约束保留的编码声明/索引及最终摘要。摘要预算不足会失败，不丢弃索引以假装成功。
- 没有保留历史 Message 数组。编码压缩仍使用完整单块缓冲，结束时暂存摘要；实际对象、压缩器、宿主十六进制桥接和调用者缓冲不计入负载计数，不能将其当作进程 RSS 上限。
- WriteSink 回调同步接纳整个 Bytes 或抛出 McapError；回调返回前不会产生下一次写入，形成同步背压。宿主写入短写应在回调内重试；Node 适配器已处理。异步队列和协作取消通过单独的 [asyncio 包](async-io.md) 提供，同步核心没有线程抢占取消。

## 状态和失败

WriterOpen 可以添加、flush、finish 或 cancel。finish 成功变为 WriterFinished，重复结束或继续添加报错。输出异常或刷新/结束失败变为 WriterFailed，不能重试写入；cancel 将打开的写入器变为 WriterCancelled，重复取消无副作用。参数校验失败且尚未刷新/写入时，已有消息不变。

取消、磁盘写满或进程中断可能留下尚无 Footer 的文件前缀；它不是成功完成的日志，可以另用 recover_prefix 恢复已完整写入的块。直接使用 create_sink 不提供事务回滚、fsync 持久性承诺或自动删除失败输出。一个 sink 必须由一个 writer 独占使用。
