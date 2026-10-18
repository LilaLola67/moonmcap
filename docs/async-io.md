# 异步读写与分段录制

导入 `LilaLola67/moonmcap/asyncio`，在 async 函数中使用 `open_file`、`open_reader`、`AsyncCursor::next_page`；使用 `create_sink`、`new_writer`、`append/flush/finish` 进行异步输出。Node 文件适配器使用 fs/promises，核心编码、校验和排序仍在 MoonBit 中执行。原有同步接口保持可用。

```sh
moon run cmd/async --target js -- query examples/files/zstd-summary.mcap /imu 20000000000 30000000000
moon run cmd/async --target js -- record recording.mcap zstd
moon run cmd/async --target js -- rotate recording zstd
python tools/async_workflows.py
```

AsyncSource 可由 `new_source(size, async read_at, abort~)` 适配其他宿主。read_exact 重试短读并检查越界、零进展、超量返回和取消；异步打开仅预取 magic、Header、DataEnd 与摘要。磁盘游标按 poll/feed 协议异步取得数据块，复用同一校验与排序实现。无索引回退仍受完整读取预算限制。

AsyncWriter 的同步编码器只向有预算的队列产生记录，drain 等待宿主接纳全部输出后才允许后续操作。默认队列 32 MiB，构造时检查可容纳块和索引；单次操作或最终摘要超过预算会报错。每个 sink 只能由一个 writer 占用；同一 writer/cursor 不允许并发操作。不要将编码器位置当作物理写入量，progress 同时提供 encoded 和实际 bytes_written。

输出失败为终止状态，不重试可能已部分落盘的记录。cancel 是协作取消，不能撤销已提交写入。AsyncSource.cancel 关闭整个源并调用 abort；AsyncCursor.cancel 仅取消本游标，不破坏共享源。取消当前异步任务也会通过官方 Promise 桥传播到支持 AbortSignal 的宿主操作。普通文件 I/O 在返回后观察取消。失败或取消的文件可能只是可恢复前缀，不生成虚假的成功 Footer。

RotatingWriter 接收仅含声明的 Log 模板与异步 sink 工厂。按 segment_messages（硬消息数量）、segment_span（日志时间跨度）或 segment_bytes_target（已写字节目标）轮换。字节目标在消息之间检查，可能超过目标一个待刷新块及最终摘要，不是严格文件大小上限。每个完成段含全部声明、独立索引、Statistics 与 Footer。空录制不创建段；段数受 segment_limit 约束。segments 仅列出已完整完成的段，未完成当前段不混入成功清单。

异步宿主目前支持 JS/Node；核心同步编解码与磁盘游标仍支持 Wasm。依赖固定为 Apache-2.0 的 moonbitlang/async 0.20.2，使用其官方 [Promise 互操作接口](https://docs.moonbitlang.com/en/latest/language/async-experimental.html)。测试除内容对照外，还检查事件循环在实际文件录制期间继续推进；这些预算均不是总进程 RSS 上限。
