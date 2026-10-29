# 流式事件与上下文片段

`stream_context_clip` 逐条拉取窗口消息并交给 StreamWriter，不保留整个结果 Log。上下文单独受 context_bytes/context_messages 限制；输出总量使用 UInt64 预算，与常驻上下文、读取游标、Chunk 和摘要预算分别计账，不将其声称为进程 RSS 上限。

```json
{
  "stream": true,
  "start": "20000000000", "end": "30000000000",
  "topics": ["/imu"],
  "context": [],
  "context_bytes": 8388608,
  "context_messages": 10000,
  "output_messages": "10000000",
  "output_payload_bytes": "107374182400",
  "copy_auxiliary": true
}
```

在 incident clip/events 原有配置增加 `stream:true` 启用；原内存路径保持兼容。`output_messages`、`output_payload_bytes` 是十进制字符串。流式命令的输入记录预算 `input_records` 默认 30000000，可显式调整；输入解析、单记录大小和单块解压仍受独立限制控制。未分块输入仍可能触发原 RangeReader 的有界内存回退，不宣称任意无索引输入都流式处理。

默认上下文预算为 8 MiB/10000 条；保持每话题最后 N 条、最大回溯时间与必需/可选语义。先验证和收集有界上下文，再稳定排序这些消息，然后写出窗口中的升序消息。会保留来源所有声明，避免预扫描整个窗口来剪枝声明；此行为与内存片段的声明剪枝有所不同，消息及引用语义保持一致。

`copy_auxiliary:true` 在原始源上再次执行完整扫描，保留全部附件和元数据，包括时间窗外的记录；默认不复制。核心 `copy_clip_auxiliary` 另外接受附件及元数据名称允许列表。复制成本包含再次读取/解压整个输入；大附件仍受单记录大小限制。此策略在提取阶段不会修改原始辅助记录。

核心只写入调用方提供的 sink，失败时可能留下不完整前缀；调用方须管理事务和资源。文件 CLI 使用已有 OutputTransaction，失败时不发布目标，已有文件拒绝覆盖。报表包含上下文记账、累计载荷、Chunk 峰值及保留摘要大小；生成元数据附来源指纹和事件证据。

## 批量窗口与恢复

配置 `batch.json`，时间窗口两端包含，其他预算和上下文配置与流式 clip 相同：

```json
{
  "windows": [
    {"start": "20000000000", "end": "30000000000"},
    {"start": "60000000000", "end": "70000000000"}
  ],
  "topics": ["/imu"],
  "context": [{"topic": "/calibration", "depth": 1, "required": true}],
  "copy_auxiliary": true
}
```

```sh
moon run cmd/incident --target js -- batch input.mcap batch.json new-job
moon run cmd/incident --target js -- resume-batch input.mcap batch.json new-job
```

输出在专用新目录中命名为 `clip-0.mcap`、`clip-1.mcap`。也可以把 events 命令报告中的窗口起止时间放入该配置；直接执行 events 的旧前缀输出方式仍按文件发布，不提供该任务恢复机制。`stop_after` 控制本次最多新执行的片段数，默认 4096，改动此项允许恢复；其他处理配置、输入 SHA-256、窗口顺序、库版本的改动均拒绝复用旧进度。最多 4096 个窗口，每份报告最多 1 MiB，整个 ABAT1 检查点最多 8 MiB。

核心 `run_artifact_batch` 接受存储适配器和逐片段执行回调。每个片段完成后，先刷新暂存文件，再把 SHA-256、执行报告和待发布状态写入带 CRC 的检查点，然后发布文件，最后提交完成状态。重启会校验所有已完成输出的 SHA-256，并补完待发布片段；没有完整收据的未完成 `.part` 会重做。恢复点位于完整输出之间，当前不恢复到单个输出内部或 Chunk 中间。孤立最终文件、损坏的收据和未知目录文件会报错，不会被当成成功，也不会被覆盖。

Node.js 适配器要求目录独占，拒绝活跃进程持有的锁；死进程的锁经独占恢复保护后重新获取。若进程恰好在锁恢复的临界区再次被终止，可能留下 `recovery` 目录，此时保守拒绝自动接管；应先确认无运行任务，再检查并移除这个空保护目录。文件会执行 fsync，支持的平台还会刷新目录；Windows 上不承诺断电后的目录持久性。该机制覆盖进程中断恢复，不等同于跨机器分布式协调。

验证：`python tools/stream_clip_workflows.py`。独立 Python writer 构造三种压缩输入，每个输出包含 1100×65536 字节的窗口载荷和上下文；独立 reader 开启 CRC 检查并逐条核对原始字节、序号和顺序。另检查附件保留、输出/上下文超限时无最终文件、已有输出拒绝及输入指纹不变。数据为合成数据，不冒充真实传感器采集。

`python tools/clip_batch_workflows.py` 实际终止正在输出的 Node.js 进程，再执行恢复命令；独立 reader 校验输出序号、顺序和 CRC。还验证完成文件不重写、活锁拒绝、配置变更拒绝、输出篡改和检查点损坏。单元测试额外在收据保存后和文件发布后注入故障，验证两侧均可恢复且不会重复执行已准备的片段。
