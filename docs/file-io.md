# 文件读取生命周期与性能

0.15.6 同步与异步 Node 文件桥接直接传递 Uint8Array/Bytes，文件写入和 SHA-256 更新不经过十六进制编码。read_exact 在一次读取满足长度时直接返回不可变字节，短读仍重试并拒绝零进展/越界；公开负载没有改为可变缓冲区。

`nodeio.open_file` 保持无需 close 的旧接口，每次物理读取打开和关闭文件。需要复用文件句柄时，使用 `open_managed_file`，并在成功和失败路径都调用 `ManagedFile.close`。`file.source()` 可供 RangeReader、SidecarReader、扫描或外部合并使用，close 可重复调用，关闭后正长度读取报错。

`read_ahead_bytes` 默认 65536，允许 0..16 MiB。预读仅缓存一个连续窗口；大于预算的请求直接读取，不保留该大块。预读是同步拉取，不创建后台任务，不改变查询游标的顺序和取消行为。每次请求（包含缓存命中）核对文件描述符及路径身份；源大小、inode 或修改时间变化时拒绝旧缓存。返回缓存片段时复制为独立字节，避免后续缓存更新修改已返回的负载。

`FileScope` 管理一组 ManagedFile，重复打开相同路径复用读取器；默认最多 64 个文件，可配置 max_open，超过数量报错。`scope.close()` 会尝试关闭全部文件后报告错误。CLI 在单次运行范围内使用最多 1024 个输入句柄，每文件最多 64 KiB 预读，并在结束时统一关闭。排序中间文件仍采用无需持有句柄的存储适配器，避免大量临时段耗尽句柄。

`ManagedFile.metrics()` 返回 physical_reads、physical_bytes、cached_bytes、closed；这是底层文件操作计数。RangeReader 原有诊断计数统计 ReadSource 逻辑请求，两者口径不同。缓存占用不超过指定预读预算，读请求返回的 payload 和解压缓存另按既有预算管理。

```moonbit
let scope = @nodeio.file_scope(max_open=8, read_ahead_bytes=65536)
let result = try {
  let reader = @mcap.open_reader(scope.open("input.mcap"))
  Ok(reader.query(@mcap.all_query()))
} catch { error => Err(error) }
scope.close()
// 再处理 result；失败时句柄也已释放。
```

`python tools/io_workflows.py` 对同一 4 MiB 加尾部字节文件进行三轮连续读取，每模式运行三次取中位数，对比旧十六进制桥接、直接字节桥接和带预读的托管句柄。SHA-256 和读取摘要必须一致；CI 检查预读占用和物理读取次数，不对机器相关的耗时/RSS设置阈值。测量命令位于 cmd/io，旧桥接仅作可复现基线。异步文件接口本版移除文本转换，生命周期仍沿用原有 AsyncSource/AsyncSink，每次 I/O 自动关闭文件。
