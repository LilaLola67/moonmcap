# 有预算约束的外部排序与合并

```sh
moon run cmd/mcap --target js -- external-merge a.mcap b.mcap --temp-dir ./temporary --output merged.mcap --compression zstd --run-bytes 2097152 --merge-bytes 16777216 --fan-in 8
python tools/external_workflows.py
```

临时父目录必须存在。每次任务在其中独占创建子目录，仅清理本次已知排序段和空任务目录；不递归删除父目录或其他文件。输出先流式写入同目录的随机暂存文件，完整完成后通过独占硬链接发布，现有目标（包括输入本身）不会被覆盖。需要支持硬链接的文件系统。普通错误/协作取消会清理；进程强制终止或断电可能留下暂存文件，不承诺崩溃清理或目录级断电持久性。

库接口 `external_merge(sources, storage, output, options~, cancelled~)` 接收按偏移源、SortStorage、WriteSink 和 ExternalOptions。可用 sort_storage 回调适配其他宿主；算法及输入/临时文件编解码在 MoonBit 中，磁盘适配器为 nodeio.sort_directory。便携测试以内存存储替代磁盘，不改变排序逻辑。storage.finish 在成功或普通失败后均执行；清理失败会报告，不隐藏为成功。

逐记录读入和校验输入，支持顺序、分块及三种压缩。校验数据区 CRC、声明顺序、块、索引、摘要和 Statistics；保留所有消息、附件、元数据，不做隐式去重。schema/channel 按内容去重并分配全局 ID，不兼容 profile 拒绝。未知记录/扩展字节不保证保留。

消息按 log_time 升序，同时间按输入文件次序及文件内物理次序稳定排列；UInt64 时间不会转为浮点或有符号整数。先在 run_bytes 内排序并生成临时段，再以 fan_in 个段归并，必要时生成中间段。临时格式含总记录数、每条 CRC 和全局稳定序号，读取时检查截断、尾部、计数和顺序。

| 参数 | 默认值与约束 |
| --- | --- |
| --run-bytes | 2 MiB，单条消息须容纳其负载加 43 字节暂存开销 |
| --merge-bytes | 16 MiB，归并时当前头部消息负载及暂存开销总和 |
| --fan-in | 8，允许 2–64 |
| --spool-bytes | 512 MiB，所有存活临时文件的实际累计字节上限，含中间段重叠 |
| --run-limit | 32768，本次创建的临时段总数上限 |
| --declaration-bytes | 8 MiB，全局声明及每个输入的声明/索引属性另行约束 |

输出沿用 StreamOptions 的块和摘要预算；输入块按 Limits 解压，默认上限 64 MiB。预算分开记账，排序缓冲、归并头、声明表之外仍有输入解压块、编码器临时空间、宿主桥接及输出缓冲，不能将 run_bytes 或 merge_bytes 当作总进程内存上限。完整文件不作为一个 Bytes 载入；本命令不使用 --max-file-bytes。

报告提供 runs_created、merge_passes、peak_run_bytes、peak_merge_bytes、peak_spool_bytes 和 output_bytes。cancelled 回调在记录、临时写入和归并步骤之间检查；同步 CLI 不提供异步任务抢占。若输入损坏，合并失败，不自动进入恢复模式。
