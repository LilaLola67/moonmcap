# 可恢复的外部合并任务

0.15.5 在同步 Node 文件入口提供持久任务。原有 `external-merge --temp-dir` 继续在退出时清理；`--job` 模式保留工作文件，便于进程中断后恢复和检查。

```sh
node _build/js/debug/build/cmd/mcap/mcap.js external-merge a.mcap b.mcap --job merge-work --output merged.mcap --run-bytes 2097152 --fan-in 8
node _build/js/debug/build/cmd/mcap/mcap.js job-inspect merge-work
# 中断后用相同输入顺序、预算和输出配置恢复：
node _build/js/debug/build/cmd/mcap/mcap.js external-merge a.mcap b.mcap --job merge-work --output merged.mcap --run-bytes 2097152 --fan-in 8 --resume
# 确认不再需要任务后，清理工作目录；已发布的输出文件保留：
node _build/js/debug/build/cmd/mcap/mcap.js job-clean merge-work
```

任务目录必须是新建的专用子目录，与输出位于同一文件系统（发布使用硬链接）。输入路径和声明的参数由命令指定，任务文件不写入仓库。最多 64 个输入，其他内存/磁盘/记录预算沿用 external_options；检查点最多 8 MiB、排序段上限 32768。声明、附件和元数据存入独立辅助 MCAP，其字节也计入临时磁盘总预算。

保存点是全部输入完成严格校验、排序段落盘之后，以及每组中间归并完成之后。检查点含版本、CRC、输入顺序与 SHA-256、辅助文件与各存活排序段摘要、完整预算/输出配置及计数。先同步排序段，再同步临时检查点并原子重命名，成功后才删除替代的旧排序段。恢复会完整验证 SHA-256、排序段帧 CRC、计数和稳定次序；输入或参数变化、排序段损坏、检查点版本未知均拒绝。

恢复复用已提交的排序段，不重新解析压缩输入。输入读取阶段中断会重做整个输入阶段；正在写的一组归并会从该组开头重做；最终输出中断会复用最终排序段重写输出。不会从任意字节继续追加 MCAP。完成文件一次性发布，已有目标拒绝覆盖；若进程在发布后、完成标记写入前退出，输出可能已经存在，应先核验输出再清理任务，不覆盖该文件。

本地任务锁拒绝存活进程的并发操作，确认原进程不存在后可接管遗留锁；PID 被复用时保守拒绝。此适配器面向单机本地文件系统，不用于跨主机共享锁。清理仅接受带专用所有权标记且内容符合白名单的目录，拒绝未知文件、目录或符号链接；没有递归删除。不要同时手工编辑任务目录。断电持久性取决于文件系统和宿主 fsync 支持，验证覆盖的是进程终止恢复。

便携核心入口 `resumable_merge` 接受 SortStorage、强摘要回调及原子 save 回调，保留提交的段供调用者管理；`checkpoint_run_ids` 提供不含路径的存活段编号。自定义存储适配器负责互斥、同步、原子替换，以及删除检查点未引用的未提交段。Node 入口 `open_merge_job` 实现这些职责，并提供 checkpoint/storage/output/save/inspect/complete/close/clean。

`python tools/checkpoint_workflows.py` 真正终止归并进程，恢复后通过官方 Python MCAP reader 验证稳定消息顺序、附件和元数据，并检查参数变化、临时段损坏、存活锁及拒绝清理未知文件。
