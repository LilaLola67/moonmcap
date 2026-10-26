# 按完整 Chunk 保存录入进度

```sh
moon build --target js
node _build/js/debug/build/cmd/chunkmerge/chunkmerge.js large.mcap --job chunk-work --output merged.mcap --compression zstd
# 进程退出后，使用相同输入顺序和预算：
node _build/js/debug/build/cmd/chunkmerge/chunkmerge.js large.mcap --job chunk-work --output merged.mcap --compression zstd --resume
node _build/js/debug/build/cmd/mcap/mcap.js job-inspect chunk-work
node _build/js/debug/build/cmd/mcap/mcap.js job-clean chunk-work
python tools/chunk_checkpoint_workflows.py
```

`resumable_chunk_merge` 是新增入口，原 resumable_merge 与文件级检查点不变。新模式接受 1–64 个完整索引输入，要求非零且匹配的摘要 CRC、可用 ChunkIndex，以及每个接纳块非零且匹配的 Chunk CRC。无索引、摘要损坏或无 Chunk CRC 的输入明确拒绝；不会默默降低验证保证。

录入按 Chunk 的原始物理位置推进。每个块通过解压、CRC、索引与声明检查后，写为独立、未压缩的临时 MCAP 副本，完成并同步后保存 CJOB1 检查点。检查点记录输入指纹、全部预算、完成输入数、当前块序号，以及每个副本的大小、消息数和 SHA-256。中断后复用已提交副本，不重建已完成块；未提交块重做，不续写半个压缩块。

**复用副本不等于免除最终验证。** 恢复仍完整核对输入 SHA-256；每个输入的块副本完成后，仍完整扫描并解压原输入，检查整体数据区 CRC、声明与摘要语义，并提取附件/元数据。此扫描会再次读取已经提交的原块，这部分工作不被块级保存点省略。未提供的数据区 CRC 在报告中为 false，不标成已验证。

全部输入完整校验后，以既有外部排序合并各副本，保持 `(时间,输入文件顺序,原始物理顺序)`，保留重复消息、附件和元数据。最终输出一次性独占发布，已有输出拒绝覆盖。最终排序阶段中断会复用全部块副本重新进行最终归并；当前入口不保存最终归并组中间进度。需要归并组保存点时，原文件级 resumable_merge 继续提供该能力。

文件命令让块副本、最终排序段和暂存输出都归同一个任务目录管理，通过编号区间隔离。恢复时只保留检查点引用的副本，清理未提交块和孤立最终排序段；进程强杀不会把排序文件散落到额外目录。锁、所有权标记、防覆盖及拒绝清理未知文件沿用 MergeJob。核心自定义适配器则须提供独立 parts_storage/merge_storage、互斥、同步、原子 save 和孤立文件清理。checkpoint_run_ids/checkpoint_progress 支持 CJOB1，旧 MJOB1/MJOB2 仍兼容。

默认最多 10,000 个副本（每个 Chunk 一个，每个输入另有一个辅助副本），可用 --max-parts 调整至 32768。检查点上限 8 MiB。spool_bytes 合并限制保留副本与最终排序文件，副本未压缩，空间可能明显大于源文件；最终归并只获得剩余预算。--record-limit 调整消息/记录上限；记录、单块解压、声明和输出摘要仍有独立预算。失败保留已提交任务；原输入不修改，重试前核对参数和空间。

核心测试通过保存点中断验证副本不被重建，核对稳定合并和附件/元数据，并要求最终数据 CRC 错误不产生输出。实际进程测试在文件系统提交点及最终排序文件创建点用隔离预加载桩暂停，再由操作系统终止进程；不修改生产实现。覆盖块级恢复、损坏副本、源变化、最终排序重试、孤立文件清理及独立读者结果核对。
