# 从 Chunk 录入到最终归并的持久恢复

0.15.23 新增 resumable_chunk_pipeline 与 CJOB2，在同一任务中保存已校验的 Chunk 副本、最终输入排序段和已提交归并组。原 resumable_chunk_merge/CJOB1 及 MJOB1/MJOB2 接口保持兼容。

```sh
moon build --target js
node _build/js/debug/build/cmd/chunkmerge/chunkmerge.js a.mcap b.mcap --job task --output merged.mcap --checkpoint-final --run-bytes 2097152 --fan-in 4
# 退出后使用相同输入顺序及预算，CJOB2 会自动选择贯通恢复路径：
node _build/js/debug/build/cmd/chunkmerge/chunkmerge.js a.mcap b.mcap --job task --output merged.mcap --resume --run-bytes 2097152 --fan-in 4
node _build/js/debug/build/cmd/mcap/mcap.js job-inspect task
node _build/js/debug/build/cmd/mcap/mcap.js job-clean task
python tools/chunk_pipeline_workflows.py
```

## 恢复边界

录入阶段沿用 [Chunk 保存点](chunk-checkpoint.md) 的条件：1–64 个输入，完整索引及非零有效摘要/Chunk CRC。每个输入完成后仍严格扫描原始输入、校验整体关系并保存附件/元数据，源文件不改动。恢复仍核对原始输入和已提交副本的 SHA-256，不承诺免除完整指纹读取。

完成录入后，每个原始输入的副本通过有界片段表组合为内部虚拟 MCAP，保留原始声明 ID 和物理消息次序。它不额外复制整个输入，不发布为用户文件；内部虚拟封装无摘要和数据 CRC，原始输入的校验结论独立保留在结果中。至多 32768 个副本/片段受 max_parts 限制。

最终排序使用已有 MJOB2 引擎，保存每个完成输入和每个完成归并组。CJOB2 同时封装 Chunk 状态与最终排序状态，带总 CRC，合计上限 8 MiB。中断后复用检查点引用的排序段、辅助记录和归并结果，重做未提交的一段/一组；最后写最终输出阶段中断，仍从已提交段重写整个最终输出，不续写半个 MCAP。

稳定顺序仍是 `(时间,原始输入顺序,原始物理顺序)`，保留重复消息，安全重映射跨输入的声明 ID，保留附件和元数据。无 --checkpoint-final 的旧任务继续使用 CJOB1；现有 CJOB1 可显式加该参数升级。CJOB2 恢复无需重复该开关，但全部预算须保持一致。

## 存储与检查

同一专用 MergeJob 目录隔离 Chunk 副本、非负最终排序段和辅助记录的编号区间。checkpoint_run_ids 同时返回所有已提交文件编号，恢复先清理未引用的自有文件，包括在保存点之后尚未删除的旧段。未知文件、任务锁、所有权标记和输出防覆盖继续按 MergeJob 规则处理。

job-inspect 显示 chunk_ingest / final_sort_merge、完成输入数和副本数，final_sort 还显示输入排序进度、已提交归并组和存活排序段。修改源、替换副本、损坏最终段或改变预算会拒绝恢复。CJOB2 是本项目格式，不是 MCAP 标准记录。

spool_bytes 共同限制保留副本和最终排序的存活临时空间，最终排序只获得扣除副本后的余额。副本没有压缩，原始源可明显更小；元信息、片段表、排序缓冲和解压预算分别受原有配置约束，不是总 RSS 限额。

库调用者须提供专用、持久且互斥的 SortStorage，并实现原子 save、持久化、恢复前孤立文件清理与最终关闭；不得在临时错误时删除已提交段。默认 sort_directory 的整目录临时清理语义不适合替代此持久存储。CLI 已实现这些生命周期步骤，成功后目录仍保留供查看，使用 job-clean 清理。

实际测试在最终归并组 checkpoint 原子提交后暂停并由操作系统终止子进程，恢复后逐条与独立 MCAP 读取器比较 320 条消息及附件/元数据；校验已提交副本的摘要和修改时间未改变，损坏最终排序段拒绝发布，孤立段被清理。核心测试另在 Wasm/JS 验证已提交文件不被重新创建。参考：[验证报告](../examples/validation/chunk-pipeline-0.15.23.json)。
