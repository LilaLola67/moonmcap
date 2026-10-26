# 聚合状态溢写与有界归并

```sh
moon build --target js
node _build/js/debug/build/cmd/spill/spill.js a.mcap b.mcap --output groups.ndjson --temp-dir temporary --value /temperature --group /device --window 1000000000 --max-groups 128 --run-bytes 2097152 --merge-bytes 16777216 --fan-in 8
python tools/aggregate_external_workflows.py
```

`aggregate_external(dataset.cursor(query), dataset.channels(), storage, output, aggregate~, external~, cancelled~)` 沿用精确十进制、字段缺失与 null 区分、主题分组及时间窗口语义。达到缓冲分组数量或状态字节预算时，将排序后的部分统计写入专用临时文件；多轮归并合并相同键，最后逐组输出 NDJSON，不收集全量结果数组。

最终次序与 aggregate_json 相同，按规范化分组键排序。sum/min/max 使用十进制字符串；平均值含精确 numerator/denominator 和 18 位、半数远离零舍入的小数。跨运行段累加仍使用 BigInt，不经过 Double。无数值组输出 null 统计。临时行含 CRC、类别标记和严格排序检查，损坏即失败。

`AggregateOptions.max_groups` 在这个入口限制每个缓冲段的分组数，最终分组数可以更多；`group_bytes` 与 ExternalOptions.run_bytes 取较小值作为缓冲预算。单个状态本身超限仍失败，不能靠溢写处理无限大的单一键。键/组编码受 Limits.max_string_bytes、行受 max_record_bytes 限制；数字指数和系数长度继续沿用原聚合限制。原 aggregate_json 仍是有总分组上限的内存接口，没有静默改为使用磁盘。

ExternalOptions 控制归并头部字节、fan_in（2–64）、累计创建段数、全部同时存活的临时磁盘字节和输入消息数量上限。归并按 fan_in+1 为各头部保守分配额度，较大的单组可能需要调高 merge_bytes。另有一个共享 64 KiB 写缓冲；负载计数不含全部运行时分配，不是 RSS 上限。输入游标自己的候选、缓存和声明预算另计。

命令支持多个输入及重复 --index，可混用 MIDX1/MIDX2，支持 --where、主题、时间、方向、offset/limit，以及原聚合字段/窗口选项。--record-limit 调整索引和聚合输入数量上限，--limit 仍控制查询选取的全局数量。输出固定为 NDJSON 聚合组，不接受 --select、--format 或 --full。没有索引的输入仍可能走既有有预算的回退，建议建立侧车。

文件命令使用新路径、临时文件及独占发布，成功后报告统计；任何失败清理本次排序段和暂存输出，原文件不变。直接库调用者必须丢弃失败输出，SortStorage 须专用于此任务。错误会取消输入游标；取消在循环与 I/O 边界检查。

独立参考回归使用 Python Decimal，三种压缩的 6,144 条消息生成 2,048 个组，缓冲仅允许 7 组，分别通过普通磁盘游标和分页侧车读取；核对计数、缺失、精确总和、极值、平均分数与舍入结果。另检查临时空间、段数、归并内存和单组预算失败时没有输出。核心测试覆盖多轮两路归并、不同精度抵消、损坏段、取消和清理。既有内存聚合同时补计最小/最大值字符串的状态字节。
