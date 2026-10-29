# 容差实验回归与证据片段

`tools/regression.py` 比较 MoonBit 归一化后的 NDJSON 视图，作为可选标准库 Python
适配器；既有严格日志 diff 保持原有语义。两个实验可分别使用各自的 schema/单位
归一化规则，然后按明确的时间偏移和字段容差比较。

```sh
python tools/regression.py baseline.ndjson candidate.ndjson examples/regression/rules.json new-report-directory
```

输入行使用 normalize 的 topic、log_time、data。时间、time_tolerance 为 UInt64
十进制字符串；baseline_offset/candidate_offset 为有符号 Int64 纳秒字符串，应用后
仍须在 UInt64 范围内。不会从数值曲线自动推测时钟。baseline 必须按对齐时间非递减。
candidate 可乱序，先构造按话题/时间/序号索引的临时 SQLite 数据库。

每条 baseline 按同话题最近时间贪心配对，等距选较早时间，相同时间选较早输入序号。
consume 默认 true，候选只用一次；false 允许复用，但 extra 仍只统计从未使用的候选。
这是明确的顺序配对，不是全局最优匹配；丢失的早期消息可能影响之后配对。

fields 最多 128 个，path 是 data 内的 JSON Pointer。kind:number 使用精确有理数判断：
`abs(a-b) <= max(abs_tolerance, rel_tolerance * max(abs(a),abs(b)))`。
abs/rel 是非负十进制字符串，数值也可为十进制字符串、JSON 整数或小数；不经过
Double，不损失超过 2⁵³ 的计数器。最多 128 位有效数字及正负 128 的十进制指数。
kind:exact 递归比较 JSON，数值作精确数值比较，布尔值与数字区分，不把字符串转数字。
missing 为 fail（默认）、ignore（任一侧缺失则忽略）或 equal（两侧都缺失才通过）。
存在的 null 与缺失区分；number 模式下 null 为类型不匹配。

thresholds 指定可接受的 field_failures、missing、extra、event_failures，默认均为 0；
minimum_pairs 默认 1，避免空输入被当成成功。退出码 0 为通过，1 为确实发生回归，
2 为非法输入或执行失败。details 默认最多 128 条，可配置至 10000 条，计数仍完整。

events 可指定 baseline/candidate 两份事件 JSON 文件和 tolerance。输入可以为数组
或 `{"events":[...]}`，事件包含 start/end 和可选 label；同一 label 按 start/end
排序后逐项比较边界，数量差或边界超容差计为事件失败。可直接接收单规则事件检测
报告（未提供 label 时为空标签）；不会自动推断事件身份。每份最多 4 MiB/10000 事件。

可选 evidence 配置 baseline_mcap/candidate_mcap、对应 baseline_sha256/
candidate_sha256、pre/post、max_cases（默认 8，最大 32）和 context 规则。
工具调用 MoonBit 流式片段导出，分别保留失败样本原始日志时间附近的消息及声明的
上下文。源 MCAP 散列先后复核，报告保存片段散列；归一化视图与源 MCAP 的对应关系
由调用方声明，不把任意同时间数据宣称为已经证明的同源观测。

输出目录必须不存在，report.json 原子发布，作为整项比较完成的标记；报告前已生成
的证据文件可能在执行失败时留在该新目录供检查，此时没有成功报告。不会自动复用
不完整目录。SQLite 默认数据库预算 1 GiB（disk_bytes 可配），缓存约 4 MiB；输入
行最多 1 MiB。数据库预算不包括文件系统开销、SQLite 临时排序或证据片段，不是 RSS
或整个作业磁盘用量上限。输入与规则散列记录在报告中，输入变化拒绝完成。

`python tools/regression_workflows.py` 检查精确容差边界、时间偏移、缺失/多余消息、
事件边界、保留用户字段，并用独立暴力匹配器核对随机小规模配对。另用独立 MCAP
reader 检查带上下文的失败片段。测试数据为合成实验。
