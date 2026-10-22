# 日志内容差异比较（0.15.8）

```sh
moon build --target js
node _build/js/debug/build/cmd/mcap/mcap.js diff before.mcap after.mcap --json-payloads --max-details 64
```

`diff_sources(left, right, options~)` 先严格校验两份源文件，再比较消息、schema/channel、附件、元数据及 profile。忽略压缩模式、分块、索引布局、Header.library 和声明 ID 数值；schema 引用按内容解析，channel/metadata 键值顺序规范化。未知私有记录和扩展字段不在语义模型中，报告不代表逐字节相同，也不是密码学签名。

消息按精确 log_time 分桶，在每桶比较包含语义通道、sequence、publish_time 和 payload 的多重集合。同一时间物理顺序变化不构成差异，重复消息数量仍保留。内容变化表示为一条 left-only 和一条 right-only；不猜测两条消息是否来自同一业务事件，不自动容忍时间漂移。默认比较原始 payload；显式 json_payloads/--json-payloads 对 json 通道规范化对象键和精确十进制数字，数组次序保留，非法/重复键 JSON 报错。

返回 equal、matching_messages、left_only_messages、right_only_messages、auxiliary_changes，以及有界 details 和 omitted_details。详细条目包含计数与最多 256 字符的 signature_preview；preview_truncated 标识省略，预览不用于相等判断。默认最多 64 条/256 KiB 详情；可用 --max-details、--detail-bytes 调整。退出码：0 相等、1 有差异、2 参数/I/O/校验失败。

时间桶、辅助签名各默认 8 MiB 编码状态预算（每侧计账），可用 --bucket-bytes/--aux-bytes 调整。消息游标另有同量候选预算；不是总 RSS 限制。同时间海量消息或大型附件不能在预算内表示时明确失败，不静默漏比。索引输入按需分页；无索引回退受 --max-file-bytes 限制。对比不覆盖原文件，结果文件独占新建。

`python tools/diff_workflows.py` 使用官方 writer 产生三种压缩、不同分块/元数据键次序的文件，覆盖 JSON 可选规范化、消息改动/缺失、附件/元数据变化、详情省略及损坏源拒绝。核心测试验证 ID 重映射及重复消息计数。
