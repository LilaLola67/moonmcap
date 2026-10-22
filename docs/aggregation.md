# 流式聚合分析（0.15.7）

`aggregate_json` 消费 DatasetCursor，逐条解析 JSON；不保存历史消息。可组合主题/时间筛选、跨文件分页、侧车索引以及已有 JSON 条件。原始 MCAP 不修改。

```sh
moon build --target js
node _build/js/debug/build/cmd/mcap/mcap.js aggregate a.mcap b.mcap --value /temperature --group /device --window 1000000000 --where '{"path":"/valid","op":"=","value":true}'
```

默认按主题分组，`--no-topic-group` 合并主题；`--group` 可重复（最多 16 个 JSON Pointer）。窗口为 `[origin+k*window,origin+(k+1)*window)`，时间小于 origin 跳过，window=0 不分窗。分组对象忽略键顺序、数字按精确数值归一化；缺失分组字段与 null 区分。输出按规范化分组键排序，不依赖输入/哈希表次序。

每组返回 count、numeric_count、missing_count、non_numeric_count、sum、min、max、avg。count 包括数值缺失或非数字的合法 JSON；数值统计只接纳选定字段的 JSON number。sum/min/max 用十进制字符串避免消费端丢精度；无数值时为 null。avg 包含精确 numerator/denominator，并给出保留 18 位小数、半数远离零舍入的 decimal 字符串；统计不使用 Double 累加。

资源边界：默认最多 1024 组、8 MiB 编码状态预算（不是总进程 RSS）；字段和值沿用 1 MiB JSON、64 层限制，数字指数绝对值最多 1024，累计系数最多 4096 字符。`--max-groups`、`--group-bytes` 可调整分组预算。无效 JSON 默认报错，显式 `--skip-invalid-json` 跳过；非 JSON 通道跳过。预算或读取失败会取消游标；CLI 只在全部聚合成功后输出或原子新建结果文件。

`python tools/aggregate_workflows.py` 对三种压缩的多文件/侧车输入以 Python Decimal 验证总和、极值、精确平均分数，以及分组/内存预算失败时不产生输出。核心测试覆盖大整数抵消、正指数、缺失与 null、规范化对象分组和窗口。
