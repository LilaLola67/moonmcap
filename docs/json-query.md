# JSON 内容查询

0.15.4 支持 `message_encoding="json"` 的消息。其他编码跳过；MCAP 中的原始 payload 不改写。字段路径采用 [RFC 6901 JSON Pointer](https://www.rfc-editor.org/rfc/rfc6901)：空路径表示根，`/a~1b/~0x/0` 表示对象键 `a/b`、`~x` 下数组第 0 项。

```sh
moon build --target js
node _build/js/debug/build/cmd/mcap/mcap.js query input.mcap --where '{"all":[{"path":"/speed","op":">","value":80},{"path":"/status","op":"=","value":"ok"}]}' --select /speed --select /status --format ndjson
```

PowerShell 可将 JSON 表达式保存到变量后传递。`query`、`index-query`、`dataset-query` 都支持 `--where`、可重复的 `--select`、`--skip-invalid-json`。`stream-transform` 支持条件筛选，先筛选再采样、分页和时间对齐，保持入选消息原始负载。输出文件采用原子新建，已有路径拒绝覆盖。

条件为 true/false、`{"all":[条件...]}`、`{"any":[条件...]}`、`{"not":条件}` 或 `{"path":"/字段","op":"操作符","value":值}`。支持 `=`、`!=`、`>`、`>=`、`<`、`<=`、字符串 `contains`；`exists` 只含 path/op。数组和对象相等递归比较，数字按十进制原文比较，不转为浮点数比较。字符串排序按 MoonBit String 次序，大小比较不跨类型转换。缺失字段的所有叶条件为 false；存在的 null 可用 exists 或与 null 相等区分，not 可表达字段缺失。

返回记录保留时间、通道、来源编号和 data_hex，并增加 decoded：`data` 是完整解析值或以请求路径为键的投影对象，`missing_fields` 明列缺失路径。null 保留为 null，大整数及小数原文在 JSON 输出中保持精度。NDJSON 逐条输出；普通 JSON 数组受查询 limit 限制。

严格使用 [RFC 8259 JSON](https://www.rfc-editor.org/rfc/rfc8259)，并拒绝重复对象键。默认无效负载报错；显式 skip 策略跳过错误负载。默认单消息 1 MiB、嵌套 64 层，100000 个值节点；数字原文最多 256 字符、指数绝对值最多 1000000；条件最多 128 节点/16 层，路径最多 64 段，投影最多 64 项。超限按相同错误策略处理。过滤器不会执行脚本。

库入口 `json_filter`、`JsonFilter.matches/project`、`QuerySource.filter_json`、`json_query_record`；内存 QueryIndex、RangeReader、SidecarReader 均可转换为 QuerySource。跨文件应先给各提供方加过滤器，再打开 Dataset；全局分页发生在匹配消息归并后。取消或解析失败会停止底层消息流。

`python tools/json_workflows.py` 使用官方 Python MCAP writer 和 Decimal，独立验证三种压缩、两种查询路径、跨文件排序/分页、过滤后采样、精度、投影及无效 JSON 的原子输出行为。
