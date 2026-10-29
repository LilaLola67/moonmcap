# 消息定义升级与统一查询视图

不同录制版本可能把 `speed_mm_s` 改名为 `speed`，或者在二进制消息末尾增加字段。`compare_payload_schemas` 分别报告旧 reader 读取新数据、新 reader 读取旧数据、旧 JSON 字段查询作用于新数据的兼容性。统一查询视图则由显式规则完成别名、投影、默认值、类型检查和单位换算，不从字段名猜测单位或业务含义。

```sh
moon run cmd/payload --target js -- compare protobuf old-descriptor.bin demo.Value new-descriptor.bin demo.Value
moon run cmd/payload --target js -- compare cdr old.msg demo/Value new.msg demo/Value
moon run cmd/payload --target js -- normalize input.mcap rules.json normalized.ndjson
```

compare 只输出报告，兼容性为 false 时仍正常退出，非法定义或预算错误才失败。FORMAT 接受 protobuf、cdr、ros1。输出中的布尔值是基于已支持解码模型的保守结构判断，不是完整 protoc、DDS/ROS 类型协商或应用安全证明。

## 比较范围

- Protobuf 按 field number 比较，区分增加/删除字段、required、普通字段类型、repeated、oneof、显式存在性、map-entry、proto2/proto3、枚举数字与名称。可选新字段可以被旧 reader 跳过；新增 required 字段会破坏新 reader 读取旧数据的能力。JSON 名称变化单独标记查询不兼容。类型改变采取保守拒绝，即使两个类型碰巧使用相同 wire type，也不会宣布其数值含义兼容。
- ROS1 和 plain XCDR1 按字段顺序、原始类型和数组布局比较，新增字段也视为结构不兼容。只改字段名不改变可解码布局，但会破坏旧字段路径。放宽/收紧字符串或序列边界会影响不同方向的可接受输入范围。ROS1 别名先按已有解析器规范化。
- 最多遍历 4096 对引用类型，递归深度 64；共享引用类型对只报告一次。默认保留最多 256 条问题，截断列表仍计算完整兼容性和问题总数。descriptor 显式默认值、自定义 options、保留字段的业务用途和单位语义不作推断；不支持的格式明确报错。

## 显式归一化

```json
{
  "topics": ["/motion"],
  "fields": [
    {"source":"/speed_mm_s","target":"/motion/speed_m_s","conversion":"scale","scale":0.001},
    {"source":"/counter","target":"/counter","conversion":"unsigned","bits":64},
    {"source":"/frame","target":"/frame_id","conversion":"text","default":"base_link"},
    {"source":"/temperature","target":"/temperature","required":false}
  ],
  "where":{"path":"/motion/speed_m_s","op":">=","value":2},
  "limit":3
}
```

只输出声明的目标字段。source 使用 JSON Pointer，可读取对象或数组；target 构造嵌套对象，不自动创建数组。缺失字段默认报错；配置 `default` 则用该值，配置 `required:false` 且没有 default 则省略。存在的 null 不等同于缺失。目标重复或互为祖先会报错，最多 128 个字段。

conversion 接受 keep、text、boolean、signed、unsigned、scale。text/boolean 严格检查类型，不把字符串 `"false"` 当成布尔值。整数转换可接收精确十进制字符串或整数字面量，bits 只能是 8/16/32/64，检查范围并保留精确十进制表示。scale 使用 `输入×scale+offset`，只接受有限 JSON 数字，拒绝已知会在转成 Double 时丢掉十进制有效信息的输入，例如精确的 `9007199254740993`。缩放结果是显式 IEEE Double 运算，不能当作精确十进制金融计算，也不自动缩放整数时间戳。NaN、Infinity、溢出和不合适的类型均报错。

NDJSON 行包含原消息 topic、序号、两种时间，以及 data、defaulted、missing，便于区分真实观测与填充值。where 在归一化之后执行，limit 在过滤之后执行；默认上限 100000 行。start/end 为包含两端的 UInt64 纳秒字符串。输出须为新文件，错误时不发布；输入完整容器先执行 CRC 校验。时间范围查询沿用 RangeReader 的有界无索引回退限制。

库中可复用已编译规则，避免逐消息解析：

```moonbit
let fields = [{ ..@mcap.normalized_field("/speed_mm_s", "/speed_m_s"), conversion: @mcap.ScaleValue(0.001, 0.0) }]
let normalizer = @mcap.payload_normalizer(fields)
let result = normalizer.normalize(document) // value / defaulted / missing
let decoder = @mcap.normalizing_decoder(@mcap.standard_payload_decoder(), fields)
let view = source.decode_payload(decoder)
// 再组合筛选、聚合、时间关联或 Dataset 全局分页。
```

每个输入版本可使用各自的规则构造相同输出结构，然后合并查询。该视图不修改原始文件或二进制 schema；实际原格式字段回写使用[载荷修改接口](payload-rewrite.md)。

## 可复现验证

`python tools/schema_evolution_workflows.py` 使用 Google Protobuf 检查新增 required 字段的实际双向初始化结果，区分可选字段扩展、wire 兼容改名和保守拒绝的类型变化；另覆盖 ROS 顺序和边界。独立 JSON/CDR writer 在三种压缩输入上构造同一实验数据，验证两个版本经不同单位/名称规则得到相同结果，保持超过 2⁵³ 的 UInt64，并核对 defaulted/missing 和先过滤再限量。错误规则、必需值缺失、隐式精度损失均不能留下最终输出。测试数据均为合成数据。

Protobuf 兼容性依据及仍需人工判断的限制见[官方更新消息类型说明](https://protobuf.dev/programming-guides/proto3/#updating)。
