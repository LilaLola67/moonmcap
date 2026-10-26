# ROS 2 ros2msg / XCDR1 解码

`cdr_decoder(schema.name, schema.data)` 解析 MCAP 内 ros2msg 消息定义和其 `MSG:` 依赖，解码带 4 字节 RTPS encapsulation 的普通 XCDR1 大端/小端 payload。实现使用 MoonBit，可在 Wasm/JS 使用，不需要 ROS 或 Python 运行时。`standard_payload_decoder()` 自动识别 `cdr` 通道配合 `ros2msg` Schema，同步/异步查询均可接入已有解码适配器。

```moonbit
let decoder = @mcap.cdr_decoder("demo/msg/Counter", b"uint64 n")
let json = decoder.decode(b"\x00\x01\x00\x00\x01\x00\x00\x00\x00\x00\x20\x00")
// 精确输出 {"n":9007199254740993}。
```

```sh
moon build --target js
node _build/js/debug/build/cmd/decode/decode.js robot.mcap --where '{"path":"/header/frame_id","op":"=","value":"base_link"}' --select /position
node _build/js/debug/build/cmd/decode/decode.js raw-cdr schema.msg message.bin demo/msg/Counter
python tools/cdr_workflows.py
```

## 支持范围

- bool，int8/16/32/64，uint8/16/32/64，float32/64，byte/octet、char，UTF-8 string；字节对齐相对于 encapsulation 后的数据起点计算。
- 固定数组、无界/有界序列、string 的声明字节上界、嵌套消息和包含依赖的 ros2msg 格式；资源名称接受 `pkg/Type` 和 `pkg/msg/Type`。time/duration 兼容名映射到 builtin_interfaces，仍须给出依赖定义。
- 常量和字段默认值不占 wire 字节；解码读取实际序列化值，不根据默认值补齐截断字段。字段名不做 camelCase 转换。
- 空 ROS 2 消息检查生成器使用的单字节零 sentinel，输出空对象；允许最多三个尾部零填充字节。内部对齐填充值不要求为零。
- 64 位整数默认输出精确 JSON 数字，现有数值条件和精确聚合可使用。可通过 CdrOptions.int64_as_string / CLI --int64-strings 输出字符串；JS 消费者解析数字时须考虑精度。非有限浮点输出字符串。

不支持 XCDR2、parameter-list/mutable 表示、非零 encapsulation options、ros2idl 定义、wstring、联合体、递归消息定义或完整 DDS 类型演化。遇到这些情况明确失败，不能据此宣称支持所有 ROS 2/CDR 文件。容器解析本身仍可保留这些原始 payload。

## 预算与缓存

默认定义 4 MiB、wire 消息 16 MiB、字符串 1 MiB、item 100000、声明字段/类型 10000、嵌套深度 64。数组和序列在创建结果前检查声明上界及剩余 item；嵌套字段和空结构也计数。缺失依赖、循环、重复字段、截断、非法 bool、NUL/UTF-8 错误或多余非填充字节均报错。

标准解码器的 Protobuf/CDR 共用一个类型 LRU 和总预算，以名称、Schema.encoding、定义字节辨认，避免跨文件局部 ID 碰撞。预算按定义字节和字段估算，非 RSS 限额。适配器另有输出 JSON 默认 1 MiB 限制，大数组可显式调整 `decode_payload(max_bytes=...)`；源文件不被修改。

## 独立验证

rosbags 0.11.0 作为开发参考序列化器，对照大小端、全部支持的标量、数组、嵌套 Header/Pose、空消息和三种压缩输入。ROS 2 rosidl 明确将 byte 映射为无符号 octet；参考 rosbags 的 byte 别名存在有符号差异，因此对照用其共同范围，另以规范边界 255 做核心断言。

`python tools/cdr_workflows.py --public` 使用已固定 SHA-256 的 SO-101 原生 MCAP，逐条与独立解码器比较 5 个主题的全部 4719 条消息，并检查序号和时间。运行前需通过 `python tools/corpus_workflows.py --public` 下载原始数据，原始数据不随库分发。[验证报告](../examples/validation/cdr-0.15.20.json)。

规范依据：[MCAP registry](https://mcap.dev/spec/registry)、[ROS 2 rosidl 类型映射](https://github.com/ros2/rosidl/blob/rolling/rosidl_adapter/rosidl_adapter/msg/__init__.py)。实现原创；未移植参考工具源码。
