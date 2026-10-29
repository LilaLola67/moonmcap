# 结构化载荷回写

`CdrDecoder.encode`、`Ros1Decoder.encode`、`ProtobufDecoder.encode` 把普通 Json 对象按消息定义写回原始二进制格式。`standard_payload_rewriter()` 提供 JSON、Protobuf、ROS1、ROS2 ros2msg/plain XCDR1 的字段修改，`stream_rewrite_payload` 将修改应用到完整 MCAP，保持通道编码、声明、序号、log_time、publish_time 和原始物理消息顺序。

## 字段规则与文件命令

```json
{
  "topics": ["/joint_states"],
  "edits": [
    {"path": "/header/frame_id", "value": "base_link"},
    {"path": "/header/stamp/sec", "value": 12},
    {"path": "/header/stamp/nanosec", "value": 500000000}
  ],
  "copy_auxiliary": true
}
```

```sh
moon run cmd/payload --target js -- rewrite input.mcap rules.json new-output.mcap
moon run cmd/payload --target js -- encode cdr schema.msg demo/Value value.json new-payload.bin
moon run cmd/payload --target js -- raw-rewrite cdr schema.msg demo/Value payload.bin rules.json new-payload.bin
```

可直接运行的合成示例见 [examples/payload-rewrite](../examples/payload-rewrite/README.md)。FORMAT 接受 `protobuf`、`cdr`、`ros1`；CDR encode 可附 `--big-endian`，回写现有 CDR 时自动保持原来的大小端。Protobuf schema 是 FileDescriptorSet，ROS schema 是带依赖定义的 msg 文本。

规则使用 JSON Pointer。`value` 可设为 null；`remove:true` 是删除，两者不能同时出现。允许替换已有值或添加最后一级对象成员；父容器必须存在。数组索引从零开始，删除后后续规则按新数组解析，禁止 `01` 等非规范索引。最多 128 条规则，禁止重复或互为祖先的路径；修改返回新对象，不改变调用者传入的 Json。空规则会校验原始载荷并原样返回其字节。

文件命令默认处理所有话题；指定 `topics` 时只修改匹配话题，其他消息保持原字节。不存在的话题会报错。默认复制全部附件和元数据，可设 `copy_auxiliary:false` 全部排除；修改载荷字段不会清理其他记录里的同名内容。输出重建 Chunk、索引和 CRC；未知容器扩展记录不复制，不保留原压缩字节和索引布局。写入新文件，拒绝覆盖；即使在文件末尾才发现 CRC 错误，也不会发布目标文件。

## 编码范围与保留语义

- ROS1 使用无对齐的小端编码，byte/int8、char/uint8 别名和 Header/time/duration 与既有解码器一致。ROS2 使用从四字节封装头之后计算的 plain XCDR1 对齐，支持大小端、嵌套消息、定长/有界/无界数组、字符串边界与空消息哨兵。沿用解码器的类型范围，不新增 XCDR2、IDL 或 wstring 支持。
- 反向编码严格检查未知成员、必需字段、整数范围、数组长度和字符串字节边界。整数接受整数字面量或精确十进制字符串，不隐式截断小数或指数记法。UInt64/Int64 全范围保持精度；调用者若事先把大整数转换成已经舍入的 Double，编码器无法恢复原值。浮点支持数值及 `NaN`、`Infinity`、`-Infinity`，有限数转 float32 溢出会报错。
- Protobuf 使用普通消息对象表示，字段名为 descriptor 的 json_name，支持既有标量、嵌套、map、repeated、oneof 与 required 检查。新编码的 repeated 标量使用合法的 unpacked 形式，不保证复现原来的字节或字段次序。普通对象下的特殊类型按其原始消息字段处理；反向 ProtoJSON 特殊映射目前明确拒绝。扩展字段、Editions 和已声明 GROUP 仍不支持。
- `ProtobufDecoder.rewrite` 按 wire 切片保留未修改字段、未知字段和未知 group；修改嵌套单值消息时合并其有效重复出现，保留嵌套未知字段。设置一个 oneof 成员会去掉其他成员；同一组同时设置两个成员会报错。可以删除可选字段；required 字段不能删除。嵌套路径只支持单值消息；repeated、map 或整个子消息需要整体替换。整体替换子消息会替换其中的未知内容，完整 `encode` 也不携带原始未知字段。修改器不能把“保留未知内容”同时解释为“彻底删除未知内容中的敏感字段”。

核心写入 sink，失败可能留下无效前缀；文件命令管理独占暂存和最终发布。默认 Protobuf 单消息/JSON 输入预算为 1 MiB，ROS1/CDR 为 16 MiB，schema 为 4 MiB，缓存为 64 个 schema / 8 MiB 逻辑状态；缓存按 schema 编码、类型名和内容共同辨认，不把局部 schema ID 当作全局身份。单消息深度、元素数量、Chunk、摘要和输入记录都有独立预算；文件命令 `input_records` 默认 30000000。上述预算不是进程 RSS 保证。

## 验证与依据

`python tools/payload_rewrite_workflows.py` 对照 Google Protobuf 和 rosbags 的独立反序列化结果，覆盖全部普通 Protobuf 标量、精确整数、嵌套/map/repeated/oneof、未知字段与 group、ROS1 紧凑布局、XCDR1 大小端和空消息。四种消息格式分别组合三种 MCAP 压缩输入，独立 MCAP reader 开启 CRC 后核对载荷、消息信封、声明和附件；另外验证错误字段、已有输出和末尾 CRC 错误均不发布新文件。使用的是独立运行时构造的合成数据。

编码依据：[Protobuf wire format](https://protobuf.dev/programming-guides/encoding/)、[ROS1 serialization](https://github.com/ros/roscpp_core/blob/noetic-devel/roscpp_serialization/include/ros/serialization.h)、[ROS2 Fast RTPS 消息生成模板](https://github.com/ros2/rosidl_typesupport_fastrtps/blob/rolling/rosidl_typesupport_fastrtps_cpp/resource/msg__type_support.cpp.em)。这是独立实现，未复制其源码。
