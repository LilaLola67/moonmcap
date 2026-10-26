# 消息解码与查询组合

`standard_payload_decoder()` 支持 JSON 和 MCAP 注册规范中的 Protobuf：Schema.encoding 为 `protobuf`，data 是包含依赖的 `FileDescriptorSet`，Schema.name 是完整消息类型名称。核心解码器使用 MoonBit 实现，在 Wasm 和 JS 上可用，无 Google Protobuf 运行时依赖。Python protobuf 6.33.0 仅用于开发对照测试。

```moonbit
let decoder = @mcap.standard_payload_decoder()
let provider = reader.query_source().decode_payload(decoder)
let filter = @mcap.json_filter("{\"path\":\"/speed\",\"op\":\">\",\"value\":30}")
let dataset = @mcap.open_dataset([provider.filter_json(filter)])
// dataset.cursor(...) 做全局分页；aggregate_json / aggregate_external 可读取解码字段。
```

`PayloadDecoder` 是可插拔回调，接收原始 Schema、Channel、payload，返回 Json。同步和 asyncio 的 QuerySource 都提供 `decode_payload`。适配器只更改返回消息的数据与查询声明，不修改输入文件；下游声明为 JSON，保留通道 ID、topic、元信息和时间。筛选应在 decode_payload 后、open_dataset 前组合，以保证全局 offset/limit 作用于命中消息。解码错误会停止对应底层消息流。

## Protobuf 覆盖

- proto2/proto3 标量（GROUP 除外）、有符号/无符号及 ZigZag、fixed、浮点、UTF-8 字符串、Base64 bytes、枚举。
- 嵌套消息、packed/unpacked repeated、map、oneof；重复标量取最后值，重复单值消息按 wire 合并，未知字段及未知 group 跳过。
- proto2 required 字段检查；未出现的普通字段省略，不展开 descriptor 中的显式默认值。map 缺失 key/value 按类型补默认值。
- 默认将 64 位整数写成精确十进制 JSON 数字，筛选和聚合使用原始数字表示，避免 Double 舍入。JS 消费者直接 JSON.parse 可能损失精度，可选 `int64_as_string: true` / CLI `--int64-strings`。字符串模式下这些字段不参与数值聚合。
- 非有限浮点输出 `NaN`、`Infinity`、`-Infinity` 字符串；有限浮点遵循其 IEEE 精度。

这是通用 wire 消息结构解码，不承诺完整 ProtoJSON：Timestamp、Duration、Any 等 well-known types 按普通消息字段展开，不做 RFC3339、Any 自动拆包等特殊映射；不解码扩展字段，不支持 Editions 和已声明的 GROUP 字段。描述符解析检查结构、重复定义和引用，但不是 protoc 的完整 schema 语义校验器。CDR、ROS1 等编码仍需要自行提供插件，默认解码器明确报不支持。

## 预算与缓存

默认 descriptor 4 MiB、单条 wire/输出 JSON 各 1 MiB、嵌套深度 64、wire item 100000、descriptor 字段及枚举值 10000。解析 descriptor 和消息分别使用 item 预算，packed 数组元素也计数。适配器同时检查 provider 的消息预算；自定义插件自身的内部内存由调用者负责。

默认类型缓存最多 64 个 schema、逻辑预算 8 MiB；按 descriptor bytes 与类型名称共同辨认，避免不同文件的局部 schema ID 冲突。缓存计费为 descriptor 字节及字段估算，属于保留状态上限，不是进程 RSS 保证。不能容纳的单个 schema 报错，LRU 淘汰不影响后续重新解析。

## 可运行命令

```sh
moon build --target js
node _build/js/debug/build/cmd/decode/decode.js recording.mcap --select /speed --limit 20
node _build/js/debug/build/cmd/decode/decode.js recording.mcap --where '{"path":"/speed","op":">","value":30}' --output decoded.ndjson
node _build/js/debug/build/cmd/decode/decode.js raw descriptor.bin payload.bin package.Message --int64-strings
python tools/protobuf_workflows.py
```

CLI 支持多个文件、MIDX1/MIDX2、字段条件、投影、排序分页，输出 NDJSON（含 decoded.data）。`--output` 仅创建新文件，失败不发布部分结果；stdout 是逐条输出，错误前输出应丢弃。官方对照包含所有支持的标量、重复消息合并、oneof、未知字段、三个压缩方式、64 位精确筛选及畸形 wire 的失败路径。

参考：[MCAP 编码注册](https://mcap.dev/spec/registry)、[Protobuf wire format](https://protobuf.dev/programming-guides/encoding/)。实现原创，未移植 Google Protobuf 源码。
