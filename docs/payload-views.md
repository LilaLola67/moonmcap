# 按需解码与大数组

`CdrDecoder.view(payload, selection=decode_selection())` 返回 `PayloadView`：保留原始载荷字节，输出所需字段，超过 `inline_array_items` 的数组使用带路径、元素数、字节长度、元素类型、步长及大小端的 `$binary` 描述符。默认内联至多 1024 项、遍历至多 16 Mi 项，源载荷仍受 CDR message_bytes 限制（默认 16 MiB）。传统 `decode` 的行为与限制不变。

CDR 固定宽度数组可跳过 JSON 构造并直接检查跨度；布尔数组仍验证每个值。变长元素数组必须遍历来确认边界，视图的 stride 为 0。字符串仍验证 UTF-8、NUL、声明长度与预算。未选字段仍检查线格式和尾部，不把未完成遍历视作成功。

- `view.read_binary(path, offset, length, max_bytes=65536)` 有界读取字段原始字节，偏移不包含序列长度前缀。
- `view.read_elements(path, start, count, max_elements=1024)` 读取固定宽度原始类型数组窗口，保留整数精度。变长元素需使用原始二进制接口。
- `view.write_binary(path, sink, block_bytes=65536, cancelled=...)` 分块导出，不先生成整字段十六进制或 JSON。

```powershell
moon build --target js
node _build/js/debug/build/cmd/view/view.js schema.msg image.bin sensor_msgs/msg/Image --inline 32 --window /data 0 16 --export /data image.raw
node _build/js/debug/build/cmd/query/query.js recording.mcap --project-decode --select /header/stamp --where true --workers 2
```

`--project-decode` 将输出选择与过滤依赖合并，先解码所需字段再精确过滤、分页。只要条件涉及尚未展开的大数组就明确失败，不能用描述符替代原数组计算 contains/比较；可以提高内联限制，或通过视图窗口处理。选择超出内联上限数组的单个元素也明确失败，改用 `read_elements`。默认不启用，确保既有 JSON 输出兼容。

`ProtobufDecoder.decode_selected(name, payload, selection)` 可跳过未请求的顶层字段，避免大 bytes 的 base64 展开和 packed repeated 的逐项构造；选中的嵌套消息沿用完整合并/map/oneof 语义再投影。必填字段缺失仍失败；未选字段执行 wire 级跳过，不承诺完整值语义验证。ProtoJSON 模式为保留 WKT 语义，使用完整解码后投影。Protobuf 的长度字段仍复制有界原始跨度，此接口并非零复制；二进制窗口支持 CDR 和 ROS1。`cmd/view ... --protobuf --field /stamp` 可复现 Protobuf 投影。

JSON 输入完整解析后投影。按需载荷解码当前在主线程执行；工作线程仍可并行解压 Chunk，explain 中 `projected_decode/payload_workers` 区分两者。查询仍需读取包含载荷的 Chunk，减少的是载荷展开成本，不能宣称按字段读取压缩文件。

复现 `python tools/payload_view_workflows.py`：独立 rosbags 序列化 1 MiB 字节数组与 32768 个 float32，验证两种大小端窗口、SHA256 一致的二进制导出、三种压缩的过滤分页和原子失败；独立 Protobuf 消息验证跳过 512 KiB bytes 与 200000 个 packed 元素。数据均为合成数据。
