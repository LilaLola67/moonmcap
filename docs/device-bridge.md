# 设备输入与当前 Foxglove SDK

`cmd/device` 接收逐行 JSON 信封，用 MoonBit RecordingSession 持久化录制。
`tools/foxglove_bridge.py` 通过当前官方 Python SDK 将同一载荷广播给 Foxglove。
适配器固定验证版本 foxglove-sdk 0.30.3，使用当前 SDK 的连接方式，不依赖旧的
foxglove-websocket 服务端库。Python 是可选连接适配器，容器写入和分段恢复由 MoonBit 实现。

```sh
python -m pip install foxglove-sdk==0.30.3
moon build --target js
python tools/foxglove_bridge.py examples/device/channels.json recording < examples/device/messages.ndjson
```

在 Foxglove 中打开 `ws://127.0.0.1:8765` 连接。示例输入结束后录制完成并关闭服务；
实际设备程序持续向 stdin 输出相同信封即可持续录制与广播。设备串口、网络协议或
厂商驱动由生产端转换，不宣称通用硬件驱动。也可直接运行 MoonBit 录制入口：

```sh
node _build/js/debug/build/cmd/device/device.js examples/device/channels.json recording < examples/device/messages.ndjson
```

每条信封包含 topic、log_time、publish_time、sequence，以及 value 或 data_hex 二选一。
时间和序号为十进制字符串，检查 UInt64/UInt32 范围。value 只接受 JSON 通道；
data_hex 是已经编码的原始字节。适配器将同一编码字节送到录制器和 SDK，不做
JSON 往返改写。SDK 广播保留 log_time；publish_time 和 sequence 保留在 MCAP 中，
不声称 SDK 消息帧携带它们。

通道配置包含 topic、message_encoding，可选 metadata 字符串映射和 schema：
`{"name":"demo.Value","encoding":"protobuf","data_hex":"..."}`。
SDK 对非 JSON 通道要求显式 schema；Protobuf 填 descriptor set，ROS2 可填 ros2msg。
设备重连恢复须保持相同配置及 identity，并传 `--resume`。未完成活动段按既有录制
恢复策略隔离；已经提交的完整段保留。

MoonBit 每接受一条消息返回 accepted，适配器收到后才广播下一条，防止生产端
无界排队。accepted 仅表示进入录制会话，committed_messages 单独报告已持久化
提交的消息数；退出成功后活动段完成。SDK 每连接积压条数设为 32，但不保证慢客户端
逐条收到消息，也不将记录与显示宣称为跨系统事务。无网络消费者时 SDK 仍可录制。

限制为每条载荷 1 MiB、输入行约 2 MiB、最多 128 通道；segment_messages 为 1–50000。
stdout 给出 ready 和最终报告。非法输入中止活动段，既有提交段仍可恢复。
默认仅绑定本机，可用 --host/--port 显式修改。该工具提供数据输出，没有设备控制接口。

`python tools/device_workflows.py` 建立真实 SDK WebSocket 订阅，以独立解码核对
JSON/Protobuf 广播帧、精确 UInt64 时间，并用 Python MCAP CRC reader 核对分段录制。
另检查非法信封和恢复。测试使用本机合成设备输入；尚未连接实体设备，也未操作
Foxglove 桌面可视化界面。

API 依据 [官方 SDK 文档](https://docs.foxglove.dev/docs/sdk) 与
[Python 接口](https://sdk-docs.foxglove.dev/python/)。SDK 为独立 MIT 许可依赖，未复制其实现。
