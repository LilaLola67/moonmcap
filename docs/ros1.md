# ROS 1 载荷

`ros1_decoder(type_name, ros1msg_bytes)` 支持 MCAP 的 `ros1msg` schema / `ros1` message encoding，标准载荷解码器、同步/异步查询、工作线程和解码后聚合均已接入。支持基本整数与浮点、bool、字符串、固定/变长数组、嵌套 MSG 定义、常量跳过、Header、time 和 duration。

ROS 1 使用小端、无对齐、无封装头的线格式；字符串长度不含 NUL，空字符串合法。byte 是有符号 int8，char 是无符号 uint8。time 输出 `{sec, nanosec}`，sec 为 uint32；duration 两项为 int32，纳秒绝对值须小于十亿。空消息不占字节，固定零长度数组合法。读取后必须恰好到载荷末尾。

```powershell
moon build --target js
node _build/js/debug/build/cmd/decode/decode.js raw-ros1 schema.msg payload.bin package/Type
node _build/js/debug/build/cmd/view/view.js schema.msg payload.bin package/Type --ros1 --inline 32 --window /data 0 16 --export /data image.raw
node _build/js/debug/build/cmd/query/query.js driving.mcap --project-decode --where true --select /header/stamp
```

`Ros1Decoder.view` 提供与 CDR 相同的有界大数组接口；其 BinaryField.aligned=false，元素窗口不插入 CDR padding。默认完整解码仍受 100000 项和 JSON 输出限制，大图像/点云应使用视图或按需查询。未提供的普通依赖定义、递归类型、ROS 2 的 bounded sequence/string 语法以及非法尾部明确失败；Header 的隐式标准定义可补齐。这里只处理消息序列化，不实现 ROS Master、TCPROS 或 bag 容器读取。

`python tools/ros1_workflows.py` 使用独立 rosbags 0.11.0 serializer，覆盖嵌套结构、别名、字符串、时间、无对齐数组、二进制窗口及三种压缩查询；完整 UInt32 time 秒数另有手工规范向量，避免参考库的 signed int32 限制掩盖边界。

`python tools/corpus_workflows.py --public` 准备固定指纹的公开录制后，`python tools/ros1_workflows.py --public` 对 Foxglove demo.bag 转换得到的 MCAP 全部 **1606 条、七个话题**逐条比较标量/内联数组和大数组描述符的元素数、字节数，验证时间和 sequence。真实文件源与转换方式详见 [公开语料](corpus-validation.md)。大数组数据内容的逐字节导出另由合成载荷测试覆盖；公开语料对照不声称逐字节比较所有大数组导出。报告见 [ROS 1 验证记录](../examples/validation/ros1-0.15.28.json)，原始录制不随包分发。
