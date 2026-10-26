# 公开采集日志与复杂分布回归

```sh
python tools/corpus_workflows.py
python tools/corpus_workflows.py --public
```

默认 CI 运行九组明确标注为 synthetic 的输入：三种压缩分别覆盖同时间戳、逆序、重叠块，每组 32 个通道，包含空负载、不同大小的二进制消息及 UInt64 时间边界。它们测试数据分布，不代表真实传感器采集。

`--public` 额外读取 [固定清单](../examples/validation/public-corpus.json)，只下载清单内的公开文件，限制总大小并强制匹配 SHA-256。已经存在但指纹不符的文件会拒绝，不静默覆盖。原始日志存入忽略目录 `_interop/public-corpus`，不会进入 Git 或 Mooncakes。公共数据沿用发布方自身条款，项目 MIT 许可不覆盖这些录制文件。

首个真实采集案例为 [Foxglove 发布的 SO-101 机械臂录制](https://foxglove.dev/blog/record-and-replay-with-a-ros-2-powered-so-101)：5 个 ROS 2 CDR 通道、4,719 条消息，包含关节反馈与坐标变换。该样例未录制摄像头，不将它称为图像/点云数据。其 DataEnd CRC 为零，报告的 data_crc_verified 为 false，不能把完整解析通过写成已验证未提供的 CRC。

每个案例由官方 Python MCAP 读取器按物理顺序建立参考结果，核对完整诊断、侧车构建与逐项验证、正序/倒序/主题分页的两种查询路径，以及输入 SHA-256 不变。公开日志和合成用例在报告中分别标记，报告位于 `_interop/corpus/report-public.json` 或 `report.json`。随仓库提供 [首轮实测报告](../examples/validation/public-corpus-0.15.13.json)。

GitHub Actions 普通提交运行离线合成回归；手动运行勾选 `public_corpus` 才下载并验证公开日志。网络失败、清单指纹变化会使该任务失败，不把未执行的样例标为通过。真实数据的容器/索引互操作与 payload 解码分别验证；0.15.20 起另提供 [CDR 解码对照](cdr-decoding.md)，核对 SO-101 全部 4719 条消息。这不证明尚未覆盖的设备、CDR 表示、文件布局和规模都能通过。

0.15.14 增加 Foxglove 公开驾驶 demo.bag：7 个通道、1,606 条消息，包含相机压缩图像、激光点云、雷达和变换。原 bag 指纹固定，使用 rosbags 0.11.0 和官方 MCAP 写入器转换，逐条核对负载字节、主题及存储时间一致；输出按参考读取器的时间顺序，MCAP sequence 新分配，publish_time 取 bag 存储时间，不冒称原发布时刻。原始及转换数据均不重新分发；复现此用例须额外安装 `rosbags==0.11.0`。转换产生的 Zstd 文件暴露了引用解码器对五字节 Huffman literal 头的位掩码错误，修正后通过完整校验；[扩展报告](../examples/validation/public-corpus-0.15.14.json)分别记录两份公开数据与九组合成用例。
