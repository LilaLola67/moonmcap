# 公开驾驶录制的雷达片段与恢复

用 MoonMCAP 从公开驾驶录制中取出一小段 `/radar/tracks`，检查导出的消息内容，再从人为截断的录制副本中恢复连续有效前缀。输入是由 Foxglove 公开 ROS 1 `demo.bag` 转换得到的 MCAP，包含雷达、相机、点云与坐标变换。下载和转换后的 SHA-256 都固定在公开清单中。 [来源及指纹](../validation/public-corpus.json)

在仓库根目录执行以下命令。工具链使用项目的固定 MoonBit 基准和 Node.js 22.3 或更新版本。

```sh
moon update
python -m pip install mcap==1.3.0 lz4==4.4.5 zstandard==0.25.0 rosbags==0.11.0
python tools/driving_workflow.py
```

首次运行通过既有公开语料流程下载和转换数据，需要网络。已有缓存时会验证固定指纹。程序打印 `_interop/driving-example/` 下本次生成的独立目录。

1. 校验源容器并读取全部 1,606 条消息。用 `rosbags` 对照 ROS 1 解码结果，长数组按该回归的边界核对长度和字节数。
2. 取雷达轨迹主题的中间五个不同记录时刻，使用闭区间查询并裁成 `radar-clip.mcap`。Python MCAP 读取器逐条核对时间、序号、负载字节、消息编码和 schema。
3. 在中间一个 Chunk 内截断副本。完整校验必须拒绝副本。随后生成 `recovered-prefix.mcap`，与独立读取器在该 Chunk 之前读出的消息逐条比较。

`summary.json` 记录起止时间、消息数、截断偏移和产物 SHA-256。输入为公开驾驶采集，截断是可复现的人为故障。恢复输出只包含截断前的完整前缀。原始和转换后的录制由运行者按来源条款获取或生成。 [语料验证说明](../../docs/corpus-validation.md)

固定指纹输入的实测结果为 1,606 条 ROS 1 消息通过独立解码对照，裁剪得到 5 条雷达轨迹消息，从截断副本恢复 788 条消息。两个输出文件均通过结构校验与独立读回对照，源文件通过操作后的 SHA-256 核对。

GitHub Actions 的 `check-build-test` 可手动勾选 `public_corpus` 复跑这个案例。下载与转换指纹、裁剪和恢复结果收录在 `public-corpus-report` 构件中的驾驶案例 `summary.json`。 [工作流](../../.github/workflows/ci.yml)
