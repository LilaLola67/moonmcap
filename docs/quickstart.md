# 首次使用：查询、裁剪和恢复

需要 moonc 0.10.17+d971670cd（项目固定基准）或经过验证的更新工具链、
Node.js 22.3+。以下从全新检出的仓库根目录执行，不需要 Python 参考库、
PyArrow、Foxglove、网络日志服务或实体设备。`moon update` 需要下载固定依赖。
这些命令也在不含工作区构建产物的源码导出副本中自动验证。

先安装依赖并构建：

```sh verify:quickstart
moon update
moon check --target js
moon build --target js
```

输入为仓库自带的合成数据，包含 `/imu` 和 `/camera` 共 100 条消息。
查询 `/imu` 的 20–30 秒闭区间，并先完整校验输入：

```sh verify:quickstart
node _build/js/debug/build/cmd/mcap/mcap.js query examples/files/zstd-summary.mcap --topic /imu --start 20000000000 --end 30000000000 --full
```

JSON 的 messages 应有 **5 条**，log_time 为字符串
`21000000000`、`23000000000`、`25000000000`、`27000000000`、`29000000000`；
diagnostics.full_validation 为 true。数值参数单位是纳秒，`20` 表示 20 纳秒。

将相同窗口写为 LZ4 文件，再完整验证输出：

```sh verify:quickstart
node _build/js/debug/build/cmd/mcap/mcap.js slice examples/files/zstd-summary.mcap --topic /imu --start 20000000000 --end 30000000000 --compression lz4 --output quickstart-slice.mcap
node _build/js/debug/build/cmd/mcap/mcap.js validate quickstart-slice.mcap
```

新文件应有 **5 条消息、1 个附件、1 份元数据**。目标路径必须不存在，父目录
须已存在；再次执行同一裁剪命令应退出 2 并保留原文件，不会自动覆盖。

恢复被截断的示例，只保留完整的连续前缀：

```sh verify:quickstart
node _build/js/debug/build/cmd/mcap/mcap.js recover examples/files/truncated-zstd.mcap --compression zstd --output quickstart-recovered.mcap
node _build/js/debug/build/cmd/mcap/mcap.js inspect quickstart-recovered.mcap
node _build/js/debug/build/cmd/mcap/mcap.js validate quickstart-recovered.mcap
```

恢复报告在标准错误中，recovered_messages 为 **48**、stop_offset 为 **3404**、
complete 为 false；成功生成片段时进程仍退出 0。新文件有 **48 条消息**，
附件和元数据均为 0；原来截断点后的数据不会被补造。
不要仅以标准错误有内容判断失败，应检查退出码和报告。

完整输入与预期输出的 SHA-256、来源及独立 MCAP reader 核对见
[文件样例](../examples/README.md)。实际大型输入应先看 [使用边界](scope.md)，
再选择流式片段、外部排序或大文件诊断。

遇到环境、路径、预算或格式错误，见 [排错说明](troubleshooting.md)。
复制源码副本并重放本文命令及 README API 示例：`python tools/quickstart_smoke.py`。
