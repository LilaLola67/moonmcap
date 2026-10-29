# 事件工作流配置

这些配置可配合仓库已有的合成 MCAP 样例运行；不含真实设备或个人数据。

```sh
moon build --target js
node _build/js/debug/build/cmd/incident/incident.js quality examples/files/zstd-summary.mcap examples/incidents/quality.json
node _build/js/debug/build/cmd/incident/incident.js events examples/files/zstd-summary.mcap examples/incidents/events.json
```

输出命令的目标必须是新文件，父目录须已存在；下面的路径仅作示例，每次运行使用新的名称：

```sh
node _build/js/debug/build/cmd/incident/incident.js clip examples/files/zstd-summary.mcap examples/incidents/clip.json fault.mcap
node _build/js/debug/build/cmd/incident/incident.js capture examples/files/zstd-summary.mcap examples/incidents/capture.json capture
node _build/js/debug/build/cmd/incident/incident.js clock examples/files/zstd-summary.mcap examples/incidents/clock.json calibrated.mcap
node _build/js/debug/build/cmd/incident/incident.js batch examples/files/zstd-summary.mcap examples/incidents/batch.json clip-job
node _build/js/debug/build/cmd/incident/incident.js resume-batch examples/files/zstd-summary.mcap examples/incidents/batch.json clip-job
```

quality 检查 20–30 秒的 `/imu` 消息，clip 导出这一区间的 5 条消息；events 的 `enter:true` 用于演示不依赖字段的事件，按间隔界限判断连续性，EOF 处事件不完整关闭。capture 是文件重放到增量 API 的演示。clock 的锚点仅给全部 log_time 平移 1000 纳秒，保留 publish_time，并用独立 probe 检查结果。

真实电流阈值、迟滞和上下文补全配置见 [使用说明](../../docs/incident-workflows.md)。`python tools/incident_workflows.py` 会另外生成有两个可控异常事件的独立合成数据，对照三种压缩及全部失败路径。

batch 使用新目录，按两个不重叠窗口输出 `/imu` 消息，并保留输入的附件及元数据。再次 resume-batch 会校验已完成输出并复用，恢复语义及预算见[流式批量导出](../../docs/stream-context.md)。
