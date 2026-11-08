# 在 Foxglove 桌面端核对 SO-101 裁剪

此案例对比公开的 SO-101 原件与 MoonMCAP 导出的片段。片段的窗口包含 14 条 `/joint_states` 和 17 条 `/tf`，并附带窗口前最后一条 `/tf_static`。静态 TF 保留原始时间，供查看片段时恢复坐标系上下文。

```sh
moon update
python -m pip install mcap==1.3.0 lz4==4.4.5 zstandard==0.25.0
python tools/foxglove_so101.py
```

脚本使用固定指纹的录制，输出 `so101-joints-and-tf.mcap`、`clip.json` 和 `summary.json`，终端会打印本次目录。原件位于 `_interop/public-corpus/so101-real.mcap`。独立读取器已对照裁剪负载、时间、序号和静态 TF 上下文，并开启可用 CRC 的校验。

桌面端需要能够打开本地文件的 Foxglove 开发者席位。 [Foxglove 本地数据文档](https://docs.foxglove.dev/docs/visualization/connecting/local-data)

1. 在两个数据源槽位分别打开原件与裁剪文件，或在两个标签页中打开后复用相同面板设置。
2. 在 Plot 添加 `/joint_states.position[0]`。用接收时间作为横轴，在原件中缩放到 `1791983428732915000` 至 `1791983428992984000` 纳秒的闭区间，和裁剪片段对照采样点。可在 Raw Messages 核对 `name[0]` 与 `position[0]` 的对应关系。
3. 在相同绝对时间查看 `/tf` 和 `/tf_static`。在 3D 面板选择两个数据源都存在的显示坐标系，核对父子坐标系及对应变换。如果面板使用 ROS 消息时间戳，核对 `header.stamp` 与记录时间的区别。 [3D 面板文档](https://docs.foxglove.dev/docs/visualization/panels/3d)
4. 记录 Foxglove 版本、面板配置、对照时刻和实际结果。截图只保留与数据和面板有关的区域。

当前证据为本地文件生成和独立读回，桌面观察待执行。原始录制由复现者从发布方获取。 [原件与指纹](../validation/public-corpus.json)
