# 离线 TF 历史树与语义上下文

`TfTree` 保存 parent_T_child，`lookup(target,source,time)` 返回 target_T_source。
链路组合与逆变换使用单位四元数；动态边在线性平移和最短弧 SLERP 之间插值，
保留原始样本作为 evidence，并标记 interpolated。时间为精确 UInt64 纳秒，
插值比例和空间坐标使用浮点数。静态边适用于所有查询时刻。

```sh
moon run cmd/payload --target js -- tf-query input.mcap examples/tf/query.json lookup.json
moon run cmd/payload --target js -- tf-clip input.mcap examples/tf/clip.json context.mcap
```

入口读取 `/tf` 与 `/tf_static`，支持已有标准解码器可解析的 JSON、ROS1 和 ROS2 CDR
TFMessage。ROS1 解码器将 time 统一为 sec/nanosec，因此同一 TF 解析器可用。
header.stamp 是查询时钟；不会把 log_time 自动当作 TF 时间。负数和溢出拒绝。
保留原始消息信封、通道和字节，context 片段不会用插值结果伪造原始观测。

树拒绝环、自指、父节点变更、静态/动态混用、同一时刻不同值、倒序更新、
零或非有限四元数、非有限平移。相同静态状态和相同时间相同动态值重复广播幂等。
四元数使用缩放后归一化，可处理很大的有限分量。未知帧、断开的根、历史范围外
均报错；不外推。max_gap 限制两侧括点距查询时刻的最大距离。
查询只遍历到最近公共祖先，共同祖先上方缺少时间数据不会阻止相对坐标查询。

`context(frames,start,end)` 对请求帧的祖先路径保留静态状态、区间内所有动态更新，
以及起止边界所需的前后括点。相同原始 TFMessage 只导出一次；该消息若携带其他
变换也整体保留。tf-clip 的 start/end 明确是 MCAP 日志时间，tf_start/tf_end
明确是 TF header 时间，两者均需配置。topics 控制日志窗口中的普通消息，
语义 TF 上下文会额外保留，即使其日志时间在窗口外。copy_auxiliary 默认 false。
输出重建 CRC 和索引，附带源散列及上下文清单；错误不发布最终文件。

默认保留最多 1000000 个样本、128 MiB 逻辑历史字节、4096 条边；CLI 可配置
sample_limit 与 byte_limit。逻辑字节按样本开销、帧名和原始消息计算，重复来源
可能重复计费，不代表 RSS。标准 JSON 解码预算为 1 MiB；查询批最多 10000 项。
拓扑改变的记录应显式拆分会话，动态历史不会自动淘汰，超预算报错。

`python tools/tf_workflows.py` 使用独立 JSON/CDR writer 覆盖三种压缩，刻意设置
不同的 header 与 log 时间，检查片段内保留的静态状态和动态括点能复现原始查询。
核心测试还覆盖旋转、逆变换、反向等价四元数、环、父节点冲突和预算错误。
