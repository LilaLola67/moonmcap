# 数据质量、事件片段与时钟校正

这些接口围绕实验数据的检查和故障复现，可独立使用。同步核心支持 Wasm/JS；文件命令需要 Node.js 22.3+。

```sh
moon build --target js
node _build/js/debug/build/cmd/incident/incident.js --help
python tools/incident_workflows.py
```

命令形式为 `incident MODE INPUT CONFIG.json [NEW_OUTPUT_OR_PREFIX]`。配置使用严格 JSON：未知键、重复键和错误类型会拒绝。时间和时间跨度一律使用**十进制纳秒字符串**，数量/字节预算使用非负 JSON 整数。源文件首先完整验证，再计算 SHA256；报告输出 stdout，不修改输入。现有文件不能覆盖。

## 质量检查

```sh
node _build/js/debug/build/cmd/incident/incident.js quality recording.mcap examples/incidents/quality.json
```

配置示例：

```json
{
  "start": "0", "end": "10000000000",
  "rules": [
    {"topic": "/imu", "minimum_messages": 100, "max_gap": "50000000", "check_sequence": true,
     "condition": {"path": "/temperature", "op": ">=", "value": -40}},
    {"topic": "/camera", "minimum_messages": 1, "warning": true}
  ],
  "pairs": [{"left": "/camera", "right": "/imu", "tolerance": "10000000", "minimum_basis_points": 9500}],
  "detail_limit": 64, "decode": true
}
```

`check_quality(source, rules, start, end)` 消费整个闭区间，不应用消息分页。每个话题一个规则，最多 128 条；`minimum_messages` 默认 1，`max_gap` 可省略。间隔检查包含区间起点到首条、末条到终点的静默时间，多个同名话题通道合并计算话题间隔；序号按 Channel 分别检查，UInt32 回绕到 0 视为连续。报告通过/警告/失败、违规总数、有界时间证据和省略数。每条规则默认最多 64 条详情，可配 0..1024。

`condition` 使用已有精确 JSON 条件，可组合范围、字段存在性等；缺失字段不匹配，非法载荷停止检查。`decode:true` 通过标准 JSON/Protobuf/CDR/ROS1 解码器读取字段；默认只适用于 JSON 字段条件。没有字段条件的规则可以检查不透明载荷。

`check_match_quality(left, right, tolerance, minimum_basis_points)` 复用有界 nearest 时间关联，以每条左消息为分母。10000 基点表示 100%，向下取整；同一右消息可以服务多条左消息，这衡量**匹配可用率**，不是一对一同步质量。空左流明确失败。CLI 的 `pairs` 可省略，指定时话题必须存在且不同。

退出码：0=通过或仅警告，1=数据不满足规则/匹配阈值，2=参数、I/O、容器或解码错误。序号跳变是线索，不证明传输丢包。排序后的查询流也不能证明源物理顺序正常。本版本不从不同设备的时间差推断网络延迟。

## 离线事件定位与片段提取

```sh
node _build/js/debug/build/cmd/incident/incident.js events recording.mcap examples/incidents/events.json
node _build/js/debug/build/cmd/incident/incident.js events recording.mcap examples/incidents/events.json ./fault
```

第二条命令创建 `fault-0.mcap` 等新文件；父目录必须存在。不提供前缀时仅输出事件清单。每个文件独立原子发布；多文件批次不是单个事务，后续失败时此前文件保留，不应自动重试并覆盖。

```json
{
  "rule": {
    "topic": "/motor", "enter": {"path": "/current", "op": ">", "value": 20},
    "leave": {"path": "/current", "op": "<", "value": 18},
    "duration": "200000000", "max_gap": "50000000", "cooldown": "1000000000",
    "pre": "5000000000", "post": "10000000000"
  },
  "topics": ["/motor", "/imu"],
  "context": [{"topic": "/calibration", "depth": 1, "max_age": "60000000000", "required": true}],
  "event_limit": 1024, "merge_gap": "0", "decode": true
}
```

`EventDetector.feed(message)` 只接收规则话题的升序消息。进入条件连续成立达到 duration 后发出 EventStarted，保存候选起点与确认时间；确认前一旦不满足则重置。确认后使用独立 leave 条件提供迟滞，不再要求 enter 始终成立。省略 leave 时为 enter 的反条件。若同一条同时满足 enter 和 leave，leave 优先。duration 默认为 0；max_gap 必须大于 0，默认 1 秒。

连续性是采样证据：间隔超过 max_gap 时，活跃事件在上一条消息处截断并标记 `closed:false`；不能跨空白认定持续成立。退出消息作为事件 end；EOF/查询边界处仍活跃的事件也标为未完整关闭。cooldown 从上次事件 end 开始，按规则话题 log_time 计。输入应已正确校准；不自动理解载荷 header 时钟。

`detect_events` 保留最多 event_limit 个事件，默认 1024、最大 100000，超限失败；支持取消并关闭源流。`event_windows` 加前后范围（UInt64 边界饱和）后合并重叠或间隔不超过 merge_gap 的窗口，保留各事件证据。start/end 可限制检测范围，但 pre/post 可以扩出检测范围到源文件的其他消息。

## 上下文补全

```sh
node _build/js/debug/build/cmd/incident/incident.js clip recording.mcap examples/incidents/clip.json fault.mcap
```

`extract_context_clip` 读取窗口内选定话题，以及各 context 话题在窗口开始前的最后 N 条消息。depth 默认 1、最大 10000；max_age 限制向前寻找范围，required 默认 true。必需上下文一条也找不到则失败，可选上下文在报告中标记 missing；depth 是保留上限，找到少于 N 条也可成功。上下文不受窗口 topics 限制，保留原始 payload、时间、序号，并与窗口消息稳定排序。上下文严格早于窗口，所以不会重复加入窗口内同一条消息。

这是**用户指定的消息上下文**：不会自动推断业务依赖，也不会把最后一条 `/tf_static` 当作完整 TF 状态。分散在多条消息中的状态应显式指定足够 depth；没有实现按坐标关系合并的 ROS 专用算法。

输出只包括相关声明、消息及生成的说明元数据；**不携带来源附件和元数据**，报告明确记录此策略。事件导出文件另外包含 `moonmcap.incident` 元数据，保存源 SHA256、触发证据和上下文清单。

每个片段的消息默认不超过 100000 条、载荷记账不超过 64 MiB（含每消息 31 字节记账），可通过 clip_messages/clip_bytes 调整。上下文和窗口消息合并计费，超限失败而非裁去尾部。核心读取按需进行，但**导出结果在内存中组装**；写出缓冲、声明、索引与上游解压另占空间，不是 RSS 上限。

## 事前缓冲与触发录制

```sh
node _build/js/debug/build/cmd/incident/incident.js capture recording.mcap examples/incidents/capture.json ./capture
```

`trigger_recorder(template, emit, options)` 接收只含声明的 Log；任意生产者都能逐条 `feed`，由业务调用 `trigger(label)`，最后 finish/cancel。输入需按 log_time 非递减；未声明通道、单条超缓冲或倒序会使实例失败并释放缓冲。默认声明上限 1 MiB。

pre/post 定义以**触发时最新已接收消息**为中心的闭窗口。环形缓冲同时受时间、字节和消息数量限制；时间淘汰与容量淘汰分开计账，丢弃的 Bytes 引用及时释放。触发后重复 trigger 会延长同一片段的结束时间，标签最多 128 个。活动片段超过 clip_bytes/clip_messages 则明确失败，不能伪装为完整成功。emit 接受 Log 和清单，同步返回构成背压；输出异常使实例失败，禁止自动重复发布。

默认 buffer_bytes=16 MiB、buffer_messages=10000、clip_bytes=64 MiB、clip_messages=100000、event_limit=1024。环形缓冲和活动片段分别记账，可能同时保留引用；声明和最终文件编码缓冲另计。首段历史不足或容量淘汰影响请求范围时 `pre_complete:false`；EOF 尚未到达后沿时 `post_complete:false`。

capture 命令把文件按时间重放进增量 API，事件确认时触发；因此 pre/post 围绕**确认时间**，离线 events 导出的窗口则围绕事件起点和退出点，两者用途不同。decode 只用于判定，输出仍保留原始序列化载荷。该命令用于可复现接入示例，没有实现 ROS 网络订阅，也不保证实时调度。缓冲不跨进程持久化；已有 RecordingSession 继续负责持久录制和故障恢复。

## 显式时钟映射

```sh
node _build/js/debug/build/cmd/incident/incident.js clock recording.mcap examples/incidents/clock.json calibrated.mcap
```

`clock_map` 接受 2..4096 个 ClockAnchor，source/target 两侧都必须严格递增；整数分段线性插值向下取整，中间乘法使用大整数，最终检查 UInt64 上下界。默认拒绝域外映射；显式 extrapolate 才允许两端线性外推，仍不能溢出。

`QuerySource.calibrate_time` 提供查询视图：时间条件和 offset/limit 在映射后应用，原输入不变。为避免逆映射取整漏查，先扫描原始校准域再精确筛选，**未承诺校正时间的索引加速**；默认视图只包含首末锚点源时间之间的数据。排序沿用原始源时间顺序，映射取整造成相同时间时保留此稳定次序。

`calibrate_log` 默认修改消息及附件的 log_time，保留 publish_time/create_time；只有 same_publish_clock=true 才声明这些时钟同源并一起映射。载荷内部的 header.stamp 等字段始终不修改。文件重写先按源 log_time 稳定排序，使映射取整后的并列顺序与查询视图一致；writer 重新生成 Statistics、索引和 CRC，原侧车不能用于新文件。映射及策略写入 `moonmcap.clock-map` 元数据。

`ClockMap.residuals` 接受独立 probes，报告预测减参考值的有符号纳秒残差；锚点本身插值为零不能证明准确。CLI 的 probes 可省略。CLI 文件重写是内存路径，默认 max_file_bytes=64 MiB，先验证来源快照再在大小上限内读取；核心查询视图则可配合已有文件源。

当前证据为独立 Python MCAP writer/reader、整数映射 oracle、边界和故障用例，**没有真实多设备同步实验，也不声称自动估计时钟偏差或校准精度**。

## 复现与失败边界

`tools/incident_workflows.py` 对三种输入压缩构造物理乱序日志，核对质量规则与匹配比例、持续事件和迟滞退出、合并窗口、上下文原始字节、事前/事后捕获、分段时钟 oracle，以及文件已存在、必需上下文缺失、预算不足和域外校正的失败输出。报告位于忽略目录 `_interop/incidents/report.json`。单元测试额外覆盖冷却、间隔截断、EOF、不完整历史、输出异常、取消、UInt64 精确值和查询分页。

下载已有固定指纹的公开语料后，可运行 `python tools/incident_workflows.py --public`，在 SO-101 原生 ROS2 MCAP 上核对字段存在性与消息数、解码事件边界、原始 CDR 上下文片段；报告另存 `_interop/incidents/report-public.json`。这不是电机故障标注，也不提供真实时钟校准精度证据，原始数据不随库分发。
