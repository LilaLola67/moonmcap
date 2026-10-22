# 时间回放（0.15.9）

`replay(QuerySource, query~, rate_numerator~, rate_denominator~)` 创建回放器，`poll(monotonic_ns)` 返回 ReplayEvent、ReplayWait（剩余纳秒）、ReplayIdle（暂停）或 ReplayEnd。首次消息立即就绪，随后按 log_time 调度；同时间消息保持查询提供方的稳定次序。提供方必须升序，拒绝倒序查询。

pause/resume、set_rate、step、seek 和 cancel 可独立调用。暂停保留整数纳秒虚拟位置；暂停状态才能单步，单步后位置移动至该条消息。修改倍速保留当前整数位置，比例两个分量均为 1..1000000。seek 在原查询范围内重新打开消息流、清除 offset、重置本次 limit/计数，保持暂停状态并增加 generation。结束后可 seek；取消或失败不可重开。时钟必须单调递增，相同时间允许；调度使用整数/大整数计算，避免浮点时间和 UInt64 乘法溢出。

`progress(now)` 返回状态、虚拟位置、已交付条数、倍速和代数。虚拟位置达到 UInt64 最大值时饱和；输出失败或取消不能撤回已交付消息。回放只保留一个待调度消息，底层游标候选仍受 pending_bytes 限制。

`asyncio.playback(replay, clock, wait, emit, cancelled~)` 接受可注入时钟、异步等待和异步消息回调，等待输出回调完成后才取下一条。单次等待最多 50 ms，允许暂停/倍速/跳转/取消在等待间生效。消息处理速度低于回放倍速时延后交付，不丢弃消息或改变时间戳；这是单机尽力调度，不是硬实时保证，也不自动向 ROS 网络发布。

```sh
moon build --target js
node _build/js/debug/build/cmd/replay/replay.js input.mcap --rate 2/1 --topic /imu --start 100 --end 1000000000
```

示例支持查询条件、JSON 筛选/投影和侧车索引，按实际时钟等待，并以有背压的 stdout 输出 NDJSON；dispatch_clock_ns 是宿主单调时钟，仅用于测量，不是 Unix 时间。`python tools/replay_workflows.py` 验证三种压缩、两档倍速、输出顺序和实际截止时间。核心注入时钟测试覆盖暂停/单步/恢复/跳转、分数倍速、同时间消息、时钟倒退及 UInt64 边界；异步测试覆盖回调背压和取消。
