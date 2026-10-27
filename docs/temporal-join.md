# 跨话题时间关联

`temporal_join(left_stream, right_stream, options=join_options())` 提供流式、按 log_time 的多对一左关联。同步/异步接口共用非阻塞 `TemporalJoinCursor` 协议，可以接入已有 Dataset、范围读取、MIDX2、远程缓存及解码管线。

- `JoinNearest` 选择绝对时间差最小的右记录；等距离选择更早时间。
- `JoinAsOf` 选择不晚于左记录的最近右记录。
- 右侧重复时间戳选择输入稳定顺序中的第一条，右记录可匹配多个左记录。
- `tolerance` 以纳秒计，默认 0（仅精确时间）；`JoinKeep` 保留未匹配左记录并返回 right=None，`JoinDrop` 丢弃。

输入须按时间升序，已观察到的倒序会立即失败；底层 MCAP 的物理消息可以无序，由 Dataset 先排序。关联器保存至多三个载荷（当前左记录、前一个与下一个右候选），分别检查消息数及字节预算，时间差使用 UInt64 安全减法。此预算不包含上游解压/cache/预取，后者仍由各自参数控制。

```powershell
moon build --target js
node _build/js/debug/build/cmd/join/join.js run1.mcap run2.mcap --left /camera --right /imu --mode nearest --tolerance 10000000 --missing keep --decode --workers 2 --offset 5 --limit 20 --output paired.ndjson
```

命令沿用统一管线的本地/HTTP 来源、索引、字段索引、缓存和按需解码参数；左右流分别展开必要的时间窗口，关联和缺失策略之后再执行 offset/limit。`--where` 条件同时应用于两个话题；输入的 `--topic`/降序选项拒绝，以免与左右定义冲突。输出保留两条消息各自的 topic/channel/sequence/log_time/publish_time，不改写时间戳。此配对格式不携带原始文件序号；同时间的选择仍遵循 Dataset 稳定顺序。

输入读取采用拉取式背压。左流结束即停止，不额外验证未使用的右侧尾部；这是关联查询，不是整个容器的完整校验。调用者需要全量校验时先执行 validate。发生错误、取消或达到 CLI 输出条数时，两个上游游标均关闭；库调用者提前停止须调用 cancel。文件输出失败不发布目标文件，stdout 可能已经包含此前有效行。

`python tools/temporal_join_workflows.py` 用独立 writer 创建物理无序、时间交叠的两个文件，对照穷举最近/as-of 匹配，覆盖三种压缩、保留/丢弃缺失、关联后分页和缓冲失败；单元测试另覆盖重复值、等距离、UInt64 极值、倒序与异步取消。
