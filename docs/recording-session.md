# 持久分段录制会话

`open_recording_session(template, storage, manifest?, options=recording_options())` 提供独立于内存 RotatingWriter 的持久录制器。template 仅含 Header/Schema/Channel；录制对象是消息，附件和 Metadata 需由其他现有接口处理。每段都是可独立完整验证的 MCAP 文件。

`RecordingStorage` 适配创建新段、读取、原子发布、保存清单、隔离尾段、枚举和 SHA256。跨平台核心可由调用者提供存储；Node.js 的 `nodeio.open_recording_directory` 提供本地实现，使用专有目录标记和独占进程锁，拒绝并发写入，进程退出后可接管过期锁。文件发布使用同目录硬链接，要求文件系统支持硬链接。

## 写入与恢复

1. MRSC1 清单绑定声明、压缩/Chunk 配置、分段阈值、序号策略与可选来源身份。先原子持久化保留编号，再创建 `.part`。
2. 按消息数、软字节目标或时间跨度轮转。完成 Footer 后同步文件、以新文件名发布 `.mcap`，完整扫描和计算 SHA256，再原子更新清单。
3. 恢复时重新扫描并校验每个已提交段的 SHA256、长度、声明、计数、时间边界和序号状态。已提交段损坏直接失败，保留原文件和清单。
4. 保留编号下的尾段若完整且匹配配置，会接管到清单，修复“文件已发布、清单未提交”的窗口；不完整或不匹配则原字节隔离为 `.quarantine`。隔离和发布之间的中断也可重复恢复。
5. 后续只用**新编号**创建新文件，恢复已提交计数和每 Channel 的序号。不会对已完成 MCAP 追加，也不会把不完整尾段伪装成已提交进度。

`PreserveSequence` 保留调用者序号；`ContinueSequence` 从每个 Channel 的 0 开始，跨段/恢复递增，超出 UInt32 明确失败。时间戳保持原值，可以物理无序。已接收但未提交尾段的消息由调用者重发，核心不承诺这些消息的跨故障恰好一次。清单只记录已提交状态，`info.active_messages` 是当前进程的暂存进度。

默认每段 10000 条、最多 4096 个编号，声明至多 1 MiB、1024 Schemas/Channels，清单至多 8 MiB，Chunk/摘要仍受 StreamWriter 原有限制。字节目标为软阈值，尚未刷出的 Chunk 和最终索引会使实际文件大于目标。恢复的全文件 SHA256/校验读取成本随已完成段总量增加，未声称常数时间启动。

## 可运行的本地来源重放

```powershell
moon build --target js
node _build/js/debug/build/cmd/session/session.js new my-session recording.mcap --segment-messages 1000 --compression zstd --sequence continue
node _build/js/debug/build/cmd/session/session.js inspect my-session
node _build/js/debug/build/cmd/session/session.js resume my-session recording.mcap --segment-messages 1000 --compression zstd --sequence continue
```

`new` 要求新目录，父目录须存在；`resume` 要求已拥有的会话目录，所有配置须与原清单一致。此示例先完整验证输入并绑定 SHA256，再以相同物理顺序重放消息；恢复时跳过已提交的源消息前缀，因此对同一固定文件可避免重复。换源或换配置会失败。`--pace-ms` 可模拟实时输入速度，不参与会话身份。`inspect` 展示清单和文件库存，只确认清单封装/CRC；完整段的内容审计发生在 resume。

本地适配同步文件与清单，在支持的平台同步目录；Windows 的目录 fsync 受平台限制。验证范围是进程中断与提交窗口，**未做断电/存储设备故障保证**。核心 storage 的持久化责任由适配器承担。库调用者异常退出须关闭目录以释放句柄，未完成尾段仍保留以供下次检查。

Windows 的清单替换采用与合并保存点相同的有限共享占用重试，持续错误仍会失败；清单成功提交后才推进持久计数。

复现 `python tools/recording_session_workflows.py`：独立 writer 生成来源；三种压缩各实际终止一个录制进程，验证已提交前缀逐字节保留、720 条消息不重复恢复、各 Channel 的连续序号、部分尾段原样隔离、完整孤立段接管、两个硬链接提交窗口的幂等恢复、损坏已提交段拒绝及并发锁。全部测试产物留在忽略目录。
