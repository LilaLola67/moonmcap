# 持久化侧车索引（0.15.3）

```sh
moon run cmd/mcap --target js -- index-build input.mcap --output input.midx
moon run cmd/mcap --target js -- index-check input.mcap --index input.midx
moon run cmd/mcap --target js -- index-query input.mcap --index input.midx --topic /imu --limit 32 --format ndjson
moon run cmd/mcap --target js -- index-query input.mcap --index input.midx --fast-index --start 1000 --end 2000
moon run cmd/mcap --target js -- index-rebuild input.mcap --output rebuilt.midx
moon run cmd/mcap --target js -- dataset-query part0.mcap part1.mcap --index part0.midx --index part1.midx --fast-index --format ndjson
python tools/sidecar_workflows.py
```

侧车文件独立于 MCAP，不修改输入。第一次按物理顺序完整验证容器，记录声明和每条消息的时间、通道、顶层记录位置/跨度、解压块内偏移；不把所有消息负载装入 Log。覆盖合法顺序文件、无摘要/无索引分块文件和已有索引文件，三种压缩均可。输入必须是完整可校验容器；截断或损坏摘要需要先使用相应恢复/重建工具，不能把损坏输入索引成“正常文件”。

私有侧车格式版本为 1，含 magic、版本、源文件大小、SHA-256、可选身份令牌、声明容器、定位表和整份侧车 CRC。CRC 始终比较，零值也不是跳过标记。解析检查大小、数量、声明、物理位置次序、重叠、跨度及尾随字节；取消息时再次检查真实记录类型、块解压/CRC、内部记录边界和消息通道/时间。index-check 逐项读取核对定位表；未知格式版本拒绝。

**两种打开方式有不同保证与成本：**

| 方式 | 检查与成本 |
| --- | --- |
| 默认内容检查 | 完整顺序读取源文件计算 SHA-256，与建索引时匹配；可检测等长内容变化。省去重复解析、解压和重新建立位置索引，仍需 O(文件字节数) 的校验 I/O。 |
| 显式 --fast-index | 比较由文件大小、设备号、inode、纳秒修改/状态时间生成的身份令牌；不扫描整个内容，content_verified 为 false。依赖文件系统身份属性可信，不能替代内容校验；复制、替换或触碰文件属性可能需要重建索引。 |

索引 CRC 和源文件摘要用于检测损坏/过期，不是签名，不能证明恶意修改并重新生成校验的索引可信。应使用可信构建过程产生的侧车文件。默认模式不信任仅相同大小；快速模式不会在报告中冒充 SHA-256 验证。Node 的 ReadSource 在每次实际读取前后检查文件身份，运行中变化也会拒绝。

库接口 build_sidecar(source, fingerprint, identity~, limits~, index_bytes~, declaration_bytes~, cancelled~) 接收宿主 SHA-256 回调。Node 提供 nodeio.source_sha256 和 file_identity_token；其他宿主需正确实现 SHA-256 及稳定源快照约定。open_sidecar 使用 VerifyContent(callback) 或显式 TrustIdentity(token)，返回 SidecarReader。回调不能用 CRC 代替 SHA-256；便携核心测试中的模拟指纹仅用于测试适配协议。

SidecarReader.cursor 支持 next/next_page、双向 seek、offset/limit、取消和 NDJSON。索引位置一次排序，二分时间边界；只读取实际返回消息所在记录，块内偏移二分查找。时间相同按原始物理位置排列，倒序反转全序。query_source 接入 Dataset，进度不计为 materialized_sources；CLI 的 --index 按输入顺序逐一配对。

默认 index_bytes 32 MiB（CLI --index-bytes）、声明 8 MiB；记录/解压/数量沿用 Limits。单块缓存默认 8 MiB，可通过 --cache-bytes 设置，超大块解码但不保留，clear_cache 主动释放共享缓存。定位表及排序位置数组为 O(消息数)，受索引/数量预算约束；不声称恒定总 RSS。取消一个游标不破坏共享读者的缓存及其他游标。

index-build/index-rebuild 独占发布新路径，已有文件拒绝覆盖。重建明确写入新文件，旧索引保留；预算或验证失败不留下成功文件。CLI 生成的索引不记录源文件路径、账号、联系方式或凭据；自定义调用者应使用不含私人信息的身份令牌。
