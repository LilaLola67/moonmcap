# 异步 MIDX2 与远程索引

`@aio.open_disk_sidecar(source, index, verification)` 接受两个 AsyncSource，支持本地异步文件、HTTP Range 或持久缓存。MIDX2 格式不变；原 MIDX1/同步接口继续使用。返回 AsyncDiskSidecarReader，其 query_source 可组合解码、JSON 条件、跨源数据集分页和 NDJSON。

```moonbit
let source = @aio.open_file("input.mcap")
let index = @aio.open_file("input.midx2")
let reader = @aio.open_disk_sidecar(source,index,@aio.AsyncVerifyContent(@aio.source_sha256))
let provider = reader.query_source().decode_payload(@mcap.standard_payload_decoder())
let dataset = @aio.open_dataset([provider])
let cursor = dataset.cursor(@mcap.all_query())
// 消费 cursor 后取消，并由持有者关闭两个源。
```

## 校验与资源

打开时读取有上限的头部、完整计算源指纹，并逐页校验整个索引的 CRC、位置范围、声明、页内及跨页排序。只在审核完成后返回 reader，不把尚未审核的索引暴露给查询。不会把完整索引作为 Bytes 加载，也不能宣称首次打开只读命中页。默认指纹模式仍可能读完整远程源；缓存能减少重复网络下载，但不免除逻辑审核工作。

查询通过二分定位、LRU 页缓存和按需源记录读取。每页最多 8192 条/36 字节记录，完整打开保留声明头及有限页；查询另有一个当前页，cache_pages=0 时仍可工作。Chunk 缓存、页缓存、源记录在途预算分别约束，非总 RSS 上限。默认声明头 8 MiB、页缓存 4 页、块缓存 8 MiB、索引上限 1 TiB；单游标默认在途 span 上限 32 MiB。

库保留显式 AsyncTrustIdentity，用于调用者已建立可信源版本与索引对应关系的场景，不自动把 ETag 当成源内容指纹。CLI 固定完整内容验证，不提供远程快速跳过验证开关。HTTP 的两个源各自须稳定快照；持久缓存使用各自的 URL/ETag/大小身份。

同一游标拒绝并发操作；支持 seek、取消、失败后停止。取消游标默认不取消其他使用者共享的源。query_source(abort_sources_on_cancel=true) 可显式把源所有权交给该 provider；使用者仍须关闭缓存 host、释放目录租约。索引错误、源定位错误或预算错误均不发布部分文件。

## 完整命令样例

```sh
moon build --target js
node _build/js/debug/build/cmd/asyncpaged/asyncpaged.js input.mcap --index input.midx2 --decode --select /position --limit 20
# cache-parent 须已存在且专用于各 item-N 子缓存。
node _build/js/debug/build/cmd/asyncpaged/asyncpaged.js https://host/input.mcap --index https://host/input.midx2 --http-cache cache-parent --offset 10 --limit 20 --output subset.ndjson
python tools/async_paged_workflows.py
```

支持多个输入并为每个输入重复指定一个 --index，支持正反序、主题/时间、条件和全局分页；只接受 MIDX2。--decode 可启用 JSON/Protobuf/ros2msg-XCDR1。stdout 写数据，stderr 写索引和缓存计数。HTTP host 为 Node.js，读取协议、页校验和游标调度核心为 MoonBit。

同步核心新增 DiskSidecarAudit 的 poll/feed/finish 和 DiskSidecarPollCursor 的 poll/feed_index/feed_source，分别报告索引页与源记录读取请求。异步适配器驱动这些状态机，避免同步读隐藏在异步查询内。审核未完成、错误顺序 feed、CRC/排序错误和越界均明确失败。

测试比较三种压缩、零页缓存、正反序及 seek，包含异步字段筛选后全局分页、跨度预算、取消及并发拒绝。远程故障服务器同时提供 MCAP 与 MIDX2：冷/热独立进程查询对照参考结果，热缓存仅产生两个元数据探测请求，仍完整校验源与索引；另覆盖损坏页和过期源拒绝。
