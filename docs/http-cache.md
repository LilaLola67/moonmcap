# 持久 HTTP Range 缓存

Node.js 22+ 的 asyncio 适配器提供 `open_cached_http(url, directory)`，返回 `CachedHttpSource`。将其 `source()` 传给现有 `open_reader`、查询、数据集或流式读取 API。使用后必须 `await close()`，它会取消在途请求、等待队列结束并释放目录租约；关闭支持重复调用和取消中的清理。Wasm 核心不依赖此功能，非 JS 主机调用会明确报不支持。

```moonbit
let cached = @aio.open_cached_http(url, "./http-cache")
try {
  let reader = @aio.open_reader(cached.source())
  let dataset = @aio.open_dataset([reader.query_source()])
  let cursor = dataset.cursor(@mcap.all_query())
  while cursor.next() is Some(message) { ignore(message) }
  cursor.cancel()
} catch { error => { cached.close(); raise error } }
cached.close()
```

上面组合查询时也可以直接使用 `open_dataset([reader.query_source()]).cursor(...)`；命令行样例提供完整的输出、错误与资源清理流程：

```sh
moon build --target js
node _build/js/debug/build/cmd/cached/cached.js https://host/recording.mcap ./http-cache --limit 20 --output subset.ndjson
node _build/js/debug/build/cmd/cached/cached.js https://host/recording.mcap ./http-cache --page-bytes 65536 --disk-bytes 33554432 --parallel 4 --prefetch 3 --topic /imu --limit 10
python tools/cached_http_workflows.py
```

stderr 最后一行是独立缓存统计，stdout 为 NDJSON。`--output` 仅创建新文件；查询错误不会发布部分文件。`--probe OFFSET LENGTH` 可读取任意范围并输出十六进制，可重复至多 16 次并发探测，单次上限 16 MiB。示例仅接入 HTTP range provider，不接受 CLI sidecar 参数；库层可组合其他适配器。

## 一致性与损坏处理

每次打开都请求 `bytes=0-0`，要求正确的 206、Content-Range、identity 编码和强 ETag。缓存身份是 URL、ETag、文件大小和页大小的 SHA-256；目录只保存散列身份，不以明文存 URL。每次缺页都携带 If-Match，412、ETag/大小变化立即报错，禁止静默回退为全文件下载或混合不同快照。远端必须正确履行 ETag 语义；服务端错误复用 ETag 无法由客户端完全识别。

页封装保存格式标识、源偏移、长度、身份和 payload SHA-256。命中时检查封装和摘要；损坏的自有普通页删除后重新获取，符号链接、硬链接及不安全文件明确拒绝。新打开取得不同 ETag 时使用不同身份，旧版本页可按 LRU 淘汰。同一次打开只读取已确认快照的页；若远端在缓存命中期间变化，仍可返回旧快照中的已缓存字节，遇到缺页时通过 If-Match 检出变化。

## 预算与并发

`http_cache_options()` 默认页 64 KiB、页文件总预算 32 MiB、最多 4 个并发页任务、向后预取 3 页、请求超时 5000 ms、重试 2 次。页大小允许 4 KiB–4 MiB，容量至多 1 GiB，任务并发 1–16，预取 0–16。容量必须容纳一整页及 84 字节封装；打开时先淘汰已有超预算数据，随后将暂存写入和最终页同时计入预算。标记和租约这两个很小的控制文件不计入页预算。

自有页最多 4096 个，目录枚举最多 8192 个条目，等待/在途页去重表最多 64 项，单 host 同时逻辑读取最多 16 个，超过预算会报错。页任务包含磁盘命中处理和网络缺页读取；预取按容量收缩，当前读取等待这批预取结束，没有无限后台下载。同一页的并发请求共享 promise。取消单个调用者不会取消其他调用者共享的网络任务；显式 `source().cancel()` 或 `close()` 才会取消整个源，close 会等待这些共享任务退出。

磁盘预算是页文件字节上限，不是 RSS 上限。内存还包括并发响应、预取结果、读取返回值和 Node/MoonBit 运行时。统计中的 requests 包含元数据探测及重试；response_bytes 是实际消费的响应正文，未包括 HTTP 头、TLS 开销和取消前由网络栈接收但未交付的字节。disk_hits/misses、active_peak/pending_peak、evictions、corrupt_pages、peak_disk_bytes 单独统计，不与上层逻辑 ReadSource 计数混用。

## 目录所有权与恢复

首次使用要求空目录；之后必须有格式匹配的所有权标记。一个缓存目录同时只允许一个存活进程/host 持有租约，多个查询可共享同一个 host。竞争者报错，不改动现有租约。进程被终止后，下次打开确认租约 PID 已不存在才回收租约，并清理符合自有命名格式的普通暂存文件。PID 被复用时保守拒绝打开，可换用新缓存目录。

目录遇到未知文件会拒绝工作并保留文件。清理只删除已识别的自有页和暂存文件，不递归删除目录、不清理其他业务文件。此目录是应用缓存，不应存放手动添加的文件；路径各级和权限应由调用者管理。页通过同目录原子 rename 发布，但不承诺断电持久性；断电损坏页依靠摘要检查后重新获取。

故障测试包含独立进程冷/热缓存、真实并发与去重、损坏修复、封装计费、ETag 变更、无 Range/无强验证器、畸形响应、超时/重试、412 的原子失败、未知文件保护及实际杀进程后恢复；异步测试另行覆盖单任务取消、共享源继续可用及源取消。
