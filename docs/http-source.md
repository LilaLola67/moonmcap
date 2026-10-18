# HTTP 范围读取

```moonbit
let source = @aio.open_http(url, timeout_ms=5000, retries=2, max_range_bytes=32 * 1024 * 1024)
let reader = @aio.open_reader(source)
let cursor = reader.cursor({ ..@mcap.all_query(), topics: ["/imu"], limit: 32, })
let page = cursor.next_page(32)
source.cancel()
```

```sh
moon run cmd/async --target js -- remote URL /imu 20000000000 30000000000
python tools/http_workflows.py
```

支持 Node.js 22.3+ 的 HTTP/HTTPS，使用 fetch/Promise 与 AbortSignal。先 GET bytes=0-0 获取总长度和强 ETag；后续每次带 Range、If-Match 和 Accept-Encoding: identity。要求 206、精确起止范围/总长度、相同强 ETag、匹配的响应体长度。弱/缺失 ETag、412、长度变化、Range 被忽略、压缩的 HTTP 表示或自动跳转均拒绝，不退回整文件下载。该源针对能提供稳定范围快照的服务器，不支持所有静态站点。

max_range_bytes 约束每次 HTTP 请求大小；更大的逻辑 read_exact 自动拆成多个范围。响应流在累计超过请求长度时中止，不能仅信任 Content-Length。MCAP 摘要、候选和缓存继续使用独立预算；HTTP 缓冲计数不是进程 RSS 上限。

timeout_ms 作用于每次尝试，retries 表示额外尝试次数（0–10）。网络错误、截断响应及 408/429/500/502/503/504 可有限退避重试；不对格式错误、快照变化或其他状态静默重试。任务取消通过 Promise AbortSignal 传播；AsyncSource.cancel 关闭整个源并中止活动 HTTP 请求。接口不接受 URL userinfo，也不添加认证头。

物理网络读取包括一次单字节探测；AsyncSource.metrics 统计后续逻辑回调的成功读取和调用，不包含探测及重试的额外网络量。测试报告另记录服务器端实际请求范围与 If-Match。

本机故障服务器验证正常查询、重试成功、快照/大小变化、200 回退、弱 ETag、错误 Content-Range、长度错误、过量/截断响应、HTTP 编码、超时、跳转和活动请求取消。协议依据 [RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html) 的 Range、ETag 和 If-Match 语义。
