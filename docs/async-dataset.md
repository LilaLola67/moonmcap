# 统一本地与远程异步查询（0.15.10）

AsyncQuerySource/AsyncMessageStream 是统一提供方协议。AsyncReader 和 AsyncSidecarReader 均可调用 query_source，再可选 filter_json，交给 asyncio.open_dataset。Dataset 复用同步核心的声明去重和 ID 映射，使用异步堆归并；next/next_page 返回带 source_index 的 DatasetMessage，支持双向稳定顺序、全局 offset/limit、seek、取消、进度和有背压的 write_ndjson。

```sh
moon build --target js
node _build/js/debug/build/cmd/dataset/dataset.js local.mcap https://example.org/remote.mcap --where '{"path":"/speed","op":">","value":80}' --select /speed --limit 100 --output selected.ndjson
```

该示例固定输出 NDJSON，文件输出独占原子新建，stdout 已发送行不可撤回。输入可以混合文件与 URL；--index 重复时必须与每个输入逐一配对。普通远程查询沿用 HTTP 强 ETag/If-Match、Range 校验及失败策略。--full 不在这个选择性查询示例中接收，完整校验使用单独入口。

异步 open_sidecar 接收 AsyncVerifyContent(async hash) 或 AsyncTrustIdentity(token)。JS 提供 source_sha256，可读取本地或 HTTP AsyncSource。内容验证会完整读取源计算 SHA-256，再按索引读取所需记录，绝不把只读 ETag 当作完整内容哈希；显式信任身份时 info.content_verified=false，调用者负责身份绑定。示例对远程侧车固定要求内容哈希；--fast-index 仅对本地使用文件身份令牌。侧车编码版本保持 1，可复用既有索引。

同步 SidecarCursor 新增 poll/feed，使用与 FileCursor 相同的 FilePoll；next 保留原签名并驱动该协议。feed 长度、记录类型、压缩/CRC、消息身份均校验，未请求/关闭后的 feed 拒绝。异步侧车等待实际 I/O 后 feed，不需要下载并保留完整源文件。

默认最多 64 个源，统一候选预算 64 MiB，分配一半给子提供方、另一半容纳堆头；侧车请求跨度必须能放入分配预算，解压缓存和声明各自另计。无索引普通读取仍是显式有预算的内存回退，materialized_sources 会反映；有侧车的无索引文件可按位置读取。源必须稳定且按查询顺序返回声明一致的消息；自定义提供方越界、乱序和负数内存报告会失败并关闭全部子流。

一个游标禁止并发 next/next_page/export/seek；cancel 可在等待时调用，取消后丢弃未交付头部。默认取消游标不会关闭共享 AsyncSource。query_source(abort_source_on_cancel=true) 显式移交源的独占取消权，适用于一次性查询；这时取消或达到 limit 可终止活动 HTTP 请求，也意味着不可再用同一源 seek/开新查询。通常应在整个数据集用完后显式 source.cancel。

测试覆盖本地 HTTP 服务器与真实文件混用、普通/侧车、无索引回退、JSON 投影、双向分页、快照变化时输出原子失败；注入异步测试还覆盖 seek、取消、并发拒绝、提供方乱序、跨度预算及导出背压。运行 `python tools/async_dataset_workflows.py`。
