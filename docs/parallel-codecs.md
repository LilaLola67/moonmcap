# 有界工作线程：解压与消息解码

0.15.24 提供可选 Node.js worker_threads 后端。实际解压、CRC 和消息解码函数仍由 MoonBit 编译，线程通信与生命周期由 Node 主机适配器处理。原有同步/异步 API 不会自动创建线程；Wasm 核心仍可运行，打开宿主线程池的非 JS 调用明确报不支持。

```sh
moon build --target js
node _build/js/debug/build/cmd/parallel/parallel.js input.mcap --workers 2 --prefetch 2 --limit 20 --output subset.ndjson
node _build/js/debug/build/cmd/parallel/parallel.js input.mcap --decode --where '{"path":"/temperature","op":">","value":80}' --select /temperature --workers 2 --output hot.ndjson
python tools/worker_workflows.py
python tools/worker_benchmark.py
```

## 如何组合

`open_worker_pool(compiled_entry)` 创建可复用的线程池。`open_parallel_reader(source,pool)` 在 FileCursor 请求之前进行有限批次预取，将足够大的压缩 Chunk 交给工作线程展开；主线程继续校验原始属性、展开内容的 CRC、记录、声明、MessageIndex 和稳定排序。未压缩或低于阈值的块走原路径。无内置索引的输入使用既有、有预算的完整回退，不承诺线程加速。

`ParallelReader.query_source()` 可进入 AsyncDataset。附加 `decode_parallel(pool)` 可并行执行标准 JSON、Protobuf、ros2msg/XCDR1 负载解码，再组合 filter_json、投影与全局分页。解码结果保留源消息时间、序号、通道和稳定次序；64 位数字仍用精确字面值。批次返回按原输入次序等待，不按线程完成先后输出。未消费的预取消息错误被推迟到实际消费该消息时报告，避免有限查询被后续无关 payload 错误影响。

首版负载线程使用标准数值解码语义，不支持把任意用户闭包传到线程，也不提供并行 ProtoJSON 配置。需要自定义解码/ProtoJSON 时，可在并行解压 provider 后使用原 decode_payload。MFLD1 首版仍为同步计划，asyncpaged 则使用独立的异步侧车路径，不能给 parallel CLI 传 sidecar 参数。库中的 source 可以来自本地、HTTP 或已有持久缓存。

CLI 默认沿用 NDJSON 的 data_hex 表示；--decode 会把该负载转换成 JSON 字节。附加 --where true 或 --select 可同时输出可读的 decoded 字段，便于直接查看结构化结果。

## 安装后的工作线程入口

仓库 `moon build --target js` 会同时构建 cmd/codecworker，parallel CLI 默认找到相邻编译产物。作为库安装后，使用方应在自己的工程建立一个可执行包，例如 cmd/worker，并导入 moonmcap/asyncio 和根包，入口为：

```moonbit
fn main {
  @aio.run_codec_worker() catch { @mcap.Invalid(reason) => abort(reason) }
}
```

然后构建 JS，并将自身 `_build/js/debug/build/cmd/worker/worker.js` 的路径传给 open_worker_pool。无需在运行时安装 MoonBit 编译器或调用外部压缩工具；编译后的工作线程程序与应用一同部署。自定义 --worker-entry 必须是可信的本地编译入口：它拥有 Node.js 进程权限，CRC 不是对任意第三方工作线程代码的安全认证。

```moonbit
let pool = @aio.open_worker_pool("./_build/js/debug/build/cmd/worker/worker.js")
let source = @aio.open_file("input.mcap")
try {
  let reader = @aio.open_parallel_reader(source,pool)
  let provider = reader.query_source().decode_parallel(pool)
  let dataset = @aio.open_dataset([provider])
  let cursor = dataset.cursor(@mcap.all_query())
  while cursor.next() is Some(message) { ignore(message) }
} catch { error => { source.cancel(); pool.close(); raise error } }
source.cancel()
pool.close()
```

## 预算、取消和背压

默认 2 个工作线程，最多 16 个等待/在途任务，预取 2 项；允许线程数 1–8、预取 1–16、任务数 1–64。单任务输入默认 16 MiB、输出上限 128 MiB、声明上下文 1 MiB、编码通信保留预算 128 MiB。实际展开大小还受源 Limits.max_decompressed_bytes 约束，不因工作线程放宽嵌套记录的原始上限。较大的单块可能要求提高通信预算或减少预取。

主机按请求 UTF-8 字节加预留响应十六进制字节计账，任务结束才释放保留额；游标对原始和展开数据以及十六进制传输作保守批次计费，单批上限独立受 buffer_bytes 限制。多游标各自的预取/候选预算和线程池的全局通信预算分别约束，不是整个进程 RSS 限额。每个线程另有运行时、解压临时分配、标准类型缓存和一份有限声明上下文。metrics 的 peak_reserved_bytes 与 peak_cursor_batch_bytes 不应被解释为进程内存。

默认压缩块阈值按展开量 64 KiB 判断，消息阈值为 4096 字节；较小工作在主线程完成，避免每条消息都跨线程。阈值可配置。每批只准备有限条目，等待本批任务结束后再交付消费，消费者不拉取就不继续准备后续批次。它不是无限后台解压，也不保证首条消息低延迟。

单调用者取消会停止等待；未开始任务移出队列，已经运行的同步 CPU 工作继续占用预算直到完成。取消游标不会关闭共享池；`pool.close()` 终止所有线程并等待退出，关闭可重复调用且支持取消中的清理。单任务超时、线程退出、协议错误或畸形超预算响应会使池失败并拒绝剩余任务。关闭不修改源文件；CLI 错误会撤销暂存输出。

## 实测与验证

[本地报告](../examples/validation/worker-benchmark-0.15.24.json)记录 Windows / Node 24.15.0 的单次测量，包括启动、I/O、通信及 NDJSON 格式化，输出交给系统丢弃。相同命令管线的主线程阈值模式与 2 线程模式比较：

| 场景 | 主线程耗时 | 2 线程耗时 | 主线程 / 2 线程峰值 RSS |
|---|---:|---:|---:|
| 64 × 64 KiB 负载的解压流程 | 1.177 s | 1.135 s | 148232 / 351548 KiB |
| 64 × 64 KiB JSON 的解码流程 | 3.171 s | 2.086 s | 146812 / 295564 KiB |

该案例中解码收益较明显，解压端到端收益很小，同时内存显著增加。主线程对照仍启动一个空闲线程，以保持同一管线；每项只测一次，不是严格统计基准，不承诺任意文件加速。因此保留显式启用和小任务阈值，实际部署应重新测量。

验证包括真实编译的 MoonBit 工作线程 ID、三种压缩的双向精确查询与原始 payload 对照、主线程小块旁路、运行中取消的预算保留、关闭回收、任务上限、超时、线程异常、非法响应、部分输出撤销及超过 16 MiB 的展开块。核心展开协议在 Wasm/JS 同样测试。

主机依据：[Node worker_threads](https://nodejs.org/docs/latest-v22.x/api/worker_threads.html)。
